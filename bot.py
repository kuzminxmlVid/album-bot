# -*- coding: utf-8 -*-
"""
Album List Telegram Bot (clean rewrite)

Env vars:
- TOKEN (required)            Telegram bot token
- DATABASE_URL (required)     Postgres connection string (Railway provides it)
- ALBUMS_DIR (optional)       Directory with .xlsx lists, default: ./albums
- ALBUM_LIST (optional)       Default list name (file stem), default: first list found
- ADMINS (optional)           Comma-separated Telegram user IDs allowed to use admin commands
- LOG_LEVEL (optional)        INFO/DEBUG, default: INFO

Optional (artist notes via OpenAI):
- OPENAI_API_KEY
- OPENAI_MODEL (default: gpt-4o-mini)
- AI_DAILY_LIMIT (default: 20)
"""

import os
import re
import html
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any
from urllib.parse import quote, unquote_plus

import pandas as pd
import aiohttp
import asyncpg

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

BOT_VERSION = os.getenv("BOT_VERSION", "clean-rewrite-2026-02-11")

# ---------------- logging ----------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("album_bot")

# ---------------- config ----------------

class Config:
    TOKEN = os.getenv("TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    BASE_DIR = Path(__file__).resolve().parent
    ALBUMS_DIR = Path(os.getenv("ALBUMS_DIR", str(BASE_DIR / "albums")))
    DEFAULT_LIST = os.getenv("ALBUM_LIST", "")
    ADMINS = os.getenv("ADMINS", "")  # comma-separated ints

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    AI_DAILY_LIMIT = int(os.getenv("AI_DAILY_LIMIT", "20"))

if not Config.TOKEN or not Config.DATABASE_URL:
    raise RuntimeError("ENV vars not set: TOKEN and/or DATABASE_URL")

# Railway sometimes provides postgres://
if Config.DATABASE_URL.startswith("postgres://"):
    Config.DATABASE_URL = Config.DATABASE_URL.replace("postgres://", "postgresql://", 1)

ADMIN_IDS: set[int] = set()
for x in (Config.ADMINS or "").split(","):
    x = x.strip()
    if x.isdigit():
        ADMIN_IDS.add(int(x))

# ---------------- helpers ----------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def canon_list_name(name: str) -> str:
    s = (name or "").strip()
    s = unquote_plus(s)
    s = re.sub(r"\s+", " ", s)
    return s

def encode_list_name(name: str) -> str:
    return quote(canon_list_name(name), safe="")

def esc(s: Any) -> str:
    return html.escape("" if s is None else str(s))

def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS

# ---------------- list loading ----------------

LIST_FILES: Dict[str, Path] = {}          # list_name -> xlsx path
LIST_TOKENS: Dict[str, str] = {}         # list_name -> short token
TOKEN_TO_LIST: Dict[str, str] = {}       # token -> list_name
ALBUM_CACHE: Dict[str, pd.DataFrame] = {}  # list_name -> df
ITUNES_CACHE: Dict[Tuple[str, str], Dict[str, str]] = {}  # (artist, album) -> {artwork_url, itunes_url, track_view_url}

def _short_token(name: str) -> str:
    # keep callback_data short and stable
    enc = encode_list_name(name)
    if len(enc) <= 32:
        return enc
    import hashlib
    h = hashlib.sha1(enc.encode("utf-8")).hexdigest()[:16]
    return f"h{h}"

def scan_lists() -> None:
    Config.ALBUMS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(Config.ALBUMS_DIR.glob("*.xlsx"))
    if not files:
        raise RuntimeError(f"No .xlsx lists found in {Config.ALBUMS_DIR}")
    LIST_FILES.clear()
    LIST_TOKENS.clear()
    TOKEN_TO_LIST.clear()
    for f in files:
        name = f.stem.strip()
        if not name:
            continue
        LIST_FILES[name] = f
    for name in LIST_FILES.keys():
        tok = _short_token(name)
        # avoid collisions
        i = 2
        base = tok
        while tok in TOKEN_TO_LIST and TOKEN_TO_LIST[tok] != name:
            tok = f"{base}{i}"
            i += 1
        LIST_TOKENS[name] = tok
        TOKEN_TO_LIST[tok] = name

def normalize_df(df: pd.DataFrame, list_name: str) -> pd.DataFrame:
    # Accept both EN and RU headers, normalize to: rank, artist, album, year, genre, cover, songlink
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols)

    def pick(*cands: str) -> Optional[str]:
        lowered = {c.lower(): c for c in df.columns}
        for cand in cands:
            if cand.lower() in lowered:
                return lowered[cand.lower()]
        return None

    col_rank = pick("rank", "позиция", "номер", "no", "№")
    col_artist = pick("artist", "исполнитель", "артист", "band", "группа")
    col_album = pick("album", "альбом", "релиз", "release", "title", "название")
    col_year = pick("year", "год")
    col_genre = pick("genre", "жанр")
    col_cover = pick("cover", "cover_url", "обложка", "арт", "artwork")
    col_songlink = pick("songlink", "songlink_url", "link", "ссылка")

    missing = []
    if not col_rank:
        missing.append("rank/позиция")
    if not col_artist:
        missing.append("artist/исполнитель")
    if not col_album:
        missing.append("album/альбом")
    if missing:
        raise ValueError(f"{list_name}: missing columns: {', '.join(missing)}")

    out = pd.DataFrame()
    out["rank"] = pd.to_numeric(df[col_rank], errors="coerce")
    out["artist"] = df[col_artist].astype(str).fillna("").str.strip()
    out["album"] = df[col_album].astype(str).fillna("").str.strip()

    out["year"] = ""
    if col_year:
        out["year"] = df[col_year].fillna("").astype(str).str.strip()

    out["genre"] = ""
    if col_genre:
        out["genre"] = df[col_genre].fillna("").astype(str).str.strip()

    out["cover"] = ""
    if col_cover:
        out["cover"] = df[col_cover].fillna("").astype(str).str.strip()

    out["songlink"] = ""
    if col_songlink:
        out["songlink"] = df[col_songlink].fillna("").astype(str).str.strip()

    out = out.dropna(subset=["rank"]).copy()
    out["rank"] = out["rank"].astype(int)
    out = out.sort_values("rank").reset_index(drop=True)

    # drop empty artist/album rows
    out = out[(out["artist"].str.len() > 0) & (out["album"].str.len() > 0)].reset_index(drop=True)
    return out

def load_list_df(list_name: str) -> pd.DataFrame:
    if list_name in ALBUM_CACHE:
        return ALBUM_CACHE[list_name]
    if list_name not in LIST_FILES:
        raise KeyError(f"Unknown list: {list_name}")
    path = LIST_FILES[list_name]
    df = pd.read_excel(path)
    df = normalize_df(df, list_name)
    ALBUM_CACHE[list_name] = df
    return df

def list_intro_text(list_name: str) -> Optional[str]:
    intro_path = Config.ALBUMS_DIR / f"{list_name}.intro.txt"
    try:
        if intro_path.exists():
            t = intro_path.read_text(encoding="utf-8").strip()
            return t or None
    except Exception as e:
        log.debug("intro read failed: %s", e)
    return None

# ---------------- DB ----------------

DB_POOL: Optional[asyncpg.Pool] = None

async def db_init() -> None:
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(Config.DATABASE_URL, min_size=1, max_size=5)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            album_list TEXT NOT NULL
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_progress (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            current_index INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, album_list)
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_inputs (
            user_id BIGINT PRIMARY KEY,
            mode TEXT NOT NULL,
            payload TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_favorites_user
        ON favorites (user_id, added_at DESC)
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS covers (
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            cover_url TEXT NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS songlinks (
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            songlink_url TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_artist_notes (
            artist_key TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_usage (
            user_id BIGINT NOT NULL,
            day DATE NOT NULL,
            cnt INTEGER NOT NULL,
            PRIMARY KEY (user_id, day)
        )
        """)

async def db_close() -> None:
    global DB_POOL
    if DB_POOL:
        await DB_POOL.close()
        DB_POOL = None

async def db_get_user_list(user_id: int) -> str:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT album_list FROM users WHERE user_id=$1", user_id)
        if row and row["album_list"] in LIST_FILES:
            return row["album_list"]
        # set default
        default = Config.DEFAULT_LIST if Config.DEFAULT_LIST in LIST_FILES else next(iter(LIST_FILES.keys()))
        await conn.execute("""
        INSERT INTO users (user_id, album_list)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET album_list=EXCLUDED.album_list
        """, user_id, default)
        return default

async def db_set_user_list(user_id: int, list_name: str) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, album_list)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET album_list=EXCLUDED.album_list
        """, user_id, list_name)

async def db_get_progress(user_id: int, list_name: str) -> int:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT current_index FROM user_progress WHERE user_id=$1 AND album_list=$2
        """, user_id, list_name)
        if row:
            return int(row["current_index"])
        await conn.execute("""
        INSERT INTO user_progress (user_id, album_list, current_index)
        VALUES ($1, $2, 0)
        ON CONFLICT (user_id, album_list) DO NOTHING
        """, user_id, list_name)
        return 0

async def db_set_progress(user_id: int, list_name: str, idx: int) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO user_progress (user_id, album_list, current_index, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (user_id, album_list)
        DO UPDATE SET current_index=EXCLUDED.current_index, updated_at=NOW()
        """, user_id, list_name, idx)

async def db_input_set(user_id: int, mode: str, payload: Optional[dict] = None) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO user_inputs (user_id, mode, payload, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET mode=EXCLUDED.mode, payload=EXCLUDED.payload, updated_at=NOW()
        """, user_id, mode, json.dumps(payload or {}, ensure_ascii=False))

async def db_input_get(user_id: int) -> Optional[Tuple[str, dict]]:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT mode, payload FROM user_inputs WHERE user_id=$1", user_id)
        if not row:
            return None
        payload = {}
        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            payload = {}
        return row["mode"], payload

async def db_input_clear(user_id: int) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("DELETE FROM user_inputs WHERE user_id=$1", user_id)

async def db_is_favorite(user_id: int, list_name: str, rank: int) -> bool:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT 1 FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3
        """, user_id, list_name, rank)
        return bool(row)

async def db_toggle_favorite(user_id: int, list_name: str, rank: int) -> bool:
    """Return new state: True if now favorite."""
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        exists = await conn.fetchrow("""
        SELECT 1 FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3
        """, user_id, list_name, rank)
        if exists:
            await conn.execute("""
            DELETE FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3
            """, user_id, list_name, rank)
            return False
        await conn.execute("""
        INSERT INTO favorites (user_id, album_list, rank) VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """, user_id, list_name, rank)
        return True

async def db_list_favorites(user_id: int, list_name: str, limit: int, offset: int) -> List[int]:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch("""
        SELECT rank FROM favorites
        WHERE user_id=$1 AND album_list=$2
        ORDER BY added_at DESC
        LIMIT $3 OFFSET $4
        """, user_id, list_name, limit, offset)
        return [int(r["rank"]) for r in rows]

async def db_count_favorites(user_id: int, list_name: str) -> int:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT COUNT(*) AS c FROM favorites WHERE user_id=$1 AND album_list=$2
        """, user_id, list_name)
        return int(row["c"] or 0)

async def db_get_cover(list_name: str, rank: int) -> Optional[str]:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT cover_url FROM covers WHERE album_list=$1 AND rank=$2
        """, list_name, rank)
        return row["cover_url"] if row else None

async def db_set_cover(list_name: str, rank: int, url: str, source: str = "manual") -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO covers (album_list, rank, cover_url, source, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (album_list, rank)
        DO UPDATE SET cover_url=EXCLUDED.cover_url, source=EXCLUDED.source, updated_at=NOW()
        """, list_name, rank, url, source)

async def db_del_cover(list_name: str, rank: int) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("DELETE FROM covers WHERE album_list=$1 AND rank=$2", list_name, rank)

async def db_get_songlink(list_name: str, rank: int) -> Optional[str]:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT songlink_url FROM songlinks WHERE album_list=$1 AND rank=$2
        """, list_name, rank)
        return row["songlink_url"] if row else None

async def db_set_songlink(list_name: str, rank: int, url: str) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO songlinks (album_list, rank, songlink_url, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (album_list, rank)
        DO UPDATE SET songlink_url=EXCLUDED.songlink_url, updated_at=NOW()
        """, list_name, rank, url)

async def db_del_songlink(list_name: str, rank: int) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("DELETE FROM songlinks WHERE album_list=$1 AND rank=$2", list_name, rank)

async def db_ai_can_use(user_id: int) -> bool:
    assert DB_POOL
    today = date.today()
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT cnt FROM ai_usage WHERE user_id=$1 AND day=$2", user_id, today)
        cnt = int(row["cnt"]) if row else 0
        return cnt < Config.AI_DAILY_LIMIT

async def db_ai_inc(user_id: int) -> None:
    assert DB_POOL
    today = date.today()
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO ai_usage (user_id, day, cnt) VALUES ($1, $2, 1)
        ON CONFLICT (user_id, day) DO UPDATE SET cnt=ai_usage.cnt+1
        """, user_id, today)

async def db_ai_get_artist(artist_key: str) -> Optional[str]:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT text FROM ai_artist_notes WHERE artist_key=$1", artist_key)
        return row["text"] if row else None

async def db_ai_set_artist(artist_key: str, text_: str) -> None:
    assert DB_POOL
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO ai_artist_notes (artist_key, text, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (artist_key) DO UPDATE SET text=EXCLUDED.text, updated_at=NOW()
        """, artist_key, text_)

# ---------------- external lookups ----------------

HTTP: Optional[aiohttp.ClientSession] = None

async def http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout_s: int = 12) -> Optional[dict]:
    assert HTTP
    try:
        async with HTTP.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def itunes_search_album(artist: str, album: str) -> Optional[dict]:
    key = (artist.strip().lower(), album.strip().lower())
    if key in ITUNES_CACHE:
        return ITUNES_CACHE[key]

    term = f"{artist} {album}".strip()
    data = await http_get_json(
        "https://itunes.apple.com/search",
        params={"term": term, "entity": "album", "limit": 1, "media": "music"},
        headers={"User-Agent": f"AlbumBot/{BOT_VERSION}"},
        timeout_s=12,
    )
    if not data or not data.get("results"):
        return None

    r = data["results"][0]
    artwork = (r.get("artworkUrl100") or "").strip()
    if artwork:
        artwork = re.sub(r"/\d+x\d+bb\.", "/600x600bb.", artwork)

    out = {
        "artwork_url": artwork,
        "itunes_url": (r.get("collectionViewUrl") or "").strip(),
        "collection_name": (r.get("collectionName") or "").strip(),
        "artist_name": (r.get("artistName") or "").strip(),
    }
    ITUNES_CACHE[key] = out
    return out

async def songlink_from_url(any_store_url: str) -> Optional[str]:
    data = await http_get_json(
        "https://api.song.link/v1-alpha.1/links",
        params={"url": any_store_url},
        headers={"User-Agent": f"AlbumBot/{BOT_VERSION}"},
        timeout_s=12,
    )
    if not data:
        return None
    page = (data.get("pageUrl") or "").strip()
    return page or None

# ---------------- AI (artist note) ----------------

async def ai_artist_note(artist: str, user_id: int) -> Optional[str]:
    if not Config.OPENAI_API_KEY:
        return None

    artist_key = artist.strip().lower()
    cached = await db_ai_get_artist(artist_key)
    if cached:
        return cached

    if not await db_ai_can_use(user_id):
        return "Лимит на сегодня. Попробуй завтра."

    prompt = (
        "Ты музыкальный редактор.\n"
        "Ответ строго по-русски.\n"
        "Дай короткую справку об артисте.\n"
        "Строго 4 строки, каждая начинается с метки.\n"
        "КТО: (1 предложение)\n"
        "ЗВУК: (1 предложение)\n"
        "ВЛИЯНИЕ: (1 предложение)\n"
        "С ЧЕГО НАЧАТЬ: (до 3 релизов через запятую)\n"
        f"Артист: {artist}\n"
    )

    payload = {
        "model": Config.OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "Отвечай фактологично. Не выдумывай. Если не уверен, так и скажи."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
        "max_tokens": 220,
    }

    assert HTTP
    try:
        async with HTTP.post(
            "https://api.openai.com/v1/chat/completions",
            json=payload,
            headers={"Authorization": f"Bearer {Config.OPENAI_API_KEY}"},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    text_ = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    if not text_:
        return None

    # basic cleanup: ensure 4 lines max
    lines = [ln.strip() for ln in text_.splitlines() if ln.strip()]
    lines = lines[:4]
    text_ = "\n".join(lines)

    await db_ai_inc(user_id)
    await db_ai_set_artist(artist_key, text_)
    return text_

# ---------------- render ----------------

@dataclass
class Album:
    rank: int
    artist: str
    album: str
    year: str = ""
    genre: str = ""
    cover: str = ""
    songlink: str = ""

def get_album(list_name: str, idx: int) -> Tuple[Album, int]:
    df = load_list_df(list_name)
    total = len(df)
    if total == 0:
        raise RuntimeError(f"List {list_name} is empty")
    idx = max(0, min(idx, total - 1))
    row = df.iloc[idx]
    return Album(
        rank=int(row["rank"]),
        artist=str(row["artist"]),
        album=str(row["album"]),
        year=str(row.get("year", "") or ""),
        genre=str(row.get("genre", "") or ""),
        cover=str(row.get("cover", "") or ""),
        songlink=str(row.get("songlink", "") or ""),
    ), total

def find_index_by_rank(list_name: str, rank: int) -> Optional[int]:
    df = load_list_df(list_name)
    hits = df.index[df["rank"] == int(rank)].tolist()
    if not hits:
        return None
    return int(hits[0])

def search_artist(list_name: str, query: str, limit: int = 20) -> List[Tuple[int, str, str]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    df = load_list_df(list_name)
    m = df["artist"].astype(str).str.lower().str.contains(re.escape(q), na=False)
    res = df[m].head(limit)
    out: List[Tuple[int, str, str]] = []
    for _, r in res.iterrows():
        out.append((int(r["rank"]), str(r["artist"]), str(r["album"])))
    return out

async def resolve_cover_url(list_name: str, alb: Album) -> Optional[str]:
    # 1) manual cache
    cached = await db_get_cover(list_name, alb.rank)
    if cached:
        return cached
    # 2) from sheet
    if alb.cover and alb.cover.startswith(("http://", "https://")):
        return alb.cover
    # 3) itunes
    it = await itunes_search_album(alb.artist, alb.album)
    if it and it.get("artwork_url"):
        await db_set_cover(list_name, alb.rank, it["artwork_url"], source="itunes")
        return it["artwork_url"]
    return None

async def resolve_songlink_url(list_name: str, alb: Album) -> Optional[str]:
    cached = await db_get_songlink(list_name, alb.rank)
    if cached:
        return cached
    if alb.songlink and alb.songlink.startswith(("http://", "https://")):
        await db_set_songlink(list_name, alb.rank, alb.songlink)
        return alb.songlink

    it = await itunes_search_album(alb.artist, alb.album)
    it_url = (it or {}).get("itunes_url") or ""
    if not it_url:
        return None

    sl = await songlink_from_url(it_url)
    if not sl:
        return None
    await db_set_songlink(list_name, alb.rank, sl)
    return sl

def album_caption(list_name: str, alb: Album, idx: int, total: int) -> str:
    parts = []
    parts.append(f"<b>{esc(list_name)}</b>  <code>{idx+1}/{total}</code>")
    parts.append(f"<b>#{alb.rank}</b>  {esc(alb.artist)} — <b>{esc(alb.album)}</b>")
    meta = []
    if alb.year and alb.year != "nan":
        meta.append(esc(alb.year))
    if alb.genre and alb.genre != "nan":
        meta.append(esc(alb.genre))
    if meta:
        parts.append(" / ".join(meta))
    parts.append("")
    parts.append("Кнопки снизу.")
    return "\n".join(parts)

async def build_album_keyboard(user_id: int, list_name: str, alb: Album, idx: int, total: int) -> InlineKeyboardMarkup:
    fav = await db_is_favorite(user_id, list_name, alb.rank)
    songlink = await db_get_songlink(list_name, alb.rank)  # may already exist
    b = InlineKeyboardBuilder()

    b.button(text="◀️", callback_data=f"nav:{LIST_TOKENS[list_name]}:{idx-1}")
    b.button(text="▶️", callback_data=f"nav:{LIST_TOKENS[list_name]}:{idx+1}")
    b.button(text=("❤️" if fav else "🤍"), callback_data=f"fav:{LIST_TOKENS[list_name]}:{alb.rank}")
    b.adjust(3)

    b.button(text="🔎 Поиск", callback_data=f"search:{LIST_TOKENS[list_name]}")
    b.button(text="📚 Списки", callback_data="lists")
    b.adjust(2)

    if Config.OPENAI_API_KEY:
        b.button(text="ℹ️ Об артисте", callback_data=f"ai:{LIST_TOKENS[list_name]}:{alb.rank}")
        b.adjust(1)

    # link button (URL buttons cannot be toggled with callback)
    sl = songlink or (alb.songlink if alb.songlink.startswith(("http://", "https://")) else "")
    if sl:
        b.row(InlineKeyboardButton(text="🔗 Слушать", url=sl))

    return b.as_markup()

# ---------------- sending / editing ----------------

async def send_or_edit_album(target: Message | CallbackQuery, list_name: str, idx: int) -> None:
    user_id = target.from_user.id if getattr(target, "from_user", None) else target.message.from_user.id  # type: ignore
    df = load_list_df(list_name)
    total = len(df)
    idx = max(0, min(idx, total - 1))
    await db_set_progress(user_id, list_name, idx)

    alb, total = get_album(list_name, idx)
    cover_url = await resolve_cover_url(list_name, alb)
    songlink = await resolve_songlink_url(list_name, alb)
    if songlink:
        alb.songlink = songlink

    cap = album_caption(list_name, alb, idx, total)
    kb = await build_album_keyboard(user_id, list_name, alb, idx, total)

    msg = target.message if isinstance(target, CallbackQuery) else target

    # Decide edit vs send new
    try:
        if cover_url:
            media = InputMediaPhoto(media=cover_url, caption=cap, parse_mode="HTML")
            if msg.photo:
                await msg.edit_media(media, reply_markup=kb)
            else:
                await msg.edit_text("Обновляю карточку…")
                await msg.answer_photo(photo=cover_url, caption=cap, reply_markup=kb, parse_mode="HTML")
        else:
            if msg.photo:
                # cannot edit photo -> switch to text by sending new
                await msg.answer(cap, reply_markup=kb, parse_mode="HTML")
            else:
                await msg.edit_text(cap, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        # fallback: send new
        if cover_url:
            await msg.answer_photo(photo=cover_url, caption=cap, reply_markup=kb, parse_mode="HTML")
        else:
            await msg.answer(cap, reply_markup=kb, parse_mode="HTML")

# ---------------- menus ----------------

def menu_keyboard(current_list: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📚 Списки", callback_data="lists")
    b.button(text="🔎 Поиск", callback_data=f"search:{LIST_TOKENS[current_list]}")
    b.button(text="❤️ Любимые", callback_data=f"favlist:{LIST_TOKENS[current_list]}:0")
    b.button(text="🎲 Рандом из ❤️", callback_data=f"favrand:{LIST_TOKENS[current_list]}")
    b.adjust(2)
    return b.as_markup()

def lists_keyboard(selected: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for name in LIST_FILES.keys():
        prefix = "✅ " if name == selected else ""
        b.button(text=f"{prefix}{name}", callback_data=f"list:{LIST_TOKENS[name]}")
    b.adjust(1)
    b.button(text="⬅️ Назад", callback_data="menu")
    return b.as_markup()

# ---------------- router ----------------

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    intro = list_intro_text(list_name)
    if intro:
        await message.answer(intro)
    await message.answer(
        f"Версия {BOT_VERSION}\nТекущий список: <b>{esc(list_name)}</b>",
        reply_markup=menu_keyboard(list_name),
        parse_mode="HTML",
    )

@router.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    await message.answer(
        f"Текущий список: <b>{esc(list_name)}</b>",
        reply_markup=menu_keyboard(list_name),
        parse_mode="HTML",
    )

@router.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery) -> None:
    list_name = await db_get_user_list(cb.from_user.id)
    await cb.message.edit_text(
        f"Текущий список: <b>{esc(list_name)}</b>",
        reply_markup=menu_keyboard(list_name),
        parse_mode="HTML",
    )
    await cb.answer()

@router.callback_query(F.data == "lists")
async def cb_lists(cb: CallbackQuery) -> None:
    list_name = await db_get_user_list(cb.from_user.id)
    await cb.message.edit_text(
        "Выбери список.",
        reply_markup=lists_keyboard(list_name),
        parse_mode="HTML",
    )
    await cb.answer()

@router.callback_query(F.data.startswith("list:"))
async def cb_select_list(cb: CallbackQuery) -> None:
    tok = cb.data.split(":", 1)[1]
    list_name = TOKEN_TO_LIST.get(tok)
    if not list_name:
        await cb.answer("Не нашёл список.")
        return
    await db_set_user_list(cb.from_user.id, list_name)
    intro = list_intro_text(list_name)
    if intro:
        await cb.message.answer(intro)
    await cb.message.edit_text(
        f"Текущий список: <b>{esc(list_name)}</b>",
        reply_markup=menu_keyboard(list_name),
        parse_mode="HTML",
    )
    await cb.answer()

@router.message(Command("open"))
async def cmd_open(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    idx = await db_get_progress(user_id, list_name)
    await send_or_edit_album(message, list_name, idx)

@router.message(Command("next"))
async def cmd_next(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    idx = await db_get_progress(user_id, list_name)
    await send_or_edit_album(message, list_name, idx + 1)

@router.message(Command("prev"))
async def cmd_prev(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    idx = await db_get_progress(user_id, list_name)
    await send_or_edit_album(message, list_name, idx - 1)

@router.message(Command("go"))
async def cmd_go(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    m = re.search(r"/go\s+(\d+)", message.text or "")
    if not m:
        await message.answer("Формат: /go 42")
        return
    rank = int(m.group(1))
    idx = find_index_by_rank(list_name, rank)
    if idx is None:
        await message.answer("Не нашёл такую позицию в списке.")
        return
    await send_or_edit_album(message, list_name, idx)

@router.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: CallbackQuery) -> None:
    try:
        _, tok, idx_s = cb.data.split(":", 2)
        list_name = TOKEN_TO_LIST.get(tok)
        if not list_name:
            await cb.answer("Список не найден.")
            return
        idx = int(idx_s)
    except Exception:
        await cb.answer("Ошибка навигации.")
        return
    await send_or_edit_album(cb, list_name, idx)
    await cb.answer()

@router.callback_query(F.data.startswith("fav:"))
async def cb_fav_toggle(cb: CallbackQuery) -> None:
    try:
        _, tok, rank_s = cb.data.split(":", 2)
        list_name = TOKEN_TO_LIST.get(tok)
        if not list_name:
            await cb.answer("Список не найден.")
            return
        rank = int(rank_s)
    except Exception:
        await cb.answer("Ошибка.")
        return

    new_state = await db_toggle_favorite(cb.from_user.id, list_name, rank)
    await cb.answer("Добавил ❤️" if new_state else "Убрал 🤍")

    # refresh current card keyboard if possible
    idx = find_index_by_rank(list_name, rank)
    if idx is not None:
        alb, total = get_album(list_name, idx)
        kb = await build_album_keyboard(cb.from_user.id, list_name, alb, idx, total)
        try:
            if cb.message.photo:
                await cb.message.edit_reply_markup(reply_markup=kb)
            else:
                await cb.message.edit_reply_markup(reply_markup=kb)
        except TelegramBadRequest:
            pass

@router.callback_query(F.data.startswith("favlist:"))
async def cb_fav_list(cb: CallbackQuery) -> None:
    try:
        _, tok, page_s = cb.data.split(":", 2)
        list_name = TOKEN_TO_LIST.get(tok)
        page = int(page_s)
    except Exception:
        await cb.answer("Ошибка.")
        return
    if not list_name:
        await cb.answer("Список не найден.")
        return

    per_page = 10
    offset = max(0, page) * per_page
    ranks = await db_list_favorites(cb.from_user.id, list_name, per_page, offset)
    total_cnt = await db_count_favorites(cb.from_user.id, list_name)

    if total_cnt == 0:
        await cb.message.edit_text(
            "Пока пусто.",
            reply_markup=menu_keyboard(list_name),
            parse_mode="HTML",
        )
        await cb.answer()
        return

    df = load_list_df(list_name)
    lines = [f"<b>Любимые</b> ({total_cnt})", ""]
    b = InlineKeyboardBuilder()
    for rank in ranks:
        idx = find_index_by_rank(list_name, rank)
        if idx is None:
            continue
        r = df.iloc[idx]
        artist = str(r["artist"])
        album = str(r["album"])
        lines.append(f"<b>#{rank}</b> {esc(artist)} — {esc(album)}")
        b.button(text=f"GO {rank}", callback_data=f"go:{tok}:{rank}")

    # paging
    nav = InlineKeyboardBuilder()
    max_page = max(0, (total_cnt - 1) // per_page)
    if page > 0:
        nav.button(text="◀️", callback_data=f"favlist:{tok}:{page-1}")
    if page < max_page:
        nav.button(text="▶️", callback_data=f"favlist:{tok}:{page+1}")
    nav.button(text="⬅️ Назад", callback_data="menu")
    nav.adjust(3)

    b.adjust(1)
    # merge keyboards
    markup = InlineKeyboardMarkup(inline_keyboard=b.as_markup().inline_keyboard + nav.as_markup().inline_keyboard)

    await cb.message.edit_text("\n".join(lines), reply_markup=markup, parse_mode="HTML")
    await cb.answer()

@router.callback_query(F.data.startswith("favrand:"))
async def cb_fav_rand(cb: CallbackQuery) -> None:
    tok = cb.data.split(":", 1)[1]
    list_name = TOKEN_TO_LIST.get(tok)
    if not list_name:
        await cb.answer("Список не найден.")
        return
    total = await db_count_favorites(cb.from_user.id, list_name)
    if total == 0:
        await cb.answer("Пока нет ❤️")
        return
    import random
    # pick random rank from last N (simple)
    ranks = await db_list_favorites(cb.from_user.id, list_name, min(200, total), 0)
    rank = random.choice(ranks)
    idx = find_index_by_rank(list_name, rank)
    if idx is None:
        await cb.answer("Не нашёл.")
        return
    await send_or_edit_album(cb, list_name, idx)
    await cb.answer()

@router.callback_query(F.data.startswith("go:"))
async def cb_go(cb: CallbackQuery) -> None:
    try:
        _, tok, rank_s = cb.data.split(":", 2)
        list_name = TOKEN_TO_LIST.get(tok)
        rank = int(rank_s)
    except Exception:
        await cb.answer("Ошибка.")
        return
    if not list_name:
        await cb.answer("Список не найден.")
        return
    idx = find_index_by_rank(list_name, rank)
    if idx is None:
        await cb.answer("Не нашёл.")
        return
    await send_or_edit_album(cb, list_name, idx)
    await cb.answer()

# --- search flow ---

@router.callback_query(F.data.startswith("search:"))
async def cb_search_prompt(cb: CallbackQuery) -> None:
    tok = cb.data.split(":", 1)[1]
    list_name = TOKEN_TO_LIST.get(tok)
    if not list_name:
        await cb.answer("Список не найден.")
        return
    await db_input_set(cb.from_user.id, "search_artist", {"list": list_name})
    await cb.message.answer("Напиши имя артиста для поиска.")
    await cb.answer()

@router.message(Command("find"))
async def cmd_find(message: Message) -> None:
    user_id = message.from_user.id
    list_name = await db_get_user_list(user_id)
    q = (message.text or "").split(maxsplit=1)
    if len(q) < 2:
        await message.answer("Формат: /find Radiohead")
        return
    query = q[1]
    await handle_artist_search(message, list_name, query)

@router.message(F.text)
async def on_text(message: Message) -> None:
    st = await db_input_get(message.from_user.id)
    if not st:
        return
    mode, payload = st
    if mode == "search_artist":
        list_name = payload.get("list") or await db_get_user_list(message.from_user.id)
        await db_input_clear(message.from_user.id)
        await handle_artist_search(message, list_name, message.text or "")

async def handle_artist_search(message: Message, list_name: str, query: str) -> None:
    res = search_artist(list_name, query, limit=25)
    if not res:
        await message.answer("Ничего не нашёл.")
        return
    b = InlineKeyboardBuilder()
    lines = [f"<b>Нашёл</b> ({len(res)})", ""]
    tok = LIST_TOKENS[list_name]
    for rank, artist, album in res[:25]:
        lines.append(f"<b>#{rank}</b> {esc(artist)} — {esc(album)}")
        b.button(text=f"GO {rank}", callback_data=f"go:{tok}:{rank}")
    b.adjust(1)
    b.button(text="⬅️ Меню", callback_data="menu")
    await message.answer("\n".join(lines), reply_markup=b.as_markup(), parse_mode="HTML")

# --- AI about artist ---

@router.callback_query(F.data.startswith("ai:"))
async def cb_ai_artist(cb: CallbackQuery) -> None:
    if not Config.OPENAI_API_KEY:
        await cb.answer("AI выключен.")
        return
    try:
        _, tok, rank_s = cb.data.split(":", 2)
        list_name = TOKEN_TO_LIST.get(tok)
        rank = int(rank_s)
    except Exception:
        await cb.answer("Ошибка.")
        return
    if not list_name:
        await cb.answer("Список не найден.")
        return
    idx = find_index_by_rank(list_name, rank)
    if idx is None:
        await cb.answer("Не нашёл.")
        return
    alb, _ = get_album(list_name, idx)
    await cb.answer("Думаю…")
    note = await ai_artist_note(alb.artist, cb.from_user.id)
    if not note:
        await cb.message.answer("Не получилось получить справку.")
        return
    await cb.message.answer(f"<b>{esc(alb.artist)}</b>\n\n{esc(note)}", parse_mode="HTML")

# --- admin commands ---

def _parse_admin_args(text_: str) -> Optional[Tuple[str, int, Optional[str]]]:
    # /set_cover [list] <rank> <url>
    parts = (text_ or "").split()
    if len(parts) < 3:
        return None
    cmd = parts[0]
    rest = parts[1:]
    list_name = ""
    rank = None
    url = None
    if len(rest) >= 3 and rest[0] in LIST_FILES:
        list_name = rest[0]
        rank = rest[1]
        url = rest[2]
    else:
        list_name = ""
        rank = rest[0]
        url = rest[1] if len(rest) >= 2 else None
    if not (rank and str(rank).isdigit()):
        return None
    return list_name, int(rank), url

async def _resolve_list_for_admin(user_id: int, maybe_list: str) -> str:
    if maybe_list and maybe_list in LIST_FILES:
        return maybe_list
    return await db_get_user_list(user_id)

@router.message(Command("set_cover"))
async def cmd_set_cover(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parsed = _parse_admin_args(message.text or "")
    if not parsed or not parsed[2]:
        await message.answer("Формат: /set_cover [list] 42 https://...")
        return
    maybe_list, rank, url = parsed
    list_name = await _resolve_list_for_admin(message.from_user.id, maybe_list)
    await db_set_cover(list_name, rank, url, source="manual")
    await message.answer("Ок.")

@router.message(Command("del_cover"))
async def cmd_del_cover(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parsed = _parse_admin_args(message.text or "")
    if not parsed:
        await message.answer("Формат: /del_cover [list] 42")
        return
    maybe_list, rank, _ = parsed
    list_name = await _resolve_list_for_admin(message.from_user.id, maybe_list)
    await db_del_cover(list_name, rank)
    await message.answer("Ок.")

@router.message(Command("set_songlink"))
async def cmd_set_songlink(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parsed = _parse_admin_args(message.text or "")
    if not parsed or not parsed[2]:
        await message.answer("Формат: /set_songlink [list] 42 https://...")
        return
    maybe_list, rank, url = parsed
    list_name = await _resolve_list_for_admin(message.from_user.id, maybe_list)
    await db_set_songlink(list_name, rank, url)
    await message.answer("Ок.")

@router.message(Command("del_songlink"))
async def cmd_del_songlink(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parsed = _parse_admin_args(message.text or "")
    if not parsed:
        await message.answer("Формат: /del_songlink [list] 42")
        return
    maybe_list, rank, _ = parsed
    list_name = await _resolve_list_for_admin(message.from_user.id, maybe_list)
    await db_del_songlink(list_name, rank)
    await message.answer("Ок.")

@router.message(Command("health"))
async def cmd_health(message: Message) -> None:
    try:
        scan_lists()
        _ = load_list_df(next(iter(LIST_FILES.keys())))
        await message.answer(f"OK. lists={len(LIST_FILES)} version={BOT_VERSION}")
    except Exception as e:
        await message.answer(f"FAIL: {e}")

# ---------------- app ----------------

async def on_startup(bot: Bot) -> None:
    global HTTP
    scan_lists()
    await db_init()
    HTTP = aiohttp.ClientSession()
    log.info("Started. version=%s lists=%s dir=%s", BOT_VERSION, len(LIST_FILES), Config.ALBUMS_DIR)

async def on_shutdown(bot: Bot) -> None:
    global HTTP
    try:
        if HTTP:
            await HTTP.close()
            HTTP = None
    finally:
        await db_close()
    log.info("Stopped.")

async def main() -> None:
    bot = Bot(token=Config.TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())
