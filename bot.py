import os
import re
import csv
import io
import asyncio
import json
import logging
import html

BOT_VERSION = os.getenv("BOT_VERSION", "v59-2026-02-10_084957-9cd7b7d9")
AI_CACHE_VERSION = 6  # bump to invalidate old AI cache
from typing import Optional, Dict, List
from urllib.parse import quote_plus, quote, unquote_plus
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import aiohttp
import asyncpg

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)
from aiogram.exceptions import TelegramBadRequest

# ================= LOGGING =================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("album_bot")

# ================= CONFIG =================

class Config:
    TOKEN = os.getenv("TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    DEFAULT_LIST = os.getenv("ALBUM_LIST", "top100")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ALBUMS_DIR = os.getenv("ALBUMS_DIR", os.path.join(BASE_DIR, "albums"))

    # MusicBrainz requires contact in User-Agent (email or site)
    MB_CONTACT = os.getenv("MB_CONTACT", "contact:not_set")
    MB_APP = os.getenv("MB_APP", "MusicAlbumClubBot/1.0")

    # Album of the day schedule
    DAILY_TZ = os.getenv("DAILY_TZ", "Europe/Berlin")
    DAILY_HOUR = int(os.getenv("DAILY_HOUR", "10"))
    DAILY_MINUTE = int(os.getenv("DAILY_MINUTE", "0"))

if not Config.TOKEN or not Config.DATABASE_URL:
    raise RuntimeError("ENV vars not set: TOKEN and/or DATABASE_URL")

if Config.DATABASE_URL.startswith("postgres://"):
    Config.DATABASE_URL = Config.DATABASE_URL.replace("postgres://", "postgresql://", 1)


# ================= AI (OpenAI) =================

AI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "").strip()

AI_MAX_DAILY_DEFAULT = 30
AI_CACHE_DAYS_DEFAULT = 30

def strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&quot;", '"').replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return s.strip()

def _ai_max_daily() -> int:
    try:
        return int(os.getenv("AI_MAX_DAILY", str(AI_MAX_DAILY_DEFAULT)))
    except Exception:
        return AI_MAX_DAILY_DEFAULT

def _ai_cache_days() -> int:
    try:
        return int(os.getenv("AI_CACHE_DAYS", str(AI_CACHE_DAYS_DEFAULT)))
    except Exception:
        return AI_CACHE_DAYS_DEFAULT

AI_MODE_LIMITS = {
    "artist": 650,
    "album": 900,
}


# ================= BOT =================

bot = Bot(token=Config.TOKEN)
dp = Dispatcher()
router = Router()

pg_pool: Optional[asyncpg.Pool] = None
http_session: Optional[aiohttp.ClientSession] = None
daily_task: Optional[asyncio.Task] = None

# ================= DATABASE =================

def _pool() -> asyncpg.Pool:
    if pg_pool is None:
        raise RuntimeError("Postgres pool is not initialized")
    return pg_pool

async def init_pg() -> None:
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=Config.DATABASE_URL, min_size=1, max_size=5)

    async with pg_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            album_list TEXT NOT NULL
        )
        """)

        
        # per-user pending input (simple state machine)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_inputs (
            user_id BIGINT PRIMARY KEY,
            mode TEXT NOT NULL,
            payload TEXT,
            updated_at TIMESTAMPTZ NOT NULL
        )
        """)

# progress per (user, list)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_progress (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            current_index INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (user_id, album_list)
        )
        """)

        # IMPORTANT: older DBs may already have updated_at NOT NULL without default.
        # Make it safe for inserts even if some code inserts without updated_at.
        try:
            await conn.execute("ALTER TABLE user_progress ALTER COLUMN updated_at SET DEFAULT NOW()")
        except Exception:
            pass
        try:
            await conn.execute("UPDATE user_progress SET updated_at=NOW() WHERE updated_at IS NULL")
        except Exception:
            pass

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            rated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ratings_user
        ON ratings (user_id, album_list, rated_at DESC)
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS covers (
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            cover_url TEXT NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (album_list, rank)
        )
        """)


        await conn.execute("""
        CREATE TABLE IF NOT EXISTS songlinks (
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            songlink_url TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (album_list, rank)
        )
        """)


        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_notes (
    album_list TEXT NOT NULL,
    rank INTEGER NOT NULL,
    mode TEXT NOT NULL,
    text TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (album_list, rank, mode)
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

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS album_facts (
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            facts_json TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (album_list, rank)
        )
        """)        # Ensure defaults for caches
        try:
            await conn.execute("ALTER TABLE songlinks ALTER COLUMN updated_at SET DEFAULT NOW()")
        except Exception:
            pass
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS relisten (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_relisten_user
        ON relisten (user_id, added_at DESC)
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


        # daily subscription
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_subscriptions (
            user_id BIGINT PRIMARY KEY,
            is_enabled BOOLEAN NOT NULL,
            album_list TEXT NOT NULL,
            send_hour INTEGER NOT NULL,
            send_minute INTEGER NOT NULL,
            tz TEXT NOT NULL,
            last_sent DATE
        )
        """)

# ================= LIST NAMES =================

def canonical_list_name(name: str) -> str:
    s = (name or "").strip()
    s = unquote_plus(s)
    s = re.sub(r"\s+", " ", s)
    return s

def encode_list_name(name: str) -> str:
    return quote(canonical_list_name(name), safe="")



def get_list_intro(list_name: str) -> Optional[str]:
    """
    Возвращает приветственную фразу для списка (если задана).
    Источники (по приоритету):
    1) Файл albums/<list_name>.intro.txt (можно добавлять без изменения кода)
    2) Словарь LIST_INTROS в коде (для базовых списков)
    """
    # 1) Intro-файл рядом со списком
    try:
        intro_path = os.path.join(Config.ALBUMS_DIR, f"{list_name}.intro.txt")
        if os.path.exists(intro_path):
            txt = Path(intro_path).read_text(encoding="utf-8").strip()
            return txt or None
    except Exception as e:
        log.debug("intro file read failed: %s", e)

    # 2) Встроенные интро (опционально)
    return LIST_INTROS.get(list_name)


# Можно задать тексты здесь для конкретных списков
# Или (лучше) положить рядом с xlsx файл: albums/<имя_списка>.intro.txt
LIST_INTROS: Dict[str, str] = {
    # "top100": "Тут можешь написать вводный текст для top100",
    # "top500 RS": "Rolling Stone Top 500. Тут можно написать правила/контекст.",
}
def list_file_names() -> List[str]:
    if not os.path.isdir(Config.ALBUMS_DIR):
        return []
    out = []
    for fn in os.listdir(Config.ALBUMS_DIR):
        if fn.lower().endswith(".xlsx"):
            out.append(fn[:-5])
    return sorted(out, key=lambda x: x.lower())

def resolve_list_name(name: str) -> Optional[str]:
    target = canonical_list_name(name).lower()
    for n in list_file_names():
        if canonical_list_name(n).lower() == target:
            return n
    return None

# ================= ALBUMS =================

album_cache: Dict[str, pd.DataFrame] = {}

def load_albums(list_name: str) -> pd.DataFrame:
    path = os.path.join(Config.ALBUMS_DIR, f"{list_name}.xlsx")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Album list file not found: {path}")

    df = pd.read_excel(path)
    required = {"rank", "artist", "album"}
    missing = required.difference(set(df.columns))
    if missing:
        raise ValueError(f"Missing columns in {path}: {', '.join(sorted(missing))}")

    if "genre" not in df.columns:
        df["genre"] = ""

    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df = df.dropna(subset=["rank"]).copy()
    df["rank"] = df["rank"].astype(int)

    return df.sort_values("rank").reset_index(drop=True)

def get_albums(list_name: str) -> pd.DataFrame:
    if list_name not in album_cache:
        album_cache[list_name] = load_albums(list_name)
    return album_cache[list_name]


def find_index_by_rank(list_name: str, rank: int) -> Optional[int]:
    albums = get_albums(list_name)
    try:
        idxs = albums.index[albums["rank"] == int(rank)].tolist()
        if not idxs:
            return None
        return int(idxs[0])
    except Exception:
        return None

# ================= USERS + PROGRESS =================

async def ensure_user(user_id: int) -> str:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow("SELECT album_list FROM users WHERE user_id=$1", user_id)
        if row:
            return row["album_list"]

        default_disk = resolve_list_name(Config.DEFAULT_LIST) or Config.DEFAULT_LIST
        await conn.execute("INSERT INTO users (user_id, album_list) VALUES ($1,$2)", user_id, default_disk)

        albums = get_albums(default_disk)
        idx = len(albums) - 1
        now = datetime.now(timezone.utc)
        await conn.execute(
            "INSERT INTO user_progress (user_id, album_list, current_index, updated_at) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list) DO NOTHING",
            user_id, default_disk, idx, now
        )
        return default_disk

async def get_selected_list(user_id: int) -> str:
    return await ensure_user(user_id)

async def set_selected_list(user_id: int, list_name: str) -> str:
    resolved = resolve_list_name(list_name)
    if not resolved:
        raise ValueError("unknown_list")
    async with _pool().acquire() as conn:
        await conn.execute("UPDATE users SET album_list=$1 WHERE user_id=$2", resolved, user_id)
        albums = get_albums(resolved)
        idx = len(albums) - 1
        now = datetime.now(timezone.utc)
        await conn.execute(
            "INSERT INTO user_progress (user_id, album_list, current_index, updated_at) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list) DO NOTHING",
            user_id, resolved, idx, now
        )
    return resolved



async def db_set_user_input(user_id: int, mode: str, payload: Optional[str] = None) -> None:
    async with _pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO user_inputs (user_id, mode, payload, updated_at)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (user_id)
               DO UPDATE SET mode=EXCLUDED.mode, payload=EXCLUDED.payload, updated_at=NOW()
            """,
            user_id, mode, payload,
        )

async def db_get_user_input(user_id: int) -> Optional[Dict]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, mode, payload, updated_at FROM user_inputs WHERE user_id=$1",
            user_id,
        )
        return dict(row) if row else None

async def db_clear_user_input(user_id: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute("DELETE FROM user_inputs WHERE user_id=$1", user_id)

async def get_index(user_id: int, list_name: str) -> int:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT current_index FROM user_progress WHERE user_id=$1 AND album_list=$2",
            user_id, list_name
        )
        if row:
            return int(row["current_index"])
        albums = get_albums(list_name)
        idx = len(albums) - 1
        now = datetime.now(timezone.utc)
        await conn.execute(
            "INSERT INTO user_progress (user_id, album_list, current_index, updated_at) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list) DO UPDATE SET current_index=EXCLUDED.current_index, updated_at=EXCLUDED.updated_at",
            user_id, list_name, idx, now
        )
        return idx

async def set_index(user_id: int, list_name: str, idx: int) -> None:
    now = datetime.now(timezone.utc)
    async with _pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO user_progress (user_id, album_list, current_index, updated_at) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list) DO UPDATE SET current_index=EXCLUDED.current_index, updated_at=EXCLUDED.updated_at",
            user_id, list_name, idx, now
        )

# ================= HTTP =================

async def init_http() -> None:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))

def _http() -> aiohttp.ClientSession:
    if http_session is None or http_session.closed:
        raise RuntimeError("HTTP session is not initialized")
    return http_session

def _mb_headers() -> Dict[str, str]:
    return {"User-Agent": f"{Config.MB_APP} ({Config.MB_CONTACT})"}

def _ms_to_mmss(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    try:
        s = int(ms) // 1000
        m = s // 60
        s = s % 60
        return f"{m}:{s:02d}"
    except Exception:
        return None

async def fetch_musicbrainz_facts(artist: str, album: str) -> dict:
    """Best-effort facts from MusicBrainz, without guessing."""
    facts: dict = {
        "artist": artist,
        "album": album,
        "source": "musicbrainz",
        "release_group_id": None,
        "first_release_date": None,
        "primary_type": None,
        "secondary_types": [],
        "tags": [],
        "label": None,
        "track_count": None,
        "tracks": [],
    }

    q = f'artist:"{artist}" AND releasegroup:"{album}"'
    try:
        async with _http().get(
            "https://musicbrainz.org/ws/2/release-group/",
            params={"query": q, "fmt": "json", "limit": 1, "inc": "tags"},
            headers=_mb_headers(),
            timeout=30,
        ) as r:
            data = await r.json(content_type=None)
            rgs = data.get("release-groups") or []
            if not rgs:
                return facts
            rg = rgs[0]
            facts["release_group_id"] = rg.get("id")
            facts["first_release_date"] = rg.get("first-release-date")
            facts["primary_type"] = rg.get("primary-type")
            facts["secondary_types"] = rg.get("secondary-types") or []
            tags = rg.get("tags") or []
            facts["tags"] = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")][:10]
    except Exception as e:
        log.debug("musicbrainz release-group facts failed: %s", e)
        return facts


async def fetch_wikipedia_summary(query: str) -> dict:
    """
    Returns {"title": str|None, "extract": str|None, "url": str|None}
    Best-effort: search -> page summary (ru, fallback en).
    """
    out = {"title": None, "extract": None, "url": None}
    q = (query or "").strip()
    if not q:
        return out
    headers = {"User-Agent": _mb_headers().get("User-Agent", "AlbumBot/1.0")}
    lang = "ru"

    async def _search(_lang: str) -> str | None:
        async with _http().get(
            f"https://{_lang}.wikipedia.org/w/api.php",
            params={"action": "query", "list": "search", "srsearch": q, "format": "json", "srlimit": 1},
            headers=headers,
            timeout=20,
        ) as r:
            data = await r.json(content_type=None)
            items = (((data or {}).get("query") or {}).get("search") or [])
            if not items:
                return None
            return items[0].get("title")

    try:
        title = await _search("ru")
        if not title:
            title = await _search("en")
            lang = "en"
        if not title:
            return out
        out["title"] = title
    except Exception:
        return out

async def fetch_lastfm_artist_info(artist: str) -> dict:
    if not LASTFM_API_KEY:
        return {}
    a = (artist or "").strip()
    if not a:
        return {}
    try:
        async with _http().get(
            "https://ws.audioscrobbler.com/2.0/",
            params={
                "method": "artist.getinfo",
                "artist": a,
                "api_key": LASTFM_API_KEY,
                "format": "json",
                "autocorrect": 1,
            },
            timeout=20,
        ) as r:
            data = await r.json(content_type=None)
            art = (data or {}).get("artist") or {}
            bio = (art.get("bio") or {})
            tags = art.get("tags", {}).get("tag") or []
            tag_names = []
            if isinstance(tags, list):
                tag_names = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")][:10]
            return {
                "name": art.get("name") or a,
                "bio": strip_html(bio.get("summary") or bio.get("content") or ""),
                "tags": tag_names,
            }
    except Exception as e:
        log.debug("lastfm artist fetch failed: %s", e)
        return {}

async def fetch_lastfm_album_info(artist: str, album: str) -> dict:
    if not LASTFM_API_KEY:
        return {}
    a = (artist or "").strip()
    b = (album or "").strip()
    if not a or not b:
        return {}
    try:
        async with _http().get(
            "https://ws.audioscrobbler.com/2.0/",
            params={
                "method": "album.getinfo",
                "artist": a,
                "album": b,
                "api_key": LASTFM_API_KEY,
                "format": "json",
                "autocorrect": 1,
            },
            timeout=20,
        ) as r:
            data = await r.json(content_type=None)
            alb = (data or {}).get("album") or {}
            wiki = alb.get("wiki") or {}
            tags = alb.get("tags", {}).get("tag") or []
            tag_names = []
            if isinstance(tags, list):
                tag_names = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")][:10]
            return {
                "name": alb.get("name") or b,
                "artist": (alb.get("artist") or a),
                "summary": strip_html(wiki.get("summary") or wiki.get("content") or ""),
                "tags": tag_names,
            }
    except Exception as e:
        log.debug("lastfm album fetch failed: %s", e)
        return {}

    try:
        rest = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{quote(out['title'])}"
        async with _http().get(rest, headers=headers, timeout=20) as r:
            data = await r.json(content_type=None)
            out["extract"] = (data or {}).get("extract")
            out["url"] = ((data or {}).get("content_urls") or {}).get("desktop", {}).get("page")
    except Exception:
        pass

    return out

def _ai_system_prompt_note() -> str:
    return (
        "Ты пишешь короткую справку для телеграм-бота о музыке. "
        "Используй ТОЛЬКО факты из входных данных. "
        "Ничего не выдумывай и не додумывай. "
        "Если во входных данных нет факта — пиши 'нет данных'. "
        "Не используй Markdown (никаких ###, **, списков с '•' тоже не надо). "
        "Пиши простыми строками. Без ссылок. Без дат релиза, лейблов и типов релиза. "
        "Старайся уложиться в 8–12 коротких строк."
    )

def _ai_user_prompt_artist(facts: dict, wiki: dict, lastfm: dict) -> str:
    wiki = wiki or {}
    lastfm = lastfm or {}
    facts = facts or {}
    return (
        "СТРОГО ПО-РУССКИ. Никакого английского.\n"
        "Сделай справку ОБ АРТИСТЕ.\n"
        "Пиши только факты из данных ниже. Не выдумывай.\n"
        "Формат 4–8 коротких строк, можно эмодзи.\n"
        "Ссылки не добавляй.\n\n"
        f"MusicBrainz facts: {json.dumps(facts, ensure_ascii=False)}\n"
        f"Wikipedia: {json.dumps(wiki, ensure_ascii=False)}\n"
        f"Last.fm: {json.dumps(lastfm, ensure_ascii=False)}\n"
    )

def _ai_user_prompt_album(facts: dict, wiki: dict, lastfm: dict) -> str:
    wiki = wiki or {}
    lastfm = lastfm or {}
    facts = facts or {}
    return (
        "СТРОГО ПО-РУССКИ. Никакого английского.\n"
        "Сделай справку ОБ АЛЬБОМЕ.\n"
        "Пиши только факты из данных ниже. Не выдумывай.\n"
        "Формат 4–8 коротких строк, можно эмодзи.\n"
        "Ссылки не добавляй.\n"
        "Добавь строку: 🎛 Треков: N (если N есть во входных данных).\n\n"
        f"MusicBrainz facts: {json.dumps(facts, ensure_ascii=False)}\n"
        f"Wikipedia: {json.dumps(wiki, ensure_ascii=False)}\n"
        f"Last.fm: {json.dumps(lastfm, ensure_ascii=False)}\n"
    )
def parse_ai_brief(text: str) -> dict:
    """Parse 4-line structured AI output.
    Preferred format:
      IDEA: ...
      SOUND: ...
      THEMES: ...
      FEATURE: ...
    Fallback:
      if markers missing, take first 4 non-empty lines as idea/sound/themes/feature.
    """
    out = {"idea": "нет данных", "sound": "нет данных", "themes": "нет данных", "feature": "нет данных"}
    if not text:
        return out

    found = 0
    for line in (text or "").splitlines():
        m = re.match(r"^\s*(IDEA|SOUND|THEMES|FEATURE)\s*:\s*(.*)\s*$", line, re.I)
        if not m:
            continue
        key = m.group(1).upper()
        val = (m.group(2) or "").strip() or "нет данных"
        if key == "IDEA":
            out["idea"] = val
        elif key == "SOUND":
            out["sound"] = val
        elif key == "THEMES":
            out["themes"] = val
        elif key == "FEATURE":
            out["feature"] = val
        found += 1

    if found == 0:
        # fallback: sequential lines
        lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
        if len(lines) >= 1:
            out["idea"] = lines[0]
        if len(lines) >= 2:
            out["sound"] = lines[1]
        if len(lines) >= 3:
            out["themes"] = lines[2]
        if len(lines) >= 4:
            out["feature"] = lines[3]
    return out


def render_ai_note(kind: str, info: dict, slim_facts: dict, ai_text: str) -> str:
    brief = parse_ai_brief(ai_text or "")
    track_count = (slim_facts or {}).get("track_count") if isinstance(slim_facts, dict) else None

    if kind == "album":
        body = (
            f"<b>💿 Об альбоме</b>\n"
            f"{html.escape(str(info.get('artist','')))} — {html.escape(str(info.get('album','')))}\n\n"
            f"Коротко:\n"
            f"• 🎭 <b>Идея</b> {html.escape(brief['idea'])}\n"
            f"• 🎧 <b>Звук</b> {html.escape(brief['sound'])}\n"
            f"• ✍️ <b>Темы</b> {html.escape(brief['themes'])}\n"
            f"• 🧠 <b>Фишка</b> {html.escape(brief['feature'])}\n"
        )
        if isinstance(track_count, int) and track_count > 0:
            body += f"\nТреков {track_count}"
        return body

    body = (
        f"<b>👤 Об артисте</b>\n"
        f"{html.escape(str(info.get('artist','')))}\n\n"
        f"Коротко:\n"
        f"• 🎭 <b>Кто это</b> {html.escape(brief['idea'])}\n"
        f"• 🎧 <b>Звук</b> {html.escape(brief['sound'])}\n"
        f"• ✍️ <b>Темы</b> {html.escape(brief['themes'])}\n"
        f"• 🧠 <b>Фишка</b> {html.escape(brief['feature'])}\n"
    )
    return body

def sanitize_ai_text(text: str) -> str:
    if not text:
        return "нет данных"
    t = text.strip()

    # remove markdown artifacts
    t = re.sub(r"^#{1,6}\s*", "", t, flags=re.MULTILINE)
    t = t.replace("**", "").replace("__", "").replace("`", "")

    # remove disallowed sections if model still outputs them
    disallowed_prefixes = (
        "Дата первого релиза",
        "Дата релиза",
        "Тип:",
        "Лейбл",
        "Ссылки",
        "Треклист",
    )
    lines = []
    for line in t.splitlines():
        s = line.strip()
        if not s:
            continue
        if any(s.startswith(p) for p in disallowed_prefixes):
            continue
        # also drop headings like "Факты:" if it only leads into removed content
        if s in ("Факты:", "Факты", "Ссылки:", "Ссылки", "Треклист:", "Треклист"):
            continue
        lines.append(s)

    # collapse to max ~14 lines
    lines = lines[:14]
    if not lines:
        return "нет данных"
    return "\n".join(lines)

async def openai_generate_note(kind: str, facts: dict, wiki: dict, lastfm: dict) -> Optional[str]:
    wiki = wiki or {}
    lastfm = lastfm or {}
    if not OPENAI_API_KEY:
        return None
    kind = (kind or "").strip().lower()
    if kind not in AI_MODE_LIMITS:
        return None

    user_prompt = _ai_user_prompt_artist(facts, wiki, lastfm) if kind == "artist" else _ai_user_prompt_album(facts, wiki, lastfm)
    payload = {
        "model": AI_MODEL,
        "input": [
            {"role": "system", "content": _ai_system_prompt_note()},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": AI_MODE_LIMITS[kind],
        "store": False,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    try:
        async with _http().post("https://api.openai.com/v1/responses", json=payload, headers=headers, timeout=60) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                log.warning("openai error %s: %s", r.status, str(data)[:500])
                return None
            text = _extract_response_text(data)
            return text.strip() if text else None
    except Exception as e:
        log.exception("openai request failed: %s", e)
        return None

    rg_id = facts.get("release_group_id")
    if not rg_id:
        return facts

    try:
        async with _http().get(
            "https://musicbrainz.org/ws/2/release/",
            params={"release-group": rg_id, "fmt": "json", "limit": 1, "inc": "recordings+labels"},
            headers=_mb_headers(),
            timeout=30,
        ) as r:
            data = await r.json(content_type=None)
            rels = data.get("releases") or []
            if not rels:
                return facts
            rel = rels[0]

            li = rel.get("label-info") or []
            if li and isinstance(li, list):
                lab = li[0].get("label") if isinstance(li[0], dict) else None
                if isinstance(lab, dict):
                    facts["label"] = lab.get("name")

            media = rel.get("media") or []
            tracks_out = []
            total_tracks = 0
            if media and isinstance(media, list):
                for m_ in media:
                    tr = m_.get("tracks") if isinstance(m_, dict) else None
                    if isinstance(tr, list):
                        for t in tr:
                            if not isinstance(t, dict):
                                continue
                            total_tracks += 1
                            if len(tracks_out) < 10:
                                tracks_out.append({
                                    "title": t.get("title"),
                                    "length": _ms_to_mmss(t.get("length")),
                                })
            facts["track_count"] = total_tracks if total_tracks else None
            facts["tracks"] = tracks_out
    except Exception as e:
        log.debug("musicbrainz release facts failed: %s", e)

    return facts


async def fetch_image_bytes(url: str) -> tuple[Optional[bytes], Optional[str]]:
    try:
        async with _http().get(url, allow_redirects=True) as r:
            if r.status != 200:
                return None, None
            ctype = (r.headers.get("Content-Type") or "").lower()
            if not ctype.startswith("image/"):
                return None, None
            data = await r.read()
            if not data or len(data) > 9_500_000:
                return None, None
            ext = "jpg"
            if "png" in ctype:
                ext = "png"
            elif "webp" in ctype:
                ext = "webp"
            return data, ext
    except Exception as e:
        log.debug("fetch_image_bytes failed: %s", e)
        return None, None

# ================= COVER CACHE + SOURCES =================

async def get_cached_cover(album_list: str, rank: int) -> Optional[str]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cover_url FROM covers WHERE album_list=$1 AND rank=$2",
            album_list, rank
        )
        return row["cover_url"] if row else None

async def set_cached_cover(album_list: str, rank: int, cover_url: str, source: str) -> None:
    now = datetime.now(timezone.utc)
    async with _pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO covers (album_list, rank, cover_url, source, updated_at)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (album_list, rank)
            DO UPDATE SET cover_url=EXCLUDED.cover_url, source=EXCLUDED.source, updated_at=EXCLUDED.updated_at
            """,
            album_list, rank, cover_url, source, now
        )

async def delete_cached_cover(album_list: str, rank: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute("DELETE FROM covers WHERE album_list=$1 AND rank=$2", album_list, rank)


async def get_cached_songlink(album_list: str, rank: int) -> Optional[str]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT songlink_url FROM songlinks WHERE album_list=$1 AND rank=$2",
            album_list, rank
        )
        return row["songlink_url"] if row else None

async def set_cached_songlink(album_list: str, rank: int, songlink_url: str) -> None:
    now = datetime.now(timezone.utc)
    async with _pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO songlinks (album_list, rank, songlink_url, updated_at) "
            "VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (album_list, rank) "
            "DO UPDATE SET songlink_url=EXCLUDED.songlink_url, updated_at=EXCLUDED.updated_at",
            album_list, rank, songlink_url, now
        )

async def delete_cached_songlink(album_list: str, rank: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute("DELETE FROM songlinks WHERE album_list=$1 AND rank=$2", album_list, rank)


async def get_cached_ai_note(album_list: str, rank: int, mode: str) -> Optional[str]:
    """Return cached AI text if fresh enough."""
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT text, updated_at FROM ai_notes WHERE album_list=$1 AND rank=$2 AND mode=$3",
            album_list, rank, mode
        )
        if not row:
            return None
        updated_at = row["updated_at"]
        if not updated_at:
            return None
        max_age_days = _ai_cache_days()
        if max_age_days > 0:
            age = datetime.now(timezone.utc) - updated_at
            if age > timedelta(days=max_age_days):
                return None
        return row["text"]

async def set_cached_ai_note(album_list: str, rank: int, mode: str, text: str) -> None:
    now = datetime.now(timezone.utc)
    async with _pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO ai_notes (album_list, rank, mode, text, updated_at) "
            "VALUES ($1,$2,$3,$4,$5) "
            "ON CONFLICT (album_list, rank, mode) "
            "DO UPDATE SET text=EXCLUDED.text, updated_at=EXCLUDED.updated_at",
            album_list, rank, mode, text, now
        )

async def _ai_usage_today_key() -> date:
    return datetime.now(timezone.utc).date()

async def get_ai_usage_today(user_id: int) -> int:
    d = await _ai_usage_today_key()
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cnt FROM ai_usage WHERE user_id=$1 AND day=$2",
            int(user_id), d
        )
        return int(row["cnt"]) if row else 0

async def inc_ai_usage_today(user_id: int) -> int:
    d = await _ai_usage_today_key()
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO ai_usage (user_id, day, cnt) VALUES ($1,$2,1) "
            "ON CONFLICT (user_id, day) DO UPDATE SET cnt=ai_usage.cnt+1 "
            "RETURNING cnt",
            int(user_id), d
        )
        return int(row["cnt"]) if row else 1


async def get_cached_album_facts(album_list: str, rank: int) -> Optional[dict]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT facts_json, updated_at FROM album_facts WHERE album_list=$1 AND rank=$2",
            album_list, rank
        )
        if not row:
            return None
        try:
            facts = json.loads(row["facts_json"])
        except Exception:
            return None
        max_age_days = _ai_cache_days()
        if max_age_days > 0 and row["updated_at"]:
            age = datetime.now(timezone.utc) - row["updated_at"]
            if age > timedelta(days=max_age_days):
                return None
        return facts if isinstance(facts, dict) else None

async def set_cached_album_facts(album_list: str, rank: int, facts: dict) -> None:
    now = datetime.now(timezone.utc)
    async with _pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO album_facts (album_list, rank, facts_json, updated_at) "
            "VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (album_list, rank) "
            "DO UPDATE SET facts_json=EXCLUDED.facts_json, updated_at=EXCLUDED.updated_at",
            album_list, rank, json.dumps(facts, ensure_ascii=False), now
        )

async def cover_from_itunes(artist: str, album: str) -> Optional[str]:
    try:
        async with _http().get(
            "https://itunes.apple.com/search",
            params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
        ) as r:
            data = await r.json(content_type=None)
            if data.get("resultCount"):
                return data["results"][0]["artworkUrl100"].replace("100x100", "600x600")
    except Exception as e:
        log.debug("itunes cover failed: %s", e)
    return None


async def itunes_album_url(artist: str, album: str) -> Optional[str]:
    """Return iTunes/Apple Music album URL (collectionViewUrl)."""
    try:
        async with _http().get(
            "https://itunes.apple.com/search",
            params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
        ) as r:
            data = await r.json(content_type=None)
            if data.get("resultCount"):
                return data["results"][0].get("collectionViewUrl")
    except Exception as e:
        log.debug("itunes album url failed: %s", e)
    return None

async def songlink_page_url_from_any(url: str) -> Optional[str]:
    """Get universal song.link pageUrl for a given platform URL."""
    try:
        async with _http().get(
            "https://api.song.link/v1-alpha.1/links",
            params={"url": url},
        ) as r:
            if r.status != 200:
                return None
            data = await r.json(content_type=None)
            page = data.get("pageUrl")
            if isinstance(page, str) and page.startswith("http"):
                return page
    except Exception as e:
        log.debug("song.link api failed: %s", e)
    return None

async def get_songlink_url(album_list: str, rank: int, artist: str, album: str) -> Optional[str]:
    cached = await get_cached_songlink(album_list, rank)
    if cached:
        return cached


def _extract_response_text(data: dict) -> str:
    if isinstance(data, dict):
        ot = data.get("output_text")
        if isinstance(ot, str) and ot.strip():
            return ot.strip()
        out = data.get("output")
        if isinstance(out, list):
            parts = []
            for item in out:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "message":
                    continue
                content = item.get("content")
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict) and c.get("type") == "output_text":
                            t = c.get("text")
                            if isinstance(t, str):
                                parts.append(t)
            text = "".join(parts).strip()
            if text:
                return sanitize_ai_text(text)
    return ""


def _ai_system_prompt(mode: str) -> str:
    return (
        "Ты помощник в телеграм-боте. "
        "Твоя задача — ТОЛЬКО аккуратно отформатировать факты, которые пришли во входных данных. "
        "Никаких домыслов, оценочных слов и 'описаний'. "
        "Если данных нет — пиши 'нет данных'. "
        "Пиши по-русски. "
        "Не добавляй лишние поля."
    )



def _ai_user_prompt(mode: str, facts: dict) -> str:
    facts_json = json.dumps(facts, ensure_ascii=False)
    if mode == "short":
        return (
            "Собери фактовую карточку альбома ТОЛЬКО по данным ниже. "
            "Если поле отсутствует или пустое — пиши 'нет данных'. "
            "Запрещено добавлять любые факты, которых нет во входе. "
            "Формат:\n"
            "Артист:\n"
            "Альбом:\n"
            "Дата первого релиза:\n"
            "Тип релиза:\n"
            "Лейбл:\n"
            "Теги/жанры:\n"
            "Треков:\n"
            "Ссылки:\n\n"
            f"ДАННЫЕ (JSON):\n{facts_json}"
        )
    return (
        "Собери расширенную фактовую карточку альбома ТОЛЬКО по данным ниже. "
        "Если поле отсутствует или пустое — пиши 'нет данных'. "
        "Запрещено добавлять любые факты, которых нет во входе. "
        "Формат:\n"
        "Артист:\n"
        "Альбом:\n"
        "Дата первого релиза:\n"
        "Тип релиза:\n"
        "Лейбл:\n"
        "Теги/жанры:\n"
        "Треков:\n"
        "Треклист (первые 10):\n"
        "Ссылки:\n\n"
        f"ДАННЫЕ (JSON):\n{facts_json}"
    )

async def openai_generate_album_note(mode: str, facts: dict) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    mode = mode.strip().lower()
    if mode not in AI_MODE_LIMITS:
        return None
    max_out = AI_MODE_LIMITS[mode]
    payload = {
        "model": AI_MODEL,
        "input": [
            {"role": "system", "content": _ai_system_prompt(mode)},
            {"role": "user", "content": _ai_user_prompt(mode, facts)},
        ],
        "max_output_tokens": max_out,
        "store": False,
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        async with _http().post(
            "https://api.openai.com/v1/responses",
            json=payload,
            headers=headers,
            timeout=60,
        ) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                log.warning("openai error %s: %s", r.status, str(data)[:500])
                return None
            text = _extract_response_text(data)
            return text.strip() if text else None
    except Exception as e:
        log.exception("openai request failed: %s", e)
        return None

    it_url = await itunes_album_url(artist, album)
    if not it_url:
        return None

    page = await songlink_page_url_from_any(it_url)
    if page:
        await set_cached_songlink(album_list, rank, page)
        return page
    return None

async def cover_from_deezer(artist: str, album: str) -> Optional[str]:
    try:
        q = quote_plus(f'artist:"{artist}" album:"{album}"')
        async with _http().get(f"https://api.deezer.com/search/album?q={q}") as r:
            data = await r.json(content_type=None)
            items = data.get("data") or []
            if not items:
                return None
            item = items[0]
            return item.get("cover_xl") or item.get("cover_big") or item.get("cover_medium") or item.get("cover")
    except Exception as e:
        log.debug("deezer cover failed: %s", e)
    return None

async def cover_from_musicbrainz_caa(artist: str, album: str) -> Optional[str]:
    try:
        q = quote_plus(f'release:"{album}" AND artist:"{artist}"')
        mb_url = f"https://musicbrainz.org/ws/2/release/?query={q}&fmt=json&limit=1"
        async with _http().get(mb_url, headers=_mb_headers()) as r:
            data = await r.json(content_type=None)
            rels = data.get("releases") or []
            if not rels:
                return None
            mbid = rels[0].get("id")
            if not mbid:
                return None

        caa_url = f"https://coverartarchive.org/release/{mbid}"
        async with _http().get(caa_url) as r:
            if r.status != 200:
                return None
            data = await r.json(content_type=None)
            imgs = data.get("images") or []
            if not imgs:
                return None
            front = next((i for i in imgs if i.get("front")), imgs[0])
            return front.get("image")
    except Exception as e:
        log.debug("musicbrainz/caa cover failed: %s", e)
    return None

async def get_cover_with_fallback(album_list: str, rank: int, artist: str, album: str) -> Optional[str]:
    cached = await get_cached_cover(album_list, rank)
    if cached:
        return cached

    url = await cover_from_itunes(artist, album)
    if url:
        await set_cached_cover(album_list, rank, url, "itunes")
        return url

    url = await cover_from_deezer(artist, album)
    if url:
        await set_cached_cover(album_list, rank, url, "deezer")
        return url

    url = await cover_from_musicbrainz_caa(artist, album)
    if url:
        await set_cached_cover(album_list, rank, url, "musicbrainz_caa")
        return url

    return None

# ================= RATINGS =================

async def get_user_rating(user_id: int, album_list: str, rank: int) -> Optional[int]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT rating FROM ratings WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        return int(row["rating"]) if row else None

async def upsert_rating(user_id: int, album_list: str, rank: int, rating: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ratings (user_id, album_list, rank, rating, rated_at)
            VALUES ($1,$2,$3,$4, NOW())
            ON CONFLICT (user_id, album_list, rank)
            DO UPDATE SET rating=EXCLUDED.rating, rated_at=NOW()
            """,
            user_id, album_list, rank, rating
        )


# ================= RELISTEN ("Переслушаю") =================

async def is_relisten(user_id: int, album_list: str, rank: int) -> bool:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM relisten WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        return row is not None

async def is_favorite(user_id: int, album_list: str, rank: int) -> bool:
    if not pg_pool:
        return False
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        return row is not None

async def toggle_favorite(user_id: int, album_list: str, rank: int) -> bool:
    """Returns new state: True if now favorited."""
    if not pg_pool:
        return False
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        if row:
            await conn.execute(
                "DELETE FROM favorites WHERE user_id=$1 AND album_list=$2 AND rank=$3",
                user_id, album_list, rank
            )
            return False
        await conn.execute(
            "INSERT INTO favorites (user_id, album_list, rank, added_at) VALUES ($1,$2,$3,NOW()) ON CONFLICT DO NOTHING",
            user_id, album_list, rank
        )
        return True

async def list_favorites(user_id: int, limit: int = 50) -> list[tuple[str,int]]:
    if not pg_pool:
        return []
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT album_list, rank FROM favorites WHERE user_id=$1 ORDER BY added_at DESC LIMIT $2",
            user_id, limit
        )
        return [(r["album_list"], int(r["rank"])) for r in rows]

async def random_favorite(user_id: int) -> Optional[tuple[str,int]]:
    if not pg_pool:
        return None
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, rank FROM favorites WHERE user_id=$1 ORDER BY random() LIMIT 1",
            user_id
        )
        if not row:
            return None
        return (row["album_list"], int(row["rank"]))

async def toggle_relisten(user_id: int, album_list: str, rank: int) -> bool:
    """Toggle relisten state. Returns True if now enabled."""
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM relisten WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        if row:
            await conn.execute(
                "DELETE FROM relisten WHERE user_id=$1 AND album_list=$2 AND rank=$3",
                user_id, album_list, rank
            )
            return False
        await conn.execute(
            "INSERT INTO relisten (user_id, album_list, rank) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
            user_id, album_list, rank
        )
        return True

async def get_relisten_items(user_id: int, limit: int = 200) -> List[asyncpg.Record]:
    async with _pool().acquire() as conn:
        return await conn.fetch(
            "SELECT album_list, rank, added_at FROM relisten WHERE user_id=$1 ORDER BY added_at DESC LIMIT $2",
            user_id, limit
        )
async def send_relisten_list(user_id: int, limit: int = 80) -> None:
    rows = await get_relisten_items(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "🔁 Список «на переслушать» пуст.")
        return

    lines = ["🔁 <b>На переслушать</b> (последние добавленные)\n"]
    for i, r in enumerate(rows, 1):
        lst = r["album_list"]
        rank = int(r["rank"])
        df = get_albums(lst).set_index("rank")
        if rank in df.index:
            artist = str(df.loc[rank]["artist"])
            album = str(df.loc[rank]["album"])
            lines.append(f"{i}. {lst} #{rank} — {artist} — {album}")
        else:
            lines.append(f"{i}. {lst} #{rank}")

    text = "\n".join(lines)
    if len(text) > 3900:
        text = text[:3900] + "\n…"
    await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=relisten_keyboard())

async def send_random_relisten(user_id: int) -> None:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, rank FROM relisten WHERE user_id=$1 ORDER BY random() LIMIT 1",
            user_id
        )
    if not row:
        await bot.send_message(user_id, "🔁 Список «на переслушать» пуст.", reply_markup=relisten_keyboard())
        return

    lst = row["album_list"]
    rank = int(row["rank"])
    albums = get_albums(lst).reset_index(drop=True)
    # find index by rank
    try:
        idx = int(albums.index[albums["rank"] == rank][0])
    except Exception:
        await bot.send_message(user_id, "Не смог найти этот альбом в файле списка. Возможно файл поменялся.")
        return
    await send_album_post(user_id, lst, idx, ctx="relisten", prefix="🎲 <b>Случайный из «Переслушаю»</b>")



# ================= UI =================

def google_link(artist: str, album: str) -> str:
    return f"https://www.google.com/search?q={quote_plus(f'{artist} {album}')}"

def album_caption(rank: int, artist: str, album: str, genre: str, user_rating: Optional[int], *, in_relisten: bool = False, prefix: str = "") -> str:
    rating_line = f"\n\n⭐ <b>Ваша оценка:</b> {user_rating}/5" if user_rating else ""
    relisten_line = "\n🔁 <b>Переслушаю:</b> да" if in_relisten else ""
    header = (prefix + "\n\n") if prefix else ""
    return (
        header +
        f"🏆 <b>#{rank}</b>\n"
        f"🎤 <b>{artist}</b>\n"
        f"💿 <b>{album}</b>\n"
        f"🎧 {genre}"
        f"{rating_line}"
        f"{relisten_line}"
    )

def album_keyboard(album_list: str, rank: int, artist: str, album: str, rated: Optional[int], ctx: str, listen_url: Optional[str], *, in_relisten: bool = False, is_fav: bool = False) -> InlineKeyboardMarkup:
    rate_text = "⭐ Оценить" if not rated else f"⭐ Оценено: {rated}"
    rel_text = "🔁 Переслушаю ✅" if in_relisten else "🔁 Переслушаю"
    fav_text = "❤️ Любимое ✅" if is_fav else "❤️ Любимое"
    enc = encode_list_name(album_list)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Слушать", url=(listen_url or google_link(artist, album)))],
            [InlineKeyboardButton(text=fav_text, callback_data=f"fav:toggle:{album_list}:{rank}"), InlineKeyboardButton(text="👤 Об артисте", callback_data=f"ai:artist:{album_list}:{rank}"), InlineKeyboardButton(text="💿 Об альбоме", callback_data=f"ai:album:{album_list}:{rank}")],
        [
            InlineKeyboardButton(text="Предыдущий", callback_data="nav:prev"),
            InlineKeyboardButton(text="Следующий", callback_data="nav:next"),
        ],
        [
            InlineKeyboardButton(text=rate_text, callback_data=f"ui:rate:{enc}:{rank}:{ctx}"),
            InlineKeyboardButton(text=rel_text, callback_data=f"ui:relisten:{enc}:{rank}:{ctx}"),
        ],
        [
            InlineKeyboardButton(text="📋 Меню", callback_data="ui:menu"),
        ],
    ])

def rating_keyboard(album_list: str, rank: int, ctx: str) -> InlineKeyboardMarkup:
    enc = encode_list_name(album_list)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"rate:{i}:{enc}:{rank}:{ctx}") for i in range(1, 6)],
        [InlineKeyboardButton(text="⬅️ Назад к посту", callback_data=f"ui:back:{enc}:{rank}:{ctx}")],
    ])

def menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить", callback_data="nav:next")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="ui:stats")],
        [InlineKeyboardButton(text="📈 Статистика+", callback_data="ui:stats_plus")],
        [InlineKeyboardButton(text="🔁 На переслушать", callback_data="ui:relisten_menu")],
        [InlineKeyboardButton(text="🔎 Поиск артиста", callback_data="ui:find_artist")],
        [InlineKeyboardButton(text="📚 Списки", callback_data="ui:lists")],
    ])
def stats_plus_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Топ-10", callback_data="ui:top")],
        [InlineKeyboardButton(text="🧨 Анти-топ-10", callback_data="ui:bottom")],
        [InlineKeyboardButton(text="🔥 Стрик", callback_data="ui:streak")],
        [InlineKeyboardButton(text="🧠 Инсайты", callback_data="ui:insights")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="ui:menu")],
    ])

def relisten_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Случайный", callback_data="ui:relisten_random")],
        [InlineKeyboardButton(text="📃 Показать список", callback_data="ui:relisten_list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="ui:menu")],
    ])

def lists_keyboard() -> InlineKeyboardMarkup:
    items = list_file_names()
    rows = []
    for name in items[:60]:
        rows.append([InlineKeyboardButton(text=name, callback_data=f"setlist:{encode_list_name(name)}")])
    if not rows:
        rows = [[InlineKeyboardButton(text="Нет списков", callback_data="noop")]]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="ui:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================= CORE =================

async def render_album(user_id: int, album_list: str, idx: int, ctx: str, prefix: str = ""):
    albums = get_albums(album_list)
    if idx < 0 or idx >= len(albums):
        return None, "📭 Альбомы закончились", InlineKeyboardMarkup(inline_keyboard=[]), -1, None, "", "", ""

    row = albums.iloc[idx]
    rank = int(row["rank"])
    artist = str(row["artist"])
    album = str(row["album"])
    genre = str(row.get("genre", "") or "")

    user_rating = await get_user_rating(user_id, album_list, rank)
    cover = await get_cover_with_fallback(album_list, rank, artist, album)
    in_rel = await is_relisten(user_id, album_list, rank)
    caption = album_caption(rank, artist, album, genre, user_rating, in_relisten=in_rel, prefix=prefix)
    listen_url = await get_songlink_url(album_list, rank, artist, album)
    is_fav = await is_favorite(user_id, album_list, rank)
    kb = album_keyboard(album_list, rank, artist, album, user_rating, ctx, listen_url, in_relisten=in_rel, is_fav=is_fav)
    return cover, caption, kb, rank, user_rating, artist, album, genre

async def send_album_post(user_id: int, album_list: str, idx: int, ctx: str = "flow", prefix: str = "") -> None:
    cover, caption, kb, rank, _, _, _, _ = await render_album(user_id, album_list, idx, ctx=ctx, prefix=prefix)
    if caption.startswith("📭"):
        await bot.send_message(user_id, caption)
        return

    if cover:
        try:
            await bot.send_photo(user_id, cover, caption=caption, parse_mode="HTML", reply_markup=kb)
            return
        except TelegramBadRequest as e:
            log.warning("Telegram cannot fetch cover URL (list=%s rank=%s): %s", album_list, rank, e)

        data, ext = await fetch_image_bytes(cover)
        if data:
            try:
                photo = BufferedInputFile(data, filename=f"cover.{ext or 'jpg'}")
                await bot.send_photo(user_id, photo, caption=caption, parse_mode="HTML", reply_markup=kb)
                return
            except TelegramBadRequest as e:
                log.warning("Telegram cannot send downloaded cover (list=%s rank=%s): %s", album_list, rank, e)

        await delete_cached_cover(album_list, rank)

    await bot.send_message(user_id, caption, parse_mode="HTML", reply_markup=kb)

async def edit_album_post(call: CallbackQuery, album_list: str, rank: int, ctx: str, prefix: str = "") -> None:
    albums = get_albums(album_list)
    rows = albums.loc[albums["rank"] == rank]
    if rows.empty:
        return
    row = rows.iloc[0]
    artist = str(row["artist"])
    album = str(row["album"])
    genre = str(row.get("genre", "") or "")
    user_rating = await get_user_rating(call.from_user.id, album_list, rank)
    in_rel = await is_relisten(call.from_user.id, album_list, rank)
    caption = album_caption(rank, artist, album, genre, user_rating, in_relisten=in_rel, prefix=prefix)
    listen_url = await get_songlink_url(album_list, rank, artist, album)
    kb = album_keyboard(album_list, rank, artist, album, user_rating, ctx, listen_url, in_relisten=in_rel)

    try:
        if call.message.photo:
            await call.message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await call.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        log.debug("edit album post failed: %s", e)

# ================= STATS / EXPORT =================

async def build_stats_text(user_id: int) -> str:
    album_list = await get_selected_list(user_id)
    total = len(get_albums(album_list))
    tz = ZoneInfo(Config.DAILY_TZ)

    async with _pool().acquire() as conn:
        dist_rows = await conn.fetch(
            "SELECT rating, COUNT(*) AS c FROM ratings WHERE user_id=$1 AND album_list=$2 GROUP BY rating ORDER BY rating",
            user_id, album_list
        )
        rated_count = await conn.fetchval(
            "SELECT COUNT(*) FROM ratings WHERE user_id=$1 AND album_list=$2",
            user_id, album_list
        )
        avg = await conn.fetchval(
            "SELECT AVG(rating) FROM ratings WHERE user_id=$1 AND album_list=$2",
            user_id, album_list
        )
        median = await conn.fetchval(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rating) FROM ratings WHERE user_id=$1 AND album_list=$2",
            user_id, album_list
        )

        since_7 = datetime.now(tz).astimezone(timezone.utc) - timedelta(days=7)
        last7 = await conn.fetchval(
            "SELECT COUNT(*) FROM ratings WHERE user_id=$1 AND rated_at >= $2",
            user_id, since_7
        )

        # streak: consecutive days with at least one rating (any list)
        days_rows = await conn.fetch(
            "SELECT DISTINCT (rated_at AT TIME ZONE $1)::date AS d FROM ratings WHERE user_id=$2 ORDER BY d DESC",
            Config.DAILY_TZ, user_id
        )

    dist = {int(r["rating"]): int(r["c"]) for r in dist_rows}
    lines = [f"{i}: {dist.get(i, 0)}" for i in range(1, 6)]

    avg_txt = f"{float(avg):.2f}" if avg is not None else "—"
    med_txt = f"{float(median):.1f}" if median is not None else "—"

    low = dist.get(1, 0) + dist.get(2, 0)
    high = dist.get(5, 0)
    strictness = "—"
    if rated_count and rated_count > 0:
        strictness = f"{(low / rated_count) * 100:.0f}% 1–2, {(high / rated_count) * 100:.0f}% 5"

    # compute streak
    streak = 0
    if days_rows:
        today = datetime.now(tz).date()
        expected = today
        for r in days_rows:
            d = r["d"]
            if d == expected:
                streak += 1
                expected = expected - timedelta(days=1)
            else:
                break

    return (
        f"📊 <b>Статистика</b>\n\n"
        f"📃 Список: <b>{album_list}</b>\n"
        f"✅ Оценено: <b>{rated_count}</b> из <b>{total}</b>\n"
        f"⭐ Средняя: <b>{avg_txt}</b>\n"
        f"🟰 Медиана: <b>{med_txt}</b>\n"
        f"😈 Строгость: <b>{strictness}</b>\n"
        f"📅 За 7 дней: <b>{last7}</b> оценок\n"
        f"🔥 Стрик: <b>{streak}</b> дней\n\n"
        f"Распределение оценок:\n" + "\n".join(lines)
    )

async def format_top_bottom(user_id: int, album_list: str, *, top: bool, limit: int = 10) -> str:
    """Top/bottom within selected list."""
    df = get_albums(album_list).set_index("rank")
    async with _pool().acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT rank, rating, rated_at
            FROM ratings
            WHERE user_id=$1 AND album_list=$2
            ORDER BY rating {'DESC' if top else 'ASC'}, rated_at DESC
            LIMIT $3
            """,
            user_id, album_list, limit
        )
    if not rows:
        return "Пока нет оценок в этом списке."
    title = "🏆 <b>Топ</b>" if top else "🧨 <b>Анти-топ</b>"
    lines = [f"{title} по списку <b>{album_list}</b>\n"]
    for i, r in enumerate(rows, 1):
        rank = int(r["rank"])
        rating = int(r["rating"])
        if rank in df.index:
            artist = str(df.loc[rank]["artist"])
            album = str(df.loc[rank]["album"])
            lines.append(f"{i}. #{rank} — {artist} — {album} — <b>{rating}/5</b>")
        else:
            lines.append(f"{i}. #{rank} — <b>{rating}/5</b>")
    return "\n".join(lines)

async def streak_text(user_id: int) -> str:
    tz = ZoneInfo(Config.DAILY_TZ)
    async with _pool().acquire() as conn:
        days_rows = await conn.fetch(
            "SELECT DISTINCT (rated_at AT TIME ZONE $1)::date AS d FROM ratings WHERE user_id=$2 ORDER BY d DESC",
            Config.DAILY_TZ, user_id
        )
        since_30 = datetime.now(tz).astimezone(timezone.utc) - timedelta(days=30)
        last30 = await conn.fetchval(
            "SELECT COUNT(*) FROM ratings WHERE user_id=$1 AND rated_at >= $2",
            user_id, since_30
        )
    streak = 0
    if days_rows:
        expected = datetime.now(tz).date()
        for r in days_rows:
            if r["d"] == expected:
                streak += 1
                expected = expected - timedelta(days=1)
            else:
                break
    return (
        f"🔥 <b>Стрик:</b> {streak} дней\n"
        f"📅 <b>Оценок за 30 дней:</b> {last30}"
    )

async def insights_text(user_id: int) -> str:
    album_list = await get_selected_list(user_id)
    async with _pool().acquire() as conn:
        last10 = await conn.fetch(
            "SELECT rating FROM ratings WHERE user_id=$1 ORDER BY rated_at DESC LIMIT 10",
            user_id
        )
        avg_all = await conn.fetchval(
            "SELECT AVG(rating) FROM ratings WHERE user_id=$1 AND album_list=$2",
            user_id, album_list
        )
    if not last10:
        return "Пока мало данных. Поставь хотя бы несколько оценок."
    v = [int(r["rating"]) for r in last10]
    avg10 = sum(v) / len(v)
    mood = "хорошая полоса" if avg10 >= 3.8 else ("режим критика" if avg10 <= 2.8 else "нейтрально")
    avg_all_txt = f"{float(avg_all):.2f}" if avg_all is not None else "—"
    return (
        f"🧠 <b>Инсайты</b>\n\n"
        f"Последние 10 оценок: <b>{avg10:.2f}</b> → <b>{mood}</b>\n"
        f"Средняя по текущему списку: <b>{avg_all_txt}</b>\n"
        f'Подсказка: если хочешь больше "пятёрок", попробуй переключить список.'
    )
async def export_ratings_csv(user_id: int) -> bytes:
    async with _pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT album_list, rank, rating, rated_at FROM ratings WHERE user_id=$1 ORDER BY rated_at DESC",
            user_id
        )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["album_list", "rank", "rating", "rated_at"])
    for r in rows:
        w.writerow([r["album_list"], r["rank"], r["rating"], r["rated_at"].isoformat()])
    return buf.getvalue().encode("utf-8")

# ================= DAILY ALBUM =================

async def toggle_daily(user_id: int) -> bool:
    album_list = await get_selected_list(user_id)
    tz = Config.DAILY_TZ
    h, m = Config.DAILY_HOUR, Config.DAILY_MINUTE
    async with _pool().acquire() as conn:
        row = await conn.fetchrow("SELECT is_enabled FROM daily_subscriptions WHERE user_id=$1", user_id)
        if not row:
            await conn.execute(
                """
                INSERT INTO daily_subscriptions (user_id, is_enabled, album_list, send_hour, send_minute, tz, last_sent)
                VALUES ($1, TRUE, $2, $3, $4, $5, NULL)
                """,
                user_id, album_list, h, m, tz
            )
            return True
        new_state = not bool(row["is_enabled"])
        await conn.execute(
            "UPDATE daily_subscriptions SET is_enabled=$1, album_list=$2, send_hour=$3, send_minute=$4, tz=$5 WHERE user_id=$6",
            new_state, album_list, h, m, tz, user_id
        )
        return new_state

def daily_pick_index(list_name: str, for_date: date, user_id: int) -> int:
    albums = get_albums(list_name)
    if len(albums) == 0:
        return -1
    days = (for_date - date(2020, 1, 1)).days
    # user-specific salt keeps it stable and different across users
    salt = (user_id * 2654435761) & 0xFFFFFFFF
    offset = (days + salt) % len(albums)
    return (len(albums) - 1) - offset

async def send_daily_album_to(user_id: int, list_name: str, today: date) -> None:
    idx = daily_pick_index(list_name, today, user_id)
    prefix = f"☀️ <b>Альбом дня</b> ({today.isoformat()})\nСписок: <b>{list_name}</b>"
    await send_album_post(user_id, list_name, idx, ctx="daily", prefix=prefix)

async def daily_loop() -> None:
    tz = ZoneInfo(Config.DAILY_TZ)
    while True:
        try:
            now = datetime.now(tz)
            today = now.date()

            if now.hour == Config.DAILY_HOUR and now.minute == Config.DAILY_MINUTE:
                async with _pool().acquire() as conn:
                    subs = await conn.fetch(
                        "SELECT user_id, album_list, last_sent FROM daily_subscriptions WHERE is_enabled=TRUE"
                    )
                for s in subs:
                    user_id = int(s["user_id"])
                    list_name = s["album_list"]
                    if s["last_sent"] == today:
                        continue
                    try:
                        await send_daily_album_to(user_id, list_name, today)
                        async with _pool().acquire() as conn:
                            await conn.execute(
                                "UPDATE daily_subscriptions SET last_sent=$1 WHERE user_id=$2",
                                today, user_id
                            )
                    except Exception as e:
                        log.warning("daily send failed user=%s: %s", user_id, e)

            nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            await asyncio.sleep(max(1.0, (nxt - datetime.now(tz)).total_seconds()))
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("daily loop crashed")
            await asyncio.sleep(5)

# ================= ERROR HANDLER =================

@router.errors()
async def on_error(event):
    exc = getattr(event, "exception", None)
    if exc:
        log.exception("Unhandled error", exc_info=exc)
    else:
        log.exception("Unhandled error (no exception)")
    return True

# ================= MANUAL COVERS =================

async def _is_image_url(url: str) -> bool:
    """Lightweight check that URL returns image/* content-type."""
    try:
        await init_http()
        async with _http().get(url, allow_redirects=True) as r:
            if r.status != 200:
                return False
            ctype = (r.headers.get("Content-Type") or "").lower()
            return ctype.startswith("image/")
    except Exception:
        return False


def _parse_set_cover_args(text: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Supports:
      /set_cover <rank> <url>
      /set_cover <list name...> <rank> <url>
    Returns: (list_name_or_none, rank_or_none, url_or_none)
    """
    parts = (text or "").split()
    if len(parts) < 3:
        return None, None, None

    if parts[1].isdigit():
        try:
            return None, int(parts[1]), parts[2]
        except Exception:
            return None, None, None

    if len(parts) < 4 or not parts[-2].isdigit():
        return None, None, None

    list_name = " ".join(parts[1:-2]).strip()
    try:
        return list_name, int(parts[-2]), parts[-1]
    except Exception:
        return None, None, None


def _parse_del_cover_args(text: str) -> tuple[Optional[str], Optional[int]]:
    """Supports: /del_cover <rank>  OR  /del_cover <list name...> <rank>"""
    parts = (text or "").split()
    if len(parts) < 2:
        return None, None


def _is_songlink_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not (u.startswith("http://") or u.startswith("https://")):
        return False
    return ("song.link" in u) or ("odesli.co" in u) or ("album.link" in u)

def _parse_set_songlink_args(text: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Returns (list_name or None, rank or None, url or None)
    Supported:
      /set_songlink 37 https://song.link/...
      /set_songlink top500 RS 412 https://song.link/...
    """
    parts = (text or "").split()
    if len(parts) < 3:
        return None, None, None
    url = parts[-1]
    try:
        rank = int(parts[-2])
    except ValueError:
        return None, None, None
    list_name = None
    if len(parts) > 3:
        list_name = " ".join(parts[1:-2])
    return list_name, rank, url

def _parse_del_songlink_args(text: str) -> tuple[Optional[str], Optional[int]]:
    """
    Supported:
      /del_songlink 37
      /del_songlink top500 RS 412
    """
    parts = (text or "").split()
    if len(parts) < 2:
        return None, None
    try:
        rank = int(parts[-1])
    except ValueError:
        return None, None
    list_name = None
    if len(parts) > 2:
        list_name = " ".join(parts[1:-1])
    return list_name, rank

    if parts[1].isdigit():
        try:
            return None, int(parts[1])
        except Exception:
            return None, None

    if len(parts) < 3 or not parts[-1].isdigit():
        return None, None

    list_name = " ".join(parts[1:-1]).strip()
    try:
        return list_name, int(parts[-1])
    except Exception:
        return None, None

# ================= COMMANDS =================



@router.message(Command("cancel"))
async def cmd_cancel(message: Message):
    await db_clear_user_input(message.from_user.id)
    await message.answer("Окей, отменил.", reply_markup=menu_keyboard())

@router.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личку 🙂")
        return
    await init_http()
    await ensure_user(msg.from_user.id)
    text = (
        "Привет.\n\n"
        "Начать: /start_albums\n"
        "Меню: /menu\n"
        "Списки: /lists\n"
        "Статистика: /stats\n"
        "Альбом из другого списка: /next_from <название>\n"
    )
    await msg.answer(text, reply_markup=menu_keyboard())

@router.message(Command("start_albums"))
async def cmd_start_albums(msg: Message):
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личку 🙂")
        return
    await init_http()
    album_list = await ensure_user(msg.from_user.id)
    idx = await get_index(msg.from_user.id, album_list)
    intro = get_list_intro(album_list)
    if intro:
        await msg.answer(intro)
    await send_album_post(msg.from_user.id, album_list, idx, ctx="flow")

@router.message(Command("menu"))
async def cmd_menu(msg: Message):
    await msg.answer("📋 Меню", reply_markup=menu_keyboard())

@router.message(Command("stats"))
async def cmd_stats(msg: Message):
    txt = await build_stats_text(msg.from_user.id)
    await msg.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())
@router.message(Command("top"))
async def cmd_top(msg: Message):
    album_list = await get_selected_list(msg.from_user.id)
    txt = await format_top_bottom(msg.from_user.id, album_list, top=True, limit=10)
    await msg.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.message(Command("bottom"))
async def cmd_bottom(msg: Message):
    album_list = await get_selected_list(msg.from_user.id)
    txt = await format_top_bottom(msg.from_user.id, album_list, top=False, limit=10)
    await msg.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.message(Command("streak"))
async def cmd_streak(msg: Message):
    txt = await streak_text(msg.from_user.id)
    await msg.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.message(Command("insights"))
async def cmd_insights(msg: Message):
    txt = await insights_text(msg.from_user.id)
    await msg.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.message(Command("relisten"))
async def cmd_relisten(msg: Message):
    items = await get_relisten_items(msg.from_user.id, limit=1)
    count = 0
    async with _pool().acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM relisten WHERE user_id=$1", msg.from_user.id)
    await msg.answer(f"🔁 <b>На переслушать</b>\n\nВсего: <b>{count}</b>", parse_mode="HTML", reply_markup=relisten_keyboard())

@router.message(Command("relisten_random"))
async def cmd_relisten_random(msg: Message):
    await send_random_relisten(msg.from_user.id)

@router.message(Command("relisten_list"))
async def cmd_relisten_list(msg: Message):
    await send_relisten_list(msg.from_user.id)



@router.message(Command("lists"))
async def cmd_lists(msg: Message):
    await msg.answer("📚 Выбери список", reply_markup=lists_keyboard())

@router.message(Command("set_list"))
async def cmd_set_list(msg: Message):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Напиши так: /set_list top100", reply_markup=lists_keyboard())
        return
    try:
        resolved = await set_selected_list(msg.from_user.id, parts[1])
    except ValueError:
        await msg.answer("Не нашёл такой список. Набери /lists", reply_markup=lists_keyboard())
        return
    idx = await get_index(msg.from_user.id, resolved)
    await msg.answer(f"Ок. Список: {resolved}")
    intro = get_list_intro(resolved)
    if intro:
        await msg.answer(intro)
    await send_album_post(msg.from_user.id, resolved, idx, ctx="flow")




@router.message(Command("next_from"))
async def cmd_next_from(msg: Message):
    """Показать следующий альбом из другого списка, не переключая текущий."""
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личные сообщения 🙂")
        return
    await init_http()

    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Напиши так: /next_from top500 RS", reply_markup=lists_keyboard())
        return

    target = parts[1].strip()
    resolved = resolve_list_name(target)
    if not resolved:
        await msg.answer("Не нашёл такой список. Набери /lists", reply_markup=lists_keyboard())
        return

    user_id = msg.from_user.id
    # Берём текущий индекс по выбранному списку и увеличиваем его на 1 в рамках target-списка
    idx = await get_index(user_id, resolved)
    await send_album_post(
        user_id,
        resolved,
        idx,
        ctx="from_other",
        prefix=f"↪️ Из списка: <b>{resolved}</b>",
    )
    await set_index(user_id, resolved, idx - 1)

@router.message(Command("go"))




async def perform_find_artist(user_id: int, needle: str) -> Dict:
    prog = await db_get_user_progress(user_id)
    active_list = None
    if isinstance(prog, dict):
        active_list = prog.get("active_list") or prog.get("album_list") or prog.get("list")
    if not active_list:
        active_list = await get_selected_list(user_id)

    lists_map = globals().get("ALBUM_LISTS") or globals().get("ALBUM_LISTS_DATA") or globals().get("ALBUM_LISTS_REGISTRY")
    if not isinstance(lists_map, dict) or active_list not in lists_map:
        return {"error": "Не могу найти выбранный список. Открой меню и выбери список заново."}

    albums = lists_map[active_list]
    if not isinstance(albums, list):
        return {"error": "Список альбомов повреждён. Проверь загрузку CSV."}

    needle_l = needle.lower()
    matches = []
    for item in albums:
        artist = str(item.get("artist", ""))
        album = str(item.get("album", ""))
        rank = item.get("rank") or item.get("position") or item.get("id")
        if needle_l in artist.lower():
            try:
                rank_int = int(rank)
            except Exception:
                rank_int = rank
            matches.append((rank_int, artist, album))

    if not matches:
        return {"active_list": active_list, "matches": []}

    matches.sort(key=lambda x: (x[0] if isinstance(x[0], int) else 10**9, x[1]))
    matches = matches[:10]

    kb = InlineKeyboardBuilder()
    lines = [f"Нашёл в списке <b>{html.escape(str(active_list))}</b>:"]
    for rank, artist, album in matches:
        lines.append(f"{rank}. {html.escape(artist)} — {html.escape(album)}")
        kb.button(text=f"GO {rank}", callback_data=f"go:{active_list}:{rank}")
    kb.adjust(5)

    return {"active_list": active_list, "matches": matches, "text": "\n".join(lines), "kb": kb.as_markup()}

@router.message(Command("find_artist"))
async def cmd_find_artist(message: Message):
    """
    Search artist within currently selected list and show rank+album with GO buttons.
    Uses the user's active_list from user_progress.
    """
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "Напиши так: <code>/find_artist имя_артиста</code>\n"
            "Я поищу в текущем выбранном списке и покажу позиции.",
            parse_mode="HTML",
        )
        return

    needle = parts[1].strip().lower()
    user_id = message.from_user.id

    # get active list
    active_list = None
    try:
        prog = db_get_user_progress(user_id)
        if isinstance(prog, dict):
            active_list = prog.get("active_list") or prog.get("album_list") or prog.get("list")
    except Exception:
        active_list = None

    if not active_list:
        await message.answer("Сначала выбери список альбомов в меню, потом используй поиск.")
        return

    lists_map = globals().get("ALBUM_LISTS") or globals().get("ALBUM_LISTS_DATA") or globals().get("ALBUM_LISTS_REGISTRY")
    if not isinstance(lists_map, dict) or active_list not in lists_map:
        await message.answer("Не могу найти выбранный список. Попробуй выбрать список заново в меню.")
        return

    albums = lists_map[active_list]
    if not isinstance(albums, list):
        await message.answer("Список альбомов повреждён. Проверь загрузку CSV.")
        return

    matches = []
    for item in albums:
        artist = str(item.get("artist", ""))
        album = str(item.get("album", ""))
        rank = item.get("rank") or item.get("position") or item.get("id")
        if needle in artist.lower():
            try:
                rank_int = int(rank)
            except Exception:
                rank_int = rank
            matches.append((rank_int, artist, album))

    if not matches:
        await message.answer(
            f"В списке <b>{html.escape(str(active_list))}</b> не нашёл артиста: <b>{html.escape(parts[1])}</b>.",
            parse_mode="HTML",
        )
        return

    matches.sort(key=lambda x: (x[0] if isinstance(x[0], int) else 10**9, x[1]))
    matches = matches[:10]

    kb = InlineKeyboardBuilder()
    lines = [f"Нашёл в списке <b>{html.escape(str(active_list))}</b>:"]
    for rank, artist, album in matches:
        lines.append(f"{rank}. {artist} — {album}")
        kb.button(text=f"GO {rank}", callback_data=f"go:{active_list}:{rank}")
    kb.adjust(5)

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb.as_markup())


async def cmd_go(msg: Message):
    """
    Переход к конкретному альбому по ранку.
    Примеры:
      /go 37
      /go top500 RS 412
    """
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личные сообщения 🙂")
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        await msg.answer("Напиши так: /go 37\nИли так: /go top500 RS 412")
        return

    try:
        rank = int(parts[-1])
    except ValueError:
        await msg.answer("Не понял rank. Пример: /go 37 или /go top500 RS 412")
        return

    if len(parts) == 2:
        album_list = await get_selected_list(msg.from_user.id)
    else:
        list_name = " ".join(parts[1:-1])
        resolved = resolve_list_name(list_name)
        if not resolved:
            await msg.answer("Не нашёл такой список. Набери /lists", reply_markup=lists_keyboard())
            return
        album_list = resolved

    idx = find_index_by_rank(album_list, rank)
    if idx is None:
        await msg.answer(f"Не нашёл альбом #{rank} в списке {album_list}.")
        return

    await set_selected_list(msg.from_user.id, album_list)
    await set_index(msg.from_user.id, album_list, idx)

    await send_album_post(
        msg.from_user.id,
        album_list,
        idx,
        ctx="jump",
        prefix=f"🎯 Переход к альбому #{rank}\nСписок: <b>{album_list}</b>",
    )




@router.message(F.text & ~F.text.startswith("/"))
async def pending_text_handler(message: Message):
    ui = await db_get_user_input(message.from_user.id)
    if not ui:
        return
    if ui.get("mode") != "find_artist":
        return

    needle = (message.text or "").strip()
    if not needle:
        await message.answer("Пусто. Напиши имя артиста текстом, или /cancel чтобы отменить.")
        return

    res = await perform_find_artist(message.from_user.id, needle)
    await db_clear_user_input(message.from_user.id)

    if res.get("error"):
        await message.answer(res["error"], reply_markup=menu_keyboard())
        return

    if not res.get("matches"):
        await message.answer(
            f"В списке <b>{html.escape(str(res.get('active_list')))}</b> не нашёл: <b>{html.escape(needle)}</b>.",
            parse_mode="HTML",
            reply_markup=menu_keyboard(),
        )
        return

    await message.answer(res["text"], parse_mode="HTML", reply_markup=res["kb"])

@router.message(Command("export_ratings"))
async def cmd_export(msg: Message):
    data = await export_ratings_csv(msg.from_user.id)
    await msg.answer_document(BufferedInputFile(data, filename="ratings.csv"))

@router.message(Command("set_cover"))
async def cmd_set_cover(msg: Message):
    """
    Set manual cover URL for an album.
    Usage:
      /set_cover <rank> <url>
      /set_cover <list name...> <rank> <url>
    """
    await init_http()
    cur_list = await ensure_user(msg.from_user.id)

    list_name, rank, url = _parse_set_cover_args(msg.text or "")
    if rank is None or not url:
        await msg.answer(
            "Формат:\n"
            "/set_cover 37 https://...jpg\n"
            "или\n"
            "/set_cover top500 RS 412 https://...jpg"
        )
        return

    target_list = cur_list
    if list_name:
        resolved = resolve_list_name(list_name)
        if not resolved:
            await msg.answer("Не нашёл такой список. Набери /lists.")
            return
        target_list = resolved

    if not await _is_image_url(url):
        await msg.answer("Ссылка не выглядит как прямая картинка (Content-Type не image/*). Дай прямой URL на файл.")
        return

    await set_cached_cover(target_list, rank, url, "manual")
    await msg.answer(f"Ок. Поставил обложку вручную: {target_list} #{rank}")


@router.message(Command("del_cover"))
async def cmd_del_cover(msg: Message):
    """
    Remove cached/manual cover so bot will re-fetch it.
    Usage:
      /del_cover <rank>
      /del_cover <list name...> <rank>
    """
    cur_list = await ensure_user(msg.from_user.id)


@router.message(Command("set_songlink"))
async def cmd_set_songlink(msg: Message):
    """
    Manually set song.link URL for an album.
    Usage:
      /set_songlink <rank> <song.link url>
      /set_songlink <list name...> <rank> <song.link url>
    """
    cur_list = await ensure_user(msg.from_user.id)

    list_name, rank, url = _parse_set_songlink_args(msg.text or "")
    if rank is None or not url:
        await msg.answer(
            "Формат:\n"
            "/set_songlink 37 https://song.link/...\n"
            "или\n"
            "/set_songlink top500 RS 412 https://song.link/..."
        )
        return

    if not _is_songlink_url(url):
        await msg.answer("Ссылка должна быть song.link, album.link или odesli.co и начинаться с http(s).")
        return

    target_list = cur_list
    if list_name:
        resolved = resolve_list_name(list_name)
        if not resolved:
            await msg.answer("Не нашёл такой список. Набери /lists.")
            return
        target_list = resolved

    await set_cached_songlink(target_list, rank, url)
    await msg.answer(f"Ок. Поставил song.link: {target_list} #{rank}")

@router.message(Command("del_songlink"))
async def cmd_del_songlink(msg: Message):
    """
    Remove cached/manual song.link so bot will re-fetch it.
    Usage:
      /del_songlink <rank>
      /del_songlink <list name...> <rank>
    """
    cur_list = await ensure_user(msg.from_user.id)

    list_name, rank = _parse_del_songlink_args(msg.text or "")
    if rank is None:
        await msg.answer(
            "Формат:\n"
            "/del_songlink 37\n"
            "или\n"
            "/del_songlink top500 RS 412"
        )
        return

    target_list = cur_list
    if list_name:
        resolved = resolve_list_name(list_name)
        if not resolved:
            await msg.answer("Не нашёл такой список. Набери /lists.")
            return
        target_list = resolved

    await delete_cached_songlink(target_list, rank)
    await msg.answer(f"Ок. Удалил кэш song.link: {target_list} #{rank}")

    list_name, rank = _parse_del_cover_args(msg.text or "")
    if rank is None:
        await msg.answer(
            "Формат:\n"
            "/del_cover 37\n"
            "или\n"
            "/del_cover top500 RS 412"
        )
        return

    target_list = cur_list
    if list_name:
        resolved = resolve_list_name(list_name)
        if not resolved:
            await msg.answer("Не нашёл такой список. Набери /lists.")
            return
        target_list = resolved

    await delete_cached_cover(target_list, rank)
    await msg.answer(f"Ок. Удалил кэш обложки: {target_list} #{rank}")

# ================= CALLBACKS =================

@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()

@router.callback_query(F.data.startswith("nav:"))
async def nav_cb(call: CallbackQuery):
    user_id = call.from_user.id
    album_list = await ensure_user(user_id)
    idx = await get_index(user_id, album_list)
    action = call.data.split(":", 1)[1]

    if action == "next":
        await set_index(user_id, album_list, idx - 1)
        await call.answer()
        await send_album_post(user_id, album_list, idx - 1, ctx="flow")
        return

    if action == "prev":
        await set_index(user_id, album_list, idx + 1)
        await call.answer()
        await send_album_post(user_id, album_list, idx + 1, ctx="flow")
        return

    if action == "reset":
        albums = get_albums(album_list)
        new_idx = len(albums) - 1
        await set_index(user_id, album_list, new_idx)
        await call.answer("Сброшено")
        await send_album_post(user_id, album_list, new_idx, ctx="flow")
        return

    await call.answer()

@router.callback_query(F.data == "ui:menu")
async def menu_cb(call: CallbackQuery):
    await call.answer()
    await call.message.answer("📋 Меню", reply_markup=menu_keyboard())


@router.callback_query(F.data == "ui:find_artist")
async def ui_find_artist_cb(call: CallbackQuery):
    await call.answer()
    await db_set_user_input(call.from_user.id, "find_artist", None)
    await call.message.answer(
        "🔎 Напиши имя артиста одним сообщением.\n"
        "Я покажу его позиции в текущем списке и дам кнопки GO.\n\n"
        "Отмена: /cancel",
    )


@router.callback_query(F.data == "ui:stats")
async def stats_cb(call: CallbackQuery):
    txt = await build_stats_text(call.from_user.id)
    await call.answer()
    await call.message.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())


@router.callback_query(F.data == "ui:stats_plus")
async def stats_plus_cb(call: CallbackQuery):
    await call.answer()
    await call.message.answer("📈 Выбери раздел", reply_markup=stats_plus_keyboard())

@router.callback_query(F.data == "ui:top")
async def top_cb(call: CallbackQuery):
    album_list = await get_selected_list(call.from_user.id)
    txt = await format_top_bottom(call.from_user.id, album_list, top=True, limit=10)
    await call.answer()
    await call.message.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.callback_query(F.data == "ui:bottom")
async def bottom_cb(call: CallbackQuery):
    album_list = await get_selected_list(call.from_user.id)
    txt = await format_top_bottom(call.from_user.id, album_list, top=False, limit=10)
    await call.answer()
    await call.message.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.callback_query(F.data == "ui:streak")
async def streak_cb(call: CallbackQuery):
    txt = await streak_text(call.from_user.id)
    await call.answer()
    await call.message.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.callback_query(F.data == "ui:insights")
async def insights_cb(call: CallbackQuery):
    txt = await insights_text(call.from_user.id)
    await call.answer()
    await call.message.answer(txt, parse_mode="HTML", reply_markup=menu_keyboard())

@router.callback_query(F.data == "ui:relisten_menu")
async def relisten_menu_cb(call: CallbackQuery):
    async with _pool().acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM relisten WHERE user_id=$1", call.from_user.id)
    await call.answer()
    await call.message.answer(
        f"🔁 <b>На переслушать</b>\n\nВсего: <b>{count}</b>",
        parse_mode="HTML",
        reply_markup=relisten_keyboard()
    )

@router.callback_query(F.data == "ui:relisten_random")
async def relisten_random_cb(call: CallbackQuery):
    await call.answer()
    await send_random_relisten(call.from_user.id)

@router.callback_query(F.data == "ui:relisten_list")
async def relisten_list_cb(call: CallbackQuery):
    await call.answer()
    await send_relisten_list(call.from_user.id)

@router.callback_query(F.data.startswith("ui:relisten:"))
async def relisten_toggle_cb(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 5:
        await call.answer("Ошибка кнопки", show_alert=True)
        return
    album_list = canonical_list_name(parts[2])
    album_list = resolve_list_name(album_list) or album_list
    rank = int(parts[3])
    ctx = parts[4]

    enabled = await toggle_relisten(call.from_user.id, album_list, rank)
    await call.answer("Добавил" if enabled else "Убрал")

    # refresh caption/keyboard for this post
    albums = get_albums(album_list)
    row = albums.loc[albums["rank"] == rank]
    if row.empty:
        return
    row = row.iloc[0]
    artist = str(row["artist"])
    album = str(row["album"])
    genre = str(row.get("genre", "") or "")
    ur = await get_user_rating(call.from_user.id, album_list, rank)
    caption = album_caption(rank, artist, album, genre, ur, in_relisten=enabled)
    listen_url = await get_songlink_url(album_list, rank, artist, album)
    kb = album_keyboard(album_list, rank, artist, album, ur, ctx, listen_url, in_relisten=enabled)

    try:
        if call.message.photo:
            await call.message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await call.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        log.debug("relisten toggle edit failed: %s", e)

@router.callback_query(F.data == "ui:lists")
async def ui_lists(call: CallbackQuery):
    await call.answer()
    await call.message.answer("📚 Выбери список", reply_markup=lists_keyboard())

@router.callback_query(F.data.startswith("setlist:"))
async def setlist_cb(call: CallbackQuery):
    enc = call.data.split(":", 1)[1]
    name = canonical_list_name(enc)
    try:
        resolved = await set_selected_list(call.from_user.id, name)
    except ValueError:
        await call.answer("Список не найден", show_alert=True)
        return
    idx = await get_index(call.from_user.id, resolved)
    await call.answer(f"Список: {resolved}")
    intro = get_list_intro(resolved)
    if intro:
        await call.message.answer(intro)
    await send_album_post(call.from_user.id, resolved, idx, ctx="flow")

@router.callback_query(F.data == "ui:daily")
async def ui_daily(call: CallbackQuery):
    enabled = await toggle_daily(call.from_user.id)
    t = f"{Config.DAILY_HOUR:02d}:{Config.DAILY_MINUTE:02d}"
    if enabled:
        await call.answer("Включил")
        await call.message.answer(
            f"☀️ Альбом дня включён.\nБуду присылать каждый день в {t} ({Config.DAILY_TZ}).",
            reply_markup=menu_keyboard()
        )
    else:
        await call.answer("Выключил")
        await call.message.answer("☀️ Альбом дня выключен.", reply_markup=menu_keyboard())

@router.callback_query(F.data.startswith("ui:rate:"))
async def rate_ui(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 5:
        await call.answer("Ошибка кнопки", show_alert=True)
        return
    album_list = canonical_list_name(parts[2])
    album_list = resolve_list_name(album_list) or album_list
    rank = int(parts[3])
    ctx = parts[4]

    albums = get_albums(album_list)
    rows = albums.loc[albums["rank"] == rank]
    if rows.empty:
        await call.answer("Альбом не найден", show_alert=True)
        return
    row = rows.iloc[0]
    artist = str(row["artist"])
    album = str(row["album"])
    genre = str(row.get("genre", "") or "")
    current_rating = await get_user_rating(call.from_user.id, album_list, rank)
    caption = album_caption(rank, artist, album, genre, current_rating)

    await call.answer()
    try:
        if call.message.photo:
            await call.message.edit_caption(
                caption="Оцени альбом:\n\n" + caption,
                parse_mode="HTML",
                reply_markup=rating_keyboard(album_list, rank, ctx),
            )
        else:
            await call.message.edit_text(
                "Оцени альбом:\n\n" + caption,
                parse_mode="HTML",
                reply_markup=rating_keyboard(album_list, rank, ctx),
            )
    except Exception as e:
        log.debug("rate ui edit failed: %s", e)
        await bot.send_message(call.from_user.id, "Оцени альбом:", reply_markup=rating_keyboard(album_list, rank, ctx))

@router.callback_query(F.data.startswith("rate:"))
async def rate_set(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 5:
        await call.answer("Ошибка кнопки", show_alert=True)
        return

    rating = int(parts[1])
    album_list = canonical_list_name(parts[2])
    album_list = resolve_list_name(album_list) or album_list
    rank = int(parts[3])
    ctx = parts[4]

    await upsert_rating(call.from_user.id, album_list, rank, rating)
    await call.answer(f"⭐ {rating} сохранено")

    await edit_album_post(call, album_list, rank, ctx)

    # Only auto-advance in main flow
    if ctx == "flow":
        idx = await get_index(call.from_user.id, album_list)
        albums = get_albums(album_list)
        if 0 <= idx < len(albums) and int(albums.iloc[idx]["rank"]) == rank:
            await set_index(call.from_user.id, album_list, idx - 1)
            await send_album_post(call.from_user.id, album_list, idx - 1, ctx="flow")

@router.callback_query(F.data.startswith("ui:back:"))
async def back(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 5:
        await call.answer()
        return
    album_list = canonical_list_name(parts[2])
    album_list = resolve_list_name(album_list) or album_list
    rank = int(parts[3])
    ctx = parts[4]
    await call.answer()
    await edit_album_post(call, album_list, rank, ctx)

# ================= START / SHUTDOWN =================

async def on_shutdown() -> None:
    global http_session, daily_task
    if daily_task and not daily_task.done():
        daily_task.cancel()
        try:
            await daily_task
        except Exception:
            pass
    if http_session and not http_session.closed:
        await http_session.close()
    if pg_pool:
        await pg_pool.close()

_ai_user_locks: dict[int, asyncio.Lock] = {}

def _get_user_lock(user_id: int) -> asyncio.Lock:
    lock = _ai_user_locks.get(int(user_id))
    if not lock:
        lock = asyncio.Lock()
        _ai_user_locks[int(user_id)] = lock
    return lock


def ai_menu_keyboard(album_list: str, rank: int) -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton(text="🧠 Коротко", callback_data=f"ai:short:{album_list}:{rank}"),
            InlineKeyboardButton(text="📚 Подробно", callback_data=f"ai:long:{album_list}:{rank}"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def _album_by_rank(album_list: str, rank: int) -> Optional[dict]:
    idx = find_index_by_rank(album_list, int(rank))
    if idx is None:
        return None
    df = get_albums(album_list)
    row = df.iloc[int(idx)]
    return {"artist": str(row["artist"]), "album": str(row["album"])}

@router.callback_query(lambda c: False)
async def ai_menu(call: CallbackQuery):
    try:
        _, _, album_list, rank_s = call.data.split(":", 3)
        rank = int(rank_s)
    except Exception:
        await call.answer("Ошибка AI-меню", show_alert=True)
        return
    await call.answer()
    await call.message.answer("Выбери режим AI:", reply_markup=ai_menu_keyboard(album_list, rank))

@router.callback_query(lambda c: False)
async def ai_generate(call: CallbackQuery):
    await call.answer()
    try:
        _, mode, album_list, rank_s = call.data.split(":", 3)
        rank = int(rank_s)
    except Exception:
        await call.message.answer("Не понял запрос.")
        return

    if not OPENAI_API_KEY:
        await call.message.answer("AI не настроен. Добавь переменную OPENAI_API_KEY в Railway.")
        return

    info = await _album_by_rank(album_list, rank)
    if not info:
        await call.message.answer("Не нашёл этот альбом в списке.")
        return

    cached = await get_cached_ai_note(album_list, rank, kind)
    if cached:
        title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
        await call.message.answer(
            f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(cached)}",
            parse_mode="HTML",
        )
        return

    used = await get_ai_usage_today(call.from_user.id)
    limit = _ai_max_daily()
    if used >= limit:
        await call.message.answer(f"Лимит AI на сегодня: {limit}. Попробуй завтра.")
        return

    lock = _get_user_lock(call.from_user.id)
    async with lock:
        cached2 = await get_cached_ai_note(album_list, rank, kind)
        if cached2:
            title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
            await call.message.answer(
                f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(cached2)}",
                parse_mode="HTML",
            )
            return

        await inc_ai_usage_today(call.from_user.id)

        thinking = await call.message.answer("⏳ Думаю...")

        facts = await get_cached_album_facts(album_list, rank)
        if not facts:
            facts = await fetch_musicbrainz_facts(info["artist"], info["album"])
        # safety: fetchers can fail and return None
        if not isinstance(facts, dict):
            facts = {
                "artist": info.get("artist"),
                "album": info.get("album"),
                "source": "none",
            }
        try:
            facts["songlink_url"] = await get_songlink_url(album_list, rank, info["artist"], info["album"])
            facts["google_url"] = google_link(info["artist"], info["album"])
            await set_cached_album_facts(album_list, rank, facts)
        except Exception as e:
            log.debug("songlink/google facts fill failed: %s", e)
        try:
            text = await openai_generate_album_note(kind, facts)

            if not text:
                await thinking.edit_text("Не получилось получить ответ AI. Попробуй позже.")
                return

            await set_cached_ai_note(album_list, rank, kind, text)

            title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
        except Exception as e:
            log.exception("AI generation failed: %s", e)
            try:
                await thinking.edit_text("AI упал на этом запросе. Попробуй ещё раз.")
            except Exception:
                pass
            return

        await thinking.edit_text(
            f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(text)}",
            parse_mode="HTML",
        )


@router.message(Command("favorites"))
async def cmd_favorites(message: Message):
    items = await list_favorites(message.from_user.id, limit=50)
    if not items:
        await message.answer("Любимых альбомов пока нет.")
        return
    lines = ["❤️ Любимые (последние 50):"]
    for (lst, rk) in items:
        info = await _album_by_rank(lst, rk)
        if info:
            lines.append(f"{lst} #{rk}: {info['artist']} — {info['album']}")
        else:
            lines.append(f"{lst} #{rk}")
    await message.answer("\n".join(lines))

@router.message(Command("rand_favorite"))
async def cmd_rand_favorite(message: Message):
    item = await random_favorite(message.from_user.id)
    if not item:
        await message.answer("Любимых альбомов пока нет.")
        return
    lst, rk = item
    await set_selected_list(message.from_user.id, lst)
    await set_progress(message.from_user.id, lst, rk)
    await send_album_post(message.from_user.id, lst)

@router.message(Command("version"))
async def cmd_version(msg: Message):
    await msg.answer(f"Версия бота: {BOT_VERSION}")




@router.callback_query(lambda c: c.data and (c.data.startswith("ai:artist:") or c.data.startswith("ai:album:")))
async def ai_artist_or_album(call: CallbackQuery):
    await call.answer()
    try:
        _, kind, album_list, rank_s = call.data.split(":", 3)
        rank = int(rank_s)
    except Exception:
        await call.message.answer("Не понял запрос.")
        return

    if not OPENAI_API_KEY:
        await call.message.answer("AI не настроен. Добавь переменную OPENAI_API_KEY в Railway.")
        return

    info = await _album_by_rank(album_list, rank)
    mode_key = f"{kind}:v{AI_CACHE_VERSION}"
    if not info:
        await call.message.answer("Не нашёл этот альбом в списке.")
        return

    cached = await get_cached_ai_note(album_list, rank, mode_key)
        # render cached in current UI format

    if cached:
        title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
        await call.message.answer(
            f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(cached)}",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    used = await get_ai_usage_today(call.from_user.id)
    limit = _ai_max_daily()
    if used >= limit:
        await call.message.answer(f"Лимит AI на сегодня: {limit}. Попробуй завтра.")
        return

    lock = _get_user_lock(call.from_user.id)
    async with lock:
        cached2 = await get_cached_ai_note(album_list, rank, mode_key)
        if cached2:
            title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
            await call.message.answer(
                f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(cached2)}",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return

        await inc_ai_usage_today(call.from_user.id)
        thinking = await call.message.answer("⏳ Думаю...")

        # facts from MusicBrainz (cached)
        facts = await get_cached_album_facts(album_list, rank)
        if not facts:
            facts = await fetch_musicbrainz_facts(info["artist"], info["album"])
        # safety: fetchers can fail and return None
        if not isinstance(facts, dict):
            facts = {
                "artist": info.get("artist"),
                "album": info.get("album"),
                "source": "none",
            }
        try:
            facts["songlink_url"] = await get_songlink_url(album_list, rank, info["artist"], info["album"])
            facts["google_url"] = google_link(info["artist"], info["album"])
            await set_cached_album_facts(album_list, rank, facts)
        except Exception as e:
            log.debug("songlink/google facts fill failed: %s", e)

        # wikipedia summary
        try:
            if kind == "artist":
                wiki = await fetch_wikipedia_summary(info["artist"])
                wiki = wiki or {}
            else:
                wiki = await fetch_wikipedia_summary(f"{info['artist']} {info['album']} album")
                wiki = wiki or {}
        except Exception as e:
            log.debug("wikipedia fetch failed: %s", e)
            wiki = {}

        try:
            lastfm = await fetch_lastfm_artist_info(info['artist']) if kind == 'artist' else await fetch_lastfm_album_info(info['artist'], info['album'])
            slim_facts = {
                'artist': facts.get('artist'),
                'album': facts.get('album'),
                'tags': facts.get('tags') or [],
                'track_count': facts.get('track_count'),
            }
            text = await openai_generate_note(kind, slim_facts, wiki, lastfm)
            body = render_ai_note(kind, info, slim_facts, text)
        
            brief = parse_ai_brief(text or "")
            track_count = (slim_facts or {}).get("track_count") if isinstance(slim_facts, dict) else None

            if kind == "album":
                body = (
                    f"<b>💿 Об альбоме</b>\n"
                    f"{html.escape(info['artist'])} — {html.escape(info['album'])}\n\n"
                    f"Коротко:\n"
                    f"• 🎭 <b>Идея</b> {html.escape(brief['idea'])}\n"
                    f"• 🎧 <b>Звук</b> {html.escape(brief['sound'])}\n"
                    f"• ✍️ <b>Темы</b> {html.escape(brief['themes'])}\n"
                    f"• 🧠 <b>Фишка</b> {html.escape(brief['feature'])}\n"
                )
                if isinstance(track_count, int) and track_count > 0:
                    body += f"\nТреков {track_count}"
            else:
                body = (
                    f"<b>👤 Об артисте</b>\n"
                    f"{html.escape(info['artist'])}\n\n"
                    f"Коротко:\n"
                    f"• 🎭 <b>Кто это</b> {html.escape(brief['idea'])}\n"
                    f"• 🎧 <b>Звук</b> {html.escape(brief['sound'])}\n"
                    f"• ✍️ <b>Темы</b> {html.escape(brief['themes'])}\n"
                    f"• 🧠 <b>Фишка</b> {html.escape(brief['feature'])}\n"
                )
        except Exception as e:
            log.exception("openai_generate_note failed: %s", e)
            try:
                await thinking.edit_text("AI завис или упал. Попробуй ещё раз.")
            except Exception:
                pass
            return
        if not text:
            await thinking.edit_text("Не получилось получить ответ AI. Попробуй позже.")
            return

        await set_cached_ai_note(album_list, rank, mode_key, text)

        title = "👤 Об артисте" if kind == "artist" else "💿 Об альбоме"
        await thinking.edit_text(
            f"{title}\n<b>{html.escape(info['artist'])} — {html.escape(info['album'])}</b>\n\n{html.escape(text)}",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

# Legacy callback data from older builds (compat)
LEGACY_NEXT = {"next", "forward", "skip", "nav_next", "nav:next_album", "album:next"}
LEGACY_PREV = {"prev", "back", "nav_prev", "nav:prev_album", "album:prev"}
LEGACY_ALL = LEGACY_NEXT | LEGACY_PREV

@router.callback_query(lambda c: (c.data or "").strip() in LEGACY_ALL)
async def cb_legacy_nav(call: CallbackQuery):
    data = (call.data or "").strip()
    if data in LEGACY_NEXT:
        call.data = "nav:next"
        return await nav_cb(call)
    if data in LEGACY_PREV:
        call.data = "nav:prev"
        return await nav_cb(call)

@router.callback_query(lambda c: c.data and c.data.startswith("fav:toggle:"))
async def fav_toggle(call: CallbackQuery):
    try:
        _, _, album_list, rank_s = call.data.split(":", 3)
        rank = int(rank_s)
    except Exception:
        await call.answer("Ошибка.", show_alert=True)
        return

    new_state = await toggle_favorite(call.from_user.id, album_list, rank)
    await call.answer("Добавлено в любимое" if new_state else "Убрано из любимого")

    # Update keyboard badge in-place
    try:
        info = await _album_by_rank(album_list, rank)
        if not info:
            return
        rated = await get_rating(call.from_user.id, album_list, rank)
        in_relisten = await is_in_relisten(call.from_user.id, album_list, rank)
        listen_url = await get_songlink_url(album_list, rank, info["artist"], info["album"])
        kb = album_keyboard(
            album_list, rank, info["artist"], info["album"],
            rated, ctx="post", listen_url=listen_url,
            in_relisten=in_relisten, is_fav=new_state
        )
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        log.debug("fav toggle edit markup failed: %s", e)




@router.callback_query()
async def cb_unknown_callback(call: CallbackQuery):
    data = (call.data or "").strip()
    await call.answer(
        f"Кнопка устарела или не поддерживается.\n\nДанные: {data}\nВерсия: {BOT_VERSION}",
        show_alert=True,
    )


async def main():
    log.info("Bot version: %s", BOT_VERSION)
    global daily_task
    await init_pg()
    await init_http()

    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)

    daily_task = asyncio.create_task(daily_loop())

    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
