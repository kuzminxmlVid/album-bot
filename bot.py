import os
import re
import csv
import io
import asyncio
import logging
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

        try:
            await conn.execute("ALTER TABLE covers ALTER COLUMN updated_at SET DEFAULT NOW()")
        except Exception:
            pass

        
        # relisten list ("Переслушаю")
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

def album_keyboard(album_list: str, rank: int, artist: str, album: str, rated: Optional[int], ctx: str, *, in_relisten: bool = False) -> InlineKeyboardMarkup:
    rate_text = "⭐ Оценить" if not rated else f"⭐ Оценено: {rated}"
    rel_text = "🔁 Переслушаю ✅" if in_relisten else "🔁 Переслушаю"
    enc = encode_list_name(album_list)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎧 Найти альбом", url=google_link(artist, album))],
        [
            InlineKeyboardButton(text="Прыдыдущий альбом", callback_data="nav:prev"),
            InlineKeyboardButton(text="Пропустить", callback_data="nav:next"),
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
        [InlineKeyboardButton(text="🔄 Сначала списка", callback_data="nav:reset")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="ui:stats")],
        [InlineKeyboardButton(text="📈 Статистика+", callback_data="ui:stats_plus")],
        [InlineKeyboardButton(text="🔁 На переслушать", callback_data="ui:relisten_menu")],
        [InlineKeyboardButton(text="📚 Списки", callback_data="ui:lists")],
        [InlineKeyboardButton(text="☀️ Альбом дня", callback_data="ui:daily")],
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
    kb = album_keyboard(album_list, rank, artist, album, user_rating, ctx, in_relisten=in_rel)
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
    kb = album_keyboard(album_list, rank, artist, album, user_rating, ctx, in_relisten=in_rel)

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
    """Show the next album from another list without switching the current list."""
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
    idx = await get_index(user_id, resolved)

    await send_album_post(
        user_id,
        resolved,
        idx,
        ctx="from_other",
        prefix=f"↪️ Из списка: <b>{resolved}</b>",
    )
    await set_index(user_id, resolved, idx - 1)
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
    kb = album_keyboard(album_list, rank, artist, album, ur, ctx, in_relisten=enabled)

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

async def main():
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
