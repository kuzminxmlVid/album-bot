import os
import asyncio
import logging
from typing import Optional, Tuple, Dict
from urllib.parse import quote_plus
from datetime import datetime, timezone

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

    # MusicBrainz просит указывать контакт в User-Agent (почта или сайт).
    MB_CONTACT = os.getenv("MB_CONTACT", "contact:not_set")
    MB_APP = os.getenv("MB_APP", "MusicAlbumClubBot/1.0")

if not Config.TOKEN or not Config.DATABASE_URL:
    raise RuntimeError("ENV vars not set: TOKEN and/or DATABASE_URL")

# asyncpg can be picky about scheme
if Config.DATABASE_URL.startswith("postgres://"):
    Config.DATABASE_URL = Config.DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ================= BOT =================

bot = Bot(token=Config.TOKEN)
dp = Dispatcher()
router = Router()

pg_pool: Optional[asyncpg.Pool] = None
http_session: Optional[aiohttp.ClientSession] = None

# ================= DATABASE =================

async def init_pg() -> None:
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=Config.DATABASE_URL, min_size=1, max_size=5)

    async with pg_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            album_list TEXT NOT NULL,
            current_index INTEGER NOT NULL
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            user_id BIGINT NOT NULL,
            album_list TEXT NOT NULL,
            rank INTEGER NOT NULL,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            PRIMARY KEY (user_id, album_list, rank)
        )
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ratings_user
        ON ratings (user_id, album_list)
        """)

        # Cover cache (per list+rank)
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
        CREATE INDEX IF NOT EXISTS idx_covers_updated
        ON covers (updated_at)
        """)

def _pool() -> asyncpg.Pool:
    if pg_pool is None:
        raise RuntimeError("Postgres pool is not initialized")
    return pg_pool

# ================= ALBUMS =================

album_cache: Dict[str, pd.DataFrame] = {}

def load_albums(name: str) -> pd.DataFrame:
    path = os.path.join(Config.ALBUMS_DIR, f"{name}.xlsx")
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

def get_albums(name: str) -> pd.DataFrame:
    if name not in album_cache:
        album_cache[name] = load_albums(name)
    return album_cache[name]

# ================= USERS =================

async def get_user(user_id: int) -> Tuple[str, int]:
    async with _pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, current_index FROM users WHERE user_id=$1",
            user_id
        )
        if not row:
            albums = get_albums(Config.DEFAULT_LIST)
            idx = len(albums) - 1
            await conn.execute(
                "INSERT INTO users (user_id, album_list, current_index) VALUES ($1,$2,$3)",
                user_id, Config.DEFAULT_LIST, idx
            )
            return Config.DEFAULT_LIST, idx
        return row["album_list"], row["current_index"]

async def set_index(user_id: int, idx: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute(
            "UPDATE users SET current_index=$1 WHERE user_id=$2",
            idx, user_id
        )

# ================= HTTP =================

async def init_http() -> None:
    global http_session
    if http_session is None or http_session.closed:
        timeout = aiohttp.ClientTimeout(total=12)
        http_session = aiohttp.ClientSession(timeout=timeout)

def _http() -> aiohttp.ClientSession:
    if http_session is None or http_session.closed:
        raise RuntimeError("HTTP session is not initialized")
    return http_session

def _mb_headers() -> Dict[str, str]:
    return {"User-Agent": f"{Config.MB_APP} ({Config.MB_CONTACT})"}

# ================= COVER CACHE =================

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

# ================= COVER SOURCES =================

async def cover_from_itunes(artist: str, album: str) -> Optional[str]:
    try:
        s = _http()
        async with s.get(
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
        s = _http()
        q = quote_plus(f'artist:"{artist}" album:"{album}"')
        async with s.get(f"https://api.deezer.com/search/album?q={q}") as r:
            data = await r.json(content_type=None)
            items = data.get("data") or []
            if not items:
                return None
            item = items[0]
            return item.get("cover_xl") or item.get("cover_big") or item.get("cover") or item.get("cover_medium")
    except Exception as e:
        log.debug("deezer cover failed: %s", e)
    return None

async def cover_from_musicbrainz_caa(artist: str, album: str) -> Optional[str]:
    try:
        s = _http()
        q = quote_plus(f'release:"{album}" AND artist:"{artist}"')
        mb_url = f"https://musicbrainz.org/ws/2/release/?query={q}&fmt=json&limit=1"
        async with s.get(mb_url, headers=_mb_headers()) as r:
            data = await r.json(content_type=None)
            rels = data.get("releases") or []
            if not rels:
                return None
            mbid = rels[0].get("id")
            if not mbid:
                return None

        caa_url = f"https://coverartarchive.org/release/{mbid}"
        async with s.get(caa_url) as r:
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
            INSERT INTO ratings (user_id, album_list, rank, rating)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (user_id, album_list, rank)
            DO UPDATE SET rating=EXCLUDED.rating
            """,
            user_id, album_list, rank, rating
        )

# ================= UI =================

def google_link(artist: str, album: str) -> str:
    return f"https://www.google.com/search?q={quote_plus(f'{artist} {album}')}"


def album_keyboard(artist: str, album: str, rated: Optional[int]) -> InlineKeyboardMarkup:
    rate_text = "⭐ Оценить" if not rated else f"⭐ Оценено: {rated}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎧 Найти альбом", url=google_link(artist, album))],
        [
            InlineKeyboardButton(text="Прыдыдущий альбом", callback_data="nav:prev"),
            InlineKeyboardButton(text="Пропустить", callback_data="nav:next")
        ],
        [
            InlineKeyboardButton(text=rate_text, callback_data="ui:rate"),
            InlineKeyboardButton(text="📋 Меню", callback_data="ui:menu")
        ]
    ])


def rating_keyboard(album_list: str, rank: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"rate:{i}:{album_list}:{rank}")
            for i in range(1, 6)
        ],
        [InlineKeyboardButton(text="⬅️ Назад к посту", callback_data="ui:back")]
    ])


def menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить", callback_data="nav:next")],
        [InlineKeyboardButton(text="🔄 Сначала списка", callback_data="nav:reset")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="ui:stats")],
    ])


def album_caption(row: pd.Series, user_rating: Optional[int]) -> str:
    genre = str(row.get("genre", "") or "")
    rating_line = f"\n\n⭐ <b>Ваша оценка:</b> {user_rating}/5" if user_rating else ""
    return (
        f"🏆 <b>#{int(row['rank'])}</b>\n"
        f"🎤 <b>{row['artist']}</b>\n"
        f"💿 <b>{row['album']}</b>\n"
        f"🎧 {genre}"
        f"{rating_line}"
    )

# ================= CORE =================

async def render_album(user_id: int) -> Tuple[Optional[str], str, InlineKeyboardMarkup, str, int, Optional[int], str, str]:
    album_list, idx = await get_user(user_id)
    albums = get_albums(album_list)

    if idx < 0 or idx >= len(albums):
        return None, "📭 Альбомы закончились", InlineKeyboardMarkup(inline_keyboard=[]), album_list, -1, None, "", ""

    row = albums.iloc[idx]
    rank = int(row["rank"])
    artist = str(row["artist"])
    album = str(row["album"])

    user_rating = await get_user_rating(user_id, album_list, rank)
    cover = await get_cover_with_fallback(album_list, rank, artist, album)
    caption = album_caption(row, user_rating)
    kb = album_keyboard(artist, album, user_rating)
    return cover, caption, kb, album_list, rank, user_rating, artist, album


async def send_album_post(user_id: int) -> None:
    cover, caption, kb, album_list, rank, _, artist, album = await render_album(user_id)

    if caption.startswith("📭"):
        await bot.send_message(user_id, caption)
        return

    if cover:
        try:
            await bot.send_photo(user_id, cover, caption=caption, parse_mode="HTML", reply_markup=kb)
            return
        except TelegramBadRequest as e:
            # Telegram не смог скачать картинку по URL (часто из-за временных проблем или блокировок хоста)
            log.warning("Telegram cannot fetch cover URL (list=%s rank=%s url=%s): %s", album_list, rank, cover, e)
            # Убираем кэш, чтобы в следующий раз попробовать другой источник
            await delete_cached_cover(album_list, rank)
            # Фоллбек: отправим без фото, чтобы бот не падал
    await bot.send_message(user_id, caption, parse_mode="HTML", reply_markup=kb)


async def edit_album_post_after_rating(call: CallbackQuery, album_list: str, rank: int, rating: int) -> None:
    albums = get_albums(album_list)
    row = albums.loc[albums["rank"] == rank]
    if row.empty:
        return
    row = row.iloc[0]
    caption = album_caption(row, rating)
    kb = album_keyboard(str(row["artist"]), str(row["album"]), rating)

    try:
        if call.message.photo:
            await call.message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await call.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        log.debug("edit after rating failed: %s", e)

# ================= STATS =================

async def build_stats_text(user_id: int) -> str:
    album_list, _ = await get_user(user_id)
    total = len(get_albums(album_list))

    async with _pool().acquire() as conn:
        rows = await conn.fetch(
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

    dist = {int(r["rating"]): int(r["c"]) for r in rows}
    lines = [f"{i}: {dist.get(i, 0)}" for i in range(1, 6)]
    avg_txt = f"{float(avg):.2f}" if avg is not None else "—"

    return (
        f"📊 <b>Статистика</b>\n\n"
        f"📃 Список: <b>{album_list}</b>\n"
        f"✅ Оценено: <b>{rated_count}</b> из <b>{total}</b>\n"
        f"⭐ Средняя: <b>{avg_txt}</b>\n\n"
        f"Распределение оценок:\n" + "\n".join(lines)
    )

# ================= ERROR HANDLER =================
# В aiogram v3 @router.errors() передаёт один аргумент event.
# Исключение лежит в event.exception

@router.errors()
async def on_error(event):
    exc = getattr(event, "exception", None)
    if exc:
        log.exception("Unhandled error", exc_info=exc)
    else:
        log.exception("Unhandled error (no exception in event)")
    return True

# ================= HANDLERS =================

@router.message(Command("start"))
async def start(msg: Message):
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личные сообщения 🙂")
        return

    await init_http()
    await get_user(msg.from_user.id)
    await send_album_post(msg.from_user.id)

@router.message(Command("menu"))
async def menu_cmd(msg: Message):
    await msg.answer("📋 Меню", reply_markup=menu_keyboard())

@router.callback_query(F.data.startswith("nav:"))
async def nav_cb(call: CallbackQuery):
    album_list, idx = await get_user(call.from_user.id)
    action = call.data.split(":", 1)[1]

    if action == "next":
        await set_index(call.from_user.id, idx - 1)
        await call.answer()
        await send_album_post(call.from_user.id)
        return

    if action == "prev":
        await set_index(call.from_user.id, idx + 1)
        await call.answer()
        await send_album_post(call.from_user.id)
        return

    if action == "reset":
        albums = get_albums(album_list)
        await set_index(call.from_user.id, len(albums) - 1)
        await call.answer("Сброшено")
        await send_album_post(call.from_user.id)
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

@router.callback_query(F.data == "ui:rate")
async def rate_ui(call: CallbackQuery):
    album_list, idx = await get_user(call.from_user.id)
    albums = get_albums(album_list)

    if idx < 0 or idx >= len(albums):
        await call.answer("Альбомы закончились", show_alert=True)
        return

    row = albums.iloc[idx]
    rank = int(row["rank"])
    current_rating = await get_user_rating(call.from_user.id, album_list, rank)
    caption = album_caption(row, current_rating)

    await call.answer()
    try:
        if call.message.photo:
            await call.message.edit_caption(
                caption="Оцени альбом:\n\n" + caption,
                parse_mode="HTML",
                reply_markup=rating_keyboard(album_list, rank),
            )
        else:
            await call.message.edit_text(
                "Оцени альбом:\n\n" + caption,
                parse_mode="HTML",
                reply_markup=rating_keyboard(album_list, rank),
            )
    except Exception as e:
        log.debug("rate ui edit failed: %s", e)
        await bot.send_message(call.from_user.id, "Оцени альбом:", reply_markup=rating_keyboard(album_list, rank))

@router.callback_query(F.data.startswith("rate:"))
async def rate_set(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 4:
        await call.answer("Ошибка кнопки", show_alert=True)
        return

    rating = int(parts[1])
    album_list = parts[2]
    rank = int(parts[3])

    await upsert_rating(call.from_user.id, album_list, rank, rating)
    await call.answer(f"⭐ {rating} сохранено")

    # 1) показать оценку в текущем посте
    await edit_album_post_after_rating(call, album_list, rank, rating)

    # 2) перейти к следующему альбому и отправить его отдельным постом
    albums = get_albums(album_list)
    idx_series = albums.index[albums["rank"] == rank]
    if len(idx_series) == 0:
        return
    idx = int(idx_series[0])

    await set_index(call.from_user.id, idx - 1)
    await send_album_post(call.from_user.id)

@router.callback_query(F.data == "ui:back")
async def back(call: CallbackQuery):
    album_list, idx = await get_user(call.from_user.id)
    albums = get_albums(album_list)
    if idx < 0 or idx >= len(albums):
        await call.answer()
        return

    row = albums.iloc[idx]
    rank = int(row["rank"])
    ur = await get_user_rating(call.from_user.id, album_list, rank)
    caption = album_caption(row, ur)
    kb = album_keyboard(str(row["artist"]), str(row["album"]), ur)

    await call.answer()
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await call.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        log.debug("back edit failed: %s", e)

# ================= START / SHUTDOWN =================

async def on_shutdown() -> None:
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()
    if pg_pool:
        await pg_pool.close()

async def main():
    await init_pg()
    await init_http()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
