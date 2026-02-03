import os
import asyncio
import logging
from urllib.parse import quote_plus
from typing import Optional, Tuple

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
    InputMediaPhoto,
)

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
    # Make path stable on Railway / Docker
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ALBUMS_DIR = os.getenv("ALBUMS_DIR", os.path.join(BASE_DIR, "albums"))

if not Config.TOKEN or not Config.DATABASE_URL:
    raise RuntimeError("ENV vars not set: TOKEN and/or DATABASE_URL")

# asyncpg is picky about scheme in some environments
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

def _pool() -> asyncpg.Pool:
    if pg_pool is None:
        raise RuntimeError("Postgres pool is not initialized")
    return pg_pool

# ================= ALBUMS =================

album_cache: dict[str, pd.DataFrame] = {}

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

    # Normalize rank to int-ish and sort
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
            # Start from the best (rank 1) OR from the end? In your original code you started from the end.
            # Keep original behavior: start from the last row, then 'next' goes backwards.
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

# ================= COVER =================

async def init_http() -> None:
    global http_session
    if http_session is None or http_session.closed:
        timeout = aiohttp.ClientTimeout(total=12)
        http_session = aiohttp.ClientSession(timeout=timeout)

def _http() -> aiohttp.ClientSession:
    if http_session is None or http_session.closed:
        raise RuntimeError("HTTP session is not initialized")
    return http_session

async def get_cover(artist: str, album: str) -> Optional[str]:
    """
    Fetch cover URL from iTunes Search API.
    We reuse one ClientSession to avoid opening sockets on every request.
    """
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
        log.debug("cover fetch failed: %s", e)
    return None

# ================= UI =================

def google_link(artist: str, album: str) -> str:
    return f"https://www.google.com/search?q={quote_plus(f'{artist} {album}')}"

def album_keyboard(artist: str, album: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎧 Найти альбом", url=google_link(artist, album))],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:prev"),
            InlineKeyboardButton(text="➡️ Далее", callback_data="nav:next")
        ],
        [
            InlineKeyboardButton(text="⭐ Оценить", callback_data="ui:rate"),
            InlineKeyboardButton(text="📋 Меню", callback_data="ui:menu")
        ]
    ])

def rating_keyboard(album_list: str, rank: int) -> InlineKeyboardMarkup:
    # Embed list + rank so rating never "slips" if user navigated and then clicks old buttons.
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"rate:{i}:{album_list}:{rank}")
            for i in range(1, 6)
        ],
        [InlineKeyboardButton(text="⬅️ К альбому", callback_data="ui:back")]
    ])

def menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить", callback_data="nav:next")],
        [InlineKeyboardButton(text="🔄 Сначала списка", callback_data="nav:reset")],
    ])

def album_caption(row: pd.Series) -> str:
    genre = str(row.get("genre", "") or "")
    return (
        f"🏆 <b>#{int(row['rank'])}</b>\n"
        f"🎤 <b>{row['artist']}</b>\n"
        f"💿 <b>{row['album']}</b>\n"
        f"🎧 {genre}"
    )

# ================= CORE =================

async def render_album(user_id: int) -> Tuple[Optional[str], str, InlineKeyboardMarkup, str, int]:
    """
    Returns (cover_url, caption_html, keyboard, album_list, rank)
    """
    album_list, idx = await get_user(user_id)
    albums = get_albums(album_list)

    if idx < 0 or idx >= len(albums):
        return None, "📭 Альбомы закончились", InlineKeyboardMarkup(inline_keyboard=[]), album_list, -1

    row = albums.iloc[idx]
    cover = await get_cover(str(row["artist"]), str(row["album"]))
    caption = album_caption(row)
    rank = int(row["rank"])
    kb = album_keyboard(str(row["artist"]), str(row["album"]))
    return cover, caption, kb, album_list, rank

async def show_album(user_id: int, message: Optional[Message] = None) -> None:
    """
    Try to keep ONE message by editing the existing one (if possible).
    If edit fails (or message is None), fall back to sending a new message.
    """
    cover, caption, kb, _, _ = await render_album(user_id)

    # End of list
    if caption.startswith("📭"):
        if message:
            try:
                # If it was a photo message, edit caption; otherwise edit text
                if message.photo:
                    await message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=None)
                else:
                    await message.edit_text(caption, parse_mode="HTML", reply_markup=None)
                return
            except Exception:
                pass
        await bot.send_message(user_id, caption)
        return

    if message:
        try:
            if cover:
                media = InputMediaPhoto(media=cover, caption=caption, parse_mode="HTML")
                if message.photo:
                    await message.edit_media(media=media, reply_markup=kb)
                else:
                    # Current message is text, but new is photo -> replace by sending new
                    raise RuntimeError("cannot edit text message into photo safely")
            else:
                # No cover -> keep it as text message or edit caption if it was photo
                if message.photo:
                    await message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
                else:
                    await message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
            return
        except Exception as e:
            log.debug("edit failed, fallback to send: %s", e)

    # Fallback send
    if cover:
        await bot.send_photo(user_id, cover, caption=caption, parse_mode="HTML", reply_markup=kb)
    else:
        await bot.send_message(user_id, caption, parse_mode="HTML", reply_markup=kb)

# ================= HANDLERS =================

@router.message(Command("start"))
async def start(msg: Message):
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личные сообщения 🙂")
        return

    await init_http()
    await get_user(msg.from_user.id)
    await show_album(msg.from_user.id)

@router.message(Command("menu"))
async def menu_cmd(msg: Message):
    await msg.answer("📋 Меню", reply_markup=menu_keyboard())

@router.callback_query(F.data.startswith("nav:"))
async def nav_cb(call: CallbackQuery):
    _, idx = await get_user(call.from_user.id)
    action = call.data.split(":", 1)[1]

    if action == "next":
        await set_index(call.from_user.id, idx - 1)
    elif action == "prev":
        await set_index(call.from_user.id, idx + 1)
    elif action == "reset":
        album_list, _ = await get_user(call.from_user.id)
        albums = get_albums(album_list)
        await set_index(call.from_user.id, len(albums) - 1)

    await call.answer()
    await show_album(call.from_user.id, message=call.message)

@router.callback_query(F.data == "ui:menu")
async def menu_cb(call: CallbackQuery):
    await call.answer()
    await call.message.answer("📋 Меню", reply_markup=menu_keyboard())

@router.callback_query(F.data == "ui:rate")
async def rate_ui(call: CallbackQuery):
    # Show rating UI on the SAME message (caption/text) to avoid spam
    _, idx = await get_user(call.from_user.id)
    album_list, _ = await get_user(call.from_user.id)
    row = get_albums(album_list).iloc[idx]
    rank = int(row["rank"])
    caption = album_caption(row)

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
    # rate:{rating}:{album_list}:{rank}
    parts = call.data.split(":")
    if len(parts) != 4:
        await call.answer("Ошибка кнопки", show_alert=True)
        return

    rating = int(parts[1])
    album_list = parts[2]
    rank = int(parts[3])

    async with _pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ratings (user_id, album_list, rank, rating)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (user_id, album_list, rank)
            DO UPDATE SET rating=EXCLUDED.rating
            """,
            call.from_user.id, album_list, rank, rating
        )

    await call.answer(f"⭐ {rating} сохранено")
    # After rating, move to next album automatically
    _, idx = await get_user(call.from_user.id)
    await set_index(call.from_user.id, idx - 1)
    await show_album(call.from_user.id, message=call.message)

@router.callback_query(F.data == "ui:back")
async def back(call: CallbackQuery):
    await call.answer()
    await show_album(call.from_user.id, message=call.message)

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
