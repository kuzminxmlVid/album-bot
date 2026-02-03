import os
import asyncio
import pandas as pd
import aiohttp
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncpg

# ================= CONFIG =================

class Config:
    TOKEN = os.getenv("TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    DEFAULT_LIST = os.getenv("ALBUM_LIST", "top100")
    ALBUMS_DIR = "albums"
    DAILY_HOUR = int(os.getenv("DAILY_HOUR", 10))

if not Config.TOKEN:
    raise RuntimeError("TOKEN not set")
if not Config.DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# ================= GLOBALS =================

bot = Bot(Config.TOKEN)
dp = Dispatcher()
router = Router()
scheduler = AsyncIOScheduler()
pg_pool: asyncpg.Pool | None = None

# ================= DATABASE =================

async def init_pg():
    global pg_pool
    pg_pool = await asyncpg.create_pool(Config.DATABASE_URL)

    async with pg_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            album_list TEXT,
            current_index INTEGER,
            daily INTEGER,
            paused INTEGER
        )
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            user_id BIGINT,
            album_list TEXT,
            rank INTEGER,
            rating INTEGER,
            PRIMARY KEY (user_id, album_list, rank)
        )
        """)

# ================= ALBUMS =================

album_cache = {}

def load_albums(list_name):
    df = pd.read_excel(f"{Config.ALBUMS_DIR}/{list_name}.xlsx")
    return df.sort_values("rank").reset_index(drop=True)

def get_albums(list_name):
    if list_name not in album_cache:
        album_cache[list_name] = load_albums(list_name)
    return album_cache[list_name]

# ================= USERS =================

async def get_user(user_id):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, current_index, daily, paused FROM users WHERE user_id=$1",
            user_id
        )

        if not row:
            albums = get_albums(Config.DEFAULT_LIST)
            index = len(albums) - 1
            await conn.execute(
                "INSERT INTO users VALUES ($1,$2,$3,0,0)",
                user_id, Config.DEFAULT_LIST, index
            )
            return Config.DEFAULT_LIST, index, 0, 0

        return row["album_list"], row["current_index"], row["daily"], row["paused"]

async def set_index(user_id, index):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET current_index=$1 WHERE user_id=$2",
            index, user_id
        )

# ================= COVERS =================

async def get_cover_and_year(artist, album):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                "https://itunes.apple.com/search",
                params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
                timeout=10
            ) as r:
                data = await r.json(content_type=None)
                if data.get("resultCount"):
                    item = data["results"][0]
                    cover = item["artworkUrl100"].replace("100x100", "600x600")
                    year = item.get("releaseDate", "")[:4]
                    return cover, year
        except:
            pass
    return None, None

# ================= UI =================

def google_album_link(artist, album):
    q = quote_plus(f"{artist} {album}")
    return f"https://www.google.com/search?q={q}"

def album_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("🎧 Найти альбом", callback_data="search")],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="prev"),
            InlineKeyboardButton("➡️ Далее", callback_data="next")
        ],
        [
            InlineKeyboardButton("⭐ Оценить", callback_data="rate_menu"),
            InlineKeyboardButton("📋 Меню", callback_data="menu")
        ]
    ])

def rating_keyboard():
    kb = InlineKeyboardMarkup(row_width=5)
    for i in range(1, 6):
        kb.insert(
            InlineKeyboardButton(f"⭐ {i}", callback_data=f"rate:{i}")
        )
    return kb

def menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("▶️ Продолжить", callback_data="menu_resume")],
        [InlineKeyboardButton("⏸ Пауза", callback_data="menu_pause")]
    ])

# ================= CORE =================

async def show_album(user_id):
    album_list, index, _, paused = await get_user(user_id)
    if paused:
        return

    albums = get_albums(album_list)
    if index < 0 or index >= len(albums):
        await bot.send_message(user_id, "📭 Альбомы закончились")
        return

    row = albums.iloc[index]
    artist, album, genre, rank = row["artist"], row["album"], row["genre"], row["rank"]

    cover, year = await get_cover_and_year(artist, album)

    caption = (
        f"🏆 <b>#{rank}</b>\n"
        f"🎤 <b>{artist}</b>\n"
        f"💿 <b>{album}</b>\n"
        f"📅 {year or '—'}\n"
        f"🎧 {genre}"
    )

    if cover:
        await bot.send_photo(user_id, cover, caption=caption,
                             parse_mode="HTML", reply_markup=album_keyboard())
    else:
        await bot.send_message(user_id, caption,
                               parse_mode="HTML", reply_markup=album_keyboard())

# ================= HANDLERS =================

@router.message(Command("start"))
async def start(message: Message):
    if message.chat.type != "private":
        await message.reply("Напиши мне в личные сообщения 🙂")
        return
    await get_user(message.from_user.id)
    await show_album(message.from_user.id)

@router.callback_query(F.data == "next")
async def next_album(call: CallbackQuery):
    album_list, index, *_ = await get_user(call.from_user.id)
    await set_index(call.from_user.id, index - 1)
    await call.answer()
    await show_album(call.from_user.id)

@router.callback_query(F.data == "prev")
async def prev_album(call: CallbackQuery):
    album_list, index, *_ = await get_user(call.from_user.id)
    await set_index(call.from_user.id, index + 1)
    await call.answer()
    await show_album(call.from_user.id)

@router.callback_query(F.data == "rate_menu")
async def rate_menu(call: CallbackQuery):
    await call.message.answer("Оцени альбом:", reply_markup=rating_keyboard())

@router.callback_query(F.data.startswith("rate:"))
async def rate_album(call: CallbackQuery):
    rating = int(call.data.split(":")[1])
    album_list, index, *_ = await get_user(call.from_user.id)
    albums = get_albums(album_list)
    rank = albums.iloc[index]["rank"]

    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ratings VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list, rank) DO UPDATE SET rating=$4",
            call.from_user.id, album_list, rank, rating
        )

    await call.answer(f"⭐ {rating} сохранено")

@router.callback_query(F.data == "search")
async def search_album(call: CallbackQuery):
    album_list, index, *_ = await get_user(call.from_user.id)
    row = get_albums(album_list).iloc[index]
    url = google_album_link(row["artist"], row["album"])
    await call.message.answer(url)
    await call.answer()

@router.callback_query(F.data == "menu")
async def menu(call: CallbackQuery):
    await call.message.answer("📋 Меню", reply_markup=menu_keyboard())
    await call.answer()

# ================= START =================

async def main():
    await init_pg()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
