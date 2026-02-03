import os
import asyncio
import pandas as pd
import aiohttp
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import asyncpg

# ================= CONFIG =================

class Config:
    TOKEN = os.getenv("TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    DEFAULT_LIST = os.getenv("ALBUM_LIST", "top100")
    ALBUMS_DIR = "albums"

if not Config.TOKEN or not Config.DATABASE_URL:
    raise RuntimeError("ENV vars not set")

# ================= BOT =================

bot = Bot(token=Config.TOKEN)
dp = Dispatcher()
router = Router()
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
            current_index INTEGER
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

def load_albums(name):
    df = pd.read_excel(f"{Config.ALBUMS_DIR}/{name}.xlsx")
    return df.sort_values("rank").reset_index(drop=True)

def get_albums(name):
    if name not in album_cache:
        album_cache[name] = load_albums(name)
    return album_cache[name]

# ================= USERS =================

async def get_user(user_id):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, current_index FROM users WHERE user_id=$1",
            user_id
        )
        if not row:
            albums = get_albums(Config.DEFAULT_LIST)
            idx = len(albums) - 1
            await conn.execute(
                "INSERT INTO users VALUES ($1,$2,$3)",
                user_id, Config.DEFAULT_LIST, idx
            )
            return Config.DEFAULT_LIST, idx
        return row["album_list"], row["current_index"]

async def set_index(user_id, idx):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET current_index=$1 WHERE user_id=$2",
            idx, user_id
        )

# ================= COVER =================

async def get_cover(artist, album):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://itunes.apple.com/search",
                params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
                timeout=10
            ) as r:
                data = await r.json(content_type=None)
                if data.get("resultCount"):
                    return data["results"][0]["artworkUrl100"].replace("100x100", "600x600")
    except:
        pass
    return None

# ================= UI =================

def google_link(a, al):
    return f"https://www.google.com/search?q={quote_plus(f'{a} {al}')}"

def album_keyboard(a, al):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎧 Найти альбом", url=google_link(a, al))],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="prev"),
            InlineKeyboardButton(text="➡️ Далее", callback_data="next")
        ],
        [
            InlineKeyboardButton(text="⭐ Оценить", callback_data="rate"),
            InlineKeyboardButton(text="📋 Меню", callback_data="menu")
        ]
    ])

def rating_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"rate:{i}")
            for i in range(1, 6)
        ],
        [InlineKeyboardButton(text="⬅️ К альбому", callback_data="back")]
    ])

def menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить", callback_data="next")]
    ])

# ================= CORE =================

async def send_album(user_id):
    album_list, idx = await get_user(user_id)
    albums = get_albums(album_list)

    if idx < 0 or idx >= len(albums):
        await bot.send_message(user_id, "📭 Альбомы закончились")
        return

    row = albums.iloc[idx]
    cover = await get_cover(row["artist"], row["album"])

    caption = (
        f"🏆 <b>#{row['rank']}</b>\n"
        f"🎤 <b>{row['artist']}</b>\n"
        f"💿 <b>{row['album']}</b>\n"
        f"🎧 {row['genre']}"
    )

    if cover:
        await bot.send_photo(
            user_id, cover,
            caption=caption,
            parse_mode="HTML",
            reply_markup=album_keyboard(row["artist"], row["album"])
        )
    else:
        await bot.send_message(
            user_id, caption,
            parse_mode="HTML",
            reply_markup=album_keyboard(row["artist"], row["album"])
        )

# ================= HANDLERS =================

@router.message(Command("start"))
async def start(msg: Message):
    if msg.chat.type != "private":
        await msg.reply("Напиши мне в личные сообщения 🙂")
        return
    await get_user(msg.from_user.id)
    await send_album(msg.from_user.id)

@router.callback_query(F.data == "next")
async def next_cb(call: CallbackQuery):
    _, idx = await get_user(call.from_user.id)
    await set_index(call.from_user.id, idx - 1)
    await call.answer()
    await send_album(call.from_user.id)

@router.callback_query(F.data == "prev")
async def prev_cb(call: CallbackQuery):
    _, idx = await get_user(call.from_user.id)
    await set_index(call.from_user.id, idx + 1)
    await call.answer()
    await send_album(call.from_user.id)

@router.callback_query(F.data == "menu")
async def menu(call: CallbackQuery):
    await call.answer()
    await bot.send_message(call.from_user.id, "📋 Меню", reply_markup=menu_keyboard())

@router.callback_query(F.data == "rate")
async def rate(call: CallbackQuery):
    await call.answer()
    await bot.send_message(call.from_user.id, "Оцени альбом:", reply_markup=rating_keyboard())

@router.callback_query(F.data.startswith("rate:"))
async def rate_set(call: CallbackQuery):
    rating = int(call.data.split(":")[1])
    album_list, idx = await get_user(call.from_user.id)
    rank = get_albums(album_list).iloc[idx]["rank"]

    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ratings VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (user_id, album_list, rank) DO UPDATE SET rating=$4",
            call.from_user.id, album_list, rank, rating
        )

    await call.answer(f"⭐ {rating} сохранено")
    await send_album(call.from_user.id)

@router.callback_query(F.data == "back")
async def back(call: CallbackQuery):
    await call.answer()
    await send_album(call.from_user.id)

# ================= START =================

async def main():
    await init_pg()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
