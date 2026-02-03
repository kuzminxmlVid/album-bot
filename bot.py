# TELEGRAM BOT — aiogram 3 + PostgreSQL
# Full rebuild with fixes for aiogram 3 (safe_edit, keyboards)
# UX unchanged

import os
import asyncio
import sqlite3
import pandas as pd
import aiohttp
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import asyncpg

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise RuntimeError("TOKEN not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

ALBUMS_DIR = "albums"
DEFAULT_LIST = os.getenv("ALBUM_LIST", "top100")

# ---------------- BOT ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ---------------- POSTGRES ----------------
pg_pool: asyncpg.Pool | None = None

async def init_pg():
    global pg_pool
    pg_pool = await asyncpg.create_pool(DATABASE_URL)

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

# ---------------- MIGRATION ----------------

async def migrate_from_sqlite():
    if not os.path.exists("users.db"):
        return

    sqlite_conn = sqlite3.connect("users.db")
    sc = sqlite_conn.cursor()

    async with pg_pool.acquire() as pg:
        for row in sc.execute("SELECT user_id, album_list, current_index, daily, paused FROM users"):
            await pg.execute(
                "INSERT INTO users VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                *row
            )

        try:
            for row in sc.execute("SELECT user_id, album_list, rank, rating FROM ratings"):
                await pg.execute(
                    "INSERT INTO ratings VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    *row
                )
        except sqlite3.OperationalError:
            pass

    sqlite_conn.close()
    os.rename("users.db", "users.db.migrated")

# ---------------- ALBUM LISTS ----------------

def load_albums(list_name: str):
    df = pd.read_excel(f"{ALBUMS_DIR}/{list_name}.xlsx")
    return df.sort_values("rank").reset_index(drop=True)

album_cache = {}

def get_albums(list_name):
    if list_name not in album_cache:
        album_cache[list_name] = load_albums(list_name)
    return album_cache[list_name]

# ---------------- USERS ----------------

async def get_user(user_id: int):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, current_index, daily, paused FROM users WHERE user_id=$1",
            user_id
        )

        if row is None:
            albums = get_albums(DEFAULT_LIST)
            start_index = len(albums) - 1
            await conn.execute(
                "INSERT INTO users VALUES ($1,$2,$3,0,0)",
                user_id, DEFAULT_LIST, start_index
            )
            return DEFAULT_LIST, start_index, 0, 0

        return row["album_list"], row["current_index"], row["daily"], row["paused"]


async def update_index(user_id, index):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET current_index=$1 WHERE user_id=$2",
            index, user_id
        )


async def set_album_list(user_id, list_name):
    albums = get_albums(list_name)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET album_list=$1, current_index=$2 WHERE user_id=$3",
            list_name, len(albums) - 1, user_id
        )


async def set_paused(user_id, value):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET paused=$1 WHERE user_id=$2",
            value, user_id
        )

# ---------------- RATINGS ----------------

async def set_rating(user_id, album_list, rank, rating):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ratings VALUES ($1,$2,$3,$4) ON CONFLICT (user_id,album_list,rank) DO UPDATE SET rating=$4",
            user_id, album_list, rank, rating
        )


async def get_rating(user_id, album_list, rank):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT rating FROM ratings WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        return row["rating"] if row else None

# ---------------- COVERS ----------------

async def itunes_cover(session, artist, album):
    try:
        async with session.get(
            "https://itunes.apple.com/search",
            params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        ) as r:
            if r.status != 200:
                return None, None
            data = await r.json(content_type=None)

        if data.get("resultCount", 0) == 0:
            return None, None

        item = data["results"][0]
        cover = item.get("artworkUrl100")
        if cover:
            cover = cover.replace("100x100", "600x600")
        year = item.get("releaseDate", "")[:4]
        return cover, year or None
    except Exception:
        return None, None


async def deezer_cover(session, artist, album):
    try:
        async with session.get(
            "https://api.deezer.com/search/album",
            params={"q": f"{artist} {album}"},
            timeout=10
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()

        if not data.get("data"):
            return None

        return data["data"][0].get("cover_xl")
    except Exception:
        return None


async def get_cover_and_year(session, artist, album):
    cover, year = await itunes_cover(session, artist, album)
    if cover:
        return cover, year

    cover = await deezer_cover(session, artist, album)
    if cover:
        return cover, None

    return None, None

# ---------------- UI HELPERS ----------------

def google_album_link(artist, album):
    return f"https://www.google.com/search?q={quote_plus(f'{artist} {album}')}"


def rating_keyboard(album_list, rank):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=str(i),
                    callback_data=f"rate:{album_list}:{rank}:{i}"
                ) for i in range(1, 6)
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")]
        ]
    )


def album_keyboard(artist, album):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Найти альбом", url=google_album_link(artist, album))],
            [InlineKeyboardButton(text="⭐ Оценить", callback_data="rate_menu")],
            [InlineKeyboardButton(text="➡️ Следующий альбом", callback_data="next")],
            [InlineKeyboardButton(text="📋 Меню", callback_data="menu")]
        ]
    )


def menu_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📚 Списки альбомов", callback_data="menu_lists")],
            [InlineKeyboardButton(text="▶️ Продолжить", callback_data="menu_resume")],
            [InlineKeyboardButton(text="⏸ Пауза", callback_data="menu_pause")]
        ]
    )


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None):
    msg = call.message
    try:
        if msg.text:
            await msg.edit_text(text, reply_markup=reply_markup)
        elif msg.caption:
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await msg.answer(text, reply_markup=reply_markup)
    except Exception:
        await msg.answer(text, reply_markup=reply_markup)

# ---------------- CORE ----------------

async def send_album(chat_id, user_id):
    album_list, index, _, paused = await get_user(user_id)
    if paused:
        return

    albums = get_albums(album_list)
    if index < 0:
        await bot.send_message(chat_id, "📭 Альбомы закончились.")
        return

    row = albums.iloc[index]
    artist = row["artist"]
    album = row["album"]
    genre = row["genre"]
    rank = row["rank"]

    total = len(albums)
    progress = total - index
    rating = await get_rating(user_id, album_list, rank)

    async with aiohttp.ClientSession() as session:
        cover, year = await get_cover_and_year(session, artist, album)

    caption = (
        f"🏆 <b>#{rank}</b>\n"
        f"🎤 <b>{artist}</b>\n"
        f"💿 <b>{album}</b>\n"
        f"📅 {year or '—'}\n"
        f"🎧 {genre}\n"
        f"📊 Прогресс: {progress}/{total}\n"
        f"⭐ Ваша оценка: {rating if rating else '—'}"
    )

    if cover:
        await bot.send_photo(chat_id, cover, caption=caption, parse_mode="HTML", reply_markup=album_keyboard(artist, album))
    else:
        await bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=album_keyboard(artist, album))

    await update_index(user_id, index - 1)

# ---------------- HANDLERS ----------------

@dp.message(Command("start", "menu"))
async def start(message: Message):
    await get_user(message.from_user.id)
    await message.answer("📋 Главное меню", reply_markup=menu_keyboard())


@dp.callback_query(F.data == "menu")
async def menu(call: CallbackQuery):
    await safe_edit(call, "📋 Главное меню", menu_keyboard())


@dp.callback_query(F.data == "menu_pause")
async def pause(call: CallbackQuery):
    await set_paused(call.from_user.id, 1)
    await call.answer("⏸ Пауза")


@dp.callback_query(F.data == "menu_resume")
async def resume(call: CallbackQuery):
    await set_paused(call.from_user.id, 0)
    await call.answer("▶️ Продолжаем")
    await send_album(call.message.chat.id, call.from_user.id)


@dp.callback_query(F.data == "menu_lists")
async def lists_menu(call: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f.replace('.xlsx',''), callback_data=f"list:{f.replace('.xlsx','')}")]
            for f in os.listdir(ALBUMS_DIR) if f.endswith(".xlsx")
        ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")]]
    )

    await safe_edit(call, "📚 Выбери список альбомов:", kb)


@dp.callback_query(F.data.startswith("list:"))
async def set_list(call: CallbackQuery):
    list_name = call.data.split(":", 1)[1]
    await set_album_list(call.from_user.id, list_name)
    await call.answer(f"Список: {list_name}")
    await send_album(call.message.chat.id, call.from_user.id)


@dp.callback_query(F.data == "next")
async def next_album(call: CallbackQuery):
    await call.answer()
    await send_album(call.message.chat.id, call.from_user.id)


@dp.callback_query(F.data == "rate_menu")
async def rate_menu(call: CallbackQuery):
    album_list, index, _, _ = await get_user(call.from_user.id)
    albums = get_albums(album_list)
    row = albums.iloc[index + 1]
    await call.message.answer(
        "⭐ Поставь оценку:",
        reply_markup=rating_keyboard(album_list, row["rank"])
    )


@dp.callback_query(F.data.startswith("rate:"))
async def rate(call: CallbackQuery):
    _, album_list, rank, rating = call.data.split(":")
    await set_rating(call.from_user.id, album_list, int(rank), int(rating))
    await call.answer(f"Оценка: {rating} ⭐")

# ---------------- DAILY ----------------

async def daily_job():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users WHERE daily=1 AND paused=0")
        for r in rows:
            await send_album(r["user_id"], r["user_id"])


async def on_startup():
    await init_pg()
    await migrate_from_sqlite()
    scheduler.add_job(daily_job, "cron", hour=10)
    scheduler.start()


async def main():
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
