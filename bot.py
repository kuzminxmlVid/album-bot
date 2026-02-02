# UPDATED TELEGRAM BOT
# Variant B: iTunes -> Deezer fallback for album covers
# + /menu command with navigation

import os
import sqlite3
import pandas as pd
import aiohttp
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN не задан в переменных окружения")

ALBUMS_DIR = "albums"
DEFAULT_LIST = os.getenv("ALBUM_LIST", "top100")

# ---------------- BOT ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler()

# ---------------- DB ----------------
conn = sqlite3.connect("users.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    album_list TEXT,
    current_index INTEGER,
    daily INTEGER,
    paused INTEGER
)
""")
conn.commit()

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

def get_user(user_id):
    cursor.execute(
        "SELECT album_list, current_index, daily, paused FROM users WHERE user_id=?",
        (user_id,)
    )
    row = cursor.fetchone()

    if row is None:
        albums = get_albums(DEFAULT_LIST)
        start_index = len(albums) - 1
        cursor.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            (user_id, DEFAULT_LIST, start_index, 0, 0)
        )
        conn.commit()
        return DEFAULT_LIST, start_index, 0, 0

    return row


def update_index(user_id, index):
    cursor.execute(
        "UPDATE users SET current_index=? WHERE user_id=?",
        (index, user_id)
    )
    conn.commit()


def set_album_list(user_id, list_name):
    albums = get_albums(list_name)
    cursor.execute(
        "UPDATE users SET album_list=?, current_index=? WHERE user_id=?",
        (list_name, len(albums) - 1, user_id)
    )
    conn.commit()


def set_paused(user_id, value):
    cursor.execute(
        "UPDATE users SET paused=? WHERE user_id=?",
        (value, user_id)
    )
    conn.commit()

# ---------------- COVER PROVIDERS ----------------

async def itunes_cover(session, artist, album):
    try:
        async with session.get(
            "https://itunes.apple.com/search",
            params={"term": f"{artist} {album}", "entity": "album", "limit": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=10)
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
            timeout=aiohttp.ClientTimeout(total=10)
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

# ---------------- UI ----------------

def artist_google_link(artist):
    return f"https://www.google.com/search?q={quote_plus(artist)}"


def album_keyboard(artist):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔎 Искать исполнителя", url=artist_google_link(artist)),
        InlineKeyboardButton("➡️ Следующий альбом", callback_data="next"),
        InlineKeyboardButton("📅 Альбом каждый день", callback_data="daily"),
        InlineKeyboardButton("📋 Меню", callback_data="menu")
    )
    return kb


def menu_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📚 Списки альбомов", callback_data="menu_lists"),
        InlineKeyboardButton("▶️ Продолжить", callback_data="menu_resume"),
        InlineKeyboardButton("⏸ Пауза", callback_data="menu_pause")
    )
    return kb

# ---------------- CORE ----------------

async def send_album(user_id):
    album_list, index, _, paused = get_user(user_id)
    if paused:
        return

    albums = get_albums(album_list)
    if index < 0:
        await bot.send_message(user_id, "📭 Альбомы закончились.")
        return

    row = albums.iloc[index]
    artist = row["artist"]
    album = row["album"]
    genre = row["genre"]
    rank = row["rank"]

    total = len(albums)
    progress = total - index

    async with aiohttp.ClientSession() as session:
        cover, year = await get_cover_and_year(session, artist, album)

    caption = (
        f"🏆 <b>#{rank}</b>\n"
        f"🎤 <b>{artist}</b>\n"
        f"💿 <b>{album}</b>\n"
        f"📅 {year or '—'}\n"
        f"🎧 {genre}\n"
        f"📊 Прогресс: {progress}/{total}"
    )

    if cover:
        await bot.send_photo(user_id, cover, caption=caption, parse_mode="HTML", reply_markup=album_keyboard(artist))
    else:
        await bot.send_message(user_id, caption, parse_mode="HTML", reply_markup=album_keyboard(artist))

    update_index(user_id, index - 1)

# ---------------- COMMANDS ----------------

@dp.message_handler(commands=["start", "menu"])
async def start(message: types.Message):
    get_user(message.from_user.id)
    await message.answer("📋 Главное меню", reply_markup=menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "menu")
async def menu(call: types.CallbackQuery):
    await call.message.edit_text("📋 Главное меню", reply_markup=menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "menu_pause")
async def pause(call: types.CallbackQuery):
    set_paused(call.from_user.id, 1)
    await call.answer("⏸ Пауза")


@dp.callback_query_handler(lambda c: c.data == "menu_resume")
async def resume(call: types.CallbackQuery):
    set_paused(call.from_user.id, 0)
    await call.answer("▶️ Продолжаем")
    await send_album(call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "menu_lists")
async def lists_menu(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(row_width=1)
    for f in os.listdir(ALBUMS_DIR):
        if f.endswith(".xlsx"):
            name = f.replace(".xlsx", "")
            kb.add(InlineKeyboardButton(name, callback_data=f"list:{name}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="menu"))

    await call.message.edit_text("📚 Выбери список альбомов:", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("list:"))
async def set_list(call: types.CallbackQuery):
    list_name = call.data.split(":", 1)[1]
    set_album_list(call.from_user.id, list_name)
    await call.answer(f"Список: {list_name}")
    await send_album(call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "next")
async def next_album(call: types.CallbackQuery):
    await call.answer()
    await send_album(call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "daily")
async def daily_on(call: types.CallbackQuery):
    cursor.execute("UPDATE users SET daily=1 WHERE user_id=?", (call.from_user.id,))
    conn.commit()
    await call.answer("📅 Daily включён")

# ---------------- DAILY ----------------

async def daily_job():
    cursor.execute("SELECT user_id FROM users WHERE daily=1 AND paused=0")
    for (user_id,) in cursor.fetchall():
        await send_album(user_id)


async def on_startup(dp):
    scheduler.add_job(daily_job, "cron", hour=10)
    scheduler.start()


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
