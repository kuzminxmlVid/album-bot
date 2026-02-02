import os
import sqlite3
import pandas as pd
import requests
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# --- TOKEN ---
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN не задан в переменных окружения")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler()


# --- ALBUMS ---
albums = pd.read_excel("albums.xlsx")

# сортируем по rank и начинаем с последнего
albums = albums.sort_values("rank").reset_index(drop=True)


# --- DB ---
conn = sqlite3.connect("users.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    current_index INTEGER,
    daily INTEGER
)
""")
conn.commit()


# --- HELPERS ---

def generate_song_link(artist, album):
    q = quote_plus(f"{artist} {album}")
    return f"https://song.link/search?q={q}"


def get_cover_and_year(artist, album):
    try:
        r = requests.get(
            "https://itunes.apple.com/search",
            params={
                "term": f"{artist} {album}",
                "entity": "album",
                "limit": 1
            },
            timeout=10
        )
        data = r.json()
        if data["resultCount"] == 0:
            return None, None

        item = data["results"][0]
        cover = item.get("artworkUrl100")
        if cover:
            cover = cover.replace("100x100", "600x600")

        year = item.get("releaseDate", "")[:4]
        return cover, year or None

    except Exception:
        return None, None


def get_user(user_id):
    cursor.execute(
        "SELECT current_index, daily FROM users WHERE user_id=?",
        (user_id,)
    )
    row = cursor.fetchone()

    if row is None:
        start_index = len(albums) - 1
        cursor.execute(
            "INSERT INTO users VALUES (?, ?, ?)",
            (user_id, start_index, 0)
        )
        conn.commit()
        return start_index, 0

    return row


def update_index(user_id, index):
    cursor.execute(
        "UPDATE users SET current_index=? WHERE user_id=?",
        (index, user_id)
    )
    conn.commit()


def set_daily(user_id, value):
    cursor.execute(
        "UPDATE users SET daily=? WHERE user_id=?",
        (value, user_id)
    )
    conn.commit()


def keyboard(artist, album):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(
            "🎧 Слушать на стримингах",
            url=generate_song_link(artist, album)
        ),
        InlineKeyboardButton("➡️ Следующий альбом", callback_data="next"),
        InlineKeyboardButton("📅 Альбом каждый день", callback_data="daily"),
        InlineKeyboardButton("❌ Остановить", callback_data="stop")
    )
    return kb


async def send_album(user_id):
    index, _ = get_user(user_id)

    if index < 0:
        await bot.send_message(user_id, "📭 Альбомы закончились.")
        return

    row = albums.iloc[index]
    artist = row["artist"]
    album_name = row["album"]
    genre = row["genre"]

    cover, year = get_cover_and_year(artist, album_name)

    caption = (
        f"🎤 <b>{artist}</b>\n"
        f"💿 <b>{album_name}</b>\n"
        f"📅 {year or '—'}\n"
        f"🎧 {genre}"
    )

    if cover:
        await bot.send_photo(
            chat_id=user_id,
            photo=cover,
            caption=caption,
            parse_mode="HTML",
            reply_markup=keyboard(artist, album_name)
        )
    else:
        await bot.send_message(
            chat_id=user_id,
            text=caption,
            parse_mode="HTML",
            reply_markup=keyboard(artist, album_name)
        )

    update_index(user_id, index - 1)


# --- HANDLERS ---

@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    get_user(message.from_user.id)
    await message.answer(
        "🎧 Я присылаю альбомы по порядку рейтинга.\n"
        "Можно листать вручную или включить ежедневную подборку."
    )
    await send_album(message.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "next")
async def next_album(call: types.CallbackQuery):
    await call.answer()
    await send_album(call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "daily")
async def daily_on(call: types.CallbackQuery):
    set_daily(call.from_user.id, 1)
    await call.answer("📅 Буду присылать альбом каждый день")


@dp.callback_query_handler(lambda c: c.data == "stop")
async def daily_off(call: types.CallbackQuery):
    set_daily(call.from_user.id, 0)
    await call.answer("❌ Рассылка остановлена")


# --- DAILY JOB ---

async def daily_job():
    cursor.execute("SELECT user_id FROM users WHERE daily=1")
    for (user_id,) in cursor.fetchall():
        await send_album(user_id)


async def on_startup(dp):
    scheduler.add_job(daily_job, "cron", hour=10)
    scheduler.start()


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
