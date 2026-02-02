{\rtf1\ansi\ansicpg1251\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red255\green255\blue255;}
{\*\expandedcolortbl;;\csgray\c0;\cssrgb\c100000\c100000\c100000;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs26 \cf2 \expnd0\expndtw0\kerning0
import os\
import sqlite3\
import pandas as pd\
import requests\
from urllib.parse import quote_plus\
\
from aiogram import Bot, Dispatcher, executor, types\
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton\
from apscheduler.schedulers.asyncio import AsyncIOScheduler\
\
\
# --- TOKEN ---\
TOKEN = os.getenv("TOKEN")\
if not TOKEN:\
    raise RuntimeError("TOKEN \uc0\u1085 \u1077  \u1079 \u1072 \u1076 \u1072 \u1085  \u1074  \u1087 \u1077 \u1088 \u1077 \u1084 \u1077 \u1085 \u1085 \u1099 \u1093  \u1086 \u1082 \u1088 \u1091 \u1078 \u1077 \u1085 \u1080 \u1103 ")\
\
bot = Bot(token=TOKEN)\
dp = Dispatcher(bot)\
scheduler = AsyncIOScheduler()\
\
\
# --- ALBUMS ---\
albums = pd.read_excel("albums.xlsx")\
\
# \uc0\u1089 \u1086 \u1088 \u1090 \u1080 \u1088 \u1091 \u1077 \u1084  \u1087 \u1086  rank \u1080  \u1085 \u1072 \u1095 \u1080 \u1085 \u1072 \u1077 \u1084  \u1089  \u1087 \u1086 \u1089 \u1083 \u1077 \u1076 \u1085 \u1077 \u1075 \u1086 \
albums = albums.sort_values("rank").reset_index(drop=True)\
\
\
# --- DB ---\
conn = sqlite3.connect("users.db", check_same_thread=False)\
cursor = conn.cursor()\
\
cursor.execute("""\
CREATE TABLE IF NOT EXISTS users (\
    user_id INTEGER PRIMARY KEY,\
    current_index INTEGER,\
    daily INTEGER\
)\
""")\
conn.commit()\
\
\
# --- HELPERS ---\
\
def generate_song_link(artist, album):\
    q = quote_plus(f"\{artist\} \{album\}")\
    return f"https://song.link/search?q=\{q\}"\
\
\
def get_cover_and_year(artist, album):\
    try:\
        r = requests.get(\
            "https://itunes.apple.com/search",\
            params=\{\
                "term": f"\{artist\} \{album\}",\
                "entity": "album",\
                "limit": 1\
            \},\
            timeout=10\
        )\
        data = r.json()\
        if data["resultCount"] == 0:\
            return None, None\
\
        item = data["results"][0]\
        cover = item.get("artworkUrl100")\
        if cover:\
            cover = cover.replace("100x100", "600x600")\
\
        year = item.get("releaseDate", "")[:4]\
        return cover, year or None\
\
    except Exception:\
        return None, None\
\
\
def get_user(user_id):\
    cursor.execute(\
        "SELECT current_index, daily FROM users WHERE user_id=?",\
        (user_id,)\
    )\
    row = cursor.fetchone()\
\
    if row is None:\
        start_index = len(albums) - 1\
        cursor.execute(\
            "INSERT INTO users VALUES (?, ?, ?)",\
            (user_id, start_index, 0)\
        )\
        conn.commit()\
        return start_index, 0\
\
    return row\
\
\
def update_index(user_id, index):\
    cursor.execute(\
        "UPDATE users SET current_index=? WHERE user_id=?",\
        (index, user_id)\
    )\
    conn.commit()\
\
\
def set_daily(user_id, value):\
    cursor.execute(\
        "UPDATE users SET daily=? WHERE user_id=?",\
        (value, user_id)\
    )\
    conn.commit()\
\
\
def keyboard(artist, album):\
    kb = InlineKeyboardMarkup(row_width=1)\
    kb.add(\
        InlineKeyboardButton(\
            "\uc0\u55356 \u57255  \u1057 \u1083 \u1091 \u1096 \u1072 \u1090 \u1100  \u1085 \u1072  \u1089 \u1090 \u1088 \u1080 \u1084 \u1080 \u1085 \u1075 \u1072 \u1093 ",\
            url=generate_song_link(artist, album)\
        ),\
        InlineKeyboardButton("\uc0\u10145 \u65039  \u1057 \u1083 \u1077 \u1076 \u1091 \u1102 \u1097 \u1080 \u1081  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084 ", callback_data="next"),\
        InlineKeyboardButton("\uc0\u55357 \u56517  \u1040 \u1083 \u1100 \u1073 \u1086 \u1084  \u1082 \u1072 \u1078 \u1076 \u1099 \u1081  \u1076 \u1077 \u1085 \u1100 ", callback_data="daily"),\
        InlineKeyboardButton("\uc0\u10060  \u1054 \u1089 \u1090 \u1072 \u1085 \u1086 \u1074 \u1080 \u1090 \u1100 ", callback_data="stop")\
    )\
    return kb\
\
\
async def send_album(user_id):\
    index, _ = get_user(user_id)\
\
    if index < 0:\
        await bot.send_message(user_id, "\uc0\u55357 \u56557  \u1040 \u1083 \u1100 \u1073 \u1086 \u1084 \u1099  \u1079 \u1072 \u1082 \u1086 \u1085 \u1095 \u1080 \u1083 \u1080 \u1089 \u1100 .")\
        return\
\
    row = albums.iloc[index]\
    artist = row["artist"]\
    album_name = row["album"]\
    genre = row["genre"]\
\
    cover, year = get_cover_and_year(artist, album_name)\
\
    caption = (\
        f"\uc0\u55356 \u57252  <b>\{artist\}</b>\\n"\
        f"\uc0\u55357 \u56511  <b>\{album_name\}</b>\\n"\
        f"\uc0\u55357 \u56517  \{year or '\'97'\}\\n"\
        f"\uc0\u55356 \u57255  \{genre\}"\
    )\
\
    if cover:\
        await bot.send_photo(\
            chat_id=user_id,\
            photo=cover,\
            caption=caption,\
            parse_mode="HTML",\
            reply_markup=keyboard(artist, album_name)\
        )\
    else:\
        await bot.send_message(\
            chat_id=user_id,\
            text=caption,\
            parse_mode="HTML",\
            reply_markup=keyboard(artist, album_name)\
        )\
\
    update_index(user_id, index - 1)\
\
\
# --- HANDLERS ---\
\
@dp.message_handler(commands=["start"])\
async def start(message: types.Message):\
    get_user(message.from_user.id)\
    await message.answer(\
        "\uc0\u55356 \u57255  \u1071  \u1087 \u1088 \u1080 \u1089 \u1099 \u1083 \u1072 \u1102  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084 \u1099  \u1087 \u1086  \u1087 \u1086 \u1088 \u1103 \u1076 \u1082 \u1091  \u1088 \u1077 \u1081 \u1090 \u1080 \u1085 \u1075 \u1072 .\\n"\
        "\uc0\u1052 \u1086 \u1078 \u1085 \u1086  \u1083 \u1080 \u1089 \u1090 \u1072 \u1090 \u1100  \u1074 \u1088 \u1091 \u1095 \u1085 \u1091 \u1102  \u1080 \u1083 \u1080  \u1074 \u1082 \u1083 \u1102 \u1095 \u1080 \u1090 \u1100  \u1077 \u1078 \u1077 \u1076 \u1085 \u1077 \u1074 \u1085 \u1091 \u1102  \u1087 \u1086 \u1076 \u1073 \u1086 \u1088 \u1082 \u1091 ."\
    )\
    await send_album(message.from_user.id)\
\
\
@dp.callback_query_handler(lambda c: c.data == "next")\
async def next_album(call: types.CallbackQuery):\
    await call.answer()\
    await send_album(call.from_user.id)\
\
\
@dp.callback_query_handler(lambda c: c.data == "daily")\
async def daily_on(call: types.CallbackQuery):\
    set_daily(call.from_user.id, 1)\
    await call.answer("\uc0\u55357 \u56517  \u1041 \u1091 \u1076 \u1091  \u1087 \u1088 \u1080 \u1089 \u1099 \u1083 \u1072 \u1090 \u1100  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084  \u1082 \u1072 \u1078 \u1076 \u1099 \u1081  \u1076 \u1077 \u1085 \u1100 ")\
\
\
@dp.callback_query_handler(lambda c: c.data == "stop")\
async def daily_off(call: types.CallbackQuery):\
    set_daily(call.from_user.id, 0)\
    await call.answer("\uc0\u10060  \u1056 \u1072 \u1089 \u1089 \u1099 \u1083 \u1082 \u1072  \u1086 \u1089 \u1090 \u1072 \u1085 \u1086 \u1074 \u1083 \u1077 \u1085 \u1072 ")\
\
\
# --- DAILY JOB ---\
\
async def daily_job():\
    cursor.execute("SELECT user_id FROM users WHERE daily=1")\
    for (user_id,) in cursor.fetchall():\
        await send_album(user_id)\
\
\
async def on_startup(dp):\
    scheduler.add_job(daily_job, "cron", hour=10)\
    scheduler.start()\
\
\
if __name__ == "__main__":\
    executor.start_polling(dp, on_startup=on_startup)\
}