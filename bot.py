{\rtf1\ansi\ansicpg1251\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red255\green255\blue255;}
{\*\expandedcolortbl;;\csgray\c0;\cssrgb\c100000\c100000\c100000;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs26 \cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec3 import sqlite3\
import pandas as pd\
from urllib.parse import quote_plus\
\
from aiogram import Bot, Dispatcher, executor, types\
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton\
from apscheduler.schedulers.asyncio import AsyncIOScheduler\
\
import os\
TOKEN = os.getenv("8371822015:AAE_3c4AtWc8t8VCDl_0dj3xQOMAAkkQFRw")\
\
bot = Bot(token=TOKEN)\
dp = Dispatcher(bot)\
scheduler = AsyncIOScheduler()\
\
# --- \uc0\u1040 \u1083 \u1100 \u1073 \u1086 \u1084 \u1099  ---\
albums = pd.read_excel("albums.xlsx")\
\
# --- \uc0\u1041 \u1044  ---\
conn = sqlite3.connect("users.db")\
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
# --- \uc0\u1042 \u1089 \u1087 \u1086 \u1084 \u1086 \u1075 \u1072 \u1090 \u1077 \u1083 \u1100 \u1085 \u1099 \u1077  \u1092 \u1091 \u1085 \u1082 \u1094 \u1080 \u1080  ---\
\
def generate_song_link(artist, album):\
    query = f"\{artist\} \{album\}"\
    return f"https://song.link/search?q=\{quote_plus(query)\}"\
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
        cursor.execute(\
            "INSERT INTO users VALUES (?, ?, ?)",\
            (user_id, len(albums) - 1, 0)\
        )\
        conn.commit()\
        return len(albums) - 1, 0\
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
def album_keyboard(artist, album):\
    kb = InlineKeyboardMarkup()\
    kb.add(\
        InlineKeyboardButton(\
            "\uc0\u55356 \u57255  \u1057 \u1083 \u1091 \u1096 \u1072 \u1090 \u1100  \u1085 \u1072  \u1089 \u1090 \u1088 \u1080 \u1084 \u1080 \u1085 \u1075 \u1072 \u1093 ",\
            url=generate_song_link(artist, album)\
        )\
    )\
    kb.add(\
        InlineKeyboardButton("\uc0\u10145 \u65039  \u1057 \u1083 \u1077 \u1076 \u1091 \u1102 \u1097 \u1080 \u1081  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084 ", callback_data="next")\
    )\
    kb.add(\
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
        await bot.send_message(\
            user_id,\
            "\uc0\u55357 \u56557  \u1040 \u1083 \u1100 \u1073 \u1086 \u1084 \u1099  \u1079 \u1072 \u1082 \u1086 \u1085 \u1095 \u1080 \u1083 \u1080 \u1089 \u1100 ."\
        )\
        return\
\
    album = albums.iloc[index]\
\
    caption = (\
        f"\uc0\u55356 \u57252  <b>\{album['artist']\}</b>\\n"\
        f"\uc0\u55357 \u56511  <b>\{album['album']\}</b>\\n"\
        f"\uc0\u55357 \u56517  \{album['year']\}\\n"\
        f"\uc0\u55356 \u57255  \{album['genre']\}"\
    )\
\
    await bot.send_photo(\
        chat_id=user_id,\
        photo=album["cover"],\
        caption=caption,\
        parse_mode="HTML",\
        reply_markup=album_keyboard(\
            album["artist"],\
            album["album"]\
        )\
    )\
\
    update_index(user_id, index - 1)\
\
\
# --- \uc0\u1061 \u1077 \u1085 \u1076 \u1083 \u1077 \u1088 \u1099  ---\
\
@dp.message_handler(commands=["start"])\
async def start(message: types.Message):\
    get_user(message.from_user.id)\
    await message.answer(\
        "\uc0\u55356 \u57255  \u1071  \u1087 \u1088 \u1080 \u1089 \u1099 \u1083 \u1072 \u1102  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084 \u1099  \u1087 \u1086  \u1087 \u1086 \u1088 \u1103 \u1076 \u1082 \u1091  \'97 \u1086 \u1090  \u1087 \u1086 \u1089 \u1083 \u1077 \u1076 \u1085 \u1077 \u1075 \u1086  \u1082  \u1087 \u1077 \u1088 \u1074 \u1086 \u1084 \u1091 .\\n\\n"\
        "\uc0\u1052 \u1086 \u1078 \u1077 \u1096 \u1100  \u1083 \u1080 \u1089 \u1090 \u1072 \u1090 \u1100  \u1074 \u1088 \u1091 \u1095 \u1085 \u1091 \u1102  \u1080 \u1083 \u1080  \u1074 \u1082 \u1083 \u1102 \u1095 \u1080 \u1090 \u1100  \u1077 \u1078 \u1077 \u1076 \u1085 \u1077 \u1074 \u1085 \u1091 \u1102  \u1087 \u1086 \u1076 \u1073 \u1086 \u1088 \u1082 \u1091 ."\
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
    await call.answer("\uc0\u55357 \u56517  \u1043 \u1086 \u1090 \u1086 \u1074 \u1086 ! \u1041 \u1091 \u1076 \u1091  \u1087 \u1088 \u1080 \u1089 \u1099 \u1083 \u1072 \u1090 \u1100  \u1072 \u1083 \u1100 \u1073 \u1086 \u1084  \u1082 \u1072 \u1078 \u1076 \u1099 \u1081  \u1076 \u1077 \u1085 \u1100 .")\
\
\
@dp.callback_query_handler(lambda c: c.data == "stop")\
async def daily_off(call: types.CallbackQuery):\
    set_daily(call.from_user.id, 0)\
    await call.answer("\uc0\u10060  \u1056 \u1072 \u1089 \u1089 \u1099 \u1083 \u1082 \u1072  \u1086 \u1089 \u1090 \u1072 \u1085 \u1086 \u1074 \u1083 \u1077 \u1085 \u1072 .")\
\
\
# --- \uc0\u1045 \u1078 \u1077 \u1076 \u1085 \u1077 \u1074 \u1085 \u1072 \u1103  \u1088 \u1072 \u1089 \u1089 \u1099 \u1083 \u1082 \u1072  ---\
\
async def daily_job():\
    cursor.execute("SELECT user_id FROM users WHERE daily=1")\
    users = cursor.fetchall()\
\
    for (user_id,) in users:\
        await send_album(user_id)\
\
\
scheduler.add_job(daily_job, "cron", hour=10)  # \uc0\u1082 \u1072 \u1078 \u1076 \u1099 \u1081  \u1076 \u1077 \u1085 \u1100  \u1074  10:00\
scheduler.start()\
\
\
if __name__ == "__main__":\
    executor.start_polling(dp)\
}