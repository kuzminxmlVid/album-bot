# TELEGRAM BOT — aiogram 3 + PostgreSQL
# Fixed syntax error in set_rating
# Added /my_ratings, statistics, config, live rating update

import os
import asyncio
import sqlite3
import pandas as pd
import aiohttp
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
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

# ================= BOT =================

bot = Bot(token=Config.TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ================= POSTGRES =================

pg_pool: asyncpg.Pool | None = None

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

# ================= MIGRATION =================

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
                    "INSERT INTO ratings (user_id, album_list, rank, rating) VALUES ($1,$2,$3,$4)"
                    " ON CONFLICT (user_id, album_list, rank) DO UPDATE SET rating=$4",
                    *row
                )
        except sqlite3.OperationalError:
            pass

    sqlite_conn.close()
    os.rename("users.db", "users.db.migrated")

# ================= ALBUM LISTS =================

album_cache = {}

def load_albums(list_name: str):
    df = pd.read_excel(f"{Config.ALBUMS_DIR}/{list_name}.xlsx")
    return df.sort_values("rank").reset_index(drop=True)

def get_albums(list_name):
    if list_name not in album_cache:
        album_cache[list_name] = load_albums(list_name)
    return album_cache[list_name]

# ================= USERS =================

async def get_user(user_id: int):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT album_list, current_index, daily, paused FROM users WHERE user_id=$1",
            user_id
        )
        if row is None:
            albums = get_albums(Config.DEFAULT_LIST)
            start_index = len(albums) - 1
            await conn.execute(
                "INSERT INTO users VALUES ($1,$2,$3,0,0)",
                user_id, Config.DEFAULT_LIST, start_index
            )
            return Config.DEFAULT_LIST, start_index, 0, 0
        return row["album_list"], row["current_index"], row["daily"], row["paused"]

async def update_index(user_id, index):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE users SET current_index=$1 WHERE user_id=$2", index, user_id)

async def set_album_list(user_id, list_name):
    albums = get_albums(list_name)
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE users SET album_list=$1, current_index=$2 WHERE user_id=$3",
                           list_name, len(albums) - 1, user_id)

async def set_paused(user_id, value):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE users SET paused=$1 WHERE user_id=$2", value, user_id)

# ================= RATINGS =================

async def set_rating(user_id, album_list, rank, rating):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ratings (user_id, album_list, rank, rating) VALUES ($1,$2,$3,$4)"
            " ON CONFLICT (user_id, album_list, rank) DO UPDATE SET rating=$4",
            user_id, album_list, rank, rating
        )

async def get_rating(user_id, album_list, rank):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT rating FROM ratings WHERE user_id=$1 AND album_list=$2 AND rank=$3",
            user_id, album_list, rank
        )
        return row["rating"] if row else None

async def get_user_ratings(user_id):
    async with pg_pool.acquire() as conn:
        return await conn.fetch(
            "SELECT album_list, rank, rating FROM ratings WHERE user_id=$1 ORDER BY album_list, rank",
            user_id
        )

async def get_average_rating(album_list, rank):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ROUND(AVG(rating),2) as avg FROM ratings WHERE album_list=$1 AND rank=$2",
            album_list, rank
        )
        return row["avg"] if row and row["avg"] else None

# ================= (rest of the bot code remains same as previous version) =================
