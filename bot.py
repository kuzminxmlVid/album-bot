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

if not Config.TOKEN:
    raise RuntimeError("TOKEN not set")
if not Config.DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

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
            "SELECT album_list, current_index FROM users WHERE user_id=$1",
            user_id
        )
        if not row:
            albums = get_albums(Config.DEFAULT_LIST)
            index = len(albums) - 1
            await conn.execute(
                "INSERT INTO users VALUES ($1,$2,$3)",
                user_id, Config.DEFAULT_LIST, index
            )
            return Config.DEFAULT_LIST, index
        return row["album_list"], row["current_index"]

async def set_index(user_id, index):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET current_index=$1 WHERE user_id=$2",
            index, user_id
        )

# ================= COVER =================

async def get_cover(artist, album):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
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

def google_album_link(artist, album):
    return f"https://www.google.com/search?q={quote_plu_
