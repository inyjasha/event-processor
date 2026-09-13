import os
import asyncpg
from pathlib import Path
from dotenv import load_dotenv

ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=False)
DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = int(os.environ["POSTGRES_PORT"])
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_POOL_MIN_SIZE = int(os.environ["DB_POOL_MIN_SIZE"])
DB_POOL_MAX_SIZE = int(os.environ["DB_POOL_MAX_SIZE"])


if not (0 <= DB_POOL_MIN_SIZE <= DB_POOL_MAX_SIZE):
    raise ValueError(
        "Размеры пула должны удовлетворять условию: 0 <= MIN <= MAX"
    )
if DB_POOL_MAX_SIZE < 1:
    raise ValueError("Максимальный размер пула должен быть не меньше 1")

async def open_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        timeout=10,
    )

async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        timeout=10,
    )