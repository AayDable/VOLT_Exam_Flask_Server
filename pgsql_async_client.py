import os
import asyncio
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def get_pool():
    pg_pool = await asyncpg.create_pool(
        dsn=os.getenv("PGSQL_CONNECTION_STRING"),
        min_size=1,
        max_size=10,
        timeout=10.0,
    )

    return pg_pool

async def main():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("select now() as ts")
            print(row)
    finally:
        await pool.close()

if __name__ == "__main__":

    asyncio.run(main())
