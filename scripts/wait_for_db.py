from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import asyncpg

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import settings


async def wait_for_db(max_attempts: int = 60, delay_seconds: int = 2) -> None:
    dsn = settings.postgres_dsn
    for attempt in range(1, max_attempts + 1):
        try:
            conn = await asyncpg.connect(dsn=dsn)
            await conn.close()
            print("Database is ready")
            return
        except Exception as exc:
            print(f"Attempt {attempt}/{max_attempts}: database unavailable ({exc})")
            await asyncio.sleep(delay_seconds)

    raise RuntimeError("Database did not become ready in time")


if __name__ == "__main__":
    asyncio.run(wait_for_db())
