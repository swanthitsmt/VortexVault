from __future__ import annotations

import asyncio
import time
from datetime import datetime
from urllib.parse import urlparse

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.celery_app import celery_app
from app.config import settings

_status_cache_lock = asyncio.Lock()
_status_cache_expires_at = 0.0
_status_cache_value = {
    "postgres": False,
    "redis": False,
    "worker": False,
}


def _compact_number(value: int) -> str:
    units = [(1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")]
    for threshold, suffix in units:
        if value >= threshold:
            scaled = value / threshold
            if scaled >= 100:
                return f"{int(scaled)}{suffix}"
            number = f"{scaled:.1f}".rstrip("0").rstrip(".")
            return f"{number}{suffix}"
    return f"{value}"


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


async def _postgres_online(session: AsyncSession) -> bool:
    try:
        await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _redis_online() -> bool:
    parsed = urlparse(settings.redis_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 6379
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=0.8)
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def _worker_online() -> bool:
    def _ping() -> bool:
        try:
            result = celery_app.control.inspect(timeout=0.8).ping()
            return bool(result)
        except Exception:
            return False

    return await asyncio.to_thread(_ping)


async def _get_runtime_status(session: AsyncSession) -> dict[str, bool]:
    global _status_cache_expires_at, _status_cache_value

    now = time.monotonic()
    if now < _status_cache_expires_at:
        return dict(_status_cache_value)

    async with _status_cache_lock:
        now = time.monotonic()
        if now < _status_cache_expires_at:
            return dict(_status_cache_value)

        postgres_ok, redis_ok, worker_ok = await asyncio.gather(
            _postgres_online(session),
            _redis_online(),
            _worker_online(),
        )
        _status_cache_value = {
            "postgres": postgres_ok,
            "redis": redis_ok,
            "worker": worker_ok,
        }
        ttl = max(int(settings.dashboard_status_cache_seconds), 1)
        _status_cache_expires_at = time.monotonic() + ttl
        return dict(_status_cache_value)


async def _queue_depths() -> dict[str, int]:
    queue_names = ("clean", "upload", "merge", "pipeline")
    try:
        client = Redis.from_url(settings.celery_broker_url, decode_responses=False)
        try:
            values = await asyncio.gather(*(client.llen(name) for name in queue_names), return_exceptions=True)
        finally:
            await client.aclose()
    except Exception:
        return {name: 0 for name in queue_names}

    result: dict[str, int] = {}
    for name, value in zip(queue_names, values, strict=False):
        if isinstance(value, Exception):
            result[name] = 0
        else:
            result[name] = int(value or 0)
    return result


async def load_dashboard_metrics(session: AsyncSession) -> dict:
    estimated_rows = int(
        (
            await session.execute(
                text(
                    """
                    SELECT COALESCE(
                        GREATEST(
                            COALESCE(
                                (
                                    SELECT GREATEST(reltuples, 0)::bigint
                                    FROM pg_class
                                    WHERE oid = 'combo_entries'::regclass
                                ),
                                0
                            ),
                            COALESCE((SELECT MAX(id) FROM combo_entries), 0)
                        ),
                        0
                    )
                    """
                )
            )
        ).scalar_one()
    )

    db_size_bytes = int((await session.execute(text("SELECT pg_database_size(current_database())"))).scalar_one())
    db_size_pretty = str((await session.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()))"))).scalar_one())

    today_inserted = int(
        (
            await session.execute(
                text(
                    """
                    SELECT COALESCE(SUM(rows_inserted), 0)
                    FROM processing_jobs
                    WHERE job_type = 'merge'
                      AND status = 'completed'
                      AND created_at >= date_trunc('day', now())
                    """
                )
            )
        ).scalar_one()
    )

    last_updated = (
        await session.execute(
            text("SELECT created_at FROM combo_entries ORDER BY id DESC LIMIT 1")
        )
    ).scalar_one_or_none()

    db_activity = (
        await session.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(blks_read), 0)::BIGINT AS blks_read,
                    COALESCE(SUM(blks_hit), 0)::BIGINT AS blks_hit,
                    COALESCE(SUM(tup_inserted), 0)::BIGINT AS tup_inserted
                FROM pg_stat_database
                WHERE datname = current_database()
                """
            )
        )
    ).one()

    runtime_status = await _get_runtime_status(session)
    queue_depths = await _queue_depths()

    return {
        "total_rows_estimate": estimated_rows,
        "total_rows_display": _compact_number(estimated_rows),
        "db_size_bytes": db_size_bytes,
        "db_size_display": db_size_pretty,
        "today_inserted": today_inserted,
        "today_inserted_display": _compact_number(today_inserted),
        "last_updated": last_updated,
        "last_updated_display": _format_dt(last_updated),
        "status": runtime_status,
        "queue_depths": queue_depths,
        "db_blks_read": int(db_activity[0] or 0),
        "db_blks_hit": int(db_activity[1] or 0),
        "db_tup_inserted": int(db_activity[2] or 0),
    }
