from __future__ import annotations

import csv
import io
import logging
import time
from collections.abc import AsyncGenerator
from typing import Literal

from sqlalchemy import Select, func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from app.config import settings
from app.db import async_engine
from app.models import ComboEntry

logger = logging.getLogger(__name__)
_es_ready_cache: dict[str, float | bool | int] = {
    "checked_at": 0.0,
    "ready": False,
    "es_docs": 0,
    "pg_live_tup": 0,
}


def _safe_csv_cell(value: str) -> str:
    """
    Guard against spreadsheet formula injection on CSV open in Excel/Sheets.
    """
    if value and value[0] in ("=", "+", "-", "@", "\t"):
        return "'" + value
    return value


def _maybe_contains(column: ColumnElement, value: str, regex: bool) -> ColumnElement:
    if regex:
        return column.op("~*")(value)
    return column.ilike(f"%{value}%")


def _has_text_filters(url: str | None, username: str | None, password: str | None) -> bool:
    return bool((url and url.strip()) or (username and username.strip()) or (password and password.strip()))


def build_conditions(
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    regex: bool,
) -> list[ColumnElement[bool]]:
    conditions: list[ColumnElement[bool]] = []

    if url:
        conditions.append(_maybe_contains(ComboEntry.url, url, regex))
    if username:
        conditions.append(_maybe_contains(ComboEntry.username, username, regex))
    if password:
        conditions.append(_maybe_contains(ComboEntry.password, password, regex))

    return conditions


async def _is_es_ready_for_search() -> bool:
    now = time.monotonic()
    ttl = max(int(settings.es_sync_check_interval_seconds), 1)
    checked_at = float(_es_ready_cache["checked_at"])
    if now - checked_at < ttl:
        return bool(_es_ready_cache["ready"])

    ready = False
    es_docs = 0
    pg_live_tup = 0
    required_docs = 1

    try:
        from app.services.es import es_index_doc_count

        es_docs = await es_index_doc_count()
        pg_live_tup_stmt = text(
            """
            SELECT COALESCE(n_live_tup, 0)::BIGINT
            FROM pg_stat_all_tables
            WHERE schemaname = 'public' AND relname = 'combo_entries'
            """
        )
        async with async_engine.connect() as conn:
            pg_live_tup = int((await conn.execute(pg_live_tup_stmt)).scalar_one_or_none() or 0)

        if pg_live_tup <= 0:
            ready = es_docs > 0
        else:
            required_docs = max(1, int(pg_live_tup * settings.es_min_sync_ratio))
            ready = es_docs >= required_docs

        if not ready:
            logger.warning(
                "ES not sufficiently synced for search coverage; using PostgreSQL "
                "(es_docs=%s pg_live_tup=%s required=%s)",
                es_docs,
                pg_live_tup,
                required_docs,
            )
    except Exception:
        logger.warning("Failed to evaluate ES readiness; using PostgreSQL fallback", exc_info=True)
        ready = False

    _es_ready_cache["checked_at"] = now
    _es_ready_cache["ready"] = ready
    _es_ready_cache["es_docs"] = es_docs
    _es_ready_cache["pg_live_tup"] = pg_live_tup

    return ready


async def run_search(
    session: AsyncSession,
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    regex: bool,
    page_size: int,
    cursor: int | None,
    direction: Literal["next", "prev"],
    include_total: bool,
    statement_timeout_ms: int,
    count_statement_timeout_ms: int,
) -> dict:
    # Route to Elasticsearch when enabled — falls back to PG on failure
    if settings.es_enabled:
        if await _is_es_ready_for_search():
            try:
                from app.services.es import es_search

                es_result = await es_search(
                    url=url,
                    username=username,
                    password=password,
                    regex=regex,
                    page_size=page_size,
                    cursor=cursor,
                    direction=direction,
                    include_total=include_total,
                )
                # Guard against temporary ES drift by falling back when first page is unexpectedly empty.
                if es_result.get("rows"):
                    return es_result
                if cursor is not None:
                    return es_result
                if not _has_text_filters(url, username, password):
                    return es_result
                logger.warning("Elasticsearch returned empty first page; falling back to PostgreSQL for accuracy.")
            except Exception:
                logger.warning("Elasticsearch search failed, falling back to PostgreSQL", exc_info=True)

    page_size = max(page_size, 1)

    conditions = build_conditions(url=url, username=username, password=password, regex=regex)

    base: Select = select(ComboEntry.id, ComboEntry.url, ComboEntry.username, ComboEntry.password)
    if conditions:
        base = base.where(*conditions)

    start = time.perf_counter()

    limit = page_size + 1
    rows_stmt = base

    if cursor is not None:
        if direction == "prev":
            rows_stmt = rows_stmt.where(ComboEntry.id > cursor)
        else:
            rows_stmt = rows_stmt.where(ComboEntry.id < cursor)

    if direction == "prev":
        rows_stmt = rows_stmt.order_by(ComboEntry.id.asc()).limit(limit)
    else:
        rows_stmt = rows_stmt.order_by(ComboEntry.id.desc()).limit(limit)

    timeout_ms = max(int(statement_timeout_ms), 100)
    count_timeout_ms = max(int(count_statement_timeout_ms), timeout_ms)
    count_timed_out = False

    async with session.begin():
        await session.execute(text(f"SET LOCAL statement_timeout = '{timeout_ms}ms'"))
        if not regex and _has_text_filters(url, username, password):
            # Avoid slow backward PK scans when trigram indexes are available.
            await session.execute(text("SET LOCAL enable_indexscan = off"))
            await session.execute(text("SET LOCAL enable_indexonlyscan = off"))
        raw_rows = list((await session.execute(rows_stmt)).all())

    total: int | None = None
    if include_total:
        try:
            async with session.begin():
                await session.execute(text(f"SET LOCAL statement_timeout = '{count_timeout_ms}ms'"))
                if not regex and _has_text_filters(url, username, password):
                    await session.execute(text("SET LOCAL enable_indexscan = off"))
                    await session.execute(text("SET LOCAL enable_indexonlyscan = off"))
                count_stmt = select(func.count()).select_from(base.subquery())
                total = int((await session.execute(count_stmt)).scalar_one())
        except DBAPIError as exc:
            message = str(getattr(exc, "orig", exc)).lower()
            if "statement timeout" in message:
                count_timed_out = True
                logger.warning("Exact count timed out for search query; returning page results without total.")
            else:
                raise

    if direction == "prev":
        has_prev = len(raw_rows) > page_size
        page_rows = raw_rows[:page_size]
        rows = list(reversed(page_rows))
        has_next = cursor is not None
    else:
        has_next = len(raw_rows) > page_size
        rows = raw_rows[:page_size]
        has_prev = cursor is not None

    first_id = int(rows[0].id) if rows else None
    last_id = int(rows[-1].id) if rows else None
    next_cursor = last_id if has_next and last_id is not None else None
    prev_cursor = first_id if has_prev and first_id is not None else None

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "total": total,
        "page_size": page_size,
        "execution_ms": elapsed_ms,
        "rows": rows,
        "cursor": cursor,
        "direction": direction,
        "has_next": has_next,
        "has_prev": has_prev,
        "next_cursor": next_cursor,
        "prev_cursor": prev_cursor,
        "first_id": first_id,
        "last_id": last_id,
        "count_timed_out": count_timed_out,
    }


async def stream_csv(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    regex: bool,
    max_rows: int,
) -> AsyncGenerator[bytes, None]:
    conditions = build_conditions(url=url, username=username, password=password, regex=regex)

    stmt = select(ComboEntry.url, ComboEntry.username, ComboEntry.password).order_by(ComboEntry.id.desc())
    if conditions:
        stmt = stmt.where(*conditions)

    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    # UTF-8 BOM improves Excel compatibility for non-ASCII data.
    yield "\ufeff".encode("utf-8")
    writer.writerow(["URL", "Username", "Password"])
    yield buffer.getvalue().encode("utf-8")
    buffer.seek(0)
    buffer.truncate(0)

    sent = 0
    async with session_factory() as session:
        stream_result = await session.stream(stmt)
        async for row in stream_result:
            writer.writerow(
                [
                    _safe_csv_cell(row.url),
                    _safe_csv_cell(row.username),
                    _safe_csv_cell(row.password),
                ]
            )
            sent += 1

            if sent % 1000 == 0:
                yield buffer.getvalue().encode("utf-8")
                buffer.seek(0)
                buffer.truncate(0)

            if sent >= max_rows:
                break

    if buffer.tell() > 0:
        yield buffer.getvalue().encode("utf-8")
