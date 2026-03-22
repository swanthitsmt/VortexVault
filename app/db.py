from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings

DROP_LEGACY_COMBO_CONSTRAINT = "ALTER TABLE combo_entries DROP CONSTRAINT IF EXISTS uq_combo_digest_payload"
DROP_LEGACY_EXACT_INDEX = "DROP INDEX IF EXISTS idx_combo_exact_lookup"
DROP_DIGEST_UNIQUE_CONSTRAINT = "ALTER TABLE combo_entries DROP CONSTRAINT IF EXISTS uq_combo_digest"
ENSURE_JOB_TYPE_MERGE_VALUE = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_type') THEN
        ALTER TYPE job_type ADD VALUE IF NOT EXISTS 'merge';
    END IF;
END
$$
"""
ENSURE_COMBO_URL_TRGM_INDEX = "CREATE INDEX IF NOT EXISTS idx_combo_url_trgm ON combo_entries USING gin (url gin_trgm_ops)"
ENSURE_COMBO_USERNAME_TRGM_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_combo_username_trgm ON combo_entries USING gin (username gin_trgm_ops)"
)
ENSURE_COMBO_PASSWORD_TRGM_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_combo_password_trgm ON combo_entries USING gin (password gin_trgm_ops)"
)
ENSURE_COMBO_CREATED_AT_DESC_INDEX = "CREATE INDEX IF NOT EXISTS idx_combo_created_at_desc ON combo_entries (created_at DESC)"
ENSURE_COMBO_DIGEST_INDEX = "CREATE INDEX IF NOT EXISTS idx_combo_digest ON combo_entries (digest)"
ENSURE_COMBO_DEFAULT_PARTITION = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'combo_entries'
    ) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS combo_entries_default PARTITION OF combo_entries DEFAULT';
    END IF;
END
$$
"""
ENSURE_COMBO_MONTHLY_PARTITIONS = """
DO $$
DECLARE
    start_ts DATE;
    end_ts DATE;
    part_name TEXT;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'combo_entries'
    ) THEN
        FOR i IN 0..1 LOOP
            start_ts := (date_trunc('month', now()) + (i || ' month')::interval)::date;
            end_ts := (date_trunc('month', now()) + ((i + 1) || ' month')::interval)::date;
            part_name := format('combo_entries_%s', to_char(start_ts, 'YYYYMM'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF combo_entries FOR VALUES FROM (%L) TO (%L)',
                part_name,
                start_ts::text,
                end_ts::text
            );
        END LOOP;
    END IF;
END
$$
"""


class Base(DeclarativeBase):
    pass


async_engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=30,
)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

sync_engine = create_engine(
    settings.sync_sqlalchemy_database_url,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=30,
)
SyncSessionLocal = sessionmaker(sync_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    from app import models  # noqa: F401

    async with async_engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await conn.execute(text(ENSURE_JOB_TYPE_MERGE_VALUE))
        await conn.run_sync(Base.metadata.create_all)
        if settings.ensure_search_indexes_on_startup:
            await conn.execute(text(ENSURE_COMBO_URL_TRGM_INDEX))
            await conn.execute(text(ENSURE_COMBO_USERNAME_TRGM_INDEX))
            await conn.execute(text(ENSURE_COMBO_PASSWORD_TRGM_INDEX))
            await conn.execute(text(ENSURE_COMBO_CREATED_AT_DESC_INDEX))
            await conn.execute(text(ENSURE_COMBO_DIGEST_INDEX))
        await conn.execute(text(ENSURE_COMBO_DEFAULT_PARTITION))
        await conn.execute(text(ENSURE_COMBO_MONTHLY_PARTITIONS))
        await conn.execute(text(DROP_LEGACY_COMBO_CONSTRAINT))
        await conn.execute(text(DROP_LEGACY_EXACT_INDEX))
        await conn.execute(text(DROP_DIGEST_UNIQUE_CONSTRAINT))


def init_db_sync() -> None:
    from app import models  # noqa: F401

    with sync_engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        conn.execute(text(ENSURE_JOB_TYPE_MERGE_VALUE))
        Base.metadata.create_all(bind=conn)
        if settings.ensure_search_indexes_on_startup:
            conn.execute(text(ENSURE_COMBO_URL_TRGM_INDEX))
            conn.execute(text(ENSURE_COMBO_USERNAME_TRGM_INDEX))
            conn.execute(text(ENSURE_COMBO_PASSWORD_TRGM_INDEX))
            conn.execute(text(ENSURE_COMBO_CREATED_AT_DESC_INDEX))
            conn.execute(text(ENSURE_COMBO_DIGEST_INDEX))
        conn.execute(text(ENSURE_COMBO_DEFAULT_PARTITION))
        conn.execute(text(ENSURE_COMBO_MONTHLY_PARTITIONS))
        conn.execute(text(DROP_LEGACY_COMBO_CONSTRAINT))
        conn.execute(text(DROP_LEGACY_EXACT_INDEX))
        conn.execute(text(DROP_DIGEST_UNIQUE_CONSTRAINT))
