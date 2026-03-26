from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from fluxdb.config import settings


class Base(DeclarativeBase):
    pass


async_engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=40,
    max_overflow=80,
    pool_timeout=30,
)
AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

sync_engine = create_engine(
    settings.database_url_sync,
    pool_pre_ping=True,
    pool_size=40,
    max_overflow=80,
    pool_timeout=30,
)
SyncSessionLocal = sessionmaker(bind=sync_engine, autoflush=False, autocommit=False, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


def get_sync_session() -> Session:
    return SyncSessionLocal()
