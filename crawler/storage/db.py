"""Async SQLAlchemy engine/session management.

The engine works against either an embedded SQLite file (the default, so the
crawler runs with no external services) or Postgres (for the distributed
fleet). SQLite gets WAL mode + a busy timeout so concurrent pipeline writers
don't trip over ``database is locked``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import Settings, get_settings

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def _is_sqlite(dsn: str) -> bool:
    return dsn.startswith("sqlite")


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    global _engine
    if _engine is None:
        s = settings or get_settings()
        if _is_sqlite(s.postgres.dsn):
            # SQLite uses a single file/connection; pool sizing doesn't apply.
            _engine = create_async_engine(s.postgres.dsn, echo=s.postgres.echo)

            @event.listens_for(_engine.sync_engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=5000")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
        else:
            _engine = create_async_engine(
                s.postgres.dsn,
                echo=s.postgres.echo,
                pool_size=s.postgres.pool_size,
                pool_pre_ping=True,
            )
    return _engine


async def ensure_schema(settings: Settings | None = None) -> None:
    """Create tables for the embedded SQLite database if they don't exist.

    Postgres deployments manage their schema with Alembic (``alembic upgrade
    head``), so this is a no-op there.
    """
    s = settings or get_settings()
    if not _is_sqlite(s.postgres.dsn):
        return
    from .orm import Base

    engine = get_engine(s)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def get_sessionmaker(settings: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(
            get_engine(settings), expire_on_commit=False, class_=AsyncSession
        )
    return _sessionmaker


@asynccontextmanager
async def session_scope(settings: Settings | None = None) -> AsyncIterator[AsyncSession]:
    """Provide a transactional session scope."""
    maker = get_sessionmaker(settings)
    async with maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None
