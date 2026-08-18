"""Shared helpers for Docker-backed integration tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from crawler.config import Settings
from crawler.storage.db import dispose_engine, get_engine
from crawler.storage.orm import Base


@asynccontextmanager
async def postgres_settings() -> AsyncIterator[Settings]:
    """Yield a Settings bound to a fresh Postgres container with tables created."""
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16", driver="asyncpg")
    container.start()
    try:
        settings = Settings()
        settings.postgres.dsn = container.get_connection_url()

        await dispose_engine()  # bind the global engine to the test DSN
        engine = get_engine(settings)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield settings
    finally:
        await dispose_engine()
        container.stop()
