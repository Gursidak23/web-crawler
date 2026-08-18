"""Document sinks: where crawled pages are persisted.

The :class:`DocumentSink` protocol decouples the crawl engine from storage so
the same pipeline can write to Postgres in production or collect results in
memory for tests and ``--dry-run`` demos (no Docker required).
"""

from __future__ import annotations

from typing import Protocol

from ..config import Settings
from ..models import PageRecord
from .db import dispose_engine, session_scope
from .object_store import ObjectStore
from .repositories import DocumentRepository


class DocumentSink(Protocol):
    async def save_page(self, record: PageRecord) -> None: ...

    async def get_validators(self, url: str) -> tuple[str | None, str | None] | None:
        """Return cached (etag, last_modified) for a URL to drive conditional GETs."""
        ...

    async def close(self) -> None: ...


class InMemoryDocumentSink:
    """Collects pages in a list - used by unit tests and ``crawler crawl --dry-run``."""

    def __init__(self) -> None:
        self.pages: list[PageRecord] = []
        self._validators: dict[str, tuple[str | None, str | None]] = {}

    async def save_page(self, record: PageRecord) -> None:
        self.pages.append(record)
        if record.etag or record.last_modified:
            self._validators[record.url] = (record.etag, record.last_modified)

    async def get_validators(self, url: str) -> tuple[str | None, str | None] | None:
        return self._validators.get(url)

    async def close(self) -> None:
        return None


class SqlDocumentSink:
    """Persists documents + link edges to Postgres with idempotent upserts.

    An optional :class:`ObjectStore` receives raw page bodies; the resulting
    storage key is recorded on the document row so bodies can be re-fetched.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        object_store: ObjectStore | None = None,
    ) -> None:
        self.settings = settings
        self.object_store = object_store

    async def save_page(self, record: PageRecord) -> None:
        async with session_scope(self.settings) as session:
            repo = DocumentRepository(session)
            src_id = await repo.upsert_document(record)
            await repo.add_link_edges(src_id, record.links)

    async def get_validators(self, url: str) -> tuple[str | None, str | None] | None:
        async with session_scope(self.settings) as session:
            repo = DocumentRepository(session)
            return await repo.get_validators(url)

    async def close(self) -> None:
        if self.object_store is not None:
            await self.object_store.close()
        await dispose_engine()
