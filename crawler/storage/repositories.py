"""Data-access helpers (idempotent upserts for documents + link edges)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import distinct, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Link, PageRecord
from .orm import Crawl, Document, LinkEdge

_U64 = (1 << 64) - 1
_I63 = 1 << 63


def to_signed64(value: int | None) -> int | None:
    """Map a 64-bit unsigned fingerprint into a signed BIGINT for Postgres."""
    if value is None:
        return None
    value &= _U64
    return value - (1 << 64) if value >= _I63 else value


def _dialect_name(session: AsyncSession) -> str:
    """Return the bound dialect name ("sqlite" or "postgresql")."""
    return session.sync_session.get_bind().dialect.name


def _upsert_insert(session: AsyncSession, table: type):
    """Pick the dialect-specific INSERT that supports ``ON CONFLICT``.

    Both the SQLite and Postgres constructs accept ``index_elements`` to infer
    the arbiter unique index, so callers can stay dialect-agnostic.
    """
    if _dialect_name(session) == "sqlite":
        return sqlite_insert(table)
    return pg_insert(table)


_DOC_UPDATE_COLUMNS = (
    "http_status",
    "content_type",
    "content_length",
    "title",
    "content_hash",
    "simhash",
    "storage_key",
    "etag",
    "last_modified",
    "depth",
    "crawl_id",
    "out_degree",
    "last_crawled",
)


class DocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_document(self, record: PageRecord) -> int:
        """Insert or update a document by its unique URL, returning its id."""
        values = {
            "url": record.url,
            "registered_domain": record.registered_domain,
            "http_status": record.http_status,
            "content_type": record.content_type,
            "content_length": record.content_length,
            "title": record.title,
            "content_hash": record.content_hash,
            "simhash": to_signed64(record.simhash),
            "storage_key": record.storage_key,
            "etag": record.etag,
            "last_modified": record.last_modified,
            "depth": record.depth,
            "crawl_id": record.crawl_id,
            "out_degree": len(record.links),
            "last_crawled": func.now(),
        }
        insert_stmt = _upsert_insert(self.session, Document).values(**values)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[Document.url],
            set_={col: getattr(insert_stmt.excluded, col) for col in _DOC_UPDATE_COLUMNS},
        ).returning(Document.id)
        result = await self.session.execute(upsert_stmt)
        return int(result.scalar_one())

    async def add_link_edges(self, src_document_id: int, links: Sequence[Link]) -> None:
        if not links:
            return
        rows = [
            {
                "src_document_id": src_document_id,
                "dst_url": link.url,
                "anchor_text": link.anchor,
            }
            for link in links
        ]
        stmt = (
            _upsert_insert(self.session, LinkEdge)
            .values(rows)
            .on_conflict_do_nothing(
                index_elements=[LinkEdge.src_document_id, LinkEdge.dst_url]
            )
        )
        await self.session.execute(stmt)

    async def get_validators(self, url: str) -> tuple[str | None, str | None] | None:
        """Return (etag, last_modified) for a previously stored URL, if any."""
        result = await self.session.execute(
            select(Document.etag, Document.last_modified).where(Document.url == url)
        )
        row = result.first()
        if row is None:
            return None
        return row[0], row[1]


class GraphRepository:
    """Read/compute over the link graph (in/out-degree, top pages)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def recompute_degrees(self) -> None:
        """Recompute in/out-degree for every document from the edge table.

        out-degree = edges leaving the document; in-degree = edges whose target
        URL matches the document. This is the building block for PageRank-style
        link analysis over the stored web graph.
        """
        # No table alias on the UPDATE target: SQLite disallows it and the
        # unaliased form is valid on Postgres too.
        await self.session.execute(
            text(
                "UPDATE document SET out_degree = "
                "(SELECT count(*) FROM link_edge e WHERE e.src_document_id = document.id)"
            )
        )
        await self.session.execute(
            text(
                "UPDATE document SET in_degree = "
                "(SELECT count(*) FROM link_edge e WHERE e.dst_url = document.url)"
            )
        )

    async def top_by_in_degree(self, limit: int = 10) -> list[tuple[str, int]]:
        result = await self.session.execute(
            select(Document.url, Document.in_degree)
            .order_by(Document.in_degree.desc())
            .limit(limit)
        )
        return [(row[0], row[1]) for row in result.all()]


class AnalyticsRepository:
    """Read-side aggregates that back the control-plane API."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def document_count(self) -> int:
        return int(
            (await self.session.execute(select(func.count()).select_from(Document))).scalar_one()
        )

    async def edge_count(self) -> int:
        return int(
            (await self.session.execute(select(func.count()).select_from(LinkEdge))).scalar_one()
        )

    async def domain_count(self) -> int:
        return int(
            (
                await self.session.execute(
                    select(func.count(distinct(Document.registered_domain)))
                )
            ).scalar_one()
        )

    async def near_duplicate_count(self) -> int:
        """Documents that share a content_hash with an earlier one (exact dupes)
        plus those carrying a simhash are a rough near-duplicate proxy here we
        count rows whose content_hash repeats."""
        subq = (
            select(Document.content_hash)
            .where(Document.content_hash.is_not(None))
            .group_by(Document.content_hash)
            .having(func.count() > 1)
            .subquery()
        )
        result = await self.session.execute(select(func.count()).select_from(subq))
        return int(result.scalar_one())

    async def status_histogram(self) -> list[tuple[int | None, int]]:
        result = await self.session.execute(
            select(Document.http_status, func.count())
            .group_by(Document.http_status)
            .order_by(func.count().desc())
        )
        return [(row[0], int(row[1])) for row in result.all()]

    async def domain_summary(self, limit: int = 50) -> list[tuple[str, int]]:
        result = await self.session.execute(
            select(Document.registered_domain, func.count())
            .group_by(Document.registered_domain)
            .order_by(func.count().desc())
            .limit(limit)
        )
        return [(row[0], int(row[1])) for row in result.all()]

    async def get_document(self, doc_id: int) -> Document | None:
        return await self.session.get(Document, doc_id)

    async def document_links(self, doc_id: int, limit: int = 200) -> list[tuple[str, str | None]]:
        result = await self.session.execute(
            select(LinkEdge.dst_url, LinkEdge.anchor_text)
            .where(LinkEdge.src_document_id == doc_id)
            .limit(limit)
        )
        return [(row[0], row[1]) for row in result.all()]

    async def list_documents(
        self,
        limit: int = 50,
        offset: int = 0,
        domain: str | None = None,
        q: str | None = None,
    ) -> tuple[list[Document], int]:
        """Return a page of documents (most recent first) plus the total count.

        Optional filters: ``domain`` (exact registered domain) and ``q`` (a
        case-insensitive substring match on URL or title).
        """
        filters = []
        if domain:
            filters.append(Document.registered_domain == domain)
        if q:
            pattern = f"%{q}%"
            filters.append(Document.url.ilike(pattern) | Document.title.ilike(pattern))

        total = int(
            (
                await self.session.execute(
                    select(func.count()).select_from(Document).where(*filters)
                )
            ).scalar_one()
        )
        result = await self.session.execute(
            select(Document)
            .where(*filters)
            .order_by(Document.id.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(result.scalars().all()), total

    async def list_crawls(self, limit: int = 50) -> list[tuple[Crawl, int]]:
        """Return recent crawls paired with their stored-document counts."""
        doc_count = (
            select(func.count(Document.id))
            .where(Document.crawl_id == Crawl.id)
            .scalar_subquery()
        )
        result = await self.session.execute(
            select(Crawl, doc_count).order_by(Crawl.id.desc()).limit(limit)
        )
        return [(row[0], int(row[1])) for row in result.all()]


class RecrawlRepository:
    """Selects documents that are due for a refresh based on age."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def select_stale(
        self, older_than_seconds: float, limit: int = 100
    ) -> list[tuple[str, int, int | None]]:
        """Return ``(url, depth, crawl_id)`` for the stalest documents.

        A document is stale if it was never crawled or its ``last_crawled`` is
        older than ``older_than_seconds``. Oldest pages come first so the
        scheduler refreshes the most out-of-date content soonest.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
        result = await self.session.execute(
            select(Document.url, Document.depth, Document.crawl_id)
            .where(
                (Document.last_crawled.is_(None)) | (Document.last_crawled < cutoff)
            )
            .order_by(Document.last_crawled.asc().nulls_first())
            .limit(limit)
        )
        return [(row[0], row[1], row[2]) for row in result.all()]
