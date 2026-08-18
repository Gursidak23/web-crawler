"""Service layer behind the control-plane API.

Keeps the route handlers thin: they call a :class:`CrawlService`, which owns the
database access (via repositories) and frontier seeding. A Protocol lets tests
swap in an in-memory fake with no Postgres/Kafka.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Protocol

from ..config import Settings, get_settings
from ..logging_setup import get_logger
from ..storage.db import session_scope
from ..storage.orm import Crawl
from ..storage.repositories import AnalyticsRepository, GraphRepository
from ..url_utils import normalize_url
from .schemas import (
    CrawlActionOut,
    CrawlCreate,
    CrawlOut,
    CrawlSummary,
    DocumentOut,
    DocumentPage,
    DocumentSummary,
    DomainOut,
    GraphNode,
    GraphOut,
    LinkOut,
    StatsOut,
    StatusCount,
)

log = get_logger(__name__)

# In-process single-node crawls launched from the dashboard, keyed by crawl id so
# they can be cancelled ("stopped") on request. Holding the reference also keeps
# the task from being garbage-collected while it runs.
_RUNNING_CRAWLS: dict[int, asyncio.Task[None]] = {}


def _iso_utc(dt: datetime | None) -> str | None:
    """Serialize a datetime as an unambiguous UTC ISO-8601 string.

    Timestamps come back naive from SQLite (which stores UTC) — tag them as UTC
    so the browser can convert to the viewer's local zone correctly.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat()


class CrawlService(Protocol):
    async def create_crawl(self, req: CrawlCreate) -> CrawlOut: ...

    async def stop_crawl(self, crawl_id: int) -> CrawlActionOut | None: ...

    async def stats(self) -> StatsOut: ...

    async def list_domains(self, limit: int) -> list[DomainOut]: ...

    async def get_document(self, doc_id: int) -> DocumentOut | None: ...

    async def graph(self, limit: int) -> GraphOut: ...

    async def list_crawls(self, limit: int) -> list[CrawlSummary]: ...

    async def list_documents(
        self, limit: int, offset: int, domain: str | None, q: str | None
    ) -> DocumentPage: ...


class SqlCrawlService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def create_crawl(self, req: CrawlCreate) -> CrawlOut:
        normalized = [n for s in req.seeds if (n := normalize_url(s)) is not None]
        status = "running" if normalized else "failed"
        async with session_scope(self.settings) as session:
            crawl = Crawl(
                name=req.name,
                seeds=normalized,
                status=status,
                max_depth=req.max_depth,
                max_pages=req.max_pages,
                allowed_domains=req.allowed_domains,
            )
            session.add(crawl)
            await session.flush()
            crawl_id = crawl.id

        seeded = 0
        if normalized:
            if self.settings.frontier.backend == "kafka":
                # Distributed mode: publish onto Kafka for the worker fleet.
                seeded = await self._seed(normalized, crawl_id, req.max_depth)
            else:
                # Single-node mode: crawl in-process so the dashboard's
                # "Start a crawl" button actually fetches pages (no Kafka or
                # separate worker fleet needed).
                seeded = self._launch_local(normalized, crawl_id, req)

        return CrawlOut(
            id=crawl_id,
            name=req.name,
            status=status,
            seeds=normalized,
            max_depth=req.max_depth,
            max_pages=req.max_pages,
            seeded=seeded,
        )

    def _launch_local(self, seeds: list[str], crawl_id: int, req: CrawlCreate) -> int:
        """Start an in-process crawl as a background task; return the seed count."""
        from ..local_crawl import execute_crawl

        task = asyncio.create_task(
            execute_crawl(
                self.settings,
                crawl_id=crawl_id,
                seeds=seeds,
                max_depth=req.max_depth,
                max_pages=req.max_pages,
                allowed_domains=req.allowed_domains,
                # An explicit allow-list drives child filtering; otherwise the
                # crawl stays on the seed domains.
                same_domain_only=not req.allowed_domains,
            ),
            name=f"crawl-{crawl_id}",
        )
        _RUNNING_CRAWLS[crawl_id] = task
        def _clear_running(_t: asyncio.Task[None], cid: int = crawl_id) -> None:
            _RUNNING_CRAWLS.pop(cid, None)

        task.add_done_callback(_clear_running)
        return len(seeds)

    async def stop_crawl(self, crawl_id: int) -> CrawlActionOut | None:
        """Cancel a running in-process crawl and mark it ``stopped``.

        Returns ``None`` if the crawl doesn't exist. Idempotent: stopping an
        already-finished crawl just reports its current status.
        """
        task = _RUNNING_CRAWLS.get(crawl_id)
        if task is not None and not task.done():
            task.cancel()

        async with session_scope(self.settings) as session:
            row = await session.get(Crawl, crawl_id)
            if row is None:
                return None
            if row.status in ("running", "pending"):
                row.status = "stopped"
            status = row.status

        log.info("crawl_stop_requested", crawl_id=crawl_id, status=status)
        return CrawlActionOut(id=crawl_id, status=status)

    async def _seed(self, seeds: list[str], crawl_id: int, max_depth: int) -> int:
        """Publish seeds onto the Kafka frontier (no-op for the memory backend)."""
        if self.settings.frontier.backend != "kafka" or not seeds:
            return 0
        from ..frontier import FrontierMessage, KafkaFrontier, ensure_topics

        await ensure_topics(self.settings)
        frontier = KafkaFrontier(self.settings)
        await frontier.start()
        count = 0
        try:
            for url in seeds:
                await frontier.add_message(
                    FrontierMessage(url=url, depth=0, crawl_id=crawl_id)
                )
                count += 1
        finally:
            await frontier.stop()
        return count

    async def stats(self) -> StatsOut:
        async with session_scope(self.settings) as session:
            repo = AnalyticsRepository(session)
            documents = await repo.document_count()
            edges = await repo.edge_count()
            domains = await repo.domain_count()
            near_dupes = await repo.near_duplicate_count()
            histogram = await repo.status_histogram()
        return StatsOut(
            documents=documents,
            edges=edges,
            domains=domains,
            near_duplicates=near_dupes,
            by_status=[StatusCount(status=s, count=c) for s, c in histogram],
        )

    async def list_domains(self, limit: int) -> list[DomainOut]:
        async with session_scope(self.settings) as session:
            rows = await AnalyticsRepository(session).domain_summary(limit)
        return [DomainOut(registered_domain=d, documents=c) for d, c in rows]

    async def get_document(self, doc_id: int) -> DocumentOut | None:
        async with session_scope(self.settings) as session:
            repo = AnalyticsRepository(session)
            doc = await repo.get_document(doc_id)
            if doc is None:
                return None
            links = await repo.document_links(doc_id)
            return DocumentOut(
                id=doc.id,
                url=doc.url,
                registered_domain=doc.registered_domain,
                http_status=doc.http_status,
                content_type=doc.content_type,
                title=doc.title,
                content_hash=doc.content_hash,
                simhash=doc.simhash,
                depth=doc.depth,
                in_degree=doc.in_degree,
                out_degree=doc.out_degree,
                storage_key=doc.storage_key,
                links=[LinkOut(url=u, anchor=a) for u, a in links],
            )

    async def graph(self, limit: int) -> GraphOut:
        async with session_scope(self.settings) as session:
            rows = await GraphRepository(session).top_by_in_degree(limit)
        return GraphOut(top_pages=[GraphNode(url=u, in_degree=d) for u, d in rows])

    async def list_crawls(self, limit: int) -> list[CrawlSummary]:
        async with session_scope(self.settings) as session:
            rows = await AnalyticsRepository(session).list_crawls(limit)
        return [
            CrawlSummary(
                id=crawl.id,
                name=crawl.name,
                status=crawl.status,
                max_depth=crawl.max_depth,
                max_pages=crawl.max_pages,
                documents=count,
                created_at=_iso_utc(crawl.created_at),
            )
            for crawl, count in rows
        ]

    async def list_documents(
        self, limit: int, offset: int, domain: str | None, q: str | None
    ) -> DocumentPage:
        async with session_scope(self.settings) as session:
            docs, total = await AnalyticsRepository(session).list_documents(
                limit=limit, offset=offset, domain=domain, q=q
            )
        return DocumentPage(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                DocumentSummary(
                    id=d.id,
                    url=d.url,
                    registered_domain=d.registered_domain,
                    http_status=d.http_status,
                    title=d.title,
                    depth=d.depth,
                    in_degree=d.in_degree,
                    out_degree=d.out_degree,
                )
                for d in docs
            ],
        )
