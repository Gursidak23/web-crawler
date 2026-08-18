"""Integration tests for Phase 5 storage (requires Docker).

Exercises the Postgres object-store backend (gzipped body round-trip) and the
link-graph in/out-degree computation against a real Postgres instance.
"""

import pytest
from sqlalchemy import text

from crawler.models import Link, PageRecord
from crawler.storage.db import session_scope
from crawler.storage.object_store import PostgresObjectStore
from crawler.storage.repositories import (
    DocumentRepository,
    GraphRepository,
    RecrawlRepository,
)

from .support import postgres_settings

pytestmark = pytest.mark.integration


async def test_postgres_object_store_round_trip():
    async with postgres_settings() as settings:
        store = PostgresObjectStore(settings)
        body = b"<html><body>" + b"abc " * 5000 + b"</body></html>"
        key = await store.put("hash-1", body, "text/html")
        assert key == "pg:hash-1"

        # Idempotent: storing the same hash again must not error or duplicate.
        await store.put("hash-1", body, "text/html")

        fetched = await store.get(key)
        assert fetched == body
        assert await store.get("pg:missing") is None


async def test_link_graph_degrees():
    async with postgres_settings() as settings:
        # Build a tiny graph: A -> B, A -> C, B -> A.
        url_a, url_b, url_c = (
            "http://a.example/a",
            "http://a.example/b",
            "http://a.example/c",
        )
        async with session_scope(settings) as session:
            repo = DocumentRepository(session)
            id_a = await repo.upsert_document(
                PageRecord(url=url_a, registered_domain="a.example", depth=0)
            )
            id_b = await repo.upsert_document(
                PageRecord(url=url_b, registered_domain="a.example", depth=1)
            )
            await repo.upsert_document(
                PageRecord(url=url_c, registered_domain="a.example", depth=1)
            )
            await repo.add_link_edges(id_a, [Link(url=url_b), Link(url=url_c)])
            await repo.add_link_edges(id_b, [Link(url=url_a)])

        async with session_scope(settings) as session:
            await GraphRepository(session).recompute_degrees()

        async with session_scope(settings) as session:
            top = await GraphRepository(session).top_by_in_degree(10)

        in_degrees = dict(top)
        # Every page is linked exactly once across the graph.
        assert in_degrees[url_a] == 1
        assert in_degrees[url_b] == 1
        assert in_degrees[url_c] == 1


async def test_recrawl_selects_stale_documents():
    async with postgres_settings() as settings:
        fresh, stale = "http://x.example/fresh", "http://x.example/stale"
        async with session_scope(settings) as session:
            repo = DocumentRepository(session)
            await repo.upsert_document(
                PageRecord(url=fresh, registered_domain="x.example", depth=0)
            )
            await repo.upsert_document(
                PageRecord(url=stale, registered_domain="x.example", depth=0)
            )

        # Age one document well past the staleness cutoff.
        async with session_scope(settings) as session:
            await session.execute(
                text(
                    "UPDATE document SET last_crawled = now() - interval '10 days' "
                    "WHERE url = :url"
                ),
                {"url": stale},
            )

        async with session_scope(settings) as session:
            due = await RecrawlRepository(session).select_stale(
                older_than_seconds=24 * 3600, limit=10
            )

        urls = [row[0] for row in due]
        assert stale in urls
        assert fresh not in urls
