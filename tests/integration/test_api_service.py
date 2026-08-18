"""Integration test for the SqlCrawlService against real Postgres (requires Docker).

Drives the read-side control-plane queries (stats, domains, document, graph)
end-to-end so the API isn't only covered by the in-memory fake.
"""

import pytest

from crawler.api.service import SqlCrawlService
from crawler.models import Link, PageRecord
from crawler.storage.db import session_scope
from crawler.storage.repositories import DocumentRepository, GraphRepository

from .support import postgres_settings

pytestmark = pytest.mark.integration


async def _seed_graph(settings) -> int:
    url_a, url_b = "http://x.example/a", "http://x.example/b"
    async with session_scope(settings) as session:
        repo = DocumentRepository(session)
        id_a = await repo.upsert_document(
            PageRecord(
                url=url_a,
                registered_domain="x.example",
                depth=0,
                http_status=200,
                title="A",
                content_hash="hash-a",
            )
        )
        await repo.upsert_document(
            PageRecord(
                url=url_b, registered_domain="x.example", depth=1, http_status=404
            )
        )
        await repo.add_link_edges(id_a, [Link(url=url_b, anchor="to-b")])
    async with session_scope(settings) as session:
        await GraphRepository(session).recompute_degrees()
    return id_a


async def test_service_stats_domains_document_graph():
    async with postgres_settings() as settings:
        id_a = await _seed_graph(settings)
        service = SqlCrawlService(settings)

        stats = await service.stats()
        assert stats.documents == 2
        assert stats.edges == 1
        assert stats.domains == 1
        statuses = {s.status: s.count for s in stats.by_status}
        assert statuses == {200: 1, 404: 1}

        domains = await service.list_domains(limit=10)
        assert domains[0].registered_domain == "x.example"
        assert domains[0].documents == 2

        doc = await service.get_document(id_a)
        assert doc is not None
        assert doc.url == "http://x.example/a"
        assert doc.out_degree == 1
        assert doc.links[0].url == "http://x.example/b"

        assert await service.get_document(999_999) is None

        graph = await service.graph(limit=5)
        urls = {n.url for n in graph.top_pages}
        assert "http://x.example/b" in urls
