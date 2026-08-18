"""End-to-end single-node crawl over a local fixture site (no Docker)."""

from urllib.parse import urlsplit

from crawler.config import get_settings
from crawler.fetcher import Fetcher
from crawler.frontier.memory import MemoryFrontier
from crawler.pipeline import CrawlEngine, Pipeline
from crawler.storage.sinks import InMemoryDocumentSink

from .support import serve_site

SITE = {
    "/": (
        "<html><title>A</title><body>"
        '<a href="/b">b</a><a href="/c">c</a>'
        "</body></html>"
    ),
    "/b": '<html><title>B</title><body><a href="/d">d</a></body></html>',
    "/c": "<html><title>C</title><body>leaf</body></html>",
    "/d": "<html><title>D</title><body>leaf</body></html>",
}


async def _crawl(base: str, *, max_depth: int, concurrency: int = 4):
    sink = InMemoryDocumentSink()
    frontier = MemoryFrontier()
    settings = get_settings()
    async with Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings)
        engine = CrawlEngine(
            pipeline,
            frontier,
            settings,
            max_depth=max_depth,
            max_pages=100,
            concurrency=concurrency,
            same_domain_only=True,
        )
        stats = await engine.run([base])
    return sink, stats


async def test_engine_breadth_first_crawl():
    async with serve_site(SITE) as base:
        sink, stats = await _crawl(base, max_depth=2)

    paths = {urlsplit(p.url).path for p in sink.pages}
    assert {"/b", "/c", "/d"} <= paths
    assert stats.processed == 4

    seed_page = max(sink.pages, key=lambda p: len(p.links))
    assert len(seed_page.links) == 2


async def test_engine_respects_max_depth():
    async with serve_site(SITE) as base:
        sink, stats = await _crawl(base, max_depth=1)

    paths = {urlsplit(p.url).path for p in sink.pages}
    assert "/b" in paths and "/c" in paths
    assert "/d" not in paths  # depth 2 is beyond the limit
    assert stats.processed == 3
