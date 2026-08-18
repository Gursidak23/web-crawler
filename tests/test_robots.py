"""Unit tests for robots.txt parsing/caching and the politeness gate."""

from urllib.parse import urlsplit

from crawler.config import Settings
from crawler.factory import build_politeness, make_fetch_text
from crawler.fetcher import Fetcher
from crawler.frontier.memory import MemoryFrontier
from crawler.pipeline import CrawlEngine, Pipeline
from crawler.robots import RobotsCache
from crawler.storage.sinks import InMemoryDocumentSink

from .support import serve_site

ROBOTS = "User-agent: *\nDisallow: /private\nCrawl-delay: 2\n"

ROBOTS_SITE = {
    "/robots.txt": ROBOTS,
    "/public": "<html><title>pub</title><body>ok</body></html>",
    "/private/secret": "<html><title>secret</title><body>no</body></html>",
}


async def test_robots_allow_disallow_and_crawl_delay():
    settings = Settings()
    async with serve_site(ROBOTS_SITE) as base, Fetcher(settings) as fetcher:
        robots = RobotsCache(make_fetch_text(fetcher), settings, redis=None)
        assert await robots.allowed(f"{base}/public") is True
        assert await robots.allowed(f"{base}/private/secret") is False
        assert await robots.crawl_delay(f"{base}/public") == 2.0


CRAWL_SITE = {
    "/robots.txt": "User-agent: *\nDisallow: /private\n",
    "/": (
        "<html><body>"
        '<a href="/public">p</a><a href="/private/secret">s</a>'
        "</body></html>"
    ),
    "/public": "<html><title>pub</title><body>ok</body></html>",
    "/private/secret": "<html><title>secret</title><body>no</body></html>",
}


async def test_engine_obeys_robots_disallow():
    settings = Settings()
    settings.politeness.respect_robots = True
    settings.politeness.default_crawl_delay_ms = 5  # keep the test fast

    async with serve_site(CRAWL_SITE) as base, Fetcher(settings) as fetcher:
        politeness = build_politeness(fetcher, settings)
        sink = InMemoryDocumentSink()
        frontier = MemoryFrontier()
        pipeline = Pipeline(fetcher, sink, settings, politeness=politeness)
        engine = CrawlEngine(
            pipeline,
            frontier,
            settings,
            max_depth=1,
            max_pages=50,
            concurrency=2,
            same_domain_only=True,
        )
        await engine.run([base])

    paths = {urlsplit(p.url).path for p in sink.pages}
    assert "/public" in paths
    assert "/private/secret" not in paths  # blocked by robots.txt
