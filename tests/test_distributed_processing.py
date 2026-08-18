"""Tests for the worker's per-message processing step (no Kafka required).

We drive ``process_message`` with a fake ``enqueue`` and an in-memory seen-set,
fetching from a local fixture server.
"""

from urllib.parse import urlsplit

from crawler.config import get_settings
from crawler.dedup.seen import InMemoryUrlSeen
from crawler.fetcher import Fetcher
from crawler.models import FrontierItem
from crawler.pipeline import Pipeline, process_message
from crawler.storage.sinks import InMemoryDocumentSink

from .support import serve_site

SITE = {
    "/": (
        "<html><title>A</title><body>"
        '<a href="/b">b</a><a href="/c">c</a><a href="/b">dup-b</a>'
        "</body></html>"
    ),
    "/b": "<html><title>B</title><body>leaf</body></html>",
    "/c": "<html><title>C</title><body>leaf</body></html>",
}


async def test_process_message_enqueues_deduped_children():
    enqueued: list[FrontierItem] = []

    async def enqueue(item: FrontierItem) -> None:
        enqueued.append(item)

    seen = InMemoryUrlSeen()
    sink = InMemoryDocumentSink()
    settings = get_settings()

    async with serve_site(SITE) as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings)
        result = await process_message(
            pipeline, enqueue, seen, FrontierItem(url=base, depth=0), max_depth=2
        )

    assert result.fetched is True
    child_paths = sorted(urlsplit(i.url).path for i in enqueued)
    # /b appears twice on the page but must be enqueued once (seen-set dedup).
    assert child_paths == ["/b", "/c"]
    assert all(i.depth == 1 for i in enqueued)


async def test_process_message_stops_at_max_depth():
    enqueued: list[FrontierItem] = []

    async def enqueue(item: FrontierItem) -> None:
        enqueued.append(item)

    seen = InMemoryUrlSeen()
    sink = InMemoryDocumentSink()
    settings = get_settings()

    async with serve_site(SITE) as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings)
        await process_message(
            pipeline, enqueue, seen, FrontierItem(url=base, depth=2), max_depth=2
        )

    # At max depth we still fetch/store the page but do not enqueue children.
    assert enqueued == []
    assert len(sink.pages) == 1
