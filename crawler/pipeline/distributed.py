"""Distributed processing step shared by the Kafka worker.

Kept free of any Kafka import so it can be unit-tested with a fake ``enqueue``
and an in-memory seen-set. ``enqueue`` is any coroutine that accepts a
``FrontierItem`` (the Kafka frontier's ``add`` in production).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..dedup.seen import UrlSeen
from ..models import FrontierItem, ProcessResult
from .pipeline import Pipeline

Enqueue = Callable[[FrontierItem], Awaitable[None]]


async def process_message(
    pipeline: Pipeline,
    enqueue: Enqueue,
    seen: UrlSeen,
    item: FrontierItem,
    max_depth: int,
) -> ProcessResult:
    """Fetch/parse/store one URL and enqueue its newly-seen child links.

    Child links are gated by the shared seen-set (Bloom filter) so the same URL
    is enqueued at most once across the whole fleet.
    """
    result = await pipeline.process(item)
    if result.fetched and not result.duplicate and item.depth < max_depth:
        for link in result.links:
            if await seen.check_and_add(link.url):
                await enqueue(
                    FrontierItem(url=link.url, depth=item.depth + 1, crawl_id=item.crawl_id)
                )
    return result
