"""Unit tests for the in-memory frontier."""

import asyncio

from crawler.frontier.memory import MemoryFrontier
from crawler.models import FrontierItem


async def test_add_dedupes_seen_urls():
    frontier = MemoryFrontier()
    assert await frontier.add(FrontierItem("http://a.com/")) is True
    assert await frontier.add(FrontierItem("http://a.com/")) is False
    assert frontier.qsize() == 1
    assert frontier.seen_count() == 1


async def test_get_task_done_and_join():
    frontier = MemoryFrontier()
    await frontier.add(FrontierItem("http://a.com/", depth=1))

    item = await frontier.get()
    assert item.url == "http://a.com/"
    assert item.depth == 1

    frontier.task_done()
    await asyncio.wait_for(frontier.join(), timeout=1.0)
