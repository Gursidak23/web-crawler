"""In-memory frontier: an asyncio FIFO queue == breadth-first crawl.

A FIFO ordering yields natural BFS by depth. A ``seen`` set provides O(1) URL
deduplication for single-node runs; Phase 3 swaps this for a Redis-backed Bloom
filter so the seen-set can scale and be shared across workers.
"""

from __future__ import annotations

import asyncio

from ..models import FrontierItem


class MemoryFrontier:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[FrontierItem] = asyncio.Queue()
        self._seen: set[str] = set()

    async def add(self, item: FrontierItem) -> bool:
        if item.url in self._seen:
            return False
        self._seen.add(item.url)
        await self._queue.put(item)
        return True

    async def get(self) -> FrontierItem:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    def qsize(self) -> int:
        return self._queue.qsize()

    def seen_count(self) -> int:
        return len(self._seen)

    async def close(self) -> None:  # noqa: D401 - nothing to release in-memory
        return None
