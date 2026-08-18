"""Global back-pressure.

A simple async semaphore that caps how many pipeline tasks run concurrently
across the whole process, independent of (and in addition to) the per-host
limits in :mod:`crawler.robots.politeness`. This protects shared resources
(DB pool, sockets, CPU) when many domains are active at once.
"""

from __future__ import annotations

import asyncio
from types import TracebackType


class Backpressure:
    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = limit
        self._sem = asyncio.Semaphore(limit)

    @property
    def in_flight(self) -> int:
        # Semaphore exposes its remaining value via its internal counter.
        return self.limit - self._sem._value  # noqa: SLF001

    async def __aenter__(self) -> Backpressure:
        await self._sem.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._sem.release()
