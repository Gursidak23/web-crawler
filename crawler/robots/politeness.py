"""Politeness gate: robots permission + per-host rate limit + concurrency cap.

Usage in the pipeline::

    if not await politeness.allowed(url):
        ...skip...
    async with politeness.slot(url):
        result = await fetcher.fetch(url)

The ``slot`` context manager bounds per-host concurrency with a semaphore and,
while held, blocks on the rate limiter so successive fetches to a host are
spaced by its effective crawl delay.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from ..config import Settings
from ..ratelimit.base import RateLimiter
from ..url_utils import host_of
from .robots_cache import RobotsCache


class Politeness:
    def __init__(
        self,
        robots: RobotsCache,
        limiter: RateLimiter,
        settings: Settings,
    ) -> None:
        self.robots = robots
        self.limiter = limiter
        self.settings = settings
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    async def allowed(self, url: str) -> bool:
        if not self.settings.politeness.respect_robots:
            return True
        try:
            return await self.robots.allowed(url)
        except Exception:  # noqa: BLE001 - fail open
            return True

    def _semaphore(self, host: str) -> asyncio.Semaphore:
        sem = self._semaphores.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self.settings.fetch.per_host_concurrency)
            self._semaphores[host] = sem
        return sem

    async def _effective_delay_ms(self, url: str) -> int:
        delay_ms = self.settings.politeness.default_crawl_delay_ms
        if self.settings.politeness.respect_robots:
            try:
                crawl_delay = await self.robots.crawl_delay(url)
            except Exception:  # noqa: BLE001
                crawl_delay = None
            if crawl_delay is not None:
                delay_ms = max(delay_ms, int(crawl_delay * 1000))
        return delay_ms

    @contextlib.asynccontextmanager
    async def slot(self, url: str) -> AsyncIterator[None]:
        host = host_of(url)
        async with self._semaphore(host):
            delay_ms = await self._effective_delay_ms(url)
            if delay_ms > 0:
                await self.limiter.acquire(host, 1000.0 / delay_ms)
            yield
