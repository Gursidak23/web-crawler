"""Fetch, parse and cache robots.txt per origin.

Parsing uses ``protego`` (wildcards + ``crawl-delay`` support). Parsed rules are
held in a process-local LRU; the raw text is optionally mirrored in Redis with a
TTL so a fleet of workers shares one fetch per origin. A single-flight lock per
origin prevents a thundering herd of robots fetches on cold start.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

import redis.asyncio as aioredis
from protego import Protego

from ..cache import LruCache
from ..config import Settings
from ..logging_setup import get_logger

log = get_logger(__name__)

FetchText = Callable[[str], Awaitable[tuple[int, str]]]


class RobotsCache:
    def __init__(
        self,
        fetch_text: FetchText,
        settings: Settings,
        redis: aioredis.Redis | None = None,
    ) -> None:
        self._fetch_text = fetch_text
        self.settings = settings
        self.redis = redis
        self._cache: LruCache[str, Protego] = LruCache(
            capacity=settings.politeness.robots_cache_size,
            ttl=settings.politeness.robots_cache_ttl_seconds,
        )
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _origin(url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}"

    def _lock(self, origin: str) -> asyncio.Lock:
        lock = self._locks.get(origin)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[origin] = lock
        return lock

    async def _parser(self, url: str) -> Protego:
        origin = self._origin(url)
        cached = self._cache.get(origin)
        if cached is not None:
            return cached
        async with self._lock(origin):
            cached = self._cache.get(origin)
            if cached is not None:
                return cached
            text = await self._load(origin)
            parser = Protego.parse(text)
            self._cache.put(origin, parser)
            return parser

    async def _load(self, origin: str) -> str:
        redis_key = f"robots:{origin}"
        if self.redis is not None:
            try:
                cached = await self.redis.get(redis_key)
                if cached is not None:
                    return cached.decode() if isinstance(cached, bytes) else cached
            except Exception as exc:  # noqa: BLE001
                log.warning("robots_redis_get_failed", origin=origin, error=repr(exc))

        try:
            status, text = await self._fetch_text(f"{origin}/robots.txt")
        except Exception as exc:  # noqa: BLE001 - unreachable robots -> allow all
            log.warning("robots_fetch_failed", origin=origin, error=repr(exc))
            status, text = 0, ""
        robots = text if status == 200 else ""

        if self.redis is not None:
            try:
                await self.redis.set(
                    redis_key, robots, ex=self.settings.politeness.robots_cache_ttl_seconds
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("robots_redis_set_failed", origin=origin, error=repr(exc))
        return robots

    async def allowed(self, url: str) -> bool:
        parser = await self._parser(url)
        return bool(parser.can_fetch(url, self.settings.fetch.user_agent))

    async def crawl_delay(self, url: str) -> float | None:
        parser = await self._parser(url)
        return parser.crawl_delay(self.settings.fetch.user_agent)
