"""Distributed per-host token bucket backed by an atomic Redis + Lua script.

All workers share one bucket per host in Redis, so politeness holds even when
many machines crawl the same domain. Fails open (allows the fetch) if Redis is
unreachable - a crawler should prefer progress over stalling on a cache outage.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import redis.asyncio as aioredis

from .. import metrics
from ..logging_setup import get_logger

log = get_logger(__name__)

_LUA = (Path(__file__).parent / "token_bucket.lua").read_text(encoding="utf-8")


class RedisRateLimiter:
    def __init__(
        self,
        redis: aioredis.Redis,
        capacity: float = 1.0,
        ttl_ms: int = 3_600_000,
        max_wait_seconds: float = 10.0,
    ) -> None:
        self.redis = redis
        self.capacity = capacity
        self.ttl_ms = ttl_ms
        self.max_wait_seconds = max_wait_seconds
        self._script = redis.register_script(_LUA)

    async def acquire(self, host: str, refill_per_sec: float) -> None:
        if refill_per_sec <= 0:
            return
        key = f"rl:{host}"
        while True:
            now_ms = int(time.time() * 1000)
            try:
                result = await self._script(
                    keys=[key],
                    args=[self.capacity, refill_per_sec, now_ms, 1, self.ttl_ms],
                )
            except Exception as exc:  # noqa: BLE001 - fail open on any Redis error
                log.warning("rate_limiter_unavailable", host=host, error=repr(exc))
                return
            allowed, wait_ms = int(result[0]), int(result[1])
            if allowed == 1:
                return
            metrics.RATE_LIMIT_WAITS.inc()
            await asyncio.sleep(min(wait_ms / 1000.0, self.max_wait_seconds))
