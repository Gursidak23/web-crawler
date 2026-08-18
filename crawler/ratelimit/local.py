"""In-process token-bucket limiter for single-node crawls (no Redis required)."""

from __future__ import annotations

import asyncio
import time

from .. import metrics


class LocalRateLimiter:
    def __init__(self, capacity: float = 1.0) -> None:
        self.capacity = capacity
        self._state: dict[str, tuple[float, float]] = {}  # host -> (tokens, ts)
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, host: str) -> asyncio.Lock:
        lock = self._locks.get(host)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[host] = lock
        return lock

    async def acquire(self, host: str, refill_per_sec: float) -> None:
        if refill_per_sec <= 0:
            return
        async with self._lock(host):
            while True:
                now = time.monotonic()
                tokens, ts = self._state.get(host, (self.capacity, now))
                tokens = min(self.capacity, tokens + (now - ts) * refill_per_sec)
                if tokens >= 1.0:
                    self._state[host] = (tokens - 1.0, now)
                    return
                wait = (1.0 - tokens) / refill_per_sec
                self._state[host] = (tokens, now)
                metrics.RATE_LIMIT_WAITS.inc()
                await asyncio.sleep(wait)
