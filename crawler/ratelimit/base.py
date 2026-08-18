"""Rate-limiter interface.

``acquire`` blocks (sleeping as needed) until the caller may fetch ``host``
again. ``refill_per_sec`` is derived from the effective crawl delay, so a host
with a 2s crawl-delay yields ``refill_per_sec = 0.5``.
"""

from __future__ import annotations

from typing import Protocol


class RateLimiter(Protocol):
    async def acquire(self, host: str, refill_per_sec: float) -> None: ...
