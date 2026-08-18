"""Per-domain crawl budgets.

Caps how many pages we'll fetch from any single registered domain so one large
site can't starve the rest of the frontier. The Redis backend makes the budget
fleet-wide (all workers share the same counter); the in-memory backend is for
single-process runs and tests.
"""

from __future__ import annotations

from typing import Protocol

import redis.asyncio as aioredis

from .. import metrics


class DomainBudget(Protocol):
    async def try_consume(self, domain: str, cost: int = 1) -> bool:
        """Charge ``cost`` against ``domain``'s budget.

        Returns True if the budget allowed it, False once exhausted.
        """
        ...

    async def remaining(self, domain: str) -> int: ...


class InMemoryDomainBudget:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._used: dict[str, int] = {}

    async def try_consume(self, domain: str, cost: int = 1) -> bool:
        if self.limit <= 0:
            return True
        used = self._used.get(domain, 0)
        if used + cost > self.limit:
            metrics.BUDGET_EXHAUSTED.inc()
            return False
        self._used[domain] = used + cost
        return True

    async def remaining(self, domain: str) -> int:
        if self.limit <= 0:
            return -1
        return max(0, self.limit - self._used.get(domain, 0))


class RedisDomainBudget:
    """Fleet-wide budget using an atomic ``INCRBY`` counter per domain."""

    def __init__(self, redis: aioredis.Redis, namespace: str, limit: int) -> None:
        self.redis = redis
        self.namespace = namespace
        self.limit = limit

    def _key(self, domain: str) -> str:
        return f"{self.namespace}:{domain}"

    async def try_consume(self, domain: str, cost: int = 1) -> bool:
        if self.limit <= 0:
            return True
        try:
            used = await self.redis.incrby(self._key(domain), cost)
        except Exception:  # noqa: BLE001 - fail open if Redis is unavailable
            return True
        if used > self.limit:
            metrics.BUDGET_EXHAUSTED.inc()
            return False
        return True

    async def remaining(self, domain: str) -> int:
        if self.limit <= 0:
            return -1
        try:
            raw = await self.redis.get(self._key(domain))
        except Exception:  # noqa: BLE001
            return -1
        used = int(raw) if raw is not None else 0
        return max(0, self.limit - used)
