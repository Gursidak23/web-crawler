"""Construction helpers that wire collaborators together for the CLI and worker."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import redis.asyncio as aioredis

from .config import Settings
from .dedup.bloom import RedisBloomFilter
from .dedup.dedup_service import ContentDedup, content_dedup_from_settings
from .dedup.seen import InMemoryUrlSeen, UrlSeen
from .fetcher import Fetcher
from .ratelimit import LocalRateLimiter, RedisRateLimiter
from .ratelimit.base import RateLimiter
from .resilience.backpressure import Backpressure
from .resilience.budget import DomainBudget, InMemoryDomainBudget, RedisDomainBudget
from .resilience.circuit_breaker import CircuitBreaker
from .robots import Politeness, RobotsCache
from .storage.object_store import ObjectStore, build_object_store


def make_fetch_text(fetcher: Fetcher) -> Callable[[str], Awaitable[tuple[int, str]]]:
    async def fetch_text(url: str) -> tuple[int, str]:
        result = await fetcher.fetch(url)
        return result.status, result.body.decode("utf-8", "replace")

    return fetch_text


def build_politeness(
    fetcher: Fetcher,
    settings: Settings,
    *,
    redis: aioredis.Redis | None = None,
) -> Politeness:
    """Assemble a :class:`Politeness` gate.

    With ``redis`` it uses the distributed Redis+Lua limiter and a shared robots
    cache; without it (single-node) it falls back to an in-process limiter.
    """
    robots = RobotsCache(make_fetch_text(fetcher), settings, redis=redis)
    limiter: RateLimiter = (
        RedisRateLimiter(redis) if redis is not None else LocalRateLimiter()
    )
    return Politeness(robots, limiter, settings)


def build_content_dedup(
    settings: Settings, *, redis: aioredis.Redis | None = None
) -> ContentDedup:
    return content_dedup_from_settings(settings, redis=redis)


def build_object_store_for(settings: Settings, *, enabled: bool = True) -> ObjectStore:
    return build_object_store(settings, enabled=enabled)


def build_circuit_breaker(settings: Settings) -> CircuitBreaker | None:
    if not settings.resilience.enable_circuit_breaker:
        return None
    return CircuitBreaker(
        failure_threshold=settings.resilience.circuit_failure_threshold,
        reset_seconds=settings.resilience.circuit_reset_seconds,
    )


def build_domain_budget(
    settings: Settings, *, redis: aioredis.Redis | None = None
) -> DomainBudget | None:
    if not settings.resilience.enable_domain_budget:
        return None
    limit = settings.crawl.max_pages_per_domain
    if redis is not None:
        return RedisDomainBudget(redis, settings.resilience.domain_budget_namespace, limit)
    return InMemoryDomainBudget(limit)


def build_backpressure(settings: Settings) -> Backpressure | None:
    limit = settings.resilience.max_global_in_flight or settings.fetch.concurrency
    if limit <= 0:
        return None
    return Backpressure(limit)


def build_url_seen(settings: Settings, *, redis: aioredis.Redis | None = None) -> UrlSeen:
    if redis is not None:
        return RedisBloomFilter(
            redis,
            settings.dedup.bloom_namespace,
            settings.dedup.bloom_expected_insertions,
            settings.dedup.bloom_false_positive_rate,
        )
    return InMemoryUrlSeen(
        settings.dedup.bloom_expected_insertions,
        settings.dedup.bloom_false_positive_rate,
    )
