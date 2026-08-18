"""Shared async Redis client factory (used by the rate limiter, Bloom filter,
LSH index, and robots cache)."""

from __future__ import annotations

import redis.asyncio as aioredis

from .config import Settings, get_settings

_client: aioredis.Redis | None = None


def get_redis(settings: Settings | None = None) -> aioredis.Redis:
    global _client
    if _client is None:
        s = settings or get_settings()
        _client = aioredis.from_url(s.redis.url, decode_responses=False)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
