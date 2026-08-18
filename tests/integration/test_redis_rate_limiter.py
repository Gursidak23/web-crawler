"""Integration test for the Redis + Lua token bucket (requires Docker)."""

import time

import pytest
import redis.asyncio as aioredis

from crawler.ratelimit import RedisRateLimiter

pytestmark = pytest.mark.integration


async def test_redis_token_bucket_spaces_requests():
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        client: aioredis.Redis = aioredis.from_url(f"redis://{host}:{port}/0")
        try:
            limiter = RedisRateLimiter(client, capacity=1.0)
            refill = 1000.0 / 100  # ~one token per 100ms

            start = time.monotonic()
            await limiter.acquire("example.com", refill)  # full bucket -> immediate
            await limiter.acquire("example.com", refill)  # waits ~100ms
            elapsed = time.monotonic() - start

            assert 0.08 <= elapsed < 1.0
        finally:
            await client.aclose()
