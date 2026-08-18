"""Unit tests for the in-process token-bucket limiter."""

import time

from crawler.ratelimit import LocalRateLimiter


async def test_spaces_successive_requests_to_same_host():
    limiter = LocalRateLimiter(capacity=1.0)
    refill = 1000.0 / 100  # ~ one token per 100ms

    start = time.monotonic()
    await limiter.acquire("host", refill)  # bucket starts full -> immediate
    await limiter.acquire("host", refill)  # must wait ~100ms for a refill
    elapsed = time.monotonic() - start

    assert 0.08 <= elapsed < 0.5


async def test_hosts_are_independent():
    limiter = LocalRateLimiter(capacity=1.0)
    refill = 1000.0 / 100

    start = time.monotonic()
    await limiter.acquire("a", refill)
    await limiter.acquire("b", refill)  # different host -> its own full bucket
    elapsed = time.monotonic() - start

    assert elapsed < 0.05
