"""Content dedup service: LSH-bucketed SimHash near-duplicate detection.

``is_duplicate`` checks whether a fingerprint is within the Hamming threshold of
any previously seen page (using LSH bands to avoid an O(N) scan) and, if not,
registers it. The in-memory variant is for single-node/tests; the Redis variant
shares the index across workers.
"""

from __future__ import annotations

from typing import Protocol

import redis.asyncio as aioredis

from ..config import Settings
from .simhash import band_values, hamming_distance


class ContentDedup(Protocol):
    async def is_duplicate(self, fingerprint: int) -> bool: ...


class InMemoryContentDedup:
    def __init__(self, bands: int = 4, bits: int = 64, threshold: int = 3) -> None:
        self.bands = bands
        self.bits = bits
        self.threshold = threshold
        self._buckets: dict[tuple[int, int], list[int]] = {}

    async def is_duplicate(self, fingerprint: int) -> bool:
        values = band_values(fingerprint, self.bands, self.bits)
        candidates: set[int] = set()
        for band, value in enumerate(values):
            candidates.update(self._buckets.get((band, value), ()))

        for other in candidates:
            if hamming_distance(fingerprint, other) <= self.threshold:
                return True

        for band, value in enumerate(values):
            self._buckets.setdefault((band, value), []).append(fingerprint)
        return False


class RedisContentDedup:
    def __init__(
        self,
        redis: aioredis.Redis,
        bands: int = 4,
        bits: int = 64,
        threshold: int = 3,
        namespace: str = "lsh",
        ttl_seconds: int = 7 * 24 * 3600,
    ) -> None:
        self.redis = redis
        self.bands = bands
        self.bits = bits
        self.threshold = threshold
        self.namespace = namespace
        self.ttl_seconds = ttl_seconds

    def _bucket_key(self, band: int, value: int) -> str:
        return f"{self.namespace}:{band}:{value}"

    async def is_duplicate(self, fingerprint: int) -> bool:
        values = band_values(fingerprint, self.bands, self.bits)

        candidates: set[int] = set()
        for band, value in enumerate(values):
            members = await self.redis.smembers(self._bucket_key(band, value))
            candidates.update(int(m) for m in members)

        for other in candidates:
            if hamming_distance(fingerprint, other) <= self.threshold:
                return True

        pipe = self.redis.pipeline()
        for band, value in enumerate(values):
            key = self._bucket_key(band, value)
            pipe.sadd(key, str(fingerprint))
            pipe.expire(key, self.ttl_seconds)
        await pipe.execute()
        return False


def content_dedup_from_settings(
    settings: Settings, redis: aioredis.Redis | None = None
) -> ContentDedup:
    bands = settings.dedup.simhash_lsh_bands
    threshold = settings.dedup.simhash_hamming_threshold
    if redis is not None:
        return RedisContentDedup(redis, bands=bands, threshold=threshold)
    return InMemoryContentDedup(bands=bands, threshold=threshold)
