"""Bloom filter for the URL "have we seen this?" set.

A Bloom filter answers set membership in O(k) time and O(m) bits with **zero
false negatives** - it may occasionally claim an unseen URL was seen (a tunable
false-positive rate), but never the reverse, so we never re-crawl a URL. This is
the right trade for a seen-set that can reach billions of entries, where storing
the URL strings themselves would be prohibitive.

Sizing follows the standard formulas for ``n`` expected insertions and target
false-positive rate ``p``::

    m = ceil(-(n * ln p) / (ln 2)^2)     # number of bits
    k = round((m / n) * ln 2)            # number of hash functions

We derive ``k`` indices from two 32-bit MurmurHash3 values via the
Kirsch-Mitzenmacher technique ``h_i = h1 + i*h2`` instead of computing ``k``
independent hashes.
"""

from __future__ import annotations

import math

import mmh3
import redis.asyncio as aioredis


def optimal_m(n: int, p: float) -> int:
    return max(8, math.ceil(-(n * math.log(p)) / (math.log(2) ** 2)))


def optimal_k(m: int, n: int) -> int:
    return max(1, round((m / n) * math.log(2)))


def bloom_indices(key: str, m: int, k: int) -> list[int]:
    data = key.encode("utf-8")
    h1 = mmh3.hash(data, 0, signed=False)
    h2 = mmh3.hash(data, h1, signed=False) | 1  # ensure odd -> good stride mod m
    return [(h1 + i * h2) % m for i in range(k)]


class BloomFilter:
    """In-memory, hand-rolled Bloom filter backed by a ``bytearray`` bit set."""

    def __init__(self, expected_insertions: int, false_positive_rate: float = 0.01) -> None:
        if expected_insertions <= 0:
            raise ValueError("expected_insertions must be positive")
        if not 0.0 < false_positive_rate < 1.0:
            raise ValueError("false_positive_rate must be in (0, 1)")
        self.n = expected_insertions
        self.p = false_positive_rate
        self.m = optimal_m(expected_insertions, false_positive_rate)
        self.k = optimal_k(self.m, expected_insertions)
        self._bits = bytearray((self.m + 7) // 8)
        self._count = 0

    def check_and_add(self, key: str) -> bool:
        """Add ``key`` and return ``True`` iff it was *absent* (newly added)."""
        was_absent = False
        for idx in bloom_indices(key, self.m, self.k):
            byte, bit = divmod(idx, 8)
            mask = 1 << bit
            if not self._bits[byte] & mask:
                was_absent = True
                self._bits[byte] |= mask
        if was_absent:
            self._count += 1
        return was_absent

    def __contains__(self, key: str) -> bool:
        return all(
            self._bits[idx // 8] >> (idx % 8) & 1 for idx in bloom_indices(key, self.m, self.k)
        )

    def __len__(self) -> int:
        return self._count


# Atomic add-and-check against a Redis bitfield: set each bit, reporting whether
# every bit was already set (i.e. the key was probably present).
_LUA_BLOOM = """
local all_set = 1
for i = 1, #ARGV do
  local idx = tonumber(ARGV[i])
  if redis.call('GETBIT', KEYS[1], idx) == 0 then
    all_set = 0
    redis.call('SETBIT', KEYS[1], idx, 1)
  end
end
return all_set
"""


class RedisBloomFilter:
    """Distributed Bloom filter sharing one Redis bitfield across all workers."""

    def __init__(
        self,
        redis: aioredis.Redis,
        key: str,
        expected_insertions: int,
        false_positive_rate: float = 0.01,
    ) -> None:
        self.redis = redis
        self.key = key
        self.m = optimal_m(expected_insertions, false_positive_rate)
        self.k = optimal_k(self.m, expected_insertions)
        self._script = redis.register_script(_LUA_BLOOM)

    async def check_and_add(self, key: str) -> bool:
        """Add ``key`` and return ``True`` iff it was *absent* (newly added)."""
        indices = [str(i) for i in bloom_indices(key, self.m, self.k)]
        all_set = await self._script(keys=[self.key], args=indices)
        return int(all_set) == 0
