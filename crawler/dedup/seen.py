"""URL "seen" set abstraction used by the distributed worker.

Single-node code dedups with an exact in-memory set (the ``MemoryFrontier``);
the distributed worker uses a Redis-backed Bloom filter because the seen-set can
grow to billions of URLs shared across workers.
"""

from __future__ import annotations

from typing import Protocol

from .bloom import BloomFilter


class UrlSeen(Protocol):
    async def check_and_add(self, url: str) -> bool: ...


class InMemoryUrlSeen:
    """In-process Bloom-backed seen-set (for tests and single-process use)."""

    def __init__(self, expected_insertions: int = 1_000_000, false_positive_rate: float = 0.01):
        self._bloom = BloomFilter(expected_insertions, false_positive_rate)

    async def check_and_add(self, url: str) -> bool:
        return self._bloom.check_and_add(url)
