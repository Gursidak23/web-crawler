"""A small, hand-rolled LRU cache with optional TTL.

Built on ``OrderedDict`` in access order: ``get`` moves a key to the most-recent
end, and inserts evict from the least-recent end once capacity is exceeded. Used
for the robots.txt and DNS caches. Single-threaded asyncio access means no lock
is required.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class LruCache(Generic[K, V]):
    def __init__(self, capacity: int, ttl: float | None = None) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.ttl = ttl
        self._data: OrderedDict[K, tuple[V, float]] = OrderedDict()

    def get(self, key: K) -> V | None:
        item = self._data.get(key)
        if item is None:
            return None
        value, stored_at = item
        if self.ttl is not None and (time.monotonic() - stored_at) > self.ttl:
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return value

    def put(self, key: K, value: V) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = (value, time.monotonic())
        while len(self._data) > self.capacity:
            self._data.popitem(last=False)

    def __contains__(self, key: object) -> bool:
        return self.get(key) is not None  # type: ignore[arg-type]

    def __len__(self) -> int:
        return len(self._data)
