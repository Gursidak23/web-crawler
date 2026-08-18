"""A consistent-hash ring with virtual nodes.

Hashing a key onto a ring and walking clockwise to the next node means that
adding or removing a node only remaps the keys near that node (~1/N of keys),
instead of reshuffling everything as ``hash(key) % N`` would. Virtual nodes
(many ring points per physical node) smooth out the otherwise lumpy key
distribution.

Used to map ``registered_domain -> worker`` (so one worker owns a domain) and to
choose the Kafka partition for a domain.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable

import mmh3


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str] | None = None, virtual_nodes: int = 150) -> None:
        if virtual_nodes <= 0:
            raise ValueError("virtual_nodes must be positive")
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}
        self._sorted_hashes: list[int] = []
        self._nodes: set[str] = set()
        for node in nodes or ():
            self.add_node(node)

    @staticmethod
    def _hash(key: str) -> int:
        return mmh3.hash(key, signed=False)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}#{i}")
            self._ring[h] = node
            bisect.insort(self._sorted_hashes, h)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}#{i}")
            if self._ring.pop(h, None) is not None:
                idx = bisect.bisect_left(self._sorted_hashes, h)
                if idx < len(self._sorted_hashes) and self._sorted_hashes[idx] == h:
                    self._sorted_hashes.pop(idx)

    def get(self, key: str) -> str | None:
        """Return the node that owns ``key`` (the next node clockwise)."""
        if not self._sorted_hashes:
            return None
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_hashes, h)
        if idx == len(self._sorted_hashes):
            idx = 0
        return self._ring[self._sorted_hashes[idx]]

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)
