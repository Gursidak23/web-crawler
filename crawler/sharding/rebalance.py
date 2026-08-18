"""Helpers to analyze how a :class:`ConsistentHashRing` distributes and
rebalances keys.

These power the ``crawler ring-demo`` command and the rebalancing tests: they
quantify (a) how evenly domains spread across workers and (b) how few domains
move when a worker is added or removed - the whole point of consistent hashing
versus naive ``hash(key) % N`` sharding.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from .consistent_hash import ConsistentHashRing


def assignments(ring: ConsistentHashRing, keys: Iterable[str]) -> dict[str, str]:
    """Map each key to its owning node (skips keys with no node, i.e. empty ring)."""
    result: dict[str, str] = {}
    for key in keys:
        node = ring.get(key)
        if node is not None:
            result[key] = node
    return result


def distribution(ring: ConsistentHashRing, keys: Iterable[str]) -> Counter[str]:
    """Count how many keys land on each node."""
    return Counter(assignments(ring, keys).values())


def imbalance(ring: ConsistentHashRing, keys: Iterable[str]) -> float:
    """Return max/mean load ratio across nodes (1.0 = perfectly even)."""
    counts = distribution(ring, keys)
    if not counts:
        return 0.0
    loads = list(counts.values())
    # Account for nodes that received zero keys.
    node_count = len(ring)
    mean = sum(loads) / node_count if node_count else 0.0
    if mean == 0:
        return 0.0
    return max(loads) / mean


def churn(before: dict[str, str], after: dict[str, str]) -> float:
    """Fraction of keys whose owner changed between two assignment snapshots."""
    if not before:
        return 0.0
    moved = sum(1 for key, node in before.items() if after.get(key) != node)
    return moved / len(before)


def naive_churn(keys: Iterable[str], before_n: int, after_n: int) -> float:
    """Churn of ``hash(key) % N`` sharding when going from ``before_n`` to ``after_n``
    nodes - the baseline consistent hashing improves on."""
    keys = list(keys)
    if not keys or before_n <= 0 or after_n <= 0:
        return 0.0
    moved = 0
    for key in keys:
        h = ConsistentHashRing._hash(key)
        if h % before_n != h % after_n:
            moved += 1
    return moved / len(keys)
