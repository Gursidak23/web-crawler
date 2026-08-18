"""Tests for consistent-hash load distribution and rebalancing churn."""

from crawler.sharding import (
    ConsistentHashRing,
    assignments,
    churn,
    imbalance,
    naive_churn,
)

KEYS = [f"domain-{i}.example" for i in range(5000)]


def test_distribution_is_reasonably_balanced():
    ring = ConsistentHashRing([f"w{i}" for i in range(4)], virtual_nodes=200)
    # With 200 virtual nodes the busiest worker stays close to the mean.
    assert imbalance(ring, KEYS) < 1.5


def test_adding_a_node_moves_few_keys():
    ring = ConsistentHashRing([f"w{i}" for i in range(4)], virtual_nodes=200)
    before = assignments(ring, KEYS)

    ring.add_node("w4")
    after = assignments(ring, KEYS)

    moved = churn(before, after)
    # Going 4 -> 5 nodes should move roughly 1/5 of keys; allow generous slack.
    assert 0.10 <= moved <= 0.30
    # And it must be far better than naive modulo resharding.
    assert moved < naive_churn(KEYS, 4, 5)


def test_removing_a_node_only_remaps_its_keys():
    ring = ConsistentHashRing([f"w{i}" for i in range(5)], virtual_nodes=200)
    before = assignments(ring, KEYS)

    ring.remove_node("w2")
    after = assignments(ring, KEYS)

    # Keys not owned by w2 must keep their owner; only w2's keys move.
    for key, node in before.items():
        if node != "w2":
            assert after[key] == node
    assert all(node != "w2" for node in after.values())


def test_naive_churn_is_high():
    # The baseline we improve on: hash % N reshuffles most keys when N changes.
    assert naive_churn(KEYS, 4, 5) > 0.5
