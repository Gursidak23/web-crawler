"""Unit tests for the consistent-hash ring."""

from crawler.sharding import ConsistentHashRing

KEYS = [f"domain-{i}.com" for i in range(2000)]


def test_assigns_every_key_to_a_member_node():
    ring = ConsistentHashRing(["w0", "w1", "w2"], virtual_nodes=100)
    assert all(ring.get(k) in {"w0", "w1", "w2"} for k in KEYS)


def test_distribution_is_roughly_balanced():
    ring = ConsistentHashRing(["w0", "w1", "w2"], virtual_nodes=200)
    counts = {"w0": 0, "w1": 0, "w2": 0}
    for key in KEYS:
        node = ring.get(key)
        assert node is not None
        counts[node] += 1
    expected = len(KEYS) / 3
    # Each node should be within ~35% of an even share.
    assert all(0.65 * expected < c < 1.35 * expected for c in counts.values())


def test_removing_a_node_only_remaps_its_keys():
    ring = ConsistentHashRing(["w0", "w1", "w2"], virtual_nodes=150)
    before = {k: ring.get(k) for k in KEYS}

    ring.remove_node("w1")
    after = {k: ring.get(k) for k in KEYS}

    for key in KEYS:
        if before[key] != "w1":
            # Keys not owned by the removed node must keep their assignment.
            assert after[key] == before[key]
        else:
            assert after[key] in {"w0", "w2"}


def test_adding_a_node_only_steals_keys_for_the_newcomer():
    ring = ConsistentHashRing(["w0", "w1"], virtual_nodes=150)
    before = {k: ring.get(k) for k in KEYS}

    ring.add_node("w2")
    after = {k: ring.get(k) for k in KEYS}

    moved = [k for k in KEYS if before[k] != after[k]]
    # Everything that moved went to the new node, and only a fraction moved.
    assert all(after[k] == "w2" for k in moved)
    assert len(moved) < len(KEYS) * 0.6
