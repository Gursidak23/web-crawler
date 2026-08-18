"""Unit tests for the hand-rolled Bloom filter."""

import pytest

from crawler.dedup import BloomFilter


def test_no_false_negatives():
    bloom = BloomFilter(expected_insertions=1000, false_positive_rate=0.01)
    keys = [f"http://example.com/{i}" for i in range(1000)]
    for key in keys:
        bloom.check_and_add(key)
    # A Bloom filter must never report an inserted key as absent.
    assert all(key in bloom for key in keys)


def test_check_and_add_reports_novelty():
    bloom = BloomFilter(expected_insertions=100, false_positive_rate=0.01)
    assert bloom.check_and_add("http://a.com/x") is True
    assert bloom.check_and_add("http://a.com/x") is False
    assert len(bloom) == 1


def test_false_positive_rate_within_bound():
    n = 10_000
    p = 0.01
    bloom = BloomFilter(expected_insertions=n, false_positive_rate=p)
    for i in range(n):
        bloom.check_and_add(f"inserted-{i}")

    trials = 10_000
    false_positives = sum(1 for i in range(trials) if f"absent-{i}" in bloom)
    observed = false_positives / trials

    # Allow generous head-room for statistical noise, but it must be in the ballpark.
    assert observed < 3 * p


def test_rejects_bad_parameters():
    with pytest.raises(ValueError):
        BloomFilter(expected_insertions=0)
    with pytest.raises(ValueError):
        BloomFilter(expected_insertions=10, false_positive_rate=1.5)
