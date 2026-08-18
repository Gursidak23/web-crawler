"""Unit tests for the hand-rolled LRU cache."""

import time

import pytest

from crawler.cache import LruCache


def test_evicts_least_recently_used():
    cache: LruCache[str, int] = LruCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1  # touch "a" so "b" becomes LRU
    cache.put("c", 3)  # evicts "b"
    assert cache.get("b") is None
    assert cache.get("a") == 1
    assert cache.get("c") == 3
    assert len(cache) == 2


def test_ttl_expiry():
    cache: LruCache[str, int] = LruCache(capacity=10, ttl=0.05)
    cache.put("a", 1)
    assert cache.get("a") == 1
    time.sleep(0.07)
    assert cache.get("a") is None


def test_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        LruCache(capacity=0)
