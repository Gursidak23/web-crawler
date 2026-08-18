"""Deduplication data structures: Bloom filter (URLs) + SimHash/LSH (content)."""

from .bloom import BloomFilter, RedisBloomFilter, bloom_indices
from .dedup_service import ContentDedup, InMemoryContentDedup, RedisContentDedup
from .seen import InMemoryUrlSeen, UrlSeen
from .simhash import band_values, hamming_distance, simhash_text

__all__ = [
    "BloomFilter",
    "RedisBloomFilter",
    "bloom_indices",
    "ContentDedup",
    "InMemoryContentDedup",
    "RedisContentDedup",
    "UrlSeen",
    "InMemoryUrlSeen",
    "simhash_text",
    "hamming_distance",
    "band_values",
]
