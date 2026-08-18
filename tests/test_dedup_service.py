"""Unit tests for the in-memory LSH-backed content dedup service.

We feed crafted fingerprints to exercise the LSH bucketing + Hamming
verification precisely (independent of SimHash text behavior).
"""

from crawler.dedup import InMemoryContentDedup

BANDS = 4
BITS = 64  # 4 bands of 16 bits each


async def test_detects_near_duplicate_sharing_a_band():
    dedup = InMemoryContentDedup(bands=BANDS, bits=BITS, threshold=3)

    base = 0
    # Differs from base by 2 bits, both inside band 0 -> bands 1..3 still match.
    near = 0b11

    assert await dedup.is_duplicate(base) is False  # first sighting -> indexed
    assert await dedup.is_duplicate(near) is True  # within threshold of base
    assert await dedup.is_duplicate(base) is True  # exact repeat is a duplicate


async def test_distinct_content_is_not_duplicate():
    dedup = InMemoryContentDedup(bands=BANDS, bits=BITS, threshold=3)

    base = 0
    # One differing bit in every 16-bit band -> shares no band with base, so it
    # is not even a candidate (and its Hamming distance of 4 exceeds threshold).
    far = 1 | (1 << 16) | (1 << 32) | (1 << 48)

    assert await dedup.is_duplicate(base) is False
    assert await dedup.is_duplicate(far) is False


async def test_threshold_is_respected():
    # With threshold 1, a 2-bit difference is NOT a duplicate even if a band matches.
    dedup = InMemoryContentDedup(bands=BANDS, bits=BITS, threshold=1)
    assert await dedup.is_duplicate(0) is False
    assert await dedup.is_duplicate(0b11) is False  # distance 2 > threshold 1
