"""Unit tests for SimHash fingerprints and Hamming distance."""

from crawler.dedup import band_values, hamming_distance, simhash_text

BASE = (
    "The quick brown fox jumps over the lazy dog. "
    "Web crawlers traverse hyperlinks to discover pages across the internet. "
    "Politeness and deduplication keep the crawl efficient and respectful."
) * 3


def test_identical_text_has_zero_distance():
    assert hamming_distance(simhash_text(BASE), simhash_text(BASE)) == 0


def test_similar_text_is_closer_than_different_text():
    similar = BASE.replace("lazy dog", "sleepy cat")
    different = "Completely unrelated content about quantum computing and linear algebra." * 5

    base_fp = simhash_text(BASE)
    near = hamming_distance(base_fp, simhash_text(similar))
    far = hamming_distance(base_fp, simhash_text(different))

    assert near < far
    assert far > 12


def test_empty_text_is_zero():
    assert simhash_text("") == 0


def test_band_values_partition_the_fingerprint():
    fp = (0xABCD << 48) | (0x1234 << 32) | (0x5678 << 16) | 0x9ABC
    assert band_values(fp, bands=4, bits=64) == [0x9ABC, 0x5678, 0x1234, 0xABCD]
