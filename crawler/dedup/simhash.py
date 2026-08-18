"""SimHash near-duplicate detection.

SimHash maps a document to a 64-bit fingerprint such that *similar* documents
have a small Hamming distance. Unlike a cryptographic hash (where one changed
word flips ~half the bits), SimHash is locality-sensitive: small content changes
move only a few bits, so boilerplate-heavy near-duplicates cluster together.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def _token_hash(token: str) -> int:
    """Stable 64-bit hash of a token (blake2b, platform-independent)."""
    return int.from_bytes(hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest(), "big")


def simhash_text(text: str, bits: int = 64) -> int:
    """Compute the ``bits``-wide SimHash of ``text`` (0 for empty input)."""
    tokens = _TOKEN_RE.findall(text.lower())
    if not tokens:
        return 0

    vector = [0] * bits
    for token, weight in Counter(tokens).items():
        h = _token_hash(token)
        for i in range(bits):
            if (h >> i) & 1:
                vector[i] += weight
            else:
                vector[i] -= weight

    fingerprint = 0
    for i in range(bits):
        if vector[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def band_values(fingerprint: int, bands: int, bits: int = 64) -> list[int]:
    """Split a fingerprint into ``bands`` equal chunks for LSH bucketing.

    Two fingerprints within Hamming distance ``< bands`` must, by pigeonhole,
    share at least one identical band - so any band collision is a candidate
    pair to verify exactly.
    """
    band_size = bits // bands
    mask = (1 << band_size) - 1
    return [(fingerprint >> (b * band_size)) & mask for b in range(bands)]
