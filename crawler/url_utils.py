"""URL normalization and domain extraction.

Normalization is critical for a crawler: it collapses the many textual forms of
the *same* resource into one canonical key so the frontier and dedup layers do
not treat ``http://Example.com/a/`` and ``https://example.com/a?utm_source=x``
as distinct pages.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlsplit

import tldextract
from w3lib.url import canonicalize_url, url_query_cleaner

# Common analytics/tracking parameters that never identify a distinct resource.
TRACKING_PARAMS: tuple[str, ...] = (
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "_ga",
)

_SKIP_SCHEME_PREFIXES = ("javascript:", "mailto:", "tel:", "data:", "#")

# Use the public-suffix snapshot bundled with tldextract (no network at runtime).
_extract = tldextract.TLDExtract(suffix_list_urls=())


def normalize_url(href: str | None, base: str | None = None) -> str | None:
    """Return a canonical absolute http(s) URL, or ``None`` if not crawlable.

    Resolves relative links against ``base``, drops fragments and tracking
    params, lowercases scheme/host, removes default ports, and percent-encoding
    normalizes via :func:`w3lib.url.canonicalize_url`.
    """
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    lowered = href.lower()
    if lowered.startswith(_SKIP_SCHEME_PREFIXES):
        return None

    url = urljoin(base, href) if base else href
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None

    try:
        cleaned = url_query_cleaner(url, TRACKING_PARAMS, remove=True)
        canonical = canonicalize_url(cleaned)
    except Exception:
        return None
    return canonical or None


def host_of(url: str) -> str:
    """Return the lowercase hostname (including subdomains)."""
    return (urlsplit(url).hostname or "").lower()


def registered_domain(url: str) -> str:
    """Return the registrable domain, e.g. ``www.bbc.co.uk`` -> ``bbc.co.uk``.

    Falls back to the bare host for IP literals or unknown suffixes so the value
    is always a stable, non-empty partition key.
    """
    host = host_of(url)
    if not host:
        return ""
    extracted = _extract(host)
    return extracted.registered_domain or host


def same_registered_domain(a: str, b: str) -> bool:
    return registered_domain(a) == registered_domain(b)
