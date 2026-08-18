"""HTML parsing via selectolax (lexbor) - fast title/link/text extraction."""

from __future__ import annotations

from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..models import Link, ParsedPage
from ..url_utils import normalize_url

_SKIP_REL = {"nofollow"}  # links the parser still returns; engine may choose to honor


def parse_page(url: str, html: str | bytes) -> ParsedPage:
    """Parse a page, returning its title and de-duplicated, normalized links."""
    tree = HTMLParser(html)

    # Honor a <base href> for relative link resolution.
    base = url
    base_node = tree.css_first("base[href]")
    if base_node is not None:
        base_href = base_node.attributes.get("href")
        if base_href:
            base = urljoin(url, base_href)

    title_node = tree.css_first("title")
    title = title_node.text(strip=True) if title_node is not None else None

    links: list[Link] = []
    seen: set[str] = set()
    for anchor in tree.css("a[href]"):
        href = anchor.attributes.get("href")
        normalized = normalize_url(href, base)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        text = anchor.text(strip=True) or None
        links.append(Link(url=normalized, anchor=text))

    return ParsedPage(url=url, title=title, links=links)


def extract_text(html: str | bytes) -> str:
    """Extract visible text content (used for content fingerprinting in Phase 3)."""
    tree = HTMLParser(html)
    for tag in tree.css("script, style, noscript, template"):
        tag.decompose()
    body = tree.body or tree.root
    if body is None:
        return ""
    return body.text(separator=" ", strip=True)
