"""In-memory domain objects passed between crawler stages."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class FrontierItem:
    """A URL waiting to be crawled."""

    url: str
    depth: int = 0
    crawl_id: int | None = None


@dataclass(slots=True)
class Link:
    """An outbound link discovered on a page."""

    url: str
    anchor: str | None = None


@dataclass(slots=True)
class FetchResult:
    url: str
    final_url: str
    status: int
    headers: dict[str, str]
    body: bytes
    content_type: str | None
    elapsed: float
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and 200 <= self.status < 300

    @property
    def is_html(self) -> bool:
        ct = (self.content_type or "").lower()
        return "text/html" in ct or "application/xhtml+xml" in ct

    def header(self, name: str) -> str | None:
        """Case-insensitive header lookup (servers vary, e.g. ETag vs Etag)."""
        target = name.lower()
        for key, value in self.headers.items():
            if key.lower() == target:
                return value
        return None


@dataclass(slots=True)
class ParsedPage:
    url: str
    title: str | None = None
    links: list[Link] = field(default_factory=list)
    text: str = ""


@dataclass(slots=True)
class PageRecord:
    """Everything we persist about a single fetched page."""

    url: str
    registered_domain: str
    depth: int
    http_status: int | None = None
    content_type: str | None = None
    content_length: int | None = None
    title: str | None = None
    content_hash: str | None = None
    simhash: int | None = None
    storage_key: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    crawl_id: int | None = None
    links: list[Link] = field(default_factory=list)


@dataclass(slots=True)
class ProcessResult:
    """Outcome of running one :class:`FrontierItem` through the pipeline."""

    item: FrontierItem
    fetched: bool = False
    stored: bool = False
    duplicate: bool = False
    skipped: str | None = None
    links: list[Link] = field(default_factory=list)
    record: PageRecord | None = None
