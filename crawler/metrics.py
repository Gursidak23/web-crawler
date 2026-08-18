"""Centralized Prometheus metrics for the crawler.

Each worker/process exposes these on its own ``/metrics`` endpoint; Prometheus
scrapes every instance and aggregates.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

PAGES_FETCHED = Counter(
    "crawler_pages_fetched_total",
    "Pages fetched, labeled by HTTP status class and outcome.",
    ["status", "outcome"],
)

FETCH_LATENCY = Histogram(
    "crawler_fetch_latency_seconds",
    "Wall-clock latency of a single fetch.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30),
)

BYTES_DOWNLOADED = Counter(
    "crawler_bytes_downloaded_total",
    "Total bytes downloaded across all fetches.",
)

LINKS_EXTRACTED = Counter(
    "crawler_links_extracted_total",
    "Links discovered while parsing pages.",
)

URLS_ENQUEUED = Counter(
    "crawler_urls_enqueued_total",
    "URLs pushed into the frontier.",
)

DEDUP_SKIPPED = Counter(
    "crawler_dedup_skipped_total",
    "Items skipped by deduplication, labeled by kind (url|content).",
    ["kind"],
)

ROBOTS_BLOCKED = Counter(
    "crawler_robots_blocked_total",
    "Fetches blocked by robots.txt.",
)

NOT_MODIFIED = Counter(
    "crawler_not_modified_total",
    "Conditional GETs that returned 304 Not Modified (recrawl savings).",
)

BODIES_STORED = Counter(
    "crawler_bodies_stored_total",
    "Raw page bodies written to the object store, labeled by backend.",
    ["backend"],
)

RATE_LIMIT_WAITS = Counter(
    "crawler_rate_limit_waits_total",
    "Number of times a fetch waited on the per-host rate limiter.",
)

BUDGET_EXHAUSTED = Counter(
    "crawler_budget_exhausted_total",
    "URLs skipped because their domain's crawl budget was used up.",
)

CIRCUIT_OPEN_SKIPS = Counter(
    "crawler_circuit_open_skips_total",
    "Fetches skipped because the per-host circuit breaker was open.",
)

CIRCUIT_STATE = Gauge(
    "crawler_circuit_breakers_open",
    "Number of hosts whose circuit breaker is currently open.",
)

RECRAWL_SCHEDULED = Counter(
    "crawler_recrawl_scheduled_total",
    "URLs scheduled for a future recrawl.",
)

FRONTIER_DEPTH = Gauge(
    "crawler_frontier_depth",
    "Approximate number of URLs currently waiting in the frontier.",
)

ACTIVE_FETCHES = Gauge(
    "crawler_active_fetches",
    "Fetches currently in flight.",
)


def status_class(status: int) -> str:
    """Map an HTTP status code to a coarse class label, e.g. 200 -> '2xx'."""
    if status <= 0:
        return "error"
    return f"{status // 100}xx"
