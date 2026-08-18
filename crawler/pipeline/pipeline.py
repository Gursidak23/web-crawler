"""Per-URL processing: fetch -> parse -> (politeness/dedup) -> store.

This is the single unit of work shared by the single-node engine (Phase 1) and
the Kafka worker (Phase 4). Politeness (Phase 2) and content dedup (Phase 3) are
injected collaborators, so this class stays the stable core of the crawler.
"""

from __future__ import annotations

import contextlib
import hashlib

from .. import metrics
from ..config import Settings, get_settings
from ..dedup.dedup_service import ContentDedup
from ..dedup.simhash import simhash_text
from ..fetcher import Fetcher
from ..models import FrontierItem, PageRecord, ProcessResult
from ..parser import extract_text, parse_page
from ..resilience.backpressure import Backpressure
from ..resilience.budget import DomainBudget
from ..resilience.circuit_breaker import CircuitBreaker
from ..robots.politeness import Politeness
from ..storage.object_store import ObjectStore
from ..storage.sinks import DocumentSink
from ..url_utils import host_of, normalize_url, registered_domain


class Pipeline:
    def __init__(
        self,
        fetcher: Fetcher,
        sink: DocumentSink,
        settings: Settings | None = None,
        *,
        politeness: Politeness | None = None,
        content_dedup: ContentDedup | None = None,
        object_store: ObjectStore | None = None,
        conditional_get: bool = False,
        circuit_breaker: CircuitBreaker | None = None,
        domain_budget: DomainBudget | None = None,
        backpressure: Backpressure | None = None,
    ) -> None:
        self.fetcher = fetcher
        self.sink = sink
        self.settings = settings or get_settings()
        self.politeness = politeness
        self.content_dedup = content_dedup
        self.object_store = object_store
        # Recrawl efficiency: re-send stored ETag/Last-Modified validators so
        # unchanged pages come back as 304 and skip download + reprocessing.
        self.conditional_get = conditional_get
        self.circuit_breaker = circuit_breaker
        self.domain_budget = domain_budget
        self.backpressure = backpressure

    def _record_outcome(self, host: str, status: int, error: str | None) -> None:
        """Feed the circuit breaker: transport errors and 5xx are host failures;
        everything else (including 4xx/304) means the host is healthy."""
        if self.circuit_breaker is None:
            return
        if error is not None or status >= 500 or status == 0:
            self.circuit_breaker.record_failure(host)
        else:
            self.circuit_breaker.record_success(host)

    async def _conditional_headers(self, url: str) -> dict[str, str] | None:
        if not self.conditional_get:
            return None
        # Validators are stored under the canonical URL, so normalize before lookup.
        lookup = normalize_url(url) or url
        validators = await self.sink.get_validators(lookup)
        if not validators:
            return None
        etag, last_modified = validators
        headers: dict[str, str] = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        return headers or None

    async def process(self, item: FrontierItem) -> ProcessResult:
        if self.politeness is not None and not await self.politeness.allowed(item.url):
            metrics.ROBOTS_BLOCKED.inc()
            return ProcessResult(item=item, skipped="robots")

        host = host_of(item.url)
        if self.circuit_breaker is not None and not self.circuit_breaker.allow(host):
            metrics.CIRCUIT_OPEN_SKIPS.inc()
            return ProcessResult(item=item, skipped="circuit_open")

        if self.domain_budget is not None:
            domain = registered_domain(item.url)
            if not await self.domain_budget.try_consume(domain):
                return ProcessResult(item=item, skipped="budget_exhausted")

        headers = await self._conditional_headers(item.url)
        slot = (
            self.politeness.slot(item.url)
            if self.politeness is not None
            else contextlib.nullcontext()
        )
        gate = self.backpressure if self.backpressure is not None else contextlib.nullcontext()
        async with gate, slot:
            result = await self.fetcher.fetch(item.url, headers=headers)

        self._record_outcome(host, result.status, result.error)
        if result.error is not None:
            return ProcessResult(item=item, skipped="fetch_error")

        if result.status == 304:
            # Unchanged since last crawl - nothing to store or expand.
            metrics.NOT_MODIFIED.inc()
            return ProcessResult(item=item, fetched=True, skipped="not_modified")

        canonical = normalize_url(result.final_url) or item.url
        body = result.body
        content_hash = hashlib.sha256(body).hexdigest() if body else None
        record = PageRecord(
            url=canonical,
            registered_domain=registered_domain(canonical),
            depth=item.depth,
            http_status=result.status,
            content_type=result.content_type,
            content_length=len(body),
            content_hash=content_hash,
            etag=result.header("ETag"),
            last_modified=result.header("Last-Modified"),
            crawl_id=item.crawl_id,
        )

        links = []
        duplicate = False
        if result.is_html and body:
            parsed = parse_page(canonical, body)
            record.title = parsed.title
            links = parsed.links
            metrics.LINKS_EXTRACTED.inc(len(links))

            if self.content_dedup is not None:
                record.simhash = simhash_text(extract_text(body))
                if await self.content_dedup.is_duplicate(record.simhash):
                    duplicate = True
                    metrics.DEDUP_SKIPPED.labels(kind="content").inc()
                    links = []  # near-duplicate: don't store edges or expand

        # Persist the raw body once per distinct content hash. Skipped for
        # near-duplicates since an equivalent body is already stored.
        if self.object_store is not None and body and content_hash and not duplicate:
            record.storage_key = await self.object_store.put(
                content_hash, body, result.content_type
            )
            metrics.BODIES_STORED.labels(
                backend=self.settings.storage.body_backend
            ).inc()

        record.links = links
        await self.sink.save_page(record)
        return ProcessResult(
            item=item,
            fetched=True,
            stored=True,
            duplicate=duplicate,
            links=links,
            record=record,
        )
