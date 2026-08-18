"""Single-node crawl engine: a pool of async workers over an in-memory frontier.

Termination is driven by ``Queue.join()``: workers call ``task_done`` for every
item (even ones skipped after the page budget is hit), so ``join`` returns once
the frontier drains. New child URLs are only enqueued while below ``max_depth``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .. import metrics
from ..config import Settings, get_settings
from ..frontier.memory import MemoryFrontier
from ..logging_setup import get_logger
from ..models import FrontierItem
from ..url_utils import normalize_url, registered_domain
from .pipeline import Pipeline

log = get_logger(__name__)


@dataclass(slots=True)
class CrawlStats:
    processed: int
    discovered: int
    elapsed: float


class CrawlEngine:
    def __init__(
        self,
        pipeline: Pipeline,
        frontier: MemoryFrontier,
        settings: Settings | None = None,
        *,
        max_depth: int = 2,
        max_pages: int = 100,
        concurrency: int = 10,
        same_domain_only: bool = True,
        allowed_domains: set[str] | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.frontier = frontier
        self.settings = settings or get_settings()
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.concurrency = concurrency
        self.same_domain_only = same_domain_only
        # When set, children are restricted to these registered domains; takes
        # precedence over ``same_domain_only``. Lets a dashboard-launched crawl
        # pass an explicit allow-list.
        self.allowed_domains = allowed_domains
        self._processed = 0
        self._seed_domains: set[str] = set()

    async def run(self, seeds: list[str], crawl_id: int | None = None) -> CrawlStats:
        start = time.perf_counter()
        for seed in seeds:
            normalized = normalize_url(seed)
            if normalized is None:
                log.warning("invalid_seed", seed=seed)
                continue
            self._seed_domains.add(registered_domain(normalized))
            await self.frontier.add(FrontierItem(url=normalized, depth=0, crawl_id=crawl_id))

        workers = [asyncio.create_task(self._worker()) for _ in range(self.concurrency)]
        await self.frontier.join()
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        return CrawlStats(
            processed=self._processed,
            discovered=self.frontier.seen_count(),
            elapsed=time.perf_counter() - start,
        )

    async def _worker(self) -> None:
        while True:
            item = await self.frontier.get()
            try:
                if self._processed >= self.max_pages:
                    continue
                await self._handle(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("worker_error", url=item.url)
            finally:
                self.frontier.task_done()

    async def _handle(self, item: FrontierItem) -> None:
        result = await self.pipeline.process(item)
        if result.fetched:
            self._processed += 1
            metrics.FRONTIER_DEPTH.set(self.frontier.qsize())

        if result.duplicate or item.depth >= self.max_depth:
            return

        for link in result.links:
            child_url = link.url
            if len(child_url) > self.settings.crawl.max_url_length:
                continue
            if self.allowed_domains is not None:
                if registered_domain(child_url) not in self.allowed_domains:
                    continue
            elif self.same_domain_only and registered_domain(child_url) not in self._seed_domains:
                continue
            added = await self.frontier.add(
                FrontierItem(url=child_url, depth=item.depth + 1, crawl_id=item.crawl_id)
            )
            if added:
                metrics.URLS_ENQUEUED.inc()
