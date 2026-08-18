"""Run a single-node crawl in-process.

Shared by the ``run.py`` launcher (``--seeds``) and the dashboard's "Start a
crawl" button, so submitting a URL in the web UI runs a real crawl without
Docker, Kafka, or a separate worker fleet. Results stream into the same SQLite
(or Postgres) database the API reads from, so the dashboard updates live.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence

from .config import Settings
from .logging_setup import get_logger

log = get_logger(__name__)


async def execute_crawl(
    settings: Settings,
    *,
    crawl_id: int,
    seeds: Sequence[str],
    max_depth: int,
    max_pages: int,
    allowed_domains: Sequence[str] | None = None,
    same_domain_only: bool = True,
    concurrency: int = 10,
    polite: bool = True,
    dedup: bool = True,
) -> None:
    """Crawl ``seeds`` in-process and mark the crawl row done when finished.

    The caller is responsible for creating the ``Crawl`` record and passing its
    ``crawl_id``; this coroutine only runs the engine and updates the row's
    final status (``completed`` / ``failed`` / ``stopped``).
    """
    from .factory import (
        build_backpressure,
        build_circuit_breaker,
        build_content_dedup,
        build_domain_budget,
        build_politeness,
    )
    from .fetcher import Fetcher
    from .frontier.memory import MemoryFrontier
    from .pipeline import CrawlEngine, Pipeline
    from .storage.db import session_scope
    from .storage.orm import Crawl
    from .storage.repositories import GraphRepository
    from .storage.sinks import SqlDocumentSink

    log.info("crawl_started", crawl_id=crawl_id, seeds=list(seeds))
    sink = SqlDocumentSink(settings, object_store=None)
    frontier = MemoryFrontier()
    status = "completed"
    try:
        async with Fetcher(settings) as fetcher:
            pipeline = Pipeline(
                fetcher,
                sink,
                settings,
                politeness=build_politeness(fetcher, settings) if polite else None,
                content_dedup=build_content_dedup(settings) if dedup else None,
                object_store=None,
                conditional_get=False,
                circuit_breaker=build_circuit_breaker(settings),
                domain_budget=build_domain_budget(settings),
                backpressure=build_backpressure(settings),
            )
            engine = CrawlEngine(
                pipeline,
                frontier,
                settings,
                max_depth=max_depth,
                max_pages=max_pages,
                concurrency=concurrency,
                same_domain_only=same_domain_only,
                allowed_domains=set(allowed_domains) if allowed_domains else None,
            )
            stats = await engine.run(list(seeds), crawl_id=crawl_id)
        log.info(
            "crawl_finished",
            crawl_id=crawl_id,
            processed=stats.processed,
            discovered=stats.discovered,
            elapsed=round(stats.elapsed, 2),
        )
        # Compute link in/out degrees so the graph view is populated right away,
        # without a separate ``crawler graph`` step. Best-effort: a failure here
        # shouldn't flip an otherwise-successful crawl to "failed".
        try:
            async with session_scope(settings) as session:
                await GraphRepository(session).recompute_degrees()
        except Exception:
            log.warning("degree_recompute_failed", crawl_id=crawl_id)
    except asyncio.CancelledError:
        status = "stopped"
        raise
    except Exception:
        status = "failed"
        log.exception("crawl_error", crawl_id=crawl_id)
    finally:
        with contextlib.suppress(Exception):
            async with session_scope(settings) as session:
                row = await session.get(Crawl, crawl_id)
                if row is not None:
                    row.status = status
