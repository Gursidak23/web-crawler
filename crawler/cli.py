"""Command-line interface for single-node crawls and ops helpers."""

from __future__ import annotations

import asyncio

import typer

from .config import get_settings
from .factory import (
    build_backpressure,
    build_circuit_breaker,
    build_content_dedup,
    build_domain_budget,
    build_object_store_for,
    build_politeness,
)
from .fetcher import Fetcher
from .frontier.memory import MemoryFrontier
from .logging_setup import configure_logging
from .pipeline import CrawlEngine, CrawlStats, Pipeline
from .storage.sinks import InMemoryDocumentSink, SqlDocumentSink

app = typer.Typer(help="Moonshot distributed web crawler", no_args_is_help=True)


@app.command()
def version() -> None:
    """Print the package version."""
    from . import __version__

    typer.echo(__version__)


async def _run_crawl(
    seeds: list[str],
    *,
    max_depth: int,
    max_pages: int,
    concurrency: int,
    same_domain: bool,
    dry_run: bool,
    polite: bool,
    dedup: bool,
    store_bodies: bool,
    conditional: bool,
) -> tuple[CrawlStats, InMemoryDocumentSink | None]:
    settings = get_settings()
    sink: InMemoryDocumentSink | SqlDocumentSink
    mem_sink: InMemoryDocumentSink | None = None
    object_store = build_object_store_for(settings, enabled=store_bodies and not dry_run)
    if dry_run:
        mem_sink = InMemoryDocumentSink()
        sink = mem_sink
    else:
        from .storage.db import ensure_schema

        await ensure_schema(settings)
        sink = SqlDocumentSink(settings, object_store)

    frontier = MemoryFrontier()
    async with Fetcher(settings) as fetcher:
        politeness = build_politeness(fetcher, settings) if polite else None
        content_dedup = build_content_dedup(settings) if dedup else None
        pipeline = Pipeline(
            fetcher,
            sink,
            settings,
            politeness=politeness,
            content_dedup=content_dedup,
            object_store=object_store if (store_bodies and not dry_run) else None,
            conditional_get=conditional,
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
            same_domain_only=same_domain,
        )
        stats = await engine.run(seeds)
    await sink.close()
    return stats, mem_sink


@app.command()
def crawl(
    seeds: list[str] = typer.Argument(..., help="One or more seed URLs"),
    max_depth: int = typer.Option(2, "--max-depth", help="Maximum link depth to follow"),
    max_pages: int = typer.Option(100, "--max-pages", help="Stop after this many pages"),
    concurrency: int = typer.Option(10, "--concurrency", help="Concurrent fetch workers"),
    same_domain: bool = typer.Option(
        True, "--same-domain/--all-domains", help="Restrict to the seed domains"
    ),
    polite: bool = typer.Option(
        True, "--polite/--impolite", help="Honor robots.txt and per-host rate limits"
    ),
    dedup: bool = typer.Option(
        True, "--dedup/--no-dedup", help="Skip near-duplicate pages (SimHash + LSH)"
    ),
    store_bodies: bool = typer.Option(
        True, "--store-bodies/--no-store-bodies", help="Persist raw page bodies (gzipped)"
    ),
    conditional: bool = typer.Option(
        False,
        "--conditional/--no-conditional",
        help="Send ETag/If-Modified-Since validators for efficient recrawls",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Do not write to Postgres; print results instead"
    ),
) -> None:
    """Run a single-node breadth-first crawl from the given seed URLs."""
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)

    stats, mem_sink = asyncio.run(
        _run_crawl(
            seeds,
            max_depth=max_depth,
            max_pages=max_pages,
            concurrency=concurrency,
            same_domain=same_domain,
            dry_run=dry_run,
            polite=polite,
            dedup=dedup,
            store_bodies=store_bodies,
            conditional=conditional,
        )
    )

    if mem_sink is not None:
        for page in mem_sink.pages[:25]:
            typer.echo(f"  {page.http_status}  {page.url}  ({len(page.links)} links)")

    typer.echo(
        f"Crawled {stats.processed} pages, discovered {stats.discovered} URLs "
        f"in {stats.elapsed:.2f}s"
    )


async def _seed(seeds: list[str], max_depth: int) -> int:
    from .frontier import FrontierMessage, KafkaFrontier, ensure_topics
    from .url_utils import normalize_url

    settings = get_settings()
    await ensure_topics(settings)
    frontier = KafkaFrontier(settings)
    await frontier.start()
    count = 0
    try:
        for seed in seeds:
            normalized = normalize_url(seed)
            if normalized is None:
                continue
            await frontier.add_message(FrontierMessage(url=normalized, depth=0))
            count += 1
    finally:
        await frontier.stop()
    return count


@app.command()
def seed(
    seeds: list[str] = typer.Argument(..., help="Seed URLs to push onto the Kafka frontier"),
    max_depth: int = typer.Option(2, "--max-depth"),
) -> None:
    """Publish seed URLs to the distributed (Kafka) frontier."""
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    count = asyncio.run(_seed(seeds, max_depth))
    typer.echo(f"Seeded {count} URL(s) to the frontier")


@app.command()
def worker(
    metrics_port: int = typer.Option(8001, "--metrics-port", help="Port for /metrics"),
) -> None:
    """Run a distributed crawler worker consuming the Kafka frontier."""
    from .worker import run_worker

    asyncio.run(run_worker(metrics_port))


async def _recrawl(older_than_hours: float, limit: int) -> int:
    from .frontier import FrontierMessage, KafkaFrontier, ensure_topics
    from .scheduler import RecrawlScheduler
    from .storage.db import dispose_engine, session_scope
    from .storage.repositories import RecrawlRepository

    settings = get_settings()
    async with session_scope(settings) as session:
        stale = await RecrawlRepository(session).select_stale(
            older_than_seconds=older_than_hours * 3600, limit=limit
        )
    await dispose_engine()

    # Order due work through the freshness priority queue (stalest first).
    scheduler = RecrawlScheduler()
    for url, depth, crawl_id in stale:
        scheduler.schedule(url, interval=0.0, depth=depth, crawl_id=crawl_id)

    await ensure_topics(settings)
    frontier = KafkaFrontier(settings)
    await frontier.start()
    count = 0
    try:
        for entry in scheduler.pop_due():
            await frontier.add_message(
                FrontierMessage(url=entry.url, depth=entry.depth, crawl_id=entry.crawl_id)
            )
            count += 1
    finally:
        await frontier.stop()
    return count


@app.command()
def recrawl(
    older_than_hours: float = typer.Option(
        1.0, "--older-than-hours", help="Recrawl pages last fetched before this many hours ago"
    ),
    limit: int = typer.Option(100, "--limit", help="Max pages to enqueue"),
) -> None:
    """Re-enqueue the stalest stored pages onto the Kafka frontier for a refresh."""
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    count = asyncio.run(_recrawl(older_than_hours, limit))
    typer.echo(f"Re-enqueued {count} stale page(s) for recrawl")


async def _recompute_graph(top: int) -> list[tuple[str, int]]:
    from .storage.db import dispose_engine, ensure_schema, session_scope
    from .storage.repositories import GraphRepository

    settings = get_settings()
    await ensure_schema(settings)
    async with session_scope(settings) as session:
        repo = GraphRepository(session)
        await repo.recompute_degrees()
    async with session_scope(settings) as session:
        repo = GraphRepository(session)
        rows = await repo.top_by_in_degree(top)
    await dispose_engine()
    return rows


@app.command()
def graph(
    top: int = typer.Option(10, "--top", help="How many most-linked pages to show"),
) -> None:
    """Recompute in/out-degree over the stored link graph and show top pages."""
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    rows = asyncio.run(_recompute_graph(top))
    typer.echo("Recomputed link-graph degrees. Most linked-to pages:")
    for url, in_degree in rows:
        typer.echo(f"  {in_degree:>6}  {url}")


@app.command()
def bench(
    pages: int = typer.Option(500, "--pages", help="Number of fixture pages to crawl"),
    concurrency: int = typer.Option(50, "--concurrency", help="Concurrent fetch workers"),
    max_depth: int = typer.Option(12, "--max-depth"),
    dedup: bool = typer.Option(True, "--dedup/--no-dedup"),
) -> None:
    """Benchmark crawl throughput against a generated in-process fixture site."""
    from .bench import run_benchmark

    settings = get_settings()
    configure_logging("WARNING", settings.log_json)
    result = asyncio.run(
        run_benchmark(pages=pages, concurrency=concurrency, max_depth=max_depth, dedup=dedup)
    )
    typer.echo(
        f"Crawled {result.pages} pages in {result.elapsed_s:.2f}s "
        f"({result.pages_per_sec:.0f} pages/s, {result.mb_per_sec:.2f} MB/s)"
    )
    typer.echo(
        f"  fetch latency  p50={result.p50_ms:.1f}ms  "
        f"p95={result.p95_ms:.1f}ms  p99={result.p99_ms:.1f}ms"
    )
    typer.echo(
        f"  near-duplicates skipped: {result.dedup_hits} "
        f"({result.dedup_ratio:.1%} of pages)"
    )


@app.command(name="ring-demo")
def ring_demo(
    domains: int = typer.Option(10_000, "--domains", help="Number of synthetic domains"),
    workers: int = typer.Option(3, "--workers", help="Initial worker count"),
    add: int = typer.Option(1, "--add", help="Workers to add for the rebalance step"),
) -> None:
    """Demonstrate consistent-hashing load balance and minimal rebalance churn.

    Shows that scaling workers moves only ~1/N of domains (vs ~(N-1)/N for naive
    modulo sharding), and how evenly domains spread across workers.
    """
    from .sharding import (
        ConsistentHashRing,
        assignments,
        churn,
        distribution,
        imbalance,
        naive_churn,
    )

    keys = [f"domain-{i}.example" for i in range(domains)]
    initial = [f"worker-{i}" for i in range(workers)]

    ring = ConsistentHashRing(initial, virtual_nodes=get_settings().sharding.virtual_nodes)
    before = assignments(ring, keys)
    dist = distribution(ring, keys)

    typer.echo(f"{domains} domains across {workers} workers (virtual nodes smooth the load):")
    for node in sorted(dist):
        typer.echo(f"  {node}: {dist[node]} domains")
    typer.echo(f"  load imbalance (max/mean): {imbalance(ring, keys):.3f}")

    for i in range(add):
        ring.add_node(f"worker-{workers + i}")
    after = assignments(ring, keys)

    moved = churn(before, after)
    naive = naive_churn(keys, workers, workers + add)
    typer.echo(f"\nScaled {workers} -> {workers + add} workers:")
    typer.echo(f"  consistent-hash churn: {moved:.1%} of domains moved")
    typer.echo(f"  naive hash%N churn:    {naive:.1%} of domains moved")


if __name__ == "__main__":
    app()
