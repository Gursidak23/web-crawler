"""Throughput benchmark against a generated, in-process fixture site.

Spins up a synthetic interlinked website on localhost, crawls it single-node
with the real pipeline (fetch -> parse -> dedup), and reports pages/sec, latency
percentiles, download throughput, and the near-duplicate hit ratio. No external
infrastructure required, so it runs anywhere (incl. CI).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from aiohttp import web
from prometheus_client import REGISTRY

from .config import Settings
from .fetcher import Fetcher
from .frontier.memory import MemoryFrontier
from .pipeline import CrawlEngine, Pipeline
from .storage.sinks import InMemoryDocumentSink

_DUP_EVERY = 10  # ~1 in 10 pages is a near-duplicate of the cluster seed


def generate_site(pages: int) -> dict[str, str]:
    """Return ``{path: html}`` for an interlinked site with a near-dup cluster."""
    site: dict[str, str] = {}
    for i in range(pages):
        # Fan out to a handful of other pages so BFS spreads quickly.
        links = "".join(
            f'<a href="/p{(i * 7 + j) % pages}">link{j}</a>' for j in range(6)
        )
        if i % _DUP_EVERY == 1:
            body = "<p>" + ("shared boilerplate duplicate content " * 30) + "</p>"
        else:
            words = " ".join(f"word{i}x{k}" for k in range(40))
            body = f"<p>unique article {i}: {words}</p>"
        site[f"/p{i}"] = (
            f"<html><head><title>Page {i}</title></head>"
            f"<body>{body}{links}</body></html>"
        )
    site["/"] = site["/p0"]
    return site


class _RecordingFetcher(Fetcher):
    """A Fetcher that records the elapsed time of each successful fetch."""

    def __init__(self, settings: Settings, latencies: list[float]) -> None:
        super().__init__(settings)
        self._latencies = latencies

    async def fetch(self, url: str, *, headers: dict[str, str] | None = None):
        result = await super().fetch(url, headers=headers)
        if result.error is None:
            self._latencies.append(result.elapsed)
        return result


@dataclass(slots=True)
class BenchResult:
    pages: int
    elapsed_s: float
    pages_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mb_per_sec: float
    dedup_hits: int
    dedup_ratio: float


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(q * len(ordered)))
    return ordered[idx]


def _dedup_content_total() -> float:
    return REGISTRY.get_sample_value(
        "crawler_dedup_skipped_total", {"kind": "content"}
    ) or 0.0


async def run_benchmark(
    pages: int = 500,
    concurrency: int = 50,
    max_depth: int = 12,
    *,
    dedup: bool = True,
) -> BenchResult:
    site = generate_site(pages)

    async def handler(request: web.Request) -> web.Response:
        body = site.get(request.path)
        if body is None:
            return web.Response(status=404, text="not found")
        return web.Response(text=body, content_type="text/html")

    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    tcp = web.TCPSite(runner, "127.0.0.1", 0)
    await tcp.start()
    port = runner.addresses[0][1]
    base = f"http://127.0.0.1:{port}/"

    settings = Settings()
    settings.fetch.concurrency = concurrency
    latencies: list[float] = []
    total_bytes = 0

    from .factory import build_content_dedup

    dedup_before = _dedup_content_total()
    try:
        async with _RecordingFetcher(settings, latencies) as fetcher:
            sink = InMemoryDocumentSink()
            content_dedup = build_content_dedup(settings) if dedup else None
            pipeline = Pipeline(fetcher, sink, settings, content_dedup=content_dedup)
            engine = CrawlEngine(
                pipeline,
                MemoryFrontier(),
                settings,
                max_depth=max_depth,
                max_pages=pages,
                concurrency=concurrency,
                same_domain_only=True,
            )
            start = time.perf_counter()
            stats = await engine.run([base])
            elapsed = time.perf_counter() - start
            total_bytes = sum((p.content_length or 0) for p in sink.pages)
    finally:
        await runner.cleanup()

    dedup_hits = int(_dedup_content_total() - dedup_before)
    pps = stats.processed / elapsed if elapsed > 0 else 0.0
    mbps = (total_bytes / 1_000_000) / elapsed if elapsed > 0 else 0.0
    return BenchResult(
        pages=stats.processed,
        elapsed_s=elapsed,
        pages_per_sec=pps,
        p50_ms=_percentile(latencies, 0.50) * 1000,
        p95_ms=_percentile(latencies, 0.95) * 1000,
        p99_ms=_percentile(latencies, 0.99) * 1000,
        mb_per_sec=mbps,
        dedup_hits=dedup_hits,
        dedup_ratio=(dedup_hits / stats.processed if stats.processed else 0.0),
    )
