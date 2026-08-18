"""Pipeline tests for Phase 6 resilience: budgets and circuit breaking."""

from crawler.config import Settings
from crawler.fetcher import Fetcher
from crawler.models import FrontierItem
from crawler.pipeline import Pipeline
from crawler.resilience.budget import InMemoryDomainBudget
from crawler.resilience.circuit_breaker import CircuitBreaker, CircuitState
from crawler.storage.sinks import InMemoryDocumentSink

from .support import serve_site

PAGE = {"/": "<html><title>Home</title><body>hi</body></html>"}


async def test_domain_budget_blocks_after_limit():
    settings = Settings()
    sink = InMemoryDocumentSink()
    budget = InMemoryDomainBudget(limit=1)

    async with serve_site(PAGE) as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, domain_budget=budget)
        first = await pipeline.process(FrontierItem(url=base, depth=0))
        second = await pipeline.process(FrontierItem(url=base, depth=0))

    assert first.stored is True
    assert second.skipped == "budget_exhausted"
    assert len(sink.pages) == 1


async def test_circuit_breaker_opens_and_short_circuits():
    # Point at a closed port so every fetch fails fast; disable retries for speed.
    settings = Settings()
    settings.fetch.retries = 0
    settings.fetch.connect_timeout_seconds = 1.0
    settings.fetch.timeout_seconds = 2.0
    sink = InMemoryDocumentSink()
    cb = CircuitBreaker(failure_threshold=1, reset_seconds=60.0)
    dead = "http://127.0.0.1:9/"

    async with Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, circuit_breaker=cb)
        first = await pipeline.process(FrontierItem(url=dead, depth=0))
        # First attempt actually tries to fetch and fails, tripping the breaker.
        assert first.skipped == "fetch_error"
        assert cb.state("127.0.0.1") is CircuitState.OPEN

        second = await pipeline.process(FrontierItem(url=dead, depth=0))
        # Now the breaker is open: we skip without even attempting a fetch.
        assert second.skipped == "circuit_open"


async def test_circuit_breaker_stays_closed_on_success():
    settings = Settings()
    sink = InMemoryDocumentSink()
    cb = CircuitBreaker(failure_threshold=2, reset_seconds=60.0)

    async with serve_site(PAGE) as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, circuit_breaker=cb)
        result = await pipeline.process(FrontierItem(url=base, depth=0))

    assert result.stored is True
    assert cb.open_count() == 0
