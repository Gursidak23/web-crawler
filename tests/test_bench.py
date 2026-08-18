"""Smoke test for the throughput benchmark harness."""

from crawler.bench import generate_site, run_benchmark


def test_generate_site_has_expected_pages_and_dup_cluster():
    site = generate_site(30)
    assert "/" in site and "/p0" in site
    assert site["/"] == site["/p0"]
    # Pages at index 1, 11, 21 share the boilerplate body text (titles/links still
    # differ) so SimHash flags them as a near-duplicate cluster.
    boiler = "shared boilerplate duplicate content"
    assert boiler in site["/p1"]
    assert boiler in site["/p11"]
    assert boiler in site["/p21"]
    assert boiler not in site["/p2"]


async def test_benchmark_runs_and_detects_duplicates():
    result = await run_benchmark(pages=40, concurrency=10, max_depth=10)
    # All interlinked pages should be reachable and crawled.
    assert result.pages >= 38
    assert result.pages_per_sec > 0
    # ~1 in 10 pages is a near-duplicate; with dedup on we should skip several.
    assert result.dedup_hits >= 2
    assert result.p50_ms >= 0
