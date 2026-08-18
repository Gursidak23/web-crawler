# Design notes & trade-offs

This document explains the "why" behind the crawler's main decisions. It is
organized roughly in the order a URL flows through the system.

## Requirements

- Crawl a configurable seed set, follow links breadth-first up to a depth/page
  budget, and persist page metadata + the discovered link graph.
- Be *polite*: honor `robots.txt`, cap per-host request rate, and identify with a
  clear User-Agent.
- Avoid redundant work: never enqueue the same URL twice, and detect
  near-duplicate page content.
- Scale horizontally by adding workers, without two workers hammering the same
  host.
- Be observable and operable: metrics, dashboards, structured logs, tests, CI.

## URL frontier (pluggable)

The frontier is the heart of a crawler. We expose a small `Frontier` interface
with two implementations:

- **In-memory** (`frontier/memory.py`): an `asyncio`-friendly BFS queue for
  single-node runs and tests. Zero infrastructure.
- **Kafka** (`frontier/kafka_frontier.py`): URLs are produced to a `frontier`
  topic **keyed by registered domain**. Kafka's partitioner (mirrored by our
  `ConsistentHashRing`) ensures all URLs for a domain land on the same partition,
  so a single worker owns that domain. This gives us **politeness and DNS/robots
  cache locality for free** and lets us scale by adding partitions + workers.

This follows the classic *Mercator* design: priority "front queues" feed
per-host "back queues" that enforce politeness.

## Politeness & robots.txt

`protego` parses `robots.txt` (supporting wildcards and `crawl-delay`). Results
are cached in a process-local LRU and in Redis so workers share them. Rate
limiting is a **token bucket** implemented as a single atomic **Redis + Lua**
script (one round-trip, no race conditions) keyed by host - the same pattern used
in the sibling url-shortener. A per-host `asyncio.Semaphore` caps concurrency.

## Deduplication (data-structure showcase)

- **URL dedup**: a **scalable Bloom filter** backed by a Redis bitfield. A Bloom
  filter answers "have we seen this URL?" in O(k) with no per-URL storage of the
  string itself - essential when the seen-set reaches billions. We accept a tuned
  false-positive rate (a FP merely skips a URL we might have crawled); false
  negatives are impossible, so we never double-crawl. We grow capacity by chaining
  filters (the "scalable" variant).
- **Content dedup**: **SimHash** produces a 64-bit fingerprint where similar
  documents have small Hamming distance. We bucket fingerprints with **LSH bands**
  in Redis so near-duplicate detection is sub-linear instead of comparing against
  every prior document. Threshold is configurable.

## Sharding

`sharding/consistent_hash.py` implements a hash ring with virtual nodes (Murmur3
via `mmh3`). Mapping domains to workers this way means adding/removing a worker
only remaps `1/N` of domains, avoiding a full reshuffle. The same ring informs
Kafka partition selection. `crawler ring-demo` quantifies this: scaling 4 -> 5
workers moves ~19% of domains with the ring versus ~80% with naive `hash % N`,
and virtual nodes keep the per-worker load within ~10% of the mean.

## Back-pressure, budgets & circuit breaking

Three independent guards keep one misbehaving site from degrading the fleet
(`resilience/`):

- **Per-domain crawl budget** caps pages per registered domain so a giant site
  can't monopolize the frontier. The Redis backend uses an atomic `INCRBY`
  counter shared across workers; an in-memory variant serves single-node runs.
- **Global back-pressure** is an `asyncio.Semaphore` capping total in-flight
  pipeline tasks per process - protecting the DB pool, sockets and CPU - on top
  of the per-host concurrency caps in politeness.
- **Per-host circuit breaker** trips after N consecutive failures (timeouts /
  5xx / connection errors), short-circuiting further fetches to that host, then
  half-opens after a cooldown to probe recovery. Because domains are pinned to a
  worker by the ring, an in-process breaker is sufficient and keeps the hot path
  Redis-free.

## Recrawl scheduling

Pages change at different rates, so a fixed interval is wasteful. `scheduler/`
keeps a min-heap (priority queue) ordered by "due time" and an **adaptive
interval**: unchanged pages back off geometrically up to a cap, while changed
pages reset to the minimum. `RecrawlRepository.select_stale` finds the oldest
documents in Postgres, and `crawler recrawl` feeds them back onto the frontier
(stalest first). Conditional GETs then make most refreshes cheap 304s.

## Storage

Postgres is the source of truth for `document` metadata and the `link_edge`
graph. Raw page bodies are large and rarely queried relationally, so they go to
**MinIO** (S3-compatible, gzipped) keyed by content hash so identical bodies are
stored once; a `postgres` body backend (the `page_body` table) is available for
a zero-dependency demo. Recrawls use conditional GETs (`ETag`/`If-Modified-Since`)
to skip unchanged pages (304).

Over the stored graph, `GraphRepository.recompute_degrees` derives each
document's in/out-degree with two set-based `UPDATE`s - the building block for
PageRank-style link analysis and the `GET /graph` "most linked-to pages" view.

## Control plane & observability

A FastAPI app exposes the control plane under `/api/v1` (create crawls, inspect
stats/domains/documents, read the top of the link graph) with auto-generated
OpenAPI docs at `/docs`. Route handlers stay thin by delegating to a
`CrawlService`; a Protocol lets tests inject an in-memory fake so the API is
covered without Postgres or Kafka. Every process serves Prometheus metrics on
`/metrics` (fetch rate/latency, frontier depth, dedup hits, 304 savings, open
circuits, budget exhaustion), scraped into a provisioned Grafana dashboard.

## Failure modes

- **Worker crash**: Kafka consumer groups re-assign partitions; at-least-once
  delivery + idempotent upserts (unique `url`) mean a reprocessed message is
  harmless.
- **Poison messages**: bounded retries route to a `retry` topic and finally a
  `dlq`.
- **Redis/Kafka down**: the limiter fails open (prefer availability for a crawler)
  and producers surface errors with backoff.
- **Crawl traps**: max depth, max pages per domain, URL normalization (strip
  fragments/tracking params), max URL length, and content-type/size caps.

## Future work

- JavaScript rendering via a headless browser (Playwright) for SPA-heavy sites.
- Focused/priority crawling (score URLs by relevance).
- Full PageRank iteration over the stored link graph (in/out-degree is in place).
- A distributed (Redis) circuit breaker if domains stop being worker-pinned.
- WARC archival output for interoperability with the wider crawling ecosystem.
