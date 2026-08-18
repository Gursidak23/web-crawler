# Distributed Web Crawler (Python)

A horizontally-scalable web crawler built to demonstrate real systems-design
depth: a Kafka-backed URL frontier, per-domain politeness, deliberate
data-structure choices (Bloom filter, SimHash, consistent hashing), and
operational maturity (tests, metrics, dashboards, CI).

It is built in independent phases, so every layer can be run and demoed on its
own. See [`DESIGN.md`](DESIGN.md) for the engineering trade-offs behind each
decision.

## Highlights

- **Async fetch pipeline** built on `asyncio` + `aiohttp` with bounded global and
  per-host concurrency, retries, timeouts and streaming size caps.
- **URL frontier** that is pluggable: an in-memory BFS frontier for single-node
  runs, or a **Kafka**-backed distributed frontier **partitioned by registered
  domain** (via a consistent-hash ring) so one worker owns a domain at a time -
  natural politeness and cache locality.
- **Politeness**: `robots.txt` parsing (with `crawl-delay`/wildcards) cached in an
  LRU + Redis, and a per-host **token-bucket rate limiter** implemented as an
  atomic **Redis + Lua** script.
- **Deduplication** as a data-structure showcase: a hand-rolled, Redis-backed
  **scalable Bloom filter** for the URL seen-set, and **64-bit SimHash** +
  Hamming distance with **LSH banding** for near-duplicate page detection.
- **Sharding**: a `ConsistentHashRing` (virtual nodes) maps domains to workers
  with minimal key movement when the worker set changes.
- **Storage**: pluggable SQL backend — an embedded **SQLite** file by default
  (zero external services) or **Postgres** for the distributed fleet — holding
  document metadata + the link graph; raw page bodies go inline or to MinIO.
- **Web dashboard**: a built-in UI at `/` (FastAPI + Jinja2 + Tailwind +
  Chart.js, no build step) to submit crawls and explore stats, domains, the
  link graph, and stored documents live.
- **Observability**: Prometheus metrics on `/metrics` (optionally scraped by a
  natively-run Prometheus/Grafana) — fetch rate, latency p50/p95/p99, frontier
  depth, dedup hit ratio.
- **Tests**: `pytest` unit tests (no services needed) + optional Testcontainers
  integration tests; GitHub Actions CI runs lint + type-check + tests.

## Architecture

```mermaid
flowchart LR
    Seeds["Seed URLs / Control API"]
    subgraph workers [Crawler workers - scalable, stateless]
        Frontier["Frontier consumer"]
        Robots["Robots + politeness (token bucket)"]
        Fetcher["Async fetcher (aiohttp)"]
        Parser["Parser + URL normalizer"]
        Dedup["Dedup: Bloom (URL) + SimHash (content)"]
    end
    Kafka[["Kafka: frontier / parsed / retry / dlq"]]
    Redis[("Redis: Bloom, token buckets, robots+DNS cache, LSH bands")]
    Postgres[("Postgres: documents + link graph + domains")]
    Object[("MinIO: raw page bodies")]
    Prom["Prometheus + Grafana"]

    Seeds --> Kafka
    Kafka --> Frontier
    Frontier --> Robots
    Robots -->|"allowed + within rate"| Fetcher
    Robots -->|"rate/robots state"| Redis
    Fetcher -->|"raw body"| Object
    Fetcher --> Parser
    Parser --> Dedup
    Dedup -->|"seen?"| Redis
    Dedup -->|"new links, partitioned by domain"| Kafka
    Parser -->|"metadata + link edges"| Postgres
    workers --> Prom
```

## Tech stack

Python 3.11+, asyncio, aiohttp, selectolax, tldextract/w3lib, protego (robots),
aiokafka, redis, SQLAlchemy 2.0 (async) with SQLite (aiosqlite) or Postgres
(asyncpg) + Alembic, MinIO, FastAPI + Jinja2 dashboard, Prometheus, structlog,
pydantic-settings, pytest + Testcontainers, ruff + mypy. Managed with
[`uv`](https://docs.astral.sh/uv/).

## Getting started

No Docker required. Out of the box the crawler runs on an **embedded SQLite
database** with an in-memory frontier and in-process rate limiting / dedup, so a
single machine needs nothing but Python.

### Prerequisites

- Python 3.11+ (3.11 or 3.12)
- Optionally [`uv`](https://docs.astral.sh/uv/) (`pip install uv`) for the manual path below

### Quick start (recommended)

```bash
bash run.sh            # creates an isolated venv, installs, and serves the dashboard
bash run.sh --seeds https://example.com --max-pages 100
```

`run.sh` is self-contained: it builds a local `.venv` (so nothing conflicts with
your system Python), installs the project, and starts everything. Re-running is
cheap — the venv and install happen only once. Pass `PYTHON=python3.12 bash run.sh`
to pick a specific interpreter.

### 1) Manual install (alternative)

```bash
uv venv --python 3.11
uv pip install -e ".[dev]"
# ...or with plain tooling:
#   python3.11 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
```

The SQLite schema is created automatically on first use (crawl / API startup),
so there is no migration step for the default setup.

### 2) Run everything at once

```bash
# Serve the API + dashboard at http://localhost:8010/
bash run.sh                 # (or: uv run python run.py)

# ...or also launch a single-node crawl that streams into the dashboard live
bash run.sh --seeds https://example.com --max-pages 100
```

Then open **http://localhost:8010/** for the dashboard (submit crawls, watch
live stats, browse domains, the link graph, and stored documents); API docs are
at `/docs`. Run `python run.py --help` for all options (host/port, crawl
depth/pages/concurrency, and `--distributed` to launch the API + Kafka workers).

### 3) Or drive it directly

```bash
uv run crawler crawl https://example.com --max-depth 2 --max-pages 50   # one-off CLI crawl
uv run uvicorn crawler.api.app:app --port 8010                          # API + dashboard only
```

Pages, metadata and the link graph are written to `./crawler.db` (add
`--dry-run` to the CLI crawl to print results without touching the database).

### 4) (Optional) Scale out to the distributed fleet

The distributed frontier/worker path uses Postgres + Redis + Kafka. Install
them natively (or use managed services) and point the crawler at them:

```bash
export CRAWLER_POSTGRES__DSN=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler
export CRAWLER_REDIS__URL=redis://localhost:6379/0
export CRAWLER_KAFKA__BOOTSTRAP_SERVERS=localhost:9092
export CRAWLER_FRONTIER__BACKEND=kafka

uv run alembic upgrade head                                   # Postgres schema
uv run uvicorn crawler.api.app:app --port 8010                # control plane
uv run python -m crawler.worker --metrics-port 8001           # one per worker
```

## CLI

The `crawler` entry point bundles operational helpers (run `crawler --help`):

| Command             | What it does                                                            |
| ------------------- | ----------------------------------------------------------------------- |
| `crawl <seeds...>`  | Single-node BFS crawl (flags: `--polite`, `--dedup`, `--conditional`, `--store-bodies`, `--dry-run`) |
| `seed <seeds...>`   | Publish seed URLs to the Kafka frontier                                 |
| `worker`            | Run a distributed worker consuming the Kafka frontier                   |
| `recrawl`           | Re-enqueue the stalest stored pages (freshness scheduler)               |
| `graph`             | Recompute link-graph in/out-degree and print the most-linked pages      |
| `ring-demo`         | Show consistent-hash load balance + rebalance churn vs naive `hash % N` |
| `bench`             | Throughput benchmark against a generated fixture site                   |

## Control-plane API

Mounted under `/api/v1`, with interactive OpenAPI docs at `/docs`:

| Method & path             | Purpose                                            |
| ------------------------- | -------------------------------------------------- |
| `POST /api/v1/crawls`     | Create a crawl and seed the frontier               |
| `GET /api/v1/crawls`      | List recent crawls + their document counts         |
| `GET /api/v1/stats`       | Document/edge/domain counts + per-status histogram |
| `GET /api/v1/domains`     | Top registered domains by document count           |
| `GET /api/v1/documents`   | Browse/search stored documents (paginated)         |
| `GET /api/v1/documents/{id}` | A document's metadata + its outbound links      |
| `GET /api/v1/graph`       | Most linked-to pages (by in-degree)                |

## Configuration

All settings use the `CRAWLER_` prefix with `__` for nesting (see
[`crawler/config.py`](crawler/config.py) and [`.env.example`](.env.example)),
e.g. `CRAWLER_FETCH__CONCURRENCY=200`, `CRAWLER_FRONTIER__BACKEND=kafka`.

## Testing

```bash
uv run pytest -q             # unit tests (no Docker required)
uv run pytest -m integration # Testcontainers integration tests (requires Docker)
```

Integration tests are marked `integration` and skip automatically when Docker is
unavailable, so the default `pytest` run works everywhere.

## Benchmarks

A reproducible throughput benchmark ships in the box: it generates an
interlinked fixture site in-process and crawls it through the real pipeline
(fetch -> parse -> SimHash dedup), no infrastructure required.

```bash
uv run crawler bench --pages 1000 --concurrency 50
```

Representative single-node run (Apple-class laptop, fixture server sharing the
crawler's event loop, so tail latency is pessimistic; **throughput** is the
meaningful figure):

| Scenario                         | Pages/s | p50    | p95    | p99     | Dedup hit ratio |
| -------------------------------- | ------- | ------ | ------ | ------- | --------------- |
| Single-node, in-memory frontier  | ~960    | ~47 ms | ~61 ms | ~221 ms | 10.0%           |
| 3 workers, Kafka frontier        | scales ~linearly with workers; run a Postgres/Redis/Kafka stack to measure on your hardware |

The 10% dedup hit ratio matches the benchmark's 1-in-10 near-duplicate cluster,
confirming SimHash + LSH detection end-to-end.

## Observability

- Dashboard: the API serves a live web UI at http://localhost:8010/.
- Metrics: every process serves `/metrics` (worker on `--metrics-port`, API on
  `:8010/metrics`) in Prometheus format; point a natively-run Prometheus at
  those targets (see `monitoring/prometheus.yml`) to feed Grafana if desired.

## Project layout

- `fetcher/` - async HTTP downloader + DNS cache
- `robots/` - robots.txt cache + per-host politeness
- `parser/` - HTML parsing, link extraction, URL normalization
- `frontier/` - in-memory + Kafka frontier backends (Mercator front/back queues)
- `dedup/` - Bloom filter, SimHash, LSH
- `ratelimit/` - Redis + Lua token bucket
- `sharding/` - consistent-hash ring
- `storage/` - SQLAlchemy models, repositories, object store
- `pipeline/` - fetch -> parse -> dedup -> store -> enqueue orchestration
- `api/` - FastAPI control plane + metrics
