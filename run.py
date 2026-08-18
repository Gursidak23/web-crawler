#!/usr/bin/env python
"""One-command launcher for the web crawler.

Runs everything at once, no Docker required:

    python run.py                          # serve the API + dashboard (SQLite)
    python run.py --seeds https://example.com
                                           # ...and run a single-node crawl that
                                           # streams live results into the dashboard
    python run.py --distributed --workers 3
                                           # launch the API + N Kafka workers
                                           # (needs Postgres/Redis/Kafka configured
                                           #  via CRAWLER_* env vars)

Open http://localhost:8010/ for the dashboard. Press Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import subprocess
import sys

import uvicorn

from crawler.config import get_settings
from crawler.logging_setup import configure_logging, get_logger

log = get_logger("run")

def _banner(host: str, port: int) -> str:
    shown = "localhost" if host in ("0.0.0.0", "127.0.0.1") else host
    base = f"http://{shown}:{port}"
    line = "=" * 52
    return (
        f"\n{line}\n"
        "  Moonshot Web Crawler\n"
        f"  Dashboard : {base}/\n"
        f"  API docs  : {base}/docs\n"
        f"  Metrics   : {base}/metrics\n"
        f"{line}\n"
    )


async def _run_crawl(settings, args: argparse.Namespace) -> None:
    """Run a single-node crawl in-process, writing results to the shared DB.

    Deliberately does not dispose the engine (the API server shares it).
    """
    from crawler.local_crawl import execute_crawl
    from crawler.storage.db import session_scope
    from crawler.storage.orm import Crawl

    # Record the crawl so it shows up on the dashboard's "Crawls" tab.
    async with session_scope(settings) as session:
        crawl = Crawl(
            name=args.name,
            seeds=list(args.seeds),
            status="running",
            max_depth=args.max_depth,
            max_pages=args.max_pages,
        )
        session.add(crawl)
        await session.flush()
        crawl_id = crawl.id

    await execute_crawl(
        settings,
        crawl_id=crawl_id,
        seeds=list(args.seeds),
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        allowed_domains=None,
        same_domain_only=not args.all_domains,
        concurrency=args.concurrency,
        polite=args.polite,
        dedup=args.dedup,
    )


async def _serve(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)

    from crawler.storage.db import dispose_engine, ensure_schema

    await ensure_schema(settings)

    config = uvicorn.Config(
        "crawler.api.app:app",
        host=args.host,
        port=args.port,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve(), name="uvicorn")

    try:
        # Give the server a moment to bind, then surface any early startup error.
        await asyncio.sleep(0.5)
        if server_task.done():
            await server_task
            return
        print(_banner(args.host, args.port), flush=True)

        if args.seeds:
            crawl_task = asyncio.create_task(_run_crawl(settings, args), name="crawl")
            # Let the crawl and server run together; stop the crawl if the
            # server is asked to exit (Ctrl+C).
            while not crawl_task.done():
                if server.should_exit:
                    crawl_task.cancel()
                    break
                await asyncio.sleep(0.2)
            with contextlib.suppress(asyncio.CancelledError):
                await crawl_task
            if not server.should_exit:
                log.info("crawl_done_dashboard_live", url=f"http://{args.host}:{args.port}/")
        await server_task
    finally:
        await dispose_engine()


def _run_distributed(args: argparse.Namespace) -> int:
    """Launch the API + N workers as subprocesses (Kafka frontier)."""
    env = dict(os.environ)
    env.setdefault("CRAWLER_FRONTIER__BACKEND", "kafka")

    if get_settings().postgres.dsn.startswith("sqlite"):
        print(
            "WARNING: distributed mode needs shared services. Set CRAWLER_POSTGRES__DSN "
            "(Postgres), CRAWLER_REDIS__URL and CRAWLER_KAFKA__BOOTSTRAP_SERVERS before "
            "running --distributed.",
            file=sys.stderr,
        )

    commands = [
        [sys.executable, "-m", "uvicorn", "crawler.api.app:app",
         "--host", args.host, "--port", str(args.port)],
    ]
    for i in range(args.workers):
        commands.append(
            [sys.executable, "-m", "crawler.worker", "--metrics-port", str(8001 + i)]
        )

    procs = [subprocess.Popen(cmd, env=env) for cmd in commands]
    print(_banner(args.host, args.port), flush=True)
    print(f"Started API + {args.workers} worker(s). Press Ctrl+C to stop.", flush=True)
    try:
        while True:
            for proc in procs:
                if proc.poll() is not None:
                    raise SystemExit(f"process {proc.pid} exited with {proc.returncode}")
            # Poll periodically.
            with contextlib.suppress(subprocess.TimeoutExpired):
                procs[0].wait(timeout=1)
    except (KeyboardInterrupt, SystemExit) as exc:
        if isinstance(exc, SystemExit) and exc.code:
            print(exc, file=sys.stderr)
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in procs:
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=10)
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the web crawler API + dashboard (and optionally a crawl).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address for the API/dashboard")
    parser.add_argument("--port", type=int, default=8010, help="Port for the API/dashboard")

    parser.add_argument(
        "--seeds", nargs="*", metavar="URL",
        help="Seed URLs to crawl now (single-node). Omit to just serve the dashboard.",
    )
    parser.add_argument("--name", default="crawl", help="Name for the crawl record")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum link depth")
    parser.add_argument("--max-pages", type=int, default=100, help="Stop after this many pages")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent fetch workers")
    parser.add_argument(
        "--all-domains", action="store_true", help="Follow links off the seed domains"
    )
    parser.add_argument(
        "--impolite", dest="polite", action="store_false",
        help="Ignore robots.txt and per-host rate limits",
    )
    parser.add_argument(
        "--no-dedup", dest="dedup", action="store_false", help="Disable near-duplicate detection"
    )

    parser.add_argument(
        "--distributed", action="store_true",
        help="Launch the API + Kafka workers as subprocesses (needs external services)",
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=3, help="Worker count for --distributed"
    )
    parser.set_defaults(polite=True, dedup=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.distributed:
        return _run_distributed(args)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_serve(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
