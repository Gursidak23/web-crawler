"""FastAPI application factory.

Exposes ``/health`` and ``/metrics``, the crawl control-plane routes
(POST /crawls, GET /crawls, GET /stats, GET /domains, GET /documents,
GET /documents/{id}, GET /graph) under ``/api/v1``, and a built-in web
dashboard at ``/``. The control plane is documented via the auto-generated
OpenAPI schema at ``/docs``.
"""

from __future__ import annotations

import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from .. import __version__
from .. import metrics as _metrics  # noqa: F401  (ensures metrics are registered)
from ..config import get_settings
from ..logging_setup import configure_logging

_HERE = Path(__file__).parent


def _restore_mail_safe_assets() -> None:
    """Restore JS assets shipped as ``.txt`` for mail-safe delivery.

    Email filters block ``.js`` attachments, so the distribution zip ships the
    dashboard bundle and its vendored libraries (Chart.js, Tailwind) as ``.txt``.
    Copy each back to its ``.js`` name so the static mount can serve it. Runs on
    every startup and is a no-op once the ``.js`` files exist (e.g. when running
    from source), so the dashboard works no matter how the app is launched.
    """
    try:
        for txt in (_HERE / "static").rglob("*.txt"):
            js = txt.with_suffix(".js")
            if not js.exists():
                shutil.copyfile(txt, js)
    except OSError:
        pass


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        # Create the embedded SQLite schema on first run (no-op for Postgres).
        from ..storage.db import ensure_schema

        await ensure_schema(settings)
        yield

    app = FastAPI(
        title="Moonshot Web Crawler",
        version=__version__,
        description=(
            "Control plane for a distributed web crawler: submit crawls, inspect "
            "documents and the link graph, and observe live metrics."
        ),
        lifespan=lifespan,
    )

    @app.get("/health", tags=["ops"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/metrics", tags=["ops"], include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    from .routes import router as crawl_router

    app.include_router(crawl_router)

    from .dashboard import router as dashboard_router

    app.include_router(dashboard_router)
    # Recreate any .js assets that were shipped as .txt for mail-safe delivery,
    # before the static mount so they're served on the very first request.
    _restore_mail_safe_assets()
    app.mount("/static", StaticFiles(directory=_HERE / "static"), name="static")

    return app


app = create_app()
