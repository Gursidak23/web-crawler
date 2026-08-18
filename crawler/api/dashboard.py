"""Server-rendered web dashboard for the crawler.

Serves a single-page UI (Tailwind + Chart.js vendored locally under
static/vendor, so it works offline with no CDN and no build step) that drives
the existing JSON control-plane API: submit crawls, watch live stats, browse
domains, the link graph, and stored documents.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .. import __version__

_HERE = Path(__file__).parent
_STATIC = _HERE / "static"
templates = Jinja2Templates(directory=str(_HERE / "templates"))

router = APIRouter(tags=["dashboard"], include_in_schema=False)


def _asset_version() -> str:
    """Cache-busting token derived from app.js's mtime.

    Changes whenever the JS is edited, so browsers fetch the new bundle instead
    of a stale cached copy (a fixed version string would never invalidate).
    """
    try:
        return str(int((_STATIC / "app.js").stat().st_mtime))
    except OSError:
        return __version__


@router.get("/", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    response = templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "version": __version__,
            "api_prefix": "/api/v1",
            "asset_version": _asset_version(),
        },
    )
    # The dashboard shell is tiny and must always reflect the latest asset token,
    # so never let a browser serve it from cache.
    response.headers["Cache-Control"] = "no-store"
    return response
