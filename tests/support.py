"""Shared test helpers.

We serve fixture pages from a real in-process aiohttp server bound to localhost
rather than mocking the client internals. This exercises the genuine streaming
fetch path and is robust across aiohttp versions.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Mapping

from aiohttp import web
from aiohttp.test_utils import TestServer


@contextlib.asynccontextmanager
async def serve_site(
    pages: Mapping[str, str],
    *,
    default_content_type: str = "text/html",
    extra_headers: Mapping[str, str] | None = None,
    etag: str | None = None,
) -> AsyncIterator[str]:
    """Serve ``{path: body}`` from a localhost server; yields the base URL.

    Paths ending in ``.txt`` are served as ``text/plain`` (handy for robots.txt).
    Unknown paths return 404. When ``etag`` is set, responses carry that ETag and
    a matching ``If-None-Match`` request gets a ``304 Not Modified`` (used to
    exercise conditional GET / recrawl behavior).
    """
    app = web.Application()

    async def handler(request: web.Request) -> web.Response:
        body = pages.get(request.path)
        if body is None:
            return web.Response(status=404, text="not found")
        headers = dict(extra_headers or {})
        if etag is not None:
            if request.headers.get("If-None-Match") == etag:
                return web.Response(status=304, headers={"ETag": etag})
            headers["ETag"] = etag
        content_type = "text/plain" if request.path.endswith(".txt") else default_content_type
        return web.Response(text=body, content_type=content_type, headers=headers)

    app.router.add_get("/{tail:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        yield f"http://127.0.0.1:{server.port}"
    finally:
        await server.close()
