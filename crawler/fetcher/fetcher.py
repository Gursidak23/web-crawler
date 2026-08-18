"""Async HTTP downloader built on aiohttp.

Provides timeouts, a global connection cap, a streaming body-size limit, and
bounded retries with exponential backoff. Politeness (robots + rate limiting) is
layered on top in Phase 2 - the fetcher itself just moves bytes.
"""

from __future__ import annotations

import asyncio
import time
from types import TracebackType

import aiohttp

from .. import metrics
from ..config import Settings, get_settings
from ..logging_setup import get_logger
from ..models import FetchResult

log = get_logger(__name__)


class Fetcher:
    def __init__(
        self,
        settings: Settings | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> Fetcher:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(
                total=self.settings.fetch.timeout_seconds,
                connect=self.settings.fetch.connect_timeout_seconds,
            )
            connector = aiohttp.TCPConnector(
                limit=self.settings.fetch.concurrency,
                ttl_dns_cache=self.settings.politeness.dns_cache_ttl_seconds,
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.settings.fetch.user_agent},
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Fetcher must be used as an async context manager")
        return self._session

    async def fetch(self, url: str, *, headers: dict[str, str] | None = None) -> FetchResult:
        retries = self.settings.fetch.retries
        last_error: str | None = None
        for attempt in range(retries + 1):
            result = await self._fetch_once(url, headers=headers)
            if result.error is None:
                return result
            last_error = result.error
            if attempt < retries:
                await asyncio.sleep(min(2**attempt * 0.25, 5.0))
        return FetchResult(
            url=url,
            final_url=url,
            status=0,
            headers={},
            body=b"",
            content_type=None,
            elapsed=0.0,
            error=last_error,
        )

    async def _fetch_once(
        self, url: str, *, headers: dict[str, str] | None
    ) -> FetchResult:
        max_bytes = self.settings.fetch.max_page_bytes
        start = time.perf_counter()
        metrics.ACTIVE_FETCHES.inc()
        try:
            async with self.session.get(url, headers=headers, allow_redirects=True) as resp:
                # Read at most max_bytes + 1 so we can detect overflow and truncate.
                body = await resp.content.read(max_bytes + 1)
                if len(body) > max_bytes:
                    body = body[:max_bytes]
                elapsed = time.perf_counter() - start
                metrics.FETCH_LATENCY.observe(elapsed)
                metrics.BYTES_DOWNLOADED.inc(len(body))
                metrics.PAGES_FETCHED.labels(
                    status=metrics.status_class(resp.status), outcome="ok"
                ).inc()
                return FetchResult(
                    url=url,
                    final_url=str(resp.url),
                    status=resp.status,
                    headers=dict(resp.headers),
                    body=body,
                    content_type=resp.headers.get("Content-Type"),
                    elapsed=elapsed,
                )
        except (aiohttp.ClientError, TimeoutError) as exc:
            elapsed = time.perf_counter() - start
            metrics.FETCH_LATENCY.observe(elapsed)
            metrics.PAGES_FETCHED.labels(status="error", outcome="error").inc()
            log.warning("fetch_failed", url=url, error=repr(exc))
            return FetchResult(
                url=url,
                final_url=url,
                status=0,
                headers={},
                body=b"",
                content_type=None,
                elapsed=elapsed,
                error=repr(exc),
            )
        finally:
            metrics.ACTIVE_FETCHES.dec()
