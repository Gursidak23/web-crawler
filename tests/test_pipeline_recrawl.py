"""Pipeline tests for Phase 5: conditional GETs and raw-body storage."""

from crawler.config import get_settings
from crawler.fetcher import Fetcher
from crawler.models import FrontierItem
from crawler.pipeline import Pipeline
from crawler.storage.sinks import InMemoryDocumentSink

from .support import serve_site

PAGE = {"/": "<html><title>Home</title><body>hi</body></html>"}


class RecordingObjectStore:
    """Captures puts so tests can assert bodies were stored and keyed by hash."""

    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes, str | None]] = []

    async def put(self, content_hash: str, body: bytes, content_type: str | None = None) -> str:
        self.puts.append((content_hash, body, content_type))
        return f"mem:{content_hash}"

    async def get(self, storage_key: str) -> bytes | None:
        return None

    async def close(self) -> None:
        return None


async def test_conditional_get_skips_unchanged_page():
    settings = get_settings()
    sink = InMemoryDocumentSink()

    async with serve_site(PAGE, etag='"v1"') as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, conditional_get=True)

        first = await pipeline.process(FrontierItem(url=base, depth=0))
        assert first.fetched is True
        assert first.stored is True
        assert first.record is not None
        assert first.record.etag == '"v1"'

        # Second pass sends If-None-Match and the server replies 304.
        second = await pipeline.process(FrontierItem(url=base, depth=0))

    assert second.fetched is True
    assert second.stored is False
    assert second.skipped == "not_modified"
    # The unchanged page was not re-stored.
    assert len(sink.pages) == 1


async def test_conditional_disabled_refetches_full_body():
    settings = get_settings()
    sink = InMemoryDocumentSink()

    async with serve_site(PAGE, etag='"v1"') as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, conditional_get=False)
        await pipeline.process(FrontierItem(url=base, depth=0))
        second = await pipeline.process(FrontierItem(url=base, depth=0))

    # Without conditional GETs we always download and store the full page.
    assert second.skipped is None
    assert second.stored is True
    assert len(sink.pages) == 2


async def test_object_store_receives_body_keyed_by_hash():
    settings = get_settings()
    sink = InMemoryDocumentSink()
    store = RecordingObjectStore()

    async with serve_site(PAGE) as base, Fetcher(settings) as fetcher:
        pipeline = Pipeline(fetcher, sink, settings, object_store=store)
        result = await pipeline.process(FrontierItem(url=base, depth=0))

    assert result.record is not None
    assert len(store.puts) == 1
    stored_hash, stored_body, _ = store.puts[0]
    assert stored_hash == result.record.content_hash
    assert b"<title>Home</title>" in stored_body
    # The document records where the body landed.
    assert result.record.storage_key == f"mem:{stored_hash}"
