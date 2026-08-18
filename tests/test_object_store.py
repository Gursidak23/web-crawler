"""Unit tests for body storage helpers and the no-op backend."""

from crawler.config import get_settings
from crawler.storage.object_store import (
    NoopObjectStore,
    build_object_store,
    gunzip_bytes,
    gzip_bytes,
)


def test_gzip_round_trip():
    original = b"<html><body>" + b"hello world " * 1000 + b"</body></html>"
    compressed = gzip_bytes(original)
    assert len(compressed) < len(original)
    assert gunzip_bytes(compressed) == original


async def test_noop_store_returns_empty_key_and_no_body():
    store = NoopObjectStore()
    key = await store.put("abc123", b"data", "text/html")
    assert key == ""
    assert await store.get("abc123") is None
    await store.close()


def test_build_object_store_disabled_is_noop():
    settings = get_settings()
    store = build_object_store(settings, enabled=False)
    assert isinstance(store, NoopObjectStore)


def test_build_object_store_postgres_default():
    settings = get_settings()
    store = build_object_store(settings, enabled=True)
    # Default body_backend is "postgres"; should not be the no-op store.
    assert not isinstance(store, NoopObjectStore)
