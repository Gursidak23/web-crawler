"""Raw page-body storage.

Bodies are large and rarely queried relationally, so they live outside the
primary tables. Two backends are provided:

* :class:`MinioObjectStore` - S3-compatible object storage (gzipped), the
  production choice; keyed by content hash so identical bodies are stored once.
* :class:`PostgresObjectStore` - a zero-dependency fallback that stores gzipped
  bodies in the ``page_body`` table.

Plus :class:`NoopObjectStore` for runs that don't retain bodies.
"""

from __future__ import annotations

import asyncio
import gzip
from io import BytesIO
from typing import Protocol

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import Settings
from .db import session_scope
from .orm import PageBody


def gzip_bytes(data: bytes) -> bytes:
    return gzip.compress(data)


def gunzip_bytes(data: bytes) -> bytes:
    return gzip.decompress(data)


class ObjectStore(Protocol):
    async def put(self, content_hash: str, body: bytes, content_type: str | None = None) -> str: ...

    async def get(self, storage_key: str) -> bytes | None: ...

    async def close(self) -> None: ...


class NoopObjectStore:
    async def put(self, content_hash: str, body: bytes, content_type: str | None = None) -> str:
        return ""

    async def get(self, storage_key: str) -> bytes | None:
        return None

    async def close(self) -> None:
        return None


class PostgresObjectStore:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings

    async def put(self, content_hash: str, body: bytes, content_type: str | None = None) -> str:
        compressed = gzip_bytes(body)
        async with session_scope(self.settings) as session:
            stmt = pg_insert(PageBody).values(
                content_hash=content_hash,
                body=compressed,
                content_type=content_type,
                size=len(body),
            ).on_conflict_do_nothing(index_elements=["content_hash"])
            await session.execute(stmt)
        return f"pg:{content_hash}"

    async def get(self, storage_key: str) -> bytes | None:
        content_hash = storage_key.split(":", 1)[-1]
        async with session_scope(self.settings) as session:
            row = await session.get(PageBody, content_hash)
            return gunzip_bytes(row.body) if row is not None else None

    async def close(self) -> None:
        return None


class MinioObjectStore:
    def __init__(self, settings: Settings) -> None:
        from minio import Minio

        self.settings = settings
        self.bucket = settings.storage.minio_bucket
        self._client = Minio(
            settings.storage.minio_endpoint,
            access_key=settings.storage.minio_access_key,
            secret_key=settings.storage.minio_secret_key,
            secure=settings.storage.minio_secure,
        )
        self._bucket_ready = False

    async def _ensure_bucket(self) -> None:
        if self._bucket_ready:
            return
        await asyncio.to_thread(self._ensure_bucket_sync)
        self._bucket_ready = True

    def _ensure_bucket_sync(self) -> None:
        if not self._client.bucket_exists(self.bucket):
            self._client.make_bucket(self.bucket)

    async def put(self, content_hash: str, body: bytes, content_type: str | None = None) -> str:
        await self._ensure_bucket()
        compressed = gzip_bytes(body)

        def _put() -> None:
            self._client.put_object(
                self.bucket,
                content_hash,
                BytesIO(compressed),
                length=len(compressed),
                content_type="application/gzip",
            )

        await asyncio.to_thread(_put)
        return f"s3://{self.bucket}/{content_hash}"

    async def get(self, storage_key: str) -> bytes | None:
        key = storage_key.rsplit("/", 1)[-1]

        def _get() -> bytes | None:
            try:
                response = self._client.get_object(self.bucket, key)
            except Exception:
                return None
            try:
                return gunzip_bytes(response.read())
            finally:
                response.close()
                response.release_conn()

        return await asyncio.to_thread(_get)

    async def close(self) -> None:
        return None


def build_object_store(settings: Settings, *, enabled: bool = True) -> ObjectStore:
    if not enabled:
        return NoopObjectStore()
    if settings.storage.body_backend == "minio":
        return MinioObjectStore(settings)
    return PostgresObjectStore(settings)
