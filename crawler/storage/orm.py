"""SQLAlchemy 2.0 ORM models.

The schema captures crawl jobs, fetched documents (with dedup fingerprints),
the discovered link graph, and per-domain politeness/budget state.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Crawl(Base):
    __tablename__ = "crawl"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200))
    seeds: Mapped[list[str]] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    max_depth: Mapped[int] = mapped_column(Integer, default=3)
    max_pages: Mapped[int] = mapped_column(Integer, default=1000)
    allowed_domains: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    documents: Mapped[list[Document]] = relationship(back_populates="crawl")


class Document(Base):
    __tablename__ = "document"
    __table_args__ = (UniqueConstraint("url", name="uq_document_url"),)

    # BIGINT on Postgres; INTEGER on SQLite so it aliases the auto-incrementing
    # rowid (SQLite only auto-assigns a bare ``INTEGER PRIMARY KEY``).
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    url: Mapped[str] = mapped_column(Text)
    registered_domain: Mapped[str] = mapped_column(String(253), index=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    content_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    # 64-bit SimHash fingerprint stored as a signed BIGINT (see codec helpers).
    simhash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_modified: Mapped[str | None] = mapped_column(String(64), nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    in_degree: Mapped[int] = mapped_column(Integer, default=0)
    out_degree: Mapped[int] = mapped_column(Integer, default=0)
    crawl_id: Mapped[int | None] = mapped_column(
        ForeignKey("crawl.id", ondelete="SET NULL"), nullable=True, index=True
    )
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_crawled: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    crawl: Mapped[Crawl | None] = relationship(back_populates="documents")


class LinkEdge(Base):
    __tablename__ = "link_edge"
    __table_args__ = (
        UniqueConstraint("src_document_id", "dst_url", name="uq_edge_src_dst"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    src_document_id: Mapped[int] = mapped_column(
        ForeignKey("document.id", ondelete="CASCADE"), index=True
    )
    dst_url: Mapped[str] = mapped_column(Text, index=True)
    anchor_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class PageBody(Base):
    """Raw (gzipped) page bodies, keyed by content hash so identical bodies are
    stored once. Used by the Postgres object-store backend; with MinIO this table
    stays empty and bodies live in object storage instead."""

    __tablename__ = "page_body"

    content_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    body: Mapped[bytes] = mapped_column(LargeBinary)
    content_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    size: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Domain(Base):
    __tablename__ = "domain"

    registered_domain: Mapped[str] = mapped_column(String(253), primary_key=True)
    robots_txt: Mapped[str | None] = mapped_column(Text, nullable=True)
    crawl_delay_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    allowed: Mapped[bool] = mapped_column(Boolean, default=True)
    budget_remaining: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pages_crawled: Mapped[int] = mapped_column(Integer, default=0)
