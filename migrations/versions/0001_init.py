"""initial schema: crawl, document, link_edge, domain

Revision ID: 0001_init
Revises:
Create Date: 2026-06-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_init"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "crawl",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("seeds", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("max_depth", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("max_pages", sa.Integer(), nullable=False, server_default="1000"),
        sa.Column("allowed_domains", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_crawl_status", "crawl", ["status"])

    op.create_table(
        "document",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("registered_domain", sa.String(length=253), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("content_length", sa.Integer(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("simhash", sa.BigInteger(), nullable=True),
        sa.Column("storage_key", sa.Text(), nullable=True),
        sa.Column("etag", sa.String(length=255), nullable=True),
        sa.Column("last_modified", sa.String(length=64), nullable=True),
        sa.Column("depth", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("in_degree", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("out_degree", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("crawl_id", sa.Integer(), sa.ForeignKey("crawl.id", ondelete="SET NULL"), nullable=True),
        sa.Column("first_seen", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_crawled", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("url", name="uq_document_url"),
    )
    op.create_index("ix_document_registered_domain", "document", ["registered_domain"])
    op.create_index("ix_document_content_hash", "document", ["content_hash"])
    op.create_index("ix_document_crawl_id", "document", ["crawl_id"])

    op.create_table(
        "link_edge",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "src_document_id",
            sa.BigInteger(),
            sa.ForeignKey("document.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dst_url", sa.Text(), nullable=False),
        sa.Column("anchor_text", sa.Text(), nullable=True),
        sa.UniqueConstraint("src_document_id", "dst_url", name="uq_edge_src_dst"),
    )
    op.create_index("ix_link_edge_src_document_id", "link_edge", ["src_document_id"])
    op.create_index("ix_link_edge_dst_url", "link_edge", ["dst_url"])

    op.create_table(
        "domain",
        sa.Column("registered_domain", sa.String(length=253), primary_key=True),
        sa.Column("robots_txt", sa.Text(), nullable=True),
        sa.Column("crawl_delay_ms", sa.Integer(), nullable=True),
        sa.Column("last_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("allowed", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("budget_remaining", sa.Integer(), nullable=True),
        sa.Column("pages_crawled", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_table("link_edge")
    op.drop_table("document")
    op.drop_table("domain")
    op.drop_index("ix_crawl_status", table_name="crawl")
    op.drop_table("crawl")
