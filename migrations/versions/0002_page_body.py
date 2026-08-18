"""page_body table for the Postgres object-store backend

Revision ID: 0002_page_body
Revises: 0001_init
Create Date: 2026-06-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_page_body"
down_revision: str | None = "0001_init"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "page_body",
        sa.Column("content_hash", sa.String(length=64), primary_key=True),
        sa.Column("body", sa.LargeBinary(), nullable=False),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("size", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("page_body")
