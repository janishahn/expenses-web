"""add balance anchors

Revision ID: 202512151200
Revises: 202512141700
Create Date: 2025-12-15 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202512151200"
down_revision = "202512141700"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "balance_anchors",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("balance_cents", sa.Integer(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "user_id", "as_of_date", name="uq_balance_anchor_user_date"
        ),
    )
    op.create_index(
        "ix_balance_anchor_user_date",
        "balance_anchors",
        ["user_id", "as_of_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_balance_anchor_user_date", table_name="balance_anchors")
    op.drop_table("balance_anchors")
