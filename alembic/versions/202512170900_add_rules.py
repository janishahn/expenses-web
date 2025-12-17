"""add rules

Revision ID: 202512170900
Revises: 202512161700
Create Date: 2025-12-17 09:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202512170900"
down_revision = "202512161700"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column(
            "match_type",
            sa.Enum("contains", "equals", "starts_with", "regex", name="rulematchtype"),
            nullable=False,
        ),
        sa.Column("match_value", sa.String(length=200), nullable=False),
        sa.Column(
            "transaction_type",
            sa.Enum("income", "expense", name="transactiontype"),
            nullable=True,
        ),
        sa.Column("min_amount_cents", sa.Integer(), nullable=True),
        sa.Column("max_amount_cents", sa.Integer(), nullable=True),
        sa.Column("set_category_id", sa.Integer(), sa.ForeignKey("categories.id")),
        sa.Column("add_tags_json", sa.Text(), nullable=True),
        sa.Column("budget_exclude_tag_id", sa.Integer(), sa.ForeignKey("tags.id")),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index(
        "ix_rules_user_enabled_priority",
        "rules",
        ["user_id", "enabled", "priority", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_rules_user_enabled_priority", table_name="rules")
    op.drop_table("rules")
