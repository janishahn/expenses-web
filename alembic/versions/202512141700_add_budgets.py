"""add budgets

Revision ID: 202512141700
Revises: add_kind_to_transactions
Create Date: 2025-12-14 17:00:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202512141700"
down_revision = "add_kind_to_transactions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "budgets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column(
            "category_id", sa.Integer(), sa.ForeignKey("categories.id"), nullable=True
        ),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("amount_cents >= 0", name="ck_budget_amount_positive"),
        sa.UniqueConstraint(
            "user_id",
            "year",
            "month",
            "category_id",
            name="uq_budget_user_month_category",
        ),
    )
    op.create_index("ix_budget_user_month", "budgets", ["user_id", "year", "month"])

    # Enforce uniqueness for (user, year, month, overall) where category_id is NULL.
    op.execute(
        "CREATE UNIQUE INDEX uq_budget_user_month_category_coalesce "
        "ON budgets(user_id, year, month, IFNULL(category_id, -1))"
    )


def downgrade() -> None:
    op.drop_index("uq_budget_user_month_category_coalesce", table_name="budgets")
    op.drop_index("ix_budget_user_month", table_name="budgets")
    op.drop_table("budgets")
