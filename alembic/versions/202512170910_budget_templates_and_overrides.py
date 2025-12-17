"""budget templates and overrides

Revision ID: 202512170910
Revises: 202512170900
Create Date: 2025-12-17 09:10:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202512170910"
down_revision = "202512170900"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "budget_templates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "frequency",
            sa.Enum("monthly", "yearly", name="budgetfrequency"),
            nullable=False,
        ),
        sa.Column(
            "category_id", sa.Integer(), sa.ForeignKey("categories.id"), nullable=True
        ),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("starts_on", sa.Date(), nullable=False),
        sa.Column("ends_on", sa.Date(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "amount_cents >= 0", name="ck_budget_template_amount_positive"
        ),
        sa.UniqueConstraint(
            "user_id",
            "frequency",
            "category_id",
            "starts_on",
            name="uq_budget_template_scope_start",
        ),
    )
    op.create_index(
        "ix_budget_template_user_freq", "budget_templates", ["user_id", "frequency"]
    )

    op.create_table(
        "budget_overrides",
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
        sa.CheckConstraint(
            "amount_cents >= 0", name="ck_budget_override_amount_positive"
        ),
        sa.UniqueConstraint(
            "user_id",
            "year",
            "month",
            "category_id",
            name="uq_budget_override_user_month_category",
        ),
    )
    op.create_index(
        "ix_budget_override_user_month",
        "budget_overrides",
        ["user_id", "year", "month"],
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_budget_override_user_month_category_coalesce "
        "ON budget_overrides(user_id, year, month, IFNULL(category_id, -1))"
    )

    # Migrate existing monthly budgets into overrides.
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if "budgets" in inspector.get_table_names():
        conn.execute(
            sa.text(
                "INSERT INTO budget_overrides(user_id, year, month, category_id, amount_cents, created_at, updated_at) "
                "SELECT user_id, year, month, category_id, amount_cents, created_at, updated_at FROM budgets"
            )
        )
        # Clean up old table & indexes.
        try:
            op.drop_index(
                "uq_budget_user_month_category_coalesce", table_name="budgets"
            )
        except Exception:
            pass
        try:
            op.drop_index("ix_budget_user_month", table_name="budgets")
        except Exception:
            pass
        op.drop_table("budgets")


def downgrade() -> None:
    # Recreate old budgets table (data loss for templates; overrides copied back).
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
    op.execute(
        "CREATE UNIQUE INDEX uq_budget_user_month_category_coalesce "
        "ON budgets(user_id, year, month, IFNULL(category_id, -1))"
    )

    conn = op.get_bind()
    conn.execute(
        sa.text(
            "INSERT INTO budgets(user_id, year, month, category_id, amount_cents, created_at, updated_at) "
            "SELECT user_id, year, month, category_id, amount_cents, created_at, updated_at FROM budget_overrides"
        )
    )

    op.drop_index(
        "uq_budget_override_user_month_category_coalesce", table_name="budget_overrides"
    )
    op.drop_index("ix_budget_override_user_month", table_name="budget_overrides")
    op.drop_table("budget_overrides")

    op.drop_index("ix_budget_template_user_freq", table_name="budget_templates")
    op.drop_table("budget_templates")
