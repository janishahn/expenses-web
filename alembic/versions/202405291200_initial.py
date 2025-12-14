"""initial schema

Revision ID: 202405291200
Revises:
Create Date: 2024-05-29 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "202405291200"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "categories",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, default=1),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column(
            "type", sa.Enum("income", "expense", name="transactiontype"), nullable=False
        ),
        sa.Column("color", sa.String(length=7)),
        sa.Column("order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("archived_at", sa.DateTime()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "user_id", "type", "name", name="uq_category_user_type_name"
        ),
    )

    op.create_table(
        "recurring_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, default=1),
        sa.Column("name", sa.String(length=120)),
        sa.Column(
            "type", sa.Enum("income", "expense", name="transactiontype"), nullable=False
        ),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column(
            "category_id", sa.Integer(), sa.ForeignKey("categories.id"), nullable=False
        ),
        sa.Column("anchor_date", sa.Date(), nullable=False),
        sa.Column(
            "interval_unit",
            sa.Enum("day", "week", "month", "year", name="intervalunit"),
            nullable=False,
        ),
        sa.Column("interval_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("next_occurrence", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date()),
        sa.Column("auto_post", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "skip_weekends", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "month_day_policy",
            sa.Enum("snap_to_end", "skip", "carry_forward", name="monthdaypolicy"),
            nullable=False,
            server_default="snap_to_end",
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("interval_count > 0", name="ck_rule_interval_positive"),
        sa.CheckConstraint("amount_cents >= 0", name="ck_rule_amount_positive"),
    )

    op.create_table(
        "monthly_rollups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, default=1),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("income_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("expense_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("user_id", "year", "month", name="uq_rollup_user_month"),
    )

    op.create_table(
        "transactions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, default=1),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column(
            "type", sa.Enum("income", "expense", name="transactiontype"), nullable=False
        ),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column(
            "category_id", sa.Integer(), sa.ForeignKey("categories.id"), nullable=False
        ),
        sa.Column("note", sa.Text()),
        sa.Column("deleted_at", sa.DateTime()),
        sa.Column("origin_rule_id", sa.Integer(), sa.ForeignKey("recurring_rules.id")),
        sa.Column("occurrence_date", sa.Date()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "user_id",
            "origin_rule_id",
            "occurrence_date",
            name="uq_txn_origin_occurrence",
        ),
        sa.CheckConstraint("amount_cents >= 0", name="ck_transactions_amount_positive"),
    )
    op.create_index("ix_transactions_user_date", "transactions", ["user_id", "date"])
    op.create_index(
        "ix_transactions_user_category_date",
        "transactions",
        ["user_id", "category_id", "date"],
    )
    op.create_index(
        "ix_transactions_user_type_date",
        "transactions",
        ["user_id", "type", "date"],
    )


def downgrade():
    op.drop_index("ix_transactions_user_type_date", table_name="transactions")
    op.drop_index("ix_transactions_user_category_date", table_name="transactions")
    op.drop_index("ix_transactions_user_date", table_name="transactions")
    op.drop_table("transactions")
    op.drop_table("monthly_rollups")
    op.drop_table("recurring_rules")
    op.drop_table("categories")
