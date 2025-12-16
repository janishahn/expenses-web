"""add recurring currency and transaction fx fields

Revision ID: 202512161600
Revises: 202512151600
Create Date: 2025-12-16 16:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202512161600"
down_revision = "202512151600"
branch_labels = None
depends_on = None


def upgrade() -> None:
    currency_enum = sa.Enum("EUR", "USD", name="currencycode")

    with op.batch_alter_table("recurring_rules") as batch_op:
        batch_op.add_column(
            sa.Column(
                "currency_code",
                currency_enum,
                nullable=False,
                server_default="EUR",
            )
        )

    with op.batch_alter_table("transactions") as batch_op:
        batch_op.add_column(sa.Column("source_currency_code", currency_enum))
        batch_op.add_column(sa.Column("source_amount_cents", sa.Integer()))
        batch_op.add_column(sa.Column("fx_rate_micros", sa.Integer()))
        batch_op.add_column(sa.Column("fx_rate_date", sa.Date()))
        batch_op.add_column(sa.Column("fx_provider", sa.String(length=40)))
        batch_op.add_column(sa.Column("fx_fetched_at", sa.DateTime()))


def downgrade() -> None:
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.drop_column("fx_fetched_at")
        batch_op.drop_column("fx_provider")
        batch_op.drop_column("fx_rate_date")
        batch_op.drop_column("fx_rate_micros")
        batch_op.drop_column("source_amount_cents")
        batch_op.drop_column("source_currency_code")

    with op.batch_alter_table("recurring_rules") as batch_op:
        batch_op.drop_column("currency_code")
