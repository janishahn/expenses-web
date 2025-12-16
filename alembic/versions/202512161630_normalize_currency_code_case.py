"""normalize currency code case

Revision ID: 202512161630
Revises: 202512161600
Create Date: 2025-12-16 16:30:00.000000

"""

from __future__ import annotations

from alembic import op


revision = "202512161630"
down_revision = "202512161600"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE recurring_rules SET currency_code = upper(currency_code) "
        "WHERE currency_code IS NOT NULL AND currency_code != upper(currency_code)"
    )
    op.execute(
        "UPDATE transactions SET source_currency_code = upper(source_currency_code) "
        "WHERE source_currency_code IS NOT NULL "
        "AND source_currency_code != upper(source_currency_code)"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE recurring_rules SET currency_code = lower(currency_code) "
        "WHERE currency_code IN ('EUR', 'USD')"
    )
    op.execute(
        "UPDATE transactions SET source_currency_code = lower(source_currency_code) "
        "WHERE source_currency_code IN ('EUR', 'USD')"
    )
