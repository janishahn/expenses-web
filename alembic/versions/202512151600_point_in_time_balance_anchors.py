"""point in time balance anchors

Revision ID: 202512151600
Revises: 202512151200
Create Date: 2025-12-15 16:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202512151600"
down_revision = "202512151200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.add_column(sa.Column("occurred_at", sa.DateTime(), nullable=True))
    op.execute(
        "UPDATE transactions SET occurred_at = datetime(\"date\" || ' 12:00:00') "
        "WHERE occurred_at IS NULL"
    )
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.alter_column(
            "occurred_at", existing_type=sa.DateTime(), nullable=False
        )
        batch_op.drop_column("kind")

    with op.batch_alter_table("balance_anchors") as batch_op:
        batch_op.add_column(sa.Column("as_of_at", sa.DateTime(), nullable=True))
    op.execute(
        "UPDATE balance_anchors SET as_of_at = datetime(as_of_date || ' 23:59:59') "
        "WHERE as_of_at IS NULL"
    )
    op.drop_index("ix_balance_anchor_user_date", table_name="balance_anchors")
    with op.batch_alter_table("balance_anchors") as batch_op:
        batch_op.drop_constraint("uq_balance_anchor_user_date", type_="unique")
        batch_op.drop_column("as_of_date")
        batch_op.alter_column("as_of_at", existing_type=sa.DateTime(), nullable=False)
    op.create_index(
        "ix_balance_anchor_user_at",
        "balance_anchors",
        ["user_id", "as_of_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_balance_anchor_user_at", table_name="balance_anchors")
    with op.batch_alter_table("balance_anchors") as batch_op:
        batch_op.add_column(sa.Column("as_of_date", sa.Date(), nullable=True))
    op.execute(
        "UPDATE balance_anchors SET as_of_date = date(as_of_at) WHERE as_of_date IS NULL"
    )
    with op.batch_alter_table("balance_anchors") as batch_op:
        batch_op.alter_column("as_of_date", existing_type=sa.Date(), nullable=False)
        batch_op.drop_column("as_of_at")
        batch_op.create_unique_constraint(
            "uq_balance_anchor_user_date", ["user_id", "as_of_date"]
        )
    op.create_index(
        "ix_balance_anchor_user_date",
        "balance_anchors",
        ["user_id", "as_of_date"],
    )

    with op.batch_alter_table("transactions") as batch_op:
        batch_op.add_column(
            sa.Column(
                "kind",
                sa.Enum("normal", "adjustment", name="transactionkind"),
                nullable=False,
                server_default="normal",
            )
        )
        batch_op.drop_column("occurred_at")
