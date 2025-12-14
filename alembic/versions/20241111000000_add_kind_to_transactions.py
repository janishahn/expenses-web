"""add_kind_to_transactions

Revision ID: add_kind_to_transactions
Revises:
Create Date: 2024-11-11 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_kind_to_transactions"
down_revision = "202405291200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.add_column(
            sa.Column(
                "kind",
                sa.Enum("normal", "adjustment", name="transactionkind"),
                nullable=False,
                server_default="normal",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.drop_column("kind")
