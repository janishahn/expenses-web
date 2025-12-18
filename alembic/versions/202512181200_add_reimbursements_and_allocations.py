"""add reimbursements and allocations

Revision ID: 202512181200
Revises: 202512170910
Create Date: 2025-12-18 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "202512181200"
down_revision = "202512170910"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("transactions") as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_reimbursement",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )
        batch_op.create_index(
            "ix_transactions_user_is_reimbursement_date",
            ["user_id", "is_reimbursement", "date"],
        )

    op.create_table(
        "reimbursement_allocations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "reimbursement_transaction_id",
            sa.Integer(),
            sa.ForeignKey("transactions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "expense_transaction_id",
            sa.Integer(),
            sa.ForeignKey("transactions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("amount_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "amount_cents >= 0", name="ck_reimbursement_allocation_amount"
        ),
        sa.UniqueConstraint(
            "user_id",
            "reimbursement_transaction_id",
            "expense_transaction_id",
            name="uq_reimbursement_allocation_pair",
        ),
    )
    op.create_index(
        "ix_reimbursement_allocations_user_reimbursement",
        "reimbursement_allocations",
        ["user_id", "reimbursement_transaction_id"],
    )
    op.create_index(
        "ix_reimbursement_allocations_user_expense",
        "reimbursement_allocations",
        ["user_id", "expense_transaction_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_reimbursement_allocations_user_expense",
        table_name="reimbursement_allocations",
    )
    op.drop_index(
        "ix_reimbursement_allocations_user_reimbursement",
        table_name="reimbursement_allocations",
    )
    op.drop_table("reimbursement_allocations")

    with op.batch_alter_table("transactions") as batch_op:
        batch_op.drop_index("ix_transactions_user_is_reimbursement_date")
        batch_op.drop_column("is_reimbursement")
