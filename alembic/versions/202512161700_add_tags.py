"""add tags

Revision ID: 202512161700
Revises: 202512161630
Create Date: 2025-12-16 17:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "202512161700"
down_revision = "202512161630"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False, default=1),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("color", sa.String(length=9), nullable=True),
        sa.Column(
            "is_hidden_from_budget",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("user_id", "name", name="uq_tag_user_name"),
    )

    op.create_table(
        "transaction_tags",
        sa.Column(
            "transaction_id",
            sa.Integer(),
            sa.ForeignKey("transactions.id"),
            primary_key=True,
        ),
        sa.Column("tag_id", sa.Integer(), sa.ForeignKey("tags.id"), primary_key=True),
    )


def downgrade():
    op.drop_table("transaction_tags")
    op.drop_table("tags")
