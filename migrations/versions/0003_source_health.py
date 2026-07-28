"""source health diagnostics

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sources", sa.Column("last_success_at", sa.DateTime(timezone=True)))
    op.add_column("sources", sa.Column("last_checked_at", sa.DateTime(timezone=True)))
    op.add_column(
        "sources",
        sa.Column("health_state", sa.String(length=20), nullable=False, server_default="HEALTHY"),
    )
    op.add_column("sources", sa.Column("last_error", sa.Text()))
    op.add_column(
        "source_fetches",
        sa.Column("loaded_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "source_fetches",
        sa.Column("extraction_success_rate", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "source_fetches",
        sa.Column("diagnostics", sa.JSON(), nullable=False, server_default="[]"),
    )


def downgrade() -> None:
    op.drop_column("source_fetches", "diagnostics")
    op.drop_column("source_fetches", "extraction_success_rate")
    op.drop_column("source_fetches", "loaded_count")
    op.drop_column("sources", "last_error")
    op.drop_column("sources", "health_state")
    op.drop_column("sources", "last_checked_at")
    op.drop_column("sources", "last_success_at")
