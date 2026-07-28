"""bounded article processing retries

Revision ID: 0006
Revises: 0005
"""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "articles",
        sa.Column(
            "processing_attempts", sa.Integer(), nullable=False, server_default="0"
        ),
    )
    op.add_column(
        "articles", sa.Column("next_processing_at", sa.DateTime(timezone=True))
    )


def downgrade() -> None:
    op.drop_column("articles", "next_processing_at")
    op.drop_column("articles", "processing_attempts")
