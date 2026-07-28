"""production controls and audit

Revision ID: 0005
Revises: 0004
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "events",
        sa.Column("manually_approved", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("platform_publications", sa.Column("deleted_at", sa.DateTime(timezone=True)))
    op.create_table(
        "runtime_control",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "publication_enabled", sa.Boolean(), nullable=False, server_default=sa.true()
        ),
        sa.Column("telegram_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("max_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("actor", sa.String(200), nullable=False),
        sa.Column("action", sa.String(200), nullable=False),
        sa.Column("target", sa.String(500), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("audit_logs")
    op.drop_table("runtime_control")
    op.drop_column("platform_publications", "deleted_at")
    op.drop_column("events", "manually_approved")
