"""serialize event draft versions

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("drafts") as batch:
        batch.create_unique_constraint(
            "uq_draft_event_version", ["event_id", "version"]
        )


def downgrade() -> None:
    with op.batch_alter_table("drafts") as batch:
        batch.drop_constraint("uq_draft_event_version", type_="unique")
