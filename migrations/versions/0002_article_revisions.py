"""Store revisions for changed article URLs."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("raw_articles") as batch:
        batch.drop_constraint("uq_raw_source_url", type_="unique")
        batch.create_unique_constraint(
            "uq_raw_source_url_payload",
            ["source_id", "canonical_url", "payload_hash"],
        )
    with op.batch_alter_table("articles") as batch:
        batch.add_column(sa.Column("revision_of_id", sa.Uuid(), nullable=True))
        batch.create_foreign_key(
            "fk_articles_revision_of_id",
            "articles",
            ["revision_of_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch.drop_constraint("uq_article_url", type_="unique")
        batch.create_unique_constraint(
            "uq_article_source_url_content",
            ["source_id", "canonical_url", "content_hash"],
        )
        batch.create_index(
            "ix_article_source_url",
            ["source_id", "canonical_url"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("articles") as batch:
        batch.drop_index("ix_article_source_url")
        batch.drop_constraint("uq_article_source_url_content", type_="unique")
        batch.create_unique_constraint("uq_article_url", ["canonical_url"])
        batch.drop_constraint("fk_articles_revision_of_id", type_="foreignkey")
        batch.drop_column("revision_of_id")
    with op.batch_alter_table("raw_articles") as batch:
        batch.drop_constraint("uq_raw_source_url_payload", type_="unique")
        batch.create_unique_constraint("uq_raw_source_url", ["source_id", "canonical_url"])
