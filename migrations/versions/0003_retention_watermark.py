"""Keep a monotonic expiry watermark when deduplication records are removed."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "retention_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cutoff", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("retention_state")
