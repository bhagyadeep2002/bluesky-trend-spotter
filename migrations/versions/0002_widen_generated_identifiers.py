"""Use 64-bit generated identifiers under sustained stream/aggregate writes."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == "sqlite":
        return  # SQLite INTEGER PRIMARY KEY is already a signed 64-bit rowid.
    for table, column in [
        ("topic_candidates", "id"),
        ("topic_terms", "topic_id"),
        ("topic_post_assignments", "topic_id"),
        ("topic_window_metrics", "topic_id"),
        ("topic_window_metrics", "id"),
        ("detection_runs", "id"),
    ]:
        op.alter_column(
            table,
            column,
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=False,
        )
    # Widen the sequences as well: changing a column doesn't widen its sequence.
    for sequence in (
        "topic_candidates_id_seq",
        "topic_window_metrics_id_seq",
        "detection_runs_id_seq",
    ):
        op.execute(sa.text(f"ALTER SEQUENCE {sequence} AS BIGINT"))


def downgrade():
    if op.get_bind().dialect.name == "sqlite":
        return
    # Refuse to truncate identifiers if a deployment has already exceeded int32.
    for table, column in [
        ("topic_terms", "topic_id"),
        ("topic_post_assignments", "topic_id"),
        ("topic_window_metrics", "topic_id"),
        ("topic_candidates", "id"),
        ("topic_window_metrics", "id"),
        ("detection_runs", "id"),
    ]:
        op.alter_column(
            table,
            column,
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=False,
        )
    for sequence in (
        "topic_candidates_id_seq",
        "topic_window_metrics_id_seq",
        "detection_runs_id_seq",
    ):
        op.execute(sa.text(f"ALTER SEQUENCE {sequence} AS INTEGER"))
