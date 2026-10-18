"""Initial posts and activity detection schema"""
from alembic import op
import sqlalchemy as sa

revision = '0001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('detection_runs',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('events_processed', sa.Integer(), nullable=False),
    sa.Column('topics_evaluated', sa.Integer(), nullable=False),
    sa.Column('error_summary', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_detection_runs_started_at'), 'detection_runs', ['started_at'], unique=False)
    op.create_table('post_events',
    sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), nullable=False),
    sa.Column('event_key', sa.String(length=64), nullable=False),
    sa.Column('uri', sa.Text(), nullable=False),
    sa.Column('author_did', sa.Text(), nullable=False),
    sa.Column('operation', sa.String(length=16), nullable=False),
    sa.Column('source_time', sa.DateTime(timezone=True), nullable=True),
    sa.Column('ingested_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('raw_event_ref', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('event_key')
    )
    op.create_index(op.f('ix_post_events_source_time'), 'post_events', ['source_time'], unique=False)
    op.create_index(op.f('ix_post_events_uri'), 'post_events', ['uri'], unique=False)
    op.create_table('posts',
    sa.Column('uri', sa.Text(), nullable=False),
    sa.Column('author_did', sa.Text(), nullable=False),
    sa.Column('text', sa.Text(), nullable=True),
    sa.Column('normalized_text', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('indexed_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('activity_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('ingested_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('language', sa.String(length=64), nullable=True),
    sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('source_cursor', sa.BigInteger(), nullable=True),
    sa.Column('raw_event_ref', sa.Text(), nullable=True),
    sa.Column('event_key', sa.String(length=64), nullable=False),
    sa.PrimaryKeyConstraint('uri')
    )
    op.create_index(op.f('ix_posts_activity_at'), 'posts', ['activity_at'], unique=False)
    op.create_index(op.f('ix_posts_deleted_at'), 'posts', ['deleted_at'], unique=False)
    op.create_index(op.f('ix_posts_indexed_at'), 'posts', ['indexed_at'], unique=False)
    op.create_table('topic_candidates',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('label', sa.Text(), nullable=False),
    sa.Column('normalized_key', sa.Text(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('active', sa.Boolean(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('normalized_key')
    )
    op.create_index(op.f('ix_topic_candidates_last_seen_at'), 'topic_candidates', ['last_seen_at'], unique=False)
    op.create_table('topic_post_assignments',
    sa.Column('topic_id', sa.Integer(), nullable=False),
    sa.Column('post_uri', sa.Text(), nullable=False),
    sa.Column('assigned_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('relevance_score', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['post_uri'], ['posts.uri'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['topic_id'], ['topic_candidates.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('topic_id', 'post_uri')
    )
    op.create_index('ix_assignment_post', 'topic_post_assignments', ['post_uri'], unique=False)
    op.create_table('topic_terms',
    sa.Column('topic_id', sa.Integer(), nullable=False),
    sa.Column('term', sa.Text(), nullable=False),
    sa.Column('weight', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['topic_id'], ['topic_candidates.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('topic_id', 'term')
    )
    op.create_table('topic_window_metrics',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('topic_id', sa.Integer(), nullable=False),
    sa.Column('window_start', sa.DateTime(timezone=True), nullable=False),
    sa.Column('window_minutes', sa.Integer(), nullable=False),
    sa.Column('post_count', sa.Integer(), nullable=False),
    sa.Column('baseline_expected_count', sa.Float(), nullable=False),
    sa.Column('absolute_increase', sa.Float(), nullable=False),
    sa.Column('rate_ratio', sa.Float(), nullable=False),
    sa.Column('spike_score', sa.Float(), nullable=False),
    sa.Column('score_components', sa.JSON(), nullable=False),
    sa.Column('explanation', sa.Text(), nullable=False),
    sa.Column('detected_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['topic_id'], ['topic_candidates.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('topic_id', 'window_start', 'window_minutes')
    )
    op.create_index('ix_topic_window_lookup', 'topic_window_metrics', ['window_start', 'window_minutes', 'spike_score'], unique=False)

def downgrade():
    op.drop_index('ix_topic_window_lookup', table_name='topic_window_metrics')
    op.drop_table('topic_window_metrics')
    op.drop_table('topic_terms')
    op.drop_index('ix_assignment_post', table_name='topic_post_assignments')
    op.drop_table('topic_post_assignments')
    op.drop_index(op.f('ix_topic_candidates_last_seen_at'), table_name='topic_candidates')
    op.drop_table('topic_candidates')
    op.drop_index(op.f('ix_posts_indexed_at'), table_name='posts')
    op.drop_index(op.f('ix_posts_deleted_at'), table_name='posts')
    op.drop_index(op.f('ix_posts_activity_at'), table_name='posts')
    op.drop_table('posts')
    op.drop_index(op.f('ix_post_events_uri'), table_name='post_events')
    op.drop_index(op.f('ix_post_events_source_time'), table_name='post_events')
    op.drop_table('post_events')
    op.drop_index(op.f('ix_detection_runs_started_at'), table_name='detection_runs')
    op.drop_table('detection_runs')
