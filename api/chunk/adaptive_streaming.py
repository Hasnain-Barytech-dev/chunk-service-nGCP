"""add adaptive streaming columns to resource table

Revision ID: add_streaming_columns
Revises: fb43d20f994d
Create Date: 2025-03-26 11:30:45.982154

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime


# revision identifiers, used by Alembic.
revision = 'add_streaming_columns'
down_revision = 'fb43d20f994d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        # Streaming URLs
        batch_op.add_column(sa.Column('hls_url', sa.String(length=500), nullable=True))
        batch_op.add_column(sa.Column('dash_url', sa.String(length=500), nullable=True))
        batch_op.add_column(sa.Column('stream_key', sa.String(length=250), nullable=True))
        
        # Video metadata
        batch_op.add_column(sa.Column('video_duration', sa.Float(), nullable=True))
        batch_op.add_column(sa.Column('video_width', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('video_height', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('video_bitrate', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('video_codec', sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column('audio_codec', sa.String(length=50), nullable=True))
        
        # Processing tracking
        batch_op.add_column(sa.Column('processing_started_at', sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column('processing_completed_at', sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column('processing_error', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('processing_progress', sa.Float(), nullable=True, default=0))

    # Add columns to chunks table for better tracking
    with op.batch_alter_table('resource_chunks', schema=None) as batch_op:
        batch_op.add_column(sa.Column('chunk_size', sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column('upload_started_at', sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column('upload_completed_at', sa.DateTime(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource_chunks', schema=None) as batch_op:
        batch_op.drop_column('upload_completed_at')
        batch_op.drop_column('upload_started_at')
        batch_op.drop_column('chunk_size')

    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.drop_column('processing_progress')
        batch_op.drop_column('processing_error')
        batch_op.drop_column('processing_completed_at')
        batch_op.drop_column('processing_started_at')
        batch_op.drop_column('audio_codec')
        batch_op.drop_column('video_codec')
        batch_op.drop_column('video_bitrate')
        batch_op.drop_column('video_height')
        batch_op.drop_column('video_width')
        batch_op.drop_column('video_duration')
        batch_op.drop_column('stream_key')
        batch_op.drop_column('dash_url')
        batch_op.drop_column('hls_url')
    # ### end Alembic commands ###