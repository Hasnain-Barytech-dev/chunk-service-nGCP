"""add column for chat resource

Revision ID: fb43d20f994d
Revises: 9cb398aff3ae
Create Date: 2025-02-11 17:26:12.077799

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fb43d20f994d'
down_revision = '9cb398aff3ae'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.add_column(sa.Column('file_upload_from_chat', sa.Boolean(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.drop_column('file_upload_from_chat')

    # ### end Alembic commands ###
