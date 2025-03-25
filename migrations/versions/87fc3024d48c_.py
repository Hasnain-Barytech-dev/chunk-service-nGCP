"""empty message

Revision ID: 87fc3024d48c
Revises: 0ef995027358
Create Date: 2024-04-09 16:53:01.946392

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '87fc3024d48c'
down_revision = '0ef995027358'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.add_column(sa.Column('department', sa.String(length=250), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.drop_column('department')

    # ### end Alembic commands ###
