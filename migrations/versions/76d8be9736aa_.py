"""empty message

Revision ID: 76d8be9736aa
Revises: a7d1445390ef
Create Date: 2024-04-02 11:19:52.756682

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '76d8be9736aa'
down_revision = 'a7d1445390ef'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.alter_column('size',
               existing_type=sa.INTEGER(),
               type_=sa.BigInteger(),
               existing_nullable=False)
        batch_op.alter_column('offset',
               existing_type=sa.INTEGER(),
               type_=sa.BigInteger(),
               existing_nullable=True)
        batch_op.alter_column('chunks_uploaded',
               existing_type=sa.INTEGER(),
               type_=sa.BigInteger(),
               existing_nullable=True)

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('resource', schema=None) as batch_op:
        batch_op.alter_column('chunks_uploaded',
               existing_type=sa.BigInteger(),
               type_=sa.INTEGER(),
               existing_nullable=True)
        batch_op.alter_column('offset',
               existing_type=sa.BigInteger(),
               type_=sa.INTEGER(),
               existing_nullable=True)
        batch_op.alter_column('size',
               existing_type=sa.BigInteger(),
               type_=sa.INTEGER(),
               existing_nullable=False)

    # ### end Alembic commands ###
