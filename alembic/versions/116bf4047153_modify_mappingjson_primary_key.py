"""modify mappingJSON primary key

Revision ID: 116bf4047153
Revises: 
Create Date: 2024-02-19 03:50:59.434643

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '116bf4047153'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# def upgrade():
#     op.drop_table('new_mappingJSON')
    
# def downgrade() -> None:
#     pass
