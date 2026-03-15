"""Revision ID: add_telegram_channel_type
Revises: add_microsoft_teams_support
Create Date: 2026-03-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_telegram_channel_type'
down_revision: Union[str, None] = 'add_microsoft_teams_support'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Use execute to run the raw SQL for altering an enum
    # In PostgreSQL, ALTER TYPE ADD VALUE cannot run in a transaction block with other things
    # so we set autocommit on the execution context
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE channel_type_enum ADD VALUE IF NOT EXISTS 'telegram'")


def downgrade() -> None:
    pass
