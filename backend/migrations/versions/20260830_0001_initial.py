"""Create the initial Vayobyzh schema.

Revision ID: 20260830_0001
Revises:
Create Date: 2026-08-30
"""

from alembic import op

from events_api import models  # noqa: F401
from events_api.database import Base

revision = "20260830_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The first release already used SQLAlchemy metadata in production. create_all
    # makes this baseline safe for both a new database and that existing schema;
    # every later schema change must be expressed as an explicit Alembic revision.
    Base.metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())
