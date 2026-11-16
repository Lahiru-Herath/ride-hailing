"""pipeline_health: one row per component

Revision ID: 7c3e5a91b2d4
Revises: ead2bc42dcb1
Create Date: 2026-10-19 18:50:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "7c3e5a91b2d4"
down_revision: Union[str, Sequence[str], None] = "ead2bc42dcb1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # pipeline_health becomes "current state": the healthcheck service upserts one row per component (D46).
    # Any rows written before this migration are history with no meaning under the new contract.
    op.execute("DELETE FROM pipeline_health")
    op.create_unique_constraint("uq_pipeline_health_component", "pipeline_health", ["component"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("uq_pipeline_health_component", "pipeline_health", type_="unique")