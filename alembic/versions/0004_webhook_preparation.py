"""Add a recoverable pre-HTTP webhook preparation lease."""

import sqlalchemy as sa

from alembic import op

revision = "0004_webhook_preparation"
down_revision = "0003_recovery_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep old sending rows unchanged because their HTTP outcome is unknown."""
    op.add_column(
        "webhook_deliveries",
        sa.Column("preparation_expires_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Remove the preparation lease column."""
    op.drop_column("webhook_deliveries", "preparation_expires_at")
