"""Persist generation checkpoints and durable delivery retry job identifiers."""

import sqlalchemy as sa

from alembic import op

revision = "0003_recovery_checkpoints"
down_revision = "0002_generation_idempotency"


def upgrade() -> None:
    """Add nullable fields without changing existing task states."""
    op.add_column("generations", sa.Column("webhook_url", sa.Text(), nullable=True))
    op.add_column("generations", sa.Column("run_token", sa.String(36), nullable=True))
    op.add_column(
        "generations", sa.Column("provider_context", sa.JSON(), nullable=True)
    )
    op.add_column(
        "webhook_deliveries", sa.Column("retry_job_id", sa.String(150), nullable=True)
    )
    op.add_column(
        "webhook_deliveries",
        sa.Column("recovery_checked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Remove recovery-only fields."""
    op.drop_column("webhook_deliveries", "retry_job_id")
    op.drop_column("webhook_deliveries", "recovery_checked_at")
    for name in ("provider_context", "run_token", "webhook_url"):
        op.drop_column("generations", name)
