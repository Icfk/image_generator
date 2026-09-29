"""initial tables"""

import sqlalchemy as sa

from alembic import op

revision = "0001_initial"
down_revision = None


def upgrade():
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("external_id", sa.String(100), nullable=False, unique=True),
        sa.Column("secret_hash", sa.String(128), nullable=False),
        sa.Column("balance", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_table(
        "generations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("image_url", sa.Text()),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("result", sa.JSON()),
        sa.Column("error", sa.Text()),
        sa.Column("cost", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column("queued_at", sa.DateTime(timezone=True)),
        sa.Column("processing_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("failed_at", sa.DateTime(timezone=True)),
        sa.Column("provider_job_id", sa.String(200)),
        sa.Column("idempotency_key", sa.String(200)),
        sa.UniqueConstraint(
            "user_id", "idempotency_key", name="uq_generation_idempotency"
        ),
    )
    op.create_table(
        "balance_transactions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("generation_id", sa.String(36)),
        sa.Column("external_key", sa.String(200), unique=True),
        sa.Column("operation", sa.String(20), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_table(
        "payment_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("external_user_id", sa.String(100), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=False, unique=True),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("generation_id", sa.String(36), nullable=False, unique=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
    )
    op.create_table(
        "job_publications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("generation_id", sa.String(36), nullable=False, unique=True),
        sa.Column("job_id", sa.String(100), nullable=False, unique=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
    )


def downgrade():
    for table in (
        "job_publications",
        "webhook_deliveries",
        "payment_events",
        "balance_transactions",
        "generations",
        "users",
    ):
        op.drop_table(table)
