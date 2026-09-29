"""Ensure generation idempotency on databases stamped by older 0001 versions.

Earlier project revisions edited 0001 in place. Inspect before adding the
column/constraint so both existing variants and clean installations can upgrade.
"""

import sqlalchemy as sa

from alembic import op

revision = "0002_generation_idempotency"
down_revision = "0001_initial"


def upgrade() -> None:
    """Install the nullable key and per-user uniqueness without rewriting data."""
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("generations")}
    if "idempotency_key" not in columns:
        op.add_column(
            "generations", sa.Column("idempotency_key", sa.String(200), nullable=True)
        )
    constraints = inspector.get_unique_constraints("generations")
    if not any(
        item["column_names"] == ["user_id", "idempotency_key"] for item in constraints
    ):
        with op.batch_alter_table("generations") as batch:
            batch.create_unique_constraint(
                "uq_generation_idempotency", ["user_id", "idempotency_key"]
            )


def downgrade() -> None:
    """Keep the key: current 0001 already contains it and deleting loses data."""
