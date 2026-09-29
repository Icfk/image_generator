"""Verify upgrading the historical schema without deleting existing tasks."""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest


def test_upgrade_historical_schema_preserves_data_and_adds_uniqueness(tmp_path):
    database = tmp_path / "old-schema.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE generations (id VARCHAR(36) PRIMARY KEY, user_id INTEGER)"
        )
        connection.execute("INSERT INTO generations VALUES ('old-task', 1)")
    root = Path(__file__).resolve().parents[1]
    environment = {
        **os.environ,
        "DATABASE_URL": f"sqlite+aiosqlite:///{database}",
        "GENERATION_PROVIDER": "mock",
    }
    for arguments in (
        ("stamp", "0001_initial"),
        ("upgrade", "0002_generation_idempotency"),
    ):
        result = subprocess.run(
            [sys.executable, "-m", "alembic", *arguments],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT id, idempotency_key FROM generations"
        ).fetchall() == [("old-task", None)]
        connection.execute("INSERT INTO generations VALUES ('new', 1, 'key')")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO generations VALUES ('duplicate', 1, 'key')")
        connection.execute("INSERT INTO generations VALUES ('other-user', 2, 'key')")


def test_recovery_migration_preserves_existing_rows(tmp_path) -> None:
    """Upgrade the real 0002 schema, preserving existing task/delivery state."""
    database = tmp_path / "recovery-schema.db"
    root = Path(__file__).resolve().parents[1]
    environment = {
        **os.environ,
        "DATABASE_URL": f"sqlite+aiosqlite:///{database}",
        "GENERATION_PROVIDER": "mock",
    }
    for revision in ("0002_generation_idempotency", "head"):
        result = subprocess.run(
            [str(Path(sys.executable).with_name("alembic")), "upgrade", revision],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        if revision != "head":
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "INSERT INTO generations (id,user_id,kind,prompt,status) "
                    "VALUES ('old',1,'image','fox','processing')"
                )
                connection.execute(
                    "INSERT INTO webhook_deliveries "
                    "(generation_id,url,status,attempts) "
                    "VALUES ('old','https://test/hook','pending',2)"
                )
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT status,provider_context,run_token,webhook_url FROM generations"
        ).fetchone() == ("processing", None, None, None)
        assert connection.execute(
            "SELECT status,attempts,retry_job_id,recovery_checked_at,"
            "preparation_expires_at "
            "FROM webhook_deliveries"
        ).fetchone() == ("pending", 2, None, None, None)
