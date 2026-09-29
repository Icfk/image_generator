"""Shared real-database fixtures for concurrency and delivery tests."""

import os
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Configuration must be selected before importing modules with composition roots.
# Never change an explicitly supplied environment (production validation is tested).
os.environ.setdefault("APP_ENV", "test")

from app.application.services import GenerationService  # noqa: E402
from app.infrastructure.database import UnitOfWorkFactory  # noqa: E402
from app.infrastructure.models import Base, UserModel  # noqa: E402

BACKENDS = ["sqlite"] + (["postgres"] if os.getenv("TEST_POSTGRES_URL") else [])


@pytest.fixture(params=BACKENDS)
async def database(request, tmp_path):
    """Isolate each test in a local SQLite file or its own PostgreSQL schema."""
    schema = f"idempotency_{uuid.uuid4().hex}"
    admin = None
    if request.param == "postgres":
        url = os.environ["TEST_POSTGRES_URL"]
        admin = create_async_engine(url)
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_async_engine(
            url, connect_args={"server_settings": {"search_path": schema}}
        )
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/test.db")

        @event.listens_for(engine.sync_engine, "connect")
        def configure_sqlite(connection, _record):
            connection.isolation_level = None

        @event.listens_for(engine.sync_engine, "begin")
        def begin_sqlite(connection):
            # Explicit BEGIN ensures releasing a savepoint does not commit the
            # outer transaction under sqlite's legacy transaction mode.
            connection.exec_driver_sql("BEGIN")

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with sessions.begin() as session:
            session.add(UserModel(external_id="user", secret_hash="hash", balance=1))
        yield SimpleNamespace(
            sessions=sessions,
            service=GenerationService(UnitOfWorkFactory(sessions)),
            factory=UnitOfWorkFactory(sessions),
            backend=request.param,
        )
    finally:
        await engine.dispose()
        if admin is not None:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await admin.dispose()
