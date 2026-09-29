"""Application records never expose ORM; mapping preserves transaction semantics."""

import ast
from pathlib import Path

import pytest
from sqlalchemy import inspect

from app.domain.entities import TaskStatus
from app.domain.records import GenerationRecord, UserRecord


async def test_application_uses_domain_records_and_commits_explicit_edits(database):
    task = await database.service.create(1, "image", "fox", None)
    snapshot = await database.service.get(task["id"])
    assert isinstance(snapshot, GenerationRecord)
    assert inspect(snapshot, raiseerr=False) is None
    async with database.service.uow_factory.create() as uow:
        assert not hasattr(uow, "session")
        assert not hasattr(uow.generations, "session")
        user = await uow.users.get(1)
        assert isinstance(user, UserRecord)
        record = await uow.generations.lock(task["id"])
        record.error = "application edit"
    assert (await database.service.get(task["id"])).error == "application edit"
    snapshot.error = "detached mutation"
    assert (await database.service.get(task["id"])).error == "application edit"


async def test_domain_edits_and_repository_changes_rollback_together(database):
    task = await database.service.create(1, "image", "fox", None)
    with pytest.raises(RuntimeError):
        async with database.service.uow_factory.create() as uow:
            record = await uow.generations.lock(task["id"])
            record.status = TaskStatus.failed
            await uow.balance.refund_once(1, task["id"], 1, f"refund:{task['id']}")
            raise RuntimeError("rollback all")
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 0
        assert (await uow.generations.get(task["id"])).status == TaskStatus.queued
        assert not await uow.balance.has_external_key(f"refund:{task['id']}")


def test_services_do_not_import_sqlalchemy_or_concrete_database() -> None:
    paths = [
        *Path("app/application").glob("*.py"),
        *Path("app/presentation").glob("*.py"),
        Path("app/worker.py"),
    ]
    for path in paths:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith(
                    (
                        "sqlalchemy",
                        "app.infrastructure.models",
                        "app.infrastructure.repositories",
                        "app.infrastructure.database",
                    )
                ), (path, node.module)
            if isinstance(node, ast.Import):
                assert not any(
                    alias.name.startswith("sqlalchemy") for alias in node.names
                )
