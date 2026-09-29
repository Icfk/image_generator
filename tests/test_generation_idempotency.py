"""Exercise real repositories/UoW; only Redis publication is substituted."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.domain.entities import TaskStatus
from app.infrastructure.models import (
    BalanceTransactionModel,
    GenerationModel,
    JobPublicationModel,
    UserModel,
)
from app.infrastructure.repositories import (
    GenerationRepository,
    JobPublicationRepository,
)
from app.main import app
from app.presentation import router as routes
from app.presentation.dependencies import current_user, rate_limit


async def assert_single_reservation(database, task_id):
    """Read committed state from another session, not from service return data."""
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 0
        for model in (GenerationModel, JobPublicationModel, BalanceTransactionModel):
            assert await session.scalar(select(func.count()).select_from(model)) == 1
        ledger = await session.scalar(select(BalanceTransactionModel))
        assert (ledger.generation_id, ledger.operation, ledger.amount) == (
            task_id,
            "RESERVE",
            -1,
        )
        assert ledger.external_key == f"reserve:{task_id}"
        job = await session.scalar(select(JobPublicationModel))
        assert job.job_id == f"generation:{task_id}"


async def test_duplicate_after_commit_returns_current_generation(database):
    service = database.service
    first = await service.create_request(1, "image", "fox", None, "key")
    # Replays remain valid with an empty balance, after completion, and after
    # changing request content: the original key identifies the original task.
    async with database.sessions.begin() as session:
        task = await session.get(GenerationModel, first.id)
        task.status = TaskStatus.completed
    duplicate = await service.create_request(1, "video", "changed", None, "key")
    assert first.created is True
    assert (duplicate.id, duplicate.status, duplicate.created) == (
        first.id,
        "completed",
        False,
    )
    await assert_single_reservation(database, first.id)


async def test_two_concurrent_requests_use_unique_constraint(database, monkeypatch):
    reread = AsyncMock(wraps=GenerationRepository.get_by_idempotency_key)

    async def count_reread(self, user_id, key):
        return await reread(self, user_id, key)

    monkeypatch.setattr(GenerationRepository, "get_by_idempotency_key", count_reread)
    first, second = await asyncio.gather(
        database.service.create_request(1, "image", "fox", None, "concurrent"),
        database.service.create_request(1, "image", "fox", None, "concurrent"),
    )
    assert first.id == second.id
    assert sorted([first.created, second.created]) == [False, True]
    assert reread.await_count == 1  # Actual IntegrityError loser re-read path.
    await assert_single_reservation(database, first.id)


async def test_savepoint_preserves_outer_transaction(database):
    first = await database.service.create_request(1, "image", "fox", None, "key")
    async with database.factory.create() as uow:
        await uow.users.create("outer-change", "hash-2")
        duplicate = await uow.create_reserved_generation(1, "image", "fox", None, "key")
        assert duplicate.id == first.id
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(UserModel)) == 2
    await assert_single_reservation(database, first.id)


async def test_duplicate_waits_for_uncommitted_winner(database, monkeypatch):
    inserted = asyncio.Event()
    release = asyncio.Event()
    second_entered = asyncio.Event()
    original = GenerationRepository.create
    calls = 0

    async def hold_first_insert(self, **values):
        nonlocal calls
        calls += 1
        if calls == 1:
            task = await original(self, **values)
            inserted.set()
            await release.wait()
            return task
        second_entered.set()
        return await original(self, **values)

    monkeypatch.setattr(GenerationRepository, "create", hold_first_insert)
    first = asyncio.create_task(
        database.service.create_request(1, "image", "fox", None, "uncommitted")
    )
    second = None
    try:
        await asyncio.wait_for(inserted.wait(), timeout=5)
        second = asyncio.create_task(
            database.service.create_request(1, "image", "fox", None, "uncommitted")
        )
        await asyncio.wait_for(second_entered.wait(), timeout=5)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(second), timeout=0.05)
    finally:
        release.set()
        tasks = [first] + ([second] if second is not None else [])
        results = await asyncio.gather(*tasks)
    assert results[0].id == results[1].id
    assert [result.created for result in results] == [True, False]
    await assert_single_reservation(database, results[0].id)


async def test_failure_after_reservation_rolls_back_everything(database, monkeypatch):
    original = JobPublicationRepository.create

    async def fail_after_insert(self, task_id):
        await original(self, task_id)
        raise RuntimeError("outbox failure")

    monkeypatch.setattr(JobPublicationRepository, "create", fail_after_insert)
    with pytest.raises(RuntimeError, match="outbox failure"):
        await database.service.create_request(1, "image", "fox", None, "key")
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 1
        for model in (GenerationModel, BalanceTransactionModel, JobPublicationModel):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_insufficient_balance_is_atomic(database):
    async with database.sessions.begin() as session:
        (await session.get(UserModel, 1)).balance = 0
    with pytest.raises(PermissionError):
        await database.service.create_request(1, "image", "fox", None, "key")
    async with database.sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(GenerationModel)) == 0
        )
        assert (
            await session.scalar(select(func.count()).select_from(JobPublicationModel))
            == 0
        )


async def test_unrelated_integrity_error_is_not_swallowed(database, monkeypatch):
    async def fail(*_args, **_kwargs):
        raise IntegrityError("INSERT", {}, RuntimeError("unrelated constraint"))

    monkeypatch.setattr(JobPublicationRepository, "create", fail)
    with pytest.raises(IntegrityError):
        await database.service.create_request(1, "image", "fox", None, "key")
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 1


async def test_keys_are_scoped_to_user_and_absent_keys_remain_independent(database):
    async with database.sessions.begin() as session:
        (await session.get(UserModel, 1)).balance = 3
        second_user = UserModel(external_id="second", secret_hash="hash-2", balance=1)
        session.add(second_user)
    first = await database.service.create_request(1, "image", "fox", None, "shared")
    second = await database.service.create_request(
        second_user.id, "image", "fox", None, "shared"
    )
    no_key_a = await database.service.create_request(1, "image", "fox", None)
    no_key_b = await database.service.create_request(1, "image", "fox", None)
    assert len({first.id, second.id, no_key_a.id, no_key_b.id}) == 4
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 0
        assert (await session.get(UserModel, second_user.id)).balance == 0


async def test_outer_rollback_restores_reservation_and_outbox(database):
    with pytest.raises(RuntimeError, match="abort outer"):
        async with database.factory.create() as uow:
            await uow.create_reserved_generation(1, "image", "fox", None, "key")
            raise RuntimeError("abort outer")
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 1
        for model in (GenerationModel, BalanceTransactionModel, JobPublicationModel):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


class RecordingQueue:
    """Model deterministic ARQ ids, including acknowledgement loss."""

    def __init__(self):
        self.jobs = {}
        self.calls = []
        self.lose_acknowledgement = False

    async def enqueue_job(self, *args, _job_id):
        self.calls.append(_job_id)
        exists = _job_id in self.jobs
        self.jobs[_job_id] = args
        if self.lose_acknowledgement:
            raise TimeoutError("ack lost after enqueue")
        return None if exists else object()

    async def aclose(self):
        """No network resources to close."""


@pytest.fixture
async def api(database, monkeypatch):
    queue = RecordingQueue()
    pool_factory = AsyncMock(return_value=queue)
    monkeypatch.setattr(routes, "generation_service", database.service)
    monkeypatch.setattr(routes, "create_pool", pool_factory)
    overrides = app.dependency_overrides.copy()
    app.dependency_overrides[current_user] = lambda: SimpleNamespace(id=1)
    app.dependency_overrides[rate_limit] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            yield client, queue, pool_factory
    finally:
        app.dependency_overrides = overrides


async def test_header_duplicate_does_not_publish_twice(database, api):
    client, queue, pool_factory = api
    headers = {"Idempotency-Key": "api-key"}
    first = await client.post(
        "/generations/images", headers=headers, json={"prompt": "fox"}
    )
    second = await client.post(
        "/generations/images", headers=headers, json={"prompt": "fox"}
    )
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert len(queue.calls) == len(queue.jobs) == pool_factory.await_count == 1
    await assert_single_reservation(database, first.json()["id"])


async def test_concurrent_http_duplicates_publish_once(database, api):
    client, queue, _ = api
    responses = await asyncio.gather(
        *(
            client.post(
                "/generations/videos",
                headers={"Idempotency-Key": "race"},
                json={"prompt": "fox"},
            )
            for _ in range(2)
        )
    )
    assert [response.status_code for response in responses] == [202, 202]
    assert responses[0].json() == responses[1].json()
    assert len(queue.calls) == len(queue.jobs) == 1
    await assert_single_reservation(database, responses[0].json()["id"])


async def test_duplicate_after_redis_accepted_but_reply_lost(database, api):
    client, queue, pool_factory = api
    queue.lose_acknowledgement = True
    headers = {"Idempotency-Key": "uncertain"}
    first = await client.post(
        "/generations/images", headers=headers, json={"prompt": "fox"}
    )
    assert first.status_code == 503
    assert len(queue.jobs) == 1
    attempts = len(queue.calls)
    pool_factory.side_effect = AssertionError("duplicate must not contact Redis")
    second = await client.post(
        "/generations/images", headers=headers, json={"prompt": "fox"}
    )
    assert second.status_code == 202
    assert len(queue.calls) == attempts
    await assert_single_reservation(database, second.json()["id"])
    async with database.sessions() as session:
        job = await session.scalar(select(JobPublicationModel))
        assert job.status == "unknown"


async def test_duplicate_when_arq_reports_existing_job(database, api):
    client, queue, _ = api
    original = queue.enqueue_job

    async def enqueue_existing(*args, _job_id):
        queue.jobs[_job_id] = args
        return await original(*args, _job_id=_job_id)

    queue.enqueue_job = enqueue_existing
    body = {"prompt": "fox", "idempotency_key": "already-in-redis"}
    first = await client.post("/generations/images", json=body)
    second = await client.post("/generations/images", json=body)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert len(queue.calls) == len(queue.jobs) == 1
    await assert_single_reservation(database, first.json()["id"])


async def test_connection_failure_preserves_pending_reservation(database, api):
    client, queue, pool_factory = api
    pool_factory.side_effect = ConnectionError("Redis unavailable")
    body = {"prompt": "fox", "idempotency_key": "connection-failure"}
    first = await client.post("/generations/images", json=body)
    second = await client.post("/generations/images", json=body)
    assert first.status_code == 503
    assert second.status_code == 202
    assert pool_factory.await_count == 1
    assert not queue.calls
    await assert_single_reservation(database, second.json()["id"])


async def test_invalid_key_rejected_before_charging(database, api):
    client, queue, _ = api
    for key in ("", "a" * 201):
        response = await client.post(
            "/generations/images",
            headers={"Idempotency-Key": key},
            json={"prompt": "fox"},
        )
        assert response.status_code == 422
    assert not queue.calls
    async with database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 1


async def test_body_key_remains_supported_and_header_mismatch_is_rejected(
    database, api
):
    client, queue, _ = api
    body = {"prompt": "fox", "idempotency_key": "body-key"}
    first = await client.post("/generations/images", json=body)
    second = await client.post("/generations/images", json=body)
    mismatch = await client.post(
        "/generations/images", json=body, headers={"Idempotency-Key": "other"}
    )
    assert first.json() == second.json()
    assert mismatch.status_code == 400
    assert len(queue.calls) == 1
    await assert_single_reservation(database, first.json()["id"])
