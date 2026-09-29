"""Recovery integration tests: real repositories, deterministic queue/provider."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from arq import Retry
from sqlalchemy import delete, func, select

from app import worker
from app.application.recovery import RecoveryService
from app.application.services import GenerationService
from app.domain.entities import RetryableProviderError, TaskStatus
from app.infrastructure.metrics import RECOVERY_ACTIONS
from app.infrastructure.models import (
    BalanceTransactionModel,
    JobPublicationModel,
    UserModel,
    WebhookDeliveryModel,
)
from app.infrastructure.repositories import GenerationRepository, UnitOfWork
from app.recovery_worker import run_cycle

URL = "https://customer.test/hook"


class Queue:
    """Simulate Redis loss, completed job retention, and uncertain enqueue ACKs."""

    def __init__(self) -> None:
        self.jobs = {}
        self.completed = set()
        self.calls = []
        self.lose_ack = False
        self.offline = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.block = False

    async def status(self, job_id: str) -> str:
        """Read a Redis-like volatile job state."""
        if self.offline:
            raise ConnectionError("Redis unavailable")
        if job_id in self.completed:
            return "complete"
        return "queued" if job_id in self.jobs else "not_found"

    async def enqueue(self, function: str, job_id: str, *args) -> None:
        """Use job ID as an idempotency key, even when the ACK is lost."""
        self.calls.append(job_id)
        self.jobs.setdefault(job_id, (function, args))
        self.entered.set()
        if self.block:
            await self.release.wait()
        if self.lose_ack:
            raise TimeoutError("Redis ACK lost")


class Provider:
    """Model durable provider state across worker crashes without network calls."""

    def __init__(self, phase: str = "success") -> None:
        self.phase = phase
        self.submits = 0
        self.polls = []
        self.result = {"url": "https://mock.test/result", "provider_job_id": "remote-1"}

    async def generate(
        self,
        kind,
        prompt,
        image_url,
        provider_job_id=None,
        *,
        on_submitted=None,
        provider_context=None,
    ) -> dict:
        """Checkpoint before polling, including simulated remote completion."""
        if self.phase == "before_call":
            raise asyncio.CancelledError()
        if self.phase == "retryable":
            raise RetryableProviderError("temporary error")
        if not provider_job_id:
            self.submits += 1
            provider_job_id = "remote-1"
            await on_submitted({"request_id": provider_job_id, "provider": "test"})
        self.polls.append(provider_job_id)
        if self.phase in ("after_submit", "after_completed"):
            raise asyncio.CancelledError()
        return self.result


@pytest.fixture
async def recovery_case(database):
    """Create a committed reservation and a clock that can expire a claim."""
    now = [datetime.now(UTC)]
    task = await database.service.create_request(
        1, "image", "fox", None, "recovery-test", URL
    )
    yield SimpleNamespace(
        database=database,
        task_id=task.id,
        now=now,
        queue=Queue(),
        recovery=RecoveryService(database.factory, lambda: now[0]),
    )


async def counts(case, expected_balance: int, refunds: int = 0) -> None:
    """Assert the ledger and balance from another committed DB session."""
    async with case.database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == expected_balance
        assert (
            await session.scalar(
                select(func.count())
                .select_from(BalanceTransactionModel)
                .where(BalanceTransactionModel.operation == "REFUND")
            )
            == refunds
        )
        assert (
            await session.scalar(select(func.count()).select_from(JobPublicationModel))
            == 1
        )


async def test_orphaned_queued_generation(recovery_case) -> None:
    case = recovery_case
    async with case.database.sessions.begin() as session:
        await session.execute(delete(JobPublicationModel))
    assert await case.recovery.run(case.queue)
    assert await case.recovery.run(case.queue)
    assert list(case.queue.jobs.values()) == [("run_generation", (case.task_id, URL))]
    assert len(case.queue.calls) == 1
    await counts(case, 0)


async def test_unknown_enqueue_ack_reconciles_without_refund(recovery_case) -> None:
    case = recovery_case
    case.queue.lose_ack = True
    assert not await case.recovery.run(case.queue)
    async with case.database.factory.create() as uow:
        publication = await uow.publications.get(case.task_id)
        assert (publication.status, publication.last_error) == (
            "unknown",
            "TimeoutError",
        )
    case.queue.lose_ack = False
    case.now[0] += timedelta(seconds=16)
    assert await case.recovery.run(case.queue)
    assert len(case.queue.calls) == 1
    async with case.database.factory.create() as uow:
        assert (await uow.publications.get(case.task_id)).status == "published"
    await counts(case, 0)


async def test_redis_restart_republishes_previously_published(recovery_case) -> None:
    case = recovery_case
    await case.recovery.run(case.queue)
    first_id = next(iter(case.queue.jobs))
    case.queue.jobs.clear()  # Volatile Redis state lost; DB still says published.
    case.queue.offline = True
    case.now[0] += timedelta(seconds=16)
    assert not await case.recovery.run(case.queue)
    await counts(case, 0)
    case.queue.offline = False
    case.now[0] += timedelta(seconds=16)
    assert await case.recovery.run(case.queue)
    assert list(case.queue.jobs) == [first_id]
    await case.recovery.run(case.queue)
    assert len(case.queue.calls) == 2  # One before loss, one after, same ID.


async def test_crash_before_provider_call_fails_and_refunds_once(recovery_case) -> None:
    case = recovery_case
    provider = Provider("before_call")
    service = GenerationService(case.database.factory, provider)
    with pytest.raises(asyncio.CancelledError):
        await service.run(case.task_id, URL)
    task = await service.get(case.task_id)
    assert task.status == TaskStatus.processing
    assert task.provider_job_id is None
    case.now[0] += timedelta(seconds=601)
    await case.recovery.run(case.queue)
    await case.recovery.run(case.queue)
    task = await service.get(case.task_id)
    assert task.status == TaskStatus.failed
    assert "no resubmit" in task.error
    assert provider.submits == 0
    assert all(job[0] == "retry_delivery" for job in case.queue.jobs.values())
    await counts(case, 1, refunds=1)


@pytest.mark.parametrize("phase", ["after_submit", "after_completed"])
async def test_crash_with_provider_id_resumes_without_submit(
    recovery_case, phase
) -> None:
    case = recovery_case
    provider = Provider(phase)
    service = GenerationService(case.database.factory, provider)
    with pytest.raises(asyncio.CancelledError):
        await service.run(case.task_id, URL)
    task = await service.get(case.task_id)
    assert task.status == TaskStatus.processing
    assert task.provider_job_id == "remote-1"
    assert task.provider_context["provider"] == "test"
    old_token = task.run_token
    case.now[0] += timedelta(seconds=601)
    await case.recovery.run(case.queue)
    await case.recovery.run(case.queue)
    assert len(case.queue.jobs) == 1
    assert next(iter(case.queue.jobs)) != f"generation:{case.task_id}"
    async with case.database.factory.create() as uow:
        assert not await uow.generations.checkpoint(
            case.task_id, old_token, {"request_id": "stale-worker"}
        )
    provider.phase = "success"
    payload = await service.run(case.task_id, None)
    assert payload["status"] == "completed"
    assert provider.submits == 1
    assert provider.polls == ["remote-1", "remote-1"]
    assert await service.run(case.task_id, None) is None
    await counts(case, 0)


async def test_crash_after_db_completed_only_recovers_delivery(recovery_case) -> None:
    case = recovery_case
    provider = Provider()
    service = GenerationService(case.database.factory, provider)
    await service.run(case.task_id, URL)
    # Crash before the first HTTP attempt / Redis delivery publication.
    await case.recovery.run(case.queue)
    await case.recovery.run(case.queue)
    assert list(case.queue.jobs.values()) == [("retry_delivery", (case.task_id, URL))]
    assert provider.submits == 1
    assert await service.run(case.task_id, URL) is None


@pytest.mark.parametrize("status", [TaskStatus.completed, TaskStatus.failed])
async def test_terminal_legacy_generation_gets_delivery_not_generation_job(
    recovery_case, status
) -> None:
    case = recovery_case
    async with case.database.factory.create() as uow:
        task = await uow.generations.get(case.task_id)
        task.status = status
    await case.recovery.run(case.queue)
    await case.recovery.run(case.queue)
    assert list(case.queue.jobs.values()) == [("retry_delivery", (case.task_id, URL))]
    async with case.database.sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(WebhookDeliveryModel))
            == 1
        )


async def test_pending_delivery_due_and_completed_redis_tombstone(
    recovery_case,
) -> None:
    case = recovery_case
    async with case.database.factory.create() as uow:
        task = await uow.generations.get(case.task_id)
        task.status = TaskStatus.completed
        row = await uow.deliveries.get_or_create(case.task_id, URL)
        row.attempts = 2
        row.last_error = "HTTP 503"
        row.next_attempt_at = case.now[0] + timedelta(seconds=15)
    await case.recovery.run(case.queue)
    assert not case.queue.jobs
    case.queue.completed.add(f"delivery:{case.task_id}:2")
    case.now[0] += timedelta(seconds=16)
    await case.recovery.run(case.queue)
    await case.recovery.run(case.queue)
    assert len(case.queue.calls) == 1
    assert next(iter(case.queue.jobs)).startswith("recovery:")
    async with case.database.factory.create() as uow:
        row = await uow.deliveries.get_by_generation(case.task_id)
        assert (row.attempts, row.last_error, row.status) == (2, "HTTP 503", "pending")


async def test_sending_delivery_is_never_stolen(recovery_case) -> None:
    case = recovery_case
    async with case.database.factory.create() as uow:
        task = await uow.generations.get(case.task_id)
        task.status = TaskStatus.completed
        row = await uow.deliveries.get_or_create(case.task_id, URL)
        row.status = "sending"
    await case.recovery.run(case.queue)
    assert not case.queue.jobs


async def test_concurrent_recovery_skips_locked_generation(recovery_case) -> None:
    case = recovery_case
    if case.database.backend != "postgres":
        pytest.skip("Row locks require PostgreSQL; SQLite does not support FOR UPDATE")
    case.queue.block = True
    first = asyncio.create_task(case.recovery.run(case.queue))
    try:
        await asyncio.wait_for(case.queue.entered.wait(), 2)
        second = RecoveryService(case.database.factory, lambda: case.now[0])
        assert await asyncio.wait_for(second.run(case.queue), 2)
        assert len(case.queue.calls) == 1
    finally:
        case.queue.release.set()
        await first
    await counts(case, 0)


async def test_result_from_old_worker_cannot_overwrite_recovery(recovery_case) -> None:
    case = recovery_case
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_provider(*args, **kwargs):
        entered.set()
        await release.wait()
        return {"url": "https://mock.test/late"}

    provider = SimpleNamespace(generate=slow_provider)
    service = GenerationService(case.database.factory, provider)
    task = asyncio.create_task(service.run(case.task_id, URL))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        case.now[0] += timedelta(seconds=601)
        await case.recovery.run(case.queue)
    finally:
        release.set()
    assert await task is None
    assert (await service.get(case.task_id)).status == TaskStatus.failed
    await counts(case, 1, refunds=1)


async def test_retryable_worker_then_final_failure_is_refunded_once(
    recovery_case, monkeypatch
) -> None:
    case = recovery_case
    provider = Provider("retryable")
    service = GenerationService(case.database.factory, provider)
    monkeypatch.setattr(worker, "generation_service", service)
    with pytest.raises(Retry):
        await worker.run_generation({"job_try": 1}, case.task_id, None)
    assert (await service.get(case.task_id)).status == TaskStatus.queued
    await counts(case, 0)
    await worker.run_generation({"job_try": 3}, case.task_id, None)
    await worker.run_generation({"job_try": 3}, case.task_id, None)
    await counts(case, 1, refunds=1)


async def test_recovery_metrics_and_heartbeat_only_after_success(recovery_case) -> None:
    case = recovery_case
    counter = RECOVERY_ACTIONS.labels("publication", "published")
    before = counter._value.get()
    heartbeat = SimpleNamespace(beat=AsyncMock())
    assert await run_cycle(case.recovery, case.queue, heartbeat)
    assert counter._value.get() == before + 1
    heartbeat.beat.assert_awaited_once()
    case.now[0] += timedelta(seconds=16)
    case.queue.offline = True
    assert not await run_cycle(case.recovery, case.queue, heartbeat)
    heartbeat.beat.assert_awaited_once()


async def test_checkpoint_failure_never_starts_another_submit(
    recovery_case, monkeypatch
) -> None:
    case = recovery_case
    provider = Provider()
    service = GenerationService(case.database.factory, provider)
    monkeypatch.setattr(
        GenerationRepository,
        "checkpoint",
        AsyncMock(side_effect=RuntimeError("DB down")),
    )
    assert await service.run(case.task_id, URL) is None
    assert (await service.get(case.task_id)).status == TaskStatus.processing
    assert await service.run(case.task_id, URL) is None
    assert provider.submits == 1
    await counts(case, 0)


@pytest.mark.parametrize("after_commit", [False, True])
async def test_enqueue_then_uncertain_db_commit_is_idempotent(
    recovery_case, monkeypatch, after_commit
) -> None:
    case = recovery_case
    case.queue.completed.add(f"generation:{case.task_id}")
    original = UnitOfWork.__aexit__

    async def faulty_exit(self, *args):
        if after_commit:
            await original(self, *args)
        else:
            await self.session.rollback()
        raise RuntimeError("commit acknowledgement lost")

    monkeypatch.setattr(UnitOfWork, "__aexit__", faulty_exit)
    with pytest.raises(RuntimeError, match="commit acknowledgement"):
        await case.recovery.recover_generation(case.task_id, case.queue)
    monkeypatch.setattr(UnitOfWork, "__aexit__", original)
    case.now[0] += timedelta(seconds=16)
    await case.recovery.run(case.queue)
    assert len(case.queue.jobs) == 1
    await counts(case, 0)


async def test_concurrent_recovery_skips_locked_delivery(recovery_case) -> None:
    case = recovery_case
    if case.database.backend != "postgres":
        pytest.skip("Row locks require PostgreSQL")
    async with case.database.factory.create() as uow:
        (await uow.generations.get(case.task_id)).status = TaskStatus.completed
        await uow.deliveries.get_or_create(case.task_id, URL)
    case.queue.block = True
    first = asyncio.create_task(case.recovery.run(case.queue))
    try:
        await asyncio.wait_for(case.queue.entered.wait(), 2)
        second = RecoveryService(case.database.factory, lambda: case.now[0])
        assert await asyncio.wait_for(second.run(case.queue), 2)
        assert len(case.queue.calls) == 1
    finally:
        case.queue.release.set()
        await first
