"""Real DB claims with mocked HTTP: duplicate jobs must not duplicate requests."""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select

from app import worker
from app.application.delivery import WebhookDelivery
from app.application.recovery import RecoveryService
from app.domain.entities import TaskStatus
from app.infrastructure.config import settings
from app.infrastructure.models import WebhookDeliveryModel
from app.infrastructure.repositories import WebhookDeliveryRepository
from app.infrastructure.webhook_http import SafeWebhookClient

URL = "https://customer.test/webhook"


class Clock:
    """Advance retry time without sleeping."""

    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self) -> None:
        """Advance by one retry interval."""
        self.now += timedelta(seconds=15)


class HTTPRecorder:
    """Expose an in-flight barrier and reject all real network traffic."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.status = 200
        self.block = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.error: httpx.HTTPError | None = None

    async def handle(self, request: httpx.Request) -> httpx.Response:
        """Record requests, optionally keeping the first worker in flight."""
        self.requests.append(request)
        self.entered.set()
        if self.block:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return httpx.Response(self.status)

    def client(self, **kwargs) -> httpx.AsyncClient:
        """Build a client whose only transport is the HTTP mock."""
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handle), **kwargs)


@pytest.fixture
async def delivery_case(database, monkeypatch) -> AsyncIterator[SimpleNamespace]:
    """Use the same real repositories with separate sessions for each worker."""
    monkeypatch.setattr(settings, "webhook_attempts", 5)
    monkeypatch.setattr(settings, "webhook_interval_seconds", 15)
    task = await database.service.create(1, "image", "fox", None, "delivery")
    task_id = task["id"]
    async with database.factory.create() as uow:
        generation = await uow.generations.get(task_id)
        generation.status = TaskStatus.completed
        generation.result = {"url": "https://result.test/image.png"}
    clock = Clock()
    http = HTTPRecorder()

    def make_service() -> WebhookDelivery:
        return WebhookDelivery(
            database.factory,
            database.service.get_payload,
            clock=clock,
            client_factory=http.client,
        )

    service = make_service()
    database.service.webhook = service
    monkeypatch.setattr(worker, "generation_service", database.service)
    pool = SimpleNamespace(enqueue_job=AsyncMock(), aclose=AsyncMock())
    pool_factory = AsyncMock(return_value=pool)
    monkeypatch.setattr(worker, "create_pool", pool_factory)
    yield SimpleNamespace(
        database=database,
        task_id=task_id,
        service=service,
        make_service=make_service,
        http=http,
        clock=clock,
        pool=pool,
        pool_factory=pool_factory,
    )


async def test_concurrent_workers_only_one_http(delivery_case) -> None:
    case = delivery_case
    await asyncio.gather(
        worker.run_generation({}, case.task_id, URL),
        worker.run_generation({}, case.task_id, URL),
    )
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("delivered", 1)
    assert len(case.http.requests) == 1
    case.pool_factory.assert_not_called()
    async with case.database.sessions() as session:
        count = await session.scalar(
            select(func.count()).select_from(WebhookDeliveryModel)
        )
        assert count == 1


async def test_second_worker_skips_while_owner_is_sending(delivery_case) -> None:
    case = delivery_case
    case.http.block = True
    owner = asyncio.create_task(case.service.deliver(case.task_id, URL))
    try:
        await asyncio.wait_for(case.http.entered.wait(), timeout=2)
        row = await case.service.get(case.task_id)
        assert (row.status, row.attempts) == ("sending", 1)
        second = case.make_service()
        result = await asyncio.wait_for(second.deliver(case.task_id, URL), timeout=2)
        assert result == (False, 1)
        await worker.retry_delivery({}, case.task_id, URL)
        assert len(case.http.requests) == 1
        case.pool_factory.assert_not_called()
    finally:
        case.http.release.set()
        await owner


@pytest.mark.parametrize("status", ["delivered", "failed"])
async def test_terminal_delivery_never_sends(delivery_case, status: str) -> None:
    case = delivery_case
    async with case.database.factory.create() as uow:
        await uow.deliveries.get_or_create(case.task_id, URL)
        await uow.deliveries.update_attempt(
            case.task_id, status=status, attempts=3, last_error="previous"
        )
    await worker.run_generation({}, case.task_id, URL)
    await worker.retry_delivery({}, case.task_id, URL, {"stale": True})
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error) == (status, 3, "previous")
    assert case.http.requests == []
    case.pool_factory.assert_not_called()


async def test_retry_loads_fresh_payload_and_stored_url(delivery_case) -> None:
    case = delivery_case
    case.http.status = 503
    await worker.run_generation({}, case.task_id, URL)
    assert json.loads(case.http.requests[0].content)["status"] == "completed"
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error) == ("pending", 1, "HTTP 503")
    assert row.next_attempt_at.replace(tzinfo=UTC) == case.clock.now + timedelta(
        seconds=15
    )
    # An early duplicate retry cannot bypass the persisted due time.
    await worker.retry_delivery({}, case.task_id, URL)
    assert len(case.http.requests) == 1
    async with case.database.factory.create() as uow:
        generation = await uow.generations.get(case.task_id)
        generation.status = TaskStatus.failed
        generation.result = None
        generation.error = "updated failure"
    case.clock.advance()
    case.http.status = 200
    # Simulate a new worker process: attempts and payload are loaded from DB.
    case.database.service.webhook = case.make_service()
    await worker.retry_delivery(
        {}, case.task_id, "https://wrong.test/hook", {"status": "completed"}
    )
    request = case.http.requests[-1]
    assert str(request.url) == URL
    assert json.loads(request.content) == {
        "id": case.task_id,
        "status": "failed",
        "result": None,
        "error": "updated failure",
    }
    for request in case.http.requests:
        assert request.headers["Idempotency-Key"] == case.task_id
        assert request.headers["X-Generation-ID"] == case.task_id
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error, row.next_attempt_at) == (
        "delivered",
        2,
        None,
        None,
    )
    job_ids = {call.kwargs["_job_id"] for call in case.pool.enqueue_job.call_args_list}
    assert job_ids == {f"delivery:{case.task_id}:1"}
    assert all(len(call.args) == 3 for call in case.pool.enqueue_job.call_args_list)


async def test_five_attempt_limit_even_with_concurrent_retry_jobs(
    delivery_case,
) -> None:
    case = delivery_case
    case.http.status = 500
    await worker.run_generation({}, case.task_id, URL)
    for attempt in range(2, 6):
        case.clock.advance()
        await asyncio.gather(
            worker.retry_delivery({}, case.task_id, URL),
            worker.retry_delivery({}, case.task_id, URL),
        )
        assert (await case.service.get(case.task_id)).attempts == attempt
    case.clock.advance()
    await worker.run_generation({}, case.task_id, URL)
    await worker.retry_delivery({}, case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error, row.next_attempt_at) == (
        "failed",
        5,
        "HTTP 500",
        None,
    )
    assert len(case.http.requests) == 5
    assert {
        call.kwargs["_job_id"] for call in case.pool.enqueue_job.call_args_list
    } == {f"delivery:{case.task_id}:{attempt}" for attempt in range(1, 5)}


async def test_stale_owner_cannot_finish_another_attempt(delivery_case) -> None:
    case = delivery_case
    case.http.status = 500
    await case.service.deliver(case.task_id, URL)
    case.clock.advance()
    async with case.database.factory.create() as uow:
        claim = await uow.deliveries.claim_attempt(case.task_id, case.clock(), 5)
        assert claim.attempts == 2
    async with case.database.factory.create() as uow:
        await uow.deliveries.finish_attempt(case.task_id, 1, "delivered", None, None)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("preparing", 2)


async def test_http_timeout_persists_retry_without_sensitive_error(
    delivery_case,
) -> None:
    case = delivery_case
    case.http.error = httpx.ReadTimeout("secret in URL must not be persisted")
    await case.service.deliver(case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error) == ("pending", 1, "ReadTimeout")
    assert row.next_attempt_at is not None


async def test_cancelled_owner_is_not_automatically_replaced(delivery_case) -> None:
    case = delivery_case
    case.http.block = True
    owner = asyncio.create_task(case.service.deliver(case.task_id, URL))
    try:
        await asyncio.wait_for(case.http.entered.wait(), timeout=2)
    finally:
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
    await case.make_service().deliver(case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("sending", 1)
    assert len(case.http.requests) == 1


async def test_failed_outcome_commit_does_not_resend(
    delivery_case, monkeypatch
) -> None:
    case = delivery_case
    original = WebhookDeliveryRepository.finish_attempt
    monkeypatch.setattr(
        WebhookDeliveryRepository,
        "finish_attempt",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    with pytest.raises(RuntimeError, match="database unavailable"):
        await case.service.deliver(case.task_id, URL)
    monkeypatch.setattr(WebhookDeliveryRepository, "finish_attempt", original)
    await case.make_service().deliver(case.task_id, URL)
    assert (await case.service.get(case.task_id)).status == "sending"
    assert len(case.http.requests) == 1


async def test_missing_generation_does_not_send_json_null(delivery_case) -> None:
    case = delivery_case
    service = WebhookDelivery(
        case.database.factory, clock=case.clock, client_factory=case.http.client
    )
    await service.deliver("missing", URL, {"stale": True})
    row = await service.get("missing")
    assert (row.status, row.attempts, row.last_error) == (
        "pending",
        1,
        "Generation payload unavailable",
    )
    assert case.http.requests == []


async def test_payload_loader_failure_retries_without_stuck_sending(
    delivery_case,
) -> None:
    case = delivery_case
    case.service.payload_loader = AsyncMock(side_effect=RuntimeError("secret"))
    await case.service.deliver(case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.last_error) == ("pending", 1, "RuntimeError")
    assert row.next_attempt_at is not None
    assert not case.http.requests
    case.clock.advance()
    await case.make_service().deliver(case.task_id, URL)
    assert (await case.service.get(case.task_id)).status == "delivered"
    assert len(case.http.requests) == 1


async def test_preparation_crash_recovers_and_fences_stale_owner(delivery_case) -> None:
    case = delivery_case
    async with case.database.factory.create() as uow:
        await uow.deliveries.get_or_create(case.task_id, URL)
        claim = await uow.deliveries.claim_attempt(case.task_id, case.clock(), 5)
    case.clock.now += timedelta(seconds=31)
    queue = SimpleNamespace(
        status=AsyncMock(return_value="not_found"), enqueue=AsyncMock()
    )
    recovery = RecoveryService(case.database.factory, case.clock)
    await recovery.run(queue)
    await recovery.run(queue)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("pending", 1)
    async with case.database.factory.create() as uow:
        assert not await uow.deliveries.begin_sending(
            case.task_id, claim.attempts, case.clock()
        )
        await uow.deliveries.finish_attempt(case.task_id, 1, "delivered", None, None)
    assert (await case.service.get(case.task_id)).status == "pending"
    case.clock.advance()
    await case.service.deliver(case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("delivered", 2)
    assert len(case.http.requests) == 1


async def test_five_preparation_failures_stop_without_http(delivery_case) -> None:
    case = delivery_case
    case.service.payload_loader = AsyncMock(side_effect=RuntimeError("offline"))
    for _ in range(6):
        await case.service.deliver(case.task_id, URL)
        case.clock.advance()
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.next_attempt_at) == ("failed", 5, None)
    assert not case.http.requests


async def test_cancel_during_preparation_does_not_leave_sending(delivery_case) -> None:
    case = delivery_case
    case.service.payload_loader = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await case.service.deliver(case.task_id, URL)
    assert (await case.service.get(case.task_id)).status == "pending"
    assert not case.http.requests


async def test_unsafe_legacy_delivery_fails_without_http(delivery_case) -> None:
    case = delivery_case
    case.service.client_factory = lambda **kw: SafeWebhookClient(
        transport=httpx.MockTransport(case.http.handle), **kw
    )
    await case.service.deliver(case.task_id, "http://127.0.0.1/private")
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts) == ("failed", 1)
    assert row.last_error == "UnsafeWebhookURL"
    assert not case.http.requests


async def test_preparation_database_failure_is_recovered(
    delivery_case, monkeypatch
) -> None:
    case = delivery_case
    case.service.payload_loader = AsyncMock(side_effect=RuntimeError("loader failed"))
    original = WebhookDeliveryRepository.finish_attempt
    monkeypatch.setattr(
        WebhookDeliveryRepository,
        "finish_attempt",
        AsyncMock(side_effect=RuntimeError("DB offline")),
    )
    with pytest.raises(RuntimeError, match="DB offline"):
        await case.service.deliver(case.task_id, URL)
    assert (await case.service.get(case.task_id)).status == "preparing"
    monkeypatch.setattr(WebhookDeliveryRepository, "finish_attempt", original)
    case.clock.now += timedelta(seconds=31)
    queue = SimpleNamespace(
        status=AsyncMock(return_value="not_found"), enqueue=AsyncMock()
    )
    await RecoveryService(case.database.factory, case.clock).run(queue)
    case.clock.advance()
    await case.make_service().deliver(case.task_id, URL)
    assert (await case.service.get(case.task_id)).status == "delivered"
    assert len(case.http.requests) == 1


async def test_expired_fifth_preparation_is_final(delivery_case) -> None:
    case = delivery_case
    async with case.database.factory.create() as uow:
        await uow.deliveries.get_or_create(case.task_id, URL)
        await uow.deliveries.update_attempt(case.task_id, attempts=4)
        await uow.deliveries.claim_attempt(case.task_id, case.clock(), 5)
    case.clock.now += timedelta(seconds=31)
    async with case.database.factory.create() as uow:
        assert await uow.deliveries.recover_preparations(case.clock(), 5, 15, 100) == 1
    await case.service.deliver(case.task_id, URL)
    row = await case.service.get(case.task_id)
    assert (row.status, row.attempts, row.next_attempt_at) == ("failed", 5, None)
    assert not case.http.requests
