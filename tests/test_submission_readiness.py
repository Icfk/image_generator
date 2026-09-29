"""Regression tests for submission blockers, with no real provider traffic."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.application.providers import FalAIProvider, ProviderManager
from app.domain.entities import RetryableProviderError, TaskStatus
from app.infrastructure.config import settings
from app.infrastructure.repositories import BalanceRepository, UnitOfWork
from app.main import app
from app.presentation import dependencies
from app.presentation import router as routes
from app.presentation.dependencies import current_user, rate_limit


async def test_unrelated_refund_error_rolls_back_failed_state(database, monkeypatch):
    task = await database.service.create(1, "image", "fox", None)
    original = BalanceRepository.add_transaction

    async def fail(self, **values):
        raise IntegrityError("INSERT", {}, RuntimeError("unrelated constraint"))

    monkeypatch.setattr(BalanceRepository, "add_transaction", fail)
    with pytest.raises(IntegrityError):
        await database.service.fail_and_refund(task["id"], "provider failed")
    async with database.factory.create() as uow:
        assert (await uow.generations.get(task["id"])).status == TaskStatus.queued
        assert (await uow.users.get(1)).balance == 0
        assert not await uow.balance.has_external_key(f"refund:{task['id']}")
    monkeypatch.setattr(BalanceRepository, "add_transaction", original)
    await database.service.fail_and_refund(task["id"], "provider failed")
    await database.service.fail_and_refund(task["id"], "provider failed")
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 1
        assert (await uow.generations.get(task["id"])).status == TaskStatus.failed


async def test_real_refund_integrity_failure_cannot_commit_failed(database):
    task = await database.service.create(1, "image", "fox", None)
    async with database.sessions.begin() as session:
        if database.backend == "postgres":
            await session.execute(
                text(
                    "ALTER TABLE balance_transactions ADD CONSTRAINT reject_refund "
                    "CHECK (operation <> 'REFUND')"
                )
            )
        else:
            await session.execute(
                text(
                    "CREATE TRIGGER reject_refund "
                    "BEFORE INSERT ON balance_transactions "
                    "WHEN NEW.operation = 'REFUND' BEGIN "
                    "SELECT RAISE(ABORT, 'unrelated refund constraint'); END"
                )
            )
    with pytest.raises(IntegrityError):
        await database.service.fail_and_refund(task["id"], "failure")
    async with database.factory.create() as uow:
        assert (await uow.generations.get(task["id"])).status == TaskStatus.queued
        assert (await uow.users.get(1)).balance == 0


async def test_concurrent_real_refunds_credit_once(database):
    if database.backend != "postgres":
        pytest.skip("Concurrent row locks require PostgreSQL")
    task = await database.service.create(1, "image", "fox", None)
    await asyncio.gather(
        *(database.service.fail_and_refund(task["id"], "failure") for _ in range(2))
    )
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 1
        assert await uow.balance.has_external_key(f"refund:{task['id']}")


async def test_duplicate_refund_integrity_error_checks_existing_key(
    database, monkeypatch
):
    task = await database.service.create(1, "image", "fox", None)
    key = f"refund:{task['id']}"
    async with database.factory.create() as uow:
        assert await uow.balance.refund_once(1, task["id"], 1, key)
    original = BalanceRepository.has_external_key
    count = 0

    async def missed_read(self, external_key):
        nonlocal count
        count += 1
        return False if count == 1 else await original(self, external_key)

    monkeypatch.setattr(BalanceRepository, "has_external_key", missed_read)
    async with database.factory.create() as uow:
        assert not await uow.balance.refund_once(1, task["id"], 1, key)
    assert count == 2
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 1


@pytest.mark.parametrize("after_commit", [False, True])
async def test_refund_retry_after_unknown_commit(database, monkeypatch, after_commit):
    task = await database.service.create(1, "image", "fox", None)
    original = UnitOfWork.__aexit__

    async def lost_ack(self, *args):
        if after_commit:
            await original(self, *args)
        else:
            await self.session.rollback()
        raise RuntimeError("commit acknowledgement lost")

    monkeypatch.setattr(UnitOfWork, "__aexit__", lost_ack)
    with pytest.raises(RuntimeError, match="commit acknowledgement"):
        await database.service.fail_and_refund(task["id"], "failure")
    monkeypatch.setattr(UnitOfWork, "__aexit__", original)
    await database.service.fail_and_refund(task["id"], "failure")
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 1
        assert (await uow.generations.get(task["id"])).status == TaskStatus.failed


@pytest.mark.parametrize(
    "kind,image,model",
    [
        ("image", None, "fal-ai/flux/dev"),
        ("image", "https://input.test/x.png", "fal-ai/flux/dev/image-to-image"),
        ("video", None, "fal-ai/wan-25-preview/text-to-video"),
        ("video", "https://input.test/x.png", "fal-ai/wan-25-preview/image-to-video"),
    ],
)
async def test_all_four_fal_modes_use_matching_endpoint(
    monkeypatch, kind, image, model
):
    monkeypatch.setattr(settings, "fal_api_key", "offline-only")
    calls = []

    def handle(request):
        calls.append(request)
        if request.method == "POST":
            assert request.url.path == "/" + model
            body = json.loads(request.content)
            assert body == {"prompt": "fox", **({"image_url": image} if image else {})}
            return httpx.Response(
                200,
                json={
                    "request_id": "job",
                    "status_url": "https://queue.fal.run/status",
                    "response_url": "https://queue.fal.run/result",
                },
            )
        if request.url.path == "/status":
            return httpx.Response(200, json={"status": "COMPLETED"})
        media = {"url": "https://result.test/ready"}
        return httpx.Response(
            200, json={"images": [media]} if kind == "image" else {"video": media}
        )

    provider = FalAIProvider(
        lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(handle), **kw)
    )
    result = await provider.generate(kind, "fox", image, on_submitted=AsyncMock())
    assert result["url"] == "https://result.test/ready"
    assert sum(request.method == "POST" for request in calls) == 1


async def test_fallback_checkpoint_resumes_without_primary(monkeypatch):
    monkeypatch.setattr(settings, "fal_api_key", "offline-only")
    checkpoint = {}
    primary = SimpleNamespace(
        name="fal", generate=AsyncMock(side_effect=RetryableProviderError())
    )
    calls = []

    def handle(request):
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "request_id": "fallback-job",
                    "status_url": "https://queue.fal.run/status",
                    "response_url": "https://queue.fal.run/result",
                },
            )
        if request.url.path == "/status":
            if calls.count("GET") == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(200, json={"images": [{"url": "https://result.test/x"}]})

    async def save(context):
        checkpoint.update(context)

    fallback = FalAIProvider(
        lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(handle), **kw),
        name="fal-fallback",
    )
    manager = ProviderManager(primary, fallback)
    with pytest.raises(RetryableProviderError):
        await manager.generate("image", "fox", None, on_submitted=save)
    assert checkpoint["provider"] == "fal-fallback"
    result = await manager.generate(
        "image", "fox", None, "fallback-job", provider_context=checkpoint
    )
    assert result["provider"] == "fal-fallback"
    primary.generate.assert_awaited_once()
    assert calls.count("POST") == 1


@pytest.mark.parametrize("path", ["/balance", "/generations/any-id"])
async def test_read_endpoints_enforce_rate_limit(monkeypatch, path):
    overrides = app.dependency_overrides.copy()
    app.dependency_overrides[current_user] = lambda: SimpleNamespace(id=1)
    limiter = AsyncMock(side_effect=PermissionError("Rate limit exceeded"))
    monkeypatch.setattr(dependencies.rate_limit_service, "check", limiter)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            response = await client.get(path)
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "60"
        limiter.assert_awaited_once_with(1)
    finally:
        app.dependency_overrides = overrides


async def test_enqueue_deadline_retains_one_reservation(database, monkeypatch):
    overrides = app.dependency_overrides.copy()
    app.dependency_overrides[current_user] = lambda: SimpleNamespace(id=1)
    app.dependency_overrides[rate_limit] = lambda: None
    monkeypatch.setattr(routes, "generation_service", database.service)
    monkeypatch.setattr(settings, "enqueue_timeout_seconds", 0.02)
    cancelled = asyncio.Event()

    async def hang(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    pool = AsyncMock(side_effect=hang)
    monkeypatch.setattr(routes, "create_pool", pool)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            first = await asyncio.wait_for(
                client.post(
                    "/generations/images",
                    json={"prompt": "fox"},
                    headers={"Idempotency-Key": "timeout"},
                ),
                1,
            )
            assert first.status_code == 503
            assert cancelled.is_set()
            second = await client.post(
                "/generations/images",
                json={"prompt": "fox"},
                headers={"Idempotency-Key": "timeout"},
            )
            assert second.status_code == 202
        pool.assert_awaited_once()
        async with database.factory.create() as uow:
            assert (await uow.users.get(1)).balance == 0
            assert (await uow.publications.get(second.json()["id"])).status == "unknown"
    finally:
        app.dependency_overrides = overrides


async def test_global_deadline_cancels_stuck_auth(monkeypatch):
    monkeypatch.setattr(settings, "api_timeout_seconds", 0.02)
    cancelled = asyncio.Event()

    async def hang():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(routes.user_service, "create", hang)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await asyncio.wait_for(client.post("/auth"), 1)
    assert response.status_code == 504
    assert cancelled.is_set()
