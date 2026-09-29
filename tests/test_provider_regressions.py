"""Fail closed on provider outages; half-open claims must always be released."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from app.application.providers import (
    CircuitBreaker,
    FalAIProvider,
    MockProvider,
    ProviderManager,
    build_provider,
)
from app.domain.entities import (
    LostGenerationClaim,
    RetryableProviderError,
    SubmissionUncertainError,
)
from app.infrastructure.config import settings
from app.infrastructure.metrics import PROVIDER_ERRORS, PROVIDER_FAILOVER


async def test_fal_rate_limit_cannot_succeed_via_mock(monkeypatch, database) -> None:
    monkeypatch.setattr(settings, "generation_provider", "fal")
    monkeypatch.setattr(settings, "fal_api_key", "offline-key")
    manager = build_provider()
    assert isinstance(manager.primary, FalAIProvider)
    assert isinstance(manager.fallback, FalAIProvider)
    manager.primary.client_factory = lambda **kw: httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(429)), **kw
    )
    manager.fallback.client_factory = manager.primary.client_factory
    database.service.provider = manager
    task = await database.service.create(1, "image", "fox", None)
    with pytest.raises(RetryableProviderError):
        await database.service.run(task["id"], None)
    payload = await database.service.get_payload(task["id"])
    assert payload["status"] == "queued"
    assert payload["result"] is None
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 0


def test_real_to_mock_fallback_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be a fallback"):
        ProviderManager(FalAIProvider(), MockProvider())


@pytest.mark.parametrize(
    "error_type",
    [
        SubmissionUncertainError,
        LostGenerationClaim,
        asyncio.CancelledError,
    ],
)
async def test_half_open_exception_releases_probe(monkeypatch, error_type) -> None:
    now = [100.0]
    monkeypatch.setattr("app.application.providers.time.monotonic", lambda: now[0])
    breaker = CircuitBreaker(1, 10)
    await breaker.failure()
    now[0] += 11
    primary = SimpleNamespace(
        name="probe", generate=AsyncMock(side_effect=error_type())
    )
    fallback = SimpleNamespace(name="fallback", generate=AsyncMock())
    manager = ProviderManager(primary, fallback, breaker)
    with pytest.raises(error_type):
        await manager.generate("image", "fox", None)
    assert breaker.state == "open"
    assert not breaker.probe_in_flight
    fallback.generate.assert_not_called()
    now[0] += 11
    primary.generate.side_effect = None
    primary.generate.return_value = {"url": "https://result.test/ready"}
    assert (await manager.generate("image", "fox", None))["url"]
    assert breaker.state == "closed"


async def test_only_one_concurrent_half_open_probe(monkeypatch) -> None:
    breaker = CircuitBreaker(1, 10)
    await breaker.failure()
    monkeypatch.setattr(
        "app.application.providers.time.monotonic", lambda: breaker.opened_at + 11
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def generate(*args) -> dict:
        entered.set()
        await release.wait()
        return {"url": "https://result.test/ready"}

    primary = SimpleNamespace(name="probe", generate=AsyncMock(side_effect=generate))
    manager = ProviderManager(primary, breaker=breaker)
    owner = asyncio.create_task(manager.generate("image", "fox", None))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(RetryableProviderError, match="circuit is open"):
            await manager.generate("image", "fox", None)
        primary.generate.assert_awaited_once()
    finally:
        release.set()
        await owner
    assert breaker.state == "closed"


async def test_fallback_error_is_counted_and_logged(caplog) -> None:
    primary = SimpleNamespace(
        name="broken-primary", generate=AsyncMock(side_effect=RuntimeError())
    )
    fallback = SimpleNamespace(
        name="broken-fallback", generate=AsyncMock(side_effect=RuntimeError("secret"))
    )
    errors = PROVIDER_ERRORS.labels(fallback.name, "image")
    failovers = PROVIDER_FAILOVER.labels(primary.name, fallback.name)
    before = errors._value.get(), failovers._value.get()
    with pytest.raises(RuntimeError):
        await ProviderManager(primary, fallback).generate("image", "fox", None)
    fallback.generate.assert_awaited_once()
    assert (errors._value.get(), failovers._value.get()) == (
        before[0] + 1,
        before[1] + 1,
    )
    assert "Fallback provider failed" in caplog.text
    assert "secret" not in caplog.text
