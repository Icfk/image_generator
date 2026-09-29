"""HTTP mock coverage of checkpoints and polling after a process restart."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from app.application.providers import FalAIProvider, MockProvider, ProviderManager
from app.domain.entities import RetryableProviderError, SubmissionUncertainError
from app.infrastructure.config import settings
from app.infrastructure.recovery_health import RecoveryHeartbeat

STATUS = "https://queue.fal.run/model/requests/remote/status"
RESULT = "https://queue.fal.run/model/requests/remote/response"


async def test_fal_checkpoint_before_poll_and_resume_completed_result(
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "fal_api_key", "mock-key-never-sent-to-network")
    calls = []
    checkpoint = {}
    crash = True

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal crash
        calls.append((request.method, str(request.url)))
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "request_id": "remote",
                    "status_url": STATUS,
                    "response_url": RESULT,
                },
            )
        assert checkpoint["request_id"] == "remote"
        if crash:
            crash = False
            raise asyncio.CancelledError()
        if str(request.url) == STATUS:
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(
            200, json={"images": [{"url": "https://result.test/ready"}]}
        )

    def factory(**kwargs):
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)

    async def save(context):
        checkpoint.update(context)

    provider = FalAIProvider(factory, sleep=AsyncMock())
    with pytest.raises(asyncio.CancelledError):
        await provider.generate("image", "fox", None, on_submitted=save)
    # A fresh process has only the persisted checkpoint, not any local variables.
    resumed = FalAIProvider(factory, sleep=AsyncMock())
    result = await resumed.generate(
        "image", "fox", None, "remote", provider_context=checkpoint
    )
    assert result["url"] == "https://result.test/ready"
    assert sum(method == "POST" for method, _ in calls) == 1
    assert calls[-2:] == [("GET", STATUS), ("GET", RESULT)]


async def test_unknown_submit_does_not_retry_or_fall_back(monkeypatch) -> None:
    monkeypatch.setattr(settings, "fal_api_key", "test-key")
    calls = []

    async def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("lost acknowledgement")

    provider = FalAIProvider(
        lambda **kwargs: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), **kwargs
        )
    )
    fallback = SimpleNamespace(name="fallback", generate=AsyncMock())
    manager = ProviderManager(provider, fallback)
    with pytest.raises(SubmissionUncertainError):
        await manager.generate("image", "fox", None, on_submitted=AsyncMock())
    assert len(calls) == 1
    fallback.generate.assert_not_called()


async def test_retryable_poll_never_switches_provider(monkeypatch) -> None:
    monkeypatch.setattr(settings, "fal_api_key", "test-key")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503)

    primary = FalAIProvider(
        lambda **kwargs: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), **kwargs
        )
    )
    fallback = SimpleNamespace(name="fallback", generate=AsyncMock())
    manager = ProviderManager(primary, fallback)
    with pytest.raises(RetryableProviderError):
        await manager.generate("image", "fox", None, "saved-job")
    assert [request.method for request in calls] == ["GET"]
    fallback.generate.assert_not_called()


async def test_default_mock_manager_accepts_checkpoint_contract() -> None:
    provider = ProviderManager(MockProvider(), MockProvider())
    result = await provider.generate(
        "image", "fox", None, None, on_submitted=AsyncMock(), provider_context=None
    )
    assert result["provider"] == "mock"


async def test_recovery_heartbeat_expiry_and_identity() -> None:
    now = [1000.0]
    redis = SimpleNamespace(set=AsyncMock(), get=AsyncMock(return_value=None))
    heartbeat = RecoveryHeartbeat(redis, clock=lambda: now[0], identity="worker-a")
    assert not await heartbeat.healthy()
    await heartbeat.beat()
    redis.set.assert_awaited_once_with("recovery:heartbeat:worker-a", "1000.0", ex=90)
    redis.get.return_value = b"1000.0"
    assert await heartbeat.healthy()
    now[0] = 1091
    assert not await heartbeat.healthy()
