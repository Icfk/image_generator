"""Bounded API series, process-local worker exporter, and atomic Redis limits."""

import asyncio
import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from redis.asyncio import Redis

from app import worker
from app.infrastructure.metrics import API_REQUESTS, GENERATIONS_SUCCESS
from app.infrastructure.rate_limit import CHECK_LIMIT, RateLimitRepository
from app.main import app


async def test_metric_labels_use_route_template_and_bounded_unknowns() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for number in range(30):
            assert (await client.get(f"/absent/{uuid.uuid4()}")).status_code == 404
            assert (await client.get(f"/generations/{uuid.uuid4()}")).status_code == 401
            await client.request(f"UNUSUAL{number}", f"/absent/{uuid.uuid4()}")
    samples = [
        sample
        for metric in API_REQUESTS.collect()
        for sample in metric.samples
        if sample.name == "api_requests_total"
    ]
    assert any(sample.labels["path"] == "/generations/{task_id}" for sample in samples)
    assert any(sample.labels["method"] == "OTHER" for sample in samples)
    assert all("/absent/" not in sample.labels["path"] for sample in samples)
    assert all(not sample.labels["method"].startswith("UNUSUAL") for sample in samples)


async def test_worker_exporter_lifecycle(monkeypatch) -> None:
    server, thread = Mock(), Mock()
    start = Mock(return_value=(server, thread))
    monkeypatch.setattr(worker, "start_http_server", start)
    ctx = {}
    await worker.WorkerSettings.on_startup(ctx)
    start.assert_called_once_with(worker.settings.worker_metrics_port)
    await worker.WorkerSettings.on_shutdown(ctx)
    server.shutdown.assert_called_once()
    server.server_close.assert_called_once()
    thread.join.assert_called_once_with(5)
    assert "metrics_server" not in ctx


async def test_rate_limit_is_one_atomic_operation() -> None:
    client = SimpleNamespace(eval=AsyncMock(return_value=1))
    assert await RateLimitRepository(client).check(42)
    client.eval.assert_awaited_once_with(
        CHECK_LIMIT, 2, "rate:user:42", "blocked:user:42", 10, 60
    )


async def test_real_worker_exporter_serves_generation_metrics(
    database, monkeypatch
) -> None:
    if not os.getenv("TEST_REDIS_URL"):
        pytest.skip("Set TEST_REDIS_URL to enable loopback network integration checks")
    monkeypatch.setattr(worker.settings, "worker_metrics_port", 0)
    monkeypatch.setattr(worker, "generation_service", database.service)
    ctx = {}
    counter = GENERATIONS_SUCCESS.labels("image")
    before = counter._value.get()
    await worker.startup(ctx)
    try:
        task = await database.service.create(1, "image", "fox", None)
        await worker.run_generation({}, task["id"], None)
        port = ctx["metrics_server"][0].server_port
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.get(f"http://127.0.0.1:{port}/metrics")
        assert response.status_code == 200
        assert (
            f'generations_success_total{{kind="image"}} {before + 1}' in response.text
        )
    finally:
        await worker.shutdown(ctx)


async def test_real_redis_atomic_limit_and_legacy_counter_repair() -> None:
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL for isolated Redis integration tests")
    client = Redis.from_url(url)
    identity = uuid.uuid4().int
    counter, blocked = f"rate:user:{identity}", f"blocked:user:{identity}"
    limiter = RateLimitRepository(client)
    try:
        results = await asyncio.gather(*(limiter.check(identity) for _ in range(30)))
        assert sum(results) == 10
        assert 0 < await client.ttl(counter) <= 60
        assert 0 < await client.ttl(blocked) <= 60
        await client.delete(counter, blocked)
        await client.set(counter, 1000)  # Legacy interrupted INCR/EXPIRE.
        assert await limiter.check(identity)
        assert await client.get(counter) == b"1"
        assert 0 < await client.ttl(counter) <= 60
        await client.set(blocked, 1)  # Repair legacy lock without expiry too.
        assert not await limiter.check(identity)
        assert 0 < await client.ttl(blocked) <= 60
    finally:
        await client.delete(counter, blocked)
        await client.aclose()
