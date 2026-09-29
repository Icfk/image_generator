import pytest

from app.application.providers import CircuitBreaker, ProviderManager


class Failing:
    name = "primary"

    async def generate(self, *args):
        raise RuntimeError("boom")


class Working:
    name = "fallback"

    async def generate(self, *args):
        return {"provider": self.name}


@pytest.mark.asyncio
async def test_provider_manager_fails_over_and_opens():
    manager = ProviderManager(Failing(), Working(), CircuitBreaker(2, 60))
    assert (await manager.generate("image", "x", None))["provider"] == "fallback"
    assert manager.breaker.state == "closed"
    assert (await manager.generate("image", "x", None))["provider"] == "fallback"
    assert manager.breaker.state == "open"


@pytest.mark.asyncio
async def test_circuit_breaker_half_open(monkeypatch):
    breaker = CircuitBreaker(1, 10)
    await breaker.failure()
    assert breaker.state == "open"
    monkeypatch.setattr(
        "app.application.providers.time.monotonic", lambda: breaker.opened_at + 11
    )
    assert await breaker.allow() is True
    assert breaker.state == "half-open"
