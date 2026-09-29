"""Security regressions; every outbound HTTP request uses a mock transport."""

from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import ValidationError

from app.domain.webhooks import UnsafeWebhookURL
from app.infrastructure.config import Settings
from app.infrastructure.webhook_http import SafeWebhookClient
from app.presentation.schemas import GenerateRequest


@pytest.mark.parametrize("environment", ["production", "development"])
def test_real_provider_requires_signed_payments(environment: str) -> None:
    with pytest.raises(ValidationError, match="signatures are required"):
        Settings(
            app_env=environment,
            generation_provider="fal",
            require_webhook_signature=False,
            auth_secret="s" * 32,
        )


@pytest.mark.parametrize("secret", ["", "change-me", "secret", " " * 32])
def test_production_rejects_weak_secret(secret: str) -> None:
    with pytest.raises(ValidationError, match="AUTH_SECRET"):
        Settings(
            app_env="production", auth_secret=secret, require_webhook_signature=True
        )


def test_explicit_development_retains_free_demo() -> None:
    config = Settings(
        app_env="development",
        generation_provider="mock",
        require_webhook_signature=False,
    )
    assert not config.require_webhook_signature
    config = Settings(
        app_env="production", auth_secret="s" * 32, require_webhook_signature=True
    )
    assert config.require_webhook_signature


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/hook",
        "https://127.0.0.1/hook",
        "https://169.254.169.254/",
        "https://10.0.0.1/",
        "https://[::1]/",
        "https://[::ffff:8.8.8.8]/",
        "https://[64:ff9b::7f00:1]/",
        "https://224.0.0.1/",
        "https://localhost/",
        "https://service.internal/",
        "https://example.com:8443/",
        "https://user:secret@example.com/",
        "https://example.com/#secret",
        "https://example.com/\n",
        "https://example.com\\@127.0.0.1/",
    ],
)
def test_generate_request_rejects_unsafe_webhook(url: str) -> None:
    with pytest.raises(ValidationError):
        GenerateRequest(prompt="fox", webhook_url=url)


async def test_service_rejects_unsafe_url_before_reservation(database) -> None:
    with pytest.raises(UnsafeWebhookURL):
        await database.service.create_request(
            1, "image", "fox", None, "bad-hook", "http://127.0.0.1/"
        )
    async with database.factory.create() as uow:
        assert (await uow.users.get(1)).balance == 1


@pytest.mark.parametrize(
    "addresses",
    [
        ["127.0.0.1"],
        ["169.254.169.254"],
        ["8.8.8.8", "10.0.0.1"],
        ["::1"],
        ["fc00::1"],
        ["ff02::1"],
    ],
)
async def test_private_dns_answers_never_reach_transport(addresses: list[str]) -> None:
    handler = AsyncMock(return_value=httpx.Response(200))
    resolver = AsyncMock(return_value=addresses)
    async with SafeWebhookClient(
        resolver=resolver, transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(UnsafeWebhookURL):
            await client.prepare("https://customer.test/hook")
    handler.assert_not_called()


async def test_pinned_ip_preserves_tls_identity_and_does_not_follow_redirect() -> None:
    requests = []
    resolver = AsyncMock(side_effect=[["8.8.8.8"], ["127.0.0.1"]])

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    async with SafeWebhookClient(
        resolver=resolver, transport=httpx.MockTransport(handle)
    ) as client:
        await client.prepare("https://customer.test/hook")
        response = await client.post(
            "https://customer.test/hook",
            json={"id": "x"},
            headers={"Idempotency-Key": "x"},
        )
    assert response.status_code == 302
    resolver.assert_awaited_once_with("customer.test")
    assert len(requests) == 1
    assert requests[0].url.host == "8.8.8.8"
    assert requests[0].headers["Host"] == "customer.test"
    assert requests[0].extensions["sni_hostname"] == "customer.test"


async def test_unprepared_transport_cannot_send() -> None:
    handler = AsyncMock(return_value=httpx.Response(200))
    async with SafeWebhookClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="not been prepared"):
            await client.post("https://customer.test/hook", json={}, headers={})
    handler.assert_not_called()
