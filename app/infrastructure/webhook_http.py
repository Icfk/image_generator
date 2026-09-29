"""Public-only HTTP transport with DNS validation and connection pinning."""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from app.domain.webhooks import validate_public_address, validate_webhook_url


async def resolve_addresses(host: str) -> list[str]:
    """Resolve once; callers pin one of the vetted addresses for the request."""
    try:
        answers = await asyncio.get_running_loop().getaddrinfo(
            host, 443, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
        )
    except OSError as exc:
        raise httpx.ConnectError("Webhook DNS resolution failed") from exc
    return list(dict.fromkeys(answer[4][0] for answer in answers))


class SafeWebhookClient:
    """Keep original TLS identity while connecting only to a validated IP."""

    def __init__(
        self,
        *,
        timeout: float = 5,
        resolver: Callable[[str], Awaitable[list[str]]] = resolve_addresses,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )
        self.resolver = resolver
        self.destination: httpx.URL | None = None
        self.address: str | None = None

    async def __aenter__(self) -> "SafeWebhookClient":
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.client.__aexit__(*args)

    async def prepare(self, url: str) -> None:
        """Validate every DNS answer before granting permission to send HTTP."""
        validate_webhook_url(url)
        destination = httpx.URL(url)
        try:
            ipaddress.ip_address(destination.host)
        except ValueError:
            addresses = await self.resolver(destination.host)
        else:
            addresses = [destination.host]
        if not addresses:
            raise httpx.ConnectError("Webhook DNS returned no addresses")
        for address in addresses:
            validate_public_address(address)
        self.destination = destination
        self.address = addresses[0]

    async def post(
        self, url: str, *, json: Any, headers: dict[str, str]
    ) -> httpx.Response:
        """Never resolve the original hostname again or follow a redirect."""
        destination = self.destination
        if destination is None or self.address is None or httpx.URL(url) != destination:
            raise RuntimeError("Webhook destination has not been prepared")
        return await self.client.post(
            destination.copy_with(host=self.address),
            json=json,
            headers={**headers, "Host": destination.netloc.decode("ascii")},
            extensions={"sni_hostname": destination.host},
        )
