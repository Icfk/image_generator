"""Bound API execution; ambiguous mutations are retried with idempotency keys."""

import asyncio

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.infrastructure.config import settings


class ApiDeadlineMiddleware:
    """Cancel timed-out API handlers before returning a retryable response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path", "").startswith("/static/"):
            await self.app(scope, receive, send)
            return
        started = False

        async def track_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            async with asyncio.timeout(settings.api_timeout_seconds):
                await self.app(scope, receive, track_send)
        except TimeoutError:
            if started:
                raise
            await JSONResponse(
                {"detail": "Request timed out; retry with the same idempotency key"},
                status_code=504,
            )(scope, receive, send)
