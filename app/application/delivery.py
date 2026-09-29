"""Single-owner delivery attempts, backed by durable repository state."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.domain.ports import UnitOfWorkFactory
from app.domain.webhooks import UnsafeWebhookURL
from app.infrastructure.config import settings
from app.infrastructure.metrics import WEBHOOK_DELIVERY_ATTEMPTS
from app.infrastructure.webhook_http import SafeWebhookClient

log = logging.getLogger("content-service")
Payload = dict[str, Any]


def utc_now() -> datetime:
    """Return an aware timestamp; replace the clock in retry tests."""
    return datetime.now(UTC)


class WebhookDelivery:
    """Claim each attempt before HTTP; never resend terminal or sending rows."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        payload_loader: Callable[[str], Awaitable[Payload | None]] | None = None,
        *,
        clock: Callable[[], datetime] = utc_now,
        client_factory: Callable[..., Any] = SafeWebhookClient,
    ) -> None:
        self.uow_factory = uow_factory.for_application()
        self.payload_loader = payload_loader
        self.clock = clock
        self.client_factory = client_factory

    async def get(self, generation_id: str) -> Any:
        """Read durable delivery state without acquiring permission to send."""
        async with self.uow_factory.create() as uow:
            return await uow.deliveries.get_by_generation(generation_id)

    async def get_payload(self, generation_id: str) -> Payload | None:
        """Load current generation state, never a payload from a Redis job."""
        if self.payload_loader is not None:
            return await self.payload_loader(generation_id)
        async with self.uow_factory.create() as uow:
            generation = await uow.generations.get(generation_id)
            if generation is None:
                return None
            return {
                "id": generation.id,
                "status": generation.status.value,
                "result": generation.result,
                "error": generation.error,
            }

    async def deliver(
        self, generation_id: str, url: str, payload: Payload | None = None
    ) -> tuple[bool, int]:
        """Send one claimed attempt, ignoring legacy caller-supplied payloads.

        A lost acknowledgement or cancelled process leaves sending intact.
        Automatically stealing that lock could overlap an in-flight request.
        """
        max_attempts = min(settings.webhook_attempts, 5)
        async with self.uow_factory.create() as uow:
            await uow.deliveries.get_or_create(generation_id, url)
        async with self.uow_factory.create() as uow:
            claim = await uow.deliveries.claim_attempt(
                generation_id,
                self.clock(),
                max_attempts,
                settings.webhook_preparation_timeout_seconds,
            )
        if claim is None:
            row = await self.get(generation_id)
            return bool(row and row.status == "delivered"), row.attempts if row else 0

        success = False
        send_started = False
        permanent = False
        error: str | None = "Generation payload unavailable"
        try:
            async with self.client_factory(timeout=5) as client:
                async with asyncio.timeout(
                    settings.webhook_preparation_timeout_seconds
                ):
                    fresh_payload = await self.get_payload(generation_id)
                    if fresh_payload is None:
                        await self._finish(generation_id, claim.attempts, False, error)
                        return False, claim.attempts
                    prepare = getattr(client, "prepare", None)
                    if prepare is not None:
                        await prepare(claim.url)
                    async with self.uow_factory.create() as uow:
                        allowed = await uow.deliveries.begin_sending(
                            generation_id, claim.attempts, self.clock()
                        )
                    if not allowed:
                        return False, claim.attempts
                # Commit permission before any HTTP bytes can leave the process.
                send_started = True
                async with asyncio.timeout(5):
                    response = await client.post(
                        claim.url,
                        json=fresh_payload,
                        headers={
                            "Idempotency-Key": generation_id,
                            "X-Generation-ID": generation_id,
                        },
                    )
                success = response.is_success
                error = None if success else f"HTTP {response.status_code}"
        except asyncio.CancelledError:
            if not send_started:
                await asyncio.shield(
                    self._finish(
                        generation_id, claim.attempts, False, "Preparation cancelled"
                    )
                )
            raise
        except Exception as exc:
            # Persist preparation errors too; never retain URLs/secrets in errors.
            error = type(exc).__name__
            permanent = isinstance(exc, UnsafeWebhookURL)
        await self._finish(generation_id, claim.attempts, success, error, permanent)
        return success, claim.attempts

    async def _finish(
        self,
        generation_id: str,
        attempt: int,
        success: bool,
        error: str | None,
        permanent: bool = False,
    ) -> None:
        """Commit one owned outcome; recovery repairs lost pre-HTTP commits."""
        status = "delivered" if success else "pending"
        if not success and (permanent or attempt >= settings.webhook_attempts):
            status = "failed"
        next_attempt_at = (
            self.clock() + timedelta(seconds=settings.webhook_interval_seconds)
            if status == "pending"
            else None
        )
        async with self.uow_factory.create() as uow:
            await uow.deliveries.finish_attempt(
                generation_id, attempt, status, error, next_attempt_at
            )
        if error is not None:
            log.warning(
                "Webhook attempt failed generation=%s attempt=%s last_error=%s",
                generation_id,
                attempt,
                error,
            )
        WEBHOOK_DELIVERY_ATTEMPTS.labels("success" if success else "failed").inc()
