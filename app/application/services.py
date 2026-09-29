import hashlib
import logging
import time
import uuid
from datetime import UTC, datetime

from app.application.delivery import WebhookDelivery
from app.application.providers import build_provider
from app.domain.entities import (
    LostGenerationClaim,
    PermanentProviderError,
    RetryableProviderError,
    SubmissionUncertainError,
    TaskStatus,
)
from app.domain.generation import GenerationCreation
from app.domain.ports import RateLimiter, UnitOfWorkFactory
from app.domain.webhooks import validate_webhook_url
from app.infrastructure.metrics import (
    GENERATION_COST,
    GENERATION_DURATION,
    GENERATIONS,
    GENERATIONS_FAILED,
    GENERATIONS_SUCCESS,
)

log = logging.getLogger("content-service")


class UserService:
    def __init__(self, uow_factory: UnitOfWorkFactory):
        self.uow_factory = uow_factory.for_application()

    async def create(self):
        external_id, api_key = str(uuid.uuid4()), uuid.uuid4().hex
        async with self.uow_factory.create() as uow:
            await uow.users.create(
                external_id, hashlib.sha256(api_key.encode()).hexdigest()
            )
        return {"external_user_id": external_id, "api_key": api_key}

    async def top_up(
        self, external_id: str, amount: int, idempotency_key: str | None
    ) -> dict[str, str | int | bool]:
        """Apply an externally identified payment; a retry never credits twice."""
        if not idempotency_key or not idempotency_key.strip():
            raise ValueError("transaction_id or idempotency_key is required")
        if len(idempotency_key) > 192:
            raise ValueError("Payment key must not exceed 192 characters")
        if amount <= 0 or amount > 1_000_000:
            raise ValueError("Invalid payment amount")
        async with self.uow_factory.create() as uow:
            receipt = await uow.apply_payment(external_id, amount, idempotency_key)
        return receipt.response()


class GenerationService:
    def __init__(self, uow_factory: UnitOfWorkFactory, provider=None, webhook=None):
        self.uow_factory = uow_factory.for_application()
        self.provider = provider or build_provider()
        self.webhook = webhook or WebhookDelivery(uow_factory, self.get_payload)

    async def get_payload(self, task_id: str) -> dict | None:
        task = await self.get(task_id)
        if task is None:
            return None
        return {
            "id": task.id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
        }

    async def get(self, task_id: str):
        async with self.uow_factory.create() as uow:
            return await uow.generations.get(task_id)

    async def get_for_user(self, task_id: str, user_id: int):
        async with self.uow_factory.create() as uow:
            return await uow.generations.get_for_user(task_id, user_id)

    async def create_request(
        self,
        user_id: int,
        kind: str,
        prompt: str,
        image_url: str | None,
        idempotency_key: str | None = None,
        webhook_url: str | None = None,
    ) -> GenerationCreation:
        """Commit the reservation once and report whether publication is needed."""
        if webhook_url is not None:
            validate_webhook_url(webhook_url)
        if idempotency_key is not None and not 1 <= len(idempotency_key) <= 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        async with self.uow_factory.create() as uow:
            return await uow.create_reserved_generation(
                user_id, kind, prompt, image_url, idempotency_key, webhook_url
            )

    async def create(
        self,
        user_id: int,
        kind: str,
        prompt: str,
        image_url: str | None,
        idempotency_key: str | None = None,
    ) -> dict[str, str]:
        """Preserve the public service response for existing callers."""
        creation = await self.create_request(
            user_id, kind, prompt, image_url, idempotency_key
        )
        return creation.response()

    async def mark_publication(
        self, task_id: str, status: str, error: str | None = None
    ):
        async with self.uow_factory.create() as uow:
            await uow.publications.record_attempt(task_id, status, error)

    async def compensate_enqueue_failure(self, task_id):
        async with self.uow_factory.create() as uow:
            task = await uow.generations.get(task_id)
            if not task or task.status != TaskStatus.queued:
                return
            key = f"refund:enqueue:{task.id}"
            if not await uow.balance.has_external_key(key):
                task.status = TaskStatus.failed
                task.failed_at = datetime.now(UTC)
                task.error = "Unable to enqueue worker job"
                await uow.balance.refund_once(task.user_id, task.id, task.cost, key)

    async def fail_and_refund(self, task_id, error):
        async with self.uow_factory.create() as uow:
            task = await uow.generations.lock(task_id)
            if not task or task.status != TaskStatus.queued:
                return
            key = f"refund:{task.id}"
            await uow.balance.refund_once(task.user_id, task.id, task.cost, key)
            task.status, task.error, task.failed_at = (
                TaskStatus.failed,
                error,
                datetime.now(UTC),
            )
            if task.webhook_url:
                await uow.deliveries.get_or_create(task.id, task.webhook_url)

    async def run(self, task_id: str, webhook_url: str | None) -> dict | None:
        """Persist claim/submission/result separately; fence stale workers."""
        token = str(uuid.uuid4())
        async with self.uow_factory.create() as uow:
            task = await uow.generations.claim(task_id, token, datetime.now(UTC))
            if task is None:
                return None
            if task.webhook_url is None and webhook_url:
                task.webhook_url = webhook_url
        started = time.monotonic()
        GENERATIONS.labels(task.kind, "processing").inc()

        async def checkpoint(context: dict) -> None:
            try:
                async with self.uow_factory.create() as uow:
                    saved = await uow.generations.checkpoint(task_id, token, context)
            except Exception as exc:
                raise SubmissionUncertainError("Provider checkpoint failed") from exc
            if not saved:
                raise LostGenerationClaim(task_id)

        retry_error = None
        result = None
        error = None
        try:
            result = await self.provider.generate(
                task.kind,
                task.prompt,
                task.image_url,
                task.provider_job_id,
                on_submitted=checkpoint,
                provider_context=task.provider_context,
            )
            if not result or not result.get("url"):
                raise PermanentProviderError("Provider returned no result URL")
        except LostGenerationClaim:
            return None
        except SubmissionUncertainError:
            # Keep the committed processing state. Recovery's explicit no-ID
            # policy handles the uncertainty without another paid submission.
            return None
        except RetryableProviderError:
            retry_error = RetryableProviderError("temporary provider error; retrying")
            error = str(retry_error)
        except PermanentProviderError as exc:
            error = str(exc)
        finally:
            GENERATION_DURATION.labels(task.kind).observe(time.monotonic() - started)

        async with self.uow_factory.create() as uow:
            current = await uow.generations.lock(task_id)
            if (
                current is None
                or current.status != TaskStatus.processing
                or current.run_token != token
            ):
                return None
            current.run_token = None
            current.error = error
            if retry_error:
                current.status = TaskStatus.queued
                current.processing_at = None
            elif error:
                await uow.balance.refund_once(
                    current.user_id, task_id, current.cost, f"refund:{task_id}"
                )
                current.status = TaskStatus.failed
                current.failed_at = datetime.now(UTC)
            else:
                current.result = result
                current.status = TaskStatus.completed
                current.completed_at = datetime.now(UTC)
            if current.status in (TaskStatus.completed, TaskStatus.failed):
                if current.webhook_url:
                    await uow.deliveries.get_or_create(task_id, current.webhook_url)
            payload = {
                "id": task_id,
                "status": current.status.value,
                "result": current.result,
                "error": current.error,
            }
        if retry_error:
            raise retry_error
        GENERATIONS.labels(task.kind, payload["status"]).inc()
        if payload["status"] == "completed":
            GENERATIONS_SUCCESS.labels(task.kind).inc()
            GENERATION_COST.labels(task.kind).inc(task.cost)
        else:
            GENERATIONS_FAILED.labels(task.kind).inc()
        return payload


class AuthService:
    """Application service for API-key authentication."""

    def __init__(self, uow_factory: UnitOfWorkFactory):
        self.uow_factory = uow_factory.for_application()

    async def authenticate(self, api_key: str):
        async with self.uow_factory.create() as uow:
            return await uow.users.by_api_key(api_key)


class RateLimitService:
    """Application service for per-user request throttling."""

    def __init__(self, repository: RateLimiter):
        self.repository = repository

    async def check(self, user_id: int) -> None:
        if not await self.repository.check(user_id):
            raise PermissionError("Rate limit exceeded; retry in 60 seconds")
