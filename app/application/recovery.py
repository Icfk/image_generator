"""Idempotent repair of durable work after worker, Redis or process crashes."""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from app.domain.entities import TaskStatus
from app.domain.ports import UnitOfWorkFactory
from app.domain.recovery import RecoveryQueue
from app.infrastructure.config import settings
from app.infrastructure.metrics import RECOVERY_ACTIONS


def utc_now() -> datetime:
    """Return recovery wall clock; tests inject a controlled clock."""
    return datetime.now(UTC)


def due(value: datetime | None, now: datetime) -> bool:
    """Treat SQLite's naive timestamps as UTC, matching PostgreSQL timestamps."""
    return value is None or value.replace(tzinfo=UTC) <= now


def replacement_id(previous: str) -> str:
    """Derive a stable successor even if enqueue succeeds but DB commit is lost."""
    return f"recovery:{uuid.uuid5(uuid.NAMESPACE_URL, previous)}"


class RecoveryService:
    """Coordinate row-locked repairs; no provider calls are made by recovery."""

    def __init__(
        self, uow_factory: UnitOfWorkFactory, clock: Callable[[], datetime] = utc_now
    ) -> None:
        self.uow_factory = uow_factory.for_application()
        self.clock = clock

    async def run(self, queue: RecoveryQueue) -> bool:
        """Run a bounded sweep; return false if any queue operation was uncertain."""
        now = self.clock()
        cutoff = now - timedelta(seconds=settings.processing_timeout_seconds)
        async with self.uow_factory.create() as uow:
            preparations = await uow.deliveries.recover_preparations(
                now,
                settings.webhook_attempts,
                settings.webhook_interval_seconds,
                settings.recovery_batch_size,
            )
            candidates = await uow.generations.recovery_candidates(
                now, cutoff, settings.recovery_batch_size
            )
            terminal = await uow.generations.terminal_without_delivery(
                settings.recovery_batch_size
            )
        RECOVERY_ACTIONS.labels("delivery_preparation", "recovered").inc(preparations)
        healthy = True
        for task_id in candidates:
            healthy = await self.recover_generation(task_id, queue) and healthy
        for task_id in terminal:
            async with self.uow_factory.create() as uow:
                task = await uow.generations.lock(task_id, skip_locked=True)
                if (
                    task
                    and task.webhook_url
                    and task.status in (TaskStatus.completed, TaskStatus.failed)
                ):
                    await uow.deliveries.get_or_create(task.id, task.webhook_url)
            RECOVERY_ACTIONS.labels("terminal_delivery", "checked").inc()
        async with self.uow_factory.create() as uow:
            delivery_ids = await uow.deliveries.due_ids(
                now, settings.recovery_batch_size
            )
        for task_id in delivery_ids:
            healthy = await self.recover_delivery(task_id, queue) and healthy
        return healthy

    async def recover_generation(self, task_id: str, queue: RecoveryQueue) -> bool:
        """Lock one task across repair and bounded Redis publication."""
        now = self.clock()
        healthy = True
        action, outcome = "publication", "existing"
        async with self.uow_factory.create() as uow:
            task = await uow.generations.lock(task_id, skip_locked=True)
            if task is None or task.status not in (
                TaskStatus.queued,
                TaskStatus.processing,
            ):
                return True
            if task.status == TaskStatus.processing:
                cutoff = now - timedelta(seconds=settings.processing_timeout_seconds)
                if not due(task.processing_at, cutoff):
                    return True
                task.run_token = None
                if not task.provider_job_id:
                    # Submit may have succeeded without an acknowledgement.
                    # Never repeat it: compensate once within this transaction.
                    await uow.balance.refund_once(
                        task.user_id, task.id, task.cost, f"refund:{task.id}"
                    )
                    task.status = TaskStatus.failed
                    task.failed_at = now
                    task.error = (
                        "processing timeout without provider job id; no resubmit"
                    )
                    if task.webhook_url:
                        await uow.deliveries.get_or_create(task.id, task.webhook_url)
                    action, outcome = "processing_no_job", "failed_refunded"
                else:
                    task.status = TaskStatus.queued
                    task.processing_at = None
                    task.error = "recovery: resume provider polling"
                    publication = await uow.publications.get(task.id)
                    if publication is None:
                        publication = await uow.publications.create(task.id)
                    publication.job_id = replacement_id(publication.job_id)
                    publication.status = "pending"
                    publication.next_attempt_at = None
                    action = "resume_polling"
            if task.status == TaskStatus.queued:
                publication = await uow.publications.get(task.id)
                if publication is None:
                    publication = await uow.publications.create(task.id)
                if not due(publication.next_attempt_at, now):
                    return True
                publication.next_attempt_at = now + timedelta(
                    seconds=settings.recovery_interval_seconds
                )
                try:
                    state = await queue.status(publication.job_id)
                    if state == "complete":
                        publication.job_id = replacement_id(publication.job_id)
                    if state in ("not_found", "complete"):
                        publication.attempts += 1
                        await queue.enqueue(
                            "run_generation",
                            publication.job_id,
                            task.id,
                            task.webhook_url,
                        )
                        outcome = "published"
                    publication.status = "published"
                    publication.last_error = None
                except Exception as exc:
                    publication.status = "unknown"
                    publication.last_error = type(exc).__name__
                    outcome, healthy = "unknown", False
        RECOVERY_ACTIONS.labels(action, outcome).inc()
        return healthy

    async def recover_delivery(self, task_id: str, queue: RecoveryQueue) -> bool:
        """Publish one due delivery job without stealing a sending claim."""
        healthy = True
        outcome = "existing"
        async with self.uow_factory.create() as uow:
            row = await uow.deliveries.lock(task_id)
            if (
                row is None
                or row.status != "pending"
                or row.attempts >= min(settings.webhook_attempts, 5)
                or not due(row.next_attempt_at, self.clock())
            ):
                return True
            row.retry_job_id = row.retry_job_id or f"delivery:{task_id}:{row.attempts}"
            row.recovery_checked_at = self.clock()
            try:
                state = await queue.status(row.retry_job_id)
                if state == "complete":
                    row.retry_job_id = replacement_id(row.retry_job_id)
                if state in ("complete", "not_found"):
                    await queue.enqueue(
                        "retry_delivery", row.retry_job_id, task_id, row.url
                    )
                    outcome = "published"
            except Exception:
                outcome, healthy = "unknown", False
        RECOVERY_ACTIONS.labels("delivery", outcome).inc()
        return healthy
