import asyncio
from typing import Any

from arq import Retry, create_pool
from arq.connections import RedisSettings
from prometheus_client import start_http_server

from app.application.recovery import RecoveryService
from app.domain.entities import RetryableProviderError
from app.infrastructure.config import settings
from app.infrastructure.container import generation_service
from app.infrastructure.logging_setup import configure_logging
from app.infrastructure.recovery_queue import ArqRecoveryQueue

recovery_service = RecoveryService(generation_service.uow_factory)


async def startup(ctx: dict[str, Any]) -> None:
    """Expose this worker process's counters; do not scrape them through API."""
    configure_logging("worker")
    ctx["metrics_server"] = start_http_server(settings.worker_metrics_port)


async def shutdown(ctx: dict[str, Any]) -> None:
    """Stop the process-local exporter without blocking the ARQ event loop."""
    exporter = ctx.pop("metrics_server", None)
    if exporter is not None:
        server, thread = exporter
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        await asyncio.to_thread(thread.join, 5)


async def run_generation(ctx, task_id: str, webhook_url: str | None):
    payload = None
    try:
        payload = await generation_service.run(task_id, webhook_url)
    except RetryableProviderError:
        if ctx.get("job_try", 1) >= 3:
            await generation_service.fail_and_refund(
                task_id, "provider retries exhausted"
            )
            payload = await generation_service.get_payload(task_id)
        else:
            raise Retry(defer=5) from None
    if payload is None:
        payload = await generation_service.get_payload(task_id)
    if payload is None or payload["status"] not in ("completed", "failed"):
        return
    if webhook_url:
        await generation_service.webhook.deliver(task_id, webhook_url)
        await schedule_delivery_retry(task_id, webhook_url)


async def schedule_delivery_retry(task_id: str, webhook_url: str) -> None:
    """Only pending rows schedule work; all publishers use the same attempt ID."""
    row = await generation_service.webhook.get(task_id)
    if (
        row is None
        or row.status != "pending"
        or row.attempts >= min(settings.webhook_attempts, 5)
    ):
        return
    pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    try:
        await pool.enqueue_job(
            "retry_delivery",
            task_id,
            webhook_url,
            _defer_by=settings.webhook_interval_seconds,
            _job_id=f"delivery:{task_id}:{row.attempts}",
        )
    finally:
        await pool.aclose()


async def retry_delivery(
    ctx, task_id: str, webhook_url: str, payload: dict | None = None
):
    # Legacy payload arguments are accepted but never used as delivery state.
    row = await generation_service.webhook.get(task_id)
    if (
        not row
        or row.status != "pending"
        or row.attempts >= min(settings.webhook_attempts, 5)
    ):
        return
    await generation_service.webhook.deliver(task_id, webhook_url)
    await schedule_delivery_retry(task_id, webhook_url)


async def recover_publications(ctx):
    """Compatibility entry point; the dedicated recovery worker runs sweeps."""
    pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    try:
        await recovery_service.run(ArqRecoveryQueue(pool))
    finally:
        await pool.aclose()


class WorkerSettings:
    on_startup = startup
    on_shutdown = shutdown
    functions = [run_generation, retry_delivery, recover_publications]
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_tries = 3
    job_timeout = settings.provider_operation_timeout_seconds + 30
