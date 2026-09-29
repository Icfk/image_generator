"""Dedicated periodic recovery process and Docker healthcheck entry point."""

import argparse
import asyncio
import logging

from arq import create_pool
from arq.connections import RedisSettings
from prometheus_client import start_http_server

from app.application.recovery import RecoveryService
from app.domain.recovery import RecoveryQueue
from app.infrastructure.config import settings
from app.infrastructure.database import UnitOfWorkFactory, engine
from app.infrastructure.logging_setup import configure_logging
from app.infrastructure.metrics import (
    RECOVERY_CYCLES,
    RECOVERY_HEARTBEAT,
)
from app.infrastructure.recovery_health import RecoveryHeartbeat
from app.infrastructure.recovery_queue import ArqRecoveryQueue

log = logging.getLogger("content-service.recovery")


async def run_cycle(
    service: RecoveryService, queue: RecoveryQueue, heartbeat: RecoveryHeartbeat
) -> bool:
    """Do not advertise health when DB/queue work fails or stops progressing."""
    try:
        healthy = await service.run(queue)
        if healthy:
            await heartbeat.beat()
            RECOVERY_HEARTBEAT.set_to_current_time()
        RECOVERY_CYCLES.labels("success" if healthy else "partial_failure").inc()
        return healthy
    except Exception:
        RECOVERY_CYCLES.labels("error").inc()
        log.exception("Recovery cycle failed")
        return False


async def serve() -> None:
    """Reconnect after Redis outages and sweep every configured interval."""
    service = RecoveryService(UnitOfWorkFactory())
    pool = None
    try:
        while True:
            try:
                if pool is None:
                    pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
                await run_cycle(
                    service, ArqRecoveryQueue(pool), RecoveryHeartbeat(pool)
                )
            except Exception:
                RECOVERY_CYCLES.labels("connection_error").inc()
                log.exception("Recovery connection failed")
            await asyncio.sleep(settings.recovery_interval_seconds)
    finally:
        if pool is not None:
            await pool.aclose()
        await engine.dispose()


async def healthcheck() -> bool:
    """Check this container's heartbeat without writing a new heartbeat."""
    pool = None
    try:
        async with asyncio.timeout(8):
            pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
            return await RecoveryHeartbeat(pool).healthy()
    except Exception:
        return False
    finally:
        if pool is not None:
            await pool.aclose()


def main() -> None:
    """Expose recovery metrics separately from the API process registry."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--healthcheck", action="store_true")
    args = parser.parse_args()
    if args.healthcheck:
        raise SystemExit(0 if asyncio.run(healthcheck()) else 1)
    configure_logging("recovery")
    start_http_server(settings.recovery_metrics_port)
    asyncio.run(serve())


if __name__ == "__main__":
    main()
