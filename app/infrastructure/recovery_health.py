"""Per-container recovery heartbeat; stale or absent keys are unhealthy."""

import asyncio
import socket
import time
from collections.abc import Callable

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.infrastructure.config import settings


class RecoveryHeartbeat:
    """Publish liveness only after a successful DB + queue sweep."""

    def __init__(
        self,
        redis: Redis,
        *,
        clock: Callable[[], float] = time.time,
        identity: str | None = None,
    ) -> None:
        self.redis = redis
        self.clock = clock
        self.key = f"recovery:heartbeat:{identity or socket.gethostname()}"

    async def beat(self) -> None:
        """Expire heartbeat automatically when this process stops progressing."""
        async with asyncio.timeout(5):
            await self.redis.set(
                self.key, str(self.clock()), ex=settings.recovery_health_max_age_seconds
            )

    async def healthy(self) -> bool:
        """A Redis failure or expired heartbeat is not a successful health check."""
        try:
            async with asyncio.timeout(5):
                value = await self.redis.get(self.key)
            return value is not None and (
                0
                <= self.clock() - float(value)
                <= settings.recovery_health_max_age_seconds
            )
        except (OSError, ValueError, TimeoutError, RedisError):
            return False
