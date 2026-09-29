"""Bounded ARQ operations for the recovery publisher."""

import asyncio

from arq.connections import ArqRedis
from arq.jobs import Job


class ArqRecoveryQueue:
    """Adapt ARQ status/deduplication to the application's queue interface."""

    def __init__(self, pool: ArqRedis) -> None:
        self.pool = pool

    async def status(self, job_id: str) -> str:
        """Read live Redis state, not merely the DB publication acknowledgement."""
        async with asyncio.timeout(5):
            return (await Job(job_id, self.pool).status()).value

    async def enqueue(self, function: str, job_id: str, *args: object) -> None:
        """Keep enqueue bounded while recovery holds a database row lock."""
        async with asyncio.timeout(5):
            await self.pool.enqueue_job(function, *args, _job_id=job_id)
