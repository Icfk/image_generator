"""Optional integration checks using an isolated real Redis instance."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from arq import create_pool
from arq.connections import RedisSettings
from arq.constants import default_queue_name, job_key_prefix

from app.application.recovery import RecoveryService
from app.infrastructure.recovery_queue import ArqRecoveryQueue


async def test_real_arq_duplicate_and_redis_state_loss(database) -> None:
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to an isolated Redis instance")
    pool = await create_pool(RedisSettings.from_dsn(url))
    task = await database.service.create(1, "image", "fox", None, "redis-test")
    job_id = f"generation:{task['id']}"
    now = [datetime.now(UTC)]
    recovery = RecoveryService(database.factory, lambda: now[0])
    queue = ArqRecoveryQueue(pool)
    try:
        assert await recovery.run(queue)
        assert await queue.status(job_id) == "queued"
        now[0] += timedelta(seconds=16)
        assert await recovery.run(queue)
        ids = await pool.zrange(default_queue_name, 0, -1)
        assert ids.count(job_id.encode()) == 1
        # Delete only this test's volatile job, simulating a Redis restart/loss.
        await pool.delete(job_key_prefix + job_id)
        await pool.zrem(default_queue_name, job_id)
        assert await queue.status(job_id) == "not_found"
        now[0] += timedelta(seconds=16)
        assert await recovery.run(queue)
        assert await queue.status(job_id) == "queued"
        ids = await pool.zrange(default_queue_name, 0, -1)
        assert ids.count(job_id.encode()) == 1
    finally:
        await pool.delete(job_key_prefix + job_id)
        await pool.zrem(default_queue_name, job_id)
        await pool.aclose()
