"""Redis-backed rate limiting infrastructure."""

from redis.asyncio import Redis

CHECK_LIMIT = """
local counter = KEYS[1]
local blocked = KEYS[2]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
-- Repair legacy counters left without a TTL by the former two-command path.
if redis.call('EXISTS', counter) == 1 and redis.call('TTL', counter) < 0 then
    redis.call('DEL', counter)
end
if redis.call('EXISTS', blocked) == 1 then
    if redis.call('TTL', blocked) < 0 then
        redis.call('EXPIRE', blocked, window)
    end
    return 0
end
local count = redis.call('INCR', counter)
if count == 1 then redis.call('EXPIRE', counter, window) end
if count > limit then
    redis.call('SETEX', blocked, window, '1')
    return 0
end
return 1
"""


class RateLimitRepository:
    """Store and evaluate per-user rate limit counters."""

    def __init__(self, client: Redis) -> None:
        self.client = client

    async def check(self, user_id: int, limit: int = 10, window: int = 60) -> bool:
        """Atomically count, set expiry, and block; also repair legacy no-TTL keys."""
        if limit < 1 or window < 1:
            raise ValueError("Rate limit and window must be positive")
        key = f"rate:user:{user_id}"
        blocked = f"blocked:user:{user_id}"
        return bool(await self.client.eval(CHECK_LIMIT, 2, key, blocked, limit, window))
