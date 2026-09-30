import time


class RateLimiter:
    """
    Sliding-window rate limiter backed by Redis sorted sets.

    For each client IP, we maintain a sorted set where:
      - each member is a unique request ID (timestamp + counter)
      - each score is the request timestamp

    On every request we:
      1. Remove entries older than the window
      2. Count remaining entries
      3. If under the limit, add the new entry
      4. Return (allowed, remaining, reset_at)
    """

    def __init__(self, redis_client, max_requests=100, window_seconds=60):
        self.redis = redis_client
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def _key(self, client_ip):
        return f"ratelimit:{client_ip}"

    async def check(self, client_ip):
        """
        Check and consume one request for the given client IP.

        Returns:
            (allowed: bool, remaining: int, reset_at: float)
        """
        now = time.time()
        window_start = now - self.window_seconds
        key = self._key(client_ip)

        pipe = self.redis.pipeline()

        # 1. Remove expired entries (scores older than window_start)
        pipe.zremrangebyscore(key, 0, window_start)

        # 2. Count current entries in the window
        pipe.zcard(key)

        # 3. Add current request (unique member using timestamp)
        # Use timestamp with enough precision to avoid collisions
        member = f"{now:.6f}"
        pipe.zadd(key, {member: now})

        # 4. Set TTL on the key so Redis auto-cleans idle keys
        pipe.expire(key, self.window_seconds)

        results = await pipe.execute()

        # results[0] = number removed, results[1] = count BEFORE adding
        current_count = results[1]

        if current_count >= self.max_requests:
            # Over the limit — remove the entry we just optimistically added
            await self.redis.zrem(key, member)

            # Find the oldest entry to calculate reset time
            oldest = await self.redis.zrange(key, 0, 0, withscores=True)
            if oldest:
                reset_at = oldest[0][1] + self.window_seconds
            else:
                reset_at = now + self.window_seconds

            return False, 0, reset_at

        remaining = self.max_requests - current_count - 1
        reset_at = now + self.window_seconds

        return True, remaining, reset_at
