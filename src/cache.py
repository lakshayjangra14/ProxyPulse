import json
import base64
import redis.asyncio as aioredis


class Cache:
    def __init__(self, redis_url, default_ttl=60, stale_ttl_multiplier=5):
        self.redis = aioredis.from_url(redis_url)
        self.default_ttl = default_ttl
        self.stale_ttl_multiplier = stale_ttl_multiplier

    def generate_key(self, method, path_qs):
        return f"cache:{method}:{path_qs}"

    def _stale_key(self, key):
        """Shadow key for stale-cache fallback."""
        return f"stale:{key}"

    async def get(self, key):
        data = await self.redis.get(key)
        if data is None:
            return None

        entry = json.loads(data)
        return {
            "status": entry["status"],
            "headers": entry["headers"],
            "body": base64.b64decode(entry["body"])
        }

    async def get_stale(self, key):
        """Retrieve a stale (expired from primary cache) entry as fallback."""
        data = await self.redis.get(self._stale_key(key))
        if data is None:
            return None

        entry = json.loads(data)
        return {
            "status": entry["status"],
            "headers": entry["headers"],
            "body": base64.b64decode(entry["body"])
        }

    async def set(self, key, status, headers, body, ttl=None):
        effective_ttl = ttl or self.default_ttl
        entry = json.dumps({
            "status": status,
            "headers": dict(headers),
            "body": base64.b64encode(body).decode("ascii")
        })

        pipe = self.redis.pipeline()
        # Primary cache entry with normal TTL
        pipe.set(key, entry, ex=effective_ttl)
        # Stale shadow with much longer TTL for graceful degradation
        pipe.set(self._stale_key(key), entry,
                 ex=effective_ttl * self.stale_ttl_multiplier)
        await pipe.execute()

    async def close(self):
        await self.redis.close()
