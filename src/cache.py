import json
import base64
import redis.asyncio as aioredis


class Cache:
    def __init__(self, redis_url, default_ttl=60):
        self.redis = aioredis.from_url(redis_url)
        self.default_ttl = default_ttl

    def generate_key(self, method, path_qs):
        return f"cache:{method}:{path_qs}"

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

    async def set(self, key, status, headers, body, ttl=None):
        entry = json.dumps({
            "status": status,
            "headers": dict(headers),
            "body": base64.b64encode(body).decode("ascii")
        })
        await self.redis.set(key, entry, ex=ttl or self.default_ttl)

    async def close(self):
        await self.redis.close()
