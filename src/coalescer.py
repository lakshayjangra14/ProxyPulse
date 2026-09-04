import asyncio


class Coalescer:
    def __init__(self):
        self._in_flight = {}

    def is_in_flight(self, key):
        """Check if someone is already fetching this key."""
        return key in self._in_flight

    async def wait(self, key):
        """Wait for an in-flight request to complete. Returns the result."""
        return await self._in_flight[key]

    def register(self, key):
        """Register a new in-flight request. Returns the Future."""
        future = asyncio.get_event_loop().create_future()
        self._in_flight[key] = future
        return future

    def resolve(self, key, result):
        """Resolve the Future — all waiters get this result."""
        future = self._in_flight.pop(key, None)
        if future and not future.done():
            future.set_result(result)

    def reject(self, key, error):
        """Reject the Future — all waiters get this error."""
        future = self._in_flight.pop(key, None)
        if future and not future.done():
            future.set_exception(error)

    @property
    def active_count(self):
        """How many requests are currently in-flight."""
        return len(self._in_flight)
