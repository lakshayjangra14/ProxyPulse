import asyncio
import random
import time


class RetryPolicy:
    """
    Retry wrapper with exponential backoff + jitter.

    Retries on:
      - Connection errors (upstream unreachable)
      - HTTP 502, 503, 504 (transient server errors)

    Does NOT retry on:
      - HTTP 400, 401, 404, 500 etc. (non-transient)
      - Successful responses

    Backoff formula:
      delay = base_delay * (2 ^ attempt) + random jitter
    """

    RETRYABLE_STATUS_CODES = {502, 503, 504}

    def __init__(self, max_retries=3, base_delay=0.5, max_delay=10.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def _backoff_delay(self, attempt):
        """Calculate delay with exponential backoff + jitter."""
        delay = self.base_delay * (2 ** attempt)
        delay = min(delay, self.max_delay)
        # Add jitter: ±25% randomness to prevent thundering herd on retries
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return delay + jitter

    async def execute(self, coro_factory):
        """
        Execute an async operation with retries.

        Args:
            coro_factory: A callable that returns (status, headers, body, elapsed).
                          Will be called on each attempt.

        Returns:
            (status, headers, body, elapsed, attempts)

        Raises:
            Last exception if all retries are exhausted.
        """
        last_exception = None
        attempts = 0

        for attempt in range(self.max_retries + 1):
            attempts = attempt + 1

            try:
                status, headers, body, elapsed = await coro_factory()

                # If retryable status and we have retries left, retry
                if status in self.RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    delay = self._backoff_delay(attempt)
                    await asyncio.sleep(delay)
                    continue

                return status, headers, body, elapsed, attempts

            except (asyncio.TimeoutError, OSError, ConnectionError) as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self._backoff_delay(attempt)
                    await asyncio.sleep(delay)
                    continue
                raise

        raise last_exception
