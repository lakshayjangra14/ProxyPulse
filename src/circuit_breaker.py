import time
import asyncio
from enum import Enum


class State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Circuit breaker for upstream protection.

    CLOSED  → requests pass through; consecutive failures are counted.
              When failures hit `failure_threshold`, trip to OPEN.

    OPEN    → all requests are immediately rejected with 503.
              After `recovery_timeout` seconds, move to HALF_OPEN.

    HALF_OPEN → allow ONE probe request through.
                If it succeeds → reset to CLOSED.
                If it fails   → back to OPEN.
    """

    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = State.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_lock = asyncio.Lock()

    @property
    def state(self):
        """
        Returns the effective state — may auto-transition
        from OPEN → HALF_OPEN if the recovery timeout has elapsed.
        """
        if self._state == State.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                self._state = State.HALF_OPEN
        return self._state

    def allow_request(self):
        """
        Should we allow this request through?

        Returns:
            (allowed: bool, state: State)
        """
        current = self.state

        if current == State.CLOSED:
            return True, current

        if current == State.HALF_OPEN:
            # Only allow one probe at a time
            if self._half_open_lock.locked():
                return False, current
            return True, current

        # OPEN
        return False, current

    async def acquire_half_open(self):
        """Acquire the half-open lock for a probe request."""
        return await self._half_open_lock.acquire()

    def record_success(self):
        """Record a successful upstream response."""
        self._failure_count = 0
        if self._state in (State.HALF_OPEN, State.OPEN):
            self._state = State.CLOSED
        if self._half_open_lock.locked():
            self._half_open_lock.release()

    def record_failure(self):
        """Record a failed upstream response."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == State.HALF_OPEN:
            # Probe failed — back to OPEN
            self._state = State.OPEN
            if self._half_open_lock.locked():
                self._half_open_lock.release()
        elif self._failure_count >= self.failure_threshold:
            self._state = State.OPEN

        return self._state

    @property
    def stats(self):
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }
