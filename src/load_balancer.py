import time
import asyncio
from dataclasses import dataclass, field


@dataclass
class Backend:
    """Represents a single upstream backend server."""
    url: str
    weight: int = 1
    healthy: bool = True
    failure_count: int = 0
    last_failure_time: float = 0.0
    total_requests: int = 0
    total_failures: int = 0

    @property
    def stats(self):
        return {
            "url": self.url,
            "weight": self.weight,
            "healthy": self.healthy,
            "failure_count": self.failure_count,
            "total_requests": self.total_requests,
            "total_failures": self.total_failures,
        }


class LoadBalancer:
    """
    Weighted round-robin load balancer with per-backend health tracking.

    Features:
      - Weighted round-robin: backends with weight=2 get 2x traffic
      - Per-backend health: after `failure_threshold` consecutive failures,
        a backend is marked unhealthy and skipped
      - Auto-recovery: unhealthy backends are retried after `recovery_timeout`
      - Fallback: if ALL backends are down, raises NoHealthyBackendError
    """

    def __init__(self, backends_config, failure_threshold=3, recovery_timeout=15):
        self.backends = []
        for entry in backends_config:
            if isinstance(entry, str):
                self.backends.append(Backend(url=entry))
            else:
                self.backends.append(Backend(
                    url=entry['url'],
                    weight=entry.get('weight', 1)
                ))

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        # Build weighted index for round-robin
        self._weighted_list = []
        for i, b in enumerate(self.backends):
            self._weighted_list.extend([i] * b.weight)

        self._current_index = 0
        self._lock = asyncio.Lock()

    def _get_healthy_backends(self):
        """Return list of (index, backend) that are healthy or ready for retry."""
        now = time.monotonic()
        available = []

        for i, backend in enumerate(self.backends):
            if backend.healthy:
                available.append((i, backend))
            elif now - backend.last_failure_time >= self.recovery_timeout:
                # Recovery timeout elapsed — give it another chance
                backend.healthy = True
                backend.failure_count = 0
                available.append((i, backend))

        return available

    def next_backend(self):
        """
        Pick the next healthy backend using weighted round-robin.

        Returns:
            Backend instance

        Raises:
            NoHealthyBackendError if all backends are down
        """
        healthy = self._get_healthy_backends()
        if not healthy:
            raise NoHealthyBackendError("All backends are unhealthy")

        healthy_indices = {i for i, _ in healthy}

        # Walk the weighted list to find the next healthy backend
        for _ in range(len(self._weighted_list)):
            idx = self._weighted_list[self._current_index % len(self._weighted_list)]
            self._current_index += 1
            if idx in healthy_indices:
                backend = self.backends[idx]
                backend.total_requests += 1
                return backend

        # Shouldn't reach here, but fallback
        raise NoHealthyBackendError("No healthy backend found in rotation")

    def record_success(self, backend):
        """Record a successful response from a backend."""
        backend.failure_count = 0
        backend.healthy = True

    def record_failure(self, backend):
        """Record a failed response from a backend."""
        backend.failure_count += 1
        backend.total_failures += 1
        backend.last_failure_time = time.monotonic()

        if backend.failure_count >= self.failure_threshold:
            backend.healthy = False

    @property
    def stats(self):
        return {
            "strategy": "weighted_round_robin",
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "backends": [b.stats for b in self.backends],
        }


class NoHealthyBackendError(Exception):
    """Raised when no healthy backends are available."""
    pass
