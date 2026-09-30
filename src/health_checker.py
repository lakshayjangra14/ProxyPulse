import asyncio
import time
import logging
from aiohttp import ClientSession, ClientTimeout

logger = logging.getLogger("proxypulse")


class HealthChecker:
    """
    Active health checker that periodically probes each backend.

    Runs as a background asyncio task. On each interval:
      1. Send GET to each backend's health endpoint
      2. If healthy response → mark backend healthy, reset failures
      3. If timeout/error/bad status → increment failure count
      4. If failures >= threshold → mark backend unhealthy

    Works alongside the passive health tracking in the LoadBalancer.
    Active checks can RESTORE backends that passive checks marked down,
    and can also DETECT failures before any real user traffic hits them.
    """

    def __init__(self, load_balancer, *,
                 health_path="/health",
                 interval=10,
                 timeout=5,
                 healthy_threshold=2,
                 unhealthy_threshold=3):
        self.lb = load_balancer
        self.health_path = health_path
        self.interval = interval
        self.timeout = timeout
        self.healthy_threshold = healthy_threshold
        self.unhealthy_threshold = unhealthy_threshold

        self._task = None
        self._session = None
        self._running = False

        # Track consecutive healthy checks for recovery
        self._healthy_streak = {}

    async def start(self):
        """Start the background health check loop."""
        self._session = ClientSession(
            timeout=ClientTimeout(total=self.timeout)
        )
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Health checker started (interval=%ds, path=%s)",
                     self.interval, self.health_path)

    async def stop(self):
        """Stop the health check loop and clean up."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        logger.info("Health checker stopped")

    async def _loop(self):
        """Main loop — runs checks at each interval."""
        while self._running:
            await self._check_all()
            await asyncio.sleep(self.interval)

    async def _check_all(self):
        """Check all backends concurrently."""
        tasks = [
            self._check_one(backend)
            for backend in self.lb.backends
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_one(self, backend):
        """Probe a single backend's health endpoint."""
        url = backend.url + self.health_path
        start = time.monotonic()

        try:
            async with self._session.get(url) as resp:
                elapsed = (time.monotonic() - start) * 1000
                body = await resp.text()

                if resp.status == 200:
                    self._on_healthy(backend, elapsed)
                else:
                    self._on_unhealthy(backend, elapsed,
                                       reason=f"status {resp.status}")

        except asyncio.TimeoutError:
            elapsed = (time.monotonic() - start) * 1000
            self._on_unhealthy(backend, elapsed, reason="timeout")

        except (OSError, ConnectionError) as e:
            elapsed = (time.monotonic() - start) * 1000
            self._on_unhealthy(backend, elapsed, reason=str(e))

    def _on_healthy(self, backend, elapsed_ms):
        """Handle a successful health check."""
        key = backend.url
        self._healthy_streak[key] = self._healthy_streak.get(key, 0) + 1

        if not backend.healthy and self._healthy_streak[key] >= self.healthy_threshold:
            # Backend recovered — restore it
            backend.healthy = True
            backend.failure_count = 0
            logger.info(
                "Backend %s recovered (healthy streak: %d, latency: %.1fms)",
                backend.url, self._healthy_streak[key], elapsed_ms
            )
        elif backend.healthy:
            # Reset failure count on healthy response
            backend.failure_count = 0

    def _on_unhealthy(self, backend, elapsed_ms, reason="unknown"):
        """Handle a failed health check."""
        self._healthy_streak[backend.url] = 0
        backend.failure_count += 1

        if backend.healthy and backend.failure_count >= self.unhealthy_threshold:
            backend.healthy = False
            backend.last_failure_time = time.monotonic()
            logger.warning(
                "Backend %s marked UNHEALTHY (reason: %s, failures: %d, latency: %.1fms)",
                backend.url, reason, backend.failure_count, elapsed_ms
            )
        elif not backend.healthy:
            backend.last_failure_time = time.monotonic()

    @property
    def stats(self):
        return {
            "interval": self.interval,
            "health_path": self.health_path,
            "timeout": self.timeout,
            "healthy_threshold": self.healthy_threshold,
            "unhealthy_threshold": self.unhealthy_threshold,
            "healthy_streaks": dict(self._healthy_streak),
        }
