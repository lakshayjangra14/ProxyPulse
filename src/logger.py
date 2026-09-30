import logging
import time
import json
import sys


class JsonFormatter(logging.Formatter):
    """
    Structured JSON log formatter.

    Each log line is a single JSON object — easy to pipe into
    log aggregators (ELK, Datadog, CloudWatch, etc.)
    """

    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge any extra structured fields
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        return json.dumps(log_entry, default=str)


def setup_logger(name="proxypulse", level=logging.INFO):
    """Configure structured JSON logging for ProxyPulse."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers on reload
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate aiohttp logs)
    logger.propagate = False

    return logger


class RequestLogger:
    """
    Logs structured request/response data for each proxied request.

    Captures:
      - Client IP, method, path
      - Response status, latency
      - Cache status (HIT/MISS/COALESCED)
      - Circuit breaker state
      - Retry attempts
      - Rate limit info
    """

    def __init__(self, logger=None):
        self.logger = logger or setup_logger()
        self._request_count = 0
        self._status_counts = {}
        self._cache_hits = 0
        self._cache_misses = 0
        self._total_latency = 0.0

    def log_request(self, *, client_ip, method, path, status, latency_ms,
                    cache_status=None, circuit_state=None, retry_attempts=None,
                    rate_limited=False, error=None):
        """Log a single proxied request with structured data."""
        self._request_count += 1
        self._status_counts[status] = self._status_counts.get(status, 0) + 1
        self._total_latency += latency_ms

        if cache_status == "HIT":
            self._cache_hits += 1
        elif cache_status == "MISS":
            self._cache_misses += 1

        extra = {
            "type": "request",
            "client_ip": client_ip,
            "method": method,
            "path": path,
            "status": status,
            "latency_ms": round(latency_ms, 2),
        }

        if cache_status:
            extra["cache"] = cache_status
        if circuit_state:
            extra["circuit"] = circuit_state
        if retry_attempts and retry_attempts > 1:
            extra["retries"] = retry_attempts
        if rate_limited:
            extra["rate_limited"] = True
        if error:
            extra["error"] = str(error)

        level = logging.WARNING if status >= 500 else logging.INFO
        if rate_limited:
            level = logging.WARNING

        record = self.logger.makeRecord(
            self.logger.name, level, "", 0,
            f"{method} {path} → {status} ({latency_ms:.1f}ms)",
            (), None
        )
        record.extra_fields = extra
        self.logger.handle(record)

    @property
    def stats(self):
        """Return aggregate metrics."""
        total = self._request_count
        return {
            "total_requests": total,
            "status_counts": dict(self._status_counts),
            "cache_hit_ratio": round(
                self._cache_hits / max(self._cache_hits + self._cache_misses, 1), 3
            ),
            "avg_latency_ms": round(
                self._total_latency / max(total, 1), 2
            ),
        }
