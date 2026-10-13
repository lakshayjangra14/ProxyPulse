# ProxyPulse

A high-performance **caching reverse proxy** built from scratch with Python's `aiohttp`, designed to sit between clients and upstream backend APIs.

## Features

- **Reverse Proxy** — Forwards any HTTP method/path to upstream backends with hop-by-hop header filtering
- **Redis Caching** — GET responses cached in Redis with configurable TTLs
- **Request Coalescing** — Concurrent requests for the same uncached resource share a single backend call (thundering herd protection)
- **Stale-Cache Fallback** — Serves expired cached responses when the backend is unavailable (graceful degradation)
- **Rate Limiting** — Per-IP sliding-window rate limiter backed by Redis sorted sets
- **Circuit Breaker** — CLOSED → OPEN → HALF_OPEN state machine to protect upstream services
- **Retry with Backoff** — Exponential backoff + jitter on transient errors (502/503/504)
- **Weighted Load Balancing** — Weighted round-robin across multiple backends with per-backend health tracking
- **Active Health Checks** — Background task periodically probes backend `/health` endpoints
- **Structured Logging** — JSON-formatted request logs with aggregate metrics
- **`/metrics` Endpoint** — Real-time observability: cache hit rate, coalesced count, avg latency

## Architecture

```
Client ──▶ ProxyPulse (:8080)
              │
              ├─ Rate Limiter (Redis)
              ├─ Cache Check (Redis)
              ├─ Coalescer (in-memory Futures)
              ├─ Circuit Breaker
              ├─ Retry Policy
              └─ Load Balancer (Weighted Round-Robin)
                    ├─▶ Backend A (:9000)
                    └─▶ Backend B (:9001)
                          ▲
                    Health Checker (background probes)
```

## Quick Start

### Prerequisites

- Python 3.10+
- Redis server running on `localhost:6379`

### Setup

```bash
# Clone and install
git clone https://github.com/lakshayjangra14/ProxyPulse.git
cd ProxyPulse
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Start mock backends (two terminals)
python -m mock_backend.server 9000
python -m mock_backend.server 9001

# Start the proxy
python -m src.server
```

### Test It

```bash
# Basic proxy request (cache MISS, then HIT)
curl -i http://localhost:8080/api/products
curl -i http://localhost:8080/api/products   # X-Cache: HIT

# Retry behavior (flaky endpoint)
curl http://localhost:8080/api/flaky

# Check which backend handled the request
curl http://localhost:8080/api/identity

# Real-time metrics
curl http://localhost:8080/metrics

# Full system status
curl http://localhost:8080/_proxy/status
```

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /metrics` | Cache hit ratio, coalesced requests, avg latency, request counts |
| `GET /_proxy/status` | Full system state: circuit breaker, load balancer, health checker, coalescer |
| `* /{anything}` | Proxied to upstream backend |

## Configuration

All settings in [`config.yml`](config.yml):

```yaml
proxy:
  host: "0.0.0.0"
  port: 8080

upstream:
  backends:
    - url: "http://localhost:9000"
      weight: 2                    # Gets 2x traffic
    - url: "http://localhost:9001"
      weight: 1
  timeout: 10                      # Upstream request timeout (seconds)
  failure_threshold: 3             # Failures before backend marked unhealthy
  recovery_timeout: 15             # Seconds before retrying unhealthy backend

cache:
  redis_url: "redis://localhost:6379"
  default_ttl: 60                  # Cache TTL (seconds)

rate_limit:
  max_requests: 100                # Max requests per window per IP
  window_seconds: 60

circuit_breaker:
  failure_threshold: 5             # Consecutive failures to trip OPEN
  recovery_timeout: 30             # Seconds before HALF_OPEN probe

retry:
  max_retries: 3
  base_delay: 0.5                  # Base delay for exponential backoff
  max_delay: 10.0                  # Max delay cap
```

## Response Headers

Every proxied response includes:

| Header | Description |
|---|---|
| `X-Proxy: ProxyPulse` | Identifies the proxy |
| `X-Cache: HIT / MISS / COALESCED / STALE` | Cache status |
| `X-Circuit: CLOSED / OPEN / HALF_OPEN` | Circuit breaker state |
| `X-Backend` | Which backend handled the request |
| `X-Upstream-Response-Time` | Backend response latency |
| `X-Retry-Attempts` | Number of attempts made |
| `X-RateLimit-Limit` | Rate limit ceiling |
| `X-RateLimit-Remaining` | Remaining requests in window |
| `X-RateLimit-Reset` | Window reset timestamp |

## Project Structure

```
ProxyPulse/
├── config.yml              # All configuration
├── requirements.txt        # aiohttp, pyyaml, redis
├── src/
│   ├── server.py           # Main proxy server + route handlers
│   ├── cache.py            # Redis cache with stale-cache fallback
│   ├── coalescer.py        # Request coalescing (asyncio.Future registry)
│   ├── rate_limiter.py     # Sliding-window rate limiter (Redis sorted sets)
│   ├── circuit_breaker.py  # CLOSED/OPEN/HALF_OPEN state machine
│   ├── retry.py            # Exponential backoff + jitter
│   ├── load_balancer.py    # Weighted round-robin + per-backend health
│   ├── health_checker.py   # Active background health probes
│   ├── logger.py           # Structured JSON logging + metrics
│   └── config.py           # YAML config loader
└── mock_backend/
    └── server.py           # Fake API server for testing
```
