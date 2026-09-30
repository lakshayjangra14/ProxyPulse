from aiohttp import web, ClientSession, ClientTimeout
import time
from src.config import load_config
from src.cache import Cache
from src.coalescer import Coalescer
from src.rate_limiter import RateLimiter
from src.circuit_breaker import CircuitBreaker
from src.retry import RetryPolicy
from src.logger import RequestLogger, setup_logger
from src.load_balancer import LoadBalancer, NoHealthyBackendError


# Headers that should NOT be forwarded between proxy and backend
HOP_BY_HOP_HEADERS = {
    'transfer-encoding', 'connection', 'keep-alive',
    'proxy-authenticate', 'proxy-authorization',
    'te', 'trailers', 'upgrade', 'host',
    'content-encoding', 'content-length'
}
def filter_headers(headers):
    return {
        key: value for key, value in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }
async def proxy_handler(request):
    request_start = time.monotonic()
    config = request.app['config']
    session = request.app['client_session']
    cache = request.app['cache']
    coalescer = request.app['coalescer']
    rate_limiter = request.app['rate_limiter']
    req_logger = request.app['request_logger']

    # --- Rate Limiting ---
    client_ip = request.remote
    allowed, remaining, reset_at = await rate_limiter.check(client_ip)

    if not allowed:
        latency = (time.monotonic() - request_start) * 1000
        req_logger.log_request(
            client_ip=client_ip, method=request.method, path=request.path,
            status=429, latency_ms=latency, rate_limited=True
        )
        return web.json_response(
            {"error": "Too Many Requests", "retry_after": round(reset_at - time.time(), 1)},
            status=429,
            headers={
                'X-Proxy': 'ProxyPulse',
                'X-RateLimit-Limit': str(rate_limiter.max_requests),
                'X-RateLimit-Remaining': '0',
                'X-RateLimit-Reset': str(int(reset_at)),
                'Retry-After': str(int(reset_at - time.time())),
            }
        )

    # Rate limit headers to attach to every response
    rate_limit_headers = {
        'X-RateLimit-Limit': str(rate_limiter.max_requests),
        'X-RateLimit-Remaining': str(remaining),
        'X-RateLimit-Reset': str(int(reset_at)),
    }

    target_url = config['upstream']['base_url'] + request.path_qs

    # Pick a backend via load balancer (if configured)
    lb = request.app.get('load_balancer')
    backend = None
    if lb:
        try:
            backend = lb.next_backend()
            target_url = backend.url + request.path_qs
        except NoHealthyBackendError:
            latency = (time.monotonic() - request_start) * 1000
            req_logger.log_request(
                client_ip=client_ip, method=request.method, path=request.path,
                status=503, latency_ms=latency, error='All backends unhealthy'
            )
            return web.json_response(
                {'error': 'Service Unavailable', 'detail': 'All backends are unhealthy'},
                status=503,
                headers={'X-Proxy': 'ProxyPulse', **rate_limit_headers}
            )

    # Only cache GET requests
    if request.method == 'GET':
        cache_key = cache.generate_key(request.method, request.path_qs)

        # Step 1: Check cache
        cached = await cache.get(cache_key)
        if cached:
            response_headers = cached['headers']
            response_headers['X-Proxy'] = 'ProxyPulse'
            response_headers['X-Cache'] = 'HIT'
            response_headers.update(rate_limit_headers)
            latency = (time.monotonic() - request_start) * 1000
            req_logger.log_request(
                client_ip=client_ip, method=request.method, path=request.path,
                status=cached['status'], latency_ms=latency, cache_status='HIT'
            )
            return web.Response(
                body=cached['body'],
                status=cached['status'],
                headers=response_headers
            )

        # Step 2: Check if someone else is already fetching this
        if coalescer.is_in_flight(cache_key):
            result = await coalescer.wait(cache_key)
            response_headers = result['headers'].copy()
            response_headers['X-Proxy'] = 'ProxyPulse'
            response_headers['X-Cache'] = 'COALESCED'
            response_headers.update(rate_limit_headers)
            latency = (time.monotonic() - request_start) * 1000
            req_logger.log_request(
                client_ip=client_ip, method=request.method, path=request.path,
                status=result['status'], latency_ms=latency, cache_status='COALESCED'
            )
            return web.Response(
                body=result['body'],
                status=result['status'],
                headers=response_headers
            )

        # Step 3: We're the first — register and fetch
        coalescer.register(cache_key)

    # --- Circuit Breaker ---
    breaker = request.app['circuit_breaker']
    cb_allowed, cb_state = breaker.allow_request()

    if not cb_allowed:
        # Circuit is OPEN — fail fast
        if request.method == 'GET' and coalescer.is_in_flight(cache_key):
            coalescer.reject(cache_key, Exception('circuit open'))
        latency = (time.monotonic() - request_start) * 1000
        req_logger.log_request(
            client_ip=client_ip, method=request.method, path=request.path,
            status=503, latency_ms=latency, circuit_state='OPEN'
        )
        return web.json_response(
            {
                'error': 'Service Unavailable',
                'detail': 'Circuit breaker is OPEN — upstream is down',
                'circuit': breaker.stats,
            },
            status=503,
            headers={
                'X-Proxy': 'ProxyPulse',
                'X-Circuit': breaker.state.value,
                **rate_limit_headers,
            }
        )

    # If HALF_OPEN, acquire probe lock
    if cb_state.value == 'HALF_OPEN':
        await breaker.acquire_half_open()

    # Forward to backend (with retries)
    body = await request.read()
    req_headers = filter_headers(request.headers)
    retry_policy = request.app['retry_policy']
    upstream_timeout = ClientTimeout(total=config['upstream']['timeout'])

    async def do_upstream_request():
        """Single upstream attempt — called by retry policy."""
        start = time.monotonic()
        async with session.request(
            method=request.method,
            url=target_url,
            headers=req_headers,
            data=body,
            timeout=upstream_timeout
        ) as resp:
            elapsed = time.monotonic() - start
            resp_body = await resp.read()
            resp_headers = filter_headers(resp.headers)
            return resp.status, resp_headers, resp_body, elapsed

    try:
        status, response_headers, response_body, elapsed, attempts = \
            await retry_policy.execute(do_upstream_request)

        # Record success/failure for circuit breaker + load balancer
        if status >= 500:
            breaker.record_failure()
            if lb and backend:
                lb.record_failure(backend)
        else:
            breaker.record_success()
            if lb and backend:
                lb.record_success(backend)

        # Cache and resolve if GET + 200
        if request.method == 'GET' and status == 200:
            await cache.set(
                cache_key,
                status,
                response_headers,
                response_body
            )
            coalescer.resolve(cache_key, {
                'status': status,
                'headers': response_headers,
                'body': response_body
            })

        response_headers['X-Proxy'] = 'ProxyPulse'
        response_headers['X-Cache'] = 'MISS'
        response_headers['X-Upstream-Response-Time'] = f'{elapsed * 1000:.1f}ms'
        response_headers['X-Circuit'] = breaker.state.value
        response_headers['X-Retry-Attempts'] = str(attempts)
        if backend:
            response_headers['X-Backend'] = backend.url
        response_headers.update(rate_limit_headers)

        latency = (time.monotonic() - request_start) * 1000
        req_logger.log_request(
            client_ip=client_ip, method=request.method, path=request.path,
            status=status, latency_ms=latency, cache_status='MISS',
            circuit_state=breaker.state.value, retry_attempts=attempts
        )

        return web.Response(
            body=response_body,
            status=status,
            headers=response_headers
        )
    except Exception as e:
        breaker.record_failure()
        if lb and backend:
            lb.record_failure(backend)
        latency = (time.monotonic() - request_start) * 1000
        req_logger.log_request(
            client_ip=client_ip, method=request.method, path=request.path,
            status=502, latency_ms=latency, circuit_state=breaker.state.value,
            error=e
        )
        if request.method == 'GET':
            coalescer.reject(cache_key, e)
        raise


async def client_session_ctx(app):
    """Create ClientSession, Cache, and Coalescer on startup, close on shutdown."""
    config = app['config']

    app['client_session'] = ClientSession()
    app['cache'] = Cache(
        redis_url=config['cache']['redis_url'],
        default_ttl=config['cache']['default_ttl']
    )
    app['coalescer'] = Coalescer()
    app['rate_limiter'] = RateLimiter(
        redis_client=app['cache'].redis,
        max_requests=config['rate_limit']['max_requests'],
        window_seconds=config['rate_limit']['window_seconds']
    )
    app['circuit_breaker'] = CircuitBreaker(
        failure_threshold=config['circuit_breaker']['failure_threshold'],
        recovery_timeout=config['circuit_breaker']['recovery_timeout']
    )
    app['retry_policy'] = RetryPolicy(
        max_retries=config['retry']['max_retries'],
        base_delay=config['retry']['base_delay'],
        max_delay=config['retry']['max_delay']
    )
    app['request_logger'] = RequestLogger()

    # Load balancer (optional — use if backends list is configured)
    upstream_cfg = config['upstream']
    if 'backends' in upstream_cfg:
        app['load_balancer'] = LoadBalancer(
            backends_config=upstream_cfg['backends'],
            failure_threshold=upstream_cfg.get('failure_threshold', 3),
            recovery_timeout=upstream_cfg.get('recovery_timeout', 15)
        )
        # Set base_url fallback for cache key generation etc.
        config['upstream'].setdefault('base_url', upstream_cfg['backends'][0]['url'])

    yield

    await app['cache'].close()
    await app['client_session'].close()



def create_app():
    config = load_config()
    app = web.Application()
    app['config'] = config
    
    # Lifecycle: create/destroy the HTTP client session
    app.cleanup_ctx.append(client_session_ctx)
    
    # Status endpoint — check circuit breaker state
    async def status_handler(request):
        breaker = request.app['circuit_breaker']
        req_logger = request.app['request_logger']
        resp = {
            'proxy': 'ProxyPulse',
            'circuit_breaker': breaker.stats,
            'coalescer_in_flight': request.app['coalescer'].active_count,
            'metrics': req_logger.stats,
        }
        lb = request.app.get('load_balancer')
        if lb:
            resp['load_balancer'] = lb.stats
        return web.json_response(resp)

    app.router.add_get('/_proxy/status', status_handler)

    # Catch-all route: any method, any path → proxy_handler
    app.router.add_route('*', '/{path_info:.*}', proxy_handler)
    
    return app


if __name__ == '__main__':
    app = create_app()
    config = load_config()
    print(f"ProxyPulse starting on http://{config['proxy']['host']}:{config['proxy']['port']}")
    if 'backends' in config['upstream']:
        urls = [b['url'] if isinstance(b, dict) else b for b in config['upstream']['backends']]
        print(f"Load balancing across: {', '.join(urls)}")
    else:
        print(f"Forwarding to {config['upstream']['base_url']}")
    web.run_app(app, host=config['proxy']['host'], port=config['proxy']['port'])
