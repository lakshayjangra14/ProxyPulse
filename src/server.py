from aiohttp import web, ClientSession
import time
from src.config import load_config
from src.cache import Cache
from src.coalescer import Coalescer   


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
    config = request.app['config']
    session = request.app['client_session']
    cache = request.app['cache']
    coalescer = request.app['coalescer']

    target_url = config['upstream']['base_url'] + request.path_qs

    # Only cache GET requests
    if request.method == 'GET':
        cache_key = cache.generate_key(request.method, request.path_qs)

        # Step 1: Check cache
        cached = await cache.get(cache_key)
        if cached:
            response_headers = cached['headers']
            response_headers['X-Proxy'] = 'ProxyPulse'
            response_headers['X-Cache'] = 'HIT'
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
            return web.Response(
                body=result['body'],
                status=result['status'],
                headers=response_headers
            )

        # Step 3: We're the first — register and fetch
        coalescer.register(cache_key)

    # Forward to backend
    body = await request.read()
    headers = filter_headers(request.headers)
    start = time.monotonic()

    try:
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=body
        ) as upstream_response:
            elapsed = time.monotonic() - start

            response_body = await upstream_response.read()
            response_headers = filter_headers(upstream_response.headers)

            # Cache and resolve if GET + 200
            if request.method == 'GET' and upstream_response.status == 200:
                await cache.set(
                    cache_key,
                    upstream_response.status,
                    response_headers,
                    response_body
                )
                coalescer.resolve(cache_key, {
                    'status': upstream_response.status,
                    'headers': response_headers,
                    'body': response_body
                })

            response_headers['X-Proxy'] = 'ProxyPulse'
            response_headers['X-Cache'] = 'MISS'
            response_headers['X-Upstream-Response-Time'] = f'{elapsed * 1000:.1f}ms'

            return web.Response(
                body=response_body,
                status=upstream_response.status,
                headers=response_headers
            )
    except Exception as e:
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

    yield

    await app['cache'].close()
    await app['client_session'].close()



def create_app():
    config = load_config()
    app = web.Application()
    app['config'] = config
    
    # Lifecycle: create/destroy the HTTP client session
    app.cleanup_ctx.append(client_session_ctx)
    
    # Catch-all route: any method, any path → proxy_handler
    app.router.add_route('*', '/{path_info:.*}', proxy_handler)
    
    return app


if __name__ == '__main__':
    app = create_app()
    config = load_config()
    print(f"ProxyPulse starting on http://{config['proxy']['host']}:{config['proxy']['port']}")
    print(f"Forwarding to {config['upstream']['base_url']}")
    web.run_app(app, host=config['proxy']['host'], port=config['proxy']['port'])
