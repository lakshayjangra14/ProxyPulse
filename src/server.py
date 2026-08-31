from aiohttp import web, ClientSession
import time
from src.config import load_config

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
    
    # Build the target URL
    target_url = config['upstream']['base_url'] + request.path_qs
    
    # Read the request body (needed for POST/PUT)
    body = await request.read()
    
    # Filter the incoming headers
    headers = filter_headers(request.headers)
    
    # Forward to backend and measure time
    start = time.monotonic()
    
    async with session.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=body
    ) as upstream_response:
        elapsed = time.monotonic() - start
        
        # Read the backend's response
        response_body = await upstream_response.read()
        response_headers = filter_headers(upstream_response.headers)
        
        # Add our custom headers
        response_headers['X-Proxy'] = 'ProxyPulse'
        response_headers['X-Upstream-Response-Time'] = f'{elapsed * 1000:.1f}ms'
        
        return web.Response(
            body=response_body,
            status=upstream_response.status,
            headers=response_headers
        )

async def client_session_ctx(app):
    """Create ClientSession on startup, close it on shutdown."""
    timeout_seconds = app['config']['upstream']['timeout']
    app['client_session'] = ClientSession()
    yield
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
