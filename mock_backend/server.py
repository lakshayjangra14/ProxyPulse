from aiohttp import web
import asyncio
import json
import sys

# Fake data — pretend this came from a database
PRODUCTS = {
    "1": {"id": 1, "name": "Mechanical Keyboard", "price": 4500},
    "2": {"id": 2, "name": "Gaming Mouse", "price": 2500},
    "3": {"id": 3, "name": "USB-C Hub", "price": 1800},
}

USERS = {
    "1": {"id": 1, "name": "Arjun", "role": "backend-engineer"},
    "2": {"id": 2, "name": "Priya", "role": "frontend-engineer"},
}

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9000


async def get_products(request):
    await asyncio.sleep(0.5)  # Simulate slow DB query (500ms)
    return web.json_response(list(PRODUCTS.values()))


async def get_product(request):
    product_id = request.match_info["id"]
    await asyncio.sleep(0.3)  # 300ms delay
    product = PRODUCTS.get(product_id)
    if product:
        return web.json_response(product)
    return web.json_response({"error": "not found"}, status=404)


async def get_user(request):
    user_id = request.match_info["id"]
    await asyncio.sleep(0.2)  # 200ms delay
    user = USERS.get(user_id)
    if user:
        return web.json_response(user)
    return web.json_response({"error": "not found"}, status=404)


async def health(request):
    return web.json_response({"status": "ok"})


async def identity(request):
    """Returns which backend instance handled the request."""
    return web.json_response({"backend_port": PORT})


# Flaky endpoint — fails first 2 times, succeeds on 3rd
flaky_counter = {"count": 0}

async def flaky(request):
    flaky_counter["count"] += 1
    if flaky_counter["count"] % 3 != 0:
        return web.json_response(
            {"error": "temporary failure", "attempt": flaky_counter["count"]},
            status=503
        )
    return web.json_response(
        {"data": "success!", "attempt": flaky_counter["count"]}
    )


app = web.Application()
app.router.add_get("/api/products", get_products)
app.router.add_get("/api/products/{id}", get_product)
app.router.add_get("/api/users/{id}", get_user)
app.router.add_get("/api/identity", identity)
app.router.add_get("/api/flaky", flaky)
app.router.add_get("/health", health)

if __name__ == "__main__":
    print(f"Mock backend starting on http://localhost:{PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT)
