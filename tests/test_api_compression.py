from starlette.middleware.gzip import GZipMiddleware

from server.api.main import app


def test_quant_platform_compresses_large_json_responses():
    middleware = [item.cls for item in app.user_middleware]

    assert GZipMiddleware in middleware
