import logging
import re

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.main import generic_exception_handler


def test_global_500_does_not_reflect_secret_host_or_filesystem_path(caplog):
    app = FastAPI()
    app.add_exception_handler(Exception, generic_exception_handler)

    @app.get("/explode")
    async def explode():
        raise RuntimeError(
            "password=super-secret host=private-db.internal "
            "path=/etc/probiga/mysql-migrator.ini"
        )

    with caplog.at_level(logging.ERROR, logger="server.api.main"):
        response = TestClient(
            app, raise_server_exceptions=False,
        ).get("/explode")
    body = response.json()

    assert response.status_code == 500
    assert body["status"] == "error"
    assert body["error"] == "internal_server_error"
    assert body["message"] == "服务内部错误，请稍后重试。"
    assert re.fullmatch(r"[0-9a-f]{32}", body["incident_id"])
    assert body["automatic_real_order_submission"] is False
    assert body["real_order_authority"] is False
    assert "super-secret" not in response.text
    assert "private-db.internal" not in response.text
    assert "mysql-migrator.ini" not in response.text
    assert "detail" not in body
    log_text = caplog.text
    assert body["incident_id"] in log_text
    assert "exception_type=RuntimeError" in log_text
    assert "method=GET" in log_text
    assert "path=/explode" in log_text
    assert "super-secret" not in log_text
    assert "private-db.internal" not in log_text
    assert "mysql-migrator.ini" not in log_text
