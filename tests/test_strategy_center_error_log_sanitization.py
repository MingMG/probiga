import json
import logging

from server.api.routers import strategy_center
from server.engine import strategy_center as strategy_center_engine


_PRIVATE_FAILURE = RuntimeError(
    "mysql+pymysql://admin:" + "super-secret@private-db.internal/PROBIGA "
    "path=C:\\private\\probiga\\release.env"
)


def _response_body(response):
    return json.loads(response.body.decode("utf-8"))


def test_strategy_center_snapshot_log_does_not_expose_exception_text(
    monkeypatch, caplog,
):
    monkeypatch.setattr(
        strategy_center,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(_PRIVATE_FAILURE),
    )
    monkeypatch.setattr(
        strategy_center,
        "canonical_unavailable_context",
        lambda: {"authoritative_trade_date": "", "last_canonical": {}},
    )

    with caplog.at_level(
        logging.ERROR, logger="server.api.routers.strategy_center",
    ):
        result = strategy_center.strategy_center_governance("")

    assert result["status"] == "degraded"
    assert result["automatic_real_order_submission"] is False
    assert result["real_order_authority"] is False
    assert "operation=governance_snapshot" in caplog.text
    assert "exception_type=RuntimeError" in caplog.text
    assert "super-secret" not in caplog.text
    assert "private-db.internal" not in caplog.text
    assert "release.env" not in caplog.text


def test_strategy_center_history_log_does_not_expose_exception_text(
    monkeypatch, caplog,
):
    monkeypatch.setattr(
        strategy_center,
        "governance_history_section_page",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(_PRIVATE_FAILURE),
    )

    with caplog.at_level(
        logging.ERROR, logger="server.api.routers.strategy_center",
    ):
        response = strategy_center.strategy_center_governance_history_section_page(
            section="lifecycle",
            limit=50,
            cursor="",
            entity_type="",
            entity_key="",
            action="",
            date_from="",
            date_to="",
        )

    body = _response_body(response)
    assert response.status_code == 500
    assert body["error"] == "governance_history_page_failed"
    assert body["automatic_real_order_submission"] is False
    assert body["real_order_authority"] is False
    assert "operation=governance_history_page" in caplog.text
    assert "super-secret" not in caplog.text
    assert "private-db.internal" not in caplog.text
    assert "release.env" not in caplog.text


def test_strategy_center_write_log_does_not_expose_exception_text(
    monkeypatch, caplog,
):
    monkeypatch.setattr(
        strategy_center, "_request_admin_actor", lambda *_args: "admin",
    )
    monkeypatch.setattr(
        strategy_center,
        "register_new_strategy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(_PRIVATE_FAILURE),
    )
    payload = strategy_center.StrategyRegistrationRequest(
        strategy_key="safe_test",
        strategy_name="安全日志测试",
        version="v1",
    )

    with caplog.at_level(
        logging.ERROR, logger="server.api.routers.strategy_center",
    ):
        response = strategy_center.strategy_center_register_strategy(
            payload, request=None,
        )

    body = _response_body(response)
    assert response.status_code == 500
    assert body["error"] == "strategy_registration_failed"
    assert body["automatic_real_order_submission"] is False
    assert body["real_order_authority"] is False
    assert "operation=strategy_registration" in caplog.text
    assert "exception_type=RuntimeError" in caplog.text
    assert "super-secret" not in caplog.text
    assert "private-db.internal" not in caplog.text
    assert "release.env" not in caplog.text


def test_strategy_center_engine_fallback_log_does_not_expose_exception_text(
    caplog,
):
    with caplog.at_level(
        logging.DEBUG, logger="server.engine.strategy_center",
    ):
        incident_id = strategy_center_engine._safe_fallback_log(
            logging.DEBUG, "reference_pool_file", _PRIVATE_FAILURE,
        )

    assert incident_id in caplog.text
    assert "operation=reference_pool_file" in caplog.text
    assert "exception_type=RuntimeError" in caplog.text
    assert "super-secret" not in caplog.text
    assert "private-db.internal" not in caplog.text
    assert "release.env" not in caplog.text
