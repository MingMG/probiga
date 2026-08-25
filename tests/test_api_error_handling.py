from __future__ import annotations

import json

from sqlalchemy.exc import OperationalError, ProgrammingError

from server.api.main import _database_error_response


def _assert_real_order_authority_disabled(payload: dict) -> None:
    assert payload["automatic_real_order_submission"] is False
    assert payload["real_order_authority"] is False


def test_operational_database_failure_remains_retryable_unavailable() -> None:
    status_code, payload = _database_error_response(
        OperationalError(
            "SELECT 1",
            {},
            ConnectionError("connection dropped"),
        )
    )

    assert status_code == 503
    assert payload["error"] == "database_unavailable"
    _assert_real_order_authority_disabled(payload)


def test_query_failure_is_not_misreported_as_database_outage() -> None:
    status_code, payload = _database_error_response(
        ProgrammingError(
            "SELECT secret_column FROM missing_table",
            {"password": "must-not-leak"},
            RuntimeError("unknown column"),
        )
    )

    assert status_code == 500
    assert payload["error"] == "database_operation_failed"
    _assert_real_order_authority_disabled(payload)
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "missing_table" not in serialized
    assert "must-not-leak" not in serialized


def test_pymysql_unknown_column_operational_error_is_not_an_outage() -> None:
    status_code, payload = _database_error_response(
        OperationalError(
            "SELECT missing_column FROM table_name",
            {},
            RuntimeError(1054, "Unknown column 'missing_column'"),
        )
    )

    assert status_code == 500
    assert payload["error"] == "database_operation_failed"
    _assert_real_order_authority_disabled(payload)
    assert "missing_column" not in json.dumps(payload, ensure_ascii=False)


def test_pymysql_connection_error_remains_database_unavailable() -> None:
    status_code, payload = _database_error_response(
        OperationalError(
            "SELECT 1",
            {},
            ConnectionError(2003, "Cannot connect to MySQL server"),
        )
    )

    assert status_code == 503
    assert payload["error"] == "database_unavailable"
    _assert_real_order_authority_disabled(payload)
