from copy import deepcopy
from datetime import date, timedelta

import pytest

from tools import prepare_strategy_governance_qmt_history as preparation
from tools import attest_qmt_daily_kline as attester


def _sessions(count=120):
    start = date(2026, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _complete_result(sessions):
    daily = {
        day: {"stock_count": 2, "stock_set_hash": str(index % 10) * 64}
        for index, day in enumerate(sessions)
    }
    rows = len(sessions) * 2
    manifest = attester.build_qmt_v2_manifest(daily)
    return {
        "run_id": "qmt_attest_test",
        "status": "COMPLETED",
        "apply": True,
        "attestation_protocol": preparation.ATTESTATION_PROTOCOL_VERSION,
        "universe_manifest_schema": preparation.UNIVERSE_MANIFEST_SCHEMA,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "target_rows": rows,
        "qmt_rows": rows,
        "matched_rows": rows,
        "missing_qmt_rows": 0,
        "mismatched_rows": 0,
        "source_only_rows": 0,
        "tolerances": manifest,
        "daily_universe": daily,
    }


def _install_prepared_schema(monkeypatch):
    monkeypatch.setattr(
        preparation,
        "plan_legacy_completed_run_binding",
        lambda _engine: {
            "legacy_run_count": 0,
            "legacy_binding_plan_hash": "0" * 64,
            "legacy_binding_marker_present": False,
            "legacy_binding_pending": False,
            "legacy_bindings": [],
        },
    )
    monkeypatch.setattr(
        preparation,
        "validate_attestation_schema",
        lambda _engine: {"table_count": 4, "trigger_count": 0},
    )


def test_schema_preparation_creates_trigger_free_tables_then_validates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        preparation,
        "ensure_attestation_tables",
        lambda engine: calls.append(("ensure", engine)),
    )
    monkeypatch.setattr(
        preparation,
        "validate_attestation_schema",
        lambda engine: calls.append(("validate", engine)) or {
            "table_count": 4,
            "trigger_count": 0,
            "immutability_enforcement": "application_hashes",
        },
    )

    result = preparation.prepare_attestation_schema("engine")

    assert calls == [("ensure", "engine"), ("validate", "engine")]
    assert result == {
        "status": "ok",
        "mode": "schema-only",
        "attestation_protocol": preparation.ATTESTATION_PROTOCOL_VERSION,
        "table_count": 4,
        "trigger_count": 0,
        "database_triggers_required": False,
        "immutability_enforcement": "application_hashes",
        "automatic_real_order_submission": False,
    }


def test_prepare_requires_and_attests_exact_120_closed_sessions(monkeypatch):
    _install_prepared_schema(monkeypatch)
    sessions = _sessions()
    monkeypatch.setattr(
        preparation,
        "authoritative_closed_trade_date",
        lambda _engine: sessions[-1],
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )
    calls = []

    def fake_attester(engine, **kwargs):
        calls.append((engine, kwargs))
        return _complete_result(sessions)

    result = preparation.prepare_governance_qmt_history(
        "engine", attester=fake_attester,
    )

    assert result["status"] == "ok"
    assert result["session_count"] == 120
    assert result["target_rows"] == 240
    assert calls == [("engine", {
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "apply": True,
        "schema_prepared": True,
    })]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload, sessions: payload.update(run_id=""),
        lambda payload, sessions: payload.update(status="PARTIAL"),
        lambda payload, sessions: payload.update(apply=False),
        lambda payload, sessions: payload.update(
            attestation_protocol="LEGACY_BATCH_V1"
        ),
        lambda payload, sessions: payload.update(
            universe_manifest_schema="unknown"
        ),
        lambda payload, sessions: payload.update(qmt_rows=239),
        lambda payload, sessions: payload.update(matched_rows=239),
        lambda payload, sessions: payload.update(missing_qmt_rows=1),
        lambda payload, sessions: payload.update(mismatched_rows=1),
        lambda payload, sessions: payload.update(source_only_rows=1),
        lambda payload, sessions: payload["daily_universe"].pop(sessions[0]),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_count=0
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_count=1
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_set_hash="A" * 64
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            extra="not-canonical"
        ),
    ],
)
def test_prepare_fails_closed_for_partial_or_inexact_attestation(
    monkeypatch, mutation,
):
    _install_prepared_schema(monkeypatch)
    sessions = _sessions()
    monkeypatch.setattr(
        preparation,
        "authoritative_closed_trade_date",
        lambda _engine: sessions[-1],
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )
    payload = _complete_result(sessions)
    mutation(payload, sessions)

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="未完整覆盖120",
    ):
        preparation.prepare_governance_qmt_history(
            "engine", attester=lambda *_args, **_kwargs: deepcopy(payload),
        )


class _BindingResult:
    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _BindingConnection:
    def __init__(self, rows, marker_hash=""):
        self.rows = list(rows)
        self.marker_hash = marker_hash
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement).strip()
        params = dict(params or {})
        self.statements.append((sql, params))
        if "FROM qmt_kline_attestation_run" in sql:
            return _BindingResult(self.rows)
        if sql.startswith("SELECT migration_hash"):
            return _BindingResult(
                [] if not self.marker_hash
                else [{"migration_hash": self.marker_hash}]
            )
        if sql.startswith("INSERT INTO qmt_kline_attestation_schema_migration"):
            self.marker_hash = params["migration_hash"]
            return _BindingResult()
        raise AssertionError(sql)


class _BindingEngine:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return _BindingContext(self.connection)

    def begin(self):
        return _BindingContext(self.connection)


class _BindingContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


def _legacy_binding_row(**changes):
    row = {
        "run_id": "legacy-1",
        "provider": attester.PROVIDER_ID,
        "start_date": "2026-07-01",
        "end_date": "2026-07-24",
        "target_rows": 100,
        "qmt_rows": 101,
        "matched_rows": 100,
        "missing_qmt_rows": 0,
        "mismatched_rows": 0,
        "already_attested_rows": 0,
        "updated_rows": 100,
        "tolerance_json": attester.LEGACY_TOLERANCE_JSON,
    }
    row.update(changes)
    return row


def test_legacy_binding_planner_is_query_only_and_hashes_exact_rows():
    row = _legacy_binding_row()
    expected = attester.legacy_completed_run_binding_plan([row])
    connection = _BindingConnection([row])
    plan = preparation.plan_legacy_completed_run_binding(
        _BindingEngine(connection),
        expected_run_count=1,
        expected_plan_hash=expected["plan_hash"],
    )

    assert plan["legacy_run_count"] == 1
    assert plan["legacy_binding_pending"] is True
    assert len(plan["legacy_binding_plan_hash"]) == 64
    assert all(
        sql.upper().startswith("SELECT ")
        for sql, _params in connection.statements
    )


def test_legacy_binding_apply_only_appends_marker_and_is_idempotent(
    monkeypatch,
):
    row = _legacy_binding_row()
    expected = attester.legacy_completed_run_binding_plan([row])
    connection = _BindingConnection([row])
    engine = _BindingEngine(connection)
    monkeypatch.setattr(
        preparation,
        "validate_attestation_schema",
        lambda _connection, **_kwargs: {"errors": []},
    )

    expectation = {
        "expected_run_count": 1,
        "expected_plan_hash": expected["plan_hash"],
    }
    first = preparation.apply_legacy_completed_run_binding(
        engine,
        **expectation,
    )
    second = preparation.apply_legacy_completed_run_binding(
        engine,
        **expectation,
    )

    assert first["legacy_binding_marker_present"] is True
    assert second["legacy_binding_pending"] is False
    assert sum(
        sql.startswith("INSERT INTO qmt_kline_attestation_schema_migration")
        for sql, _params in connection.statements
    ) == 1
    assert not any(
        sql.upper().startswith("UPDATE QMT_KLINE_ATTESTATION_RUN")
        for sql, _params in connection.statements
    )


def test_production_legacy_planner_rejects_count_or_hash_drift():
    row = _legacy_binding_row()
    engine = _BindingEngine(_BindingConnection([row]))

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="冻结的计数与聚合哈希",
    ):
        preparation.plan_legacy_completed_run_binding(engine)

    expected = attester.legacy_completed_run_binding_plan([row])
    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="冻结的计数与聚合哈希",
    ):
        preparation.plan_legacy_completed_run_binding(
            engine,
            expected_run_count=1,
            expected_plan_hash="f" * 64
            if expected["plan_hash"] != "f" * 64 else "e" * 64,
        )


def test_schema_only_cli_fails_closed_and_disposes_engine(monkeypatch, capsys):
    class Engine:
        disposed = False

        def dispose(self):
            self.disposed = True

    engine = Engine()
    monkeypatch.setattr(preparation, "load_project_env", lambda: None)
    monkeypatch.setattr(
        preparation,
        "create_batch_engine",
        lambda **_kwargs: engine,
    )
    monkeypatch.setattr(
        preparation,
        "prepare_attestation_schema",
        lambda _engine: (_ for _ in ()).throw(RuntimeError("schema drift")),
    )

    assert preparation.main(["--schema-only"]) == 2
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "schema drift" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_latest_closed_sessions_requires_exact_count_and_high_watermark():
    class Result:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class Connection:
        def __init__(self, rows):
            self.rows = rows

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return Result(self.rows)

    class Engine:
        def __init__(self, rows):
            self.rows = rows

        def connect(self):
            return Connection(self.rows)

    sessions = _sessions()
    engine = Engine([{"trade_date": day} for day in reversed(sessions)])
    assert preparation._latest_closed_sessions(
        engine, target_trade_date=sessions[-1],
    ) == sessions

    short_engine = Engine([
        {"trade_date": day} for day in reversed(sessions[:-1])
    ])
    with pytest.raises(preparation.GovernanceQmtHistoryNotReady):
        preparation._latest_closed_sessions(
            short_engine, target_trade_date=sessions[-1],
        )
