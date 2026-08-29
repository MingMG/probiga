from copy import deepcopy
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.common.qmt_stock_catalog import a_share_stock_code_sql
from tools import prepare_strategy_governance_qmt_history as preparation
from tools import attest_qmt_daily_kline as attester


def _sessions(count=120):
    start = date(2026, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _frozen_window(sessions):
    return {
        "expected_target_trade_date": sessions[-1],
        "expected_start_date": sessions[0],
        "expected_end_date": sessions[-1],
        "expected_session_window_sha256": preparation._session_window_sha256(
            sessions
        ),
    }


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
        "privileged_migrate_attestation_tables",
        lambda engine: calls.append(("migrate", engine)),
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

    assert calls == [("migrate", "engine"), ("validate", "engine")]
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


def test_closed_trade_date_uses_shared_clock_before_loading_weekend_receipt(
    monkeypatch,
):
    observed = {}

    def resolve(_engine, now=None):
        observed["clock_now"] = now
        return "2026-08-28"

    def load(_connection, **kwargs):
        observed["receipt"] = kwargs
        return SimpleNamespace(
            sessions_between=lambda _start, _end: ["2026-08-28"]
        )

    monkeypatch.setattr(
        preparation,
        "resolve_authoritative_closed_trade_date",
        resolve,
    )
    monkeypatch.setattr(preparation, "load_trade_calendar_receipt", load)
    engine = SimpleNamespace(connect=lambda: nullcontext(object()))

    result = preparation.authoritative_closed_trade_date(
        engine,
        now=datetime(2026, 8, 29, 9, 30),
    )

    assert result == "2026-08-28"
    assert observed["clock_now"] == datetime(2026, 8, 29, 9, 30)
    assert observed["receipt"]["end_date"] == "2026-08-28"
    assert observed["receipt"]["decision_known_at"] == datetime(
        2026, 8, 29, 9, 30
    )


def test_closed_trade_date_rejects_receipt_missing_shared_clock_target(
    monkeypatch,
):
    monkeypatch.setattr(
        preparation,
        "resolve_authoritative_closed_trade_date",
        lambda _engine, now=None: "2026-08-28",
    )
    monkeypatch.setattr(
        preparation,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: SimpleNamespace(
            sessions_between=lambda _start, _end: ["2026-08-27"]
        ),
    )
    engine = SimpleNamespace(connect=lambda: nullcontext(object()))

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="未包含共享时钟解析的目标交易日",
    ):
        preparation.authoritative_closed_trade_date(
            engine,
            now=datetime(2026, 8, 29, 9, 30),
        )


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
        "engine", attester=fake_attester, **_frozen_window(sessions),
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
            "engine",
            attester=lambda *_args, **_kwargs: deepcopy(payload),
            **_frozen_window(sessions),
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


class _ReadinessConnection:
    def __init__(self, target_rows, source_rows, exact_rows):
        self.target_rows = list(target_rows)
        self.source_rows = list(source_rows)
        self.exact_rows = list(exact_rows)
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement).strip()
        self.statements.append((sql, dict(params or {})))
        if "joined_pair_count" in sql:
            return _BindingResult(self.exact_rows)
        if "sm_stock_kline" in sql:
            return _BindingResult(self.target_rows)
        if "qmt_local_stock_kline" in sql:
            return _BindingResult(self.source_rows)
        raise AssertionError(sql)


def _readiness_rows(sessions):
    target_rows = [
        {"trade_date": day, "row_count": 2, "unique_stock_count": 2}
        for day in sessions
    ]
    source_rows = [
        {
            "trade_date": day,
            "row_count": 2,
            "unique_stock_count": 2,
            "native_row_count": 2,
        }
        for day in sessions
    ]
    exact_rows = [
        {
            "trade_date": day,
            "joined_pair_count": 2,
            "joined_target_count": 2,
            "joined_source_count": 2,
            "matched_target_count": 2,
            "matched_source_count": 2,
        }
        for day in sessions
    ]
    return target_rows, source_rows, exact_rows


def test_readiness_preflight_is_select_only_and_requires_exact_native_matches(
    monkeypatch,
):
    sessions = _sessions()
    target_rows, source_rows, exact_rows = _readiness_rows(sessions)
    connection = _ReadinessConnection(target_rows, source_rows, exact_rows)
    engine = _BindingEngine(connection)
    monkeypatch.setattr(
        preparation, "validate_attestation_schema", lambda _engine: {"errors": []}
    )
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

    result = preparation.preflight_governance_qmt_history_readiness(
        engine,
        table_resolver=lambda _engine: (
            "`probiga`.`sm_stock_kline`",
            "`probiga_qmt_history`.`qmt_local_stock_kline`",
        ),
    )

    assert result["status"] == "ok"
    assert result["mode"] == "readiness-only"
    assert result["session_count"] == 120
    assert result["target_rows"] == 240
    assert result["native_qmt_rows"] == 240
    assert result["exact_matched_rows"] == 240
    assert result["session_window_sha256"] == preparation._session_window_sha256(
        sessions
    )
    assert result["database_writes"] is False
    assert len(connection.statements) == 3
    assert all(
        sql.upper().startswith("SELECT ")
        for sql, _params in connection.statements
    )
    assert "INNER JOIN" in connection.statements[2][0]
    assert "matched_target_count" in connection.statements[2][0]
    assert "pre_close_origin" in connection.statements[2][0]
    assert "AND period='1d' AND k_type=1 AND adjust_type=0" in (
        connection.statements[1][0]
    )
    assert "AND q.period='1d' AND q.k_type=1 AND q.adjust_type=0" in (
        connection.statements[2][0]
    )
    assert a_share_stock_code_sql("stock_code") in connection.statements[0][0]
    assert a_share_stock_code_sql("stock_code") in connection.statements[1][0]
    assert a_share_stock_code_sql("t.stock_code") in connection.statements[2][0]
    assert a_share_stock_code_sql("q.stock_code") in connection.statements[2][0]
    assert all(
        "^(0|3|4|6|8|9)" not in sql
        for sql, _params in connection.statements
    )
    assert attester.QMT_ATTESTATION_COLLATION in connection.statements[2][0]
    assert all(
        params == {
            "provider": attester.PROVIDER_ID,
            "start_date": sessions[0],
            "end_date": sessions[-1],
            "price_tolerance": attester.PRICE_TOLERANCE,
            "volume_absolute_tolerance": attester.VOLUME_ABSOLUTE_TOLERANCE,
            "volume_rel_tolerance": attester.VOLUME_REL_TOLERANCE,
            "amount_rel_tolerance": attester.AMOUNT_REL_TOLERANCE,
        }
        for _sql, params in connection.statements
    )


@pytest.mark.parametrize("failure", ["missing_day", "count", "provenance"])
def test_readiness_preflight_blocks_incomplete_source_before_cutover(
    monkeypatch, failure,
):
    sessions = _sessions()
    target_rows, source_rows, exact_rows = _readiness_rows(sessions)
    if failure == "missing_day":
        source_rows.pop()
    elif failure == "count":
        source_rows[-1]["row_count"] = 1
        source_rows[-1]["native_row_count"] = 1
    else:
        source_rows[-1]["native_row_count"] = 1
    engine = _BindingEngine(
        _ReadinessConnection(target_rows, source_rows, exact_rows)
    )
    monkeypatch.setattr(
        preparation, "validate_attestation_schema", lambda _engine: {"errors": []}
    )
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

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="拒绝进入停服务切换",
    ):
        preparation.preflight_governance_qmt_history_readiness(
            engine,
            table_resolver=lambda _engine: (
                "`probiga`.`sm_stock_kline`",
                "`probiga_qmt_history`.`qmt_local_stock_kline`",
            ),
        )


def test_readiness_preflight_blocks_equal_count_stock_substitution(monkeypatch):
    sessions = _sessions()
    target_rows, source_rows, exact_rows = _readiness_rows(sessions)
    # Target and source still each contain two rows, but only one stock key is
    # shared.  Daily count-only readiness would incorrectly accept this.
    exact_rows[-1].update(
        joined_pair_count=1,
        joined_target_count=1,
        joined_source_count=1,
        matched_target_count=1,
        matched_source_count=1,
    )
    engine = _BindingEngine(
        _ReadinessConnection(target_rows, source_rows, exact_rows)
    )
    monkeypatch.setattr(
        preparation, "validate_attestation_schema", lambda _engine: {"errors": []}
    )
    monkeypatch.setattr(
        preparation, "authoritative_closed_trade_date", lambda _engine: sessions[-1]
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="相同股票集合",
    ):
        preparation.preflight_governance_qmt_history_readiness(
            engine,
            table_resolver=lambda _engine: (
                "`probiga`.`sm_stock_kline`",
                "`probiga_qmt_history`.`qmt_local_stock_kline`",
            ),
        )


def test_readiness_preflight_blocks_ohlcv_value_mismatch(monkeypatch):
    sessions = _sessions()
    target_rows, source_rows, exact_rows = _readiness_rows(sessions)
    # The universe joins exactly, but one row fails the formal frozen OHLCV
    # tolerance predicate returned by the production attester's match SQL.
    exact_rows[-1].update(matched_target_count=1, matched_source_count=1)
    engine = _BindingEngine(
        _ReadinessConnection(target_rows, source_rows, exact_rows)
    )
    monkeypatch.setattr(
        preparation, "validate_attestation_schema", lambda _engine: {"errors": []}
    )
    monkeypatch.setattr(
        preparation, "authoritative_closed_trade_date", lambda _engine: sessions[-1]
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="OHLC/量额",
    ):
        preparation.preflight_governance_qmt_history_readiness(
            engine,
            table_resolver=lambda _engine: (
                "`probiga`.`sm_stock_kline`",
                "`probiga_qmt_history`.`qmt_local_stock_kline`",
            ),
        )


@pytest.mark.parametrize("drift", ["target", "sessions"])
def test_prepare_blocks_frozen_window_drift_before_attestation(monkeypatch, drift):
    _install_prepared_schema(monkeypatch)
    sessions = _sessions()
    observed_sessions = list(sessions)
    observed_target = sessions[-1]
    if drift == "target":
        observed_target = (date.fromisoformat(sessions[-1]) + timedelta(days=1)).isoformat()
    else:
        observed_sessions[50], observed_sessions[51] = (
            observed_sessions[51],
            observed_sessions[50],
        )
    monkeypatch.setattr(
        preparation,
        "authoritative_closed_trade_date",
        lambda _engine: observed_target,
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: observed_sessions,
    )
    calls = []

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="窗口",
    ):
        preparation.prepare_governance_qmt_history(
            "engine",
            attester=lambda *_args, **_kwargs: calls.append(True),
            **_frozen_window(sessions),
        )

    assert calls == []


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


def test_readiness_cli_uses_and_disposes_protected_windows_engines(
    monkeypatch,
    capsys,
):
    from tools import backfill_guojin_qmt_local_history as backfill_tool

    class Engine:
        def __init__(self, name):
            self.name = name
            self.disposed = False

        def dispose(self):
            self.disposed = True

    primary = Engine("primary")
    history = Engine("history")
    events = []
    monkeypatch.setattr(preparation, "load_project_env", lambda: None)
    monkeypatch.setattr(
        preparation,
        "create_batch_engine",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("configured production engine must not be opened")
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda: (primary, history),
    )
    monkeypatch.setattr(
        preparation,
        "_table_names",
        lambda engine, *, local_history_engine: events.append(
            (engine, local_history_engine)
        )
        or ("target", "source"),
    )

    def fake_readiness(engine, *, table_resolver):
        assert table_resolver(engine) == ("target", "source")
        return {"status": "ready", "mode": "readiness-only"}

    monkeypatch.setattr(
        preparation,
        "preflight_governance_qmt_history_readiness",
        fake_readiness,
    )

    assert preparation.main(
        ["--readiness-only", "--windows-local-option-file"]
    ) == 0
    assert events == [(primary, history)]
    assert primary.disposed is True
    assert history.disposed is True
    assert __import__("json").loads(capsys.readouterr().out)["status"] == "ready"


def test_windows_option_file_is_rejected_outside_readiness(capsys):
    with pytest.raises(SystemExit) as exc_info:
        preparation.main(["--schema-only", "--windows-local-option-file"])

    assert exc_info.value.code == 2
    assert "only valid with --readiness-only" in capsys.readouterr().err


def test_latest_closed_sessions_requires_exact_count_and_high_watermark(
    monkeypatch,
):
    class Connection:
        def __init__(self, rows):
            self.rows = rows

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Engine:
        def __init__(self, rows):
            self.rows = rows

        def connect(self):
            return Connection(self.rows)

    class Receipt:
        def __init__(self, rows):
            self.sessions = sorted(
                str(row["trade_date"]) for row in rows
            )

        def sessions_between(self, start_date, end_date):
            return [
                day for day in self.sessions
                if start_date <= day <= end_date
            ]

    monkeypatch.setattr(
        preparation,
        "load_trade_calendar_receipt",
        lambda connection, **_kwargs: Receipt(connection.rows),
    )

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
