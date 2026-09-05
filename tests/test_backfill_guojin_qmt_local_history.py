from __future__ import annotations

import inspect
import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from integrations.qmt import local_history
from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_MINUTE_TABLE,
    LocalBackfillBatchResult,
    LocalBackfillResult,
)
from tools import backfill_guojin_qmt_local_history as backfill_tool


TEST_DAILY_LOCK = backfill_tool.Path(
    r"C:\ProgramData\ProBigA\qmt-local-gap-repair\qmt-local-daily-backfill.lock"
)


def test_governance_backfill_queries_share_the_exact_a_share_predicate():
    functions = (
        backfill_tool._target_window_unattestable_codes,
        backfill_tool._repair_target_source_only_rows,
        backfill_tool._quarantine_invalid_target_rows_without_native,
        backfill_tool._quarantine_source_only_legacy_rows,
    )

    for function in functions:
        source = inspect.getsource(function)
        assert "a_share_stock_code_sql" in source
        assert "^(0|3|4|6|8|9)" not in source


@pytest.fixture(autouse=True)
def _ready_target_quarantine_schema(monkeypatch):
    def validate(engine):
        with engine.begin() as connection:
            connection.execute(backfill_tool.text(
                "SELECT id, run_id, original_id, action, reason, "
                "native_provider, stock_code, trade_date, k_type, "
                "adjust_type, row_payload, row_sha256, quarantined_at, "
                "restored_at, restore_run_id "
                "FROM `probiga`.`qmt_target_daily_quarantine` WHERE 1=0"
            ))
        return {"ready": True, "ddl_executed": False}

    monkeypatch.setattr(
        backfill_tool,
        "_validate_target_daily_quarantine_table",
        validate,
    )


def _patch_daily_lock_path(monkeypatch):
    monkeypatch.setattr(
        backfill_tool,
        "_validated_daily_backfill_lock_path",
        lambda: (TEST_DAILY_LOCK.parent, TEST_DAILY_LOCK),
    )


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _SequenceConnection:
    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), dict(params or {})))
        return _Rows(self.result_sets.pop(0))


class _SequenceEngine:
    def __init__(self, result_sets):
        self.connection = _SequenceConnection(result_sets)

    def begin(self):
        return nullcontext(self.connection)


class _DisposableEngine:
    def __init__(self, database):
        self.url = make_url(f"mysql+pymysql:///{database}")
        self.disposed = False

    def dispose(self):
        self.disposed = True


def _result(
    *,
    code_count=2,
    fetched_rows=2,
    written_rows=0,
    allowed_missing_codes=(),
):
    return LocalBackfillResult(
        run_id="run-1",
        dataset=LOCAL_KLINE_TABLE,
        status="SUCCESS",
        local_database="probiga_qmt_history",
        start_date="2026-08-19",
        end_date="2026-08-19",
        code_count=code_count,
        batch_count=1,
        fetched_rows=fetched_rows,
        written_rows=written_rows,
        batches=[
            LocalBackfillBatchResult(
                dataset=LOCAL_KLINE_TABLE,
                period="1d",
                start_date="2026-08-19",
                end_date="2026-08-19",
                requested_codes=code_count,
                fetched_rows=fetched_rows,
                written_rows=written_rows,
                skipped=written_rows == 0,
                allowed_missing_codes=tuple(allowed_missing_codes),
            )
        ],
    )


def test_positive_rows_cannot_resolve_a_partial_or_unattested_gap():
    partial_minute = LocalBackfillResult(
        run_id="run-partial",
        dataset=LOCAL_MINUTE_TABLE,
        status="SUCCESS",
        local_database="probiga_qmt_history",
        start_date="2026-08-19",
        end_date="2026-08-19",
        code_count=80,
        batch_count=1,
        fetched_rows=1,
        written_rows=1,
        batches=[],
        coverage_status="PARTIAL",
    )

    assert backfill_tool._result_proves_exact_gap_coverage(
        dataset="sm_stock_minute.1m",
        result=partial_minute,
        authoritative_codes=[f"{code:06d}" for code in range(80)],
        trade_dates=["2026-08-19"],
    ) is False
    assert backfill_tool._result_proves_exact_gap_coverage(
        dataset="sm_stock_kline.1d",
        result=_result(written_rows=1),
        authoritative_codes=["000001", "600000"],
        trade_dates=["2026-08-19"],
    ) is False


def test_target_window_universe_uses_exact_a_share_union_and_dates(
    monkeypatch,
):
    engine = _SequenceEngine([])
    monkeypatch.setattr(
        backfill_tool,
        "load_stock_catalog",
        lambda _connection, **_kwargs: SimpleNamespace(
            manifest_hash="a" * 64,
            eligible_codes=lambda day: (
                ["000001", "600000"]
                if day == "2026-08-18"
                else ["000001", "300001"]
            )
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: SimpleNamespace(
            manifest_hash="b" * 64,
            sessions_between=lambda _start, _end: [
                "2026-08-18", "2026-08-19"
            ]
        ),
    )

    codes, trade_dates, source_batch_id = backfill_tool._target_window_codes(
        engine,
        start_date="20260818",
        end_date="2026-08-19",
    )

    assert codes == ["000001", "300001", "600000"]
    assert trade_dates == ["2026-08-18", "2026-08-19"]
    assert source_batch_id == backfill_tool.daily_market_source_batch_id(
        catalog_manifest_hash="a" * 64,
        calendar_manifest_hash="b" * 64,
    )
    assert engine.connection.statements == []


def test_target_window_universe_rejects_empty_catalog_day(monkeypatch):
    engine = _SequenceEngine([])
    monkeypatch.setattr(
        backfill_tool,
        "load_stock_catalog",
        lambda _connection, **_kwargs: SimpleNamespace(
            eligible_codes=lambda day: (
                ["000001"] if day == "2026-08-18" else []
            )
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: SimpleNamespace(
            sessions_between=lambda _start, _end: [
                "2026-08-18", "2026-08-19"
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="target universe is empty"):
        backfill_tool._target_window_codes(
            engine,
            start_date="2026-08-18",
            end_date="2026-08-19",
        )


@pytest.mark.parametrize(
    "raw_codes, expected",
    [
        ("002231", ["002231"]),
        ("603056,002231", ["002231", "603056"]),
        ("", []),
    ],
)
def test_exact_lifecycle_no_row_codes_are_explicit_and_canonical(
    raw_codes,
    expected,
):
    assert backfill_tool._exact_lifecycle_no_row_codes(raw_codes) == expected


@pytest.mark.parametrize(
    "raw_codes",
    [
        "002231.SZ",
        " 002231",
        "002231 ",
        "002231,002231",
        "123",
        "002231,,300344",
        ",002231",
        "002231,",
        ",".join(f"{index:06d}" for index in range(33)),
    ],
)
def test_exact_lifecycle_no_row_codes_reject_ambiguous_or_broad_input(
    raw_codes,
):
    with pytest.raises(ValueError, match="unique exact six-digit"):
        backfill_tool._exact_lifecycle_no_row_codes(raw_codes)


def test_not_yet_listed_no_row_codes_are_exact_reviewed_subset_only():
    assert backfill_tool._not_yet_listed_no_row_codes(
        "301699,301688"
    ) == ["301688", "301699"]
    with pytest.raises(ValueError, match="allowed"):
        backfill_tool._not_yet_listed_no_row_codes("688835")
    with pytest.raises(ValueError, match="unique exact six-digit"):
        backfill_tool._not_yet_listed_no_row_codes("301688.SZ")


def _exact_no_row_catalog(*, list_date="2008-05-12", expire_date="2026-03-26"):
    member = {
        "stock_code": "002231",
        "qmt_code": "002231.SZ",
        "list_date": list_date,
        "expire_date": expire_date,
    }

    def eligible_codes(trade_date):
        if expire_date in (None, ""):
            active = list_date <= trade_date
        else:
            active = list_date <= trade_date <= expire_date
        return ["002231"] if active else []

    return SimpleNamespace(
        batch_id="catalog-batch",
        member_set_hash="c" * 64,
        manifest_hash="a" * 64,
        members=(member,),
        eligible_codes=eligible_codes,
    )


def _exact_no_row_calendar():
    sessions = [
        "2026-03-06",
        "2026-03-09",
        "2026-03-25",
        "2026-03-26",
        "2026-08-27",
    ]
    return SimpleNamespace(
        batch_id="calendar-batch",
        session_set_hash="d" * 64,
        manifest_hash="b" * 64,
        known_at="2026-08-27 18:00:00",
        sessions_between=lambda _start, _end: list(sessions),
    )


def _exact_no_row_engine(*, target_rows=0, history_rows=0):
    statements = []

    class _Result:
        def mappings(self):
            return self

        def one(self):
            return {
                "target_rows": target_rows,
                "history_rows": history_rows,
            }

    class _Connection:
        def execute(self, statement, params=None):
            statements.append((str(statement), dict(params or {})))
            return _Result()

    class _Engine:
        def begin(self):
            return nullcontext(_Connection())

    engine = _Engine()
    engine.statements = statements
    return engine


def test_exact_lifecycle_no_row_proof_binds_catalog_calendar_and_zero_rows(
    monkeypatch,
):
    engine = _exact_no_row_engine()
    monkeypatch.setattr(
        backfill_tool,
        "load_stock_catalog",
        lambda _connection, **_kwargs: _exact_no_row_catalog(),
    )
    monkeypatch.setattr(
        backfill_tool,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: _exact_no_row_calendar(),
    )

    first = backfill_tool._prove_exact_lifecycle_no_row_codes(
        engine,
        stock_codes=["002231"],
        start_date="2026-03-06",
        end_date="2026-08-27",
    )
    second = backfill_tool._prove_exact_lifecycle_no_row_codes(
        engine,
        stock_codes=["002231"],
        start_date="2026-03-06",
        end_date="2026-08-27",
    )

    assert first == second
    assert first["schema"] == (
        "probiga.qmt-daily-no-row-exceptions.v1"
    )
    assert first["exact_lifecycle_no_row_codes"] == ["002231"]
    assert first["not_yet_listed_no_row_codes"] == []
    assert first["entities"] == [
        {
            "category": "EXACT_LIFECYCLE_NO_ROW",
            "stock_code": "002231",
            "qmt_code": "002231.SZ",
            "list_date": "2008-05-12",
            "expire_date": "2026-03-26",
            "affected_trade_dates": [
                "2026-03-06", "2026-03-09", "2026-03-25",
                "2026-03-26",
            ],
            "affected_trade_dates_sha256": first["entities"][0][
                "affected_trade_dates_sha256"
            ],
            "target_rows": 0,
            "history_rows": 0,
        }
    ]
    assert len(first["proof_sha256"]) == 64
    assert len(first["entities"][0]["affected_trade_dates_sha256"]) == 64
    sql, params = engine.statements[0]
    assert "`probiga`.`sm_stock_kline`" in sql
    assert "`probiga_qmt_history`.`qmt_local_stock_kline`" in sql
    assert params == {
        "stock_code": "002231",
        "start_date": "2026-03-06",
        "end_date": "2026-08-27",
    }


@pytest.mark.parametrize(
    "list_date, expire_date, target_rows, history_rows, error",
    [
        ("1970-01-01", "2026-03-26", 0, 0, "finite in-window"),
        ("2008-05-12", None, 0, 0, "finite in-window"),
        ("2008-05-12", "2026-09-01", 0, 0, "finite in-window"),
        ("2008-05-12", "2026-03-26", 1, 0, "already has daily rows"),
        ("2008-05-12", "2026-03-26", 0, 1, "already has daily rows"),
    ],
)
def test_exact_lifecycle_no_row_proof_fails_closed(
    monkeypatch,
    list_date,
    expire_date,
    target_rows,
    history_rows,
    error,
):
    monkeypatch.setattr(
        backfill_tool,
        "load_stock_catalog",
        lambda _connection, **_kwargs: _exact_no_row_catalog(
            list_date=list_date,
            expire_date=expire_date,
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: _exact_no_row_calendar(),
    )

    with pytest.raises(RuntimeError, match=error):
        backfill_tool._prove_exact_lifecycle_no_row_codes(
            _exact_no_row_engine(
                target_rows=target_rows,
                history_rows=history_rows,
            ),
            stock_codes=["002231"],
            start_date="2026-03-06",
            end_date="2026-08-27",
        )


def test_target_window_unattestable_codes_requires_all_pre_close_invalid():
    engine = _SequenceEngine([[('688693',), ('000001',)]])

    codes = backfill_tool._target_window_unattestable_codes(
        engine,
        start_date="20260316",
        end_date="2026-03-27",
    )

    assert codes == ["000001", "688693"]
    sql, params = engine.connection.statements[0]
    assert "GROUP BY stock_code" in sql
    assert "HAVING SUM" in sql
    assert "pre_close IS NOT NULL AND pre_close > 0" in sql
    assert "volume=0" in sql
    assert "pre_close=`close`" in sql
    assert "adjust_type=0" in sql
    assert params == {
        "start_date": "2026-03-16",
        "end_date": "2026-03-27",
    }


def test_universe_proof_is_sorted_deduplicated_and_stable():
    first = backfill_tool._universe_proof(
        ["600000", "000001", "000001"],
        source=backfill_tool.TARGET_WINDOW_UNIVERSE_SOURCE,
        start_date="20260818",
        end_date="20260819",
        target_trade_dates=["2026-08-18", "2026-08-19"],
    )
    second = backfill_tool._universe_proof(
        ["000001", "600000"],
        source=backfill_tool.TARGET_WINDOW_UNIVERSE_SOURCE,
        start_date="2026-08-18",
        end_date="2026-08-19",
        target_trade_dates=["2026-08-18", "2026-08-19"],
    )

    assert first == second
    assert first["stock_count"] == 2
    assert first["target_trade_date_count"] == 2
    assert len(first["stock_codes_sha256"]) == 64


def _patch_repair_market_roots(monkeypatch):
    monkeypatch.setattr(
        backfill_tool,
        "load_stock_catalog",
        lambda *_a, **_k: SimpleNamespace(
            batch_id="catalog-1", manifest_hash="a" * 64,
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "load_trade_calendar_receipt",
        lambda *_a, **_k: SimpleNamespace(
            batch_id="calendar-1", manifest_hash="b" * 64,
        ),
    )


def test_target_source_only_repair_inserts_missing_native_rows_only(
    monkeypatch,
):
    _patch_repair_market_roots(monkeypatch)
    class _Result:
        def __init__(self, *, scalar_value=None, rowcount=0):
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(scalar_value=556),
                _Result(rowcount=556),
                _Result(scalar_value=0),
            ]
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append((str(statement), dict(params or {})))
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    engine = _Engine()
    result = backfill_tool._repair_target_source_only_rows(
        engine,
        start_date="20260302",
        end_date="2026-03-13",
        provider="gj_big_qmt_inner",
    )

    assert result == {
        "status": "APPLIED",
        "provider": "gj_big_qmt_inner",
        "start_date": "2026-03-02",
        "end_date": "2026-03-13",
        "catalog_batch_id": "catalog-1",
        "catalog_manifest_hash": "a" * 64,
        "calendar_batch_id": "calendar-1",
        "calendar_manifest_hash": "b" * 64,
        "source_only_before": 556,
        "inserted_rows": 556,
        "source_only_after": 0,
        "existing_rows_updated": 0,
    }
    statements = "\n".join(sql for sql, _params in engine.connection.statements)
    assert "INSERT INTO `probiga`.`sm_stock_kline`" in statements
    assert "`probiga_qmt_history`.`qmt_local_stock_kline`" in statements
    assert (
        "t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci"
        in statements
    )
    assert "BINARY s.pre_close_origin=BINARY 'NATIVE_QMT'" in statements
    assert (
        "member.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci"
        in statements
    )
    assert "member.expire_date>=s.trade_date" in statements
    assert "qmt_trade_calendar_session" in statements
    assert "t.id IS NULL" in statements
    assert "UPDATE" not in statements.upper()
    assert all(
        params == {
            "start_date": "2026-03-02",
            "end_date": "2026-03-13",
            "provider": "gj_big_qmt_inner",
            "catalog_batch_id": "catalog-1",
            "calendar_batch_id": "calendar-1",
        }
        for _sql, params in engine.connection.statements
    )


def test_target_source_only_repair_fails_closed_on_count_drift(monkeypatch):
    _patch_repair_market_roots(monkeypatch)
    class _Result:
        def __init__(self, *, scalar_value=None, rowcount=0):
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(scalar_value=2),
                _Result(rowcount=1),
                _Result(scalar_value=1),
            ]

        def execute(self, _statement, _params=None):
            return self.results.pop(0)

    class _Engine:
        def begin(self):
            return nullcontext(_Connection())

    with pytest.raises(RuntimeError, match="target repair is incomplete"):
        backfill_tool._repair_target_source_only_rows(
            _Engine(),
            start_date="2026-03-02",
            end_date="2026-03-13",
            provider="gj_big_qmt_inner",
        )


def test_target_source_only_repair_cli_requires_strict_local_apply(capsys):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(["daily", "--repair-target-source-only"])

    assert exc_info.value.code == 2
    assert "requires daily mode" in capsys.readouterr().err


def test_invalid_target_quarantine_preserves_full_rows_before_delete():
    class _Result:
        def __init__(self, *, rows=None, scalar_value=None, rowcount=0):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(),
                _Result(
                    rows=[
                        {
                            "id": 71,
                            "stock_code": "688693",
                            "short_name": "test",
                            "trade_time": "2026-03-16 15:00:00",
                            "trade_date": "2026-03-16",
                            "k_type": 1,
                            "adjust_type": 0,
                            "open": "45.75",
                            "close": "45.75",
                            "high": "45.75",
                            "low": "45.75",
                            "volume": "0",
                            "amount": "0",
                            "change": None,
                            "change_pct": None,
                            "turnover_ratio": None,
                            "pre_close": "0",
                            "etl_sync_at": "2026-08-23 20:00:00",
                            "qmt_code": "688693.SH",
                            "data_source": "gj_big_qmt_inner",
                            "source_time": "2026-03-16 15:00:00",
                            "received_at": "2026-08-23 20:00:00",
                            "batch_id": "old-run",
                            "data_version": "old-version",
                            "quality_status": "QMT_ATTESTED",
                            "permission_status": "SUPPORTED",
                        },
                        {
                            "id": 72,
                            "stock_code": "300955",
                            "short_name": "suspended",
                            "trade_time": "2026-03-17 15:00:00",
                            "trade_date": "2026-03-17",
                            "k_type": 1,
                            "adjust_type": 0,
                            "open": "32.68",
                            "close": "32.68",
                            "high": "32.68",
                            "low": "32.68",
                            "volume": "0",
                            "amount": "0",
                            "change": "0",
                            "change_pct": "0",
                            "turnover_ratio": "0",
                            "pre_close": "32.68",
                            "etl_sync_at": "2026-08-23 20:00:00",
                            "qmt_code": "300955.SZ",
                            "data_source": "gj_big_qmt_inner",
                            "source_time": "2026-03-17 15:00:00",
                            "received_at": "2026-08-23 20:00:00",
                            "batch_id": "old-synthetic-run",
                            "data_version": "old-synthetic-version",
                            "quality_status": "QMT_ATTESTED",
                            "permission_status": "SUPPORTED",
                        },
                    ]
                ),
                _Result(scalar_value=0),
                _Result(scalar_value=0),
                _Result(scalar_value=0),
                _Result(scalar_value=0),
                _Result(rowcount=2),
                _Result(rowcount=2),
                _Result(scalar_value=0),
                _Result(scalar_value=0),
                _Result(rowcount=1),
            ]
            self.statements = []
            self.begin_count = 0

        def execute(self, statement, params=None):
            self.statements.append((str(statement), params))
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            self.connection.begin_count += 1
            return nullcontext(self.connection)

    engine = _Engine()
    result = backfill_tool._quarantine_invalid_target_rows_without_native(
        engine,
        start_date="20260316",
        end_date="2026-03-27",
        provider="gj_big_qmt_inner",
    )

    assert result["status"] == "APPLIED"
    assert result["selected_rows"] == 2
    assert result["candidate_target_rows_checked"] == 2
    assert result["invalid_target_rows_checked"] == 1
    assert result["synthetic_target_rows_checked"] == 1
    assert result["native_backed_invalid_rows"] == 0
    assert result["native_backed_synthetic_rows"] == 0
    assert result["audit_copied_rows"] == 2
    assert result["deleted_rows"] == 2
    assert result["remaining_rows"] == 0
    assert result["recoverable"] is True
    assert result["existing_valid_target_rows_updated"] == 0
    assert len(result["row_set_sha256"]) == 64
    assert engine.connection.begin_count == 2

    statements = [sql for sql, _params in engine.connection.statements]
    combined = "\n".join(statements)
    assert "WHERE 1=0" in statements[0]
    assert "CREATE TABLE" not in combined
    assert "ALTER TABLE" not in combined
    assert "FOR UPDATE" in combined
    assert "NOT EXISTS" in combined
    assert "BINARY s.pre_close_origin=BINARY 'NATIVE_QMT'" in combined
    assert "INSERT INTO `probiga`.`qmt_target_daily_quarantine`" in combined
    assert "DELETE t" in combined
    assert "sm_stock_kline_target_quarantine" in statements[-1]
    assert combined.index("INSERT INTO `probiga`.`qmt_target_daily_quarantine`") < (
        combined.index("DELETE t")
    )

    quarantine_params = next(
        params
        for sql, params in engine.connection.statements
        if "INSERT INTO `probiga`.`qmt_target_daily_quarantine`" in sql
    )
    assert isinstance(quarantine_params, list)
    assert len(quarantine_params) == 2
    by_id = {item["original_id"]: item for item in quarantine_params}
    preserved = json.loads(by_id[71]["row_payload"])
    assert preserved["id"] == 71
    assert preserved["stock_code"] == "688693"
    assert preserved["pre_close"] == "0"
    assert preserved["data_source"] == "gj_big_qmt_inner"
    assert len(by_id[71]["row_sha256"]) == 64
    assert by_id[71]["action"] == (
        backfill_tool.TARGET_INVALID_QUARANTINE_ACTION
    )
    synthetic = json.loads(by_id[72]["row_payload"])
    assert synthetic["pre_close"] == "32.68"
    assert synthetic["volume"] == "0"
    assert by_id[72]["action"] == (
        backfill_tool.TARGET_SYNTHETIC_QUARANTINE_ACTION
    )

    audit_params = engine.connection.statements[-1][1]
    audit = json.loads(audit_params["extra_json"])
    assert audit["recoverable"] is True
    assert audit["full_row_payload_preserved"] is True
    assert audit["existing_valid_target_rows_updated"] == 0
    assert audit["selected_action_counts"] == {
        backfill_tool.TARGET_INVALID_QUARANTINE_ACTION: 1,
        backfill_tool.TARGET_SYNTHETIC_QUARANTINE_ACTION: 1,
    }


def test_invalid_target_quarantine_fails_closed_on_count_drift():
    class _Result:
        def __init__(self, *, rows=None, scalar_value=None, rowcount=0):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(),
                _Result(
                    rows=[
                        {
                            "id": 71,
                            "stock_code": "688693",
                            "trade_date": "2026-03-16",
                            "k_type": 1,
                            "adjust_type": 0,
                        }
                    ]
                ),
                _Result(scalar_value=0),
                _Result(scalar_value=0),
                _Result(rowcount=1),
                _Result(rowcount=0),
                _Result(scalar_value=1),
            ]

        def execute(self, _statement, _params=None):
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    with pytest.raises(RuntimeError, match="quarantine is incomplete"):
        backfill_tool._quarantine_invalid_target_rows_without_native(
            _Engine(),
            start_date="2026-03-16",
            end_date="2026-03-27",
            provider="gj_big_qmt_inner",
        )


def test_target_quarantine_fails_closed_above_bounded_row_limit():
    class _Result:
        def __init__(self, *, rows=None):
            self.rows = list(rows or [])
            self.rowcount = 0

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return 0

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(),
                _Result(
                    rows=[
                        {
                            "id": offset,
                            "stock_code": str(offset).zfill(6),
                            "trade_date": "2026-03-16",
                            "pre_close": 0,
                        }
                        for offset in range(3)
                    ]
                ),
                _Result(),
                _Result(),
                _Result(),
            ]

        def execute(self, _statement, _params=None):
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    with pytest.raises(RuntimeError, match="exceeds the bounded row limit"):
        backfill_tool._quarantine_invalid_target_rows_without_native(
            _Engine(),
            start_date="2026-03-16",
            end_date="2026-03-27",
            provider="gj_big_qmt_inner",
            max_rows=2,
        )


def test_invalid_target_quarantine_keeps_rows_with_valid_native_source():
    class _Result:
        def __init__(self, *, rows=None, scalar_value=None, rowcount=0):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(),
                _Result(
                    rows=[
                        {
                            "id": 72,
                            "stock_code": "301682",
                            "trade_date": "2026-03-25",
                            "k_type": 1,
                            "adjust_type": 0,
                        }
                    ]
                ),
                _Result(scalar_value=9001),
                _Result(rowcount=1),
            ]
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append((str(statement), params))
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    engine = _Engine()
    result = backfill_tool._quarantine_invalid_target_rows_without_native(
        engine,
        start_date="2026-03-16",
        end_date="2026-03-27",
        provider="gj_big_qmt_inner",
    )

    assert result["invalid_target_rows_checked"] == 1
    assert result["native_backed_invalid_rows"] == 1
    assert result["selected_rows"] == 0
    assert result["deleted_rows"] == 0
    combined = "\n".join(sql for sql, _params in engine.connection.statements)
    assert "FORCE INDEX (uk_qmt_local_kline)" in combined
    assert "DELETE t" not in combined


def test_invalid_target_quarantine_stops_before_delete_on_audit_conflict():
    class _Result:
        def __init__(self, *, rows=None, scalar_value=None):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = 0

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(),
                _Result(
                    rows=[
                        {
                            "id": 71,
                            "stock_code": "688693",
                            "trade_date": "2026-03-16",
                            "k_type": 1,
                            "adjust_type": 0,
                        }
                    ]
                ),
                _Result(scalar_value=0),
                _Result(scalar_value=1),
            ]
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append((str(statement), params))
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    engine = _Engine()
    with pytest.raises(RuntimeError, match="existing audit copy"):
        backfill_tool._quarantine_invalid_target_rows_without_native(
            engine,
            start_date="2026-03-16",
            end_date="2026-03-27",
            provider="gj_big_qmt_inner",
        )

    combined = "\n".join(sql for sql, _params in engine.connection.statements)
    assert "DELETE t" not in combined
    assert "sm_stock_kline_target_quarantine'," not in combined


def test_invalid_target_quarantine_cli_requires_full_strict_chain(capsys):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(
            ["daily", "--quarantine-invalid-target-no-native"]
        )

    assert exc_info.value.code == 2
    assert "requires --quarantine-source-only-legacy" in capsys.readouterr().err


def test_source_only_legacy_quarantine_is_audited_and_non_destructive():
    class _Result:
        def __init__(
            self,
            *,
            rows=None,
            scalar_value=None,
            rowcount=0,
        ):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self):
            self.results = [
                _Result(
                    rows=[
                        {
                            "id": 7,
                            "stock_code": "000001",
                            "trade_date": "2026-03-02",
                            "adjust_type": 0,
                            "data_version": "legacy-a",
                        },
                        {
                            "id": 9,
                            "stock_code": "600000",
                            "trade_date": "2026-03-03",
                            "adjust_type": 0,
                            "data_version": "legacy-b",
                        },
                    ]
                ),
                _Result(scalar_value=0),
                _Result(rowcount=2),
                _Result(scalar_value=0),
                _Result(rowcount=1),
            ]
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append((str(statement), dict(params or {})))
            return self.results.pop(0)

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            return nullcontext(self.connection)

    engine = _Engine()
    result = backfill_tool._quarantine_source_only_legacy_rows(
        engine,
        start_date="20260302",
        end_date="2026-03-13",
        provider="gj_big_qmt_inner",
    )

    assert result["status"] == "APPLIED"
    assert result["selected_rows"] == 2
    assert result["quarantined_rows"] == 2
    assert result["remaining_rows"] == 0
    assert result["existing_target_rows_updated"] == 0
    assert len(result["row_identity_sha256"]) == 64
    statements = "\n".join(sql for sql, _params in engine.connection.statements)
    assert "FOR UPDATE" in statements
    assert "BINARY s.pre_close_origin=BINARY 'UNVERIFIED_LEGACY'" in statements
    assert "SET s.provider=:quarantine_provider" in statements
    assert "qmt_local_stock_kline_quarantine" in statements
    assert "DELETE" not in statements.upper()
    audit_params = engine.connection.statements[-1][1]
    audit = json.loads(audit_params["extra_json"])
    assert audit["reason"] == "SOURCE_ONLY_UNVERIFIED_LEGACY"
    assert audit["existing_target_rows_updated"] == 0
    assert audit_params["row_count"] == 2
    assert audit_params["requested_codes"] == 2


def test_source_only_legacy_quarantine_cli_requires_native_repair(capsys):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(
            ["daily", "--quarantine-source-only-legacy"]
        )

    assert exc_info.value.code == 2
    assert "requires --repair-target-source-only" in capsys.readouterr().err


def test_source_only_legacy_quarantine_separates_runtime_and_history_dml():
    rows = [
        {
            "id": 7,
            "stock_code": "000001",
            "trade_date": "2026-03-02",
            "adjust_type": 0,
            "data_version": "legacy-a",
        },
        {
            "id": 9,
            "stock_code": "600000",
            "trade_date": "2026-03-03",
            "adjust_type": 0,
            "data_version": "legacy-b",
        },
    ]

    class _Result:
        def __init__(self, *, rows=None, scalar_value=None, rowcount=0):
            self.rows = list(rows or [])
            self.scalar_value = scalar_value
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return list(self.rows)

        def scalar(self):
            return self.scalar_value

    class _Connection:
        def __init__(self, results, statements):
            self.results = results
            self.statements = statements

        def execute(self, statement, params=None):
            self.statements.append((str(statement), dict(params or {})))
            return self.results.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            return False

    class _Engine:
        def __init__(self, results):
            self.results = list(results)
            self.statements = []

        def connect(self):
            return _Connection(self.results, self.statements)

        def begin(self):
            return nullcontext(_Connection(self.results, self.statements))

    source_engine = _Engine(
        [
            _Result(rows=rows),
            _Result(scalar_value=0),
        ]
    )
    history_engine = _Engine(
        [
            _Result(rows=rows),
            _Result(scalar_value=0),
            _Result(rowcount=2),
            _Result(scalar_value=0),
            _Result(rowcount=1),
        ]
    )

    result = backfill_tool._quarantine_source_only_legacy_rows(
        source_engine,
        history_engine=history_engine,
        start_date="2026-03-02",
        end_date="2026-03-13",
        provider="gj_big_qmt_inner",
    )

    assert result["status"] == "APPLIED"
    assert result["separated_history_writer"] is True
    assert result["selected_rows"] == 2
    assert result["quarantined_rows"] == 2
    source_sql = "\n".join(sql for sql, _params in source_engine.statements)
    history_sql = "\n".join(sql for sql, _params in history_engine.statements)
    assert "UPDATE `probiga_qmt_history`" not in source_sql
    assert "INSERT INTO `probiga_qmt_history`" not in source_sql
    assert "DELETE" not in source_sql.upper()
    assert "UPDATE `probiga_qmt_history`" in history_sql
    assert "INSERT INTO `probiga_qmt_history`" in history_sql
    assert "`probiga`.`sm_stock_kline`" not in history_sql


def test_windows_option_file_route_uses_safe_urls_and_fixed_databases(
    monkeypatch,
):
    history_engine = _DisposableEngine("probiga_qmt_history")
    events = []

    class _DbapiConnection:
        def select_db(self, database):
            events.append(("select_db", database))

        def close(self):
            events.append("dbapi_close")

    class _Scalar:
        def scalar(self):
            return "probiga"

    class _PrimaryConnection:
        def execute(self, statement, params=None):
            events.append(("execute", str(statement), dict(params or {})))
            return _Scalar()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    class _PrimaryEngine(_DisposableEngine):
        def connect(self):
            return _PrimaryConnection()

    def fake_create_engine(url, *, creator, **kwargs):
        rendered = url.render_as_string(hide_password=False)
        events.append(("url", rendered, kwargs))
        creator()
        return _PrimaryEngine("probiga")

    monkeypatch.setattr(
        backfill_tool,
        "_create_windows_local_history_engine",
        lambda: history_engine,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_validate_windows_local_mysql84_boundary",
        lambda engine: events.append(("boundary", engine)) or {"ready": True},
    )
    monkeypatch.setattr(
        backfill_tool,
        "_connect_from_windows_option_file",
        lambda path: events.append(("option_file", path)) or _DbapiConnection(),
    )
    monkeypatch.setattr(backfill_tool, "create_engine", fake_create_engine)

    primary_engine, returned_history = backfill_tool._windows_local_engines()

    assert returned_history is history_engine
    assert primary_engine.url.username is None
    assert primary_engine.url.password is None
    rendered_urls = [event[1] for event in events if event[0] == "url"]
    assert rendered_urls == ["mysql+pymysql:///probiga"]
    assert ("select_db", "probiga") in events
    assert ("boundary", history_engine) in events


def test_windows_history_writer_grants_accept_mysql84_account_level_tls_format():
    result = backfill_tool._validate_windows_history_writer_grants(
        (
            "GRANT USAGE ON *.* TO `writer`@`127.0.0.1`",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON "
            "`probiga_qmt_history`.* TO `writer`@`127.0.0.1`",
        )
    )

    assert result["ready"] is True
    assert result["schema_privileges"] == [
        "DELETE",
        "INSERT",
        "SELECT",
        "UPDATE",
    ]
    assert result["ddl_privileges"] == []
    assert result["grant_option"] is False


@pytest.mark.parametrize(
    "grants",
    [
        (
            "GRANT USAGE ON *.* TO 'writer'@'127.0.0.1' REQUIRE SSL",
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON "
            "probiga_qmt_history.* TO 'writer'@'127.0.0.1'",
        ),
        (
            "GRANT USAGE ON *.* TO 'writer'@'127.0.0.1' REQUIRE SSL",
            "GRANT SELECT, INSERT, UPDATE ON probiga_qmt_history.* "
            "TO 'writer'@'127.0.0.1'",
        ),
        (
            "GRANT USAGE ON *.* TO 'writer'@'127.0.0.1' REQUIRE SSL",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON probiga.* "
            "TO 'writer'@'127.0.0.1'",
        ),
        (
            "GRANT USAGE ON *.* TO 'writer'@'127.0.0.1' REQUIRE SSL",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON "
            "probiga_qmt_history.* TO 'writer'@'127.0.0.1' "
            "WITH GRANT OPTION",
        ),
    ],
)
def test_windows_history_writer_grants_reject_excess_or_incomplete(grants):
    with pytest.raises(RuntimeError, match="grants differ"):
        backfill_tool._validate_windows_history_writer_grants(grants)


def test_windows_history_writer_account_accepts_mysql84_create_user_contract():
    result = backfill_tool._validate_windows_history_writer_account(
        create_user=(
            "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
            "IDENTIFIED WITH 'caching_sha2_password' AS '<secret>' REQUIRE SSL "
            "PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"
        ),
        active_roles="NONE",
        expected_identity="pb_qmt_hist_writer_0123abcdef89@127.0.0.1",
    )

    assert result == {
        "ready": True,
        "plugin": "caching_sha2_password",
        "tls_required": True,
        "account_unlocked": True,
        "active_roles": "NONE",
    }


@pytest.mark.parametrize(
    "create_user, active_roles",
    [
        (
            "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
            "IDENTIFIED WITH 'caching_sha2_password' AS '<secret>' REQUIRE NONE "
            "ACCOUNT UNLOCK",
            "NONE",
        ),
        (
            "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
            "IDENTIFIED WITH 'mysql_native_password' AS '<secret>' REQUIRE SSL "
            "ACCOUNT UNLOCK",
            "NONE",
        ),
        (
            "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
            "IDENTIFIED WITH 'caching_sha2_password' AS '<secret>' REQUIRE SSL "
            "ACCOUNT LOCK",
            "NONE",
        ),
        (
            "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
            "IDENTIFIED WITH 'caching_sha2_password' AS '<secret>' REQUIRE SSL "
            "ACCOUNT UNLOCK",
            "`unexpected_role`@`%`",
        ),
    ],
)
def test_windows_history_writer_account_rejects_unsafe_contract(
    create_user,
    active_roles,
):
    with pytest.raises(RuntimeError, match="account metadata differs"):
        backfill_tool._validate_windows_history_writer_account(
            create_user=create_user,
            active_roles=active_roles,
            expected_identity="pb_qmt_hist_writer_0123abcdef89@127.0.0.1",
        )


def test_windows_history_writer_boundary_splits_grants_from_account_tls(
    monkeypatch,
):
    expected_identity = "pb_qmt_hist_writer_0123abcdef89@127.0.0.1"

    class _Result:
        def __init__(self, *, scalars=None, row=None, scalar=None):
            self._scalars = scalars
            self._row = row
            self._scalar = scalar

        def scalars(self):
            return self._scalars

        def one(self):
            return self._row

        def scalar_one(self):
            return self._scalar

    class _Connection:
        def execute(self, statement):
            sql = str(statement)
            if sql == "SHOW GRANTS FOR CURRENT_USER()":
                return _Result(scalars=(
                    "GRANT USAGE ON *.* TO `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1`",
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON `probiga_qmt_history`.* "
                    "TO `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1`",
                ))
            if sql == "SHOW CREATE USER CURRENT_USER()":
                return _Result(row=(
                    "CREATE USER `pb_qmt_hist_writer_0123abcdef89`@`127.0.0.1` "
                    "IDENTIFIED WITH 'caching_sha2_password' AS '<secret>' "
                    "REQUIRE SSL ACCOUNT UNLOCK",
                ))
            if sql == "SELECT CURRENT_ROLE()":
                return _Result(scalar="NONE")
            raise AssertionError(sql)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    class _Engine:
        def connect(self):
            return _Connection()

    monkeypatch.setattr(
        backfill_tool,
        "_validate_windows_local_mysql84_boundary",
        lambda _engine, **_kwargs: {"ready": True, "tls": True},
    )

    result = backfill_tool._validate_windows_history_writer_boundary(
        _Engine(),
        expected_identity=expected_identity,
    )

    assert result["ready"] is True
    assert result["tls"] is True
    assert result["account"]["tls_required"] is True
    assert result["least_privilege"]["ddl_privileges"] == []


@pytest.mark.parametrize(
    "user, accepted",
    [
        ("pb_qmt_hist_writer_0123abcdef89", True),
        ("pb_qmt_hist_writer_0123ABCDEf89", False),
        ("probiga_qmt_history_writer", False),
        ("pb_qmt_hist_writer_0123abcdef890", False),
    ],
)
def test_windows_history_writer_identity_uses_random_controlled_name(
    monkeypatch,
    tmp_path,
    user,
    accepted,
):
    option_file = tmp_path / "writer.ini"
    option_file.write_text(f"[client]\nuser={user}\n", encoding="utf-8")
    monkeypatch.setattr(
        backfill_tool,
        "_validate_windows_option_file_shape",
        lambda _path: None,
    )

    if accepted:
        assert backfill_tool._windows_history_writer_identity(option_file) == (
            f"{user}@127.0.0.1"
        )
    else:
        with pytest.raises(RuntimeError, match="user differs"):
            backfill_tool._windows_history_writer_identity(option_file)


def test_windows_history_writer_option_file_checks_secret_and_profile_acls(
    monkeypatch,
    tmp_path,
):
    profile = tmp_path / "Administrator"
    secret = profile / ".probiga-secrets"
    secret.mkdir(parents=True)
    option_file = secret / "mysql84-qmt-history-writer.ini"
    option_file.write_text("protected", encoding="utf-8")
    snapshots = []
    monkeypatch.setattr(
        backfill_tool,
        "WINDOWS_LOCAL_HISTORY_WRITER_OPTION_FILE",
        option_file,
    )
    monkeypatch.setattr(
        backfill_tool,
        "WINDOWS_LOCAL_HISTORY_WRITER_PROFILE_ROOT",
        profile,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_protected_windows_option_file",
        lambda path: path.resolve(strict=True),
    )

    def acl_snapshot(path):
        snapshots.append(("path", path))
        return {
            "owner_sid": "current",
            "current_user_sid": "current",
            "protected": True,
            "rules": [
                {
                    "sid": "current",
                    "access_type": "Allow",
                    "inherited": False,
                    "rights": 1,
                },
                {
                    "sid": "read-only-group",
                    "access_type": "Allow",
                    "inherited": True,
                    "rights": 1,
                },
            ],
        }

    monkeypatch.setattr(backfill_tool, "_windows_acl_snapshot", acl_snapshot)

    assert backfill_tool._validated_windows_history_writer_option_file() == (
        option_file.resolve()
    )
    checked_paths = [item[1] for item in snapshots if item[0] == "path"]
    assert checked_paths == [secret.resolve(), profile.resolve()]


@pytest.mark.parametrize("file_fault", [None, "readable", "inherited", "unprotected"])
def test_writer_directory_read_access_does_not_relax_private_file(
    monkeypatch, tmp_path, file_fault,
):
    from tools import migrate_qmt_local_history_provenance as migration

    profile = tmp_path / "profile"
    secret = profile / "secrets"
    secret.mkdir(parents=True)
    option_file = secret / "writer.ini"
    option_file.write_text("not a real credential", encoding="utf-8")
    monkeypatch.setattr(backfill_tool, "WINDOWS_LOCAL_HISTORY_WRITER_OPTION_FILE", option_file)
    monkeypatch.setattr(backfill_tool, "WINDOWS_LOCAL_HISTORY_WRITER_PROFILE_ROOT", profile)
    monkeypatch.setattr(migration, "_running_on_windows", lambda: True)

    def acl(path):
        is_file = path == option_file
        rules = [
            {"sid": sid, "access_type": "Allow", "inherited": False, "rights": 0x1F01FF}
            for sid in ("current", migration._WINDOWS_SYSTEM_SID,
                        migration._WINDOWS_ADMINISTRATORS_SID)
        ]
        if not is_file or file_fault == "readable":
            rules.append({"sid": "readonly", "access_type": "Allow",
                          "inherited": False, "rights": 0x1200A9})
        if is_file and file_fault == "inherited":
            rules[0]["inherited"] = True
        return {"current_user_sid": "current", "owner_sid": "current",
                "protected": not (is_file and file_fault == "unprotected"),
                "rules": rules}

    monkeypatch.setattr(migration, "_windows_acl_snapshot", acl)
    monkeypatch.setattr(backfill_tool, "_windows_acl_snapshot", acl)
    if file_fault:
        with pytest.raises(migration.WindowsLocalHistoryBoundaryError, match="not private"):
            backfill_tool._validated_windows_history_writer_option_file()
    else:
        assert backfill_tool._validated_windows_history_writer_option_file() == option_file


@pytest.mark.parametrize("unsafe_parent", ["secret", "profile"])
@pytest.mark.parametrize("rights", [0x0002, 0x0004, 0x0010, 0x0040, 0x0100,
                                   0x10000, 0x40000, 0x80000, 0x10000000, 0x40000000])
def test_windows_history_writer_option_file_rejects_writable_parent(
    monkeypatch,
    tmp_path,
    unsafe_parent,
    rights,
):
    profile = tmp_path / "Administrator"
    secret = profile / ".probiga-secrets"
    secret.mkdir(parents=True)
    option_file = secret / "mysql84-qmt-history-writer.ini"
    option_file.write_text("protected", encoding="utf-8")
    monkeypatch.setattr(
        backfill_tool,
        "WINDOWS_LOCAL_HISTORY_WRITER_OPTION_FILE",
        option_file,
    )
    monkeypatch.setattr(
        backfill_tool,
        "WINDOWS_LOCAL_HISTORY_WRITER_PROFILE_ROOT",
        profile,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_protected_windows_option_file",
        lambda path: path.resolve(strict=True),
    )

    def acl_snapshot(path):
        rules = [
            {
                "sid": "current",
                "access_type": "Allow",
                "inherited": False,
                "rights": 1,
            }
        ]
        if path == (secret if unsafe_parent == "secret" else profile).resolve():
            rules.append(
                {
                    "sid": "untrusted-group",
                    "access_type": "Allow",
                    "inherited": True,
                    "rights": rights,
                }
            )
        return {
            "owner_sid": "current",
            "current_user_sid": "current",
            "protected": True,
            "rules": rules,
        }

    monkeypatch.setattr(backfill_tool, "_windows_acl_snapshot", acl_snapshot)

    with pytest.raises(RuntimeError, match="directory is not private"):
        backfill_tool._validated_windows_history_writer_option_file()


def test_windows_history_writer_route_separates_primary_and_history(
    monkeypatch,
):
    runtime_boundary_engine = _DisposableEngine("probiga_qmt_history")
    writer_engine = _DisposableEngine("probiga_qmt_history")
    events = []
    writer_path = backfill_tool.Path(
        r"C:\Users\Administrator\.probiga-secrets\mysql84-qmt-history-writer.ini"
    )

    class _DbapiConnection:
        def select_db(self, database):
            events.append(("primary_select_db", database))

        def close(self):
            events.append("primary_dbapi_close")

    class _Scalar:
        def scalar(self):
            return "probiga"

    class _PrimaryConnection:
        def execute(self, _statement, _params=None):
            return _Scalar()

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            return False

    class _PrimaryEngine(_DisposableEngine):
        def connect(self):
            return _PrimaryConnection()

    def create_history_engine(path=backfill_tool.WINDOWS_LOCAL_OPTION_FILE):
        events.append(("history_option", path))
        return (
            runtime_boundary_engine
            if path == backfill_tool.WINDOWS_LOCAL_OPTION_FILE
            else writer_engine
        )

    monkeypatch.setattr(
        backfill_tool,
        "_create_windows_local_history_engine",
        create_history_engine,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_validate_windows_local_mysql84_boundary",
        lambda engine, **kwargs: events.append(
            ("runtime_boundary", engine, kwargs)
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_validated_windows_history_writer_option_file",
        lambda: writer_path,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_windows_history_writer_identity",
        lambda path: "pb_qmt_hist_writer_0123abcdef89@127.0.0.1",
    )
    monkeypatch.setattr(
        backfill_tool,
        "_validate_windows_history_writer_boundary",
        lambda engine, **kwargs: events.append(
            ("writer_boundary", engine, kwargs)
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_connect_from_windows_option_file",
        lambda path: events.append(("primary_option", path))
        or _DbapiConnection(),
    )
    monkeypatch.setattr(
        backfill_tool,
        "create_engine",
        lambda _url, *, creator, **_kwargs: creator()
        and _PrimaryEngine("probiga"),
    )

    primary_engine, history_engine = backfill_tool._windows_local_engines(
        history_writer=True
    )

    assert isinstance(primary_engine, _PrimaryEngine)
    assert history_engine is writer_engine
    assert runtime_boundary_engine.disposed is True
    assert ("history_option", writer_path) in events
    assert (
        "primary_option",
        backfill_tool.WINDOWS_LOCAL_OPTION_FILE,
    ) in events
    writer_boundaries = [item for item in events if item[0] == "writer_boundary"]
    assert writer_boundaries[0][2]["expected_identity"].startswith(
        "pb_qmt_hist_writer_"
    )


def test_live_history_writer_helper_disposes_unused_primary(monkeypatch):
    primary_engine = _DisposableEngine("probiga")
    history_engine = _DisposableEngine("probiga_qmt_history")
    calls = []
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda *, history_writer=False: calls.append(history_writer)
        or (primary_engine, history_engine),
    )

    result = backfill_tool.create_validated_windows_history_writer_engine()

    assert calls == [True]
    assert result is history_engine
    assert primary_engine.disposed is True
    assert history_engine.disposed is False


def test_daily_main_locks_and_reports_exact_universe(monkeypatch, capsys):
    source_engine = object()
    local_engine = _DisposableEngine("probiga_qmt_history")
    events = []
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda: (source_engine, local_engine),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda engine: events.append(("ensure", engine)),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_codes",
        lambda engine, **kwargs: (
            ["000001", "600000"],
            ["2026-08-19"],
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_acquire_lock",
        lambda path: events.append(("acquire", path)) or (True, ""),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_release_lock",
        lambda path: events.append(("release", path)),
    )
    monkeypatch.setattr(
        backfill_tool,
        "backfill_daily_kline_local",
        lambda **kwargs: events.append(("backfill", kwargs)) or _result(),
    )

    exit_code = backfill_tool.main(
        [
            "daily",
            "--windows-local-option-file",
            "--target-window-universe",
            "--start-date",
            "2026-08-19",
            "--end-date",
            "2026-08-19",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "SUCCESS"
    assert payload["connection_mode"] == "fixed_protected_windows_option_file"
    assert payload["universe"]["source"] == (
        "qmt_stock_catalog.target_window_exact_union"
    )
    assert payload["universe"]["stock_count"] == 2
    assert payload["universe"]["target_trade_date_count"] == 1
    assert events[1] == ("acquire", TEST_DAILY_LOCK)
    assert events[-1] == ("release", TEST_DAILY_LOCK)


def test_plain_daily_apply_uses_separated_history_writer(monkeypatch, capsys):
    source_engine = object()
    writer_engine = _DisposableEngine("probiga_qmt_history")
    events = []
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda *, history_writer=False: events.append(
            ("engines", history_writer)
        )
        or (source_engine, writer_engine),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda engine: events.append(("schema", engine)),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_codes",
        lambda _engine, **_kwargs: (["000001"], ["2026-08-19"]),
    )
    monkeypatch.setattr(backfill_tool, "_acquire_lock", lambda _path: (True, ""))
    monkeypatch.setattr(
        backfill_tool,
        "_release_lock",
        lambda _path: events.append("release"),
    )

    def fake_backfill(**kwargs):
        events.append(("backfill", kwargs))
        return _result(code_count=1, fetched_rows=1, written_rows=1)

    monkeypatch.setattr(
        backfill_tool,
        "backfill_daily_kline_local",
        fake_backfill,
    )

    exit_code = backfill_tool.main(
        [
            "daily",
            "--windows-history-writer-option-file",
            "--target-window-universe",
            "--start-date",
            "2026-08-19",
            "--end-date",
            "2026-08-19",
            "--apply",
            "--json",
        ]
    )

    assert exit_code == 0
    assert events[0] == ("engines", True)
    backfill_event = next(item for item in events if item[0] == "backfill")
    assert backfill_event[1]["source_engine"] is source_engine
    assert backfill_event[1]["local_engine"] is writer_engine
    assert backfill_event[1]["dry_run"] is False
    payload = json.loads(capsys.readouterr().out)
    assert payload["connection_mode"] == (
        "fixed_protected_windows_history_writer_option_file"
    )


def test_daily_exact_lifecycle_allowlist_is_proven_and_only_then_forwarded(
    monkeypatch,
    capsys,
):
    source_engine = object()
    writer_engine = _DisposableEngine("probiga_qmt_history")
    events = []
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda *, history_writer=False: (source_engine, writer_engine),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_codes",
        lambda _engine, **_kwargs: (
            ["002231", "600000"],
            ["2026-03-06", "2026-08-27"],
        ),
    )
    proof = {
        "schema": backfill_tool.EXACT_LIFECYCLE_NO_ROW_PROOF_SCHEMA,
        "policy": "OPERATOR_EXPLICIT_FINITE_EXPIRY_ZERO_DAILY_ROWS",
        "stock_codes": ["002231"],
        "proof_sha256": "c" * 64,
    }
    monkeypatch.setattr(
        backfill_tool,
        "_prove_reviewed_no_row_codes",
        lambda engine, **kwargs: events.append(("proof", engine, kwargs))
        or proof,
    )
    monkeypatch.setattr(backfill_tool, "_acquire_lock", lambda _path: (True, ""))
    monkeypatch.setattr(backfill_tool, "_release_lock", lambda _path: None)

    def fake_backfill(**kwargs):
        events.append(("backfill", kwargs))
        return _result(
            code_count=2,
            fetched_rows=1,
            written_rows=1,
            allowed_missing_codes=("002231",),
        )

    monkeypatch.setattr(
        backfill_tool,
        "backfill_daily_kline_local",
        fake_backfill,
    )

    exit_code = backfill_tool.main(
        [
            "daily",
            "--windows-history-writer-option-file",
            "--target-window-universe",
            "--exact-lifecycle-no-row-codes",
            "002231",
            "--start-date",
            "2026-03-06",
            "--end-date",
            "2026-08-27",
            "--apply",
            "--json",
        ]
    )

    assert exit_code == 0
    assert events[0] == (
        "proof",
        source_engine,
        {
            "exact_lifecycle_codes": ["002231"],
            "not_yet_listed_codes": [],
            "start_date": "2026-03-06",
            "end_date": "2026-08-27",
        },
    )
    backfill_kwargs = next(
        event[1] for event in events if event[0] == "backfill"
    )
    assert backfill_kwargs["allowed_missing_stock_codes"] == ["002231"]
    payload = json.loads(capsys.readouterr().out)
    assert payload["reviewed_no_row_allowlist"] == {
        **proof,
        "used_missing_codes": ["002231"],
        "used_missing_code_count": 1,
    }


def test_daily_exact_lifecycle_allowlist_rejects_code_outside_target_union(
    monkeypatch,
):
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda *, history_writer=False: (
            object(),
            _DisposableEngine("probiga_qmt_history"),
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_codes",
        lambda _engine, **_kwargs: (["600000"], ["2026-03-06"]),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_prove_exact_lifecycle_no_row_codes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("outside-universe code must fail before proof")
        ),
    )

    with pytest.raises(RuntimeError, match="outside the target universe"):
        backfill_tool.main(
            [
                "daily",
                "--windows-history-writer-option-file",
                "--target-window-universe",
                "--exact-lifecycle-no-row-codes",
                "002231",
                "--start-date",
                "2026-03-06",
                "--end-date",
                "2026-08-27",
                "--apply",
            ]
        )


def test_daily_main_runs_strict_quarantine_chain_in_safe_order(
    monkeypatch,
    capsys,
):
    source_engine = object()
    local_engine = _DisposableEngine("probiga_qmt_history")
    events = []
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda *, history_writer=False: (
            (source_engine, local_engine)
            if history_writer
            else (_ for _ in ()).throw(
                AssertionError("daily apply must use the history writer")
            )
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_codes",
        lambda _engine, **_kwargs: (
            ["600000", "688693"],
            ["2026-03-16"],
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_target_window_unattestable_codes",
        lambda _engine, **_kwargs: events.append("allow-list")
        or ["688693"],
    )
    monkeypatch.setattr(
        backfill_tool,
        "_acquire_lock",
        lambda _path: (True, ""),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_release_lock",
        lambda _path: events.append("release"),
    )

    def fake_backfill(**kwargs):
        events.append(("backfill", kwargs))
        return _result(code_count=2, fetched_rows=1, written_rows=1)

    monkeypatch.setattr(
        backfill_tool,
        "backfill_daily_kline_local",
        fake_backfill,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_quarantine_invalid_target_rows_without_native",
        lambda _engine, **_kwargs: events.append("target-quarantine")
        or {"status": "APPLIED", "deleted_rows": 1},
    )
    monkeypatch.setattr(
        backfill_tool,
        "_repair_target_source_only_rows",
        lambda _engine, **_kwargs: events.append("target-repair")
        or {"status": "APPLIED"},
    )
    monkeypatch.setattr(
        backfill_tool,
        "_quarantine_source_only_legacy_rows",
        lambda _engine, **_kwargs: events.append("legacy-quarantine")
        or {"status": "APPLIED"},
    )

    exit_code = backfill_tool.main(
        [
            "daily",
            "--windows-history-writer-option-file",
            "--target-window-universe",
            "--repair-target-source-only",
            "--quarantine-source-only-legacy",
            "--quarantine-invalid-target-no-native",
            "--provider",
            "gj_big_qmt_inner",
            "--dividend-type",
            "none",
            "--start-date",
            "2026-03-16",
            "--end-date",
            "2026-03-27",
            "--apply",
            "--json",
        ]
    )

    assert exit_code == 0
    event_names = [event[0] if isinstance(event, tuple) else event for event in events]
    assert event_names == [
        "allow-list",
        "backfill",
        "target-quarantine",
        "target-repair",
        "legacy-quarantine",
        "release",
    ]
    backfill_kwargs = events[1][1]
    assert backfill_kwargs["allowed_missing_stock_codes"] == ["688693"]
    payload = json.loads(capsys.readouterr().out)
    assert payload["allowed_missing_target_codes"]["stock_codes"] == [
        "688693"
    ]
    assert payload["invalid_target_quarantine"]["deleted_rows"] == 1


@pytest.mark.parametrize(
    "argv",
    [
        ["daily", "--windows-history-writer-option-file"],
        ["minute", "--windows-history-writer-option-file", "--apply"],
        ["from-gaps", "--windows-history-writer-option-file", "--apply"],
        ["init", "--windows-history-writer-option-file", "--apply"],
        ["validate-schema", "--windows-history-writer-option-file", "--apply"],
    ],
)
def test_history_writer_option_file_is_only_valid_for_daily_apply(
    argv,
    capsys,
):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(argv)

    assert exc_info.value.code == 2
    assert "restricted to daily --apply" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv",
    [
        [
            "daily",
            "--target-window-universe",
            "--exact-lifecycle-no-row-codes",
            "002231",
            "--apply",
        ],
        [
            "daily",
            "--windows-history-writer-option-file",
            "--exact-lifecycle-no-row-codes",
            "002231",
            "--apply",
        ],
        [
            "daily",
            "--windows-history-writer-option-file",
            "--target-window-universe",
            "--exact-lifecycle-no-row-codes",
            "002231",
            "--provider",
            "gj_qmt",
            "--apply",
        ],
    ],
)
def test_exact_lifecycle_no_row_cli_requires_strict_protected_route(
    argv,
    capsys,
):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(argv)

    assert exc_info.value.code == 2
    assert "requires protected daily" in capsys.readouterr().err


@pytest.mark.parametrize("mode", ["daily", "minute", "from-gaps"])
def test_apply_rejects_read_only_windows_identity(mode, capsys):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(
            [mode, "--windows-local-option-file", "--apply"]
        )

    assert exc_info.value.code == 2
    assert "requires --windows-history-writer-option-file" in (
        capsys.readouterr().err
    )


def test_repair_rejects_the_read_only_windows_identity(capsys):
    with pytest.raises(SystemExit) as exc_info:
        backfill_tool.main(
            [
                "daily",
                "--windows-local-option-file",
                "--target-window-universe",
                "--repair-target-source-only",
                "--start-date",
                "2026-03-01",
                "--end-date",
                "2026-03-31",
                "--apply",
            ]
        )

    assert exc_info.value.code == 2
    assert "requires --windows-history-writer-option-file" in (
        capsys.readouterr().err
    )


def test_daily_main_lock_contention_is_nonzero_and_does_not_fetch(
    monkeypatch,
    capsys,
):
    _patch_daily_lock_path(monkeypatch)
    monkeypatch.setattr(backfill_tool, "_source_engine", lambda: object())
    monkeypatch.setattr(
        backfill_tool,
        "get_local_history_engine",
        lambda _url=None: _DisposableEngine("probiga_qmt_history"),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_codes_from_arg",
        lambda _engine, _codes, *, limit: ["000001"],
    )
    monkeypatch.setattr(
        backfill_tool,
        "_acquire_lock",
        lambda _path: (False, "1234 2026-08-23T12:00:00"),
    )
    monkeypatch.setattr(
        backfill_tool,
        "backfill_daily_kline_local",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("QMT fetch must not start")
        ),
    )

    exit_code = backfill_tool.main(
        [
            "daily",
            "--codes",
            "000001",
            "--start-date",
            "2026-08-19",
            "--end-date",
            "2026-08-19",
            "--json",
        ]
    )

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "already_running"
    assert payload["mode"] == "daily"


def test_atomic_daily_lock_allows_one_owner(tmp_path):
    lock_path = tmp_path / "daily.lock"

    acquired, owner = backfill_tool._acquire_lock(lock_path)
    second_acquired, second_owner = backfill_tool._acquire_lock(lock_path)

    assert acquired is True
    assert owner == ""
    assert second_acquired is False
    assert second_owner.startswith(str(backfill_tool.os.getpid()))
    backfill_tool._release_lock(lock_path)
    assert not lock_path.exists()


def test_atomic_daily_lock_never_unlinks_a_fresh_initializing_owner(tmp_path):
    lock_path = tmp_path / "daily.lock"
    lock_path.write_bytes(b"")

    acquired, owner = backfill_tool._acquire_lock(lock_path)

    assert acquired is False
    assert owner == "lock_initializing"
    assert lock_path.exists()


def _patch_gap_repair_main_dependencies(monkeypatch):
    monkeypatch.setattr(backfill_tool, "_source_engine", lambda: object())
    monkeypatch.setattr(
        backfill_tool,
        "get_local_history_engine",
        lambda _url=None: _DisposableEngine("probiga_qmt_history"),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        backfill_tool,
        "_codes_from_arg",
        lambda _engine, _codes, *, limit: ["000001"],
    )
    monkeypatch.setattr(
        backfill_tool,
        "_gap_rows",
        lambda *_args, **_kwargs: [],
    )


def test_gap_repair_apply_lock_io_error_is_not_false_already_running(
    monkeypatch,
    capsys,
    tmp_path,
):
    state_root = tmp_path / "gap-state"
    state_root.mkdir(mode=0o700)
    _patch_gap_repair_main_dependencies(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_acquire_lock",
        lambda _path: (False, "lock_error:PermissionError"),
    )

    exit_code = backfill_tool.main(
        [
            "from-gaps",
            "--gap-limit",
            "2",
            "--apply",
            "--state-root",
            str(state_root),
            "--lock-path",
            str(state_root / "gap.lock"),
            "--json",
        ]
    )

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "lock_error"
    assert payload["owner"] == "lock_error:PermissionError"


def test_gap_repair_true_active_lock_is_distinct_and_nonzero(
    monkeypatch,
    capsys,
    tmp_path,
):
    state_root = tmp_path / "gap-state"
    state_root.mkdir(mode=0o700)
    _patch_gap_repair_main_dependencies(monkeypatch)
    monkeypatch.setattr(
        backfill_tool,
        "_acquire_lock",
        lambda _path: (False, "1234 2026-08-25T07:05:00"),
    )

    exit_code = backfill_tool.main(
        [
            "from-gaps",
            "--apply",
            "--state-root",
            str(state_root),
            "--lock-path",
            str(state_root / "gap.lock"),
            "--json",
        ]
    )

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "already_running"


def test_gap_repair_state_root_is_outside_sealed_code_tree(
    monkeypatch,
    tmp_path,
):
    code_root = tmp_path / "sealed-code"
    code_root.mkdir(mode=0o700)
    state_root = tmp_path / "service-state"
    state_root.mkdir(mode=0o700)
    code_root.chmod(0o555)
    monkeypatch.setattr(backfill_tool, "ROOT", code_root)

    root, lock = backfill_tool._validated_gap_repair_lock_path(
        state_root=str(state_root),
        lock_path=str(state_root / "gap.lock"),
    )

    assert root == state_root
    assert lock == state_root / "gap.lock"
    code_root.chmod(0o700)


def test_gap_repair_windows_mapping_is_fixed_under_programdata():
    root, lock = backfill_tool._windows_gap_repair_state_mapping(
        r"C:\ProgramData"
    )

    assert root == r"C:\ProgramData\ProBigA\qmt-local-gap-repair"
    assert lock == root + r"\qmt-local-gap-repair.lock"
    with pytest.raises(RuntimeError, match="absolute drive path"):
        backfill_tool._windows_gap_repair_state_mapping("relative")

    daily_root, daily_lock = (
        backfill_tool._windows_daily_backfill_state_mapping(
            r"C:\ProgramData"
        )
    )
    assert daily_root == root
    assert daily_lock == root + r"\qmt-local-daily-backfill.lock"


def test_daily_backfill_lock_reuses_protected_gap_state_root(tmp_path):
    state_root = tmp_path / "qmt-state"
    state_root.mkdir(mode=0o700)

    root, lock = backfill_tool._validated_daily_backfill_lock_path(
        state_root=str(state_root),
        lock_path=str(state_root / "qmt-local-daily-backfill.lock"),
    )

    assert root == state_root
    assert lock.parent == state_root
    assert lock.name == "qmt-local-daily-backfill.lock"
    assert not backfill_tool._is_relative_to(
        root.resolve(),
        backfill_tool.ROOT.resolve(),
    )


def _patch_daily_dependencies(monkeypatch, prepared_rows):
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda _engine, *, stock_codes, start_date, end_date: {
            (code, str(row.get("trade_date") or "")[:10])
            for code in stock_codes
            for row in (prepared_rows or [{"trade_date": ""}])
        },
    )
    run_events = []
    monkeypatch.setattr(
        local_history,
        "_record_run_start",
        lambda *_args, **kwargs: run_events.append(("start", kwargs)),
    )
    monkeypatch.setattr(
        local_history,
        "_record_run_finish",
        lambda *_args, **kwargs: run_events.append(("finish", kwargs)),
    )
    monkeypatch.setattr(
        "integrations.bigqmt.backend.BigQmtBackend.fetch_kline",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        local_history,
        "_prepare_kline_rows",
        lambda *_args, **_kwargs: list(prepared_rows),
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("incomplete batch must not be written")
        ),
    )
    return run_events


@pytest.mark.parametrize(
    "prepared_rows, expected_fragment",
    [
        ([], "fetched_rows=0"),
        ([{"stock_code": "000001"}], "missing_count=1"),
    ],
)
def test_daily_backfill_fails_closed_on_empty_or_missing_batch(
    monkeypatch,
    prepared_rows,
    expected_fragment,
):
    run_events = _patch_daily_dependencies(monkeypatch, prepared_rows)
    local_engine = _DisposableEngine("probiga_qmt_history")

    with pytest.raises(RuntimeError, match=expected_fragment):
        local_history.backfill_daily_kline_local(
            source_engine=object(),
            local_engine=local_engine,
            stock_codes=["000001", "600000"],
            start_date="2026-08-19",
            end_date="2026-08-19",
            batch_size=2,
            dry_run=False,
        )

    assert run_events[0][0] == "start"
    assert run_events[-1][0] == "finish"
    assert run_events[-1][1]["status"] == "FAILED"
    assert expected_fragment in run_events[-1][1]["error_message"]


@pytest.mark.parametrize(
    "prepared_rows",
    [[], [{"stock_code": "000001"}]],
)
def test_daily_backfill_allows_only_explicitly_proven_missing_codes(
    monkeypatch,
    prepared_rows,
):
    requested_codes = ["600000"] if not prepared_rows else ["000001", "600000"]
    run_events = _patch_daily_dependencies(monkeypatch, prepared_rows)
    local_engine = _DisposableEngine("probiga_qmt_history")

    result = local_history.backfill_daily_kline_local(
        source_engine=object(),
        local_engine=local_engine,
        stock_codes=requested_codes,
        allowed_missing_stock_codes=["600000"],
        start_date="2026-08-19",
        end_date="2026-08-19",
        batch_size=2,
        dry_run=True,
    )

    assert result.status == "SUCCESS"
    assert result.batches[0].allowed_missing_codes == ("600000",)
    assert result.batches[0].written_rows == 0
    assert run_events[0][1]["extra"]["allowed_missing_stock_codes"] == [
        "600000"
    ]
    assert run_events[-1][1]["status"] == "SUCCESS"


def test_daily_backfill_deduplicates_normalized_requested_codes(monkeypatch):
    run_events = _patch_daily_dependencies(
        monkeypatch,
        [{"stock_code": "000001"}, {"stock_code": "600000"}],
    )

    result = local_history.backfill_daily_kline_local(
        source_engine=object(),
        local_engine=_DisposableEngine("probiga_qmt_history"),
        stock_codes=["000001.SZ", "600000", "000001"],
        start_date="2026-08-19",
        end_date="2026-08-19",
        batch_size=10,
        dry_run=True,
    )

    assert result.code_count == 2
    assert result.batches[0].requested_codes == 2
    assert run_events[0][1]["requested_codes"] == 2
    assert run_events[-1][1]["status"] == "SUCCESS"


def test_daily_backfill_rebinds_transport_batches_to_formal_source_root(
    monkeypatch,
):
    prepared_rows = [{
        "stock_code": "000001",
        "trade_date": "2026-08-19",
        "batch_id": "transient-bridge-request",
    }]
    run_events = _patch_daily_dependencies(monkeypatch, prepared_rows)
    persisted = []
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda _engine, *, rows, **_kwargs: persisted.extend(rows) or len(rows),
    )
    source_batch_id = "c" * 64

    result = local_history.backfill_daily_kline_local(
        source_engine=object(),
        local_engine=_DisposableEngine("probiga_qmt_history"),
        stock_codes=["000001"],
        start_date="2026-08-19",
        end_date="2026-08-19",
        batch_size=10,
        dry_run=False,
        source_batch_id=source_batch_id,
    )

    assert result.status == "SUCCESS"
    assert persisted[0]["batch_id"] == source_batch_id
    assert run_events[0][1]["extra"]["source_batch_id"] == source_batch_id


def test_daily_backfill_discards_920093_pre_listing_placeholder_before_write(
    monkeypatch,
):
    prepared_rows = [
        {"stock_code": "920093", "trade_date": day}
        for day in (
            "2026-08-12", "2026-08-21", "2026-08-24",
            "2026-08-25", "2026-08-26", "2026-08-27",
        )
    ]
    _patch_daily_dependencies(monkeypatch, prepared_rows)
    expected = {
        ("920093", day)
        for day in (
            "2026-08-21", "2026-08-24", "2026-08-25",
            "2026-08-26", "2026-08-27",
        )
    }
    written = []
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda *_args, **_kwargs: expected,
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda _engine, **kwargs: written.extend(kwargs["rows"])
        or len(kwargs["rows"]),
    )

    result = local_history.backfill_daily_kline_local(
        source_engine=object(),
        local_engine=_DisposableEngine("probiga_qmt_history"),
        stock_codes=["920093"],
        start_date="2026-08-06",
        end_date="2026-08-27",
        dry_run=False,
    )

    assert len(written) == 5
    assert all(row["trade_date"] != "2026-08-12" for row in written)
    assert result.discarded_outside_catalog_rows == 1
    assert result.batches[0].discarded_outside_catalog_rows == 1


def test_daily_backfill_keeps_all_four_real_expire_day_rows(monkeypatch):
    expire_rows = [
        {"stock_code": code, "trade_date": day, "volume": 1, "amount": 1}
        for code, day in (
            ("000004", "2026-07-13"),
            ("002808", "2026-07-13"),
            ("002898", "2026-07-16"),
            ("300029", "2026-07-09"),
        )
    ]
    _patch_daily_dependencies(monkeypatch, expire_rows)
    expected = {
        (row["stock_code"], row["trade_date"]) for row in expire_rows
    }
    written = []
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda *_args, **_kwargs: expected,
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda _engine, **kwargs: written.extend(kwargs["rows"])
        or len(kwargs["rows"]),
    )

    result = local_history.backfill_daily_kline_local(
        source_engine=object(),
        local_engine=_DisposableEngine("probiga_qmt_history"),
        stock_codes=["000004", "002808", "002898", "300029"],
        start_date="2026-07-09",
        end_date="2026-07-16",
        dry_run=False,
    )

    assert {(row["stock_code"], row["trade_date"]) for row in written} == expected
    assert result.discarded_outside_catalog_rows == 0


def test_daily_backfill_rejects_allowed_code_outside_requested_universe(
    monkeypatch,
):
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda _engine: (_ for _ in ()).throw(
            AssertionError("invalid allow-list must fail before schema access")
        ),
    )

    with pytest.raises(ValueError, match="must be included"):
        local_history.backfill_daily_kline_local(
            source_engine=object(),
            local_engine=object(),
            stock_codes=["000001"],
            allowed_missing_stock_codes=["600000"],
            start_date="2026-08-19",
            end_date="2026-08-19",
        )


def test_daily_backfill_rejects_empty_requested_universe_before_run(
    monkeypatch,
):
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda _engine: (_ for _ in ()).throw(
            AssertionError("empty universe must fail before schema access")
        ),
    )

    with pytest.raises(ValueError, match="at least one stock code"):
        local_history.backfill_daily_kline_local(
            source_engine=object(),
            local_engine=object(),
            stock_codes=[],
            start_date="2026-08-19",
            end_date="2026-08-19",
        )
