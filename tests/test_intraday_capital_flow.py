from __future__ import annotations

import copy
import importlib
import json
import sys
from contextlib import nullcontext
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text


def _crawler():
    return importlib.import_module("tools.crawl_minute_kline")


def _context(day="2026-09-11", clock="09:32:00"):
    return {
        "started_at": f"{day} {clock}", "decision_known_at": f"{day} {clock}",
        "requested_trade_date": day, "build_sha": "a" * 40, "run_uid": "b" * 32,
        "catalog_batch_id": "catalog", "catalog_manifest_hash": "c" * 64,
        "catalog_captured_at": f"{day} 08:00:00", "native_no_trade_evidence": None,
    }


def _line(kind, day="2026-09-11", minute="09:31"):
    return (f"{day} {minute},10,10.01,10.02,9.99,0,0,0.3,0.1,0.01,0" if kind == "stock"
            else f"{day} {minute},-1,0,3,4,5")


def _collection(monkeypatch, kind, native, *, minimum=0.5, no_trade=(), context=None, limit=0):
    c = _crawler()
    context = context or _context()
    day = context["started_at"][:10]
    now = datetime.fromisoformat(context["started_at"]) + timedelta(seconds=5)
    monkeypatch.setattr(c, "_now", lambda: now)
    for name in ("DELAY", "JITTER", "BATCH_EVERY", "RETRY_DELAY"):
        monkeypatch.setattr(c, name, 0)
    codes = [(code, 1 if code.startswith("6") else 0) for code in native]
    staged = []
    def fetch(code, market, **kwargs):
        value = native[code]
        if isinstance(value, BaseException):
            raise value
        return value
    def append(_connection, _stage, rows, *_args):
        staged.extend(copy.deepcopy(rows))
        return len(rows)
    publish = MagicMock(side_effect=lambda *args, **kwargs: len(staged))
    drop = MagicMock()
    monkeypatch.setattr(c, "fetch_minute_flow" if kind == "flow" else "fetch_minute_kline", fetch)
    stage_kind = "flow" if kind == "flow" else "kline"
    monkeypatch.setattr(c, f"_create_{stage_kind}_stage", lambda *args: ("stage", object()))
    monkeypatch.setattr(c, f"_append_{stage_kind}_stage", append)
    monkeypatch.setattr(c, f"_publish_{stage_kind}_stage", publish)
    monkeypatch.setattr(c, f"_drop_{stage_kind}_stage", drop)
    result = c._collect_minutes(object(), codes, kind=kind, table=c.DATASET_TABLES[kind],
                               trade_date=day, min_coverage=minimum, context=context,
                               no_trade_codes=set(no_trade), limit=limit, receipt_engine=object())
    return result, staged, publish, drop


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_old_day_is_missing_and_low_coverage_cannot_publish(monkeypatch, kind):
    result, rows, publish, drop = _collection(monkeypatch, kind, {
        "000001": [_line(kind)], "600000": [_line(kind, "2026-09-10")],
    }, minimum=0.75)
    assert result["coverage"] == 0.5
    assert result["status"] == "coverage_failed"
    assert result["written_rows"] == 0
    assert result["missing_codes_csv"] == "600000"
    assert [row["stock_code"] for row in rows] == ["000001"]
    publish.assert_not_called()
    drop.assert_called_once()


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_published_partial_keeps_exact_missing_and_value_proof(monkeypatch, kind):
    result, rows, publish, drop = _collection(monkeypatch, kind, {
        "000001": [_line(kind)], "600000": None,
    })
    c = _crawler()
    assert result["acquisition_status"] == "PARTIAL"
    assert result["written_rows"] == 1
    assert result["missing_count"] == 1
    assert result["missing_codes_sha256"] == c._digest(["600000"])
    assert result["business_rows_sha256"] == c._digest([
        ["000001", 1, c._code_rows_digest(rows, kind), str(rows[0]["received_at"])]
    ])
    assert c.validate_result(result) == "degraded"
    publish.assert_called_once()
    drop.assert_called_once()


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_no_trade_requires_proof_and_actual_bar_contradiction_stays_missing(monkeypatch, kind):
    result, rows, _, _ = _collection(monkeypatch, kind, {
        "000001": [_line(kind)], "000002": None, "600000": [_line(kind)],
    }, no_trade={"000002", "600000"})
    assert result["no_trade_codes_csv"] == "000002"
    assert result["missing_codes_csv"] == "600000"
    assert result["failure_reason_counts"] == {"NO_TRADE_CONTRADICTION": 1}
    assert [row["stock_code"] for row in rows] == ["000001"]
    assert result["coverage"] == 2 / 3


def test_invalid_identity_cannot_borrow_daily_no_trade(monkeypatch):
    c = _crawler()
    result, _, _, _ = _collection(monkeypatch, "flow", {
        "000001": [_line("flow")], "000002": c.MinuteSourceError("WRONG_IDENTITY"),
    }, no_trade={"000002"})
    assert result["missing_codes_csv"] == "000002"
    assert result["no_trade_count"] == 0


def test_limit_preserves_full_catalog_denominator(monkeypatch):
    result, _, _, _ = _collection(monkeypatch, "stock", {
        "000001": [_line("stock")], "600000": [_line("stock")],
    }, limit=1)
    assert result["expected_count"] == 2
    assert result["coverage"] == 0.5
    assert result["missing_codes_csv"] == "600000"


def test_cleanup_failure_does_not_mask_source_failure(monkeypatch):
    c = _crawler()
    monkeypatch.setattr(c, "_create_flow_stage", lambda *_: ("stage", object()))
    monkeypatch.setattr(c, "fetch_with_retries", MagicMock(side_effect=RuntimeError("source failed")))
    monkeypatch.setattr(c, "_drop_flow_stage", MagicMock(side_effect=RuntimeError("cleanup failed")))
    with pytest.raises(RuntimeError, match="source failed"):
        c.crawl_flow(object(), [("000001", 0)], 0, 1, trade_date="2026-09-11", context=_context())


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_closed_day_requires_full_grid_and_rejects_interior_gaps(monkeypatch, kind):
    c = _crawler()
    context = _context(clock="16:00:00")
    full = [_line(kind, minute=minute) for minute in c.MINUTE_GRID]
    result, rows, _, _ = _collection(monkeypatch, kind, {
        "000001": full, "600000": full[:90] + full[91:],
    }, context=context)
    assert len(rows) == 240
    assert result["missing_codes_csv"] == "600000"
    assert result["failure_reason_counts"] == {"INCOMPLETE_MINUTE_GRID": 1}


@pytest.mark.parametrize("bad", [None, "-", "", "NaN", "Infinity", "1e100", "1e-1000000", True])
def test_required_source_numbers_are_not_fabricated_as_zero(bad):
    with pytest.raises(ValueError):
        _crawler()._number(bad)


def test_native_kline_zero_trade_values_and_absent_average_are_preserved():
    c = _crawler()
    row = c.parse_kline("000001", [_line("stock")], trade_date="2026-09-11")[0]
    assert row["volume"] == row["amount"] == 0
    assert row["avg_price"] is None
    assert row["price"] == Decimal("10.01")
    assert row["change_pct"] == Decimal("0.1")


@pytest.mark.parametrize("replacement", ["2026-09-11 12:00", "nonsense"])
def test_invalid_native_minute_rejected(replacement):
    c = _crawler()
    with pytest.raises(ValueError):
        c.parse_flow("000001", [_line("flow").replace("2026-09-11 09:31", replacement)], trade_date="2026-09-11")


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("bad_identity", [{"code": "000002", "market": 0}, {"code": "000001", "market": 1}, {"code": "000001", "market": False}])
def test_native_source_identity_is_exact_and_never_retries_another_market(kind, bad_identity):
    c = _crawler()
    session = SimpleNamespace(get=MagicMock(return_value=SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {"data": {**bad_identity, "klines": [_line(kind)]}},
    )))
    with pytest.raises(c.MinuteSourceError, match="WRONG_IDENTITY"):
        c._fetch_native_minutes("000001", 0, session=session, trade_date="2026-09-11", kind=kind)
    session.get.assert_called_once()
    assert session.get.call_args.kwargs["params"]["secid"] == "0.000001"


def test_tls_verification_is_enabled():
    session = _crawler()._new_minute_session()
    try:
        assert session.verify is True
        assert session.trust_env is False
    finally:
        session.close()


def test_catalog_preserves_newly_listed_and_resumed_codes_and_native_bse_market():
    catalog = SimpleNamespace(members=[
        {"stock_code": "301686", "qmt_code": "301686.SZ"},
        {"stock_code": "002731", "qmt_code": "002731.SZ"},
        {"stock_code": "920002", "qmt_code": "920002.BJ"},
        {"stock_code": "600000", "qmt_code": "600000.SH"},
    ])
    assert _crawler().catalog_stock_codes(catalog, ["002731", "301686", "600000", "920002"]) == [
        ("002731", 0), ("301686", 0), ("600000", 1), ("920002", 0),
    ]


def test_flow_main_uses_primary_catalog_and_separate_minute_database(monkeypatch):
    c = _crawler()
    primary, minute = object(), object()
    catalog = SimpleNamespace(batch_id="catalog", manifest_hash="c" * 64, captured_at="2026-09-11 08:00:00",
        members=[{"stock_code": "301686", "qmt_code": "301686.SZ"}])
    load = MagicMock(return_value=(catalog, ["301686"]))
    collect = MagicMock(return_value={"status": "written"})
    monkeypatch.setattr(sys, "argv", ["crawl_minute_kline.py", "--type", "flow", "--trade-date", "2026-09-11", "--skip-closed", "--min-coverage", "0.5"])
    monkeypatch.setattr(c, "_now", lambda: datetime(2026, 9, 12, 10))
    monkeypatch.setattr(c, "create_batch_engine", lambda: primary)
    monkeypatch.setattr(c, "get_minute_engine", lambda: minute)
    monkeypatch.setattr(c, "get_kline_engine", MagicMock(side_effect=AssertionError("daily Kline catalog must not be read")))
    monkeypatch.setattr(c, "is_trading_time", MagicMock(side_effect=AssertionError("explicit historical date must not be skipped")))
    monkeypatch.setattr(c, "_is_trade_day", lambda *_: True)
    monkeypatch.setattr(c, "load_target_stock_catalog", load)
    monkeypatch.setattr(c, "verified_no_trade_codes", lambda *_a, **_kw: (set(), None))
    monkeypatch.setattr(c, "crawl_flow", collect)
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", "a" * 40)
    monkeypatch.delenv("PROBIGA_SCHEDULER_TASK_TYPE", raising=False)
    assert c.main() == 0
    assert load.call_args.args == (primary,)
    assert load.call_args.kwargs["target_date"] == "2026-09-11"
    assert collect.call_args.args == (minute, [("301686", 0)], 0, 0.5)


def test_task_dataset_mismatch_fails_before_database_or_provider(monkeypatch, capsys):
    c = _crawler()
    monkeypatch.setattr(sys, "argv", ["crawl_minute_kline.py", "--type", "flow"])
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", "intraday_minute_kline")
    create = MagicMock()
    monkeypatch.setattr(c, "create_batch_engine", create)
    assert c.main() == 2
    create.assert_not_called()
    assert json.loads(capsys.readouterr().out)["status"] == "failed"

def test_kline_stage_insert_failure_rolls_back_partition_delete(monkeypatch):
    crawler = _crawler()
    statements = []
    transaction = SimpleNamespace(saw_error=False)

    class _Result:
        def __init__(self, *, scalar_value=None, rows=None, rowcount=0):
            self._scalar_value = scalar_value
            self._rows = rows or []
            self.rowcount = rowcount

        def scalar(self):
            return self._scalar_value

        def fetchall(self):
            return self._rows

        def one(self):
            return self._rows[0]

    class _Transaction:
        def __enter__(self):
            return connection

        def __exit__(self, exc_type, _exc, _tb):
            transaction.saw_error = exc_type is not None
            return False

    class _Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            statements.append((sql, params))
            upper = sql.upper()
            if "SELECT COUNT(*)" in upper:
                return _Result(scalar_value=2)
            if "INFORMATION_SCHEMA.COLUMNS" in upper:
                return _Result(rows=[("stock_code",), ("trade_date",), ("price",)])
            if "MIN(TRADE_DATE)" in upper:
                return _Result(
                    rows=[
                        (
                            datetime(2026, 8, 25, 9, 30),
                            datetime(2026, 8, 25, 9, 31),
                        )
                    ]
                )
            if upper.lstrip().startswith("INSERT INTO"):
                raise RuntimeError("insert failed")
            return _Result(rowcount=2)

        def commit(self):
            return None

        def begin(self):
            return _Transaction()

    connection = _Connection()
    monkeypatch.setattr(crawler, "mysql_named_lock", lambda *args, **kwargs: nullcontext())

    def _receipt_database_down(*_args, **_kwargs):
        raise RuntimeError("receipt database unavailable")

    monkeypatch.setattr(
        crawler,
        "supersede_overlapping_qmt_minute_forward_receipts",
        _receipt_database_down,
    )
    with pytest.raises(RuntimeError, match="receipt database unavailable"):
        crawler._publish_kline_stage(
            object(),
            connection,
            "minute_stage",
            "sm_stock_minute",
            receipt_engine=object(),
        )
    assert not any(
        sql.upper().lstrip().startswith(("DELETE", "INSERT"))
        for sql, _ in statements
    )
    statements.clear()
    monkeypatch.setattr(
        crawler,
        "supersede_overlapping_qmt_minute_forward_receipts",
        lambda *args, **kwargs: 1,
    )

    with pytest.raises(RuntimeError, match="insert failed"):
        crawler._publish_kline_stage(
            object(),
            connection,
            "minute_stage",
            "sm_stock_minute",
            receipt_engine=object(),
        )

    assert transaction.saw_error is True
    assert any("DELETE TARGET_ROWS" in sql.upper() for sql, _ in statements)
    assert any("SELECT DISTINCT" in sql.upper() for sql, _ in statements)


def test_publish_flow_stage_replaces_day_in_one_transaction(monkeypatch):
    crawler = _crawler()
    statements = []

    class _Connection:
        def begin(self):
            return nullcontext(self)

        def execute(self, statement, params=None):
            statements.append((str(statement), params))
            return SimpleNamespace(rowcount=7)

    connection = _Connection()
    engine = object()
    monkeypatch.setattr(crawler, "mysql_named_lock", lambda *args, **kwargs: nullcontext())

    assert crawler._publish_flow_stage(
        engine,
        connection,
        "flow_stage",
        "2026-08-11",
    ) == 7
    assert len(statements) == 2
    assert statements[0][0].lstrip().upper().startswith("DELETE TARGET_ROWS")
    assert "SELECT DISTINCT STOCK_CODE" in statements[0][0].upper()
    assert "TRADE_TIME >=" in statements[0][0].upper()
    assert statements[1][0].lstrip().upper().startswith("INSERT INTO")
    assert "SELECT" in statements[1][0].upper()


def test_fetch_with_retries_recovers_transient_empty_result(monkeypatch):
    crawler = _crawler()
    fetcher = MagicMock(side_effect=[None, ["row"]])
    sleep = MagicMock()
    monkeypatch.setattr(crawler, "FETCH_ATTEMPTS", 2)
    monkeypatch.setattr(crawler, "RETRY_DELAY", 0.25)
    monkeypatch.setattr(crawler.time, "sleep", sleep)

    assert crawler.fetch_with_retries(fetcher, "000001", 0) == ["row"]
    assert fetcher.call_count == 2
    sleep.assert_called_once_with(0.25)


def test_batch_router_and_scheduler_metadata_use_minute_engine(monkeypatch):
    from server.common import batch_db, minute_data, scheduler_validation

    primary_engine = SimpleNamespace(connect=MagicMock())

    class _Result:
        def mappings(self):
            return self

        def all(self):
            return [{"COLUMN_NAME": "stock_code"}, {"COLUMN_NAME": "trade_time"}]

    minute_connection = SimpleNamespace(execute=lambda statement, params: _Result())
    minute_engine = SimpleNamespace(connect=lambda: nullcontext(minute_connection))
    monkeypatch.setattr(batch_db, "should_use_kline_engine", lambda _sql: False)
    monkeypatch.setattr(minute_data, "get_minute_engine", lambda: minute_engine)

    sql = "SELECT * FROM sm_stock_capital_flow_min"
    assert batch_db.routed_read_engine(sql, primary_engine) is minute_engine
    assert scheduler_validation._table_columns(primary_engine, "sm_stock_capital_flow_min") == {
        "stock_code", "trade_time",
    }
    primary_engine.connect.assert_not_called()


def test_intraday_flow_validation_accepts_first_sparse_bar_per_stock():
    from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS

    requirement = TASK_OUTPUT_REQUIREMENTS["intraday_minute_flow"][0]
    assert requirement.min_rows == 5000
    assert requirement.min_distinct == 5000


def _task(kind, *, old=False, explicit=True):
    task_type = ({"stock": "stock_minute", "flow": "stock_minute_flow"} if old
                 else {"stock": "intraday_minute_kline", "flow": "intraday_minute_flow"})[kind]
    return {
        "id": 42 if kind == "stock" else 43,
        "task_type": task_type, "script_path": "tools/crawl_minute_kline.py",
        "script_args": f"--type {kind} --min-coverage 0.5" + (" --trade-date 2026-09-11" if explicit else " --skip-closed"),
        "_scheduler_expected_build_sha": "a" * 40, "_scheduler_history_run_uid": "b" * 32,
        "_scheduler_target_trade_date": "2026-09-11",
    }


def _replay_database(monkeypatch, result, rows, *, native_codes=(), native_ref=None):
    c = _crawler()
    from server.common import batch_db, minute_data
    primary = create_engine("sqlite://")
    target = create_engine("sqlite://")
    catalog = SimpleNamespace(batch_id="catalog", manifest_hash="c" * 64,
                             captured_at="2026-09-11 08:00:00")
    expected = sorted({row["stock_code"] for row in rows}
                      | set(c._parse_codes_csv(result["missing_codes_csv"]))
                      | set(c._parse_codes_csv(result["no_trade_codes_csv"])))
    load = MagicMock(return_value=(catalog, expected))
    monkeypatch.setattr(c, "load_target_stock_catalog", load)
    monkeypatch.setattr(c, "_is_trade_day", lambda *_: True)
    monkeypatch.setattr(c, "verified_no_trade_codes", lambda *_a, **_kw: (set(native_codes), native_ref))
    # Use the actual router, with distinct real databases. The primary deliberately
    # has no minute table, so a missing route cannot accidentally pass the replay.
    monkeypatch.setattr(batch_db, "get_kline_engine", lambda: target)
    monkeypatch.setattr(minute_data, "get_minute_engine", lambda: target)
    table = result["table"]
    fields = c.VALUE_FIELDS[result["dataset"]]
    columns = ", ".join(f"`{field}` NUMERIC" for field in fields)
    with target.begin() as connection:
        connection.execute(text(f"CREATE TABLE `{table}` (stock_code TEXT, trade_time TEXT, trade_date TEXT, "
                                f"etl_sync_at TEXT, source_time TEXT, received_at TEXT, data_source TEXT, {columns})"))
        for row in rows:
            record = {"stock_code": row["stock_code"], "trade_time": str(row["trade_time"]),
                      "trade_date": result["trade_date"],
                      "etl_sync_at": result["finished_at"], "source_time": str(row["source_time"]),
                      "received_at": str(row["received_at"]), "data_source": row["data_source"],
                      **{field: float(row[field]) if row[field] is not None else None for field in fields}}
            names = list(record)
            connection.execute(text(f"INSERT INTO `{table}` ({','.join('`'+name+'`' for name in names)}) "
                                    f"VALUES ({','.join(':'+name for name in names)})"), record)
    return primary, target, load


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("old", [False, True])
def test_partial_receipt_independently_replays_catalog_and_real_split_database(monkeypatch, kind, old):
    c = _crawler()
    from server.common import scheduler_validation as validation
    result, rows, _, _ = _collection(monkeypatch, kind, {"000001": [_line(kind)], "600000": None})
    primary, target, load = _replay_database(monkeypatch, result, rows)
    output = c._result_json(result)
    task = _task(kind, old=old)
    try:
        assert validation.scheduler_output_status(task, output, return_code=0) == "degraded"
        checked = validation.validate_scheduler_task_result(
            task, engine=primary, started_at=datetime(2026, 9, 11, 9, 32),
            now=datetime(2026, 9, 11, 9, 33), output=output,
        )
        assert checked.checked and checked.ok, checked.message
        load.assert_called_once_with(primary, target_date="2026-09-11",
                                     decision_known_at=datetime(2026, 9, 11, 9, 32), batch_id="catalog")
    finally:
        primary.dispose()
        target.dispose()


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("corruption", ["value", "stale_etl", "foreign_code", "duplicate", "missing", "timestamp", "future", "catalog"])
def test_persisted_mutations_cannot_replay_success(monkeypatch, kind, corruption):
    c = _crawler()
    result, rows, _, _ = _collection(monkeypatch, kind, {"000001": [_line(kind)], "600000": None})
    primary, target, load = _replay_database(monkeypatch, result, rows)
    table = result["table"]
    sql = {
        "value": f"UPDATE `{table}` SET `{c.VALUE_FIELDS[kind][0]}`=123",
        "stale_etl": f"UPDATE `{table}` SET etl_sync_at='2026-09-10 09:32:00'",
        "foreign_code": f"UPDATE `{table}` SET stock_code='600000'",
        "duplicate": f"INSERT INTO `{table}` SELECT * FROM `{table}`",
        "missing": f"DELETE FROM `{table}`",
        "timestamp": f"UPDATE `{table}` SET trade_time='2026-09-11 09:31:01'",
        "future": f"UPDATE `{table}` SET trade_time='2026-09-11 09:33:00'",
    }
    try:
        if corruption == "catalog":
            load.return_value[0].manifest_hash = "d" * 64
        else:
            with target.begin() as connection:
                connection.execute(text(sql[corruption]))
        with pytest.raises(ValueError):
            c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 9, 32), now=datetime(2026, 9, 11, 9, 33))
    finally:
        primary.dispose()
        target.dispose()


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_native_no_trade_replay_rejects_existing_target_bars(monkeypatch, kind):
    c = _crawler()
    context = _context()
    context["native_no_trade_evidence"] = {"proof_sha256": "e" * 64}
    result, rows, _, _ = _collection(monkeypatch, kind, {
        "000001": [_line(kind)], "000002": None,
    }, no_trade={"000002"}, context=context)
    primary, target, _ = _replay_database(monkeypatch, result, rows, native_codes={"000002"},
                                         native_ref=context["native_no_trade_evidence"])
    try:
        assert c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 9, 32), now=datetime(2026, 9, 11, 9, 33)) == "success"
        with target.begin() as connection:
            table = result["table"]
            fields = ','.join('`'+name+'`' for name in (*c.VALUE_FIELDS[kind], "source_time", "received_at", "data_source", "trade_date"))
            connection.execute(text(f"INSERT INTO `{table}` (stock_code,trade_time,etl_sync_at,{fields}) "
                                    f"SELECT '000002',trade_time,etl_sync_at,{fields} FROM `{table}`"))
        with pytest.raises(ValueError):
            c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 9, 32), now=datetime(2026, 9, 11, 9, 33))
    finally:
        primary.dispose()
        target.dispose()


@pytest.mark.parametrize("mutation", [
    {"missing_codes_csv": "600000,600000"}, {"missing_codes_csv": "SH600000"},
    {"missing_codes_csv": "600001"}, {"missing_count": True}, {"coverage": float("nan")},
    {"min_coverage": 0.49}, {"acquisition_status": "COMPLETE"}, {"written_rows": 2},
    {"run_uid": "c" * 32}, {"build_sha": "d" * 40}, {"dataset": "stock"},
    {"trade_date": "2026-09-10"}, {"requested_trade_date": None},
])
def test_forged_partial_receipt_cannot_pass_status_gate(monkeypatch, mutation):
    c = _crawler()
    from server.common import scheduler_validation as validation
    result, _, _, _ = _collection(monkeypatch, "flow", {"000001": [_line("flow")], "600000": None})
    result.update(mutation)
    try:
        result = c._seal_result(result)
        output = c._result_json(result)
    except ValueError:
        output = json.dumps(result)
    assert validation.scheduler_output_status(_task("flow"), output, return_code=0) == "failed"


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("old", [False, True])
def test_fixed_closed_skip_and_task_type_binding(kind, old):
    from server.common import scheduler_validation as validation
    payload = json.dumps({"status": "skipped", "reason": "market_closed", "now": "2026-09-11 16:00:00"})
    task = _task(kind, old=old, explicit=False)
    assert validation.scheduler_output_status(task, payload, return_code=0) == "skipped"
    task["script_args"] = "--type " + ("flow" if kind == "stock" else "stock") + " --skip-closed"
    assert validation.scheduler_output_status(task, payload, return_code=0) == "failed"
    task = _task(kind, old=old)
    assert validation.scheduler_output_status(task, payload, return_code=0) == "failed"
    task["script_path"] = "tools/other.py"
    assert validation.scheduler_output_status(task, payload, return_code=0) == "failed"


@pytest.mark.parametrize("count, expected_ok", [(2780, True), (3500, False)])
def test_complete_partial_history_budget_is_checked_before_publication(monkeypatch, count, expected_ok):
    c = _crawler()
    from server.api import scheduler_runtime as runtime
    # At the production flow gate (50%), a 5560-name universe produces about
    # 21 KiB of compact missing codes and still fits the actual 24 KiB history.
    codes = [f"{index:06}" for index in range(count * 2)]
    native = {code: [_line("flow")] if index < count else None for index, code in enumerate(codes)}
    if not expected_ok:
        with pytest.raises(ValueError, match="receipt budget"):
            _collection(monkeypatch, "flow", native)
        c._publish_flow_stage.assert_not_called()
        return
    result, _, _, _ = _collection(monkeypatch, "flow", native)
    output = c._result_json(result)
    assert len(output.encode()) <= 24000
    replay = runtime._history_validation_replay_output(output)
    assert json.loads(replay) == result
    evidence = runtime._build_history_validation_evidence(
        _task("flow"), run_uid="b" * 32, machine_output=output, status="degraded", exit_code=0,
        started_at=datetime(2026, 9, 11, 9, 32), validation_message="exact values verified",
    )
    parsed = runtime._history_validation_evidence(evidence)
    assert parsed["status"] == "degraded"
    assert json.loads(parsed["replay_output"]) == result


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_future_same_day_source_minutes_remain_missing(monkeypatch, kind):
    c = _crawler()
    result, rows, _, _ = _collection(monkeypatch, kind, {
        "000001": [_line(kind)],
        "600000": [_line(kind, minute=minute) for minute in c.MINUTE_GRID[:3]],
    })
    assert result["missing_codes_csv"] == "600000"
    assert result["failure_reason_counts"] == {"FUTURE_MINUTE": 1}
    assert len(rows) == 1


def test_stock_trade_date_column_is_independently_replayed(monkeypatch):
    c = _crawler()
    result, rows, _, _ = _collection(monkeypatch, "stock", {"000001": [_line("stock")]})
    primary, target, _ = _replay_database(monkeypatch, result, rows)
    try:
        with target.begin() as connection:
            connection.execute(text("UPDATE sm_stock_minute SET trade_date='2026-09-10'"))
        with pytest.raises(ValueError, match="persisted identity"):
            c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 9, 32), now=datetime(2026, 9, 11, 9, 33))
    finally:
        primary.dispose()
        target.dispose()


def test_source_precision_must_survive_existing_decimal_columns(monkeypatch):
    result, rows, _, _ = _collection(monkeypatch, "flow", {
        "000001": [_line("flow")], "600000": [_line("flow").replace(",-1,", ",0.1234567,")],
    })
    assert result["missing_codes_csv"] == "600000"
    assert result["failure_reason_counts"] == {"INVALID_PERSISTED_PRECISION": 1}
    assert len(rows) == 1


@pytest.mark.parametrize("capture, due", [
    ("2026-09-11 09:31:00", None), ("2026-09-11 09:35:00", "09:32"),
    ("2026-09-11 11:32:00", "11:29"), ("2026-09-11 11:35:00", "11:30"),
    ("2026-09-11 12:30:00", "11:30"), ("2026-09-11 13:02:00", "11:30"),
    ("2026-09-11 13:04:00", "13:01"), ("2026-09-11 14:00:00", "13:57"),
    ("2026-09-11 15:00:00", "15:00"), ("2026-09-12 00:00:01", "15:00"),
])
def test_source_due_watermark_handles_break_close_and_cross_day(capture, due):
    grid = _crawler()._required_grid("2026-09-11", datetime.fromisoformat(capture))
    assert (grid[-1] if grid else None) == due


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("clock", ["09:35:00", "12:30:00", "13:04:00", "14:00:00", "15:00:00"])
def test_stale_same_day_prefix_is_partial_and_valid_due_prefix_replays(monkeypatch, kind, clock):
    c = _crawler()
    context = _context(clock=clock)
    captured = datetime.fromisoformat(context["started_at"]) + timedelta(seconds=5)
    fresh = [_line(kind, minute=minute) for minute in c._required_grid("2026-09-11", captured)]
    result, rows, publish, _ = _collection(monkeypatch, kind, {
        "000001": fresh, "600000": [_line(kind)],
    }, context=context)
    assert result["acquisition_status"] == "PARTIAL"
    assert result["missing_codes_csv"] == "600000"
    assert result["failure_reason_counts"] == {"STALE_SOURCE_MINUTE": 1}
    assert result["source_delay_budget_seconds"] == 180
    publish.assert_called_once()
    primary, target, _ = _replay_database(monkeypatch, result, rows)
    try:
        assert c.replay_result(result, primary, started_at=datetime.fromisoformat(context["started_at"]),
                               now=captured + timedelta(seconds=1)) == "degraded"
    finally:
        primary.dispose()
        target.dispose()


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("received", ["2026-09-11 09:32:05", "2026-09-11 14:00:05"])
def test_forged_capture_cannot_make_afternoon_single_old_bar_fresh(monkeypatch, kind, received):
    c = _crawler()
    result, rows, _, _ = _collection(monkeypatch, kind, {"000001": [_line(kind)]})
    result.update(started_at="2026-09-11 14:00:00", decision_known_at="2026-09-11 14:00:00",
                  finished_at="2026-09-11 14:00:05")
    result["business_rows_sha256"] = c._digest([["000001", 1, c._code_rows_digest(rows, kind), received]])
    result = c._seal_result(result)
    rows[0]["received_at"] = datetime.fromisoformat(received)
    primary, target, _ = _replay_database(monkeypatch, result, rows)
    try:
        with pytest.raises(ValueError):
            c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 14), now=datetime(2026, 9, 11, 14, 1))
    finally:
        primary.dispose()
        target.dispose()


@pytest.mark.parametrize("field, value", [
    ("received_at", "2026-09-11 09:32:04"),
    ("source_time", "2026-09-11 09:32:00"),
    ("data_source", "other_provider"),
])
def test_capture_metadata_is_written_and_independently_bound(monkeypatch, field, value):
    c = _crawler()
    result, rows, _, _ = _collection(monkeypatch, "flow", {"000001": [_line("flow")]})
    assert rows[0]["source_time"] == rows[0]["trade_time"]
    assert rows[0]["received_at"] == datetime(2026, 9, 11, 9, 32, 5)
    assert rows[0]["data_source"] == c.PUBLIC_MINUTE_SOURCE
    primary, target, _ = _replay_database(monkeypatch, result, rows)
    try:
        with target.begin() as connection:
            connection.execute(text(f"UPDATE sm_stock_capital_flow_min SET `{field}`=:value"), {"value": value})
        with pytest.raises(ValueError):
            c.replay_result(result, primary, started_at=datetime(2026, 9, 11, 9, 32), now=datetime(2026, 9, 11, 9, 33))
    finally:
        primary.dispose()
        target.dispose()
