"""Real source/cache data survives an overwritten or failed minute receipt."""
from datetime import date, datetime
import json
from pathlib import Path
import re
import sqlite3
from time import monotonic
from types import SimpleNamespace

import numpy as np
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from integrations.qmt.datadir import CST, DAT_HEADER, MINUTE_PERIOD
from integrations.qmt import datadir_import
from integrations.qmt.datadir_import import ATTEMPT_DATASET, SOURCE, Universe, UniverseMember
from server.common.auxiliary_runtime_schema import QMT_DATADIR_STAGE_INDEX_NAME
from server.common import qmt_datadir_minute_coverage as coverage
from server.common import qmt_datadir_result as results
from server.common.qmt_minute_publication_scope import (
    load_verified_minute_security_rows, seal_minute_security_publication,
)
from tools import import_qmt_datadir_history as importer


DAY = date(2026, 11, 2)
CODES = ("000001", "600000")


@pytest.fixture
def existing_partition(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path / "jobs"))
    engine = create_engine("sqlite:///:memory:", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    native = [dict(stock_code=code, trade_time=str(DAY) + " " + moment, trade_date=str(DAY),
        price=10, change=0, change_pct=0, volume=100, amount=1000,
        qmt_code=code + (".SH" if code.startswith("6") else ".SZ"),
        data_source="qmt_datadir", source_time=str(DAY) + " " + moment,
        batch_id="original-source-batch", data_version="qmt_datadir_v1",
        quality_status="VERIFIED", permission_status="SUPPORTED")
        for code in CODES for moment in coverage.CORE_GRID]
    scope = dict(codes=CODES, full_market_manifest_hash="a" * 64,
        daily_close_by_code={code: "10" for code in CODES}, eligible_code_count=2,
        expected_traded_code_set_sha256=importer.canonical_digest(list(CODES)))
    with engine.begin() as connection:
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-03 04:40:00")
        connection.execute(text("CREATE TABLE sm_stock_minute (id INTEGER PRIMARY KEY," + ",".join(key + " " + (
            "REAL" if key in {"price", "change", "change_pct", "volume", "amount"} else "TEXT")
            for key in native[0]) + ")"))
        connection.execute(text("INSERT INTO sm_stock_minute (" + ",".join("`"+key+"`" for key in native[0]) +
                                ") VALUES (" + ",".join(":" + key for key in native[0]) + ")"), native)
        connection.execute(text("ALTER TABLE sm_stock_minute ADD COLUMN etl_sync_at TEXT"))
        connection.execute(text("ALTER TABLE sm_stock_minute ADD COLUMN received_at TEXT"))
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT,trade_date DATE,source_root TEXT,status TEXT,code_count INTEGER,
            row_count INTEGER,content_sha256 TEXT,run_id TEXT,started_at TEXT,
            finished_at TEXT,detail_json TEXT,PRIMARY KEY(dataset,trade_date))"""))
        connection.execute(text("INSERT INTO qmt_datadir_import_partition VALUES ('stock_minute',:day,:root,'FAILED',2,482,:hash,'failed-new-run',:at,:at,:detail)"),
            dict(day=str(DAY), root=str(tmp_path), hash="b" * 64, at=str(DAY) + " 20:00:00",
                 detail=json.dumps({"error": "2013 read timeout", "staged_rows_retained": True})))
        connection.execute(text("CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY,build_sha TEXT,output TEXT,task_type TEXT,status TEXT,finished_at TEXT,exit_code INTEGER,run_at TEXT)"))
        connection.execute(text("CREATE TABLE qmt_datadir_minute_stage (run_id TEXT,stock_code TEXT,trade_time TEXT,trade_date TEXT,price REAL,`change` REAL,change_pct REAL,volume REAL,amount REAL,qmt_code TEXT)"))
        connection.execute(text("""CREATE TABLE qmt_history_coverage_manifest (
            trade_date DATE, manifest_hash TEXT PRIMARY KEY, dataset TEXT, provider TEXT,
            schema_version TEXT, status TEXT, strategy_eligible INTEGER, captured_at TEXT,
            period TEXT, run_id TEXT, grid_profile TEXT, expected_entity_count INTEGER,
            entity_count INTEGER, expected_traded_count INTEGER, actual_traded_count INTEGER,
            bar_count INTEGER, expected_entity_set_hash TEXT, expected_traded_set_hash TEXT,
            actual_traded_set_hash TEXT, no_trade_set_hash TEXT, entity_root_hash TEXT,
            manifest_json TEXT, created_at TEXT)"""))
    monkeypatch.setattr(importer, "trade_scope", lambda *_args, **_kwargs: scope)
    universe = []
    for code in CODES:
        market = "SH" if code.startswith("6") else "SZ"
        universe.append(UniverseMember(code + "." + market, market, code))
        records = np.zeros((len(coverage.CORE_GRID), 16), dtype="<u4")
        for index, moment in enumerate(coverage.CORE_GRID):
            stamp = datetime.fromisoformat(str(DAY) + " " + moment).replace(tzinfo=CST)
            records[index, 0] = int(stamp.timestamp())
            records[index, 4] = records[index, 13] = 10000
            records[index, 6], records[index, 8] = 100, 1000
        path = tmp_path / market / MINUTE_PERIOD / (code + ".DAT")
        path.parent.mkdir(parents=True)
        path.write_bytes(DAT_HEADER + records.tobytes())
    yield SimpleNamespace(engine=engine, scope=scope, root=tmp_path, universe=universe)
    engine.dispose()


def _stored_rows(engine):
    with engine.connect() as connection:
        return connection.execute(text("SELECT * FROM sm_stock_minute ORDER BY stock_code,trade_time")).fetchall()


def _publish_original(item):
    old_scope = {**item.scope, "full_market_manifest_hash": "b" * 64}
    actual = coverage.persisted_matrix(item.engine, str(DAY), scope=old_scope)
    with item.engine.begin() as connection:
        importer.write_receipt(connection, session=DAY, root=str(item.root), status="COMPLETE",
            run_id="original-minute-run", code_count=2, row_count=482, content_sha256="c" * 64,
            detail=importer._completion_detail(actual), finished=True)
        connection.execute(text("UPDATE qmt_datadir_import_partition SET finished_at=:published "
                                "WHERE dataset='stock_minute'"), {"published": str(DAY) + " 19:00:00"})
        connection.execute(text("""INSERT INTO qmt_history_coverage_manifest
            (trade_date,manifest_hash,dataset,provider,schema_version,status,strategy_eligible,captured_at) VALUES
            (:day,:old,'stock_daily','qmt_datadir',:schema,'EXACT',1,:old_at),
            (:day,:new,'stock_daily','qmt_datadir',:schema,'EXACT',1,'2026-11-03 03:00:00')"""),
            {"day": str(DAY), "old": old_scope["full_market_manifest_hash"],
             "new": item.scope["full_market_manifest_hash"], "schema": coverage.DAILY_SCHEMA,
             "old_at": str(DAY) + " 18:00:00"})
    return old_scope


def test_rebind_keeps_prior_cutoff_proof_without_rewriting_minutes(existing_partition, monkeypatch):
    item = existing_partition
    old_scope = _publish_original(item)
    old_cutoff = str(DAY) + " 20:00:00"
    monkeypatch.setattr(coverage, "trade_scope", lambda *_a, decision_known_at, **_kw:
        old_scope if decision_known_at == datetime.fromisoformat(old_cutoff) else item.scope)
    first_proof = coverage.load_verified_minute_publication(item.engine, str(DAY), decision_known_at=old_cutoff)
    before = _stored_rows(item.engine)

    assert importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope)

    assert _stored_rows(item.engine) == before
    assert coverage.load_verified_minute_publication(item.engine, str(DAY), decision_known_at=old_cutoff) == first_proof
    current = coverage.load_verified_minute_publication(item.engine, str(DAY),
        decision_known_at="2026-11-03 05:00:00")
    assert current["full_market_manifest_hash"] == item.scope["full_market_manifest_hash"]
    assert current["row_hash"] == first_proof["row_hash"]
    assert current["captured_at"] == "2026-11-03 04:40:00"


def test_stage_timeout_preserves_old_complete_publication_and_retries_landed_run(existing_partition, monkeypatch):
    item = existing_partition
    old_scope = _publish_original(item)
    with item.engine.begin() as connection:
        connection.execute(text("INSERT INTO qmt_datadir_minute_stage SELECT 'retry-run',stock_code,trade_time,trade_date,price,`change`,change_pct,volume,amount,qmt_code FROM sm_stock_minute"))
    before = _stored_rows(item.engine)
    original = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))

    def fail_publication(connection, **_kwargs):
        connection.execute(text("DELETE FROM sm_stock_minute WHERE stock_code='000001'"))
        raise TimeoutError("simulated minute publication timeout")

    with monkeypatch.context() as patch:
        patch.setattr(importer, "publish_session", fail_publication)
        failure = importer._publish_one(item.engine, session=DAY, scope=item.scope,
            root=item.root, run_id="retry-run", replace=False, replace_sources=[],
            entry=dict(rows=482, codes=list(CODES), digest="d" * 64))

    assert failure["status"] == "FAILED"
    assert _stored_rows(item.engine) == before
    assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY)) == original
    old_cutoff = str(DAY) + " 20:00:00"
    def sealed_scope(*_args, decision_known_at, verify_rows):
        assert not verify_rows  # Later corrected daily rows differ from the original immutable scope.
        return old_scope if decision_known_at == datetime.fromisoformat(old_cutoff) else item.scope

    monkeypatch.setattr(coverage, "trade_scope", sealed_scope)
    assert coverage.load_verified_minute_publication(item.engine, str(DAY), decision_known_at=old_cutoff)["row_count"] == 482
    with pytest.raises(ValueError, match="differs from the daily scope"):
        coverage.load_verified_minute_publication(item.engine, str(DAY), decision_known_at="2026-11-03 05:00:00")
    attempt = results.load_receipt(item.engine, dataset=ATTEMPT_DATASET, trade_date=str(DAY))
    assert attempt["run_id"] == "retry-run" and attempt["status"] == "FAILED"
    assert attempt["detail"]["staged_rows_retained"]
    retry = importer._resume_staged_partition(item.engine, session=DAY, scope=item.scope,
        replace=False, replace_sources=[])
    assert retry["status"] == "COMPLETE"
    assert results.load_receipt(item.engine, dataset=ATTEMPT_DATASET, trade_date=str(DAY)) is None
    published = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))
    assert published["run_id"] == "retry-run" and published["status"] == "COMPLETE"


def _bounded_mysql_publication(connection, *, attempts, fail_batch, **kwargs):
    """Run the actual indexed reconciliation and INSERT SELECT on SQLite."""
    driver = connection.connection.driver_connection

    def execute(statement, parameters):
        sql = statement.text
        if "information_schema.COLUMNS" in sql:
            sql = sql.replace("information_schema.COLUMNS", "test_mysql_columns")
            sql = sql.replace("TABLE_SCHEMA=DATABASE() AND ", "")
        sql = re.sub(r"FORCE INDEX \(([^)]+)\)", r"INDEXED BY \1", sql)
        sql = re.sub(r"CAST\(([^()]+) AS BINARY\)", r"\1 COLLATE BINARY", sql)
        adapted = text(sql).bindparams(*statement._bindparams.values())
        if not sql.lstrip().startswith(f"INSERT INTO `{datadir_import.TARGET_TABLE}`"):
            return connection.execute(adapted, parameters)
        attempt = dict(codes=tuple(parameters["codes"]), inserted=None, steps=0)
        attempts.append(attempt)
        budget = 1000 if len(attempts) == fail_batch else 500_000

        def budget_progress():
            attempt["steps"] += 100
            return int(attempt["steps"] == budget)

        driver.set_progress_handler(budget_progress, 100)
        try:
            inserted = connection.execute(adapted, parameters)
            attempt["inserted"] = inserted.rowcount
            return inserted
        except OperationalError as exc:
            if "interrupted" not in str(exc.orig):
                raise
            raise OperationalError(sql, {}, Exception(2013, "bounded minute INSERT SELECT reply timed out")) from exc
        finally:
            driver.set_progress_handler(None, 0)

    mysql = SimpleNamespace(dialect=SimpleNamespace(name="mysql"), execute=execute)
    return datadir_import.publish_session(mysql, batch_codes=50, **kwargs)


def test_indexed_minute_insert_batches_fit_budget_and_commit_one_exact_receipt(existing_partition, monkeypatch):
    item = existing_partition
    _publish_original(item)
    day = str(DAY)
    codes = ("000001", *(f"{code:06d}" for code in range(600000, 600054)))
    run = "qdd_20261103044000_abcdef12"
    row_count = len(codes) * len(coverage.CORE_GRID)
    scope = {**item.scope, "codes": codes, "eligible_code_count": len(codes),
        "daily_close_by_code": dict.fromkeys(codes, "10"),
        "expected_traded_code_set_sha256": importer.canonical_digest(list(codes))}
    monkeypatch.setattr(importer, "trade_scope", lambda *_args, **_kwargs: scope)
    with item.engine.begin() as connection:
        connection.execute(text(f"CREATE INDEX `{QMT_DATADIR_STAGE_INDEX_NAME}` "
            "ON qmt_datadir_minute_stage(run_id,trade_date,stock_code,trade_time)"))
        connection.execute(text("CREATE INDEX idx_smm_date_source_code_time "
            "ON sm_stock_minute(trade_date,data_source,stock_code,trade_time)"))
        connection.execute(text("CREATE TABLE test_mysql_columns "
            "(TABLE_NAME, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_NAME)"))
        connection.execute(text("INSERT INTO test_mysql_columns VALUES "
            "('sm_stock_minute','utf8mb4','utf8mb4_bin','stock_code'),"
            "('qmt_datadir_minute_stage','utf8mb4','utf8mb4_bin','stock_code')"))
        connection.execute(text("INSERT INTO qmt_datadir_minute_stage VALUES "
            "(:run,:code,:stamp,:day,10,0,0,100,1000,:symbol)"), [
                dict(run=run, code=code, stamp=f"{day} {moment}", day=day,
                     symbol=code + (".SH" if code.startswith("6") else ".SZ"))
                for code in codes for moment in coverage.CORE_GRID
            ])

    original_rows = _stored_rows(item.engine)
    original_receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=day)
    entry = dict(rows=row_count, codes=list(codes), digest="d" * 64)
    attempts = []
    fail_batch = None

    def publish(connection, **kwargs):
        return _bounded_mysql_publication(connection, attempts=attempts, fail_batch=fail_batch, **kwargs)

    def attempt_publication():
        return importer._publish_one(item.engine, session=DAY, entry=entry, run_id=run,
            root=item.root, replace=False, replace_sources=[], scope=scope)

    def assert_failed_attempt_retained():
        assert _stored_rows(item.engine) == original_rows
        assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=day) == original_receipt
        failure = results.load_receipt(item.engine, dataset=ATTEMPT_DATASET, trade_date=day)
        assert failure["status"] == "FAILED" and failure["run_id"] == run
        assert failure["detail"]["staged_rows_retained"] is True
        with item.engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage "
                "WHERE run_id=:run"), {"run": run}).scalar_one() == row_count

    monkeypatch.setattr(importer, "publish_session", publish)
    # Replay the old 50-security statement under the identical per-query budget.
    with monkeypatch.context() as old_width:
        old_width.setattr(datadir_import, "MINUTE_PUBLICATION_BATCH_CODES", 50, raising=False)
        assert attempt_publication()["status"] == "FAILED"
    assert attempts == [dict(codes=codes[:50], inserted=None, steps=500_000)]
    assert_failed_attempt_retained()

    attempts.clear()
    fail_batch = 2
    assert attempt_publication()["status"] == "FAILED"
    assert [attempt["codes"] for attempt in attempts] == [codes[:16], codes[16:32]]
    assert attempts[0]["inserted"] == 14 * len(coverage.CORE_GRID)
    assert attempts[1]["inserted"] is None
    assert_failed_attempt_retained()
    log_path = item.root / "jobs" / "qmt-datadir-progress" / f"{run}.jsonl"
    logged = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    failure = [event for event in logged if event["phase"] == "server_publication" and event["status"] == "FAILED"][-1]
    assert (failure["sql_phase"], failure["batch_number"], failure["batch_security_count"]) == ("insert_missing", 2, 16)
    assert failure["completed_units"] == 16 * len(coverage.CORE_GRID)

    attempts.clear()
    fail_batch = None
    completed = importer._resume_staged_partition(item.engine, session=DAY, scope=scope,
        replace=False, replace_sources=[])
    assert completed["status"] == "COMPLETE" and completed["rows"] == row_count
    assert [attempt["codes"] for attempt in attempts] == [codes[offset:offset + 16] for offset in range(0, len(codes), 16)]
    assert all(0 < attempt["steps"] < 500_000 for attempt in attempts)
    assert sum(attempt["inserted"] for attempt in attempts) == row_count - len(original_rows)
    assert results.load_receipt(item.engine, dataset=ATTEMPT_DATASET, trade_date=day) is None
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=day)
    assert (receipt["status"], receipt["run_id"], receipt["content_sha256"]) == ("COMPLETE", run, entry["digest"])
    assert (receipt["row_count"], receipt["code_count"]) == (row_count, len(codes))
    matrix = coverage.persisted_matrix(item.engine, day, scope=scope)
    assert receipt["detail"]["row_hash"] == matrix["row_hash"]
    with item.engine.connect() as connection:
        proof, rows = load_verified_minute_security_rows(connection, connection,
            dataset="stock_minute", trade_date=day, decision_known_at="2026-11-03 05:00:00", codes=codes)
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar_one() == 0
    assert proof["run_id"] == run and proof["partition_row_hash"] == matrix["row_hash"]
    assert set(rows) == set(codes)
    assert all(len(bars) == len(coverage.CORE_GRID) for bars in rows.values())
    final_log = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert {event["budget_seconds"] for event in final_log if event.get("completed_securities") == len(codes)} == {2100}


def test_post_commit_cleanup_timeout_keeps_pass_and_retries_without_harvest(
    existing_partition, monkeypatch, capsys,
):
    item = existing_partition
    day = str(DAY)
    _publish_original(item)
    fields = "stock_code,trade_time,trade_date,price,`change`,change_pct,volume,amount,qmt_code"
    with item.engine.begin() as conn:
        native = [dict(row) for row in conn.execute(text("SELECT " + fields + " FROM sm_stock_minute")).mappings()]
        conn.execute(text("DELETE FROM sm_stock_minute WHERE stock_code='600000'"))
    monkeypatch.setattr(importer, "load_project_env", lambda: None)
    monkeypatch.setattr(importer, "get_minute_mysql_url", lambda: "sqlite://")
    monkeypatch.setattr(importer, "create_pooled_engine", lambda *_a, **_k: item.engine)
    monkeypatch.setattr(importer, "_sessions", lambda *_a: [DAY])
    monkeypatch.setattr(importer, "load_universe", lambda *_a, **_k: Universe(
        {member.qmt_code: member for member in item.universe}, {}))
    monkeypatch.setattr(importer, "require_schema", lambda *_a: None)
    monkeypatch.setattr(importer, "existing_sources", lambda *_a: {DAY: [SOURCE]})
    monkeypatch.setattr(importer, "new_run_id", lambda: "qdd_20261103043000_abcdef12")

    def harvest(conn, *, run_id, **_kwargs):
        params = ",".join(":" + name.strip("`") for name in fields.split(","))
        conn.execute(text("INSERT INTO qmt_datadir_minute_stage (run_id," + fields + ") VALUES (:run_id," + params + ")"),
                     [{**row, "run_id": run_id} for row in native])
        conn.commit()
        return {DAY: dict(rows=482, codes=list(CODES), code_rows={code: 241 for code in CODES}, digest="c" * 64)}

    monkeypatch.setattr(importer, "harvest", harvest)
    faults = []

    def cleanup_timeout(_conn, _cursor, sql, parameters, _context, _many):
        if sql.lstrip().startswith("DELETE FROM `qmt_datadir_minute_stage`"):
            faults.append(sql)
            raise OperationalError(sql, parameters, Exception(2013, "minute staging cleanup read timeout"))

    argv = ["--start", day, "--end", day, "--apply", "--json", "--datadir-root", str(item.root)]
    event.listen(item.engine, "before_cursor_execute", cleanup_timeout)
    try:
        assert importer.main(argv) == 0
    finally:
        event.remove(item.engine, "before_cursor_execute", cleanup_timeout)
    first = capsys.readouterr()
    assert json.loads(first.out.splitlines()[-1])["status"] == "PASS"
    assert "minute-stage-cleanup" in first.err and faults
    published = results.load_receipt(item.engine, dataset="stock_minute", trade_date=day)
    assert published["run_id"] == "qdd_20261103043000_abcdef12" and published["row_count"] == 482
    with item.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar_one() == 482

    monkeypatch.setattr(importer, "harvest", lambda *_a, **_k: pytest.fail("current publication must not reharvest"))
    assert importer.main(argv) == 0
    second = capsys.readouterr()
    assert json.loads(second.out.splitlines()[-1])["status"] == "PASS"
    assert any(json.loads(line)["status"] == "COMPLETE" for line in second.err.splitlines()
               if "minute-stage-cleanup" in line)
    assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=day) == published
    with item.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar_one() == 0


def test_cleanup_resumes_committed_batches_and_preserves_other_stage_scope(existing_partition):
    item = existing_partition
    _publish_original(item)
    with item.engine.begin() as conn:
        fields = "stock_code,trade_time,trade_date,price,`change`,change_pct,volume,amount,qmt_code"
        conn.execute(text("INSERT INTO qmt_datadir_minute_stage SELECT 'original-minute-run'," + fields +
                          " FROM sm_stock_minute"))
        conn.execute(text("INSERT INTO qmt_datadir_minute_stage SELECT 'other-run'," + fields +
                          " FROM sm_stock_minute LIMIT 1"))
    delete_attempts = []

    def interrupt_after_first_batch(_conn, _cursor, sql, params, _context, _many):
        if sql.lstrip().startswith("DELETE FROM `qmt_datadir_minute_stage`"):
            delete_attempts.append(params)
            if len(delete_attempts) > 1:
                raise OperationalError(sql, params, Exception(2013, "read timeout after first committed batch"))

    event.listen(item.engine, "before_cursor_execute", interrupt_after_first_batch)
    try:
        partial = importer.drop_staged(item.engine, "original-minute-run", session=DAY,
                                       deadline=monotonic() + 5, batch_rows=100)
    finally:
        event.remove(item.engine, "before_cursor_execute", interrupt_after_first_batch)
    assert partial["status"] == "DEFERRED" and partial["deleted_rows"] == 100
    assert len(delete_attempts) == 3
    with item.engine.connect() as conn:
        assert dict(conn.execute(text("SELECT run_id,COUNT(*) FROM qmt_datadir_minute_stage GROUP BY run_id")).all()) == {
            "original-minute-run": 382, "other-run": 1,
        }

    completed = importer.drop_staged(item.engine, "original-minute-run", session=DAY,
                                     deadline=monotonic() + 5, batch_rows=100)
    assert completed["status"] == "COMPLETE"
    assert completed["deleted_rows"] == 382 and completed["completed_batches"] == 4
    with item.engine.connect() as conn:
        assert conn.execute(text("SELECT run_id FROM qmt_datadir_minute_stage")).scalars().all() == ["other-run"]


@pytest.mark.parametrize("case", ["current", "revised", "missing", "other_source", "shortfall"])
def test_minute_cli_checks_current_daily_authority_before_reusing_complete_receipt(
        existing_partition, monkeypatch, capsys, case):
    item = existing_partition
    day = str(DAY)
    current_hash = item.scope["full_market_manifest_hash"]
    receipt_hash = current_hash if case == "current" else "b" * 64
    actual = coverage.persisted_matrix(item.engine, day, scope=item.scope,
        include_security_fingerprints=(case == "current"))
    detail = {**importer._completion_detail(actual), "full_market_manifest_hash": receipt_hash}
    with item.engine.begin() as connection:
        importer.write_receipt(connection, session=DAY, root=str(item.root), status="COMPLETE",
            run_id="original-minute-run", code_count=2, row_count=482, content_sha256="c" * 64,
            detail=detail, finished=True)
        if case == "current":
            seal_minute_security_publication(connection, dataset="stock_minute",
                trade_date=day, fingerprints=actual)
        if case != "missing":
            connection.execute(text("""INSERT INTO qmt_history_coverage_manifest
                (trade_date,manifest_hash,dataset,provider,schema_version,status,strategy_eligible,captured_at) VALUES
                (:day,:old,'stock_daily','qmt_datadir','probiga.qmt-datadir-daily-coverage.v1','EXACT',1,:older),
                (:day,:current,'stock_daily','qmt_datadir','probiga.qmt-datadir-daily-coverage.v1','EXACT',1,:newer),
                (:day,:future,'stock_daily','qmt_datadir','probiga.qmt-datadir-daily-coverage.v1','EXACT',1,:future_time)"""),
                {"day": day, "old": "b" * 64, "current": current_hash, "future": "d" * 64,
                 "older": day + " 16:00:00", "newer": day + " 19:00:00", "future_time": "9999-12-31 00:00:00"})
    monkeypatch.setattr(importer, "load_project_env", lambda: None)
    monkeypatch.setattr(importer, "get_minute_mysql_url", lambda: "sqlite://")
    monkeypatch.setattr(importer, "create_pooled_engine", lambda *_args, **_kwargs: item.engine)
    monkeypatch.setattr(importer, "_sessions", lambda *_args: [DAY])
    monkeypatch.setattr(importer, "load_universe", lambda *_args, **_kwargs: Universe(
        {member.qmt_code: member for member in item.universe}, {}))
    monkeypatch.setattr(importer, "require_schema", lambda *_args: None)
    monkeypatch.setattr(importer, "existing_sources", lambda *_args: {
        DAY: [SOURCE, "other"] if case == "other_source" else [SOURCE]})
    monkeypatch.setattr(importer, "_source_matrix", lambda *_args, **_kwargs:
        pytest.fail("revalidation must reuse the existing database proof"))
    if case == "missing":
        def no_authority(*_args, **_kwargs):
            raise ValueError("DATA_BLOCKED: exact daily manifest is required before minute publication")
        monkeypatch.setattr(importer, "trade_scope", no_authority)
    if case in {"current", "other_source"}:
        monkeypatch.setattr(importer, "trade_scope", lambda *_args, **_kwargs:
            pytest.fail("an unchanged or skipped partition must not reload the full daily scope"))
    if case == "shortfall":
        item.scope["daily_close_by_code"][CODES[0]] = "11"
        for source_file in item.root.rglob("*.DAT"):
            source_file.unlink()
    before = _stored_rows(item.engine)

    code = importer.main(["--start", day, "--end", day, "--apply", "--json",
                          "--datadir-root", str(item.root)])

    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=day)
    assert receipt["run_id"] == "original-minute-run"
    assert receipt["status"] == "COMPLETE"
    assert _stored_rows(item.engine) == before
    if case in {"current", "revised"}:
        assert code == 0 and payload["status"] == "PASS"
        assert payload["partitions"][0]["full_market_manifest_hash"] == current_hash
        assert receipt["detail"]["full_market_manifest_hash"] == current_hash
        assert bool(receipt["detail"].get("reused_existing_partition")) == (case == "revised")
    else:
        assert code != 0 and payload["status"] in {"BLOCKED", "FAILED"}
        assert receipt["detail"]["full_market_manifest_hash"] == receipt_hash


def test_failed_cursor_without_any_prior_history_reuses_exact_native_data_without_landing(existing_partition):
    item = existing_partition
    before = _stored_rows(item.engine)
    outcome = importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope,
        datadir_roots=[item.root], universe=item.universe)
    assert outcome["status"] == "COMPLETE" and outcome["reused_existing_partition"] is True
    assert _stored_rows(item.engine) == before  # Preserve original batch IDs and every value.
    with item.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar() == 0
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))
    assert receipt["status"] == "COMPLETE"
    assert receipt["detail"]["coverage"] == 1.0
    assert receipt["detail"]["full_market_manifest_hash"] == item.scope["full_market_manifest_hash"]


def test_bad_compressed_transport_does_not_authorize_replacement(existing_partition, monkeypatch):
    item = existing_partition
    before = _stored_rows(item.engine)

    def corrupt_readback(*_args, **_kwargs):
        return coverage._decode_matrix_group(dict(stock_code=CODES[0], row_count=241,
            packed_rows=b"\x01\x00\x00\x00bad-zlib"), expected_codes=set(CODES))

    monkeypatch.setattr(importer, "persisted_matrix", corrupt_readback)
    monkeypatch.setattr(importer, "_source_matrix", lambda *_args, **_kwargs: pytest.fail("bad transport authorized a source replacement"))
    with pytest.raises(RuntimeError, match="compressed package"):
        importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope,
            datadir_roots=[item.root], universe=item.universe)
    assert _stored_rows(item.engine) == before
    assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))["status"] == "FAILED"


@pytest.mark.parametrize("stored_pct,expected_reuse", [("11.111111", True), ("11.111112", False)])
def test_native_fallback_compares_nonterminating_percentage_at_actual_mysql_scale(
        existing_partition, stored_pct, expected_reuse):
    item = existing_partition
    # The native formula is (10 - 9) / 9 * 100 = 11.111... . Production
    # sm_stock_minute stores all five numeric business fields as DECIMAL(50,6).
    for member in item.universe:
        path = item.root / member.market / MINUTE_PERIOD / (member.code + ".DAT")
        records = np.frombuffer(path.read_bytes()[len(DAT_HEADER):], dtype="<u4").reshape(-1, 16).copy()
        records[:, 13] = 9000
        path.write_bytes(DAT_HEADER + records.tobytes())
    with item.engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_minute SET `change`=1,change_pct=:pct"), {"pct": stored_pct})
    before = _stored_rows(item.engine)
    outcome = importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope,
        datadir_roots=[item.root], universe=item.universe)
    assert bool(outcome) is expected_reuse
    assert _stored_rows(item.engine) == before
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))
    assert receipt["status"] == ("COMPLETE" if expected_reuse else "FAILED")
    with item.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar() == 0


@pytest.mark.parametrize(("delta", "stored_pct", "expected_reuse"), [
    (10, "0.039063", True),
    (-10, "-0.039063", True),
    (10, "0.039062", False),
])
def test_native_fallback_rounds_dat_midpoints_like_mysql(
        existing_partition, delta, stored_pct, expected_reuse):
    item = existing_partition
    moment = "09:53:00"
    index = coverage.CORE_GRID.index(moment)
    code = CODES[0]
    path = item.root / "SZ" / MINUTE_PERIOD / (code + ".DAT")
    records = np.frombuffer(path.read_bytes()[len(DAT_HEADER):], dtype="<u4").reshape(-1, 16).copy()
    records[index, 4] = 25600 + delta
    records[index, 13] = 25600
    path.write_bytes(DAT_HEADER + records.tobytes())
    with item.engine.begin() as connection:
        connection.execute(text(
            "UPDATE sm_stock_minute SET price=:price,`change`=:delta,change_pct=:pct "
            "WHERE stock_code=:code AND trade_time=:stamp"
        ), dict(price=str((25600 + delta) / 1000), delta=str(delta / 1000),
                pct=stored_pct, code=code, stamp=f"{DAY} {moment}"))
    before = _stored_rows(item.engine)
    outcome = importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope,
        datadir_roots=[item.root], universe=item.universe)
    assert bool(outcome) is expected_reuse
    assert _stored_rows(item.engine) == before


def test_prior_completed_history_preserves_original_identity_after_failed_cursor(existing_partition):
    item = existing_partition
    actual = coverage.persisted_matrix(item.engine, str(DAY), scope=item.scope)
    partition = dict(trade_date=str(DAY), status="COMPLETE", source_root=str(item.root),
        run_id="original-completed-run", row_count=482, code_count=2,
        readback_rows=482, readback_codes=2, row_hash=actual["row_hash"],
        content_sha256="c" * 64, coverage=5571 / 5578, eligible_code_count=5578)
    payload = results.build_result(dataset="stock_minute", sessions=[str(DAY)],
        partitions=[partition], build_sha="d" * 40)
    with item.engine.begin() as connection:
        connection.execute(text("INSERT INTO st_scheduled_task_history VALUES (1,:sha,:output,'qmt_stock_minute_canonical','failed','2026-11-03 01:00:00',0,'2026-11-03 00:34:29')"),
            {"sha": "d" * 40, "output": json.dumps(payload)})
    outcome = importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope)
    assert outcome["run_id"] == "original-completed-run"
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))
    assert receipt["content_sha256"] == "c" * 64
    assert receipt["detail"]["coverage"] == 1.0  # New exact daily scope, not the old threshold.


@pytest.mark.parametrize("change", ["source_value", "missing_bar", "wrong_anchor"])
def test_native_reuse_rejects_changed_missing_or_wrong_close_data(existing_partition, change):
    item = existing_partition
    with item.engine.begin() as connection:
        if change == "missing_bar":
            connection.execute(text("DELETE FROM sm_stock_minute WHERE stock_code='000001' AND trade_time=:at"), {"at": str(DAY) + " 09:31:00"})
        else:
            moment = "15:00:00" if change == "wrong_anchor" else "09:31:00"
            connection.execute(text("UPDATE sm_stock_minute SET price=11 WHERE stock_code='000001' AND trade_time=:at"), {"at": str(DAY) + " " + moment})
    assert importer._reuse_existing_partition(item.engine, session=DAY, scope=item.scope,
        datadir_roots=[item.root], universe=item.universe) is None
    assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))["status"] == "FAILED"


def test_failed_publication_retains_source_and_resumes_original_stage(existing_partition, monkeypatch):
    item = existing_partition
    with item.engine.begin() as connection:
        connection.execute(text("INSERT INTO qmt_datadir_minute_stage SELECT 'failed-new-run',stock_code,trade_time,trade_date,price,`change`,change_pct,volume,amount,qmt_code FROM sm_stock_minute"))
    def fail_publication(connection, **kwargs):
        connection.execute(text("DELETE FROM sm_stock_minute WHERE stock_code='000001'"))
        raise TimeoutError("minute publication lost read connection")
    before = _stored_rows(item.engine)
    monkeypatch.setattr(importer, "publish_session", fail_publication)
    failed = importer._resume_staged_partition(item.engine, session=DAY, scope=item.scope,
        replace=False, replace_sources=[])
    assert failed["status"] == "FAILED" and _stored_rows(item.engine) == before
    with item.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage")).scalar() == 482
    from integrations.qmt.datadir_import import publish_session
    monkeypatch.setattr(importer, "publish_session", publish_session)
    with item.engine.begin() as connection:
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-03 04:40:00")
    complete = importer._resume_staged_partition(item.engine, session=DAY, scope=item.scope,
        replace=False, replace_sources=[])
    assert complete["status"] == "COMPLETE"
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(DAY))
    assert receipt["run_id"] == "failed-new-run" and receipt["detail"]["coverage"] == 1.0
