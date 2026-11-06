"""Real source/cache data survives an overwritten or failed minute receipt."""
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import numpy as np
import pytest
from sqlalchemy import create_engine, text

from integrations.qmt.datadir import CST, DAT_HEADER, MINUTE_PERIOD
from integrations.qmt.datadir_import import SOURCE, Universe, UniverseMember
from server.common import qmt_datadir_minute_coverage as coverage
from server.common import qmt_datadir_result as results
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
            finished_at TEXT,detail_json TEXT)"""))
        connection.execute(text("INSERT INTO qmt_datadir_import_partition VALUES ('stock_minute',:day,:root,'FAILED',2,482,:hash,'failed-new-run',:at,:at,:detail)"),
            dict(day=str(DAY), root=str(tmp_path), hash="b" * 64, at=str(DAY) + " 20:00:00",
                 detail=json.dumps({"error": "2013 read timeout", "staged_rows_retained": True})))
        connection.execute(text("CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY,build_sha TEXT,output TEXT,task_type TEXT,status TEXT,finished_at TEXT,exit_code INTEGER,run_at TEXT)"))
        connection.execute(text("CREATE TABLE qmt_datadir_minute_stage (run_id TEXT,stock_code TEXT,trade_time TEXT,trade_date TEXT,price REAL,`change` REAL,change_pct REAL,volume REAL,amount REAL,qmt_code TEXT)"))
    def write_receipt(connection, **kwargs):
        connection.execute(text("UPDATE qmt_datadir_import_partition SET status=:status,run_id=:run_id,source_root=:root,row_count=:row_count,code_count=:code_count,content_sha256=:content_sha256,finished_at=:finished,detail_json=:detail"),
            {**kwargs, "finished": "2026-11-03 04:40:00" if kwargs.get("finished") else None,
             "detail": json.dumps(kwargs.get("detail") or {})})
    monkeypatch.setattr(importer, "write_receipt", write_receipt)
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


@pytest.mark.parametrize("case", ["current", "revised", "missing", "other_source", "shortfall"])
def test_minute_cli_checks_current_daily_authority_before_reusing_complete_receipt(
        existing_partition, monkeypatch, capsys, case):
    item = existing_partition
    day = str(DAY)
    current_hash = item.scope["full_market_manifest_hash"]
    receipt_hash = current_hash if case == "current" else "b" * 64
    actual = coverage.persisted_matrix(item.engine, day, scope=item.scope)
    detail = {**importer._completion_detail(actual), "full_market_manifest_hash": receipt_hash}
    with item.engine.begin() as connection:
        importer.write_receipt(connection, session=DAY, root=str(item.root), status="COMPLETE",
            run_id="original-minute-run", code_count=2, row_count=482, content_sha256="c" * 64,
            detail=detail, finished=True)
        connection.execute(text("""CREATE TABLE qmt_history_coverage_manifest (
            trade_date TEXT, manifest_hash TEXT, dataset TEXT, provider TEXT,
            schema_version TEXT, status TEXT, strategy_eligible INTEGER, captured_at TEXT)"""))
        if case != "missing":
            connection.execute(text("""INSERT INTO qmt_history_coverage_manifest VALUES
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
