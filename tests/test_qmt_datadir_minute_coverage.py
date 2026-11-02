from copy import deepcopy
from datetime import datetime
import json

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_datadir_minute_coverage as coverage
from server.common import qmt_datadir_result as result

DAY = "2026-11-02"
CODES = ("000001", "600000")


def scope():
    from server.common.qmt_attestation_contract import canonical_digest
    return dict(codes=CODES, full_market_manifest_hash="a" * 64,
        daily_close_by_code={code: "10" for code in CODES},
        eligible_code_count=2, expected_traded_code_set_sha256=canonical_digest(list(CODES)))


def rows():
    return [dict(stock_code=code, trade_time=DAY + " " + moment, trade_date=DAY,
        price=10, change=0, change_pct=0, volume=100, amount=1000,
        qmt_code=code + (".SH" if code.startswith("6") else ".SZ"),
        data_source="qmt_datadir", source_time=DAY + " " + moment,
        batch_id="source-batch", data_version="qmt_datadir_v1",
        quality_status="VERIFIED", permission_status="SUPPORTED")
        for code in CODES for moment in coverage.CORE_GRID]


def test_full_native_matrix_hash_is_identical_to_canonical_persisted_hash():
    native = rows()
    actual = coverage.validate_matrix(iter(native), day=DAY, scope=scope())
    assert actual["row_count"] == 482 and actual["code_count"] == 2
    assert actual["coverage"] == 1.0
    assert actual["row_hash"] == result.row_hash(native)


@pytest.mark.parametrize("failure", ["security", "timestamp", "duplicate", "wrong_day", "price",
                                       "source_time", "quality_status", "permission_status", "qmt_code", "anchor"])
def test_exact_minute_matrix_rejects_each_missing_or_invalid_dimension(failure):
    native = rows()
    if failure == "security":
        native = [row for row in native if row["stock_code"] != CODES[0]]
    elif failure == "timestamp":
        native.pop()
    elif failure == "duplicate":
        native[-1] = dict(native[-2])  # Count remains 482; one native minute is absent.
    elif failure == "wrong_day":
        native[-1]["trade_time"] = "2026-10-30 15:00:00"
    elif failure == "price":
        native[-1]["price"] = 0
    elif failure == "anchor":
        native[-1]["price"] = 11
    else:
        native[-1][failure] = "wrong"
    with pytest.raises(ValueError):
        coverage.validate_matrix(native, day=DAY, scope=scope())


def test_trade_scope_excludes_proven_suspensions_and_future_listing(monkeypatch):
    proof = dict(manifest_hash="a" * 64, entities=[
        dict(stock_code=code, classification="TRADED", evidence=dict(volume="100", amount="1000", close="10"))
        for code in CODES] + [
        dict(stock_code="000831", classification="NO_TRADE", evidence={}),
        dict(stock_code="001381", classification="NOT_LISTED", evidence={})])
    monkeypatch.setattr(coverage, "load_daily_manifest", lambda *a, **kw: proof)
    assert coverage.trade_scope(object(), DAY) == scope()
    monkeypatch.setattr(coverage, "load_daily_manifest", lambda *a, **kw: None)
    with pytest.raises(ValueError, match="exact daily manifest"):
        coverage.trade_scope(object(), DAY)


def test_real_database_minute_receipt_replay_rejects_mutation_and_stale_cutoff(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    native = rows()
    verified = coverage.validate_matrix(native, day=DAY, scope=scope())
    detail = {**verified, "readback_rows": 482, "readback_codes": 2}
    with engine.begin() as c:
        c.execute(text("CREATE TABLE sm_stock_minute (" + ",".join(key + " " + (
            "REAL" if key in {"price", "change", "change_pct", "volume", "amount"} else "TEXT")
            for key in native[0]) + ")"))
        c.execute(text("INSERT INTO sm_stock_minute VALUES (" + ",".join(":"+key for key in native[0]) + ")"), native)
        c.execute(text("""CREATE TABLE qmt_datadir_import_partition (
          dataset TEXT,trade_date TEXT,source_root TEXT,status TEXT,code_count INTEGER,
          row_count INTEGER,content_sha256 TEXT,run_id TEXT,started_at TEXT,
          finished_at TEXT,detail_json TEXT)"""))
        # Hash is computed over the database's canonical numeric representation.
    actual = coverage.persisted_matrix(engine, DAY, scope=scope())
    detail.update(actual)
    with engine.begin() as c:
        c.execute(text("INSERT INTO qmt_datadir_import_partition VALUES ('stock_minute',:day,'QMT','COMPLETE',2,482,:hash,'minute-run',:at,:at,:detail)"),
            dict(day=DAY, hash="b" * 64, at=DAY+" 20:00:00", detail=json.dumps(detail)))
    monkeypatch.setattr(coverage, "trade_scope", lambda *a, **kw: scope())
    proof = coverage.load_verified_minute_publication(engine, DAY, decision_known_at=DAY+" 20:01:00")
    assert proof["status"] == "EXACT" and proof["row_count"] == 482
    with pytest.raises(ValueError, match="decision cutoff"):
        coverage.load_verified_minute_publication(engine, DAY, decision_known_at=DAY+" 19:59:00")
    with engine.begin() as c:
        c.execute(text("UPDATE sm_stock_minute SET price=11 WHERE stock_code='600000' AND trade_time=:at"),
            {"at": DAY+" 09:31:00"})
    with pytest.raises(ValueError, match="receipt and persisted matrix"):
        coverage.load_verified_minute_publication(engine, DAY, decision_known_at=DAY+" 20:01:00")
    engine.dispose()


def test_minute_result_cannot_call_99_percent_a_completed_matrix():
    native = rows()
    actual = coverage.validate_matrix(native, day=DAY, scope=scope())
    partition = dict(trade_date=DAY, status="COMPLETE", source_root="QMT", run_id="minute-run",
        content_sha256="b"*64, readback_rows=482, readback_codes=2, **actual)
    payload = result.build_result(dataset="stock_minute", sessions=[DAY], partitions=[partition], build_sha="c"*40)
    assert result.validate_task_result("qmt_stock_minute_canonical", payload, return_code=0, expected_build_sha="c"*40) == "complete"
    partition["coverage"] = .99
    payload = result.build_result(dataset="stock_minute", sessions=[DAY], partitions=[partition], build_sha="c"*40)
    assert result.validate_task_result("qmt_stock_minute_canonical", payload, return_code=0, expected_build_sha="c"*40) == "failed"


def test_failed_publication_readback_rolls_back_minutes_and_records_failed_receipt(monkeypatch):
    from pathlib import Path
    from datetime import date
    from tools import import_qmt_datadir_history as importer

    engine = create_engine("sqlite:///:memory:")
    native = rows()
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sm_stock_minute (" + ",".join(key + " " + (
            "REAL" if key in {"price", "change", "change_pct", "volume", "amount"} else "TEXT")
            for key in native[0]) + ")"))
        connection.execute(text("INSERT INTO sm_stock_minute VALUES (" + ",".join(":"+key for key in native[0]) + ")"), native)
        connection.execute(text("CREATE TABLE receipt(status TEXT)"))
    before = coverage.persisted_matrix(engine, DAY, scope=scope())
    monkeypatch.setattr(importer, "trade_scope", lambda *a, **kw: scope())
    def corrupt_publication(connection, **kwargs):
        connection.execute(text("UPDATE sm_stock_minute SET price=12 WHERE trade_time=:at"),
            {"at": DAY + " 09:31:00"})
        connection.execute(text("DELETE FROM sm_stock_minute WHERE stock_code='000001' AND trade_time=:at"),
            {"at": DAY + " 15:00:00"})
    monkeypatch.setattr(importer, "publish_session", corrupt_publication)
    def receipt(connection, **kwargs):
        connection.execute(text("DELETE FROM receipt"))
        connection.execute(text("INSERT INTO receipt VALUES (:status)"), {"status": kwargs["status"]})
    monkeypatch.setattr(importer, "write_receipt", receipt)
    actual = importer._publish_one(engine, session=date.fromisoformat(DAY),
        entry=dict(rows=482, codes=list(CODES), digest="b"*64), run_id="attempt",
        root=Path("QMT"), replace=True, replace_sources=["old"], scope=scope())
    assert actual["status"] == "FAILED"
    assert coverage.persisted_matrix(engine, DAY, scope=scope()) == before
    with engine.connect() as connection:
        assert connection.execute(text("SELECT status FROM receipt")).scalar() == "FAILED"
    engine.dispose()
