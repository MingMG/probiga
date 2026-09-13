from copy import deepcopy
from datetime import datetime
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from biz.stock_market import sync_dividend_eastmoney as d

NOW = datetime(2026, 9, 12, 10, 20)


def test_frozen_dividend_scope_does_not_depend_on_later_mutable_catalog(monkeypatch):
    catalog = SimpleNamespace(batch_id="before-ipo", manifest_hash="a" * 64,
                              member_set_hash="b" * 64, captured_at="2026-09-11T03:20:42")
    monkeypatch.setattr(d, "validate_stock_catalog_runtime_schema", lambda engine: None)
    calls = []
    def load(engine, **kwargs):
        calls.append(kwargs)
        return catalog, ("600000", "000001")
    monkeypatch.setattr(d, "load_target_stock_catalog", load)
    # The immutable catalog loader is the authority. A later si_all_code
    # observation cannot be used to reject this exact frozen revision.
    universe = d.load_authoritative_universe(object(), as_of="2026-09-12", known_at=NOW)
    assert universe.codes == ("000001", "600000")
    assert universe.catalog_batch_id == "before-ipo"
    assert universe.catalog_captured_at == catalog.captured_at
    assert calls == [{"target_date": "2026-09-12", "decision_known_at": NOW}]


def native(code="000001", period="2025-12-31", **changes):
    return {"SECURITY_CODE": code, "SECUCODE": code + (".SH" if code.startswith("6") else ".SZ"),
            "REPORT_DATE": period + " 00:00:00", "NOTICE_DATE": "2026-05-20 00:00:00",
            "PLAN_NOTICE_DATE": "2026-04-29 00:00:00", "EX_DIVIDEND_DATE": "2026-05-28 00:00:00",
            "ASSIGN_PROGRESS": "实施分配", "IMPL_PLAN_PROFILE": "10派1.00元(含税)", **changes}


def provider(rows, calls=None):
    rows = sorted(rows, key=lambda row: (row["SECURITY_CODE"], row["REPORT_DATE"]))
    def fetch(number):
        if calls is not None:
            calls.append(number)
        return {"success": True, "code": 0, "version": "a" * 32,
                "result": {"count": len(rows), "pages": (len(rows)+499)//500, "data": deepcopy(rows[(number-1)*500:number*500])}}
    return d.EastmoneyDividendProvider(fetch_page=fetch)


def collection(rows=None, codes=("000001", "000002")):
    return d.collect_snapshot(codes, provider=provider(rows or [native()]), as_of="2026-09-12", observed_at=NOW)


def engine():
    result = create_engine("sqlite://")
    with result.begin() as c:
        cols = ",".join(name + " TEXT" + (" UNIQUE NOT NULL" if name == "event_id" else "") for name in d.ROW_COLUMNS)
        c.exec_driver_sql("CREATE TABLE sm_dividend (" + cols + ",batch_id TEXT,received_at DATETIME,etl_sync_at DATETIME)")
        c.exec_driver_sql("CREATE TABLE sm_dividend_source_revision (event_id TEXT,source_hash TEXT,source_payload_json TEXT,first_received_at DATETIME,PRIMARY KEY(event_id,source_hash))")
        c.exec_driver_sql("CREATE TABLE sm_dividend_source_snapshot (batch_id TEXT PRIMARY KEY,observed_at DATETIME,manifest_json TEXT,manifest_hash TEXT)")
    return result


def test_same_notice_different_periods_are_separate_events_and_plan_is_lossless():
    plan = "10派4.20元(含税,扣税后3.78元)(中小股东10派4.921元;大股东10派3.906元)"
    rows = [native("300760", "2025-12-31"), native("300760", "2026-03-31"), native("600989", IMPL_PLAN_PROFILE=plan)]
    parsed = [d.normalize_native_row(r, as_of="2026-09-12") for r in rows]
    assert len({r["event_id"] for r in parsed}) == 3
    assert parsed[0]["report_date"] == parsed[1]["report_date"] == "2026-05-20"
    assert parsed[0]["report_period"] != parsed[1]["report_period"]
    assert parsed[2]["dividend_plan"] == plan
    assert json.loads(parsed[2]["source_payload_json"]) == rows[2]


def test_complete_two_pass_pagination_proves_absence_for_catalog_codes():
    calls = []
    rows = [native(f"{index:06d}") for index in range(1, 502)]
    p = provider(rows, calls)
    result = d.collect_snapshot([r["SECURITY_CODE"] for r in rows] + ["000999"], provider=p, as_of="2026-09-12", observed_at=NOW)
    evidence = d.validate_collection(result)
    assert sorted(calls) == [1, 1, 2, 2]
    assert evidence["row_count"] == 501 and evidence["authoritative_empty_code_count"] == 1
    assert result.empty_codes == ("000999",)


@pytest.mark.parametrize("change", [
    lambda p: p.update(success=False, code=9201),
    lambda p: p["result"].update(count=2),
    lambda p: p["result"].update(pages=0),
    lambda p: p["result"].update(data=[]),
    lambda p: p.update(version=None),
])
def test_ambiguous_empty_and_truncated_pages_cannot_publish(change):
    p = {"success": True, "code": 0, "version": "a" * 32, "result": {"count": 1, "pages": 1, "data": [native()]}}
    change(p)
    with pytest.raises(RuntimeError):
        d.EastmoneyDividendProvider(fetch_page=lambda n: p).full_snapshot(as_of="2026-09-12")


def test_same_event_conflict_blocks_and_changed_complete_identity_set_blocks():
    with pytest.raises(RuntimeError, match="DUPLICATE_SOURCE_EVENT"):
        provider([native(), native(IMPL_PLAN_PROFILE="10派2元")]).full_snapshot(as_of="2026-09-12")
    calls = []
    def fetch(n):
        calls.append(n)
        return {"success": True, "code": 0, "version": "a" * 32, "result": {"count": 1, "pages": 1, "data": [native("000001" if len(calls)==1 else "000002")]}}
    with pytest.raises(RuntimeError, match="COMPLETE_PASSES_DIFFER"):
        d.EastmoneyDividendProvider(fetch_page=fetch).full_snapshot(as_of="2026-09-12")


@pytest.mark.parametrize("changes", [{"SECUCODE": "000001.SH"}, {"NOTICE_DATE": "2026-09-13"}, {"REPORT_DATE": None}, {"IMPL_PLAN_PROFILE": ""}])
def test_wrong_identity_or_missing_required_date_is_not_native_null(changes):
    with pytest.raises((RuntimeError, ValueError)):
        d.normalize_native_row(native(**changes), as_of="2026-09-12")


def test_native_missing_fields_are_saved_and_explicitly_exposed():
    batch = collection([native(IMPL_PLAN_PROFILE=None, ASSIGN_PROGRESS=None)])
    evidence = d.validate_collection(batch)
    assert evidence["source_quality"]["status"] == "SOURCE_FIELDS_MISSING"
    assert evidence["source_quality"]["missing_event_count"] == 1
    db = engine()
    result = d.replace_snapshot(db, collection=batch, evidence=evidence)
    with db.connect() as c:
        row = c.execute(text("SELECT dividend_plan,assign_progress,quality_status FROM sm_dividend")).one()
    assert row == (None, None, "SOURCE_FIELDS_MISSING")
    assert result["row_count"] == 1


def test_revision_changes_preserve_prior_source_snapshot_and_never_delete_events():
    db = engine()
    old = collection()
    old_result = d.replace_snapshot(db, collection=old, evidence=d.validate_collection(old))
    new = collection([native(IMPL_PLAN_PROFILE="10派2元")])
    new_result = d.replace_snapshot(db, collection=new, evidence=d.validate_collection(new))
    assert old_result["batch_id"] != new_result["batch_id"]
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend")).scalar() == 1
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_source_revision")).scalar() == 2
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_source_snapshot")).scalar() == 2
        old_manifest = json.loads(c.execute(text("SELECT manifest_json FROM sm_dividend_source_snapshot WHERE batch_id=:b"), {"b": old_result["batch_id"]}).scalar())
    assert old_manifest["members"] == [[old.rows[0]["event_id"], old.rows[0]["data_version"]]]


def test_source_outside_current_catalog_is_retained_and_replayed_for_full_pagination():
    db = engine()
    batch = collection([native(), native("000003")])
    result = d.replace_snapshot(db, collection=batch, evidence=d.validate_collection(batch))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend")).scalar() == 1
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_source_revision")).scalar() == 2
        d._validate_persisted_batch(c, result["batch_id"], d.validate_collection(batch))


def test_atomic_failure_keeps_previous_projection_and_all_audit():
    db = engine()
    old = collection()
    d.replace_snapshot(db, collection=old, evidence=d.validate_collection(old))
    with db.begin() as c:
        c.exec_driver_sql("CREATE TRIGGER fail_dividend BEFORE UPDATE ON sm_dividend BEGIN SELECT RAISE(ABORT, 'forced'); END")
    new = collection([native(IMPL_PLAN_PROFILE="10派3元")])
    with pytest.raises(Exception, match="forced"):
        d.replace_snapshot(db, collection=new, evidence=d.validate_collection(new))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_source_revision")).scalar() == 1
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_source_snapshot")).scalar() == 1
        assert c.execute(text("SELECT dividend_plan FROM sm_dividend")).scalar() == old.rows[0]["dividend_plan"]


def test_retained_revision_tampering_blocks_transaction_and_replay():
    db = engine()
    batch = collection()
    result = d.replace_snapshot(db, collection=batch, evidence=d.validate_collection(batch))
    with db.begin() as c:
        c.execute(text("UPDATE sm_dividend_source_revision SET source_payload_json='{}'"))
    with db.connect() as c, pytest.raises(RuntimeError, match="RETAINED_SOURCE_REVISION_DIFFERS"):
        d._validate_persisted_batch(c, result["batch_id"], d.validate_collection(batch))
    with pytest.raises(RuntimeError, match="RETAINED_SOURCE_REVISION_DIFFERS"):
        d.replace_snapshot(db, collection=batch, evidence=d.validate_collection(batch))


def test_no_runtime_ddl_or_history_deletion():
    source = d.Path(d.__file__).read_text(encoding="utf8").upper()
    assert not any(sql in source for sql in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "DELETE FROM"))
