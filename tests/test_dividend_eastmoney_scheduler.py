from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
import json

import pytest
from sqlalchemy import text

from biz.stock_market import sync_dividend_eastmoney as d
from server.common import scheduler_validation as validation
from test_dividend_eastmoney_pipeline import engine, native, provider


NOW = datetime(2026, 9, 12, 11, 0)
TASK = {"task_type": "stock_dividend_eastmoney"}


def receipt(monkeypatch, *, missing=False):
    codes = ("000001", "000002")
    universe = SimpleNamespace(as_of="2026-09-12", codes=codes, code_set_hash=d.code_set_hash(codes),
                               catalog_batch_id="catalog", catalog_manifest_hash="a"*64,
                               catalog_member_set_hash="b"*64, catalog_captured_at="2026-09-12T09:00:00")
    monkeypatch.setattr(d, "load_authoritative_universe", lambda *a, **k: universe)
    monkeypatch.setattr(d, "validate_runtime_schema", lambda e: {"schema_hash":"c"*64})
    db = engine()
    raw = native(IMPL_PLAN_PROFILE=None) if missing else native()
    result = d.run_sync(db, now=NOW, provider=provider([raw]))
    return db, result


def test_machine_receipt_binds_compact_proof_counts_quality_and_scope(monkeypatch):
    _db, result = receipt(monkeypatch)
    assert validation.scheduler_output_status(TASK, json.dumps(result), return_code=0) == "success"
    mutations = [lambda p:p["collection"].update(responded_code_count=1),
                 lambda p:p["collection"].update(authoritative_empty_code_count=0),
                 lambda p:p["collection"].update(nonempty_code_ratio=1),
                 lambda p:p["source_identity"].update(endpoint="https://example.com"),
                 lambda p:p["pagination_summary"].update(full_proof_hash="0"*64),
                 lambda p:p["pagination_summary"]["passes"][1].update(event_set_hash="0"*64),
                 lambda p:p["source_quality"].update(missing_event_count=1),
                 lambda p:p["database"].update(row_hash="0"*64),
                 lambda p:p.update(schema="probiga.stock-dividend-baidu-receipt.v2")]
    for mutate in mutations:
        bad = deepcopy(result)
        bad.pop("receipt_id")
        mutate(bad)
        assert validation.scheduler_output_status(TASK, json.dumps(d._receipt(bad)), return_code=0) == "failed"


def test_database_replay_resolves_full_paginated_native_source_and_blocks_tampering(monkeypatch):
    db, result = receipt(monkeypatch)
    ok, _ = validation._validate_dividend_eastmoney_receipt(db, output=json.dumps(result), now=NOW)
    assert ok
    assert not validation._validate_dividend_eastmoney_receipt(db, output=json.dumps(result), now=datetime(2026,9,13,11))[0]
    with db.begin() as c:
        c.execute(text("UPDATE sm_dividend_source_revision SET source_payload_json='{}'"))
    assert not validation._validate_dividend_eastmoney_receipt(db, output=json.dumps(result), now=NOW)[0]


def test_source_field_gaps_are_explicit_even_when_acquisition_is_complete(monkeypatch):
    db, result = receipt(monkeypatch, missing=True)
    assert result["acquisition_status"] == "COMPLETE"
    assert result["source_quality"]["status"] == "SOURCE_FIELDS_MISSING"
    assert result["source_quality"]["missing_event_count"] == 1
    ok, message = validation._validate_dividend_eastmoney_receipt(db, output=json.dumps(result), now=NOW)
    assert ok and "source_fields_missing=1" in message


def test_native_pagination_receipts_stay_in_retained_audit_not_scheduler_output(monkeypatch):
    _db, result = receipt(monkeypatch)
    proof = {"schema":"probiga.dividend-full-pagination.v1", "complete_passes":2,
             "source_count":500000,"snapshot_row_hash":"a"*64,"passes":[]}
    for _ in range(2):
        proof["passes"].append({"source_count":500000,"page_count":1000,"event_set_hash":"b"*64,
                                "page_receipts":[{"page":p,"row_count":500,"version":"c"*32,"row_hash":"d"*64,"event_set_hash":"e"*64} for p in range(1,1001)]})
    result.pop("receipt_id")
    result["pagination_summary"] = d.pagination_summary(proof)
    result["collection"]["pagination_hash"] = d._digest(proof)
    signed = d._receipt(result)
    d.validate_receipt_source_proof(signed)
    assert "page_receipts" not in d._canonical_json(signed)
    assert len(d._canonical_json(signed).encode("utf8")) < 24000
    result["oversized"] = "x" * 24000
    with pytest.raises(RuntimeError, match="SCHEDULER_STORAGE"):
        d._receipt(result)


def test_failed_acquisition_remains_retryable_under_existing_policy():
    result = {"schema": d.RECEIPT_SCHEMA, "status":"DATA_BLOCKED"}
    assert validation.scheduler_output_status(TASK, json.dumps(result), return_code=2) == "failed"


@pytest.mark.filterwarnings("ignore:The default datetime adapter is deprecated:DeprecationWarning")
def test_full_5550_stock_receipt_survives_actual_scheduler_history_builder(monkeypatch):
    from server.api import scheduler_runtime as runtime
    codes = tuple(f"{n:06d}" for n in range(1, 5551))
    universe = SimpleNamespace(as_of="2026-09-12", codes=codes, code_set_hash=d.code_set_hash(codes),
                               catalog_batch_id="catalog", catalog_manifest_hash="a"*64,
                               catalog_member_set_hash="b"*64, catalog_captured_at="2026-09-12T09:00:00")
    monkeypatch.setattr(d, "load_authoritative_universe", lambda *a, **k: universe)
    monkeypatch.setattr(d, "validate_runtime_schema", lambda e: {"schema_hash":"c"*64})
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "f"*40)
    rows = [native(code) for code in codes]
    result = d.run_sync(engine(), now=NOW, provider=provider(rows))
    machine = d._canonical_json(result)
    assert result["collection"]["requested_code_count"] == result["collection"]["row_count"] == 5550
    assert len(machine.encode("utf8")) <= 24000
    task = {**TASK, "id":121, "task_name":"东财全市场分红原始事件同步", "_scheduler_target_trade_date":"2026-09-12"}
    evidence = runtime._build_history_validation_evidence(task, run_uid="e"*32, machine_output=machine,
                                                         status="success", exit_code=0, started_at=NOW,
                                                         validation_message="complete acquisition; source fields independently validated")
    stored = runtime._history_output_with_validation_evidence(machine, evidence)
    extracted = runtime._history_validation_evidence(stored)
    assert extracted["replay_output"] == runtime._history_validation_replay_output(machine)
    restored = validation._single_nested_machine_payload(extracted["replay_output"], schema=d.RECEIPT_SCHEMA)
    assert restored == result
    d.validate_receipt_source_proof(restored)
    assert len(evidence.encode("utf8")) <= runtime._HISTORY_EVIDENCE_LIMIT
    assert len(stored.encode("utf8")) <= runtime._HISTORY_OUTPUT_LIMIT
