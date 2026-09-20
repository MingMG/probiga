from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from integrations.bigqmt.spool import BigQmtResourceBlocked
from server.common import minute_acquisition_reuse as reuse
from server.common import qmt_history_coverage as coverage
from server.common.qmt_history_coverage import QmtHistoryCoverageError
from test_qmt_history_coverage import (
    TRADE_DATE, HASH_A, HASH_B, _daily_row, _minute_context, _minute_rows,
)
from test_qmt_minute_checkpoint import DAY, publisher  # noqa: F401


def finality(day=TRADE_DATE, at=None):
    return {
        "schema": "probiga.qmt-daily-market-consumer-truth.v1",
        "run_id": "attested-daily", "run_start_date": day, "run_end_date": day,
        "run_finished_at": f"{day} 15:34:00",
        "decision_known_at": at or f"{day} 15:35:00",
        "catalog_batch_id": "catalog", "catalog_manifest_hash": HASH_A,
        "catalog_member_set_hash": "c" * 64, "calendar_batch_id": "calendar",
        "calendar_manifest_hash": HASH_B, "calendar_session_set_hash": "d" * 64,
        "attested_row_count": 1, "requested_sessions": [day], "truth_hash": "e" * 64,
    }


def same_day_bundle(evidence=None, minute_rows=None):
    context = _minute_context()
    context["captured_at"] = f"{TRADE_DATE} 15:35:00"
    return coverage.assess_minute_coverage(
        expected_codes=["000001"], daily_rows=[_daily_row("000001")],
        minute_rows=_minute_rows("000001") if minute_rows is None else minute_rows,
        daily_finality_evidence=finality() if evidence is None else evidence,
        attested_closing_prices={"000001": 10.5},
        **context,
    )


def test_same_day_finality_requires_daily_attestation_and_matching_complete_close():
    bundle = same_day_bundle()
    assert coverage.require_exact_coverage(bundle)["daily_finality_evidence"] == finality()
    combined = coverage.combine_minute_coverage_partitions(expected_codes=["000001"], partitions=[bundle])
    assert coverage.require_exact_coverage(combined)["daily_finality_evidence"] == finality()
    rows = _minute_rows("000001")
    rows[-1]["price"] = 10
    with pytest.raises(QmtHistoryCoverageError, match="MINUTE_DAILY_CLOSE_MISMATCH"):
        coverage.require_exact_coverage(same_day_bundle(minute_rows=rows))
    with pytest.raises(QmtHistoryCoverageError, match="MINUTE_GRID_MISMATCH"):
        coverage.require_exact_coverage(same_day_bundle(minute_rows=rows[:-1]))


@pytest.mark.parametrize("changes", [
    {"run_finished_at": f"{TRADE_DATE} 15:29:59"},
    {"run_finished_at": f"{TRADE_DATE} 15:35:01"},
    {"decision_known_at": f"{TRADE_DATE} 15:36:00"},
    {"requested_sessions": ["2026-08-20"]}, {"truth_hash": "invalid"},
    {"attested_row_count": 0}, {"schema": "unverified"},
])
def test_same_day_finality_rejects_wrong_scope_or_unfinal_daily_receipt(changes):
    with pytest.raises(QmtHistoryCoverageError, match="daily finality"):
        same_day_bundle(evidence={**finality(), **changes})


def test_same_day_finality_requires_replay_of_immutable_authority(monkeypatch):
    from server.common import qmt_stock_catalog, qmt_trade_calendar
    monkeypatch.setattr(qmt_stock_catalog, "load_stock_catalog", lambda *_a, **_k: SimpleNamespace(
        manifest_hash=HASH_A, eligible_codes=lambda _d: ["000001"],
    ))
    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_receipt", lambda *_a, **_k: SimpleNamespace(
        manifest_hash=HASH_B, sessions_between=lambda *_d: [TRADE_DATE],
    ))
    monkeypatch.setattr(coverage, "load_minute_daily_finality_evidence", lambda *_a, **_k: finality())
    assert coverage.validate_coverage_authority(object(), same_day_bundle())["status"] == "EXACT"
    monkeypatch.setattr(coverage, "load_minute_daily_finality_evidence", lambda *_a, **_k: {
        **finality(), "run_id": "different-native-run",
    })
    with pytest.raises(QmtHistoryCoverageError, match="daily finality authority"):
        coverage.validate_coverage_authority(object(), same_day_bundle())


def test_old_exact_close_conflict_keeps_raw_evidence_then_refetches_only_bad_batch(publisher, monkeypatch):
    p = publisher
    real_assess = p.sync.assess_minute_coverage
    real_fetch = p.backend.fetch_minute

    def stale_minute(*args, **kwargs):
        frame = real_fetch(*args, **kwargs)
        frame.loc[frame["trade_time"].dt.strftime("%H:%M:%S").eq("15:00:00"), "price"] = 10
        return frame

    def prior_assessment(**kwargs):
        kwargs["daily_rows"] = [{**row, "close": 10} for row in kwargs["daily_rows"]]
        return real_assess(**kwargs)

    monkeypatch.setattr(p.backend, "fetch_minute", stale_minute)
    monkeypatch.setattr(p.sync, "assess_minute_coverage", prior_assessment)
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    original = next(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz")).read_bytes()
    monkeypatch.setattr(p.sync, "assess_minute_coverage", real_assess)
    monkeypatch.setattr(p.backend, "fetch_minute", real_fetch)
    p.backend.fail = False
    p.backend.calls.clear()
    p.clock.current += timedelta(minutes=5)
    with pytest.raises(QmtHistoryCoverageError, match="MINUTE_DAILY_CLOSE_MISMATCH"):
        p.run()
    assert p.backend.calls == [("minute", p.codes[5:]), ("daily", p.codes[5:])]
    assert next(p.root.glob("qmt-minute-checkpoints/*/rejected-*.json.gz")).read_bytes() == original
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 1
    assert not p.publications
    p.backend.calls.clear()
    p.clock.current += timedelta(minutes=5)
    p.run()
    assert p.backend.calls == [("minute", p.codes[:5]), ("daily", p.codes[:5])]
    assert p.receipts[-1]["quality_status"] == "PASS"
    assert len(p.publications) == 1


@pytest.mark.parametrize("daily_close,ok", [(10, True), (10.0001, True), (11, False), (None, False)])
def test_native_persisted_close_readback_requires_attested_daily_anchor(daily_close, ok):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE sm_stock_kline
            (stock_code TEXT,trade_date TEXT,k_type INTEGER,adjust_type INTEGER,
             close REAL,data_source TEXT,quality_status TEXT,permission_status TEXT)"""))
        connection.execute(text("""INSERT INTO sm_stock_kline VALUES
            ('000001',:day,1,0,:close,'gj_big_qmt_inner','QMT_ATTESTED','SUPPORTED')"""),
            {"day": TRADE_DATE, "close": daily_close})
        assert reuse.native_stock_closes_match(connection, trade_date=TRADE_DATE,
                                               closing_prices={"000001": 10}) is ok
        connection.execute(text("UPDATE sm_stock_kline SET quality_status='VERIFIED'"))
        assert not reuse.native_stock_closes_match(connection, trade_date=TRADE_DATE,
                                                   closing_prices={"000001": 10})


def test_same_day_capture_publishes_only_with_attested_close_readback(publisher, monkeypatch):
    p = publisher
    p.backend.fail = False
    p.clock.current = datetime(2026, 9, 1, 16)
    proof = finality(DAY, p.clock.current.isoformat(sep=" "))
    monkeypatch.setattr(p.sync, "load_minute_daily_finality_evidence", lambda *_a, **_k: proof)
    monkeypatch.setattr(reuse, "load_native_attested_closes", lambda *_a, **_k: {code: 10.5 for code in p.codes})
    checked = []
    def check(_connection, **kwargs):
        checked.append(kwargs)
        return True
    monkeypatch.setattr(reuse, "native_stock_closes_match", check)
    p.run()
    assert checked == [{"trade_date": DAY, "closing_prices": {code: 10.5 for code in p.codes}}]
    assert p.receipts[-1]["quality_status"] == "PASS"
    assert p.receipts[-1]["evidence"]["minute_coverage_manifest"]["daily_finality_evidence"] == proof
    assert p.receipts[-1]["forward_eligible"] is False


def test_same_day_attested_close_conflict_is_pending_and_can_recover(publisher, monkeypatch):
    p = publisher
    p.backend.fail = False
    p.clock.current = datetime(2026, 9, 1, 16)
    monkeypatch.setattr(p.sync, "load_minute_daily_finality_evidence", lambda *_a, **_k: finality(
        DAY, p.clock.current.isoformat(sep=" "),
    ))
    monkeypatch.setattr(reuse, "load_native_attested_closes", lambda *_a, **_k: {code: 10.5 for code in p.codes})
    monkeypatch.setattr(reuse, "native_stock_closes_match", lambda *_a, **_k: True)
    real_minute, real_daily = p.backend.fetch_minute, p.backend.fetch_kline
    def minute(*args, **kwargs):
        frame = real_minute(*args, **kwargs)
        frame.loc[frame["trade_time"].dt.strftime("%H:%M:%S").eq("15:00:00"), "price"] = 11
        return frame
    def daily(*args, **kwargs):
        frame = real_daily(*args, **kwargs)
        frame["close"] = 11
        return frame
    monkeypatch.setattr(p.backend, "fetch_minute", minute)
    monkeypatch.setattr(p.backend, "fetch_kline", daily)
    with pytest.raises(QmtHistoryCoverageError, match="MINUTE_ATTESTED_DAILY_CLOSE_MISMATCH"):
        p.run()
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 2
    assert not list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))
    assert not p.publications
    monkeypatch.setattr(p.backend, "fetch_minute", real_minute)
    monkeypatch.setattr(p.backend, "fetch_kline", real_daily)
    p.clock.current += timedelta(minutes=5)
    p.run()
    assert p.receipts[-1]["quality_status"] == "PASS"


def test_complete_native_inventory_is_not_reused_when_last_close_is_stale(monkeypatch):
    from server.common import qmt_stock_catalog
    from tools import crawl_minute_kline
    from test_minute_acquisition_reuse import inventory_row
    monkeypatch.setattr(crawl_minute_kline, "_is_trade_day", lambda *_a: True)
    monkeypatch.setattr(qmt_stock_catalog, "load_target_stock_catalog", lambda *_a, **_k: (
        SimpleNamespace(batch_id="catalog", manifest_hash=HASH_A), ["000001"],
    ))
    monkeypatch.setattr(crawl_minute_kline, "verified_no_trade_codes", lambda *_a, **_k: (set(), None))
    monkeypatch.setattr(reuse, "load_numeric_layout", lambda *_a: {})
    monkeypatch.setattr(reuse, "_native_stock_proof", lambda *_a, **_k: {"verified": True})
    state = {"minute_close": 10}
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_a): pass
        def exec_driver_sql(self, _sql): return SimpleNamespace(scalar_one=lambda: 1024)
        def execute(self, sql, _params):
            if "FROM sm_stock_kline" in str(sql):
                rows = [{"stock_code": "000001", "close": 10.5, "data_source": "gj_big_qmt_inner",
                         "quality_status": "QMT_ATTESTED", "permission_status": "SUPPORTED"}]
            else:
                rows = [{**inventory_row(native=True), "closing_price": state["minute_close"]}]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
    engine = SimpleNamespace(connect=lambda: Connection())
    kwargs = {"kind": "stock", "trade_date": TRADE_DATE, "now": datetime(2026, 8, 22, 1)}
    assert reuse.inspect_complete_partition(object(), engine, **kwargs) is None
    state["minute_close"] = 10.5
    assert reuse.inspect_complete_partition(object(), engine, **kwargs)["row_count"] == 241


def test_same_day_reuse_requires_published_native_receipt_and_bound_inventory(publisher, monkeypatch):
    import json
    from copy import deepcopy
    from tools import sync_qmt_stock_edge as edge
    from test_minute_acquisition_reuse import inventory_row

    p = publisher
    p.backend.fail = False
    p.clock.current = datetime(2026, 9, 1, 16)
    monkeypatch.setattr(p.sync, "load_minute_daily_finality_evidence", lambda *_a, **_k: finality(
        DAY, p.clock.current.isoformat(sep=" "),
    ))
    monkeypatch.setattr(reuse, "load_native_attested_closes", lambda *_a, **_k: {code: 10.5 for code in p.codes})
    monkeypatch.setattr(reuse, "native_stock_closes_match", lambda *_a, **_k: True)
    bundles = []
    monkeypatch.setattr(p.sync, "insert_coverage_bundle", lambda _c, bundle: bundles.append(bundle) or {"inserted": True})
    p.run()
    bundle = bundles[-1]
    original = p.receipts[-1]
    signed_payload = {**original, "first_trade_time": original["first_trade_time"].isoformat(),
                      "last_trade_time": original["last_trade_time"].isoformat()}
    stored = {**original, "receipt_id": coverage.canonical_digest(signed_payload)[:32], "coverage": 1.0,
              "evidence_json": json.dumps(original["evidence"], default=str)}
    stored.pop("evidence")
    state = {"receipt": stored, "manifest_json": bundle["manifest"]["manifest_json"], "identity": True}
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_a): pass
        def execute(self, sql, _params):
            if "st_qmt_minute_sync_receipt_v2" in str(sql):
                rows = [state["receipt"]] if state["receipt"] else []
            elif "qmt_history_coverage_entity" in str(sql):
                rows = bundle["entities"]
            else:
                rows = [{"manifest_json": state["manifest_json"]}] if state["manifest_json"] else []
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
    engine = SimpleNamespace(connect=lambda: Connection())
    monkeypatch.setattr(edge, "validate_coverage_authority", lambda _c, observed: coverage.require_exact_coverage(observed))
    monkeypatch.setattr(edge, "_valid_release_identity", lambda *_a, **_k: state["identity"])
    rows = [{**inventory_row(code, native=True), "batch_count": 1, "missing_batch_count": 0,
             "batch_id": bundle["manifest"]["run_id"]} for code in p.codes]
    from server.common import qmt_minute_content as content
    layout = original["evidence"]["canonical_content_proof"]["numeric_layout"]
    raw_hashes = content.input_content_rows(p.sync._records_without_nan(p.publications[-1]), layout=layout,
                                           trade_date=DAY, run_id=bundle["manifest"]["run_id"])
    for row, hashed in zip(rows, raw_hashes):
        row.update(canonical_row_hash=hashed["row_hash"], canonical_hashed_bytes=241 * 64)
    kwargs = {"rows": rows, "trade_date": DAY, "current": p.clock.current,
              "catalog": SimpleNamespace(batch_id="catalog", manifest_hash="a" * 64),
              "expected": p.codes, "no_trade": [], "numeric_layout": layout}
    assert reuse._native_stock_proof(engine, **kwargs)["manifest_hash"] == bundle["manifest"]["manifest_hash"]
    # Change exactly one middle bar from the actual publisher's canonical
    # frame. Its day, native batch, full grid and closing price stay intact.
    original_frame = p.publications[-1]
    for field in ("price", "amount"):
        altered = original_frame.copy()
        middle = altered.index[altered["stock_code"].eq(p.codes[0])][30]
        altered.loc[middle, field] += 1
        altered_rows = deepcopy(rows)
        altered_rows[0]["canonical_row_hash"] = content.input_content_rows(
            p.sync._records_without_nan(altered[altered["stock_code"].eq(p.codes[0])]), layout=layout,
            trade_date=DAY, run_id=bundle["manifest"]["run_id"],
        )[0]["row_hash"]
        assert altered.iloc[-1]["price"] == original_frame.iloc[-1]["price"]
        for decision in (p.clock.current, p.clock.current + timedelta(days=1)):
            assert reuse._native_stock_proof(engine, **{**kwargs, "rows": altered_rows, "current": decision}) is None, field
    # These receipts are emitted by the actual publisher, including the daily
    # no-trade evidence response kind, not a hand-written alternate contract.
    assert {item["kind"] for item in original["evidence"]["source_response_receipts"]} == {"minute", "daily_no_trade_evidence"}
    for mutation in ("missing_receipt", "missing_manifest", "changed_manifest", "missing_content", "identity", "early", "future", "public", "mixed_batch"):
        prior = deepcopy(state)
        changed_rows = deepcopy(rows)
        if mutation == "missing_receipt": state["receipt"] = None
        elif mutation == "missing_manifest": state["manifest_json"] = None
        elif mutation == "changed_manifest": state["manifest_json"] = "{}"
        elif mutation == "identity": state["identity"] = False
        elif mutation == "missing_content":
            evidence = json.loads(state["receipt"]["evidence_json"])
            evidence.pop("canonical_content_proof")
            state["receipt"]["evidence_json"] = json.dumps(evidence)
            state["receipt"]["receipt_id"] = coverage.canonical_digest({**signed_payload, "evidence": evidence})[:32]
        elif mutation in {"early", "future"}:
            evidence = json.loads(state["receipt"]["evidence_json"])
            evidence["minute_coverage_manifest"]["captured_at"] = f"{DAY} " + ("15:10:00" if mutation == "early" else "16:01:00")
            state["receipt"]["evidence_json"] = json.dumps(evidence)
        elif mutation == "public": changed_rows[0]["data_source"] = "east_push2delay"
        else: changed_rows[0]["batch_id"] = "intraday-old-run"
        assert reuse._native_stock_proof(engine, **{**kwargs, "rows": changed_rows}) is None, mutation
        state.clear()
        state.update(prior)


@pytest.mark.parametrize("boundary", ["stage", "target"])
@pytest.mark.parametrize("field", ["price", "amount", "change", "change_pct", "volume", "source_time"])
def test_publisher_rejects_write_corruption_before_pass(publisher, monkeypatch, boundary, field):
    p = publisher
    p.backend.fail = False
    target = "_append_qmt_minute_stage" if boundary == "stage" else "_commit_qmt_minute_stage"
    original = getattr(p.sync, target)

    def corrupt(*args, **kwargs):
        result = original(*args, **kwargs)
        frame = p.staged[-1] if boundary == "stage" else p.publications[-1]
        if field == "source_time":
            frame.iloc[30, frame.columns.get_loc(field)] = DAY + " 10:01:00"
        else:
            if field not in frame.columns:
                frame[field] = 0
            frame.iloc[30, frame.columns.get_loc(field)] = 12345
        return result

    monkeypatch.setattr(p.sync, target, corrupt)
    with pytest.raises((QmtHistoryCoverageError, ValueError), match="content differs|input provenance differs"):
        p.run()
    assert not any(item["quality_status"] == "PASS" for item in p.receipts)
    if boundary == "stage":
        assert not p.receipts
    else:
        assert p.receipts[-1]["quality_status"] == "FAILED"
    assert list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))


def test_same_day_inventory_without_native_finality_never_produces_reused_result(monkeypatch):
    from test_minute_acquisition_reuse import partition

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return cls(2026, 9, 13, 19, tzinfo=tz)
    monkeypatch.setattr(reuse, "datetime", Clock)
    part = partition(day="2026-09-13", native=True)
    part.pop("native_finality")
    with pytest.raises(ValueError, match="same-day native minute finality"):
        reuse.result_for([part], task_type="qmt_stock_minute_canonical", build_sha="a" * 40,
                         started_at=datetime(2026, 9, 13, 19))


@pytest.mark.parametrize("native", [False, True])
def test_same_day_full_inventory_requires_native_proof_before_reuse(monkeypatch, native):
    from server.common import qmt_stock_catalog, qmt_daily_market_truth
    from tools import crawl_minute_kline
    from test_minute_acquisition_reuse import inventory_row

    monkeypatch.setattr(crawl_minute_kline, "_is_trade_day", lambda *_a: True)
    monkeypatch.setattr(crawl_minute_kline, "verified_no_trade_codes", lambda *_a, **_k: (set(), None))
    monkeypatch.setattr(qmt_stock_catalog, "load_target_stock_catalog", lambda *_a, **_k: (
        SimpleNamespace(batch_id="catalog", manifest_hash=HASH_A), ["000001"],
    ))
    monkeypatch.setattr(reuse, "native_stock_closes_match", lambda *_a, **_k: True)
    monkeypatch.setattr(qmt_daily_market_truth, "load_qmt_daily_market_truth", lambda *_a, **_k: object())
    calls = []
    monkeypatch.setattr(reuse, "load_numeric_layout", lambda *_a: {})
    monkeypatch.setattr(reuse, "_native_stock_proof", lambda *_a, **kw: calls.append(kw) or None)
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_a): pass
        def exec_driver_sql(self, _sql): return SimpleNamespace(scalar_one=lambda: 1024)
        def execute(self, *_a):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [inventory_row(native=native)]))
    engine = SimpleNamespace(connect=lambda: Connection())
    assert reuse.inspect_complete_partition(object(), engine, kind="stock", trade_date=TRADE_DATE,
                                            now=datetime.fromisoformat(TRADE_DATE + " 16:00:00")) is None
    assert len(calls) == 1
