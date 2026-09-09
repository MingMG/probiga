from argparse import Namespace
from copy import deepcopy
import json

import pytest

from server.api.routers import screener
from server.common import daily_delivery_control
from server.common.analysis_pool_receipt import (
    build_pool_manifest, build_score_snapshot, decode_score_snapshot, build_upper_limit_evidence,
    build_preliminary_analysis_snapshot,
    publication_is_complete_empty, publication_receipt_is_valid,
)
from server.engine.production_selector import score_production_candidate
from tests.score_snapshot_helpers import scored_publication


def test_complete_zero_candidate_publication_keeps_independent_universe_identity():
    receipt = scored_publication()
    assert publication_receipt_is_valid(receipt)
    assert publication_is_complete_empty(receipt)
    assert receipt["publisher_run_uids"] == [receipt["run_uid"]]
    assert len(receipt["membership_proofs"]) == 1
    assert receipt["analysis_count"] == receipt["score_stock_count"] == 1
    assert receipt["recommendation_count"] == receipt["executable_count"] == 0


def test_unverified_or_data_blocked_zero_rows_are_never_complete_empty():
    receipt = scored_publication([{"stock_code": "600001", "event_pit_status": "DATA_BLOCKED"}])
    assert not publication_is_complete_empty(receipt)
    assert receipt["score_data_blocked_count"] == 1
    plain = build_pool_manifest(trade_date="2026-09-08", analysis_rows=[], recommendation_rows=[])
    assert not publication_is_complete_empty(plain)


def test_score_snapshot_rejects_missing_stock_and_tampered_content():
    receipt = scored_publication([{"stock_code": "600001"}, {"stock_code": "600002"}])
    snapshot = receipt["score_snapshot"]
    payload = decode_score_snapshot(snapshot)
    with pytest.raises(ValueError, match="UNIVERSE_INCOMPLETE"):
        build_score_snapshot(
            trade_date=payload["trade_date"], decision_at=payload["decision_at"], run_uid=payload["run_uid"],
            build_sha=payload["build_sha"], analysis_rows=payload["analysis_rows"], scored_rows=payload["scored_rows"][:1],
        )
    tampered = deepcopy(snapshot)
    tampered["stock_count"] = 10
    with pytest.raises(ValueError, match="BINDING_MISMATCH"):
        decode_score_snapshot(tampered)
    with pytest.raises(ValueError, match="BINDING_MISMATCH"):
        decode_score_snapshot(snapshot, trade_date="2026-09-07")


def test_mutable_daily_projection_cannot_change_a_published_score():
    receipt = scored_publication()
    payload = decode_score_snapshot(receipt["score_snapshot"])
    changed = deepcopy(payload["analysis_rows"])
    changed[0]["data_quality_flags"] = "[]"
    with pytest.raises(ValueError, match="PUBLICATION_ROWS_MISMATCH"):
        build_pool_manifest(
            trade_date=payload["trade_date"], analysis_rows=changed,
            recommendation_rows=[], score_snapshot=receipt["score_snapshot"],
        )


def test_late_upper_execution_evidence_preserves_original_scoring_cutoff():
    from tests.test_scheduler_validation_thresholds import _upper_limit_evidence
    original = decode_score_snapshot(scored_publication(trade_date="2026-08-26")["score_snapshot"])
    scores = deepcopy(original["scored_rows"])
    upper = json.loads(_upper_limit_evidence("600001"))
    upper["published_at"] = "2026-08-27T03:04:30"
    upper.update(stage_known_at="2026-08-27T03:04:45", stage_attempt_uid="d" * 32, stage_evidence_sha256="e" * 64)
    scores[0]["upper_limit_evidence_json"] = build_upper_limit_evidence(upper)

    def seal():
        return build_score_snapshot(
            trade_date=original["trade_date"], decision_at="2026-08-27T03:05:00",
            run_uid=original["run_uid"], build_sha=original["build_sha"],
            analysis_rows=original["analysis_rows"], scored_rows=scores,
        )

    frozen = decode_score_snapshot(seal())
    assert frozen["input_decision_at"] == "2026-08-26T19:00:00"
    assert frozen["decision_at"] == "2026-08-27T03:05:00"
    assert frozen["execution_evidence"][0]["published_at"] == upper["published_at"]
    assert frozen["scored_rows"][0]["final_trade_score"] == original["scored_rows"][0]["final_trade_score"]
    upper["published_at"] = "2026-08-27T03:06:00"
    scores[0]["upper_limit_evidence_json"] = build_upper_limit_evidence(upper)
    with pytest.raises(ValueError, match="publication time differs"):
        seal()


def test_final_publication_keeps_the_full_frozen_universe_when_upper_covers_80(monkeypatch):
    from biz.analysis import sync_analysis_fast as analysis
    frozen = [
        {"stock_code": str(600000 + index), "final_trade_score": 90 - index / 10,
         "finance_revision_id": f"frozen-{index}", "ordinary_buy_eligible": False}
        for index in range(81)
    ]
    snapshot = build_preliminary_analysis_snapshot(
        analysis_rows=[{"stock_code": row["stock_code"], "analysis_date": "2026-09-08"} for row in frozen],
        scored_rows=frozen, candidate_rows=frozen[:80], market_mood_score=60,
        flow_date="2026-09-08", hot_date="2026-09-08",
    )
    monkeypatch.setattr(analysis, "load_latest_preliminary_analysis_receipt", lambda *_a, **_k: {"analysis_snapshot": snapshot, "receipt_sha256": "a" * 64})
    monkeypatch.setattr(analysis, "load_finance", lambda *_a, **_k: pytest.fail("frozen scores must not reselect finance"))
    def upper(**kwargs):
        result = kwargs["scored"].copy()
        result["ordinary_buy_eligible"] = True
        return result
    monkeypatch.setattr(analysis, "_refresh_exact_upper_limit_execution_evidence", upper)
    monkeypatch.setattr(analysis, "build_analysis_rows", lambda frame, _date: frame.to_dict("records"))
    monkeypatch.setattr(analysis, "build_recommendation_rows", lambda frame, *_a, **_k: frame.to_dict("records"))
    result = analysis._prepare_batch_outputs(
        engine=object(), trade_date="2026-09-08", min_score=62, top_n=80,
        news_cutoff_time="2026-09-08T22:00:00", publisher_build_sha="2" * 40,
        reuse_preliminary_snapshot=True, return_scored=True,
    )
    complete = result[-1].sort_values("stock_code").to_dict("records")
    assert len(complete) == 81 and len(result[1]) == 80
    assert [row["final_trade_score"] for row in complete] == [row["final_trade_score"] for row in frozen]
    assert [row["finance_revision_id"] for row in complete] == [row["finance_revision_id"] for row in frozen]
    assert all(row["ordinary_buy_eligible"] for row in complete[:80])
    assert complete[80]["ordinary_buy_eligible"] is False


def test_missing_daily_analysis_is_one_blocked_batch_not_100_rejected_stocks(monkeypatch):
    monkeypatch.setattr(screener, "get_engine", lambda: object())
    monkeypatch.setattr(daily_delivery_control, "load_published_analysis_receipt", lambda *_args, **_kwargs: None)
    stocks = [{"stock_code": str(600000 + index)} for index in range(100)]
    with pytest.raises(screener.ScreenerDataBlocked, match="ANALYSIS_PUBLICATION_MISSING"):
        screener._enrich_selector_evidence(stocks, "2026-09-08")
    def blocked(*_args):
        raise screener.ScreenerDataBlocked("ANALYSIS_PUBLICATION_MISSING")
    monkeypatch.setattr(screener, "_run_preset", blocked)
    monkeypatch.setattr(screener, "_runtime_status", lambda: {})
    monkeypatch.setattr(screener, "_persist_screener_run", lambda *_args: {"persisted": True, "run_uid": "a" * 32})
    result = screener.execute_screener_task(screener.ScreenerRunRequest(as_of_date="2026-09-08"))
    assert result["batch_status"] == "BLOCKED"
    assert result["data"] == []
    assert result["qualified_count"] is None
    assert result["stats"]["input_count"] is None


def test_unavailable_stock_evidence_does_not_become_a_risk_rejection():
    row = score_production_candidate({
        "stock_code": "600001", "pit_strategy_status": "DATA_BLOCKED",
        "pit_strategy_reason": "PIT_FINANCE_DATA_EXCLUDED:MISSING_REPORT",
        "ordinary_buy_eligible": False, "final_trade_score": 95,
    })
    assert row["candidate_grade"] == "DATA_BLOCKED"
    assert row["risk_gate"]["hard_veto"] is False
    assert row["risk_gate"]["reject_reasons"] == []
    assert row["decision_readiness"]["new_buy_ready"] is False


def test_screener_cli_accepts_verified_empty_and_fails_blocked(monkeypatch, capsys):
    from tools import run_screener_delivery as cli
    monkeypatch.setattr(cli, "parse_args", lambda: Namespace(preset="capital_support", top=100, as_of_date="2026-09-08", request_token="x"))
    monkeypatch.setattr(cli, "decode_screener_task_request", lambda _token: screener.ScreenerRunRequest(notify=False))
    result = {"status": "ok", "batch_status": "COMPLETED", "selection_status": "EMPTY", "data": [], "run": {"persisted": True}}
    monkeypatch.setattr(cli, "execute_screener_task", lambda _request: result)
    assert cli.main() == 0
    result.update(status="blocked", batch_status="BLOCKED", error="ANALYSIS_PUBLICATION_MISSING")
    assert cli.main() == 4
    assert '"qualified_count": null' in capsys.readouterr().out
