"""Small complete scored-universe fixtures; no production data or database."""
import json

from server.common.analysis_pool_receipt import (
    build_pool_manifest, build_publication_receipt, build_score_snapshot,
)


def scored_publication(rows=None, *, trade_date="2026-09-08", run_uid="1" * 32, build_sha="2" * 40):
    rows = rows if rows is not None else [{"stock_code": "600001"}]
    decision_at = trade_date + "T19:00:00"
    scores, analysis = [], []
    for raw in rows:
        code = raw["stock_code"]
        row = {
            "stock_code": code, "industry_snapshot_date": trade_date,
            "industry_snapshot_source": "gj_big_qmt_inner", "membership_proof_sha256": "3" * 64,
            "pit_common_receipt_root_hash": "4" * 64,
            "turnover_full_market_count": len(rows), "turnover_full_market_proof_root_sha256": "5" * 64,
            "flow_input_root_sha256": "6" * 64, "flow_input_decision_at": decision_at,
            "finance_pit_status": "AVAILABLE", "event_pit_status": "AVAILABLE",
            "finance_revision_id": "finance-" + code, "finance_content_hash": "7" * 64,
            "event_revision_ids": ["event-" + code], "event_content_hashes": ["8" * 64],
            "recommend_status": "ALLOW", "signal_status": "CONFIRM",
            "ordinary_buy_eligible": True, "chase_risk_status": "ALLOW", "event_risk_level": "LOW",
            "entry_score": 78, "risk_reward_ratio": 2.0, "final_trade_score": 81,
            "amount": 10_000_000, "volume": 1000, "close": 10, "high": 10.2, "low": 9.8,
            "market_mood_score": 66, "fundamental": 70,
            **raw,
        }
        scores.append(row)
        analysis.append({
            "stock_code": code, "analysis_date": trade_date,
            "data_quality_flags": json.dumps([
                "finance_revision_id=" + row["finance_revision_id"],
                "finance_content_hash=" + row["finance_content_hash"],
            ]),
            "event_risk_detail": json.dumps({"event_revision_ids": row["event_revision_ids"], "event_content_hashes": row["event_content_hashes"]}),
        })
    snapshot = build_score_snapshot(
        trade_date=trade_date, decision_at=decision_at, run_uid=run_uid,
        build_sha=build_sha, analysis_rows=analysis, scored_rows=scores,
    )
    manifest = build_pool_manifest(trade_date=trade_date, analysis_rows=analysis, recommendation_rows=[], score_snapshot=snapshot)
    return build_publication_receipt(
        manifest=manifest, run_uid=run_uid, publisher_task_type="analysis_fast",
        build_sha=build_sha, published_at=trade_date + "T19:01:00", score_snapshot=snapshot,
    )
