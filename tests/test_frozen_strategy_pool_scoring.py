from datetime import datetime, timedelta
import hashlib
import json
from types import MappingProxyType

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.analysis import sync_analysis_fast as analysis
from server.common.analysis_pool_receipt import build_score_snapshot, decode_score_snapshot
from server.common import daily_delivery_control as control
from server.common.daily_delivery_control import FrozenPremarketAnnouncement
from tests.score_snapshot_helpers import scored_publication


def test_two_frozen_scores_keep_the_initial_input_boundary_after_a_late_event(monkeypatch):
    target = "2026-11-10"
    universe = [{"stock_code": "600001"}, {"stock_code": "600002"}]
    fixture = decode_score_snapshot(
        scored_publication(universe, trade_date=target)["score_snapshot"]
    )
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)")
        conn.exec_driver_sql("INSERT INTO si_trade_calendar VALUES ('2026-11-10',1),('2026-11-11',1)")
        conn.exec_driver_sql("CREATE TABLE notice_fixture (stock_code TEXT, available_at TEXT)")
        conn.exec_driver_sql("INSERT INTO notice_fixture VALUES ('600002','2026-11-10T22:21:00')")
        conn.exec_driver_sql("CREATE TABLE st_recommended_stocks (stock_code TEXT)")
        conn.exec_driver_sql("INSERT INTO st_recommended_stocks VALUES ('600001')")

    def score_at_cutoff(*, engine, trade_date, news_cutoff_time, **_):
        with engine.connect() as conn:
            late = conn.execute(text(
                "SELECT stock_code FROM notice_fixture WHERE available_at<=:cutoff"
            ), {"cutoff": news_cutoff_time}).scalar()
        return (
            fixture["analysis_rows"],
            [{"stock_code": late or "600001"}],
            66,
            trade_date,
            trade_date,
            pd.DataFrame([
                {**row, "flow_input_decision_at": news_cutoff_time}
                for row in fixture["scored_rows"]
            ]),
        )

    monkeypatch.setattr(analysis, "_prepare_batch_outputs", score_at_cutoff)
    monkeypatch.setattr(analysis, "_now_shanghai_naive", lambda: datetime(2026, 11, 11, 8, 38))
    common = {"engine": engine, "data_trade_date": target, "build_sha": "2" * 40}
    initial = analysis.prepare_frozen_strategy_pool_scoring(
        **common, phase="INITIAL", run_uid="1" * 32,
    )
    revised = analysis.prepare_frozen_strategy_pool_scoring(
        **common, phase="PREMARKET_REVISION", run_uid="3" * 32,
        required_premarket_announcement=FrozenPremarketAnnouncement({}),
    )
    assert initial["cutoff_at"] == "2026-11-10T22:20:00+08:00"
    assert revised["cutoff_at"] == "2026-11-11T08:30:00+08:00"
    assert initial["evaluated_at"] == revised["evaluated_at"] == "2026-11-11T08:38:00+08:00"
    assert initial["recommendation_rows"] == [{"stock_code": "600001"}]
    assert revised["recommendation_rows"] == [{"stock_code": "600002"}]
    assert decode_score_snapshot(initial["score_snapshot"])["input_decision_at"] == "2026-11-10T22:20:00"
    assert decode_score_snapshot(revised["score_snapshot"])["input_decision_at"] == "2026-11-11T08:30:00"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT stock_code FROM st_recommended_stocks")).scalar_one() == "600001"


def test_fixed_initial_score_seals_consumed_announcement_instead_of_newer_stage(monkeypatch):
    from server.common.qmt_announcement_pit import QMT_ANNOUNCEMENT_TASK_SCHEMA

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    engine = create_engine("sqlite://")
    control.privileged_migrate_daily_delivery_schema(engine)
    trade_date = "2026-11-10"
    cutoff = datetime.fromisoformat(trade_date + "T22:20:00")
    stage = "qmt_announcement_pit"

    def seal(uid, batch_hash, fact_cutoff, received, finished, *, window_start="2026-10-11"):
        started = finished - timedelta(seconds=30)
        monkeypatch.setattr(control, "_control_now", lambda: started)
        control.start_daily_stage_attempt(
            engine, scheduler_run_uid=uid, stage_name=stage, trade_date=trade_date,
            release_id="a" * 40, strategy_release_id="b" * 64, lease_owner="windows-fixture",
        )
        result = {
            "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA, "status": "COMPLETE",
            "reason_code": "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE",
            "source": "qmt.announcement", "batch_id": "qmt-ann-" + uid, "batch_root_hash": batch_hash,
            "window_start": window_start, "window_end": trade_date,
            "catalog_batch_id": "verified-catalog", "catalog_manifest_hash": "e" * 64,
            "catalog_member_set_hash": "f" * 64,
            "fact_cutoff_at": fact_cutoff.isoformat(),
            "decision_at": received.isoformat(), "received_at": received.isoformat(),
            "stock_count": 2, "coverage_count": 2,
            "capture_seconds": int((received - fact_cutoff).total_seconds()),
            "automatic_real_order_submission": False, "real_order_authority": False,
        }
        replay = json.dumps(result)
        digest = hashlib.sha256(replay.encode()).hexdigest()
        evidence = {
            "schema": control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
            "run_uid": uid, "task_type": stage, "build_sha": "a" * 40,
            "status": "success", "exit_code": 0, "validation_checked": True, "validation_ok": True,
            "replay_output": replay, "replay_output_sha256": digest,
            "input_receipt_root_sha256": digest, "target_trade_date": trade_date,
        }
        with engine.begin() as conn:
            control.finish_daily_stage_attempt(
                conn, scheduler_run_uid=uid, status="success", input_root_sha256=digest,
                checkpoint={**evidence, "evidence_sha256": control.canonical_sha256(evidence)},
                now=finished,
            )
        return {**result, "catalog_member_count": 2}

    early = seal("1" * 32, "c" * 64, cutoff - timedelta(hours=2),
                 cutoff - timedelta(minutes=20), cutoff - timedelta(minutes=19))
    seal("4" * 32, "9" * 64, cutoff - timedelta(minutes=10),
         cutoff - timedelta(minutes=5), cutoff - timedelta(minutes=4),
         window_start="2026-10-12")
    late = seal("2" * 32, "d" * 64, cutoff - timedelta(minutes=25),
                cutoff - timedelta(minutes=2), cutoff + timedelta(minutes=1))
    assert control.load_signed_announcement_source(engine, batch=late, decision_at=cutoff) is None
    source = control.load_signed_announcement_source(engine, batch=early, decision_at=cutoff)
    selected = control.select_frozen_initial_announcement_source(engine, trade_date=trade_date, decision_at=cutoff)
    assert selected.score_binding == source
    assert selected.receipt["batch_id"] == early["batch_id"]
    assert selected.receipt["stage_evidence_sha256"] == source["evidence_sha256"]
    assert source["scheduler_run_uid"] == "1" * 32 and source["batch_root_hash"] == "c" * 64
    assert source["finished_at"] == "2026-11-10T22:01:00.000000"
    fixture = decode_score_snapshot(scored_publication(trade_date=trade_date)["score_snapshot"])
    core = {
        "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "daily", "root": "e" * 64},
        "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turnover", "root": "f" * 64},
        "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "a" * 64},
        "announcement": {"stage": stage, "dataset_id": source["scheduler_run_uid"],
                         "root": source["evidence_sha256"], "input_root": source["batch_root_hash"]},
    }
    args = {
        "trade_date": trade_date, "decision_at": cutoff, "run_uid": "3" * 32, "build_sha": "b" * 40,
        "analysis_rows": fixture["analysis_rows"],
        "scored_rows": [{**row, "flow_input_decision_at": cutoff.isoformat()} for row in fixture["scored_rows"]],
    }
    sealed = decode_score_snapshot(build_score_snapshot(
        **args, frozen_announcement_source=source, frozen_core_inputs=core,
    ))
    assert sealed["frozen_announcement_source"] == source
    assert sealed["frozen_core_inputs"] == core
    assert "frozen_announcement_source" not in decode_score_snapshot(build_score_snapshot(**args))
    with pytest.raises(ValueError, match="FROZEN_ANNOUNCEMENT_MISMATCH"):
        build_score_snapshot(
            **args, frozen_announcement_source={**source, "finished_at": (cutoff + timedelta(seconds=1)).isoformat()},
        )
    with pytest.raises(ValueError, match="FROZEN_CORE_INPUTS_MISMATCH"):
        build_score_snapshot(
            **args, frozen_announcement_source=source,
            frozen_core_inputs={**core, "announcement": {**core["announcement"], "input_root": late["batch_root_hash"]}},
        )


def test_fixed_initial_does_not_reuse_preliminary_without_its_exact_source_contract(monkeypatch):
    monkeypatch.setattr(
        analysis, "load_latest_preliminary_analysis_receipt",
        lambda *_args, **_kwargs: pytest.fail("legacy preliminary cannot prove fixed sources"),
    )

    def entered_full_fixed_scoring(*_args, **kwargs):
        assert kwargs["decision_known_at"] == datetime(2026, 11, 10, 22, 20)
        raise RuntimeError("entered full fixed scoring")

    monkeypatch.setattr(analysis, "load_kline_features", entered_full_fixed_scoring)
    with pytest.raises(RuntimeError, match="entered full fixed scoring"):
        analysis._prepare_batch_outputs(
            engine=object(), trade_date="2026-11-10", min_score=62, top_n=80,
            news_cutoff_time="2026-11-10T22:20:00", publisher_build_sha="2" * 40,
            reuse_preliminary_snapshot=True, pool_decision_phase="INITIAL",
        )


@pytest.mark.parametrize("source", ["finance", "event"])
def test_formal_scoring_stops_on_global_pit_source_failure(source, monkeypatch, caplog):
    from server.common.pit_facts import PITFactBatch, PIT_SCHEMA_UNAVAILABLE

    codes = ["600001", "600002"]
    trade_date = "2026-11-11"
    reason = f"PIT_{source.upper()}_SCHEMA_UNAVAILABLE"
    stage = f"{source}-revision-payload:80"
    boundary = "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE"
    public_reason = f"{reason}:stage={stage}:reason={boundary}"
    batch = PITFactBatch(
        global_status=PIT_SCHEMA_UNAVAILABLE,
        reason_by_code=dict.fromkeys(codes, f"{reason}:driver detail must stay private:stage={stage}:reason={boundary}"),
    )
    reader, load = (
        ("load_finance_facts", analysis.load_finance)
        if source == "finance" else ("load_event_facts", analysis.load_notice_features)
    )
    monkeypatch.setattr(analysis, reader, lambda *_args, **_kwargs: batch)
    args = dict(engine=object(), trade_date=trade_date, stock_codes=codes, decision_at=f"{trade_date}T22:20:00")
    with caplog.at_level("INFO", logger=analysis.logger.name):
        with pytest.raises(RuntimeError, match=f"ANALYSIS_{source.upper()}_SOURCE_UNAVAILABLE:{public_reason}"):
            load(**args, require_complete_source=True)
    assert f"source={source} trade_date={trade_date}" in caplog.text
    assert f"('{public_reason}', 2)" in caplog.text
    assert "driver detail" not in caplog.text
    assert len(load(**args)) == len(codes)


def test_formal_scoring_keeps_sealed_exclusions_and_empty_announcement(monkeypatch):
    from server.common.pit_facts import PITFactBatch, PIT_DATA_BLOCKED

    args = dict(engine=object(), trade_date="2026-11-11", stock_codes=["600001", "600002"],
                decision_at="2026-11-11T22:20:00", require_complete_source=True)
    exclusion = {"coverage_status": "DATA_EXCLUDED", "coverage_id": "1" * 64}
    finance = PITFactBatch(
        status_by_code={"600001": PIT_DATA_BLOCKED, "600002": analysis.PIT_AVAILABLE},
        reason_by_code={"600001": "PIT_FINANCE_DATA_EXCLUDED:PROVEN_EXCEPTION"},
        coverage_by_code={"600001": exclusion},
    )
    event = PITFactBatch(
        reason_by_code={"600001": "PIT_EVENT_AUTHORITATIVE_EMPTY"},
        coverage_by_code={"600001": {"coverage_id": "2" * 64}},
    )
    monkeypatch.setattr(analysis, "load_finance_facts", lambda *_args, **_kwargs: finance)
    monkeypatch.setattr(analysis, "load_event_facts", lambda *_args, **_kwargs: event)
    assert analysis.load_finance(**args).iloc[0]["finance_data_exclusion"] == exclusion
    notices = analysis.load_notice_features(**args)
    assert notices.iloc[0]["event_pit_status"] == analysis.PIT_AVAILABLE
    assert bool(notices.iloc[0]["event_authoritative_empty"])


def test_initial_scoring_serializes_rows_with_frozen_announcement_evidence(monkeypatch):
    trade_date = "2026-11-11"
    cutoff = "2026-11-12T07:39:50"
    binding = MappingProxyType({
        "scheduler_run_uid": "1" * 32,
        "evidence_sha256": "2" * 64,
        "batch_root_hash": "3" * 64,
    })
    announcement = control.FrozenInitialAnnouncement(MappingProxyType({}), binding)
    kline = pd.DataFrame([{"stock_code": "600001", "pre_close": 10.0}])
    kline.attrs["qmt_daily_input_window"] = {
        "daily_truth_sha256": "4" * 64,
        "daily_partition_roots": {trade_date: "5" * 64},
    }
    kline.attrs["frozen_daily_market_rows"] = []
    scored = pd.DataFrame([{
        "stock_code": "600001", "short_name": "样本",
        "recommend_status": "BLOCK", "signal_status": "BLOCK", "event_risk_level": "LOW",
        "amount": 100_000_000, "entry_score": 0.0, "final_trade_score": 0.0,
    }])
    for name in (
        "load_finance", "load_notice_features", "load_confidence_features",
        "load_recommendation_history", "load_failure_features", "load_sector_rotation_features",
        "load_intraday_evidence",
    ):
        monkeypatch.setattr(analysis, name, lambda *a, **kw: pd.DataFrame())
    monkeypatch.setattr(analysis, "load_kline_features", lambda *a, **kw: kline)
    monkeypatch.setattr(analysis, "load_hot_rank", lambda *a, **kw: (pd.DataFrame(), trade_date))
    monkeypatch.setattr(analysis, "load_flow_features", lambda *a, **kw: (pd.DataFrame(), trade_date))
    monkeypatch.setattr(analysis, "_verify_full_market_turnover_inputs", lambda *a, **kw: {
        "turnover_snapshot_run_id": "turnover",
        "turnover_full_market_proof_root_sha256": "6" * 64,
    })
    monkeypatch.setattr(analysis, "validate_exact_daily_flow_coverage", lambda *a, **kw: {
        "flow_input_root_sha256": "7" * 64, "flow_input_count": 1,
        "flow_input_min_etl_sync_at": cutoff, "flow_input_max_etl_sync_at": cutoff,
        "flow_input_decision_at": cutoff,
    })
    monkeypatch.setattr(analysis, "select_frozen_initial_announcement_source", lambda *a, **kw: announcement)
    monkeypatch.setattr(analysis, "resolve_strategy_fact_cutoffs", lambda *a, **kw: {
        "status": analysis.PIT_AVAILABLE,
        "finance_atomic_batch": {"seal_coverage_id": "finance", "batch_root_sha256": "8" * 64},
    })
    monkeypatch.setattr(analysis, "merge_event_features", lambda notices, news: notices)
    monkeypatch.setattr(analysis, "compute_market_mood", lambda frame: 50.0)
    monkeypatch.setattr(analysis, "load_tactical_context", lambda *a, **kw: {"mode": "UNKNOWN"})
    monkeypatch.setattr(analysis, "attach_daily_tactical_features", lambda frame, **kw: frame)
    monkeypatch.setattr(analysis, "tactical_candidate_codes", lambda frame: [])
    monkeypatch.setattr(analysis, "get_kline_engine", object)
    monkeypatch.setattr(analysis, "load_index_stability", lambda *a, **kw: {})
    monkeypatch.setattr(analysis, "compute_scores", lambda **kw: scored)
    monkeypatch.setattr(analysis, "_build_text_fields", lambda frame, **kw: frame)
    monkeypatch.setattr("server.engine.strategy_center.build_fixed_market_context", lambda **kw: {})

    rows, recommendations, _, _, _, scored_with_evidence = analysis._prepare_batch_outputs(
        engine=object(), trade_date=trade_date, min_score=62, top_n=80,
        news_cutoff_time=cutoff, pool_decision_phase="INITIAL", refresh_upper_limit=False,
        return_scored=True,
    )
    assert [row["stock_code"] for row in rows] == ["600001"]
    assert recommendations == []
    assert scored_with_evidence.to_dict("records")[0]["stock_code"] == "600001"
    assert scored_with_evidence.attrs["frozen_announcement_source"] == binding
    assert scored_with_evidence.attrs["frozen_core_inputs"]["announcement"]["root"] == binding["evidence_sha256"]
