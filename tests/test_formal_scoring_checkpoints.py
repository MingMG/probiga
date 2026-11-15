from contextlib import nullcontext
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from biz.analysis import sync_analysis_fast as analysis
from biz.analysis.formal_scoring_checkpoints import checkpoint_factor_frame
from server.common.analysis_pool_receipt import canonical_sha256
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema


def test_factor_checkpoint_preserves_object_payload_and_pandas_dtypes(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "formal-checkpoint.sqlite"))
    privileged_migrate_formal_analysis_schema(engine)
    store = FormalAnalysisStore(engine)
    index = [7, 1, 9]
    original = pd.DataFrame({
        "stock_code": pd.Series(["000001", "000005", "688613"], index=index, dtype="str"),
        "mixed": pd.Series(
            [date(2026, 11, 13), Decimal("3.200"), {"z": (None, pd.NA), "a": [False, 0]}],
            index=index, dtype="object",
        ),
        "seen_at": pd.Series(pd.to_datetime(
            ["2026-11-13 15:31:01.123456789", None, "2026-11-13 15:33:00"],
            format="ISO8601",
        ), index=index),
        "score": pd.Series([-0.0, np.nan, 0.30000000000000004], index=index),
        "available": pd.Series([True, pd.NA, False], index=index, dtype="boolean"),
    })
    original.index.name = "display_order"
    original.attrs["receipt"] = {"b": "first", "a": ["second"]}
    _, reused = checkpoint_factor_frame(
        store, "a" * 64, "factor", "b" * 64, lambda: original, rows_per_part=2,
    )
    assert reused is False
    restored, reused = checkpoint_factor_frame(
        store, "a" * 64, "factor", "b" * 64,
        lambda: pytest.fail("the exact source checkpoint was not reused"),
    )
    assert reused is True
    pd.testing.assert_frame_equal(original, restored, check_exact=True)
    assert restored.attrs == original.attrs
    assert list(restored.attrs["receipt"]) == ["b", "a"]
    assert list(restored.loc[9, "mixed"]) == ["z", "a"]
    assert np.signbit(restored.loc[7, "score"])


def test_formal_kline_reuses_only_unchanged_technical_factors_and_reranks(monkeypatch, tmp_path):
    days = list(pd.bdate_range(end="2026-11-13", periods=60).strftime("%Y-%m-%d"))
    catalog = {day: "catalog-" + day for day in days}
    roots = {day: canonical_sha256(day) for day in days}
    rows = pd.DataFrame([
        {
            "stock_code": code, "short_name": "A", "trade_date": date.fromisoformat(day),
            "open": 10, "high": 12, "low": 9,
            "close": 10 + (i % 4) / 10 + j / 10,
            "pre_close": 10, "volume": 100_000 + i, "amount": 100_000_000 * (j + 1) + i,
            "change_pct": 14.5, "turnover_ratio": 2.0,
            "data_source": "qmt", "batch_id": catalog[day], "data_version": 1,
            "quality_status": "PASS", "permission_status": "GRANTED",
            "received_at": datetime.fromisoformat(day + "T16:05:00"),
        }
        for i, day in enumerate(days)
        for j, code in enumerate(("000001", "000005", "688613"))
        if i >= j
    ])
    monkeypatch.setattr(analysis, "daily_input_snapshot", nullcontext)
    monkeypatch.setattr(analysis, "_kline_rolling_state_path", lambda: None)
    monkeypatch.setattr(analysis, "_attach_canonical_chase_risk_evidence", lambda frame, *_a, **_k: frame)
    monkeypatch.setenv("PROBIGA_KLINE_FEATURE_CHUNK_DAYS", "5")
    monkeypatch.setattr(analysis, "load_daily_input_window", lambda *_a, **_k: {
        "sessions": days,
        "catalog_batches_by_session": catalog,
        "daily_partition_roots": dict(roots),
    })
    queried = []
    interrupted = False

    def read(_engine, _sql, *, params, **_kwargs):
        start, end = params["chunk_start_date"], params["chunk_end_date"]
        queried.append(start)
        if interrupted and start == days[5]:
            raise OSError("test interruption after a durable first partition")
        return rows.loc[rows.trade_date.between(date.fromisoformat(start), date.fromisoformat(end))].reset_index(drop=True)

    monkeypatch.setattr(analysis, "_read_kline_feature_frame", read)
    exact = datetime(2026, 11, 13, 18)

    def run(cutoff=exact, **kwargs):
        return analysis.load_kline_features(object(), days[-1], decision_known_at=cutoff, **kwargs)

    reference = run()
    all_partitions = queried[:]
    queried.clear()
    interrupted = True
    artifact_engine = create_engine("sqlite:///" + str(tmp_path / "kline-checkpoint.sqlite"))
    privileged_migrate_formal_analysis_schema(artifact_engine)
    store = FormalAnalysisStore(artifact_engine)
    formal = {"formal_store": store, "formal_generation_sha256": "d" * 64}
    with pytest.raises(analysis.KlineFeatureDataBlocked, match="KLINE_FEATURE_CHUNK_READ_FAILED"):
        run(**formal)

    queried.clear()
    interrupted = False
    technical = analysis._kline_technical_window
    computed = []
    interrupt_technical = True

    def compute(frame, target):
        codes = frame["stock_code"].unique().tolist()
        computed.append(codes)
        if interrupt_technical and "688613" in codes:
            raise OSError("test interruption after a durable first stock part")
        return technical(frame, target)

    monkeypatch.setattr(analysis, "_kline_technical_window", compute)
    with pytest.raises(OSError, match="durable first stock part"):
        run(**formal)
    assert queried == all_partitions[1:]
    assert computed == [["000001", "000005"], ["688613"]]

    queried.clear()
    computed.clear()
    interrupt_technical = False
    resumed = run(**formal)
    assert queried == []
    assert computed == [["688613"]]
    pd.testing.assert_frame_equal(resumed, reference, check_exact=True)
    assert resumed.attrs == reference.attrs

    queried.clear()
    computed.clear()
    roots[days[7]] = "e" * 64
    later = run(**formal)
    assert queried == [all_partitions[1]]
    assert computed == []
    assert later.attrs["qmt_daily_input_window"]["daily_partition_roots"][days[7]] == "e" * 64

    # The revision sees a newly attested close and amount for only one security.
    last = rows["trade_date"] == date.fromisoformat(days[-1])
    revised = last & (rows["stock_code"] == "000001")
    rows.loc[revised, "close"] = 10.8
    rows.loc[revised, "amount"] = 400_000_000
    renamed = last & (rows["stock_code"] == "688613")
    rows.loc[renamed, "short_name"] = "Fresh target name"
    roots[days[-1]] = "f" * 64
    premarket_cutoff = datetime(2026, 11, 16, 8, 30)
    premarket = {**formal, "formal_generation_sha256": "f" * 64}
    broad_reference = run(cutoff=premarket_cutoff)
    queried.clear()
    computed.clear()
    progress = []
    revised_market = run(cutoff=premarket_cutoff, progress_callback=progress.append, **premarket)
    assert queried == all_partitions
    assert computed == [["000001"]]
    assert progress[-1]["kline_technical_recomputed_code_count"] == 1
    assert progress[-1]["kline_technical_reused_code_count"] == 2
    assert revised_market.loc[revised_market.stock_code == "688613", "short_name"].item() == "Fresh target name"
    pd.testing.assert_frame_equal(revised_market, broad_reference, check_exact=True)
    assert revised_market.attrs == broad_reference.attrs

    def ranked(frame):
        scores = frame.assign(
            ai_score=88, long_term_score=82, short_term_score=86,
            quality_score=90, final_trade_score=89, entry_score=85, capital_score=84,
            risk_reward_ratio=2.0, turnover_ratio=5.0, main_wave_score=80,
            main_wave_signal="WATCH", signal_status="WATCH",
            recommend_status="SUSPENDED", event_risk_level="LOW",
        )
        return [row["stock_code"] for row in analysis.build_recommendation_rows(
            scores, days[-1], top_n=80, min_score=62,
        )]

    assert ranked(reference) == ["000005", "688613"]
    assert ranked(revised_market) == ranked(broad_reference) == ["000001", "688613"]

    queried.clear()
    computed.clear()
    progress.clear()
    retried = run(cutoff=premarket_cutoff, progress_callback=progress.append, **premarket)
    assert queried == computed == []
    assert progress[-1]["kline_technical_recomputed_code_count"] == 0
    assert progress[-1]["kline_technical_reused_code_count"] == 3
    pd.testing.assert_frame_equal(retried, revised_market, check_exact=True)
    artifact_engine.dispose()
