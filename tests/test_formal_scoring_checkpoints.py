import copy
from contextlib import nullcontext
from datetime import date, datetime
from decimal import Decimal
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from biz.analysis import sync_analysis_fast as analysis
from biz.analysis.formal_scoring_checkpoints import checkpoint_factor_frame
from server.common.analysis_pool_receipt import canonical_sha256
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema


class CheckpointStore:
    def __init__(self):
        self.members = {}
        self.artifacts = {}

    def put_many(self, generation, stage, payloads):
        return {key: self.put(generation, stage, key, payload) for key, payload in payloads.items()}

    def put(self, generation, stage, key, payload):
        sha256 = canonical_sha256(payload)
        self.artifacts[sha256] = json.loads(json.dumps(payload, sort_keys=True, allow_nan=False))
        member = (generation, stage, key)
        assert self.members.get(member, sha256) == sha256
        self.members[member] = sha256
        return sha256

    def get(self, generation, stage, key):
        sha256 = self.members.get((generation, stage, key))
        return self.load_artifact(sha256) if sha256 else None

    def load_artifact(self, sha256):
        return copy.deepcopy(self.artifacts[sha256])

    def load_artifacts(self, hashes):
        return {sha256: self.load_artifact(sha256) for sha256 in hashes}


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


def test_formal_kline_resumes_rows_and_preserves_full_market_features(monkeypatch):
    days = list(pd.bdate_range(end="2026-11-13", periods=60).strftime("%Y-%m-%d"))
    catalog = {day: "catalog-" + day for day in days}
    roots = {day: canonical_sha256(day) for day in days}
    rows = pd.DataFrame([
        {
            "stock_code": code, "short_name": "A", "trade_date": date.fromisoformat(day),
            "open": 10, "high": 12, "low": 9,
            "close": 10 + (i % 4) / 10 + j / 10,
            "pre_close": 10, "volume": 100_000 + i, "amount": 10_000_000 + i,
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
    monkeypatch.setattr(analysis, "_KLINE_TECHNICAL_STOCKS_PER_PART", 2)
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

    def run(**kwargs):
        return analysis.load_kline_features(object(), days[-1], decision_known_at=exact, **kwargs)

    reference = run()
    all_partitions = queried[:]
    queried.clear()
    interrupted = True
    store = CheckpointStore()
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
    roots[days[7]] = "e" * 64
    later = run(**formal)
    assert queried == [all_partitions[1]]
    assert later.attrs["qmt_daily_input_window"]["daily_partition_roots"][days[7]] == "e" * 64
