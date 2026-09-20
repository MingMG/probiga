from contextlib import nullcontext
from datetime import datetime

import pandas as pd
import pytest

from biz.analysis import sync_analysis_fast as analysis


@pytest.fixture
def native_window(monkeypatch):
    monkeypatch.setattr(analysis, "daily_input_snapshot", nullcontext)
    days = [day.date().isoformat() for day in pd.bdate_range(end="2026-09-18", periods=60)]
    rows = pd.DataFrame([
        {"stock_code": "000001", "short_name": "Example", "trade_date": day,
         "open": 10, "high": 11, "low": 9, "close": 10 + index % 3 / 10,
         "pre_close": 10, "volume": 1000, "amount": 10000,
         "change_pct": 999 + index, "turnover_ratio": 2}
        for index, day in enumerate(days)
    ])
    monkeypatch.setattr(analysis, "load_daily_input_window", lambda *_a, **_k: {
        "sessions": days,
        "catalog_batches_by_session": {day: "catalog" for day in days},
        "daily_partition_roots": {day: "a" * 64 for day in days},
    })
    monkeypatch.setattr(analysis, "_kline_rolling_state_path", lambda: None)
    monkeypatch.setattr(analysis, "_attach_canonical_chase_risk_evidence", lambda frame, *_a, **_k: frame)

    def read(_engine, _sql, *, params, **_kwargs):
        return rows.loc[rows.trade_date.between(params["chunk_start_date"], params["chunk_end_date"])].copy()

    monkeypatch.setattr(analysis, "_read_kline_feature_frame", read)
    return rows


def _run():
    return analysis.load_kline_features(
        object(), "2026-09-18", decision_known_at=datetime(2026, 9, 18, 18),
    )


def test_analysis_volatility_ignores_tampered_stored_change_pct(native_window):
    first = _run()
    native_window["change_pct"] = -8888
    second = _run()
    expected = ((native_window.close / native_window.pre_close - 1) * 100).tail(20).std()
    assert first.iloc[0]["volatility_20"] == pytest.approx(expected)
    assert second.iloc[0]["volatility_20"] == pytest.approx(expected)
    assert second.iloc[0]["change_pct"] == pytest.approx(2.0)


@pytest.mark.parametrize("invalid", [None, 0, -1, float("nan"), float("inf"), "bad"])
def test_analysis_native_pre_close_is_never_guessed(native_window, invalid):
    native_window["pre_close"] = native_window["pre_close"].astype(object)
    native_window.loc[native_window.index[-1], "pre_close"] = invalid
    with pytest.raises(analysis.KlineFeatureDataBlocked, match="KLINE_NATIVE_RETURN_INVALID"):
        _run()
