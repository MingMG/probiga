"""Deterministic fixtures are confined to tests; production uses verified bars."""
from copy import deepcopy
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.engine.strategy_price_backtest import technical_history, supports_price_rule, _group_inputs, _identity_rows
from server.engine.strategy_rule_replay import replay_rule


def bars_fixture(n=75):
    days = [d.date().isoformat() for d in pd.bdate_range("2026-06-01", periods=n)]
    return [{"stock_code": code, "trade_date": day, "open": 10 * rate ** (i - 1),
        "pre_close": 10 * rate ** (i - 1), "close": 10 * rate ** i,
        "high": 10.2 * rate ** i, "low": 9.8 * rate ** i,
        "amount": 1e8 + i * 10000, "volume": 1e7}
        for code, rate in [("000001", 1.01), ("000002", .99)] for i, day in enumerate(days)]


def test_trailing_features_match_actual_close_producer_without_future_bars():
    from biz.analysis.sync_analysis_fast import _kline_technical_window
    raw = bars_fixture()
    result = technical_history(raw)
    target = sorted(set(r["trade_date"] for r in raw))[62]
    frame = pd.DataFrame(raw)
    frame["trade_date"] = pd.to_datetime(frame.trade_date).dt.date
    frame = frame.sort_values(["stock_code", "trade_date"])
    frame["change_pct"] = (frame.close / frame.pre_close - 1) * 100
    expected = _kline_technical_window(frame.copy(), date.fromisoformat(target)).set_index("stock_code")
    actual = result.loc[result.trade_date == target].set_index("stock_code")
    for key in ["pct_5", "pct_20", "previous_change_pct", "dist_ma20", "volatility_20", "amount_ratio_5", "amount_ratio_20", "drawdown_60", "from_low_60"]:
        assert actual[key].tolist() == pytest.approx(expected[key].tolist())
    changed = deepcopy(raw)
    for row in changed:
        if row["trade_date"] > target:
            for key in ["open", "high", "low", "close", "pre_close", "amount"]:
                row[key] *= 100
    pd.testing.assert_frame_equal(result.loc[result.trade_date <= target].reset_index(drop=True),
        technical_history(changed).loc[lambda f: f.trade_date <= target].reset_index(drop=True))


def test_duplicate_or_missing_market_facts_are_not_zero_filled():
    raw = bars_fixture(25)
    with pytest.raises(ValueError, match="重复"):
        technical_history(raw + [raw[0]])
    raw[0]["pre_close"] = None
    with pytest.raises(ValueError, match="缺失"):
        technical_history(raw)


def test_unrelated_ai_and_flow_scores_still_require_original_frozen_evidence():
    rule = {"market_all": [{"field": "market_breadth_pct"}], "entry_all": [{"field": "pct_20"}], "entry_any": [], "ranking": [{"field": "volatility_20"}]}
    assert supports_price_rule(rule)
    for field in ["capital_score", "technical_score", "turnover_ratio", "sector_rotation_score"]:
        rule["entry_all"][0]["field"] = field
        assert not supports_price_rule(rule)


def test_archive_uses_retained_proof_and_recent_days_use_their_actual_manifest():
    from server.common.qmt_datadir_daily_coverage import MANIFEST_TABLE, SCHEMA, PROVIDER
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE qmt_kline_attestation_run(run_id TEXT,start_date TEXT,end_date TEXT,status TEXT,finished_at TEXT,provider TEXT)"))
        c.execute(text("INSERT INTO qmt_kline_attestation_run VALUES('bulk','2026-01-01','2026-07-31','COMPLETED','2026-08-01','gj_big_qmt_inner')"))
        c.execute(text(f"CREATE TABLE {MANIFEST_TABLE}(trade_date TEXT,schema_version TEXT,dataset TEXT,provider TEXT,status TEXT,strategy_eligible INT,captured_at TEXT)"))
        c.execute(text(f"INSERT INTO {MANIFEST_TABLE} VALUES('2026-07-31',:schema,'stock_daily',:provider,'EXACT',1,'2026-08-02')"), {"schema": SCHEMA, "provider": PROVIDER})
        groups = _group_inputs(c, ["2026-07-29", "2026-07-30", "2026-07-31", "2026-08-03"], "2026-11-22")
    assert groups[0]["sources"] == {"2026-07-29": "QMT_RETAINED", "2026-07-30": "QMT_RETAINED", "2026-07-31": "QMT_MANIFEST", "2026-08-03": "VERIFIED_PUBLIC"}


def test_old_day_never_borrows_present_day_name_or_industry():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE qmt_membership_snapshot_run(snapshot_date TEXT,source TEXT)"))
        c.execute(text("INSERT INTO qmt_membership_snapshot_run VALUES('2026-11-20','gj_big_qmt_inner')"))
        rows, proof = _identity_rows(c, "2026-08-31")
    assert rows == {} and proof["status"] == "UNAVAILABLE"


def test_missing_entry_identity_excludes_that_entry_but_preserves_known_cash_outcome():
    raw = bars_fixture(30)
    days = sorted(set(r["trade_date"] for r in raw))
    rule = {"holding_days": 2, "stop_loss_pct": 5}
    signals = [{"stock_code": "000001", "stock_name": "", "industry": None, "rank": 1,
                "signal_known_at": days[20] + " 18:00:00"},
               {"stock_code": "000001", "stock_name": "测试证券", "industry": "测试行业", "rank": 1,
                "signal_known_at": days[22] + " 18:00:00"}]
    r = replay_rule(rule, signals, raw, days, evaluation_start=days[20])
    assert r["status"] == "PARTIAL" and r["net_return_pct"] is None
    assert r["verified_input_subset_return_pct"] is not None
    assert r["input_exclusion_count"] == 1
    assert r["completed_trade_count"] == 1
    assert len(r["nav"]) == 10 and r["nav"][0]["trade_date"] == days[20]
    assert r["funding_eligible"] is False
