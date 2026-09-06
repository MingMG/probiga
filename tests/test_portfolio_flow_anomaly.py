from datetime import datetime, timedelta

from server.api.routers import hot_data


TRADE_DATE = "2026-09-06"


def _fresh_row(
    code: str,
    normalized_flow: float,
    *,
    amount: float = 1_000_000_000.0,
) -> dict:
    return {
        "stock_code": code,
        "flow_status": "fresh",
        "flow_attitude_basis": "minute_5m_fresh",
        "flow_5m": normalized_flow * amount,
        "quote_amount": amount,
        "flow_latest_time": f"{TRADE_DATE} 10:05:00",
        "flow_trade_date": TRADE_DATE,
        "expected_flow_date": TRADE_DATE,
        "quote_trade_date": TRADE_DATE,
        "quote_snapshot_at": f"{TRADE_DATE} 10:05:00",
        "quote_status": "fresh",
        "quote_age_seconds": 30,
        "flow_age_seconds": 30,
        "flow_source": "qmt_min_flow",
    }


def test_portfolio_flow_anomaly_uses_relative_cross_section_not_absolute_amount():
    rows = [
        _fresh_row("000001", 0.0010),
        _fresh_row("000002", 0.0011),
        _fresh_row("000003", 0.0009),
        # Large absolute inflow, but ordinary after turnover normalization.
        _fresh_row("000004", 0.0012, amount=100_000_000_000.0),
        _fresh_row("000005", 0.0008),
        # Smaller absolute inflow, but exceptional relative to turnover.
        _fresh_row("000006", 0.0100, amount=100_000_000.0),
    ]

    hot_data._portfolio_apply_flow_anomalies(
        rows,
        expected_trade_date=TRADE_DATE,
    )

    by_code = {row["stock_code"]: row["flow_anomaly"] for row in rows}
    alert = by_code["000006"]
    large_absolute = by_code["000004"]

    assert alert["status"] == "alert"
    assert alert["direction"] == "inflow"
    assert alert["robust_z"] >= 2.0
    assert alert["threshold"] == 2.0
    assert alert["sample_size"] == 6
    assert alert["normalized_value"] == 0.01
    assert alert["normalized_flow_pct"] == 1.0
    assert alert["method"] == "flow_5m_over_cumulative_amount_cross_section_robust_z"
    assert alert["flow_time"] == f"{TRADE_DATE} 10:05:00"
    assert alert["flow_date"] == TRADE_DATE
    assert alert["flow_age_seconds"] == 30
    assert alert["source"] == "qmt_min_flow"
    assert "达到 2.0" in alert["reason"]

    assert large_absolute["status"] == "normal"
    assert large_absolute["robust_z"] < 2.0
    assert rows[3]["flow_5m"] > rows[5]["flow_5m"]


def test_portfolio_flow_anomaly_builds_baseline_and_fails_closed():
    rows = [
        _fresh_row("000001", 0.0010),
        _fresh_row("000002", 0.0011),
        _fresh_row("000003", 0.0009),
        _fresh_row("000004", 0.0012),
    ]
    expired = _fresh_row("000005", 0.0200)
    expired["flow_age_seconds"] = hot_data.PORTFOLIO_FLOW_FRESH_SECONDS + 1
    wrong_day = _fresh_row("000006", 0.0200)
    wrong_day["flow_trade_date"] = "2026-09-05"
    wrong_day["flow_latest_time"] = "2026-09-05 10:05:00"
    missing_flow = _fresh_row("000007", 0.0200)
    missing_flow["flow_5m"] = None
    missing_amount = _fresh_row("000008", 0.0200)
    missing_amount["quote_amount"] = 0
    stale = _fresh_row("000009", 0.0200)
    stale["flow_status"] = "stale"
    wrong_basis = _fresh_row("000010", 0.0200)
    wrong_basis["flow_attitude_basis"] = "minute_current_fresh"
    missing_source = _fresh_row("000011", 0.0200)
    missing_source["flow_source"] = ""
    rows.extend(
        [
            expired,
            wrong_day,
            missing_flow,
            missing_amount,
            stale,
            wrong_basis,
            missing_source,
        ]
    )

    hot_data._portfolio_apply_flow_anomalies(
        rows,
        expected_trade_date=TRADE_DATE,
    )

    for row in rows[:4]:
        anomaly = row["flow_anomaly"]
        assert anomaly["status"] == "baseline_building"
        assert anomaly["sample_size"] == 4
        assert anomaly["robust_z"] is None
        assert "至少需要 5 个" in anomaly["reason"]

    for row in rows[4:]:
        anomaly = row["flow_anomaly"]
        assert anomaly["status"] == "unavailable"
        assert anomaly["sample_size"] == 4
        assert anomaly["robust_z"] is None

    assert "过期" in expired["flow_anomaly"]["reason"]
    assert "日期" in wrong_day["flow_anomaly"]["reason"]
    assert "5分钟资金增量" in missing_flow["flow_anomaly"]["reason"]
    assert "累计成交额" in missing_amount["flow_anomaly"]["reason"]
    assert "不是当前交易日的新鲜数据" in stale["flow_anomaly"]["reason"]
    assert "5分钟资金基线" in wrong_basis["flow_anomaly"]["reason"]
    assert "资金来源缺失" in missing_source["flow_anomaly"]["reason"]


def test_portfolio_flow_anomaly_accepts_same_day_amount_fallback():
    rows = [
        _fresh_row("000001", 0.0010),
        _fresh_row("000002", 0.0011),
        _fresh_row("000003", 0.0009),
        _fresh_row("000004", 0.0012),
        _fresh_row("000005", 0.0008),
        _fresh_row("000006", 0.0100),
    ]
    fallback = rows[-1]
    fallback["amount"] = fallback.pop("quote_amount")

    hot_data._portfolio_apply_flow_anomalies(
        rows,
        expected_trade_date=TRADE_DATE,
    )

    assert fallback["flow_anomaly"]["status"] == "alert"
    assert fallback["flow_anomaly"]["normalized_flow_pct"] == 1.0


def test_portfolio_flow_anomaly_rejects_stale_or_misaligned_turnover():
    rows = [
        _fresh_row("000001", 0.0010),
        _fresh_row("000002", 0.0011),
        _fresh_row("000003", 0.0009),
        _fresh_row("000004", 0.0012),
        _fresh_row("000005", 0.0008),
        _fresh_row("000006", 0.0100),
        _fresh_row("000007", 0.0200),
    ]
    stale_quote = rows[-2]
    stale_quote["quote_status"] = "stale"
    stale_quote["quote_age_seconds"] = hot_data.PORTFOLIO_LIVE_FRESH_SECONDS + 1
    skewed_quote = rows[-1]
    skewed_quote["quote_snapshot_at"] = f"{TRADE_DATE} 10:00:00"

    hot_data._portfolio_apply_flow_anomalies(
        rows,
        expected_trade_date=TRADE_DATE,
    )

    assert stale_quote["flow_anomaly"]["status"] == "unavailable"
    assert "不是新鲜快照" in stale_quote["flow_anomaly"]["reason"]
    assert skewed_quote["flow_anomaly"]["status"] == "unavailable"
    assert skewed_quote["flow_anomaly"]["time_skew_seconds"] == 300
    assert "时刻不一致" in skewed_quote["flow_anomaly"]["reason"]
    assert all(row["flow_anomaly"]["sample_size"] == 5 for row in rows)


def test_portfolio_watch_analysis_labels_minute_ratio_as_direction_share():
    intraday = hot_data._portfolio_build_watch_analysis(
        {
            "stock_code": "000001",
            "cur_price": 10.0,
            "change_pct": 2.0,
            "main_net_inflow": 50_000_000.0,
            "flow_status": "fresh",
            "flow_attitude_basis": "minute_5m_fresh",
            "flow_attitude": "strong_in",
            "flow_attitude_label": "强进",
            "flow_attitude_ratio": 12.5,
            "flow_5m": 10_000_000.0,
            "quote_amount": 200_000_000.0,
        }
    )
    intraday_funds = next(
        item for item in intraday["evidence"] if item["label"] == "资金"
    )

    assert "5分钟方向占比 12.5%" in intraday_funds["value"]
    assert "占成交额 12.5%" not in intraday_funds["value"]

    closed = hot_data._portfolio_build_watch_analysis(
        {
            "stock_code": "000001",
            "cur_price": 10.0,
            "change_pct": 1.0,
            "main_net_inflow": 10_000_000.0,
            "flow_status": "closed",
            "flow_attitude_basis": "daily_close",
            "quote_amount": 200_000_000.0,
        }
    )
    closed_funds = next(
        item for item in closed["evidence"] if item["label"] == "资金"
    )

    assert "占当日成交额 5.0%" in closed_funds["value"]


def test_portfolio_five_minute_flow_rejects_old_baseline_after_collection_gap(monkeypatch):
    trade_date = datetime.now().strftime("%Y-%m-%d")
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        lambda *_args, **_kwargs: [
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 10:00:00",
                "main_net_inflow": 10_000_000.0,
            },
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 14:00:00",
                "main_net_inflow": 90_000_000.0,
            },
        ],
    )
    monkeypatch.setattr(hot_data, "_portfolio_time_age_seconds", lambda _value: 12)

    result = hot_data._portfolio_min_flow_summary(
        ["000001"], trade_date=trade_date, market_mode="intraday"
    )["000001"]

    assert result["flow_status"] == "fresh"
    assert result["flow_5m"] is None
    assert result["flow_5m_status"] == "baseline_building"
    assert result["flow_attitude_basis"] == "minute_current_fresh"


def test_portfolio_five_minute_flow_accepts_four_to_seven_minute_baselines(monkeypatch):
    trade_date = datetime.now().strftime("%Y-%m-%d")
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        lambda *_args, **_kwargs: [
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 10:16:00",
                "main_net_inflow": 10_000_000.0,
            },
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 10:20:00",
                "main_net_inflow": 25_000_000.0,
            },
            {
                "stock_code": "000002",
                "trade_time": f"{trade_date} 10:13:00",
                "main_net_inflow": -20_000_000.0,
            },
            {
                "stock_code": "000002",
                "trade_time": f"{trade_date} 10:20:00",
                "main_net_inflow": -5_000_000.0,
            },
        ],
    )
    monkeypatch.setattr(hot_data, "_portfolio_time_age_seconds", lambda _value: 12)

    result = hot_data._portfolio_min_flow_summary(
        ["000001", "000002"], trade_date=trade_date, market_mode="intraday"
    )

    assert result["000001"]["flow_5m"] == 15_000_000.0
    assert result["000002"]["flow_5m"] == 15_000_000.0
    assert result["000001"]["flow_5m_status"] == "available"
    assert result["000002"]["flow_attitude_basis"] == "minute_5m_fresh"


def test_portfolio_minute_flow_does_not_treat_future_timestamp_as_fresh(monkeypatch):
    trade_date = datetime.now().strftime("%Y-%m-%d")
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        lambda *_args, **_kwargs: [
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 10:15:00",
                "main_net_inflow": 10_000_000.0,
            },
            {
                "stock_code": "000001",
                "trade_time": f"{trade_date} 10:20:00",
                "main_net_inflow": 25_000_000.0,
            },
        ],
    )
    monkeypatch.setattr(hot_data, "_portfolio_time_age_seconds", lambda _value: -30)

    result = hot_data._portfolio_min_flow_summary(
        ["000001"], trade_date=trade_date, market_mode="intraday"
    )["000001"]

    assert result["flow_status"] == "stale"
    assert result["flow_attitude"] == ""
    assert result["flow_attitude_basis"] == ""
    assert result["flow_5m_status"] == "unavailable"


def test_portfolio_time_age_preserves_future_clock_skew():
    future = datetime.now() + timedelta(minutes=1)
    assert hot_data._portfolio_time_age_seconds(future) < 0
