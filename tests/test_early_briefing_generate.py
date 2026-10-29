import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text

from biz.early_briefing import generate
from integrations.wecom.delivery import WeComDeliveryError


TARGET_DATE = "2026-08-25"


def test_expected_briefing_indices_use_the_five_configured_market_indices():
    assert generate.EXPECTED_A_SHARE_INDEX_CODES == {
        "000001",
        "399001",
        "399006",
        "000688",
        "899050",
    }


def _valid_market_data() -> dict:
    return {
        "A股数据日期": TARGET_DATE,
        "DB快讯": [],
        "_data_contract": {
            "status": "PASS",
            "target_trade_date": TARGET_DATE,
            "expected_stock_count": 5200,
            "kline_coverage": 0.998,
            "traded_flow_coverage": 1.0,
            "index_count": 5,
            "hot_concept_count": 20,
            "fused_stock_count": 20,
        },
    }


def test_delivery_error_propagates_out_of_early_briefing_main(monkeypatch):
    engine = object()
    market_data = _valid_market_data()
    delivery_error = WeComDeliveryError(
        "briefing webhook is not configured",
        delivery_id="delivery-early-test",
    )
    monkeypatch.setattr(sys, "argv", ["early-briefing"])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(
        generate, "resolve_target_trade_date", Mock(return_value=TARGET_DATE)
    )
    collect = Mock(return_value=market_data)
    monkeypatch.setattr(generate, "collect_market_data", collect)
    monkeypatch.setattr(generate, "crawl_all_news", Mock(return_value=[]))
    monkeypatch.setattr(
        generate,
        "get_settings",
        lambda: SimpleNamespace(deepseek_api_key=None),
    )
    monkeypatch.setattr(generate, "_generate_fallback", Mock(return_value="早报正文"))
    monkeypatch.setattr(generate, "append_tech_risk", lambda content, _data: content)
    monkeypatch.setattr(generate, "append_research_radar", lambda content, _data: content)
    monkeypatch.setattr(generate, "get_wecom_webhook", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(generate, "deliver_markdown", Mock(side_effect=delivery_error))

    with pytest.raises(WeComDeliveryError) as exc_info:
        generate.main()

    assert exc_info.value is delivery_error
    collect.assert_called_once_with(engine, TARGET_DATE)


def test_stale_contract_fails_before_news_or_delivery(monkeypatch):
    engine = object()
    market_data = _valid_market_data()
    market_data["_data_contract"]["target_trade_date"] = "2026-08-22"
    crawl = Mock(side_effect=AssertionError("stale briefing must not crawl news"))
    push = Mock(side_effect=AssertionError("stale briefing must not be delivered"))
    monkeypatch.setattr(sys, "argv", ["early-briefing"])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(
        generate, "resolve_target_trade_date", Mock(return_value=TARGET_DATE)
    )
    monkeypatch.setattr(generate, "collect_market_data", Mock(return_value=market_data))
    monkeypatch.setattr(generate, "crawl_all_news", crawl)
    monkeypatch.setattr(generate, "push_to_wecom", push)

    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*stale or incomplete"):
        generate.main()

    crawl.assert_not_called()
    push.assert_not_called()


def test_early_briefing_contract_rejects_coverage_below_briefing_floor():
    market_data = _valid_market_data()
    market_data["_data_contract"]["kline_coverage"] = 0.97

    with pytest.raises(RuntimeError, match="stale or incomplete"):
        generate._assert_market_data_contract(market_data, TARGET_DATE)


def test_finalize_briefing_removes_action_language_and_adds_etf_evidence():
    content = "科技触发先跑预警，建议买入高位股。"
    rendered = generate._finalize_briefing(content, {
        "数据完整性": "K线覆盖 99.82%",
        "宽基ETF份额推算": {
            "数据状态": "已获取",
            "当日净申购": "+255.74亿元",
            "覆盖": "4/4",
            "申赎主体": "未确认",
        },
    })

    assert "先跑" not in rendered
    assert "建议买入" not in rendered
    assert "宽基ETF份额推算：+255.74亿元" in rendered
    assert "以上为信息整理，不构成投资建议。" in rendered


def test_core_snapshot_rejects_mixed_kline_date(monkeypatch):
    reads = iter(
        [
            [{"stock_code": "000001", "trade_date": "2026-08-22", "volume": 1, "amount": 1}],
            [{"stock_code": "000001", "trade_date": TARGET_DATE, "main_net_inflow": 1}],
        ]
    )
    monkeypatch.setattr(generate, "_read_required_sql", lambda *_args, **_kwargs: next(reads))

    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*mixed dates"):
        generate._load_core_market_snapshot(object(), TARGET_DATE)


def test_core_snapshot_routes_pure_capital_flow_to_minute_db(monkeypatch):
    primary = create_engine("sqlite+pysqlite:///:memory:", future=True)
    minute = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with primary.begin() as conn:
            conn.execute(text("""
                CREATE TABLE sm_stock_kline (
                    stock_code TEXT, trade_date TEXT, volume REAL, amount REAL,
                    k_type INTEGER, adjust_type INTEGER
                )
            """))
            conn.execute(text("""
                INSERT INTO sm_stock_kline VALUES
                ('000001','2026-08-25',100,1000,1,0)
            """))
            conn.execute(text("CREATE TABLE si_all_code (stock_code TEXT, short_name TEXT)"))
            conn.execute(text("INSERT INTO si_all_code VALUES ('000001','平安银行')"))
            conn.execute(text("""
                CREATE TABLE sm_stock_capital_flow_daily (
                    stock_code TEXT, trade_date TEXT, main_net_inflow REAL
                )
            """))
            conn.execute(text("""
                INSERT INTO sm_stock_capital_flow_daily VALUES
                ('000001','2026-08-25',-999)
            """))
        with minute.begin() as conn:
            conn.execute(text("""
                CREATE TABLE sm_stock_capital_flow_daily (
                    stock_code TEXT, trade_date TEXT, main_net_inflow REAL
                )
            """))
            conn.execute(text("""
                INSERT INTO sm_stock_capital_flow_daily VALUES
                ('000001','2026-08-25',123)
            """))

        required_read = generate._read_required_sql

        def read_with_complete_non_stock_fixtures(engine, sql, params=None):
            if "FROM sm_index_kline" in sql:
                return [
                    {
                        "index_code": code,
                        "price": 1,
                        "change_pct": 0,
                        "trade_date": TARGET_DATE,
                    }
                    for code in generate.EXPECTED_A_SHARE_INDEX_CODES
                ]
            if "FROM st_hot_concept_ths_daily" in sql:
                return [
                    {
                        "concept_code": f"C{idx:02d}",
                        "snapshot_date": TARGET_DATE,
                        "plate_type": 1 if idx < 10 else 2,
                    }
                    for idx in range(20)
                ]
            if "FROM st_hot_rank_fused" in sql:
                return [
                    {
                        "stock_code": f"{idx:06d}",
                        "snapshot_date": TARGET_DATE,
                    }
                    for idx in range(20)
                ]
            return required_read(engine, sql, params)

        monkeypatch.setattr(
            "server.common.minute_data.get_minute_engine", lambda: minute
        )
        monkeypatch.setattr("server.common.batch_db.get_kline_engine", lambda: primary)
        monkeypatch.setattr(
            generate, "_read_required_sql", read_with_complete_non_stock_fixtures
        )
        monkeypatch.setattr(generate, "load_daily_stock_universe", lambda *_args, **_kwargs: object())
        monkeypatch.setattr(
            generate,
            "validate_daily_stock_coverage",
            lambda *_args, **_kwargs: {"status": "PASS"},
        )

        snapshot = generate._load_core_market_snapshot(primary, TARGET_DATE)

        assert snapshot["flow_rows"] == [
            {
                "stock_code": "000001",
                "trade_date": TARGET_DATE,
                "main_net_inflow": 123.0,
                "sn": "平安银行",
            }
        ]
        assert snapshot["kline_rows"][0]["stock_code"] == "000001"
    finally:
        primary.dispose()
        minute.dispose()


def test_market_trend_markdown_explains_state_without_calling_low_a_bottom():
    trend = {
        "methodology": {
            "indicators": [
                {"name": "SMA", "parameters": {"fast": 20, "slow": 60}},
                {"name": "RSI", "parameters": {"period": 14}},
            ]
        },
        "indices": [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "data_cutoff": TARGET_DATE,
                "summary": {
                    "daily": "日线：当前反复震荡。",
                    "weekly": "周线：当前下行（本周尚未结束，属于暂时变化）。",
                    "monthly": "月线背景：当前下行（本月尚未结束，属于暂时变化）。",
                    "position": "所处位置：指标进入历史偏低区域，但低位不等于底部。",
                    "overall": "综合判断：短期变化尚未改变中期趋势。",
                    "watch": "后续观察：周线是否停止创新低。",
                },
            }
        ],
    }

    rendered = generate.format_market_trend_markdown(trend)

    assert "沪深300" in rendered
    assert "暂时变化" in rendered
    assert "月线背景" in rendered
    assert "低位不等于底部" in rendered
    assert "SMA" in rendered and "RSI" in rendered
    assert generate.append_market_trend("早报正文", {"大盘中长期趋势": trend}).count(
        "大盘中长期趋势"
    ) == 1
    assert generate.append_market_trend(rendered, {"大盘中长期趋势": trend}) == rendered
    ai_only = "## 大盘中长期趋势\n模型生成的概括"
    appended = generate.append_market_trend(ai_only, {"大盘中长期趋势": trend})
    assert "模型生成的概括" in appended
    assert "**🧭 大盘中长期趋势（系统计算）**" in appended


def test_market_trend_prompt_view_drops_long_transition_history():
    trend = {
        "status": "ok",
        "indices": [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "data_cutoff": TARGET_DATE,
                "summary": {"daily": "日线：反复震荡。"},
                "periods": {
                    "daily": {
                        "confirmation_status": "final",
                        "direction": "range",
                        "position": "middle",
                        "bottoming": "not_seen",
                        "strengthening": "not_confirmed",
                        "metrics": {"rsi14": 48.0},
                        "evidence": ["RSI14为48.0"],
                        "history": [{"changed_at": "2026-08-01"}],
                    }
                },
            }
        ],
    }

    prompt_view = generate._market_trend_prompt_view(trend)

    assert prompt_view["indices"][0]["periods"]["daily"]["direction"] == "range"
    assert "history" not in prompt_view["indices"][0]["periods"]["daily"]


def test_external_market_loader_uses_public_snapshot_evidence(monkeypatch):
    snapshot = {
        "acquisition_status": "COMPLETE",
        "available_count": 2,
        "expected_count": 2,
        "missing_symbols": [],
        "items": [
            {
                "symbol": "nasdaq",
                "availability": "available",
                "price": 26477.81,
                "change_pct": 1.18,
                "market_time": "2026-10-28 04:00:00",
            },
            {
                "symbol": "a50",
                "availability": "available",
                "price": 13098.0,
                "change_pct": 0.52,
                "market_time": "2026-10-28 08:30:00",
            },
        ],
    }
    fetch = Mock(return_value=snapshot)
    monkeypatch.setattr(generate, "fetch_external_market_snapshot", fetch)

    result = generate._collect_external_market_data(
        as_of=datetime(2026, 10, 28, 8, 30)
    )

    assert result["美股"]["纳斯达克"].startswith("+1.18%")
    assert result["外围市场"]["富时中国A50"].startswith("+0.52%")
    assert result["外围数据状态"] == "COMPLETE：2/2 项可用"
    fetch.assert_called_once_with(as_of=datetime(2026, 10, 28, 8, 30))


def test_external_market_loader_formats_treasury_change_in_basis_points():
    rendered = generate._format_external_quote({
        "symbol": "us10y",
        "price": 5.05,
        "change_pct": 1.2,
        "market_time": "2026-10-28 18:00:00",
        "payload": {"change_bp": 6},
    })

    assert rendered.startswith("5.05%（+6bp")
    assert "+1.20%" not in rendered


def test_fallback_renders_public_us_and_external_quotes():
    content = generate._generate_fallback(
        {
            "A股数据日期": TARGET_DATE,
            "美股": {"纳斯达克": "+1.18%（26,477.81）"},
            "外围市场": {"富时中国A50": "+0.52%（13,098.00）"},
            "外围数据状态": "COMPLETE：2/2 项可用",
        },
        [],
    )

    assert "纳斯达克 +1.18%" in content
    assert "富时中国A50 +0.52%" in content
    assert "COMPLETE：2/2 项可用" in content
