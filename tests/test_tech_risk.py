# -*- coding: utf-8 -*-
from server.common.tech_risk import build_black_swan_signal, format_black_swan_markdown


def test_black_swan_signal_flags_exposed_holding_with_market_confirmation():
    news_rows = [
        {
            "title": "Meta计划出租剩余AI算力，全球科技股大跌",
            "content": "算力供给过剩担忧升温，芯片、光模块、AI服务器方向集体杀跌。",
            "source": "sample",
            "subjects": "[]",
            "stocks": "[]",
        }
    ]
    portfolio_rows = [
        {
            "stock_code": "300308",
            "short_name": "中际旭创",
            "shares": 100,
            "industry_name": "通信",
            "concept_tag": "CPO;AI算力;光模块",
            "cost_price": 100,
            "cur_price": 95,
            "change_pct": -4.2,
        }
    ]
    sector_rows = [{"concept_name": "CPO", "change_pct": -4.0, "hot_value": 80}]
    market_rows = [
        {"_kind": "overview", "up_count": 900, "down_count": 4200, "total": 5200, "avg_change_pct": -1.8},
        {"_kind": "index", "index_code": "000688", "change_pct": -2.3},
        {"_kind": "index", "index_code": "399006", "change_pct": -2.0},
    ]

    signal = build_black_swan_signal(news_rows, portfolio_rows, sector_rows, market_rows, [])

    assert signal["triggered"] is True
    assert signal["status"] == "escape_now"
    assert signal["exposed_holdings"][0]["stock_code"] == "300308"
    assert "中际旭创" in signal["action"]
    assert signal["market_context"]["phase"] == "risk_off"


def test_decision_radar_surfaces_opportunity_sectors_and_candidate_stocks():
    news_rows = [
        {
            "title": "国常会加码设备更新和以旧换新，补贴范围继续扩围",
            "content": "政策支持稳增长，工程机械、家电汽车等方向有望受益。",
            "source": "sample",
            "subjects": "[]",
            "stocks": "[]",
        }
    ]
    sector_rows = [{"concept_name": "工程机械", "change_pct": 3.2, "hot_value": 90}]
    candidate_rows = [
        {
            "stock_code": "000425",
            "short_name": "徐工机械",
            "industry_name": "工程机械",
            "concept_tag": "设备更新;稳增长",
            "final_trade_score": 82,
        }
    ]
    market_rows = [
        {"_kind": "overview", "up_count": 2800, "down_count": 2200, "total": 5200, "avg_change_pct": 0.6},
    ]

    signal = build_black_swan_signal(news_rows, [], sector_rows, market_rows, candidate_rows)
    opportunity = signal["opportunity"]

    assert opportunity["status"] == "watch"
    assert any(item["name"] == "工程机械" for item in opportunity["opportunity_sectors"])
    assert opportunity["candidate_stocks"][0]["stock_code"] == "000425"
    assert "徐工机械" in format_black_swan_markdown(signal)
