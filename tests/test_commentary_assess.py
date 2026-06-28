# -*- coding: utf-8 -*-
from server.api.commentary_utils import build_rule_checks, build_verdict, parse_commentary_text, project_feasibility_summary


SAMPLE_TEXT = """
📟 PCB/算力硬件方向（6月9日板块涨停潮）
5  002938  鹏鼎控股  全球PCB龙头，6月9日板块反弹后若进入缩量震荡、不破一买低点，则形成日线二买。
6  002916  深南电路  6月9日反弹后回调，若能在前期低点上方企稳，30分钟级别二买确认。
"""


def test_parse_commentary_text_extracts_sector_codes_and_dates():
    result = parse_commentary_text(SAMPLE_TEXT, reference_date="2026-06-13")

    assert result["reference_date"] == "2026-06-13"
    assert len(result["items"]) == 2
    assert result["items"][0]["stock_code"] == "002938"
    assert result["items"][0]["sector"].startswith("📟 PCB/算力硬件方向")
    assert result["items"][0]["anchor_dates"] == ["2026-06-09"]


def test_build_verdict_marks_track_when_rules_are_healthy():
    checks = build_rule_checks(
        phase="premarket",
        current_price=100.0,
        ma5=99.0,
        ma10=98.0,
        support=97.0,
        anchor_low=95.0,
        anchor_volume=1000.0,
        latest_volume=700.0,
        news_count=2,
    )

    verdict = build_verdict(checks)

    assert verdict["status"] == "TRACK"


def test_build_verdict_marks_risk_when_anchor_low_breaks():
    checks = build_rule_checks(
        phase="intraday",
        current_price=94.0,
        ma5=99.0,
        ma10=98.0,
        support=97.0,
        anchor_low=95.0,
        anchor_volume=1000.0,
        latest_volume=1200.0,
        news_count=0,
    )

    verdict = build_verdict(checks)

    assert verdict["status"] == "RISK"


def test_project_feasibility_summary_keeps_intraday_conservative():
    summary = project_feasibility_summary({"source": "gml", "kind": "ohlc"})

    assert summary["overall"] == "high"
    assert summary["premarket"] == "high"
    assert summary["intraday"] == "medium"
