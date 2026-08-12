import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

from server.api.routers import hot_data


def _quant_row(*, review_date="2026-08-12", status="ready"):
    return {
        "review_date": date.fromisoformat(review_date),
        "adjust_type": 0,
        "publish_status": status,
        "compact_review": "大势分析\n真实量化正文" if status == "ready" else "",
        "quality_json": json.dumps(
            {
                "status": "pass" if status == "ready" else "blocked",
                "coverage": {"target": 0.995},
                "errors": [] if status == "ready" else ["目标日有效行情覆盖率不足"],
            },
            ensure_ascii=False,
        ),
        "market_structure_json": "{}",
        "industry_rotation_json": "{}",
        "factor_validation_json": "{}",
        "data_cutoff_at": "2026-08-12 20:57:06",
    }


def test_quant_review_exact_blocked_never_falls_back():
    blocked = _quant_row(status="blocked")
    with patch("server.api.routers.hot_data._read_sql", return_value=[blocked]) as read_sql:
        result = hot_data.quant_daily_review("2026-08-12", 0)

    assert result["date"] == "2026-08-12"
    assert result["fallback"] is False
    assert result["data"] == [blocked]
    assert read_sql.call_count == 1
    sql = read_sql.call_args.args[0]
    assert "review_date = :d" in sql
    assert "review_date < :d AND publish_status = 'ready'" in sql


def test_quant_review_missing_exact_uses_latest_earlier_ready():
    ready = _quant_row(review_date="2026-08-11")
    with patch("server.api.routers.hot_data._read_sql", return_value=[ready]) as read_sql:
        result = hot_data.quant_daily_review("2026-08-12", 0)

    assert result["date"] == "2026-08-11"
    assert result["requested_date"] == "2026-08-12"
    assert result["fallback"] is True
    assert result["data"] == [ready]
    assert "publish_status = 'ready'" in read_sql.call_args.args[0]
    assert read_sql.call_count == 1


def test_daily_review_dates_survives_either_table_missing():
    def read_rows(sql, params=None):
        if "st_quant_review_digest" in sql:
            raise RuntimeError("table does not exist")
        return [{"d": date(2026, 8, 11)}, {"d": "2026-08-10"}]

    with patch("server.api.routers.hot_data._read_sql", side_effect=read_rows):
        result = hot_data.daily_review_dates()

    assert result["dates"] == ["2026-08-11", "2026-08-10"]
    assert result["warnings"] == ["st_quant_review_digest: table does not exist"]


def test_daily_review_dates_merges_and_deduplicates_both_tables():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[
            [{"d": date(2026, 8, 12)}, {"d": date(2026, 8, 11)}],
            [{"d": "2026-08-11"}, {"d": "2026-08-05"}],
        ],
    ):
        result = hot_data.daily_review_dates()

    assert result == {"dates": ["2026-08-12", "2026-08-11", "2026-08-05"]}


def test_export_exact_blocked_returns_quality_without_legacy_lookup():
    blocked = _quant_row(status="blocked")
    with patch("server.api.routers.hot_data._read_sql", return_value=[blocked]) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["publish_status"] == "blocked"
    assert result["fallback"] is False
    assert result["error"] == "量化复盘未通过质量门禁"
    assert result["quality"]["errors"] == ["目标日有效行情覆盖率不足"]
    assert "text" not in result
    assert read_sql.call_count == 1


def test_export_ready_quant_is_preferred_and_marks_fallback():
    ready = _quant_row(review_date="2026-08-11")
    with patch("server.api.routers.hot_data._read_sql", return_value=[ready]) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["date"] == "2026-08-11"
    assert result["requested_date"] == "2026-08-12"
    assert result["fallback"] is True
    assert result["publish_status"] == "ready"
    assert result["text"] == "大势分析\n真实量化正文"
    assert read_sql.call_count == 1


def test_export_uses_legacy_only_when_quant_has_no_record():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[[], [{"pro_review": "旧版历史复盘"}]],
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-05")

    assert result == {"date": "2026-08-05", "text": "旧版历史复盘"}
    assert read_sql.call_count == 2


def test_export_does_not_hide_operational_quant_query_failure_with_legacy():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=RuntimeError("database connection lost"),
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["publish_status"] == "error"
    assert "database connection lost" in result["error"]
    assert read_sql.call_count == 1


def test_export_allows_legacy_when_quant_table_is_not_deployed():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[
            RuntimeError("Table 'probiga.st_quant_review_digest' doesn't exist"),
            [{"pro_review": "旧版历史复盘"}],
        ],
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-05")

    assert result == {"date": "2026-08-05", "text": "旧版历史复盘"}
    assert read_sql.call_count == 2


def test_review_page_contract_requests_quant_and_exports_markdown():
    source = (
        Path(__file__).resolve().parents[1] / "server" / "static" / "js" / "app.js"
    ).read_text(encoding="utf-8")

    assert "apiGet('/daily-review/quant?review_date=' + d)" in source
    assert "量化三段式为默认视图" in source
    assert "质量门禁未通过" in source
    assert "fetch('/api/hot-data/daily-review/export?review_date='" in source
    assert "text/markdown;charset=utf-8" in source
