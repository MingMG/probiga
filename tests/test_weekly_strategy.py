from datetime import date, timedelta

from biz.review.weekly_strategy import build_weekly_strategy


def test_exact_week_preserves_gaps_and_calculates_only_supported_comparisons():
    monday = date(2026, 11, 9)
    base = date(2026, 11, 6)
    friday = date(2026, 11, 13)
    calendar = [
        {"trade_date": monday + timedelta(days=offset), "trade_status": offset in {-3, 0, 4}}
        for offset in range(-7, 7)
    ]

    def digest(day, amount):
        return {
            "review_date": day, "publish_status": "ready",
            "market_structure_json": {
                "total_amount": amount,
                "amount_quality": {"status": "OBSERVED" if amount else "UNKNOWN"},
                "decision_facts": {"breadth": {"evidence_type": "OBSERVED", "up_count": 765}},
            },
        }

    options = {
        "week_start": monday,
        "calendar": calendar,
        "digests": [digest(base, 100), digest(monday, None), digest(friday, 110)],
        "indices": [
            {"index_code": "000688", "trade_date": base, "close": 100},
            {"index_code": "000688", "trade_date": friday, "close": 102},
            {"index_code": "000852", "trade_date": base, "close": 100},
            {"index_code": "000852", "trade_date": friday, "close": 98},
            {"index_code": "000852", "trade_date": friday, "close": 98.1},
        ],
        "margins": [],
        "source_errors": {},
    }
    report = build_weekly_strategy(**options)
    assert report["baseline_date"] == "2026-11-06"
    assert report["coverage"]["turnover_days"] == 1
    assert report["turnover"] == {
        "daily_mean_cny": None, "previous_daily_mean_cny": 100, "change_pct": None,
    }
    assert report["days"][0]["amount_cny"] is None
    assert report["days"][1]["amount_change_pct"] is None
    assert report["days"][1]["breadth"]["up_count"] == 765
    assert report["index_paths"][1]["week_return_pct"] == 2
    assert report["index_paths"][1]["missing_dates"] == ["2026-11-09"]
    assert report["index_paths"][4]["week_return_pct"] is None
    assert report["snapshot_sha256"] == build_weekly_strategy(**options)["snapshot_sha256"]

    later = options["digests"][-1]
    later["data_cutoff_at"] = "2026-11-16T00:17:50"
    later["market_structure_json"].update(
        previous_total_amount=90,
        previous_amount_quality={"status": "OBSERVED", "trade_date": base.isoformat()},
    )
    assert build_weekly_strategy(**options)["days"][0]["amount_cny"] is None
    later["market_structure_json"]["previous_amount_quality"]["trade_date"] = monday.isoformat()
    report = build_weekly_strategy(**options)
    assert report["days"][0]["amount_cny"] == 90
    assert report["days"][0]["amount_evidence_date"] == friday.isoformat()
    assert report["days"][0]["amount_evidence_cutoff_at"] == later["data_cutoff_at"]
    assert report["turnover"]["daily_mean_cny"] == 100
