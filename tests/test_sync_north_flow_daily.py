from __future__ import annotations

from datetime import date

import pytest

from tools.sync_north_flow_daily import parse_ths_day_chart


def test_north_flow_close_snapshot_uses_final_ths_values():
    snapshot = parse_ths_day_chart(
        {
            "time": ["09:30", "15:00"],
            "hgt": [1.25, -9.28],
            "sgt": [-2.5, 379.75],
        },
        target_date="2026-10-28",
    )

    assert snapshot["daily"] == {
        "trade_date": date(2026, 10, 28),
        "net_hgt": -928_000_000,
        "net_sgt": 37_975_000_000,
        "net_tgt": 37_047_000_000,
    }
    assert snapshot["latest"]["trade_time"].strftime("%H:%M") == "15:00"
    assert len(snapshot["minute_rows"]) == 2


def test_north_flow_rejects_unfinished_session():
    with pytest.raises(ValueError, match="completed 15:00"):
        parse_ths_day_chart(
            {
                "time": ["09:30", "14:59"],
                "hgt": [1.0, 2.0],
                "sgt": [-1.0, -2.0],
            },
            target_date="2026-10-28",
        )


def test_north_flow_rejects_misaligned_provider_arrays():
    with pytest.raises(ValueError, match="misaligned"):
        parse_ths_day_chart(
            {"time": ["15:00"], "hgt": [], "sgt": [1.0]},
            target_date="2026-10-28",
        )


def test_north_flow_allows_trailing_shenzhen_window():
    snapshot = parse_ths_day_chart(
        {
            "time": ["09:30", "09:31", "15:00"],
            "hgt": [1.0, 2.0, -9.28],
            "sgt": [401.15, 379.75],
        },
        target_date="2026-10-28",
    )

    assert snapshot["daily"]["net_tgt"] == 37_047_000_000
    assert snapshot["minute_rows"] == []
