from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tools import sync_market_realtime


def _quotes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "short_name": "Ping An",
                "price": 10.0,
                "change": 0.1,
                "change_pct": 1.0,
                "volume": 100,
                "amount": 1000,
            }
        ]
    )


def test_subset_refresh_uses_sina_and_never_replaces_full_snapshot():
    engine = object()
    with patch.object(sync_market_realtime, "_load_short_name_map", return_value={}), patch.object(
        sync_market_realtime, "fetch_list_market_current", return_value=_quotes()
    ) as fetch, patch.object(
        sync_market_realtime, "_write_current_subset", return_value=1
    ) as write_subset, patch.object(
        sync_market_realtime, "save_to_mysql", return_value=0
    ):
        result = sync_market_realtime.sync_market_realtime(
            engine=engine,
            codes=["000001"],
            source="sina",
            archive_snapshot=False,
            skip_closed=False,
        )

    fetch.assert_called_once()
    write_subset.assert_called_once()
    assert result["source"] == "sina"
    assert result["coverage"] == 1.0


def test_full_market_auto_source_uses_atomic_eastmoney_snapshot():
    engine = object()
    with patch.object(sync_market_realtime, "refresh_snapshot", return_value=5500) as refresh, patch.object(
        sync_market_realtime, "fetch_list_market_current"
    ) as fetch:
        result = sync_market_realtime.sync_market_realtime(
            engine=engine,
            source="auto",
            archive_snapshot=True,
            skip_closed=False,
            min_coverage=0.70,
        )

    refresh.assert_called_once_with(engine, min_coverage=0.70, archive_snapshot=True)
    fetch.assert_not_called()
    assert result["source"] == "eastmoney"
    assert result["current_rows"] == 5500


def test_closed_market_skips_before_any_provider_call():
    engine = object()
    with patch.object(sync_market_realtime, "is_trading_time", return_value=False), patch.object(
        sync_market_realtime, "refresh_snapshot"
    ) as refresh:
        result = sync_market_realtime.sync_market_realtime(engine=engine, skip_closed=True)

    refresh.assert_not_called()
    assert result == {
        "status": "skipped",
        "reason": "market_closed",
        "generated_at": result["generated_at"],
    }
