from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

from server.trading_v2 import public_quote_failover as quotes


@contextmanager
def _lock(*_args, **_kwargs):
    yield object()


def test_portfolio_quote_refresh_uses_only_two_source_watchlist() -> None:
    reconciled = {
        "rows": [{
            "stock_code": "000001",
            "short_name": "平安银行",
            "price": 10.2,
            "pre_close": 10.0,
            "change_pct": 2.0,
            "volume": 100.0,
            "amount": 200.0,
            "source_count": 2,
            "provider_mask": "sina,tencent",
            "price_deviation_pct": 0.0,
        }],
        "quality_status": "PASS",
        "expected_count": 1,
        "observed_count": 1,
        "coverage": 1.0,
        "provider_count": 2,
        "agreement_ratio": 1.0,
        "evidence": ["pass"],
    }
    with patch.object(quotes, "mysql_named_lock", _lock), patch.object(
        quotes,
        "_load_portfolio_universe",
        return_value=(["000001"], {"000001": "平安银行"}),
    ), patch.object(
        quotes,
        "fetch_provider_quotes",
        return_value=({"sina": {}, "tencent": {}}, {"sina": {}, "tencent": {}}),
    ) as fetch_mock, patch.object(
        quotes,
        "reconcile_provider_quotes",
        return_value=reconciled,
    ), patch.object(
        quotes,
        "_persist_portfolio_result",
        return_value="batch-1",
    ) as persist_mock:
        out = quotes.collect_portfolio_quote_refresh(
            object(),
            now=datetime(2026, 9, 2, 14, 30),
            config={"providers": ["eastmoney"]},
        )

    assert out["status"] == "success"
    assert out["observed_count"] == 1
    used_config = fetch_mock.call_args.kwargs["config"]
    assert used_config["providers"] == ["sina", "tencent"]
    assert used_config["minimum_sources_per_symbol"] == 2
    persist_mock.assert_called_once()


def test_portfolio_quote_refresh_does_not_publish_blocked_batch() -> None:
    blocked = {
        "rows": [],
        "quality_status": "BLOCK",
        "expected_count": 2,
        "observed_count": 0,
        "coverage": 0.0,
        "provider_count": 1,
        "agreement_ratio": 0.0,
        "evidence": ["blocked"],
    }
    with patch.object(quotes, "mysql_named_lock", _lock), patch.object(
        quotes,
        "_load_portfolio_universe",
        return_value=(["000001", "600000"], {}),
    ), patch.object(
        quotes,
        "fetch_provider_quotes",
        return_value=({}, {}),
    ), patch.object(
        quotes,
        "reconcile_provider_quotes",
        return_value=blocked,
    ), patch.object(
        quotes,
        "_persist_portfolio_result",
        return_value="batch-blocked",
    ) as persist_mock:
        out = quotes.collect_portfolio_quote_refresh(
            object(),
            now=datetime(2026, 9, 2, 14, 30),
            config={},
        )

    assert out["status"] == "blocked"
    assert persist_mock.call_args.kwargs["result"]["quality_status"] == "BLOCK"


def test_forced_after_close_refresh_accepts_only_same_day_close_quotes() -> None:
    reconciled = {
        "rows": [],
        "quality_status": "BLOCK",
        "expected_count": 1,
        "observed_count": 0,
        "coverage": 0.0,
        "provider_count": 0,
        "agreement_ratio": 0.0,
        "evidence": ["test"],
    }
    with patch.object(quotes, "mysql_named_lock", _lock), patch.object(
        quotes,
        "_load_portfolio_universe",
        return_value=(["000001"], {}),
    ), patch.object(
        quotes,
        "fetch_provider_quotes",
        return_value=({}, {}),
    ), patch.object(
        quotes,
        "reconcile_provider_quotes",
        return_value=reconciled,
    ) as reconcile_mock, patch.object(
        quotes,
        "_persist_portfolio_result",
        return_value="batch-close",
    ):
        quotes.collect_portfolio_quote_refresh(
            object(),
            now=datetime(2026, 9, 2, 18, 30),
            config={"maximum_source_age_seconds": 45},
            force=True,
        )

    used_config = reconcile_mock.call_args.kwargs["config"]
    assert used_config["minimum_source_time"] == datetime(2026, 9, 2, 15, 0)
    assert used_config["maximum_source_age_seconds"] >= 3 * 60 * 60 + 120
