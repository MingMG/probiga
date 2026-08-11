import inspect
from unittest.mock import patch

from tools import promote_qmt_local_history_to_business as promote


def test_derive_daily_ignores_zero_price_minute_bars():
    source = inspect.getsource(promote.derive_daily_from_business_minute)

    assert "price > 0" in source


def test_promote_main_uses_batch_engine():
    engine = object()

    with patch.object(
        promote.sys,
        "argv",
        [
            "promote_qmt_local_history_to_business.py",
            "--daily-dates",
            "2026-07-01",
            "--minute-dates",
            "2026-07-01",
            "--derive-daily-from-minute-dates",
            "2026-07-01",
        ],
    ), patch("tools.promote_qmt_local_history_to_business.load_project_env"), patch(
        "tools.promote_qmt_local_history_to_business.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.promote_qmt_local_history_to_business.promote_daily",
        return_value=[],
    ) as promote_daily, patch(
        "tools.promote_qmt_local_history_to_business.promote_minute",
        return_value=[],
    ) as promote_minute, patch(
        "tools.promote_qmt_local_history_to_business.derive_daily_from_business_minute",
        return_value=[],
    ) as derive_daily:
        assert promote.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    promote_daily.assert_called_once_with(engine, dates=["2026-07-01"], min_rows=4441)
    promote_minute.assert_called_once_with(
        engine,
        dates=["2026-07-01"],
        min_rows=1_070_425,
        stock_batch_size=200,
    )
    derive_daily.assert_called_once_with(
        engine,
        dates=["2026-07-01"],
        min_rows=1_070_425,
        complete_rows=1_250_000,
    )
