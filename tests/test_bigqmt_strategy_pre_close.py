from datetime import datetime

from integrations.bigqmt.qmt_strategy import probiga_big_qmt_bridge as producer


def test_bigqmt_strategy_uses_only_native_pre_close():
    rows = producer._bar_rows(
        {
            "000001.SZ": [
                {
                    "time": "2026-08-20 15:00:00",
                    "open": 10,
                    "close": 10,
                    "high": 10,
                    "low": 10,
                    "volume": 1,
                    "amount": 1000,
                    "preClose": 9.8,
                },
                {
                    "time": "2026-08-21 15:00:00",
                    "open": 5,
                    "close": 5,
                    "high": 5,
                    "low": 5,
                    "volume": 1,
                    "amount": 500,
                },
            ]
        },
        "1d",
    )

    assert rows[0]["pre_close"] == 9.8
    assert rows[0]["pre_close_origin"] == "NATIVE_QMT"
    assert rows[1]["pre_close"] is None
    assert rows[1]["pre_close_origin"] == "MISSING_NATIVE_QMT"
    assert rows[1]["pre_close"] != rows[0]["close"]


def test_daily_bar_time_is_normalized_to_market_close_for_every_qmt_shape():
    midnight_ms = int(datetime(2026, 8, 21).timestamp() * 1000)
    assert producer._time_text(midnight_ms, "1d") == "2026-08-21 15:00:00"
    assert producer._time_text("20260821000000", "1d") == (
        "2026-08-21 15:00:00"
    )
    assert producer._time_text("2026-08-21 00:00:00", "1d") == (
        "2026-08-21 15:00:00"
    )
    assert producer._time_text("20260821103105", "1m") == (
        "2026-08-21 10:31:05"
    )


def test_daily_reader_never_requests_synthetic_filled_bars():
    class Context:
        def __init__(self):
            self.calls = []

        def get_market_data_ex_ori(self, *args, **kwargs):
            self.calls.append(kwargs)
            return {}

    context = Context()
    params = {
        "stock_codes": ["000001.SZ"],
        "start_date": "2026-08-21",
        "end_date": "2026-08-21",
    }
    assert producer._market_rows(context, params, "1d") == []
    assert context.calls[-1]["fill_data"] is False
    assert producer._market_rows(context, params, "1m") == []
    assert context.calls[-1]["fill_data"] is True
