import pandas as pd
import pytest

from tools.rebuild_shsz_kline_range import (
    _derive_pre_close,
    _parse_tencent_history,
)


def test_parse_tencent_shsz_history_converts_hands_to_shares():
    payload = {
        "code": 0,
        "data": {
            "sz300780": {
                "day": [[
                    "2026-07-14", "29.090", "29.060",
                    "29.590", "27.500", "78888.490",
                ]],
            },
        },
    }

    frame = _parse_tencent_history(payload, "sz300780")

    assert frame.to_dict(orient="records") == [{
        "trade_date": "2026-07-14",
        "open": 29.09,
        "close": 29.06,
        "high": 29.59,
        "low": 27.5,
        "volume": 7888849.0,
    }]


def test_derive_pre_close_uses_adjusted_return_ratio():
    adjusted = [
        {"trade_date": "2026-07-13", "close": 20.0},
        {"trade_date": "2026-07-14", "close": 18.0},
    ]

    assert _derive_pre_close(
        9.0,
        "2026-07-14",
        adjusted,
    ) == pytest.approx(10.0)


def test_parse_tencent_adjusted_history_reads_qfqday():
    payload = {
        "code": 0,
        "data": {
            "sh600000": {
                "qfqday": [[
                    "2026-07-14", "10", "10.5", "10.6", "9.9", "100",
                ]],
            },
        },
    }

    frame = _parse_tencent_history(
        payload,
        "sh600000",
        adjusted=True,
    )

    assert isinstance(frame, pd.DataFrame)
    assert frame.iloc[0]["close"] == 10.5


def test_parse_tencent_star_market_history_keeps_share_volume():
    payload = {
        "code": 0,
        "data": {
            "sh688001": {
                "day": [[
                    "2026-07-23", "53.33", "50.32",
                    "55.28", "49.50", "9597887",
                ]],
            },
        },
    }

    frame = _parse_tencent_history(payload, "sh688001")

    assert frame.iloc[0]["volume"] == 9597887.0
