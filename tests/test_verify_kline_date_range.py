import pandas as pd

from tools.verify_kline_date_range import (
    _match_reference,
    _sample_rows,
)


def test_range_verification_sample_is_deterministic_per_date():
    frame = pd.DataFrame([
        {
            "stock_code": code,
            "trade_date": trade_date,
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "amount": 1000,
        }
        for trade_date in ("2026-07-10", "2026-07-13")
        for code in ("000001", "000002", "600000")
    ])

    first = _sample_rows(frame, 2)
    second = _sample_rows(frame.sample(frac=1, random_state=7), 2)

    assert first == second
    assert len(first) == 4
    assert {row["trade_date"] for row in first} == {"2026-07-10", "2026-07-13"}


def test_bse_verification_uses_official_bulk_only_when_needed():
    primary = {
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "amount": 1_100_000,
    }
    reference = {
        **primary,
        "amount": 1_000_000,
    }

    matched, _differences, reconciled, compared_reference = _match_reference(
        primary,
        reference,
        bse_bulk_amount=100_000,
    )

    assert matched is True
    assert reconciled is True
    assert compared_reference["amount"] == primary["amount"]
