from __future__ import annotations

import json

import pandas as pd
import pytest

from tools.sync_etf_history import (
    ETF_META_UPSERT,
    ETFMeta,
    _data_version,
    _parse_ths_history_payload,
    cross_validate_raw,
    exchange_for_code,
    fetch_external_validation_histories,
    prepare_records,
)


def _frame(close_shift: float = 0.0, volume_shift: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-07-01", "2026-07-02"]),
            "open": [3.101 + close_shift, 3.111 + close_shift],
            "high": [3.121 + close_shift, 3.131 + close_shift],
            "low": [3.091 + close_shift, 3.101 + close_shift],
            "close": [3.111 + close_shift, 3.121 + close_shift],
            "volume": [1_000_000 + volume_shift, 2_000_000 + volume_shift],
            "amount": [3_110_000.0, 6_240_000.0],
        }
    )


def test_exchange_and_sina_symbol() -> None:
    assert exchange_for_code("510300") == "sh"
    assert exchange_for_code("159915") == "sz"
    assert ETFMeta("510300", "沪深300ETF", "A股宽基").sina_symbol == "sh510300"
    with pytest.raises(ValueError):
        exchange_for_code("000001")


def test_incremental_metadata_upsert_preserves_full_date_bounds() -> None:
    normalized = " ".join(ETF_META_UPSERT.split())
    assert "LEAST(`list_date`, VALUES(`list_date`))" in normalized
    assert "GREATEST(`last_trade_date`, VALUES(`last_trade_date`))" in normalized


def test_parse_ths_history_payload() -> None:
    payload = {
        "total": 2,
        "data": (
            "20260701,3.101,3.121,3.091,3.111,1000000,3110000;"
            "20260702,3.111,3.131,3.101,3.121,2000000,6240000"
        ),
    }
    raw = f"callback({json.dumps(payload)})"
    frame = _parse_ths_history_payload(
        raw, "510300", "2026-07-01", "2026-07-02"
    )
    assert len(frame) == 2
    assert frame.iloc[1]["pre_close"] == pytest.approx(3.111)
    assert frame.iloc[1]["change"] == pytest.approx(0.010)


def test_cross_validation_accepts_sina_cent_rounding() -> None:
    primary = _frame()
    secondary = _frame()
    for column in ("open", "high", "low", "close"):
        secondary[column] = secondary[column].round(2)
    validation, summary = cross_validate_raw(primary, secondary)
    assert validation["validation_passed"].all()
    assert summary["pass_ratio"] == 1.0


def test_cross_validation_rejects_material_price_error() -> None:
    primary = _frame()
    secondary = _frame(close_shift=0.08)
    with pytest.raises(ValueError, match="cross-source validation failed"):
        cross_validate_raw(primary, secondary)


def test_cross_validation_uses_tertiary_for_bad_sina_volume() -> None:
    primary = _frame()
    secondary = _frame()
    secondary.loc[1, "volume"] = 0.0
    tertiary = _frame()
    validation, summary = cross_validate_raw(primary, secondary, tertiary)
    assert validation["validation_passed"].all()
    assert validation.iloc[1]["validation_source"] == "tencent"
    assert summary["tencent_fallback_rows"] == 1


def test_cross_validation_accepts_qmt_sub_lot_volume_rounding() -> None:
    primary = _frame()
    secondary = _frame()
    secondary.loc[0, "volume"] = primary.loc[0, "volume"] + 99
    validation, summary = cross_validate_raw(primary, secondary)
    assert validation["validation_passed"].all()
    assert summary["pass_ratio"] == 1.0


def test_external_validation_uses_tencent_when_sina_is_late(
    monkeypatch,
) -> None:
    meta = ETFMeta("510300", "沪深300ETF", "A股宽基")
    ths = _frame(close_shift=0.08)
    tencent = _frame()
    monkeypatch.setattr(
        "tools.sync_etf_history.fetch_ths_history",
        lambda *_args, **_kwargs: ths,
    )
    monkeypatch.setattr(
        "tools.sync_etf_history.fetch_sina_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("Sina returned no history")
        ),
    )
    monkeypatch.setattr(
        "tools.sync_etf_history.fetch_tencent_history",
        lambda *_args, **_kwargs: tencent,
    )

    secondary, tertiary, errors = fetch_external_validation_histories(
        meta,
        "2026-07-01",
        "2026-07-02",
    )

    assert secondary[0] == "10jqka"
    assert secondary[1] is ths
    assert tertiary is not None
    assert tertiary[0] == "tencent"
    assert tertiary[1] is tencent
    assert "sina" in errors


def test_prepare_records_excludes_unmatched_dates_and_versions_are_stable() -> None:
    frame = _frame()
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["change_pct"] = frame["change"] / frame["pre_close"] * 100
    validation = pd.DataFrame(
        {
            "trade_date": frame["trade_date"],
            "price_max_delta": [0.001, None],
            "volume_delta_pct": [0.0, None],
            "validation_passed": [True, False],
            "validation_source": ["sina", "sina"],
        }
    )
    records = prepare_records(
        ETFMeta("510300", "沪深300ETF", "A股宽基"),
        frame,
        validation,
        adjust_type=0,
        batch_id="test",
        now=pd.Timestamp("2026-07-24 12:00:00").to_pydatetime(),
    )
    assert len(records) == 1
    assert records[0]["validation_status"] == "passed"
    assert records[0]["data_version"] == _data_version(records[0])
