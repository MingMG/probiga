from __future__ import annotations

import pandas as pd

from integrations.qmt.aggregate import (
    aggregate_concept_current,
    aggregate_concept_kline,
    aggregate_concept_minute,
)
from integrations.qmt.sectors import _fetch_memberships, build_concept_catalog


def test_build_concept_catalog_prefers_tdgn_variant() -> None:
    df = build_concept_catalog(["GN5G", "TGN5G", "TDGN5G概念", "GN机器人"])
    codes = df.set_index("name")["concept_code"].to_dict()
    assert codes["5G概念"] == "TDGN5G概念"
    assert codes["机器人"] == "GN机器人"


def test_memberships_are_batched_and_non_a_share_symbols_are_filtered(monkeypatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setenv("QMT_SECTOR_MEMBER_BATCH_SIZE", "5")

    def fake_members(sectors, **_kwargs):
        calls.append(list(sectors))
        return pd.DataFrame(
            [
                {"sector_name": sectors[0], "stock_code": "000001", "qmt_code": "000001.SZ"},
                {"sector_name": sectors[0], "stock_code": "910001", "qmt_code": "910001.BJ"},
            ]
        )

    monkeypatch.setattr("integrations.qmt.sectors.bridge.sector_members_many", fake_members)

    out = _fetch_memberships([f"TGN{i}" for i in range(11)])

    assert [len(batch) for batch in calls] == [5, 5, 1]
    assert set(out["qmt_code"]) == {"000001.SZ"}


def test_aggregate_concept_current() -> None:
    members = pd.DataFrame(
        [
            {"concept_code": "C1", "stock_code": "000001"},
            {"concept_code": "C1", "stock_code": "000002"},
        ]
    )
    stock_current = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "pre_close": 10,
                "open": 10.1,
                "price": 10.5,
                "high": 10.6,
                "low": 9.9,
                "change_pct": 5.0,
                "volume": 100,
                "amount": 1000,
                "snapshot_at": "2026-06-24 10:00:00",
            },
            {
                "stock_code": "000002",
                "pre_close": 20,
                "open": 19.8,
                "price": 20.4,
                "high": 20.5,
                "low": 19.7,
                "change_pct": 2.0,
                "volume": 200,
                "amount": 4000,
                "snapshot_at": "2026-06-24 10:00:00",
            },
        ]
    )
    out = aggregate_concept_current(members, stock_current)
    row = out.iloc[0]
    assert row["index_code"] == "C1"
    assert round(float(row["change_pct"]), 2) == 2.60
    assert float(row["volume"]) == 300
    assert float(row["amount"]) == 5000


def test_aggregate_concept_minute() -> None:
    members = pd.DataFrame(
        [
            {"concept_code": "C1", "stock_code": "000001"},
            {"concept_code": "C1", "stock_code": "000002"},
        ]
    )
    stock_minute = pd.DataFrame(
        [
            {"stock_code": "000001", "trade_time": "2026-06-24 09:30:00", "change_pct": 1.0, "volume": 100, "amount": 1000},
            {"stock_code": "000002", "trade_time": "2026-06-24 09:30:00", "change_pct": 3.0, "volume": 200, "amount": 4000},
        ]
    )
    out = aggregate_concept_minute(members, stock_minute, snapshot_at=pd.Timestamp("2026-06-24 09:31:00"))
    row = out.iloc[0]
    assert row["index_code"] == "C1"
    assert round(float(row["change_pct"]), 2) == 2.60
    assert round(float(row["price"]), 2) == 1026.00


def test_aggregate_concept_kline() -> None:
    members = pd.DataFrame(
        [
            {"concept_code": "C1", "stock_code": "000001"},
            {"concept_code": "C1", "stock_code": "000002"},
        ]
    )
    stock_kline = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "trade_date": "2026-06-23",
                "pre_close": 10,
                "open": 10,
                "close": 10.5,
                "high": 10.7,
                "low": 9.9,
                "change_pct": 5.0,
                "volume": 100,
                "amount": 1000,
            },
            {
                "stock_code": "000002",
                "trade_date": "2026-06-23",
                "pre_close": 20,
                "open": 20,
                "close": 20.4,
                "high": 20.6,
                "low": 19.8,
                "change_pct": 2.0,
                "volume": 200,
                "amount": 4000,
            },
            {
                "stock_code": "000001",
                "trade_date": "2026-06-24",
                "pre_close": 10.5,
                "open": 10.4,
                "close": 10.2,
                "high": 10.6,
                "low": 10.1,
                "change_pct": -2.8571,
                "volume": 110,
                "amount": 1000,
            },
            {
                "stock_code": "000002",
                "trade_date": "2026-06-24",
                "pre_close": 20.4,
                "open": 20.5,
                "close": 20.808,
                "high": 21.0,
                "low": 20.3,
                "change_pct": 2.0,
                "volume": 220,
                "amount": 4000,
            },
        ]
    )
    out = aggregate_concept_kline(members, stock_kline)
    assert list(out["trade_date"]) == ["2026-06-23", "2026-06-24"]
    assert round(float(out.iloc[0]["change_pct"]), 2) == 2.60
    assert round(float(out.iloc[1]["change_pct"]), 2) == 1.03
