from __future__ import annotations

import pandas as pd
import pytest

from biz.stock_market import sync_stock_market


def _concept_current_frame(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "index_code": code,
                "trade_time": "2026-08-10 14:30:00",
                "trade_date": "2026-08-10",
                "open": 100.0,
                "price": 101.0,
                "high": 102.0,
                "low": 99.0,
                "volume": 10.0,
                "amount": 1000.0,
                "change": 1.0,
                "change_pct": 1.0,
                "snapshot_at": "2026-08-10 14:30:01",
            }
        ]
    )


def test_east_concept_current_replaces_only_after_full_fetch(monkeypatch) -> None:
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "east")
    monkeypatch.setattr(sync_stock_market, "_concept_east_instance", lambda: object())
    monkeypatch.setattr(
        sync_stock_market,
        "_concurrent_run",
        lambda *_args, **_kwargs: [
            _concept_current_frame("BK001"),
            _concept_current_frame("BK002"),
        ],
    )
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: pytest.fail("current snapshot must not truncate first"),
    )
    captured: dict[str, object] = {}

    def fake_replace(frame, table, engine, **_kwargs):
        captured.update(frame=frame, table=table, engine=engine)
        return len(frame)

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", fake_replace)
    engine = object()

    sync_stock_market.step_concept_east_current(engine, ["BK001", "BK002"])

    assert captured["table"] == "sm_concept_east_current"
    assert captured["engine"] is engine
    assert set(captured["frame"]["index_code"]) == {"BK001", "BK002"}


def test_east_concept_current_preserves_previous_snapshot_on_empty_fetch(monkeypatch) -> None:
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "east")
    monkeypatch.setattr(sync_stock_market, "_concept_east_instance", lambda: object())
    monkeypatch.setattr(sync_stock_market, "_concurrent_run", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        sync_stock_market,
        "replace_table_rows",
        lambda *_args, **_kwargs: pytest.fail("empty fetch must preserve the previous snapshot"),
    )

    with pytest.raises(RuntimeError, match="preserving previous snapshot"):
        sync_stock_market.step_concept_east_current(object(), ["BK001"])
