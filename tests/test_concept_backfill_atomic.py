from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from biz.stock_market import sync_stock_market
from integrations.qmt.aggregate import aggregate_concept_current


def _concept_kline_frame(dates=("2026-08-07",)) -> pd.DataFrame:
    rows = []
    for code in ("C1", "C2"):
        for trade_date in dates:
            rows.append({
                "index_code": code,
                "trade_time": f"{trade_date} 15:00:00",
                "trade_date": trade_date,
                "k_type": 1,
                "open": 1000.0,
                "close": 1010.0,
                "high": 1020.0,
                "low": 990.0,
                "volume": 100.0,
                "amount": 1000.0,
                "change": 10.0,
                "change_pct": 1.0,
            })
    return pd.DataFrame(rows)


def test_bigqmt_current_without_pre_close_derives_reference_price() -> None:
    members = pd.DataFrame([{"concept_code": "C1", "stock_code": "000001"}])
    current = pd.DataFrame([{
        "stock_code": "000001",
        "snapshot_at": "2026-08-10 15:00:00",
        "open": 10.0,
        "price": 10.5,
        "high": 10.6,
        "low": 9.9,
        "change": 0.5,
        "change_pct": 5.0,
        "volume": 100.0,
        "amount": 1000.0,
    }])

    out = aggregate_concept_current(members, current)

    assert len(out) == 1
    assert out.iloc[0]["trade_date"] == "2026-08-10"
    assert round(float(out.iloc[0]["change_pct"]), 2) == 5.0


def test_bigqmt_concept_current_reuses_local_current_and_daily_ohlc(monkeypatch) -> None:
    members = pd.DataFrame([{"concept_code": "C1", "stock_code": "000001"}])
    current = pd.DataFrame([{
        "stock_code": "000001",
        "snapshot_at": "2026-08-10 15:00:00",
        "price": 10.5,
        "change": 0.5,
        "change_pct": 5.0,
        "volume": 100.0,
        "amount": 1000.0,
    }])
    daily = pd.DataFrame([{
        "stock_code": "000001",
        "open": 10.0,
        "high": 10.6,
        "low": 9.9,
        "pre_close": 10.0,
    }])
    reads = iter((current, daily))
    captured = {}
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "bigqmt")
    monkeypatch.setattr(sync_stock_market, "_read_qmt_concept_meta", lambda *_args: (members, {}))
    monkeypatch.setattr(sync_stock_market, "read_frame", lambda *_args, **_kwargs: next(reads).copy())
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: "history-engine")
    monkeypatch.setattr(
        "integrations.bigqmt.bridge.current",
        lambda *_args, **_kwargs: pytest.fail("BigQMT concept current must reuse local canonical rows"),
    )

    def replace(frame, table, engine, **_kwargs):
        captured.update(frame=frame, table=table, engine=engine)
        return len(frame)

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", replace)
    engine = object()
    sync_stock_market.step_concept_east_current(engine, ["C1"])

    assert captured["table"] == "sm_concept_east_current"
    assert captured["engine"] is engine
    assert len(captured["frame"]) == 1
    assert captured["frame"].iloc[0]["trade_date"] == "2026-08-10"


def test_concept_kline_source_requests_are_bounded_to_twenty_five(monkeypatch) -> None:
    calls = []

    class Bridge:
        @staticmethod
        def kline(codes, **_kwargs):
            calls.append(list(codes))
            return pd.DataFrame([{"stock_code": codes[0]}])

    monkeypatch.setenv("QMT_CONCEPT_KLINE_REQUEST_BATCH_SIZE", "25")
    out = sync_stock_market._fetch_concept_kline_batches(
        Bridge,
        [f"{index:06d}.SZ" for index in range(51)],
        start_date="2026-08-01",
        end_date="2026-08-07",
    )

    assert [len(batch) for batch in calls] == [25, 25, 1]
    assert len(out) == 3


def test_concept_kline_validation_drops_only_fully_unavailable_rows() -> None:
    frame = _concept_kline_frame(("2026-08-07",))
    unavailable = frame.iloc[[0]].copy()
    unavailable["trade_date"] = "2026-08-06"
    unavailable["trade_time"] = "2026-08-06 15:00:00"
    unavailable[["open", "close", "high", "low"]] = None

    out = sync_stock_market._validate_concept_kline_replacement(
        pd.concat([frame, unavailable], ignore_index=True),
        ["C1", "C2"],
        completed_end="2026-08-07",
    )

    assert len(out) == 2
    assert set(out["trade_date"]) == {"2026-08-07"}


def test_bigqmt_concept_list_is_full_by_default(monkeypatch) -> None:
    captured = {}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement):
            captured["sql"] = str(statement)
            return SimpleNamespace(fetchall=lambda: [("C1",), ("C2",)])

    engine = SimpleNamespace(connect=lambda: Connection())
    monkeypatch.delenv("SM_MAX_CONCEPTS", raising=False)
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "bigqmt")

    assert sync_stock_market.read_concept_east_codes(engine) == ["C1", "C2"]
    assert "LIKE 'BK%%'" not in captured["sql"]
    assert "LIMIT" not in captured["sql"]


def test_bigqmt_concept_kline_validates_before_atomic_window_replace(monkeypatch) -> None:
    members = pd.DataFrame([
        {"concept_code": "C1", "stock_code": "000001"},
        {"concept_code": "C2", "stock_code": "600000"},
    ])
    source = pd.DataFrame([{"stock_code": "000001", "trade_date": "2026-08-07"}])
    captured = {}

    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "bigqmt")
    monkeypatch.setattr(sync_stock_market, "_latest_completed_stock_trade_date", lambda *_args: "2026-08-07")
    monkeypatch.setattr(sync_stock_market, "_read_qmt_concept_meta", lambda *_args: (members, {}))
    monkeypatch.setattr(sync_stock_market, "read_frame", lambda *_args, **_kwargs: source)
    monkeypatch.setattr(
        "integrations.bigqmt.bridge.kline",
        lambda *_args, **_kwargs: pytest.fail("BigQMT concept K-line must reuse canonical local daily rows"),
    )
    monkeypatch.setattr(
        "integrations.qmt.aggregate.aggregate_concept_kline",
        lambda *_args: _concept_kline_frame(("2026-08-07", "2026-08-10")),
    )
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: "history-engine")
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: pytest.fail("concept K-line must not truncate first"),
    )

    def replace(frame, table, engine, **kwargs):
        captured.update(frame=frame, table=table, engine=engine, kwargs=kwargs)
        return len(frame)

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", replace)

    sync_stock_market.step_concept_east_kline(
        object(), ["C1", "C2"], "2026-08-01", "2026-08-10"
    )

    assert captured["table"] == "sm_concept_east_kline"
    assert captured["engine"] == "history-engine"
    assert set(captured["frame"]["trade_date"]) == {"2026-08-07"}
    assert captured["kwargs"]["params"]["end_date"] == "2026-08-07"
    assert "trade_date >= :start_date" in captured["kwargs"]["where_sql"]


def test_bigqmt_concept_kline_empty_source_preserves_previous_rows(monkeypatch) -> None:
    members = pd.DataFrame([{"concept_code": "C1", "stock_code": "000001"}])
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "bigqmt")
    monkeypatch.setattr(sync_stock_market, "_latest_completed_stock_trade_date", lambda *_args: "2026-08-07")
    monkeypatch.setattr(sync_stock_market, "_read_qmt_concept_meta", lambda *_args: (members, {}))
    monkeypatch.setattr(sync_stock_market, "read_frame", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        "integrations.bigqmt.bridge.kline",
        lambda *_args, **_kwargs: pytest.fail("BigQMT concept K-line must not call the history bridge"),
    )
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: "history-engine")
    monkeypatch.setattr(
        sync_stock_market,
        "replace_table_rows",
        lambda *_args, **_kwargs: pytest.fail("empty source must preserve old rows"),
    )

    with pytest.raises(RuntimeError, match="returned no stock rows"):
        sync_stock_market.step_concept_east_kline(object(), ["C1"], "2026-08-01", "2026-08-07")


def test_bigqmt_concept_flow_uses_completed_local_inputs_then_atomic_replace(monkeypatch) -> None:
    members = pd.DataFrame([
        {"concept_code": "C1", "stock_code": "000001"},
        {"concept_code": "C2", "stock_code": "600000"},
    ])
    dates = ["2026-08-06", "2026-08-07"]
    flow_rows = []
    kline_rows = []
    for trade_date in dates:
        for stock_code in ("000001", "600000"):
            flow_rows.append({
                "stock_code": stock_code,
                "trade_date": trade_date,
                "main_net_inflow": 10.0,
                "max_net_inflow": 5.0,
                "lg_net_inflow": 3.0,
                "mid_net_inflow": 1.0,
                "sm_net_inflow": -1.0,
            })
            kline_rows.append({
                "stock_code": stock_code,
                "trade_date": trade_date,
                "amount": 1000.0,
                "change_pct": 1.0,
            })
    frames = {
        "SELECT DISTINCT trade_date": pd.DataFrame({"trade_date": dates}),
        "FROM sm_stock_capital_flow_daily": pd.DataFrame(flow_rows),
        "FROM sm_stock_kline": pd.DataFrame(kline_rows),
    }

    def read_frame(statement, _engine, params=None):
        sql = str(statement)
        return next(frame.copy() for token, frame in frames.items() if token in sql)

    captured = {}
    monkeypatch.setattr(sync_stock_market, "_concept_source", lambda _kind: "bigqmt")
    monkeypatch.setattr(sync_stock_market, "read_concept_east_codes", lambda _engine: ["C1", "C2"])
    monkeypatch.setattr(sync_stock_market, "_read_qmt_concept_meta", lambda *_args: (members, {"C1": "One", "C2": "Two"}))
    monkeypatch.setattr(sync_stock_market, "_latest_completed_stock_trade_date", lambda *_args: "2026-08-07")
    monkeypatch.setattr(sync_stock_market, "read_frame", read_frame)
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: "history-engine")
    monkeypatch.setattr(sync_stock_market, "_load_stock_short_name_map", lambda _engine: {})
    monkeypatch.setattr("integrations.qmt.info.to_qmt_stock_symbols", lambda values: list(values))
    monkeypatch.setattr(
        "integrations.qmt.aggregate.aggregate_concept_kline",
        lambda *_args: _concept_kline_frame(tuple(dates)),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: pytest.fail("concept flow must not truncate first"),
    )

    def replace(frame, table, engine, **kwargs):
        captured.update(frame=frame, table=table, engine=engine)
        return len(frame)

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", replace)

    sync_stock_market.step_concept_flow_east(object())

    assert captured["table"] == "sm_concept_capital_flow_east"
    assert len(captured["frame"]) == 6
    assert set(captured["frame"]["days_type"]) == {1, 5, 10}
    assert set(captured["frame"]["index_code"]) == {"C1", "C2"}
