from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from integrations.bigqmt.backend import BigQmtBackend
from integrations.bigqmt import bridge
from integrations.bigqmt.spool import PROVIDER_ID
from tools import run_big_qmt_bridge


def _tick(price: float, timestamp: int) -> dict:
    return {
        "time": timestamp,
        "lastPrice": price,
        "lastClose": 10.0,
        "volume": 100,
        "amount": 1000,
    }


def _level1_tick(price: float, source_at: datetime, received_at: datetime) -> dict:
    return {
        **_tick(price, int(source_at.timestamp() * 1000)),
        "bidPrice": [price - 0.01],
        "askPrice": [price + 0.01],
        "bidVol": [1000],
        "askVol": [1200],
        "_probiga_received_at": received_at.isoformat(sep=" ", timespec="seconds"),
    }


def test_backend_merges_full_and_tracked_with_tracked_winning(monkeypatch):
    payloads = {
        "full": {"quotes": {"000001.SZ": _tick(10.1, 1784602800000), "600000.SH": _tick(9.0, 1784602800000)}},
        "tracked": {"quotes": {"000001.SZ": _tick(10.5, 1784602805000)}},
    }

    monkeypatch.setattr(
        "integrations.bigqmt.backend.read_snapshot",
        lambda kind, **_kwargs: payloads[kind],
    )
    frame = BigQmtBackend().fetch_current(["000001"])

    assert len(frame) == 1
    assert frame.iloc[0]["stock_code"] == "000001"
    assert frame.iloc[0]["price"] == 10.5


def test_registry_exposes_bigqmt_backend(monkeypatch):
    from integrations.registry import get_backend, resolve_source

    monkeypatch.setenv("DATA_SOURCE_CURRENT", "big_qmt")
    assert resolve_source("current") == "bigqmt"
    assert isinstance(get_backend("current"), BigQmtBackend)


def test_level1_snapshot_accepts_only_fresh_subscription_callback(monkeypatch, tmp_path):
    now = datetime(2026, 7, 27, 10, 0, 10)
    payload = {
        "generated_ts": now.timestamp(),
        "batch_id": "tracked-live-1",
        "quotes": {
            "000001.SZ": _level1_tick(
                10.5,
                datetime(2026, 7, 27, 10, 0, 8),
                datetime(2026, 7, 27, 10, 0, 9),
            ),
            # A cached initial get_full_tick has no callback receipt marker.
            "600000.SH": _tick(9.0, int(now.timestamp() * 1000)),
        },
    }
    heartbeat = {
        "status": "running",
        "updated_ts": now.timestamp(),
        "pid": 123,
        "subscription_id": 7,
    }
    monkeypatch.setattr(bridge, "bridge_paths", lambda _home=None: {"heartbeat": tmp_path / "heartbeat.json"})
    monkeypatch.setattr(bridge, "read_json", lambda _path: heartbeat)
    monkeypatch.setattr(bridge, "read_snapshot", lambda *_args, **_kwargs: payload)

    frame, receipt = bridge.level1_snapshot(now=now)

    assert receipt["status"] == "PASS"
    assert receipt["capture_mode"] == "LIVE_CALLBACK"
    assert receipt["live_rows"] == 1
    assert frame["stock_code"].tolist() == ["000001"]


def test_level1_snapshot_rejects_historical_initial_quote(monkeypatch, tmp_path):
    now = datetime(2026, 7, 27, 10, 0, 10)
    payload = {
        "generated_ts": now.timestamp(),
        "quotes": {
            "000001.SZ": _level1_tick(
                10.5,
                datetime(2026, 7, 24, 15, 0),
                now,
            ),
        },
    }
    heartbeat = {
        "status": "running",
        "updated_ts": now.timestamp(),
        "subscription_id": 7,
    }
    monkeypatch.setattr(bridge, "bridge_paths", lambda _home=None: {"heartbeat": tmp_path / "heartbeat.json"})
    monkeypatch.setattr(bridge, "read_json", lambda _path: heartbeat)
    monkeypatch.setattr(bridge, "read_snapshot", lambda *_args, **_kwargs: payload)

    frame, receipt = bridge.level1_snapshot(now=now)

    assert frame.empty
    assert receipt["status"] == "BLOCK"
    assert receipt["reason"] == "no_fresh_live_callback"


def test_level1_reconnect_touches_watchlist_without_changing_content(tmp_path):
    root = tmp_path / "userdata" / "probiga_bridge"
    root.mkdir(parents=True)
    watchlist = root / "watchlist.json"
    content = '{"tracked_codes":["000001.SZ"]}'
    watchlist.write_text(content, encoding="utf-8")
    os_time = 1_700_000_000
    import os

    os.utime(watchlist, (os_time, os_time))
    before = watchlist.stat().st_mtime_ns

    result = bridge.request_level1_reconnect(qmt_home=tmp_path)

    assert result["status"] == "requested"
    assert watchlist.read_text(encoding="utf-8") == content
    assert watchlist.stat().st_mtime_ns > before


def test_backend_exposes_verified_live_level1(monkeypatch):
    now = datetime(2026, 7, 27, 10, 0, 10)
    frame = pd.DataFrame(
        [{
            "stock_code": "000001",
            "qmt_code": "000001.SZ",
            "source_time": now,
            "received_at": now,
            "bid1": 10.49,
            "ask1": 10.51,
        }]
    )
    receipt = {"status": "PASS", "receipt_id": "receipt-1"}
    monkeypatch.setattr(bridge, "level1_snapshot", lambda *_args, **_kwargs: (frame, receipt))

    result = BigQmtBackend().fetch_level1(["000001"], now=now)

    assert result.iloc[0]["quality_status"] == "VERIFIED_LIVE"
    assert result.iloc[0]["data_version"] == "bigqmt_live_level1_v1"
    assert result.attrs["level1_receipt"]["receipt_id"] == "receipt-1"


def test_history_frames_keep_standard_qmt_provenance(monkeypatch):
    monkeypatch.setattr(
        "integrations.bigqmt.backend.bridge.minute",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{
                "qmt_code": "000001.SZ",
                "stock_code": "000001",
                "trade_time": "2026-07-21 09:30:00",
                "trade_date": "2026-07-21",
                "price": 10.5,
                "avg_price": 10.4,
                "change": 0.1,
                "change_pct": 0.96,
                "volume": 100,
                "amount": 1050,
            }]
        ),
    )

    frame = BigQmtBackend().fetch_minute(["000001"], "2026-07-21")

    assert frame.iloc[0]["data_source"] == PROVIDER_ID
    assert frame.iloc[0]["qmt_code"] == "000001.SZ"
    assert frame.iloc[0]["data_version"] == "bigqmt_inner_v2"


def test_minute_request_expands_bare_dates_to_intraday_bounds(monkeypatch):
    captured = {}

    def fake_call(action, **kwargs):
        captured["action"] = action
        captured.update(kwargs)
        return {"rows": []}

    monkeypatch.setattr(bridge, "_call", fake_call)

    bridge.minute(
        ["000001.SZ"],
        trade_date="2026-07-27",
        start_date="2026-07-27",
        end_date="2026-07-27",
        count=20,
    )

    assert captured["action"] == "minute"
    assert captured["start_date"] == "2026-07-27 00:00:00"
    assert captured["end_date"] == "2026-07-27 23:59:59"


def test_daily_history_normalizes_qmt_lots_to_shares(monkeypatch):
    monkeypatch.setattr(
        "integrations.bigqmt.backend.bridge.kline",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{
                "qmt_code": "510300.SH",
                "stock_code": "510300",
                "trade_time": "2026-07-21 15:00:00",
                "trade_date": "2026-07-21",
                "open": 4.1,
                "close": 4.2,
                "high": 4.3,
                "low": 4.0,
                "volume": 1234,
                "amount": 518280,
                "change": 0.1,
                "change_pct": 2.44,
                "pre_close": 4.1,
            }]
        ),
    )

    frame = BigQmtBackend().fetch_kline(
        ["510300"], "2026-07-21", "2026-07-21"
    )

    assert frame.iloc[0]["volume"] == 123400
    assert frame.iloc[0]["data_source"] == PROVIDER_ID


def test_refresh_watchlist_prioritizes_production_portfolio(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(run_big_qmt_bridge, "_read_universe", lambda _engine: (["000001"], {"000001": "Ping An"}))
    monkeypatch.setattr(run_big_qmt_bridge, "_read_remote_portfolio_codes", lambda _limit: ["600522", "002284"])
    monkeypatch.setattr(run_big_qmt_bridge, "_read_tracked_codes", lambda _engine, _limit: ["000001", "600522"])
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "write_watchlist",
        lambda **kwargs: captured.update(kwargs) or tmp_path / "watchlist.json",
    )

    result = run_big_qmt_bridge.refresh_watchlist(object(), qmt_home=tmp_path, tracked_limit=3)

    assert captured["tracked_codes"] == ["600522", "002284", "000001"]
    assert result["remote_portfolio"] == ["600522", "002284"]


def test_realtime_universe_uses_latest_tradable_daily_pool() -> None:
    normalized = " ".join(run_big_qmt_bridge.ACTIVE_UNIVERSE_SQL.split()).lower()
    assert "join sm_stock_kline" in normalized
    assert "max(latest.trade_date)" in normalized
    assert "latest.adjust_type = 0" in normalized
    assert "latest.k_type = 1" in normalized


def test_remote_portfolio_codes_survive_temporary_endpoint_failure(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({"data": [{"stock_code": "600522"}, {"stock_code": "2284"}]}).encode()

    run_big_qmt_bridge._last_remote_portfolio_codes = []
    monkeypatch.setattr(run_big_qmt_bridge.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    assert run_big_qmt_bridge._read_remote_portfolio_codes(280) == ["600522", "002284"]

    def fail(*_args, **_kwargs):
        raise OSError("temporary production outage")

    monkeypatch.setattr(run_big_qmt_bridge.urllib.request, "urlopen", fail)
    assert run_big_qmt_bridge._read_remote_portfolio_codes(280) == ["600522", "002284"]


def test_snapshot_freshness_is_required_only_during_trading_session() -> None:
    saturday = datetime(2026, 7, 25, 10, 30)
    monday_open = datetime(2026, 7, 27, 10, 30)
    monday_after_close = datetime(2026, 7, 27, 16, 0)

    assert not run_big_qmt_bridge._is_live_market_window(
        saturday,
        is_trade_day=False,
    )
    assert run_big_qmt_bridge._is_live_market_window(
        monday_open,
        is_trade_day=True,
    )
    assert not run_big_qmt_bridge._is_live_market_window(
        monday_after_close,
        is_trade_day=True,
    )


def test_off_session_snapshot_refresh_does_not_persist_level1_events(
    monkeypatch,
    tmp_path,
) -> None:
    tracked_frame = pd.DataFrame(
        [{
            "stock_code": "000001",
            "price": 10.5,
            "snapshot_at": datetime(2026, 7, 24, 15, 0),
            "etl_sync_at": datetime(2026, 7, 26, 8, 0),
        }]
    )
    monkeypatch.setattr(run_big_qmt_bridge, "_snapshot_freshness_required", lambda _engine: False)
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_read_snapshot_if_changed",
        lambda kind, **_kwargs: (
            ({}, "full-file")
            if kind == "full"
            else ({"generated_ts": "tracked-1"}, "tracked-file")
        ),
    )
    monkeypatch.setattr(run_big_qmt_bridge, "snapshot_frame", lambda *_args, **_kwargs: tracked_frame)
    monkeypatch.setattr(run_big_qmt_bridge, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "persist_quote_events",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not persist off-session")),
    )
    monkeypatch.setattr(run_big_qmt_bridge, "_replace_tracked_subset", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(run_big_qmt_bridge, "read_json", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(run_big_qmt_bridge, "_write_status", lambda *_args, **_kwargs: None)

    result = run_big_qmt_bridge.ingest_once(
        object(),
        qmt_home=tmp_path,
        universe=["000001"],
        tracked=["000001"],
        short_name_map={"000001": "Ping An"},
    )

    assert result["status"] == "idle_market_closed"
    assert result["quote_events_skipped_off_session"] == 1
    assert "quote_events_inserted" not in result


def test_live_snapshot_persists_only_callback_attested_level1_rows(
    monkeypatch,
    tmp_path,
) -> None:
    cached_frame = pd.DataFrame([
        {"stock_code": "000001", "price": 10.5},
        {"stock_code": "000002", "price": 11.5},
    ])
    live_frame = pd.DataFrame([
        {"stock_code": "000001", "price": 10.5},
    ])
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_snapshot_freshness_required",
        lambda _engine: True,
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_read_snapshot_if_changed",
        lambda kind, **_kwargs: (
            ({}, "full-file")
            if kind == "full"
            else ({"generated_ts": "tracked-live"}, "tracked-file")
        ),
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "snapshot_frame",
        lambda *_args, **_kwargs: cached_frame,
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "level1_snapshot",
        lambda *_args, **_kwargs: (
            live_frame,
            {"status": "PASS", "reason": "live_callback_verified"},
        ),
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_table_exists",
        lambda *_args: True,
    )
    captured = []
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "persist_quote_events",
        lambda _engine, rows: (
            captured.extend(rows)
            or {"received": len(rows), "inserted": len(rows)}
        ),
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_replace_tracked_subset",
        lambda *_args, **_kwargs: 2,
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "read_json",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_write_status",
        lambda *_args, **_kwargs: None,
    )

    result = run_big_qmt_bridge.ingest_once(
        object(),
        qmt_home=tmp_path,
        universe=["000001", "000002"],
        tracked=["000001", "000002"],
        short_name_map={},
    )

    assert [row["stock_code"] for row in captured] == ["000001"]
    assert result["quote_events_received"] == 1
    assert result["level1_receipt"]["status"] == "PASS"


def test_windows_bridge_owns_due_membership_snapshot(monkeypatch) -> None:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 1
    task = {
        "id": 65,
        "enabled": 1,
        "cron_time": "15:12",
        "task_type": "qmt_membership_snapshot",
    }
    monkeypatch.setattr(run_big_qmt_bridge, "_membership_snapshot_task", lambda _engine: task)
    monkeypatch.setattr(run_big_qmt_bridge, "_membership_snapshot_exists", lambda *_args: False)
    monkeypatch.setattr(run_big_qmt_bridge, "claim_scheduler_task_run", lambda *_args: True)
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "_run_membership_snapshot",
        lambda *_args: {
            "counts": {"concept_members": 44408},
            "snapshot": {"status": "created"},
        },
    )
    updates = []
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "update_scheduler_task",
        lambda *args, **kwargs: updates.append((args, kwargs)),
    )

    with patch(
        "tools.sync_bigqmt_reference.resolve_snapshot_date",
        return_value=datetime(2026, 7, 27).date(),
    ):
        result = run_big_qmt_bridge.maybe_sync_membership_snapshot(
            engine,
            now=datetime(2026, 7, 27, 15, 13),
        )

    assert result["status"] == "success"
    assert result["snapshot_date"] == "2026-07-27"
    assert updates
    assert updates[-1][0][2]["last_run_status"] == "success"


def test_explicit_off_session_refresh_does_not_persist_level1_events(
    monkeypatch,
    tmp_path,
) -> None:
    frame = pd.DataFrame(
        [{
            "stock_code": "000001",
            "price": 10.5,
            "snapshot_at": datetime(2026, 7, 24, 15, 0),
            "etl_sync_at": datetime(2026, 7, 26, 8, 0),
        }]
    )
    monkeypatch.setattr(run_big_qmt_bridge, "resolve_big_qmt_home", lambda **_kwargs: tmp_path)
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "refresh_watchlist",
        lambda *_args, **_kwargs: {
            "universe": ["000001"],
            "tracked": ["000001"],
            "short_name_map": {"000001": "Ping An"},
        },
    )
    monkeypatch.setattr(run_big_qmt_bridge, "_snapshot_freshness_required", lambda _engine: False)
    monkeypatch.setattr(run_big_qmt_bridge, "read_snapshot", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(run_big_qmt_bridge, "snapshot_frame", lambda *_args, **_kwargs: frame)
    monkeypatch.setattr(run_big_qmt_bridge, "merge_snapshot_frames", lambda *_args: frame)
    monkeypatch.setattr(run_big_qmt_bridge, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "persist_quote_events",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not persist off-session")),
    )
    monkeypatch.setattr(run_big_qmt_bridge, "_replace_tracked_subset", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(run_big_qmt_bridge, "_write_status", lambda *_args, **_kwargs: None)

    result = run_big_qmt_bridge.sync_big_qmt_realtime(
        engine=object(),
        codes=["000001"],
    )

    assert result["market_session"] == "off_session"
    assert result["quote_events_skipped_off_session"] == 1
