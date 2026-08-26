from __future__ import annotations

import json
import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from integrations.bigqmt.backend import BigQmtBackend
from integrations.bigqmt import bridge
from integrations.bigqmt.release_identity import render_strategy_artifact
from integrations.bigqmt.spool import PROVIDER_ID
from integrations.qmt import bridge as qmt_bridge
from tools import run_big_qmt_bridge


def _tick(price: float, timestamp: int) -> dict:
    return {
        "time": timestamp,
        "lastPrice": price,
        "lastClose": 10.0,
        "volume": 100,
        "amount": 1000,
    }


def test_exact_build_strategy_installer_hash_verifies_all_qmt_aliases(
    monkeypatch, tmp_path,
):
    qmt_home = tmp_path / "QMT"
    expected_sha = "a" * 40
    source_bytes = Path(run_big_qmt_bridge.STRATEGY_SOURCE_PATH).read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    monkeypatch.setattr(
        run_big_qmt_bridge,
        "git_strategy_artifact",
        lambda **_kwargs: {
            "build_sha": expected_sha,
            "git_blob": "b" * 40,
            "source_bytes": source_bytes,
            "source_sha256": source_hash,
            "repository_path": (
                "integrations/bigqmt/qmt_strategy/"
                "probiga_big_qmt_bridge.py"
            ),
        },
    )
    rendered = render_strategy_artifact(
        source_bytes,
        build_sha=expected_sha,
        git_blob="b" * 40,
        source_sha256=source_hash,
    )

    result = run_big_qmt_bridge.install_strategy_release(
        qmt_home=qmt_home,
        expected_build_sha=expected_sha,
        git_head=expected_sha,
    )

    assert result["status"] == "installed"
    assert result["build_sha"] == expected_sha
    assert result["strategy_git_blob"] == "b" * 40
    assert result["strategy_source_sha256"] == source_hash
    assert result["database_writes"] is False
    assert result["automatic_order_submission"] is False
    assert result["installed_paths"]
    assert set(result["installed_hashes"].values()) == {
        rendered["artifact_sha256"]
    }
    assert result["strategy_artifact_sha256"] == rendered["artifact_sha256"]
    assert result["strategy_loaded_identity_sha256"] == rendered[
        "identity_sha256"
    ]
    manifest = json.loads(
        Path(result["strategy_release_manifest"]).read_text(encoding="utf-8")
    )
    assert manifest["strategy_build_sha"] == expected_sha
    assert manifest["strategy_git_blob"] == "b" * 40
    assert manifest["strategy_source_sha256"] == source_hash
    assert manifest["strategy_artifact_sha256"] == rendered[
        "artifact_sha256"
    ]


def test_strategy_installer_rejects_checkout_drift_before_copy(tmp_path):
    qmt_home = tmp_path / "QMT"
    with pytest.raises(RuntimeError, match="checkout differs"):
        run_big_qmt_bridge.install_strategy_release(
            qmt_home=qmt_home,
            expected_build_sha="a" * 40,
            git_head="b" * 40,
        )
    assert not (qmt_home / "python").exists()


def _level1_tick(price: float, source_at: datetime, received_at: datetime) -> dict:
    return {
        **_tick(price, int(source_at.timestamp() * 1000)),
        "bidPrice": [price - 0.01],
        "askPrice": [price + 0.01],
        "bidVol": [1000],
        "askVol": [1200],
        "_probiga_received_at": received_at.isoformat(sep=" ", timespec="seconds"),
    }


def test_remote_qmt_gateway_does_not_require_local_windows_python(
    monkeypatch,
    tmp_path,
):
    missing_python = tmp_path / "missing" / "python.exe"
    expected = {"ok": True, "rows": [{"stock_code": "000001"}]}
    monkeypatch.setattr(qmt_bridge, "python_path", lambda: missing_python)
    monkeypatch.setattr(
        qmt_bridge,
        "_run_gateway",
        lambda _payload, *, timeout: expected,
    )
    popen = MagicMock()
    monkeypatch.setattr(qmt_bridge.subprocess, "run", popen)

    assert qmt_bridge._run({"action": "probe"}, timeout=5) == expected
    popen.assert_not_called()


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
        "integrations.bigqmt.backend.bridge.minute_capture",
        lambda *_args, **_kwargs: {"rows": [{
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
                "pre_close": 10.4,
            }]},
    )

    frame = BigQmtBackend().fetch_minute(["000001"], "2026-07-21")

    assert frame.iloc[0]["data_source"] == PROVIDER_ID
    assert frame.iloc[0]["qmt_code"] == "000001.SZ"
    assert frame.iloc[0]["pre_close"] == 10.4
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


def test_trading_calendar_capture_preserves_native_source_evidence(monkeypatch):
    captured = {}

    def fake_call(action, **kwargs):
        captured["action"] = action
        captured.update(kwargs)
        return {
            "rows": [{"trade_date": "2026-08-26", "trade_status": 1}],
            "source_method": "ContextInfo.get_trading_dates",
            "observed_start_date": "2026-08-26",
            "observed_end_date": "2026-08-26",
        }

    monkeypatch.setattr(bridge, "_call", fake_call)
    result = bridge.trading_calendar_capture(
        "SH",
        start_date="2026-08-24",
        end_date="2026-08-26",
        timeout=17,
    )

    assert captured == {
        "action": "trading_calendar",
        "timeout": 17,
        "market": "SH",
        "start_date": "2026-08-24",
        "end_date": "2026-08-26",
        "source_stock_code": "000001.SH",
    }
    assert result["source_method"] == "ContextInfo.get_trading_dates"
    assert result["observed_end_date"] == "2026-08-26"


@pytest.mark.parametrize(
    ("code_count", "batch_size", "expected_sizes"),
    [
        (49, 25, [25, 24]),
        (100, 500, [50, 50]),
        (101, 500, [50, 50, 1]),
    ],
)
def test_minute_request_splits_full_market_into_ordered_spool_batches(
    monkeypatch,
    code_count,
    batch_size,
    expected_sizes,
):
    calls = []
    codes = [f"{index:06d}.SZ" for index in range(code_count)]

    def fake_call(action, **kwargs):
        calls.append((action, kwargs))
        return {
            "rows": [
                {"stock_code": code.split(".", 1)[0]}
                for code in kwargs["stock_codes"]
            ]
        }

    monkeypatch.setattr(bridge, "_call", fake_call)

    frame = bridge.minute(
        codes,
        trade_date="2026-07-27",
        start_date="2026-07-27 09:30:00",
        end_date="2026-07-27 15:00:00",
        count=20,
        download_history=False,
        batch_size=batch_size,
        timeout=17,
    )

    assert [len(kwargs["stock_codes"]) for _, kwargs in calls] == expected_sizes
    assert frame["stock_code"].tolist() == [
        code.split(".", 1)[0] for code in codes
    ]
    for action, kwargs in calls:
        assert action == "minute"
        assert kwargs["batch_size"] == min(batch_size, 50)
        assert 0 < kwargs["timeout"] <= 17
        assert kwargs["trade_date"] == "2026-07-27"
        assert kwargs["start_date"] == "2026-07-27 09:30:00"
        assert kwargs["end_date"] == "2026-07-27 15:00:00"
        assert kwargs["count"] == 20
        assert kwargs["download_history"] is False


def test_minute_request_fails_closed_when_any_spool_batch_fails(monkeypatch):
    calls = []

    def fake_call(action, **kwargs):
        calls.append((action, kwargs))
        if len(calls) == 2:
            raise TimeoutError("second minute batch timed out")
        return {"rows": [{"stock_code": kwargs["stock_codes"][0]}]}

    monkeypatch.setattr(bridge, "_call", fake_call)

    with pytest.raises(TimeoutError, match="second minute batch timed out"):
        bridge.minute(
            [f"{index:06d}.SZ" for index in range(401)],
            trade_date="2026-07-27",
            batch_size=200,
        )

    assert len(calls) == 2
    assert [len(kwargs["stock_codes"]) for _, kwargs in calls] == [50, 50]


def test_minute_request_uses_one_total_timeout_deadline(monkeypatch):
    clock = [100.0]
    batch_timeouts = []

    def fake_call(_action, **kwargs):
        batch_timeouts.append(kwargs["timeout"])
        clock[0] += 6.0
        return {"rows": []}

    monkeypatch.setattr(bridge.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(bridge, "_call", fake_call)

    with pytest.raises(TimeoutError, match="10.0s total"):
        bridge.minute(
            [f"{index:06d}.SZ" for index in range(101)],
            trade_date="2026-07-27",
            batch_size=50,
            timeout=10,
        )

    assert batch_timeouts == pytest.approx([10.0, 4.0])


def test_minute_request_preserves_empty_request_semantics(monkeypatch):
    calls = []

    def fake_call(action, **kwargs):
        calls.append((action, kwargs))
        return {"rows": []}

    monkeypatch.setattr(bridge, "_call", fake_call)

    frame = bridge.minute([], trade_date="2026-07-27", batch_size=100)

    assert frame.empty
    assert len(calls) == 1
    assert calls[0][0] == "minute"
    assert calls[0][1]["stock_codes"] == []
    assert calls[0][1]["batch_size"] == 50


def test_minute_request_rejects_malformed_rows_from_later_batch(monkeypatch):
    calls = []

    def fake_call(action, **kwargs):
        calls.append((action, kwargs))
        if len(calls) == 2:
            return {"rows": {"stock_code": "malformed"}}
        return {"rows": [{"stock_code": kwargs["stock_codes"][0]}]}

    monkeypatch.setattr(bridge, "_call", fake_call)

    with pytest.raises(RuntimeError, match="response rows must be a list"):
        bridge.minute(
            [f"{index:06d}.SZ" for index in range(101)],
            trade_date="2026-07-27",
            batch_size=50,
        )

    assert len(calls) == 2
    assert [len(kwargs["stock_codes"]) for _, kwargs in calls] == [50, 50]


def test_daily_history_normalizes_qmt_lots_to_shares(monkeypatch):
    monkeypatch.setattr(
        "integrations.bigqmt.backend.bridge.kline_capture",
        lambda *_args, **_kwargs: {"rows": [{
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
                "pre_close_origin": "NATIVE_QMT",
            }]},
    )

    frame = BigQmtBackend().fetch_kline(
        ["510300"], "2026-07-21", "2026-07-21"
    )

    assert frame.iloc[0]["volume"] == 123400
    assert frame.iloc[0]["data_source"] == PROVIDER_ID
    assert frame.iloc[0]["pre_close_origin"] == "NATIVE_QMT"


def test_daily_history_never_promotes_missing_native_pre_close(monkeypatch):
    monkeypatch.setattr(
        "integrations.bigqmt.backend.bridge.kline_capture",
        lambda *_args, **_kwargs: {"rows": [{
                "qmt_code": "510300.SH",
                "stock_code": "510300",
                "trade_time": "2026-07-22 15:00:00",
                "trade_date": "2026-07-22",
                "open": 4.1,
                "close": 4.2,
                "high": 4.3,
                "low": 4.0,
                "volume": 1234,
                "amount": 518280,
                "pre_close": None,
            }]},
    )

    frame = BigQmtBackend().fetch_kline(
        ["510300"], "2026-07-22", "2026-07-22"
    )

    assert pd.isna(frame.iloc[0]["pre_close"])
    assert frame.iloc[0]["pre_close_origin"] == "MISSING_NATIVE_QMT"


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


def test_realtime_universe_uses_independent_qmt_catalog() -> None:
    normalized = " ".join(run_big_qmt_bridge.ACTIVE_UNIVERSE_SQL.split()).lower()
    assert "qmt_stock_catalog_member" in normalized
    assert "qmt_stock_catalog_batch" in normalized
    assert "member.batch_id=:batch_id" in normalized
    assert "member.list_date <= :target_date" in normalized
    assert "member.expire_date > :target_date" in normalized
    assert "sm_stock_kline" not in normalized


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
