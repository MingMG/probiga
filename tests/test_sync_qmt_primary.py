from __future__ import annotations

import json
import subprocess
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from integrations.qmt import bridge
from integrations.registry import get_backend
from tools import sync_qmt_primary


def test_remote_gateway_is_a_valid_configuration_without_local_qmt_python(monkeypatch, tmp_path):
    worker = tmp_path / "worker.py"
    worker.write_text("", encoding="utf-8")
    monkeypatch.setattr(bridge, "WORKER", worker)
    monkeypatch.setenv("QMT_PYTHON", str(tmp_path / "missing-python"))
    monkeypatch.setenv("QMT_GATEWAY_ENABLED", "1")

    assert bridge.is_configured() is True

    monkeypatch.setenv("QMT_GATEWAY_ENABLED", "0")
    assert bridge.is_configured() is False


def test_qmt_primary_wrapper_sets_bounded_minute_policy():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"PYTHONPATH": "repo", "LEGACY_MINIQMT_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ) as run:
        result = sync_qmt_primary.run_dataset(
            "minute_price", date_str="2026-07-17", minute_count=20,
        )

    assert result["status"] == "success"
    env = run.call_args.kwargs["env"]
    assert env["DATA_SOURCE_MINUTE"] == "qmt"
    assert env["QMT_GATEWAY_REQUIRED"] == "1"
    assert env["SM_MAX_STOCKS"] == "0"
    assert env["SM_MAX_INDEXES"] == "0"
    assert env["QMT_MINUTE_COUNT"] == "20"
    assert env["MYQUANT_MINUTE_DATE"] == "2026-07-17"
    assert env["QMT_PRODUCTION_MINUTE_BATCH_SIZE"] == "200"
    assert env["BIG_QMT_MINUTE_BATCH_SIZE"] == "200"
    assert env["QMT_PRODUCTION_INDEX_MINUTE_BATCH_SIZE"] == "40"
    assert env["QMT_MINUTE_DB_CHUNK_SIZE"] == "1000"
    assert "MINUTE_SKIP_CLOSED" not in env
    assert run.call_args.args[0][-2:] == ["sm_stock_minute", "2026-07-17"]


def test_recurring_minute_wrapper_enables_closed_market_guard():
    completed = SimpleNamespace(returncode=0)
    with patch("tools.sync_qmt_primary.build_child_env", return_value={}), patch(
        "tools.sync_qmt_primary.subprocess.run", return_value=completed
    ) as run:
        result = sync_qmt_primary.run_dataset("minute_price", minute_count=20)

    assert result["status"] == "success"
    assert run.call_args.kwargs["env"]["MINUTE_SKIP_CLOSED"] == "1"


def test_qmt_primary_wrapper_reports_timeout_without_hanging_scheduler():
    with patch("tools.sync_qmt_primary.build_child_env", return_value={}), patch(
        "tools.sync_qmt_primary.child_process_timeout",
        return_value=7,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        side_effect=subprocess.TimeoutExpired(["python"], 7),
    ):
        result = sync_qmt_primary.run_dataset("realtime")

    assert result["status"] == "failed"
    assert result["returncode"] == 124
    assert result["error"] == "job timed out after 7s"


def test_qmt_concept_reference_uses_atomic_catalog_membership_job():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"LEGACY_MINIQMT_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary.subprocess.run", return_value=completed
    ) as run:
        result = sync_qmt_primary.run_dataset("concept_reference")

    assert result["status"] == "success"
    assert run.call_args.args[0][1:] == ["tools/sync_qmt_concept_reference.py"]


def test_public_sources_are_primary_when_legacy_miniqmt_is_not_enabled():
    completed = SimpleNamespace(returncode=0)
    with patch("tools.sync_qmt_primary.build_child_env", return_value={}), patch(
        "tools.sync_qmt_primary._qmt_runtime_available"
    ) as qmt_probe, patch(
        "tools.sync_qmt_primary.subprocess.run", return_value=completed
    ) as run:
        result = sync_qmt_primary.run_dataset("realtime")

    qmt_probe.assert_not_called()
    assert result["source_policy"] == "external_primary_miniqmt_disabled"
    assert run.call_args.kwargs["env"]["DATA_SOURCE_CURRENT"] == "adata"
    assert run.call_args.kwargs["env"]["QMT_GATEWAY_ENABLED"] == "0"
    assert run.call_args.args[0][1] == "scripts/sync_realtime_quotes.py"


def test_bigqmt_is_primary_for_index_kline_on_windows_owner():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ) as run:
        result = sync_qmt_primary.run_dataset("index_kline")

    assert result["source_policy"] == "bigqmt_primary"
    assert run.call_args.kwargs["env"]["DATA_SOURCE_INDEX_KLINE"] == "bigqmt"
    assert run.call_args.kwargs["env"]["QMT_GATEWAY_ENABLED"] == "0"
    assert (
        run.call_args.kwargs["env"]["QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK"]
        == "0"
    )


def test_bigqmt_backend_is_registered_for_daily_kline(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_KLINE", "bigqmt")

    backend = get_backend("kline")

    assert backend is not None
    assert backend.name == "bigqmt"


def test_bigqmt_is_primary_for_concept_reference_on_windows_owner():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ) as run:
        result = sync_qmt_primary.run_dataset("concept_reference")

    assert result["source_policy"] == "bigqmt_primary"
    assert run.call_args.kwargs["env"]["SI_CONCEPT_SOURCE"] == "bigqmt"
    assert run.call_args.kwargs["env"]["DATA_SOURCE_CONCEPT_LIST"] == "bigqmt"
    assert run.call_args.args[0][1:] == ["tools/sync_qmt_concept_reference.py"]


def test_bigqmt_realtime_uses_continuous_level1_consumer():
    capture = {
        "status": "success",
        "returncode": 0,
        "error": "",
        "capture_mode": "LIVE_FORWARD",
        "active_session": True,
        "polls": 50,
        "receipt": {"status": "PASS"},
    }
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary._run_bigqmt_level1_window",
        return_value=capture,
    ) as continuous, patch(
        "tools.sync_qmt_primary._archive_bigqmt_current_snapshot",
        return_value=5545,
    ) as archive, patch("tools.sync_qmt_primary.subprocess.run") as run:
        result = sync_qmt_primary.run_dataset("realtime")

    assert result["source_policy"] == "bigqmt_primary"
    assert result["level1_capture"] == {
        **capture,
        "snapshot_rows": 5545,
        "snapshot_archive_status": "archived",
    }
    continuous.assert_called_once()
    archive.assert_called_once_with()
    run.assert_not_called()


def test_bigqmt_realtime_skips_snapshot_archive_outside_active_session():
    capture = {
        "status": "success",
        "returncode": 0,
        "error": "",
        "capture_mode": "OFF_SESSION_SNAPSHOT",
        "active_session": False,
        "polls": 1,
        "receipt": {"status": "PASS"},
    }
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary._run_bigqmt_level1_window",
        return_value=capture,
    ), patch(
        "tools.sync_qmt_primary._archive_bigqmt_current_snapshot",
    ) as archive, patch("tools.sync_qmt_primary.subprocess.run") as run:
        result = sync_qmt_primary.run_dataset("realtime")

    assert result["status"] == "success"
    assert result["level1_capture"] == {
        **capture,
        "snapshot_archive_status": "skipped_off_session",
    }
    archive.assert_not_called()
    run.assert_not_called()


def test_bigqmt_realtime_archives_fresh_current_table_snapshot():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    lock_result = MagicMock()
    lock_result.scalar.return_value = 1
    source_lock_result = MagicMock()
    source_lock_result.scalar.return_value = 1
    receipt_result = MagicMock()
    receipt_result.mappings.return_value.first.return_value = {
        "receipt_id": "receipt-1",
        "source_provider": "gj_big_qmt_inner",
        "source_generated_at": datetime(2026, 8, 19, 13, 45, 57),
        "heartbeat_at": datetime(2026, 8, 19, 13, 45, 59),
        "expected_count": 5545,
        "observed_count": 5545,
        "coverage": 1.0,
        "published_at": datetime(2026, 8, 19, 13, 46, 0),
        "capture_mode": "LIVE_FORWARD",
        "quality_status": "PASS",
        "evidence_json": json.dumps(
            {"full_batch_id": "bigqmt_full_1", "full_quote_count": 5545}
        ),
    }
    source_result = MagicMock()
    source_result.mappings.return_value.first.return_value = {
        "row_count": 5545,
        "stock_count": 5545,
        "full_batch_rows": 5445,
        "fresh_source_rows": 5511,
        "fresh_ingest_rows": 5545,
        "first_received_at": datetime(2026, 8, 19, 13, 45, 58),
        "latest_received_at": datetime(2026, 8, 19, 13, 45, 59),
    }
    existing_result = MagicMock()
    existing_result.mappings.return_value.first.return_value = {
        "row_count": 0,
        "stock_count": 0,
    }
    insert_result = MagicMock(rowcount=5545)
    release_result = MagicMock()
    connection.execute.side_effect = [
        lock_result,
        source_lock_result,
        receipt_result,
        source_result,
        existing_result,
        insert_result,
        release_result,
        MagicMock(),
    ]
    connection.in_transaction.return_value = False

    with patch(
        "server.common.batch_db.create_batch_engine",
        return_value=engine,
    ):
        inserted = sync_qmt_primary._archive_bigqmt_current_snapshot(
            now=datetime(2026, 8, 19, 13, 46, 0),
        )

    assert inserted == 5545
    insert_sql = str(connection.execute.call_args_list[5].args[0])
    assert "INSERT INTO sm_rt_quote_snapshot" in insert_sql
    insert_params = connection.execute.call_args_list[5].args[1]
    assert insert_params["snapshot_at"] == datetime(2026, 8, 19, 13, 45, 57)
    assert insert_params["provider"] == "gj_big_qmt_inner"
    assert insert_params["quality_status"] == "VALIDATED"
    source_sql = str(connection.execute.call_args_list[3].args[0])
    assert "data_source = :provider" in source_sql
    assert "quality_status = :quality_status" in source_sql
    assert "SUM(batch_id = :full_batch_id)" in source_sql
    assert "snapshot_at >= :source_fresh_after" in source_sql
    assert "received_at >= :source_fresh_after" in source_sql
    released = [call.args[1]["lock_name"] for call in connection.execute.call_args_list[-2:]]
    assert released == [
        "probiga:stock_current",
        "probiga:qmt-realtime-snapshot-archive",
    ]
    engine.dispose.assert_called_once_with()


def test_bigqmt_realtime_archive_rejects_partial_receipt():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    lock_result = MagicMock()
    lock_result.scalar.return_value = 1
    source_lock_result = MagicMock()
    source_lock_result.scalar.return_value = 1
    receipt_result = MagicMock()
    receipt_result.mappings.return_value.first.return_value = {
        "receipt_id": "receipt-partial",
        "source_provider": "gj_big_qmt_inner",
        "source_generated_at": datetime(2026, 8, 19, 13, 45, 57),
        "heartbeat_at": datetime(2026, 8, 19, 13, 45, 59),
        "expected_count": 5545,
        "observed_count": 1000,
        "coverage": 1000 / 5545,
        "published_at": datetime(2026, 8, 19, 13, 46, 0),
        "capture_mode": "LIVE_FORWARD",
        "quality_status": "PASS",
        "evidence_json": json.dumps(
            {"full_batch_id": "bigqmt_full_partial", "full_quote_count": 1000}
        ),
    }
    connection.execute.side_effect = [
        lock_result,
        source_lock_result,
        receipt_result,
        MagicMock(),
        MagicMock(),
    ]
    connection.in_transaction.return_value = False

    with patch("server.common.batch_db.create_batch_engine", return_value=engine):
        with pytest.raises(RuntimeError, match="coverage is below contract"):
            sync_qmt_primary._archive_bigqmt_current_snapshot(
                now=datetime(2026, 8, 19, 13, 46, 0),
            )

    assert all(
        "INSERT INTO sm_rt_quote_snapshot" not in str(call.args[0])
        for call in connection.execute.call_args_list
    )
    assert all(
        "RELEASE_LOCK" in str(call.args[0])
        for call in connection.execute.call_args_list[-2:]
    )


def test_bigqmt_realtime_archive_retry_is_idempotent():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    lock_result = MagicMock()
    lock_result.scalar.return_value = 1
    source_lock_result = MagicMock()
    source_lock_result.scalar.return_value = 1
    receipt_result = MagicMock()
    receipt_result.mappings.return_value.first.return_value = {
        "receipt_id": "receipt-repeat",
        "source_provider": "gj_big_qmt_inner",
        "source_generated_at": datetime(2026, 8, 19, 13, 45, 57),
        "heartbeat_at": datetime(2026, 8, 19, 13, 45, 59),
        "expected_count": 5545,
        "observed_count": 5545,
        "coverage": 1.0,
        "published_at": datetime(2026, 8, 19, 13, 46, 0),
        "capture_mode": "LIVE_FORWARD",
        "quality_status": "PASS",
        "evidence_json": json.dumps(
            {"full_batch_id": "bigqmt_full_repeat", "full_quote_count": 5545}
        ),
    }
    source_result = MagicMock()
    source_result.mappings.return_value.first.return_value = {
        "row_count": 5545,
        "stock_count": 5545,
        "full_batch_rows": 5445,
        "fresh_source_rows": 5511,
        "fresh_ingest_rows": 5545,
    }
    existing_result = MagicMock()
    existing_result.mappings.return_value.first.return_value = {
        "row_count": 5545,
        "stock_count": 5545,
    }
    connection.execute.side_effect = [
        lock_result,
        source_lock_result,
        receipt_result,
        source_result,
        existing_result,
        MagicMock(),
        MagicMock(),
    ]
    connection.in_transaction.return_value = False

    with patch("server.common.batch_db.create_batch_engine", return_value=engine):
        inserted = sync_qmt_primary._archive_bigqmt_current_snapshot(
            now=datetime(2026, 8, 19, 13, 46, 0),
        )

    assert inserted == 5545
    assert all(
        "INSERT INTO sm_rt_quote_snapshot" not in str(call.args[0])
        for call in connection.execute.call_args_list
    )
    assert all(
        "RELEASE_LOCK" in str(call.args[0])
        for call in connection.execute.call_args_list[-2:]
    )


def test_continuous_level1_capture_reconnects_and_recovers(monkeypatch, tmp_path):
    from integrations.bigqmt import bridge as bigqmt_bridge
    from tools import run_big_qmt_bridge as consumer

    class Engine:
        def dispose(self):
            return None

    clock = [0.0]
    now = datetime(2026, 7, 27, 10, 0, 0)
    calls = {"receipt": 0, "reconnect": 0}
    persisted: list[dict] = []

    monkeypatch.setattr("server.common.batch_db.create_batch_engine", lambda **_kwargs: Engine())
    monkeypatch.setattr("integrations.bigqmt.spool.resolve_big_qmt_home", lambda **_kwargs: tmp_path)
    monkeypatch.setattr(sync_qmt_primary, "_level1_collection_window", lambda *_args: True)
    monkeypatch.setattr(
        consumer,
        "refresh_watchlist",
        lambda *_args, **_kwargs: {
            "universe": ["000001"],
            "tracked": ["000001"],
            "short_name_map": {},
        },
    )

    def original_persist(_engine, rows):
        persisted.extend(rows)
        return {"received": len(rows), "inserted": len(rows)}

    monkeypatch.setattr(consumer, "persist_quote_events", original_persist)

    def ingest_once(engine, **_kwargs):
        consumer.persist_quote_events(engine, [{"unsafe_history": True}])
        return {"status": "success"}

    monkeypatch.setattr(consumer, "ingest_once", ingest_once)

    live_frame = pd.DataFrame(
        [{
            "stock_code": "000001",
            "source_time": now,
            "received_at": now,
            "bid1": 10.0,
            "ask1": 10.01,
            "bid1_volume": 100,
            "ask1_volume": 100,
        }]
    )

    def receipt(*_args, **_kwargs):
        calls["receipt"] += 1
        if calls["receipt"] <= 2:
            return pd.DataFrame(), {
                "status": "BLOCK",
                "reason": "no_fresh_live_callback",
            }
        return live_frame, {"status": "PASS", "reason": "live_callback_verified"}

    monkeypatch.setattr(bigqmt_bridge, "level1_snapshot", receipt)
    monkeypatch.setattr(
        bigqmt_bridge,
        "request_level1_reconnect",
        lambda **_kwargs: calls.__setitem__("reconnect", calls["reconnect"] + 1) or {"status": "requested"},
    )

    result = sync_qmt_primary._run_bigqmt_level1_window(
        {
            "BIG_QMT_LEVEL1_WINDOW_SECONDS": "0.2",
            "BIG_QMT_LEVEL1_POLL_SECONDS": "0.2",
        },
        now_fn=lambda: now,
        monotonic_fn=lambda: clock[0],
        sleep_fn=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    assert result["status"] == "success"
    assert result["polls"] == 2
    assert result["accepted_rows"] == 1
    assert result["inserted_rows"] == 1
    assert result["reconnects"] == 1
    assert calls["reconnect"] == 1
    assert persisted and "unsafe_history" not in persisted[0]


def test_bigqmt_daily_kline_requires_same_day_attestation():
    completed = SimpleNamespace(returncode=0)
    attested = {
        "status": "COMPLETED",
        "start_date": "2026-07-27",
        "end_date": "2026-07-27",
    }
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ), patch(
        "server.common.batch_db.create_batch_engine",
        return_value=object(),
    ), patch(
        "tools.attest_qmt_daily_kline.attest_range",
        return_value=attested,
    ) as attest:
        result = sync_qmt_primary.run_dataset(
            "daily_kline",
            date_str="2026-07-27",
        )

    assert result["status"] == "success"
    assert result["attestation"] == attested
    assert result["source_policy"] == "bigqmt_primary"
    assert attest.call_args.kwargs["start_date"] == "2026-07-27"
    assert attest.call_args.kwargs["end_date"] == "2026-07-27"
    assert attest.call_args.kwargs["apply"] is True


def test_enabled_bigqmt_fallback_stays_failed_until_daily_attestation_completes():
    completed = SimpleNamespace(returncode=0)
    empty_attestation = {
        "status": "EMPTY_TARGET",
        "start_date": "2026-08-19",
        "end_date": "2026-08-19",
    }
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=False,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ) as run, patch(
        "server.common.batch_db.create_batch_engine",
        return_value=object(),
    ), patch(
        "tools.attest_qmt_daily_kline.attest_range",
        return_value=empty_attestation,
    ) as attest:
        result = sync_qmt_primary.run_dataset(
            "daily_kline",
            date_str="2026-08-19",
        )

    assert result["status"] == "failed"
    assert result["returncode"] == 3
    assert result["source_policy"] == "external_primary_miniqmt_disabled"
    assert "EMPTY_TARGET" in result["error"]
    assert run.call_args.kwargs["env"]["SM_SKIP_DDL"] == "1"
    attest.assert_called_once()


def test_external_only_daily_kline_does_not_require_qmt_attestation():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={},
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ), patch(
        "tools.attest_qmt_daily_kline.attest_range",
    ) as attest:
        result = sync_qmt_primary.run_dataset(
            "daily_kline",
            date_str="2026-08-19",
        )

    assert result["status"] == "success"
    assert result["attestation"] is None
    attest.assert_not_called()


def test_qmt_index_kline_acceptance_can_request_a_bounded_history_range():
    completed = SimpleNamespace(returncode=0)
    with patch("tools.sync_qmt_primary.build_child_env", return_value={}), patch(
        "tools.sync_qmt_primary.subprocess.run", return_value=completed
    ) as run:
        result = sync_qmt_primary.run_dataset(
            "index_kline",
            start_date="2020-01-01",
            end_date="2026-07-17",
        )

    command = run.call_args.args[0]
    assert result["status"] == "success"
    assert run.call_args.kwargs["env"]["DATA_SOURCE_INDEX_KLINE"] == "tencent"
    assert command[1:3] == ["-m", "biz.stock_market.sync_stock_market"]
    assert command[-4:] == ["--kline-start", "2020-01-01", "--kline-end", "2026-07-17"]
