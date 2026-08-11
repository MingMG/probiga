from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from server.trading_v2.quotes import validate_level1_continuity
from tools import validate_trading_v2_level1


def test_level1_reads_remote_qmt_evidence_and_writes_primary_capability():
    primary = MagicMock()
    evidence = MagicMock()

    primary_reader = primary.connect.return_value.__enter__.return_value
    primary_reader.execute.return_value.fetchall.return_value = [
        (date(2026, 7, day),)
        for day in (28, 27, 24, 23, 22)
    ]
    evidence_reader = evidence.connect.return_value.__enter__.return_value
    evidence_reader.execute.return_value.mappings.return_value.first.return_value = {
        "event_count": 1000,
        "session_minutes": 240,
        "complete_events": 1000,
        "maximum_ingress_seconds": 2,
    }

    result = validate_level1_continuity(
        primary,
        end_date=date(2026, 7, 28),
        evidence_engine=evidence,
    )

    assert result["status"] == "PASS"
    assert result["consecutive_trade_days"] == 5
    assert evidence_reader.execute.call_count == 5
    primary.begin.assert_called_once()


def test_level1_acceptance_requires_continuous_live_receipts():
    evidence = MagicMock()
    reader = evidence.connect.return_value.__enter__.return_value
    reader.execute.return_value.mappings.return_value.first.return_value = {
        "receipt_count": 240,
        "receipt_minutes": 228,
        "latest_source_at": "2026-07-28 15:00:00",
        "latest_published_at": "2026-07-28 15:00:02",
    }

    result = validate_trading_v2_level1._live_receipt_gate(
        evidence,
        date(2026, 7, 28),
    )

    assert result["status"] == "PASS"
    assert result["receipt_minutes"] == 228


def test_level1_acceptance_blocks_when_receipt_minutes_are_incomplete():
    evidence = MagicMock()
    reader = evidence.connect.return_value.__enter__.return_value
    reader.execute.return_value.mappings.return_value.first.return_value = {
        "receipt_count": 80,
        "receipt_minutes": 80,
    }

    result = validate_trading_v2_level1._live_receipt_gate(
        evidence,
        date(2026, 7, 28),
    )

    assert result["status"] == "BLOCK"
    assert result["reason"] == "insufficient_live_receipts"


def test_level1_acceptance_checks_receipts_for_every_evidence_day():
    evidence = MagicMock()
    reader = evidence.connect.return_value.__enter__.return_value
    rows = [
        {
            "receipt_count": 240,
            "receipt_minutes": minutes,
            "latest_source_at": "2026-07-28 15:00:00",
            "latest_published_at": "2026-07-28 15:00:02",
        }
        for minutes in (228, 228, 227, 228, 228)
    ]
    reader.execute.return_value.mappings.return_value.first.side_effect = rows

    result = validate_trading_v2_level1._live_receipt_gate(
        evidence,
        [date(2026, 7, day) for day in (28, 27, 24, 23, 22)],
    )

    assert result["status"] == "BLOCK"
    assert result["trade_day_count"] == 5
    assert result["receipt_minutes"] == 227
    assert reader.execute.call_count == 5


def test_level1_acceptance_rejects_non_qmt_or_backfilled_event_shortcuts():
    evidence = MagicMock()
    reader = evidence.connect.return_value.__enter__.return_value
    reader.execute.return_value.mappings.return_value.first.return_value = {
        "event_count": 1000,
        "session_minutes": 227,
        "complete_events": 1000,
        "minimum_ingress_seconds": 0,
        "maximum_ingress_seconds": 2,
    }

    result = validate_trading_v2_level1._genuine_level1_event_gate(
        evidence,
        date(2026, 7, 28),
    )

    assert result["status"] == "BLOCK"
    assert result["reason"] == "insufficient_genuine_level1_events"
    sql = str(reader.execute.call_args.args[0]).lower()
    assert "source_provider = 'gj_big_qmt_inner'" in sql
    assert "received_at >= quote_at" in sql
    assert "<= 15" in sql


def test_level1_validation_block_exits_nonzero(monkeypatch, capsys):
    monkeypatch.setattr(
        validate_trading_v2_level1,
        "run_validation",
        lambda: {"status": "BLOCK", "block_reason": "insufficient_live_receipts"},
    )
    monkeypatch.setattr(validate_trading_v2_level1, "load_project_env", lambda: None)

    returncode = validate_trading_v2_level1.main()

    assert returncode == 3
    assert '"status": "BLOCK"' in capsys.readouterr().out


def test_level1_validation_runtime_error_exits_error(monkeypatch, capsys):
    monkeypatch.setattr(
        validate_trading_v2_level1,
        "run_validation",
        lambda: (_ for _ in ()).throw(RuntimeError("evidence database unavailable")),
    )
    monkeypatch.setattr(validate_trading_v2_level1, "load_project_env", lambda: None)

    returncode = validate_trading_v2_level1.main()

    assert returncode == 2
    assert '"status": "ERROR"' in capsys.readouterr().out
