from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from tools import sync_upper_limit_snapshot as command


def _codes(count: int = 80) -> list[str]:
    return [f"{number:06d}" for number in range(1, count + 1)]


def _sessions() -> list[str]:
    target = date(2026, 8, 21)
    values: list[date] = []
    current = target
    while len(values) < 21:
        if current.weekday() < 5:
            values.append(current)
        current -= timedelta(days=1)
    return [item.isoformat() for item in sorted(values)]


def test_codes_are_exact_80_unique_a_shares() -> None:
    assert command._parse_codes(",".join(_codes())) == _codes()
    with pytest.raises(RuntimeError, match="exact 80"):
        command._parse_codes(",".join(_codes(79)))
    with pytest.raises(RuntimeError, match="exact 80"):
        command._parse_codes(",".join([*_codes(79), "830001"]))


def test_cli_rejects_bare_codes_without_preliminary_receipt() -> None:
    with pytest.raises(SystemExit):
        command.main([
            "--target-date", "2026-08-21",
            "--decision-at", "2026-08-27T18:50:00",
            "--codes", ",".join(_codes()),
        ])


def test_main_binds_calendar_authority_into_collected_subject(
    monkeypatch, capsys,
) -> None:
    engine = object()
    observed = {}
    proof = {
        "calendar_batch_id": "calendar-batch-1",
        "calendar_manifest_hash": "1" * 64,
        "calendar_session_set_hash": "2" * 64,
    }
    sentinel_run = SimpleNamespace(run_id="3" * 32)

    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        command, "validate_market_field_capture_runtime", lambda value: value
    )
    monkeypatch.setattr(
        command, "validate_trade_calendar_runtime_schema", lambda value: value
    )
    monkeypatch.setattr(
        command, "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-21",
    )
    monkeypatch.setattr(
        command,
        "_load_preliminary_receipt",
        lambda *_args, **_kwargs: {
            "ordered_stock_codes": _codes(),
            "receipt_sha256": "4" * 64,
            "ordered_candidate_sha256": "5" * 64,
        },
    )
    monkeypatch.setattr(
        command, "_load_sessions", lambda *_args, **_kwargs: (_sessions(), proof)
    )
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "a" * 40)
    monkeypatch.setattr(
        command,
        "recover_completed_upper_limit_receipt",
        lambda *_args, **_kwargs: None,
    )

    def collect(**kwargs):
        observed.update(kwargs)
        return sentinel_run

    monkeypatch.setattr(command, "collect_upper_limit_snapshot", collect)
    monkeypatch.setattr(
        command,
        "publish_upper_limit_snapshot",
        lambda actual_engine, run: {
            "status": "COMPLETED",
            "run_id": run.run_id,
            "expected_count": 1680,
        },
    )

    assert command.main([
        "--target-date", "2026-08-21",
        "--decision-at", "2099-08-27 18:50:00",
        "--preliminary-receipt-file", "preliminary.json",
    ]) == 0
    subject = observed["subject"]
    assert subject.calendar_batch_id == proof["calendar_batch_id"]
    assert subject.calendar_manifest_sha256 == proof["calendar_manifest_hash"]
    assert subject.calendar_session_set_sha256 == proof["calendar_session_set_hash"]
    assert len(subject.stock_codes) == 80
    assert len(subject.trade_dates) == 21
    assert subject.subject_identity == f"preview:{'4' * 64}"
    assert '"expected_count":1680' in capsys.readouterr().out


def test_main_recovers_completed_run_before_myquant_call(
    monkeypatch, capsys,
) -> None:
    engine = object()
    proof = {
        "calendar_batch_id": "calendar-batch-1",
        "calendar_manifest_hash": "1" * 64,
        "calendar_session_set_hash": "2" * 64,
    }
    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        command, "validate_market_field_capture_runtime", lambda value: value
    )
    monkeypatch.setattr(
        command, "validate_trade_calendar_runtime_schema", lambda value: value
    )
    monkeypatch.setattr(
        command,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-21",
    )
    monkeypatch.setattr(
        command,
        "_load_preliminary_receipt",
        lambda *_args, **_kwargs: {
            "ordered_stock_codes": _codes(),
            "receipt_sha256": "4" * 64,
            "ordered_candidate_sha256": "5" * 64,
        },
    )
    monkeypatch.setattr(
        command, "_load_sessions", lambda *_args, **_kwargs: (_sessions(), proof)
    )
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "a" * 40)
    monkeypatch.setattr(
        command,
        "recover_completed_upper_limit_receipt",
        lambda *_args, **_kwargs: {
            "schema": "probiga.market-field-capture.v1",
            "status": "COMPLETED",
            "run_id": "3" * 32,
            "expected_count": 1680,
            "expected_stock_count": 80,
            "expected_date_count": 21,
            "collector_build_sha": "a" * 40,
            "preliminary_receipt_sha256": "4" * 64,
            "recovered": True,
        },
    )
    monkeypatch.setattr(
        command,
        "collect_upper_limit_snapshot",
        lambda **_kwargs: pytest.fail("MyQuant must not run during recovery"),
    )

    assert command.main([
        "--target-date", "2026-08-21",
        "--decision-at", "2026-08-21 23:55:00",
        "--preliminary-receipt-file", "preliminary.json",
    ]) == 0
    assert '"recovered":true' in capsys.readouterr().out


def test_main_after_cutoff_without_completed_run_never_calls_myquant(
    monkeypatch,
) -> None:
    engine = object()
    proof = {
        "calendar_batch_id": "calendar-batch-1",
        "calendar_manifest_hash": "1" * 64,
        "calendar_session_set_hash": "2" * 64,
    }
    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        command, "validate_market_field_capture_runtime", lambda value: value
    )
    monkeypatch.setattr(
        command, "validate_trade_calendar_runtime_schema", lambda value: value
    )
    monkeypatch.setattr(
        command,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-21",
    )
    monkeypatch.setattr(
        command,
        "_load_preliminary_receipt",
        lambda *_args, **_kwargs: {
            "ordered_stock_codes": _codes(),
            "receipt_sha256": "4" * 64,
            "ordered_candidate_sha256": "5" * 64,
        },
    )
    monkeypatch.setattr(
        command, "_load_sessions", lambda *_args, **_kwargs: (_sessions(), proof)
    )
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "a" * 40)
    monkeypatch.setattr(
        command,
        "recover_completed_upper_limit_receipt",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        command,
        "collect_upper_limit_snapshot",
        lambda **_kwargs: pytest.fail("MyQuant must not run after cutoff"),
    )

    with pytest.raises(RuntimeError, match="cutoff has elapsed"):
        command.main([
            "--target-date", "2026-08-21",
            "--decision-at", "2026-08-21 23:55:00",
            "--preliminary-receipt-file", "preliminary.json",
        ])
