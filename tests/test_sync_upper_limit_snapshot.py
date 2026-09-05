from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text

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


def _preflight_engine(*, completed=False):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE st_market_field_capture_run (
            target_date TEXT, decision_at TIMESTAMP, collector_build_sha TEXT,
            status TEXT, capture_kind TEXT, provider TEXT
        )"""))
        connection.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, k_type INTEGER,
            adjust_type INTEGER, volume REAL
        )"""))
        connection.execute(text("INSERT INTO sm_stock_kline VALUES ('600000', '2026-09-04', 1, 0, 100)"))
        if completed:
            connection.execute(text("""INSERT INTO st_market_field_capture_run VALUES (
                '2026-09-04', :decision, :build_sha, 'COMPLETED', :kind, :provider
            )"""), {
                "decision": datetime(2026, 9, 4, 22, 20),
                "build_sha": "a" * 40, "kind": command.UPPER_LIMIT_CAPTURE_KIND,
                "provider": command.UPPER_LIMIT_PROVIDER,
            })
    return engine


def _run_preflight(engine, *, now=datetime(2026, 9, 4, 22, 0)):
    command._preflight_upper_provider(
        engine, target=date(2026, 9, 4), decision_at=datetime(2026, 9, 4, 22, 20),
        build_sha="a" * 40, now=now, timeout_seconds=300, kline_engine=engine,
    )


def test_preflight_uses_bounded_real_field_api_and_does_not_publish(monkeypatch):
    observed = []
    monkeypatch.setattr(command, "upper_limit_history_evidence", lambda *args, **kwargs: observed.append((args, kwargs)))
    monkeypatch.setattr(command, "publish_upper_limit_snapshot", lambda *_args: pytest.fail("probe is not evidence"))
    _run_preflight(_preflight_engine())
    assert observed == [((['600000'],), {
        "start_date": "2026-09-04", "end_date": "2026-09-04", "timeout": 15,
    })]


def test_preflight_recovery_does_not_need_current_provider_connectivity(monkeypatch):
    monkeypatch.setattr(command, "upper_limit_history_evidence", lambda *_args, **_kwargs: pytest.fail("recovery must remain offline"))
    _run_preflight(_preflight_engine(completed=True), now=datetime(2026, 9, 5, 12))


def test_preflight_does_not_call_provider_after_cutoff(monkeypatch):
    monkeypatch.setattr(command, "upper_limit_history_evidence", lambda *_args, **_kwargs: pytest.fail("cutoff has passed"))
    with pytest.raises(RuntimeError, match="cutoff has elapsed"):
        _run_preflight(_preflight_engine(), now=datetime(2026, 9, 5, 12))


def test_main_provider_failure_happens_before_expensive_preliminary_analysis(monkeypatch):
    engine = _preflight_engine()
    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(command, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(command, "validate_market_field_capture_runtime", lambda _engine: None)
    monkeypatch.setattr(command, "validate_trade_calendar_runtime_schema", lambda _engine: None)
    monkeypatch.setattr(command, "authoritative_closed_trade_date", lambda *_args, **_kwargs: "2026-09-04")
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "a" * 40)
    monkeypatch.setattr(command, "_load_sessions", lambda *_args, **_kwargs: ([], {}))
    monkeypatch.setattr(command, "prepare_preliminary_upper_subject_receipt", lambda *_args, **_kwargs: pytest.fail("expensive analysis must not run"))

    def unavailable(*_args, **_kwargs):
        raise command.MyQuantBridgeError("terminal service unavailable")

    monkeypatch.setattr(command, "upper_limit_history_evidence", unavailable)
    with pytest.raises(RuntimeError, match="capability probe failed before preliminary"):
        command.main([
            "--target-date", "2026-09-04", "--decision-at", "2099-09-04 22:20:00",
            "--prepare-preliminary",
        ])


def test_preflight_reads_sample_from_canonical_kline_route_not_business_database(monkeypatch):
    business, canonical = _preflight_engine(), _preflight_engine()
    with business.begin() as connection:
        connection.execute(text("DROP TABLE sm_stock_kline"))
    with canonical.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET stock_code='000001'"))
    monkeypatch.setattr(command, "get_kline_engine", lambda: canonical)
    samples = []
    monkeypatch.setattr(command, "upper_limit_history_evidence", lambda codes, **_kwargs: samples.extend(codes))
    command._preflight_upper_provider(
        business, target=date(2026, 9, 4), decision_at=datetime(2026, 9, 4, 22, 20),
        build_sha="a" * 40, now=datetime(2026, 9, 4, 22, 0), timeout_seconds=300,
    )
    assert samples == ["000001"]


def test_preflight_timeout_is_limited_by_remaining_cutoff(monkeypatch):
    calls = []
    monkeypatch.setattr(command, "upper_limit_history_evidence", lambda *_args, **kwargs: calls.append(kwargs))
    _run_preflight(_preflight_engine(), now=datetime(2026, 9, 4, 22, 19, 55))
    assert calls[0]["timeout"] == 5


def test_preliminary_does_not_start_when_probe_consumed_cutoff(monkeypatch):
    engine = _preflight_engine()
    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(command, "validate_market_field_capture_runtime", lambda _engine: None)
    monkeypatch.setattr(command, "validate_trade_calendar_runtime_schema", lambda _engine: None)
    monkeypatch.setattr(command, "authoritative_closed_trade_date", lambda *_args, **_kwargs: "2026-09-04")
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "a" * 40)
    monkeypatch.setattr(command, "_load_sessions", lambda *_args, **_kwargs: ([], {}))
    monkeypatch.setattr(command, "_preflight_upper_provider", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(command, "prepare_preliminary_upper_subject_receipt", lambda *_args, **_kwargs: pytest.fail("deadline elapsed"))
    clock = Mock(wraps=datetime)
    before, after = datetime(2026, 9, 4, 22, 19, 55), datetime(2026, 9, 4, 22, 20, 1)
    clock.now.side_effect = [before, before, after]
    monkeypatch.setattr(command, "datetime", clock)
    with pytest.raises(RuntimeError, match="cutoff elapsed during capability probe"):
        command.main([
            "--target-date", "2026-09-04", "--decision-at", "2026-09-04 22:20:00",
            "--prepare-preliminary",
        ])
