import json
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_trade_calendar import (
    build_calendar_manifest,
    calendar_source_batch_id,
    load_trade_calendar_receipt,
    load_trade_calendar_window_receipt,
)


@pytest.fixture
def calendar_db():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_batch (
                batch_id TEXT PRIMARY KEY, source_batch_id TEXT,
                known_at TEXT, start_date TEXT, end_date TEXT, status TEXT,
                session_count INTEGER, session_set_hash TEXT,
                manifest_json TEXT, manifest_hash TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_session (
                batch_id TEXT, trade_date TEXT
            )
        """))
    yield engine
    engine.dispose()


def _weekdays(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    return [
        current.isoformat()
        for offset in range((last - first).days + 1)
        if (current := first + timedelta(days=offset)).weekday() < 5
    ]


def _insert(engine, batch="official-2026", *, start="2026-01-01",
            end="2026-12-31", known="2026-09-05 01:38:32", sessions=None):
    sessions = sessions if sessions is not None else _weekdays(start, end)
    manifest, sessions = build_calendar_manifest(
        batch_id=batch,
        source_batch_id=calendar_source_batch_id(
            start_date=start, end_date=end, sessions=sessions,
            source_provider="SSE_SZSE_OFFICIAL",
        ),
        known_at=known, start_date=start, end_date=end, sessions=sessions,
        source_provider="SSE_SZSE_OFFICIAL",
    )
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO qmt_trade_calendar_batch
            VALUES (:batch_id, :source_batch_id, :known_at, :start_date,
                    :end_date, 'COMPLETE', :session_count,
                    :session_set_hash, :manifest_json, :manifest_hash)
        """), {
            **manifest, "manifest_json": json.dumps(manifest),
            "manifest_hash": canonical_digest(manifest),
        })
        connection.execute(text("""
            INSERT INTO qmt_trade_calendar_session VALUES (:batch_id, :day)
        """), [{"batch_id": batch, "day": day} for day in sessions])
    return manifest, tuple(sessions)


def _load(engine, target="2026-09-18", cutoff="2026-09-18 23:59:59"):
    with engine.connect() as connection:
        return load_trade_calendar_window_receipt(
            connection, end_date=target, required_sessions=120,
            decision_known_at=cutoff,
        )


def test_existing_year_receipt_proves_120_without_unneeded_previous_year(calendar_db):
    manifest, sessions = _insert(calendar_db)
    with calendar_db.connect() as connection:
        with pytest.raises(RuntimeError, match="covers target range"):
            load_trade_calendar_receipt(
                connection, start_date="2025-09-17", end_date="2026-09-18",
                decision_known_at="2026-09-18 23:59:59",
            )
    receipt = _load(calendar_db)
    window = receipt.sessions_between(receipt.start_date, "2026-09-18")[-120:]
    assert len(window) == len(set(window)) == 120
    assert window[-1] == "2026-09-18"
    assert receipt.sessions == sessions  # Full source remains hash-verifiable.
    assert receipt.session_count > 120
    assert receipt.manifest_hash == canonical_digest(manifest)
    assert receipt.session_set_hash == manifest["session_set_hash"]
    assert receipt.known_at == "2026-09-05 01:38:32"


def test_preparation_uses_same_validated_exact_window(calendar_db, monkeypatch):
    from tools import prepare_strategy_governance_qmt_history as preparation

    _manifest, sessions = _insert(calendar_db)
    monkeypatch.setattr(preparation, "_calendar_decision_time",
                        lambda: "2026-09-18 23:59:59")
    assert preparation._latest_closed_sessions(
        calendar_db, target_trade_date="2026-09-18",
    ) == [day for day in sessions if day <= "2026-09-18"][-120:]


def test_latest_eligible_receipt_is_selected_without_late_receipt(calendar_db):
    _insert(calendar_db, "older", known="2026-09-04 00:00:00")
    _insert(calendar_db, "selected")
    _insert(calendar_db, "future", known="2026-09-19 00:00:00")
    assert _load(calendar_db).batch_id == "selected"


def test_late_receipt_cannot_recover_past_known_evidence(calendar_db):
    _insert(calendar_db, known="2026-09-19 00:00:00")
    with pytest.raises(RuntimeError, match="required trading sessions"):
        _load(calendar_db)


def test_future_sessions_do_not_satisfy_120_closed_sessions(calendar_db):
    sessions = _weekdays("2026-01-01", "2026-09-18")[-119:]
    sessions += _weekdays("2026-09-21", "2026-12-31")
    _insert(calendar_db, sessions=sessions)
    with pytest.raises(RuntimeError, match="required trading sessions"):
        _load(calendar_db)


def test_receipts_are_not_combined_to_complete_a_window(calendar_db):
    sessions = _weekdays("2026-01-01", "2026-09-18")[-120:]
    _insert(calendar_db, "first-half", sessions=sessions[:60] + sessions[-1:])
    _insert(calendar_db, "second-half", sessions=sessions[60:])
    with pytest.raises(RuntimeError, match="required trading sessions"):
        _load(calendar_db)


def test_newer_short_receipt_does_not_hide_existing_complete_receipt(calendar_db):
    _insert(calendar_db)
    _insert(calendar_db, "short", start="2026-09-01", end="2026-09-18",
            known="2026-09-17 12:00:00")
    assert _load(calendar_db).batch_id == "official-2026"


def test_non_session_target_is_rejected(calendar_db):
    _insert(calendar_db)
    with pytest.raises(ValueError, match="not a trading session"):
        _load(calendar_db, target="2026-09-19", cutoff="2026-09-19 23:59:59")


@pytest.mark.parametrize("mutation", [
    "UPDATE qmt_trade_calendar_batch SET manifest_hash='bad' WHERE batch_id='newest'",
    "DELETE FROM qmt_trade_calendar_session WHERE batch_id='newest' AND trade_date='2026-12-31'",
])
def test_corrupt_full_receipt_cannot_fall_back_to_older_evidence(calendar_db, mutation):
    _insert(calendar_db, "older")
    _insert(calendar_db, "newest", known="2026-09-17 12:00:00")
    with calendar_db.begin() as connection:
        connection.execute(text(mutation))
    with pytest.raises(ValueError):
        _load(calendar_db)


def test_single_cross_year_receipt_is_supported(calendar_db):
    _insert(calendar_db, start="2025-01-01", end="2026-12-31",
            known="2026-01-01 00:00:00")
    receipt = _load(calendar_db, target="2026-02-02", cutoff="2026-02-02 23:59:59")
    window = receipt.sessions_between(receipt.start_date, "2026-02-02")[-120:]
    assert len(window) == 120
    assert window[0] < "2026-01-01"


def test_governance_loader_preserves_bound_connection_and_exact_cutoff(monkeypatch):
    from server.engine import strategy_governance as governance

    bound, expected = object(), object()
    calls = []
    monkeypatch.setattr(governance, "current_bound_sql_connection", lambda: bound)
    monkeypatch.setattr(governance, "get_engine", lambda: pytest.fail("unbound DB"))
    monkeypatch.setattr(governance, "load_trade_calendar_window_receipt",
                        lambda connection, **kwargs: calls.append((connection, kwargs)) or expected)
    assert governance._immutable_calendar_window_receipt(
        end_date="2026-09-18", required_sessions=120,
        decision_known_at="2026-09-18 23:59:59",
    ) is expected
    assert calls == [(bound, {
        "end_date": "2026-09-18", "required_sessions": 120,
        "decision_known_at": "2026-09-18 23:59:59",
    })]


def _governance_load(engine, monkeypatch, *, target, window):
    from server.engine import strategy_governance as governance

    monkeypatch.setattr(governance, "get_engine", lambda: pytest.fail("unbound DB"))
    with engine.connect() as connection:
        monkeypatch.setattr(governance, "current_bound_sql_connection", lambda: connection)
        if window:
            return governance._immutable_calendar_window_receipt(
                end_date=target, required_sessions=120,
                decision_known_at=f"{target} 23:59:59",
            )
        return governance._immutable_calendar_receipt(
            start_date="2026-01-01", end_date=target,
            decision_known_at=f"{target} 23:59:59",
        )


@pytest.mark.parametrize("window", [False, True])
def test_governance_august_release_catchup_retains_original_full_binding(
    calendar_db, monkeypatch, window,
):
    from server.engine import strategy_governance as governance

    manifest, sessions = _insert(calendar_db, known="2026-09-01 20:00:00")
    receipt = _governance_load(
        calendar_db, monkeypatch, target="2026-08-31", window=window,
    )
    window_sessions = receipt.sessions_between(receipt.start_date, "2026-08-31")[-120:]
    assert len(window_sessions) == 120
    assert window_sessions[-1] == "2026-08-31"
    assert receipt.sessions == sessions
    binding = governance._calendar_receipt_binding(receipt)
    assert binding["known_at"] == "2026-09-01 20:00:00"
    assert binding["start_date"] == "2026-01-01"
    assert binding["end_date"] == "2026-12-31"
    assert binding["session_count"] == len(sessions)
    assert binding["session_set_hash"] == manifest["session_set_hash"]
    assert binding["manifest_hash"] == canonical_digest(manifest)


@pytest.mark.parametrize("window", [False, True])
@pytest.mark.parametrize("target,known", [
    ("2026-08-31", "2026-09-02 00:00:00"),
    ("2026-09-18", "2026-09-19 00:00:00"),
])
def test_governance_catchup_does_not_extend_date_or_known_at_limit(
    calendar_db, monkeypatch, window, target, known,
):
    _insert(calendar_db, known=known)
    with pytest.raises(RuntimeError, match="no immutable"):
        _governance_load(calendar_db, monkeypatch, target=target, window=window)


@pytest.mark.parametrize("window", [False, True])
def test_governance_catchup_does_not_replace_corrupt_known_receipt(
    calendar_db, monkeypatch, window,
):
    _insert(calendar_db, "known-corrupt", known="2026-08-31 12:00:00")
    _insert(calendar_db, "valid-late", known="2026-09-01 12:00:00")
    with calendar_db.begin() as connection:
        connection.execute(text("""
            UPDATE qmt_trade_calendar_batch SET manifest_hash='bad'
            WHERE batch_id='known-corrupt'
        """))
    with pytest.raises(ValueError):
        _governance_load(calendar_db, monkeypatch, target="2026-08-31", window=window)
