from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, text

from server.trading_v3.daily_features import _qmt_attestation_evidence


def _attestation_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE qmt_kline_attestation_run (
                    run_id TEXT PRIMARY KEY,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status TEXT NOT NULL,
                    target_rows INTEGER NOT NULL,
                    qmt_rows INTEGER NOT NULL,
                    matched_rows INTEGER NOT NULL,
                    missing_qmt_rows INTEGER NOT NULL,
                    mismatched_rows INTEGER NOT NULL,
                    started_at DATETIME NOT NULL
                )
                """
            )
        )
    return engine


def test_qmt_attestation_ignores_newer_empty_target_run():
    engine = _attestation_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO qmt_kline_attestation_run VALUES
                    ('valid', '2026-08-19', '2026-08-19', 'COMPLETED',
                     5547, 5547, 5547, 0, 0, '2026-08-19 21:10:00'),
                    ('empty', '2026-08-19', '2026-08-19', 'EMPTY_TARGET',
                     0, 0, 0, 0, 0, '2026-08-20 09:00:00')
                """
            )
        )

    evidence = _qmt_attestation_evidence(
        engine,
        trade_date=date(2026, 8, 19),
    )

    assert evidence["qmt_attestation_current"] is True
    assert evidence["qmt_attestation_run_id"] == "valid"
    assert evidence["qmt_attestation_target_rows"] == 5547


def test_qmt_attestation_reports_missing_when_only_empty_target_runs_exist():
    engine = _attestation_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO qmt_kline_attestation_run VALUES
                    ('empty', '2026-08-19', '2026-08-19', 'EMPTY_TARGET',
                     0, 0, 0, 0, 0, '2026-08-20 09:00:00')
                """
            )
        )

    evidence = _qmt_attestation_evidence(
        engine,
        trade_date=date(2026, 8, 19),
    )

    assert evidence == {
        "qmt_attestation_current": False,
        "qmt_attestation_status": "MISSING",
        "qmt_attestation_reason": "NO_NONEMPTY_RUN_COVERS_TRADE_DATE",
    }
