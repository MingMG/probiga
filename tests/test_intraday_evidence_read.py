from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from biz.analysis.intraday_reversal import load_intraday_evidence
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import persisted_fingerprint
from server.common.qmt_minute_publication_scope import SCHEMA


DAY = date(2026, 11, 10)
CODES = ("000001", "600000")
DECISION = "2026-11-10 22:20:00"


def _minute_rows(day: date, code: str) -> list[dict]:
    clocks = [
        datetime.combine(day, time(9, 30)) + timedelta(minutes=offset)
        for offset in range(121)
    ] + [
        datetime.combine(day, time(13, 1)) + timedelta(minutes=offset)
        for offset in range(120)
    ]
    return [
        {
            "stock_code": code,
            "trade_date": day.isoformat(),
            "trade_time": stamp.isoformat(sep=" "),
            "price": 10.0 + offset / 1000,
            "change": offset / 1000,
            "change_pct": offset / 100,
            "volume": 100.0,
            "amount": (10.0 + offset / 1000) * 100.0,
            "data_source": "qmt_datadir",
            "qmt_code": code + (".SH" if code.startswith("6") else ".SZ"),
            "source_time": stamp.isoformat(sep=" "),
            "batch_id": "native-run",
            "data_version": "qmt_datadir_v1",
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
        }
        for offset, stamp in enumerate(clocks)
    ]


def _insert_rows(connection, rows):
    connection.execute(text(
        "INSERT INTO sm_stock_minute VALUES (:stock_code, :trade_date, "
        ":trade_time, :price, :change, :change_pct, :volume, :amount, :data_source, "
        ":qmt_code, :source_time, :batch_id, :data_version, :quality_status, :permission_status)"
    ), rows)


def _publish_native_fixture(engine, *, known_at="2026-11-10 20:00:00"):
    # Hash the actual native rows once, as the completed publisher does.
    fingerprints = persisted_fingerprint(
        engine, dataset="stock_minute", trade_date=DAY.isoformat(),
        include_security_fingerprints=True,
    )
    manifest = {
        "schema": SCHEMA, "dataset": "stock_minute", "trade_date": DAY.isoformat(),
        "known_at": known_at, "run_id": "native-run", "source_content_sha256": "a" * 64,
        "partition_row_hash": fingerprints["row_hash"],
        "security_fingerprints": fingerprints["security_fingerprints"],
    }
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO qmt_history_coverage_manifest VALUES "
            "(:digest,:schema,'stock_minute_security_publication',:day,'qmt_datadir','EXACT',"
            "1,:known_at,:payload,'native-run',:bars,:codes,:root,:known_at)"
        ), {
            "digest": canonical_digest(manifest), "schema": SCHEMA, "day": DAY.isoformat(),
            "known_at": known_at, "payload": json.dumps(manifest),
            "bars": fingerprints["row_count"], "codes": fingerprints["code_count"],
            "root": canonical_digest(fingerprints["security_fingerprints"]),
        })


def _load(engine, codes):
    return load_intraday_evidence(
        engine, trade_date=DAY.isoformat(), stock_codes=codes,
        decision_known_at=DECISION, daily_pre_close={str(code).zfill(6): 10 for code in codes},
    )


@pytest.fixture
def minute_engine():
    engine = create_engine("sqlite://", poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE sm_stock_minute (stock_code TEXT, trade_date TEXT, "
            "trade_time TEXT, price REAL, `change` REAL, change_pct REAL, "
            "volume REAL, amount REAL, data_source TEXT, qmt_code TEXT, source_time TEXT, "
            "batch_id TEXT, data_version TEXT, quality_status TEXT, permission_status TEXT)"
        ))
        # The production incident has this index, without a leading date key.
        connection.execute(text(
            "CREATE INDEX idx_sm_code_time ON sm_stock_minute(stock_code, trade_time)"
        ))
        connection.execute(text(
            "CREATE TABLE qmt_history_coverage_manifest (manifest_hash TEXT PRIMARY KEY,"
            "schema_version TEXT,dataset TEXT,trade_date TEXT,provider TEXT,status TEXT,"
            "strategy_eligible INTEGER,captured_at TEXT,manifest_json TEXT,run_id TEXT,"
            "bar_count INTEGER,actual_traded_count INTEGER,entity_root_hash TEXT,created_at TEXT)"
        ))
        rows = [
            row
            for offset in range(61)
            for code in CODES
            for row in _minute_rows(DAY - timedelta(days=offset), code)
        ]
        _insert_rows(connection, rows)
    _publish_native_fixture(engine)
    yield engine
    engine.dispose()


@contextmanager
def _vm_budget(engine, *, steps: int):
    """Bound actual SQLite execution work without a wall-clock race."""
    raw = engine.raw_connection()
    connection = raw.driver_connection
    raw.close()
    work = {"steps": 0}

    def progress():
        work["steps"] += 100
        return int(work["steps"] > steps)

    connection.set_progress_handler(progress, 100)
    try:
        yield work
    finally:
        connection.set_progress_handler(None, 0)


def test_intraday_read_uses_day_range_instead_of_scanning_stock_history(minute_engine):
    # Reproduce the observed query on the same index and historical layout.
    # Its missing time range exceeds the budget even for just two securities.
    with _vm_budget(minute_engine, steps=40_000):
        with pytest.raises(OperationalError, match="interrupted"):
            with minute_engine.connect() as connection:
                connection.execute(text(
                    "SELECT stock_code, trade_time, price, `change`, change_pct, "
                    "volume, amount, data_source FROM sm_stock_minute "
                    "WHERE trade_date=:day AND stock_code IN ('000001','600000') "
                    "AND price IS NOT NULL AND price>0 ORDER BY stock_code, trade_time"
                ), {"day": DAY.isoformat()}).all()

    with _vm_budget(minute_engine, steps=40_000):
        actual = _load(minute_engine, CODES)

    assert list(actual["stock_code"]) == list(CODES)
    assert actual["minute_available"].all()
    assert list(actual["minute_rows"]) == [241, 241]
    for row in actual.to_dict("records"):
        event = json.loads(row["intraday_event_json"])
        assert event["first_trade_time"] == "2026-11-10 09:30:00"
        assert event["last_trade_time"] == "2026-11-10 15:00:00"
        assert event["amount_cny"] == pytest.approx(243892.0)


def test_intraday_read_keeps_date_scope_and_blocks_changed_native_rows(minute_engine):
    with minute_engine.begin() as connection:
        # A bad date label must not pull next-day data into today's summary.
        baseline = _minute_rows(DAY, "000001")[0]
        _insert_rows(connection, [
            {**baseline, "trade_time": "2026-11-11 00:00:00", "price": 99},
            {**baseline, "trade_date": "2026-11-09", "trade_time": "2026-11-10 09:29:00"},
            {**baseline, "trade_time": "2026-11-10 09:29:00", "data_source": "another_source"},
        ])
        # Native values changed after publication must fail their prior hash.
        connection.execute(text(
            "UPDATE sm_stock_minute SET price=0 "
            "WHERE stock_code='600000' AND trade_time='2026-11-10 09:30:00'"
        ))

    actual = _load(minute_engine, ["600000", "1", "000001"]).set_index("stock_code")

    assert list(actual.index) == list(CODES)
    assert actual.loc["000001", "minute_available"]
    assert actual.loc["000001", "minute_rows"] == 241
    assert not actual.loc["600000", "minute_available"]
    event = json.loads(actual.loc["600000", "intraday_event_json"])
    assert event["status"] == "DATA_BLOCKED"
    assert event["reason"] == "verified stock minute rows missing or changed"


def test_intraday_read_bounds_executed_batches_without_omitting_securities(minute_engine):
    additional_codes = [str(500000 + offset) for offset in range(81)]
    expected_codes = set(CODES) | set(additional_codes)
    with minute_engine.begin() as connection:
        _insert_rows(connection, [row for code in additional_codes for row in _minute_rows(DAY, code)])
    _publish_native_fixture(minute_engine, known_at="2026-11-10 20:01:00")

    executed_batches = []
    reply_limits = []
    proof_reads = []

    def record_batch(connection, cursor, statement, parameters, context, executemany):
        if "FROM sm_stock_minute" in statement.replace(chr(96), ""):
            bound = context.compiled_parameters[0]
            executed_batches.append([
                value for key, value in bound.items() if key.startswith("codes_")
            ])
            reply_limits.append(bound["row_limit"])
        elif "FROM qmt_history_coverage_manifest" in statement:
            proof_reads.append(statement)

    event.listen(minute_engine, "before_cursor_execute", record_batch)
    try:
        actual = _load(
            minute_engine, additional_codes[::-1] + list(CODES) + additional_codes[:2],
        )
    finally:
        event.remove(minute_engine, "before_cursor_execute", record_batch)

    assert [len(batch) for batch in executed_batches] == [40, 40, 3]
    assert reply_limits == [40 * 241 + 1, 40 * 241 + 1, 3 * 241 + 1]
    assert len(proof_reads) == 1
    executed_codes = [code for batch in executed_batches for code in batch]
    assert len(executed_codes) == len(expected_codes)
    assert set(executed_codes) == expected_codes
    assert set(actual["stock_code"]) == expected_codes
    assert len(actual) == len(expected_codes)
    assert actual["minute_available"].all()
    assert actual["minute_rows"].eq(241).all()
    assert actual["minute_source"].eq("qmt_datadir").all()


@pytest.mark.parametrize("codes", [[], ["999999"]])
def test_intraday_read_does_not_fill_missing_securities(minute_engine, codes):
    actual = _load(minute_engine, codes)
    if not codes:
        assert actual.empty
    else:
        assert list(actual["stock_code"]) == codes
        assert not actual["minute_available"].any()
        assert json.loads(actual.iloc[0]["intraday_event_json"])["status"] == "DATA_BLOCKED"
