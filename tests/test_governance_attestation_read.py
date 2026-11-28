from datetime import datetime
from decimal import Decimal
import hashlib
import json
import sqlite3
from types import SimpleNamespace
import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError

from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import governance_daily_market as market


@pytest.fixture
def evidence(tmp_path):
    # MySQL drivers return DECIMAL as Decimal. Keeping the physical bytes as
    # SQLite TEXT lets the test distinguish amounts below double precision.
    sqlite3.register_converter("DECIMALTEXT", lambda raw: Decimal(raw.decode("ascii")))
    engine = create_engine("sqlite:///" + str(tmp_path / "evidence.db"),
                           connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})

    @event.listens_for(engine, "connect")
    def functions(db, _):
        db.create_function("CONCAT_WS", -1, lambda sep, *items: sep.join(map(str, items)))
        db.create_function("SHA2", 2, lambda value, _: hashlib.sha256(value.encode()).hexdigest())

    privileged_migrate_formal_analysis_schema(engine)
    protocol = "QMT_DAILY_UNADJUSTED_PRECLOSE_V2"
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE qmt_kline_attestation_row (
            attestation_id TEXT PRIMARY KEY, trade_date TEXT, stock_code TEXT,
            protocol_version TEXT, target_id INT, qmt_id INT, run_id TEXT,
            source_data_version TEXT, source_pre_close_origin TEXT, created_at TEXT,
            source_pre_close TEXT, attested_open TEXT, attested_close TEXT,
            attested_high TEXT, attested_low TEXT, attested_volume TEXT, attested_amount DECIMALTEXT)"""))
        c.execute(text("CREATE INDEX dated ON qmt_kline_attestation_row(trade_date,protocol_version,stock_code,attestation_id)"))
        c.execute(text("""INSERT INTO qmt_kline_attestation_row VALUES
            (:id,'2026-06-01',:code,:protocol,:target,:target,'completed',
             'version','NATIVE_QMT',:created,'10','10','10','10','10','100','1000')"""), [
            {"id": hashlib.sha256("|".join(map(str, (protocol, i, i, 'version', 10, 10, 10, 10, 10, 100, 1000))).encode()).hexdigest(),
             "code": f"00{i:04d}", "protocol": protocol, "target": i,
             "created": "2026-06-02 15:00:00" if i == 2601 else "2026-06-01 15:00:00"}
            for i in range(1, 2602)
        ])
    params = {"start_date": "2026-06-01", "as_of_date": "2026-06-01",
              "protocol_version": protocol, "decision_known_at": datetime(2026, 6, 3)}
    yield engine, params
    engine.dispose()


def _read_rows(sql, params, *, database):
    with database.connect() as conn:
        return [dict(row) for row in conn.execute(text(sql), params).mappings()]


def _seal_rows(engine):
    with engine.begin() as conn:
        for operation in ("UPDATE", "DELETE"):
            conn.exec_driver_sql(f"""CREATE TRIGGER sealed_attestation_{operation} BEFORE {operation}
                ON qmt_kline_attestation_row BEGIN SELECT RAISE(ABORT, 'append only'); END""")
    with pytest.raises(IntegrityError, match="append only"):
        with engine.begin() as conn:
            conn.exec_driver_sql("UPDATE qmt_kline_attestation_row SET run_id='changed'")


def test_verified_pages_resume_for_same_security_and_preserve_original_times(evidence, monkeypatch):
    engine, params = evidence
    with engine.begin() as conn:
        conn.execute(text("UPDATE qmt_kline_attestation_row SET stock_code='000001'"))
    _seal_rows(engine)
    fetched = []
    now = [0]
    monkeypatch.setattr(market.time, "monotonic", lambda: now[0])

    def read(sql, args, *, database):
        rows = _read_rows(sql, args, database=database)
        if "attestation_id in" in sql.lower():
            assert 0 < len(rows) <= 64
            fetched.extend(row["attestation_id"] for row in rows)
            if len(fetched) == 128:
                now[0] = 2
        return rows

    with pytest.raises(TimeoutError):
        market.read_legacy_attestations(read, params, database=engine, deadline=1, sealed_attestations=True)
    engine.dispose()
    original_times = set()
    rows = market.read_legacy_attestations(read, params, database=engine, deadline=3,
                                          sealed_attestations=True, original_times=original_times)
    ids = {row["attestation_id"] for row in rows}
    assert len(ids) == len(rows) == len(fetched) == len(set(fetched)) == 2601
    assert original_times == {"2026-06-01T15:00:00", "2026-06-02T15:00:00"}
    fetched.clear()
    original_times.clear()
    earlier = {**params, "decision_known_at": datetime(2026, 6, 1, 23, 59, 59)}
    visible = market.read_legacy_attestations(read, earlier, database=engine, deadline=3,
                                             sealed_attestations=True, original_times=original_times)
    assert len(visible) == 2600 and not fetched
    assert original_times == {"2026-06-01T15:00:00", "2026-06-02T15:00:00"}


def test_invalid_hash_and_non_native_origin_are_not_admitted(evidence):
    engine, params = evidence
    with engine.begin() as c:
        c.execute(text("UPDATE qmt_kline_attestation_row SET attested_close='11' WHERE target_id=1"))
        c.execute(text("UPDATE qmt_kline_attestation_row SET source_pre_close_origin='PUBLIC' WHERE target_id=2"))
    rows = market.read_legacy_attestations(_read_rows, params, database=engine, deadline=time.monotonic() + 10)
    assert len(rows) == 2599
    assert not {'000001', '000002'} & {row['stock_code'] for row in rows}


# A rolling exchange calendar and real SQL rows exercise the production window
# entry point. Change only the calendar/SQL dialect and serialize the workers
# so that an interrupted partition and its committed predecessors are repeatable.
@pytest.fixture
def governance_window(evidence, monkeypatch):
    from server.common.qmt_trade_calendar import QmtTradeCalendarReceipt
    from server.engine import strategy_governance as governance

    engine, params = evidence
    protocol = params["protocol_version"]
    days = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04"]
    codes = ["000001", "000002"]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM qmt_kline_attestation_row"))
        conn.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, k_type INT, adjust_type INT,
            pre_close TEXT, open TEXT, close TEXT, high TEXT, low TEXT, volume INT, amount DECIMALTEXT,
            batch_id TEXT, data_version TEXT, received_at TEXT, source_time TEXT,
            data_source TEXT, quality_status TEXT, permission_status TEXT)"""))
        conn.execute(text("""CREATE TABLE qmt_kline_attestation_run (
            run_id TEXT, start_date TEXT, end_date TEXT, tolerance_json TEXT, finished_at TEXT,
            provider TEXT, status TEXT)"""))
        for day_index, day in enumerate(days):
            universe = governance.expected_stock_set_contract(day, codes)
            manifest = governance.build_qmt_v2_manifest({day: universe})
            conn.execute(text("""INSERT INTO qmt_kline_attestation_run VALUES (
                :day,:day,:day,:manifest,:completed,'gj_big_qmt_inner','COMPLETED')"""),
                {"day": day, "manifest": json.dumps(manifest), "completed": day + " 15:30:00"})
            for index, code in enumerate(codes, start=1):
                target = day_index * 2 + index
                row_id = hashlib.sha256("|".join(map(str, (
                    protocol, target, target, "v1", 10, 10, 10, 10, 10, 100, 1000,
                ))).encode()).hexdigest()
                conn.execute(text("""INSERT INTO sm_stock_kline VALUES (
                    :code,:day,1,0,'10','10','10','10','10',100,'1000',
                    :day,'v1',:received,:source,'gj_big_qmt_inner','QMT_ATTESTED','SUPPORTED')"""),
                    {"code": code, "day": day, "source": day + " 15:00:00", "received": day + " 15:01:00"})
                conn.execute(text("""INSERT INTO qmt_kline_attestation_row VALUES (
                    :id,:day,:code,:protocol,:target,:target,:day,'v1','NATIVE_QMT',:created,
                    '10','10','10','10','10','100','1000')"""),
                    {"id": row_id, "day": day, "code": code, "protocol": protocol,
                     "target": target, "created": day + " 15:02:00"})

    def calendar(*, end_date, required_sessions, decision_known_at):
        sessions = tuple(day for day in days if day <= end_date)[-required_sessions:]
        digest = governance._digest(sessions)
        return QmtTradeCalendarReceipt(
            batch_id=digest, source_batch_id=digest, known_at=end_date + " 15:00:00",
            start_date=sessions[0], end_date=end_date, session_count=len(sessions),
            session_set_hash=digest, manifest_hash=digest, sessions=sessions,
        )

    monkeypatch.setattr(governance, "WINDOWS", (3,))
    monkeypatch.setattr(governance, "_immutable_calendar_window_receipt", calendar)
    monkeypatch.setattr(governance, "get_engine", lambda: engine)
    monkeypatch.setattr(governance, "datadir_manifest_days", lambda *_a, **_kw: set())
    monkeypatch.setattr(governance, "read_close_batches",
                        lambda batches, read, *, deadline: ((batch, read(batch)) for batch in batches))
    monkeypatch.setenv("PROBIGA_GOVERNANCE_QMT_WINDOW_BATCH_DAYS", "1")
    reads = []
    failed_day = [None]
    read_exact = governance.read_legacy_attestations

    def trace_exact(read, params, **kwargs):
        day = params["start_date"]
        reads.append(day)
        if day == failed_day[0]:
            raise TimeoutError(f"QMT certification read deadline: {day}")
        return read_exact(read, params, **kwargs)

    monkeypatch.setattr(governance, "read_legacy_attestations", trace_exact)
    return SimpleNamespace(engine=engine, days=days, prove=governance._authoritative_session_windows_with_proof,
                           reads=reads, failed_day=failed_day)


def test_second_consumer_and_next_session_reuse_exact_day_proofs(governance_window):
    case = governance_window
    first, second, third, fourth = case.days
    first_window, first_proof = case.prove(third)
    assert case.reads == [first, second, third]
    assert [row["exact_attestation_stock_count"] for row in first_proof["sessions"]] == [2, 2, 2]
    case.reads.clear()
    assert case.prove(third) == (first_window, first_proof)
    assert case.reads == []
    next_window, next_proof = case.prove(fourth)
    assert case.reads == [fourth]
    assert next_proof["sessions"][:2] == first_proof["sessions"][1:]
    assert next_window[3]["sessions"] == [second, third, fourth]
    assert next_window[3]["calendar_manifest_hash"] != first_window[3]["calendar_manifest_hash"]


def test_revised_market_day_rechecked_with_unchanged_count_and_timestamps(governance_window):
    case = governance_window
    _, changed, end, _ = case.days
    case.prove(end)
    case.reads.clear()
    with case.engine.begin() as c:
        c.execute(text("UPDATE sm_stock_kline SET data_version='v2' "
                       "WHERE trade_date=:day AND stock_code='000001'"), {"day": changed})
    windows, _ = case.prove(end)
    assert case.reads == [changed]
    assert windows[3]["session_attestations"][1]["max_data_version"] == "v2"
    with case.engine.begin() as c:
        c.execute(text("UPDATE sm_stock_kline SET amount='999' "
                       "WHERE trade_date=:day AND stock_code='000001'"), {"day": changed})
    case.reads.clear()
    with pytest.raises(ValueError, match=changed):
        case.prove(end)
    assert case.reads == [changed]


def test_cached_late_attestation_rejected_at_old_decision_cutoff(governance_window):
    case = governance_window
    end = case.days[2]
    with case.engine.begin() as c:
        c.execute(text("UPDATE qmt_kline_attestation_row SET created_at=:late "
                       "WHERE trade_date=:day AND stock_code='000002'"), {"day": end, "late": end + " 17:00:00"})
    windows, proof = case.prove(end, decision_known_at=end + " 19:00:00")
    assert proof["sessions"][-1]["exact_attestation_stock_count"] == 2
    with pytest.raises(ValueError, match=end):
        case.prove(end, decision_known_at=end + " 16:00:00")
    assert case.prove(end, decision_known_at=end + " 19:00:00") == (windows, proof)


def test_interrupted_window_resumes_after_last_committed_day(governance_window, monkeypatch):
    monkeypatch.setenv("PROBIGA_GOVERNANCE_QMT_WINDOW_BATCH_DAYS", "3")
    case = governance_window
    first, second, third, _ = case.days
    case.failed_day[0] = third
    with pytest.raises(ValueError, match=third):
        case.prove(third)
    assert case.reads == [first, second, third]
    case.reads.clear()
    case.failed_day[0] = None
    windows, _ = case.prove(third)
    assert case.reads == [third]
    assert windows[3]["sessions"] == [first, second, third]


def _append_extra_security(conn, day, run, created):
    conn.execute(text("""INSERT INTO qmt_kline_attestation_row
            SELECT SHA2(CONCAT_WS('|', protocol_version,99,99,source_data_version,
                                   source_pre_close,attested_open,attested_close,attested_high,
                                   attested_low,attested_volume,attested_amount),256),
                   trade_date, '000003', protocol_version, 99, 99, :run, source_data_version,
                   source_pre_close_origin,:created,source_pre_close,attested_open,attested_close,
                   attested_high,attested_low,attested_volume,attested_amount
            FROM qmt_kline_attestation_row WHERE trade_date=:day AND stock_code='000001'"""), {"day": day, "run": run, "created": created})


def test_new_completed_attestation_version_is_checked_per_security(governance_window):
    case = governance_window
    _, changed, end, _ = case.days
    windows, _ = case.prove(end)
    assert windows[3]["session_attestations"][1]["expected_stock_count"] == 2
    case.reads.clear()
    # A newly completed run claims the same two-code universe, but contains
    # an unexpected third code. Its valid row hash cannot extend that universe.
    with case.engine.begin() as c:
        c.execute(text("""INSERT INTO qmt_kline_attestation_run
            SELECT 'revised', start_date, end_date, tolerance_json, finished_at, provider, status
            FROM qmt_kline_attestation_run WHERE run_id=:day"""), {"day": changed})
        _append_extra_security(c, changed, "revised", changed + " 15:03:00")
    with pytest.raises(ValueError, match=changed):
        case.prove(end)
    assert case.reads == [changed]


def test_changed_attestation_owner_invalidates_retained_proof_without_price_change(governance_window):
    case = governance_window
    _, changed, end, _ = case.days
    case.prove(end)
    with case.engine.begin() as conn:
        conn.execute(text("UPDATE qmt_kline_attestation_row SET run_id='unverified' "
                          "WHERE trade_date=:day AND stock_code='000001'"), {"day": changed})
    case.reads.clear()
    with pytest.raises(ValueError, match=changed):
        case.prove(end)
    assert case.reads == [changed]


def test_exact_decimal_revision_below_binary_float_precision_is_rejected(governance_window):
    case = governance_window
    _, changed, end, _ = case.days
    original, revised = "1000000000000.000001", "1000000000000.000002"
    assert Decimal(original) != Decimal(revised) and float(original) == float(revised)
    scope = " WHERE trade_date=:day AND stock_code='000001'"
    with case.engine.begin() as conn:
        conn.execute(text("UPDATE sm_stock_kline SET amount=:amount" + scope), {"day": changed, "amount": original})
        conn.execute(text("""UPDATE qmt_kline_attestation_row SET attested_amount=:amount,
            attestation_id=SHA2(CONCAT_WS('|',protocol_version,target_id,qmt_id,source_data_version,
                source_pre_close,attested_open,attested_close,attested_high,attested_low,attested_volume,:amount),256)"""
            + scope), {"day": changed, "amount": original})
    assert case.prove(end)[1]["sessions"][1]["exact_attestation_stock_count"] == 2
    case.reads.clear()
    with case.engine.begin() as conn:
        conn.execute(text("UPDATE sm_stock_kline SET amount=:amount" + scope), {"day": changed, "amount": revised})
    with pytest.raises(ValueError, match=changed):
        case.prove(end)
    assert case.reads == [changed]

def test_sealed_day_reused_within_interval_until_another_security_arrives(governance_window, monkeypatch):
    from server.engine import strategy_governance as governance

    case = governance_window
    end = case.days[2]
    with case.engine.begin() as conn:
        _append_extra_security(conn, end, end, end + " 17:00:00")
    _seal_rows(case.engine)
    monkeypatch.setattr(governance, "_sealed_governance_attestations", lambda *_a, **_kw: True)
    prior = case.prove(end, decision_known_at=end + " 16:00:00")
    case.reads.clear()
    assert case.prove(end, decision_known_at=end + " 16:59:59") == prior
    assert case.reads == []
    # No write is needed to cross this boundary. The new code's attestation
    # must invalidate the two-stock proof even with unchanged market/ID hashes.
    with pytest.raises(ValueError, match=end):
        case.prove(end, decision_known_at=end + " 17:00:00")
    assert case.reads == [end]
    case.reads.clear()
    assert case.prove(end, decision_known_at=end + " 16:45:00") == prior
    assert case.reads == []


def test_sealed_v1_page_recovers_exact_scale_or_rereads_ambiguous_decimal(evidence, monkeypatch):
    import re
    from server.common.formal_analysis_store import FormalAnalysisStore

    engine, params = evidence
    amounts = {"000001": "4294967295.999999", "000002": "1000000000000.000002"}
    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM qmt_kline_attestation_row WHERE target_id > 2")
        for code, amount in amounts.items():
            conn.execute(text("""UPDATE qmt_kline_attestation_row SET attested_amount=:amount,
                attestation_id=SHA2(CONCAT_WS('|',protocol_version,target_id,qmt_id,source_data_version,
                    source_pre_close,attested_open,attested_close,attested_high,attested_low,attested_volume,:amount),256)
                WHERE stock_code=:code"""), {"amount": amount, "code": code})
    exact = market.read_legacy_attestations(_read_rows, params, database=engine, deadline=time.monotonic() + 10)
    ids = sorted(row["attestation_id"] for row in exact)
    old_rows = [{**row, **{field: float(row[field]) for field in market._PRICE_FIELDS}} for row in exact]
    FormalAnalysisStore(engine).put(market._V1_MYSQL_PAGE_GENERATION, "governance_attestation_rows",
                                    market.canonical_digest(ids), {"ids": ids, "rows": old_rows})
    _seal_rows(engine)
    monkeypatch.setattr(engine.dialect, "name", "mysql")
    fetched = []

    def mysql_read(sql, args, *, database):
        sql = re.sub(r" FORCE INDEX \([^)]+\)", "", sql).replace("BINARY ", "")
        rows = _read_rows(sql, args, database=database)
        if "attestation_id in" in sql.lower():
            fetched.extend(row["stock_code"] for row in rows)
        return rows

    restored = market.read_legacy_attestations(mysql_read, params, database=engine, sealed_attestations=True,
                                              deadline=time.monotonic() + 10)
    assert fetched == ["000002"]
    assert {row["stock_code"]: row["attested_amount"] for row in restored} == amounts
