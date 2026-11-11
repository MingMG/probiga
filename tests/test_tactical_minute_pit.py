"""Cutoff reads can open selected verified minute rows without a market scan."""

from collections import defaultdict
import json

from sqlalchemy import create_engine, event, text

from biz.analysis.intraday_reversal import load_index_stability, load_intraday_evidence
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_minute_coverage import CORE_GRID, persisted_matrix
from server.common.qmt_datadir_result import persisted_fingerprint
from server.common.qmt_minute_publication_scope import seal_minute_security_publication


DAY = "2026-11-10"
INITIAL = "2026-11-10 20:00:00"
PREMARKET = "2026-11-11 08:30:00"
LATE = "2026-11-11 09:00:00"


def _create_receipt_tables(engine):
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_datadir_import_partition (
                dataset TEXT,trade_date TEXT,source_root TEXT,status TEXT,code_count INTEGER,
                row_count INTEGER,content_sha256 TEXT,run_id TEXT,started_at TEXT,finished_at TEXT,
                detail_json TEXT,PRIMARY KEY(dataset,trade_date)
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_history_coverage_manifest (
                trade_date TEXT, manifest_hash TEXT PRIMARY KEY, dataset TEXT, provider TEXT,
                schema_version TEXT, status TEXT, strategy_eligible INTEGER, captured_at TEXT,
                period TEXT, run_id TEXT, grid_profile TEXT, expected_entity_count INTEGER,
                entity_count INTEGER, expected_traded_count INTEGER, actual_traded_count INTEGER,
                bar_count INTEGER, expected_entity_set_hash TEXT, expected_traded_set_hash TEXT,
                actual_traded_set_hash TEXT, no_trade_set_hash TEXT, entity_root_hash TEXT,
                manifest_json TEXT, created_at TEXT
            )
        """))


def _publish(engine, *, dataset, fingerprint, run_id, known_at, seal=True):
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT OR REPLACE INTO qmt_datadir_import_partition
            VALUES (:dataset,:day,'QMT','COMPLETE',:codes,:rows,:raw_hash,:run,:at,:at,:detail)
        """), {
            "dataset": dataset, "day": DAY, "codes": fingerprint["code_count"],
            "rows": fingerprint["row_count"], "raw_hash": "b" * 64,
            "run": run_id, "at": known_at, "detail": json.dumps({
                "row_hash": fingerprint["row_hash"], "readback_rows": fingerprint["row_count"],
                "readback_codes": fingerprint["code_count"],
            }),
        })
        if seal:
            return seal_minute_security_publication(
                connection, dataset=dataset, trade_date=DAY, fingerprints=fingerprint,
            )


def _bars_fixture(engine, table, rows):
    definitions = []
    for key in rows[0]:
        sql_type = "REAL" if key in {"price", "avg_price", "change", "change_pct", "volume", "amount"} else "TEXT"
        definitions.append(chr(96) + key + chr(96) + " " + sql_type)
    with engine.begin() as connection:
        connection.execute(text(f"CREATE TABLE {table} ({','.join(definitions)})"))
        values = ",".join(f":{key}" for key in rows[0])
        connection.execute(text(f"INSERT INTO {table} VALUES ({values})"), rows)


def _bar(trade_clock):
    return {
        "trade_time": f"{DAY} {trade_clock}", "trade_date": DAY, "price": 10,
        "change": 0, "change_pct": 0, "volume": 100, "amount": 1000,
    }


def _stock_inputs(engine, codes, cutoff=PREMARKET, anchors=None):
    result = load_intraday_evidence(
        engine, trade_date=DAY, stock_codes=codes, decision_known_at=cutoff,
        daily_pre_close={code: 10 for code in codes} if anchors is None else anchors,
    )
    return {row["stock_code"]: json.loads(row["intraday_event_json"]) for row in result.to_dict("records")}


def test_stock_minutes_use_only_opened_securities_at_the_decision_cutoff(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path / "jobs"))
    engine = create_engine("sqlite://")
    _create_receipt_tables(engine)
    codes = ("000001", "600000")
    native = [
        {
            **_bar(moment), "stock_code": code, "qmt_code": f"{code}.{suffix}",
            "data_source": "qmt_datadir", "source_time": f"{DAY} {moment}",
            "batch_id": "native-run", "data_version": "qmt_datadir_v1",
            "quality_status": "VERIFIED", "permission_status": "SUPPORTED",
        }
        for code, suffix in zip(codes, ("SZ", "SH")) for moment in CORE_GRID
    ]
    _bars_fixture(engine, "sm_stock_minute", native)
    scope = {
        "codes": codes, "full_market_manifest_hash": "a" * 64,
        "daily_close_by_code": dict.fromkeys(codes, "10"),
        "eligible_code_count": len(codes), "expected_traded_code_set_sha256": canonical_digest(list(codes)),
    }

    def fingerprint():
        return persisted_matrix(engine, DAY, scope=scope, include_security_fingerprints=True)

    first = fingerprint()
    _publish(engine, dataset="stock_minute", fingerprint=first, run_id="legacy", known_at=INITIAL, seal=False)
    assert _stock_inputs(engine, codes)["000001"]["status"] == "DATA_BLOCKED"
    published = _publish(engine, dataset="stock_minute", fingerprint=first, run_id="initial", known_at=INITIAL)
    assert _stock_inputs(engine, codes, "2026-11-10 19:59:59")["000001"]["status"] == "DATA_BLOCKED"

    statements = defaultdict(list)

    @event.listens_for(engine, "before_cursor_execute")
    def track_rows(_connection, _cursor, statement, params, _context, _many):
        if "FROM " + chr(96) + "sm_stock_minute" + chr(96) in statement:
            statements["stock_minute"].append((statement, params))

    opened = _stock_inputs(engine, ("000001",))["000001"]
    assert opened["status"] == "AVAILABLE"
    assert opened["known_at"] == INITIAL
    assert opened["publication_manifest_sha256"] == published
    assert opened["source_row_sha256"] == first["security_fingerprints"]["000001"]["row_hash"]
    assert len(statements["stock_minute"]) == 1
    selected_sql, selected_params = statements["stock_minute"][0]
    assert "stock_code IN" in selected_sql and "LIMIT" in selected_sql
    assert "600000" not in selected_params

    assert _stock_inputs(engine, ("000001",), anchors={})["000001"]["status"] == "DATA_BLOCKED"
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE sm_stock_minute SET price=9
            WHERE stock_code='600000' AND trade_time=:stamp
        """), {"stamp": f"{DAY} 10:00:00"})
    newer = _publish(engine, dataset="stock_minute", fingerprint=fingerprint(), run_id="later", known_at=LATE)
    old_cutoff = _stock_inputs(engine, codes)
    assert old_cutoff["000001"]["publication_manifest_sha256"] == published
    assert old_cutoff["600000"]["status"] == "DATA_BLOCKED"
    late = _stock_inputs(engine, codes, LATE)
    assert late["600000"]["status"] == "AVAILABLE"
    assert late["600000"]["session_low_price"] == 9
    assert late["600000"]["publication_manifest_sha256"] == newer
    engine.dispose()


def test_index_minutes_require_the_same_receipt_before_cutoff_on_separate_history_db():
    primary = create_engine("sqlite://")
    history = create_engine("sqlite://")
    _create_receipt_tables(primary)
    codes = ("000001", "000300", "399001", "399006")
    _bars_fixture(history, "sm_index_minute", [
        {
            **_bar(moment), "index_code": code, "avg_price": None, "snapshot_at": f"{DAY} {moment}",
        }
        for code in codes for moment in CORE_GRID
    ])
    fingerprint = persisted_fingerprint(history, dataset="index_minute", trade_date=DAY, include_security_fingerprints=True)
    digest = _publish(primary, dataset="index_minute", fingerprint=fingerprint, run_id="indexes", known_at=INITIAL)

    def read(cutoff):
        return load_index_stability(primary, trade_date=DAY, decision_known_at=cutoff, history_engine=history)

    assert read("2026-11-10 19:59:59")["status"] == "DATA_BLOCKED"
    opened = read(PREMARKET)
    assert opened["status"] == "AVAILABLE"
    assert opened["stable_count"] == 3
    assert opened["publication_manifest_sha256"] == digest
    assert {row["index_code"] for row in opened["indices"]} == {"000001", "399001", "399006"}
    with history.begin() as connection:
        connection.execute(text("UPDATE sm_index_minute SET price=9 WHERE index_code='000300'"))
    assert read(PREMARKET) == opened
    with history.begin() as connection:
        connection.execute(text("UPDATE sm_index_minute SET price=9 WHERE index_code='399006'"))
    assert read(PREMARKET)["status"] == "DATA_BLOCKED"
    primary.dispose()
    history.dispose()
