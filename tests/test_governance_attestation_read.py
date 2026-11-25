from datetime import datetime
import hashlib
import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import governance_daily_market as market


@pytest.fixture
def evidence(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "evidence.db"))

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
            attested_high TEXT, attested_low TEXT, attested_volume TEXT, attested_amount TEXT)"""))
        c.execute(text("CREATE INDEX dated ON qmt_kline_attestation_row(trade_date,protocol_version,stock_code,attestation_id)"))
        c.execute(text("""INSERT INTO qmt_kline_attestation_row VALUES
            (:id,'2026-06-01',:code,:protocol,:target,:target,'completed',
             'version','NATIVE_QMT',:created,'10','10','10','10','10','100','1000')"""), [
            {"id": hashlib.sha256("|".join(map(str, (protocol, i, i, 'version', 10, 10, 10, 10, 10, 100, 1000))).encode()).hexdigest(),
             "code": f"00{i:04d}", "protocol": protocol, "target": i,
             "created": "2026-06-02 15:00:00" if i == 1536 else "2026-06-01 15:00:00"}
            for i in range(1, 1537)
        ])
    params = {"start_date": "2026-06-01", "as_of_date": "2026-06-01",
              "protocol_version": protocol, "decision_known_at": datetime(2026, 6, 3)}
    yield engine, params
    engine.dispose()


def test_bounded_primary_reads_pass_statement_budget_and_reuse_original_availability(evidence):
    engine, params = evidence
    body_sql = []

    def read(sql, params, *, database):
        with database.connect() as c:
            # Reproduce an interrupted all-row payload statement, while the
            # covering inventory and bounded primary reads fit the same budget.
            ticks = 0

            def budget():
                nonlocal ticks
                ticks += 1
                return ticks > (20 if "a.source_pre_close" in sql else 60)

            db = c.connection.driver_connection
            db.set_progress_handler(budget, 1000)
            try:
                rows = [dict(row) for row in c.execute(text(sql), params).mappings()]
                if "a.source_pre_close" in sql:
                    body_sql.append(sql)
                return rows
            finally:
                db.set_progress_handler(None, 0)

    rows = market.read_legacy_attestations(read, params, database=engine, deadline=time.monotonic() + 10)
    assert len(rows) == 1536
    unbounded = body_sql[0].split(" AND a.attestation_id IN (")[0]
    with pytest.raises(OperationalError, match="interrupted"):
        read(unbounded, params, database=engine)
    large_ids=[row["attestation_id"] for row in rows[:512]]
    placeholders=",".join(f":cold_{i}" for i in range(len(large_ids)))
    with pytest.raises(OperationalError, match="interrupted"):
        read(unbounded+f" AND a.attestation_id IN ({placeholders})",
             {**params,**{f"cold_{i}":value for i,value in enumerate(large_ids)}},database=engine)
    body_sql.clear()
    earlier = {**params, "decision_known_at": datetime(2026, 6, 1, 23, 59, 59)}
    rows = market.read_legacy_attestations(read, earlier, database=engine, deadline=time.monotonic() + 10)
    assert len(rows) == 1535 and body_sql == []


def test_deadline_resumes_completed_pages_without_rereading_them(evidence, monkeypatch):
    engine, params = evidence
    clock = [0]
    monkeypatch.setattr(market.time, "monotonic", lambda: clock[0])
    fetched = []

    def read(sql, params, *, database):
        with database.connect() as c:
            rows = [dict(row) for row in c.execute(text(sql), params).mappings()]
        if "a.source_pre_close" in sql:
            fetched.extend(row['attestation_id'] for row in rows)
            if len(fetched) == 1024:
                clock[0] = 2
        return rows

    with pytest.raises(TimeoutError, match="1024/1536; verified pages retained"):
        market.read_legacy_attestations(read, params, database=engine, deadline=1)
    rows = market.read_legacy_attestations(read, params, database=engine, deadline=3)
    assert len(rows) == len(fetched) == len(set(fetched)) == 1536


def test_inventory_keyset_reads_fit_a_budget_that_interrupts_the_old_range(evidence):
    engine,params=evidence
    work=[];limit=[None]
    def read(sql,params,*,database):
        with database.connect() as c:
            used=0
            def progress():
                nonlocal used
                used+=50
                return limit[0] is not None and used>limit[0]
            driver=c.connection.driver_connection
            driver.set_progress_handler(progress,50)
            try:return [dict(row) for row in c.execute(text(sql),params).mappings()]
            finally:
                driver.set_progress_handler(None,0)
                if 'a.source_pre_close' not in sql:work.append(used)
    old=("SELECT a.trade_date,a.attestation_id FROM qmt_kline_attestation_row a "
         "WHERE a.protocol_version=:protocol_version AND a.trade_date BETWEEN :start_date AND :as_of_date "
         f"AND {market.a_share_stock_code_sql('a.stock_code')} LIMIT 250001")
    assert len(read(old,params,database=engine))==1536
    old_cost=work.pop()
    assert len(market.read_legacy_attestations(read,params,database=engine,deadline=time.monotonic()+30))==1536
    assert work and max(work)<old_cost
    limit[0]=old_cost-50
    with pytest.raises(OperationalError,match='interrupted'):read(old,params,database=engine)
    assert len(market.read_legacy_attestations(read,params,database=engine,deadline=time.monotonic()+30))==1536


def test_invalid_hash_and_non_native_origin_are_not_admitted(evidence):
    engine, params = evidence
    with engine.begin() as c:
        c.execute(text("UPDATE qmt_kline_attestation_row SET attested_close='11' WHERE target_id=1"))
        c.execute(text("UPDATE qmt_kline_attestation_row SET source_pre_close_origin='PUBLIC' WHERE target_id=2"))

    def read(sql, params, *, database):
        with database.connect() as c:
            return [dict(row) for row in c.execute(text(sql), params).mappings()]

    rows = market.read_legacy_attestations(read, params, database=engine, deadline=time.monotonic() + 10)
    assert len(rows) == 1534 and not {'000001', '000002'} & {row['stock_code'] for row in rows}
