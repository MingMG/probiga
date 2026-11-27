from datetime import datetime

import pytest
from fastapi.encoders import jsonable_encoder
from sqlalchemy import create_engine, event, text
from starlette.responses import JSONResponse

from integrations.qmt.datadir_import import Universe, UniverseMember
from server.api.routers import hot_data
from server.common import qmt_datadir_daily_coverage as coverage
from server.common.qmt_history_coverage import coverage_table_ddl_statements
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_qmt_datadir_daily_coverage import CUTOFF, DAY, IPO, SUSPENDED, TRADED, authorities, bar, listing, snapshot
from tests.test_qmt_datadir_import import _epoch, _record, _write
from tools import import_qmt_datadir_daily as daily


@pytest.mark.parametrize("stored", [
    '{"event_coverage_id":NaN,"event_coverage_response_hash":NaN,"evidence":[0,"published"]}',
    {"event_coverage_id": float("nan"), "event_coverage_response_hash": float("nan"), "evidence": [0, "published"]},
])
def test_stock_detail_serializes_missing_event_evidence_as_null(monkeypatch, stored):
    monkeypatch.setattr(hot_data, "_table_columns", lambda *args: set())
    monkeypatch.setattr(hot_data, "_read_sql", lambda *args: [{"event_risk_detail": stored}])
    snapshot = hot_data._load_latest_analysis_snapshot(TRADED, trade_date=DAY)
    assert snapshot["event_risk_detail"] == {
        "event_coverage_id": None, "event_coverage_response_hash": None, "evidence": [0, "published"],
    }
    assert JSONResponse(jsonable_encoder({"analysis_snapshot": snapshot})).status_code == 200


def _promote_daily(engine, *, full, harvested, root, finished):
    day = full["trade_date"]
    with engine.begin() as connection:
        connection.connection.driver_connection.create_function("current_timestamp", 0, lambda: finished)
        connection.execute(text("""INSERT INTO sm_stock_kline
            (stock_code,trade_time,trade_date,open,close,high,low,volume,amount,pre_close,
             data_source,k_type,adjust_type,quality_status,permission_status,`change`,change_pct,
             qmt_code,source_time,received_at,batch_id,data_version)
            VALUES (:stock_code,:event_at,:trade_date,:open,:close,:high,:low,:volume,:amount,:pre_close,
                    'qmt_datadir',1,0,'VERIFIED','SUPPORTED',:change,:change_pct,:symbol,:event_at,:known,:batch,'qmt_datadir_v1')
        """), [{**{k: str(v) for k, v in row.items()}, "symbol": row["stock_code"] + ".SH",
                "event_at": day + " 15:00:00", "known": finished, "batch": full["run_id"]}
               for row in daily._harvested_daily_rows(harvested)])
        fingerprint = daily._completion_fingerprint(connection, day=day, proof=full)
        coverage.insert_manifest(connection, full)
        daily.write_receipt(connection, dataset="stock_daily", session=datetime.fromisoformat(day).date(),
                            root=str(root), status="COMPLETE", run_id=full["run_id"],
                            row_count=fingerprint["row_count"], code_count=fingerprint["code_count"],
                            content_sha256=harvested["digest"], finished=True,
                            detail={"full_market_manifest_hash": full["manifest_hash"], "row_hash": fingerprint["row_hash"],
                                    "coverage": 1.0, "eligible_code_count": len(full["entities"]),
                                    "readback_rows": fingerprint["row_count"], "readback_codes": fingerprint["code_count"]})
        connection.execute(text("DELETE FROM qmt_datadir_import_partition WHERE dataset=:attempt AND trade_date=:day"),
                           {"attempt": daily.ATTEMPT_DATASET, "day": day})


def test_daily_close_reaches_stock_detail_and_holdings_through_exact_publication(monkeypatch, tmp_path, caplog):
    known_at = DAY + " 20:01:00"
    engine = create_engine("sqlite://")
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as connection:
        connection.connection.driver_connection.create_function("NOW", 0, lambda: known_at)
        for ddl in coverage_table_ddl_statements():
            # Keep the published column/PK/FK definitions, omit MySQL secondary-index syntax.
            connection.exec_driver_sql(ddl.split(",\n            KEY ", 1)[0] + ")")
        connection.exec_driver_sql("""CREATE TABLE sm_stock_kline (
            stock_code TEXT,trade_time TEXT,trade_date TEXT,open REAL,close REAL,high REAL,low REAL,
            volume REAL,amount REAL,pre_close REAL,data_source TEXT,k_type INT,adjust_type INT,
            quality_status TEXT,permission_status TEXT,`change` REAL,change_pct REAL,qmt_code TEXT,
            source_time TEXT,received_at TEXT,batch_id TEXT,data_version TEXT,
            PRIMARY KEY(stock_code,trade_date,k_type,adjust_type))""")
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT,trade_date DATE,source_root TEXT,status TEXT,code_count INT,row_count INT,
            content_sha256 TEXT,run_id TEXT,started_at TEXT,finished_at TEXT,detail_json TEXT,
            PRIMARY KEY(dataset,trade_date))"""))
    monkeypatch.setattr(daily, "_daily_transaction", lambda db: db.begin())
    catalog, calendar = authorities()
    day = datetime.fromisoformat(DAY).date()
    other = "600001"
    eligible = [*catalog.eligible_codes(DAY), other]
    catalog.eligible_codes = lambda target: sorted(eligible)
    members = {code+".SH": UniverseMember(code+".SH", "SH", code) for code in (TRADED, other)}
    universe = Universe(members, {symbol: (DAY, None) for symbol in members})
    record = _record("15:00", 10000, 11000, 10000, 11000, 1, 1100, 10000)
    record[0] = _epoch("15:00", day)
    for code in (TRADED, other):
        _write(tmp_path / "SH" / daily.DAILY_PERIOD / (code + ".DAT"), [record])
    harvested = daily._harvest(tmp_path, universe, day, isolate_invalid=True)
    proof = coverage.build_manifest(catalog=catalog, calendar=calendar, day=DAY,
                                    rows=daily._harvested_daily_rows(harvested),
                                    run_id="test-display", captured_at=CUTOFF, snapshot=snapshot(),
                                    listings={}, allow_partial=True)
    assert proof["status"] == "PARTIAL"
    daily._save_daily_attempt(engine, proof=proof, source_root=tmp_path, harvested=harvested,
                              previous_run=(None, None))
    monkeypatch.setattr(hot_data, "get_engine", lambda: engine)
    monkeypatch.setattr(hot_data, "_portfolio_market_mode", lambda: "post_close")
    monkeypatch.setattr(hot_data, "_stock_detail_portfolio_context", lambda code: {})
    old = {"stock_code": TRADED, "market": {"price": 8}, "requested_trade_date": DAY,
           "quote_trade_date": "2026-10-30", "quote_source": "snapshot", "analysis_trade_date": "2026-10-30"}
    monkeypatch.setattr(hot_data, "_cache_get", lambda *args, **kw: old)

    class Clock(datetime):
        moment = DAY + " 20:00:59"

        @classmethod
        def now(cls, tz=None):
            return cls.fromisoformat(cls.moment).replace(tzinfo=tz)

    monkeypatch.setattr(hot_data, "datetime", Clock)
    assert "verified_daily_bar" not in hot_data.stock_detail(TRADED)
    Clock.moment = known_at
    result = hot_data.stock_detail(TRADED)
    observed = result["verified_daily_bar"]
    assert observed == {**coverage.canonical_bar(bar()), "data_source": "qmt_datadir",
                        "known_at": known_at, "change_pct": 10.0}
    assert result["market"] == old["market"] and result["analysis_trade_date"] == old["analysis_trade_date"]
    assert "verified_daily_bar" not in old

    def position(code, date, **kw):
        return {"stock_code": code, "cur_price": 8, "quote_trade_date": date, **kw}

    rows = [position(TRADED, DAY, quote_snapshot_at=DAY + " 10:00:00"),
            position(IPO, ""), position(SUSPENDED[0], "")]
    hot_data._attach_verified_display_bars(rows, DAY)
    assert rows[0]["verified_daily_bar"] == observed and rows[0]["cur_price"] == 8
    assert all("verified_daily_bar" not in row for row in rows[1:])

    full = coverage.build_manifest(catalog=catalog, calendar=calendar, day=DAY, rows=daily._harvested_daily_rows(harvested),
                                   run_id="daily-display-exact", captured_at=DAY + " 20:02:00",
                                   snapshot=snapshot(), listings=listing())
    known_at = DAY + " 20:03:00"
    _promote_daily(engine, full=full, harvested=harvested, root=tmp_path, finished=known_at)
    assert daily.load_receipt(engine, dataset=daily.ATTEMPT_DATASET, trade_date=DAY) is None
    Clock.moment = DAY + " 20:02:59"
    assert "verified_daily_bar" not in hot_data.stock_detail(TRADED)
    Clock.moment = known_at
    queries = []
    event.listen(engine, "before_cursor_execute", lambda _, __, sql, *args: queries.append(sql.lower()))
    exact = hot_data.stock_detail(TRADED)
    assert exact["verified_daily_bar"] == {**observed, "known_at": known_at}
    assert exact["market"] == old["market"] and exact["analysis_trade_date"] == old["analysis_trade_date"]
    holdings = [position(code, "", pnl=0) for code in (TRADED, IPO, SUSPENDED[0])]
    hot_data._attach_verified_display_bars(holdings, DAY)
    assert holdings[0]["verified_daily_bar"] == exact["verified_daily_bar"]
    assert all(row["pnl"] == 0 and row["cur_price"] == 8 for row in holdings)
    assert all("verified_daily_bar" not in row for row in holdings[1:])
    assert not any("order by stock_code" in sql and "stock_code in" not in sql for sql in queries)
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET amount=amount+1 WHERE stock_code=:c"), {"c": other})
    pair = [position(code, "") for code in (other, TRADED)]
    hot_data._attach_verified_display_bars(pair, DAY)
    assert "verified_daily_bar" not in pair[0] and pair[1]["verified_daily_bar"] == exact["verified_daily_bar"]
    assert other in caplog.text
    with engine.begin() as connection:
        connection.execute(text("UPDATE qmt_datadir_import_partition SET run_id='mismatched' WHERE dataset='stock_daily'"))
    batch = [position(code, "") for code in (TRADED, other)]
    hot_data._attach_verified_display_bars(batch, DAY)
    assert all("verified_daily_bar" not in row and row["cur_price"] == 8 for row in batch)
    closed = position(TRADED, DAY, quote_status="closed", quote_snapshot_at=DAY + " 15:00:00")
    monkeypatch.setattr(hot_data, "get_engine", lambda: pytest.fail("unexpected read"))
    hot_data._attach_verified_display_bars([closed], DAY)
    assert "verified_daily_bar" not in closed
    assert hot_data._stock_detail_display(old, "intraday") is old


@pytest.mark.parametrize("error", [KeyError("bar_group_artifacts"), TypeError("unresolved_codes"),
                                   ArithmeticError("invalid pre_close")])
def test_corrupt_optional_evidence_preserves_stock_detail_and_holdings(monkeypatch, error):
    def corrupt(*args, **kwargs):
        raise error

    monkeypatch.setattr(hot_data, "get_engine", object)
    monkeypatch.setattr(hot_data, "read_verified_daily_display_bars", corrupt)
    records = []
    monkeypatch.setattr(hot_data, "_record_fallback", lambda stage, exc: records.append((stage, exc)))
    cached = {"stock_code": TRADED, "market": {"price": 8}, "quote_trade_date": "2026-10-30",
              "requested_trade_date": DAY}
    monkeypatch.setattr(hot_data, "_portfolio_market_mode", lambda: "post_close")
    monkeypatch.setattr(hot_data, "_stock_detail_portfolio_context", lambda code: {})
    monkeypatch.setattr(hot_data, "_cache_get", lambda *args, **kw: cached)
    detail = hot_data.stock_detail(TRADED)
    assert detail["market"] == {"price": 8} and detail["quote_is_stale"]
    assert "verified_daily_bar" not in detail
    holding = {"stock_code": TRADED, "cur_price": 8, "quote_trade_date": "2026-10-30"}
    hot_data._attach_verified_display_bars([holding], DAY)
    assert holding == {"stock_code": TRADED, "cur_price": 8, "quote_trade_date": "2026-10-30"}
    assert records == [("verified_daily_display", error)] * 2
