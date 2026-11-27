from datetime import datetime

import pytest
from fastapi.encoders import jsonable_encoder
from sqlalchemy import create_engine, event, text
from starlette.responses import JSONResponse

from integrations.qmt.datadir_import import Universe, UniverseMember
from server.api.routers import hot_data
from server.common import qmt_datadir_daily_coverage as coverage
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_qmt_datadir_daily_coverage import CUTOFF, DAY, IPO, SUSPENDED, TRADED, authorities, bar, snapshot
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


def test_partial_daily_bar_reaches_stock_detail_and_portfolio_only_after_persistence(monkeypatch, tmp_path):
    known_at = DAY + " 20:01:00"
    engine = create_engine("sqlite://")
    event.listen(engine, "connect", lambda db, _: db.create_function("NOW", 0, lambda: known_at))
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT,trade_date DATE,source_root TEXT,status TEXT,code_count INT,row_count INT,
            content_sha256 TEXT,run_id TEXT,started_at TEXT,finished_at TEXT,detail_json TEXT,
            PRIMARY KEY(dataset,trade_date))"""))
    monkeypatch.setattr(daily, "_daily_transaction", lambda db: db.begin())
    catalog, calendar = authorities()
    day = datetime.fromisoformat(DAY).date()
    symbol = TRADED + ".SH"
    universe = Universe({symbol: UniverseMember(symbol, "SH", TRADED)}, {symbol: (DAY, None)})
    record = _record("15:00", 10000, 11000, 10000, 11000, 1, 1100, 10000)
    record[0] = _epoch("15:00", day)
    _write(tmp_path / "SH" / daily.DAILY_PERIOD / (TRADED + ".DAT"), [record])
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
    monkeypatch.setattr(hot_data, "read_verified_attempt_bars", corrupt)
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
