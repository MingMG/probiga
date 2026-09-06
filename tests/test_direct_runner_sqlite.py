"""Provider -> raw file -> real SQLAlchemy transaction -> restart, offline only."""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import Column, Date, MetaData, String, Table, create_engine, select

from acquisition.config import Config
from acquisition.models import WorkUnit
from acquisition.runner import Runner

NOW = datetime(2026, 9, 4, 16, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


class Provider:
    calls = 0

    def fetch_batch(self, dataset, request):
        self.calls += 1
        return {"request": request, "source_method": "eastmoney.RPT_DAILYBILLBOARD_DETAILSNEW", "received_at": NOW.isoformat(),
                "outcomes": {code: {"status": "data", "rows": [dict(
                    stock_code=code.split(".")[0], trade_date="2026-09-04", trade_id="T1")]} for code in request["codes"]}}


def environment(tmp_path):
    engine = create_engine("sqlite:///:memory:")
    md = MetaData()
    catalog = Table("si_all_code", md, Column("stock_code", String, primary_key=True), Column("exchange", String))
    event = Table("st_a_list_daily", md, Column("stock_code", String, primary_key=True),
                  Column("trade_date", Date, primary_key=True), Column("trade_id", String, primary_key=True))
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(catalog.insert().values(stock_code="000001", exchange="SZ"))
    config = Config({"state_dir": str(tmp_path), "write_enabled": True, "start_date": "2026-09-04"}, tmp_path / "config.json")
    provider = Provider()
    runner = Runner(config, engines={"primary": engine}, provider=provider, clock=lambda: NOW)
    runner.store("primary").prepare_progress_schema()
    return runner, engine, event, provider


def test_database_failure_replays_durable_raw_without_fetching_provider_again(tmp_path, monkeypatch):
    runner, engine, table, provider = environment(tmp_path)
    store = runner.store("primary")
    unit = WorkUnit("alist_daily", "eastmoney", "2026-09-04", "000001.SZ")
    original = store.commit
    with monkeypatch.context() as patch:
        def unavailable(*args):
            raise ConnectionError("isolated transient fixture")
        patch.setattr(store, "commit", unavailable)
        with pytest.raises(ConnectionError):
            runner.acquire([unit], 180)
    assert provider.calls == 1
    assert store.states("alist_daily")[0]["status"] == "running"
    assert len(list((tmp_path / "http").glob("*.ready.json"))) == 1
    runner.recover_http()
    assert runner.errors == []
    assert provider.calls == 1
    assert store.states("alist_daily")[0]["status"] == "complete"
    with engine.connect() as conn:
        rows = conn.execute(select(table)).mappings().all()
    assert len(rows) == 1 and rows[0]["trade_id"] == "T1"
    assert not list((tmp_path / "http").glob("*.ready.json"))
    runner.recover_http()
    assert provider.calls == 1
