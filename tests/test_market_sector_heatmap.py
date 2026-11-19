from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import market_home
from server.common.sector_heatmap_store import (
    publish_concept_names,
    publish_industry_flow,
    read_industry_flow,
)


DAY = "2026-11-06"
NOW = datetime(2026, 11, 8, 9)
SESSIONS = ["2026-10-30", "2026-11-02", "2026-11-03", "2026-11-04", DAY]


def _identity(code, name):
    return {"index_code": code, "index_name": name, "source_snapshot_at": f"{DAY} 15:39:00",
            "etl_sync_at": "2026-11-07 09:00:00"}


def _flow(code, name, value):
    return {**_identity(code, name), "trade_date": DAY, "main_net_inflow": value,
            "max_net_inflow": value / 2, "lg_net_inflow": value / 2,
            "mid_net_inflow": 0, "sm_net_inflow": -value}


@pytest.fixture
def heatmap_db(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        for ddl in (
            "CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)",
            "CREATE TABLE sm_concept_east_kline (index_code TEXT, trade_date TEXT, trade_time TEXT, k_type INTEGER, "
            "change_pct REAL, close REAL, `change` REAL, etl_sync_at TEXT)",
            "CREATE TABLE sm_concept_east_minute (index_code TEXT, trade_date TEXT, trade_time TEXT, price REAL, etl_sync_at TEXT)",
            "CREATE TABLE sm_concept_capital_flow_east (index_code TEXT, index_name TEXT, days_type INTEGER, snapshot_at TEXT, "
            "main_net_inflow REAL, max_net_inflow REAL, lg_net_inflow REAL, etl_sync_at TEXT)",
            "CREATE TABLE st_hot_concept_ths_daily (plate_type INTEGER, concept_name TEXT, concept_code TEXT, snapshot_date TEXT, "
            "change_pct REAL, hot_tag TEXT, etl_sync_at TEXT)",
        ):
            connection.execute(text(ddl))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:day,1)"), [{"day": day} for day in SESSIONS])
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-01',0),('2026-11-07',0)"))
    monkeypatch.setattr(market_home, "get_engine", lambda: engine)
    monkeypatch.setattr(market_home, "get_kline_engine", lambda: engine)
    yield engine
    engine.dispose()


def _quote(connection, code, day, previous, close, *, marker="00:00:00"):
    connection.execute(text("""
        INSERT INTO sm_concept_east_kline VALUES (:code,:day,:stamp,1,:pct,:close,:change,:etl)
    """), {"code": code, "day": day, "stamp": f"{day} {marker}", "pct": (close / previous - 1) * 100,
           "close": close, "change": close - previous, "etl": f"{day} 16:00:00"})


def test_heatmap_ranks_full_sources_and_binds_each_metric_to_the_selected_session(heatmap_db):
    with heatmap_db.begin() as connection:
        for number in range(1, 12):
            _quote(connection, f"BK{number:04d}", DAY, 100, 100 - number)
        previous = 100
        for day, close in zip(SESSIONS, [110, 112, 114, 108, 120]):
            _quote(connection, "BK0120", day, previous, close)
            previous = close
        _quote(connection, "BK0999", DAY, 100, 150, marker="15:00:00")
        _quote(connection, "BK0998", "2026-11-04", 100, 150)
        _quote(connection, "BK0997", DAY, 100, 155)
        _quote(connection, "BK0997", DAY, 100, 160)

        for opened in (datetime(2026, 11, 6, 9, 30), datetime(2026, 11, 6, 13)):
            connection.execute(text("INSERT INTO sm_concept_east_minute VALUES ('BK0120',:day,:minute,120,:etl)"), [
                {"day": DAY, "minute": opened + timedelta(minutes=minute), "etl": f"{DAY} 16:00:00"}
                for minute in range(1, 121)
            ])
        connection.execute(text("""
            INSERT INTO sm_concept_capital_flow_east VALUES
                ('BK0120','同日概念名称',1,'2026-11-06 00:00:00',120,80,40,'2026-11-06 16:00:00'),
                ('BK0119','历史资金流',1,'2026-11-04 00:00:00',900,600,300,'2026-11-04 16:00:00'),
                ('BK0118','分量缺失',1,'2026-11-06 00:00:00',120,120,NULL,'2026-11-06 16:00:00'),
                ('BK0117','重复',1,'2026-11-06 00:00:00',120,80,40,'2026-11-06 16:00:00'),
                ('BK0117','重复',1,'2026-11-06 00:00:00',120,80,40,'2026-11-06 16:00:00')
        """))
        for day in SESSIONS:
            connection.execute(text("""
                INSERT INTO st_hot_concept_ths_daily VALUES
                    (3,'计算机','BK1201',:day,2,'东财一级行业成交额',:etl),
                    (4,'软件开发','BK1202',:day,5,'东财二级行业成交额',:etl),
                    (3,'合成一级','BK1203',:day,3,'东财一级行业成交额/3个二级',:etl)
            """), {"day": day, "etl": f"{day} 16:00:00"})
    publish_concept_names(DAY, [_identity("BK0120", "原生概念名称")])
    publish_industry_flow(DAY, [_flow("BK1201", "东财原生计算机", -120), _flow("BK1202", "软件开发", 900),
                                _flow("BK1203", "代表二级板块", 1000)])

    payload = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=NOW)
    assert payload["trade_date"] == payload["requested_date"] == DAY
    concept, industry = payload["concept"], payload["industry"]
    daily = concept["change"]["rows"]
    assert len(daily) == 7
    assert daily[0]["name"] == "原生概念名称"
    assert daily[0]["value"] == pytest.approx((120 / 108 - 1) * 100)
    assert daily[0]["series"] == [120] * 49 and daily[0]["series_label"] == "当日分时"
    assert [row["code"] for row in daily[1:]] == [f"BK{number:04d}" for number in range(11, 5, -1)]
    five_day = concept["five_day_return"]["rows"]
    assert [(row["code"], row["value"]) for row in five_day] == [("BK0120", 20.0)]
    assert five_day[0]["series"] == [0, 10, 12, 14, 8, 20]
    assert [(row["name"], row["value"]) for row in concept["net_inflow"]["rows"]] == [("原生概念名称", 120)]
    assert [row["name"] for row in industry["change"]["rows"]] == ["合成一级", "计算机"]
    assert industry["five_day_return"]["rows"][1]["value"] == pytest.approx((1.02 ** 5 - 1) * 100)
    assert [(row["name"], row["value"]) for row in industry["net_inflow"]["rows"]] == [("计算机", -120)]


def test_missing_or_nontrading_day_does_not_relabel_an_earlier_snapshot(heatmap_db):
    for day, now in [(date(2026, 11, 7), NOW), (date(2026, 11, 6), datetime(2026, 11, 6, 9))]:
        payload = market_home.build_sector_heatmap(day, now=now)
        assert payload["requested_date"] == day.isoformat()
        assert payload["trade_date"] is None
        assert all(metric["status"] == "unavailable" and not metric["rows"]
                   for section in (payload["concept"], payload["industry"]) for metric in section.values())


def test_historical_fund_flow_reports_verified_coverage(heatmap_db, monkeypatch):
    with heatmap_db.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_concept_capital_flow_east VALUES
                ('BK0120','历史概念名称',1,'2026-11-06 00:00:00',-120,-80,-40,'2026-11-08 08:00:00')
        """))
    observed = {}

    def coverage(day, now, codes, deadline):
        observed.update(day=day, codes=codes)
        return {"observed_count": 101, "eligible_count": 100, "excluded_count": 1,
                "source": "eastmoney.push2his"}

    monkeypatch.setattr(market_home, "_concept_flow_history_coverage", coverage)
    metric = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=NOW)["concept"]["net_inflow"]
    assert observed == {"day": DAY, "codes": {"BK0120"}}
    assert metric["status"] == "available"
    assert "100/101" in metric["note"] and "1 个未返回可核验数据" in metric["note"]
    assert metric["rows"][0]["source"] == "东财历史日资金流"
    assert metric["rows"][0]["value"] == -120


def test_private_flow_partition_is_idempotent_and_preserves_last_valid_publication(heatmap_db):
    first = publish_industry_flow(DAY, [_flow("BK1201", "计算机", 0)])
    reused = publish_industry_flow(DAY, [{**_flow("BK1201", "计算机", 0), "etl_sync_at": "2026-11-08 08:00:00"}])
    assert first["row_sha256"] == reused["row_sha256"]
    assert first["database_writes"] == 1 and reused["database_writes"] == 0
    with pytest.raises(ValueError, match="components differ"):
        publish_industry_flow(DAY, [{**_flow("BK1201", "计算机", 4), "lg_net_inflow": 1}])
    assert float(read_industry_flow(DAY, now=NOW)[0]["main_net_inflow"]) == 0
    assert read_industry_flow("2026-11-05", now=NOW) == []


def _live_row(code, name, stamp, pct=2):
    from zoneinfo import ZoneInfo
    return {"f12": code, "f14": name, "f124": int(stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()),
            "f2": 120, "f3": pct, "f4": 12, "f6": 1000,
            "f62": 120, "f66": 80, "f72": 40}


def test_intraday_endpoint_uses_live_observations_and_previous_four_closes(heatmap_db, monkeypatch):
    from server.common import sector_live
    now = datetime(2026, 11, 6, 10, 30)
    with heatmap_db.begin() as c:
        previous = 100
        for day, close in zip(SESSIONS[:-1], [110, 112, 114, 108]):
            _quote(c, "BK0120", day, previous, close)
            previous = close
    calls = []
    def capture(kind, selected, observed, deadline):
        calls.append(kind)
        return [_live_row("BK0120" if kind == 3 else "BK1201", "概念盘中" if kind == 3 else "计算机", now - timedelta(seconds=20))]
    monkeypatch.setattr(sector_live, "_fetch", capture)
    payload = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=now)
    assert payload["trade_date"] == DAY and payload["observation_mode"] == "intraday"
    assert payload["concept"]["change"]["rows"][0]["name"] == "概念盘中"
    assert payload["concept"]["change"]["as_of"] == f"{DAY} 10:29:40"
    assert payload["concept"]["five_day_return"]["rows"][0]["value"] == 20
    assert payload["concept"]["net_inflow"]["rows"][0]["value"] == 120
    assert payload["industry"]["change"]["rows"][0]["name"] == "计算机"
    assert payload["industry"]["net_inflow"]["rows"][0]["value"] == 120
    again = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=now + timedelta(seconds=10))
    assert again["concept"] == payload["concept"]
    assert sorted(calls) == [2, 3]  # Durable snapshot reused without new source capture.


@pytest.mark.parametrize("clock,source,available", [("10:30", "10:20", False), ("12:30", "11:29", True)])
def test_live_staleness_and_lunch_pause(heatmap_db, monkeypatch, clock, source, available):
    from server.common import sector_live
    now = datetime.fromisoformat(f"{DAY} {clock}")
    stamp = datetime.fromisoformat(f"{DAY} {source}")
    monkeypatch.setattr(sector_live, "_fetch", lambda kind, *args: [_live_row("BK1201", "计算机", stamp)])
    payload = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=now)
    assert (payload["concept"]["change"]["status"] == "available") == available
    if not available:
        assert payload["concept"]["change"]["rows"] == []
        assert "已过期" in payload["concept"]["change"]["note"]


def test_live_pagination_rejects_duplicates_and_partial_inventory(monkeypatch):
    from server.common import sector_live
    from tools import sync_eastmoney_concept_market as provider
    from time import monotonic
    now = datetime(2026, 11, 6, 10, 30)
    def request(url, params, **kwargs):
        rows = [_live_row(f"BK{i:04d}", str(i), now) for i in range(100)]
        if params["pn"] == 2:
            rows = rows[:1]  # Duplicate identity in another page.
        return {"rc": 0, "data": {"total": 101, "diff": rows}}
    monkeypatch.setattr(provider, "_resolved_curl_json", request)
    with pytest.raises(ValueError, match="identity"):
        sector_live._fetch(3, DAY, now, monotonic() + 10)


def test_live_source_failure_does_not_block_other_board_or_reuse_stale_rows(heatmap_db, monkeypatch):
    from server.common import sector_live
    now = datetime(2026, 11, 6, 10, 30)
    def capture(kind, *args):
        if kind == 3:
            raise RuntimeError("sector_live: source timed out")
        return [_live_row("BK1201", "计算机", now)]
    monkeypatch.setattr(sector_live, "_fetch", capture)
    result = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=now)
    assert result["concept"]["change"]["status"] == "unavailable"
    assert result["industry"]["change"]["status"] == "available"


def test_bulk_source_fields_supply_the_intraday_five_day_return(heatmap_db, monkeypatch):
    from tools import sync_eastmoney_concept_market as provider
    now = datetime(2026, 11, 6, 10, 30)
    with heatmap_db.begin() as c:
        previous = 100
        for day, close in zip(SESSIONS[:-1], [110, 112, 114, 108]):
            _quote(c, "BK0120", day, previous, close)
            previous = close
    def request(url, params, **kwargs):
        concept = params["fs"].endswith(":3")
        row = _live_row("BK0120" if concept else "BK1201", "概念" if concept else "计算机", now)
        returned = {key: value for key, value in row.items() if key in params["fields"].split(",")}
        return {"rc": 0, "data": {"total": 1, "diff": [returned]}}
    monkeypatch.setattr(provider, "_resolved_curl_json", request)
    payload = market_home.build_sector_heatmap(date.fromisoformat(DAY), now=now)
    assert payload["concept"]["five_day_return"]["status"] == "available"
    assert payload["concept"]["five_day_return"]["rows"][0]["value"] == 20
