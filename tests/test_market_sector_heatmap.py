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
    for day, now in [(date(2026, 11, 7), NOW), (date(2026, 11, 6), datetime(2026, 11, 6, 13))]:
        payload = market_home.build_sector_heatmap(day, now=now)
        assert payload["requested_date"] == day.isoformat()
        assert payload["trade_date"] is None
        assert all(metric["status"] == "unavailable" and not metric["rows"]
                   for section in (payload["concept"], payload["industry"]) for metric in section.values())


def test_private_flow_partition_is_idempotent_and_preserves_last_valid_publication(heatmap_db):
    first = publish_industry_flow(DAY, [_flow("BK1201", "计算机", 0)])
    reused = publish_industry_flow(DAY, [{**_flow("BK1201", "计算机", 0), "etl_sync_at": "2026-11-08 08:00:00"}])
    assert first["row_sha256"] == reused["row_sha256"]
    assert first["database_writes"] == 1 and reused["database_writes"] == 0
    with pytest.raises(ValueError, match="components differ"):
        publish_industry_flow(DAY, [{**_flow("BK1201", "计算机", 4), "lg_net_inflow": 1}])
    assert float(read_industry_flow(DAY, now=NOW)[0]["main_net_inflow"]) == 0
    assert read_industry_flow("2026-11-05", now=NOW) == []
