from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from tools import fetch_sector_heat_east_daily as sector


TARGET_DATE = "2026-08-26"


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_hot_concept_ths_daily ("
                "snapshot_date TEXT NOT NULL, plate_type INTEGER NOT NULL, "
                "`rank` INTEGER NOT NULL, concept_code TEXT NOT NULL, "
                "concept_name TEXT NOT NULL, change_pct REAL, hot_value REAL, "
                "hot_tag TEXT NOT NULL, etl_sync_at TEXT)"
            )
        )
    return engine


def _complete_provider_rows(snapshot_date: str = TARGET_DATE):
    mapping = sector._industry_map()
    names = list(mapping) + [child for children in mapping.values() for child in children]
    unique_names = list(dict.fromkeys(names))
    return [
        {
            "rank": index,
            "concept_code": f"BK{index:06d}",
            "concept_name": name,
            "change_pct": round(0.1 + index / 1000, 4),
            "hot_value": float(1_000_000_000 - index * 10_000),
            "hot_tag": "东财成交额",
            "_trade_date_hint": snapshot_date,
        }
        for index, name in enumerate(unique_names, start=1)
    ]


def _receipt_hash(receipt):
    payload = dict(receipt)
    payload.pop("receipt_id")
    return sector._canonical_hash(payload)


def test_formal_sector_snapshot_is_complete_atomic_and_read_back(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", _complete_provider_rows)

    receipt = sector.fetch_sector_heat_east_daily(
        TARGET_DATE,
        formal=True,
        engine=engine,
        now=datetime(2026, 8, 26, 17, 8),
    )

    assert receipt["status"] == "PASS"
    assert receipt["published"] is True
    assert receipt["requested_date"] == TARGET_DATE
    assert receipt["data_date"] == TARGET_DATE
    assert receipt["evidence"]["l1_count"] == 31
    assert receipt["evidence"]["l2_count"] == 128
    assert receipt["evidence"]["row_count"] == 159
    assert receipt["evidence"]["coverage"] == 1.0
    assert len(receipt["evidence"]["row_hash"]) == 64
    assert receipt["receipt_id"] == _receipt_hash(receipt)
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_hot_concept_ths_daily")
        ).scalar_one() == 159


def test_formal_sector_rejects_cross_day_provider_snapshot_before_write(monkeypatch):
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_hot_concept_ths_daily VALUES "
                "(:d,3,1,'OLD','old',1,1,'old','old')"
            ),
            {"d": TARGET_DATE},
        )
    monkeypatch.setattr(
        sector,
        "_fetch_eastmoney_industries",
        lambda: _complete_provider_rows("2026-08-25"),
    )

    with pytest.raises(sector.SectorHeatContractError, match="differs from target"):
        sector.fetch_sector_heat_east_daily(
            TARGET_DATE,
            formal=True,
            engine=engine,
            now=datetime(2026, 8, 26, 17, 8),
        )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT concept_code FROM st_hot_concept_ths_daily")
        ).scalars().all()
    assert rows == ["OLD"]


def test_formal_sector_rejects_any_unmatched_fixed_industry(monkeypatch):
    engine = _engine()
    rows = _complete_provider_rows()
    mapping = sector._industry_map()
    missing_child = next(iter(next(iter(mapping.values()))))
    rows = [row for row in rows if row["concept_name"] != missing_child]
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: rows)

    with pytest.raises(sector.SectorHeatContractError, match="lacks provider evidence"):
        sector.fetch_sector_heat_east_daily(
            TARGET_DATE,
            formal=True,
            engine=engine,
            now=datetime(2026, 8, 26, 17, 8),
        )

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_hot_concept_ths_daily")
        ).scalar_one() == 0


def test_formal_sector_readback_failure_rolls_back_replace(monkeypatch):
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_hot_concept_ths_daily VALUES "
                "(:d,3,1,'OLD','old',1,1,'old','old')"
            ),
            {"d": TARGET_DATE},
        )
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", _complete_provider_rows)
    monkeypatch.setattr(sector, "_select_sector_rows", lambda *_args, **_kwargs: [])

    with pytest.raises(sector.SectorHeatContractError, match="empty"):
        sector.fetch_sector_heat_east_daily(
            TARGET_DATE,
            formal=True,
            engine=engine,
            now=datetime(2026, 8, 26, 17, 8),
        )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT concept_code FROM st_hot_concept_ths_daily")
        ).scalars().all()
    assert rows == ["OLD"]


def test_cache_is_only_an_explicit_non_publishing_diagnostic(monkeypatch):
    cached = {
        "snapshot_date": "2026-08-25",
        "requested_date": TARGET_DATE,
        "db_rows": [
            {
                "snapshot_date": "2026-08-25",
                "plate_type": 3,
                "rank": 1,
                "concept_code": "CACHE",
                "concept_name": "cached",
                "change_pct": 1,
                "hot_value": 1,
                "hot_tag": "cached",
            }
        ],
    }
    monkeypatch.setattr(
        sector,
        "_fetch_eastmoney_industries",
        lambda: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    monkeypatch.setattr(sector, "_load_cached_rows", lambda: cached)
    monkeypatch.setattr(
        sector,
        "_cache_rows",
        lambda *_args, **_kwargs: pytest.fail("cache fallback must not be republished"),
    )

    receipt = sector.fetch_sector_heat_east_daily(
        TARGET_DATE,
        dry_run=True,
        diagnostic_cache=True,
        engine=object(),
        now=datetime(2026, 8, 26, 17, 8),
    )

    assert receipt["status"] == "DIAGNOSTIC_CACHE"
    assert receipt["published"] is False
    assert receipt["data_date"] == "2026-08-25"
    with pytest.raises(sector.SectorHeatContractError, match="only with --dry-run"):
        sector.fetch_sector_heat_east_daily(
            TARGET_DATE,
            formal=True,
            dry_run=True,
            diagnostic_cache=True,
        )


def test_formal_target_date_uses_open_exchange_calendar():
    engine = create_engine("sqlite://", poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)")
        )
        connection.execute(
            text("INSERT INTO si_trade_calendar VALUES ('2026-08-25',1),('2026-08-26',0)")
        )
    assert sector.resolve_formal_sector_target_date(
        engine,
        now=datetime(2026, 8, 26, 17, 8),
    ) == "2026-08-25"


def test_formal_target_rolls_at_provider_close_cutoff():
    engine = create_engine("sqlite://", poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)")
        )
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar VALUES "
                "('2026-08-25',1),('2026-08-26',1)"
            )
        )

    assert sector.resolve_formal_sector_target_date(
        engine,
        now=datetime(2026, 8, 26, 15, 9),
    ) == "2026-08-25"
    assert sector.resolve_formal_sector_target_date(
        engine,
        now=datetime(2026, 8, 26, 15, 10),
    ) == "2026-08-26"


def test_formal_cli_failure_is_one_hashed_json_and_nonzero(monkeypatch, capsys):
    monkeypatch.setattr(
        sector,
        "fetch_sector_heat_east_daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider down")),
    )

    exit_code = sector.main([TARGET_DATE, "--formal", "--json"])

    lines = capsys.readouterr().out.strip().splitlines()
    assert exit_code == 1
    assert len(lines) == 1
    receipt = json.loads(lines[0])
    assert receipt["status"] == "FAILED"
    assert receipt["published"] is False
    assert receipt["receipt_id"] == _receipt_hash(receipt)


def test_formal_cli_without_date_resolves_calendar_target(monkeypatch, capsys):
    engine = object()
    observed = {}
    monkeypatch.setattr(sector, "resolve_tool_mysql_url", lambda: "mysql://formal-test")
    monkeypatch.setattr(sector, "create_tool_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(
        sector,
        "resolve_formal_sector_target_date",
        lambda resolved_engine, **_kwargs: (
            observed.__setitem__("resolved_engine", resolved_engine) or TARGET_DATE
        ),
    )

    def fake_fetch(snapshot_date, **kwargs):
        observed["snapshot_date"] = snapshot_date
        observed["fetch_engine"] = kwargs["engine"]
        observed["formal"] = kwargs["formal"]
        return sector._formal_sector_receipt(
            status="PASS",
            requested_date=snapshot_date,
            data_date=snapshot_date,
            started_at=kwargs["now"],
            finished_at=kwargs["now"],
            published=True,
            evidence={"row_count": 159, "row_hash": "a" * 64},
        )

    monkeypatch.setattr(sector, "fetch_sector_heat_east_daily", fake_fetch)

    exit_code = sector.main(["--formal", "--json"])

    receipt = json.loads(capsys.readouterr().out.strip())
    assert exit_code == 0
    assert receipt["status"] == "PASS"
    assert observed == {
        "resolved_engine": engine,
        "snapshot_date": TARGET_DATE,
        "fetch_engine": engine,
        "formal": True,
    }
