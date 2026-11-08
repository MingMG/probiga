from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from tools import fetch_sector_heat_east_daily as sector


TARGET_DATE = "2026-08-26"


def test_provider_dns_reset_uses_proven_edge_without_changing_source(monkeypatch):
    from tools import sync_eastmoney_concept_market as provider
    seen = {}
    def edge(url, params, *, host, timeout):
        seen.update(url=url, params=params, host=host, timeout=timeout)
        # The industry response that succeeded through the verified edge in
        # the production canary; the DNS route reset before returning JSON.
        return {"rc": 0, "data": {"total": 1, "diff": [{"f12": "BK1201", "f14": "电子",
                "f6": 396532043173.0, "f3": 2.72, "f124": 1793605170}]}}
    monkeypatch.setattr(provider, "_resolved_curl_json", edge)
    capture = sector._fetch_eastmoney_industries()
    assert capture.pagination_complete
    rows = capture.heat_rows
    assert len(rows) == 1 and rows[0]["concept_code"] == "BK1201"
    assert rows[0]["_trade_date_hint"] == "2026-11-02"
    assert seen["host"] == "push2delay.eastmoney.com" and seen["params"]["fs"] == "m:90+t:2"


def test_historical_industry_rows_do_not_inherit_previous_quotes_or_current_turnover(monkeypatch):
    quotes = {
        "BK1001": [{"trade_date": "2026-08-25", "change_pct": 1, "amount": 50}],
        "BK1002": [{"trade_date": TARGET_DATE, "change_pct": 1, "amount": None, "volume": 60}],
        "BK1003": [{"trade_date": TARGET_DATE, "change_pct": 0, "amount": 0}],
    }
    monkeypatch.setattr(sector, "_fetch_industry_kline", lambda code, day: quotes[code])
    rows = [{"concept_code": code, "hot_value": 1_000} for code in quotes]
    assert sector._apply_historical_values(rows, TARGET_DATE) == [
        {"concept_code": "BK1003", "rank": 1, "change_pct": 0, "hot_value": 0.0},
    ]


@pytest.mark.parametrize("url", [
    "http://push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2delay.eastmoney.com.attacker.test/api/qt/clist/get",
    "https://user:password@push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2delay.eastmoney.com/api/other",
])
def test_sector_source_identity_cannot_be_changed(url):
    with pytest.raises(sector.SectorHeatContractError, match="endpoint identity"):
        sector._fetch_json(url)


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)")
        )
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:d,1)"), {"d": TARGET_DATE})
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
            "concept_code": f"BK{index:04d}",
            "concept_name": name,
            "change_pct": round(0.1 + index / 1000, 4),
            "hot_value": float(1_000_000_000 - index * 10_000),
            "hot_tag": "东财成交额",
            "_trade_date_hint": snapshot_date,
        }
        for index, name in enumerate(unique_names, start=1)
    ]


def _capture(heat_rows=None, *, provider_rows=None, complete=False):
    heat_rows = _complete_provider_rows() if heat_rows is None else heat_rows
    rows = [] if provider_rows is None else provider_rows
    return sector.IndustryCapture(heat_rows, rows, len(rows), 1, complete)


def _native_rows(snapshot_date=TARGET_DATE):
    stamp = int(datetime.fromisoformat(f"{snapshot_date}T15:39:30").replace(
        tzinfo=PRODUCTION_TIMEZONE).timestamp())
    return [
        {"f12": "BK1201", "f14": "电子", "f124": stamp, "f62": -13864036864.0,
         "f66": -6100863488.0, "f72": -7763173376.0, "f78": -414842880.0,
         "f84": 14189051904.0},
        {"f12": "BK1036", "f14": "半导体", "f124": stamp, "f62": -4629633024.0,
         "f66": -2001957376.0, "f72": -2627675648.0, "f78": -757641216.0,
         "f84": 5331265792.0},
    ]


def _patch_store(monkeypatch, publisher):
    monkeypatch.setitem(sys.modules, "server.common.sector_heatmap_store", SimpleNamespace(
        publish_industry_flow=publisher,
    ))


def _receipt_hash(receipt):
    payload = dict(receipt)
    payload.pop("receipt_id")
    return sector._canonical_hash(payload)


def test_formal_sector_snapshot_is_complete_atomic_and_read_back(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", _capture)

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
    assert receipt["industry_flow"]["status"] == "UNAVAILABLE"
    assert receipt["industry_flow"]["reason_code"] == "NATIVE_PAGINATION_INCOMPLETE"
    assert len(receipt["evidence"]["row_hash"]) == 64
    assert receipt["receipt_id"] == _receipt_hash(receipt)
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_hot_concept_ths_daily")
        ).scalar_one() == 159


def test_native_industry_flow_seals_exact_source_values_and_independent_scheduler_receipt(monkeypatch, tmp_path):
    from server.common import sector_heatmap_store
    from server.common.scheduler_validation import _sector_heat_east_output_status

    monkeypatch.setattr(sector_heatmap_store, "journal_runtime_root", lambda: tmp_path)
    engine = _engine()
    captured = _capture(provider_rows=_native_rows(), complete=True)
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: captured)
    options = {"formal": True, "engine": engine, "now": datetime(2026, 8, 26, 17, 8)}
    validation = sector.fetch_sector_heat_east_daily(TARGET_DATE, dry_run=True, **options)
    assert validation["industry_flow"]["status"] == "VALIDATED"
    assert list(tmp_path.iterdir()) == []
    first = sector.fetch_sector_heat_east_daily(TARGET_DATE, **options)
    repeated = sector.fetch_sector_heat_east_daily(TARGET_DATE, **options)
    assert first["industry_flow"]["status"] == repeated["industry_flow"]["status"] == "PASS"
    assert first["industry_flow"]["database_writes"] is True
    assert repeated["industry_flow"]["database_writes"] is False
    assert first["industry_flow"]["receipt_id"] == _receipt_hash(first["industry_flow"])
    assert _sector_heat_east_output_status(json.dumps(first), return_code=0) == "success"
    rows = sector_heatmap_store.read_industry_flow(TARGET_DATE, now=options["now"])
    assert [(row["index_code"], row["main_net_inflow"]) for row in rows] == [
        ("BK1036", "-4629633024.00"), ("BK1201", "-13864036864.00"),
    ]
    assert rows[0]["source_snapshot_at"] == "2026-08-26T15:39:30"
    assert rows[0]["etl_sync_at"] == "2026-08-26T17:08:00"


@pytest.mark.parametrize(("changed", "reason"), [
    ({"f124": 0}, "NATIVE_EXACT_DATE_UNAVAILABLE"),
    ({"f124": int(datetime(2026, 8, 26, 15, 9, tzinfo=PRODUCTION_TIMEZONE).timestamp())},
     "NATIVE_CLOSE_TIME_UNPROVEN"),
    ({"f62": "-"}, "NATIVE_FLOW_VALUE_INVALID"),
    ({"f62": 0}, "NATIVE_MAIN_FLOW_LEGS_DIFFER"),
    ({"f12": "BK1036"}, "NATIVE_INDUSTRY_IDENTITY_INVALID"),
])
def test_unproven_native_flow_does_not_affect_verified_heat(monkeypatch, changed, reason):
    raw = _native_rows()
    raw[0].update(changed)
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: _capture(provider_rows=raw, complete=True))
    _patch_store(monkeypatch, lambda *_args: pytest.fail("unproven flow must not reach persistence"))

    receipt = sector.fetch_sector_heat_east_daily(TARGET_DATE, formal=True, engine=_engine(),
                                                 now=datetime(2026, 8, 26, 17, 8))
    assert receipt["status"] == "PASS"
    assert receipt["industry_flow"]["status"] == "UNAVAILABLE"
    assert receipt["industry_flow"]["reason_code"] == reason
    assert receipt["industry_flow"]["database_writes"] is False


def test_latest_provider_flow_cannot_be_published_on_older_heat_partition(monkeypatch):
    capture = _capture(provider_rows=_native_rows("2026-08-27"), complete=True)
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: capture)
    _patch_store(monkeypatch, lambda *_args: pytest.fail("a historical date must not inherit latest flow"))

    receipt = sector.fetch_sector_heat_east_daily(TARGET_DATE, formal=True, engine=_engine(),
                                                 now=datetime(2026, 8, 27, 17, 8))
    assert receipt["status"] == "PASS"
    assert receipt["industry_flow"]["reason_code"] == "NATIVE_EXACT_DATE_UNAVAILABLE"


def test_native_flow_readback_failure_does_not_claim_publication_or_break_heat(monkeypatch):
    capture = _capture(provider_rows=_native_rows(), complete=True)
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: capture)
    _patch_store(monkeypatch, lambda *_args: {"row_count": 1, "code_count": 1})
    receipt = sector.fetch_sector_heat_east_daily(TARGET_DATE, formal=True, engine=_engine(),
                                                 now=datetime(2026, 8, 26, 17, 8))
    assert receipt["status"] == "PASS"
    assert receipt["industry_flow"]["status"] == "FAILED"
    assert receipt["industry_flow"]["published"] is False


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
        lambda: _capture(_complete_provider_rows("2026-08-25")),
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
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", lambda: _capture(rows))

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
    monkeypatch.setattr(sector, "_fetch_eastmoney_industries", _capture)
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
            started_at=datetime(2026, 8, 26, 17, 8),
            finished_at=datetime(2026, 8, 26, 17, 8),
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
