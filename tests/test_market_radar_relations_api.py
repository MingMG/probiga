# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path

import pytest

from server.api.routers import market_radar


def test_relation_index_uses_only_verified_canonical_candidates(monkeypatch):
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda _engine, sql, *args, **kwargs: (
            [{"stock_code": "000001", "shares": 200}]
            if "st_user_portfolio" in sql
            else []
        ),
    )
    monkeypatch.setattr(
        market_radar,
        "canonical_governance_decision",
        lambda: {
            "context": {
                "decision_integrity_verified": True,
                "run_status": "COMPLETED",
                "run_uid": "a" * 32,
                "decision_date": "2026-09-04",
            },
            "pool": {
                "pool_readable": True,
                "items": [
                    {"stock_code": "000002", "is_strategy_candidate": True},
                    {"stock_code": "000003", "is_strategy_candidate": False},
                ],
            },
        },
    )
    monkeypatch.setattr(
        market_radar, "authoritative_closed_trade_date", lambda _engine: "2026-09-04"
    )

    result = market_radar._load_relation_index(object())
    assert result["candidate_status"] == "available"
    assert result["candidate_date"] == "2026-09-04"
    assert result["candidate_run_uid"] == "a" * 32
    assert result["members"]["000002"]["strategy_candidate"] is True
    assert "000003" not in result["members"]


def test_unverified_canonical_pool_fails_closed(monkeypatch):
    monkeypatch.setattr(market_radar, "_read_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(market_radar, "canonical_governance_decision", lambda: None)
    result = market_radar._load_relation_index(object())
    assert result["candidate_status"] == "unavailable"
    assert result["candidate_date"] == ""


def test_old_canonical_pool_is_visible_as_stale_but_not_used_as_current_scope(monkeypatch):
    monkeypatch.setattr(market_radar, "_read_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        market_radar,
        "canonical_governance_decision",
        lambda: {
            "context": {
                "decision_integrity_verified": True,
                "run_status": "COMPLETED",
                "run_uid": "c" * 32,
                "decision_date": "2026-09-03",
            },
            "pool": {
                "pool_readable": True,
                "items": [
                    {"stock_code": "000002", "is_strategy_candidate": True}
                ],
            },
        },
    )
    monkeypatch.setattr(
        market_radar, "authoritative_closed_trade_date", lambda _engine: "2026-09-04"
    )

    result = market_radar._load_relation_index(object())

    assert result["candidate_status"] == "stale"
    assert result["candidate_date"] == "2026-09-03"
    assert result["members"] == {}
    assert market_radar._scope_available("strategy_candidate", result) is False


def test_stock_scope_filters_api_rows_and_exposes_lineage(monkeypatch):
    relation_index = market_radar.build_radar_relation_index(
        [{"stock_code": "000001", "shares": 100}],
        [{"stock_code": "000002"}],
        candidate_date="2026-09-04",
        candidate_run_uid="b" * 32,
    )
    monkeypatch.setattr(market_radar, "get_engine", lambda: object())
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: relation_index)
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *args, **kwargs: [
            {"stock_code": "000001", "score": 80, "signal_tags": "[]"},
            {"stock_code": "000002", "score": 70, "signal_tags": "[]"},
        ],
    )

    result = market_radar.radar_stocks(direction="", scope="holding", limit=20)
    assert result["status"] == "ok"
    assert [row["stock_code"] for row in result["rows"]] == ["000001"]
    assert result["relation_context"]["candidate_run_uid"] == "b" * 32


def test_sector_scope_includes_an_ordinary_related_concept_member(monkeypatch):
    relation_index = market_radar.build_radar_relation_index(
        [{"stock_code": "000009", "shares": 100}],
        [],
        candidate_status="unavailable",
    )
    monkeypatch.setattr(market_radar, "get_engine", lambda: object())
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: relation_index)

    def fake_read(_engine, sql, *args, **kwargs):
        if "si_concept_constituent_east" in sql:
            return [{"concept_code": "C100", "stock_code": "000009"}]
        return [
            {
                "sector_code": "CONCEPT:C100",
                "sector_name": "测试板块",
                "sector_type": "concept",
                "score": 80,
                "dragon_json": "[]",
                "core_json": "[]",
                "follower_json": "[]",
            }
        ]

    monkeypatch.setattr(market_radar, "_read_rows", fake_read)
    result = market_radar.radar_sectors(
        direction="", sector_type="", scope="holding", limit=20
    )

    assert result["status"] == "ok"
    assert result["sector_relation_membership_status"] == "available"
    assert [row["sector_code"] for row in result["rows"]] == ["CONCEPT:C100"]
    assert result["rows"][0]["relation_codes"]["holding"] == ["000009"]


def test_event_scope_includes_an_ordinary_related_concept_member(monkeypatch):
    relation_index = market_radar.build_radar_relation_index(
        [{"stock_code": "000009", "shares": 100}],
        [],
        candidate_status="unavailable",
    )
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: relation_index)
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )

    def fake_read(_engine, sql, *args, **kwargs):
        if "si_concept_constituent_east" in sql:
            return [{"concept_code": "C100", "stock_code": "000009"}]
        return [
            {
                "event_id": 1,
                "event_type": "sector_anomaly",
                "sector_code": "CONCEPT:C100",
                "sector_name": "测试板块",
                "stock_code": None,
                "snapshot_at": "2026-09-04 09:59:00",
                "direction": "UP",
                "score": 80,
                "detail_json": "{}",
            }
        ]

    monkeypatch.setattr(market_radar, "_read_rows", fake_read)

    result = market_radar.radar_events(scope="holding", limit=20)

    assert result["status"] == "ok"
    assert result["sector_relation_membership_status"] == "available"
    assert [row["sector_code"] for row in result["rows"]] == ["CONCEPT:C100"]
    assert result["rows"][0]["relation_codes"]["holding"] == ["000009"]


def _empty_relation_index():
    return market_radar.build_radar_relation_index([], [])


def test_status_is_partial_when_stock_is_fresh_but_sector_was_never_scanned(monkeypatch):
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *_args, **_kwargs: [
            {
                "stock_rows": 12,
                "sector_rows": 0,
                "event_rows": 0,
                "latest_stock_at": "2026-09-04 09:59:00",
                "latest_sector_at": None,
                "latest_event_at": None,
            }
        ],
    )

    result = market_radar.radar_status()

    assert result["data_status"] == "partial"
    assert result["channel_status"]["stock"]["data_status"] == "fresh"
    assert result["channel_status"]["sector"]["data_status"] == "unavailable"
    assert result["latest"]["latest_sector_status"] == "unavailable"


def test_current_radar_with_only_old_events_reports_fresh_empty_event_channel(monkeypatch):
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *_args, **_kwargs: [
            {
                "stock_rows": 12,
                "sector_rows": 5,
                "event_rows": 7,
                "latest_stock_at": "2026-09-04 09:59:00",
                "latest_sector_at": "2026-09-04 09:59:00",
                "latest_event_at": "2026-09-04 09:30:00",
            }
        ],
    )

    result = market_radar.radar_status()

    assert result["data_status"] == "fresh"
    assert result["channel_status"]["event"] == {
        "data_status": "fresh_empty",
        "data_cutoff": "2026-09-04 09:59:00",
        "freshness_reason": "NO_FRESH_EVENTS_IN_CURRENT_RADAR_WINDOW",
    }
    assert result["latest"]["latest_event_status"] == "fresh_empty"


@pytest.mark.parametrize("kind", ["stock", "sector", "event"])
def test_fresh_source_with_no_scope_match_is_an_empty_result_not_unavailable(
    monkeypatch, kind
):
    engine = object()
    relation_index = market_radar.build_radar_relation_index(
        [{"stock_code": "000999", "shares": 100}],
        [],
        candidate_status="unavailable",
    )
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: relation_index)
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    row = {
        "snapshot_at": "2026-09-04 09:59:00",
        "direction": "UP",
        "score": 80,
    }
    if kind == "stock":
        row.update({"stock_code": "000001", "signal_tags": "[]"})
    elif kind == "sector":
        row.update(
            {
                "sector_code": "CONCEPT:C1",
                "sector_name": "测试板块",
                "sector_type": "concept",
                "dragon_json": "[]",
                "core_json": "[]",
                "follower_json": "[]",
            }
        )
        monkeypatch.setattr(
            market_radar,
            "_attach_sector_relation_members",
            lambda *_args, **_kwargs: "available",
        )
    else:
        row.update({"event_id": 1, "stock_code": "000001", "detail_json": "{}"})
    monkeypatch.setattr(market_radar, "_read_rows", lambda *_args, **_kwargs: [row])

    if kind == "stock":
        result = market_radar.radar_stocks(direction="UP", scope="holding", limit=20)
    elif kind == "sector":
        result = market_radar.radar_sectors(
            direction="UP", sector_type="", scope="holding", limit=20
        )
    else:
        result = market_radar.radar_events(scope="holding", limit=20)

    assert result["rows"] == []
    assert result["data_status"] == "fresh"
    assert result["fresh_rows"] == 1


def test_stock_api_recomputes_cross_day_snapshot_as_stale(monkeypatch):
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: _empty_relation_index())
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *_args, **_kwargs: [
            {
                "stock_code": "000001",
                "snapshot_at": "2026-09-03 14:59:00",
                "direction": "UP",
                "score": 80,
                "stale": 0,
                "signal_tags": "[]",
            }
        ],
    )

    result = market_radar.radar_stocks(direction="UP", scope="all", limit=20)

    assert result["data_status"] == "stale"
    assert result["data_cutoff"] == "2026-09-03 14:59:00"
    assert result["expected_trade_date"] == "2026-09-04"
    assert result["rows"][0]["stale"] is True
    assert result["rows"][0]["freshness_status"] == "stale"
    assert result["rows"][0]["freshness_reason"] == "RADAR_TRADE_DATE_MISMATCH"


def test_same_day_snapshot_becomes_stale_when_radar_task_stops(monkeypatch):
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: _empty_relation_index())
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *_args, **_kwargs: [
            {
                "sector_code": "CONCEPT:C1",
                "sector_name": "测试板块",
                "sector_type": "concept",
                "snapshot_at": "2026-09-04 09:30:00",
                "direction": "UP",
                "score": 80,
                "dragon_json": "[]",
                "core_json": "[]",
                "follower_json": "[]",
            }
        ],
    )

    result = market_radar.radar_sectors(
        direction="UP", sector_type="", scope="all", limit=20
    )

    assert result["data_status"] == "stale"
    assert result["rows"][0]["age_seconds"] == 1800.0
    assert result["rows"][0]["freshness_reason"] == "RADAR_SNAPSHOT_EXPIRED"


def test_old_radar_event_is_explicitly_historical(monkeypatch):
    engine = object()
    monkeypatch.setattr(market_radar, "get_engine", lambda: engine)
    monkeypatch.setattr(market_radar, "ensure_radar_tables", lambda _engine: None)
    monkeypatch.setattr(market_radar, "_load_relation_index", lambda _engine: _empty_relation_index())
    monkeypatch.setattr(
        market_radar,
        "_radar_now",
        lambda: datetime(2026, 9, 4, 10, 0, tzinfo=market_radar.PRODUCTION_TIMEZONE),
    )
    monkeypatch.setattr(
        market_radar, "_radar_authoritative_trade_date", lambda _engine, _now: "2026-09-04"
    )
    monkeypatch.setattr(
        market_radar,
        "_read_rows",
        lambda *_args, **_kwargs: [
            {
                "event_id": 1,
                "stock_code": "000001",
                "snapshot_at": "2026-09-04 09:40:00",
                "direction": "UP",
                "score": 80,
                "detail_json": "{}",
            }
        ],
    )

    result = market_radar.radar_events(scope="all", limit=20)

    assert result["freshness_threshold_seconds"] == market_radar.RADAR_EVENT_FRESH_SECONDS
    assert result["data_status"] == "stale"
    assert result["rows"][0]["stale"] is True


def test_market_radar_ui_labels_realtime_cutoff_and_historical_rows():
    script = (Path(__file__).resolve().parents[1] / "server/static/js/app.js").read_text(
        encoding="utf-8"
    )
    renderer = script.split("function loadMarketRadarPage", 1)[1].split(
        "/* ===== 策略与模拟", 1
    )[0]

    assert "实时观察页，不跟随全局历史日期" in renderer
    assert "实时雷达：" in renderer
    assert "status.data_cutoff" in renderer
    assert "数据已过期；以下仅为历史快照，不代表当前异动。" in renderer
    assert "雷达快照不可用或尚未扫描" in renderer
    assert "本次扫描暂无符合条件的异动" in renderer
    assert "channel_status" in renderer
    assert "radarChannelLabel('sector')" in renderer
    assert "radarChannelLabel('event')" in renderer
    assert "'当前无新事件'" in renderer
    assert "events.sector_relation_membership_status" in renderer
    assert "当前事件筛选可能漏计" in renderer
    assert "当前扫描无新事件" in renderer
    assert "'历史事件'" in renderer
    assert "radarFreshnessText" in renderer
