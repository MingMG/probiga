# -*- coding: utf-8 -*-
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
