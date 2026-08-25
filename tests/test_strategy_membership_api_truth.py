from __future__ import annotations

from copy import deepcopy

from server.api.routers import strategy_center as strategy_center_router
from server.api.routers import trading_v2
from server.engine import strategy_center
from tools.strategy_governance_task_contract import TASK


TARGET = "2026-08-21"


def _verified_membership(member_type: str) -> dict:
    return {
        "status": "verified",
        "snapshot_complete": True,
        "snapshot_date": TARGET,
        "member_type": member_type,
        "data_category": "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP",
        "excluded_data_categories": [
            "SECTOR_HEAT_HISTORY",
            "SECTOR_ROTATION_HISTORY",
        ],
        "data": [],
    }


class _EvidenceRepository:
    def latest_snapshot(self):
        return {"snapshot_id": "snapshot-1"}

    def execution_capability(self, _capability_code):
        # BLOCK is the truthful real-order safety boundary, not a read error.
        return {"status": "BLOCK"}


def test_data_evidence_propagates_nested_membership_error(monkeypatch):
    monkeypatch.setattr(trading_v2, "_repo", lambda: _EvidenceRepository())
    monkeypatch.setattr(
        trading_v2,
        "load_qmt_kline_attestation_status",
        lambda limit: {"status": "complete", "runs": []},
    )

    def membership(*, member_type, **_kwargs):
        if member_type == "concept":
            return {
                **_verified_membership(member_type),
                "status": "integrity_error",
                "snapshot_complete": False,
                "reason": "canonical hash mismatch",
            }
        return _verified_membership(member_type)

    monkeypatch.setattr(
        trading_v2, "load_membership_snapshot_history", membership,
    )

    result = trading_v2.data_evidence()

    assert result["status"] == "degraded"
    assert result["data"]["historical_data_ready"] is False
    assert result["data"]["membership_and_kline_history_ready"] is False
    assert result["data"]["all_historical_data_ready"] is False
    assert result["data"]["component_status"][
        "concept_membership"
    ] == "integrity_error"
    assert "concept成员快照未完整验真" in result["data"][
        "component_issues"
    ]


def test_data_evidence_is_ok_only_when_historical_components_are_verified(
    monkeypatch,
):
    monkeypatch.setattr(trading_v2, "_repo", lambda: _EvidenceRepository())
    monkeypatch.setattr(
        trading_v2,
        "load_qmt_kline_attestation_status",
        lambda limit: {"status": "complete", "runs": []},
    )
    monkeypatch.setattr(
        trading_v2,
        "load_membership_snapshot_history",
        lambda *, member_type, **_kwargs: _verified_membership(member_type),
    )

    result = trading_v2.data_evidence()

    assert result["status"] == "ok"
    assert result["data"]["historical_data_ready"] is True
    assert result["data"]["membership_and_kline_history_ready"] is True
    assert result["data"]["all_historical_data_ready"] is False
    assert result["data"]["historical_data_ready_scope"] == (
        "QMT_KLINE_ATTESTATION_AND_POINT_IN_TIME_MEMBERSHIP_ONLY"
    )
    assert result["data"]["verified_historical_scopes"] == [
        "QMT_DAILY_KLINE_ATTESTATION",
        "POINT_IN_TIME_CONCEPT_MEMBERSHIP",
        "POINT_IN_TIME_INDUSTRY_MEMBERSHIP",
    ]
    assert result["data"]["level1"]["status"] == "BLOCK"
    assert result["data"]["membership_data_boundary"] == {
        "category": "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP",
        "description": "概念/行业成分归属快照，与热度历史、轮动历史分开",
        "excluded_categories": [
            "SECTOR_HEAT_HISTORY",
            "SECTOR_ROTATION_HISTORY",
            "QMT_NATIVE_SECTOR_INDEX_REALTIME",
            "QMT_NATIVE_SECTOR_INDEX_MINUTE",
            "QMT_NATIVE_SECTOR_INDEX_DAILY_HISTORY",
        ],
    }
    assert result["data"]["unverified_or_excluded_historical_scopes"] == [
        "SECTOR_HEAT_HISTORY",
        "SECTOR_ROTATION_HISTORY",
        "QMT_NATIVE_SECTOR_INDEX_REALTIME",
        "QMT_NATIVE_SECTOR_INDEX_MINUTE",
        "QMT_NATIVE_SECTOR_INDEX_DAILY_HISTORY",
    ]
    categories = result["data"]["industry_history_evidence_categories"]
    assert set(categories) == {
        "point_in_time_constituent_membership",
        "source_specific_sector_heat_history",
        "constituent_aggregated_strength_history",
        "qmt_native_bkzs_index_history",
    }
    assert categories["point_in_time_constituent_membership"]["ready"] is True
    assert categories["source_specific_sector_heat_history"]["ready"] is False
    assert categories["constituent_aggregated_strength_history"]["ready"] is False
    assert categories["qmt_native_bkzs_index_history"]["ready"] is False
    assert categories["qmt_native_bkzs_index_history"][
        "synthetic_substitution_allowed"
    ] is False
    assert result["data"]["all_historical_data_ready"] is False


def test_strategy_center_membership_error_response_is_fail_closed(
    monkeypatch,
):
    monkeypatch.setattr(
        strategy_center_router,
        "load_membership_snapshot_history",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )

    result = strategy_center_router.strategy_center_membership_history(
        snapshot_date=TARGET,
        member_type="industry",
        group_code="",
        stock_code="",
        limit=1,
    )

    assert result["status"] == "degraded"
    assert result["snapshot_date"] == TARGET
    assert result["snapshot_complete"] is False
    assert result["data"] == []
    assert result["data_category_label"] == "行业成分归属历史"
    assert "不代表板块热度" in result["data_semantics"]
    assert result["automatic_real_order_submission"] is False


def test_authoritative_candidate_loader_ignores_presentation_limit(
    monkeypatch,
):
    rows = [
        {
            "stock_code": str(index).zfill(6),
            "pick_date": TARGET,
            "final_trade_score": 1000 - index,
            "industry_name": "测试行业",
        }
        for index in range(1, 506)
    ]
    queries: list[str] = []
    monkeypatch.setattr(
        strategy_center, "load_reference_candidate_pool", lambda _date: None,
    )
    monkeypatch.setattr(strategy_center, "_table_exists", lambda _name: True)

    def db_read(sql, _params=None):
        normalized = " ".join(str(sql).split())
        queries.append(normalized)
        if "information_schema.COLUMNS" in normalized:
            return [
                {"COLUMN_NAME": name}
                for name in (
                    "stock_code",
                    "pick_date",
                    "final_trade_score",
                    "industry_name",
                )
            ]
        if "FROM st_recommended_stocks" in normalized:
            return deepcopy(rows)
        raise AssertionError(normalized)

    monkeypatch.setattr(strategy_center, "_db_read", db_read)

    loaded = strategy_center.load_recommendation_rows(TARGET, limit=1)

    assert len(loaded) == 505
    detail_query = next(
        sql for sql in queries
        if "FROM st_recommended_stocks" in sql
    )
    assert " LIMIT " not in f" {detail_query.upper()} "


def test_explicit_candidate_date_never_uses_older_industry_membership(
    monkeypatch,
):
    queries: list[str] = []
    monkeypatch.setattr(
        strategy_center, "load_reference_candidate_pool", lambda _date: None,
    )
    monkeypatch.setattr(strategy_center, "_table_exists", lambda _name: True)

    def db_read(sql, _params=None):
        normalized = " ".join(str(sql).split())
        queries.append(normalized)
        if "information_schema.COLUMNS" in normalized:
            return [
                {"COLUMN_NAME": name}
                for name in (
                    "stock_code",
                    "pick_date",
                    "final_trade_score",
                    "industry_name",
                )
            ]
        if "FROM st_recommended_stocks" in normalized:
            return [{
                "stock_code": "000001",
                "pick_date": TARGET,
                "final_trade_score": 99,
                "industry_name": "",
            }]
        if "FROM qmt_industry_member_snapshot" in normalized:
            # No exact-date snapshot exists. An older snapshot must not be
            # silently substituted into an explicit-date candidate row.
            return []
        raise AssertionError(normalized)

    monkeypatch.setattr(strategy_center, "_db_read", db_read)

    loaded = strategy_center.load_recommendation_rows(TARGET, limit=1)

    assert loaded[0]["industry_name"] == ""
    industry_query = next(
        sql for sql in queries
        if "FROM qmt_industry_member_snapshot" in sql
    )
    assert "snapshot_date = :trade_date" in industry_query
    assert "MAX(snapshot_date)" not in industry_query
    assert "snapshot_date <=" not in industry_query


def test_daily_governance_task_contract_is_enabled_and_limit_is_not_universe_cap():
    assert TASK["task_type"] == "strategy_governance_daily"
    assert TASK["enabled"] == 1
    assert TASK["cron_time"] == "22:35"
    assert TASK["script_args"] == "--limit 500"
    # The preceding loader test proves this compatibility argument is not
    # applied as a SQL or in-memory authoritative-universe limit.


def test_strategy_discovery_counts_zero_runtime_strategies():
    static_keys = {
        str(item["key"]) for item in strategy_center.STRATEGY_CATALOG
    }
    configs = {
        key: {"enabled": index % 2 == 0}
        for index, key in enumerate(sorted(static_keys))
    }

    result = strategy_center._strategy_discovery_counts(configs, [])

    expected_enabled = sum(
        bool(configs[key]["enabled"]) for key in static_keys
    )
    assert result["static_catalog_count"] == len(static_keys)
    assert result["runtime_registry_count"] == 0
    assert result["total_discovered_strategy_count"] == len(static_keys)
    assert result["strategy_count"] == len(static_keys)
    assert result["enabled_count"] == expected_enabled
    assert result["runtime_registry_discovery_status"] == "COMPLETE"


def test_strategy_discovery_counts_many_runtime_keys_and_deduplicates_overlap():
    static_keys = sorted(
        str(item["key"]) for item in strategy_center.STRATEGY_CATALOG
    )
    overlap = static_keys[0]
    configs = {key: {"enabled": True} for key in static_keys}
    runtime = [
        {"strategy_key": overlap, "enabled": False},
        {"strategy_key": "runtime_alpha", "enabled": True},
        {"strategy_key": "runtime_beta", "enabled": False},
        # A duplicate status row cannot inflate the unique registry count.
        {"strategy_key": "runtime_alpha", "enabled": True},
        {"strategy_key": "", "enabled": True},
    ]

    result = strategy_center._strategy_discovery_counts(configs, runtime)

    assert result["runtime_registry_count"] == 3
    assert result["enabled_runtime_count"] == 1
    assert result["total_discovered_strategy_count"] == len(static_keys) + 2
    assert result["strategy_count"] == len(static_keys) + 2
    # Runtime disabled state overrides the colliding static enabled flag.
    assert result["enabled_count"] == len(static_keys)
    assert result["strategy_count_semantics"] == (
        "UNIQUE_DISCOVERED_STRATEGY_KEYS"
    )
    assert result["canonical_governance_count_source"] == (
        "strategy_governance_registry"
    )


def test_strategy_discovery_count_marks_registry_failure_unavailable():
    result = strategy_center._strategy_discovery_counts(
        {},
        [{
            "strategy_key": "",
            "status": "DISCOVERY_UNAVAILABLE",
            "enabled": False,
        }],
    )

    assert result["runtime_registry_count"] == 0
    assert result["runtime_registry_discovery_status"] == "UNAVAILABLE"


def test_strategy_center_snapshot_publishes_dynamic_discovery_counts(
    monkeypatch,
):
    static_keys = sorted(
        str(item["key"]) for item in strategy_center.STRATEGY_CATALOG
    )
    overlap = static_keys[0]
    configs = {key: {"enabled": True} for key in static_keys}
    runtime_statuses = [
        {"strategy_key": overlap, "enabled": False},
        {"strategy_key": "runtime_alpha", "enabled": True},
        {"strategy_key": "runtime_beta", "enabled": True},
    ]
    monkeypatch.setattr(
        strategy_center, "latest_recommendation_date", lambda _date: TARGET,
    )
    monkeypatch.setattr(
        strategy_center, "load_reference_candidate_pool", lambda _date: None,
    )
    monkeypatch.setattr(
        strategy_center,
        "load_market_snapshot",
        lambda *_args, **_kwargs: {
            "source_status": "fresh",
            "state": {"key": "trend_bullish"},
        },
    )
    monkeypatch.setattr(strategy_center, "load_strategy_configs", lambda: configs)
    monkeypatch.setattr(strategy_center, "load_strategy_metrics", lambda _date: {})
    monkeypatch.setattr(
        strategy_center, "load_recommendation_rows", lambda *_args: [],
    )
    monkeypatch.setattr(
        strategy_center,
        "_dynamic_execution_signals",
        lambda **_kwargs: ([], runtime_statuses),
    )
    monkeypatch.setattr(
        strategy_center, "aggregate_candidates", lambda *_args, **_kwargs: ([], []),
    )
    monkeypatch.setattr(
        strategy_center,
        "_candidate_source_contract",
        lambda *_args, **_kwargs: {
            "status": "COMPLETED",
            "reason": "complete",
        },
    )
    monkeypatch.setattr(strategy_center, "build_strategy_cards", lambda *_args: [])
    monkeypatch.setattr(
        strategy_center,
        "load_stock_manifest",
        lambda: {"manifest_version": "fixture"},
    )
    monkeypatch.setattr(strategy_center, "stock_manifest_hash", lambda: "s" * 64)
    monkeypatch.setattr(
        strategy_center,
        "load_market_state_config",
        lambda: {"config_version": "fixture"},
    )
    monkeypatch.setattr(
        strategy_center, "market_state_config_hash", lambda: "m" * 64,
    )

    result = strategy_center.build_strategy_center_snapshot(TARGET, limit=1)

    summary = result["summary"]
    assert summary["static_catalog_count"] == len(static_keys)
    assert summary["runtime_registry_count"] == 3
    assert summary["total_discovered_strategy_count"] == len(static_keys) + 2
    assert summary["strategy_count"] == len(static_keys) + 2
