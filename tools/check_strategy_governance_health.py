#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only production acceptance checks for dynamic strategy governance."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import secrets
import subprocess
import sys
import threading
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_strategy_governance_daily import authoritative_closed_trade_date
from tools.strategy_governance_task_contract import TASK as GOVERNANCE_TASK
from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    ANALYSIS_UPPER_EVIDENCE_CRON,
    TASK as QMT_ANNOUNCEMENT_TASK,
    STRATEGY_GOVERNANCE_CRON,
    validate_pipeline_order as validate_qmt_announcement_pipeline_order,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS
from server.common.scheduler_runtime_health import (
    check_linux_standalone_scheduler_heartbeat,
    check_qmt_windows_edge_executor,
    check_qmt_windows_edge_release_receipt,
)
from server.engine.strategy_funding_checkpoint import (
    FUNDING_CHECKPOINT_AUDIT_SCHEMA,
    FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
    FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
    FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
    FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
    FUNDING_CHECKPOINT_MIGRATION_HASH,
    FUNDING_CHECKPOINT_MIGRATION_KEY,
    FUNDING_CHECKPOINT_SCHEMA,
    FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
    FUNDING_CHECKPOINT_TABLE_NAME,
    FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
    FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
    FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
    FUNDING_CHECKPOINT_TRIGGER_CONTRACTS,
    FUNDING_DAILY_FACT_SCHEMA,
    FUNDING_DAILY_FACT_TABLE_NAME,
    canonical_hash as _funding_canonical_hash,
    canonical_json as _funding_canonical_json,
    checkpoint_chain_payload,
    checkpoint_identity,
    checkpoint_state_hash,
    funding_daily_fact_hash,
    funding_daily_fact_identity,
    ordered_funding_fact_set_hash,
    validate_strategy_funding_checkpoint_schema,
)
from server.engine.strategy_industry_history import (
    QMT_PREVIOUS_SESSION_FALLBACKS,
    STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE,
)


QMT_PRECLOSE_ATTESTATION_PROTOCOL = "QMT_DAILY_UNADJUSTED_PRECLOSE_V2"
CANONICAL_FUNDING_PROVENANCE = (
    "INTERNAL_PORTFOLIO_CHECKPOINT_FACT_LEDGER_V3"
)
EXPECTED_FUNDING_TABLE_COUNTS = {
    FUNDING_DAILY_FACT_TABLE_NAME: {
        "column_count": 29,
        "index_count": 9,
        "foreign_key_count": 3,
        "check_count": 7,
    },
    FUNDING_CHECKPOINT_TABLE_NAME: {
        "column_count": 46,
        "index_count": 12,
        "foreign_key_count": 7,
        "check_count": 13,
    },
}
GOVERNANCE_HEALTH_CONTRACT_VERSION = (
    "probiga.strategy-governance-health.v1"
)
EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_COUNT = 38
EXPECTED_METRIC_REVIEW_TRIGGER_COUNT = 2
EXPECTED_GOVERNANCE_TRIGGER_COUNT = 40
_RAW_METRIC_REPLAY_LOCK = threading.Lock()
METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS = {
    "trg_strategy_metric_input_review_bu": (
        "BEFORE",
        "UPDATE",
        "st_strategy_metric_input",
        "b7159019d00be1e1cc0bc78da48bcb2f7076aeffa03d8e238c3bcc61879a4aa3",
    ),
    "trg_strategy_metric_input_immutable_bd": (
        "BEFORE",
        "DELETE",
        "st_strategy_metric_input",
        "58e40642ad1887fae025bacb964b2de6b90cd293ff25aa03c404eb1136911f77",
    ),
}
METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.clear()
GOVERNANCE_TABLES = (
    "st_strategy_governance_schema_migration",
    "st_strategy_registry",
    "st_strategy_version",
    "st_strategy_lifecycle_event",
    "st_strategy_metric_input",
    "st_strategy_health_snapshot",
    "st_strategy_combination",
    "st_strategy_combination_version",
    "st_strategy_combination_health_snapshot",
    "st_strategy_governance_run",
    "st_strategy_pool_snapshot",
    "st_strategy_allocation_snapshot",
    "st_strategy_adapter_run_receipt",
    "st_strategy_industry_history",
    "st_strategy_governance_audit",
    "st_strategy_adapter_candidate_fact",
    "st_dynamic_shadow_trial_plan",
    "st_dynamic_shadow_trial_chain",
    "st_dynamic_shadow_trial_exit_binding",
    FUNDING_DAILY_FACT_TABLE_NAME,
    FUNDING_CHECKPOINT_TABLE_NAME,
)
DYNAMIC_SHADOW_TABLES = frozenset({
    "st_strategy_adapter_candidate_fact",
    "st_dynamic_shadow_trial_plan",
    "st_dynamic_shadow_trial_chain",
    "st_dynamic_shadow_trial_exit_binding",
})
REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "st_strategy_governance_schema_migration": frozenset(
        {"migration_key", "migration_hash", "completed_at"}
    ),
    "st_strategy_registry": frozenset(
        {
            "strategy_key",
            "strategy_name",
            "category",
            "family_key",
            "description",
            "owner_name",
            "discovery_mode",
            "enabled",
            "current_version",
            "current_status",
            "status_reason",
            "recovery_conditions_json",
            "created_at",
            "updated_at",
        }
    ),
    "st_strategy_version": frozenset(
        {
            "strategy_key",
            "version",
            "version_hash",
            "content_hash",
            "parent_version",
            "evaluator_type",
            "evaluator_config_json",
            "parameters_json",
            "source_kind",
            "created_by",
            "created_at",
        }
    ),
    "st_strategy_lifecycle_event": frozenset(
        {
            "event_id",
            "entity_type",
            "entity_key",
            "entity_version",
            "previous_status",
            "next_status",
            "reason",
            "trigger_type",
            "evidence_json",
            "payload_json",
            "event_hash",
            "operator_name",
            "occurred_at",
        }
    ),
    "st_strategy_metric_input": frozenset(
        {
            "evidence_id",
            "entity_type",
            "strategy_key",
            "strategy_version",
            "as_of_date",
            "window_days",
            "metrics_json",
            "source",
            "evidence_protocol",
            "artifact_hash",
            "artifact_json",
            "source_dataset_hash",
            "evidence_revision_at",
            "verification_status",
            "funding_provenance",
            "submitted_by",
            "reviewed_by",
            "reviewed_at",
            "evidence_hash",
            "created_at",
        }
    ),
    "st_strategy_health_snapshot": frozenset(
        {
            "run_uid",
            "strategy_key",
            "strategy_version",
            "trade_date",
            "window_days",
            "completed_trades",
            "coverage_days",
            "win_rate_pct",
            "average_win_pct",
            "average_loss_pct",
            "payoff_ratio",
            "gross_expectancy_pct",
            "estimated_cost_pct",
            "net_expectancy_pct",
            "profit_factor",
            "max_drawdown_pct",
            "walk_forward_segments",
            "positive_segments",
            "cost_stress_expectancy_pct",
            "top5_profit_contribution_pct",
            "market_match_score",
            "health_score",
            "profit_gate_passed",
            "gate_reason",
            "recommended_status",
            "evidence_json",
            "result_hash",
            "created_at",
        }
    ),
    "st_strategy_combination": frozenset(
        {
            "combination_key",
            "combination_name",
            "description",
            "owner_name",
            "enabled",
            "current_version",
            "current_status",
            "status_reason",
            "created_at",
            "updated_at",
        }
    ),
    "st_strategy_combination_version": frozenset(
        {
            "combination_key",
            "version",
            "members_json",
            "constraints_json",
            "config_hash",
            "created_by",
            "created_at",
        }
    ),
    "st_strategy_combination_health_snapshot": frozenset(
        {
            "run_uid",
            "combination_key",
            "combination_version",
            "trade_date",
            "ranking_score",
            "profit_gate_passed",
            "gate_reason",
            "recommended_status",
            "evidence_json",
            "result_hash",
            "created_at",
        }
    ),
    "st_strategy_governance_run": frozenset(
        {
            "run_uid",
            "trade_date",
            "run_revision",
            "supersedes_run_uid",
            "is_canonical",
            "market_state",
            "source_status",
            "input_ready",
            "input_hash",
            "build_commit_sha",
            "router_policy_version",
            "router_snapshot_hash",
            "decision_hash",
            "status",
            "strategy_count",
            "formal_count",
            "shadow_count",
            "combination_count",
            "observation_count",
            "confirmation_count",
            "tradable_count",
            "allocation_count",
            "summary_json",
            "created_at",
            "finished_at",
        }
    ),
    "st_strategy_pool_snapshot": frozenset(
        {
            "run_uid",
            "trade_date",
            "pool_level",
            "stock_code",
            "stock_name",
            "rank_no",
            "opportunity_score",
            "execution_score",
            "dominant_strategy",
            "strategies_json",
            "industry_name",
            "gate_status",
            "reason_json",
            "evidence_json",
            "created_at",
        }
    ),
    "st_strategy_allocation_snapshot": frozenset(
        {
            "run_uid",
            "target_type",
            "target_key",
            "target_version",
            "funding_gate_hash",
            "market_state",
            "market_match_score",
            "router_decision_hash",
            "lifecycle_status",
            "lifecycle_status_label",
            "lifecycle_risk_multiplier",
            "base_competitive_weight_pct",
            "simulated_weight_pct",
            "member_sleeves_json",
            "member_sleeve_hash",
            "cash_discount_bp",
            "reason",
            "real_order_authority",
            "created_at",
        }
    ),
    "st_strategy_adapter_run_receipt": frozenset(
        {
            "run_uid",
            "strategy_key",
            "strategy_version",
            "strategy_version_hash",
            "execution_binding_hash",
            "adapter_artifact_sha256",
            "cost_model_hash",
            "adapter_key",
            "adapter_version",
            "trade_date",
            "completed_at",
            "status",
            "input_hash",
            "output_hash",
            "stable_result_hash",
            "candidate_count",
            "candidate_identity_json",
            "receipt_json",
            "receipt_hash",
            "created_at",
        }
    ),
    "st_strategy_industry_history": frozenset(
        {
            "snapshot_id",
            "trade_date",
            "as_of_exclusive",
            "stock_code",
            "industry_name",
            "industry_type",
            "source_system",
            "source_fact_id",
            "source_effective_at",
            "source_etl_sync_at",
            "row_hash",
            "created_at",
        }
    ),
    "st_strategy_governance_audit": frozenset(
        {
            "audit_id",
            "entity_type",
            "entity_key",
            "action",
            "reason",
            "operator_name",
            "before_json",
            "after_json",
            "evidence_json",
            "payload_json",
            "audit_hash",
            "created_at",
        }
    ),
    "st_scheduled_tasks": frozenset(
        {
            "id",
            "task_name",
            "task_type",
            "group_name",
            "script_path",
            "script_args",
            "cron_time",
            "interval_minutes",
            "date_param",
            "enabled",
        }
    ),
}
REQUIRED_INDEXES: dict[str, frozenset[str]] = {
    "st_strategy_governance_schema_migration": frozenset({"PRIMARY"}),
    "st_strategy_registry": frozenset({"PRIMARY"}),
    "st_strategy_version": frozenset(
        {"PRIMARY", "uk_strategy_version_content"}
    ),
    "st_strategy_governance_run": frozenset(
        {
            "PRIMARY",
            "uk_strategy_governance_decision",
            "idx_strategy_governance_canonical",
        }
    ),
    "st_strategy_health_snapshot": frozenset({"PRIMARY"}),
    "st_strategy_combination_health_snapshot": frozenset({"PRIMARY"}),
    "st_strategy_combination": frozenset({"PRIMARY"}),
    "st_strategy_combination_version": frozenset(
        {"PRIMARY", "uk_strategy_combination_hash"}
    ),
    "st_strategy_pool_snapshot": frozenset({"PRIMARY"}),
    "st_strategy_allocation_snapshot": frozenset({"PRIMARY"}),
    "st_strategy_adapter_run_receipt": frozenset(
        {
            "PRIMARY",
            "uk_strategy_adapter_receipt_hash",
            "idx_strategy_adapter_stable_result",
            "idx_strategy_adapter_input",
        }
    ),
    "st_strategy_industry_history": frozenset(
        {
            "PRIMARY",
            "uk_strategy_industry_row_hash",
            "uk_strategy_industry_source_fact",
            "idx_strategy_industry_asof",
        }
    ),
    "st_strategy_lifecycle_event": frozenset(
        {"PRIMARY", "uk_strategy_lifecycle_event_hash"}
    ),
    "st_strategy_metric_input": frozenset(
        {
            "PRIMARY",
            "uk_strategy_metric_evidence",
            "uk_strategy_metric_version_date",
            "uk_strategy_metric_artifact",
            "uk_strategy_metric_dataset",
            "uk_strategy_metric_artifact_global",
            "uk_strategy_metric_dataset_global",
        }
    ),
    "st_strategy_governance_audit": frozenset(
        {"PRIMARY", "uk_strategy_governance_audit_hash"}
    ),
}
REQUIRED_INDEX_CONTRACTS: dict[
    str, dict[str, tuple[tuple[str, ...], bool]]
] = {
    "st_strategy_governance_schema_migration": {
        "PRIMARY": (("migration_key",), True)
    },
    "st_strategy_registry": {"PRIMARY": (("strategy_key",), True)},
    "st_strategy_version": {
        "PRIMARY": (("strategy_key", "version"), True),
        "uk_strategy_version_content": (
            ("strategy_key", "content_hash"),
            True,
        ),
    },
    "st_strategy_lifecycle_event": {
        "PRIMARY": (("event_id",), True),
        "uk_strategy_lifecycle_event_hash": (("event_hash",), True),
    },
    "st_strategy_metric_input": {
        "PRIMARY": (("evidence_id",), True),
        "uk_strategy_metric_evidence": (("evidence_hash",), True),
        "uk_strategy_metric_version_date": (
            (
                "entity_type",
                "strategy_key",
                "strategy_version",
                "as_of_date",
                "window_days",
            ),
            True,
        ),
        "uk_strategy_metric_artifact": (
            (
                "entity_type",
                "strategy_key",
                "strategy_version",
                "window_days",
                "artifact_hash",
            ),
            True,
        ),
        "uk_strategy_metric_dataset": (
            (
                "entity_type",
                "strategy_key",
                "strategy_version",
                "window_days",
                "source_dataset_hash",
            ),
            True,
        ),
        "uk_strategy_metric_artifact_global": (("artifact_hash",), True),
        "uk_strategy_metric_dataset_global": (
            ("source_dataset_hash",),
            True,
        ),
    },
    "st_strategy_health_snapshot": {
        "PRIMARY": (
            ("run_uid", "strategy_key", "strategy_version", "window_days"),
            True,
        )
    },
    "st_strategy_combination": {
        "PRIMARY": (("combination_key",), True)
    },
    "st_strategy_combination_version": {
        "PRIMARY": (("combination_key", "version"), True),
        "uk_strategy_combination_hash": (
            ("combination_key", "config_hash"),
            True,
        ),
    },
    "st_strategy_combination_health_snapshot": {
        "PRIMARY": (
            ("run_uid", "combination_key", "combination_version"),
            True,
        )
    },
    "st_strategy_governance_run": {
        "PRIMARY": (("run_uid",), True),
        "uk_strategy_governance_decision": (("decision_hash",), True),
        "idx_strategy_governance_canonical": (
            ("trade_date", "is_canonical", "run_revision"),
            False,
        ),
    },
    "st_strategy_pool_snapshot": {
        "PRIMARY": (("run_uid", "pool_level", "stock_code"), True)
    },
    "st_strategy_allocation_snapshot": {
        "PRIMARY": (("run_uid", "target_type", "target_key"), True)
    },
    "st_strategy_adapter_run_receipt": {
        "PRIMARY": (("run_uid",), True),
        "uk_strategy_adapter_receipt_hash": (("receipt_hash",), True),
        "idx_strategy_adapter_stable_result": (
            (
                "strategy_key",
                "strategy_version",
                "trade_date",
                "execution_binding_hash",
                "stable_result_hash",
            ),
            False,
        ),
        "idx_strategy_adapter_input": (
            ("trade_date", "input_hash", "output_hash"),
            False,
        ),
    },
    "st_strategy_industry_history": {
        "PRIMARY": (("snapshot_id", "stock_code"), True),
        "uk_strategy_industry_row_hash": (("row_hash",), True),
        "uk_strategy_industry_source_fact": (
            ("source_system", "source_fact_id"),
            True,
        ),
        "idx_strategy_industry_asof": (
            ("trade_date", "as_of_exclusive", "stock_code"),
            False,
        ),
    },
    "st_strategy_governance_audit": {
        "PRIMARY": (("audit_id",), True),
        "uk_strategy_governance_audit_hash": (("audit_hash",), True),
    },
}
LIFECYCLE_STATES = ("ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED")
LIFECYCLE_TRANSITIONS: dict[str, frozenset[str]] = {
    "ACTIVE": frozenset({
        "ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED",
    }),
    "REDUCE": frozenset({
        "ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED",
    }),
    "SHADOW": frozenset({
        "ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED",
    }),
    "SUSPENDED": frozenset({"SHADOW", "SUSPENDED", "RETIRED"}),
    "RETIRED": frozenset({"RETIRED"}),
}


def governance_health_required_check_names(
    disposition: str,
    *,
    require_scheduler_heartbeat: bool = False,
) -> frozenset[str]:
    """Return the frozen producer-side check inventory for a passing report."""

    column_tables = set(REQUIRED_COLUMNS) - set(DYNAMIC_SHADOW_TABLES)
    index_tables = set(REQUIRED_INDEXES) - set(DYNAMIC_SHADOW_TABLES)
    common = {
        "required_tables", "daily_scheduler_task_unique",
        "daily_scheduler_task_contract",
        "qmt_announcement_scheduler_task_unique",
        "qmt_announcement_scheduler_task_contract",
        "qmt_operations_scheduler_tasks_unique",
        "qmt_operations_scheduler_tasks_contract",
        "supporting_release_trigger_inventory_exact",
        "full_database_trigger_inventory_exact",
        "qmt_reference_physical_schema_and_seal",
        "qmt_history_coverage_physical_schema_and_seal",
        "qmt_history_capability_matrix_fail_closed",
        "qmt_windows_edge_executor_and_last_success",
        "qmt_windows_edge_release_bootstrap",
        "scheduler_task_history_physical_schema",
        "pit_fact_physical_schema_exact",
        "latest_qmt_announcement_full_market_batch",
        "strategy_metric_input_application_state_machine",
        "governance_append_only_application_integrity",
        "strategy_funding_schema_exact",
        "dynamic_shadow_ledger_schema_exact",
        "dynamic_shadow_candidate_plan_fill_forward_ledger",
        "forward_strategy_version_schema", "forward_strategy_version_relations",
        "v2_raw_fill_cash_ledgers_are_immutable",
        "forward_exit_allocation_v3_frozen_schema",
        "forward_exit_allocation_v3_fifo_conservation",
        "qmt_pre_close_v2_frozen_schema",
        "governance_canonical_revision_migration", "authoritative_trade_date",
        "dynamic_strategy_registry", "strategy_lifecycle_domain",
        "strategy_current_versions", "dynamic_combination_registry",
        "combination_lifecycle_domain", "combination_current_versions",
        "all_immutable_version_hashes",
        "all_lifecycle_and_audit_payload_hashes_and_run_bindings",
        "registry_lifecycle_projection_matches_immutable_events",
        "strategy_industry_history_exact_qmt_full_replay",
        "all_governance_detail_snapshot_hashes_and_run_bindings",
        "metric_evidence_state_domain",
        "all_metric_evidence_submission_and_review_audits",
        "metric_and_challenger_evidence_hashes_globally_unique",
        "global_real_order_authority_closed",
        "historical_canonical_run_inventory",
        "authoritative_date_has_one_canonical_revision",
    } | {f"schema_columns:{name}" for name in column_tables} | {
        f"schema_indexes:{name}" for name in index_tables
    } | {f"schema_index_contracts:{name}" for name in index_tables}
    if require_scheduler_heartbeat:
        common.add("linux_standalone_scheduler_heartbeat_current")
    if disposition == "input_not_ready":
        tail = {
            "expected_build_date_run",
            "authoritative_session_windows_qmt_close_attested",
            "qmt_pre_close_v2_rows_bind_current_kline",
            "no_historical_canonical_run",
        }
    elif disposition == "completed":
        tail = {
            "candidate_pool_industry_snapshot_binds_exact_qmt_history",
            "authoritative_session_windows_qmt_close_attested",
            "qmt_pre_close_v2_rows_bind_current_kline",
            "latest_completed_run_identity", "expected_build_date_run_unique",
            "expected_run_identity", "expected_run_completed",
            "expected_run_input_fresh", "completed_run_has_hash_valid_audit",
            "funding_checkpoint_manifest_partition_and_persistence",
            "run_registry_counts", "market_router_snapshot_is_reproducible",
            "current_canonical_metrics_replay_from_raw_ledgers",
            "strategy_health_three_windows", "combination_health_one_snapshot_each",
            "funding_snapshots_use_confirmed_evidence",
            "pool_counts_and_dates_match_run",
            "pool_rows_snapshot_hash_and_funding_references",
            "allocation_candidate_snapshot_and_decision_hashes",
            "paper_allocation_exactly_closed",
            "allocation_targets_are_funding_eligible",
            "allocation_lifecycle_budget_exact",
            "allocation_obeys_market_router_risk_budget",
        }
    else:
        raise ValueError("passing governance health disposition is invalid")
    return frozenset(common | tail)


EXPECTED_WINDOWS = (20, 60, 120)
BUILD_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
RESULT_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
ROUTER_POLICY_VERSION = "strategy_market_router.v1"
ALLOCATION_POLICY_VERSION = "strategy_capital_competition.v5"
LEGACY_ALLOCATION_POLICY_VERSION = "strategy_capital_competition.v4"
ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE = "2026-08-25"
DAILY_NAV_RANKING_BASIS = "DAILY_NET_NAV_20_60_120_V1"
DAILY_NAV_RANKING_BASIS_LABEL = "同口径20/60/120日扣费后日频净值健康分"
ALLOCATION_TYPE_LANE_POLICY = (
    "UNIFIED_RISK_ADJUSTED_QUALITY_MUTUAL_EXCLUSION_V1"
)
POOL_ROW_SCHEMA = "probiga.strategy-pool-row.v1"
POOL_ROW_EVIDENCE_SCHEMA = "probiga.strategy-pool-row-evidence.v1"
POOL_SNAPSHOT_SCHEMA = "probiga.strategy-pool-snapshot.v1"
INDUSTRY_BINDING_SCHEMA = "probiga.governance-industry-binding.v1"
INDUSTRY_SNAPSHOT_SCHEMA = "probiga.governance-industry-snapshot.v2"
L1_INDUSTRY_TYPES = frozenset({"L1", "一级行业", "申万一级", "SW2021"})
QMT_VALIDATED = "QMT_VALIDATED"
_QMT_INDUSTRY_FACT_ID_RE = re.compile(
    r"^qmt:[0-9a-f]{64}:[0-9a-f]{64}$"
)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
PORTFOLIO_RISK_EVIDENCE_SCHEMA = (
    "probiga.strategy-portfolio-risk-evidence.v2"
)
AUTOMATIC_TRANSITION_PLAN_SCHEMA = (
    "probiga.strategy-automatic-transition-plan.v1"
)
VERIFIED_WALK_FORWARD_PROTOCOLS = frozenset(
    {
        "PURGED_WALK_FORWARD_V2",
        "COMBINATORIAL_PURGED_WALK_FORWARD_V2",
    }
)
MARKET_REGIME_STATES = (
    "trend_bullish",
    "high_range",
    "risk_declining",
    "extreme_event",
)
MARKET_RISK_CAP_PCT = {
    "trend_bullish": Decimal("85.0000"),
    "high_range": Decimal("50.0000"),
    "risk_declining": Decimal("20.0000"),
    "extreme_event": Decimal("0.0000"),
}
GLOBAL_PORTFOLIO_POLICY: dict[str, Any] = {
    "maximum_funded_sleeves": 8,
    "maximum_single_stock_weight_pct": 5.0,
    "maximum_industry_weight_pct": 20.0,
    "maximum_pairwise_correlation": 0.80,
    "minimum_pairwise_observations": 60,
    "maximum_pairwise_stock_overlap_pct": 40.0,
    "maximum_planned_positions": 25,
    # New-buy increases are capped; reductions and complete exits are not.
    "maximum_new_buy_turnover_pct": 30.0,
    "maximum_daily_expected_shortfall_95_pct": 3.0,
    "maximum_annualized_volatility_pct": 35.0,
    "reference_capital_cny": 1_000_000.0,
    "board_lot_size": 100,
    "real_order_authority": False,
}
LIFECYCLE_LABELS = {
    "ACTIVE": "正常运行",
    "REDUCE": "降权运行",
    "SHADOW": "影子观察",
    "SUSPENDED": "暂停使用",
    "RETIRED": "已淘汰",
}
LIFECYCLE_RISK_MULTIPLIER = {
    "ACTIVE": Decimal("1.0000"),
    "REDUCE": Decimal("0.5000"),
}


def _one(connection, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    row = connection.execute(text(sql), params or {}).mappings().first()
    return dict(row or {})


def _rows(
    connection, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(sql), params or {}).mappings().all()
    ]


def _safe_exception_message(
    exc: BaseException,
    *,
    error_code: str = "governance_health_check_failed",
) -> str:
    """Return correlatable failure metadata without exception text/SQL/DSNs."""

    return (
        f"error_code={error_code};"
        f"exception_type={type(exc).__name__};"
        f"incident_id={secrets.token_hex(8)}"
    )


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _iso_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _canonical_revision_chain(
    rows: list[dict[str, Any]], *, allow_empty: bool
) -> tuple[bool, dict[str, Any]]:
    """Validate one contiguous, single-canonical revision chain for a day."""

    if not rows:
        return allow_empty, {
            "row_count": 0,
            "canonical_count": 0,
            "allow_empty": allow_empty,
            "errors": [] if allow_empty else ["canonical run is missing"],
        }
    errors: list[str] = []
    by_revision: dict[int, dict[str, Any]] = {}
    canonical_rows: list[dict[str, Any]] = []
    trade_dates = {_iso_date(row.get("trade_date")) for row in rows}
    for row in rows:
        revision = _integer(row.get("run_revision"))
        run_uid = str(row.get("run_uid") or "")
        supersedes = str(row.get("supersedes_run_uid") or "")
        canonical = _integer(row.get("is_canonical"))
        if revision < 1 or revision in by_revision:
            errors.append(f"invalid or duplicate revision {revision}")
        else:
            by_revision[revision] = row
        if not re.fullmatch(r"[0-9a-f]{32}", run_uid):
            errors.append(f"revision {revision} has invalid run_uid")
        if supersedes and not re.fullmatch(r"[0-9a-f]{32}", supersedes):
            errors.append(f"revision {revision} has invalid supersedes_run_uid")
        if canonical not in {0, 1}:
            errors.append(f"revision {revision} has invalid is_canonical")
        elif canonical == 1:
            canonical_rows.append(row)
        if str(row.get("status") or "") != "COMPLETED":
            errors.append(f"revision {revision} is not COMPLETED")
    expected_revisions = list(range(1, len(rows) + 1))
    if sorted(by_revision) != expected_revisions:
        errors.append("run revisions are not a contiguous sequence starting at 1")
    for revision, row in sorted(by_revision.items()):
        supersedes = str(row.get("supersedes_run_uid") or "")
        expected_supersedes = (
            ""
            if revision == 1
            else str(by_revision.get(revision - 1, {}).get("run_uid") or "")
        )
        if supersedes != expected_supersedes:
            errors.append(f"revision {revision} does not supersede revision {revision - 1}")
    if len(trade_dates) != 1 or "" in trade_dates:
        errors.append("revision chain contains inconsistent trade dates")
    if len(canonical_rows) != 1:
        errors.append(f"expected one canonical revision, found {len(canonical_rows)}")
    elif _integer(canonical_rows[0].get("run_revision")) != len(rows):
        errors.append("canonical revision is not the latest revision")
    return not errors, {
        "row_count": len(rows),
        "canonical_count": len(canonical_rows),
        "revisions": sorted(by_revision),
        "canonical_run_uid": (
            canonical_rows[0].get("run_uid") if len(canonical_rows) == 1 else None
        ),
        "errors": errors,
    }


def _resolve_build_sha(explicit: str = "") -> str:
    candidates = (
        explicit,
        os.environ.get("PROBIGA_EXPECTED_GIT_SHA", ""),
        os.environ.get("PROBIGA_BUILD_COMMIT_SHA", ""),
    )
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if normalized:
            if not BUILD_SHA_RE.fullmatch(normalized):
                raise ValueError(
                    "expected build SHA must be one full lowercase Git object id"
                )
            return normalized
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={ROOT.as_posix()}",
            "-C",
            str(ROOT),
            "rev-parse",
            "HEAD",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    normalized = completed.stdout.strip()
    if not BUILD_SHA_RE.fullmatch(normalized):
        raise RuntimeError("could not resolve an exact build commit")
    return normalized


def _authoritative_trade_date(engine, explicit: str = "") -> tuple[str, str]:
    if explicit:
        try:
            parsed = date.fromisoformat(explicit.strip())
        except ValueError as exc:
            raise ValueError("expected trade date must use YYYY-MM-DD") from exc
        authoritative = _iso_date(authoritative_closed_trade_date(engine))
        if parsed.isoformat() != authoritative:
            raise ValueError(
                "expected trade date differs from the authoritative "
                "closed trading-calendar day"
            )
        return parsed.isoformat(), "command_line_verified_against_calendar"

    latest = authoritative_closed_trade_date(engine)
    return _iso_date(latest), "authoritative_closed_trading_calendar_day"


def _authoritative_session_window_attestation_check(
    trade_date: str,
) -> tuple[bool, dict[str, Any]]:
    """Verify exact 20/60/120-session hashes include QMT close attestations."""

    try:
        from server.engine.strategy_governance import (
            _authoritative_session_windows_with_proof,
        )

        windows, row_binding_proof = (
            _authoritative_session_windows_with_proof(trade_date)
        )
    except Exception as exc:
        return False, {"error": _safe_exception_message(exc)}

    window_fields = {
        "schema",
        "window_days",
        "start_date",
        "end_date",
        "session_count",
        "sessions",
        "session_attestations",
        "calendar_manifest_hash",
        "calendar_session_set_hash",
        "calendar_receipt_binding_hash",
        "calendar_receipt",
        "session_hash",
    }
    attestation_fields = {
        "trade_date",
        "attested_bar_count",
        "expected_stock_count",
        "expected_stock_set_hash",
        "batch_count",
        "min_data_version",
        "max_data_version",
        "latest_received_at",
        "pre_close_attestation_protocol",
    }
    errors: list[dict[str, Any]] = []
    summaries: dict[str, Any] = {}
    for expected_window in EXPECTED_WINDOWS:
        window = windows.get(expected_window)
        window = window if isinstance(window, dict) else {}
        sessions = window.get("sessions")
        sessions = sessions if isinstance(sessions, list) else []
        attestations = window.get("session_attestations")
        attestations = attestations if isinstance(attestations, list) else []
        normalized_sessions = [
            _iso_date(value) for value in sessions if _iso_date(value)
        ]
        attestation_valid = (
            len(attestations) == expected_window
            and len(normalized_sessions) == expected_window
        )
        if attestation_valid:
            for index, attestation in enumerate(attestations):
                if not isinstance(attestation, dict):
                    attestation_valid = False
                    break
                if set(attestation) != attestation_fields:
                    attestation_valid = False
                    break
                if (
                    _iso_date(attestation.get("trade_date"))
                    != normalized_sessions[index]
                    or _integer(attestation.get("attested_bar_count")) <= 0
                    or _integer(attestation.get("expected_stock_count")) <= 0
                    or _integer(attestation.get("attested_bar_count"))
                    != _integer(attestation.get("expected_stock_count"))
                    or RESULT_HASH_RE.fullmatch(
                        str(
                            attestation.get("expected_stock_set_hash")
                            or ""
                        )
                    )
                    is None
                    or _integer(attestation.get("batch_count")) <= 0
                    or not str(attestation.get("min_data_version") or "")
                    or not str(attestation.get("max_data_version") or "")
                    or not _normalized_datetime_text(
                        attestation.get("latest_received_at")
                    )
                    or str(
                        attestation.get(
                            "pre_close_attestation_protocol"
                        )
                        or ""
                    )
                    != QMT_PRECLOSE_ATTESTATION_PROTOCOL
                ):
                    attestation_valid = False
                    break
        payload = {
            key: value for key, value in window.items() if key != "session_hash"
        }
        calendar_receipt = window.get("calendar_receipt")
        try:
            from server.engine.strategy_governance import (
                _valid_calendar_receipt_binding,
            )
            calendar_valid = _valid_calendar_receipt_binding(
                calendar_receipt,
                start_date=(normalized_sessions[0]
                            if normalized_sessions else ""),
                end_date=(normalized_sessions[-1]
                          if normalized_sessions else ""),
            )
        except Exception:
            calendar_valid = False
        valid = (
            set(window) == window_fields
            and window.get("schema")
            == "probiga.authoritative-session-window.v1"
            and _integer(window.get("window_days")) == expected_window
            and _integer(window.get("session_count")) == expected_window
            and len(normalized_sessions) == expected_window
            and normalized_sessions == sorted(set(normalized_sessions))
            and _iso_date(window.get("start_date"))
            == normalized_sessions[0]
            and _iso_date(window.get("end_date"))
            == normalized_sessions[-1]
            and normalized_sessions[-1] == trade_date
            and attestation_valid
            and calendar_valid
            and str(window.get("calendar_manifest_hash") or "")
            == str((calendar_receipt or {}).get("manifest_hash") or "")
            and str(window.get("calendar_session_set_hash") or "")
            == str((calendar_receipt or {}).get("session_set_hash") or "")
            and str(window.get("calendar_receipt_binding_hash") or "")
            == str((calendar_receipt or {}).get("binding_hash") or "")
            and RESULT_HASH_RE.fullmatch(
                str(window.get("session_hash") or "")
            )
            is not None
            and _canonical_digest(payload)
            == str(window.get("session_hash") or "")
        )
        summaries[str(expected_window)] = {
            "start_date": window.get("start_date"),
            "end_date": window.get("end_date"),
            "session_count": window.get("session_count"),
            "attestation_count": len(attestations),
            "session_hash": window.get("session_hash"),
            "sessions": normalized_sessions,
            "expected_stock_sets": {
                _iso_date(attestation.get("trade_date")): {
                    "expected_stock_count": _integer(
                        attestation.get("expected_stock_count")
                    ),
                    "expected_stock_set_hash": str(
                        attestation.get("expected_stock_set_hash") or ""
                    ),
                }
                for attestation in attestations
                if isinstance(attestation, dict)
                and _iso_date(attestation.get("trade_date"))
            },
            "pre_close_attestation_protocol": (
                QMT_PRECLOSE_ATTESTATION_PROTOCOL
            ),
        }
        if not valid:
            errors.append(
                {
                    "window_days": expected_window,
                    "reason": "session/QMT close attestation contract invalid",
                }
            )
    return not errors, {
        "windows": summaries,
        "row_binding_proof": row_binding_proof,
        "errors": errors,
    }


def _reused_qmt_row_attestation_binding_check(
    trade_date: str,
    expected_sessions: list[str],
    stock_contracts: dict[str, Any],
    raw_proof: Any,
) -> tuple[bool, dict[str, Any]] | None:
    """Validate the row proof already produced with session windows."""

    if raw_proof is None:
        return None
    if not isinstance(raw_proof, dict):
        return False, {
            "proof_reused": True,
            "error": "authoritative QMT row-binding proof is not an object",
        }
    proof_fields = {
        "schema",
        "as_of_date",
        "start_date",
        "end_date",
        "session_count",
        "protocol_version",
        "source_pre_close_origin",
        "row_run_binding",
        "sessions",
        "proof_hash",
    }
    session_fields = {
        "trade_date",
        "target_stock_count",
        "target_stock_set_hash",
        "completed_attestation_stock_count",
        "completed_attestation_stock_set_hash",
        "exact_attestation_stock_count",
        "exact_attestation_stock_set_hash",
        "attested_bar_count",
        "matching_completed_manifest_run_count",
    }
    proof_sessions = raw_proof.get("sessions")
    proof_sessions = proof_sessions if isinstance(proof_sessions, list) else []
    payload = {
        key: value for key, value in raw_proof.items() if key != "proof_hash"
    }
    errors: list[dict[str, Any]] = []
    if (
        set(raw_proof) != proof_fields
        or raw_proof.get("schema") != "probiga.qmt-row-binding-proof.v1"
        or _iso_date(raw_proof.get("as_of_date")) != trade_date
        or _iso_date(raw_proof.get("start_date"))
        != expected_sessions[0]
        or _iso_date(raw_proof.get("end_date")) != trade_date
        or _integer(raw_proof.get("session_count"))
        != len(expected_sessions)
        or str(raw_proof.get("protocol_version") or "")
        != QMT_PRECLOSE_ATTESTATION_PROTOCOL
        or raw_proof.get("source_pre_close_origin") != "NATIVE_QMT"
        or raw_proof.get("row_run_binding") != "SAME_COMPLETED_RUN_ID"
        or RESULT_HASH_RE.fullmatch(str(raw_proof.get("proof_hash") or ""))
        is None
        or _canonical_digest(payload) != raw_proof.get("proof_hash")
        or len(proof_sessions) != len(expected_sessions)
    ):
        errors.append({
            "reason": "authoritative QMT row-binding proof envelope is invalid"
        })

    total_target = 0
    total_completed = 0
    total_exact = 0
    observed_days: list[str] = []
    for index, session in enumerate(expected_sessions):
        row = proof_sessions[index] if index < len(proof_sessions) else {}
        row = row if isinstance(row, dict) else {}
        day = _iso_date(row.get("trade_date"))
        observed_days.append(day)
        contract = stock_contracts.get(session)
        contract = contract if isinstance(contract, dict) else {}
        target_count = _integer(row.get("target_stock_count"))
        completed_count = _integer(
            row.get("completed_attestation_stock_count")
        )
        exact_count = _integer(row.get("exact_attestation_stock_count"))
        target_hash = str(row.get("target_stock_set_hash") or "")
        completed_hash = str(
            row.get("completed_attestation_stock_set_hash") or ""
        )
        exact_hash = str(row.get("exact_attestation_stock_set_hash") or "")
        total_target += target_count
        total_completed += completed_count
        total_exact += exact_count
        if (
            set(row) != session_fields
            or day != session
            or target_count <= 0
            or completed_count != target_count
            or exact_count != target_count
            or _integer(row.get("attested_bar_count")) != target_count
            or _integer(
                row.get("matching_completed_manifest_run_count")
            )
            <= 0
            or RESULT_HASH_RE.fullmatch(target_hash) is None
            or completed_hash != target_hash
            or exact_hash != target_hash
            or _integer(contract.get("expected_stock_count"))
            != target_count
            or str(contract.get("expected_stock_set_hash") or "")
            != target_hash
        ):
            errors.append({
                "trade_date": session,
                "reason": (
                    "reused authoritative target, same-run completed "
                    "attestation and exact-current contracts differ"
                ),
            })
    if observed_days != expected_sessions:
        errors.append({
            "reason": "authoritative QMT row-binding proof dates differ"
        })
    return not errors, {
        "table_exists": True,
        "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
        "source_pre_close_origin": "NATIVE_QMT",
        "row_run_binding": "SAME_COMPLETED_RUN_ID",
        "proof_reused": True,
        "proof_hash": raw_proof.get("proof_hash"),
        "database_query_count": 0,
        "expected_session_count": len(expected_sessions),
        "covered_session_count": len(expected_sessions) if not errors else 0,
        "target_stock_count": total_target,
        "completed_attestation_stock_count": total_completed,
        "exact_attestation_stock_count": total_exact,
        "errors": errors[:100],
    }


def _qmt_row_attestation_binding_check(
    trade_date: str,
    session_window_detail: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """Require identical target, completed-history and exact-current sets."""

    windows = session_window_detail.get("windows")
    windows = windows if isinstance(windows, dict) else {}
    longest = windows.get(str(max(EXPECTED_WINDOWS)))
    longest = longest if isinstance(longest, dict) else {}
    expected_sessions = [
        _iso_date(value)
        for value in longest.get("sessions", [])
        if _iso_date(value)
    ]
    if (
        len(expected_sessions) != max(EXPECTED_WINDOWS)
        or expected_sessions != sorted(set(expected_sessions))
        or expected_sessions[-1:] != [trade_date]
    ):
        return False, {
            "error": "authoritative session dates are unavailable",
            "expected_session_count": max(EXPECTED_WINDOWS),
            "observed_session_count": len(expected_sessions),
        }
    stock_contracts = longest.get("expected_stock_sets")
    stock_contracts = (
        stock_contracts if isinstance(stock_contracts, dict) else {}
    )
    if set(stock_contracts) != set(expected_sessions):
        return False, {
            "error": "session expected-stock contracts are unavailable",
            "expected_session_count": len(expected_sessions),
            "contract_session_count": len(stock_contracts),
        }
    reused = _reused_qmt_row_attestation_binding_check(
        trade_date,
        expected_sessions,
        stock_contracts,
        session_window_detail.get("row_binding_proof"),
    )
    if reused is not None:
        return reused

    try:
        from server.engine.strategy_governance import _db_read

        table_rows = _db_read(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema=DATABASE() "
            "AND table_name IN ('qmt_kline_attestation_row', "
            "'qmt_kline_attestation_run')"
        )
        table_exists = (
            len(table_rows) == 1
            and _integer(table_rows[0].get("cnt")) == 2
        )
        if not table_exists:
            return False, {
                "table_exists": False,
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
                "error": "QMT attestation row/run table inventory is missing",
            }

        universe_rows = _db_read(
            "SELECT u.trade_date, u.stock_code, "
            "MAX(u.in_target) AS in_target, "
            "MAX(u.in_completed_attestation) "
            "AS in_completed_attestation, "
            "MAX(u.in_exact_attestation) AS in_exact_attestation "
            "FROM ("
            "SELECT k.trade_date, k.stock_code, 1 AS in_target, "
            "0 AS in_completed_attestation, 0 AS in_exact_attestation "
            "FROM sm_stock_kline k "
            "WHERE k.k_type=1 AND k.adjust_type=0 "
            "AND k.stock_code REGEXP '^(0|3|6)' "
            "AND k.trade_date BETWEEN :start_date AND :trade_date "
            "UNION ALL "
            "SELECT a.trade_date, a.stock_code, 0 AS in_target, "
            "1 AS in_completed_attestation, 0 AS in_exact_attestation "
            "FROM qmt_kline_attestation_row a "
            "JOIN qmt_kline_attestation_run r ON r.run_id=a.run_id "
            "AND BINARY r.run_id=BINARY a.run_id "
            "AND r.status='COMPLETED' "
            "AND r.provider='gj_big_qmt_inner' "
            "AND BINARY JSON_UNQUOTE(JSON_EXTRACT("
            "r.tolerance_json, '$.attestation_protocol'))="
            "BINARY :protocol_version "
            "AND BINARY JSON_UNQUOTE(JSON_EXTRACT("
            "r.tolerance_json, '$.universe_manifest_schema'))="
            "BINARY 'probiga.qmt-daily-universe.v1' "
            "AND JSON_EXTRACT(r.tolerance_json, CONCAT("
            "'$.daily_universe.\"', "
            "DATE_FORMAT(a.trade_date, '%Y-%m-%d'), '\"')) "
            "IS NOT NULL "
            "AND a.trade_date BETWEEN r.start_date AND r.end_date "
            "WHERE BINARY a.protocol_version=BINARY :protocol_version "
            "AND a.stock_code REGEXP '^(0|3|6)' "
            "AND a.trade_date BETWEEN :start_date AND :trade_date "
            "UNION ALL "
            "SELECT k.trade_date, k.stock_code, 0 AS in_target, "
            "0 AS in_completed_attestation, 1 AS in_exact_attestation "
            "FROM sm_stock_kline k "
            "JOIN qmt_kline_attestation_row a ON a.target_id=k.id "
            "AND a.qmt_id>0 "
            "AND a.trade_date=k.trade_date "
            "AND BINARY a.stock_code=BINARY k.stock_code "
            "AND BINARY a.protocol_version=BINARY :protocol_version "
            "AND BINARY a.source_data_version=BINARY k.data_version "
            "AND BINARY a.source_pre_close_origin=BINARY 'NATIVE_QMT' "
            "AND a.source_pre_close=k.pre_close "
            "AND a.attested_open=k.`open` "
            "AND a.attested_close=k.`close` "
            "AND a.attested_high=k.`high` "
            "AND a.attested_low=k.`low` "
            "AND a.attested_volume=k.volume "
            "AND a.attested_amount=k.amount "
            "AND BINARY a.attestation_id=BINARY SHA2(CONCAT_WS('|', "
            "a.protocol_version, a.target_id, a.qmt_id, "
            "a.source_data_version, a.source_pre_close, "
            "a.attested_open, a.attested_close, a.attested_high, "
            "a.attested_low, a.attested_volume, a.attested_amount), 256) "
            "JOIN qmt_kline_attestation_run r ON r.run_id=a.run_id "
            "AND BINARY r.run_id=BINARY a.run_id "
            "AND r.status='COMPLETED' "
            "AND r.provider='gj_big_qmt_inner' "
            "AND BINARY JSON_UNQUOTE(JSON_EXTRACT("
            "r.tolerance_json, '$.attestation_protocol'))="
            "BINARY :protocol_version "
            "AND BINARY JSON_UNQUOTE(JSON_EXTRACT("
            "r.tolerance_json, '$.universe_manifest_schema'))="
            "BINARY 'probiga.qmt-daily-universe.v1' "
            "AND JSON_EXTRACT(r.tolerance_json, CONCAT("
            "'$.daily_universe.\"', "
            "DATE_FORMAT(a.trade_date, '%Y-%m-%d'), '\"')) "
            "IS NOT NULL "
            "AND a.trade_date BETWEEN r.start_date AND r.end_date "
            "WHERE k.k_type=1 AND k.adjust_type=0 "
            "AND k.stock_code REGEXP '^(0|3|6)' "
            "AND k.data_source='gj_big_qmt_inner' "
            "AND k.quality_status='QMT_ATTESTED' "
            "AND k.permission_status='SUPPORTED' "
            "AND k.source_time IS NOT NULL AND k.received_at IS NOT NULL "
            "AND k.source_time>=TIMESTAMP(k.trade_date, '15:00:00') "
            "AND k.received_at>=k.source_time "
            "AND k.batch_id<>'' AND k.data_version<>'' "
            "AND k.trade_date BETWEEN :start_date AND :trade_date"
            ") u GROUP BY u.trade_date, u.stock_code "
            "ORDER BY u.trade_date, u.stock_code",
            {
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
                "start_date": expected_sessions[0],
                "trade_date": trade_date,
            },
        )
        completed_run_rows = _db_read(
            "SELECT run_id, start_date, end_date, tolerance_json "
            "FROM qmt_kline_attestation_run "
            "WHERE status='COMPLETED' "
            "AND provider='gj_big_qmt_inner' "
            "AND end_date>=:start_date AND start_date<=:trade_date "
            "AND BINARY JSON_UNQUOTE(JSON_EXTRACT("
            "tolerance_json, '$.attestation_protocol'))="
            "BINARY :protocol_version "
            "ORDER BY start_date, end_date, run_id",
            {
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
                "start_date": expected_sessions[0],
                "trade_date": trade_date,
            },
        )
    except Exception as exc:
        return False, {
            "table_exists": None,
            "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            "error": _safe_exception_message(exc),
        }

    by_date: dict[str, dict[str, set[str]]] = {
        session: {
            "target": set(),
            "completed_attestation": set(),
            "exact_attestation": set(),
        }
        for session in expected_sessions
    }
    errors: list[dict[str, Any]] = []
    manifest_contracts: dict[str, set[tuple[int, str]]] = {
        session: set() for session in expected_sessions
    }
    from tools.attest_qmt_daily_kline import validated_universe_manifest

    for run_row in completed_run_rows:
        run_id = str(run_row.get("run_id") or "")
        start_date = _iso_date(run_row.get("start_date"))
        end_date = _iso_date(run_row.get("end_date"))
        try:
            manifest = validated_universe_manifest(
                run_row.get("tolerance_json"),
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            errors.append(
                {
                    "run_id": run_id,
                    "reason": (
                        "completed QMT run universe manifest is invalid: "
                        + _safe_exception_message(exc)
                    ),
                }
            )
            continue
        for day, contract in manifest.items():
            if day in manifest_contracts:
                manifest_contracts[day].add(
                    (
                        _integer(contract.get("stock_count")),
                        str(contract.get("stock_set_hash") or ""),
                    )
                )
    extra_dates: set[str] = set()
    for row in universe_rows:
        day = _iso_date(row.get("trade_date"))
        if day not in by_date:
            if day:
                extra_dates.add(day)
            continue
        stock_code = str(row.get("stock_code") or "").strip()
        flags = {
            "target": _integer(row.get("in_target")),
            "completed_attestation": _integer(
                row.get("in_completed_attestation")
            ),
            "exact_attestation": _integer(
                row.get("in_exact_attestation")
            ),
        }
        if (
            not stock_code
            or any(flag not in {0, 1} for flag in flags.values())
            or not any(flags.values())
        ):
            errors.append(
                {
                    "trade_date": day,
                    "stock_code": stock_code,
                    "reason": "QMT three-set membership row is invalid",
                }
            )
            continue
        for set_name, present in flags.items():
            if present:
                by_date[day][set_name].add(stock_code)

    for session in expected_sessions:
        day_sets = by_date[session]
        target_set = day_sets["target"]
        completed_set = day_sets["completed_attestation"]
        exact_set = day_sets["exact_attestation"]
        contract = stock_contracts.get(session)
        contract = contract if isinstance(contract, dict) else {}
        expected_hash = _canonical_digest(
            {
                "schema": "probiga.qmt-expected-stock-set.v1",
                "trade_date": session,
                "stock_codes": sorted(target_set),
            }
        )
        accepted_manifest_contract = (len(target_set), expected_hash)
        if (
            not target_set
            or target_set != completed_set
            or target_set != exact_set
            or _integer(contract.get("expected_stock_count"))
            != len(target_set)
            or str(contract.get("expected_stock_set_hash") or "")
            != expected_hash
            or manifest_contracts.get(session)
            != {accepted_manifest_contract}
        ):
            errors.append(
                {
                    "trade_date": session,
                    "target_stock_count": len(target_set),
                    "completed_attestation_stock_count": len(completed_set),
                    "exact_attestation_stock_count": len(exact_set),
                    "contract_stock_count": _integer(
                        contract.get("expected_stock_count")
                    ),
                    "expected_stock_set_hash": expected_hash,
                    "contract_stock_set_hash": str(
                        contract.get("expected_stock_set_hash") or ""
                    ),
                    "completed_run_manifest_contracts": sorted(
                        manifest_contracts.get(session, set())
                    ),
                    "reason": (
                        "production raw target, completed historical V2 "
                        "attestation and exact current binding stock sets "
                        "must be non-empty and identical"
                    ),
                }
            )
    if extra_dates:
        errors.append(
            {
                "reason": "QMT attestation coverage contains non-session dates",
                "trade_dates": sorted(extra_dates)[:20],
            }
        )
    return not errors, {
        "table_exists": True,
        "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
        "source_pre_close_origin": "NATIVE_QMT",
        "row_run_binding": "SAME_COMPLETED_RUN_ID",
        "proof_reused": False,
        "database_query_count": 3,
        "expected_session_count": len(expected_sessions),
        "covered_session_count": sum(
            bool(day_sets["target"]) for day_sets in by_date.values()
        ),
        "target_stock_count": sum(
            len(day_sets["target"]) for day_sets in by_date.values()
        ),
        "completed_attestation_stock_count": sum(
            len(day_sets["completed_attestation"])
            for day_sets in by_date.values()
        ),
        "exact_attestation_stock_count": sum(
            len(day_sets["exact_attestation"])
            for day_sets in by_date.values()
        ),
        "errors": errors[:100],
    }


def _qmt_attestation_frozen_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Require frozen QMT proof tables plus all database mutation guards."""

    from tools.attest_qmt_daily_kline import (
        ATTESTATION_TRIGGER_STATEMENTS,
        QmtAttestationSchemaError,
        validate_attestation_schema,
    )
    from tools.prepare_strategy_governance_schema import (
        EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH,
        _frozen_non_v3_release_trigger_contracts,
        _non_v3_trigger_contracts,
        validate_release_trigger_contracts,
    )

    try:
        schema_detail = validate_attestation_schema(
            connection,
            require_triggers=False,
        )
        names = frozenset(ATTESTATION_TRIGGER_STATEMENTS)
        release_contracts = _frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        contracts = {
            name: release_contracts[name]
            for name in names
            if name in release_contracts
        }
        if (
            len(names) != 6
            or set(contracts) != set(names)
            or any(
                contract.owner != "qmt_attestation"
                for contract in contracts.values()
            )
        ):
            raise RuntimeError("QMT attestation trigger contract differs")
        trigger_detail = validate_release_trigger_contracts(
            connection,
            required=contracts,
            optional={},
            controlled_contracts=contracts,
        )
        return True, {
            **schema_detail,
            **trigger_detail,
            "trigger_names": sorted(names),
            "trigger_count": len(names),
            "expected_trigger_count": 6,
            "database_triggers_required": True,
            "release_trigger_source_contract_hash": (
                EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
            ),
            "immutability_enforcement": (
                "mysql_frozen_completed_run_and_row_mutation_guards"
            ),
        }
    except QmtAttestationSchemaError as exc:
        return False, {
            "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            "trigger_count": 0,
            "expected_trigger_count": 6,
            "database_triggers_required": True,
            "errors": [_safe_exception_message(
                exc, error_code="qmt_attestation_schema_validation_failed"
            )],
        }
    except Exception as exc:
        return False, {
            "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            "trigger_count": 0,
            "expected_trigger_count": 6,
            "database_triggers_required": True,
            "errors": [_safe_exception_message(exc)],
        }


def _supporting_release_trigger_inventory_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Require the release broker's exact supporting-trigger contract."""

    try:
        from tools.prepare_strategy_governance_schema import (
            EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT,
            EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH,
            _frozen_non_v3_release_trigger_contracts,
            _non_v3_trigger_contracts,
            validate_release_trigger_contracts,
        )

        contracts = _frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        owner_counts: dict[str, int] = {}
        for contract in contracts.values():
            owner_counts[contract.owner] = (
                owner_counts.get(contract.owner, 0) + 1
            )
        expected_owner_counts = {
            "market_field_capture": 5,
            "pit_facts": 6,
            "qmt_attestation": 6,
            "qmt_history_coverage": 4,
            "qmt_membership": 6,
            "qmt_reference": 10,
            "scheduler_task_history": 2,
            "schema_recovery_evidence": 2,
            "strategy_governance": 40,
        }
        if (
            len(contracts) != EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
            or owner_counts != expected_owner_counts
        ):
            raise RuntimeError("supporting release trigger inventory differs")
        detail = validate_release_trigger_contracts(
            connection,
            required=contracts,
            optional={},
            controlled_contracts=contracts,
        )
        return True, {
            **detail,
            "expected_trigger_count": EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT,
            "owner_counts": owner_counts,
            "expected_owner_counts": expected_owner_counts,
            "source_contract_hash": (
                EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
            ),
            "database_triggers_required": True,
        }
    except Exception as exc:
        return False, {
            "trigger_count": 0,
            "expected_trigger_count": 81,
            "expected_owner_counts": {
                "market_field_capture": 5,
                "pit_facts": 6,
                "qmt_attestation": 6,
                "qmt_history_coverage": 4,
                "qmt_membership": 6,
                "qmt_reference": 10,
                "scheduler_task_history": 2,
                "schema_recovery_evidence": 2,
                "strategy_governance": 40,
            },
            "database_triggers_required": True,
            "errors": [_safe_exception_message(exc)],
        }


def _full_database_trigger_inventory_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Re-attest the base inventory plus the complete applied V4 group."""

    try:
        from tools.prepare_strategy_governance_schema import (
            EXPECTED_FULL_RELEASE_TRIGGER_COUNT,
            EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH,
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT,
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH,
            EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH,
            EXPECTED_OPTIONAL_V4_TRIGGER_COUNT,
            EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH,
            _final_v3_trigger_contracts,
            _frozen_non_v3_release_trigger_contracts,
            _non_v3_trigger_contracts,
            validate_full_database_trigger_inventory,
        )

        managed = {
            **_final_v3_trigger_contracts(),
            **_frozen_non_v3_release_trigger_contracts(
                _non_v3_trigger_contracts()
            ),
        }
        detail = validate_full_database_trigger_inventory(
            connection,
            managed_contracts=managed,
            include_applied_v4=True,
        )
        optional_v4_count = detail.get("optional_v4_count")
        expected_count = (
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
            if optional_v4_count == EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
            else EXPECTED_FULL_RELEASE_TRIGGER_COUNT
        )
        expected_nameset_hash = (
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH
            if optional_v4_count == EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
            else EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH
        )
        exact = (
            type(optional_v4_count) is int
            and optional_v4_count in {0, EXPECTED_OPTIONAL_V4_TRIGGER_COUNT}
            and detail.get("expected_count") == expected_count
            and detail.get("observed_count") == expected_count
            and detail.get("v2_count") == 41
            and detail.get("managed_count") == 101
            and detail.get("nameset_sha256") == expected_nameset_hash
            and detail.get("base_nameset_sha256")
            == EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH
            and detail.get("v2_source_contract_sha256")
            == EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH
            and detail.get("managed_source_contract_sha256")
            == EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH
            and detail.get("metadata_frozen") is True
            and detail.get("read_only") is True
        )
        if not exact:
            raise RuntimeError("full release trigger inventory differs")
        return True, detail
    except Exception as exc:
        return False, {
            "expected_count": 142,
            "observed_count": 0,
            "v2_count": 41,
            "managed_count": 101,
            "optional_v4_count": 0,
            "nameset_sha256": (
                "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
            ),
            "base_nameset_sha256": (
                "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
            ),
            "v2_source_contract_sha256": (
                "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883"
            ),
            "managed_source_contract_sha256": (
                "7e42c91e534dd3d61d212f0c16fa7297c29b8f4756812de2e072874179537423"
            ),
            "metadata_frozen": False,
            "read_only": True,
            "errors": [_safe_exception_message(exc)],
        }


def _qmt_reference_frozen_schema_check(
    engine,
) -> tuple[bool, dict[str, Any]]:
    """Verify the live QMT reference schema and its immutable physical seal."""

    try:
        from tools.sync_guojin_qmt_reference_data import (
            REFERENCE_SCHEMA_CONTRACT_HASH,
            REFERENCE_SCHEMA_CONTRACT_KEY,
            REFERENCE_TABLE_NAMES,
            REFERENCE_TRIGGER_NAMES,
            validate_reference_tables,
        )

        validate_reference_tables(engine, verify_triggers=True)
        if len(REFERENCE_TRIGGER_NAMES) != 10:
            raise RuntimeError("QMT reference trigger inventory differs")
        return True, {
            "contract_key": REFERENCE_SCHEMA_CONTRACT_KEY,
            "contract_hash": REFERENCE_SCHEMA_CONTRACT_HASH,
            "table_names": list(REFERENCE_TABLE_NAMES),
            "table_count": len(REFERENCE_TABLE_NAMES),
            "trigger_names": list(REFERENCE_TRIGGER_NAMES),
            "trigger_count": len(REFERENCE_TRIGGER_NAMES),
            "expected_trigger_count": 10,
            "physical_schema_verified": True,
            "physical_seal_verified": True,
        }
    except Exception as exc:
        return False, {
            "trigger_count": 0,
            "expected_trigger_count": 10,
            "physical_schema_verified": False,
            "physical_seal_verified": False,
            "errors": [_safe_exception_message(exc)],
        }


def _qmt_history_coverage_frozen_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify primary-schema coverage tables and all four append-only seals."""

    try:
        from server.common.qmt_history_coverage import (
            COVERAGE_TABLE_NAMES,
            COVERAGE_TRIGGER_NAMES,
            validate_coverage_schema,
        )

        detail = validate_coverage_schema(
            connection,
            require_triggers=True,
        )
        exact = (
            detail.get("database") == "probiga"
            and detail.get("table_names") == list(COVERAGE_TABLE_NAMES)
            and detail.get("table_count") == 2
            and detail.get("foreign_key_count") == 3
            and detail.get("trigger_names") == list(COVERAGE_TRIGGER_NAMES)
            and detail.get("trigger_count") == 4
            and detail.get("runtime_ddl_required") is False
            and detail.get("physical_schema_verified") is True
            and detail.get("physical_seal_verified") is True
        )
        if not exact:
            raise RuntimeError("QMT history coverage schema differs")
        return True, detail
    except Exception as exc:
        return False, {
            "database": "probiga",
            "table_count": 0,
            "expected_table_count": 2,
            "trigger_count": 0,
            "expected_trigger_count": 4,
            "runtime_ddl_required": False,
            "physical_schema_verified": False,
            "physical_seal_verified": False,
            "errors": [_safe_exception_message(exc)],
        }


def _qmt_history_capability_matrix_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    try:
        from server.common.qmt_history_capabilities import (
            assess_qmt_history_capabilities,
        )

        evidence_healthy, detail = assess_qmt_history_capabilities(connection)
        datasets = list(detail.get("datasets") or ())
        fail_closed = all(
            bool(item.get("strategy_eligible"))
            or str(item.get("status") or "") == "UNAVAILABLE"
            for item in datasets
        )
        detail = {
            **detail,
            "fail_closed_verified": fail_closed,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        return bool(evidence_healthy and fail_closed), detail
    except Exception as exc:
        return False, {
            "status": "UNHEALTHY",
            "strategy_eligible_dataset_count": 0,
            "fail_closed_verified": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "errors": [
                _safe_exception_message(
                    exc,
                    error_code="qmt_history_capability_matrix_failed",
                )
            ],
        }


def _scheduler_task_history_frozen_schema_check(
    engine,
) -> tuple[bool, dict[str, Any]]:
    try:
        from server.common.scheduler_task_history_schema import (
            validate_scheduler_task_history_schema,
        )

        detail = validate_scheduler_task_history_schema(engine)
        return True, detail
    except Exception as exc:
        return False, {
            "physical_contract_verified": False,
            "runtime_ddl_required": False,
            "read_only": True,
            "errors": [
                _safe_exception_message(
                    exc,
                    error_code="scheduler_task_history_schema_failed",
                )
            ],
        }


def _pit_fact_frozen_schema_check(
    engine,
) -> tuple[bool, dict[str, Any]]:
    """Require all PIT fact tables, columns and six append-only guards."""

    try:
        from server.common.pit_facts import (
            PIT_FACT_TABLE_NAMES,
            PIT_FACT_TRIGGER_STATEMENTS,
            pit_fact_schema_health,
        )

        detail = pit_fact_schema_health(engine)
        exact = (
            detail.get("valid") is True
            and int(detail.get("table_count") or 0)
            == len(PIT_FACT_TABLE_NAMES)
            and int(detail.get("trigger_count") or 0)
            == len(PIT_FACT_TRIGGER_STATEMENTS)
            and len(PIT_FACT_TRIGGER_STATEMENTS) == 6
            and not detail.get("missing_tables")
            and not detail.get("missing_columns")
            and not detail.get("missing_triggers")
            and RESULT_HASH_RE.fullmatch(
                str(detail.get("contract_hash") or "")
            )
            is not None
        )
        return exact, {
            **detail,
            "expected_table_count": len(PIT_FACT_TABLE_NAMES),
            "expected_trigger_count": 6,
            "physical_schema_verified": bool(exact),
        }
    except Exception as exc:
        return False, {
            "valid": False,
            "expected_trigger_count": 6,
            "physical_schema_verified": False,
            "errors": [_safe_exception_message(exc)],
        }


def _latest_qmt_announcement_batch_check(
    engine,
    trade_date: str,
) -> tuple[bool, dict[str, Any]]:
    """Validate one strict QMT-first authoritative announcement batch.

    The shared read-only validator owns calendar, target-to-decision time
    selection, catalog-exact coverage and global-root verification.  Keeping
    health on that path prevents a weekend fallback capture from being rejected
    merely because its real ``known_at`` is later than the closed trade date.
    """

    blocked = {
        "status": "DATA_BLOCKED",
        "trade_date": str(trade_date or ""),
        "source": "",
        "funding_eligible": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if not _iso_date(trade_date):
        return False, {
            **blocked,
            "reason_code": "QMT_ANNOUNCEMENT_TRADE_DATE_UNAVAILABLE",
        }
    try:
        from server.common.qmt_announcement_pit import (
            ANNOUNCEMENT_FALLBACK_REASON_CODES,
            AUTHORITATIVE_ANNOUNCEMENT_SOURCES,
            QMTAnnouncementBlocked,
            QMT_ANNOUNCEMENT_SOURCE,
        )
        from tools.sync_qmt_announcement_pit import (
            validate_existing_complete_qmt_announcement_batch,
            validate_existing_task_result,
        )

        proof = validate_existing_complete_qmt_announcement_batch(
            engine,
            expected_trade_date=trade_date,
        )
        if validate_existing_task_result(
            proof,
            0,
            expected_trade_date=trade_date,
        ) != "complete":
            raise ValueError("announcement existing-batch disposition differs")

        source_name = str(proof.get("source") or "")
        fallback_valid = bool(
            source_name == QMT_ANNOUNCEMENT_SOURCE
            or (
                source_name in AUTHORITATIVE_ANNOUNCEMENT_SOURCES
                and proof.get("primary_source") == QMT_ANNOUNCEMENT_SOURCE
                and str(proof.get("fallback_reason") or "")
                in ANNOUNCEMENT_FALLBACK_REASON_CODES
            )
        )
        stock_count = _integer(proof.get("stock_count"))
        coverage_count = _integer(proof.get("coverage_count"))
        exact = (
            proof.get("status") == "COMPLETE"
            and proof.get("trade_date") == trade_date
            and source_name in AUTHORITATIVE_ANNOUNCEMENT_SOURCES
            and fallback_valid
            and proof.get("funding_eligible") is True
            and proof.get("database_writes") is False
            and proof.get("automatic_real_order_submission") is False
            and proof.get("real_order_authority") is False
            and stock_count > 0
            and coverage_count == stock_count
            and RESULT_HASH_RE.fullmatch(
                str(proof.get("batch_root_hash") or "")
            )
            is not None
        )
        return exact, {
            **proof,
            "trade_date": trade_date,
            "source": source_name,
            # Preserve the health/deploy detail aliases while the shared task
            # result uses stock_count/coverage_count.
            "catalog_member_count": stock_count,
            "coverage_row_count": coverage_count,
            "funding_eligible": bool(exact),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except QMTAnnouncementBlocked as exc:
        return False, {
            **blocked,
            "reason_code": str(
                getattr(exc, "reason_code", "")
                or "QMT_ANNOUNCEMENT_DATA_BLOCKED"
            ),
        }
    except Exception as exc:
        return False, {
            **blocked,
            "reason_code": "QMT_ANNOUNCEMENT_HEALTH_VALIDATION_FAILED",
            "errors": [_safe_exception_message(exc)],
        }


def _normalized_metric_input_trigger_body(value: Any) -> str:
    pieces = re.split(r"('(?:''|[^'])*')", str(value or ""))
    for index in range(0, len(pieces), 2):
        outside = pieces[index].replace("`", "")
        outside = re.sub(
            r"\bSQLSTATE\s+VALUE\b",
            "SQLSTATE",
            outside,
            flags=re.IGNORECASE,
        )
        outside = re.sub(r"\s+", " ", outside).lower()
        outside = re.sub(r"\s*=\s*", "=", outside)
        outside = re.sub(r"\s*;\s*", ";", outside)
        pieces[index] = outside
    return "".join(pieces).strip()


def _metric_input_review_trigger_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Require the two constrained metric review/delete database guards."""

    try:
        from server.engine.strategy_governance import (
            METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH,
            METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS,
        )
        from tools.prepare_strategy_governance_schema import (
            EXPECTED_CHARACTER_SET_CLIENT,
            EXPECTED_COLLATION_CONNECTION,
            EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH,
            EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH,
            EXPECTED_DATABASE_COLLATION,
            EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH,
            EXPECTED_GOVERNANCE_TRIGGER_NAMES,
            EXPECTED_MIGRATOR_USER,
            EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES,
            EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH,
            EXPECTED_SQL_MODE,
            _frozen_governance_release_trigger_contracts,
            _non_v3_trigger_contracts,
            _normalized_trigger_body,
            _release_trigger_source_contract_hash,
            validate_release_trigger_contracts,
        )

        names = set(METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS)
        release_contracts = _frozen_governance_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        contracts = {
            name: contract
            for name, contract in release_contracts.items()
            if name in names
        }
        if (
            len(names) != EXPECTED_METRIC_REVIEW_TRIGGER_COUNT
            or names != EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
            or set(contracts) != names
            or set(release_contracts) != EXPECTED_GOVERNANCE_TRIGGER_NAMES
            or METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH
            != EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
        ):
            raise RuntimeError("metric trigger contract inventory differs")
        source_contract_hash = _release_trigger_source_contract_hash(
            release_contracts
        )
        if (
            source_contract_hash
            != EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
        ):
            raise RuntimeError("governance trigger source contract differs")
        metadata = validate_release_trigger_contracts(
            connection,
            required=contracts,
            optional={},
            controlled_contracts=contracts,
        )
        members = [{
            "name": name,
            "timing": contract.timing,
            "event": contract.event,
            "table": contract.table,
            "body_hash": hashlib.sha256(
                _normalized_trigger_body(contract, contract.body).encode(
                    "utf-8"
                )
            ).hexdigest(),
        } for name, contract in sorted(contracts.items())]
        contract_hash = _canonical_digest({
            "schema": "probiga.strategy-metric-review-trigger-contract.v1",
            "members": members,
            "definer": EXPECTED_MIGRATOR_USER,
            "sql_mode": EXPECTED_SQL_MODE,
            "character_set_client": EXPECTED_CHARACTER_SET_CLIENT,
            "collation_connection": EXPECTED_COLLATION_CONNECTION,
            "database_collation": EXPECTED_DATABASE_COLLATION,
        })
        if contract_hash != EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH:
            raise RuntimeError("metric trigger physical contract differs")
    except Exception as exc:
        return False, {
            "table": "st_strategy_metric_input",
            "trigger_count": 0,
            "expected_trigger_count": EXPECTED_METRIC_REVIEW_TRIGGER_COUNT,
            "database_triggers_required": True,
            "enforcement": "mysql_constrained_review_and_delete_rejection",
            "errors": [type(exc).__name__],
        }
    return True, {
        **metadata,
        "table": "st_strategy_metric_input",
        "trigger_names": sorted(contracts),
        "trigger_count": len(contracts),
        "expected_trigger_count": EXPECTED_METRIC_REVIEW_TRIGGER_COUNT,
        "contract_hash": contract_hash,
        "source_contract_hash": source_contract_hash,
        "core_append_only_contract_hash": (
            EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        ),
        "core_metric_review_contract_hash": (
            EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
        ),
        "database_triggers_required": True,
        "enforcement": "mysql_constrained_review_and_delete_rejection",
        "errors": [],
    }


def _governance_append_only_trigger_check(
    connection,
    funding_schema_detail: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Require every append-only/delete guard with frozen MySQL metadata."""

    try:
        from server.engine.strategy_governance import (
            GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH,
            GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS,
        )
        from tools.prepare_strategy_governance_schema import (
            EXPECTED_CHARACTER_SET_CLIENT,
            EXPECTED_COLLATION_CONNECTION,
            EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH,
            EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH,
            EXPECTED_DATABASE_COLLATION,
            EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH,
            EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH,
            EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES,
            EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH,
            EXPECTED_GOVERNANCE_TRIGGER_NAMES,
            EXPECTED_MIGRATOR_USER,
            EXPECTED_SQL_MODE,
            _frozen_governance_release_trigger_contracts,
            _non_v3_trigger_contracts,
            _normalized_trigger_body,
            _release_trigger_source_contract_hash,
            validate_release_trigger_contracts,
        )

        names = set(GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS)
        release_contracts = _frozen_governance_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        contracts = {
            name: contract
            for name, contract in release_contracts.items()
            if name in names
        }
        if (
            len(names) != EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_COUNT
            or names != EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
            or set(contracts) != names
            or set(release_contracts) != EXPECTED_GOVERNANCE_TRIGGER_NAMES
            or GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH
            != EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        ):
            raise RuntimeError("append-only trigger contract inventory differs")
        source_contract_hash = _release_trigger_source_contract_hash(
            release_contracts
        )
        if (
            source_contract_hash
            != EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
        ):
            raise RuntimeError("governance trigger source contract differs")
        detail = (
            dict(funding_schema_detail)
            if funding_schema_detail is not None
            else validate_strategy_funding_checkpoint_schema(connection)
        )
        if (
            _integer(detail.get("trigger_count"))
            != len(FUNDING_CHECKPOINT_TRIGGER_CONTRACTS)
            or detail.get("contract_hash")
            != EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
        ):
            raise RuntimeError("funding trigger contract differs")
        metadata = validate_release_trigger_contracts(
            connection,
            required=contracts,
            optional={},
            controlled_contracts=contracts,
        )
        members = [{
            "name": name,
            "timing": contract.timing,
            "event": contract.event,
            "table": contract.table,
            "body_hash": hashlib.sha256(
                _normalized_trigger_body(contract, contract.body).encode(
                    "utf-8"
                )
            ).hexdigest(),
        } for name, contract in sorted(contracts.items())]
        contract_hash = _canonical_digest({
            "schema": "probiga.strategy-append-only-trigger-contract.v1",
            "members": members,
            "definer": EXPECTED_MIGRATOR_USER,
            "sql_mode": EXPECTED_SQL_MODE,
            "character_set_client": EXPECTED_CHARACTER_SET_CLIENT,
            "collation_connection": EXPECTED_COLLATION_CONNECTION,
            "database_collation": EXPECTED_DATABASE_COLLATION,
        })
        if (
            contract_hash
            != EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH
        ):
            raise RuntimeError("append-only trigger physical contract differs")
    except Exception as exc:
        return False, {
            "trigger_count": 0,
            "expected_trigger_count": (
                EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_COUNT
            ),
            "database_triggers_required": True,
            "enforcement": "mysql_frozen_row_mutation_contract",
            "errors": [type(exc).__name__],
        }
    return True, {
        **metadata,
        "trigger_names": sorted(contracts),
        "trigger_count": len(contracts),
        "expected_trigger_count": EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_COUNT,
        "total_governance_trigger_count": (
            len(contracts) + EXPECTED_METRIC_REVIEW_TRIGGER_COUNT
        ),
        "contract_hash": contract_hash,
        "source_contract_hash": source_contract_hash,
        "core_contract_hash": (
            EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        ),
        "core_metric_review_contract_hash": (
            EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
        ),
        "funding_contract_hash": str(detail.get("contract_hash") or ""),
        "database_triggers_required": True,
        "enforcement": "mysql_frozen_row_mutation_contract",
        "errors": [],
    }


def _strategy_funding_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Delegate both normalized funding tables to the frozen validator."""

    try:
        detail = validate_strategy_funding_checkpoint_schema(connection)
    except Exception as exc:
        return False, {
            "expected_contract_hash": FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
            "errors": [type(exc).__name__],
        }
    expected_budgets = {
        "checkpoint_target_average_bytes": FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
        "checkpoint_total_target_bytes": FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
        "checkpoint_total_hard_bytes": FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
        "batch_max_rows": FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
        "batch_max_bytes": FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
        "manifest_max_bytes": FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
        "audit_max_bytes": FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
    }
    valid = (
        int(detail.get("table_count") or 0) == 2
        and detail.get("tables") == EXPECTED_FUNDING_TABLE_COUNTS
        and int(detail.get("trigger_count") or 0)
        == len(FUNDING_CHECKPOINT_TRIGGER_CONTRACTS)
        and str(detail.get("contract_hash") or "")
        == FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH
        and detail.get("rolling_history_storage")
        == "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
        and detail.get("automatic_real_order_submission") is False
        and detail.get("real_order_authority") is False
        and {
            name: detail.get(name) for name in expected_budgets
        }
        == expected_budgets
    )
    return valid, {
        **detail,
        "expected_contract_hash": FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
        "expected_budgets": expected_budgets,
        "errors": [] if valid else ["funding schema contract differs"],
    }


def _funding_entity_set_hash(entities: list[dict[str, str]]) -> str:
    """Hash one sorted generic strategy/combination version set."""

    return _funding_canonical_hash({
        "schema": "probiga.strategy-funding-entity-set.v1",
        "entities": entities,
    })


FUNDING_MANIFEST_BATCH_MAX_ROWS = 100


def _funding_manifest_row_sort_key(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("entity_type") or ""),
        str(row.get("entity_key") or ""),
        str(row.get("entity_version") or ""),
        str(row.get("account_id") or ""),
        str(row.get("trade_date") or ""),
        str(row.get("checkpoint_id") or ""),
        str(row.get("recipe_hash") or ""),
        str(row.get("reason_code") or ""),
    )


def _funding_manifest_batch_root(
    rows: list[dict[str, Any]], *, kind: str,
) -> dict[str, Any]:
    """Independently reproduce the compact canonical manifest root."""

    normalized_kind = str(kind or "").upper()
    if normalized_kind not in {
        "CHECKPOINT", "COMBINATION_RECIPE", "INELIGIBLE",
    }:
        raise ValueError("invalid funding manifest root kind")
    ordered = sorted(
        (dict(row) for row in rows), key=_funding_manifest_row_sort_key,
    )
    if len(ordered) != len({
        _funding_canonical_json(row) for row in ordered
    }):
        raise ValueError("duplicate funding manifest rows")
    batches: list[dict[str, Any]] = []
    for batch_index, offset in enumerate(
        range(0, len(ordered), FUNDING_MANIFEST_BATCH_MAX_ROWS)
    ):
        batch_rows = ordered[
            offset:offset + FUNDING_MANIFEST_BATCH_MAX_ROWS
        ]
        batch_payload = {
            "schema": "probiga.strategy-funding-manifest-batch.v1",
            "kind": normalized_kind,
            "batch_index": batch_index,
            "rows": batch_rows,
        }
        batches.append({
            "batch_index": batch_index,
            "count": len(batch_rows),
            "first_row_hash": _funding_canonical_hash(batch_rows[0]),
            "last_row_hash": _funding_canonical_hash(batch_rows[-1]),
            "batch_hash": _funding_canonical_hash(batch_payload),
        })
    root_payload = {
        "schema": "probiga.strategy-funding-manifest-root.v1",
        "kind": normalized_kind,
        "count": len(ordered),
        "set_hash": _funding_canonical_hash({
            "schema": "probiga.strategy-funding-manifest-row-set.v1",
            "kind": normalized_kind,
            "rows": ordered,
        }),
        "batches": batches,
    }
    return {
        **root_payload,
        "root_hash": _funding_canonical_hash(root_payload),
    }


def _funding_checkpoint_manifest_entry(
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    key = str(checkpoint.get("strategy_key") or "")
    version = str(checkpoint.get("strategy_version") or "")
    entry = {
        "entity_type": "STRATEGY",
        "entity_key": key,
        "entity_version": version,
        "checkpoint_id": str(checkpoint.get("checkpoint_id") or ""),
        "strategy_key": key,
        "strategy_version": version,
        "account_id": str(checkpoint.get("account_id") or ""),
        "trade_date": _iso_date(checkpoint.get("trade_date")),
        "replay_mode": str(checkpoint.get("replay_mode") or ""),
        "replay_session_count": _integer(
            checkpoint.get("replay_session_count")
        ),
        "max_holding_days": _integer(checkpoint.get("max_holding_days")),
        "checkpoint_hash": str(checkpoint.get("checkpoint_hash") or ""),
        "chain_hash": str(checkpoint.get("chain_hash") or ""),
        "history_fact_count": _integer(
            checkpoint.get("history_fact_count")
        ),
        "history_fact_set_hash": str(
            checkpoint.get("history_fact_set_hash") or ""
        ),
        "history_tip_fact_id": str(
            checkpoint.get("history_tip_fact_id") or ""
        ),
        "history_tip_fact_hash": str(
            checkpoint.get("history_tip_fact_hash") or ""
        ),
        "new_fact_count": _integer(checkpoint.get("new_fact_count")),
        "new_fact_set_hash": str(
            checkpoint.get("new_fact_set_hash") or ""
        ),
        "new_fact_first_id": str(
            checkpoint.get("new_fact_first_id") or ""
        ),
        "new_fact_tip_id": str(
            checkpoint.get("new_fact_tip_id") or ""
        ),
        "bootstrap_full_history_scan": (
            str(checkpoint.get("replay_mode") or "") == "FULL_BOOTSTRAP"
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    hashes = (
        "checkpoint_id", "checkpoint_hash", "chain_hash",
        "history_fact_set_hash", "history_tip_fact_id",
        "history_tip_fact_hash", "new_fact_set_hash",
        "new_fact_first_id", "new_fact_tip_id",
    )
    if (
        not key
        or not version
        or not entry["account_id"]
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(entry[field])) is None
            for field in hashes
        )
        or checkpoint.get("automatic_real_order_submission") not in (0, False)
        or checkpoint.get("real_order_authority") not in (0, False)
    ):
        raise ValueError("funding checkpoint manifest entry is invalid")
    return entry


def _validated_combination_risk_binding(
    value: Any, *,
    constraint_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("combination drift-risk binding is missing")
    expected_fields = {
        "schema", "window_days", "risk_path_hash",
        "constraint_evaluation_hash", "constraint_passed",
        "peak_member_weight", "current_member_weight",
        "peak_pairwise_stock_overlap_pct",
        "current_pairwise_stock_overlap_pct",
        "peak_industry_weight_pct", "current_industry_weight_pct",
        "industry_snapshot_path_hash", "industry_trade_dates_hash",
        "industry_stock_code_sets_hash",
        "automatic_real_order_submission",
        "real_order_authority", "binding_hash",
    }
    numeric_fields = (
        "peak_member_weight", "current_member_weight",
        "peak_pairwise_stock_overlap_pct",
        "current_pairwise_stock_overlap_pct",
        "peak_industry_weight_pct", "current_industry_weight_pct",
    )
    numbers: dict[str, float] = {}
    for field in numeric_fields:
        try:
            number = float(value.get(field))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "combination drift-risk value is invalid"
            ) from exc
        if not math.isfinite(number) or number < 0:
            raise ValueError("combination drift-risk value is invalid")
        numbers[field] = number
    payload = {
        "schema": str(value.get("schema") or ""),
        "window_days": _integer(value.get("window_days")),
        "risk_path_hash": str(value.get("risk_path_hash") or ""),
        "constraint_evaluation_hash": str(
            value.get("constraint_evaluation_hash") or ""
        ),
        "constraint_passed": value.get("constraint_passed") is True,
        **numbers,
        "industry_snapshot_path_hash": str(
            value.get("industry_snapshot_path_hash") or ""
        ),
        "industry_trade_dates_hash": str(
            value.get("industry_trade_dates_hash") or ""
        ),
        "industry_stock_code_sets_hash": str(
            value.get("industry_stock_code_sets_hash") or ""
        ),
        "automatic_real_order_submission": value.get(
            "automatic_real_order_submission"
        ),
        "real_order_authority": value.get("real_order_authority"),
    }
    if (
        set(value) != expected_fields
        or payload["schema"]
        != "probiga.combination-drift-risk-binding.v2"
        or payload["window_days"] != 60
        or any(
            re.fullmatch(r"[0-9a-f]{64}", payload[field]) is None
            for field in (
                "risk_path_hash", "constraint_evaluation_hash",
                "industry_snapshot_path_hash",
                "industry_trade_dates_hash",
                "industry_stock_code_sets_hash",
            )
        )
        or payload["constraint_passed"] is not True
        or payload["peak_member_weight"] > 1.0
        or payload["current_member_weight"] > 1.0
        or any(
            payload[field] > 100.0
            for field in (
                "peak_pairwise_stock_overlap_pct",
                "current_pairwise_stock_overlap_pct",
                "peak_industry_weight_pct",
                "current_industry_weight_pct",
            )
        )
        or payload["automatic_real_order_submission"] is not False
        or payload["real_order_authority"] is not False
        or str(value.get("binding_hash") or "")
        != _funding_canonical_hash(payload)
    ):
        raise ValueError("combination drift-risk binding differs")
    if constraint_evaluation is not None and (
        constraint_evaluation.get("passed") is not True
        or str(constraint_evaluation.get("evaluation_hash") or "")
        != payload["constraint_evaluation_hash"]
        or str((constraint_evaluation.get("drift_risk_path") or {}).get(
            "risk_path_hash"
        ) or "") != payload["risk_path_hash"]
        or str((constraint_evaluation.get(
            "industry_snapshot_path"
        ) or {}).get("industry_snapshot_path_hash") or "")
        != payload["industry_snapshot_path_hash"]
        or str((constraint_evaluation.get(
            "industry_snapshot_path"
        ) or {}).get("industry_trade_dates_hash") or "")
        != payload["industry_trade_dates_hash"]
        or str((constraint_evaluation.get(
            "industry_snapshot_path"
        ) or {}).get("industry_stock_code_sets_hash") or "")
        != payload["industry_stock_code_sets_hash"]
        or (constraint_evaluation.get("industry_snapshot_path") or {}).get(
            "status"
        ) != "COMPLETED"
        or _integer((constraint_evaluation.get(
            "industry_snapshot_path"
        ) or {}).get("window_days")) != 60
        or constraint_evaluation.get("risk_binding") != value
    ):
        raise ValueError(
            "combination drift-risk binding does not match evaluation"
        )
    return {**payload, "binding_hash": str(value["binding_hash"])}


def _combination_recipe_manifest_entry(
    combination: dict[str, Any], *, trade_date: str,
) -> dict[str, Any]:
    """Independently replay a combination's member-checkpoint recipe."""

    key = str(combination.get("combination_key") or "")
    version = str(combination.get("current_version") or "")
    ref = combination.get("combination_recipe_ref")
    if not key or not version or not isinstance(ref, dict):
        raise ValueError("combination funding recipe is missing")
    members = ref.get("members")
    if not isinstance(members, list) or not members:
        raise ValueError("combination funding recipe members are missing")
    normalized_members: list[dict[str, Any]] = []
    total_weight = Decimal("0")
    for member in members:
        if not isinstance(member, dict):
            raise ValueError("combination funding recipe member is invalid")
        try:
            weight = Decimal(str(member.get("weight")))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("combination funding recipe weight is invalid") from exc
        normalized = {
            "strategy_key": str(member.get("strategy_key") or ""),
            "strategy_version": str(member.get("strategy_version") or ""),
            "weight": round(float(weight), 8),
            "checkpoint_id": str(member.get("checkpoint_id") or ""),
            "account_id": str(member.get("account_id") or ""),
            "checkpoint_hash": str(member.get("checkpoint_hash") or ""),
            "chain_hash": str(member.get("chain_hash") or ""),
            "history_fact_set_hash": str(
                member.get("history_fact_set_hash") or ""
            ),
            "checkpoint_trade_date": str(
                member.get("checkpoint_trade_date") or ""
            ),
        }
        if (
            not weight.is_finite()
            or weight <= 0
            or not normalized["strategy_key"]
            or not normalized["strategy_version"]
            or not normalized["account_id"]
            or normalized["checkpoint_trade_date"] != trade_date
            or any(
                re.fullmatch(r"[0-9a-f]{64}", normalized[field]) is None
                for field in (
                    "checkpoint_id", "checkpoint_hash", "chain_hash",
                    "history_fact_set_hash",
                )
            )
        ):
            raise ValueError("combination funding recipe binding is invalid")
        total_weight += weight
        normalized_members.append(normalized)
    if (
        normalized_members != sorted(
            normalized_members, key=lambda row: row["strategy_key"]
        )
        or len({row["strategy_key"] for row in normalized_members})
        != len(normalized_members)
        or abs(total_weight - Decimal("1")) > Decimal("0.00000005")
    ):
        raise ValueError("combination funding recipe ordering differs")
    risk_binding = _validated_combination_risk_binding(
        ref.get("risk_constraint_binding"),
        constraint_evaluation=(
            combination.get("constraint_evaluation")
            if isinstance(combination.get("constraint_evaluation"), dict)
            else None
        ),
    )
    recipe_payload = {
        "schema": "probiga.combination-member-fact-recipe.v1",
        "combination_key": key,
        "combination_version": version,
        "trade_date": trade_date,
        "members": normalized_members,
        "risk_constraint_binding": risk_binding,
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_hash = _funding_canonical_hash(recipe_payload)
    pre_recipe_hash = str(ref.get("pre_recipe_funding_gate_hash") or "")
    gate_payload = {
        "schema": "probiga.combination-recipe-funding-gate.v1",
        "combination_key": key,
        "combination_version": version,
        "trade_date": trade_date,
        "pre_recipe_funding_gate_hash": pre_recipe_hash,
        "recipe_hash": recipe_hash,
        "recipe_ready": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_gate_hash = _funding_canonical_hash(gate_payload)
    statistical_decision = combination.get("statistical_family_decision")
    confirmation = combination.get("confirmation_guard")
    statistical_decision_hash = str(
        (statistical_decision or {}).get("decision_hash")
        or (statistical_decision or {}).get("source_decision_hash")
        or (statistical_decision or {}).get("source_hash") or ""
    )
    confirmation_guard_hash = str(
        (confirmation or {}).get("compact_hash")
        or (confirmation or {}).get("source_compact_hash")
        or (confirmation or {}).get("source_hash") or ""
    )
    final_funding_gate_hash = str(
        combination.get("funding_gate_hash") or ""
    )
    final_payload = {
        "schema": "probiga.strategy-final-funding-gate.v1",
        "entity_type": "COMBINATION",
        "entity_key": key,
        "entity_version": version,
        "pre_confirmation_funding_gate_hash": recipe_gate_hash,
        "statistical_family_decision_hash": statistical_decision_hash,
        "confirmation_guard_hash": confirmation_guard_hash,
        "confirmation_passed": (confirmation or {}).get("passed") is True,
        "projected_status": str(
            combination.get("projected_status")
            or combination.get("current_status") or ""
        ),
        "paper_allocation_eligible": combination.get(
            "paper_allocation_eligible"
        ) is True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if (
        ref.get("schema") != recipe_payload["schema"]
        or ref.get("combination_key") != key
        or ref.get("combination_version") != version
        or ref.get("trade_date") != trade_date
        or ref.get("members") != normalized_members
        or ref.get("risk_constraint_binding") != risk_binding
        or ref.get("cash_fact_materialized") is not False
        or ref.get("automatic_real_order_submission") is not False
        or ref.get("real_order_authority") is not False
        or ref.get("member_fact_sets_ready") is not True
        or ref.get("recipe_hash") != recipe_hash
        or re.fullmatch(r"[0-9a-f]{64}", pre_recipe_hash) is None
        or ref.get("recipe_gate_hash") != recipe_gate_hash
        or combination.get("pre_confirmation_funding_gate_hash")
        != recipe_gate_hash
        or re.fullmatch(r"[0-9a-f]{64}", statistical_decision_hash) is None
        or re.fullmatch(r"[0-9a-f]{64}", confirmation_guard_hash) is None
        or re.fullmatch(r"[0-9a-f]{64}", final_funding_gate_hash) is None
        or _funding_canonical_hash(final_payload) != final_funding_gate_hash
    ):
        raise ValueError("combination funding recipe hash differs")
    return {
        "entity_type": "COMBINATION",
        "entity_key": key,
        "entity_version": version,
        "trade_date": trade_date,
        "recipe_hash": recipe_hash,
        "recipe_gate_hash": recipe_gate_hash,
        "pre_confirmation_funding_gate_hash": recipe_gate_hash,
        "statistical_family_decision_hash": statistical_decision_hash,
        "confirmation_guard_hash": confirmation_guard_hash,
        "funding_gate_hash": final_funding_gate_hash,
        "member_count": len(normalized_members),
        "member_checkpoint_set_hash": _funding_canonical_hash({
            "schema": "probiga.combination-member-checkpoint-set.v1",
            "members": normalized_members,
        }),
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _funding_fact_storage_projection(
    row: dict[str, Any], fact: dict[str, Any], *, run_uid: str,
) -> dict[str, Any]:
    return {
        "fact_id": str(row.get("fact_id") or ""),
        "entity_type": "STRATEGY",
        "entity_key": fact.get("entity_key"),
        "entity_version": fact.get("entity_version"),
        "entity_version_hash": fact.get("entity_version_hash"),
        "execution_binding_hash": fact.get("execution_binding_hash") or None,
        "account_id": fact.get("account_id"),
        "trade_date": fact.get("trade_date"),
        "origin_checkpoint_id": fact.get("origin_checkpoint_id"),
        "previous_fact_id": fact.get("previous_fact_id") or None,
        "previous_fact_hash": fact.get("previous_fact_hash") or None,
        "opening_cash_cny": fact.get("opening_cash_cny"),
        "closing_cash_cny": fact.get("closing_cash_cny"),
        "opening_equity_cny": fact.get("opening_equity_cny"),
        "closing_equity_cny": fact.get("closing_equity_cny"),
        "daily_return_pct": fact.get("daily_return_pct"),
        "cumulative_fee_cny": fact.get("cumulative_fee_cny"),
        "high_watermark_equity_cny": fact.get(
            "high_watermark_equity_cny"
        ),
        "stock_exposure_json": _funding_canonical_json(
            fact.get("stock_risk_exposure")
        ),
        "closed_evidence_ids_json": _funding_canonical_json(
            fact.get("closed_evidence_ids")
        ),
        "fact_json": str(row.get("fact_json") or ""),
        "fact_hash": str(row.get("fact_hash") or ""),
        "anchor_run_uid": run_uid,
        "canonical_result_hash": "0" * 64,
        "anchor_audit_id": "0" * 32,
        "anchor_audit_hash": "0" * 64,
        "automatic_real_order_submission": 0,
        "real_order_authority": 0,
    }


def _funding_checkpoint_storage_projection(
    row: dict[str, Any], state: dict[str, Any], *, run_uid: str,
) -> dict[str, Any]:
    fields = (
        "strategy_key", "strategy_version", "strategy_version_hash",
        "execution_binding_hash", "account_id", "trade_date",
        "replay_mode", "replay_start_date", "replay_session_count",
        "max_holding_days", "opening_cash_cny", "closing_cash_cny",
        "opening_equity_cny", "closing_equity_cny", "cumulative_fee_cny",
        "high_watermark_equity_cny", "history_start_date",
        "history_end_date", "history_fact_count", "history_opening_equity",
        "history_opening_date", "history_tip_fact_id",
        "history_tip_fact_hash", "history_fact_set_hash", "new_fact_count",
        "new_fact_set_hash", "new_fact_first_id", "new_fact_tip_id",
        "evidence_watermark", "input_set_hash",
    )
    projected = {field: state.get(field) for field in fields}
    projected.update({
        "checkpoint_id": str(row.get("checkpoint_id") or ""),
        "execution_binding_hash": state.get("execution_binding_hash") or None,
        "holdings_json": str(row.get("holdings_json") or ""),
        "history_opening_date": state.get("history_opening_date") or None,
        "previous_checkpoint_id": row.get("previous_checkpoint_id") or None,
        "previous_checkpoint_hash": row.get("previous_checkpoint_hash") or None,
        "previous_chain_hash": row.get("previous_chain_hash") or None,
        "state_json": str(row.get("state_json") or ""),
        "checkpoint_hash": str(row.get("checkpoint_hash") or ""),
        "chain_payload_json": str(row.get("chain_payload_json") or ""),
        "chain_hash": str(row.get("chain_hash") or ""),
        "anchor_run_uid": run_uid,
        "canonical_result_hash": "0" * 64,
        "anchor_audit_id": "0" * 32,
        "anchor_audit_hash": "0" * 64,
        "automatic_real_order_submission": 0,
        "real_order_authority": 0,
    })
    return projected


def _funding_manifest_persistence_check(
    connection, *, run: dict[str, Any], result: dict[str, Any],
    trade_date: str,
) -> tuple[bool, dict[str, Any]]:
    """Replay the exact current-version partition and its persisted anchors."""

    errors: list[dict[str, str]] = []

    def reject(reason: str, *, record_id: str = "") -> None:
        item = {"reason": reason}
        if record_id:
            item["record_id"] = record_id[:160]
        errors.append(item)

    run_uid = str(run.get("run_uid") or "")
    raw_result = str(run.get("result_json") or "")
    result_hash = str(run.get("result_hash") or "")
    manifest = result.get("funding_checkpoint_manifest")
    if (
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or _iso_date(run.get("trade_date")) != trade_date
        or hashlib.sha256(raw_result.encode("utf-8")).hexdigest()
        != result_hash
        or result.get("run_uid") != run_uid
        or result.get("is_canonical") is not True
        or not isinstance(manifest, dict)
    ):
        return False, {
            "run_uid": run_uid,
            "errors": [{"reason": "canonical run or funding manifest is invalid"}],
        }

    expected_manifest_keys = {
        "schema", "run_uid", "trade_date", "coverage", "checkpoint_root",
        "combination_recipe_root", "ineligible_root",
        "ineligible_reason_code_counts", "checkpoint_storage_bytes", "fact_storage_bytes",
        "total_storage_bytes", "target_total_bytes", "hard_total_bytes",
        "target_total_met", "bootstrap_is_daily_bounded",
        "automatic_real_order_submission", "real_order_authority",
        "manifest_hash",
    }
    manifest_payload = {
        key: value for key, value in manifest.items()
        if key != "manifest_hash"
    }
    coverage = manifest.get("coverage")
    if (
        set(manifest) != expected_manifest_keys
        or manifest.get("schema")
        != "probiga.strategy-funding-checkpoint-manifest.v2"
        or manifest.get("run_uid") != run_uid
        or manifest.get("trade_date") != trade_date
        or not isinstance(coverage, dict)
        or not isinstance(manifest.get("checkpoint_root"), dict)
        or not isinstance(manifest.get("combination_recipe_root"), dict)
        or not isinstance(manifest.get("ineligible_root"), dict)
        or not isinstance(manifest.get("ineligible_reason_code_counts"), dict)
        or _funding_canonical_hash(manifest_payload)
        != str(manifest.get("manifest_hash") or "")
        or len(_funding_canonical_json(manifest).encode("utf-8"))
        > FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES
        or manifest.get("bootstrap_is_daily_bounded") is not False
        or manifest.get("automatic_real_order_submission") is not False
        or manifest.get("real_order_authority") is not False
        or _integer(manifest.get("target_total_bytes"))
        != FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES
        or _integer(manifest.get("hard_total_bytes"))
        != FUNDING_CHECKPOINT_TOTAL_HARD_BYTES
    ):
        reject("funding manifest identity, shape, hash or budget differs")
        coverage = coverage if isinstance(coverage, dict) else {}

    strategies = result.get("strategies")
    combinations = result.get("combinations")
    if not isinstance(strategies, list) or not isinstance(combinations, list):
        strategies = []
        combinations = []
        reject("funding result entity collections are invalid")
    result_entities = [
        {
            "entity_type": "STRATEGY",
            "entity_key": str(item.get("strategy_key") or ""),
            "entity_version": str(item.get("current_version") or ""),
        }
        for item in strategies if isinstance(item, dict)
    ] + [
        {
            "entity_type": "COMBINATION",
            "entity_key": str(item.get("combination_key") or ""),
            "entity_version": str(item.get("current_version") or ""),
        }
        for item in combinations if isinstance(item, dict)
    ]
    result_entities.sort(key=lambda item: (
        item["entity_type"], item["entity_key"], item["entity_version"],
    ))
    registry_rows = _rows(
        connection,
        "SELECT entity_type, entity_key, entity_version FROM ("
        "SELECT 'STRATEGY' AS entity_type, strategy_key AS entity_key, "
        "current_version AS entity_version FROM st_strategy_registry "
        "UNION ALL SELECT 'COMBINATION' AS entity_type, "
        "combination_key AS entity_key, current_version AS entity_version "
        "FROM st_strategy_combination) funding_registry "
        "ORDER BY BINARY entity_type, BINARY entity_key, "
        "BINARY entity_version LIMIT :registry_limit",
        {"registry_limit": len(result_entities) + 1},
    )
    current_entities = [
        {
            "entity_type": str(row.get("entity_type") or ""),
            "entity_key": str(row.get("entity_key") or ""),
            "entity_version": str(row.get("entity_version") or ""),
        }
        for row in registry_rows
    ]
    current_entities.sort(key=lambda item: (
        item["entity_type"], item["entity_key"], item["entity_version"],
    ))
    checkpoints: list[dict[str, Any]] = []
    combination_recipes: list[dict[str, Any]] = []
    ineligible: list[dict[str, Any]] = []
    for strategy in strategies:
        if not isinstance(strategy, dict):
            reject("funding strategy result member is invalid")
            continue
        key = str(strategy.get("strategy_key") or "")
        version = str(strategy.get("current_version") or "")
        ref = strategy.get("funding_checkpoint_ref")
        ineligible_row = strategy.get("funding_manifest_ineligible")
        if strategy.get("funding_checkpoint_ready") is True:
            try:
                entry = _funding_checkpoint_manifest_entry(
                    ref if isinstance(ref, dict) else {}
                )
            except (TypeError, ValueError) as exc:
                reject(type(exc).__name__, record_id=key)
                continue
            if (
                ineligible_row is not None
                or entry["strategy_key"] != key
                or entry["strategy_version"] != version
                or entry["trade_date"] != trade_date
            ):
                reject("funding strategy checkpoint partition differs", record_id=key)
                continue
            checkpoints.append(entry)
        elif (
            isinstance(ineligible_row, dict)
            and ref is None
            and ineligible_row.get("entity_type") == "STRATEGY"
            and ineligible_row.get("entity_key") == key
            and ineligible_row.get("entity_version") == version
        ):
            ineligible.append(dict(ineligible_row))
        else:
            reject("funding strategy ineligible partition differs", record_id=key)
    for combination in combinations:
        if not isinstance(combination, dict):
            reject("funding combination result member is invalid")
            continue
        key = str(combination.get("combination_key") or "")
        version = str(combination.get("current_version") or "")
        ineligible_row = combination.get("funding_manifest_ineligible")
        if combination.get("funding_recipe_ready") is True:
            try:
                recipe = _combination_recipe_manifest_entry(
                    combination, trade_date=trade_date,
                )
            except (TypeError, ValueError) as exc:
                reject(type(exc).__name__, record_id=key)
                continue
            if (
                ineligible_row is not None
                or combination.get("paper_allocation_eligible") is not True
                or recipe["entity_key"] != key
                or recipe["entity_version"] != version
            ):
                reject("funding combination recipe partition differs", record_id=key)
                continue
            combination_recipes.append(recipe)
        elif (
            isinstance(ineligible_row, dict)
            and combination.get("paper_allocation_eligible") is not True
            and ineligible_row.get("entity_type") == "COMBINATION"
            and ineligible_row.get("entity_key") == key
            and ineligible_row.get("entity_version") == version
        ):
            ineligible.append(dict(ineligible_row))
        else:
            reject("funding combination ineligible partition differs", record_id=key)

    checkpoint_entities: list[dict[str, str]] = []
    checkpoint_by_id: dict[str, dict[str, Any]] = {}
    checkpoint_identity_set: set[tuple[str, str, str]] = set()
    expected_checkpoint_keys = {
        "entity_type", "entity_key", "entity_version", "checkpoint_id",
        "strategy_key", "strategy_version", "account_id", "trade_date",
        "replay_mode", "replay_session_count", "max_holding_days",
        "checkpoint_hash", "chain_hash", "history_fact_count",
        "history_fact_set_hash", "history_tip_fact_id",
        "history_tip_fact_hash", "new_fact_count", "new_fact_set_hash",
        "new_fact_first_id", "new_fact_tip_id", "bootstrap_full_history_scan",
        "automatic_real_order_submission", "real_order_authority",
    }
    for item in checkpoints:
        if not isinstance(item, dict):
            reject("funding checkpoint manifest member is not an object")
            continue
        checkpoint_id = str(item.get("checkpoint_id") or "")
        identity = (
            str(item.get("entity_type") or ""),
            str(item.get("entity_key") or ""),
            str(item.get("entity_version") or ""),
        )
        valid = (
            set(item) == expected_checkpoint_keys
            and identity[0] == "STRATEGY"
            and identity[1] == str(item.get("strategy_key") or "")
            and identity[2] == str(item.get("strategy_version") or "")
            and identity not in checkpoint_identity_set
            and re.fullmatch(r"[0-9a-f]{64}", checkpoint_id) is not None
            and checkpoint_id not in checkpoint_by_id
            and item.get("trade_date") == trade_date
            and item.get("automatic_real_order_submission") is False
            and item.get("real_order_authority") is False
            and item.get("bootstrap_full_history_scan")
            is (item.get("replay_mode") == "FULL_BOOTSTRAP")
        )
        if not valid:
            reject("funding checkpoint manifest member differs", record_id=checkpoint_id)
            continue
        checkpoint_identity_set.add(identity)
        checkpoint_by_id[checkpoint_id] = item
        checkpoint_entities.append({
            "entity_type": identity[0],
            "entity_key": identity[1],
            "entity_version": identity[2],
        })
    checkpoint_entities.sort(key=lambda item: (
        item["entity_type"], item["entity_key"], item["entity_version"],
    ))

    ineligible_entities: list[dict[str, str]] = []
    ineligible_identity_set: set[tuple[str, str, str]] = set()
    for item in ineligible:
        if not isinstance(item, dict):
            reject("funding ineligible member is not an object")
            continue
        identity = (
            str(item.get("entity_type") or ""),
            str(item.get("entity_key") or ""),
            str(item.get("entity_version") or ""),
        )
        reason_code = str(item.get("reason_code") or "")
        reason = str(item.get("reason") or "")
        if (
            set(item) != {
                "entity_type", "entity_key", "entity_version",
                "reason_code", "reason",
            }
            or identity[0] not in {"STRATEGY", "COMBINATION"}
            or not identity[1]
            or not identity[2]
            or identity in ineligible_identity_set
            or not reason_code
            or not reason.strip()
            or len(reason) > 240
        ):
            reject("funding ineligible member differs", record_id=identity[1])
            continue
        ineligible_identity_set.add(identity)
        ineligible_entities.append({
            "entity_type": identity[0],
            "entity_key": identity[1],
            "entity_version": identity[2],
        })
    ineligible_entities.sort(key=lambda item: (
        item["entity_type"], item["entity_key"], item["entity_version"],
    ))
    recipe_entities = [{
        "entity_type": "COMBINATION",
        "entity_key": str(item.get("entity_key") or ""),
        "entity_version": str(item.get("entity_version") or ""),
    } for item in combination_recipes]
    recipe_entities.sort(key=lambda item: (
        item["entity_type"], item["entity_key"], item["entity_version"],
    ))
    partition = sorted(
        checkpoint_entities + recipe_entities + ineligible_entities,
        key=lambda item: (
            item["entity_type"], item["entity_key"], item["entity_version"],
        ),
    )
    expected_coverage_keys = {
        "current_entity_count", "funding_ready_count", "eligible_count",
        "strategy_checkpoint_count", "combination_recipe_count",
        "checkpointed_count", "ineligible_count", "current_entity_set_hash",
        "checkpointed_set_hash", "combination_recipe_set_hash",
        "funding_ready_set_hash", "ineligible_set_hash",
        "eligible_persistence_coverage_pct",
    }
    funding_ready_entities = sorted(
        checkpoint_entities + recipe_entities,
        key=lambda item: (
            item["entity_type"], item["entity_key"], item["entity_version"],
        ),
    )
    try:
        expected_checkpoint_root = _funding_manifest_batch_root(
            checkpoints, kind="CHECKPOINT"
        )
        expected_recipe_root = _funding_manifest_batch_root(
            combination_recipes, kind="COMBINATION_RECIPE"
        )
        expected_ineligible_root = _funding_manifest_batch_root(
            ineligible, kind="INELIGIBLE"
        )
    except (TypeError, ValueError) as exc:
        reject(type(exc).__name__)
        expected_checkpoint_root = None
        expected_recipe_root = None
        expected_ineligible_root = None
    if (
        current_entities != result_entities
        or len(current_entities) != len({
            (item["entity_type"], item["entity_key"], item["entity_version"])
            for item in current_entities
        })
        or checkpoint_identity_set & ineligible_identity_set
        or {
            (item["entity_type"], item["entity_key"], item["entity_version"])
            for item in recipe_entities
        } & ineligible_identity_set
        or partition != current_entities
        or set(coverage) != expected_coverage_keys
        or _integer(coverage.get("current_entity_count"))
        != len(current_entities)
        or _integer(coverage.get("eligible_count"))
        != len(funding_ready_entities)
        or _integer(coverage.get("funding_ready_count"))
        != len(funding_ready_entities)
        or _integer(coverage.get("strategy_checkpoint_count"))
        != len(checkpoint_entities)
        or _integer(coverage.get("combination_recipe_count"))
        != len(recipe_entities)
        or _integer(coverage.get("checkpointed_count"))
        != len(checkpoint_entities)
        or _integer(coverage.get("ineligible_count"))
        != len(ineligible_entities)
        or coverage.get("eligible_persistence_coverage_pct") != 100.0
        or coverage.get("current_entity_set_hash")
        != _funding_entity_set_hash(current_entities)
        or coverage.get("checkpointed_set_hash")
        != _funding_entity_set_hash(checkpoint_entities)
        or coverage.get("combination_recipe_set_hash")
        != _funding_entity_set_hash(recipe_entities)
        or coverage.get("funding_ready_set_hash")
        != _funding_entity_set_hash(funding_ready_entities)
        or coverage.get("ineligible_set_hash")
        != _funding_entity_set_hash(ineligible_entities)
        or manifest.get("checkpoint_root") != expected_checkpoint_root
        or manifest.get("combination_recipe_root") != expected_recipe_root
        or manifest.get("ineligible_root") != expected_ineligible_root
        or manifest.get("ineligible_reason_code_counts") != {
            code: sum(1 for row in ineligible if row.get("reason_code") == code)
            for code in sorted({str(row.get("reason_code") or "") for row in ineligible})
        }
    ):
        reject("funding eligible/ineligible partition or set hashes differ")

    checkpoint_rows = _rows(
        connection,
        f"SELECT cp.*, prev.checkpoint_hash AS referenced_previous_hash, "
        "prev.chain_hash AS referenced_previous_chain_hash, "
        "prev.history_tip_fact_id AS referenced_previous_tip_id, "
        "prev.history_tip_fact_hash AS referenced_previous_tip_hash "
        f"FROM {FUNDING_CHECKPOINT_TABLE_NAME} cp LEFT JOIN "
        f"{FUNDING_CHECKPOINT_TABLE_NAME} prev "
        "ON prev.checkpoint_id=cp.previous_checkpoint_id "
        "WHERE cp.anchor_run_uid=:run_uid "
        "ORDER BY BINARY cp.checkpoint_id LIMIT :checkpoint_limit",
        {"run_uid": run_uid, "checkpoint_limit": len(checkpoint_by_id) + 1},
    )
    persisted_checkpoint_by_id = {
        str(row.get("checkpoint_id") or ""): row
        for row in checkpoint_rows
    }
    if (
        len(persisted_checkpoint_by_id) != len(checkpoint_rows)
        or set(persisted_checkpoint_by_id) != set(checkpoint_by_id)
    ):
        reject("persisted checkpoint set differs from canonical manifest")

    checkpoint_storage_bytes = 0
    for checkpoint_id, entry in sorted(checkpoint_by_id.items()):
        row = persisted_checkpoint_by_id.get(checkpoint_id)
        if not isinstance(row, dict):
            continue
        state_raw = str(row.get("state_json") or "")
        chain_raw = str(row.get("chain_payload_json") or "")
        holdings_raw = str(row.get("holdings_json") or "")
        state = _json_object(state_raw)
        chain = _json_object(chain_raw)
        holdings_ok, holdings = _json_document(holdings_raw)
        checkpoint_bytes = len(_funding_canonical_json(
            _funding_checkpoint_storage_projection(
                row,
                state if isinstance(state, dict) else {},
                run_uid=run_uid,
            )
        ).encode("utf-8"))
        checkpoint_storage_bytes += checkpoint_bytes
        previous_id = str(row.get("previous_checkpoint_id") or "")
        previous_hash = str(row.get("previous_checkpoint_hash") or "")
        previous_chain_hash = str(row.get("previous_chain_hash") or "")
        state_valid = (
            isinstance(state, dict)
            and isinstance(chain, dict)
            and holdings_ok
            and isinstance(holdings, list)
            and _funding_canonical_json(state) == state_raw
            and _funding_canonical_json(chain) == chain_raw
            and _funding_canonical_json(holdings) == holdings_raw
            and state.get("schema") == FUNDING_CHECKPOINT_SCHEMA
            and checkpoint_state_hash(state)
            == str(row.get("checkpoint_hash") or "")
            and _funding_canonical_hash(chain)
            == str(row.get("chain_hash") or "")
            and chain == checkpoint_chain_payload(
                checkpoint_hash=str(row.get("checkpoint_hash") or ""),
                previous_checkpoint_id=previous_id,
                previous_checkpoint_hash=previous_hash,
                previous_chain_hash=previous_chain_hash,
            )
            and state.get("strategy_key") == row.get("strategy_key")
            and state.get("strategy_version") == row.get("strategy_version")
            and state.get("account_id") == row.get("account_id")
            and state.get("trade_date") == _iso_date(row.get("trade_date"))
            and state.get("replay_mode") == row.get("replay_mode")
            and _integer(state.get("replay_session_count"))
            == _integer(row.get("replay_session_count"))
            and _integer(state.get("max_holding_days"))
            == _integer(row.get("max_holding_days"))
            and _integer(state.get("history_fact_count"))
            == _integer(row.get("history_fact_count"))
            and state.get("history_fact_set_hash")
            == row.get("history_fact_set_hash")
            and state.get("history_tip_fact_id")
            == row.get("history_tip_fact_id")
            and state.get("history_tip_fact_hash")
            == row.get("history_tip_fact_hash")
            and _integer(state.get("new_fact_count"))
            == _integer(row.get("new_fact_count"))
            and state.get("new_fact_set_hash") == row.get("new_fact_set_hash")
            and state.get("new_fact_first_id") == row.get("new_fact_first_id")
            and state.get("new_fact_tip_id") == row.get("new_fact_tip_id")
            and state.get("holdings") == holdings
            and state.get("automatic_real_order_submission") is False
            and state.get("real_order_authority") is False
            and checkpoint_identity(
                strategy_key=str(row.get("strategy_key") or ""),
                strategy_version=str(row.get("strategy_version") or ""),
                account_id=str(row.get("account_id") or ""),
                trade_date=_iso_date(row.get("trade_date")),
                anchor_run_uid=run_uid,
            ) == checkpoint_id
        )
        replay_mode = str(row.get("replay_mode") or "")
        predecessor_valid = (
            replay_mode == "FULL_BOOTSTRAP"
            and not previous_id
            and not previous_hash
            and not previous_chain_hash
            or replay_mode == "BOUNDED_INCREMENTAL"
            and bool(previous_id)
            and previous_hash
            == str(row.get("referenced_previous_hash") or "")
            and previous_chain_hash
            == str(row.get("referenced_previous_chain_hash") or "")
        )
        manifest_valid = all((
            entry.get("strategy_key") == row.get("strategy_key"),
            entry.get("strategy_version") == row.get("strategy_version"),
            entry.get("account_id") == row.get("account_id"),
            entry.get("trade_date") == _iso_date(row.get("trade_date")),
            entry.get("replay_mode") == replay_mode,
            _integer(entry.get("replay_session_count"))
            == _integer(row.get("replay_session_count")),
            _integer(entry.get("max_holding_days"))
            == _integer(row.get("max_holding_days")),
            entry.get("checkpoint_hash") == row.get("checkpoint_hash"),
            entry.get("chain_hash") == row.get("chain_hash"),
            _integer(entry.get("history_fact_count"))
            == _integer(row.get("history_fact_count")),
            entry.get("history_fact_set_hash")
            == row.get("history_fact_set_hash"),
            entry.get("history_tip_fact_id")
            == row.get("history_tip_fact_id"),
            entry.get("history_tip_fact_hash")
            == row.get("history_tip_fact_hash"),
            _integer(entry.get("new_fact_count"))
            == _integer(row.get("new_fact_count")),
            entry.get("new_fact_set_hash") == row.get("new_fact_set_hash"),
            entry.get("new_fact_first_id") == row.get("new_fact_first_id"),
            entry.get("new_fact_tip_id") == row.get("new_fact_tip_id"),
            _integer(row.get("automatic_real_order_submission")) == 0,
            _integer(row.get("real_order_authority")) == 0,
            str(row.get("canonical_result_hash") or "") == result_hash,
        ))
        if not state_valid or not predecessor_valid or not manifest_valid:
            reject("persisted checkpoint state, chain or manifest binding differs", record_id=checkpoint_id)

    expected_fact_count = sum(
        _integer(entry.get("new_fact_count"))
        for entry in checkpoint_by_id.values()
    )
    if any(
        _integer(entry.get("new_fact_count")) not in range(1, 371)
        for entry in checkpoint_by_id.values()
    ):
        reject("funding checkpoint daily-fact count is outside 1..370")
    fact_rows = _rows(
        connection,
        f"SELECT * FROM {FUNDING_DAILY_FACT_TABLE_NAME} "
        "WHERE anchor_run_uid=:run_uid "
        "ORDER BY BINARY origin_checkpoint_id, trade_date, BINARY fact_id "
        "LIMIT :fact_limit",
        {
            "run_uid": run_uid,
            "fact_limit": expected_fact_count + 1,
        },
    )
    facts_by_origin: dict[str, list[dict[str, Any]]] = {}
    fact_storage_bytes = 0
    for row in fact_rows:
        origin_id = str(row.get("origin_checkpoint_id") or "")
        facts_by_origin.setdefault(origin_id, []).append(row)
    if set(facts_by_origin) != set(checkpoint_by_id):
        reject("persisted daily-fact origin set differs from checkpoints")
    for origin_id, entry in sorted(checkpoint_by_id.items()):
        rows = facts_by_origin.get(origin_id, [])
        checkpoint_row = persisted_checkpoint_by_id.get(origin_id) or {}
        members: list[dict[str, str]] = []
        previous: dict[str, Any] | None = None
        valid_group = len(rows) == _integer(entry.get("new_fact_count"))
        for row in rows:
            fact_raw = str(row.get("fact_json") or "")
            fact = _json_object(fact_raw)
            fact_id = str(row.get("fact_id") or "")
            fact_hash = str(row.get("fact_hash") or "")
            day = _iso_date(row.get("trade_date"))
            valid_fact = (
                isinstance(fact, dict)
                and _funding_canonical_json(fact) == fact_raw
                and fact.get("schema") == FUNDING_DAILY_FACT_SCHEMA
                and funding_daily_fact_hash(fact) == fact_hash
                and funding_daily_fact_identity(
                    entity_type="STRATEGY",
                    entity_key=str(row.get("entity_key") or ""),
                    entity_version=str(row.get("entity_version") or ""),
                    account_id=str(row.get("account_id") or ""),
                    trade_date=day,
                    anchor_run_uid=run_uid,
                ) == fact_id
                and fact.get("entity_type") == "STRATEGY"
                and fact.get("entity_key") == checkpoint_row.get("strategy_key")
                and fact.get("entity_version")
                == checkpoint_row.get("strategy_version")
                and fact.get("account_id") == checkpoint_row.get("account_id")
                and fact.get("trade_date") == day
                and fact.get("origin_checkpoint_id") == origin_id
                and str(fact.get("previous_fact_id") or "")
                == str(row.get("previous_fact_id") or "")
                and str(fact.get("previous_fact_hash") or "")
                == str(row.get("previous_fact_hash") or "")
                and fact.get("automatic_real_order_submission") is False
                and fact.get("real_order_authority") is False
                and _integer(row.get("automatic_real_order_submission")) == 0
                and _integer(row.get("real_order_authority")) == 0
                and str(row.get("canonical_result_hash") or "") == result_hash
            )
            fact_storage_bytes += len(_funding_canonical_json(
                _funding_fact_storage_projection(
                    row,
                    fact if isinstance(fact, dict) else {},
                    run_uid=run_uid,
                )
            ).encode("utf-8"))
            for field in (
                "opening_cash_cny", "closing_cash_cny",
                "opening_equity_cny", "closing_equity_cny",
                "daily_return_pct", "cumulative_fee_cny",
                "high_watermark_equity_cny",
            ):
                try:
                    valid_fact = valid_fact and (
                        Decimal(str(fact.get(field)))
                        == Decimal(str(row.get(field)))
                    )
                except (InvalidOperation, TypeError, ValueError):
                    valid_fact = False
            exposure_ok, exposure = _json_document(row.get("stock_exposure_json"))
            closed_ok, closed_ids = _json_document(
                row.get("closed_evidence_ids_json")
            )
            valid_fact = valid_fact and (
                exposure_ok
                and closed_ok
                and fact.get("stock_risk_exposure") == exposure
                and fact.get("closed_evidence_ids") == closed_ids
            )
            if previous is None:
                if str(checkpoint_row.get("replay_mode") or "") == "FULL_BOOTSTRAP":
                    valid_fact = valid_fact and not str(
                        row.get("previous_fact_id") or ""
                    ) and not str(row.get("previous_fact_hash") or "")
                else:
                    valid_fact = valid_fact and (
                        str(row.get("previous_fact_id") or "")
                        == str(checkpoint_row.get("referenced_previous_tip_id") or "")
                        and str(row.get("previous_fact_hash") or "")
                        == str(checkpoint_row.get("referenced_previous_tip_hash") or "")
                    )
            else:
                valid_fact = valid_fact and (
                    str(row.get("previous_fact_id") or "")
                    == str(previous.get("fact_id") or "")
                    and str(row.get("previous_fact_hash") or "")
                    == str(previous.get("fact_hash") or "")
                    and day > str(previous.get("trade_date") or "")
                )
            if not valid_fact:
                valid_group = False
            members.append({"fact_id": fact_id, "fact_hash": fact_hash})
            previous = {"fact_id": fact_id, "fact_hash": fact_hash, "trade_date": day}
        if (
            not rows
            or members[0]["fact_id"] != str(entry.get("new_fact_first_id") or "")
            or members[-1]["fact_id"] != str(entry.get("new_fact_tip_id") or "")
            or ordered_funding_fact_set_hash(members)
            != str(entry.get("new_fact_set_hash") or "")
        ):
            valid_group = False
        if not valid_group:
            reject("persisted daily-fact batch or chain differs", record_id=origin_id)

    audit_rows = _rows(
        connection,
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "WHERE action='ANCHOR_FUNDING_CHECKPOINT_MANIFEST' "
        "AND entity_type='SYSTEM' "
        "AND entity_key='strategy_funding_checkpoint_manifest' "
        "AND JSON_UNQUOTE(JSON_EXTRACT(evidence_json,'$.run_uid'))=:run_uid "
        "ORDER BY created_at, BINARY audit_id LIMIT 2",
        {"run_uid": run_uid},
    )
    audit_valid = len(audit_rows) == 1
    if audit_valid:
        audit = audit_rows[0]
        before_ok, before = _json_document(audit.get("before_json"))
        after_ok, after = _json_document(audit.get("after_json"))
        evidence_ok, evidence = _json_document(audit.get("evidence_json"))
        payload = _json_object(audit.get("payload_json"))
        expected_evidence = {
            "schema": FUNDING_CHECKPOINT_AUDIT_SCHEMA,
            "run_uid": run_uid,
            "canonical_result_hash": result_hash,
            "checkpoint_manifest_hash": str(manifest.get("manifest_hash") or ""),
            "coverage": coverage,
            "checkpoint_root": manifest.get("checkpoint_root"),
            "combination_recipe_root": manifest.get(
                "combination_recipe_root"
            ),
            "ineligible_root": manifest.get("ineligible_root"),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        expected_payload = {
            "entity_type": "SYSTEM",
            "entity_key": "strategy_funding_checkpoint_manifest",
            "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
            "reason": str(audit.get("reason") or ""),
            "operator": str(audit.get("operator_name") or ""),
            "before": before,
            "after": after,
            "evidence": evidence,
            "nonce": str((payload or {}).get("nonce") or ""),
        }
        audit_id = str(audit.get("audit_id") or "")
        audit_hash = str(audit.get("audit_hash") or "")
        audit_bytes = len(str(audit.get("payload_json") or "").encode("utf-8")) + len(
            str(audit.get("evidence_json") or "").encode("utf-8")
        )
        audit_valid = (
            before_ok
            and after_ok
            and evidence_ok
            and before == {}
            and after == {
                "run_uid": run_uid,
                "manifest_hash": str(manifest.get("manifest_hash") or ""),
                "checkpoint_count": len(checkpoint_by_id),
            }
            and evidence == expected_evidence
            and payload == expected_payload
            and re.fullmatch(r"[0-9a-f]{32}", audit_id) is not None
            and re.fullmatch(r"[0-9a-f]{32}", expected_payload["nonce"])
            is not None
            and _funding_canonical_hash(payload) == audit_hash
            and audit_bytes <= FUNDING_CHECKPOINT_AUDIT_MAX_BYTES
            and all(
                str(row.get("anchor_audit_id") or "") == audit_id
                and str(row.get("anchor_audit_hash") or "") == audit_hash
                for row in checkpoint_rows + fact_rows
            )
        )
    if not audit_valid:
        reject("funding manifest audit anchor differs")

    total_storage_bytes = checkpoint_storage_bytes + fact_storage_bytes
    storage_valid = (
        checkpoint_storage_bytes
        == _integer(manifest.get("checkpoint_storage_bytes"))
        and fact_storage_bytes == _integer(manifest.get("fact_storage_bytes"))
        and total_storage_bytes == _integer(manifest.get("total_storage_bytes"))
        and total_storage_bytes <= FUNDING_CHECKPOINT_TOTAL_HARD_BYTES
        and manifest.get("target_total_met")
        is (total_storage_bytes <= FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES)
        and (
            not checkpoint_rows
            or checkpoint_storage_bytes
            <= FUNDING_CHECKPOINT_TARGET_AVG_BYTES * len(checkpoint_rows)
        )
        and all(
            len(str(row.get("fact_json") or "").encode("utf-8"))
            <= FUNDING_CHECKPOINT_BATCH_MAX_BYTES
            for row in fact_rows
        )
    )
    if not storage_valid:
        reject("funding checkpoint/fact storage budget or byte totals differ")

    summary = result.get("summary")
    if not isinstance(summary, dict) or (
        summary.get("funding_checkpoint_manifest_hash")
        != manifest.get("manifest_hash")
        or _integer(summary.get("funding_checkpoint_eligible_count"))
        != len(funding_ready_entities)
        or _integer(summary.get("funding_checkpointed_count"))
        != len(checkpoint_by_id)
        or _integer(summary.get("funding_strategy_checkpoint_count"))
        != len(checkpoint_by_id)
        or _integer(summary.get("funding_combination_recipe_count"))
        != len(combination_recipes)
        or _integer(summary.get("funding_ready_count"))
        != len(funding_ready_entities)
        or _integer(summary.get("funding_checkpoint_ineligible_count"))
        != len(ineligible_entities)
    ):
        reject("funding manifest summary binding differs")

    return not errors, {
        "run_uid": run_uid,
        "current_entity_count": len(current_entities),
        "funding_ready_count": len(funding_ready_entities),
        "checkpoint_count": len(checkpoint_rows),
        "strategy_checkpoint_count": len(checkpoint_rows),
        "combination_recipe_count": len(combination_recipes),
        "ineligible_count": len(ineligible_entities),
        "daily_fact_count": len(fact_rows),
        "checkpoint_storage_bytes": checkpoint_storage_bytes,
        "fact_storage_bytes": fact_storage_bytes,
        "total_storage_bytes": total_storage_bytes,
        "target_total_met": manifest.get("target_total_met"),
        "manifest_hash": str(manifest.get("manifest_hash") or ""),
        "checkpoint_root_hash": str(
            (manifest.get("checkpoint_root") or {}).get("root_hash") or ""
        ),
        "combination_recipe_root_hash": str(
            (manifest.get("combination_recipe_root") or {}).get("root_hash")
            or ""
        ),
        "ineligible_root_hash": str(
            (manifest.get("ineligible_root") or {}).get("root_hash") or ""
        ),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _dynamic_shadow_schema_constraints_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Delegate the full frozen contract to its sole authoritative validator."""

    from server.engine.dynamic_shadow_ledger_schema import (
        validate_dynamic_shadow_ledger_schema,
    )

    detail = validate_dynamic_shadow_ledger_schema(connection)
    return True, detail


def _dynamic_shadow_ledger_integrity_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Replay every persisted dynamic candidate fact and shadow trial."""

    from server.engine.strategy_execution_adapters import (
        batch_dynamic_shadow_ledger_readiness,
        batch_verify_all_strategy_adapter_candidate_facts,
    )

    errors: list[dict[str, str]] = []
    try:
        candidate_runs = batch_verify_all_strategy_adapter_candidate_facts(
            connection
        )
        plan_count = int(connection.execute(text(
            "SELECT COUNT(*) FROM st_dynamic_shadow_trial_plan"
        )).scalar_one())
        identity_rows = _rows(
            connection,
            "SELECT strategy_key, strategy_version, strategy_version_hash, "
            "execution_binding_hash FROM st_dynamic_shadow_trial_plan "
            "GROUP BY strategy_key, strategy_version, "
            "strategy_version_hash, execution_binding_hash "
            "ORDER BY strategy_key, strategy_version, "
            "strategy_version_hash, execution_binding_hash "
            "LIMIT :authoritative_limit",
            {"authoritative_limit": plan_count + 1},
        )
        identities = [(
            str(row.get("strategy_key") or ""),
            str(row.get("strategy_version") or ""),
            str(row.get("strategy_version_hash") or ""),
            str(row.get("execution_binding_hash") or ""),
        ) for row in identity_rows]
        if len(identity_rows) > plan_count or len(identities) != len(set(identities)):
            raise RuntimeError("historical plan identity batch was truncated")
        readiness_index = batch_dynamic_shadow_ledger_readiness(
            connection,
            identities,
            include_historical=True,
        )
        readiness_rows = list(readiness_index.values())
        observed_plan_count = sum(
            int(row.get("plan_count") or 0) for row in readiness_rows
        )
        if observed_plan_count != plan_count:
            raise RuntimeError("historical plan readiness did not cover all rows")
    except Exception as exc:
        candidate_runs = {}
        plan_count = 0
        readiness_rows = []
        errors.append({
            "candidate_run_uid": "",
            "error_type": type(exc).__name__,
            "reason": _safe_exception_message(exc),
        })
    invalid = [
        item
        for readiness in readiness_rows
        for item in (readiness.get("invalid_chains") or [])
        if isinstance(item, dict)
    ]
    if invalid or any(
        readiness.get("schema_readable") is not True
        or readiness.get("automatic_real_order_submission") is not False
        or readiness.get("real_order_authority") is not False
        for readiness in readiness_rows
    ):
        errors.append({
            "candidate_run_uid": "",
            "error_type": "DynamicShadowLedgerIntegrityError",
            "reason": "dynamic shadow historical plan/chain replay failed",
        })
    pending_plan_count = sum(
        int(row.get("pending_plan_count") or 0) for row in readiness_rows
    )
    verified_chain_count = sum(
        int(row.get("verified_chain_count") or 0) for row in readiness_rows
    )
    return not errors, {
        "candidate_fact_run_count": len(candidate_runs),
        "plan_count": plan_count,
        "pending_plan_count": pending_plan_count,
        "verified_chain_count": verified_chain_count,
        "invalid_chain_count": len(invalid),
        "shadow_trial_producer_ready": not invalid,
        "funding_pipeline_ready": bool(verified_chain_count) and not invalid,
        "ledger_hash": _canonical_digest(sorted(
            str(row.get("ledger_hash") or "") for row in readiness_rows
        )),
        "errors": errors[:100],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _schema_checks(connection, add) -> tuple[set[str], bool]:
    required_tables = set(GOVERNANCE_TABLES) | {"st_scheduled_tasks"}
    table_rows = connection.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=DATABASE() AND table_name IN ("
            + ",".join(f"'{name}'" for name in sorted(required_tables))
            + ")"
        )
    ).scalars().all()
    existing = {str(value) for value in table_rows}
    missing = sorted(required_tables - existing)
    add(
        "required_tables",
        not missing,
        {
            "required": len(required_tables),
            "existing": len(existing),
            "missing": missing,
        },
    )

    schema_ok = not missing
    for table_name, required in REQUIRED_COLUMNS.items():
        if table_name in DYNAMIC_SHADOW_TABLES:
            continue
        if table_name not in existing:
            continue
        rows = connection.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name=:table_name"
            ),
            {"table_name": table_name},
        ).scalars().all()
        columns = {str(value) for value in rows}
        absent = sorted(required - columns)
        add(
            f"schema_columns:{table_name}",
            not absent,
            {"missing": absent, "required_count": len(required)},
        )
        schema_ok = schema_ok and not absent

    for table_name, required in REQUIRED_INDEXES.items():
        if table_name in DYNAMIC_SHADOW_TABLES:
            continue
        if table_name not in existing:
            continue
        rows = connection.execute(
            text(
                "SELECT DISTINCT index_name FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name=:table_name"
            ),
            {"table_name": table_name},
        ).scalars().all()
        indexes = {str(value) for value in rows}
        absent = sorted(required - indexes)
        add(
            f"schema_indexes:{table_name}",
            not absent,
            {"missing": absent, "required": sorted(required)},
        )
        schema_ok = schema_ok and not absent
        if absent:
            continue
        contract_rows = _rows(
            connection,
            "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
            "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, "
            "SUB_PART AS sub_part FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name=:table_name "
            "ORDER BY index_name, seq_in_index",
            {"table_name": table_name},
        )
        by_index: dict[str, list[dict[str, Any]]] = {}
        for row in contract_rows:
            by_index.setdefault(str(row.get("index_name") or ""), []).append(
                row
            )
        invalid_contracts: list[dict[str, Any]] = []
        for index_name, (expected_columns, must_be_unique) in (
            REQUIRED_INDEX_CONTRACTS.get(table_name, {}).items()
        ):
            actual_rows = by_index.get(index_name, [])
            actual_columns = tuple(
                str(row.get("column_name") or "") for row in actual_rows
            )
            unique = bool(actual_rows) and all(
                _integer(row.get("non_unique")) == 0 for row in actual_rows
            )
            no_prefixes = bool(actual_rows) and all(
                row.get("sub_part") is None for row in actual_rows
            )
            if (
                actual_columns != expected_columns
                or unique != must_be_unique
                or not no_prefixes
            ):
                invalid_contracts.append(
                    {
                        "index_name": index_name,
                        "expected_columns": expected_columns,
                        "actual_columns": actual_columns,
                        "expected_unique": must_be_unique,
                        "actual_unique": unique,
                        "no_prefixes": no_prefixes,
                    }
                )
        add(
            f"schema_index_contracts:{table_name}",
            not invalid_contracts,
            {"invalid": invalid_contracts},
        )
        schema_ok = schema_ok and not invalid_contracts
    return existing, schema_ok


def _scheduler_checks(connection, existing: set[str], add) -> bool:
    if "st_scheduled_tasks" not in existing:
        add("daily_scheduler_task_unique", False, {"reason": "table missing"})
        return False
    tasks = _rows(
        connection,
        "SELECT id, task_name, task_type, group_name, script_path, "
        "script_args, cron_time, interval_minutes, date_param, enabled "
        "FROM st_scheduled_tasks "
        "WHERE task_type=:task_type OR script_path=:script_path "
        "ORDER BY id",
        {
            "task_type": GOVERNANCE_TASK["task_type"],
            "script_path": GOVERNANCE_TASK["script_path"],
        },
    )
    add(
        "daily_scheduler_task_unique",
        len(tasks) == 1,
        {"row_count": len(tasks), "rows": tasks},
    )
    if len(tasks) != 1:
        return False
    task = tasks[0]
    cron_raw = str(task.get("cron_time") or "")
    cron = (
        cron_raw[:5]
        if re.fullmatch(r"[0-9]{2}:[0-9]{2}(?::00)?", cron_raw)
        else cron_raw
    )
    exact = (
        str(task.get("task_name") or "") == GOVERNANCE_TASK["task_name"]
        and str(task.get("task_type") or "") == GOVERNANCE_TASK["task_type"]
        and str(task.get("group_name") or "") == GOVERNANCE_TASK["group_name"]
        and str(task.get("script_path") or "") == GOVERNANCE_TASK["script_path"]
        and str(task.get("script_args") or "") == GOVERNANCE_TASK["script_args"]
        and cron == GOVERNANCE_TASK["cron_time"]
        and _integer(task.get("interval_minutes")) == 0
        and str(task.get("date_param") or "") == ""
        and _integer(task.get("enabled")) == 1
    )
    add(
        "daily_scheduler_task_contract",
        exact,
        {
            "actual": task,
            "expected": {
                key: GOVERNANCE_TASK[key]
                for key in (
                    "task_name",
                    "task_type",
                    "group_name",
                    "script_path",
                    "script_args",
                    "cron_time",
                    "interval_minutes",
                    "date_param",
                    "enabled",
                )
            },
        },
    )
    return exact


def _qmt_announcement_scheduler_checks(
    connection,
    existing: set[str],
    add,
) -> bool:
    """Require one enabled Windows-owned QMT announcement capture task."""

    if "st_scheduled_tasks" not in existing:
        add(
            "qmt_announcement_scheduler_task_unique",
            False,
            {"reason": "table missing"},
        )
        add(
            "qmt_announcement_scheduler_task_contract",
            False,
            {"reason": "table missing"},
        )
        return False
    tasks = _rows(
        connection,
        "SELECT id, task_name, task_type, group_name, script_path, "
        "script_args, cron_time, interval_minutes, date_param, enabled "
        "FROM st_scheduled_tasks "
        "WHERE task_type=:task_type OR script_path=:script_path "
        "ORDER BY id",
        {
            "task_type": QMT_ANNOUNCEMENT_TASK["task_type"],
            "script_path": QMT_ANNOUNCEMENT_TASK["script_path"],
        },
    )
    add(
        "qmt_announcement_scheduler_task_unique",
        len(tasks) == 1,
        {"row_count": len(tasks), "rows": tasks},
    )
    if len(tasks) != 1:
        add(
            "qmt_announcement_scheduler_task_contract",
            False,
            {
                "reason": "QMT announcement scheduler identity is not unique",
                "row_count": len(tasks),
            },
        )
        return False
    task = tasks[0]
    cron_raw = str(task.get("cron_time") or "")
    cron = (
        cron_raw[:5]
        if re.fullmatch(r"[0-9]{2}:[0-9]{2}(?::00)?", cron_raw)
        else cron_raw
    )
    pipeline_tasks = _rows(
        connection,
        "SELECT task_type, cron_time, enabled FROM st_scheduled_tasks "
        "WHERE task_type IN "
        "(:task_type_upper,:task_type_analysis,:task_type_governance) "
        "ORDER BY task_type, id",
        {
            "task_type_upper": "analysis_upper_evidence_prepare",
            "task_type_analysis": "analysis_fast",
            "task_type_governance": GOVERNANCE_TASK["task_type"],
        },
    )
    expected_pipeline_crons = {
        "analysis_upper_evidence_prepare": ANALYSIS_UPPER_EVIDENCE_CRON,
        "analysis_fast": ANALYSIS_FAST_CRON,
        GOVERNANCE_TASK["task_type"]: STRATEGY_GOVERNANCE_CRON,
    }
    pipeline_by_type: dict[str, list[dict[str, Any]]] = {}
    for pipeline_task in pipeline_tasks:
        pipeline_by_type.setdefault(
            str(pipeline_task.get("task_type") or ""), []
        ).append(pipeline_task)
    normalized_pipeline_crons: dict[str, str] = {}
    pipeline_rows_exact = True
    for task_type, expected_cron in expected_pipeline_crons.items():
        matches = pipeline_by_type.get(task_type, [])
        if len(matches) != 1:
            pipeline_rows_exact = False
            continue
        raw = str(matches[0].get("cron_time") or "")
        normalized = (
            raw[:5]
            if re.fullmatch(r"[0-9]{2}:[0-9]{2}(?::00)?", raw)
            else raw
        )
        normalized_pipeline_crons[task_type] = normalized
        pipeline_rows_exact = pipeline_rows_exact and (
            normalized == expected_cron
            and _integer(matches[0].get("enabled")) == 1
        )
    try:
        order = validate_qmt_announcement_pipeline_order(
            upper_evidence_cron=normalized_pipeline_crons.get(
                "analysis_upper_evidence_prepare", ""
            ),
            analysis_cron=normalized_pipeline_crons.get("analysis_fast", ""),
            governance_cron=normalized_pipeline_crons.get(
                GOVERNANCE_TASK["task_type"], ""
            ),
        )
        order_valid = pipeline_rows_exact
    except Exception as exc:
        order = {"error": _safe_exception_message(exc)}
        order_valid = False
    exact = (
        str(task.get("task_name") or "")
        == QMT_ANNOUNCEMENT_TASK["task_name"]
        and str(task.get("task_type") or "")
        == QMT_ANNOUNCEMENT_TASK["task_type"]
        and str(task.get("group_name") or "")
        == QMT_ANNOUNCEMENT_TASK["group_name"]
        and str(task.get("script_path") or "")
        == QMT_ANNOUNCEMENT_TASK["script_path"]
        and str(task.get("script_args") or "")
        == QMT_ANNOUNCEMENT_TASK["script_args"]
        and cron == QMT_ANNOUNCEMENT_TASK["cron_time"]
        and _integer(task.get("interval_minutes")) == 0
        and str(task.get("date_param") or "") == ""
        and _integer(task.get("enabled")) == 1
        and order_valid
    )
    add(
        "qmt_announcement_scheduler_task_contract",
        exact,
        {
            "actual": task,
            "expected": {
                key: QMT_ANNOUNCEMENT_TASK[key]
                for key in (
                    "task_name",
                    "task_type",
                    "group_name",
                    "script_path",
                    "script_args",
                    "cron_time",
                    "interval_minutes",
                    "date_param",
                    "enabled",
                )
            },
            "pipeline_order": order,
            "pipeline_tasks": pipeline_tasks,
            "expected_pipeline_crons": expected_pipeline_crons,
        },
    )
    return exact


def _qmt_operations_scheduler_checks(
    connection,
    existing: set[str],
    add,
) -> bool:
    """Require the exact five enabled QMT foundation scheduler contracts."""

    unique_name = "qmt_operations_scheduler_tasks_unique"
    contract_name = "qmt_operations_scheduler_tasks_contract"
    if "st_scheduled_tasks" not in existing:
        add(unique_name, False, {"reason": "table missing"})
        add(contract_name, False, {"reason": "table missing"})
        return False
    predicates: list[str] = []
    params: dict[str, str] = {}
    for index, expected in enumerate(QMT_OPERATIONS_TASKS):
        predicates.append(
            f"task_type=:task_type_{index} OR script_path=:script_path_{index}"
        )
        params[f"task_type_{index}"] = str(expected["task_type"])
        params[f"script_path_{index}"] = str(expected["script_path"])
    tasks = _rows(
        connection,
        "SELECT id, task_name, task_type, group_name, script_path, "
        "script_args, cron_time, interval_minutes, date_param, enabled "
        "FROM st_scheduled_tasks WHERE "
        + " OR ".join(f"({item})" for item in predicates)
        + " ORDER BY id",
        params,
    )
    matches: dict[str, list[dict[str, Any]]] = {}
    for expected in QMT_OPERATIONS_TASKS:
        task_type = str(expected["task_type"])
        script_path = str(expected["script_path"])
        matches[task_type] = [
            row
            for row in tasks
            if str(row.get("task_type") or "") == task_type
            or str(row.get("script_path") or "") == script_path
        ]
    matched_ids = [
        int(rows[0].get("id") or 0)
        for rows in matches.values()
        if len(rows) == 1
    ]
    unique = (
        len(tasks) == len(QMT_OPERATIONS_TASKS)
        and all(len(rows) == 1 for rows in matches.values())
        and len(matched_ids) == len(set(matched_ids))
        and all(item > 0 for item in matched_ids)
    )
    add(
        unique_name,
        unique,
        {
            "row_count": len(tasks),
            "expected_row_count": len(QMT_OPERATIONS_TASKS),
            "match_counts": {
                key: len(rows) for key, rows in sorted(matches.items())
            },
            "rows": tasks,
        },
    )
    if not unique:
        add(
            contract_name,
            False,
            {"reason": "QMT operations scheduler identities are not unique"},
        )
        return False
    expected_fields = (
        "task_name",
        "task_type",
        "group_name",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "date_param",
        "enabled",
    )
    expected_by_type = {
        str(item["task_type"]): {
            key: item[key] for key in expected_fields
        }
        for item in QMT_OPERATIONS_TASKS
    }
    actual_by_type: dict[str, dict[str, Any]] = {}
    exact = True
    for task_type, expected in expected_by_type.items():
        task = matches[task_type][0]
        cron_raw = str(task.get("cron_time") or "")
        cron = (
            cron_raw[:5]
            if re.fullmatch(r"[0-9]{2}:[0-9]{2}(?::00)?", cron_raw)
            else cron_raw
        )
        actual = {
            "task_name": str(task.get("task_name") or ""),
            "task_type": str(task.get("task_type") or ""),
            "group_name": str(task.get("group_name") or ""),
            "script_path": str(task.get("script_path") or ""),
            "script_args": str(task.get("script_args") or ""),
            "cron_time": cron,
            "interval_minutes": _integer(task.get("interval_minutes")),
            "date_param": str(task.get("date_param") or ""),
            "enabled": _integer(task.get("enabled")),
        }
        actual_by_type[task_type] = actual
        exact = exact and actual == expected
    add(
        contract_name,
        exact,
        {
            "actual": actual_by_type,
            "expected": expected_by_type,
        },
    )
    return exact


def _forward_strategy_version_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the trigger-free additive V3 strategy-version contract."""

    try:
        from server.db.migrations_v3 import (
            FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
            MIGRATIONS,
            _checksum,
        )

        declared = [
            item
            for item in MIGRATIONS
            if item["version"] == FORWARD_STRATEGY_VERSION_MIGRATION_VERSION
        ]
        if len(declared) != 1:
            raise RuntimeError("forward strategy-version migration declaration drifted")
        statements = tuple(declared[0]["statements"])
        expected_checksum = _checksum(statements)
        expected_statement_count = len(statements)
        declaration_valid = (
            expected_statement_count == 3
            and expected_checksum
            == "1804a2d2c3473e98c1be77d03d324e61cb5cdb5682e7d87cf647841218b756e6"
        )
        ledger_rows = _rows(
            connection,
            "SELECT version, checksum, statement_count "
            "FROM schema_migration_v3 WHERE version=:version",
            {"version": FORWARD_STRATEGY_VERSION_MIGRATION_VERSION},
        )
        ledger_valid = (
            len(ledger_rows) == 1
            and str(ledger_rows[0].get("version") or "")
            == FORWARD_STRATEGY_VERSION_MIGRATION_VERSION
            and str(ledger_rows[0].get("checksum") or "")
            == expected_checksum
            and _integer(ledger_rows[0].get("statement_count"))
            == expected_statement_count
        )

        column = _one(
            connection,
            "SELECT COLUMN_TYPE AS column_type, "
            "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default "
            "FROM information_schema.columns "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_forward_trade_evidence_v3' "
            "AND column_name='strategy_version'",
        )
        column_valid = (
            str(column.get("column_type") or "").casefold()
            == "varchar(160)"
            and str(column.get("is_nullable") or "").upper() == "NO"
            and column.get("column_default") is not None
            and str(column.get("column_default")) == ""
        )

        index_rows = _rows(
            connection,
            "SELECT NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
            "COLUMN_NAME AS column_name, SUB_PART AS sub_part "
            "FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_forward_trade_evidence_v3' "
            "AND index_name='idx_v3_forward_strategy_version' "
            "ORDER BY seq_in_index",
        )
        expected_index_columns = (
            "strategy_key",
            "strategy_version",
            "evidence_status",
            "exit_at",
        )
        index_valid = (
            tuple(str(row.get("column_name") or "") for row in index_rows)
            == expected_index_columns
            and tuple(_integer(row.get("seq_in_index")) for row in index_rows)
            == tuple(range(1, len(expected_index_columns) + 1))
            and all(_integer(row.get("non_unique")) == 1 for row in index_rows)
            and all(row.get("sub_part") is None for row in index_rows)
        )

        valid = declaration_valid and ledger_valid and column_valid and index_valid
        return valid, {
            "migration_version": FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
            "expected_checksum": expected_checksum,
            "expected_statement_count": expected_statement_count,
            "declaration_valid": declaration_valid,
            "ledger_rows": ledger_rows,
            "column": column,
            "index_columns": [
                row.get("column_name") for row in index_rows
            ],
            "database_triggers_required": False,
            "database_trigger_inventory_checked": False,
            "existing_database_triggers": "unmanaged_and_allowed",
            "immutability_enforcement": (
                "application_writer_relation_checks_and_evidence_hash_replay"
            ),
        }
    except Exception as exc:
        return False, {
            "error": _safe_exception_message(exc),
        }


def _forward_strategy_version_data_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Audit every V3 forward row against its immutable ownership chain.

    Empty versions are retained only as explicitly quarantined legacy rows.
    The query deliberately aggregates the complete current-version population;
    it has no row limit or pagination that could silently drop evidence.
    """

    relation = """
        SELECT DISTINCT e2.evidence_id,
               CONCAT(r.model_version, ':', e2.strategy_key)
                   AS expected_strategy_version
        FROM st_forward_trade_evidence_v3 e2
        JOIN st_alpha_forecast_v3 f
          ON f.forecast_id=e2.source_forecast_id
         AND f.run_uid=e2.source_run_uid
         AND f.stock_code=e2.stock_code
         AND f.strategy_key=e2.strategy_key
        JOIN st_decision_run_v3 r
          ON r.run_uid=f.run_uid AND r.status='COMPLETED'
        JOIN st_trade_intent_v2 i
          ON i.intent_id=e2.source_intent_id
         AND i.decision_run_uid=e2.source_run_uid
         AND i.account_id=e2.account_id
         AND i.stock_code=e2.stock_code
         AND i.action='BUY'
         AND BINARY i.strategy_version=BINARY r.model_version
         AND i.reason_code IN (
             'V3_PAPER_DISCOVERY', 'V3_VALIDATED_POSITIVE'
         )
        JOIN st_order_v2 o
          ON o.order_id=e2.entry_order_id
         AND o.intent_id=e2.source_intent_id
         AND o.account_id=e2.account_id
         AND o.stock_code=e2.stock_code
         AND o.side='BUY'
        JOIN st_fill_v2 x
          ON x.fill_id=e2.entry_fill_id
         AND x.order_id=e2.entry_order_id
         AND x.account_id=e2.account_id
         AND x.stock_code=e2.stock_code
         AND x.side='BUY'
         AND x.quantity=e2.entry_quantity
         AND x.price=e2.entry_price
         AND x.gross_amount=e2.entry_gross_cny
         AND x.fee_amount=e2.entry_fee_cny
         AND x.filled_at=e2.entry_at
         AND DATE(x.filled_at)=e2.entry_trade_date
        JOIN st_cash_ledger_v2 c
          ON c.account_id=e2.account_id
         AND c.related_order_id=e2.entry_order_id
         AND c.related_fill_id=e2.entry_fill_id
         AND c.event_type='BUY_FILL'
         AND c.amount=x.net_cash_amount
         AND c.occurred_at=x.filled_at
        WHERE e2.source_run_uid<>''
          AND e2.source_forecast_id<>''
          AND e2.source_intent_id<>''
          AND e2.strategy_key<>''
          AND e2.sample_owner_role='PRIMARY'
          AND e2.attribution_status IN (
              'VERIFIED_SNAPSHOT', 'LEGACY_VERSION_DERIVED',
              'LEGACY_SINGLE_STRATEGY_RESOLVED'
          )
          AND e2.attribution_version='V3_PRIMARY_FORECAST_SNAPSHOT_V1'
          AND e2.evidence_kind='EXECUTED_PAPER'
          AND e2.protocol_version='PAPER_EXECUTED_LEDGER_V1'
          AND e2.evidence_id=SHA2(CONCAT(
              e2.entry_fill_id, '|', e2.strategy_key,
              '|PAPER_EXECUTED_LEDGER_V1'
          ), 256)
          AND e2.ownership_hash=SHA2(CONCAT(
              e2.source_run_uid, '|', e2.source_forecast_id, '|',
              e2.stock_code, '|', e2.strategy_key
          ), 256)
          AND r.model_version<>''
          AND CHAR_LENGTH(CONCAT(
              r.model_version, ':', e2.strategy_key
          ))<=160
          AND JSON_VALID(i.evidence_json)
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.model_version'
          )), '')=BINARY r.model_version
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.primary_strategy_key'
          )), '')=BINARY e2.strategy_key
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.primary_forecast_id'
          )), '')=BINARY e2.source_forecast_id
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.ownership_hash'
          )), '')=BINARY e2.ownership_hash
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.run_uid'
          )), '')=BINARY e2.source_run_uid
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.sample_owner_role'
          )), '')=BINARY e2.sample_owner_role
          AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
              i.evidence_json, '$.attribution_version'
          )), '')=BINARY e2.attribution_version
          AND (
              JSON_CONTAINS(
                  COALESCE(
                      JSON_EXTRACT(
                          i.evidence_json, '$.supporting_strategy_keys'
                      ),
                      JSON_ARRAY()
                  ),
                  JSON_QUOTE(e2.strategy_key)
              )=1
              OR JSON_CONTAINS(
                  COALESCE(
                      JSON_EXTRACT(
                          i.evidence_json, '$.signal_strategy_keys'
                      ),
                      JSON_ARRAY()
                  ),
                  JSON_QUOTE(e2.strategy_key)
              )=1
          )
          AND (
              COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
                  i.evidence_json, '$.primary_strategy_version'
              )), '')=''
              OR BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
                  i.evidence_json, '$.primary_strategy_version'
              )), '')=BINARY CONCAT(
                  r.model_version, ':', e2.strategy_key
              )
          )
    """
    backfill_relation = relation.replace(
        "'VERIFIED_SNAPSHOT', 'LEGACY_VERSION_DERIVED',\n"
        "              'LEGACY_SINGLE_STRATEGY_RESOLVED'",
        "'VERIFIED_SNAPSHOT',\n"
        "              'LEGACY_SINGLE_STRATEGY_RESOLVED'",
    )
    aggregate_sql = f"""
        SELECT COUNT(*) AS total_count,
               COALESCE(SUM(CASE WHEN e.strategy_version<>''
                   THEN 1 ELSE 0 END), 0)
                   AS versioned_count,
               COALESCE(SUM(CASE WHEN e.strategy_version=''
                   THEN 1 ELSE 0 END), 0)
                   AS quarantined_count,
               COALESCE(SUM(CASE WHEN e.strategy_version=''
                   AND backfill_rel.evidence_id IS NOT NULL
                   THEN 1 ELSE 0 END), 0) AS eligible_empty_count,
               COALESCE(SUM(CASE WHEN e.strategy_version<>'' AND (
                       insert_rel.evidence_id IS NULL
                       OR BINARY e.strategy_version<>
                          BINARY insert_rel.expected_strategy_version
                   )
                   THEN 1 ELSE 0 END), 0) AS invalid_nonempty_count,
               COALESCE(SUM(CASE WHEN e.strategy_version<>''
                   AND insert_rel.evidence_id IS NOT NULL
                   AND BINARY e.strategy_version=
                       BINARY insert_rel.expected_strategy_version
                   AND current_strategy.strategy_key IS NOT NULL
                   AND e.evidence_status IN (
                       'MATURED', 'OPEN', 'PARTIALLY_CLOSED'
                   )
                   THEN 1 ELSE 0 END), 0)
                   AS current_version_evidence_count
        FROM st_forward_trade_evidence_v3 e
        LEFT JOIN ({relation}) insert_rel
          ON insert_rel.evidence_id=e.evidence_id
        LEFT JOIN ({backfill_relation}) backfill_rel
          ON backfill_rel.evidence_id=e.evidence_id
        LEFT JOIN st_strategy_registry current_strategy
          ON BINARY current_strategy.strategy_key=
             BINARY e.strategy_key
         AND BINARY current_strategy.current_version=
             BINARY e.strategy_version
    """
    try:
        current_version_query_has_limit = bool(
            re.search(r"\blimit\b", aggregate_sql, flags=re.IGNORECASE)
        )
        counts = _one(connection, aggregate_sql)
        total = _integer(counts.get("total_count"))
        versioned = _integer(counts.get("versioned_count"))
        quarantined = _integer(counts.get("quarantined_count"))
        eligible_empty = _integer(counts.get("eligible_empty_count"))
        invalid_nonempty = _integer(
            counts.get("invalid_nonempty_count")
        )
        valid = (
            total == versioned + quarantined
            and eligible_empty == 0
            and invalid_nonempty == 0
            and not current_version_query_has_limit
        )
        return valid, {
            **counts,
            "total_count": total,
            "versioned_count": versioned,
            "quarantined_count": quarantined,
            "eligible_empty_count": eligible_empty,
            "invalid_nonempty_count": invalid_nonempty,
            "current_version_evidence_count": _integer(
                counts.get("current_version_evidence_count")
            ),
            "all_nonempty_versions_require_exact_relation": True,
            "eligible_empty_required": 0,
            "current_version_query_has_limit": (
                current_version_query_has_limit
            ),
        }
    except Exception as exc:
        return False, {
            "error": _safe_exception_message(exc),
            "current_version_query_has_limit": None,
        }


def _v2_raw_ledger_immutability_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the RDS-safe raw-ledger application-integrity marker."""

    try:
        from server.db.migrations_v3 import (
            MIGRATIONS,
            V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
            _checksum,
        )

        declared = [
            item
            for item in MIGRATIONS
            if item["version"]
            == V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION
        ]
        if len(declared) != 1:
            raise RuntimeError("raw-ledger immutability migration declaration drifted")
        statements = tuple(declared[0]["statements"])
        expected_checksum = _checksum(statements)
        expected_statement_count = len(statements)
        declaration_valid = (
            expected_statement_count == 0
            and expected_checksum
            == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        ledger_rows = _rows(
            connection,
            "SELECT version, checksum, statement_count "
            "FROM schema_migration_v3 WHERE version=:version",
            {"version": V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION},
        )
        ledger_valid = (
            len(ledger_rows) == 1
            and str(ledger_rows[0].get("version") or "")
            == V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION
            and str(ledger_rows[0].get("checksum") or "")
            == expected_checksum
            and _integer(ledger_rows[0].get("statement_count"))
            == expected_statement_count
        )
        return declaration_valid and ledger_valid, {
            "migration_version": (
                V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION
            ),
            "expected_checksum": expected_checksum,
            "expected_statement_count": expected_statement_count,
            "declaration_valid": declaration_valid,
            "ledger_rows": ledger_rows,
            "database_triggers_required": False,
            "database_trigger_inventory_checked": False,
            "existing_database_triggers": "unmanaged_and_allowed",
            "immutability_enforcement": (
                "application_append_only_writers_and_accounting_evidence_hashes"
            ),
        }
    except Exception as exc:
        return False, {"error": _safe_exception_message(exc)}


def _forward_exit_allocation_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the trigger-free 003 FIFO exit-allocation schema and ledger."""

    frozen_checksum = (
        "f2e99ea79df11e578e17298ebd9a829cc0715d334708ca760bd99970a6a5d460"
    )
    frozen_statement_count = 1
    frozen_migration_count = 27
    errors: list[str] = []
    try:
        from server.db.migrations_v3 import (
            FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
            MIGRATIONS,
            _checksum,
        )

        declared = [
            item
            for item in MIGRATIONS
            if item["version"]
            == FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION
        ]
        statements = (
            tuple(declared[0]["statements"])
            if len(declared) == 1
            else ()
        )
        declared_checksum = _checksum(statements) if statements else ""
        if len(declared) != 1:
            errors.append("003 migration declaration is not unique")
        if len(MIGRATIONS) != frozen_migration_count:
            errors.append("V3 migration declaration count differs")
        if declared_checksum != frozen_checksum:
            errors.append("003 migration declaration checksum differs")
        if len(statements) != frozen_statement_count:
            errors.append("003 migration statement count differs")

        migration_total = _one(
            connection,
            "SELECT COUNT(*) AS total_count FROM schema_migration_v3",
        )
        if _integer(migration_total.get("total_count")) != (
            frozen_migration_count
        ):
            errors.append("applied V3 migration ledger count differs")
        ledger_rows = _rows(
            connection,
            "SELECT version, checksum, statement_count "
            "FROM schema_migration_v3 WHERE version=:version",
            {"version": FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION},
        )
        ledger_valid = (
            len(ledger_rows) == 1
            and str(ledger_rows[0].get("version") or "")
            == FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION
            and str(ledger_rows[0].get("checksum") or "")
            == frozen_checksum
            and _integer(ledger_rows[0].get("statement_count"))
            == frozen_statement_count
        )
        if not ledger_valid:
            errors.append("003 migration ledger row differs")

        table = _one(
            connection,
            "SELECT ENGINE AS engine, TABLE_COLLATION AS table_collation "
            "FROM information_schema.tables "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_forward_exit_allocation_v3'",
        )
        table_valid = (
            str(table.get("engine") or "").upper() == "INNODB"
            and str(table.get("table_collation") or "")
            .casefold()
            .startswith("utf8mb4_")
        )
        if not table_valid:
            errors.append("forward exit-allocation table is missing or drifted")

        columns = _rows(
            connection,
            "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, "
            "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
            "NUMERIC_PRECISION AS numeric_precision, "
            "NUMERIC_SCALE AS numeric_scale, IS_NULLABLE AS is_nullable, "
            "COLUMN_DEFAULT AS column_default, EXTRA AS extra "
            "FROM information_schema.columns "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_forward_exit_allocation_v3' "
            "ORDER BY ORDINAL_POSITION",
        )
        expected_columns = (
            ("allocation_id", "char", 64, None, None, False),
            ("evidence_id", "char", 64, None, None, True),
            ("attribution_status", "varchar", 32, None, None, False),
            ("account_id", "varchar", 64, None, None, False),
            ("stock_code", "varchar", 16, None, None, False),
            ("entry_fill_id", "varchar", 64, None, None, False),
            ("exit_fill_id", "varchar", 64, None, None, False),
            ("exit_order_id", "varchar", 64, None, None, False),
            ("allocation_sequence", "bigint", None, None, None, False),
            ("allocated_quantity", "bigint", None, None, None, False),
            ("allocated_gross_cny", "decimal", None, 20, 6, False),
            ("allocated_fee_cny", "decimal", None, 20, 6, False),
            ("exit_filled_at", "datetime", None, None, None, False),
            (
                "allocation_protocol_version",
                "varchar",
                80,
                None,
                None,
                False,
            ),
            ("created_at", "datetime", None, None, None, False),
        )
        if len(columns) != len(expected_columns):
            errors.append("forward exit-allocation column count differs")
        else:
            for row, expected in zip(columns, expected_columns):
                name, data_type, char_length, precision, scale, nullable = (
                    expected
                )
                valid = (
                    str(row.get("column_name") or "").casefold() == name
                    and str(row.get("data_type") or "").casefold()
                    == data_type
                    and str(row.get("is_nullable") or "").upper()
                    == ("YES" if nullable else "NO")
                    and row.get("column_default") is None
                    and str(row.get("extra") or "") == ""
                    and (
                        char_length is None
                        or _integer(row.get("character_maximum_length"))
                        == char_length
                    )
                    and (
                        precision is None
                        or _integer(row.get("numeric_precision"))
                        == precision
                    )
                    and (
                        scale is None
                        or _integer(row.get("numeric_scale")) == scale
                    )
                )
                if not valid:
                    errors.append(
                        "forward exit-allocation column differs: " + name
                    )

        index_rows = _rows(
            connection,
            "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
            "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, "
            "SUB_PART AS sub_part FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_forward_exit_allocation_v3' "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        )
        observed_indexes: dict[str, tuple[bool, tuple[str, ...]]] = {}
        for index_name in sorted(
            {str(row.get("index_name") or "") for row in index_rows}
        ):
            members = [
                row
                for row in index_rows
                if str(row.get("index_name") or "") == index_name
            ]
            metadata_valid = (
                [
                    _integer(row.get("seq_in_index")) for row in members
                ]
                == list(range(1, len(members) + 1))
                and all(row.get("sub_part") is None for row in members)
                and len(
                    {_integer(row.get("non_unique")) for row in members}
                )
                == 1
            )
            if not metadata_valid:
                errors.append("forward exit-allocation index metadata differs")
            observed_indexes[index_name.casefold()] = (
                _integer(members[0].get("non_unique")) == 0,
                tuple(
                    str(row.get("column_name") or "").casefold()
                    for row in members
                ),
            )
        expected_indexes = {
            "primary": (True, ("allocation_id",)),
            "uk_v3_forward_exit_evidence_fill": (
                True,
                ("evidence_id", "exit_fill_id"),
            ),
            "uk_v3_forward_exit_fill_sequence": (
                True,
                ("exit_fill_id", "allocation_sequence"),
            ),
            "uk_v3_forward_exit_fill_entry": (
                True,
                ("exit_fill_id", "entry_fill_id"),
            ),
            "idx_v3_forward_exit_evidence": (
                False,
                ("evidence_id", "exit_filled_at"),
            ),
            "idx_v3_forward_exit_entry": (False, ("entry_fill_id",)),
            "idx_v3_forward_exit_account": (
                False,
                ("account_id", "stock_code", "exit_filled_at"),
            ),
        }
        if observed_indexes != expected_indexes:
            errors.append("forward exit-allocation index inventory differs")

        foreign_key_rows = _rows(
            connection,
            "SELECT k.CONSTRAINT_NAME AS constraint_name, "
            "k.COLUMN_NAME AS column_name, "
            "k.REFERENCED_TABLE_NAME AS referenced_table_name, "
            "k.REFERENCED_COLUMN_NAME AS referenced_column_name, "
            "r.UPDATE_RULE AS update_rule, r.DELETE_RULE AS delete_rule "
            "FROM information_schema.key_column_usage k "
            "JOIN information_schema.referential_constraints r "
            "ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA "
            "AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
            "AND r.TABLE_NAME=k.TABLE_NAME "
            "WHERE k.CONSTRAINT_SCHEMA=DATABASE() "
            "AND k.TABLE_NAME='st_forward_exit_allocation_v3' "
            "ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION",
        )
        observed_foreign_keys = {
            str(row.get("constraint_name") or "").casefold(): (
                str(row.get("column_name") or "").casefold(),
                str(row.get("referenced_table_name") or "").casefold(),
                str(row.get("referenced_column_name") or "").casefold(),
                str(row.get("update_rule") or "").upper(),
                str(row.get("delete_rule") or "").upper(),
            )
            for row in foreign_key_rows
        }
        expected_foreign_keys = {
            "fk_v3_forward_exit_allocation_evidence": (
                "evidence_id",
                "st_forward_trade_evidence_v3",
                "evidence_id",
            ),
            "fk_v3_forward_exit_allocation_fill": (
                "exit_fill_id",
                "st_fill_v2",
                "fill_id",
            ),
            "fk_v3_forward_exit_allocation_entry_fill": (
                "entry_fill_id",
                "st_fill_v2",
                "fill_id",
            ),
        }
        if set(observed_foreign_keys) != set(expected_foreign_keys):
            errors.append(
                "forward exit-allocation foreign-key inventory differs"
            )
        for name, expected in expected_foreign_keys.items():
            observed = observed_foreign_keys.get(name)
            if (
                observed is None
                or observed[:3] != expected
                or observed[3] not in {"RESTRICT", "NO ACTION"}
                or observed[4] not in {"RESTRICT", "NO ACTION"}
            ):
                errors.append(
                    "forward exit-allocation foreign key differs: " + name
                )

        return not errors, {
            "migration_version": (
                FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION
            ),
            "frozen_checksum": frozen_checksum,
            "declared_checksum": declared_checksum,
            "frozen_statement_count": frozen_statement_count,
            "declared_statement_count": len(statements),
            "frozen_migration_count": frozen_migration_count,
            "declared_migration_count": len(MIGRATIONS),
            "applied_migration_count": _integer(
                migration_total.get("total_count")
            ),
            "ledger_rows": ledger_rows,
            "table": table,
            "column_count": len(columns),
            "index_names": sorted(observed_indexes),
            "foreign_key_names": sorted(observed_foreign_keys),
            "database_triggers_required": False,
            "database_trigger_inventory_checked": False,
            "existing_database_triggers": "unmanaged_and_allowed",
            "immutability_enforcement": (
                "application_fifo_writer_unique_keys_foreign_keys_and_hash_replay"
            ),
            "errors": errors[:100],
        }
    except Exception as exc:
        return False, {
            "frozen_checksum": frozen_checksum,
            "frozen_statement_count": frozen_statement_count,
            "frozen_migration_count": frozen_migration_count,
            "error": _safe_exception_message(exc),
        }


def _forward_exit_allocation_data_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Replay every raw SELL through FIFO and compare the immutable ledger."""

    try:
        from server.trading_v3.forward_evidence import (
            EXIT_ALLOCATION_PROTOCOL,
            _allocation_contract,
            _exit_allocation_id,
            reconstruct_executed_forward_records,
        )

        raw_sell_total = _one(
            connection,
            "SELECT COUNT(*) AS raw_sell_count FROM st_fill_v2 "
            "WHERE side='SELL'",
        )
        fill_rows = _rows(
            connection,
            "SELECT f.fill_id, f.order_id, f.account_id, f.stock_code, "
            "f.side, f.quantity, f.price, f.gross_amount, f.fee_amount, "
            "f.filled_at, i.intent_id, i.decision_run_uid, "
            "i.reason_code AS intent_reason_code, i.evidence_json "
            "FROM st_fill_v2 f "
            "JOIN st_order_v2 o ON o.order_id=f.order_id "
            "AND o.account_id=f.account_id "
            "AND o.stock_code=f.stock_code AND o.side=f.side "
            "JOIN st_trade_intent_v2 i ON i.intent_id=o.intent_id "
            "AND i.account_id=f.account_id "
            "AND i.stock_code=f.stock_code AND i.action=f.side "
            "WHERE f.side IN ('BUY','SELL') "
            "ORDER BY f.account_id, f.filled_at, f.fill_id",
        )
        forecast_rows = _rows(
            connection,
            "SELECT f.forecast_id, f.run_uid, f.stock_code, "
            "f.strategy_key, r.model_version AS run_model_version "
            "FROM st_alpha_forecast_v3 f "
            "JOIN st_decision_run_v3 r ON r.run_uid=f.run_uid "
            "AND r.status='COMPLETED'",
        )
        observed_rows = _rows(
            connection,
            "SELECT a.allocation_id, a.evidence_id, "
            "a.attribution_status, a.account_id, a.stock_code, "
            "a.entry_fill_id, a.exit_fill_id, a.exit_order_id, "
            "a.allocation_sequence, a.allocated_quantity, "
            "a.allocated_gross_cny, a.allocated_fee_cny, "
            "a.exit_filled_at, a.allocation_protocol_version, "
            "e.evidence_id AS bound_evidence_id, "
            "e.account_id AS evidence_account_id, "
            "e.stock_code AS evidence_stock_code, "
            "e.entry_fill_id AS evidence_entry_fill_id "
            "FROM st_forward_exit_allocation_v3 a "
            "LEFT JOIN st_forward_trade_evidence_v3 e "
            "ON e.evidence_id=a.evidence_id "
            "ORDER BY a.account_id, a.exit_filled_at, "
            "a.exit_fill_id, a.allocation_sequence",
        )
    except Exception as exc:
        return False, {"error": _safe_exception_message(exc)}

    errors: list[dict[str, Any]] = []

    def fail(reason: str, **detail: Any) -> None:
        if len(errors) < 100:
            errors.append({"reason": reason, **detail})

    forecast_ids: dict[tuple[str, str, str], str] = {}
    run_model_versions: dict[str, str] = {}
    for row in forecast_rows:
        key = (
            str(row.get("run_uid") or ""),
            str(row.get("stock_code") or ""),
            str(row.get("strategy_key") or ""),
        )
        forecast_id = str(row.get("forecast_id") or "")
        model_version = str(row.get("run_model_version") or "")
        if not all(key) or not forecast_id or not model_version:
            fail("forecast ownership relation is incomplete", key=key)
            continue
        if key in forecast_ids and forecast_ids[key] != forecast_id:
            fail("forecast ownership relation is not unique", key=key)
        forecast_ids[key] = forecast_id
        run_uid = key[0]
        if (
            run_uid in run_model_versions
            and run_model_versions[run_uid] != model_version
        ):
            fail("decision run model version is inconsistent", run_uid=run_uid)
        run_model_versions[run_uid] = model_version

    fills_by_account: dict[str, list[dict[str, Any]]] = {}
    raw_sell_by_id: dict[str, dict[str, Any]] = {}
    joined_sell_count = 0
    for row in fill_rows:
        account_id = str(row.get("account_id") or "")
        fill_id = str(row.get("fill_id") or "")
        if not account_id or not fill_id:
            fail("raw fill identity is incomplete", fill_id=fill_id)
            continue
        fills_by_account.setdefault(account_id, []).append(row)
        if str(row.get("side") or "").upper() == "SELL":
            joined_sell_count += 1
            if fill_id in raw_sell_by_id:
                fail("raw SELL fill identity is duplicated", fill_id=fill_id)
            raw_sell_by_id[fill_id] = row
    raw_sell_count = _integer(raw_sell_total.get("raw_sell_count"))
    if raw_sell_count != joined_sell_count:
        fail(
            "raw SELL joins do not cover the complete fill ledger",
            raw_sell_count=raw_sell_count,
            joined_sell_count=joined_sell_count,
        )

    expected_rows: list[dict[str, Any]] = []
    replay_diagnostics: dict[str, int] = {}
    for account_id, account_fills in sorted(fills_by_account.items()):
        account_diagnostics: dict[str, int] = {}
        account_allocations: list[dict[str, Any]] = []
        try:
            reconstruct_executed_forward_records(
                account_fills,
                forecast_ids=forecast_ids,
                run_model_versions=run_model_versions,
                diagnostics=account_diagnostics,
                allocation_rows=account_allocations,
            )
        except Exception as exc:
            fail(
                "deterministic FIFO replay raised",
                account_id=account_id,
                error=_safe_exception_message(exc),
            )
            continue
        for key, value in account_diagnostics.items():
            replay_diagnostics[key] = (
                replay_diagnostics.get(key, 0) + _integer(value)
            )
        for row in account_allocations:
            row["account_id"] = account_id
            expected_rows.append(row)
    if replay_diagnostics.get("SELL_FIFO_COVERAGE_GAP", 0):
        fail(
            "raw SELL fills do not have complete FIFO BUY coverage",
            count=replay_diagnostics["SELL_FIFO_COVERAGE_GAP"],
        )

    expected_contracts = sorted(
        _allocation_contract(row) for row in expected_rows
    )
    observed_contracts = sorted(
        _allocation_contract(row) for row in observed_rows
    )
    if expected_contracts != observed_contracts:
        fail(
            "persisted exit allocations differ from deterministic FIFO replay",
            expected_count=len(expected_contracts),
            observed_count=len(observed_contracts),
        )

    observed_by_exit: dict[str, list[dict[str, Any]]] = {}
    for row in observed_rows:
        exit_fill_id = str(row.get("exit_fill_id") or "")
        observed_by_exit.setdefault(exit_fill_id, []).append(row)
        evidence_id = str(row.get("evidence_id") or "")
        status = str(row.get("attribution_status") or "")
        allocation_id = str(row.get("allocation_id") or "")
        sequence = _integer(row.get("allocation_sequence"))
        entry_fill_id = str(row.get("entry_fill_id") or "")
        expected_id = _exit_allocation_id(
            exit_fill_id,
            sequence,
            entry_fill_id,
        )
        relation_valid = (
            status in {"ATTRIBUTED", "UNATTRIBUTED"}
            and (status == "ATTRIBUTED") == bool(evidence_id)
            and (
                status == "UNATTRIBUTED"
                or (
                    str(row.get("bound_evidence_id") or "")
                    == evidence_id
                    and str(row.get("evidence_account_id") or "")
                    == str(row.get("account_id") or "")
                    and str(row.get("evidence_stock_code") or "")
                    == str(row.get("stock_code") or "")
                    and str(row.get("evidence_entry_fill_id") or "")
                    == entry_fill_id
                )
            )
            and (
                status == "ATTRIBUTED"
                or not str(row.get("bound_evidence_id") or "")
            )
        )
        row_valid = (
            relation_valid
            and str(row.get("allocation_protocol_version") or "")
            == EXIT_ALLOCATION_PROTOCOL
            and allocation_id == expected_id
            and bool(RESULT_HASH_RE.fullmatch(allocation_id))
            and sequence >= 0
            and _integer(row.get("allocated_quantity")) > 0
        )
        if not row_valid:
            fail(
                "exit allocation row identity/protocol/attribution differs",
                allocation_id=allocation_id,
                exit_fill_id=exit_fill_id,
                allocation_sequence=sequence,
            )

    expected_exit_ids = set(raw_sell_by_id)
    observed_exit_ids = set(observed_by_exit) - {""}
    if observed_exit_ids != expected_exit_ids:
        fail(
            "raw SELL and exit-allocation fill inventories differ",
            missing_exit_fill_ids=sorted(
                expected_exit_ids - observed_exit_ids
            )[:100],
            extra_exit_fill_ids=sorted(
                observed_exit_ids - expected_exit_ids
            )[:100],
        )

    def fixed_six(value: Any) -> Decimal | None:
        parsed = _decimal(value)
        return (
            parsed.quantize(Decimal("0.000001"))
            if parsed is not None
            else None
        )

    for exit_fill_id, rows in observed_by_exit.items():
        raw = raw_sell_by_id.get(exit_fill_id)
        if raw is None:
            continue
        ordered = sorted(
            rows,
            key=lambda row: _integer(row.get("allocation_sequence")),
        )
        sequences = [
            _integer(row.get("allocation_sequence")) for row in ordered
        ]
        quantity_sum = sum(
            _integer(row.get("allocated_quantity")) for row in ordered
        )
        gross_values = [
            fixed_six(row.get("allocated_gross_cny")) for row in ordered
        ]
        fee_values = [
            fixed_six(row.get("allocated_fee_cny")) for row in ordered
        ]
        binding_valid = all(
            str(row.get("account_id") or "")
            == str(raw.get("account_id") or "")
            and str(row.get("stock_code") or "")
            == str(raw.get("stock_code") or "")
            and str(row.get("exit_order_id") or "")
            == str(raw.get("order_id") or "")
            and _normalized_datetime_text(row.get("exit_filled_at"))
            == _normalized_datetime_text(raw.get("filled_at"))
            for row in ordered
        )
        conserved = (
            sequences == list(range(len(ordered)))
            and quantity_sum == _integer(raw.get("quantity"))
            and all(value is not None for value in gross_values)
            and all(value is not None for value in fee_values)
            and sum(gross_values, Decimal("0.000000"))
            == fixed_six(raw.get("gross_amount"))
            and sum(fee_values, Decimal("0.000000"))
            == fixed_six(raw.get("fee_amount"))
            and binding_valid
        )
        if not conserved:
            fail(
                "raw SELL quantity/gross/fee or sequence is not conserved",
                exit_fill_id=exit_fill_id,
                sequences=sequences,
                raw_quantity=raw.get("quantity"),
                allocated_quantity=quantity_sum,
                raw_gross=str(fixed_six(raw.get("gross_amount"))),
                allocated_gross=str(
                    sum(gross_values, Decimal("0.000000"))
                    if all(value is not None for value in gross_values)
                    else None
                ),
                raw_fee=str(fixed_six(raw.get("fee_amount"))),
                allocated_fee=str(
                    sum(fee_values, Decimal("0.000000"))
                    if all(value is not None for value in fee_values)
                    else None
                ),
            )

    return not errors, {
        "protocol_version": EXIT_ALLOCATION_PROTOCOL,
        "raw_fill_count": len(fill_rows),
        "raw_sell_count": raw_sell_count,
        "forecast_relation_count": len(forecast_rows),
        "expected_allocation_count": len(expected_rows),
        "observed_allocation_count": len(observed_rows),
        "replay_diagnostics": dict(sorted(replay_diagnostics.items())),
        "full_replay_has_limit": False,
        "errors": errors,
    }


def _governance_schema_migration_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    from server.engine.strategy_governance import (
        RUN_REVISION_MIGRATION_HASH,
        RUN_REVISION_MIGRATION_KEY,
        STRATEGY_CONTENT_HASH_MIGRATION_HASH,
        STRATEGY_CONTENT_HASH_MIGRATION_KEY,
    )

    rows = _rows(
        connection,
        "SELECT migration_key, migration_hash, completed_at "
        "FROM st_strategy_governance_schema_migration "
        "WHERE migration_key IN (:run_revision_key, :content_hash_key, "
        ":funding_checkpoint_key) "
        "ORDER BY BINARY migration_key",
        {
            "run_revision_key": RUN_REVISION_MIGRATION_KEY,
            "content_hash_key": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
            "funding_checkpoint_key": FUNDING_CHECKPOINT_MIGRATION_KEY,
        },
    )
    expected = {
        RUN_REVISION_MIGRATION_KEY: RUN_REVISION_MIGRATION_HASH,
        STRATEGY_CONTENT_HASH_MIGRATION_KEY:
            STRATEGY_CONTENT_HASH_MIGRATION_HASH,
        FUNDING_CHECKPOINT_MIGRATION_KEY:
            FUNDING_CHECKPOINT_MIGRATION_HASH,
    }
    observed = {
        str(row.get("migration_key") or ""): row for row in rows
    }
    valid = set(observed) == set(expected) and all(
        str(observed[key].get("migration_hash") or "") == expected_hash
        and observed[key].get("completed_at") is not None
        for key, expected_hash in expected.items()
    )
    return valid, {
        "expected_migrations": expected,
        "matching_rows": rows,
    }


def _all_immutable_version_hash_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Recompute every historical strategy and combination digest."""

    from server.engine.strategy_governance import (
        _strategy_content_digest,
        _strategy_version_digest,
    )

    errors: list[dict[str, Any]] = []
    strategy_rows = _rows(
        connection,
        "SELECT strategy_key, version, version_hash, content_hash, "
        "evaluator_type, evaluator_config_json, parameters_json, "
        "source_kind FROM st_strategy_version "
        "ORDER BY BINARY strategy_key, BINARY version",
    )
    strategy_contents: dict[tuple[str, str], str] = {}
    for row in strategy_rows:
        evaluator_config = _json_object(row.get("evaluator_config_json"))
        parameters = _json_object(row.get("parameters_json"))
        strategy_key = str(row.get("strategy_key") or "")
        version = str(row.get("version") or "")
        expected_version_hash = ""
        expected_content_hash = ""
        if evaluator_config is not None and parameters is not None:
            try:
                expected_version_hash = _strategy_version_digest(
                    strategy_key=strategy_key,
                    version=version,
                    evaluator_type=str(row.get("evaluator_type") or ""),
                    evaluator_config=evaluator_config,
                    parameters=parameters,
                    source_kind=str(row.get("source_kind") or ""),
                )
                expected_content_hash = _strategy_content_digest(
                    strategy_key=strategy_key,
                    evaluator_type=str(row.get("evaluator_type") or ""),
                    evaluator_config=evaluator_config,
                    parameters=parameters,
                    source_kind=str(row.get("source_kind") or ""),
                )
            except Exception:
                expected_version_hash = ""
                expected_content_hash = ""
        duplicate_version = strategy_contents.get(
            (strategy_key, expected_content_hash)
        )
        valid = (
            bool(strategy_key)
            and bool(version)
            and RESULT_HASH_RE.fullmatch(expected_version_hash) is not None
            and RESULT_HASH_RE.fullmatch(expected_content_hash) is not None
            and expected_version_hash
            == str(row.get("version_hash") or "")
            and expected_content_hash
            == str(row.get("content_hash") or "")
            and duplicate_version is None
        )
        if not valid:
            errors.append(
                {
                    "entity_type": "STRATEGY",
                    "entity_key": strategy_key,
                    "entity_version": version,
                    "reason": "historical immutable version/content hash differs",
                    "duplicate_content_version": duplicate_version,
                }
            )
        if expected_content_hash:
            strategy_contents[(strategy_key, expected_content_hash)] = version

    combination_rows = _rows(
        connection,
        "SELECT combination_key, version, members_json, constraints_json, "
        "config_hash FROM st_strategy_combination_version "
        "ORDER BY BINARY combination_key, BINARY version",
    )
    combination_contents: dict[tuple[str, str], str] = {}
    for row in combination_rows:
        members = _json_array(row.get("members_json"))
        constraints = _json_object(row.get("constraints_json"))
        combination_key = str(row.get("combination_key") or "")
        version = str(row.get("version") or "")
        expected_hash = (
            _canonical_digest(
                {"members": members, "constraints": constraints}
            )
            if members is not None and constraints is not None
            else ""
        )
        duplicate_version = combination_contents.get(
            (combination_key, expected_hash)
        )
        valid = (
            bool(combination_key)
            and bool(version)
            and RESULT_HASH_RE.fullmatch(expected_hash) is not None
            and expected_hash == str(row.get("config_hash") or "")
            and duplicate_version is None
        )
        if not valid:
            errors.append(
                {
                    "entity_type": "COMBINATION",
                    "entity_key": combination_key,
                    "entity_version": version,
                    "reason": "historical immutable config hash differs",
                    "duplicate_content_version": duplicate_version,
                }
            )
        if expected_hash:
            combination_contents[(combination_key, expected_hash)] = version
    return not errors, {
        "strategy_count": len(strategy_rows),
        "combination_count": len(combination_rows),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _json_array(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, list) else None


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _industry_iso_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(
            str(value or "").strip().replace(" ", "T").replace(
                "Z", "+00:00"
            )
        )
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def _canonical_qmt_industry_hash(rows: list[dict[str, Any]]) -> str:
    values = [
        tuple(str(row.get(column) or "") for column in (
            "industry_code", "industry_name", "industry_type",
            "stock_code", "short_name",
        ))
        for row in rows
    ]
    payload = json.dumps(
        sorted(values), ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _strategy_industry_history_contract_check(
    connection,
) -> tuple[
    bool,
    dict[str, Any],
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    """Independently rebuild every industry ledger date from raw QMT rows."""

    history_rows = _rows(
        connection,
        "SELECT snapshot_id, trade_date, as_of_exclusive, stock_code, "
        "industry_name, industry_type, source_system, source_fact_id, "
        "source_effective_at, source_etl_sync_at, row_hash "
        "FROM st_strategy_industry_history "
        "ORDER BY trade_date, source_system, stock_code",
    )
    cutover = date.fromisoformat(
        STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE
    ).isoformat()
    legacy_rows = [
        row for row in history_rows
        if _iso_date(row.get("trade_date")) < cutover
    ]
    production_rows = [
        row for row in history_rows
        if _iso_date(row.get("trade_date")) >= cutover
    ]
    legacy_dates = sorted({
        _iso_date(row.get("trade_date")) for row in legacy_rows
    })
    isolation_detail = {
        "production_cutover_date": cutover,
        "legacy_isolation_status": "LEGACY_RESEARCH_ONLY",
        "legacy_isolated": True,
        "legacy_isolated_trade_dates": legacy_dates,
        "legacy_isolated_trade_date_count": len(legacy_dates),
        "legacy_isolated_row_count": len(legacy_rows),
        "production_history_row_count": len(production_rows),
    }
    if not production_rows:
        return True, {
            **isolation_detail,
            "trade_date_count": 0,
            "history_row_count": len(history_rows),
            "qmt_run_count": 0,
            "qmt_member_count": 0,
            "invalid_count": 0,
            "errors": [],
        }, {}, {}

    history_source_dates: dict[tuple[str, str], set[str]] = {}
    for row in production_rows:
        history_key = (
            _iso_date(row.get("trade_date")),
            str(row.get("source_system") or ""),
        )
        history_source_dates.setdefault(history_key, set()).add(
            _industry_iso_datetime(row.get("source_effective_at"))[:10]
        )
    source_keys = sorted({
        (trade_date, next(iter(source_dates)), source)
        for (trade_date, source), source_dates in history_source_dates.items()
        if len(source_dates) == 1
    })
    params: dict[str, Any] = {}
    clauses: list[str] = []
    for index, (_trade_date, source_date, source) in enumerate(source_keys):
        params[f"industry_date_{index}"] = source_date
        params[f"industry_source_{index}"] = source
        clauses.append(
            f"(snapshot_date=:industry_date_{index} "
            f"AND source=:industry_source_{index})"
        )
    where = " OR ".join(clauses)
    run_rows = _rows(
        connection,
        "SELECT snapshot_date, source, quality_status, capture_mode, "
        "industry_count, industry_relation_count, industry_hash, captured_at "
        "FROM qmt_membership_snapshot_run WHERE " + where
        + " ORDER BY snapshot_date, source, captured_at",
        params,
    ) if where else []
    member_rows = _rows(
        connection,
        "SELECT snapshot_date, source, industry_code, industry_name, "
        "industry_type, stock_code, short_name, quality_status, captured_at "
        "FROM qmt_industry_member_snapshot WHERE " + where
        + " ORDER BY snapshot_date, source, industry_code, stock_code",
        params,
    ) if where else []
    runs_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    members_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    history_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in run_rows:
        runs_by_key.setdefault((
            _iso_date(row.get("snapshot_date")),
            str(row.get("source") or ""),
        ), []).append(row)
    for row in member_rows:
        members_by_key.setdefault((
            _iso_date(row.get("snapshot_date")),
            str(row.get("source") or ""),
        ), []).append(row)
    for row in production_rows:
        history_by_key.setdefault((
            _iso_date(row.get("trade_date")),
            str(row.get("source_system") or ""),
        ), []).append(row)

    errors: list[dict[str, Any]] = []
    bindings: dict[tuple[str, str], dict[str, Any]] = {}
    snapshots: dict[str, dict[str, Any]] = {}
    dates_to_sources: dict[str, set[str]] = {}
    for trade_date, _source_date, source in source_keys:
        dates_to_sources.setdefault(trade_date, set()).add(source)
    for trade_date, sources in dates_to_sources.items():
        if len(sources) != 1:
            errors.append({
                "trade_date": trade_date,
                "reason": "industry history date has multiple source systems",
                "sources": sorted(sources),
            })

    for (trade_date, source), source_dates in history_source_dates.items():
        if len(source_dates) != 1:
            errors.append({
                "trade_date": trade_date,
                "source": source,
                "reason": "industry history date has multiple source snapshot dates",
                "source_snapshot_dates": sorted(source_dates),
            })

    for trade_date, source_date, source in source_keys:
        identity = {
            "trade_date": trade_date,
            "source_snapshot_date": source_date,
            "source": source,
        }
        runs = runs_by_key.get((source_date, source), [])
        members = members_by_key.get((source_date, source), [])
        observed_history = history_by_key.get((trade_date, source), [])
        if len(runs) != 1:
            errors.append({
                **identity,
                "reason": "exact QMT industry run is missing or duplicate",
                "run_count": len(runs),
            })
            continue
        run = runs[0]
        try:
            captured_at = _industry_iso_datetime(run.get("captured_at"))
            captured_value = datetime.fromisoformat(captured_at)
            cutoff = (
                date.fromisoformat(source_date) + timedelta(days=1)
            ).isoformat() + "T00:00:00"
            earliest = datetime.combine(
                date.fromisoformat(source_date), datetime.min.time()
            ).replace(hour=15)
            published_hash = str(run.get("industry_hash") or "").lower()
            relation_count = int(run.get("industry_relation_count") or 0)
            industry_count = int(run.get("industry_count") or 0)
            actual_industries = len({
                str(row.get("industry_code") or "") for row in members
            })
            members_valid = all(
                _iso_date(row.get("snapshot_date")) == source_date
                and str(row.get("source") or "") == source
                and str(row.get("quality_status") or "") == QMT_VALIDATED
                and _industry_iso_datetime(row.get("captured_at"))
                == captured_at
                for row in members
            )
            if (
                str(run.get("quality_status") or "") != QMT_VALIDATED
                or str(run.get("capture_mode") or "")
                != "qmt_close_full_refresh"
                or RESULT_HASH_RE.fullmatch(published_hash) is None
                or relation_count <= 0 or relation_count != len(members)
                or industry_count <= 0 or industry_count != actual_industries
                or not members_valid
                or _canonical_qmt_industry_hash(members) != published_hash
                or not earliest <= captured_value < datetime.fromisoformat(cutoff)
            ):
                raise ValueError("raw QMT run/member snapshot contract differs")

            fallback_reason = ""
            if source_date != trade_date:
                target_runs = _rows(
                    connection,
                    "SELECT snapshot_date, source, quality_status, "
                    "capture_mode, captured_at "
                    "FROM qmt_membership_snapshot_run "
                    "WHERE snapshot_date=:fallback_target_date "
                    "ORDER BY source, captured_at",
                    {"fallback_target_date": trade_date},
                )
                if target_runs:
                    raise ValueError(
                        "target-date QMT run exists; previous-session fallback denied"
                    )
                fallback_reason = QMT_PREVIOUS_SESSION_FALLBACKS.get(
                    trade_date, "",
                )
                previous_rows = _rows(
                    connection,
                    "SELECT MAX(trade_date) AS trade_date "
                    "FROM si_trade_calendar "
                    "WHERE trade_date < :trade_date AND trade_status=1",
                    {"trade_date": trade_date},
                )
                previous_date = _iso_date(
                    (previous_rows[0] if previous_rows else {}).get(
                        "trade_date"
                    )
                )
                if not fallback_reason or previous_date != source_date:
                    raise ValueError(
                        "QMT previous-session fallback is not explicitly authorized"
                    )

            normalized: list[dict[str, Any]] = []
            seen_codes: set[str] = set()
            for member in members:
                industry_type = str(
                    member.get("industry_type") or ""
                ).strip()
                if industry_type not in L1_INDUSTRY_TYPES:
                    continue
                stock_code = str(
                    member.get("stock_code") or ""
                ).strip().zfill(6)
                industry_code = str(
                    member.get("industry_code") or ""
                ).strip()
                industry_name = str(
                    member.get("industry_name") or ""
                ).strip()
                if (
                    re.fullmatch(r"[0-9]{6}", stock_code) is None
                    or stock_code in seen_codes
                    or not industry_code or not industry_name
                    or len(industry_name) > 120
                    or len(industry_type) > 40
                    or not source or len(source) > 80
                ):
                    raise ValueError(
                        "raw QMT L1 member identity is incomplete or duplicate"
                    )
                seen_codes.add(stock_code)
                fact_payload = {
                    "trade_date": trade_date,
                    "source": source,
                    "industry_hash": published_hash,
                    "industry_code": industry_code,
                    "industry_name": industry_name,
                    "industry_type": industry_type,
                    "stock_code": stock_code,
                }
                if fallback_reason:
                    fact_payload.update({
                        "source_snapshot_date": source_date,
                        "capture_mode": "qmt_close_full_refresh",
                        "fallback_reason": fallback_reason,
                    })
                fact_digest = _canonical_digest(fact_payload)
                normalized.append({
                    "stock_code": stock_code,
                    "industry_name": industry_name,
                    "industry_type": industry_type,
                    "source_system": source,
                    "source_fact_id": (
                        f"qmt:{published_hash}:{fact_digest}"
                    ),
                    "source_effective_at": captured_at,
                    "source_etl_sync_at": captured_at,
                })
            normalized.sort(key=lambda row: row["stock_code"])
            if not normalized:
                raise ValueError("raw QMT snapshot has no L1 stock facts")
            target_cutoff = (
                date.fromisoformat(trade_date) + timedelta(days=1)
            ).isoformat() + "T00:00:00"
            snapshot_payload = {
                "schema": "probiga.strategy-industry-qmt-snapshot.v2",
                "trade_date": trade_date,
                "as_of_exclusive": target_cutoff,
                "qmt_source": source,
                "qmt_industry_hash": published_hash,
                "qmt_captured_at": captured_at,
                "facts": normalized,
            }
            if fallback_reason:
                snapshot_payload.update({
                    "qmt_source_snapshot_date": source_date,
                    "qmt_capture_mode": "qmt_close_full_refresh",
                    "qmt_fallback_reason": fallback_reason,
                })
            snapshot_id = _canonical_digest(snapshot_payload)
            expected_rows = []
            for normalized_row in normalized:
                row_payload = {
                    "snapshot_id": snapshot_id,
                    "trade_date": trade_date,
                    "as_of_exclusive": target_cutoff,
                    **normalized_row,
                }
                expected_rows.append({
                    **row_payload,
                    "row_hash": _canonical_digest(row_payload),
                })
            normalized_observed = []
            for row in observed_history:
                normalized_observed.append({
                    "snapshot_id": str(row.get("snapshot_id") or ""),
                    "trade_date": _iso_date(row.get("trade_date")),
                    "as_of_exclusive": _industry_iso_datetime(
                        row.get("as_of_exclusive")
                    ),
                    "stock_code": str(row.get("stock_code") or ""),
                    "industry_name": str(row.get("industry_name") or ""),
                    "industry_type": str(row.get("industry_type") or ""),
                    "source_system": str(row.get("source_system") or ""),
                    "source_fact_id": str(row.get("source_fact_id") or ""),
                    "source_effective_at": _industry_iso_datetime(
                        row.get("source_effective_at")
                    ),
                    "source_etl_sync_at": _industry_iso_datetime(
                        row.get("source_etl_sync_at")
                    ),
                    "row_hash": str(row.get("row_hash") or ""),
                })
            normalized_observed.sort(key=lambda row: row["stock_code"])
            if normalized_observed != expected_rows:
                raise ValueError(
                    "strategy industry history differs from exact QMT replay"
                )
            if trade_date not in snapshots:
                snapshots[trade_date] = {
                    "snapshot_id": snapshot_id,
                    "source": source,
                    "source_snapshot_hash": published_hash,
                    "source_snapshot_date": source_date,
                    "capture_mode": "qmt_close_full_refresh",
                    "fallback_reason": fallback_reason,
                    "captured_at": captured_at,
                    "row_count": len(expected_rows),
                }
            for row in expected_rows:
                bindings[(trade_date, row["stock_code"])] = row
        except Exception as exc:
            errors.append({
                **identity,
                "reason": _safe_exception_message(exc),
            })

    return not errors, {
        **isolation_detail,
        "trade_date_count": len(dates_to_sources),
        "history_row_count": len(history_rows),
        "qmt_run_count": len(run_rows),
        "qmt_member_count": len(member_rows),
        "replayed_binding_count": len(bindings),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }, bindings, snapshots


def _candidate_industry_snapshot_contract(
    result: dict[str, Any],
    *,
    trade_date: str,
    history_bindings: dict[tuple[str, str], dict[str, Any]],
    history_snapshots: dict[str, dict[str, Any]],
) -> tuple[bool, dict[str, Any], dict[str, dict[str, Any]]]:
    snapshot = result.get("candidate_industry_snapshot")
    summary = result.get("summary")
    errors: list[dict[str, Any]] = []
    if not isinstance(snapshot, dict) or not isinstance(summary, dict):
        return False, {
            "errors": [{"reason": "candidate industry snapshot is missing"}]
        }, {}
    expected_fields = {
        "schema", "snapshot_id", "trade_date", "as_of_exclusive",
        "status", "requested_stock_codes", "rows", "reason",
        "snapshot_hash",
    }
    requested = snapshot.get("requested_stock_codes")
    rows = snapshot.get("rows")
    cutoff = (
        date.fromisoformat(trade_date) + timedelta(days=1)
    ).isoformat() + "T00:00:00"
    snapshot_hash = str(snapshot.get("snapshot_hash") or "")
    snapshot_id = str(snapshot.get("snapshot_id") or "")
    source_snapshot = history_snapshots.get(trade_date) or {}
    if (
        set(snapshot) != expected_fields
        or snapshot.get("schema") != INDUSTRY_SNAPSHOT_SCHEMA
        or str(snapshot.get("trade_date") or "") != trade_date
        or str(snapshot.get("as_of_exclusive") or "") != cutoff
        or not isinstance(requested, list)
        or requested != sorted(set(str(code) for code in requested))
        or any(re.fullmatch(r"[0-9]{6}", str(code)) is None for code in requested)
        or not isinstance(rows, list)
        or RESULT_HASH_RE.fullmatch(snapshot_hash) is None
        or _canonical_digest({
            str(key): value for key, value in snapshot.items()
            if str(key) != "snapshot_hash"
        }) != snapshot_hash
        or snapshot_hash
        != str(result.get("candidate_industry_snapshot_hash") or "")
        or snapshot_hash
        != str(summary.get("candidate_industry_snapshot_hash") or "")
        or snapshot_id
        != str(summary.get("candidate_industry_snapshot_id") or "")
        or str(snapshot.get("status") or "")
        != str(summary.get("candidate_industry_snapshot_status") or "")
    ):
        errors.append({"reason": "candidate industry wrapper/hash differs"})
    observed_by_code: dict[str, dict[str, Any]] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            errors.append({"reason": "candidate industry row is not an object"})
            continue
        code = str(row.get("stock_code") or "")
        expected = history_bindings.get((trade_date, code))
        if code in observed_by_code or expected is None or row != expected:
            errors.append({
                "reason": "candidate industry row differs from QMT replay",
                "stock_code": code,
            })
            continue
        observed_by_code[code] = row
    expected_available = {
        code for code in (requested or [])
        if (trade_date, str(code)) in history_bindings
    }
    observed_codes = set(observed_by_code)
    if observed_codes != expected_available:
        errors.append({
            "reason": "candidate industry rows omit or add an available fact",
            "expected_codes": sorted(expected_available),
            "observed_codes": sorted(observed_codes),
        })
    expected_status = (
        "COMPLETED"
        if requested and len(expected_available) == len(requested)
        else "INCOMPLETE"
    )
    expected_snapshot_id = (
        str(source_snapshot.get("snapshot_id") or "")
        if observed_codes else ""
    )
    if (
        str(snapshot.get("status") or "") != expected_status
        or snapshot_id != expected_snapshot_id
        or (observed_codes and RESULT_HASH_RE.fullmatch(snapshot_id) is None)
    ):
        errors.append({
            "reason": "candidate industry completion/snapshot identity differs",
            "expected_status": expected_status,
            "expected_snapshot_id": expected_snapshot_id,
        })
    bindings = {
        code: {
            "schema": INDUSTRY_BINDING_SCHEMA,
            "snapshot_id": snapshot_id,
            "snapshot_hash": snapshot_hash,
            "row_hash": row["row_hash"],
            "trade_date": trade_date,
            "as_of_exclusive": cutoff,
            "stock_code": code,
            "industry_name": row["industry_name"],
            "industry_type": row["industry_type"],
            "source_system": row["source_system"],
            "source_fact_id": row["source_fact_id"],
            "source_effective_at": row["source_effective_at"],
            "source_etl_sync_at": row["source_etl_sync_at"],
        }
        for code, row in observed_by_code.items()
    }
    return not errors, {
        "snapshot_id": snapshot_id,
        "snapshot_hash": snapshot_hash,
        "status": snapshot.get("status"),
        "requested_count": len(requested or []),
        "bound_count": len(bindings),
        "errors": errors[:100],
    }, bindings


def _json_document(value: Any) -> tuple[bool, Any]:
    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        return True, value
    try:
        return True, json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, None


def _automatic_transition_plan_payload(
    *, trade_date: str,
    transitions: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized = []
    for item in transitions:
        normalized_item = dict(item)
        evidence = normalized_item.get("evidence")
        normalized_item["evidence"] = {
            str(key): value
            for key, value in (evidence or {}).items()
            if str(key) != "run_uid"
        }
        normalized.append(normalized_item)
    normalized.sort(key=lambda item: (
        item["entity_type"],
        item["entity_key"],
        item["entity_version"],
        item["previous_status"],
        item["next_status"],
        _canonical_digest(item),
    ))
    return {
        "schema": AUTOMATIC_TRANSITION_PLAN_SCHEMA,
        "trade_date": trade_date,
        "transition_count": len(normalized),
        "transitions": normalized,
    }


def _immutable_lifecycle_and_audit_history_check(
    connection,
) -> tuple[bool, dict[str, Any], dict[str, str]]:
    """Replay every event/audit hash and bind automatic events to runs."""

    lifecycle_rows = _rows(
        connection,
        "SELECT event_id, entity_type, entity_key, entity_version, "
        "previous_status, next_status, reason, trigger_type, evidence_json, "
        "payload_json, event_hash, operator_name, occurred_at "
        "FROM st_strategy_lifecycle_event "
        "ORDER BY occurred_at, BINARY event_id",
    )
    audit_rows = _rows(
        connection,
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "ORDER BY created_at, BINARY audit_id",
    )
    run_rows = _rows(
        connection,
        "SELECT run_uid, trade_date, run_revision, supersedes_run_uid, "
        "is_canonical, market_state, source_status, input_ready, "
        "input_hash, build_commit_sha, router_policy_version, "
        "router_snapshot_hash, decision_hash, status, strategy_count, "
        "formal_count, shadow_count, combination_count, "
        "observation_count, confirmation_count, tradable_count, "
        "allocation_count, summary_json, created_at, finished_at "
        "FROM st_strategy_governance_run WHERE status='COMPLETED' "
        "ORDER BY BINARY run_uid",
    )
    errors: list[dict[str, Any]] = []
    valid_audits: list[dict[str, Any]] = []
    seen_audit_ids: set[str] = set()
    seen_audit_hashes: set[str] = set()
    for row in audit_rows:
        audit_id = str(row.get("audit_id") or "")
        audit_hash = str(row.get("audit_hash") or "")
        before_ok, before = _json_document(row.get("before_json"))
        after_ok, after = _json_document(row.get("after_json"))
        evidence_ok, evidence = _json_document(row.get("evidence_json"))
        payload = _json_object(row.get("payload_json"))
        valid = (
            re.fullmatch(r"[0-9a-f]{32}", audit_id) is not None
            and audit_id not in seen_audit_ids
            and RESULT_HASH_RE.fullmatch(audit_hash) is not None
            and audit_hash not in seen_audit_hashes
            and before_ok
            and after_ok
            and evidence_ok
            and payload is not None
            and set(payload)
            == {
                "entity_type",
                "entity_key",
                "action",
                "reason",
                "operator",
                "before",
                "after",
                "evidence",
                "nonce",
            }
            and payload.get("entity_type") == row.get("entity_type")
            and payload.get("entity_key") == row.get("entity_key")
            and payload.get("action") == row.get("action")
            and payload.get("reason") == row.get("reason")
            and payload.get("operator") == row.get("operator_name")
            and payload.get("before") == before
            and payload.get("after") == after
            and payload.get("evidence") == evidence
            and re.fullmatch(
                r"[0-9a-f]{32}", str(payload.get("nonce") or "")
            )
            is not None
            and _canonical_digest(payload) == audit_hash
        )
        if not valid:
            errors.append({
                "record_type": "AUDIT",
                "record_id": audit_id,
                "reason": "audit payload/hash/column binding differs",
            })
        else:
            valid_audits.append({
                "row": row,
                "before": before,
                "after": after,
                "evidence": evidence,
                "payload": payload,
            })
        seen_audit_ids.add(audit_id)
        seen_audit_hashes.add(audit_hash)

    runs: dict[str, dict[str, Any]] = {}
    for row in run_rows:
        run_uid = str(row.get("run_uid") or "")
        summary = _json_object(row.get("summary_json"))
        if (
            re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
            or run_uid in runs
            or summary is None
        ):
            errors.append({
                "record_type": "RUN",
                "record_id": run_uid,
                "reason": "completed run identity/summary differs",
            })
            continue
        runs[run_uid] = {
            "trade_date": _iso_date(row.get("trade_date")),
            "summary": summary,
            "transitions": [],
            "row": row,
        }

    seen_event_ids: set[str] = set()
    seen_event_hashes: set[str] = set()
    seen_automatic_transitions: set[tuple[str, ...]] = set()
    matched_transition_audits: set[str] = set()
    for row in lifecycle_rows:
        event_id = str(row.get("event_id") or "")
        event_hash = str(row.get("event_hash") or "")
        evidence = _json_object(row.get("evidence_json"))
        payload = _json_object(row.get("payload_json"))
        entity_type = str(row.get("entity_type") or "")
        entity_key = str(row.get("entity_key") or "")
        entity_version = str(row.get("entity_version") or "")
        previous_status = str(row.get("previous_status") or "")
        next_status = str(row.get("next_status") or "")
        trigger_type = str(row.get("trigger_type") or "")
        payload_version = str(
            (payload or {}).get("entity_version")
            or (payload or {}).get("new_version")
            or ""
        )
        valid = (
            re.fullmatch(r"[0-9a-f]{32}", event_id) is not None
            and event_id not in seen_event_ids
            and RESULT_HASH_RE.fullmatch(event_hash) is not None
            and event_hash not in seen_event_hashes
            and entity_type in {"STRATEGY", "COMBINATION"}
            and bool(entity_key)
            and bool(entity_version)
            and previous_status in LIFECYCLE_STATES
            and next_status in LIFECYCLE_STATES
            and bool(str(row.get("reason") or ""))
            and bool(trigger_type)
            and bool(str(row.get("operator_name") or ""))
            and evidence is not None
            and payload is not None
            and _canonical_digest(payload) == event_hash
            and payload.get("entity_type") == entity_type
            and payload.get("entity_key") == entity_key
            and payload_version == entity_version
            and payload.get("previous_status") == previous_status
            and payload.get("next_status") == next_status
            and (
                "reason" not in payload
                or payload.get("reason") == row.get("reason")
            )
            and (
                "evidence" not in payload
                or payload.get("evidence") == evidence
            )
            and (
                "nonce" not in payload
                or re.fullmatch(
                    r"[0-9a-f]{32}", str(payload.get("nonce") or "")
                )
                is not None
            )
        )
        if not valid:
            errors.append({
                "record_type": "LIFECYCLE_EVENT",
                "record_id": event_id,
                "reason": "event payload/hash/column binding differs",
            })
        seen_event_ids.add(event_id)
        seen_event_hashes.add(event_hash)
        if trigger_type != "AUTOMATIC_GATE":
            continue
        run_uid = str((evidence or {}).get("run_uid") or "")
        trade_date = _iso_date((evidence or {}).get("trade_date"))
        entry = {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "entity_version": entity_version,
            "previous_status": previous_status,
            "next_status": next_status,
            "reason": str(row.get("reason") or ""),
            "evidence": evidence,
        }
        run = runs.get(run_uid)
        transition_identity = (
            run_uid,
            entity_type,
            entity_key,
            entity_version,
            previous_status,
            next_status,
        )
        if (
            not valid
            or run is None
            or trade_date != run.get("trade_date")
            or payload is None
            or payload.get("evidence") != evidence
            or re.fullmatch(
                r"[0-9a-f]{32}", str(payload.get("nonce") or "")
            )
            is None
            or transition_identity in seen_automatic_transitions
        ):
            errors.append({
                "record_type": "AUTOMATIC_TRANSITION",
                "record_id": event_id,
                "run_uid": run_uid,
                "reason": "automatic event is not bound to its completed run",
            })
            continue
        seen_automatic_transitions.add(transition_identity)
        run["transitions"].append(entry)
        matching_audits = []
        for audit in valid_audits:
            audit_row = audit["row"]
            before = audit["before"]
            after = audit["after"]
            if (
                audit_row.get("entity_type") == entity_type
                and audit_row.get("entity_key") == entity_key
                and audit_row.get("action") == "LIFECYCLE_TRANSITION"
                and audit_row.get("reason") == row.get("reason")
                and audit_row.get("operator_name")
                == row.get("operator_name")
                and isinstance(before, dict)
                and isinstance(after, dict)
                and before.get("status") == previous_status
                and before.get("version") == entity_version
                and after.get("status") == next_status
                and after.get("version") == entity_version
                and audit.get("evidence") == evidence
            ):
                matching_audits.append(audit)
        if len(matching_audits) != 1:
            errors.append({
                "record_type": "AUTOMATIC_TRANSITION",
                "record_id": event_id,
                "run_uid": run_uid,
                "reason": (
                    "automatic event requires exactly one matching audit"
                ),
                "matching_audit_count": len(matching_audits),
            })
        else:
            matched_transition_audits.add(
                str(matching_audits[0]["row"].get("audit_id") or "")
            )

    run_plan_hashes: dict[str, str] = {}
    for run_uid, run in sorted(runs.items()):
        plan = _automatic_transition_plan_payload(
            trade_date=run["trade_date"],
            transitions=run["transitions"],
        )
        plan_hash = _canonical_digest(plan)
        run_plan_hashes[run_uid] = plan_hash
        summary = run["summary"]
        run_row = run["row"]
        expected_after = {
            "status": "COMPLETED",
            "trade_date": run["trade_date"],
            "run_revision": _integer(run_row.get("run_revision")),
            "supersedes_run_uid": str(
                run_row.get("supersedes_run_uid") or ""
            ),
            "is_canonical": True,
            "summary": summary,
        }
        expected_evidence = {
            "run_uid": run_uid,
            "run_revision": _integer(run_row.get("run_revision")),
            "supersedes_run_uid": str(
                run_row.get("supersedes_run_uid") or ""
            ),
            "input_hash": str(run_row.get("input_hash") or ""),
            "decision_hash": str(run_row.get("decision_hash") or ""),
            "build_commit_sha": str(
                run_row.get("build_commit_sha") or ""
            ),
            "router_policy_version": str(
                run_row.get("router_policy_version") or ""
            ),
            "router_snapshot_hash": str(
                run_row.get("router_snapshot_hash") or ""
            ),
            "automatic_real_order_submission": False,
        }
        run_audits = [
            audit for audit in valid_audits
            if audit["row"].get("entity_type") == "SYSTEM"
            and audit["row"].get("entity_key")
            == "strategy_governance_daily"
            and audit["row"].get("action") == "RUN_GOVERNANCE"
            and isinstance(audit.get("evidence"), dict)
            and audit["evidence"].get("run_uid") == run_uid
            and audit.get("before") == {}
            and audit.get("after") == expected_after
            and audit.get("evidence") == expected_evidence
        ]
        if (
            _integer(summary.get("automatic_transition_count"))
            != plan["transition_count"]
            or str(summary.get("automatic_transition_plan_hash") or "")
            != plan_hash
            or len(run_audits) != 1
        ):
            errors.append({
                "record_type": "RUN_TRANSITION_PLAN",
                "record_id": run_uid,
                "reason": "run summary/audit transition plan binding differs",
                "expected_transition_count": plan["transition_count"],
                "stored_transition_count": summary.get(
                    "automatic_transition_count"
                ),
                "expected_transition_plan_hash": plan_hash,
                "stored_transition_plan_hash": summary.get(
                    "automatic_transition_plan_hash"
                ),
                "matching_run_audit_count": len(run_audits),
            })

    for audit in valid_audits:
        audit_row = audit["row"]
        evidence = audit.get("evidence")
        if (
            audit_row.get("action") == "LIFECYCLE_TRANSITION"
            and isinstance(evidence, dict)
            and str(evidence.get("run_uid") or "") in runs
            and str(audit_row.get("audit_id") or "")
            not in matched_transition_audits
        ):
            errors.append({
                "record_type": "AUDIT",
                "record_id": audit_row.get("audit_id"),
                "reason": "orphan or duplicate automatic transition audit",
            })
    return not errors, {
        "lifecycle_event_count": len(lifecycle_rows),
        "audit_count": len(audit_rows),
        "completed_run_count": len(runs),
        "automatic_transition_count": sum(
            len(run["transitions"]) for run in runs.values()
        ),
        "run_transition_plan_hashes": run_plan_hashes,
        "invalid_count": len(errors),
        "errors": errors[:100],
    }, run_plan_hashes


def _lifecycle_registry_projection_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Project current registry state only from the immutable event ledger.

    Registry rows must remain mutable so a legitimate transition can advance
    its current pointer.  That pointer is never authoritative by itself: this
    check replays the append-only lifecycle ledger and requires the version,
    state and reason projection to match exactly.  A new immutable version may
    reset a retired predecessor to SHADOW; RETIRED is terminal within one
    version.
    """

    registry_rows = _rows(
        connection,
        "SELECT entity_type, entity_key, entity_version, current_status, "
        "status_reason, enabled FROM ("
        "SELECT 'STRATEGY' AS entity_type, strategy_key AS entity_key, "
        "current_version AS entity_version, current_status, status_reason, "
        "enabled FROM st_strategy_registry UNION ALL "
        "SELECT 'COMBINATION' AS entity_type, "
        "combination_key AS entity_key, current_version AS entity_version, "
        "current_status, status_reason, enabled "
        "FROM st_strategy_combination) current_registry "
        "ORDER BY BINARY entity_type, BINARY entity_key",
    )
    event_rows = _rows(
        connection,
        "SELECT event_id, entity_type, entity_key, entity_version, "
        "previous_status, next_status, reason, trigger_type, payload_json, "
        "event_hash, occurred_at FROM st_strategy_lifecycle_event "
        "ORDER BY occurred_at, BINARY event_id",
    )
    errors: list[dict[str, Any]] = []
    registry: dict[tuple[str, str], dict[str, Any]] = {}
    for row in registry_rows:
        identity = (
            str(row.get("entity_type") or ""),
            str(row.get("entity_key") or ""),
        )
        if (
            identity[0] not in {"STRATEGY", "COMBINATION"}
            or not identity[1]
            or identity in registry
        ):
            errors.append({
                "record_type": "REGISTRY",
                "entity_type": identity[0],
                "entity_key": identity[1],
                "reason": "current registry identity is invalid or repeated",
            })
            continue
        registry[identity] = row

    projection: dict[tuple[str, str], dict[str, str]] = {}
    seen_event_ids: set[str] = set()
    for row in event_rows:
        event_id = str(row.get("event_id") or "")
        entity_type = str(row.get("entity_type") or "")
        entity_key = str(row.get("entity_key") or "")
        entity_version = str(row.get("entity_version") or "")
        previous_status = str(row.get("previous_status") or "")
        next_status = str(row.get("next_status") or "")
        trigger_type = str(row.get("trigger_type") or "")
        reason = str(row.get("reason") or "")
        identity = (entity_type, entity_key)
        payload = _json_object(row.get("payload_json"))
        payload_version = str(
            (payload or {}).get("entity_version")
            or (payload or {}).get("new_version")
            or ""
        )
        base_valid = (
            re.fullmatch(r"[0-9a-f]{32}", event_id) is not None
            and event_id not in seen_event_ids
            and identity in registry
            and bool(entity_version)
            and previous_status in LIFECYCLE_STATES
            and next_status in LIFECYCLE_STATES
            and bool(reason)
            and payload is not None
            and payload.get("entity_type") == entity_type
            and payload.get("entity_key") == entity_key
            and payload_version == entity_version
            and payload.get("previous_status") == previous_status
            and payload.get("next_status") == next_status
            and _canonical_digest(payload)
            == str(row.get("event_hash") or "")
        )
        if not base_valid:
            errors.append({
                "record_type": "LIFECYCLE_PROJECTION",
                "record_id": event_id,
                "entity_type": entity_type,
                "entity_key": entity_key,
                "reason": "event identity, hash or column binding differs",
            })
            seen_event_ids.add(event_id)
            continue

        previous = projection.get(identity)
        if trigger_type == "VERSION_REGISTRATION":
            old_version = str(payload.get("old_version") or "")
            new_version = str(payload.get("new_version") or "")
            valid_transition = (
                new_version == entity_version
                and new_version != old_version
                and next_status == "SHADOW"
                and (
                    previous is None
                    and old_version == ""
                    or previous is not None
                    and old_version == previous["entity_version"]
                    and previous_status == previous["current_status"]
                )
            )
        else:
            valid_transition = (
                trigger_type in {"AUTOMATIC_GATE", "MANUAL_GOVERNANCE"}
                and previous is not None
                and entity_version == previous["entity_version"]
                and previous_status == previous["current_status"]
                and next_status
                in LIFECYCLE_TRANSITIONS.get(previous_status, frozenset())
                and payload.get("reason") == reason
            )
        if not valid_transition:
            errors.append({
                "record_type": "LIFECYCLE_PROJECTION",
                "record_id": event_id,
                "entity_type": entity_type,
                "entity_key": entity_key,
                "reason": "event does not continue the frozen lifecycle",
            })
        else:
            projection[identity] = {
                "entity_version": entity_version,
                "current_status": next_status,
                "status_reason": reason,
                "event_id": event_id,
            }
        seen_event_ids.add(event_id)

    for identity, row in sorted(registry.items()):
        projected = projection.get(identity)
        if projected is None:
            errors.append({
                "record_type": "REGISTRY_PROJECTION",
                "entity_type": identity[0],
                "entity_key": identity[1],
                "reason": "registry has no complete immutable event history",
            })
            continue
        if (
            str(row.get("entity_version") or "")
            != projected["entity_version"]
            or str(row.get("current_status") or "")
            != projected["current_status"]
            or str(row.get("status_reason") or "")
            != projected["status_reason"]
            or (
                projected["current_status"] == "RETIRED"
                and _integer(row.get("enabled")) != 0
            )
        ):
            errors.append({
                "record_type": "REGISTRY_PROJECTION",
                "entity_type": identity[0],
                "entity_key": identity[1],
                "reason": "current registry differs from immutable projection",
            })

    projection_rows = [
        {
            "entity_type": identity[0],
            "entity_key": identity[1],
            **projected,
        }
        for identity, projected in sorted(projection.items())
    ]
    return not errors, {
        "registry_count": len(registry_rows),
        "event_count": len(event_rows),
        "projected_count": len(projection),
        "projection_hash": _canonical_digest({
            "schema": "probiga.strategy-lifecycle-projection.v1",
            "entities": projection_rows,
        }),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _all_governance_snapshot_history_check(
    connection,
    *,
    industry_history_bindings: dict[
        tuple[str, str], dict[str, Any]
    ] | None = None,
    industry_history_trade_dates: set[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Recompute every immutable detail snapshot bound by a completed run."""

    industry_history_bindings = industry_history_bindings or {}
    industry_history_trade_dates = industry_history_trade_dates or set()
    all_run_rows = _rows(
        connection,
        "SELECT run_uid, trade_date, market_state, status, summary_json "
        "FROM st_strategy_governance_run WHERE status='COMPLETED' "
        "ORDER BY BINARY run_uid",
    )
    cutover = STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE
    legacy_run_uids = {
        str(row.get("run_uid") or "")
        for row in all_run_rows
        if _iso_date(row.get("trade_date")) < cutover
    }
    run_rows = [
        row for row in all_run_rows
        if _iso_date(row.get("trade_date")) >= cutover
    ]
    runs: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    for row in run_rows:
        run_uid = str(row.get("run_uid") or "")
        summary = _json_object(row.get("summary_json"))
        if (
            re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
            or run_uid in runs
            or summary is None
        ):
            errors.append({
                "snapshot_type": "RUN",
                "run_uid": run_uid,
                "reason": "completed run summary identity differs",
            })
            continue
        runs[run_uid] = {
            "trade_date": _iso_date(row.get("trade_date")),
            "market_state": str(row.get("market_state") or ""),
            "summary": summary,
        }

    strategy_rows = _rows(
        connection,
        "SELECT run_uid, strategy_key, strategy_version, trade_date, "
        "window_days, profit_gate_passed, recommended_status, "
        "evidence_json, result_hash FROM st_strategy_health_snapshot "
        "ORDER BY BINARY run_uid, BINARY strategy_key, "
        "BINARY strategy_version, window_days",
    )
    strategy_windows: dict[tuple[str, str, str], list[int]] = {}
    legacy_strategy_row_count = 0
    for row in strategy_rows:
        run_uid = str(row.get("run_uid") or "")
        if run_uid in legacy_run_uids:
            legacy_strategy_row_count += 1
            continue
        payload = _json_object(row.get("evidence_json"))
        key = str(row.get("strategy_key") or "")
        version = str(row.get("strategy_version") or "")
        window = _integer(row.get("window_days"))
        run = runs.get(run_uid)
        valid = (
            run is not None
            and payload is not None
            and _canonical_digest(payload)
            == str(row.get("result_hash") or "")
            and RESULT_HASH_RE.fullmatch(
                str(row.get("result_hash") or "")
            ) is not None
            and payload.get("strategy_key") == key
            and payload.get("strategy_version") == version
            and _iso_date(payload.get("trade_date")) == run.get("trade_date")
            and _iso_date(row.get("trade_date")) == run.get("trade_date")
            and _integer(payload.get("window_days")) == window
            and window in EXPECTED_WINDOWS
            and isinstance(payload.get("gate"), dict)
            and payload["gate"].get("passed")
            is (_integer(row.get("profit_gate_passed")) == 1)
            and str(row.get("recommended_status") or "")
            in LIFECYCLE_STATES
        )
        if not valid:
            errors.append({
                "snapshot_type": "STRATEGY_HEALTH",
                "run_uid": run_uid,
                "entity_key": key,
                "window_days": window,
                "reason": "strategy snapshot identity/payload hash differs",
            })
        strategy_windows.setdefault((run_uid, key, version), []).append(window)

    combination_rows = _rows(
        connection,
        "SELECT run_uid, combination_key, combination_version, trade_date, "
        "profit_gate_passed, recommended_status, evidence_json, result_hash "
        "FROM st_strategy_combination_health_snapshot "
        "ORDER BY BINARY run_uid, BINARY combination_key, "
        "BINARY combination_version",
    )
    combination_keys: dict[str, list[tuple[str, str]]] = {}
    legacy_combination_row_count = 0
    for row in combination_rows:
        run_uid = str(row.get("run_uid") or "")
        if run_uid in legacy_run_uids:
            legacy_combination_row_count += 1
            continue
        payload = _json_object(row.get("evidence_json"))
        key = str(row.get("combination_key") or "")
        version = str(row.get("combination_version") or "")
        run = runs.get(run_uid)
        metrics = (payload or {}).get("metrics")
        valid = (
            run is not None
            and payload is not None
            and _canonical_digest(payload)
            == str(row.get("result_hash") or "")
            and RESULT_HASH_RE.fullmatch(
                str(row.get("result_hash") or "")
            ) is not None
            and payload.get("combination_key") == key
            and payload.get("combination_version") == version
            and _iso_date(payload.get("trade_date")) == run.get("trade_date")
            and _iso_date(row.get("trade_date")) == run.get("trade_date")
            and payload.get("overall_profit_gate_passed")
            is (_integer(row.get("profit_gate_passed")) == 1)
            and isinstance(metrics, dict)
            and set(metrics) == {str(value) for value in EXPECTED_WINDOWS}
            and str(row.get("recommended_status") or "")
            in LIFECYCLE_STATES
        )
        if not valid:
            errors.append({
                "snapshot_type": "COMBINATION_HEALTH",
                "run_uid": run_uid,
                "entity_key": key,
                "reason": "combination snapshot identity/payload hash differs",
            })
        combination_keys.setdefault(run_uid, []).append((key, version))

    pool_rows = _rows(
        connection,
        "SELECT run_uid, trade_date, pool_level, stock_code, stock_name, "
        "rank_no, opportunity_score, execution_score, dominant_strategy, "
        "strategies_json, industry_name, gate_status, reason_json, "
        "evidence_json FROM st_strategy_pool_snapshot "
        "ORDER BY BINARY run_uid, BINARY pool_level, rank_no, "
        "BINARY stock_code",
    )
    pool_contracts: dict[str, list[dict[str, Any]]] = {}
    legacy_pool_row_count = 0
    for row in pool_rows:
        run_uid = str(row.get("run_uid") or "")
        if run_uid in legacy_run_uids:
            legacy_pool_row_count += 1
            continue
        run = runs.get(run_uid)
        strategies = _json_array(row.get("strategies_json"))
        reason = _json_object(row.get("reason_json"))
        envelope = _json_object(row.get("evidence_json"))
        try:
            industry_binding = (envelope or {}).get("industry_binding")
            industry_names = (envelope or {}).get("industry_names")
            industry_by_strategy = (envelope or {}).get(
                "industry_by_strategy"
            )
            payload = {
                "schema": POOL_ROW_SCHEMA,
                "trade_date": _iso_date(row.get("trade_date")),
                "pool_level": str(row.get("pool_level") or ""),
                "stock_code": str(row.get("stock_code") or ""),
                "stock_name": str(row.get("stock_name") or ""),
                "rank_no": _integer(row.get("rank_no")),
                "opportunity_score": _pool_score_text(row.get("opportunity_score")),
                "execution_score": _pool_score_text(row.get("execution_score")),
                "dominant_strategy": str(row.get("dominant_strategy") or ""),
                "strategies": strategies,
                "industry_name": str(row.get("industry_name") or ""),
                "industry_type": str(
                    (industry_binding or {}).get("industry_type") or ""
                ),
                "industry_snapshot_id": str(
                    (industry_binding or {}).get("snapshot_id") or ""
                ),
                "industry_snapshot_hash": str(
                    (industry_binding or {}).get("snapshot_hash") or ""
                ),
                "industry_row_hash": str(
                    (industry_binding or {}).get("row_hash") or ""
                ),
                "industry_source_system": str(
                    (industry_binding or {}).get("source_system") or ""
                ),
                "industry_source_fact_id": str(
                    (industry_binding or {}).get("source_fact_id") or ""
                ),
                "industry_binding": industry_binding,
                "industry_names": industry_names,
                "industry_by_strategy": industry_by_strategy,
                "gate_status": str(row.get("gate_status") or ""),
                "reason": reason,
                "evidence": (envelope or {}).get("source_evidence"),
            }
            row_hash = _canonical_digest(payload)
            valid = (
                run is not None
                and payload["trade_date"] == run.get("trade_date")
                and payload["pool_level"]
                in {"OBSERVATION", "CONFIRMATION", "TRADABLE"}
                and payload["rank_no"] > 0
                and isinstance(strategies, list)
                and isinstance(reason, dict)
                and isinstance(envelope, dict)
                and envelope.get("schema") == POOL_ROW_EVIDENCE_SCHEMA
                and set(envelope) == {
                    "schema", "source_evidence", "industry_names",
                    "industry_by_strategy", "industry_binding",
                    "pool_row_hash",
                }
                and isinstance(industry_binding, dict)
                and isinstance(industry_names, list)
                and isinstance(industry_by_strategy, dict)
                and str(envelope.get("pool_row_hash") or "") == row_hash
            )
            expected_history = industry_history_bindings.get((
                payload["trade_date"], payload["stock_code"]
            ))
            if expected_history is not None:
                expected_binding = {
                    "schema": INDUSTRY_BINDING_SCHEMA,
                    "snapshot_id": expected_history["snapshot_id"],
                    "snapshot_hash": str(
                        (run or {}).get("summary", {}).get(
                            "candidate_industry_snapshot_hash"
                        ) or ""
                    ),
                    "row_hash": expected_history["row_hash"],
                    "trade_date": expected_history["trade_date"],
                    "as_of_exclusive": expected_history["as_of_exclusive"],
                    "stock_code": expected_history["stock_code"],
                    "industry_name": expected_history["industry_name"],
                    "industry_type": expected_history["industry_type"],
                    "source_system": expected_history["source_system"],
                    "source_fact_id": expected_history["source_fact_id"],
                    "source_effective_at": expected_history[
                        "source_effective_at"
                    ],
                    "source_etl_sync_at": expected_history[
                        "source_etl_sync_at"
                    ],
                }
                valid = bool(
                    valid
                    and industry_binding == expected_binding
                    and payload["industry_name"]
                    == expected_history["industry_name"]
                )
            else:
                valid = bool(
                    valid
                    and payload["pool_level"] == "OBSERVATION"
                    and industry_binding == {}
                    and payload["industry_name"] == ""
                    and "目标日QMT一级行业冻结事实缺失或无效"
                    in (reason.get("blocking_reasons") or [])
                )
        except Exception:
            valid = False
            row_hash = ""
            payload = {
                "pool_level": str(row.get("pool_level") or ""),
                "rank_no": _integer(row.get("rank_no")),
                "stock_code": str(row.get("stock_code") or ""),
            }
        if not valid:
            errors.append({
                "snapshot_type": "POOL",
                "run_uid": run_uid,
                "stock_code": payload.get("stock_code"),
                "reason": "pool row canonical payload hash differs",
            })
        pool_contracts.setdefault(run_uid, []).append({
            "pool_level": payload.get("pool_level"),
            "rank_no": payload.get("rank_no"),
            "stock_code": payload.get("stock_code"),
            "pool_row_hash": row_hash,
        })

    allocation_rows = _rows(
        connection,
        "SELECT run_uid, target_type, target_key, target_version, "
        "funding_gate_hash, market_state, market_match_score, "
        "router_decision_hash, lifecycle_status, lifecycle_status_label, "
        "lifecycle_risk_multiplier, base_competitive_weight_pct, "
        "simulated_weight_pct, member_sleeves_json, member_sleeve_hash, "
        "cash_discount_bp, real_order_authority "
        "FROM st_strategy_allocation_snapshot ORDER BY BINARY run_uid, "
        "BINARY target_type, BINARY target_key",
    )
    allocations_by_run: dict[str, list[dict[str, Any]]] = {}
    legacy_allocation_row_count = 0
    for row in allocation_rows:
        run_uid = str(row.get("run_uid") or "")
        if run_uid in legacy_run_uids:
            legacy_allocation_row_count += 1
            continue
        if run_uid not in runs or _integer(row.get("real_order_authority")) != 0:
            errors.append({
                "snapshot_type": "ALLOCATION",
                "run_uid": run_uid,
                "target_key": row.get("target_key"),
                "reason": "allocation is orphaned or has order authority",
            })
        allocations_by_run.setdefault(run_uid, []).append(row)

    for run_uid, run in runs.items():
        summary = run["summary"]
        if run["trade_date"] not in industry_history_trade_dates:
            errors.append({
                "snapshot_type": "INDUSTRY_HISTORY",
                "run_uid": run_uid,
                "reason": "completed run has no exact-date QMT industry history",
                "trade_date": run["trade_date"],
            })
        entities = {
            (key, version)
            for observed_run, key, version in strategy_windows
            if observed_run == run_uid
        }
        for key, version in entities:
            windows = strategy_windows.get((run_uid, key, version), [])
            if sorted(windows) != list(EXPECTED_WINDOWS):
                errors.append({
                    "snapshot_type": "STRATEGY_HEALTH",
                    "run_uid": run_uid,
                    "entity_key": key,
                    "reason": "strategy windows are incomplete or duplicate",
                    "windows": sorted(windows),
                })
        pool = pool_contracts.get(run_uid, [])
        pool.sort(key=lambda item: (
            str(item.get("pool_level") or ""),
            _integer(item.get("rank_no")),
            str(item.get("stock_code") or ""),
        ))
        pool_hash = _canonical_digest({
            "schema": POOL_SNAPSHOT_SCHEMA,
            "trade_date": run["trade_date"],
            "row_count": len(pool),
            "rows": pool,
        })
        allocations = _stored_allocation_snapshot(
            allocations_by_run.get(run_uid, [])
        )
        allocation_hash = _canonical_digest({
            "schema": "probiga.strategy-allocation-snapshot.v1",
            "allocation_policy_version": str(
                summary.get("allocation_policy_version") or ""
            ),
            "trade_date": run["trade_date"],
            "market_state": run["market_state"],
            "market_risk_cap_pct": float(
                _decimal(summary.get("market_risk_cap_pct")) or 0
            ),
            "trading_gate_passed": summary.get("trading_gate_passed") is True,
            "candidate_set_hash": str(summary.get("candidate_set_hash") or ""),
            "allocations": allocations,
        })
        if (
            _integer(summary.get("strategy_count")) != len(entities)
            or _integer(summary.get("combination_count"))
            != len(set(combination_keys.get(run_uid, [])))
            or _integer(summary.get("pool_row_count")) != len(pool)
            or str(summary.get("pool_snapshot_hash") or "") != pool_hash
            or _integer(summary.get("allocation_count"))
            != sum(row.get("target_type") != "CASH" for row in allocations)
            or str(summary.get("allocation_snapshot_hash") or "")
            != allocation_hash
        ):
            errors.append({
                "snapshot_type": "RUN_SUMMARY",
                "run_uid": run_uid,
                "reason": "run detail snapshots differ from summary hashes/counts",
                "expected_pool_snapshot_hash": pool_hash,
                "expected_allocation_snapshot_hash": allocation_hash,
            })
    return not errors, {
        "production_cutover_date": cutover,
        "legacy_isolation_status": "LEGACY_RESEARCH_ONLY",
        "legacy_completed_run_count": len(legacy_run_uids),
        "legacy_strategy_health_row_count": legacy_strategy_row_count,
        "legacy_combination_health_row_count": legacy_combination_row_count,
        "legacy_pool_row_count": legacy_pool_row_count,
        "legacy_allocation_row_count": legacy_allocation_row_count,
        "completed_run_count": len(runs),
        "strategy_health_row_count": len(strategy_rows) - legacy_strategy_row_count,
        "combination_health_row_count": (
            len(combination_rows) - legacy_combination_row_count
        ),
        "pool_row_count": len(pool_rows) - legacy_pool_row_count,
        "allocation_row_count": (
            len(allocation_rows) - legacy_allocation_row_count
        ),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _metric_evidence_audit_history_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Replay every metric-evidence add/review audit, including non-funded rows."""

    metric_rows = _rows(
        connection,
        "SELECT evidence_id, entity_type, strategy_key, strategy_version, "
        "as_of_date, window_days, metrics_json, source, evidence_protocol, "
        "artifact_hash, source_dataset_hash, evidence_revision_at, "
        "verification_status, funding_provenance, submitted_by, reviewed_by, "
        "reviewed_at, evidence_hash, created_at "
        "FROM st_strategy_metric_input ORDER BY created_at, evidence_id",
    )
    audit_rows = _rows(
        connection,
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit WHERE action IN "
        "('ADD_METRIC_EVIDENCE','CONFIRM_METRIC_EVIDENCE',"
        "'REJECT_METRIC_EVIDENCE') ORDER BY created_at, audit_id",
    )
    from server.engine.strategy_governance import (
        _metric_evidence_audit_index,
        metric_evidence_audit_binding,
    )

    errors: list[dict[str, Any]] = []
    audit_index = _metric_evidence_audit_index(audit_rows)
    rows_by_id: dict[str, dict[str, Any]] = {}
    status_counts = {"PENDING": 0, "CONFIRMED": 0, "REJECTED": 0}
    for row in metric_rows:
        evidence_id = str(row.get("evidence_id") or "")
        if evidence_id in rows_by_id:
            errors.append({
                "record_type": "METRIC_EVIDENCE",
                "record_id": evidence_id,
                "reason": "duplicate evidence identity",
            })
            continue
        rows_by_id[evidence_id] = row
        status = str(row.get("verification_status") or "")
        if status in status_counts:
            status_counts[status] += 1
        valid, detail = metric_evidence_audit_binding(row, audit_index)
        if not valid:
            errors.append({
                "record_type": "METRIC_EVIDENCE",
                "record_id": evidence_id,
                "reason": "submission/review immutable audit binding differs",
                "detail": detail,
            })

    metric_actions = {
        "ADD_METRIC_EVIDENCE",
        "CONFIRM_METRIC_EVIDENCE",
        "REJECT_METRIC_EVIDENCE",
    }
    for audit in audit_rows:
        if str(audit.get("action") or "") not in metric_actions:
            continue
        evidence = _json_object(audit.get("evidence_json"))
        evidence_id = str((evidence or {}).get("evidence_id") or "")
        if not evidence_id or evidence_id not in rows_by_id:
            errors.append({
                "record_type": "METRIC_EVIDENCE_AUDIT",
                "record_id": audit.get("audit_id"),
                "evidence_id": evidence_id,
                "reason": "orphan audit or missing evidence identity",
            })
    return not errors, {
        "evidence_count": len(metric_rows),
        "metric_audit_count": len(audit_rows),
        "status_counts": status_counts,
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


_CHALLENGER_EVIDENCE_SUBMISSION_FIELDS = frozenset({
    "schema", "challenger_id", "proposal_hash", "proposed_version_hash",
    "proposal_submitted_at", "submitted_by", "as_of_date", "window_days",
    "evidence_protocol", "evidence_revision_at", "metrics",
    "artifact_manifest", "artifact_hash", "source_dataset_hash",
    "automatic_real_order_submission", "real_order_authority",
    "server_replay_validation_hash", "evidence_submission_hash",
})


def _health_validated_challenger_evidence_claim(
    row: dict[str, Any],
) -> dict[str, str]:
    """Independently validate a frozen challenger claim and audit envelope."""

    before = _json_object(row.get("before_json"))
    after = _json_object(row.get("after_json"))
    evidence = _json_object(row.get("evidence_json"))
    payload = _json_object(row.get("payload_json"))
    envelope_fields = {
        "entity_type", "entity_key", "action", "reason", "operator",
        "before", "after", "evidence", "nonce",
    }
    envelope_valid = bool(
        isinstance(payload, dict)
        and set(payload) == envelope_fields
        and payload.get("entity_type") == row.get("entity_type")
        and payload.get("entity_key") == row.get("entity_key")
        and payload.get("action") == row.get("action")
        and payload.get("reason") == row.get("reason")
        and payload.get("operator") == row.get("operator_name")
        and payload.get("before") == before
        and payload.get("after") == after
        and payload.get("evidence") == evidence
        and re.fullmatch(
            r"[0-9a-f]{32}", str(payload.get("nonce") or "")
        ) is not None
        and RESULT_HASH_RE.fullmatch(str(row.get("audit_hash") or ""))
        is not None
        and _canonical_digest(payload) == str(row.get("audit_hash") or "")
    )
    if not envelope_valid or not isinstance(evidence, dict):
        raise ValueError("challenger audit envelope hash/column binding differs")

    challenger_id = str(evidence.get("challenger_id") or "")
    artifact_hash = str(evidence.get("artifact_hash") or "")
    source_dataset_hash = str(evidence.get("source_dataset_hash") or "")
    submission_hash = str(evidence.get("evidence_submission_hash") or "")
    manifest = evidence.get("artifact_manifest")
    frozen = {
        str(key): value for key, value in evidence.items()
        if str(key) != "evidence_submission_hash"
    }
    expected_after = {
        "challenger_id": challenger_id,
        "status": "REVIEW_PENDING",
        "evidence_submission_hash": submission_hash,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
    }
    claim_valid = bool(
        set(evidence) == _CHALLENGER_EVIDENCE_SUBMISSION_FIELDS
        and row.get("action") == "SUBMIT_CHALLENGER_EVIDENCE"
        and row.get("entity_type") == "STRATEGY"
        and bool(str(row.get("entity_key") or ""))
        and re.fullmatch(r"[0-9a-f]{32}", challenger_id) is not None
        and before == {"challenger_id": challenger_id, "status": "VALIDATING"}
        and after == expected_after
        and evidence.get("schema")
        == "probiga.strategy-challenger-evidence-submission.v1"
        and evidence.get("submitted_by") == row.get("operator_name")
        and isinstance(evidence.get("metrics"), dict)
        and isinstance(manifest, dict)
        and str(manifest.get("source_dataset_hash") or "").lower()
        == source_dataset_hash
        and RESULT_HASH_RE.fullmatch(artifact_hash) is not None
        and RESULT_HASH_RE.fullmatch(source_dataset_hash) is not None
        and RESULT_HASH_RE.fullmatch(
            str(evidence.get("proposal_hash") or "")
        ) is not None
        and RESULT_HASH_RE.fullmatch(
            str(evidence.get("proposed_version_hash") or "")
        ) is not None
        and RESULT_HASH_RE.fullmatch(
            str(evidence.get("server_replay_validation_hash") or "")
        ) is not None
        and RESULT_HASH_RE.fullmatch(submission_hash) is not None
        and _canonical_digest(frozen) == submission_hash
        and evidence.get("automatic_real_order_submission") is False
        and evidence.get("real_order_authority") is False
    )
    if not claim_valid:
        raise ValueError("challenger frozen evidence submission contract differs")
    return {
        "challenger_id": challenger_id,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
    }


def _global_evidence_claim_uniqueness_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Replay the global artifact/dataset namespace across both ledgers."""

    metric_rows = _rows(
        connection,
        "SELECT evidence_id, artifact_hash, source_dataset_hash "
        "FROM st_strategy_metric_input ORDER BY created_at, evidence_id",
    )
    raw_audits = _rows(
        connection,
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "WHERE action='SUBMIT_CHALLENGER_EVIDENCE' "
        "ORDER BY created_at, audit_id",
    )
    challenger_audits = [
        row for row in raw_audits
        if str(row.get("action") or "") == "SUBMIT_CHALLENGER_EVIDENCE"
    ]
    errors: list[dict[str, Any]] = []
    artifact_owners: dict[str, str] = {}
    dataset_owners: dict[str, str] = {}

    def claim(
        *, namespace: str, claim_hash: str, owner: str,
        ledger: str,
    ) -> None:
        owners = (
            artifact_owners if namespace == "artifact_hash"
            else dataset_owners
        )
        prior = owners.get(claim_hash)
        if prior is not None:
            errors.append({
                "record_type": "GLOBAL_EVIDENCE_CLAIM",
                "namespace": namespace,
                "claim_hash": claim_hash,
                "owner": owner,
                "prior_owner": prior,
                "ledger": ledger,
                "reason": "artifact/dataset claim reused across evidence owners",
            })
            return
        owners[claim_hash] = owner

    for row in metric_rows:
        evidence_id = str(row.get("evidence_id") or "")
        artifact_hash = str(row.get("artifact_hash") or "")
        dataset_hash = str(row.get("source_dataset_hash") or "")
        if (
            re.fullmatch(r"[0-9a-f]{32}", evidence_id) is None
            or RESULT_HASH_RE.fullmatch(artifact_hash) is None
            or RESULT_HASH_RE.fullmatch(dataset_hash) is None
        ):
            errors.append({
                "record_type": "METRIC_EVIDENCE",
                "record_id": evidence_id,
                "reason": "metric global evidence identity/hash is invalid",
            })
            continue
        owner = f"METRIC:{evidence_id}"
        claim(
            namespace="artifact_hash", claim_hash=artifact_hash,
            owner=owner, ledger="st_strategy_metric_input",
        )
        claim(
            namespace="source_dataset_hash", claim_hash=dataset_hash,
            owner=owner, ledger="st_strategy_metric_input",
        )

    valid_challenger_count = 0
    for row in challenger_audits:
        try:
            challenger = _health_validated_challenger_evidence_claim(row)
        except Exception as exc:
            errors.append({
                "record_type": "CHALLENGER_EVIDENCE_AUDIT",
                "record_id": str(row.get("audit_id") or ""),
                "reason": _safe_exception_message(exc),
            })
            continue
        valid_challenger_count += 1
        owner = f"CHALLENGER:{challenger['challenger_id']}"
        claim(
            namespace="artifact_hash",
            claim_hash=challenger["artifact_hash"],
            owner=owner,
            ledger="st_strategy_governance_audit",
        )
        claim(
            namespace="source_dataset_hash",
            claim_hash=challenger["source_dataset_hash"],
            owner=owner,
            ledger="st_strategy_governance_audit",
        )
    return not errors, {
        "metric_claim_count": len(metric_rows),
        "challenger_claim_count": len(challenger_audits),
        "valid_challenger_claim_count": valid_challenger_count,
        "unique_artifact_count": len(artifact_owners),
        "unique_source_dataset_count": len(dataset_owners),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _normalized_datetime_text(value: Any) -> str:
    return str(value or "").strip().replace(" ", "T")


def _validate_metric_artifact_binding(
    connection, evidence: dict[str, Any], reference: dict[str, Any]
) -> str:
    """Reuse the write-path verifier to prove stored evidence is canonical."""

    stored_metrics = _json_object(evidence.get("metrics_json"))
    artifact = _json_object(evidence.get("artifact_json"))
    embedded_metrics = reference.get("embedded_metrics")
    if stored_metrics is None or artifact is None:
        return "stored metrics or artifact is not a JSON object"
    if not isinstance(embedded_metrics, dict):
        return "snapshot metrics are missing"

    metadata_pairs = {
        "evidence_protocol": evidence.get("evidence_protocol"),
        "artifact_hash": evidence.get("artifact_hash"),
        "source_dataset_hash": evidence.get("source_dataset_hash"),
        "selection_evidence_hash": evidence.get("evidence_hash"),
        "selection_validation_revision_at": _normalized_datetime_text(
            evidence.get("evidence_revision_at")
        ),
        "verification_status": evidence.get("verification_status"),
        "submitted_by": evidence.get("submitted_by"),
        "reviewed_by": evidence.get("reviewed_by"),
        "reviewed_at": _normalized_datetime_text(evidence.get("reviewed_at")),
    }
    for key, value in metadata_pairs.items():
        embedded_value = embedded_metrics.get(key)
        if key in {"selection_validation_revision_at", "reviewed_at"}:
            embedded_value = _normalized_datetime_text(embedded_value)
        if embedded_value != value:
            return f"snapshot evidence metadata differs from reviewed row: {key}"

    selection_pairs = {
        "walk_forward_verified": stored_metrics.get(
            "walk_forward_verified"
        )
        is True,
        "walk_forward_segments": _integer(
            stored_metrics.get("walk_forward_segments")
        ),
        "positive_segments": _integer(stored_metrics.get("positive_segments")),
        "selection_validation_completed_trades": _integer(
            stored_metrics.get("completed_trades")
        ),
        "selection_validation_coverage_days": _integer(
            stored_metrics.get("coverage_days")
        ),
        "selection_validation_independent_oos": stored_metrics.get(
            "independent_oos"
        )
        is True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
    }
    for key, value in selection_pairs.items():
        embedded_value = embedded_metrics.get(key)
        if key in {
            "walk_forward_segments",
            "positive_segments",
            "selection_validation_completed_trades",
            "selection_validation_coverage_days",
        }:
            embedded_value = _integer(embedded_value)
        if embedded_value != value:
            return f"snapshot selection metric differs from reviewed row: {key}"

    revision_at = _normalized_datetime_text(evidence.get("evidence_revision_at"))
    pending_payload = {
        "strategy_key": str(evidence.get("strategy_key") or ""),
        "entity_type": str(evidence.get("entity_type") or ""),
        "strategy_version": str(evidence.get("strategy_version") or ""),
        "as_of_date": _iso_date(evidence.get("as_of_date")),
        "window_days": _integer(evidence.get("window_days")),
        "metrics": stored_metrics,
        "source": str(evidence.get("source") or ""),
        "evidence_protocol": str(evidence.get("evidence_protocol") or ""),
        "artifact_hash": str(evidence.get("artifact_hash") or ""),
        "source_dataset_hash": str(
            evidence.get("source_dataset_hash") or ""
        ),
        "evidence_revision_at": revision_at,
        "verification_status": "PENDING",
        "funding_provenance": str(
            evidence.get("funding_provenance") or ""
        ),
    }
    if _canonical_digest(pending_payload) != str(
        evidence.get("evidence_hash") or ""
    ):
        return "evidence hash does not match the immutable submitted payload"

    try:
        from server.engine.strategy_governance import (
            _validate_metric_artifact,
            _validated_metric_evidence,
            _version_label_horizon_days,
            _version_max_holding_days,
        )

        core_metrics = dict(stored_metrics)
        for field in (
            "version_bound_evidence",
            "evidence_protocol",
            "artifact_hash",
            "source_dataset_hash",
            "evidence_revision_at",
            "funding_provenance",
        ):
            core_metrics.pop(field, None)
        validated_metrics = _validated_metric_evidence(core_metrics)
        validated_artifact = _validate_metric_artifact(
            artifact,
            entity_type=str(evidence.get("entity_type") or ""),
            entity_key=str(evidence.get("strategy_key") or ""),
            entity_version=str(evidence.get("strategy_version") or ""),
            as_of_date=_iso_date(evidence.get("as_of_date")),
            window_days=_integer(evidence.get("window_days")),
            evidence_protocol=str(evidence.get("evidence_protocol") or ""),
            evidence_revision_at=revision_at,
            metrics=validated_metrics,
            artifact_hash=str(evidence.get("artifact_hash") or ""),
            version_created_at=str(evidence.get("version_frozen_at") or ""),
            expected_max_holding_days=_version_max_holding_days(
                str(evidence.get("entity_type") or ""),
                str(evidence.get("strategy_key") or ""),
                str(evidence.get("strategy_version") or ""),
                connection=connection,
            ),
            expected_label_horizon_days=_version_label_horizon_days(
                str(evidence.get("entity_type") or ""),
                str(evidence.get("strategy_key") or ""),
                str(evidence.get("strategy_version") or ""),
                connection=connection,
            ),
        )
    except Exception as exc:
        return _safe_exception_message(
            exc, error_code="canonical_artifact_verification_failed"
        )
    if str(validated_artifact.get("source_dataset_hash") or "") != str(
        evidence.get("source_dataset_hash") or ""
    ):
        return "artifact dataset hash differs from evidence row"
    return ""


def _run_audit_check(
    connection, run: dict[str, Any], authoritative_date: str
) -> tuple[bool, dict[str, Any]]:
    rows = _rows(
        connection,
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "WHERE entity_type='SYSTEM' "
        "AND entity_key='strategy_governance_daily' "
        "AND action='RUN_GOVERNANCE' "
        "ORDER BY created_at DESC, audit_id DESC LIMIT 100",
    )
    run_uid = str(run.get("run_uid") or "")
    matching: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        evidence = _json_object(row.get("evidence_json"))
        payload = _json_object(row.get("payload_json"))
        if (
            evidence is not None
            and payload is not None
            and str(evidence.get("run_uid") or "") == run_uid
        ):
            matching.append((row, evidence, payload))
    errors: list[str] = []
    if len(matching) != 1:
        errors.append(f"expected one run audit, found {len(matching)}")
    else:
        row, evidence, payload = matching[0]
        before = _json_object(row.get("before_json"))
        after = _json_object(row.get("after_json"))
        summary = _json_object(run.get("summary_json"))
        valid = (
            before == {}
            and after is not None
            and summary is not None
            and payload.get("entity_type") == "SYSTEM"
            and payload.get("entity_key") == "strategy_governance_daily"
            and payload.get("action") == "RUN_GOVERNANCE"
            and payload.get("reason") == row.get("reason")
            and payload.get("operator") == row.get("operator_name")
            and payload.get("before") == before
            and payload.get("after") == after
            and payload.get("evidence") == evidence
            and bool(
                re.fullmatch(r"[0-9a-f]{32}", str(payload.get("nonce") or ""))
            )
            and _canonical_digest(payload) == str(row.get("audit_hash") or "")
            and after.get("status") == "COMPLETED"
            and _iso_date(after.get("trade_date")) == authoritative_date
            and _integer(after.get("run_revision"))
            == _integer(run.get("run_revision"))
            and str(after.get("supersedes_run_uid") or "")
            == str(run.get("supersedes_run_uid") or "")
            and after.get("is_canonical") is True
            and after.get("summary") == summary
            and _integer(evidence.get("run_revision"))
            == _integer(run.get("run_revision"))
            and str(evidence.get("supersedes_run_uid") or "")
            == str(run.get("supersedes_run_uid") or "")
            and evidence.get("input_hash") == run.get("input_hash")
            and evidence.get("decision_hash") == run.get("decision_hash")
            and evidence.get("build_commit_sha") == run.get("build_commit_sha")
            and evidence.get("router_policy_version")
            == run.get("router_policy_version")
            and evidence.get("router_snapshot_hash")
            == run.get("router_snapshot_hash")
            and evidence.get("automatic_real_order_submission") is False
            and bool(str(row.get("operator_name") or "").strip())
        )
        if not valid:
            errors.append("run audit payload/hash/bindings are invalid")
    return not errors, {
        "candidate_count": len(rows),
        "matching_run_count": len(matching),
        "errors": errors,
    }


def _market_router_config_contract() -> tuple[str, str]:
    from server.common.versioned_strategy_config import (
        load_market_state_config,
        market_state_config_hash,
    )

    config = load_market_state_config()
    return (
        str(config.get("config_version") or ""),
        str(market_state_config_hash() or ""),
    )


def _canonical_window_gate(metrics: Any) -> dict[str, Any] | None:
    if not isinstance(metrics, dict):
        return None
    try:
        from server.engine.strategy_governance import evaluate_window_gate

        gate = evaluate_window_gate(metrics)
    except Exception:
        return None
    return gate if isinstance(gate, dict) else None


def _canonical_compact_gate_binding(
    metrics: Any, stored_gate: Any,
) -> tuple[bool, dict[str, Any]]:
    """Validate the persisted v7 compact window against its full gate.

    The persisted window deliberately omits the raw daily NAV arrays.  A
    health check must therefore not run ``evaluate_window_gate`` against the
    compact display object (doing so treats omitted fields as failures).  The
    full gate is hash-bound in the snapshot and the raw-ledger replay below
    independently rebuilds both the compact window and the full gate.
    """

    if not isinstance(metrics, dict) or not isinstance(stored_gate, dict):
        return False, {}
    compact_gate = metrics.get("profit_gate")
    expected_compact = {
        "passed": stored_gate.get("passed") is True,
        "failed": len(stored_gate.get("failed_checks") or []),
    }
    window = _integer(metrics.get("window_days"))
    detail_ref = metrics.get("detail_ref")
    statistical_guard = metrics.get("statistical_guard")
    forward_guard = metrics.get("internal_forward_stability")
    valid = bool(
        window in EXPECTED_WINDOWS
        and compact_gate == expected_compact
        and type(stored_gate.get("passed")) is bool
        and isinstance(stored_gate.get("checks"), list)
        and isinstance(stored_gate.get("failed_checks"), list)
        and isinstance(statistical_guard, dict)
        and type(statistical_guard.get("valid")) is bool
        and type(statistical_guard.get("passed")) is bool
        and isinstance(forward_guard, dict)
        and type(forward_guard.get("valid")) is bool
        and type(forward_guard.get("passed")) is bool
        and isinstance(metrics.get("point_health_score"), (int, float))
        and not isinstance(metrics.get("point_health_score"), bool)
        and isinstance(metrics.get("health_score"), (int, float))
        and not isinstance(metrics.get("health_score"), bool)
        and isinstance(detail_ref, dict)
        and RESULT_HASH_RE.fullmatch(
            str(detail_ref.get("detail_hash") or "")
        ) is not None
        and RESULT_HASH_RE.fullmatch(
            str(metrics.get("source_root") or "")
        ) is not None
        and (
            window != 60
            or (
                isinstance(metrics.get("selection_validation"), dict)
                and metrics["selection_validation"].get(
                    "funding_authority"
                ) is False
            )
        )
    )
    return valid, expected_compact


def _snapshot_v7_statistical_binding(
    *, payload: dict[str, Any], row: dict[str, Any], entity_type: str,
    entity_key: str, entity_version: str,
) -> tuple[bool, dict[str, Any]]:
    """Replay one persisted v7 family/confirmation/final-gate chain."""

    try:
        from server.engine.strategy_governance import (
            STATISTICAL_DECISION_CONTRACT,
            STATISTICAL_POLICY_HASH,
        )
    except Exception:
        return False, {"reason": "v7 statistical policy import failed"}
    decision = payload.get("statistical_family_decision")
    confirmation = payload.get("confirmation_guard")
    if not isinstance(decision, dict) or not isinstance(confirmation, dict):
        return False, {"reason": "v7 decision or confirmation is missing"}
    decision_hash = str(decision.get("decision_hash") or "")
    confirmation_hash = str(confirmation.get("compact_hash") or "")
    pre_gate_hash = str(
        payload.get("pre_confirmation_funding_gate_hash") or ""
    )
    final_gate_hash = str(payload.get("funding_gate_hash") or "")
    projected_status = str(row.get("recommended_status") or "")
    decision_payload = {
        key: value for key, value in decision.items()
        if key != "decision_hash"
    }
    confirmation_payload = {
        key: value for key, value in confirmation.items()
        if key != "compact_hash"
    }
    final_payload = {
        "schema": "probiga.strategy-final-funding-gate.v1",
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_version": entity_version,
        "pre_confirmation_funding_gate_hash": pre_gate_hash,
        "statistical_family_decision_hash": decision_hash,
        "confirmation_guard_hash": confirmation_hash,
        "confirmation_passed": confirmation.get("passed") is True,
        "projected_status": projected_status,
        "paper_allocation_eligible": payload.get(
            "paper_allocation_eligible"
        ) is True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    valid = bool(
        payload.get("decision_contract_version")
        == STATISTICAL_DECISION_CONTRACT
        and payload.get("statistical_policy_hash")
        == STATISTICAL_POLICY_HASH
        and decision.get("schema")
        == "probiga.strategy-family-by-decision-compact.v1"
        and str(decision.get("entity_type") or "") == entity_type
        and str(decision.get("entity_key") or "") == entity_key
        and str(decision.get("entity_version") or "") == entity_version
        and decision.get("statistical_policy_hash")
        == STATISTICAL_POLICY_HASH
        and type(decision.get("valid")) is bool
        and type(decision.get("passed")) is bool
        and decision.get("automatic_real_order_submission") is False
        and decision.get("real_order_authority") is False
        and RESULT_HASH_RE.fullmatch(decision_hash) is not None
        and _canonical_digest(decision_payload) == decision_hash
        and confirmation.get("schema")
        == "probiga.strategy-spaced-confirmation-compact.v1"
        and confirmation.get("statistical_policy_hash")
        == STATISTICAL_POLICY_HASH
        and type(confirmation.get("valid")) is bool
        and type(confirmation.get("passed")) is bool
        and confirmation.get("automatic_real_order_submission") is False
        and confirmation.get("real_order_authority") is False
        and RESULT_HASH_RE.fullmatch(confirmation_hash) is not None
        and _canonical_digest(confirmation_payload) == confirmation_hash
        and RESULT_HASH_RE.fullmatch(pre_gate_hash) is not None
        and RESULT_HASH_RE.fullmatch(final_gate_hash) is not None
        and projected_status in LIFECYCLE_STATES
        and payload.get("statistical_family_passed")
        is bool(decision.get("valid") is True and decision.get("passed") is True)
        and _canonical_digest(final_payload) == final_gate_hash
        and (
            payload.get("paper_allocation_eligible") is not True
            or (
                payload.get("overall_profit_gate_passed") is True
                and projected_status in {"ACTIVE", "REDUCE"}
                and decision.get("valid") is True
                and decision.get("passed") is True
                and confirmation.get("valid") is True
                and confirmation.get("passed") is True
            )
        )
    )
    return valid, {
        "decision_hash": decision_hash,
        "confirmation_hash": confirmation_hash,
        "pre_confirmation_funding_gate_hash": pre_gate_hash,
        "funding_gate_hash": final_gate_hash,
        "projected_status": projected_status,
        "decision_passed": bool(
            decision.get("valid") is True and decision.get("passed") is True
        ),
        "confirmation_passed": bool(
            confirmation.get("valid") is True
            and confirmation.get("passed") is True
        ),
    }


def _canonical_result_allocation_candidates(
    run: dict[str, Any],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    result = _json_object(run.get("result_json")) or {}
    rows = result.get("allocation_candidate_set")
    if not isinstance(rows, list):
        return {}
    candidates: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        identity = (
            str(item.get("target_type") or ""),
            str(item.get("target_key") or ""),
            str(item.get("target_version") or ""),
        )
        if not all(identity) or identity in candidates:
            return {}
        candidates[identity] = item
    return candidates


def _market_router_binding(
    connection,
    run: dict[str, Any],
    trade_date: str,
    session_window_bindings: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any], dict[tuple[str, str, str], dict[str, Any]]]:
    """Recompute the persisted regime/router proof and funding bindings."""

    strategy_rows = _rows(
        connection,
        "SELECT h.strategy_key, h.strategy_version, h.trade_date, "
        "h.window_days, h.market_match_score, h.health_score, "
        "h.profit_gate_passed, h.recommended_status, h.evidence_json, "
        "h.result_hash, v.version_hash, v.evaluator_type, "
        "v.evaluator_config_json, v.parameters_json, v.source_kind, "
        "r.strategy_name AS registry_name, "
        "r.current_version AS registry_current_version, "
        "r.current_status AS registry_current_status, "
        "r.enabled AS registry_enabled "
        "FROM st_strategy_health_snapshot h "
        "LEFT JOIN st_strategy_version v "
        "ON v.strategy_key=h.strategy_key "
        "AND v.version=h.strategy_version "
        "LEFT JOIN st_strategy_registry r "
        "ON r.strategy_key=h.strategy_key "
        "AND r.current_version=h.strategy_version "
        "WHERE h.run_uid=:run_uid "
        "ORDER BY h.strategy_key, h.strategy_version, h.window_days",
        {"run_uid": run.get("run_uid")},
    )
    combination_rows = _rows(
        connection,
        "SELECT h.combination_key, h.combination_version, h.trade_date, "
        "h.ranking_score, h.profit_gate_passed, h.recommended_status, "
        "h.evidence_json, "
        "h.result_hash, v.members_json, "
        "v.constraints_json, v.config_hash, "
        "c.combination_name AS registry_name, "
        "c.current_version AS registry_current_version, "
        "c.current_status AS registry_current_status, "
        "c.enabled AS registry_enabled "
        "FROM st_strategy_combination_health_snapshot h "
        "LEFT JOIN st_strategy_combination_version v "
        "ON v.combination_key=h.combination_key "
        "AND v.version=h.combination_version "
        "LEFT JOIN st_strategy_combination c "
        "ON c.combination_key=h.combination_key "
        "AND c.current_version=h.combination_version "
        "WHERE h.run_uid=:run_uid "
        "ORDER BY h.combination_key, h.combination_version",
        {"run_uid": run.get("run_uid")},
    )
    errors: list[dict[str, Any]] = []
    session_window_bindings = session_window_bindings or {}
    result_candidates = _canonical_result_allocation_candidates(run)

    def fail(entity_type: str, entity_key: str, reason: str) -> None:
        if len(errors) < 100:
            errors.append(
                {
                    "entity_type": entity_type,
                    "entity_key": entity_key,
                    "reason": reason,
                }
            )

    try:
        config_version, config_hash = _market_router_config_contract()
    except Exception as exc:
        config_version, config_hash = "", ""
        fail(
            "SYSTEM",
            "market_router",
            _safe_exception_message(
                exc, error_code="current_market_config_verification_failed"
            ),
        )
    if not config_version or not RESULT_HASH_RE.fullmatch(config_hash):
        fail("SYSTEM", "market_router", "current market config contract is invalid")

    run_state = str(run.get("market_state") or "")
    if run_state not in MARKET_REGIME_STATES:
        fail("SYSTEM", "market_router", "run market_state is unsupported")
    if str(run.get("router_policy_version") or "") != ROUTER_POLICY_VERSION:
        fail("SYSTEM", "market_router", "run router policy version differs")
    if not RESULT_HASH_RE.fullmatch(
        str(run.get("router_snapshot_hash") or "")
    ):
        fail("SYSTEM", "market_router", "run router snapshot hash is invalid")

    strategy_route_keys = {
        "schema",
        "policy_version",
        "strategy_key",
        "strategy_version",
        "trade_date",
        "data_date",
        "market_state",
        "market_state_config_hash",
        "route_source",
        "source_binding",
        "multiplier",
        "market_match_score",
        "eligible",
        "reason",
        "router_decision_hash",
    }
    combination_route_keys = {
        "schema",
        "policy_version",
        "combination_key",
        "combination_version",
        "trade_date",
        "market_state",
        "member_route_hashes",
        "market_match_score",
        "eligible",
        "reason",
        "router_decision_hash",
    }
    grouped_strategies: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in strategy_rows:
        key = str(row.get("strategy_key") or "")
        version = str(row.get("strategy_version") or "")
        payload = _json_object(row.get("evidence_json"))
        route = (
            payload.get("market_route")
            if isinstance(payload, dict)
            and isinstance(payload.get("market_route"), dict)
            else None
        )
        valid_payload = (
            payload is not None
            and _canonical_digest(payload) == str(row.get("result_hash") or "")
            and str(payload.get("strategy_key") or "") == key
            and str(payload.get("strategy_version") or "") == version
            and _iso_date(payload.get("trade_date")) == trade_date
            and _iso_date(row.get("trade_date")) == trade_date
            and _integer(payload.get("window_days"))
            == _integer(row.get("window_days"))
        )
        if not valid_payload or route is None:
            fail("STRATEGY", key, "snapshot payload/result hash/route is invalid")
            continue
        registry_status = str(row.get("registry_current_status") or "")
        registry_valid = (
            bool(str(row.get("registry_name") or "").strip())
            and str(row.get("registry_current_version") or "") == version
            and registry_status in LIFECYCLE_STATES
            and row.get("registry_enabled") is not None
            and _integer(row.get("registry_enabled")) in {0, 1}
        )
        if not registry_valid:
            fail(
                "STRATEGY",
                key,
                "current strategy registry binding is invalid",
            )
        metrics = payload.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        stored_gate = payload.get("gate")
        compact_gate_valid, compact_gate = _canonical_compact_gate_binding(
            metrics, stored_gate,
        )
        if not compact_gate_valid or (
            (_integer(row.get("profit_gate_passed")) == 1)
            != (stored_gate.get("passed") is True)
        ):
            fail(
                "STRATEGY",
                key,
                "persisted v7 compact window/full gate binding differs",
            )
        payload_health_score = _decimal(metrics.get("health_score"))
        stored_health_score = _decimal(row.get("health_score"))
        if (
            payload_health_score is None
            or stored_health_score != payload_health_score
        ):
            fail(
                "STRATEGY",
                key,
                "health score column differs from hash-bound snapshot metrics",
            )
        route_hash = str(route.get("router_decision_hash") or "")
        route_payload = {
            name: value
            for name, value in route.items()
            if name != "router_decision_hash"
        }
        score = _decimal(route.get("market_match_score"))
        row_score = _decimal(row.get("market_match_score"))
        metrics = payload.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        route_valid = (
            set(route) == strategy_route_keys
            and route.get("schema") == "probiga.strategy-market-route.v1"
            and route.get("policy_version") == ROUTER_POLICY_VERSION
            and str(route.get("strategy_key") or "") == key
            and str(route.get("strategy_version") or "") == version
            and _iso_date(route.get("trade_date")) == trade_date
            and _iso_date(route.get("data_date")) == trade_date
            and str(route.get("market_state") or "") == run_state
            and str(route.get("market_state_config_hash") or "")
            == config_hash
            and isinstance(route.get("source_binding"), dict)
            and type(route.get("eligible")) is bool
            and score is not None
            and Decimal("0") <= score <= Decimal("100")
            and row_score == score
            and _decimal(metrics.get("market_match_score")) == score
            and RESULT_HASH_RE.fullmatch(route_hash) is not None
            and _canonical_digest(route_payload) == route_hash
        )
        if not route_valid:
            fail("STRATEGY", key, "strategy market route binding is invalid")

        evaluator = _json_object(row.get("evaluator_config_json")) or {}
        parameters = _json_object(row.get("parameters_json"))
        expected_version_hash = (
            _canonical_digest(
                {
                    "schema": "probiga.strategy-version.v1",
                    "strategy_key": key,
                    "version": version,
                    "evaluator_type": str(row.get("evaluator_type") or ""),
                    "evaluator_config": evaluator,
                    "parameters": parameters,
                    "source_kind": str(row.get("source_kind") or ""),
                }
            )
            if parameters is not None
            else ""
        )
        if expected_version_hash != str(row.get("version_hash") or ""):
            fail("STRATEGY", key, "immutable strategy version hash differs")
        binding = route.get("source_binding")
        binding = binding if isinstance(binding, dict) else {}
        multiplier = _decimal(route.get("multiplier"))
        if route.get("route_source") == "immutable_strategy_version":
            policy = binding.get("policy")
            policy = policy if isinstance(policy, dict) else {}
            policy_multiplier = _decimal(policy.get(run_state))
            expected_score = (
                min(Decimal("100"), max(Decimal("0"), policy_multiplier * 100))
                if policy_multiplier is not None
                else None
            )
            immutable_binding_valid = (
                str(binding.get("version_hash") or "")
                == str(row.get("version_hash") or "")
                and RESULT_HASH_RE.fullmatch(
                    str(row.get("version_hash") or "")
                )
                is not None
                and binding.get("policy_version") == ROUTER_POLICY_VERSION
                and evaluator.get("market_router_policy_version")
                == ROUTER_POLICY_VERSION
                and policy == evaluator.get("market_regime_multipliers")
                and binding.get("market_state_config_version")
                == config_version
                and evaluator.get("market_state_config_version")
                == config_version
                and binding.get("market_state_config_hash") == config_hash
                and evaluator.get("market_state_config_hash") == config_hash
                and set(policy) == set(MARKET_REGIME_STATES)
                and multiplier == policy_multiplier
                and score == expected_score
            )
            if not immutable_binding_valid:
                fail("STRATEGY", key, "immutable strategy route source is invalid")
        elif route.get("eligible") is True:
            fail(
                "STRATEGY",
                key,
                "eligible route is not bound to the immutable strategy version",
            )
        grouped_strategies.setdefault((key, version), []).append(
            {
                "row": row,
                "payload": payload,
                "route": route,
                "health_score": payload_health_score,
                "canonical_gate": compact_gate,
                "stored_gate": stored_gate,
            }
        )

    route_bindings: dict[tuple[str, str, str], dict[str, Any]] = {}
    strategy_routes: dict[str, str] = {}
    for (key, version), items in grouped_strategies.items():
        windows = {_integer(item["row"].get("window_days")) for item in items}
        routes = [item["route"] for item in items]
        payloads = [item["payload"] for item in items]
        hashes = {str(route.get("router_decision_hash") or "") for route in routes}
        gate_hashes = {
            str(payload.get("funding_gate_hash") or "") for payload in payloads
        }
        overall_values = [
            payload.get("overall_profit_gate_passed") for payload in payloads
        ]
        paper_values = [
            payload.get("paper_allocation_eligible") for payload in payloads
        ]
        booleans_consistent = (
            bool(overall_values)
            and bool(paper_values)
            and all(type(value) is bool for value in overall_values + paper_values)
            and all(value is overall_values[0] for value in overall_values)
            and all(value is paper_values[0] for value in paper_values)
        )
        if (
            len(items) != len(EXPECTED_WINDOWS)
            or windows != set(EXPECTED_WINDOWS)
            or len(hashes) != 1
            or len(gate_hashes) != 1
            or not all(route == routes[0] for route in routes)
            or not booleans_consistent
        ):
            fail("STRATEGY", key, "three-window route/funding binding differs")
            continue
        by_window = {
            _integer(item["row"].get("window_days")): item["payload"]
            for item in items
        }
        canonical_gates = {
            _integer(item["row"].get("window_days")): item.get(
                "canonical_gate"
            )
            for item in items
        }
        canonical_windows_valid = (
            set(canonical_gates) == set(EXPECTED_WINDOWS)
            and all(
                isinstance(canonical_gates.get(window), dict)
                for window in EXPECTED_WINDOWS
            )
        )
        statistical_bindings = [
            _snapshot_v7_statistical_binding(
                payload=item["payload"], row=item["row"],
                entity_type="STRATEGY", entity_key=key,
                entity_version=version,
            )
            for item in items
        ]
        statistical_contract_valid = bool(
            statistical_bindings
            and all(valid for valid, _detail in statistical_bindings)
            and all(
                detail == statistical_bindings[0][1]
                for _valid, detail in statistical_bindings
            )
        )
        if not statistical_contract_valid:
            fail(
                "STRATEGY", key,
                "v7 family/confirmation/final funding binding differs",
            )
        statistical_detail = statistical_bindings[0][1]
        overall_prerequisites = bool(
            canonical_windows_valid
            and _integer(items[0]["row"].get("registry_enabled")) == 1
            and all(
                canonical_gates[window].get("passed") is True
                for window in EXPECTED_WINDOWS
            )
            and statistical_detail.get("decision_passed") is True
        )
        expected_overall_gate = bool(overall_values[0])
        expected_paper_eligible = bool(
            expected_overall_gate
            and str(items[0]["row"].get("recommended_status") or "")
            in {"ACTIVE", "REDUCE"}
            and routes[0].get("eligible") is True
            and statistical_detail.get("confirmation_passed") is True
        )
        if (
            not canonical_windows_valid
            or (expected_overall_gate and not overall_prerequisites)
            or any(value is not expected_overall_gate for value in overall_values)
            or any(value is not expected_paper_eligible for value in paper_values)
        ):
            fail(
                "STRATEGY",
                key,
                "strategy overall gate differs from canonical three-window gates",
            )
        health_scores = {
            _integer(item["row"].get("window_days")): item["health_score"]
            for item in items
            if item["health_score"] is not None
        }
        ranking_score = None
        if set(health_scores) == set(EXPECTED_WINDOWS):
            ranking_score = Decimal(
                str(
                    round(
                        float(health_scores[20]) * 0.25
                        + float(health_scores[60]) * 0.50
                        + float(health_scores[120]) * 0.25,
                        2,
                    )
                )
            )
        else:
            fail("STRATEGY", key, "hash-bound health scores are incomplete")
        funding_gate_hash = next(iter(gate_hashes))
        if not RESULT_HASH_RE.fullmatch(funding_gate_hash):
            fail("STRATEGY", key, "strategy funding gate hash is not reproducible")
        canonical_candidate = result_candidates.get(
            ("STRATEGY", key, version), {}
        )
        route = routes[0]
        strategy_routes[key] = str(route.get("router_decision_hash") or "")
        route_bindings[("STRATEGY", key, version)] = {
            "router_decision_hash": strategy_routes[key],
            "market_match_score": _decimal(route.get("market_match_score")),
            "market_state": str(route.get("market_state") or ""),
            "eligible": route.get("eligible") is True,
            "paper_allocation_eligible": expected_paper_eligible,
            "funding_gate_hash": funding_gate_hash,
            "members": frozenset({key}),
            "ranking_score": ranking_score,
            "target_name": str(items[0]["row"].get("registry_name") or ""),
            "enabled": bool(
                _integer(items[0]["row"].get("registry_enabled"))
            ),
            "lifecycle_status": str(
                items[0]["row"].get("registry_current_status") or ""
            ),
            "profit_gate_passed": expected_overall_gate,
            "constraint_passed": True,
            "portfolio_risk_metrics": {},
            "portfolio_risk_evidence": canonical_candidate.get(
                "portfolio_risk_evidence"
            ),
            "pre_confirmation_funding_gate_hash": statistical_detail.get(
                "pre_confirmation_funding_gate_hash"
            ),
            "statistical_family_decision_hash": statistical_detail.get(
                "decision_hash"
            ),
            "confirmation_guard_hash": statistical_detail.get(
                "confirmation_hash"
            ),
            "statistical_confirmation_passed": statistical_detail.get(
                "confirmation_passed"
            ) is True,
        }

    combination_routes: dict[str, str] = {}
    current_strategy_bindings: dict[str, tuple[str, dict[str, Any]]] = {}
    for (entity_type, strategy_key, version), binding in route_bindings.items():
        if entity_type != "STRATEGY":
            continue
        if strategy_key in current_strategy_bindings:
            fail(
                "STRATEGY",
                strategy_key,
                "multiple current strategy snapshot bindings exist",
            )
            continue
        current_strategy_bindings[strategy_key] = (version, binding)
    for row in combination_rows:
        key = str(row.get("combination_key") or "")
        version = str(row.get("combination_version") or "")
        payload = _json_object(row.get("evidence_json"))
        route = (
            payload.get("market_route")
            if isinstance(payload, dict)
            and isinstance(payload.get("market_route"), dict)
            else None
        )
        members_list = _json_array(row.get("members_json"))
        constraints = _json_object(row.get("constraints_json"))
        if not isinstance(members_list, list):
            fail("COMBINATION", key, "current combination members are invalid")
            members_list = []
        config_hash_valid = bool(
            constraints is not None
            and _canonical_digest(
                {"members": members_list, "constraints": constraints}
            )
            == str(row.get("config_hash") or "")
        )
        if not config_hash_valid:
            fail("COMBINATION", key, "immutable combination config hash differs")
        valid_payload = (
            payload is not None
            and _canonical_digest(payload) == str(row.get("result_hash") or "")
            and str(payload.get("combination_key") or "") == key
            and str(payload.get("combination_version") or "") == version
            and _iso_date(payload.get("trade_date")) == trade_date
            and _iso_date(row.get("trade_date")) == trade_date
        )
        if not valid_payload or route is None:
            fail("COMBINATION", key, "snapshot payload/result hash/route is invalid")
            continue
        registry_status = str(row.get("registry_current_status") or "")
        if not (
            bool(str(row.get("registry_name") or "").strip())
            and str(row.get("registry_current_version") or "") == version
            and registry_status in LIFECYCLE_STATES
            and row.get("registry_enabled") is not None
            and _integer(row.get("registry_enabled")) in {0, 1}
        ):
            fail(
                "COMBINATION",
                key,
                "current combination registry binding is invalid",
            )
        metrics = payload.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        canonical_combo_gates: dict[int, dict[str, Any] | None] = {}
        combination_health_scores = {
            window: _decimal(
                (metrics.get(str(window)) or {}).get("health_score")
            )
            for window in EXPECTED_WINDOWS
            if isinstance(metrics.get(str(window)), dict)
        }
        stored_multi_window_gate = payload.get("multi_window_gate")
        stored_multi_window_gate = (
            stored_multi_window_gate
            if isinstance(stored_multi_window_gate, dict) else {}
        )
        for expected_window in EXPECTED_WINDOWS:
            window_metrics = metrics.get(str(expected_window))
            window_metrics = (
                window_metrics if isinstance(window_metrics, dict) else {}
            )
            stored_gate = stored_multi_window_gate.get(str(expected_window))
            compact_valid, compact_gate = _canonical_compact_gate_binding(
                window_metrics, stored_gate,
            )
            canonical_combo_gates[expected_window] = compact_gate
            if not compact_valid:
                fail(
                    "COMBINATION",
                    key,
                    "combination v7 compact window/full gate binding differs",
                )
        expected_multi_window_gate = {
            str(window): stored_multi_window_gate.get(str(window))
            for window in EXPECTED_WINDOWS
        }
        if (
            any(
                not isinstance(canonical_combo_gates.get(window), dict)
                for window in EXPECTED_WINDOWS
            )
            or stored_multi_window_gate != expected_multi_window_gate
        ):
            fail(
                "COMBINATION",
                key,
                "combination multi-window gate is not canonical",
            )
        member_specs = [
            {
                "strategy_key": str(item.get("strategy_key") or ""),
                "strategy_version": str(item.get("strategy_version") or ""),
                "weight": _decimal(item.get("weight")),
            }
            for item in members_list
            if isinstance(item, dict)
        ]
        member_weights = [item["weight"] for item in member_specs]
        has_independent_evidence = all(
            isinstance(metrics.get(str(window)), dict)
            and isinstance(
                metrics[str(window)].get("statistical_guard"), dict
            )
            and isinstance(
                metrics[str(window)].get("internal_forward_stability"),
                dict,
            )
            and RESULT_HASH_RE.fullmatch(
                str(metrics[str(window)].get("source_root") or "")
            )
            is not None
            for window in EXPECTED_WINDOWS
        )
        member_ranking_values: list[tuple[Decimal, Decimal]] = []
        for member in member_specs:
            current_binding = current_strategy_bindings.get(
                member["strategy_key"]
            )
            binding = current_binding[1] if current_binding else None
            member_ranking = (binding or {}).get("ranking_score")
            if (
                isinstance(member_ranking, Decimal)
                and isinstance(member["weight"], Decimal)
            ):
                member_ranking_values.append(
                    (member["weight"], member_ranking)
                )
        ranking_score = None
        if (
            set(combination_health_scores) == set(EXPECTED_WINDOWS)
            and all(
                value is not None
                for value in combination_health_scores.values()
            )
            and member_weights
            and all(value is not None and value >= 0 for value in member_weights)
            and sum(member_weights, Decimal("0")) > 0
            and len(member_ranking_values) == len(member_specs)
        ):
            independent_health = round(
                float(combination_health_scores[20]) * 0.25
                + float(combination_health_scores[60]) * 0.50
                + float(combination_health_scores[120]) * 0.25,
                2,
            )
            member_total = sum(member_weights, Decimal("0"))
            concentration = max(
                float(value / member_total) for value in member_weights
            )
            member_score = sum(
                float(weight) * float(member_ranking)
                for weight, member_ranking in member_ranking_values
            ) / float(member_total)
            base_score = (
                independent_health
                if has_independent_evidence
                else member_score
            )
            ranking_score = Decimal(
                str(
                    round(
                        max(
                            0.0,
                            base_score
                            - max(0.0, concentration - 0.5) * 20.0,
                        ),
                        2,
                    )
                )
            )
        if (
            payload.get("paper_allocation_eligible") is True
            and not has_independent_evidence
        ):
            fail(
                "COMBINATION",
                key,
                "paper-eligible combination lacks independent evidence",
            )
        stored_ranking_score = _decimal(row.get("ranking_score"))
        if ranking_score is None or stored_ranking_score != ranking_score:
            fail(
                "COMBINATION",
                key,
                "ranking score column differs from hash-bound snapshot inputs",
            )
        member_keys = {
            str(item.get("strategy_key") or "")
            for item in members_list
            if isinstance(item, dict) and str(item.get("strategy_key") or "")
        }
        expected_member_hashes = {
            member_key: strategy_routes.get(member_key, "")
            for member_key in sorted(member_keys)
        }
        member_hashes = route.get("member_route_hashes")
        member_hashes = member_hashes if isinstance(member_hashes, dict) else {}
        score = _decimal(route.get("market_match_score"))
        member_scores = [
            binding.get("market_match_score")
            for (entity_type, strategy_key, _strategy_version), binding
            in route_bindings.items()
            if entity_type == "STRATEGY" and strategy_key in member_keys
            and isinstance(binding.get("market_match_score"), Decimal)
        ]
        expected_score = min(member_scores) if member_scores else Decimal("0")
        route_hash = str(route.get("router_decision_hash") or "")
        route_payload = {
            name: value
            for name, value in route.items()
            if name != "router_decision_hash"
        }
        member_details = payload.get("member_details")
        member_details = member_details if isinstance(member_details, list) else []
        constraint_evaluation = payload.get("constraint_evaluation")
        constraint_evaluation = (
            constraint_evaluation
            if isinstance(constraint_evaluation, dict)
            else {}
        )
        constraint_evaluation_hash = str(
            constraint_evaluation.get("evaluation_hash") or ""
        )
        constraint_payload = {
            name: value
            for name, value in constraint_evaluation.items()
            if name not in {"passed", "evaluation_hash"}
        }
        constraint_valid = (
            type(constraint_evaluation.get("passed")) is bool
            and RESULT_HASH_RE.fullmatch(constraint_evaluation_hash)
            is not None
            and _canonical_digest(constraint_payload)
            == constraint_evaluation_hash
            and (
                payload.get("paper_allocation_eligible") is not True
                or constraint_evaluation.get("passed") is True
            )
        )
        if not constraint_valid:
            fail(
                "COMBINATION",
                key,
                "combination constraint evaluation hash/status is invalid",
            )
        expected_member_detail_fields = {
            "strategy_key",
            "strategy_name",
            "strategy_version",
            "current_strategy_version",
            "version_match",
            "weight",
            "status_label",
            "lifecycle_status",
            "lifecycle_risk_multiplier",
            "effective_weight_after_lifecycle",
            "contribution_score",
        }
        detail_by_key = {
            str(item.get("strategy_key") or ""): item
            for item in member_details
            if isinstance(item, dict)
        }
        member_total = sum(
            (
                member["weight"]
                for member in member_specs
                if isinstance(member["weight"], Decimal)
            ),
            Decimal("0"),
        )
        member_detail_valid = (
            len(member_details) == len(member_specs)
            and len(detail_by_key) == len(member_specs)
            and member_total > 0
        )
        frozen_versions: dict[str, dict[str, Any]] = {}
        version_mismatch_present = False
        member_sleeve_risk_multiplier = Decimal("0")
        for member in member_specs:
            member_key = member["strategy_key"]
            detail = detail_by_key.get(member_key)
            current_binding = current_strategy_bindings.get(member_key)
            current_version = current_binding[0] if current_binding else ""
            frozen_version = member["strategy_version"]
            version_match = bool(
                frozen_version and frozen_version == current_version
            )
            version_mismatch_present = (
                version_mismatch_present or not version_match
            )
            ranking = (
                current_binding[1].get("ranking_score")
                if current_binding
                else None
            )
            normalized_weight = (
                member["weight"] / member_total
                if isinstance(member["weight"], Decimal)
                and member_total > 0
                else None
            )
            member_status = str(
                (current_binding or ("", {}))[1].get("lifecycle_status")
                or ""
            )
            member_multiplier = LIFECYCLE_RISK_MULTIPLIER.get(
                member_status, Decimal("0")
            )
            if normalized_weight is not None:
                member_sleeve_risk_multiplier += (
                    normalized_weight * member_multiplier
                )
            frozen_versions[member_key] = {
                "frozen": frozen_version,
                "current": current_version,
                "lifecycle_status": member_status,
                "lifecycle_risk_multiplier": float(member_multiplier),
            }
            expected_contribution = (
                Decimal(
                    str(round(float(normalized_weight * ranking), 2))
                )
                if isinstance(normalized_weight, Decimal)
                and isinstance(ranking, Decimal)
                else None
            )
            if not (
                isinstance(detail, dict)
                and set(detail) == expected_member_detail_fields
                and str(detail.get("strategy_version") or "")
                == frozen_version
                and str(detail.get("current_strategy_version") or "")
                == current_version
                and type(detail.get("version_match")) is bool
                and detail.get("version_match") is version_match
                and bool(str(detail.get("strategy_name") or "").strip())
                and str(detail.get("status_label") or "")
                == LIFECYCLE_LABELS.get(member_status, "未知状态")
                and str(detail.get("lifecycle_status") or "")
                == member_status
                and _decimal(detail.get("lifecycle_risk_multiplier"))
                == member_multiplier
                and _decimal(detail.get("weight"))
                == (
                    Decimal(str(round(float(normalized_weight), 8)))
                    if normalized_weight is not None
                    else None
                )
                and _decimal(detail.get("effective_weight_after_lifecycle"))
                == (
                    Decimal(str(round(
                        float(normalized_weight * member_multiplier), 6
                    )))
                    if normalized_weight is not None
                    else None
                )
                and _decimal(detail.get("contribution_score"))
                == expected_contribution
            ):
                member_detail_valid = False
        if (
            not member_detail_valid
            or version_mismatch_present
            and payload.get("paper_allocation_eligible") is True
        ):
            fail(
                "COMBINATION",
                key,
                "combination member version/detail binding is invalid",
            )
        member_gate_passed = bool(
            member_specs
            and member_detail_valid
            and not version_mismatch_present
            and all(
                current_strategy_bindings.get(member["strategy_key"])
                is not None
                and current_strategy_bindings[member["strategy_key"]][1].get(
                    "profit_gate_passed"
                )
                is True
                and current_strategy_bindings[member["strategy_key"]][1].get(
                    "lifecycle_status"
                )
                in {"ACTIVE", "REDUCE"}
                for member in member_specs
            )
        )
        canonical_windows_passed = all(
            isinstance(canonical_combo_gates.get(window), dict)
            and canonical_combo_gates[window].get("passed") is True
            for window in EXPECTED_WINDOWS
        )
        statistical_contract_valid, statistical_detail = (
            _snapshot_v7_statistical_binding(
                payload=payload, row=row, entity_type="COMBINATION",
                entity_key=key, entity_version=version,
            )
        )
        if not statistical_contract_valid:
            fail(
                "COMBINATION", key,
                "v7 family/confirmation/final funding binding differs",
            )
        overall_prerequisites = bool(
            _integer(row.get("registry_enabled")) == 1
            and has_independent_evidence
            and canonical_windows_passed
            and member_gate_passed
            and constraint_valid
            and constraint_evaluation.get("passed") is True
            and config_hash_valid
            and statistical_detail.get("decision_passed") is True
        )
        expected_overall_gate = bool(
            payload.get("overall_profit_gate_passed") is True
        )
        expected_paper_eligible = bool(
            expected_overall_gate
            and str(row.get("recommended_status") or "")
            in {"ACTIVE", "REDUCE"}
            and route.get("eligible") is True
            and constraint_evaluation.get("passed") is True
            and statistical_detail.get("confirmation_passed") is True
            and isinstance(payload.get("combination_recipe_ref"), dict)
            and payload["combination_recipe_ref"].get(
                "member_fact_sets_ready"
            ) is True
        )
        if (
            (expected_overall_gate and not overall_prerequisites)
            or
            payload.get("overall_profit_gate_passed")
            is not expected_overall_gate
            or (_integer(row.get("profit_gate_passed")) == 1)
            != expected_overall_gate
            or payload.get("paper_allocation_eligible")
            is not expected_paper_eligible
        ):
            fail(
                "COMBINATION",
                key,
                "combination overall gate differs from canonical windows",
            )
        route_valid = (
            set(route) == combination_route_keys
            and route.get("schema") == "probiga.combination-market-route.v1"
            and route.get("policy_version") == ROUTER_POLICY_VERSION
            and str(route.get("combination_key") or "") == key
            and str(route.get("combination_version") or "") == version
            and _iso_date(route.get("trade_date")) == trade_date
            and str(route.get("market_state") or "") == run_state
            and member_keys
            and member_hashes == expected_member_hashes
            and all(
                RESULT_HASH_RE.fullmatch(str(value or ""))
                for value in member_hashes.values()
            )
            and score == expected_score
            and type(route.get("eligible")) is bool
            and RESULT_HASH_RE.fullmatch(route_hash) is not None
            and _canonical_digest(route_payload) == route_hash
        )
        if not route_valid:
            fail("COMBINATION", key, "combination market route binding is invalid")
        funding_gate_hash = str(payload.get("funding_gate_hash") or "")
        if not RESULT_HASH_RE.fullmatch(funding_gate_hash):
            fail(
                "COMBINATION",
                key,
                "combination funding gate hash is not reproducible",
            )
        canonical_candidate = result_candidates.get(
            ("COMBINATION", key, version), {}
        )
        combination_routes[key] = route_hash
        route_bindings[("COMBINATION", key, version)] = {
            "router_decision_hash": route_hash,
            "market_match_score": score,
            "market_state": str(route.get("market_state") or ""),
            "eligible": route.get("eligible") is True,
            "paper_allocation_eligible": expected_paper_eligible,
            "funding_gate_hash": funding_gate_hash,
            "members": frozenset(member_keys),
            "ranking_score": ranking_score,
            "target_name": str(row.get("registry_name") or ""),
            "enabled": bool(_integer(row.get("registry_enabled"))),
            "lifecycle_status": registry_status,
            "profit_gate_passed": expected_overall_gate,
            "constraint_passed": constraint_evaluation.get("passed")
            is True,
            "member_sleeve_risk_multiplier": Decimal(str(round(
                float(member_sleeve_risk_multiplier),
                8,
            ))),
            "member_sleeves_source": [
                {
                    "strategy_key": str(item.get("strategy_key") or ""),
                    "strategy_version": str(
                        item.get("strategy_version") or ""
                    ),
                    "current_strategy_version": str(
                        item.get("current_strategy_version") or ""
                    ),
                    "version_match": item.get("version_match") is True,
                    "original_weight": round(
                        float(_decimal(item.get("weight")) or 0), 8
                    ),
                    "member_lifecycle_status": str(
                        item.get("lifecycle_status") or ""
                    ),
                    "member_lifecycle_multiplier": round(
                        float(
                            _decimal(item.get("lifecycle_risk_multiplier"))
                            or 0
                        ),
                        8,
                    ),
                }
                for item in member_details
            ],
            "portfolio_risk_metrics": {},
            "portfolio_risk_evidence": canonical_candidate.get(
                "portfolio_risk_evidence"
            ),
            "pre_confirmation_funding_gate_hash": statistical_detail.get(
                "pre_confirmation_funding_gate_hash"
            ),
            "statistical_family_decision_hash": statistical_detail.get(
                "decision_hash"
            ),
            "confirmation_guard_hash": statistical_detail.get(
                "confirmation_hash"
            ),
            "statistical_confirmation_passed": statistical_detail.get(
                "confirmation_passed"
            ) is True,
        }

    snapshot_payload = {
        "schema": "probiga.strategy-market-router-snapshot.v1",
        "policy_version": ROUTER_POLICY_VERSION,
        "trade_date": trade_date,
        "market_state": run_state,
        "market_state_config_hash": config_hash,
        "strategy_routes": {
            key: strategy_routes[key] for key in sorted(strategy_routes)
        },
        "combination_routes": {
            key: combination_routes[key] for key in sorted(combination_routes)
        },
    }
    expected_snapshot_hash = _canonical_digest(snapshot_payload)
    summary = _json_object(run.get("summary_json"))
    summary = summary if isinstance(summary, dict) else {}
    snapshot_valid = (
        len(strategy_routes) == _integer(run.get("strategy_count"))
        and len(combination_routes) == _integer(run.get("combination_count"))
        and len(strategy_rows)
        == _integer(run.get("strategy_count")) * len(EXPECTED_WINDOWS)
        and len(combination_rows) == _integer(run.get("combination_count"))
        and expected_snapshot_hash
        == str(run.get("router_snapshot_hash") or "")
        and summary.get("router_policy_version") == ROUTER_POLICY_VERSION
        and summary.get("router_snapshot_hash") == expected_snapshot_hash
        and _integer(summary.get("strategy_route_eligible_count"))
        == sum(
            1
            for (kind, _key, _version), binding in route_bindings.items()
            if kind == "STRATEGY" and binding.get("eligible") is True
        )
        and _integer(summary.get("combination_route_eligible_count"))
        == sum(
            1
            for (kind, _key, _version), binding in route_bindings.items()
            if kind == "COMBINATION" and binding.get("eligible") is True
        )
    )
    if not snapshot_valid:
        fail("SYSTEM", "market_router", "router snapshot hash/count/summary differs")
    return not errors, {
        "strategy_route_count": len(strategy_routes),
        "combination_route_count": len(combination_routes),
        "expected_router_snapshot_hash": expected_snapshot_hash,
        "stored_router_snapshot_hash": run.get("router_snapshot_hash"),
        "market_state": run_state,
        "market_state_config_version": config_version,
        "market_state_config_hash": config_hash,
        "errors": errors,
    }, route_bindings


def _portfolio_risk_evidence(metrics: Any) -> dict[str, Any]:
    """Independently canonicalize the complete 60-session risk path."""

    unavailable = {
        "schema": PORTFOLIO_RISK_EVIDENCE_SCHEMA,
        "status": "UNAVAILABLE",
        "window_days": 60,
        "daily_returns": [],
        "daily_stock_exposures": [],
        "current_stock_exposure": [],
        "peak_gross_exposure_value": None,
        "peak_gross_exposure_trade_date": "",
        "exposure_path_hash": "",
    }
    metrics = metrics if isinstance(metrics, dict) else {}
    records = metrics.get("internal_daily_records")
    raw_daily_exposure = metrics.get(
        "internal_daily_stock_market_values"
    )
    try:
        if not isinstance(records, list) or not isinstance(
            raw_daily_exposure, list
        ):
            raise ValueError("portfolio risk inputs are missing")
        daily_returns: list[dict[str, str]] = []
        seen_dates: set[str] = set()
        for item in records:
            if not isinstance(item, dict):
                raise ValueError("portfolio daily return row is invalid")
            trade_date = date.fromisoformat(
                str(item.get("trade_date") or "")
            ).isoformat()
            value = Decimal(str(item.get("return_pct"))).quantize(
                Decimal("0.00000001"), rounding=ROUND_HALF_EVEN
            )
            if trade_date in seen_dates or not value.is_finite():
                raise ValueError("portfolio daily returns are invalid")
            seen_dates.add(trade_date)
            daily_returns.append({
                "trade_date": trade_date,
                "return_pct": format(value, ".8f"),
            })
        daily_returns.sort(key=lambda item: item["trade_date"])
        exposure_path: list[dict[str, Any]] = []
        exposure_dates: set[str] = set()
        for raw_row in raw_daily_exposure:
            if not isinstance(raw_row, dict):
                raise ValueError("portfolio daily exposure row is invalid")
            trade_day = date.fromisoformat(
                str(raw_row.get("trade_date") or "")
            ).isoformat()
            raw_values = raw_row.get("stock_risk_exposure")
            if trade_day in exposure_dates or not isinstance(
                raw_values, dict
            ):
                raise ValueError("portfolio daily exposure identity is invalid")
            exposure_dates.add(trade_day)
            values: dict[str, Decimal] = {}
            for raw_code, raw_value in raw_values.items():
                code = str(raw_code).strip()
                value = Decimal(str(raw_value))
                if (
                    re.fullmatch(r"[0-9]{6}", code) is None
                    or code in values
                    or not value.is_finite()
                    or value < 0
                ):
                    raise ValueError("portfolio daily stock exposure is invalid")
                if value > 0:
                    values[code] = value
            gross = sum(values.values(), Decimal("0"))
            normalized = [
                {
                    "stock_code": code,
                    "normalized_weight": format(
                        (value / gross).quantize(
                            Decimal("0.000000000001"),
                            rounding=ROUND_HALF_EVEN,
                        ),
                        ".12f",
                    ),
                }
                for code, value in sorted(values.items())
            ] if gross > 0 else []
            exposure_path.append({
                "trade_date": trade_day,
                "gross_exposure_value": format(
                    gross.quantize(
                        Decimal("0.00000001"),
                        rounding=ROUND_HALF_EVEN,
                    ),
                    ".8f",
                ),
                "normalized_stock_weights": normalized,
            })
        exposure_path.sort(key=lambda item: item["trade_date"])
        if (
            len(daily_returns) != 60
            or len(exposure_path) != 60
            or [item["trade_date"] for item in daily_returns]
            != [item["trade_date"] for item in exposure_path]
        ):
            raise ValueError(
                "portfolio risk path must cover the same 60 sessions"
            )
        peak_row = max(
            exposure_path,
            key=lambda item: Decimal(item["gross_exposure_value"]),
        )
        exposure_path_payload = {
            "schema": "probiga.strategy-daily-stock-exposure-path.v1",
            "rows": exposure_path,
        }
        payload = {
            "schema": PORTFOLIO_RISK_EVIDENCE_SCHEMA,
            "status": "READY",
            "window_days": 60,
            "daily_returns": daily_returns,
            "daily_stock_exposures": exposure_path,
            "current_stock_exposure": list(
                exposure_path[-1]["normalized_stock_weights"]
            ),
            "peak_gross_exposure_value": peak_row[
                "gross_exposure_value"
            ],
            "peak_gross_exposure_trade_date": peak_row["trade_date"],
            "exposure_path_hash": _canonical_digest(
                exposure_path_payload
            ),
        }
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        payload = unavailable
    return {**payload, "evidence_hash": _canonical_digest(payload)}


def _validated_portfolio_risk_evidence(
    evidence: Any,
) -> tuple[dict[str, float], dict[str, dict[str, Any]]] | None:
    if not isinstance(evidence, dict) or set(evidence) != {
        "schema", "status", "window_days", "daily_returns",
        "daily_stock_exposures", "current_stock_exposure",
        "peak_gross_exposure_value", "peak_gross_exposure_trade_date",
        "exposure_path_hash", "evidence_hash",
    }:
        return None
    payload = {
        key: value for key, value in evidence.items()
        if key != "evidence_hash"
    }
    if (
        evidence.get("schema") != PORTFOLIO_RISK_EVIDENCE_SCHEMA
        or evidence.get("status") != "READY"
        or evidence.get("window_days") != 60
        or RESULT_HASH_RE.fullmatch(str(evidence.get("evidence_hash") or ""))
        is None
        or _canonical_digest(payload) != str(evidence.get("evidence_hash") or "")
    ):
        return None
    daily_rows = evidence.get("daily_returns")
    exposure_rows = evidence.get("daily_stock_exposures")
    if not isinstance(daily_rows, list) or not isinstance(exposure_rows, list):
        return None
    daily: dict[str, float] = {}
    exposure_path: dict[str, dict[str, Any]] = {}
    try:
        for item in daily_rows:
            if not isinstance(item, dict) or set(item) != {
                "trade_date", "return_pct",
            }:
                return None
            trade_date = date.fromisoformat(
                str(item.get("trade_date") or "")
            ).isoformat()
            raw_value = str(item.get("return_pct") or "")
            value = Decimal(raw_value)
            if (
                trade_date in daily or not value.is_finite()
                or format(value.quantize(
                    Decimal("0.00000001"), rounding=ROUND_HALF_EVEN
                ), ".8f") != raw_value
            ):
                return None
            daily[trade_date] = float(value)
        for item in exposure_rows:
            if not isinstance(item, dict) or set(item) != {
                "trade_date", "gross_exposure_value",
                "normalized_stock_weights",
            }:
                return None
            trade_day = date.fromisoformat(
                str(item.get("trade_date") or "")
            ).isoformat()
            gross_text = str(item.get("gross_exposure_value") or "")
            gross = Decimal(gross_text)
            if (
                trade_day in exposure_path or not gross.is_finite()
                or gross < 0
                or format(gross.quantize(
                    Decimal("0.00000001"), rounding=ROUND_HALF_EVEN
                ), ".8f") != gross_text
            ):
                return None
            raw_weights = item.get("normalized_stock_weights")
            if not isinstance(raw_weights, list):
                return None
            weights: dict[str, float] = {}
            weight_total = Decimal("0")
            for raw_weight in raw_weights:
                if not isinstance(raw_weight, dict) or set(raw_weight) != {
                    "stock_code", "normalized_weight",
                }:
                    return None
                code = str(raw_weight.get("stock_code") or "")
                weight_text = str(
                    raw_weight.get("normalized_weight") or ""
                )
                weight = Decimal(weight_text)
                if (
                    re.fullmatch(r"[0-9]{6}", code) is None
                    or code in weights or not weight.is_finite()
                    or weight <= 0
                    or format(weight.quantize(
                        Decimal("0.000000000001"),
                        rounding=ROUND_HALF_EVEN,
                    ), ".12f") != weight_text
                ):
                    return None
                weights[code] = float(weight)
                weight_total += weight
            tolerance = Decimal("0.000000000001") * max(1, len(weights))
            if (
                (gross == 0 and weights)
                or (gross > 0 and not weights)
                or (weights and abs(weight_total - Decimal("1")) > tolerance)
            ):
                return None
            exposure_path[trade_day] = {
                "gross_exposure_value": float(gross),
                "weights": weights,
            }
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return None
    if (
        len(daily) != 60 or len(exposure_path) != 60
        or list(daily) != sorted(daily)
        or list(exposure_path) != sorted(exposure_path)
        or list(daily) != list(exposure_path)
        or evidence.get("current_stock_exposure")
        != exposure_rows[-1].get("normalized_stock_weights")
        or str(evidence.get("peak_gross_exposure_trade_date") or "")
        not in exposure_path
    ):
        return None
    peak_day = str(evidence.get("peak_gross_exposure_trade_date") or "")
    try:
        peak_value = Decimal(str(
            evidence.get("peak_gross_exposure_value") or ""
        ))
        observed_peak = max(
            Decimal(str(row["gross_exposure_value"]))
            for row in exposure_rows
        )
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return None
    if (
        not peak_value.is_finite() or peak_value != observed_peak
        or Decimal(str(exposure_path[peak_day]["gross_exposure_value"]))
        != observed_peak
        or str(evidence.get("exposure_path_hash") or "")
        != _canonical_digest({
            "schema": "probiga.strategy-daily-stock-exposure-path.v1",
            "rows": exposure_rows,
        })
    ):
        return None
    return daily, exposure_path


def _allocation_candidate_contract(
    route_bindings: dict[tuple[str, str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Rebuild the complete, pre-overlap capital-competition population."""

    candidates: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for (target_type, target_key, target_version), binding in sorted(
        route_bindings.items()
    ):
        status = str(binding.get("lifecycle_status") or "")
        enabled = binding.get("enabled") is True
        profit_gate_passed = binding.get("profit_gate_passed") is True
        route_eligible = binding.get("eligible") is True
        constraint_passed = binding.get("constraint_passed") is True
        # End-to-end health route bindings always carry this v7 proof.  The
        # pure allocation replay helper also accepts already-validated test or
        # historical bindings that predate the explicit field; in that case
        # their declared paper eligibility is the upstream validation result.
        confirmation_passed = binding.get(
            "statistical_confirmation_passed"
        )
        if confirmation_passed is None:
            confirmation_passed = binding.get(
                "paper_allocation_eligible"
            ) is True
        expected_paper_eligible = (
            enabled
            and status in {"ACTIVE", "REDUCE"}
            and profit_gate_passed
            and route_eligible
            and confirmation_passed is True
            and (
                constraint_passed if target_type == "COMBINATION" else True
            )
        )
        ranking_score = binding.get("ranking_score")
        market_match_score = binding.get("market_match_score")
        funding_gate_hash = str(binding.get("funding_gate_hash") or "")
        router_decision_hash = str(
            binding.get("router_decision_hash") or ""
        )
        target_name = str(binding.get("target_name") or "")
        exposures = sorted(set(binding.get("members") or ()))
        paper_eligible = (
            binding.get("paper_allocation_eligible") is True
        )
        multiplier = LIFECYCLE_RISK_MULTIPLIER.get(status, Decimal("0"))
        member_sleeve_multiplier = (
            binding.get("member_sleeve_risk_multiplier")
            if target_type == "COMBINATION"
            else None
        )
        member_sleeves_source = (
            binding.get("member_sleeves_source")
            if target_type == "COMBINATION"
            else None
        )
        declared_risk_evidence = binding.get("portfolio_risk_evidence")
        portfolio_risk_evidence = (
            declared_risk_evidence
            if _validated_portfolio_risk_evidence(
                declared_risk_evidence
            ) is not None
            else _portfolio_risk_evidence(
                binding.get("portfolio_risk_metrics")
            )
        )
        risk_evidence_ready = (
            _validated_portfolio_risk_evidence(
                portfolio_risk_evidence
            ) is not None
        )
        valid = (
            target_type in {"STRATEGY", "COMBINATION"}
            and bool(target_key)
            and bool(target_version)
            and bool(target_name.strip())
            and status in LIFECYCLE_STATES
            and isinstance(ranking_score, Decimal)
            and ranking_score >= 0
            and isinstance(market_match_score, Decimal)
            and Decimal("0") <= market_match_score <= Decimal("100")
            and RESULT_HASH_RE.fullmatch(funding_gate_hash) is not None
            and RESULT_HASH_RE.fullmatch(router_decision_hash) is not None
            and bool(exposures)
            and paper_eligible is expected_paper_eligible
            and (not expected_paper_eligible or risk_evidence_ready)
            and (
                target_type != "COMBINATION"
                or (
                    isinstance(member_sleeve_multiplier, Decimal)
                    and Decimal("0") <= member_sleeve_multiplier
                    <= Decimal("1")
                    and isinstance(member_sleeves_source, list)
                    and bool(member_sleeves_source)
                )
            )
        )
        if not valid:
            errors.append(
                {
                    "target_type": target_type,
                    "target_key": target_key,
                    "reason": "allocation candidate binding is invalid",
                }
            )
        candidates.append(
            {
                "target_type": target_type,
                "target_key": target_key,
                "target_version": target_version,
                "target_name": target_name,
                "enabled": enabled,
                "funding_gate_hash": funding_gate_hash,
                "ranking_score": round(float(ranking_score or 0), 4),
                "ranking_basis": DAILY_NAV_RANKING_BASIS,
                "ranking_basis_label": DAILY_NAV_RANKING_BASIS_LABEL,
                "profit_gate_passed": profit_gate_passed,
                "paper_allocation_eligible": paper_eligible,
                "market_state": str(binding.get("market_state") or ""),
                "market_route_eligible": route_eligible,
                "market_match_score": round(
                    float(market_match_score or 0), 4
                ),
                "router_decision_hash": router_decision_hash,
                "exposure_keys": exposures,
                "lifecycle_status": status,
                "lifecycle_status_label": LIFECYCLE_LABELS.get(
                    status, "未知状态"
                ),
                "lifecycle_risk_multiplier": round(float(multiplier), 4),
                "constraint_passed": constraint_passed,
                "portfolio_risk_evidence": portfolio_risk_evidence,
                **(
                    {
                        "combination_lifecycle_risk_multiplier": round(
                            float(multiplier), 4
                        ),
                        "member_sleeve_risk_multiplier": round(
                            float(member_sleeve_multiplier or 0), 8
                        ),
                        "lifecycle_risk_multiplier": round(
                            float(
                                multiplier
                                * (member_sleeve_multiplier or Decimal("0"))
                            ),
                            8,
                        ),
                        "member_sleeves_source": member_sleeves_source or [],
                    }
                    if target_type == "COMBINATION"
                    else {}
                ),
            }
        )
    candidates.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return candidates, errors


def _largest_remainder_basis_points(
    total_basis_points: int,
    weighted_keys: list[tuple[str, Decimal]],
) -> dict[str, int]:
    """Replay deterministic integer-bp allocation with exact conservation."""

    if total_basis_points < 0 or not weighted_keys:
        raise ValueError("member sleeve bp budget or weights are invalid")
    total_weight = sum(
        (weight for _key, weight in weighted_keys), Decimal("0")
    )
    if not total_weight.is_finite() or total_weight <= 0:
        raise ValueError("member sleeve total weight is invalid")
    raw = {
        key: Decimal(total_basis_points) * weight / total_weight
        for key, weight in weighted_keys
    }
    assigned = {key: int(value) for key, value in raw.items()}
    remainder = total_basis_points - sum(assigned.values())
    order = sorted(
        raw,
        key=lambda key: (-(raw[key] - Decimal(assigned[key])), key),
    )
    for key in order[:remainder]:
        assigned[key] += 1
    if sum(assigned.values()) != total_basis_points:
        raise RuntimeError("member sleeve base bp is not conserved")
    return assigned


def _combination_member_sleeve_contract(
    row: dict[str, Any], base_basis_points: int,
) -> tuple[list[dict[str, Any]], str, int, int]:
    """Independently replay the immutable v3 per-member sleeve contract."""

    sources = row.get("member_sleeves_source")
    if not isinstance(sources, list) or not sources:
        raise ValueError("combination allocation lacks member sleeve sources")
    identities: set[str] = set()
    weighted: list[tuple[str, Decimal]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for item in sources:
        if not isinstance(item, dict):
            raise ValueError("combination member sleeve source is invalid")
        key = str(item.get("strategy_key") or "")
        weight = Decimal(str(item.get("original_weight") or "0"))
        if (
            not key
            or key in identities
            or not weight.is_finite()
            or weight <= 0
            or item.get("version_match") is not True
        ):
            raise ValueError(
                "combination member sleeve identity/version/weight is invalid"
            )
        identities.add(key)
        weighted.append((key, weight))
        by_key[key] = item
    base_by_key = _largest_remainder_basis_points(
        base_basis_points, weighted
    )
    combination_status = str(row.get("lifecycle_status") or "")
    combination_multiplier = LIFECYCLE_RISK_MULTIPLIER.get(
        combination_status, Decimal("0")
    )
    sleeves: list[dict[str, Any]] = []
    effective_total = 0
    for key in sorted(base_by_key):
        source = by_key[key]
        member_status = str(source.get("member_lifecycle_status") or "")
        member_multiplier = LIFECYCLE_RISK_MULTIPLIER.get(
            member_status, Decimal("0")
        )
        declared_member_multiplier = Decimal(str(
            source.get("member_lifecycle_multiplier") or "0"
        ))
        if declared_member_multiplier != member_multiplier:
            raise ValueError(
                "member lifecycle multiplier differs from frozen enum"
            )
        base_bp = base_by_key[key]
        effective_bp = int(
            (
                Decimal(base_bp)
                * member_multiplier
                * combination_multiplier
            ).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
        )
        if effective_bp < 0 or effective_bp > base_bp:
            raise ValueError("member effective bp is outside its base sleeve")
        effective_total += effective_bp
        sleeve_row = {
            "strategy_key": key,
            "strategy_version": str(source.get("strategy_version") or ""),
            "current_strategy_version": str(
                source.get("current_strategy_version") or ""
            ),
            "original_weight": format(
                Decimal(str(source.get("original_weight") or "0")), ".8f"
            ),
            "configured_weight_pct": round(
                float(
                    Decimal(str(source.get("original_weight") or "0"))
                    * 100
                ),
                8,
            ),
            "base_bp": base_bp,
            "base_weight_pct": base_bp / 100.0,
            "member_lifecycle_status": member_status,
            "member_lifecycle_multiplier": format(
                member_multiplier, ".8f"
            ),
            "member_multiplier": float(member_multiplier),
            "combination_lifecycle_status": combination_status,
            "combination_lifecycle_multiplier": format(
                combination_multiplier, ".8f"
            ),
            "combination_multiplier": float(combination_multiplier),
            "effective_bp": effective_bp,
            "effective_weight_pct": effective_bp / 100.0,
            "cash_discount_bp": base_bp - effective_bp,
            "discount_to_cash_pct": (base_bp - effective_bp) / 100.0,
        }
        sleeves.append(
            {
                **sleeve_row,
                "sleeve_row_hash": _canonical_digest(
                    {
                        "schema": (
                            "probiga.strategy-combination-member-sleeve-row.v1"
                        ),
                        **sleeve_row,
                    }
                ),
            }
        )
    cash_discount_bp = base_basis_points - effective_total
    if (
        sum(item["base_bp"] for item in sleeves) != base_basis_points
        or sum(item["effective_bp"] for item in sleeves) != effective_total
        or sum(item["cash_discount_bp"] for item in sleeves)
        != cash_discount_bp
    ):
        raise RuntimeError("combination member sleeves do not conserve 1bp")
    payload = {
        "schema": "probiga.strategy-combination-member-sleeves.v1",
        "combination_key": str(row.get("target_key") or ""),
        "combination_version": str(row.get("target_version") or ""),
        "base_bp": base_basis_points,
        "effective_bp": effective_total,
        "cash_discount_bp": cash_discount_bp,
        "members": sleeves,
    }
    return (
        sleeves,
        _canonical_digest(payload),
        effective_total,
        cash_discount_bp,
    )


def _portfolio_pair_check(
    candidate: dict[str, Any], selected: dict[str, Any],
) -> dict[str, Any]:
    left = _validated_portfolio_risk_evidence(
        candidate.get("portfolio_risk_evidence")
    )
    right = _validated_portfolio_risk_evidence(
        selected.get("portfolio_risk_evidence")
    )
    left_daily, left_exposure_path = left or ({}, {})
    right_daily, right_exposure_path = right or ({}, {})
    common = sorted(
        set(left_daily)
        & set(right_daily)
        & set(left_exposure_path)
        & set(right_exposure_path)
    )
    left_values = [left_daily[day] for day in common]
    right_values = [right_daily[day] for day in common]
    correlation: float | None = None
    if len(left_values) == len(right_values) and len(left_values) >= 2:
        left_mean = sum(left_values) / len(left_values)
        right_mean = sum(right_values) / len(right_values)
        numerator = sum(
            (a - left_mean) * (b - right_mean)
            for a, b in zip(left_values, right_values)
        )
        left_var = sum((value - left_mean) ** 2 for value in left_values)
        right_var = sum((value - right_mean) ** 2 for value in right_values)
        if left_var > 0 and right_var > 0:
            correlation = max(-1.0, min(
                1.0, numerator / math.sqrt(left_var * right_var)
            ))
    overlap_path: list[dict[str, Any]] = []
    for day in common:
        left_weights = left_exposure_path[day]["weights"]
        right_weights = right_exposure_path[day]["weights"]
        overlap = sum(
            min(
                float(left_weights.get(code) or 0.0),
                float(right_weights.get(code) or 0.0),
            )
            for code in set(left_weights) | set(right_weights)
        ) * 100.0
        overlap_path.append({
            "trade_date": day,
            "stock_overlap_pct": round(overlap, 4),
            "left_gross_exposure_value": round(
                float(left_exposure_path[day]["gross_exposure_value"]), 8
            ),
            "right_gross_exposure_value": round(
                float(right_exposure_path[day]["gross_exposure_value"]), 8
            ),
            "combined_gross_exposure_value": round(
                float(left_exposure_path[day]["gross_exposure_value"])
                + float(right_exposure_path[day]["gross_exposure_value"]),
                8,
            ),
        })
    peak_overlap = max(
        overlap_path,
        key=lambda item: item["stock_overlap_pct"],
        default=None,
    )
    current_overlap = overlap_path[-1] if overlap_path else None
    peak_capacity = max(
        overlap_path,
        key=lambda item: item["combined_gross_exposure_value"],
        default=None,
    )
    enough = len(common) == 60 and len(common) >= int(
        GLOBAL_PORTFOLIO_POLICY["minimum_pairwise_observations"]
    )
    overlap_limit = float(GLOBAL_PORTFOLIO_POLICY[
        "maximum_pairwise_stock_overlap_pct"
    ])
    capacity_path_valid = bool(
        len(overlap_path) == 60
        and peak_capacity is not None
        and current_overlap is not None
        and float(peak_capacity["combined_gross_exposure_value"]) >= 0.0
        and float(current_overlap["combined_gross_exposure_value"]) >= 0.0
        and float(peak_capacity["combined_gross_exposure_value"])
        >= float(current_overlap["combined_gross_exposure_value"])
    )
    passed = bool(
        enough
        and capacity_path_valid
        and correlation is not None
        and correlation <= float(
            GLOBAL_PORTFOLIO_POLICY["maximum_pairwise_correlation"]
        )
        and peak_overlap is not None
        and current_overlap is not None
        and float(peak_overlap["stock_overlap_pct"]) <= overlap_limit
        and float(current_overlap["stock_overlap_pct"]) <= overlap_limit
    )
    return {
        "left": f"{candidate['target_type']}:{candidate['target_key']}",
        "right": f"{selected['target_type']}:{selected['target_key']}",
        "observations": len(common),
        "correlation": round(correlation, 6) if correlation is not None else None,
        "stock_overlap_pct": (
            peak_overlap["stock_overlap_pct"] if peak_overlap else None
        ),
        "peak_stock_overlap_pct": (
            peak_overlap["stock_overlap_pct"] if peak_overlap else None
        ),
        "peak_stock_overlap_trade_date": (
            peak_overlap["trade_date"] if peak_overlap else ""
        ),
        "current_stock_overlap_pct": (
            current_overlap["stock_overlap_pct"] if current_overlap else None
        ),
        "current_stock_overlap_trade_date": (
            current_overlap["trade_date"] if current_overlap else ""
        ),
        "peak_combined_gross_exposure_value": (
            peak_capacity["combined_gross_exposure_value"]
            if peak_capacity else None
        ),
        "peak_capacity_trade_date": (
            peak_capacity["trade_date"] if peak_capacity else ""
        ),
        "current_combined_gross_exposure_value": (
            current_overlap["combined_gross_exposure_value"]
            if current_overlap else None
        ),
        "capacity_path_valid": capacity_path_valid,
        "daily_exposure_path_hash": _canonical_digest({
            "schema": "probiga.global-pair-daily-exposure-path.v1",
            "left": f"{candidate['target_type']}:{candidate['target_key']}",
            "right": f"{selected['target_type']}:{selected['target_key']}",
            "rows": overlap_path,
        }),
        "passed": passed,
        "reason": (
            "同步收益相关性、逐日持仓重叠及当前/峰值容量路径通过"
            if passed else
            "同步样本/容量路径不足或相关性/当前及峰值持仓重叠超过全局限制"
        ),
    }


def _replay_global_portfolio_gate(
    lanes: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    maximum = int(GLOBAL_PORTFOLIO_POLICY["maximum_funded_sleeves"])
    pending = [list(lane) for lane in lanes if lane]
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    funded_exposures: set[str] = set()
    while pending:
        next_round: list[list[dict[str, Any]]] = []
        for lane in pending:
            if not lane:
                continue
            candidate = lane.pop(0)
            exposures = set(candidate.get("exposure_keys") or ())
            evidence = candidate.get("portfolio_risk_evidence")
            evidence_ready = (
                _validated_portfolio_risk_evidence(evidence) is not None
            )
            evidence_rows = (
                evidence.get("daily_stock_exposures")
                if isinstance(evidence, dict) else None
            )
            current_capacity = (
                str(evidence_rows[-1].get("gross_exposure_value") or "")
                if evidence_ready and isinstance(evidence_rows, list)
                and evidence_rows else ""
            )
            comparisons = [
                _portfolio_pair_check(candidate, existing)
                for existing in accepted
            ]
            overlap_free = bool(
                exposures and not exposures.intersection(funded_exposures)
            )
            expanded_count = len(funded_exposures | exposures)
            pairwise_passed = all(item["passed"] for item in comparisons)
            passed = bool(
                evidence_ready
                and overlap_free
                and expanded_count <= maximum
                and pairwise_passed
            )
            candidate["global_portfolio_gate"] = {
                "passed": passed,
                "allocation_admission": (
                    "COMPLETE_60D_DAILY_EXPOSURE_PEAK_AND_CURRENT_CAP_V2"
                ),
                "portfolio_risk_evidence_hash": str(
                    (evidence or {}).get("evidence_hash") or ""
                ),
                "daily_exposure_path_hash": str(
                    (evidence or {}).get("exposure_path_hash") or ""
                ),
                "current_gross_exposure_value": current_capacity,
                "peak_60d_gross_exposure_value": str(
                    (evidence or {}).get("peak_gross_exposure_value") or ""
                ),
                "peak_60d_gross_exposure_trade_date": str(
                    (evidence or {}).get(
                        "peak_gross_exposure_trade_date"
                    ) or ""
                ),
                "expanded_strategy_sleeves": sorted(exposures),
                "funded_sleeve_count_after_admission": expanded_count,
                "pairwise_checks": comparisons,
                "policy": GLOBAL_PORTFOLIO_POLICY,
            }
            if passed:
                accepted.append(candidate)
                funded_exposures.update(exposures)
            else:
                reason = (
                    "缺少有效的冻结60日收益与持仓证据"
                    if not evidence_ready
                    else "组合展开后存在重复成员策略袖套"
                    if not overlap_free
                    else "组合展开后的唯一成员策略袖套超过单日上限"
                    if expanded_count > maximum
                    else "全局候选相关性或持仓重叠未通过"
                )
                rejected.append({
                    "target_type": candidate["target_type"],
                    "target_key": candidate["target_key"],
                    "reason": reason,
                    "portfolio_risk_evidence_hash": str(
                        (evidence or {}).get("evidence_hash") or ""
                    ),
                    "daily_exposure_path_hash": str(
                        (evidence or {}).get("exposure_path_hash") or ""
                    ),
                    "current_gross_exposure_value": current_capacity,
                    "peak_60d_gross_exposure_value": str(
                        (evidence or {}).get(
                            "peak_gross_exposure_value"
                        ) or ""
                    ),
                    "peak_60d_gross_exposure_trade_date": str(
                        (evidence or {}).get(
                            "peak_gross_exposure_trade_date"
                        ) or ""
                    ),
                    "expanded_strategy_sleeves": sorted(exposures),
                    "funded_sleeve_count_after_admission": expanded_count,
                    "comparisons": comparisons,
                })
            if lane:
                next_round.append(lane)
        pending = next_round
    audit_payload = {
        "schema": "probiga.global-portfolio-gate.v2",
        "policy": GLOBAL_PORTFOLIO_POLICY,
        "accepted": [
            {
                "target_type": row["target_type"],
                "target_key": row["target_key"],
                "portfolio_risk_evidence_hash": str(
                    (row.get("portfolio_risk_evidence") or {}).get(
                        "evidence_hash"
                    ) or ""
                ),
                "daily_exposure_path_hash": str(
                    (row.get("portfolio_risk_evidence") or {}).get(
                        "exposure_path_hash"
                    ) or ""
                ),
                "current_gross_exposure_value": str(
                    ((row.get("portfolio_risk_evidence") or {}).get(
                        "daily_stock_exposures"
                    ) or [{}])[-1].get("gross_exposure_value") or ""
                ),
                "peak_60d_gross_exposure_value": str(
                    (row.get("portfolio_risk_evidence") or {}).get(
                        "peak_gross_exposure_value"
                    ) or ""
                ),
                "peak_60d_gross_exposure_trade_date": str(
                    (row.get("portfolio_risk_evidence") or {}).get(
                        "peak_gross_exposure_trade_date"
                    ) or ""
                ),
                "expanded_strategy_sleeves": sorted(
                    row.get("exposure_keys") or ()
                ),
                "pairwise_checks": (
                    row.get("global_portfolio_gate") or {}
                ).get("pairwise_checks") or [],
            }
            for row in accepted
        ],
        "rejected": rejected,
    }
    return accepted, rejected, _canonical_digest(audit_payload)


def _expected_allocation_snapshot(
    candidates: list[dict[str, Any]],
    *,
    market_state: str,
    trading_gate_passed: bool,
    allocation_policy_version: str = ALLOCATION_POLICY_VERSION,
) -> list[dict[str, Any]]:
    """Independently replay v5 unified quality competition and sleeves."""

    eligible = [
        dict(row)
        for row in candidates
        if (
            row.get("paper_allocation_eligible") is True
            and row.get("enabled") is True
            and row.get("profit_gate_passed") is True
            and row.get("market_route_eligible") is True
            and str(row.get("lifecycle_status") or "")
            in {"ACTIVE", "REDUCE"}
            and (
                row.get("target_type") != "COMBINATION"
                or row.get("constraint_passed") is True
            )
            and row.get("ranking_basis") == DAILY_NAV_RANKING_BASIS
        )
    ]
    if allocation_policy_version == LEGACY_ALLOCATION_POLICY_VERSION:
        combinations_lane = sorted(
            (row for row in eligible if row["target_type"] == "COMBINATION"),
            key=lambda row: (
                -float(row["ranking_score"])
                * float(row["market_match_score"]) / 100.0,
                row["target_key"],
            ),
        )
        strategies_lane = sorted(
            (row for row in eligible if row["target_type"] == "STRATEGY"),
            key=lambda row: (
                -float(row["ranking_score"])
                * float(row["market_match_score"]) / 100.0,
                row["target_key"],
            ),
        )
        selected_combinations: list[dict[str, Any]] = []
        used_exposures: set[str] = set()
        for row in combinations_lane:
            exposures = set(row.get("exposure_keys") or ())
            if not exposures or exposures.intersection(used_exposures):
                continue
            selected_combinations.append(row)
            used_exposures.update(exposures)
        selected_strategies = [
            row for row in strategies_lane
            if set(row.get("exposure_keys") or ())
            and not set(row.get("exposure_keys") or ()).intersection(
                used_exposures
            )
        ]
        replay_lanes = [selected_combinations, selected_strategies]
    elif allocation_policy_version == ALLOCATION_POLICY_VERSION:
        eligible.sort(key=lambda row: (
            -float(row["ranking_score"])
            * float(row["market_match_score"])
            / 100.0,
            0 if row["target_type"] == "STRATEGY" else 1,
            row["target_key"],
        ))
        replay_lanes = [eligible]
    else:
        raise ValueError("unsupported allocation policy version")
    selected, _rejected, _global_gate_hash = (
        _replay_global_portfolio_gate(replay_lanes)
    )

    risk_cap = MARKET_RISK_CAP_PCT.get(market_state)
    allocations: list[dict[str, Any]] = []
    assigned_after_lifecycle = 0
    if (
        trading_gate_passed
        and selected
        and risk_cap is not None
        and risk_cap > 0
    ):
        cap_basis_points = int(round(float(risk_cap) * 100))
        if allocation_policy_version == LEGACY_ALLOCATION_POLICY_VERSION:
            selected_combinations = [
                row for row in selected
                if row["target_type"] == "COMBINATION"
            ]
            selected_strategies = [
                row for row in selected if row["target_type"] == "STRATEGY"
            ]
            nonempty_lanes = [
                lane for lane in (
                    selected_combinations, selected_strategies,
                ) if lane
            ]
            lane_base = cap_basis_points // len(nonempty_lanes)
            lane_remainder = (
                cap_basis_points - lane_base * len(nonempty_lanes)
            )
            assigned_by_identity: dict[tuple[str, str], int] = {}
            for lane_index, lane in enumerate(nonempty_lanes):
                lane_budget = lane_base + (
                    1 if lane_index < lane_remainder else 0
                )
                lane_values = [
                    max(
                        0.0001,
                        float(row["ranking_score"])
                        * float(row["market_match_score"]) / 100.0,
                    ) for row in lane
                ]
                lane_total = sum(lane_values)
                raw_lane = [
                    lane_budget * value / lane_total
                    for value in lane_values
                ]
                assigned_lane = [int(value) for value in raw_lane]
                remainder = lane_budget - sum(assigned_lane)
                order = sorted(
                    range(len(lane)),
                    key=lambda index: (
                        -(raw_lane[index] - assigned_lane[index]),
                        lane[index]["target_key"],
                    ),
                )
                for index in order[:remainder]:
                    assigned_lane[index] += 1
                for row, basis_points in zip(lane, assigned_lane):
                    assigned_by_identity[
                        (row["target_type"], row["target_key"])
                    ] = basis_points
            assigned = [
                assigned_by_identity[(row["target_type"], row["target_key"])]
                for row in selected
            ]
        else:
            competitive_values = [
                max(
                    0.0001,
                    float(row["ranking_score"])
                    * float(row["market_match_score"]) / 100.0,
                )
                for row in selected
            ]
            competitive_total = sum(competitive_values)
            raw_weights = [
                cap_basis_points * value / competitive_total
                for value in competitive_values
            ]
            assigned = [int(value) for value in raw_weights]
            remainder = cap_basis_points - sum(assigned)
            order = sorted(
                range(len(selected)),
                key=lambda index: (
                    -(raw_weights[index] - assigned[index]),
                    selected[index]["target_type"],
                    selected[index]["target_key"],
                ),
            )
            for index in order[:remainder]:
                assigned[index] += 1
        for row, base_basis_points in zip(selected, assigned):
            member_sleeves: list[dict[str, Any]] = []
            member_sleeve_hash = ""
            if row["target_type"] == "COMBINATION":
                (
                    member_sleeves,
                    member_sleeve_hash,
                    basis_points,
                    cash_discount_bp,
                ) = _combination_member_sleeve_contract(
                    row, base_basis_points
                )
            else:
                multiplier = Decimal(
                    str(row.get("lifecycle_risk_multiplier") or "0")
                )
                basis_points = int(
                    (Decimal(base_basis_points) * multiplier).quantize(
                        Decimal("1"), rounding=ROUND_HALF_EVEN
                    )
                )
                cash_discount_bp = base_basis_points - basis_points
            if basis_points <= 0:
                continue
            assigned_after_lifecycle += basis_points
            allocations.append(
                {
                    "target_type": row["target_type"],
                    "target_key": row["target_key"],
                    "target_version": row["target_version"],
                    "funding_gate_hash": row["funding_gate_hash"],
                    "market_state": row["market_state"],
                    "market_match_score": round(
                        float(row["market_match_score"]), 4
                    ),
                    "router_decision_hash": row["router_decision_hash"],
                    "lifecycle_status": row["lifecycle_status"],
                    "lifecycle_status_label": row[
                        "lifecycle_status_label"
                    ],
                    "lifecycle_risk_multiplier": round(
                        float(row["lifecycle_risk_multiplier"]), 4
                    ),
                    "base_competitive_weight_pct": round(
                        base_basis_points / 100.0, 4
                    ),
                    "simulated_weight_pct": round(
                        basis_points / 100.0, 4
                    ),
                    "member_sleeves": member_sleeves,
                    "member_sleeve_hash": member_sleeve_hash,
                    "cash_discount_bp": cash_discount_bp,
                    "real_order_authority": False,
                }
            )
    allocations.append(
        {
            "target_type": "CASH",
            "target_key": "cash",
            "target_version": "",
            "funding_gate_hash": "",
            "market_state": market_state,
            "market_match_score": 0.0,
            "router_decision_hash": "",
            "lifecycle_status": "",
            "lifecycle_status_label": "",
            "lifecycle_risk_multiplier": 0.0,
            "base_competitive_weight_pct": 0.0,
            "simulated_weight_pct": round(
                (10_000 - assigned_after_lifecycle) / 100.0, 4
            ),
            "member_sleeves": [],
            "member_sleeve_hash": "",
            "cash_discount_bp": 0,
            "real_order_authority": False,
        }
    )
    allocations.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return allocations


def _stored_allocation_snapshot(
    allocation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for row in allocation_rows:
        member_sleeves_value = (
            row.get("member_sleeves_json")
            if "member_sleeves_json" in row
            else row.get("member_sleeves")
        )
        member_sleeves = _json_array(member_sleeves_value)
        if member_sleeves is None:
            # Preserve structural invalidity in the snapshot so replay fails
            # closed instead of silently treating malformed JSON as empty.
            member_sleeves = [{"invalid_member_sleeves_json": True}]
        rows.append(
            {
                "target_type": str(row.get("target_type") or ""),
                "target_key": str(row.get("target_key") or ""),
                "target_version": str(row.get("target_version") or ""),
                "funding_gate_hash": str(row.get("funding_gate_hash") or ""),
                "market_state": str(row.get("market_state") or ""),
                "market_match_score": round(
                    float(_decimal(row.get("market_match_score")) or 0), 4
                ),
                "router_decision_hash": str(
                    row.get("router_decision_hash") or ""
                ),
                "lifecycle_status": str(row.get("lifecycle_status") or ""),
                "lifecycle_status_label": str(
                    row.get("lifecycle_status_label") or ""
                ),
                "lifecycle_risk_multiplier": round(
                    float(
                        _decimal(row.get("lifecycle_risk_multiplier")) or 0
                    ),
                    4,
                ),
                "base_competitive_weight_pct": round(
                    float(
                        _decimal(row.get("base_competitive_weight_pct")) or 0
                    ),
                    4,
                ),
                "simulated_weight_pct": round(
                    float(
                        _decimal(row.get("simulated_weight_pct")) or 0
                    ),
                    4,
                ),
                "member_sleeves": member_sleeves,
                "member_sleeve_hash": str(
                    row.get("member_sleeve_hash") or ""
                ),
                "cash_discount_bp": _integer(
                    row.get("cash_discount_bp")
                ),
                "real_order_authority": bool(
                    _integer(row.get("real_order_authority"))
                ),
            }
        )
    rows.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return rows


def _pool_score_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value)).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_EVEN
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("pool score cannot be normalized") from exc
    if not parsed.is_finite():
        raise ValueError("pool score must be finite")
    return format(parsed, ".4f")


def _pool_snapshot_contract_check(
    rows: list[dict[str, Any]],
    *,
    trade_date: str,
    route_bindings: dict[tuple[str, str, str], dict[str, Any]],
    summary: dict[str, Any],
    industry_bindings: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, dict[str, Any], str]:
    industry_bindings = industry_bindings or {}
    errors: list[dict[str, Any]] = []
    row_contracts: list[dict[str, Any]] = []
    ranks: dict[str, list[int]] = {
        "OBSERVATION": [],
        "CONFIRMATION": [],
        "TRADABLE": [],
    }
    qualified_references: dict[str, list[tuple[str, str, str]]] = {}
    for binding_key, binding in route_bindings.items():
        entity_type, entity_key, entity_version = binding_key
        qualified = (
            binding.get("enabled") is True
            and binding.get("profit_gate_passed") is True
            and binding.get("eligible") is True
            and binding.get("paper_allocation_eligible") is True
            and str(binding.get("lifecycle_status") or "")
            in {"ACTIVE", "REDUCE"}
            and RESULT_HASH_RE.fullmatch(
                str(binding.get("funding_gate_hash") or "")
            )
            is not None
            and (
                entity_type != "COMBINATION"
                or binding.get("constraint_passed") is True
            )
        )
        if qualified:
            qualified_references.setdefault(entity_key, []).append(
                (entity_type, entity_key, entity_version)
            )

    structurally_complete = True
    for row in rows:
        level = str(row.get("pool_level") or "")
        stock_code = str(row.get("stock_code") or "").strip()
        rank_no = _integer(row.get("rank_no"))
        strategies = _json_array(row.get("strategies_json"))
        reason = _json_object(row.get("reason_json"))
        evidence_envelope = _json_object(row.get("evidence_json"))
        row_identity = {
            "pool_level": level,
            "stock_code": stock_code,
            "rank_no": rank_no,
        }
        if (
            level not in ranks
            or not stock_code
            or rank_no <= 0
            or strategies is None
            or reason is None
            or evidence_envelope is None
            or set(reason) != {"reason", "blocking_reasons"}
            or not isinstance(reason.get("reason"), str)
            or not isinstance(reason.get("blocking_reasons"), list)
            or any(
                not isinstance(value, str)
                for value in reason.get("blocking_reasons", [])
            )
            or set(evidence_envelope)
            != {
                "schema", "source_evidence", "industry_names",
                "industry_by_strategy", "industry_binding",
                "pool_row_hash",
            }
            or evidence_envelope.get("schema")
            != POOL_ROW_EVIDENCE_SCHEMA
            or not isinstance(
                evidence_envelope.get("source_evidence"), dict
            )
            or not isinstance(
                evidence_envelope.get("industry_names"), list
            )
            or not isinstance(
                evidence_envelope.get("industry_by_strategy"), dict
            )
            or not isinstance(
                evidence_envelope.get("industry_binding"), dict
            )
            or any(not isinstance(value, str) for value in strategies or [])
        ):
            structurally_complete = False
            errors.append(
                {**row_identity, "reason": "pool row JSON/identity differs"}
            )
            continue
        try:
            industry_binding = evidence_envelope["industry_binding"]
            payload = {
                "schema": POOL_ROW_SCHEMA,
                "trade_date": _iso_date(row.get("trade_date")),
                "pool_level": level,
                "stock_code": stock_code,
                "stock_name": str(row.get("stock_name") or ""),
                "rank_no": rank_no,
                "opportunity_score": _pool_score_text(
                    row.get("opportunity_score")
                ),
                "execution_score": _pool_score_text(
                    row.get("execution_score")
                ),
                "dominant_strategy": str(
                    row.get("dominant_strategy") or ""
                ),
                "strategies": strategies,
                "industry_name": str(row.get("industry_name") or ""),
                "industry_type": str(
                    industry_binding.get("industry_type") or ""
                ),
                "industry_snapshot_id": str(
                    industry_binding.get("snapshot_id") or ""
                ),
                "industry_snapshot_hash": str(
                    industry_binding.get("snapshot_hash") or ""
                ),
                "industry_row_hash": str(
                    industry_binding.get("row_hash") or ""
                ),
                "industry_source_system": str(
                    industry_binding.get("source_system") or ""
                ),
                "industry_source_fact_id": str(
                    industry_binding.get("source_fact_id") or ""
                ),
                "industry_binding": industry_binding,
                "industry_names": evidence_envelope["industry_names"],
                "industry_by_strategy": evidence_envelope[
                    "industry_by_strategy"
                ],
                "gate_status": str(row.get("gate_status") or ""),
                "reason": reason,
                "evidence": evidence_envelope["source_evidence"],
            }
            row_hash = _canonical_digest(payload)
        except Exception as exc:
            structurally_complete = False
            errors.append(
                {
                    **row_identity,
                    "reason": _safe_exception_message(
                        exc, error_code="pool_row_normalization_failed"
                    ),
                }
            )
            continue
        stored_row_hash = str(
            evidence_envelope.get("pool_row_hash") or ""
        )
        if (
            payload["trade_date"] != trade_date
            or stored_row_hash != row_hash
            or RESULT_HASH_RE.fullmatch(stored_row_hash) is None
        ):
            errors.append(
                {
                    **row_identity,
                    "reason": "pool row canonical hash/date differs",
                    "expected_pool_row_hash": row_hash,
                    "stored_pool_row_hash": stored_row_hash,
                }
            )
        expected_binding = industry_bindings.get(stock_code)
        if expected_binding is not None:
            if (
                payload["industry_binding"] != expected_binding
                or payload["industry_name"]
                != str(expected_binding.get("industry_name") or "")
                or payload["industry_names"]
                != [str(expected_binding.get("industry_name") or "")]
                or payload["industry_by_strategy"] != {
                    key: str(expected_binding.get("industry_name") or "")
                    for key in strategies
                }
            ):
                errors.append({
                    **row_identity,
                    "reason": "pool industry binding differs from QMT replay",
                })
        elif (
            level != "OBSERVATION"
            or payload["industry_binding"] != {}
            or payload["industry_name"]
            or payload["industry_names"]
            or payload["industry_by_strategy"]
            or "目标日QMT一级行业冻结事实缺失或无效"
            not in (reason.get("blocking_reasons") or [])
        ):
            errors.append({
                **row_identity,
                "reason": "missing QMT industry fact is not observation-only",
            })
        ranks[level].append(rank_no)
        row_contracts.append(
            {
                "pool_level": level,
                "rank_no": rank_no,
                "stock_code": stock_code,
                "pool_row_hash": row_hash,
            }
        )
        if level == "TRADABLE":
            references = list(strategies)
            dominant = payload["dominant_strategy"]
            invalid_references = [
                reference
                for reference in references
                if len(qualified_references.get(reference, [])) != 1
            ]
            if (
                payload["gate_status"] != "模拟资金候选"
                or not references
                or len(references) != len(set(references))
                or dominant not in references
                or invalid_references
            ):
                errors.append(
                    {
                        **row_identity,
                        "reason": (
                            "TRADABLE row references a non-funding-qualified "
                            "or ambiguous strategy/combination"
                        ),
                        "dominant_strategy": dominant,
                        "strategy_references": references,
                        "invalid_references": invalid_references,
                    }
                )

    for level, observed_ranks in ranks.items():
        if sorted(observed_ranks) != list(range(1, len(observed_ranks) + 1)):
            errors.append(
                {
                    "pool_level": level,
                    "reason": "pool ranks are not a complete unique sequence",
                    "observed_ranks": sorted(observed_ranks),
                }
            )
    row_contracts.sort(
        key=lambda item: (
            item["pool_level"],
            item["rank_no"],
            item["stock_code"],
        )
    )
    snapshot_payload = {
        "schema": POOL_SNAPSHOT_SCHEMA,
        "trade_date": trade_date,
        "row_count": len(row_contracts),
        "rows": row_contracts,
    }
    snapshot_hash = (
        _canonical_digest(snapshot_payload) if structurally_complete else ""
    )
    if (
        _integer(summary.get("pool_row_count")) != len(rows)
        or str(summary.get("pool_snapshot_hash") or "") != snapshot_hash
        or RESULT_HASH_RE.fullmatch(snapshot_hash) is None
    ):
        errors.append(
            {
                "reason": "pool snapshot summary hash/count differs",
                "expected_pool_snapshot_hash": snapshot_hash,
                "stored_pool_snapshot_hash": summary.get(
                    "pool_snapshot_hash"
                ),
                "expected_pool_row_count": len(rows),
                "stored_pool_row_count": summary.get("pool_row_count"),
            }
        )
    return not errors, {
        "pool_row_schema": POOL_ROW_SCHEMA,
        "pool_snapshot_schema": POOL_SNAPSHOT_SCHEMA,
        "pool_row_count": len(rows),
        "expected_pool_snapshot_hash": snapshot_hash,
        "stored_pool_snapshot_hash": summary.get("pool_snapshot_hash"),
        "qualified_reference_count": sum(
            len(values) for values in qualified_references.values()
        ),
        "errors": errors[:100],
    }, snapshot_hash


def _paper_execution_plan_contract_check(
    run: dict[str, Any],
    *,
    trade_date: str,
    pool_rows: list[dict[str, Any]] | None,
    allocation_rows: list[dict[str, Any]],
    industry_bindings: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, dict[str, Any], str]:
    """Independently verify the persisted stock-level paper funding plan."""

    errors: list[dict[str, Any]] = []
    industry_bindings = industry_bindings or {}
    raw_result = run.get("result_json")
    stored_result_hash = str(run.get("result_hash") or "")
    if not isinstance(raw_result, str) or not raw_result:
        return False, {
            "expected_plan_hash": "",
            "stored_result_hash": stored_result_hash,
            "errors": [{"reason": "canonical result_json is missing"}],
        }, ""
    raw_result_hash = hashlib.sha256(raw_result.encode("utf-8")).hexdigest()
    result = _json_object(raw_result)
    if (
        RESULT_HASH_RE.fullmatch(stored_result_hash) is None
        or raw_result_hash != stored_result_hash
        or result is None
    ):
        return False, {
            "expected_plan_hash": "",
            "stored_result_hash": stored_result_hash,
            "recomputed_result_hash": raw_result_hash,
            "errors": [{"reason": "canonical result_json hash is invalid"}],
        }, ""
    plan = result.get("paper_execution_plan")
    summary = _json_object(run.get("summary_json")) or {}
    if not isinstance(plan, dict):
        return False, {
            "expected_plan_hash": "",
            "stored_result_hash": stored_result_hash,
            "errors": [{"reason": "paper execution plan is missing"}],
        }, ""
    plan_payload = {
        str(key): value for key, value in plan.items()
        if str(key) != "plan_hash"
    }
    expected_plan_hash = _canonical_digest(plan_payload)
    stored_plan_hash = str(plan.get("plan_hash") or "")
    if (
        expected_plan_hash != stored_plan_hash
        or stored_plan_hash
        != str(result.get("paper_execution_plan_hash") or "")
        or stored_plan_hash
        != str(summary.get("paper_execution_plan_hash") or "")
    ):
        errors.append({"reason": "paper plan hash binding differs"})
    expected_plan_fields = {
        "schema", "trade_date", "policy", "funded_sleeves",
        "industry_snapshot_id", "industry_snapshot_hash",
        "industry_snapshot_status",
        "portfolio_risk", "requested_new_buy_turnover_bp",
        "new_buy_turnover_multiplier", "actual_new_buy_turnover_bp",
        "targets", "exit_targets", "target_count", "invested_bp",
        "cash_bp", "automatic_real_order_submission",
        "real_order_authority", "plan_hash",
    }
    if set(plan) != expected_plan_fields:
        errors.append({"reason": "paper plan fields differ from v1 schema"})
    if (
        plan.get("schema")
        != "probiga.governance-paper-execution-plan.v1"
        or str(plan.get("trade_date") or "") != trade_date
        or str(plan.get("industry_snapshot_id") or "")
        != str((result.get("candidate_industry_snapshot") or {}).get(
            "snapshot_id"
        ) or "")
        or str(plan.get("industry_snapshot_hash") or "")
        != str(result.get("candidate_industry_snapshot_hash") or "")
        or str(plan.get("industry_snapshot_status") or "")
        != str((result.get("candidate_industry_snapshot") or {}).get(
            "status"
        ) or "")
        or plan.get("policy") != GLOBAL_PORTFOLIO_POLICY
        or plan.get("automatic_real_order_submission") is not False
        or plan.get("real_order_authority") is not False
    ):
        errors.append({"reason": "paper plan identity/policy/safety differs"})

    sleeves = plan.get("funded_sleeves")
    sleeve_keys: set[str] = set()
    if not isinstance(sleeves, list):
        errors.append({"reason": "funded sleeves are not a list"})
        sleeves = []
    for sleeve in sleeves:
        key = str(sleeve.get("strategy_key") or "") if isinstance(
            sleeve, dict
        ) else ""
        if (
            not key or key in sleeve_keys
            or _integer((sleeve or {}).get("budget_bp")) <= 0
            or str((sleeve or {}).get("allocation_target_type") or "")
            not in {"STRATEGY", "COMBINATION"}
        ):
            errors.append({"reason": "funded sleeve identity/budget differs"})
            continue
        sleeve_keys.add(key)
    if len(sleeves) > int(GLOBAL_PORTFOLIO_POLICY[
        "maximum_funded_sleeves"
    ]):
        errors.append({"reason": "funded sleeve cap exceeded"})
    expected_sleeves: dict[str, dict[str, Any]] = {}
    for allocation in allocation_rows:
        target_type = str(allocation.get("target_type") or "")
        target_key = str(allocation.get("target_key") or "")
        target_version = str(allocation.get("target_version") or "")
        allocation_weight = _decimal(
            allocation.get("simulated_weight_pct")
        )
        budget_bp = int((allocation_weight or Decimal("0")) * 100)
        if target_type == "STRATEGY" and target_key and budget_bp > 0:
            expected_sleeves[target_key] = {
                "strategy_key": target_key,
                "strategy_version": target_version,
                "allocation_target_type": "STRATEGY",
                "allocation_target_key": target_key,
                "allocation_target_version": target_version,
                "budget_bp": budget_bp,
            }
        elif target_type == "COMBINATION":
            members = _json_array(allocation.get("member_sleeves_json")) or []
            for member in members:
                key = str(member.get("strategy_key") or "") if isinstance(
                    member, dict
                ) else ""
                member_bp = _integer((member or {}).get("effective_bp"))
                if not key or member_bp <= 0 or key in expected_sleeves:
                    errors.append({
                        "reason": "persisted allocation has duplicate/invalid member sleeve",
                        "strategy_key": key,
                    })
                    continue
                expected_sleeves[key] = {
                    "strategy_key": key,
                    "strategy_version": str(
                        (member or {}).get("strategy_version") or ""
                    ),
                    "allocation_target_type": "COMBINATION",
                    "allocation_target_key": target_key,
                    "allocation_target_version": target_version,
                    "budget_bp": member_bp,
                }
    expected_sleeve_rows = [
        expected_sleeves[key] for key in sorted(expected_sleeves)
    ]
    if sleeves != expected_sleeve_rows:
        errors.append({
            "reason": "paper funded sleeves differ from persisted allocations",
            "expected_sleeves": expected_sleeve_rows,
        })

    targets = plan.get("targets")
    if not isinstance(targets, list):
        errors.append({"reason": "paper targets are not a list"})
        targets = []
    target_codes: set[str] = set()
    industry_bp: dict[str, int] = {}
    invested_bp = 0
    actual_turnover_bp = 0
    stock_cap_bp = int(Decimal(str(
        GLOBAL_PORTFOLIO_POLICY["maximum_single_stock_weight_pct"]
    )) * 100)
    industry_cap_bp = int(Decimal(str(
        GLOBAL_PORTFOLIO_POLICY["maximum_industry_weight_pct"]
    )) * 100)
    for target in targets:
        if not isinstance(target, dict):
            errors.append({"reason": "paper target row is not an object"})
            continue
        code = str(target.get("stock_code") or "")
        target_bp = _integer(target.get("target_bp"))
        previous_bp = _integer(target.get("previous_target_bp"))
        delta_bp = _integer(target.get("new_buy_delta_bp"))
        target_payload = {
            str(key): value for key, value in target.items()
            if str(key) != "target_hash"
        }
        target_hash = _canonical_digest({
            "schema": "probiga.governance-paper-target.v1",
            **target_payload,
        })
        expected_industry_binding = industry_bindings.get(code)
        if (
            not code or code in target_codes or target_bp <= 0
            or target_bp > stock_cap_bp or previous_bp < 0
            or delta_bp != max(0, target_bp - previous_bp)
            or str(target.get("strategy_key") or "") not in sleeve_keys
            or target.get("allocation_backed") is not True
            or target.get("new_buy_allowed") is not True
            or target.get("exit_always_allowed") is not True
            or target.get("real_order_authority") is not False
            or str(target.get("target_hash") or "") != target_hash
            or target.get("industry_binding")
            != expected_industry_binding
            or str(target.get("industry_name") or "")
            != str((expected_industry_binding or {}).get(
                "industry_name"
            ) or "")
            or str(target.get("industry_type") or "")
            != str((expected_industry_binding or {}).get(
                "industry_type"
            ) or "")
            or str(target.get("industry_snapshot_id") or "")
            != str((expected_industry_binding or {}).get(
                "snapshot_id"
            ) or "")
            or str(target.get("industry_snapshot_hash") or "")
            != str((expected_industry_binding or {}).get(
                "snapshot_hash"
            ) or "")
            or str(target.get("industry_row_hash") or "")
            != str((expected_industry_binding or {}).get("row_hash") or "")
            or str(target.get("industry_source_fact_id") or "")
            != str((expected_industry_binding or {}).get(
                "source_fact_id"
            ) or "")
        ):
            errors.append({
                "reason": "paper target identity/cap/hash differs",
                "stock_code": code,
            })
        target_codes.add(code)
        invested_bp += target_bp
        actual_turnover_bp += delta_bp
        industry = str(target.get("industry_name") or "")
        industry_bp[industry] = industry_bp.get(industry, 0) + target_bp
    if (
        len(targets) > int(GLOBAL_PORTFOLIO_POLICY[
            "maximum_planned_positions"
        ])
        or any(value > industry_cap_bp for value in industry_bp.values())
        or invested_bp < 0 or invested_bp > 10_000
        or _integer(plan.get("target_count")) != len(targets)
        or _integer(plan.get("invested_bp")) != invested_bp
        or _integer(plan.get("cash_bp")) != 10_000 - invested_bp
    ):
        errors.append({"reason": "paper target aggregate limits differ"})
    if pool_rows is not None:
        persisted_tradable_codes = {
            str(row.get("stock_code") or "")
            for row in pool_rows
            if str(row.get("pool_level") or "") == "TRADABLE"
        }
        if target_codes != persisted_tradable_codes:
            errors.append({
                "reason": "paper targets differ from persisted tradable pool",
                "target_codes": sorted(target_codes),
                "persisted_tradable_codes": sorted(persisted_tradable_codes),
            })
        for row in pool_rows:
            if str(row.get("pool_level") or "") != "TRADABLE":
                continue
            code = str(row.get("stock_code") or "")
            envelope = _json_object(row.get("evidence_json")) or {}
            if envelope.get("industry_binding") != industry_bindings.get(code):
                errors.append({
                    "reason": "paper target and persisted pool industry binding differ",
                    "stock_code": code,
                })

    requested_turnover_bp = _integer(
        plan.get("requested_new_buy_turnover_bp")
    )
    turnover_cap_bp = int(Decimal(str(
        GLOBAL_PORTFOLIO_POLICY["maximum_new_buy_turnover_pct"]
    )) * 100)
    expected_turnover_multiplier = min(
        Decimal("1"),
        Decimal(turnover_cap_bp) / Decimal(requested_turnover_bp)
        if requested_turnover_bp > 0 else Decimal("1"),
    ).quantize(Decimal("0.00000001"))
    if (
        requested_turnover_bp < actual_turnover_bp
        or actual_turnover_bp > turnover_cap_bp
        or _integer(plan.get("actual_new_buy_turnover_bp"))
        != actual_turnover_bp
        or _decimal(plan.get("new_buy_turnover_multiplier"))
        != expected_turnover_multiplier
    ):
        errors.append({"reason": "paper turnover cap/replay differs"})

    exit_targets = plan.get("exit_targets")
    exit_codes: set[str] = set()
    if not isinstance(exit_targets, list):
        errors.append({"reason": "paper exit targets are not a list"})
        exit_targets = []
    for target in exit_targets:
        code = str(target.get("stock_code") or "") if isinstance(
            target, dict
        ) else ""
        if (
            not code or code in exit_codes or code in target_codes
            or _integer((target or {}).get("previous_target_bp")) <= 0
            or _integer((target or {}).get("target_bp")) != 0
            or (target or {}).get("new_buy_allowed") is not False
            or (target or {}).get("exit_always_allowed") is not True
            or (target or {}).get("real_order_authority") is not False
        ):
            errors.append({
                "reason": "paper exit target safety differs",
                "stock_code": code,
            })
        exit_codes.add(code)

    if (
        _integer(summary.get("paper_target_count")) != len(targets)
        or _decimal(summary.get("paper_invested_weight_pct"))
        != Decimal(invested_bp) / Decimal(100)
    ):
        errors.append({"reason": "paper plan summary differs"})
    return not errors, {
        "expected_plan_hash": expected_plan_hash,
        "stored_plan_hash": stored_plan_hash,
        "stored_result_hash": stored_result_hash,
        "target_count": len(targets),
        "invested_bp": invested_bp,
        "actual_new_buy_turnover_bp": actual_turnover_bp,
        "errors": errors[:100],
    }, expected_plan_hash


def _allocation_decision_contract_check(
    run: dict[str, Any],
    route_bindings: dict[tuple[str, str, str], dict[str, Any]],
    allocation_rows: list[dict[str, Any]],
    trade_date: str,
    *,
    pool_snapshot_hash: str | None = None,
    pool_rows: list[dict[str, Any]] | None = None,
    automatic_transition_plan_hash: str | None = None,
    industry_bindings: dict[str, dict[str, Any]] | None = None,
    current_build_commit_sha: str | None = None,
    completed_v5_canonical_count: int | None = None,
) -> tuple[bool, dict[str, Any], list[dict[str, Any]]]:
    candidates, candidate_errors = _allocation_candidate_contract(
        route_bindings
    )
    summary = _json_object(run.get("summary_json")) or {}
    market_state = str(run.get("market_state") or "")
    trading_gate_raw = summary.get("trading_gate_passed")
    trading_gate_passed = trading_gate_raw is True
    risk_cap = MARKET_RISK_CAP_PCT.get(market_state)
    declared_policy_version = str(
        summary.get("allocation_policy_version") or ""
    )
    current_build_sha = str(current_build_commit_sha or "")
    run_build_sha = str(run.get("build_commit_sha") or "")
    legacy_v4_allowed = bool(
        declared_policy_version == LEGACY_ALLOCATION_POLICY_VERSION
        and BUILD_SHA_RE.fullmatch(current_build_sha)
        and BUILD_SHA_RE.fullmatch(run_build_sha)
        and run_build_sha != current_build_sha
        and completed_v5_canonical_count == 0
        and trade_date < ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE
        and _iso_date(run.get("finished_at"))
        < ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE
    )
    declared_policy_valid = bool(
        declared_policy_version == ALLOCATION_POLICY_VERSION
        or legacy_v4_allowed
    )
    candidate_payload = {
        "schema": "probiga.strategy-allocation-candidate-set.v1",
        "allocation_policy_version": declared_policy_version,
        "trade_date": trade_date,
        "market_state": market_state,
        "candidates": candidates,
    }
    candidate_hash = _canonical_digest(candidate_payload)
    allocation_replay_errors: list[dict[str, Any]] = []
    try:
        expected_allocations = _expected_allocation_snapshot(
            candidates,
            market_state=market_state,
            trading_gate_passed=trading_gate_passed,
            allocation_policy_version=declared_policy_version,
        )
    except (ArithmeticError, KeyError, RuntimeError, TypeError, ValueError):
        expected_allocations = []
        allocation_replay_errors.append(
            {"reason": "allocation replay inputs or policy are invalid"}
        )
    stored_allocations = _stored_allocation_snapshot(allocation_rows)
    allocation_payload = {
        "schema": "probiga.strategy-allocation-snapshot.v1",
        "allocation_policy_version": declared_policy_version,
        "trade_date": trade_date,
        "market_state": market_state,
        "market_risk_cap_pct": float(risk_cap or 0),
        "trading_gate_passed": trading_gate_passed,
        "candidate_set_hash": candidate_hash,
        "allocations": stored_allocations,
    }
    allocation_hash = _canonical_digest(allocation_payload)
    strategy_candidates = [
        row for row in candidates if row["target_type"] == "STRATEGY"
    ]
    lane_order = {
        "ACTIVE": 0,
        "REDUCE": 0,
        "SHADOW": 1,
        "SUSPENDED": 2,
        "RETIRED": 3,
    }
    strategy_candidates.sort(
        key=lambda row: (
            lane_order.get(row["lifecycle_status"], 9),
            -float(row["ranking_score"]),
            row["target_key"],
        )
    )
    combination_candidates = [
        row for row in candidates if row["target_type"] == "COMBINATION"
    ]
    combination_candidates.sort(
        key=lambda row: (-float(row["ranking_score"]), row["target_key"])
    )
    canonical_result = _json_object(run.get("result_json")) or {}
    has_canonical_result = bool(canonical_result)
    stored_candidate_contract = canonical_result.get(
        "allocation_candidate_set"
    )
    if not isinstance(stored_candidate_contract, list):
        stored_candidate_contract = []
    try:
        if not has_canonical_result:
            raise LookupError("legacy allocation replay without full result")
        from server.engine import strategy_governance as governance

        decision_contract = str(
            canonical_result.get("decision_contract_version") or ""
        )
        if decision_contract != governance.STATISTICAL_DECISION_CONTRACT:
            raise ValueError("canonical result is not the v7 funding contract")
        statistical_extension = governance._canonical_v7_statistical_extension(
            canonical_result,
            canonical_result.get("strategies") or [],
            canonical_result.get("combinations") or [],
        )
    except LookupError:
        decision_contract = "strategy-governance-decision.v6"
        statistical_extension = {
            "top": {},
            "strategies": [{} for _ in strategy_candidates],
            "combinations": [{} for _ in combination_candidates],
        }
    except Exception as exc:
        decision_contract = ""
        statistical_extension = {
            "top": {},
            "strategies": [{} for _ in strategy_candidates],
            "combinations": [{} for _ in combination_candidates],
        }
        allocation_replay_errors.append({
            "reason": "canonical v7 statistical extension is invalid",
            "detail": _safe_exception_message(exc),
        })
    bound_pool_snapshot_hash = (
        str(pool_snapshot_hash)
        if pool_snapshot_hash is not None
        else str(summary.get("pool_snapshot_hash") or "")
    )
    bound_transition_plan_hash = (
        str(automatic_transition_plan_hash)
        if automatic_transition_plan_hash is not None
        else str(summary.get("automatic_transition_plan_hash") or "")
    )
    verify_paper_plan = pool_rows is not None or bool(run.get("result_json"))
    if verify_paper_plan:
        paper_plan_ok, paper_plan_detail, paper_plan_hash = (
            _paper_execution_plan_contract_check(
                run,
                trade_date=trade_date,
                pool_rows=pool_rows,
                allocation_rows=allocation_rows,
                industry_bindings=industry_bindings,
            )
        )
    else:
        paper_plan_ok = True
        paper_plan_hash = str(
            summary.get("paper_execution_plan_hash") or ""
        )
        paper_plan_detail = {
            "verification_skipped": True,
            "target_count": 0,
            "invested_bp": 0,
            "errors": [],
        }
    decision_payload = {
        "schema": decision_contract,
        "trade_date": trade_date,
        "build_commit_sha": str(run.get("build_commit_sha") or ""),
        "input_hash": str(run.get("input_hash") or ""),
        "router_snapshot_hash": str(run.get("router_snapshot_hash") or ""),
        "allocation_policy_version": declared_policy_version,
        "trading_gate_passed": trading_gate_passed,
        "market_risk_cap_pct": float(risk_cap or 0),
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": sum(
            row["paper_allocation_eligible"] is True for row in candidates
        ),
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "paper_execution_plan_hash": paper_plan_hash,
        "pool_snapshot_hash": bound_pool_snapshot_hash,
        "candidate_industry_snapshot_hash": str(
            summary.get("candidate_industry_snapshot_hash") or ""
        ),
        **({
            "funding_checkpoint_manifest_hash": str(
                summary.get("funding_checkpoint_manifest_hash") or ""
            ),
            **statistical_extension["top"],
        } if has_canonical_result else {}),
        "strategies": [
            {
                "strategy_key": row["target_key"],
                "strategy_version": row["target_version"],
                "enabled": row["enabled"],
                "projected_status": row["lifecycle_status"],
                **(
                    statistical_extension["strategies"][index]
                    if has_canonical_result else {}
                ),
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for index, row in enumerate(strategy_candidates)
        ],
        "combinations": [
            {
                "combination_key": row["target_key"],
                "combination_version": row["target_version"],
                "enabled": row["enabled"],
                "projected_status": row["lifecycle_status"],
                **(
                    statistical_extension["combinations"][index]
                    if has_canonical_result else {}
                ),
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for index, row in enumerate(combination_candidates)
        ],
    }
    decision_hash = _canonical_digest(decision_payload)
    eligible_count = sum(
        row["paper_allocation_eligible"] is True for row in candidates
    )
    expected_allocation_count = sum(
        row["target_type"] != "CASH" for row in expected_allocations
    )
    expected_cash_weight = next(
        (
            _decimal(row.get("simulated_weight_pct"))
            for row in expected_allocations
            if row["target_type"] == "CASH"
        ),
        None,
    )
    errors = [*candidate_errors, *allocation_replay_errors]
    if not paper_plan_ok:
        errors.append({
            "reason": "paper execution plan contract differs",
            "detail": paper_plan_detail,
        })
    summary_valid = (
        declared_policy_valid
        and type(trading_gate_raw) is bool
        and _decimal(summary.get("market_risk_cap_pct")) == risk_cap
        and _integer(summary.get("allocation_candidate_count"))
        == len(candidates)
        and summary.get("eligible_candidate_count") is not None
        and _integer(summary.get("eligible_candidate_count"))
        == eligible_count
        and _integer(summary.get("allocation_count"))
        == expected_allocation_count
        and _decimal(summary.get("cash_weight_pct"))
        == expected_cash_weight
        and str(summary.get("candidate_set_hash") or "") == candidate_hash
        and str(summary.get("allocation_snapshot_hash") or "")
        == allocation_hash
        and _integer(summary.get("pool_row_count")) >= 0
        and str(summary.get("pool_snapshot_hash") or "")
        == bound_pool_snapshot_hash
        and RESULT_HASH_RE.fullmatch(bound_pool_snapshot_hash) is not None
        and _integer(summary.get("automatic_transition_count")) >= 0
        and str(summary.get("automatic_transition_plan_hash") or "")
        == bound_transition_plan_hash
        and RESULT_HASH_RE.fullmatch(bound_transition_plan_hash) is not None
        and (
            not has_canonical_result
            or (
                summary.get("decision_contract_version")
                == decision_contract
                and all(
                    summary.get(field) == value
                    for field, value
                    in statistical_extension["top"].items()
                )
                and RESULT_HASH_RE.fullmatch(str(
                    summary.get("funding_checkpoint_manifest_hash") or ""
                )) is not None
            )
        )
        and (
            not verify_paper_plan
            or (
                str(summary.get("paper_execution_plan_hash") or "")
                == paper_plan_hash
                and _integer(summary.get("paper_target_count"))
                == _integer(paper_plan_detail.get("target_count"))
                and _decimal(summary.get("paper_invested_weight_pct"))
                == Decimal(_integer(paper_plan_detail.get("invested_bp")))
                / Decimal(100)
            )
        )
    )
    if not summary_valid:
        errors.append({"reason": "allocation summary contract differs"})
    if stored_allocations != expected_allocations:
        errors.append({"reason": "persisted allocation replay differs"})
    if has_canonical_result and stored_candidate_contract != candidates:
        errors.append({"reason": "canonical candidate contract differs"})
    if str(run.get("decision_hash") or "") != decision_hash:
        errors.append({"reason": "governance decision v7 hash differs"})
    return not errors, {
        "allocation_policy_version": declared_policy_version,
        "legacy_v4_allowed": legacy_v4_allowed,
        "current_build_commit_sha": current_build_sha,
        "run_build_commit_sha": run_build_sha,
        "completed_v5_canonical_count": completed_v5_canonical_count,
        "v5_effective_trade_date": (
            ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE
        ),
        "trading_gate_passed": trading_gate_raw,
        "market_risk_cap_pct": summary.get("market_risk_cap_pct"),
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": eligible_count,
        "expected_candidate_set_hash": candidate_hash,
        "stored_candidate_set_hash": summary.get("candidate_set_hash"),
        "expected_allocation_snapshot_hash": allocation_hash,
        "stored_allocation_snapshot_hash": summary.get(
            "allocation_snapshot_hash"
        ),
        "expected_pool_snapshot_hash": bound_pool_snapshot_hash,
        "stored_pool_snapshot_hash": summary.get("pool_snapshot_hash"),
        "expected_automatic_transition_plan_hash": (
            bound_transition_plan_hash
        ),
        "stored_automatic_transition_plan_hash": summary.get(
            "automatic_transition_plan_hash"
        ),
        "paper_execution_plan": paper_plan_detail,
        "expected_decision_hash": decision_hash,
        "stored_decision_hash": run.get("decision_hash"),
        "expected_allocations": expected_allocations,
        "stored_allocations": stored_allocations,
        "errors": errors,
    }, expected_allocations


def _confirmed_funding_evidence(
    connection, run_uid: str, trade_date: str
) -> tuple[bool, dict[str, Any]]:
    """Prove every passing snapshot is bound to reviewed immutable evidence."""

    strategy_rows = _rows(
        connection,
        "SELECT strategy_key AS entity_key, "
        "strategy_version AS entity_version, window_days, "
        "trade_date, profit_gate_passed, recommended_status, "
        "evidence_json, result_hash "
        "FROM st_strategy_health_snapshot WHERE run_uid=:run_uid",
        {"run_uid": run_uid},
    )
    combination_rows = _rows(
        connection,
        "SELECT combination_key AS entity_key, "
        "combination_version AS entity_version, "
        "trade_date, profit_gate_passed, recommended_status, "
        "evidence_json, result_hash "
        "FROM st_strategy_combination_health_snapshot "
        "WHERE run_uid=:run_uid",
        {"run_uid": run_uid},
    )
    references: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def append_reference(
        *,
        entity_type: str,
        entity_key: str,
        entity_version: str,
        window_days: int,
        metrics: Any,
    ) -> None:
        if not isinstance(metrics, dict):
            errors.append(
                {
                    "entity_type": entity_type,
                    "entity_key": entity_key,
                    "window_days": window_days,
                    "reason": "missing embedded metrics",
                }
            )
            return
        evidence_hash = str(metrics.get("evidence_hash") or "")
        selection_evidence_hash = str(
            metrics.get("selection_evidence_hash") or ""
        )
        internal_trade_evidence_hash = str(
            metrics.get("internal_trade_evidence_hash") or ""
        )
        internal_ledger_hash = str(
            metrics.get("internal_ledger_hash") or ""
        )
        artifact_hash = str(metrics.get("artifact_hash") or "")
        composite_payload = {
            "internal_trade_evidence_hash": internal_trade_evidence_hash,
            "internal_ledger_hash": internal_ledger_hash,
            "selection_evidence_hash": selection_evidence_hash,
            "selection_artifact_hash": artifact_hash,
            (
                "strategy_key"
                if entity_type == "STRATEGY"
                else "combination_key"
            ): entity_key,
            (
                "strategy_version"
                if entity_type == "STRATEGY"
                else "combination_version"
            ): entity_version,
            "window_days": window_days,
        }
        if (
            metrics.get("verification_status") != "CONFIRMED"
            or not RESULT_HASH_RE.fullmatch(evidence_hash)
            or not RESULT_HASH_RE.fullmatch(selection_evidence_hash)
            or not RESULT_HASH_RE.fullmatch(internal_trade_evidence_hash)
            or metrics.get("funding_provenance")
            != CANONICAL_FUNDING_PROVENANCE
            or not RESULT_HASH_RE.fullmatch(internal_ledger_hash)
            or not RESULT_HASH_RE.fullmatch(artifact_hash)
            or _canonical_digest(composite_payload) != evidence_hash
            or metrics.get("drawdown_basis")
            != "internal_version_bound_portfolio_equity"
            or metrics.get("cost_basis") != "actual_ledger_fees"
        ):
            errors.append(
                {
                    "entity_type": entity_type,
                    "entity_key": entity_key,
                    "window_days": window_days,
                    "evidence_hash": evidence_hash,
                    "reason": (
                        "passing snapshot lacks confirmed selection evidence "
                        "or internal portfolio-ledger economics"
                    ),
                }
            )
            return
        references.append(
            {
                "entity_type": entity_type,
                "entity_key": entity_key,
                "entity_version": entity_version,
                "window_days": window_days,
                "evidence_hash": selection_evidence_hash,
                "composite_evidence_hash": evidence_hash,
                "internal_trade_evidence_hash": (
                    internal_trade_evidence_hash
                ),
                "internal_ledger_hash": internal_ledger_hash,
                "embedded_metrics": metrics,
            }
        )

    for row in strategy_rows:
        payload = _json_object(row.get("evidence_json"))
        gate = (payload or {}).get("gate")
        snapshot_valid = (
            payload is not None
            and _canonical_digest(payload) == str(row.get("result_hash") or "")
            and str(payload.get("strategy_key") or "")
            == str(row.get("entity_key") or "")
            and str(payload.get("strategy_version") or "")
            == str(row.get("entity_version") or "")
            and _iso_date(payload.get("trade_date")) == trade_date
            and _iso_date(row.get("trade_date")) == trade_date
            and _integer(payload.get("window_days"))
            == _integer(row.get("window_days"))
            and isinstance(gate, dict)
            and gate.get("passed")
            is (_integer(row.get("profit_gate_passed")) == 1)
            and type(payload.get("overall_profit_gate_passed")) is bool
            and bool(
                RESULT_HASH_RE.fullmatch(
                    str(payload.get("funding_gate_hash") or "")
                )
            )
        )
        if not snapshot_valid:
            errors.append(
                {
                    "entity_type": "STRATEGY",
                    "entity_key": row.get("entity_key"),
                    "window_days": row.get("window_days"),
                    "reason": "strategy snapshot payload or result hash is invalid",
                }
            )
            continue
        if (
            _integer(row.get("profit_gate_passed")) != 1
            and payload.get("overall_profit_gate_passed") is not True
        ):
            continue
        append_reference(
            entity_type="STRATEGY",
            entity_key=str(row.get("entity_key") or ""),
            entity_version=str(row.get("entity_version") or ""),
            window_days=_integer(row.get("window_days")),
            metrics=(payload or {}).get("metrics"),
        )
    for row in combination_rows:
        payload = _json_object(row.get("evidence_json"))
        metrics_by_window = (payload or {}).get("metrics")
        snapshot_valid = (
            payload is not None
            and _canonical_digest(payload) == str(row.get("result_hash") or "")
            and str(payload.get("combination_key") or "")
            == str(row.get("entity_key") or "")
            and str(payload.get("combination_version") or "")
            == str(row.get("entity_version") or "")
            and _iso_date(payload.get("trade_date")) == trade_date
            and _iso_date(row.get("trade_date")) == trade_date
            and payload.get("overall_profit_gate_passed")
            is (_integer(row.get("profit_gate_passed")) == 1)
            and bool(
                RESULT_HASH_RE.fullmatch(
                    str(payload.get("funding_gate_hash") or "")
                )
            )
            and isinstance(metrics_by_window, dict)
            and set(metrics_by_window) == {"20", "60", "120"}
            and all(
                isinstance(metrics_by_window.get(str(window)), dict)
                and _integer(
                    metrics_by_window[str(window)].get("window_days")
                )
                == window
                for window in EXPECTED_WINDOWS
            )
        )
        if not snapshot_valid:
            errors.append(
                {
                    "entity_type": "COMBINATION",
                    "entity_key": row.get("entity_key"),
                    "reason": "combination snapshot payload or result hash is invalid",
                }
            )
            continue
        if _integer(row.get("profit_gate_passed")) != 1:
            continue
        for window_days in EXPECTED_WINDOWS:
            metrics = (
                metrics_by_window.get(str(window_days))
                if isinstance(metrics_by_window, dict)
                else None
            )
            append_reference(
                entity_type="COMBINATION",
                entity_key=str(row.get("entity_key") or ""),
                entity_version=str(row.get("entity_version") or ""),
                window_days=window_days,
                metrics=metrics,
            )

    evidence_by_hash: dict[str, dict[str, Any]] = {}
    if references:
        hashes = sorted({item["evidence_hash"] for item in references})
        bind = {f"evidence_hash_{index}": value for index, value in enumerate(hashes)}
        placeholders = ",".join(f":{key}" for key in bind)
        evidence_rows = _rows(
            connection,
            "SELECT i.evidence_id, i.entity_type, i.strategy_key, "
            "i.strategy_version, i.as_of_date, i.window_days, "
            "i.metrics_json, i.source, i.evidence_protocol, "
            "i.artifact_hash, i.artifact_json, "
            "i.source_dataset_hash, i.evidence_revision_at, "
            "i.verification_status, i.funding_provenance, "
            "i.submitted_by, i.reviewed_by, "
            "i.reviewed_at, i.evidence_hash, "
            "COALESCE(sv.created_at, cv.created_at) AS version_frozen_at, "
            "CASE WHEN i.evidence_revision_at >= "
            "COALESCE(sv.created_at, cv.created_at) "
            "THEN 1 ELSE 0 END AS evidence_after_version_freeze "
            "FROM st_strategy_metric_input i "
            "LEFT JOIN st_strategy_version sv "
            "ON i.entity_type='STRATEGY' "
            "AND sv.strategy_key=i.strategy_key "
            "AND sv.version=i.strategy_version "
            "LEFT JOIN st_strategy_combination_version cv "
            "ON i.entity_type='COMBINATION' "
            "AND cv.combination_key=i.strategy_key "
            "AND cv.version=i.strategy_version "
            f"WHERE i.evidence_hash IN ({placeholders})",
            bind,
        )
        evidence_by_hash = {
            str(row.get("evidence_hash") or ""): row for row in evidence_rows
        }

    for reference in references:
        evidence = evidence_by_hash.get(reference["evidence_hash"])
        if evidence is None:
            errors.append({**reference, "reason": "evidence row is missing"})
            continue
        artifact = _json_object(evidence.get("artifact_json"))
        as_of_date = _iso_date(evidence.get("as_of_date"))
        revision_date = _iso_date(evidence.get("evidence_revision_at"))
        valid = (
            str(evidence.get("entity_type") or "") == reference["entity_type"]
            and str(evidence.get("strategy_key") or "")
            == reference["entity_key"]
            and str(evidence.get("strategy_version") or "")
            == reference["entity_version"]
            and _integer(evidence.get("window_days"))
            == reference["window_days"]
            and bool(as_of_date)
            and as_of_date <= trade_date
            and bool(revision_date)
            and revision_date <= trade_date
            and str(evidence.get("verification_status") or "") == "CONFIRMED"
            and str(evidence.get("funding_provenance") or "")
            == "EXTERNAL_SUBMITTED"
            and str(evidence.get("evidence_protocol") or "")
            in VERIFIED_WALK_FORWARD_PROTOCOLS
            and bool(
                RESULT_HASH_RE.fullmatch(
                    str(evidence.get("artifact_hash") or "")
                )
            )
            and bool(
                RESULT_HASH_RE.fullmatch(
                    str(evidence.get("source_dataset_hash") or "")
                )
            )
            and artifact is not None
            and bool(str(evidence.get("submitted_by") or ""))
            and bool(str(evidence.get("reviewed_by") or ""))
            and str(evidence.get("submitted_by") or "")
            != str(evidence.get("reviewed_by") or "")
            and evidence.get("reviewed_at") is not None
            and evidence.get("version_frozen_at") is not None
            and _integer(evidence.get("evidence_after_version_freeze")) == 1
        )
        artifact_error = (
            _validate_metric_artifact_binding(connection, evidence, reference)
            if valid
            else ""
        )
        valid = valid and not artifact_error
        if not valid:
            errors.append(
                {
                    **reference,
                    "evidence_id": evidence.get("evidence_id"),
                    "verification_status": evidence.get(
                        "verification_status"
                    ),
                    "reason": artifact_error
                    or "confirmed evidence binding is invalid",
                }
            )
    return not errors, {
        "passing_snapshot_evidence_references": len(references),
        "distinct_evidence_rows": len(evidence_by_hash),
        "invalid_count": len(errors),
        "errors": errors[:100],
    }


def _json_clone(value: Any) -> Any:
    """Normalize an in-memory replay exactly as it would be stored in JSON."""

    return json.loads(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ))


def _metric_snapshot_replay_contract(connection, run_uid: str) -> dict[str, Any]:
    strategies: dict[str, dict[str, Any]] = {}
    for row in _rows(
        connection,
        "SELECT run_uid, strategy_key, strategy_version, trade_date, "
        "window_days, profit_gate_passed, recommended_status, "
        "evidence_json, result_hash "
        "FROM st_strategy_health_snapshot WHERE run_uid=:run_uid "
        "ORDER BY BINARY strategy_key, BINARY strategy_version, window_days",
        {"run_uid": run_uid},
    ):
        if str(row.get("run_uid") or run_uid) != run_uid:
            continue
        payload = _json_object(row.get("evidence_json"))
        if payload is None:
            continue
        key = "|".join((
            "STRATEGY",
            str(row.get("strategy_key") or ""),
            str(row.get("strategy_version") or ""),
            str(_integer(row.get("window_days"))),
        ))
        strategies[key] = {
            "metrics": payload.get("metrics"),
            "gate": payload.get("gate"),
            "overall_profit_gate_passed": payload.get(
                "overall_profit_gate_passed"
            ),
            "funding_gate_hash": payload.get(
                "pre_confirmation_funding_gate_hash"
            ),
            "market_route": payload.get("market_route"),
        }
    combinations: dict[str, dict[str, Any]] = {}
    for row in _rows(
        connection,
        "SELECT run_uid, combination_key, combination_version, trade_date, "
        "profit_gate_passed, recommended_status, evidence_json, result_hash "
        "FROM st_strategy_combination_health_snapshot "
        "WHERE run_uid=:run_uid ORDER BY BINARY combination_key, "
        "BINARY combination_version",
        {"run_uid": run_uid},
    ):
        if str(row.get("run_uid") or run_uid) != run_uid:
            continue
        payload = _json_object(row.get("evidence_json"))
        if payload is None:
            continue
        key = "|".join((
            "COMBINATION",
            str(row.get("combination_key") or ""),
            str(row.get("combination_version") or ""),
        ))
        combinations[key] = {
            "metrics": payload.get("metrics"),
            "multi_window_gate": payload.get("multi_window_gate"),
            "overall_profit_gate_passed": payload.get(
                "overall_profit_gate_passed"
            ),
            "funding_gate_hash": payload.get(
                "pre_confirmation_funding_gate_hash"
            ),
            "market_route": payload.get("market_route"),
        }
    return {"strategies": strategies, "combinations": combinations}


def _replay_current_metrics_from_raw(
    connection,
    *,
    run_uid: str,
    trade_date: str,
    authoritative_windows: dict[int, dict[str, Any]],
    snapshot_contract: dict[str, Any],
) -> dict[str, Any]:
    """Re-run the production metric path against raw immutable trading facts."""

    # Unit-test engines may provide an explicit replay fixture.  A production
    # SQLAlchemy Engine has no such hook and always executes the raw DB path.
    fixture = getattr(
        getattr(connection, "engine", None),
        "_strategy_governance_raw_replay_fixture",
        None,
    )
    if callable(fixture):
        return fixture(
            snapshot_contract=deepcopy(snapshot_contract),
            run_uid=run_uid,
            trade_date=trade_date,
            authoritative_windows=deepcopy(authoritative_windows),
        )

    from server.engine import strategy_governance as governance

    def connection_read(sql: str, params: dict[str, Any] | None = None):
        return _rows(connection, sql, params or {})

    with _RAW_METRIC_REPLAY_LOCK, governance.bind_sql_connection(connection):
        original_reader = governance._db_read
        governance._db_read = connection_read
        try:
            registry = governance.load_registry()
            strategy_routes: dict[tuple[str, str], dict[str, Any]] = {}
            for key, item in snapshot_contract["strategies"].items():
                _kind, entity_key, version, _window = key.split("|", 3)
                route = item.get("market_route")
                if isinstance(route, dict):
                    strategy_routes[(entity_key, version)] = route
            for strategy in registry:
                identity = (
                    str(strategy.get("strategy_key") or ""),
                    str(strategy.get("current_version") or ""),
                )
                strategy["market_route"] = deepcopy(
                    strategy_routes.get(identity, {})
                )
            metrics = governance._metrics_for_registry(
                {},
                registry,
                trade_date,
                authoritative_windows=authoritative_windows,
            )
            statistical_inventory = (
                governance._load_statistical_trial_inventory()
            )
            strategy_decisions, _strategy_fdr_summary = (
                governance._statistical_family_fdr_decisions(
                    entity_type="STRATEGY",
                    current_rows=registry,
                    metrics_by_key=metrics,
                    family=statistical_inventory["strategy"],
                )
            )
            rankings = governance._strategy_rankings(
                registry,
                metrics,
                statistical_decisions=strategy_decisions,
            )
            projected_rankings = governance._canonical_competition_rows(
                rankings, entity_type="STRATEGY",
            )
            replay_strategies: dict[str, dict[str, Any]] = {}
            for strategy, projected_strategy in zip(
                rankings, projected_rankings, strict=True,
            ):
                for window in governance.WINDOWS:
                    window_metrics = strategy["metrics"][str(window)]
                    key = "|".join((
                        "STRATEGY",
                        str(strategy.get("strategy_key") or ""),
                        str(strategy.get("current_version") or ""),
                        str(window),
                    ))
                    replay_strategies[key] = {
                        "metrics": projected_strategy["metrics"][str(window)],
                        "gate": window_metrics.get("profit_gate"),
                        "overall_profit_gate_passed": strategy.get(
                            "profit_gate_passed"
                        ),
                        "funding_gate_hash": strategy.get("funding_gate_hash"),
                        "market_route": strategy.get("market_route"),
                    }

            combination_registry = governance.load_combinations()
            combination_versions = {
                str(row.get("combination_key") or ""): str(
                    row.get("current_version") or ""
                )
                for row in combination_registry
            }
            combination_inputs = governance._load_metric_inputs(
                trade_date,
                entity_type="COMBINATION",
                current_versions=combination_versions,
            )
            combination_statistical_context: dict[str, Any] = {}
            combination_rankings = governance._combination_rankings(
                combination_registry,
                rankings,
                combination_inputs,
                trade_date,
                statistical_family=statistical_inventory["combination"],
                statistical_context=combination_statistical_context,
            )
            projected_combinations = governance._canonical_competition_rows(
                combination_rankings, entity_type="COMBINATION",
            )
            replay_combinations: dict[str, dict[str, Any]] = {}
            for combination, projected_combination in zip(
                combination_rankings, projected_combinations, strict=True,
            ):
                key = "|".join((
                    "COMBINATION",
                    str(combination.get("combination_key") or ""),
                    str(combination.get("current_version") or ""),
                ))
                replay_combinations[key] = {
                    "metrics": projected_combination.get("metrics"),
                    "multi_window_gate": combination.get(
                        "multi_window_gate"
                    ),
                    "overall_profit_gate_passed": combination.get(
                        "profit_gate_passed"
                    ),
                    "funding_gate_hash": combination.get("funding_gate_hash"),
                    "market_route": combination.get("market_route"),
                }
            return _json_clone({
                "strategies": replay_strategies,
                "combinations": replay_combinations,
            })
        finally:
            governance._db_read = original_reader


def _contract_differences(
    expected: Any,
    actual: Any,
    *,
    path: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    def walk(left: Any, right: Any, current: str) -> None:
        if len(errors) >= limit:
            return
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(set(left) | set(right), key=str):
                child = f"{current}.{key}" if current else str(key)
                if key not in left or key not in right:
                    errors.append({
                        "path": child,
                        "reason": "missing or unexpected replay field",
                    })
                else:
                    walk(left[key], right[key], child)
            return
        if isinstance(left, list) and isinstance(right, list):
            if len(left) != len(right):
                errors.append({
                    "path": current,
                    "reason": "replay list length differs",
                    "expected_length": len(left),
                    "actual_length": len(right),
                })
                return
            for index, (left_item, right_item) in enumerate(zip(left, right)):
                walk(left_item, right_item, f"{current}[{index}]")
            return
        if _canonical_digest(left) != _canonical_digest(right):
            errors.append({
                "path": current,
                "reason": "raw-ledger replay value differs",
                "expected_hash": _canonical_digest(left),
                "actual_hash": _canonical_digest(right),
            })

    walk(expected, actual, path)
    return errors


def _current_canonical_raw_metric_replay_check(
    connection,
    *,
    run_uid: str,
    trade_date: str,
    authoritative_windows: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """Bind current 20/60/120 snapshots to intent/fill/cash/QMT replay."""

    expected = _metric_snapshot_replay_contract(connection, run_uid)
    normalized_windows = {
        int(window): dict(binding)
        for window, binding in (authoritative_windows or {}).items()
        if str(window).isdigit() and isinstance(binding, dict)
    }
    try:
        actual = _replay_current_metrics_from_raw(
            connection,
            run_uid=run_uid,
            trade_date=trade_date,
            authoritative_windows=normalized_windows,
            snapshot_contract=expected,
        )
        actual = _json_clone(actual)
        expected = _json_clone(expected)
        errors = _contract_differences(expected, actual)
    except Exception as exc:
        errors = [{
            "path": "raw_metric_replay",
            "reason": (
                "raw metric replay could not be completed: "
                + _safe_exception_message(exc)
            ),
        }]
        actual = {"strategies": {}, "combinations": {}}
    return not errors, {
        "run_uid": run_uid,
        "trade_date": trade_date,
        "strategy_window_count": len(expected.get("strategies") or {}),
        "combination_count": len(expected.get("combinations") or {}),
        "replayed_strategy_window_count": len(
            actual.get("strategies") or {}
        ),
        "replayed_combination_count": len(
            actual.get("combinations") or {}
        ),
        "invalid_count": len(errors),
        "errors": errors[:100],
        "source_contract": (
            "st_trade_intent_v2/st_order_v2/st_fill_v2/st_cash_ledger_v2/"
            "st_forward_exit_allocation_v3/QMT-attested sm_stock_kline"
        ),
    }


def collect_governance_health(
    engine,
    *,
    expected_build_sha: str = "",
    expected_trade_date: str = "",
    allow_input_not_ready: bool = False,
    expected_scheduler_pid: int = 0,
) -> dict[str, Any]:
    """Collect a read-only, build-bound production acceptance report."""

    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any, *, waived: bool = False) -> None:
        checks.append(
            {
                "name": name,
                "passed": bool(passed),
                "waived": bool(waived),
                "detail": detail,
            }
        )

    build_sha = _resolve_build_sha(expected_build_sha)
    authoritative_date = ""
    date_source = ""
    date_error = ""
    try:
        authoritative_date, date_source = _authoritative_trade_date(
            engine, expected_trade_date
        )
    except Exception as exc:
        date_error = _safe_exception_message(
            exc, error_code="authoritative_trade_date_failed"
        )

    run_disposition = "unverified"
    with engine.connect() as connection:
        existing, schema_ok = _schema_checks(connection, add)
        scheduler_ok = _scheduler_checks(connection, existing, add)
        qmt_announcement_scheduler_ok = _qmt_announcement_scheduler_checks(
            connection,
            existing,
            add,
        )
        qmt_operations_scheduler_ok = _qmt_operations_scheduler_checks(
            connection,
            existing,
            add,
        )
        scheduler_ok = (
            scheduler_ok
            and qmt_announcement_scheduler_ok
            and qmt_operations_scheduler_ok
        )
        if expected_scheduler_pid:
            heartbeat_ok, heartbeat_detail = (
                check_linux_standalone_scheduler_heartbeat(
                    connection,
                    expected_build_sha=build_sha,
                    expected_pid=expected_scheduler_pid,
                )
            )
            add(
                "linux_standalone_scheduler_heartbeat_current",
                heartbeat_ok,
                heartbeat_detail,
            )
            scheduler_ok = scheduler_ok and heartbeat_ok
        qmt_edge_ok, qmt_edge_detail = check_qmt_windows_edge_executor(
            connection,
            expected_build_sha=build_sha,
        )
        add(
            "qmt_windows_edge_executor_and_last_success",
            qmt_edge_ok,
            qmt_edge_detail,
        )
        scheduler_ok = scheduler_ok and qmt_edge_ok
        qmt_release_ok, qmt_release_detail = (
            check_qmt_windows_edge_release_receipt(
                connection,
                expected_build_sha=build_sha,
            )
        )
        add(
            "qmt_windows_edge_release_bootstrap",
            qmt_release_ok,
            qmt_release_detail,
        )
        scheduler_ok = scheduler_ok and qmt_release_ok
        task_history_ok, task_history_detail = (
            _scheduler_task_history_frozen_schema_check(engine)
        )
        add(
            "scheduler_task_history_physical_schema",
            task_history_ok,
            task_history_detail,
        )
        schema_ok = schema_ok and task_history_ok
        supporting_triggers_ok, supporting_triggers_detail = (
            _supporting_release_trigger_inventory_check(connection)
        )
        add(
            "supporting_release_trigger_inventory_exact",
            supporting_triggers_ok,
            supporting_triggers_detail,
        )
        schema_ok = schema_ok and supporting_triggers_ok
        full_triggers_ok, full_triggers_detail = (
            _full_database_trigger_inventory_check(connection)
        )
        add(
            "full_database_trigger_inventory_exact",
            full_triggers_ok,
            full_triggers_detail,
        )
        schema_ok = schema_ok and full_triggers_ok
        qmt_reference_ok, qmt_reference_detail = (
            _qmt_reference_frozen_schema_check(engine)
        )
        add(
            "qmt_reference_physical_schema_and_seal",
            qmt_reference_ok,
            qmt_reference_detail,
        )
        schema_ok = schema_ok and qmt_reference_ok
        qmt_coverage_ok, qmt_coverage_detail = (
            _qmt_history_coverage_frozen_schema_check(connection)
        )
        add(
            "qmt_history_coverage_physical_schema_and_seal",
            qmt_coverage_ok,
            qmt_coverage_detail,
        )
        schema_ok = schema_ok and qmt_coverage_ok
        capability_ok, capability_detail = (
            _qmt_history_capability_matrix_check(connection)
        )
        add(
            "qmt_history_capability_matrix_fail_closed",
            capability_ok,
            capability_detail,
        )
        schema_ok = schema_ok and capability_ok
        pit_fact_schema_ok, pit_fact_schema_detail = (
            _pit_fact_frozen_schema_check(engine)
        )
        add(
            "pit_fact_physical_schema_exact",
            pit_fact_schema_ok,
            pit_fact_schema_detail,
        )
        schema_ok = schema_ok and pit_fact_schema_ok
        metric_integrity_ok, metric_integrity_detail = (
            _metric_input_review_trigger_check(connection)
        )
        add(
            "strategy_metric_input_application_state_machine",
            metric_integrity_ok,
            metric_integrity_detail,
        )
        schema_ok = schema_ok and metric_integrity_ok
        funding_schema_ok, funding_schema_detail = (
            _strategy_funding_schema_check(connection)
        )
        add(
            "strategy_funding_schema_exact",
            funding_schema_ok,
            funding_schema_detail,
        )
        schema_ok = schema_ok and funding_schema_ok
        append_only_ok, append_only_detail = (
            _governance_append_only_trigger_check(
                connection,
                funding_schema_detail if funding_schema_ok else None,
            )
        )
        add(
            "governance_append_only_application_integrity",
            append_only_ok,
            append_only_detail,
        )
        schema_ok = schema_ok and append_only_ok
        dynamic_tables = DYNAMIC_SHADOW_TABLES
        if dynamic_tables <= existing:
            try:
                dynamic_constraints_ok, dynamic_constraints_detail = (
                    _dynamic_shadow_schema_constraints_check(connection)
                )
            except Exception as exc:
                dynamic_constraints_ok = False
                dynamic_constraints_detail = {
                    "errors": [_safe_exception_message(exc)]
                }
            dynamic_ledger_ok, dynamic_ledger_detail = (
                _dynamic_shadow_ledger_integrity_check(connection)
            )
        else:
            dynamic_constraints_ok = False
            dynamic_constraints_detail = {
                "missing_tables": sorted(dynamic_tables - existing),
                "errors": [{
                    "reason": "dynamic shadow constraint tables missing"
                }],
            }
            dynamic_ledger_ok = False
            dynamic_ledger_detail = {
                "missing_tables": sorted(dynamic_tables - existing),
                "errors": [{"reason": "dynamic shadow ledger tables missing"}],
            }
        add(
            "dynamic_shadow_ledger_schema_exact",
            dynamic_constraints_ok,
            dynamic_constraints_detail,
        )
        schema_ok = schema_ok and dynamic_constraints_ok
        add(
            "dynamic_shadow_candidate_plan_fill_forward_ledger",
            dynamic_ledger_ok,
            dynamic_ledger_detail,
        )
        schema_ok = schema_ok and dynamic_ledger_ok
        forward_schema_ok, forward_schema_detail = (
            _forward_strategy_version_schema_check(connection)
        )
        add(
            "forward_strategy_version_schema",
            forward_schema_ok,
            forward_schema_detail,
        )
        schema_ok = schema_ok and forward_schema_ok
        if forward_schema_ok:
            forward_data_ok, forward_data_detail = (
                _forward_strategy_version_data_check(connection)
            )
        else:
            forward_data_ok = False
            forward_data_detail = {
                "error": "forward strategy-version schema is invalid"
            }
        add(
            "forward_strategy_version_relations",
            forward_data_ok,
            forward_data_detail,
        )
        schema_ok = schema_ok and forward_data_ok
        raw_ledger_schema_ok, raw_ledger_schema_detail = (
            _v2_raw_ledger_immutability_schema_check(connection)
        )
        add(
            "v2_raw_fill_cash_ledgers_are_immutable",
            raw_ledger_schema_ok,
            raw_ledger_schema_detail,
        )
        schema_ok = schema_ok and raw_ledger_schema_ok
        exit_allocation_schema_ok, exit_allocation_schema_detail = (
            _forward_exit_allocation_schema_check(connection)
        )
        add(
            "forward_exit_allocation_v3_frozen_schema",
            exit_allocation_schema_ok,
            exit_allocation_schema_detail,
        )
        schema_ok = schema_ok and exit_allocation_schema_ok
        if exit_allocation_schema_ok:
            exit_allocation_data_ok, exit_allocation_data_detail = (
                _forward_exit_allocation_data_check(connection)
            )
        else:
            exit_allocation_data_ok = False
            exit_allocation_data_detail = {
                "error": "forward exit-allocation schema is invalid"
            }
        add(
            "forward_exit_allocation_v3_fifo_conservation",
            exit_allocation_data_ok,
            exit_allocation_data_detail,
        )
        schema_ok = schema_ok and exit_allocation_data_ok
        qmt_schema_ok, qmt_schema_detail = (
            _qmt_attestation_frozen_schema_check(connection)
        )
        add(
            "qmt_pre_close_v2_frozen_schema",
            qmt_schema_ok,
            qmt_schema_detail,
        )
        schema_ok = schema_ok and qmt_schema_ok
        if "st_strategy_governance_schema_migration" in existing:
            governance_migration_ok, governance_migration_detail = (
                _governance_schema_migration_check(connection)
            )
        else:
            governance_migration_ok = False
            governance_migration_detail = {"error": "migration table missing"}
        add(
            "governance_canonical_revision_migration",
            governance_migration_ok,
            governance_migration_detail,
        )
        schema_ok = schema_ok and governance_migration_ok
        if not authoritative_date:
            can_waive_date = bool(allow_input_not_ready and not date_error)
            add(
                "authoritative_trade_date",
                can_waive_date,
                {
                    "trade_date": "",
                    "source": date_source,
                    "error": date_error or "no authoritative completed trade date",
                },
                waived=can_waive_date,
            )
        else:
            add(
                "authoritative_trade_date",
                True,
                {"trade_date": authoritative_date, "source": date_source},
            )

        qmt_event_ok, qmt_event_detail = (
            _latest_qmt_announcement_batch_check(
                engine,
                authoritative_date,
            )
        )
        add(
            "latest_qmt_announcement_full_market_batch",
            qmt_event_ok,
            qmt_event_detail,
        )

        governance_schema_ok = schema_ok and all(
            name in existing for name in GOVERNANCE_TABLES
        )
        if not governance_schema_ok:
            return {
                "status": "FAIL",
                "run_disposition": "schema_invalid",
                "expected": {
                    "build_commit_sha": build_sha,
                    "trade_date": authoritative_date,
                    "trade_date_source": date_source,
                },
                "checks": checks,
                "automatic_real_order_submission": False,
            }

        registry = _one(
            connection,
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN current_status IS NULL OR current_status NOT IN "
            "('ACTIVE','REDUCE','SHADOW','SUSPENDED','RETIRED') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_status_count "
            "FROM st_strategy_registry",
        )
        strategy_count = _integer(registry.get("total"))
        add("dynamic_strategy_registry", strategy_count > 0, registry)
        add(
            "strategy_lifecycle_domain",
            _integer(registry.get("invalid_status_count")) == 0,
            registry,
        )
        strategy_versions = _one(
            connection,
            "SELECT COUNT(*) AS registry_count, "
            "SUM(v.strategy_key IS NULL) AS missing_current_version_count, "
            "SUM(CASE WHEN v.version_hash IS NULL OR "
            "v.version_hash NOT REGEXP '^[0-9a-f]{64}$' OR "
            "BINARY v.version_hash<>BINARY LOWER(v.version_hash) "
            "THEN 1 ELSE 0 END) "
            "AS invalid_version_hash_count "
            "FROM st_strategy_registry r "
            "LEFT JOIN st_strategy_version v "
            "ON v.strategy_key=r.strategy_key AND v.version=r.current_version",
        )
        add(
            "strategy_current_versions",
            _integer(strategy_versions.get("registry_count")) == strategy_count
            and _integer(strategy_versions.get("missing_current_version_count")) == 0
            and _integer(strategy_versions.get("invalid_version_hash_count")) == 0,
            strategy_versions,
        )

        combination_registry = _one(
            connection,
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN current_status IS NULL OR current_status NOT IN "
            "('ACTIVE','REDUCE','SHADOW','SUSPENDED','RETIRED') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_status_count "
            "FROM st_strategy_combination",
        )
        combination_count = _integer(combination_registry.get("total"))
        add(
            "dynamic_combination_registry",
            combination_count > 0,
            combination_registry,
        )
        add(
            "combination_lifecycle_domain",
            _integer(combination_registry.get("invalid_status_count")) == 0,
            combination_registry,
        )
        combination_versions = _one(
            connection,
            "SELECT COUNT(*) AS registry_count, "
            "SUM(v.combination_key IS NULL) AS missing_current_version_count, "
            "SUM(CASE WHEN v.config_hash IS NULL OR "
            "v.config_hash NOT REGEXP '^[0-9a-f]{64}$' OR "
            "BINARY v.config_hash<>BINARY LOWER(v.config_hash) "
            "THEN 1 ELSE 0 END) "
            "AS invalid_config_hash_count "
            "FROM st_strategy_combination c "
            "LEFT JOIN st_strategy_combination_version v "
            "ON v.combination_key=c.combination_key "
            "AND v.version=c.current_version",
        )
        add(
            "combination_current_versions",
            _integer(combination_versions.get("registry_count"))
            == combination_count
            and _integer(
                combination_versions.get("missing_current_version_count")
            )
            == 0
            and _integer(combination_versions.get("invalid_config_hash_count"))
            == 0,
            combination_versions,
        )
        immutable_hashes_ok, immutable_hashes_detail = (
            _all_immutable_version_hash_check(connection)
        )
        add(
            "all_immutable_version_hashes",
            immutable_hashes_ok,
            immutable_hashes_detail,
        )
        try:
            (
                immutable_history_ok,
                immutable_history_detail,
                run_transition_plan_hashes,
            ) = _immutable_lifecycle_and_audit_history_check(connection)
        except Exception as exc:
            immutable_history_ok = False
            run_transition_plan_hashes = {}
            immutable_history_detail = {
                "errors": [_safe_exception_message(exc)]
            }
        add(
            "all_lifecycle_and_audit_payload_hashes_and_run_bindings",
            immutable_history_ok,
            immutable_history_detail,
        )
        try:
            lifecycle_projection_ok, lifecycle_projection_detail = (
                _lifecycle_registry_projection_check(connection)
            )
        except Exception as exc:
            lifecycle_projection_ok = False
            lifecycle_projection_detail = {
                "errors": [type(exc).__name__],
            }
        add(
            "registry_lifecycle_projection_matches_immutable_events",
            lifecycle_projection_ok,
            lifecycle_projection_detail,
        )
        try:
            (
                industry_history_ok,
                industry_history_detail,
                industry_history_bindings,
                industry_history_snapshots,
            ) = _strategy_industry_history_contract_check(connection)
        except Exception as exc:
            industry_history_ok = False
            industry_history_bindings = {}
            industry_history_snapshots = {}
            industry_history_detail = {
                "errors": [_safe_exception_message(exc)]
            }
        add(
            "strategy_industry_history_exact_qmt_full_replay",
            industry_history_ok,
            industry_history_detail,
        )
        try:
            snapshot_history_ok, snapshot_history_detail = (
                _all_governance_snapshot_history_check(
                    connection,
                    industry_history_bindings=industry_history_bindings,
                    industry_history_trade_dates=set(
                        industry_history_snapshots
                    ),
                )
            )
        except Exception as exc:
            snapshot_history_ok = False
            snapshot_history_detail = {
                "errors": [_safe_exception_message(exc)]
            }
        add(
            "all_governance_detail_snapshot_hashes_and_run_bindings",
            snapshot_history_ok,
            snapshot_history_detail,
        )
        metric_domain = _one(
            connection,
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN verification_status IS NULL OR "
            "verification_status NOT IN ('PENDING','CONFIRMED','REJECTED') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_status_count, "
            "SUM(CASE WHEN funding_provenance IS NULL OR "
            "funding_provenance<>'EXTERNAL_SUBMITTED' "
            "THEN 1 ELSE 0 END) "
            "AS invalid_provenance_count, "
            "SUM(CASE WHEN entity_type IS NULL "
            "OR entity_type NOT IN ('STRATEGY','COMBINATION') "
            "OR window_days IS NULL OR window_days NOT IN (20,60,120) "
            "OR evidence_hash IS NULL "
            "OR evidence_hash NOT REGEXP '^[0-9a-f]{64}$' "
            "OR BINARY evidence_hash<>BINARY LOWER(evidence_hash) "
            "OR artifact_hash IS NULL "
            "OR artifact_hash NOT REGEXP '^[0-9a-f]{64}$' "
            "OR BINARY artifact_hash<>BINARY LOWER(artifact_hash) "
            "OR source_dataset_hash IS NULL "
            "OR source_dataset_hash NOT REGEXP '^[0-9a-f]{64}$' "
            "OR BINARY source_dataset_hash<>BINARY "
            "LOWER(source_dataset_hash) THEN 1 ELSE 0 END) "
            "AS invalid_contract_count, "
            "SUM(CASE WHEN verification_status IN ('CONFIRMED','REJECTED') "
            "AND (submitted_by IS NULL OR reviewed_by IS NULL "
            "OR reviewed_by='' OR reviewed_by=submitted_by "
            "OR reviewed_at IS NULL) THEN 1 ELSE 0 END) "
            "AS invalid_review_count, "
            "SUM(CASE WHEN verification_status='PENDING' "
            "AND (reviewed_by IS NULL OR reviewed_by<>'' "
            "OR reviewed_at IS NOT NULL) "
            "THEN 1 ELSE 0 END) AS mutated_pending_count, "
            "SUM(CASE WHEN verification_status='CONFIRMED' "
            "AND (artifact_json IS NULL OR artifact_json='') "
            "THEN 1 ELSE 0 END) AS confirmed_missing_artifact_count, "
            "SUM(CASE WHEN verification_status='CONFIRMED' "
            "AND (evidence_protocol IS NULL OR evidence_protocol NOT IN "
            "('PURGED_WALK_FORWARD_V2',"
            "'COMBINATORIAL_PURGED_WALK_FORWARD_V2')) "
            "THEN 1 ELSE 0 END) AS invalid_confirmed_protocol_count "
            "FROM st_strategy_metric_input",
        )
        add(
            "metric_evidence_state_domain",
            _integer(metric_domain.get("invalid_status_count")) == 0
            and _integer(metric_domain.get("invalid_provenance_count")) == 0
            and _integer(metric_domain.get("invalid_contract_count")) == 0
            and _integer(metric_domain.get("invalid_review_count")) == 0
            and _integer(metric_domain.get("mutated_pending_count")) == 0
            and _integer(
                metric_domain.get("confirmed_missing_artifact_count")
            )
            == 0
            and _integer(
                metric_domain.get("invalid_confirmed_protocol_count")
            )
            == 0,
            metric_domain,
        )
        try:
            metric_audit_ok, metric_audit_detail = (
                _metric_evidence_audit_history_check(connection)
            )
        except Exception as exc:
            metric_audit_ok = False
            metric_audit_detail = {
                "errors": [_safe_exception_message(exc)]
            }
        add(
            "all_metric_evidence_submission_and_review_audits",
            metric_audit_ok,
            metric_audit_detail,
        )
        try:
            global_evidence_ok, global_evidence_detail = (
                _global_evidence_claim_uniqueness_check(connection)
            )
        except Exception as exc:
            global_evidence_ok = False
            global_evidence_detail = {
                "errors": [_safe_exception_message(exc)]
            }
        add(
            "metric_and_challenger_evidence_hashes_globally_unique",
            global_evidence_ok,
            global_evidence_detail,
        )
        global_allocation_authority = _one(
            connection,
            "SELECT COUNT(*) AS total_snapshot_count, "
            "SUM(real_order_authority IS NULL OR real_order_authority<>0) "
            "AS forbidden_authority_count "
            "FROM st_strategy_allocation_snapshot",
        )
        add(
            "global_real_order_authority_closed",
            _integer(
                global_allocation_authority.get("forbidden_authority_count")
            )
            == 0,
            global_allocation_authority,
        )

        canonical_inventory = _one(
            connection,
            "SELECT COUNT(*) AS canonical_count, "
            "SUM(CASE WHEN status IS NULL OR status<>'COMPLETED' "
            "THEN 1 ELSE 0 END) AS invalid_status_count, "
            "SUM(CASE WHEN status='COMPLETED' AND "
            "JSON_UNQUOTE(JSON_EXTRACT(summary_json, "
            "'$.allocation_policy_version'))="
            ":allocation_policy_v5 THEN 1 ELSE 0 END) "
            "AS completed_v5_canonical_count "
            "FROM st_strategy_governance_run WHERE is_canonical=1",
            {"allocation_policy_v5": ALLOCATION_POLICY_VERSION},
        )
        canonical_count = _integer(
            canonical_inventory.get("canonical_count")
        )
        canonical_inventory_ok = (
            _integer(canonical_inventory.get("invalid_status_count")) == 0
        )
        add(
            "historical_canonical_run_inventory",
            canonical_inventory_ok,
            canonical_inventory,
        )
        clean_install_without_history = canonical_count == 0

        day_runs = (
            _rows(
                connection,
                "SELECT run_uid, trade_date, run_revision, "
                "supersedes_run_uid, is_canonical, status, "
                "build_commit_sha, created_at, finished_at "
                "FROM st_strategy_governance_run "
                "WHERE trade_date=:trade_date "
                "ORDER BY run_revision, created_at, run_uid",
                {"trade_date": authoritative_date},
            )
            if authoritative_date
            else []
        )
        may_waive_empty_expected_date = bool(
            allow_input_not_ready and clean_install_without_history
        )
        revision_chain_ok, revision_chain_detail = _canonical_revision_chain(
            day_runs,
            allow_empty=may_waive_empty_expected_date,
        )
        add(
            "authoritative_date_has_one_canonical_revision",
            revision_chain_ok,
            revision_chain_detail,
            waived=bool(may_waive_empty_expected_date and not day_runs),
        )

        build_runs = _rows(
            connection,
            "SELECT * FROM st_strategy_governance_run "
            "WHERE build_commit_sha=:build_commit_sha "
            "ORDER BY trade_date DESC, created_at DESC, run_uid DESC",
            {"build_commit_sha": build_sha},
        )
        build_date_runs = (
            [
                row
                for row in build_runs
                if _iso_date(row.get("trade_date")) == authoritative_date
            ]
            if authoritative_date
            else []
        )
        exact_runs = [
            row
            for row in build_date_runs
            if _integer(row.get("is_canonical")) == 1
        ]
        latest_completed = _one(
            connection,
            "SELECT run_uid, trade_date, run_revision, supersedes_run_uid, "
            "is_canonical, source_status, input_ready, "
            "input_hash, decision_hash, build_commit_sha, status, "
            "market_state, router_policy_version, router_snapshot_hash, "
            "strategy_count, combination_count, observation_count, "
            "confirmation_count, tradable_count, allocation_count, "
            "summary_json, result_json, result_hash, "
            "created_at, finished_at FROM st_strategy_governance_run "
            "WHERE status='COMPLETED' AND is_canonical=1 "
            "ORDER BY trade_date DESC, run_revision DESC, "
            "finished_at DESC, created_at DESC, run_uid DESC LIMIT 1",
        )

        historical_baseline = False
        validation_trade_date = authoritative_date
        validation_revision_chain_detail = revision_chain_detail
        prevalidated_run_audit: tuple[bool, dict[str, Any]] | None = None
        if not exact_runs:
            can_waive_run = bool(
                allow_input_not_ready and clean_install_without_history
            )
            # A row for this deployment build and authoritative date proves
            # governance started writing.  Even if that row is no longer the
            # canonical revision, it is not the "no input yet" condition and
            # must never be hidden by the deployment waiver.
            if build_date_runs:
                can_waive_run = False
            # Once this build has written a row but the authoritative date
            # cannot be established, the row cannot be assumed healthy.
            if not authoritative_date and build_runs:
                can_waive_run = False
            run_disposition = (
                "input_not_ready" if can_waive_run else "required_run_missing"
            )
            add(
                "expected_build_date_run",
                can_waive_run,
                {
                    "expected_build_commit_sha": build_sha,
                    "expected_trade_date": authoritative_date,
                    "matching_canonical_run_count": 0,
                    "matching_build_date_run_count": len(build_date_runs),
                    "historical_canonical_run_count": canonical_count,
                    "other_build_run_dates": sorted(
                        {
                            _iso_date(row.get("trade_date"))
                            for row in build_runs
                            if row.get("trade_date")
                        }
                    ),
                },
                waived=can_waive_run,
            )
            if clean_install_without_history:
                if authoritative_date:
                    clean_session_ok, clean_session_detail = (
                        _authoritative_session_window_attestation_check(
                            authoritative_date
                        )
                    )
                    clean_qmt_ok, clean_qmt_detail = (
                        _qmt_row_attestation_binding_check(
                            authoritative_date,
                            clean_session_detail,
                        )
                    )
                else:
                    clean_session_ok = False
                    clean_session_detail = {
                        "error": "authoritative trade date is unavailable"
                    }
                    clean_qmt_ok = False
                    clean_qmt_detail = {
                        "table_exists": None,
                        "protocol_version": (
                            QMT_PRECLOSE_ATTESTATION_PROTOCOL
                        ),
                        "error": "authoritative trade date is unavailable",
                    }
                add(
                    "authoritative_session_windows_qmt_close_attested",
                    clean_session_ok,
                    clean_session_detail,
                )
                add(
                    "qmt_pre_close_v2_rows_bind_current_kline",
                    clean_qmt_ok,
                    clean_qmt_detail,
                )
                no_history_valid = not latest_completed
                add(
                    "no_historical_canonical_run",
                    no_history_valid,
                    {
                        "canonical_count": canonical_count,
                        "latest_completed_run_uid": latest_completed.get(
                            "run_uid"
                        ),
                    },
                )
                status = (
                    "PASS"
                    if can_waive_run
                    and schema_ok
                    and scheduler_ok
                    and all(item["passed"] for item in checks)
                    else "FAIL"
                )
                return {
                    "status": status,
                    "run_disposition": run_disposition,
                    "expected": {
                        "build_commit_sha": build_sha,
                        "trade_date": authoritative_date,
                        "trade_date_source": date_source,
                    },
                    "checks": checks,
                    "automatic_real_order_submission": False,
                }

            latest_date = _iso_date(latest_completed.get("trade_date"))
            latest_baseline_valid = (
                bool(latest_completed.get("run_uid"))
                and _integer(latest_completed.get("is_canonical")) == 1
                and str(latest_completed.get("source_status") or "") == "fresh"
                and _integer(latest_completed.get("input_ready")) == 1
                and latest_completed.get("finished_at") is not None
                and bool(
                    RESULT_HASH_RE.fullmatch(
                        str(latest_completed.get("input_hash") or "")
                    )
                )
                and bool(
                    RESULT_HASH_RE.fullmatch(
                        str(latest_completed.get("decision_hash") or "")
                    )
                )
                and bool(
                    BUILD_SHA_RE.fullmatch(
                        str(latest_completed.get("build_commit_sha") or "")
                    )
                )
                and (
                    not authoritative_date
                    or bool(latest_date)
                    and latest_date <= authoritative_date
                )
            )
            add(
                "latest_completed_run_baseline",
                latest_baseline_valid,
                latest_completed or {"row_count": 0},
            )
            if not latest_baseline_valid or not canonical_inventory_ok:
                return {
                    "status": "FAIL",
                    "run_disposition": "historical_canonical_run_invalid",
                    "expected": {
                        "build_commit_sha": build_sha,
                        "trade_date": authoritative_date,
                        "trade_date_source": date_source,
                    },
                    "checks": checks,
                    "automatic_real_order_submission": False,
                }

            historical_baseline = True
            run_disposition = "required_run_missing_historical_baseline"
            validation_trade_date = latest_date
            validation_day_runs = _rows(
                connection,
                "SELECT run_uid, trade_date, run_revision, "
                "supersedes_run_uid, is_canonical, status, "
                "build_commit_sha, created_at, finished_at "
                "FROM st_strategy_governance_run "
                "WHERE trade_date=:trade_date "
                "ORDER BY run_revision, created_at, run_uid",
                {"trade_date": validation_trade_date},
            )
            (
                validation_revision_chain_ok,
                validation_revision_chain_detail,
            ) = _canonical_revision_chain(
                validation_day_runs,
                allow_empty=False,
            )
            add(
                "historical_baseline_has_one_canonical_revision",
                validation_revision_chain_ok,
                validation_revision_chain_detail,
            )
            prevalidated_run_audit = _run_audit_check(
                connection,
                latest_completed,
                validation_trade_date,
            )
            add(
                "latest_completed_run_has_hash_valid_audit",
                prevalidated_run_audit[0],
                prevalidated_run_audit[1],
            )
            run = latest_completed
        else:
            run = exact_runs[0]

        run_uid = str(run.get("run_uid") or "")
        run_result = _json_object(run.get("result_json")) or {}
        (
            candidate_industry_ok,
            candidate_industry_detail,
            candidate_industry_bindings,
        ) = _candidate_industry_snapshot_contract(
            run_result,
            trade_date=validation_trade_date,
            history_bindings=industry_history_bindings,
            history_snapshots=industry_history_snapshots,
        )
        add(
            "candidate_pool_industry_snapshot_binds_exact_qmt_history",
            candidate_industry_ok
            and validation_trade_date in industry_history_snapshots,
            candidate_industry_detail,
        )
        if historical_baseline:
            run_disposition = "required_run_missing_historical_baseline"
        else:
            run_disposition = (
                "completed" if run.get("status") == "COMPLETED" else "invalid"
            )
        session_windows_ok, session_windows_detail = (
            _authoritative_session_window_attestation_check(
                validation_trade_date
            )
        )
        add(
            "authoritative_session_windows_qmt_close_attested",
            session_windows_ok,
            session_windows_detail,
        )
        qmt_row_binding_ok, qmt_row_binding_detail = (
            _qmt_row_attestation_binding_check(
                validation_trade_date,
                session_windows_detail,
            )
        )
        add(
            "qmt_pre_close_v2_rows_bind_current_kline",
            qmt_row_binding_ok,
            qmt_row_binding_detail,
        )
        add(
            "latest_completed_run_identity",
            bool(latest_completed)
            and str(latest_completed.get("run_uid") or "") == run_uid
            and _iso_date(latest_completed.get("trade_date"))
            == validation_trade_date
            and (
                bool(
                    BUILD_SHA_RE.fullmatch(
                        str(
                            latest_completed.get("build_commit_sha") or ""
                        )
                    )
                )
                if historical_baseline
                else str(latest_completed.get("build_commit_sha") or "")
                == build_sha
            )
            and str(latest_completed.get("source_status") or "") == "fresh"
            and _integer(latest_completed.get("input_ready")) == 1
            and _integer(latest_completed.get("is_canonical")) == 1,
            {
                "run_uid": latest_completed.get("run_uid"),
                "trade_date": latest_completed.get("trade_date"),
                "build_commit_sha": latest_completed.get("build_commit_sha"),
                "source_status": latest_completed.get("source_status"),
                "input_ready": latest_completed.get("input_ready"),
                "run_revision": latest_completed.get("run_revision"),
                "is_canonical": latest_completed.get("is_canonical"),
                "validation_mode": (
                    "historical_baseline"
                    if historical_baseline
                    else "expected_build_date"
                ),
            },
        )
        if historical_baseline:
            add(
                "historical_baseline_run_identity",
                bool(run_uid)
                and _iso_date(run.get("trade_date"))
                == validation_trade_date
                and bool(
                    BUILD_SHA_RE.fullmatch(
                        str(run.get("build_commit_sha") or "")
                    )
                )
                and _integer(run.get("is_canonical")) == 1
                and run_uid
                == validation_revision_chain_detail.get(
                    "canonical_run_uid"
                ),
                {
                    "run_uid": run_uid,
                    "trade_date": run.get("trade_date"),
                    "build_commit_sha": run.get("build_commit_sha"),
                    "run_revision": run.get("run_revision"),
                    "supersedes_run_uid": run.get("supersedes_run_uid"),
                    "is_canonical": run.get("is_canonical"),
                    "current_registry_versions_required": True,
                },
            )
        else:
            add(
                "expected_build_date_run_unique",
                len(exact_runs) == 1,
                {
                    "matching_run_count": len(exact_runs),
                    "run_uids": [
                        str(row.get("run_uid") or "")
                        for row in exact_runs
                    ],
                },
            )
            add(
                "expected_run_identity",
                bool(run_uid)
                and _iso_date(run.get("trade_date"))
                == authoritative_date
                and str(run.get("build_commit_sha") or "") == build_sha
                and _integer(run.get("is_canonical")) == 1
                and run_uid
                == revision_chain_detail.get("canonical_run_uid"),
                {
                    "run_uid": run_uid,
                    "trade_date": run.get("trade_date"),
                    "build_commit_sha": run.get("build_commit_sha"),
                    "run_revision": run.get("run_revision"),
                    "supersedes_run_uid": run.get(
                        "supersedes_run_uid"
                    ),
                    "is_canonical": run.get("is_canonical"),
                },
            )
        add(
            "expected_run_completed",
            run.get("status") == "COMPLETED"
            and run.get("finished_at") is not None
            and bool(RESULT_HASH_RE.fullmatch(str(run.get("input_hash") or "")))
            and bool(RESULT_HASH_RE.fullmatch(str(run.get("decision_hash") or ""))),
            {
                "status": run.get("status"),
                "finished_at": run.get("finished_at"),
                "input_hash": run.get("input_hash"),
                "decision_hash": run.get("decision_hash"),
                "router_policy_version": run.get("router_policy_version"),
                "router_snapshot_hash": run.get("router_snapshot_hash"),
            },
        )
        add(
            "expected_run_input_fresh",
            str(run.get("source_status") or "") == "fresh"
            and _integer(run.get("input_ready")) == 1,
            {
                "source_status": run.get("source_status"),
                "input_ready": run.get("input_ready"),
            },
        )
        if prevalidated_run_audit is None:
            run_audit_ok, run_audit_detail = _run_audit_check(
                connection, run, validation_trade_date
            )
        else:
            run_audit_ok, run_audit_detail = prevalidated_run_audit
        add(
            "completed_run_has_hash_valid_audit",
            run_audit_ok,
            run_audit_detail,
        )
        try:
            funding_manifest_ok, funding_manifest_detail = (
                _funding_manifest_persistence_check(
                    connection,
                    run=run,
                    result=run_result,
                    trade_date=validation_trade_date,
                )
            )
        except Exception as exc:
            funding_manifest_ok = False
            funding_manifest_detail = {
                "errors": [{"reason": type(exc).__name__}],
            }
        add(
            "funding_checkpoint_manifest_partition_and_persistence",
            funding_manifest_ok,
            funding_manifest_detail,
        )
        add(
            "run_registry_counts",
            _integer(run.get("strategy_count")) == strategy_count
            and _integer(run.get("combination_count")) == combination_count,
            {
                "run_strategy_count": run.get("strategy_count"),
                "registry_strategy_count": strategy_count,
                "run_combination_count": run.get("combination_count"),
                "registry_combination_count": combination_count,
            },
        )
        try:
            router_ok, router_detail, route_bindings = (
                _market_router_binding(
                    connection,
                    run,
                    validation_trade_date,
                    session_windows_detail.get("windows")
                    if isinstance(session_windows_detail, dict)
                    else {},
                )
            )
        except Exception as exc:
            router_ok = False
            route_bindings = {}
            router_detail = {
                "errors": [
                    {
                        "entity_type": "SYSTEM",
                        "entity_key": "market_router",
                        "reason": (
                            "router proof could not be parsed: "
                            + _safe_exception_message(exc)
                        ),
                    }
                ]
            }
        add(
            "market_router_snapshot_is_reproducible",
            router_ok,
            router_detail,
        )
        raw_replay_ok, raw_replay_detail = (
            _current_canonical_raw_metric_replay_check(
                connection,
                run_uid=run_uid,
                trade_date=validation_trade_date,
                authoritative_windows=(
                    session_windows_detail.get("windows")
                    if isinstance(session_windows_detail, dict)
                    else {}
                ),
            )
        )
        add(
            "current_canonical_metrics_replay_from_raw_ledgers",
            raw_replay_ok,
            raw_replay_detail,
        )

        health = _one(
            connection,
            "SELECT COUNT(*) AS row_count, "
            "COUNT(DISTINCT strategy_key) AS strategy_count, "
            "COUNT(DISTINCT window_days) AS window_count, "
            "SUM(CASE WHEN window_days IS NULL OR "
            "window_days NOT IN (20,60,120) THEN 1 ELSE 0 END) "
            "AS invalid_window_count, "
            "SUM(CASE WHEN profit_gate_passed IS NULL OR "
            "profit_gate_passed NOT IN (0,1) THEN 1 ELSE 0 END) "
            "AS invalid_gate_count, "
            "SUM(CASE WHEN recommended_status IS NULL OR "
            "recommended_status NOT IN "
            "('ACTIVE','REDUCE','SHADOW','SUSPENDED','RETIRED') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_recommended_status_count, "
            "SUM(CASE WHEN trade_date IS NULL OR trade_date<>:trade_date "
            "THEN 1 ELSE 0 END) AS wrong_trade_date_count, "
            "SUM(CASE WHEN result_hash IS NULL OR "
            "result_hash NOT REGEXP '^[0-9a-f]{64}$' OR "
            "BINARY result_hash<>BINARY LOWER(result_hash) "
            "THEN 1 ELSE 0 END) "
            "AS invalid_result_hash_count "
            "FROM st_strategy_health_snapshot WHERE run_uid=:run_uid",
            {"run_uid": run_uid, "trade_date": validation_trade_date},
        )
        health_binding = _one(
            connection,
            "SELECT "
            "SUM(CASE WHEN r.strategy_key IS NULL "
            "OR h.strategy_version IS NULL OR r.current_version IS NULL "
            "OR BINARY h.strategy_version<>BINARY r.current_version "
            "THEN 1 ELSE 0 END) AS version_mismatch_count "
            "FROM st_strategy_health_snapshot h "
            "LEFT JOIN st_strategy_registry r "
            "ON r.strategy_key=h.strategy_key "
            "WHERE h.run_uid=:run_uid",
            {"run_uid": run_uid},
        )
        incomplete_strategy_windows = _one(
            connection,
            "SELECT COUNT(*) AS incomplete_count FROM ("
            "SELECT strategy_key, strategy_version "
            "FROM st_strategy_health_snapshot WHERE run_uid=:run_uid "
            "GROUP BY strategy_key, strategy_version "
            "HAVING COUNT(*)<>3 OR COUNT(DISTINCT window_days)<>3 "
            "OR MIN(window_days)<>20 OR MAX(window_days)<>120"
            ") x",
            {"run_uid": run_uid},
        )
        expected_health_rows = strategy_count * len(EXPECTED_WINDOWS)
        add(
            "strategy_health_three_windows",
            _integer(health.get("row_count")) == expected_health_rows
            and _integer(health.get("strategy_count")) == strategy_count
            and _integer(health.get("window_count")) == len(EXPECTED_WINDOWS)
            and _integer(health.get("invalid_window_count")) == 0
            and _integer(health.get("invalid_gate_count")) == 0
            and _integer(health.get("invalid_recommended_status_count")) == 0
            and _integer(health.get("wrong_trade_date_count")) == 0
            and _integer(health.get("invalid_result_hash_count")) == 0
            and _integer(health_binding.get("version_mismatch_count")) == 0
            and _integer(incomplete_strategy_windows.get("incomplete_count"))
            == 0,
            {
                **health,
                **health_binding,
                **incomplete_strategy_windows,
                "expected_rows": expected_health_rows,
                "expected_windows": EXPECTED_WINDOWS,
            },
        )

        combination_health = _one(
            connection,
            "SELECT COUNT(*) AS row_count, "
            "COUNT(DISTINCT combination_key) AS combination_count, "
            "SUM(CASE WHEN profit_gate_passed IS NULL OR "
            "profit_gate_passed NOT IN (0,1) THEN 1 ELSE 0 END) "
            "AS invalid_gate_count, "
            "SUM(CASE WHEN recommended_status IS NULL OR "
            "recommended_status NOT IN "
            "('ACTIVE','REDUCE','SHADOW','SUSPENDED','RETIRED') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_recommended_status_count, "
            "SUM(CASE WHEN trade_date IS NULL OR trade_date<>:trade_date "
            "THEN 1 ELSE 0 END) AS wrong_trade_date_count, "
            "SUM(CASE WHEN result_hash IS NULL OR "
            "result_hash NOT REGEXP '^[0-9a-f]{64}$' OR "
            "BINARY result_hash<>BINARY LOWER(result_hash) "
            "THEN 1 ELSE 0 END) "
            "AS invalid_result_hash_count "
            "FROM st_strategy_combination_health_snapshot "
            "WHERE run_uid=:run_uid",
            {"run_uid": run_uid, "trade_date": validation_trade_date},
        )
        combination_binding = _one(
            connection,
            "SELECT "
            "SUM(CASE WHEN c.combination_key IS NULL "
            "OR h.combination_version IS NULL OR c.current_version IS NULL "
            "OR BINARY h.combination_version<>BINARY c.current_version "
            "THEN 1 ELSE 0 END) "
            "AS version_mismatch_count "
            "FROM st_strategy_combination_health_snapshot h "
            "LEFT JOIN st_strategy_combination c "
            "ON c.combination_key=h.combination_key "
            "WHERE h.run_uid=:run_uid",
            {"run_uid": run_uid},
        )
        add(
            "combination_health_one_snapshot_each",
            _integer(combination_health.get("row_count")) == combination_count
            and _integer(combination_health.get("combination_count"))
            == combination_count
            and _integer(combination_health.get("invalid_gate_count")) == 0
            and _integer(
                combination_health.get("invalid_recommended_status_count")
            )
            == 0
            and _integer(combination_health.get("wrong_trade_date_count")) == 0
            and _integer(
                combination_health.get("invalid_result_hash_count")
            )
            == 0
            and _integer(combination_binding.get("version_mismatch_count"))
            == 0,
            {
                **combination_health,
                **combination_binding,
                "expected_rows": combination_count,
            },
        )
        confirmed_evidence_ok, confirmed_evidence_detail = (
            _confirmed_funding_evidence(
                connection, run_uid, validation_trade_date
            )
        )
        add(
            "funding_snapshots_use_confirmed_evidence",
            confirmed_evidence_ok,
            confirmed_evidence_detail,
        )

        pools = _one(
            connection,
            "SELECT COUNT(*) AS row_count, "
            "SUM(pool_level='OBSERVATION') AS observation_count, "
            "SUM(pool_level='CONFIRMATION') AS confirmation_count, "
            "SUM(pool_level='TRADABLE') AS tradable_count, "
            "SUM(CASE WHEN pool_level IS NULL OR pool_level NOT IN "
            "('OBSERVATION','CONFIRMATION','TRADABLE') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_pool_level_count, "
            "SUM(CASE WHEN trade_date IS NULL OR trade_date<>:trade_date "
            "THEN 1 ELSE 0 END) AS wrong_trade_date_count "
            "FROM st_strategy_pool_snapshot WHERE run_uid=:run_uid",
            {"run_uid": run_uid, "trade_date": validation_trade_date},
        )
        pool_match = all(
            _integer(pools.get(key)) == _integer(run.get(key))
            for key in (
                "observation_count",
                "confirmation_count",
                "tradable_count",
            )
        )
        add(
            "pool_counts_and_dates_match_run",
            pool_match
            and _integer(pools.get("row_count"))
            == sum(
                _integer(run.get(key))
                for key in (
                    "observation_count",
                    "confirmation_count",
                    "tradable_count",
                )
            )
            and _integer(pools.get("invalid_pool_level_count")) == 0
            and _integer(pools.get("wrong_trade_date_count")) == 0,
            {"run": {
                key: run.get(key)
                for key in (
                    "observation_count",
                    "confirmation_count",
                    "tradable_count",
                )
            }, "pool": pools},
        )
        pool_rows = _rows(
            connection,
            "SELECT trade_date, pool_level, stock_code, stock_name, "
            "rank_no, opportunity_score, execution_score, "
            "dominant_strategy, strategies_json, industry_name, "
            "gate_status, reason_json, evidence_json "
            "FROM st_strategy_pool_snapshot WHERE run_uid=:run_uid "
            "ORDER BY BINARY pool_level, rank_no, BINARY stock_code",
            {"run_uid": run_uid},
        )
        pool_summary = _json_object(run.get("summary_json")) or {}
        pool_contract_ok, pool_contract_detail, pool_snapshot_hash = (
            _pool_snapshot_contract_check(
                pool_rows,
                trade_date=validation_trade_date,
                route_bindings=route_bindings,
                summary=pool_summary,
                industry_bindings=candidate_industry_bindings,
            )
        )
        add(
            "pool_rows_snapshot_hash_and_funding_references",
            pool_contract_ok,
            pool_contract_detail,
        )

        allocation = _one(
            connection,
            "SELECT COUNT(*) AS row_count, "
            "COALESCE(SUM(simulated_weight_pct),0) AS weight_sum, "
            "SUM(real_order_authority IS NULL OR real_order_authority<>0) "
            "AS forbidden_authority_count, "
            "SUM(CASE WHEN simulated_weight_pct IS NULL "
            "OR simulated_weight_pct<0 THEN 1 ELSE 0 END) "
            "AS negative_weight_count, "
            "SUM(CASE WHEN target_type IS NULL OR "
            "target_type NOT IN ('STRATEGY','COMBINATION','CASH') "
            "THEN 1 ELSE 0 END) "
            "AS invalid_target_type_count, "
            "SUM(target_type='CASH') AS cash_rows "
            "FROM st_strategy_allocation_snapshot WHERE run_uid=:run_uid",
            {"run_uid": run_uid},
        )
        allocation_rows = _rows(
            connection,
            "SELECT a.target_type, a.target_key, a.target_version, "
            "a.funding_gate_hash, a.market_state, a.market_match_score, "
            "a.router_decision_hash, a.lifecycle_status, "
            "a.lifecycle_status_label, a.lifecycle_risk_multiplier, "
            "a.base_competitive_weight_pct, a.simulated_weight_pct, "
            "a.member_sleeves_json, a.member_sleeve_hash, "
            "a.cash_discount_bp, a.real_order_authority, "
            "r.strategy_key AS strategy_registry_key, "
            "r.current_version AS strategy_current_version, "
            "r.current_status AS strategy_current_status, "
            "r.enabled AS strategy_enabled, "
            "c.combination_key AS combination_registry_key, "
            "c.current_version AS combination_current_version, "
            "c.current_status AS combination_current_status, "
            "c.enabled AS combination_enabled "
            "FROM st_strategy_allocation_snapshot a "
            "LEFT JOIN st_strategy_registry r "
            "ON a.target_type='STRATEGY' AND r.strategy_key=a.target_key "
            "LEFT JOIN st_strategy_combination c "
            "ON a.target_type='COMBINATION' "
            "AND c.combination_key=a.target_key "
            "WHERE a.run_uid=:run_uid ORDER BY a.target_type, a.target_key",
            {"run_uid": run_uid},
        )
        allocation_contract_ok, allocation_contract_detail, (
            expected_allocation_rows
        ) = _allocation_decision_contract_check(
            run,
            route_bindings,
            allocation_rows,
            validation_trade_date,
            pool_snapshot_hash=pool_snapshot_hash,
            pool_rows=pool_rows,
            automatic_transition_plan_hash=(
                run_transition_plan_hashes.get(run_uid, "")
            ),
            industry_bindings=candidate_industry_bindings,
            current_build_commit_sha=build_sha,
            completed_v5_canonical_count=_integer(
                canonical_inventory.get("completed_v5_canonical_count")
            ),
        )
        add(
            "allocation_candidate_snapshot_and_decision_hashes",
            allocation_contract_ok,
            allocation_contract_detail,
        )
        strategy_funding_rows = _rows(
            connection,
            "SELECT strategy_key, strategy_version, evidence_json "
            "FROM st_strategy_health_snapshot WHERE run_uid=:run_uid",
            {"run_uid": run_uid},
        )
        combination_funding_rows = _rows(
            connection,
            "SELECT combination_key, combination_version, evidence_json "
            "FROM st_strategy_combination_health_snapshot "
            "WHERE run_uid=:run_uid",
            {"run_uid": run_uid},
        )
        strategy_funding: dict[
            tuple[str, str], list[tuple[bool, str]]
        ] = {}
        for item in strategy_funding_rows:
            payload = _json_object(item.get("evidence_json")) or {}
            key = (
                str(item.get("strategy_key") or ""),
                str(item.get("strategy_version") or ""),
            )
            strategy_funding.setdefault(key, []).append(
                (
                    payload.get("overall_profit_gate_passed") is True,
                    str(payload.get("funding_gate_hash") or ""),
                )
            )
        combination_funding = {
            (
                str(item.get("combination_key") or ""),
                str(item.get("combination_version") or ""),
            ): (
                (_json_object(item.get("evidence_json")) or {}).get(
                    "overall_profit_gate_passed"
                )
                is True,
                str(
                    (_json_object(item.get("evidence_json")) or {}).get(
                        "funding_gate_hash"
                    )
                    or ""
                ),
            )
            for item in combination_funding_rows
        }
        competitive_scores: dict[tuple[str, str, str], Decimal] = {
            key: binding["ranking_score"]
            for key, binding in route_bindings.items()
            if isinstance(binding.get("ranking_score"), Decimal)
        }
        invalid_allocation_targets: list[dict[str, Any]] = []
        selected_exposures: set[str] = set()
        overlap_errors: list[dict[str, Any]] = []
        lifecycle_budget_rows: list[dict[str, Any]] = []
        noncash_weight = Decimal("0")
        cash_weight: Decimal | None = None
        for item in allocation_rows:
            target_type = str(item.get("target_type") or "")
            target_key = str(item.get("target_key") or "")
            target_version = str(item.get("target_version") or "")
            funding_gate_hash = str(item.get("funding_gate_hash") or "")
            market_state = str(item.get("market_state") or "")
            market_match_score = _decimal(item.get("market_match_score"))
            router_decision_hash = str(
                item.get("router_decision_hash") or ""
            )
            lifecycle_status = str(item.get("lifecycle_status") or "")
            lifecycle_status_label = str(
                item.get("lifecycle_status_label") or ""
            )
            lifecycle_risk_multiplier = _decimal(
                item.get("lifecycle_risk_multiplier")
            )
            base_competitive_weight = _decimal(
                item.get("base_competitive_weight_pct")
            )
            weight = _decimal(item.get("simulated_weight_pct"))
            member_sleeves = _json_array(item.get("member_sleeves_json"))
            member_sleeve_hash = str(
                item.get("member_sleeve_hash") or ""
            )
            cash_discount_bp = _integer(item.get("cash_discount_bp"))
            eligible = False
            if target_type == "CASH":
                eligible = (
                    target_key == "cash"
                    and target_version == ""
                    and funding_gate_hash == ""
                    and market_state == str(run.get("market_state") or "")
                    and market_match_score == Decimal("0")
                    and router_decision_hash == ""
                    and lifecycle_status == ""
                    and lifecycle_status_label == ""
                    and lifecycle_risk_multiplier == Decimal("0")
                    and base_competitive_weight == Decimal("0")
                    and weight is not None
                    and member_sleeves == []
                    and member_sleeve_hash == ""
                    and cash_discount_bp == 0
                )
                cash_weight = weight
            elif target_type == "STRATEGY":
                version = str(item.get("strategy_current_version") or "")
                gates = strategy_funding.get((target_key, version), [])
                route = route_bindings.get(
                    ("STRATEGY", target_key, target_version)
                )
                eligible = (
                    str(item.get("strategy_registry_key") or "") == target_key
                    and _integer(item.get("strategy_enabled")) == 1
                    and str(item.get("strategy_current_status") or "")
                    in {"ACTIVE", "REDUCE"}
                    and len(gates) == len(EXPECTED_WINDOWS)
                    and all(passed for passed, _gate_hash in gates)
                    and len({gate_hash for _passed, gate_hash in gates}) == 1
                    and target_version == version
                    and funding_gate_hash
                    == (gates[0][1] if gates else "")
                    and bool(RESULT_HASH_RE.fullmatch(funding_gate_hash))
                    and route is not None
                    and route.get("eligible") is True
                    and route.get("paper_allocation_eligible") is True
                    and route.get("funding_gate_hash") == funding_gate_hash
                    and route.get("market_state") == market_state
                    and market_state == str(run.get("market_state") or "")
                    and route.get("market_match_score")
                    == market_match_score
                    and route.get("router_decision_hash")
                    == router_decision_hash
                    and RESULT_HASH_RE.fullmatch(router_decision_hash)
                    is not None
                    and market_match_score is not None
                    and Decimal("0") < market_match_score <= Decimal("100")
                    and weight is not None
                    and weight > 0
                )
            elif target_type == "COMBINATION":
                version = str(item.get("combination_current_version") or "")
                gate = combination_funding.get((target_key, version))
                route = route_bindings.get(
                    ("COMBINATION", target_key, target_version)
                )
                eligible = (
                    str(item.get("combination_registry_key") or "")
                    == target_key
                    and _integer(item.get("combination_enabled")) == 1
                    and str(item.get("combination_current_status") or "")
                    in {"ACTIVE", "REDUCE"}
                    and target_version == version
                    and gate is not None
                    and gate[0] is True
                    and funding_gate_hash == gate[1]
                    and bool(RESULT_HASH_RE.fullmatch(funding_gate_hash))
                    and route is not None
                    and route.get("eligible") is True
                    and route.get("paper_allocation_eligible") is True
                    and route.get("funding_gate_hash") == funding_gate_hash
                    and route.get("market_state") == market_state
                    and market_state == str(run.get("market_state") or "")
                    and route.get("market_match_score")
                    == market_match_score
                    and route.get("router_decision_hash")
                    == router_decision_hash
                    and RESULT_HASH_RE.fullmatch(router_decision_hash)
                    is not None
                    and market_match_score is not None
                    and Decimal("0") < market_match_score <= Decimal("100")
                    and weight is not None
                    and weight > 0
                )
            if target_type != "CASH" and weight is not None:
                noncash_weight += weight
                registry_status = str(
                    item.get(
                        "strategy_current_status"
                        if target_type == "STRATEGY"
                        else "combination_current_status"
                    )
                    or ""
                )
                expected_multiplier = LIFECYCLE_RISK_MULTIPLIER.get(
                    registry_status
                )
                route = route_bindings.get(
                    (target_type, target_key, target_version)
                )
                if target_type == "COMBINATION":
                    expected_multiplier = (
                        Decimal(str(round(
                            float(
                                expected_multiplier
                                * (
                                    (route or {}).get(
                                        "member_sleeve_risk_multiplier"
                                    )
                                    or Decimal("0")
                                )
                            ),
                            4,
                        )))
                        if expected_multiplier is not None
                        else None
                    )
                ranking_score = competitive_scores.get(
                    (target_type, target_key, target_version)
                )
                lifecycle_valid = (
                    lifecycle_status == registry_status
                    and lifecycle_status_label
                    == LIFECYCLE_LABELS.get(registry_status, "")
                    and expected_multiplier is not None
                    and lifecycle_risk_multiplier == expected_multiplier
                    and base_competitive_weight is not None
                    and base_competitive_weight > 0
                    and ranking_score is not None
                    and member_sleeves is not None
                    and (
                        (
                            target_type == "COMBINATION"
                            and bool(member_sleeves)
                            and RESULT_HASH_RE.fullmatch(
                                member_sleeve_hash
                            )
                            is not None
                        )
                        or (
                            target_type == "STRATEGY"
                            and member_sleeves == []
                            and member_sleeve_hash == ""
                        )
                    )
                    and cash_discount_bp
                    == int(
                        (
                            (base_competitive_weight - weight)
                            * Decimal("100")
                        ).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
                    )
                )
                eligible = eligible and lifecycle_valid
                lifecycle_budget_rows.append(
                    {
                        "target_type": target_type,
                        "target_key": target_key,
                        "target_version": target_version,
                        "ranking_score": ranking_score,
                        "market_match_score": market_match_score,
                        "lifecycle_status": lifecycle_status,
                        "lifecycle_status_label": lifecycle_status_label,
                        "lifecycle_risk_multiplier": (
                            lifecycle_risk_multiplier
                        ),
                        "base_competitive_weight_pct": (
                            base_competitive_weight
                        ),
                        "simulated_weight_pct": weight,
                    }
                )
                exposures = set((route or {}).get("members") or ())
                overlap = sorted(selected_exposures & exposures)
                if overlap:
                    overlap_errors.append(
                        {
                            "target_type": target_type,
                            "target_key": target_key,
                            "overlapping_strategy_keys": overlap,
                        }
                    )
                    eligible = False
                selected_exposures.update(exposures)
            if not eligible:
                invalid_allocation_targets.append(
                    {
                        "target_type": target_type,
                        "target_key": target_key,
                        "target_version": target_version,
                        "funding_gate_hash": funding_gate_hash,
                        "market_state": market_state,
                        "market_match_score": (
                            str(market_match_score)
                            if market_match_score is not None
                            else None
                        ),
                        "router_decision_hash": router_decision_hash,
                        "lifecycle_status": lifecycle_status,
                        "lifecycle_status_label": lifecycle_status_label,
                        "lifecycle_risk_multiplier": (
                            str(lifecycle_risk_multiplier)
                            if lifecycle_risk_multiplier is not None
                            else None
                        ),
                        "base_competitive_weight_pct": (
                            str(base_competitive_weight)
                            if base_competitive_weight is not None
                            else None
                        ),
                        "reason": "target/version/gate/router is not current, enabled and funded",
                    }
                )
        weight_sum = _decimal(allocation.get("weight_sum"))
        add(
            "paper_allocation_exactly_closed",
            _integer(allocation.get("row_count"))
            == _integer(run.get("allocation_count")) + 1
            and weight_sum == Decimal("100.0000")
            and _integer(allocation.get("forbidden_authority_count")) == 0
            and _integer(allocation.get("negative_weight_count")) == 0
            and _integer(allocation.get("invalid_target_type_count")) == 0
            and _integer(allocation.get("cash_rows")) == 1,
            {
                **allocation,
                "weight_sum": str(weight_sum) if weight_sum is not None else None,
                "expected_rows": _integer(run.get("allocation_count")) + 1,
                "required_weight_sum": "100.0000",
                "required_real_order_authority": 0,
            },
        )
        add(
            "allocation_targets_are_funding_eligible",
            len(allocation_rows) == _integer(allocation.get("row_count"))
            and not invalid_allocation_targets,
            {
                "row_count": len(allocation_rows),
                "invalid": invalid_allocation_targets,
            },
        )
        risk_cap = MARKET_RISK_CAP_PCT.get(
            str(run.get("market_state") or "")
        )
        lifecycle_budget_errors: list[dict[str, Any]] = []
        stored_base_weight = sum(
            (
                row["base_competitive_weight_pct"]
                for row in lifecycle_budget_rows
                if isinstance(
                    row.get("base_competitive_weight_pct"), Decimal
                )
            ),
            Decimal("0"),
        )
        expected_actual_weight = sum(
            (
                Decimal(str(row["simulated_weight_pct"]))
                for row in expected_allocation_rows
                if row["target_type"] != "CASH"
            ),
            Decimal("0"),
        )
        expected_cash_weight = next(
            (
                Decimal(str(row["simulated_weight_pct"]))
                for row in expected_allocation_rows
                if row["target_type"] == "CASH"
            ),
            Decimal("0"),
        )
        if not allocation_contract_ok:
            lifecycle_budget_errors.append(
                {"reason": "allocation candidate/snapshot replay differs"}
            )
        add(
            "allocation_lifecycle_budget_exact",
            not lifecycle_budget_errors
            and allocation_contract_ok
            and noncash_weight == expected_actual_weight
            and cash_weight == expected_cash_weight,
            {
                "risk_cap_pct": (
                    str(risk_cap) if risk_cap is not None else None
                ),
                "stored_base_competitive_weight_pct": str(
                    stored_base_weight
                ),
                "expected_actual_weight_pct": str(expected_actual_weight),
                "stored_actual_weight_pct": str(noncash_weight),
                "expected_cash_weight_pct": str(expected_cash_weight),
                "stored_cash_weight_pct": (
                    str(cash_weight) if cash_weight is not None else None
                ),
                "errors": lifecycle_budget_errors,
            },
        )
        add(
            "allocation_obeys_market_router_risk_budget",
            risk_cap is not None
            and not overlap_errors
            and Decimal("0") <= noncash_weight <= risk_cap
            and cash_weight == expected_cash_weight
            and not (
                str(run.get("market_state") or "") == "extreme_event"
                and noncash_weight != 0
            ),
            {
                "market_state": run.get("market_state"),
                "risk_cap_pct": str(risk_cap) if risk_cap is not None else None,
                "noncash_weight_pct": str(noncash_weight),
                "cash_weight_pct": (
                    str(cash_weight) if cash_weight is not None else None
                ),
                "expected_cash_weight_pct": str(expected_cash_weight),
                "overlap_errors": overlap_errors,
            },
        )

    return {
        "status": (
            "PASS"
            if checks and all(item["passed"] for item in checks)
            else "FAIL"
        ),
        "run_disposition": run_disposition,
        "expected": {
            "build_commit_sha": build_sha,
            "trade_date": authoritative_date,
            "trade_date_source": date_source,
        },
        "checks": checks,
        "automatic_real_order_submission": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="检查动态策略治理生产闭环"
    )
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--expected-trade-date", default="")
    parser.add_argument("--allow-input-not-ready", action="store_true")
    parser.add_argument("--expected-scheduler-pid", type=int, default=0)
    args = parser.parse_args()
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = None
    try:
        try:
            from server.engine.strategy_execution_adapters import (
                bootstrap_strategy_execution_adapter_registry,
            )

            adapter_registry = bootstrap_strategy_execution_adapter_registry()
            engine = create_tool_engine()
            result = collect_governance_health(
                engine,
                expected_build_sha=args.expected_build_sha,
                expected_trade_date=args.expected_trade_date,
                allow_input_not_ready=args.allow_input_not_ready,
                expected_scheduler_pid=args.expected_scheduler_pid,
            )
            if result.get("status") == "PASS":
                check_names = [
                    str(item.get("name") or "")
                    for item in result.get("checks", [])
                    if isinstance(item, dict)
                ]
                required_names = governance_health_required_check_names(
                    str(result.get("run_disposition") or ""),
                    require_scheduler_heartbeat=bool(
                        args.expected_scheduler_pid
                    ),
                )
                if (
                    len(check_names) != len(set(check_names))
                    or set(check_names) != set(required_names)
                ):
                    raise RuntimeError(
                        "governance health producer check inventory differs"
                    )
            result["adapter_registry"] = {
                "registry_sealed": adapter_registry["registry_sealed"],
                "registry_seal_hash": adapter_registry["registry_seal_hash"],
                "registry_integrity_ready": adapter_registry[
                    "registry_integrity_ready"
                ],
                "adapter_configured": adapter_registry[
                    "adapter_configured"
                ],
                "candidate_execution_ready": adapter_registry[
                    "candidate_execution_ready"
                ],
                "funding_pipeline_ready": adapter_registry[
                    "funding_pipeline_ready"
                ],
                "governance_paper_execution_ready": adapter_registry[
                    "governance_paper_execution_ready"
                ],
                "production_execution_ready": adapter_registry[
                    "production_execution_ready"
                ],
                "real_order_submission_enabled": adapter_registry[
                    "real_order_submission_enabled"
                ],
                "automatic_real_order_submission": adapter_registry[
                    "automatic_real_order_submission"
                ],
                "adapter_count": adapter_registry["adapter_count"],
            }
        except Exception as exc:
            result = {
                "contract_version": GOVERNANCE_HEALTH_CONTRACT_VERSION,
                "status": "FAIL",
                "run_disposition": "checker_error",
                "error": _safe_exception_message(exc),
                "automatic_real_order_submission": False,
            }
        result["contract_version"] = GOVERNANCE_HEALTH_CONTRACT_VERSION
    finally:
        if engine is not None:
            engine.dispose()
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=None if args.compact else 2,
            default=str,
        )
    )
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
    FUNDING_CHECKPOINT_SCHEMA,
    FUNDING_DAILY_FACT_SCHEMA,
