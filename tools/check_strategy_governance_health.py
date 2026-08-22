#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only production acceptance checks for dynamic strategy governance."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
from copy import deepcopy
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_strategy_governance_daily import authoritative_closed_trade_date
from tools.strategy_governance_task_contract import TASK as GOVERNANCE_TASK


QMT_PRECLOSE_ATTESTATION_PROTOCOL = "QMT_DAILY_UNADJUSTED_PRECLOSE_V2"
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
    "st_strategy_governance_audit",
)
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
            "reason",
            "real_order_authority",
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
    "st_strategy_governance_audit": {
        "PRIMARY": (("audit_id",), True),
        "uk_strategy_governance_audit_hash": (("audit_hash",), True),
    },
}
LIFECYCLE_STATES = ("ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED")
EXPECTED_WINDOWS = (20, 60, 120)
BUILD_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
RESULT_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
ROUTER_POLICY_VERSION = "strategy_market_router.v1"
ALLOCATION_POLICY_VERSION = "strategy_capital_competition.v2"
POOL_ROW_SCHEMA = "probiga.strategy-pool-row.v1"
POOL_ROW_EVIDENCE_SCHEMA = "probiga.strategy-pool-row-evidence.v1"
POOL_SNAPSHOT_SCHEMA = "probiga.strategy-pool-snapshot.v1"
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
            _authoritative_session_windows,
        )

        windows = _authoritative_session_windows(trade_date)
    except Exception as exc:
        return False, {"error": f"{type(exc).__name__}: {exc}"}

    window_fields = {
        "schema",
        "window_days",
        "start_date",
        "end_date",
        "session_count",
        "sessions",
        "session_attestations",
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
    return not errors, {"windows": summaries, "errors": errors}


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
            "WHERE BINARY a.protocol_version=BINARY :protocol_version "
            "AND a.stock_code REGEXP '^(0|3|6)' "
            "AND a.trade_date BETWEEN :start_date AND :trade_date "
            "AND EXISTS ("
            "SELECT 1 FROM qmt_kline_attestation_run r "
            "WHERE r.status='COMPLETED' "
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
            "AND a.trade_date BETWEEN r.start_date AND r.end_date"
            ") "
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
            "error": f"{type(exc).__name__}: {exc}",
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
                        f"{type(exc).__name__}: {exc}"
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
    """Independently require the frozen V2 proof tables and triggers."""

    from tools.attest_qmt_daily_kline import (
        QmtAttestationSchemaError,
        validate_attestation_schema,
    )

    try:
        return True, validate_attestation_schema(connection)
    except QmtAttestationSchemaError as exc:
        return False, exc.detail
    except Exception as exc:
        return False, {
            "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            "errors": [f"{type(exc).__name__}: {exc}"],
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
    """Require the exact one-way review and delete guards in production."""

    try:
        rows = connection.execute(text(
            "SELECT TRIGGER_NAME AS trigger_name, "
            "ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, "
            "EVENT_OBJECT_TABLE AS event_object_table, "
            "ACTION_ORIENTATION AS action_orientation, "
            "ACTION_STATEMENT AS action_statement "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=DATABASE() "
            "AND EVENT_OBJECT_TABLE='st_strategy_metric_input' "
            "ORDER BY BINARY TRIGGER_NAME"
        )).mappings().all()
    except Exception as exc:
        return False, {"errors": [f"{type(exc).__name__}: {exc}"]}
    observed = {
        str(row.get("trigger_name") or ""): dict(row) for row in rows
    }
    errors: list[str] = []
    expected_names = set(METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS)
    if set(observed) != expected_names:
        errors.append(
            "metric input trigger inventory differs: expected="
            f"{sorted(expected_names)!r}, observed={sorted(observed)!r}"
        )
    observed_hashes: dict[str, str] = {}
    for trigger_name, expected in (
        METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.items()
    ):
        timing, event, table_name, body_hash = expected
        row = observed.get(trigger_name, {})
        normalized_body = _normalized_metric_input_trigger_body(
            row.get("action_statement")
        )
        observed_hash = hashlib.sha256(
            normalized_body.encode("utf-8")
        ).hexdigest()
        observed_hashes[trigger_name] = observed_hash
        if (
            str(row.get("action_timing") or "").upper() != timing
            or str(row.get("event_manipulation") or "").upper() != event
            or str(row.get("event_object_table") or "") != table_name
            or str(row.get("action_orientation") or "").upper() != "ROW"
            or observed_hash != body_hash
            or not re.search(
                r"\bsignal\s+sqlstate\s+'45000'", normalized_body
            )
        ):
            errors.append(f"metric input trigger differs: {trigger_name}")
    return not errors, {
        "table": "st_strategy_metric_input",
        "trigger_names": sorted(observed),
        "trigger_count": len(observed),
        "body_hashes": observed_hashes,
        "errors": errors,
    }


def _governance_append_only_trigger_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Require the exact eight UPDATE/DELETE rejection guards."""

    from server.engine.strategy_governance import (
        GovernanceAppendOnlySchemaError,
        validate_governance_append_only_triggers,
    )

    try:
        return True, validate_governance_append_only_triggers(connection)
    except GovernanceAppendOnlySchemaError as exc:
        return False, exc.detail
    except Exception as exc:
        return False, {"errors": [f"{type(exc).__name__}: {exc}"]}


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


def _forward_strategy_version_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the exact additive V3 strategy-version migration contract."""

    try:
        from server.db.migrations_v3 import (
            FORWARD_STRATEGY_VERSION_DDL,
            FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
            MIGRATIONS,
            _checksum,
            _CREATE_TRIGGER_RE,
            _normalized_sql,
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

        expected_triggers: dict[str, tuple[str, str, str, str]] = {}
        for statement in FORWARD_STRATEGY_VERSION_DDL:
            match = _CREATE_TRIGGER_RE.match(str(statement))
            if match is None:
                continue
            name, timing, event, table_name, body = match.groups()
            expected_triggers[name] = (timing, event, table_name, body)
        trigger_names = sorted(expected_triggers)
        trigger_rows = _rows(
            connection,
            "SELECT TRIGGER_NAME AS trigger_name, "
            "EVENT_OBJECT_TABLE AS event_object_table, "
            "ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, "
            "ACTION_STATEMENT AS action_statement "
            "FROM information_schema.triggers "
            "WHERE trigger_schema=DATABASE() "
            "AND event_object_table='st_forward_trade_evidence_v3'",
        )
        observed_triggers = {
            str(row.get("trigger_name") or ""): row for row in trigger_rows
        }
        trigger_errors: list[str] = []
        for name, (timing, event, table_name, body) in expected_triggers.items():
            row = observed_triggers.get(name)
            if row is None:
                trigger_errors.append(f"missing trigger: {name}")
                continue
            if not (
                str(row.get("event_object_table") or "").casefold()
                == table_name.casefold()
                and str(row.get("action_timing") or "").upper()
                == timing.upper()
                and str(row.get("event_manipulation") or "").upper()
                == event.upper()
                and _normalized_sql(row.get("action_statement"))
                == _normalized_sql(body)
            ):
                trigger_errors.append(f"drifted trigger: {name}")
        if set(observed_triggers) != set(expected_triggers):
            trigger_errors.append("forward trigger inventory differs")
        insert_body = _normalized_sql(
            expected_triggers.get(
                "trg_v3_forward_owner_required_bi", ("", "", "", "")
            )[3]
        )
        update_body = _normalized_sql(
            expected_triggers.get(
                "trg_v3_forward_owner_immutable_bu", ("", "", "", "")
            )[3]
        )
        legacy_empty_compatible = "new.strategy_version = '' or (" in insert_body
        nonempty_relation_strict = all(
            token in insert_body
            for token in (
                "binary new.strategy_version = binary concat(",
                "$.primary_strategy_key",
                "$.primary_strategy_version",
                "json_valid(i.evidence_json)",
            )
        )
        update_immutable = (
            "binary new.strategy_version <> binary old.strategy_version"
            in update_body
        )
        triggers_valid = (
            len(expected_triggers) == 2
            and not trigger_errors
            and legacy_empty_compatible
            and nonempty_relation_strict
            and update_immutable
        )
        valid = ledger_valid and column_valid and index_valid and triggers_valid
        return valid, {
            "migration_version": FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
            "expected_checksum": expected_checksum,
            "expected_statement_count": expected_statement_count,
            "ledger_rows": ledger_rows,
            "column": column,
            "index_columns": [
                row.get("column_name") for row in index_rows
            ],
            "trigger_names": trigger_names,
            "trigger_errors": trigger_errors,
            "legacy_empty_strategy_version_compatible": legacy_empty_compatible,
            "nonempty_strategy_version_relation_strict": nonempty_relation_strict,
            "strategy_version_update_immutable": update_immutable,
        }
    except Exception as exc:
        return False, {
            "error": f"{type(exc).__name__}: {exc}",
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
          ON current_strategy.strategy_key=e.strategy_key
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
            "error": f"{type(exc).__name__}: {exc}",
            "current_version_query_has_limit": None,
        }


def _v2_raw_ledger_immutability_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the exact V3 migration freezing raw fill/cash facts."""

    try:
        from server.db.migrations_v3 import (
            MIGRATIONS,
            V2_RAW_LEDGER_IMMUTABILITY_DDL,
            V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
            _checksum,
            _CREATE_TRIGGER_RE,
            _normalized_sql,
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
        expected_triggers: dict[str, tuple[str, str, str, str]] = {}
        for statement in V2_RAW_LEDGER_IMMUTABILITY_DDL:
            match = _CREATE_TRIGGER_RE.match(str(statement))
            if match is None:
                continue
            name, timing, event, table_name, body = match.groups()
            expected_triggers[name] = (timing, event, table_name, body)
        trigger_rows = _rows(
            connection,
            "SELECT TRIGGER_NAME AS trigger_name, "
            "EVENT_OBJECT_TABLE AS event_object_table, "
            "ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, "
            "ACTION_STATEMENT AS action_statement "
            "FROM information_schema.triggers "
            "WHERE trigger_schema=DATABASE() "
            "AND event_object_table IN ('st_fill_v2','st_cash_ledger_v2')",
        )
        observed = {
            str(row.get("trigger_name") or ""): row for row in trigger_rows
        }
        errors: list[str] = []
        for name, (timing, event, table_name, body) in expected_triggers.items():
            row = observed.get(name)
            if row is None:
                errors.append(f"missing trigger: {name}")
                continue
            if not (
                str(row.get("event_object_table") or "").casefold()
                == table_name.casefold()
                and str(row.get("action_timing") or "").upper()
                == timing.upper()
                and str(row.get("event_manipulation") or "").upper()
                == event.upper()
                and _normalized_sql(row.get("action_statement"))
                == _normalized_sql(body)
            ):
                errors.append(f"drifted trigger: {name}")
        if set(observed) != set(expected_triggers):
            errors.append("raw-ledger trigger inventory differs")
        triggers_valid = len(expected_triggers) == 4 and not errors
        return ledger_valid and triggers_valid, {
            "migration_version": (
                V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION
            ),
            "expected_checksum": expected_checksum,
            "expected_statement_count": expected_statement_count,
            "ledger_rows": ledger_rows,
            "expected_trigger_names": sorted(expected_triggers),
            "observed_trigger_names": sorted(observed),
            "trigger_errors": errors,
        }
    except Exception as exc:
        return False, {"error": f"{type(exc).__name__}: {exc}"}


def _forward_exit_allocation_schema_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Verify the frozen 003 FIFO exit-allocation schema and ledger."""

    frozen_checksum = (
        "deeff7acffcea37b535a25a3f00216b91b15ffb8c2d9bf8fa05db7426e32053a"
    )
    frozen_statement_count = 5
    frozen_migration_count = 27
    errors: list[str] = []
    try:
        from server.db.migrations_v3 import (
            FORWARD_EXIT_ALLOCATION_DDL,
            FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
            MIGRATIONS,
            _checksum,
            _CREATE_TRIGGER_RE,
            _normalized_sql,
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

        expected_triggers: dict[str, tuple[str, str, str, str]] = {}
        for statement in statements:
            match = _CREATE_TRIGGER_RE.match(str(statement))
            if match is None:
                continue
            name, timing, event, table_name, body = match.groups()
            expected_triggers[name] = (timing, event, table_name, body)
        trigger_rows = _rows(
            connection,
            "SELECT TRIGGER_NAME AS trigger_name, "
            "EVENT_OBJECT_TABLE AS event_object_table, "
            "ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, "
            "ACTION_STATEMENT AS action_statement "
            "FROM information_schema.triggers "
            "WHERE trigger_schema=DATABASE() "
            "AND event_object_table='st_forward_exit_allocation_v3'",
        )
        observed_triggers = {
            str(row.get("trigger_name") or ""): row
            for row in trigger_rows
        }
        if len(expected_triggers) != 2:
            errors.append("003 declared trigger count differs")
        if set(observed_triggers) != set(expected_triggers):
            errors.append("forward exit-allocation trigger inventory differs")
        for name, (timing, event, table_name, body) in (
            expected_triggers.items()
        ):
            row = observed_triggers.get(name, {})
            if not (
                str(row.get("event_object_table") or "").casefold()
                == table_name.casefold()
                and str(row.get("action_timing") or "").upper()
                == timing.upper()
                and str(row.get("event_manipulation") or "").upper()
                == event.upper()
                and _normalized_sql(row.get("action_statement"))
                == _normalized_sql(body)
            ):
                errors.append(
                    "forward exit-allocation trigger differs: " + name
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
            "trigger_names": sorted(observed_triggers),
            "errors": errors[:100],
        }
    except Exception as exc:
        return False, {
            "frozen_checksum": frozen_checksum,
            "frozen_statement_count": frozen_statement_count,
            "frozen_migration_count": frozen_migration_count,
            "error": f"{type(exc).__name__}: {exc}",
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
        return False, {"error": f"{type(exc).__name__}: {exc}"}

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
                error=f"{type(exc).__name__}: {exc}",
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
        "WHERE migration_key IN (:run_revision_key, :content_hash_key) "
        "ORDER BY BINARY migration_key",
        {
            "run_revision_key": RUN_REVISION_MIGRATION_KEY,
            "content_hash_key": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
        },
    )
    expected = {
        RUN_REVISION_MIGRATION_KEY: RUN_REVISION_MIGRATION_HASH,
        STRATEGY_CONTENT_HASH_MIGRATION_KEY:
            STRATEGY_CONTENT_HASH_MIGRATION_HASH,
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


def _all_governance_snapshot_history_check(
    connection,
) -> tuple[bool, dict[str, Any]]:
    """Recompute every immutable detail snapshot bound by a completed run."""

    run_rows = _rows(
        connection,
        "SELECT run_uid, trade_date, market_state, status, summary_json "
        "FROM st_strategy_governance_run WHERE status='COMPLETED' "
        "ORDER BY BINARY run_uid",
    )
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
    for row in strategy_rows:
        run_uid = str(row.get("run_uid") or "")
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
    for row in combination_rows:
        run_uid = str(row.get("run_uid") or "")
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
    for row in pool_rows:
        run_uid = str(row.get("run_uid") or "")
        run = runs.get(run_uid)
        strategies = _json_array(row.get("strategies_json"))
        reason = _json_object(row.get("reason_json"))
        envelope = _json_object(row.get("evidence_json"))
        try:
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
                and str(envelope.get("pool_row_hash") or "") == row_hash
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
        "simulated_weight_pct, real_order_authority "
        "FROM st_strategy_allocation_snapshot ORDER BY BINARY run_uid, "
        "BINARY target_type, BINARY target_key",
    )
    allocations_by_run: dict[str, list[dict[str, Any]]] = {}
    for row in allocation_rows:
        run_uid = str(row.get("run_uid") or "")
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
        "completed_run_count": len(runs),
        "strategy_health_row_count": len(strategy_rows),
        "combination_health_row_count": len(combination_rows),
        "pool_row_count": len(pool_rows),
        "allocation_row_count": len(allocation_rows),
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
        metric_evidence_audit_binding,
    )

    errors: list[dict[str, Any]] = []
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
        valid, detail = metric_evidence_audit_binding(row, audit_rows)
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
        return f"canonical artifact verification failed: {type(exc).__name__}: {exc}"
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
        "h.profit_gate_passed, h.evidence_json, "
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
        "h.ranking_score, h.profit_gate_passed, h.evidence_json, "
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
            f"current market config cannot be verified: {type(exc).__name__}: {exc}",
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
        canonical_gate = _canonical_window_gate(metrics)
        stored_gate = payload.get("gate")
        if (
            canonical_gate is None
            or not isinstance(stored_gate, dict)
            or stored_gate != canonical_gate
            or metrics.get("profit_gate") != canonical_gate
            or (_integer(row.get("profit_gate_passed")) == 1)
            != (canonical_gate.get("passed") is True)
        ):
            fail(
                "STRATEGY",
                key,
                "persisted window gate differs from canonical metrics gate",
            )
        expected_session = session_window_bindings.get(
            str(_integer(row.get("window_days")))
        )
        expected_session = (
            expected_session if isinstance(expected_session, dict) else {}
        )
        session_binding_valid = (
            metrics.get("session_window_valid") is True
            and _iso_date(metrics.get("session_window_start"))
            == _iso_date(expected_session.get("start_date"))
            and _iso_date(metrics.get("session_window_end"))
            == _iso_date(expected_session.get("end_date"))
            and _integer(metrics.get("session_window_count"))
            == _integer(expected_session.get("session_count"))
            and str(metrics.get("session_window_hash") or "")
            == str(expected_session.get("session_hash") or "")
            and RESULT_HASH_RE.fullmatch(
                str(metrics.get("session_window_hash") or "")
            )
            is not None
        )
        if not session_binding_valid:
            fail(
                "STRATEGY",
                key,
                "snapshot session window is not bound to QMT close attestations",
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
            and str(metrics.get("market_route_hash") or "") == route_hash
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
                "canonical_gate": canonical_gate,
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
        expected_overall_gate = bool(
            canonical_windows_valid
            and _integer(items[0]["row"].get("registry_enabled")) == 1
            and all(
                canonical_gates[window].get("passed") is True
                for window in EXPECTED_WINDOWS
            )
        )
        expected_paper_eligible = bool(
            expected_overall_gate
            and str(
                items[0]["row"].get("registry_current_status") or ""
            )
            in {"ACTIVE", "REDUCE"}
            and routes[0].get("eligible") is True
        )
        if (
            not canonical_windows_valid
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
        window_evidence = {}
        for window in EXPECTED_WINDOWS:
            window_metrics = by_window[window].get("metrics")
            window_metrics = (
                window_metrics if isinstance(window_metrics, dict) else {}
            )
            window_evidence[str(window)] = window_metrics.get("evidence_hash")
        expected_gate_hash = _canonical_digest(
            {
                "strategy_key": key,
                "strategy_version": version,
                "window_evidence": window_evidence,
                "router_decision_hash": next(iter(hashes)),
                "overall_gate_passed": expected_overall_gate,
            }
        )
        funding_gate_hash = next(iter(gate_hashes))
        if (
            not RESULT_HASH_RE.fullmatch(funding_gate_hash)
            or funding_gate_hash != expected_gate_hash
        ):
            fail("STRATEGY", key, "strategy funding gate hash is not reproducible")
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
        for expected_window in EXPECTED_WINDOWS:
            window_metrics = metrics.get(str(expected_window))
            window_metrics = (
                window_metrics if isinstance(window_metrics, dict) else {}
            )
            canonical_gate = _canonical_window_gate(window_metrics)
            canonical_combo_gates[expected_window] = canonical_gate
            if (
                canonical_gate is None
                or window_metrics.get("profit_gate") != canonical_gate
            ):
                fail(
                    "COMBINATION",
                    key,
                    "combination window gate differs from canonical metrics gate",
                )
            expected_session = session_window_bindings.get(
                str(expected_window)
            )
            expected_session = (
                expected_session
                if isinstance(expected_session, dict)
                else {}
            )
            if not (
                window_metrics.get("session_window_valid") is True
                and _iso_date(window_metrics.get("session_window_start"))
                == _iso_date(expected_session.get("start_date"))
                and _iso_date(window_metrics.get("session_window_end"))
                == _iso_date(expected_session.get("end_date"))
                and _integer(window_metrics.get("session_window_count"))
                == _integer(expected_session.get("session_count"))
                and str(window_metrics.get("session_window_hash") or "")
                == str(expected_session.get("session_hash") or "")
                and RESULT_HASH_RE.fullmatch(
                    str(window_metrics.get("session_window_hash") or "")
                )
                is not None
            ):
                fail(
                    "COMBINATION",
                    key,
                    "snapshot session window is not bound to QMT close attestations",
                )
        expected_multi_window_gate = {
            str(window): canonical_combo_gates.get(window)
            for window in EXPECTED_WINDOWS
        }
        if (
            any(
                not isinstance(canonical_combo_gates.get(window), dict)
                for window in EXPECTED_WINDOWS
            )
            or payload.get("multi_window_gate") != expected_multi_window_gate
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
            and metrics[str(window)].get("funding_provenance")
            == "INTERNAL_PORTFOLIO_LEDGER_V1"
            and metrics[str(window)].get("verification_status") == "CONFIRMED"
            and metrics[str(window)].get("selection_validation_scope")
            == "VERSION_SELECTION_ONLY"
            and RESULT_HASH_RE.fullmatch(
                str(metrics[str(window)].get("selection_evidence_hash") or "")
            )
            is not None
            and RESULT_HASH_RE.fullmatch(
                str(metrics[str(window)].get("internal_ledger_hash") or "")
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
        frozen_versions: dict[str, dict[str, str]] = {}
        version_mismatch_present = False
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
            frozen_versions[member_key] = {
                "frozen": frozen_version,
                "current": current_version,
            }
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
                and bool(str(detail.get("status_label") or "").strip())
                and _decimal(detail.get("weight"))
                == (
                    Decimal(str(round(float(normalized_weight), 4)))
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
        expected_overall_gate = bool(
            _integer(row.get("registry_enabled")) == 1
            and has_independent_evidence
            and canonical_windows_passed
            and member_gate_passed
            and constraint_valid
            and constraint_evaluation.get("passed") is True
            and config_hash_valid
        )
        expected_paper_eligible = bool(
            expected_overall_gate
            and registry_status in {"ACTIVE", "REDUCE"}
            and route.get("eligible") is True
            and constraint_evaluation.get("passed") is True
        )
        if (
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
        expected_gate_hash = _canonical_digest(
            {
                "combination_key": key,
                "combination_version": version,
                "window_evidence": {
                    str(window): (
                        metrics.get(str(window)) or {}
                    ).get("evidence_hash")
                    for window in EXPECTED_WINDOWS
                },
                "member_versions": frozen_versions,
                "router_decision_hash": route_hash,
                "constraint_evaluation_hash": constraint_evaluation_hash,
                "profit_gate_passed": expected_overall_gate,
            }
        )
        funding_gate_hash = str(payload.get("funding_gate_hash") or "")
        if (
            not RESULT_HASH_RE.fullmatch(funding_gate_hash)
            or funding_gate_hash != expected_gate_hash
        ):
            fail(
                "COMBINATION",
                key,
                "combination funding gate hash is not reproducible",
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
        expected_paper_eligible = (
            enabled
            and status in {"ACTIVE", "REDUCE"}
            and profit_gate_passed
            and route_eligible
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
            }
        )
    candidates.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return candidates, errors


def _expected_allocation_snapshot(
    candidates: list[dict[str, Any]],
    *,
    market_state: str,
    trading_gate_passed: bool,
) -> list[dict[str, Any]]:
    """Independently replay overlap, largest remainder and REDUCE discount."""

    eligible = [
        dict(row)
        for row in candidates
        if row.get("paper_allocation_eligible") is True
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["ranking_score"])
            * float(row["market_match_score"])
            / 100.0,
            row["target_type"],
            row["target_key"],
        )
    )
    selected: list[dict[str, Any]] = []
    used_exposures: set[str] = set()
    for row in eligible:
        exposures = set(row.get("exposure_keys") or ())
        if not exposures or exposures.intersection(used_exposures):
            continue
        selected.append(row)
        used_exposures.update(exposures)

    risk_cap = MARKET_RISK_CAP_PCT.get(market_state)
    allocations: list[dict[str, Any]] = []
    assigned_after_lifecycle = 0
    if (
        trading_gate_passed
        and selected
        and risk_cap is not None
        and risk_cap > 0
    ):
        competitive_values = [
            max(
                0.0001,
                float(row["ranking_score"])
                * float(row["market_match_score"])
                / 100.0,
            )
            for row in selected
        ]
        competitive_total = sum(competitive_values)
        cap_basis_points = int(round(float(risk_cap) * 100))
        raw_basis_points = [
            cap_basis_points * value / competitive_total
            for value in competitive_values
        ]
        assigned = [int(value) for value in raw_basis_points]
        remainder = cap_basis_points - sum(assigned)
        remainder_order = sorted(
            range(len(selected)),
            key=lambda index: (
                -(raw_basis_points[index] - assigned[index]),
                selected[index]["target_key"],
            ),
        )
        for index in remainder_order[:remainder]:
            assigned[index] += 1
        for row, base_basis_points in zip(selected, assigned):
            multiplier = Decimal(
                str(row.get("lifecycle_risk_multiplier") or "0")
            )
            basis_points = int(
                (Decimal(base_basis_points) * multiplier).quantize(
                    Decimal("1"), rounding=ROUND_HALF_EVEN
                )
            )
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
) -> tuple[bool, dict[str, Any], str]:
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
            != {"schema", "source_evidence", "pool_row_hash"}
            or evidence_envelope.get("schema")
            != POOL_ROW_EVIDENCE_SCHEMA
            or not isinstance(
                evidence_envelope.get("source_evidence"), dict
            )
            or any(not isinstance(value, str) for value in strategies or [])
        ):
            structurally_complete = False
            errors.append(
                {**row_identity, "reason": "pool row JSON/identity differs"}
            )
            continue
        try:
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
                    "reason": f"pool row normalization failed: {exc}",
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


def _allocation_decision_contract_check(
    run: dict[str, Any],
    route_bindings: dict[tuple[str, str, str], dict[str, Any]],
    allocation_rows: list[dict[str, Any]],
    trade_date: str,
    *,
    pool_snapshot_hash: str | None = None,
    automatic_transition_plan_hash: str | None = None,
) -> tuple[bool, dict[str, Any], list[dict[str, Any]]]:
    candidates, candidate_errors = _allocation_candidate_contract(
        route_bindings
    )
    summary = _json_object(run.get("summary_json")) or {}
    market_state = str(run.get("market_state") or "")
    trading_gate_raw = summary.get("trading_gate_passed")
    trading_gate_passed = trading_gate_raw is True
    risk_cap = MARKET_RISK_CAP_PCT.get(market_state)
    candidate_payload = {
        "schema": "probiga.strategy-allocation-candidate-set.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trade_date": trade_date,
        "market_state": market_state,
        "candidates": candidates,
    }
    candidate_hash = _canonical_digest(candidate_payload)
    expected_allocations = _expected_allocation_snapshot(
        candidates,
        market_state=market_state,
        trading_gate_passed=trading_gate_passed,
    )
    stored_allocations = _stored_allocation_snapshot(allocation_rows)
    allocation_payload = {
        "schema": "probiga.strategy-allocation-snapshot.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
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
    decision_payload = {
        "schema": "strategy-governance-decision.v6",
        "trade_date": trade_date,
        "build_commit_sha": str(run.get("build_commit_sha") or ""),
        "input_hash": str(run.get("input_hash") or ""),
        "router_snapshot_hash": str(run.get("router_snapshot_hash") or ""),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": trading_gate_passed,
        "market_risk_cap_pct": float(risk_cap or 0),
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": sum(
            row["paper_allocation_eligible"] is True for row in candidates
        ),
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "pool_snapshot_hash": bound_pool_snapshot_hash,
        "strategies": [
            {
                "strategy_key": row["target_key"],
                "strategy_version": row["target_version"],
                "enabled": row["enabled"],
                "projected_status": row["lifecycle_status"],
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for row in strategy_candidates
        ],
        "combinations": [
            {
                "combination_key": row["target_key"],
                "combination_version": row["target_version"],
                "enabled": row["enabled"],
                "projected_status": row["lifecycle_status"],
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for row in combination_candidates
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
    errors = list(candidate_errors)
    summary_valid = (
        summary.get("allocation_policy_version")
        == ALLOCATION_POLICY_VERSION
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
    )
    if not summary_valid:
        errors.append({"reason": "allocation summary contract differs"})
    if stored_allocations != expected_allocations:
        errors.append({"reason": "persisted allocation replay differs"})
    if str(run.get("decision_hash") or "") != decision_hash:
        errors.append({"reason": "governance decision v6 hash differs"})
    return not errors, {
        "allocation_policy_version": summary.get(
            "allocation_policy_version"
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
            != "INTERNAL_PORTFOLIO_LEDGER_V1"
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
            "funding_gate_hash": payload.get("funding_gate_hash"),
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
            "funding_gate_hash": payload.get("funding_gate_hash"),
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

    with _RAW_METRIC_REPLAY_LOCK:
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
            rankings = governance._strategy_rankings(registry, metrics)
            replay_strategies: dict[str, dict[str, Any]] = {}
            for strategy in rankings:
                for window in governance.WINDOWS:
                    window_metrics = strategy["metrics"][str(window)]
                    key = "|".join((
                        "STRATEGY",
                        str(strategy.get("strategy_key") or ""),
                        str(strategy.get("current_version") or ""),
                        str(window),
                    ))
                    replay_strategies[key] = {
                        "metrics": window_metrics,
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
            combination_rankings = governance._combination_rankings(
                combination_registry,
                rankings,
                combination_inputs,
                trade_date,
            )
            replay_combinations: dict[str, dict[str, Any]] = {}
            for combination in combination_rankings:
                key = "|".join((
                    "COMBINATION",
                    str(combination.get("combination_key") or ""),
                    str(combination.get("current_version") or ""),
                ))
                replay_combinations[key] = {
                    "metrics": combination.get("metrics"),
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
                f"{type(exc).__name__}: {exc}"
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
        date_error = f"{type(exc).__name__}: {exc}"

    run_disposition = "unverified"
    with engine.connect() as connection:
        existing, schema_ok = _schema_checks(connection, add)
        scheduler_ok = _scheduler_checks(connection, existing, add)
        metric_trigger_ok, metric_trigger_detail = (
            _metric_input_review_trigger_check(connection)
        )
        add(
            "strategy_metric_input_review_triggers_frozen",
            metric_trigger_ok,
            metric_trigger_detail,
        )
        schema_ok = schema_ok and metric_trigger_ok
        append_only_ok, append_only_detail = (
            _governance_append_only_trigger_check(connection)
        )
        add(
            "governance_append_only_triggers_frozen",
            append_only_ok,
            append_only_detail,
        )
        schema_ok = schema_ok and append_only_ok
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
                "errors": [f"{type(exc).__name__}: {exc}"]
            }
        add(
            "all_lifecycle_and_audit_payload_hashes_and_run_bindings",
            immutable_history_ok,
            immutable_history_detail,
        )
        try:
            snapshot_history_ok, snapshot_history_detail = (
                _all_governance_snapshot_history_check(connection)
            )
        except Exception as exc:
            snapshot_history_ok = False
            snapshot_history_detail = {
                "errors": [f"{type(exc).__name__}: {exc}"]
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
                "errors": [f"{type(exc).__name__}: {exc}"]
            }
        add(
            "all_metric_evidence_submission_and_review_audits",
            metric_audit_ok,
            metric_audit_detail,
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
            "THEN 1 ELSE 0 END) AS invalid_status_count "
            "FROM st_strategy_governance_run WHERE is_canonical=1",
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
            "summary_json, "
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
                            f"{type(exc).__name__}: {exc}"
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
            "a.real_order_authority, "
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
            automatic_transition_plan_hash=(
                run_transition_plan_hashes.get(run_uid, "")
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
                route = route_bindings.get(
                    (target_type, target_key, target_version)
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
    args = parser.parse_args()
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = create_tool_engine()
    try:
        try:
            result = collect_governance_health(
                engine,
                expected_build_sha=args.expected_build_sha,
                expected_trade_date=args.expected_trade_date,
                allow_input_not_ready=args.allow_input_not_ready,
            )
        except Exception as exc:
            result = {
                "status": "FAIL",
                "run_disposition": "checker_error",
                "error": f"{type(exc).__name__}: {exc}",
                "automatic_real_order_submission": False,
            }
    finally:
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
