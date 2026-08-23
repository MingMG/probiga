# -*- coding: utf-8 -*-
"""Dynamic strategy governance, ranking, pool and paper-allocation domain.

The governance layer consumes the existing strategy-center snapshot, but owns
its own append-only evidence and lifecycle records.  It never grants broker or
real-order authority.  New strategy families and versions can be registered at
runtime; strategies without sufficient forward evidence stay in shadow mode.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import uuid
from collections import defaultdict
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from typing import Any, Callable, Iterable

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)
from server.common.sql_reader import (
    bind_sql_connection,
    current_bound_sql_connection,
    read_sql_rows,
)
from server.common.qmt_attestation_contract import (
    ATTESTATION_PROTOCOL_VERSION as QMT_PRECLOSE_ATTESTATION_PROTOCOL,
    build_qmt_v2_manifest,
    expected_stock_set_contract,
    validated_universe_manifest,
)
from server.common.versioned_strategy_config import (
    legacy_strategy_merge_map,
    load_market_state_config,
    load_stock_manifest,
    market_state_config_hash,
    stock_strategy_catalog,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3.hypotheses import STRATEGY_LABELS as V3_STRATEGY_LABELS
from server.trading_v3.forward_evidence import (
    INTENT_EPISODE_PROTOCOL,
    intent_episode_id,
)
from server.engine.strategy_execution_adapters import (
    normalize_execution_binding,
    strategy_execution_adapter_capabilities,
    strategy_execution_adapter_status,
    validate_strategy_adapter_run_receipt,
    verify_persisted_strategy_adapter_run_receipt,
)


LIFECYCLE_LABELS: dict[str, str] = {
    "ACTIVE": "正常运行",
    "REDUCE": "降权运行",
    "SHADOW": "影子观察",
    "SUSPENDED": "暂停使用",
    "RETIRED": "已淘汰",
}

GOVERNANCE_TABLE_NAMES = (
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
)
GOVERNANCE_SCHEMA_COLLATION = "utf8mb4_unicode_ci"


class GovernanceEvidenceNotReady(RuntimeError):
    """Authoritative market/evidence inputs are incomplete but not corrupt."""

    def __init__(self, message: str, *, blocking_record: dict[str, Any] | None = None):
        super().__init__(message)
        self.blocking_record = blocking_record or {
            "schema": "probiga.strategy-governance-block.v1",
            "status": "INPUT_NOT_READY",
            "status_label": "治理输入未就绪",
            "reason": str(message),
            "automatic_real_order_submission": False,
        }

EVIDENCE_STATUS_LABELS: dict[str, str] = {
    "PENDING": "等待独立复核",
    "CONFIRMED": "已确认",
    "REJECTED": "已驳回",
}

LIFECYCLE_TRANSITIONS: dict[str, frozenset[str]] = {
    "ACTIVE": frozenset({"ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED"}),
    "REDUCE": frozenset({"ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED"}),
    "SHADOW": frozenset({"ACTIVE", "REDUCE", "SHADOW", "SUSPENDED", "RETIRED"}),
    "SUSPENDED": frozenset({"SHADOW", "SUSPENDED", "RETIRED"}),
    "RETIRED": frozenset({"RETIRED"}),
}

PROFIT_GATE_POLICY: dict[str, Any] = {
    "minimum_completed_trades": 80,
    "minimum_coverage_days": 60,
    "minimum_net_expectancy_pct": 0.0,
    "minimum_cost_safety_multiple": 3.0,
    "minimum_payoff_ratio": 1.10,
    "target_payoff_ratio": 1.50,
    "minimum_profit_factor": 1.30,
    "maximum_drawdown_pct": 12.0,
    "walk_forward_segments": 5,
    "minimum_positive_segments": 4,
    "cost_stress_multiple": 1.5,
    "maximum_top5_profit_contribution_pct": 70.0,
    "minimum_consecutive_gate_passes": 3,
    "maximum_evidence_age_days": 7,
}
DECAY_GATE_20_POLICY: dict[str, Any] = {
    "window_days": 20,
    "minimum_completed_trades": 20,
    "minimum_portfolio_coverage_days": 20,
    "minimum_selection_completed_trades": 20,
    "minimum_selection_coverage_days": 20,
    "minimum_net_expectancy_pct": 0.0,
    "minimum_profit_factor_exclusive": 1.0,
    "cost_stress_multiple": 1.5,
}

HEALTH_SCORE_WEIGHTS = {
    "net_expectancy": 25.0,
    "profit_factor": 20.0,
    "sample_reliability": 20.0,
    "payoff_ratio": 15.0,
    "drawdown": 10.0,
    "market_cost_stability": 10.0,
}

DEFAULT_ROUND_TRIP_COST_PCT = 0.25
WINDOWS = (20, 60, 120)
MARKET_REGIME_STATES = (
    "trend_bullish",
    "high_range",
    "risk_declining",
    "extreme_event",
)
MARKET_ROUTER_POLICY_VERSION = "strategy_market_router.v1"
ALLOCATION_POLICY_VERSION = "strategy_capital_competition.v3"
DAILY_NAV_RANKING_BASIS = "DAILY_NET_NAV_20_60_120_V1"
DAILY_NAV_RANKING_BASIS_LABEL = "同口径20/60/120日扣费后日频净值健康分"
ALLOCATION_TYPE_LANE_POLICY = "FIXED_EQUAL_LANES_NO_CROSS_TYPE_RAW_SCORE_V1"
POOL_ROW_SCHEMA = "probiga.strategy-pool-row.v1"
POOL_ROW_EVIDENCE_SCHEMA = "probiga.strategy-pool-row-evidence.v1"
POOL_SNAPSHOT_SCHEMA = "probiga.strategy-pool-snapshot.v1"
AUTOMATIC_TRANSITION_PLAN_SCHEMA = (
    "probiga.strategy-automatic-transition-plan.v1"
)
MARKET_RISK_CAP_PCT = {
    "trend_bullish": 85.0,
    "high_range": 50.0,
    "risk_declining": 20.0,
    "extreme_event": 0.0,
}

# A REDUCE sleeve keeps its competitive rank, but receives only half of the
# capital an ACTIVE sleeve would receive.  The discounted budget stays in
# cash; it is never redistributed to another strategy in the same run.
LIFECYCLE_RISK_MULTIPLIER: dict[str, float] = {
    "ACTIVE": 1.0,
    "REDUCE": 0.5,
}
DEFAULT_COMBINATION_CONSTRAINTS: dict[str, Any] = {
    "formal_requires_all_members_eligible": True,
    "maximum_member_weight": 0.60,
    "maximum_pairwise_correlation": 0.80,
    "minimum_pairwise_observations": 60,
    "maximum_stock_overlap_pct": 40.0,
    "maximum_industry_weight_pct": 45.0,
    "real_order_authority": False,
}
_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,79}$")
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_EQUITY_BASE = Decimal("100.00000000")
_EQUITY_QUANTUM = Decimal("0.00000001")
_EQUITY_MAX = Decimal("1e30")
_METRIC_GLOBAL_UNIQUE_INDEXES = (
    (
        "uk_strategy_metric_artifact_global",
        "artifact_hash",
        "验证产物",
    ),
    (
        "uk_strategy_metric_dataset_global",
        "source_dataset_hash",
        "底层样本集",
    ),
)
_VERIFIED_WALK_FORWARD_PROTOCOLS = frozenset({
    "PURGED_WALK_FORWARD_V2",
    "COMBINATORIAL_PURGED_WALK_FORWARD_V2",
})
RUN_REVISION_MIGRATION_KEY = "20260822_001_canonical_run_revision"
RUN_REVISION_MIGRATION_HASH = hashlib.sha256(
    b"probiga.strategy-governance.canonical-run-revision.v1"
).hexdigest()
STRATEGY_CONTENT_HASH_MIGRATION_KEY = (
    "20260822_002_strategy_content_hash"
)
STRATEGY_CONTENT_HASH_MIGRATION_HASH = hashlib.sha256(
    b"probiga.strategy-governance.strategy-content-hash.v1"
).hexdigest()
GOVERNANCE_APPEND_ONLY_TABLES = (
    "st_strategy_version",
    "st_strategy_combination_version",
    "st_strategy_lifecycle_event",
    "st_strategy_governance_audit",
)
GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS = {
    "trg_strategy_version_immutable_bu": """
        CREATE TRIGGER trg_strategy_version_immutable_bu
        BEFORE UPDATE ON st_strategy_version
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy version is append only';
        END
    """,
    "trg_strategy_version_immutable_bd": """
        CREATE TRIGGER trg_strategy_version_immutable_bd
        BEFORE DELETE ON st_strategy_version
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy version cannot be deleted';
        END
    """,
    "trg_strategy_combination_version_immutable_bu": """
        CREATE TRIGGER trg_strategy_combination_version_immutable_bu
        BEFORE UPDATE ON st_strategy_combination_version
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy combination version is append only';
        END
    """,
    "trg_strategy_combination_version_immutable_bd": """
        CREATE TRIGGER trg_strategy_combination_version_immutable_bd
        BEFORE DELETE ON st_strategy_combination_version
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy combination version cannot be deleted';
        END
    """,
    "trg_strategy_lifecycle_event_immutable_bu": """
        CREATE TRIGGER trg_strategy_lifecycle_event_immutable_bu
        BEFORE UPDATE ON st_strategy_lifecycle_event
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy lifecycle event is append only';
        END
    """,
    "trg_strategy_lifecycle_event_immutable_bd": """
        CREATE TRIGGER trg_strategy_lifecycle_event_immutable_bd
        BEFORE DELETE ON st_strategy_lifecycle_event
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy lifecycle event cannot be deleted';
        END
    """,
    "trg_strategy_governance_audit_immutable_bu": """
        CREATE TRIGGER trg_strategy_governance_audit_immutable_bu
        BEFORE UPDATE ON st_strategy_governance_audit
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy governance audit is append only';
        END
    """,
    "trg_strategy_governance_audit_immutable_bd": """
        CREATE TRIGGER trg_strategy_governance_audit_immutable_bd
        BEFORE DELETE ON st_strategy_governance_audit
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'strategy governance audit cannot be deleted';
        END
    """,
}
GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS = {
    "trg_strategy_version_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "st_strategy_version",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy version is append only'; END",
    ),
    "trg_strategy_version_immutable_bd": (
        "BEFORE",
        "DELETE",
        "st_strategy_version",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy version cannot be deleted'; END",
    ),
    "trg_strategy_combination_version_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "st_strategy_combination_version",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy combination version is append only'; "
        "END",
    ),
    "trg_strategy_combination_version_immutable_bd": (
        "BEFORE",
        "DELETE",
        "st_strategy_combination_version",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = "
        "'strategy combination version cannot be deleted'; END",
    ),
    "trg_strategy_lifecycle_event_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "st_strategy_lifecycle_event",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy lifecycle event is append only'; END",
    ),
    "trg_strategy_lifecycle_event_immutable_bd": (
        "BEFORE",
        "DELETE",
        "st_strategy_lifecycle_event",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy lifecycle event cannot be deleted'; "
        "END",
    ),
    "trg_strategy_governance_audit_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "st_strategy_governance_audit",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy governance audit is append only'; END",
    ),
    "trg_strategy_governance_audit_immutable_bd": (
        "BEFORE",
        "DELETE",
        "st_strategy_governance_audit",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'strategy governance audit cannot be deleted'; "
        "END",
    ),
}

# Completed governance facts are historical ledgers too.  The four detail
# snapshot tables are strictly append-only.  A run may only be demoted from
# canonical=1 to canonical=0 when a same-day revision supersedes it; every
# other run field is frozen with MySQL's NULL-safe equality operator.
_GOVERNANCE_SNAPSHOT_TRIGGER_TABLES = {
    "strategy_health_snapshot": "st_strategy_health_snapshot",
    "strategy_combination_health_snapshot": (
        "st_strategy_combination_health_snapshot"
    ),
    "strategy_pool_snapshot": "st_strategy_pool_snapshot",
    "strategy_allocation_snapshot": "st_strategy_allocation_snapshot",
    "strategy_adapter_run_receipt": "st_strategy_adapter_run_receipt",
    "strategy_industry_history": "st_strategy_industry_history",
}
for _trigger_stem, _trigger_table in (
    _GOVERNANCE_SNAPSHOT_TRIGGER_TABLES.items()
):
    _update_name = f"trg_{_trigger_stem}_immutable_bu"
    _delete_name = f"trg_{_trigger_stem}_immutable_bd"
    _update_message = f"{_trigger_stem} is append only"
    _delete_message = f"{_trigger_stem} cannot be deleted"
    GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS[_update_name] = f"""
        CREATE TRIGGER {_update_name}
        BEFORE UPDATE ON {_trigger_table}
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = '{_update_message}';
        END
    """
    GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS[_delete_name] = f"""
        CREATE TRIGGER {_delete_name}
        BEFORE DELETE ON {_trigger_table}
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = '{_delete_message}';
        END
    """
    GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[_update_name] = (
        "BEFORE",
        "UPDATE",
        _trigger_table,
        "BEGIN SIGNAL SQLSTATE '45000' "
        f"SET MESSAGE_TEXT = '{_update_message}'; END",
    )
    GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[_delete_name] = (
        "BEFORE",
        "DELETE",
        _trigger_table,
        "BEGIN SIGNAL SQLSTATE '45000' "
        f"SET MESSAGE_TEXT = '{_delete_message}'; END",
    )

_GOVERNANCE_RUN_BINARY_COLUMNS = (
    "run_uid",
    "supersedes_run_uid",
    "market_state",
    "source_status",
    "input_hash",
    "build_commit_sha",
    "router_policy_version",
    "router_snapshot_hash",
    "decision_hash",
    "status",
    "summary_json",
    "result_json",
    "result_hash",
)
_GOVERNANCE_RUN_VALUE_COLUMNS = (
    "trade_date",
    "run_revision",
    "input_ready",
    "strategy_count",
    "formal_count",
    "shadow_count",
    "combination_count",
    "observation_count",
    "confirmation_count",
    "tradable_count",
    "allocation_count",
    "created_at",
    "finished_at",
)
_GOVERNANCE_RUN_FROZEN_PREDICATE = " AND ".join(
    [
        "OLD.is_canonical <=> 1",
        "NEW.is_canonical <=> 0",
        *(
            f"BINARY OLD.{column} <=> BINARY NEW.{column}"
            for column in _GOVERNANCE_RUN_BINARY_COLUMNS
        ),
        *(
            f"OLD.{column} <=> NEW.{column}"
            for column in _GOVERNANCE_RUN_VALUE_COLUMNS
        ),
    ]
)
_GOVERNANCE_RUN_UPDATE_BODY = (
    "BEGIN IF NOT (" + _GOVERNANCE_RUN_FROZEN_PREDICATE + ") THEN "
    "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
    "'governance run is immutable except canonical demotion'; "
    "END IF; END"
)
GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS[
    "trg_strategy_governance_run_frozen_bu"
] = (
    "CREATE TRIGGER trg_strategy_governance_run_frozen_bu "
    "BEFORE UPDATE ON st_strategy_governance_run FOR EACH ROW "
    + _GOVERNANCE_RUN_UPDATE_BODY
)
GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS[
    "trg_strategy_governance_run_immutable_bd"
] = """
    CREATE TRIGGER trg_strategy_governance_run_immutable_bd
    BEFORE DELETE ON st_strategy_governance_run
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'governance run cannot be deleted';
    END
"""
GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[
    "trg_strategy_governance_run_frozen_bu"
] = (
    "BEFORE",
    "UPDATE",
    "st_strategy_governance_run",
    _GOVERNANCE_RUN_UPDATE_BODY,
)
GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[
    "trg_strategy_governance_run_immutable_bd"
] = (
    "BEFORE",
    "DELETE",
    "st_strategy_governance_run",
    "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
    "'governance run cannot be deleted'; END",
)
GOVERNANCE_APPEND_ONLY_TABLES = (
    *GOVERNANCE_APPEND_ONLY_TABLES,
    *_GOVERNANCE_SNAPSHOT_TRIGGER_TABLES.values(),
    "st_strategy_governance_run",
)

# Version, lifecycle, snapshot and audit immutability is enforced by append-only
# application writers, unique identities and full hash replay.  Emptying these
# exported plans prevents every setup path (including the legacy privileged
# migrator) from issuing CREATE TRIGGER on managed RDS.  Existing triggers are
# intentionally neither queried nor removed.
GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS.clear()
GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS.clear()
_LEGACY_MAP = legacy_strategy_merge_map()
_SEED_LOCK = threading.Lock()
_SEED_READY = False


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _rebuild_equity_curve(
    normalized_trades: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compound canonical trade returns into one exact end-of-day curve."""

    equity = _EQUITY_BASE
    rebuilt: list[dict[str, Any]] = []
    for item in normalized_trades:
        try:
            net_return = Decimal(str(item["net_return_pct"]))
            factor = Decimal("1") + net_return / Decimal("100")
            next_equity = (equity * factor).quantize(
                _EQUITY_QUANTUM,
                rounding=ROUND_HALF_EVEN,
            )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            raise ValueError("逐笔净收益无法重建组合权益曲线") from exc
        if (
            not next_equity.is_finite()
            or next_equity <= 0
            or next_equity > _EQUITY_MAX
        ):
            raise ValueError("逐笔净收益重建后的组合权益超出有效范围")
        equity = next_equity
        trade_day = str(
            item.get("label_available_at") or item.get("trade_date") or ""
        )[:10]
        point = {
            "trade_date": trade_day,
            "equity": float(equity),
        }
        if rebuilt and rebuilt[-1]["trade_date"] == trade_day:
            rebuilt[-1] = point
        else:
            rebuilt.append(point)
    return rebuilt


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_text(value).encode("utf-8")).hexdigest()


def _trade_date(value: Any, *, default_today: bool = True) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        if default_today:
            return date.today().isoformat()
        raise ValueError("日期不能为空")
    raw = raw_value[:10]
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        raise ValueError(f"无效ISO日期：{raw_value[:40]}") from None


def _normalize_evidence_revision(value: Any) -> str:
    raw = str(value or "").strip().replace(" ", "T")
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def _require_authoritative_closed_trade_date(target: str) -> str:
    """Fail closed unless a write targets the one currently closed session."""

    try:
        authoritative_target = authoritative_closed_trade_date(
            current_bound_sql_connection() or get_engine()
        )
    except Exception as exc:
        raise GovernanceEvidenceNotReady(
            "权威交易日历暂不可用，拒绝持久化治理结果"
        ) from exc
    if not authoritative_target:
        raise GovernanceEvidenceNotReady(
            "权威交易日历没有已收盘交易日，拒绝持久化治理结果"
        )
    if target != authoritative_target:
        raise GovernanceEvidenceNotReady(
            "治理目标日不是权威已收盘交易日"
            f"（要求{authoritative_target}，实际{target}）"
        )
    return authoritative_target


def _db_read(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return read_sql_rows(get_engine(), sql, params, context="strategy_governance", stringify_datetime=True)


def _db_write(sql: str, params: dict[str, Any] | None = None) -> None:
    bound_connection = current_bound_sql_connection()
    if bound_connection is not None:
        bound_connection.execute(text(sql), params or {})
        return
    with get_engine().begin() as connection:
        connection.execute(text(sql), params or {})


def _table_exists(table_name: str) -> bool:
    try:
        rows = _db_read(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = :table_name",
            {"table_name": table_name},
        )
        return bool(rows and _int(rows[0].get("cnt")) > 0)
    except Exception:
        return False


def _table_columns(table_name: str) -> set[str]:
    try:
        return {
            str(row.get("column_name") or row.get("COLUMN_NAME") or "")
            for row in _db_read(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = :table_name",
                {"table_name": table_name},
            )
        }
    except Exception:
        return set()


def _strict_table_exists(table_name: str) -> bool:
    rows = _db_read(
        "SELECT COUNT(*) AS cnt FROM information_schema.tables "
        "WHERE table_schema=DATABASE() AND table_name=:table_name",
        {"table_name": table_name},
    )
    return bool(rows and _int(rows[0].get("cnt")) > 0)


def _strict_table_columns(table_name: str) -> set[str]:
    return {
        str(row.get("column_name") or row.get("COLUMN_NAME") or "")
        for row in _db_read(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name=:table_name",
            {"table_name": table_name},
        )
    }


def validate_strategy_key(value: str) -> str:
    key = str(value or "").strip().lower()
    if not _KEY_PATTERN.fullmatch(key):
        raise ValueError("策略代码必须以小写字母开头，只能包含小写字母、数字和下划线，长度3至80")
    return key


def _validated_market_regime_multipliers(
    value: Any, *, required: bool = True,
) -> dict[str, float]:
    """Normalize an immutable per-version market routing contract."""

    if value in (None, "") and not required:
        return {}
    if not isinstance(value, dict) or set(value) != set(MARKET_REGIME_STATES):
        raise ValueError(
            "市场路由必须完整声明趋势偏多、高位震荡、风险下降、极端事件四种状态"
        )
    result: dict[str, float] = {}
    for state in MARKET_REGIME_STATES:
        multiplier = _num(value.get(state), None)
        if multiplier is None or not 0.0 <= multiplier <= 1.5:
            raise ValueError(f"市场状态{state}的策略系数必须在0至1.5之间")
        result[state] = round(multiplier, 4)
    if not any(value > 0 for value in result.values()):
        raise ValueError("策略至少要适配一种非极端市场状态")
    if result["extreme_event"] != 0.0:
        raise ValueError("极端事件状态禁止为新增模拟资金配置正系数")
    return result


def _strategy_version_digest(
    *, strategy_key: str, version: str, evaluator_type: str,
    evaluator_config: dict[str, Any], parameters: dict[str, Any],
    source_kind: str,
) -> str:
    """Hash every immutable field that can change strategy behaviour."""

    return _digest({
        "schema": "probiga.strategy-version.v1",
        "strategy_key": strategy_key,
        "version": version,
        "evaluator_type": evaluator_type,
        "evaluator_config": evaluator_config,
        "parameters": parameters,
        "source_kind": source_kind,
    })


def _strategy_content_digest(
    *, strategy_key: str, evaluator_type: str,
    evaluator_config: dict[str, Any], parameters: dict[str, Any],
    source_kind: str,
) -> str:
    """Hash behaviour content independently from its display version."""

    return _digest({
        "schema": "probiga.strategy-content.v1",
        "strategy_key": strategy_key,
        "evaluator_type": evaluator_type,
        "evaluator_config": evaluator_config,
        "parameters": parameters,
        "source_kind": source_kind,
    })


def _validated_combination_constraints(value: Any) -> dict[str, Any]:
    """Normalize the complete, immutable risk contract for a combination."""

    if value in (None, ""):
        raw: dict[str, Any] = {}
    elif isinstance(value, dict):
        raw = dict(value)
    else:
        raise ValueError("组合约束必须是对象")
    unknown = sorted(set(raw) - set(DEFAULT_COMBINATION_CONSTRAINTS))
    if unknown:
        raise ValueError("组合约束包含不受支持字段：" + "、".join(unknown))
    result = {**DEFAULT_COMBINATION_CONSTRAINTS, **raw}
    if result.get("formal_requires_all_members_eligible") is not True:
        raise ValueError("组合正式运行必须要求全部成员通过资金门槛")
    if result.get("real_order_authority") is not False:
        raise ValueError("策略治理组合不得取得真实下单权限")
    maximum_member_weight = _num(result.get("maximum_member_weight"), None)
    maximum_correlation = _num(
        result.get("maximum_pairwise_correlation"), None
    )
    minimum_observations = _int(
        result.get("minimum_pairwise_observations"), 0
    )
    maximum_stock_overlap = _num(
        result.get("maximum_stock_overlap_pct"), None
    )
    maximum_industry_weight = _num(
        result.get("maximum_industry_weight_pct"), None
    )
    if maximum_member_weight is None or not 0.05 <= maximum_member_weight <= 1.0:
        raise ValueError("组合最大成员权重必须在0.05至1.00之间")
    if maximum_correlation is None or not -1.0 <= maximum_correlation <= 1.0:
        raise ValueError("组合最大成员相关系数必须在-1至1之间")
    if not 20 <= minimum_observations <= 5000:
        raise ValueError("组合相关性最少同步观测数必须在20至5000之间")
    if maximum_stock_overlap is None or not 0.0 <= maximum_stock_overlap <= 100.0:
        raise ValueError("组合最大个股重叠率必须在0至100之间")
    if maximum_industry_weight is None or not 1.0 <= maximum_industry_weight <= 100.0:
        raise ValueError("组合最大单一行业权重必须在1至100之间")
    return {
        "formal_requires_all_members_eligible": True,
        "maximum_member_weight": round(maximum_member_weight, 6),
        "maximum_pairwise_correlation": round(maximum_correlation, 6),
        "minimum_pairwise_observations": minimum_observations,
        "maximum_stock_overlap_pct": round(maximum_stock_overlap, 4),
        "maximum_industry_weight_pct": round(maximum_industry_weight, 4),
        "real_order_authority": False,
    }


def _manifest_regime_multipliers(strategy_key: str) -> dict[str, float]:
    config = load_market_state_config()
    raw = {
        state: (config.get("strategy_multipliers") or {}).get(state, {}).get(
            strategy_key, 0.0
        )
        for state in MARKET_REGIME_STATES
    }
    return _validated_market_regime_multipliers(raw)


_METRIC_INPUT_REVIEW_UPDATE_BODY = """
BEGIN
  IF NOT (
    BINARY OLD.evidence_id <=> BINARY NEW.evidence_id
    AND BINARY OLD.entity_type <=> BINARY NEW.entity_type
    AND BINARY OLD.strategy_key <=> BINARY NEW.strategy_key
    AND BINARY OLD.strategy_version <=> BINARY NEW.strategy_version
    AND OLD.as_of_date <=> NEW.as_of_date
    AND OLD.window_days <=> NEW.window_days
    AND BINARY OLD.metrics_json <=> BINARY NEW.metrics_json
    AND BINARY OLD.source <=> BINARY NEW.source
    AND BINARY OLD.evidence_protocol <=> BINARY NEW.evidence_protocol
    AND BINARY OLD.artifact_hash <=> BINARY NEW.artifact_hash
    AND BINARY OLD.artifact_json <=> BINARY NEW.artifact_json
    AND BINARY OLD.source_dataset_hash <=> BINARY NEW.source_dataset_hash
    AND OLD.evidence_revision_at <=> NEW.evidence_revision_at
    AND BINARY OLD.funding_provenance <=> BINARY NEW.funding_provenance
    AND BINARY OLD.submitted_by <=> BINARY NEW.submitted_by
    AND BINARY OLD.evidence_hash <=> BINARY NEW.evidence_hash
    AND OLD.created_at <=> NEW.created_at
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT='strategy metric evidence core is immutable';
  END IF;
  IF NOT (
    BINARY OLD.verification_status = BINARY 'PENDING'
    AND (
      BINARY NEW.verification_status = BINARY 'CONFIRMED'
      OR BINARY NEW.verification_status = BINARY 'REJECTED'
    )
    AND BINARY OLD.reviewed_by = BINARY ''
    AND OLD.reviewed_at IS NULL
    AND BINARY NEW.reviewed_by <> BINARY ''
    AND BINARY NEW.reviewed_by <> BINARY NEW.submitted_by
    AND NEW.reviewed_at IS NOT NULL
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT='strategy metric evidence review transition invalid';
  END IF;
END
""".strip()
_METRIC_INPUT_DELETE_BODY = """
BEGIN
  SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT='strategy metric evidence cannot be deleted';
END
""".strip()
METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS: dict[str, dict[str, str]] = {
    "trg_strategy_metric_input_review_bu": {
        "table": "st_strategy_metric_input",
        "timing": "BEFORE",
        "event": "UPDATE",
        "body": _METRIC_INPUT_REVIEW_UPDATE_BODY,
    },
    "trg_strategy_metric_input_immutable_bd": {
        "table": "st_strategy_metric_input",
        "timing": "BEFORE",
        "event": "DELETE",
        "body": _METRIC_INPUT_DELETE_BODY,
    },
}

# Managed production MySQL does not grant the deployment account authority to
# create binary-log protected triggers.  Review transitions are enforced by
# ``review_metric_input`` under a row lock and are independently replayed from
# hash-bound audit rows.  Export an empty trigger contract so legacy migration
# tooling also stops planning these database objects; an already-installed
# trigger may remain without becoming a deployment prerequisite.
METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.clear()


def _normalized_metric_input_trigger_body(value: Any) -> str:
    """Canonicalize information_schema text without weakening the contract."""

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


def _metric_input_trigger_inventory(connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(
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
    ]


def validate_metric_input_review_triggers(connection) -> dict[str, Any]:
    """Return the application-level review enforcement contract.

    The historical function name is retained for callers during a rolling
    release, but this validator deliberately performs no trigger inventory
    query and treats existing database triggers as unmanaged, compatible
    guards.
    """

    del connection
    return {
        "table": "st_strategy_metric_input",
        "trigger_names": [],
        "trigger_count": 0,
        "database_triggers_required": False,
        "enforcement": "row_lock_state_machine_and_hash_bound_audit",
        "errors": [],
    }


def _ensure_metric_input_review_triggers(
    connection,
    *,
    trigger_ddl_executor: Callable[[str], None] | None = None,
) -> None:
    """Compatibility shim; governance setup no longer manages triggers."""

    del connection
    if trigger_ddl_executor is not None and not callable(trigger_ddl_executor):
        raise TypeError("trigger_ddl_executor must be callable")


class GovernanceAppendOnlySchemaError(RuntimeError):
    """Raised when an append-only governance trigger has drifted."""

    def __init__(self, detail: dict[str, Any]):
        self.detail = detail
        errors = detail.get("errors") or ["unknown trigger drift"]
        super().__init__(
            "governance append-only trigger validation failed: "
            + "; ".join(str(error) for error in errors[:20])
        )


def _normalized_governance_trigger_body(value: Any) -> str:
    normalized = str(value or "").replace("`", "")
    normalized = re.sub(
        r"\bSQLSTATE\s+VALUE\b",
        "SQLSTATE",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    normalized = re.sub(r"\s*=\s*", "=", normalized)
    normalized = re.sub(r"\s*;\s*", ";", normalized)
    return normalized


def _governance_append_only_trigger_inventory(connection):
    table_names_sql = ", ".join(
        f"'{table_name}'" for table_name in GOVERNANCE_APPEND_ONLY_TABLES
    )
    return connection.execute(text(
        "SELECT TRIGGER_NAME AS trigger_name, "
        "ACTION_TIMING AS action_timing, "
        "EVENT_MANIPULATION AS event_manipulation, "
        "EVENT_OBJECT_TABLE AS event_object_table, "
        "ACTION_ORIENTATION AS action_orientation, "
        "ACTION_STATEMENT AS action_statement "
        "FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA=DATABASE() "
        f"AND EVENT_OBJECT_TABLE IN ({table_names_sql}) "
        "ORDER BY BINARY TRIGGER_NAME"
    )).mappings().all()


def _validate_governance_append_only_triggers_connection(
    connection,
) -> dict[str, Any]:
    del connection
    return {
        "table_names": list(GOVERNANCE_APPEND_ONLY_TABLES),
        "trigger_names": [],
        "trigger_count": 0,
        "database_triggers_required": False,
        "enforcement": "append_only_writers_unique_identity_and_hash_replay",
        "errors": [],
    }


def _ensure_governance_append_only_triggers(
    connection,
    *,
    trigger_ddl_executor: Callable[[str], None] | None = None,
) -> None:
    """Compatibility shim; governance setup no longer manages triggers."""

    del connection
    if trigger_ddl_executor is not None and not callable(trigger_ddl_executor):
        raise TypeError("trigger_ddl_executor must be callable")


def validate_governance_append_only_triggers(bind) -> dict[str, Any]:
    """Return the application-level append-only enforcement contract."""

    try:
        if hasattr(bind, "execute"):
            return _validate_governance_append_only_triggers_connection(bind)
        with bind.connect() as connection:
            return _validate_governance_append_only_triggers_connection(
                connection
            )
    except GovernanceAppendOnlySchemaError:
        raise
    except Exception as exc:
        raise GovernanceAppendOnlySchemaError(
            {"errors": [f"{type(exc).__name__}: {exc}"]}
        ) from exc


def _strict_governance_json(value: Any, expected_type: type) -> Any:
    if isinstance(value, expected_type):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("不可变治理版本包含无效JSON") from exc
    if not isinstance(parsed, expected_type):
        raise RuntimeError("不可变治理版本JSON类型错误")
    return parsed


def _strict_unique_index_contract(
    connection,
    *,
    table_name: str,
    index_name: str,
    columns: tuple[str, ...],
    allow_create: bool,
) -> None:
    rows = connection.execute(text(
        "SELECT NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
        "COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
        "INDEX_TYPE AS index_type "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
        "AND INDEX_NAME=:index_name ORDER BY SEQ_IN_INDEX"
    ), {"table_name": table_name, "index_name": index_name}).mappings().all()
    if not rows:
        if not allow_create:
            raise RuntimeError(
                f"冻结唯一索引缺失：{table_name}.{index_name}"
            )
        quoted_columns = ", ".join(columns)
        connection.execute(text(
            f"ALTER TABLE {table_name} ADD UNIQUE INDEX "
            f"{index_name} ({quoted_columns})"
        ))
        return
    observed_columns = tuple(
        str(row.get("column_name") or "") for row in rows
    )
    valid = (
        observed_columns == columns
        and all(int(row.get("non_unique") or 0) == 0 for row in rows)
        and all(
            int(row.get("seq_in_index") or 0) == index
            for index, row in enumerate(rows, 1)
        )
        and all(row.get("sub_part") is None for row in rows)
        and all(
            str(row.get("index_type") or "").upper() == "BTREE"
            for row in rows
        )
    )
    if not valid:
        raise RuntimeError(
            f"冻结唯一索引定义漂移：{table_name}.{index_name}"
        )


def _ensure_strategy_content_hash_schema(connection) -> None:
    migration = connection.execute(text(
        "SELECT migration_hash "
        "FROM st_strategy_governance_schema_migration "
        "WHERE migration_key=:migration_key"
    ), {
        "migration_key": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
    }).mappings().first()
    migration_pending = migration is None
    if (
        migration is not None
        and str(migration.get("migration_hash") or "")
        != STRATEGY_CONTENT_HASH_MIGRATION_HASH
    ):
        raise RuntimeError("策略内容哈希迁移标记不一致")

    column = connection.execute(text(
        "SELECT DATA_TYPE AS data_type, "
        "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
        "IS_NULLABLE AS is_nullable "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        "AND TABLE_NAME='st_strategy_version' "
        "AND COLUMN_NAME='content_hash'"
    )).mappings().first()
    if column is None:
        if not migration_pending:
            raise RuntimeError("策略内容哈希迁移已登记但字段缺失")
        connection.execute(text(
            "ALTER TABLE st_strategy_version "
            "ADD COLUMN content_hash CHAR(64) NULL AFTER version_hash"
        ))
        column = {
            "data_type": "char",
            "character_maximum_length": 64,
            "is_nullable": "YES",
        }
    if (
        str(column.get("data_type") or "").lower() != "char"
        or _int(column.get("character_maximum_length")) != 64
        or str(column.get("is_nullable") or "").upper()
        not in {"YES", "NO"}
    ):
        raise RuntimeError("策略内容哈希字段定义漂移")

    strategy_rows = connection.execute(text(
        "SELECT strategy_key, version, version_hash, content_hash, "
        "evaluator_type, evaluator_config_json, parameters_json, "
        "source_kind FROM st_strategy_version "
        "ORDER BY BINARY strategy_key, BINARY version"
    )).mappings().all()
    observed_contents: dict[tuple[str, str], str] = {}
    for row in strategy_rows:
        strategy_key = str(row.get("strategy_key") or "")
        version = str(row.get("version") or "")
        evaluator_config = _strict_governance_json(
            row.get("evaluator_config_json"), dict
        )
        parameters = _strict_governance_json(
            row.get("parameters_json"), dict
        )
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
        if expected_version_hash != str(row.get("version_hash") or ""):
            raise RuntimeError(
                f"策略历史版本哈希不一致：{strategy_key}:{version}"
            )
        duplicate_key = (strategy_key, expected_content_hash)
        if duplicate_key in observed_contents:
            raise RuntimeError(
                "同一策略存在内容完全相同的不同版本："
                f"{strategy_key}:{observed_contents[duplicate_key]}:{version}"
            )
        observed_contents[duplicate_key] = version
        stored_content_hash = str(row.get("content_hash") or "")
        if stored_content_hash and stored_content_hash != expected_content_hash:
            raise RuntimeError(
                f"策略历史内容哈希不一致：{strategy_key}:{version}"
            )
        if not stored_content_hash:
            if not migration_pending:
                raise RuntimeError(
                    f"策略内容哈希迁移后出现空值：{strategy_key}:{version}"
                )
            updated = connection.execute(text(
                "UPDATE st_strategy_version SET content_hash=:content_hash "
                "WHERE strategy_key=:strategy_key AND version=:version "
                "AND (content_hash IS NULL OR content_hash='')"
            ), {
                "content_hash": expected_content_hash,
                "strategy_key": strategy_key,
                "version": version,
            })
            if updated.rowcount != 1:
                raise RuntimeError("策略内容哈希回填发生并发冲突")

    combination_rows = connection.execute(text(
        "SELECT combination_key, version, members_json, constraints_json, "
        "config_hash FROM st_strategy_combination_version "
        "ORDER BY BINARY combination_key, BINARY version"
    )).mappings().all()
    observed_combinations: dict[tuple[str, str], str] = {}
    for row in combination_rows:
        combination_key = str(row.get("combination_key") or "")
        version = str(row.get("version") or "")
        members = _strict_governance_json(row.get("members_json"), list)
        constraints = _strict_governance_json(
            row.get("constraints_json"), dict
        )
        expected_hash = _digest({
            "members": members,
            "constraints": constraints,
        })
        stored_hash = str(row.get("config_hash") or "")
        if stored_hash != expected_hash:
            raise RuntimeError(
                f"组合历史内容哈希不一致：{combination_key}:{version}"
            )
        duplicate_key = (combination_key, expected_hash)
        if duplicate_key in observed_combinations:
            raise RuntimeError(
                "同一组合存在内容完全相同的不同版本："
                f"{combination_key}:"
                f"{observed_combinations[duplicate_key]}:{version}"
            )
        observed_combinations[duplicate_key] = version

    if str(column.get("is_nullable") or "").upper() == "YES":
        if not migration_pending:
            raise RuntimeError("策略内容哈希迁移后字段仍允许NULL")
        connection.execute(text(
            "ALTER TABLE st_strategy_version "
            "MODIFY COLUMN content_hash CHAR(64) NOT NULL"
        ))
    _strict_unique_index_contract(
        connection,
        table_name="st_strategy_version",
        index_name="uk_strategy_version_content",
        columns=("strategy_key", "content_hash"),
        allow_create=migration_pending,
    )
    _strict_unique_index_contract(
        connection,
        table_name="st_strategy_combination_version",
        index_name="uk_strategy_combination_hash",
        columns=("combination_key", "config_hash"),
        allow_create=migration_pending,
    )
    if migration_pending:
        connection.execute(text(
            "INSERT INTO st_strategy_governance_schema_migration "
            "(migration_key, migration_hash) "
            "VALUES (:migration_key, :migration_hash)"
        ), {
            "migration_key": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
            "migration_hash": STRATEGY_CONTENT_HASH_MIGRATION_HASH,
        })


def governance_table_ddl_statements() -> tuple[str, ...]:
    """Return the frozen create-table source used by setup and validation."""

    return (
        """
        CREATE TABLE IF NOT EXISTS st_strategy_governance_schema_migration (
            migration_key VARCHAR(80) PRIMARY KEY,
            migration_hash CHAR(64) NOT NULL,
            completed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_registry (
            strategy_key VARCHAR(80) PRIMARY KEY,
            strategy_name VARCHAR(120) NOT NULL,
            category VARCHAR(80) NOT NULL,
            family_key VARCHAR(80) NOT NULL,
            description VARCHAR(1000) NOT NULL DEFAULT '',
            owner_name VARCHAR(80) NOT NULL DEFAULT 'system',
            discovery_mode VARCHAR(40) NOT NULL DEFAULT 'dynamic',
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            current_version VARCHAR(160) NOT NULL,
            current_status VARCHAR(24) NOT NULL DEFAULT 'SHADOW',
            status_reason VARCHAR(500) NOT NULL DEFAULT '等待独立前向证据',
            recovery_conditions_json LONGTEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_strategy_registry_status (current_status, enabled),
            KEY idx_strategy_registry_family (family_key, strategy_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_version (
            strategy_key VARCHAR(80) NOT NULL,
            version VARCHAR(160) NOT NULL,
            version_hash CHAR(64) NOT NULL,
            content_hash CHAR(64) NOT NULL,
            parent_version VARCHAR(160) NOT NULL DEFAULT '',
            evaluator_type VARCHAR(40) NOT NULL DEFAULT 'external_evidence',
            evaluator_config_json LONGTEXT NOT NULL,
            parameters_json LONGTEXT NOT NULL,
            source_kind VARCHAR(40) NOT NULL DEFAULT 'runtime_registry',
            created_by VARCHAR(80) NOT NULL DEFAULT 'system',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (strategy_key, version),
            UNIQUE KEY uk_strategy_version_content
                (strategy_key, content_hash),
            KEY idx_strategy_version_hash (strategy_key, version_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_lifecycle_event (
            event_id CHAR(32) PRIMARY KEY,
            entity_type VARCHAR(24) NOT NULL DEFAULT 'STRATEGY',
            entity_key VARCHAR(80) NOT NULL,
            entity_version VARCHAR(160) NOT NULL,
            previous_status VARCHAR(24) NOT NULL,
            next_status VARCHAR(24) NOT NULL,
            reason VARCHAR(500) NOT NULL,
            trigger_type VARCHAR(40) NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            payload_json LONGTEXT NOT NULL,
            event_hash CHAR(64) NOT NULL,
            operator_name VARCHAR(80) NOT NULL,
            occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_lifecycle_event_hash (event_hash),
            KEY idx_strategy_lifecycle_entity (entity_type, entity_key, occurred_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_metric_input (
            evidence_id CHAR(32) PRIMARY KEY,
            entity_type VARCHAR(24) NOT NULL DEFAULT 'STRATEGY',
            strategy_key VARCHAR(80) NOT NULL,
            strategy_version VARCHAR(160) NOT NULL,
            as_of_date DATE NOT NULL,
            window_days INT NOT NULL,
            metrics_json LONGTEXT NOT NULL,
            source VARCHAR(80) NOT NULL,
            evidence_protocol VARCHAR(80) NOT NULL,
            artifact_hash CHAR(64) NOT NULL,
            artifact_json LONGTEXT NOT NULL,
            source_dataset_hash CHAR(64) NOT NULL,
            evidence_revision_at DATETIME NOT NULL,
            verification_status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
            funding_provenance VARCHAR(40) NOT NULL DEFAULT 'EXTERNAL_SUBMITTED',
            submitted_by VARCHAR(80) NOT NULL,
            reviewed_by VARCHAR(80) NOT NULL DEFAULT '',
            reviewed_at DATETIME DEFAULT NULL,
            evidence_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_metric_evidence (evidence_hash),
            UNIQUE KEY uk_strategy_metric_version_date
                (entity_type, strategy_key, strategy_version,
                 as_of_date, window_days),
            UNIQUE KEY uk_strategy_metric_artifact
                (entity_type, strategy_key, strategy_version,
                 window_days, artifact_hash),
            UNIQUE KEY uk_strategy_metric_dataset
                (entity_type, strategy_key, strategy_version,
                 window_days, source_dataset_hash),
            UNIQUE KEY uk_strategy_metric_artifact_global (artifact_hash),
            UNIQUE KEY uk_strategy_metric_dataset_global (source_dataset_hash),
            KEY idx_strategy_metric_latest (entity_type, strategy_key, strategy_version, as_of_date, window_days)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_health_snapshot (
            run_uid CHAR(32) NOT NULL,
            strategy_key VARCHAR(80) NOT NULL,
            strategy_version VARCHAR(160) NOT NULL,
            trade_date DATE NOT NULL,
            window_days INT NOT NULL,
            completed_trades INT NOT NULL DEFAULT 0,
            coverage_days INT NOT NULL DEFAULT 0,
            win_rate_pct DECIMAL(10,4) DEFAULT NULL,
            average_win_pct DECIMAL(10,4) DEFAULT NULL,
            average_loss_pct DECIMAL(10,4) DEFAULT NULL,
            payoff_ratio DECIMAL(10,4) DEFAULT NULL,
            gross_expectancy_pct DECIMAL(10,4) DEFAULT NULL,
            estimated_cost_pct DECIMAL(10,4) DEFAULT NULL,
            net_expectancy_pct DECIMAL(10,4) DEFAULT NULL,
            profit_factor DECIMAL(10,4) DEFAULT NULL,
            max_drawdown_pct DECIMAL(10,4) DEFAULT NULL,
            walk_forward_segments INT NOT NULL DEFAULT 0,
            positive_segments INT NOT NULL DEFAULT 0,
            cost_stress_expectancy_pct DECIMAL(10,4) DEFAULT NULL,
            top5_profit_contribution_pct DECIMAL(10,4) DEFAULT NULL,
            market_match_score DECIMAL(10,4) DEFAULT NULL,
            health_score DECIMAL(10,4) NOT NULL DEFAULT 0,
            profit_gate_passed TINYINT(1) NOT NULL DEFAULT 0,
            gate_reason VARCHAR(1000) NOT NULL,
            recommended_status VARCHAR(24) NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            result_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_uid, strategy_key, strategy_version, window_days),
            KEY idx_strategy_health_latest (strategy_key, trade_date, window_days),
            KEY idx_strategy_health_gate (profit_gate_passed, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_combination (
            combination_key VARCHAR(80) PRIMARY KEY,
            combination_name VARCHAR(120) NOT NULL,
            description VARCHAR(1000) NOT NULL DEFAULT '',
            owner_name VARCHAR(80) NOT NULL DEFAULT 'system',
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            current_version VARCHAR(160) NOT NULL,
            current_status VARCHAR(24) NOT NULL DEFAULT 'SHADOW',
            status_reason VARCHAR(500) NOT NULL DEFAULT '等待组合独立验证',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_strategy_combination_status (current_status, enabled)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_combination_version (
            combination_key VARCHAR(80) NOT NULL,
            version VARCHAR(160) NOT NULL,
            members_json LONGTEXT NOT NULL,
            constraints_json LONGTEXT NOT NULL,
            config_hash CHAR(64) NOT NULL,
            created_by VARCHAR(80) NOT NULL DEFAULT 'system',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (combination_key, version),
            UNIQUE KEY uk_strategy_combination_hash
                (combination_key, config_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_combination_health_snapshot (
            run_uid CHAR(32) NOT NULL,
            combination_key VARCHAR(80) NOT NULL,
            combination_version VARCHAR(160) NOT NULL,
            trade_date DATE NOT NULL,
            ranking_score DECIMAL(10,4) NOT NULL DEFAULT 0,
            profit_gate_passed TINYINT(1) NOT NULL DEFAULT 0,
            gate_reason VARCHAR(1000) NOT NULL,
            recommended_status VARCHAR(24) NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            result_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_uid, combination_key, combination_version),
            KEY idx_combination_health_latest
                (combination_key, trade_date, profit_gate_passed)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_governance_run (
            run_uid CHAR(32) PRIMARY KEY,
            trade_date DATE NOT NULL,
            run_revision INT NOT NULL DEFAULT 1,
            supersedes_run_uid CHAR(32) NOT NULL DEFAULT '',
            is_canonical TINYINT(1) NOT NULL DEFAULT 1,
            market_state VARCHAR(40) NOT NULL DEFAULT 'unknown',
            source_status VARCHAR(24) NOT NULL DEFAULT 'missing',
            input_ready TINYINT(1) NOT NULL DEFAULT 0,
            input_hash CHAR(64) NOT NULL,
            build_commit_sha VARCHAR(64) NOT NULL DEFAULT '',
            router_policy_version VARCHAR(80) NOT NULL,
            router_snapshot_hash CHAR(64) NOT NULL,
            decision_hash CHAR(64) NOT NULL,
            status VARCHAR(24) NOT NULL,
            strategy_count INT NOT NULL DEFAULT 0,
            formal_count INT NOT NULL DEFAULT 0,
            shadow_count INT NOT NULL DEFAULT 0,
            combination_count INT NOT NULL DEFAULT 0,
            observation_count INT NOT NULL DEFAULT 0,
            confirmation_count INT NOT NULL DEFAULT 0,
            tradable_count INT NOT NULL DEFAULT 0,
            allocation_count INT NOT NULL DEFAULT 0,
            summary_json LONGTEXT NOT NULL,
            result_json LONGTEXT NOT NULL,
            result_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at DATETIME DEFAULT NULL,
            UNIQUE KEY uk_strategy_governance_decision (decision_hash),
            KEY idx_strategy_governance_run_date (trade_date, created_at),
            KEY idx_strategy_governance_canonical
                (trade_date, is_canonical, run_revision)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_pool_snapshot (
            run_uid CHAR(32) NOT NULL,
            trade_date DATE NOT NULL,
            pool_level VARCHAR(24) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            stock_name VARCHAR(120) NOT NULL DEFAULT '',
            rank_no INT NOT NULL,
            opportunity_score DECIMAL(10,4) DEFAULT NULL,
            execution_score DECIMAL(10,4) DEFAULT NULL,
            dominant_strategy VARCHAR(80) NOT NULL DEFAULT '',
            strategies_json LONGTEXT NOT NULL,
            industry_name VARCHAR(120) NOT NULL DEFAULT '',
            gate_status VARCHAR(40) NOT NULL,
            reason_json LONGTEXT NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_uid, pool_level, stock_code),
            KEY idx_strategy_pool_date (trade_date, pool_level, rank_no)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_allocation_snapshot (
            run_uid CHAR(32) NOT NULL,
            target_type VARCHAR(24) NOT NULL,
            target_key VARCHAR(80) NOT NULL,
            target_version VARCHAR(160) NOT NULL DEFAULT '',
            funding_gate_hash CHAR(64) NOT NULL DEFAULT '',
            market_state VARCHAR(40) NOT NULL DEFAULT '',
            market_match_score DECIMAL(10,4) DEFAULT NULL,
            router_decision_hash CHAR(64) NOT NULL DEFAULT '',
            lifecycle_status VARCHAR(16) NOT NULL DEFAULT '',
            lifecycle_status_label VARCHAR(40) NOT NULL DEFAULT '',
            lifecycle_risk_multiplier DECIMAL(8,4) NOT NULL DEFAULT 0,
            base_competitive_weight_pct DECIMAL(10,4) NOT NULL DEFAULT 0,
            simulated_weight_pct DECIMAL(10,4) NOT NULL,
            member_sleeves_json LONGTEXT NOT NULL,
            member_sleeve_hash CHAR(64) NOT NULL DEFAULT '',
            cash_discount_bp INT NOT NULL DEFAULT 0,
            reason VARCHAR(500) NOT NULL,
            real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_uid, target_type, target_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_adapter_run_receipt (
            run_uid CHAR(32) PRIMARY KEY,
            strategy_key VARCHAR(80) NOT NULL,
            strategy_version VARCHAR(160) NOT NULL,
            strategy_version_hash CHAR(64) NOT NULL,
            execution_binding_hash CHAR(64) NOT NULL,
            adapter_artifact_sha256 CHAR(64) NOT NULL,
            cost_model_hash CHAR(64) NOT NULL,
            adapter_key VARCHAR(80) NOT NULL,
            adapter_version VARCHAR(160) NOT NULL,
            trade_date DATE NOT NULL,
            completed_at DATETIME(6) NOT NULL,
            status VARCHAR(24) NOT NULL,
            input_hash CHAR(64) NOT NULL,
            output_hash CHAR(64) NOT NULL,
            stable_result_hash CHAR(64) NOT NULL,
            candidate_count INT NOT NULL,
            candidate_identity_json LONGTEXT NOT NULL,
            receipt_json LONGTEXT NOT NULL,
            receipt_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_adapter_receipt_hash (receipt_hash),
            KEY idx_strategy_adapter_stable_result
                (strategy_key, strategy_version, trade_date,
                 execution_binding_hash, stable_result_hash),
            KEY idx_strategy_adapter_input
                (trade_date, input_hash, output_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_industry_history (
            snapshot_id CHAR(64) NOT NULL,
            trade_date DATE NOT NULL,
            as_of_exclusive DATETIME(6) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            industry_name VARCHAR(120) NOT NULL,
            industry_type VARCHAR(40) NOT NULL,
            source_system VARCHAR(80) NOT NULL,
            source_fact_id VARCHAR(160) NOT NULL,
            source_effective_at DATETIME(6) NOT NULL,
            source_etl_sync_at DATETIME(6) NOT NULL,
            row_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_id, stock_code),
            UNIQUE KEY uk_strategy_industry_row_hash (row_hash),
            UNIQUE KEY uk_strategy_industry_source_fact
                (source_system, source_fact_id),
            KEY idx_strategy_industry_asof
                (trade_date, as_of_exclusive, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_governance_audit (
            audit_id CHAR(32) PRIMARY KEY,
            entity_type VARCHAR(24) NOT NULL,
            entity_key VARCHAR(80) NOT NULL,
            action VARCHAR(40) NOT NULL,
            reason VARCHAR(500) NOT NULL,
            operator_name VARCHAR(80) NOT NULL,
            before_json LONGTEXT NOT NULL,
            after_json LONGTEXT NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            payload_json LONGTEXT NOT NULL,
            audit_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_governance_audit_hash (audit_hash),
            KEY idx_strategy_governance_audit_entity (entity_type, entity_key, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    )


def _split_governance_ddl_items(body: str) -> list[str]:
    items: list[str] = []
    start = 0
    depth = 0
    quoted = False
    index = 0
    while index < len(body):
        char = body[index]
        if char == "'":
            if quoted and index + 1 < len(body) and body[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                items.append(body[start:index].strip())
                start = index + 1
        index += 1
    items.append(body[start:].strip())
    return [item for item in items if item]


def _governance_table_schema_contract() -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for statement in governance_table_ddl_statements():
        match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)\s*"
            r"\((.*)\)\s*ENGINE=InnoDB",
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match is None:
            raise RuntimeError("治理建表语句无法生成冻结结构契约")
        table_name, body = match.groups()
        columns: list[dict[str, Any]] = []
        indexes: dict[str, tuple[int, tuple[str, ...]]] = {}
        for item in _split_governance_ddl_items(body):
            normalized = " ".join(item.split())
            primary = re.fullmatch(
                r"PRIMARY KEY\s*\((.+)\)",
                normalized,
                flags=re.IGNORECASE,
            )
            keyed = re.fullmatch(
                r"(UNIQUE\s+)?KEY\s+([a-z0-9_]+)\s*\((.+)\)",
                normalized,
                flags=re.IGNORECASE,
            )
            if primary is not None or keyed is not None:
                raw_columns = (
                    primary.group(1) if primary is not None else keyed.group(3)
                )
                index_name = "PRIMARY" if primary is not None else keyed.group(2)
                non_unique = 0 if primary is not None or keyed.group(1) else 1
                indexes[index_name] = (
                    non_unique,
                    tuple(
                        value.strip().strip("`")
                        for value in raw_columns.split(",")
                    ),
                )
                continue
            column = re.match(
                r"`?([a-z0-9_]+)`?\s+([a-z]+(?:\(\d+(?:,\d+)?\))?)(.*)$",
                normalized,
                flags=re.IGNORECASE,
            )
            if column is None:
                raise RuntimeError(
                    f"治理字段声明无法生成冻结结构契约：{table_name}"
                )
            column_name, column_type, suffix = column.groups()
            default_match = re.search(
                r"\bDEFAULT\s+('(?:''|[^'])*'|CURRENT_TIMESTAMP|NULL|"
                r"-?\d+(?:\.\d+)?)",
                suffix,
                flags=re.IGNORECASE,
            )
            default: str | None = None
            if default_match is not None:
                default = default_match.group(1)
                if default.startswith("'") and default.endswith("'"):
                    default = default[1:-1].replace("''", "'")
                elif default.upper() == "NULL":
                    default = None
                elif default.upper() == "CURRENT_TIMESTAMP":
                    default = "CURRENT_TIMESTAMP"
            columns.append({
                "name": column_name,
                "column_type": column_type.casefold(),
                "nullable": "NO" if re.search(
                    r"\bNOT\s+NULL\b", suffix, flags=re.IGNORECASE
                ) else "YES",
                "default": default,
                "auto_increment": bool(re.search(
                    r"\bAUTO_INCREMENT\b", suffix, flags=re.IGNORECASE
                )),
                "on_update_current_timestamp": bool(re.search(
                    r"\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b",
                    suffix,
                    flags=re.IGNORECASE,
                )),
            })
            inline_primary = re.search(
                r"\bPRIMARY\s+KEY\b", suffix, flags=re.IGNORECASE
            )
            if inline_primary:
                indexes["PRIMARY"] = (0, (column_name,))
        # MySQL makes every PRIMARY KEY column NOT NULL even when the DDL
        # omits the redundant spelling.  The frozen contract must model the
        # metadata MySQL actually exposes through information_schema, for
        # both inline and table-level primary-key declarations.
        primary_index = indexes.get("PRIMARY")
        if primary_index is not None:
            primary_columns = set(primary_index[1])
            for column in columns:
                if column["name"] in primary_columns:
                    column["nullable"] = "NO"
        contracts[table_name] = {
            "columns": columns,
            "indexes": indexes,
        }
    if set(contracts) != set(GOVERNANCE_TABLE_NAMES):
        raise RuntimeError("治理建表语句集合与冻结表清单不一致")
    return contracts


def _normalized_governance_column_default(
    value: Any,
    column_type: str,
) -> Any:
    if value is None:
        return None
    raw = str(value)
    base_type = column_type.split("(", 1)[0]
    if base_type in {"tinyint", "int", "bigint", "decimal"}:
        try:
            return Decimal(raw).normalize()
        except InvalidOperation:
            return raw
    if base_type in {"datetime", "timestamp"} and raw.casefold().replace(
        "()", ""
    ) == "current_timestamp":
        return "current_timestamp"
    return raw


def validate_governance_table_schema(connection) -> dict[str, int]:
    """Read-only exact engine/column/index gate for all governance tables."""

    contracts = _governance_table_schema_contract()
    placeholders = ", ".join(
        f"'{name}'" for name in sorted(contracts)
    )
    table_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
        "TABLE_COLLATION AS table_collation FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ("
        f"{placeholders}) ORDER BY BINARY TABLE_NAME"
    )).mappings().all()
    observed_tables = {
        str(row.get("table_name") or ""): dict(row) for row in table_rows
    }
    if set(observed_tables) != set(contracts):
        raise RuntimeError("生产治理表清单不完整或包含结构漂移")
    for name, row in observed_tables.items():
        if (
            str(row.get("engine") or "").casefold() != "innodb"
            or str(row.get("table_collation") or "").casefold()
            != GOVERNANCE_SCHEMA_COLLATION
        ):
            raise RuntimeError(f"生产治理表引擎或字符集漂移：{name}")

    column_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
        "ORDINAL_POSITION AS ordinal_position, COLUMN_TYPE AS column_type, "
        "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, "
        "EXTRA AS extra, CHARACTER_SET_NAME AS character_set_name, "
        "COLLATION_NAME AS collation_name "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY TABLE_NAME, ORDINAL_POSITION"
    )).mappings().all()
    observed_columns: dict[str, list[dict[str, Any]]] = {
        name: [] for name in contracts
    }
    for row in column_rows:
        table_name = str(row.get("table_name") or "")
        if table_name not in observed_columns:
            raise RuntimeError("生产治理字段归属表异常")
        observed_columns[table_name].append(dict(row))
    for table_name, contract in contracts.items():
        expected_columns = contract["columns"]
        rows = observed_columns[table_name]
        if len(rows) != len(expected_columns):
            raise RuntimeError(f"生产治理字段数量漂移：{table_name}")
        for ordinal, (row, expected) in enumerate(
            zip(rows, expected_columns),
            1,
        ):
            extra = str(row.get("extra") or "").casefold()
            character_type = expected["column_type"].split("(", 1)[0] in {
                "char", "varchar", "text", "mediumtext", "longtext",
            }
            if (
                int(row.get("ordinal_position") or 0) != ordinal
                or str(row.get("column_name") or "") != expected["name"]
                or str(row.get("column_type") or "").casefold()
                != expected["column_type"]
                or str(row.get("is_nullable") or "").upper()
                != expected["nullable"]
                or _normalized_governance_column_default(
                    row.get("column_default"), expected["column_type"]
                ) != _normalized_governance_column_default(
                    expected["default"], expected["column_type"]
                )
                or ("auto_increment" in extra)
                != expected["auto_increment"]
                or ("on update current_timestamp" in extra)
                != expected["on_update_current_timestamp"]
                or (
                    character_type
                    and (
                        str(row.get("character_set_name") or "").casefold()
                        != "utf8mb4"
                        or str(row.get("collation_name") or "").casefold()
                        != GOVERNANCE_SCHEMA_COLLATION
                    )
                )
                or (
                    not character_type
                    and (
                        row.get("character_set_name") is not None
                        or row.get("collation_name") is not None
                    )
                )
            ):
                raise RuntimeError(
                    f"生产治理字段契约漂移：{table_name}.{expected['name']}"
                )

    index_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, "
        "NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
        "COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
        "INDEX_TYPE AS index_type FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY TABLE_NAME, BINARY INDEX_NAME, SEQ_IN_INDEX"
    )).mappings().all()
    observed_indexes: dict[str, dict[str, dict[str, Any]]] = {
        name: {} for name in contracts
    }
    for row in index_rows:
        table_name = str(row.get("table_name") or "")
        index_name = str(row.get("index_name") or "")
        if table_name not in observed_indexes:
            raise RuntimeError("生产治理索引归属表异常")
        entry = observed_indexes[table_name].setdefault(index_name, {
            "non_unique": int(row.get("non_unique") or 0),
            "columns": [],
            "valid": True,
        })
        entry["valid"] = bool(
            entry["valid"]
            and entry["non_unique"] == int(row.get("non_unique") or 0)
            and int(row.get("seq_in_index") or 0)
            == len(entry["columns"]) + 1
            and row.get("sub_part") is None
            and str(row.get("index_type") or "").upper() == "BTREE"
        )
        entry["columns"].append(str(row.get("column_name") or ""))
    for table_name, contract in contracts.items():
        normalized = {
            name: (entry["non_unique"], tuple(entry["columns"]))
            for name, entry in observed_indexes[table_name].items()
            if entry["valid"]
        }
        if normalized != contract["indexes"]:
            raise RuntimeError(f"生产治理索引契约漂移：{table_name}")
    return {
        "table_count": len(contracts),
        "column_count": sum(
            len(contract["columns"]) for contract in contracts.values()
        ),
        "index_count": sum(
            len(contract["indexes"]) for contract in contracts.values()
        ),
    }


def ensure_strategy_governance_tables(
    *,
    engine: Any | None = None,
    trigger_ddl_executor: Callable[[str], None] | None = None,
) -> None:
    """Create governance tables, columns and indexes without trigger DDL.

    Normal callers use the shared runtime engine.  The production release
    broker supplies its separately authenticated, schema-scoped migration
    engine while all writers are fenced.  ``trigger_ddl_executor`` is retained
    for rolling compatibility but is never invoked.
    """

    if trigger_ddl_executor is not None and not callable(
        trigger_ddl_executor
    ):
        raise TypeError("trigger_ddl_executor must be callable")
    statements = governance_table_ddl_statements()
    schema_engine = engine or get_engine()
    with schema_engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        metric_entity_type = connection.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_metric_input' "
                "AND column_name='entity_type'"
            )
        ).scalar()
        if not metric_entity_type:
            connection.execute(text(
                "ALTER TABLE st_strategy_metric_input "
                "ADD COLUMN entity_type VARCHAR(24) NOT NULL "
                "DEFAULT 'STRATEGY' AFTER evidence_id"
            ))
        metric_columns = {
            str(row[0])
            for row in connection.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_metric_input'"
            )).all()
        }
        for column_name, definition in (
            ("evidence_protocol", "VARCHAR(80) NOT NULL DEFAULT 'LEGACY_UNVERIFIED'"),
            ("artifact_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("artifact_json", "LONGTEXT NULL"),
            ("source_dataset_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("evidence_revision_at", "DATETIME NULL"),
            ("verification_status", "VARCHAR(24) NOT NULL DEFAULT 'PENDING'"),
            ("funding_provenance", "VARCHAR(40) NOT NULL DEFAULT 'EXTERNAL_SUBMITTED'"),
            ("submitted_by", "VARCHAR(80) NOT NULL DEFAULT 'legacy_unknown'"),
            ("reviewed_by", "VARCHAR(80) NOT NULL DEFAULT ''"),
            ("reviewed_at", "DATETIME NULL"),
        ):
            if column_name not in metric_columns:
                connection.execute(text(
                    "ALTER TABLE st_strategy_metric_input "
                    f"ADD COLUMN {column_name} {definition}"
                ))
        connection.execute(text(
            "UPDATE st_strategy_metric_input "
            "SET artifact_hash=SHA2(CONCAT('legacy:', evidence_id), 256) "
            "WHERE artifact_hash='' OR artifact_hash IS NULL"
        ))
        connection.execute(text(
            "UPDATE st_strategy_metric_input "
            "SET source_dataset_hash=SHA2(CONCAT('legacy-dataset:', evidence_id), 256) "
            "WHERE source_dataset_hash='' OR source_dataset_hash IS NULL"
        ))
        for table_name in (
            "st_strategy_lifecycle_event",
            "st_strategy_governance_audit",
        ):
            payload_column = connection.execute(text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name=:table_name "
                "AND column_name='payload_json'"
            ), {"table_name": table_name}).scalar()
            if not payload_column:
                connection.execute(text(
                    f"ALTER TABLE {table_name} ADD COLUMN payload_json "
                    "LONGTEXT NULL"
                ))
        run_columns = {
            str(row[0])
            for row in connection.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_governance_run'"
            )).all()
        }
        for column_name, definition in (
            ("run_revision", "INT NOT NULL DEFAULT 1"),
            ("supersedes_run_uid", "CHAR(32) NOT NULL DEFAULT ''"),
            ("is_canonical", "TINYINT(1) NOT NULL DEFAULT 1"),
            ("source_status", "VARCHAR(24) NOT NULL DEFAULT 'missing'"),
            ("input_ready", "TINYINT(1) NOT NULL DEFAULT 0"),
            ("input_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("build_commit_sha", "VARCHAR(64) NOT NULL DEFAULT ''"),
            ("router_policy_version", "VARCHAR(80) NOT NULL DEFAULT ''"),
            ("router_snapshot_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("decision_hash", "CHAR(64) NULL"),
            ("result_json", "LONGTEXT NULL"),
            ("result_hash", "CHAR(64) NOT NULL DEFAULT ''"),
        ):
            if column_name not in run_columns:
                connection.execute(text(
                    f"ALTER TABLE st_strategy_governance_run "
                    f"ADD COLUMN {column_name} {definition}"
                ))
        connection.execute(text(
            "UPDATE st_strategy_governance_run SET result_json='{}' "
            "WHERE result_json IS NULL OR result_json=''"
        ))
        connection.execute(text(
            "UPDATE st_strategy_governance_run SET result_hash=SHA2(result_json, 256) "
            "WHERE result_hash='' OR result_hash IS NULL"
        ))
        result_json_nullable = connection.execute(text(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_strategy_governance_run' "
            "AND column_name='result_json'"
        )).scalar()
        if str(result_json_nullable or "").upper() != "NO":
            connection.execute(text(
                "ALTER TABLE st_strategy_governance_run "
                "MODIFY COLUMN result_json LONGTEXT NOT NULL"
            ))
        migration_row = connection.execute(text(
            "SELECT migration_hash "
            "FROM st_strategy_governance_schema_migration "
            "WHERE migration_key=:migration_key"
        ), {"migration_key": RUN_REVISION_MIGRATION_KEY}).mappings().first()
        if migration_row is None:
            historical_runs = connection.execute(text(
                "SELECT run_uid, trade_date FROM st_strategy_governance_run "
                "ORDER BY trade_date, created_at, run_uid"
            )).mappings().all()
            by_day: dict[str, list[str]] = defaultdict(list)
            for run in historical_runs:
                by_day[str(run.get("trade_date") or "")[:10]].append(
                    str(run.get("run_uid") or "")
                )
            for run_ids in by_day.values():
                previous_uid = ""
                for revision, run_uid in enumerate(run_ids, 1):
                    connection.execute(text(
                        "UPDATE st_strategy_governance_run "
                        "SET run_revision=:run_revision, "
                        "supersedes_run_uid=:supersedes_run_uid, "
                        "is_canonical=:is_canonical "
                        "WHERE run_uid=:run_uid"
                    ), {
                        "run_revision": revision,
                        "supersedes_run_uid": previous_uid,
                        "is_canonical": 1 if revision == len(run_ids) else 0,
                        "run_uid": run_uid,
                    })
                    previous_uid = run_uid
            connection.execute(text(
                "INSERT INTO st_strategy_governance_schema_migration "
                "(migration_key, migration_hash) "
                "VALUES (:migration_key, :migration_hash)"
            ), {
                "migration_key": RUN_REVISION_MIGRATION_KEY,
                "migration_hash": RUN_REVISION_MIGRATION_HASH,
            })
        elif str(migration_row.get("migration_hash") or "") != (
            RUN_REVISION_MIGRATION_HASH
        ):
            raise RuntimeError("治理运行修订迁移标记哈希不一致")
        revision_rows = connection.execute(text(
            "SELECT run_uid, trade_date, run_revision, "
            "supersedes_run_uid, is_canonical "
            "FROM st_strategy_governance_run "
            "ORDER BY trade_date, run_revision, created_at, run_uid"
        )).mappings().all()
        revisions_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in revision_rows:
            revisions_by_day[str(run.get("trade_date") or "")[:10]].append(
                dict(run)
            )
        for trade_day, day_rows in revisions_by_day.items():
            previous_uid = ""
            for expected_revision, run in enumerate(day_rows, 1):
                is_latest = expected_revision == len(day_rows)
                if (
                    _int(run.get("run_revision")) != expected_revision
                    or str(run.get("supersedes_run_uid") or "")
                    != previous_uid
                    or bool(_int(run.get("is_canonical"))) != is_latest
                ):
                    raise RuntimeError(
                        f"治理运行修订链不完整：{trade_day}；拒绝继续部署"
                    )
                previous_uid = str(run.get("run_uid") or "")
        canonical_index_columns = [
            str(row[0])
            for row in connection.execute(text(
                "SELECT column_name FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_governance_run' "
                "AND index_name='idx_strategy_governance_canonical' "
                "ORDER BY seq_in_index"
            )).all()
        ]
        if canonical_index_columns and canonical_index_columns != [
            "trade_date", "is_canonical", "run_revision",
        ]:
            connection.execute(text(
                "ALTER TABLE st_strategy_governance_run "
                "DROP INDEX idx_strategy_governance_canonical"
            ))
            canonical_index_columns = []
        if not canonical_index_columns:
            connection.execute(text(
                "ALTER TABLE st_strategy_governance_run "
                "ADD INDEX idx_strategy_governance_canonical "
                "(trade_date, is_canonical, run_revision)"
            ))
        for _index_name, hash_column, evidence_label in (
            _METRIC_GLOBAL_UNIQUE_INDEXES
        ):
            duplicate = connection.execute(text(
                f"SELECT {hash_column} AS duplicate_hash, COUNT(*) AS cnt "
                "FROM st_strategy_metric_input "
                f"WHERE {hash_column}<>'' "
                f"GROUP BY {hash_column} HAVING COUNT(*)>1 LIMIT 1"
            )).mappings().first()
            if duplicate is not None:
                raise RuntimeError(
                    f"{evidence_label}已被多条资金证据复用，必须先隔离冲突记录："
                    f"{duplicate.get('duplicate_hash')}"
                )
        allocation_columns = {
            str(row[0])
            for row in connection.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_allocation_snapshot'"
            )).all()
        }
        for column_name, definition in (
            ("target_version", "VARCHAR(160) NOT NULL DEFAULT ''"),
            ("funding_gate_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("market_state", "VARCHAR(40) NOT NULL DEFAULT ''"),
            ("market_match_score", "DECIMAL(10,4) NULL"),
            ("router_decision_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("lifecycle_status", "VARCHAR(16) NOT NULL DEFAULT ''"),
            ("lifecycle_status_label", "VARCHAR(40) NOT NULL DEFAULT ''"),
            ("lifecycle_risk_multiplier", "DECIMAL(8,4) NOT NULL DEFAULT 0"),
            ("base_competitive_weight_pct", "DECIMAL(10,4) NOT NULL DEFAULT 0"),
            ("member_sleeves_json", "LONGTEXT NULL"),
            ("member_sleeve_hash", "CHAR(64) NOT NULL DEFAULT ''"),
            ("cash_discount_bp", "INT NOT NULL DEFAULT 0"),
        ):
            if column_name not in allocation_columns:
                connection.execute(text(
                    "ALTER TABLE st_strategy_allocation_snapshot "
                    f"ADD COLUMN {column_name} {definition}"
                ))
        connection.execute(text(
            "UPDATE st_strategy_allocation_snapshot "
            "SET member_sleeves_json='[]' "
            "WHERE member_sleeves_json IS NULL OR member_sleeves_json=''"
        ))
        member_json_nullable = connection.execute(text(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_strategy_allocation_snapshot' "
            "AND column_name='member_sleeves_json'"
        )).scalar()
        if str(member_json_nullable or "").upper() != "NO":
            connection.execute(text(
                "ALTER TABLE st_strategy_allocation_snapshot "
                "MODIFY COLUMN member_sleeves_json LONGTEXT NOT NULL"
            ))
        for table_name, index_name, columns in (
            (
                "st_strategy_governance_run",
                "uk_strategy_governance_decision",
                "decision_hash",
            ),
            (
                "st_strategy_metric_input",
                "uk_strategy_metric_version_date",
                "entity_type, strategy_key, strategy_version, as_of_date, window_days",
            ),
            (
                "st_strategy_metric_input",
                "uk_strategy_metric_artifact",
                "entity_type, strategy_key, strategy_version, window_days, artifact_hash",
            ),
            (
                "st_strategy_metric_input",
                "uk_strategy_metric_dataset",
                "entity_type, strategy_key, strategy_version, window_days, source_dataset_hash",
            ),
            (
                "st_strategy_metric_input",
                "uk_strategy_metric_artifact_global",
                "artifact_hash",
            ),
            (
                "st_strategy_metric_input",
                "uk_strategy_metric_dataset_global",
                "source_dataset_hash",
            ),
        ):
            actual_columns = [
                str(row[0])
                for row in connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.statistics "
                        "WHERE table_schema=DATABASE() AND table_name=:table_name "
                        "AND index_name=:index_name ORDER BY seq_in_index"
                    ),
                    {"table_name": table_name, "index_name": index_name},
                ).all()
            ]
            expected_columns = [item.strip() for item in columns.split(",")]
            if actual_columns and actual_columns != expected_columns:
                connection.execute(text(
                    f"ALTER TABLE {table_name} DROP INDEX {index_name}"
                ))
                actual_columns = []
            index_exists = connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.statistics "
                    "WHERE table_schema=DATABASE() AND table_name=:table_name "
                    "AND index_name=:index_name"
                ),
                {"table_name": table_name, "index_name": index_name},
            ).scalar()
            if not index_exists or not actual_columns:
                connection.execute(text(
                    f"ALTER TABLE {table_name} ADD UNIQUE INDEX "
                    f"{index_name} ({columns})"
                ))
        _ensure_strategy_content_hash_schema(connection)


def _audit_record(
    *, entity_type: str, entity_key: str, action: str, reason: str,
    operator: str, before: Any, after: Any, evidence: Any,
) -> tuple[str, dict[str, Any]]:
    stored_reason = str(reason or "")[:500]
    stored_operator = str(operator or "system")[:80]
    payload = {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "action": action,
        "reason": stored_reason,
        "operator": stored_operator,
        "before": before,
        "after": after,
        "evidence": evidence,
        "nonce": uuid.uuid4().hex,
    }
    return (
        """
        INSERT INTO st_strategy_governance_audit
        (audit_id, entity_type, entity_key, action, reason, operator_name,
         before_json, after_json, evidence_json, payload_json, audit_hash)
        VALUES (:audit_id, :entity_type, :entity_key, :action, :reason, :operator,
                :before_json, :after_json, :evidence_json, :payload_json,
                :audit_hash)
        """,
        {
            "audit_id": uuid.uuid4().hex,
            "entity_type": entity_type,
            "entity_key": entity_key,
            "action": action,
            "reason": stored_reason,
            "operator": stored_operator,
            "before_json": _json_text(before),
            "after_json": _json_text(after),
            "evidence_json": _json_text(evidence),
            "payload_json": _json_text(payload),
            "audit_hash": _digest(payload),
        },
    )


def _append_audit_connection(
    connection, *, entity_type: str, entity_key: str, action: str,
    reason: str, operator: str, before: Any, after: Any, evidence: Any,
) -> None:
    sql, params = _audit_record(
        entity_type=entity_type,
        entity_key=entity_key,
        action=action,
        reason=reason,
        operator=operator,
        before=before,
        after=after,
        evidence=evidence,
    )
    connection.execute(text(sql), params)


def _append_audit(
    *, entity_type: str, entity_key: str, action: str, reason: str,
    operator: str, before: Any, after: Any, evidence: Any,
) -> None:
    with get_engine().begin() as connection:
        _append_audit_connection(
            connection,
            entity_type=entity_type,
            entity_key=entity_key,
            action=action,
            reason=reason,
            operator=operator,
            before=before,
            after=after,
            evidence=evidence,
        )


def _recovery_conditions(status: str = "SHADOW") -> list[str]:
    common = [
        "扣除费用后净期望大于0，并保留至少3倍往返成本安全边际",
        "成熟交易不少于80笔且覆盖不少于60个交易日",
        "盈亏比不低于1.10、利润因子不低于1.30、最大回撤不超过12%",
        "5段Walk-Forward至少4段为正，1.5倍成本压力下仍为正",
    ]
    if status == "SUSPENDED":
        return ["先以新证据恢复为影子观察，不允许直接恢复资金", *common]
    if status == "RETIRED":
        return ["原版本不可恢复；必须注册新版本并从影子观察重新验证"]
    return common


def seed_manifest_strategies() -> None:
    """Register the current manifest as the initial inventory, not a cap."""

    manifest = load_stock_manifest()
    manifest_version = str(manifest["manifest_version"])
    frozen_at = _normalize_evidence_revision(manifest.get("frozen_at"))
    if not frozen_at:
        raise RuntimeError("股票策略清单缺少有效冻结时间")
    by_key = {str(item["key"]): item for item in manifest["strategies"]}
    for catalog_item in stock_strategy_catalog():
        key = str(catalog_item["key"])
        raw = by_key[key]
        # This is the exact immutable version written to st_trade_intent_v2.
        # Keeping the strategy suffix is essential: the same manifest contains
        # several independently evaluated strategies.
        strategy_version = f"{manifest_version}:{key}"
        evaluator_config = {
            "score_field": raw.get("score_field"),
            "model_version": manifest.get("model_version"),
            "market_regime_multipliers": _manifest_regime_multipliers(key),
            "market_router_policy_version": MARKET_ROUTER_POLICY_VERSION,
            "market_state_config_version": load_market_state_config()[
                "config_version"
            ],
            "market_state_config_hash": market_state_config_hash(),
        }
        parameters = dict(raw.get("parameters") or {})
        version_hash = _strategy_version_digest(
            strategy_key=key,
            version=strategy_version,
            evaluator_type="manifest_score_adapter",
            evaluator_config=evaluator_config,
            parameters=parameters,
            source_kind="immutable_manifest",
        )
        content_hash = _strategy_content_digest(
            strategy_key=key,
            evaluator_type="manifest_score_adapter",
            evaluator_config=evaluator_config,
            parameters=parameters,
            source_kind="immutable_manifest",
        )
        existing = _db_read(
            "SELECT current_version, current_status, discovery_mode "
            "FROM st_strategy_registry WHERE strategy_key=:key",
            {"key": key},
        )
        versions = _db_read(
            "SELECT version_hash, content_hash FROM st_strategy_version "
            "WHERE strategy_key=:key AND version=:version",
            {"key": key, "version": strategy_version},
        )
        if versions and (
            str(versions[0].get("version_hash") or "") != version_hash
            or str(versions[0].get("content_hash") or "") != content_hash
        ):
            raise RuntimeError("清单策略版本与已冻结版本内容冲突")
        duplicate_content = _db_read(
            "SELECT version FROM st_strategy_version "
            "WHERE strategy_key=:key AND content_hash=:content_hash LIMIT 1",
            {"key": key, "content_hash": content_hash},
        )
        if (
            duplicate_content
            and str(duplicate_content[0].get("version") or "")
            != strategy_version
        ):
            raise RuntimeError("清单策略新版本与已有版本内容完全相同")
        old_version = str(existing[0].get("current_version") or "") if existing else ""
        previous_status = str(existing[0].get("current_status") or "SHADOW") if existing else "SHADOW"
        runtime_owned = bool(
            existing and str(existing[0].get("discovery_mode") or "") == "dynamic"
        )
        changed = not existing or (
            not runtime_owned and old_version != strategy_version
        )
        params = {
            "key": key,
            "name": catalog_item["name"],
            "category": catalog_item["category"],
            "family": key,
            "description": catalog_item.get("description") or "",
            "version": strategy_version,
            "version_hash": version_hash,
            "content_hash": content_hash,
            "recovery": _json_text(_recovery_conditions()),
            "evaluator": _json_text(evaluator_config),
            "parameters": _json_text(parameters),
            "version_created_at": frozen_at,
        }
        with get_engine().begin() as connection:
            if not versions:
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_version
                    (strategy_key, version, version_hash, content_hash,
                     evaluator_type, evaluator_config_json, parameters_json,
                     source_kind, created_by, created_at)
                    VALUES (:key, :version, :version_hash, :content_hash,
                            'manifest_score_adapter', :evaluator, :parameters,
                            'immutable_manifest', 'manifest_registry',
                            :version_created_at)
                    """
                ), params)
            if not existing:
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_registry
                    (strategy_key, strategy_name, category, family_key,
                     description, owner_name, discovery_mode, enabled,
                     current_version, current_status, status_reason,
                     recovery_conditions_json)
                    VALUES (:key, :name, :category, :family, :description,
                            'manifest_registry', 'manifest_adapter', 1,
                            :version, 'SHADOW', '等待独立前向证据', :recovery)
                    """
                ), params)
            elif not runtime_owned:
                connection.execute(text(
                    """
                    UPDATE st_strategy_registry
                    SET strategy_name=:name, category=:category,
                        description=:description,
                        current_status=IF(current_version<>:version,
                                          'SHADOW', current_status),
                        status_reason=IF(current_version<>:version,
                          '清单版本更新，重新进入影子验证', status_reason),
                        current_version=:version
                    WHERE strategy_key=:key AND discovery_mode<>'dynamic'
                      AND current_version=:old_version
                    """
                ), {**params, "old_version": old_version})
            if changed:
                reason = "清单版本更新，重新进入影子验证" if existing else "初始策略清单注册为影子观察"
                event_payload = {
                    "entity_type": "STRATEGY", "entity_key": key,
                    "old_version": old_version, "new_version": strategy_version,
                    "previous_status": previous_status,
                    "next_status": "SHADOW", "reason": reason,
                }
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_lifecycle_event
                    (event_id, entity_type, entity_key, entity_version,
                     previous_status, next_status, reason, trigger_type,
                     evidence_json, payload_json, event_hash, operator_name)
                    VALUES (:event_id, 'STRATEGY', :entity_key,
                            :entity_version, :previous_status, 'SHADOW',
                            :reason, 'VERSION_REGISTRATION', :evidence_json,
                            :payload_json, :event_hash, 'manifest_registry')
                    ON DUPLICATE KEY UPDATE event_hash=event_hash
                    """
                ), {
                    "event_id": uuid.uuid4().hex,
                    "entity_key": key,
                    "entity_version": strategy_version,
                    "previous_status": previous_status,
                    "reason": reason,
                    "evidence_json": _json_text({"old_version": old_version, "new_version": strategy_version, "version_hash": version_hash}),
                    "payload_json": _json_text(event_payload),
                    "event_hash": _digest(event_payload),
                })
                _append_audit_connection(
                    connection,
                    entity_type="STRATEGY",
                    entity_key=key,
                    action="REGISTER_VERSION",
                    reason=reason,
                    operator="manifest_registry",
                    before={
                        "version": old_version,
                        "status": previous_status,
                    },
                    after={
                        "version": strategy_version,
                        "status": "SHADOW",
                    },
                    evidence={"version_hash": version_hash},
                )


def seed_v3_strategies() -> None:
    """Register every immutable V3 sleeve as an independent strategy."""

    config = load_v3_config()
    model_version = str(config.get("strategy_version") or "").strip()
    frozen_at = _normalize_evidence_revision(config.get("frozen_at"))
    sleeves = config.get("sleeves")
    if not model_version or not frozen_at or not isinstance(sleeves, dict):
        raise RuntimeError("V3策略配置缺少版本、冻结时间或策略袖套")
    for raw_key, raw_sleeve in sorted(sleeves.items()):
        key = validate_strategy_key(str(raw_key))
        sleeve = raw_sleeve if isinstance(raw_sleeve, dict) else {}
        strategy_version = f"{model_version}:{key}"
        evaluator_config = {
            "strategy_key": key,
            "model_version": model_version,
            "evidence_owner": "primary_strategy_key",
            "market_regime_policy": "UNCONFIGURED_FAIL_CLOSED",
        }
        version_hash = _strategy_version_digest(
            strategy_key=key,
            version=strategy_version,
            evaluator_type="v3_primary_sleeve_adapter",
            evaluator_config=evaluator_config,
            parameters=dict(sleeve),
            source_kind="immutable_v3_sleeve",
        )
        content_hash = _strategy_content_digest(
            strategy_key=key,
            evaluator_type="v3_primary_sleeve_adapter",
            evaluator_config=evaluator_config,
            parameters=dict(sleeve),
            source_kind="immutable_v3_sleeve",
        )
        existing = _db_read(
            "SELECT current_version, current_status, discovery_mode "
            "FROM st_strategy_registry WHERE strategy_key=:key",
            {"key": key},
        )
        versions = _db_read(
            "SELECT version_hash, content_hash FROM st_strategy_version "
            "WHERE strategy_key=:key AND version=:version",
            {"key": key, "version": strategy_version},
        )
        if versions and (
            str(versions[0].get("version_hash") or "") != version_hash
            or str(versions[0].get("content_hash") or "") != content_hash
        ):
            raise RuntimeError("V3策略版本与已冻结版本内容冲突")
        duplicate_content = _db_read(
            "SELECT version FROM st_strategy_version "
            "WHERE strategy_key=:key AND content_hash=:content_hash LIMIT 1",
            {"key": key, "content_hash": content_hash},
        )
        if (
            duplicate_content
            and str(duplicate_content[0].get("version") or "")
            != strategy_version
        ):
            raise RuntimeError("V3策略新版本与已有版本内容完全相同")
        old_version = (
            str(existing[0].get("current_version") or "") if existing else ""
        )
        previous_status = (
            str(existing[0].get("current_status") or "SHADOW")
            if existing else "SHADOW"
        )
        runtime_owned = bool(
            existing
            and str(existing[0].get("discovery_mode") or "") == "dynamic"
        )
        changed = not existing or (
            not runtime_owned and old_version != strategy_version
        )
        params = {
            "key": key,
            "name": V3_STRATEGY_LABELS.get(key, key),
            "description": str(sleeve.get("description") or "V3独立策略袖套"),
            "version": strategy_version,
            "version_hash": version_hash,
            "content_hash": content_hash,
            "version_created_at": frozen_at,
            "evaluator": _json_text(evaluator_config),
            "parameters": _json_text(sleeve),
            "recovery": _json_text(_recovery_conditions()),
        }
        with get_engine().begin() as connection:
            if not versions:
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_version
                    (strategy_key, version, version_hash, content_hash,
                     evaluator_type, evaluator_config_json, parameters_json,
                     source_kind, created_by, created_at)
                    VALUES (:key, :version, :version_hash, :content_hash,
                            'v3_primary_sleeve_adapter', :evaluator,
                            :parameters, 'immutable_v3_sleeve', 'v3_registry',
                            :version_created_at)
                    """
                ), params)
            if not existing:
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_registry
                    (strategy_key, strategy_name, category, family_key,
                     description, owner_name, discovery_mode, enabled,
                     current_version, current_status, status_reason,
                     recovery_conditions_json)
                    VALUES (:key, :name, 'V3前向策略', :key, :description,
                            'v3_registry', 'v3_sleeve_adapter', 1,
                            :version, 'SHADOW', '等待版本绑定的真实前向证据',
                            :recovery)
                    """
                ), params)
            elif not runtime_owned:
                connection.execute(text(
                    """
                    UPDATE st_strategy_registry
                    SET strategy_name=:name, category='V3前向策略',
                        description=:description,
                        current_status=IF(current_version<>:version,
                                          'SHADOW', current_status),
                        status_reason=IF(current_version<>:version,
                          'V3版本更新，重新进入影子验证', status_reason),
                        current_version=:version
                    WHERE strategy_key=:key AND discovery_mode<>'dynamic'
                      AND current_version=:old_version
                    """
                ), {**params, "old_version": old_version})
            if changed:
                reason = (
                    "V3版本更新，重新进入影子验证"
                    if existing else "初始V3策略注册为影子观察"
                )
                event_payload = {
                    "entity_type": "STRATEGY",
                    "entity_key": key,
                    "old_version": old_version,
                    "new_version": strategy_version,
                    "previous_status": previous_status,
                    "next_status": "SHADOW",
                    "reason": reason,
                }
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_lifecycle_event
                    (event_id, entity_type, entity_key, entity_version,
                     previous_status, next_status, reason, trigger_type,
                     evidence_json, payload_json, event_hash, operator_name)
                    VALUES (:event_id, 'STRATEGY', :entity_key,
                            :entity_version, :previous_status, 'SHADOW',
                            :reason, 'VERSION_REGISTRATION', :evidence_json,
                            :payload_json, :event_hash, 'v3_registry')
                    ON DUPLICATE KEY UPDATE event_hash=event_hash
                    """
                ), {
                    "event_id": uuid.uuid4().hex,
                    "entity_key": key,
                    "entity_version": strategy_version,
                    "previous_status": previous_status,
                    "reason": reason,
                    "evidence_json": _json_text({
                        "old_version": old_version,
                        "new_version": strategy_version,
                        "version_hash": version_hash,
                    }),
                    "payload_json": _json_text(event_payload),
                    "event_hash": _digest(event_payload),
                })
                _append_audit_connection(
                    connection,
                    entity_type="STRATEGY",
                    entity_key=key,
                    action="REGISTER_VERSION",
                    reason=reason,
                    operator="v3_registry",
                    before={
                        "version": old_version,
                        "status": previous_status,
                    },
                    after={
                        "version": strategy_version,
                        "status": "SHADOW",
                    },
                    evidence={"version_hash": version_hash},
                )


DEFAULT_SEEDED_COMBINATIONS = (
    (
        "trend_attack", "趋势进攻组合", "趋势、短线与日内的分层进攻组合",
        {"main_wave": 0.50, "short_term": 0.30, "ultra_short": 0.20},
    ),
    (
        "balanced_rotation", "均衡轮动组合", "短线轮动、波段与趋势的均衡组合",
        {"short_term": 0.40, "swing": 0.35, "main_wave": 0.25},
    ),
    (
        "defensive_observation", "防守观察组合", "以波段和低频轮动为主的防守观察组合",
        {"swing": 0.60, "short_term": 0.40},
    ),
    (
        "v3_mainline_attack", "V3主线进攻组合",
        "右侧主升、板块扩散和低位点火的主线组合",
        {"right_side_trend": 0.45, "theme_diffusion": 0.35,
         "low_base_ignition": 0.20},
    ),
    (
        "v3_event_rotation", "V3事件轮动组合",
        "事件漂移、盘中超预期和板块扩散的催化组合",
        {"event_drift": 0.40, "intraday_surprise": 0.35,
         "theme_diffusion": 0.25},
    ),
    (
        "v3_defensive_repair", "V3防守修复组合",
        "质量动量、弱市主线和超跌修复的防守组合",
        {"quality_momentum": 0.40,
         "weak_market_structural_mainline": 0.35,
         "oversold_reversal": 0.25},
    ),
)


def seed_default_combinations() -> None:
    registered = {
        row["strategy_key"]: row["current_version"]
        for row in load_registry()
    }
    for key, name, description, weights in DEFAULT_SEEDED_COMBINATIONS:
        available = {member: weight for member, weight in weights.items() if member in registered}
        if len(available) < 2:
            continue
        total = sum(available.values())
        members = [
            {
                "strategy_key": member,
                "strategy_version": registered[member],
                "weight": round(weight / total, 8),
            }
            for member, weight in available.items()
        ]
        constraints = _validated_combination_constraints({})
        config_hash = _digest({
            "members": members,
            "constraints": constraints,
        })
        version = f"seed-{config_hash[:16]}"
        existing = _db_read(
            "SELECT owner_name, current_version, current_status "
            "FROM st_strategy_combination WHERE combination_key=:key",
            {"key": key},
        )
        system_owned = bool(
            not existing
            or str(existing[0].get("owner_name") or "") == "system_seed"
        )
        if existing and not system_owned:
            continue
        old_version = (
            str(existing[0].get("current_version") or "") if existing else ""
        )
        previous_status = (
            str(existing[0].get("current_status") or "SHADOW")
            if existing else "SHADOW"
        )
        changed = system_owned and (not existing or old_version != version)
        params = {
            "key": key, "name": name, "description": description,
            "version": version,
            "members": _json_text(members),
            "constraints": _json_text(constraints),
            "config_hash": config_hash,
        }
        with get_engine().begin() as connection:
            connection.execute(text(
                """
                INSERT INTO st_strategy_combination_version
                (combination_key, version, members_json, constraints_json,
                 config_hash, created_by)
                VALUES (:key, :version, :members, :constraints, :config_hash,
                        'system_seed')
                ON DUPLICATE KEY UPDATE config_hash=config_hash
                """
            ), params)
            if changed:
                reason = (
                    "组合成员版本变化，重新进入影子验证"
                    if existing else "系统组合注册为影子观察"
                )
                event_payload = {
                    "entity_type": "COMBINATION",
                    "entity_key": key,
                    "old_version": old_version,
                    "new_version": version,
                    "previous_status": previous_status,
                    "next_status": "SHADOW",
                    "reason": reason,
                    "config_hash": config_hash,
                }
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_lifecycle_event
                    (event_id, entity_type, entity_key, entity_version,
                     previous_status, next_status, reason, trigger_type,
                     evidence_json, payload_json, event_hash, operator_name)
                    VALUES (:event_id, 'COMBINATION', :entity_key,
                            :entity_version, :previous_status, 'SHADOW',
                            :reason, 'VERSION_REGISTRATION', :evidence_json,
                            :payload_json, :event_hash, 'system_seed')
                    ON DUPLICATE KEY UPDATE event_hash=event_hash
                    """
                ), {
                    "event_id": uuid.uuid4().hex,
                    "entity_key": key,
                    "entity_version": version,
                    "previous_status": previous_status,
                    "reason": reason,
                    "evidence_json": _json_text({
                        "old_version": old_version,
                        "new_version": version,
                        "config_hash": config_hash,
                    }),
                    "payload_json": _json_text(event_payload),
                    "event_hash": _digest(event_payload),
                })
                _append_audit_connection(
                    connection,
                    entity_type="COMBINATION",
                    entity_key=key,
                    action="REGISTER_VERSION",
                    reason=reason,
                    operator="system_seed",
                    before={
                        "version": old_version,
                        "status": previous_status,
                    },
                    after={"version": version, "status": "SHADOW"},
                    evidence={"config_hash": config_hash},
                )
            connection.execute(text(
                """
                INSERT INTO st_strategy_combination
                (combination_key, combination_name, description, owner_name,
                 enabled, current_version, current_status, status_reason)
                VALUES (:key, :name, :description, 'system_seed', 1, :version,
                        'SHADOW', '等待组合独立验证')
                ON DUPLICATE KEY UPDATE
                    combination_name=IF(owner_name='system_seed',
                                        VALUES(combination_name),
                                        combination_name),
                    description=IF(owner_name='system_seed',
                                   VALUES(description), description),
                    current_status=IF(owner_name='system_seed'
                                      AND current_version<>VALUES(current_version),
                                      'SHADOW', current_status),
                    status_reason=IF(owner_name='system_seed'
                                     AND current_version<>VALUES(current_version),
                                     '组合成员版本变化，重新进入影子验证',
                                     status_reason),
                    current_version=IF(owner_name='system_seed',
                                       VALUES(current_version), current_version)
                """
            ), params)


def _default_governance_seed_contract() -> dict[str, Any]:
    manifest = load_stock_manifest()
    manifest_version = str(manifest["manifest_version"])
    manifest_by_key = {
        str(item["key"]): item for item in manifest["strategies"]
    }
    market_config = load_market_state_config()
    strategies: dict[str, dict[str, Any]] = {}
    for catalog_item in stock_strategy_catalog():
        key = str(catalog_item["key"])
        raw = manifest_by_key[key]
        version = f"{manifest_version}:{key}"
        evaluator = {
            "score_field": raw.get("score_field"),
            "model_version": manifest.get("model_version"),
            "market_regime_multipliers": _manifest_regime_multipliers(key),
            "market_router_policy_version": MARKET_ROUTER_POLICY_VERSION,
            "market_state_config_version": market_config["config_version"],
            "market_state_config_hash": market_state_config_hash(),
        }
        parameters = dict(raw.get("parameters") or {})
        strategies[key] = {
            "strategy_key": key,
            "strategy_name": str(catalog_item["name"]),
            "category": str(catalog_item["category"]),
            "family_key": key,
            "description": str(catalog_item.get("description") or ""),
            "owner_name": "manifest_registry",
            "discovery_mode": "manifest_adapter",
            "enabled": 1,
            "current_version": version,
            "evaluator_type": "manifest_score_adapter",
            "evaluator_config": evaluator,
            "parameters": parameters,
            "source_kind": "immutable_manifest",
            "created_by": "manifest_registry",
            "version_hash": _strategy_version_digest(
                strategy_key=key,
                version=version,
                evaluator_type="manifest_score_adapter",
                evaluator_config=evaluator,
                parameters=parameters,
                source_kind="immutable_manifest",
            ),
            "content_hash": _strategy_content_digest(
                strategy_key=key,
                evaluator_type="manifest_score_adapter",
                evaluator_config=evaluator,
                parameters=parameters,
                source_kind="immutable_manifest",
            ),
        }

    v3_config = load_v3_config()
    model_version = str(v3_config.get("strategy_version") or "").strip()
    sleeves = v3_config.get("sleeves")
    if not model_version or not isinstance(sleeves, dict):
        raise RuntimeError("V3策略配置不能生成默认播种验收契约")
    for raw_key, raw_sleeve in sorted(sleeves.items()):
        key = validate_strategy_key(str(raw_key))
        sleeve = raw_sleeve if isinstance(raw_sleeve, dict) else {}
        version = f"{model_version}:{key}"
        evaluator = {
            "strategy_key": key,
            "model_version": model_version,
            "evidence_owner": "primary_strategy_key",
            "market_regime_policy": "UNCONFIGURED_FAIL_CLOSED",
        }
        parameters = dict(sleeve)
        strategies[key] = {
            "strategy_key": key,
            "strategy_name": str(V3_STRATEGY_LABELS.get(key, key)),
            "category": "V3前向策略",
            "family_key": key,
            "description": str(
                sleeve.get("description") or "V3独立策略袖套"
            ),
            "owner_name": "v3_registry",
            "discovery_mode": "v3_sleeve_adapter",
            "enabled": 1,
            "current_version": version,
            "evaluator_type": "v3_primary_sleeve_adapter",
            "evaluator_config": evaluator,
            "parameters": parameters,
            "source_kind": "immutable_v3_sleeve",
            "created_by": "v3_registry",
            "version_hash": _strategy_version_digest(
                strategy_key=key,
                version=version,
                evaluator_type="v3_primary_sleeve_adapter",
                evaluator_config=evaluator,
                parameters=parameters,
                source_kind="immutable_v3_sleeve",
            ),
            "content_hash": _strategy_content_digest(
                strategy_key=key,
                evaluator_type="v3_primary_sleeve_adapter",
                evaluator_config=evaluator,
                parameters=parameters,
                source_kind="immutable_v3_sleeve",
            ),
        }

    combinations: dict[str, dict[str, Any]] = {}
    versions = {
        key: contract["current_version"]
        for key, contract in strategies.items()
    }
    for key, name, description, weights in DEFAULT_SEEDED_COMBINATIONS:
        available = {
            member: weight for member, weight in weights.items()
            if member in versions
        }
        if len(available) < 2:
            continue
        total = sum(available.values())
        members = [
            {
                "strategy_key": member,
                "strategy_version": versions[member],
                "weight": round(weight / total, 8),
            }
            for member, weight in available.items()
        ]
        constraints = _validated_combination_constraints({})
        config_hash = _digest({
            "members": members,
            "constraints": constraints,
        })
        combinations[key] = {
            "combination_key": key,
            "combination_name": name,
            "description": description,
            "owner_name": "system_seed",
            "enabled": 1,
            "current_version": f"seed-{config_hash[:16]}",
            "members": members,
            "constraints": constraints,
            "config_hash": config_hash,
            "created_by": "system_seed",
        }
    return {
        "strategies": strategies,
        "combinations": combinations,
    }


def _strict_seed_json(value: Any, *, expected_type: type) -> Any:
    try:
        parsed = value if isinstance(value, expected_type) else json.loads(
            str(value)
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("默认治理播种JSON损坏") from exc
    if not isinstance(parsed, expected_type):
        raise RuntimeError("默认治理播种JSON类型漂移")
    return parsed


def validate_default_governance_seed_contract(
    engine: Any | None = None,
    *,
    require_initial_shadow: bool = False,
) -> dict[str, Any]:
    """Read-only proof that every built-in seed has its exact frozen version."""

    if type(require_initial_shadow) is not bool:
        raise TypeError("require_initial_shadow must be bool")
    expected = _default_governance_seed_contract()
    strategy_contracts = expected["strategies"]
    combination_contracts = expected["combinations"]
    runtime_engine = engine or get_engine()
    strategy_params = {
        f"strategy_{index}": key
        for index, key in enumerate(sorted(strategy_contracts))
    }
    strategy_placeholders = ", ".join(
        f":{key}" for key in strategy_params
    )
    combination_params = {
        f"combination_{index}": key
        for index, key in enumerate(sorted(combination_contracts))
    }
    combination_placeholders = ", ".join(
        f":{key}" for key in combination_params
    )
    with runtime_engine.connect() as connection:
        strategy_rows = connection.execute(text(
            "SELECT r.strategy_key, r.strategy_name, r.category, "
            "r.family_key, r.description, r.owner_name, r.discovery_mode, "
            "r.enabled, r.current_version, r.current_status, "
            "r.recovery_conditions_json, v.version_hash, v.content_hash, "
            "v.evaluator_type, v.evaluator_config_json, v.parameters_json, "
            "v.source_kind, v.created_by FROM st_strategy_registry r "
            "LEFT JOIN st_strategy_version v ON "
            "v.strategy_key=r.strategy_key AND v.version=r.current_version "
            f"WHERE r.strategy_key IN ({strategy_placeholders}) "
            "ORDER BY BINARY r.strategy_key"
        ), strategy_params).mappings().all()
        combination_rows = connection.execute(text(
            "SELECT c.combination_key, c.combination_name, c.description, "
            "c.owner_name, c.enabled, c.current_version, c.current_status, "
            "v.members_json, v.constraints_json, v.config_hash, v.created_by "
            "FROM st_strategy_combination c LEFT JOIN "
            "st_strategy_combination_version v ON "
            "v.combination_key=c.combination_key "
            "AND v.version=c.current_version "
            f"WHERE c.combination_key IN ({combination_placeholders}) "
            "ORDER BY BINARY c.combination_key"
        ), combination_params).mappings().all()
    observed_strategies = {
        str(row.get("strategy_key") or ""): dict(row)
        for row in strategy_rows
    }
    if set(observed_strategies) != set(strategy_contracts):
        raise RuntimeError("默认治理策略播种集合不完整")
    for key, contract in strategy_contracts.items():
        row = observed_strategies[key]
        for field in (
            "strategy_name", "category", "family_key", "description",
            "owner_name", "discovery_mode", "current_version",
            "version_hash", "content_hash", "evaluator_type",
            "source_kind", "created_by",
        ):
            if str(row.get(field) or "") != str(contract[field]):
                raise RuntimeError(f"默认治理策略播种漂移：{key}.{field}")
        status = str(row.get("current_status") or "")
        if (
            int(row.get("enabled") or 0) != 1
            or status not in LIFECYCLE_LABELS
            or (require_initial_shadow and status != "SHADOW")
            or _strict_seed_json(
                row.get("evaluator_config_json"), expected_type=dict
            ) != contract["evaluator_config"]
            or _strict_seed_json(
                row.get("parameters_json"), expected_type=dict
            ) != contract["parameters"]
            or _strict_seed_json(
                row.get("recovery_conditions_json"), expected_type=list
            ) != _recovery_conditions()
        ):
            raise RuntimeError(f"默认治理策略播种状态或内容漂移：{key}")

    observed_combinations = {
        str(row.get("combination_key") or ""): dict(row)
        for row in combination_rows
    }
    if set(observed_combinations) != set(combination_contracts):
        raise RuntimeError("默认治理组合播种集合不完整")
    for key, contract in combination_contracts.items():
        row = observed_combinations[key]
        for field in (
            "combination_name", "description", "owner_name",
            "current_version", "config_hash", "created_by",
        ):
            if str(row.get(field) or "") != str(contract[field]):
                raise RuntimeError(f"默认治理组合播种漂移：{key}.{field}")
        status = str(row.get("current_status") or "")
        if (
            int(row.get("enabled") or 0) != 1
            or status not in LIFECYCLE_LABELS
            or (require_initial_shadow and status != "SHADOW")
            or _strict_seed_json(
                row.get("members_json"), expected_type=list
            ) != contract["members"]
            or _strict_seed_json(
                row.get("constraints_json"), expected_type=dict
            ) != contract["constraints"]
        ):
            raise RuntimeError(f"默认治理组合播种状态或内容漂移：{key}")
    contract_hash = _digest(expected)
    return {
        "seeded_strategy_count": len(strategy_contracts),
        "seeded_combination_count": len(combination_contracts),
        "seed_contract_hash": contract_hash,
        "initial_shadow_required": require_initial_shadow,
    }


def seed_governance_registry() -> None:
    """Idempotently seed governance rows after the schema is verified."""

    global _SEED_READY
    if _SEED_READY:
        return
    with _SEED_LOCK:
        if _SEED_READY:
            return
        seed_manifest_strategies()
        seed_v3_strategies()
        seed_default_combinations()
        _SEED_READY = True


def validate_prepared_governance_runtime(
    engine: Any | None = None,
) -> dict[str, Any]:
    """Read-only production gate for the pre-migrated governance schema."""

    runtime_engine = engine or get_engine()
    expected_migrations = {
        RUN_REVISION_MIGRATION_KEY: RUN_REVISION_MIGRATION_HASH,
        STRATEGY_CONTENT_HASH_MIGRATION_KEY: STRATEGY_CONTENT_HASH_MIGRATION_HASH,
    }
    with runtime_engine.connect() as connection:
        observed_tables = {
            str(row[0])
            for row in connection.execute(
                text(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE()"
                )
            )
            if str(row[0]) in GOVERNANCE_TABLE_NAMES
        }
        if observed_tables != set(GOVERNANCE_TABLE_NAMES):
            raise RuntimeError("生产治理结构尚未由迁移账号完整准备")
        migration_rows = connection.execute(
            text(
                "SELECT migration_key, migration_hash "
                "FROM st_strategy_governance_schema_migration "
                "WHERE migration_key IN (:run_revision, :content_hash)"
            ),
            {
                "run_revision": RUN_REVISION_MIGRATION_KEY,
                "content_hash": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
            },
        ).mappings().all()
        observed_migrations = {
            str(row.get("migration_key") or ""): str(
                row.get("migration_hash") or ""
            )
            for row in migration_rows
        }
        if observed_migrations != expected_migrations:
            raise RuntimeError("生产治理结构迁移标记不完整或已漂移")
        schema_detail = validate_governance_table_schema(connection)
    seed_detail = validate_default_governance_seed_contract(runtime_engine)
    return {
        "table_count": int(schema_detail["table_count"]),
        "column_count": int(schema_detail["column_count"]),
        "index_count": int(schema_detail["index_count"]),
        "migration_count": len(observed_migrations),
        "seeded_strategy_count": int(seed_detail["seeded_strategy_count"]),
        "seeded_combination_count": int(
            seed_detail["seeded_combination_count"]
        ),
        "seed_contract_hash": str(seed_detail["seed_contract_hash"]),
        "trigger_count": 0,
        "database_triggers_required": False,
        "immutability_enforcement": (
            "application_state_machine_unique_identity_and_hash_replay"
        ),
    }


def ensure_and_seed_governance() -> None:
    global _SEED_READY
    if os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production":
        if _SEED_READY:
            return
        with _SEED_LOCK:
            if _SEED_READY:
                return
            validate_prepared_governance_runtime()
            _SEED_READY = True
        return
    development_engine = get_engine()

    ensure_strategy_governance_tables(
        engine=development_engine,
    )
    seed_governance_registry()


def load_registry() -> list[dict[str, Any]]:
    if not _table_exists("st_strategy_registry"):
        return []
    rows = _db_read(
        "SELECT r.strategy_key, r.strategy_name, r.category, r.family_key, "
        "r.description, "
        "r.owner_name, r.discovery_mode, r.enabled, r.current_version, "
        "r.current_status, r.status_reason, r.recovery_conditions_json, "
        "r.created_at, r.updated_at, v.created_at AS version_created_at, "
        "v.version_hash, v.content_hash, v.evaluator_type, "
        "v.evaluator_config_json, "
        "v.parameters_json, v.source_kind "
        "FROM st_strategy_registry r LEFT JOIN st_strategy_version v "
        "ON v.strategy_key=r.strategy_key AND v.version=r.current_version "
        "ORDER BY r.created_at, r.strategy_key"
    )
    for row in rows:
        status = str(row.get("current_status") or "SHADOW")
        row["status_label"] = LIFECYCLE_LABELS.get(status, "未知状态")
        row["enabled"] = bool(_int(row.get("enabled")))
        row["recovery_conditions"] = _json(row.pop("recovery_conditions_json", None), _recovery_conditions(status))
        row["evaluator_config"] = _json(
            row.pop("evaluator_config_json", None), {}
        )
        row["parameters"] = _json(row.pop("parameters_json", None), {})
        try:
            expected_version_hash = _strategy_version_digest(
                strategy_key=str(row.get("strategy_key") or ""),
                version=str(row.get("current_version") or ""),
                evaluator_type=str(row.get("evaluator_type") or ""),
                evaluator_config=row["evaluator_config"],
                parameters=row["parameters"],
                source_kind=str(row.get("source_kind") or ""),
            )
            expected_content_hash = _strategy_content_digest(
                strategy_key=str(row.get("strategy_key") or ""),
                evaluator_type=str(row.get("evaluator_type") or ""),
                evaluator_config=row["evaluator_config"],
                parameters=row["parameters"],
                source_kind=str(row.get("source_kind") or ""),
            )
            row["version_integrity_valid"] = (
                expected_version_hash == str(row.get("version_hash") or "")
                and expected_content_hash
                == str(row.get("content_hash") or "")
            )
        except (TypeError, ValueError):
            row["version_integrity_valid"] = False
        adapter_status = strategy_execution_adapter_status(row)
        row["execution_adapter"] = adapter_status
        row["execution_adapter_executable"] = (
            adapter_status.get("executable") is True
        )
        row["execution_adapter_reason"] = str(
            adapter_status.get("reason") or "执行适配器未部署/无效"
        )
        row["execution_binding_hash"] = str(
            adapter_status.get("execution_binding_hash") or ""
        )
        row["adapter_artifact_sha256"] = str(
            adapter_status.get("artifact_sha256") or ""
        )
        row["cost_model_hash"] = str(
            adapter_status.get("cost_model_hash") or ""
        )
        row["funding_pipeline_ready"] = (
            adapter_status.get("funding_pipeline_ready") is True
        )
        row["funding_pipeline_reason"] = (
            "动态策略意图、订单、成交和前向证据已完成精确绑定"
            if row["funding_pipeline_ready"]
            else "动态策略仅接通影子候选；意图、订单、成交、前向证据的精确绑定闭环尚未部署"
        )
    return rows


def register_strategy(payload: dict[str, Any], *, operator: str = "api") -> dict[str, Any]:
    ensure_and_seed_governance()
    key = validate_strategy_key(str(payload.get("strategy_key") or ""))
    name = str(payload.get("strategy_name") or "").strip()
    version = str(payload.get("version") or "").strip()
    if not name or len(name) > 120:
        raise ValueError("策略名称不能为空且不能超过120字")
    if not version or len(version) > 160:
        raise ValueError("策略版本不能为空且不能超过160字")
    evaluator_type = str(payload.get("evaluator_type") or "external_evidence")[:40]
    evaluator_config_raw = payload.get("evaluator_config") or {}
    parameters = payload.get("parameters") or {}
    if not isinstance(evaluator_config_raw, dict) or not isinstance(parameters, dict):
        raise ValueError("策略评估配置和参数必须是对象")
    parameters = dict(parameters)
    parameters["max_holding_days"] = _holding_horizon_from_parameters(parameters)
    parameters["label_horizon_days"] = _label_horizon_from_parameters(
        parameters
    )
    evaluator_config = dict(evaluator_config_raw)
    top_level_binding = payload.get("execution_binding")
    nested_binding = evaluator_config.get("execution_adapter")
    if (
        top_level_binding is not None
        and nested_binding is not None
        and top_level_binding != nested_binding
    ):
        raise ValueError("execution_binding与evaluator_config中的适配器绑定不一致")
    binding_raw = (
        top_level_binding
        if top_level_binding is not None else nested_binding
    )
    if binding_raw is not None:
        evaluator_config["execution_adapter"] = normalize_execution_binding(
            binding_raw,
            strategy_version=version,
        )
    evaluator_config["market_regime_multipliers"] = (
        _validated_market_regime_multipliers(
            evaluator_config.get("market_regime_multipliers")
        )
    )
    evaluator_config["market_router_policy_version"] = (
        MARKET_ROUTER_POLICY_VERSION
    )
    evaluator_config["market_state_config_version"] = (
        load_market_state_config()["config_version"]
    )
    evaluator_config["market_state_config_hash"] = market_state_config_hash()
    version_hash = _strategy_version_digest(
        strategy_key=key,
        version=version,
        evaluator_type=evaluator_type,
        evaluator_config=evaluator_config,
        parameters=parameters,
        source_kind="runtime_registry",
    )
    content_hash = _strategy_content_digest(
        strategy_key=key,
        evaluator_type=evaluator_type,
        evaluator_config=evaluator_config,
        parameters=parameters,
        source_kind="runtime_registry",
    )
    existing = _db_read(
        "SELECT * FROM st_strategy_registry WHERE strategy_key = :key",
        {"key": key},
    )
    before = existing[0] if existing else {}
    versions = _db_read(
        "SELECT version_hash, content_hash FROM st_strategy_version "
        "WHERE strategy_key=:key AND version=:version",
        {"key": key, "version": version},
    )
    if versions:
        if (
            str(versions[0].get("version_hash") or "") != version_hash
            or str(versions[0].get("content_hash") or "") != content_hash
        ):
            raise ValueError("同一策略版本不可覆盖；请使用新版本号")
        if not existing or str(existing[0].get("current_version") or "") != version:
            raise ValueError("历史版本不能重新设为当前版本；请注册新版本")
        return next(row for row in load_registry() if row["strategy_key"] == key)
    duplicate_config = _db_read(
        "SELECT version FROM st_strategy_version "
        "WHERE strategy_key=:key AND content_hash=:content_hash LIMIT 1",
        {"key": key, "content_hash": content_hash},
    )
    if duplicate_config:
        raise ValueError("新版本配置与已有版本完全相同，无需创建新版本")
    parent = str(existing[0].get("current_version") or "") if existing else ""
    params = {
        "key": key, "name": name,
        "category": str(payload.get("category") or "未分类")[:80],
        "family": str(payload.get("family_key") or key)[:80],
        "description": str(payload.get("description") or "")[:1000],
        "owner": str(operator or payload.get("owner_name") or "api")[:80],
        "version": version, "version_hash": version_hash,
        "content_hash": content_hash, "parent": parent,
        "evaluator_type": evaluator_type,
        "evaluator_config": _json_text(evaluator_config),
        "parameters": _json_text(parameters),
        "recovery": _json_text(_recovery_conditions()),
    }
    previous_status = str(before.get("current_status") or "SHADOW")
    reason = str(payload.get("reason") or "注册策略版本")[:500]
    event_payload = {
        "entity_type": "STRATEGY", "entity_key": key,
        "old_version": parent, "new_version": version,
        "previous_status": previous_status, "next_status": "SHADOW",
        "version_hash": version_hash,
    }
    after_audit = {
        "strategy_key": key, "strategy_name": name,
        "current_version": version, "current_status": "SHADOW",
        "status_reason": "新版本重新进入影子验证",
    }
    with get_engine().begin() as connection:
        connection.execute(text(
            """
            INSERT INTO st_strategy_version
            (strategy_key, version, version_hash, content_hash,
             parent_version, evaluator_type, evaluator_config_json,
             parameters_json, source_kind, created_by)
            VALUES (:key, :version, :version_hash, :content_hash, :parent,
                    :evaluator_type, :evaluator_config, :parameters,
                    'runtime_registry', :owner)
            """
        ), params)
        if existing:
            updated = connection.execute(text(
                """
                UPDATE st_strategy_registry
                SET strategy_name=:name, category=:category,
                    family_key=:family, description=:description,
                    owner_name=:owner, discovery_mode='dynamic', enabled=1,
                    current_version=:version, current_status='SHADOW',
                    status_reason='新版本重新进入影子验证',
                    recovery_conditions_json=:recovery
                WHERE strategy_key=:key AND current_version=:parent
                """
            ), params)
            if updated.rowcount != 1:
                raise RuntimeError("策略版本已被并发更新，请重新注册")
        else:
            connection.execute(text(
                """
                INSERT INTO st_strategy_registry
                (strategy_key, strategy_name, category, family_key,
                 description, owner_name, discovery_mode, enabled,
                 current_version, current_status, status_reason,
                 recovery_conditions_json)
                VALUES (:key, :name, :category, :family, :description,
                        :owner, 'dynamic', 1, :version, 'SHADOW',
                        '新策略或新版本必须先进行影子验证', :recovery)
                """
            ), params)
        connection.execute(text(
            """
            INSERT INTO st_strategy_lifecycle_event
            (event_id, entity_type, entity_key, entity_version,
             previous_status, next_status, reason, trigger_type,
             evidence_json, payload_json, event_hash, operator_name)
            VALUES (:event_id, 'STRATEGY', :entity_key, :entity_version,
                    :previous_status, 'SHADOW', :reason,
                    'VERSION_REGISTRATION', :evidence_json, :payload_json,
                    :event_hash,
                    :operator_name)
            """
        ), {
            "event_id": uuid.uuid4().hex, "entity_key": key,
            "entity_version": version, "previous_status": previous_status,
            "reason": reason, "evidence_json": _json_text(event_payload),
            "payload_json": _json_text(event_payload),
            "event_hash": _digest(event_payload), "operator_name": params["owner"],
        })
        _append_audit_connection(
            connection, entity_type="STRATEGY", entity_key=key,
            action="REGISTER_VERSION", reason=reason, operator=params["owner"],
            before=before, after=after_audit,
            evidence={"version_hash": version_hash},
        )
    return next(row for row in load_registry() if row["strategy_key"] == key)


def register_combination(payload: dict[str, Any], *, operator: str = "api") -> dict[str, Any]:
    ensure_and_seed_governance()
    key = validate_strategy_key(str(payload.get("combination_key") or ""))
    name = str(payload.get("combination_name") or "").strip()
    version = str(payload.get("version") or "").strip()
    members = payload.get("members") or []
    if (
        not name or not version or not isinstance(members, list)
        or not 2 <= len(members) <= 50
    ):
        raise ValueError("组合名称、版本不能为空，且必须包含2至50个成员")
    registry_versions = {
        row["strategy_key"]: str(row["current_version"])
        for row in load_registry()
    }
    normalized: list[dict[str, Any]] = []
    seen_members: set[str] = set()
    for member in members:
        member_key = str((member or {}).get("strategy_key") or "")
        weight = _num((member or {}).get("weight"), 0.0) or 0.0
        if member_key not in registry_versions:
            raise ValueError(f"组合成员未注册：{member_key}")
        declared_version = str(
            (member or {}).get("strategy_version") or ""
        ).strip()
        current_member_version = registry_versions[member_key]
        if declared_version and declared_version != current_member_version:
            raise ValueError(
                f"组合成员{member_key}声明版本与当前版本不一致"
            )
        if weight <= 0:
            raise ValueError("组合权重必须大于0")
        if member_key in seen_members:
            raise ValueError(f"组合成员不能重复：{member_key}")
        seen_members.add(member_key)
        normalized.append({
            "strategy_key": member_key,
            "strategy_version": current_member_version,
            "weight": weight,
        })
    total = sum(item["weight"] for item in normalized)
    if total <= 0:
        raise ValueError("组合总权重必须大于0")
    for item in normalized:
        item["weight"] = round(item["weight"] / total, 8)
    constraints = _validated_combination_constraints(
        payload.get("constraints")
    )
    if max(item["weight"] for item in normalized) > (
        constraints["maximum_member_weight"] + 0.00000001
    ):
        raise ValueError("组合成员权重超过不可变最大成员权重约束")
    config = {"members": normalized, "constraints": constraints}
    config_hash = _digest(config)
    existing = _db_read("SELECT * FROM st_strategy_combination WHERE combination_key=:key", {"key": key})
    before = existing[0] if existing else {}
    versions = _db_read(
        "SELECT config_hash FROM st_strategy_combination_version "
        "WHERE combination_key=:key AND version=:version",
        {"key": key, "version": version},
    )
    if versions:
        if str(versions[0].get("config_hash") or "") != config_hash:
            raise ValueError("同一组合版本不可覆盖；请使用新版本号")
        if not existing or str(existing[0].get("current_version") or "") != version:
            raise ValueError("历史组合版本不能重新设为当前版本；请注册新版本")
        return load_combinations(key)[0]
    duplicate_config = _db_read(
        "SELECT version FROM st_strategy_combination_version "
        "WHERE combination_key=:key AND config_hash=:config_hash LIMIT 1",
        {"key": key, "config_hash": config_hash},
    )
    if duplicate_config:
        raise ValueError("新组合版本与已有版本完全相同，无需创建")
    parent = str(before.get("current_version") or "")
    previous_status = str(before.get("current_status") or "SHADOW")
    reason = str(payload.get("reason") or "注册组合版本")[:500]
    params = {
        "key": key, "name": name[:120],
        "description": str(payload.get("description") or "")[:1000],
        "operator": str(operator)[:80], "version": version[:160],
        "parent": parent, "members": _json_text(normalized),
        "constraints": _json_text(constraints),
        "config_hash": config_hash,
    }
    event_payload = {
        "entity_type": "COMBINATION", "entity_key": key,
        "old_version": parent, "new_version": version,
        "previous_status": previous_status, "next_status": "SHADOW",
        "config_hash": config_hash,
    }
    after_audit = {
        "combination_key": key, "combination_name": name,
        "current_version": version, "current_status": "SHADOW",
        "members": normalized,
    }
    with get_engine().begin() as connection:
        connection.execute(text(
            """
            INSERT INTO st_strategy_combination_version
            (combination_key, version, members_json, constraints_json,
             config_hash, created_by)
            VALUES (:key, :version, :members, :constraints, :config_hash,
                    :operator)
            """
        ), params)
        if existing:
            updated = connection.execute(text(
                """
                UPDATE st_strategy_combination
                SET combination_name=:name, description=:description,
                    owner_name=:operator, enabled=1,
                    current_version=:version, current_status='SHADOW',
                    status_reason='新版本重新进入影子验证'
                WHERE combination_key=:key AND current_version=:parent
                """
            ), params)
            if updated.rowcount != 1:
                raise RuntimeError("组合版本已被并发更新，请重新注册")
        else:
            connection.execute(text(
                """
                INSERT INTO st_strategy_combination
                (combination_key, combination_name, description, owner_name,
                 enabled, current_version, current_status, status_reason)
                VALUES (:key, :name, :description, :operator, 1, :version,
                        'SHADOW', '新组合必须先进行独立影子验证')
                """
            ), params)
        connection.execute(text(
            """
            INSERT INTO st_strategy_lifecycle_event
            (event_id, entity_type, entity_key, entity_version,
             previous_status, next_status, reason, trigger_type,
             evidence_json, payload_json, event_hash, operator_name)
            VALUES (:event_id, 'COMBINATION', :entity_key, :entity_version,
                    :previous_status, 'SHADOW', :reason,
                    'VERSION_REGISTRATION', :evidence_json, :payload_json,
                    :event_hash,
                    :operator_name)
            """
        ), {
            "event_id": uuid.uuid4().hex, "entity_key": key,
            "entity_version": version, "previous_status": previous_status,
            "reason": reason, "evidence_json": _json_text(event_payload),
            "payload_json": _json_text(event_payload),
            "event_hash": _digest(event_payload), "operator_name": params["operator"],
        })
        _append_audit_connection(
            connection, entity_type="COMBINATION", entity_key=key,
            action="REGISTER_VERSION", reason=reason,
            operator=params["operator"], before=before, after=after_audit,
            evidence={"config_hash": config_hash},
        )
    return load_combinations(key)[0]


def load_combinations(only_key: str = "") -> list[dict[str, Any]]:
    if not _table_exists("st_strategy_combination"):
        return []
    where = "WHERE c.combination_key=:key" if only_key else ""
    rows = _db_read(
        "SELECT c.*, v.members_json, v.constraints_json, v.config_hash, "
        "v.created_at AS version_created_at "
        "FROM st_strategy_combination c LEFT JOIN st_strategy_combination_version v "
        "ON v.combination_key=c.combination_key AND v.version=c.current_version "
        f"{where} ORDER BY c.created_at, c.combination_key",
        {"key": only_key} if only_key else None,
    )
    for row in rows:
        status = str(row.get("current_status") or "SHADOW")
        row["status_label"] = LIFECYCLE_LABELS.get(status, "未知状态")
        row["enabled"] = bool(_int(row.get("enabled")))
        row["members"] = _json(row.pop("members_json", None), [])
        row["constraints"] = _json(row.pop("constraints_json", None), {})
        row["config_integrity_valid"] = (
            _digest({
                "members": row["members"],
                "constraints": row["constraints"],
            }) == str(row.get("config_hash") or "")
        )
    return rows


def _holding_horizon_from_parameters(parameters: Any) -> int:
    payload = parameters if isinstance(parameters, dict) else {}
    value = _int(
        payload.get("max_holding_days", payload.get("horizon_days")), 0
    )
    if not 1 <= value <= 250:
        raise ValueError("不可变策略版本必须声明1至250日最大持有期")
    return value


def _label_horizon_from_parameters(parameters: Any) -> int:
    payload = parameters if isinstance(parameters, dict) else {}
    value = _int(
        payload.get(
            "label_horizon_days",
            payload.get(
                "horizon_days",
                payload.get("max_holding_days"),
            ),
        ),
        0,
    )
    if not 1 <= value <= 250:
        raise ValueError("不可变策略版本必须声明1至250日标签期限")
    return value


def _version_max_holding_days(
    entity_type: str, entity_key: str, entity_version: str, *, connection=None,
) -> int:
    def read_one(sql: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if connection is not None:
            return connection.execute(text(sql), params).mappings().first()
        rows = _db_read(sql, params)
        return rows[0] if rows else None

    if entity_type == "STRATEGY":
        row = read_one(
            "SELECT parameters_json FROM st_strategy_version "
            "WHERE strategy_key=:entity_key AND version=:entity_version",
            {"entity_key": entity_key, "entity_version": entity_version},
        )
        if row is None:
            raise ValueError("证据绑定的不可变策略版本不存在")
        return _holding_horizon_from_parameters(
            _json(row.get("parameters_json"), {})
        )

    row = read_one(
        "SELECT members_json FROM st_strategy_combination_version "
        "WHERE combination_key=:entity_key AND version=:entity_version",
        {"entity_key": entity_key, "entity_version": entity_version},
    )
    if row is None:
        raise ValueError("证据绑定的不可变组合版本不存在")
    members = _json(row.get("members_json"), [])
    horizons = []
    for member in members:
        if not isinstance(member, dict):
            raise ValueError("组合成员版本配置无效")
        horizons.append(_version_max_holding_days(
            "STRATEGY",
            str(member.get("strategy_key") or ""),
            str(member.get("strategy_version") or ""),
            connection=connection,
        ))
    if not horizons:
        raise ValueError("组合版本没有可验证成员")
    return max(horizons)


def _version_label_horizon_days(
    entity_type: str, entity_key: str, entity_version: str, *, connection=None,
) -> int:
    def read_one(sql: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if connection is not None:
            return connection.execute(text(sql), params).mappings().first()
        rows = _db_read(sql, params)
        return rows[0] if rows else None

    if entity_type == "STRATEGY":
        row = read_one(
            "SELECT parameters_json FROM st_strategy_version "
            "WHERE strategy_key=:entity_key AND version=:entity_version",
            {"entity_key": entity_key, "entity_version": entity_version},
        )
        if row is None:
            raise ValueError("证据绑定的不可变策略版本不存在")
        return _label_horizon_from_parameters(
            _json(row.get("parameters_json"), {})
        )
    row = read_one(
        "SELECT members_json FROM st_strategy_combination_version "
        "WHERE combination_key=:entity_key AND version=:entity_version",
        {"entity_key": entity_key, "entity_version": entity_version},
    )
    if row is None:
        raise ValueError("证据绑定的不可变组合版本不存在")
    members = _json(row.get("members_json"), [])
    horizons = [
        _version_label_horizon_days(
            "STRATEGY",
            str(member.get("strategy_key") or ""),
            str(member.get("strategy_version") or ""),
            connection=connection,
        )
        for member in members if isinstance(member, dict)
    ]
    if not horizons:
        raise ValueError("组合版本没有可验证成员")
    return max(horizons)


def _trading_sessions_between(start_exclusive: str, end_exclusive: str) -> int:
    rows = _db_read(
        "SELECT COUNT(*) AS cnt FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date>:start_date "
        "AND trade_date<:end_date",
        {"start_date": start_exclusive, "end_date": end_exclusive},
    )
    if len(rows) != 1:
        raise ValueError("无法取得权威交易日历用于时序隔离校验")
    return _int(rows[0].get("cnt"), -1)


def _is_authoritative_trade_session(
    trade_day: str, *, cache: dict[str, bool] | None = None,
) -> bool:
    """Return whether one ISO date is exactly one authoritative session."""

    if cache is not None and trade_day in cache:
        return cache[trade_day]
    current = date.fromisoformat(trade_day)
    result = _trading_sessions_between(
        (current - timedelta(days=1)).isoformat(),
        (current + timedelta(days=1)).isoformat(),
    ) == 1
    if cache is not None:
        cache[trade_day] = result
    return result


def _authoritative_maturity_sessions(
    entry_day: str, label_available_day: str,
    *, session_cache: dict[str, bool] | None = None,
    distance_cache: dict[tuple[str, str], int] | None = None,
) -> int:
    """Count closed sessions after entry through actual label availability."""

    distance_key = (entry_day, label_available_day)
    if distance_cache is not None and distance_key in distance_cache:
        return distance_cache[distance_key]
    if (
        label_available_day < entry_day
        or not _is_authoritative_trade_session(
            entry_day, cache=session_cache,
        )
        or not _is_authoritative_trade_session(
            label_available_day, cache=session_cache,
        )
    ):
        raise ValueError("样本入场日或标签成熟日不是权威交易日")
    label_day = date.fromisoformat(label_available_day)
    result = _trading_sessions_between(
        entry_day,
        (label_day + timedelta(days=1)).isoformat(),
    )
    if distance_cache is not None:
        distance_cache[distance_key] = result
    return result


def _authoritative_session_windows(
    as_of_date: str,
) -> dict[int, dict[str, Any]]:
    """Resolve exact closed-session windows from the exchange calendar."""

    rows = _db_read(
        "SELECT trade_date FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date<=:as_of_date "
        "ORDER BY trade_date DESC LIMIT 120",
        {"as_of_date": as_of_date},
    )
    descending: list[str] = []
    seen: set[str] = set()
    for row in rows:
        day = _trade_date(row.get("trade_date"), default_today=False)
        if day in seen:
            raise ValueError("权威交易日历存在重复开市日")
        seen.add(day)
        descending.append(day)
    if not descending or descending[0] != as_of_date:
        raise ValueError("截止日不是权威交易日历中的已收盘交易日")
    if len(descending) < max(WINDOWS):
        raise ValueError("权威交易日历不足120个已收盘交易日")
    oldest_required = descending[max(WINDOWS) - 1]
    try:
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
            "AND k.trade_date BETWEEN :start_date AND :as_of_date "
            "UNION ALL "
            "SELECT a.trade_date, a.stock_code, 0 AS in_target, "
            "1 AS in_completed_attestation, 0 AS in_exact_attestation "
            "FROM qmt_kline_attestation_row a "
            "WHERE BINARY a.protocol_version=BINARY :protocol_version "
            "AND a.stock_code REGEXP '^(0|3|6)' "
            "AND a.trade_date BETWEEN :start_date AND :as_of_date "
            "UNION ALL "
            "SELECT k.trade_date, k.stock_code, 0 AS in_target, "
            "0 AS in_completed_attestation, 1 AS in_exact_attestation "
            "FROM sm_stock_kline k "
            "JOIN qmt_kline_attestation_row a ON a.target_id=k.id "
            "AND BINARY a.protocol_version=BINARY :protocol_version "
            "AND BINARY a.source_data_version=BINARY k.data_version "
            "AND a.qmt_id>0 AND a.trade_date=k.trade_date "
            "AND BINARY a.stock_code=BINARY k.stock_code "
            "AND BINARY a.attestation_id=BINARY SHA2(CONCAT_WS('|', "
            "a.protocol_version, a.target_id, a.qmt_id, "
            "a.source_data_version, a.source_pre_close, "
            "a.attested_open, a.attested_close, a.attested_high, "
            "a.attested_low, a.attested_volume, a.attested_amount), 256) "
            "AND BINARY a.source_pre_close_origin=BINARY 'NATIVE_QMT' "
            "AND a.source_pre_close=k.pre_close "
            "AND a.attested_open=k.`open` AND a.attested_close=k.`close` "
            "AND a.attested_high=k.`high` AND a.attested_low=k.`low` "
            "AND a.attested_volume=k.volume AND a.attested_amount=k.amount "
            "WHERE k.k_type=1 AND k.adjust_type=0 "
            "AND k.stock_code REGEXP '^(0|3|6)' "
            "AND k.data_source='gj_big_qmt_inner' "
            "AND k.quality_status='QMT_ATTESTED' "
            "AND k.permission_status='SUPPORTED' "
            "AND k.source_time IS NOT NULL AND k.received_at IS NOT NULL "
            "AND k.source_time>=TIMESTAMP(k.trade_date, '15:00:00') "
            "AND k.received_at>=k.source_time "
            "AND k.batch_id<>'' AND k.data_version<>'' "
            "AND k.trade_date BETWEEN :start_date AND :as_of_date"
            ") u GROUP BY u.trade_date, u.stock_code "
            "ORDER BY u.trade_date DESC, u.stock_code",
            {
                "start_date": oldest_required,
                "as_of_date": as_of_date,
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            },
        )
        completed_run_rows = _db_read(
            "SELECT run_id, start_date, end_date, tolerance_json "
            "FROM qmt_kline_attestation_run "
            "WHERE BINARY status=BINARY 'COMPLETED' "
            "AND BINARY provider=BINARY 'gj_big_qmt_inner' "
            "AND start_date<=:as_of_date AND end_date>=:start_date "
            "ORDER BY finished_at DESC, run_id",
            {
                "start_date": oldest_required,
                "as_of_date": as_of_date,
            },
        )
        attestation_rows = _db_read(
            "SELECT k.trade_date, COUNT(DISTINCT k.id) AS attested_bar_count, "
            "COUNT(DISTINCT k.batch_id) AS batch_count, "
            "MIN(k.data_version) AS min_data_version, "
            "MAX(k.data_version) AS max_data_version, "
            "MAX(k.received_at) AS latest_received_at "
            "FROM sm_stock_kline k "
            "JOIN qmt_kline_attestation_row a ON a.target_id=k.id "
            "AND BINARY a.protocol_version=BINARY :protocol_version "
            "AND BINARY a.source_data_version=BINARY k.data_version "
            "AND a.qmt_id>0 AND a.trade_date=k.trade_date "
            "AND BINARY a.stock_code=BINARY k.stock_code "
            "AND BINARY a.attestation_id=BINARY SHA2(CONCAT_WS('|', "
            "a.protocol_version, a.target_id, a.qmt_id, "
            "a.source_data_version, a.source_pre_close, "
            "a.attested_open, a.attested_close, a.attested_high, "
            "a.attested_low, a.attested_volume, a.attested_amount), 256) "
            "AND BINARY a.source_pre_close_origin=BINARY 'NATIVE_QMT' "
            "AND a.source_pre_close=k.pre_close "
            "AND a.attested_open=k.`open` AND a.attested_close=k.`close` "
            "AND a.attested_high=k.`high` AND a.attested_low=k.`low` "
            "AND a.attested_volume=k.volume AND a.attested_amount=k.amount "
            "WHERE k.k_type=1 AND k.adjust_type=0 "
            "AND k.data_source='gj_big_qmt_inner' "
            "AND k.quality_status='QMT_ATTESTED' "
            "AND k.permission_status='SUPPORTED' "
            "AND k.source_time IS NOT NULL AND k.received_at IS NOT NULL "
            "AND k.source_time>=TIMESTAMP(k.trade_date, '15:00:00') "
            "AND k.received_at>=k.source_time "
            "AND k.batch_id<>'' AND k.data_version<>'' "
            "AND k.trade_date BETWEEN :start_date AND :as_of_date "
            "GROUP BY k.trade_date ORDER BY k.trade_date DESC",
            {
                "start_date": oldest_required,
                "as_of_date": as_of_date,
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            },
        )
    except Exception as exc:
        raise ValueError(
            "QMT原始前收盘价逐行认证尚未就绪"
        ) from exc
    expected_days = set(descending[:max(WINDOWS)])
    universe_sets: dict[str, dict[str, set[str]]] = {
        day: {
            "target": set(),
            "completed_attestation": set(),
            "exact_attestation": set(),
        }
        for day in expected_days
    }
    for row in universe_rows:
        day = _trade_date(row.get("trade_date"), default_today=False)
        if day not in universe_sets:
            raise ValueError("交易日历与QMT逐行认证日期集合不一致")
        stock_code = str(row.get("stock_code") or "").strip()
        flags = {
            "target": _int(row.get("in_target"), -1),
            "completed_attestation": _int(
                row.get("in_completed_attestation"), -1
            ),
            "exact_attestation": _int(
                row.get("in_exact_attestation"), -1
            ),
        }
        if (
            not stock_code
            or any(flag not in {0, 1} for flag in flags.values())
            or not any(flags.values())
        ):
            raise ValueError("QMT逐日股票集合绑定行无效")
        for set_name, present in flags.items():
            if present:
                universe_sets[day][set_name].add(stock_code)

    universe_failures: list[str] = []
    for day in descending[:max(WINDOWS)]:
        day_sets = universe_sets[day]
        target_set = day_sets["target"]
        completed_set = day_sets["completed_attestation"]
        exact_set = day_sets["exact_attestation"]
        if (
            not target_set
            or target_set != completed_set
            or target_set != exact_set
        ):
            universe_failures.append(
                f"{day}(目标{len(target_set)}/"
                f"历史认证{len(completed_set)}/"
                f"当前绑定{len(exact_set)})"
            )
    if universe_failures:
        raise ValueError(
            "QMT生产目标、已完成历史认证与当前精确绑定股票集合"
            "必须逐日非空且完全一致；"
            + "、".join(universe_failures[:5])
        )
    expected_universes = {
        day: expected_stock_set_contract(
            day, universe_sets[day]["target"]
        )
        for day in expected_days
    }
    matching_manifest_runs: dict[str, set[str]] = {
        day: set() for day in expected_days
    }
    for row in completed_run_rows:
        run_id = str(row.get("run_id") or "").strip()
        try:
            run_start = _trade_date(
                row.get("start_date"), default_today=False
            )
            run_end = _trade_date(row.get("end_date"), default_today=False)
        except ValueError:
            continue
        if not run_id or run_start > run_end:
            continue
        try:
            daily_universe = validated_universe_manifest(
                row.get("tolerance_json"),
                start_date=run_start,
                end_date=run_end,
            )
        except (TypeError, ValueError):
            continue
        for day in expected_days:
            if not (run_start <= day <= run_end):
                continue
            manifest = daily_universe.get(day)
            expected = expected_universes[day]
            if (
                isinstance(manifest, dict)
                and type(manifest.get("stock_count")) is int
                and manifest.get("stock_count") == expected["stock_count"]
                and manifest.get("stock_set_hash")
                == expected["stock_set_hash"]
            ):
                matching_manifest_runs[day].add(run_id)
    missing_manifest_days = sorted(
        day for day, run_ids in matching_manifest_runs.items()
        if not run_ids
    )
    if missing_manifest_days:
        raise ValueError(
            "QMT V2已完成认证运行缺少与生产目标精确匹配的"
            "逐日股票全集清单；"
            + "、".join(missing_manifest_days[:5])
        )
    attestation_days = [
        _trade_date(row.get("trade_date"), default_today=False)
        for row in attestation_rows
    ]
    if len(attestation_days) != len(set(attestation_days)):
        raise ValueError("QMT收盘日线认证摘要存在重复交易日")
    missing_attestations = sorted(expected_days - set(attestation_days))
    extra_attested_sessions = sorted(set(attestation_days) - expected_days)
    if missing_attestations or extra_attested_sessions:
        raise ValueError(
            "交易日历与QMT已收盘日线日期集合不一致；缺少："
            + ("、".join(missing_attestations[:5]) or "无")
            + "；额外："
            + ("、".join(extra_attested_sessions[:5]) or "无")
        )
    attestations: dict[str, dict[str, Any]] = {}
    for row in attestation_rows:
        day = _trade_date(row.get("trade_date"), default_today=False)
        attested_count = _int(row.get("attested_bar_count"), 0)
        batch_count = _int(row.get("batch_count"), 0)
        if (
            day in attestations or attested_count <= 0 or batch_count <= 0
            or not str(row.get("min_data_version") or "")
            or not str(row.get("max_data_version") or "")
            or not _normalize_evidence_revision(row.get("latest_received_at"))
        ):
            raise ValueError("QMT收盘日线认证摘要无效")
        expected_universe = expected_universes[day]
        if attested_count != expected_universe["stock_count"]:
            raise ValueError("QMT收盘日线认证行数与精确股票集合不一致")
        attestations[day] = {
            "trade_date": day,
            "attested_bar_count": attested_count,
            "expected_stock_count": expected_universe["stock_count"],
            "expected_stock_set_hash": expected_universe["stock_set_hash"],
            "batch_count": batch_count,
            "min_data_version": str(row.get("min_data_version") or ""),
            "max_data_version": str(row.get("max_data_version") or ""),
            "latest_received_at": _normalize_evidence_revision(
                row.get("latest_received_at")
            ),
            "pre_close_attestation_protocol": (
                QMT_PRECLOSE_ATTESTATION_PROTOCOL
            ),
        }
    result: dict[int, dict[str, Any]] = {}
    for window in WINDOWS:
        if len(descending) < window:
            continue
        sessions = list(reversed(descending[:window]))
        payload = {
            "schema": "probiga.authoritative-session-window.v1",
            "window_days": window,
            "start_date": sessions[0],
            "end_date": sessions[-1],
            "session_count": len(sessions),
            "sessions": sessions,
            "session_attestations": [
                attestations[day] for day in sessions
            ],
        }
        result[window] = {
            **payload,
            "session_hash": _digest(payload),
        }
    return result


def _validated_metric_evidence(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("指标证据必须是对象")
    source_metrics = dict(raw)
    metrics: dict[str, Any] = {}
    numeric_bounds: dict[str, tuple[float, float]] = {
        "completed_trades": (0.0, 10_000_000.0),
        "coverage_days": (0.0, 100_000.0),
        "win_rate_pct": (0.0, 100.0),
        "average_win_pct": (0.0, 10_000.0),
        "average_loss_pct": (0.0, 100.0),
        "payoff_ratio": (0.0, 10_000.0),
        "gross_expectancy_pct": (-100.0, 10_000.0),
        "estimated_cost_pct": (0.0, 10.0),
        "net_expectancy_pct": (-100.0, 10_000.0),
        "profit_factor": (0.0, 10_000.0),
        "max_drawdown_pct": (0.0, 100.0),
        "walk_forward_segments": (0.0, 1_000.0),
        "positive_segments": (0.0, 1_000.0),
        "cost_stress_expectancy_pct": (-100.0, 10_000.0),
        "top5_profit_contribution_pct": (0.0, 100_000.0),
        "market_match_score": (0.0, 100.0),
    }
    required = set(numeric_bounds) | {
        "walk_forward_verified", "independent_oos",
        "drawdown_basis", "cost_basis",
    }
    missing = sorted(field for field in required if field not in source_metrics)
    if missing:
        raise ValueError("指标证据字段不完整：" + "、".join(missing))
    for field, (minimum, maximum) in numeric_bounds.items():
        value = _num(source_metrics.get(field), None)
        if value is None or not minimum <= value <= maximum:
            raise ValueError(f"指标字段{field}超出有效范围")
        metrics[field] = int(value) if field in {
            "completed_trades", "coverage_days", "walk_forward_segments",
            "positive_segments",
        } else value
    for field in ("walk_forward_verified", "independent_oos"):
        if type(source_metrics.get(field)) is not bool:
            raise ValueError(f"指标字段{field}必须是布尔值")
        metrics[field] = source_metrics[field]
    drawdown_basis = str(source_metrics.get("drawdown_basis") or "").strip()
    cost_basis = str(source_metrics.get("cost_basis") or "").strip()
    if drawdown_basis != "sequential_trade_compounded_equity":
        raise ValueError(
            "外部v2产物只能声明逐笔顺序复利诊断；真实组合权益必须由内部组合账本生成"
        )
    if cost_basis not in {"actual_ledger_fees", "validated_fee_model_v1"}:
        raise ValueError("资金门槛的成本必须来自实际费用或已验证费率模型")
    metrics["drawdown_basis"] = drawdown_basis
    metrics["cost_basis"] = cost_basis
    if metrics["positive_segments"] > metrics["walk_forward_segments"]:
        raise ValueError("正收益分段数不能超过Walk-Forward分段数")
    expected_net = metrics["gross_expectancy_pct"] - metrics["estimated_cost_pct"]
    if abs(metrics["net_expectancy_pct"] - expected_net) > 0.02:
        raise ValueError("扣费后净期望与毛期望、成本不一致")
    expected_stress = (
        metrics["gross_expectancy_pct"]
        - metrics["estimated_cost_pct"] * PROFIT_GATE_POLICY["cost_stress_multiple"]
    )
    if abs(metrics["cost_stress_expectancy_pct"] - expected_stress) > 0.02:
        raise ValueError("成本压力期望与毛期望、成本不一致")
    if metrics["average_loss_pct"] > 0:
        expected_payoff = metrics["average_win_pct"] / metrics["average_loss_pct"]
        if abs(metrics["payoff_ratio"] - expected_payoff) > 0.02:
            raise ValueError("盈亏比与平均盈利、平均亏损不一致")
    return metrics


def _validate_metric_artifact(
    raw: Any, *, entity_type: str, entity_key: str,
    entity_version: str, as_of_date: str, window_days: int,
    evidence_protocol: str, evidence_revision_at: str,
    metrics: dict[str, Any], artifact_hash: str,
    version_created_at: str, expected_max_holding_days: int,
    expected_label_horizon_days: int,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("验证产物清单必须是对象")
    artifact = dict(raw)
    session_window = _authoritative_session_windows(as_of_date).get(
        window_days
    )
    if session_window is None:
        raise ValueError("验证产物声明窗口缺少足够的权威已收盘交易日")
    expected_bindings = {
        "schema_version": "probiga.strategy-validation-artifact.v3",
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_version": entity_version,
        "as_of_date": as_of_date,
        "window_days": window_days,
        "evidence_protocol": evidence_protocol,
        "evidence_revision_at": evidence_revision_at,
        "metrics_hash": _digest(metrics),
        "window_session_start": session_window["start_date"],
        "window_session_end": session_window["end_date"],
        "window_session_count": session_window["session_count"],
        "window_session_hash": session_window["session_hash"],
    }
    allowed_artifact_fields = set(expected_bindings) | {
        "trades", "equity_curve", "source_dataset_hash", "segments",
        "validation_protocol",
    }
    if set(artifact) != allowed_artifact_fields:
        raise ValueError("验证产物字段集合不符合固定协议")
    for field, expected in expected_bindings.items():
        actual = artifact.get(field)
        if field in {"window_days", "window_session_count"}:
            actual = _int(actual, -1)
        else:
            actual = str(actual or "")
        if actual != expected:
            raise ValueError(f"验证产物绑定字段不一致：{field}")
    protocol_config = artifact.get("validation_protocol")
    if not isinstance(protocol_config, dict) or set(protocol_config) != {
        "label_horizon_days", "max_holding_days", "purge_days",
        "embargo_days",
    }:
        raise ValueError("验证产物缺少固定的标签、持有期、purge和embargo协议")
    label_horizon_days = _int(protocol_config.get("label_horizon_days"), 0)
    max_holding_days = _int(protocol_config.get("max_holding_days"), 0)
    purge_days = _int(protocol_config.get("purge_days"), 0)
    embargo_days = _int(protocol_config.get("embargo_days"), 0)
    if (
        label_horizon_days != expected_label_horizon_days
        or max_holding_days != expected_max_holding_days
        or purge_days < max(label_horizon_days, max_holding_days)
        or embargo_days < label_horizon_days
        or purge_days > 500 or embargo_days > 500
    ):
        raise ValueError("Walk-Forward的标签期限、最大持有期、purge或embargo不满足不可变版本协议")
    version_day = _trade_date(
        str(version_created_at or "")[:10], default_today=False
    )
    window_start = str(session_window["start_date"])
    trades = artifact.get("trades")
    equity_curve = artifact.get("equity_curve")
    if not isinstance(trades, list) or not trades:
        raise ValueError("验证产物必须包含逐笔样本外净收益")
    if len(trades) > 100_000:
        raise ValueError("单份验证产物逐笔样本不得超过100000笔")
    if not isinstance(equity_curve, list) or len(equity_curve) < 2:
        raise ValueError("验证产物必须包含组合权益曲线")
    if len(equity_curve) > 200_000:
        raise ValueError("单份验证产物权益曲线不得超过200000点")
    normalized_trades: list[dict[str, Any]] = []
    seen_trade_ids: set[str] = set()
    latest_observed_at = ""
    session_positions = {
        session_day: index
        for index, session_day in enumerate(session_window["sessions"])
    }
    for index, raw_trade in enumerate(trades, 1):
        if not isinstance(raw_trade, dict):
            raise ValueError(f"第{index}笔样本外交易格式无效")
        if set(raw_trade) != {
            "evidence_id", "trade_date", "label_available_at", "observed_at",
            "net_return_pct", "cost_pct",
        }:
            raise ValueError(f"第{index}笔样本外交易字段不符合固定协议")
        trade_id = str(raw_trade.get("evidence_id") or "").strip().lower()
        if (
            not _HASH_PATTERN.fullmatch(trade_id)
            or trade_id in seen_trade_ids
        ):
            raise ValueError("样本外交易缺少全局规范样本编号或编号重复")
        seen_trade_ids.add(trade_id)
        trade_day = _trade_date(
            raw_trade.get("trade_date"), default_today=False
        )
        observed_at = _normalize_evidence_revision(
            raw_trade.get("observed_at")
        )
        label_available_at = _normalize_evidence_revision(
            raw_trade.get("label_available_at")
        )
        if not observed_at or not label_available_at:
            raise ValueError("样本外交易缺少标签成熟时间或观测高水位")
        if not (
            version_day < trade_day <= as_of_date
            and window_start <= trade_day
        ):
            raise ValueError("样本外交易必须晚于版本冻结日、位于声明窗口且不晚于截止日")
        label_day = label_available_at[:10]
        entry_position = session_positions.get(trade_day)
        label_position = session_positions.get(label_day)
        if entry_position is None or label_position is None:
            raise ValueError("样本外交易入场日和标签成熟日必须属于绑定的权威交易日窗口")
        maturity_sessions = label_position - entry_position
        if not (
            expected_label_horizon_days <= maturity_sessions
            <= expected_max_holding_days
        ):
            raise ValueError("样本外交易标签成熟期与不可变版本期限不一致")
        if (
            label_available_at > observed_at
            or observed_at[:10] > as_of_date
        ):
            raise ValueError("样本外交易标签尚未成熟或观测高水位早于标签")
        net_return = _num(raw_trade.get("net_return_pct"), None)
        cost_pct = _num(raw_trade.get("cost_pct"), None)
        if (
            net_return is None or not -100.0 < net_return <= 10_000.0
            or cost_pct is None or not 0.0 <= cost_pct <= 10.0
        ):
            raise ValueError("样本外交易收益或成本超出有效范围")
        normalized_trades.append({
            "evidence_id": trade_id,
            "trade_date": trade_day,
            "label_available_at": label_available_at,
            "observed_at": observed_at,
            "net_return_pct": net_return,
            "cost_pct": cost_pct,
        })
        latest_observed_at = max(latest_observed_at, observed_at)
    normalized_trades.sort(key=lambda item: (
        item["trade_date"], item["label_available_at"],
        item["observed_at"], item["evidence_id"],
    ))
    if latest_observed_at != evidence_revision_at:
        raise ValueError("证据高水位必须等于最新底层观测时间")

    normalized_equity: list[dict[str, Any]] = []
    seen_equity_days: set[str] = set()
    for index, raw_point in enumerate(equity_curve, 1):
        if not isinstance(raw_point, dict):
            raise ValueError(f"组合权益曲线第{index}点格式无效")
        if set(raw_point) != {"trade_date", "equity"}:
            raise ValueError(f"组合权益曲线第{index}点字段不符合固定协议")
        equity_day = _trade_date(
            raw_point.get("trade_date"), default_today=False
        )
        equity = _num(raw_point.get("equity"), None)
        if (
            equity_day in seen_equity_days
            or not (
                version_day < equity_day <= as_of_date
                and window_start <= equity_day
            )
            or equity is None or equity <= 0
        ):
            raise ValueError("组合权益曲线日期或净值无效")
        seen_equity_days.add(equity_day)
        normalized_equity.append({
            "trade_date": equity_day, "equity": equity,
        })
    normalized_equity.sort(key=lambda item: item["trade_date"])
    rebuilt_equity = _rebuild_equity_curve(normalized_trades)
    if len(normalized_equity) != len(rebuilt_equity):
        raise ValueError("组合权益曲线必须逐点对应逐笔净收益的日终重建结果")
    for actual, expected in zip(normalized_equity, rebuilt_equity):
        try:
            actual_equity = Decimal(str(actual["equity"]))
            expected_equity = Decimal(str(expected["equity"]))
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            raise ValueError("组合权益曲线无法逐点精确校验") from exc
        if (
            actual["trade_date"] != expected["trade_date"]
            or actual_equity != expected_equity
        ):
            raise ValueError("组合权益曲线无法由逐笔净收益序列逐点精确重建")
    normalized_equity = rebuilt_equity
    dataset_payload = {
        "trades": normalized_trades,
        "equity_curve": normalized_equity,
    }
    source_dataset_hash = str(
        artifact.get("source_dataset_hash") or ""
    ).lower()
    if (
        not _HASH_PATTERN.fullmatch(source_dataset_hash)
        or source_dataset_hash != _digest(dataset_payload)
    ):
        raise ValueError("底层样本集SHA-256无效")

    test_samples_by_id = {
        item["evidence_id"]: item for item in normalized_trades
    }
    global_train_contracts: dict[str, dict[str, Any]] = {}
    global_train_content_owners: dict[str, str] = {}
    training_session_cache: dict[str, bool] = {}
    training_maturity_cache: dict[tuple[str, str], int] = {}

    segments = artifact.get("segments")
    if not isinstance(segments, list) or len(segments) != _int(
        metrics.get("walk_forward_segments")
    ):
        raise ValueError("验证产物分段数与Walk-Forward指标不一致")
    if len(segments) > 100:
        raise ValueError("Walk-Forward分段不得超过100段")
    if len(segments) < PROFIT_GATE_POLICY["walk_forward_segments"]:
        raise ValueError("Walk-Forward验证产物分段不足")
    previous_test_end = ""
    prior_test_segments: list[dict[str, Any]] = []
    test_assignment_counts: dict[str, int] = defaultdict(int)
    positive_segments = 0
    completed_trades = 0
    for index, raw_segment in enumerate(segments, 1):
        if not isinstance(raw_segment, dict):
            raise ValueError(f"Walk-Forward第{index}段格式无效")
        if set(raw_segment) != {
            "train_start", "train_end", "test_start", "test_end",
            "completed_trades", "net_expectancy_pct",
            "train_dataset", "train_dataset_hash", "test_dataset_hash",
        }:
            raise ValueError(f"Walk-Forward第{index}段字段不符合固定协议")
        train_start = _trade_date(
            raw_segment.get("train_start"), default_today=False
        )
        train_end = _trade_date(
            raw_segment.get("train_end"), default_today=False
        )
        test_start = _trade_date(
            raw_segment.get("test_start"), default_today=False
        )
        test_end = _trade_date(
            raw_segment.get("test_end"), default_today=False
        )
        if not (train_start <= train_end < test_start <= test_end):
            raise ValueError(f"Walk-Forward第{index}段存在时序穿越")
        if previous_test_end and test_start <= previous_test_end:
            raise ValueError("Walk-Forward测试段必须严格按时间排序且不得重叠")
        if _trading_sessions_between(train_end, test_start) < purge_days:
            raise ValueError(f"Walk-Forward第{index}段未执行足够purge隔离")
        if (
            test_start <= version_day
            or test_start < window_start
            or test_end > as_of_date
        ):
            raise ValueError("Walk-Forward样本外区间必须位于版本冻结后的声明窗口内")
        train_dataset = raw_segment.get("train_dataset")
        if not isinstance(train_dataset, list) or not train_dataset:
            raise ValueError(f"Walk-Forward第{index}段缺少可重算训练集")
        if len(train_dataset) > 100_000:
            raise ValueError("Walk-Forward单段训练集不得超过100000条")
        normalized_train: list[dict[str, Any]] = []
        seen_train_ids: set[str] = set()
        for raw_train in train_dataset:
            if not isinstance(raw_train, dict) or set(raw_train) != {
                "observation_id", "observed_at", "label_available_at",
                "feature_snapshot_hash", "label_snapshot_hash",
            }:
                raise ValueError(f"Walk-Forward第{index}段训练样本字段无效")
            observation_id = str(
                raw_train.get("observation_id") or ""
            ).strip().lower()
            observed_at = _normalize_evidence_revision(
                raw_train.get("observed_at")
            )
            label_available_at = _normalize_evidence_revision(
                raw_train.get("label_available_at")
            )
            feature_hash = str(
                raw_train.get("feature_snapshot_hash") or ""
            ).lower()
            label_hash = str(
                raw_train.get("label_snapshot_hash") or ""
            ).lower()
            if (
                not _HASH_PATTERN.fullmatch(observation_id)
                or observation_id in seen_train_ids
                or not observed_at
                or not label_available_at
                or not train_start <= observed_at[:10] <= train_end
                or observed_at > label_available_at
                or label_available_at > f"{train_end}T23:59:59"
                or not _HASH_PATTERN.fullmatch(feature_hash)
                or not _HASH_PATTERN.fullmatch(label_hash)
            ):
                raise ValueError(
                    f"Walk-Forward第{index}段训练样本不可重算、标签未成熟或超过训练高水位"
                )
            maturity_sessions = _authoritative_maturity_sessions(
                observed_at[:10], label_available_at[:10],
                session_cache=training_session_cache,
                distance_cache=training_maturity_cache,
            )
            if not (
                label_horizon_days <= maturity_sessions
                <= max_holding_days
            ):
                raise ValueError(
                    f"Walk-Forward第{index}段训练标签成熟期与不可变版本期限不一致"
                )
            seen_train_ids.add(observation_id)
            normalized_item = {
                "observation_id": observation_id,
                "observed_at": observed_at,
                "label_available_at": label_available_at,
                "feature_snapshot_hash": feature_hash,
                "label_snapshot_hash": label_hash,
            }
            prior_contract = global_train_contracts.get(observation_id)
            if prior_contract is not None and prior_contract != normalized_item:
                raise ValueError("全局规范样本编号被绑定到不同训练事实")
            content_fingerprint = _digest({
                key: value for key, value in normalized_item.items()
                if key != "observation_id"
            })
            content_owner = global_train_content_owners.get(
                content_fingerprint
            )
            if content_owner is not None and content_owner != observation_id:
                raise ValueError("同一训练样本事实使用了不同规范编号别名")
            global_train_contracts.setdefault(
                observation_id, normalized_item,
            )
            global_train_content_owners.setdefault(
                content_fingerprint, observation_id,
            )
            test_sample = test_samples_by_id.get(observation_id)
            if test_sample is not None and (
                observed_at[:10] != test_sample["trade_date"]
                or label_available_at != test_sample["label_available_at"]
                or test_sample["trade_date"] >= test_start
            ):
                raise ValueError("全局规范样本编号无法证明来自已完成的既往测试样本")
            normalized_train.append(normalized_item)
        normalized_train.sort(key=lambda item: (
            item["observed_at"], item["label_available_at"],
            item["observation_id"],
        ))
        # OOS folds may be contiguous.  Embargo constrains what a later
        # training fold may consume.  Its clock begins only after every label
        # in the prior test fold has actually matured, never at test_end.
        for prior in prior_test_segments:
            consumes_prior_period = any(
                (
                    prior["test_start"]
                    <= item["observed_at"][:10]
                    <= prior["test_end"]
                )
                or item["observation_id"] in prior["sample_ids"]
                for item in normalized_train
            )
            matured_sessions = _trading_sessions_between(
                prior["label_maturity_boundary"][:10],
                (date.fromisoformat(train_end) + timedelta(days=1)).isoformat(),
            )
            if consumes_prior_period and matured_sessions < embargo_days:
                raise ValueError(
                    "Walk-Forward训练集在既往测试标签全部成熟后仍未满足embargo隔离"
                )
        segment_net = _num(raw_segment.get("net_expectancy_pct"), None)
        segment_trades = _int(raw_segment.get("completed_trades"), -1)
        if segment_net is None or segment_trades <= 0:
            raise ValueError(f"Walk-Forward第{index}段结果不完整")
        segment_rows = [
            item for item in normalized_trades
            if test_start <= item["trade_date"] <= test_end
        ]
        for item in segment_rows:
            test_assignment_counts[item["evidence_id"]] += 1
        recalculated_segment_net = (
            sum(item["net_return_pct"] for item in segment_rows)
            / len(segment_rows)
            if segment_rows else None
        )
        if (
            segment_trades != len(segment_rows)
            or recalculated_segment_net is None
            or abs(segment_net - recalculated_segment_net) > 0.02
        ):
            raise ValueError(f"Walk-Forward第{index}段结果无法从底层样本重算")
        train_dataset_hash = str(
            raw_segment.get("train_dataset_hash") or ""
        ).lower()
        test_dataset_hash = str(
            raw_segment.get("test_dataset_hash") or ""
        ).lower()
        expected_test_hash = _digest({
            "segment_index": index,
            "test_start": test_start,
            "test_end": test_end,
            "trades": segment_rows,
        })
        expected_train_hash = _digest({
            "segment_index": index,
            "train_start": train_start,
            "train_end": train_end,
            "observations": normalized_train,
        })
        if (
            train_dataset_hash != expected_train_hash
            or test_dataset_hash != expected_test_hash
            or train_dataset_hash == test_dataset_hash
        ):
            raise ValueError(f"Walk-Forward第{index}段训练集或测试集哈希无效")
        prior_test_segments.append({
            "test_start": test_start,
            "test_end": test_end,
            "label_maturity_boundary": max(
                item["label_available_at"] for item in segment_rows
            ),
            "sample_ids": frozenset(
                item["evidence_id"] for item in segment_rows
            ),
        })
        previous_test_end = test_end
        positive_segments += 1 if recalculated_segment_net > 0 else 0
        completed_trades += len(segment_rows)
    if set(test_assignment_counts) != seen_trade_ids or any(
        count != 1 for count in test_assignment_counts.values()
    ):
        raise ValueError("Walk-Forward每笔样本外交易必须且只能归属一个测试段")
    if positive_segments != _int(metrics.get("positive_segments")):
        raise ValueError("Walk-Forward正收益分段数与指标不一致")
    if completed_trades != _int(metrics.get("completed_trades")):
        raise ValueError("Walk-Forward分段样本数与指标不一致")
    if previous_test_end > evidence_revision_at[:10]:
        raise ValueError("证据高水位早于最后样本外分段")
    net_values = [item["net_return_pct"] for item in normalized_trades]
    cost_values = [item["cost_pct"] for item in normalized_trades]
    gross_values = [
        item["net_return_pct"] + item["cost_pct"]
        for item in normalized_trades
    ]
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    average_win = sum(wins) / len(wins) if wins else 0.0
    average_loss = abs(sum(losses) / len(losses)) if losses else 0.0
    net_profit = sum(net_values)
    peak = float(_EQUITY_BASE)
    max_drawdown = 0.0
    for point in normalized_equity:
        peak = max(peak, point["equity"])
        max_drawdown = max(
            max_drawdown,
            (peak - point["equity"]) / peak * 100.0,
        )
    recalculated = {
        "completed_trades": len(net_values),
        "coverage_days": len({item["trade_date"] for item in normalized_trades}),
        "win_rate_pct": len(wins) / len(net_values) * 100.0,
        "average_win_pct": average_win,
        "average_loss_pct": average_loss,
        "payoff_ratio": (
            average_win / average_loss if average_loss > 0 else 999.0
        ),
        "gross_expectancy_pct": sum(gross_values) / len(gross_values),
        "estimated_cost_pct": sum(cost_values) / len(cost_values),
        "net_expectancy_pct": sum(net_values) / len(net_values),
        "profit_factor": (
            sum(wins) / abs(sum(losses)) if losses else 999.0
        ),
        "max_drawdown_pct": max_drawdown,
        "walk_forward_segments": len(segments),
        "positive_segments": positive_segments,
        "cost_stress_expectancy_pct": (
            sum(gross_values) / len(gross_values)
            - sum(cost_values) / len(cost_values)
            * PROFIT_GATE_POLICY["cost_stress_multiple"]
        ),
        "top5_profit_contribution_pct": (
            sum(sorted(wins, reverse=True)[:5]) / net_profit * 100.0
            if net_profit > 0 else 100_000.0
        ),
    }
    for field, expected in recalculated.items():
        actual = _num(metrics.get(field), None)
        tolerance = 0.0 if field in {
            "completed_trades", "coverage_days", "walk_forward_segments",
            "positive_segments",
        } else 0.02
        if actual is None or abs(actual - expected) > tolerance:
            raise ValueError(f"指标{field}无法从验证产物重算")
    if _digest(artifact) != artifact_hash:
        raise ValueError("验证产物内容与SHA-256不一致")
    return artifact


def _assert_global_metric_evidence_unclaimed(
    connection,
    evidence_payload: dict[str, Any],
) -> None:
    """Give every artifact and underlying dataset one immutable owner row."""

    for _index_name, hash_column, evidence_label in (
        _METRIC_GLOBAL_UNIQUE_INDEXES
    ):
        owner = connection.execute(text(
            "SELECT evidence_id, entity_type, strategy_key, strategy_version "
            "FROM st_strategy_metric_input "
            f"WHERE {hash_column}=:{hash_column} LIMIT 1 FOR UPDATE"
        ), evidence_payload).mappings().first()
        if owner is None:
            continue
        owner_identity = ":".join((
            str(owner.get("entity_type") or "STRATEGY"),
            str(owner.get("strategy_key") or ""),
            str(owner.get("strategy_version") or ""),
        ))
        raise ValueError(
            f"同一{evidence_label}已归属于实体版本{owner_identity}，"
            "不能作为另一条独立资金证据复用"
        )


_METRIC_EVIDENCE_AUDIT_ACTIONS = frozenset({
    "ADD_METRIC_EVIDENCE",
    "CONFIRM_METRIC_EVIDENCE",
    "REJECT_METRIC_EVIDENCE",
})


def _valid_audit_envelope(row: dict[str, Any]) -> dict[str, Any] | None:
    """Return a hash/column-bound immutable audit envelope."""

    before = _json(row.get("before_json"), None)
    after = _json(row.get("after_json"), None)
    evidence = _json(row.get("evidence_json"), None)
    payload = _json(row.get("payload_json"), None)
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "entity_type", "entity_key", "action", "reason", "operator",
            "before", "after", "evidence", "nonce",
        }
        or payload.get("entity_type") != row.get("entity_type")
        or payload.get("entity_key") != row.get("entity_key")
        or payload.get("action") != row.get("action")
        or payload.get("reason") != row.get("reason")
        or payload.get("operator") != row.get("operator_name")
        or payload.get("before") != before
        or payload.get("after") != after
        or payload.get("evidence") != evidence
        or re.fullmatch(
            r"[0-9a-f]{32}", str(payload.get("nonce") or "")
        )
        is None
        or _digest(payload) != str(row.get("audit_hash") or "")
    ):
        return None
    return {
        "row": row,
        "before": before,
        "after": after,
        "evidence": evidence,
        "payload": payload,
    }


def _metric_submission_contract(row: dict[str, Any]) -> dict[str, Any] | None:
    metrics = _json(row.get("metrics_json"), None)
    if not isinstance(metrics, dict):
        return None
    try:
        as_of_date = _trade_date(row.get("as_of_date"), default_today=False)
    except ValueError:
        return None
    revision_at = _normalize_evidence_revision(
        row.get("evidence_revision_at")
    )
    if not revision_at:
        return None
    return {
        "strategy_key": str(row.get("strategy_key") or ""),
        "entity_type": str(row.get("entity_type") or ""),
        "strategy_version": str(row.get("strategy_version") or ""),
        "as_of_date": as_of_date,
        "window_days": _int(row.get("window_days")),
        "metrics": metrics,
        "source": str(row.get("source") or ""),
        "evidence_protocol": str(row.get("evidence_protocol") or ""),
        "artifact_hash": str(row.get("artifact_hash") or ""),
        "source_dataset_hash": str(row.get("source_dataset_hash") or ""),
        "evidence_revision_at": revision_at,
        "verification_status": "PENDING",
        "funding_provenance": "EXTERNAL_SUBMITTED",
    }


def metric_evidence_audit_binding(
    row: dict[str, Any], audit_rows: list[dict[str, Any]],
) -> tuple[bool, dict[str, Any]]:
    """Bind one evidence row to exactly one add and one terminal review audit.

    This pure contract is shared by the funding loader and production health
    replay.  A legal-looking direct database review without its immutable
    audit therefore remains research-only and fails closed.
    """

    evidence_id = str(row.get("evidence_id") or "")
    status = str(row.get("verification_status") or "")
    submitted_by = str(row.get("submitted_by") or "")
    reviewed_by = str(row.get("reviewed_by") or "")
    reviewed_at = _normalize_evidence_revision(row.get("reviewed_at"))
    created_at = _normalize_evidence_revision(row.get("created_at"))
    submission = _metric_submission_contract(row)
    base_valid = bool(
        re.fullmatch(r"[0-9a-f]{32}", evidence_id)
        and status in {"PENDING", "CONFIRMED", "REJECTED"}
        and submitted_by
        and created_at
        and submission is not None
        and submission["entity_type"] in {"STRATEGY", "COMBINATION"}
        and submission["strategy_key"]
        and submission["strategy_version"]
        and submission["window_days"] in WINDOWS
        and str(row.get("funding_provenance") or "")
        == "EXTERNAL_SUBMITTED"
        and _digest(submission) == str(row.get("evidence_hash") or "")
    )
    related_raw: list[dict[str, Any]] = []
    related_valid: list[dict[str, Any]] = []
    for audit_row in audit_rows:
        if str(audit_row.get("action") or "") not in (
            _METRIC_EVIDENCE_AUDIT_ACTIONS
        ):
            continue
        raw_evidence = _json(audit_row.get("evidence_json"), None)
        if (
            isinstance(raw_evidence, dict)
            and str(raw_evidence.get("evidence_id") or "") == evidence_id
        ):
            related_raw.append(audit_row)
            envelope = _valid_audit_envelope(audit_row)
            if envelope is not None:
                related_valid.append(envelope)

    add_audits = [
        item for item in related_valid
        if item["row"].get("action") == "ADD_METRIC_EVIDENCE"
    ]
    confirm_audits = [
        item for item in related_valid
        if item["row"].get("action") == "CONFIRM_METRIC_EVIDENCE"
    ]
    reject_audits = [
        item for item in related_valid
        if item["row"].get("action") == "REJECT_METRIC_EVIDENCE"
    ]
    expected_add_evidence = {
        "evidence_id": evidence_id,
        "evidence_hash": str(row.get("evidence_hash") or ""),
        "artifact_hash": str(row.get("artifact_hash") or ""),
        "source_dataset_hash": str(row.get("source_dataset_hash") or ""),
        "verification_status": "PENDING",
    }
    add_valid = bool(
        len(add_audits) == 1
        and add_audits[0]["row"].get("entity_type")
        == row.get("entity_type")
        and add_audits[0]["row"].get("entity_key")
        == row.get("strategy_key")
        and add_audits[0]["row"].get("operator_name") == submitted_by
        and bool(str(add_audits[0]["row"].get("reason") or "").strip())
        and add_audits[0]["before"] == {}
        and add_audits[0]["after"] == submission
        and add_audits[0]["evidence"] == expected_add_evidence
        and _normalize_evidence_revision(
            add_audits[0]["row"].get("created_at")
        )
        >= created_at
    )

    expected_review_count = 0 if status == "PENDING" else 1
    expected_review_action = (
        "CONFIRM_METRIC_EVIDENCE"
        if status == "CONFIRMED"
        else "REJECT_METRIC_EVIDENCE"
    )
    expected_review_audits = (
        confirm_audits if status == "CONFIRMED" else reject_audits
    )
    review_valid = bool(
        (status == "PENDING" and not reviewed_by and not reviewed_at)
        or (
            status in {"CONFIRMED", "REJECTED"}
            and reviewed_by
            and reviewed_by != submitted_by
            and reviewed_at
            and reviewed_at >= created_at
            and len(expected_review_audits) == 1
            and expected_review_audits[0]["row"].get("action")
            == expected_review_action
            and expected_review_audits[0]["row"].get("entity_type")
            == row.get("entity_type")
            and expected_review_audits[0]["row"].get("entity_key")
            == row.get("strategy_key")
            and expected_review_audits[0]["row"].get("operator_name")
            == reviewed_by
            and bool(str(
                expected_review_audits[0]["row"].get("reason") or ""
            ).strip())
            and expected_review_audits[0]["before"]
            == {"verification_status": "PENDING"}
            and expected_review_audits[0]["after"]
            == {
                "verification_status": status,
                "reviewed_by": reviewed_by,
                "reviewed_at": reviewed_at,
            }
            and expected_review_audits[0]["evidence"]
            == {
                "evidence_id": evidence_id,
                "evidence_hash": str(row.get("evidence_hash") or ""),
                "artifact_hash": str(row.get("artifact_hash") or ""),
                "submitted_by": submitted_by,
                "reviewed_by": reviewed_by,
                "reviewed_at": reviewed_at,
            }
            and _normalize_evidence_revision(
                expected_review_audits[0]["row"].get("created_at")
            )
            >= reviewed_at
        )
    )
    expected_total = 1 + expected_review_count
    counts_valid = bool(
        len(related_raw) == expected_total
        and len(related_valid) == expected_total
        and len(add_audits) == 1
        and len(confirm_audits) == (1 if status == "CONFIRMED" else 0)
        and len(reject_audits) == (1 if status == "REJECTED" else 0)
    )
    valid = base_valid and add_valid and review_valid and counts_valid
    return valid, {
        "evidence_id": evidence_id,
        "verification_status": status,
        "base_valid": base_valid,
        "add_audit_count": len(add_audits),
        "confirm_audit_count": len(confirm_audits),
        "reject_audit_count": len(reject_audits),
        "related_raw_audit_count": len(related_raw),
        "related_valid_audit_count": len(related_valid),
        "add_audit_valid": add_valid,
        "review_audit_valid": review_valid,
        "counts_valid": counts_valid,
    }


def _metric_evidence_audit_rows(
    entity_type: str, *, connection=None,
) -> list[dict[str, Any]]:
    sql = (
        "SELECT audit_id, entity_type, entity_key, action, reason, "
        "operator_name, before_json, after_json, evidence_json, "
        "payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "WHERE entity_type=:entity_type AND action IN "
        "('ADD_METRIC_EVIDENCE','CONFIRM_METRIC_EVIDENCE',"
        "'REJECT_METRIC_EVIDENCE') "
        "ORDER BY created_at, audit_id"
    )
    params = {"entity_type": entity_type}
    if connection is None:
        return _db_read(sql, params)
    return [
        dict(row) for row in connection.execute(
            text(sql), params,
        ).mappings().all()
    ]


def record_metric_input(payload: dict[str, Any], *, operator: str = "api") -> dict[str, Any]:
    ensure_and_seed_governance()
    key = validate_strategy_key(str(payload.get("strategy_key") or ""))
    entity_type = str(payload.get("entity_type") or "STRATEGY").upper()
    if entity_type not in {"STRATEGY", "COMBINATION"}:
        raise ValueError("指标证据对象只能是策略或组合")
    if entity_type == "COMBINATION":
        entities = {row["combination_key"]: row for row in load_combinations()}
        if key not in entities:
            raise ValueError("组合未注册")
        version = entities[key]["current_version"]
    else:
        entities = {row["strategy_key"]: row for row in load_registry()}
        if key not in entities:
            raise ValueError("策略未注册")
        version = entities[key]["current_version"]
    bound_version = str(payload.get("bound_strategy_version") or "").strip()
    if bound_version != version:
        raise ValueError("证据声明的策略版本与当前不可变版本不一致")
    window_days = _int(payload.get("window_days"), 60)
    if window_days not in WINDOWS:
        raise ValueError("窗口只能是20、60或120日")
    metrics = _validated_metric_evidence(payload.get("metrics"))
    protocol = str(payload.get("evidence_protocol") or "").strip().upper()
    artifact_hash = str(payload.get("artifact_hash") or "").strip().lower()
    if metrics.get("walk_forward_verified") is True:
        if protocol not in _VERIFIED_WALK_FORWARD_PROTOCOLS:
            raise ValueError("已验证Walk-Forward必须使用受支持的时序隔离验证协议")
        if metrics.get("independent_oos") is not True:
            raise ValueError("Walk-Forward验证必须是独立样本外证据")
    elif not protocol:
        raise ValueError("指标证据必须声明验证协议")
    if not _HASH_PATTERN.fullmatch(artifact_hash):
        raise ValueError("指标证据必须绑定64位小写SHA-256产物哈希")
    revision_raw = str(payload.get("evidence_revision_at") or "").strip()
    try:
        revision_at = datetime.fromisoformat(
            revision_raw.replace("Z", "+00:00")
        )
    except ValueError:
        raise ValueError("证据高水位必须是ISO日期或时间") from None
    if revision_at.tzinfo is not None:
        revision_at = revision_at.astimezone().replace(tzinfo=None)
    as_of_date = _trade_date(payload.get("as_of_date"), default_today=False)
    if revision_at.date() > date.fromisoformat(as_of_date):
        raise ValueError("证据高水位不能晚于指标截止日")
    if revision_at > datetime.now():
        raise ValueError("证据高水位不能晚于当前时间")
    normalized_revision = revision_at.isoformat(timespec="seconds")
    version_created_at = str(entities[key].get("version_created_at") or "")
    expected_max_holding_days = _version_max_holding_days(
        entity_type, key, version
    )
    artifact = _validate_metric_artifact(
        payload.get("artifact_manifest"),
        entity_type=entity_type,
        entity_key=key,
        entity_version=version,
        as_of_date=as_of_date,
        window_days=window_days,
        evidence_protocol=protocol,
        evidence_revision_at=normalized_revision,
        metrics=metrics,
        artifact_hash=artifact_hash,
        version_created_at=version_created_at,
        expected_max_holding_days=expected_max_holding_days,
        expected_label_horizon_days=_version_label_horizon_days(
            entity_type, key, version
        ),
    )
    source_dataset_hash = str(artifact.get("source_dataset_hash") or "")
    metrics.update({
        "version_bound_evidence": True,
        "evidence_protocol": protocol,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
        "evidence_revision_at": normalized_revision,
        # Browser/API submissions are useful research evidence, but cannot
        # attest overlapping positions, daily marks and cash movements.
        "funding_provenance": "EXTERNAL_SUBMITTED",
    })
    evidence_payload = {
        "strategy_key": key,
        "entity_type": entity_type,
        "strategy_version": version,
        "as_of_date": as_of_date,
        "window_days": window_days,
        "metrics": metrics,
        "source": str(payload.get("source") or "manual_evidence")[:80],
        "evidence_protocol": protocol,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
        "evidence_revision_at": normalized_revision,
        "verification_status": "PENDING",
        "funding_provenance": "EXTERNAL_SUBMITTED",
    }
    if date.fromisoformat(evidence_payload["as_of_date"]) > date.today():
        raise ValueError("指标证据日期不能晚于今天")
    evidence_hash = _digest(evidence_payload)
    evidence_id = uuid.uuid4().hex
    submitted_by = str(operator or "api")[:80]
    table = (
        "st_strategy_combination"
        if entity_type == "COMBINATION"
        else "st_strategy_registry"
    )
    key_column = (
        "combination_key"
        if entity_type == "COMBINATION"
        else "strategy_key"
    )
    duplicate_sql = (
        "SELECT i.*, e.current_version AS registry_current_version "
        "FROM st_strategy_metric_input i "
        f"INNER JOIN {table} e ON e.{key_column}=i.strategy_key "
        "WHERE i.entity_type=:entity_type "
        "AND i.strategy_key=:strategy_key "
        "AND i.strategy_version=:strategy_version "
        "AND i.as_of_date=:as_of_date AND i.window_days=:window_days"
    )

    def idempotent_result(existing: dict[str, Any]) -> dict[str, Any]:
        if (
            str(existing.get("strategy_version") or "") != version
            or str(existing.get("registry_current_version") or "") != version
        ):
            raise RuntimeError("指标证据绑定版本已非当前版本，请按新版本重新提交")
        if str(existing.get("evidence_hash") or "") != evidence_hash:
            raise ValueError("同一版本、日期和窗口的指标证据不可覆盖")
        audit_valid, _audit_detail = metric_evidence_audit_binding(
            existing, _metric_evidence_audit_rows(entity_type),
        )
        if not audit_valid:
            raise RuntimeError("已有指标证据缺少完整不可变提交/复核审计")
        return {
            **evidence_payload,
            "evidence_id": existing.get("evidence_id"),
            "verification_status": existing.get("verification_status"),
            "evidence_hash": evidence_hash,
            "idempotent_replay": True,
        }

    duplicate = _db_read(
        duplicate_sql,
        evidence_payload,
    )
    if duplicate:
        return idempotent_result(duplicate[0])
    reason = str(payload.get("reason") or "新增验证证据")[:500]
    try:
        with get_engine().begin() as connection:
            current = connection.execute(text(
                f"SELECT current_version FROM {table} "
                f"WHERE {key_column}=:key FOR UPDATE"
            ), {"key": key}).mappings().first()
            if current is None or str(current.get("current_version") or "") != version:
                raise RuntimeError("证据写入期间版本已更新，请按新版本重新提交")
            latest = connection.execute(text(
                "SELECT evidence_revision_at, artifact_hash "
                "FROM st_strategy_metric_input "
                "WHERE entity_type=:entity_type "
                "AND strategy_key=:strategy_key "
                "AND strategy_version=:strategy_version "
                "AND window_days=:window_days "
                "ORDER BY evidence_revision_at DESC, as_of_date DESC, "
                "created_at DESC LIMIT 1 FOR UPDATE"
            ), evidence_payload).mappings().first()
            if latest is not None:
                latest_revision = _normalize_evidence_revision(
                    latest.get("evidence_revision_at")
                )
                requested_revision = _normalize_evidence_revision(
                    evidence_payload["evidence_revision_at"]
                )
                if latest_revision and requested_revision <= latest_revision:
                    raise ValueError(
                        "新指标证据的高水位必须严格晚于该版本、该窗口已有证据"
                    )
            _assert_global_metric_evidence_unclaimed(
                connection,
                evidence_payload,
            )
            connection.execute(text(
                """
                INSERT INTO st_strategy_metric_input
                (evidence_id, entity_type, strategy_key, strategy_version,
                 as_of_date, window_days, metrics_json, source,
                 evidence_protocol, artifact_hash, artifact_json,
                  source_dataset_hash, evidence_revision_at,
                 verification_status, funding_provenance, submitted_by,
                 evidence_hash)
                VALUES (:evidence_id, :entity_type, :strategy_key,
                        :strategy_version, :as_of_date, :window_days,
                        :metrics_json, :source, :evidence_protocol,
                        :artifact_hash, :artifact_json,
                        :source_dataset_hash, :evidence_revision_at, 'PENDING',
                        'EXTERNAL_SUBMITTED', :submitted_by, :evidence_hash)
                """
            ), {
                **evidence_payload, "evidence_id": evidence_id,
                "metrics_json": _json_text(metrics),
                "artifact_json": _json_text(artifact),
                "submitted_by": submitted_by,
                "evidence_hash": evidence_hash,
            })
            _append_audit_connection(
                connection, entity_type=entity_type, entity_key=key,
                action="ADD_METRIC_EVIDENCE", reason=reason,
                operator=submitted_by,
                before={}, after=evidence_payload,
                evidence={
                    "evidence_id": evidence_id,
                    "evidence_hash": evidence_hash,
                    "artifact_hash": artifact_hash,
                    "source_dataset_hash": source_dataset_hash,
                    "verification_status": "PENDING",
                },
            )
    except IntegrityError as exc:
        # A concurrent identical first submission blocks on the unique key
        # until its winner commits.  Re-read only that immutable identity and
        # accept it solely when version, content hash and audit chain all match.
        concurrent = _db_read(duplicate_sql, evidence_payload)
        if not concurrent:
            raise
        try:
            return idempotent_result(concurrent[0])
        except (RuntimeError, ValueError) as replay_error:
            raise replay_error from exc
    return {
        **evidence_payload,
        "evidence_id": evidence_id,
        "verification_status": "PENDING",
        "evidence_hash": evidence_hash,
        "idempotent_replay": False,
    }


def review_metric_input(
    evidence_id: str, *, decision: str, reason: str, operator: str,
) -> dict[str, Any]:
    """Independently confirm or reject a submitted validation artifact."""

    ensure_and_seed_governance()
    evidence_key = str(evidence_id or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", evidence_key):
        raise ValueError("证据编号格式无效")
    review_decision = str(decision or "").strip().upper()
    if review_decision not in {"CONFIRM", "REJECT"}:
        raise ValueError("复核结果只能是确认或驳回")
    review_reason = str(reason or "").strip()[:500]
    if not review_reason:
        raise ValueError("证据复核必须填写理由")
    reviewer = str(operator or "api")[:80]
    with get_engine().begin() as connection:
        row = connection.execute(text(
            "SELECT * FROM st_strategy_metric_input "
            "WHERE evidence_id=:evidence_id FOR UPDATE"
        ), {"evidence_id": evidence_key}).mappings().first()
        if row is None:
            raise ValueError("指标证据不存在")
        current_status = str(row.get("verification_status") or "PENDING")
        target_status = (
            "CONFIRMED" if review_decision == "CONFIRM" else "REJECTED"
        )
        submitter = str(row.get("submitted_by") or "")
        entity_type = str(row.get("entity_type") or "STRATEGY")
        entity_key = str(row.get("strategy_key") or "")
        entity_version = str(row.get("strategy_version") or "")
        if not submitter or submitter == reviewer:
            raise ValueError("证据提交者与复核者必须分离")
        audit_valid, _audit_detail = metric_evidence_audit_binding(
            dict(row),
            _metric_evidence_audit_rows(
                entity_type, connection=connection,
            ),
        )
        if current_status != "PENDING":
            if current_status == target_status and audit_valid:
                return {
                    "evidence_id": evidence_key,
                    "verification_status": current_status,
                    "reviewed_by": row.get("reviewed_by"),
                    "reviewed_at": _normalize_evidence_revision(
                        row.get("reviewed_at")
                    ),
                    "idempotent_replay": True,
                }
            if current_status == target_status:
                raise RuntimeError("已复核指标证据缺少完整不可变审计")
            raise ValueError("证据已完成复核，不可改写结果")
        if not audit_valid:
            raise RuntimeError("待复核指标证据缺少完整不可变提交审计")
        if target_status == "CONFIRMED":
            table = (
                "st_strategy_combination"
                if entity_type == "COMBINATION"
                else "st_strategy_registry"
            )
            key_column = (
                "combination_key"
                if entity_type == "COMBINATION"
                else "strategy_key"
            )
            current_version = connection.execute(text(
                f"SELECT current_version FROM {table} "
                f"WHERE {key_column}=:entity_key FOR UPDATE"
            ), {"entity_key": entity_key}).scalar()
            if str(current_version or "") != entity_version:
                raise ValueError("证据绑定版本已非当前版本，不得确认")
            stored_metrics = _json(row.get("metrics_json"), {})
            core_metrics = dict(stored_metrics)
            for field in (
                "version_bound_evidence", "evidence_protocol",
                "artifact_hash", "source_dataset_hash",
                "evidence_revision_at", "funding_provenance",
            ):
                core_metrics.pop(field, None)
            version_table = (
                "st_strategy_combination_version"
                if entity_type == "COMBINATION"
                else "st_strategy_version"
            )
            version_key_column = (
                "combination_key"
                if entity_type == "COMBINATION"
                else "strategy_key"
            )
            version_created_at = connection.execute(text(
                f"SELECT created_at FROM {version_table} "
                f"WHERE {version_key_column}=:entity_key "
                "AND version=:entity_version"
            ), {
                "entity_key": entity_key,
                "entity_version": entity_version,
            }).scalar()
            if version_created_at is None:
                raise ValueError("证据绑定的不可变版本不存在")
            artifact = _validate_metric_artifact(
                _json(row.get("artifact_json"), None),
                entity_type=entity_type,
                entity_key=entity_key,
                entity_version=entity_version,
                as_of_date=_trade_date(
                    row.get("as_of_date"), default_today=False
                ),
                window_days=_int(row.get("window_days")),
                evidence_protocol=str(row.get("evidence_protocol") or ""),
                evidence_revision_at=_normalize_evidence_revision(
                    row.get("evidence_revision_at")
                ),
                metrics=core_metrics,
                artifact_hash=str(row.get("artifact_hash") or ""),
                version_created_at=str(version_created_at),
                expected_max_holding_days=_version_max_holding_days(
                    entity_type, entity_key, entity_version,
                    connection=connection,
                ),
                expected_label_horizon_days=_version_label_horizon_days(
                    entity_type, entity_key, entity_version,
                    connection=connection,
                ),
            )
            if str(row.get("source_dataset_hash") or "") != str(
                artifact.get("source_dataset_hash") or ""
            ):
                raise ValueError("证据记录与底层样本集哈希不一致")
        reviewed_at_value = connection.execute(text(
            "SELECT NOW() AS reviewed_at"
        )).scalar()
        reviewed_at_text = _normalize_evidence_revision(reviewed_at_value)
        if not reviewed_at_text:
            raise RuntimeError("数据库未返回有效复核时间")
        updated = connection.execute(text(
            "UPDATE st_strategy_metric_input "
            "SET verification_status=:target_status, "
            "reviewed_by=:reviewed_by, reviewed_at=:reviewed_at "
            "WHERE evidence_id=:evidence_id "
            "AND verification_status='PENDING'"
        ), {
            "target_status": target_status,
            "reviewed_by": reviewer,
            "reviewed_at": reviewed_at_value,
            "evidence_id": evidence_key,
        })
        if updated.rowcount != 1:
            raise RuntimeError("证据复核状态已被并发更新")
        _append_audit_connection(
            connection,
            entity_type=entity_type,
            entity_key=entity_key,
            action=(
                "CONFIRM_METRIC_EVIDENCE"
                if target_status == "CONFIRMED"
                else "REJECT_METRIC_EVIDENCE"
            ),
            reason=review_reason,
            operator=reviewer,
            before={"verification_status": "PENDING"},
            after={
                "verification_status": target_status,
                "reviewed_by": reviewer,
                "reviewed_at": reviewed_at_text,
            },
            evidence={
                "evidence_id": evidence_key,
                "evidence_hash": row.get("evidence_hash"),
                "artifact_hash": row.get("artifact_hash"),
                "submitted_by": submitter,
                "reviewed_by": reviewer,
                "reviewed_at": reviewed_at_text,
            },
        )
    return {
        "evidence_id": evidence_key,
        "verification_status": target_status,
        "reviewed_by": reviewer,
        "reviewed_at": reviewed_at_text,
        "idempotent_replay": False,
    }


def metric_evidence_detail(evidence_id: str) -> dict[str, Any]:
    """Return one stored artifact for an authenticated independent reviewer."""

    if not _table_exists("st_strategy_metric_input"):
        raise RuntimeError("策略治理表尚未由部署流程创建")
    evidence_key = str(evidence_id or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", evidence_key):
        raise ValueError("证据编号格式无效")
    rows = _db_read(
        "SELECT * FROM st_strategy_metric_input WHERE evidence_id=:evidence_id",
        {"evidence_id": evidence_key},
    )
    if not rows:
        raise ValueError("指标证据不存在")
    row = dict(rows[0])
    metrics = _json(row.pop("metrics_json", None), {})
    artifact = _json(row.pop("artifact_json", None), None)
    entity_type = str(row.get("entity_type") or "STRATEGY")
    entity_key = str(row.get("strategy_key") or "")
    entity_version = str(row.get("strategy_version") or "")
    version_table = (
        "st_strategy_combination_version"
        if entity_type == "COMBINATION"
        else "st_strategy_version"
    )
    version_key_column = (
        "combination_key"
        if entity_type == "COMBINATION"
        else "strategy_key"
    )
    version_rows = _db_read(
        f"SELECT created_at FROM {version_table} "
        f"WHERE {version_key_column}=:entity_key AND version=:entity_version",
        {"entity_key": entity_key, "entity_version": entity_version},
    )
    validation_status = "VALID"
    validation_reason = "逐笔样本、组合权益曲线、窗口、版本和哈希可重算"
    try:
        if not version_rows:
            raise ValueError("证据绑定的不可变版本不存在")
        core_metrics = dict(metrics)
        for field in (
            "version_bound_evidence", "evidence_protocol", "artifact_hash",
            "source_dataset_hash", "evidence_revision_at",
            "funding_provenance",
        ):
            core_metrics.pop(field, None)
        _validate_metric_artifact(
            artifact,
            entity_type=entity_type,
            entity_key=entity_key,
            entity_version=entity_version,
            as_of_date=_trade_date(row.get("as_of_date"), default_today=False),
            window_days=_int(row.get("window_days")),
            evidence_protocol=str(row.get("evidence_protocol") or ""),
            evidence_revision_at=_normalize_evidence_revision(
                row.get("evidence_revision_at")
            ),
            metrics=core_metrics,
            artifact_hash=str(row.get("artifact_hash") or ""),
            version_created_at=str(version_rows[0].get("created_at") or ""),
            expected_max_holding_days=_version_max_holding_days(
                entity_type, entity_key, entity_version
            ),
            expected_label_horizon_days=_version_label_horizon_days(
                entity_type, entity_key, entity_version
            ),
        )
        if str(row.get("source_dataset_hash") or "") != str(
            artifact.get("source_dataset_hash") if isinstance(artifact, dict) else ""
        ):
            raise ValueError("证据记录与底层样本集哈希不一致")
    except (TypeError, ValueError) as exc:
        validation_status = "INVALID"
        validation_reason = str(exc)[:500]
    return {
        **row,
        "verification_status_label": EVIDENCE_STATUS_LABELS.get(
            str(row.get("verification_status") or ""), "未知复核状态"
        ),
        "validation_status": validation_status,
        "validation_status_label": (
            "产物可重算" if validation_status == "VALID" else "产物校验失败"
        ),
        "validation_reason": validation_reason,
        "metrics": metrics,
        "artifact_manifest": artifact,
        "automatic_real_order_submission": False,
    }


def _aggregate_forward_intent_episodes(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse immutable fill facts into one cash-return sample per intent.

    An episode is the exact ``(source_intent_id, strategy_version)`` pair.
    Every BUY fill belonging to that immutable intent must be present and cash
    bound.  The episode matures only after every member fill is fully closed;
    its return is recomputed from aggregate entry/exit cash flows.  Therefore
    broker partial fills, order slicing, and replay order cannot increase the
    statistical sample count.
    """

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    intent_contracts: dict[str, tuple[str, str, str, str, str]] = {}
    seen_evidence_ids: set[str] = set()
    seen_entry_fill_ids: set[str] = set()
    for source in records:
        row = dict(source)
        source_intent_id = str(row.get("source_intent_id") or "").strip()
        strategy_version = str(
            row.get("bound_strategy_version")
            or row.get("strategy_version")
            or ""
        ).strip()
        account_id = str(row.get("account_id") or "").strip()
        strategy_key = str(row.get("strategy_key") or "").strip()
        stock_code = str(row.get("stock_code") or "").strip()
        source_run_uid = str(row.get("source_run_uid") or "").strip()
        evidence_id = str(row.get("evidence_id") or "").strip()
        entry_fill_id = str(row.get("entry_fill_id") or "").strip()
        if (
            not re.fullmatch(r"[0-9A-Za-z_.:-]{1,64}", source_intent_id)
            or not strategy_version
            or len(strategy_version) > 160
            or not account_id
            or not strategy_key
            or not re.fullmatch(r"[0-9]{6}", stock_code)
            or not source_run_uid
            or not evidence_id
            or not entry_fill_id
        ):
            raise ValueError("前向意图样本缺少不可变意图、版本、成交或归属标识")
        if evidence_id in seen_evidence_ids or entry_fill_id in seen_entry_fill_ids:
            raise ValueError("前向意图样本包含重复证据或重复买入成交")
        seen_evidence_ids.add(evidence_id)
        seen_entry_fill_ids.add(entry_fill_id)
        contract = (
            account_id, strategy_version, strategy_key, stock_code,
            source_run_uid,
        )
        existing_contract = intent_contracts.setdefault(
            source_intent_id, contract,
        )
        if existing_contract != contract:
            raise ValueError("同一不可变意图被绑定到不同账户、版本、策略、证券或决策运行")

        quantity = _int(row.get("entry_quantity"), -1)
        closed_quantity = _int(row.get("closed_quantity"), -1)
        status = str(row.get("evidence_status") or "")
        if (
            quantity <= 0
            or closed_quantity < 0
            or closed_quantity > quantity
            or status not in {"MATURED", "OPEN", "PARTIALLY_CLOSED"}
            or _int(row.get("entry_cash_binding_count"), 0) != 1
        ):
            raise ValueError("前向意图样本的数量、状态或买入现金绑定无效")
        numeric: dict[str, Decimal] = {}
        for field in (
            "entry_gross_cny", "entry_fee_cny", "exit_gross_cny",
            "exit_fee_cny",
        ):
            value = Decimal(str(row.get(field) or "0"))
            if not value.is_finite() or value < 0:
                raise ValueError("前向意图样本包含无效现金金额")
            numeric[field] = value
        if numeric["entry_gross_cny"] <= 0:
            raise ValueError("前向意图样本缺少有效买入资金")
        if status == "MATURED":
            exit_at = _normalize_evidence_revision(row.get("exit_at"))
            if closed_quantity != quantity or not exit_at:
                raise ValueError("成熟意图成员未完全平仓")
            cost_basis = (
                numeric["entry_gross_cny"] + numeric["entry_fee_cny"]
            )
            expected_return = (
                numeric["exit_gross_cny"] - numeric["exit_fee_cny"]
                - cost_basis
            ) / cost_basis * Decimal("100")
            if row.get("return_pct") is None:
                raise ValueError("成熟意图成员缺少可重算收益率")
            reported_return = Decimal(str(row.get("return_pct")))
            if (
                not reported_return.is_finite()
                or abs(reported_return - expected_return) > Decimal("0.0001")
            ):
                raise ValueError("成熟意图成员收益率无法由资金事实重算")
        row["_episode_numeric"] = numeric
        groups[(source_intent_id, strategy_version)].append(row)

    episodes: list[dict[str, Any]] = []
    for (source_intent_id, strategy_version), members in groups.items():
        expected_counts = {
            _int(item.get("source_intent_buy_fill_count"), -1)
            for item in members
        }
        expected_quantities = {
            _int(item.get("source_intent_entry_quantity"), -1)
            for item in members
        }
        expected_gross = {
            Decimal(str(
                item.get("source_intent_entry_gross_cny")
                if item.get("source_intent_entry_gross_cny") is not None
                else "-1"
            ))
            for item in members
        }
        expected_fees = {
            Decimal(str(
                item.get("source_intent_entry_fee_cny")
                if item.get("source_intent_entry_fee_cny") is not None
                else "-1"
            ))
            for item in members
        }
        if (
            len(expected_counts) != 1
            or next(iter(expected_counts)) != len(members)
            or len(expected_quantities) != 1
            or len(expected_gross) != 1
            or len(expected_fees) != 1
        ):
            raise ValueError("意图买入成交全集认证缺失、不一致或未完整覆盖")
        entry_quantity = sum(
            _int(item.get("entry_quantity")) for item in members
        )
        entry_gross = sum(
            (item["_episode_numeric"]["entry_gross_cny"] for item in members),
            Decimal("0"),
        )
        entry_fee = sum(
            (item["_episode_numeric"]["entry_fee_cny"] for item in members),
            Decimal("0"),
        )
        if (
            next(iter(expected_quantities)) != entry_quantity
            or abs(next(iter(expected_gross)) - entry_gross)
                > Decimal("0.000001")
            or abs(next(iter(expected_fees)) - entry_fee)
                > Decimal("0.000001")
        ):
            raise ValueError("意图买入成交数量或资金无法与原始成交全集对账")
        if any(
            str(item.get("evidence_status") or "") != "MATURED"
            for item in members
        ):
            continue
        exit_gross = sum(
            (item["_episode_numeric"]["exit_gross_cny"] for item in members),
            Decimal("0"),
        )
        exit_fee = sum(
            (item["_episode_numeric"]["exit_fee_cny"] for item in members),
            Decimal("0"),
        )
        cost_basis = entry_gross + entry_fee
        if cost_basis <= 0 or exit_gross <= 0:
            raise ValueError("成熟意图样本缺少有效资金流")
        net_pnl = exit_gross - exit_fee - cost_basis
        net_return = net_pnl / cost_basis * Decimal("100")
        cost_pct = (entry_fee + exit_fee) / entry_gross * Decimal("100")
        entry_times = [
            _normalize_evidence_revision(item.get("entry_at"))
            for item in members
        ]
        exit_times = [
            _normalize_evidence_revision(item.get("exit_at"))
            for item in members
        ]
        if not all(entry_times) or not all(exit_times):
            raise ValueError("成熟意图样本缺少完整成交时间")
        episode_id = intent_episode_id(source_intent_id, strategy_version)
        first = members[0]
        episodes.append({
            "evidence_id": episode_id,
            "episode_id": episode_id,
            "episode_protocol": INTENT_EPISODE_PROTOCOL,
            "source_intent_id": source_intent_id,
            "account_id": str(first.get("account_id") or ""),
            "source_run_uid": str(first.get("source_run_uid") or ""),
            "stock_code": str(first.get("stock_code") or ""),
            "strategy_key": str(first.get("strategy_key") or ""),
            "strategy_version": strategy_version,
            "bound_strategy_version": strategy_version,
            "entry_at": min(entry_times),
            "entry_trade_date": min(value[:10] for value in entry_times),
            "entry_quantity": entry_quantity,
            "entry_gross_cny": entry_gross,
            "entry_fee_cny": entry_fee,
            "closed_quantity": entry_quantity,
            "exit_at": max(exit_times),
            "trade_date": max(value[:10] for value in exit_times),
            "exit_gross_cny": exit_gross,
            "exit_fee_cny": exit_fee,
            "realized_net_pnl_cny": net_pnl,
            "return_pct": float(net_return),
            "actual_cost_pct": float(cost_pct),
            "is_net_return": True,
            "evidence_status": "MATURED",
            "evidence_revision_at": max(exit_times),
            "episode_member_fill_count": len(members),
            "source_entry_fill_ids": sorted(
                str(item.get("entry_fill_id") or "") for item in members
            ),
        })
    episodes.sort(key=lambda item: (
        item["trade_date"], item["evidence_revision_at"], item["episode_id"],
    ))
    return episodes


def calculate_return_metrics(
    records: Iterable[dict[str, Any]], *, window_days: int,
    estimated_cost_pct: float = DEFAULT_ROUND_TRIP_COST_PCT,
    market_match_score: float | None = None,
    walk_forward_verified: bool = False,
    version_bound_evidence: bool = False,
    independent_oos: bool = False,
) -> dict[str, Any]:
    rows = []
    for item in records:
        value = _num(item.get("return_pct"), None)
        if value is None:
            continue
        trade_day = _trade_date(
            item.get("trade_date"), default_today=False
        )
        revision_at = str(
            item.get("evidence_revision_at") or trade_day
        ).strip().replace(" ", "T")
        actual_cost = _num(item.get("actual_cost_pct"), None)
        rows.append({
            "return_pct": value,
            "trade_date": trade_day,
            "evidence_revision_at": revision_at,
            "is_net_return": item.get("is_net_return") is True,
            "cost_pct": (
                actual_cost if actual_cost is not None
                else estimated_cost_pct
            ),
            "actual_cost_bound": actual_cost is not None,
            "evidence_id": str(item.get("evidence_id") or ""),
        })
    rows.sort(key=lambda item: (
        item["trade_date"], item["evidence_revision_at"],
        item["evidence_id"],
    ))
    values = [
        item["return_pct"] + item["cost_pct"]
        if item["is_net_return"] else item["return_pct"]
        for item in rows
    ]
    net_values = [
        item["return_pct"]
        if item["is_net_return"] else item["return_pct"] - item["cost_pct"]
        for item in rows
    ]
    effective_cost_pct = (
        sum(item["cost_pct"] for item in rows) / len(rows)
        if rows else estimated_cost_pct
    )
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    avg_win = sum(wins) / len(wins) if wins else None
    avg_loss = abs(sum(losses) / len(losses)) if losses else None
    win_rate = len(wins) / len(values) * 100 if values else None
    payoff = avg_win / avg_loss if avg_win is not None and avg_loss else None
    gross_expectancy = sum(values) / len(values) if values else None
    net_expectancy = sum(net_values) / len(net_values) if net_values else None
    profit_factor = sum(wins) / abs(sum(losses)) if losses else (None if not wins else 999.0)
    equity = peak = 1.0
    max_drawdown = 0.0
    for value in net_values:
        equity *= max(0.0, 1.0 + value / 100.0)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak * 100.0)
    coverage_days = len({item["trade_date"] for item in rows})
    segment_count = min(5, len(net_values))
    positive_segments = 0
    if segment_count:
        for index in range(segment_count):
            start = index * len(net_values) // segment_count
            end = (index + 1) * len(net_values) // segment_count
            if sum(net_values[start:end]) > 0:
                positive_segments += 1
    net_profit = sum(net_values)
    top5_contribution = None
    if net_profit > 0:
        top5_contribution = sum(sorted(wins, reverse=True)[:5]) / net_profit * 100
    return {
        "window_days": window_days,
        "completed_trades": len(values),
        "coverage_days": coverage_days,
        "win_rate_pct": round(win_rate, 4) if win_rate is not None else None,
        "average_win_pct": round(avg_win, 4) if avg_win is not None else None,
        "average_loss_pct": round(avg_loss, 4) if avg_loss is not None else None,
        "payoff_ratio": round(payoff, 4) if payoff is not None else None,
        "gross_expectancy_pct": round(gross_expectancy, 4) if gross_expectancy is not None else None,
        "estimated_cost_pct": round(effective_cost_pct, 4),
        "net_expectancy_pct": round(net_expectancy, 4) if net_expectancy is not None else None,
        "profit_factor": round(min(profit_factor, 999.0), 4) if profit_factor is not None else None,
        "max_drawdown_pct": round(max_drawdown, 4),
        "walk_forward_segments": segment_count,
        "positive_segments": positive_segments,
        "walk_forward_verified": bool(walk_forward_verified),
        "version_bound_evidence": bool(version_bound_evidence),
        "independent_oos": bool(independent_oos),
        "evidence_as_of_date": rows[-1]["trade_date"] if rows else None,
        "evidence_revision_at": (
            rows[-1]["evidence_revision_at"] if rows else None
        ),
        "drawdown_basis": "sequential_trade_diagnostic",
        "cost_basis": (
            "actual_ledger_fees"
            if rows and all(item.get("actual_cost_bound") for item in rows)
            else "fixed_round_trip_estimate"
        ),
        "evidence_hash": _digest({
            "records": rows,
            "effective_cost_pct": effective_cost_pct,
            "walk_forward_verified": bool(walk_forward_verified),
            "version_bound_evidence": bool(version_bound_evidence),
            "independent_oos": bool(independent_oos),
        }),
        "cost_stress_expectancy_pct": round(gross_expectancy - effective_cost_pct * PROFIT_GATE_POLICY["cost_stress_multiple"], 4) if gross_expectancy is not None else None,
        "top5_profit_contribution_pct": round(top5_contribution, 4) if top5_contribution is not None else None,
        "market_match_score": round(market_match_score, 4) if market_match_score is not None else None,
        "source": "derived_forward_records",
    }


def _gate_result(
    checks: list[tuple[str, bool, str]], *, passed_reason: str,
) -> dict[str, Any]:
    details = [
        {"name": name, "passed": bool(passed), "requirement": requirement}
        for name, passed, requirement in checks
    ]
    failed = [item["name"] for item in details if not item["passed"]]
    return {
        "passed": not failed,
        "checks": details,
        "failed_checks": failed,
        "reason": passed_reason if not failed else "未通过：" + "、".join(failed),
    }


def _funding_evidence_gate_checks(
    metrics: dict[str, Any], *, minimum_completed_trades: int,
    minimum_portfolio_coverage_days: int,
    minimum_selection_completed_trades: int,
    minimum_selection_coverage_days: int,
) -> list[tuple[str, bool, str]]:
    evidence_protocol = str(
        metrics.get("evidence_protocol") or ""
    ).strip().upper()
    artifact_hash = str(metrics.get("artifact_hash") or "").strip().lower()
    source_dataset_hash = str(
        metrics.get("source_dataset_hash") or ""
    ).strip().lower()
    internal_ledger_hash = str(
        metrics.get("internal_ledger_hash") or ""
    ).strip().lower()
    revision_at = _normalize_evidence_revision(
        metrics.get("evidence_revision_at")
    )
    session_window_hash = str(
        metrics.get("session_window_hash") or ""
    ).strip().lower()
    return [
        ("版本证据绑定", metrics.get("version_bound_evidence") is True, "证据必须绑定当前不可变版本"),
        ("独立样本外证据", metrics.get("independent_oos") is True, "不得使用回测或训练样本"),
        ("验证协议", evidence_protocol in _VERIFIED_WALK_FORWARD_PROTOCOLS, "必须使用受支持的时序隔离Walk-Forward协议"),
        ("验证产物", bool(revision_at) and bool(_HASH_PATTERN.fullmatch(artifact_hash)) and bool(_HASH_PATTERN.fullmatch(source_dataset_hash)), "必须保留可重算产物、底层样本集SHA-256和证据高水位"),
        ("不可伪造来源", metrics.get("funding_provenance") == "INTERNAL_PORTFOLIO_LEDGER_V1" and bool(_HASH_PATTERN.fullmatch(internal_ledger_hash)), "资金证据必须由内部版本绑定组合账本生成并保留账本哈希；外部上传只用于研究"),
        ("独立复核", metrics.get("verification_status") == "CONFIRMED" and bool(metrics.get("reviewed_by")) and str(metrics.get("reviewed_by")) != str(metrics.get("submitted_by") or "") and bool(metrics.get("reviewed_at")) and metrics.get("review_audit_valid") is True, "提交者与复核者分离、证据已确认，且复核审计绑定有效"),
        ("组合权益回撤", metrics.get("drawdown_basis") == "internal_version_bound_portfolio_equity", "最大回撤必须基于内部逐日盯市、含重叠持仓和现金的版本绑定组合权益"),
        ("成本口径", metrics.get("cost_basis") == "actual_ledger_fees", "资金门槛只接受内部成交账本实际费用"),
        ("精确交易日窗口", metrics.get("session_window_valid") is True and _int(metrics.get("session_window_count")) == _int(metrics.get("window_days")) and bool(_HASH_PATTERN.fullmatch(session_window_hash)), "20/60/120窗口必须来自权威交易日历的精确已收盘交易日序列并保留哈希"),
        ("证据新鲜度", metrics.get("evidence_fresh") is True, f"证据日期距治理日不超过{PROFIT_GATE_POLICY['maximum_evidence_age_days']}日"),
        ("选择验证新鲜度", metrics.get("selection_validation_fresh") is True, f"版本选择Walk-Forward产物距治理日不超过{PROFIT_GATE_POLICY['maximum_evidence_age_days']}日"),
        ("成熟交易", _int(metrics.get("completed_trades")) >= minimum_completed_trades, f"至少{minimum_completed_trades}笔"),
        ("组合净值覆盖", _int(metrics.get("portfolio_coverage_days")) >= minimum_portfolio_coverage_days, f"内部逐日组合净值至少覆盖{minimum_portfolio_coverage_days}个权威交易日；不要求每个交易日都有平仓"),
        ("Walk-Forward", metrics.get("walk_forward_verified") is True and metrics.get("selection_validation_independent_oos") is True and metrics.get("selection_validation_scope") == "VERSION_SELECTION_ONLY" and _int(metrics.get("selection_validation_completed_trades")) >= minimum_selection_completed_trades and _int(metrics.get("selection_validation_coverage_days")) >= minimum_selection_coverage_days and _int(metrics.get("walk_forward_segments")) >= PROFIT_GATE_POLICY["walk_forward_segments"] and _int(metrics.get("positive_segments")) >= PROFIT_GATE_POLICY["minimum_positive_segments"], f"版本选择经独立时序隔离验证，至少{minimum_selection_completed_trades}笔、覆盖{minimum_selection_coverage_days}个权威交易日，且5段Walk-Forward至少4段为正"),
    ]


def evaluate_profit_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    cost = _num(metrics.get("estimated_cost_pct"), DEFAULT_ROUND_TRIP_COST_PCT) or DEFAULT_ROUND_TRIP_COST_PCT
    max_drawdown = _num(metrics.get("max_drawdown_pct"), None)
    top5_contribution = _num(metrics.get("top5_profit_contribution_pct"), None)
    checks = _funding_evidence_gate_checks(
        metrics,
        minimum_completed_trades=PROFIT_GATE_POLICY[
            "minimum_completed_trades"
        ],
        minimum_portfolio_coverage_days=PROFIT_GATE_POLICY[
            "minimum_coverage_days"
        ],
        minimum_selection_completed_trades=PROFIT_GATE_POLICY[
            "minimum_completed_trades"
        ],
        minimum_selection_coverage_days=PROFIT_GATE_POLICY[
            "minimum_coverage_days"
        ],
    )
    checks.extend([
        ("扣费后净期望", (_num(metrics.get("net_expectancy_pct"), -999.0) or -999.0) > PROFIT_GATE_POLICY["minimum_net_expectancy_pct"] and (_num(metrics.get("gross_expectancy_pct"), -999.0) or -999.0) >= cost * PROFIT_GATE_POLICY["minimum_cost_safety_multiple"], "为正且总期望覆盖3倍成本"),
        ("盈亏比", (_num(metrics.get("payoff_ratio"), -1.0) or -1.0) >= PROFIT_GATE_POLICY["minimum_payoff_ratio"], f"不低于{PROFIT_GATE_POLICY['minimum_payoff_ratio']:.2f}"),
        ("利润因子", (_num(metrics.get("profit_factor"), -1.0) or -1.0) >= PROFIT_GATE_POLICY["minimum_profit_factor"], f"不低于{PROFIT_GATE_POLICY['minimum_profit_factor']:.2f}"),
        ("最大回撤", max_drawdown is not None and max_drawdown <= PROFIT_GATE_POLICY["maximum_drawdown_pct"], f"不高于{PROFIT_GATE_POLICY['maximum_drawdown_pct']:.0f}%"),
        ("成本压力", (_num(metrics.get("cost_stress_expectancy_pct"), -999.0) or -999.0) > 0, "1.5倍成本仍为正"),
        ("极端依赖", top5_contribution is not None and top5_contribution <= PROFIT_GATE_POLICY["maximum_top5_profit_contribution_pct"], "最好5笔贡献不超过70%"),
    ])
    return _gate_result(checks, passed_reason="全部盈利硬门槛通过")


def evaluate_decay_gate_20(metrics: dict[str, Any]) -> dict[str, Any]:
    """Canonical 20-session decay gate; it never grants a standalone fund."""

    gross_expectancy = _num(metrics.get("gross_expectancy_pct"), None)
    actual_cost = _num(metrics.get("estimated_cost_pct"), None)
    stored_stress = _num(metrics.get("cost_stress_expectancy_pct"), None)
    expected_stress = (
        round(
            gross_expectancy
            - actual_cost * DECAY_GATE_20_POLICY["cost_stress_multiple"],
            4,
        )
        if gross_expectancy is not None and actual_cost is not None
        else None
    )
    checks = [
        (
            "20日窗口",
            _int(metrics.get("window_days"), -1)
            == DECAY_GATE_20_POLICY["window_days"],
            "衰减门槛只能用于20个权威交易日窗口",
        ),
        *_funding_evidence_gate_checks(
            metrics,
            minimum_completed_trades=DECAY_GATE_20_POLICY[
                "minimum_completed_trades"
            ],
            minimum_portfolio_coverage_days=DECAY_GATE_20_POLICY[
                "minimum_portfolio_coverage_days"
            ],
            minimum_selection_completed_trades=DECAY_GATE_20_POLICY[
                "minimum_selection_completed_trades"
            ],
            minimum_selection_coverage_days=DECAY_GATE_20_POLICY[
                "minimum_selection_coverage_days"
            ],
        ),
        (
            "扣费后净期望",
            (_num(metrics.get("net_expectancy_pct"), None) is not None)
            and _num(metrics.get("net_expectancy_pct"), None)
            > DECAY_GATE_20_POLICY["minimum_net_expectancy_pct"],
            "20日扣除真实费用后的净期望必须严格为正",
        ),
        (
            "利润因子",
            (_num(metrics.get("profit_factor"), None) is not None)
            and _num(metrics.get("profit_factor"), None)
            > DECAY_GATE_20_POLICY["minimum_profit_factor_exclusive"],
            "20日利润因子必须严格大于1.00",
        ),
        (
            "成本压力",
            expected_stress is not None
            and stored_stress is not None
            and stored_stress == expected_stress
            and expected_stress > 0,
            "毛期望减去1.5倍内部账本真实成本后按4位小数重算必须严格为正且完全一致",
        ),
    ]
    return _gate_result(checks, passed_reason="20日衰减门槛通过")


def evaluate_window_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    """The only dispatcher for persisted 20/60/120 window gates."""

    window_days = _int(metrics.get("window_days"), -1)
    if window_days == DECAY_GATE_20_POLICY["window_days"]:
        return evaluate_decay_gate_20(metrics)
    if window_days in {60, 120}:
        return evaluate_profit_gate(metrics)
    return _gate_result(
        [("窗口", False, "盈利门槛窗口只能是20、60或120个权威交易日")],
        passed_reason="窗口盈利门槛通过",
    )


def calculate_health_score(metrics: dict[str, Any]) -> float:
    net = _num(metrics.get("net_expectancy_pct"), None)
    pf = _num(metrics.get("profit_factor"), None)
    payoff = _num(metrics.get("payoff_ratio"), None)
    drawdown = _num(metrics.get("max_drawdown_pct"), None)
    sample = _int(metrics.get("completed_trades"))
    market_match = _num(metrics.get("market_match_score"), None)
    cost_stress = _num(metrics.get("cost_stress_expectancy_pct"), None)
    parts = {
        "net_expectancy": 0.0 if net is None else max(0.0, min(1.0, (net + 0.25) / 1.25)),
        "profit_factor": 0.0 if pf is None else max(0.0, min(1.0, (pf - 0.75) / 0.75)),
        "sample_reliability": max(0.0, min(1.0, sample / PROFIT_GATE_POLICY["minimum_completed_trades"])),
        "payoff_ratio": 0.0 if payoff is None else max(0.0, min(1.0, payoff / PROFIT_GATE_POLICY["target_payoff_ratio"])),
        "drawdown": 0.0 if drawdown is None else max(0.0, min(1.0, 1.0 - drawdown / (PROFIT_GATE_POLICY["maximum_drawdown_pct"] * 1.5))),
        "market_cost_stability": 0.0 if market_match is None or cost_stress is None else max(0.0, min(1.0, market_match / 100.0)) * (1.0 if cost_stress > 0 else 0.35),
    }
    return round(sum(parts[key] * HEALTH_SCORE_WEIGHTS[key] for key in parts), 2)


def recommend_lifecycle_status(current_status: str, metrics: dict[str, Any]) -> tuple[str, str]:
    current = current_status if current_status in LIFECYCLE_LABELS else "SHADOW"
    if current == "RETIRED":
        return current, "已淘汰版本保持终态；只能注册新版本重新验证"
    gate = evaluate_profit_gate(metrics)
    health = calculate_health_score(metrics)
    sample = _int(metrics.get("completed_trades"))
    net = _num(metrics.get("net_expectancy_pct"), None)
    pf = _num(metrics.get("profit_factor"), None)
    if current == "SUSPENDED":
        recovery_ready = (
            sample >= 20
            and (net if net is not None else -999.0) > 0
            and (pf if pf is not None else -999.0) >= 1.10
        )
        if gate["passed"] or recovery_ready:
            return "SHADOW", "恢复条件初步满足，先返回影子观察重新积累独立证据"
        return "SUSPENDED", f"恢复条件尚未满足；{gate['reason']}"
    if gate["passed"]:
        return ("ACTIVE", f"盈利硬门槛全部通过，健康分{health:.1f}") if health >= 80 else ("REDUCE", f"盈利硬门槛通过但健康分仅{health:.1f}，降低模拟权重")
    if sample >= 20 and ((net is not None and net <= 0) or (pf is not None and pf < 1.0)):
        return "SUSPENDED", f"近期证据显示扣费后期望或利润因子失效；{gate['reason']}"
    return "SHADOW", gate["reason"]


def _load_metric_inputs(
    as_of_date: str, *, entity_type: str = "STRATEGY",
    current_versions: dict[str, str] | None = None,
) -> dict[tuple[str, int], dict[str, Any]]:
    if not _table_exists("st_strategy_metric_input"):
        return {}
    entity_type = str(entity_type or "").upper()
    if entity_type == "STRATEGY":
        current_join = (
            "INNER JOIN st_strategy_registry current_entity "
            "ON current_entity.strategy_key=i.strategy_key "
            "AND BINARY current_entity.current_version="
            "BINARY i.strategy_version"
        )
    elif entity_type == "COMBINATION":
        current_join = (
            "INNER JOIN st_strategy_combination current_entity "
            "ON current_entity.combination_key=i.strategy_key "
            "AND BINARY current_entity.current_version="
            "BINARY i.strategy_version"
        )
    else:
        raise ValueError("指标证据对象只能是策略或组合")
    rows = _db_read(
        f"""
        SELECT i.evidence_id, i.entity_type, i.strategy_key,
               i.strategy_version, i.as_of_date,
               i.window_days, i.metrics_json, i.source, i.evidence_hash,
               i.evidence_protocol, i.artifact_hash,
               i.source_dataset_hash,
               i.evidence_revision_at, i.verification_status,
               i.funding_provenance,
               i.submitted_by, i.reviewed_by, i.reviewed_at, i.created_at
        FROM st_strategy_metric_input i
        {current_join}
        WHERE i.as_of_date <= :as_of_date AND i.entity_type = :entity_type
          AND i.verification_status='CONFIRMED'
        ORDER BY i.evidence_revision_at DESC, i.as_of_date DESC,
                 i.created_at DESC, i.evidence_id DESC
        """,
        {"as_of_date": as_of_date, "entity_type": entity_type},
    )
    audit_rows = _metric_evidence_audit_rows(entity_type)
    result = {}
    for row in rows:
        review_audit_valid, _audit_detail = metric_evidence_audit_binding(
            row, audit_rows,
        )
        if not review_audit_valid:
            continue
        key = str(row.get("strategy_key") or "")
        version = str(row.get("strategy_version") or "")
        if current_versions is not None and current_versions.get(key) != version:
            continue
        result_key = (key, _int(row.get("window_days")))
        if result_key in result:
            continue
        metrics = _json(row.get("metrics_json"), {})
        metrics.update({
            "source": row.get("source"),
            "evidence_hash": row.get("evidence_hash"),
            "as_of_date": row.get("as_of_date"),
            "evidence_protocol": row.get("evidence_protocol"),
            "artifact_hash": row.get("artifact_hash"),
            "source_dataset_hash": row.get("source_dataset_hash"),
            "evidence_revision_at": row.get("evidence_revision_at"),
            "verification_status": row.get("verification_status"),
            "funding_provenance": row.get("funding_provenance"),
            "submitted_by": row.get("submitted_by"),
            "reviewed_by": row.get("reviewed_by"),
            "reviewed_at": row.get("reviewed_at"),
            "review_audit_valid": True,
        })
        result[result_key] = metrics
    return result


def _load_forward_records(
    as_of_date: str, registry: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    current_versions = {
        str(row["strategy_key"]): str(row["current_version"])
        for row in registry
    }
    version_frozen_at = {
        str(row["strategy_key"]): _normalize_evidence_revision(
            row.get("version_created_at")
        )
        for row in registry
    }
    if (
        _strict_table_exists("st_forward_trade_evidence_v3")
        and _strict_table_exists("st_forward_exit_allocation_v3")
    ):
        columns = _strict_table_columns("st_forward_trade_evidence_v3")
        allocation_columns = _strict_table_columns(
            "st_forward_exit_allocation_v3"
        )
        required = {
            "evidence_id", "strategy_key", "strategy_version", "entry_at",
            "entry_trade_date", "exit_at",
            "realized_net_return_pct", "evidence_status", "evidence_kind",
            "protocol_version", "sample_owner_role", "attribution_status",
            "source_run_uid", "source_forecast_id", "source_intent_id",
            "entry_order_id", "entry_fill_id",
            "entry_gross_cny", "entry_fee_cny",
            "exit_fee_cny", "account_id", "stock_code", "entry_quantity",
            "closed_quantity", "entry_price", "exit_average_price",
            "exit_gross_cny",
        }
        allocation_required = {
            "allocation_id", "evidence_id", "attribution_status",
            "account_id", "stock_code", "entry_fill_id", "exit_fill_id",
            "exit_order_id", "allocation_sequence",
            "allocated_quantity", "allocated_gross_cny",
            "allocated_fee_cny", "exit_filled_at",
            "allocation_protocol_version",
        }
        if (
            required.issubset(columns)
            and allocation_required.issubset(allocation_columns)
        ):
            rows = _db_read(
                """
                SELECT e.evidence_id, e.account_id, e.stock_code,
                       e.source_run_uid, e.source_forecast_id,
                       e.source_intent_id, e.entry_order_id,
                       e.entry_fill_id,
                       e.strategy_key, e.entry_at, e.entry_trade_date,
                       e.entry_quantity, e.closed_quantity, e.entry_price,
                       e.exit_average_price, e.exit_at,
                       DATE(exit_at) AS trade_date,
                       e.realized_net_return_pct AS return_pct,
                       e.entry_gross_cny, e.entry_fee_cny,
                       e.exit_gross_cny, e.exit_fee_cny,
                       e.evidence_status,
                       intent_buy.buy_fill_count
                           AS source_intent_buy_fill_count,
                       intent_buy.entry_quantity
                           AS source_intent_entry_quantity,
                       intent_buy.entry_gross_cny
                           AS source_intent_entry_gross_cny,
                       intent_buy.entry_fee_cny
                           AS source_intent_entry_fee_cny,
                       e.exit_fill_ids_json,
                       e.exit_order_ids_json,
                       (SELECT COUNT(*) FROM st_cash_ledger_v2 entry_cash
                         JOIN st_fill_v2 entry_fill
                           ON entry_fill.fill_id=
                              entry_cash.related_fill_id
                          AND entry_fill.order_id=
                              entry_cash.related_order_id
                          AND entry_fill.account_id=
                              entry_cash.account_id
                         JOIN st_order_v2 entry_order_truth
                           ON entry_order_truth.order_id=entry_fill.order_id
                          AND entry_order_truth.intent_id=e.source_intent_id
                          AND entry_order_truth.account_id=e.account_id
                          AND entry_order_truth.stock_code=e.stock_code
                          AND entry_order_truth.side='BUY'
                         JOIN st_cash_event_binding_v2 entry_binding
                           ON entry_binding.cash_event_id=
                              entry_cash.cash_event_id
                          AND entry_binding.account_id=
                              entry_cash.account_id
                          AND entry_binding.cash_event_type=
                              entry_cash.event_type
                          AND entry_binding.related_order_id=
                              entry_cash.related_order_id
                          AND entry_binding.related_fill_id=
                              entry_cash.related_fill_id
                          AND entry_binding.occurred_at=entry_fill.filled_at
                          AND entry_binding.history_origin=
                              'COMPLETE_FROM_DECLARED_ORIGIN'
                          AND entry_binding.authority_status=
                              'CONTENT_HASH_ONLY'
                          AND JSON_VALID(
                              entry_binding.cash_event_payload_json
                          )
                          AND BINARY entry_binding.cash_event_payload_hash=
                              BINARY SHA2(CONCAT(
                                  '{"namespace":"trading-v2.canonical-json.v1",',
                                  '"payload":{"value":',
                                  entry_binding.cash_event_payload_json,
                                  '}}'
                              ), 256)
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.cash_event_id'
                          ))=BINARY entry_cash.cash_event_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.account_id'
                          ))=BINARY entry_cash.account_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.business_event_key'
                          ))=BINARY entry_cash.business_event_key
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.event_type'
                          ))=BINARY entry_cash.event_type
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.related_order_id'
                          ))=BINARY entry_cash.related_order_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.related_fill_id'
                          ))=BINARY entry_cash.related_fill_id
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.amount'
                          )) AS DECIMAL(20,2))=entry_cash.amount
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_binding.cash_event_payload_json,
                              '$.balance_after'
                          )) AS DECIMAL(20,2))=entry_cash.balance_after
                          AND entry_binding.fill_execution_evidence_id
                              IS NOT NULL
                          AND entry_binding.fill_execution_evidence_hash
                              IS NOT NULL
                         JOIN st_fill_execution_evidence_v2 entry_execution
                           ON entry_execution.fill_execution_evidence_id=
                              entry_binding.fill_execution_evidence_id
                          AND entry_execution.evidence_hash=
                              entry_binding.fill_execution_evidence_hash
                          AND entry_execution.fill_id=entry_fill.fill_id
                          AND entry_execution.order_id=entry_fill.order_id
                          AND entry_execution.account_id=
                              entry_fill.account_id
                          AND entry_execution.stock_code=
                              entry_fill.stock_code
                          AND entry_execution.executed_at=
                              entry_fill.filled_at
                          AND JSON_VALID(entry_execution.fill_payload_json)
                          AND BINARY entry_execution.fill_payload_hash=
                              BINARY SHA2(CONCAT(
                                  '{"namespace":"trading-v2.canonical-json.v1",',
                                  '"payload":{"value":',
                                  entry_execution.fill_payload_json,
                                  '}}'
                              ), 256)
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.fill_id'
                          ))=BINARY entry_fill.fill_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.order_id'
                          ))=BINARY entry_fill.order_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.account_id'
                          ))=BINARY entry_fill.account_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.stock_code'
                          ))=BINARY entry_fill.stock_code
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.side'
                          ))=BINARY entry_fill.side
                          AND CAST(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.quantity'
                          ) AS UNSIGNED)=entry_fill.quantity
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json, '$.price'
                          )) AS DECIMAL(20,6))=entry_fill.price
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.gross_amount'
                          )) AS DECIMAL(20,2))=entry_fill.gross_amount
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.fee_amount'
                          )) AS DECIMAL(20,2))=entry_fill.fee_amount
                          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.net_cash_amount'
                          )) AS DECIMAL(20,2))=entry_fill.net_cash_amount
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.quote_event_id'
                          ))=BINARY entry_fill.quote_event_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.match_event_id'
                          ))=BINARY entry_fill.match_event_id
                          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                              entry_execution.fill_payload_json,
                              '$.idempotency_key'
                          ))=BINARY entry_fill.idempotency_key
                         WHERE entry_cash.account_id=e.account_id
                           AND entry_cash.related_order_id=e.entry_order_id
                           AND entry_cash.related_fill_id=e.entry_fill_id
                           AND entry_cash.event_type='BUY_FILL'
                           AND entry_fill.stock_code=e.stock_code
                           AND entry_fill.side='BUY'
                           AND entry_cash.amount=entry_fill.net_cash_amount
                           AND entry_cash.occurred_at=entry_fill.filled_at)
                           AS entry_cash_binding_count,
                       JSON_LENGTH(IF(
                           JSON_VALID(e.exit_fill_ids_json),
                           e.exit_fill_ids_json, JSON_ARRAY()
                       )) AS exit_fill_id_count,
                       JSON_LENGTH(IF(
                           JSON_VALID(e.exit_order_ids_json),
                           e.exit_order_ids_json, JSON_ARRAY()
                       )) AS exit_order_id_count,
                       COALESCE(
                           exit_alloc.exit_allocation_count, 0
                       ) AS exit_allocation_count,
                       COALESCE(
                           exit_alloc.exit_allocated_fill_count, 0
                       ) AS exit_allocated_fill_count,
                       COALESCE(
                           exit_alloc.exit_allocated_order_count, 0
                       ) AS exit_allocated_order_count,
                       COALESCE(
                           exit_alloc.exit_fill_binding_count, 0
                       ) AS exit_fill_binding_count,
                       COALESCE(
                           exit_alloc.exit_order_binding_count, 0
                       ) AS exit_order_binding_count,
                       COALESCE(
                           exit_alloc.exit_cash_binding_count, 0
                       ) AS exit_cash_binding_count,
                       COALESCE(
                           exit_alloc.exit_global_conservation_count, 0
                       ) AS exit_global_conservation_count,
                       COALESCE(
                           exit_alloc.exit_allocation_protocol_count, 0
                       ) AS exit_allocation_protocol_count,
                       COALESCE(
                           exit_alloc.exit_fill_trade_day_count, 0
                       ) AS exit_fill_trade_day_count,
                       exit_alloc.exit_fill_latest_at,
                       COALESCE(
                           exit_alloc.exit_fill_quantity_sum, 0
                       ) AS exit_fill_quantity_sum,
                       COALESCE(
                           exit_alloc.exit_fill_gross_sum, 0
                       ) AS exit_fill_gross_sum,
                       COALESCE(
                           exit_alloc.exit_fill_fee_sum, 0
                       ) AS exit_fill_fee_sum,
                       e.strategy_version AS bound_strategy_version
                FROM st_forward_trade_evidence_v3 e
                INNER JOIN st_strategy_registry current_strategy
                  ON current_strategy.strategy_key=e.strategy_key
                 AND BINARY current_strategy.current_version=
                     BINARY e.strategy_version
                INNER JOIN st_trade_intent_v2 source_intent
                  ON source_intent.intent_id=e.source_intent_id
                 AND source_intent.account_id=e.account_id
                 AND source_intent.stock_code=e.stock_code
                 AND source_intent.decision_run_uid=e.source_run_uid
                 AND source_intent.action='BUY'
                 AND source_intent.reason_code IN (
                     'V3_PAPER_DISCOVERY', 'V3_VALIDATED_POSITIVE'
                 )
                INNER JOIN (
                    SELECT buy_order.intent_id, raw_buy.account_id,
                           raw_buy.stock_code,
                           COUNT(DISTINCT raw_buy.fill_id) AS buy_fill_count,
                           SUM(raw_buy.quantity) AS entry_quantity,
                           SUM(raw_buy.gross_amount) AS entry_gross_cny,
                           SUM(raw_buy.fee_amount) AS entry_fee_cny
                    FROM st_fill_v2 raw_buy
                    JOIN st_order_v2 buy_order
                      ON buy_order.order_id=raw_buy.order_id
                     AND buy_order.account_id=raw_buy.account_id
                     AND buy_order.stock_code=raw_buy.stock_code
                     AND buy_order.side='BUY'
                    WHERE raw_buy.side='BUY'
                    GROUP BY buy_order.intent_id, raw_buy.account_id,
                             raw_buy.stock_code
                ) intent_buy
                  ON intent_buy.intent_id=e.source_intent_id
                 AND intent_buy.account_id=e.account_id
                 AND intent_buy.stock_code=e.stock_code
                LEFT JOIN (
                    SELECT detail.evidence_id,
                           COUNT(*) AS exit_allocation_count,
                           COUNT(DISTINCT detail.exit_fill_id)
                               AS exit_allocated_fill_count,
                           COUNT(DISTINCT detail.exit_order_id)
                               AS exit_allocated_order_count,
                           COUNT(DISTINCT CASE
                               WHEN detail.fill_binding_valid=1
                               THEN detail.exit_fill_id END
                           ) AS exit_fill_binding_count,
                           COUNT(DISTINCT CASE
                               WHEN detail.fill_binding_valid=1
                               THEN detail.exit_order_id END
                           ) AS exit_order_binding_count,
                           SUM(CASE
                               WHEN detail.fill_binding_valid=1
                                AND detail.cash_binding_count=1
                               THEN 1 ELSE 0 END
                           ) AS exit_cash_binding_count,
                           SUM(detail.global_conservation_valid)
                               AS exit_global_conservation_count,
                           SUM(CASE
                               WHEN detail.allocation_protocol_version=
                                    'PAPER_FIFO_EXIT_ALLOCATION_V1'
                               THEN 1 ELSE 0 END
                           ) AS exit_allocation_protocol_count,
                           COUNT(DISTINCT DATE(detail.exit_filled_at))
                               AS exit_fill_trade_day_count,
                           MAX(detail.exit_filled_at)
                               AS exit_fill_latest_at,
                           SUM(detail.allocated_quantity)
                               AS exit_fill_quantity_sum,
                           SUM(detail.allocated_gross_cny)
                               AS exit_fill_gross_sum,
                           SUM(detail.allocated_fee_cny)
                               AS exit_fill_fee_sum
                    FROM (
                        SELECT allocation.evidence_id,
                               allocation.exit_fill_id,
                               allocation.exit_order_id,
                               allocation.allocated_quantity,
                               allocation.allocated_gross_cny,
                               allocation.allocated_fee_cny,
                               allocation.exit_filled_at,
                               allocation.allocation_protocol_version,
                               COALESCE(sell_truth.valid_binding_count, 0)
                                   AS cash_binding_count,
                               CASE WHEN
                                   allocation.account_id=parent.account_id
                                   AND allocation.stock_code=parent.stock_code
                                   AND allocation.entry_fill_id=
                                       parent.entry_fill_id
                                   AND allocation.attribution_status=
                                       'ATTRIBUTED'
                                   AND allocation.evidence_id IS NOT NULL
                                   AND allocation.allocation_protocol_version=
                                       'PAPER_FIFO_EXIT_ALLOCATION_V1'
                                   AND BINARY allocation.allocation_id=
                                       BINARY SHA2(CONCAT(
                                           allocation.exit_fill_id, '|',
                                           allocation.allocation_sequence, '|',
                                           allocation.entry_fill_id, '|',
                                           'PAPER_FIFO_EXIT_ALLOCATION_V1'
                                       ), 256)
                                   AND raw_sell.fill_id IS NOT NULL
                                   AND raw_sell.account_id=
                                       allocation.account_id
                                   AND raw_sell.stock_code=
                                       allocation.stock_code
                                   AND raw_sell.side='SELL'
                                   AND raw_sell.order_id=
                                       allocation.exit_order_id
                                   AND raw_sell.filled_at=
                                       allocation.exit_filled_at
                                   AND raw_sell.quantity > 0
                                   AND allocation.allocated_quantity > 0
                                   AND allocation.allocated_quantity <=
                                       raw_sell.quantity
                                   AND global_allocation.valid_row_count=
                                       global_allocation.allocation_row_count
                                   AND JSON_VALID(parent.exit_fill_ids_json)
                                   AND JSON_CONTAINS(
                                       parent.exit_fill_ids_json,
                                       JSON_QUOTE(allocation.exit_fill_id)
                                   )=1
                                   AND JSON_VALID(parent.exit_order_ids_json)
                                   AND JSON_CONTAINS(
                                       parent.exit_order_ids_json,
                                       JSON_QUOTE(allocation.exit_order_id)
                                   )=1
                                   THEN 1 ELSE 0
                               END AS fill_binding_valid,
                               CASE WHEN
                                   raw_sell.fill_id IS NOT NULL
                                   AND global_allocation.allocation_row_count > 0
                                   AND global_allocation.distinct_sequence_count=
                                       global_allocation.allocation_row_count
                                   AND global_allocation.minimum_sequence=0
                                   AND global_allocation.maximum_sequence=
                                       global_allocation.allocation_row_count - 1
                                   AND global_allocation.allocated_quantity=
                                       raw_sell.quantity
                                   AND global_allocation.allocated_gross_cny=
                                       CAST(raw_sell.gross_amount
                                           AS DECIMAL(20,6))
                                   AND global_allocation.allocated_fee_cny=
                                       CAST(raw_sell.fee_amount
                                           AS DECIMAL(20,6))
                                   AND global_allocation.valid_row_count=
                                       global_allocation.allocation_row_count
                                   THEN 1 ELSE 0
                               END AS global_conservation_valid
                        FROM st_forward_exit_allocation_v3 allocation
                        JOIN st_forward_trade_evidence_v3 parent
                          ON parent.evidence_id=allocation.evidence_id
                        LEFT JOIN st_fill_v2 raw_sell
                          ON raw_sell.fill_id=allocation.exit_fill_id
                        LEFT JOIN (
                            SELECT totals.exit_fill_id,
                                   totals.allocation_row_count,
                                   totals.distinct_sequence_count,
                                   totals.minimum_sequence,
                                   totals.maximum_sequence,
                                   totals.allocated_quantity,
                                   totals.allocated_gross_cny,
                                   totals.allocated_fee_cny,
                                   SUM(CASE WHEN
                                       raw_member_fill.fill_id IS NOT NULL
                                       AND raw_member_fill.side='SELL'
                                       AND raw_member_fill.account_id=
                                           member.account_id
                                       AND raw_member_fill.stock_code=
                                           member.stock_code
                                       AND raw_member_fill.order_id=
                                           member.exit_order_id
                                       AND raw_member_fill.filled_at=
                                           member.exit_filled_at
                                       AND raw_member_fill.quantity > 0
                                       AND raw_member_fill.gross_amount >= 0
                                       AND raw_member_fill.fee_amount >= 0
                                       AND raw_entry_fill.fill_id IS NOT NULL
                                       AND raw_entry_fill.side='BUY'
                                       AND raw_entry_fill.account_id=
                                           member.account_id
                                       AND raw_entry_fill.stock_code=
                                           member.stock_code
                                       AND raw_entry_fill.filled_at <=
                                           member.exit_filled_at
                                       AND member.allocation_sequence >= 0
                                       AND member.allocated_quantity > 0
                                       AND member.allocated_quantity <=
                                           raw_member_fill.quantity
                                       AND member.allocated_gross_cny >= 0
                                       AND member.allocated_fee_cny >= 0
                                       AND member.allocation_protocol_version=
                                           'PAPER_FIFO_EXIT_ALLOCATION_V1'
                                       AND BINARY member.allocation_id=
                                           BINARY SHA2(CONCAT(
                                               member.exit_fill_id, '|',
                                               member.allocation_sequence, '|',
                                               member.entry_fill_id, '|',
                                               'PAPER_FIFO_EXIT_ALLOCATION_V1'
                                           ), 256)
                                       AND (
                                           (
                                               member.attribution_status=
                                                   'ATTRIBUTED'
                                               AND member.evidence_id IS NOT NULL
                                               AND member_parent.evidence_id
                                                   IS NOT NULL
                                               AND member_parent.account_id=
                                                   member.account_id
                                               AND member_parent.stock_code=
                                                   member.stock_code
                                               AND member_parent.entry_fill_id=
                                                   member.entry_fill_id
                                               AND JSON_VALID(
                                                   member_parent.exit_fill_ids_json
                                               )
                                               AND JSON_CONTAINS(
                                                   member_parent.exit_fill_ids_json,
                                                   JSON_QUOTE(member.exit_fill_id)
                                               )=1
                                               AND JSON_VALID(
                                                   member_parent.exit_order_ids_json
                                               )
                                               AND JSON_CONTAINS(
                                                   member_parent.exit_order_ids_json,
                                                   JSON_QUOTE(member.exit_order_id)
                                               )=1
                                           )
                                           OR (
                                               member.attribution_status=
                                                   'UNATTRIBUTED'
                                               AND member.evidence_id IS NULL
                                           )
                                       )
                                       AND (
                                           (
                                               member.allocation_sequence <
                                                   totals.maximum_sequence
                                               AND member.allocated_gross_cny=
                                                   ROUND(
                                                       raw_member_fill.gross_amount *
                                                       member.allocated_quantity /
                                                       raw_member_fill.quantity,
                                                       6
                                                   )
                                               AND member.allocated_fee_cny=
                                                   ROUND(
                                                       raw_member_fill.fee_amount *
                                                       member.allocated_quantity /
                                                       raw_member_fill.quantity,
                                                       6
                                                   )
                                           )
                                           OR member.allocation_sequence=
                                               totals.maximum_sequence
                                       )
                                       THEN 1 ELSE 0 END
                                   ) AS valid_row_count
                            FROM (
                                SELECT exit_fill_id,
                                       COUNT(*) AS allocation_row_count,
                                       COUNT(DISTINCT allocation_sequence)
                                           AS distinct_sequence_count,
                                       MIN(allocation_sequence)
                                           AS minimum_sequence,
                                       MAX(allocation_sequence)
                                           AS maximum_sequence,
                                       SUM(allocated_quantity)
                                           AS allocated_quantity,
                                       SUM(allocated_gross_cny)
                                           AS allocated_gross_cny,
                                       SUM(allocated_fee_cny)
                                           AS allocated_fee_cny
                                FROM st_forward_exit_allocation_v3
                                GROUP BY exit_fill_id
                            ) totals
                            JOIN st_forward_exit_allocation_v3 member
                              ON member.exit_fill_id=totals.exit_fill_id
                            LEFT JOIN st_fill_v2 raw_member_fill
                              ON raw_member_fill.fill_id=member.exit_fill_id
                            LEFT JOIN st_fill_v2 raw_entry_fill
                              ON raw_entry_fill.fill_id=member.entry_fill_id
                            LEFT JOIN st_forward_trade_evidence_v3 member_parent
                              ON member_parent.evidence_id=member.evidence_id
                            GROUP BY totals.exit_fill_id,
                                     totals.allocation_row_count,
                                     totals.distinct_sequence_count,
                                     totals.minimum_sequence,
                                     totals.maximum_sequence,
                                     totals.allocated_quantity,
                                     totals.allocated_gross_cny,
                                     totals.allocated_fee_cny
                        ) global_allocation
                          ON global_allocation.exit_fill_id=
                             allocation.exit_fill_id
                        LEFT JOIN (
                            SELECT sell_cash.account_id,
                                   sell_cash.related_fill_id,
                                   sell_cash.related_order_id,
                                   COUNT(*) AS valid_binding_count
                            FROM st_cash_ledger_v2 sell_cash
                            JOIN st_fill_v2 bound_sell_fill
                              ON bound_sell_fill.fill_id=
                                 sell_cash.related_fill_id
                             AND bound_sell_fill.order_id=
                                 sell_cash.related_order_id
                             AND bound_sell_fill.account_id=
                                 sell_cash.account_id
                             AND bound_sell_fill.side='SELL'
                             AND sell_cash.event_type='SELL_FILL'
                             AND sell_cash.amount=
                                 bound_sell_fill.net_cash_amount
                             AND sell_cash.occurred_at=
                                 bound_sell_fill.filled_at
                            JOIN st_cash_event_binding_v2 sell_binding
                              ON sell_binding.cash_event_id=
                                 sell_cash.cash_event_id
                             AND sell_binding.account_id=
                                 sell_cash.account_id
                             AND sell_binding.cash_event_type=
                                 sell_cash.event_type
                             AND sell_binding.related_order_id=
                                 sell_cash.related_order_id
                             AND sell_binding.related_fill_id=
                                 sell_cash.related_fill_id
                             AND sell_binding.occurred_at=
                                 bound_sell_fill.filled_at
                             AND sell_binding.history_origin=
                                 'COMPLETE_FROM_DECLARED_ORIGIN'
                             AND sell_binding.authority_status=
                                 'CONTENT_HASH_ONLY'
                             AND JSON_VALID(
                                 sell_binding.cash_event_payload_json
                             )
                             AND BINARY sell_binding.cash_event_payload_hash=
                                 BINARY SHA2(CONCAT(
                                     '{"namespace":"trading-v2.canonical-json.v1",',
                                     '"payload":{"value":',
                                     sell_binding.cash_event_payload_json,
                                     '}}'
                                 ), 256)
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.cash_event_id'
                             ))=BINARY sell_cash.cash_event_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.account_id'
                             ))=BINARY sell_cash.account_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.business_event_key'
                             ))=BINARY sell_cash.business_event_key
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.event_type'
                             ))=BINARY sell_cash.event_type
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.related_order_id'
                             ))=BINARY sell_cash.related_order_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.related_fill_id'
                             ))=BINARY sell_cash.related_fill_id
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.amount'
                             )) AS DECIMAL(20,2))=sell_cash.amount
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_binding.cash_event_payload_json,
                                 '$.balance_after'
                             )) AS DECIMAL(20,2))=sell_cash.balance_after
                             AND sell_binding.fill_execution_evidence_id
                                 IS NOT NULL
                             AND sell_binding.fill_execution_evidence_hash
                                 IS NOT NULL
                            JOIN st_fill_execution_evidence_v2 sell_execution
                              ON sell_execution.fill_execution_evidence_id=
                                 sell_binding.fill_execution_evidence_id
                             AND sell_execution.evidence_hash=
                                 sell_binding.fill_execution_evidence_hash
                             AND sell_execution.fill_id=
                                 bound_sell_fill.fill_id
                             AND sell_execution.order_id=
                                 bound_sell_fill.order_id
                             AND sell_execution.account_id=
                                 bound_sell_fill.account_id
                             AND sell_execution.stock_code=
                                 bound_sell_fill.stock_code
                             AND sell_execution.executed_at=
                                 bound_sell_fill.filled_at
                             AND JSON_VALID(
                                 sell_execution.fill_payload_json
                             )
                             AND BINARY sell_execution.fill_payload_hash=
                                 BINARY SHA2(CONCAT(
                                     '{"namespace":"trading-v2.canonical-json.v1",',
                                     '"payload":{"value":',
                                     sell_execution.fill_payload_json,
                                     '}}'
                                 ), 256)
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.fill_id'
                             ))=BINARY bound_sell_fill.fill_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.order_id'
                             ))=BINARY bound_sell_fill.order_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.account_id'
                             ))=BINARY bound_sell_fill.account_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.stock_code'
                             ))=BINARY bound_sell_fill.stock_code
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.side'
                             ))=BINARY bound_sell_fill.side
                             AND CAST(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.quantity'
                             ) AS UNSIGNED)=bound_sell_fill.quantity
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.price'
                             )) AS DECIMAL(20,6))=bound_sell_fill.price
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.gross_amount'
                             )) AS DECIMAL(20,2))=
                                 bound_sell_fill.gross_amount
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.fee_amount'
                             )) AS DECIMAL(20,2))=
                                 bound_sell_fill.fee_amount
                             AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.net_cash_amount'
                             )) AS DECIMAL(20,2))=
                                 bound_sell_fill.net_cash_amount
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.quote_event_id'
                             ))=BINARY bound_sell_fill.quote_event_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.match_event_id'
                             ))=BINARY bound_sell_fill.match_event_id
                             AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                                 sell_execution.fill_payload_json,
                                 '$.idempotency_key'
                             ))=BINARY bound_sell_fill.idempotency_key
                            GROUP BY sell_cash.account_id,
                                     sell_cash.related_fill_id,
                                     sell_cash.related_order_id
                        ) sell_truth
                          ON sell_truth.account_id=allocation.account_id
                         AND sell_truth.related_fill_id=
                             allocation.exit_fill_id
                         AND sell_truth.related_order_id=
                             allocation.exit_order_id
                    ) detail
                    GROUP BY detail.evidence_id
                ) exit_alloc
                  ON exit_alloc.evidence_id=e.evidence_id
                WHERE e.evidence_status IN (
                    'MATURED', 'OPEN', 'PARTIALLY_CLOSED'
                )
                  AND e.evidence_kind='EXECUTED_PAPER'
                  AND e.protocol_version='PAPER_EXECUTED_LEDGER_V1'
                  AND e.sample_owner_role='PRIMARY'
                  AND e.attribution_status IN (
                      'VERIFIED_SNAPSHOT',
                      'LEGACY_VERSION_DERIVED',
                      'LEGACY_SINGLE_STRATEGY_RESOLVED'
                  )
                AND e.strategy_version<>''
                AND DATE(e.entry_at) <= :as_of_date
                ORDER BY e.entry_at, e.evidence_id
                """,
                {"as_of_date": as_of_date},
            )
            for row in rows:
                key = str(row.get("strategy_key") or "")
                if (
                    key in current_versions
                    and bool(version_frozen_at.get(key))
                    and str(row.get("bound_strategy_version") or "")
                        == current_versions[key]
                    and _normalize_evidence_revision(row.get("entry_at"))
                        > version_frozen_at.get(key, "")
                ):
                    entry_gross = _num(row.get("entry_gross_cny"), 0.0) or 0.0
                    actual_cost_pct = None
                    if (
                        entry_gross > 0
                        and str(row.get("evidence_status") or "") == "MATURED"
                    ):
                        actual_cost_pct = (
                            ((_num(row.get("entry_fee_cny"), 0.0) or 0.0)
                             + (_num(row.get("exit_fee_cny"), 0.0) or 0.0))
                            / entry_gross * 100.0
                        )
                    records[key].append({
                        **row,
                        "is_net_return": (
                            str(row.get("evidence_status") or "") == "MATURED"
                        ),
                        "actual_cost_pct": actual_cost_pct,
                        "evidence_revision_at": (
                            row.get("exit_at") or f"{as_of_date}T15:00:00"
                        ),
                    })
    # st_sim_position intentionally is not used here: it has no immutable
    # strategy_version binding, so allowing it to fund a strategy would mix
    # returns from different formulas under the same display key.
    for items in records.values():
        items.sort(key=lambda item: (
            str(item.get("entry_trade_date") or item.get("entry_at") or ""),
            str(item.get("evidence_revision_at") or ""),
            str(item.get("evidence_id") or ""),
        ))
    return records


def _internal_strategy_portfolio_ledger(
    records: list[dict[str, Any]], *, as_of_date: str,
    strategy_key: str, strategy_version: str, version_hash: str,
    execution_binding_hash: str | None = None,
) -> dict[str, Any]:
    """Rebuild a version-bound, unlevered virtual sleeve from internal fills."""

    if not records:
        return {"valid": False, "reason": "没有版本绑定的内部成交"}
    try:
        account_ids = {str(row.get("account_id") or "") for row in records}
        if len(account_ids) != 1 or "" in account_ids:
            raise ValueError("内部组合账本必须绑定唯一模拟账户")
        if not _HASH_PATTERN.fullmatch(str(version_hash or "")):
            raise ValueError("内部组合账本缺少不可变策略版本哈希")
        effective_execution_binding_hash = (
            None if execution_binding_hash is None
            else str(execution_binding_hash)
        )
        if (
            effective_execution_binding_hash is not None
            and not _HASH_PATTERN.fullmatch(effective_execution_binding_hash)
        ):
            raise ValueError("内部组合账本缺少适配器与成本模型绑定哈希")
        account_id = next(iter(account_ids))
        normalized: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        codes: set[str] = set()
        cutoff = date.fromisoformat(as_of_date)
        for source in records:
            row = dict(source)
            evidence_id = str(row.get("evidence_id") or "")
            source_intent_id = str(
                row.get("source_intent_id") or ""
            ).strip()
            code = str(row.get("stock_code") or "")
            quantity = _int(row.get("entry_quantity"), -1)
            closed_quantity = _int(row.get("closed_quantity"), -1)
            status = str(row.get("evidence_status") or "")
            entry_cash_binding_count = _int(
                row.get("entry_cash_binding_count"), 0
            )
            entry_day = _trade_date(
                row.get("entry_trade_date") or row.get("entry_at"),
                default_today=False,
            )
            entry_at = _normalize_evidence_revision(row.get("entry_at"))
            exit_at = _normalize_evidence_revision(row.get("exit_at"))
            exit_day = exit_at[:10] if exit_at else ""
            # A later close is future information for a historical cutoff; at
            # the cutoff this lot was still open and must be marked to market.
            if exit_day and date.fromisoformat(exit_day) > cutoff:
                status, closed_quantity, exit_at, exit_day = "OPEN", 0, "", ""
                row["exit_gross_cny"] = 0
                row["exit_fee_cny"] = 0
                row["exit_fill_ids_json"] = "[]"
                row["exit_order_ids_json"] = "[]"
                for field in (
                    "exit_fill_id_count", "exit_order_id_count",
                    "exit_allocation_count", "exit_allocated_fill_count",
                    "exit_allocated_order_count", "exit_fill_binding_count",
                    "exit_order_binding_count", "exit_cash_binding_count",
                    "exit_global_conservation_count",
                    "exit_allocation_protocol_count",
                    "exit_fill_trade_day_count", "exit_fill_quantity_sum",
                ):
                    row[field] = 0
                row["exit_fill_gross_sum"] = 0
                row["exit_fill_fee_sum"] = 0
                row["exit_fill_latest_at"] = None
            if (
                str(row.get("strategy_key") or "") != strategy_key
                or str(row.get("bound_strategy_version") or "")
                != strategy_version
            ):
                raise ValueError("内部组合账本混入其他策略或版本成交")
            if (
                not evidence_id or evidence_id in seen_ids
                or not re.fullmatch(r"[0-9A-Za-z_.:-]{1,160}", evidence_id)
                or not re.fullmatch(
                    r"[0-9A-Za-z_.:-]{1,64}", source_intent_id
                )
                or not re.fullmatch(r"[0-9]{6}", code)
                or not entry_at or quantity <= 0 or entry_day > as_of_date
                or entry_cash_binding_count != 1
            ):
                raise ValueError("内部成交的编号、证券、数量、日期或买入现金绑定不满足组合账本协议")
            if status == "PARTIALLY_CLOSED":
                raise ValueError("聚合证据无法还原部分平仓现金时点，资金证据拒绝该样本")
            if status == "MATURED":
                if quantity != closed_quantity or not exit_at or entry_day > exit_day:
                    raise ValueError("成熟内部成交的数量或平仓日期无效")
                exit_fill_count = _int(row.get("exit_fill_id_count"), 0)
                exit_allocation_count = _int(
                    row.get("exit_allocation_count"), 0
                )
                exit_allocated_fill_count = _int(
                    row.get("exit_allocated_fill_count"), 0
                )
                exit_fill_binding_count = _int(
                    row.get("exit_fill_binding_count"), 0
                )
                exit_order_count = _int(row.get("exit_order_id_count"), 0)
                exit_allocated_order_count = _int(
                    row.get("exit_allocated_order_count"), 0
                )
                exit_order_binding_count = _int(
                    row.get("exit_order_binding_count"), 0
                )
                exit_cash_binding_count = _int(
                    row.get("exit_cash_binding_count"), 0
                )
                exit_fill_latest_at = _normalize_evidence_revision(
                    row.get("exit_fill_latest_at")
                )
                if (
                    exit_allocation_count <= 0
                    or exit_fill_count != exit_allocation_count
                    or exit_allocated_fill_count != exit_allocation_count
                    or exit_fill_binding_count != exit_allocation_count
                    or exit_order_count <= 0
                    or exit_allocated_order_count != exit_order_count
                    or exit_order_binding_count != exit_order_count
                    or exit_cash_binding_count != exit_allocation_count
                    or _int(
                        row.get("exit_global_conservation_count"), 0
                    ) != exit_allocation_count
                    or _int(
                        row.get("exit_allocation_protocol_count"), 0
                    ) != exit_allocation_count
                    or _int(row.get("exit_fill_trade_day_count"), 0) != 1
                    or not exit_fill_latest_at
                    or exit_fill_latest_at != exit_at
                ):
                    raise ValueError(
                        "成熟内部成交缺少正规化逐笔卖出分摊、现金绑定或全局守恒，或跨交易日分批平仓无法按日重建"
                    )
            elif status == "OPEN":
                if (
                    closed_quantity != 0
                    or exit_at
                    or _int(row.get("exit_allocation_count"), 0) != 0
                    or _int(row.get("exit_fill_id_count"), 0) != 0
                    or _int(row.get("exit_order_id_count"), 0) != 0
                ):
                    raise ValueError("未平仓内部成交包含无法解释的平仓事实")
            else:
                raise ValueError("内部成交状态不受组合账本协议支持")
            seen_ids.add(evidence_id)
            codes.add(code)
            numeric: dict[str, Decimal] = {}
            for field in (
                "entry_price", "exit_average_price", "entry_gross_cny",
                "entry_fee_cny", "exit_gross_cny", "exit_fee_cny",
            ):
                value = Decimal(str(row.get(field) or "0"))
                if not value.is_finite() or value < 0:
                    raise ValueError("内部成交价格、金额或费用无效")
                numeric[field] = value
            if numeric["entry_price"] <= 0 or numeric["entry_gross_cny"] <= 0:
                raise ValueError("内部成交缺少有效买入价格或金额")
            entry_expected = numeric["entry_price"] * Decimal(quantity)
            if abs(numeric["entry_gross_cny"] - entry_expected) > Decimal("0.02"):
                raise ValueError("内部成交买入金额无法由价格和数量重算")
            if status == "MATURED":
                if numeric["exit_average_price"] <= 0 or numeric["exit_gross_cny"] <= 0:
                    raise ValueError("成熟内部成交缺少有效卖出价格或金额")
                exit_fill_gross_sum = Decimal(str(
                    row.get("exit_fill_gross_sum") or "0"
                ))
                exit_fill_fee_sum = Decimal(str(
                    row.get("exit_fill_fee_sum") or "0"
                ))
                if (
                    _int(row.get("exit_fill_quantity_sum"), 0) != quantity
                    or not exit_fill_gross_sum.is_finite()
                    or not exit_fill_fee_sum.is_finite()
                    or abs(
                        exit_fill_gross_sum - numeric["exit_gross_cny"]
                    ) > Decimal("0.02")
                    or abs(
                        exit_fill_fee_sum - numeric["exit_fee_cny"]
                    ) > Decimal("0.02")
                ):
                    raise ValueError(
                        "聚合卖出金额、费用或数量无法由正规化SELL分摊账本重建"
                    )
                exit_expected = numeric["exit_average_price"] * Decimal(quantity)
                if abs(numeric["exit_gross_cny"] - exit_expected) > Decimal("0.02"):
                    raise ValueError("内部成交卖出金额无法由价格和数量重算")
                cost_basis = numeric["entry_gross_cny"] + numeric["entry_fee_cny"]
                realized = (
                    numeric["exit_gross_cny"] - numeric["exit_fee_cny"]
                    - cost_basis
                ) / cost_basis * Decimal("100")
                reported = Decimal(str(row.get("return_pct") or "0"))
                if not reported.is_finite() or abs(reported - realized) > Decimal("0.0001"):
                    raise ValueError("内部成交净收益率无法由现金事实重算")
            normalized.append({
                "evidence_id": evidence_id,
                "source_intent_id": source_intent_id,
                "stock_code": code,
                "entry_day": entry_day,
                "entry_at": entry_at,
                "exit_day": exit_day,
                "exit_at": exit_at,
                "status": status,
                "quantity": quantity,
                "entry_cash_binding_count": entry_cash_binding_count,
                "exit_fill_ids_json": str(
                    row.get("exit_fill_ids_json") or "[]"
                ),
                "exit_order_ids_json": str(
                    row.get("exit_order_ids_json") or "[]"
                ),
                "exit_fill_id_count": _int(row.get("exit_fill_id_count"), 0),
                "exit_allocation_count": _int(
                    row.get("exit_allocation_count"), 0
                ),
                "exit_allocated_fill_count": _int(
                    row.get("exit_allocated_fill_count"), 0
                ),
                "exit_fill_binding_count": _int(
                    row.get("exit_fill_binding_count"), 0
                ),
                "exit_order_id_count": _int(
                    row.get("exit_order_id_count"), 0
                ),
                "exit_allocated_order_count": _int(
                    row.get("exit_allocated_order_count"), 0
                ),
                "exit_order_binding_count": _int(
                    row.get("exit_order_binding_count"), 0
                ),
                "exit_cash_binding_count": _int(
                    row.get("exit_cash_binding_count"), 0
                ),
                "exit_global_conservation_count": _int(
                    row.get("exit_global_conservation_count"), 0
                ),
                "exit_allocation_protocol_count": _int(
                    row.get("exit_allocation_protocol_count"), 0
                ),
                "exit_fill_trade_day_count": _int(
                    row.get("exit_fill_trade_day_count"), 0
                ),
                "exit_fill_quantity_sum": _int(
                    row.get("exit_fill_quantity_sum"), 0
                ),
                "exit_fill_gross_sum": str(
                    row.get("exit_fill_gross_sum") or "0"
                ),
                "exit_fill_fee_sum": str(
                    row.get("exit_fill_fee_sum") or "0"
                ),
                **numeric,
            })
        normalized.sort(key=lambda row: (row["entry_at"], row["evidence_id"]))
        start_day = min(row["entry_day"] for row in normalized)
        calendar_rows = _db_read(
            "SELECT trade_date FROM si_trade_calendar "
            "WHERE trade_status=1 AND trade_date BETWEEN :start_day AND :as_of_date "
            "ORDER BY trade_date",
            {"start_day": start_day, "as_of_date": as_of_date},
        )
        calendar = [
            _trade_date(row.get("trade_date"), default_today=False)
            for row in calendar_rows
        ]
        if not calendar or calendar[0] != start_day or calendar[-1] != as_of_date:
            raise ValueError("内部组合账本缺少从首笔成交到截止日的完整交易日历")
        code_params = {
            f"code_{index}": code for index, code in enumerate(sorted(codes))
        }
        placeholders = ",".join(f":{key}" for key in code_params)
        price_rows = _db_read(
            "SELECT k.stock_code, k.trade_date, k.close, k.pre_close, "
            "k.data_source, k.quality_status, k.permission_status, "
            "k.source_time, k.received_at, k.batch_id, k.data_version, "
            "a.attestation_id, a.protocol_version "
            "AS pre_close_attestation_protocol, "
            "a.source_pre_close_origin "
            "FROM sm_stock_kline k "
            "JOIN qmt_kline_attestation_row a ON a.target_id=k.id "
            "AND BINARY a.protocol_version=BINARY :protocol_version "
            "AND BINARY a.source_data_version=BINARY k.data_version "
            "AND a.qmt_id>0 AND a.trade_date=k.trade_date "
            "AND BINARY a.stock_code=BINARY k.stock_code "
            "AND BINARY a.attestation_id=BINARY SHA2(CONCAT_WS('|', "
            "a.protocol_version, a.target_id, a.qmt_id, "
            "a.source_data_version, a.source_pre_close, "
            "a.attested_open, a.attested_close, a.attested_high, "
            "a.attested_low, a.attested_volume, a.attested_amount), 256) "
            "AND BINARY a.source_pre_close_origin=BINARY 'NATIVE_QMT' "
            "AND a.source_pre_close=k.pre_close "
            "AND a.attested_open=k.`open` AND a.attested_close=k.`close` "
            "AND a.attested_high=k.`high` AND a.attested_low=k.`low` "
            "AND a.attested_volume=k.volume AND a.attested_amount=k.amount "
            "WHERE k.k_type=1 AND k.adjust_type=0 "
            f"AND k.stock_code IN ({placeholders}) "
            "AND k.trade_date BETWEEN :start_day AND :as_of_date "
            "ORDER BY k.trade_date, k.stock_code",
            {
                **code_params,
                "start_day": start_day,
                "as_of_date": as_of_date,
                "protocol_version": QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            },
        )
        prices_by_day: dict[str, dict[str, Decimal]] = defaultdict(dict)
        bar_facts: dict[tuple[str, str], dict[str, Any]] = {}
        for raw_price in price_rows:
            code = str(raw_price.get("stock_code") or "")
            day = _trade_date(raw_price.get("trade_date"), default_today=False)
            price = Decimal(str(raw_price.get("close") or "0"))
            key = (day, code)
            if key in bar_facts:
                raise ValueError("内部组合账本估值行情存在重复日线")
            if code not in codes or not price.is_finite() or price <= 0:
                continue
            pre_close = Decimal(str(raw_price.get("pre_close") or "0"))
            bar_facts[key] = {
                "close": price,
                "pre_close": pre_close,
                "data_source": str(raw_price.get("data_source") or ""),
                "quality_status": str(
                    raw_price.get("quality_status") or ""
                ),
                "permission_status": str(
                    raw_price.get("permission_status") or ""
                ),
                "source_time": str(raw_price.get("source_time") or ""),
                "received_at": str(raw_price.get("received_at") or ""),
                "batch_id": str(raw_price.get("batch_id") or ""),
                "data_version": str(raw_price.get("data_version") or ""),
                "attestation_id": str(
                    raw_price.get("attestation_id") or ""
                ),
                "pre_close_attestation_protocol": str(
                    raw_price.get("pre_close_attestation_protocol") or ""
                ),
                "source_pre_close_origin": str(
                    raw_price.get("source_pre_close_origin") or ""
                ),
            }
            prices_by_day[day][code] = price

        account_rows = _db_read(
            "SELECT account_id, status, initial_cash, policy_version, "
            "policy_hash, real_trading_enabled, created_at "
            "FROM st_trade_account_v2 WHERE account_id=:account_id LIMIT 1",
            {"account_id": account_id},
        )
        if len(account_rows) != 1:
            raise ValueError("内部组合账本缺少权威模拟账户初始事实")
        account_fact = account_rows[0]
        initial_cash = Decimal(str(account_fact.get("initial_cash") or "0"))
        policy_hash = str(account_fact.get("policy_hash") or "")
        if (
            not initial_cash.is_finite() or initial_cash <= 0
            or not _HASH_PATTERN.fullmatch(policy_hash)
            or _int(account_fact.get("real_trading_enabled"), 1) != 0
        ):
            raise ValueError("权威模拟账户资金、政策哈希或纸面交易边界无效")
        frozen_account = load_v3_config().get("account") or {}
        frozen_initial = Decimal(str(frozen_account.get("initial_cash_cny") or "0"))
        if abs(frozen_initial - initial_cash) > Decimal("0.01"):
            raise ValueError("模拟账户初始资金与部署冻结配置不一致")

        entries: dict[str, list[dict[str, Any]]] = defaultdict(list)
        exits: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in normalized:
            entries[row["entry_day"]].append(row)
            if row["status"] == "MATURED":
                exits[row["exit_day"]].append(row)
        cash = initial_cash
        holdings: dict[str, dict[str, Any]] = {}
        last_prices: dict[str, Decimal] = {}
        equity_curve: list[dict[str, Any]] = []
        daily_records: list[dict[str, Any]] = []
        price_marks: list[dict[str, str]] = []
        daily_stock_market_values: list[dict[str, Any]] = []
        corporate_action_checks: list[dict[str, str]] = []
        previous_equity = initial_cash
        peak = _EQUITY_BASE
        maximum_drawdown = Decimal("0")

        def require_attested_close(
            fact: dict[str, Any] | None, *, trade_day: str,
        ) -> dict[str, Any]:
            if fact is None:
                raise ValueError(
                    "持仓交易日缺少认证日线；无法区分停牌与行情漏采，拒绝沿用旧价"
                )
            source_time = _normalize_evidence_revision(
                fact.get("source_time")
            )
            received_at = _normalize_evidence_revision(
                fact.get("received_at")
            )
            close_cutoff = f"{trade_day}T15:00:00"
            if (
                fact.get("data_source") != "gj_big_qmt_inner"
                or fact.get("quality_status") != "QMT_ATTESTED"
                or fact.get("permission_status") != "SUPPORTED"
                or not source_time
                or not received_at
                or source_time < close_cutoff
                or received_at < source_time
                or not fact.get("batch_id")
                or not fact.get("data_version")
                or not _HASH_PATTERN.fullmatch(
                    str(fact.get("attestation_id") or "")
                )
                or fact.get("pre_close_attestation_protocol")
                != QMT_PRECLOSE_ATTESTATION_PROTOCOL
                or fact.get("source_pre_close_origin") != "NATIVE_QMT"
            ):
                raise ValueError(
                    "内部组合账本持仓估值缺少QMT权威收盘日线认证"
                )
            return fact

        for day in calendar:
            prior_held_codes = {
                str(holding["stock_code"])
                for holding in holdings.values()
            }
            for code in sorted(prior_held_codes):
                fact = require_attested_close(
                    bar_facts.get((day, code)), trade_day=day,
                )
                prior_close = last_prices.get(code)
                pre_close = fact["pre_close"]
                if (
                    prior_close is None
                    or not pre_close.is_finite()
                    or pre_close <= 0
                    or abs(pre_close / prior_close - Decimal("1"))
                    * Decimal("100") > Decimal("0.05")
                ):
                    raise ValueError(
                        "持仓期间发现除权除息或原始价格断点；未建立公司行动账本前拒绝资金证据"
                    )
                corporate_action_checks.append({
                    "trade_date": day,
                    "stock_code": code,
                    "prior_raw_close": str(prior_close),
                    "reported_pre_close": str(pre_close),
                    "status": "NO_DISCONTINUITY",
                })
            for code, price in prices_by_day.get(day, {}).items():
                last_prices[code] = price
            day_fees = Decimal("0")
            day_trade_gross: dict[str, Decimal] = defaultdict(Decimal)
            events = [
                (row["entry_at"], "ENTRY", row["evidence_id"], row)
                for row in entries.get(day, [])
            ] + [
                (row["exit_at"], "EXIT", row["evidence_id"], row)
                for row in exits.get(day, [])
            ]
            for _occurred_at, event_type, _evidence_id, row in sorted(events):
                if event_type == "ENTRY":
                    if row["evidence_id"] in holdings:
                        raise ValueError("内部组合账本成交编号重复开仓")
                    cash -= row["entry_gross_cny"] + row["entry_fee_cny"]
                    day_fees += row["entry_fee_cny"]
                    day_trade_gross[row["stock_code"]] += row[
                        "entry_gross_cny"
                    ]
                    holdings[row["evidence_id"]] = row
                else:
                    if row["evidence_id"] not in holdings:
                        raise ValueError("内部组合账本出现无持仓平仓")
                    cash += row["exit_gross_cny"] - row["exit_fee_cny"]
                    day_fees += row["exit_fee_cny"]
                    day_trade_gross[row["stock_code"]] += row[
                        "exit_gross_cny"
                    ]
                    del holdings[row["evidence_id"]]
                if cash < Decimal("-0.01"):
                    raise ValueError("策略隔离虚拟袖套现金为负，不能证明无杠杆")
            market_value = Decimal("0")
            day_stock_values: dict[str, Decimal] = defaultdict(Decimal)
            for holding in holdings.values():
                price = last_prices.get(holding["stock_code"])
                if price is None:
                    raise ValueError("内部组合账本持仓缺少收盘估值")
                current_fact = require_attested_close(
                    bar_facts.get((day, holding["stock_code"])),
                    trade_day=day,
                )
                holding_value = price * Decimal(holding["quantity"])
                market_value += holding_value
                day_stock_values[holding["stock_code"]] += holding_value
                price_marks.append({
                    "trade_date": day,
                    "stock_code": holding["stock_code"],
                    "close": str(price),
                    "pre_close": str(current_fact["pre_close"]),
                    "source_trade_date": day,
                    "data_source": str(current_fact["data_source"]),
                    "quality_status": str(current_fact["quality_status"]),
                    "permission_status": str(
                        current_fact["permission_status"]
                    ),
                    "source_time": _normalize_evidence_revision(
                        current_fact["source_time"]
                    ),
                    "received_at": _normalize_evidence_revision(
                        current_fact["received_at"]
                    ),
                    "batch_id": str(current_fact["batch_id"]),
                    "data_version": str(current_fact["data_version"]),
                })
            day_risk_exposure = {
                code: max(
                    day_stock_values.get(code, Decimal("0")),
                    day_trade_gross.get(code, Decimal("0")),
                )
                for code in set(day_stock_values) | set(day_trade_gross)
            }
            daily_stock_market_values.append({
                "trade_date": day,
                "stock_closing_market_values": {
                    code: str(value)
                    for code, value in sorted(day_stock_values.items())
                },
                "stock_intraday_turnover_proxy": {
                    code: str(value)
                    for code, value in sorted(day_trade_gross.items())
                },
                "stock_risk_exposure": {
                    code: str(value)
                    for code, value in sorted(day_risk_exposure.items())
                },
            })
            equity = cash + market_value
            if not equity.is_finite() or equity <= 0 or previous_equity <= 0:
                raise ValueError("内部组合账本权益无效")
            normalized_equity = (
                equity / initial_cash * Decimal("100")
            ).quantize(_EQUITY_QUANTUM, rounding=ROUND_HALF_EVEN)
            daily_return = (
                (equity / previous_equity - Decimal("1")) * Decimal("100")
            )
            daily_cost = day_fees / previous_equity * Decimal("100")
            peak = max(peak, normalized_equity)
            maximum_drawdown = max(
                maximum_drawdown,
                (peak - normalized_equity) / peak * Decimal("100"),
            )
            equity_curve.append({
                "trade_date": day,
                "equity": float(normalized_equity),
            })
            daily_records.append({
                "trade_date": day,
                "return_pct": float(daily_return),
                "actual_cost_pct": float(daily_cost),
                "is_net_return": True,
                "evidence_revision_at": f"{day}T15:00:00",
            })
            previous_equity = equity
        funding_evidence_revision_at = (
            f"{as_of_date}T15:00:00"
            if holdings
            else max(
                (
                    str(row.get("exit_at") or "")
                    for row in normalized
                    if row.get("status") == "MATURED"
                ),
                default="",
            )
        )
        if not _normalize_evidence_revision(funding_evidence_revision_at):
            raise ValueError("内部组合账本缺少可验证的经济事实高水位")
        ledger_payload = {
            "schema": "probiga.internal-strategy-portfolio-ledger.v1",
            "strategy_key": strategy_key,
            "strategy_version": strategy_version,
            "strategy_version_hash": version_hash,
            "account_fact": {
                "account_id": account_id,
                "status": str(account_fact.get("status") or ""),
                "initial_cash_cny": str(initial_cash),
                "policy_version": str(account_fact.get("policy_version") or ""),
                "policy_hash": policy_hash,
                "real_trading_enabled": False,
                "created_at": str(account_fact.get("created_at") or ""),
            },
            "as_of_date": as_of_date,
            "funding_evidence_revision_at": funding_evidence_revision_at,
            "trades": [
                {
                    key: (str(value) if isinstance(value, Decimal) else value)
                    for key, value in row.items()
                }
                for row in normalized
            ],
            "price_marks": price_marks,
            "daily_stock_market_values": daily_stock_market_values,
            "corporate_action_guard": {
                "method": "PRE_CLOSE_VS_PRIOR_UNADJUSTED_CLOSE",
                "tolerance_pct": "0.05",
                "unresolved_action_policy": "FAIL_CLOSED",
                "checks": corporate_action_checks,
            },
            "equity_curve": equity_curve,
        }
        if effective_execution_binding_hash is not None:
            ledger_payload["execution_binding_hash"] = (
                effective_execution_binding_hash
            )
        return {
            "valid": True,
            "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
            "drawdown_basis": "internal_version_bound_portfolio_equity",
            "cost_basis": "actual_ledger_fees",
            "max_drawdown_pct": round(float(maximum_drawdown), 4),
            "portfolio_coverage_days": len(equity_curve),
            "internal_ledger_hash": _digest(ledger_payload),
            "internal_ledger_schema": ledger_payload["schema"],
            "execution_binding_hash": (
                effective_execution_binding_hash or ""
            ),
            "equity_curve": equity_curve,
            "daily_records": daily_records,
            "trade_exposures": [
                {
                    "trade_date": row["entry_day"],
                    "source_intent_id": row["source_intent_id"],
                    "stock_code": row["stock_code"],
                    "entry_gross_cny": str(row["entry_gross_cny"]),
                    "status": row["status"],
                }
                for row in normalized
            ],
            "daily_stock_market_values": daily_stock_market_values,
            "completed_trade_count": len({
                    row["source_intent_id"]
                    for row in normalized
                    if row["status"] == "MATURED"
            }),
            "open_position_count": len(holdings),
            "funding_evidence_revision_at": funding_evidence_revision_at,
        }
    except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
        return {"valid": False, "reason": str(exc)[:500]}
    except Exception as exc:
        return {
            "valid": False,
            "reason": f"内部组合账本不可用：{type(exc).__name__}",
        }


def _slice_internal_ledger(
    ledger: dict[str, Any], *, start_date: str, as_of_date: str,
    session_window: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if ledger.get("valid") is not True:
        return dict(ledger)
    full_curve = [
        dict(point) for point in (ledger.get("equity_curve") or [])
    ]
    curve = [
        dict(point) for point in full_curve
        if start_date <= str(point.get("trade_date") or "") <= as_of_date
    ]
    daily_records = [
        dict(item) for item in (ledger.get("daily_records") or [])
        if start_date <= str(item.get("trade_date") or "") <= as_of_date
    ]
    if not curve or not daily_records:
        return {"valid": False, "reason": "内部组合账本在声明窗口内没有日频净值"}
    prior_points = [
        point for point in full_curve
        if str(point.get("trade_date") or "") < start_date
    ]
    if prior_points:
        opening_equity = Decimal(str(prior_points[-1].get("equity") or "0"))
        opening_equity_source = "PREVIOUS_AUTHORITATIVE_SESSION"
        opening_equity_date = str(prior_points[-1].get("trade_date") or "")
    else:
        # The full internal ledger is normalized to the immutable account's
        # initial capital at 100.  This is the only valid baseline before its
        # first recorded session and ensures a first-day loss is not hidden.
        opening_equity = Decimal("100")
        opening_equity_source = "INTERNAL_LEDGER_INITIAL_CAPITAL"
        opening_equity_date = ""
    if not opening_equity.is_finite() or opening_equity <= 0:
        return {"valid": False, "reason": "内部组合账本窗口期初净值无效"}
    peak = opening_equity
    maximum_drawdown = Decimal("0")
    for point in curve:
        equity = Decimal(str(point.get("equity") or "0"))
        if not equity.is_finite() or equity <= 0:
            return {"valid": False, "reason": "内部组合账本窗口净值无效"}
        peak = max(peak, equity)
        maximum_drawdown = max(
            maximum_drawdown,
            (peak - equity) / peak * Decimal("100"),
        )
    exposure_totals: dict[str, Decimal] = defaultdict(Decimal)
    exposure_rows = [
        dict(item)
        for item in (ledger.get("daily_stock_market_values") or [])
        if start_date <= str(item.get("trade_date") or "") <= as_of_date
    ]
    curve_days = [str(point.get("trade_date") or "") for point in curve]
    if [str(item.get("trade_date") or "") for item in exposure_rows] != curve_days:
        return {
            "valid": False,
            "reason": "内部组合账本缺少逐日真实持仓市值敞口",
        }
    for item in exposure_rows:
        values = item.get("stock_risk_exposure")
        if not isinstance(values, dict):
            return {"valid": False, "reason": "逐日持仓市值敞口格式无效"}
        for raw_code, raw_value in values.items():
            code = str(raw_code or "")
            value = Decimal(str(raw_value or "0"))
            if (
                not re.fullmatch(r"[0-9]{6}", code)
                or not value.is_finite()
                or value < 0
            ):
                return {"valid": False, "reason": "逐日持仓市值敞口内容无效"}
            exposure_totals[code] += value
    time_weighted_exposure = {
        code: value / Decimal(len(curve))
        for code, value in exposure_totals.items()
        if value > 0
    }
    completed_trades = len({
        str(item.get("source_intent_id") or "")
        for item in (ledger.get("trade_exposures") or [])
        if start_date <= str(item.get("trade_date") or "") <= as_of_date
        and item.get("status") == "MATURED"
        and str(item.get("source_intent_id") or "")
    })
    parent_hash = str(ledger.get("internal_ledger_hash") or "")
    slice_hash = _digest({
        "schema": "probiga.internal-ledger-window.v2",
        "parent_ledger_hash": parent_hash,
        "start_date": start_date,
        "as_of_date": as_of_date,
        "session_window": session_window or {},
        "opening_equity": str(opening_equity),
        "opening_equity_source": opening_equity_source,
        "opening_equity_date": opening_equity_date,
        "equity_curve": curve,
        "daily_records": daily_records,
        "daily_stock_market_values": exposure_rows,
        "stock_time_weighted_market_value": {
            code: str(value)
            for code, value in sorted(time_weighted_exposure.items())
        },
    })
    return {
        **ledger,
        "valid": True,
        "parent_internal_ledger_hash": parent_hash,
        "internal_ledger_hash": slice_hash,
        "max_drawdown_pct": round(float(maximum_drawdown), 4),
        "opening_equity": float(opening_equity),
        "opening_equity_source": opening_equity_source,
        "opening_equity_date": opening_equity_date,
        "portfolio_coverage_days": len(curve),
        "equity_curve": curve,
        "daily_records": daily_records,
        "exposure_basis": (
            "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
        ),
        "daily_stock_market_values": exposure_rows,
        "stock_exposure": {
            code: str(value)
            for code, value in sorted(time_weighted_exposure.items())
        },
        "completed_trade_count": completed_trades,
        "session_window": dict(session_window or {}),
    }


def _strategy_market_route(
    snapshot: dict[str, Any], strategy: dict[str, Any],
) -> dict[str, Any]:
    """Build a fail-closed, version-bound route for the current market state."""

    market = snapshot.get("market_state")
    market = market if isinstance(market, dict) else {}
    state = str(market.get("key") or "unknown")
    configured_hash = str(market.get("config_hash") or "")
    expected_config_hash = market_state_config_hash()
    route_source = ""
    source_binding: dict[str, Any] = {}
    multiplier: float | None = None
    config_error = ""

    evaluator_config = strategy.get("evaluator_config")
    evaluator_config = evaluator_config if isinstance(evaluator_config, dict) else {}
    raw_policy = evaluator_config.get("market_regime_multipliers")
    if raw_policy is not None:
        try:
            policy = _validated_market_regime_multipliers(raw_policy)
            multiplier = policy.get(state)
            route_source = "immutable_strategy_version"
            source_binding = {
                "version_hash": str(strategy.get("version_hash") or ""),
                "policy": policy,
                "policy_version": str(
                    evaluator_config.get("market_router_policy_version")
                    or MARKET_ROUTER_POLICY_VERSION
                ),
            }
            if source_binding["policy_version"] != MARKET_ROUTER_POLICY_VERSION:
                config_error = "策略版本绑定的市场路由政策已变化，必须注册新版本"
            frozen_market_hash = str(
                evaluator_config.get("market_state_config_hash") or ""
            )
            frozen_market_version = str(
                evaluator_config.get("market_state_config_version") or ""
            )
            source_binding["market_state_config_version"] = frozen_market_version
            source_binding["market_state_config_hash"] = frozen_market_hash
            if not frozen_market_hash or not frozen_market_version:
                config_error = "策略版本未冻结市场状态配置，必须注册新版本"
            elif frozen_market_hash != configured_hash:
                config_error = "策略版本绑定的市场配置已变化，必须注册新版本"
        except ValueError as exc:
            config_error = str(exc)

    if multiplier is None and not config_error:
        cards = {
            str(card.get("key") or ""): card
            for card in (snapshot.get("strategies") or [])
            if isinstance(card, dict)
        }
        card = cards.get(str(strategy.get("strategy_key") or ""))
        if card is not None:
            multiplier = _num(card.get("state_multiplier"), None)
            route_source = "strategy_center_market_card"
            source_binding = {
                "manifest_hash": str(card.get("manifest_hash") or ""),
                "model_version": str(card.get("model_version") or ""),
                "state_multiplier": multiplier,
            }

    input_ready, input_reason = governance_input_ready(snapshot)
    reasons: list[str] = []
    if strategy.get("version_integrity_valid") is False:
        reasons.append("不可变策略版本内容哈希校验失败")
    if strategy.get("execution_adapter_executable") is not True:
        reasons.append(str(
            strategy.get("execution_adapter_reason")
            or "执行适配器未部署/无效"
        ))
    if not input_ready:
        reasons.append(input_reason)
    if state not in MARKET_REGIME_STATES:
        reasons.append("市场状态未知或不受支持")
    if not _HASH_PATTERN.fullmatch(configured_hash):
        reasons.append("市场状态缺少有效配置哈希")
    elif configured_hash != expected_config_hash:
        reasons.append("市场状态配置不是当前部署冻结版本")
    if config_error:
        reasons.append(config_error)
    if multiplier is None:
        reasons.append("当前不可变策略版本未配置市场路由")
    elif not 0.0 <= multiplier <= 1.5:
        reasons.append("市场路由系数超出允许范围")
    elif multiplier <= 0:
        reasons.append("该策略不适配当前市场状态")
    if state == "extreme_event":
        reasons.append("极端事件状态禁止新增模拟资金")

    eligible = not reasons
    match_score = (
        round(max(0.0, min(100.0, float(multiplier) * 100.0)), 4)
        if multiplier is not None and math.isfinite(float(multiplier)) else 0.0
    )
    route_payload = {
        "schema": "probiga.strategy-market-route.v1",
        "policy_version": MARKET_ROUTER_POLICY_VERSION,
        "strategy_key": str(strategy.get("strategy_key") or ""),
        "strategy_version": str(strategy.get("current_version") or ""),
        "trade_date": str(snapshot.get("trade_date") or "")[:10],
        "data_date": str(snapshot.get("data_date") or "")[:10],
        "market_state": state,
        "market_state_config_hash": configured_hash,
        "route_source": route_source or "missing",
        "source_binding": source_binding,
        "multiplier": round(float(multiplier), 4) if multiplier is not None else None,
        "market_match_score": match_score,
        "eligible": eligible,
        "reason": "适配当前市场状态" if eligible else "；".join(dict.fromkeys(reasons)),
    }
    return {**route_payload, "router_decision_hash": _digest(route_payload)}


def _attach_market_routes(
    snapshot: dict[str, Any], registry: list[dict[str, Any]],
) -> None:
    runtime_statuses = {
        str(item.get("strategy_key") or ""): item
        for item in (snapshot.get("dynamic_adapter_statuses") or [])
        if isinstance(item, dict) and str(item.get("strategy_key") or "")
    }
    for strategy in registry:
        if str(strategy.get("source_kind") or "") == "runtime_registry":
            runtime = runtime_statuses.get(
                str(strategy.get("strategy_key") or "")
            )
            if (
                not isinstance(runtime, dict)
                or str(runtime.get("status") or "")
                != "SHADOW_RUN_COMPLETED"
                or str(runtime.get("strategy_version") or "")
                != str(strategy.get("current_version") or "")
                or str(runtime.get("execution_binding_hash") or "")
                != str(strategy.get("execution_binding_hash") or "")
                or str(runtime.get("adapter_artifact_sha256") or "")
                != str(strategy.get("adapter_artifact_sha256") or "")
                or str(runtime.get("cost_model_hash") or "")
                != str(strategy.get("cost_model_hash") or "")
                or runtime.get("run_receipt_valid") is not True
                or not _HASH_PATTERN.fullmatch(str(
                    runtime.get("candidate_receipt_hash") or ""
                ))
                or not _HASH_PATTERN.fullmatch(str(
                    runtime.get("candidate_input_hash") or ""
                ))
                or not _HASH_PATTERN.fullmatch(str(
                    runtime.get("candidate_output_hash") or ""
                ))
            ):
                strategy["execution_adapter_executable"] = False
                strategy["execution_adapter_reason"] = str(
                    (runtime or {}).get("reason")
                    or "执行适配器未部署/无效：本次候选生成缺少版本绑定运行证明"
                )
                strategy["candidate_run_receipt_valid"] = False
            else:
                strategy["candidate_run_receipt_valid"] = True
                strategy["candidate_receipt_hash"] = str(
                    runtime.get("candidate_receipt_hash") or ""
                )
                strategy["candidate_input_hash"] = str(
                    runtime.get("candidate_input_hash") or ""
                )
                strategy["candidate_output_hash"] = str(
                    runtime.get("candidate_output_hash") or ""
                )
        strategy["market_route"] = _strategy_market_route(snapshot, strategy)


def _metrics_for_registry(
    snapshot: dict[str, Any], registry: list[dict[str, Any]], trade_date: str,
    *, authoritative_windows: dict[int, dict[str, Any]] | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    current_versions = {
        row["strategy_key"]: row["current_version"] for row in registry
    }
    manual = _load_metric_inputs(
        trade_date, current_versions=current_versions
    )
    records = _load_forward_records(trade_date, registry)
    result: dict[str, dict[int, dict[str, Any]]] = {}
    target = date.fromisoformat(trade_date)
    try:
        session_windows = (
            authoritative_windows
            if authoritative_windows is not None
            else _authoritative_session_windows(trade_date)
        )
        session_window_error = ""
    except (TypeError, ValueError) as exc:
        session_windows = {}
        session_window_error = str(exc)[:500]
    for row in registry:
        key = row["strategy_key"]
        result[key] = {}
        strategy_records = records.get(key, [])
        full_internal_ledger = _internal_strategy_portfolio_ledger(
            strategy_records,
            as_of_date=trade_date,
            strategy_key=key,
            strategy_version=str(row["current_version"]),
            version_hash=str(row.get("version_hash") or ""),
            execution_binding_hash=(
                str(row.get("execution_binding_hash") or "")
                if str(row.get("source_kind") or "") == "runtime_registry"
                else None
            ),
        )
        try:
            strategy_episodes = _aggregate_forward_intent_episodes(
                strategy_records
            )
            episode_valid = True
            episode_reason = "意图级成交全集与资金流校验通过"
        except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
            strategy_episodes = []
            episode_valid = False
            episode_reason = str(exc)[:500]
        for window in WINDOWS:
            override = manual.get((key, window))
            session_window = session_windows.get(window)
            start_date = str(
                (session_window or {}).get("start_date") or "9999-12-31"
            )
            eligible = [
                item for item in strategy_episodes
                if str(item.get("evidence_status") or "") == "MATURED"
                and item.get("trade_date")
                and start_date
                <= _trade_date(item.get("trade_date"))
                <= trade_date
                and _trade_date(
                    item.get("entry_trade_date") or item.get("entry_at")
                ) >= start_date
            ]
            if eligible:
                metrics = calculate_return_metrics(
                    eligible,
                    window_days=window,
                    market_match_score=(row.get("market_route") or {}).get(
                        "market_match_score"
                    ),
                    version_bound_evidence=True,
                    independent_oos=True,
                    # A chronological summary of executed trades is useful
                    # forward evidence, but it is not by itself a purged
                    # Walk-Forward experiment.
                    walk_forward_verified=False,
                )
                internal_ledger = _slice_internal_ledger(
                    full_internal_ledger,
                    start_date=start_date,
                    as_of_date=trade_date,
                    session_window=session_window,
                )
                if internal_ledger.get("valid") is True:
                    metrics.update({
                        "funding_provenance": internal_ledger["funding_provenance"],
                        "drawdown_basis": internal_ledger["drawdown_basis"],
                        "max_drawdown_pct": internal_ledger["max_drawdown_pct"],
                        "portfolio_coverage_days": internal_ledger[
                            "portfolio_coverage_days"
                        ],
                        "internal_ledger_hash": internal_ledger[
                            "internal_ledger_hash"
                        ],
                        "internal_ledger_schema": internal_ledger[
                            "internal_ledger_schema"
                        ],
                        "parent_internal_ledger_hash": internal_ledger.get(
                            "parent_internal_ledger_hash"
                        ),
                        "cost_basis": internal_ledger["cost_basis"],
                        "internal_daily_records": internal_ledger[
                            "daily_records"
                        ],
                        "internal_equity_curve": internal_ledger[
                            "equity_curve"
                        ],
                        "internal_stock_exposure": internal_ledger[
                            "stock_exposure"
                        ],
                        "internal_stock_exposure_basis": internal_ledger[
                            "exposure_basis"
                        ],
                        "internal_open_position_count": internal_ledger[
                            "open_position_count"
                        ],
                        "evidence_revision_at": internal_ledger[
                            "funding_evidence_revision_at"
                        ],
                    })
                else:
                    metrics["funding_provenance"] = "INTERNAL_LEDGER_INVALID"
                    metrics["internal_ledger_reason"] = str(
                        internal_ledger.get("reason") or "内部组合账本不可用"
                    )
                if override is not None:
                    # The external artifact may attest the frozen model's
                    # purged selection protocol, but never supplies economics,
                    # costs, drawdown or provenance for funding.
                    internal_trade_evidence_hash = str(
                        metrics.get("evidence_hash") or ""
                    )
                    selection_evidence_hash = str(
                        override.get("evidence_hash") or ""
                    )
                    for field in (
                        "evidence_protocol", "artifact_hash",
                        "source_dataset_hash", "verification_status",
                        "submitted_by", "reviewed_by", "reviewed_at",
                        "review_audit_valid",
                    ):
                        metrics[field] = override.get(field)
                    metrics["walk_forward_verified"] = (
                        override.get("walk_forward_verified") is True
                    )
                    metrics["walk_forward_segments"] = _int(
                        override.get("walk_forward_segments")
                    )
                    metrics["positive_segments"] = _int(
                        override.get("positive_segments")
                    )
                    metrics["selection_validation_completed_trades"] = _int(
                        override.get("completed_trades")
                    )
                    metrics["selection_validation_coverage_days"] = _int(
                        override.get("coverage_days")
                    )
                    metrics["selection_validation_revision_at"] = str(
                        override.get("evidence_revision_at") or ""
                    )
                    metrics["selection_validation_independent_oos"] = (
                        override.get("independent_oos") is True
                    )
                    metrics["selection_validation_scope"] = (
                        "VERSION_SELECTION_ONLY"
                    )
                    metrics["internal_trade_evidence_hash"] = (
                        internal_trade_evidence_hash
                    )
                    metrics["selection_evidence_hash"] = (
                        selection_evidence_hash
                    )
                    metrics["evidence_hash"] = _digest({
                        "internal_trade_evidence_hash": (
                            internal_trade_evidence_hash
                        ),
                        "internal_ledger_hash": metrics.get("internal_ledger_hash"),
                        "selection_evidence_hash": selection_evidence_hash,
                        "selection_artifact_hash": metrics.get("artifact_hash"),
                        "strategy_key": key,
                        "strategy_version": row["current_version"],
                        "execution_binding_hash": str(
                            row.get("execution_binding_hash") or ""
                        ),
                        "adapter_artifact_sha256": str(
                            row.get("adapter_artifact_sha256") or ""
                        ),
                        "cost_model_hash": str(
                            row.get("cost_model_hash") or ""
                        ),
                        "window_days": window,
                    })
            elif override is not None:
                metrics = {"window_days": window, **override}
                metrics.setdefault("estimated_cost_pct", DEFAULT_ROUND_TRIP_COST_PCT)
            else:
                metrics = calculate_return_metrics(
                    [], window_days=window,
                    market_match_score=(row.get("market_route") or {}).get(
                        "market_match_score"
                    ),
                    version_bound_evidence=False,
                    independent_oos=False,
                    walk_forward_verified=False,
                )
            metrics["forward_episode_protocol"] = INTENT_EPISODE_PROTOCOL
            metrics["execution_binding_hash"] = str(
                row.get("execution_binding_hash") or ""
            )
            metrics["adapter_artifact_sha256"] = str(
                row.get("adapter_artifact_sha256") or ""
            )
            metrics["cost_model_hash"] = str(
                row.get("cost_model_hash") or ""
            )
            metrics["forward_episode_valid"] = episode_valid
            metrics["forward_episode_reason"] = episode_reason
            metrics["forward_fill_fact_count"] = len(strategy_records)
            metrics["forward_intent_episode_count"] = len(eligible)
            metrics["session_window_valid"] = bool(
                session_window
                and _int(session_window.get("session_count")) == window
                and str(session_window.get("end_date") or "") == trade_date
                and _HASH_PATTERN.fullmatch(
                    str(session_window.get("session_hash") or "")
                )
            )
            metrics["session_window_start"] = str(
                (session_window or {}).get("start_date") or ""
            )
            metrics["session_window_end"] = str(
                (session_window or {}).get("end_date") or ""
            )
            metrics["session_window_count"] = _int(
                (session_window or {}).get("session_count")
            )
            metrics["session_window_hash"] = str(
                (session_window or {}).get("session_hash") or ""
            )
            if not metrics["session_window_valid"]:
                metrics["session_window_reason"] = (
                    session_window_error
                    or f"权威交易日历不足{window}个已收盘交易日"
                )
            # Market match is an authoritative daily router output.  Submitted
            # validation artifacts cannot choose their own current-market score.
            metrics["market_match_score"] = (
                (row.get("market_route") or {}).get("market_match_score")
            )
            metrics["market_route_hash"] = str(
                (row.get("market_route") or {}).get("router_decision_hash") or ""
            )
            evidence_date = str(
                metrics.get("evidence_revision_at")
                or metrics.get("evidence_as_of_date")
                or metrics.get("as_of_date")
                or ""
            )[:10]
            try:
                evidence_age = (target - date.fromisoformat(evidence_date)).days
            except ValueError:
                evidence_age = 999999
            metrics["evidence_age_days"] = evidence_age
            metrics["evidence_fresh"] = (
                0 <= evidence_age
                <= PROFIT_GATE_POLICY["maximum_evidence_age_days"]
            )
            selection_date = str(
                metrics.get("selection_validation_revision_at") or ""
            )[:10]
            try:
                selection_age = (target - date.fromisoformat(selection_date)).days
            except ValueError:
                selection_age = 999999
            metrics["selection_validation_fresh"] = (
                0 <= selection_age
                <= PROFIT_GATE_POLICY["maximum_evidence_age_days"]
            )
            metrics["health_score"] = calculate_health_score(metrics)
            metrics["profit_gate"] = evaluate_window_gate(metrics)
            result[key][window] = metrics
    return result


def transition_lifecycle(
    entity_key: str, next_status: str, *, reason: str, operator: str,
    evidence: dict[str, Any] | None = None, entity_type: str = "STRATEGY",
    automatic: bool = False,
    _connection=None,
    _verified_manual_disable_reenable: bool = False,
) -> dict[str, Any]:
    if _connection is None:
        ensure_and_seed_governance()
    next_value = str(next_status or "").upper()
    if next_value not in LIFECYCLE_LABELS:
        raise ValueError("未知生命周期状态")
    if entity_type == "COMBINATION":
        table, key_col = "st_strategy_combination", "combination_key"
    else:
        table, key_col = "st_strategy_registry", "strategy_key"
    select_sql = (
        f"SELECT current_version, current_status, status_reason, enabled "
        f"FROM {table} WHERE {key_col}=:key"
    )
    if _connection is None:
        rows = _db_read(select_sql, {"key": entity_key})
    else:
        locked = _connection.execute(
            text(select_sql + " FOR UPDATE"), {"key": entity_key}
        ).mappings().first()
        rows = [dict(locked)] if locked is not None else []
    if not rows:
        raise ValueError("治理实体不存在")
    current = str(rows[0].get("current_status") or "SHADOW")
    current_enabled = bool(_int(rows[0].get("enabled")))
    if next_value not in LIFECYCLE_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"不允许从{LIFECYCLE_LABELS.get(current, current)}直接转为{LIFECYCLE_LABELS[next_value]}")
    if not reason.strip():
        raise ValueError("状态变化必须填写理由")
    if not automatic and next_value in {"ACTIVE", "REDUCE"}:
        raise ValueError("正常运行和降权运行只能由盈利硬门槛自动授予，人工不能直接授予资金资格")
    toggle_reenable = bool(
        entity_type == "STRATEGY"
        and current == "SUSPENDED"
        and next_value == "SHADOW"
        and current_enabled is False
        and isinstance(evidence, dict)
        and evidence.get("source") == "strategy_toggle"
        and evidence.get("enabled") is True
        and _verified_manual_disable_reenable
    )
    if (
        not automatic
        and current == "SUSPENDED"
        and next_value == "SHADOW"
        and not toggle_reenable
    ):
        raise ValueError("暂停恢复只能由每日治理使用暂停后新证据自动执行，人工不能绕过恢复门槛")
    if (
        current == next_value
        and str(rows[0].get("status_reason") or "") == reason
        and not (next_value == "RETIRED" and current_enabled)
    ):
        return {"entity_key": entity_key, "previous_status": current, "next_status": next_value, "changed": False}
    version = str(rows[0].get("current_version") or "")
    event_payload = {"entity_type": entity_type, "entity_key": entity_key, "entity_version": version, "previous_status": current, "next_status": next_value, "reason": reason, "evidence": evidence or {}, "nonce": uuid.uuid4().hex}

    def apply(connection) -> None:
        updated = connection.execute(
            text(
                f"UPDATE {table} SET current_status=:status, "
                f"status_reason=:reason, "
                f"enabled=IF(:retiring=1, 0, enabled), updated_at=NOW() "
                f"WHERE {key_col}=:key AND current_status=:current_status "
                "AND current_version=:current_version"
            ),
            {
                "status": next_value,
                "reason": reason[:500],
                "key": entity_key,
                "current_status": current,
                "current_version": version,
                "retiring": 1 if next_value == "RETIRED" else 0,
            },
        )
        if updated.rowcount != 1:
            raise RuntimeError("生命周期状态或版本已被并发更新，请重新计算治理结果")
        if entity_type == "STRATEGY" and next_value == "RETIRED":
            center_exists = connection.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=DATABASE() "
                "AND table_name='st_strategy_center_config'"
            )).scalar()
            if center_exists:
                center_row = connection.execute(text(
                    "SELECT enabled FROM st_strategy_center_config "
                    "WHERE strategy_key=:key FOR UPDATE"
                ), {"key": entity_key}).mappings().first()
                if center_row is not None and bool(
                    _int(center_row.get("enabled"))
                ):
                    connection.execute(text(
                        """
                        UPDATE st_strategy_center_config
                        SET enabled=0, version=version+1,
                            updated_by=:operator, updated_at=NOW()
                        WHERE strategy_key=:key AND enabled<>0
                        """
                    ), {"key": entity_key, "operator": operator[:80]})
                    connection.execute(text(
                        """
                        INSERT INTO st_strategy_center_audit
                        (strategy_key, action, old_value, new_value,
                         reason, operator)
                        VALUES (:key, 'retire', :old_value, :new_value,
                                :reason, :operator)
                        """
                    ), {
                        "key": entity_key,
                        "old_value": _json_text({"enabled": True}),
                        "new_value": _json_text({"enabled": False}),
                        "reason": reason[:500],
                        "operator": operator[:80],
                    })
        connection.execute(
            text(
                """
                INSERT INTO st_strategy_lifecycle_event
                (event_id, entity_type, entity_key, entity_version,
                 previous_status, next_status, reason, trigger_type,
                 evidence_json, payload_json, event_hash, operator_name)
                VALUES (:event_id, :entity_type, :entity_key, :entity_version,
                        :previous_status, :next_status, :reason, :trigger_type,
                        :evidence_json, :payload_json, :event_hash,
                        :operator_name)
                """
            ),
            {
                **event_payload,
                "event_id": uuid.uuid4().hex,
                "trigger_type": "AUTOMATIC_GATE" if automatic else "MANUAL_GOVERNANCE",
                "evidence_json": _json_text(evidence or {}),
                "payload_json": _json_text(event_payload),
                "event_hash": _digest(event_payload),
                "operator_name": operator[:80],
            },
        )
        _append_audit_connection(
            connection,
            entity_type=entity_type,
            entity_key=entity_key,
            action="LIFECYCLE_TRANSITION",
            reason=reason,
            operator=operator,
            before={
                "status": current,
                "version": version,
                "enabled": current_enabled,
            },
            after={
                "status": next_value,
                "version": version,
                "enabled": False if next_value == "RETIRED" else current_enabled,
            },
            evidence=evidence or {},
        )

    if _connection is None:
        with get_engine().begin() as connection:
            apply(connection)
    else:
        apply(_connection)
    return {"entity_key": entity_key, "previous_status": current, "previous_status_label": LIFECYCLE_LABELS[current], "next_status": next_value, "next_status_label": LIFECYCLE_LABELS[next_value], "changed": True, "reason": reason, "enabled": False if next_value == "RETIRED" else current_enabled}


def _is_verified_manual_disable_suspension(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    payload = _json(row.get("payload_json"), None)
    evidence = _json(row.get("evidence_json"), None)
    return bool(
        isinstance(payload, dict)
        and isinstance(evidence, dict)
        and _digest(payload) == str(row.get("event_hash") or "")
        and str(row.get("trigger_type") or "") == "MANUAL_GOVERNANCE"
        and str(row.get("previous_status") or "")
        == str(payload.get("previous_status") or "")
        and str(row.get("next_status") or "")
        == str(payload.get("next_status") or "")
        and str(payload.get("previous_status") or "") != "SUSPENDED"
        and str(payload.get("next_status") or "") == "SUSPENDED"
        and evidence.get("source") == "strategy_toggle"
        and evidence.get("enabled") is False
    )


def toggle_strategy_enabled(
    strategy_key: str, enabled: bool, *, reason: str, operator: str,
) -> dict[str, Any]:
    """Atomically align legacy signal configuration and governance state."""

    ensure_and_seed_governance()
    key = validate_strategy_key(strategy_key)
    rows = _db_read(
        "SELECT current_version, current_status, enabled "
        "FROM st_strategy_registry WHERE strategy_key=:key",
        {"key": key},
    )
    if not rows:
        raise ValueError("策略未注册")
    current_enabled = bool(_int(rows[0].get("enabled")))
    current_status = str(rows[0].get("current_status") or "SHADOW")
    if current_status == "RETIRED" and enabled:
        raise ValueError("已淘汰版本不可重新启用；请注册新版本并重新进入影子验证")
    if current_enabled == bool(enabled):
        return {
            "strategy_key": key,
            "enabled": current_enabled,
            "reason": "启停状态未变化，幂等返回",
            "updated_by": operator,
            "lifecycle": {
                "entity_key": key,
                "previous_status": current_status,
                "next_status": current_status,
                "changed": False,
            },
        }
    target_status = (
        "RETIRED" if current_status == "RETIRED"
        else "SHADOW" if enabled else "SUSPENDED"
    )
    transition_reason = reason.strip() or (
        "重新启用后先进入影子观察" if enabled else "人工关闭策略信号"
    )
    with get_engine().begin() as connection:
        verified_manual_reenable = False
        if enabled and current_status == "SUSPENDED":
            suspension_row = connection.execute(text(
                "SELECT previous_status, next_status, trigger_type, "
                "evidence_json, payload_json, event_hash "
                "FROM st_strategy_lifecycle_event "
                "WHERE entity_type='STRATEGY' AND entity_key=:key "
                "AND entity_version=:version AND next_status='SUSPENDED' "
                "AND previous_status<>'SUSPENDED' "
                "ORDER BY occurred_at DESC, event_id DESC LIMIT 1 FOR UPDATE"
            ), {
                "key": key,
                "version": str(rows[0].get("current_version") or ""),
            }).mappings().first()
            verified_manual_reenable = _is_verified_manual_disable_suspension(
                dict(suspension_row) if suspension_row is not None else None
            )
            if not verified_manual_reenable:
                raise ValueError(
                    "该暂停由盈利门槛或自动风控触发；必须由每日治理使用暂停后新证据恢复"
                )
        if current_status == "SUSPENDED" and not enabled:
            # Disabling an already risk-suspended strategy must not overwrite
            # the original suspension cause with a forged manual-disable event.
            transition = {
                "entity_key": key,
                "previous_status": current_status,
                "next_status": current_status,
                "changed": False,
            }
        else:
            transition = transition_lifecycle(
                key, target_status, reason=transition_reason,
                operator=operator,
                evidence={"source": "strategy_toggle", "enabled": enabled},
                _connection=connection,
                _verified_manual_disable_reenable=verified_manual_reenable,
            )
        post_transition_status = str(
            transition.get("next_status") or current_status
        )
        updated = connection.execute(text(
            """
            UPDATE st_strategy_registry
            SET enabled=:enabled
            WHERE strategy_key=:key AND current_version=:version
            AND current_status=:current_status AND enabled=:current_enabled
            """
        ), {
            "enabled": 1 if enabled else 0, "key": key,
            "version": str(rows[0].get("current_version") or ""),
            "current_status": post_transition_status,
            "current_enabled": 1 if current_enabled else 0,
        })
        if updated.rowcount != 1:
            raise RuntimeError("策略启停状态已被并发更新")
        center_exists = connection.execute(text(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=DATABASE() "
            "AND table_name='st_strategy_center_config'"
        )).scalar()
        if center_exists:
            old_center = connection.execute(text(
                "SELECT enabled FROM st_strategy_center_config "
                "WHERE strategy_key=:key"
            ), {"key": key}).mappings().first()
            if old_center is not None:
                connection.execute(text(
                    """
                    UPDATE st_strategy_center_config
                    SET enabled=:enabled, version=version+1,
                        updated_by=:operator, updated_at=NOW()
                    WHERE strategy_key=:key
                    """
                ), {
                    "enabled": 1 if enabled else 0, "operator": operator[:80],
                    "key": key,
                })
                connection.execute(text(
                    """
                    INSERT INTO st_strategy_center_audit
                    (strategy_key, action, old_value, new_value, reason, operator)
                    VALUES (:key, 'toggle', :old_value, :new_value,
                            :reason, :operator)
                    """
                ), {
                    "key": key,
                    "old_value": _json_text({"enabled": bool(_int(old_center.get("enabled")))}),
                    "new_value": _json_text({"enabled": bool(enabled)}),
                    "reason": transition_reason[:500],
                    "operator": operator[:80],
                })
    return {
        "strategy_key": key, "enabled": bool(enabled),
        "reason": transition_reason, "updated_by": operator,
        "lifecycle": transition,
    }


def _funding_evidence_revision_at(
    window_metrics: dict[int, dict[str, Any]],
) -> str:
    """Return the conservative high-watermark shared by every gate window."""

    revisions: list[str] = []
    for window in WINDOWS:
        raw = str(
            window_metrics[window].get("evidence_revision_at")
            or window_metrics[window].get("as_of_date")
            or window_metrics[window].get("evidence_as_of_date")
            or ""
        ).strip().replace(" ", "T")
        if not raw:
            return ""
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return ""
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        revisions.append(parsed.isoformat(timespec="seconds"))
    return min(revisions) if revisions else ""


def _strategy_rankings(registry: list[dict[str, Any]], metrics: dict[str, dict[int, dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    lane_order = {"ACTIVE": 0, "REDUCE": 0, "SHADOW": 1, "SUSPENDED": 2, "RETIRED": 3}
    for item in registry:
        market_route = item.get("market_route") or {}
        window_metrics = metrics[item["strategy_key"]]
        primary = window_metrics[60]
        medium_gate = primary["profit_gate"]
        long_gate = window_metrics[120]["profit_gate"]
        short = window_metrics[20]
        short_gate = short["profit_gate"]
        short_gate_passed = bool(short_gate["passed"])
        enabled = bool(item.get("enabled"))
        adapter_executable = item.get("execution_adapter_executable") is True
        runtime_strategy = (
            str(item.get("source_kind") or "") == "runtime_registry"
        )
        funding_pipeline_ready = bool(
            not runtime_strategy or item.get("funding_pipeline_ready") is True
        )
        overall_gate_passed = bool(
            enabled and adapter_executable and funding_pipeline_ready
            and short_gate_passed
            and medium_gate["passed"] and long_gate["passed"]
        )
        failed_windows = []
        evidence_block_reasons = list(dict.fromkeys(
            str(window_metrics[window].get("internal_ledger_reason") or "").strip()
            for window in WINDOWS
            if str(window_metrics[window].get("internal_ledger_reason") or "").strip()
        ))
        if not enabled:
            failed_windows.append("策略已禁用")
        if not adapter_executable:
            failed_windows.append(
                str(item.get("execution_adapter_reason")
                    or "执行适配器未部署/无效")
            )
        if not funding_pipeline_ready:
            failed_windows.append(str(
                item.get("funding_pipeline_reason")
                or "动态策略资金证据闭环尚未部署，仅允许影子研究"
            ))
        if not short_gate_passed:
            failed_windows.append(
                "20日近期稳定性"
                + (f"（{evidence_block_reasons[0]}）" if evidence_block_reasons else "")
            )
        if not medium_gate["passed"]:
            failed_windows.append("60日盈利硬门槛")
        if not long_gate["passed"]:
            failed_windows.append("120日长期稳定性")
        overall_reason = (
            "20日、60日和120日盈利硬门槛全部通过"
            if overall_gate_passed
            else "未通过：" + "、".join(failed_windows)
        )
        ranking_score = round(
            short["health_score"] * 0.25
            + primary["health_score"] * 0.50
            + window_metrics[120]["health_score"] * 0.25,
            2,
        )
        funding_gate_hash = _digest({
            "strategy_key": item["strategy_key"],
            "strategy_version": item["current_version"],
            "window_evidence": {
                str(window): window_metrics[window].get("evidence_hash")
                for window in WINDOWS
            },
            "router_decision_hash": market_route.get("router_decision_hash"),
            "overall_gate_passed": overall_gate_passed,
        })
        funding_evidence_revision_at = _funding_evidence_revision_at(
            window_metrics
        )
        status = item["current_status"]
        rows.append({
            **item,
            "lane": "正式赛道" if status in {"ACTIVE", "REDUCE"} else "观察赛道" if status == "SHADOW" else "暂停赛道" if status == "SUSPENDED" else "历史档案",
            "ranking_score": ranking_score,
            "ranking_basis": DAILY_NAV_RANKING_BASIS,
            "ranking_basis_label": DAILY_NAV_RANKING_BASIS_LABEL,
            "profit_gate_passed": overall_gate_passed,
            "profit_gate_reason": overall_reason,
            "execution_adapter_executable": adapter_executable,
            "execution_adapter_reason": str(
                item.get("execution_adapter_reason")
                or "执行适配器未部署/无效"
            ),
            "funding_pipeline_ready": funding_pipeline_ready,
            "funding_pipeline_reason": str(
                item.get("funding_pipeline_reason") or ""
            ),
            "evidence_block_reasons": evidence_block_reasons,
            "market_route": market_route,
            "market_route_eligible": market_route.get("eligible") is True,
            "market_route_reason": str(market_route.get("reason") or "市场路由缺失"),
            "multi_window_gate": {
                "20": short_gate,
                "60": medium_gate,
                "120": long_gate,
            },
            "funding_gate_hash": funding_gate_hash,
            "funding_evidence_revision_at": funding_evidence_revision_at,
            "metrics": {str(window): window_metrics[window] for window in WINDOWS},
            "primary_metrics": primary,
            "win_rate_pct": primary.get("win_rate_pct"),
            "payoff_ratio": primary.get("payoff_ratio"),
            "profit_factor": primary.get("profit_factor"),
            "net_expectancy_pct": primary.get("net_expectancy_pct"),
            "paper_allocation_eligible": (
                status in {"ACTIVE", "REDUCE"}
                and overall_gate_passed
                and adapter_executable
                and market_route.get("eligible") is True
            ),
            "real_order_authority": False,
        })
    rows.sort(key=lambda row: (lane_order.get(row["current_status"], 9), -float(row["ranking_score"]), row["strategy_key"]))
    lane_ranks: dict[str, int] = defaultdict(int)
    for index, row in enumerate(rows, 1):
        row["overall_rank"] = index
        lane_ranks[row["lane"]] += 1
        row["lane_rank"] = lane_ranks[row["lane"]]
    return rows


def _prior_consecutive_gate_passes(
    strategy_key: str, strategy_version: str, before_date: str,
    current_hash: str, current_revision_at: str, limit: int = 2,
    minimum_revision_exclusive: str = "",
    minimum_trade_date_exclusive: str = "",
) -> int:
    if not _table_exists("st_strategy_health_snapshot"):
        return 0
    rows = _db_read(
        """
        SELECT h.trade_date, h.profit_gate_passed, h.evidence_json
        FROM st_strategy_health_snapshot h
        INNER JOIN st_strategy_governance_run r ON r.run_uid=h.run_uid
        WHERE h.strategy_key=:strategy_key
          AND h.strategy_version=:strategy_version
          AND h.window_days=60 AND h.trade_date < :before_date
          AND r.status='COMPLETED' AND r.is_canonical=1
        ORDER BY h.trade_date DESC, h.created_at DESC, h.run_uid DESC
        LIMIT :limit
        """,
        {
            "strategy_key": strategy_key,
            "strategy_version": strategy_version,
            "before_date": before_date,
            "limit": max(20, int(limit) * 20),
        },
    )
    expected_sessions = _db_read(
        "SELECT trade_date FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date<:before_date "
        "ORDER BY trade_date DESC LIMIT :limit",
        {"before_date": before_date, "limit": max(1, int(limit))},
    )
    ceiling = _normalize_evidence_revision(current_revision_at)
    if not ceiling:
        return 0
    expected_days = [
        _trade_date(row.get("trade_date"), default_today=False)
        for row in expected_sessions[: max(1, int(limit))]
    ]
    if len(expected_days) < limit or len(expected_days) != len(set(expected_days)):
        return 0
    rows_by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        trade_day = str(row.get("trade_date") or "")[:10]
        if trade_day in rows_by_day:
            return 0
        rows_by_day[trade_day] = row
    count = 0
    seen_hashes: set[str] = {current_hash}
    for trade_day in expected_days:
        if (
            minimum_trade_date_exclusive
            and trade_day <= minimum_trade_date_exclusive
        ):
            break
        row = rows_by_day.get(trade_day)
        if row is None or _int(row.get("profit_gate_passed")) != 1:
            break
        evidence = _json(row.get("evidence_json"), {})
        passed = evidence.get("overall_profit_gate_passed") is True
        evidence_hash = str(evidence.get("funding_gate_hash") or "")
        revision_at = _normalize_evidence_revision(
            evidence.get("funding_evidence_revision_at")
        )
        if not passed or not evidence_hash or not revision_at:
            break
        if (
            minimum_revision_exclusive
            and revision_at <= minimum_revision_exclusive
        ):
            break
        if revision_at > ceiling:
            break
        if revision_at == ceiling or evidence_hash in seen_hashes:
            break
        seen_hashes.add(evidence_hash)
        ceiling = revision_at
        count += 1
        if count >= limit:
            break
    return count


def _prior_consecutive_combination_gate_passes(
    entity_key: str, entity_version: str, before_date: str,
    current_hash: str, current_revision_at: str, limit: int = 2,
    minimum_revision_exclusive: str = "",
    minimum_trade_date_exclusive: str = "",
) -> int:
    """Count distinct, prior passing combination evidence updates."""

    if not _table_exists("st_strategy_combination_health_snapshot"):
        return 0
    rows = _db_read(
        """
        SELECT h.trade_date, h.profit_gate_passed, h.evidence_json
        FROM st_strategy_combination_health_snapshot h
        INNER JOIN st_strategy_governance_run r ON r.run_uid=h.run_uid
        WHERE h.combination_key=:entity_key
          AND h.combination_version=:entity_version
          AND h.trade_date < :before_date
          AND r.status='COMPLETED' AND r.is_canonical=1
        ORDER BY h.trade_date DESC, h.created_at DESC, h.run_uid DESC
        LIMIT :limit
        """,
        {
            "entity_key": entity_key,
            "entity_version": entity_version,
            "before_date": before_date,
            "limit": max(20, int(limit) * 20),
        },
    )
    expected_sessions = _db_read(
        "SELECT trade_date FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date<:before_date "
        "ORDER BY trade_date DESC LIMIT :limit",
        {"before_date": before_date, "limit": max(1, int(limit))},
    )
    ceiling = _normalize_evidence_revision(current_revision_at)
    if not ceiling:
        return 0
    expected_days = [
        _trade_date(row.get("trade_date"), default_today=False)
        for row in expected_sessions[: max(1, int(limit))]
    ]
    if len(expected_days) < limit or len(expected_days) != len(set(expected_days)):
        return 0
    rows_by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        evidence_date = str(row.get("trade_date") or "")[:10]
        if evidence_date in rows_by_day:
            return 0
        rows_by_day[evidence_date] = row
    count = 0
    seen_hashes: set[str] = {current_hash}
    for evidence_date in expected_days:
        if (
            minimum_trade_date_exclusive
            and evidence_date <= minimum_trade_date_exclusive
        ):
            break
        row = rows_by_day.get(evidence_date)
        if row is None or _int(row.get("profit_gate_passed")) != 1:
            break
        evidence = _json(row.get("evidence_json"), {})
        evidence_hash = str(evidence.get("funding_gate_hash") or "")
        revision_at = _normalize_evidence_revision(
            evidence.get("funding_evidence_revision_at")
        )
        if (
            evidence.get("overall_profit_gate_passed") is not True
            or not evidence_hash
            or not revision_at
        ):
            break
        if (
            minimum_revision_exclusive
            and revision_at <= minimum_revision_exclusive
        ):
            break
        if revision_at > ceiling:
            break
        if revision_at == ceiling or evidence_hash in seen_hashes:
            break
        seen_hashes.add(evidence_hash)
        ceiling = revision_at
        count += 1
        if count >= limit:
            break
    return count


def _combination_market_route(
    combination: dict[str, Any], members: list[dict[str, Any]],
    version_mismatches: list[dict[str, Any]], trade_date: str,
) -> dict[str, Any]:
    member_routes = {
        item["strategy"]["strategy_key"]: item["strategy"].get("market_route") or {}
        for item in members
    }
    states = {
        str(route.get("market_state") or "unknown")
        for route in member_routes.values()
    }
    state = next(iter(states)) if len(states) == 1 else "unknown"
    reasons: list[str] = []
    if not members:
        reasons.append("组合没有有效成员")
    if combination.get("config_integrity_valid") is not True:
        reasons.append("组合不可变版本配置哈希校验失败")
    if version_mismatches:
        reasons.append("组合成员版本与冻结版本不一致")
    if len(states) != 1:
        reasons.append("组合成员市场状态不一致")
    ineligible = [
        key for key, route in member_routes.items()
        if route.get("eligible") is not True
    ]
    if ineligible:
        reasons.append("成员不适配当前市场：" + "、".join(sorted(ineligible)))
    scores = [
        _num(route.get("market_match_score"), 0.0) or 0.0
        for route in member_routes.values()
    ]
    match_score = round(min(scores), 4) if scores else 0.0
    route_payload = {
        "schema": "probiga.combination-market-route.v1",
        "policy_version": MARKET_ROUTER_POLICY_VERSION,
        "combination_key": str(combination.get("combination_key") or ""),
        "combination_version": str(combination.get("current_version") or ""),
        "trade_date": trade_date,
        "market_state": state,
        "member_route_hashes": {
            key: str(route.get("router_decision_hash") or "")
            for key, route in sorted(member_routes.items())
        },
        "market_match_score": match_score,
        "eligible": not reasons,
        "reason": "全部成员适配当前市场状态" if not reasons else "；".join(reasons),
    }
    return {**route_payload, "router_decision_hash": _digest(route_payload)}


def _pearson_correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum(
        (a - left_mean) * (b - right_mean)
        for a, b in zip(left, right)
    )
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0 or right_var <= 0:
        return None
    return max(-1.0, min(1.0, numerator / math.sqrt(left_var * right_var)))


def _internal_combination_portfolio_ledger(
    combination: dict[str, Any], members: list[dict[str, Any]],
    *, window: int, trade_date: str,
) -> dict[str, Any]:
    """Build NAV from fixed initial sleeves whose weights naturally drift."""

    try:
        if not members:
            raise ValueError("组合没有可重建的成员")
        total = sum(item["weight"] for item in members)
        if total <= 0:
            raise ValueError("组合成员权重无效")
        member_payloads: list[dict[str, Any]] = []
        member_revisions: list[str] = []
        member_session_windows: list[dict[str, Any]] = []
        common_days: set[str] | None = None
        for item in members:
            strategy = item["strategy"]
            metrics = strategy["metrics"][str(window)]
            if (
                item.get("version_match") is not True
                or metrics.get("funding_provenance")
                != "INTERNAL_PORTFOLIO_LEDGER_V1"
                or not _HASH_PATTERN.fullmatch(
                    str(metrics.get("internal_ledger_hash") or "")
                )
                or metrics.get("internal_stock_exposure_basis")
                != "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
            ):
                raise ValueError("组合成员缺少相同窗口的内部版本绑定日频净值")
            daily = {
                str(row.get("trade_date") or ""): row
                for row in (metrics.get("internal_daily_records") or [])
                if str(row.get("trade_date") or "")
            }
            if not daily:
                raise ValueError("组合成员内部日频净值为空")
            equity = {
                str(point.get("trade_date") or ""): Decimal(
                    str(point.get("equity") or "0")
                )
                for point in (metrics.get("internal_equity_curve") or [])
                if str(point.get("trade_date") or "")
            }
            if set(equity) != set(daily) or any(
                not value.is_finite() or value <= 0
                for value in equity.values()
            ):
                raise ValueError("组合成员日频收益与内部净值无法逐日对应")
            member_revision = _normalize_evidence_revision(
                metrics.get("evidence_revision_at")
            )
            if not member_revision:
                raise ValueError("组合成员缺少真实经济事实高水位")
            member_revisions.append(member_revision)
            member_session_window = {
                "valid": metrics.get("session_window_valid") is True,
                "start_date": str(
                    metrics.get("session_window_start") or ""
                ),
                "end_date": str(metrics.get("session_window_end") or ""),
                "session_count": _int(metrics.get("session_window_count")),
                "session_hash": str(
                    metrics.get("session_window_hash") or ""
                ),
            }
            if (
                member_session_window["valid"] is not True
                or member_session_window["session_count"] != window
                or not _HASH_PATTERN.fullmatch(
                    member_session_window["session_hash"]
                )
            ):
                raise ValueError("组合成员缺少精确权威交易日窗口")
            member_session_windows.append(member_session_window)
            common_days = set(daily) if common_days is None else common_days & set(daily)
            member_payloads.append({
                "strategy_key": strategy["strategy_key"],
                "strategy_version": strategy["current_version"],
                "weight": item["weight"] / total,
                "metrics": metrics,
                "daily": daily,
                "equity": equity,
            })
        ordered_days = sorted(common_days or ())
        if (
            len(ordered_days) != window
            or not ordered_days
            or ordered_days[0] != member_session_windows[0]["start_date"]
            or ordered_days[-1] != member_session_windows[0]["end_date"]
        ):
            raise ValueError("组合成员日频净值未完整覆盖精确交易日窗口")
        if len({
            _digest(item) for item in member_session_windows
        }) != 1:
            raise ValueError("组合成员的权威交易日窗口不一致")
        session_window = member_session_windows[0]
        daily_records: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = []
        member_sleeves = {
            item["strategy_key"]: (
                _EQUITY_BASE * Decimal(str(item["weight"]))
            )
            for item in member_payloads
        }
        peak = _EQUITY_BASE
        maximum_drawdown = Decimal("0")
        for day_index, day in enumerate(ordered_days):
            prior_equity = sum(
                member_sleeves.values(), Decimal("0")
            )
            actual_cost_value = Decimal("0")
            for item in member_payloads:
                daily_row = item["daily"][day]
                daily_return = Decimal(str(
                    _num(daily_row.get("return_pct"), -999.0)
                ))
                daily_cost = Decimal(str(
                    _num(daily_row.get("actual_cost_pct"), -1.0)
                ))
                if (
                    daily_row.get("is_net_return") is not True
                    or not daily_return.is_finite()
                    or daily_return <= Decimal("-100")
                    or not daily_cost.is_finite()
                    or daily_cost < 0
                ):
                    raise ValueError("组合成员日频净收益或实际成本无效")
                if day_index > 0:
                    previous_day = ordered_days[day_index - 1]
                    absolute_return = (
                        item["equity"][day]
                        / item["equity"][previous_day]
                        - Decimal("1")
                    ) * Decimal("100")
                    if abs(absolute_return - daily_return) > Decimal("0.0001"):
                        raise ValueError("组合成员日频净收益无法由内部净值重算")
                key = item["strategy_key"]
                prior_sleeve = member_sleeves[key]
                actual_cost_value += (
                    prior_sleeve * daily_cost / Decimal("100")
                )
                member_sleeves[key] = (
                    prior_sleeve
                    * (Decimal("1") + daily_return / Decimal("100"))
                )
            equity = sum(
                member_sleeves.values(), Decimal("0")
            ).quantize(_EQUITY_QUANTUM, rounding=ROUND_HALF_EVEN)
            if not equity.is_finite() or equity <= 0:
                raise ValueError("组合独立虚拟净值无效")
            net_return = float(
                (equity / prior_equity - Decimal("1")) * Decimal("100")
            )
            actual_cost = float(
                actual_cost_value / prior_equity * Decimal("100")
            )
            peak = max(peak, equity)
            maximum_drawdown = max(
                maximum_drawdown,
                (peak - equity) / peak * Decimal("100"),
            )
            daily_records.append({
                "trade_date": day,
                "return_pct": round(net_return, 10),
                "actual_cost_pct": round(actual_cost, 10),
                "is_net_return": True,
                "evidence_revision_at": f"{day}T15:00:00",
            })
            equity_curve.append({"trade_date": day, "equity": float(equity)})
        stock_exposure: dict[str, Decimal] = defaultdict(Decimal)
        for item in member_payloads:
            member_exposure = {
                str(code): Decimal(str(raw_value or "0"))
                for code, raw_value in (
                    item["metrics"].get("internal_stock_exposure") or {}
                ).items()
            }
            if any(
                not value.is_finite() or value < 0
                for value in member_exposure.values()
            ):
                raise ValueError("组合成员真实持仓市值敞口无效")
            member_total = sum(member_exposure.values(), Decimal("0"))
            if member_total <= 0:
                continue
            for code, value in member_exposure.items():
                if value.is_finite() and value > 0:
                    stock_exposure[str(code)] += (
                        value / member_total * Decimal(str(item["weight"]))
                    )
        ledger_payload = {
            "schema": "probiga.internal-combination-portfolio-ledger.v2",
            "combination_key": combination["combination_key"],
            "combination_version": combination["current_version"],
            "combination_config_hash": combination.get("config_hash"),
            "window_days": window,
            "trade_date": trade_date,
            "allocation_semantics": (
                "WINDOW_OPEN_REBASED_FIXED_SLEEVES_NATURAL_WEIGHT_DRIFT_V2"
            ),
            "funding_evidence_revision_at": min(member_revisions),
            "session_window": session_window,
            "member_ledgers": [
                {
                    "strategy_key": item["strategy_key"],
                    "strategy_version": item["strategy_version"],
                    "weight": round(item["weight"], 8),
                    "internal_ledger_hash": item["metrics"].get(
                        "internal_ledger_hash"
                    ),
                }
                for item in member_payloads
            ],
            "daily_records": daily_records,
            "equity_curve": equity_curve,
            "stock_exposure_basis": (
                "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
            ),
            "stock_exposure": {
                code: str(value)
                for code, value in sorted(stock_exposure.items())
            },
        }
        return {
            "valid": True,
            "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
            "drawdown_basis": "internal_version_bound_portfolio_equity",
            "cost_basis": "actual_ledger_fees",
            "internal_ledger_schema": ledger_payload["schema"],
            "internal_ledger_hash": _digest(ledger_payload),
            "allocation_semantics": ledger_payload[
                "allocation_semantics"
            ],
            "session_window": dict(session_window),
            "max_drawdown_pct": round(float(maximum_drawdown), 4),
            "portfolio_coverage_days": len(equity_curve),
            "daily_records": daily_records,
            "equity_curve": equity_curve,
            "stock_exposure_basis": ledger_payload[
                "stock_exposure_basis"
            ],
            "stock_exposure": {
                code: str(value)
                for code, value in sorted(stock_exposure.items())
            },
            "completed_trade_count": min(
                _int(item["metrics"].get("completed_trades"))
                for item in member_payloads
            ),
            "funding_evidence_revision_at": min(member_revisions),
        }
    except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
        return {"valid": False, "reason": str(exc)[:500]}


def _frozen_industry_snapshot(
    trade_date: str, stock_codes: Iterable[str],
) -> dict[str, Any]:
    """Freeze an as-of industry mapping; missing facts fail replay closed."""

    target = _trade_date(trade_date, default_today=False)
    codes = sorted({
        str(code).strip().zfill(6) for code in stock_codes
        if re.fullmatch(r"[0-9]{1,6}", str(code).strip())
    })
    rows_payload: list[dict[str, Any]] = []
    reason = "append-only行业历史已按治理交易日冻结"
    status = "COMPLETED"
    snapshot_id = ""
    if not codes:
        status, reason = "INCOMPLETE", "组合没有可冻结的持仓行业暴露"
    else:
        try:
            table_ready = _strict_table_exists(
                "st_strategy_industry_history"
            )
        except Exception:
            table_ready = False
    if codes and not table_ready:
        status, reason = "MISSING", "缺少可按交易日冻结的行业事实表或字段"
    elif codes:
        code_params = {
            f"industry_code_{index}": code
            for index, code in enumerate(codes)
        }
        placeholders = ",".join(f":{key}" for key in code_params)
        cutoff = (
            date.fromisoformat(target) + timedelta(days=1)
        ).isoformat() + "T00:00:00"
        try:
            industry_rows = _db_read(
                "SELECT snapshot_id, trade_date, as_of_exclusive, stock_code, "
                "industry_name, industry_type, source_system, source_fact_id, "
                "source_effective_at, source_etl_sync_at, row_hash "
                "FROM st_strategy_industry_history "
                f"WHERE stock_code IN ({placeholders}) "
                "AND trade_date=:industry_trade_date "
                "AND as_of_exclusive=:industry_cutoff "
                "ORDER BY stock_code, snapshot_id",
                {
                    **code_params,
                    "industry_trade_date": target,
                    "industry_cutoff": cutoff,
                },
            )
        except Exception:
            industry_rows = []
            status = "INCOMPLETE"
            reason = "行业快照查询未完成"
        seen: set[str] = set()
        snapshot_ids: set[str] = set()
        for row in industry_rows:
            code = str(row.get("stock_code") or "").strip().zfill(6)
            name = str(row.get("industry_name") or "").strip()
            source_effective_at = _normalize_evidence_revision(
                row.get("source_effective_at")
            )
            source_etl_sync_at = _normalize_evidence_revision(
                row.get("source_etl_sync_at")
            )
            observed_snapshot_id = str(row.get("snapshot_id") or "")
            row_payload = {
                "snapshot_id": observed_snapshot_id,
                "trade_date": target,
                "as_of_exclusive": cutoff,
                "stock_code": code,
                "industry_name": name,
                "industry_type": str(row.get("industry_type") or ""),
                "source_system": str(row.get("source_system") or ""),
                "source_fact_id": str(row.get("source_fact_id") or ""),
                "source_effective_at": source_effective_at,
                "source_etl_sync_at": source_etl_sync_at,
            }
            if (
                code not in codes or code in seen or not name
                or not _HASH_PATTERN.fullmatch(observed_snapshot_id)
                or not source_effective_at or not source_etl_sync_at
                or source_effective_at >= cutoff
                or source_etl_sync_at >= cutoff
                or _digest(row_payload) != str(row.get("row_hash") or "")
            ):
                status = "INVALID"
                reason = "append-only行业历史存在重复、越界或哈希漂移"
                continue
            seen.add(code)
            snapshot_ids.add(observed_snapshot_id)
            rows_payload.append({**row_payload, "row_hash": str(row.get("row_hash") or "")})
        missing = sorted(set(codes) - seen)
        if missing:
            status = "INCOMPLETE"
            reason = "治理交易日前行业快照不完整：" + "、".join(missing)
        elif len(snapshot_ids) != 1:
            status = "INVALID"
            reason = "行业历史同一治理日存在多个snapshot_id"
        else:
            snapshot_id = next(iter(snapshot_ids))
    payload = {
        "schema": "probiga.governance-industry-snapshot.v2",
        "snapshot_id": snapshot_id,
        "trade_date": target,
        "as_of_exclusive": (
            date.fromisoformat(target) + timedelta(days=1)
        ).isoformat() + "T00:00:00",
        "status": status,
        "requested_stock_codes": codes,
        "rows": sorted(rows_payload, key=lambda item: item["stock_code"]),
        "reason": reason,
    }
    return {**payload, "snapshot_hash": _digest(payload)}


def _combination_constraint_evaluation(
    combination: dict[str, Any], members: list[dict[str, Any]],
    *, trade_date: str | None = None,
    industry_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    constraints = _validated_combination_constraints(
        combination.get("constraints")
    )
    checks: list[dict[str, Any]] = []
    total = sum(item["weight"] for item in members) or 1.0
    maximum_member = max(
        (item["weight"] / total for item in members), default=1.0
    )
    checks.append({
        "name": "最大成员权重",
        "passed": maximum_member <= constraints["maximum_member_weight"] + 1e-9,
        "actual": round(maximum_member, 6),
        "limit": constraints["maximum_member_weight"],
    })
    pairwise: list[dict[str, Any]] = []
    stock_overlaps: list[dict[str, Any]] = []
    for left_index in range(len(members)):
        for right_index in range(left_index + 1, len(members)):
            left = members[left_index]
            right = members[right_index]
            left_metrics = left["strategy"]["metrics"]["60"]
            right_metrics = right["strategy"]["metrics"]["60"]
            left_daily = {
                str(row.get("trade_date") or ""): _num(
                    row.get("return_pct"), 0.0
                ) or 0.0
                for row in (left_metrics.get("internal_daily_records") or [])
            }
            right_daily = {
                str(row.get("trade_date") or ""): _num(
                    row.get("return_pct"), 0.0
                ) or 0.0
                for row in (right_metrics.get("internal_daily_records") or [])
            }
            common = sorted(set(left_daily) & set(right_daily))
            correlation = _pearson_correlation(
                [left_daily[day] for day in common],
                [right_daily[day] for day in common],
            )
            correlation_passed = (
                len(common) >= constraints["minimum_pairwise_observations"]
                and correlation is not None
                and correlation <= constraints["maximum_pairwise_correlation"]
            )
            pairwise.append({
                "left": left["strategy"]["strategy_key"],
                "right": right["strategy"]["strategy_key"],
                "observations": len(common),
                "correlation": (
                    round(correlation, 6) if correlation is not None else None
                ),
                "passed": correlation_passed,
            })
            left_exposure = {
                str(code): Decimal(str(value or "0"))
                for code, value in (
                    left_metrics.get("internal_stock_exposure") or {}
                ).items()
            }
            right_exposure = {
                str(code): Decimal(str(value or "0"))
                for code, value in (
                    right_metrics.get("internal_stock_exposure") or {}
                ).items()
            }
            left_sum = sum(left_exposure.values(), Decimal("0"))
            right_sum = sum(right_exposure.values(), Decimal("0"))
            overlap: Decimal | None = None
            if left_sum > 0 and right_sum > 0:
                overlap = sum(
                    min(
                        left_exposure.get(code, Decimal("0")) / left_sum,
                        right_exposure.get(code, Decimal("0")) / right_sum,
                    )
                    for code in set(left_exposure) | set(right_exposure)
                ) * Decimal("100")
            overlap_value = float(overlap) if overlap is not None else None
            stock_overlaps.append({
                "left": left["strategy"]["strategy_key"],
                "right": right["strategy"]["strategy_key"],
                "overlap_pct": (
                    round(overlap_value, 4)
                    if overlap_value is not None else None
                ),
                "passed": (
                    overlap_value is not None
                    and overlap_value <= (
                        constraints["maximum_stock_overlap_pct"] + 1e-9
                    )
                ),
            })
    checks.append({
        "name": "成员同步收益相关性",
        "passed": bool(pairwise) and all(row["passed"] for row in pairwise),
        "limit": constraints["maximum_pairwise_correlation"],
        "minimum_observations": constraints["minimum_pairwise_observations"],
    })
    checks.append({
        "name": "成员个股重叠",
        "passed": bool(stock_overlaps) and all(
            row["passed"] for row in stock_overlaps
        ),
        "limit_pct": constraints["maximum_stock_overlap_pct"],
    })

    combined_stock: dict[str, Decimal] = defaultdict(Decimal)
    for item in members:
        member_weight = Decimal(str(item["weight"] / total))
        exposures = {
            str(code): Decimal(str(value or "0"))
            for code, value in (
                item["strategy"]["metrics"]["60"].get(
                    "internal_stock_exposure"
                ) or {}
            ).items()
        }
        exposure_sum = sum(exposures.values(), Decimal("0"))
        if exposure_sum > 0:
            for code, value in exposures.items():
                combined_stock[code] += member_weight * value / exposure_sum
    effective_industry_snapshot = industry_snapshot
    if effective_industry_snapshot is None and trade_date:
        effective_industry_snapshot = _frozen_industry_snapshot(
            trade_date, combined_stock
        )
    effective_industry_snapshot = (
        effective_industry_snapshot
        if isinstance(effective_industry_snapshot, dict) else {}
    )
    snapshot_payload = {
        str(key): value
        for key, value in effective_industry_snapshot.items()
        if str(key) != "snapshot_hash"
    }
    expected_industry_codes = sorted(combined_stock)
    expected_cutoff = (
        date.fromisoformat(_trade_date(trade_date, default_today=False))
        + timedelta(days=1)
    ).isoformat() + "T00:00:00" if trade_date else ""
    industry_rows = effective_industry_snapshot.get("rows")
    industry_rows = industry_rows if isinstance(industry_rows, list) else []
    observed_codes = [
        str(row.get("stock_code") or "")
        for row in industry_rows if isinstance(row, dict)
    ]
    snapshot_id = str(effective_industry_snapshot.get("snapshot_id") or "")
    row_contract_valid = bool(industry_rows)
    for row in industry_rows:
        if not isinstance(row, dict):
            row_contract_valid = False
            break
        row_hash = str(row.get("row_hash") or "")
        row_payload = {
            str(key): value for key, value in row.items()
            if str(key) != "row_hash"
        }
        source_effective = _normalize_evidence_revision(
            row.get("source_effective_at")
        )
        source_sync = _normalize_evidence_revision(
            row.get("source_etl_sync_at")
        )
        if (
            set(row) != {
                "snapshot_id", "trade_date", "as_of_exclusive",
                "stock_code", "industry_name", "industry_type",
                "source_system", "source_fact_id", "source_effective_at",
                "source_etl_sync_at", "row_hash",
            }
            or str(row.get("snapshot_id") or "") != snapshot_id
            or str(row.get("trade_date") or "")
            != _trade_date(trade_date, default_today=False)
            or str(row.get("as_of_exclusive") or "") != expected_cutoff
            or not source_effective or not source_sync
            or source_effective >= expected_cutoff
            or source_sync >= expected_cutoff
            or not _HASH_PATTERN.fullmatch(row_hash)
            or _digest(row_payload) != row_hash
        ):
            row_contract_valid = False
            break
    industry_snapshot_valid = bool(
        set(effective_industry_snapshot) == {
            "schema", "snapshot_id", "trade_date", "as_of_exclusive",
            "status", "requested_stock_codes", "rows", "reason",
            "snapshot_hash",
        }
        and effective_industry_snapshot.get("schema")
        == "probiga.governance-industry-snapshot.v2"
        and
        effective_industry_snapshot.get("status") == "COMPLETED"
        and trade_date
        and str(effective_industry_snapshot.get("trade_date") or "")
        == _trade_date(trade_date, default_today=False)
        and _HASH_PATTERN.fullmatch(str(
            effective_industry_snapshot.get("snapshot_hash") or ""
        ))
        and _digest(snapshot_payload)
        == str(effective_industry_snapshot.get("snapshot_hash") or "")
        and _HASH_PATTERN.fullmatch(snapshot_id)
        and str(effective_industry_snapshot.get("as_of_exclusive") or "")
        == expected_cutoff
        and effective_industry_snapshot.get("requested_stock_codes")
        == expected_industry_codes
        and observed_codes == expected_industry_codes
        and len(observed_codes) == len(set(observed_codes))
        and row_contract_valid
    )
    industry_by_code = {
        str(row.get("stock_code") or ""): str(
            row.get("industry_name") or ""
        )
        for row in (effective_industry_snapshot.get("rows") or [])
        if isinstance(row, dict)
    } if industry_snapshot_valid else {}
    industry_weights: dict[str, Decimal] = defaultdict(Decimal)
    missing_industry: list[str] = []
    for code, weight in combined_stock.items():
        industry = industry_by_code.get(code)
        if industry:
            industry_weights[industry] += weight
        else:
            missing_industry.append(code)
    maximum_industry = max(industry_weights.values(), default=Decimal("0"))
    checks.append({
        "name": "单一行业集中度",
        "passed": (
            industry_snapshot_valid
            and
            bool(industry_weights)
            and not missing_industry
            and float(maximum_industry * Decimal("100"))
            <= constraints["maximum_industry_weight_pct"] + 1e-9
        ),
        "actual_pct": round(float(maximum_industry * Decimal("100")), 4),
        "limit_pct": constraints["maximum_industry_weight_pct"],
        "missing_industry_codes": sorted(missing_industry),
        "industry_snapshot_valid": industry_snapshot_valid,
        "industry_snapshot_reason": str(
            effective_industry_snapshot.get("reason")
            or "缺少治理交易日冻结行业快照"
        ),
        "industry_snapshot_hash": str(
            effective_industry_snapshot.get("snapshot_hash") or ""
        ),
    })
    payload = {
        "schema": "probiga.combination-constraint-evaluation.v1",
        "combination_key": combination["combination_key"],
        "combination_version": combination["current_version"],
        "constraints": constraints,
        "checks": checks,
        "pairwise_correlations": pairwise,
        "pairwise_stock_overlaps": stock_overlaps,
        "industry_snapshot": effective_industry_snapshot,
        "industry_weights_pct": {
            key: round(float(value * Decimal("100")), 4)
            for key, value in sorted(industry_weights.items())
        },
    }
    passed = bool(checks) and all(check["passed"] for check in checks)
    return {
        **payload,
        "passed": passed,
        "evaluation_hash": _digest(payload),
    }


def _combination_rankings(
    combinations: list[dict[str, Any]],
    strategies: list[dict[str, Any]],
    metric_inputs: dict[tuple[str, int], dict[str, Any]],
    trade_date: str,
) -> list[dict[str, Any]]:
    by_key = {row["strategy_key"]: row for row in strategies}
    target = date.fromisoformat(trade_date)
    rows = []
    lane_order = {
        "ACTIVE": 0, "REDUCE": 0, "SHADOW": 1,
        "SUSPENDED": 2, "RETIRED": 3,
    }
    for combo in combinations:
        members = []
        missing = []
        version_mismatches = []
        for raw in combo.get("members") or []:
            member_key = str(raw.get("strategy_key") or "")
            member = by_key.get(member_key)
            if member is None:
                missing.append(member_key)
                continue
            frozen_version = str(raw.get("strategy_version") or "")
            version_match = bool(
                frozen_version
                and frozen_version == str(member["current_version"])
            )
            if not version_match:
                version_mismatches.append({
                    "strategy_key": member_key,
                    "frozen_version": frozen_version,
                    "current_version": str(member["current_version"]),
                })
            members.append({
                "weight": _num(raw.get("weight"), 0.0) or 0.0,
                "strategy": member,
                "frozen_version": frozen_version,
                "version_match": version_match,
            })
        total = sum(item["weight"] for item in members) or 1.0
        member_sleeve_risk_multiplier = sum(
            item["weight"]
            * LIFECYCLE_RISK_MULTIPLIER.get(
                str(item["strategy"].get("current_status") or ""), 0.0
            )
            for item in members
        ) / total if members else 0.0
        market_route = _combination_market_route(
            combo, members, version_mismatches, trade_date
        )
        score = sum(item["weight"] * item["strategy"]["ranking_score"] for item in members) / total if members else 0.0
        primary_values = [item["strategy"]["primary_metrics"] for item in members]
        samples = min((_int(value.get("completed_trades")) for value in primary_values), default=0)
        net = sum(item["weight"] * (_num(item["strategy"]["primary_metrics"].get("net_expectancy_pct"), 0.0) or 0.0) for item in members) / total if members else None
        pf_values = [_num(value.get("profit_factor"), None) for value in primary_values]
        pf = sum(item["weight"] * (_num(item["strategy"]["primary_metrics"].get("profit_factor"), 0.0) or 0.0) for item in members) / total if members and all(value is not None for value in pf_values) else None
        drawdown = sum(item["weight"] * (_num(item["strategy"]["primary_metrics"].get("max_drawdown_pct"), 0.0) or 0.0) for item in members) / total if members else None
        concentration = max((item["weight"] / total for item in members), default=1.0)
        member_aggregate = {
            "completed_trades": samples,
            "net_expectancy_pct": round(net, 4) if net is not None else None,
            "profit_factor": round(pf, 4) if pf is not None else None,
            "max_drawdown_pct": round(drawdown, 4) if drawdown is not None else None,
        }
        window_metrics: dict[int, dict[str, Any]] = {}
        internal_combo_ledgers: dict[int, dict[str, Any]] = {}
        for window in WINDOWS:
            evidence = metric_inputs.get((combo["combination_key"], window))
            internal_ledger = _internal_combination_portfolio_ledger(
                combo, members, window=window, trade_date=trade_date
            )
            internal_combo_ledgers[window] = internal_ledger
            if internal_ledger.get("valid") is True:
                metrics = calculate_return_metrics(
                    internal_ledger["daily_records"],
                    window_days=window,
                    market_match_score=market_route["market_match_score"],
                    version_bound_evidence=True,
                    independent_oos=True,
                    walk_forward_verified=False,
                )
                metrics.update({
                    "completed_trades": internal_ledger[
                        "completed_trade_count"
                    ],
                    "coverage_days": internal_ledger[
                        "portfolio_coverage_days"
                    ],
                    "funding_provenance": internal_ledger[
                        "funding_provenance"
                    ],
                    "drawdown_basis": internal_ledger["drawdown_basis"],
                    "cost_basis": internal_ledger["cost_basis"],
                    "max_drawdown_pct": internal_ledger[
                        "max_drawdown_pct"
                    ],
                    "portfolio_coverage_days": internal_ledger[
                        "portfolio_coverage_days"
                    ],
                    "internal_ledger_hash": internal_ledger[
                        "internal_ledger_hash"
                    ],
                    "internal_ledger_schema": internal_ledger[
                        "internal_ledger_schema"
                    ],
                    "internal_daily_records": internal_ledger[
                        "daily_records"
                    ],
                    "internal_equity_curve": internal_ledger[
                        "equity_curve"
                    ],
                    "internal_stock_exposure": internal_ledger[
                        "stock_exposure"
                    ],
                    "internal_stock_exposure_basis": internal_ledger[
                        "stock_exposure_basis"
                    ],
                    "evidence_revision_at": internal_ledger[
                        "funding_evidence_revision_at"
                    ],
                    "session_window_valid": (
                        internal_ledger["session_window"].get("valid") is True
                    ),
                    "session_window_start": internal_ledger[
                        "session_window"
                    ].get("start_date"),
                    "session_window_end": internal_ledger[
                        "session_window"
                    ].get("end_date"),
                    "session_window_count": internal_ledger[
                        "session_window"
                    ].get("session_count"),
                    "session_window_hash": internal_ledger[
                        "session_window"
                    ].get("session_hash"),
                    "source": "internal_combination_virtual_nav",
                })
                if evidence is not None:
                    internal_trade_evidence_hash = str(
                        metrics.get("evidence_hash") or ""
                    )
                    selection_evidence_hash = str(
                        evidence.get("evidence_hash") or ""
                    )
                    for field in (
                        "evidence_protocol", "artifact_hash",
                        "source_dataset_hash", "verification_status",
                        "submitted_by", "reviewed_by", "reviewed_at",
                        "review_audit_valid",
                    ):
                        metrics[field] = evidence.get(field)
                    metrics["walk_forward_verified"] = (
                        evidence.get("walk_forward_verified") is True
                    )
                    metrics["walk_forward_segments"] = _int(
                        evidence.get("walk_forward_segments")
                    )
                    metrics["positive_segments"] = _int(
                        evidence.get("positive_segments")
                    )
                    metrics["selection_validation_completed_trades"] = _int(
                        evidence.get("completed_trades")
                    )
                    metrics["selection_validation_coverage_days"] = _int(
                        evidence.get("coverage_days")
                    )
                    metrics["selection_validation_revision_at"] = str(
                        evidence.get("evidence_revision_at") or ""
                    )
                    metrics["selection_validation_independent_oos"] = (
                        evidence.get("independent_oos") is True
                    )
                    metrics["selection_validation_scope"] = (
                        "VERSION_SELECTION_ONLY"
                    )
                    metrics["internal_trade_evidence_hash"] = (
                        internal_trade_evidence_hash
                    )
                    metrics["selection_evidence_hash"] = (
                        selection_evidence_hash
                    )
                    metrics["evidence_hash"] = _digest({
                        "internal_trade_evidence_hash": (
                            internal_trade_evidence_hash
                        ),
                        "internal_ledger_hash": metrics[
                            "internal_ledger_hash"
                        ],
                        "selection_evidence_hash": selection_evidence_hash,
                        "selection_artifact_hash": metrics.get(
                            "artifact_hash"
                        ),
                        "combination_key": combo["combination_key"],
                        "combination_version": combo["current_version"],
                        "window_days": window,
                    })
            elif evidence is None:
                metrics = {
                    "window_days": window,
                    "completed_trades": 0,
                    "source": "missing_combination_evidence",
                    "funding_provenance": "INTERNAL_LEDGER_INVALID",
                    "internal_ledger_reason": str(
                        internal_ledger.get("reason")
                        or "组合内部虚拟净值不可用"
                    ),
                    "version_bound_evidence": False,
                    "independent_oos": False,
                    "walk_forward_verified": False,
                    "evidence_fresh": False,
                }
            else:
                metrics = {"window_days": window, **evidence}
                metrics["funding_provenance"] = "EXTERNAL_SUBMITTED"
                metrics["internal_ledger_reason"] = str(
                    internal_ledger.get("reason")
                    or "组合内部虚拟净值不可用"
                )
                metrics.setdefault(
                    "estimated_cost_pct", DEFAULT_ROUND_TRIP_COST_PCT
                )
            evidence_date = str(
                metrics.get("evidence_revision_at")
                or metrics.get("as_of_date")
                or ""
            )[:10]
            try:
                evidence_age = (
                    target - date.fromisoformat(evidence_date)
                ).days
            except ValueError:
                evidence_age = 999999
            metrics["evidence_age_days"] = evidence_age
            metrics["evidence_fresh"] = (
                0 <= evidence_age
                <= PROFIT_GATE_POLICY["maximum_evidence_age_days"]
            )
            selection_date = str(
                metrics.get("selection_validation_revision_at") or ""
            )[:10]
            try:
                selection_age = (
                    target - date.fromisoformat(selection_date)
                ).days
            except ValueError:
                selection_age = 999999
            metrics["selection_validation_fresh"] = (
                0 <= selection_age
                <= PROFIT_GATE_POLICY["maximum_evidence_age_days"]
            )
            metrics["market_match_score"] = market_route["market_match_score"]
            metrics["market_route_hash"] = market_route["router_decision_hash"]
            metrics["health_score"] = calculate_health_score(metrics)
            metrics["profit_gate"] = evaluate_window_gate(metrics)
            window_metrics[window] = metrics
        short = window_metrics[20]
        independent_gate = window_metrics[60]["profit_gate"]
        long_gate = window_metrics[120]["profit_gate"]
        short_gate = short["profit_gate"]
        short_gate_passed = bool(short_gate["passed"])
        has_all_independent_evidence = all(
            internal_combo_ledgers[window].get("valid") is True
            and metric_inputs.get((combo["combination_key"], window)) is not None
            for window in WINDOWS
        )
        independent_health = round(
            window_metrics[20]["health_score"] * 0.25
            + window_metrics[60]["health_score"] * 0.50
            + window_metrics[120]["health_score"] * 0.25,
            2,
        )
        member_gate_passed = (
            bool(members)
            and not missing
            and not version_mismatches
            and all(
                item["strategy"]["profit_gate_passed"]
                and item["strategy"]["current_status"] in {"ACTIVE", "REDUCE"}
                for item in members
            )
        )
        constraint_evaluation = _combination_constraint_evaluation(
            combo, members, trade_date=trade_date
        )
        concentration_penalty = max(0.0, concentration - 0.5) * 20.0
        base_score = independent_health if has_all_independent_evidence else score
        adjusted_score = max(0.0, base_score - concentration_penalty)
        profit_gate_passed = bool(
            combo.get("enabled")
            and has_all_independent_evidence
            and short_gate_passed
            and independent_gate["passed"]
            and long_gate["passed"]
            and member_gate_passed
            and constraint_evaluation["passed"]
            and combo.get("config_integrity_valid") is True
        )
        status = combo["current_status"]
        funding_gate_hash = _digest({
            "combination_key": combo["combination_key"],
            "combination_version": combo["current_version"],
            "window_evidence": {
                str(window): window_metrics[window].get("evidence_hash")
                for window in WINDOWS
            },
            "member_versions": {
                item["strategy"]["strategy_key"]: {
                    "frozen": item["frozen_version"],
                    "current": item["strategy"]["current_version"],
                    "lifecycle_status": str(
                        item["strategy"].get("current_status") or ""
                    ),
                    "lifecycle_risk_multiplier": (
                        LIFECYCLE_RISK_MULTIPLIER.get(
                            str(item["strategy"].get("current_status") or ""),
                            0.0,
                        )
                    ),
                }
                for item in members
            },
            "member_sleeve_risk_multiplier": round(
                member_sleeve_risk_multiplier, 8
            ),
            "router_decision_hash": market_route["router_decision_hash"],
            "constraint_evaluation_hash": constraint_evaluation[
                "evaluation_hash"
            ],
            "profit_gate_passed": profit_gate_passed,
        })
        funding_evidence_revision_at = _funding_evidence_revision_at(
            window_metrics
        )
        if status == "RETIRED":
            recommended = "RETIRED"
            recommendation_reason = "已淘汰组合保持终态；只能注册新版本重新验证"
        elif not combo.get("enabled"):
            recommended = "SUSPENDED"
            recommendation_reason = "组合已禁用"
        elif version_mismatches:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else "SHADOW"
            recommendation_reason = "组合成员版本已变化，必须注册新组合版本并重新积累独立证据"
        elif not has_all_independent_evidence:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else status
            recommendation_reason = "缺少组合20/60/120日独立前向证据，成员加权结果不能替代组合验证"
        elif combo.get("config_integrity_valid") is not True:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else "SHADOW"
            recommendation_reason = "组合不可变版本配置哈希校验失败"
        elif not constraint_evaluation["passed"]:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else "SHADOW"
            failed_names = [
                check["name"] for check in constraint_evaluation["checks"]
                if not check["passed"]
            ]
            recommendation_reason = "组合风险约束未通过：" + "、".join(failed_names)
        elif not member_gate_passed:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else "SHADOW"
            recommendation_reason = "组合成员尚未全部取得模拟资金资格"
        elif profit_gate_passed:
            recommended = "ACTIVE" if adjusted_score >= 80 else "REDUCE"
            recommendation_reason = f"组合多窗口盈利硬门槛通过，健康分{adjusted_score:.1f}"
        else:
            recommended = "SUSPENDED" if status in {"ACTIVE", "REDUCE"} else "SHADOW"
            recommendation_reason = "组合多窗口盈利硬门槛未全部通过"
        gate_reason = (
            "组合20/60/120日独立盈利门槛和全部成员门槛通过"
            if profit_gate_passed
            else recommendation_reason
        )
        rows.append({
            **combo,
            "lane": (
                "正式赛道" if status in {"ACTIVE", "REDUCE"}
                else "观察赛道" if status == "SHADOW"
                else "暂停赛道" if status == "SUSPENDED"
                else "历史档案"
            ),
            "ranking_score": round(adjusted_score, 2),
            "ranking_basis": DAILY_NAV_RANKING_BASIS,
            "ranking_basis_label": DAILY_NAV_RANKING_BASIS_LABEL,
            "member_sleeve_risk_multiplier": round(
                member_sleeve_risk_multiplier, 8
            ),
            "member_sleeve_discount_pct": round(
                (1.0 - member_sleeve_risk_multiplier) * 100.0, 4
            ),
            "member_details": [{"strategy_key": item["strategy"]["strategy_key"], "strategy_name": item["strategy"]["strategy_name"], "strategy_version": item["frozen_version"], "current_strategy_version": item["strategy"]["current_version"], "version_match": item["version_match"], "weight": round(item["weight"] / total, 8), "status_label": item["strategy"]["status_label"], "lifecycle_status": str(item["strategy"].get("current_status") or ""), "lifecycle_risk_multiplier": LIFECYCLE_RISK_MULTIPLIER.get(str(item["strategy"].get("current_status") or ""), 0.0), "effective_weight_after_lifecycle": round(item["weight"] / total * LIFECYCLE_RISK_MULTIPLIER.get(str(item["strategy"].get("current_status") or ""), 0.0), 6), "contribution_score": round(item["weight"] / total * item["strategy"]["ranking_score"], 2)} for item in members],
            "metrics": {str(window): window_metrics[window] for window in WINDOWS},
            "primary_metrics": window_metrics[60],
            "win_rate_pct": window_metrics[60].get("win_rate_pct"),
            "payoff_ratio": window_metrics[60].get("payoff_ratio"),
            "profit_factor": window_metrics[60].get("profit_factor"),
            "net_expectancy_pct": window_metrics[60].get(
                "net_expectancy_pct"
            ),
            "member_aggregate_metrics": member_aggregate,
            "has_independent_evidence": has_all_independent_evidence,
            "profit_gate_passed": profit_gate_passed,
            "multi_window_gate": {
                "20": short_gate,
                "60": independent_gate,
                "120": long_gate,
            },
            "funding_gate_hash": funding_gate_hash,
            "funding_evidence_revision_at": funding_evidence_revision_at,
            "market_route": market_route,
            "market_route_eligible": market_route["eligible"],
            "market_route_reason": market_route["reason"],
            "constraint_evaluation": constraint_evaluation,
            "correlation_status": (
                "相关性与重叠约束已通过"
                if constraint_evaluation["passed"]
                else "相关性或重叠约束未通过"
            ),
            "paper_allocation_eligible": (
                profit_gate_passed
                and status in {"ACTIVE", "REDUCE"}
                and market_route["eligible"]
                and constraint_evaluation["passed"]
            ),
            "gate_reason": gate_reason,
            "recommended_status": recommended,
            "recommended_status_label": LIFECYCLE_LABELS[recommended],
            "recommendation_reason": recommendation_reason,
            "missing_members": missing,
            "member_version_mismatches": version_mismatches,
            "real_order_authority": False,
        })
    rows.sort(key=lambda row: (
        lane_order.get(row["current_status"], 9),
        -float(row["ranking_score"]),
        row["combination_key"],
    ))
    lane_ranks: dict[str, int] = defaultdict(int)
    for index, row in enumerate(rows, 1):
        row["rank"] = index
        lane_ranks[row["lane"]] += 1
        row["lane_rank"] = lane_ranks[row["lane"]]
    return rows


def governance_input_ready(snapshot: dict[str, Any]) -> tuple[bool, str]:
    source_status = str(snapshot.get("source_status") or "")
    if source_status != "fresh":
        return False, f"底层票池来源状态为{source_status or 'missing'}"
    if snapshot.get("is_stale") is True:
        return False, "底层票池已过期"
    trade_raw = snapshot.get("trade_date")
    data_raw = snapshot.get("data_date")
    if not str(trade_raw or "").strip() or not str(data_raw or "").strip():
        return False, "底层票池缺少交易日或数据日"
    try:
        trade_day = _trade_date(trade_raw, default_today=False)
        data_day = _trade_date(data_raw, default_today=False)
    except ValueError:
        return False, "底层票池交易日或数据日格式无效"
    if data_day != trade_day:
        return False, "底层票池交易日与数据日不一致"
    candidate_source = snapshot.get("candidate_source")
    if not isinstance(candidate_source, dict):
        return False, "候选源缺少完成证明，不能把空结果视为合法票池"
    if (
        str(candidate_source.get("status") or "") != "COMPLETED"
        or candidate_source.get("query_completed") is not True
    ):
        return False, str(
            candidate_source.get("reason")
            or "候选源尚未完成，禁止更新生命周期和模拟资金"
        )
    source_hash = str(candidate_source.get("source_hash") or "")
    source_schema = str(candidate_source.get("schema") or "")
    excluded_source_hash_fields = {"source_hash"}
    if source_schema == "probiga.strategy-candidate-source.v2":
        excluded_source_hash_fields.add("dynamic_adapter_receipts")
    source_payload = {
        str(key): value
        for key, value in candidate_source.items()
        if str(key) not in excluded_source_hash_fields
    }
    if (
        not _HASH_PATTERN.fullmatch(source_hash)
        or _digest(source_payload) != source_hash
    ):
        return False, "候选源完成证明哈希无效"
    try:
        source_trade_date = _trade_date(
            candidate_source.get("trade_date"), default_today=False,
        )
        source_data_date = _trade_date(
            candidate_source.get("data_date"), default_today=False,
        )
    except ValueError:
        return False, "候选源完成证明日期无效"
    if source_trade_date != trade_day or source_data_date != data_day:
        return False, "候选源完成证明与治理交易日不一致"
    candidates = snapshot.get("candidates")
    if not isinstance(candidates, list):
        return False, "候选源结果不是列表"
    if (
        _int(candidate_source.get("source_row_count"), -1) < 0
        or _int(candidate_source.get("loaded_row_count"), -1) < 0
        or _int(candidate_source.get("loaded_row_count"), -1)
        != _int(candidate_source.get("source_row_count"), -2)
        or _int(candidate_source.get("candidate_count"), -1)
        != len(candidates)
    ):
        return False, "候选源完成证明行数与实际票池不一致"
    loaded_rows_hash = str(candidate_source.get("loaded_rows_hash") or "")
    if not _HASH_PATTERN.fullmatch(loaded_rows_hash):
        return False, "候选源完成证明缺少完整明细哈希"
    actual_identity = sorted({
        str(item.get("stock_code") or "").strip().zfill(6)
        for item in candidates if isinstance(item, dict)
        and str(item.get("stock_code") or "").strip()
    })
    declared_identity = candidate_source.get("candidate_identity")
    if (
        not isinstance(declared_identity, list)
        or [str(value) for value in declared_identity] != actual_identity
    ):
        return False, "候选源完成证明证券身份与实际票池不一致"
    runtime_statuses = [
        item for item in (snapshot.get("dynamic_adapter_statuses") or [])
        if isinstance(item, dict)
        and str(item.get("strategy_key") or "")
        and str(item.get("adapter_capability_status") or "")
        == "RESEARCH_READY"
        and item.get("enabled") is True
        and str(item.get("lifecycle_status") or "")
        not in {"RETIRED", "SUSPENDED"}
    ]
    receipts = candidate_source.get("dynamic_adapter_receipts")
    if runtime_statuses:
        if not isinstance(receipts, list) or len(receipts) != len(runtime_statuses):
            return False, "动态策略候选源缺少逐策略运行回执"
        results = candidate_source.get("dynamic_adapter_results")
        results_hash = str(
            candidate_source.get("dynamic_adapter_results_hash") or ""
        )
        if (
            source_schema != "probiga.strategy-candidate-source.v2"
            or not isinstance(results, list)
            or len(results) != len(runtime_statuses)
            or not _HASH_PATTERN.fullmatch(results_hash)
            or _digest(results) != results_hash
        ):
            return False, "动态策略候选稳定结果集合哈希无效"
        by_identity = {
            (
                str(item.get("strategy_key") or ""),
                str(item.get("strategy_version") or ""),
            ): item
            for item in receipts if isinstance(item, dict)
        }
        for runtime in runtime_statuses:
            identity = (
                str(runtime.get("strategy_key") or ""),
                str(runtime.get("strategy_version") or ""),
            )
            receipt = by_identity.get(identity)
            if not isinstance(receipt, dict):
                return False, "动态策略候选运行回执身份缺失"
            try:
                verified_receipt = validate_strategy_adapter_run_receipt(receipt)
            except ValueError:
                return False, "动态策略CandidateBatch运行回执无效或字段不精确"
            receipt_hash = str(verified_receipt.get("receipt_hash") or "")
            result = next((
                item for item in results
                if isinstance(item, dict)
                and str(item.get("strategy_key") or "") == identity[0]
                and str(item.get("strategy_version") or "") == identity[1]
            ), None)
            if (
                not isinstance(result, dict)
                or receipt_hash != str(runtime.get("candidate_receipt_hash") or "")
                or str(receipt.get("execution_binding_hash") or "")
                != str(runtime.get("execution_binding_hash") or "")
                or str(receipt.get("adapter_artifact_sha256") or "")
                != str(runtime.get("adapter_artifact_sha256") or "")
                or str(receipt.get("cost_model_hash") or "")
                != str(runtime.get("cost_model_hash") or "")
                or str(receipt.get("trade_date") or "") != trade_day
                or str(receipt.get("input_hash") or "")
                != str(result.get("candidate_input_hash") or "")
                or str(receipt.get("output_hash") or "")
                != str(result.get("candidate_output_hash") or "")
                or str(receipt.get("stable_result_hash") or "")
                != str(result.get("candidate_stable_result_hash") or "")
                or _int(receipt.get("candidate_count"), -1)
                != _int(result.get("candidate_count"), -2)
            ):
                return False, "动态策略CandidateBatch运行回执无效或身份不一致"
    return True, "底层票池数据新鲜且日期一致"


def _snapshot_trading_gate(snapshot: dict[str, Any]) -> dict[str, Any]:
    ready, input_reason = governance_input_ready(snapshot)
    gate = snapshot.get("global_gate")
    gate_status = (
        str(gate.get("status") or "").upper()
        if isinstance(gate, dict) else "MISSING"
    )
    gate_reason = (
        str(gate.get("reason") or "")
        if isinstance(gate, dict) else "缺少全局市场门禁"
    )
    market = snapshot.get("market_state")
    market = market if isinstance(market, dict) else {}
    market_state = str(market.get("key") or "unknown")
    risk_cap = MARKET_RISK_CAP_PCT.get(market_state, 0.0)
    allowed_statuses = {"ALLOW_NEW_BUY", "REDUCE_NEW_BUY"}
    trading_allowed = bool(
        ready and gate_status in allowed_statuses and risk_cap > 0
    )
    if not ready:
        reason = input_reason
    elif gate_status not in allowed_statuses:
        reason = gate_reason or f"全局门禁{gate_status or 'MISSING'}阻断新增模拟资金"
    elif risk_cap <= 0:
        reason = "当前市场状态风险上限为0，禁止新增模拟资金"
    elif gate_status == "REDUCE_NEW_BUY":
        reason = f"市场降权门禁允许模拟资金候选，总风险上限{risk_cap:.2f}%"
    else:
        reason = f"市场门禁允许模拟资金候选，总风险上限{risk_cap:.2f}%"
    return {
        "schema": "probiga.strategy-trading-gate.v1",
        "status": gate_status,
        "status_label": (
            "降权允许新增模拟资金"
            if gate_status == "REDUCE_NEW_BUY"
            else "允许新增模拟资金"
            if gate_status == "ALLOW_NEW_BUY"
            else "禁止新增模拟资金"
        ),
        "input_ready": ready,
        "trading_allowed": trading_allowed,
        "market_state": market_state,
        "market_risk_cap_pct": risk_cap,
        "reason": reason,
        "candidate_source_hash": str(
            (snapshot.get("candidate_source") or {}).get("source_hash") or ""
        ),
    }


def _snapshot_trading_allowed(snapshot: dict[str, Any]) -> bool:
    return _snapshot_trading_gate(snapshot)["trading_allowed"] is True


def _pool_score_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value)).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_EVEN
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("股票池评分无法规范化") from exc
    if not parsed.is_finite():
        raise ValueError("股票池评分必须为有限数值")
    return format(parsed, ".4f")


def _pool_runtime_row_contract(
    trade_date: str,
    pool_level: str,
    row: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    level = str(pool_level or "")
    if level not in {"OBSERVATION", "CONFIRMATION", "TRADABLE"}:
        raise ValueError("股票池层级无效")
    stock_code = str(row.get("stock_code") or "").strip()
    rank_no = _int(row.get("rank"), 0)
    strategies = row.get("strategies") or []
    reason = {
        "reason": str(row.get("reason") or ""),
        "blocking_reasons": [
            str(value) for value in (row.get("blocking_reasons") or [])
        ],
    }
    evidence = row.get("evidence") or {}
    if (
        not stock_code
        or rank_no <= 0
        or not isinstance(strategies, list)
        or not isinstance(evidence, dict)
    ):
        raise ValueError("股票池行身份、排名、策略或证据无效")
    payload = {
        "schema": POOL_ROW_SCHEMA,
        "trade_date": _trade_date(trade_date, default_today=False),
        "pool_level": level,
        "stock_code": stock_code,
        "stock_name": str(row.get("stock_name") or ""),
        "rank_no": rank_no,
        "opportunity_score": _pool_score_text(
            row.get("opportunity_score")
        ),
        "execution_score": _pool_score_text(row.get("execution_score")),
        "dominant_strategy": str(row.get("dominant_strategy") or ""),
        "strategies": [str(value) for value in strategies],
        "industry_name": str(row.get("industry_name") or ""),
        "industry_names": sorted({
            str(value) for value in (row.get("industry_names") or [])
            if str(value)
        }),
        "industry_by_strategy": {
            str(key): str(value)
            for key, value in sorted(
                (row.get("industry_by_strategy") or {}).items()
            )
            if str(key) and str(value)
        },
        "gate_status": str(row.get("gate_status") or "观察"),
        "reason": reason,
        "evidence": evidence,
    }
    return payload, _digest(payload)


def _pool_snapshot_contract(
    trade_date: str,
    pools: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], str, dict[tuple[str, str], str]]:
    level_names = (
        ("OBSERVATION", "observation"),
        ("CONFIRMATION", "confirmation"),
        ("TRADABLE", "tradable"),
    )
    row_contracts: list[dict[str, Any]] = []
    row_hashes: dict[tuple[str, str], str] = {}
    for level, pool_name in level_names:
        for row in pools.get(pool_name) or []:
            payload, row_hash = _pool_runtime_row_contract(
                trade_date, level, row
            )
            identity = (level, payload["stock_code"])
            if identity in row_hashes:
                raise ValueError("股票池同层股票身份重复")
            row_hashes[identity] = row_hash
            row_contracts.append(
                {
                    "pool_level": level,
                    "rank_no": payload["rank_no"],
                    "stock_code": payload["stock_code"],
                    "pool_row_hash": row_hash,
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
        "trade_date": _trade_date(trade_date, default_today=False),
        "row_count": len(row_contracts),
        "rows": row_contracts,
    }
    return snapshot_payload, _digest(snapshot_payload), row_hashes


def _automatic_transition_plan_contract(
    trade_date: str,
    run_uid: str,
    plans: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    target = _trade_date(trade_date, default_today=False)
    normalized_run_uid = str(run_uid or "")
    if not re.fullmatch(r"[0-9a-f]{32}", normalized_run_uid):
        raise ValueError("自动生命周期计划缺少有效治理运行编号")
    transitions: list[dict[str, Any]] = []
    for plan in plans:
        evidence = plan.get("evidence")
        decision_evidence = (
            {
                str(key): value
                for key, value in evidence.items()
                if str(key) != "run_uid"
            }
            if isinstance(evidence, dict)
            else None
        )
        entry = {
            "entity_type": str(plan.get("entity_type") or ""),
            "entity_key": str(plan.get("entity_key") or ""),
            "entity_version": str(plan.get("entity_version") or ""),
            "previous_status": str(plan.get("previous_status") or ""),
            "next_status": str(plan.get("next_status") or ""),
            "reason": str(plan.get("reason") or ""),
            # run_uid is deliberately excluded from the decision-bound view.
            # It is random per attempt and remains bound independently by the
            # immutable lifecycle event/audit written for the accepted run.
            "evidence": decision_evidence,
        }
        if (
            entry["entity_type"] not in {"STRATEGY", "COMBINATION"}
            or not entry["entity_key"]
            or not entry["entity_version"]
            or entry["previous_status"] not in LIFECYCLE_LABELS
            or entry["next_status"] not in LIFECYCLE_LABELS
            or not entry["reason"]
            or not isinstance(evidence, dict)
            or str(evidence.get("run_uid") or "") != normalized_run_uid
            or _trade_date(
                evidence.get("trade_date"), default_today=False
            )
            != target
        ):
            raise ValueError("自动生命周期计划字段或运行绑定无效")
        transitions.append(entry)
    transitions.sort(key=lambda item: (
        item["entity_type"],
        item["entity_key"],
        item["entity_version"],
        item["previous_status"],
        item["next_status"],
        _digest(item),
    ))
    payload = {
        "schema": AUTOMATIC_TRANSITION_PLAN_SCHEMA,
        "trade_date": target,
        "transition_count": len(transitions),
        "transitions": transitions,
    }
    return payload, _digest(payload)


def _build_pools(
    snapshot: dict[str, Any], strategies: list[dict[str, Any]],
) -> dict[str, Any]:
    strategy_map = {row["strategy_key"]: row for row in strategies}
    trading_gate = _snapshot_trading_gate(snapshot)
    trading_allowed = trading_gate["trading_allowed"] is True
    observation = []
    confirmation = []
    tradable = []
    for candidate in snapshot.get("candidates") or []:
        raw_keys = [str(key) for key in candidate.get("strategies") or []]
        keys = list(dict.fromkeys(raw_keys))
        # Candidate-level scores/status/reason may have been aggregated from
        # every raw contributor.  Silently deleting an ineligible contributor
        # while retaining those aggregate fields would let a suspended or
        # forged strategy lend its confidence/payoff to an active strategy.
        # Without a separately hash-bound per-signal recomputation, the whole
        # contaminated candidate must therefore stay out of every pool.
        if not keys or any(
            key not in strategy_map
            or strategy_map[key].get("execution_adapter_executable") is not True
            or strategy_map[key].get("enabled") is not True
            or str(strategy_map[key].get("current_status") or "")
            not in {"ACTIVE", "REDUCE", "SHADOW"}
            for key in keys
        ):
            continue
        member_rows = [strategy_map[key] for key in keys if key in strategy_map]
        dominant_key = str(candidate.get("dominant_strategy") or "")
        if dominant_key not in keys:
            dominant_key = keys[0]
        dominant_row = strategy_map.get(dominant_key)
        raw_industry_by_strategy = candidate.get("industry_by_strategy")
        raw_industry_by_strategy = (
            raw_industry_by_strategy
            if isinstance(raw_industry_by_strategy, dict) else {}
        )
        industry_by_strategy = {
            key: str(raw_industry_by_strategy.get(key) or "").strip()
            for key in keys
            if str(raw_industry_by_strategy.get(key) or "").strip()
        }
        fallback_industry = str(
            candidate.get("industry_name") or candidate.get("industry")
            or candidate.get("theme_code") or ""
        ).strip()
        for key in keys:
            if key not in industry_by_strategy and fallback_industry:
                industry_by_strategy[key] = fallback_industry
        industry_names = sorted(set(industry_by_strategy.values()))
        best_health = float(dominant_row["ranking_score"]) if dominant_row else 0.0
        confidence = _num(candidate.get("model_confidence"), 0.0) or 0.0
        risk_reward = _num(candidate.get("risk_reward_ratio"), None)
        blocking = list(candidate.get("blocking_reasons") or [])
        final_status = str(candidate.get("final_status") or "INSUFFICIENT_DATA")
        opportunity = round(confidence * 0.7 + best_health * 0.3, 2)
        execution = round((40.0 if not blocking else 0.0) + (30.0 if snapshot.get("source_status") == "fresh" else 10.0) + min(30.0, max(0.0, (risk_reward or 0.0) / 3.0 * 30.0)), 2)
        row = {
            "stock_code": candidate.get("stock_code"),
            "stock_name": candidate.get("stock_name") or "",
            "strategies": keys,
            "dominant_strategy": dominant_key,
            "dominant_strategy_name": (
                str(dominant_row.get("strategy_name") or dominant_key)
                if dominant_row else dominant_key
            ),
            "industry_name": candidate.get("industry_name") or candidate.get("industry") or candidate.get("theme_code") or "",
            "industry_names": industry_names,
            "industry_by_strategy": industry_by_strategy,
            "opportunity_score": opportunity,
            "execution_score": execution,
            "model_confidence": candidate.get("model_confidence"),
            "risk_reward_ratio": risk_reward,
            "final_status": final_status,
            "blocking_reasons": blocking,
            "reason": candidate.get("today_signal") or candidate.get("conflict_summary") or "等待验证",
            "evidence": {"data_date": candidate.get("data_date"), "risk_level": candidate.get("risk_level"), "source_status": snapshot.get("source_status"), "trading_gate_status": trading_gate["status"], "market_risk_cap_pct": trading_gate["market_risk_cap_pct"], "candidate_source_hash": trading_gate["candidate_source_hash"]},
            "real_order_authority": False,
        }
        observation.append(row)
        if final_status in {"READY", "WATCH"} and confidence >= 60 and not blocking:
            confirmation.append({**row, "gate_status": "研究确认"})
        all_signal_strategies_eligible = bool(keys) and len(member_rows) == len(set(keys)) and all(
            member["paper_allocation_eligible"] for member in member_rows
        )
        dominant_eligible = bool(
            dominant_row and dominant_row["paper_allocation_eligible"]
        )
        if trading_allowed and final_status == "READY" and dominant_eligible and all_signal_strategies_eligible and not blocking and risk_reward is not None and risk_reward >= PROFIT_GATE_POLICY["minimum_payoff_ratio"]:
            tradable.append({
                **row,
                "gate_status": (
                    "降权模拟资金候选"
                    if trading_gate["status"] == "REDUCE_NEW_BUY"
                    else "模拟资金候选"
                ),
                "market_gate_status": trading_gate["status"],
                "market_risk_cap_pct": trading_gate["market_risk_cap_pct"],
                "paper_allocation_eligible": True,
            })
    for pool in (observation, confirmation, tradable):
        pool.sort(key=lambda row: (-float(row["opportunity_score"]), -float(row["execution_score"]), str(row["stock_code"])))
        for index, row in enumerate(pool, 1):
            row["rank"] = index
    return {
        "observation": observation,
        "confirmation": confirmation,
        "tradable": tradable,
        "trading_gate": trading_gate,
    }


def _allocation_candidate_contract(
    strategies: list[dict[str, Any]],
    combinations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Freeze every ranked entity considered by capital competition."""

    candidates: list[dict[str, Any]] = []
    for row in combinations:
        member_keys = sorted({
            str(item.get("strategy_key") or "")
            for item in (row.get("member_details") or [])
            if str(item.get("strategy_key") or "")
        })
        route = row.get("market_route") or {}
        candidates.append({
            "target_type": "COMBINATION",
            "target_key": row["combination_key"],
            "target_version": row["current_version"],
            "target_name": row["combination_name"],
            "enabled": bool(row.get("enabled")),
            "funding_gate_hash": row["funding_gate_hash"],
            "ranking_score": round(float(row["ranking_score"]), 4),
            "ranking_basis": str(
                row.get("ranking_basis") or DAILY_NAV_RANKING_BASIS
            ),
            "ranking_basis_label": str(
                row.get("ranking_basis_label")
                or DAILY_NAV_RANKING_BASIS_LABEL
            ),
            "profit_gate_passed": bool(row.get("profit_gate_passed")),
            "paper_allocation_eligible": bool(
                row.get("paper_allocation_eligible")
            ),
            "market_state": str(route.get("market_state") or ""),
            "market_route_eligible": route.get("eligible") is True,
            "market_match_score": round(
                _num(route.get("market_match_score"), 0.0) or 0.0, 4,
            ),
            "router_decision_hash": str(route.get("router_decision_hash") or ""),
            "exposure_keys": member_keys,
            "lifecycle_status": str(row.get("current_status") or ""),
            "lifecycle_status_label": LIFECYCLE_LABELS.get(
                str(row.get("current_status") or ""), "未知状态"
            ),
            "combination_lifecycle_risk_multiplier": (
                LIFECYCLE_RISK_MULTIPLIER.get(
                    str(row.get("current_status") or ""), 0.0
                )
            ),
            "member_sleeve_risk_multiplier": round(
                _num(row.get("member_sleeve_risk_multiplier"), 0.0)
                or 0.0,
                8,
            ),
            "lifecycle_risk_multiplier": round(
                LIFECYCLE_RISK_MULTIPLIER.get(
                    str(row.get("current_status") or ""), 0.0
                )
                * (
                    _num(row.get("member_sleeve_risk_multiplier"), 0.0)
                    or 0.0
                ),
                8,
            ),
            "constraint_passed": (
                (row.get("constraint_evaluation") or {}).get("passed")
                is True
            ),
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
                        _num(item.get("weight"), 0.0) or 0.0, 8
                    ),
                    "member_lifecycle_status": str(
                        item.get("lifecycle_status") or ""
                    ),
                    "member_lifecycle_multiplier": round(
                        _num(item.get("lifecycle_risk_multiplier"), 0.0)
                        or 0.0,
                        8,
                    ),
                }
                for item in (row.get("member_details") or [])
            ],
        })
    for row in strategies:
        route = row.get("market_route") or {}
        candidates.append({
            "target_type": "STRATEGY",
            "target_key": row["strategy_key"],
            "target_version": row["current_version"],
            "target_name": row["strategy_name"],
            "enabled": bool(row.get("enabled")),
            "funding_gate_hash": row["funding_gate_hash"],
            "ranking_score": round(float(row["ranking_score"]), 4),
            "ranking_basis": str(
                row.get("ranking_basis") or DAILY_NAV_RANKING_BASIS
            ),
            "ranking_basis_label": str(
                row.get("ranking_basis_label")
                or DAILY_NAV_RANKING_BASIS_LABEL
            ),
            "profit_gate_passed": bool(row.get("profit_gate_passed")),
            "paper_allocation_eligible": bool(
                row.get("paper_allocation_eligible")
            ),
            "market_state": str(route.get("market_state") or ""),
            "market_route_eligible": route.get("eligible") is True,
            "market_match_score": round(
                _num(route.get("market_match_score"), 0.0) or 0.0, 4,
            ),
            "router_decision_hash": str(route.get("router_decision_hash") or ""),
            "exposure_keys": [row["strategy_key"]],
            "lifecycle_status": str(row.get("current_status") or ""),
            "lifecycle_status_label": LIFECYCLE_LABELS.get(
                str(row.get("current_status") or ""), "未知状态"
            ),
            "lifecycle_risk_multiplier": LIFECYCLE_RISK_MULTIPLIER.get(
                str(row.get("current_status") or ""), 0.0
            ),
            "constraint_passed": True,
        })
    candidates.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return candidates


def _largest_remainder_basis_points(
    total_basis_points: int,
    weighted_keys: list[tuple[str, Decimal]],
) -> dict[str, int]:
    """Allocate an integer bp budget deterministically with exact conservation."""

    if total_basis_points < 0 or not weighted_keys:
        raise ValueError("逐成员袖套bp预算或权重无效")
    total_weight = sum((weight for _key, weight in weighted_keys), Decimal("0"))
    if not total_weight.is_finite() or total_weight <= 0:
        raise ValueError("逐成员袖套权重合计无效")
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
        raise RuntimeError("逐成员袖套base bp未守恒")
    return assigned


def _combination_member_sleeve_contract(
    row: dict[str, Any], base_basis_points: int,
) -> tuple[list[dict[str, Any]], str, int, int]:
    sources = row.get("member_sleeves_source")
    if not isinstance(sources, list) or not sources:
        raise ValueError("组合分配缺少逐成员袖套来源")
    identities: set[str] = set()
    weighted: list[tuple[str, Decimal]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for item in sources:
        key = str(item.get("strategy_key") or "")
        weight = Decimal(str(item.get("original_weight") or "0"))
        if (
            not key
            or key in identities
            or not weight.is_finite()
            or weight <= 0
            or item.get("version_match") is not True
        ):
            raise ValueError("组合逐成员袖套身份、版本或权重无效")
        identities.add(key)
        weighted.append((key, weight))
        by_key[key] = item
    base_by_key = _largest_remainder_basis_points(base_basis_points, weighted)
    combination_status = str(row.get("lifecycle_status") or "")
    combination_multiplier = Decimal(str(
        LIFECYCLE_RISK_MULTIPLIER.get(combination_status, 0.0)
    ))
    sleeves: list[dict[str, Any]] = []
    effective_total = 0
    for key in sorted(base_by_key):
        source = by_key[key]
        member_status = str(source.get("member_lifecycle_status") or "")
        member_multiplier = Decimal(str(
            LIFECYCLE_RISK_MULTIPLIER.get(member_status, 0.0)
        ))
        declared_member_multiplier = Decimal(str(
            source.get("member_lifecycle_multiplier") or "0"
        ))
        if declared_member_multiplier != member_multiplier:
            raise ValueError("组合逐成员生命周期倍率与冻结枚举不一致")
        base_bp = base_by_key[key]
        effective_bp = int(
            (
                Decimal(base_bp)
                * member_multiplier
                * combination_multiplier
            ).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
        )
        if effective_bp < 0 or effective_bp > base_bp:
            raise ValueError("组合逐成员有效bp越界")
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
                float(Decimal(str(source.get("original_weight") or "0")) * 100),
                8,
            ),
            "base_bp": base_bp,
            "base_weight_pct": base_bp / 100.0,
            "member_lifecycle_status": member_status,
            "member_lifecycle_multiplier": format(member_multiplier, ".8f"),
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
        sleeves.append({
            **sleeve_row,
            "sleeve_row_hash": _digest({
                "schema": "probiga.strategy-combination-member-sleeve-row.v1",
                **sleeve_row,
            }),
        })
    cash_discount_bp = base_basis_points - effective_total
    if (
        sum(item["base_bp"] for item in sleeves) != base_basis_points
        or sum(item["effective_bp"] for item in sleeves) != effective_total
        or sum(item["cash_discount_bp"] for item in sleeves)
        != cash_discount_bp
    ):
        raise RuntimeError("组合逐成员袖套未满足1bp守恒")
    payload = {
        "schema": "probiga.strategy-combination-member-sleeves.v1",
        "combination_key": str(row.get("target_key") or ""),
        "combination_version": str(row.get("target_version") or ""),
        "base_bp": base_basis_points,
        "effective_bp": effective_total,
        "cash_discount_bp": cash_discount_bp,
        "members": sleeves,
    }
    return sleeves, _digest(payload), effective_total, cash_discount_bp


def _attach_pool_industry_focus(
    strategies: list[dict[str, Any]], pools: dict[str, Any],
) -> None:
    """Aggregate visible stock-pool industries onto each strategy rank row."""

    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total: dict[str, int] = defaultdict(int)
    for pool_row in pools.get("observation") or []:
        for key in {
            str(value) for value in (pool_row.get("strategies") or [])
            if str(value)
        }:
            by_strategy = pool_row.get("industry_by_strategy")
            by_strategy = by_strategy if isinstance(by_strategy, dict) else {}
            industry = str(
                by_strategy.get(key)
                or pool_row.get("industry_name")
                or "未分类"
            ).strip()
            counts[key][industry] += 1
            total[key] += 1
    for strategy in strategies:
        key = str(strategy.get("strategy_key") or "")
        denominator = total.get(key, 0)
        focus = [
            {
                "industry_name": industry,
                "candidate_count": count,
                "candidate_share_pct": round(
                    count / denominator * 100.0, 2
                ) if denominator else 0.0,
            }
            for industry, count in sorted(
                counts.get(key, {}).items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        strategy["industry_candidate_count"] = denominator
        strategy["industry_focus"] = focus
        strategy["primary_industry"] = (
            focus[0]["industry_name"] if focus else "暂无票池行业"
        )


def _allocation(
    strategies: list[dict[str, Any]], combinations: list[dict[str, Any]],
    market_state: str, *, trading_allowed: bool,
    candidate_contract: list[dict[str, Any]] | None = None,
    trading_gate: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    contract = (
        candidate_contract
        if candidate_contract is not None
        else _allocation_candidate_contract(strategies, combinations)
    )
    candidates = [
        {
            "target_type": row["target_type"],
            "target_key": row["target_key"],
            "target_version": row["target_version"],
            "funding_gate_hash": row["funding_gate_hash"],
            "name": row["target_name"],
            "score": row["ranking_score"],
            "ranking_basis": str(
                row.get("ranking_basis") or DAILY_NAV_RANKING_BASIS
            ),
            "ranking_basis_label": str(
                row.get("ranking_basis_label")
                or DAILY_NAV_RANKING_BASIS_LABEL
            ),
            "market_state": row["market_state"],
            "market_match_score": row["market_match_score"],
            "router_decision_hash": row["router_decision_hash"],
            "exposure_keys": frozenset(row["exposure_keys"]),
            "lifecycle_status": row["lifecycle_status"],
            "lifecycle_status_label": row["lifecycle_status_label"],
            "lifecycle_risk_multiplier": row[
                "lifecycle_risk_multiplier"
            ],
            "member_sleeves_source": row.get("member_sleeves_source") or [],
        }
        for row in contract
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
        )
    ]
    candidates = [
        row for row in candidates
        if row["ranking_basis"] == DAILY_NAV_RANKING_BASIS
    ]
    # Strategies and combinations compete only inside their own fixed lane.
    # This avoids comparing raw scores across different portfolio objects.
    # A selected combination owns its member exposures by explicit policy;
    # member strategies are not selected again in the strategy lane.
    combinations_lane = sorted(
        (row for row in candidates if row["target_type"] == "COMBINATION"),
        key=lambda row: (
            -float(row["score"]) * float(row["market_match_score"]) / 100.0,
            row["target_key"],
        ),
    )
    strategies_lane = sorted(
        (row for row in candidates if row["target_type"] == "STRATEGY"),
        key=lambda row: (
            -float(row["score"]) * float(row["market_match_score"]) / 100.0,
            row["target_key"],
        ),
    )
    selected_combinations: list[dict[str, Any]] = []
    used_exposures: set[str] = set()
    for row in combinations_lane:
        exposures = set(row["exposure_keys"])
        if not exposures or exposures.intersection(used_exposures):
            continue
        selected_combinations.append(row)
        used_exposures.update(exposures)
    selected_strategies = [
        row for row in strategies_lane
        if set(row["exposure_keys"])
        and not set(row["exposure_keys"]).intersection(used_exposures)
    ]
    candidates = selected_combinations + selected_strategies
    cap = MARKET_RISK_CAP_PCT.get(market_state, 0.0)
    gate_status = str((trading_gate or {}).get("status") or "")
    gate_reason = str((trading_gate or {}).get("reason") or "")
    if not trading_allowed or not candidates or cap <= 0:
        return [{"target_type": "CASH", "target_key": "cash", "target_version": "", "funding_gate_hash": "", "market_state": market_state, "market_match_score": 0.0, "router_decision_hash": "", "name": "现金", "simulated_weight_pct": 100.0, "market_gate_status": gate_status, "market_risk_cap_pct": cap, "reason": gate_reason or "没有策略或组合同时通过盈利硬门槛与当前市场状态门槛", "real_order_authority": False}]
    cap_basis_points = int(round(cap * 100))
    nonempty_lanes = [
        lane for lane in (selected_combinations, selected_strategies) if lane
    ]
    lane_base = cap_basis_points // len(nonempty_lanes)
    lane_remainder = cap_basis_points - lane_base * len(nonempty_lanes)
    assigned_by_identity: dict[tuple[str, str], int] = {}
    for lane_index, lane in enumerate(nonempty_lanes):
        lane_budget = lane_base + (1 if lane_index < lane_remainder else 0)
        lane_total = sum(
            max(
                0.0001,
                float(row["score"])
                * float(row["market_match_score"]) / 100.0,
            )
            for row in lane
        )
        raw_lane = [
            lane_budget
            * max(
                0.0001,
                float(row["score"])
                * float(row["market_match_score"]) / 100.0,
            )
            / lane_total
            for row in lane
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
            assigned_by_identity[(row["target_type"], row["target_key"])] = (
                basis_points
            )
    assigned = [
        assigned_by_identity[(row["target_type"], row["target_key"])]
        for row in candidates
    ]
    result = []
    assigned_after_lifecycle = 0
    for row, base_basis_points in zip(candidates, assigned):
        member_sleeves: list[dict[str, Any]] = []
        member_sleeve_hash = ""
        if row["target_type"] == "COMBINATION":
            (
                member_sleeves,
                member_sleeve_hash,
                basis_points,
                cash_discount_bp,
            ) = _combination_member_sleeve_contract(row, base_basis_points)
            lifecycle_multiplier = (
                Decimal(basis_points) / Decimal(base_basis_points)
                if base_basis_points > 0 else Decimal("0")
            )
        else:
            lifecycle_multiplier = Decimal(str(
                row.get("lifecycle_risk_multiplier") or "0"
            ))
            basis_points = int(
                (Decimal(base_basis_points) * lifecycle_multiplier).quantize(
                    Decimal("1"), rounding=ROUND_HALF_EVEN
                )
            )
            cash_discount_bp = base_basis_points - basis_points
        if basis_points <= 0:
            continue
        assigned_after_lifecycle += basis_points
        public_row = {
            key: value for key, value in row.items()
            if key not in {"exposure_keys", "member_sleeves_source"}
        }
        lifecycle_reason = (
            "；组合状态或成员袖套处于降权运行，折扣资金留在现金"
            if lifecycle_multiplier < Decimal("1") else ""
        )
        result.append({**public_row, "allocation_type_lane_policy": ALLOCATION_TYPE_LANE_POLICY, "simulated_weight_pct": basis_points / 100.0, "base_competitive_weight_pct": base_basis_points / 100.0, "member_sleeves": member_sleeves, "member_sleeve_hash": member_sleeve_hash, "cash_discount_bp": cash_discount_bp, "market_gate_status": gate_status, "market_risk_cap_pct": cap, "reason": f"盈利门槛通过且适配{market_state}，在同类型日频净值赛道内按健康分与路由匹配度分配；全局风险上限{cap:.2f}%{lifecycle_reason}", "real_order_authority": False})
    result.append({"target_type": "CASH", "target_key": "cash", "target_version": "", "funding_gate_hash": "", "market_state": market_state, "market_match_score": 0.0, "router_decision_hash": "", "name": "现金", "simulated_weight_pct": (10_000 - assigned_after_lifecycle) / 100.0, "market_gate_status": gate_status, "market_risk_cap_pct": cap, "reason": "当前市场风险预算及生命周期降权折扣资金保留", "real_order_authority": False})
    return result


def _allocation_snapshot_contract(
    allocations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for row in allocations:
        target_type = str(row.get("target_type") or "")
        base_bp = int(round(
            (_num(row.get("base_competitive_weight_pct"), 0.0) or 0.0)
            * 100
        ))
        effective_bp = int(round(
            (_num(row.get("simulated_weight_pct"), 0.0) or 0.0) * 100
        ))
        cash_discount_bp = _int(row.get("cash_discount_bp"), 0)
        member_sleeves = row.get("member_sleeves") or []
        member_sleeve_hash = str(row.get("member_sleeve_hash") or "")
        if target_type == "COMBINATION":
            expected_sleeve_fields = {
                "strategy_key", "strategy_version", "current_strategy_version",
                "original_weight", "configured_weight_pct", "base_bp",
                "base_weight_pct", "member_lifecycle_status",
                "member_lifecycle_multiplier", "member_multiplier",
                "combination_lifecycle_status",
                "combination_lifecycle_multiplier", "combination_multiplier",
                "effective_bp", "effective_weight_pct", "cash_discount_bp",
                "discount_to_cash_pct", "sleeve_row_hash",
            }
            sleeve_payload = {
                "schema": "probiga.strategy-combination-member-sleeves.v1",
                "combination_key": str(row.get("target_key") or ""),
                "combination_version": str(row.get("target_version") or ""),
                "base_bp": base_bp,
                "effective_bp": effective_bp,
                "cash_discount_bp": cash_discount_bp,
                "members": member_sleeves,
            }
            if (
                not isinstance(member_sleeves, list)
                or not member_sleeves
                or not _HASH_PATTERN.fullmatch(member_sleeve_hash)
                or _digest(sleeve_payload) != member_sleeve_hash
                or sum(_int(item.get("base_bp"), -1) for item in member_sleeves)
                != base_bp
                or sum(_int(item.get("effective_bp"), -1) for item in member_sleeves)
                != effective_bp
                or sum(_int(item.get("cash_discount_bp"), -1) for item in member_sleeves)
                != cash_discount_bp
                or effective_bp + cash_discount_bp != base_bp
                or [
                    str(item.get("strategy_key") or "")
                    for item in member_sleeves
                ] != sorted({
                    str(item.get("strategy_key") or "")
                    for item in member_sleeves
                })
                or any(
                    not isinstance(item, dict)
                    or set(item) != expected_sleeve_fields
                    or not _HASH_PATTERN.fullmatch(str(
                        item.get("sleeve_row_hash") or ""
                    ))
                    or _digest({
                        "schema": (
                            "probiga.strategy-combination-member-sleeve-row.v1"
                        ),
                        **{
                            str(key): value for key, value in item.items()
                            if str(key) != "sleeve_row_hash"
                        },
                    }) != str(item.get("sleeve_row_hash") or "")
                    for item in member_sleeves
                )
            ):
                raise ValueError("组合分配逐成员袖套哈希或1bp守恒无效")
            combination_status = str(row.get("lifecycle_status") or "")
            combination_multiplier = Decimal(str(
                LIFECYCLE_RISK_MULTIPLIER.get(combination_status, -1.0)
            ))
            weighted_members: list[tuple[str, Decimal]] = []
            for item in member_sleeves:
                def required_decimal(field: str) -> Decimal:
                    raw_value = item.get(field)
                    if raw_value is None:
                        return Decimal("-1")
                    try:
                        return Decimal(str(raw_value))
                    except (InvalidOperation, TypeError, ValueError):
                        return Decimal("-1")

                member_status = str(item.get("member_lifecycle_status") or "")
                member_multiplier = Decimal(str(
                    LIFECYCLE_RISK_MULTIPLIER.get(member_status, -1.0)
                ))
                original_weight = required_decimal("original_weight")
                item_base_bp = _int(item.get("base_bp"), -1)
                expected_effective_bp = int((
                    Decimal(item_base_bp)
                    * member_multiplier
                    * combination_multiplier
                ).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))
                if (
                    combination_multiplier < 0
                    or member_multiplier < 0
                    or not original_weight.is_finite()
                    or original_weight <= 0
                    or str(item.get("strategy_version") or "")
                    != str(item.get("current_strategy_version") or "")
                    or required_decimal("member_lifecycle_multiplier")
                    != member_multiplier
                    or required_decimal("combination_lifecycle_multiplier")
                    != combination_multiplier
                    or required_decimal("member_multiplier")
                    != member_multiplier
                    or required_decimal("combination_multiplier")
                    != combination_multiplier
                    or required_decimal("configured_weight_pct")
                    != original_weight * Decimal("100")
                    or required_decimal("base_weight_pct")
                    != Decimal(item_base_bp) / Decimal("100")
                    or _int(item.get("effective_bp"), -1)
                    != expected_effective_bp
                    or required_decimal("effective_weight_pct")
                    != Decimal(expected_effective_bp) / Decimal("100")
                    or _int(item.get("cash_discount_bp"), -1)
                    != item_base_bp - expected_effective_bp
                    or required_decimal("discount_to_cash_pct")
                    != Decimal(item_base_bp - expected_effective_bp)
                    / Decimal("100")
                    or str(item.get("combination_lifecycle_status") or "")
                    != combination_status
                ):
                    raise ValueError("组合分配逐成员生命周期或bp公式无效")
                weighted_members.append((
                    str(item.get("strategy_key") or ""), original_weight,
                ))
            expected_base_by_key = _largest_remainder_basis_points(
                base_bp, weighted_members,
            )
            if any(
                _int(item.get("base_bp"), -1)
                != expected_base_by_key[str(item.get("strategy_key") or "")]
                for item in member_sleeves
            ):
                raise ValueError("组合分配逐成员基础权重不是最大余数精确分配")
        elif member_sleeves or member_sleeve_hash:
            raise ValueError("非组合分配不得伪造逐成员袖套")
        rows.append({
            "target_type": target_type,
            "target_key": str(row.get("target_key") or ""),
            "target_version": str(row.get("target_version") or ""),
            "funding_gate_hash": str(row.get("funding_gate_hash") or ""),
            "market_state": str(row.get("market_state") or ""),
            "market_match_score": round(
                _num(row.get("market_match_score"), 0.0) or 0.0, 4,
            ),
            "router_decision_hash": str(
                row.get("router_decision_hash") or ""
            ),
            "lifecycle_status": str(row.get("lifecycle_status") or ""),
            "lifecycle_status_label": str(
                row.get("lifecycle_status_label") or ""
            ),
            "lifecycle_risk_multiplier": round(
                _num(row.get("lifecycle_risk_multiplier"), 0.0) or 0.0,
                4,
            ),
            "base_competitive_weight_pct": round(
                _num(row.get("base_competitive_weight_pct"), 0.0) or 0.0,
                4,
            ),
            "simulated_weight_pct": round(
                _num(row.get("simulated_weight_pct"), 0.0) or 0.0, 4,
            ),
            "member_sleeves": member_sleeves,
            "member_sleeve_hash": member_sleeve_hash,
            "cash_discount_bp": cash_discount_bp,
            "real_order_authority": bool(
                row.get("real_order_authority")
            ),
        })
    rows.sort(key=lambda row: (row["target_type"], row["target_key"]))
    return rows


def _persist_health(connection, run_uid: str, trade_date: str, strategies: list[dict[str, Any]]) -> None:
    for strategy in strategies:
        for window in WINDOWS:
            metrics = strategy["metrics"][str(window)]
            gate = metrics["profit_gate"]
            payload = {"strategy_key": strategy["strategy_key"], "strategy_version": strategy["current_version"], "trade_date": trade_date, "window_days": window, "metrics": metrics, "gate": gate, "overall_profit_gate_passed": strategy["profit_gate_passed"], "market_route": strategy["market_route"], "paper_allocation_eligible": strategy["paper_allocation_eligible"], "funding_gate_hash": strategy["funding_gate_hash"], "funding_evidence_revision_at": strategy["funding_evidence_revision_at"]}
            connection.execute(
                text("""
                INSERT INTO st_strategy_health_snapshot
                (run_uid, strategy_key, strategy_version, trade_date, window_days,
                 completed_trades, coverage_days, win_rate_pct, average_win_pct,
                 average_loss_pct, payoff_ratio, gross_expectancy_pct,
                 estimated_cost_pct, net_expectancy_pct, profit_factor,
                 max_drawdown_pct, walk_forward_segments, positive_segments,
                 cost_stress_expectancy_pct, top5_profit_contribution_pct,
                 market_match_score, health_score, profit_gate_passed,
                 gate_reason, recommended_status, evidence_json, result_hash)
                VALUES (:run_uid, :strategy_key, :strategy_version, :trade_date,
                        :window_days, :completed_trades, :coverage_days,
                        :win_rate_pct, :average_win_pct, :average_loss_pct,
                        :payoff_ratio, :gross_expectancy_pct, :estimated_cost_pct,
                        :net_expectancy_pct, :profit_factor, :max_drawdown_pct,
                        :walk_forward_segments, :positive_segments,
                        :cost_stress_expectancy_pct, :top5_profit_contribution_pct,
                        :market_match_score, :health_score, :profit_gate_passed,
                        :gate_reason, :recommended_status, :evidence_json, :result_hash)
                """),
                {
                    "run_uid": run_uid, "strategy_key": strategy["strategy_key"], "strategy_version": strategy["current_version"], "trade_date": trade_date, "window_days": window,
                    "completed_trades": _int(metrics.get("completed_trades")), "coverage_days": _int(metrics.get("coverage_days")), "win_rate_pct": metrics.get("win_rate_pct"), "average_win_pct": metrics.get("average_win_pct"), "average_loss_pct": metrics.get("average_loss_pct"), "payoff_ratio": metrics.get("payoff_ratio"), "gross_expectancy_pct": metrics.get("gross_expectancy_pct"), "estimated_cost_pct": metrics.get("estimated_cost_pct"), "net_expectancy_pct": metrics.get("net_expectancy_pct"), "profit_factor": metrics.get("profit_factor"), "max_drawdown_pct": metrics.get("max_drawdown_pct"), "walk_forward_segments": _int(metrics.get("walk_forward_segments")), "positive_segments": _int(metrics.get("positive_segments")), "cost_stress_expectancy_pct": metrics.get("cost_stress_expectancy_pct"), "top5_profit_contribution_pct": metrics.get("top5_profit_contribution_pct"), "market_match_score": metrics.get("market_match_score"), "health_score": metrics.get("health_score") or 0.0, "profit_gate_passed": 1 if gate["passed"] else 0, "gate_reason": gate["reason"][:1000], "recommended_status": strategy["recommended_status"], "evidence_json": _json_text(payload), "result_hash": _digest(payload),
                },
            )


def _persist_combinations(connection, payload: dict[str, Any]) -> None:
    for combo in payload["combinations"]:
        evidence = {
            "combination_key": combo["combination_key"],
            "combination_version": combo["current_version"],
            "trade_date": payload["trade_date"],
            "metrics": combo["metrics"],
            "multi_window_gate": combo["multi_window_gate"],
            "funding_gate_hash": combo["funding_gate_hash"],
            "funding_evidence_revision_at": combo[
                "funding_evidence_revision_at"
            ],
            "overall_profit_gate_passed": combo["profit_gate_passed"],
            "market_route": combo["market_route"],
            "paper_allocation_eligible": combo["paper_allocation_eligible"],
            "member_details": combo["member_details"],
            "constraint_evaluation": combo.get("constraint_evaluation") or {},
        }
        connection.execute(text(
            """
            INSERT INTO st_strategy_combination_health_snapshot
            (run_uid, combination_key, combination_version, trade_date,
             ranking_score, profit_gate_passed, gate_reason,
             recommended_status, evidence_json, result_hash)
            VALUES (:run_uid, :combination_key, :combination_version,
                    :trade_date, :ranking_score, :profit_gate_passed,
                    :gate_reason, :recommended_status, :evidence_json,
                    :result_hash)
            """
        ), {
            "run_uid": payload["run_uid"],
            "combination_key": combo["combination_key"],
            "combination_version": combo["current_version"],
            "trade_date": payload["trade_date"],
            "ranking_score": combo["ranking_score"],
            "profit_gate_passed": 1 if combo["profit_gate_passed"] else 0,
            "gate_reason": combo["gate_reason"][:1000],
            "recommended_status": combo["recommended_status"],
            "evidence_json": _json_text(evidence),
            "result_hash": _digest(evidence),
        })


def _persist_run(connection, payload: dict[str, Any]) -> None:
    run_uid = payload["run_uid"]
    summary = payload["summary"]
    transition_plan = payload.get("automatic_transition_plan")
    if not isinstance(transition_plan, dict):
        raise ValueError("自动生命周期计划缺失")
    transition_plan_hash = _digest(transition_plan)
    if (
        set(transition_plan)
        != {
            "schema",
            "trade_date",
            "transition_count",
            "transitions",
        }
        or transition_plan.get("schema")
        != AUTOMATIC_TRANSITION_PLAN_SCHEMA
        or transition_plan.get("trade_date") != payload["trade_date"]
        or not isinstance(transition_plan.get("transitions"), list)
        or _int(transition_plan.get("transition_count"), -1)
        != len(transition_plan.get("transitions") or [])
        or transition_plan_hash
        != str(payload.get("automatic_transition_plan_hash") or "")
        or transition_plan_hash
        != str(summary.get("automatic_transition_plan_hash") or "")
        or _int(summary.get("automatic_transition_count"), -1)
        != _int(transition_plan.get("transition_count"), -2)
    ):
        raise ValueError("自动生命周期计划在持久化前发生变化")
    pool_snapshot, pool_snapshot_hash, pool_row_hashes = (
        _pool_snapshot_contract(payload["trade_date"], payload["pools"])
    )
    if (
        pool_snapshot_hash != str(payload.get("pool_snapshot_hash") or "")
        or pool_snapshot_hash
        != str(summary.get("pool_snapshot_hash") or "")
        or _int(summary.get("pool_row_count"), -1)
        != _int(pool_snapshot.get("row_count"), -2)
    ):
        raise ValueError("股票池快照在持久化前发生变化")
    stored_result = {
        **payload,
        "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True,
        "idempotent_replay": False,
    }
    result_json = _json_text(stored_result)
    result_hash = hashlib.sha256(result_json.encode("utf-8")).hexdigest()
    payload["result_mode"] = "CANONICAL_PERSISTED"
    payload["is_canonical"] = True
    payload["canonical_result_hash"] = result_hash
    connection.execute(
        text("""
        INSERT INTO st_strategy_governance_run
        (run_uid, trade_date, run_revision, supersedes_run_uid,
         is_canonical, market_state, source_status, input_ready,
         input_hash, build_commit_sha, router_policy_version,
         router_snapshot_hash, decision_hash, status,
         strategy_count, formal_count,
         shadow_count, combination_count, observation_count, confirmation_count,
         tradable_count, allocation_count, summary_json, result_json,
         result_hash, finished_at)
        VALUES (:run_uid, :trade_date, :run_revision,
                :supersedes_run_uid, 1, :market_state, :source_status, 1,
                :input_hash, :build_commit_sha, :router_policy_version,
                :router_snapshot_hash, :decision_hash, 'COMPLETED', :strategy_count,
                :formal_count, :shadow_count, :combination_count, :observation_count,
                :confirmation_count, :tradable_count, :allocation_count,
                :summary_json, :result_json, :result_hash, NOW())
        """),
        {"run_uid": run_uid, "trade_date": payload["trade_date"], "run_revision": payload["run_revision"], "supersedes_run_uid": payload.get("supersedes_run_uid") or "", "market_state": payload["market_state"].get("key") or "unknown", "source_status": payload["source_status"], "input_hash": payload["input_hash"], "build_commit_sha": payload["build_commit_sha"], "router_policy_version": payload["router_policy_version"], "router_snapshot_hash": payload["router_snapshot_hash"], "decision_hash": payload["decision_hash"], **{key: summary[key] for key in ("strategy_count", "formal_count", "shadow_count", "combination_count", "observation_count", "confirmation_count", "tradable_count", "allocation_count")}, "summary_json": _json_text(summary), "result_json": result_json, "result_hash": result_hash},
    )
    levels = (("OBSERVATION", payload["pools"]["observation"]), ("CONFIRMATION", payload["pools"]["confirmation"]), ("TRADABLE", payload["pools"]["tradable"]))
    for level, rows in levels:
        for row in rows:
            stock_code = str(row.get("stock_code") or "").strip()
            pool_row_hash = pool_row_hashes.get((level, stock_code), "")
            source_evidence = row.get("evidence") or {}
            evidence_envelope = {
                "schema": POOL_ROW_EVIDENCE_SCHEMA,
                "source_evidence": source_evidence,
                "pool_row_hash": pool_row_hash,
            }
            connection.execute(
                text("""
                INSERT INTO st_strategy_pool_snapshot
                (run_uid, trade_date, pool_level, stock_code, stock_name, rank_no,
                 opportunity_score, execution_score, dominant_strategy,
                 strategies_json, industry_name, gate_status, reason_json, evidence_json)
                VALUES (:run_uid, :trade_date, :pool_level, :stock_code, :stock_name,
                        :rank_no, :opportunity_score, :execution_score,
                        :dominant_strategy, :strategies_json, :industry_name,
                        :gate_status, :reason_json, :evidence_json)
                """),
                {"run_uid": run_uid, "trade_date": payload["trade_date"], "pool_level": level, "stock_code": stock_code, "stock_name": row.get("stock_name") or "", "rank_no": row.get("rank") or 0, "opportunity_score": row.get("opportunity_score"), "execution_score": row.get("execution_score"), "dominant_strategy": row.get("dominant_strategy") or "", "strategies_json": _json_text(row.get("strategies") or []), "industry_name": row.get("industry_name") or "", "gate_status": row.get("gate_status") or "观察", "reason_json": _json_text({"reason": str(row.get("reason") or ""), "blocking_reasons": [str(value) for value in (row.get("blocking_reasons") or [])]}), "evidence_json": _json_text(evidence_envelope)},
            )
    for row in payload["allocations"]:
        connection.execute(
            text("""
            INSERT INTO st_strategy_allocation_snapshot
            (run_uid, target_type, target_key, target_version,
             funding_gate_hash, market_state, market_match_score,
             router_decision_hash, lifecycle_status,
             lifecycle_status_label, lifecycle_risk_multiplier,
             base_competitive_weight_pct, simulated_weight_pct,
             member_sleeves_json, member_sleeve_hash, cash_discount_bp,
             reason, real_order_authority)
            VALUES (:run_uid, :target_type, :target_key, :target_version,
                    :funding_gate_hash, :market_state, :market_match_score,
                    :router_decision_hash, :lifecycle_status,
                    :lifecycle_status_label, :lifecycle_risk_multiplier,
                    :base_competitive_weight_pct, :weight,
                    :member_sleeves_json, :member_sleeve_hash,
                    :cash_discount_bp, :reason, 0)
            """),
            {"run_uid": run_uid, "target_type": row["target_type"], "target_key": row["target_key"], "target_version": row.get("target_version") or "", "funding_gate_hash": row.get("funding_gate_hash") or "", "market_state": row.get("market_state") or "", "market_match_score": row.get("market_match_score"), "router_decision_hash": row.get("router_decision_hash") or "", "lifecycle_status": row.get("lifecycle_status") or "", "lifecycle_status_label": row.get("lifecycle_status_label") or "", "lifecycle_risk_multiplier": row.get("lifecycle_risk_multiplier") or 0, "base_competitive_weight_pct": row.get("base_competitive_weight_pct") or 0, "weight": row["simulated_weight_pct"], "member_sleeves_json": _json_text(row.get("member_sleeves") or []), "member_sleeve_hash": row.get("member_sleeve_hash") or "", "cash_discount_bp": _int(row.get("cash_discount_bp"), 0), "reason": row["reason"][:500]},
        )
    _append_audit_connection(
        connection,
        entity_type="SYSTEM",
        entity_key="strategy_governance_daily",
        action="RUN_GOVERNANCE",
        reason=f"完成{payload['trade_date']}动态策略治理闭环",
        operator=str(payload.get("operator") or "daily_governance")[:80],
        before={},
        after={
            "status": "COMPLETED",
            "trade_date": payload["trade_date"],
            "run_revision": payload["run_revision"],
            "supersedes_run_uid": payload.get("supersedes_run_uid") or "",
            "is_canonical": True,
            "summary": summary,
        },
        evidence={
            "run_uid": run_uid,
            "run_revision": payload["run_revision"],
            "supersedes_run_uid": payload.get("supersedes_run_uid") or "",
            "input_hash": payload["input_hash"],
            "decision_hash": payload["decision_hash"],
            "build_commit_sha": payload["build_commit_sha"],
            "router_policy_version": payload["router_policy_version"],
            "router_snapshot_hash": payload["router_snapshot_hash"],
            "automatic_real_order_submission": False,
        },
    )


def _recommend_strategy_row(row: dict[str, Any]) -> tuple[str, str]:
    current = row["current_status"]
    if current == "RETIRED":
        return "RETIRED", "已淘汰版本保持终态；只能注册新版本重新验证"
    if row.get("version_integrity_valid") is not True:
        return (
            "SUSPENDED" if current in {"ACTIVE", "REDUCE"} else "SHADOW",
            "不可变策略版本内容哈希校验失败",
        )
    if row.get("execution_adapter_executable") is not True:
        return (
            "SUSPENDED" if current in {"ACTIVE", "REDUCE"} else "SHADOW",
            str(row.get("execution_adapter_reason")
                or "执行适配器未部署/无效"),
        )
    if (
        str(row.get("source_kind") or "") == "runtime_registry"
        and row.get("funding_pipeline_ready") is not True
    ):
        return (
            "SUSPENDED" if current in {"ACTIVE", "REDUCE"} else "SHADOW",
            str(
                row.get("funding_pipeline_reason")
                or "动态策略资金证据闭环尚未部署，仅允许影子研究"
            ),
        )
    if not row.get("enabled"):
        return "SUSPENDED", "策略已禁用，不授予模拟资金资格"
    if row["profit_gate_passed"]:
        if row["ranking_score"] >= 80:
            return "ACTIVE", f"多窗口盈利硬门槛通过，健康分{row['ranking_score']:.1f}"
        return "REDUCE", f"多窗口盈利硬门槛通过但健康分仅{row['ranking_score']:.1f}"
    short = row["metrics"]["20"]
    short_sample = _int(short.get("completed_trades"))
    short_net = _num(short.get("net_expectancy_pct"), None)
    short_pf = _num(short.get("profit_factor"), None)
    recent_failure = short_sample >= 20 and (
        (short_net is not None and short_net <= 0)
        or (short_pf is not None and short_pf < 1.0)
    )
    if current in {"ACTIVE", "REDUCE"} or recent_failure:
        return "SUSPENDED", f"盈利能力失效或证据过期；{row['profit_gate_reason']}"
    if current == "SUSPENDED":
        primary = row["primary_metrics"]
        recovery_ready = (
            primary.get("evidence_fresh") is True
            and (_num(primary.get("net_expectancy_pct"), -999.0) or -999.0) > 0
            and (_num(primary.get("profit_factor"), -1.0) or -1.0) >= 1.10
        )
        if recovery_ready:
            return "SHADOW", "恢复条件初步满足，先返回影子观察重新积累独立证据"
        return "SUSPENDED", f"恢复条件尚未满足；{row['profit_gate_reason']}"
    return "SHADOW", row["profit_gate_reason"]


def _latest_suspension_boundary(
    entity_type: str, entity_key: str, entity_version: str,
) -> dict[str, str]:
    if not _table_exists("st_strategy_lifecycle_event"):
        return {}
    rows = _db_read(
        "SELECT occurred_at, evidence_json, event_hash "
        "FROM st_strategy_lifecycle_event "
        "WHERE entity_type=:entity_type AND entity_key=:entity_key "
        "AND entity_version=:entity_version AND next_status='SUSPENDED' "
        "ORDER BY occurred_at DESC, event_id DESC LIMIT 1",
        {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "entity_version": entity_version,
        },
    )
    if not rows:
        return {}
    row = rows[0]
    evidence = _json(row.get("evidence_json"), {})
    occurred_at = _normalize_evidence_revision(row.get("occurred_at"))
    boundary_revision = _normalize_evidence_revision(
        evidence.get("funding_evidence_revision_at")
    ) or occurred_at
    trade_date = str(evidence.get("trade_date") or occurred_at)[:10]
    return {
        "evidence_revision_at": boundary_revision,
        "trade_date": trade_date,
        "event_hash": str(row.get("event_hash") or ""),
        "occurred_at": occurred_at,
    }


def _apply_suspended_recovery_rule(
    *, current_status: str, recommended_status: str, reason: str,
    primary_metrics: dict[str, Any], funding_evidence_revision_at: str,
    suspension_boundary: dict[str, str], blockers: list[str] | None = None,
) -> tuple[str, str]:
    if current_status != "SUSPENDED":
        return recommended_status, reason
    blocking_reasons = [item for item in (blockers or []) if item]
    boundary_revision = _normalize_evidence_revision(
        suspension_boundary.get("evidence_revision_at")
    )
    current_revision = _normalize_evidence_revision(
        funding_evidence_revision_at
    )
    if not boundary_revision:
        return "SUSPENDED", "找不到可核验的暂停证据边界，继续暂停并等待人工审计"
    if not current_revision or current_revision <= boundary_revision:
        return (
            "SUSPENDED",
            "暂停后尚无严格更新的独立证据，旧证据不能触发恢复",
        )
    if blocking_reasons:
        return "SUSPENDED", "暂停后证据仍有阻断项：" + "、".join(blocking_reasons)
    reviewer = str(primary_metrics.get("reviewed_by") or "")
    submitter = str(primary_metrics.get("submitted_by") or "")
    recovery_ready = (
        primary_metrics.get("evidence_fresh") is True
        and primary_metrics.get("verification_status") == "CONFIRMED"
        and bool(reviewer)
        and reviewer != submitter
        and bool(_HASH_PATTERN.fullmatch(str(
            primary_metrics.get("source_dataset_hash") or ""
        ).lower()))
        and _int(primary_metrics.get("completed_trades")) >= 20
        and (_num(
            primary_metrics.get("net_expectancy_pct"), -999.0
        ) or -999.0) > 0
        and (_num(
            primary_metrics.get("profit_factor"), -1.0
        ) or -1.0) >= 1.10
    )
    if not recovery_ready:
        return "SUSPENDED", "暂停后虽有新证据，但尚未达到恢复观察门槛"
    return "SHADOW", "暂停后新证据达到初步恢复门槛；先进入影子观察，不能直接取得资金资格"


def _confirmed_funding_status(
    *, entity_type: str, entity_key: str, entity_version: str,
    current_status: str, recommended_status: str, reason: str,
    trade_date: str, funding_gate_hash: str,
    funding_evidence_revision_at: str,
    suspension_boundary: dict[str, str] | None = None,
) -> tuple[str, str]:
    if recommended_status not in {"ACTIVE", "REDUCE"}:
        return recommended_status, reason
    if current_status == "SUSPENDED":
        boundary_revision = _normalize_evidence_revision(
            (suspension_boundary or {}).get("evidence_revision_at")
        )
        current_revision = _normalize_evidence_revision(
            funding_evidence_revision_at
        )
        if (
            not boundary_revision
            or not current_revision
            or current_revision <= boundary_revision
        ):
            return "SUSPENDED", "暂停后的资金门槛仍在使用旧证据，继续暂停"
        return "SHADOW", "恢复盈利门槛已通过，但按恢复规则先进入影子观察"
    if not _normalize_evidence_revision(funding_evidence_revision_at):
        return "SHADOW", "缺少可验证的证据高水位，不授予模拟资金资格"
    required = int(PROFIT_GATE_POLICY["minimum_consecutive_gate_passes"])
    boundary = suspension_boundary or {}
    minimum_revision = _normalize_evidence_revision(
        boundary.get("evidence_revision_at")
    )
    minimum_trade_date = str(boundary.get("trade_date") or "")[:10]
    if entity_type == "COMBINATION":
        previous = _prior_consecutive_combination_gate_passes(
            entity_key, entity_version, trade_date, funding_gate_hash,
            funding_evidence_revision_at,
            limit=required - 1,
            minimum_revision_exclusive=minimum_revision,
            minimum_trade_date_exclusive=minimum_trade_date,
        )
    else:
        previous = _prior_consecutive_gate_passes(
            entity_key, entity_version, trade_date, funding_gate_hash,
            funding_evidence_revision_at,
            limit=required - 1,
            minimum_revision_exclusive=minimum_revision,
            minimum_trade_date_exclusive=minimum_trade_date,
        )
    confirmations = previous + 1
    if current_status == "SHADOW" and confirmations < required:
        return "SHADOW", f"盈利门槛通过，独立证据更新连续确认{confirmations}/{required}，暂留影子观察"
    if current_status == "REDUCE" and recommended_status == "ACTIVE" and confirmations < required:
        return "REDUCE", f"恢复正常权重需连续{required}次独立证据更新，当前{confirmations}/{required}"
    return recommended_status, reason


def _latest_completed_governance_date() -> str:
    if not _table_exists("st_strategy_governance_run"):
        return ""
    rows = _db_read(
        "SELECT MAX(trade_date) AS trade_date "
        "FROM st_strategy_governance_run "
        "WHERE status='COMPLETED' AND is_canonical=1"
    )
    return str(rows[0].get("trade_date") or "")[:10] if rows else ""


def _require_no_real_order_authority(value: Any, *, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            name = str(key)
            if name in {"real_order_authority", "automatic_real_order_submission"}:
                if item is not False:
                    raise RuntimeError(f"canonical治理结果包含真实下单权限：{path}.{name}")
            _require_no_real_order_authority(item, path=f"{path}.{name}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _require_no_real_order_authority(item, path=f"{path}[{index}]")


def _validate_canonical_governance_result(result: dict[str, Any]) -> None:
    """Independently replay canonical hashes and non-trading invariants."""

    _require_no_real_order_authority(result)
    trade_date = _trade_date(result.get("trade_date"), default_today=False)
    market = result.get("market_state")
    summary = result.get("summary")
    strategies = result.get("strategies")
    combinations = result.get("combinations")
    pools = result.get("pools")
    allocations = result.get("allocations")
    candidates = result.get("allocation_candidate_set")
    router_snapshot = result.get("router_snapshot")
    transition_plan = result.get("automatic_transition_plan")
    if (
        result.get("status") != "ok"
        or result.get("input_ready") is not True
        or result.get("status_labels") != LIFECYCLE_LABELS
        or str(result.get("allocation_policy_version") or "")
        != ALLOCATION_POLICY_VERSION
        or str(result.get("router_policy_version") or "")
        != MARKET_ROUTER_POLICY_VERSION
        or not isinstance(market, dict)
        or not isinstance(summary, dict)
        or not isinstance(strategies, list)
        or not isinstance(combinations, list)
        or not isinstance(pools, dict)
        or not isinstance(allocations, list)
        or not isinstance(candidates, list)
        or not isinstance(router_snapshot, dict)
        or not isinstance(transition_plan, dict)
    ):
        raise RuntimeError("canonical治理完整结果缺少可复算决策字段")
    market_state = str(market.get("key") or "unknown")
    risk_cap = MARKET_RISK_CAP_PCT.get(market_state, 0.0)
    candidate_payload = {
        "schema": "probiga.strategy-allocation-candidate-set.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trade_date": trade_date,
        "market_state": market_state,
        "candidates": candidates,
    }
    candidate_hash = _digest(candidate_payload)
    allocation_rows = _allocation_snapshot_contract(allocations)
    allocation_payload = {
        "schema": "probiga.strategy-allocation-snapshot.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trade_date": trade_date,
        "market_state": market_state,
        "market_risk_cap_pct": risk_cap,
        "trading_gate_passed": result.get("trading_gate_passed") is True,
        "candidate_set_hash": candidate_hash,
        "allocations": allocation_rows,
    }
    allocation_hash = _digest(allocation_payload)
    allocation_total_bp = sum(int(round(
        (_num(item.get("simulated_weight_pct"), 0.0) or 0.0) * 100
    )) for item in allocations)
    cash_rows = [
        item for item in allocations
        if str(item.get("target_type") or "") == "CASH"
    ]
    funded_bp = sum(int(round(
        (_num(item.get("simulated_weight_pct"), 0.0) or 0.0) * 100
    )) for item in allocations if str(item.get("target_type") or "") != "CASH")
    if (
        candidate_hash != str(result.get("candidate_set_hash") or "")
        or candidate_hash != str(summary.get("candidate_set_hash") or "")
        or allocation_hash != str(result.get("allocation_snapshot_hash") or "")
        or allocation_hash != str(summary.get("allocation_snapshot_hash") or "")
        or allocation_total_bp != 10_000
        or len(cash_rows) != 1
        or funded_bp > int(round(risk_cap * 100))
        or any(item.get("real_order_authority") is not False for item in allocations)
    ):
        raise RuntimeError("canonical治理资金分配公式、哈希或现金守恒无效")
    pool_snapshot, pool_hash, _row_hashes = _pool_snapshot_contract(
        trade_date, pools,
    )
    if (
        pool_snapshot != result.get("pool_snapshot")
        or pool_hash != str(result.get("pool_snapshot_hash") or "")
        or pool_hash != str(summary.get("pool_snapshot_hash") or "")
    ):
        raise RuntimeError("canonical治理股票池快照不可复算")
    expected_strategy_routes = {
        str(item.get("strategy_key") or ""): str(
            (item.get("market_route") or {}).get("router_decision_hash") or ""
        )
        for item in sorted(strategies, key=lambda row: str(row.get("strategy_key") or ""))
    }
    expected_combination_routes = {
        str(item.get("combination_key") or ""): str(
            (item.get("market_route") or {}).get("router_decision_hash") or ""
        )
        for item in sorted(
            combinations, key=lambda row: str(row.get("combination_key") or "")
        )
    }
    if (
        router_snapshot.get("schema")
        != "probiga.strategy-market-router-snapshot.v1"
        or router_snapshot.get("policy_version") != MARKET_ROUTER_POLICY_VERSION
        or router_snapshot.get("trade_date") != trade_date
        or router_snapshot.get("market_state") != market_state
        or router_snapshot.get("strategy_routes") != expected_strategy_routes
        or router_snapshot.get("combination_routes")
        != expected_combination_routes
        or _digest(router_snapshot)
        != str(result.get("router_snapshot_hash") or "")
    ):
        raise RuntimeError("canonical治理市场路由快照不可复算")
    transition_hash = _digest(transition_plan)
    if (
        set(transition_plan) != {
            "schema", "trade_date", "transition_count", "transitions",
        }
        or transition_plan.get("schema") != AUTOMATIC_TRANSITION_PLAN_SCHEMA
        or transition_plan.get("trade_date") != trade_date
        or not isinstance(transition_plan.get("transitions"), list)
        or _int(transition_plan.get("transition_count"), -1)
        != len(transition_plan.get("transitions") or [])
        or transition_hash
        != str(result.get("automatic_transition_plan_hash") or "")
        or transition_hash
        != str(summary.get("automatic_transition_plan_hash") or "")
    ):
        raise RuntimeError("canonical治理生命周期计划不可复算")
    expected_decision = _digest({
        "schema": "strategy-governance-decision.v6",
        "trade_date": trade_date,
        "build_commit_sha": str(result.get("build_commit_sha") or ""),
        "input_hash": str(result.get("input_hash") or ""),
        "router_snapshot_hash": str(result.get("router_snapshot_hash") or ""),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": result.get("trading_gate_passed") is True,
        "market_risk_cap_pct": risk_cap,
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": sum(
            item.get("paper_allocation_eligible") is True for item in candidates
        ),
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "pool_snapshot_hash": pool_hash,
        "strategies": [{
            "strategy_key": str(item.get("strategy_key") or ""),
            "strategy_version": str(item.get("current_version") or ""),
            "enabled": bool(item.get("enabled")),
            "projected_status": str(item.get("current_status") or ""),
            "funding_gate_hash": str(item.get("funding_gate_hash") or ""),
        } for item in strategies],
        "combinations": [{
            "combination_key": str(item.get("combination_key") or ""),
            "combination_version": str(item.get("current_version") or ""),
            "enabled": bool(item.get("enabled")),
            "projected_status": str(item.get("current_status") or ""),
            "funding_gate_hash": str(item.get("funding_gate_hash") or ""),
        } for item in combinations],
    })
    if (
        expected_decision != str(result.get("decision_hash") or "")
        or _int(summary.get("allocation_candidate_count"), -1) != len(candidates)
        or _int(summary.get("eligible_candidate_count"), -1)
        != sum(item.get("paper_allocation_eligible") is True for item in candidates)
        or _num(summary.get("market_risk_cap_pct"), -1.0) != risk_cap
    ):
        raise RuntimeError("canonical治理决策哈希或汇总不可复算")


def _canonical_governance_result_from_row(
    row: dict[str, Any],
) -> dict[str, Any]:
    raw = row.get("result_json")
    if not isinstance(raw, str) or not raw:
        raise RuntimeError("canonical治理运行缺少完整持久结果")
    expected_hash = str(row.get("result_hash") or "")
    if (
        not _HASH_PATTERN.fullmatch(expected_hash)
        or hashlib.sha256(raw.encode("utf-8")).hexdigest() != expected_hash
    ):
        raise RuntimeError("canonical治理完整结果哈希无效")
    result = _json(raw, None)
    if (
        not isinstance(result, dict)
        or str(result.get("run_uid") or "") != str(row.get("run_uid") or "")
        or str(result.get("trade_date") or "")[:10]
        != str(row.get("trade_date") or "")[:10]
        or str(result.get("input_hash") or "")
        != str(row.get("input_hash") or "")
        or str(result.get("decision_hash") or "")
        != str(row.get("decision_hash") or "")
        or result.get("is_canonical") is not True
        or str(result.get("result_mode") or "") != "CANONICAL_PERSISTED"
    ):
        raise RuntimeError("canonical治理完整结果身份与运行账本不一致")
    _validate_canonical_governance_result(result)
    return {
        **result,
        "canonical_result_hash": expected_hash,
        "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True,
    }


def load_canonical_governance_snapshot(
    trade_date: str = "",
) -> dict[str, Any]:
    """Read, never recompute, the latest immutable canonical full result."""

    if not _table_exists("st_strategy_governance_run"):
        raise RuntimeError("策略治理表尚未由部署流程创建")
    requested = str(trade_date or "").strip()[:10]
    params: dict[str, Any] = {}
    where = ""
    if requested:
        requested = _trade_date(requested, default_today=False)
        where = "AND trade_date=:trade_date"
        params["trade_date"] = requested
    rows = _db_read(
        "SELECT run_uid, trade_date, input_hash, decision_hash, result_json, "
        "result_hash FROM st_strategy_governance_run "
        "WHERE status='COMPLETED' AND is_canonical=1 "
        f"{where} ORDER BY trade_date DESC, run_revision DESC, created_at DESC "
        "LIMIT 2",
        params,
    )
    if not rows:
        raise RuntimeError("尚无可展示的canonical完整治理结果；请先运行日终治理")
    if len(rows) > 1 and (
        requested
        or str(rows[0].get("trade_date") or "")[:10]
        == str(rows[1].get("trade_date") or "")[:10]
    ):
        raise RuntimeError("同一交易日存在多条canonical治理结果")
    return _canonical_governance_result_from_row(rows[0])


def _governance_build_commit_sha() -> str:
    value = str(os.getenv("PROBIGA_BUILD_COMMIT_SHA") or "").strip()
    return value[:64] if value else "WORKTREE_UNVERSIONED"


_DYNAMIC_AUDIT_ONLY_KEYS = frozenset({
    "candidate_run_uid", "candidate_receipt_hash", "candidate_completed_at",
    "candidate_run_receipt", "dynamic_adapter_receipts",
})


def _stable_dynamic_input(value: Any) -> Any:
    """Remove per-attempt audit ids while preserving authoritative facts."""

    if isinstance(value, dict):
        return {
            str(key): _stable_dynamic_input(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _DYNAMIC_AUDIT_ONLY_KEYS
        }
    if isinstance(value, list):
        return [_stable_dynamic_input(item) for item in value]
    if isinstance(value, tuple):
        return [_stable_dynamic_input(item) for item in value]
    return value


def _governance_source_input_hash(snapshot: dict[str, Any]) -> str:
    """Hash only stable source inputs; exclude run ids and wall-clock fields."""

    return _digest({
        "schema": "strategy-governance-source-input.v1",
        "trade_date": snapshot.get("trade_date"),
        "data_date": snapshot.get("data_date"),
        "source_status": snapshot.get("source_status"),
        "is_stale": snapshot.get("is_stale"),
        "data_sources": snapshot.get("data_sources"),
        "configuration": snapshot.get("configuration"),
        "market_state": snapshot.get("market_state"),
        "global_gate": snapshot.get("global_gate"),
        "candidate_source": _stable_dynamic_input(
            snapshot.get("candidate_source")
        ),
        "dynamic_adapter_statuses": _stable_dynamic_input(snapshot.get(
            "dynamic_adapter_statuses"
        ) or []),
        "strategies": snapshot.get("strategies") or [],
        "candidates": _stable_dynamic_input(snapshot.get("candidates") or []),
        "conflicts": snapshot.get("conflicts") or [],
    })


def governance_snapshot(
    *, trade_date: str = "", strategy_snapshot: dict[str, Any] | None = None,
    persist: bool = False, operator: str = "daily_governance",
    strategy_limit: int = 500, _governance_connection=None,
) -> dict[str, Any]:
    if persist and _governance_connection is None:
        ensure_and_seed_governance()
        from server.engine.strategy_center import ensure_strategy_center_tables

        ensure_strategy_center_tables()
    elif not _table_exists("st_strategy_registry"):
        raise RuntimeError("策略治理表尚未由部署流程创建，读取接口不会现场建表")
    if persist and _governance_connection is None:
        connection = get_engine().connect()
        acquired = False
        try:
            acquired = int(connection.execute(text(
                "SELECT GET_LOCK('probiga_strategy_governance_daily_v1', 30)"
            )).scalar() or 0) == 1
            connection.commit()
            if not acquired:
                raise RuntimeError("治理日终单写者锁获取超时")
            connection = connection.execution_options(
                isolation_level="REPEATABLE READ",
            )
            with connection.begin():
                with bind_sql_connection(connection):
                    return governance_snapshot(
                        trade_date=trade_date,
                        strategy_snapshot=strategy_snapshot,
                        persist=True,
                        operator=operator,
                        strategy_limit=strategy_limit,
                        _governance_connection=connection,
                    )
        finally:
            if connection.in_transaction():
                connection.rollback()
            if acquired:
                connection.execute(text(
                    "SELECT RELEASE_LOCK('probiga_strategy_governance_daily_v1')"
                ))
                connection.commit()
            connection.close()
    persist_built_strategy_snapshot = strategy_snapshot is None and persist
    if strategy_snapshot is None:
        from server.engine.strategy_center import (
            build_strategy_center_snapshot,
        )

        strategy_snapshot = build_strategy_center_snapshot(
            trade_date,
            limit=max(1, min(500, int(strategy_limit))),
            fresh_market=persist,
        )
    target = _trade_date(strategy_snapshot.get("trade_date") or trade_date)
    input_ready, input_reason = governance_input_ready(strategy_snapshot)
    authoritative_windows: dict[int, dict[str, Any]] | None = None
    if persist:
        if not input_ready:
            raise GovernanceEvidenceNotReady(
                f"治理输入未通过新鲜度校验：{input_reason}",
                blocking_record={
                    "schema": "probiga.strategy-governance-block.v1",
                    "status": "INPUT_NOT_READY",
                    "status_label": "治理输入未就绪",
                    "trade_date": target,
                    "reason": input_reason,
                    "candidate_source": (
                        strategy_snapshot.get("candidate_source") or {}
                    ),
                    "global_gate": (
                        strategy_snapshot.get("global_gate") or {}
                    ),
                    "automatic_real_order_submission": False,
                },
            )
        _require_authoritative_closed_trade_date(target)
        try:
            authoritative_windows = _authoritative_session_windows(target)
        except (TypeError, ValueError) as exc:
            raise GovernanceEvidenceNotReady(
                f"治理交易日窗口未通过QMT收盘交叉认证：{exc}"
            ) from exc
        if date.fromisoformat(target) > date.today():
            raise ValueError("不能使用未来日期更新当前生命周期")
        latest_completed = _latest_completed_governance_date()
        if latest_completed and target < latest_completed:
            raise ValueError(
                f"历史日期{target}早于当前治理日期{latest_completed}，只能回看不能修改当前生命周期"
            )
    registry = load_registry()
    initial_registry_state = {
        row["strategy_key"]: {
            "current_version": str(row["current_version"]),
            "current_status": str(row["current_status"]),
            "enabled": bool(row.get("enabled")),
        }
        for row in registry
    }
    _attach_market_routes(strategy_snapshot, registry)
    metrics = _metrics_for_registry(
        strategy_snapshot, registry, target,
        authoritative_windows=authoritative_windows,
    )
    rankings = _strategy_rankings(registry, metrics)
    run_uid = uuid.uuid4().hex
    transition_plans: list[dict[str, Any]] = []
    for row in rankings:
        current_status = row["current_status"]
        suspension_boundary = _latest_suspension_boundary(
            "STRATEGY", row["strategy_key"], row["current_version"]
        )
        row["suspension_boundary"] = suspension_boundary
        recommended, reason = _recommend_strategy_row(row)
        recommended, reason = _apply_suspended_recovery_rule(
            current_status=current_status,
            recommended_status=recommended,
            reason=reason,
            primary_metrics=row["primary_metrics"],
            funding_evidence_revision_at=row[
                "funding_evidence_revision_at"
            ],
            suspension_boundary=suspension_boundary,
            blockers=[] if row.get("enabled") else ["策略已禁用"],
        )
        recommended, reason = _confirmed_funding_status(
            entity_type="STRATEGY",
            entity_key=row["strategy_key"],
            entity_version=row["current_version"],
            current_status=current_status,
            recommended_status=recommended,
            reason=reason,
            trade_date=target,
            funding_gate_hash=row["funding_gate_hash"],
            funding_evidence_revision_at=row[
                "funding_evidence_revision_at"
            ],
            suspension_boundary=suspension_boundary,
        )
        row["recommended_status"] = recommended
        row["recommended_status_label"] = LIFECYCLE_LABELS[recommended]
        row["recommendation_reason"] = reason
        if persist and recommended != current_status:
            transition_plans.append({
                "entity_type": "STRATEGY",
                "entity_key": row["strategy_key"],
                "entity_version": row["current_version"],
                "previous_status": current_status,
                "next_status": recommended,
                "reason": reason,
                "evidence": {
                    "run_uid": run_uid, "trade_date": target,
                    "multi_window_gate": row["multi_window_gate"],
                    "funding_gate_hash": row["funding_gate_hash"],
                    "funding_evidence_revision_at": row[
                        "funding_evidence_revision_at"
                    ],
                },
            })
            row["current_status"] = recommended
            row["status_label"] = LIFECYCLE_LABELS[recommended]
            row["status_reason"] = reason
            row["paper_allocation_eligible"] = (
                recommended in {"ACTIVE", "REDUCE"}
                and row["profit_gate_passed"]
                and row.get("execution_adapter_executable") is True
                and row.get("market_route_eligible") is True
            )
            row["lane"] = "正式赛道" if recommended in {"ACTIVE", "REDUCE"} else "观察赛道" if recommended == "SHADOW" else "暂停赛道" if recommended == "SUSPENDED" else "历史档案"
    if persist:
        lane_order = {"ACTIVE": 0, "REDUCE": 0, "SHADOW": 1, "SUSPENDED": 2, "RETIRED": 3}
        rankings.sort(key=lambda row: (lane_order.get(row["current_status"], 9), -float(row["ranking_score"]), row["strategy_key"]))
        lane_ranks: dict[str, int] = defaultdict(int)
        for index, row in enumerate(rankings, 1):
            row["overall_rank"] = index
            lane_ranks[row["lane"]] += 1
            row["lane_rank"] = lane_ranks[row["lane"]]
    combination_registry = load_combinations()
    initial_combination_state = {
        row["combination_key"]: {
            "current_version": str(row["current_version"]),
            "current_status": str(row["current_status"]),
            "enabled": bool(row.get("enabled")),
        }
        for row in combination_registry
    }
    combination_versions = {
        row["combination_key"]: row["current_version"]
        for row in combination_registry
    }
    combination_metrics = _load_metric_inputs(
        target,
        entity_type="COMBINATION",
        current_versions=combination_versions,
    )
    combinations = _combination_rankings(
        combination_registry, rankings, combination_metrics, target
    )
    for row in combinations:
        current_status = row["current_status"]
        suspension_boundary = _latest_suspension_boundary(
            "COMBINATION", row["combination_key"], row["current_version"]
        )
        row["suspension_boundary"] = suspension_boundary
        recovery_blockers = []
        if not row.get("enabled"):
            recovery_blockers.append("组合已禁用")
        if row.get("missing_members"):
            recovery_blockers.append("组合成员缺失")
        if row.get("member_version_mismatches"):
            recovery_blockers.append("组合成员版本不一致")
        if not row.get("has_independent_evidence"):
            recovery_blockers.append("组合20/60/120日独立证据不完整")
        recommended, reason = _apply_suspended_recovery_rule(
            current_status=current_status,
            recommended_status=row["recommended_status"],
            reason=row["recommendation_reason"],
            primary_metrics=row["primary_metrics"],
            funding_evidence_revision_at=row[
                "funding_evidence_revision_at"
            ],
            suspension_boundary=suspension_boundary,
            blockers=recovery_blockers,
        )
        recommended, reason = _confirmed_funding_status(
            entity_type="COMBINATION",
            entity_key=row["combination_key"],
            entity_version=row["current_version"],
            current_status=current_status,
            recommended_status=recommended,
            reason=reason,
            trade_date=target,
            funding_gate_hash=row["funding_gate_hash"],
            funding_evidence_revision_at=row[
                "funding_evidence_revision_at"
            ],
            suspension_boundary=suspension_boundary,
        )
        row["recommended_status"] = recommended
        row["recommended_status_label"] = LIFECYCLE_LABELS[recommended]
        row["recommendation_reason"] = reason
        if persist and recommended != current_status:
            transition_plans.append({
                "entity_type": "COMBINATION",
                "entity_key": row["combination_key"],
                "entity_version": row["current_version"],
                "previous_status": current_status,
                "next_status": recommended,
                "reason": reason,
                "evidence": {
                    "run_uid": run_uid, "trade_date": target,
                    "multi_window_gate": row["multi_window_gate"],
                    "funding_gate_hash": row["funding_gate_hash"],
                    "funding_evidence_revision_at": row[
                        "funding_evidence_revision_at"
                    ],
                },
            })
            row["current_status"] = recommended
            row["status_label"] = LIFECYCLE_LABELS[recommended]
            row["status_reason"] = reason
            row["paper_allocation_eligible"] = (
                recommended in {"ACTIVE", "REDUCE"}
                and row["profit_gate_passed"]
                and row.get("market_route_eligible") is True
                and (row.get("constraint_evaluation") or {}).get("passed")
                is True
            )
            row["lane"] = (
                "正式赛道" if recommended in {"ACTIVE", "REDUCE"}
                else "观察赛道" if recommended == "SHADOW"
                else "暂停赛道" if recommended == "SUSPENDED"
                else "历史档案"
            )
    if persist:
        combination_lane_order = {
            "ACTIVE": 0, "REDUCE": 0, "SHADOW": 1,
            "SUSPENDED": 2, "RETIRED": 3,
        }
        combinations.sort(key=lambda row: (
            combination_lane_order.get(row["current_status"], 9),
            -float(row["ranking_score"]),
            row["combination_key"],
        ))
        combination_lane_ranks: dict[str, int] = defaultdict(int)
        for index, row in enumerate(combinations, 1):
            row["rank"] = index
            combination_lane_ranks[row["lane"]] += 1
            row["lane_rank"] = combination_lane_ranks[row["lane"]]
    market_state = strategy_snapshot.get("market_state") or {"key": "unknown", "name": "数据不足"}
    pools = _build_pools(strategy_snapshot, rankings)
    _attach_pool_industry_focus(rankings, pools)
    pool_snapshot, pool_snapshot_hash, _pool_row_hashes = (
        _pool_snapshot_contract(target, pools)
    )
    trading_gate = _snapshot_trading_gate(strategy_snapshot)
    trading_allowed = trading_gate["trading_allowed"] is True
    allocation_candidates = _allocation_candidate_contract(
        rankings, combinations,
    )
    candidate_set_payload = {
        "schema": "probiga.strategy-allocation-candidate-set.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trade_date": target,
        "market_state": str(market_state.get("key") or "unknown"),
        "candidates": allocation_candidates,
    }
    candidate_set_hash = _digest(candidate_set_payload)
    allocations = _allocation(
        rankings, combinations, str(market_state.get("key") or "unknown"),
        trading_allowed=trading_allowed,
        candidate_contract=allocation_candidates,
        trading_gate=trading_gate,
    )
    allocation_snapshot_rows = _allocation_snapshot_contract(allocations)
    allocation_snapshot_payload = {
        "schema": "probiga.strategy-allocation-snapshot.v1",
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trade_date": target,
        "market_state": str(market_state.get("key") or "unknown"),
        "market_risk_cap_pct": MARKET_RISK_CAP_PCT.get(
            str(market_state.get("key") or "unknown"), 0.0,
        ),
        "trading_gate_passed": bool(trading_allowed),
        "candidate_set_hash": candidate_set_hash,
        "allocations": allocation_snapshot_rows,
    }
    allocation_snapshot_hash = _digest(allocation_snapshot_payload)
    router_snapshot_payload = {
        "schema": "probiga.strategy-market-router-snapshot.v1",
        "policy_version": MARKET_ROUTER_POLICY_VERSION,
        "trade_date": target,
        "market_state": str(market_state.get("key") or "unknown"),
        "market_state_config_hash": str(market_state.get("config_hash") or ""),
        "strategy_routes": {
            row["strategy_key"]: row["market_route"]["router_decision_hash"]
            for row in sorted(rankings, key=lambda item: item["strategy_key"])
        },
        "combination_routes": {
            row["combination_key"]: row["market_route"]["router_decision_hash"]
            for row in sorted(combinations, key=lambda item: item["combination_key"])
        },
    }
    router_snapshot_hash = _digest(router_snapshot_payload)
    automatic_transition_plan, automatic_transition_plan_hash = (
        _automatic_transition_plan_contract(
            target, run_uid, transition_plans
        )
    )
    status_counts = {status: sum(1 for row in rankings if row["current_status"] == status) for status in LIFECYCLE_LABELS}
    summary = {
        "strategy_count": len(rankings),
        "formal_count": status_counts["ACTIVE"] + status_counts["REDUCE"],
        "shadow_count": status_counts["SHADOW"],
        "suspended_count": status_counts["SUSPENDED"],
        "retired_count": status_counts["RETIRED"],
        "combination_count": len(combinations),
        "combination_formal_count": sum(1 for row in combinations if row["current_status"] in {"ACTIVE", "REDUCE"}),
        "observation_count": len(pools["observation"]),
        "confirmation_count": len(pools["confirmation"]),
        "tradable_count": len(pools["tradable"]),
        "pool_row_count": pool_snapshot["row_count"],
        "pool_snapshot_hash": pool_snapshot_hash,
        "allocation_count": sum(1 for row in allocations if row["target_type"] != "CASH"),
        "cash_weight_pct": next((row["simulated_weight_pct"] for row in allocations if row["target_type"] == "CASH"), 0.0),
        "strategy_route_eligible_count": sum(
            1 for row in rankings if row.get("market_route_eligible") is True
        ),
        "combination_route_eligible_count": sum(
            1 for row in combinations if row.get("market_route_eligible") is True
        ),
        "router_policy_version": MARKET_ROUTER_POLICY_VERSION,
        "router_snapshot_hash": router_snapshot_hash,
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": bool(trading_allowed),
        "trading_gate_status": trading_gate["status"],
        "trading_gate_status_label": trading_gate["status_label"],
        "trading_gate_reason": trading_gate["reason"],
        "market_risk_cap_pct": MARKET_RISK_CAP_PCT.get(
            str(market_state.get("key") or "unknown"), 0.0,
        ),
        "allocation_candidate_count": len(allocation_candidates),
        "eligible_candidate_count": sum(
            row.get("paper_allocation_eligible") is True
            for row in allocation_candidates
        ),
        "candidate_set_hash": candidate_set_hash,
        "allocation_snapshot_hash": allocation_snapshot_hash,
        "automatic_transition_count": automatic_transition_plan[
            "transition_count"
        ],
        "automatic_transition_plan_hash": automatic_transition_plan_hash,
    }
    input_hash = _governance_source_input_hash(strategy_snapshot)
    build_commit_sha = _governance_build_commit_sha()
    decision_hash = _digest({
        "schema": "strategy-governance-decision.v6",
        "trade_date": target,
        "build_commit_sha": build_commit_sha,
        "input_hash": input_hash,
        "router_snapshot_hash": router_snapshot_hash,
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": bool(trading_allowed),
        "market_risk_cap_pct": summary["market_risk_cap_pct"],
        "allocation_candidate_count": len(allocation_candidates),
        "eligible_candidate_count": summary["eligible_candidate_count"],
        "candidate_set_hash": candidate_set_hash,
        "allocation_snapshot_hash": allocation_snapshot_hash,
        "pool_snapshot_hash": pool_snapshot_hash,
        "strategies": [
            {
                "strategy_key": row["strategy_key"],
                "strategy_version": row["current_version"],
                "enabled": bool(row.get("enabled")),
                "projected_status": row["current_status"],
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for row in rankings
        ],
        "combinations": [
            {
                "combination_key": row["combination_key"],
                "combination_version": row["current_version"],
                "enabled": bool(row.get("enabled")),
                "projected_status": row["current_status"],
                "funding_gate_hash": row["funding_gate_hash"],
            }
            for row in combinations
        ],
    })
    payload = {
        "status": "ok",
        "result_mode": "PERSIST_CANDIDATE" if persist else "PREVIEW_REALTIME",
        "run_uid": run_uid,
        "run_revision": 0,
        "supersedes_run_uid": "",
        "is_canonical": False,
        "trade_date": target,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_status": strategy_snapshot.get("source_status") or "missing",
        "input_ready": input_ready,
        "input_reason": input_reason,
        "input_hash": input_hash,
        "build_commit_sha": build_commit_sha,
        "router_policy_version": MARKET_ROUTER_POLICY_VERSION,
        "router_snapshot_hash": router_snapshot_hash,
        "router_snapshot": router_snapshot_payload,
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "allocation_candidate_set": allocation_candidates,
        "candidate_set_hash": candidate_set_hash,
        "allocation_snapshot_hash": allocation_snapshot_hash,
        "pool_snapshot": pool_snapshot,
        "pool_snapshot_hash": pool_snapshot_hash,
        "automatic_transition_plan": automatic_transition_plan,
        "automatic_transition_plan_hash": automatic_transition_plan_hash,
        "decision_hash": decision_hash,
        "idempotent_replay": False,
        "operator": str(operator or "daily_governance")[:80],
        "trading_gate_passed": trading_allowed,
        "trading_gate": trading_gate,
        "candidate_source": strategy_snapshot.get("candidate_source") or {},
        "market_state": market_state,
        "status_labels": LIFECYCLE_LABELS,
        "adapter_capabilities": strategy_execution_adapter_capabilities()[
            "adapters"
        ],
        "profit_gate_policy": PROFIT_GATE_POLICY,
        "health_score_weights": HEALTH_SCORE_WEIGHTS,
        "summary": summary,
        "strategies": rankings,
        "combinations": combinations,
        "pools": pools,
        "allocations": allocations,
        "lifecycle_transitions": [],
        "automatic_real_order_submission": False,
        "no_qualified_strategy_policy": "没有合格策略或组合时，模拟资金保持现金，不强制交易",
    }
    if persist:
        transitions = []
        connection = _governance_connection
        if connection is None:  # pragma: no cover - guarded above
            raise RuntimeError("持久化治理缺少单写者连接")
        try:
            transaction_scope = (
                nullcontext()
                if connection.in_transaction()
                else connection.begin()
            )
            with transaction_scope:
                locked_registry_rows = connection.execute(text(
                    "SELECT strategy_key, current_version, current_status, "
                    "enabled FROM st_strategy_registry "
                    "ORDER BY strategy_key FOR UPDATE"
                )).mappings().all()
                locked_registry_state = {
                    str(row["strategy_key"]): {
                        "current_version": str(row["current_version"]),
                        "current_status": str(row["current_status"]),
                        "enabled": bool(_int(row.get("enabled"))),
                    }
                    for row in locked_registry_rows
                }
                locked_combination_rows = connection.execute(text(
                    "SELECT combination_key, current_version, "
                    "current_status, enabled "
                    "FROM st_strategy_combination "
                    "ORDER BY combination_key FOR UPDATE"
                )).mappings().all()
                locked_combination_state = {
                    str(row["combination_key"]): {
                        "current_version": str(row["current_version"]),
                        "current_status": str(row["current_status"]),
                        "enabled": bool(_int(row.get("enabled"))),
                    }
                    for row in locked_combination_rows
                }
                if (
                    locked_registry_state != initial_registry_state
                    or locked_combination_state != initial_combination_state
                ):
                    raise RuntimeError(
                        "计算期间策略或组合版本/状态/启停已改变，本轮回滚并要求重算"
                    )
                existing = connection.execute(text(
                    "SELECT run_uid, run_revision, supersedes_run_uid, "
                    "is_canonical, trade_date, input_hash, decision_hash, "
                    "result_json, result_hash FROM st_strategy_governance_run "
                    "WHERE decision_hash=:decision_hash "
                    "AND status='COMPLETED' LIMIT 1"
                ), {"decision_hash": decision_hash}).mappings().first()
                if existing is not None:
                    if not bool(_int(existing.get("is_canonical"))):
                        raise ValueError(
                            "该决定哈希仅存在于已被替代的历史修订；"
                            "必须使用新的输入高水位或构建版本重新治理"
                        )
                    return {
                        **_canonical_governance_result_from_row(dict(existing)),
                        "idempotent_replay": True,
                    }
                # Recheck the date while holding the cross-process writer lock.
                latest = connection.execute(text(
                    "SELECT MAX(trade_date) AS trade_date "
                    "FROM st_strategy_governance_run "
                    "WHERE status='COMPLETED' AND is_canonical=1 FOR UPDATE"
                )).mappings().first()
                latest_day = str((latest or {}).get("trade_date") or "")[:10]
                if latest_day and target < latest_day:
                    raise ValueError(
                        f"历史日期{target}早于当前治理日期{latest_day}，只能回看不能修改当前生命周期"
                    )
                canonical_rows = connection.execute(text(
                    "SELECT run_uid, run_revision "
                    "FROM st_strategy_governance_run "
                    "WHERE trade_date=:trade_date AND status='COMPLETED' "
                    "AND is_canonical=1 "
                    "ORDER BY run_revision DESC, created_at DESC, run_uid DESC "
                    "FOR UPDATE"
                ), {"trade_date": target}).mappings().all()
                if len(canonical_rows) > 1:
                    raise RuntimeError("同一交易日存在多条canonical治理结果，拒绝继续写入")
                previous_canonical = canonical_rows[0] if canonical_rows else None
                payload["run_revision"] = (
                    _int(previous_canonical.get("run_revision"), 0) + 1
                    if previous_canonical else 1
                )
                payload["supersedes_run_uid"] = (
                    str(previous_canonical.get("run_uid") or "")
                    if previous_canonical else ""
                )
                payload["is_canonical"] = True
                if previous_canonical is not None:
                    demoted = connection.execute(text(
                        "UPDATE st_strategy_governance_run "
                        "SET is_canonical=0 WHERE run_uid=:run_uid "
                        "AND is_canonical=1"
                    ), {"run_uid": payload["supersedes_run_uid"]})
                    if demoted.rowcount != 1:
                        raise RuntimeError("前一canonical治理结果已被并发修改")
                if persist_built_strategy_snapshot:
                    from server.engine.strategy_center import (
                        persist_strategy_center_snapshot,
                    )

                    persisted_center = persist_strategy_center_snapshot(
                        strategy_snapshot,
                        ensure_tables=False,
                    )
                    payload["strategy_center_run_uid"] = str(
                        persisted_center.get("run_uid") or ""
                    )
                for plan in transition_plans:
                    transitions.append(transition_lifecycle(
                        plan["entity_key"], plan["next_status"],
                        reason=plan["reason"], operator=operator,
                        evidence=plan["evidence"],
                        entity_type=plan["entity_type"], automatic=True,
                        _connection=connection,
                    ))
                payload["lifecycle_transitions"] = transitions
                _persist_health(connection, run_uid, target, rankings)
                _persist_combinations(connection, payload)
                _persist_run(connection, payload)
        except IntegrityError:
            existing = _db_read(
                "SELECT run_uid, run_revision, supersedes_run_uid, "
                "is_canonical, trade_date, input_hash, decision_hash, "
                "result_json, result_hash FROM st_strategy_governance_run "
                "WHERE decision_hash=:decision_hash "
                "AND status='COMPLETED' LIMIT 1",
                {"decision_hash": decision_hash},
            )
            if existing:
                if not bool(_int(existing[0].get("is_canonical"))):
                    raise
                return {
                    **_canonical_governance_result_from_row(existing[0]),
                    "idempotent_replay": True,
                }
            raise
        payload["lifecycle_transitions"] = transitions
    return payload


def governance_history(limit: int = 100) -> dict[str, Any]:
    if not _table_exists("st_strategy_governance_run"):
        raise RuntimeError("策略治理表尚未由部署流程创建，读取接口不会现场建表")
    limit = max(1, min(500, int(limit)))
    lifecycle_events = _db_read(
        "SELECT * FROM st_strategy_lifecycle_event "
        "ORDER BY occurred_at DESC, event_id DESC LIMIT :limit",
        {"limit": limit},
    )
    for row in lifecycle_events:
        raw_payload = row.get("payload_json")
        payload_value = _json(raw_payload, None)
        row["hash_valid"] = bool(
            isinstance(payload_value, dict)
            and _digest(payload_value) == str(row.get("event_hash") or "")
        )
        row["previous_status_label"] = LIFECYCLE_LABELS.get(
            str(row.get("previous_status") or ""), "未知状态"
        )
        row["next_status_label"] = LIFECYCLE_LABELS.get(
            str(row.get("next_status") or ""), "未知状态"
        )
    audit_events = _db_read(
        "SELECT * FROM st_strategy_governance_audit "
        "ORDER BY created_at DESC, audit_id DESC LIMIT :limit",
        {"limit": limit},
    )
    for row in audit_events:
        payload_value = _json(row.get("payload_json"), None)
        row["hash_valid"] = bool(
            isinstance(payload_value, dict)
            and _digest(payload_value) == str(row.get("audit_hash") or "")
        )
    metric_evidence = _db_read(
        "SELECT evidence_id, entity_type, strategy_key, strategy_version, "
        "as_of_date, window_days, metrics_json, source, evidence_protocol, "
        "artifact_hash, source_dataset_hash, evidence_revision_at, "
        "verification_status, funding_provenance, submitted_by, reviewed_by, reviewed_at, "
        "evidence_hash, created_at FROM st_strategy_metric_input "
        "ORDER BY created_at DESC, evidence_id DESC LIMIT :limit",
        {"limit": limit},
    )
    for row in metric_evidence:
        metrics = _json(row.pop("metrics_json", None), {})
        row["verification_status_label"] = EVIDENCE_STATUS_LABELS.get(
            str(row.get("verification_status") or ""), "未知复核状态"
        )
        row["independent_review"] = bool(
            row.get("reviewed_by")
            and str(row.get("reviewed_by"))
            != str(row.get("submitted_by") or "")
        )
        row["metrics"] = {
            key: metrics.get(key)
            for key in (
                "completed_trades", "coverage_days", "net_expectancy_pct",
                "payoff_ratio", "profit_factor", "max_drawdown_pct",
                "cost_stress_expectancy_pct",
            )
        }
    adapter_run_receipts = _db_read(
        "SELECT run_uid, strategy_key, strategy_version, "
        "strategy_version_hash, trade_date, completed_at, status, "
        "execution_binding_hash, adapter_artifact_sha256, cost_model_hash, "
        "adapter_key, adapter_version, input_hash, output_hash, "
        "stable_result_hash, candidate_count, candidate_identity_json, "
        "receipt_json, receipt_hash, created_at "
        "FROM st_strategy_adapter_run_receipt "
        "ORDER BY completed_at DESC, run_uid DESC LIMIT :limit",
        {"limit": limit},
    )
    for row in adapter_run_receipts:
        raw_receipt = _json(row.get("receipt_json"), None)
        try:
            verified = verify_persisted_strategy_adapter_run_receipt(
                raw_receipt, row,
            )
            row["candidate_identity"] = verified["candidate_identity"]
            row["hash_valid"] = True
        except ValueError:
            row["candidate_identity"] = []
            row["hash_valid"] = False
        row.pop("receipt_json", None)
        row.pop("candidate_identity_json", None)
    return {
        "status": "ok",
        "evidence_status_labels": EVIDENCE_STATUS_LABELS,
        "metric_evidence": metric_evidence,
        "adapter_run_receipts": adapter_run_receipts,
        "lifecycle_events": lifecycle_events,
        "audit_events": audit_events,
        "runs": _db_read(
            "SELECT run_uid, trade_date, run_revision, supersedes_run_uid, "
            "is_canonical, market_state, source_status, "
            "input_ready, input_hash, build_commit_sha, decision_hash, "
            "status, strategy_count, formal_count, shadow_count, "
            "combination_count, observation_count, confirmation_count, "
            "tradable_count, allocation_count, created_at, finished_at "
            "FROM st_strategy_governance_run "
            "ORDER BY trade_date DESC, created_at DESC LIMIT :limit",
            {"limit": limit},
        ),
    }


__all__ = [
    "DECAY_GATE_20_POLICY",
    "HEALTH_SCORE_WEIGHTS",
    "LIFECYCLE_LABELS",
    "LIFECYCLE_TRANSITIONS",
    "PROFIT_GATE_POLICY",
    "calculate_health_score",
    "calculate_return_metrics",
    "ensure_and_seed_governance",
    "evaluate_decay_gate_20",
    "evaluate_profit_gate",
    "evaluate_window_gate",
    "governance_history",
    "governance_input_ready",
    "governance_snapshot",
    "load_canonical_governance_snapshot",
    "metric_evidence_detail",
    "record_metric_input",
    "recommend_lifecycle_status",
    "review_metric_input",
    "register_combination",
    "register_strategy",
    "transition_lifecycle",
    "toggle_strategy_enabled",
    "validate_strategy_key",
]
