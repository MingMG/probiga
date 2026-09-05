# -*- coding: utf-8 -*-
"""Strategy-center domain logic.

The module deliberately keeps the decision layer deterministic and explainable.
It adapts existing recommendation rows into a normalized, multi-strategy view;
it does not place orders or connect to a broker.
"""
from __future__ import annotations

import json
import logging
import math
import re
import uuid
import hashlib
import copy
import threading
import time
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from integrations.bigqmt.reference import PROVIDER_ID
from server.api.routers._engine import get_engine
from server.common.kline_data import get_kline_engine
from server.common.analysis_pool_receipt import (
    ANALYSIS_POOL_PUBLISHER_TASK_TYPES,
)
from server.common.sql_reader import (
    current_bound_sql_connection,
    read_sql_rows,
)
from server.common.versioned_strategy_config import (
    legacy_strategy_merge_map,
    load_market_state_config,
    load_stock_manifest,
    market_state_config_hash,
    stock_manifest_hash,
    stock_strategy_catalog,
    strategy_score_field_map,
    validate_versioned_strategy_runtime,
)
from server.engine.market_state_v2 import transition_market_state
from server.engine.market_trend import compact_market_trend_observation
from server.engine.strategy_execution_adapters import (
    execute_dynamic_adapter_candidate_batch,
    persist_strategy_adapter_run_receipt,
)
from server.engine.dynamic_shadow_ledger import (
    create_dynamic_shadow_trial_plans_from_candidate_facts,
    persist_strategy_adapter_candidate_facts,
)

logger = logging.getLogger(__name__)


def _safe_fallback_log(
    level: int, operation: str, exc: BaseException,
) -> str:
    """Record fallback diagnostics without exception text or traceback data."""

    incident_id = uuid.uuid4().hex
    logger.log(
        level,
        "Strategy center fallback: incident_id=%s exception_type=%s "
        "operation=%s",
        incident_id,
        type(exc).__name__,
        operation,
    )
    return incident_id

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_REFERENCE_POOL_DIR = _PROJECT_ROOT / "data" / "strategy_center"
_MARKET_SNAPSHOT_CACHE: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
_MARKET_SNAPSHOT_CACHE_LOCK = threading.Lock()


STRATEGY_CATALOG: tuple[dict[str, Any], ...] = stock_strategy_catalog()

MARKET_STATES: dict[str, dict[str, Any]] = {
    "trend_bullish": {"name": "趋势偏多", "color": "#dc2626", "description": "指数趋势和市场宽度支持顺势研究"},
    "high_range": {"name": "高位震荡", "color": "#d97706", "description": "位置偏高但宽度收窄，降低追高"},
    "risk_declining": {"name": "风险下降", "color": "#2563eb", "description": "风险释放后出现止跌或修复，逐步恢复确认"},
    "extreme_event": {"name": "极端事件", "color": "#b91c1c", "description": "事件或波动异常，停止新增买入"},
}

STATE_MULTIPLIERS: dict[str, dict[str, float]] = {
    str(state): {str(key): float(value) for key, value in values.items()}
    for state, values in load_market_state_config()["strategy_multipliers"].items()
}

LEGACY_STRATEGY_MAP = legacy_strategy_merge_map()
_STRATEGY_SCORE_FIELDS = strategy_score_field_map()

# The morning external-market task binds one exact persisted snapshot while it
# builds a governance revision.  Ordinary post-close governance runs have no
# bound context and therefore keep their existing scores unchanged.
_EXTERNAL_MARKET_OVERLAY_CONTEXT: ContextVar[dict[str, Any] | None] = (
    ContextVar("strategy_external_market_overlay", default=None)
)


@contextmanager
def bind_external_market_overlay(context: Mapping[str, Any] | None):
    """Bind one external snapshot to the current governance execution only."""

    value = dict(context) if isinstance(context, Mapping) else None
    token = _EXTERNAL_MARKET_OVERLAY_CONTEXT.set(value)
    try:
        yield
    finally:
        _EXTERNAL_MARKET_OVERLAY_CONTEXT.reset(token)


def external_market_score_adjustment(
    context: Mapping[str, Any] | None,
) -> float:
    """Return a neutral-by-default, bounded global risk-score adjustment."""

    if not isinstance(context, Mapping):
        return 0.0
    quality = str(
        context.get("external_market_data_quality") or "UNKNOWN"
    ).strip().upper()
    status = str(
        context.get("external_market_status") or "UNKNOWN"
    ).strip().upper()
    if quality not in {"PASS", "WATCH"} or status not in {
        "SUPPORT", "RISK", "NEUTRAL",
    }:
        return 0.0
    score = _num(context.get("external_market_score"), None)
    if score is None:
        return 0.0
    return round(max(-3.0, min(3.0, (score - 50.0) * 0.15)), 2)


def apply_external_market_score_overlay(
    rows: Iterable[dict[str, Any]],
    context: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Copy recommendation rows and apply the same bounded market-risk tilt."""

    adjustment = external_market_score_adjustment(context)
    summary = dict(context) if isinstance(context, Mapping) else {}
    score_fields = {
        "ai_score",
        "final_trade_score",
        "quality_score",
        "short_term_score",
        *_STRATEGY_SCORE_FIELDS.values(),
    }
    adjusted: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        if adjustment:
            for field in score_fields:
                current = _num(row.get(field), None)
                if current is not None:
                    row[field] = round(max(0.0, min(100.0, current + adjustment)), 2)
        row.update({
            "external_market_adjustment": adjustment,
            "external_market_score": _num(
                summary.get("external_market_score"), 50.0
            ),
            "external_market_status": str(
                summary.get("external_market_status") or "UNKNOWN"
            ).upper(),
            "external_market_data_quality": str(
                summary.get("external_market_data_quality") or "UNKNOWN"
            ).upper(),
            "external_market_snapshot_id": str(
                summary.get("snapshot_id") or ""
            ),
            "external_market_captured_at": str(
                summary.get("captured_at")
                or summary.get("external_market_captured_at")
                or ""
            )[:19],
        })
        adjusted.append(row)
    return adjusted

_STRATEGY_BY_KEY = {item["key"]: item for item in STRATEGY_CATALOG}
_ACTIONABLE_NEW_BUY_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _is_explicit_database_true(value: Any) -> bool:
    """Recognize only a real bool or a MySQL TINYINT(1) value."""
    return value is True or (type(value) is int and value == 1)


def _clamp(value: Any, low: float = 0.0, high: float = 100.0, default: float | None = None) -> float | None:
    number = _num(value, default)
    if number is None:
        return default
    return round(max(low, min(high, number)), 2)


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _json_text(value: Any, default: Any) -> str:
    return json.dumps(_json_value(value, default), ensure_ascii=False, separators=(",", ":"))


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _candidate_reconstruction_contract(
    trade_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Carry one exact retrospective PIT proof through candidate governance."""

    proofs: list[dict[str, Any]] = []
    declared_rows = 0
    for row in rows:
        detail = _json_value(row.get("event_risk_detail"), {})
        if not isinstance(detail, dict):
            detail = {}
        mode = str(detail.get("pit_reconstruction_mode") or "").strip()
        if not mode:
            continue
        declared_rows += 1
        provenance = detail.get("pit_reconstruction_provenance")
        reconstruction_sha = str(
            detail.get("pit_reconstruction_sha256") or ""
        ).lower()
        reconstructed_at = str(
            detail.get("pit_reconstructed_at") or ""
        ).strip()
        if not isinstance(provenance, dict):
            return {
                "schema": "probiga.strategy-candidate-reconstruction.v1",
                "mode": "INVALID",
                "reason": "推荐明细缺少历史重建来源证明",
            }
        core = {
            str(key): value for key, value in provenance.items()
            if str(key) != "reconstruction_sha256"
        }
        try:
            source_cutoff = datetime.fromisoformat(str(
                provenance.get("source_query_cutoff_at") or ""
            ))
            reconstructed = datetime.fromisoformat(reconstructed_at)
            if source_cutoff.tzinfo is not None or reconstructed.tzinfo is not None:
                raise ValueError("reconstruction timestamps must be local")
        except ValueError:
            return {
                "schema": "probiga.strategy-candidate-reconstruction.v1",
                "mode": "INVALID",
                "reason": "推荐明细历史重建时间无效",
            }
        if (
            mode != "HISTORICAL_RECONSTRUCTION"
            or provenance.get("schema")
            != "probiga.qmt-announcement-historical-reconstruction.v2"
            or provenance.get("mode") != mode
            or provenance.get("target_trade_date") != trade_date
            or provenance.get("reconstruction_sha256") != reconstruction_sha
            or _canonical_hash(core) != reconstruction_sha
            or provenance.get("reconstructed_at") != reconstructed_at
            or provenance.get("known_at") != reconstructed_at
            or provenance.get("provider") != "cninfo.announcement"
            or provenance.get("source") != "cninfo.announcement"
            or re.fullmatch(
                r"[0-9a-f]{32}",
                str(provenance.get("scheduler_run_uid") or ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(provenance.get("build_sha") or ""),
            ) is None
            or provenance.get("build_sha") == "0" * 40
            or source_cutoff.date().isoformat() != trade_date
            or source_cutoff.time() != datetime.max.time()
            or reconstructed <= source_cutoff
            or provenance.get("automatic_real_order_submission") is not False
            or provenance.get("real_order_authority") is not False
        ):
            return {
                "schema": "probiga.strategy-candidate-reconstruction.v1",
                "mode": "INVALID",
                "reason": "推荐明细历史重建来源证明漂移",
            }
        proofs.append(dict(provenance))
    if not declared_rows:
        return {
            "schema": "probiga.strategy-candidate-reconstruction.v1",
            "mode": "NONE",
            "trade_date": trade_date,
        }
    if declared_rows != len(rows) or not proofs:
        return {
            "schema": "probiga.strategy-candidate-reconstruction.v1",
            "mode": "INVALID",
            "reason": "推荐明细仅部分携带历史重建证明",
        }
    first = proofs[0]
    if any(value != first for value in proofs[1:]):
        return {
            "schema": "probiga.strategy-candidate-reconstruction.v1",
            "mode": "INVALID",
            "reason": "推荐明细历史重建证明不一致",
        }
    return {
        "schema": "probiga.strategy-candidate-reconstruction.v1",
        "mode": "HISTORICAL_RECONSTRUCTION",
        "trade_date": trade_date,
        "reconstruction_sha256": first["reconstruction_sha256"],
        "reconstructed_at": first["reconstructed_at"],
        "known_at": first["known_at"],
        "provenance": first,
    }


def _recommendation_publication_proof(
    trade_date: str,
    *,
    source_row_count: int,
) -> dict[str, Any]:
    """Verify that the mutable recommendation partition was published.

    A successful ``COUNT(*)`` proves only that a database query completed. It
    cannot distinguish a legitimately empty strategy run from a missed or
    failed producer.  The terminal publication history is the authority for
    that distinction.
    """

    target = normalize_trade_date(trade_date)
    proof: dict[str, Any] = {
        "schema": "probiga.strategy-candidate-publication-proof.v1",
        "status": "NOT_PUBLISHED",
        "trade_date": target,
        "publisher_run_uid": "",
        "publisher_task_type": "",
        "published_at": None,
        "finished_at": None,
        "published_row_count": None,
        "canonical_pool_sha256": "",
        "reason": "候选源缺少同交易日成功发布回执",
    }
    if not target or not _table_exists("st_recommended_run_history"):
        return proof
    required = {
        "run_uid", "trade_date", "status", "finished_at", "published_at",
        "publisher_task_type", "canonical_pool_sha256", "passed", "total",
        "build_sha", "membership_snapshot_date", "membership_snapshot_source",
        "membership_proof_sha256",
    }
    if not required.issubset(_table_columns("st_recommended_run_history")):
        return {
            **proof,
            "status": "INVALID_SCHEMA",
            "reason": "推荐发布历史缺少完整回执字段",
        }
    try:
        rows = _db_read(
            """
            SELECT run_uid, trade_date, status, finished_at, published_at,
                   publisher_task_type, canonical_pool_sha256, passed, total,
                   build_sha, membership_snapshot_date,
                   membership_snapshot_source, membership_proof_sha256
            FROM st_recommended_run_history
            WHERE trade_date=:trade_date
              AND status='done'
            ORDER BY published_at DESC, finished_at DESC, id DESC
            LIMIT 2
            """,
            {"trade_date": target},
        )
    except Exception as exc:
        return {
            **proof,
            "status": "UNAVAILABLE",
            "reason": f"推荐发布回执读取失败：{type(exc).__name__}",
        }
    if not rows:
        return proof
    row = dict(rows[0])
    run_uid = str(row.get("run_uid") or "").strip().lower()
    task_type = str(row.get("publisher_task_type") or "").strip()
    canonical_hash = str(
        row.get("canonical_pool_sha256") or ""
    ).strip().lower()
    build_sha = str(row.get("build_sha") or "").strip().lower()
    membership_hash = str(
        row.get("membership_proof_sha256") or ""
    ).strip().lower()
    try:
        published_count = int(row.get("passed"))
        analysis_count = int(row.get("total"))
    except (TypeError, ValueError):
        published_count = -1
        analysis_count = -1
    valid = all((
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is not None,
        task_type in ANALYSIS_POOL_PUBLISHER_TASK_TYPES,
        re.fullmatch(r"[0-9a-f]{64}", canonical_hash) is not None,
        re.fullmatch(r"[0-9a-f]{40}", build_sha) is not None,
        build_sha != "0" * 40,
        re.fullmatch(r"[0-9a-f]{64}", membership_hash) is not None,
        str(row.get("trade_date") or "")[:10] == target,
        str(row.get("membership_snapshot_date") or "")[:10] == target,
        bool(str(row.get("membership_snapshot_source") or "").strip()),
        row.get("published_at") is not None,
        row.get("finished_at") is not None,
        published_count == int(source_row_count),
        analysis_count >= published_count >= 0,
    ))
    if not valid:
        return {
            **proof,
            "status": "MISMATCH",
            "publisher_run_uid": run_uid,
            "publisher_task_type": task_type,
            "published_at": row.get("published_at"),
            "finished_at": row.get("finished_at"),
            "published_row_count": published_count,
            "canonical_pool_sha256": canonical_hash,
            "reason": "推荐发布回执与当前候选分区不一致",
        }
    return {
        **proof,
        "status": "COMPLETED",
        "publisher_run_uid": run_uid,
        "publisher_task_type": task_type,
        "published_at": row.get("published_at"),
        "finished_at": row.get("finished_at"),
        "published_row_count": published_count,
        "analysis_row_count": analysis_count,
        "canonical_pool_sha256": canonical_hash,
        "build_sha": build_sha,
        "membership_snapshot_source": str(
            row.get("membership_snapshot_source") or ""
        ),
        "membership_proof_sha256": membership_hash,
        "reason": "同交易日推荐任务已成功发布并回读验证",
    }


def _candidate_source_contract(
    trade_date: str,
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    *,
    reference_pool: dict[str, Any] | None = None,
    dynamic_adapter_statuses: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Prove that an empty candidate list came from a completed source read."""

    target = normalize_trade_date(trade_date)
    source = "dated_reference_pool" if reference_pool else "st_recommended_stocks"
    status = "COMPLETED"
    reason = "候选源读取完成，零行也有明确完成证明"
    query_completed = True
    source_row_count = len(rows)
    if reference_pool:
        if not target or not reference_pool.get("_path"):
            status, query_completed = "INCOMPLETE", False
            reason = "日期化候选源缺少可验证路径或交易日"
    elif not _table_exists("st_recommended_stocks"):
        status, query_completed = "MISSING", False
        reason = "候选源表st_recommended_stocks不存在"
    else:
        columns = _table_columns("st_recommended_stocks")
        if not {"stock_code", "pick_date"}.issubset(columns):
            status, query_completed = "INVALID_SCHEMA", False
            reason = "候选源表缺少stock_code或pick_date"
        else:
            try:
                count_rows = _db_read(
                    "SELECT COUNT(*) AS cnt FROM st_recommended_stocks "
                    "WHERE pick_date=:trade_date",
                    {"trade_date": target},
                )
                source_row_count = int(
                    count_rows[0].get("cnt") if count_rows else 0
                )
                if source_row_count != len(rows):
                    status, query_completed = "INCOMPLETE", False
                    reason = (
                        "候选源明细未完整读取："
                        f"源行数{source_row_count}，已加载{len(rows)}"
                    )
            except Exception as exc:
                status, query_completed = "INCOMPLETE", False
                reason = f"候选源完成证明查询失败：{type(exc).__name__}"
    publication_proof: dict[str, Any] | None = None
    if not reference_pool and status == "COMPLETED":
        publication_proof = _recommendation_publication_proof(
            target,
            source_row_count=source_row_count,
        )
        if publication_proof.get("status") != "COMPLETED":
            status, query_completed = (
                str(publication_proof.get("status") or "NOT_PUBLISHED"),
                False,
            )
            reason = str(
                publication_proof.get("reason")
                or "候选源缺少成功发布回执"
            )
        else:
            publisher_uid = str(
                publication_proof.get("publisher_run_uid") or ""
            )
            row_publishers = {
                str(row.get("publisher_run_uid") or "").strip().lower()
                for row in rows
            }
            if source_row_count and row_publishers != {publisher_uid}:
                status, query_completed = "MISMATCH", False
                reason = "候选明细与成功发布回执不属于同一运行"
                publication_proof = {
                    **publication_proof,
                    "status": "MISMATCH",
                    "reason": reason,
                }
            else:
                reason = str(publication_proof.get("reason") or reason)
    dynamic_statuses = [
        dict(item) for item in (dynamic_adapter_statuses or [])
        if isinstance(item, dict) and str(item.get("strategy_key") or "")
    ]
    invalid_dynamic_runs = [
        item for item in dynamic_statuses
        if item.get("enabled") is True
        and str(item.get("lifecycle_status") or "")
        not in {"RETIRED", "SUSPENDED"}
        and str(item.get("adapter_capability_status") or "")
        == "RESEARCH_READY"
        and item.get("run_receipt_valid") is not True
    ]
    if invalid_dynamic_runs:
        status, query_completed = "INCOMPLETE", False
        reason = "动态策略候选运行缺少完整CandidateBatch回执"
    dynamic_receipts = [
        dict(item.get("candidate_run_receipt") or {})
        for item in sorted(
            dynamic_statuses,
            key=lambda value: str(value.get("strategy_key") or ""),
        )
        if item.get("run_receipt_valid") is True
    ]
    dynamic_results = [
        {
            "strategy_key": str(item.get("strategy_key") or ""),
            "strategy_version": str(item.get("strategy_version") or ""),
            "execution_binding_hash": str(
                item.get("execution_binding_hash") or ""
            ),
            "adapter_artifact_sha256": str(
                item.get("adapter_artifact_sha256") or ""
            ),
            "cost_model_hash": str(item.get("cost_model_hash") or ""),
            "trade_date": target,
            "candidate_input_hash": str(
                item.get("candidate_input_hash") or ""
            ),
            "candidate_output_hash": str(
                item.get("candidate_output_hash") or ""
            ),
            "candidate_stable_result_hash": str(
                item.get("candidate_stable_result_hash") or ""
            ),
            "candidate_count": int(item.get("candidate_count") or 0),
            "status": str(item.get("status") or ""),
        }
        for item in sorted(
            dynamic_statuses,
            key=lambda value: (
                str(value.get("strategy_key") or ""),
                str(value.get("strategy_version") or ""),
            ),
        )
        if item.get("run_receipt_valid") is True
    ]
    reconstruction = _candidate_reconstruction_contract(target, rows)
    if reconstruction.get("mode") == "INVALID":
        status, query_completed = "INCOMPLETE", False
        reason = str(
            reconstruction.get("reason") or "历史重建来源证明无效"
        )
    payload = {
        "schema": "probiga.strategy-candidate-source.v2",
        "source": source,
        "status": status,
        "query_completed": query_completed,
        "trade_date": target,
        "data_date": target,
        "source_row_count": source_row_count,
        "loaded_row_count": len(rows),
        "loaded_rows_hash": _canonical_hash(rows),
        "candidate_count": len(candidates),
        "candidate_identity": sorted(
            {
                str(item.get("stock_code") or "").zfill(6)
                for item in candidates
                if str(item.get("stock_code") or "").strip()
            }
        ),
        "dynamic_adapter_results": dynamic_results,
        "dynamic_adapter_results_hash": _canonical_hash(dynamic_results),
        "publication_proof": publication_proof,
        "pit_reconstruction": reconstruction,
        "reason": reason,
    }
    return {
        **payload,
        "source_hash": _canonical_hash(payload),
        # Audit identities remain visible, but do not revise the authoritative
        # candidate-source hash when stable input/output are unchanged.
        "dynamic_adapter_receipts": dynamic_receipts,
    }


def _strategy_discovery_counts(
    configs: dict[str, dict[str, Any]],
    dynamic_adapter_statuses: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Count unbounded discovered keys without double-counting migrations.

    A runtime-registry entry with the same key supersedes the research-only
    static enabled flag for the compatibility aggregate. Canonical governance
    continues to publish its own registry counts independently.
    """

    static_keys = {
        str(item.get("key") or "").strip()
        for item in STRATEGY_CATALOG
        if str(item.get("key") or "").strip()
    }
    enabled_static_keys = {
        key for key in static_keys
        if bool((configs.get(key) or {}).get("enabled", True))
    }
    runtime_enabled_by_key: dict[str, bool] = {}
    runtime_discovery_complete = True
    for raw in dynamic_adapter_statuses:
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("strategy_key") or "").strip()
        if not key:
            if str(raw.get("status") or "") == "DISCOVERY_UNAVAILABLE":
                runtime_discovery_complete = False
            continue
        enabled = raw.get("enabled") is True
        runtime_enabled_by_key[key] = (
            runtime_enabled_by_key.get(key, True) and enabled
        )
    runtime_keys = set(runtime_enabled_by_key)
    enabled_runtime_keys = {
        key for key, enabled in runtime_enabled_by_key.items() if enabled
    }
    discovered_keys = static_keys | runtime_keys
    enabled_discovered_keys = (
        (enabled_static_keys - runtime_keys) | enabled_runtime_keys
    )
    return {
        "static_catalog_count": len(static_keys),
        "enabled_static_catalog_count": len(enabled_static_keys),
        "runtime_registry_count": len(runtime_keys),
        "enabled_runtime_count": len(enabled_runtime_keys),
        "total_discovered_strategy_count": len(discovered_keys),
        "enabled_discovered_strategy_count": len(enabled_discovered_keys),
        "runtime_registry_discovery_status": (
            "COMPLETE" if runtime_discovery_complete else "UNAVAILABLE"
        ),
        # Compatibility fields now reflect the complete unique discovery set,
        # not the fixed static catalog.
        "strategy_count": len(discovered_keys),
        "enabled_count": len(enabled_discovered_keys),
        "strategy_count_semantics": "UNIQUE_DISCOVERED_STRATEGY_KEYS",
        "enabled_count_semantics": (
            "RUNTIME_REGISTRY_OVERRIDES_STATIC_ON_KEY_COLLISION"
        ),
        "canonical_governance_count_source": "strategy_governance_registry",
    }


def normalize_trade_date(value: str | None) -> str:
    raw = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(raw).isoformat() if raw else ""
    except ValueError:
        return ""


def strategy_item(strategy_key: str) -> dict[str, Any] | None:
    return _STRATEGY_BY_KEY.get(str(strategy_key or "").strip())


def market_state_info(state: str) -> dict[str, Any]:
    key = state if state in MARKET_STATES else "risk_declining"
    return {"key": key, **MARKET_STATES[key]}


def infer_market_state(
    snapshot: dict[str, Any] | None,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply the frozen V2 classifier, including hysteresis and cooldown."""
    result = transition_market_state(snapshot, previous=previous)
    key = str(result.get("final_state") or result.get("key") or "unknown")
    if key == "unknown":
        return {
            **result,
            "key": "unknown",
            "name": "数据不足",
            "color": "#64748b",
            "description": "缺少必要市场输入，禁止生成确定性新增买入动作",
            "confidence": 0.0,
        }
    confidence = {
        "extreme_event": 92.0,
        "risk_declining": 76.0,
        "high_range": 72.0,
        "trend_bullish": 80.0,
    }.get(key, 0.0)
    return {
        **market_state_info(key),
        **result,
        "key": key,
        "confidence": confidence,
    }


def performance_multiplier(metric: dict[str, Any] | None) -> float:
    metric = metric or {}
    sample_count = int(_num(metric.get("sample_count"), 0) or 0)
    if sample_count < 10:
        return 1.0
    win_rate = _num(metric.get("win_rate_pct", metric.get("win_rate")), 50.0) or 50.0
    avg_return = _num(metric.get("avg_return_pct", metric.get("avg_profit_rate")), 0.0) or 0.0
    if win_rate < 42 or avg_return < -1:
        return 0.72
    if win_rate >= 58 and avg_return >= 1:
        return 1.08
    return 1.0


def effective_weight(strategy_key: str, state: str, config: dict[str, Any] | None = None, metric: dict[str, Any] | None = None, data_quality: float = 1.0) -> dict[str, float | str]:
    strategy_key = LEGACY_STRATEGY_MAP.get(strategy_key, strategy_key) or ""
    item = strategy_item(strategy_key) or {"base_weight": 0.0}
    config = config or {}
    base = _num(config.get("base_weight"), _num(item.get("base_weight"), 1.0)) or 0.0
    state_multiplier = (STATE_MULTIPLIERS.get(state) or {}).get(strategy_key, 0.0 if state == "extreme_event" else 1.0)
    quality_multiplier = _clamp(data_quality, 0.0, 1.0, 1.0) or 1.0
    perf_multiplier = performance_multiplier(metric)
    enabled = bool(config.get("enabled", True))
    weight = base * state_multiplier * perf_multiplier * quality_multiplier
    if not enabled:
        weight = 0.0
    weight = round(max(0.0, min(1.5, weight)), 4)
    reason = []
    if not enabled:
        reason.append("策略已停用")
    if state_multiplier < 1:
        reason.append(f"市场状态系数 {state_multiplier:.2f}")
    elif state_multiplier > 1:
        reason.append(f"市场状态加权 {state_multiplier:.2f}")
    if perf_multiplier != 1:
        reason.append(f"复盘表现系数 {perf_multiplier:.2f}")
    if quality_multiplier < 1:
        reason.append(f"数据质量系数 {quality_multiplier:.2f}")
    return {
        "base_weight": round(base, 4),
        "state_multiplier": round(state_multiplier, 4),
        "performance_multiplier": round(perf_multiplier, 4),
        "data_quality_multiplier": round(quality_multiplier, 4),
        "effective_weight": weight,
        "weight_reason": "；".join(reason) or "按基础权重运行",
    }


def resolve_conflict(signals: Iterable[dict[str, Any]], market_state: str = "") -> dict[str, Any]:
    """Resolve opposing signals without hiding any source signal."""
    source = [dict(item) for item in signals if isinstance(item, dict)]
    if not source:
        return {
            "final_direction": "NO_SIGNAL", "final_status": "INSUFFICIENT_DATA", "buy_score": 0.0,
            "sell_score": 0.0, "hold_score": 0.0, "dominant_strategy": "", "conflict": False,
            "blocking_reasons": ["没有策略信号"], "conflict_summary": "暂无可用策略信号",
        }

    hard_blocks = [
        item for item in source
        if str(item.get("gate_status") or "").upper() == "BLOCK"
        or str(item.get("risk_level") or "").upper() == "CRITICAL"
    ]
    buy_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) == "BUY")
    sell_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) == "SELL")
    hold_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) in {"HOLD", "NO_SIGNAL"})
    best = max(source, key=lambda item: (_num(item.get("effective_score"), 0.0) or 0.0, _num(item.get("model_confidence"), 0.0) or 0.0))
    reasons = [str(item.get("gate_reason") or "") for item in hard_blocks if item.get("gate_reason")]

    if hard_blocks:
        final_direction = "SELL" if any(str(item.get("signal_direction")) == "SELL" for item in hard_blocks) else "HOLD"
        return {
            "final_direction": final_direction, "final_status": "BLOCKED", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": bool(buy_score and sell_score),
            "blocking_reasons": reasons or ["存在硬性风险门禁"],
            "conflict_summary": "硬性风险门禁优先，阻断新增买入",
        }

    gap = abs(buy_score - sell_score)
    conflict = buy_score > 0 and sell_score > 0
    if conflict and gap < max(10.0, max(buy_score, sell_score) * 0.2):
        return {
            "final_direction": "HOLD", "final_status": "CONFLICT", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": True,
            "blocking_reasons": [], "conflict_summary": "多空有效权重接近，暂不形成新增买入信号",
        }

    if market_state == "extreme_event" and buy_score >= sell_score:
        return {
            "final_direction": "HOLD", "final_status": "BLOCKED", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": ["极端事件模式停止新增买入"], "conflict_summary": "极端事件模式下仅保留观察和防守信息",
        }

    if buy_score > sell_score and buy_score >= max(30.0, hold_score):
        status = "READY" if not conflict or gap >= 18 else "WATCH"
        if market_state in {"high_range", "risk_declining"}:
            status = "WATCH"
        return {
            "final_direction": "BUY", "final_status": status, "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": [], "conflict_summary": "有效权重偏多，但仍需满足价格与板块触发条件",
        }
    if sell_score > buy_score and sell_score >= max(30.0, hold_score):
        return {
            "final_direction": "SELL", "final_status": "SELL_ALERT", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": [], "conflict_summary": "有效权重偏空，优先查看止损/减仓条件",
        }
    return {
        "final_direction": "HOLD", "final_status": "WATCH", "buy_score": round(buy_score, 2),
        "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
        "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
        "blocking_reasons": [], "conflict_summary": "信号强度不足，维持观察",
    }


def _legacy_keys(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    raw = _json_value(row.get("suitable_strategies"), [])
    if not isinstance(raw, list):
        raw = [part.strip() for part in str(row.get("suitable_strategies") or "").split(",") if part.strip()]
    for value in raw + [row.get("primary_strategy"), row.get("strategy_profile")]:
        key = str(value or "").strip()
        if not key:
            continue
        mapped = LEGACY_STRATEGY_MAP.get(key, key)
        if mapped and mapped in _STRATEGY_BY_KEY and mapped not in values:
            values.append(mapped)
    if not values and (
        row.get("short_term_score") is not None
        or row.get("final_trade_score") is not None
    ):
        values.append("short_term")
    return values


def _score_for_strategy(row: dict[str, Any], strategy_key: str) -> float | None:
    field = _STRATEGY_SCORE_FIELDS.get(strategy_key)
    if not field:
        return None
    value = _clamp(row.get(field), default=None)
    return value if value is not None and value > 0 else None


def _strategy_signal_basis(
    row: dict[str, Any],
    strategy_key: str,
    score: float | None,
) -> dict[str, Any]:
    """Separate strategy-specific confirmation from the generic recommendation.

    The generic recommendation is deliberately conservative and may suspend a
    trend trade for valuation or a short trade for weak fundamentals.  Those
    are strategy-relative concerns, not universal data failures.  This helper
    keeps universal hard blocks authoritative while applying the frozen
    per-strategy block list before confirming BUY.
    """
    manifest = load_stock_manifest()
    routing = manifest.get("paper_trial_routing") or {}
    raw_flags = _json_value(row.get("data_quality_flags"), [])
    flags = {
        str(value).strip()
        for value in (raw_flags if isinstance(raw_flags, list) else [])
        if str(value).strip()
    }
    hard_flags = {
        str(value)
        for value in (routing.get("hard_block_flags") or [])
    }
    strategy_flags = {
        str(value)
        for value in (
            (routing.get("strategy_block_flags") or {}).get(
                strategy_key,
                [],
            )
        )
    }
    hard_hits = sorted(flags & hard_flags)
    strategy_hits = sorted(flags & strategy_flags)
    recommend_status = str(
        row.get("recommend_status") or "SUSPENDED"
    ).upper()
    source_status = str(
        row.get("signal_status") or recommend_status or "WATCH"
    ).upper()
    risk_level = str(row.get("event_risk_level") or "DATA_BLOCKED").upper()
    chase_risk_status = str(
        row.get("chase_risk_status") or "DATA_BLOCKED"
    ).upper()
    ordinary_buy_eligible = _is_explicit_database_true(
        row.get("ordinary_buy_eligible")
    )
    publication_status = str(
        row.get("publication_status") or "PENDING"
    ).upper()
    item = next(
        (
            value
            for value in (manifest.get("strategies") or [])
            if str(value.get("key") or "") == strategy_key
        ),
        {},
    )
    parameters = item.get("parameters") or {}
    min_score = _num(parameters.get("min_score"), 100.0) or 100.0
    confirm_score = (
        _num(parameters.get("confirm_score"), min_score) or min_score
    )
    explicit_strategy_signal = (
        str(row.get("main_wave_signal") or "").upper()
        if strategy_key == "main_wave"
        else ""
    )

    # Exit/reduce signals are never suppressed by a new-buy qualification gate.
    if source_status in {"SELL_ALERT", "REDUCE"} or (
        strategy_key == "main_wave"
        and explicit_strategy_signal in {"SELL_ALERT", "REDUCE"}
    ):
        return {
            "direction": "SELL",
            "hard_block": False,
            "reason": "上游趋势失效或减仓信号已触发",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if publication_status != "ACTIVE":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": "推荐票池尚未通过调度后验证并激活",
            "hard_hits": [*hard_hits, "publication_not_active"],
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if recommend_status != "ALLOW":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": str(
                row.get("recommend_reason")
                or f"recommend gate is {recommend_status}, not ALLOW"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if source_status not in _ACTIONABLE_NEW_BUY_SIGNAL_STATUSES:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": f"upstream signal {source_status} is not confirmed",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if chase_risk_status != "ALLOW" or not ordinary_buy_eligible:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": (
                "追高与成交能力硬门未显式通过："
                f"{chase_risk_status}"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if recommend_status == "BLOCK":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": str(
                row.get("recommend_reason")
                or "基础数据或个股硬风险门槛未通过"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if risk_level in {"HIGH", "CRITICAL", "DATA_BLOCKED", "UNKNOWN"}:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": f"个股事件风险为 {risk_level}",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if hard_hits:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": "通用硬门槛触发：" + "、".join(hard_hits[:4]),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if strategy_hits:
        return {
            "direction": "HOLD",
            "hard_block": False,
            "reason": (
                f"{strategy_key} 专属门槛触发："
                + "、".join(strategy_hits[:4])
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    confirmed = score is not None and score >= confirm_score
    if (
        strategy_key == "main_wave"
        and explicit_strategy_signal == "BUY_READY"
        and score is not None
        and score >= min_score
    ):
        confirmed = True
    return {
        "direction": "BUY" if confirmed else "HOLD",
        "hard_block": False,
        "reason": (
            f"{strategy_key} 独立得分 {float(score or 0):.1f}"
            f"/确认线 {confirm_score:.1f}"
        ),
        "hard_hits": hard_hits,
        "strategy_hits": strategy_hits,
        "min_score": min_score,
        "confirm_score": confirm_score,
    }


def adapt_recommendation_row(row: dict[str, Any], strategy_key: str, market: dict[str, Any], config: dict[str, Any] | None = None, metric: dict[str, Any] | None = None) -> dict[str, Any]:
    strategy_key = LEGACY_STRATEGY_MAP.get(strategy_key, strategy_key) or ""
    state = str(market.get("market_state") or "risk_declining")
    is_reference = bool(row.get("reference_fixture"))
    score = _score_for_strategy(row, strategy_key)
    data_quality_score = _clamp(row.get("data_quality_score"), default=0.0) or 0.0
    weight = effective_weight(strategy_key, state, config=config, metric=metric, data_quality=data_quality_score / 100.0)
    risk_level = str(row.get("event_risk_level") or "DATA_BLOCKED").upper()
    row_status = str(row.get("signal_status") or row.get("recommend_status") or "WATCH").upper()
    signal_basis = _strategy_signal_basis(row, strategy_key, score)
    direction = str(signal_basis["direction"])

    gate_status = "PASS"
    gate_reason = ""
    if not bool((config or {}).get("enabled", True)):
        gate_status, gate_reason = "BLOCK", "策略已停用"
    elif bool(signal_basis.get("hard_block")):
        gate_status, gate_reason = "BLOCK", str(
            signal_basis.get("reason") or "个股硬风险门槛未通过"
        )
    elif state == "extreme_event" and direction == "BUY":
        gate_status, gate_reason = "BLOCK", "极端事件模式停止新增买入"
    elif state in {"high_range", "risk_declining"} and direction == "BUY":
        gate_status, gate_reason = "REDUCE", f"{MARKET_STATES[state]['name']}模式自动降权，需二次确认"
    elif str(row.get("recommend_status") or "").upper() == "BLOCK":
        gate_status, gate_reason = "BLOCK", str(row.get("recommend_reason") or "基础推荐门禁未通过")
    elif direction == "HOLD" and signal_basis.get("strategy_hits"):
        gate_status, gate_reason = "REDUCE", str(
            signal_basis.get("reason") or "策略专属条件尚未满足"
        )

    confidence = _clamp(row.get("confidence_score"), default=None)
    if confidence is None and score is not None:
        confidence = _clamp(50 + abs(score - 50) * 0.6, default=50.0)
    if score is None and not is_reference:
        status = "INSUFFICIENT_DATA"
        direction = "NO_SIGNAL"
        confidence = None
        gate_status = "BLOCK"
        gate_reason = "当前策略缺少独立可追溯分数"
    elif gate_status == "BLOCK":
        status = "BLOCKED"
    elif gate_status == "REDUCE":
        status = "WATCH"
    elif direction == "BUY":
        status = "READY"
    elif direction == "SELL":
        status = "SELL_ALERT"
    else:
        status = "WATCH"

    if is_reference and score is None:
        # A dated reference pool is deliberately a watchlist, not a fabricated
        # model score. It remains visible while the price/sector confirmation
        # gates are pending.
        direction = str(row.get("reference_signal_direction") or "HOLD").upper()
        if direction not in {"BUY", "SELL", "HOLD"}:
            direction = "HOLD"
        if gate_status != "BLOCK":
            gate_status = "REDUCE"
            gate_reason = "日期化研究候选，等待盘前/盘中价格与板块条件确认"
        status = "WATCH" if gate_status != "BLOCK" else "BLOCKED"
        confidence = None

    evidence = _json_value(row.get("evidence_chain_json"), [])
    if not isinstance(evidence, list):
        evidence = [evidence]
    evidence = evidence[:30]
    evidence.append({
        "module": "strategy_center",
        "text": gate_reason or "兼容现有推荐数据生成策略信号",
        "source": "dated_reference_pool" if is_reference else "existing_recommendation",
    })
    if "external_market_adjustment" in row:
        evidence.append({
            "module": "external_market_overlay",
            "text": (
                "外围市场全局风险修正 "
                f"{float(row.get('external_market_adjustment') or 0.0):+.2f}分"
            ),
            "source": "st_external_market_context",
            "snapshot_id": row.get("external_market_snapshot_id") or "",
            "captured_at": row.get("external_market_captured_at") or "",
            "status": row.get("external_market_status") or "UNKNOWN",
            "data_quality": (
                row.get("external_market_data_quality") or "UNKNOWN"
            ),
        })
    return {
        "stock_code": str(row.get("stock_code") or "").zfill(6),
        "stock_name": row.get("short_name") or row.get("stock_name") or row.get("stock_code") or "",
        "strategy_key": strategy_key,
        "strategy_name": (_STRATEGY_BY_KEY.get(strategy_key) or {}).get("name", strategy_key),
        "market_state": state,
        "signal_direction": direction,
        "signal_status": status,
        "raw_score": score,
        "effective_score": round((score or 0.0) * float(weight["effective_weight"]), 2) if score is not None else None,
        "model_confidence": confidence,
        "today_signal": str(row.get("signal_reason") or row.get("recommend_reason") or row.get("reason") or "")[:500],
        "entry_low": row.get("entry_price_low"),
        "entry_high": row.get("entry_price_high"),
        "trigger_conditions": _json_value(row.get("entry_conditions_json"), []),
        "stop_loss": row.get("stop_loss_price") or row.get("trend_stop_price"),
        "take_profit_1": row.get("take_profit_1"),
        "take_profit_2": row.get("take_profit_2"),
        "no_chase_price": row.get("no_chase_price") or row.get("resistance_price"),
        "risk_level": risk_level,
        # Preserve the upstream fact. A strategy score may consume a
        # CONFIRM/BUY_READY signal, but must not manufacture one from WATCH.
        "source_signal_status": row_status,
        "source_recommend_status": str(
            row.get("recommend_status") or ""
        ).upper(),
        "source_chase_risk_status": str(
            row.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper(),
        "source_ordinary_buy_eligible": _is_explicit_database_true(
            row.get("ordinary_buy_eligible")
        ),
        "chase_risk_status": str(
            row.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper(),
        "ordinary_buy_eligible": _is_explicit_database_true(
            row.get("ordinary_buy_eligible")
        ),
        "market_only_downgrade": bool(
            gate_status == "REDUCE"
            and direction == "BUY"
            and state in {"high_range", "risk_declining"}
            and not bool(signal_basis.get("hard_block"))
            and not bool(signal_basis.get("strategy_hits"))
        ),
        "signal_basis": signal_basis,
        "risk_reward_ratio": row.get("risk_reward_ratio"),
        "gate_status": gate_status,
        "gate_reason": gate_reason,
        "effective_weight": weight["effective_weight"],
        "weight_detail": weight,
        "evidence_chain": evidence,
        "data_date": str(row.get("pick_date") or "")[:10],
        "data_quality_score": data_quality_score,
        "adapter_mode": "dated_reference_pool_adapter" if is_reference else "legacy_recommendation_adapter",
        "model_version": row.get("model_version") or "legacy-adapter",
        "external_market_adjustment": float(
            row.get("external_market_adjustment") or 0.0
        ),
        "external_market_score": row.get("external_market_score"),
        "external_market_status": (
            row.get("external_market_status") or "UNKNOWN"
        ),
        "external_market_data_quality": (
            row.get("external_market_data_quality") or "UNKNOWN"
        ),
        "external_market_snapshot_id": (
            row.get("external_market_snapshot_id") or ""
        ),
        "external_market_captured_at": (
            row.get("external_market_captured_at") or ""
        ),
        "reference_fixture": is_reference,
        "reference_priority": row.get("reference_priority"),
        "reference_source": row.get("reference_source"),
        "reference_as_of_date": row.get("reference_as_of_date"),
        "theme_code": (
            row.get("sector_industry_name")
            or row.get("industry_name")
            or row.get("industry")
            or ""
        ),
        "industry_name": (
            row.get("industry_name")
            or row.get("sector_industry_name")
            or row.get("industry")
            or ""
        ),
        "db_verified": bool(row.get("db_verified")),
        "db_close": row.get("db_close"),
        "db_verification_reason": row.get("db_verification_reason"),
        "position_cap_pct": row.get("position_cap_pct"),
        "pool_cap_pct": row.get("pool_cap_pct"),
        "global_invalidation_condition": row.get("global_invalidation_condition"),
    }


def aggregate_candidates(
    rows: Iterable[dict[str, Any]],
    market: dict[str, Any],
    configs: dict[str, dict[str, Any]] | None = None,
    metrics: dict[str, dict[str, Any]] | None = None,
    *,
    additional_signals: Iterable[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    configs = configs or {}
    metrics = metrics or {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for key in _legacy_keys(row):
            signal = adapt_recommendation_row(row, key, market, configs.get(key), metrics.get(key))
            if signal.get("stock_code"):
                grouped[signal["stock_code"]].append(signal)
    for raw_signal in additional_signals or ():
        if not isinstance(raw_signal, dict):
            continue
        signal = dict(raw_signal)
        code = str(signal.get("stock_code") or "").strip().zfill(6)
        if code and signal.get("strategy_key"):
            signal["stock_code"] = code
            grouped[code].append(signal)

    candidates: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for code, signals in grouped.items():
        decision = resolve_conflict(signals, str(market.get("market_state") or ""))
        best = max(signals, key=lambda item: (_num(item.get("effective_score"), 0.0) or 0.0, _num(item.get("model_confidence"), 0.0) or 0.0))
        industry_by_strategy = {
            str(item.get("strategy_key") or ""): str(
                item.get("industry_name") or item.get("theme_code") or ""
            ).strip()
            for item in signals
            if str(item.get("strategy_key") or "").strip()
            and str(
                item.get("industry_name") or item.get("theme_code") or ""
            ).strip()
        }
        industry_names = sorted(set(industry_by_strategy.values()))
        candidate = {
            "priority": best.get("reference_priority") or ("A" if decision["final_status"] in {"READY", "WATCH"} and decision["final_direction"] == "BUY" else "B"),
            "stock_code": code,
            "stock_name": best.get("stock_name") or code,
            "final_direction": decision["final_direction"],
            "final_status": decision["final_status"],
            "model_confidence": max((_num(item.get("model_confidence"), 0.0) or 0.0 for item in signals), default=0.0) or None,
            "today_signal": best.get("today_signal") or decision.get("conflict_summary"),
            "entry_low": best.get("entry_low"),
            "entry_high": best.get("entry_high"),
            "trigger_conditions": best.get("trigger_conditions") or [],
            "stop_loss": best.get("stop_loss"),
            "take_profit_1": best.get("take_profit_1"),
            "take_profit_2": best.get("take_profit_2"),
            "no_chase_price": best.get("no_chase_price"),
            "risk_level": max((str(item.get("risk_level") or "LOW") for item in signals), key=lambda value: {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}.get(value, 0)),
            "risk_reward_ratio": best.get("risk_reward_ratio"),
            "dominant_strategy": decision.get("dominant_strategy") or best.get("strategy_key"),
            "strategies": sorted({item.get("strategy_key") for item in signals if item.get("strategy_key")}),
            "buy_score": decision["buy_score"],
            "sell_score": decision["sell_score"],
            "hold_score": decision["hold_score"],
            "conflict": decision["conflict"],
            "conflict_summary": decision["conflict_summary"],
            "blocking_reasons": decision["blocking_reasons"],
            "strategy_signals": signals,
            "data_date": best.get("data_date"),
            "adapter_mode": best.get("adapter_mode") or "legacy_recommendation_adapter",
            "reference_fixture": bool(best.get("reference_fixture")),
            "reference_source": best.get("reference_source"),
            "reference_as_of_date": best.get("reference_as_of_date"),
            "theme_code": best.get("theme_code") or "",
            "industry_name": (
                best.get("industry_name")
                or best.get("theme_code")
                or ""
            ),
            "industry_names": industry_names,
            "industry_by_strategy": industry_by_strategy,
            "db_verified": all(bool(item.get("db_verified")) for item in signals if item.get("reference_fixture")) if any(item.get("reference_fixture") for item in signals) else None,
            "db_close": best.get("db_close"),
            "db_verification_reason": best.get("db_verification_reason"),
            "position_cap_pct": best.get("position_cap_pct"),
            "pool_cap_pct": best.get("pool_cap_pct"),
            "global_invalidation_condition": best.get("global_invalidation_condition"),
        }
        candidates.append(candidate)
        if decision["conflict"] or decision["final_status"] in {"BLOCKED", "CONFLICT"}:
            conflicts.append({
                "stock_code": code,
                "stock_name": candidate["stock_name"],
                "market_state": market.get("market_state"),
                "decision": decision,
                "signals": signals,
            })
    candidates.sort(key=lambda item: ({"READY": 0, "WATCH": 1, "CONFLICT": 2, "SELL_ALERT": 3, "BLOCKED": 4, "INSUFFICIENT_DATA": 5}.get(item.get("final_status"), 9), -(item.get("model_confidence") or 0), item.get("stock_code", "")))
    return candidates, conflicts


def _db_read(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    engine = get_engine()
    return read_sql_rows(engine, sql, params, context="strategy_center")


def _db_write(sql: str, params: dict[str, Any] | None = None) -> None:
    bound_connection = current_bound_sql_connection()
    if bound_connection is not None:
        bound_connection.execute(text(sql), params or {})
        return
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(text(sql), params or {})


def _table_exists(table_name: str) -> bool:
    try:
        rows = _db_read("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = :table_name
        """, {"table_name": table_name})
        return bool(rows and int(rows[0].get("cnt") or 0))
    except Exception:
        return False


def _kline_table_exists(table_name: str) -> bool:
    try:
        rows = read_sql_rows(
            get_kline_engine(),
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = :table_name
            """,
            {"table_name": table_name},
            context="strategy_center_kline_table",
        )
        return bool(rows and int(rows[0].get("cnt") or 0))
    except Exception:
        return False


def privileged_migrate_strategy_center_tables(engine=None) -> None:
    """Create the six strategy-center tables during a privileged deploy step."""

    engine = engine or get_engine()
    ddl = (
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_config (
            strategy_key VARCHAR(40) PRIMARY KEY,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            base_weight DECIMAL(8,4) NOT NULL DEFAULT 1.0,
            config_json LONGTEXT,
            version INT NOT NULL DEFAULT 1,
            updated_by VARCHAR(80) NOT NULL DEFAULT 'system',
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_run (
            run_uid VARCHAR(40) PRIMARY KEY,
            trade_date DATE NOT NULL,
            market_state VARCHAR(40) NOT NULL DEFAULT 'unknown',
            state_confidence DECIMAL(6,2) DEFAULT NULL,
            source_status VARCHAR(20) NOT NULL DEFAULT 'degraded',
            model_version VARCHAR(40) DEFAULT '',
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            signal_count INT NOT NULL DEFAULT 0,
            candidate_count INT NOT NULL DEFAULT 0,
            conflict_count INT NOT NULL DEFAULT 0,
            error_message VARCHAR(500) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at DATETIME DEFAULT NULL,
            KEY idx_strategy_center_run_date (trade_date, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_signal (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_uid VARCHAR(40) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(10) NOT NULL,
            stock_name VARCHAR(80) DEFAULT '',
            strategy_key VARCHAR(40) NOT NULL,
            market_state VARCHAR(40) DEFAULT '',
            signal_direction VARCHAR(20) NOT NULL DEFAULT 'NO_SIGNAL',
            signal_status VARCHAR(30) NOT NULL DEFAULT 'INSUFFICIENT_DATA',
            raw_score DECIMAL(8,2) DEFAULT NULL,
            effective_score DECIMAL(8,2) DEFAULT NULL,
            model_confidence DECIMAL(8,2) DEFAULT NULL,
            effective_weight DECIMAL(8,4) DEFAULT NULL,
            risk_level VARCHAR(20) DEFAULT 'LOW',
            gate_status VARCHAR(20) DEFAULT 'PASS',
            gate_reason VARCHAR(500) DEFAULT '',
            entry_low DECIMAL(12,4) DEFAULT NULL,
            entry_high DECIMAL(12,4) DEFAULT NULL,
            stop_loss DECIMAL(12,4) DEFAULT NULL,
            take_profit_1 DECIMAL(12,4) DEFAULT NULL,
            take_profit_2 DECIMAL(12,4) DEFAULT NULL,
            no_chase_price DECIMAL(12,4) DEFAULT NULL,
            risk_reward_ratio DECIMAL(8,2) DEFAULT NULL,
            today_signal VARCHAR(500) DEFAULT '',
            trigger_conditions_json LONGTEXT,
            evidence_chain_json LONGTEXT,
            data_snapshot_json LONGTEXT,
            model_version VARCHAR(40) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_signal_date (trade_date, strategy_key),
            KEY idx_strategy_center_signal_stock (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_conflict (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_uid VARCHAR(40) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(10) NOT NULL,
            stock_name VARCHAR(80) DEFAULT '',
            market_state VARCHAR(40) DEFAULT '',
            final_direction VARCHAR(20) DEFAULT 'NO_SIGNAL',
            final_status VARCHAR(30) DEFAULT 'INSUFFICIENT_DATA',
            buy_score DECIMAL(10,2) DEFAULT 0,
            sell_score DECIMAL(10,2) DEFAULT 0,
            hold_score DECIMAL(10,2) DEFAULT 0,
            decision_json LONGTEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_conflict_date (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_metric (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            strategy_key VARCHAR(40) NOT NULL,
            sample_count INT NOT NULL DEFAULT 0,
            today_signal_count INT NOT NULL DEFAULT 0,
            return_pct DECIMAL(10,4) DEFAULT NULL,
            max_drawdown_pct DECIMAL(10,4) DEFAULT NULL,
            win_rate_pct DECIMAL(10,4) DEFAULT NULL,
            profit_factor DECIMAL(10,4) DEFAULT NULL,
            avg_return_pct DECIMAL(10,4) DEFAULT NULL,
            source VARCHAR(40) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_center_metric (as_of_date, strategy_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_key VARCHAR(40) NOT NULL,
            action VARCHAR(40) NOT NULL,
            old_value VARCHAR(500) DEFAULT '',
            new_value VARCHAR(500) DEFAULT '',
            reason VARCHAR(500) DEFAULT '',
            operator VARCHAR(80) DEFAULT 'api',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_audit (strategy_key, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    )
    with engine.begin() as connection:
        for statement in ddl:
            connection.execute(text(statement))


_STRATEGY_CENTER_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "st_strategy_center_config": (
        "strategy_key", "enabled", "base_weight", "config_json", "version",
        "updated_by", "updated_at",
    ),
    "st_strategy_center_run": (
        "run_uid", "trade_date", "market_state", "state_confidence",
        "source_status", "model_version", "status", "signal_count",
        "candidate_count", "conflict_count", "error_message", "created_at",
        "finished_at",
    ),
    "st_strategy_center_signal": (
        "id", "run_uid", "trade_date", "stock_code", "stock_name",
        "strategy_key", "market_state", "signal_direction", "signal_status",
        "raw_score", "effective_score", "model_confidence",
        "effective_weight", "risk_level", "gate_status", "gate_reason",
        "entry_low", "entry_high", "stop_loss", "take_profit_1",
        "take_profit_2", "no_chase_price", "risk_reward_ratio",
        "today_signal", "trigger_conditions_json", "evidence_chain_json",
        "data_snapshot_json", "model_version", "created_at",
    ),
    "st_strategy_center_conflict": (
        "id", "run_uid", "trade_date", "stock_code", "stock_name",
        "market_state", "final_direction", "final_status", "buy_score",
        "sell_score", "hold_score", "decision_json", "created_at",
    ),
    "st_strategy_center_metric": (
        "id", "as_of_date", "strategy_key", "sample_count",
        "today_signal_count", "return_pct", "max_drawdown_pct",
        "win_rate_pct", "profit_factor", "avg_return_pct", "source",
        "created_at",
    ),
    "st_strategy_center_audit": (
        "id", "strategy_key", "action", "old_value", "new_value", "reason",
        "operator", "created_at",
    ),
}

_STRATEGY_CENTER_COLUMN_CONTRACTS: dict[
    str, dict[str, tuple[str, bool, int | None, int | None, int | None, bool]]
] = {
    "st_strategy_center_config": {
        "strategy_key": ("varchar", False, 40, None, None, False),
        "enabled": ("tinyint", False, None, None, None, False),
        "base_weight": ("decimal", False, None, 8, 4, False),
        "config_json": ("longtext", True, None, None, None, False),
        "version": ("int", False, None, None, None, False),
        "updated_by": ("varchar", False, 80, None, None, False),
        "updated_at": ("datetime", False, None, None, None, False),
    },
    "st_strategy_center_run": {
        "run_uid": ("varchar", False, 40, None, None, False),
        "trade_date": ("date", False, None, None, None, False),
        "market_state": ("varchar", False, 40, None, None, False),
        "state_confidence": ("decimal", True, None, 6, 2, False),
        "source_status": ("varchar", False, 20, None, None, False),
        "model_version": ("varchar", True, 40, None, None, False),
        "status": ("varchar", False, 20, None, None, False),
        "signal_count": ("int", False, None, None, None, False),
        "candidate_count": ("int", False, None, None, None, False),
        "conflict_count": ("int", False, None, None, None, False),
        "error_message": ("varchar", True, 500, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
        "finished_at": ("datetime", True, None, None, None, False),
    },
    "st_strategy_center_signal": {
        "id": ("bigint", False, None, None, None, True),
        "run_uid": ("varchar", False, 40, None, None, False),
        "trade_date": ("date", False, None, None, None, False),
        "stock_code": ("varchar", False, 10, None, None, False),
        "stock_name": ("varchar", True, 80, None, None, False),
        "strategy_key": ("varchar", False, 40, None, None, False),
        "market_state": ("varchar", True, 40, None, None, False),
        "signal_direction": ("varchar", False, 20, None, None, False),
        "signal_status": ("varchar", False, 30, None, None, False),
        "raw_score": ("decimal", True, None, 8, 2, False),
        "effective_score": ("decimal", True, None, 8, 2, False),
        "model_confidence": ("decimal", True, None, 8, 2, False),
        "effective_weight": ("decimal", True, None, 8, 4, False),
        "risk_level": ("varchar", True, 20, None, None, False),
        "gate_status": ("varchar", True, 20, None, None, False),
        "gate_reason": ("varchar", True, 500, None, None, False),
        "entry_low": ("decimal", True, None, 12, 4, False),
        "entry_high": ("decimal", True, None, 12, 4, False),
        "stop_loss": ("decimal", True, None, 12, 4, False),
        "take_profit_1": ("decimal", True, None, 12, 4, False),
        "take_profit_2": ("decimal", True, None, 12, 4, False),
        "no_chase_price": ("decimal", True, None, 12, 4, False),
        "risk_reward_ratio": ("decimal", True, None, 8, 2, False),
        "today_signal": ("varchar", True, 500, None, None, False),
        "trigger_conditions_json": ("longtext", True, None, None, None, False),
        "evidence_chain_json": ("longtext", True, None, None, None, False),
        "data_snapshot_json": ("longtext", True, None, None, None, False),
        "model_version": ("varchar", True, 40, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
    },
    "st_strategy_center_conflict": {
        "id": ("bigint", False, None, None, None, True),
        "run_uid": ("varchar", False, 40, None, None, False),
        "trade_date": ("date", False, None, None, None, False),
        "stock_code": ("varchar", False, 10, None, None, False),
        "stock_name": ("varchar", True, 80, None, None, False),
        "market_state": ("varchar", True, 40, None, None, False),
        "final_direction": ("varchar", True, 20, None, None, False),
        "final_status": ("varchar", True, 30, None, None, False),
        "buy_score": ("decimal", True, None, 10, 2, False),
        "sell_score": ("decimal", True, None, 10, 2, False),
        "hold_score": ("decimal", True, None, 10, 2, False),
        "decision_json": ("longtext", True, None, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
    },
    "st_strategy_center_metric": {
        "id": ("bigint", False, None, None, None, True),
        "as_of_date": ("date", False, None, None, None, False),
        "strategy_key": ("varchar", False, 40, None, None, False),
        "sample_count": ("int", False, None, None, None, False),
        "today_signal_count": ("int", False, None, None, None, False),
        "return_pct": ("decimal", True, None, 10, 4, False),
        "max_drawdown_pct": ("decimal", True, None, 10, 4, False),
        "win_rate_pct": ("decimal", True, None, 10, 4, False),
        "profit_factor": ("decimal", True, None, 10, 4, False),
        "avg_return_pct": ("decimal", True, None, 10, 4, False),
        "source": ("varchar", True, 40, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
    },
    "st_strategy_center_audit": {
        "id": ("bigint", False, None, None, None, True),
        "strategy_key": ("varchar", False, 40, None, None, False),
        "action": ("varchar", False, 40, None, None, False),
        "old_value": ("varchar", True, 500, None, None, False),
        "new_value": ("varchar", True, 500, None, None, False),
        "reason": ("varchar", True, 500, None, None, False),
        "operator": ("varchar", True, 80, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
    },
}

_STRATEGY_CENTER_REQUIRED_INDEXES: dict[
    str, tuple[tuple[bool, tuple[str, ...]], ...]
] = {
    "st_strategy_center_config": ((True, ("strategy_key",)),),
    "st_strategy_center_run": (
        (True, ("run_uid",)),
        (False, ("trade_date", "created_at")),
    ),
    "st_strategy_center_signal": (
        (True, ("id",)),
        (False, ("trade_date", "strategy_key")),
        (False, ("trade_date", "stock_code")),
    ),
    "st_strategy_center_conflict": (
        (True, ("id",)),
        (False, ("trade_date", "stock_code")),
    ),
    "st_strategy_center_metric": (
        (True, ("id",)),
        (True, ("as_of_date", "strategy_key")),
    ),
    "st_strategy_center_audit": (
        (True, ("id",)),
        (False, ("strategy_key", "created_at")),
    ),
}


def _strategy_center_schema_rows(connection, *, kind: str) -> list[dict[str, Any]]:
    tables = tuple(_STRATEGY_CENTER_TABLE_COLUMNS)
    placeholders = ", ".join(
        f":table_{index}" for index in range(len(tables))
    )
    if kind == "columns":
        sql = (
            "SELECT table_name AS table_name, column_name AS column_name, "
            "data_type AS data_type, is_nullable AS is_nullable, "
            "character_maximum_length AS character_maximum_length, "
            "numeric_precision AS numeric_precision, "
            "numeric_scale AS numeric_scale, extra AS extra "
            "FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name IN "
            f"({placeholders})"
        )
    elif kind == "indexes":
        sql = (
            "SELECT table_name AS table_name, index_name AS index_name, "
            "non_unique AS non_unique, seq_in_index AS seq_in_index, "
            "column_name AS column_name FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name IN "
            f"({placeholders}) ORDER BY table_name, index_name, seq_in_index"
        )
    else:  # pragma: no cover - internal programming error
        raise ValueError(f"unsupported strategy-center schema kind: {kind}")
    return [
        dict(row) for row in connection.execute(
            text(sql),
            {f"table_{index}": table for index, table in enumerate(tables)},
        ).mappings().all()
    ]


def _validate_strategy_center_schema(connection) -> None:
    columns_by_table: dict[str, dict[str, dict[str, Any]]] = {
        table: {} for table in _STRATEGY_CENTER_TABLE_COLUMNS
    }
    for row in _strategy_center_schema_rows(connection, kind="columns"):
        table = str(row.get("table_name") or "")
        if table in columns_by_table:
            columns_by_table[table][str(row.get("column_name") or "")] = row
    for table, required in _STRATEGY_CENTER_TABLE_COLUMNS.items():
        missing = sorted(set(required) - set(columns_by_table[table]))
        if missing:
            raise RuntimeError(
                f"strategy-center runtime schema is not prepared: "
                f"{table} missing columns {missing}"
            )
    for table, contracts in _STRATEGY_CENTER_COLUMN_CONTRACTS.items():
        for column, expected in contracts.items():
            row = columns_by_table[table][column]
            actual = (
                str(row.get("data_type") or "").lower(),
                str(row.get("is_nullable") or "").upper() == "YES",
                (
                    int(row["character_maximum_length"])
                    if row.get("character_maximum_length") is not None else None
                ),
                (
                    int(row["numeric_precision"])
                    if row.get("numeric_precision") is not None else None
                ),
                (
                    int(row["numeric_scale"])
                    if row.get("numeric_scale") is not None else None
                ),
                "auto_increment" in str(row.get("extra") or "").lower(),
            )
            comparable_actual = (
                actual[0], actual[1],
                actual[2] if expected[2] is not None else None,
                actual[3] if expected[3] is not None else None,
                actual[4] if expected[4] is not None else None,
                actual[5],
            )
            if comparable_actual != expected:
                raise RuntimeError(
                    f"strategy-center runtime schema type drift: "
                    f"{table}.{column} expected={expected} "
                    f"actual={comparable_actual}"
                )

    index_parts: dict[str, dict[str, list[tuple[int, str]]]] = {
        table: {} for table in _STRATEGY_CENTER_TABLE_COLUMNS
    }
    index_unique: dict[str, dict[str, bool]] = {
        table: {} for table in _STRATEGY_CENTER_TABLE_COLUMNS
    }
    for row in _strategy_center_schema_rows(connection, kind="indexes"):
        table = str(row.get("table_name") or "")
        if table not in index_parts:
            continue
        name = str(row.get("index_name") or "")
        index_parts[table].setdefault(name, []).append((
            int(row.get("seq_in_index") or 0),
            str(row.get("column_name") or ""),
        ))
        index_unique[table][name] = int(row.get("non_unique") or 0) == 0
    for table, required_indexes in _STRATEGY_CENTER_REQUIRED_INDEXES.items():
        actual = {
            (
                bool(index_unique[table].get(name)),
                tuple(column for _, column in sorted(parts)),
            )
            for name, parts in index_parts[table].items()
        }
        missing = [spec for spec in required_indexes if spec not in actual]
        if missing:
            raise RuntimeError(
                f"strategy-center runtime schema is not prepared: "
                f"{table} missing indexes {missing}"
            )


def _expected_strategy_center_configs(
    registration: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    manifest = load_stock_manifest()
    manifest_items = {
        str(item["key"]): item for item in manifest["strategies"]
    }
    return {
        str(item["key"]): {
            "base_weight": float(item["base_weight"]),
            "config": {
                **manifest_items[item["key"]],
                "manifest_version": registration["stock_manifest_version"],
                "config_hash": registration["stock_manifest_hash"],
            },
        }
        for item in STRATEGY_CATALOG
    }


def privileged_seed_strategy_center_configs(engine=None) -> dict[str, Any]:
    """Seed current strategy-center configs after all nine tables are migrated."""

    engine = engine or get_engine()
    with engine.connect() as connection:
        _validate_strategy_center_schema(connection)
        registration = validate_versioned_strategy_runtime(
            engine, connection=connection,
        )
    expected = _expected_strategy_center_configs(registration)
    with engine.begin() as connection:
        for strategy_key, identity in expected.items():
            connection.execute(text("""
                INSERT INTO st_strategy_center_config
                (strategy_key, enabled, base_weight, config_json, version, updated_by)
                VALUES
                (:strategy_key, 1, :base_weight, :config_json, 2, 'manifest_registry')
                ON DUPLICATE KEY UPDATE
                    base_weight = VALUES(base_weight),
                    config_json = VALUES(config_json),
                    version = GREATEST(version, VALUES(version)),
                    updated_by = VALUES(updated_by)
            """), {
                "strategy_key": strategy_key,
                "base_weight": identity["base_weight"],
                "config_json": json.dumps(
                    identity["config"], ensure_ascii=False,
                ),
            })
        active_keys = sorted(_STRATEGY_BY_KEY)
        placeholders = ", ".join(
            f":active_{index}" for index in range(len(active_keys))
        )
        connection.execute(
            text(f"""
                UPDATE st_strategy_center_config
                SET enabled = 0, updated_by = 'legacy_merge_v2'
                WHERE strategy_key NOT IN ({placeholders})
            """),
            {f"active_{index}": key for index, key in enumerate(active_keys)},
        )
    validate_strategy_center_runtime(engine)
    return registration


def _validate_strategy_center_config_seed(
    connection, registration: dict[str, Any],
) -> None:
    expected = _expected_strategy_center_configs(registration)
    keys = tuple(sorted(expected))
    placeholders = ", ".join(f":key_{index}" for index in range(len(keys)))
    rows = [
        dict(row) for row in connection.execute(
            text(
                "SELECT strategy_key, enabled, base_weight, config_json, version "
                "FROM st_strategy_center_config WHERE strategy_key IN "
                f"({placeholders})"
            ),
            {f"key_{index}": key for index, key in enumerate(keys)},
        ).mappings().all()
    ]
    by_key = {str(row.get("strategy_key") or ""): row for row in rows}
    if len(by_key) != len(rows) or set(by_key) != set(expected):
        raise RuntimeError(
            "strategy-center config seed identity drift: current manifest rows missing"
        )
    for strategy_key, identity in expected.items():
        row = by_key[strategy_key]
        try:
            config = json.loads(str(row.get("config_json") or ""))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"strategy-center config seed identity drift: "
                f"{strategy_key} config_json invalid"
            ) from exc
        try:
            enabled_valid = int(row.get("enabled")) in {0, 1}
            version_valid = int(row.get("version") or 0) >= 2
            weight_valid = abs(
                float(row.get("base_weight")) - float(identity["base_weight"])
            ) <= 0.000001
        except (TypeError, ValueError):
            enabled_valid = version_valid = weight_valid = False
        if not (
            enabled_valid
            and version_valid
            and weight_valid
            and config == identity["config"]
        ):
            raise RuntimeError(
                f"strategy-center config seed identity drift: {strategy_key}"
            )


def validate_strategy_center_runtime(
    engine=None, *, connection=None,
) -> dict[str, Any]:
    """Read-only production guard for all nine tables and current seed identity."""

    engine = engine or get_engine()

    def validate(bound_connection) -> dict[str, Any]:
        _validate_strategy_center_schema(bound_connection)
        registration = validate_versioned_strategy_runtime(
            engine, connection=bound_connection,
        )
        _validate_strategy_center_config_seed(bound_connection, registration)
        return registration

    if connection is not None:
        return validate(connection)
    with engine.connect() as bound_connection:
        return validate(bound_connection)


def ensure_strategy_center_tables() -> None:
    """Compatibility guard: validate only; never create or seed at runtime."""

    validate_strategy_center_runtime(
        get_engine(), connection=current_bound_sql_connection(),
    )


def load_strategy_configs() -> dict[str, dict[str, Any]]:
    result = {
        item["key"]: {"enabled": True, "base_weight": item["base_weight"], "version": 1, "updated_by": "default"}
        for item in STRATEGY_CATALOG
    }
    if not _table_exists("st_strategy_center_config"):
        return result
    try:
        rows = _db_read("SELECT strategy_key, enabled, base_weight, config_json, version, updated_by, updated_at FROM st_strategy_center_config")
        for row in rows:
            key = str(row.get("strategy_key") or "")
            if key in result:
                result[key].update({
                    "enabled": bool(int(row.get("enabled") or 0)),
                    "base_weight": _num(row.get("base_weight"), result[key]["base_weight"]),
                    "version": int(row.get("version") or 1),
                    "updated_by": row.get("updated_by") or "system",
                    "updated_at": str(row.get("updated_at") or ""),
                    "config": _json_value(row.get("config_json"), {}),
                })
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "configuration", exc)
    return result


def load_strategy_metrics(as_of_date: str) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    if _table_exists("st_strategy_center_metric"):
        try:
            rows = _db_read("""
                SELECT m.*
                FROM st_strategy_center_metric m
                INNER JOIN (
                    SELECT strategy_key, MAX(as_of_date) AS latest_date
                    FROM st_strategy_center_metric
                    WHERE as_of_date <= :as_of_date
                    GROUP BY strategy_key
                ) latest ON latest.strategy_key = m.strategy_key AND latest.latest_date = m.as_of_date
            """, {"as_of_date": as_of_date})
            for row in rows:
                metrics[str(row.get("strategy_key") or "")] = row
        except Exception as exc:
            _safe_fallback_log(logging.DEBUG, "metric_snapshot", exc)

    # Backfill visible metrics from the existing simulated-trade ledger until
    # the strategy-center metric job has produced its first snapshot. The
    # drawdown is calculated on the realized-return curve, not from a single
    # losing trade.
    if _table_exists("st_sim_position"):
        try:
            rows = _db_read("""
                SELECT strategy_type, sell_date, id, profit, profit_rate
                FROM st_sim_position
                WHERE status = 'sold' AND sell_date IS NOT NULL AND sell_date <= :as_of_date
                ORDER BY strategy_type, sell_date, id
                LIMIT 10000
            """, {"as_of_date": as_of_date})
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                raw_key = str(row.get("strategy_type") or "")
                key = LEGACY_STRATEGY_MAP.get(raw_key, raw_key)
                if key in _STRATEGY_BY_KEY:
                    grouped[key].append(row)
            for key, items in grouped.items():
                if key in metrics or not items:
                    continue
                returns = [_num(item.get("profit_rate"), 0.0) or 0.0 for item in items]
                gross_profit = sum(value for value in returns if value > 0)
                gross_loss = abs(sum(value for value in returns if value < 0))
                cumulative = 0.0
                peak = 0.0
                max_drawdown = 0.0
                for value in returns:
                    cumulative += value
                    peak = max(peak, cumulative)
                    max_drawdown = min(max_drawdown, cumulative - peak)
                metrics[key] = {
                    "strategy_key": key,
                    "as_of_date": as_of_date,
                    "sample_count": len(items),
                    "return_pct": round(cumulative, 4),
                    "max_drawdown_pct": round(max_drawdown, 4),
                    "win_rate_pct": round(sum(value > 0 for value in returns) / len(returns) * 100, 4),
                    "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else None,
                    "avg_return_pct": round(sum(returns) / len(returns), 4),
                    "source": "st_sim_position",
                }
        except Exception as exc:
            _safe_fallback_log(logging.DEBUG, "simulated_metric", exc)

    # The recommendation ledger contains historical forward-review labels.
    # Use them only for the four registered strategies; disabled legacy labels
    # must not silently reappear as synthetic strategy cards.
    if _table_exists("st_recommended_stocks"):
        try:
            available = _table_columns("st_recommended_stocks")
            review_field = next((field for field in ("review_5d_pct", "review_3d_pct", "review_1d_pct") if field in available), "")
            if {"suitable_strategies", "pick_date", review_field}.issubset(available):
                rows = _db_read(
                    f"SELECT suitable_strategies, primary_strategy, strategy_profile, `{review_field}` AS forward_return "
                    "FROM st_recommended_stocks "
                    f"WHERE pick_date <= :as_of_date AND `{review_field}` IS NOT NULL LIMIT 20000",
                    {"as_of_date": as_of_date},
                )
                grouped: dict[str, list[float]] = defaultdict(list)
                for row in rows:
                    raw = _json_value(row.get("suitable_strategies"), [])
                    if not isinstance(raw, list):
                        raw = [part.strip() for part in str(raw or "").split(",") if part.strip()]
                    raw.extend([row.get("primary_strategy"), row.get("strategy_profile")])
                    keys = {LEGACY_STRATEGY_MAP.get(str(value or "").strip(), str(value or "").strip()) for value in raw}
                    value = _num(row.get("forward_return"), None)
                    if value is None:
                        continue
                    for key in keys:
                        if key in _STRATEGY_BY_KEY:
                            grouped[key].append(value)
                for key, returns in grouped.items():
                    if key in metrics or not returns:
                        continue
                    wins = [value for value in returns if value > 0]
                    losses = [value for value in returns if value < 0]
                    cumulative = sum(returns)
                    peak = 0.0
                    curve = 0.0
                    drawdown = 0.0
                    for value in returns:
                        curve += value
                        peak = max(peak, curve)
                        drawdown = min(drawdown, curve - peak)
                    metrics[key] = {
                        "strategy_key": key,
                        "as_of_date": as_of_date,
                        "sample_count": len(returns),
                        "return_pct": round(cumulative / len(returns), 4),
                        "max_drawdown_pct": round(min(returns), 4),
                        "win_rate_pct": round(len(wins) / len(returns) * 100, 4),
                        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else None,
                        "avg_return_pct": round(cumulative / len(returns), 4),
                        "source": f"st_recommended_stocks_{review_field}",
                        "model_status": "historical_review",
                        "metric_note": f"{review_field} 横截面复盘基线，不等同于独立模型回测",
                    }
        except Exception as exc:
            _safe_fallback_log(
                logging.DEBUG, "recommendation_review_metric", exc,
            )
    return metrics


def load_reference_candidate_pool(trade_date: str) -> dict[str, Any] | None:
    """Load an exact-date research pool without changing production signals.

    The pool is a dated research artifact, not a permanent stock-code rule. It
    is only eligible for the exact ``trade_date`` encoded in its filename and
    remains explicitly marked as reference data in every downstream signal.
    """
    target = normalize_trade_date(trade_date)
    if not target:
        return None
    path = _REFERENCE_POOL_DIR / f"a_share_pool_{target}.json"
    try:
        if not path.is_file():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or normalize_trade_date(payload.get("trade_date")) != target:
            return None
        candidates = payload.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            return None
        payload["_path"] = str(path.relative_to(_PROJECT_ROOT))
        return payload
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        _safe_fallback_log(logging.WARNING, "reference_pool_file", exc)
        return None


def _table_columns(table_name: str) -> set[str]:
    try:
        return {
            str(row.get("COLUMN_NAME") or "")
            for row in _db_read("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            """, {"table_name": table_name})
            if row.get("COLUMN_NAME")
        }
    except Exception:
        return set()


def _reference_db_crosscheck(candidates: list[dict[str, Any]], as_of_date: str) -> dict[str, dict[str, Any]]:
    """Cross-check dated reference prices against the operational market tables."""
    codes = [str(item.get("stock_code") or "").zfill(6) for item in candidates if item.get("stock_code")]
    if not codes or not as_of_date:
        return {}
    params = {f"code_{index}": code for index, code in enumerate(codes)}
    placeholders = ", ".join(f":code_{index}" for index in range(len(codes)))
    result: dict[str, dict[str, Any]] = {}
    for table_name in ("sm_stock_snapshot", "sm_stock_kline"):
        columns = _table_columns(table_name)
        if not {"stock_code", "trade_date"}.issubset(columns):
            continue
        selected = ["stock_code", "trade_date"]
        for column in ("close", "price", "change_pct", "amount", "k_type"):
            if column in columns:
                selected.append(column)
        where = f"trade_date = :as_of_date AND stock_code IN ({placeholders})"
        if table_name == "sm_stock_kline" and "k_type" in columns:
            where += " AND (k_type = 1 OR k_type IS NULL)"
        params_with_date = {**params, "as_of_date": as_of_date}
        try:
            rows = _db_read(
                f"SELECT {', '.join(f'`{column}`' for column in selected)} FROM {table_name} WHERE {where}",
                params_with_date,
            )
        except Exception as exc:
            _safe_fallback_log(
                logging.DEBUG, "reference_pool_database_cross_check", exc,
            )
            continue
        for row in rows:
            code = str(row.get("stock_code") or "").zfill(6)
            if code in result:
                continue
            result[code] = {
                "db_verified": True,
                "db_table": table_name,
                "db_trade_date": str(row.get("trade_date") or "")[:10],
                "db_close": row.get("close") if row.get("close") is not None else row.get("price"),
                "db_price": row.get("price") if row.get("price") is not None else row.get("close"),
                "db_change_pct": row.get("change_pct"),
                "db_amount": row.get("amount"),
                "price": row.get("price") if row.get("price") is not None else row.get("close"),
                "change_pct": row.get("change_pct"),
                "db_verification_reason": f"已从生产库 {table_name} 交叉验证 {as_of_date} 收盘记录",
            }
    for code in codes:
        result.setdefault(code, {
            "db_verified": False,
            "db_verification_reason": f"生产库中未找到 {as_of_date} 的收盘记录",
        })
    return result


def _reference_candidate_rows(pool: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    as_of_date = normalize_trade_date(pool.get("reference_as_of_date") or pool.get("data_date"))
    candidates = [item for item in pool.get("candidates", []) if isinstance(item, dict)]
    crosscheck = _reference_db_crosscheck(candidates, as_of_date)
    position_limits = pool.get("position_limits") if isinstance(pool.get("position_limits"), dict) else {}
    global_gate = pool.get("global_gate") if isinstance(pool.get("global_gate"), dict) else {}
    rows: list[dict[str, Any]] = []
    # Governance completion is only valid for the complete dated fixture.
    _ = limit
    for item in candidates:
        code = str(item.get("stock_code") or "").zfill(6)
        strategy_keys = [
            str(value).strip() for value in (item.get("strategy_keys") or [])
            if str(value).strip() in _STRATEGY_BY_KEY
        ]
        verified = crosscheck.get(code) or {}
        selection_reason = str(item.get("selection_reason") or item.get("strategy_label") or "")[:500]
        rows.append({
            "stock_code": code,
            "short_name": item.get("stock_name") or code,
            "pick_date": as_of_date,
            "suitable_strategies": json.dumps(strategy_keys, ensure_ascii=False),
            "primary_strategy": strategy_keys[0] if strategy_keys else "",
            "strategy_profile": strategy_keys[0] if strategy_keys else "",
            "signal_status": "WATCH",
            "recommend_status": "WATCH",
            "signal_reason": selection_reason,
            "recommend_reason": selection_reason,
            "reason": selection_reason,
            "entry_price_low": item.get("observation_low"),
            "entry_price_high": item.get("observation_high"),
            "stop_loss_price": item.get("stop_loss"),
            "take_profit_1": item.get("take_profit_1"),
            "take_profit_2": item.get("take_profit_2"),
            "resistance_price": item.get("no_chase_price"),
            "no_chase_price": item.get("no_chase_price"),
            "entry_conditions_json": json.dumps(item.get("trigger_conditions") or [], ensure_ascii=False),
            "event_risk_level": str(item.get("risk_level") or "HIGH").upper(),
            "model_version": "reference-pool-v1",
            "data_quality_score": 100 if verified.get("db_verified") else 75,
            "reference_fixture": True,
            "reference_priority": item.get("priority") or "B",
            "reference_source": pool.get("source") or "dated_reference_pool",
            "reference_trade_date": pool.get("trade_date"),
            "reference_as_of_date": as_of_date,
            "reference_strategy_label": item.get("strategy_label") or "",
            "reference_signal_direction": item.get("reference_signal_direction") or "HOLD",
            "position_cap_pct": position_limits.get("single_pct"),
            "pool_cap_pct": position_limits.get("aggregate_pct"),
            "global_invalidation_condition": global_gate.get("invalidation_condition") or "",
            **verified,
        })
    return rows


def latest_recommendation_date(requested: str = "") -> str:
    requested = normalize_trade_date(requested)
    if requested:
        return requested
    try:
        rows = _db_read("SELECT MAX(pick_date) AS trade_date FROM st_recommended_stocks")
        return normalize_trade_date(str(rows[0].get("trade_date") or "")[:10]) if rows else ""
    except Exception:
        return ""


def load_recommendation_rows(trade_date: str, limit: int = 200) -> list[dict[str, Any]]:
    target = normalize_trade_date(trade_date) or latest_recommendation_date()
    if target:
        reference_pool = load_reference_candidate_pool(target)
        if reference_pool:
            return _reference_candidate_rows(reference_pool, limit)
    if not _table_exists("st_recommended_stocks"):
        return []
    columns = {
        "stock_code", "short_name", "pick_date", "ai_score", "final_trade_score", "long_term_score", "short_term_score",
        "ultra_short_score", "swing_score", "main_wave_score", "trend_hold_score", "main_wave_signal", "main_wave_reason",
        "quality_score", "entry_score", "valuation", "fundamental", "technical", "sector_rotation_score", "sector_width_pct",
        "heat_overload_score", "confidence_score", "event_score", "event_risk_level", "recommend_status", "recommend_reason",
        "chase_risk_status", "ordinary_buy_eligible", "publication_status",
        "signal_status", "signal_reason", "primary_strategy", "strategy_profile", "suitable_strategies", "entry_price_low",
        "entry_price_high", "stop_loss_price", "trend_stop_price", "take_profit_1", "take_profit_2", "resistance_price",
        "risk_reward_ratio", "entry_conditions_json", "evidence_chain_json", "event_risk_detail", "data_quality_score", "data_quality_flags", "model_version",
        "price", "change_pct", "max_holding_days", "suggested_position", "no_chase_price",
        "industry_name", "publisher_run_uid",
    }
    try:
        available = {row["COLUMN_NAME"] for row in _db_read("""
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'st_recommended_stocks'
        """)}
        selected = [column for column in columns if column in available]
        if "stock_code" not in selected or "pick_date" not in selected:
            return []
        if not target:
            return []
        # Retain the argument for API compatibility; authoritative inputs must
        # be read in full. Presentation limits belong after governance.
        _ = max(1, int(limit))
        select_sql = ", ".join(f"`{column}`" for column in sorted(selected))
        order_column = "final_trade_score" if "final_trade_score" in selected else ("ai_score" if "ai_score" in selected else "stock_code")
        rows = _db_read(
            f"SELECT {select_sql} FROM st_recommended_stocks "
            f"WHERE pick_date = :trade_date "
            f"ORDER BY `{order_column}` DESC, `stock_code` ASC",
            {"trade_date": target},
        )
        if rows and not all(
            str(row.get("industry_name") or "").strip() for row in rows
        ):
            try:
                industry_rows = _db_read(
                    """
                    SELECT stock_code, industry_name
                    FROM qmt_industry_member_snapshot
                    WHERE snapshot_date = :trade_date
                      AND industry_name IS NOT NULL
                      AND industry_name <> ''
                    ORDER BY stock_code, industry_code
                    """,
                    {"trade_date": target},
                )
                industry_by_code: dict[str, str] = {}
                for item in industry_rows:
                    code = str(item.get("stock_code") or "").zfill(6)
                    name = str(item.get("industry_name") or "").strip()
                    if code and name and code not in industry_by_code:
                        industry_by_code[code] = name
                for row in rows:
                    if not str(row.get("industry_name") or "").strip():
                        row["industry_name"] = industry_by_code.get(
                            str(row.get("stock_code") or "").zfill(6),
                            "",
                        )
            except Exception as exc:
                _safe_fallback_log(
                    logging.DEBUG, "exact_date_industry_membership", exc,
                )
        return rows
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "recommendation", exc)
        return []


def _previous_market_state(trade_date: str) -> dict[str, Any] | None:
    if not trade_date or not _table_exists("st_market_state_daily"):
        return None
    try:
        rows = _db_read(
            """
            SELECT trade_date, candidate_state, final_state, candidate_streak,
                   state_days, cooldown_remaining, source_status
            FROM st_market_state_daily
            WHERE config_version = :config_version
              AND trade_date < :trade_date
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """,
            {
                "config_version": load_market_state_config()["config_version"],
                "trade_date": trade_date,
            },
        )
        return rows[0] if rows else None
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "market_state_history", exc)
        return None


def _kline_market_features(trade_date: str) -> dict[str, Any]:
    """Build deterministic market-state inputs from the dedicated K-line DB."""
    if not trade_date or not _kline_table_exists("sm_stock_kline"):
        return {}
    config = load_market_state_config()
    fallback = config.get("feature_fallbacks") or {}
    try:
        rows = read_sql_rows(
            get_kline_engine(),
            """
            SELECT trade_date,
                   AVG(change_pct) AS market_change_pct,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END)
                     * 100.0 / NULLIF(COUNT(*), 0) AS breadth_pct,
                   COUNT(*) AS universe_count
            FROM sm_stock_kline
            WHERE trade_date <= :trade_date
              AND trade_date >= DATE_SUB(:trade_date, INTERVAL 45 DAY)
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(0|3|6)'
              AND change_pct IS NOT NULL
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 20
            """,
            {"trade_date": trade_date},
            context="strategy_center_market_features",
        )
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "kline_market_features", exc)
        return {}
    minimum_count = int(fallback.get("minimum_daily_universe_count") or 1000)
    valid = [
        row for row in rows
        if int(_num(row.get("universe_count"), 0) or 0) >= minimum_count
    ]
    if not valid:
        return {}
    changes = [float(_num(row.get("market_change_pct"), 0.0) or 0.0) for row in valid]
    breadths = [float(_num(row.get("breadth_pct"), 0.0) or 0.0) for row in valid]
    ret_5d = sum(changes[:5])
    ret_20d = sum(changes[:20])
    breadth = breadths[0]
    breadth_5d = sum(breadths[:5]) / max(1, min(5, len(breadths)))
    market_change = changes[0]
    trend = _clamp(50 + ret_5d * 3 + ret_20d + (breadth - 50) * 0.4, default=50.0)
    switch = _clamp(abs(breadth - breadth_5d) * 2 + abs(ret_5d) * 3, default=0.0)
    risk = _clamp(
        20
        + max(0.0, -market_change) * 10
        + max(0.0, 50 - breadth) * 1.2
        + max(0.0, -ret_5d) * 4,
        default=20.0,
    )
    latest_date = normalize_trade_date(str(valid[0].get("trade_date") or "")[:10])
    return {
        "data_date": latest_date,
        "market_change_pct": round(market_change, 4),
        "breadth_pct": round(breadth, 2),
        "trend_score": trend,
        "switch_score": switch,
        "risk_score": risk,
        "ret_5d_pct": round(ret_5d, 4),
        "ret_20d_pct": round(ret_20d, 4),
        "universe_count": int(_num(valid[0].get("universe_count"), 0) or 0),
        "source": str(fallback.get("source") or "sm_stock_kline_equal_weight_a_share"),
        "is_current": latest_date == trade_date,
    }


def _fuse_event_and_tape_risk(
    snapshot: dict[str, Any],
    kline: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Require QMT tape confirmation before a news alert blocks the market."""
    config = config or load_market_state_config()
    policy = config.get("event_fusion") or {}
    kline_current = bool(kline.get("is_current"))
    if kline_current:
        for key in (
            "market_change_pct",
            "breadth_pct",
            "trend_score",
            "switch_score",
        ):
            if kline.get(key) is not None:
                snapshot[key] = kline[key]
        snapshot["market_input_source"] = (
            "qmt_attested_daily_equal_weight"
        )

    kline_risk = _num(kline.get("risk_score"), None)
    event_scores = [
        _num(snapshot.get("risk_off_score"), None),
        _num(snapshot.get("tech_risk_score"), None),
    ]
    event_scores = [value for value in event_scores if value is not None]
    event_risk = max(event_scores) if event_scores else None
    event_alert = bool(snapshot.get("tech_triggered")) or (
        event_risk is not None
        and event_risk
        >= float(policy.get("event_alert_score_gte", 70))
    )
    change = _num(snapshot.get("market_change_pct"), None)
    breadth = _num(snapshot.get("breadth_pct"), None)
    price_stress = (
        (
            change is not None
            and change
            <= float(policy.get("price_stress_market_change_lte", -1.5))
        )
        or (
            breadth is not None
            and breadth
            <= float(policy.get("price_stress_breadth_lte", 35))
        )
        or (
            kline_risk is not None
            and kline_risk
            >= float(policy.get("price_stress_kline_risk_gte", 70))
        )
    )
    systemic_event = (
        event_alert
        and event_risk is not None
        and event_risk
        >= float(policy.get("systemic_event_score_gte", 82))
        and (price_stress or not kline_current)
    )
    hard_event = bool(snapshot.get("hard_event"))
    risk_candidates = [
        value for value in (kline_risk, event_risk) if value is not None
    ]
    if (
        kline_current
        and event_alert
        and not systemic_event
        and not hard_event
    ):
        event_cap = float(
            policy.get("unconfirmed_event_risk_score_cap", 54)
        )
        risk_candidates = [
            value
            for value in (
                kline_risk,
                min(event_risk, event_cap)
                if event_risk is not None
                else None,
            )
            if value is not None
        ]
        trend_cap = float(
            policy.get("unconfirmed_event_trend_score_cap", 67)
        )
        if snapshot.get("trend_score") is not None:
            snapshot["trend_score"] = min(
                float(snapshot["trend_score"]),
                trend_cap,
            )
        snapshot["event_risk_status"] = (
            "SECTOR_CAUTION_TAPE_NOT_CONFIRMED"
        )
    elif systemic_event or hard_event:
        snapshot["event_risk_status"] = "SYSTEMIC_CONFIRMED"
    else:
        snapshot["event_risk_status"] = "NO_SYSTEMIC_EVENT"
    snapshot["event_risk_score"] = event_risk
    snapshot["price_stress_confirmed"] = bool(price_stress)
    snapshot["tech_triggered_raw"] = bool(
        snapshot.get("tech_triggered")
    )
    snapshot["tech_triggered"] = bool(systemic_event or hard_event)
    snapshot["risk_score"] = (
        max(risk_candidates) if risk_candidates else None
    )
    snapshot["extreme_event"] = bool(systemic_event or hard_event)
    return snapshot


def load_market_snapshot(
    trade_date: str, *, use_cache: bool = True,
) -> dict[str, Any]:
    """Combine current adapters with frozen, reproducible K-line fallbacks."""
    cache_ttl = 120 if trade_date == date.today().isoformat() else 600
    now_monotonic = time.monotonic()
    if use_cache:
        with _MARKET_SNAPSHOT_CACHE_LOCK:
            cached = _MARKET_SNAPSHOT_CACHE.get(trade_date)
            if cached and now_monotonic - cached[0] < cache_ttl:
                _MARKET_SNAPSHOT_CACHE.move_to_end(trade_date)
                result = copy.deepcopy(cached[1])
                result["cache_status"] = "hit"
                return result
            if cached:
                _MARKET_SNAPSHOT_CACHE.pop(trade_date, None)
    snapshot: dict[str, Any] = {
        "trade_date": trade_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_status": "degraded",
        "evidence": [],
    }
    adapter_errors: list[str] = []
    try:
        from server.api.routers.hot_data import (
            market_trend,
            market_sentiment,
            style_switch_signal,
            tech_risk_signal,
        )

        sentiment = market_sentiment(days=20, date=trade_date, top=8, include_signal=True)
        style = (
            sentiment.get("style_switch_signal") or {}
            if isinstance(sentiment, dict)
            else {}
        )
        if not style:
            style = style_switch_signal(date=trade_date, days=20)
        tech = style.get("tech_risk_signal") if isinstance(style, dict) else {}
        if not isinstance(tech, dict) or not tech:
            tech = tech_risk_signal(date=trade_date, days=2)
        long_term_trend = market_trend(date=trade_date)
        snapshot.update({
            "sentiment": sentiment if isinstance(sentiment, dict) else {},
            "style": style if isinstance(style, dict) else {},
            "tech": tech if isinstance(tech, dict) else {},
            "long_term_trend": (
                long_term_trend if isinstance(long_term_trend, dict) else {}
            ),
        })
        style = snapshot["style"]
        tech = snapshot["tech"]
        theme = (snapshot["sentiment"].get("theme_analysis") or {}) if isinstance(snapshot["sentiment"], dict) else {}
        breadth = (snapshot["sentiment"].get("breadth") or {}) if isinstance(snapshot["sentiment"], dict) else {}
        snapshot.update({
            "risk_off_score": _num(style.get("risk_off_score"), None),
            "switch_score": _num(style.get("switch_score"), None),
            "tech_risk_score": _num(tech.get("score"), None),
            "tech_triggered": bool(tech.get("triggered")),
            "breadth_pct": _num(breadth.get("up_ratio"), _num(breadth.get("up_pct"), None)),
            "trend_score": _num(theme.get("trend_score"), _num(theme.get("score"), None)),
        })
        if (
            snapshot.get("breadth_pct") is not None
            and 0 <= float(snapshot["breadth_pct"]) <= 1
        ):
            snapshot["breadth_pct"] = round(float(snapshot["breadth_pct"]) * 100, 2)
        adapter_errors = [
            str(item.get("error"))
            for item in snapshot.values()
            if isinstance(item, dict) and item.get("error")
        ]
        snapshot["evidence"] = (style.get("evidence") or [])[:8] + (tech.get("reasons") or tech.get("evidence") or [])[:8]
    except Exception as exc:
        adapter_errors = [str(exc)[:300]]
        snapshot["adapter_error"] = adapter_errors[0]
        snapshot["evidence"] = ["现有市场状态接口暂不可用，转用冻结K线公式"]

    state_config = load_market_state_config()
    kline = _kline_market_features(trade_date)
    snapshot["kline_fallback"] = kline
    for key in ("market_change_pct", "breadth_pct", "trend_score", "switch_score"):
        if snapshot.get(key) is None and kline.get(key) is not None:
            snapshot[key] = kline[key]
    snapshot = _fuse_event_and_tape_risk(
        snapshot,
        kline,
        config=state_config,
    )
    required = state_config["required_inputs"]
    missing = [key for key in required if _num(snapshot.get(key), None) is None]
    if missing:
        snapshot["source_status"] = "missing"
        snapshot["missing_inputs"] = missing
    elif kline and not bool(kline.get("is_current")):
        snapshot["source_status"] = "degraded"
        snapshot["evidence"].append(
            f"K线市场输入最新日期为{kline.get('data_date')}，晚于该日的数据尚未到库"
        )
    else:
        snapshot["source_status"] = "fresh"
    if kline:
        snapshot["evidence"].append(
            f"K线等权市场输入：{kline.get('data_date')}，有效股票{kline.get('universe_count')}只"
        )
    if adapter_errors:
        snapshot["adapter_errors"] = adapter_errors[:5]
    previous = _previous_market_state(trade_date)
    state = infer_market_state(snapshot, previous=previous)
    state["previous_state"] = previous
    state["input"] = {
        key: snapshot.get(key)
        for key in state_config["required_inputs"]
    }
    snapshot["market_state"] = state["key"]
    snapshot["state"] = state
    snapshot["cache_status"] = "fresh_compute"
    if use_cache:
        with _MARKET_SNAPSHOT_CACHE_LOCK:
            _MARKET_SNAPSHOT_CACHE[trade_date] = (
                time.monotonic(), copy.deepcopy(snapshot),
            )
            _MARKET_SNAPSHOT_CACHE.move_to_end(trade_date)
            while len(_MARKET_SNAPSHOT_CACHE) > 32:
                _MARKET_SNAPSHOT_CACHE.popitem(last=False)
    return snapshot


def build_strategy_cards(market: dict[str, Any], candidates: list[dict[str, Any]], configs: dict[str, dict[str, Any]], metrics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    counts = defaultdict(int)
    for candidate in candidates:
        for key in candidate.get("strategies") or []:
            counts[key] += 1
    cards = []
    state = market.get("market_state") or "risk_declining"
    for item in STRATEGY_CATALOG:
        key = item["key"]
        manifest_item = next(
            (
                value
                for value in load_stock_manifest()["strategies"]
                if value["key"] == key
            ),
            {},
        )
        metric = metrics.get(key) or {}
        weight = effective_weight(key, state, configs.get(key), metric, 1.0 if market.get("source_status") == "fresh" else 0.75)
        cards.append({
            **item,
            "enabled": bool((configs.get(key) or {}).get("enabled", True)),
            "version": (configs.get(key) or {}).get("version", 1),
            **weight,
            "today_signal_count": counts.get(key, 0),
            "sample_count": int(_num(metric.get("sample_count"), 0) or 0),
            "return_pct": metric.get("return_pct"),
            "max_drawdown_pct": metric.get("max_drawdown_pct"),
            "win_rate_pct": metric.get("win_rate_pct"),
            "profit_factor": metric.get("profit_factor"),
            "metric_source": metric.get("source") or "暂无复盘样本",
            "metric_as_of_date": str(metric.get("as_of_date") or "")[:10],
            "metric_note": metric.get("metric_note") or "",
            "model_status": metric.get("model_status") or "frozen_manifest_adapter",
            "model_version": metric.get("model_version") or load_stock_manifest()["manifest_version"],
            "manifest_hash": stock_manifest_hash(),
            "formula": manifest_item.get("score_formula"),
            "hold_formula": manifest_item.get("hold_formula"),
            "parameters": manifest_item.get("parameters"),
            "entry_rules": manifest_item.get("entry_rules") or [],
            "exit_rules": manifest_item.get("exit_rules") or {},
        })
    return cards


def versioned_strategy_configuration() -> dict[str, Any]:
    stock = load_stock_manifest()
    market = load_market_state_config()
    return {
        "status": "ok",
        "stock": {
            "manifest_version": stock["manifest_version"],
            "config_hash": stock_manifest_hash(),
            "schema_version": stock["schema_version"],
            "status": stock["status"],
            "frozen_at": stock["frozen_at"],
            "strategies": stock["strategies"],
            "legacy_merge_map": stock.get("legacy_merge_map") or {},
            "disabled_labels": stock.get("disabled_labels") or {},
        },
        "market_state": {
            "config_version": market["config_version"],
            "config_hash": market_state_config_hash(),
            "schema_version": market["schema_version"],
            "status": market["status"],
            "frozen_at": market["frozen_at"],
            "required_inputs": market["required_inputs"],
            "feature_fallbacks": market.get("feature_fallbacks") or {},
            "thresholds": market["thresholds"],
            "transition": market["transition"],
            "strategy_multipliers": market["strategy_multipliers"],
            "calibration": market.get("calibration") or {},
        },
        "automatic_order_submission": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_etf_forward_ledger(limit: int = 100) -> dict[str, Any]:
    """Expose the append-only QMT ETF forward ledger from the K-line DB."""
    limit = max(1, min(500, int(limit)))
    engine = get_kline_engine()
    if not _kline_table_exists("st_etf_forward_strategy"):
        return {
            "status": "not_registered",
            "message": "ETF前向策略尚未在QMT本地库注册",
            "strategies": [],
            "observations": [],
            "observation_count": 0,
            "automatic_order_submission": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    strategies = read_sql_rows(
        engine,
        """
        SELECT strategy_version, config_hash, frozen_at, forward_start_date,
               mode, status, registered_at
        FROM st_etf_forward_strategy
        ORDER BY registered_at DESC
        """,
        context="strategy_center_etf_registry",
        stringify_datetime=True,
    )
    observations: list[dict[str, Any]] = []
    if _kline_table_exists("st_etf_forward_observation"):
        observations = read_sql_rows(
            engine,
            f"""
            SELECT strategy_version, config_hash, data_date, observed_at,
                   data_source, input_hash, signal_type, execution_date,
                   target_json, context_json, created_at
            FROM st_etf_forward_observation
            ORDER BY data_date DESC, id DESC
            LIMIT {limit}
            """,
            context="strategy_center_etf_observations",
            stringify_datetime=True,
        )
        for row in observations:
            row["target"] = _json_value(row.pop("target_json", None), {})
            row["context"] = _json_value(row.pop("context_json", None), {})
    latest_etf_date = ""
    try:
        rows = read_sql_rows(
            engine,
            """
            SELECT MAX(trade_date) AS data_date
            FROM sm_etf_kline
            WHERE k_type = 1 AND adjust_type = 1
              AND validation_status = 'passed'
              AND quality_status = 'validated'
            """,
            context="strategy_center_etf_latest",
            stringify_datetime=True,
        )
        latest_etf_date = str((rows[0] if rows else {}).get("data_date") or "")[:10]
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "etf_forward_latest_date", exc)
    if observations:
        status = "collecting"
        message = "已产生真实前向观察记录"
    else:
        starts = [
            normalize_trade_date(str(item.get("forward_start_date") or "")[:10])
            for item in strategies
        ]
        valid_starts = [item for item in starts if item]
        earliest = min(valid_starts) if valid_starts else ""
        status = "waiting_forward_start" if earliest and date.today().isoformat() < earliest else "waiting_validated_close"
        message = (
            f"冻结完成，等待{earliest}及之后的真实收盘数据自然产生"
            if earliest
            else "冻结完成，等待真实收盘数据自然产生"
        )
    return {
        "status": status,
        "message": message,
        "strategies": strategies,
        "observations": observations,
        "observation_count": len(observations),
        "latest_validated_etf_date": latest_etf_date,
        "backfill": "prohibited",
        "automatic_order_submission": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


_QMT_MEMBERSHIP_QUALITY = "QMT_VALIDATED"
_QMT_MEMBERSHIP_CAPTURE_MODE = "qmt_close_full_refresh"
_QMT_MEMBERSHIP_HASH_CHARS = frozenset("0123456789abcdef")
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_MEMBERSHIP_DATA_CATEGORY = "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP"
_MEMBERSHIP_EXCLUDED_DATA_CATEGORIES = (
    "SECTOR_HEAT_HISTORY",
    "SECTOR_ROTATION_HISTORY",
)


def _membership_truth_boundary(member_type: str) -> dict[str, Any]:
    scope_label = "概念成分归属历史" if member_type == "concept" else "行业成分归属历史"
    return {
        "data_category": _MEMBERSHIP_DATA_CATEGORY,
        "data_category_label": scope_label,
        "data_semantics": (
            "指定交易日收盘后的全量成分归属关系；"
            "不代表板块热度、资金强弱或轮动信号"
        ),
        "excluded_data_categories": list(
            _MEMBERSHIP_EXCLUDED_DATA_CATEGORIES
        ),
    }


def _membership_snapshot_hash(
    rows: Iterable[dict[str, Any]], *, member_type: str,
) -> str:
    columns = (
        ("concept_code", "concept_name", "stock_code", "short_name")
        if member_type == "concept"
        else (
            "industry_code", "industry_name", "industry_type",
            "stock_code", "short_name",
        )
    )
    payload = json.dumps(
        sorted(
            tuple(str(row.get(column) or "") for column in columns)
            for row in rows
        ),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _membership_snapshot_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace(" ", "T")
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI_TZ).replace(tzinfo=None)
    return parsed


def _membership_run_view(row: dict[str, Any]) -> dict[str, Any]:
    snapshot_date = normalize_trade_date(str(row.get("snapshot_date") or "")[:10])
    return {
        "snapshot_date": snapshot_date,
        "source": str(row.get("source") or ""),
        "quality_status": str(row.get("quality_status") or ""),
        "capture_mode": str(row.get("capture_mode") or ""),
        "concept_count": int(_num(row.get("concept_count"), 0) or 0),
        "concept_relation_count": int(
            _num(row.get("concept_relation_count"), 0) or 0
        ),
        "industry_count": int(_num(row.get("industry_count"), 0) or 0),
        "industry_relation_count": int(
            _num(row.get("industry_relation_count"), 0) or 0
        ),
        "concept_hash": str(row.get("concept_hash") or "").lower(),
        "industry_hash": str(row.get("industry_hash") or "").lower(),
        "captured_at": str(row.get("captured_at") or ""),
        "declared_contract_eligible": bool(
            snapshot_date
            and str(row.get("source") or "") == PROVIDER_ID
            and str(row.get("quality_status") or "")
            == _QMT_MEMBERSHIP_QUALITY
            and str(row.get("capture_mode") or "")
            == _QMT_MEMBERSHIP_CAPTURE_MODE
        ),
    }


def load_membership_snapshot_history(
    *,
    snapshot_date: str = "",
    member_type: str = "concept",
    group_code: str = "",
    stock_code: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """Read one fully replayed immutable BigQMT membership snapshot.

    ``limit`` is presentation-only.  The published relation count, group
    count, row identities, capture time and canonical hash are always checked
    against the complete exact-date/provider snapshot before any filter is
    applied.  An explicit date is exact; this function never silently serves
    an older snapshot for a requested day.
    """
    member_type = str(member_type or "concept").strip().lower()
    if member_type not in {"concept", "industry"}:
        raise ValueError("member_type must be concept or industry")
    truth_boundary = _membership_truth_boundary(member_type)
    limit = max(1, min(1000, int(limit)))
    raw_requested = str(snapshot_date or "").strip()
    requested = normalize_trade_date(raw_requested)
    if raw_requested and (
        not requested or raw_requested != requested
    ):
        raise ValueError("snapshot_date must be an ISO calendar date")
    table = (
        "qmt_concept_member_snapshot"
        if member_type == "concept"
        else "qmt_industry_member_snapshot"
    )
    if (
        not _kline_table_exists("qmt_membership_snapshot_run")
        or not _kline_table_exists(table)
    ):
        return {
            **truth_boundary,
            "status": "not_initialized",
            "status_label": "历史快照表未初始化",
            "snapshot_date": "",
            "member_type": member_type,
            "runs": [],
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }
    engine = get_kline_engine()
    runs = read_sql_rows(
        engine,
        """
        SELECT snapshot_date, source, quality_status, capture_mode,
               concept_count, concept_relation_count, industry_count,
               industry_relation_count, concept_hash, industry_hash, captured_at
        FROM qmt_membership_snapshot_run
        WHERE source = :source
        ORDER BY snapshot_date DESC, id DESC
        LIMIT 1000
        """,
        {"source": PROVIDER_ID},
        context="strategy_center_membership_runs",
        stringify_datetime=True,
    )
    run_views = [_membership_run_view(dict(row)) for row in runs]
    available_dates = sorted({
        str(item.get("snapshot_date") or "")
        for item in run_views
        if str(item.get("snapshot_date") or "")
    })
    target = requested or (available_dates[-1] if available_dates else "")
    if requested:
        selected_runs = read_sql_rows(
            engine,
            """
            SELECT snapshot_date, source, quality_status, capture_mode,
                   concept_count, concept_relation_count, industry_count,
                   industry_relation_count, concept_hash, industry_hash,
                   captured_at
            FROM qmt_membership_snapshot_run
            WHERE source = :source AND snapshot_date = :snapshot_date
            ORDER BY captured_at, id
            """,
            {"source": PROVIDER_ID, "snapshot_date": requested},
            context="strategy_center_membership_exact_run",
            stringify_datetime=True,
        )
        if selected_runs and requested not in available_dates:
            run_views = [
                *[_membership_run_view(dict(row)) for row in selected_runs],
                *run_views,
            ]
    else:
        selected_runs = [
            dict(row) for row in runs
            if normalize_trade_date(str(row.get("snapshot_date") or "")[:10])
            == target
            and str(row.get("source") or "") == PROVIDER_ID
        ]
    if not target:
        return {
            **truth_boundary,
            "status": "empty",
            "status_label": "暂无历史快照",
            "reason": "指定QMT提供方尚未发布任何成员快照",
            "snapshot_date": "",
            "requested_date": requested,
            "member_type": member_type,
            "runs": run_views,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }
    if not selected_runs:
        return {
            **truth_boundary,
            "status": "empty",
            "status_label": "精确日期快照不存在",
            "reason": f"{target}没有{PROVIDER_ID}精确日期成员快照，不回退旧日数据",
            "snapshot_date": target,
            "requested_date": requested,
            "selection_mode": "EXACT" if requested else "LATEST_DECLARED",
            "member_type": member_type,
            "runs": run_views,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }
    if len(selected_runs) != 1:
        return {
            **truth_boundary,
            "status": "integrity_error",
            "status_label": "快照完整性失败",
            "reason": f"{target}存在重复QMT运行记录",
            "snapshot_date": target,
            "requested_date": requested,
            "member_type": member_type,
            "runs": run_views,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }
    run = selected_runs[0]
    if str(run.get("source") or "") != PROVIDER_ID:
        return {
            **truth_boundary,
            "status": "integrity_error",
            "status_label": "快照完整性失败",
            "reason": f"{target}运行记录来源不是{PROVIDER_ID}",
            "snapshot_date": target,
            "requested_date": requested,
            "member_type": member_type,
            "runs": run_views,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }
    if str(run.get("quality_status") or "") != _QMT_MEMBERSHIP_QUALITY:
        return {
            **truth_boundary,
            "status": "not_ready",
            "status_label": "快照尚未验真",
            "reason": f"{target}快照未达到{_QMT_MEMBERSHIP_QUALITY}",
            "snapshot_date": target,
            "requested_date": requested,
            "member_type": member_type,
            "runs": run_views,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }

    if member_type == "concept":
        group_column = "concept_code"
        group_name_column = "concept_name"
        snapshot_columns = (
            "concept_code, concept_name, stock_code, short_name"
        )
        count_field = "concept_relation_count"
        group_count_field = "concept_count"
        hash_field = "concept_hash"
    else:
        group_column = "industry_code"
        group_name_column = "industry_name"
        snapshot_columns = (
            "industry_code, industry_name, industry_type, stock_code, short_name"
        )
        count_field = "industry_relation_count"
        group_count_field = "industry_count"
        hash_field = "industry_hash"
    raw_rows = read_sql_rows(
        engine,
        f"""
        SELECT snapshot_date, source, {snapshot_columns},
               quality_status, captured_at
        FROM `{table}`
        WHERE snapshot_date = :snapshot_date AND source = :source
        ORDER BY `{group_column}`, stock_code
        """,
        {"snapshot_date": target, "source": PROVIDER_ID},
        context="strategy_center_membership_history_full_replay",
        stringify_datetime=True,
    )
    expected_count = int(_num(run.get(count_field), 0) or 0)
    expected_group_count = int(_num(run.get(group_count_field), 0) or 0)
    published_hash = str(run.get(hash_field) or "").lower()
    run_time = _membership_snapshot_time(run.get("captured_at"))
    target_day = date.fromisoformat(target)
    earliest = datetime.combine(target_day, datetime.min.time()).replace(hour=15)
    latest = datetime.combine(target_day + timedelta(days=1), datetime.min.time())
    integrity_errors: list[str] = []
    if str(run.get("source") or "") != PROVIDER_ID:
        integrity_errors.append("运行记录来源不是指定QMT提供方")
    if str(run.get("capture_mode") or "") != _QMT_MEMBERSHIP_CAPTURE_MODE:
        integrity_errors.append("运行记录不是QMT收盘全量冻结模式")
    if (
        len(published_hash) != 64
        or any(char not in _QMT_MEMBERSHIP_HASH_CHARS for char in published_hash)
    ):
        integrity_errors.append("运行记录缺少有效发布哈希")
    if expected_count <= 0 or len(raw_rows) != expected_count:
        integrity_errors.append(
            f"发布关系数{expected_count}与完整读取数{len(raw_rows)}不一致"
        )
    actual_group_count = len({
        str(row.get(group_column) or "") for row in raw_rows
        if str(row.get(group_column) or "")
    })
    if expected_group_count <= 0 or actual_group_count != expected_group_count:
        integrity_errors.append(
            f"发布分组数{expected_group_count}与重算数{actual_group_count}不一致"
        )
    if run_time is None or not earliest <= run_time < latest:
        integrity_errors.append("快照发布时间不在目标交易日收盘后窗口")
    for row in raw_rows:
        row_time = _membership_snapshot_time(row.get("captured_at"))
        if (
            normalize_trade_date(str(row.get("snapshot_date") or "")[:10])
            != target
            or str(row.get("source") or "") != PROVIDER_ID
            or str(row.get("quality_status") or "")
            != _QMT_MEMBERSHIP_QUALITY
            or row_time is None
            or run_time is None
            or row_time != run_time
        ):
            integrity_errors.append("成员行的日期、来源、验真状态或发布时间漂移")
            break
    actual_hash = _membership_snapshot_hash(
        [dict(row) for row in raw_rows], member_type=member_type,
    )
    if actual_hash != published_hash:
        integrity_errors.append("完整成员集合canonical hash与发布哈希不一致")
    if integrity_errors:
        return {
            **truth_boundary,
            "status": "integrity_error",
            "status_label": "快照完整性失败",
            "reason": "；".join(integrity_errors),
            "snapshot_date": target,
            "requested_date": requested,
            "selection_mode": "EXACT" if requested else "LATEST_DECLARED",
            "member_type": member_type,
            "source": PROVIDER_ID,
            "runs": run_views,
            "published_relation_count": expected_count,
            "verified_relation_count": len(raw_rows),
            "published_snapshot_hash": published_hash,
            "replayed_snapshot_hash": actual_hash,
            "data": [],
            "snapshot_complete": False,
            "automatic_real_order_submission": False,
        }

    normalized_stock = ""
    if stock_code:
        normalized_stock = str(stock_code).strip().split(".", 1)[0].zfill(6)
        if len(normalized_stock) != 6 or not normalized_stock.isdigit():
            raise ValueError("stock_code must contain a six-digit security code")
    selected_group = str(group_code or "").strip()
    filtered = [
        dict(row) for row in raw_rows
        if (not selected_group or str(row.get(group_column) or "") == selected_group)
        and (not normalized_stock or str(row.get("stock_code") or "").zfill(6) == normalized_stock)
    ]
    display_rows = []
    for row in filtered[:limit]:
        item = {
            "snapshot_date": target,
            "source": PROVIDER_ID,
            "group_code": str(row.get(group_column) or ""),
            "group_name": str(row.get(group_name_column) or ""),
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "short_name": str(row.get("short_name") or ""),
            "quality_status": _QMT_MEMBERSHIP_QUALITY,
            "captured_at": str(row.get("captured_at") or ""),
        }
        if member_type == "industry":
            item["industry_type"] = str(row.get("industry_type") or "")
        display_rows.append(item)
    contract = {
        "schema": "probiga.qmt-membership-history-view.v2",
        "data_category": truth_boundary["data_category"],
        "excluded_data_categories": truth_boundary[
            "excluded_data_categories"
        ],
        "snapshot_date": target,
        "source": PROVIDER_ID,
        "quality_status": _QMT_MEMBERSHIP_QUALITY,
        "capture_mode": _QMT_MEMBERSHIP_CAPTURE_MODE,
        "member_type": member_type,
        "published_group_count": expected_group_count,
        "published_relation_count": expected_count,
        "published_snapshot_hash": published_hash,
        "captured_at": run_time.isoformat(timespec="seconds") if run_time else "",
    }
    return {
        **truth_boundary,
        "status": "verified",
        "status_label": "完整快照已验真",
        "reason": "完整成员集合的来源、行数、分组数、时间与canonical hash均已重放",
        "snapshot_date": target,
        "requested_date": requested,
        "selection_mode": "EXACT" if requested else "LATEST_VERIFIED",
        "member_type": member_type,
        "source": PROVIDER_ID,
        "quality_status": _QMT_MEMBERSHIP_QUALITY,
        "capture_mode": _QMT_MEMBERSHIP_CAPTURE_MODE,
        "captured_at": contract["captured_at"],
        "runs": run_views,
        "published_group_count": expected_group_count,
        "published_relation_count": expected_count,
        "verified_relation_count": len(raw_rows),
        "published_snapshot_hash": published_hash,
        "replayed_snapshot_hash": actual_hash,
        "snapshot_contract_hash": _canonical_hash(contract),
        "snapshot_complete": True,
        "verified_full_snapshot_before_filters": True,
        "filter_group_code": selected_group,
        "filter_stock_code": normalized_stock,
        "filtered_relation_count": len(filtered),
        "total_returned": len(display_rows),
        "display_truncated": len(filtered) > len(display_rows),
        "data": display_rows,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_qmt_kline_attestation_status(limit: int = 30) -> dict[str, Any]:
    """Expose row-level BigQMT attestation runs from the K-line database."""
    limit = max(1, min(200, int(limit)))
    if not _kline_table_exists("qmt_kline_attestation_run"):
        return {
            "status": "not_initialized",
            "message": "旧日K逐行QMT补证尚未在Windows本地数据边界运行",
            "runs": [],
            "mismatches": [],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    engine = get_kline_engine()
    runs = read_sql_rows(
        engine,
        f"""
        SELECT run_id, provider, start_date, end_date, status,
               target_rows, qmt_rows, matched_rows, missing_qmt_rows,
               mismatched_rows, already_attested_rows, updated_rows,
               tolerance_json, started_at, finished_at, error_message
        FROM qmt_kline_attestation_run
        ORDER BY started_at DESC
        LIMIT {limit}
        """,
        context="strategy_center_qmt_attestation_runs",
        stringify_datetime=True,
    )
    for row in runs:
        row["tolerances"] = _json_value(row.pop("tolerance_json", None), {})
        target_rows = int(_num(row.get("target_rows"), 0) or 0)
        matched_rows = int(_num(row.get("matched_rows"), 0) or 0)
        row["coverage_pct"] = round(matched_rows * 100.0 / target_rows, 4) if target_rows else 0.0
    mismatches: list[dict[str, Any]] = []
    if runs and _kline_table_exists("qmt_kline_attestation_mismatch"):
        mismatches = read_sql_rows(
            engine,
            """
            SELECT run_id, trade_date, stock_code, reason,
                   target_close, qmt_close, target_volume, qmt_volume,
                   target_amount, qmt_amount, created_at
            FROM qmt_kline_attestation_mismatch
            WHERE run_id = :run_id
            ORDER BY trade_date, stock_code
            LIMIT 200
            """,
            {"run_id": runs[0]["run_id"]},
            context="strategy_center_qmt_attestation_mismatch",
            stringify_datetime=True,
        )
    latest = runs[0] if runs else None
    status = (
        "complete"
        if latest and latest.get("status") == "COMPLETED"
        else "partial"
        if latest
        else "empty"
    )
    return {
        "status": status,
        "provider_required": "gj_big_qmt_inner",
        "quality_status_on_match": "QMT_ATTESTED",
        "unmatched_rows_are_modified": False,
        "runs": runs,
        "latest_mismatches": mismatches,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_persisted_strategy_center_compact(
    trade_date: str = "",
    limit: int = 200,
) -> dict[str, Any] | None:
    """Rebuild the small candidate view from the exact canonical-bound run.

    A strategy-center run is display-authoritative only when the current
    canonical governance row names its exact ``strategy_center_run_uid``.
    Selecting the newest standalone ``done`` row would let a legacy/manual
    writer replace the pool shown by the UI without a canonical governance
    decision.
    """
    limit = max(1, min(500, int(limit)))
    requested = normalize_trade_date(trade_date)
    run_where = "AND trade_date = :trade_date" if requested else ""
    params = {"trade_date": requested} if requested else {}
    runs = _db_read(
        f"""
        SELECT canonical.governance_run_uid,
               canonical.governance_trade_date,
               canonical.governance_result_json,
               canonical.governance_result_hash,
               canonical.governance_finished_at,
               center.run_uid, center.trade_date, center.market_state,
               center.state_confidence, center.source_status,
               center.candidate_count, center.conflict_count,
               center.finished_at
        FROM (
            SELECT run_uid AS governance_run_uid,
                   trade_date AS governance_trade_date,
                   result_json AS governance_result_json,
                   result_hash AS governance_result_hash,
                   finished_at AS governance_finished_at
            FROM st_strategy_governance_run
            WHERE status = 'COMPLETED' AND is_canonical = 1 {run_where}
            ORDER BY trade_date DESC, run_revision DESC,
                     finished_at DESC, created_at DESC, run_uid DESC
            LIMIT 1
        ) AS canonical
        LEFT JOIN st_strategy_center_run AS center
          ON center.run_uid = JSON_UNQUOTE(JSON_EXTRACT(
                 canonical.governance_result_json,
                 '$.strategy_center_run_uid'
             ))
         AND center.trade_date = canonical.governance_trade_date
         AND center.status = 'done'
        LIMIT 1
        """,
        params,
    )
    if not runs:
        return None
    run = runs[0]
    governance_result_json = run.get("governance_result_json")
    if not isinstance(governance_result_json, str):
        return None
    governance_result_hash = str(run.get("governance_result_hash") or "")
    if (
        len(governance_result_hash) != 64
        or any(character not in "0123456789abcdef" for character in governance_result_hash)
        or hashlib.sha256(governance_result_json.encode("utf-8")).hexdigest()
        != governance_result_hash
    ):
        return None
    governance_result = _json_value(governance_result_json, None)
    if not isinstance(governance_result, dict):
        return None
    governance_run_uid = str(run.get("governance_run_uid") or "").strip()
    governance_trade_date = normalize_trade_date(
        run.get("governance_trade_date")
    )
    bound_run_uid = str(
        governance_result.get("strategy_center_run_uid") or ""
    ).strip()
    center_run_uid = str(run.get("run_uid") or "").strip()
    valid_uid = lambda value: (
        len(value) == 32
        and all(character in "0123456789abcdef" for character in value)
    )
    if (
        not valid_uid(governance_run_uid)
        or not valid_uid(bound_run_uid)
        or center_run_uid != bound_run_uid
        or governance_result.get("is_canonical") is not True
        or governance_result.get("result_mode") != "CANONICAL_PERSISTED"
        or str(governance_result.get("run_uid") or "") != governance_run_uid
        or normalize_trade_date(governance_result.get("trade_date"))
        != governance_trade_date
        or normalize_trade_date(run.get("trade_date")) != governance_trade_date
        or (requested and governance_trade_date != requested)
    ):
        return None
    signals = _db_read(
        """
        SELECT stock_code, stock_name, strategy_key, market_state,
               signal_direction, signal_status, effective_score,
               model_confidence, risk_level, gate_status, gate_reason,
               entry_low, entry_high, stop_loss, today_signal,
               data_snapshot_json
        FROM st_strategy_center_signal
        WHERE run_uid = :run_uid
        ORDER BY stock_code, strategy_key
        """,
        {"run_uid": run["run_uid"]},
    )
    if not signals:
        return None
    state_key = str(run.get("market_state") or "unknown")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        code = str(signal.get("stock_code") or "").zfill(6)
        if not code:
            continue
        data_snapshot = _json_value(signal.pop("data_snapshot_json", None), {})
        signal["data_date"] = normalize_trade_date(data_snapshot.get("data_date"))
        signal["industry_name"] = str(
            data_snapshot.get("industry_name")
            or data_snapshot.get("theme_code")
            or ""
        )
        for binding_field in (
            "strategy_version", "strategy_version_hash",
            "execution_binding_hash", "adapter_artifact_sha256",
            "cost_model_hash", "candidate_run_uid",
            "candidate_receipt_hash", "candidate_input_hash",
            "candidate_output_hash",
        ):
            signal[binding_field] = str(
                data_snapshot.get(binding_field) or ""
            )
        grouped[code].append(signal)

    candidates: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    risk_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    for code, stock_signals in grouped.items():
        decision = resolve_conflict(stock_signals, state_key)
        best = max(
            stock_signals,
            key=lambda item: (
                _num(item.get("effective_score"), 0.0) or 0.0,
                _num(item.get("model_confidence"), 0.0) or 0.0,
            ),
        )
        industry_by_strategy = {
            str(item.get("strategy_key") or ""): str(
                item.get("industry_name") or ""
            ).strip()
            for item in stock_signals
            if str(item.get("strategy_key") or "").strip()
            and str(item.get("industry_name") or "").strip()
        }
        industry_names = sorted(set(industry_by_strategy.values()))
        candidate = {
            "priority": (
                "A"
                if decision["final_status"] in {"READY", "WATCH"}
                and decision["final_direction"] == "BUY"
                else "B"
            ),
            "stock_code": code,
            "stock_name": best.get("stock_name") or code,
            "final_direction": decision["final_direction"],
            "final_status": decision["final_status"],
            "model_confidence": max(
                (
                    _num(item.get("model_confidence"), 0.0) or 0.0
                    for item in stock_signals
                ),
                default=0.0,
            )
            or None,
            "today_signal": best.get("today_signal") or decision.get("conflict_summary"),
            "entry_low": best.get("entry_low"),
            "entry_high": best.get("entry_high"),
            "stop_loss": best.get("stop_loss"),
            "risk_level": max(
                (str(item.get("risk_level") or "LOW") for item in stock_signals),
                key=lambda value: risk_order.get(value, 0),
            ),
            "dominant_strategy": decision.get("dominant_strategy")
            or best.get("strategy_key"),
            "strategies": sorted(
                {
                    item.get("strategy_key")
                    for item in stock_signals
                    if item.get("strategy_key")
                }
            ),
            "conflict_summary": decision["conflict_summary"],
            "blocking_reasons": decision["blocking_reasons"],
            "data_date": best.get("data_date")
            or normalize_trade_date(run.get("trade_date")),
            "industry_name": best.get("industry_name") or "",
            "industry_names": industry_names,
            "industry_by_strategy": industry_by_strategy,
        }
        candidates.append(candidate)
        if decision["conflict"] or decision["final_status"] in {"BLOCKED", "CONFLICT"}:
            conflicts.append(
                {
                    "stock_code": code,
                    "stock_name": candidate["stock_name"],
                    "conflict_summary": decision["conflict_summary"],
                    "strategies": candidate["strategies"],
                }
            )
    candidates.sort(
        key=lambda item: (
            {
                "READY": 0,
                "WATCH": 1,
                "CONFLICT": 2,
                "SELL_ALERT": 3,
                "BLOCKED": 4,
                "INSUFFICIENT_DATA": 5,
            }.get(item.get("final_status"), 9),
            -(item.get("model_confidence") or 0),
            item.get("stock_code", ""),
        )
    )
    candidates = candidates[:limit]
    selected_codes = {item["stock_code"] for item in candidates}
    conflicts = [
        item for item in conflicts if item.get("stock_code") in selected_codes
    ]
    market_state = market_state_info(state_key)
    market_state["confidence"] = _num(run.get("state_confidence"))
    if state_key == "extreme_event":
        gate_status = "BLOCK_NEW_BUY"
        gate_reason = "极端事件模式自动停止新增买入信号"
    elif state_key in {"high_range", "risk_declining"}:
        gate_status = "REDUCE_NEW_BUY"
        gate_reason = f"{market_state.get('name')}模式自动降权新增买入"
    else:
        gate_status = "ALLOW_NEW_BUY"
        gate_reason = "市场状态允许生成研究候选，仍需价格和板块条件确认"
    source_payload = {
        "schema": "probiga.strategy-candidate-source.v1",
        "source": "st_strategy_center_run",
        "status": "COMPLETED",
        "query_completed": True,
        "trade_date": normalize_trade_date(run.get("trade_date")),
        "data_date": normalize_trade_date(run.get("trade_date")),
        "source_row_count": len(signals),
        "loaded_row_count": len(signals),
        "candidate_count": len(candidates),
        "candidate_identity": sorted(
            str(item.get("stock_code") or "") for item in candidates
        ),
        "completed_run_uid": str(run.get("run_uid") or ""),
        "completed_at": str(run.get("finished_at") or ""),
        "governance_run_uid": governance_run_uid,
        "governance_result_hash": governance_result_hash,
        "canonical_binding_verified": True,
        "reason": "规范治理结果精确绑定的策略中心运行提供候选源证明",
    }
    return {
        "status": "ok",
        "trade_date": normalize_trade_date(run.get("trade_date")),
        "data_date": max(
            (item.get("data_date") or "" for item in candidates),
            default=normalize_trade_date(run.get("trade_date")),
        ),
        "source_status": run.get("source_status") or "degraded",
        "is_stale": False,
        "market_state": market_state,
        "global_gate": {"status": gate_status, "reason": gate_reason},
        "candidate_source": {
            **source_payload,
            "source_hash": _canonical_hash(source_payload),
        },
        "candidates": candidates,
        "conflicts": conflicts,
        "summary": {
            "candidate_count": len(candidates),
            "conflict_count": len(conflicts),
        },
        "disclaimer": "仅用于研究候选和风险提示；未经明确确认不会执行任何交易。",
        "persisted_run_uid": run["run_uid"],
    }


def _dynamic_shadow_trade_session_ordinal(
    connection: Any, *, trade_date: str,
) -> int:
    """Return the exact 1-based open-session ordinal for ``trade_date``.

    A calendar-date hash cannot prove bounded waiting because exchange
    holidays may skip arbitrary hash positions.  The persisted exchange
    calendar gives the stable cursor required by the fairness contract.  A
    missing target session fails closed instead of silently weakening that
    contract.
    """

    target = normalize_trade_date(trade_date)
    if not target:
        raise ValueError("动态影子轮询缺少有效交易日")
    row = connection.execute(text("""
        SELECT COUNT(*) AS trade_session_ordinal,
               COALESCE(SUM(
                   CASE WHEN trade_date=:trade_date THEN 1 ELSE 0 END
               ), 0) AS target_open_session_count
        FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date<=:trade_date
    """), {"trade_date": date.fromisoformat(target)}).mappings().one()
    target_count = int(row.get("target_open_session_count") or 0)
    ordinal = int(row.get("trade_session_ordinal") or 0)
    if target_count != 1 or ordinal < 1:
        raise RuntimeError(
            "动态影子公平轮询缺少唯一的权威开市交易日"
        )
    return ordinal


def _dynamic_shadow_round_robin_plan_ids(
    plan_groups: Iterable[dict[str, Any]],
    *,
    trade_date: str,
    trade_session_ordinal: int,
    maximum_paper_orders_per_run: int,
    maximum_plans_scanned_per_run: int,
) -> tuple[list[str], dict[str, Any]]:
    """Order shadow plans with a stable, bounded-wait capacity cursor.

    The cursor advances by one full paper-capacity window per authoritative
    open session.  Therefore, while the eligible strategy set is stable and
    every strategy supplies a first plan, every first plan reaches the risk
    scan within ``ceil(strategy_count / capacity)`` consecutive competition
    runs.  The guarantee is deliberately about scan opportunity, never order
    acceptance: risk rejection consumes no paper-order capacity.
    """

    target = normalize_trade_date(trade_date)
    if not target:
        raise ValueError("动态影子轮询缺少有效交易日")
    if (
        type(trade_session_ordinal) is not int
        or trade_session_ordinal < 1
    ):
        raise ValueError("动态影子轮询缺少有效交易日序号")
    if (
        type(maximum_paper_orders_per_run) is not int
        or maximum_paper_orders_per_run < 1
    ):
        raise ValueError("动态影子轮询缺少有效模拟容量")
    if (
        type(maximum_plans_scanned_per_run) is not int
        or maximum_plans_scanned_per_run < maximum_paper_orders_per_run
    ):
        raise ValueError("动态影子轮询扫描上限小于模拟容量")
    normalized: list[dict[str, Any]] = []
    empty_strategy_keys: list[str] = []
    seen_strategies: set[str] = set()
    seen_plans: set[str] = set()
    for raw in plan_groups:
        strategy_key = str(raw.get("strategy_key") or "").strip()
        if not strategy_key or strategy_key in seen_strategies:
            raise ValueError("动态影子轮询策略身份缺失或重复")
        seen_strategies.add(strategy_key)
        plan_ids = [
            str(value).strip()
            for value in (raw.get("plan_ids") or ())
            if str(value).strip()
        ]
        if len(plan_ids) != len(set(plan_ids)):
            raise ValueError("同一动态策略的影子计划身份重复")
        overlap = seen_plans.intersection(plan_ids)
        if overlap:
            raise ValueError("动态影子计划被多个策略重复声明")
        seen_plans.update(plan_ids)
        if plan_ids:
            normalized.append({
                "strategy_key": strategy_key,
                "plan_ids": plan_ids,
            })
        else:
            empty_strategy_keys.append(strategy_key)
    normalized.sort(key=lambda item: item["strategy_key"])
    empty_strategy_keys.sort()
    strategy_count = len(normalized)
    cursor_index = (
        ((trade_session_ordinal - 1) * maximum_paper_orders_per_run)
        % strategy_count
        if strategy_count else 0
    )
    normalized = normalized[cursor_index:] + normalized[:cursor_index]
    ordered_plan_ids: list[str] = []
    maximum_depth = max(
        (len(item["plan_ids"]) for item in normalized), default=0
    )
    for candidate_index in range(maximum_depth):
        for item in normalized:
            if candidate_index < len(item["plan_ids"]):
                ordered_plan_ids.append(item["plan_ids"][candidate_index])
    bounded_wait_runs = (
        math.ceil(strategy_count / maximum_paper_orders_per_run)
        if strategy_count else 0
    )
    stable_strategy_keys = sorted(
        item["strategy_key"] for item in normalized
    )
    fairness_conditions = (
        "authoritative_open_session_ordinal_increments_by_one",
        "eligible_strategy_set_hash_remains_stable",
        "each_eligible_strategy_supplies_at_least_one_plan_per_run",
        "competition_runs_once_per_open_session",
        "paper_materializer_continues_scanning_after_risk_rejection",
        "paper_account_and_risk_controller_remain_available",
    )
    payload = {
        "schema": "probiga.dynamic-shadow-round-robin.v2",
        "trade_date": target,
        "trade_session_ordinal": trade_session_ordinal,
        "selection_policy": (
            "stable_open_session_capacity_cursor_then_candidate_round_robin"
        ),
        "cursor_source": "si_trade_calendar.trade_status=1",
        "strategy_cursor_index": cursor_index,
        "cursor_advance_per_session": maximum_paper_orders_per_run,
        "maximum_paper_orders_per_run": maximum_paper_orders_per_run,
        "maximum_plans_scanned_per_run": maximum_plans_scanned_per_run,
        "ordered_strategy_keys": [
            item["strategy_key"] for item in normalized
        ],
        "stable_strategy_set_hash": _canonical_hash(stable_strategy_keys),
        "strategy_count": strategy_count,
        "declared_strategy_count": strategy_count + len(empty_strategy_keys),
        "empty_strategy_count": len(empty_strategy_keys),
        "empty_strategy_keys_hash": _canonical_hash(empty_strategy_keys),
        "plan_count": len(ordered_plan_ids),
        "ordered_plan_ids_hash": _canonical_hash(ordered_plan_ids),
        "candidate_ordering": "candidate_index_round_robin",
        "bounded_wait_applies_to": "FIRST_PLAN_RISK_SCAN_OPPORTUNITY",
        "bounded_wait_maximum_consecutive_competition_runs": (
            bounded_wait_runs
        ),
        "bounded_wait_contract_status": "CONDITIONAL",
        "bounded_wait_required_conditions": list(fairness_conditions),
        "current_run_verified_inputs": {
            "exact_authoritative_open_session_ordinal": True,
            "participating_strategies_have_first_plan": True,
            "positive_paper_capacity": True,
            "scan_limit_covers_paper_capacity": True,
        },
        "bounded_wait_guarantees_order_acceptance": False,
        "risk_rejection_consumes_paper_order_capacity": False,
        "risk_rejection_counts_as_capacity_underallocation": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return ordered_plan_ids, {
        **payload,
        "competition_hash": _canonical_hash(payload),
    }


def _dynamic_execution_signals(
    *,
    trade_date: str,
    recommendation_rows: list[dict[str, Any]],
    market: dict[str, Any],
    configs: dict[str, dict[str, Any]],
    metrics: dict[str, dict[str, Any]],
    persist_receipts: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Discover any number of code-backed dynamic strategy adapters."""

    try:
        from server.engine.strategy_governance import load_registry

        registry = load_registry()
    except Exception as exc:
        _safe_fallback_log(logging.DEBUG, "dynamic_adapter_discovery", exc)
        return [], [{
            "strategy_key": "",
            "status": "DISCOVERY_UNAVAILABLE",
            "status_label": "执行适配器未部署/无效",
            "reason": f"动态适配器发现不可用：{type(exc).__name__}",
        }]
    context = {
        "trade_date": trade_date,
        "recommendation_rows": tuple(copy.deepcopy(recommendation_rows)),
        "market": copy.deepcopy(market),
        "configs": copy.deepcopy(configs),
        "metrics": copy.deepcopy(metrics),
    }
    signals: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []
    shadow_plan_groups: list[dict[str, Any]] = []
    governance_connection = (
        current_bound_sql_connection() if persist_receipts else None
    )
    if persist_receipts and governance_connection is None:
        raise RuntimeError("持久化动态适配器回执缺少治理事务连接")
    for strategy in registry:
        if str(strategy.get("source_kind") or "") != "runtime_registry":
            continue
        lifecycle = str(strategy.get("current_status") or "")
        enabled = strategy.get("enabled") is True
        adapter = strategy.get("execution_adapter") or {}
        status = {
            "strategy_key": str(strategy.get("strategy_key") or ""),
            "strategy_version": str(strategy.get("current_version") or ""),
            "enabled": enabled,
            "lifecycle_status": lifecycle,
            "status": str(adapter.get("status") or "UNDEPLOYED_OR_INVALID"),
            "status_label": str(
                adapter.get("status_label") or "执行适配器未部署/无效"
            ),
            "reason": str(
                adapter.get("reason") or "执行适配器未部署/无效"
            ),
            "adapter_capability_status": str(
                adapter.get("status") or "UNDEPLOYED_OR_INVALID"
            ),
            "execution_binding_hash": str(
                adapter.get("execution_binding_hash") or ""
            ),
            "adapter_artifact_sha256": str(
                adapter.get("artifact_sha256") or ""
            ),
            "cost_model_hash": str(adapter.get("cost_model_hash") or ""),
            "funding_pipeline_ready": (
                adapter.get("funding_pipeline_ready") is True
            ),
            "run_receipt_valid": False,
        }
        statuses.append(status)
        if not enabled:
            status.update({
                "status": "DISABLED",
                "status_label": "已禁用（禁止执行）",
                "reason": "策略已禁用，不执行、不进入候选源完成quorum或股票池",
            })
            continue
        if lifecycle == "RETIRED":
            status.update({
                "status": "RETIRED",
                "status_label": "已淘汰（禁止执行）",
                "reason": "策略已淘汰，不执行、不进入候选源完成quorum或股票池",
            })
            continue
        if lifecycle == "SUSPENDED":
            status.update({
                "status": "DIAGNOSTIC_ONLY",
                "status_label": "暂停诊断",
                "reason": "策略已暂停，仅保留独立诊断能力，不进入候选源quorum或股票池",
            })
            continue
        if adapter.get("executable") is not True:
            continue
        try:
            execution = execute_dynamic_adapter_candidate_batch(
                strategy, context, adapter_status=adapter,
            )
            receipt = execution["receipt"]
            shadow_plan_set: dict[str, Any] | None = None
            if persist_receipts:
                persist_strategy_adapter_run_receipt(
                    governance_connection, receipt
                )
                persist_strategy_adapter_candidate_facts(
                    governance_connection,
                    candidate_receipt=receipt,
                    candidates=execution.get("candidate_facts") or (),
                )
                if lifecycle == "SHADOW":
                    shadow_plan_set = (
                        create_dynamic_shadow_trial_plans_from_candidate_facts(
                            governance_connection,
                            strategy=strategy,
                            candidate_receipt=receipt,
                            maximum_target_bp=100,
                        )
                    )
                    shadow_plan_groups.append({
                        "strategy_key": str(
                            strategy.get("strategy_key") or ""
                        ),
                        "strategy_version": str(
                            strategy.get("current_version") or ""
                        ),
                        "plan_ids": tuple(
                            shadow_plan_set.get("plan_ids") or ()
                        ),
                        "status": status,
                    })
            signals.extend(execution["signals"])
            status.update({
                "status": "SHADOW_RUN_COMPLETED",
                "status_label": "影子候选运行完成",
                "reason": (
                    "CandidateBatch输入、输出、身份与运行回执校验通过；"
                    "仅current SHADOW且风险复算通过时创建不超过100bp的"
                    "内部模拟路径，未授予真实下单资格"
                ),
                "run_receipt_valid": True,
                "candidate_receipt_hash": str(
                    receipt.get("receipt_hash") or ""
                ),
                "candidate_input_hash": str(receipt.get("input_hash") or ""),
                "candidate_output_hash": str(
                    receipt.get("output_hash") or ""
                ),
                "candidate_stable_result_hash": str(
                    receipt.get("stable_result_hash") or ""
                ),
                "candidate_count": int(receipt.get("candidate_count") or 0),
                "candidate_run_uid": str(receipt.get("run_uid") or ""),
                "candidate_completed_at": str(
                    receipt.get("completed_at") or ""
                ),
                "candidate_run_receipt": dict(receipt),
                "shadow_trial_plan_count": int(
                    (shadow_plan_set or {}).get("plan_count") or 0
                ),
                "shadow_trial_plan_set_hash": str(
                    (shadow_plan_set or {}).get("plan_set_hash") or ""
                ),
                "shadow_bootstrap_paper_order_count": 0,
                "shadow_bootstrap_real_order_count": 0,
                "shadow_bootstrap_result": {
                    "status": (
                        "PENDING_GLOBAL_ROUND_ROBIN"
                        if persist_receipts and lifecycle == "SHADOW"
                        else "NOT_APPLICABLE_LIFECYCLE"
                    ),
                    "paper_order_count": 0,
                    "real_order_count": 0,
                    "automatic_real_order_submission": False,
                    "real_order_authority": False,
                },
            })
        except Exception as exc:
            if persist_receipts:
                raise
            status.update({
                "status": "ADAPTER_RUNTIME_INVALID",
                "status_label": "执行适配器未部署/无效",
                "reason": f"执行适配器候选校验失败：{type(exc).__name__}",
                "run_receipt_valid": False,
            })
            _safe_fallback_log(logging.WARNING, "dynamic_adapter_runtime", exc)
    if persist_receipts and shadow_plan_groups:
        from server.trading_v3.paper_execution import (
            DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN,
            DYNAMIC_SHADOW_BOOTSTRAP_MAX_PLANS_SCANNED_PER_RUN,
            materialize_dynamic_shadow_bootstrap_orders,
        )

        trade_session_ordinal = _dynamic_shadow_trade_session_ordinal(
            governance_connection,
            trade_date=trade_date,
        )
        ordered_plan_ids, competition = (
            _dynamic_shadow_round_robin_plan_ids(
                shadow_plan_groups,
                trade_date=trade_date,
                trade_session_ordinal=trade_session_ordinal,
                maximum_paper_orders_per_run=(
                    DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN
                ),
                maximum_plans_scanned_per_run=(
                    DYNAMIC_SHADOW_BOOTSTRAP_MAX_PLANS_SCANNED_PER_RUN
                ),
            )
        )
        global_bootstrap = materialize_dynamic_shadow_bootstrap_orders(
            governance_connection,
            plan_ids=ordered_plan_ids,
        )
        priority_rank = {
            str(strategy_key): index
            for index, strategy_key in enumerate(
                competition.get("ordered_strategy_keys") or (), 1
            )
        }
        compact_competition = {
            key: value for key, value in competition.items()
            if key != "ordered_strategy_keys"
        }
        created_by_plan = {
            str(item.get("plan_id") or ""): dict(item)
            for item in (global_bootstrap.get("created") or [])
            if isinstance(item, dict)
        }
        skipped_by_plan = {
            str(item.get("plan_id") or ""): dict(item)
            for item in (global_bootstrap.get("skipped") or [])
            if isinstance(item, dict)
        }
        scanned_plan_ids = {
            str(value) for value in (
                global_bootstrap.get("scanned_plan_ids") or ()
            )
        }
        for group in shadow_plan_groups:
            plan_ids = [str(value) for value in group["plan_ids"]]
            created = [
                created_by_plan[plan_id]
                for plan_id in plan_ids if plan_id in created_by_plan
            ]
            skipped = [
                skipped_by_plan[plan_id]
                for plan_id in plan_ids if plan_id in skipped_by_plan
            ]
            scanned_skipped = [
                item for item in skipped
                if str(item.get("plan_id") or "") in scanned_plan_ids
                and not str(item.get("reason") or "").endswith("_DEFERRED")
            ]
            capacity_deferred = [
                item for item in skipped
                if str(item.get("reason") or "").endswith("_DEFERRED")
            ]
            unresolved = [
                plan_id for plan_id in plan_ids
                if plan_id not in created_by_plan
                and plan_id not in skipped_by_plan
            ]
            if unresolved:
                raise RuntimeError("动态影子轮询结果没有覆盖全部持久化计划")
            per_strategy_result = {
                "status": str(global_bootstrap.get("status") or "ok"),
                "created": created,
                "skipped": skipped,
                "paper_order_count": len(created),
                "new_paper_order_count": sum(
                    item.get("idempotent_replay") is False
                    for item in created
                ),
                "idempotent_paper_order_count": sum(
                    item.get("idempotent_replay") is True
                    for item in created
                ),
                "scanned_plan_count": sum(
                    plan_id in scanned_plan_ids for plan_id in plan_ids
                ),
                "capacity_opportunity_plan_count": sum(
                    plan_id in scanned_plan_ids for plan_id in plan_ids
                ),
                "risk_or_eligibility_rejected_plan_count": len(
                    scanned_skipped
                ),
                "paper_capacity_consumed_plan_count": len(created),
                "deferred_plan_count": len(capacity_deferred),
                "capacity_deferred_plan_count": len(capacity_deferred),
                "risk_rejection_consumes_paper_order_capacity": False,
                "risk_rejection_counts_as_capacity_underallocation": False,
                "maximum_paper_orders_per_run": int(
                    global_bootstrap.get("maximum_paper_orders_per_run")
                    or 0
                ),
                "strategy_priority_rank": int(
                    priority_rank.get(str(group["strategy_key"])) or 0
                ),
                "global_competition": compact_competition,
                "real_order_count": 0,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
            group_status = group["status"]
            group_status.update({
                "shadow_bootstrap_paper_order_count": len(created),
                "shadow_bootstrap_real_order_count": 0,
                "shadow_bootstrap_result": per_strategy_result,
            })
    return signals, statuses


def build_strategy_center_snapshot(
    trade_date: str = "", limit: int = 200, *, fresh_market: bool = False,
) -> dict[str, Any]:
    target = latest_recommendation_date(trade_date)
    if not target:
        target = normalize_trade_date(trade_date) or date.today().isoformat()
    reference_pool = load_reference_candidate_pool(target)
    market = load_market_snapshot(target, use_cache=not fresh_market)
    configs = load_strategy_configs()
    metrics = load_strategy_metrics(target)
    rows = load_recommendation_rows(target, limit)
    external_market_overlay = _EXTERNAL_MARKET_OVERLAY_CONTEXT.get()
    if external_market_overlay is not None:
        rows = apply_external_market_score_overlay(
            rows,
            external_market_overlay,
        )
    normalized_market = {
        **market,
        "market_state": (market.get("state") or {}).get(
            "key", market.get("market_state")
        ),
    }
    dynamic_signals, dynamic_adapter_statuses = _dynamic_execution_signals(
        trade_date=target,
        recommendation_rows=rows,
        market=normalized_market,
        configs=configs,
        metrics=metrics,
        persist_receipts=fresh_market,
    )
    candidates, conflicts = aggregate_candidates(
        rows,
        normalized_market,
        configs,
        metrics,
        additional_signals=dynamic_signals,
    )
    candidate_source = _candidate_source_contract(
        target,
        rows,
        candidates,
        reference_pool=reference_pool,
        dynamic_adapter_statuses=dynamic_adapter_statuses,
    )
    strategy_counts = _strategy_discovery_counts(
        configs, dynamic_adapter_statuses,
    )
    state = market.get("state") or infer_market_state(market)
    gate_status = "ALLOW_NEW_BUY"
    gate_reason = "市场状态允许生成研究候选，仍需价格和板块条件确认"
    if state.get("key") == "extreme_event":
        gate_status, gate_reason = "BLOCK_NEW_BUY", "极端事件模式自动停止新增买入信号"
    elif state.get("key") in {"high_range", "risk_declining"}:
        gate_status, gate_reason = "REDUCE_NEW_BUY", f"{state.get('name')}模式自动降权新增买入"
    if market.get("source_status") == "missing" or state.get("key") == "unknown":
        gate_status, gate_reason = "DATA_NOT_READY", "市场状态数据缺失，不生成确定性动作"
    if candidate_source.get("status") != "COMPLETED":
        gate_status = "DATA_NOT_READY"
        gate_reason = str(
            candidate_source.get("reason") or "候选源尚未完成"
        )
    if reference_pool:
        reference_gate = reference_pool.get("global_gate") if isinstance(reference_pool.get("global_gate"), dict) else {}
        gate_status = "REVIEW_REQUIRED"
        gate_reason = str(reference_gate.get("reason") or "参考池仅用于研究，需盘前/盘中复核后才可更新信号")
        source_status = "reference_verified" if all(bool(row.get("db_verified")) for row in rows) else "reference_unverified"
        data_date = normalize_trade_date(reference_pool.get("reference_as_of_date")) or target
    else:
        source_status = market.get("source_status", "degraded")
        data_date = target
    reference_meta = {
        "enabled": bool(reference_pool),
        "source": reference_pool.get("source") if reference_pool else None,
        "path": reference_pool.get("_path") if reference_pool else None,
        "reference_as_of_date": normalize_trade_date(reference_pool.get("reference_as_of_date")) if reference_pool else None,
        "recheck_after": reference_pool.get("recheck_after") if reference_pool else None,
        "position_limits": reference_pool.get("position_limits") if reference_pool else None,
        "global_gate": reference_pool.get("global_gate") if reference_pool else None,
    }
    return {
        "status": "ok" if rows or market.get("source_status") != "missing" else "degraded",
        "trade_date": target,
        "data_date": data_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_status": source_status,
        "is_stale": bool(reference_pool) or market.get("source_status") != "fresh",
        "data_sources": [
            "st_recommended_stocks" if not reference_pool else "dated_reference_pool",
            "sm_stock_snapshot/sm_stock_kline" if reference_pool else "existing_market_adapters+sm_stock_kline",
            *(
                ["st_external_market_context"]
                if external_market_overlay is not None
                else []
            ),
        ],
        "external_market_overlay": (
            {
                "snapshot_id": str(
                    external_market_overlay.get("snapshot_id") or ""
                ),
                "captured_at": str(
                    external_market_overlay.get("captured_at") or ""
                )[:19],
                "status": str(
                    external_market_overlay.get("external_market_status")
                    or "UNKNOWN"
                ).upper(),
                "data_quality": str(
                    external_market_overlay.get(
                        "external_market_data_quality"
                    ) or "UNKNOWN"
                ).upper(),
                "score": _num(
                    external_market_overlay.get("external_market_score"),
                    50.0,
                ),
                "score_adjustment": external_market_score_adjustment(
                    external_market_overlay
                ),
            }
            if external_market_overlay is not None
            else None
        ),
        "configuration": {
            "stock_manifest_version": load_stock_manifest()["manifest_version"],
            "stock_manifest_hash": stock_manifest_hash(),
            "market_state_config_version": load_market_state_config()["config_version"],
            "market_state_config_hash": market_state_config_hash(),
            "automatic_order_submission": False,
        },
        "reference_pool": reference_meta,
        "candidate_source": candidate_source,
        "dynamic_adapter_statuses": dynamic_adapter_statuses,
        "long_term_market_trend": compact_market_trend_observation(
            market.get("long_term_trend") or {}
        ),
        "market_state": state,
        "global_gate": {
            "status": gate_status,
            "reason": gate_reason,
            "recheck_after": reference_meta.get("recheck_after"),
            "position_limits": reference_meta.get("position_limits"),
            "invalidation_condition": (reference_meta.get("global_gate") or {}).get("invalidation_condition") if reference_pool else None,
        },
        "strategies": build_strategy_cards(market, candidates, configs, metrics),
        "candidates": candidates,
        "conflicts": conflicts,
        "summary": {
            **strategy_counts,
            "candidate_count": len(candidates),
            "conflict_count": len(conflicts),
            "buy_count": sum(1 for item in candidates if item.get("final_direction") == "BUY"),
            "blocked_count": sum(1 for item in candidates if item.get("final_status") in {"BLOCKED", "SELL_ALERT"}),
        },
        "disclaimer": "仅用于研究候选和风险提示；未经明确确认不会执行任何交易。",
    }


def persist_strategy_center_snapshot(
    snapshot: dict[str, Any], *, ensure_tables: bool = True,
) -> dict[str, Any]:
    if ensure_tables:
        ensure_strategy_center_tables()
    run_uid = uuid.uuid4().hex
    state = snapshot.get("market_state") or {}
    candidates = snapshot.get("candidates") or []
    conflicts = snapshot.get("conflicts") or []
    signal_count = sum(len(item.get("strategy_signals") or []) for item in candidates)
    _db_write("""
        INSERT INTO st_strategy_center_run
        (run_uid, trade_date, market_state, state_confidence, source_status, model_version, status, signal_count, candidate_count, conflict_count)
        VALUES (:run_uid, :trade_date, :market_state, :state_confidence, :source_status, :model_version, 'running', :signal_count, :candidate_count, :conflict_count)
    """, {
        "run_uid": run_uid,
        "trade_date": snapshot.get("trade_date"),
        "market_state": state.get("key") or "unknown",
        "state_confidence": state.get("confidence"),
        "source_status": snapshot.get("source_status") or "degraded",
        "model_version": str(load_stock_manifest()["manifest_version"])[:40],
        "signal_count": signal_count,
        "candidate_count": len(candidates),
        "conflict_count": len(conflicts),
    })
    for candidate in candidates:
        for signal in candidate.get("strategy_signals") or []:
            _db_write("""
                INSERT INTO st_strategy_center_signal
                (run_uid, trade_date, stock_code, stock_name, strategy_key, market_state, signal_direction, signal_status,
                 raw_score, effective_score, model_confidence, effective_weight, risk_level, gate_status, gate_reason,
                 entry_low, entry_high, stop_loss, take_profit_1, take_profit_2, no_chase_price, risk_reward_ratio,
                 today_signal, trigger_conditions_json, evidence_chain_json, data_snapshot_json, model_version)
                VALUES (:run_uid, :trade_date, :stock_code, :stock_name, :strategy_key, :market_state, :signal_direction, :signal_status,
                        :raw_score, :effective_score, :model_confidence, :effective_weight, :risk_level, :gate_status, :gate_reason,
                        :entry_low, :entry_high, :stop_loss, :take_profit_1, :take_profit_2, :no_chase_price, :risk_reward_ratio,
                        :today_signal, :trigger_conditions_json, :evidence_chain_json, :data_snapshot_json, :model_version)
            """, {
                "run_uid": run_uid, "trade_date": snapshot.get("trade_date"), "stock_code": signal.get("stock_code"), "stock_name": signal.get("stock_name"),
                "strategy_key": signal.get("strategy_key"), "market_state": signal.get("market_state"), "signal_direction": signal.get("signal_direction"), "signal_status": signal.get("signal_status"),
                "raw_score": signal.get("raw_score"), "effective_score": signal.get("effective_score"), "model_confidence": signal.get("model_confidence"), "effective_weight": signal.get("effective_weight"),
                "risk_level": signal.get("risk_level"), "gate_status": signal.get("gate_status"), "gate_reason": signal.get("gate_reason"), "entry_low": signal.get("entry_low"), "entry_high": signal.get("entry_high"),
                "stop_loss": signal.get("stop_loss"), "take_profit_1": signal.get("take_profit_1"), "take_profit_2": signal.get("take_profit_2"), "no_chase_price": signal.get("no_chase_price"),
                "risk_reward_ratio": signal.get("risk_reward_ratio"), "today_signal": signal.get("today_signal"), "trigger_conditions_json": _json_text(signal.get("trigger_conditions"), []),
                "evidence_chain_json": _json_text(signal.get("evidence_chain"), []), "data_snapshot_json": _json_text({"data_date": signal.get("data_date"), "adapter_mode": signal.get("adapter_mode"), "industry_name": signal.get("industry_name") or signal.get("theme_code") or "", "theme_code": signal.get("theme_code") or "", "strategy_version": signal.get("strategy_version") or "", "strategy_version_hash": signal.get("strategy_version_hash") or "", "execution_binding_hash": signal.get("execution_binding_hash") or "", "adapter_artifact_sha256": signal.get("adapter_artifact_sha256") or "", "cost_model_hash": signal.get("cost_model_hash") or "", "candidate_run_uid": signal.get("candidate_run_uid") or "", "candidate_receipt_hash": signal.get("candidate_receipt_hash") or "", "candidate_input_hash": signal.get("candidate_input_hash") or "", "candidate_output_hash": signal.get("candidate_output_hash") or ""}, {}),
                "model_version": signal.get("model_version") or str(load_stock_manifest()["manifest_version"])[:40],
            })
    for conflict in conflicts:
        decision = conflict.get("decision") or {}
        _db_write("""
            INSERT INTO st_strategy_center_conflict
            (run_uid, trade_date, stock_code, stock_name, market_state, final_direction, final_status, buy_score, sell_score, hold_score, decision_json)
            VALUES (:run_uid, :trade_date, :stock_code, :stock_name, :market_state, :final_direction, :final_status, :buy_score, :sell_score, :hold_score, :decision_json)
        """, {
            "run_uid": run_uid, "trade_date": snapshot.get("trade_date"), "stock_code": conflict.get("stock_code"), "stock_name": conflict.get("stock_name"),
            "market_state": conflict.get("market_state"), "final_direction": decision.get("final_direction"), "final_status": decision.get("final_status"),
            "buy_score": decision.get("buy_score"), "sell_score": decision.get("sell_score"), "hold_score": decision.get("hold_score"), "decision_json": _json_text(conflict, {}),
        })
    market_input = {
        key: (snapshot.get("market_state") or {}).get("input", {}).get(key)
        for key in load_market_state_config()["required_inputs"]
    }
    if not any(value is not None for value in market_input.values()):
        # Older adapters put raw inputs directly in the market-state payload.
        market_input = {
            key: (snapshot.get("market_state") or {}).get(key)
            for key in load_market_state_config()["required_inputs"]
        }
    state_payload = {
        "market_input": market_input,
        "source_status": snapshot.get("source_status"),
        "trade_date": snapshot.get("trade_date"),
    }
    input_hash = hashlib.sha256(
        json.dumps(
            state_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    stored_evidence = list(state.get("evidence") or [])
    trend_observation = snapshot.get("long_term_market_trend") or {}
    if trend_observation.get("indices"):
        stored_evidence.append(trend_observation)
    _db_write(
        """
        INSERT INTO st_market_state_daily
        (trade_date, run_uid, config_version, config_hash, input_hash,
         candidate_state, final_state, candidate_streak, state_days,
         cooldown_remaining, source_status, input_json, evidence_json)
        VALUES
        (:trade_date, :run_uid, :config_version, :config_hash, :input_hash,
         :candidate_state, :final_state, :candidate_streak, :state_days,
         :cooldown_remaining, :source_status, :input_json, :evidence_json)
        ON DUPLICATE KEY UPDATE id = id
        """,
        {
            "trade_date": snapshot.get("trade_date"),
            "run_uid": run_uid,
            "config_version": state.get("config_version")
            or load_market_state_config()["config_version"],
            "config_hash": state.get("config_hash") or market_state_config_hash(),
            "input_hash": input_hash,
            "candidate_state": state.get("candidate_state") or state.get("key") or "unknown",
            "final_state": state.get("final_state") or state.get("key") or "unknown",
            "candidate_streak": int(state.get("candidate_streak") or 1),
            "state_days": int(state.get("state_days") or 1),
            "cooldown_remaining": int(state.get("cooldown_remaining") or 0),
            "source_status": snapshot.get("source_status") or "degraded",
            "input_json": json.dumps(state_payload, ensure_ascii=False, default=str),
            "evidence_json": json.dumps(stored_evidence, ensure_ascii=False),
        },
    )
    _db_write("UPDATE st_strategy_center_run SET status = 'done', finished_at = NOW() WHERE run_uid = :run_uid", {"run_uid": run_uid})
    return {
        **snapshot,
        "run_uid": run_uid,
        "execution_status": "done",
    }


def set_strategy_enabled(strategy_key: str, enabled: bool, reason: str = "", operator: str = "api") -> dict[str, Any]:
    if strategy_key not in _STRATEGY_BY_KEY:
        raise ValueError(f"unknown strategy_key: {strategy_key}")
    ensure_strategy_center_tables()
    configs = load_strategy_configs()
    old = configs.get(strategy_key) or {}
    _db_write("""
        UPDATE st_strategy_center_config
        SET enabled = :enabled, version = version + 1, updated_by = :operator, updated_at = NOW()
        WHERE strategy_key = :strategy_key
    """, {"enabled": 1 if enabled else 0, "operator": operator[:80], "strategy_key": strategy_key})
    _db_write("""
        INSERT INTO st_strategy_center_audit (strategy_key, action, old_value, new_value, reason, operator)
        VALUES (:strategy_key, 'toggle', :old_value, :new_value, :reason, :operator)
    """, {
        "strategy_key": strategy_key, "old_value": json.dumps({"enabled": old.get("enabled", True)}, ensure_ascii=False),
        "new_value": json.dumps({"enabled": bool(enabled)}, ensure_ascii=False), "reason": str(reason or "")[:500], "operator": operator[:80],
    })
    return {"strategy_key": strategy_key, "enabled": bool(enabled), "reason": reason, "updated_by": operator}
