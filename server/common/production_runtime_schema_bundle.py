# -*- coding: utf-8 -*-
"""One fenced release bundle for every non-core runtime schema contract.

The strategy-governance migrator owns this entrypoint.  API, scheduler and
collector processes use only :func:`validate_runtime_schema_bundle`.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from typing import Any

from biz.intraday_alert.schema import (
    privileged_migrate_intraday_alert_tables,
    validate_intraday_alert_runtime,
)
from biz.market_context.external_market import (
    privileged_migrate_external_market_tables,
    validate_external_market_runtime,
)
from biz.market_radar.core import privileged_migrate_radar_tables, validate_radar_runtime
from biz.premarket.theme_forecast import (
    privileged_migrate_premarket_theme_tables,
    validate_premarket_theme_runtime,
)
from biz.review.quant_digest import (
    privileged_migrate_quant_digest_tables,
    validate_quant_digest_runtime,
)
from biz.sentiment.sync_sentiment import validate_sentiment_runtime_schema
from biz.stock_info.sync_stock_info import validate_stock_info_runtime_schema
from biz.stock_market.realtime_quotes import (
    privileged_migrate_rt_snapshot_table,
    validate_rt_snapshot_runtime,
)
from biz.stock_market.sync_stock_market import (
    privileged_migrate_sm_stock_kline_short_name,
    validate_stock_market_runtime_schema,
)
from integrations.qmt.audit import (
    plan_audit_schema_recovery,
    privileged_migrate_audit_schema,
    validate_audit_schema,
)
from integrations.qmt.catalog import (
    plan_catalog_schema_recovery,
    privileged_migrate_catalog_schema,
    privileged_seed_catalog_registry,
    validate_catalog_registry_seed,
    validate_catalog_schema,
)
from integrations.wecom.delivery import (
    privileged_migrate_delivery_receipt_table,
    validate_delivery_receipt_runtime,
)
from server.ai_bridge.schema import (
    plan_ai_bridge_recovery,
    privileged_migrate_ai_bridge_schema,
    validate_ai_bridge_runtime_schema,
)
from server.auth.schema import privileged_migrate_auth_schema, validate_auth_runtime_schema
from server.common.analysis_output_schema import (
    migrate_analysis_output_schema,
    plan_analysis_output_recovery,
    validate_analysis_output_schema,
)
from server.common.auxiliary_runtime_schema import (
    privileged_migrate_auxiliary_runtime_schema,
    validate_auxiliary_runtime_schema,
)
from server.common.commentary_profile_schema import (
    privileged_migrate_commentary_profile_table,
    validate_commentary_profile_runtime,
)
from server.common.hot_rank_schema import (
    privileged_migrate_hot_rank_schema,
    validate_hot_rank_runtime_schema,
)
from server.common.jq_minute_schema import (
    privileged_migrate_jq_minute_tables,
    validate_jq_minute_runtime,
)
from server.common.portfolio_schema import (
    privileged_migrate_portfolio_schema,
    validate_portfolio_runtime_schema,
)
from server.common.recommended_run_history_schema import (
    migrate_recommended_run_history,
    plan_recommended_run_history_recovery,
    validate_recommended_run_history_schema,
)
from server.common.scheduler_tasks import (
    privileged_migrate_scheduler_task_columns,
    validate_scheduler_task_runtime_schema,
)
from server.common.scheduler_task_history_schema import (
    migrate_scheduler_task_history,
    validate_scheduler_task_history_schema,
)
from server.common.screener_schema import (
    privileged_migrate_screener_tables,
    validate_screener_runtime,
)
from server.common.sim_trade_schema import (
    plan_sim_trade_legacy_recovery,
    privileged_migrate_sim_trade_schema,
    validate_sim_trade_runtime_schema,
)
from server.common.market_field_capture_schema import (
    privileged_migrate_market_field_capture_schema,
    validate_market_field_capture_runtime,
)
from server.common.qmt_trade_calendar import (
    privileged_migrate_trade_calendar_schema,
    validate_trade_calendar_runtime_schema,
)
from server.common.qmt_stock_catalog import (
    privileged_migrate_stock_catalog_schema,
    validate_stock_catalog_runtime_schema,
)
from server.common.versioned_strategy_config import (
    privileged_migrate_versioned_strategy_tables,
    privileged_seed_versioned_strategy_configs,
    validate_versioned_strategy_runtime,
)
from server.engine.strategy_center import (
    privileged_migrate_strategy_center_tables,
    privileged_seed_strategy_center_configs,
    validate_strategy_center_runtime,
)


SchemaCallable = Callable[[Any], Any]
BUNDLE_CONTRACT_SCHEMA = "probiga.production-runtime-schema-bundle.v1"

_BROKER_OWNED_TRIGGER_MIGRATION_NAMES = (
    "qmt_stock_catalog_truth",
    "qmt_trade_calendar",
    "market_field_capture",
    "auxiliary_runtime",
)


def _migrate_stock_catalog_tables(engine) -> Any:
    return privileged_migrate_stock_catalog_schema(
        engine,
        install_triggers=False,
    )


def _migrate_trade_calendar_tables(engine) -> Any:
    return privileged_migrate_trade_calendar_schema(
        engine,
        install_triggers=False,
    )


def _migrate_market_field_capture_tables(engine) -> Any:
    return privileged_migrate_market_field_capture_schema(
        engine,
        install_triggers=False,
    )


def _migrate_auxiliary_runtime_tables(engine) -> Any:
    return privileged_migrate_auxiliary_runtime_schema(
        engine,
        install_triggers=False,
    )

_MIGRATIONS: tuple[tuple[str, SchemaCallable], ...] = (
    ("scheduler_tasks", privileged_migrate_scheduler_task_columns),
    ("scheduler_task_history", migrate_scheduler_task_history),
    ("auth", privileged_migrate_auth_schema),
    ("ai_bridge", privileged_migrate_ai_bridge_schema),
    ("analysis_output", migrate_analysis_output_schema),
    ("qmt_stock_catalog_truth", _migrate_stock_catalog_tables),
    ("qmt_trade_calendar", _migrate_trade_calendar_tables),
    ("market_field_capture", _migrate_market_field_capture_tables),
    ("recommended_run_history", migrate_recommended_run_history),
    ("versioned_strategy", privileged_migrate_versioned_strategy_tables),
    ("strategy_center", privileged_migrate_strategy_center_tables),
    ("sim_trade", privileged_migrate_sim_trade_schema),
    ("portfolio", privileged_migrate_portfolio_schema),
    ("commentary_profile", privileged_migrate_commentary_profile_table),
    ("screener", privileged_migrate_screener_tables),
    ("jq_minute", privileged_migrate_jq_minute_tables),
    ("market_radar", privileged_migrate_radar_tables),
    ("external_market", privileged_migrate_external_market_tables),
    ("premarket_theme", privileged_migrate_premarket_theme_tables),
    ("intraday_alert", privileged_migrate_intraday_alert_tables),
    ("quant_digest", privileged_migrate_quant_digest_tables),
    ("wecom_delivery", privileged_migrate_delivery_receipt_table),
    ("realtime_quote_snapshot", privileged_migrate_rt_snapshot_table),
    ("stock_kline_short_name", privileged_migrate_sm_stock_kline_short_name),
    ("hot_rank", privileged_migrate_hot_rank_schema),
    ("auxiliary_runtime", _migrate_auxiliary_runtime_tables),
    ("qmt_catalog", privileged_migrate_catalog_schema),
    ("qmt_audit", privileged_migrate_audit_schema),
)

_SEEDS: tuple[tuple[str, SchemaCallable], ...] = (
    ("versioned_strategy", privileged_seed_versioned_strategy_configs),
    ("strategy_center", privileged_seed_strategy_center_configs),
    ("qmt_catalog", privileged_seed_catalog_registry),
)

_VALIDATORS: tuple[tuple[str, SchemaCallable], ...] = (
    ("scheduler_tasks", validate_scheduler_task_runtime_schema),
    ("scheduler_task_history", validate_scheduler_task_history_schema),
    ("auth", validate_auth_runtime_schema),
    ("ai_bridge", validate_ai_bridge_runtime_schema),
    ("analysis_output", validate_analysis_output_schema),
    ("qmt_stock_catalog_truth", validate_stock_catalog_runtime_schema),
    ("qmt_trade_calendar", validate_trade_calendar_runtime_schema),
    ("market_field_capture", validate_market_field_capture_runtime),
    ("recommended_run_history", validate_recommended_run_history_schema),
    ("versioned_strategy", validate_versioned_strategy_runtime),
    ("strategy_center", validate_strategy_center_runtime),
    ("sim_trade", validate_sim_trade_runtime_schema),
    ("portfolio", validate_portfolio_runtime_schema),
    ("commentary_profile", validate_commentary_profile_runtime),
    ("screener", validate_screener_runtime),
    ("jq_minute", validate_jq_minute_runtime),
    ("market_radar", validate_radar_runtime),
    ("external_market", validate_external_market_runtime),
    ("premarket_theme", validate_premarket_theme_runtime),
    ("intraday_alert", validate_intraday_alert_runtime),
    ("quant_digest", validate_quant_digest_runtime),
    ("wecom_delivery", validate_delivery_receipt_runtime),
    ("realtime_quote_snapshot", validate_rt_snapshot_runtime),
    ("stock_market", validate_stock_market_runtime_schema),
    ("stock_info", validate_stock_info_runtime_schema),
    ("sentiment", validate_sentiment_runtime_schema),
    ("hot_rank", validate_hot_rank_runtime_schema),
    ("auxiliary_runtime", validate_auxiliary_runtime_schema),
    ("qmt_catalog", validate_catalog_schema),
    ("qmt_catalog_seed", validate_catalog_registry_seed),
    ("qmt_audit", validate_audit_schema),
)

_RECOVERY_PLANNERS: tuple[tuple[str, SchemaCallable], ...] = (
    ("ai_bridge", plan_ai_bridge_recovery),
    ("analysis_output", plan_analysis_output_recovery),
    ("recommended_run_history", plan_recommended_run_history_recovery),
    ("sim_trade", plan_sim_trade_legacy_recovery),
    ("qmt_catalog", plan_catalog_schema_recovery),
    ("qmt_audit", plan_audit_schema_recovery),
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _contract_metadata() -> dict[str, Any]:
    payload = {
        "schema": BUNDLE_CONTRACT_SCHEMA,
        "migration_names": [name for name, _ in _MIGRATIONS],
        "seed_names": [name for name, _ in _SEEDS],
        "validator_names": [name for name, _ in _VALIDATORS],
        "recovery_planner_names": [name for name, _ in _RECOVERY_PLANNERS],
        "trigger_installation_policy": "FROZEN_RELEASE_BROKER_ONLY",
        "broker_owned_trigger_migration_names": list(
            _BROKER_OWNED_TRIGGER_MIGRATION_NAMES
        ),
    }
    payload["recovery_planner_count"] = len(payload["recovery_planner_names"])
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        **payload,
        "migration_count": len(payload["migration_names"]),
        "seed_count": len(payload["seed_names"]),
        "validator_count": len(payload["validator_names"]),
        "contract_hash": hashlib.sha256(encoded).hexdigest(),
    }


def _result(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {"completed": True} if value is None else {"result": value}


def _run(entries: tuple[tuple[str, SchemaCallable], ...], engine) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, function in entries:
        if name in result:
            raise RuntimeError(f"duplicate production schema bundle key: {name}")
        result[name] = _result(function(engine))
    return result


def _normalize_recovery_plan(name: str, value: Any) -> dict[str, Any]:
    plan = _result(value)
    atomic_hash = plan.get("plan_sha256")
    bundle_hash = plan.get("recovery_bundle_sha256")
    declared_atomic_hash = plan.get("atomic_plan_sha256")
    declared_hashes = [
        candidate
        for candidate in (atomic_hash, bundle_hash, declared_atomic_hash)
        if candidate is not None
    ]
    canonical_hash = bundle_hash if bundle_hash is not None else atomic_hash
    ready = plan.get("ready_for_privileged_apply")
    if name == "sim_trade" and ready is None:
        ready = plan.get("safe_automatic_rewrite")
    declared_read_only = plan.get("read_only")
    if (
        not declared_hashes
        or any(
            not isinstance(candidate, str)
            or _SHA256.fullmatch(candidate) is None
            for candidate in declared_hashes
        )
        or (
            declared_atomic_hash is not None
            and declared_atomic_hash != atomic_hash
        )
        or type(ready) is not bool
        or (declared_read_only is not None and declared_read_only is not True)
    ):
        raise RuntimeError("production schema recovery plan contract differs")
    normalized = {
        **plan,
        "plan_sha256": canonical_hash,
        "ready_for_privileged_apply": ready,
        "read_only": True,
        "status": "PLANNED",
    }
    if bundle_hash is not None and atomic_hash is not None:
        normalized["atomic_plan_sha256"] = atomic_hash
    return normalized


def _collect_recovery_plans(engine) -> dict[str, Any]:
    plans: dict[str, Any] = {}
    for name, function in _RECOVERY_PLANNERS:
        if name in plans:
            raise RuntimeError(f"duplicate production recovery planner key: {name}")
        try:
            plans[name] = _normalize_recovery_plan(name, function(engine))
        except Exception as exc:  # fail closed without exposing database details
            plans[name] = {
                "status": "PLAN_UNAVAILABLE",
                "error_type": type(exc).__name__,
                "plan_sha256": None,
                "ready_for_privileged_apply": False,
                "read_only": True,
            }
    ready = (
        len(plans) == len(_RECOVERY_PLANNERS)
        and all(
            item.get("status") == "PLANNED"
            and item.get("read_only") is True
            and item.get("ready_for_privileged_apply") is True
            and isinstance(item.get("plan_sha256"), str)
            and _SHA256.fullmatch(item["plan_sha256"]) is not None
            for item in plans.values()
        )
    )
    return {
        "recovery_plans": plans,
        "recovery_plan_count": len(plans),
        "recovery_ready_for_privileged_apply": ready,
    }


def privileged_migrate_runtime_schema_bundle(
    engine,
    *,
    defer_trigger_validation: bool = False,
) -> dict[str, Any]:
    """Run all non-core DDL and configuration seeds inside the release fence."""

    if type(defer_trigger_validation) is not bool:
        raise TypeError("defer_trigger_validation must be bool")

    migrations = _run(_MIGRATIONS, engine)
    seeds = _run(_SEEDS, engine)
    validation = (
        {
            **_contract_metadata(),
            "required_surface_verified": False,
            "trigger_validation_deferred": True,
            "deferred_until": "FROZEN_RELEASE_BROKER_COMPLETED",
            "read_only": True,
        }
        if defer_trigger_validation
        else validate_runtime_schema_bundle(engine)
    )
    recovery = _collect_recovery_plans(engine)
    return {
        **_contract_metadata(),
        "migrations": migrations,
        "seeds": seeds,
        "runtime_validation": validation,
        "trigger_validation_deferred": defer_trigger_validation,
        **recovery,
        "runtime_ddl_required": False,
        "privileged_migration": True,
    }


def validate_runtime_schema_bundle(engine) -> dict[str, Any]:
    """Read-only validation for API/scheduler/collector startup and release."""

    contracts = _run(_VALIDATORS, engine)
    return {
        **_contract_metadata(),
        "contracts": contracts,
        "contract_count": len(contracts),
        "required_surface_verified": True,
        "read_only": True,
    }


def preflight_runtime_schema_bundle(engine) -> dict[str, Any]:
    """Read-only preflight that reports which bundle members need migration."""

    contracts: dict[str, Any] = {}
    for name, function in _VALIDATORS:
        try:
            contracts[name] = {**_result(function(engine)), "status": "READY"}
        except Exception as exc:  # schema drift is expected during preflight
            contracts[name] = {
                "status": "MIGRATION_REQUIRED",
                "error_type": type(exc).__name__,
                "read_only": True,
            }
    recovery = _collect_recovery_plans(engine)
    return {
        **_contract_metadata(),
        "contracts": contracts,
        "contract_count": len(contracts),
        "migration_required": (
            any(item["status"] != "READY" for item in contracts.values())
            or not recovery["recovery_ready_for_privileged_apply"]
        ),
        **recovery,
        "read_only": True,
    }


__all__ = [
    "BUNDLE_CONTRACT_SCHEMA",
    "preflight_runtime_schema_bundle",
    "privileged_migrate_runtime_schema_bundle",
    "validate_runtime_schema_bundle",
]
