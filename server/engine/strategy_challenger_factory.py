# -*- coding: utf-8 -*-
"""Append-only champion/challenger strategy improvement workflow.

Challengers never overwrite a production strategy.  A proposal is frozen in
the governance audit ledger, then independently reviewed by replaying its
purged walk-forward trades, out-of-sample segments, and cost-stress evidence.
Only that server-recomputable contract can promote a brand-new SHADOW version;
daily governance remains the only mechanism that can award simulated capital.
"""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.canonical_json import validate_canonical_json
from server.common.versioned_strategy_config import (
    load_market_state_config,
    market_state_config_hash,
)
from server.engine.strategy_execution_adapters import (
    normalize_execution_binding,
)
from server.engine.strategy_governance import (
    EXTERNAL_SELECTION_SOURCE_AUTHORITY,
    EXTERNAL_SELECTION_SOURCE_AUTHORITY_LABEL,
    MARKET_ROUTER_POLICY_VERSION,
    _append_audit_connection,
    _digest,
    _holding_horizon_from_parameters,
    _json,
    _label_horizon_from_parameters,
    _strategy_version_digest,
    _validate_metric_artifact,
    _validated_metric_evidence,
    _validated_market_regime_multipliers,
    ensure_and_seed_governance,
    load_registry,
    register_strategy,
    validate_strategy_key,
)


CHALLENGER_POLICY: dict[str, Any] = {
    "minimum_completed_trades": 80,
    "minimum_coverage_days": 60,
    "minimum_walk_forward_segments": 5,
    "minimum_positive_segments": 4,
    "minimum_profit_factor": 1.30,
    "minimum_payoff_ratio": 1.10,
    "minimum_net_expectancy_pct": 0.0,
    "minimum_cost_stress_expectancy_pct": 0.0,
    "required_window_days": 120,
    "required_protocols": [
        "PURGED_WALK_FORWARD_V2",
        "COMBINATORIAL_PURGED_WALK_FORWARD_V2",
    ],
    "automatic_real_order_submission": False,
    "real_order_authority": False,
}


class StrategyAlreadyRegisteredError(ValueError):
    """The public new-key registrar cannot create a version for an old key."""


def _canonical_payload(raw: Any) -> dict[str, Any]:
    value = _json(raw, None)
    if not isinstance(value, dict):
        raise RuntimeError("挑战者审计载荷无效")
    return value


def _verified_audit_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = _canonical_payload(row.get("payload_json"))
    if _digest(payload) != str(row.get("audit_hash") or ""):
        raise RuntimeError("挑战者审计哈希漂移")
    return payload


def _proposal_events(
    connection, strategy_key: str = "",
) -> list[dict[str, Any]]:
    """Load one or every challenger audit chain with two bounded queries."""

    key_clause = " AND entity_key=:strategy_key" if strategy_key else ""
    params: dict[str, Any] = {}
    if strategy_key:
        params["strategy_key"] = strategy_key
    action_clause = (
        " AND action IN ('REGISTER_CHALLENGER',"
        "'SUBMIT_CHALLENGER_EVIDENCE','REVIEW_CHALLENGER',"
        "'PROMOTE_CHALLENGER')"
    )
    where_clause = " WHERE entity_type='STRATEGY'" + key_clause + action_clause
    row_count = int(connection.execute(
        text("SELECT COUNT(*) FROM st_strategy_governance_audit" + where_clause),
        params,
    ).scalar() or 0)
    if row_count < 0:
        raise RuntimeError("挑战者审计权威计数无效")
    rows = connection.execute(
        text(
            "SELECT audit_id, entity_key, action, operator_name, payload_json, "
            "audit_hash, created_at FROM st_strategy_governance_audit"
            + where_clause
            + " ORDER BY entity_key, created_at, audit_id LIMIT :event_limit"
        ),
        {**params, "event_limit": row_count + 1},
    ).mappings().all()
    if len(rows) != row_count:
        raise RuntimeError("挑战者审计权威计数发生漂移，拒绝截断")
    return [dict(row) for row in rows]


def _challengers_from_grouped_events(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        strategy_key = validate_strategy_key(str(row.get("entity_key") or ""))
        grouped.setdefault(strategy_key, []).append(row)
    challengers: list[dict[str, Any]] = []
    for strategy_key in sorted(grouped):
        replayed = _challengers_from_events(grouped[strategy_key])
        if any(
            str(item.get("strategy_key") or "") != strategy_key
            for item in replayed
        ):
            raise RuntimeError("挑战者审计对象与策略代码不一致")
        challengers.extend(replayed)
    return challengers


def _lock_challenger_namespace(connection) -> None:
    """Serialize registry admission and global challenger evidence claims."""

    locked = connection.execute(text(
        "SELECT migration_key FROM "
        "st_strategy_governance_schema_migration "
        "ORDER BY migration_key LIMIT 1 FOR UPDATE"
    )).first()
    if locked is None:
        raise RuntimeError("策略治理全局注册锁未初始化")


def register_new_strategy(
    payload: dict[str, Any], *, operator: str,
) -> dict[str, Any]:
    """Allow /registry to create a key once, never to bypass challenger flow."""

    ensure_and_seed_governance()
    key = validate_strategy_key(str(payload.get("strategy_key") or ""))
    with get_engine().begin() as connection:
        _lock_challenger_namespace(connection)
        existing = connection.execute(text(
            "SELECT current_version FROM st_strategy_registry "
            "WHERE strategy_key=:strategy_key FOR UPDATE"
        ), {"strategy_key": key}).mappings().first()
        if existing is not None:
            raise StrategyAlreadyRegisteredError(
                "该策略代码已存在；所有新版本必须先登记挑战者、"
                "提交可重算证据并经独立复核后晋级"
            )
        # The global database row lock remains held while the existing
        # append-only registrar commits, so concurrent /registry requests
        # cannot both observe an absent key.
        strategy = register_strategy(
            dict(payload), operator=operator,
            _global_inventory_lock_held=True,
        )
    return strategy


def _normalized_registration_payload(
    payload: dict[str, Any], *, operator: str,
) -> tuple[dict[str, Any], str]:
    key = validate_strategy_key(str(payload.get("strategy_key") or ""))
    name = str(payload.get("strategy_name") or "").strip()
    version = str(payload.get("version") or "").strip()
    if not name or len(name) > 120:
        raise ValueError("策略名称不能为空且不能超过120字")
    if not version or len(version) > 160:
        raise ValueError("挑战者版本不能为空且不能超过160字")
    evaluator_type = str(
        payload.get("evaluator_type") or "external_evidence"
    )[:40]
    canonical_registration = validate_canonical_json({
        "evaluator_config": payload.get("evaluator_config") or {},
        "parameters": payload.get("parameters") or {},
        "execution_binding": payload.get("execution_binding"),
    }, label="挑战者评估配置与参数")
    evaluator_config = canonical_registration["evaluator_config"]
    parameters = canonical_registration["parameters"]
    if not isinstance(evaluator_config, dict) or not isinstance(parameters, dict):
        raise ValueError("挑战者评估配置和参数必须是对象")
    evaluator_config = dict(evaluator_config)
    parameters = dict(parameters)
    parameters["max_holding_days"] = _holding_horizon_from_parameters(
        parameters
    )
    parameters["label_horizon_days"] = _label_horizon_from_parameters(
        parameters
    )
    binding = canonical_registration["execution_binding"]
    nested = evaluator_config.get("execution_adapter")
    if binding is not None and nested is not None and binding != nested:
        raise ValueError("挑战者执行适配器绑定不一致")
    if binding is not None or nested is not None:
        evaluator_config["execution_adapter"] = normalize_execution_binding(
            binding if binding is not None else nested,
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
    finalized_registration = validate_canonical_json({
        "evaluator_config": evaluator_config,
        "parameters": parameters,
    }, label="规范化挑战者评估配置与参数")
    evaluator_config = finalized_registration["evaluator_config"]
    parameters = finalized_registration["parameters"]
    normalized = {
        "strategy_key": key,
        "strategy_name": name,
        "version": version,
        "category": str(payload.get("category") or "未分类")[:80],
        "family_key": str(payload.get("family_key") or key)[:80],
        "description": str(payload.get("description") or "")[:1000],
        "evaluator_type": evaluator_type,
        "evaluator_config": evaluator_config,
        "parameters": parameters,
        "reason": str(payload.get("reason") or "挑战者晋级")[:500],
        "owner_name": str(payload.get("owner_name") or operator or "api")[:80],
    }
    version_hash = _strategy_version_digest(
        strategy_key=key,
        version=version,
        evaluator_type=evaluator_type,
        evaluator_config=evaluator_config,
        parameters=parameters,
        source_kind="runtime_registry",
    )
    return normalized, version_hash


def _challengers_from_events(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Replay the append-only challenger state machine fail closed."""

    def hash64(value: Any) -> bool:
        raw = str(value or "")
        return len(raw) == 64 and all(c in "0123456789abcdef" for c in raw)

    proposals: dict[str, dict[str, Any]] = {}
    for row in events:
        payload = _verified_audit_payload(row)
        after = payload.get("after") if isinstance(payload.get("after"), dict) else {}
        evidence = (
            payload.get("evidence")
            if isinstance(payload.get("evidence"), dict) else {}
        )
        challenger_id = str(
            after.get("challenger_id")
            or evidence.get("challenger_id")
            or ""
        )
        if not challenger_id:
            raise RuntimeError("挑战者审计缺少不可变身份")
        action = str(row.get("action") or "")
        if action == "REGISTER_CHALLENGER":
            registration = after.get("registration_payload")
            expected_proposal_hash = _digest({
                "strategy_key": after.get("strategy_key"),
                "parent_version": after.get("parent_version"),
                "proposed_version_hash": after.get("proposed_version_hash"),
                "registration_payload": registration,
            })
            if (
                challenger_id in proposals
                or not isinstance(registration, dict)
                or after.get("challenger_id") != challenger_id
                or after.get("strategy_key") != registration.get("strategy_key")
                or after.get("proposed_version") != registration.get("version")
                or not hash64(after.get("proposed_version_hash"))
                or after.get("proposal_hash") != expected_proposal_hash
                or after.get("automatic_real_order_submission") is not False
                or after.get("real_order_authority") is not False
            ):
                raise RuntimeError("挑战者登记审计合同无效")
            proposals[challenger_id] = {
                **after,
                "status": "VALIDATING",
                "status_label": "等待提交可重算证据",
                "submitted_by": str(payload.get("operator") or ""),
                "submitted_at": str(row.get("created_at") or ""),
                "evidence_submission": None,
                "_frozen_evidence_submission": None,
                "latest_validation": None,
                "promoted_version_hash": "",
            }
            continue
        selected = proposals.get(challenger_id)
        if selected is None:
            raise RuntimeError("挑战者事件引用未知或乱序身份")
        if action == "SUBMIT_CHALLENGER_EVIDENCE":
            frozen_payload = {
                key: value for key, value in evidence.items()
                if key != "evidence_submission_hash"
            }
            submission_hash = str(
                evidence.get("evidence_submission_hash") or ""
            )
            if (
                selected.get("status") != "VALIDATING"
                or evidence.get("schema")
                != "probiga.strategy-challenger-evidence-submission.v1"
                or evidence.get("challenger_id") != challenger_id
                or evidence.get("proposal_hash") != selected.get("proposal_hash")
                or evidence.get("proposed_version_hash")
                != selected.get("proposed_version_hash")
                or evidence.get("proposal_submitted_at")
                != selected.get("submitted_at")
                or evidence.get("submitted_by")
                != str(payload.get("operator") or "")
                or not hash64(evidence.get("artifact_hash"))
                or not hash64(evidence.get("source_dataset_hash"))
                or not isinstance(evidence.get("metrics"), dict)
                or not isinstance(evidence.get("artifact_manifest"), dict)
                or evidence.get("automatic_real_order_submission") is not False
                or evidence.get("real_order_authority") is not False
                or not hash64(submission_hash)
                or _digest(frozen_payload) != submission_hash
                or after != {
                    "challenger_id": challenger_id,
                    "status": "REVIEW_PENDING",
                    "evidence_submission_hash": submission_hash,
                    "artifact_hash": evidence.get("artifact_hash"),
                    "source_dataset_hash": evidence.get("source_dataset_hash"),
                }
            ):
                raise RuntimeError("挑战者冻结证据审计合同无效")
            selected["status"] = "REVIEW_PENDING"
            selected["status_label"] = "等待独立复核"
            selected["evidence_submitted_by"] = str(
                payload.get("operator") or ""
            )
            selected["evidence_submitted_at"] = str(
                row.get("created_at") or ""
            )
            selected["evidence_submission"] = {
                key: evidence.get(key) for key in (
                    "schema", "as_of_date", "window_days",
                    "evidence_protocol", "evidence_revision_at", "metrics",
                    "artifact_hash", "source_dataset_hash",
                    "evidence_submission_hash",
                )
            }
            selected["_frozen_evidence_submission"] = evidence
        elif action == "REVIEW_CHALLENGER":
            gate = evidence.get("gate_validation")
            validation_payload = {
                key: value for key, value in evidence.items()
                if key != "validation_hash"
            }
            decision = str(evidence.get("decision") or "")
            reviewer = str(payload.get("operator") or "")
            expected_passed = bool(
                decision == "CONFIRM"
                and isinstance(gate, dict)
                and gate.get("passed") is True
            )
            gate_payload = {
                key: value for key, value in (gate or {}).items()
                if key != "validation_hash"
            }
            submission = selected.get("_frozen_evidence_submission") or {}
            if (
                selected.get("status") != "REVIEW_PENDING"
                or evidence.get("schema")
                != "probiga.strategy-challenger-review.v2"
                or evidence.get("challenger_id") != challenger_id
                or evidence.get("proposal_hash") != selected.get("proposal_hash")
                or evidence.get("proposed_version_hash")
                != selected.get("proposed_version_hash")
                or evidence.get("evidence_submission_hash")
                != submission.get("evidence_submission_hash")
                or evidence.get("artifact_hash") != submission.get("artifact_hash")
                or evidence.get("source_dataset_hash")
                != submission.get("source_dataset_hash")
                or reviewer != evidence.get("reviewer")
                or reviewer in {
                    str(selected.get("submitted_by") or ""),
                    str(selected.get("evidence_submitted_by") or ""),
                }
                or decision not in {"CONFIRM", "REJECT"}
                or not isinstance(gate, dict)
                or not hash64(gate.get("validation_hash"))
                or _digest(gate_payload) != gate.get("validation_hash")
                or not hash64(evidence.get("validation_hash"))
                or _digest(validation_payload)
                != evidence.get("validation_hash")
                or evidence.get("passed") is not expected_passed
                or evidence.get("automatic_real_order_submission") is not False
                or evidence.get("real_order_authority") is not False
                or after != {
                    "challenger_id": challenger_id,
                    "status": "READY" if expected_passed else "REJECTED",
                    "validation_hash": evidence.get("validation_hash"),
                }
            ):
                raise RuntimeError("挑战者复核审计合同无效")
            selected["status"] = (
                "READY" if expected_passed else "REJECTED"
            )
            selected["status_label"] = (
                "可晋级" if expected_passed else "已驳回"
            )
            selected["latest_validation"] = evidence
            selected["reviewed_by"] = reviewer
            selected["reviewed_at"] = str(
                row.get("created_at") or ""
            )
        elif action == "PROMOTE_CHALLENGER":
            latest = selected.get("latest_validation") or {}
            submission = selected.get("_frozen_evidence_submission") or {}
            if (
                selected.get("status") != "READY"
                or evidence.get("challenger_id") != challenger_id
                or evidence.get("proposal_hash") != selected.get("proposal_hash")
                or evidence.get("validation_hash")
                != latest.get("validation_hash")
                or evidence.get("evidence_submission_hash")
                != submission.get("evidence_submission_hash")
                or evidence.get("artifact_hash") != submission.get("artifact_hash")
                or evidence.get("source_dataset_hash")
                != submission.get("source_dataset_hash")
                or evidence.get("promoted_version_hash")
                != selected.get("proposed_version_hash")
                or evidence.get("automatic_real_order_submission") is not False
                or evidence.get("real_order_authority") is not False
            ):
                raise RuntimeError("挑战者晋级审计合同无效")
            selected["status"] = "PROMOTED"
            selected["status_label"] = "已晋级为新影子版本"
            selected["promoted_version_hash"] = str(
                evidence.get("promoted_version_hash") or ""
            )
            selected["promoted_at"] = str(
                row.get("created_at") or ""
            )
        else:
            raise RuntimeError("挑战者审计动作不受支持")
    return sorted(
        proposals.values(),
        key=lambda item: (
            str(item.get("submitted_at") or ""),
            str(item.get("challenger_id") or ""),
        ),
        reverse=True,
    )


def list_strategy_challengers(strategy_key: str = "") -> dict[str, Any]:
    ensure_and_seed_governance()
    selected_key = validate_strategy_key(strategy_key) if strategy_key else ""
    with get_engine().connect() as connection:
        events = _proposal_events(connection, selected_key)
    challengers = _challengers_from_grouped_events(events)
    challengers.sort(
        key=lambda item: (
            str(item.get("submitted_at") or ""),
            str(item.get("challenger_id") or ""),
        ),
        reverse=True,
    )
    public_challengers = []
    for item in challengers:
        public = {
            key: value for key, value in item.items()
            if not str(key).startswith("_")
        }
        authority = {
            "source_authority": EXTERNAL_SELECTION_SOURCE_AUTHORITY,
            "source_authority_label": (
                EXTERNAL_SELECTION_SOURCE_AUTHORITY_LABEL
            ),
            "review_scope": "STRUCTURE_AND_REPRODUCIBILITY_ONLY",
            "funding_authority": False,
            "real_order_authority": False,
        }
        public.update(authority)
        for field in ("evidence_submission", "latest_validation"):
            if isinstance(public.get(field), dict):
                public[field] = {**public[field], **authority}
        public_challengers.append(public)
    return {
        "status": "ok",
        "policy": CHALLENGER_POLICY,
        "challengers": public_challengers,
        "automatic_formula_mutation": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def register_strategy_challenger(
    payload: dict[str, Any], *, operator: str,
) -> dict[str, Any]:
    ensure_and_seed_governance()
    normalized, version_hash = _normalized_registration_payload(
        payload, operator=operator,
    )
    key = normalized["strategy_key"]
    challenger_id = uuid.uuid4().hex
    with get_engine().begin() as connection:
        _lock_challenger_namespace(connection)
        registry = connection.execute(
            text(
                "SELECT current_version FROM st_strategy_registry "
                "WHERE strategy_key=:strategy_key FOR UPDATE"
            ),
            {"strategy_key": key},
        ).mappings().first()
        if registry is None:
            raise ValueError("挑战者必须属于已注册策略家族")
        parent = str(registry.get("current_version") or "")
        events = _proposal_events(connection, key)
        existing = _challengers_from_events(events)
        if any(
            str(item.get("proposed_version") or "") == normalized["version"]
            for item in existing
        ):
            raise ValueError("同一挑战者版本已经登记")
        version_exists = connection.execute(
            text(
                "SELECT COUNT(*) FROM st_strategy_version "
                "WHERE strategy_key=:strategy_key AND version=:version"
            ),
            {"strategy_key": key, "version": normalized["version"]},
        ).scalar()
        if int(version_exists or 0):
            raise ValueError("该版本已经是正式版本，不能重复作为挑战者")
        proposal_payload = {
            "challenger_id": challenger_id,
            "strategy_key": key,
            "parent_version": parent,
            "proposed_version": normalized["version"],
            "proposed_version_hash": version_hash,
            "registration_payload": normalized,
            "proposal_hash": _digest({
                "strategy_key": key,
                "parent_version": parent,
                "proposed_version_hash": version_hash,
                "registration_payload": normalized,
            }),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        _append_audit_connection(
            connection,
            entity_type="STRATEGY",
            entity_key=key,
            action="REGISTER_CHALLENGER",
            reason=str(payload.get("reason") or "登记挑战者版本")[:500],
            operator=operator,
            before={"champion_version": parent},
            after=proposal_payload,
            evidence={
                "policy_hash": _digest(CHALLENGER_POLICY),
                "requires_independent_review": True,
            },
        )
    return next(
        item for item in list_strategy_challengers(key)["challengers"]
        if item["challenger_id"] == challenger_id
    )


def _validate_challenger_evidence(
    metrics: dict[str, Any], *, evidence_protocol: str = "",
    window_days: int = 0, artifact_hash: str = "",
    source_dataset_hash: str = "", artifact_replayed: bool = False,
) -> dict[str, Any]:
    """Apply only gates reproducible from the validated trade/WF artifact."""

    try:
        normalized = _validated_metric_evidence(metrics)
    except (TypeError, ValueError):
        normalized = {}

    def number(name: str) -> float | None:
        value = normalized.get(name)
        return float(value) if isinstance(value, (int, float)) else None

    completed_trades = number("completed_trades")
    coverage_days = number("coverage_days")
    walk_forward_segments = number("walk_forward_segments")
    positive_segments = number("positive_segments")
    profit_factor = number("profit_factor")
    payoff_ratio = number("payoff_ratio")
    net_expectancy = number("net_expectancy_pct")
    cost_stress_expectancy = number("cost_stress_expectancy_pct")
    checks = {
        "server_replayed_artifact": artifact_replayed is True,
        "artifact_hash_bound": len(artifact_hash) == 64
        and all(c in "0123456789abcdef" for c in artifact_hash),
        "source_dataset_hash_bound": len(source_dataset_hash) == 64
        and all(c in "0123456789abcdef" for c in source_dataset_hash),
        "required_window": window_days
        == int(CHALLENGER_POLICY["required_window_days"]),
        "purged_walk_forward_protocol": evidence_protocol in CHALLENGER_POLICY[
            "required_protocols"
        ],
        "independent_oos": normalized.get("independent_oos") is True
        and normalized.get("walk_forward_verified") is True,
        "completed_trades": completed_trades is not None
        and completed_trades >= CHALLENGER_POLICY["minimum_completed_trades"],
        "coverage_days": coverage_days is not None
        and coverage_days >= CHALLENGER_POLICY["minimum_coverage_days"],
        "walk_forward_segments": walk_forward_segments is not None
        and walk_forward_segments
        >= CHALLENGER_POLICY["minimum_walk_forward_segments"],
        "positive_segments": positive_segments is not None
        and positive_segments >= CHALLENGER_POLICY["minimum_positive_segments"]
        and walk_forward_segments is not None
        and positive_segments <= walk_forward_segments,
        "profit_factor": profit_factor is not None
        and profit_factor >= CHALLENGER_POLICY["minimum_profit_factor"],
        "payoff_ratio": payoff_ratio is not None
        and payoff_ratio >= CHALLENGER_POLICY["minimum_payoff_ratio"],
        "net_expectancy": net_expectancy is not None
        and net_expectancy > CHALLENGER_POLICY["minimum_net_expectancy_pct"],
        "cost_stress_expectancy": cost_stress_expectancy is not None
        and cost_stress_expectancy
        > CHALLENGER_POLICY["minimum_cost_stress_expectancy_pct"],
    }
    passed = all(checks.values())
    payload = {
        "schema": "probiga.strategy-challenger-validation.v2",
        "policy": CHALLENGER_POLICY,
        "metrics": normalized,
        "evidence_protocol": evidence_protocol,
        "window_days": window_days,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
        "checks": checks,
        "passed": passed,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**payload, "validation_hash": _digest(payload)}


def _replay_frozen_challenger_evidence(
    selected: dict[str, Any], evidence: dict[str, Any],
) -> dict[str, Any]:
    registration = selected.get("registration_payload")
    if not isinstance(registration, dict):
        raise RuntimeError("挑战者缺少冻结版本定义")
    parameters = registration.get("parameters")
    if not isinstance(parameters, dict):
        raise RuntimeError("挑战者缺少冻结持有期定义")
    metrics = _validated_metric_evidence(evidence.get("metrics"))
    artifact_hash = str(evidence.get("artifact_hash") or "")
    artifact = _validate_metric_artifact(
        evidence.get("artifact_manifest"),
        entity_type="STRATEGY",
        entity_key=str(selected.get("strategy_key") or ""),
        entity_version=str(selected.get("proposed_version") or ""),
        as_of_date=str(evidence.get("as_of_date") or ""),
        window_days=int(evidence.get("window_days") or 0),
        evidence_protocol=str(evidence.get("evidence_protocol") or ""),
        evidence_revision_at=str(evidence.get("evidence_revision_at") or ""),
        metrics=metrics,
        artifact_hash=artifact_hash,
        version_created_at=str(selected.get("submitted_at") or ""),
        expected_max_holding_days=_holding_horizon_from_parameters(parameters),
        expected_label_horizon_days=_label_horizon_from_parameters(parameters),
    )
    source_dataset_hash = str(
        artifact.get("source_dataset_hash") or ""
    ).lower()
    if source_dataset_hash != str(evidence.get("source_dataset_hash") or ""):
        raise ValueError("挑战者底层样本哈希与冻结审计不一致")
    return _validate_challenger_evidence(
        metrics,
        evidence_protocol=str(evidence.get("evidence_protocol") or ""),
        window_days=int(evidence.get("window_days") or 0),
        artifact_hash=artifact_hash,
        source_dataset_hash=source_dataset_hash,
        artifact_replayed=True,
    )


def _assert_challenger_evidence_unclaimed(
    connection, *, challenger_id: str, artifact_hash: str,
    source_dataset_hash: str,
) -> None:
    metric_claim = connection.execute(text(
        "SELECT evidence_id, artifact_hash, source_dataset_hash "
        "FROM st_strategy_metric_input "
        "WHERE BINARY artifact_hash=:artifact_hash "
        "OR BINARY source_dataset_hash=:source_dataset_hash "
        "ORDER BY evidence_id FOR UPDATE"
    ), {
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
    }).mappings().first()
    if metric_claim is not None:
        if str(metric_claim.get("artifact_hash") or "") == artifact_hash:
            raise ValueError("验证产物已经被普通指标证据占用")
        raise ValueError("底层样本集已经被普通指标证据占用")

    rows = connection.execute(text(
        "SELECT action, operator_name, payload_json, audit_hash, created_at "
        "FROM st_strategy_governance_audit "
        "WHERE action='SUBMIT_CHALLENGER_EVIDENCE' "
        "ORDER BY created_at, audit_id FOR UPDATE"
    )).mappings().all()
    for raw in rows:
        payload = _verified_audit_payload(dict(raw))
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict):
            raise RuntimeError("历史挑战者证据审计无效")
        owner = str(evidence.get("challenger_id") or "")
        if owner == challenger_id:
            raise ValueError("该挑战者已经冻结过验证产物")
        if evidence.get("artifact_hash") == artifact_hash:
            raise ValueError("同一验证产物不能跨挑战者复用")
        if evidence.get("source_dataset_hash") == source_dataset_hash:
            raise ValueError("同一底层样本集不能跨挑战者复用")


def submit_strategy_challenger_evidence(
    challenger_id: str, evidence_payload: dict[str, Any], *,
    operator: str, reason: str,
) -> dict[str, Any]:
    ensure_and_seed_governance()
    if len(str(challenger_id or "")) != 32:
        raise ValueError("挑战者身份无效")
    selected = next((
        item for item in list_strategy_challengers()["challengers"]
        if str(item.get("challenger_id") or "") == challenger_id
    ), None)
    if selected is None:
        raise ValueError("挑战者不存在")
    key = str(selected.get("strategy_key") or "")
    with get_engine().begin() as connection:
        _lock_challenger_namespace(connection)
        connection.execute(text(
            "SELECT current_version FROM st_strategy_registry "
            "WHERE strategy_key=:strategy_key FOR UPDATE"
        ), {"strategy_key": key}).first()
        selected = next((
            item for item in _challengers_from_events(
                _proposal_events(connection, key)
            ) if str(item.get("challenger_id") or "") == challenger_id
        ), None)
        if selected is None or selected.get("status") != "VALIDATING":
            raise ValueError("挑战者当前状态不允许提交验证产物")
        metrics = _validated_metric_evidence(evidence_payload.get("metrics"))
        provisional = {
            "schema": "probiga.strategy-challenger-evidence-submission.v1",
            "challenger_id": challenger_id,
            "proposal_hash": str(selected.get("proposal_hash") or ""),
            "proposed_version_hash": str(
                selected.get("proposed_version_hash") or ""
            ),
            "proposal_submitted_at": str(selected.get("submitted_at") or ""),
            "submitted_by": str(operator or ""),
            "as_of_date": str(evidence_payload.get("as_of_date") or ""),
            "window_days": int(evidence_payload.get("window_days") or 0),
            "evidence_protocol": str(
                evidence_payload.get("evidence_protocol") or ""
            ),
            "evidence_revision_at": str(
                evidence_payload.get("evidence_revision_at") or ""
            ),
            "metrics": metrics,
            "artifact_manifest": evidence_payload.get("artifact_manifest"),
            "artifact_hash": str(evidence_payload.get("artifact_hash") or ""),
            "source_dataset_hash": str(
                (evidence_payload.get("artifact_manifest") or {}).get(
                    "source_dataset_hash"
                ) or ""
            ).lower(),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        gate = _replay_frozen_challenger_evidence(selected, provisional)
        provisional["server_replay_validation_hash"] = gate["validation_hash"]
        _assert_challenger_evidence_unclaimed(
            connection,
            challenger_id=challenger_id,
            artifact_hash=provisional["artifact_hash"],
            source_dataset_hash=provisional["source_dataset_hash"],
        )
        submission = {
            **provisional,
            "evidence_submission_hash": _digest(provisional),
        }
        _append_audit_connection(
            connection,
            entity_type="STRATEGY",
            entity_key=key,
            action="SUBMIT_CHALLENGER_EVIDENCE",
            reason=str(reason or "提交挑战者可重算验证产物")[:500],
            operator=operator,
            before={"challenger_id": challenger_id, "status": "VALIDATING"},
            after={
                "challenger_id": challenger_id,
                "status": "REVIEW_PENDING",
                "evidence_submission_hash": submission[
                    "evidence_submission_hash"
                ],
                "artifact_hash": submission["artifact_hash"],
                "source_dataset_hash": submission["source_dataset_hash"],
            },
            evidence=submission,
        )
    return next(
        item for item in list_strategy_challengers(key)["challengers"]
        if item["challenger_id"] == challenger_id
    )


def review_strategy_challenger(
    challenger_id: str, decision: str, *, operator: str, reason: str,
) -> dict[str, Any]:
    ensure_and_seed_governance()
    if len(str(challenger_id or "")) != 32:
        raise ValueError("挑战者身份无效")
    selected = next(
        (
            item for item in list_strategy_challengers()["challengers"]
            if str(item.get("challenger_id") or "") == challenger_id
        ),
        None,
    )
    if selected is None:
        raise ValueError("挑战者不存在")
    selected_key = str(selected.get("strategy_key") or "")
    with get_engine().begin() as connection:
        _lock_challenger_namespace(connection)
        connection.execute(
            text(
                "SELECT current_version FROM st_strategy_registry "
                "WHERE strategy_key=:strategy_key FOR UPDATE"
            ),
            {"strategy_key": selected_key},
        ).first()
        selected = next(
            (
                item for item in _challengers_from_events(
                    _proposal_events(connection, selected_key)
                )
                if str(item.get("challenger_id") or "") == challenger_id
            ),
            None,
        )
        if selected is None:
            raise ValueError("挑战者不存在")
        if selected.get("status") != "REVIEW_PENDING":
            raise ValueError("挑战者尚未提交可重算产物或已经完成复核")
        if str(operator or "") in {
            str(selected.get("submitted_by") or ""),
            str(selected.get("evidence_submitted_by") or ""),
        }:
            raise ValueError("挑战者提交人与独立复核人必须分离")
        decision = str(decision or "").strip().upper()
        if decision not in {"CONFIRM", "REJECT"}:
            raise ValueError("复核结论只能是CONFIRM或REJECT")
        frozen = selected.get("_frozen_evidence_submission")
        if not isinstance(frozen, dict):
            raise RuntimeError("挑战者冻结产物在复核时丢失")
        gate = _replay_frozen_challenger_evidence(selected, frozen)
        validation_payload = {
            "schema": "probiga.strategy-challenger-review.v2",
            "challenger_id": challenger_id,
            "proposal_hash": str(selected.get("proposal_hash") or ""),
            "proposed_version_hash": str(
                selected.get("proposed_version_hash") or ""
            ),
            "evidence_submission_hash": str(
                frozen.get("evidence_submission_hash") or ""
            ),
            "artifact_hash": str(frozen.get("artifact_hash") or ""),
            "source_dataset_hash": str(
                frozen.get("source_dataset_hash") or ""
            ),
            "reviewer": str(operator or ""),
            "decision": decision,
            "review_reason": str(reason or "")[:500],
            "gate_validation": gate,
            "passed": decision == "CONFIRM" and gate["passed"] is True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        validation = {
            **validation_payload,
            "validation_hash": _digest(validation_payload),
        }
        _append_audit_connection(
            connection,
            entity_type="STRATEGY",
            entity_key=selected_key,
            action="REVIEW_CHALLENGER",
            reason=str(reason or "独立复核挑战者")[:500],
            operator=operator,
            before={
                "challenger_id": challenger_id,
                "status": selected.get("status"),
            },
            after={
                "challenger_id": challenger_id,
                "status": "READY" if validation["passed"] else "REJECTED",
                "validation_hash": validation["validation_hash"],
            },
            evidence=validation,
        )
    return next(
        item for item in list_strategy_challengers(selected_key)["challengers"]
        if item["challenger_id"] == challenger_id
    )


def promote_strategy_challenger(
    challenger_id: str, *, operator: str, reason: str,
) -> dict[str, Any]:
    ensure_and_seed_governance()
    all_rows = list_strategy_challengers()["challengers"]
    selected = next(
        (
            item for item in all_rows
            if str(item.get("challenger_id") or "") == challenger_id
        ),
        None,
    )
    if selected is None:
        raise ValueError("挑战者不存在")
    key = str(selected.get("strategy_key") or "")
    if selected.get("status") == "PROMOTED":
        current = next(row for row in load_registry() if row["strategy_key"] == key)
        return {"challenger": selected, "strategy": current, "idempotent": True}
    if selected.get("status") != "READY":
        raise ValueError("挑战者尚未通过可重算样本外产物的独立复核，不能晋级")
    final_validation = selected.get("latest_validation")
    if not isinstance(final_validation, dict):
        raise RuntimeError("挑战者缺少最终复核合同")
    final_validation_payload = {
        key: value for key, value in final_validation.items()
        if key != "validation_hash"
    }
    if (
        final_validation.get("passed") is not True
        or _digest(final_validation_payload)
        != str(final_validation.get("validation_hash") or "")
    ):
        raise RuntimeError("挑战者最终复核哈希无效")
    registration_payload = selected.get("registration_payload")
    if not isinstance(registration_payload, dict):
        raise RuntimeError("挑战者缺少冻结版本定义")
    normalized, expected_hash = _normalized_registration_payload(
        registration_payload, operator=operator,
    )
    if (
        expected_hash != str(selected.get("proposed_version_hash") or "")
        or _digest({
            "strategy_key": key,
            "parent_version": selected.get("parent_version"),
            "proposed_version_hash": expected_hash,
            "registration_payload": normalized,
        }) != str(selected.get("proposal_hash") or "")
    ):
        raise ValueError("挑战者定义或市场路由配置已变化，必须重新登记验证")
    current = next(row for row in load_registry() if row["strategy_key"] == key)
    current_version = str(current.get("current_version") or "")
    parent_version = str(selected.get("parent_version") or "")
    proposed_version = str(selected.get("proposed_version") or "")
    with get_engine().begin() as admission_connection:
        _lock_challenger_namespace(admission_connection)
        locked_current_version = str(admission_connection.execute(text(
            "SELECT current_version FROM st_strategy_registry "
            "WHERE strategy_key=:strategy_key"
        ), {"strategy_key": key}).scalar() or "")
        if locked_current_version != current_version:
            raise ValueError("冠军版本已变化；该挑战者必须基于新冠军重新验证")
        if current_version == parent_version:
            strategy = register_strategy(
                normalized, operator=operator,
                _global_inventory_lock_held=True,
            )
        elif current_version == proposed_version:
            # Recover safely when version registration committed but the
            # promotion audit transaction was interrupted.
            if str(current.get("version_hash") or "") != expected_hash:
                raise RuntimeError("当前挑战者版本哈希与已复核对象不一致")
            strategy = current
        else:
            raise ValueError("冠军版本已变化；该挑战者必须基于新冠军重新验证")
    if str(strategy.get("version_hash") or "") != expected_hash:
        raise RuntimeError("晋级后的不可变版本哈希与挑战者验证对象不一致")
    already_promoted = False
    with get_engine().begin() as connection:
        _lock_challenger_namespace(connection)
        locked = connection.execute(
            text(
                "SELECT current_version FROM st_strategy_registry "
                "WHERE strategy_key=:strategy_key FOR UPDATE"
            ),
            {"strategy_key": key},
        ).mappings().first()
        if str((locked or {}).get("current_version") or "") != proposed_version:
            raise RuntimeError("挑战者晋级期间当前版本再次变化")
        locked_selected = next(
            (
                item for item in _challengers_from_events(
                    _proposal_events(connection, key)
                )
                if str(item.get("challenger_id") or "") == challenger_id
            ),
            None,
        )
        if locked_selected is None:
            raise RuntimeError("挑战者审计链在晋级期间丢失")
        already_promoted = locked_selected.get("status") == "PROMOTED"
        if not already_promoted:
            if locked_selected.get("status") != "READY":
                raise RuntimeError("挑战者在晋级期间失去可晋级状态")
            locked_validation = locked_selected.get("latest_validation") or {}
            frozen = (
                locked_selected.get("_frozen_evidence_submission") or {}
            )
            if (
                locked_validation.get("validation_hash")
                != final_validation.get("validation_hash")
                or locked_validation.get("passed") is not True
            ):
                raise RuntimeError("挑战者最终复核合同在晋级期间发生变化")
            _append_audit_connection(
                connection,
                entity_type="STRATEGY",
                entity_key=key,
                action="PROMOTE_CHALLENGER",
                reason=str(reason or "挑战者晋级为新影子版本")[:500],
                operator=operator,
                before={
                    "challenger_id": challenger_id,
                    "champion_version": selected.get("parent_version"),
                },
                after={
                    "challenger_id": challenger_id,
                    "new_version": strategy.get("current_version"),
                    "new_status": strategy.get("current_status"),
                },
                evidence={
                    "challenger_id": challenger_id,
                    "proposal_hash": selected.get("proposal_hash"),
                    "validation_hash": (
                        locked_validation.get("validation_hash")
                    ),
                    "evidence_submission_hash": frozen.get(
                        "evidence_submission_hash"
                    ),
                    "artifact_hash": frozen.get("artifact_hash"),
                    "source_dataset_hash": frozen.get(
                        "source_dataset_hash"
                    ),
                    "promoted_version_hash": expected_hash,
                    "automatic_real_order_submission": False,
                    "real_order_authority": False,
                },
            )
    refreshed = next(
        item for item in list_strategy_challengers(key)["challengers"]
        if item["challenger_id"] == challenger_id
    )
    return {
        "challenger": refreshed,
        "strategy": strategy,
        "idempotent": already_promoted,
    }


__all__ = [
    "CHALLENGER_POLICY",
    "StrategyAlreadyRegisteredError",
    "list_strategy_challengers",
    "promote_strategy_challenger",
    "register_new_strategy",
    "register_strategy_challenger",
    "review_strategy_challenger",
    "submit_strategy_challenger_evidence",
]
