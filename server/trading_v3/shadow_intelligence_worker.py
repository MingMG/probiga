from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from .continuous_calibration import (
    HorizonModelLifecycleAdapter,
    ImmutableEvidenceStore,
    continuous_cycle_lock,
    run_continuous_calibration_orchestration,
)
from .config import config_hash as current_config_hash
from .config import load_v3_config
from .horizon_contracts import (
    CalibrationEvidence,
    HorizonForecastContract,
    HorizonOutcomeEvidence,
    PredictionKind,
)
from .horizon_models import (
    HorizonModelError,
    predict_horizon_artifact,
    verify_horizon_artifact,
)
from .learning_intelligence import counterfactual_learning_metrics
from .release_governance import (
    CalibrationGateDecision,
    ContinuousCalibrationEvidence,
    ReleaseEvent,
    ReleaseStage,
    enforce_continuous_gate,
    evaluate_continuous_calibration,
    transition_shadow_release,
)
from .shadow_intelligence_repository import ShadowIntelligenceRepository


MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
PROXY_SCORER_ALGORITHM_VERSION = "BOUNDED_LINEAR_PROXY_V1"
PROXY_SCORER_SOURCE_HASH = hashlib.sha256(
    Path(__file__).read_bytes()
).hexdigest()
FORWARD_SHADOW_BINDING_PROTOCOL = (
    "probiga.trading-v3.forward-shadow-artifact-binding.v1"
)
QMT_OUTCOME_ATTESTATION_PROTOCOL = (
    "probiga.trading-v3.qmt-attested-outcome-bars.v1"
)


class _CandidateFeatureBlocked(HorizonModelError):
    """A candidate-local feature-quality failure, not a suite failure."""

    def __init__(
        self,
        reason_code: str,
        *,
        observed_history_sessions: int | None = None,
        required_history_sessions: int | None = None,
    ) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.observed_history_sessions = observed_history_sessions
        self.required_history_sessions = required_history_sessions


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = str(value or "").strip()
        result = datetime.fromisoformat(
            raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        )
    if result.tzinfo is None or result.utcoffset() is None:
        result = result.replace(tzinfo=MARKET_TIMEZONE)
    return result


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _future_trade_dates(
    calendar_engine: Engine,
    *,
    after: date,
    count: int,
) -> tuple[date, ...]:
    with calendar_engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date > :after
                ORDER BY trade_date
                LIMIT :limit
                """
            ),
            {"after": after, "limit": max(1, int(count))},
        ).scalars().all()
    result = tuple(
        item if isinstance(item, date) else date.fromisoformat(str(item))
        for item in rows
    )
    return result


def _model_specs(config: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = dict(config.get("multi_horizon_forecasts") or {})
    if (
        str(raw.get("lifecycle") or "") != "SHADOW_RESEARCH_ONLY"
        or raw.get("order_allowed") is not False
        or raw.get("can_activate_model") is not False
    ):
        raise ValueError("multi-horizon runtime must remain research-only")
    if str(raw.get("scorer_algorithm_version") or "") != (
        PROXY_SCORER_ALGORITHM_VERSION
    ):
        raise ValueError("proxy scorer algorithm version is not frozen")
    models = dict(raw.get("models") or {})
    specs = []
    for label, item in models.items():
        payload = dict(item or {})
        try:
            horizon = int(str(label).replace("T+", ""))
        except ValueError as exc:
            raise ValueError(f"invalid horizon model label: {label}") from exc
        if horizon not in {1, 5, 20}:
            raise ValueError(f"unsupported horizon model: {label}")
        sources = tuple(
            str(value)
            for value in (payload.get("source_strategy_keys") or ())
            if str(value)
        )
        if not sources:
            raise ValueError(f"{label} source_strategy_keys must not be empty")
        for field in ("model_key", "model_version", "cost_model_version"):
            if not str(payload.get(field) or "").strip():
                raise ValueError(f"{label} {field} must not be empty")
        try:
            cost_assumption = float(payload["cost_assumption_pct"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"{label} cost_assumption_pct must be numeric"
            ) from exc
        if cost_assumption < 0:
            raise ValueError(
                f"{label} cost_assumption_pct must not be negative"
            )
        if (
            str(payload.get("lifecycle") or "")
            != "SHADOW_RESEARCH_ONLY"
            or payload.get("order_allowed") is not False
        ):
            raise ValueError(f"{label} must remain research-only")
        PredictionKind(str(payload.get("prediction_kind") or ""))
        protocol = dict(payload.get("feature_protocol") or {})
        inputs = dict(protocol.get("inputs") or {})
        if not str(protocol.get("protocol_version") or "") or not inputs:
            raise ValueError(f"{label} feature_protocol must be frozen")
        for feature_key, raw_rule in inputs.items():
            rule = dict(raw_rule or {})
            try:
                weight = float(rule["weight"])
                lower = float(rule["lower"])
                upper = float(rule["upper"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"{label} feature rule {feature_key} is invalid"
                ) from exc
            if (
                not all(math.isfinite(value) for value in (weight, lower, upper))
                or weight <= 0
                or upper <= lower
            ):
                raise ValueError(
                    f"{label} feature rule {feature_key} is invalid"
                )
        feature_protocol_hash = _canonical_hash(protocol)
        artifact_payload = {
            "model_key": payload["model_key"],
            "model_version": payload["model_version"],
            "horizon_days": horizon,
            "prediction_kind": payload["prediction_kind"],
            "source_strategy_keys": list(sources),
            "feature_protocol_hash": feature_protocol_hash,
            "cost_assumption_pct": cost_assumption,
            "cost_model_version": payload["cost_model_version"],
            "scorer_algorithm_version": PROXY_SCORER_ALGORITHM_VERSION,
            "scorer_source_hash": PROXY_SCORER_SOURCE_HASH,
        }
        specs.append({
            **payload,
            "label": label,
            "horizon_days": horizon,
            "source_strategy_keys": sources,
            "cost_assumption_pct": cost_assumption,
            "feature_protocol_hash": feature_protocol_hash,
            "scorer_source_hash": PROXY_SCORER_SOURCE_HASH,
            "model_artifact_hash": _canonical_hash(artifact_payload),
        })
    if {item["horizon_days"] for item in specs} != {1, 5, 20}:
        raise ValueError("T+1, T+5 and T+20 model specs are all required")
    if len({str(item.get("model_key")) for item in specs}) != 3:
        raise ValueError("horizon model keys must be independent")
    if len({item["feature_protocol_hash"] for item in specs}) != 3:
        raise ValueError("horizon feature protocols must be independent")
    if len({item["model_artifact_hash"] for item in specs}) != 3:
        raise ValueError("horizon model artifacts must be independent")
    return tuple(sorted(specs, key=lambda item: item["horizon_days"]))


def _trainable_model_specs(
    config: Mapping[str, Any],
) -> dict[int, dict[str, Any]]:
    """Validate the three configured OOS identities used for registry lookup."""

    suite = dict(config.get("multi_horizon_forecasts") or {})
    selection = dict(suite.get("runtime_model_selection") or {})
    if (
        selection.get("policy")
        != "CURRENT_DB_OOS_VERIFIED_ELSE_FROZEN_PROXY"
        or selection.get("artifact_must_match_current_config_and_code")
        is not True
        or selection.get("fallback_prediction_kind") != "PROXY_SCORE"
        or selection.get("fallback_can_activate_model") is not False
    ):
        raise RuntimeError("HORIZON_RUNTIME_SELECTION_POLICY_INVALID")
    raw_models = dict(suite.get("trainable_models") or {})
    specs: dict[int, dict[str, Any]] = {}
    for label, raw in raw_models.items():
        try:
            horizon = int(str(label).replace("T+", ""))
        except ValueError as exc:
            raise RuntimeError(
                "HORIZON_TRAINABLE_MODEL_LABEL_INVALID"
            ) from exc
        payload = dict(raw or {})
        sources = tuple(
            str(item)
            for item in (payload.get("source_strategy_keys") or ())
            if str(item)
        )
        features = tuple(
            str(item)
            for item in (payload.get("features") or ())
            if str(item)
        )
        if (
            horizon not in {1, 5, 20}
            or not str(payload.get("model_key") or "").strip()
            or not str(payload.get("model_version") or "").strip()
            or not str(payload.get("algorithm") or "").strip()
            or not sources
            or not features
            or payload.get("order_allowed") is not False
        ):
            raise RuntimeError(
                f"T+{horizon}_TRAINABLE_MODEL_SPEC_INVALID"
            )
        specs[horizon] = {
            **payload,
            "label": f"T+{horizon}",
            "horizon_days": horizon,
            "source_strategy_keys": sources,
            "features": features,
        }
    if set(specs) != {1, 5, 20}:
        raise RuntimeError("HORIZON_TRAINABLE_MODELS_INCOMPLETE")
    if len({item["model_key"] for item in specs.values()}) != 3:
        raise RuntimeError("HORIZON_TRAINABLE_MODELS_NOT_INDEPENDENT")
    return specs


def _source_contract_evidence(
    row: Mapping[str, Any],
    *,
    stock_code: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_forecast_id = str(row["forecast_id"])
    source_snapshot = {
        "forecast_id": source_forecast_id,
        "run_uid": str(row["run_uid"]),
        "stock_code": stock_code,
        "strategy_key": str(row["strategy_key"]),
        "source_model_version": str(row.get("source_model_version") or ""),
        "dataset_hash": str(row.get("dataset_hash") or ""),
        "forecast_status": str(row.get("forecast_status") or ""),
        "raw_score": float(row["raw_score"]),
        "feature_time": _aware(row["feature_time"]).isoformat(),
        "valid_until": _aware(row["valid_until"]).isoformat(),
        "features": dict(row["features"]),
        "reasons_json": str(row.get("reasons_json") or ""),
        "decision_result_hash": str(row.get("decision_result_hash") or ""),
        "data_snapshot_hash": str(row.get("data_snapshot_hash") or ""),
        "decision_config_hash": str(row.get("config_hash") or ""),
    }
    selection_evidence = {
        "source_forecast_id": source_forecast_id,
        "source_forecast_hash": _canonical_hash(source_snapshot),
        "run_uid": str(row["run_uid"]),
        "source_strategy_key": str(row["strategy_key"]),
        "decision_result_hash": str(row["decision_result_hash"]),
        "selection_snapshot": dict(row["selection_snapshot"]),
    }
    return source_snapshot, selection_evidence


def _artifact_model_inputs(
    artifact: Mapping[str, Any],
    features: Mapping[str, Any],
) -> tuple[dict[str, float], tuple[str, ...]]:
    protocol = artifact.get("feature_protocol")
    if not isinstance(protocol, Mapping):
        raise HorizonModelError("artifact feature protocol is invalid")
    raw_required = protocol.get("history_sessions_required")
    if isinstance(raw_required, bool):
        raise HorizonModelError("artifact history requirement is invalid")
    try:
        required = int(raw_required)
    except (TypeError, ValueError) as exc:
        raise HorizonModelError(
            "artifact history requirement is invalid"
        ) from exc
    if required < 1 or float(raw_required) != float(required):
        raise HorizonModelError("artifact history requirement is invalid")

    history_fields = (
        "observed_history_sessions",
        "history_sessions_consecutive",
        "history_session_dates_hash",
        "expected_history_session_dates_hash",
    )
    if any(field not in features for field in history_fields):
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_EVIDENCE_MISSING",
            required_history_sessions=required,
        )
    raw_observed = features.get("observed_history_sessions")
    if isinstance(raw_observed, bool):
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_EVIDENCE_INVALID",
            required_history_sessions=required,
        )
    try:
        observed = int(raw_observed)
    except (TypeError, ValueError) as exc:
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_EVIDENCE_INVALID",
            required_history_sessions=required,
        ) from exc
    if observed < 0 or float(raw_observed) != float(observed):
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_EVIDENCE_INVALID",
            required_history_sessions=required,
        )
    if observed < required:
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_INSUFFICIENT",
            observed_history_sessions=observed,
            required_history_sessions=required,
        )
    observed_hash = str(
        features.get("history_session_dates_hash") or ""
    ).strip().lower()
    expected_hash = str(
        features.get("expected_history_session_dates_hash") or ""
    ).strip().lower()
    valid_hashes = all(
        len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
        for value in (observed_hash, expected_hash)
    )
    if not valid_hashes:
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_EVIDENCE_INVALID",
            observed_history_sessions=observed,
            required_history_sessions=required,
        )
    if (
        features.get("history_sessions_consecutive") is not True
        or observed_hash != expected_hash
    ):
        raise _CandidateFeatureBlocked(
            "FEATURE_HISTORY_NON_CONSECUTIVE",
            observed_history_sessions=observed,
            required_history_sessions=required,
        )

    model = dict(artifact.get("final_model") or {})
    names = tuple(str(item) for item in (model.get("features") or ()))
    medians = tuple(float(item) for item in (model.get("medians") or ()))
    if not names or len(names) != len(medians):
        raise HorizonModelError("artifact model input protocol is invalid")
    inputs: dict[str, float] = {}
    imputed: list[str] = []
    for name, median in zip(names, medians, strict=True):
        try:
            value = float(features.get(name))
        except (TypeError, ValueError):
            value = median
            imputed.append(name)
        if not math.isfinite(value):
            value = median
            if name not in imputed:
                imputed.append(name)
        inputs[name] = value
    return dict(sorted(inputs.items())), tuple(sorted(imputed))


def _artifact_calibration_evidence(
    artifact: Mapping[str, Any],
) -> CalibrationEvidence:
    oos = dict(artifact["oos_evidence"])
    execution = dict(artifact["execution_feasibility"])
    valid_until = datetime.combine(
        _date_value(artifact["valid_until"]),
        time(23, 59, 59, 999999),
        tzinfo=timezone.utc,
    )
    return CalibrationEvidence(
        evidence_id=str(artifact["oos_evidence_hash"]),
        model_key=str(artifact["model_key"]),
        model_version=str(artifact["model_version"]),
        horizon_days=int(artifact["horizon_days"]),
        dataset_hash=str(artifact["dataset_hash"]),
        feature_protocol_hash=str(artifact["feature_protocol_hash"]),
        cost_model_version=str(execution["cost_model_version"]),
        cost_assumption_pct=float(execution["cost_assumption_pct"]),
        matured_sample_count=int(oos["matured_sample_count"]),
        oos_sample_count=int(oos["oos_sample_count"]),
        walk_forward_fold_count=int(oos["walk_forward_fold_count"]),
        outcomes_include_costs=bool(oos["outcomes_include_costs"]),
        score_direction_valid=True,
        calibration_mae=float(oos["calibration_mae"]),
        brier_score=float(oos["brier_score"]),
        generated_at=_aware(artifact["created_at"]),
        valid_until=valid_until,
    )


def _verified_runtime_artifact(
    repository: ShadowIntelligenceRepository,
    *,
    spec: Mapping[str, Any],
    decision_as_of: datetime,
    source_config_hash: str,
) -> tuple[dict[str, Any] | None, str, list[str]]:
    """Resolve one current registry artifact without leaking storage errors."""

    if source_config_hash != current_config_hash():
        return None, "AVAILABLE", ["SOURCE_RUN_CONFIG_NOT_CURRENT"]
    lookup = getattr(repository, "latest_verified_horizon_artifact", None)
    if not callable(lookup):
        return None, "UNAVAILABLE", ["ARTIFACT_REGISTRY_UNAVAILABLE"]
    try:
        registered = lookup(
            model_key=str(spec["model_key"]),
            model_version=str(spec["model_version"]),
            horizon_days=int(spec["horizon_days"]),
            decision_as_of=decision_as_of,
        )
    except Exception:
        return None, "UNAVAILABLE", ["ARTIFACT_REGISTRY_UNAVAILABLE"]
    if registered is None:
        return None, "AVAILABLE", ["NO_CURRENT_OOS_VERIFIED_ARTIFACT"]
    try:
        artifact = verify_horizon_artifact(dict(registered["artifact"]))
        if (
            str(registered.get("artifact_status") or "") != "OOS_VERIFIED"
            or artifact["config_hash"] != source_config_hash
            or artifact["model_key"] != spec["model_key"]
            or artifact["model_version"] != spec["model_version"]
            or int(artifact["horizon_days"]) != int(spec["horizon_days"])
            or artifact["gate"]["status"] != "PASS"
            or artifact.get("contract_eligible") is not True
            or artifact.get("order_authority") is not False
        ):
            raise HorizonModelError("registry artifact identity differs")
    except (KeyError, TypeError, ValueError, HorizonModelError):
        return None, "AVAILABLE", ["VERIFIED_ARTIFACT_REJECTED"]
    return artifact, "AVAILABLE", []


def _verified_runtime_suite(
    repository: ShadowIntelligenceRepository,
    *,
    specs: Mapping[int, Mapping[str, Any]],
    decision_as_of: datetime,
    source_config_hash: str,
) -> tuple[dict[int, dict[str, Any]], str, list[str], str | None]:
    """Resolve one release-authorized, complete real-model suite."""

    if source_config_hash != current_config_hash():
        return {}, "AVAILABLE", ["SOURCE_RUN_CONFIG_NOT_CURRENT"], None
    lookup = getattr(repository, "latest_verified_horizon_suite", None)
    if not callable(lookup):
        return {}, "UNAVAILABLE", ["ARTIFACT_REGISTRY_UNAVAILABLE"], None
    try:
        registered = lookup(
            model_specs=specs,
            decision_as_of=decision_as_of,
        )
    except Exception:
        return {}, "UNAVAILABLE", ["ARTIFACT_REGISTRY_UNAVAILABLE"], None
    if registered is None:
        return (
            {},
            "AVAILABLE",
            ["NO_COMPLETE_RELEASED_OOS_VERIFIED_SUITE"],
            None,
        )
    try:
        suite_release_id = str(
            registered.get("suite_release_id") or ""
        ).strip()
        raw_artifacts = dict(
            registered.get("artifacts_by_horizon") or {}
        )
        raw_releases = dict(
            registered.get("release_states_by_horizon") or {}
        )
        artifacts_by_horizon = {
            int(key): dict(value) for key, value in raw_artifacts.items()
        }
        releases_by_horizon = {
            int(key): dict(value) for key, value in raw_releases.items()
        }
        if (
            not suite_release_id
            or set(artifacts_by_horizon) != {1, 5, 20}
            or set(releases_by_horizon) != {1, 5, 20}
            or bool(registered.get("order_authority"))
        ):
            raise HorizonModelError("registry suite is incomplete")
        verified: dict[int, dict[str, Any]] = {}
        allowed_stages = {"SHADOW", "CALIBRATION_REVIEW", "PAPER_ELIGIBLE"}
        for horizon in (1, 5, 20):
            row = artifacts_by_horizon[horizon]
            artifact = verify_horizon_artifact(dict(row["artifact"]))
            release = releases_by_horizon[horizon]
            spec = specs[horizon]
            if (
                str(row.get("artifact_status") or "") != "OOS_VERIFIED"
                or artifact["suite_release_id"] != suite_release_id
                or artifact["config_hash"] != source_config_hash
                or artifact["model_key"] != spec["model_key"]
                or artifact["model_version"] != spec["model_version"]
                or int(artifact["horizon_days"]) != horizon
                or artifact["gate"]["status"] != "PASS"
                or artifact.get("contract_eligible") is not True
                or artifact.get("order_authority") is not False
                or str(release.get("release_id") or "")
                != str(artifact["release_id"])
                or str(release.get("current_stage") or "")
                not in allowed_stages
                or bool(release.get("order_authority"))
            ):
                raise HorizonModelError("registry suite identity differs")
            verified[horizon] = artifact
    except (KeyError, TypeError, ValueError, HorizonModelError):
        return {}, "AVAILABLE", ["VERIFIED_ARTIFACT_SUITE_REJECTED"], None
    return verified, "AVAILABLE", [], suite_release_id


def _verified_forward_shadow_research_suite(
    repository: ShadowIntelligenceRepository,
    *,
    specs: Mapping[int, Mapping[str, Any]],
    decision_as_of: datetime,
    source_config_hash: str,
) -> tuple[dict[int, dict[str, Any]], str, list[str], str | None]:
    """Resolve a complete current suite even when its historical Gate BLOCKs.

    The repository lookup requires process provenance and a fully streamed
    candidate ledger.  This path is research-only: it has no release-state
    authority and the resulting contracts remain unable to promote or order.
    """

    if source_config_hash != current_config_hash():
        return {}, "AVAILABLE", ["SOURCE_RUN_CONFIG_NOT_CURRENT"], None
    lookup = getattr(
        repository, "latest_forward_shadow_research_suite", None
    )
    if not callable(lookup):
        return {}, "UNAVAILABLE", ["RESEARCH_ARTIFACT_REGISTRY_UNAVAILABLE"], None
    try:
        registered = lookup(
            model_specs=specs,
            decision_as_of=decision_as_of,
        )
    except Exception:
        return {}, "UNAVAILABLE", ["RESEARCH_ARTIFACT_REGISTRY_UNAVAILABLE"], None
    if registered is None:
        return {}, "AVAILABLE", ["NO_COMPLETE_FORWARD_RESEARCH_SUITE"], None
    try:
        if (
            str(registered.get("binding_protocol") or "")
            != FORWARD_SHADOW_BINDING_PROTOCOL
            or registered.get("promotion_eligible") is not False
            or bool(registered.get("order_authority"))
        ):
            raise HorizonModelError("research suite authority invalid")
        suite_release_id = str(
            registered.get("suite_release_id") or ""
        ).strip()
        raw_artifacts = {
            int(key): dict(value)
            for key, value in dict(
                registered.get("artifacts_by_horizon") or {}
            ).items()
        }
        if not suite_release_id or set(raw_artifacts) != {1, 5, 20}:
            raise HorizonModelError("research suite incomplete")
        verified: dict[int, dict[str, Any]] = {}
        for horizon in (1, 5, 20):
            row = raw_artifacts[horizon]
            artifact = verify_horizon_artifact(dict(row["artifact"]))
            spec = specs[horizon]
            gate_status = str(
                dict(artifact.get("gate") or {}).get("status") or ""
            )
            expected_registry_status = (
                "OOS_VERIFIED" if gate_status == "PASS" else "BLOCKED"
            )
            if (
                gate_status not in {"PASS", "BLOCK"}
                or str(row.get("artifact_status") or "")
                != expected_registry_status
                or str(row.get("training_receipt_status") or "")
                != "PROCESS_VERIFIED"
                or artifact["suite_release_id"] != suite_release_id
                or artifact["config_hash"] != source_config_hash
                or artifact["model_key"] != spec["model_key"]
                or artifact["model_version"] != spec["model_version"]
                or int(artifact["horizon_days"]) != horizon
                or artifact.get("order_authority") is not False
                or bool(row.get("order_authority"))
            ):
                raise HorizonModelError("research suite identity differs")
            verified[horizon] = artifact
    except (KeyError, TypeError, ValueError, HorizonModelError):
        return {}, "AVAILABLE", ["FORWARD_RESEARCH_SUITE_REJECTED"], None
    return verified, "AVAILABLE", [], suite_release_id


def score_proxy_model(
    features: Mapping[str, Any],
    *,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate one frozen research proxy without return calibration claims."""

    protocol = dict(spec.get("feature_protocol") or {})
    rules = dict(protocol.get("inputs") or {})
    if not rules:
        raise ValueError("PROXY_FEATURE_PROTOCOL_MISSING")
    model_inputs: dict[str, float] = {}
    weighted = 0.0
    total_weight = 0.0
    for feature_key, raw_rule in rules.items():
        if feature_key not in features or features.get(feature_key) is None:
            raise ValueError(f"PROXY_FEATURE_MISSING:{feature_key}")
        try:
            value = float(features[feature_key])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"PROXY_FEATURE_INVALID:{feature_key}") from exc
        if not math.isfinite(value):
            raise ValueError(f"PROXY_FEATURE_INVALID:{feature_key}")
        rule = dict(raw_rule or {})
        weight = float(rule["weight"])
        lower = float(rule["lower"])
        upper = float(rule["upper"])
        normalized = min(1.0, max(0.0, (value - lower) / (upper - lower)))
        if bool(rule.get("invert")):
            normalized = 1.0 - normalized
        weighted += weight * normalized
        total_weight += weight
        model_inputs[str(feature_key)] = value
    if total_weight <= 0:
        raise ValueError("PROXY_FEATURE_WEIGHT_SUM_INVALID")
    intercept = float(protocol.get("intercept") or 0.0)
    score = min(1.0, max(0.0, intercept + weighted / total_weight))
    return {
        "score": round(score, 8),
        "model_inputs": dict(sorted(model_inputs.items())),
        "feature_protocol_hash": str(spec["feature_protocol_hash"]),
        "scorer_source_hash": str(spec["scorer_source_hash"]),
        "model_artifact_hash": str(spec["model_artifact_hash"]),
        "prediction_kind": "PROXY_SCORE",
        "order_authority": False,
    }


def materialize_proxy_horizon_contracts(
    repository: ShadowIntelligenceRepository,
    calendar_engine: Engine,
    *,
    config: Mapping[str, Any],
    evaluated_at: datetime,
) -> dict[str, Any]:
    """Materialize current OOS models, failing over to frozen proxies.

    The historical name remains a compatibility surface for the scheduled
    worker.  Runtime selection itself is server-side and evidence backed.
    """

    source_rows = repository.latest_forecast_rows()
    if not source_rows:
        return {
            "status": "EMPTY",
            "source_forecast_count": 0,
            "inserted_count": 0,
            "existing_count": 0,
            "blockers": ["NO_COMPLETED_FORECAST_RUN"],
            "order_authority": False,
        }
    proxy_specs = _model_specs(config)
    real_specs = _trainable_model_specs(config)
    decision_dates = {
        (
            row.get("requested_as_of")
            or _aware(row["decision_at"]).date()
        )
        for row in source_rows
    }
    if len(decision_dates) != 1:
        raise RuntimeError("SHADOW_SOURCE_RUN_HAS_MIXED_DECISION_DATES")
    decision_date = next(iter(decision_dates))
    if not isinstance(decision_date, date):
        decision_date = date.fromisoformat(str(decision_date))
    run_uids = {str(row.get("run_uid") or "") for row in source_rows}
    source_config_hashes = {
        str(row.get("config_hash") or "") for row in source_rows
    }
    decision_instants = {
        _aware(row["decision_at"]).astimezone(timezone.utc)
        for row in source_rows
    }
    if len(run_uids) != 1 or "" in run_uids:
        raise RuntimeError("SHADOW_SOURCE_FORECAST_RUN_NOT_UNIQUE")
    if len(source_config_hashes) != 1 or "" in source_config_hashes:
        raise RuntimeError("SHADOW_SOURCE_RUN_CONFIG_HASH_NOT_UNIQUE")
    if len(decision_instants) != 1:
        raise RuntimeError("SHADOW_SOURCE_RUN_DECISION_CLOCK_NOT_UNIQUE")
    decision_as_of = next(iter(decision_instants))
    if decision_as_of.astimezone(MARKET_TIMEZONE).date() != decision_date:
        raise RuntimeError("SHADOW_SOURCE_RUN_DECISION_SESSION_MISMATCH")
    source_config_hash = next(iter(source_config_hashes))
    future_dates = _future_trade_dates(
        calendar_engine,
        after=decision_date,
        count=max(item["horizon_days"] for item in proxy_specs) + 1,
    )
    if len(future_dates) < 21:
        return {
            "status": "COLLECTING",
            "source_forecast_count": len(source_rows),
            "inserted_count": 0,
            "existing_count": 0,
            "blockers": ["FUTURE_TRADE_CALENDAR_INCOMPLETE"],
            "order_authority": False,
        }

    by_strategy_stock: dict[tuple[str, str], dict[str, Any]] = {}
    for row in source_rows:
        key = (str(row["strategy_key"]), str(row["stock_code"]))
        existing = by_strategy_stock.get(key)
        if existing is None or float(row["raw_score"]) > float(existing["raw_score"]):
            by_strategy_stock[key] = dict(row)
    rows_to_save: list[tuple[HorizonForecastContract, str]] = []
    blockers: list[str] = []
    runtime_selection: dict[str, dict[str, Any]] = {}
    real_contract_count = 0
    blocked_research_contract_count = 0
    proxy_contract_count = 0
    (
        suite_artifacts,
        registry_status,
        suite_reason_codes,
        runtime_suite_release_id,
    ) = _verified_runtime_suite(
        repository,
        specs=real_specs,
        decision_as_of=decision_as_of,
        source_config_hash=source_config_hash,
    )
    runtime_suite_authority = (
        "RELEASED_SHADOW" if suite_artifacts else "FROZEN_PROXY_FALLBACK"
    )
    if not suite_artifacts:
        (
            research_artifacts,
            research_registry_status,
            research_reason_codes,
            research_suite_release_id,
        ) = _verified_forward_shadow_research_suite(
            repository,
            specs=real_specs,
            decision_as_of=decision_as_of,
            source_config_hash=source_config_hash,
        )
        if research_artifacts:
            suite_artifacts = research_artifacts
            registry_status = research_registry_status
            suite_reason_codes = [
                "HISTORICAL_GATE_BLOCK_FORWARD_RESEARCH_ONLY"
            ]
            runtime_suite_release_id = research_suite_release_id
            runtime_suite_authority = "BLOCKED_GATE_FORWARD_RESEARCH"
        else:
            registry_status = (
                "UNAVAILABLE"
                if "UNAVAILABLE" in {
                    registry_status, research_registry_status,
                }
                else "AVAILABLE"
            )
            suite_reason_codes = list(dict.fromkeys([
                *suite_reason_codes,
                *research_reason_codes,
            ]))
    real_candidates_by_horizon: dict[int, dict[str, dict[str, Any]]] = {}
    real_candidate_blocks_by_horizon: dict[int, list[dict[str, Any]]] = {
        horizon: [] for horizon in (1, 5, 20)
    }
    if suite_artifacts:
        try:
            for proxy_spec in proxy_specs:
                horizon = int(proxy_spec["horizon_days"])
                label = str(proxy_spec["label"])
                artifact = suite_artifacts[horizon]
                real_spec = real_specs[horizon]
                candidates: dict[str, dict[str, Any]] = {}
                for strategy_key in real_spec["source_strategy_keys"]:
                    for (
                        observed_strategy,
                        stock_code,
                    ), row in by_strategy_stock.items():
                        if observed_strategy != strategy_key:
                            continue
                        features = row.get("features")
                        if not isinstance(features, Mapping):
                            blockers.append(
                                f"{label}:{stock_code}:FEATURE_SNAPSHOT_INVALID"
                            )
                            continue
                        try:
                            model_inputs, imputed = _artifact_model_inputs(
                                artifact,
                                features,
                            )
                        except _CandidateFeatureBlocked as exc:
                            blocker = (
                                f"{label}:{stock_code}:{exc.reason_code}"
                            )
                            blockers.append(blocker)
                            real_candidate_blocks_by_horizon[horizon].append({
                                "stock_code": stock_code,
                                "source_strategy_key": observed_strategy,
                                "reason_code": exc.reason_code,
                                "observed_history_sessions": (
                                    exc.observed_history_sessions
                                ),
                                "required_history_sessions": (
                                    exc.required_history_sessions
                                ),
                            })
                            continue
                        if (
                            runtime_suite_authority
                            == "BLOCKED_GATE_FORWARD_RESEARCH"
                            and imputed
                        ):
                            reason = (
                                "FORWARD_RESEARCH_FEATURE_IMPUTATION_FORBIDDEN"
                            )
                            blockers.append(f"{label}:{stock_code}:{reason}")
                            real_candidate_blocks_by_horizon[horizon].append({
                                "stock_code": stock_code,
                                "source_strategy_key": observed_strategy,
                                "reason_code": reason,
                                "imputed_feature_keys": list(imputed),
                            })
                            continue
                        prediction = predict_horizon_artifact(
                            artifact,
                            model_inputs,
                        )
                        scored_row = {
                            **row,
                            "prediction": prediction,
                            "model_inputs": model_inputs,
                            "imputed_feature_keys": imputed,
                        }
                        current = candidates.get(stock_code)
                        if (
                            current is None
                            or float(prediction.score)
                            > float(current["prediction"].score)
                        ):
                            candidates[stock_code] = scored_row
                real_candidates_by_horizon[horizon] = candidates
        except (KeyError, TypeError, ValueError, HorizonModelError):
            # Prediction is suite-atomic too.  If one real member cannot score,
            # discard every real member and recompute all three frozen proxies.
            suite_artifacts = {}
            real_candidates_by_horizon = {}
            real_candidate_blocks_by_horizon = {
                horizon: [] for horizon in (1, 5, 20)
            }
            runtime_suite_release_id = None
            runtime_suite_authority = "FROZEN_PROXY_FALLBACK"
            suite_reason_codes = ["VERIFIED_ARTIFACT_SUITE_PREDICTION_FAILED"]
    for proxy_spec in proxy_specs:
        horizon = int(proxy_spec["horizon_days"])
        label = str(proxy_spec["label"])
        artifact = suite_artifacts.get(horizon)
        reason_codes = list(suite_reason_codes)
        candidates = dict(real_candidates_by_horizon.get(horizon) or {})
        if artifact is None:
            for code in reason_codes:
                blockers.append(f"{label}:{code}")
            for strategy_key in proxy_spec["source_strategy_keys"]:
                for (
                    observed_strategy,
                    stock_code,
                ), row in by_strategy_stock.items():
                    if observed_strategy != strategy_key:
                        continue
                    features = row.get("features")
                    if not isinstance(features, Mapping):
                        blockers.append(
                            f"{label}:{stock_code}:FEATURE_SNAPSHOT_INVALID"
                        )
                        continue
                    try:
                        proxy = score_proxy_model(features, spec=proxy_spec)
                    except ValueError as exc:
                        blockers.append(f"{label}:{stock_code}:{exc}")
                        continue
                    scored_row = {
                        **row,
                        "prediction": proxy,
                        "model_inputs": dict(proxy["model_inputs"]),
                        "imputed_feature_keys": (),
                    }
                    current = candidates.get(stock_code)
                    if (
                        current is None
                        or float(proxy["score"])
                        > float(current["prediction"]["score"])
                    ):
                        candidates[stock_code] = scored_row
        blocked_research_artifact = (
            artifact is not None
            and runtime_suite_authority == "BLOCKED_GATE_FORWARD_RESEARCH"
        )
        selected_kind = (
            PredictionKind.CALIBRATED_OOS
            if artifact is not None and not blocked_research_artifact
            else PredictionKind.PROXY_SCORE
        )
        selected_spec: Mapping[str, Any] = (
            artifact if artifact is not None else proxy_spec
        )
        calibration_evidence = (
            _artifact_calibration_evidence(artifact)
            if artifact is not None and not blocked_research_artifact
            else None
        )
        imputed_union: set[str] = set()
        for stock_code, row in sorted(candidates.items()):
            source_forecast_id = str(row["forecast_id"])
            source_snapshot, selection_evidence = _source_contract_evidence(
                row,
                stock_code=stock_code,
            )
            prediction = row["prediction"]
            artifact_hash = (
                str(artifact["artifact_hash"])
                if artifact is not None
                else str(proxy_spec["model_artifact_hash"])
            )
            forecast_id = hashlib.sha256(
                (
                    f"{source_forecast_id}|{selected_spec['model_key']}|"
                    f"T+{horizon}|{artifact_hash}"
                ).encode("utf-8")
            ).hexdigest()
            if artifact is not None:
                score = float(prediction.score)
                expected_return = (
                    None
                    if blocked_research_artifact
                    else float(prediction.expected_return_net_pct)
                )
                probability_positive = (
                    None
                    if blocked_research_artifact
                    else float(prediction.probability_positive)
                )
                feature_protocol_hash = str(
                    prediction.feature_protocol_hash
                )
                execution = dict(artifact["execution_feasibility"])
                cost_assumption = float(execution["cost_assumption_pct"])
                cost_model_version = str(execution["cost_model_version"])
                if blocked_research_artifact:
                    blocked_research_contract_count += 1
                else:
                    real_contract_count += 1
            else:
                score = float(prediction["score"])
                expected_return = None
                probability_positive = None
                feature_protocol_hash = str(
                    prediction["feature_protocol_hash"]
                )
                cost_assumption = float(
                    proxy_spec.get("cost_assumption_pct", 0.0)
                )
                cost_model_version = str(proxy_spec["cost_model_version"])
                proxy_contract_count += 1
            imputed_feature_keys = (
                ()
                if blocked_research_artifact
                else tuple(row["imputed_feature_keys"])
            )
            imputed_union.update(imputed_feature_keys)
            contract = HorizonForecastContract(
                forecast_id=forecast_id,
                run_uid=str(row["run_uid"]),
                stock_code=stock_code,
                model_key=str(selected_spec["model_key"]),
                model_version=str(selected_spec["model_version"]),
                source_strategy_key=str(row["strategy_key"]),
                source_forecast_hash=_canonical_hash(source_snapshot),
                source_evidence=source_snapshot,
                decision_result_hash=str(row["decision_result_hash"]),
                feature_protocol_hash=feature_protocol_hash,
                model_artifact_hash=artifact_hash,
                model_inputs=dict(row["model_inputs"]),
                selection_status=str(row["selection_status"]),
                selection_reason_code=str(row["selection_reason_code"]),
                selection_evidence_hash=_canonical_hash(selection_evidence),
                selection_evidence=selection_evidence,
                horizon_days=horizon,
                prediction_kind=selected_kind,
                decision_as_of=_aware(row["decision_at"]),
                feature_as_of=_aware(row["feature_time"]),
                decision_session_date=decision_date,
                entry_trade_date=future_dates[0],
                earliest_exit_trade_date=future_dates[1],
                outcome_matures_on=future_dates[horizon],
                entry_session_sequence=1,
                earliest_exit_session_sequence=2,
                outcome_maturity_session_sequence=1 + horizon,
                score=score,
                expected_return_net_pct=expected_return,
                probability_positive=probability_positive,
                cost_assumption_pct=cost_assumption,
                cost_model_version=cost_model_version,
                calibration_evidence=calibration_evidence,
                imputed_feature_keys=imputed_feature_keys,
            )
            rows_to_save.append((contract, source_forecast_id))
        runtime_selection[label] = {
            "status": (
                (
                    "BLOCKED_GATE_FORWARD_SHADOW"
                    if runtime_suite_authority
                    == "BLOCKED_GATE_FORWARD_RESEARCH"
                    else "REAL_OOS_MODEL"
                )
                if artifact is not None
                else "PROXY_FALLBACK"
            ),
            "registry_status": registry_status,
            "prediction_kind": selected_kind.value,
            "model_key": str(selected_spec["model_key"]),
            "model_version": str(selected_spec["model_version"]),
            "suite_release_id": (
                runtime_suite_release_id if artifact is not None else None
            ),
            "suite_authority": runtime_suite_authority,
            "binding_protocol": (
                FORWARD_SHADOW_BINDING_PROTOCOL
                if artifact is not None
                else None
            ),
            "promotion_eligible": False,
            "artifact_hash": (
                str(artifact["artifact_hash"])
                if artifact is not None
                else str(proxy_spec["model_artifact_hash"])
            ),
            "artifact_valid_until": (
                str(artifact["valid_until"])
                if artifact is not None
                else None
            ),
            "artifact_gate_status": (
                str(artifact["gate"]["status"])
                if artifact is not None
                else "NOT_APPLICABLE"
            ),
            "reason_codes": list(reason_codes),
            "contract_count": len(candidates),
            "blocked_candidate_count": len(
                real_candidate_blocks_by_horizon[horizon]
            ),
            "blocked_candidates": list(
                real_candidate_blocks_by_horizon[horizon]
            ),
            "imputed_feature_keys": sorted(imputed_union),
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    saved = repository.save_horizon_contracts(
        rows_to_save,
        created_at=evaluated_at,
    )
    return {
        **saved,
        "status": "READY" if rows_to_save else "COLLECTING",
        "source_forecast_count": len(source_rows),
        "materialized_contract_count": len(rows_to_save),
        "real_oos_contract_count": real_contract_count,
        "blocked_gate_forward_research_contract_count": (
            blocked_research_contract_count
        ),
        "proxy_fallback_contract_count": proxy_contract_count,
        "runtime_model_selection": runtime_selection,
        "runtime_suite_release_id": runtime_suite_release_id,
        "runtime_suite_authority": runtime_suite_authority,
        "artifact_registry_status": (
            "UNAVAILABLE"
            if any(
                item["registry_status"] == "UNAVAILABLE"
                for item in runtime_selection.values()
            )
            else "AVAILABLE"
        ),
        "blockers": blockers,
        "order_authority": False,
    }


def materialize_horizon_outcomes(
    repository: ShadowIntelligenceRepository,
    market_engine: Engine,
    *,
    evaluated_at: datetime,
) -> dict[str, Any]:
    """Create one cost-inclusive label from each contract's exact sessions."""

    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("evaluated_at must include a timezone")
    evaluation_date = evaluated_at.astimezone(MARKET_TIMEZONE).date()
    # Daily bars are mutable during their own Shanghai session.  A one-day
    # knowledge lag makes the label immutable without trusting an intraday bar
    # or an unverified "final" flag.
    latest_immutable_date = evaluation_date - timedelta(days=1)
    contracts = repository.mature_horizon_contracts(
        evaluation_date=latest_immutable_date
    )
    if not contracts:
        observed_contracts = repository.horizon_contracts(
            evaluation_date=evaluation_date,
            limit=10000,
        )
        verified_or_quarantined = sum(
            1 for item in observed_contracts
            if str(item.get("derived_contract_status"))
            in {"MATURED_VERIFIED", "QUARANTINED"}
        )
        return {
            "status": (
                "READY" if verified_or_quarantined else "COLLECTING"
            ),
            "mature_contract_count": 0,
            "inserted_count": 0,
            "existing_count": 0,
            "unresolved": [],
            "blockers": (
                [] if verified_or_quarantined
                else ["NO_MATURE_CONTRACT_OUTCOME"]
            ),
            "order_authority": False,
        }
    codes = sorted({str(item["stock_code"]) for item in contracts})
    start_date = min(_date_value(item["entry_trade_date"]) for item in contracts)
    end_date = max(_date_value(item["outcome_matures_on"]) for item in contracts)
    statement = text(
        """
        SELECT stock_code, trade_date, open, high, low, close,
               pre_close, amount, etl_sync_at, data_source,
               quality_status, source_time, received_at, batch_id,
               data_version, permission_status
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND stock_code IN :codes
          AND trade_date BETWEEN :start_date AND :end_date
        ORDER BY stock_code, trade_date
        """
    ).bindparams(bindparam("codes", expanding=True))
    with market_engine.connect() as connection:
        raw_bars = connection.execute(
            statement,
            {
                "codes": codes,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().all()
        calendar_rows = connection.execute(
            text(
                """
                SELECT trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date BETWEEN :start_date AND :end_date
                ORDER BY trade_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).scalars().all()
    calendar = tuple(_date_value(item) for item in calendar_rows)
    bars_by_key: dict[tuple[str, date], dict[str, Any]] = {}
    duplicate_keys: set[tuple[str, date]] = set()
    for raw in raw_bars:
        item = dict(raw)
        key = (str(item["stock_code"]), _date_value(item["trade_date"]))
        if key in bars_by_key:
            duplicate_keys.add(key)
        bars_by_key[key] = item

    outcomes: list[tuple[HorizonOutcomeEvidence, Mapping[str, Any]]] = []
    unresolved: list[dict[str, str]] = []
    for contract in contracts:
        contract_id = str(contract["contract_id"])
        stock_code = str(contract["stock_code"])
        horizon = int(contract["horizon_days"])
        entry_date = _date_value(contract["entry_trade_date"])
        exit_date = _date_value(contract["outcome_matures_on"])
        sessions = tuple(
            item for item in calendar if entry_date <= item <= exit_date
        )
        expected_dates = (horizon + 1)
        if (
            len(sessions) != expected_dates
            or not sessions
            or sessions[0] != entry_date
            or sessions[-1] != exit_date
            or _date_value(contract["earliest_exit_trade_date"])
            != sessions[1]
            or int(contract["entry_session_sequence"]) != 1
            or int(contract["earliest_exit_session_sequence"]) != 2
            or int(contract["outcome_maturity_session_sequence"])
            != horizon + 1
        ):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "FROZEN_EXCHANGE_SESSION_SEQUENCE_MISMATCH",
            })
            continue
        keys = tuple((stock_code, item) for item in sessions)
        if any(key in duplicate_keys for key in keys):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "MARKET_BAR_DUPLICATE",
            })
            continue
        if any(key not in bars_by_key for key in keys):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "FROZEN_MARKET_BAR_MISSING",
            })
            continue
        bars = [bars_by_key[key] for key in keys]
        try:
            normalized_bars = [
                {
                    "trade_date": sessions[index].isoformat(),
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "pre_close": (
                        float(item["pre_close"])
                        if item.get("pre_close") is not None
                        else None
                    ),
                    "amount": (
                        float(item["amount"])
                        if item.get("amount") is not None
                        else None
                    ),
                    "etl_sync_at": str(item.get("etl_sync_at") or ""),
                    "data_source": str(item.get("data_source") or ""),
                    "quality_status": str(
                        item.get("quality_status") or ""
                    ),
                    "source_time": str(item.get("source_time") or ""),
                    "received_at": str(item.get("received_at") or ""),
                    "batch_id": str(item.get("batch_id") or ""),
                    "data_version": str(item.get("data_version") or ""),
                    "permission_status": str(
                        item.get("permission_status") or ""
                    ),
                }
                for index, item in enumerate(bars)
            ]
        except (TypeError, ValueError, OverflowError):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "MARKET_BAR_INVALID",
            })
            continue
        if any(
            item["data_source"] != "gj_big_qmt_inner"
            or item["quality_status"] != "QMT_ATTESTED"
            or not item["source_time"]
            or not item["received_at"]
            or not item["batch_id"]
            or not item["data_version"]
            for item in normalized_bars
        ):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "QMT_DAILY_BAR_ATTESTATION_REQUIRED",
            })
            continue
        entry_price = normalized_bars[0]["open"]
        exit_price = normalized_bars[-1]["close"]
        numeric_values = [
            float(item[field])
            for item in normalized_bars
            for field in ("open", "high", "low", "close")
        ] + [
            float(item[field])
            for item in normalized_bars
            for field in ("pre_close", "amount")
            if item[field] is not None
        ]
        if (
            entry_price <= 0
            or exit_price <= 0
            or not all(math.isfinite(value) for value in numeric_values)
            or any(not item["etl_sync_at"] for item in normalized_bars)
            or any(
                item[field] <= 0
                for item in normalized_bars
                for field in ("open", "high", "low", "close")
            )
        ):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "MARKET_BAR_NON_POSITIVE_PRICE",
            })
            continue
        if any(
            item["high"] < max(item["open"], item["close"])
            or item["low"] > min(item["open"], item["close"])
            or item["high"] < item["low"]
            for item in normalized_bars
        ):
            unresolved.append({
                "contract_id": contract_id,
                "reason_code": "MARKET_BAR_OHLC_INCONSISTENT",
            })
            continue
        cost = float(contract["cost_assumption_pct"])
        gross = (exit_price / entry_price - 1.0) * 100.0
        preclose_tolerance_pct = 0.05
        corporate_action_detected = any(
            item["pre_close"] is None
            or item["pre_close"] <= 0
            or abs(
                float(item["pre_close"])
                / float(normalized_bars[index - 1]["close"])
                - 1.0
            )
            * 100.0
            > preclose_tolerance_pct
            for index, item in enumerate(normalized_bars[1:], start=1)
        )
        outcome_status = (
            "QUARANTINED" if corporate_action_detected
            else "MATURED_VERIFIED"
        )
        qmt_projection = [
            {
                "trade_date": item["trade_date"],
                "data_source": item["data_source"],
                "quality_status": item["quality_status"],
                "source_time": item["source_time"],
                "received_at": item["received_at"],
                "batch_id": item["batch_id"],
                "data_version": item["data_version"],
            }
            for item in normalized_bars
        ]
        evidence_payload = {
            "schema_version": "probiga.trading-v3.horizon-market-evidence.v1",
            "contract_id": contract_id,
            "contract_hash": str(contract["contract_hash"]),
            "stock_code": stock_code,
            "horizon_days": horizon,
            "entry_trade_date": entry_date.isoformat(),
            "exit_trade_date": exit_date.isoformat(),
            "exchange_timezone": "Asia/Shanghai",
            "market_data_source": "sm_stock_kline.daily.unadjusted",
            "k_type": 1,
            "adjust_type": 0,
            "cost_model_version": str(contract["cost_model_version"]),
            "realized_cost_pct": cost,
            "execution_feasibility": "UNVERIFIED_RESEARCH",
            "qmt_attestation": {
                "protocol": QMT_OUTCOME_ATTESTATION_PROTOCOL,
                "status": "QMT_ATTESTED",
                "provider": "gj_big_qmt_inner",
                "attested_bar_count": len(normalized_bars),
                "attestation_hash": _canonical_hash(qmt_projection),
            },
            "execution_assumptions": {
                "entry": "NEXT_SESSION_OPEN_REFERENCE_ONLY",
                "exit": "FROZEN_MATURITY_CLOSE_REFERENCE_ONLY",
                "limit_state_checked": False,
                "suspension_checked": False,
                "capacity_checked": False,
                "fill_verified": False,
            },
            "knowledge_cutoff": evaluated_at.isoformat(),
            "same_session_outcome_forbidden": True,
            "corporate_action_guard": {
                "method": "PRE_CLOSE_VS_PRIOR_UNADJUSTED_CLOSE",
                "tolerance_pct": preclose_tolerance_pct,
                "detected": corporate_action_detected,
            },
            "bars": normalized_bars,
        }
        evidence_hash = _canonical_hash(evidence_payload)
        outcomes.append((
            HorizonOutcomeEvidence(
                contract_id=contract_id,
                contract_hash=str(contract["contract_hash"]),
                stock_code=stock_code,
                horizon_days=horizon,
                entry_trade_date=entry_date,
                exit_trade_date=exit_date,
                entry_price=entry_price,
                exit_price=exit_price,
                gross_return_pct=gross,
                realized_cost_pct=cost,
                realized_net_return_pct=gross - cost,
                realized_mae_pct=min(
                    (float(item["low"]) / entry_price - 1.0) * 100.0
                    for item in normalized_bars
                ),
                realized_mfe_pct=max(
                    (float(item["high"]) / entry_price - 1.0) * 100.0
                    for item in normalized_bars
                ),
                bar_count=len(normalized_bars),
                cost_model_version=str(contract["cost_model_version"]),
                market_data_source="sm_stock_kline.daily.unadjusted",
                market_evidence_hash=evidence_hash,
                execution_feasibility="UNVERIFIED_RESEARCH",
                outcome_status=outcome_status,
                observed_at=evaluated_at,
            ),
            evidence_payload,
        ))
    saved = repository.save_horizon_outcomes(
        outcomes,
        created_at=evaluated_at,
    )
    return {
        **saved,
        "status": (
            "COLLECTING"
            if unresolved
            else (
                "QUARANTINED"
                if any(
                    outcome.outcome_status == "QUARANTINED"
                    for outcome, _ in outcomes
                )
                else "READY"
            )
        ),
        "mature_contract_count": len(contracts),
        "materialized_outcome_count": len(outcomes),
        "unresolved": unresolved,
        "order_authority": False,
    }


def _blocked_gate(
    *,
    release_id: str,
    horizon_days: int,
    evaluated_at: datetime,
    failure_codes: tuple[str, ...],
) -> CalibrationGateDecision:
    timestamp = evaluated_at.astimezone(timezone.utc).isoformat()
    return CalibrationGateDecision(
        status="BLOCK",
        failure_codes=failure_codes,
        release_id=release_id,
        horizon_days=horizon_days,
        evidence_observed_at=timestamp,
        evidence_valid_until=timestamp,
        evaluated_at=timestamp,
        recommended_stage="BLOCKED",
        order_authority=False,
    )


def _calibration_evidence(
    *,
    payload: Mapping[str, Any],
    release_id: str,
    spec: Mapping[str, Any],
) -> ContinuousCalibrationEvidence:
    return ContinuousCalibrationEvidence(
        release_id=release_id,
        model_key=str(spec["model_key"]),
        model_version=str(spec["model_version"]),
        horizon_days=int(spec["horizon_days"]),
        prediction_kind=str(spec["prediction_kind"]),
        matured_sample_count=int(payload["matured_sample_count"]),
        oos_sample_count=int(payload["oos_sample_count"]),
        walk_forward_fold_count=int(payload["walk_forward_fold_count"]),
        direction_rank_correlation=float(payload["direction_rank_correlation"]),
        calibration_mae=float(payload["calibration_mae"]),
        brier_score=float(payload["brier_score"]),
        population_stability_index=float(
            payload["population_stability_index"]
        ),
        net_expectancy_after_cost_pct=float(
            payload["net_expectancy_after_cost_pct"]
        ),
        profit_factor=float(payload["profit_factor"]),
        cost_coverage_ratio=float(payload["cost_coverage_ratio"]),
        observed_at=_aware(payload["observed_at"]),
        valid_until=_aware(payload["valid_until"]),
    )


def evaluate_shadow_releases(
    repository: ShadowIntelligenceRepository,
    *,
    config: Mapping[str, Any],
    evaluated_at: datetime,
) -> dict[str, Any]:
    policy = dict(config.get("continuous_calibration") or {})
    release_policy = dict(config.get("shadow_release") or {})
    if (
        policy.get("automatic_promotion_allowed") is not False
        or policy.get("order_allowed") is not False
        or release_policy.get("automatic_promotion_allowed") is not False
        or release_policy.get("order_allowed") is not False
        or release_policy.get("real_order_allowed") is not False
    ):
        raise ValueError("Shadow release policy must remain fail-closed")
    current_config_hash = _canonical_hash(config)
    rows = []
    for spec in _model_specs(config):
        release_id = repository.release_id(
            model_key=str(spec["model_key"]),
            model_version=str(spec["model_version"]),
            horizon_days=int(spec["horizon_days"]),
        )
        latest = repository.ensure_release(
            model_key=str(spec["model_key"]),
            model_version=str(spec["model_version"]),
            horizon_days=int(spec["horizon_days"]),
            config_hash=current_config_hash,
            occurred_at=evaluated_at,
        )
        if str(latest["current_stage"]) == ReleaseStage.DRAFT.value:
            started = transition_shadow_release(
                ReleaseStage.DRAFT,
                ReleaseEvent.START_SHADOW,
            )
            latest = repository.append_release_transition(
                release_id=release_id,
                transition=started,
                evidence_hash=_canonical_hash({"event": "START_SHADOW"}),
                config_hash=current_config_hash,
                occurred_at=evaluated_at,
            )
        raw_evidence = repository.verified_calibration_evidence(
            release_id=release_id,
            model_key=str(spec["model_key"]),
            model_version=str(spec["model_version"]),
            horizon_days=int(spec["horizon_days"]),
        )
        evidence = None
        if raw_evidence is None:
            preview = repository.calibration_evidence_payload(
                model_key=str(spec["model_key"]),
                model_version=str(spec["model_version"]),
            )
            raw_evidence = {
                "status": "UNVERIFIED_PREVIEW" if preview else "MISSING",
                "model_key": spec["model_key"],
                "model_version": spec["model_version"],
                "horizon_days": spec["horizon_days"],
                "preview": preview,
                "evidence_provenance_status": "UNVERIFIED_PREVIEW",
            }
            gate = _blocked_gate(
                release_id=release_id,
                horizon_days=int(spec["horizon_days"]),
                evaluated_at=evaluated_at,
                failure_codes=(
                    (
                        "CALIBRATION_EVIDENCE_PROVENANCE_UNVERIFIED"
                        if preview
                        else "CALIBRATION_EVIDENCE_MISSING"
                    ),
                    "PREDICTION_IS_PROXY_NOT_CALIBRATED",
                ),
            )
        else:
            try:
                evidence = _calibration_evidence(
                    payload=raw_evidence,
                    release_id=release_id,
                    spec=spec,
                )
                gate = evaluate_continuous_calibration(
                    evidence,
                    policy=policy,
                    evaluated_at=evaluated_at,
                )
                gate = replace(
                    gate,
                    evidence_provenance_status="PERSISTED_VERIFIED",
                )
            except (KeyError, TypeError, ValueError) as exc:
                raw_evidence = {
                    **dict(raw_evidence),
                    "parse_error": str(exc),
                }
                gate = _blocked_gate(
                    release_id=release_id,
                    horizon_days=int(spec["horizon_days"]),
                    evaluated_at=evaluated_at,
                    failure_codes=("CALIBRATION_EVIDENCE_INVALID",),
                )
                evidence = None
        gate_row = repository.save_calibration_gate(
            release_id=release_id,
            model_key=str(spec["model_key"]),
            model_version=str(spec["model_version"]),
            horizon_days=int(spec["horizon_days"]),
            prediction_kind=str(spec["prediction_kind"]),
            decision=gate,
            evidence=evidence,
            raw_evidence=raw_evidence,
            policy=policy,
            evaluated_at=evaluated_at,
        )
        latest = repository.latest_release(release_id)
        assert latest is not None
        transition = enforce_continuous_gate(
            str(latest["current_stage"]),
            gate,
        )
        if str(latest["current_stage"]) != ReleaseStage.RETIRED.value:
            prior_transition = repository.release_transition_for_gate(
                release_id=release_id,
                gate_evaluation_id=str(gate_row["gate_evaluation_id"]),
            )
            if prior_transition is not None:
                latest = repository.latest_release(release_id) or prior_transition
            else:
                latest = repository.append_release_transition(
                    release_id=release_id,
                    transition=transition,
                    evidence_hash=str(gate_row["evidence_hash"]),
                    config_hash=current_config_hash,
                    occurred_at=evaluated_at,
                    gate_evaluation_id=str(gate_row["gate_evaluation_id"]),
                )
        rows.append({
            "release_id": release_id,
            "stage": str(latest["current_stage"]),
            "gate_status": gate.status,
            "failure_codes": list(gate.failure_codes),
            "evidence_provenance_status": (
                gate.evidence_provenance_status
            ),
            "gate_evaluation_id": str(gate_row["gate_evaluation_id"]),
            "learning_run_id": gate_row.get("learning_run_id"),
            "order_authority": False,
        })
    return {
        "status": "BLOCKED" if any(item["gate_status"] == "BLOCK" for item in rows) else "PASS",
        "releases": rows,
        "automatic_promotion_allowed": False,
        "order_authority": False,
    }


def _run_shadow_intelligence_cycle_unlocked(
    primary_engine: Engine,
    calendar_engine: Engine,
    *,
    evaluated_at: datetime,
    repository: ShadowIntelligenceRepository | None = None,
    config: Mapping[str, Any] | None = None,
    lifecycle_adapter: HorizonModelLifecycleAdapter | None = None,
    evidence_store: ImmutableEvidenceStore | None = None,
    run_continuous_calibration: bool = True,
) -> dict[str, Any]:
    """Run the append-only Shadow learning and release evaluation cycle."""

    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("evaluated_at must include a timezone")
    current = _aware(evaluated_at)
    repo = repository or ShadowIntelligenceRepository(primary_engine)
    cfg = dict(config or load_v3_config())
    contracts = materialize_proxy_horizon_contracts(
        repo,
        calendar_engine,
        config=cfg,
        evaluated_at=current,
    )
    outcomes = materialize_horizon_outcomes(
        repo,
        calendar_engine,
        evaluated_at=current,
    )
    learning_policy = dict(cfg.get("continuous_calibration") or {})
    minimum_by_horizon = dict(
        learning_policy.get("minimum_mature_samples") or {}
    )
    if set(map(str, minimum_by_horizon)) != {"1", "5", "20"}:
        raise RuntimeError("CONTINUOUS_CALIBRATION_HORIZONS_INCOMPLETE")
    minimum_values = {
        str(key): int(value)
        for key, value in minimum_by_horizon.items()
    }
    samples = repo.counterfactual_learning_samples(
        evaluation_date=current.astimezone(MARKET_TIMEZONE).date()
    )
    learning = counterfactual_learning_metrics(
        samples,
        minimum_mature_samples=min(minimum_values.values()),
        minimum_mature_samples_by_horizon=minimum_values,
    )
    learning_row = repo.save_learning_run(
        policy=learning_policy,
        evaluation_date=current.astimezone(MARKET_TIMEZONE).date(),
        evaluated_at=current,
    )
    releases = evaluate_shadow_releases(
        repo,
        config=cfg,
        evaluated_at=current,
    )
    lifecycle = (
        run_continuous_calibration_orchestration(
            repo,
            primary_engine,
            calendar_engine,
            config=cfg,
            evaluated_at=current,
            learning_run=learning_row,
            lifecycle_adapter=lifecycle_adapter,
            evidence_store=evidence_store,
        )
        if run_continuous_calibration
        else {
            "status": "DEFERRED",
            "reason_code": "SEPARATE_CONTINUOUS_CALIBRATION_TASK",
            "order_authority": False,
        }
    )
    cycle_status = (
        "EMPTY"
        if str(contracts.get("status") or "") == "EMPTY"
        and str(outcomes.get("status") or "") == "COLLECTING"
        and int(outcomes.get("mature_contract_count") or 0) == 0
        else "COLLECTING"
    )
    return {
        "status": cycle_status,
        "evaluated_at": current.isoformat(),
        "contracts": contracts,
        "outcomes": outcomes,
        "learning": {
            "status": learning["status"],
            "sample_count": learning["overall"]["sample_count"],
            "learning_run_id": learning_row["learning_run_id"],
            "can_activate_model": False,
        },
        "release_gate": releases,
        "continuous_calibration": lifecycle,
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
        "real_order_allowed": False,
    }


def run_shadow_intelligence_cycle(
    primary_engine: Engine,
    calendar_engine: Engine,
    *,
    evaluated_at: datetime,
    repository: ShadowIntelligenceRepository | None = None,
    config: Mapping[str, Any] | None = None,
    lifecycle_adapter: HorizonModelLifecycleAdapter | None = None,
    evidence_store: ImmutableEvidenceStore | None = None,
    lock_timeout_seconds: int = 0,
    run_continuous_calibration: bool = True,
) -> dict[str, Any]:
    """Run one serialized, append-only Shadow learning lifecycle."""

    store = evidence_store or ImmutableEvidenceStore()
    with continuous_cycle_lock(
        primary_engine,
        timeout_seconds=lock_timeout_seconds,
    ):
        store.put(
            "cycle_started",
            {
                "evaluated_at": evaluated_at.isoformat(),
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            created_at=evaluated_at,
        )
        try:
            return _run_shadow_intelligence_cycle_unlocked(
                primary_engine,
                calendar_engine,
                evaluated_at=evaluated_at,
                repository=repository,
                config=config,
                lifecycle_adapter=lifecycle_adapter,
                evidence_store=store,
                run_continuous_calibration=run_continuous_calibration,
            )
        except Exception as exc:
            store.put(
                "cycle_failure",
                {
                    "evaluated_at": evaluated_at.isoformat(),
                    "error_type": type(exc).__name__,
                    "error_code": str(exc),
                    "decision_scope": "RESEARCH_ONLY",
                    "order_authority": False,
                },
                created_at=evaluated_at,
            )
            raise


def run_continuous_model_lifecycle_cycle(
    primary_engine: Engine,
    calendar_engine: Engine,
    *,
    evaluated_at: datetime,
    repository: ShadowIntelligenceRepository | None = None,
    config: Mapping[str, Any] | None = None,
    lifecycle_adapter: HorizonModelLifecycleAdapter | None = None,
    evidence_store: ImmutableEvidenceStore | None = None,
    lock_timeout_seconds: int = 0,
) -> dict[str, Any]:
    """Run artifact discovery/training/release as its own scheduled task."""

    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("evaluated_at must include a timezone")
    current = _aware(evaluated_at)
    repo = repository or ShadowIntelligenceRepository(primary_engine)
    cfg = dict(config or load_v3_config())
    store = evidence_store or ImmutableEvidenceStore()
    with continuous_cycle_lock(
        primary_engine,
        timeout_seconds=lock_timeout_seconds,
    ):
        store.put(
            "model_cycle_started",
            {
                "evaluated_at": current.isoformat(),
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            created_at=current,
        )
        try:
            learning_row = repo.latest_learning_run() or {}
            result = run_continuous_calibration_orchestration(
                repo,
                primary_engine,
                calendar_engine,
                config=cfg,
                evaluated_at=current,
                learning_run=learning_row,
                lifecycle_adapter=lifecycle_adapter,
                evidence_store=store,
            )
            return {
                **result,
                "task_scope": "CONTINUOUS_MODEL_CALIBRATION",
                "order_authority": False,
                "real_order_allowed": False,
            }
        except Exception as exc:
            store.put(
                "model_cycle_failure",
                {
                    "evaluated_at": current.isoformat(),
                    "error_type": type(exc).__name__,
                    "error_code": str(exc),
                    "order_authority": False,
                },
                created_at=current,
            )
            raise


__all__ = [
    "evaluate_shadow_releases",
    "materialize_horizon_outcomes",
    "materialize_proxy_horizon_contracts",
    "run_continuous_model_lifecycle_cycle",
    "run_shadow_intelligence_cycle",
    "score_proxy_model",
]
