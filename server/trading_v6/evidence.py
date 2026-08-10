"""Strict, standard-library-only audit of byte-frozen V6 history."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Mapping


CAMPAIGN_ID = "trading_v6_multi_sleeve_pit_finance_exploratory_20260802"
CANDIDATE_IDS = (
    "regime_expert_trend_pit_finance_l30_v1",
    "regime_expert_reversal_pit_finance_l30_v1",
    "hurdle_trend_pit_finance_l30_v1",
)
FOLD_NAMES = (
    "oos_2022", "oos_2023", "oos_2024", "oos_2025_h1",
    "oos_2025h2_2026h1",
)
BLOCK_REASONS = (
    "OOS_SAMPLE_COUNT_TOO_LOW",
    "OOS_NET_EXPECTANCY_MISSING",
    "OOS_PROFIT_FACTOR_MISSING",
    "OOS_PAYOFF_RATIO_MISSING",
    "PORTFOLIO_TRADE_COUNT_TOO_LOW",
    "PORTFOLIO_NET_EXPECTANCY_MISSING",
    "PORTFOLIO_PROFIT_FACTOR_MISSING",
    "PORTFOLIO_PAYOFF_RATIO_MISSING",
    "PORTFOLIO_NET_PROFIT_NOT_POSITIVE",
    "POSITIVE_OUTER_FOLD_COUNT_TOO_LOW",
    "CALIBRATION_DIRECTION_FAILED_IN_OUTER_FOLD",
)
EXPECTED_BASE_EPISODES = {
    CANDIDATE_IDS[0]: 16_773,
    CANDIDATE_IDS[1]: 174,
    CANDIDATE_IDS[2]: 16_773,
}
EXPECTED_REGISTRATION_HASHES = {
    CANDIDATE_IDS[0]: "d6925735021a9fbaf9732396f4515aec930866a4a85e4fb906602c72c54cd9c7",
    CANDIDATE_IDS[1]: "29beafc4280f9a0ee03d05129b7066220785cb80a07fb6005d0bd1549fca7662",
    CANDIDATE_IDS[2]: "c5482ce11a2373ec26047574202fecedf549ba9fd918c1d89c98a4c34117bf6a",
}
EXPECTED_RESULT_HASHES = {
    CANDIDATE_IDS[0]: "cae1a59a7607740741aa2022f05a3207750135770e4cfe215d9a00d6d2036dc4",
    CANDIDATE_IDS[1]: "aa92f1aa4111b1981da3b206d56f301c9ee8c245384e2a5c2592c2994f51c4af",
    CANDIDATE_IDS[2]: "5ebf0e845036d73262262d120e6fe97a25da19dde8973f25265fdc2e66e02080",
}
ACTIVATION_KEYS = frozenset(
    {
        "activation_eligible", "paper_eligible", "production_eligible",
        "actionable_output_allowed", "paper_orders_allowed", "real_orders_allowed",
        "real_order_submission", "confirmatory_claim_allowed",
    }
)


class V6EvidenceError(ValueError):
    """Raised when historical V6 evidence drifts or overclaims."""


@dataclass(frozen=True, slots=True)
class CandidateEvidenceAudit:
    candidate_id: str
    base_episode_count: int
    raw_closed_trade_count: int
    final_trade_count: int
    calibration_valid_folds: tuple[str, ...]
    registration_contract_sha256: str
    result_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "status": "BLOCK",
            "recorded_gate_status": "BLOCK",
            "base_episode_count": self.base_episode_count,
            "raw_closed_trade_count": self.raw_closed_trade_count,
            "final_trade_count": self.final_trade_count,
            "positive_outer_folds": 0,
            "outer_fold_count": 5,
            "calibration_valid_folds": list(self.calibration_valid_folds),
            "block_reasons": list(BLOCK_REASONS),
            "registration_contract_sha256": self.registration_contract_sha256,
            "result_sha256": self.result_sha256,
            "verified_from_recorded_trades": {
                "raw_closed_trade_metrics": True,
                "final_empty_portfolio_metrics": True,
                "calibration_direction_from_recorded_buckets": True,
            },
            "recorded_but_not_independently_recomputed": {
                "raw_validation_metrics": True,
                "accepted_validation_metrics": True,
                "portfolio_net_profit_and_drawdown": True,
                "pit_finance_source_rows": True,
                "model_training_and_predictions": True,
            },
            "activation_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class V6EvidenceAudit:
    campaign_sha256: str
    artifact_sha256: str
    runner_sha256: str
    log_sha256: tuple[tuple[str, str], ...]
    research_contract_sha256: str
    candidates: tuple[CandidateEvidenceAudit, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "probiga.trading-v6-frozen-evidence-audit.v1",
            "campaign_id": CAMPAIGN_ID,
            "status": "BLOCK",
            "lifecycle_status": "RESEARCH_ONLY",
            "campaign_sha256": self.campaign_sha256,
            "artifact_sha256": self.artifact_sha256,
            "historical_runner_sha256": self.runner_sha256,
            "log_sha256": dict(self.log_sha256),
            "research_contract_sha256": self.research_contract_sha256,
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "historical_algorithm": {
                "hurdle_probability_claim": "LEGACY_CLIPPED_LINEAR_SCORE_NOT_TRUE_PROBABILITY",
                "pit_finance_time_precision": "LEGACY_DATE_LEVEL_NOT_INTRADAY_CERTIFIED",
                "current_source_reproducible": False,
                "raw_data_snapshot_bound": False,
                "runtime_environment_bound": False,
            },
            "multiple_testing": {
                "declared_iterations": 2000,
                "performed_iterations_inferred_from_bound_early_exit": 0,
                "performed_iteration_counter_recorded": False,
                "adjusted_p_values": {candidate: None for candidate in CANDIDATE_IDS},
                "complete": False,
            },
            "stress_matrix": {
                "expected_scenarios_per_candidate": 36,
                "expected_scenarios_total": 108,
                "recorded_scenarios": 0,
                "complete": False,
            },
            "governance_status": (
                "RECORDED_GOVERNED_EXPLORATORY_BYTE_FROZEN_NOT_RECOMPUTED"
            ),
            "completed_log_status": "RECORDED_COMPLETE_WITH_EMPTY_STDERR",
            "failed_attempt_log_status": "HISTORICAL_FATAL_RUNTIME_ERROR_RECORDED",
            "prospective_evidence": {
                "minimum_trading_days": 120,
                "minimum_mature_portfolio_trades": 80,
                "recorded_trading_days": 0,
                "recorded_mature_portfolio_trades": 0,
                "status": "NOT_EVALUATED",
                "counts_as_pass": False,
            },
            "registered_model_count": 0,
            "forecasts": [],
            "actions": [],
            "execution_intents": [],
            "activation_eligible": False,
            "paper_eligible": False,
            "production_eligible": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
        }


def audit_v6_evidence_bytes(
    campaign_bytes: bytes,
    artifact_bytes: bytes,
    runner_bytes: bytes,
    log_bytes: Mapping[str, bytes],
    *,
    expected_campaign_sha256: str,
    expected_artifact_sha256: str,
    expected_runner_sha256: str,
    expected_log_sha256: Mapping[str, str],
) -> V6EvidenceAudit:
    """Audit frozen evidence without filesystem, DB, network, or old imports."""

    campaign_sha = _require_hash(campaign_bytes, expected_campaign_sha256, "campaign")
    artifact_sha = _require_hash(artifact_bytes, expected_artifact_sha256, "artifact")
    runner_sha = _require_hash(runner_bytes, expected_runner_sha256, "runner")
    required_logs = {
        "failed_stdout", "failed_stderr", "completed_stdout", "completed_stderr"
    }
    if set(log_bytes) != required_logs or set(expected_log_sha256) != required_logs:
        raise V6EvidenceError("historical log set differs")
    log_hashes = tuple(
        (name, _require_hash(log_bytes[name], expected_log_sha256[name], name))
        for name in sorted(required_logs)
    )

    campaign = _strict_json(campaign_bytes, "campaign")
    artifact = _strict_json(artifact_bytes, "artifact")
    _reject_true_activation(campaign, "campaign")
    _reject_true_activation(artifact, "artifact")
    _exact(campaign, "schema_version", "probiga.trading-v6-multi-sleeve-pit-finance-research.v1")
    _exact(artifact, "schema_version", campaign.get("artifact_schema_version"))
    _exact(campaign, "campaign_id", CAMPAIGN_ID)
    _exact(artifact, "campaign_id", CAMPAIGN_ID)
    contract_hash = _canonical_sha256(campaign)
    _exact(artifact, "research_contract_sha256", contract_hash)
    _validate_execution_boundary(campaign, artifact)
    _validate_evidence_status(campaign, artifact)

    candidate_control = _mapping(
        campaign.get("candidate_control"), "candidate_control"
    )
    candidates = candidate_control.get("candidates")
    if not isinstance(candidates, list):
        raise V6EvidenceError("campaign candidates must be a list")
    candidate_ids = tuple(_text(item.get("id"), "candidate id") for item in candidates)
    if candidate_ids != CANDIDATE_IDS:
        raise V6EvidenceError("campaign candidate order differs")
    if artifact.get("candidate_order") != list(CANDIDATE_IDS):
        raise V6EvidenceError("artifact candidate order differs")
    if artifact.get("ranking") != list(CANDIDATE_IDS):
        raise V6EvidenceError("artifact ranking differs")
    results = _mapping(artifact.get("results"), "results")
    build_report = _mapping(artifact.get("candidate_build_report"), "candidate_build_report")
    if tuple(results) != CANDIDATE_IDS or tuple(build_report) != CANDIDATE_IDS:
        raise V6EvidenceError("artifact candidate mappings differ")
    folds = campaign.get("outer_folds")
    if not isinstance(folds, list) or tuple(item.get("name") for item in folds) != FOLD_NAMES:
        raise V6EvidenceError("campaign outer folds differ")

    multiple = _mapping(artifact.get("multiple_testing"), "multiple_testing")
    _exact(multiple, "method", "calendar_month_block_bootstrap_max_t")
    _exact(multiple, "iterations", 2000)
    adjusted = _mapping(multiple.get("adjusted_p_values"), "adjusted_p_values")
    if tuple(adjusted) != CANDIDATE_IDS or any(value is not None for value in adjusted.values()):
        raise V6EvidenceError("multiple-testing adjusted p-values differ")
    if "stress_test_matrix" in artifact:
        raise V6EvidenceError("legacy artifact unexpectedly claims a stress matrix")
    _exact(artifact, "real_order_submission", False)

    audits = tuple(
        _audit_candidate(candidate_id, build_report[candidate_id], results[candidate_id])
        for candidate_id in CANDIDATE_IDS
    )
    if sum(candidate.final_trade_count for candidate in audits) != 0:
        raise V6EvidenceError("bootstrap early-exit inference requires zero final trades")
    _validate_governance(artifact, results)
    _validate_logs(log_bytes, artifact)

    result = V6EvidenceAudit(
        campaign_sha256=campaign_sha,
        artifact_sha256=artifact_sha,
        runner_sha256=runner_sha,
        log_sha256=log_hashes,
        research_contract_sha256=contract_hash,
        candidates=audits,
    )
    json.dumps(result.as_dict(), allow_nan=False, sort_keys=True)
    return result


def _audit_candidate(
    candidate_id: str,
    build_report: Any,
    result: Any,
) -> CandidateEvidenceAudit:
    build = _mapping(build_report, "candidate build report")
    item = _mapping(result, "candidate result")
    _exact(build, "labeled_episode_count", EXPECTED_BASE_EPISODES[candidate_id])
    outer_folds = item.get("outer_folds")
    if not isinstance(outer_folds, list) or tuple(fold.get("name") for fold in outer_folds) != FOLD_NAMES:
        raise V6EvidenceError(f"{candidate_id} outer folds differ")
    raw_count = 0
    final_count = 0
    calibration_valid: list[str] = []
    for fold in outer_folds:
        raw_trades = fold.get("raw_trades")
        final_trades = fold.get("trades")
        if not isinstance(raw_trades, list) or not isinstance(final_trades, list):
            raise V6EvidenceError("fold trade details must be lists")
        _verify_trade_metrics(raw_trades, fold.get("raw_portfolio"), "raw_portfolio")
        _verify_empty_metrics(final_trades, fold.get("portfolio"), "portfolio")
        direction = _calibration_direction(fold.get("calibration"))
        if fold.get("calibration_direction_valid") is not direction:
            raise V6EvidenceError("calibration direction flag differs from buckets")
        if direction:
            calibration_valid.append(fold["name"])
        raw_count += len(raw_trades)
        final_count += len(final_trades)
    aggregate = _mapping(item.get("aggregate"), "aggregate")
    _exact(aggregate, "gate_status", "BLOCK")
    _exact(aggregate, "activation_eligible", False)
    _exact(aggregate, "positive_outer_folds", 0)
    _exact(aggregate, "outer_fold_count", 5)
    _exact(aggregate, "multiple_testing_adjusted_p", None)
    _number_close(
        aggregate.get("multiple_testing_significance_alpha"),
        0.0005952380952380953,
        "familywise alpha",
    )
    if aggregate.get("block_reasons") != list(BLOCK_REASONS):
        raise V6EvidenceError("candidate BLOCK reasons differ")
    _verify_empty_metrics([], aggregate.get("validation"), "aggregate validation")
    _verify_empty_metrics(
        [], aggregate.get("portfolio"), "aggregate portfolio", portfolio=True
    )
    if final_count != 0:
        raise V6EvidenceError("historical candidate unexpectedly has final trades")
    return CandidateEvidenceAudit(
        candidate_id=candidate_id,
        base_episode_count=EXPECTED_BASE_EPISODES[candidate_id],
        raw_closed_trade_count=raw_count,
        final_trade_count=final_count,
        calibration_valid_folds=tuple(calibration_valid),
        registration_contract_sha256=EXPECTED_REGISTRATION_HASHES[candidate_id],
        result_sha256=EXPECTED_RESULT_HASHES[candidate_id],
    )


def _verify_trade_metrics(trades: list[Any], recorded: Any, label: str) -> None:
    metrics = _mapping(recorded, label)
    returns: list[float] = []
    identities: set[tuple[Any, ...]] = set()
    for trade in trades:
        row = _mapping(trade, "trade")
        value = _finite(row.get("net_return_pct"), "trade net_return_pct")
        identity = (
            row.get("stock_code"), row.get("entry_date"), row.get("exit_date"),
            row.get("candidate_id"), row.get("exit_sleeve"),
        )
        if identity in identities:
            raise V6EvidenceError("duplicate historical trade identity")
        identities.add(identity)
        returns.append(value)
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    expected = {
        "trade_count": len(returns),
        "sample_count": len(returns),
        "win_rate": len(wins) / len(returns) if returns else None,
        "net_expectancy_pct": sum(returns) / len(returns) if returns else None,
        "average_win_pct": sum(wins) / len(wins) if wins else None,
        "average_loss_pct": gross_loss / len(losses) if losses else None,
        "payoff_ratio": (
            (sum(wins) / len(wins)) / (gross_loss / len(losses))
            if wins and losses and gross_loss > 0 else None
        ),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
        "gross_profit_pct": gross_profit,
        "gross_loss_pct": gross_loss,
        "total_net_return_pct": sum(returns),
    }
    for name, value in expected.items():
        _metric_equal(metrics.get(name), value, f"{label}.{name}")


def _verify_empty_metrics(
    trades: list[Any], recorded: Any, label: str, *, portfolio: bool = False
) -> None:
    if trades:
        raise V6EvidenceError(f"{label} is expected to have no trades")
    metrics = _mapping(recorded, label)
    for name in (
        "trade_count", "sample_count", "gross_profit_pct", "gross_loss_pct",
        "total_net_return_pct",
    ):
        _metric_equal(metrics.get(name), 0, f"{label}.{name}")
    for name in (
        "win_rate", "net_expectancy_pct", "average_win_pct", "average_loss_pct",
        "payoff_ratio", "profit_factor",
    ):
        if metrics.get(name) is not None:
            raise V6EvidenceError(f"{label}.{name} must be null")
    if portfolio:
        _metric_equal(metrics.get("net_profit_cny"), 0.0, f"{label}.net_profit_cny")
        _metric_equal(
            metrics.get("maximum_drawdown_pct"), 0.0, f"{label}.maximum_drawdown_pct"
        )


def _calibration_direction(value: Any) -> bool:
    calibration = _mapping(value, "calibration")
    buckets = calibration.get("buckets")
    if not isinstance(buckets, list) or len(buckets) <= 1:
        return False
    ordered = sorted(buckets, key=lambda item: _finite(item.get("lower_score"), "lower_score"))
    returns = [_finite(item.get("expected_return_net_pct"), "bucket return") for item in ordered]
    if returns[-1] <= 0:
        return False
    return all(right + 0.25 >= left for left, right in zip(returns, returns[1:]))


def _validate_governance(artifact: Mapping[str, Any], results: Mapping[str, Any]) -> None:
    governance = _mapping(artifact.get("research_governance"), "research_governance")
    _exact(governance, "status", "GOVERNED")
    _exact(governance, "prior_recorded_candidate_searches", 81)
    _exact(governance, "cumulative_familywise_candidate_count", 84)
    _number_close(
        governance.get("familywise_significance_alpha"),
        0.0005952380952380953,
        "governance alpha",
    )
    registrations = governance.get("registrations")
    if not isinstance(registrations, list) or tuple(item.get("candidate_id") for item in registrations) != CANDIDATE_IDS:
        raise V6EvidenceError("governance registrations differ")
    envelopes = _mapping(governance.get("result_envelopes"), "result_envelopes")
    if tuple(envelopes) != CANDIDATE_IDS:
        raise V6EvidenceError("governance result envelopes differ")
    for candidate_id in CANDIDATE_IDS:
        envelope = _mapping(envelopes[candidate_id], "result envelope")
        _exact(envelope, "candidate_id", candidate_id)
        _exact(envelope, "preregistration_contract_hash", EXPECTED_REGISTRATION_HASHES[candidate_id])
        _exact(envelope, "preregistered_classification", "exploratory")
        _exact(envelope, "evidence_classification", "exploratory")
        _exact(envelope, "confirmatory_claim_allowed", False)
        summary = _mapping(envelope.get("result"), "governed result")
        _exact(summary, "gate_status", "BLOCK")
        _exact(summary, "result_sha256", EXPECTED_RESULT_HASHES[candidate_id])
        if summary.get("block_reasons") != list(BLOCK_REASONS):
            raise V6EvidenceError("governed BLOCK reasons differ")


def _validate_logs(logs: Mapping[str, bytes], artifact: Mapping[str, Any]) -> None:
    if logs["completed_stderr"] != b"":
        raise V6EvidenceError("completed historical stderr must remain empty")
    completed = _strict_json_lines(logs["completed_stdout"], "completed stdout")
    phases = [item.get("phase") for item in completed]
    if phases != [
        "load_history", "features_ready", "label_base_episodes",
        "nested_oos", "candidate_complete", "nested_oos", "candidate_complete",
        "nested_oos", "candidate_complete", "complete",
    ]:
        raise V6EvidenceError("completed log phases differ")
    complete = completed[-1]
    if complete.get("ranking") != list(CANDIDATE_IDS) or complete.get("passes") != []:
        raise V6EvidenceError("completed log attempts a PASS or ranking drift")
    completed_ids = tuple(
        item.get("candidate_id") for item in completed if item.get("phase") == "candidate_complete"
    )
    if completed_ids != CANDIDATE_IDS:
        raise V6EvidenceError("completed log candidate order differs")
    if any(
        item.get("gate_status") != "BLOCK"
        for item in completed if item.get("phase") == "candidate_complete"
    ):
        raise V6EvidenceError("completed log contains a non-BLOCK candidate")
    failed = _strict_json_lines(logs["failed_stdout"], "failed stdout")
    if len(failed) != 1 or failed[0].get("phase") != "load_history":
        raise V6EvidenceError("failed attempt stdout differs")
    try:
        failed_stderr = logs["failed_stderr"].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise V6EvidenceError("failed stderr is not UTF-8") from exc
    if "Fatal Python error" not in failed_stderr:
        raise V6EvidenceError("failed attempt no longer records its fatal error")


def _validate_execution_boundary(campaign: Mapping[str, Any], artifact: Mapping[str, Any]) -> None:
    expected = {
        "research_only": True,
        "shadow_only": True,
        "paper_orders_only_after_full_gate": True,
        "real_order_submission": False,
        "historical_success_ceiling": "EXPLORATORY_PAPER_CANDIDATE_NOT_ACTIVATABLE",
    }
    for document, label in ((campaign, "campaign"), (artifact, "artifact")):
        boundary = _mapping(document.get("execution_boundary"), f"{label} execution_boundary")
        if boundary != expected:
            raise V6EvidenceError(f"{label} execution boundary differs")


def _validate_evidence_status(campaign: Mapping[str, Any], artifact: Mapping[str, Any]) -> None:
    expected = {
        "campaign_mode": "EXPLORATORY_ONLY",
        "all_history_through_2026_07_31_is_contaminated_by_prior_inspection": True,
        "historical_pass_cannot_activate_model": True,
        "activation_eligible": False,
        "prospective_paper_start": "2026-08-03",
        "minimum_prospective_trading_days": 120,
        "minimum_prospective_mature_portfolio_trades": 80,
    }
    if campaign.get("evidence_status") != expected or artifact.get("evidence_status") != expected:
        raise V6EvidenceError("historical evidence status differs")


def _strict_json(payload: bytes, label: str) -> Mapping[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise V6EvidenceError(f"{label} has duplicate key: {key}")
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise V6EvidenceError(f"{label} has non-finite constant: {value}")

    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V6EvidenceError(f"{label} is not strict UTF-8 JSON") from exc
    result = _mapping(value, label)
    _reject_nonfinite_tree(result, label)
    return result


def _strict_json_lines(payload: bytes, label: str) -> list[Mapping[str, Any]]:
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise V6EvidenceError(f"{label} is not UTF-8") from exc
    if not lines:
        raise V6EvidenceError(f"{label} is empty")
    return [_strict_json(line.encode("utf-8"), f"{label} line") for line in lines]


def _reject_true_activation(value: Any, path: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in ACTIVATION_KEYS and child is not False:
                raise V6EvidenceError(f"{path}.{key} attempts an activation claim")
            _reject_true_activation(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_true_activation(child, f"{path}[{index}]")


def _reject_nonfinite_tree(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise V6EvidenceError(f"{path} is non-finite")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_nonfinite_tree(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_nonfinite_tree(child, f"{path}[{index}]")


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V6EvidenceError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise V6EvidenceError(f"{label} must be non-empty text")
    return value


def _exact(document: Mapping[str, Any], name: str, expected: Any) -> None:
    actual = document.get(name)
    if actual != expected or type(actual) is not type(expected):
        raise V6EvidenceError(f"{name} differs")


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise V6EvidenceError(f"{label} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise V6EvidenceError(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise V6EvidenceError(f"{label} must be finite")
    return result


def _metric_equal(actual: Any, expected: Any, label: str) -> None:
    if expected is None:
        if actual is not None:
            raise V6EvidenceError(f"{label} differs")
    elif isinstance(expected, int):
        if type(actual) is not int or actual != expected:
            raise V6EvidenceError(f"{label} differs")
    else:
        _number_close(actual, expected, label)


def _number_close(actual: Any, expected: float, label: str) -> None:
    value = _finite(actual, label)
    if not math.isclose(value, float(expected), rel_tol=1e-10, abs_tol=1e-10):
        raise V6EvidenceError(f"{label} differs")


def _require_hash(payload: bytes, expected: str, label: str) -> str:
    if not isinstance(expected, str) or len(expected) != 64:
        raise V6EvidenceError(f"expected {label} SHA-256 is invalid")
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise V6EvidenceError(f"{label} byte SHA-256 mismatch")
    return actual


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "CANDIDATE_IDS", "V6EvidenceAudit", "V6EvidenceError",
    "audit_v6_evidence_bytes",
]
