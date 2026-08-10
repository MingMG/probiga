"""Fail-closed auditor for the legacy Trading V5 research evidence.

The historical V5 artifacts are inputs to an audit, never executable model
registrations.  This module deliberately has no database, V2, V3 or V4
dependency.  It verifies the frozen campaign contract, recomputes aggregate
metrics from the recorded fold trades, and preserves every known limitation as
a blocking reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import itertools
import json
import math
from typing import Any, Mapping, Sequence


AUDIT_SCHEMA_VERSION = "probiga.trading-v5-evidence-audit.v1"
AUDIT_RELEASE_ID = "trading_v5.0.0-research"
_ACTIVATION_KEYS = frozenset(
    {
        "actionable_output_allowed",
        "activation_eligible",
        "confirmatory_claim_allowed",
        "paper_eligible",
        "paper_orders_allowed",
        "production_eligible",
        "real_order_submission",
        "real_orders_allowed",
    }
)
_CORE_METRICS = (
    "trade_count",
    "sample_count",
    "win_rate",
    "net_expectancy_pct",
    "average_win_pct",
    "average_loss_pct",
    "payoff_ratio",
    "profit_factor",
    "gross_profit_pct",
    "gross_loss_pct",
    "total_net_return_pct",
)
_KNOWN_LEGACY_NON_STRICT_ARTIFACTS = {
    "1a4f8c5fa229352ad20324f8cf4cc9ad85891d5d6aed78d1eb76795a64ba6259": (
        "Infinity",
        "Infinity",
    ),
}


class EvidenceContractError(ValueError):
    """Raised when historical evidence violates its frozen contract."""


@dataclass(frozen=True, slots=True)
class CandidateAudit:
    candidate_id: str
    status: str
    recorded_status: str
    block_reasons: tuple[str, ...]
    recorded_block_reasons: tuple[str, ...]
    trade_count: int
    net_expectancy_pct: float | None
    profit_factor: float | None
    payoff_ratio: float | None
    trade_pnl_sum_cny: float
    recorded_portfolio_net_profit_cny: float
    recorded_maximum_drawdown_pct: float
    positive_outer_folds: int
    outer_fold_count: int
    calibration_direction_all_valid: bool
    multiple_testing_adjusted_p: float | None
    multiple_testing_alpha: float
    activation_eligible: bool = False
    paper_eligible: bool = False
    production_eligible: bool = False

    def __post_init__(self) -> None:
        if self.status != "BLOCK":
            raise EvidenceContractError("a V5 historical audit can only BLOCK")
        if (
            self.activation_eligible
            or self.paper_eligible
            or self.production_eligible
        ):
            raise EvidenceContractError("historical V5 evidence cannot activate")

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "status": "BLOCK",
            "legacy_recorded_gate_status": (
                "BLOCK"
                if self.recorded_status == "BLOCK"
                else "UNTRUSTED_NON_BLOCK_CLAIM"
            ),
            "block_reasons": list(self.block_reasons),
            "recorded_block_reasons": list(self.recorded_block_reasons),
            "verified_from_recorded_trades": {
                "trade_count": self.trade_count,
                "net_expectancy_pct": self.net_expectancy_pct,
                "profit_factor": self.profit_factor,
                "payoff_ratio": self.payoff_ratio,
                "trade_pnl_sum_cny": self.trade_pnl_sum_cny,
                "positive_outer_folds": self.positive_outer_folds,
                "outer_fold_count": self.outer_fold_count,
                "calibration_direction_all_valid": (
                    self.calibration_direction_all_valid
                ),
                "multiple_testing_alpha": self.multiple_testing_alpha,
            },
            "recorded_but_not_independently_recomputed": {
                "portfolio_net_profit_cny": (
                    self.recorded_portfolio_net_profit_cny
                ),
                "maximum_drawdown_pct": self.recorded_maximum_drawdown_pct,
                "multiple_testing_adjusted_p": (
                    self.multiple_testing_adjusted_p
                ),
                "portfolio_accounting_verified": False,
                "drawdown_verified": False,
                "multiple_testing_recomputed": False,
            },
            "activation_eligible": False,
            "paper_eligible": False,
            "production_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class CampaignAudit:
    campaign_id: str
    campaign_sha256: str
    artifact_sha256: str
    research_contract_sha256: str
    evidence_classification: str
    governance_status: str
    non_strict_json_constants: tuple[str, ...]
    stress_matrix_expected_scenarios: int
    stress_matrix_recorded_scenarios: int
    stress_matrix_complete: bool
    reproducible_with_current_source: bool
    candidates: tuple[CandidateAudit, ...]
    status: str = "BLOCK"
    lifecycle_status: str = "RESEARCH_ONLY"
    activation_eligible: bool = False
    paper_eligible: bool = False
    production_eligible: bool = False

    def __post_init__(self) -> None:
        if self.status != "BLOCK" or self.lifecycle_status != "RESEARCH_ONLY":
            raise EvidenceContractError("legacy V5 evidence must remain blocked")
        if (
            self.activation_eligible
            or self.paper_eligible
            or self.production_eligible
            or self.reproducible_with_current_source
        ):
            raise EvidenceContractError("legacy V5 evidence cannot be activated")

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "release_id": AUDIT_RELEASE_ID,
            "campaign_id": self.campaign_id,
            "campaign_sha256": self.campaign_sha256,
            "artifact_sha256": self.artifact_sha256,
            "research_contract_sha256": self.research_contract_sha256,
            "status": "BLOCK",
            "integrity_status": (
                "BYTE_HASH_MATCHED_WITH_LEGACY_JSON_WARNING"
                if self.non_strict_json_constants
                else "BYTE_HASH_MATCHED_WITH_LIMITED_RECOMPUTATION"
            ),
            "anchor_status": "EXPECTED_BYTE_HASHES_REQUIRED",
            "historical_gate_status": "BLOCK",
            "activation_status": "BLOCK",
            "lifecycle_status": "RESEARCH_ONLY",
            "evidence_classification": self.evidence_classification,
            "governance_status": self.governance_status,
            "legacy_json": {
                "strict": not bool(self.non_strict_json_constants),
                "non_finite_constants": list(self.non_strict_json_constants),
            },
            "stress_matrix": {
                "expected_scenarios": self.stress_matrix_expected_scenarios,
                "recorded_scenarios": self.stress_matrix_recorded_scenarios,
                "complete": self.stress_matrix_complete,
            },
            "reproducible_with_current_source": False,
            "candidates": [item.as_dict() for item in self.candidates],
            "activation_eligible": False,
            "paper_eligible": False,
            "production_eligible": False,
            "actionable_output_allowed": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def audit_campaign_bytes(
    campaign_bytes: bytes,
    artifact_bytes: bytes,
    *,
    expected_campaign_sha256: str,
    expected_artifact_sha256: str,
) -> CampaignAudit:
    """Audit one frozen V5 campaign/artifact pair without ambient I/O."""

    campaign_sha = hashlib.sha256(campaign_bytes).hexdigest()
    artifact_sha = hashlib.sha256(artifact_bytes).hexdigest()
    _require_expected_hash(
        label="campaign",
        actual=campaign_sha,
        expected=expected_campaign_sha256,
    )
    _require_expected_hash(
        label="artifact",
        actual=artifact_sha,
        expected=expected_artifact_sha256,
    )
    campaign, campaign_constants = _load_json(
        campaign_bytes,
        label="campaign",
    )
    artifact, artifact_constants = _load_json(
        artifact_bytes,
        label="artifact",
        permit_legacy_constants=True,
    )
    if campaign_constants:
        raise EvidenceContractError("campaign JSON must be strict and finite")
    if artifact_constants and _KNOWN_LEGACY_NON_STRICT_ARTIFACTS.get(
        artifact_sha
    ) != tuple(artifact_constants):
        raise EvidenceContractError(
            "non-strict numbers are permitted only in the byte-frozen legacy artifact"
        )
    _require_mapping(campaign, "campaign")
    _require_mapping(artifact, "artifact")
    _reject_non_finite_tree(campaign, "campaign")
    _reject_non_finite_tree(artifact, "artifact")
    _reject_true_activation_fields(campaign, "campaign")
    _reject_true_activation_fields(artifact, "artifact")

    campaign_id = _non_empty_string(campaign.get("campaign_id"), "campaign_id")
    artifact_schema = _non_empty_string(
        campaign.get("artifact_schema_version"),
        "artifact_schema_version",
    )
    if artifact.get("schema_version") != artifact_schema:
        raise EvidenceContractError("artifact schema differs from campaign")
    if artifact.get("campaign_id") != campaign_id:
        raise EvidenceContractError("artifact campaign_id does not match contract")
    contract_hash = _canonical_sha256(campaign)
    if artifact.get("research_contract_sha256") != contract_hash:
        raise EvidenceContractError(
            "artifact research_contract_sha256 does not match campaign"
        )
    _validate_execution_boundary(campaign, artifact)
    evidence_status = _require_mapping(
        campaign.get("evidence_status"),
        "campaign.evidence_status",
    )
    if evidence_status.get(
        "all_history_through_2026_07_31_is_contaminated_by_prior_inspection"
    ) is not True:
        raise EvidenceContractError("historical inspection contamination missing")
    if evidence_status.get("historical_pass_cannot_activate_model") is not True:
        raise EvidenceContractError("historical activation ceiling missing")

    control = _require_mapping(
        campaign.get("candidate_control"),
        "campaign.candidate_control",
    )
    specs = _require_list(control.get("candidates"), "candidate candidates")
    candidate_ids = tuple(
        _non_empty_string(
            _require_mapping(item, "candidate spec").get("id"),
            "candidate id",
        )
        for item in specs
    )
    if len(candidate_ids) != len(set(candidate_ids)):
        raise EvidenceContractError("candidate ids must be unique")
    maximum_new = _positive_int(
        control.get("maximum_new_candidate_count"),
        "maximum_new_candidate_count",
    )
    prior_count = _non_negative_int(
        control.get("prior_recorded_candidate_searches"),
        "prior_recorded_candidate_searches",
    )
    familywise_count = _positive_int(
        control.get("maximum_familywise_candidate_count"),
        "maximum_familywise_candidate_count",
    )
    if len(candidate_ids) > maximum_new:
        raise EvidenceContractError("candidate count exceeds the frozen maximum")
    if prior_count + len(candidate_ids) > familywise_count:
        raise EvidenceContractError("familywise candidate count is inconsistent")
    recorded_order = tuple(
        _non_empty_string(item, "artifact candidate_order")
        for item in _require_list(
            artifact.get("candidate_order"),
            "artifact.candidate_order",
        )
    )
    if recorded_order != candidate_ids:
        raise EvidenceContractError("candidate order differs from frozen contract")
    results = _require_mapping(artifact.get("results"), "artifact.results")
    if set(results) != set(candidate_ids):
        raise EvidenceContractError("artifact result candidates are incomplete")
    ranking = tuple(
        _non_empty_string(item, "ranking candidate")
        for item in _require_list(artifact.get("ranking"), "artifact.ranking")
    )
    if len(ranking) != len(candidate_ids) or set(ranking) != set(candidate_ids):
        raise EvidenceContractError("artifact ranking is not a candidate permutation")

    expected_stress, recorded_stress, stress_complete = _audit_stress_matrix(
        campaign,
        artifact,
        candidate_ids,
    )
    multiple_testing = _require_mapping(
        artifact.get("multiple_testing"),
        "artifact.multiple_testing",
    )
    minimum_iterations = _positive_int(
        _require_mapping(
            campaign.get("stress_tests"),
            "campaign.stress_tests",
        ).get("minimum_bootstrap_iterations"),
        "minimum_bootstrap_iterations",
    )
    recorded_iterations = _non_negative_int(
        multiple_testing.get("iterations"),
        "multiple_testing.iterations",
    )
    adjusted_values = _require_mapping(
        multiple_testing.get("adjusted_p_values"),
        "multiple_testing.adjusted_p_values",
    )
    if set(adjusted_values) != set(candidate_ids):
        raise EvidenceContractError("adjusted p-value candidate set differs")

    gate = _require_mapping(campaign.get("profit_gate"), "profit_gate")
    fold_contracts = tuple(
        _require_mapping(item, "outer fold")
        for item in _require_list(campaign.get("outer_folds"), "outer_folds")
    )
    expected_folds = tuple(
        _non_empty_string(item.get("name"), "outer fold name")
        for item in fold_contracts
    )
    if len(expected_folds) != len(set(expected_folds)):
        raise EvidenceContractError("outer fold names must be unique")
    alpha = _finite_number(
        control.get("fallback_bonferroni_one_sided_alpha"),
        "fallback_bonferroni_one_sided_alpha",
    )
    if not 0.0 < alpha < 1.0:
        raise EvidenceContractError("multiple-testing alpha must be within 0..1")
    expected_alpha = 0.05 / familywise_count
    if not math.isclose(alpha, expected_alpha, rel_tol=0.0, abs_tol=1e-15):
        raise EvidenceContractError("Bonferroni alpha differs from familywise count")

    candidate_audits = tuple(
        _audit_candidate(
            candidate_id=candidate_id,
            result=_require_mapping(
                results[candidate_id],
                f"results.{candidate_id}",
            ),
            expected_folds=expected_folds,
            fold_contracts=fold_contracts,
            gate=gate,
            adjusted_value=adjusted_values[candidate_id],
            alpha=alpha,
            bootstrap_complete=recorded_iterations >= minimum_iterations,
            stress_complete=stress_complete,
            legacy_non_strict=bool(artifact_constants),
        )
        for candidate_id in candidate_ids
    )
    governance_status = _governance_status(campaign, artifact)
    return CampaignAudit(
        campaign_id=campaign_id,
        campaign_sha256=campaign_sha,
        artifact_sha256=artifact_sha,
        research_contract_sha256=contract_hash,
        evidence_classification="HISTORICAL_EXPLORATORY_BLOCK",
        governance_status=governance_status,
        non_strict_json_constants=tuple(artifact_constants),
        stress_matrix_expected_scenarios=expected_stress,
        stress_matrix_recorded_scenarios=recorded_stress,
        stress_matrix_complete=stress_complete,
        reproducible_with_current_source=False,
        candidates=candidate_audits,
    )


def _audit_candidate(
    *,
    candidate_id: str,
    result: Mapping[str, Any],
    expected_folds: tuple[str, ...],
    fold_contracts: tuple[Mapping[str, Any], ...],
    gate: Mapping[str, Any],
    adjusted_value: Any,
    alpha: float,
    bootstrap_complete: bool,
    stress_complete: bool,
    legacy_non_strict: bool,
) -> CandidateAudit:
    folds = _require_list(result.get("outer_folds"), f"{candidate_id}.outer_folds")
    fold_names = tuple(
        _non_empty_string(
            _require_mapping(item, "candidate outer fold").get("name"),
            "candidate outer fold name",
        )
        for item in folds
    )
    if fold_names != expected_folds:
        raise EvidenceContractError(
            f"{candidate_id} fold sequence differs from contract"
        )
    all_trades: list[Mapping[str, Any]] = []
    positive_folds = 0
    calibration_valid = True
    minimum_fold_pf = _finite_number(
        gate.get("minimum_outer_fold_profit_factor"),
        "minimum_outer_fold_profit_factor",
    )
    net_profit = 0.0
    trade_pnl_sum = 0.0
    maximum_drawdown = 0.0
    for fold, fold_contract in zip(folds, fold_contracts):
        fold_map = _require_mapping(fold, "candidate fold")
        trades = _require_list(fold_map.get("trades"), "fold.trades")
        typed_trades = [
            _require_mapping(item, "fold trade") for item in trades
        ]
        fold_trade_pnl = sum(
            _finite_number(item.get("net_pnl_cny"), "trade net_pnl_cny")
            for item in typed_trades
        )
        trade_pnl_sum += fold_trade_pnl
        fold_trade_metrics = _metrics_from_trades(typed_trades)
        validation_start = _iso_date(
            fold_contract.get("validation_start"),
            "fold validation_start",
        )
        validation_end = _iso_date(
            fold_contract.get("validation_end"),
            "fold validation_end",
        )
        if validation_start > validation_end:
            raise EvidenceContractError("fold validation boundary is inverted")
        for trade in typed_trades:
            _non_empty_string(trade.get("stock_code"), "trade stock_code")
            entry_date = _iso_date(trade.get("entry_date"), "trade entry_date")
            exit_date = _iso_date(trade.get("exit_date"), "trade exit_date")
            if not validation_start <= entry_date <= exit_date <= validation_end:
                raise EvidenceContractError(
                    f"{candidate_id} trade escapes its validation fold"
                )
            _finite_number(trade.get("net_return_pct"), "trade net_return_pct")
            _finite_number(trade.get("net_pnl_cny"), "trade net_pnl_cny")
            _non_empty_string(trade.get("exit_reason"), "trade exit_reason")
            _non_empty_string(trade.get("candidate_id"), "trade candidate lineage")
        all_trades.extend(typed_trades)
        portfolio = _require_mapping(fold_map.get("portfolio"), "fold.portfolio")
        fold_count = _non_negative_int(
            portfolio.get("trade_count"),
            "fold portfolio trade_count",
        )
        if fold_count != len(typed_trades):
            raise EvidenceContractError(
                f"{candidate_id} fold trade_count differs from trades"
            )
        if _non_negative_int(
            portfolio.get("sample_count"),
            "fold portfolio sample_count",
        ) != len(typed_trades):
            raise EvidenceContractError(
                f"{candidate_id} fold sample_count differs from trades"
            )
        fold_profit = _finite_number(
            portfolio.get("net_profit_cny"),
            "fold net_profit_cny",
        )
        fold_pf = _optional_finite_number(
            portfolio.get("profit_factor"),
            "fold profit_factor",
        )
        drawdown = _finite_number(
            portfolio.get("maximum_drawdown_pct"),
            "fold maximum_drawdown_pct",
        )
        net_profit += fold_profit
        maximum_drawdown = max(maximum_drawdown, drawdown)
        verified_fold_pf = fold_trade_metrics["profit_factor"]
        if (
            fold_trade_pnl > 0
            and verified_fold_pf is not None
            and verified_fold_pf >= minimum_fold_pf
        ):
            positive_folds += 1
        calibration = _require_mapping(
            fold_map.get("calibration"),
            "fold.calibration",
        )
        if calibration.get("strategy_key") != candidate_id:
            raise EvidenceContractError("calibration strategy key differs")
        expected_model_version = f"{candidate_id}-{fold_map['name']}-nested-oof"
        if calibration.get("model_version") != expected_model_version:
            raise EvidenceContractError("calibration model version differs")
        _digest_text(calibration.get("dataset_hash"), "calibration dataset_hash")
        calibration_buckets = _require_list(
            calibration.get("buckets"),
            "calibration.buckets",
        )
        bucket_samples = sum(
            _non_negative_int(
                _require_mapping(item, "calibration bucket").get("sample_count"),
                "calibration bucket sample_count",
            )
            for item in calibration_buckets
        )
        if bucket_samples != _non_negative_int(
            fold_map.get("inner_oof_samples"),
            "inner_oof_samples",
        ):
            raise EvidenceContractError("calibration sample count differs from inner OOF")
        recomputed_direction = _calibration_direction_valid(calibration)
        recorded_direction = fold_map.get("calibration_direction_valid")
        if type(recorded_direction) is not bool:
            raise EvidenceContractError("calibration direction flag must be bool")
        if recorded_direction != recomputed_direction:
            raise EvidenceContractError(
                f"{candidate_id} calibration direction flag is inconsistent"
            )
        calibration_valid = calibration_valid and recomputed_direction

    metrics = _metrics_from_trades(all_trades)
    aggregate = _require_mapping(result.get("aggregate"), f"{candidate_id}.aggregate")
    validation = _require_mapping(aggregate.get("validation"), "aggregate.validation")
    portfolio = _require_mapping(aggregate.get("portfolio"), "aggregate.portfolio")
    _compare_core_metrics(validation, metrics, "aggregate.validation")
    _compare_core_metrics(portfolio, metrics, "aggregate.portfolio")
    if not math.isclose(
        _finite_number(portfolio.get("net_profit_cny"), "portfolio net profit"),
        net_profit,
        rel_tol=1e-12,
        abs_tol=1e-9,
    ):
        raise EvidenceContractError(f"{candidate_id} aggregate net profit mismatch")
    if not math.isclose(
        _finite_number(
            portfolio.get("maximum_drawdown_pct"),
            "portfolio maximum drawdown",
        ),
        maximum_drawdown,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise EvidenceContractError(f"{candidate_id} aggregate drawdown mismatch")
    if _non_negative_int(
        aggregate.get("positive_outer_folds"),
        "aggregate positive_outer_folds",
    ) != positive_folds:
        raise EvidenceContractError(f"{candidate_id} positive fold count mismatch")
    if _non_negative_int(
        aggregate.get("outer_fold_count"),
        "aggregate outer_fold_count",
    ) != len(folds):
        raise EvidenceContractError(f"{candidate_id} outer fold count mismatch")

    reasons = _metric_gate_failures(
        validation=metrics,
        portfolio_net_profit=trade_pnl_sum,
        maximum_drawdown=maximum_drawdown,
        positive_folds=positive_folds,
        calibration_valid=calibration_valid,
        gate=gate,
    )
    adjusted_p = _optional_probability(adjusted_value, "adjusted p-value")
    recorded_adjusted = _optional_probability(
        aggregate.get("multiple_testing_adjusted_p"),
        "aggregate adjusted p-value",
    )
    if recorded_adjusted != adjusted_p:
        raise EvidenceContractError(f"{candidate_id} adjusted p-value mismatch")
    if adjusted_p is None or adjusted_p >= alpha:
        reasons.append("MULTIPLE_TESTING_MAX_T_NOT_SIGNIFICANT")
    reasons.append("MULTIPLE_TESTING_RESULT_NOT_RECOMPUTED")
    if not bootstrap_complete:
        reasons.append("BOOTSTRAP_ITERATION_FLOOR_NOT_MET")
    if not stress_complete:
        reasons.append("STRESS_MATRIX_INCOMPLETE")
    if legacy_non_strict:
        reasons.append("LEGACY_NON_STRICT_JSON")
    reasons.extend(
        (
            "FOLD_BOUNDARIES_NOT_EMBEDDED_IN_ARTIFACT",
            "FOLD_EQUITY_CURVE_MISSING",
            "CALIBRATION_DATASET_HASH_OPAQUE",
            "TRADE_PNL_PORTFOLIO_RECONCILIATION_UNPROVEN",
            "BOOTSTRAP_SEED_NOT_BOUND_BY_CAMPAIGN",
            "HISTORICAL_DATA_CONTAMINATED_BY_PRIOR_INSPECTION",
            "HISTORICAL_ARTIFACT_NOT_REPRODUCIBLE_WITH_CURRENT_SOURCE",
            "PROSPECTIVE_GATE_NOT_EVALUATED",
        )
    )
    if not math.isclose(
        trade_pnl_sum,
        net_profit,
        rel_tol=1e-12,
        abs_tol=1e-9,
    ):
        reasons.append("TRADE_PNL_DOES_NOT_RECONCILE_TO_PORTFOLIO")
    recorded_status = _non_empty_string(
        aggregate.get("gate_status"),
        "aggregate.gate_status",
    )
    recorded_reasons = tuple(
        _non_empty_string(item, "recorded block reason")
        for item in _require_list(
            aggregate.get("block_reasons"),
            "aggregate.block_reasons",
        )
    )
    if recorded_status != "BLOCK":
        reasons.append("RECORDED_GATE_STATUS_INCONSISTENT")
    if not set(_metric_gate_failures(
        validation=metrics,
        portfolio_net_profit=trade_pnl_sum,
        maximum_drawdown=maximum_drawdown,
        positive_folds=positive_folds,
        calibration_valid=calibration_valid,
        gate=gate,
    )).issubset(recorded_reasons):
        reasons.append("RECORDED_BLOCK_REASONS_INCOMPLETE")
    return CandidateAudit(
        candidate_id=candidate_id,
        status="BLOCK",
        recorded_status=recorded_status,
        block_reasons=tuple(dict.fromkeys(reasons)),
        recorded_block_reasons=recorded_reasons,
        trade_count=int(metrics["trade_count"]),
        net_expectancy_pct=metrics["net_expectancy_pct"],
        profit_factor=metrics["profit_factor"],
        payoff_ratio=metrics["payoff_ratio"],
        trade_pnl_sum_cny=_close(trade_pnl_sum),
        recorded_portfolio_net_profit_cny=_close(net_profit),
        recorded_maximum_drawdown_pct=_close(maximum_drawdown),
        positive_outer_folds=positive_folds,
        outer_fold_count=len(folds),
        calibration_direction_all_valid=calibration_valid,
        multiple_testing_adjusted_p=adjusted_p,
        multiple_testing_alpha=alpha,
    )


def _metric_gate_failures(
    *,
    validation: Mapping[str, Any],
    portfolio_net_profit: float,
    maximum_drawdown: float,
    positive_folds: int,
    calibration_valid: bool,
    gate: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    count = int(validation["trade_count"])
    expectancy = validation["net_expectancy_pct"]
    profit_factor = validation["profit_factor"]
    payoff = validation["payoff_ratio"]
    if count < _positive_int(gate.get("minimum_oos_samples"), "minimum_oos_samples"):
        reasons.append("OOS_SAMPLE_COUNT_TOO_LOW")
    minimum_expectancy = _finite_number(
        gate.get("minimum_expected_return_net_pct"),
        "minimum_expected_return_net_pct",
    )
    if expectancy is None:
        reasons.append("OOS_NET_EXPECTANCY_MISSING")
    elif expectancy <= minimum_expectancy:
        reasons.append("OOS_NET_EXPECTANCY_NOT_POSITIVE")
    if profit_factor is None:
        reasons.append("OOS_PROFIT_FACTOR_MISSING")
    elif profit_factor < _finite_number(
        gate.get("minimum_profit_factor"),
        "minimum_profit_factor",
    ):
        reasons.append("OOS_PROFIT_FACTOR_TOO_LOW")
    if payoff is None:
        reasons.append("OOS_PAYOFF_RATIO_MISSING")
    elif payoff < _finite_number(
        gate.get("minimum_payoff_ratio"),
        "minimum_payoff_ratio",
    ):
        reasons.append("OOS_PAYOFF_RATIO_TOO_LOW")
    if count < _positive_int(
        gate.get("minimum_portfolio_trades"),
        "minimum_portfolio_trades",
    ):
        reasons.append("PORTFOLIO_TRADE_COUNT_TOO_LOW")
    portfolio_minimum_expectancy = _finite_number(
        gate.get("minimum_portfolio_net_expectancy_pct"),
        "minimum_portfolio_net_expectancy_pct",
    )
    if expectancy is None:
        reasons.append("PORTFOLIO_NET_EXPECTANCY_MISSING")
    elif expectancy <= portfolio_minimum_expectancy:
        reasons.append("PORTFOLIO_NET_EXPECTANCY_NOT_POSITIVE")
    if profit_factor is None:
        reasons.append("PORTFOLIO_PROFIT_FACTOR_MISSING")
    elif profit_factor < _finite_number(
        gate.get("minimum_portfolio_profit_factor"),
        "minimum_portfolio_profit_factor",
    ):
        reasons.append("PORTFOLIO_PROFIT_FACTOR_TOO_LOW")
    if payoff is None:
        reasons.append("PORTFOLIO_PAYOFF_RATIO_MISSING")
    elif payoff < _finite_number(
        gate.get("minimum_portfolio_payoff_ratio"),
        "minimum_portfolio_payoff_ratio",
    ):
        reasons.append("PORTFOLIO_PAYOFF_RATIO_TOO_LOW")
    if portfolio_net_profit <= 0:
        reasons.append("PORTFOLIO_NET_PROFIT_NOT_POSITIVE")
    if maximum_drawdown > _finite_number(
        gate.get("maximum_drawdown_pct"),
        "maximum_drawdown_pct",
    ):
        reasons.append("PORTFOLIO_MAXIMUM_DRAWDOWN_TOO_HIGH")
    if positive_folds < _positive_int(
        gate.get("minimum_positive_outer_folds"),
        "minimum_positive_outer_folds",
    ):
        reasons.append("POSITIVE_OUTER_FOLD_COUNT_TOO_LOW")
    if not calibration_valid:
        reasons.append("CALIBRATION_DIRECTION_FAILED_IN_OUTER_FOLD")
    return reasons


def _metrics_from_trades(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [
        _finite_number(item.get("net_return_pct"), "trade.net_return_pct")
        for item in trades
    ]
    count = len(returns)
    if not returns:
        return {
            "trade_count": 0,
            "sample_count": 0,
            "win_rate": None,
            "net_expectancy_pct": None,
            "average_win_pct": None,
            "average_loss_pct": None,
            "payoff_ratio": None,
            "profit_factor": None,
            "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0,
            "total_net_return_pct": 0.0,
        }
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    average_win = gross_profit / len(wins) if wins else None
    average_loss = gross_loss / len(losses) if losses else None
    payoff = (
        average_win / average_loss
        if average_win is not None and average_loss not in (None, 0.0)
        else None
    )
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    total = sum(returns)
    return {
        "trade_count": count,
        "sample_count": count,
        "win_rate": len(wins) / count,
        "net_expectancy_pct": total / count,
        "average_win_pct": average_win,
        "average_loss_pct": average_loss,
        "payoff_ratio": payoff,
        "profit_factor": profit_factor,
        "gross_profit_pct": gross_profit,
        "gross_loss_pct": gross_loss,
        "total_net_return_pct": total,
    }


def _compare_core_metrics(
    recorded: Mapping[str, Any],
    recomputed: Mapping[str, Any],
    label: str,
) -> None:
    for key in _CORE_METRICS:
        expected = recomputed[key]
        actual = recorded.get(key)
        if key in {"trade_count", "sample_count"}:
            if _non_negative_int(actual, f"{label}.{key}") != expected:
                raise EvidenceContractError(f"{label}.{key} is inconsistent")
            continue
        parsed = _optional_finite_number(actual, f"{label}.{key}")
        if expected is None:
            if parsed is not None:
                raise EvidenceContractError(f"{label}.{key} is inconsistent")
        elif parsed is None or not math.isclose(
            parsed,
            float(expected),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise EvidenceContractError(f"{label}.{key} is inconsistent")


def _calibration_direction_valid(calibration: Mapping[str, Any]) -> bool:
    buckets = _require_list(calibration.get("buckets"), "calibration.buckets")
    if len(buckets) <= 1:
        return False
    ordered = sorted(
        (_require_mapping(item, "calibration bucket") for item in buckets),
        key=lambda item: _finite_number(item.get("lower_score"), "lower_score"),
    )
    returns = [
        _finite_number(item.get("expected_return_net_pct"), "bucket expected return")
        for item in ordered
    ]
    if returns[-1] <= 0:
        return False
    return all(right + 0.25 >= left for left, right in zip(returns, returns[1:]))


def _audit_stress_matrix(
    campaign: Mapping[str, Any],
    artifact: Mapping[str, Any],
    candidate_ids: tuple[str, ...],
) -> tuple[int, int, bool]:
    stress = _require_mapping(campaign.get("stress_tests"), "stress_tests")
    dimensions = (
        ("cost_multiplier", "cost_multipliers", _finite_number),
        ("execution_delay_sessions", "execution_delay_sessions", _non_negative_int),
        ("remove_best_trade_count", "remove_best_trade_counts", _non_negative_int),
        ("top_n", "top_n_sensitivity", _positive_int),
    )
    values: list[tuple[Any, ...]] = []
    for _, contract_key, parser in dimensions:
        raw = _require_list(stress.get(contract_key), f"stress_tests.{contract_key}")
        parsed = tuple(parser(item, contract_key) for item in raw)
        if not parsed or len(parsed) != len(set(parsed)):
            raise EvidenceContractError(f"{contract_key} must be non-empty and unique")
        values.append(parsed)
    expected = {
        (candidate_id, *scenario)
        for candidate_id in candidate_ids
        for scenario in itertools.product(*values)
    }
    recorded_raw = artifact.get("stress_test_matrix")
    if recorded_raw is None:
        return len(expected), 0, False
    recorded_rows = _require_list(recorded_raw, "artifact.stress_test_matrix")
    recorded: set[tuple[Any, ...]] = set()
    duplicate = False
    for row in recorded_rows:
        item = _require_mapping(row, "stress scenario")
        key = (
            _non_empty_string(item.get("candidate_id"), "stress candidate_id"),
            _finite_number(item.get("cost_multiplier"), "stress cost_multiplier"),
            _non_negative_int(
                item.get("execution_delay_sessions"),
                "stress execution_delay_sessions",
            ),
            _non_negative_int(
                item.get("remove_best_trade_count"),
                "stress remove_best_trade_count",
            ),
            _positive_int(item.get("top_n"), "stress top_n"),
        )
        duplicate = duplicate or key in recorded
        recorded.add(key)
    coordinate_coverage_complete = not duplicate and recorded == expected
    # V5.0 has no frozen scenario-result schema or raw scenario trades.  A
    # complete coordinate grid therefore cannot prove that the stress tests
    # actually ran.  Preserve the observed row count, but never call the
    # result matrix complete in this release.
    _ = coordinate_coverage_complete
    return len(expected), len(recorded_rows), False


def _governance_status(
    campaign: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> str:
    campaign_governance = campaign.get("research_governance")
    artifact_governance = artifact.get("research_governance")
    if campaign_governance is None:
        if artifact_governance is not None:
            raise EvidenceContractError("ungoverned campaign gained governance")
        return "LEGACY_UNGOVERNED"
    governance = _require_mapping(campaign_governance, "research_governance")
    if governance.get("research_classification") != "exploratory":
        raise EvidenceContractError("V5 historical classification must be exploratory")
    artifact_map = _require_mapping(
        artifact_governance,
        "artifact.research_governance",
    )
    if artifact_map.get("status") != "GOVERNED":
        raise EvidenceContractError("governed campaign artifact status differs")
    registrations = artifact_map.get("registrations")
    envelopes = artifact_map.get("result_envelopes")
    if not isinstance(registrations, list) or not registrations:
        raise EvidenceContractError("governance registrations are missing")
    if not isinstance(envelopes, Mapping) or not envelopes:
        raise EvidenceContractError("governance result envelopes are missing")
    return "RECORDED_GOVERNED_EXPLORATORY_BYTE_FROZEN_NOT_RECOMPUTED"


def _validate_execution_boundary(
    campaign: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> None:
    campaign_boundary = _require_mapping(
        campaign.get("execution_boundary"),
        "campaign.boundary",
    )
    artifact_boundary = _require_mapping(
        artifact.get("execution_boundary"),
        "artifact.boundary",
    )
    if artifact_boundary != campaign_boundary:
        raise EvidenceContractError("artifact execution boundary differs from campaign")
    for root, label in ((campaign, "campaign"), (artifact, "artifact")):
        boundary = _require_mapping(root.get("execution_boundary"), f"{label}.boundary")
        if boundary.get("research_only") is not True:
            raise EvidenceContractError(f"{label} must be research_only")
        if boundary.get("shadow_only") is not True:
            raise EvidenceContractError(f"{label} must be shadow_only")
        if boundary.get("real_order_submission") is not False:
            raise EvidenceContractError(f"{label} cannot submit real orders")
    if artifact.get("real_order_submission") is not False:
        raise EvidenceContractError("artifact real_order_submission must be false")


def _load_json(
    payload: bytes,
    *,
    label: str,
    permit_legacy_constants: bool = False,
) -> tuple[Any, list[str]]:
    constants: list[str] = []

    def parse_constant(value: str) -> None:
        if not permit_legacy_constants:
            raise EvidenceContractError(f"{label} contains non-finite {value}")
        constants.append(value)
        return None

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=parse_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError(f"{label} is not valid UTF-8 JSON") from exc
    return value, constants


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_non_finite_tree(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise EvidenceContractError(f"{path} contains a non-finite number")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_non_finite_tree(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_non_finite_tree(child, f"{path}[{index}]")


def _reject_true_activation_fields(value: Any, path: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in _ACTIVATION_KEYS and child is not False:
                raise EvidenceContractError(
                    f"{path}.{key} attempts to claim activation"
                )
            _reject_true_activation_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_true_activation_fields(child, f"{path}[{index}]")


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_expected_hash(
    *,
    label: str,
    actual: str,
    expected: str,
) -> None:
    if not isinstance(expected, str) or len(expected) != 64:
        raise EvidenceContractError(f"expected {label} SHA-256 is invalid")
    if actual != expected.lower():
        raise EvidenceContractError(f"{label} byte SHA-256 mismatch")


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError(f"{label} must be an object")
    return value


def _require_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise EvidenceContractError(f"{label} must be an array")
    return value


def _non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceContractError(f"{label} must be a non-empty string")
    return value


def _iso_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise EvidenceContractError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise EvidenceContractError(f"{label} must be canonical YYYY-MM-DD")
    return parsed


def _digest_text(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise EvidenceContractError(f"{label} must be a lowercase SHA-256")
    return value


def _finite_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceContractError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise EvidenceContractError(f"{label} must be finite")
    return result


def _optional_finite_number(value: Any, label: str) -> float | None:
    if value is None:
        return None
    return _finite_number(value, label)


def _optional_probability(value: Any, label: str) -> float | None:
    result = _optional_finite_number(value, label)
    if result is not None and not 0.0 <= result <= 1.0:
        raise EvidenceContractError(f"{label} must be within 0..1")
    return result


def _non_negative_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise EvidenceContractError(f"{label} must be a non-negative integer")
    return value


def _positive_int(value: Any, label: str) -> int:
    result = _non_negative_int(value, label)
    if result == 0:
        raise EvidenceContractError(f"{label} must be positive")
    return result


def _close(value: float) -> float:
    return round(float(value), 12)


__all__ = [
    "AUDIT_RELEASE_ID",
    "AUDIT_SCHEMA_VERSION",
    "CampaignAudit",
    "CandidateAudit",
    "EvidenceContractError",
    "audit_campaign_bytes",
]
