from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from types import MappingProxyType
from zoneinfo import ZoneInfo


HORIZON_CONTRACT_SCHEMA = "probiga.trading-v3.horizon-forecast.v1"
HORIZON_OUTCOME_SCHEMA = "probiga.trading-v3.horizon-outcome.v1"
SUPPORTED_HORIZONS = frozenset({1, 5, 20})
EXCHANGE_TIMEZONE = ZoneInfo("Asia/Shanghai")


class HorizonContractError(ValueError):
    """Raised when forecast timing or evidence would be ambiguous."""


class PredictionKind(str, Enum):
    PROXY_SCORE = "PROXY_SCORE"
    CALIBRATED_OOS = "CALIBRATED_OOS"


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise HorizonContractError(f"{field} must not be empty")
    return result


def _number(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise HorizonContractError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise HorizonContractError(f"{field} must be finite")
    return result


def _aware(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = _text(value, field)
        try:
            result = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise HorizonContractError(
                f"{field} must be an ISO-8601 datetime"
            ) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise HorizonContractError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


def _date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        raise HorizonContractError(f"{field} must be a date, not datetime")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(_text(value, field))
    except ValueError as exc:
        raise HorizonContractError(f"{field} must be an ISO-8601 date") from exc


def _hash(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise HorizonContractError(
            f"{field} must be a 64-character SHA-256 digest"
        )
    return result


def _evidence_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise HorizonContractError(f"{field} must be a non-empty mapping")
    try:
        return json.loads(json.dumps(
            dict(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ))
    except (TypeError, ValueError) as exc:
        raise HorizonContractError(f"{field} must be canonical JSON") from exc


def _mapping_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CalibrationEvidence:
    evidence_id: str
    model_key: str
    model_version: str
    horizon_days: int
    dataset_hash: str
    feature_protocol_hash: str
    cost_model_version: str
    cost_assumption_pct: float
    matured_sample_count: int
    oos_sample_count: int
    walk_forward_fold_count: int
    outcomes_include_costs: bool
    score_direction_valid: bool
    calibration_mae: float
    brier_score: float
    generated_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "evidence_id"))
        object.__setattr__(self, "model_key", _text(self.model_key, "model_key"))
        object.__setattr__(self, "model_version", _text(self.model_version, "model_version"))
        if int(self.horizon_days) not in SUPPORTED_HORIZONS:
            raise HorizonContractError("horizon_days must be one of 1, 5 or 20")
        object.__setattr__(self, "horizon_days", int(self.horizon_days))
        object.__setattr__(self, "dataset_hash", _hash(self.dataset_hash, "dataset_hash"))
        object.__setattr__(
            self,
            "feature_protocol_hash",
            _hash(self.feature_protocol_hash, "feature_protocol_hash"),
        )
        object.__setattr__(
            self,
            "cost_model_version",
            _text(self.cost_model_version, "cost_model_version"),
        )
        cost = _number(self.cost_assumption_pct, "cost_assumption_pct")
        if cost < 0:
            raise HorizonContractError("cost_assumption_pct must not be negative")
        object.__setattr__(self, "cost_assumption_pct", cost)
        for field in (
            "matured_sample_count",
            "oos_sample_count",
            "walk_forward_fold_count",
        ):
            value = int(getattr(self, field))
            if value <= 0:
                raise HorizonContractError(f"{field} must be positive")
            object.__setattr__(self, field, value)
        if self.oos_sample_count > self.matured_sample_count:
            raise HorizonContractError(
                "oos_sample_count must not exceed matured_sample_count"
            )
        for field in ("calibration_mae", "brier_score"):
            value = _number(getattr(self, field), field)
            if not 0 <= value <= 1:
                raise HorizonContractError(f"{field} must be between 0 and 1")
            object.__setattr__(self, field, value)
        generated = _aware(self.generated_at, "generated_at")
        valid_until = _aware(self.valid_until, "valid_until")
        if valid_until <= generated:
            raise HorizonContractError("valid_until must follow generated_at")
        object.__setattr__(self, "generated_at", generated)
        object.__setattr__(self, "valid_until", valid_until)

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["generated_at"] = self.generated_at.isoformat()
        value["valid_until"] = self.valid_until.isoformat()
        return value


@dataclass(frozen=True, slots=True)
class HorizonForecastContract:
    """An immutable forecast whose label and execution clock are explicit."""

    forecast_id: str
    run_uid: str
    stock_code: str
    model_key: str
    model_version: str
    source_strategy_key: str
    source_forecast_hash: str
    source_evidence: Mapping[str, Any]
    decision_result_hash: str
    feature_protocol_hash: str
    model_artifact_hash: str
    model_inputs: Mapping[str, float]
    selection_status: str
    selection_reason_code: str
    selection_evidence_hash: str
    selection_evidence: Mapping[str, Any]
    horizon_days: int
    prediction_kind: PredictionKind | str
    decision_as_of: datetime
    feature_as_of: datetime
    decision_session_date: date
    entry_trade_date: date
    earliest_exit_trade_date: date
    outcome_matures_on: date
    entry_session_sequence: int
    earliest_exit_session_sequence: int
    outcome_maturity_session_sequence: int
    score: float
    expected_return_net_pct: float | None
    probability_positive: float | None
    cost_assumption_pct: float
    cost_model_version: str
    calibration_evidence: CalibrationEvidence | None = None
    imputed_feature_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field in (
            "forecast_id",
            "run_uid",
            "stock_code",
            "model_key",
            "model_version",
            "source_strategy_key",
            "selection_reason_code",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            kind = PredictionKind(self.prediction_kind)
        except ValueError as exc:
            raise HorizonContractError(
                "prediction_kind must be PROXY_SCORE or CALIBRATED_OOS"
            ) from exc
        object.__setattr__(self, "prediction_kind", kind)
        for field in (
            "source_forecast_hash",
            "decision_result_hash",
            "feature_protocol_hash",
            "model_artifact_hash",
            "selection_evidence_hash",
        ):
            object.__setattr__(self, field, _hash(getattr(self, field), field))
        source_evidence = _evidence_mapping(
            self.source_evidence, "source_evidence"
        )
        if _mapping_hash(source_evidence) != self.source_forecast_hash:
            raise HorizonContractError(
                "source_forecast_hash does not match source_evidence"
            )
        selection_evidence = _evidence_mapping(
            self.selection_evidence, "selection_evidence"
        )
        if _mapping_hash(selection_evidence) != self.selection_evidence_hash:
            raise HorizonContractError(
                "selection_evidence_hash does not match selection_evidence"
            )
        object.__setattr__(
            self, "source_evidence", MappingProxyType(source_evidence)
        )
        object.__setattr__(
            self, "selection_evidence", MappingProxyType(selection_evidence)
        )
        selection_status = _text(self.selection_status, "selection_status")
        if selection_status not in {"SELECTED", "REJECTED"}:
            raise HorizonContractError(
                "selection_status must be SELECTED or REJECTED"
            )
        object.__setattr__(self, "selection_status", selection_status)
        if not isinstance(self.model_inputs, Mapping) or not self.model_inputs:
            raise HorizonContractError("model_inputs must not be empty")
        normalized_inputs = {
            _text(key, "model_inputs key"): _number(
                value, f"model_inputs.{key}"
            )
            for key, value in self.model_inputs.items()
        }
        object.__setattr__(
            self,
            "model_inputs",
            MappingProxyType(dict(sorted(normalized_inputs.items()))),
        )
        horizon = int(self.horizon_days)
        if horizon not in SUPPORTED_HORIZONS:
            raise HorizonContractError("horizon_days must be one of 1, 5 or 20")
        object.__setattr__(self, "horizon_days", horizon)
        decision_at = _aware(self.decision_as_of, "decision_as_of")
        feature_at = _aware(self.feature_as_of, "feature_as_of")
        if feature_at > decision_at:
            raise HorizonContractError("feature_as_of must not follow decision_as_of")
        object.__setattr__(self, "decision_as_of", decision_at)
        object.__setattr__(self, "feature_as_of", feature_at)
        decision_session = _date(
            self.decision_session_date, "decision_session_date"
        )
        if decision_at.astimezone(EXCHANGE_TIMEZONE).date() != decision_session:
            raise HorizonContractError(
                "decision_session_date must match decision_as_of in Asia/Shanghai"
            )
        object.__setattr__(self, "decision_session_date", decision_session)
        entry = _date(self.entry_trade_date, "entry_trade_date")
        earliest_exit = _date(
            self.earliest_exit_trade_date, "earliest_exit_trade_date"
        )
        maturity = _date(self.outcome_matures_on, "outcome_matures_on")
        if entry <= decision_session:
            raise HorizonContractError(
                "entry_trade_date must follow the Shanghai decision session; same-close fills are forbidden"
            )
        if earliest_exit <= entry:
            raise HorizonContractError(
                "earliest_exit_trade_date must follow entry_trade_date for T+1"
            )
        if maturity < earliest_exit:
            raise HorizonContractError(
                "outcome_matures_on must not precede earliest_exit_trade_date"
            )
        if (maturity - entry).days < horizon:
            raise HorizonContractError(
                "outcome_matures_on is too early for the declared horizon"
            )
        try:
            entry_sequence = int(self.entry_session_sequence)
            exit_sequence = int(self.earliest_exit_session_sequence)
            maturity_sequence = int(self.outcome_maturity_session_sequence)
        except (TypeError, ValueError) as exc:
            raise HorizonContractError(
                "exchange session sequences must be integers"
            ) from exc
        if entry_sequence <= 0:
            raise HorizonContractError(
                "entry_session_sequence must follow the decision session"
            )
        if exit_sequence != entry_sequence + 1:
            raise HorizonContractError(
                "earliest_exit_session_sequence must enforce T+1"
            )
        if maturity_sequence != entry_sequence + horizon:
            raise HorizonContractError(
                "outcome_maturity_session_sequence must match the forecast horizon"
            )
        object.__setattr__(self, "entry_session_sequence", entry_sequence)
        object.__setattr__(
            self, "earliest_exit_session_sequence", exit_sequence
        )
        object.__setattr__(
            self, "outcome_maturity_session_sequence", maturity_sequence
        )
        object.__setattr__(self, "entry_trade_date", entry)
        object.__setattr__(self, "earliest_exit_trade_date", earliest_exit)
        object.__setattr__(self, "outcome_matures_on", maturity)
        object.__setattr__(self, "score", _number(self.score, "score"))
        cost = _number(self.cost_assumption_pct, "cost_assumption_pct")
        if cost < 0:
            raise HorizonContractError("cost_assumption_pct must not be negative")
        object.__setattr__(self, "cost_assumption_pct", cost)
        object.__setattr__(
            self,
            "cost_model_version",
            _text(self.cost_model_version, "cost_model_version"),
        )
        imputed_feature_keys = tuple(sorted({
            _text(item, "imputed_feature_keys item")
            for item in self.imputed_feature_keys
        }))
        if any(item not in self.model_inputs for item in imputed_feature_keys):
            raise HorizonContractError(
                "imputed_feature_keys must be a subset of model_inputs"
            )
        if kind is PredictionKind.PROXY_SCORE and imputed_feature_keys:
            raise HorizonContractError(
                "a proxy score must not claim model median imputation"
            )
        object.__setattr__(self, "imputed_feature_keys", imputed_feature_keys)

        if kind is PredictionKind.PROXY_SCORE:
            if self.expected_return_net_pct is not None:
                raise HorizonContractError(
                    "a proxy score must not claim expected_return_net_pct"
                )
            if self.probability_positive is not None:
                raise HorizonContractError(
                    "a proxy score must not claim probability_positive"
                )
            if self.calibration_evidence is not None:
                raise HorizonContractError(
                    "a proxy score must not attach calibration evidence"
                )
        else:
            expected = _number(
                self.expected_return_net_pct, "expected_return_net_pct"
            )
            probability = _number(
                self.probability_positive, "probability_positive"
            )
            if not 0 <= probability <= 1:
                raise HorizonContractError(
                    "probability_positive must be between 0 and 1"
                )
            evidence = self.calibration_evidence
            if evidence is None:
                raise HorizonContractError(
                    "CALIBRATED_OOS requires calibration_evidence"
                )
            if evidence.model_key != self.model_key:
                raise HorizonContractError("calibration model_key does not match")
            if evidence.model_version != self.model_version:
                raise HorizonContractError("calibration model_version does not match")
            if evidence.horizon_days != horizon:
                raise HorizonContractError("calibration horizon_days does not match")
            if evidence.feature_protocol_hash != self.feature_protocol_hash:
                raise HorizonContractError(
                    "calibration feature protocol does not match"
                )
            if not evidence.outcomes_include_costs:
                raise HorizonContractError(
                    "calibrated outcomes must include execution costs"
                )
            if abs(evidence.cost_assumption_pct - cost) > 1e-9:
                raise HorizonContractError(
                    "forecast and calibration cost assumptions must match"
                )
            if evidence.cost_model_version != self.cost_model_version:
                raise HorizonContractError(
                    "forecast and calibration cost model versions must match"
                )
            if not evidence.score_direction_valid:
                raise HorizonContractError(
                    "calibration score direction must be valid"
                )
            if not evidence.generated_at <= decision_at <= evidence.valid_until:
                raise HorizonContractError(
                    "calibration evidence is unavailable or expired at decision_as_of"
                )
            object.__setattr__(self, "expected_return_net_pct", expected)
            object.__setattr__(self, "probability_positive", probability)

    @property
    def decision_scope(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def order_authority(self) -> bool:
        return False

    def sample_is_mature(self, evaluation_date: date | str) -> bool:
        return _date(evaluation_date, "evaluation_date") >= self.outcome_matures_on

    def outcome_can_exit(self, exit_trade_date: date | str) -> bool:
        return _date(exit_trade_date, "exit_trade_date") >= self.earliest_exit_trade_date

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": HORIZON_CONTRACT_SCHEMA,
            "forecast_id": self.forecast_id,
            "run_uid": self.run_uid,
            "stock_code": self.stock_code,
            "model_key": self.model_key,
            "model_version": self.model_version,
            "source_strategy_key": self.source_strategy_key,
            "source_forecast_hash": self.source_forecast_hash,
            "source_evidence": dict(self.source_evidence),
            "decision_result_hash": self.decision_result_hash,
            "feature_protocol_hash": self.feature_protocol_hash,
            "model_artifact_hash": self.model_artifact_hash,
            "model_inputs": dict(self.model_inputs),
            "selection_status": self.selection_status,
            "selection_reason_code": self.selection_reason_code,
            "selection_evidence_hash": self.selection_evidence_hash,
            "selection_evidence": dict(self.selection_evidence),
            "horizon_days": self.horizon_days,
            "prediction_kind": self.prediction_kind.value,
            "decision_as_of": self.decision_as_of.isoformat(),
            "feature_as_of": self.feature_as_of.isoformat(),
            "decision_session_date": self.decision_session_date.isoformat(),
            "entry_trade_date": self.entry_trade_date.isoformat(),
            "earliest_exit_trade_date": self.earliest_exit_trade_date.isoformat(),
            "outcome_matures_on": self.outcome_matures_on.isoformat(),
            "entry_session_sequence": self.entry_session_sequence,
            "earliest_exit_session_sequence": (
                self.earliest_exit_session_sequence
            ),
            "outcome_maturity_session_sequence": (
                self.outcome_maturity_session_sequence
            ),
            "score": self.score,
            "expected_return_net_pct": self.expected_return_net_pct,
            "probability_positive": self.probability_positive,
            "cost_assumption_pct": self.cost_assumption_pct,
            "cost_model_version": self.cost_model_version,
            "calibration_evidence": (
                self.calibration_evidence.as_dict()
                if self.calibration_evidence is not None
                else None
            ),
            "imputed_feature_keys": list(self.imputed_feature_keys),
            "sample_maturity": "PENDING_UNTIL_OUTCOME_MATURES",
            "decision_scope": self.decision_scope,
            "order_authority": self.order_authority,
        }


@dataclass(frozen=True, slots=True)
class HorizonOutcomeEvidence:
    """Immutable label produced from one contract's frozen entry/exit sessions."""

    contract_id: str
    contract_hash: str
    stock_code: str
    horizon_days: int
    entry_trade_date: date
    exit_trade_date: date
    entry_price: float
    exit_price: float
    gross_return_pct: float
    realized_cost_pct: float
    realized_net_return_pct: float
    realized_mae_pct: float
    realized_mfe_pct: float
    bar_count: int
    cost_model_version: str
    market_data_source: str
    market_evidence_hash: str
    execution_feasibility: str
    outcome_status: str
    observed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "contract_id", _hash(self.contract_id, "contract_id")
        )
        object.__setattr__(
            self, "contract_hash", _hash(self.contract_hash, "contract_hash")
        )
        object.__setattr__(
            self, "market_evidence_hash",
            _hash(self.market_evidence_hash, "market_evidence_hash"),
        )
        object.__setattr__(self, "stock_code", _text(self.stock_code, "stock_code"))
        object.__setattr__(
            self,
            "cost_model_version",
            _text(self.cost_model_version, "cost_model_version"),
        )
        object.__setattr__(
            self,
            "market_data_source",
            _text(self.market_data_source, "market_data_source"),
        )
        status = _text(self.outcome_status, "outcome_status")
        if status not in {"MATURED_VERIFIED", "QUARANTINED"}:
            raise HorizonContractError(
                "outcome_status must be MATURED_VERIFIED or QUARANTINED"
            )
        object.__setattr__(self, "outcome_status", status)
        feasibility = _text(
            self.execution_feasibility, "execution_feasibility"
        )
        if feasibility not in {
            "UNVERIFIED_RESEARCH", "EXECUTABLE_VERIFIED"
        }:
            raise HorizonContractError("execution_feasibility is invalid")
        object.__setattr__(self, "execution_feasibility", feasibility)
        horizon = int(self.horizon_days)
        if horizon not in SUPPORTED_HORIZONS:
            raise HorizonContractError("horizon_days must be one of 1, 5 or 20")
        object.__setattr__(self, "horizon_days", horizon)
        entry_date = _date(self.entry_trade_date, "entry_trade_date")
        exit_date = _date(self.exit_trade_date, "exit_trade_date")
        if exit_date <= entry_date:
            raise HorizonContractError("exit_trade_date must follow entry_trade_date")
        object.__setattr__(self, "entry_trade_date", entry_date)
        object.__setattr__(self, "exit_trade_date", exit_date)
        entry_price = _number(self.entry_price, "entry_price")
        exit_price = _number(self.exit_price, "exit_price")
        if entry_price <= 0 or exit_price <= 0:
            raise HorizonContractError("entry_price and exit_price must be positive")
        object.__setattr__(self, "entry_price", entry_price)
        object.__setattr__(self, "exit_price", exit_price)
        gross = _number(self.gross_return_pct, "gross_return_pct")
        cost = _number(self.realized_cost_pct, "realized_cost_pct")
        net = _number(self.realized_net_return_pct, "realized_net_return_pct")
        if cost < 0:
            raise HorizonContractError("realized_cost_pct must not be negative")
        price_gross = (exit_price / entry_price - 1.0) * 100.0
        if abs(gross - price_gross) > 1e-6:
            raise HorizonContractError("gross_return_pct does not match frozen prices")
        if abs(net - (gross - cost)) > 1e-6:
            raise HorizonContractError("realized_net_return_pct must include cost")
        object.__setattr__(self, "gross_return_pct", gross)
        object.__setattr__(self, "realized_cost_pct", cost)
        object.__setattr__(self, "realized_net_return_pct", net)
        object.__setattr__(
            self,
            "realized_mae_pct",
            _number(self.realized_mae_pct, "realized_mae_pct"),
        )
        object.__setattr__(
            self,
            "realized_mfe_pct",
            _number(self.realized_mfe_pct, "realized_mfe_pct"),
        )
        bar_count = int(self.bar_count)
        if bar_count != horizon + 1:
            raise HorizonContractError(
                "bar_count must cover every frozen entry-to-maturity session"
            )
        object.__setattr__(self, "bar_count", bar_count)
        object.__setattr__(self, "observed_at", _aware(self.observed_at, "observed_at"))

    @property
    def order_authority(self) -> bool:
        return False

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": HORIZON_OUTCOME_SCHEMA,
            "contract_id": self.contract_id,
            "contract_hash": self.contract_hash,
            "stock_code": self.stock_code,
            "horizon_days": self.horizon_days,
            "entry_trade_date": self.entry_trade_date.isoformat(),
            "exit_trade_date": self.exit_trade_date.isoformat(),
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "gross_return_pct": self.gross_return_pct,
            "realized_cost_pct": self.realized_cost_pct,
            "realized_net_return_pct": self.realized_net_return_pct,
            "realized_mae_pct": self.realized_mae_pct,
            "realized_mfe_pct": self.realized_mfe_pct,
            "bar_count": self.bar_count,
            "cost_model_version": self.cost_model_version,
            "market_data_source": self.market_data_source,
            "market_evidence_hash": self.market_evidence_hash,
            "execution_feasibility": self.execution_feasibility,
            "observed_at": self.observed_at.isoformat(),
            "outcome_status": self.outcome_status,
            "order_authority": False,
        }


def validate_independent_horizon_suite(
    contracts: Iterable[HorizonForecastContract],
) -> dict[str, Any]:
    """Require genuinely separate T+1, T+5 and T+20 model contracts."""

    rows = tuple(contracts)
    if len(rows) != 3:
        raise HorizonContractError("a horizon suite must contain exactly three forecasts")
    horizons = {item.horizon_days for item in rows}
    if horizons != SUPPORTED_HORIZONS:
        raise HorizonContractError("a horizon suite must contain T+1, T+5 and T+20")
    if len({item.forecast_id for item in rows}) != len(rows):
        raise HorizonContractError("forecast_id must be unique across horizons")
    if len({item.model_key for item in rows}) != len(rows):
        raise HorizonContractError("each horizon requires an independent model_key")
    if len({item.model_artifact_hash for item in rows}) != len(rows):
        raise HorizonContractError(
            "each horizon requires an independent model artifact"
        )
    if len({item.feature_protocol_hash for item in rows}) != len(rows):
        raise HorizonContractError(
            "each horizon requires an independent feature protocol"
        )
    if len({item.source_strategy_key for item in rows}) != len(rows):
        raise HorizonContractError(
            "each horizon suite member requires an independent source sleeve"
        )
    if len({item.stock_code for item in rows}) != 1:
        raise HorizonContractError("a horizon suite must refer to one stock")
    if len({item.run_uid for item in rows}) != 1:
        raise HorizonContractError("a horizon suite must refer to one frozen run")
    if len({item.decision_as_of for item in rows}) != 1:
        raise HorizonContractError(
            "a horizon suite must share one decision_as_of"
        )
    if len({item.feature_as_of for item in rows}) != 1:
        raise HorizonContractError(
            "a horizon suite must share one feature cutoff"
        )
    if len({item.decision_session_date for item in rows}) != 1:
        raise HorizonContractError(
            "a horizon suite must share one Shanghai decision session"
        )
    if len({item.entry_trade_date for item in rows}) != 1 or len({
        item.entry_session_sequence for item in rows
    }) != 1:
        raise HorizonContractError(
            "a horizon suite must share one earliest entry session"
        )
    calibrated_ids = [
        item.calibration_evidence.evidence_id
        for item in rows
        if item.calibration_evidence is not None
    ]
    if len(calibrated_ids) != len(set(calibrated_ids)):
        raise HorizonContractError(
            "calibration evidence must not be reused across horizons"
        )
    return {
        "schema_version": HORIZON_CONTRACT_SCHEMA,
        "status": "VALID",
        "stock_code": rows[0].stock_code,
        "run_uid": rows[0].run_uid,
        "horizons": {
            f"T+{item.horizon_days}": item.as_dict()
            for item in sorted(rows, key=lambda value: value.horizon_days)
        },
        "independent_model_keys": True,
        "independent_calibration_evidence": (
            len(calibrated_ids) == 3
        ),
        "calibration_evidence_status": (
            "INDEPENDENT_CALIBRATED_OOS"
            if len(calibrated_ids) == 3
            else "NOT_APPLICABLE_PROXY"
        ),
        "order_authority": False,
    }
