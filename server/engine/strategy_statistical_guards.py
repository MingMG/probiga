# -*- coding: utf-8 -*-
"""Deterministic statistical guards for dynamic strategy governance.

The functions in this module are deliberately pure and use only the Python
standard library.  They do not grant capital or order authority.  Callers are
expected to bind their results into the immutable governance state before a
strategy can pass a funding gate.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from statistics import NormalDist
from typing import Any, Iterable, Mapping, Sequence


_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_NORMAL = NormalDist()
_GUARD_FLAGS = {
    "automatic_real_order_submission": False,
    "real_order_authority": False,
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _finite_number(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric observation")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("observation is not finite")
    return result


def _stable_raw_value(value: Any) -> Any:
    """Represent even invalid inputs deterministically for failure hashes."""

    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "__NONFINITE_NAN__"
        if math.isinf(value):
            return "__NONFINITE_POS_INF__" if value > 0 else "__NONFINITE_NEG_INF__"
        return value
    return str(value)


def _guard_result(payload: dict[str, Any]) -> dict[str, Any]:
    bound = {**payload, **_GUARD_FLAGS}
    return {**bound, "result_hash": _digest(bound)}


def _invalid_guard(
    *, schema: str, reason: str, input_hash: str, parameter_hash: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _guard_result({
        "schema": schema,
        "valid": False,
        "passed": False,
        "reason": reason,
        "input_hash": input_hash,
        "parameter_hash": parameter_hash,
        **dict(extra or {}),
    })


def _long_run_variance(values: Sequence[float], bandwidth: int) -> float:
    """Newey-West long-run variance with Bartlett kernel weights."""

    count = len(values)
    mean = math.fsum(values) / count
    centered = [value - mean for value in values]
    variance = math.fsum(value * value for value in centered) / count
    for lag in range(1, bandwidth + 1):
        covariance = math.fsum(
            centered[index] * centered[index - lag]
            for index in range(lag, count)
        ) / count
        variance += 2.0 * (1.0 - lag / (bandwidth + 1.0)) * covariance
    # Bartlett Newey-West is positive semi-definite in exact arithmetic.
    # Remove only machine-roundoff negatives; material negatives are invalid.
    tolerance = 1e-12 * max(1.0, math.fsum(value * value for value in centered))
    if variance < -tolerance:
        raise ValueError("Newey-West long-run variance is negative")
    return max(0.0, variance)


def _one_sided_positive_test(
    estimate: float, standard_error: float, confidence_level: float,
) -> tuple[float, float]:
    alpha = 1.0 - confidence_level
    critical = _NORMAL.inv_cdf(confidence_level)
    lower_bound = estimate - critical * standard_error
    if standard_error > 0:
        p_value = 1.0 - _NORMAL.cdf(estimate / standard_error)
    elif estimate > 0:
        p_value = 0.0
    else:
        p_value = 1.0
    return lower_bound, min(1.0, max(0.0, p_value if alpha > 0 else 1.0))


def _log_ratio_guard(
    *, estimate: float, influence: Sequence[float], bandwidth: int,
    confidence_level: float,
) -> dict[str, float]:
    long_run_variance = _long_run_variance(influence, bandwidth)
    standard_error = math.sqrt(long_run_variance / len(influence))
    critical = _NORMAL.inv_cdf(confidence_level)
    log_estimate = math.log(estimate)
    lower_bound = math.exp(log_estimate - critical * standard_error)
    z_score = log_estimate / standard_error if standard_error > 0 else math.inf
    p_value = 0.0 if standard_error == 0 and log_estimate > 0 else (
        1.0 if standard_error == 0 else 1.0 - _NORMAL.cdf(z_score)
    )
    return {
        "estimate": estimate,
        "log_estimate": log_estimate,
        "log_standard_error": standard_error,
        "log_long_run_variance": long_run_variance,
        "one_sided_95_lcb": lower_bound,
        "one_sided_p_value_vs_one": min(1.0, max(0.0, p_value)),
    }


def newey_west_nav_statistics(
    records: Iterable[Mapping[str, Any]], *,
    confidence_level: float = 0.95,
    bandwidth: int | None = None,
    minimum_positive_days: int = 2,
    minimum_negative_days: int = 2,
) -> dict[str, Any]:
    """Return HAC uncertainty statistics for capital-weighted daily NAV.

    ``records`` must contain unique ISO ``trade_date`` values and finite
    ``return_pct`` observations strictly greater than -100.  Profit factor and
    payoff uncertainty are obtained on the log scale from per-day contribution
    vectors, preserving serial dependence through the same Bartlett HAC kernel.
    """

    schema = "probiga.strategy-nav-hac-guard.v1"
    raw_rows = [
        {
            "trade_date": _stable_raw_value(
                item.get("trade_date") if isinstance(item, Mapping) else None
            ),
            "return_pct": _stable_raw_value(
                item.get("return_pct") if isinstance(item, Mapping) else item
            ),
        }
        for item in records
    ]
    input_hash = _digest({"schema": schema, "records": raw_rows})
    requested_parameters = {
        "kernel": "BARTLETT",
        "bandwidth_rule": "FLOOR_SQRT_N" if bandwidth is None else "EXPLICIT",
        "requested_bandwidth": bandwidth,
        "confidence_level": confidence_level,
        "minimum_positive_days": minimum_positive_days,
        "minimum_negative_days": minimum_negative_days,
    }
    parameter_hash = _digest(requested_parameters)
    try:
        if not 0.5 < float(confidence_level) < 1.0:
            raise ValueError("confidence_level must be within (0.5, 1)")
        if minimum_positive_days < 2 or minimum_negative_days < 2:
            raise ValueError("positive and negative day minimums must be at least two")
        normalized: list[dict[str, Any]] = []
        seen_dates: set[str] = set()
        for raw in raw_rows:
            trade_date = date.fromisoformat(str(raw["trade_date"])).isoformat()
            value = _finite_number(raw["return_pct"])
            if trade_date in seen_dates:
                raise ValueError("daily NAV observations contain duplicate sessions")
            if value <= -100.0:
                raise ValueError("daily NAV return must be strictly greater than -100")
            seen_dates.add(trade_date)
            normalized.append({"trade_date": trade_date, "return_pct": value})
        normalized.sort(key=lambda item: item["trade_date"])
        values = [float(item["return_pct"]) for item in normalized]
        count = len(values)
        if count < minimum_positive_days + minimum_negative_days:
            raise ValueError("daily NAV sample is too short")
        resolved_bandwidth = (
            math.floor(math.sqrt(count)) if bandwidth is None else int(bandwidth)
        )
        if not 0 <= resolved_bandwidth < count:
            raise ValueError("Newey-West bandwidth must be within 0..n-1")
        positives = [value for value in values if value > 0]
        negatives = [value for value in values if value < 0]
        if len(positives) < minimum_positive_days:
            raise ValueError("positive daily NAV observations are insufficient")
        if len(negatives) < minimum_negative_days:
            raise ValueError("negative daily NAV observations are insufficient")

        mean = math.fsum(values) / count
        long_run_variance = _long_run_variance(values, resolved_bandwidth)
        standard_error = math.sqrt(long_run_variance / count)
        mean_lcb, mean_p = _one_sided_positive_test(
            mean, standard_error, confidence_level,
        )
        centered_variance = math.fsum(
            (value - mean) ** 2 for value in values
        ) / count
        if centered_variance <= 0:
            effective_sample_size = 1.0
        elif long_run_variance <= 0:
            effective_sample_size = float(count)
        else:
            effective_sample_size = count * centered_variance / long_run_variance
        effective_sample_size = min(float(count), max(1.0, effective_sample_size))

        positive_contributions = [max(value, 0.0) for value in values]
        negative_contributions = [max(-value, 0.0) for value in values]
        win_indicators = [1.0 if value > 0 else 0.0 for value in values]
        loss_indicators = [1.0 if value < 0 else 0.0 for value in values]
        positive_mean = math.fsum(positive_contributions) / count
        negative_mean = math.fsum(negative_contributions) / count
        win_probability = math.fsum(win_indicators) / count
        loss_probability = math.fsum(loss_indicators) / count
        if min(positive_mean, negative_mean, win_probability, loss_probability) <= 0:
            raise ValueError("profit-factor contribution vector is degenerate")

        profit_factor = positive_mean / negative_mean
        profit_factor_influence = [
            (positive - positive_mean) / positive_mean
            - (negative - negative_mean) / negative_mean
            for positive, negative in zip(
                positive_contributions, negative_contributions
            )
        ]
        payoff_ratio = (
            positive_mean / win_probability
        ) / (
            negative_mean / loss_probability
        )
        payoff_influence = [
            (positive - positive_mean) / positive_mean
            - (negative - negative_mean) / negative_mean
            - (win - win_probability) / win_probability
            + (loss - loss_probability) / loss_probability
            for positive, negative, win, loss in zip(
                positive_contributions,
                negative_contributions,
                win_indicators,
                loss_indicators,
            )
        ]
        pf_guard = _log_ratio_guard(
            estimate=profit_factor,
            influence=profit_factor_influence,
            bandwidth=resolved_bandwidth,
            confidence_level=confidence_level,
        )
        payoff_guard = _log_ratio_guard(
            estimate=payoff_ratio,
            influence=payoff_influence,
            bandwidth=resolved_bandwidth,
            confidence_level=confidence_level,
        )
        resolved_parameters = {
            **requested_parameters,
            "resolved_bandwidth": resolved_bandwidth,
            "observation_count": count,
        }
        result_payload = {
            "schema": schema,
            "valid": True,
            "passed": bool(
                mean_lcb > 0
                and pf_guard["one_sided_95_lcb"] > 1
                and payoff_guard["one_sided_95_lcb"] > 1
            ),
            "reason": "HAC置信下界通过" if (
                mean_lcb > 0
                and pf_guard["one_sided_95_lcb"] > 1
                and payoff_guard["one_sided_95_lcb"] > 1
            ) else "HAC置信下界未全部通过",
            "parameters": resolved_parameters,
            "parameter_hash": _digest(resolved_parameters),
            "input_hash": _digest({"schema": schema, "records": normalized}),
            "observation_count": count,
            "positive_day_count": len(positives),
            "negative_day_count": len(negatives),
            "zero_day_count": count - len(positives) - len(negatives),
            "net_expectancy_pct": mean,
            "net_expectancy_standard_error": standard_error,
            "net_expectancy_long_run_variance": long_run_variance,
            "net_expectancy_one_sided_95_lcb_pct": mean_lcb,
            "net_expectancy_one_sided_p_value": mean_p,
            "effective_sample_size": effective_sample_size,
            "profit_factor": pf_guard,
            "payoff_ratio": payoff_guard,
        }
        return _guard_result(result_payload)
    except (ArithmeticError, TypeError, ValueError) as exc:
        return _invalid_guard(
            schema=schema,
            reason=str(exc),
            input_hash=input_hash,
            parameter_hash=parameter_hash,
            extra={
                "parameters": requested_parameters,
                "observation_count": len(raw_rows),
            },
        )


def benjamini_yekutieli_fdr(
    p_values: Mapping[str, Any], *,
    total_hypotheses: int,
    q: float = 0.05,
    trial_inventory: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Apply deterministic Benjamini-Yekutieli FDR under arbitrary dependence.

    When the server-side trial inventory is larger than the available p-value
    set, its complete stable-key inventory is mandatory.  This prevents a
    caller from tightening or relaxing the family merely by omitting trials.
    """

    schema = "probiga.strategy-family-by-fdr.v1"
    raw_items = sorted(
        (str(key), _stable_raw_value(value)) for key, value in p_values.items()
    )
    raw_inventory = (
        sorted(str(key) for key in trial_inventory)
        if trial_inventory is not None else [key for key, _value in raw_items]
    )
    input_payload = {
        "schema": schema,
        "p_values": [{"key": key, "p_value": value} for key, value in raw_items],
        "total_hypotheses": total_hypotheses,
        "q": q,
        "trial_inventory": raw_inventory,
    }
    input_hash = _digest(input_payload)
    parameters = {
        "method": "BENJAMINI_YEKUTIELI_ARBITRARY_DEPENDENCE",
        "total_hypotheses": total_hypotheses,
        "q": q,
    }
    parameter_hash = _digest(parameters)
    inventory_hash = _digest({
        "schema": "probiga.strategy-trial-inventory.v1",
        "total_hypotheses": total_hypotheses,
        "trial_keys": raw_inventory,
    })
    try:
        if isinstance(total_hypotheses, bool) or int(total_hypotheses) < 1:
            raise ValueError("total_hypotheses must be positive")
        total = int(total_hypotheses)
        if not 0 < float(q) < 1:
            raise ValueError("q must be within (0, 1)")
        if not raw_items:
            raise ValueError("at least one p-value is required")
        if len({key for key, _value in raw_items}) != len(raw_items):
            raise ValueError("p-value keys collide after stable string normalization")
        if total < len(raw_items):
            raise ValueError("total_hypotheses cannot be below available p-values")
        if len(raw_inventory) != total or len(set(raw_inventory)) != total:
            raise ValueError("complete unique server trial inventory is required")
        observed_keys = {key for key, _value in raw_items}
        if not observed_keys.issubset(set(raw_inventory)):
            raise ValueError("p-value key is absent from trial inventory")
        normalized = []
        for key, raw_value in raw_items:
            if not key:
                raise ValueError("trial key cannot be empty")
            value = _finite_number(raw_value)
            if not 0 <= value <= 1:
                raise ValueError("p-value must be within [0, 1]")
            normalized.append((key, value))
        ordered = sorted(normalized, key=lambda item: (item[1], item[0]))
        harmonic = math.fsum(1.0 / index for index in range(1, total + 1))
        cutoff_rank = 0
        ranked_rows = []
        for rank, (key, value) in enumerate(ordered, 1):
            critical = rank * float(q) / (total * harmonic)
            if value <= critical:
                cutoff_rank = rank
            ranked_rows.append({
                "key": key,
                "p_value": value,
                "rank": rank,
                "critical_value": critical,
            })
        decisions_by_key = {
            row["key"]: {
                **row,
                "passed": row["rank"] <= cutoff_rank,
            }
            for row in ranked_rows
        }
        decisions = [decisions_by_key[key] for key in sorted(decisions_by_key)]
        return _guard_result({
            "schema": schema,
            "valid": True,
            "passed": cutoff_rank > 0,
            "reason": (
                "至少一个候选通过BY任意依赖FDR门槛"
                if cutoff_rank > 0 else "没有候选通过BY任意依赖FDR门槛"
            ),
            "method": parameters["method"],
            "q": float(q),
            "total_hypotheses": total,
            "available_p_value_count": len(normalized),
            "harmonic_correction": harmonic,
            "cutoff_rank": cutoff_rank,
            "trial_inventory_hash": inventory_hash,
            "input_hash": input_hash,
            "parameter_hash": parameter_hash,
            "decisions": decisions,
        })
    except (ArithmeticError, TypeError, ValueError) as exc:
        return _invalid_guard(
            schema=schema,
            reason=str(exc),
            input_hash=input_hash,
            parameter_hash=parameter_hash,
            extra={
                "method": parameters["method"],
                "q": q,
                "total_hypotheses": total_hypotheses,
                "trial_inventory_hash": inventory_hash,
                "decisions": [],
            },
        )


def _normalized_revision(value: Any) -> str:
    raw = str(value or "").strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def _confirmation_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    trade_date = date.fromisoformat(str(raw.get("trade_date") or "")).isoformat()
    passed = raw.get("passed")
    gate_hash = str(raw.get("funding_gate_hash") or "")
    revision = _normalized_revision(raw.get("evidence_revision_at"))
    if type(passed) is not bool:
        raise ValueError("confirmation passed flag must be boolean")
    if not _HASH_PATTERN.fullmatch(gate_hash):
        raise ValueError("confirmation funding gate hash is invalid")
    return {
        "trade_date": trade_date,
        "passed": passed,
        "funding_gate_hash": gate_hash,
        "evidence_revision_at": revision,
    }


def spaced_consecutive_gate_confirmations(
    current_evidence: Mapping[str, Any],
    history: Iterable[Mapping[str, Any]],
    authoritative_sessions_desc: Sequence[str], *,
    minimum_new_sessions: int,
    required_total_confirmations: int = 3,
) -> dict[str, Any]:
    """Count only fully continuous, sufficiently spaced passing milestones."""

    schema = "probiga.strategy-spaced-gate-confirmations.v1"
    raw_history = [dict(item) for item in history]
    raw_input = {
        "schema": schema,
        "current_evidence": {
            str(key): _stable_raw_value(value)
            for key, value in current_evidence.items()
        },
        "history": [
            {str(key): _stable_raw_value(value) for key, value in item.items()}
            for item in raw_history
        ],
        "authoritative_sessions_desc": list(authoritative_sessions_desc),
    }
    input_hash = _digest(raw_input)
    parameters = {
        "minimum_new_sessions": minimum_new_sessions,
        "required_total_confirmations": required_total_confirmations,
        "continuity": "EVERY_AUTHORITATIVE_SESSION_MUST_PASS",
        "milestone_spacing": "SESSION_INDEX_DISTANCE",
    }
    parameter_hash = _digest(parameters)
    try:
        if isinstance(minimum_new_sessions, bool) or minimum_new_sessions < 1:
            raise ValueError("minimum_new_sessions must be positive")
        if (
            isinstance(required_total_confirmations, bool)
            or required_total_confirmations < 1
        ):
            raise ValueError("required_total_confirmations must be positive")
        sessions = [date.fromisoformat(str(value)).isoformat()
                    for value in authoritative_sessions_desc]
        if not sessions or len(sessions) != len(set(sessions)):
            raise ValueError("authoritative session sequence is empty or duplicated")
        if sessions != sorted(sessions, reverse=True):
            raise ValueError("authoritative sessions must be strictly descending")
        current = _confirmation_row(current_evidence)
        if current["trade_date"] != sessions[0]:
            raise ValueError("current evidence is not bound to the newest session")
        if current["passed"] is not True:
            raise ValueError("current evidence did not pass")
        by_day: dict[str, dict[str, Any]] = {}
        for raw in raw_history:
            row = _confirmation_row(raw)
            day = row["trade_date"]
            if day in by_day:
                raise ValueError("history contains duplicate trade dates")
            by_day[day] = row
        if set(by_day) - set(sessions[1:]):
            raise ValueError("history contains dates outside authoritative sessions")

        prior_count = 0
        milestones = [{"session_index": 0, **current}]
        last_milestone_index = 0
        prior_revision = current["evidence_revision_at"]
        seen_hashes = {current["funding_gate_hash"]}
        continuous_session_count = 1
        for session_index, session_day in enumerate(sessions[1:], 1):
            row = by_day.get(session_day)
            if row is None:
                raise ValueError("confirmation history is missing an authoritative session")
            if row["passed"] is not True:
                raise ValueError("confirmation pass sequence is interrupted")
            if row["funding_gate_hash"] in seen_hashes:
                raise ValueError("confirmation history reuses a funding gate hash")
            if row["evidence_revision_at"] >= prior_revision:
                raise ValueError("confirmation revisions are not strictly descending")
            seen_hashes.add(row["funding_gate_hash"])
            prior_revision = row["evidence_revision_at"]
            continuous_session_count += 1
            if session_index - last_milestone_index >= minimum_new_sessions:
                prior_count += 1
                milestones.append({"session_index": session_index, **row})
                last_milestone_index = session_index
                if prior_count + 1 >= required_total_confirmations:
                    break
        total_confirmations = prior_count + 1
        return _guard_result({
            "schema": schema,
            "valid": True,
            "passed": total_confirmations >= required_total_confirmations,
            "reason": (
                "达到最小新增权威会话间隔确认门槛"
                if total_confirmations >= required_total_confirmations
                else "连续证据存在，但新增权威会话不足"
            ),
            "input_hash": input_hash,
            "parameter_hash": parameter_hash,
            "parameters": parameters,
            "prior_confirmation_count": prior_count,
            "total_confirmation_count": total_confirmations,
            "continuous_session_count": continuous_session_count,
            "milestones": milestones,
        })
    except (TypeError, ValueError) as exc:
        return _invalid_guard(
            schema=schema,
            reason=str(exc),
            input_hash=input_hash,
            parameter_hash=parameter_hash,
            extra={
                "parameters": parameters,
                "prior_confirmation_count": 0,
                "total_confirmation_count": 0,
                "continuous_session_count": 0,
                "milestones": [],
            },
        )


__all__ = [
    "benjamini_yekutieli_fdr",
    "newey_west_nav_statistics",
    "spaced_consecutive_gate_confirmations",
]
