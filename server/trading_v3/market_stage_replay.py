"""Replay market-stage assessments and score only fully observable outcomes."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
import math
from statistics import mean
from typing import Any

from server.common.pit_facts import normalize_decision_at

from .market_stage import evaluate_market_stage, valid_market_bar
from .metrics import maximum_drawdown


HORIZONS = (5, 10, 20)
REFERENCE_SESSIONS = 20
SCHEMA_VERSION = "probiga.market-stage-replay.v1"
BOTTOM_STATES = frozenset({
    "STABILIZATION_ATTEMPT", "BOTTOMING_CANDIDATE", "TECHNICAL_REBOUND",
    "REVERSAL_CANDIDATE",
})
TRANSITION_STATES = frozenset({"TECHNICAL_REBOUND", "REVERSAL_CANDIDATE"})


def _trade_date(value: date | str) -> date:
    return date.fromisoformat(str(value))


def _number(row: Mapping[str, Any], key: str) -> float:
    value = float(row[key])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"invalid historical benchmark {key}")
    return value


def _has_reversal_evidence(assessment: Mapping[str, Any] | None) -> bool:
    if not assessment or assessment.get("status") != "READY":
        return False
    breadth = assessment.get("breadth") or {}
    if breadth.get("above_ma20_change_5d_pp") is None:
        return False
    periods = assessment["periods"]
    return (bool(periods["weekly"]["features"] and periods["monthly"]["features"])
            and periods["daily"]["features"].get("turnover_ratio") is not None)


def _outcome(
    *,
    assessment: Mapping[str, Any],
    close: float,
    future: Sequence[Mapping[str, Any]],
    trailing: Sequence[Mapping[str, Any]],
    following_assessments: Sequence[Mapping[str, Any] | None],
) -> dict[str, Any]:
    state = assessment["state"]
    closes = [_number(bar, "close") for bar in future]
    lows = [_number(bar, "low") for bar in future]
    highs = [_number(bar, "high") for bar in future]
    drawdown = maximum_drawdown([close, *closes])
    reference_low = None
    resistance = assessment.get("periods", {}).get("daily", {}).get("features", {}).get("resistance")
    first_low_offset = None
    if len(trailing) == REFERENCE_SESSIONS:
        trailing_lows = [_number(bar, "low") for bar in trailing]
        reference_low = min(trailing_lows)
        if state in BOTTOM_STATES:
            window_lows = trailing_lows + lows
            first_low_offset = window_lows.index(min(window_lows)) - (len(trailing) - 1)

    transition_evaluable = state in TRANSITION_STATES and all(map(_has_reversal_evidence, following_assessments))
    first_confirmation = None
    if transition_evaluable:
        first_confirmation = next((str(item["trade_date"]) for item in following_assessments
                                   if item is not None and item["state"] == "REVERSAL_CONFIRMED"), None)

    return {
        "end_date": str(future[-1]["trade_date"]),
        "return_pct": round((closes[-1] / close - 1) * 100, 6),
        "max_upside_pct": round(max(0.0, (max(highs) / close - 1) * 100), 6),
        "max_downside_pct": round(min(0.0, (min(lows) / close - 1) * 100), 6),
        "max_drawdown_pct": round(drawdown, 6) if drawdown is not None else None,
        "reference_low_20_sessions": reference_low,
        "new_20_session_low": min(lows) < reference_low if reference_low is not None else None,
        "decision_resistance": resistance,
        "resistance_broken": max(closes) > resistance if resistance is not None else None,
        "sessions_from_assessment_to_window_low": first_low_offset,
        "transition_evaluable": transition_evaluable,
        "first_reversal_confirmation_date": first_confirmation,
    }


def _summarize(rows: Sequence[Mapping[str, Any]], horizon: int) -> list[dict[str, Any]]:
    groups: dict[str | None, list[Mapping[str, Any] | None]] = {}
    for row in rows:
        if row["status"] != "READY":
            continue
        groups.setdefault(row["assessment"]["state"], []).append(row["outcomes"][str(horizon)])

    summary = []
    for state, outcomes in sorted(groups.items(), key=lambda group: group[0] or ""):
        observed = [outcome for outcome in outcomes if outcome is not None]
        lows = [item["new_20_session_low"] for item in observed if item["new_20_session_low"] is not None]
        transitions = [item for item in observed if item["transition_evaluable"]]
        confirmed = sum(item["first_reversal_confirmation_date"] is not None for item in transitions)
        lead_times = [item["sessions_from_assessment_to_window_low"] for item in observed
                      if item["sessions_from_assessment_to_window_low"] is not None]
        summary.append({
            "state": state,
            "assessment_count": len(outcomes),
            "matured_count": len(observed),
            "mean_return_pct": round(mean(item["return_pct"] for item in observed), 6) if observed else None,
            "new_low_evaluated_count": len(lows),
            "new_low_count": sum(lows),
            "new_low_rate": sum(lows) / len(lows) if lows else None,
            "window_low_lead_time_count": len(lead_times),
            "mean_sessions_to_window_low": mean(lead_times) if lead_times else None,
            "transition_evaluated_count": len(transitions),
            "reversal_confirmation_count": confirmed,
            "observed_reversal_confirmation_rate": confirmed / len(transitions) if transitions else None,
        })
    return summary


def replay_market_stage(
    bars: Sequence[Mapping[str, Any]],
    *,
    trading_sessions: Sequence[date | str],
    start_date: date | str,
    end_date: date | str,
    outcome_as_of: datetime | str,
    breadth_by_date: Mapping[date | str, Mapping[str, Any]] | None = None,
    decision_time: time | str = "20:00:00",
) -> dict[str, Any]:
    """Assess at each historical local cutoff using only evidence available then.

    The exchange calendar is the authority for offsets. Missing bars in a
    forward window censor its outcome. Only a finished observation window can
    support a realized frequency. Transition counts track the model's later
    classifications, while price outcomes are reported separately.
    """
    sessions = [_trade_date(day) for day in trading_sessions]
    if not sessions or sessions != sorted(set(sessions)):
        raise ValueError("market stage replay requires an ordered unique exchange calendar")
    start = _trade_date(start_date)
    end = _trade_date(end_date)
    if not sessions[0] <= start <= end <= sessions[-1]:
        raise ValueError("market stage replay dates exceed the exchange calendar")
    clock = time.fromisoformat(decision_time) if isinstance(decision_time, str) else decision_time
    if clock.tzinfo is not None or clock < time(15):
        raise ValueError("market stage replay requires a Shanghai close or post-close decision time")
    cutoff = normalize_decision_at(outcome_as_of)
    positions = {day: index for index, day in enumerate(sessions)}
    bar_by_date: dict[date, Mapping[str, Any]] = {}
    availability: dict[date, datetime] = {}
    seen: set[date] = set()
    for bar in bars:
        day = _trade_date(bar["trade_date"])
        if day not in positions or day in seen:
            raise ValueError("market stage replay found a duplicate or out-of-calendar benchmark bar")
        seen.add(day)
        available_at = normalize_decision_at(bar["available_at"])
        if available_at < datetime.combine(day, time(15)):
            raise ValueError("market stage replay benchmark bar predates session close")
        if not valid_market_bar(bar):
            continue
        bar_by_date[day] = bar
        availability[day] = available_at
    breadth = {_trade_date(day): value for day, value in (breadth_by_date or {}).items()}
    selected = [index for index, day in enumerate(sessions) if start <= day <= end]
    assessments: dict[date, Mapping[str, Any]] = {}
    if selected:
        stop = min(len(sessions), selected[-1] + 1 + max(HORIZONS))
        for index in range(selected[0], stop):
            day = sessions[index]
            decision_at = datetime.combine(day, clock)
            if decision_at > cutoff or day not in availability or availability[day] > decision_at:
                continue
            known_bars = [bar_by_date[date_] for date_ in sessions[:index + 1]
                          if date_ in availability and availability[date_] <= decision_at]
            assessments[day] = evaluate_market_stage(known_bars, as_of=decision_at,
                                                      trading_sessions=sessions, breadth=breadth.get(day))

    rows = []
    for index in selected:
        day = sessions[index]
        decision_at = datetime.combine(day, clock)
        assessment = assessments.get(day)
        status = str(assessment["status"]) if assessment else "DATA_BLOCKED"
        horizons: dict[str, dict[str, Any] | None] = {str(horizon): None for horizon in HORIZONS}
        if status == "READY":
            trailing_dates = sessions[max(0, index - REFERENCE_SESSIONS + 1):index + 1]
            trailing = [bar_by_date[date_] for date_ in trailing_dates
                        if date_ in availability and availability[date_] <= decision_at]
            for horizon in HORIZONS:
                future_dates = sessions[index + 1:index + horizon + 1]
                if len(future_dates) != horizon or any(
                    date_ not in availability or availability[date_] > cutoff for date_ in future_dates
                ):
                    continue
                horizons[str(horizon)] = _outcome(
                    assessment=assessment, close=_number(bar_by_date[day], "close"),
                    future=[bar_by_date[date_] for date_ in future_dates], trailing=trailing,
                    following_assessments=[assessments.get(date_) for date_ in future_dates],
                )
        rows.append({"trade_date": day.isoformat(), "decision_known_at": decision_at.isoformat(),
                     "status": status, "assessment": assessment, "outcomes": horizons})
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "READY" if any(row["status"] == "READY" for row in rows) else "DATA_BLOCKED",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "outcome_as_of": cutoff.isoformat(),
        "horizon_sessions": list(HORIZONS),
        "rows": rows,
        "summary": {str(horizon): _summarize(rows, horizon) for horizon in HORIZONS},
    }
