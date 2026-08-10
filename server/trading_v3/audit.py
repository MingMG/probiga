from __future__ import annotations

from collections import Counter
from statistics import mean
from typing import Any, Iterable


def build_counterfactual_records(
    decisions: Iterable[dict[str, Any]],
    outcomes: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    """Attach realized outcomes to accepted and rejected forecasts.

    This ledger is used to distinguish a sound rejection from a missed
    opportunity. It never rewrites the historical decision.
    """

    records: list[dict[str, Any]] = []
    for item in decisions:
        code = str(item.get("stock_code") or "")
        outcome = outcomes.get(code)
        if not code or not outcome:
            continue
        realized = float(outcome.get("net_return_pct") or 0.0)
        q10 = item.get("return_q10_pct")
        accepted = bool(item.get("accepted"))
        reason_code = str(item.get("reason_code") or "")
        missed = not accepted and realized > 0
        false_positive = accepted and realized <= 0
        calibration_breach = (
            q10 is not None and realized < float(q10)
        )
        records.append({
            "stock_code": code,
            "stock_name": str(item.get("stock_name") or ""),
            "strategy_key": str(item.get("strategy_key") or ""),
            "rank": int(item.get("rank") or 0),
            "accepted": accepted,
            "reason_code": reason_code,
            "expected_return_net_pct": item.get(
                "expected_return_net_pct"
            ),
            "realized_net_return_pct": realized,
            "realized_mae_pct": outcome.get("mae_pct"),
            "realized_mfe_pct": outcome.get("mfe_pct"),
            "missed_opportunity": missed,
            "false_positive": false_positive,
            "calibration_breach": calibration_breach,
            "attribution": (
                "MISSED_BY_" + (reason_code or "UNKNOWN")
                if missed
                else (
                    "FALSE_POSITIVE"
                    if false_positive
                    else "DECISION_SUPPORTED"
                )
            ),
        })
    return records


def opportunity_recall(
    ranked_decisions: Iterable[dict[str, Any]],
    outcomes: dict[str, dict[str, float]],
    *,
    top_ks: tuple[int, ...] = (20, 50),
    winner_threshold_pct: float = 3.0,
) -> dict[str, Any]:
    rows = sorted(
        (dict(item) for item in ranked_decisions),
        key=lambda item: int(item.get("rank") or 10**9),
    )
    winners = {
        code
        for code, outcome in outcomes.items()
        if float(outcome.get("net_return_pct") or 0.0)
        >= winner_threshold_pct
    }
    accepted = {
        str(item.get("stock_code") or "")
        for item in rows
        if item.get("accepted")
    }
    missed_reasons = Counter(
        str(item.get("reason_code") or "UNKNOWN")
        for item in rows
        if (
            str(item.get("stock_code") or "") in winners
            and not item.get("accepted")
        )
    )
    recalls: dict[str, float | None] = {}
    for top_k in top_ks:
        top_codes = {
            str(item.get("stock_code") or "")
            for item in rows[:top_k]
        }
        recalls[f"recall_at_{top_k}"] = (
            len(winners & top_codes) / len(winners)
            if winners
            else None
        )
    accepted_returns = [
        float(outcomes[code].get("net_return_pct") or 0.0)
        for code in accepted
        if code in outcomes
    ]
    return {
        **recalls,
        "winner_threshold_pct": winner_threshold_pct,
        "winner_count": len(winners),
        "accepted_winner_count": len(winners & accepted),
        "missed_winner_count": len(winners - accepted),
        "accepted_average_net_return_pct": (
            mean(accepted_returns) if accepted_returns else None
        ),
        "missed_reason_counts": dict(missed_reasons),
    }
