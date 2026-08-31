"""Deterministic read-model enrichments for the daily V3 stock pool.

The immutable forecast and target ledgers remain the ranking authority.  This
module adds desk-facing context (daily change, dynamic role and related
alternatives) without rewriting either ledger or manufacturing order
authority.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


CANDIDATE_FORECAST_STATUSES = frozenset({
    "VALIDATED_POSITIVE",
    "PAPER_DISCOVERY_CANDIDATE",
    "LEFT_SIDE_PREPARE",
})

_ACTION_PRIORITY = {
    "BUY_ZONE": 0,
    "WAIT_TRIGGER": 1,
    "PAPER_ONLY": 2,
    "RESEARCH_ONLY": 3,
    "REJECTED": 4,
}


def _number(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _code(value: Any) -> str:
    return str(value or "").strip().split(".", 1)[0].zfill(6)


def _primary_theme(item: Mapping[str, Any]) -> str:
    features = item.get("features")
    features = features if isinstance(features, Mapping) else {}
    values = [
        item.get("theme_code"),
        features.get("theme_name"),
        *((item.get("theme_codes") or []) if isinstance(
            item.get("theme_codes"), (list, tuple)
        ) else []),
        *((features.get("theme_names") or []) if isinstance(
            features.get("theme_names"), (list, tuple)
        ) else []),
    ]
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    strategy_keys = item.get("strategy_keys") or []
    strategy = str(strategy_keys[0] if strategy_keys else "").strip()
    return f"未归属主题:{strategy or 'unknown'}"


def _sort_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    raw_score = _number(item.get("raw_score"))
    expected_return = _number(item.get("expected_return_net_pct"))
    rank = item.get("rank_no")
    try:
        normalized_rank = int(rank)
    except (TypeError, ValueError):
        normalized_rank = 999_999
    return (
        _ACTION_PRIORITY.get(str(item.get("actionability") or ""), 9),
        -(raw_score if raw_score is not None else -1_000_000.0),
        -(expected_return if expected_return is not None else -1_000_000.0),
        normalized_rank,
        _code(item.get("stock_code")),
    )


def _role_for(
    item: Mapping[str, Any],
    *,
    theme_rank: int,
    theme_size: int,
) -> str:
    features = item.get("features")
    features = features if isinstance(features, Mapping) else {}
    leadership = _number(features.get("stock_leadership_score"))
    if leadership is not None:
        if leadership >= 0.78:
            return "LEADER"
        if leadership >= 0.48:
            return "CORE"
        if _number(features.get("theme_opportunity_score")) is not None:
            return "CONDITIONAL"
    if theme_size <= 1:
        return "INDEPENDENT"
    if theme_rank == 1:
        return "PRIMARY"
    if theme_rank <= 3:
        return "CORE_ALTERNATIVE"
    return "OBSERVE"


def candidate_snapshot_from_forecast_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Project a prior immutable forecast ledger for continuity comparison."""

    grouped: dict[str, dict[str, Any]] = {}
    for raw in rows:
        code = _code(raw.get("stock_code"))
        if not code.strip("0"):
            continue
        status = str(raw.get("forecast_status") or "").strip()
        item = grouped.setdefault(code, {
            "stock_code": code,
            "stock_name": str(raw.get("short_name") or code),
            "rank_no": None,
            "raw_score": None,
            "theme_codes": [],
            "strategy_keys": [],
            "forecast_statuses": [],
            "is_strategy_candidate": False,
            "actionability": "RESEARCH_ONLY",
            "features": {},
        })
        try:
            rank = int(raw.get("rank_no"))
        except (TypeError, ValueError):
            rank = None
        if rank is not None and (
            item["rank_no"] is None or rank < item["rank_no"]
        ):
            item["rank_no"] = rank
        score = _number(raw.get("raw_score"))
        if score is not None and (
            item["raw_score"] is None or score > item["raw_score"]
        ):
            item["raw_score"] = score
        for source, target in (
            ("theme_code", "theme_codes"),
            ("strategy_key", "strategy_keys"),
            ("forecast_status", "forecast_statuses"),
        ):
            value = str(raw.get(source) or "").strip()
            if value and value not in item[target]:
                item[target].append(value)
        if status in CANDIDATE_FORECAST_STATUSES:
            item["is_strategy_candidate"] = True
    return list(grouped.values())


def build_strategy_execution_summary(
    forecast_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Describe which strategy sleeves produced evidence in one daily run."""

    groups: dict[str, dict[str, Any]] = {}
    for raw in forecast_rows:
        strategy = str(raw.get("strategy_key") or "").strip()
        if not strategy:
            continue
        row = groups.setdefault(strategy, {
            "strategy_key": strategy,
            "forecast_count": 0,
            "candidate_count": 0,
            "insufficient_data_count": 0,
            "top_stock_code": "",
            "top_stock_name": "",
            "top_raw_score": None,
        })
        row["forecast_count"] += 1
        status = str(raw.get("forecast_status") or "").strip()
        if status in CANDIDATE_FORECAST_STATUSES:
            row["candidate_count"] += 1
        if status == "INSUFFICIENT_DATA":
            row["insufficient_data_count"] += 1
        score = _number(raw.get("raw_score"))
        if score is not None and (
            row["top_raw_score"] is None or score > row["top_raw_score"]
        ):
            row["top_raw_score"] = score
            row["top_stock_code"] = _code(raw.get("stock_code"))
            row["top_stock_name"] = str(
                raw.get("short_name") or row["top_stock_code"]
            )
    strategies = []
    for strategy in sorted(groups):
        row = groups[strategy]
        if row["forecast_count"] == row["insufficient_data_count"]:
            status = "DATA_BLOCKED"
        elif row["candidate_count"]:
            status = "COMPLETED_WITH_CANDIDATES"
        else:
            status = "COMPLETED_NO_CANDIDATE"
        strategies.append({**row, "status": status})
    return {
        "strategy_count": len(strategies),
        "completed_count": sum(
            row["status"].startswith("COMPLETED") for row in strategies
        ),
        "blocked_count": sum(
            row["status"] == "DATA_BLOCKED" for row in strategies
        ),
        "candidate_strategy_count": sum(
            row["candidate_count"] > 0 for row in strategies
        ),
        "strategies": strategies,
    }


def enrich_candidate_dynamics(
    items: Iterable[Mapping[str, Any]],
    *,
    previous_items: Iterable[Mapping[str, Any]] = (),
    previous_batch_available: bool | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Add dynamic roles and daily change without changing source ranking."""

    enriched = [dict(item) for item in items]
    previous = {
        _code(item.get("stock_code")): dict(item)
        for item in previous_items
        if item.get("is_strategy_candidate") is True
    }
    has_previous_batch = (
        bool(previous)
        if previous_batch_available is None
        else bool(previous_batch_available)
    )
    current_candidates = {
        _code(item.get("stock_code")): item
        for item in enriched
        if item.get("is_strategy_candidate") is True
    }
    theme_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in current_candidates.values():
        item["primary_theme"] = _primary_theme(item)
        theme_groups[item["primary_theme"]].append(item)

    for theme, rows in theme_groups.items():
        ordered = sorted(rows, key=_sort_key)
        for theme_rank, item in enumerate(ordered, 1):
            item["theme_rank"] = theme_rank
            item["dynamic_role"] = _role_for(
                item,
                theme_rank=theme_rank,
                theme_size=len(ordered),
            )
            item["related_candidates"] = [
                {
                    "stock_code": _code(other.get("stock_code")),
                    "stock_name": str(
                        other.get("stock_name")
                        or other.get("short_name")
                        or other.get("stock_code")
                    ),
                    "theme_rank": index,
                    "relation": "SAME_SCENARIO_CANDIDATE",
                }
                for index, other in enumerate(ordered, 1)
                if other is not item
            ][:5]

    counts = defaultdict(int)
    for code, item in current_candidates.items():
        prior = previous.get(code)
        current_rank = item.get("rank_no")
        prior_rank = prior.get("rank_no") if prior else None
        current_score = _number(item.get("raw_score"))
        prior_score = _number(prior.get("raw_score")) if prior else None
        if prior is None and not has_previous_batch:
            change = "BASELINE"
            explanation = (
                "缺少更早的可验证批次，当前仅作为后续日变化的比较基线"
            )
        elif prior is None:
            change = "NEW"
            explanation = "本交易日首次进入策略候选池"
        else:
            try:
                rank_delta = int(prior_rank) - int(current_rank)
            except (TypeError, ValueError):
                rank_delta = 0
            if rank_delta > 0:
                change = "UPGRADED"
                explanation = f"相对上一批次排名提升{rank_delta}位"
            elif rank_delta < 0:
                change = "DOWNGRADED"
                explanation = f"相对上一批次排名下降{abs(rank_delta)}位"
            else:
                change = "RETAINED"
                explanation = "交易逻辑仍在候选范围内，排名保持稳定"
        score_delta = (
            round(current_score - prior_score, 6)
            if current_score is not None and prior_score is not None
            else None
        )
        if score_delta is not None and score_delta != 0:
            explanation += (
                f"；原始分较上一批次{'增加' if score_delta > 0 else '减少'}"
                f"{abs(score_delta):.3f}"
            )
        item["daily_change"] = change
        item["previous_rank_no"] = prior_rank
        item["raw_score_delta"] = score_delta
        item["continuity_explanation"] = explanation
        counts[change] += 1

    removed_codes = sorted(set(previous) - set(current_candidates))
    return enriched, {
        "status": "READY" if has_previous_batch else "NO_PREVIOUS_BATCH",
        "baseline_count": counts["BASELINE"],
        "new_count": counts["NEW"],
        "retained_count": counts["RETAINED"],
        "upgraded_count": counts["UPGRADED"],
        "downgraded_count": counts["DOWNGRADED"],
        "removed_count": len(removed_codes),
        "removed_stock_codes": removed_codes[:50],
    }


__all__ = [
    "CANDIDATE_FORECAST_STATUSES",
    "build_strategy_execution_summary",
    "candidate_snapshot_from_forecast_rows",
    "enrich_candidate_dynamics",
]
