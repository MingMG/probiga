from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .portfolio_constraints import estimate_roundtrip_cost_pct


BATCH_DIFF_SCHEMA = "probiga.trading-v3.batch-diff.v1"
PORTFOLIO_INTELLIGENCE_SCHEMA = (
    "probiga.trading-v3.portfolio-intelligence.v1"
)


class DecisionIntelligenceError(ValueError):
    """Raised when an advisory calculation cannot be made safely."""


def _required_text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DecisionIntelligenceError(f"{field} must not be empty")
    return result


def _finite(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionIntelligenceError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise DecisionIntelligenceError(f"{field} must be finite")
    return result


def _non_negative(value: Any, field: str) -> float:
    result = _finite(value, field)
    if result < 0:
        raise DecisionIntelligenceError(f"{field} must not be negative")
    return result


def _ratio(value: Any, field: str) -> float:
    result = _finite(value, field)
    if not 0 <= result <= 1:
        raise DecisionIntelligenceError(f"{field} must be between 0 and 1")
    return result


def _optional_finite(value: Any, field: str) -> float | None:
    if value is None or value == "":
        return None
    return _finite(value, field)


def _aware_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = _required_text(value, field)
        try:
            result = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise DecisionIntelligenceError(
                f"{field} must be an ISO-8601 datetime"
            ) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise DecisionIntelligenceError(f"{field} must include a timezone")
    return result


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        try:
            values = tuple(value)
        except TypeError as exc:
            raise DecisionIntelligenceError(
                "a string collection was expected"
            ) from exc
    return tuple(sorted({str(item).strip() for item in values if str(item).strip()}))


def _decision_key(item: Mapping[str, Any], index: int) -> str:
    # forecast_id is a persistence UUID generated afresh for every run.  Using
    # it here would turn every unchanged stock/strategy/horizon into a false
    # remove+add pair.  Only an explicitly stable cross-run id may override the
    # economic composite key.
    explicit = str(item.get("decision_item_id") or "").strip()
    if explicit:
        return explicit
    stock_code = _required_text(item.get("stock_code"), f"items[{index}].stock_code")
    strategy_key = _required_text(
        item.get("strategy_key"), f"items[{index}].strategy_key"
    )
    try:
        horizon_days = int(item.get("horizon_days"))
    except (TypeError, ValueError) as exc:
        raise DecisionIntelligenceError(
            f"items[{index}].horizon_days must be an integer"
        ) from exc
    if horizon_days <= 0:
        raise DecisionIntelligenceError(
            f"items[{index}].horizon_days must be positive"
        )
    return f"{stock_code}|{strategy_key}|T+{horizon_days}"


def _normalise_batch(batch: Mapping[str, Any], name: str) -> dict[str, Any]:
    run_uid = _required_text(batch.get("run_uid"), f"{name}.run_uid")
    decision_as_of = _aware_datetime(
        batch.get("decision_as_of"), f"{name}.decision_as_of"
    )
    raw_items = batch.get("items")
    if isinstance(raw_items, (str, bytes)) or not isinstance(raw_items, Iterable):
        raise DecisionIntelligenceError(f"{name}.items must be an iterable")
    items: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(raw_items):
        if not isinstance(raw, Mapping):
            raise DecisionIntelligenceError(f"{name}.items[{index}] must be an object")
        item = dict(raw)
        key = _decision_key(item, index)
        if key in items:
            raise DecisionIntelligenceError(
                f"{name}.items contains duplicate decision key {key}"
            )
        item["decision_key"] = key
        item["gate_codes"] = list(_strings(item.get("gate_codes")))
        item["theme_codes"] = list(_strings(item.get("theme_codes")))
        item["target_weight"] = _optional_finite(
            item.get("target_weight"), f"{name}.items[{index}].target_weight"
        )
        items[key] = item
    return {
        "run_uid": run_uid,
        "decision_as_of": decision_as_of,
        "items": items,
    }


def diff_run_batches(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare two immutable decision batches without inferring missing data.

    A stable forecast id is preferred.  When it is unavailable, the tuple
    ``stock_code, strategy_key, horizon_days`` becomes the comparison key.
    Duplicate or incomplete keys fail the whole comparison instead of
    silently merging forecasts.
    """

    before = _normalise_batch(previous, "previous")
    after = _normalise_batch(current, "current")
    if after["decision_as_of"] < before["decision_as_of"]:
        raise DecisionIntelligenceError(
            "current.decision_as_of must not precede previous.decision_as_of"
        )
    before_items = before["items"]
    after_items = after["items"]
    added_keys = sorted(after_items.keys() - before_items.keys())
    removed_keys = sorted(before_items.keys() - after_items.keys())
    tracked = (
        "selection_status",
        "portfolio_selected",
        "grade",
        "action",
        "target_weight",
        "expected_return_net_pct",
        "conservative_return_pct",
        "gate_codes",
        "theme_codes",
        "model_version",
        "evidence_as_of",
        "valid_until",
    )
    changes: list[dict[str, Any]] = []
    field_counts: defaultdict[str, int] = defaultdict(int)
    for key in sorted(before_items.keys() & after_items.keys()):
        old = before_items[key]
        new = after_items[key]
        fields = []
        for field in tracked:
            if old.get(field) != new.get(field):
                fields.append({
                    "field": field,
                    "before": old.get(field),
                    "after": new.get(field),
                })
                field_counts[field] += 1
        if fields:
            changes.append({
                "decision_key": key,
                "stock_code": str(new.get("stock_code") or old.get("stock_code") or ""),
                "changes": fields,
            })
    return {
        "schema_version": BATCH_DIFF_SCHEMA,
        "status": "CHANGED" if added_keys or removed_keys or changes else "UNCHANGED",
        "previous_run_uid": before["run_uid"],
        "current_run_uid": after["run_uid"],
        "previous_decision_as_of": before["decision_as_of"].isoformat(),
        "current_decision_as_of": after["decision_as_of"].isoformat(),
        "added": [after_items[key] for key in added_keys],
        "removed": [before_items[key] for key in removed_keys],
        "changed": changes,
        "summary": {
            "previous_count": len(before_items),
            "current_count": len(after_items),
            "added_count": len(added_keys),
            "removed_count": len(removed_keys),
            "changed_count": len(changes),
            "field_change_counts": dict(sorted(field_counts.items())),
        },
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }


@dataclass(frozen=True, slots=True)
class ReplacementOption:
    candidate_code: str
    incumbent_code: str
    candidate_forward_net_pct: float
    incumbent_forward_net_pct: float
    incremental_net_edge_pct: float
    replacement_weight: float
    capacity_weight: float
    post_replacement_theme_weights: dict[str, float]
    eligible: bool
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _theme_weights(
    holdings: Iterable[Mapping[str, Any]],
) -> defaultdict[str, float]:
    result: defaultdict[str, float] = defaultdict(float)
    for index, holding in enumerate(holdings):
        weight = _ratio(holding.get("current_weight"), f"holdings[{index}].current_weight")
        for theme in _strings(holding.get("theme_codes")):
            result[theme] += weight
    return result


def analyze_replacement_opportunities(
    candidates: Iterable[Mapping[str, Any]],
    holdings: Iterable[Mapping[str, Any]],
    *,
    equity_cny: float,
    maximum_participation_rate: float,
    capacity_sessions: int,
    maximum_theme_weight: float,
    minimum_incremental_net_edge_pct: float,
) -> dict[str, Any]:
    """Rank candidate-for-holding substitutions after cost and capacity.

    Expected returns are deliberately supplied as *gross* values.  Candidate
    entry/exit costs and incumbent exit cost are deducted here, preventing a
    caller from comparing a gross candidate score with a net holding score.
    T+1 locked holdings are reported but never marked eligible.
    """

    equity = _non_negative(equity_cny, "equity_cny")
    if equity <= 0:
        raise DecisionIntelligenceError("equity_cny must be positive")
    participation = _ratio(maximum_participation_rate, "maximum_participation_rate")
    if participation <= 0:
        raise DecisionIntelligenceError("maximum_participation_rate must be positive")
    if int(capacity_sessions) <= 0:
        raise DecisionIntelligenceError("capacity_sessions must be positive")
    theme_cap = _ratio(maximum_theme_weight, "maximum_theme_weight")
    minimum_edge = _finite(
        minimum_incremental_net_edge_pct,
        "minimum_incremental_net_edge_pct",
    )
    holding_rows = [dict(item) for item in holdings]
    candidate_rows = [dict(item) for item in candidates]
    current_themes = _theme_weights(holding_rows)
    options: list[ReplacementOption] = []
    for candidate_index, candidate in enumerate(candidate_rows):
        candidate_code = _required_text(
            candidate.get("stock_code"),
            f"candidates[{candidate_index}].stock_code",
        )
        candidate_gross = _finite(
            candidate.get("expected_return_gross_pct"),
            f"candidates[{candidate_index}].expected_return_gross_pct",
        )
        candidate_entry_cost = _non_negative(
            candidate.get("entry_cost_pct"),
            f"candidates[{candidate_index}].entry_cost_pct",
        )
        candidate_exit_cost = _non_negative(
            candidate.get("exit_cost_pct"),
            f"candidates[{candidate_index}].exit_cost_pct",
        )
        candidate_haircut = _non_negative(
            candidate.get("uncertainty_haircut_pct", 0.0),
            f"candidates[{candidate_index}].uncertainty_haircut_pct",
        )
        adv = _non_negative(
            candidate.get("average_daily_value_cny"),
            f"candidates[{candidate_index}].average_daily_value_cny",
        )
        capacity_weight = min(
            1.0,
            adv * participation * int(capacity_sessions) / equity,
        )
        candidate_net = (
            candidate_gross
            - candidate_entry_cost
            - candidate_exit_cost
            - candidate_haircut
        )
        candidate_themes = set(_strings(candidate.get("theme_codes")))
        for holding_index, holding in enumerate(holding_rows):
            incumbent_code = _required_text(
                holding.get("stock_code"),
                f"holdings[{holding_index}].stock_code",
            )
            weight = _ratio(
                holding.get("current_weight"),
                f"holdings[{holding_index}].current_weight",
            )
            incumbent_gross = _finite(
                holding.get("expected_return_gross_pct"),
                f"holdings[{holding_index}].expected_return_gross_pct",
            )
            incumbent_exit_cost = _non_negative(
                holding.get("exit_cost_pct"),
                f"holdings[{holding_index}].exit_cost_pct",
            )
            incumbent_haircut = _non_negative(
                holding.get("uncertainty_haircut_pct", 0.0),
                f"holdings[{holding_index}].uncertainty_haircut_pct",
            )
            # Both branches are compared at the same horizon.  Replacing the
            # incumbent requires liquidating it now, so the candidate branch
            # must also pay that cost.  Omitting it would add the incumbent's
            # exit cost back into the apparent incremental edge.
            switch_net = candidate_net - incumbent_exit_cost
            incumbent_net = (
                incumbent_gross
                - incumbent_exit_cost
                - incumbent_haircut
            )
            incremental = switch_net - incumbent_net
            post_themes = dict(current_themes)
            for theme in _strings(holding.get("theme_codes")):
                post_themes[theme] = max(0.0, post_themes.get(theme, 0.0) - weight)
            for theme in candidate_themes:
                post_themes[theme] = post_themes.get(theme, 0.0) + weight
            reasons: list[str] = []
            if bool(holding.get("sell_locked")):
                reasons.append("INCUMBENT_T1_SELL_LOCKED")
            if weight > capacity_weight + 1e-12:
                reasons.append("CANDIDATE_CAPACITY_TOO_LOW")
            if any(value > theme_cap + 1e-12 for value in post_themes.values()):
                reasons.append("THEME_CONCENTRATION_CAP")
            if incremental < minimum_edge:
                reasons.append("INCREMENTAL_NET_EDGE_TOO_LOW")
            options.append(ReplacementOption(
                candidate_code=candidate_code,
                incumbent_code=incumbent_code,
                candidate_forward_net_pct=round(switch_net, 8),
                incumbent_forward_net_pct=round(incumbent_net, 8),
                incremental_net_edge_pct=round(incremental, 8),
                replacement_weight=weight,
                capacity_weight=round(capacity_weight, 8),
                post_replacement_theme_weights={
                    key: round(value, 8)
                    for key, value in sorted(post_themes.items())
                    if value > 0
                },
                eligible=not reasons,
                reason_codes=tuple(reasons),
            ))
    ranked = sorted(
        options,
        key=lambda item: (
            not item.eligible,
            -item.incremental_net_edge_pct,
            item.candidate_code,
            item.incumbent_code,
        ),
    )
    return {
        "schema_version": PORTFOLIO_INTELLIGENCE_SCHEMA,
        "status": "READY",
        "options": [item.as_dict() for item in ranked],
        "eligible_count": sum(item.eligible for item in ranked),
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }


def _optimizer_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "equity_cny": _non_negative(policy.get("equity_cny"), "policy.equity_cny"),
        "risk_asset_cap": _ratio(policy.get("risk_asset_cap"), "policy.risk_asset_cap"),
        "maximum_positions": int(policy.get("maximum_positions", 0)),
        "maximum_single_weight": _ratio(
            policy.get("maximum_single_weight"), "policy.maximum_single_weight"
        ),
        "maximum_theme_weight": _ratio(
            policy.get("maximum_theme_weight"), "policy.maximum_theme_weight"
        ),
        "maximum_cluster_weight": _ratio(
            policy.get("maximum_cluster_weight"), "policy.maximum_cluster_weight"
        ),
        "maximum_turnover_weight": _ratio(
            policy.get("maximum_turnover_weight"), "policy.maximum_turnover_weight"
        ),
        "maximum_participation_rate": _ratio(
            policy.get("maximum_participation_rate"),
            "policy.maximum_participation_rate",
        ),
        "capacity_sessions": int(policy.get("capacity_sessions", 0)),
        "minimum_order_cny": _non_negative(
            policy.get("minimum_order_cny"), "policy.minimum_order_cny"
        ),
        "minimum_edge_to_cost_multiple": _non_negative(
            policy.get("minimum_edge_to_cost_multiple"),
            "policy.minimum_edge_to_cost_multiple",
        ),
        "standard_trade_risk": _ratio(
            policy.get("standard_trade_risk"), "policy.standard_trade_risk"
        ),
        "board_lot": int(policy.get("board_lot", 100)),
        "fees": dict(policy.get("fees") or {}),
    }
    if result["equity_cny"] <= 0:
        raise DecisionIntelligenceError("policy.equity_cny must be positive")
    if result["maximum_positions"] <= 0:
        raise DecisionIntelligenceError("policy.maximum_positions must be positive")
    if result["capacity_sessions"] <= 0:
        raise DecisionIntelligenceError("policy.capacity_sessions must be positive")
    if result["board_lot"] <= 0:
        raise DecisionIntelligenceError("policy.board_lot must be positive")
    if result["maximum_participation_rate"] <= 0:
        raise DecisionIntelligenceError(
            "policy.maximum_participation_rate must be positive"
        )
    required_fee_fields = (
        "commission_rate",
        "minimum_commission_cny",
        "transfer_fee_rate",
        "sell_stamp_duty_rate",
        "default_slippage_rate",
    )
    for field in required_fee_fields:
        result["fees"][field] = _non_negative(
            result["fees"].get(field), f"policy.fees.{field}"
        )
    return result


def optimize_advisory_portfolio(
    candidates: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any],
    current_positions: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build a deterministic capacity/cost/concentration-aware target set.

    The result is an advisory target set only.  It intentionally has no state
    transition that can create an intent or order.  Execution must revalidate
    cash, account and market state in the canonical V2 ledger.
    """

    cfg = _optimizer_policy(policy)
    equity = cfg["equity_cny"]
    position_rows = [dict(item) for item in current_positions]
    current_codes: set[str] = set()
    theme_weights: defaultdict[str, float] = defaultdict(float)
    cluster_weights: defaultdict[str, float] = defaultdict(float)
    invested_weight = 0.0
    for index, position in enumerate(position_rows):
        code = _required_text(
            position.get("stock_code"), f"current_positions[{index}].stock_code"
        )
        if code in current_codes:
            raise DecisionIntelligenceError(
                f"current_positions contains duplicate stock_code {code}"
            )
        current_codes.add(code)
        weight = _ratio(
            position.get("current_weight"),
            f"current_positions[{index}].current_weight",
        )
        invested_weight += weight
        for theme in _strings(position.get("theme_codes")):
            theme_weights[theme] += weight
        cluster = str(position.get("cluster_key") or "").strip()
        if cluster:
            cluster_weights[cluster] += weight
    if invested_weight > 1.0 + 1e-12:
        raise DecisionIntelligenceError("current position weights exceed 100%")

    prepared: list[tuple[float, float, str, int, dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    for index, raw in enumerate(candidates):
        candidate = dict(raw)
        try:
            code = _required_text(
                candidate.get("stock_code"), f"candidates[{index}].stock_code"
            )
            if code in seen_candidates:
                raise DecisionIntelligenceError("duplicate stock_code")
            seen_candidates.add(code)
            if code in current_codes:
                rejected.append({
                    "stock_code": code,
                    "reason_code": "ALREADY_HELD",
                    "reason": "candidate is already present in current positions",
                })
                continue
            score = _finite(
                candidate.get("selection_score"),
                f"candidates[{index}].selection_score",
            )
            conservative_gross = _finite(
                candidate.get("conservative_return_gross_pct"),
                f"candidates[{index}].conservative_return_gross_pct",
            )
            price = _non_negative(
                candidate.get("price"), f"candidates[{index}].price"
            )
            adv = _non_negative(
                candidate.get("average_daily_value_cny"),
                f"candidates[{index}].average_daily_value_cny",
            )
            explicit_distance = candidate.get(
                "initial_stop_distance_pct"
            )
            if explicit_distance is not None:
                stop_pct = _non_negative(
                    explicit_distance,
                    f"candidates[{index}].initial_stop_distance_pct",
                )
            else:
                # The canonical forecast ledger stores stop levels as negative
                # percentages (for example -5).  Sizing needs their positive
                # distance from entry.
                stop_pct = abs(_finite(
                    candidate.get("initial_stop_pct"),
                    f"candidates[{index}].initial_stop_pct",
                ))
            if price <= 0 or adv <= 0 or stop_pct <= 0:
                raise DecisionIntelligenceError(
                    "price, average_daily_value_cny and stop distance must be positive"
                )
            desired = _ratio(
                candidate.get("desired_weight", cfg["maximum_single_weight"]),
                f"candidates[{index}].desired_weight",
            )
            candidate.update({
                "stock_code": code,
                "selection_score": score,
                "conservative_return_gross_pct": conservative_gross,
                "price": price,
                "average_daily_value_cny": adv,
                "initial_stop_pct": stop_pct,
                "initial_stop_distance_pct": stop_pct,
                "desired_weight": desired,
                "theme_codes": _strings(candidate.get("theme_codes")),
                "cluster_key": str(candidate.get("cluster_key") or "").strip(),
            })
            prepared.append((-score, -conservative_gross, code, index, candidate))
        except DecisionIntelligenceError as exc:
            rejected.append({
                "stock_code": str(candidate.get("stock_code") or ""),
                "reason_code": "CANDIDATE_DATA_INVALID",
                "reason": str(exc),
            })

    selected: list[dict[str, Any]] = []
    remaining_asset = max(0.0, cfg["risk_asset_cap"] - invested_weight)
    remaining_turnover = cfg["maximum_turnover_weight"]
    for _score_key, _return_key, code, _index, candidate in sorted(prepared):
        if len(current_codes) + len(selected) >= cfg["maximum_positions"]:
            rejected.append({
                "stock_code": code,
                "reason_code": "POSITION_CAP",
                "reason": "maximum position count reached",
            })
            continue
        stop_distance = candidate["initial_stop_pct"] / 100.0
        risk_weight = cfg["standard_trade_risk"] / stop_distance
        capacity_weight = min(
            1.0,
            candidate["average_daily_value_cny"]
            * cfg["maximum_participation_rate"]
            * cfg["capacity_sessions"]
            / equity,
        )
        target_weight = min(
            candidate["desired_weight"],
            cfg["maximum_single_weight"],
            risk_weight,
            capacity_weight,
            remaining_asset,
            remaining_turnover,
        )
        for theme in candidate["theme_codes"]:
            target_weight = min(
                target_weight,
                cfg["maximum_theme_weight"] - theme_weights[theme],
            )
        if candidate["cluster_key"]:
            target_weight = min(
                target_weight,
                cfg["maximum_cluster_weight"]
                - cluster_weights[candidate["cluster_key"]],
            )
        if target_weight <= 0:
            rejected.append({
                "stock_code": code,
                "reason_code": "CAPACITY_OR_CONCENTRATION_FULL",
                "reason": "risk, capacity, turnover or concentration budget is exhausted",
            })
            continue
        lot = cfg["board_lot"]
        quantity = math.floor(equity * target_weight / candidate["price"] / lot) * lot
        order_value = quantity * candidate["price"]
        actual_weight = order_value / equity
        if quantity <= 0 or order_value < cfg["minimum_order_cny"]:
            rejected.append({
                "stock_code": code,
                "reason_code": "ORDER_NOT_ECONOMIC",
                "reason": "capacity-aware board-lot order is below the economic minimum",
            })
            continue
        fees = cfg["fees"]
        cost_pct = estimate_roundtrip_cost_pct(
            order_value,
            commission_rate=fees["commission_rate"],
            minimum_commission=fees["minimum_commission_cny"],
            transfer_fee_rate=fees["transfer_fee_rate"],
            sell_stamp_duty_rate=fees["sell_stamp_duty_rate"],
            slippage_rate=fees["default_slippage_rate"],
        )
        required_edge = cost_pct * cfg["minimum_edge_to_cost_multiple"]
        if candidate["conservative_return_gross_pct"] <= required_edge:
            rejected.append({
                "stock_code": code,
                "reason_code": "NET_EDGE_BELOW_COST_BUFFER",
                "reason": "conservative gross edge does not cover the required cost multiple",
                "estimated_roundtrip_cost_pct": round(cost_pct, 8),
                "required_gross_edge_pct": round(required_edge, 8),
            })
            continue
        selected.append({
            "stock_code": code,
            "stock_name": str(candidate.get("stock_name") or ""),
            "selection_score": candidate["selection_score"],
            "target_weight": round(actual_weight, 8),
            "target_value": round(order_value, 2),
            "target_quantity": quantity,
            "capacity_weight": round(capacity_weight, 8),
            "estimated_roundtrip_cost_pct": round(cost_pct, 8),
            "conservative_return_gross_pct": candidate[
                "conservative_return_gross_pct"
            ],
            "conservative_return_after_cost_pct": round(
                candidate["conservative_return_gross_pct"] - cost_pct, 8
            ),
            "initial_stop_distance_pct": candidate[
                "initial_stop_distance_pct"
            ],
            "theme_codes": list(candidate["theme_codes"]),
            "cluster_key": candidate["cluster_key"],
            "reason": "capacity, cost, risk and concentration gates passed",
        })
        remaining_asset = max(0.0, remaining_asset - actual_weight)
        remaining_turnover = max(0.0, remaining_turnover - actual_weight)
        for theme in candidate["theme_codes"]:
            theme_weights[theme] += actual_weight
        if candidate["cluster_key"]:
            cluster_weights[candidate["cluster_key"]] += actual_weight

    return {
        "schema_version": PORTFOLIO_INTELLIGENCE_SCHEMA,
        "status": "TARGETS_READY" if selected else "VALID_EMPTY",
        "targets": selected,
        "rejected": rejected,
        "target_risk_asset_weight": round(
            invested_weight + sum(item["target_weight"] for item in selected), 8
        ),
        "remaining_risk_asset_weight": round(remaining_asset, 8),
        "estimated_turnover_weight": round(
            sum(item["target_weight"] for item in selected), 8
        ),
        "theme_weights": {
            key: round(value, 8) for key, value in sorted(theme_weights.items())
        },
        "cluster_weights": {
            key: round(value, 8) for key, value in sorted(cluster_weights.items())
        },
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
        "execution_revalidation_required": True,
    }
