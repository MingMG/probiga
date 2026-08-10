# -*- coding: utf-8 -*-
"""Fail-closed V3/V4/V5/V6 production stock-selection ensemble.

The selector consumes point-in-time candidate evidence and owns only advisory
ranking.  It never grants order authority.  Version responsibilities are
deliberately non-overlapping:

* V3: base alpha/ranking score.
* V4: entry, event, chase, liquidity and execution risk; owns hard vetoes.
* V5: one global market regime plus stock-to-regime fit.
* V6: point-in-time finance, valuation and long-horizon quality.

The v2 contract adds cross-sectional normalization, board-aware risk metadata,
multi-horizon scores, realistic cost/capacity diagnostics, portfolio
constraints, candidate grades and a deterministic model fingerprint.  Missing
or time-unsafe evidence fails closed and transfers soft weight back to V3.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence


SELECTOR_MODE = "V3_V4_V5_V6_GATED_MULTI_HORIZON_ENSEMBLE"
CONTRACT_VERSION = "production-selector.v2"
POLICY_VERSION = "2026-08-10.p0-p3"
ADVISORY_WEIGHTS = {"V4": 0.12, "V5": 0.10, "V6": 0.08}
HORIZON_WEIGHTS = {"T+1": 0.35, "T+5": 0.35, "T+20": 0.30}
FEATURE_OWNERS = {
    "V3": ("final_trade_score", "ai_score", "score"),
    "V4": (
        "entry_score", "risk_reward_ratio", "event_risk_level",
        "chase_risk_status", "heat_overload_score", "failure_penalty_score",
        "amount", "turnover_ratio", "ordinary_buy_eligible",
    ),
    "V5": (
        "global_market_regime_score", "ultra_short_score", "short_term_score",
        "swing_score", "main_wave_score", "capital_score",
        "sector_rotation_score",
    ),
    "V6": (
        "net_asset_ps", "oper_cf_ps", "net_profit_yoy_gr", "roe_wtd",
        "gross_margin", "net_margin", "cash_flow_ratio", "asset_liab_ratio",
        "finance_report_date", "finance_notice_date", "finance_knowledge_at",
    ),
}
PORTFOLIO_POLICY = {
    "max_per_industry": 3,
    "max_per_theme": 3,
    "max_per_correlation_cluster": 2,
    "correlation_window_days": 60,
    "correlation_threshold": 0.85,
    "minimum_correlation_overlap_sessions": 10,
    "default_candidate_notional": 100_000.0,
    "minimum_daily_amount": 5_000_000.0,
}
RELEASE_GATES = {
    "minimum_universe_coverage": 0.95,
    "minimum_mature_samples_per_horizon": 80,
    "minimum_oos_profit_factor": 1.30,
    "minimum_oos_average_win_loss": 1.0,
    "minimum_shadow_sessions": 20,
    "maximum_data_missing_rate": 0.05,
}


def _fingerprint() -> str:
    payload = {
        "mode": SELECTOR_MODE,
        "contract": CONTRACT_VERSION,
        "policy": POLICY_VERSION,
        "weights": ADVISORY_WEIGHTS,
        "horizons": HORIZON_WEIGHTS,
        "owners": FEATURE_OWNERS,
        "portfolio": PORTFOLIO_POLICY,
        "release_gates": RELEASE_GATES,
    }
    encoded = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


MODEL_FINGERPRINT = _fingerprint()


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _score(value: Any, *, positive_only: bool = False) -> float | None:
    number = _number(value)
    if number is None or (positive_only and number <= 0):
        return None
    return max(0.0, min(100.0, number))


def _mean(values: Iterable[float | None]) -> float | None:
    usable = [float(value) for value in values if value is not None]
    return sum(usable) / len(usable) if usable else None


def _explicit_true(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def _explicit_false(value: Any) -> bool:
    return value is False or (type(value) is int and value == 0)


def _date_value(value: Any) -> date | None:
    raw = str(value or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def board_limit_pct(stock_code: Any, short_name: Any = "") -> float:
    """Return the official daily limit percentage for an ordinary session."""
    code = str(stock_code or "").strip().zfill(6)
    name = str(short_name or "").upper().replace(" ", "")
    if "ST" in name:
        return 5.0
    if code.startswith("92"):
        return 30.0
    if code.startswith(("30", "68")):
        return 20.0
    return 10.0


def board_limit_trigger_pct(
    stock_code: Any,
    short_name: Any = "",
    *,
    tolerance: float = 0.95,
) -> float:
    """Return a data-tolerant limit-up trigger without one-size-fits-all 9.5%."""
    return round(board_limit_pct(stock_code, short_name) * max(0.8, min(1.0, tolerance)), 4)


def _percentile_values(
    rows: Sequence[Mapping[str, Any]],
    key: str,
    *,
    higher_is_better: bool = True,
) -> dict[str, float]:
    values: list[tuple[str, float]] = []
    for row in rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        value = _number(row.get(key))
        if code and value is not None:
            values.append((code, value))
    ordered = sorted(value for _code, value in values)
    if not ordered:
        return {}
    output: dict[str, float] = {}
    for code, value in values:
        if len(ordered) == 1:
            percentile = 50.0
        else:
            left = bisect_left(ordered, value)
            right = bisect_right(ordered, value) - 1
            percentile = ((left + right) / 2.0) / (len(ordered) - 1) * 100.0
        if not higher_is_better:
            percentile = 100.0 - percentile
        output[code] = round(max(0.0, min(100.0, percentile)), 2)
    return output


def _prepare_cross_section(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Attach comparable percentile features without mutating source rows."""
    prepared = [dict(row) for row in rows]
    for row in prepared:
        roe = _number(row.get("roe_wtd"))
        gross = _number(row.get("gross_margin"))
        net = _number(row.get("net_margin"))
        debt = _number(row.get("asset_liab_ratio"))
        oper_cf = _number(row.get("oper_cf_ps"))
        cash_ratio = _number(row.get("cash_flow_ratio"))
        close = _number(row.get("close", row.get("price")))
        nav = _number(row.get("net_asset_ps"))
        if None not in (roe, gross, net, debt):
            row["v6_quality_raw"] = roe + gross * 0.25 + net * 0.25 - debt * 0.15
        if None not in (oper_cf, cash_ratio):
            row["v6_cashflow_raw"] = oper_cf + cash_ratio * 0.1
        if close is not None and nav is not None and close > 0 and nav > 0:
            row["v6_valuation_raw"] = close / nav

    specs = (
        ("entry_score", "v4_entry_percentile", True),
        ("risk_reward_ratio", "v4_risk_reward_percentile", True),
        ("amount", "v4_liquidity_percentile", True),
        ("ultra_short_score", "v5_ultra_short_percentile", True),
        ("short_term_score", "v5_short_term_percentile", True),
        ("swing_score", "v5_swing_percentile", True),
        ("main_wave_score", "v5_main_wave_percentile", True),
        ("capital_score", "v5_capital_percentile", True),
        ("sector_rotation_score", "v5_rotation_percentile", True),
        ("v6_quality_raw", "v6_quality_percentile", True),
        ("v6_cashflow_raw", "v6_cashflow_percentile", True),
        ("v6_valuation_raw", "v6_valuation_percentile", False),
        ("net_profit_yoy_gr", "v6_growth_percentile", True),
    )
    for source, target, higher_is_better in specs:
        mapping = _percentile_values(
            prepared, source, higher_is_better=higher_is_better
        )
        for row in prepared:
            code = str(row.get("stock_code") or "").strip().zfill(6)
            if code in mapping:
                row[target] = mapping[code]
    peer_count = len({str(row.get("stock_code") or "") for row in prepared})
    for row in prepared:
        row["normalization"] = {
            "method": "same-date-cross-sectional-percentile",
            "peer_count": peer_count,
            "range": [0, 100],
            "feature_owner_policy": POLICY_VERSION,
        }
    return prepared


def _advisory(
    version: str,
    *,
    score: float | None,
    evidence: list[str],
    reason: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    active = score is not None and len(evidence) >= 2
    return {
        "version": version,
        "status": "ACTIVE_BOUNDED" if active else "FALLBACK_TO_V3",
        "score": round(float(score), 2) if active else None,
        "evidence_count": len(evidence),
        "evidence": evidence,
        "reason": reason if active else f"证据不足，已回退 V3：{reason}",
        "max_weight": ADVISORY_WEIGHTS[version],
        "release_gate": "BLOCK_REAL_ORDER_AUTHORITY",
        "order_authority": False,
        "details": dict(details or {}),
    }


def _v4_hard_gate(row: Mapping[str, Any]) -> dict[str, Any]:
    reject: list[str] = []
    missing: list[str] = []
    event_level = str(row.get("event_risk_level") or "").strip().upper()
    chase = str(row.get("chase_risk_status") or "").strip().upper()
    if event_level in {"HIGH", "CRITICAL"}:
        reject.append(f"event_risk:{event_level}")
    elif not event_level:
        missing.append("event_risk_level")
    if chase in {"RISK", "BLOCK", "EXECUTION_BLOCKED"}:
        reject.append(f"chase_risk:{chase}")
    elif not chase:
        missing.append("chase_risk_status")
    ordinary = row.get("ordinary_buy_eligible")
    if _explicit_false(ordinary):
        reject.append("ordinary_buy_ineligible")
    elif not _explicit_true(ordinary):
        missing.append("ordinary_buy_eligible")
    if row.get("suspended") is True:
        reject.append("suspended")
    if row.get("limit_up_locked") is True:
        reject.append("limit_up_locked")
    if row.get("tradable_next_session") is False:
        reject.append("next_session_untradable")
    volume = _number(row.get("volume"))
    amount = _number(row.get("amount"))
    if volume is not None and volume <= 0:
        reject.append("zero_volume")
    if amount is not None and amount <= 0:
        reject.append("zero_amount")
    if _score(row.get("entry_score"), positive_only=True) is None:
        missing.append("entry_score")
    if _number(row.get("risk_reward_ratio")) is None:
        missing.append("risk_reward_ratio")
    status = "REJECT" if reject else ("DATA_BLOCKED" if missing else "PASS")
    return {
        "status": status,
        "reject_reasons": reject,
        "missing_fields": sorted(set(missing)),
        "hard_veto": bool(reject),
    }


def _v4_advisory(row: Mapping[str, Any]) -> dict[str, Any]:
    gate = _v4_hard_gate(row)
    components: list[float | None] = []
    evidence: list[str] = []
    fields = (
        ("v4_entry_percentile", "entry_score", "入场质量"),
        ("v4_risk_reward_percentile", None, "盈亏比"),
        ("v4_liquidity_percentile", None, "流动性"),
    )
    for percentile_key, fallback_key, label in fields:
        value = _score(row.get(percentile_key))
        if value is None and fallback_key:
            value = _score(row.get(fallback_key), positive_only=True)
        components.append(value)
        if value is not None:
            evidence.append(label)

    risk_reward = _number(row.get("risk_reward_ratio"))
    if components[1] is None and risk_reward is not None and risk_reward > 0:
        components[1] = max(0.0, min(100.0, risk_reward * 25.0))
        evidence.append("盈亏比")
    heat = _score(row.get("heat_overload_score"), positive_only=True)
    failure = _score(row.get("failure_penalty_score"), positive_only=True)
    if heat is not None:
        components.append(100.0 - heat)
        evidence.append("热度不过载")
    if failure is not None:
        components.append(100.0 - failure)
        evidence.append("历史失效惩罚")
    event_level = str(row.get("event_risk_level") or "").strip().upper()
    event_safety = {"LOW": 90.0, "MEDIUM": 55.0, "HIGH": 0.0, "CRITICAL": 0.0}.get(event_level)
    if event_safety is not None:
        components.append(event_safety)
        evidence.append("事件风险")
    chase = str(row.get("chase_risk_status") or "").strip().upper()
    chase_safety = {
        "ALLOW": 90.0, "WATCH": 55.0, "RISK": 0.0,
        "BLOCK": 0.0, "EXECUTION_BLOCKED": 0.0,
    }.get(chase)
    if chase_safety is not None:
        components.append(chase_safety)
        evidence.append("追高门禁")

    result = _advisory(
        "V4",
        score=_mean(components) if len(evidence) >= 2 else None,
        evidence=evidence,
        reason="入场、盈亏比、流动性、事件和追高风险的唯一责任层",
        details={"hard_gate": gate, "chase_risk_status": chase or "DATA_BLOCKED", "event_risk_level": event_level or "UNKNOWN"},
    )
    if gate["status"] == "REJECT":
        result.update({"status": "HARD_REJECT", "score": 0.0})
        result["reason"] = "V4 硬门禁拒绝：" + ",".join(gate["reject_reasons"])
    return result


def _v5_advisory(row: Mapping[str, Any]) -> dict[str, Any]:
    global_regime = _score(
        row.get("global_market_regime_score", row.get("market_mood_score")),
        positive_only=True,
    )
    regime = "UNKNOWN"
    if global_regime is not None:
        regime = "RISK_ON" if global_regime >= 60 else ("RISK_OFF" if global_regime <= 40 else "NEUTRAL")
    field_groups = {
        "RISK_ON": (
            ("v5_ultra_short_percentile", "ultra_short_score", "超短匹配"),
            ("v5_main_wave_percentile", "main_wave_score", "主升匹配"),
            ("v5_capital_percentile", "capital_score", "资金匹配"),
            ("v5_rotation_percentile", "sector_rotation_score", "板块轮动"),
        ),
        "RISK_OFF": (
            ("v5_swing_percentile", "swing_score", "波段防御"),
            ("v5_capital_percentile", "capital_score", "资金防御"),
        ),
        "NEUTRAL": (
            ("v5_swing_percentile", "swing_score", "波段匹配"),
            ("v5_short_term_percentile", "short_term_score", "短线匹配"),
            ("v5_rotation_percentile", "sector_rotation_score", "板块轮动"),
        ),
    }
    components: list[float | None] = []
    evidence: list[str] = ["全市场状态"] if global_regime is not None else []
    for percentile_key, fallback_key, label in field_groups.get(regime, ()):
        value = _score(row.get(percentile_key))
        if value is None:
            value = _score(row.get(fallback_key), positive_only=True)
        components.append(value)
        if value is not None:
            evidence.append(label)
    return _advisory(
        "V5",
        score=_mean(components) if global_regime is not None and components else None,
        evidence=evidence,
        reason="全市场状态与个股适配特征分离，个股情绪不再定义全局状态",
        details={"regime": regime, "global_regime_score": round(global_regime, 2) if global_regime is not None else None},
    )


def _pit_verified(row: Mapping[str, Any]) -> tuple[bool, list[str]]:
    missing: list[str] = []
    if not _explicit_true(row.get("finance_pit_verified", row.get("pit_verified"))):
        missing.append("finance_pit_verified")
    signal_date = _date_value(row.get("data_date", row.get("as_of_date")))
    report_date = _date_value(row.get("finance_report_date", row.get("report_date")))
    notice_date = _date_value(row.get("finance_notice_date", row.get("notice_date")))
    knowledge_date = _date_value(row.get("finance_knowledge_at", row.get("knowledge_at")))
    if signal_date is None:
        missing.append("data_date")
    for label, value in (
        ("finance_report_date", report_date),
        ("finance_notice_date", notice_date),
        ("finance_knowledge_at", knowledge_date),
    ):
        if value is None:
            missing.append(label)
    if signal_date and report_date and report_date > signal_date:
        missing.append("future_report_date")
    if signal_date and notice_date and notice_date > signal_date:
        missing.append("future_notice_date")
    if signal_date and knowledge_date and knowledge_date > signal_date:
        missing.append("future_knowledge_date")
    if report_date and notice_date and notice_date < report_date:
        missing.append("notice_precedes_report")
    return not missing, missing


def _v6_advisory(row: Mapping[str, Any]) -> dict[str, Any]:
    pit_ok, pit_failures = _pit_verified(row)
    fields = (
        ("v6_quality_percentile", "fundamental", "质量分位"),
        ("v6_cashflow_percentile", None, "现金流分位"),
        ("v6_valuation_percentile", "valuation", "估值分位"),
        ("v6_growth_percentile", "growth_score", "成长分位"),
    )
    components: list[float | None] = []
    evidence: list[str] = ["PIT时点校验"] if pit_ok else []
    for percentile_key, fallback_key, label in fields:
        value = _score(row.get(percentile_key))
        if value is None and fallback_key:
            value = _score(row.get(fallback_key), positive_only=True)
        components.append(value)
        if value is not None:
            evidence.append(label)
    return _advisory(
        "V6",
        score=_mean(components) if pit_ok and len(evidence) >= 3 else None,
        evidence=evidence,
        reason="仅使用公告日与知识时间均不晚于决策日的财务证据",
        details={
            "pit_verified": pit_ok,
            "pit_failures": pit_failures,
            "report_date": row.get("finance_report_date", row.get("report_date")),
            "notice_date": row.get("finance_notice_date", row.get("notice_date")),
            "knowledge_at": row.get("finance_knowledge_at", row.get("knowledge_at")),
            "source": row.get("finance_source") or "si_stock_finance",
        },
    )


def _multi_horizon(row: Mapping[str, Any], base: float) -> dict[str, Any]:
    specs = {
        "T+1": ("horizon_t1_score", "ultra_short_score", "entry_score"),
        "T+5": ("horizon_t5_score", "swing_score", "short_term_score"),
        "T+20": ("horizon_t20_score", "long_term_score", "v6_quality_percentile"),
    }
    scores: dict[str, float] = {}
    sources: dict[str, str] = {}
    for horizon, keys in specs.items():
        value = None
        source = "V3_BASE_FALLBACK"
        for key in keys:
            value = _score(row.get(key), positive_only=True)
            if value is not None:
                source = key
                break
        scores[horizon] = round(value if value is not None else base, 2)
        sources[horizon] = source
    combined = sum(HORIZON_WEIGHTS[horizon] * scores[horizon] for horizon in HORIZON_WEIGHTS)
    return {
        "scores": scores,
        "weights": dict(HORIZON_WEIGHTS),
        "sources": sources,
        "combined_score": round(combined, 2),
    }


def _execution_diagnostics(row: Mapping[str, Any]) -> dict[str, Any]:
    amount = _number(row.get("amount"))
    notional = _number(row.get("candidate_notional")) or PORTFOLIO_POLICY["default_candidate_notional"]
    commission_bps = 6.0
    tax_and_fees_bps = 10.0
    if amount is None or amount <= 0:
        return {
            "status": "DATA_BLOCKED",
            "estimated_round_trip_cost_bps": None,
            "capacity_ratio": None,
            "reason": "缺少有效成交额，不能证明容量和滑点",
        }
    capacity_ratio = notional / amount
    spread_bps = 4.0 if amount >= 100_000_000 else (8.0 if amount >= 20_000_000 else (15.0 if amount >= 5_000_000 else 30.0))
    impact_bps = max(1.0, min(50.0, capacity_ratio * 10_000.0 * 0.25))
    cost = commission_bps + tax_and_fees_bps + spread_bps + impact_bps
    status = "PASS" if amount >= PORTFOLIO_POLICY["minimum_daily_amount"] and capacity_ratio <= 0.02 else "CAPACITY_BLOCKED"
    return {
        "status": status,
        "estimated_round_trip_cost_bps": round(cost, 2),
        "capacity_ratio": round(capacity_ratio, 6),
        "candidate_notional": round(notional, 2),
        "daily_amount": round(amount, 2),
        "components_bps": {
            "commission": commission_bps,
            "tax_and_fees": tax_and_fees_bps,
            "spread": spread_bps,
            "impact": round(impact_bps, 2),
        },
        "reason": "成本和容量通过" if status == "PASS" else "成交额或候选规模未通过容量门",
    }


def score_production_candidate(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return one copied candidate with gates, scores and audit diagnostics."""
    item = dict(row)
    base = None
    base_source = "RULE_FALLBACK"
    for key in ("final_trade_score", "ai_score", "score"):
        base = _score(item.get(key))
        if base is not None:
            base_source = key
            break
    if base is None:
        base = 50.0

    versions = {
        "V3": {
            "version": "V3", "status": "ACTIVE_BASE",
            "score": round(base, 2), "min_weight": 0.70,
            "source": base_source, "order_authority": False,
        },
        "V4": _v4_advisory(item),
        "V5": _v5_advisory(item),
        "V6": _v6_advisory(item),
    }
    horizon = _multi_horizon(item, base)
    execution = _execution_diagnostics(item)
    ensemble = 0.70 * base + 0.30 * horizon["combined_score"]
    contributions: dict[str, float] = {}
    active_versions = ["V3"]
    for version in ("V4", "V5", "V6"):
        advisory = versions[version]
        if advisory["status"] != "ACTIVE_BOUNDED":
            contributions[version] = 0.0
            continue
        delta = ADVISORY_WEIGHTS[version] * (float(advisory["score"]) - base)
        ensemble += delta
        contributions[version] = round(delta, 2)
        active_versions.append(version)
    ensemble = round(max(0.0, min(100.0, ensemble)), 2)

    gate = versions["V4"]["details"]["hard_gate"]
    evidence_fields = 1 + sum(
        versions[version]["evidence_count"] for version in ("V4", "V5", "V6")
    )
    evidence_completeness = min(1.0, evidence_fields / 12.0)
    if gate["status"] == "REJECT":
        grade = "REJECT"
    elif gate["status"] == "DATA_BLOCKED" or execution["status"] == "DATA_BLOCKED" or ensemble < 55:
        grade = "C"
    elif (
        set(active_versions) == {"V3", "V4", "V5", "V6"}
        and evidence_completeness >= 0.80
        and execution["status"] == "PASS"
        and ensemble >= 70
    ):
        grade = "A"
    else:
        grade = "B"

    item.update(
        {
            "base_score": round(base, 2),
            "score": ensemble,
            "ensemble_score": ensemble,
            "selector_mode": SELECTOR_MODE,
            "selector_contract_version": CONTRACT_VERSION,
            "selector_policy_version": POLICY_VERSION,
            "model_fingerprint": MODEL_FINGERPRINT,
            "selector_versions": versions,
            "selector_contributions": contributions,
            "active_versions": active_versions,
            "multi_horizon": horizon,
            "execution_diagnostics": execution,
            "candidate_grade": grade,
            "candidate_pool": grade,
            "evidence_completeness": round(evidence_completeness, 4),
            "risk_gate": gate,
            "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
            "order_authority": False,
            "automatic_real_order_submission": False,
        }
    )
    return item


def _portfolio_bucket(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _apply_portfolio_constraints(rows: list[dict[str, Any]]) -> None:
    industries: dict[str, int] = {}
    themes: dict[str, int] = {}
    clusters: dict[str, int] = {}
    portfolio_rank = 0
    for row in rows:
        reasons: list[str] = []
        if row.get("candidate_grade") == "REJECT":
            reasons.extend(row.get("risk_gate", {}).get("reject_reasons") or ["hard_gate_reject"])
        if row.get("execution_diagnostics", {}).get("status") == "CAPACITY_BLOCKED":
            reasons.append("capacity_blocked")
        if row.get("correlation_cluster_status") != "VERIFIED_60D":
            reasons.append("correlation_evidence_missing")
        industry = _portfolio_bucket(row, "industry_name", "industry", "sector_name")
        theme = _portfolio_bucket(row, "primary_concept", "theme_name", "concept_name")
        cluster = _portfolio_bucket(row, "correlation_cluster")
        if industry and industries.get(industry, 0) >= PORTFOLIO_POLICY["max_per_industry"]:
            reasons.append("industry_concentration")
        if theme and themes.get(theme, 0) >= PORTFOLIO_POLICY["max_per_theme"]:
            reasons.append("theme_concentration")
        if cluster and clusters.get(cluster, 0) >= PORTFOLIO_POLICY["max_per_correlation_cluster"]:
            reasons.append("correlation_concentration")
        eligible = not reasons and row.get("candidate_grade") in {"A", "B"}
        row["portfolio_eligible"] = eligible
        row["portfolio_reject_reasons"] = reasons
        if eligible:
            portfolio_rank += 1
            row["portfolio_rank"] = portfolio_rank
            if industry:
                industries[industry] = industries.get(industry, 0) + 1
            if theme:
                themes[theme] = themes.get(theme, 0) + 1
            if cluster:
                clusters[cluster] = clusters.get(cluster, 0) + 1
        else:
            row["portfolio_rank"] = None


def rank_production_candidates(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Normalize, score, constrain and deterministically rank candidates."""
    source_rows = list(rows)
    prepared = _prepare_cross_section(source_rows)
    ranked = [score_production_candidate(row) for row in prepared]
    grade_order = {"A": 0, "B": 1, "C": 2, "REJECT": 3}
    ranked.sort(
        key=lambda row: (
            grade_order.get(str(row.get("candidate_grade")), 9),
            -float(row.get("ensemble_score") or 0.0),
            -float(row.get("base_score") or 0.0),
            str(row.get("stock_code") or ""),
        )
    )
    for index, row in enumerate(ranked, 1):
        row["rank"] = index
    _apply_portfolio_constraints(ranked)
    return ranked


def selector_run_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    grades = {grade: 0 for grade in ("A", "B", "C", "REJECT")}
    version_active = {version: 0 for version in ("V4", "V5", "V6")}
    reject_reasons: dict[str, int] = {}
    for row in rows:
        grade = str(row.get("candidate_grade") or "C")
        grades[grade] = grades.get(grade, 0) + 1
        for version in version_active:
            if version in (row.get("active_versions") or []):
                version_active[version] += 1
        for reason in row.get("risk_gate", {}).get("reject_reasons") or []:
            reject_reasons[reason] = reject_reasons.get(reason, 0) + 1
    count = len(rows)
    return {
        "candidate_count": count,
        "grades": grades,
        "portfolio_eligible_count": sum(bool(row.get("portfolio_eligible")) for row in rows),
        "version_activation_rate": {
            version: round(value / count, 4) if count else 0.0
            for version, value in version_active.items()
        },
        "hard_reject_reasons": reject_reasons,
        "model_fingerprint": MODEL_FINGERPRINT,
        "policy_version": POLICY_VERSION,
    }


def selector_contract() -> dict[str, Any]:
    return {
        "mode": SELECTOR_MODE,
        "contract_version": CONTRACT_VERSION,
        "policy_version": POLICY_VERSION,
        "model_fingerprint": MODEL_FINGERPRINT,
        "base_version": "V3",
        "advisory_weights": dict(ADVISORY_WEIGHTS),
        "horizon_weights": dict(HORIZON_WEIGHTS),
        "feature_owners": {key: list(value) for key, value in FEATURE_OWNERS.items()},
        "portfolio_policy": dict(PORTFOLIO_POLICY),
        "release_gates": dict(RELEASE_GATES),
        "max_total_advisory_weight": round(sum(ADVISORY_WEIGHTS.values()), 2),
        "missing_data_policy": "FAIL_CLOSED_TRANSFER_SOFT_WEIGHT_TO_V3",
        "risk_gate_policy": "V4_HARD_VETO_CANNOT_BE_OVERRIDDEN",
        "finance_policy": "V6_POINT_IN_TIME_REQUIRED",
        "normalization_policy": "SAME_DATE_CROSS_SECTIONAL_PERCENTILE",
        "research_release_gates": {"V4": "BLOCK_ORDER_AUTHORITY", "V5": "BLOCK_ORDER_AUTHORITY", "V6": "BLOCK_ORDER_AUTHORITY"},
        "rollback": {
            "required_assets": ["code", "config", "model_fingerprint", "data_route", "result_snapshot"],
            "target": "previous_production_champion",
        },
        "production_ranking_active": True,
        "order_authority": False,
        "automatic_real_order_submission": False,
    }


__all__ = [
    "ADVISORY_WEIGHTS", "CONTRACT_VERSION", "MODEL_FINGERPRINT",
    "POLICY_VERSION", "SELECTOR_MODE", "board_limit_pct",
    "board_limit_trigger_pct", "rank_production_candidates",
    "score_production_candidate", "selector_contract", "selector_run_summary",
]
