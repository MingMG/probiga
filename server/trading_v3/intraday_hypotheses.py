from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from sqlalchemy.engine import Engine

from server.trading_v2.repository import TradingV2ReadRepository

from .domain import TradeHypothesis
from .hypotheses import apply_evidence
from .order_flow import load_recent_order_flow
from .repository import TradingV3Repository


ACCOUNT_ID = "paper-main-v2"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _datetime(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    text_value = str(value or "").strip()
    if not text_value:
        return fallback
    try:
        return datetime.fromisoformat(text_value)
    except ValueError:
        return fallback


def _date(value: Any, fallback: date) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return fallback


def _intraday_discovery_hypothesis(
    decision: dict[str, Any],
    *,
    run_uid: str,
    trade_date: date,
    observed_at: datetime,
) -> TradeHypothesis:
    status = str(decision.get("status") or "WATCHING")
    action = str(decision.get("action") or "WATCH")
    if status == "ORDER_CREATED":
        probability = 0.72
        state = "ACTIVE"
        proposed_action = "PAPER_ORDER_CREATED"
    elif status == "ACTIVATABLE" or action.startswith("ACTIVATE_"):
        probability = 0.66
        state = "TRIGGER_READY"
        proposed_action = "PAPER_PROBE_IF_CONFIRMED"
    elif status == "RISK_REJECTED":
        probability = 0.34
        state = "WEAKEN"
        proposed_action = "NO_NEW_BUY"
    else:
        probability = 0.52
        state = "PREPARE"
        proposed_action = "WATCH_CLOSELY"
    code = str(decision.get("stock_code") or "").zfill(6)
    name = str(decision.get("short_name") or code)
    theme = str(
        decision.get("theme_name")
        or decision.get("theme_code")
        or ""
    )
    evidence = tuple(
        str(item)
        for item in (decision.get("evidence") or [])
        if str(item).strip()
    )
    source_strategy = str(
        decision.get("source_strategy_version")
        or "intraday_market_radar_v2"
    )
    max_weight = max(
        0.0,
        min(
            0.08,
            _float(decision.get("opening_target_fraction")),
        ),
    )
    if status == "RISK_REJECTED":
        max_weight = 0.0
    end_of_session = datetime.combine(
        trade_date,
        time(15, 10),
    )
    return TradeHypothesis(
        hypothesis_key=(
            f"INTRADAY:{trade_date.isoformat()}:{code}"
        ),
        run_uid=run_uid,
        trade_date=trade_date.isoformat(),
        scope_type="STOCK",
        scope_code=code,
        scope_name=name,
        direction="LONG",
        state=state,
        probability=probability,
        prior_probability=probability,
        probability_kind="INTRADAY_STRUCTURED_PRIOR",
        confidence=min(
            0.78,
            0.30 + max(0.0, _float(decision.get("raw_score"))) * 0.45,
        ),
        score=_float(decision.get("raw_score")),
        horizon_minutes=120,
        alpha_half_life_minutes=25,
        proposed_action=proposed_action,
        max_position_weight=max_weight,
        theme_code=theme,
        role=str(decision.get("role") or "INTRADAY_DISCOVERY"),
        thesis=(
            f"{name}不是因为日线排名被硬塞进来，而是盘中出现了"
            "相对强度、量能或板块共振，进入实时验证队列。"
        ),
        counter_thesis=(
            "若放量不能推动价格、板块宽度回落或相对强度转负，"
            "本次盘中超预期假设立即降级。"
        ),
        supporting_evidence=evidence[:8]
        or ("盘中雷达发现了新的量价变化，等待持续性确认。",),
        opposing_evidence=(
            "盘中信号半衰期短，不能用一次脉冲替代持续承接。",
        ),
        triggers=(
            "价格站稳VWAP或关键突破位，回踩不放量跌破。",
            "实时成交额倍率、个股相对强度与板块宽度至少两项共振。",
            "盘口主动买盘增强，且放量能够继续推动价格。",
        ),
        invalidations=(
            "跌破触发前低或保护位且不能快速收回。",
            "板块转弱、龙头开板走弱，个股相对强度同步转负。",
            "盘口卖压占优，放量滞涨或冲高快速回落。",
        ),
        strategy_keys=(source_strategy,),
        feature_time=observed_at,
        valid_until=max(
            observed_at + timedelta(minutes=30),
            end_of_session,
        ),
        source_forecast_count=0,
    )


def _market_update(
    hypothesis: TradeHypothesis,
    state: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[TradeHypothesis, Any]:
    quality = str(state.get("quality_status") or "")
    actionable = bool(int(_float(state.get("actionable"))))
    breadth = _float(state.get("positive_breadth_pct"))
    equal_weight = _float(state.get("equal_weight_return_pct"))
    confirming = int(_float(state.get("confirming_points")))
    if quality == "BLOCK":
        polarity = "NEGATIVE"
        strength = 0.90
        summary = "盘中市场数据或确认点不足，禁止据此扩大风险仓位。"
    elif actionable:
        polarity = "POSITIVE"
        strength = min(
            1.10,
            0.45
            + max(0.0, breadth - 50.0) / 70.0
            + max(0.0, equal_weight) / 5.0,
        )
        summary = (
            f"盘中市场确认有效：上涨宽度{breadth:.1f}%，"
            f"等权涨幅{equal_weight:.2f}%，确认点{confirming}个。"
        )
    elif breadth < 38 or equal_weight < -0.8:
        polarity = "NEGATIVE"
        strength = min(
            0.95,
            0.35
            + max(0.0, 45.0 - breadth) / 60.0
            + max(0.0, -equal_weight) / 5.0,
        )
        summary = (
            f"盘中市场承接偏弱：上涨宽度{breadth:.1f}%，"
            f"等权涨幅{equal_weight:.2f}%。"
        )
    else:
        polarity = "NEUTRAL"
        strength = 0.0
        summary = (
            f"盘中市场仍在观察：上涨宽度{breadth:.1f}%，"
            f"等权涨幅{equal_weight:.2f}%。"
        )
    return apply_evidence(
        hypothesis,
        observed_at=observed_at,
        evidence_type="INTRADAY_MARKET_BREADTH",
        source=str(state.get("source_provider") or "QMT"),
        summary=summary,
        strength=strength,
        polarity=polarity,
        payload={
            "state_id": state.get("state_id"),
            "market_state": state.get("state"),
            "quality_status": quality,
            "actionable": actionable,
            "positive_breadth_pct": breadth,
            "equal_weight_return_pct": equal_weight,
            "confirming_points": confirming,
        },
        trigger_confirmed=actionable,
    )


def _stock_update(
    hypothesis: TradeHypothesis,
    decision: dict[str, Any],
    order_flow: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[TradeHypothesis, Any]:
    status = str(decision.get("status") or "WATCHING")
    action = str(decision.get("action") or "WATCH")
    current_return = _float(decision.get("current_return_pct"))
    relative = _float(decision.get("relative_strength_pct"))
    amount_ratio = _float(decision.get("intraday_amount_ratio"))
    breadth = _float(
        decision.get("theme_positive_breadth_pct")
    )
    ofi = _float(order_flow.get("ofi_normalized"))
    queue = _float(order_flow.get("queue_imbalance"))
    spread = order_flow.get("spread_bps")
    positive = 0.0
    negative = 0.0
    if status == "ORDER_CREATED":
        positive += 0.90
    elif status == "ACTIVATABLE" or action.startswith("ACTIVATE_"):
        positive += 0.65
    elif status == "RISK_REJECTED":
        negative += 0.75
    if relative >= 1.0:
        positive += min(0.35, relative / 10.0)
    elif relative <= -1.0:
        negative += min(0.35, abs(relative) / 10.0)
    if amount_ratio >= 1.50:
        positive += min(0.35, (amount_ratio - 1.0) / 4.0)
    elif 0 < amount_ratio < 0.70:
        negative += 0.18
    if breadth >= 55:
        positive += min(0.25, (breadth - 45.0) / 100.0)
    elif 0 < breadth < 35:
        negative += 0.22
    if order_flow.get("quality_status") == "PASS":
        if ofi >= 0.45 and queue > 0:
            positive += min(0.35, ofi / 8.0 + queue / 5.0)
        elif ofi <= -0.45 and queue < 0:
            negative += min(0.35, abs(ofi) / 8.0 + abs(queue) / 5.0)
        if spread is not None and _float(spread) > 35:
            negative += 0.15
    net = positive - negative
    if net >= 0.12:
        polarity = "POSITIVE"
        strength = min(1.35, max(0.15, net))
    elif net <= -0.12:
        polarity = "NEGATIVE"
        strength = min(1.35, max(0.15, abs(net)))
    else:
        polarity = "NEUTRAL"
        strength = 0.0
    summary = (
        f"{hypothesis.scope_name}盘中证据：涨幅{current_return:.2f}%，"
        f"跑赢市场{relative:.2f}%，量能{amount_ratio:.2f}倍，"
        f"板块上涨宽度{breadth:.1f}%，"
        f"盘口OFI {ofi:.2f}，结论{polarity}。"
    )
    hard_invalidation = bool(
        current_return <= -5.0
        and relative <= -2.0
        and (
            order_flow.get("quality_status") != "PASS"
            or (ofi < -0.80 and queue < 0)
        )
    )
    trigger_confirmed = bool(
        polarity == "POSITIVE"
        and strength >= 0.55
        and (
            status in {"ACTIVATABLE", "ORDER_CREATED"}
            or action.startswith("ACTIVATE_")
        )
    )
    return apply_evidence(
        hypothesis,
        observed_at=observed_at,
        evidence_type="INTRADAY_STOCK_CONFIRMATION",
        source=(
            str(order_flow.get("source_provider") or "")
            or "QMT_INTRADAY_ACTIVATION"
        ),
        summary=summary,
        strength=strength,
        polarity=polarity,
        payload={
            "activation_id": decision.get("activation_id"),
            "state_id": decision.get("state_id"),
            "status": status,
            "action": action,
            "reason_code": decision.get("reason_code"),
            "current_return_pct": current_return,
            "relative_strength_pct": relative,
            "intraday_amount_ratio": amount_ratio,
            "theme_positive_breadth_pct": breadth,
            "order_flow": order_flow,
        },
        hard_invalidation=hard_invalidation,
        trigger_confirmed=trigger_confirmed,
    )


def update_intraday_hypotheses(
    engine: Engine,
    *,
    observed_at: datetime | None = None,
    intraday_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Update V3 hypotheses from one QMT-backed, paper-only intraday tick."""
    if (
        intraday_result
        and intraday_result.get("status") == "skipped"
    ):
        return {
            "status": "skipped",
            "reason": intraday_result.get("reason"),
            "updated_count": 0,
            "created_count": 0,
            "real_order_count": 0,
        }
    v2_repository = TradingV2ReadRepository(engine)
    v3_repository = TradingV3Repository(engine)
    summary = v2_repository.intraday_summary(
        account_id=ACCOUNT_ID,
        limit=1000,
    )
    state = summary.get("market_state")
    if not state:
        return {
            "status": "collecting",
            "reason": "INTRADAY_MARKET_STATE_MISSING",
            "updated_count": 0,
            "created_count": 0,
            "real_order_count": 0,
        }
    fallback_time = observed_at or datetime.now()
    evidence_time = _datetime(
        state.get("observed_at"),
        fallback_time,
    )
    trading_date = _date(
        state.get("trade_date"),
        evidence_time.date(),
    )
    hypotheses = v3_repository.active_hypotheses_for_intraday(
        trade_date=trading_date,
        limit=1000,
    )
    latest_run = (
        v3_repository.latest_run_metadata(trading_date)
        or v3_repository.latest_run_metadata()
    )
    if not latest_run:
        return {
            "status": "blocked",
            "reason": "V3_DAILY_DECISION_MISSING",
            "updated_count": 0,
            "created_count": 0,
            "real_order_count": 0,
        }
    by_code = {
        hypothesis.scope_code: (hypothesis_id, hypothesis)
        for hypothesis_id, hypothesis in hypotheses
        if hypothesis.scope_type == "STOCK"
    }
    market_item = next(
        (
            item
            for item in hypotheses
            if item[1].scope_type == "MARKET"
        ),
        None,
    )
    decisions = [
        dict(item) for item in (summary.get("decisions") or [])
    ]
    created_codes: set[str] = set()
    for decision in decisions:
        code = str(decision.get("stock_code") or "").zfill(6)
        if not code or code in by_code:
            continue
        discovery = _intraday_discovery_hypothesis(
            decision,
            run_uid=str(latest_run["run_uid"]),
            trade_date=trading_date,
            observed_at=evidence_time,
        )
        persisted = v3_repository.ensure_intraday_hypothesis(
            discovery
        )
        if persisted is not None:
            by_code[code] = persisted
            created_codes.add(code)
    order_flows = load_recent_order_flow(
        engine,
        stock_codes=by_code,
        observed_at=evidence_time,
        lookback_minutes=10,
    )
    updated: list[dict[str, Any]] = []
    if market_item is not None:
        hypothesis_id, hypothesis = market_item
        next_hypothesis, event = _market_update(
            hypothesis,
            state,
            observed_at=evidence_time,
        )
        if v3_repository.save_hypothesis_evidence(
            hypothesis_id=hypothesis_id,
            updated=next_hypothesis,
            evidence=event,
        ):
            updated.append(
                {
                    "scope_code": hypothesis.scope_code,
                    "scope_name": hypothesis.scope_name,
                    "state_before": event.state_before,
                    "state_after": event.state_after,
                    "probability_before": event.probability_before,
                    "probability_after": event.probability_after,
                    "summary": event.summary,
                }
            )
    decision_by_code = {
        str(item.get("stock_code") or "").zfill(6): item
        for item in decisions
    }
    for code, (hypothesis_id, hypothesis) in by_code.items():
        decision = decision_by_code.get(code)
        if decision is None or code in created_codes:
            continue
        next_hypothesis, event = _stock_update(
            hypothesis,
            decision,
            order_flows.get(code, {}),
            observed_at=evidence_time,
        )
        if v3_repository.save_hypothesis_evidence(
            hypothesis_id=hypothesis_id,
            updated=next_hypothesis,
            evidence=event,
        ):
            updated.append(
                {
                    "scope_code": code,
                    "scope_name": hypothesis.scope_name,
                    "state_before": event.state_before,
                    "state_after": event.state_after,
                    "probability_before": event.probability_before,
                    "probability_after": event.probability_after,
                    "summary": event.summary,
                }
            )
    return {
        "status": "ok",
        "trade_date": trading_date.isoformat(),
        "observed_at": evidence_time.isoformat(sep=" "),
        "updated_count": len(updated),
        "created_count": len(created_codes),
        "created_codes": sorted(created_codes),
        "updates": updated,
        "order_flow_coverage": len(order_flows),
        "real_order_count": 0,
    }
