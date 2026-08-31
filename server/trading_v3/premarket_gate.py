"""V3-native call-auction review for an immutable daily stock pool.

This is a read-only advisory overlay.  It never changes the close-decision
ledger, never assumes a fixed primary/backup chain and never grants order
authority.  Every candidate is re-evaluated against the same point-in-time
quote evidence, after which the whole set is ranked again.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from .order_flow import calculate_order_flow


AUCTION_GATE_SCHEMA = "probiga.trading-v3.premarket-gate.v1"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _code(value: Any) -> str:
    return str(value or "").strip().split(".", 1)[0].zfill(6)


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=None)
    except ValueError:
        return None


def _base_score(item: Mapping[str, Any]) -> float:
    score = _number(item.get("raw_score"))
    if score is None:
        return 0.0
    return _clamp(score / 100.0 if abs(score) > 1.5 else score)


def _gap_score(gap_pct: float) -> float:
    if -1.5 <= gap_pct <= 3.5:
        return 1.0
    if -3.0 <= gap_pct < -1.5:
        return 0.65
    if 3.5 < gap_pct < 5.0:
        return 0.70
    if -4.0 < gap_pct < -3.0:
        return 0.35
    if 5.0 <= gap_pct < 7.0:
        return 0.30
    return 0.0


def _order_flow_score(flow: Mapping[str, Any]) -> float:
    if str(flow.get("quality_status") or "") != "PASS":
        return 0.0
    queue = _number(flow.get("queue_imbalance")) or 0.0
    ofi = _number(flow.get("ofi_normalized")) or 0.0
    spread = _number(flow.get("spread_bps"))
    spread_penalty = 0.0 if spread is None else _clamp(spread / 100.0) * 0.25
    return _clamp(
        0.50 + 0.25 * _clamp(queue, -1.0, 1.0)
        + 0.15 * math.tanh(ofi) - spread_penalty
    )


def _latest_event(
    events: Iterable[Mapping[str, Any]],
    *,
    cutoff_at: datetime,
) -> dict[str, Any] | None:
    eligible = []
    for raw in events:
        observed = _datetime(raw.get("quote_at"))
        if observed is None or observed > cutoff_at:
            continue
        eligible.append((observed, dict(raw)))
    if not eligible:
        return None
    eligible.sort(key=lambda value: (
        value[0], str(value[1].get("quote_event_id") or "")
    ))
    return eligible[-1][1]


def _advisory_action(
    *,
    original_actionability: str,
    gate_status: str,
    review_score: float,
) -> str:
    if gate_status in {
        "DATA_BLOCKED", "UNBUYABLE", "REJECT_CHASE", "REJECT_WEAK",
    }:
        return gate_status
    if original_actionability == "BUY_ZONE":
        return "BUY_CANDIDATE" if review_score >= 0.65 else "WAIT_OPEN_CONFIRM"
    if original_actionability == "WAIT_TRIGGER":
        return "WAIT_OPEN_CONFIRM"
    if original_actionability == "PAPER_ONLY":
        return "PAPER_REVIEW"
    if original_actionability == "REJECTED":
        return "REJECTED"
    return "RESEARCH_ONLY"


def assess_premarket_candidates(
    pool: Mapping[str, Any],
    quote_events: Iterable[Mapping[str, Any]],
    *,
    session_date: date | str,
    cutoff_at: datetime,
) -> dict[str, Any]:
    """Re-evaluate all pool candidates using one frozen auction cutoff."""

    session = (
        session_date.isoformat()
        if isinstance(session_date, date)
        else str(session_date or "")[:10]
    )
    if cutoff_at.tzinfo is not None or cutoff_at.date().isoformat() != session:
        raise ValueError("auction cutoff must be naive and match session date")
    if pool.get("pool_readable") is not True:
        return {
            "schema": AUCTION_GATE_SCHEMA,
            "status": "UPSTREAM_UNAVAILABLE",
            "session_date": session,
            "cutoff_at": cutoff_at.isoformat(sep=" "),
            "source_run_uid": pool.get("run_uid"),
            "reason": "策略池不可读，不能把上游缺失解释为竞价无候选",
            "assessments": [],
            "summary": {"candidate_count": 0, "reviewed_count": 0},
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    items = [
        dict(item) for item in (pool.get("items") or [])
        if isinstance(item, Mapping)
        and item.get("is_strategy_candidate") is True
    ]
    if not items:
        return {
            "schema": AUCTION_GATE_SCHEMA,
            "status": "VALID_EMPTY",
            "session_date": session,
            "cutoff_at": cutoff_at.isoformat(sep=" "),
            "source_run_uid": pool.get("run_uid"),
            "reason": "已验证策略批次没有候选，竞价层无需重评",
            "assessments": [],
            "summary": {"candidate_count": 0, "reviewed_count": 0},
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    by_code: dict[str, list[dict[str, Any]]] = {}
    for raw in quote_events:
        code = _code(raw.get("stock_code"))
        if code.strip("0"):
            by_code.setdefault(code, []).append(dict(raw))

    assessments: list[dict[str, Any]] = []
    for item in items:
        code = _code(item.get("stock_code"))
        events = by_code.get(code, [])
        latest = _latest_event(events, cutoff_at=cutoff_at)
        flow = calculate_order_flow(events)
        gate_status = "CONFIRMED"
        reasons: list[str] = []
        gap_pct: float | None = None
        freshness_seconds: float | None = None
        price: float | None = None
        if latest is None:
            gate_status = "DATA_BLOCKED"
            reasons.append("集合竞价截止前没有点时行情")
        else:
            observed_at = _datetime(latest.get("quote_at"))
            freshness_seconds = (
                max(0.0, (cutoff_at - observed_at).total_seconds())
                if observed_at is not None else None
            )
            price = _number(latest.get("last_price"))
            pre_close = _number(latest.get("pre_close"))
            upper_limit = _number(latest.get("upper_limit"))
            ask1 = _number(latest.get("ask1"))
            ask1_volume = _number(latest.get("ask1_volume"))
            suspended = bool(int(_number(latest.get("suspended")) or 0))
            if price is not None and pre_close is not None and pre_close > 0:
                gap_pct = (price / pre_close - 1.0) * 100.0
            if (
                price is None or price <= 0 or pre_close is None
                or pre_close <= 0 or gap_pct is None
            ):
                gate_status = "DATA_BLOCKED"
                reasons.append("竞价价格或前收盘价不完整")
            elif freshness_seconds is None or freshness_seconds > 180:
                gate_status = "DATA_BLOCKED"
                reasons.append("竞价行情超过三分钟未更新")
            elif str(flow.get("quality_status") or "") != "PASS":
                gate_status = "DATA_BLOCKED"
                reasons.append("盘口事件不足，不能用单点价格冒充竞价确认")
            elif suspended:
                gate_status = "UNBUYABLE"
                reasons.append("证券停牌或不可交易")
            elif upper_limit is not None and price >= upper_limit * 0.999:
                gate_status = "UNBUYABLE"
                reasons.append("竞价已接近涨停，缺少可验证成交空间")
            elif ask1 is None or ask1 <= 0 or ask1_volume is None or ask1_volume <= 0:
                gate_status = "UNBUYABLE"
                reasons.append("卖一价格或数量缺失，不能确认可成交")
            elif gap_pct >= 7.0 or (
                gap_pct >= 5.0
                and (_number(flow.get("queue_imbalance")) or 0.0) < 0
            ):
                gate_status = "REJECT_CHASE"
                reasons.append("竞价溢价与盘口承接不匹配，拒绝机械追高")
            elif gap_pct <= -4.0 and (
                _number(flow.get("queue_imbalance")) or 0.0
            ) < 0:
                gate_status = "REJECT_WEAK"
                reasons.append("竞价显著走弱且卖盘占优")
            else:
                reasons.append("竞价价格、可成交性和盘口质量通过基础复核")

        base_score = _base_score(item)
        gap_component = _gap_score(gap_pct) if gap_pct is not None else 0.0
        order_component = _order_flow_score(flow)
        review_score = round(
            0.55 * base_score + 0.20 * gap_component
            + 0.25 * order_component,
            6,
        )
        original_actionability = str(
            item.get("actionability") or "RESEARCH_ONLY"
        ).upper()
        action = _advisory_action(
            original_actionability=original_actionability,
            gate_status=gate_status,
            review_score=review_score,
        )
        if action in {"RESEARCH_ONLY", "PAPER_REVIEW", "WAIT_OPEN_CONFIRM"}:
            reasons.append("竞价层不升级盘后批次原有权限")
        assessments.append({
            "stock_code": code,
            "stock_name": str(item.get("stock_name") or code),
            "source_rank_no": item.get("rank_no"),
            "primary_theme": item.get("primary_theme"),
            "dynamic_role": item.get("dynamic_role"),
            "theme_rank": item.get("theme_rank"),
            "daily_change": item.get("daily_change"),
            "original_actionability": original_actionability,
            "quote_price": round(price, 4) if price is not None else None,
            "gap_pct": round(gap_pct, 4) if gap_pct is not None else None,
            "quote_at": (
                _datetime((latest or {}).get("quote_at")).isoformat(sep=" ")
                if _datetime((latest or {}).get("quote_at")) else None
            ),
            "freshness_seconds": (
                round(freshness_seconds, 3)
                if freshness_seconds is not None else None
            ),
            "gate_status": gate_status,
            "advisory_action": action,
            "review_score": review_score,
            "score_components": {
                "close_pool": round(base_score, 6),
                "auction_gap": round(gap_component, 6),
                "order_flow": round(order_component, 6),
            },
            "order_flow": flow,
            "reasons": reasons,
            "related_candidates": list(item.get("related_candidates") or []),
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        })

    rankable_actions = {
        "BUY_CANDIDATE", "WAIT_OPEN_CONFIRM", "PAPER_REVIEW", "RESEARCH_ONLY",
    }
    rankable = sorted(
        [row for row in assessments if row["advisory_action"] in rankable_actions],
        key=lambda row: (
            -float(row["review_score"]),
            int(row.get("source_rank_no") or 999_999),
            row["stock_code"],
        ),
    )
    rank_by_code = {
        row["stock_code"]: index for index, row in enumerate(rankable, 1)
    }
    for row in assessments:
        row["decision_rank"] = rank_by_code.get(row["stock_code"])
        alternatives = [
            {
                "stock_code": other["stock_code"],
                "stock_name": other["stock_name"],
                "decision_rank": rank_by_code.get(other["stock_code"]),
                "primary_theme": other.get("primary_theme"),
                "relation": (
                    "SAME_SCENARIO"
                    if other.get("primary_theme") == row.get("primary_theme")
                    else "OTHER_SCENARIO"
                ),
            }
            for other in rankable
            if other["stock_code"] != row["stock_code"]
        ]
        row["alternative_set"] = alternatives[:5]

    summary = {
        "candidate_count": len(items),
        "reviewed_count": len(assessments),
        "buy_candidate_count": sum(
            row["advisory_action"] == "BUY_CANDIDATE" for row in assessments
        ),
        "wait_count": sum(
            row["advisory_action"] == "WAIT_OPEN_CONFIRM" for row in assessments
        ),
        "blocked_count": sum(
            row["gate_status"] in {"DATA_BLOCKED", "UNBUYABLE"}
            for row in assessments
        ),
        "rejected_count": sum(
            row["gate_status"] in {"REJECT_CHASE", "REJECT_WEAK"}
            for row in assessments
        ),
    }
    core = {
        "schema": AUCTION_GATE_SCHEMA,
        "status": "COMPLETED",
        "stage": (
            "FINAL_0925"
            if cutoff_at.time() >= time(9, 25) else "OBSERVATION_0920"
        ),
        "session_date": session,
        "cutoff_at": cutoff_at.isoformat(sep=" "),
        "source_run_uid": pool.get("run_uid"),
        "source_data_date": pool.get("trade_date") or pool.get("data_date"),
        "summary": summary,
        "assessments": sorted(
            assessments,
            key=lambda row: (
                row.get("decision_rank") or 999_999,
                row.get("source_rank_no") or 999_999,
                row["stock_code"],
            ),
        ),
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
        "automatic_substitution": False,
    }
    gate_hash = hashlib.sha256(json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()
    return {**core, "gate_hash": gate_hash}


def load_auction_quote_events(
    engine: Engine,
    *,
    stock_codes: Iterable[str],
    session_date: date,
    cutoff_at: datetime,
) -> list[dict[str, Any]]:
    codes = sorted({_code(code) for code in stock_codes if _code(code).strip("0")})
    if not codes:
        return []
    start_at = datetime.combine(session_date, time(9, 15))
    statement = text(
        """
        SELECT quote_event_id, stock_code, quote_at, received_at,
               bid1, bid1_volume, ask1, ask1_volume, last_price,
               pre_close, upper_limit, lower_limit, suspended,
               source_provider, source_batch_id, payload_hash
        FROM st_quote_event_v2
        WHERE stock_code IN :codes
          AND quote_at >= :start_at
          AND quote_at <= :cutoff_at
        ORDER BY stock_code, quote_at, quote_event_id
        """
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(statement, {
            "codes": codes,
            "start_at": start_at,
            "cutoff_at": cutoff_at,
        }).mappings().all()
    return [dict(row) for row in rows]


def build_premarket_gate(
    engine: Engine,
    pool: Mapping[str, Any],
    *,
    session_date: date,
    cutoff_at: datetime,
) -> dict[str, Any]:
    codes = [
        _code(item.get("stock_code")) for item in (pool.get("items") or [])
        if isinstance(item, Mapping)
        and item.get("is_strategy_candidate") is True
    ]
    events = load_auction_quote_events(
        engine,
        stock_codes=codes,
        session_date=session_date,
        cutoff_at=cutoff_at,
    )
    return assess_premarket_candidates(
        pool,
        events,
        session_date=session_date,
        cutoff_at=cutoff_at,
    )


__all__ = [
    "AUCTION_GATE_SCHEMA",
    "assess_premarket_candidates",
    "build_premarket_gate",
    "load_auction_quote_events",
]
