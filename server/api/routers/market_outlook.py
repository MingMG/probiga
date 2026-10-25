"""Next-session market and tactical-pool outlook."""
from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from typing import Any, Mapping

from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.api.routers._engine import get_engine


router = APIRouter(tags=["market-outlook"])


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _iso_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat(sep=" ")
        except TypeError:
            return value.isoformat()
    return str(value).replace("T", " ")


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _active_pool_date(connection) -> str:
    row = connection.execute(text(
        "SELECT pick_date FROM st_recommended_stocks "
        "WHERE publication_status='ACTIVE' "
        "GROUP BY pick_date ORDER BY pick_date DESC LIMIT 1"
    )).first()
    return str(row[0])[:10] if row and row[0] else ""


def _calendar_neighbors(connection, trade_date: str) -> tuple[str, str]:
    previous = connection.execute(text(
        "SELECT trade_date FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date<:d "
        "ORDER BY trade_date DESC LIMIT 1"
    ), {"d": trade_date}).scalar()
    following = connection.execute(text(
        "SELECT trade_date FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date>:d "
        "ORDER BY trade_date ASC LIMIT 1"
    ), {"d": trade_date}).scalar()
    return (
        str(previous)[:10] if previous else "",
        str(following)[:10] if following else "",
    )


def _market_snapshot(connection, trade_date: str) -> dict[str, Any]:
    row = connection.execute(text(
        """
        SELECT COUNT(*) AS stock_count,
               AVG(change_pct) AS average_change_pct,
               SUM(CASE WHEN change_pct>0 THEN 1 ELSE 0 END) AS advancing_count,
               SUM(CASE WHEN change_pct<0 THEN 1 ELSE 0 END) AS declining_count,
               SUM(CASE WHEN change_pct>=9.7 THEN 1 ELSE 0 END) AS limit_up_count,
               SUM(CASE WHEN change_pct<=-9.7 THEN 1 ELSE 0 END) AS limit_down_count,
               SUM(amount) AS amount_cny
        FROM sm_stock_kline
        WHERE trade_date=:d AND k_type=1 AND adjust_type=0
        """
    ), {"d": trade_date}).mappings().first()
    if not row or int(row.get("stock_count") or 0) <= 0:
        return {"trade_date": trade_date, "available": False}
    count = int(row["stock_count"])
    advancing = int(row.get("advancing_count") or 0)
    limit_up = int(row.get("limit_up_count") or 0)
    limit_down = int(row.get("limit_down_count") or 0)
    return {
        "trade_date": trade_date,
        "available": True,
        "stock_count": count,
        "advancing_count": advancing,
        "declining_count": int(row.get("declining_count") or 0),
        "limit_up_count": limit_up,
        "limit_down_count": limit_down,
        "breadth_pct": round(advancing * 100.0 / count, 4),
        "average_change_pct": round(
            _finite(row.get("average_change_pct")), 4
        ),
        "limit_up_pct": round(limit_up * 100.0 / count, 4),
        "limit_down_pct": round(limit_down * 100.0 / count, 4),
        "amount_cny": round(_finite(row.get("amount_cny")), 2),
    }


def _market_mode(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> tuple[str, str, str, list[str]]:
    current_breadth = _finite(current.get("breadth_pct"))
    previous_breadth = _finite(previous.get("breadth_pct"))
    current_average = _finite(current.get("average_change_pct"))
    previous_average = _finite(previous.get("average_change_pct"))
    current_limits = int(current.get("limit_up_count") or 0)
    previous_limits = int(previous.get("limit_up_count") or 0)
    amount_ratio = (
        _finite(current.get("amount_cny"))
        / max(1.0, _finite(previous.get("amount_cny")))
    )
    prior_stress = (
        previous_breadth <= 35.0 and previous_average <= -1.5
    )
    broad_repair = current_breadth >= 65.0 and current_average >= 0.8
    limit_expansion = (
        current_limits >= 70
        and current_limits >= max(55, int(previous_limits * 1.5))
    )
    if (
        current_breadth < 30.0
        or current_average <= -1.5
        or _finite(current.get("limit_down_pct")) >= 2.0
    ):
        mode = "DEFENSIVE"
        label = "防守"
        tactic = "禁止新开仓，等待市场止跌"
    elif prior_stress and broad_repair and limit_expansion and amount_ratio >= 1.1:
        mode = "PANIC_REBOUND"
        label = "恐慌修复"
        tactic = "龙头反核确认与同周期跟随票低吸"
    elif (
        current_limits >= 80
        and current_breadth >= 50.0
        and current_average > 0.2
    ):
        mode = "SPECULATIVE"
        label = "妖股短线"
        tactic = "封板确认与跟随票恐慌低吸"
    elif current_breadth >= 60.0 and current_average >= 0.5:
        mode = "TREND"
        label = "趋势行情"
        tactic = "趋势回踩与主升浪"
    else:
        mode = "ROTATION"
        label = "结构轮动"
        tactic = "题材轮动与强势回踩"
    evidence = [
        f"上涨宽度 {current_breadth:.2f}%",
        f"全市场等权涨幅 {current_average:.2f}%",
        f"涨停代理 {current_limits} 家 / 跌停代理 "
        f"{int(current.get('limit_down_count') or 0)} 家",
        f"成交额较前一交易日 {amount_ratio:.2f} 倍",
    ]
    return mode, label, tactic, evidence


def _scenario_plan(
    mode: str,
    current: Mapping[str, Any],
) -> list[dict[str, Any]]:
    breadth = _finite(current.get("breadth_pct"))
    limit_up = int(current.get("limit_up_count") or 0)
    if mode == "PANIC_REBOUND":
        templates = (
            (
                "DIVERGENCE",
                "高开分歧、核心抱团",
                50,
                "最可能：指数高开后回落，涨停家数收缩，前排与跟风明显分层",
                "只在核心票回踩昨日VWAP/昨收附近确认承接时观察，不追开盘急拉",
            ),
            (
                "CONTINUATION",
                "修复延续、短线扩散",
                30,
                "竞价溢价维持，10:00前涨停扩散，成交额不明显萎缩",
                "龙头保持强度时做跟随票低吸；龙头不稳则取消跟随",
            ),
            (
                "RETREAT",
                "二次退潮",
                20,
                "前排低开或快速炸板，跌停代理增加，市场宽度跌回35%以下",
                "停止新开仓，已有仓位按失效线退出",
            ),
        )
    elif mode == "SPECULATIVE":
        templates = (
            (
                "DIVERGENCE", "高位分歧", 45,
                "涨停仍多但晋级率下降，资金向新核心集中",
                "只做首次分歧回封和低位补涨，不接一致加速",
            ),
            (
                "CONTINUATION", "情绪延续", 35,
                "涨停扩散且跌停维持低位",
                "龙头确认后参与低位跟随",
            ),
            (
                "RETREAT", "情绪退潮", 20,
                "前排批量断板，跌停增加",
                "停止新开仓",
            ),
        )
    elif mode == "DEFENSIVE":
        templates = (
            (
                "RETREAT", "风险释放", 65,
                "宽度弱、跌停多，反抽持续性不足",
                "空仓观察，不做左侧猜底",
            ),
            (
                "STABILIZE", "弱势止跌", 25,
                "跌停收敛但赚钱效应尚未恢复",
                "仅观察抗跌核心，不主动开仓",
            ),
            (
                "REPAIR", "超跌修复", 10,
                "指数和宽度同步转强",
                "等待连续两个观察窗确认后再评估",
            ),
        )
    else:
        templates = (
            (
                "ROTATION", "轮动分化", 45,
                "指数震荡，板块强弱切换",
                "做强势板块核心回踩",
            ),
            (
                "CONTINUATION", "趋势延续", 35,
                "宽度和成交额同步改善",
                "持有强趋势，避免追高",
            ),
            (
                "RETREAT", "转弱", 20,
                "核心板块跌破承接位",
                "降低仓位",
            ),
        )
    result = []
    for key, label, probability, description, action in templates:
        if key == "CONTINUATION" and breadth >= 75 and limit_up >= 120:
            probability += 5
        elif key == "DIVERGENCE" and breadth >= 70:
            probability -= 5
        result.append({
            "key": key,
            "label": label,
            "probability_pct": max(5, probability),
            "description": description,
            "action": action,
        })
    total = sum(int(item["probability_pct"]) for item in result)
    if total != 100:
        result[0]["probability_pct"] += 100 - total
    return result


def _limit_ratio(stock_code: str, stock_name: str) -> float:
    upper_name = str(stock_name or "").upper()
    if "ST" in upper_name:
        return 0.05
    if stock_code.startswith(("300", "301", "688", "689")):
        return 0.20
    if stock_code.startswith(("4", "8", "92")):
        return 0.30
    return 0.10


def _stock_outlook(row: Mapping[str, Any]) -> dict[str, Any]:
    conditions = _json_list(row.get("entry_conditions_json"))
    event: dict[str, Any] = {}
    for item in conditions:
        text_value = str(item or "")
        if text_value.startswith("intraday_event="):
            event = _json_object(text_value.split("=", 1)[1])
            break
    code = str(row.get("stock_code") or "").zfill(6)
    name = str(row.get("short_name") or code)
    reason = str(row.get("signal_reason") or "")
    role = ""
    match = re.match(r"^[A-Z_]+\s+(.+?)：", reason)
    if match:
        role = match.group(1)
    elif "龙头" in reason:
        role = "龙头封板"
    elif "跟随" in reason:
        role = "跟随低吸"
    close = _finite(row.get("close"))
    pre_close = _finite(row.get("pre_close"))
    ratio = _limit_ratio(code, name)
    next_limit = (
        round(close * (1.0 + ratio) + 1e-9, 2)
        if close > 0
        else None
    )
    return {
        "stock_code": code,
        "short_name": name,
        "role": role,
        "close": close,
        "pre_close": pre_close,
        "previous_vwap": _finite(event.get("vwap")) or None,
        "previous_low": _finite(event.get("session_low_price")) or None,
        "low_return_pct": _finite(event.get("session_low_return_pct")),
        "rebound_from_low_pct": _finite(event.get("rebound_from_low_pct")),
        "first_limit_time": event.get("first_limit_time"),
        "last_limit_time": event.get("last_limit_time"),
        "preclose_reclaim_time": event.get("preclose_reclaim_time"),
        "open_board_count": int(event.get("open_board_count") or 0),
        "next_limit_up_price": next_limit,
        "entry_price_low": _finite(row.get("entry_price_low")) or None,
        "entry_price_high": _finite(row.get("entry_price_high")) or None,
        "stop_loss_price": _finite(row.get("stop_loss_price")) or None,
        "take_profit_1": _finite(row.get("take_profit_1")) or None,
        "risk_reward_ratio": _finite(row.get("risk_reward_ratio")) or None,
        "signal_status": str(row.get("signal_status") or ""),
        "signal_reason": reason,
        "trigger": str(conditions[0] if conditions else ""),
        "invalidation": str(row.get("invalidation_reason") or ""),
        "short_term_score": _finite(row.get("short_term_score")),
        "capital_score": _finite(row.get("capital_score")),
        "technical_score": _finite(row.get("technical")),
    }


def _session_timeline(stocks: list[Mapping[str, Any]]) -> list[dict[str, str]]:
    leader = next(
        (item for item in stocks if "龙头" in str(item.get("role"))),
        stocks[0] if stocks else {},
    )
    follower = next(
        (item for item in stocks if item is not leader),
        {},
    )
    leader_name = str(leader.get("short_name") or "核心龙头")
    follower_name = str(follower.get("short_name") or "跟随票")
    return [
        {
            "time": "09:25",
            "title": "集合竞价定强弱",
            "condition": (
                f"{leader_name}溢价应明显强于{follower_name}；"
                "两者同时低于昨收则先按退潮处理"
            ),
        },
        {
            "time": "09:45",
            "title": "第一轮分歧",
            "condition": "看龙头是否守住分时均线；跟风票跌破昨收不接",
        },
        {
            "time": "10:30",
            "title": "主攻方向确认",
            "condition": "龙头回封且市场上涨宽度保持50%以上，跟随票才有低吸资格",
        },
        {
            "time": "13:30",
            "title": "午后持续性与炸板检查",
            "condition": "炸板扩散或龙头无法回封时取消所有跟随计划",
        },
        {
            "time": "14:30",
            "title": "尾盘去留",
            "condition": "指数稳定、龙头封单保持，才考虑保留短线仓；否则按失效条件退出",
        },
    ]


def build_market_outlook(
    engine: Engine,
    *,
    trade_date: str = "",
) -> dict[str, Any]:
    with engine.connect() as connection:
        requested = str(trade_date or "").strip()[:10]
        target = requested or _active_pool_date(connection)
        if not target:
            return {
                "status": "DATA_BLOCKED",
                "reason": "当前没有 ACTIVE 策略票池",
                "scenarios": [],
                "stocks": [],
            }
        try:
            target = date.fromisoformat(target).isoformat()
        except ValueError:
            return {
                "status": "DATA_BLOCKED",
                "reason": "交易日期格式无效",
                "scenarios": [],
                "stocks": [],
            }
        previous_date, next_date = _calendar_neighbors(connection, target)
        current = _market_snapshot(connection, target)
        previous = (
            _market_snapshot(connection, previous_date)
            if previous_date else {"trade_date": "", "available": False}
        )
        active_identity = connection.execute(text(
            "SELECT publisher_run_uid,COUNT(*) AS active_count "
            "FROM st_recommended_stocks "
            "WHERE pick_date=:d AND publication_status='ACTIVE' "
            "GROUP BY publisher_run_uid ORDER BY active_count DESC "
            "LIMIT 2"
        ), {"d": target}).mappings().all()
        if len(active_identity) > 1:
            return {
                "status": "DATA_BLOCKED",
                "reason": "当前 ACTIVE 票池存在多个发布身份",
                "trade_date": target,
                "next_trade_date": next_date,
                "scenarios": [],
                "stocks": [],
            }
        active_run_uid = (
            str(active_identity[0].get("publisher_run_uid") or "")
            if active_identity else ""
        )
        history_params: dict[str, Any] = {"d": target}
        history_condition = "trade_date=:d AND status='done'"
        if active_run_uid:
            history_condition += " AND run_uid=:run_uid"
            history_params["run_uid"] = active_run_uid
        history = connection.execute(text(
            "SELECT run_uid,status,total,passed,executable_count,"
            "canonical_pool_sha256,build_sha,published_at "
            "FROM st_recommended_run_history "
            f"WHERE {history_condition} "
            "ORDER BY published_at DESC,run_uid DESC LIMIT 1"
        ), history_params).mappings().first()
        pool_rows = connection.execute(text(
            "SELECT r.stock_code,r.short_name,r.short_term_score,"
            "r.capital_score,r.technical,r.signal_status,r.signal_reason,"
            "r.entry_price_low,r.entry_price_high,r.stop_loss_price,"
            "r.take_profit_1,r.risk_reward_ratio,r.entry_conditions_json,"
            "r.invalidation_reason,k.close,k.pre_close "
            "FROM st_recommended_stocks r "
            "INNER JOIN sm_stock_kline k "
            "ON k.stock_code=r.stock_code AND k.trade_date=r.pick_date "
            "AND k.k_type=1 AND k.adjust_type=0 "
            "WHERE r.pick_date=:d AND r.publication_status='ACTIVE' "
            "AND r.sources LIKE '%tactical%' "
            "ORDER BY CASE WHEN r.signal_reason LIKE '%龙头%' THEN 0 "
            "WHEN r.signal_reason LIKE '%跟随%' THEN 1 ELSE 2 END, "
            "r.short_term_score DESC,r.stock_code"
        ), {"d": target}).mappings().all()
    if not current.get("available") or not previous.get("available"):
        return {
            "status": "DATA_BLOCKED",
            "reason": "目标日或前一交易日市场数据不完整",
            "trade_date": target,
            "next_trade_date": next_date,
            "scenarios": [],
            "stocks": [],
        }
    if history is None:
        return {
            "status": "DATA_BLOCKED",
            "reason": "当前正式策略池缺少 done 发布记录",
            "trade_date": target,
            "next_trade_date": next_date,
            "scenarios": [],
            "stocks": [],
        }
    mode, mode_label, tactic, evidence = _market_mode(current, previous)
    stocks = [
        item
        for row in pool_rows
        if (item := _stock_outlook(dict(row))).get("first_limit_time")
    ]
    stocks.sort(
        key=lambda item: (
            0 if "龙头" in str(item["role"]) else 1,
            -_finite(item.get("short_term_score")),
            str(item["stock_code"]),
        )
    )
    return {
        "status": "READY",
        "trade_date": target,
        "next_trade_date": next_date,
        "generated_at": datetime.now().replace(microsecond=0).isoformat(
            sep=" "
        ),
        "market_mode": mode,
        "market_mode_label": mode_label,
        "tactical_style": tactic,
        "evidence": evidence,
        "current_market": current,
        "previous_market": previous,
        "scenarios": _scenario_plan(mode, current),
        "stocks": stocks[:6],
        "session_timeline": _session_timeline(stocks),
        "pool": {
            "run_uid": str(history.get("run_uid") or ""),
            "build_sha": str(history.get("build_sha") or ""),
            "analysis_count": int(history.get("total") or 0),
            "recommendation_count": int(history.get("passed") or 0),
            "executable_count": int(history.get("executable_count") or 0),
            "canonical_pool_sha256": str(
                history.get("canonical_pool_sha256") or ""
            ),
            "published_at": (
                _iso_text(history.get("published_at"))
            ),
        },
        "notice": (
            "情景概率为规则加权推演，不是收益承诺；"
            "所有入场都要求次日盘中再次确认。"
        ),
    }


@router.get("/market-outlook")
def market_outlook(trade_date: str = Query(default="")):
    try:
        return build_market_outlook(get_engine(), trade_date=trade_date)
    except Exception as exc:
        return {
            "status": "DATA_BLOCKED",
            "reason": f"{type(exc).__name__}: {str(exc)[:300]}",
            "scenarios": [],
            "stocks": [],
        }
