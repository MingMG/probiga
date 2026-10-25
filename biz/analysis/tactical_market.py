"""Market-state routing and daily tactical candidate features.

The long-term trend model is not allowed to interpret every session as a
trend.  This module first separates normal trend/rotation sessions from panic
rebound and speculative short-term sessions, then builds the daily evidence
needed by the intraday confirmation stage.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any, Mapping

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


MODE_DEFENSIVE = "DEFENSIVE"
MODE_PANIC_REBOUND = "PANIC_REBOUND"
MODE_SPECULATIVE = "SPECULATIVE"
MODE_ROTATION = "ROTATION"
MODE_TREND = "TREND"

MODE_POLICY: dict[str, dict[str, Any]] = {
    MODE_TREND: {
        "label": "趋势行情",
        "tactic": "趋势回踩与主升浪",
        "risk_asset_cap_pct": 80,
        "strategy_weights": {
            "ultra_short": 0.90,
            "short_term": 1.00,
            "swing": 1.10,
            "main_wave": 1.20,
        },
    },
    MODE_ROTATION: {
        "label": "结构轮动",
        "tactic": "题材轮动与强势回踩",
        "risk_asset_cap_pct": 60,
        "strategy_weights": {
            "ultra_short": 1.00,
            "short_term": 1.15,
            "swing": 0.80,
            "main_wave": 0.70,
        },
    },
    MODE_PANIC_REBOUND: {
        "label": "恐慌修复",
        "tactic": "龙头反核与同题材低吸",
        "risk_asset_cap_pct": 25,
        "strategy_weights": {
            "ultra_short": 1.30,
            "short_term": 1.15,
            "swing": 0.35,
            "main_wave": 0.00,
        },
    },
    MODE_SPECULATIVE: {
        "label": "妖股短线",
        "tactic": "封板确认、跟随票恐慌低吸",
        "risk_asset_cap_pct": 35,
        "strategy_weights": {
            "ultra_short": 1.35,
            "short_term": 1.10,
            "swing": 0.45,
            "main_wave": 0.20,
        },
    },
    MODE_DEFENSIVE: {
        "label": "防守",
        "tactic": "禁止新开仓，等待市场止跌",
        "risk_asset_cap_pct": 0,
        "strategy_weights": {
            "ultra_short": 0.00,
            "short_term": 0.00,
            "swing": 0.00,
            "main_wave": 0.00,
        },
    },
}


@dataclass(frozen=True)
class MarketSnapshot:
    trade_date: str
    stock_count: int
    advancing_count: int
    declining_count: int
    limit_up_count: int
    limit_down_count: int
    average_change_pct: float
    median_change_pct: float
    breadth_pct: float
    limit_up_pct: float
    limit_down_pct: float
    amount_cny: float


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def market_snapshot(frame: pd.DataFrame, trade_date: str) -> MarketSnapshot:
    change = pd.to_numeric(frame.get("change_pct"), errors="coerce").dropna()
    amount = pd.to_numeric(frame.get("amount"), errors="coerce").fillna(0.0)
    count = int(len(change))
    if count <= 0:
        raise ValueError("market snapshot has no valid daily returns")
    advancing = int((change > 0).sum())
    declining = int((change < 0).sum())
    limit_up = int((change >= 9.7).sum())
    limit_down = int((change <= -9.7).sum())
    return MarketSnapshot(
        trade_date=trade_date,
        stock_count=count,
        advancing_count=advancing,
        declining_count=declining,
        limit_up_count=limit_up,
        limit_down_count=limit_down,
        average_change_pct=round(float(change.mean()), 6),
        median_change_pct=round(float(change.median()), 6),
        breadth_pct=round(advancing / count * 100.0, 6),
        limit_up_pct=round(limit_up / count * 100.0, 6),
        limit_down_pct=round(limit_down / count * 100.0, 6),
        amount_cny=round(float(amount.sum()), 2),
    )


def _previous_session(trade_date: str, frame: pd.DataFrame) -> str:
    window = frame.attrs.get("qmt_daily_input_window") or {}
    sessions = [
        str(item)[:10]
        for item in (window.get("sessions") or ())
        if str(item)[:10] < trade_date
    ]
    if sessions:
        return max(sessions)
    return (date.fromisoformat(trade_date) - timedelta(days=1)).isoformat()


def load_previous_snapshot(
    engine: Engine,
    *,
    trade_date: str,
    frame: pd.DataFrame,
) -> MarketSnapshot:
    previous_date = _previous_session(trade_date, frame)
    query = text(
        """
        SELECT /*+ MAX_EXECUTION_TIME(30000) */
               change_pct, amount
        FROM sm_stock_kline
        WHERE trade_date=:trade_date
          AND k_type=1
          AND adjust_type=0
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(
            query, {"trade_date": previous_date}
        ).mappings().all()
    previous = pd.DataFrame(rows)
    if previous.empty:
        raise ValueError(f"previous session market data is missing: {previous_date}")
    return market_snapshot(previous, previous_date)


def classify_mode(
    current: MarketSnapshot,
    previous: MarketSnapshot,
) -> dict[str, Any]:
    amount_expansion = (
        current.amount_cny / previous.amount_cny
        if previous.amount_cny > 0
        else 0.0
    )
    prior_stress = (
        previous.breadth_pct <= 35.0
        and previous.average_change_pct <= -1.50
    )
    broad_repair = (
        current.breadth_pct >= 65.0
        and current.average_change_pct >= 0.80
    )
    limit_expansion = (
        current.limit_up_count >= 70
        and current.limit_up_count
        >= max(55, int(previous.limit_up_count * 1.50))
    )
    if (
        current.breadth_pct < 30.0
        or current.average_change_pct <= -1.50
        or current.limit_down_pct >= 2.0
    ):
        mode = MODE_DEFENSIVE
        evidence = [
            f"上涨宽度{current.breadth_pct:.1f}%",
            f"等权涨幅{current.average_change_pct:.2f}%",
            f"跌停代理{current.limit_down_count}只",
        ]
    elif (
        prior_stress
        and broad_repair
        and limit_expansion
        and amount_expansion >= 1.10
    ):
        mode = MODE_PANIC_REBOUND
        evidence = [
            (
                f"前一交易日宽度{previous.breadth_pct:.1f}%、"
                f"等权涨幅{previous.average_change_pct:.2f}%形成恐慌"
            ),
            (
                f"当日宽度修复至{current.breadth_pct:.1f}%，"
                f"等权涨幅{current.average_change_pct:.2f}%"
            ),
            (
                f"涨停代理由{previous.limit_up_count}只扩至"
                f"{current.limit_up_count}只，成交额放大"
                f"{amount_expansion:.2f}倍"
            ),
        ]
    elif (
        current.limit_up_count >= 80
        and current.breadth_pct >= 50.0
        and current.average_change_pct > 0.20
    ):
        mode = MODE_SPECULATIVE
        evidence = [
            f"涨停代理{current.limit_up_count}只，情绪扩散",
            f"上涨宽度{current.breadth_pct:.1f}%",
            "指数和趋势确认不足，优先短线妖股模式",
        ]
    elif (
        current.breadth_pct >= 60.0
        and current.average_change_pct >= 0.50
        and previous.breadth_pct >= 45.0
        and current.limit_up_count >= previous.limit_up_count
    ):
        mode = MODE_TREND
        evidence = [
            f"连续上涨宽度{previous.breadth_pct:.1f}%→{current.breadth_pct:.1f}%",
            f"等权涨幅{current.average_change_pct:.2f}%",
            f"涨停代理{current.limit_up_count}只",
        ]
    else:
        mode = MODE_ROTATION
        evidence = [
            f"上涨宽度{current.breadth_pct:.1f}%",
            f"全市场等权涨幅{current.average_change_pct:.2f}%",
            "指数级趋势证据不足，按结构轮动处理",
        ]
    policy = MODE_POLICY[mode]
    return {
        "mode": mode,
        **policy,
        "prior_stress": prior_stress,
        "broad_repair": broad_repair,
        "limit_expansion": limit_expansion,
        "amount_expansion": round(amount_expansion, 6),
        "evidence": evidence,
        "current": asdict(current),
        "previous": asdict(previous),
        "as_of": current.trade_date,
    }


def load_tactical_context(
    engine: Engine,
    *,
    trade_date: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    current = market_snapshot(frame, trade_date)
    previous = load_previous_snapshot(
        engine, trade_date=trade_date, frame=frame
    )
    return classify_mode(current, previous)


def attach_daily_tactical_features(
    frame: pd.DataFrame,
    *,
    context: Mapping[str, Any],
) -> pd.DataFrame:
    out = frame.copy()
    close = pd.to_numeric(out.get("close"), errors="coerce")
    low = pd.to_numeric(out.get("low"), errors="coerce")
    high = pd.to_numeric(out.get("high"), errors="coerce")
    pre_close = pd.to_numeric(out.get("pre_close"), errors="coerce")
    amount = pd.to_numeric(out.get("amount"), errors="coerce").fillna(0.0)
    turnover = pd.to_numeric(
        out.get("turnover_ratio"), errors="coerce"
    ).fillna(0.0)
    previous_change = pd.to_numeric(
        out.get("previous_change_pct", 0.0), errors="coerce"
    ).fillna(0.0)
    change_pct = pd.to_numeric(
        out.get("change_pct"), errors="coerce"
    ).fillna(0.0)
    low_return = (low / pre_close.replace(0, np.nan) - 1.0) * 100.0
    rebound = (close / low.replace(0, np.nan) - 1.0) * 100.0
    spread = (high - low).replace(0, np.nan)
    close_location = ((close - low) / spread).clip(0.0, 1.0).fillna(0.5)
    amount_rank = amount.rank(pct=True, method="average").fillna(0.0) * 100.0
    amount_ratio = pd.to_numeric(
        out.get("amount_ratio_5", 0.0), errors="coerce"
    ).fillna(0.0)
    prior_limit = previous_change >= 9.7
    current_limit = change_pct >= 9.7
    deep_reversal = (
        low_return.le(-3.0)
        & rebound.ge(5.0)
        & close_location.ge(0.65)
        & change_pct.ge(-1.0)
    )
    reacceleration = prior_limit & current_limit & rebound.ge(6.0)
    follower_reversal = prior_limit & deep_reversal & ~current_limit
    broad_reversal = deep_reversal & amount_ratio.ge(1.20)
    mode = str(context.get("mode") or MODE_ROTATION)
    tactical_mode = mode in {
        MODE_PANIC_REBOUND,
        MODE_SPECULATIVE,
    }
    candidate = (
        tactical_mode
        & (reacceleration | follower_reversal | broad_reversal)
        & amount.ge(80_000_000)
    )
    score = (
        prior_limit.astype(float) * 15.0
        + current_limit.astype(float) * 12.0
        + deep_reversal.astype(float) * 18.0
        + reacceleration.astype(float) * 8.0
        + rebound.clip(0.0, 22.0) / 22.0 * 20.0
        + close_location * 12.0
        + (amount_rank / 100.0) * 8.0
        + turnover.clip(0.0, 35.0) / 35.0 * 5.0
    ).clip(0.0, 100.0)
    if tactical_mode:
        score = (score + 15.0).clip(0.0, 100.0)
    score = score.where(candidate, np.minimum(score, 45.0))
    out["market_mode"] = mode
    out["market_mode_label"] = str(context.get("label") or mode)
    out["tactical_style"] = str(context.get("tactic") or "")
    out["prior_day_change_pct"] = previous_change.round(6)
    out["intraday_low_return_pct"] = low_return.round(6)
    out["rebound_from_low_pct"] = rebound.round(6)
    out["close_location_value"] = close_location.round(6)
    out["tactical_candidate"] = candidate.astype(bool)
    out["tactical_reacceleration"] = reacceleration.astype(bool)
    out["tactical_follower_reversal"] = follower_reversal.astype(bool)
    out["tactical_score"] = score.round(2)
    role = pd.Series("", index=out.index, dtype="object")
    role.loc[reacceleration] = "龙头再加速"
    role.loc[follower_reversal] = "同题材跟随低吸"
    role.loc[broad_reversal & ~prior_limit] = "恐慌反核"
    out["tactical_daily_role"] = role
    out["market_mode_evidence"] = "；".join(
        str(item) for item in context.get("evidence") or ()
    )
    return out


def tactical_candidate_codes(
    frame: pd.DataFrame,
    *,
    limit: int = 120,
) -> list[str]:
    if "tactical_candidate" not in frame:
        return []
    selected = frame[frame["tactical_candidate"].fillna(False)].copy()
    if selected.empty:
        return []
    selected = selected.sort_values(
        ["tactical_score", "amount", "stock_code"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    return (
        selected["stock_code"].astype(str).str.zfill(6)
        .drop_duplicates().head(max(0, int(limit))).tolist()
    )


def attach_intraday_tactical_features(
    frame: pd.DataFrame,
    *,
    intraday: pd.DataFrame | None,
    index_stability: Mapping[str, Any] | None,
) -> pd.DataFrame:
    """Merge minute evidence and derive the next-session tactical plan."""

    out = frame.copy()
    if intraday is not None and not intraday.empty:
        evidence = intraday.copy()
        evidence["stock_code"] = (
            evidence["stock_code"].astype(str).str.zfill(6)
        )
        out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
        out = out.merge(
            evidence.drop_duplicates("stock_code", keep="last"),
            on="stock_code",
            how="left",
        )
    for column, default in (
        ("minute_available", False),
        ("minute_rows", 0),
        ("minute_rebound_from_low_pct", 0.0),
        ("minute_close_vs_vwap_pct", 0.0),
        ("minute_limit_hold_minutes", 0),
        ("minute_open_board_count", 0),
        ("minute_early_seal", False),
        ("minute_late_seal", False),
        ("minute_last_30m_return_pct", 0.0),
    ):
        if column not in out:
            out[column] = default
        elif column in {
            "minute_rebound_from_low_pct",
            "minute_close_vs_vwap_pct",
            "minute_last_30m_return_pct",
        }:
            out[column] = pd.to_numeric(
                out[column], errors="coerce"
            ).fillna(default)

    stable = bool((index_stability or {}).get("stable"))
    candidate = out.get(
        "tactical_candidate", pd.Series(False, index=out.index)
    ).fillna(False).astype(bool)
    available = out["minute_available"].fillna(False).astype(bool)
    rebound = pd.to_numeric(
        out["minute_rebound_from_low_pct"], errors="coerce"
    ).fillna(0.0)
    close_vs_vwap = pd.to_numeric(
        out["minute_close_vs_vwap_pct"], errors="coerce"
    ).fillna(0.0)
    hold = pd.to_numeric(
        out["minute_limit_hold_minutes"], errors="coerce"
    ).fillna(0.0)
    open_count = pd.to_numeric(
        out["minute_open_board_count"], errors="coerce"
    ).fillna(0.0)
    early_seal = out["minute_early_seal"].fillna(False).astype(bool)
    late_seal = out["minute_late_seal"].fillna(False).astype(bool)
    daily_score = pd.to_numeric(
        out.get("tactical_score", 0.0), errors="coerce"
    ).fillna(0.0)
    intraday_bonus = (
        available.astype(float) * 4.0
        + rebound.clip(0.0, 22.0) / 22.0 * 18.0
        + close_vs_vwap.clip(-5.0, 5.0).add(5.0) / 10.0 * 10.0
        + hold.clip(0.0, 120.0) / 120.0 * 12.0
        - open_count.clip(0.0, 5.0) * 3.0
        + (5.0 if stable else 0.0)
    )
    out["intraday_tactical_score"] = (
        daily_score + intraday_bonus
    ).clip(0.0, 100.0).where(candidate, 0.0).round(2)

    roles: list[str] = []
    triggers: list[str] = []
    invalidations: list[str] = []
    entry_lows: list[float | None] = []
    entry_highs: list[float | None] = []
    stops: list[float | None] = []
    targets: list[float | None] = []
    for row in out.to_dict(orient="records"):
        if not bool(row.get("tactical_candidate")):
            roles.append(str(row.get("tactical_daily_role") or ""))
            triggers.append("")
            invalidations.append("")
            entry_lows.append(None)
            entry_highs.append(None)
            stops.append(None)
            targets.append(None)
            continue
        first_limit = row.get("minute_first_limit_time")
        is_leader = bool(
            first_limit is not None
            and not pd.isna(first_limit)
            and row.get("minute_early_seal")
        )
        is_late_follower = bool(
            first_limit is not None
            and not pd.isna(first_limit)
            and row.get("minute_late_seal")
        )
        daily_role = str(row.get("tactical_daily_role") or "")
        if is_leader:
            role = "龙头封板"
        elif is_late_follower:
            role = "尾盘跟随封板"
        elif "低吸" in daily_role or row.get("tactical_follower_reversal"):
            role = "同题材恐慌低吸"
        else:
            role = "恐慌反核观察"
        roles.append(role)
        vwap = _finite(row.get("minute_vwap"), 0.0)
        pre_close = _finite(row.get("pre_close"), 0.0)
        close = _finite(row.get("close"), 0.0)
        limit_time = row.get("minute_first_limit_time")
        limit_clock = (
            pd.Timestamp(limit_time).strftime("%H:%M")
            if limit_time is not None and not pd.isna(limit_time)
            else "未封板"
        )
        low_time = row.get("minute_low_time")
        low_clock = (
            pd.Timestamp(low_time).strftime("%H:%M")
            if low_time is not None and not pd.isna(low_time)
            else "未识别"
        )
        reclaim_time = row.get("minute_preclose_reclaim_time")
        reclaim_clock = (
            pd.Timestamp(reclaim_time).strftime("%H:%M")
            if reclaim_time is not None and not pd.isna(reclaim_time)
            else "未收复"
        )
        anchor = vwap if vwap > 0 else close
        if role in {"龙头封板", "尾盘跟随封板"}:
            entry_low = min(anchor, pre_close or anchor) * 0.99
            entry_high = min(anchor, pre_close or anchor) * 1.02
            stop_pct = 0.035
            trigger = (
                f"昨日{limit_clock}首次封板，{low_clock}见日内低点，"
                f"{reclaim_clock}收复昨收；次日回踩昨日VWAP/昨收不破，"
                "分时重新放量且龙头未炸板再考虑低吸"
            )
        else:
            entry_low = min(vwap or close, close)
            entry_high = max(vwap or close, pre_close or close)
            stop_pct = 0.045
            trigger = (
                f"昨日{low_clock}见恐慌低点，{reclaim_clock}收复昨收；"
                "次日不再破前日低点，收复VWAP后横盘承接，"
                "且同题材龙头未明显走弱"
            )
        entry_mid = (entry_low + entry_high) / 2.0
        stop = entry_mid * (1.0 - stop_pct)
        target = entry_mid * 1.08
        triggers.append(trigger)
        invalidations.append(
            "跌破入场参考价的止损位、龙头炸板且无法回封、"
            "或同题材出现批量负反馈"
        )
        entry_lows.append(round(entry_low, 3))
        entry_highs.append(round(entry_high, 3))
        stops.append(round(stop, 3))
        targets.append(round(target, 3))
    out["tactical_intraday_role"] = roles
    out["tactical_trigger"] = triggers
    out["tactical_invalidation"] = invalidations
    out["tactical_entry_low"] = entry_lows
    out["tactical_entry_high"] = entry_highs
    out["tactical_stop_loss"] = stops
    out["tactical_take_profit"] = targets
    out["index_stability_json"] = json.dumps(
        dict(index_stability or {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    return out
