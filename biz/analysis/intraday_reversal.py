"""Intraday leader-seal and follower-reversal evidence for tactical pools."""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _minute_vwap(frame: pd.DataFrame) -> float:
    amount = pd.to_numeric(frame["amount"], errors="coerce").fillna(0.0)
    volume = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
    price = pd.to_numeric(frame["price"], errors="coerce")
    if amount.sum() <= 0 or volume.sum() <= 0 or price.notna().sum() == 0:
        return 0.0
    average_price = float(price.dropna().mean())
    candidates = []
    for denominator_scale in (1.0, 100.0):
        vwap = float(amount.sum()) / (
            float(volume.sum()) * denominator_scale
        )
        if vwap > 0:
            candidates.append((abs(math.log(vwap / average_price)), vwap))
    return min(candidates)[1] if candidates else 0.0


def summarize_stock_minutes(
    frame: pd.DataFrame,
    *,
    stock_code: str,
    source: str,
) -> dict[str, Any]:
    ordered = frame.sort_values("trade_time").reset_index(drop=True)
    if ordered.empty:
        return {
            "stock_code": stock_code,
            "minute_available": False,
            "minute_rows": 0,
            "minute_source": source,
            "intraday_event_json": json.dumps(
                {"status": "DATA_BLOCKED", "reason": "no minute rows"},
                ensure_ascii=False,
                sort_keys=True,
            ),
        }
    ordered["trade_time"] = pd.to_datetime(ordered["trade_time"])
    price = pd.to_numeric(ordered["price"], errors="coerce")
    change = pd.to_numeric(ordered["change_pct"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(ordered["amount"], errors="coerce").fillna(0.0)
    volume = pd.to_numeric(ordered["volume"], errors="coerce").fillna(0.0)
    pre_close = _finite(ordered.iloc[0].get("pre_close"), 0.0)
    if pre_close <= 0:
        price_change = change
        derived_pre_close = float(price.iloc[-1]) / (
            1.0 + float(change.iloc[-1]) / 100.0
        )
        pre_close = derived_pre_close if derived_pre_close > 0 else 0.0
        del price_change
    low_index = int(price.idxmin())
    high_index = int(price.idxmax())
    low_price = float(price.loc[low_index])
    close = float(price.iloc[-1])
    vwap = _minute_vwap(ordered)
    limit_mask = change >= 9.7
    limit_indices = list(np.flatnonzero(limit_mask.to_numpy()))
    first_limit = (
        ordered.loc[limit_indices[0], "trade_time"] if limit_indices else None
    )
    last_limit = (
        ordered.loc[limit_indices[-1], "trade_time"] if limit_indices else None
    )
    sealed_hold_minutes = 0
    open_board_count = 0
    if limit_indices:
        final_limit_index = limit_indices[-1]
        sealed_hold_minutes = max(
            0,
            int(
                (
                    ordered.loc[len(ordered) - 1, "trade_time"]
                    - ordered.loc[final_limit_index, "trade_time"]
                ).total_seconds()
                // 60
            ),
        )
        in_limit = False
        for index in range(limit_indices[0], len(ordered)):
            value = float(change.iloc[index])
            if value >= 9.7 and not in_limit:
                in_limit = True
            elif value < 9.4 and in_limit:
                open_board_count += 1
                in_limit = False
    low_time = ordered.loc[low_index, "trade_time"]
    reclaim_rows = ordered[
        (ordered["trade_time"] > low_time)
        & (pd.to_numeric(ordered["price"], errors="coerce") >= pre_close)
    ]
    reclaim_time = (
        reclaim_rows.iloc[0]["trade_time"] if not reclaim_rows.empty else None
    )
    low_return = (
        (low_price / pre_close - 1.0) * 100.0 if pre_close > 0 else 0.0
    )
    rebound = (
        (close / low_price - 1.0) * 100.0 if low_price > 0 else 0.0
    )
    close_location = (
        (close - low_price) / ((float(price.iloc[high_index]) - low_price))
        if float(price.iloc[high_index]) > low_price
        else 0.5
    )
    cutoff = datetime.combine(ordered.iloc[-1]["trade_time"].date(), time(14, 30))
    late = ordered[ordered["trade_time"] >= cutoff]
    last_30m_return = (
        (float(late.iloc[-1]["price"]) / float(late.iloc[0]["price"]) - 1.0)
        * 100.0
        if len(late) >= 2 and float(late.iloc[0]["price"]) > 0
        else 0.0
    )
    earlier_seal = bool(
        first_limit is not None and first_limit.time() <= time(13, 30)
    )
    late_seal = bool(
        first_limit is not None and first_limit.time() >= time(14, 20)
    )
    event = {
        "status": "AVAILABLE",
        "source": source,
        "rows": int(len(ordered)),
        "first_trade_time": ordered.iloc[0]["trade_time"].isoformat(sep=" "),
        "last_trade_time": ordered.iloc[-1]["trade_time"].isoformat(sep=" "),
        "session_low_time": low_time.isoformat(sep=" "),
        "session_low_price": round(low_price, 6),
        "session_low_return_pct": round(low_return, 4),
        "rebound_from_low_pct": round(rebound, 4),
        "close_location_value": round(close_location, 4),
        "vwap": round(vwap, 6),
        "close_vs_vwap_pct": (
            round((close / vwap - 1.0) * 100.0, 4) if vwap > 0 else None
        ),
        "first_limit_time": (
            first_limit.isoformat(sep=" ") if first_limit is not None else None
        ),
        "last_limit_time": (
            last_limit.isoformat(sep=" ") if last_limit is not None else None
        ),
        "limit_hold_minutes": sealed_hold_minutes,
        "open_board_count": open_board_count,
        "early_seal": earlier_seal,
        "late_seal": late_seal,
        "preclose_reclaim_time": (
            reclaim_time.isoformat(sep=" ")
            if reclaim_time is not None
            else None
        ),
        "last_30m_return_pct": round(last_30m_return, 4),
        "amount_cny": round(float(amount.sum()), 2),
        "volume": round(float(volume.sum()), 6),
    }
    return {
        "stock_code": stock_code,
        "minute_available": True,
        "minute_rows": int(len(ordered)),
        "minute_source": source,
        "minute_low_time": low_time,
        "minute_low_price": low_price,
        "minute_low_return_pct": low_return,
        "minute_rebound_from_low_pct": rebound,
        "minute_close_location": close_location,
        "minute_vwap": vwap,
        "minute_close_vs_vwap_pct": (
            (close / vwap - 1.0) * 100.0 if vwap > 0 else np.nan
        ),
        "minute_first_limit_time": first_limit,
        "minute_last_limit_time": last_limit,
        "minute_limit_hold_minutes": sealed_hold_minutes,
        "minute_open_board_count": open_board_count,
        "minute_early_seal": earlier_seal,
        "minute_late_seal": late_seal,
        "minute_preclose_reclaim_time": reclaim_time,
        "minute_last_30m_return_pct": last_30m_return,
        "intraday_event_json": json.dumps(
            event,
            ensure_ascii=False,
            sort_keys=True,
        ),
    }


def load_index_stability(
    engine: Engine,
    *,
    trade_date: str,
) -> dict[str, Any]:
    target = date.fromisoformat(trade_date)
    next_day = (target + timedelta(days=1)).isoformat()
    query = text(
        """
        SELECT /*+ MAX_EXECUTION_TIME(15000) */
               index_code, trade_time, price
        FROM sm_index_minute
        WHERE index_code=:index_code
          AND trade_time>=:trade_date
          AND trade_time<:next_day
        ORDER BY trade_time
        """
    )
    try:
        with engine.connect() as connection:
            rows = []
            for index_code in ("000001", "399001", "399006"):
                rows.extend(
                    connection.execute(
                        query,
                        {
                            "index_code": index_code,
                            "trade_date": trade_date,
                            "next_day": next_day,
                        },
                    ).mappings().all()
                )
    except Exception as exc:
        return {
            "status": "DATA_BLOCKED",
            "reason": f"{type(exc).__name__}: index minute unavailable",
        }
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {"status": "DATA_BLOCKED", "reason": "index minute rows missing"}
    frame["trade_time"] = pd.to_datetime(frame["trade_time"])
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    cutoff = datetime.combine(
        datetime.fromisoformat(trade_date).date(), time(14, 30)
    )
    items = []
    for index_code, group in frame.dropna(subset=["price"]).groupby("index_code"):
        group = group.sort_values("trade_time")
        late = group[group["trade_time"] >= cutoff]
        if len(late) < 2 or float(late.iloc[0]["price"]) <= 0:
            continue
        late_return = (
            float(late.iloc[-1]["price"]) / float(late.iloc[0]["price"]) - 1.0
        ) * 100.0
        session_open = float(group.iloc[0]["price"])
        session_return = (
            float(group.iloc[-1]["price"]) / session_open - 1.0
        ) * 100.0
        items.append({
            "index_code": str(index_code),
            "session_return_pct": round(session_return, 4),
            "last_30m_return_pct": round(late_return, 4),
            "stable": bool(session_return >= -0.5 and late_return >= -0.2),
        })
    if not items:
        return {"status": "DATA_BLOCKED", "reason": "late index rows missing"}
    return {
        "status": "AVAILABLE",
        "indices": items,
        "stable_count": sum(item["stable"] for item in items),
        "stable": sum(item["stable"] for item in items) >= 2,
    }


def load_intraday_evidence(
    engine: Engine,
    *,
    trade_date: str,
    stock_codes: Sequence[str],
) -> pd.DataFrame:
    codes = sorted({str(code).zfill(6) for code in stock_codes})
    if not codes:
        return pd.DataFrame()
    statement = text(
        """
        SELECT /*+ MAX_EXECUTION_TIME(30000) */
               stock_code, trade_time, price, `change`, change_pct,
               volume, amount, data_source
        FROM sm_stock_minute
        WHERE trade_date=:trade_date
          AND stock_code IN :stock_codes
          AND price IS NOT NULL
          AND price > 0
        ORDER BY stock_code, trade_time
        """
    ).bindparams(bindparam("stock_codes", expanding=True))
    frames = []
    with engine.connect() as connection:
        for offset in range(0, len(codes), 200):
            batch = codes[offset:offset + 200]
            rows = connection.execute(
                statement,
                {"trade_date": trade_date, "stock_codes": batch},
            ).mappings().all()
            if rows:
                frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    anchor_statement = text(
        """
        SELECT stock_code, pre_close
        FROM sm_stock_kline
        WHERE trade_date=:trade_date
          AND k_type=1
          AND adjust_type=0
          AND stock_code IN :stock_codes
        """
    ).bindparams(bindparam("stock_codes", expanding=True))
    anchor_frames = []
    with engine.connect() as connection:
        for offset in range(0, len(codes), 400):
            batch = codes[offset:offset + 400]
            rows = connection.execute(
                anchor_statement,
                {"trade_date": trade_date, "stock_codes": batch},
            ).mappings().all()
            if rows:
                anchor_frames.append(pd.DataFrame(rows))
    if anchor_frames:
        anchors = pd.concat(anchor_frames, ignore_index=True).drop_duplicates(
            "stock_code", keep="last"
        )
        anchors["stock_code"] = anchors["stock_code"].astype(str).str.zfill(6)
        frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
        frame = frame.merge(anchors, on="stock_code", how="left")
    summaries = []
    for code, group in frame.groupby("stock_code", sort=True):
        sources = sorted(
            str(value) for value in group["data_source"].dropna().unique()
        )
        if len(sources) != 1:
            summaries.append({
                "stock_code": str(code).zfill(6),
                "minute_available": False,
                "minute_rows": int(len(group)),
                "minute_source": ",".join(sources),
                "intraday_event_json": json.dumps(
                    {
                        "status": "DATA_BLOCKED",
                        "reason": "mixed minute sources",
                        "sources": sources,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            })
            continue
        summaries.append(
            summarize_stock_minutes(
                group,
                stock_code=str(code).zfill(6),
                source=sources[0],
            )
        )
    return pd.DataFrame(summaries)
