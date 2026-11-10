"""Intraday leader-seal and follower-reversal evidence for tactical pools."""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from server.common.qmt_index_minute_grid import CORE_GRID
from server.common.qmt_minute_publication_scope import load_verified_minute_security_rows


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


def _minute_provenance(proof: Mapping[str, Any], code: str) -> dict[str, Any]:
    return {
        "known_at": proof["known_at"],
        "publication_manifest_sha256": proof["manifest_sha256"],
        "source_row_sha256": proof["security_fingerprints"][code]["row_hash"],
    }


def load_index_stability(
    engine: Engine,
    *,
    trade_date: str,
    decision_known_at: datetime | str | None,
    history_engine: Engine,
) -> dict[str, Any]:
    codes = ("000001", "399001", "399006")
    if decision_known_at is None:
        return {"status": "DATA_BLOCKED", "reason": "index minute decision cutoff unavailable"}
    try:
        with engine.connect() as receipt_connection, history_engine.connect() as data_connection:
            proof, rows_by_code = load_verified_minute_security_rows(
                receipt_connection, data_connection, dataset="index_minute",
                trade_date=trade_date, decision_known_at=decision_known_at, codes=codes,
            )
    except (SQLAlchemyError, ValueError, TypeError, TimeoutError) as exc:
        return {"status": "DATA_BLOCKED", "reason": f"index minute proof unavailable ({type(exc).__name__})"}
    if set(rows_by_code) != set(codes):
        return {"status": "DATA_BLOCKED", "reason": "index minute publication incomplete at decision cutoff"}

    late_cutoff = datetime.combine(date.fromisoformat(trade_date), time(14, 30))
    items = []
    for code in codes:
        frame = pd.DataFrame(rows_by_code[code])
        trade_times = pd.to_datetime(frame["trade_time"])
        prices = pd.to_numeric(frame["price"], errors="coerce")
        if (
            trade_times.dt.strftime("%H:%M:%S").tolist() != list(CORE_GRID)
            or not prices.map(math.isfinite).all() or not (prices > 0).all()
        ):
            return {"status": "DATA_BLOCKED", "reason": "verified index minute grid or price differs"}
        late = prices[trade_times >= late_cutoff]
        late_return = (float(late.iloc[-1]) / float(late.iloc[0]) - 1.0) * 100.0
        session_return = (float(prices.iloc[-1]) / float(prices.iloc[0]) - 1.0) * 100.0
        items.append({
            "index_code": code,
            "session_return_pct": round(session_return, 4),
            "last_30m_return_pct": round(late_return, 4),
            "stable": session_return >= -0.5 and late_return >= -0.2,
            **_minute_provenance(proof, code),
        })
    stable_count = sum(item["stable"] for item in items)
    return {
        "status": "AVAILABLE",
        "source": "qmt_datadir",
        "known_at": proof["known_at"],
        "publication_manifest_sha256": proof["manifest_sha256"],
        "indices": items,
        "stable_count": stable_count,
        "stable": stable_count >= 2,
    }


def _blocked_stock_minutes(code: str, reason: str) -> dict[str, Any]:
    return {
        "stock_code": code,
        "minute_available": False,
        "intraday_event_json": json.dumps(
            {"status": "DATA_BLOCKED", "reason": reason}, ensure_ascii=False, sort_keys=True,
        ),
    }


def load_intraday_evidence(
    engine: Engine,
    *,
    trade_date: str,
    stock_codes: Sequence[str],
    decision_known_at: datetime | str | None,
    daily_pre_close: Mapping[str, Any],
) -> pd.DataFrame:
    """Read selected native minute values against one prior publication."""
    codes = sorted({str(code).zfill(6) for code in stock_codes})
    if not codes:
        return pd.DataFrame()
    if decision_known_at is None:
        return pd.DataFrame([_blocked_stock_minutes(code, "stock minute decision cutoff unavailable") for code in codes])

    anchors = {code: _finite(daily_pre_close.get(code), math.nan) for code in codes}
    requested = [code for code in codes if math.isfinite(anchors[code]) and anchors[code] > 0]
    proof, rows_by_code = {}, {}
    missing_reason = "stock minute publication unavailable at decision cutoff"
    if requested:
        try:
            with engine.connect() as connection:
                proof, rows_by_code = load_verified_minute_security_rows(
                    connection, connection, dataset="stock_minute", trade_date=trade_date,
                    decision_known_at=decision_known_at, codes=requested,
                )
        except (SQLAlchemyError, ValueError, TypeError, TimeoutError) as exc:
            missing_reason = f"stock minute proof unavailable ({type(exc).__name__})"
    summaries = []
    for code in codes:
        if code not in requested:
            summaries.append(_blocked_stock_minutes(code, "verified daily pre-close unavailable"))
            continue
        if code not in rows_by_code:
            reason = "verified stock minute rows missing or changed" if proof else missing_reason
            summaries.append(_blocked_stock_minutes(code, reason))
            continue
        frame = pd.DataFrame(rows_by_code[code])
        frame["pre_close"] = anchors[code]
        summary = summarize_stock_minutes(frame, stock_code=code, source="qmt_datadir")
        event = json.loads(summary["intraday_event_json"])
        event.update(_minute_provenance(proof, code))
        summary["intraday_event_json"] = json.dumps(event, ensure_ascii=False, sort_keys=True)
        summaries.append(summary)
    return pd.DataFrame(summaries)
