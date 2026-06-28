from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


def _safe_numeric(series: pd.Series | Iterable[object] | object) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    val = _safe_numeric(values)
    wgt = _safe_numeric(weights).fillna(0)
    mask = val.notna()
    if not mask.any():
        return None
    val = val[mask]
    wgt = wgt[mask]
    if (wgt > 0).any():
        total = float(wgt.sum())
        if total > 0:
            return float((val * wgt).sum() / total)
    return float(val.mean()) if not val.empty else None


def _weighted_percent_frame(frame: pd.DataFrame, column: str, weight_col: str = "amount") -> float | None:
    if column not in frame.columns:
        return None
    if weight_col in frame.columns:
        value = _weighted_mean(frame[column], frame[weight_col])
        if value is not None:
            return value
    if "volume" in frame.columns:
        value = _weighted_mean(frame[column], frame["volume"])
        if value is not None:
            return value
    return _weighted_mean(frame[column], pd.Series([1] * len(frame), index=frame.index))


def aggregate_concept_current(members: pd.DataFrame, stock_current: pd.DataFrame) -> pd.DataFrame:
    if members is None or members.empty or stock_current is None or stock_current.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"])

    current = stock_current.copy()
    current["stock_code"] = current["stock_code"].astype(str).str.zfill(6)
    merged = members[["concept_code", "stock_code"]].merge(current, on="stock_code", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"])

    pre_close = _safe_numeric(merged.get("pre_close"))
    merged["open_pct"] = (_safe_numeric(merged.get("open")) / pre_close.replace({0: np.nan}) - 1) * 100
    merged["high_pct"] = (_safe_numeric(merged.get("high")) / pre_close.replace({0: np.nan}) - 1) * 100
    merged["low_pct"] = (_safe_numeric(merged.get("low")) / pre_close.replace({0: np.nan}) - 1) * 100

    rows: list[dict[str, object]] = []
    for concept_code, frame in merged.groupby("concept_code", sort=False):
        close_pct = _weighted_percent_frame(frame, "change_pct")
        open_pct = _weighted_percent_frame(frame, "open_pct")
        high_pct = _weighted_percent_frame(frame, "high_pct")
        low_pct = _weighted_percent_frame(frame, "low_pct")
        price = 1000 * (1 + (close_pct or 0) / 100)
        open_price = 1000 * (1 + (open_pct or 0) / 100)
        high_price = 1000 * (1 + max(high_pct or close_pct or 0, close_pct or 0, open_pct or 0) / 100)
        low_price = 1000 * (1 + min(low_pct or close_pct or 0, close_pct or 0, open_pct or 0) / 100)
        snapshot_at = pd.to_datetime(frame.get("snapshot_at"), errors="coerce").max()
        if pd.isna(snapshot_at):
            snapshot_at = pd.Timestamp.now().floor("s")
        rows.append(
            {
                "index_code": concept_code,
                "trade_time": snapshot_at.to_pydatetime(),
                "trade_date": snapshot_at.strftime("%Y-%m-%d"),
                "open": open_price,
                "price": price,
                "high": high_price,
                "low": low_price,
                "volume": _safe_numeric(frame.get("volume")).sum(min_count=1),
                "amount": _safe_numeric(frame.get("amount")).sum(min_count=1),
                "change": price - 1000,
                "change_pct": close_pct,
                "snapshot_at": snapshot_at.to_pydatetime(),
            }
        )
    return pd.DataFrame(rows)


def aggregate_concept_minute(members: pd.DataFrame, stock_minute: pd.DataFrame, *, snapshot_at: pd.Timestamp | None = None) -> pd.DataFrame:
    if members is None or members.empty or stock_minute is None or stock_minute.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"])

    minute = stock_minute.copy()
    minute["stock_code"] = minute["stock_code"].astype(str).str.zfill(6)
    minute["trade_time"] = pd.to_datetime(minute["trade_time"], errors="coerce")
    merged = members[["concept_code", "stock_code"]].merge(minute, on="stock_code", how="inner")
    merged = merged[merged["trade_time"].notna()]
    if merged.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"])

    snap = snapshot_at or pd.Timestamp.now().floor("s")
    rows: list[dict[str, object]] = []
    for (concept_code, trade_time), frame in merged.groupby(["concept_code", "trade_time"], sort=False):
        close_pct = _weighted_percent_frame(frame, "change_pct")
        price = 1000 * (1 + (close_pct or 0) / 100)
        rows.append(
            {
                "index_code": concept_code,
                "trade_time": trade_time.to_pydatetime(),
                "trade_date": trade_time.strftime("%Y-%m-%d"),
                "price": price,
                "avg_price": price,
                "change": price - 1000,
                "change_pct": close_pct,
                "volume": _safe_numeric(frame.get("volume")).sum(min_count=1),
                "amount": _safe_numeric(frame.get("amount")).sum(min_count=1),
                "snapshot_at": snap.to_pydatetime(),
            }
        )
    return pd.DataFrame(rows)


def aggregate_concept_kline(members: pd.DataFrame, stock_kline: pd.DataFrame) -> pd.DataFrame:
    if members is None or members.empty or stock_kline is None or stock_kline.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"])

    kline = stock_kline.copy()
    kline["stock_code"] = kline["stock_code"].astype(str).str.zfill(6)
    kline["trade_date"] = pd.to_datetime(kline["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged = members[["concept_code", "stock_code"]].merge(kline, on="stock_code", how="inner")
    merged = merged[merged["trade_date"].notna()]
    if merged.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"])

    pre_close = _safe_numeric(merged.get("pre_close"))
    merged["open_rel"] = _safe_numeric(merged.get("open")) / pre_close.replace({0: np.nan}) - 1
    merged["close_rel"] = _safe_numeric(merged.get("close")) / pre_close.replace({0: np.nan}) - 1
    merged["high_rel"] = _safe_numeric(merged.get("high")) / pre_close.replace({0: np.nan}) - 1
    merged["low_rel"] = _safe_numeric(merged.get("low")) / pre_close.replace({0: np.nan}) - 1

    day_rows: list[dict[str, object]] = []
    for (concept_code, trade_date), frame in merged.groupby(["concept_code", "trade_date"], sort=False):
        day_rows.append(
            {
                "index_code": concept_code,
                "trade_date": trade_date,
                "open_rel": _weighted_mean(frame["open_rel"], frame.get("amount", frame.get("volume"))),
                "close_rel": _weighted_mean(frame["close_rel"], frame.get("amount", frame.get("volume"))),
                "high_rel": _weighted_mean(frame["high_rel"], frame.get("amount", frame.get("volume"))),
                "low_rel": _weighted_mean(frame["low_rel"], frame.get("amount", frame.get("volume"))),
                "volume": _safe_numeric(frame.get("volume")).sum(min_count=1),
                "amount": _safe_numeric(frame.get("amount")).sum(min_count=1),
            }
        )
    daily = pd.DataFrame(day_rows).sort_values(["index_code", "trade_date"]).reset_index(drop=True)
    if daily.empty:
        return pd.DataFrame(columns=["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"])

    rows: list[dict[str, object]] = []
    for concept_code, frame in daily.groupby("index_code", sort=False):
        prev_close_value = 1000.0
        for _, row in frame.iterrows():
            open_rel = float(row.get("open_rel") or 0)
            close_rel = float(row.get("close_rel") or 0)
            high_rel = float(row.get("high_rel") or close_rel)
            low_rel = float(row.get("low_rel") or close_rel)
            open_price = prev_close_value * (1 + open_rel)
            close_price = prev_close_value * (1 + close_rel)
            high_price = prev_close_value * (1 + max(high_rel, open_rel, close_rel))
            low_price = prev_close_value * (1 + min(low_rel, open_rel, close_rel))
            rows.append(
                {
                    "index_code": concept_code,
                    "trade_time": pd.Timestamp(f"{row['trade_date']} 15:00:00").to_pydatetime(),
                    "trade_date": row["trade_date"],
                    "k_type": 1,
                    "open": open_price,
                    "close": close_price,
                    "high": high_price,
                    "low": low_price,
                    "volume": row.get("volume"),
                    "amount": row.get("amount"),
                    "change": close_price - prev_close_value,
                    "change_pct": close_rel * 100,
                }
            )
            prev_close_value = close_price
    return pd.DataFrame(rows)
