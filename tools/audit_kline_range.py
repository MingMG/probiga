#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit a daily K-line range in the production primary and read databases.

The command is read-only. It checks calendar completeness, business-key and
OHLC integrity, official-reference return consistency, master-data membership,
provenance coverage, and exact primary/read-database equality.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, read_frame_direct  # noqa: E402
from tools.fetch_sm_stock_kline_daily import _distinct_kline_url  # noqa: E402


_CODE_PATTERN_SQL = "^(00|30|60|68|92)[0-9]{4}$"
_COLUMNS = [
    "stock_code", "short_name", "trade_date", "k_type", "adjust_type",
    "open", "high", "low", "close", "volume", "amount", "pre_close",
    "change", "change_pct", "turnover_ratio",
]
_NUMERIC_COLUMNS = [
    "open", "high", "low", "close", "volume", "amount", "pre_close",
    "change", "change_pct", "turnover_ratio",
]


def _load_rows(
    engine: Engine,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    frame = read_frame_direct(text(f"""
        SELECT {", ".join(f"`{column}`" for column in _COLUMNS)}
        FROM sm_stock_kline
        WHERE trade_date BETWEEN :start_date AND :end_date
          AND k_type = 1
          AND adjust_type = 0
          AND stock_code REGEXP '{_CODE_PATTERN_SQL}'
        ORDER BY trade_date, stock_code
    """), engine, params={
        "start_date": start_date,
        "end_date": end_date,
    })
    if frame.empty:
        return frame
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
    frame["short_name"] = frame["short_name"].fillna("").astype(str)
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    for column in _NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values(
        ["trade_date", "stock_code"],
        kind="stable",
    ).reset_index(drop=True)


def _dataset_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    canonical = frame[_COLUMNS].copy()
    for column in _NUMERIC_COLUMNS:
        canonical[column] = canonical[column].round(8)
    payload = canonical.to_json(
        orient="records",
        double_precision=8,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _frame_metrics(
    frame: pd.DataFrame,
    expected_dates: list[str],
    master_list_dates: dict[str, str],
) -> dict[str, Any]:
    actual_dates = (
        sorted(frame["trade_date"].unique().tolist())
        if not frame.empty else []
    )
    if frame.empty:
        return {
            "row_count": 0,
            "stock_count": 0,
            "actual_trade_dates": [],
            "missing_trade_dates": expected_dates,
            "rows_by_date": {},
            "duplicate_business_keys": 0,
            "bad_ohlc": 0,
            "invalid_prices": 0,
            "negative_volume_or_amount": 0,
            "zero_volume_rows": 0,
            "missing_pre_close_rows": 0,
            "missing_pre_close_listing_day_rows": 0,
            "missing_pre_close_non_listing_day_rows": 0,
            "inconsistent_reference_return_rows": 0,
            "unknown_master_codes": [],
            "dataset_sha256": "",
        }
    prices = frame[["open", "high", "low", "close"]]
    bad_ohlc = (
        frame["high"].lt(prices.max(axis=1))
        | frame["low"].gt(prices.min(axis=1))
    )
    invalid_prices = prices.isna().any(axis=1) | prices.le(0).any(axis=1)
    negative_activity = frame["volume"].lt(0) | frame["amount"].lt(0)
    reference_mask = (
        frame["pre_close"].notna()
        & frame["pre_close"].gt(0)
        & frame["close"].notna()
        & frame["change_pct"].notna()
    )
    reference_delta = (
        (
            frame.loc[reference_mask, "close"]
            / frame.loc[reference_mask, "pre_close"]
            - 1.0
        ) * 100.0
        - frame.loc[reference_mask, "change_pct"]
    ).abs()
    missing_pre_close = (
        frame["pre_close"].isna()
        | frame["pre_close"].le(0)
    )
    listing_day = pd.Series(
        [
            (
                master_list_dates.get(str(code), "")
                == str(trade_date)
            )
            for code, trade_date in zip(
                frame["stock_code"],
                frame["trade_date"],
            )
        ],
        index=frame.index,
    )
    return {
        "row_count": len(frame),
        "stock_count": int(frame["stock_code"].nunique()),
        "actual_trade_dates": actual_dates,
        "missing_trade_dates": sorted(set(expected_dates) - set(actual_dates)),
        "rows_by_date": {
            str(key): int(value)
            for key, value in frame.groupby("trade_date").size().items()
        },
        "duplicate_business_keys": int(
            frame.duplicated(
                ["stock_code", "trade_date", "k_type", "adjust_type"],
            ).sum()
        ),
        "bad_ohlc": int(bad_ohlc.sum()),
        "invalid_prices": int(invalid_prices.sum()),
        "negative_volume_or_amount": int(negative_activity.sum()),
        "zero_volume_rows": int(frame["volume"].eq(0).sum()),
        "missing_pre_close_rows": int(missing_pre_close.sum()),
        "missing_pre_close_listing_day_rows": int(
            (missing_pre_close & listing_day).sum()
        ),
        "missing_pre_close_non_listing_day_rows": int(
            (missing_pre_close & ~listing_day).sum()
        ),
        "inconsistent_reference_return_rows": int(
            reference_delta.gt(0.05).sum()
        ),
        "unknown_master_codes": sorted(
            set(frame["stock_code"]) - set(master_list_dates)
        ),
        "dataset_sha256": _dataset_hash(frame),
    }


def _difference_count(primary: pd.DataFrame, mirror: pd.DataFrame) -> int:
    keys = ["stock_code", "trade_date", "k_type", "adjust_type"]
    merged = primary.merge(
        mirror,
        on=keys,
        how="inner",
        suffixes=("_primary", "_mirror"),
    )
    if merged.empty:
        return 0
    different = pd.Series(False, index=merged.index)
    for column in ["short_name", *_NUMERIC_COLUMNS]:
        left = merged[f"{column}_primary"]
        right = merged[f"{column}_mirror"]
        if column in _NUMERIC_COLUMNS:
            equal = (
                (left.isna() & right.isna())
                | (left.sub(right).abs().le(0.000001))
            )
        else:
            equal = left.fillna("").astype(str).eq(
                right.fillna("").astype(str)
            )
        different |= ~equal
    return int(different.sum())


def _provenance_metrics(
    engine: Engine,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    with engine.connect() as conn:
        trace = conn.execute(text(f"""
            SELECT
              COUNT(*) AS row_count,
              SUM(verification_status = 'cross_checked') AS cross_checked,
              COUNT(DISTINCT run_id) AS run_count
            FROM st_kline_source_trace
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '{_CODE_PATTERN_SQL}'
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).one()
        modes = conn.execute(text("""
            SELECT mode, COUNT(*) AS run_count,
                   MIN(coverage) AS min_coverage,
                   MAX(finished_at) AS latest_finished_at
            FROM st_kline_ingestion_run
            WHERE target_date BETWEEN :start_date AND :end_date
              AND mode IN (
                'shsz_dual_source_range_rebuild',
                'bse_dual_source_gap_repair'
              )
            GROUP BY mode
            ORDER BY mode
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
    return {
        "trace_rows": int(trace.row_count or 0),
        "cross_checked_rows": int(trace.cross_checked or 0),
        "run_count": int(trace.run_count or 0),
        "ingestion_modes": [
            {
                "mode": str(row.mode),
                "run_count": int(row.run_count or 0),
                "min_coverage": float(row.min_coverage or 0),
                "latest_finished_at": str(row.latest_finished_at),
            }
            for row in modes
        ],
    }


def audit_range(start_date: str, end_date: str) -> dict[str, Any]:
    primary_engine = create_batch_engine()
    mirror_url = _distinct_kline_url(str(primary_engine.url))
    if not mirror_url:
        return {
            "status": "fail",
            "error": "distinct K-line read database is not configured",
        }
    mirror_engine = create_batch_engine(mirror_url)
    with primary_engine.connect() as conn:
        expected_dates = [
            str(row[0])[:10]
            for row in conn.execute(text("""
                SELECT trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date BETWEEN :start_date AND :end_date
                ORDER BY trade_date
            """), {
                "start_date": start_date,
                "end_date": end_date,
            }).fetchall()
        ]
        master_list_dates = {
            str(row[0]).strip().zfill(6): str(row[1] or "")[:10]
            for row in conn.execute(text(f"""
                SELECT stock_code, list_date
                FROM si_all_code
                WHERE stock_code REGEXP '{_CODE_PATTERN_SQL}'
            """)).fetchall()
        }
    primary = _load_rows(primary_engine, start_date, end_date)
    mirror = _load_rows(mirror_engine, start_date, end_date)
    primary_metrics = _frame_metrics(
        primary,
        expected_dates,
        master_list_dates,
    )
    mirror_metrics = _frame_metrics(
        mirror,
        expected_dates,
        master_list_dates,
    )
    keys = ["stock_code", "trade_date", "k_type", "adjust_type"]
    primary_keys = set(map(tuple, primary[keys].itertuples(index=False, name=None)))
    mirror_keys = set(map(tuple, mirror[keys].itertuples(index=False, name=None)))
    equality = {
        "primary_only_keys": len(primary_keys - mirror_keys),
        "mirror_only_keys": len(mirror_keys - primary_keys),
        "different_value_rows": _difference_count(primary, mirror),
        "hash_equal": (
            primary_metrics["dataset_sha256"]
            == mirror_metrics["dataset_sha256"]
        ),
    }
    provenance = _provenance_metrics(
        primary_engine,
        start_date,
        end_date,
    )
    hard_failures = {
        "primary_missing_trade_dates": primary_metrics["missing_trade_dates"],
        "mirror_missing_trade_dates": mirror_metrics["missing_trade_dates"],
        "primary_duplicate_business_keys": primary_metrics["duplicate_business_keys"],
        "mirror_duplicate_business_keys": mirror_metrics["duplicate_business_keys"],
        "primary_bad_ohlc": primary_metrics["bad_ohlc"],
        "mirror_bad_ohlc": mirror_metrics["bad_ohlc"],
        "primary_invalid_prices": primary_metrics["invalid_prices"],
        "mirror_invalid_prices": mirror_metrics["invalid_prices"],
        "primary_negative_volume_or_amount": primary_metrics["negative_volume_or_amount"],
        "mirror_negative_volume_or_amount": mirror_metrics["negative_volume_or_amount"],
        "primary_inconsistent_reference_returns": primary_metrics[
            "inconsistent_reference_return_rows"
        ],
        "mirror_inconsistent_reference_returns": mirror_metrics[
            "inconsistent_reference_return_rows"
        ],
        "primary_missing_pre_close_non_listing_day_rows": primary_metrics[
            "missing_pre_close_non_listing_day_rows"
        ],
        "mirror_missing_pre_close_non_listing_day_rows": mirror_metrics[
            "missing_pre_close_non_listing_day_rows"
        ],
        "primary_unknown_master_codes": primary_metrics["unknown_master_codes"],
        "mirror_unknown_master_codes": mirror_metrics["unknown_master_codes"],
        **equality,
        "provenance_not_cross_checked": (
            provenance["trace_rows"] - provenance["cross_checked_rows"]
        ),
        "provenance_row_gap": (
            primary_metrics["row_count"] - provenance["trace_rows"]
        ),
    }
    blocked = any(
        value is False
        if key == "hash_equal"
        else bool(value)
        for key, value in hard_failures.items()
    )
    return {
        "status": "fail" if blocked else "pass",
        "start_date": start_date,
        "end_date": end_date,
        "expected_trade_dates": expected_dates,
        "primary": primary_metrics,
        "mirror": mirror_metrics,
        "equality": equality,
        "provenance": provenance,
        "hard_failures": hard_failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    report = audit_range(args.start_date, args.end_date)
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
        print(path)
    else:
        print(payload)
    return 0 if report.get("status") == "pass" else 3


if __name__ == "__main__":
    raise SystemExit(main())
