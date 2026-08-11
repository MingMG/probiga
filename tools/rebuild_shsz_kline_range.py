#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rebuild a Shanghai/Shenzhen daily-K range from two independent providers.

Tonghuashun supplies licensed actual OHLCV/amount rows. Tencent actual history
verifies every OHLCV row, while Tonghuashun forward-adjusted closes derive the
official economic previous-close reference across ex-right/dividend events.

The command is read-only by default. ``--apply`` replaces only current-format
Shanghai/Shenzhen stock codes for the requested trade dates in both K-line
databases after every quality gate passes.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline  # noqa: E402
from biz.stock_market.stock_kline_akshare import (  # noqa: E402
    akshare_daily_to_sm_kline,
    em_code_to_sina_symbol,
)
from server.common.batch_db import create_batch_engine, write_frame  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.fetch_sm_stock_kline_daily import (  # noqa: E402
    _build_source_trace,
    _dataset_hash,
    _distinct_kline_url,
    _ensure_provenance_tables,
    _fetch_ths_history,
    _rows_match,
    _ths_auth_headers,
    _validate_daily_frame,
)


BUSINESS_COLUMNS = [
    "stock_code", "short_name", "trade_time", "trade_date", "k_type",
    "adjust_type", "open", "close", "high", "low", "volume", "amount",
    "change", "change_pct", "turnover_ratio", "pre_close", "etl_sync_at",
]
_SHSZ_PATTERN_SQL = "^(00|30|60|68)[0-9]{4}$"
_TENCENT_PLAIN_API = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
_TENCENT_ADJUSTED_API = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


@dataclass
class CodeOutcome:
    code: str
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    matched_rows: int = 0
    no_data_dates: list[str] = field(default_factory=list)
    ths_only_dates: list[str] = field(default_factory=list)
    tencent_only_dates: list[str] = field(default_factory=list)
    mismatch_rows: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


def _tencent_symbol(code: str) -> str:
    normalized = str(code or "").strip().zfill(6)
    return f"sh{normalized}" if normalized.startswith("6") else f"sz{normalized}"


def _parse_tencent_history(
    payload: dict[str, Any],
    symbol: str,
    *,
    adjusted: bool = False,
) -> pd.DataFrame:
    if int(payload.get("code") or 0) != 0:
        return pd.DataFrame()
    node = (payload.get("data") or {}).get(symbol) or {}
    rows = (
        node.get("qfqday")
        if adjusted
        else node.get("day")
    ) or []
    records: list[dict[str, Any]] = []
    for values in rows:
        if not isinstance(values, list) or len(values) < 5:
            continue
        try:
            row = {
                "trade_date": str(values[0])[:10],
                "open": float(values[1]),
                "close": float(values[2]),
                "high": float(values[3]),
                "low": float(values[4]),
            }
            if len(values) > 5:
                reported_volume = float(values[5])
                # Tencent's endpoint is inconsistent across boards: Shanghai
                # STAR Market rows are already shares, while main-board and
                # Shenzhen rows are hands. Normalize all rows to shares.
                row["volume"] = round(
                    reported_volume
                    if symbol.startswith("sh68")
                    else reported_volume * 100.0,
                    6,
                )
        except (TypeError, ValueError):
            continue
        records.append(row)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).drop_duplicates(
        subset=["trade_date"],
        keep="last",
    ).sort_values("trade_date")


def _fetch_tencent_history(
    code: str,
    history_start: str,
    end_date: str,
    *,
    adjusted: bool,
) -> pd.DataFrame:
    symbol = _tencent_symbol(code)
    endpoint = _TENCENT_ADJUSTED_API if adjusted else _TENCENT_PLAIN_API
    suffix = ",qfq" if adjusted else ""
    response = requests.get(
        endpoint,
        params={
            "param": (
                f"{symbol},day,{history_start},{end_date},120{suffix}"
            ),
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=25,
    )
    response.raise_for_status()
    return _parse_tencent_history(
        response.json(),
        symbol,
        adjusted=adjusted,
    )


def _fetch_sina_history(
    code: str,
    short_name: str,
    history_start: str,
    end_date: str,
) -> pd.DataFrame:
    symbol = em_code_to_sina_symbol(code)
    if not symbol:
        return pd.DataFrame()
    raw = fetch_sina_a_daily_kline(
        symbol,
        history_start.replace("-", ""),
        end_date.replace("-", ""),
        "",
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    frame = akshare_daily_to_sm_kline(
        raw,
        code,
        1,
        0,
        short_name=short_name,
    )
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    return frame.sort_values("trade_date").drop_duplicates(
        subset=["trade_date"],
        keep="last",
    )


def _records_by_date(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    return {
        str(row["trade_date"])[:10]: row
        for row in frame.to_dict(orient="records")
    }


def _derive_pre_close(
    raw_close: float,
    target_date: str,
    adjusted_rows: list[dict[str, Any]],
) -> float | None:
    ordered = sorted(
        (
            row for row in adjusted_rows
            if str(row.get("trade_date") or "")[:10] <= target_date
        ),
        key=lambda row: str(row.get("trade_date") or "")[:10],
    )
    if len(ordered) < 2:
        return None
    current, previous = ordered[-1], ordered[-2]
    if str(current.get("trade_date") or "")[:10] != target_date:
        return None
    current_close = float(current.get("close") or 0)
    previous_close = float(previous.get("close") or 0)
    if raw_close <= 0 or current_close <= 0 or previous_close <= 0:
        return None
    return raw_close / (current_close / previous_close)


def _fetch_code_once(
    code: str,
    short_name: str,
    target_dates: list[str],
    history_start: str,
    end_date: str,
    outstanding_shares: float | None = None,
) -> CodeOutcome:
    ths_actual = _fetch_ths_history(
        code,
        count=120,
        adjust_type="actual",
    )
    tencent = _fetch_tencent_history(
        code,
        history_start,
        end_date,
        adjusted=False,
    )
    adjusted = _fetch_ths_history(
        code,
        count=120,
        adjust_type="forward",
    )
    ths_by_date = _records_by_date(ths_actual)
    tencent_by_date = _records_by_date(tencent)
    fallback_needed = any(
        (target_date in ths_by_date) != (target_date in tencent_by_date)
        for target_date in target_dates
    )
    sina_by_date: dict[str, dict[str, Any]] = {}
    tencent_adjusted = pd.DataFrame()
    if fallback_needed:
        sina_by_date = _records_by_date(_fetch_sina_history(
            code,
            short_name,
            history_start,
            end_date,
        ))
        tencent_adjusted = _fetch_tencent_history(
            code,
            history_start,
            end_date,
            adjusted=True,
        )
    ths_adjusted_rows = adjusted.to_dict(orient="records")
    tencent_adjusted_rows = tencent_adjusted.to_dict(orient="records")
    outcome = CodeOutcome(code=code)
    rows: list[dict[str, Any]] = []
    for target_date in target_dates:
        ths_row = ths_by_date.get(target_date)
        tencent_row = tencent_by_date.get(target_date)
        sina_row = sina_by_date.get(target_date)
        primary_source = "ths"
        reference_source = "tencent"
        primary = ths_row
        reference = tencent_row
        if primary is None and reference is not None and sina_row is not None:
            primary_source = "sina"
            primary = sina_row
        elif primary is not None and reference is None and sina_row is not None:
            reference_source = "sina"
            reference = sina_row

        if primary is None and reference is None:
            outcome.no_data_dates.append(target_date)
            continue
        if primary is not None and reference is None:
            outcome.ths_only_dates.append(target_date)
            continue
        if primary is None and reference is not None:
            outcome.tencent_only_dates.append(target_date)
            continue
        assert primary is not None and reference is not None
        primary_values = {
            column: float(primary[column])
            for column in ("open", "high", "low", "close")
        }
        reference_values = {
            column: float(reference[column])
            for column in ("open", "high", "low", "close")
        }
        matched, differences = _rows_match(
            primary_values,
            reference_values,
            price_tolerance=0.011,
        )
        primary_volume = float(primary.get("volume") or 0)
        reference_volume = float(reference.get("volume") or 0)
        volume_delta = abs(primary_volume - reference_volume)
        differences["volume"] = round(volume_delta, 6)
        if volume_delta > max(100.0, abs(reference_volume) * 0.0001):
            matched = False
        if not matched:
            outcome.mismatch_rows.append({
                "stock_code": code,
                "trade_date": target_date,
                "differences": differences,
                "ths": {
                    **primary_values,
                    "volume": primary_volume,
                },
                "tencent": {
                    **reference_values,
                    "volume": reference_volume,
                },
            })
            continue

        volume = float(primary.get("volume") or 0)
        row = {
            "stock_code": code,
            "short_name": short_name,
            "trade_time": f"{target_date} 15:00:00",
            "trade_date": target_date,
            "k_type": 1,
            "adjust_type": 0,
            "open": float(primary["open"]),
            "close": float(primary["close"]),
            "high": float(primary["high"]),
            "low": float(primary["low"]),
            "volume": volume,
            "amount": float(primary.get("amount") or 0),
            "turnover_ratio": (
                volume / outstanding_shares
                if outstanding_shares and outstanding_shares > 0
                else None
            ),
        }
        close = float(row.get("close") or 0)
        pre_close = _derive_pre_close(
            close,
            target_date,
            ths_adjusted_rows,
        )
        if pre_close is None and tencent_adjusted_rows:
            pre_close = _derive_pre_close(
                close,
                target_date,
                tencent_adjusted_rows,
            )
        if pre_close is None:
            existing = float(row.get("pre_close") or 0)
            pre_close = existing if existing > 0 else None
        row["pre_close"] = pre_close
        row["change"] = close - pre_close if pre_close and pre_close > 0 else None
        row["change_pct"] = (
            (close / pre_close - 1.0) * 100.0
            if pre_close and pre_close > 0
            else None
        )
        row["_data_source"] = primary_source
        row["_reference_source"] = reference_source
        rows.append(row)
        outcome.matched_rows += 1
    outcome.frame = pd.DataFrame(rows)
    return outcome


def _fetch_code(
    code: str,
    short_name: str,
    target_dates: list[str],
    history_start: str,
    end_date: str,
    outstanding_shares: float | None = None,
) -> CodeOutcome:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return _fetch_code_once(
                code,
                short_name,
                target_dates,
                history_start,
                end_date,
                outstanding_shares,
            )
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (2 ** attempt) + random.uniform(0, 0.4))
    assert last_error is not None
    return CodeOutcome(
        code=code,
        error=f"{type(last_error).__name__}: {str(last_error)[:300]}",
    )


def _load_scope(
    engine: Engine,
    start_date: str,
    end_date: str,
) -> tuple[
    list[str],
    list[str],
    dict[str, str],
    dict[str, int],
    dict[str, float],
]:
    with engine.connect() as conn:
        dates = [
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
        rows = conn.execute(text(f"""
            SELECT DISTINCT k.stock_code, a.short_name
            FROM sm_stock_kline k
            INNER JOIN si_all_code a ON a.stock_code = k.stock_code
            WHERE k.trade_date BETWEEN :start_date AND :end_date
              AND k.k_type = 1
              AND k.adjust_type = 0
              AND k.stock_code REGEXP '{_SHSZ_PATTERN_SQL}'
            ORDER BY k.stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
        existing = conn.execute(text(f"""
            SELECT trade_date, COUNT(DISTINCT stock_code)
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '{_SHSZ_PATTERN_SQL}'
            GROUP BY trade_date
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
        share_rows = conn.execute(text(f"""
            SELECT
              stock_code,
              AVG(volume / turnover_ratio) AS implied_outstanding_shares
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '{_SHSZ_PATTERN_SQL}'
              AND volume > 0
              AND turnover_ratio > 0
            GROUP BY stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
    codes = [str(row[0]).zfill(6) for row in rows]
    names = {str(row[0]).zfill(6): str(row[1] or "") for row in rows}
    existing_by_date = {
        str(row[0])[:10]: int(row[1])
        for row in existing
    }
    outstanding_by_code = {
        str(row[0]).zfill(6): float(row[1])
        for row in share_rows
        if row[1] is not None and float(row[1]) > 0
    }
    return dates, codes, names, existing_by_date, outstanding_by_code


def _build_by_date(
    outcomes: list[CodeOutcome],
    target_dates: list[str],
) -> dict[str, pd.DataFrame]:
    parts = [item.frame for item in outcomes if not item.frame.empty]
    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    result: dict[str, pd.DataFrame] = {}
    for target_date in target_dates:
        frame = combined[
            combined["trade_date"].astype(str).str[:10] == target_date
        ].copy() if not combined.empty else pd.DataFrame()
        result[target_date] = (
            _validate_daily_frame(frame, target_date)
            if not frame.empty
            else frame
        )
    return result


def _provenance(
    by_date: dict[str, pd.DataFrame],
    existing_by_date: dict[str, int],
    started_at: datetime,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    finished_at = datetime.now().replace(microsecond=0)
    run_records: list[dict[str, Any]] = []
    traces: list[pd.DataFrame] = []
    for target_date, frame in by_date.items():
        run_id = str(uuid.uuid4())
        verified_codes = {
            str(row["stock_code"]).zfill(6): str(row["_reference_source"])
            for row in frame.to_dict(orient="records")
        }
        traces.append(_build_source_trace(
            frame,
            run_id=run_id,
            verified_codes=verified_codes,
            fetched_at=finished_at,
        ))
        expected = existing_by_date.get(target_date, len(frame))
        run_records.append({
            "run_id": run_id,
            "target_date": target_date,
            "mode": "shsz_dual_source_range_rebuild",
            "source_chain": "ths_actual,tencent_actual,ths_forward",
            "universe_source": "existing_range_intersect_si_all_code",
            "expected_count": expected,
            "fetched_count": len(frame),
            "coverage": round(len(frame) / max(expected, 1), 6),
            "source_counts_json": json.dumps(
                {
                    str(source): int(count)
                    for source, count in frame["_data_source"].value_counts().items()
                },
                sort_keys=True,
            ),
            "cross_validation_json": json.dumps({
                "status": "pass",
                "primary_source": "ths_or_sina",
                "reference_source": "tencent_or_sina",
                "compared": len(frame),
                "matched": len(frame),
                "mismatched": 0,
                "unavailable": 0,
            }, sort_keys=True),
            "dataset_sha256": _dataset_hash(frame),
            "status": "written",
            "started_at": started_at,
            "finished_at": finished_at,
        })
    return run_records, pd.concat(traces, ignore_index=True)


def _write_range(
    engine: Engine,
    by_date: dict[str, pd.DataFrame],
    existing_by_date: dict[str, int],
    *,
    started_at: datetime,
    provenance: bool,
) -> int:
    combined = pd.concat(list(by_date.values()), ignore_index=True)
    combined = combined.replace({np.nan: None, pd.NaT: None})
    combined["etl_sync_at"] = datetime.now().replace(microsecond=0)
    run_records: list[dict[str, Any]] = []
    traces = pd.DataFrame()
    if provenance:
        _ensure_provenance_tables(engine)
        run_records, traces = _provenance(
            by_date,
            existing_by_date,
            started_at,
        )
    with engine.begin() as conn:
        for target_date in by_date:
            conn.execute(text(f"""
                DELETE FROM sm_stock_kline
                WHERE trade_date = :target_date
                  AND k_type = 1
                  AND adjust_type = 0
                  AND stock_code REGEXP '{_SHSZ_PATTERN_SQL}'
            """), {"target_date": target_date})
        written = int(write_frame(
            combined[BUSINESS_COLUMNS],
            "sm_stock_kline",
            conn,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        ) or len(combined))
        if provenance:
            for target_date in by_date:
                conn.execute(text(f"""
                    DELETE FROM st_kline_source_trace
                    WHERE trade_date = :target_date
                      AND k_type = 1
                      AND adjust_type = 0
                      AND stock_code REGEXP '{_SHSZ_PATTERN_SQL}'
                """), {"target_date": target_date})
            write_frame(
                traces,
                "st_kline_source_trace",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            conn.execute(text("""
                INSERT INTO st_kline_ingestion_run (
                  run_id, target_date, mode, source_chain, universe_source,
                  expected_count, fetched_count, coverage, source_counts_json,
                  cross_validation_json, dataset_sha256, status, started_at,
                  finished_at
                ) VALUES (
                  :run_id, :target_date, :mode, :source_chain, :universe_source,
                  :expected_count, :fetched_count, :coverage, :source_counts_json,
                  :cross_validation_json, :dataset_sha256, :status, :started_at,
                  :finished_at
                )
            """), run_records)
    return written


def rebuild(
    start_date: str,
    end_date: str,
    *,
    min_coverage: float = 0.99,
    workers: int = 12,
    apply: bool = False,
) -> int:
    started_at = datetime.now().replace(microsecond=0)
    engine = create_batch_engine()
    (
        target_dates,
        codes,
        names,
        existing_by_date,
        outstanding_by_code,
    ) = _load_scope(
        engine,
        start_date,
        end_date,
    )
    if not target_dates or not codes:
        print("No Shanghai/Shenzhen K-line scope found")
        return 2
    history_start = (
        datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=45)
    ).strftime("%Y-%m-%d")
    _ths_auth_headers()
    print(
        f"SH/SZ dual-source rebuild: {start_date}..{end_date}, "
        f"dates={len(target_dates)}, codes={len(codes)}, "
        f"workers={workers}, apply={apply}"
    )
    outcomes: list[CodeOutcome] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 20))) as pool:
        futures = {
            pool.submit(
                _fetch_code,
                code,
                names.get(code, ""),
                target_dates,
                history_start,
                end_date,
                outstanding_by_code.get(code),
            ): code
            for code in codes
        }
        for done, future in enumerate(as_completed(futures), start=1):
            outcome = future.result()
            outcomes.append(outcome)
            if outcome.error:
                print(f"  {outcome.code} error: {outcome.error}")
            if done % 250 == 0 or done == len(futures):
                print(f"  progress {done}/{len(futures)}", flush=True)

    errors = [item for item in outcomes if item.error]
    ths_only = [
        (item.code, value)
        for item in outcomes
        for value in item.ths_only_dates
    ]
    tencent_only = [
        (item.code, value)
        for item in outcomes
        for value in item.tencent_only_dates
    ]
    mismatches = [
        value
        for item in outcomes
        for value in item.mismatch_rows
    ]
    by_date = _build_by_date(outcomes, target_dates)
    coverage_by_date = {
        target_date: (
            len(by_date[target_date])
            / max(
                sum(
                    target_date not in item.no_data_dates
                    for item in outcomes
                ),
                1,
            )
        )
        for target_date in target_dates
    }
    source_expected_by_date = {
        target_date: sum(
            target_date not in item.no_data_dates
            for item in outcomes
        )
        for target_date in target_dates
    }
    no_data_existing_samples = [
        (item.code, target_date)
        for item in outcomes
        for target_date in item.no_data_dates
    ][:30]
    missing_pre_close = sum(
        int((frame["pre_close"].isna() | frame["pre_close"].le(0)).sum())
        for frame in by_date.values()
        if not frame.empty
    )
    summary = {
        "status": "pass",
        "codes": len(codes),
        "target_dates": target_dates,
        "existing_by_date": existing_by_date,
        "source_expected_by_date": source_expected_by_date,
        "rebuilt_by_date": {
            key: len(value)
            for key, value in by_date.items()
        },
        "coverage_by_date": {
            key: round(value, 6)
            for key, value in coverage_by_date.items()
        },
        "worker_errors": len(errors),
        "ths_only_rows": len(ths_only),
        "tencent_only_rows": len(tencent_only),
        "mismatched_rows": len(mismatches),
        "missing_pre_close_rows": missing_pre_close,
        "both_sources_no_data_samples": no_data_existing_samples,
        "error_samples": [
            {"stock_code": item.code, "error": item.error}
            for item in errors[:10]
        ],
        "ths_only_samples": ths_only[:20],
        "tencent_only_samples": tencent_only[:20],
        "mismatch_samples": mismatches[:10],
    }
    blocked = (
        bool(errors)
        or bool(ths_only)
        or bool(tencent_only)
        or bool(mismatches)
        or any(value < min_coverage for value in coverage_by_date.values())
    )
    if blocked:
        summary["status"] = "fail"
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        print("SH/SZ rebuild blocked; existing databases retained")
        return 3
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    combined = pd.concat(list(by_date.values()), ignore_index=True)
    print(f"dataset_sha256={_dataset_hash(combined)}")
    if not apply:
        print("[dry-run] all gates passed; databases unchanged")
        return 0

    primary_written = _write_range(
        engine,
        by_date,
        existing_by_date,
        started_at=started_at,
        provenance=True,
    )
    mirror_url = _distinct_kline_url(str(engine.url))
    mirror_written = 0
    if mirror_url:
        mirror_written = _write_range(
            create_batch_engine(mirror_url),
            by_date,
            existing_by_date,
            started_at=started_at,
            provenance=False,
        )
    print(
        f"SH/SZ rebuild completed: primary_rows={primary_written}, "
        f"mirror_rows={mirror_written}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-coverage", type=float, default=0.99)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    engine = create_batch_engine()
    with mysql_named_lock(
        engine,
        "probiga:stock_kline_daily",
        timeout_seconds=int(
            os.environ.get("SHSZ_KLINE_REBUILD_LOCK_TIMEOUT", "60")
        ),
    ):
        return rebuild(
            args.start_date,
            args.end_date,
            min_coverage=max(0.0, min(1.0, args.min_coverage)),
            workers=max(1, args.workers),
            apply=args.apply,
        )


if __name__ == "__main__":
    raise SystemExit(main())
