#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Cross-source verification for a K-line date range.

The verifier compares a deterministic per-session sample from the production
database with independent historical daily-bar endpoints: Tencent for
Shanghai/Shenzhen and Tonghuashun for Beijing. It records evidence only when
``--record`` is passed; the business K-line table is never modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline
from biz.stock_market.stock_kline_akshare import em_code_to_sina_symbol
from server.common.batch_db import create_batch_engine, read_frame_direct, write_frame
from server.common.kline_data import get_kline_engine
from tools.fetch_sm_stock_kline_daily import (
    _fetch_independent_reference,
    _rows_match,
    _ths_auth_headers,
)
from tools.backfill_bse_kline_gap import _fetch_bse_bulk_trades


def _fetch_sina_amount_reference(
    code: str,
    target_date: str,
) -> float | None:
    symbol = em_code_to_sina_symbol(code)
    if not symbol:
        return None
    api_date = target_date.replace("-", "")
    frame = fetch_sina_a_daily_kline(
        symbol,
        api_date,
        api_date,
        "",
    )
    if frame is None or frame.empty or "amount" not in frame.columns:
        return None
    rows = frame[
        frame["date"].astype(str).str[:10] == target_date
    ]
    if rows.empty:
        return None
    value = pd.to_numeric(rows.iloc[-1].get("amount"), errors="coerce")
    return float(value) if pd.notna(value) and float(value) >= 0 else None


def _expected_trade_dates(start_date: str, end_date: str) -> list[str]:
    with create_batch_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
        """), {"start_date": start_date, "end_date": end_date}).fetchall()
    return [str(row[0])[:10] for row in rows]


def _load_rows(start_date: str, end_date: str) -> pd.DataFrame:
    frame = read_frame_direct(text("""
            SELECT stock_code, trade_date, `open`, `high`, `low`, `close`, amount
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
            ORDER BY trade_date, stock_code
        """), get_kline_engine(), params={"start_date": start_date, "end_date": end_date})
    if frame.empty:
        return frame
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    for column in ("open", "high", "low", "close", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _dataset_hash(frame: pd.DataFrame) -> str:
    payload = frame[
        ["stock_code", "trade_date", "open", "high", "low", "close", "amount"]
    ].to_json(orient="records", double_precision=10)
    return hashlib.sha256(payload.encode()).hexdigest()


def _sample_rows(frame: pd.DataFrame, per_date: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for trade_date, group in frame.groupby("trade_date", sort=True):
        records = group.to_dict(orient="records")
        records.sort(
            key=lambda row: hashlib.sha256(
                f"{trade_date}:{row['stock_code']}".encode()
            ).hexdigest()
        )
        selected.extend(records[: min(per_date, len(records))])
    return selected


def _match_reference(
    primary: dict[str, float],
    reference: dict[str, float],
    *,
    bse_bulk_amount: float = 0.0,
) -> tuple[bool, dict[str, float], bool, dict[str, float]]:
    matched, differences = _rows_match(primary, reference)
    if matched or bse_bulk_amount <= 0:
        return matched, differences, False, reference
    adjusted = dict(reference)
    adjusted["amount"] = (
        float(adjusted.get("amount") or 0)
        + bse_bulk_amount
    )
    matched, differences = _rows_match(primary, adjusted)
    return matched, differences, matched, adjusted


def verify_range(
    start_date: str,
    end_date: str,
    *,
    sample_per_date: int = 20,
    workers: int = 8,
    record: bool = False,
) -> dict[str, Any]:
    expected_dates = _expected_trade_dates(start_date, end_date)
    frame = _load_rows(start_date, end_date)
    if frame.empty:
        return {
            "status": "fail",
            "error": "no K-line rows in requested date range",
            "start_date": start_date,
            "end_date": end_date,
        }
    selected = _sample_rows(frame, sample_per_date)
    if any(
        str(row["stock_code"]).startswith(("4", "8", "92"))
        for row in selected
    ):
        # Resolve the public quote-page credential once before worker fan-out.
        _ths_auth_headers()
    bse_bulk_trades = (
        _fetch_bse_bulk_trades(start_date, end_date)
        if any(
            str(row["stock_code"]).startswith("92")
            for row in selected
        )
        else {}
    )

    def _verify(row: dict[str, Any]) -> dict[str, Any]:
        code = str(row["stock_code"])
        trade_date = str(row["trade_date"])
        primary = {
            column: float(row[column])
            for column in ("open", "high", "low", "close", "amount")
        }
        reference_source = "ths" if code.startswith(("4", "8", "92")) else "tencent"
        try:
            reference_source, reference = _fetch_independent_reference(code, trade_date)
            if reference is None:
                return {
                    "trade_date": trade_date,
                    "stock_code": code,
                    "reference_source": reference_source,
                    "status": "unavailable",
                    "db_ohlc_json": json.dumps(primary, sort_keys=True),
                    "reference_ohlc_json": "{}",
                    "differences_json": "{}",
                }
            if reference_source == "tencent":
                sina_amount = _fetch_sina_amount_reference(
                    code,
                    trade_date,
                )
                if sina_amount is not None:
                    reference["amount"] = sina_amount
                    reference_source = "tencent+sina_amount"
            bulk_amount = float(
                bse_bulk_trades.get(
                    (code, trade_date),
                    {},
                ).get("amount") or 0
            )
            matched, differences, bulk_reconciled, compared_reference = _match_reference(
                primary,
                reference,
                bse_bulk_amount=bulk_amount,
            )
            if bulk_reconciled:
                reference_source = (
                    f"{reference_source}+bse_official_bulk"
                )
            return {
                "trade_date": trade_date,
                "stock_code": code,
                "reference_source": reference_source,
                "status": "matched" if matched else "mismatched",
                "db_ohlc_json": json.dumps(primary, sort_keys=True),
                "reference_ohlc_json": json.dumps(compared_reference, sort_keys=True),
                "differences_json": json.dumps(differences, sort_keys=True),
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "trade_date": trade_date,
                "stock_code": code,
                "reference_source": reference_source,
                "status": "unavailable",
                "db_ohlc_json": json.dumps(primary, sort_keys=True),
                "reference_ohlc_json": "{}",
                "differences_json": json.dumps({
                    "error": f"{type(exc).__name__}: {str(exc)[:240]}",
                }),
            }

    evidence: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 16))) as pool:
        futures = [pool.submit(_verify, row) for row in selected]
        for done, future in enumerate(
            as_completed(futures),
            start=1,
        ):
            evidence.append(future.result())
            if done % 250 == 0 or done == len(futures):
                print(
                    f"  verification progress {done}/{len(futures)}",
                    flush=True,
                )

    per_date: dict[str, dict[str, int]] = {}
    for trade_date in sorted(frame["trade_date"].unique().tolist()):
        rows = [row for row in evidence if row["trade_date"] == trade_date]
        counts = {
            "requested": len(rows),
            "matched": sum(row["status"] == "matched" for row in rows),
            "mismatched": sum(row["status"] == "mismatched" for row in rows),
            "unavailable": sum(row["status"] == "unavailable" for row in rows),
        }
        counts["compared"] = counts["matched"] + counts["mismatched"]
        per_date[trade_date] = counts

    matched = sum(row["status"] == "matched" for row in evidence)
    mismatched = sum(row["status"] == "mismatched" for row in evidence)
    unavailable = sum(row["status"] == "unavailable" for row in evidence)
    compared = matched + mismatched
    actual_dates = sorted(frame["trade_date"].unique().tolist())
    missing_trade_dates = sorted(set(expected_dates) - set(actual_dates))
    date_gate_failures = [
        trade_date
        for trade_date, counts in per_date.items()
        if counts["mismatched"] > 0
        or counts["compared"] < max(1, int(counts["requested"] * 0.60))
    ]
    status = (
        "pass"
        if mismatched == 0 and not date_gate_failures and not missing_trade_dates
        else "fail"
    )
    run_id = str(uuid.uuid4())
    checked_at = datetime.now().replace(microsecond=0)
    dataset_sha256 = _dataset_hash(frame)
    report = {
        "status": status,
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "trade_date_count": int(frame["trade_date"].nunique()),
        "expected_trade_date_count": len(expected_dates),
        "missing_trade_dates": missing_trade_dates,
        "dataset_rows": len(frame),
        "dataset_sha256": dataset_sha256,
        "sample_per_date": sample_per_date,
        "requested": len(selected),
        "compared": compared,
        "matched": matched,
        "mismatched": mismatched,
        "unavailable": unavailable,
        "date_gate_failures": date_gate_failures,
        "per_date": per_date,
        "mismatch_samples": [row for row in evidence if row["status"] == "mismatched"][:20],
        "unavailable_samples": [row for row in evidence if row["status"] == "unavailable"][:20],
        "checked_at": checked_at.isoformat(),
    }
    if record:
        _record_report(report, evidence, checked_at)
        report["recorded"] = True
    return report


def _record_report(
    report: dict[str, Any],
    evidence: list[dict[str, Any]],
    checked_at: datetime,
) -> None:
    engine = create_batch_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_kline_verification_run (
              run_id VARCHAR(36) NOT NULL,
              start_date DATE NOT NULL,
              end_date DATE NOT NULL,
              status VARCHAR(16) NOT NULL,
              dataset_rows INT NOT NULL,
              dataset_sha256 CHAR(64) NOT NULL,
              sample_per_date INT NOT NULL,
              requested INT NOT NULL,
              compared INT NOT NULL,
              matched INT NOT NULL,
              mismatched INT NOT NULL,
              unavailable INT NOT NULL,
              details_json LONGTEXT NOT NULL,
              checked_at DATETIME NOT NULL,
              PRIMARY KEY (run_id),
              KEY idx_kline_verify_range (start_date, end_date, status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_kline_verification_sample (
              run_id VARCHAR(36) NOT NULL,
              trade_date DATE NOT NULL,
              stock_code VARCHAR(10) NOT NULL,
              reference_source VARCHAR(32) NOT NULL,
              status VARCHAR(16) NOT NULL,
              db_ohlc_json LONGTEXT NOT NULL,
              reference_ohlc_json LONGTEXT NOT NULL,
              differences_json LONGTEXT NOT NULL,
              checked_at DATETIME NOT NULL,
              PRIMARY KEY (run_id, trade_date, stock_code),
              KEY idx_kline_verify_sample_status (trade_date, status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO st_kline_verification_run (
              run_id, start_date, end_date, status, dataset_rows, dataset_sha256,
              sample_per_date, requested, compared, matched, mismatched,
              unavailable, details_json, checked_at
            ) VALUES (
              :run_id, :start_date, :end_date, :status, :dataset_rows, :dataset_sha256,
              :sample_per_date, :requested, :compared, :matched, :mismatched,
              :unavailable, :details_json, :checked_at
            )
        """), {
            **report,
            "details_json": json.dumps(report, ensure_ascii=False, sort_keys=True, default=str),
            "checked_at": checked_at,
        })
        sample_frame = pd.DataFrame([
            {
                **row,
                "run_id": report["run_id"],
                "checked_at": checked_at,
            }
            for row in evidence
        ])
        write_frame(
            sample_frame[
                [
                    "run_id", "trade_date", "stock_code", "reference_source",
                    "status", "db_ohlc_json", "reference_ohlc_json",
                    "differences_json", "checked_at",
                ]
            ],
            "st_kline_verification_sample",
            conn,
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample-per-date", type=int, default=20)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args()
    report = verify_range(
        args.start_date,
        args.end_date,
        sample_per_date=max(1, args.sample_per_date),
        workers=max(1, args.workers),
        record=args.record,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report["status"] == "pass" else 3


if __name__ == "__main__":
    raise SystemExit(main())
