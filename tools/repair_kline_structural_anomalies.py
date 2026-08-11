#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Repair structurally impossible daily bars only after two-source agreement.

The command is read-only by default. Pass ``--apply`` to update exact business
keys and persist before/after/source evidence in ``st_kline_repair_audit``.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.config import get_kline_mysql_url
from tools.fetch_sm_stock_kline_daily import (
    _fetch_builtin_one,
    _fetch_tencent_reference,
    _normalize_source_name,
    _rows_match,
    _validate_daily_frame,
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _read_bad_rows(engine: Engine, start_date: str, end_date: str) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
              `open`, `high`, `low`, `close`, volume, amount, `change`, change_pct,
              turnover_ratio, pre_close, etl_sync_at
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND (
                `high` < `low` OR `high` < `open` OR `high` < `close`
                OR `low` > `open` OR `low` > `close`
              )
            ORDER BY trade_date, stock_code
        """), {"start_date": start_date, "end_date": end_date}).mappings().all()
    return [dict(row) for row in rows]


def _replacement_from_sina(row: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    code = str(row["stock_code"]).zfill(6)
    target_date = str(row["trade_date"])[:10]
    name = str(row.get("short_name") or "")
    evidence: dict[str, Any] = {
        "stock_code": code,
        "trade_date": target_date,
        "primary_source": "sina",
        "reference_source": "tencent",
    }
    try:
        sina_frame = _fetch_builtin_one(code, target_date, name, "sina")
        tencent = _fetch_tencent_reference(code, target_date)
    except Exception as exc:  # pylint: disable=broad-except
        evidence["status"] = "source_error"
        evidence["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        return None, evidence
    if sina_frame is None or sina_frame.empty or tencent is None:
        evidence["status"] = "source_unavailable"
        return None, evidence

    source_row = sina_frame.iloc[-1].to_dict()
    primary = {
        column: float(source_row[column])
        for column in ("open", "high", "low", "close")
    }
    matched, differences = _rows_match(primary, tencent)
    evidence["sina"] = primary
    evidence["tencent"] = tencent
    evidence["differences"] = differences
    evidence["status"] = "agreed" if matched else "conflict"
    if not matched:
        return None, evidence

    replacement = dict(row)
    for column in (
        "open", "high", "low", "close", "volume", "amount",
        "change", "change_pct", "turnover_ratio", "pre_close",
    ):
        value = source_row.get(column)
        replacement[column] = None if pd.isna(value) else value
    replacement["stock_code"] = code
    replacement["trade_date"] = target_date
    replacement["etl_sync_at"] = datetime.now().replace(microsecond=0)
    checked = _validate_daily_frame(pd.DataFrame([replacement]), target_date).iloc[0].to_dict()
    return checked, evidence


def _ensure_audit_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_kline_repair_audit (
              repair_id VARCHAR(36) NOT NULL,
              stock_code VARCHAR(10) NOT NULL,
              trade_date DATE NOT NULL,
              k_type INT NOT NULL,
              adjust_type INT NOT NULL,
              reason VARCHAR(128) NOT NULL,
              before_json LONGTEXT NOT NULL,
              after_json LONGTEXT NOT NULL,
              evidence_json LONGTEXT NOT NULL,
              repaired_at DATETIME NOT NULL,
              PRIMARY KEY (repair_id),
              KEY idx_kline_repair_key (trade_date, stock_code, k_type, adjust_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


def _apply_one(
    engine: Engine,
    before: dict[str, Any],
    after: dict[str, Any],
    evidence: dict[str, Any],
    *,
    write_audit: bool,
) -> None:
    params = {
        **after,
        "repair_id": str(uuid.uuid4()),
        "reason": "structurally_impossible_ohlc_confirmed_by_sina_and_tencent",
        "before_json": _json(before),
        "after_json": _json(after),
        "evidence_json": _json(evidence),
        "repaired_at": datetime.now().replace(microsecond=0),
    }
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE sm_stock_kline
            SET
              `open` = :open,
              `high` = :high,
              `low` = :low,
              `close` = :close,
              volume = :volume,
              amount = :amount,
              `change` = :change,
              change_pct = :change_pct,
              turnover_ratio = :turnover_ratio,
              pre_close = :pre_close,
              etl_sync_at = :etl_sync_at
            WHERE stock_code = :stock_code
              AND trade_date = :trade_date
              AND k_type = :k_type
              AND adjust_type = :adjust_type
        """), params)
        if result.rowcount != 1:
            raise RuntimeError(
                "repair expected exactly one row for "
                f"{params['stock_code']} {params['trade_date']}, got {result.rowcount}"
            )
        if write_audit:
            conn.execute(text("""
                INSERT INTO st_kline_repair_audit (
                  repair_id, stock_code, trade_date, k_type, adjust_type,
                  reason, before_json, after_json, evidence_json, repaired_at
                ) VALUES (
                  :repair_id, :stock_code, :trade_date, :k_type, :adjust_type,
                  :reason, :before_json, :after_json, :evidence_json, :repaired_at
                )
            """), params)


def _mirror_engine(primary: Engine) -> Engine | None:
    try:
        url = get_kline_mysql_url().strip()
    except Exception:
        return None
    if not url or url == str(primary.url):
        return None
    return create_batch_engine(url)


def repair(start_date: str, end_date: str, *, apply: bool = False) -> int:
    engine = create_batch_engine()
    bad_rows = _read_bad_rows(engine, start_date, end_date)
    print(f"structural anomalies: {len(bad_rows)}")
    if not bad_rows:
        return 0

    approved: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []
    for row in bad_rows:
        replacement, evidence = _replacement_from_sina(row)
        print(
            row["stock_code"],
            str(row["trade_date"])[:10],
            evidence.get("status"),
            evidence.get("differences", {}),
        )
        if replacement is None:
            rejected.append(evidence)
        else:
            approved.append((row, replacement, evidence))

    if rejected:
        print(f"repair blocked: {len(rejected)} rows lack two-source agreement")
        print(_json(rejected))
        return 3
    if not apply:
        print(f"[dry-run] {len(approved)} rows approved; database unchanged")
        return 0

    _ensure_audit_table(engine)
    mirror = _mirror_engine(engine)
    for before, after, evidence in approved:
        if mirror is not None:
            _apply_one(mirror, before, after, evidence, write_audit=False)
        _apply_one(engine, before, after, evidence, write_audit=True)

    remaining = _read_bad_rows(engine, start_date, end_date)
    if remaining:
        raise RuntimeError(f"structural anomalies remain after repair: {remaining}")
    print(f"repair completed: {len(approved)} rows; remaining=0")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    return repair(args.start_date, args.end_date, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
