#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify that a staged capital-flow artifact was written without mutation.

The command is read-only. It compares business keys, source labels, every
numeric field, and a type-normalized dataset hash.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, read_frame_direct  # noqa: E402
from tools.rebuild_capital_flow_range_baidu import (  # noqa: E402
    _FIELDS,
    _dataset_hash,
    _read_staging,
)


_KEYS = ["stock_code", "trade_date"]


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[
        [*_KEYS, *_FIELDS, "data_source"]
    ].copy()
    result["stock_code"] = (
        result["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    result["trade_date"] = (
        result["trade_date"].astype(str).str[:10]
    )
    result["data_source"] = (
        result["data_source"].fillna("").astype(str)
    )
    for field in _FIELDS:
        result[field] = pd.to_numeric(
            result[field],
            errors="coerce",
        )
    return result.sort_values(_KEYS).reset_index(drop=True)


def verify(
    staging_path: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    staged = _normalize(_read_staging(staging_path))
    database = _normalize(read_frame_direct(text("""
        SELECT
          stock_code, trade_date, main_net_inflow, max_net_inflow,
          lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
        FROM sm_stock_capital_flow_daily
        WHERE trade_date BETWEEN :start_date AND :end_date
          AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
        ORDER BY trade_date, stock_code
    """), create_batch_engine(), params={
        "start_date": start_date,
        "end_date": end_date,
    }))
    staged = staged[
        staged["trade_date"].between(start_date, end_date)
    ].copy()
    staged_duplicate_keys = int(staged.duplicated(_KEYS).sum())
    database_duplicate_keys = int(database.duplicated(_KEYS).sum())
    merged = staged.merge(
        database,
        on=_KEYS,
        how="outer",
        suffixes=("_staged", "_database"),
        indicator=True,
    )
    staged_only = int((merged["_merge"] == "left_only").sum())
    database_only = int((merged["_merge"] == "right_only").sum())
    common = merged[merged["_merge"] == "both"].copy()
    different = pd.Series(False, index=common.index)
    max_absolute_differences: dict[str, float] = {}
    for field in _FIELDS:
        delta = (
            common[f"{field}_staged"]
            - common[f"{field}_database"]
        ).abs()
        max_absolute_differences[field] = (
            float(delta.max())
            if not delta.empty and pd.notna(delta.max())
            else 0.0
        )
        equal = (
            (
                common[f"{field}_staged"].isna()
                & common[f"{field}_database"].isna()
            )
            | delta.le(0.000001)
        )
        different |= ~equal
    source_equal = common["data_source_staged"].eq(
        common["data_source_database"],
    )
    different |= ~source_equal
    staged_hash = _dataset_hash(staged)
    database_hash = _dataset_hash(database)
    hard_failures = {
        "staged_duplicate_keys": staged_duplicate_keys,
        "database_duplicate_keys": database_duplicate_keys,
        "staged_only_keys": staged_only,
        "database_only_keys": database_only,
        "different_value_rows": int(different.sum()),
        "hash_mismatch": staged_hash != database_hash,
    }
    return {
        "status": (
            "pass"
            if not any(bool(value) for value in hard_failures.values())
            else "fail"
        ),
        "start_date": start_date,
        "end_date": end_date,
        "staging_path": str(Path(staging_path).resolve()),
        "staged_rows": len(staged),
        "database_rows": len(database),
        "staged_sha256": staged_hash,
        "database_sha256": database_hash,
        "max_absolute_differences": max_absolute_differences,
        "hard_failures": hard_failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging-path", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    report = verify(
        args.staging_path,
        args.start_date,
        args.end_date,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "pass" else 3


if __name__ == "__main__":
    raise SystemExit(main())
