#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Remove explicitly named non-stock/pre-listing daily bars with an audit trail.

The command is read-only by default. It refuses to touch a code present in the
listed-security master and deletes only exact code/date/business-key matches.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.fetch_sm_stock_kline_daily import _distinct_kline_url  # noqa: E402
from tools.repair_kline_structural_anomalies import (  # noqa: E402
    _ensure_audit_table,
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_codes(raw_codes: list[str]) -> list[str]:
    values: list[str] = []
    for raw in raw_codes:
        for token in re.split(r"[\s,;]+", str(raw or "").strip()):
            if not token:
                continue
            code = token.zfill(6)
            if not re.fullmatch(r"\d{6}", code):
                raise ValueError(f"invalid security code: {token}")
            if code not in values:
                values.append(code)
    if not values:
        raise ValueError("at least one code is required")
    return values


def _load_rows(
    engine: Engine,
    start_date: str,
    end_date: str,
    codes: list[str],
) -> list[dict[str, Any]]:
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {
        "start_date": start_date,
        "end_date": end_date,
        **{f"code_{idx}": code for idx, code in enumerate(codes)},
    }
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              stock_code, short_name, trade_time, trade_date, k_type,
              adjust_type, `open`, `close`, `high`, `low`, volume, amount,
              `change`, change_pct, turnover_ratio, pre_close, etl_sync_at
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code IN ({placeholders})
            ORDER BY trade_date, stock_code
        """), params).mappings().all()
    return [dict(row) for row in rows]


def _listed_codes(engine: Engine, codes: list[str]) -> list[str]:
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {f"code_{idx}": code for idx, code in enumerate(codes)}
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT stock_code
            FROM si_all_code
            WHERE stock_code IN ({placeholders})
        """), params).fetchall()
    return sorted(str(row[0]).zfill(6) for row in rows)


def _delete_rows(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    write_audit: bool,
) -> int:
    if not rows:
        return 0
    if write_audit:
        _ensure_audit_table(engine)
    now = datetime.now().replace(microsecond=0)
    params = [
        {
            **row,
            "repair_id": str(uuid.uuid4()),
            "reason": "invalid_instrument_or_prelisting_placeholder",
            "before_json": _json(row),
            "after_json": _json({"deleted": True}),
            "evidence_json": _json({
                "master_status": "absent",
                "volume": row.get("volume"),
                "amount": row.get("amount"),
                "classification": (
                    "prelisting_placeholder"
                    if re.fullmatch(r"(?:00|30|60|68|92)\d{4}", str(row["stock_code"]))
                    else "non_stock_instrument"
                ),
            }),
            "repaired_at": now,
        }
        for row in rows
    ]
    with engine.begin() as conn:
        deleted = 0
        for item in params:
            result = conn.execute(text("""
                DELETE FROM sm_stock_kline
                WHERE stock_code = :stock_code
                  AND trade_date = :trade_date
                  AND k_type = :k_type
                  AND adjust_type = :adjust_type
            """), item)
            if result.rowcount != 1:
                raise RuntimeError(
                    "expected one exact row for "
                    f"{item['stock_code']} {item['trade_date']}, "
                    f"got {result.rowcount}"
                )
            deleted += result.rowcount
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
    return deleted


def remove_invalid(
    start_date: str,
    end_date: str,
    raw_codes: list[str],
    *,
    apply: bool = False,
) -> int:
    codes = _normalize_codes(raw_codes)
    primary = create_batch_engine()
    listed = _listed_codes(primary, codes)
    if listed:
        print(_json({"status": "blocked", "listed_master_codes": listed}))
        return 3
    rows = _load_rows(primary, start_date, end_date, codes)
    summary = {
        "status": "pass",
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "rows": len(rows),
        "keys": [
            [str(row["trade_date"])[:10], str(row["stock_code"])]
            for row in rows
        ],
    }
    print(_json(summary))
    if not apply:
        print("[dry-run] exact rows identified; database unchanged")
        return 0

    mirror_url = _distinct_kline_url(str(primary.url))
    mirror = create_batch_engine(mirror_url) if mirror_url else None
    mirror_rows = (
        _load_rows(mirror, start_date, end_date, codes)
        if mirror is not None
        else []
    )
    print(_json({
        "primary_exact_rows": len(rows),
        "mirror_exact_rows": len(mirror_rows),
    }))
    mirror_deleted = (
        _delete_rows(mirror, mirror_rows, write_audit=False)
        if mirror is not None
        else 0
    )
    primary_deleted = _delete_rows(primary, rows, write_audit=True)
    remaining_primary = _load_rows(primary, start_date, end_date, codes)
    remaining_mirror = (
        _load_rows(mirror, start_date, end_date, codes)
        if mirror is not None
        else []
    )
    if remaining_primary or remaining_mirror:
        raise RuntimeError("invalid rows remain after deletion")
    print(_json({
        **summary,
        "status": "deleted",
        "primary_deleted": primary_deleted,
        "mirror_deleted": mirror_deleted,
    }))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--codes", nargs="+", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    engine = create_batch_engine()
    with mysql_named_lock(
        engine,
        "probiga:stock_kline_daily",
        timeout_seconds=60,
    ):
        return remove_invalid(
            args.start_date,
            args.end_date,
            args.codes,
            apply=args.apply,
        )


if __name__ == "__main__":
    raise SystemExit(main())
