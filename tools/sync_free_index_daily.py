#!/usr/bin/env python3
"""Publish a public-source index daily partition without a QMT runtime."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sync_stock_market import step_index_kline
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.common.qmt_attestation_contract import canonical_digest
from tools.env_config import load_project_env


RESULT_SCHEMA = "probiga.free-index-daily-result.v1"
RECEIPT_SCHEMA = "probiga.free-index-daily-receipt.v1"
DATASET = "index_daily"
PARTITION_KEY = "__index_daily_partition__"
PROVIDER = "tencent_east_verified"
EXCLUDED_UNSUPPORTED_INDEX_CODES = frozenset(
    {
        "988006",
        "988007",
        "988106",
        "988107",
        "988201",
    }
)


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _canonical_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        result.append(
            {
                "index_code": str(row["index_code"]).zfill(6),
                "trade_date": str(row["trade_date"])[:10],
                "open": str(row["open"]),
                "close": str(row["close"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "volume": str(row["volume"]),
                "amount": str(row["amount"]),
            }
        )
    return sorted(result, key=lambda item: item["index_code"])


def _read_target(engine: Any, target_date: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT index_code,trade_date,`open`,`close`,high,low,"
                    "volume,amount FROM sm_index_kline "
                    "WHERE trade_date=:target AND k_type=1 "
                    "ORDER BY index_code"
                ),
                {"target": target_date},
            ).mappings()
        ]


def _expected_codes(engine: Any, target_date: str) -> list[str]:
    with engine.connect() as connection:
        prior = connection.execute(
            text(
                "SELECT MAX(trade_date) FROM sm_index_kline "
                "WHERE trade_date<:target AND k_type=1"
            ),
            {"target": target_date},
        ).scalar()
        if prior is None:
            raise RuntimeError(
                "DATA_BLOCKED: no persisted verified index scope precedes target"
            )
        codes = connection.execute(
            text(
                "SELECT DISTINCT index_code FROM sm_index_kline "
                "WHERE trade_date=:prior AND k_type=1"
            ),
            {"prior": str(prior)[:10]},
        ).scalars()
        return sorted(
            str(code).zfill(6)
            for code in codes
            if str(code).zfill(6) not in EXCLUDED_UNSUPPORTED_INDEX_CODES
        )


def _write_receipt(
    engine: Any,
    *,
    target_date: str,
    status: str,
    expected_codes: list[str],
    rows: list[dict[str, Any]],
    captured_at: str,
) -> dict[str, Any]:
    canonical = _canonical_rows(rows)
    codes = sorted({row["index_code"] for row in canonical})
    missing = sorted(set(expected_codes) - set(codes))
    unsigned = {
        "schema": RECEIPT_SCHEMA,
        "dataset": DATASET,
        "provider": PROVIDER,
        "target_date": target_date,
        "captured_at": captured_at,
        "status": status,
        "expected_count": len(expected_codes),
        "row_count": len(canonical),
        "code_count": len(codes),
        "missing_codes": missing,
        "expected_code_set_hash": canonical_digest(expected_codes),
        "code_set_hash": canonical_digest(codes),
        "row_hash": canonical_digest(canonical),
        "qmt_attestation_claimed": False,
    }
    receipt = {**unsigned, "receipt_id": canonical_digest(unsigned)}
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO acquisition_partition_state (
                    dataset, source, target_date, partition_key, status,
                    request_id, written_rows, last_attempt_at, last_success_at,
                    next_retry_at, last_error_code, last_error, detail_json,
                    updated_at
                ) VALUES (
                    :dataset, :source, :target_date, :partition_key, :status,
                    :request_id, :written_rows, :now_value,
                    :success_at, NULL, NULL, :last_error, :detail_json,
                    :now_value
                )
                ON DUPLICATE KEY UPDATE
                    status=VALUES(status), request_id=VALUES(request_id),
                    written_rows=VALUES(written_rows),
                    last_attempt_at=VALUES(last_attempt_at),
                    last_success_at=VALUES(last_success_at),
                    next_retry_at=NULL, last_error_code=NULL,
                    last_error=VALUES(last_error),
                    detail_json=VALUES(detail_json), updated_at=VALUES(updated_at)
                """
            ),
            {
                "dataset": DATASET,
                "source": PROVIDER,
                "target_date": target_date,
                "partition_key": PARTITION_KEY,
                "status": status,
                "request_id": receipt["receipt_id"][:64],
                "written_rows": len(rows),
                "now_value": captured_at,
                "success_at": captured_at if status == "complete" else None,
                "last_error": (
                    None
                    if status == "complete"
                    else "missing public price indices: "
                    + ",".join(missing[:50])
                ),
                "detail_json": json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    return receipt


def publish(
    target_date: str,
    *,
    apply: bool = True,
    codes: list[str] | None = None,
) -> dict[str, Any]:
    engine = create_batch_engine()
    history = get_kline_engine()
    try:
        requested = sorted({
            str(code or "").strip().zfill(6)
            for code in (codes or [])
            if str(code or "").strip()
        })
        if requested and any(
            len(code) != 6 or not code.isdigit() for code in requested
        ):
            raise ValueError("explicit index codes must be six digits")
        expected = requested or _expected_codes(engine, target_date)
        if not expected:
            raise RuntimeError("DATA_BLOCKED: persisted index scope is empty")
        if apply:
            os.environ["DATA_SOURCE_INDEX_KLINE"] = "tencent"
            step_index_kline(
                engine,
                expected,
                target_date,
                target_date,
            )
        rows = _read_target(history, target_date)
        codes = {str(row["index_code"]).zfill(6) for row in rows}
        missing = sorted(set(expected) - codes)
        status = "complete" if not missing else "partial"
        captured = _now().isoformat(sep=" ")
        receipt = (
            _write_receipt(
                engine,
                target_date=target_date,
                status=status,
                expected_codes=expected,
                rows=rows,
                captured_at=captured,
            )
            if apply
            else {
                "status": status,
                "receipt_id": hashlib.sha256(
                    (target_date + ":" + ",".join(sorted(codes))).encode()
                ).hexdigest(),
            }
        )
        return {
            "schema": RESULT_SCHEMA,
            "status": "COMPLETE" if status == "complete" else "PARTIAL",
            "provider": PROVIDER,
            "target_date": target_date,
            "expected_count": len(expected),
            "row_count": len(rows),
            "code_count": len(codes),
            "missing_codes": missing,
            "receipt_id": receipt["receipt_id"],
            "database_writes": bool(apply),
            "qmt_attestation_claimed": False,
        }
    finally:
        engine.dispose()
        history.dispose()


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="latest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--codes",
        default="",
        help="comma-separated exact index codes; empty uses persisted index scope",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    target = str(args.date or "latest")
    try:
        engine = create_batch_engine()
        try:
            target = (
                authoritative_closed_trade_date(engine)
                if args.date == "latest"
                else date.fromisoformat(args.date).isoformat()
            )
            if not target:
                raise RuntimeError(
                    "DATA_BLOCKED: authoritative closed trade date is unavailable"
                )
        finally:
            engine.dispose()
        codes = [
            item.strip()
            for item in str(args.codes or "").split(",")
            if item.strip()
        ]
        result = publish(target, apply=not args.dry_run, codes=codes)
    except Exception as exc:
        result = {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "provider": PROVIDER,
            "target_date": target,
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "database_writes": False,
            "qmt_attestation_claimed": False,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
