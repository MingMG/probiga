#!/usr/bin/env python3
"""Publish a public-source index daily partition without a QMT runtime."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import bindparam, text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sync_stock_market import _fetch_verified_external_index_kline, step_index_kline
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, authoritative_closed_trade_date
from server.common.batch_db import create_batch_engine
from server.common.index_daily_publication import FREE_RECEIPT_SCHEMA, canonical_free_index_rows
from server.common.kline_data import get_kline_engine
from server.common.mysql_lock import mysql_named_lock
from server.common.qmt_attestation_contract import canonical_digest
from tools.env_config import load_project_env


RESULT_SCHEMA = "probiga.free-index-daily-result.v1"
RECEIPT_SCHEMA = FREE_RECEIPT_SCHEMA
DATASET = "index_daily"
PARTITION_KEY = "__index_daily_partition__"
PROVIDER = "tencent_east_verified"
REPAIR_RESULT_SCHEMA = "probiga.free-index-daily-repair-result.v1"
INDEX_WRITE_LOCK = "probiga:exact:sm_index_kline"
MAX_REPAIR_CODES = 8
_DATABASE_IDENTITY = text("SELECT @@server_uuid,DATABASE()")
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


def _repair_rows(connection, target_date: str, codes: list[str]) -> dict[str, dict]:
    rows = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ index_code,trade_time,trade_date,k_type,
               `open`,`close`,high,low,volume,amount,`change`,change_pct
          FROM sm_index_kline WHERE trade_date=:target AND k_type=1 AND index_code IN :codes
         LIMIT :limit
    """).bindparams(bindparam("codes", expanding=True)), {
        "target": target_date, "codes": codes, "limit": len(codes) + 1,
    }).mappings().all()
    indexed = {str(row["index_code"]): dict(row) for row in rows}
    if len(indexed) != len(rows):
        raise RuntimeError("index_repair_readback: duplicate exact keys")
    return indexed


def _repair_source(code: str, target_date: str) -> tuple[str, dict]:
    frame, variant = _fetch_verified_external_index_kline(code, target_date, target_date)
    if len(frame) != 1 or variant not in {"cross_checked", "tencent_only", "east_only", "cnindex_official"}:
        raise ValueError("index_repair_source: target bar unavailable")
    row = frame.iloc[0].to_dict()
    close_time = f"{target_date} 15:00:00"
    if (str(row.get("index_code")) != code or str(row.get("trade_date"))[:10] != target_date
            or str(row.get("trade_time")) != close_time or row.get("k_type") != 1):
        raise ValueError("index_repair_source: exact date, close time or code differs")
    values = {name: float(row[name]) for name in ("open", "close", "high", "low", "volume", "change", "change_pct")}
    amount = float(row["amount"]) if row.get("amount") is not None else None
    if amount is not None and math.isnan(amount):
        amount = None
    if (not all(math.isfinite(value) for value in values.values())
            or not 0 < values["low"] <= min(values["open"], values["close"])
            or max(values["open"], values["close"]) > values["high"] or values["volume"] < 0
            or (amount is not None and (not math.isfinite(amount) or amount < 0))):
        raise ValueError("index_repair_source: invalid price or volume")
    return variant, {**values, "amount": amount, "index_code": code, "trade_time": close_time,
                     "trade_date": target_date, "k_type": 1}


def _matches_repair_bar(actual: dict, expected: dict) -> bool:
    for field in ("index_code", "trade_date", "trade_time", "k_type"):
        if str(actual[field]) != str(expected[field]):
            return False
    for field in ("open", "close", "high", "low", "volume", "amount", "change", "change_pct"):
        observed, source = actual[field], expected[field]
        if observed is None or source is None:
            if observed is not source:
                return False
        elif isinstance(observed, Decimal):
            unit = Decimal(1).scaleb(observed.as_tuple().exponent)
            if Decimal(str(source)).quantize(unit, rounding=ROUND_HALF_UP) != observed:
                return False
        elif not math.isclose(observed, source, rel_tol=1e-12, abs_tol=1e-9):
            return False
    return True


def _repair_publish(connection, target_date: str, pending: dict[str, tuple[str, dict]]) -> list[dict]:
    from server.common.index_daily_publication import (
        FREE_BAR_PARTITION_PREFIX, FREE_BAR_RECEIPT_SCHEMA, free_index_bar_hash,
    )

    concurrent = _repair_rows(connection, target_date, list(pending))
    captured = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    inserts = [dict(row, etl_sync_at=captured) for code, (_, row) in pending.items() if code not in concurrent]
    if not inserts:
        return []
    connection.execute(text("""
        INSERT INTO sm_index_kline (index_code,trade_time,trade_date,k_type,
            `open`,`close`,high,low,volume,amount,`change`,change_pct,etl_sync_at)
        VALUES (:index_code,:trade_time,:trade_date,:k_type,
            :open,:close,:high,:low,:volume,:amount,:change,:change_pct,:etl_sync_at)
    """), inserts)
    codes = [row["index_code"] for row in inserts]
    persisted = _repair_rows(connection, target_date, codes)
    if set(persisted) != set(codes) or any(
        not _matches_repair_bar(persisted[code], pending[code][1]) for code in codes
    ):
        raise RuntimeError("index_repair_readback: exact inserts differ")
    available_at = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    receipts = []
    for code in codes:
        content_hash = free_index_bar_hash(persisted[code])
        proof = dict(schema=FREE_BAR_RECEIPT_SCHEMA, provider=PROVIDER, target_date=target_date,
                     index_code=code, source_variant=pending[code][0], available_at=available_at,
                     row_hash=content_hash)
        receipts.append({**proof, "receipt_id": canonical_digest(proof)})
    connection.execute(text("""
        INSERT INTO acquisition_partition_state (dataset,source,target_date,partition_key,status,
            request_id,written_rows,last_attempt_at,last_success_at,detail_json,updated_at)
        VALUES (:dataset,:source,:day,:partition_key,'complete',:receipt_id,1,:available_at,
            :available_at,:detail,:available_at)
    """), [dict(dataset=DATASET, source=PROVIDER, day=target_date,
        partition_key=f"{FREE_BAR_PARTITION_PREFIX}{item['index_code']}:{item['row_hash'][:40]}",
        receipt_id=item["receipt_id"], available_at=available_at,
        detail=json.dumps(item, ensure_ascii=False, separators=(",", ":"))) for item in receipts])
    return receipts


def repair_missing(target_date: str, *, apply: bool, codes: list[str]) -> dict:
    requested = sorted(set(codes))
    if not 0 < len(requested) <= MAX_REPAIR_CODES or any(
        len(code) != 6 or not code.isascii() or not code.isdigit() for code in requested
    ):
        raise ValueError(f"index_repair_scope: supply 1 to {MAX_REPAIR_CODES} exact six-digit index codes")
    primary = create_batch_engine()
    history = get_kline_engine()
    try:
        with primary.connect() as connection:
            closed = authoritative_closed_trade_date(connection, close_ready_time=time(15, 35))
            day_state = connection.execute(text("""
                SELECT DISTINCT trade_status FROM si_trade_calendar WHERE trade_date=:target
            """), {"target": target_date}).scalars().all()
            identity = connection.execute(_DATABASE_IDENTITY).one() if apply else None
        if not closed or target_date > closed or day_state != [1]:
            raise ValueError("index_repair_scope: target is not an authoritative closed session")
        with history.connect() as connection:
            if apply and identity != connection.execute(_DATABASE_IDENTITY).one():
                raise ValueError("index_repair_scope: index rows and publication receipts are on different databases")
            present = _repair_rows(connection, target_date, requested)
        missing = [code for code in requested if code not in present]
        pending = {}
        errors = {}
        if missing:
            with ThreadPoolExecutor(max_workers=min(len(missing), 4)) as workers:
                futures = {code: workers.submit(_repair_source, code, target_date) for code in missing}
                for code, future in futures.items():
                    try:
                        pending[code] = future.result()
                    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
                        errors[code] = type(exc).__name__
        receipts = []
        if apply and pending:
            with mysql_named_lock(history, INDEX_WRITE_LOCK, timeout_seconds=5) as connection:
                connection.commit()
                with connection.begin():
                    if identity != connection.execute(_DATABASE_IDENTITY).one():
                        raise RuntimeError("index_repair_publish: database identity changed")
                    receipts = _repair_publish(connection, target_date, pending)
        written = {receipt["index_code"] for receipt in receipts}
        already_present = sorted(set(present) | (set(pending) - written if apply else set()))
        return {"schema": REPAIR_RESULT_SCHEMA, "provider": PROVIDER, "target_date": target_date,
                "status": "PARTIAL" if errors else "DRY_RUN" if not apply else "COMPLETE" if receipts else "ALREADY_PRESENT",
                "requested_codes": requested, "present_codes": already_present, "source_ready_codes": sorted(pending),
                "missing_codes": sorted(errors), "source_errors": errors, "receipts": receipts,
                "database_writes": bool(receipts), "qmt_attestation_claimed": False}
    finally:
        primary.dispose()
        history.dispose()


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
    canonical = canonical_free_index_rows(rows)
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
    parser.add_argument("--repair-missing", action="store_true", help="fill only missing bars for one explicit date and code set")
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
                authoritative_closed_trade_date(engine, close_ready_time=time(15, 35))
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
        if args.repair_missing:
            if args.date == "latest":
                raise ValueError("index_repair_scope: an explicit --date is required")
            result = repair_missing(target, apply=not args.dry_run, codes=codes)
        else:
            if codes and args.date != "latest" and not args.dry_run:
                raise ValueError("index_repair_scope: exact-code writes require --repair-missing")
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
    return 0 if result["status"] in {"COMPLETE", "ALREADY_PRESENT", "DRY_RUN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
