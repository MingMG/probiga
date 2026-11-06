#!/usr/bin/env python3
"""Publish a public-source index daily partition without a QMT runtime."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
import json
import math
from pathlib import Path
import sys
from time import monotonic

from sqlalchemy import bindparam, text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sync_stock_market import _fetch_verified_external_index_kline
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, authoritative_closed_trade_date
from server.common.batch_db import create_batch_engine
from server.common.index_daily_publication import (
    FREE_BAR_PARTITION_PREFIX, FREE_BAR_RECEIPT_SCHEMA, FREE_INDEX_RESULT_SCHEMA,
    PUBLIC_INDEX_DAILY_CODES,
    free_index_bar_hash, index_scope_fingerprint, load_verified_index_daily_bars,
)
from server.common.kline_data import get_kline_engine
from server.common.mysql_lock import mysql_named_lock
from server.common.qmt_attestation_contract import canonical_digest
from tools.env_config import load_project_env


RESULT_SCHEMA = FREE_INDEX_RESULT_SCHEMA
DATASET = "index_daily"
PROVIDER = "tencent_east_verified"
INDEX_WRITE_LOCK = "probiga:exact:sm_index_kline"
MAX_REPAIR_CODES = 16
_DATABASE_IDENTITY = text("SELECT @@server_uuid,DATABASE()")


def _repair_rows(connection, target_date: str, codes: list[str]) -> dict[str, dict]:
    rows = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ index_code,trade_time,trade_date,k_type,
               `open`,`close`,high,low,volume,amount,`change`,change_pct,etl_sync_at
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
    concurrent = _repair_rows(connection, target_date, list(pending))
    captured = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    inserts = [dict(row, etl_sync_at=captured) for code, (_, row) in pending.items() if code not in concurrent]
    if inserts:
        connection.execute(text("""
        INSERT INTO sm_index_kline (index_code,trade_time,trade_date,k_type,
            `open`,`close`,high,low,volume,amount,`change`,change_pct,etl_sync_at)
        VALUES (:index_code,:trade_time,:trade_date,:k_type,
            :open,:close,:high,:low,:volume,:amount,:change,:change_pct,:etl_sync_at)
        """), inserts)
    persisted = _repair_rows(connection, target_date, list(pending))
    if any(row["index_code"] not in persisted or not _matches_repair_bar(
        persisted[row["index_code"]], row) for row in inserts
    ):
        raise RuntimeError("index_repair_readback: exact inserts differ")
    available_at = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    receipts = []
    for code, bar in persisted.items():
        if (not _matches_repair_bar(bar, pending[code][1]) or bar["etl_sync_at"] is None
                or not f"{target_date} 15:00:00" <= str(bar["etl_sync_at"]) <= available_at):
            continue
        content_hash = free_index_bar_hash(bar)
        proof = dict(schema=FREE_BAR_RECEIPT_SCHEMA, provider=PROVIDER, target_date=target_date,
                     index_code=code, source_variant=pending[code][0], available_at=available_at,
                     row_hash=content_hash)
        key = f"{FREE_BAR_PARTITION_PREFIX}{code}:{content_hash[:40]}"
        exists = connection.execute(text("""
            SELECT 1 FROM acquisition_partition_state WHERE dataset=:dataset AND source=:source
            AND target_date=:day AND partition_key=:key
        """), dict(dataset=DATASET, source=PROVIDER, day=target_date, key=key)).scalar()
        if exists is None:
            receipts.append({**proof, "receipt_id": canonical_digest(proof)})
    if not receipts:
        return []
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


def publish(target_date: str, *, apply: bool, codes: list[str]) -> dict:
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
        def read_evidence():
            return load_verified_index_daily_bars(
                primary, history, sessions=[target_date], codes=requested,
                as_of=datetime.now(PRODUCTION_TIMEZONE), deadline=monotonic() + 15,
            )

        verified = read_evidence()
        present = {bar["index_code"] for bar in verified}
        missing = sorted(set(requested) - present)
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
        if apply and pending:
            verified = read_evidence()
        verified_codes = sorted(bar["index_code"] for bar in verified)
        unresolved = sorted(set(requested) - set(verified_codes))
        status = "DRY_RUN" if not apply else "PARTIAL" if unresolved else "COMPLETE"
        return {"schema": RESULT_SCHEMA, "provider": PROVIDER, "target_date": target_date,
                "status": status, "requested_codes": requested, "verified_codes": verified_codes,
                "missing_codes": unresolved, "source_ready_codes": sorted(pending),
                "source_errors": errors, "receipts": receipts, "proof_sha256": index_scope_fingerprint(verified),
                "database_writes": bool(receipts), "qmt_attestation_claimed": False}
    finally:
        primary.dispose()
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
        help="comma-separated index codes, added to the consumer scope for the latest session",
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
        if args.repair_missing == (args.date == "latest"):
            raise ValueError("index_repair_scope: an explicit --date and --repair-missing are required together")
        if args.date == "latest":
            codes = sorted(set(codes).union(PUBLIC_INDEX_DAILY_CODES))
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
    return 0 if result["status"] in {"COMPLETE", "DRY_RUN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
