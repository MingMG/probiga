#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Insert explicitly named missing daily K-line rows after dual-source proof.

The command is read-only by default. Pass ``--apply`` only after the dry run
reports full source agreement. Exact requested rows must be absent in both the
primary and K-line read databases; existing rows are never overwritten.
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

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, write_frame  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.fetch_sm_stock_kline_daily import (  # noqa: E402
    _build_source_trace,
    _dataset_hash,
    _distinct_kline_url,
    _ensure_provenance_tables,
    _fetch_builtin_one,
    _fetch_independent_reference,
    _rows_match,
    _validate_daily_frame,
)
from tools.repair_kline_structural_anomalies import (  # noqa: E402
    _ensure_audit_table,
)


BUSINESS_COLUMNS = [
    "stock_code", "short_name", "trade_time", "trade_date", "k_type",
    "adjust_type", "open", "close", "high", "low", "volume", "amount",
    "change", "change_pct", "turnover_ratio", "pre_close", "etl_sync_at",
]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_codes(raw_codes: list[str]) -> list[str]:
    codes: list[str] = []
    for raw in raw_codes:
        for token in re.split(r"[\s,;]+", str(raw or "").strip()):
            if not token:
                continue
            code = token.zfill(6)
            if not re.fullmatch(r"(?:00|30|60|68|92)\d{4}", code):
                raise ValueError(f"unsupported A-share code: {token}")
            if code not in codes:
                codes.append(code)
    if not codes:
        raise ValueError("at least one stock code is required")
    return codes


def _load_names(engine: Engine, codes: list[str]) -> dict[str, str]:
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {f"code_{idx}": code for idx, code in enumerate(codes)}
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT stock_code, short_name
            FROM si_all_code
            WHERE stock_code IN ({placeholders})
        """), params).fetchall()
    names = {str(code).zfill(6): str(name or "") for code, name in rows}
    missing = sorted(set(codes) - set(names))
    if missing:
        raise RuntimeError(f"codes absent from si_all_code: {missing}")
    return names


def _existing_keys(
    engine: Engine,
    trade_date: str,
    codes: list[str],
) -> list[str]:
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {
        "trade_date": trade_date,
        **{f"code_{idx}": code for idx, code in enumerate(codes)},
    }
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT stock_code
            FROM sm_stock_kline
            WHERE trade_date = :trade_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code IN ({placeholders})
        """), params).fetchall()
    return sorted(str(row[0]).zfill(6) for row in rows)


def _fetch_replacement(
    code: str,
    short_name: str,
    trade_date: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    evidence: dict[str, Any] = {
        "stock_code": code,
        "trade_date": trade_date,
        "primary_source": "sina",
    }
    try:
        primary_frame = _fetch_builtin_one(
            code,
            trade_date,
            short_name,
            "sina",
        )
        reference_source, reference = _fetch_independent_reference(
            code,
            trade_date,
        )
    except Exception as exc:  # pylint: disable=broad-except
        evidence["status"] = "source_error"
        evidence["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        return None, evidence
    evidence["reference_source"] = reference_source
    if primary_frame is None or primary_frame.empty or reference is None:
        evidence["status"] = "source_unavailable"
        return None, evidence

    source_row = primary_frame.iloc[-1].to_dict()
    primary = {
        column: float(source_row[column])
        for column in ("open", "high", "low", "close")
    }
    matched, differences = _rows_match(primary, reference)
    evidence["primary"] = primary
    evidence["reference"] = reference
    evidence["differences"] = differences
    evidence["status"] = "agreed" if matched else "conflict"
    if not matched:
        return None, evidence

    row = {
        column: (
            None
            if pd.isna(source_row.get(column))
            else source_row.get(column)
        )
        for column in BUSINESS_COLUMNS
    }
    row["stock_code"] = code
    row["short_name"] = short_name
    row["trade_date"] = trade_date
    row["etl_sync_at"] = datetime.now().replace(microsecond=0)
    pre_close = float(row.get("pre_close") or 0)
    close = float(row.get("close") or 0)
    if pre_close <= 0:
        evidence["status"] = "invalid_pre_close"
        return None, evidence
    expected_change_pct = (close / pre_close - 1.0) * 100.0
    actual_change_pct = float(row.get("change_pct") or 0)
    if abs(expected_change_pct - actual_change_pct) > 0.02:
        evidence["status"] = "inconsistent_return"
        evidence["expected_change_pct"] = expected_change_pct
        evidence["actual_change_pct"] = actual_change_pct
        return None, evidence
    checked = _validate_daily_frame(
        pd.DataFrame([row]),
        trade_date,
    ).iloc[0].to_dict()
    checked["_data_source"] = "sina"
    evidence["row_sha256"] = _dataset_hash(pd.DataFrame([checked]))
    return checked, evidence


def _write_mirror(engine: Engine, frame: pd.DataFrame) -> int:
    with engine.begin() as conn:
        return int(write_frame(
            frame[BUSINESS_COLUMNS],
            "sm_stock_kline",
            conn,
            if_exists="append",
            index=False,
            chunksize=100,
            method="multi",
        ) or len(frame))


def _write_primary(
    engine: Engine,
    frame: pd.DataFrame,
    evidences: list[dict[str, Any]],
    *,
    trade_date: str,
) -> tuple[int, str]:
    _ensure_audit_table(engine)
    _ensure_provenance_tables(engine)
    run_id = str(uuid.uuid4())
    now = datetime.now().replace(microsecond=0)
    verified_codes = {
        str(item["stock_code"]).zfill(6): str(item["reference_source"])
        for item in evidences
    }
    trace = _build_source_trace(
        frame,
        run_id=run_id,
        verified_codes=verified_codes,
        fetched_at=now,
    )
    run_record = {
        "run_id": run_id,
        "target_date": trade_date,
        "mode": "missing_row_dual_source_repair",
        "source_chain": "sina,tencent",
        "universe_source": "explicit_missing_codes",
        "expected_count": len(frame),
        "fetched_count": len(frame),
        "coverage": 1.0,
        "source_counts_json": _json({"sina": len(frame)}),
        "cross_validation_json": _json({
            "status": "pass",
            "compared": len(frame),
            "matched": len(frame),
            "mismatched": 0,
            "unavailable": 0,
            "verified_sources": verified_codes,
        }),
        "dataset_sha256": _dataset_hash(frame),
        "status": "written",
        "started_at": now,
        "finished_at": now,
    }
    evidence_by_code = {
        str(item["stock_code"]).zfill(6): item
        for item in evidences
    }
    audit_rows = []
    for row in frame.to_dict(orient="records"):
        code = str(row["stock_code"]).zfill(6)
        audit_rows.append({
            "repair_id": str(uuid.uuid4()),
            "stock_code": code,
            "trade_date": trade_date,
            "k_type": 1,
            "adjust_type": 0,
            "reason": "missing_row_confirmed_by_sina_and_independent_source",
            "before_json": _json({"missing": True}),
            "after_json": _json(row),
            "evidence_json": _json(evidence_by_code[code]),
            "repaired_at": now,
        })

    with engine.begin() as conn:
        written = int(write_frame(
            frame[BUSINESS_COLUMNS],
            "sm_stock_kline",
            conn,
            if_exists="append",
            index=False,
            chunksize=100,
            method="multi",
        ) or len(frame))
        write_frame(
            trace,
            "st_kline_source_trace",
            conn,
            if_exists="append",
            index=False,
            chunksize=100,
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
        """), run_record)
        conn.execute(text("""
            INSERT INTO st_kline_repair_audit (
              repair_id, stock_code, trade_date, k_type, adjust_type,
              reason, before_json, after_json, evidence_json, repaired_at
            ) VALUES (
              :repair_id, :stock_code, :trade_date, :k_type, :adjust_type,
              :reason, :before_json, :after_json, :evidence_json, :repaired_at
            )
        """), audit_rows)
    return written, run_id


def repair_missing(
    trade_date: str,
    raw_codes: list[str],
    *,
    apply: bool = False,
) -> int:
    codes = _normalize_codes(raw_codes)
    primary = create_batch_engine()
    mirror_url = _distinct_kline_url(str(primary.url))
    mirror = create_batch_engine(mirror_url) if mirror_url else None
    names = _load_names(primary, codes)
    existing_primary = _existing_keys(primary, trade_date, codes)
    existing_mirror = (
        _existing_keys(mirror, trade_date, codes)
        if mirror is not None
        else []
    )
    if existing_primary or existing_mirror:
        print(_json({
            "status": "blocked",
            "existing_primary": existing_primary,
            "existing_mirror": existing_mirror,
        }))
        return 3

    rows: list[dict[str, Any]] = []
    evidences: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for code in codes:
        row, evidence = _fetch_replacement(
            code,
            names[code],
            trade_date,
        )
        print(code, evidence.get("status"), evidence.get("differences", {}))
        if row is None:
            rejected.append(evidence)
        else:
            rows.append(row)
            evidences.append(evidence)
    if rejected or len(rows) != len(codes):
        print(_json({"status": "blocked", "rejected": rejected}))
        return 3

    frame = pd.DataFrame(rows)
    frame = _validate_daily_frame(frame, trade_date)
    frame["_data_source"] = "sina"
    summary = {
        "status": "pass",
        "trade_date": trade_date,
        "requested": len(codes),
        "approved": len(frame),
        "codes": codes,
        "dataset_sha256": _dataset_hash(frame),
    }
    print(_json(summary))
    if not apply:
        print("[dry-run] all gates passed; database unchanged")
        return 0

    mirror_written = _write_mirror(mirror, frame) if mirror is not None else 0
    primary_written, run_id = _write_primary(
        primary,
        frame,
        evidences,
        trade_date=trade_date,
    )
    remaining_primary = sorted(
        set(codes) - set(_existing_keys(primary, trade_date, codes))
    )
    remaining_mirror = (
        sorted(set(codes) - set(_existing_keys(mirror, trade_date, codes)))
        if mirror is not None
        else []
    )
    if remaining_primary or remaining_mirror:
        raise RuntimeError(
            f"missing rows remain: primary={remaining_primary}, "
            f"mirror={remaining_mirror}"
        )
    print(_json({
        **summary,
        "status": "written",
        "primary_written": primary_written,
        "mirror_written": mirror_written,
        "run_id": run_id,
    }))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--codes", nargs="+", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    engine = create_batch_engine()
    with mysql_named_lock(
        engine,
        "probiga:stock_kline_daily",
        timeout_seconds=60,
    ):
        return repair_missing(
            args.trade_date,
            args.codes,
            apply=args.apply,
        )


if __name__ == "__main__":
    raise SystemExit(main())
