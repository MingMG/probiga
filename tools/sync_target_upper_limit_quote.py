#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish exact Top80 target-day Eastmoney upper-limit quote evidence.

This command is intentionally not a replacement for the 80 x 21 MyQuant
history job.  Its machine receipt always states that it covers 80 target-day
rows and leaves the remaining historical requirement unresolved.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import (
    prepare_preliminary_upper_subject_receipt,
)
from server.common.analysis_pool_receipt import (
    validate_preliminary_upper_subject_receipt,
)
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.eastmoney_upper_limit_quote import (
    build_eastmoney_upper_limit_subject,
    collect_eastmoney_upper_limit_quote_run,
    freeze_qmt_upper_limit_quote_targets,
    publish_eastmoney_upper_limit_quote_run,
    recover_completed_eastmoney_upper_limit_quote_receipt,
)
from server.common.market_field_capture_schema import (
    validate_market_field_capture_runtime,
)
from tools.env_config import create_tool_engine, load_project_env
from tools.sync_target_turnover_snapshot import resolve_build_sha


def _target_date(value: str) -> date:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--target-date must be exact YYYY-MM-DD") from exc
    if parsed.isoformat() != raw:
        raise ValueError("--target-date must be exact YYYY-MM-DD")
    return parsed


def _decision_at(value: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--decision-at must be an exact ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    if parsed.microsecond:
        raise ValueError("--decision-at must be second-exact")
    return parsed


def _load_preliminary_receipt(
    path: str,
    *,
    target_date: str,
    decision_at: datetime,
    build_sha: str,
) -> dict:
    source = Path(str(path or "").strip()).resolve()
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: preliminary upper subject receipt is unavailable"
        ) from exc
    try:
        receipt = validate_preliminary_upper_subject_receipt(payload)
    except ValueError as exc:
        raise RuntimeError(
            f"DATA_BLOCKED: preliminary upper subject receipt is invalid: {exc}"
        ) from exc
    if (
        receipt["trade_date"] != target_date
        or receipt["decision_at"] != decision_at.isoformat(timespec="seconds")
        or receipt["build_sha"] != build_sha
    ):
        raise RuntimeError(
            "DATA_BLOCKED: preliminary upper subject receipt identity differs"
        )
    return receipt


def _machine_receipt(receipt: dict) -> dict:
    return {
        **receipt,
        "formal_history_required_count": 80 * 21,
        "formal_history_covered_count": 80,
        "formal_history_remaining_count": 80 * 20,
        "formal_history_status": "DATA_BLOCKED_TARGET_DAY_ONLY",
        "formal_history_reason": (
            "DATA_BLOCKED: exact historical upper-limit provider evidence "
            "for the preceding 20 sessions is not supplied by stock/get"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish immutable Eastmoney Top80 target-day limit quotes"
    )
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--decision-at", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--preliminary-receipt-file")
    source.add_argument("--prepare-preliminary", action="store_true")
    parser.add_argument("--min-score", type=float, default=62.0)
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--workers", type=int, choices=range(1, 17), default=8)
    parser.add_argument("--timeout-seconds", type=float, default=12.0)
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_tool_engine()
    validate_market_field_capture_runtime(engine)
    target = _target_date(args.target_date)
    decision_at = _decision_at(args.decision_at)
    build_sha = resolve_build_sha(args.expected_build_sha)
    closed = authoritative_closed_trade_date(
        engine, now=datetime.now(PRODUCTION_TIMEZONE)
    )
    if not closed or target.isoformat() > closed:
        raise RuntimeError("DATA_BLOCKED: target session is not closed")
    production = (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    )
    if production and not args.prepare_preliminary:
        raise RuntimeError(
            "DATA_BLOCKED: production upper quote evidence must compute its "
            "preliminary Top80 subject in-process"
        )
    preliminary = (
        prepare_preliminary_upper_subject_receipt(
            engine,
            trade_date=target.isoformat(),
            decision_at=decision_at,
            build_sha=build_sha,
            min_score=float(args.min_score),
        )
        if args.prepare_preliminary
        else _load_preliminary_receipt(
            args.preliminary_receipt_file,
            target_date=target.isoformat(),
            decision_at=decision_at,
            build_sha=build_sha,
        )
    )
    subject = build_eastmoney_upper_limit_subject(
        target_date=target,
        decision_at=decision_at,
        preliminary_receipt=preliminary,
    )
    recovered = recover_completed_eastmoney_upper_limit_quote_receipt(
        engine, subject=subject, collector_build_sha=build_sha
    )
    if recovered is not None:
        print(json.dumps(
            _machine_receipt(recovered),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ))
        return 0
    now = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    if now > decision_at:
        raise RuntimeError(
            "DATA_BLOCKED: decision cutoff elapsed before target-day quote capture"
        )
    with engine.connect() as connection:
        targets = freeze_qmt_upper_limit_quote_targets(
            connection, subject=subject
        )
    run = collect_eastmoney_upper_limit_quote_run(
        subject=subject,
        targets=targets,
        collector_build_sha=build_sha,
        workers=args.workers,
        timeout_seconds=args.timeout_seconds,
    )
    receipt = publish_eastmoney_upper_limit_quote_run(engine, run)
    print(json.dumps(
        _machine_receipt(receipt),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
