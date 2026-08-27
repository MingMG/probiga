#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export the sealed read-only pre-upper top-80 analysis subject."""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import (
    prepare_preliminary_upper_subject_receipt,
)
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
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
    if (
        parsed.tzinfo is not None
        or parsed.microsecond != 0
        or parsed.isoformat(timespec="seconds") != raw
    ):
        raise ValueError(
            "--decision-at must be naive Asia/Shanghai with second precision"
        )
    return parsed


def _write_receipt(path: str, receipt: dict) -> Path:
    target = Path(str(path or "").strip()).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(payload, encoding="utf-8", newline="\n")
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export deterministic preliminary top-80 upper-limit subject"
    )
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--decision-at", required=True)
    parser.add_argument("--min-score", type=float, default=62.0)
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_tool_engine()
    target = _target_date(args.target_date)
    decision = _decision_at(args.decision_at)
    now = datetime.now(PRODUCTION_TIMEZONE)
    if now.replace(tzinfo=None) > decision:
        raise RuntimeError("DATA_BLOCKED: preliminary decision cutoff has elapsed")
    closed = authoritative_closed_trade_date(engine, now=now)
    if not closed or target.isoformat() > closed:
        raise RuntimeError("DATA_BLOCKED: preliminary target session is not closed")
    build_sha = resolve_build_sha(args.expected_build_sha)
    receipt = prepare_preliminary_upper_subject_receipt(
        engine,
        trade_date=target.isoformat(),
        decision_at=decision,
        build_sha=build_sha,
        min_score=float(args.min_score),
    )
    output_path = _write_receipt(args.output, receipt)
    print(json.dumps(
        {
            "status": "COMPLETED",
            "receipt_sha256": receipt["receipt_sha256"],
            "ordered_candidate_sha256": receipt[
                "ordered_candidate_sha256"
            ],
            "code_set_sha256": receipt["code_set_sha256"],
            "target_date": receipt["trade_date"],
            "decision_at": receipt["decision_at"],
            "output": str(output_path),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
