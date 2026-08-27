#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish one immutable MyQuant 80-stock x 21-session upper-limit run."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.market_field_capture_schema import (
    validate_market_field_capture_runtime,
)
from server.common.analysis_pool_receipt import (
    validate_preliminary_upper_subject_receipt,
)
from biz.analysis.sync_analysis_fast import (
    prepare_preliminary_upper_subject_receipt,
)
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)
from server.common.upper_limit_snapshot import (
    build_upper_limit_subject,
    collect_upper_limit_snapshot,
    publish_upper_limit_snapshot,
    recover_completed_upper_limit_receipt,
)
from tools.env_config import create_tool_engine, load_project_env
from tools.sync_target_turnover_snapshot import resolve_build_sha


_CODE = re.compile(r"(?:0|3|6)[0-9]{5}")


def _decision_at(value: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--decision-at must be an exact ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    return parsed


def _target_date(value: str) -> date:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--target-date must be exact YYYY-MM-DD") from exc
    if parsed.isoformat() != raw:
        raise ValueError("--target-date must be exact YYYY-MM-DD")
    return parsed


def _parse_codes(raw: str) -> list[str]:
    codes = sorted({item.strip() for item in re.split(r"[,\s]+", raw) if item.strip()})
    if len(codes) != 80 or any(_CODE.fullmatch(code) is None for code in codes):
        raise RuntimeError("DATA_BLOCKED: upper-limit capture requires exact 80 A-share codes")
    return codes


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
        or receipt["decision_at"]
        != decision_at.isoformat(timespec="seconds")
        or receipt["build_sha"] != build_sha
    ):
        raise RuntimeError(
            "DATA_BLOCKED: preliminary upper subject receipt identity differs"
        )
    receipt["ordered_stock_codes"] = _parse_codes(
        "\n".join(receipt["ordered_stock_codes"])
    )
    return receipt


def _load_sessions(engine, *, target: date, decision_at: datetime) -> tuple[list[str], dict]:
    start = target - timedelta(days=90)
    with engine.connect() as connection:
        receipt = load_trade_calendar_receipt(
            connection,
            start_date=start.isoformat(),
            end_date=target.isoformat(),
            decision_known_at=decision_at,
        )
    sessions = [day for day in receipt.sessions if day <= target.isoformat()][-21:]
    if len(sessions) != 21 or sessions[-1] != target.isoformat():
        raise RuntimeError(
            "DATA_BLOCKED: immutable QMT calendar does not prove exact 21-session window"
        )
    return sessions, {
        "calendar_batch_id": receipt.batch_id,
        "calendar_manifest_hash": receipt.manifest_hash,
        "calendar_session_set_hash": receipt.session_set_hash,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish immutable MyQuant historical upper-limit evidence"
    )
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--decision-at", required=True)
    preliminary_source = parser.add_mutually_exclusive_group(required=True)
    preliminary_source.add_argument("--preliminary-receipt-file")
    preliminary_source.add_argument("--prepare-preliminary", action="store_true")
    parser.add_argument("--min-score", type=float, default=62.0)
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_tool_engine()
    validate_market_field_capture_runtime(engine)
    validate_trade_calendar_runtime_schema(engine)
    target = _target_date(args.target_date)
    decision_at = _decision_at(args.decision_at)
    now = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    closed = authoritative_closed_trade_date(
        engine, now=datetime.now(PRODUCTION_TIMEZONE)
    )
    if not closed or target.isoformat() > closed:
        raise RuntimeError("DATA_BLOCKED: upper-limit target session is not closed")
    build_sha = resolve_build_sha(args.expected_build_sha)
    production = (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "")
        .strip()
        .lower()
        == "production"
    )
    if production and not args.prepare_preliminary:
        raise RuntimeError(
            "DATA_BLOCKED: production upper evidence must compute its "
            "preliminary subject in-process"
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
    codes = list(preliminary["ordered_stock_codes"])
    sessions, calendar_proof = _load_sessions(
        engine, target=target, decision_at=decision_at
    )
    subject = build_upper_limit_subject(
        target_date=target,
        stock_codes=codes,
        trade_dates=sessions,
        calendar_batch_id=calendar_proof["calendar_batch_id"],
        calendar_manifest_sha256=calendar_proof["calendar_manifest_hash"],
        calendar_session_set_sha256=calendar_proof[
            "calendar_session_set_hash"
        ],
        preliminary_receipt_sha256=preliminary["receipt_sha256"],
    )
    recovered = recover_completed_upper_limit_receipt(
        engine,
        subject=subject,
        decision_at=decision_at,
        collector_build_sha=build_sha,
    )
    if recovered is not None:
        print(json.dumps(
            {
                **recovered,
                **calendar_proof,
                "preliminary_ordered_candidate_sha256": preliminary[
                    "ordered_candidate_sha256"
                ],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ))
        return 0
    if now > decision_at:
        raise RuntimeError(
            "DATA_BLOCKED: upper-limit decision cutoff has elapsed and no "
            "completed immutable run can be recovered"
        )
    run = collect_upper_limit_snapshot(
        subject=subject,
        decision_at=decision_at,
        collector_build_sha=build_sha,
        preliminary_receipt=preliminary,
        timeout=args.timeout_seconds,
    )
    receipt = publish_upper_limit_snapshot(engine, run)
    print(json.dumps(
        {
            **receipt,
            **calendar_proof,
            "collector_build_sha": build_sha,
            "preliminary_receipt_sha256": preliminary["receipt_sha256"],
            "preliminary_ordered_candidate_sha256": preliminary[
                "ordered_candidate_sha256"
            ],
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
