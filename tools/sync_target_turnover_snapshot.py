#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Collect and atomically promote one full target-session f61 snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.turnover_snapshot import (
    DEFAULT_PUSH2HIS_RESOLVE_IP,
    PinnedCurlEastmoneyTurnoverCollector,
    collect_turnover_snapshot,
    freeze_qmt_turnover_targets,
    load_turnover_universe_authority,
    publish_turnover_snapshot,
    recover_completed_turnover_receipt,
)
from server.common.market_field_capture_schema import (
    validate_market_field_capture_runtime,
)
from tools.env_config import create_tool_engine, load_project_env


_SHA40 = re.compile(r"[0-9a-f]{40}")
TURNOVER_SOURCE_CLOSE_READY_TIME = time(15, 30)


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip().lower()


def _git_status_porcelain() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip()


def collector_bundle_sha256() -> str:
    artifacts = []
    for relative in (
        Path("server/common/turnover_snapshot.py"),
        Path("tools/sync_target_turnover_snapshot.py"),
    ):
        payload = (ROOT / relative).read_bytes()
        artifacts.append({
            "path": relative.as_posix(),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
    encoded = json.dumps(
        artifacts,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_build_sha(explicit: str = "") -> str:
    checkout = _git_head()
    if _git_status_porcelain():
        raise RuntimeError("DATA_BLOCKED: turnover collector checkout is dirty")
    scheduler = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    resolved = str(explicit or scheduler or checkout).strip().lower()
    if _SHA40.fullmatch(resolved) is None or resolved == "0" * 40:
        raise RuntimeError("DATA_BLOCKED: exact turnover collector build SHA unavailable")
    if scheduler and scheduler != resolved:
        raise RuntimeError("DATA_BLOCKED: turnover scheduler build SHA differs")
    if checkout != resolved:
        raise RuntimeError("DATA_BLOCKED: turnover collector checkout differs from build")
    return resolved


def _exact_decision_at(value: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--decision-at must be an exact ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    return parsed


def _require_open_closed_target(engine, target_date: str, *, now: datetime) -> None:
    try:
        target = date.fromisoformat(target_date)
    except ValueError as exc:
        raise RuntimeError("DATA_BLOCKED: turnover target date is invalid") from exc
    if target.isoformat() != target_date:
        raise RuntimeError("DATA_BLOCKED: turnover target date is invalid")
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT trade_status FROM si_trade_calendar "
                "WHERE trade_date=:target_date"
            ),
            {"target_date": target_date},
        ).fetchall()
    if len(rows) != 1 or int(rows[0][0] or 0) != 1:
        raise RuntimeError(
            "DATA_BLOCKED: immutable calendar does not prove one open turnover session"
        )
    latest_closed = authoritative_closed_trade_date(
        engine,
        now=now,
        close_ready_time=TURNOVER_SOURCE_CLOSE_READY_TIME,
    )
    if not latest_closed or target_date > latest_closed:
        raise RuntimeError("DATA_BLOCKED: turnover target session is not closed")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish one immutable full-market Eastmoney f61 turnover snapshot"
    )
    parser.add_argument("--target-date", required=True, help="exact YYYY-MM-DD session")
    parser.add_argument("--decision-at", required=True, help="Asia/Shanghai ISO cutoff")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--delay-seconds", type=float, default=1.2)
    parser.add_argument("--batch-every", type=int, default=50)
    parser.add_argument("--batch-pause-seconds", type=float, default=30.0)
    parser.add_argument("--transport-attempts", type=int, default=3)
    parser.add_argument("--transport-backoff-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_tool_engine()
    validate_market_field_capture_runtime(engine)
    decision_at = _exact_decision_at(args.decision_at)
    now = datetime.now(PRODUCTION_TIMEZONE)
    _require_open_closed_target(engine, args.target_date, now=now)
    build_sha = resolve_build_sha(args.expected_build_sha)
    binary_sha = collector_bundle_sha256()
    with engine.connect() as connection:
        authority = load_turnover_universe_authority(
            connection,
            target_date=args.target_date,
            decision_at=decision_at,
        )
    recovered = recover_completed_turnover_receipt(
        engine,
        target_date=args.target_date,
        decision_at=decision_at,
        collector_build_sha=build_sha,
        collector_binary_sha256=binary_sha,
        authority=authority,
    )
    if recovered is not None:
        print(json.dumps(
            recovered,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ))
        return 0
    if now.replace(tzinfo=None) > decision_at:
        raise RuntimeError(
            "DATA_BLOCKED: turnover decision cutoff has elapsed and no "
            "completed immutable run can be recovered"
        )
    with engine.connect() as connection:
        targets = freeze_qmt_turnover_targets(
            connection,
            target_date=args.target_date,
            decision_at=decision_at,
            authority=authority,
        )
    collector = PinnedCurlEastmoneyTurnoverCollector(
        resolve_ip=str(
            os.environ.get("TURNOVER_PUSH2HIS_RESOLVE_IP")
            or DEFAULT_PUSH2HIS_RESOLVE_IP
        ).strip(),
        curl_binary=str(
            os.environ.get("TURNOVER_CURL_BINARY")
            or ("curl.exe" if os.name == "nt" else "curl")
        ),
        timeout_seconds=args.timeout_seconds,
    )
    run = collect_turnover_snapshot(
        targets=targets,
        target_date=args.target_date,
        decision_at=decision_at,
        collector_build_sha=build_sha,
        collector_binary_sha256=binary_sha,
        authority=authority,
        collector=collector,
        delay_seconds=args.delay_seconds,
        batch_every=args.batch_every,
        batch_pause_seconds=args.batch_pause_seconds,
        transport_attempts=args.transport_attempts,
        transport_backoff_seconds=args.transport_backoff_seconds,
    )
    receipt = publish_turnover_snapshot(engine, run)
    print(
        json.dumps(
            receipt,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
