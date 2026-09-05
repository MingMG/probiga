#!/usr/bin/env python3
"""Prepare and publish the exact-date analysis pool during a production release."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.analysis_pool_receipt import read_persisted_pool_manifest
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine
from tools.env_config import load_project_env


RESULT_SCHEMA = "probiga.release-analysis-fast-result.v1"
TASK_TYPE = "analysis_fast"
EXPECTED_SCRIPT_PATH = "tools/run_ai_recommendation_premarket.py"
EXPECTED_SCRIPT_ARGS = "--top-n 80 --min-score 62 --json"
_SHA40 = re.compile(r"[0-9a-f]{40}")


def _target_date(value: str) -> str:
    raw = str(value or "").strip()
    parsed = date.fromisoformat(raw)
    if parsed.isoformat() != raw:
        raise ValueError("target date is not canonical")
    return raw


def _build_sha(value: str) -> str:
    supplied = str(value or "").strip().lower()
    environment = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").strip().lower()
    if (
        _SHA40.fullmatch(supplied) is None
        or supplied == "0" * 40
        or supplied != environment
    ):
        raise ValueError("release analysis build identity differs")
    return supplied


def _upper_readiness(engine, *, target: str, build_sha: str) -> dict[str, object]:
    with engine.connect() as connection:
        flow_rows = int(connection.execute(text(
            "SELECT COUNT(*) FROM sm_stock_capital_flow_daily "
            "WHERE trade_date=:target"
        ), {"target": target}).scalar() or 0)
        upper_rows = connection.execute(text("""
            SELECT run_id, decision_at, published_at
            FROM st_market_field_capture_run
            WHERE target_date=:target
              AND status='COMPLETED'
              AND capture_kind='DAILY_UPPER_LIMIT_HISTORY'
              AND provider='myquant.gm.get_history_instruments'
              AND collector_build_sha=:build_sha
            ORDER BY published_at DESC, run_id DESC
            LIMIT 1
        """), {"target": target, "build_sha": build_sha}).mappings().all()
    upper = dict(upper_rows[0]) if len(upper_rows) == 1 else None
    now = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    cutoff = upper.get("decision_at") if upper else None
    if cutoff is not None and not isinstance(cutoff, datetime):
        cutoff = datetime.fromisoformat(str(cutoff))
    ready = bool(
        flow_rows >= 5000
        and upper is not None
        and isinstance(cutoff, datetime)
        and cutoff.tzinfo is None
        and cutoff <= now
        and re.fullmatch(r"[0-9a-f]{32}", str(upper.get("run_id") or ""))
    )
    return {
        "ready": ready,
        "flow_rows": flow_rows,
        "upper_run_id": str(upper.get("run_id") or "") if upper else "",
        "decision_at": cutoff.isoformat(timespec="seconds") if cutoff else "",
    }


def _result(
    *,
    status: str,
    target: str,
    build_sha: str,
    readiness: dict[str, object],
    **extra: object,
) -> dict[str, object]:
    return {
        "schema": RESULT_SCHEMA,
        "status": status,
        "task_type": TASK_TYPE,
        "target_trade_date": target,
        "build_sha": build_sha,
        **readiness,
        **extra,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--readiness-only", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=0)
    args = parser.parse_args(argv)

    if str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower() != "production":
        raise RuntimeError("release analysis is production-only")
    target = _target_date(args.target_date)
    build_sha = _build_sha(args.expected_build_sha)
    load_project_env()
    engine = create_batch_engine(future=True)
    try:
        authoritative = authoritative_closed_trade_date(
            engine,
            now=datetime.now(PRODUCTION_TIMEZONE),
        )
        if authoritative != target:
            raise RuntimeError("release analysis target differs from authoritative closed session")

        deadline = time.monotonic() + max(0, min(int(args.wait_seconds), 1800))
        while True:
            readiness = _upper_readiness(engine, target=target, build_sha=build_sha)
            if readiness["ready"] or time.monotonic() >= deadline:
                break
            time.sleep(5)
        if not readiness["ready"]:
            print(json.dumps(_result(
                status="DATA_BLOCKED",
                target=target,
                build_sha=build_sha,
                readiness=readiness,
            ), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            return 2
        if args.readiness_only:
            print(json.dumps(_result(
                status="READY",
                target=target,
                build_sha=build_sha,
                readiness=readiness,
            ), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            return 0

        if os.name != "posix" or os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != "linux_standalone":
            raise RuntimeError("release analysis requires the Linux scheduler executor")
        with engine.connect() as connection:
            rows = [dict(row) for row in connection.execute(text(
                "SELECT * FROM st_scheduled_tasks WHERE task_type=:task_type "
                "ORDER BY id LIMIT 2"
            ), {"task_type": TASK_TYPE}).mappings().all()]
        if len(rows) != 1:
            raise RuntimeError("analysis_fast task registration is unavailable or ambiguous")
        row = rows[0]
        if (
            int(row.get("enabled") or 0) != 1
            or str(row.get("script_path") or "").replace("\\", "/") != EXPECTED_SCRIPT_PATH
            or str(row.get("script_args") or "").strip() != EXPECTED_SCRIPT_ARGS
        ):
            raise RuntimeError("analysis_fast task registration differs from release contract")

        from server.api import scheduler_runtime

        if not scheduler_runtime._claim_task_run(row, engine):
            raise RuntimeError("analysis_fast task is already claimed")
        run_uid = uuid.uuid4().hex
        execution_row = {
            **row,
            "_trigger_source": "release_catchup",
            "_scheduler_target_available": True,
            "_scheduler_target_trade_date": target,
            "_history_run_uid": run_uid,
        }
        scheduler_runtime._run_task(execution_row, ROOT, engine)
        with engine.connect() as connection:
            histories = connection.execute(text("""
                SELECT status, exit_code, build_sha, trigger_source
                FROM st_scheduled_task_history
                WHERE run_uid=:run_uid
                LIMIT 2
            """), {"run_uid": run_uid}).mappings().all()
            manifest = read_persisted_pool_manifest(connection, target)
        if len(histories) != 1:
            raise RuntimeError("analysis_fast terminal audit is unavailable")
        history = histories[0]
        publisher_uids = list(manifest.get("publisher_run_uids") or [])
        statuses = list(manifest.get("publication_statuses") or [])
        if (
            str(history.get("status") or "") != "success"
            or int(history.get("exit_code") or 0) != 0
            or str(history.get("build_sha") or "").lower() != build_sha
            or str(history.get("trigger_source") or "") != "release_catchup"
            or int(manifest.get("analysis_count") or 0) < 1000
            or (publisher_uids and publisher_uids != [run_uid])
            or statuses not in ([], ["ACTIVE"])
            or manifest.get("live_gate_alignment") is not True
        ):
            raise RuntimeError("analysis_fast terminal audit or activated pool differs")
        print(json.dumps(_result(
            status="COMPLETE",
            target=target,
            build_sha=build_sha,
            readiness=readiness,
            run_uid=run_uid,
            analysis_count=int(manifest["analysis_count"]),
            recommendation_count=int(manifest["recommendation_count"]),
            executable_count=int(manifest["executable_count"]),
            canonical_pool_sha256=str(manifest["canonical_pool_sha256"]),
        ), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
