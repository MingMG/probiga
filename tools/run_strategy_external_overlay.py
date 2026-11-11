#!/usr/bin/env python3
"""Prepare one frozen next-session strategy pool without replacing the first."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import prepare_frozen_strategy_pool_scoring
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE, STRATEGY_POOL_PREMARKET_PUBLICATION_DEADLINE,
    authoritative_elapsed_trade_date, resolve_strategy_pool_decision_slot,
)
from server.common.batch_db import create_batch_engine
from server.common.component_release import runtime_contract_build_sha
from server.common.daily_delivery_control import load_frozen_premarket_announcement, read_daily_delivery
from server.common.frozen_strategy_pool_work import PREMARKET_POOL_TASK, stage_frozen_pool_work
from server.common.scheduler_terminal_journal import TerminalJournal
from tools.run_strategy_governance_daily import _load_project_env


def _exact_running_audit(engine, *, run_uid: str, build_sha: str, task_id: str) -> None:
    if (
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or not task_id.isdecimal() or int(task_id) <= 0
        or os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") != PREMARKET_POOL_TASK
    ):
        raise RuntimeError("premarket pool requires its exact scheduled audit")
    with engine.connect() as connection:
        audit = connection.execute(text("""
            SELECT task_id, task_type, status, build_sha, trigger_source
              FROM st_scheduled_task_history WHERE run_uid=:uid
        """), {"uid": run_uid}).mappings().one_or_none()
    if audit is None or dict(audit) != {
        "task_id": int(task_id), "task_type": PREMARKET_POOL_TASK, "status": "running",
        "build_sha": build_sha, "trigger_source": "scheduled",
    }:
        raise RuntimeError("premarket pool scheduler claim differs")


def prepare_revision(engine, journal: TerminalJournal, *, run_uid: str, build_sha: str, task_id: str):
    _exact_running_audit(engine, run_uid=run_uid, build_sha=build_sha, task_id=task_id)
    now = datetime.now(PRODUCTION_TIMEZONE)
    target = authoritative_elapsed_trade_date(engine, now=now)
    slot = resolve_strategy_pool_decision_slot(
        engine, data_trade_date=target, phase="PREMARKET_REVISION", evaluated_at=now,
    )
    if slot.execution_session_date != now.date().isoformat() or now.time() >= STRATEGY_POOL_PREMARKET_PUBLICATION_DEADLINE:
        raise RuntimeError("premarket pool is outside the next session's evaluation window")
    contract = runtime_contract_build_sha(expected_build_sha=build_sha)
    initial = (read_daily_delivery(engine, trade_date=target, release_id=contract, attempt_limit=1) or {}).get("receipt")
    decision = (initial or {}).get("frozen_pool_decision") or {}
    if decision.get("phase") != "INITIAL" or decision.get("execution_session_date") != slot.execution_session_date:
        return None, target, "PREMARKET_INITIAL_POOL_UNAVAILABLE"
    source = load_frozen_premarket_announcement(engine, target, contract, slot)
    if source is None:
        return None, target, "PREMARKET_ANNOUNCEMENT_UNAVAILABLE_AT_SLOT"
    scoring = prepare_frozen_strategy_pool_scoring(
        engine, data_trade_date=target, phase="PREMARKET_REVISION",
        run_uid=run_uid, build_sha=build_sha, required_premarket_announcement=source,
    )
    return stage_frozen_pool_work(
        journal, run_uid=run_uid, build_sha=build_sha,
        parent_receipt_uid=initial["receipt_uid"],
        source_evidence_sha256=str(source.receipt["stage_evidence_sha256"]), scoring=scoring,
    ), target, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="08:30正式策略票池复核与修订")
    parser.add_argument("--json", action="store_true")
    parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    _load_project_env()
    run_uid = os.environ.get("PROBIGA_SCHEDULER_HISTORY_RUN_UID", "")
    build_sha = os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA", "")
    task_id = os.environ.get("PROBIGA_SCHEDULER_TASK_ID", "")
    log_root = Path(os.environ.get("PROBIGA_JOB_LOG_ROOT") or "/var/lib/probiga/jobs")
    engine = create_batch_engine()
    try:
        work, target, reason = prepare_revision(
            engine, TerminalJournal(log_root), run_uid=run_uid, build_sha=build_sha, task_id=task_id,
        )
    finally:
        engine.dispose()
    if work is None:
        print(json.dumps({
            "schema": "probiga.premarket-pool-skipped.v1", "status": "blocked",
            "trade_date": target, "reason_code": reason,
            "automatic_real_order_submission": False, "real_order_authority": False,
        }, ensure_ascii=False))
        return 2
    print(json.dumps(work, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
