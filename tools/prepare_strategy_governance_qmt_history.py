#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare the exact 120-session QMT V2 evidence required by governance.

This is a deployment gate, not a best-effort backfill. The first governance
run must not start until every A-share row in every required session has an
immutable native-QMT ``preClose`` attestation and the source/target universes
are exactly equal.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine
from tools.attest_qmt_daily_kline import (
    ATTESTATION_PROTOCOL_VERSION,
    UNIVERSE_MANIFEST_SCHEMA,
    attest_range,
    ensure_attestation_tables,
    validate_attestation_schema,
    validated_universe_manifest,
)
from tools.env_config import load_project_env


REQUIRED_GOVERNANCE_SESSIONS = 120


class GovernanceQmtHistoryNotReady(RuntimeError):
    """Raised when the immutable 120-session QMT evidence cannot be proven."""


def prepare_attestation_schema(engine) -> dict[str, Any]:
    """Install the one-way legacy migration and prove the frozen V2 schema.

    Deployment invokes this while the existing runtime is still active.  The
    later writer-fenced attestation invokes the same installer again, so any
    drift between preparation and cutover also fails closed.
    """

    ensure_attestation_tables(engine)
    detail = validate_attestation_schema(engine)
    return {
        "status": "ok",
        "mode": "schema-only",
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        "table_count": int(detail.get("table_count") or 0),
        "trigger_count": int(detail.get("trigger_count") or 0),
        "automatic_real_order_submission": False,
    }


def _latest_closed_sessions(
    engine,
    *,
    target_trade_date: str,
    required_sessions: int = REQUIRED_GOVERNANCE_SESSIONS,
) -> list[str]:
    if required_sessions != REQUIRED_GOVERNANCE_SESSIONS:
        raise ValueError("策略治理历史窗口固定为120个权威交易日")
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT trade_date FROM si_trade_calendar "
                "WHERE trade_status=1 AND trade_date<=:target_trade_date "
                "ORDER BY trade_date DESC LIMIT 120"
            ),
            {"target_trade_date": target_trade_date},
        ).mappings().all()
    sessions = sorted(str(row.get("trade_date") or "")[:10] for row in rows)
    if (
        len(sessions) != required_sessions
        or len(set(sessions)) != required_sessions
        or any(len(value) != 10 for value in sessions)
        or sessions[-1] != target_trade_date
    ):
        raise GovernanceQmtHistoryNotReady(
            "权威交易日历不足120个唯一已收盘会话，拒绝生成不完整治理证据"
        )
    return sessions


def prepare_governance_qmt_history(
    engine,
    *,
    attester: Callable[..., dict[str, Any]] = attest_range,
) -> dict[str, Any]:
    target_trade_date = authoritative_closed_trade_date(engine)
    if not target_trade_date:
        raise GovernanceQmtHistoryNotReady("权威交易日历没有已收盘交易日")
    sessions = _latest_closed_sessions(
        engine,
        target_trade_date=target_trade_date,
    )
    result = attester(
        engine,
        start_date=sessions[0],
        end_date=sessions[-1],
        apply=True,
    )
    try:
        daily_universe = validated_universe_manifest(
            result,
            start_date=sessions[0],
            end_date=sessions[-1],
        )
    except (TypeError, ValueError):
        daily_universe = {}
    target_rows = int(result.get("target_rows") or 0)
    exact_universe_days = (
        set(daily_universe) == set(sessions)
        and sum(
            int(contract["stock_count"])
            for contract in daily_universe.values()
        )
        == target_rows
    )
    run_id = str(result.get("run_id") or "").strip()
    valid = (
        bool(run_id)
        and result.get("universe_manifest_schema")
        == UNIVERSE_MANIFEST_SCHEMA
        and result.get("status") == "COMPLETED"
        and result.get("apply") is True
        and result.get("attestation_protocol")
        == ATTESTATION_PROTOCOL_VERSION
        and str(result.get("start_date") or "") == sessions[0]
        and str(result.get("end_date") or "") == sessions[-1]
        and target_rows > 0
        and int(result.get("qmt_rows") or 0) == target_rows
        and int(result.get("matched_rows") or 0) == target_rows
        and int(result.get("missing_qmt_rows") or 0) == 0
        and int(result.get("mismatched_rows") or 0) == 0
        and int(result.get("source_only_rows") or 0) == 0
        and exact_universe_days
    )
    if not valid:
        raise GovernanceQmtHistoryNotReady(
            "QMT V2历史认证未完整覆盖120个权威交易日，拒绝首次治理运行"
        )
    return {
        "status": "ok",
        "target_trade_date": target_trade_date,
        "session_count": len(sessions),
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "attestation_run_id": run_id,
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        "target_rows": target_rows,
        "matched_rows": int(result.get("matched_rows") or 0),
        "daily_universe_count": len(daily_universe),
        "automatic_real_order_submission": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help=(
            "install/validate the frozen QMT attestation schema without "
            "reading or writing market history"
        ),
    )
    args = parser.parse_args(argv)
    load_project_env()
    engine = create_batch_engine(future=True)
    try:
        try:
            result = (
                prepare_attestation_schema(engine)
                if args.schema_only
                else prepare_governance_qmt_history(engine)
            )
        except Exception as exc:
            print(json.dumps({
                "status": "blocked",
                "reason": f"{type(exc).__name__}: {str(exc)[:500]}",
                "required_session_count": REQUIRED_GOVERNANCE_SESSIONS,
                "automatic_real_order_submission": False,
            }, ensure_ascii=False))
            return 2
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
