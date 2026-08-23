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
from server.common.qmt_attestation_contract import (
    ATTESTATION_PROTOCOL_VERSION,
    UNIVERSE_MANIFEST_SCHEMA,
    validated_universe_manifest,
)
from tools.attest_qmt_daily_kline import (
    EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH,
    EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT,
    LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY,
    _legacy_completed_run_binding,
    attest_range,
    ensure_attestation_tables,
    legacy_completed_run_binding_plan,
    validate_legacy_completed_run_release_contract,
    validate_attestation_schema,
)
from tools.env_config import load_project_env


REQUIRED_GOVERNANCE_SESSIONS = 120


class GovernanceQmtHistoryNotReady(RuntimeError):
    """Raised when the immutable 120-session QMT evidence cannot be proven."""


_COMPLETED_RUN_BINDING_SQL = (
    "SELECT run_id, provider, start_date, end_date, target_rows, qmt_rows, "
    "matched_rows, missing_qmt_rows, mismatched_rows, "
    "already_attested_rows, updated_rows, tolerance_json "
    "FROM qmt_kline_attestation_run WHERE status='COMPLETED' "
    "ORDER BY start_date, end_date, run_id"
)


def _legacy_binding_plan_connection(
    connection,
    *,
    lock_rows: bool,
    expected_run_count: int,
    expected_plan_hash: str,
) -> dict[str, Any]:
    statement = _COMPLETED_RUN_BINDING_SQL + (" FOR UPDATE" if lock_rows else "")
    rows = connection.execute(text(statement)).mappings().all()
    legacy_rows: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        try:
            validated_universe_manifest(
                row.get("tolerance_json"),
                start_date=str(row.get("start_date") or "")[:10],
                end_date=str(row.get("end_date") or "")[:10],
            )
            continue
        except Exception:
            pass
        try:
            _legacy_completed_run_binding(row)
        except Exception as exc:
            raise GovernanceQmtHistoryNotReady(
                "COMPLETED认证行既不是当前V2清单，也不是唯一允许的旧协议"
            ) from exc
        legacy_rows.append(row)
    plan = legacy_completed_run_binding_plan(legacy_rows)
    if legacy_rows:
        try:
            validate_legacy_completed_run_release_contract(
                plan,
                expected_run_count=expected_run_count,
                expected_plan_hash=expected_plan_hash,
            )
        except ValueError as exc:
            raise GovernanceQmtHistoryNotReady(
                "旧协议不可授资行不符合本次发布冻结的计数与聚合哈希"
            ) from exc
    marker_rows = connection.execute(text(
        "SELECT migration_hash FROM qmt_kline_attestation_schema_migration "
        "WHERE migration_key=:migration_key"
    ), {
        "migration_key": LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY,
    }).mappings().all()
    marker_hash = (
        str(marker_rows[0].get("migration_hash") or "")
        if len(marker_rows) == 1 else ""
    )
    if len(marker_rows) > 1 or (
        marker_hash and marker_hash != plan["plan_hash"]
    ):
        raise GovernanceQmtHistoryNotReady(
            "旧协议不可授资绑定标记与当前历史行聚合哈希不一致"
        )
    if not legacy_rows and marker_rows:
        raise GovernanceQmtHistoryNotReady(
            "旧协议不可授资绑定标记没有对应历史行"
        )
    return {
        "legacy_run_count": int(plan["legacy_run_count"]),
        "legacy_binding_plan_hash": str(plan["plan_hash"]),
        "legacy_binding_marker_present": bool(marker_rows),
        "legacy_binding_pending": bool(legacy_rows and not marker_rows),
        "legacy_bindings": list(plan["runs"]),
    }


def plan_legacy_completed_run_binding(
    bind,
    *,
    expected_run_count: int = EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT,
    expected_plan_hash: str = EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH,
) -> dict[str, Any]:
    """Build the exact grandfather plan using SELECT statements only."""

    if hasattr(bind, "execute"):
        return _legacy_binding_plan_connection(
            bind,
            lock_rows=False,
            expected_run_count=expected_run_count,
            expected_plan_hash=expected_plan_hash,
        )
    with bind.connect() as connection:
        return _legacy_binding_plan_connection(
            connection,
            lock_rows=False,
            expected_run_count=expected_run_count,
            expected_plan_hash=expected_plan_hash,
        )


def apply_legacy_completed_run_binding(
    engine,
    *,
    expected_run_count: int = EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT,
    expected_plan_hash: str = EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH,
) -> dict[str, Any]:
    """Append one aggregate marker; never rewrite a historical run row."""

    with engine.begin() as connection:
        plan = _legacy_binding_plan_connection(
            connection,
            lock_rows=True,
            expected_run_count=expected_run_count,
            expected_plan_hash=expected_plan_hash,
        )
        if plan["legacy_binding_pending"]:
            connection.execute(text(
                "INSERT INTO qmt_kline_attestation_schema_migration "
                "(migration_key, migration_hash, completed_at) "
                "VALUES (:migration_key, :migration_hash, NOW())"
            ), {
                "migration_key": LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY,
                "migration_hash": plan["legacy_binding_plan_hash"],
            })
            plan = {
                **plan,
                "legacy_binding_marker_present": True,
                "legacy_binding_pending": False,
            }
        validate_attestation_schema(
            connection,
            expected_legacy_run_count=expected_run_count,
            expected_legacy_plan_hash=expected_plan_hash,
        )
    return plan


def prepare_attestation_schema(engine) -> dict[str, Any]:
    """Create and validate the trigger-free QMT V2 table/index schema."""

    ensure_attestation_tables(engine)
    detail = validate_attestation_schema(engine)
    return {
        "status": "ok",
        "mode": "schema-only",
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        "table_count": int(detail.get("table_count") or 0),
        "trigger_count": int(detail.get("trigger_count") or 0),
        "database_triggers_required": False,
        "immutability_enforcement": detail.get("immutability_enforcement"),
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
    # Production history preparation has no schema authority.  Schema-only
    # preparation must already have installed the frozen tables/indexes and the
    # legacy-ineligible marker.  Database triggers are not part of this gate.
    legacy_binding = plan_legacy_completed_run_binding(engine)
    if (
        legacy_binding["legacy_run_count"]
        and not legacy_binding["legacy_binding_marker_present"]
    ):
        raise GovernanceQmtHistoryNotReady(
            "旧协议不可授资绑定标记尚未由迁移账号准备"
        )
    validate_attestation_schema(engine)
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
        schema_prepared=True,
    )
    try:
        daily_universe = validated_universe_manifest(
            result.get("tolerances"),
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
        "legacy_binding": legacy_binding,
        "automatic_real_order_submission": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help=(
            "validate the preinstalled frozen QMT attestation schema without "
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
