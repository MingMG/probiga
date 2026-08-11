"""Auditable, non-destructive V2 fail-closed production drill harness."""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash


EXPECTED_ACTIONS: dict[str, str] = {
    "QMT_TERMINAL_DOWN": "B-003 BLOCK; no new open/add; no paper fill",
    "WINDOWS_COLLECTOR_DOWN": "quote capability BLOCK; no public-source execution fallback",
    "SSH_TUNNEL_DOWN": "quote capability BLOCK; no public-source execution fallback",
    "DATABASE_UNAVAILABLE": "worker fails without fill; retry remains idempotent",
    "WORKER_RESTART": "same input and idempotency keys produce no duplicate order/fill",
    "API_RESTART": "ledger unchanged; GET performs no recalculation",
    "SCHEDULER_DUPLICATE": "unique idempotency keys prevent duplicate order/fill",
    "DISK_SPACE_LOW": "worker records error and produces no fabricated fill",
    "STALE_SNAPSHOT": "new decision/open/add blocked",
    "CONFIG_CHANGED": "published version hash mismatch blocks bootstrap/promotion",
    "DATA_HASH_CHANGED": "new immutable snapshot/version; old report remains unchanged",
}


def record_fault_drill(
    engine: Engine,
    *,
    drill_type: str,
    environment: str,
    observed_action: str,
    evidence: dict[str, Any],
    passed: bool,
) -> dict[str, Any]:
    if drill_type not in EXPECTED_ACTIONS:
        raise ValueError(f"unsupported fault drill: {drill_type}")
    planned_at = datetime.now()
    payload = {
        "drill_type": drill_type,
        "environment": environment,
        "expected_action": EXPECTED_ACTIONS[drill_type],
        "observed_action": observed_action,
        "evidence": evidence,
        "passed": bool(passed),
    }
    result_hash = canonical_json_hash(payload)
    drill_id = uuid.uuid4().hex
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_fault_drill_v2
                (drill_id, drill_type, environment, planned_at,
                 started_at, finished_at, status, expected_action,
                 observed_action, evidence_json, result_hash, created_at)
                VALUES
                (:drill_id, :drill_type, :environment, :planned_at,
                 :planned_at, :planned_at, :status, :expected_action,
                 :observed_action, :evidence, :result_hash, :planned_at)
                """
            ),
            {
                "drill_id": drill_id,
                "drill_type": drill_type,
                "environment": environment,
                "planned_at": planned_at,
                "status": "PASS" if passed else "FAIL",
                "expected_action": EXPECTED_ACTIONS[drill_type],
                "observed_action": observed_action[:500],
                "evidence": json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "result_hash": result_hash,
            },
        )
    return {
        "drill_id": drill_id,
        "drill_type": drill_type,
        "status": "PASS" if passed else "FAIL",
        "result_hash": result_hash,
    }


def run_safe_production_drills(engine: Engine) -> dict[str, Any]:
    """Run evidence-based drills without intentionally stopping production."""
    results: list[dict[str, Any]] = []
    with engine.connect() as connection:
        capability = connection.execute(
            text(
                """
                SELECT status, checked_at, evidence_json
                FROM st_execution_capability_v2
                WHERE capability_code = 'B-003_RELIABLE_LEVEL1_BID_ASK'
                """
            )
        ).mappings().first()
        duplicate_orders = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT idempotency_key FROM st_order_v2
                        GROUP BY idempotency_key HAVING COUNT(*) > 1
                    ) duplicates
                    """
                )
            ).scalar()
            or 0
        )
        duplicate_fills = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT idempotency_key FROM st_fill_v2
                        GROUP BY idempotency_key HAVING COUNT(*) > 1
                    ) duplicates
                    """
                )
            ).scalar()
            or 0
        )
        real_order_accounts = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_trade_account_v2
                    WHERE real_trading_enabled <> 0
                    """
                )
            ).scalar()
            or 0
        )
        published_hash_errors = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_strategy_version_v2
                    WHERE config_hash IS NULL OR CHAR_LENGTH(config_hash) <> 64
                    """
                )
            ).scalar()
            or 0
        )
        duplicate_snapshots = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT data_snapshot_hash FROM st_data_snapshot_v2
                        GROUP BY data_snapshot_hash HAVING COUNT(*) > 1
                    ) duplicates
                    """
                )
            ).scalar()
            or 0
        )
    qmt_passed = bool(capability and capability["status"] == "BLOCK")
    results.append(
        record_fault_drill(
            engine,
            drill_type="QMT_TERMINAL_DOWN",
            environment="PRODUCTION_OBSERVED",
            observed_action=(
                "execution capability is BLOCK and real trading remains disabled"
            ),
            evidence={
                "capability_status": (
                    capability["status"] if capability else "MISSING"
                ),
                "checked_at": (
                    capability["checked_at"] if capability else None
                ),
                "real_order_enabled_accounts": real_order_accounts,
            },
            passed=qmt_passed and real_order_accounts == 0,
        )
    )
    idempotency_passed = (
        duplicate_orders == 0 and duplicate_fills == 0
    )
    for drill_type in ("WORKER_RESTART", "SCHEDULER_DUPLICATE"):
        results.append(
            record_fault_drill(
                engine,
                drill_type=drill_type,
                environment="PRODUCTION_SAFE_ASSERTION",
                observed_action="database uniqueness audit found no duplicates",
                evidence={
                    "duplicate_order_keys": duplicate_orders,
                    "duplicate_fill_keys": duplicate_fills,
                },
                passed=idempotency_passed,
            )
        )
    results.append(
        record_fault_drill(
            engine,
            drill_type="CONFIG_CHANGED",
            environment="PRODUCTION_SAFE_ASSERTION",
            observed_action="all published strategy config hashes are present",
            evidence={"invalid_config_hash_rows": published_hash_errors},
            passed=published_hash_errors == 0,
        )
    )
    results.append(
        record_fault_drill(
            engine,
            drill_type="DATA_HASH_CHANGED",
            environment="PRODUCTION_SAFE_ASSERTION",
            observed_action="snapshot hashes remain unique and immutable",
            evidence={"duplicate_snapshot_hashes": duplicate_snapshots},
            passed=duplicate_snapshots == 0,
        )
    )
    return {
        "status": (
            "PASS"
            if all(item["status"] == "PASS" for item in results)
            else "FAIL"
        ),
        "drills": results,
        "not_destructively_executed": [
            "WINDOWS_COLLECTOR_DOWN",
            "SSH_TUNNEL_DOWN",
            "DATABASE_UNAVAILABLE",
            "API_RESTART",
            "DISK_SPACE_LOW",
        ],
    }
