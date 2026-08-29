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
import hashlib
import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date as resolve_authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine
from server.common.qmt_attestation_contract import (
    AMOUNT_REL_TOLERANCE,
    ATTESTATION_PROTOCOL_VERSION,
    PRICE_TOLERANCE,
    QMT_ATTESTATION_COLLATION,
    UNIVERSE_MANIFEST_SCHEMA,
    VOLUME_ABSOLUTE_TOLERANCE,
    VOLUME_REL_TOLERANCE,
    validated_universe_manifest,
)
from server.common.qmt_stock_catalog import a_share_stock_code_sql
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from tools.attest_qmt_daily_kline import (
    EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH,
    EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT,
    LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY,
    PROVIDER_ID,
    _legacy_completed_run_binding,
    _match_sql,
    _table_names,
    attest_range,
    privileged_migrate_attestation_tables,
    legacy_completed_run_binding_plan,
    validate_legacy_completed_run_release_contract,
    validate_attestation_schema,
)
from tools.env_config import load_project_env


REQUIRED_GOVERNANCE_SESSIONS = 120
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SESSION_WINDOW_DOMAIN = b"probiga.qmt-governance-session-window.v1\x00"


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

    privileged_migrate_attestation_tables(engine)
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


def _calendar_decision_time(now: datetime | None = None) -> datetime:
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    return current.replace(microsecond=0)


def authoritative_closed_trade_date(
    engine,
    now: datetime | None = None,
) -> str:
    """Resolve the shared closed day and bind it to an immutable QMT root."""

    decision_time = _calendar_decision_time(now)
    target_trade_date = resolve_authoritative_closed_trade_date(
        engine,
        now=decision_time,
    )
    if not target_trade_date:
        raise GovernanceQmtHistoryNotReady(
            "共享权威市场时钟未能解析已收盘交易日"
        )
    try:
        target = date.fromisoformat(target_trade_date)
    except (TypeError, ValueError) as exc:
        raise GovernanceQmtHistoryNotReady(
            "共享权威市场时钟返回了无效交易日"
        ) from exc
    if target > decision_time.date():
        raise GovernanceQmtHistoryNotReady(
            "共享权威市场时钟返回了未来交易日"
        )
    start_date = (target - timedelta(days=550)).isoformat()
    try:
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start_date,
                end_date=target_trade_date,
                decision_known_at=decision_time,
            )
    except Exception as exc:
        raise GovernanceQmtHistoryNotReady(
            "没有在决策时点已知且覆盖治理窗口的不可变QMT交易日历"
        ) from exc
    candidates = receipt.sessions_between(start_date, target_trade_date)
    if not candidates or candidates[-1] != target_trade_date:
        raise GovernanceQmtHistoryNotReady(
            "不可变QMT交易日历未包含共享时钟解析的目标交易日"
        )
    return target_trade_date


def _latest_closed_sessions(
    engine,
    *,
    target_trade_date: str,
    required_sessions: int = REQUIRED_GOVERNANCE_SESSIONS,
) -> list[str]:
    if required_sessions != REQUIRED_GOVERNANCE_SESSIONS:
        raise ValueError("策略治理历史窗口固定为120个权威交易日")
    try:
        target = date.fromisoformat(target_trade_date)
    except (TypeError, ValueError) as exc:
        raise GovernanceQmtHistoryNotReady(
            "QMT治理目标交易日无效"
        ) from exc
    start_date = (target - timedelta(days=550)).isoformat()
    try:
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start_date,
                end_date=target_trade_date,
                decision_known_at=_calendar_decision_time(),
            )
        eligible_sessions = receipt.sessions_between(
            start_date, target_trade_date
        )
    except Exception as exc:
        raise GovernanceQmtHistoryNotReady(
            "不可变QMT交易日历未覆盖120会话治理窗口"
        ) from exc
    sessions = eligible_sessions[-required_sessions:]
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


def _session_window_sha256(sessions: list[str]) -> str:
    if (
        len(sessions) != REQUIRED_GOVERNANCE_SESSIONS
        or sessions != sorted(set(sessions))
    ):
        raise GovernanceQmtHistoryNotReady("QMT治理会话窗口不是120个有序唯一交易日")
    hasher = hashlib.sha256(_SESSION_WINDOW_DOMAIN)
    for value in sessions:
        try:
            canonical = date.fromisoformat(value).isoformat()
        except (TypeError, ValueError) as exc:
            raise GovernanceQmtHistoryNotReady("QMT治理会话窗口含无效交易日") from exc
        if canonical != value:
            raise GovernanceQmtHistoryNotReady("QMT治理会话窗口含非规范交易日")
        hasher.update(value.encode("ascii"))
        hasher.update(b"\x00")
    return hasher.hexdigest()


def preflight_governance_qmt_history_readiness(
    engine,
    *,
    table_resolver: Callable[[Any], tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Prove source coverage without creating ledgers or changing market rows."""

    validate_attestation_schema(engine)
    target_trade_date = authoritative_closed_trade_date(engine)
    if not target_trade_date:
        raise GovernanceQmtHistoryNotReady("权威交易日历没有已收盘交易日")
    sessions = _latest_closed_sessions(
        engine,
        target_trade_date=target_trade_date,
    )
    target_table, source_table = (table_resolver or _table_names)(engine)
    params = {
        "provider": PROVIDER_ID,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "price_tolerance": PRICE_TOLERANCE,
        "volume_absolute_tolerance": VOLUME_ABSOLUTE_TOLERANCE,
        "volume_rel_tolerance": VOLUME_REL_TOLERANCE,
        "amount_rel_tolerance": AMOUNT_REL_TOLERANCE,
    }
    match_sql = _match_sql("t", "q")
    unqualified_a_share = a_share_stock_code_sql("stock_code")
    target_a_share = a_share_stock_code_sql("t.stock_code")
    source_a_share = a_share_stock_code_sql("q.stock_code")
    with engine.connect() as connection:
        target_rows = connection.execute(
            text(
                f"""
                SELECT trade_date, COUNT(*) AS row_count,
                       COUNT(DISTINCT stock_code) AS unique_stock_count
                FROM {target_table}
                WHERE trade_date BETWEEN :start_date AND :end_date
                  AND k_type=1 AND adjust_type=0
                  AND {unqualified_a_share}
                GROUP BY trade_date
                ORDER BY trade_date
                """
            ),
            params,
        ).mappings().all()
        source_rows = connection.execute(
            text(
                f"""
                SELECT trade_date, COUNT(*) AS row_count,
                       COUNT(DISTINCT stock_code) AS unique_stock_count,
                       SUM(CASE
                             WHEN BINARY pre_close_origin=BINARY 'NATIVE_QMT'
                              AND pre_close IS NOT NULL AND pre_close > 0
                             THEN 1 ELSE 0
                           END) AS native_row_count
                FROM {source_table}
                WHERE trade_date BETWEEN :start_date AND :end_date
                  AND period='1d' AND k_type=1 AND adjust_type=0
                  AND provider=:provider
                  AND {unqualified_a_share}
                GROUP BY trade_date
                ORDER BY trade_date
                """
            ),
            params,
        ).mappings().all()
        exact_rows = connection.execute(
            text(
                f"""
                SELECT trade_date,
                       COUNT(*) AS joined_pair_count,
                       COUNT(DISTINCT target_id) AS joined_target_count,
                       COUNT(DISTINCT source_id) AS joined_source_count,
                       COUNT(DISTINCT CASE WHEN is_match
                                          THEN target_id END)
                           AS matched_target_count,
                       COUNT(DISTINCT CASE WHEN is_match
                                          THEN source_id END)
                           AS matched_source_count
                FROM (
                    SELECT t.id AS target_id, q.id AS source_id,
                           t.trade_date AS trade_date,
                           ({match_sql}) AS is_match
                    FROM {target_table} t
                    INNER JOIN {source_table} q
                      ON q.stock_code COLLATE {QMT_ATTESTATION_COLLATION}
                         =t.stock_code
                     AND q.trade_date=t.trade_date
                     AND q.period='1d' AND q.k_type=1 AND q.adjust_type=0
                     AND q.provider=:provider
                    WHERE t.trade_date BETWEEN :start_date AND :end_date
                      AND t.k_type=1 AND t.adjust_type=0
                      AND {target_a_share}
                      AND {source_a_share}
                ) exact_match_rows
                GROUP BY trade_date
                ORDER BY trade_date
                """
            ),
            params,
        ).mappings().all()

    def keyed(
        rows: list[Any], *, evidence_name: str,
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for raw_row in rows:
            row = dict(raw_row)
            trade_date = str(row.get("trade_date") or "")[:10]
            if len(trade_date) != 10 or trade_date in result:
                raise GovernanceQmtHistoryNotReady(
                    f"QMT发布前{evidence_name}证据包含无效或重复交易日，"
                    "拒绝进入停服务切换"
                )
            result[trade_date] = row
        return result

    target_by_day = keyed(target_rows, evidence_name="目标")
    source_by_day = keyed(source_rows, evidence_name="来源")
    exact_by_day = keyed(exact_rows, evidence_name="精确匹配")
    expected_days = set(sessions)
    exact_days = (
        set(target_by_day)
        == expected_days
        == set(source_by_day)
        == set(exact_by_day)
    )

    def day_is_exact(day: str) -> bool:
        target = target_by_day[day]
        source = source_by_day[day]
        exact = exact_by_day[day]
        target_count = int(target.get("row_count") or 0)
        source_count = int(source.get("row_count") or 0)
        return bool(
            target_count > 0
            and int(target.get("unique_stock_count") or 0) == target_count
            and source_count == target_count
            and int(source.get("unique_stock_count") or 0) == source_count
            and int(source.get("native_row_count") or 0) == source_count
            and int(exact.get("joined_pair_count") or 0) == target_count
            and int(exact.get("joined_target_count") or 0) == target_count
            and int(exact.get("joined_source_count") or 0) == source_count
            and int(exact.get("matched_target_count") or 0) == target_count
            and int(exact.get("matched_source_count") or 0) == source_count
        )

    if not exact_days or not all(day_is_exact(day) for day in sessions):
        raise GovernanceQmtHistoryNotReady(
            "QMT来源尚未以相同股票集合、正式冻结容差内的OHLC/量额和"
            "原生preClose逐日精确覆盖120个权威交易日，"
            "拒绝进入停服务切换"
        )
    target_total = sum(
        int(target_by_day[day].get("row_count") or 0) for day in sessions
    )
    return {
        "status": "ok",
        "mode": "readiness-only",
        "target_trade_date": target_trade_date,
        "session_count": len(sessions),
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "session_window_sha256": _session_window_sha256(sessions),
        "target_rows": target_total,
        "native_qmt_rows": target_total,
        "exact_matched_rows": target_total,
        "database_writes": False,
        "automatic_real_order_submission": False,
    }


def prepare_governance_qmt_history(
    engine,
    *,
    expected_target_trade_date: str,
    expected_start_date: str,
    expected_end_date: str,
    expected_session_window_sha256: str,
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
    try:
        expected_dates = tuple(
            date.fromisoformat(value).isoformat()
            for value in (
                expected_target_trade_date,
                expected_start_date,
                expected_end_date,
            )
        )
    except (TypeError, ValueError) as exc:
        raise GovernanceQmtHistoryNotReady("冻结的QMT治理窗口日期无效") from exc
    if (
        expected_dates
        != (
            expected_target_trade_date,
            expected_start_date,
            expected_end_date,
        )
        or expected_end_date != expected_target_trade_date
        or not _LOWER_SHA256_RE.fullmatch(expected_session_window_sha256 or "")
        or target_trade_date != expected_target_trade_date
    ):
        raise GovernanceQmtHistoryNotReady("QMT治理窗口在发布切换期间发生漂移")
    sessions = _latest_closed_sessions(
        engine,
        target_trade_date=expected_target_trade_date,
    )
    if (
        sessions[0] != expected_start_date
        or sessions[-1] != expected_end_date
        or _session_window_sha256(sessions) != expected_session_window_sha256
    ):
        raise GovernanceQmtHistoryNotReady("QMT治理120会话窗口与发布前冻结证据不一致")
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--schema-only",
        action="store_true",
        help=(
            "validate the preinstalled frozen QMT attestation schema without "
            "reading or writing market history"
        ),
    )
    parser.add_argument("--expected-target-trade-date")
    parser.add_argument("--expected-start-date")
    parser.add_argument("--expected-end-date")
    parser.add_argument("--expected-session-window-sha256")
    mode.add_argument(
        "--readiness-only",
        action="store_true",
        help=(
            "read-only proof that native-QMT source counts cover the exact "
            "120-session target before service cutover"
        ),
    )
    parser.add_argument(
        "--windows-local-option-file",
        action="store_true",
        help=(
            "For --readiness-only, use the fixed protected Windows MySQL "
            "option file for primary and QMT history schemas."
        ),
    )
    args = parser.parse_args(argv)
    if args.windows_local_option_file and not args.readiness_only:
        parser.error(
            "--windows-local-option-file is only valid with --readiness-only"
        )
    load_project_env()
    local_history_engine = None
    if args.windows_local_option_file:
        from tools.backfill_guojin_qmt_local_history import (
            _windows_local_engines,
        )

        engine, local_history_engine = _windows_local_engines()
    else:
        engine = create_batch_engine(future=True)
    try:
        try:
            if args.schema_only:
                result = prepare_attestation_schema(engine)
            elif args.readiness_only:
                table_resolver = None
                if local_history_engine is not None:
                    table_resolver = lambda target_engine: _table_names(
                        target_engine,
                        local_history_engine=local_history_engine,
                    )
                result = preflight_governance_qmt_history_readiness(
                    engine,
                    table_resolver=table_resolver,
                )
            else:
                result = prepare_governance_qmt_history(
                    engine,
                    expected_target_trade_date=args.expected_target_trade_date,
                    expected_start_date=args.expected_start_date,
                    expected_end_date=args.expected_end_date,
                    expected_session_window_sha256=(
                        args.expected_session_window_sha256
                    ),
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
        if local_history_engine is not None:
            local_history_engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
