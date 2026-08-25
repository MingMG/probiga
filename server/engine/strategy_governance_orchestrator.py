# -*- coding: utf-8 -*-
"""One authoritative orchestration path for scheduled and manual governance."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import uuid
from datetime import date, datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import (
    DAILY_CLOSE_READY_HOUR,
    authoritative_closed_trade_date,
)
from server.engine.strategy_execution_adapters import (
    strategy_execution_adapter_capabilities,
)
from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
    capture_industry_history,
)
from server.common.governance_safety import (
    assert_real_order_authority_closed,
)


logger = logging.getLogger(__name__)
EXCHANGE_TIMEZONE = ZoneInfo("Asia/Shanghai")

COMPLETED = "COMPLETED"
NOT_DUE = "NOT_DUE"
NOT_READY = "NOT_READY"
INTEGRITY_ERROR = "INTEGRITY_ERROR"
PROGRAM_ERROR = "PROGRAM_ERROR"

_RUN_UID_PATTERN = re.compile(r"[0-9a-f]{32}")
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}")
_STATISTICAL_DECISION_CONTRACT = "strategy-governance-decision.v7"


def _log_incident(*, level: str, stage: str, exc: BaseException) -> str:
    """Log only a correlation id, exception class and bounded stage.

    Database and adapter exception text or tracebacks may contain connection
    URLs, credentials, SQL parameters or vendor payloads.  They are never
    emitted by the governance boundary.
    """

    incident_id = uuid.uuid4().hex
    log_method = getattr(logger, level)
    log_method(
        "strategy-governance incident_id=%s error_type=%s stage=%s",
        incident_id,
        type(exc).__name__,
        str(stage or "UNKNOWN")[:64],
    )
    return incident_id


def validate_governance_safety_contract(result: Any) -> None:
    """Fail closed unless a governance result is explicitly paper-only."""

    if not isinstance(result, dict):
        raise ValueError("治理结果必须是对象")
    if result.get("automatic_real_order_submission") is not False:
        raise ValueError("治理结果未显式关闭自动真实下单")
    if result.get("real_order_authority") is not False:
        raise ValueError("治理结果未显式关闭真实下单授权")
    allocations = result.get("allocations")
    if not isinstance(allocations, list) or not allocations:
        raise ValueError("治理结果缺少显式模拟资金分配")
    total_weight = 0.0
    for index, allocation in enumerate(allocations):
        if not isinstance(allocation, dict):
            raise ValueError(f"治理资金分配第{index + 1}行不是对象")
        if allocation.get("real_order_authority") is not False:
            raise ValueError(f"治理资金分配第{index + 1}行未关闭真实下单权限")
        raw_weight = allocation.get("simulated_weight_pct")
        if isinstance(raw_weight, bool):
            raise ValueError(f"治理资金分配第{index + 1}行权重无效")
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"治理资金分配第{index + 1}行权重无效"
            ) from exc
        if not math.isfinite(weight) or weight < 0 or weight > 100:
            raise ValueError(f"治理资金分配第{index + 1}行权重越界")
        total_weight += weight
    if abs(total_weight - 100.0) > 0.000001:
        raise ValueError("治理模拟资金权重未精确闭合为100%")
    assert_real_order_authority_closed(result)


def validate_governance_completion_contract(
    result: Any, *, target_trade_date: str,
    expected_build_sha: str = "",
) -> None:
    """Validate the persisted canonical identity before claiming completion."""

    if not isinstance(result, dict):
        raise ValueError("治理完成结果必须是对象")
    if result.get("status") != "ok":
        raise ValueError("治理完成结果状态不是ok")
    if not _RUN_UID_PATTERN.fullmatch(str(result.get("run_uid") or "")):
        raise ValueError("治理完成结果缺少规范run_uid")
    if str(result.get("trade_date") or "") != str(target_trade_date or ""):
        raise ValueError("治理完成结果交易日与目标交易日不一致")
    if result.get("is_canonical") is not True:
        raise ValueError("治理完成结果没有canonical身份")
    if result.get("result_mode") != "CANONICAL_PERSISTED":
        raise ValueError("治理完成结果不是已持久化canonical结果")
    if (
        result.get("decision_contract_version")
        != _STATISTICAL_DECISION_CONTRACT
        or result.get("statistical_funding_eligible") is not True
    ):
        raise ValueError("治理完成结果不是可授资的v7统计决策合同")
    if not isinstance(result.get("summary"), dict):
        raise ValueError("治理完成结果缺少汇总")
    build_sha = result.get("build_commit_sha")
    if not isinstance(build_sha, str) or not build_sha or len(build_sha) > 64:
        raise ValueError("治理完成结果缺少规范构建版本")
    if expected_build_sha and build_sha != expected_build_sha:
        raise ValueError("治理完成结果构建版本与发布要求不一致")
    if not _HASH_PATTERN.fullmatch(
        str(result.get("canonical_result_hash") or "")
    ):
        raise ValueError("治理完成结果缺少canonical结果哈希")
    from server.engine.strategy_governance import (
        validate_funding_checkpoint_manifest_contract,
    )

    validate_funding_checkpoint_manifest_contract(result)


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _exchange_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(EXCHANGE_TIMEZONE)
    if value.tzinfo is None:
        return value.replace(tzinfo=EXCHANGE_TIMEZONE)
    return value.astimezone(EXCHANGE_TIMEZONE)


def _cash_allocation(reason: str) -> list[dict[str, Any]]:
    return [{
        "target_type": "CASH",
        "target_key": "cash",
        "name": "现金",
        "simulated_weight_pct": 100.0,
        "reason": reason,
        "real_order_authority": False,
    }]


def _calendar_status(engine, trade_date: str) -> int | None:
    with engine.connect() as connection:
        value = connection.execute(text(
            "SELECT trade_status FROM si_trade_calendar "
            "WHERE trade_date=:trade_date LIMIT 1"
        ), {"trade_date": trade_date}).scalar()
    return None if value is None else int(value)


def _canonical_run(engine, trade_date: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(text("""
            SELECT run_uid, trade_date, run_revision, decision_hash,
                   build_commit_sha, finished_at
            FROM st_strategy_governance_run
            WHERE trade_date=:trade_date AND status='COMPLETED'
              AND is_canonical=1
            ORDER BY run_revision DESC, finished_at DESC
            LIMIT 1
        """), {"trade_date": trade_date}).mappings().first()
    return dict(row) if row is not None else None


def _exact_canonical_governance_readback(
    engine, *, runner_result: dict[str, Any], target_trade_date: str,
    expected_build_sha: str = "",
) -> dict[str, Any]:
    """Bind a successful persistent runner receipt to one exact DB row."""

    run_uid = str(runner_result.get("run_uid") or "")
    runner_hash = str(runner_result.get("canonical_result_hash") or "")
    runner_build_sha = str(runner_result.get("build_commit_sha") or "")
    if (
        runner_result.get("status") != "ok"
        or runner_result.get("is_canonical") is not True
        or runner_result.get("result_mode") != "CANONICAL_PERSISTED"
        or str(runner_result.get("trade_date") or "")
        != target_trade_date
        or not _RUN_UID_PATTERN.fullmatch(run_uid)
        or not _HASH_PATTERN.fullmatch(runner_hash)
        or not runner_build_sha
        or len(runner_build_sha) > 64
    ):
        raise RuntimeError("治理运行器没有返回可精确回读的canonical身份")
    try:
        with engine.connect() as connection:
            rows = connection.execute(text("""
                SELECT run_uid, trade_date, input_hash, decision_hash,
                       build_commit_sha, status, is_canonical,
                       result_json, result_hash
                FROM st_strategy_governance_run
                WHERE run_uid=:run_uid AND trade_date=:trade_date
            """), {
                "run_uid": run_uid,
                "trade_date": target_trade_date,
            }).mappings().all()
    except Exception as exc:
        raise RuntimeError("canonical治理结果数据库精确回读失败") from exc
    if len(rows) != 1:
        raise RuntimeError("canonical治理结果数据库精确回读不唯一")
    row = dict(rows[0])
    if (
        str(row.get("run_uid") or "") != run_uid
        or str(row.get("trade_date") or "")[:10] != target_trade_date
        or str(row.get("status") or "") != COMPLETED
        or row.get("is_canonical") not in (1, True)
        or str(row.get("result_hash") or "") != runner_hash
        or str(row.get("build_commit_sha") or "") != runner_build_sha
        or str(row.get("input_hash") or "")
        != str(runner_result.get("input_hash") or "")
        or str(row.get("decision_hash") or "")
        != str(runner_result.get("decision_hash") or "")
        or (expected_build_sha and runner_build_sha != expected_build_sha)
    ):
        raise RuntimeError("canonical治理结果数据库身份与运行器回执不一致")
    from server.engine.strategy_governance import (
        _canonical_governance_result_from_row,
    )

    readback = _canonical_governance_result_from_row(row)
    if (
        str(readback.get("canonical_result_hash") or "") != runner_hash
        or str(readback.get("run_uid") or "") != run_uid
        or str(readback.get("trade_date") or "") != target_trade_date
        or str(readback.get("build_commit_sha") or "") != runner_build_sha
    ):
        raise RuntimeError("canonical治理完整回读与运行器回执不一致")
    return readback


def canonical_unavailable_context(
    *, engine=None, now: datetime | None = None,
) -> dict[str, Any]:
    """Best-effort public context without leaking the underlying DB error."""

    authoritative = ""
    last_canonical: dict[str, Any] = {}
    try:
        database = engine or get_engine()
    except Exception as exc:
        _log_incident(
            level="warning", stage="CANONICAL_CONTEXT_ENGINE", exc=exc,
        )
        return {
            "authoritative_trade_date": authoritative,
            "last_canonical": last_canonical,
        }
    try:
        authoritative = authoritative_closed_trade_date(
            database, now=_exchange_now(now),
        )
    except Exception as exc:
        _log_incident(
            level="warning", stage="CANONICAL_CONTEXT_CALENDAR", exc=exc,
        )
    try:
        with database.connect() as connection:
            row = connection.execute(text("""
                SELECT run_uid, trade_date, run_revision, source_status,
                       input_ready, decision_hash, strategy_count,
                       combination_count, tradable_count, allocation_count,
                       finished_at
                FROM st_strategy_governance_run
                WHERE status='COMPLETED' AND is_canonical=1
                ORDER BY trade_date DESC, run_revision DESC, finished_at DESC
                LIMIT 1
            """)).mappings().first()
        if row is not None:
            last_canonical = dict(row)
    except Exception as exc:
        _log_incident(
            level="warning", stage="CANONICAL_CONTEXT_LAST_RUN", exc=exc,
        )
    return {
        "authoritative_trade_date": authoritative,
        "last_canonical": last_canonical,
    }


def _attempt_payload(
    result: dict[str, Any], *, operator: str,
) -> tuple[str, str, dict[str, Any], dict[str, Any], dict[str, Any]]:
    stored_operator = str(operator or "daily_governance")[:80]
    evidence = {
        "schema": "probiga.strategy-governance-attempt.v1",
        "orchestration_status": result["orchestration_status"],
        "error_class": result["error_class"],
        "retryable": result["retryable"],
        "reason_code": result["reason_code"],
        "blocking_stage": result["blocking_stage"],
        "target_trade_date": result.get("target_trade_date") or "",
        "requested_trade_date": result.get("requested_trade_date") or "",
        "input_trade_date": result.get("input_trade_date") or "",
        "blocking_record": result.get("blocking_record") or {},
        "industry_snapshot": result.get("industry_snapshot") or {},
        "adapter_registry_seal_hash": result.get(
            "adapter_registry_seal_hash"
        ) or "",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    attempt_hash = _digest({
        "schema": evidence["schema"],
        "operator": stored_operator,
        **evidence,
    })
    before: dict[str, Any] = {}
    after = {
        "status": "BLOCKED",
        "orchestration_status": result["orchestration_status"],
        "retryable": result["retryable"],
        "target_trade_date": result.get("target_trade_date") or "",
    }
    payload = {
        "entity_type": "SYSTEM",
        "entity_key": "strategy_governance_daily",
        "action": "RUN_BLOCKED",
        "reason": str(result.get("reason") or "")[:500],
        "operator": stored_operator,
        "before": before,
        "after": after,
        "evidence": {**evidence, "attempt_hash": attempt_hash},
        "nonce": attempt_hash[32:64],
    }
    return attempt_hash[:32], _digest(payload), payload, before, after


def persist_blocked_attempt(
    engine, result: dict[str, Any], *, operator: str,
) -> dict[str, Any]:
    """Append one deterministic blocked-attempt audit; exact retries dedupe."""

    audit_id, audit_hash, payload, before, after = _attempt_payload(
        result, operator=operator,
    )
    params = {
        "audit_id": audit_id,
        "entity_type": payload["entity_type"],
        "entity_key": payload["entity_key"],
        "action": payload["action"],
        "reason": payload["reason"],
        "operator": payload["operator"],
        "before_json": _json(before),
        "after_json": _json(after),
        "evidence_json": _json(payload["evidence"]),
        "payload_json": _json(payload),
        "audit_hash": audit_hash,
    }

    def existing_receipt() -> dict[str, Any] | None:
        with engine.connect() as connection:
            row = connection.execute(text("""
                SELECT audit_id, audit_hash
                FROM st_strategy_governance_audit
                WHERE audit_id=:audit_id OR audit_hash=:audit_hash
                LIMIT 1
            """), {
                "audit_id": audit_id,
                "audit_hash": audit_hash,
            }).mappings().first()
        if row is None:
            return None
        if (
            str(row.get("audit_id") or "") != audit_id
            or str(row.get("audit_hash") or "") != audit_hash
        ):
            raise IndustrySnapshotIntegrityError(
                "治理阻断回执身份发生哈希冲突"
            )
        return {
            "audit_id": audit_id,
            "audit_hash": audit_hash,
            "idempotent_replay": True,
        }

    receipt = existing_receipt()
    if receipt is not None:
        return receipt
    try:
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO st_strategy_governance_audit
                (audit_id, entity_type, entity_key, action, reason,
                 operator_name, before_json, after_json, evidence_json,
                 payload_json, audit_hash)
                VALUES
                (:audit_id, :entity_type, :entity_key, :action, :reason,
                 :operator, :before_json, :after_json, :evidence_json,
                 :payload_json, :audit_hash)
            """), params)
    except IntegrityError:
        receipt = existing_receipt()
        if receipt is not None:
            return receipt
        raise
    return {
        "audit_id": audit_id,
        "audit_hash": audit_hash,
        "idempotent_replay": False,
    }


def _adapter_seal_hash() -> str:
    try:
        return str(
            strategy_execution_adapter_capabilities().get(
                "registry_seal_hash"
            ) or ""
        )
    except Exception:
        return ""


def _blocked_result(
    engine,
    *,
    orchestration_status: str,
    error_class: str,
    retryable: bool,
    reason_code: str,
    blocking_stage: str,
    reason: str,
    operator: str,
    target_trade_date: str = "",
    requested_trade_date: str = "",
    input_trade_date: str = "",
    blocking_record: dict[str, Any] | None = None,
    industry_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "status": "blocked",
        "orchestration_status": orchestration_status,
        "error_class": error_class,
        "retryable": bool(retryable),
        "reason_code": reason_code,
        "blocking_stage": blocking_stage,
        "reason": str(reason)[:500],
        "trade_date": target_trade_date,
        "target_trade_date": target_trade_date,
        "requested_trade_date": requested_trade_date,
        "input_trade_date": input_trade_date,
        "input_ready": False,
        "blocking_record": blocking_record or {},
        "industry_snapshot": industry_snapshot or {},
        "adapter_registry_seal_hash": _adapter_seal_hash(),
        "allocations": _cash_allocation(str(reason)[:500]),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    try:
        result["attempt_audit"] = persist_blocked_attempt(
            engine, result, operator=operator,
        )
        result["attempt_audit_persisted"] = True
    except Exception as exc:
        _log_incident(
            level="error", stage="BLOCKED_ATTEMPT_AUDIT", exc=exc,
        )
        result["attempt_audit"] = {}
        result["attempt_audit_persisted"] = False
        result["attempt_audit_error_type"] = type(exc).__name__
    return result


def _not_due_result(
    *, reason_code: str, reason: str, target_trade_date: str,
    requested_trade_date: str = "", current_run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": "not_due",
        "orchestration_status": NOT_DUE,
        "error_class": "NONE",
        "retryable": False,
        "reason_code": reason_code,
        "blocking_stage": "SCHEDULE",
        "reason": reason,
        "trade_date": target_trade_date,
        "target_trade_date": target_trade_date,
        "requested_trade_date": requested_trade_date,
        "input_ready": False,
        "current_run": current_run or {},
        "allocations": _cash_allocation(reason),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def orchestrate_strategy_governance(
    *,
    requested_trade_date: str = "",
    strategy_limit: int = 500,
    operator: str = "daily_governance",
    allow_revision: bool = False,
    now: datetime | None = None,
    engine=None,
    industry_capture: Callable[..., dict[str, Any]] | None = None,
    governance_runner: Callable[..., dict[str, Any]] | None = None,
    process_preflight: Callable[[], Any] | None = None,
    ensure_build_commit_sha: str = "",
) -> dict[str, Any]:
    """Run exact-date input capture and governance through one shared path.

    ``strategy_limit`` is a legacy candidate-stock source request limit.  The
    runtime strategy registry and governance rankings are always loaded in
    full and are intentionally independent of this value.
    """

    try:
        database = engine or get_engine()
    except Exception as exc:
        _log_incident(
            level="error", stage="DATABASE_ENGINE", exc=exc,
        )
        reason = f"治理程序无法连接权威数据库（{type(exc).__name__}）"
        return {
            "status": "blocked",
            "orchestration_status": PROGRAM_ERROR,
            "error_class": "PROGRAM",
            "retryable": False,
            "reason_code": "DATABASE_ENGINE_UNAVAILABLE",
            "blocking_stage": "PROGRAM",
            "reason": reason,
            "trade_date": "",
            "target_trade_date": "",
            "requested_trade_date": str(requested_trade_date or "")[:10],
            "input_trade_date": "",
            "input_ready": False,
            "blocking_record": {},
            "industry_snapshot": {},
            "attempt_audit": {},
            "attempt_audit_persisted": False,
            "attempt_audit_error_type": type(exc).__name__,
            "allocations": _cash_allocation(reason),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    exchange_now = _exchange_now(now)
    requested = str(requested_trade_date or "").strip()
    if process_preflight is not None:
        try:
            process_preflight()
        except Exception as exc:
            _log_incident(
                level="error", stage="ADAPTER_PREFLIGHT", exc=exc,
            )
            return _blocked_result(
                database,
                orchestration_status=PROGRAM_ERROR,
                error_class="PROGRAM",
                retryable=False,
                reason_code="ADAPTER_BOOTSTRAP_FAILED",
                blocking_stage="PROGRAM",
                reason=f"执行适配器启动失败（{type(exc).__name__}）",
                operator=operator,
                requested_trade_date=requested,
            )
    if requested:
        try:
            requested = date.fromisoformat(requested[:10]).isoformat()
        except ValueError:
            return _blocked_result(
                database,
                orchestration_status=INTEGRITY_ERROR,
                error_class="INTEGRITY",
                retryable=False,
                reason_code="INVALID_TRADE_DATE",
                blocking_stage="REQUEST",
                reason="指定交易日格式无效，必须使用YYYY-MM-DD",
                operator=operator,
                requested_trade_date=requested,
            )
    try:
        authoritative = authoritative_closed_trade_date(
            database, now=exchange_now,
        )
    except Exception as exc:
        _log_incident(
            level="warning", stage="AUTHORITATIVE_CALENDAR", exc=exc,
        )
        return _blocked_result(
            database,
            orchestration_status=NOT_READY,
            error_class="NOT_READY",
            retryable=True,
            reason_code="CALENDAR_UNAVAILABLE",
            blocking_stage="CALENDAR",
            reason=(
                "权威交易日历暂不可用；未写入治理状态，模拟资金保持现金"
                f"（{type(exc).__name__}）"
            ),
            operator=operator,
            requested_trade_date=requested,
        )
    if not authoritative:
        return _blocked_result(
            database,
            orchestration_status=NOT_READY,
            error_class="NOT_READY",
            retryable=True,
            reason_code="NO_CLOSED_SESSION",
            blocking_stage="CALENDAR",
            reason="权威交易日历没有已收盘交易日；模拟资金保持现金",
            operator=operator,
            requested_trade_date=requested,
        )
    exchange_date = exchange_now.date().isoformat()
    try:
        current_session_status = _calendar_status(database, exchange_date)
    except Exception as exc:
        return _blocked_result(
            database,
            orchestration_status=NOT_READY,
            error_class="NOT_READY",
            retryable=True,
            reason_code="CALENDAR_SESSION_UNAVAILABLE",
            blocking_stage="CALENDAR",
            reason=f"无法确认当日交易所日历状态（{type(exc).__name__}）",
            operator=operator,
            target_trade_date=authoritative,
            requested_trade_date=requested,
        )
    if current_session_status is None:
        return _blocked_result(
            database,
            orchestration_status=NOT_READY,
            error_class="NOT_READY",
            retryable=True,
            reason_code="CALENDAR_SESSION_NOT_READY",
            blocking_stage="CALENDAR",
            reason="Asia/Shanghai当日尚无权威交易所日历记录",
            operator=operator,
            target_trade_date=authoritative,
            requested_trade_date=requested,
        )
    if current_session_status not in {0, 1}:
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="CALENDAR_SESSION_INTEGRITY",
            blocking_stage="CALENDAR",
            reason="权威交易所日历状态不是0或1",
            operator=operator,
            target_trade_date=authoritative,
            requested_trade_date=requested,
        )
    after_close = exchange_now.hour >= DAILY_CLOSE_READY_HOUR
    if (
        current_session_status == 1
        and after_close
        and authoritative != exchange_date
    ):
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="AUTHORITATIVE_DATE_INTEGRITY",
            blocking_stage="CALENDAR",
            reason="当日已开市并达到收盘就绪时间，但权威日期未推进到当日",
            operator=operator,
            target_trade_date=authoritative,
            requested_trade_date=requested,
        )
    if requested and requested != authoritative:
        if requested == exchange_now.date().isoformat():
            if current_session_status != 1:
                return _not_due_result(
                    reason_code="NON_SESSION_DATE",
                    reason="指定日期不是交易所开市日，本次治理未到期",
                    target_trade_date=authoritative,
                    requested_trade_date=requested,
                )
            if exchange_now.hour < DAILY_CLOSE_READY_HOUR:
                return _not_due_result(
                    reason_code="SESSION_NOT_CLOSED",
                    reason="当前交易日尚未达到Asia/Shanghai收盘数据就绪时间",
                    target_trade_date=authoritative,
                    requested_trade_date=requested,
                )
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="TARGET_NOT_AUTHORITATIVE",
            blocking_stage="REQUEST",
            reason=(
                "指定交易日不是权威已收盘交易日"
                f"（要求{authoritative}，指定{requested}）"
            ),
            operator=operator,
            target_trade_date=authoritative,
            requested_trade_date=requested,
        )
    target = requested or authoritative
    if not allow_revision:
        try:
            existing = _canonical_run(database, target)
        except Exception as exc:
            _log_incident(
                level="error", stage="CANONICAL_RUN_LOOKUP", exc=exc,
            )
            return _blocked_result(
                database,
                orchestration_status=PROGRAM_ERROR,
                error_class="PROGRAM",
                retryable=False,
                reason_code="CANONICAL_LOOKUP_FAILED",
                blocking_stage="PROGRAM",
                reason=f"治理程序无法读取规范运行记录（{type(exc).__name__}）",
                operator=operator,
                target_trade_date=target,
                requested_trade_date=requested,
            )
        if existing is not None and (
            not ensure_build_commit_sha
            or str(existing.get("build_commit_sha") or "")
            == str(ensure_build_commit_sha)
        ):
            return _not_due_result(
                reason_code="CANONICAL_ALREADY_COMPLETED",
                reason=f"{target}已有生效治理结果，本次调度无需重复运行",
                target_trade_date=target,
                requested_trade_date=requested,
                current_run=existing,
            )
        # Release cutover is stricter than the ordinary once-per-session
        # scheduler.  A canonical result produced by another build proves the
        # date is complete, but it cannot prove this release.  Continue through
        # the normal immutable-revision path so the new build is exercised and
        # health can bind the canonical result to its exact SHA.  When the
        # current canonical already belongs to this build, the branch above is
        # the idempotent NOT_DUE path and no extra revision is created.

    capture = industry_capture or capture_industry_history
    try:
        industry = capture(database, trade_date=target)
    except IndustrySnapshotNotReady as exc:
        return _blocked_result(
            database,
            orchestration_status=NOT_READY,
            error_class="NOT_READY",
            retryable=True,
            reason_code="QMT_INDUSTRY_SNAPSHOT_NOT_READY",
            blocking_stage="INDUSTRY_SNAPSHOT",
            reason=str(exc),
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
        )
    except IndustrySnapshotIntegrityError as exc:
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="QMT_INDUSTRY_SNAPSHOT_INTEGRITY",
            blocking_stage="INDUSTRY_SNAPSHOT",
            reason=str(exc),
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
        )
    except Exception as exc:
        _log_incident(
            level="error", stage="INDUSTRY_SNAPSHOT_PROGRAM", exc=exc,
        )
        return _blocked_result(
            database,
            orchestration_status=PROGRAM_ERROR,
            error_class="PROGRAM",
            retryable=False,
            reason_code="INDUSTRY_CAPTURE_PROGRAM_ERROR",
            blocking_stage="PROGRAM",
            reason=f"行业快照程序执行失败（{type(exc).__name__}）",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
        )

    from server.engine.strategy_governance import governance_snapshot

    authoritative_persistent_runner = (
        governance_runner is None or governance_runner is governance_snapshot
    )
    if governance_runner is None:
        governance_runner = governance_snapshot
    try:
        result = governance_runner(
            trade_date=target,
            persist=True,
            operator=operator,
            strategy_limit=max(1, min(500, int(strategy_limit))),
        )
    except Exception as exc:
        from server.engine.strategy_governance import GovernanceEvidenceNotReady

        if isinstance(exc, GovernanceEvidenceNotReady):
            return _blocked_result(
                database,
                orchestration_status=NOT_READY,
                error_class="NOT_READY",
                retryable=True,
                reason_code="GOVERNANCE_EVIDENCE_NOT_READY",
                blocking_stage="GOVERNANCE_INPUT",
                reason=str(exc),
                operator=operator,
                target_trade_date=target,
                requested_trade_date=requested,
                input_trade_date=target,
                blocking_record=exc.blocking_record,
                industry_snapshot=industry,
            )
        _log_incident(
            level="error", stage="GOVERNANCE_PROGRAM", exc=exc,
        )
        return _blocked_result(
            database,
            orchestration_status=PROGRAM_ERROR,
            error_class="PROGRAM",
            retryable=False,
            reason_code="GOVERNANCE_PROGRAM_ERROR",
            blocking_stage="PROGRAM",
            reason=f"治理程序执行失败（{type(exc).__name__}）",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
            industry_snapshot=industry,
        )
    if not isinstance(result, dict) or result.get("status") != "ok":
        return _blocked_result(
            database,
            orchestration_status=PROGRAM_ERROR,
            error_class="PROGRAM",
            retryable=False,
            reason_code="GOVERNANCE_RESULT_INVALID",
            blocking_stage="PROGRAM",
            reason="治理程序返回了非规范完成状态",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
            industry_snapshot=industry,
        )
    if authoritative_persistent_runner:
        try:
            result = _exact_canonical_governance_readback(
                database,
                runner_result=result,
                target_trade_date=target,
                expected_build_sha=ensure_build_commit_sha,
            )
        except RuntimeError as exc:
            _log_incident(
                level="error", stage="COMPLETION_CANONICAL_READBACK", exc=exc,
            )
            return _blocked_result(
                database,
                orchestration_status=INTEGRITY_ERROR,
                error_class="INTEGRITY",
                retryable=False,
                reason_code="GOVERNANCE_COMPLETION_IDENTITY_INVALID",
                blocking_stage="GOVERNANCE_RESULT",
                reason="治理完成结果未通过数据库canonical精确回读",
                operator=operator,
                target_trade_date=target,
                requested_trade_date=requested,
                input_trade_date=target,
                industry_snapshot=industry,
            )
    if (
        ensure_build_commit_sha
        and str(result.get("build_commit_sha") or "")
        != str(ensure_build_commit_sha)
    ):
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="GOVERNANCE_BUILD_SHA_MISMATCH",
            blocking_stage="GOVERNANCE_RESULT",
            reason="治理完成结果未绑定发布要求的精确构建SHA",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
            industry_snapshot=industry,
        )
    try:
        validate_governance_completion_contract(
            result,
            target_trade_date=target,
            expected_build_sha=ensure_build_commit_sha,
        )
    except (TypeError, ValueError, RuntimeError) as exc:
        _log_incident(
            level="error", stage="COMPLETION_IDENTITY_CONTRACT", exc=exc,
        )
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="GOVERNANCE_COMPLETION_IDENTITY_INVALID",
            blocking_stage="GOVERNANCE_RESULT",
            reason="治理完成结果的canonical身份、构建版本或资金清单无效",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
            industry_snapshot=industry,
        )
    try:
        validate_governance_safety_contract(result)
    except (TypeError, ValueError) as exc:
        _log_incident(
            level="error", stage="ORDER_AUTHORITY_CONTRACT", exc=exc,
        )
        return _blocked_result(
            database,
            orchestration_status=INTEGRITY_ERROR,
            error_class="INTEGRITY",
            retryable=False,
            reason_code="GOVERNANCE_ORDER_AUTHORITY_INVALID",
            blocking_stage="GOVERNANCE_RESULT",
            reason="治理完成结果的模拟资金或真实下单权限合同无效",
            operator=operator,
            target_trade_date=target,
            requested_trade_date=requested,
            input_trade_date=target,
            industry_snapshot=industry,
        )
    return {
        **result,
        "orchestration_status": COMPLETED,
        "error_class": "NONE",
        "retryable": False,
        "reason_code": "GOVERNANCE_COMPLETED",
        "blocking_stage": "",
        "target_trade_date": target,
        "requested_trade_date": requested,
        "industry_snapshot": industry,
    }


__all__ = [
    "COMPLETED",
    "INTEGRITY_ERROR",
    "NOT_DUE",
    "NOT_READY",
    "PROGRAM_ERROR",
    "canonical_unavailable_context",
    "orchestrate_strategy_governance",
    "persist_blocked_attempt",
    "validate_governance_completion_contract",
    "validate_governance_safety_contract",
]
