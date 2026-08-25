# -*- coding: utf-8 -*-
"""Dynamic candidate to internal-paper forward-evidence binding.

This module deliberately does not submit or match orders.  It produces a
small, explicit shadow-trial plan and can only complete that plan by loading
facts already written by the existing V2 internal-paper OMS and V3
fill-backed forward-evidence worker.  A caller-provided dictionary can never
stand in for those database facts.

No demonstration adapter is added to the trusted startup manifest here.  The
existing immutable-manifest and immutable-V3-sleeve adapters already exercise
the code-owned path; registering a zero-signal demo would change the sealed
production manifest and misleadingly advertise a usable alpha source.  A new
dynamic adapter must still be a reviewed release artifact bound to its exact
strategy version and expected registry-seal hash.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text

from server.common.sql_reader import current_bound_sql_connection
from server.trading_v3.forward_evidence import (
    ATTRIBUTION_VERSION,
    EXECUTED_FORWARD_PROTOCOL,
    EXIT_ALLOCATION_PROTOCOL,
    EXECUTED_INTENT_REASONS,
)


INTERNAL_PAPER_ACCOUNT_ID = "paper-main-v2"
TRIAL_PLAN_SCHEMA = "probiga.dynamic-shadow-trial-plan.v1"
TRIAL_PLAN_ID_SCHEMA = "probiga.dynamic-shadow-trial-plan-identity.v1"
TRIAL_CANDIDATE_ENVELOPE_SCHEMA = (
    "probiga.dynamic-shadow-candidate-envelope.v1"
)
TRIAL_CHAIN_SCHEMA = "probiga.dynamic-shadow-trial-chain.v1"
TRIAL_CHAIN_ID_SCHEMA = "probiga.dynamic-shadow-trial-chain-identity.v1"
TRIAL_EXIT_BINDING_SCHEMA = "probiga.dynamic-shadow-trial-exit-binding.v1"
BOOTSTRAP_AUTHORIZATION_SCHEMA = (
    "probiga.dynamic-shadow-bootstrap-authorization.v1"
)
BOOTSTRAP_REASON_CODE = "DYNAMIC_SHADOW_BOOTSTRAP"
BOOTSTRAP_RISK_SCHEMA = "probiga.dynamic-shadow-bootstrap-risk.v1"
_ACTIONABLE_SHADOW_SIGNAL_STATUSES = frozenset({
    "READY", "CONFIRM", "BUY_READY",
})
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_STOCK_CODE_PATTERN = re.compile(r"^[0-9]{6}$")


class DynamicShadowLedgerError(RuntimeError):
    """The internal-paper chain is incomplete, inconsistent, or tampered."""


_LOGGER = logging.getLogger(__name__)


def _readiness_error_detail(exc: Exception) -> dict[str, str]:
    """Expose contract errors, but never serialize infrastructure exceptions."""

    if isinstance(exc, DynamicShadowLedgerError):
        return {
            "error_type": type(exc).__name__,
            "error_code": "DYNAMIC_SHADOW_LEDGER_CONTRACT_INVALID",
            "reason": str(exc),
            "incident_id": "",
        }
    incident_id = uuid.uuid4().hex
    _LOGGER.error(
        "dynamic shadow ledger readiness incident_id=%s error_type=%s",
        incident_id,
        type(exc).__name__,
    )
    return {
        "error_type": type(exc).__name__,
        "error_code": "DYNAMIC_SHADOW_LEDGER_INTERNAL_FAILURE",
        "reason": "动态影子账本验证发生内部错误，请按事件编号排查",
        "incident_id": incident_id,
    }


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise DynamicShadowLedgerError("影子证据包含非有限数值")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise DynamicShadowLedgerError(
        f"影子证据包含不可序列化类型：{type(value).__name__}"
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _versioned_ownership_hash(
    source_run_uid: Any,
    source_forecast_id: Any,
    stock_code: Any,
    strategy_key: Any,
    strategy_version: Any,
) -> str:
    return hashlib.sha256(
        (
            f"{source_run_uid}|{source_forecast_id}|{stock_code}|"
            f"{strategy_key}|{strategy_version}"
        ).encode("utf-8")
    ).hexdigest()


def _strict_json(value: Any, *, label: str, expected: type) -> Any:
    if isinstance(value, expected):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DynamicShadowLedgerError(f"{label}不是有效JSON") from exc
    if not isinstance(parsed, expected):
        raise DynamicShadowLedgerError(f"{label}类型错误")
    # Strict serialization rejects NaN and unsupported runtime values.
    return json.loads(_canonical_json(parsed))


def _rows(connection: Any, statement: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(statement), dict(params)).mappings().all()
    ]


def _rows_expanding(
    connection: Any,
    statement: str,
    params: Mapping[str, Any],
    *expanding_names: str,
) -> list[dict[str, Any]]:
    query = text(statement)
    if expanding_names:
        query = query.bindparams(*(
            bindparam(name, expanding=True) for name in expanding_names
        ))
    return [
        dict(row)
        for row in connection.execute(query, dict(params)).mappings().all()
    ]


def _scalar_count(
    connection: Any,
    statement: str,
    params: Mapping[str, Any],
    *expanding_names: str,
) -> int:
    query = text(statement)
    if expanding_names:
        query = query.bindparams(*(
            bindparam(name, expanding=True) for name in expanding_names
        ))
    return int(connection.execute(query, dict(params)).scalar() or 0)


def _one(
    connection: Any,
    statement: str,
    params: Mapping[str, Any],
    *,
    label: str,
) -> dict[str, Any]:
    rows = _rows(connection, statement, params)
    if len(rows) != 1:
        raise DynamicShadowLedgerError(f"{label}必须且只能存在一条，实际{len(rows)}条")
    return rows[0]


def _no_real_authority(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            if name in {
                "real_order_authority",
                "automatic_real_order_submission",
                "real_trading_enabled",
            } and item is not False:
                raise DynamicShadowLedgerError(
                    f"影子证据错误声明真实下单权限：{path}.{name}"
                )
            _no_real_authority(item, path=f"{path}.{name}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _no_real_authority(item, path=f"{path}[{index}]")


_RECEIPT_SELECT = """
    SELECT run_uid, strategy_key, strategy_version, strategy_version_hash,
           execution_binding_hash, adapter_artifact_sha256, cost_model_hash,
           adapter_key, adapter_version, trade_date, completed_at, status,
           input_hash, output_hash, stable_result_hash, candidate_count,
           candidate_identity_json, receipt_json, receipt_hash
    FROM st_strategy_adapter_run_receipt
    WHERE run_uid=:run_uid
"""


def _verified_receipt(
    connection: Any,
    *,
    run_uid: str,
    receipt_hash: str,
) -> dict[str, Any]:
    stored = _one(
        connection,
        _RECEIPT_SELECT,
        {"run_uid": run_uid},
        label="动态候选运行回执",
    )
    return _verified_receipt_row(stored, receipt_hash=receipt_hash)


def _verified_receipt_row(
    stored: Mapping[str, Any],
    *,
    receipt_hash: str,
) -> dict[str, Any]:
    """Replay one already-loaded receipt without another database read."""

    # Lazy import avoids a module cycle when the public adapter verifier routes
    # to this persistent ledger.
    from server.engine.strategy_execution_adapters import (
        verify_persisted_strategy_adapter_run_receipt,
    )

    raw_receipt = _strict_json(
        stored.get("receipt_json"),
        label="动态候选运行回执",
        expected=dict,
    )
    try:
        verified = verify_persisted_strategy_adapter_run_receipt(
            raw_receipt, stored,
        )
    except ValueError as exc:
        raise DynamicShadowLedgerError("动态候选运行回执复算失败") from exc
    if str(verified.get("receipt_hash") or "") != receipt_hash:
        raise DynamicShadowLedgerError("动态候选运行回执哈希与计划不一致")
    return verified


def _candidate_fact_payload(
    *,
    receipt: Mapping[str, Any],
    candidate_index: int,
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": "probiga.strategy-adapter-candidate-fact.v1",
        "candidate_run_uid": str(receipt.get("run_uid") or ""),
        "candidate_receipt_hash": str(receipt.get("receipt_hash") or ""),
        "candidate_index": candidate_index,
        "stock_code": str(candidate.get("stock_code") or "").strip().zfill(6),
        "candidate": json.loads(_canonical_json(candidate)),
    }


def _validate_candidate_facts(
    receipt: Mapping[str, Any],
    candidates: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(candidates):
        if not isinstance(raw, Mapping):
            raise DynamicShadowLedgerError("持久化动态候选事实必须是对象")
        candidate = json.loads(_canonical_json(raw))
        code = str(candidate.get("stock_code") or "").strip().zfill(6)
        if not _STOCK_CODE_PATTERN.fullmatch(code) or code in seen:
            raise DynamicShadowLedgerError("持久化动态候选事实股票身份无效或重复")
        seen.add(code)
        for field in (
            "strategy_key",
            "strategy_version",
            "strategy_version_hash",
            "execution_binding_hash",
            "adapter_artifact_sha256",
            "cost_model_hash",
        ):
            if str(candidate.get(field) or "") != str(receipt.get(field) or ""):
                raise DynamicShadowLedgerError(
                    f"持久化动态候选事实字段{field}与回执不一致"
                )
        if (
            str(candidate.get("trade_date") or "") != str(receipt.get("trade_date") or "")
            or str(candidate.get("data_date") or "") != str(receipt.get("trade_date") or "")
        ):
            raise DynamicShadowLedgerError("持久化动态候选事实日期越界")
        _no_real_authority(candidate, path=f"candidate_facts[{index}]")
        normalized.append(candidate)
    if (
        len(normalized) != int(receipt.get("candidate_count") or 0)
        or sorted(seen) != list(receipt.get("candidate_identity") or [])
    ):
        raise DynamicShadowLedgerError("持久化动态候选事实数量或身份集合与回执不一致")
    output_payload = {
        "schema": "probiga.strategy-candidate-output.v1",
        "trade_date": str(receipt.get("trade_date") or ""),
        "strategy_key": str(receipt.get("strategy_key") or ""),
        "strategy_version": str(receipt.get("strategy_version") or ""),
        "execution_binding_hash": str(receipt.get("execution_binding_hash") or ""),
        "candidates": normalized,
    }
    if _digest(output_payload) != str(receipt.get("output_hash") or ""):
        raise DynamicShadowLedgerError("持久化动态候选事实无法复算候选输出哈希")
    return normalized


def persist_strategy_adapter_candidate_facts(
    connection: Any,
    *,
    candidate_receipt: Mapping[str, Any],
    candidates: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Append the exact raw CandidateBatch rows behind one run receipt."""

    receipt = _verified_receipt(
        connection,
        run_uid=str(candidate_receipt.get("run_uid") or ""),
        receipt_hash=str(candidate_receipt.get("receipt_hash") or ""),
    )
    if _canonical_json(receipt) != _canonical_json(candidate_receipt):
        raise DynamicShadowLedgerError("候选事实调用回执与持久化回执不一致")
    normalized = _validate_candidate_facts(receipt, candidates)
    existing = _rows(connection, """
        SELECT candidate_run_uid, stock_code, candidate_index, trade_date,
               candidate_json, candidate_hash
        FROM st_strategy_adapter_candidate_fact
        WHERE candidate_run_uid=:run_uid
        ORDER BY candidate_index
    """, {"run_uid": receipt["run_uid"]})
    if existing:
        verified = verify_persisted_strategy_adapter_candidate_facts(
            connection,
            candidate_receipt=receipt,
        )
        if verified["candidates"] != normalized:
            raise DynamicShadowLedgerError("同一运行回执的候选事实发生冲突")
        return verified["candidate_facts"]
    result: list[dict[str, Any]] = []
    for index, candidate in enumerate(normalized):
        payload = _candidate_fact_payload(
            receipt=receipt,
            candidate_index=index,
            candidate=candidate,
        )
        candidate_hash = _digest(payload)
        connection.execute(text("""
            INSERT INTO st_strategy_adapter_candidate_fact (
                candidate_run_uid, stock_code, candidate_index, trade_date,
                candidate_json, candidate_hash
            ) VALUES (
                :candidate_run_uid, :stock_code, :candidate_index,
                :trade_date, :candidate_json, :candidate_hash
            )
        """), {
            "candidate_run_uid": receipt["run_uid"],
            "stock_code": payload["stock_code"],
            "candidate_index": index,
            "trade_date": receipt["trade_date"],
            "candidate_json": _canonical_json(candidate),
            "candidate_hash": candidate_hash,
        })
        result.append({
            **payload,
            "candidate_hash": candidate_hash,
        })
    return result


def verify_persisted_strategy_adapter_candidate_facts(
    connection: Any,
    *,
    candidate_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Replay all raw rows and the receipt's exact order-sensitive output."""

    receipt = _verified_receipt(
        connection,
        run_uid=str(candidate_receipt.get("run_uid") or ""),
        receipt_hash=str(candidate_receipt.get("receipt_hash") or ""),
    )
    rows = _rows(connection, """
        SELECT candidate_run_uid, stock_code, candidate_index, trade_date,
               candidate_json, candidate_hash
        FROM st_strategy_adapter_candidate_fact
        WHERE candidate_run_uid=:run_uid
        ORDER BY candidate_index
    """, {"run_uid": receipt["run_uid"]})
    return _verify_candidate_fact_rows(receipt, rows)


def _verify_candidate_fact_rows(
    receipt: Mapping[str, Any],
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Replay a complete, already-loaded candidate-fact batch."""

    rows = [dict(row) for row in rows]
    if len(rows) != int(receipt.get("candidate_count") or 0):
        raise DynamicShadowLedgerError("候选事实表未完整覆盖运行回执")
    candidates: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    for expected_index, row in enumerate(rows):
        candidate = _strict_json(
            row.get("candidate_json"),
            label="持久化动态候选事实",
            expected=dict,
        )
        payload = _candidate_fact_payload(
            receipt=receipt,
            candidate_index=expected_index,
            candidate=candidate,
        )
        if (
            int(row.get("candidate_index") or 0) != expected_index
            or str(row.get("candidate_run_uid") or "") != str(receipt["run_uid"])
            or str(row.get("stock_code") or "") != payload["stock_code"]
            or str(row.get("trade_date") or "")[:10] != str(receipt["trade_date"])
            or str(row.get("candidate_hash") or "") != _digest(payload)
        ):
            raise DynamicShadowLedgerError("持久化动态候选事实身份或哈希无效")
        candidates.append(candidate)
        facts.append({**payload, "candidate_hash": _digest(payload)})
    candidates = _validate_candidate_facts(receipt, candidates)
    return {
        "candidate_receipt": receipt,
        "candidates": candidates,
        "candidate_facts": facts,
        "candidate_fact_set_hash": _digest([
            {
                "candidate_index": fact["candidate_index"],
                "candidate_hash": fact["candidate_hash"],
            }
            for fact in facts
        ]),
    }


def _candidate_signal_contract(
    signal: Mapping[str, Any],
    receipt: Mapping[str, Any],
    candidate_fact: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the system-owned envelope for one exact persisted raw row.

    The caller is deliberately not allowed to pass an enriched strategy-center
    signal here.  Every caller-controlled key must be byte-for-byte equivalent
    after canonical JSON normalization to the raw CandidateBatch row already
    bound by ``output_hash``.  Receipt/fact identities are added only here, so
    an extra caller field can never enter the shadow-plan hash domain.
    """

    if not isinstance(signal, Mapping):
        raise DynamicShadowLedgerError("影子试验候选必须是对象")
    supplied = json.loads(_canonical_json(signal))
    raw_candidate = candidate_fact.get("candidate")
    if not isinstance(raw_candidate, Mapping):
        raise DynamicShadowLedgerError("影子试验缺少持久化原始候选事实")
    persisted = json.loads(_canonical_json(raw_candidate))
    if supplied != persisted:
        raise DynamicShadowLedgerError(
            "影子试验候选必须与持久化原始候选事实完全一致，禁止注入或改写字段"
        )
    stock_code = str(candidate_fact.get("stock_code") or "")
    if not _STOCK_CODE_PATTERN.fullmatch(stock_code):
        raise DynamicShadowLedgerError("影子试验候选股票代码无效")
    if stock_code not in set(receipt.get("candidate_identity") or []):
        raise DynamicShadowLedgerError("影子试验股票不在候选回执身份集合中")
    if str(persisted.get("stock_code") or "").strip().zfill(6) != stock_code:
        raise DynamicShadowLedgerError("影子试验候选股票与原始候选事实不一致")
    envelope = {
        "schema": TRIAL_CANDIDATE_ENVELOPE_SCHEMA,
        "candidate_run_uid": str(receipt.get("run_uid") or ""),
        "candidate_receipt_hash": str(receipt.get("receipt_hash") or ""),
        "candidate_index": int(candidate_fact.get("candidate_index") or 0),
        "candidate_fact_hash": str(candidate_fact.get("candidate_hash") or ""),
        "stock_code": stock_code,
        "candidate": persisted,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    _no_real_authority(envelope, path="candidate_signal")
    return envelope


def _is_actionable_shadow_candidate(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and str(value.get("signal_direction") or "").upper() == "BUY"
        and str(value.get("signal_status") or "").upper()
        in _ACTIONABLE_SHADOW_SIGNAL_STATUSES
    )


def _verified_candidate_signal_envelope(
    stored_signal: Mapping[str, Any],
    receipt: Mapping[str, Any],
    candidate_fact: Mapping[str, Any],
) -> dict[str, Any]:
    raw_candidate = candidate_fact.get("candidate")
    if not isinstance(raw_candidate, Mapping):
        raise DynamicShadowLedgerError("影子试验缺少持久化原始候选事实")
    expected = _candidate_signal_contract(
        raw_candidate,
        receipt,
        candidate_fact,
    )
    observed = json.loads(_canonical_json(stored_signal))
    if observed != expected:
        raise DynamicShadowLedgerError("影子试验候选信封与持久化原始事实漂移")
    return expected


_PLAN_SELECT = """
    SELECT plan_id, candidate_run_uid, candidate_receipt_hash,
           strategy_key, strategy_version, strategy_version_hash,
           execution_binding_hash, trade_date, stock_code, account_id,
           maximum_target_bp, candidate_fact_hash, candidate_signal_json,
           candidate_signal_hash, plan_payload_json, plan_hash,
           plan_status, automatic_real_order_submission,
           real_order_authority, created_at
    FROM st_dynamic_shadow_trial_plan
    WHERE plan_id=:plan_id
"""


def _plan_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": TRIAL_PLAN_SCHEMA,
        "candidate_run_uid": str(row.get("candidate_run_uid") or ""),
        "candidate_receipt_hash": str(row.get("candidate_receipt_hash") or ""),
        "strategy_key": str(row.get("strategy_key") or ""),
        "strategy_version": str(row.get("strategy_version") or ""),
        "strategy_version_hash": str(row.get("strategy_version_hash") or ""),
        "execution_binding_hash": str(row.get("execution_binding_hash") or ""),
        "trade_date": str(row.get("trade_date") or "")[:10],
        "stock_code": str(row.get("stock_code") or ""),
        "account_id": str(row.get("account_id") or ""),
        "maximum_target_bp": int(row.get("maximum_target_bp") or 0),
        "candidate_fact_hash": str(row.get("candidate_fact_hash") or ""),
        "candidate_signal_hash": str(row.get("candidate_signal_hash") or ""),
        "plan_status": str(row.get("plan_status") or ""),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _expected_plan_id(payload: Mapping[str, Any]) -> str:
    return _digest({
        "schema": TRIAL_PLAN_ID_SCHEMA,
        "candidate_receipt_hash": payload["candidate_receipt_hash"],
        "strategy_key": payload["strategy_key"],
        "strategy_version": payload["strategy_version"],
        "stock_code": payload["stock_code"],
        "account_id": payload["account_id"],
    })


def _iso_second(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    raw = str(value or "").strip().replace(" ", "T")
    try:
        return datetime.fromisoformat(raw).isoformat(timespec="seconds")
    except ValueError as exc:
        raise DynamicShadowLedgerError("行业历史时间字段无效") from exc


def _verified_industry_fact(
    connection: Any,
    *,
    trade_date: str,
    stock_code: str,
) -> dict[str, Any]:
    row = _one(connection, """
        SELECT snapshot_id, trade_date, as_of_exclusive, stock_code,
               industry_name, industry_type, source_system, source_fact_id,
               source_effective_at, source_etl_sync_at, row_hash
        FROM st_strategy_industry_history
        WHERE trade_date=:trade_date AND stock_code=:stock_code
        ORDER BY snapshot_id
    """, {
        "trade_date": str(trade_date)[:10],
        "stock_code": str(stock_code),
    }, label="动态影子目标日行业历史事实")
    return _verified_industry_row(
        row,
        trade_date=str(trade_date)[:10],
        stock_code=str(stock_code),
    )


def _verified_industry_row(
    row: Mapping[str, Any],
    *,
    trade_date: str,
    stock_code: str,
) -> dict[str, Any]:
    """Replay one already-loaded exact-date industry membership fact."""

    payload = {
        "snapshot_id": str(row.get("snapshot_id") or ""),
        "trade_date": str(row.get("trade_date") or "")[:10],
        "as_of_exclusive": _iso_second(row.get("as_of_exclusive")),
        "stock_code": str(row.get("stock_code") or ""),
        "industry_name": str(row.get("industry_name") or ""),
        "industry_type": str(row.get("industry_type") or ""),
        "source_system": str(row.get("source_system") or ""),
        "source_fact_id": str(row.get("source_fact_id") or ""),
        "source_effective_at": _iso_second(row.get("source_effective_at")),
        "source_etl_sync_at": _iso_second(row.get("source_etl_sync_at")),
    }
    expected_hash = _digest(payload)
    if (
        not _SHA256_PATTERN.fullmatch(payload["snapshot_id"])
        or not _SHA256_PATTERN.fullmatch(str(row.get("row_hash") or ""))
        or expected_hash != str(row.get("row_hash") or "")
        or payload["trade_date"] != str(trade_date)[:10]
        or payload["stock_code"] != str(stock_code)
        or not payload["industry_name"]
        or not payload["industry_type"]
    ):
        raise DynamicShadowLedgerError("目标日行业历史事实身份或row_hash无效")
    return {**payload, "row_hash": expected_hash}


def verify_dynamic_shadow_industry_fact(
    connection: Any,
    *,
    trade_date: str,
    stock_code: str,
) -> dict[str, Any]:
    """Publicly replay one exact-date industry row used by bootstrap risk."""

    return _verified_industry_fact(
        connection,
        trade_date=str(trade_date)[:10],
        stock_code=str(stock_code),
    )


def _current_shadow_registry_version(
    connection: Any,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    row = _one(connection, """
        SELECT r.current_version, r.current_status, r.enabled,
               v.version_hash, v.source_kind
        FROM st_strategy_registry r
        JOIN st_strategy_version v
          ON v.strategy_key=r.strategy_key
         AND v.version=r.current_version
        WHERE r.strategy_key=:strategy_key
    """, {"strategy_key": plan["strategy_key"]}, label="动态影子当前策略版本")
    if (
        int(row.get("enabled") or 0) != 1
        or str(row.get("current_status") or "") != "SHADOW"
        or str(row.get("current_version") or "") != plan["strategy_version"]
        or str(row.get("version_hash") or "")
        != plan["strategy_version_hash"]
        or str(row.get("source_kind") or "") != "runtime_registry"
    ):
        raise DynamicShadowLedgerError("bootstrap仅允许精确当前SHADOW动态版本")
    return dict(row)


def build_dynamic_shadow_bootstrap_authorization(
    connection: Any,
    *,
    plan_id: str,
) -> dict[str, Any]:
    """Authorize one current-version internal-paper bootstrap trial only."""

    plan = verify_dynamic_shadow_trial_plan(connection, str(plan_id))
    _current_shadow_registry_version(connection, plan)
    raw_candidate = plan["candidate_fact"].get("candidate")
    if (
        not isinstance(raw_candidate, Mapping)
        or str(raw_candidate.get("signal_direction") or "").upper() != "BUY"
    ):
        raise DynamicShadowLedgerError("bootstrap只接受哈希绑定的BUY原始候选")
    signal_status = str(raw_candidate.get("signal_status") or "").upper()
    if signal_status not in _ACTIONABLE_SHADOW_SIGNAL_STATUSES:
        raise DynamicShadowLedgerError("bootstrap候选信号状态不可执行")
    industry = _verified_industry_fact(
        connection,
        trade_date=plan["trade_date"],
        stock_code=plan["stock_code"],
    )
    payload = {
        "schema": BOOTSTRAP_AUTHORIZATION_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_run_uid": plan["candidate_run_uid"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "candidate_fact_hash": plan["candidate_fact_hash"],
        "candidate_signal_hash": plan["candidate_signal_hash"],
        "strategy_key": plan["strategy_key"],
        "strategy_version": plan["strategy_version"],
        "strategy_version_hash": plan["strategy_version_hash"],
        "execution_binding_hash": plan["execution_binding_hash"],
        "trade_date": plan["trade_date"],
        "stock_code": plan["stock_code"],
        "account_id": plan["account_id"],
        "maximum_target_bp": int(plan["maximum_target_bp"]),
        "industry_snapshot_id": industry["snapshot_id"],
        "industry_row_hash": industry["row_hash"],
        "industry_name": industry["industry_name"],
        "industry_type": industry["industry_type"],
        "shadow_forecast_id": _digest({
            "schema": "probiga.dynamic-shadow-bootstrap-forecast.v1",
            "plan_id": plan["plan_id"],
            "plan_hash": plan["plan_hash"],
        }),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**payload, "authorization_hash": _digest(payload)}


def verify_dynamic_shadow_bootstrap_authorization(
    connection: Any,
    authorization: Mapping[str, Any],
    *,
    require_current_shadow: bool,
    _verified_plan: Mapping[str, Any] | None = None,
    _verified_industry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    observed = json.loads(_canonical_json(authorization))
    authorization_hash = str(observed.pop("authorization_hash", ""))
    if (
        observed.get("schema") != BOOTSTRAP_AUTHORIZATION_SCHEMA
        or not _SHA256_PATTERN.fullmatch(authorization_hash)
        or _digest(observed) != authorization_hash
        or observed.get("automatic_real_order_submission") is not False
        or observed.get("real_order_authority") is not False
    ):
        raise DynamicShadowLedgerError("bootstrap授权身份、权限或哈希无效")
    plan = (
        dict(_verified_plan)
        if _verified_plan is not None
        else verify_dynamic_shadow_trial_plan(
            connection,
            str(observed.get("plan_id") or ""),
        )
    )
    exact = {
        "plan_hash": plan["plan_hash"],
        "candidate_run_uid": plan["candidate_run_uid"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "candidate_fact_hash": plan["candidate_fact_hash"],
        "candidate_signal_hash": plan["candidate_signal_hash"],
        "strategy_key": plan["strategy_key"],
        "strategy_version": plan["strategy_version"],
        "strategy_version_hash": plan["strategy_version_hash"],
        "execution_binding_hash": plan["execution_binding_hash"],
        "trade_date": plan["trade_date"],
        "stock_code": plan["stock_code"],
        "account_id": plan["account_id"],
        "maximum_target_bp": int(plan["maximum_target_bp"]),
    }
    if any(observed.get(field) != value for field, value in exact.items()):
        raise DynamicShadowLedgerError("bootstrap授权与影子计划精确身份不一致")
    industry = (
        dict(_verified_industry)
        if _verified_industry is not None
        else _verified_industry_fact(
            connection,
            trade_date=plan["trade_date"],
            stock_code=plan["stock_code"],
        )
    )
    expected_observed = {
        "schema": BOOTSTRAP_AUTHORIZATION_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_run_uid": plan["candidate_run_uid"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "candidate_fact_hash": plan["candidate_fact_hash"],
        "candidate_signal_hash": plan["candidate_signal_hash"],
        "strategy_key": plan["strategy_key"],
        "strategy_version": plan["strategy_version"],
        "strategy_version_hash": plan["strategy_version_hash"],
        "execution_binding_hash": plan["execution_binding_hash"],
        "trade_date": plan["trade_date"],
        "stock_code": plan["stock_code"],
        "account_id": plan["account_id"],
        "maximum_target_bp": int(plan["maximum_target_bp"]),
        "industry_snapshot_id": industry["snapshot_id"],
        "industry_row_hash": industry["row_hash"],
        "industry_name": industry["industry_name"],
        "industry_type": industry["industry_type"],
        "shadow_forecast_id": _digest({
            "schema": "probiga.dynamic-shadow-bootstrap-forecast.v1",
            "plan_id": plan["plan_id"],
            "plan_hash": plan["plan_hash"],
        }),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if observed != expected_observed:
        raise DynamicShadowLedgerError("bootstrap授权的行业snapshot/row_hash漂移")
    if require_current_shadow:
        _current_shadow_registry_version(connection, plan)
    return {
        **observed,
        "authorization_hash": authorization_hash,
        "plan": plan,
        "industry_fact": industry,
    }


def verify_dynamic_shadow_bootstrap_risk_binding(
    connection: Any,
    binding: Mapping[str, Any],
    *,
    intent_id: str,
    require_current_shadow: bool,
    _verified_plan: Mapping[str, Any] | None = None,
    _verified_industry: Mapping[str, Any] | None = None,
    _risk_decision: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay the trusted risk controller's exact bootstrap decision.

    The strategy adapter never receives portfolio/order state. The system
    risk controller freezes those inputs in this hash-bound receipt and this
    verifier compares it with the canonical V2 risk row before execution or
    forward-evidence binding may proceed.
    """

    observed = json.loads(_canonical_json(binding))
    binding_hash = str(observed.pop("binding_hash", ""))
    decision_payload = observed.get("decision_payload")
    if (
        observed.get("schema") != BOOTSTRAP_RISK_SCHEMA
        or not isinstance(decision_payload, Mapping)
        or not _SHA256_PATTERN.fullmatch(binding_hash)
        or _digest(observed) != binding_hash
        or observed.get("automatic_real_order_submission") is not False
        or observed.get("real_order_authority") is not False
        or set(observed) != {
            "schema", "decision_payload", "decision_hash",
            "automatic_real_order_submission", "real_order_authority",
        }
    ):
        raise DynamicShadowLedgerError("bootstrap风险绑定身份、权限或哈希无效")
    payload = json.loads(_canonical_json(decision_payload))
    expected_payload_keys = {
        "schema", "plan_id", "authorization_hash", "authorization",
        "intent_id", "account_id", "strategy_key", "strategy_version",
        "trade_date", "execution_date", "stock_code",
        "industry_snapshot_id", "industry_row_hash", "industry_name",
        "equity_cny", "reference_price", "worst_price", "initial_stop",
        "maximum_target_bp", "requested_quantity", "approved_quantity",
        "current_code_value", "current_total_value",
        "current_industry_value", "current_open_risk_cny",
        "current_daily_buy_turnover_cny", "available_cash_cny",
        "live_position_count", "limits", "decision_status", "checks",
        "first_failure", "trade_risk", "post_single_weight",
        "post_total_weight", "post_theme_weight", "post_open_risk_cny",
        "post_cash", "post_turnover_weight",
        "automatic_real_order_submission", "real_order_authority",
    }
    if set(payload) != expected_payload_keys:
        raise DynamicShadowLedgerError("bootstrap风险决策字段集合发生注入或缺失")
    authorization = payload.get("authorization")
    if not isinstance(authorization, Mapping):
        raise DynamicShadowLedgerError("bootstrap风险绑定缺少影子计划授权")
    verified = verify_dynamic_shadow_bootstrap_authorization(
        connection,
        authorization,
        require_current_shadow=require_current_shadow,
        _verified_plan=_verified_plan,
        _verified_industry=_verified_industry,
    )
    expected_intent_id = str(intent_id or "")
    decision_hash = _digest(payload)
    if (
        payload.get("schema")
        != "probiga.dynamic-shadow-bootstrap-risk-decision.v1"
        or str(payload.get("intent_id") or "") != expected_intent_id
        or str(payload.get("plan_id") or "") != verified["plan_id"]
        or str(payload.get("authorization_hash") or "")
        != verified["authorization_hash"]
        or str(payload.get("account_id") or "") != verified["account_id"]
        or str(payload.get("stock_code") or "") != verified["stock_code"]
        or str(payload.get("strategy_key") or "") != verified["strategy_key"]
        or str(payload.get("strategy_version") or "")
        != verified["strategy_version"]
        or str(payload.get("industry_snapshot_id") or "")
        != verified["industry_snapshot_id"]
        or str(payload.get("industry_row_hash") or "")
        != verified["industry_row_hash"]
        or str(observed.get("decision_hash") or "") != decision_hash
        or payload.get("automatic_real_order_submission") is not False
        or payload.get("real_order_authority") is not False
    ):
        raise DynamicShadowLedgerError("bootstrap风险决策与计划、行业或意图不一致")
    row = (
        dict(_risk_decision)
        if _risk_decision is not None
        else _one(connection, """
            SELECT intent_id, decision_status, requested_quantity,
                   approved_quantity, trade_risk, post_single_weight,
                   post_total_weight, post_theme_weight, post_open_risk,
                   post_cash, checks_json, first_failure, decision_hash,
                   created_at
            FROM st_risk_decision_v2 WHERE intent_id=:intent_id
        """, {"intent_id": expected_intent_id}, label="bootstrap V2风险决策")
    )
    checks = _strict_json(
        row.get("checks_json"), label="bootstrap V2风险检查", expected=dict,
    )
    expected_checks = payload.get("checks")
    required_checks = {
        "CASH_AVAILABLE",
        "SINGLE_POSITION_CAP",
        "TOTAL_RISK_ASSET_CAP",
        "THEME_EXPOSURE_CAP",
        "OPEN_RISK_CAP",
        "DAILY_TURNOVER_CAP",
        "LIVE_POSITION_CAP",
        "REAL_TRADING_DISABLED",
    }
    if (
        not isinstance(expected_checks, Mapping)
        or checks != json.loads(_canonical_json(expected_checks))
        or set(checks) != required_checks
        or any(value is not True for value in checks.values())
        or str(row.get("intent_id") or "") != expected_intent_id
        or str(row.get("decision_status") or "") != "APPROVED"
        or int(row.get("requested_quantity") or 0) <= 0
        or int(row.get("approved_quantity") or 0)
        != int(row.get("requested_quantity") or 0)
        or int(payload.get("requested_quantity") or 0)
        != int(row.get("requested_quantity") or 0)
        or int(payload.get("approved_quantity") or 0)
        != int(row.get("approved_quantity") or 0)
        or str(row.get("decision_hash") or "") != decision_hash
        or str(row.get("first_failure") or "")
        != str(payload.get("first_failure") or "")
    ):
        raise DynamicShadowLedgerError("bootstrap V2风险决策未通过或与绑定漂移")
    numeric_fields = (
        ("trade_risk", "trade_risk"),
        ("post_single_weight", "post_single_weight"),
        ("post_total_weight", "post_total_weight"),
        ("post_theme_weight", "post_theme_weight"),
        ("post_open_risk", "post_open_risk_cny"),
        ("post_cash", "post_cash"),
    )
    if any(
        Decimal(str(row.get(row_name) or 0))
        != Decimal(str(payload.get(payload_name) or 0))
        for row_name, payload_name in numeric_fields
    ):
        raise DynamicShadowLedgerError("bootstrap V2风险数值与冻结绑定漂移")
    equity = Decimal(str(payload.get("equity_cny") or 0))
    worst_price = Decimal(str(payload.get("worst_price") or 0))
    requested = Decimal(int(row.get("requested_quantity") or 0))
    maximum_target_bp = int(payload.get("maximum_target_bp") or 0)
    limits = payload.get("limits")
    if (
        equity <= 0
        or worst_price <= 0
        or not 1 <= maximum_target_bp <= 100
        or maximum_target_bp != int(verified["maximum_target_bp"])
        or requested * worst_price / equity
        > Decimal(maximum_target_bp) / Decimal(10000)
        or not isinstance(limits, Mapping)
        or Decimal(str(payload.get("post_single_weight") or 0))
        > Decimal(str(limits.get("maximum_single_weight") or 0))
        or Decimal(str(payload.get("post_total_weight") or 0))
        > Decimal(str(limits.get("maximum_total_weight") or 0))
        or Decimal(str(payload.get("post_theme_weight") or 0))
        > Decimal(str(limits.get("maximum_industry_weight") or 0))
        or Decimal(str(payload.get("post_open_risk_cny") or 0)) / equity
        > Decimal(str(limits.get("maximum_open_risk_weight") or 0))
        or Decimal(str(payload.get("post_turnover_weight") or 0))
        > Decimal(str(limits.get("maximum_daily_buy_turnover_weight") or 0))
    ):
        raise DynamicShadowLedgerError("bootstrap风险上限未被精确执行")
    return {
        **observed,
        "binding_hash": binding_hash,
        "decision_hash": decision_hash,
        "decision_payload": payload,
        "risk_decision": dict(row),
        "authorization": verified,
    }


def verify_dynamic_shadow_trial_plan(
    connection: Any,
    plan_id: str,
) -> dict[str, Any]:
    """Replay one persisted candidate-backed, no-order-authority trial plan."""

    row = _one(
        connection,
        _PLAN_SELECT,
        {"plan_id": str(plan_id)},
        label="动态影子试验计划",
    )
    receipt = _verified_receipt(
        connection,
        run_uid=str(row.get("candidate_run_uid") or ""),
        receipt_hash=str(row.get("candidate_receipt_hash") or ""),
    )
    batch_facts = verify_persisted_strategy_adapter_candidate_facts(
        connection,
        candidate_receipt=receipt,
    )
    return _verify_dynamic_shadow_trial_plan_row(
        row,
        receipt=receipt,
        batch_facts=batch_facts,
    )


def _verify_dynamic_shadow_trial_plan_row(
    row: Mapping[str, Any],
    *,
    receipt: Mapping[str, Any],
    batch_facts: Mapping[str, Any],
) -> dict[str, Any]:
    """Replay one already-loaded plan and its complete candidate batch."""

    if (
        int(row.get("automatic_real_order_submission") or 0) != 0
        or int(row.get("real_order_authority") or 0) != 0
        or str(row.get("account_id") or "") != INTERNAL_PAPER_ACCOUNT_ID
        or str(row.get("plan_status") or "") != "PLANNED_SHADOW_TRIAL"
        or not 1 <= int(row.get("maximum_target_bp") or 0) <= 100
    ):
        raise DynamicShadowLedgerError("影子试验计划权限、账户、状态或仓位上限无效")
    for field in (
        "strategy_key",
        "strategy_version",
        "strategy_version_hash",
        "execution_binding_hash",
    ):
        if str(row.get(field) or "") != str(receipt.get(field) or ""):
            raise DynamicShadowLedgerError(f"影子试验计划字段{field}与回执漂移")
    matching_facts = [
        fact for fact in batch_facts["candidate_facts"]
        if str(fact.get("stock_code") or "") == str(row.get("stock_code") or "")
    ]
    if len(matching_facts) != 1:
        raise DynamicShadowLedgerError("影子试验找不到唯一持久化原始候选事实")
    candidate_fact = matching_facts[0]
    if str(candidate_fact.get("candidate_hash") or "") != str(
        row.get("candidate_fact_hash") or ""
    ):
        raise DynamicShadowLedgerError("影子试验原始候选事实哈希漂移")
    signal = _strict_json(
        row.get("candidate_signal_json"),
        label="影子试验候选",
        expected=dict,
    )
    signal = _verified_candidate_signal_envelope(
        signal,
        receipt,
        candidate_fact,
    )
    signal_hash = _digest(signal)
    if signal_hash != str(row.get("candidate_signal_hash") or ""):
        raise DynamicShadowLedgerError("影子试验候选哈希无效")
    payload = _plan_payload(row)
    stored_payload = _strict_json(
        row.get("plan_payload_json"),
        label="影子试验计划载荷",
        expected=dict,
    )
    plan_hash = _digest(payload)
    if (
        payload != stored_payload
        or plan_hash != str(row.get("plan_hash") or "")
        or _expected_plan_id(payload) != str(row.get("plan_id") or "")
        or payload["stock_code"] != str(signal.get("stock_code") or "")
        or payload["trade_date"] != str(receipt.get("trade_date") or "")
    ):
        raise DynamicShadowLedgerError("影子试验计划身份或哈希无效")
    return {
        **payload,
        "plan_id": str(row["plan_id"]),
        "plan_hash": plan_hash,
        "candidate_signal": signal,
        "candidate_fact": candidate_fact,
        "candidate_receipt": receipt,
    }


def create_dynamic_shadow_trial_plan(
    connection: Any,
    *,
    strategy: Mapping[str, Any],
    candidate_receipt: Mapping[str, Any],
    candidate_signal: Mapping[str, Any],
    maximum_target_bp: int = 100,
) -> dict[str, Any]:
    """Persist an explicit, capped internal-paper trial request.

    This producer grants neither real-order nor paper-order authority.  The
    separate trusted bootstrap risk controller may consume this exact plan to
    create a capped internal-paper V2/V3 path; the plan itself cannot do so.
    """

    if not isinstance(strategy, Mapping) or not isinstance(
        candidate_receipt, Mapping
    ):
        raise DynamicShadowLedgerError("动态策略和候选回执必须是对象")
    if type(maximum_target_bp) is not int or not 1 <= maximum_target_bp <= 100:
        raise DynamicShadowLedgerError("影子试验最大仓位必须为1至100个基点")
    lifecycle = str(strategy.get("current_status") or "")
    if (
        strategy.get("enabled") is not True
        or lifecycle != "SHADOW"
        or str(strategy.get("source_kind") or "") != "runtime_registry"
    ):
        raise DynamicShadowLedgerError("只有启用且处于影子观察的动态策略可创建影子试验")
    receipt_hash = str(candidate_receipt.get("receipt_hash") or "")
    run_uid = str(candidate_receipt.get("run_uid") or "")
    if not _SHA256_PATTERN.fullmatch(receipt_hash):
        raise DynamicShadowLedgerError("动态候选回执哈希无效")
    receipt = _verified_receipt(
        connection,
        run_uid=run_uid,
        receipt_hash=receipt_hash,
    )
    if _canonical_json(receipt) != _canonical_json(candidate_receipt):
        raise DynamicShadowLedgerError("调用方候选回执与持久化事实不一致")
    for strategy_field, receipt_field in (
        ("strategy_key", "strategy_key"),
        ("current_version", "strategy_version"),
        ("version_hash", "strategy_version_hash"),
    ):
        if str(strategy.get(strategy_field) or "") != str(
            receipt.get(receipt_field) or ""
        ):
            raise DynamicShadowLedgerError(
                f"动态策略字段{strategy_field}与候选回执不一致"
            )
    batch_facts = verify_persisted_strategy_adapter_candidate_facts(
        connection,
        candidate_receipt=receipt,
    )
    signal_code = str(candidate_signal.get("stock_code") or "").strip().zfill(6)
    matching_facts = [
        fact for fact in batch_facts["candidate_facts"]
        if str(fact.get("stock_code") or "") == signal_code
    ]
    if len(matching_facts) != 1:
        raise DynamicShadowLedgerError("影子试验缺少唯一持久化原始候选事实")
    candidate_fact = matching_facts[0]
    if not _is_actionable_shadow_candidate(candidate_fact.get("candidate")):
        raise DynamicShadowLedgerError(
            "影子试验只接受明确BUY且已就绪的持久化原始候选"
        )
    signal = _candidate_signal_contract(
        candidate_signal, receipt, candidate_fact,
    )
    signal_hash = _digest(signal)
    seed = {
        "candidate_run_uid": run_uid,
        "candidate_receipt_hash": receipt_hash,
        "strategy_key": str(receipt["strategy_key"]),
        "strategy_version": str(receipt["strategy_version"]),
        "strategy_version_hash": str(receipt["strategy_version_hash"]),
        "execution_binding_hash": str(receipt["execution_binding_hash"]),
        "trade_date": str(receipt["trade_date"]),
        "stock_code": str(signal["stock_code"]),
        "account_id": INTERNAL_PAPER_ACCOUNT_ID,
        "maximum_target_bp": maximum_target_bp,
        "candidate_fact_hash": str(candidate_fact["candidate_hash"]),
        "candidate_signal_hash": signal_hash,
        "plan_status": "PLANNED_SHADOW_TRIAL",
    }
    payload = _plan_payload(seed)
    plan_id = _expected_plan_id(payload)
    existing = _rows(
        connection,
        _PLAN_SELECT,
        {"plan_id": plan_id},
    )
    if existing:
        verified = verify_dynamic_shadow_trial_plan(connection, plan_id)
        if verified["plan_hash"] != _digest(payload):
            raise DynamicShadowLedgerError("同一候选影子试验计划发生冲突")
        return {**verified, "idempotent_replay": True}
    connection.execute(text("""
        INSERT INTO st_dynamic_shadow_trial_plan (
            plan_id, candidate_run_uid, candidate_receipt_hash,
            strategy_key, strategy_version, strategy_version_hash,
            execution_binding_hash, trade_date, stock_code, account_id,
            maximum_target_bp, candidate_signal_json,
            candidate_fact_hash, candidate_signal_hash,
            plan_payload_json, plan_hash,
            plan_status, automatic_real_order_submission,
            real_order_authority
        ) VALUES (
            :plan_id, :candidate_run_uid, :candidate_receipt_hash,
            :strategy_key, :strategy_version, :strategy_version_hash,
            :execution_binding_hash, :trade_date, :stock_code, :account_id,
            :maximum_target_bp, :candidate_signal_json,
            :candidate_fact_hash, :candidate_signal_hash,
            :plan_payload_json, :plan_hash,
            :plan_status, 0, 0
        )
    """), {
        **seed,
        "plan_id": plan_id,
        "candidate_signal_json": _canonical_json(signal),
        "plan_payload_json": _canonical_json(payload),
        "plan_hash": _digest(payload),
    })
    return {
        **verify_dynamic_shadow_trial_plan(connection, plan_id),
        "idempotent_replay": False,
    }


def create_dynamic_shadow_trial_plans_from_candidate_facts(
    connection: Any,
    *,
    strategy: Mapping[str, Any],
    candidate_receipt: Mapping[str, Any],
    maximum_target_bp: int = 100,
) -> dict[str, Any]:
    """Create the deterministic bounded plan set behind one persisted run.

    This is the production post-receipt producer used by strategy center.  It
    consumes only replayed raw candidate facts and never submits either paper
    or real orders.  A separate trusted controller may later materialize a
    bounded internal-paper intent/order, while V2 remains the sole matcher and
    the scheduled V3 worker remains the sole forward-evidence producer.
    """

    batch = verify_persisted_strategy_adapter_candidate_facts(
        connection,
        candidate_receipt=candidate_receipt,
    )
    eligible_facts = [
        fact for fact in batch["candidate_facts"]
        if _is_actionable_shadow_candidate(fact.get("candidate"))
    ]
    plans = [
        create_dynamic_shadow_trial_plan(
            connection,
            strategy=strategy,
            candidate_receipt=batch["candidate_receipt"],
            candidate_signal=fact["candidate"],
            maximum_target_bp=maximum_target_bp,
        )
        for fact in eligible_facts
    ]
    contract = {
        "schema": "probiga.dynamic-shadow-trial-plan-set.v2",
        "candidate_run_uid": str(candidate_receipt.get("run_uid") or ""),
        "candidate_receipt_hash": str(
            candidate_receipt.get("receipt_hash") or ""
        ),
        "strategy_key": str(candidate_receipt.get("strategy_key") or ""),
        "strategy_version": str(
            candidate_receipt.get("strategy_version") or ""
        ),
        "maximum_target_bp": maximum_target_bp,
        "candidate_fact_count": len(batch["candidate_facts"]),
        "eligible_candidate_count": len(eligible_facts),
        "ineligible_candidate_count": (
            len(batch["candidate_facts"]) - len(eligible_facts)
        ),
        "plan_count": len(plans),
        "plan_ids": [str(plan["plan_id"]) for plan in plans],
        "plan_hashes": [str(plan["plan_hash"]) for plan in plans],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**contract, "plan_set_hash": _digest(contract)}


_INTENT_FIELDS = (
    "intent_id", "account_id", "decision_run_uid", "strategy_version",
    "stock_code", "action", "current_quantity", "target_quantity",
    "target_weight", "earliest_at", "expires_at", "limit_price",
    "worst_price", "initial_stop", "protective_stop",
    "invalidation_condition", "reason_code", "evidence_json",
    "intent_version", "idempotency_key", "created_at",
)
_ORDER_FIELDS = (
    "order_id", "account_id", "intent_id", "stock_code", "side",
    "order_type", "limit_price", "quantity", "filled_quantity", "status",
    "waiting_reason", "earliest_at", "expires_at", "idempotency_key",
    "created_at", "updated_at",
)
_FILL_FIELDS = (
    "fill_id", "order_id", "account_id", "stock_code", "side", "quantity",
    "price", "gross_amount", "fee_amount", "net_cash_amount",
    "quote_event_id", "match_event_id", "idempotency_key", "filled_at",
    "created_at",
)
_EVIDENCE_FIELDS = (
    "evidence_id", "account_id", "source_run_uid", "source_forecast_id",
    "source_intent_id", "stock_code", "strategy_key", "strategy_version",
    "sample_owner_role", "attribution_status", "attribution_version",
    "supporting_strategy_keys_json", "ownership_hash", "evidence_kind",
    "protocol_version", "entry_order_id", "entry_fill_id",
    "entry_trade_date", "entry_at", "entry_quantity", "entry_price",
    "entry_gross_cny", "entry_fee_cny", "closed_quantity",
    "exit_fill_ids_json", "exit_order_ids_json", "exit_at",
    "exit_average_price", "exit_gross_cny", "exit_fee_cny",
    "realized_net_pnl_cny", "realized_net_return_pct", "realized_mae_pct",
    "realized_mfe_pct", "exit_reason", "evidence_status",
)
_ALLOCATION_FIELDS = (
    "allocation_id", "evidence_id", "attribution_status", "account_id",
    "stock_code", "entry_fill_id", "exit_fill_id", "exit_order_id",
    "allocation_sequence", "allocated_quantity", "allocated_gross_cny",
    "allocated_fee_cny", "exit_filled_at", "allocation_protocol_version",
)
_RISK_FIELDS = (
    "intent_id", "decision_status", "requested_quantity",
    "approved_quantity", "trade_risk", "post_single_weight",
    "post_total_weight", "post_theme_weight", "post_open_risk",
    "post_cash", "checks_json", "first_failure", "decision_hash",
    "created_at",
)


def _fact_payload(
    kind: str,
    row: Mapping[str, Any],
    fields: Iterable[str],
    *,
    json_fields: Iterable[str] = (),
) -> dict[str, Any]:
    json_names = set(json_fields)
    values: dict[str, Any] = {}
    for field in fields:
        if field not in row:
            raise DynamicShadowLedgerError(f"{kind}事实缺少字段{field}")
        value = row.get(field)
        if field in json_names:
            expected_json_type = (
                dict if field in {"evidence_json", "checks_json"} else list
            )
            value = _strict_json(
                value,
                label=f"{kind}.{field}",
                expected=expected_json_type,
            )
        values[field] = _canonical_value(value)
    return {
        "schema": "probiga.dynamic-shadow-existing-paper-fact.v1",
        "fact_kind": kind,
        "fields": values,
    }


def _paper_fact_rows(
    connection: Any,
    *,
    plan: Mapping[str, Any],
    forward_evidence_id: str,
) -> dict[str, Any]:
    evidence = _one(connection, """
        SELECT evidence_id, account_id, source_run_uid, source_forecast_id,
               source_intent_id, stock_code, strategy_key, strategy_version,
               sample_owner_role, attribution_status, attribution_version,
               supporting_strategy_keys_json, ownership_hash, evidence_kind,
               protocol_version, entry_order_id, entry_fill_id,
               entry_trade_date, entry_at, entry_quantity, entry_price,
               entry_gross_cny, entry_fee_cny, closed_quantity,
               exit_fill_ids_json, exit_order_ids_json, exit_at,
               exit_average_price, exit_gross_cny, exit_fee_cny,
               realized_net_pnl_cny, realized_net_return_pct,
               realized_mae_pct, realized_mfe_pct, exit_reason,
               evidence_status
        FROM st_forward_trade_evidence_v3
        WHERE evidence_id=:evidence_id
    """, {"evidence_id": forward_evidence_id}, label="V3前向证据")
    intent = _one(connection, """
        SELECT intent_id, account_id, decision_run_uid, strategy_version,
               stock_code, action, current_quantity, target_quantity,
               target_weight, earliest_at, expires_at, limit_price,
               worst_price, initial_stop, protective_stop,
               invalidation_condition, reason_code, evidence_json,
               intent_version, idempotency_key, created_at
        FROM st_trade_intent_v2 WHERE intent_id=:intent_id
    """, {"intent_id": evidence.get("source_intent_id")}, label="V2模拟买入意图")
    risk_decision = _one(connection, """
        SELECT intent_id, decision_status, requested_quantity,
               approved_quantity, trade_risk, post_single_weight,
               post_total_weight, post_theme_weight, post_open_risk,
               post_cash, checks_json, first_failure, decision_hash,
               created_at
        FROM st_risk_decision_v2 WHERE intent_id=:intent_id
    """, {"intent_id": evidence.get("source_intent_id")}, label="V2模拟风险决策")
    entry_order = _one(connection, """
        SELECT order_id, account_id, intent_id, stock_code, side,
               order_type, limit_price, quantity, filled_quantity, status,
               waiting_reason, earliest_at, expires_at, idempotency_key,
               created_at, updated_at
        FROM st_order_v2 WHERE order_id=:order_id
    """, {"order_id": evidence.get("entry_order_id")}, label="V2模拟买入订单")
    entry_fill = _one(connection, """
        SELECT fill_id, order_id, account_id, stock_code, side, quantity,
               price, gross_amount, fee_amount, net_cash_amount,
               quote_event_id, match_event_id, idempotency_key, filled_at,
               created_at
        FROM st_fill_v2 WHERE fill_id=:fill_id
    """, {"fill_id": evidence.get("entry_fill_id")}, label="V2模拟买入成交")
    allocations = _rows(connection, """
        SELECT allocation_id, evidence_id, attribution_status, account_id,
               stock_code, entry_fill_id, exit_fill_id, exit_order_id,
               allocation_sequence, allocated_quantity,
               allocated_gross_cny, allocated_fee_cny, exit_filled_at,
               allocation_protocol_version
        FROM st_forward_exit_allocation_v3
        WHERE evidence_id=:evidence_id AND entry_fill_id=:entry_fill_id
        ORDER BY allocation_sequence, allocation_id
    """, {
        "evidence_id": forward_evidence_id,
        "entry_fill_id": evidence.get("entry_fill_id"),
    })
    exits: list[dict[str, Any]] = []
    for allocation in allocations:
        exit_order = _one(connection, """
            SELECT order_id, account_id, intent_id, stock_code, side,
                   order_type, limit_price, quantity, filled_quantity, status,
                   waiting_reason, earliest_at, expires_at, idempotency_key,
                   created_at, updated_at
            FROM st_order_v2 WHERE order_id=:order_id
        """, {"order_id": allocation.get("exit_order_id")}, label="V2模拟退出订单")
        exit_fill = _one(connection, """
            SELECT fill_id, order_id, account_id, stock_code, side, quantity,
                   price, gross_amount, fee_amount, net_cash_amount,
                   quote_event_id, match_event_id, idempotency_key, filled_at,
                   created_at
            FROM st_fill_v2 WHERE fill_id=:fill_id
        """, {"fill_id": allocation.get("exit_fill_id")}, label="V2模拟退出成交")
        exits.append({
            "allocation": allocation,
            "order": exit_order,
            "fill": exit_fill,
        })
    result = {
        "evidence": evidence,
        "intent": intent,
        "risk_decision": risk_decision,
        "entry_order": entry_order,
        "entry_fill": entry_fill,
        "exits": exits,
    }
    _validate_paper_fact_relationships(
        plan,
        result,
        connection=connection,
    )
    return result


def _validate_governance_receipt(
    intent_evidence: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    connection: Any,
    industry_fact: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bootstrap = intent_evidence.get("dynamic_shadow_bootstrap")
    if isinstance(bootstrap, Mapping):
        verified = verify_dynamic_shadow_bootstrap_authorization(
            connection,
            bootstrap,
            require_current_shadow=False,
            _verified_plan=plan,
            _verified_industry=industry_fact,
        )
        if (
            verified["plan_id"] != plan["plan_id"]
            or verified["plan_hash"] != plan["plan_hash"]
            or verified["strategy_key"] != plan["strategy_key"]
            or verified["strategy_version"] != plan["strategy_version"]
            or verified["stock_code"] != plan["stock_code"]
            or verified["real_order_authority"] is not False
        ):
            raise DynamicShadowLedgerError("bootstrap买入授权与计划不一致")
        return verified
    receipt = intent_evidence.get("strategy_governance")
    if not isinstance(receipt, Mapping):
        raise DynamicShadowLedgerError("V2模拟买入意图缺少治理个股计划回执")
    result = json.loads(_canonical_json(receipt))
    receipt_hash = str(result.pop("receipt_hash", ""))
    if (
        result.get("schema") != "probiga.governance-paper-buy-receipt.v1"
        or not _SHA256_PATTERN.fullmatch(receipt_hash)
        or _digest(result) != receipt_hash
        or result.get("new_buy_allowed") is not True
        or result.get("exit_always_allowed") is not True
        or result.get("real_order_authority") is not False
        or str(result.get("stock_code") or "") != plan["stock_code"]
        or str(result.get("strategy_key") or "") != plan["strategy_key"]
        or str(result.get("strategy_version") or "") != plan["strategy_version"]
        or str(result.get("strategy_version_hash") or "")
        != plan["strategy_version_hash"]
        or str(result.get("strategy_source_kind") or "")
        != "runtime_registry"
        or str(result.get("trade_date") or "") != plan["trade_date"]
        or not 1 <= int(result.get("target_bp") or 0) <= int(
            plan["maximum_target_bp"]
        )
    ):
        raise DynamicShadowLedgerError("V2模拟买入意图的治理计划回执无效")
    return {**result, "receipt_hash": receipt_hash}


def _validate_paper_fact_relationships(
    plan: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    connection: Any,
    industry_fact: Mapping[str, Any] | None = None,
) -> None:
    evidence = facts["evidence"]
    intent = facts["intent"]
    entry_order = facts["entry_order"]
    entry_fill = facts["entry_fill"]
    exits = facts["exits"]
    account = INTERNAL_PAPER_ACCOUNT_ID
    code = str(plan["stock_code"])
    if any(
        str(row.get("account_id") or "") != account
        or str(row.get("stock_code") or "") != code
        for row in (intent, entry_order, entry_fill, evidence)
    ):
        raise DynamicShadowLedgerError("影子证据链账户或股票身份不一致")
    if (
        str(intent.get("intent_id") or "") != str(evidence.get("source_intent_id") or "")
        or str(intent.get("decision_run_uid") or "") != str(evidence.get("source_run_uid") or "")
        or str(evidence.get("source_run_uid") or "")
        != str(plan.get("candidate_run_uid") or "")
        or str(intent.get("action") or "").upper() != "BUY"
        or str(intent.get("reason_code") or "") not in EXECUTED_INTENT_REASONS
        or str(entry_order.get("intent_id") or "") != str(intent.get("intent_id") or "")
        or str(entry_order.get("order_id") or "") != str(evidence.get("entry_order_id") or "")
        or str(entry_order.get("side") or "").upper() != "BUY"
        or str(entry_order.get("status") or "").upper() != "FILLED"
        or int(entry_order.get("quantity") or 0) != int(entry_order.get("filled_quantity") or -1)
        or str(entry_fill.get("order_id") or "") != str(entry_order.get("order_id") or "")
        or str(entry_fill.get("fill_id") or "") != str(evidence.get("entry_fill_id") or "")
        or str(entry_fill.get("side") or "").upper() != "BUY"
        or int(entry_fill.get("quantity") or 0) != int(evidence.get("entry_quantity") or -1)
        or str(evidence.get("strategy_key") or "") != str(plan["strategy_key"])
        or str(evidence.get("strategy_version") or "")
        != str(plan["strategy_version"])
        or str(evidence.get("sample_owner_role") or "") != "PRIMARY"
        or str(evidence.get("attribution_status") or "")
        != "VERIFIED_SNAPSHOT"
        or str(evidence.get("attribution_version") or "")
        != ATTRIBUTION_VERSION
        or str(evidence.get("evidence_kind") or "") != "EXECUTED_PAPER"
        or str(evidence.get("protocol_version") or "") != EXECUTED_FORWARD_PROTOCOL
        or str(evidence.get("evidence_status") or "") != "MATURED"
        or int(evidence.get("closed_quantity") or 0) != int(evidence.get("entry_quantity") or -1)
    ):
        raise DynamicShadowLedgerError("V2/V3模拟买入与成熟前向证据关系无效")
    intent_evidence = _strict_json(
        intent.get("evidence_json"),
        label="V2模拟意图证据",
        expected=dict,
    )
    governance_receipt = _validate_governance_receipt(
        intent_evidence,
        plan,
        connection=connection,
        industry_fact=industry_fact,
    )
    supporting_keys = _strict_json(
        evidence.get("supporting_strategy_keys_json"),
        label="V3前向证据策略归属集合",
        expected=list,
    )
    bootstrap_risk = intent_evidence.get("dynamic_shadow_risk")
    if isinstance(intent_evidence.get("dynamic_shadow_bootstrap"), Mapping):
        if str(intent.get("strategy_version") or "") != str(
            plan["strategy_version"]
        ):
            raise DynamicShadowLedgerError("bootstrap V2意图版本与影子计划不一致")
        if not isinstance(bootstrap_risk, Mapping):
            raise DynamicShadowLedgerError("bootstrap模拟意图缺少冻结风险绑定")
        verified_risk = verify_dynamic_shadow_bootstrap_risk_binding(
            connection,
            bootstrap_risk,
            intent_id=str(intent.get("intent_id") or ""),
            require_current_shadow=False,
            _verified_plan=plan,
            _verified_industry=industry_fact,
            _risk_decision=facts["risk_decision"],
        )
        if (
            verified_risk["authorization"]["plan_id"] != plan["plan_id"]
            or int(entry_order.get("quantity") or 0)
            != int(verified_risk["risk_decision"]["approved_quantity"] or 0)
            or str(evidence.get("source_run_uid") or "")
            != str(plan["candidate_run_uid"])
            or str(evidence.get("source_forecast_id") or "")
            != str(governance_receipt["shadow_forecast_id"])
            or supporting_keys != [plan["strategy_key"]]
            or str(intent_evidence.get("sample_owner_role") or "")
            != "PRIMARY"
            or str(intent_evidence.get("attribution_status") or "")
            != "VERIFIED_SNAPSHOT"
            or str(intent_evidence.get("attribution_version") or "")
            != ATTRIBUTION_VERSION
        ):
            raise DynamicShadowLedgerError("bootstrap风险绑定与计划或买入订单不一致")
    expected_ownership_hash = _versioned_ownership_hash(
        evidence.get("source_run_uid"),
        evidence.get("source_forecast_id"),
        evidence.get("stock_code"),
        evidence.get("strategy_key"),
        evidence.get("strategy_version"),
    )
    if (
        str(intent_evidence.get("primary_strategy_key") or "") != plan["strategy_key"]
        or str(intent_evidence.get("primary_strategy_version") or "")
        != str(plan["strategy_version"])
        or str(intent_evidence.get("primary_forecast_id") or "")
        != str(evidence.get("source_forecast_id") or "")
        or str(intent_evidence.get("run_uid") or "")
        != str(evidence.get("source_run_uid") or "")
        or plan["strategy_key"] not in {
            str(value) for value in supporting_keys
        }
        or str(evidence.get("strategy_version") or "")
        != str(plan["strategy_version"])
        or str(intent_evidence.get("ownership_hash") or "")
        != str(evidence.get("ownership_hash") or "")
        or str(evidence.get("ownership_hash") or "")
        != expected_ownership_hash
        or governance_receipt.get("real_order_authority") is not False
    ):
        raise DynamicShadowLedgerError("模拟意图的策略归属或治理回执与前向证据不一致")
    if not exits:
        raise DynamicShadowLedgerError("成熟前向证据缺少FIFO退出分配")
    allocated_quantity = 0
    exit_fill_ids: list[str] = []
    exit_order_ids: list[str] = []
    for item in exits:
        allocation = item["allocation"]
        order = item["order"]
        fill = item["fill"]
        if (
            str(allocation.get("evidence_id") or "") != str(evidence.get("evidence_id") or "")
            or str(allocation.get("attribution_status") or "") != "ATTRIBUTED"
            or str(allocation.get("account_id") or "") != account
            or str(allocation.get("stock_code") or "") != code
            or str(allocation.get("entry_fill_id") or "") != str(entry_fill.get("fill_id") or "")
            or str(allocation.get("allocation_protocol_version") or "") != EXIT_ALLOCATION_PROTOCOL
            or str(order.get("order_id") or "") != str(allocation.get("exit_order_id") or "")
            or str(order.get("account_id") or "") != account
            or str(order.get("stock_code") or "") != code
            or str(order.get("side") or "").upper() != "SELL"
            or str(order.get("status") or "").upper() != "FILLED"
            or int(order.get("quantity") or 0) != int(order.get("filled_quantity") or -1)
            or str(fill.get("fill_id") or "") != str(allocation.get("exit_fill_id") or "")
            or str(fill.get("order_id") or "") != str(order.get("order_id") or "")
            or str(fill.get("account_id") or "") != account
            or str(fill.get("stock_code") or "") != code
            or str(fill.get("side") or "").upper() != "SELL"
            or int(allocation.get("allocated_quantity") or 0) <= 0
        ):
            raise DynamicShadowLedgerError("FIFO退出分配与V2模拟退出事实不一致")
        allocated_quantity += int(allocation["allocated_quantity"])
        exit_fill_ids.append(str(fill["fill_id"]))
        exit_order_ids.append(str(order["order_id"]))
    evidence_fill_ids = _strict_json(
        evidence.get("exit_fill_ids_json"),
        label="V3前向证据退出成交集合",
        expected=list,
    )
    evidence_order_ids = _strict_json(
        evidence.get("exit_order_ids_json"),
        label="V3前向证据退出订单集合",
        expected=list,
    )
    if (
        allocated_quantity != int(evidence.get("closed_quantity") or 0)
        or sorted(set(exit_fill_ids)) != sorted(set(str(item) for item in evidence_fill_ids))
        or sorted(set(exit_order_ids)) != sorted(set(str(item) for item in evidence_order_ids))
    ):
        raise DynamicShadowLedgerError("FIFO退出数量或退出身份集合不守恒")


def _fact_contracts(facts: Mapping[str, Any]) -> dict[str, Any]:
    intent = _fact_payload(
        "PAPER_INTENT", facts["intent"], _INTENT_FIELDS,
        json_fields=("evidence_json",),
    )
    entry_order = _fact_payload("ENTRY_ORDER", facts["entry_order"], _ORDER_FIELDS)
    risk_decision = _fact_payload(
        "RISK_DECISION", facts["risk_decision"], _RISK_FIELDS,
        json_fields=("checks_json",),
    )
    entry_fill = _fact_payload("ENTRY_FILL", facts["entry_fill"], _FILL_FIELDS)
    evidence = _fact_payload(
        "FORWARD_EVIDENCE", facts["evidence"], _EVIDENCE_FIELDS,
        json_fields=(
            "supporting_strategy_keys_json",
            "exit_fill_ids_json",
            "exit_order_ids_json",
        ),
    )
    exits: list[dict[str, Any]] = []
    for item in facts["exits"]:
        allocation = _fact_payload(
            "EXIT_ALLOCATION", item["allocation"], _ALLOCATION_FIELDS,
        )
        order = _fact_payload("EXIT_ORDER", item["order"], _ORDER_FIELDS)
        fill = _fact_payload("EXIT_FILL", item["fill"], _FILL_FIELDS)
        exits.append({
            "allocation": allocation,
            "allocation_hash": _digest(allocation),
            "order": order,
            "order_hash": _digest(order),
            "fill": fill,
            "fill_hash": _digest(fill),
        })
    return {
        "intent": intent,
        "intent_hash": _digest(intent),
        "entry_order": entry_order,
        "entry_order_hash": _digest(entry_order),
        "risk_decision": risk_decision,
        "risk_decision_hash": _digest(risk_decision),
        "entry_fill": entry_fill,
        "entry_fill_hash": _digest(entry_fill),
        "evidence": evidence,
        "evidence_hash": _digest(evidence),
        "exits": exits,
    }


def _chain_contract(
    plan: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> dict[str, Any]:
    contracts = _fact_contracts(facts)
    evidence = facts["evidence"]
    chain_id = _digest({
        "schema": TRIAL_CHAIN_ID_SCHEMA,
        "plan_id": plan["plan_id"],
        "forward_evidence_id": str(evidence["evidence_id"]),
    })
    exit_rows: list[dict[str, Any]] = []
    for source, contract in zip(facts["exits"], contracts["exits"]):
        allocation_id = str(source["allocation"]["allocation_id"])
        payload = {
            "schema": TRIAL_EXIT_BINDING_SCHEMA,
            "chain_id": chain_id,
            "allocation_id": allocation_id,
            "exit_order_id": str(source["order"]["order_id"]),
            "exit_fill_id": str(source["fill"]["fill_id"]),
            "allocation_fact_hash": contract["allocation_hash"],
            "exit_order_fact_hash": contract["order_hash"],
            "exit_fill_fact_hash": contract["fill_hash"],
            "real_order_authority": False,
        }
        exit_rows.append({
            **payload,
            "binding_id": _digest({
                "schema": "probiga.dynamic-shadow-trial-exit-identity.v1",
                "chain_id": chain_id,
                "allocation_id": allocation_id,
            }),
            "binding_hash": _digest(payload),
        })
    exit_rows.sort(key=lambda item: (item["allocation_id"], item["binding_id"]))
    exit_set_hash = _digest([
        {"binding_id": row["binding_id"], "binding_hash": row["binding_hash"]}
        for row in exit_rows
    ])
    payload = {
        "schema": TRIAL_CHAIN_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "source_intent_id": str(facts["intent"]["intent_id"]),
        "entry_order_id": str(facts["entry_order"]["order_id"]),
        "entry_fill_id": str(facts["entry_fill"]["fill_id"]),
        "forward_evidence_id": str(evidence["evidence_id"]),
        "intent_fact_hash": contracts["intent_hash"],
        "risk_decision_fact_hash": contracts["risk_decision_hash"],
        "entry_order_fact_hash": contracts["entry_order_hash"],
        "entry_fill_fact_hash": contracts["entry_fill_hash"],
        "forward_evidence_fact_hash": contracts["evidence_hash"],
        "exit_set_hash": exit_set_hash,
        "exit_binding_count": len(exit_rows),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {
        "chain_id": chain_id,
        "chain_payload": payload,
        "chain_hash": _digest(payload),
        "exit_rows": exit_rows,
    }


_CHAIN_SELECT = """
    SELECT chain_id, plan_id, source_intent_id, entry_order_id,
           entry_fill_id, forward_evidence_id, intent_fact_hash,
           risk_decision_fact_hash,
           entry_order_fact_hash, entry_fill_fact_hash,
           forward_evidence_fact_hash, exit_set_hash,
           exit_binding_count, chain_payload_json, chain_hash,
           automatic_real_order_submission, real_order_authority, created_at
    FROM st_dynamic_shadow_trial_chain WHERE plan_id=:plan_id
"""


def bind_dynamic_shadow_trial_to_existing_paper_evidence(
    connection: Any,
    *,
    plan_id: str,
    forward_evidence_id: str,
) -> dict[str, Any]:
    """Seal a completed plan using only existing, matured V2/V3 paper facts."""

    plan = verify_dynamic_shadow_trial_plan(connection, plan_id)
    existing = _rows(connection, _CHAIN_SELECT, {"plan_id": plan_id})
    if existing:
        verified = verify_dynamic_shadow_trial(connection, plan_id)
        if verified.get("forward_evidence_id") != forward_evidence_id:
            raise DynamicShadowLedgerError("影子试验计划已绑定另一条前向证据")
        return {**verified, "idempotent_replay": True}
    reused = _rows(connection, """
        SELECT plan_id
        FROM st_dynamic_shadow_trial_chain
        WHERE forward_evidence_id=:forward_evidence_id
          AND plan_id<>:plan_id
        ORDER BY plan_id
    """, {
        "forward_evidence_id": str(forward_evidence_id),
        "plan_id": str(plan_id),
    })
    if reused:
        raise DynamicShadowLedgerError(
            "同一成熟前向证据已绑定另一动态影子试验计划"
        )
    facts = _paper_fact_rows(
        connection,
        plan=plan,
        forward_evidence_id=str(forward_evidence_id),
    )
    contract = _chain_contract(plan, facts)
    _insert_dynamic_shadow_chain_contract(connection, contract)
    return {
        **verify_dynamic_shadow_trial(connection, plan_id),
        "idempotent_replay": False,
    }


def _insert_dynamic_shadow_chain_contract(
    connection: Any,
    contract: Mapping[str, Any],
) -> None:
    """Append a fully replayed chain contract without re-reading its facts."""

    payload = contract["chain_payload"]
    connection.execute(text("""
        INSERT INTO st_dynamic_shadow_trial_chain (
            chain_id, plan_id, source_intent_id, entry_order_id,
            entry_fill_id, forward_evidence_id, intent_fact_hash,
            risk_decision_fact_hash,
            entry_order_fact_hash, entry_fill_fact_hash,
            forward_evidence_fact_hash, exit_set_hash,
            exit_binding_count, chain_payload_json, chain_hash,
            automatic_real_order_submission, real_order_authority
        ) VALUES (
            :chain_id, :plan_id, :source_intent_id, :entry_order_id,
            :entry_fill_id, :forward_evidence_id, :intent_fact_hash,
            :risk_decision_fact_hash,
            :entry_order_fact_hash, :entry_fill_fact_hash,
            :forward_evidence_fact_hash, :exit_set_hash,
            :exit_binding_count, :chain_payload_json, :chain_hash, 0, 0
        )
    """), {
        **payload,
        "chain_id": contract["chain_id"],
        "chain_payload_json": _canonical_json(payload),
        "chain_hash": contract["chain_hash"],
    })
    for row in contract["exit_rows"]:
        exit_payload = {
            key: row[key]
            for key in (
                "schema", "chain_id", "allocation_id", "exit_order_id",
                "exit_fill_id", "allocation_fact_hash",
                "exit_order_fact_hash", "exit_fill_fact_hash",
                "real_order_authority",
            )
        }
        connection.execute(text("""
            INSERT INTO st_dynamic_shadow_trial_exit_binding (
                binding_id, chain_id, allocation_id, exit_order_id,
                exit_fill_id, allocation_fact_hash,
                exit_order_fact_hash, exit_fill_fact_hash,
                binding_payload_json, binding_hash, real_order_authority
            ) VALUES (
                :binding_id, :chain_id, :allocation_id, :exit_order_id,
                :exit_fill_id, :allocation_fact_hash,
                :exit_order_fact_hash, :exit_fill_fact_hash,
                :binding_payload_json, :binding_hash, 0
            )
        """), {
            **row,
            "binding_payload_json": _canonical_json(exit_payload),
        })


def verify_dynamic_shadow_trial(
    connection: Any,
    plan_id: str,
) -> dict[str, Any]:
    """Recompute one complete candidate→paper→exit→forward chain."""

    plan = verify_dynamic_shadow_trial_plan(connection, plan_id)
    row = _one(
        connection,
        _CHAIN_SELECT,
        {"plan_id": plan_id},
        label="动态影子试验完整链",
    )
    if (
        int(row.get("automatic_real_order_submission") or 0) != 0
        or int(row.get("real_order_authority") or 0) != 0
    ):
        raise DynamicShadowLedgerError("动态影子完整链错误声明真实下单权限")
    facts = _paper_fact_rows(
        connection,
        plan=plan,
        forward_evidence_id=str(row.get("forward_evidence_id") or ""),
    )
    expected = _chain_contract(plan, facts)
    payload = expected["chain_payload"]
    stored_payload = _strict_json(
        row.get("chain_payload_json"),
        label="动态影子完整链载荷",
        expected=dict,
    )
    scalar_fields = (
        "plan_id", "source_intent_id", "entry_order_id", "entry_fill_id",
        "forward_evidence_id", "intent_fact_hash", "risk_decision_fact_hash",
        "entry_order_fact_hash",
        "entry_fill_fact_hash", "forward_evidence_fact_hash", "exit_set_hash",
    )
    if (
        str(row.get("chain_id") or "") != expected["chain_id"]
        or str(row.get("chain_hash") or "") != expected["chain_hash"]
        or int(row.get("exit_binding_count") or 0) != len(expected["exit_rows"])
        or stored_payload != payload
        or any(str(row.get(field) or "") != str(payload.get(field) or "") for field in scalar_fields)
    ):
        raise DynamicShadowLedgerError("动态影子完整链身份或哈希复算失败")
    stored_exits = _rows(connection, """
        SELECT binding_id, chain_id, allocation_id, exit_order_id,
               exit_fill_id, allocation_fact_hash, exit_order_fact_hash,
               exit_fill_fact_hash, binding_payload_json, binding_hash,
               real_order_authority
        FROM st_dynamic_shadow_trial_exit_binding
        WHERE chain_id=:chain_id
        ORDER BY allocation_id, binding_id
    """, {"chain_id": expected["chain_id"]})
    if len(stored_exits) != len(expected["exit_rows"]):
        raise DynamicShadowLedgerError("动态影子退出绑定数量不一致")
    for stored, wanted in zip(stored_exits, expected["exit_rows"]):
        wanted_payload = {
            key: wanted[key]
            for key in (
                "schema", "chain_id", "allocation_id", "exit_order_id",
                "exit_fill_id", "allocation_fact_hash",
                "exit_order_fact_hash", "exit_fill_fact_hash",
                "real_order_authority",
            )
        }
        stored_payload = _strict_json(
            stored.get("binding_payload_json"),
            label="动态影子退出绑定载荷",
            expected=dict,
        )
        if (
            int(stored.get("real_order_authority") or 0) != 0
            or stored_payload != wanted_payload
            or any(
                str(stored.get(field) or "") != str(wanted.get(field) or "")
                for field in (
                    "binding_id", "chain_id", "allocation_id",
                    "exit_order_id", "exit_fill_id", "allocation_fact_hash",
                    "exit_order_fact_hash", "exit_fill_fact_hash",
                    "binding_hash",
                )
            )
        ):
            raise DynamicShadowLedgerError("动态影子退出绑定哈希复算失败")
    return {
        "schema": "probiga.dynamic-shadow-trial-verification.v1",
        "status": "VERIFIED_MATURED_INTERNAL_PAPER_CHAIN",
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_run_uid": plan["candidate_run_uid"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "chain_id": expected["chain_id"],
        "chain_hash": expected["chain_hash"],
        "strategy_key": plan["strategy_key"],
        "strategy_version": plan["strategy_version"],
        "stock_code": plan["stock_code"],
        "forward_evidence_id": payload["forward_evidence_id"],
        "exit_binding_count": len(expected["exit_rows"]),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


_PENDING_PLAN_FILTER = """
    c.plan_id IS NULL
    AND p.account_id=:account_id
    AND p.plan_status='PLANNED_SHADOW_TRIAL'
    AND p.automatic_real_order_submission=0
    AND p.real_order_authority=0
"""

_PENDING_EVIDENCE_PAIR_FROM = f"""
    FROM st_dynamic_shadow_trial_plan p
    LEFT JOIN st_dynamic_shadow_trial_chain c ON c.plan_id=p.plan_id
    JOIN st_forward_trade_evidence_v3 e
      ON e.account_id=p.account_id
     AND e.stock_code=p.stock_code
     AND e.strategy_key=p.strategy_key
     AND e.strategy_version=p.strategy_version
     AND e.source_run_uid=p.candidate_run_uid
     AND e.evidence_kind='EXECUTED_PAPER'
     AND e.protocol_version=:protocol_version
     AND e.evidence_status='MATURED'
    LEFT JOIN st_dynamic_shadow_trial_chain reused
      ON reused.forward_evidence_id=e.evidence_id
    WHERE {_PENDING_PLAN_FILTER}
      AND reused.forward_evidence_id IS NULL
"""


def _bounded_related_rows(
    connection: Any,
    *,
    count_statement: str,
    select_statement: str,
    params: Mapping[str, Any],
    expanding_names: tuple[str, ...],
    label: str,
) -> list[dict[str, Any]]:
    count = _scalar_count(
        connection, count_statement, params, *expanding_names,
    )
    if count == 0:
        return []
    rows = _rows_expanding(
        connection,
        select_statement + " LIMIT :row_limit",
        {**dict(params), "row_limit": count + 1},
        *expanding_names,
    )
    if len(rows) != count:
        raise DynamicShadowLedgerError(
            f"{label}精确计数与批量读取不一致：{count}/{len(rows)}"
        )
    return rows


def _unique_rows_by(
    rows: Iterable[Mapping[str, Any]],
    key_name: str,
    *,
    label: str,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        key = str(row.get(key_name) or "")
        if not key or key in result:
            raise DynamicShadowLedgerError(f"{label}身份为空或重复")
        result[key] = row
    return result


def _batch_industry_rows(
    connection: Any,
    keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    exact_keys = sorted({(str(day)[:10], str(code)) for day, code in keys})
    if not exact_keys:
        return {}
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for index, (trade_date, stock_code) in enumerate(exact_keys):
        clauses.append(
            f"(trade_date=:industry_day_{index} "
            f"AND stock_code=:industry_code_{index})"
        )
        params[f"industry_day_{index}"] = trade_date
        params[f"industry_code_{index}"] = stock_code
    exact_filter = " OR ".join(clauses)
    row_count = _scalar_count(connection, f"""
        SELECT COUNT(*)
        FROM st_strategy_industry_history
        WHERE {exact_filter}
    """, params)
    # The immutable capture contract allows at most one snapshot row for one
    # (trade_date, stock_code).  Its physical PK also contains snapshot_id, so
    # count first and cap the application read even if storage was corrupted.
    if row_count > len(exact_keys):
        raise DynamicShadowLedgerError("目标日行业历史事实数量超过精确身份边界")
    if row_count == 0:
        return {}
    rows = _rows(connection, f"""
        SELECT snapshot_id, trade_date, as_of_exclusive, stock_code,
               industry_name, industry_type, source_system, source_fact_id,
               source_effective_at, source_etl_sync_at, row_hash
        FROM st_strategy_industry_history
        WHERE {exact_filter}
        ORDER BY trade_date, stock_code, snapshot_id
        LIMIT :row_limit
    """, {**params, "row_limit": row_count + 1})
    if len(rows) != row_count:
        raise DynamicShadowLedgerError("目标日行业历史事实精确计数发生漂移")
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("trade_date") or "")[:10], str(row.get("stock_code") or ""))
        if key in result:
            raise DynamicShadowLedgerError("目标日行业历史事实重复")
        result[key] = _verified_industry_row(
            row, trade_date=key[0], stock_code=key[1],
        )
    return result


def _batch_pending_binding_facts(
    connection: Any,
    pair_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    run_uids = sorted({str(row["candidate_run_uid"]) for row in pair_rows})
    evidence_ids = sorted({str(row["evidence_id"]) for row in pair_rows})
    receipt_rows = _rows_expanding(connection, """
        SELECT run_uid, strategy_key, strategy_version, strategy_version_hash,
               execution_binding_hash, adapter_artifact_sha256, cost_model_hash,
               adapter_key, adapter_version, trade_date, completed_at, status,
               input_hash, output_hash, stable_result_hash, candidate_count,
               candidate_identity_json, receipt_json, receipt_hash
        FROM st_strategy_adapter_run_receipt
        WHERE run_uid IN :run_uids
        ORDER BY run_uid
    """, {"run_uids": run_uids}, "run_uids")
    receipt_rows_by_run = _unique_rows_by(
        receipt_rows, "run_uid", label="动态候选运行回执",
    )
    candidate_rows = _bounded_related_rows(
        connection,
        count_statement="""
            SELECT COUNT(*) FROM st_strategy_adapter_candidate_fact
            WHERE candidate_run_uid IN :run_uids
        """,
        select_statement="""
            SELECT candidate_run_uid, stock_code, candidate_index, trade_date,
                   candidate_json, candidate_hash
            FROM st_strategy_adapter_candidate_fact
            WHERE candidate_run_uid IN :run_uids
            ORDER BY candidate_run_uid, candidate_index
        """,
        params={"run_uids": run_uids},
        expanding_names=("run_uids",),
        label="动态候选事实",
    )
    candidate_rows_by_run: dict[str, list[dict[str, Any]]] = {}
    for row in candidate_rows:
        candidate_rows_by_run.setdefault(
            str(row.get("candidate_run_uid") or ""), [],
        ).append(row)

    evidence_rows = _rows_expanding(connection, """
        SELECT evidence_id, account_id, source_run_uid, source_forecast_id,
               source_intent_id, stock_code, strategy_key, strategy_version,
               sample_owner_role, attribution_status, attribution_version,
               supporting_strategy_keys_json, ownership_hash, evidence_kind,
               protocol_version, entry_order_id, entry_fill_id,
               entry_trade_date, entry_at, entry_quantity, entry_price,
               entry_gross_cny, entry_fee_cny, closed_quantity,
               exit_fill_ids_json, exit_order_ids_json, exit_at,
               exit_average_price, exit_gross_cny, exit_fee_cny,
               realized_net_pnl_cny, realized_net_return_pct,
               realized_mae_pct, realized_mfe_pct, exit_reason,
               evidence_status
        FROM st_forward_trade_evidence_v3
        WHERE evidence_id IN :evidence_ids
        ORDER BY evidence_id
    """, {"evidence_ids": evidence_ids}, "evidence_ids")
    evidence_by_id = _unique_rows_by(
        evidence_rows, "evidence_id", label="成熟V3前向证据",
    )
    intent_ids = sorted({
        str(row.get("source_intent_id") or "") for row in evidence_rows
    } - {""})
    intents = _rows_expanding(connection, """
        SELECT intent_id, account_id, decision_run_uid, strategy_version,
               stock_code, action, current_quantity, target_quantity,
               target_weight, earliest_at, expires_at, limit_price,
               worst_price, initial_stop, protective_stop,
               invalidation_condition, reason_code, evidence_json,
               intent_version, idempotency_key, created_at
        FROM st_trade_intent_v2 WHERE intent_id IN :intent_ids
        ORDER BY intent_id
    """, {"intent_ids": intent_ids}, "intent_ids") if intent_ids else []
    intent_by_id = _unique_rows_by(intents, "intent_id", label="V2模拟买入意图")
    risks = _rows_expanding(connection, """
        SELECT intent_id, decision_status, requested_quantity,
               approved_quantity, trade_risk, post_single_weight,
               post_total_weight, post_theme_weight, post_open_risk,
               post_cash, checks_json, first_failure, decision_hash,
               created_at
        FROM st_risk_decision_v2 WHERE intent_id IN :intent_ids
        ORDER BY intent_id
    """, {"intent_ids": intent_ids}, "intent_ids") if intent_ids else []
    risk_by_intent = _unique_rows_by(
        risks, "intent_id", label="V2模拟风险决策",
    )

    allocations = _bounded_related_rows(
        connection,
        count_statement="""
            SELECT COUNT(*) FROM st_forward_exit_allocation_v3
            WHERE evidence_id IN :evidence_ids
        """,
        select_statement="""
            SELECT allocation_id, evidence_id, attribution_status, account_id,
                   stock_code, entry_fill_id, exit_fill_id, exit_order_id,
                   allocation_sequence, allocated_quantity,
                   allocated_gross_cny, allocated_fee_cny, exit_filled_at,
                   allocation_protocol_version
            FROM st_forward_exit_allocation_v3
            WHERE evidence_id IN :evidence_ids
            ORDER BY evidence_id, allocation_sequence, allocation_id
        """,
        params={"evidence_ids": evidence_ids},
        expanding_names=("evidence_ids",),
        label="V3 FIFO退出分配",
    )
    allocations_by_evidence: dict[str, list[dict[str, Any]]] = {}
    for row in allocations:
        allocations_by_evidence.setdefault(
            str(row.get("evidence_id") or ""), [],
        ).append(row)

    order_ids = sorted(({
        str(row.get("entry_order_id") or "") for row in evidence_rows
    } | {
        str(row.get("exit_order_id") or "") for row in allocations
    }) - {""})
    orders = _rows_expanding(connection, """
        SELECT order_id, account_id, intent_id, stock_code, side,
               order_type, limit_price, quantity, filled_quantity, status,
               waiting_reason, earliest_at, expires_at, idempotency_key,
               created_at, updated_at
        FROM st_order_v2 WHERE order_id IN :order_ids
        ORDER BY order_id
    """, {"order_ids": order_ids}, "order_ids") if order_ids else []
    order_by_id = _unique_rows_by(orders, "order_id", label="V2模拟订单")
    fill_ids = sorted(({
        str(row.get("entry_fill_id") or "") for row in evidence_rows
    } | {
        str(row.get("exit_fill_id") or "") for row in allocations
    }) - {""})
    fills = _rows_expanding(connection, """
        SELECT fill_id, order_id, account_id, stock_code, side, quantity,
               price, gross_amount, fee_amount, net_cash_amount,
               quote_event_id, match_event_id, idempotency_key, filled_at,
               created_at
        FROM st_fill_v2 WHERE fill_id IN :fill_ids
        ORDER BY fill_id
    """, {"fill_ids": fill_ids}, "fill_ids") if fill_ids else []
    fill_by_id = _unique_rows_by(fills, "fill_id", label="V2模拟成交")
    industry_by_key = _batch_industry_rows(connection, (
        (str(row.get("trade_date") or "")[:10], str(row.get("stock_code") or ""))
        for row in pair_rows
    ))
    return {
        "receipt_rows_by_run": receipt_rows_by_run,
        "candidate_rows_by_run": candidate_rows_by_run,
        "evidence_by_id": evidence_by_id,
        "intent_by_id": intent_by_id,
        "risk_by_intent": risk_by_intent,
        "allocations_by_evidence": allocations_by_evidence,
        "order_by_id": order_by_id,
        "fill_by_id": fill_by_id,
        "industry_by_key": industry_by_key,
    }


def _prefetched_paper_facts(
    connection: Any,
    *,
    plan: Mapping[str, Any],
    evidence_id: str,
    batch: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = dict(batch["evidence_by_id"][evidence_id])
    intent_id = str(evidence.get("source_intent_id") or "")
    intent = dict(batch["intent_by_id"][intent_id])
    risk = dict(batch["risk_by_intent"][intent_id])
    entry_order = dict(batch["order_by_id"][str(evidence.get("entry_order_id") or "")])
    entry_fill = dict(batch["fill_by_id"][str(evidence.get("entry_fill_id") or "")])
    exits: list[dict[str, Any]] = []
    for allocation in batch["allocations_by_evidence"].get(evidence_id, []):
        exits.append({
            "allocation": dict(allocation),
            "order": dict(batch["order_by_id"][str(allocation.get("exit_order_id") or "")]),
            "fill": dict(batch["fill_by_id"][str(allocation.get("exit_fill_id") or "")]),
        })
    result = {
        "evidence": evidence,
        "intent": intent,
        "risk_decision": risk,
        "entry_order": entry_order,
        "entry_fill": entry_fill,
        "exits": exits,
    }
    intent_payload = _strict_json(
        intent.get("evidence_json"), label="V2模拟意图证据", expected=dict,
    )
    industry = batch["industry_by_key"].get((
        str(plan["trade_date"])[:10], str(plan["stock_code"]),
    ))
    if isinstance(intent_payload.get("dynamic_shadow_bootstrap"), Mapping) and not industry:
        raise DynamicShadowLedgerError("bootstrap成熟证据缺少目标日行业历史事实")
    _validate_paper_fact_relationships(
        plan, result, connection=connection, industry_fact=industry,
    )
    return result


def _bind_pending_dynamic_shadow_trials_on_connection(
    connection: Any,
) -> dict[str, Any]:
    params = {
        "account_id": INTERNAL_PAPER_ACCOUNT_ID,
        "protocol_version": EXECUTED_FORWARD_PROTOCOL,
    }
    total_pending_count = _scalar_count(connection, f"""
        SELECT COUNT(*)
        FROM st_dynamic_shadow_trial_plan p
        LEFT JOIN st_dynamic_shadow_trial_chain c ON c.plan_id=p.plan_id
        WHERE {_PENDING_PLAN_FILTER}
    """, params)
    pair_count = _scalar_count(
        connection,
        "SELECT COUNT(*) " + _PENDING_EVIDENCE_PAIR_FROM,
        params,
    )
    pair_rows = (
        _rows(connection, """
            SELECT p.plan_id, p.candidate_run_uid,
                   p.candidate_receipt_hash, p.strategy_key,
                   p.strategy_version, p.strategy_version_hash,
                   p.execution_binding_hash, p.trade_date, p.stock_code,
                   p.account_id, p.maximum_target_bp, p.candidate_fact_hash,
                   p.candidate_signal_json, p.candidate_signal_hash,
                   p.plan_payload_json, p.plan_hash, p.plan_status,
                   p.automatic_real_order_submission,
                   p.real_order_authority, p.created_at, e.evidence_id
            """ + _PENDING_EVIDENCE_PAIR_FROM + """
            ORDER BY p.trade_date, p.plan_id, e.entry_at, e.evidence_id
            LIMIT :row_limit
        """, {**params, "row_limit": pair_count + 1})
        if pair_count
        else []
    )
    if len(pair_rows) != pair_count:
        raise DynamicShadowLedgerError("待绑定计划/成熟证据精确计数发生漂移")
    plan_rows: dict[str, dict[str, Any]] = {}
    evidence_ids_by_plan: dict[str, list[str]] = {}
    for pair in pair_rows:
        plan_id = str(pair.get("plan_id") or "")
        evidence_id = str(pair.get("evidence_id") or "")
        plan_row = {key: value for key, value in pair.items() if key != "evidence_id"}
        if plan_id in plan_rows and _canonical_json(plan_rows[plan_id]) != _canonical_json(plan_row):
            raise DynamicShadowLedgerError("批量待绑定计划事实不一致")
        plan_rows[plan_id] = plan_row
        evidence_ids_by_plan.setdefault(plan_id, []).append(evidence_id)

    bound: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    if pair_rows:
        batch = _batch_pending_binding_facts(connection, pair_rows)
        verified_runs: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        run_errors: dict[str, Exception] = {}
        for run_uid in sorted(batch["receipt_rows_by_run"]):
            try:
                stored = batch["receipt_rows_by_run"][run_uid]
                receipt = _verified_receipt_row(
                    stored, receipt_hash=str(stored.get("receipt_hash") or ""),
                )
                candidate_batch = _verify_candidate_fact_rows(
                    receipt, batch["candidate_rows_by_run"].get(run_uid, []),
                )
                verified_runs[run_uid] = (receipt, candidate_batch)
            except Exception as exc:
                run_errors[run_uid] = exc
        for plan_id in sorted(plan_rows):
            row = plan_rows[plan_id]
            run_uid = str(row.get("candidate_run_uid") or "")
            try:
                if run_uid in run_errors:
                    raise run_errors[run_uid]
                receipt, candidate_batch = verified_runs[run_uid]
                plan = _verify_dynamic_shadow_trial_plan_row(
                    row, receipt=receipt, batch_facts=candidate_batch,
                )
                eligible: list[tuple[str, dict[str, Any]]] = []
                evidence_errors: list[dict[str, str]] = []
                for evidence_id in evidence_ids_by_plan[plan_id]:
                    try:
                        facts = _prefetched_paper_facts(
                            connection,
                            plan=plan,
                            evidence_id=evidence_id,
                            batch=batch,
                        )
                        eligible.append((evidence_id, facts))
                    except Exception as exc:
                        evidence_errors.append({
                            "evidence_id": evidence_id,
                            "error_type": type(exc).__name__,
                        })
                if evidence_errors:
                    rejected.append({
                        "plan_id": plan_id,
                        "reason": "EXACT_MATURED_EVIDENCE_INVALID",
                        "invalid_evidence": evidence_errors,
                    })
                    continue
                if len(eligible) != 1:
                    rejected.append({
                        "plan_id": plan_id,
                        "reason": "MULTIPLE_EXACT_MATURED_EVIDENCE_MATCHES",
                        "eligible_evidence_ids": [item[0] for item in eligible],
                    })
                    continue
                evidence_id, facts = eligible[0]
                contract = _chain_contract(plan, facts)
                # A chain and all of its exit bindings are one immutable fact.
                # The scheduled binder deliberately records and continues past
                # one bad plan, so a plain INSERT followed by a caught exit-row
                # failure would otherwise commit a permanently partial chain.
                # A per-plan savepoint makes that continue-on-error policy safe.
                with connection.begin_nested():
                    _insert_dynamic_shadow_chain_contract(connection, contract)
                bound.append({
                    "plan_id": plan_id,
                    "forward_evidence_id": evidence_id,
                    "chain_hash": str(contract["chain_hash"]),
                })
            except Exception as exc:
                rejected.append({
                    "plan_id": plan_id,
                    "reason": type(exc).__name__,
                })
    processed_plan_ids = sorted(plan_rows)
    bound_plan_ids = {
        str(item.get("plan_id") or "") for item in bound
    }
    processed_pending_plan_ids = sorted(
        set(processed_plan_ids) - bound_plan_ids
    )
    unmatched_pending_count = max(0, total_pending_count - len(processed_plan_ids))
    remaining_unbound_count = max(0, total_pending_count - len(bound))
    contract = {
        "processed_plan_ids": processed_plan_ids,
        "bound": bound,
        # Unmatched plans are intentionally count-only: loading every old plan
        # merely to report IDs would reintroduce the starvation/scan problem.
        "still_pending_plan_ids": processed_pending_plan_ids,
        "unmatched_pending_plan_count": unmatched_pending_count,
        "remaining_unbound_plan_count": remaining_unbound_count,
        "rejected": rejected,
    }
    return {
        "schema": "probiga.dynamic-shadow-scheduled-binding.v1",
        "status": "INVALID" if rejected else "OK",
        "processed_plan_count": len(processed_plan_ids),
        "bound_plan_count": len(bound),
        "pending_plan_count": remaining_unbound_count,
        "rejected_plan_count": len(rejected),
        **contract,
        "binding_result_hash": _digest(contract),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def bind_pending_dynamic_shadow_trials(engine: Any) -> dict[str, Any]:
    """Bind mature V2/V3 facts during the existing forward-evidence schedule.

    This worker is read-only with respect to intents/orders/fills.  Its only
    writes are immutable FK/hash bindings to facts that already exist and pass
    the complete replay contract.
    """

    try:
        with engine.begin() as connection:
            return _bind_pending_dynamic_shadow_trials_on_connection(
                connection,
            )
    except Exception as exc:
        contract = {
            "error_type": type(exc).__name__,
            "processed_plan_ids": [],
            "bound": [],
            "still_pending_plan_ids": [],
            "rejected": [],
        }
        return {
            "schema": "probiga.dynamic-shadow-scheduled-binding.v1",
            "status": "UNAVAILABLE_OR_INVALID",
            "processed_plan_count": 0,
            "bound_plan_count": 0,
            "pending_plan_count": 0,
            "rejected_plan_count": 1,
            **contract,
            "binding_result_hash": _digest(contract),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }


def _readiness_on_connection(
    connection: Any,
    *,
    strategy_key: str = "",
    strategy_version: str = "",
    strategy_version_hash: str = "",
    execution_binding_hash: str = "",
) -> dict[str, Any]:
    identity = {
        "strategy_key": str(strategy_key or ""),
        "strategy_version": str(strategy_version or ""),
        "strategy_version_hash": str(strategy_version_hash or ""),
        "execution_binding_hash": str(execution_binding_hash or ""),
    }
    if any(identity.values()) and not (
        identity["strategy_key"] and identity["strategy_version"]
    ):
        raise DynamicShadowLedgerError(
            "影子账本就绪度必须同时绑定策略键和精确策略版本"
        )
    params: dict[str, Any] = {}
    clauses: list[str] = []
    for field, value in identity.items():
        if value:
            clauses.append(f"{field}=:{field}")
            params[field] = value
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    plans = _rows(
        connection,
        "SELECT plan_id, strategy_key, strategy_version, "
        "strategy_version_hash, execution_binding_hash "
        "FROM st_dynamic_shadow_trial_plan"
        + where
        + " ORDER BY plan_id",
        params,
    )
    verified: list[dict[str, Any]] = []
    pending: list[str] = []
    invalid: list[dict[str, str]] = []
    for item in plans:
        plan_id = str(item.get("plan_id") or "")
        try:
            plan = verify_dynamic_shadow_trial_plan(connection, plan_id)
            for field, value in identity.items():
                if value and str(plan.get(field) or "") != value:
                    raise DynamicShadowLedgerError(
                        f"影子账本计划字段{field}越出就绪度版本边界"
                    )
            chain = _rows(
                connection,
                _CHAIN_SELECT,
                {"plan_id": plan_id},
            )
            if not chain:
                pending.append(plan_id)
            elif len(chain) == 1:
                verified.append(verify_dynamic_shadow_trial(connection, plan_id))
            else:
                raise DynamicShadowLedgerError("同一计划存在多条完整链")
            if plan.get("real_order_authority") is not False:
                raise DynamicShadowLedgerError("计划真实下单权限未关闭")
        except Exception as exc:
            invalid.append({
                "plan_id": plan_id,
                **_readiness_error_detail(exc),
            })
    producer_ready = not invalid
    # Capital eligibility requires at least one genuinely matured, replayable
    # internal-paper round trip for this exact strategy.  Merely deploying
    # empty tables cannot bootstrap funding authority.
    funding_ready = bool(verified) and producer_ready
    status = (
        "INVALID"
        if invalid
        else "VERIFIED"
        if verified
        else "VERIFIED_EMPTY"
        if not plans
        else "VERIFIED_PENDING"
    )
    ledger_contract = {
        **identity,
        "plan_ids": [str(row.get("plan_id") or "") for row in plans],
        "pending_plan_ids": pending,
        "verified_chain_hashes": sorted(
            str(row.get("chain_hash") or "") for row in verified
        ),
        "invalid_plan_ids": sorted(row["plan_id"] for row in invalid),
    }
    return {
        "schema": "probiga.dynamic-shadow-ledger-readiness.v1",
        "status": status,
        **identity,
        "schema_readable": True,
        "shadow_trial_producer_ready": producer_ready,
        "funding_pipeline_ready": funding_ready,
        "verified_forward_evidence_ready": funding_ready,
        "plan_count": len(plans),
        "pending_plan_count": len(pending),
        "verified_chain_count": len(verified),
        "invalid_chain_count": len(invalid),
        "invalid_chains": invalid,
        "ledger_hash": _digest(ledger_contract),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def dynamic_shadow_ledger_readiness(
    *,
    connection: Any | None = None,
    strategy_key: str = "",
    strategy_version: str = "",
    strategy_version_hash: str = "",
    execution_binding_hash: str = "",
) -> dict[str, Any]:
    """Return fail-closed readiness recomputed from the persistent ledger."""

    owned_connection = None
    effective = connection or current_bound_sql_connection()
    try:
        if effective is None:
            from server.api.routers._engine import get_engine

            owned_connection = get_engine().connect()
            effective = owned_connection
        return _readiness_on_connection(
            effective,
            strategy_key=str(strategy_key or ""),
            strategy_version=str(strategy_version or ""),
            strategy_version_hash=str(strategy_version_hash or ""),
            execution_binding_hash=str(execution_binding_hash or ""),
        )
    except Exception as exc:
        return {
            "schema": "probiga.dynamic-shadow-ledger-readiness.v1",
            "status": "UNAVAILABLE_OR_INVALID",
            "strategy_key": str(strategy_key or ""),
            "strategy_version": str(strategy_version or ""),
            "strategy_version_hash": str(strategy_version_hash or ""),
            "execution_binding_hash": str(execution_binding_hash or ""),
            "schema_readable": False,
            "shadow_trial_producer_ready": False,
            "funding_pipeline_ready": False,
            "verified_forward_evidence_ready": False,
            "plan_count": 0,
            "pending_plan_count": 0,
            "verified_chain_count": 0,
            "invalid_chain_count": 1,
            "invalid_chains": [{
                "plan_id": "",
                **_readiness_error_detail(exc),
            }],
            "ledger_hash": "",
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    finally:
        if owned_connection is not None:
            owned_connection.close()


__all__ = [
    "BOOTSTRAP_REASON_CODE",
    "DynamicShadowLedgerError",
    "INTERNAL_PAPER_ACCOUNT_ID",
    "bind_dynamic_shadow_trial_to_existing_paper_evidence",
    "bind_pending_dynamic_shadow_trials",
    "build_dynamic_shadow_bootstrap_authorization",
    "create_dynamic_shadow_trial_plan",
    "create_dynamic_shadow_trial_plans_from_candidate_facts",
    "dynamic_shadow_ledger_readiness",
    "persist_strategy_adapter_candidate_facts",
    "verify_dynamic_shadow_trial",
    "verify_dynamic_shadow_bootstrap_authorization",
    "verify_dynamic_shadow_bootstrap_risk_binding",
    "verify_dynamic_shadow_industry_fact",
    "verify_dynamic_shadow_trial_plan",
    "verify_persisted_strategy_adapter_candidate_facts",
]
