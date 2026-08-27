# -*- coding: utf-8 -*-
"""Immutable MyQuant historical upper-limit evidence for Frozen V4.

The producer is deliberately evidence-only: it never derives a limit price,
never edits ``sm_stock_kline`` and never treats a diagnostic QMT comparison as
an authority.  One completed run must cover one exact strategy code set across
the exact 21-session window before any recommendation can consume it.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from integrations.myquant.bridge import (
    UPPER_LIMIT_HISTORY_ACTION,
    UPPER_LIMIT_HISTORY_COLUMNS,
    UPPER_LIMIT_HISTORY_FIELDS,
    upper_limit_history_evidence,
)
from server.common.analysis_pool_receipt import (
    build_upper_limit_evidence,
    validate_preliminary_upper_subject_receipt,
)
from server.common.mysql_lock import mysql_named_lock
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_immutability,
)
from server.common.turnover_snapshot_schema import (
    FIELD_CAPTURE_ROW_TABLE,
    FIELD_CAPTURE_RUN_TABLE,
)


UPPER_LIMIT_SNAPSHOT_VERSION = "probiga.market-field-capture.v1"
UPPER_LIMIT_CAPTURE_KIND = "DAILY_UPPER_LIMIT_HISTORY"
UPPER_LIMIT_PROVIDER = "myquant.gm.get_history_instruments"
UPPER_LIMIT_API_PATH = "gm.api.get_history_instruments"
UPPER_LIMIT_TRANSPORT = "MYQUANT_GM_SDK_FIXED_ACTION_V1"
UPPER_LIMIT_SOURCE_FIELD = "upper_limit"
UPPER_LIMIT_UNIT = "PRICE_CNY"
UPPER_LIMIT_MATCH_POLICY = "EXACT_STRATEGY_POOL_80_X_21"
UPPER_LIMIT_PROMOTION_MODE = "EVIDENCE_ONLY"
UPPER_LIMIT_SUBJECT_KIND = "STRATEGY_RECOMMENDATION_CODESET"
UPPER_LIMIT_EXPECTED_STOCK_COUNT = 80
UPPER_LIMIT_EXPECTED_DATE_COUNT = 21
MARKET_FIELD_CAPTURE_LOCK_NAME = "probiga:market-field-capture"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODE = re.compile(r"(?:0|3|6)[0-9]{5}")
_RUN_ID = re.compile(r"[0-9a-f]{32}")
_SHA40 = re.compile(r"[0-9a-f]{40}")
_SHA64 = re.compile(r"[0-9a-f]{64}")


def _now_shanghai() -> datetime:
    return datetime.now(_SHANGHAI).replace(tzinfo=None)


class UpperLimitSnapshotBlocked(RuntimeError):
    """Raised when one immutable upper-limit proof cannot be established."""


def _blocked(message: str) -> UpperLimitSnapshotBlocked:
    return UpperLimitSnapshotBlocked(f"DATA_BLOCKED: {message}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256(value: bytes | str | Any) -> str:
    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, str):
        payload = value.encode("utf-8")
    else:
        payload = _canonical_json(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _stock_code(value: Any) -> str:
    raw = str(value or "").strip()
    if _CODE.fullmatch(raw) is None:
        raise _blocked(f"unsupported strategy stock code {value!r}")
    return raw


def _gm_symbol_to_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if re.fullmatch(r"SHSE\.6[0-9]{5}", raw):
        return raw.split(".", 1)[1]
    if re.fullmatch(r"SZSE\.[03][0-9]{5}", raw):
        return raw.split(".", 1)[1]
    raise _blocked(f"unsupported MyQuant symbol {value!r}")


def _gm_symbol(code: str) -> str:
    normalized = _stock_code(code)
    return ("SHSE." if normalized.startswith("6") else "SZSE.") + normalized


def _exact_date(value: Any, *, field: str) -> date:
    raw = value.isoformat() if isinstance(value, date) else str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise _blocked(f"{field} is not an exact ISO date") from exc
    if parsed.isoformat() != raw:
        raise _blocked(f"{field} is not an exact ISO date")
    return parsed


def _local_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").strip())
        except ValueError as exc:
            raise _blocked(f"{field} is not an ISO datetime") from exc
    if parsed.tzinfo is not None:
        if parsed.utcoffset() != _SHANGHAI.utcoffset(parsed):
            parsed = parsed.astimezone(_SHANGHAI)
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _datetime_text(value: datetime) -> str:
    return _local_datetime(value, field="datetime").isoformat(
        sep=" ", timespec="microseconds"
    )


def _price(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise _blocked(f"{field} is not an exact price")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise _blocked(f"{field} is not an exact price") from exc
    if not number.is_finite() or number <= 0:
        raise _blocked(f"{field} is not a positive two-decimal price")
    canonical = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    # GM serializes these fields from float32 and may expose a sub-micro-cent
    # representation artifact (for example 8.9399995803833).  Canonicalize
    # that transport artifact, but reject any economically meaningful drift.
    if abs(number - canonical) > Decimal("0.0001"):
        raise _blocked(f"{field} differs from the cent price contract")
    return canonical


def _decimal_text(value: Decimal | Any) -> str:
    number = value if isinstance(value, Decimal) else _price(value, field="price")
    return format(number.quantize(Decimal("0.01")), ".2f")


def _bytes(value: Any, *, field: str) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise _blocked(f"{field} is not immutable bytes")


@dataclass(frozen=True)
class UpperLimitSubject:
    target_date: date
    stock_codes: tuple[str, ...]
    trade_dates: tuple[date, ...]
    subject_identity: str
    subject_sha256: str
    code_set_sha256: str
    trade_dates_sha256: str
    expected_keyset_sha256: str
    calendar_batch_id: str
    calendar_manifest_sha256: str
    calendar_session_set_sha256: str


@dataclass(frozen=True)
class CapturedUpperLimitRow:
    stock_code: str
    trade_date: date
    pre_close: Decimal
    upper_limit: Decimal
    lower_limit: Decimal
    is_suspended: int
    raw_row_text: str
    raw_payload: bytes
    raw_payload_sha256: str
    snapshot_row_sha256: str
    target_key_sha256: str
    captured_at: datetime


@dataclass(frozen=True)
class UpperLimitCaptureRun:
    run_id: str
    collector_build_sha: str
    subject: UpperLimitSubject
    subject_payload: bytes
    subject_payload_sha256: str
    decision_at: datetime
    request_started_at: datetime
    captured_at: datetime
    provider_response_payload: bytes
    provider_response_sha256: str
    provider_request_payload: bytes
    provider_request_sha256: str
    worker_sha256: str
    sdk_version: str
    runtime_version: str
    timezone: str
    entitlement_status: str
    raw_payload_root_sha256: str
    field_value_root_sha256: str
    target_fingerprint_root_sha256: str
    semantic_sha256: str
    rows: tuple[CapturedUpperLimitRow, ...]


def build_upper_limit_subject(
    *,
    target_date: date | str,
    stock_codes: Iterable[str],
    trade_dates: Iterable[date | str],
    expected_stock_count: int = UPPER_LIMIT_EXPECTED_STOCK_COUNT,
    expected_date_count: int = UPPER_LIMIT_EXPECTED_DATE_COUNT,
    calendar_batch_id: str = "",
    calendar_manifest_sha256: str = "",
    calendar_session_set_sha256: str = "",
    preliminary_receipt_sha256: str = "",
) -> UpperLimitSubject:
    """Freeze the exact pool codes and exact authoritative session keys."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    codes = tuple(sorted(_stock_code(value) for value in stock_codes))
    sessions = tuple(sorted(
        value if isinstance(value, date) else _exact_date(value, field="trade_date")
        for value in trade_dates
    ))
    if len(codes) != len(set(codes)) or len(codes) != int(expected_stock_count):
        raise _blocked(
            f"upper-limit subject requires exactly {int(expected_stock_count)} unique stocks"
        )
    if (
        len(sessions) != len(set(sessions))
        or len(sessions) != int(expected_date_count)
        or not sessions
        or sessions[-1] != target
    ):
        raise _blocked(
            f"upper-limit subject requires exactly {int(expected_date_count)} sessions ending at target"
        )
    code_payload = {
        "schema": "probiga.upper-limit-code-set.v1",
        "target_date": target.isoformat(),
        "stock_codes": list(codes),
    }
    date_payload = {
        "schema": "probiga.upper-limit-session-set.v1",
        "target_date": target.isoformat(),
        "trade_dates": [item.isoformat() for item in sessions],
    }
    code_hash = _sha256(code_payload)
    date_hash = _sha256(date_payload)
    calendar_batch = str(calendar_batch_id or "").strip()
    calendar_manifest = str(calendar_manifest_sha256 or "").strip().lower()
    calendar_sessions = str(calendar_session_set_sha256 or "").strip().lower()
    if any((calendar_batch, calendar_manifest, calendar_sessions)) and (
        not all((calendar_batch, calendar_manifest, calendar_sessions))
        or len(calendar_batch) > 64
        or _SHA64.fullmatch(calendar_manifest) is None
        or _SHA64.fullmatch(calendar_sessions) is None
    ):
        raise _blocked("upper-limit calendar authority proof is incomplete")
    preliminary_receipt = str(
        preliminary_receipt_sha256 or ""
    ).strip().lower()
    if preliminary_receipt and _SHA64.fullmatch(preliminary_receipt) is None:
        raise _blocked("upper-limit preliminary subject receipt is invalid")
    subject_payload = {
        "schema": "probiga.upper-limit-subject.v1",
        "target_date": target.isoformat(),
        "code_set_sha256": code_hash,
        "trade_dates_sha256": date_hash,
        "calendar_batch_id": calendar_batch,
        "calendar_manifest_sha256": calendar_manifest,
        "calendar_session_set_sha256": calendar_sessions,
        "preliminary_receipt_sha256": preliminary_receipt,
    }
    subject_hash = _sha256(subject_payload)
    keyset = [
        {"stock_code": code, "trade_date": session.isoformat()}
        for code in codes
        for session in sessions
    ]
    return UpperLimitSubject(
        target_date=target,
        stock_codes=codes,
        trade_dates=sessions,
        subject_identity=(
            f"preview:{preliminary_receipt}"
            if preliminary_receipt
            else f"{target.isoformat()}:{code_hash[:24]}"
        ),
        subject_sha256=subject_hash,
        code_set_sha256=code_hash,
        trade_dates_sha256=date_hash,
        expected_keyset_sha256=_sha256(keyset),
        calendar_batch_id=calendar_batch,
        calendar_manifest_sha256=calendar_manifest,
        calendar_session_set_sha256=calendar_sessions,
    )


def _canonical_source_row(
    *,
    stock_code: str,
    trade_date: date,
    pre_close: Decimal,
    upper_limit: Decimal,
    lower_limit: Decimal,
    is_suspended: int,
) -> dict[str, Any]:
    return {
        "symbol": _gm_symbol(stock_code),
        "trade_date": trade_date.isoformat(),
        "pre_close": _decimal_text(pre_close),
        "upper_limit": _decimal_text(upper_limit),
        "lower_limit": _decimal_text(lower_limit),
        "is_suspended": int(is_suspended),
    }


def _row_semantic_payload(
    *,
    subject: UpperLimitSubject,
    source_row: Mapping[str, Any],
    captured_at: datetime,
    provider_response_sha256: str,
    provider_request_sha256: str,
    worker_sha256: str,
) -> dict[str, Any]:
    return {
        "schema": "probiga.upper-limit-row.v1",
        "subject_sha256": subject.subject_sha256,
        "source": dict(source_row),
        "captured_at": _datetime_text(captured_at),
        "provider_response_sha256": provider_response_sha256,
        "provider_request_sha256": provider_request_sha256,
        "worker_sha256": worker_sha256,
    }


def build_upper_limit_capture_run(
    *,
    subject: UpperLimitSubject,
    bridge_result: Mapping[str, Any],
    decision_at: datetime | str,
    collector_build_sha: str,
    preliminary_receipt: Mapping[str, Any] | None = None,
    run_id: str | None = None,
) -> UpperLimitCaptureRun:
    """Validate one complete fixed-action response and freeze its hashes."""

    identity = str(run_id or uuid.uuid4().hex).lower()
    build_sha = str(collector_build_sha or "").strip().lower()
    cutoff = _local_datetime(decision_at, field="decision_at")
    if (
        _RUN_ID.fullmatch(identity) is None
        or _SHA40.fullmatch(build_sha) is None
        or build_sha == "0" * 40
    ):
        raise _blocked("upper-limit run/build identity is invalid")
    subject_payload = b""
    subject_payload_sha256 = ""
    preliminary_identity = (
        subject.subject_identity.split(":", 1)[1]
        if subject.subject_identity.startswith("preview:")
        else ""
    )
    if preliminary_identity:
        try:
            validated_preliminary = validate_preliminary_upper_subject_receipt(
                preliminary_receipt or {}
            )
        except ValueError as exc:
            raise _blocked(
                "upper-limit immutable preliminary receipt is invalid"
            ) from exc
        if (
            validated_preliminary["receipt_sha256"] != preliminary_identity
            or validated_preliminary["trade_date"]
            != subject.target_date.isoformat()
            or validated_preliminary["decision_at"]
            != cutoff.isoformat(timespec="seconds")
            or validated_preliminary["build_sha"] != build_sha
            or sorted(validated_preliminary["ordered_stock_codes"])
            != list(subject.stock_codes)
        ):
            raise _blocked(
                "upper-limit immutable preliminary receipt identity differs"
            )
        subject_payload = _canonical_json(validated_preliminary).encode("utf-8")
        subject_payload_sha256 = _sha256(subject_payload)
    elif preliminary_receipt is not None:
        raise _blocked("upper-limit diagnostic subject may not claim a preview")
    result = dict(bridge_result)
    if (
        result.get("ok") is not True
        or result.get("action") != UPPER_LIMIT_HISTORY_ACTION
        or result.get("fields") != UPPER_LIMIT_HISTORY_FIELDS
        or result.get("columns") != list(UPPER_LIMIT_HISTORY_COLUMNS)
        or result.get("entitlement_status") != "SUPPORTED"
        or result.get("timezone") != "Asia/Shanghai"
        or result.get("errors") != {}
    ):
        raise _blocked("MyQuant fixed-action response contract differs")
    requested = result.get("requested_symbols")
    expected_symbols = [_gm_symbol(code) for code in subject.stock_codes]
    if requested != expected_symbols:
        raise _blocked("MyQuant requested symbol set differs from frozen subject")
    if (
        str(result.get("start_date") or "") != subject.trade_dates[0].isoformat()
        or str(result.get("end_date") or "") != subject.target_date.isoformat()
    ):
        raise _blocked("MyQuant request window differs from frozen sessions")

    started = _local_datetime(result.get("request_started_at"), field="request_started_at")
    captured = _local_datetime(result.get("captured_at"), field="captured_at")
    if not started <= captured <= cutoff:
        raise _blocked("MyQuant response crossed the decision cutoff")

    raw_stdout = str(result.get("raw_stdout") or "")
    response_payload = raw_stdout.encode("utf-8")
    response_hash = str(result.get("raw_stdout_sha256") or "").lower()
    request_json = str(result.get("canonical_request_json") or "")
    request_payload = request_json.encode("utf-8")
    request_hash = str(result.get("canonical_request_sha256") or "").lower()
    worker_hash = str(result.get("worker_sha256") or "").lower()
    if (
        not response_payload
        or _SHA64.fullmatch(response_hash) is None
        or _sha256(response_payload) != response_hash
        or not request_payload
        or _SHA64.fullmatch(request_hash) is None
        or _sha256(request_payload) != request_hash
        or _SHA64.fullmatch(worker_hash) is None
    ):
        raise _blocked("MyQuant transport/request/worker hash differs")
    try:
        response_echo = json.loads(raw_stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _blocked("MyQuant raw worker response is invalid JSON") from exc
    capture_only_keys = {
        "raw_stdout",
        "raw_stdout_sha256",
        "canonical_request_json",
        "canonical_request_sha256",
        "worker_sha256",
    }
    if response_echo != {
        key: value for key, value in result.items() if key not in capture_only_keys
    }:
        raise _blocked("MyQuant raw worker response differs from decoded evidence")
    try:
        request_echo = json.loads(request_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _blocked("MyQuant canonical request is invalid JSON") from exc
    if request_echo != {
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "end_date": subject.target_date.isoformat(),
        "start_date": subject.trade_dates[0].isoformat(),
        "symbols": expected_symbols,
    }:
        raise _blocked("MyQuant canonical request differs from frozen subject")

    source_rows = result.get("rows")
    if not isinstance(source_rows, list):
        raise _blocked("MyQuant upper-limit rows are unavailable")
    expected_keys = {
        (code, session) for code in subject.stock_codes for session in subject.trade_dates
    }
    observed_keys: set[tuple[str, date]] = set()
    captured_rows: list[CapturedUpperLimitRow] = []
    for raw in source_rows:
        if not isinstance(raw, Mapping):
            raise _blocked("MyQuant upper-limit row is not an object")
        code = _gm_symbol_to_code(raw.get("symbol"))
        trade_at = _local_datetime(raw.get("trade_date"), field="GM trade_date")
        session = trade_at.date()
        key = (code, session)
        if key not in expected_keys or key in observed_keys:
            raise _blocked("MyQuant upper-limit row keyset contains extra/duplicate keys")
        observed_keys.add(key)
        pre_close = _price(raw.get("pre_close"), field="pre_close")
        upper_limit = _price(raw.get("upper_limit"), field="upper_limit")
        lower_limit = _price(raw.get("lower_limit"), field="lower_limit")
        suspended = raw.get("is_suspended")
        if isinstance(suspended, bool) or type(suspended) is not int or suspended != 0:
            raise _blocked(f"upper-limit subject contains suspended/unknown row {code} {session}")
        if not upper_limit > pre_close > lower_limit:
            raise _blocked(f"upper-limit price relationship differs for {code} {session}")
        canonical_source = _canonical_source_row(
            stock_code=code,
            trade_date=session,
            pre_close=pre_close,
            upper_limit=upper_limit,
            lower_limit=lower_limit,
            is_suspended=suspended,
        )
        row_text = _canonical_json(canonical_source)
        row_payload = row_text.encode("utf-8")
        raw_hash = _sha256(row_payload)
        semantic = _row_semantic_payload(
            subject=subject,
            source_row=canonical_source,
            captured_at=captured,
            provider_response_sha256=response_hash,
            provider_request_sha256=request_hash,
            worker_sha256=worker_hash,
        )
        target_key_hash = _sha256({
            "schema": "probiga.upper-limit-target-key.v1",
            "subject_sha256": subject.subject_sha256,
            "stock_code": code,
            "trade_date": session.isoformat(),
        })
        captured_rows.append(CapturedUpperLimitRow(
            stock_code=code,
            trade_date=session,
            pre_close=pre_close,
            upper_limit=upper_limit,
            lower_limit=lower_limit,
            is_suspended=suspended,
            raw_row_text=row_text,
            raw_payload=row_payload,
            raw_payload_sha256=raw_hash,
            snapshot_row_sha256=_sha256(semantic),
            target_key_sha256=target_key_hash,
            captured_at=captured,
        ))
    if observed_keys != expected_keys:
        raise _blocked(
            f"MyQuant upper-limit coverage is {len(observed_keys)}, expected {len(expected_keys)}"
        )
    captured_rows.sort(key=lambda item: (item.stock_code, item.trade_date))
    raw_root = _sha256([
        {
            "stock_code": row.stock_code,
            "trade_date": row.trade_date.isoformat(),
            "raw_payload_sha256": row.raw_payload_sha256,
        }
        for row in captured_rows
    ])
    target_root = _sha256([
        {
            "stock_code": row.stock_code,
            "trade_date": row.trade_date.isoformat(),
            "target_key_sha256": row.target_key_sha256,
        }
        for row in captured_rows
    ])
    value_root = _sha256([
        {
            "stock_code": row.stock_code,
            "trade_date": row.trade_date.isoformat(),
            "upper_limit": _decimal_text(row.upper_limit),
        }
        for row in captured_rows
    ])
    semantic_root = _sha256([
        {
            "stock_code": row.stock_code,
            "trade_date": row.trade_date.isoformat(),
            "snapshot_row_sha256": row.snapshot_row_sha256,
        }
        for row in captured_rows
    ])
    sdk_version = str(result.get("sdk_version") or "").strip()
    runtime_version = str(result.get("python_version") or "").strip()
    if not sdk_version or not runtime_version:
        raise _blocked("MyQuant SDK/runtime identity is missing")
    return UpperLimitCaptureRun(
        run_id=identity,
        collector_build_sha=build_sha,
        subject=subject,
        subject_payload=subject_payload,
        subject_payload_sha256=subject_payload_sha256,
        decision_at=cutoff,
        request_started_at=started,
        captured_at=captured,
        provider_response_payload=response_payload,
        provider_response_sha256=response_hash,
        provider_request_payload=request_payload,
        provider_request_sha256=request_hash,
        worker_sha256=worker_hash,
        sdk_version=sdk_version,
        runtime_version=runtime_version,
        timezone="Asia/Shanghai",
        entitlement_status="SUPPORTED",
        raw_payload_root_sha256=raw_root,
        field_value_root_sha256=value_root,
        target_fingerprint_root_sha256=target_root,
        semantic_sha256=semantic_root,
        rows=tuple(captured_rows),
    )


def collect_upper_limit_snapshot(
    *,
    subject: UpperLimitSubject,
    decision_at: datetime | str,
    collector_build_sha: str,
    preliminary_receipt: Mapping[str, Any] | None = None,
    timeout: int | None = None,
    run_id: str | None = None,
) -> UpperLimitCaptureRun:
    result = upper_limit_history_evidence(
        subject.stock_codes,
        start_date=subject.trade_dates[0].isoformat(),
        end_date=subject.target_date.isoformat(),
        timeout=timeout,
    )
    return build_upper_limit_capture_run(
        subject=subject,
        bridge_result=result,
        decision_at=decision_at,
        collector_build_sha=collector_build_sha,
        preliminary_receipt=preliminary_receipt,
        run_id=run_id,
    )


def _run_params(run: UpperLimitCaptureRun, *, published_at: datetime) -> dict[str, Any]:
    count = len(run.rows)
    return {
        "run_id": run.run_id,
        "schema_version": UPPER_LIMIT_SNAPSHOT_VERSION,
        "collector_build_sha": run.collector_build_sha,
        "capture_kind": UPPER_LIMIT_CAPTURE_KIND,
        "target_date": run.subject.target_date.isoformat(),
        "window_start_date": run.subject.trade_dates[0].isoformat(),
        "window_end_date": run.subject.target_date.isoformat(),
        "decision_at": _datetime_text(run.decision_at),
        "provider": UPPER_LIMIT_PROVIDER,
        "api_path": UPPER_LIMIT_API_PATH,
        "transport_contract": UPPER_LIMIT_TRANSPORT,
        "resolved_endpoint": "gm-sdk-token-session",
        "source_field": UPPER_LIMIT_SOURCE_FIELD,
        "unit": UPPER_LIMIT_UNIT,
        "match_policy": UPPER_LIMIT_MATCH_POLICY,
        "promotion_mode": UPPER_LIMIT_PROMOTION_MODE,
        "promotion_table": "",
        "promotion_column": "",
        "k_type": 1,
        "adjust_type": 0,
        "subject_kind": UPPER_LIMIT_SUBJECT_KIND,
        "subject_identity": run.subject.subject_identity,
        "subject_sha256": run.subject.subject_sha256,
        "subject_payload": run.subject_payload or None,
        "subject_payload_sha256": run.subject_payload_sha256 or None,
        "authority_proof_kind": "QMT_TRADE_CALENDAR",
        "authority_proof_identity": run.subject.calendar_batch_id,
        "authority_proof_sha256": run.subject.calendar_manifest_sha256,
        "authority_set_sha256": run.subject.calendar_session_set_sha256,
        "expected_count": count,
        "fetched_count": count,
        "valid_count": count,
        "matched_count": count,
        "promoted_count": 0,
        "expected_keyset_sha256": run.subject.expected_keyset_sha256,
        "provider_request_payload": run.provider_request_payload,
        "provider_request_sha256": run.provider_request_sha256,
        "provider_response_payload": run.provider_response_payload,
        "provider_response_sha256": run.provider_response_sha256,
        "collector_binary_sha256": run.worker_sha256,
        "provider_sdk_version": run.sdk_version,
        "collector_runtime_version": run.runtime_version,
        "source_timezone": run.timezone,
        "entitlement_status": run.entitlement_status,
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "target_fingerprint_root_sha256": run.target_fingerprint_root_sha256,
        "semantic_sha256": run.semantic_sha256,
        "request_started_at": _datetime_text(run.request_started_at),
        "captured_max_at": _datetime_text(run.captured_at),
        "provider_observed_max_at": _datetime_text(run.captured_at),
        "published_at": _datetime_text(published_at),
        "status": "BUILDING",
        "error_message": "",
        "created_at": _datetime_text(published_at),
    }


_RUN_INSERT_SQL = f"""
INSERT INTO {FIELD_CAPTURE_RUN_TABLE} (
  run_id, schema_version, collector_build_sha, capture_kind, target_date,
  window_start_date, window_end_date, decision_at,
  provider, api_path, transport_contract, resolved_endpoint,
  source_field, unit, match_policy, promotion_mode, promotion_table,
  promotion_column, k_type, adjust_type, subject_kind, subject_identity,
  subject_sha256, subject_payload, subject_payload_sha256,
  authority_proof_kind, authority_proof_identity,
  authority_proof_sha256, authority_set_sha256,
  expected_count, fetched_count, valid_count, matched_count,
  promoted_count, expected_keyset_sha256, provider_request_payload,
  provider_request_sha256, provider_response_payload, provider_response_sha256,
  collector_binary_sha256, provider_sdk_version, collector_runtime_version,
  source_timezone, entitlement_status, raw_payload_root_sha256,
  field_value_root_sha256,
  target_fingerprint_root_sha256, semantic_sha256, request_started_at,
  captured_max_at, provider_observed_max_at, published_at, status,
  error_message, created_at
) VALUES (
  :run_id, :schema_version, :collector_build_sha, :capture_kind, :target_date,
  :window_start_date, :window_end_date, :decision_at,
  :provider, :api_path, :transport_contract, :resolved_endpoint,
  :source_field, :unit, :match_policy, :promotion_mode, :promotion_table,
  :promotion_column, :k_type, :adjust_type, :subject_kind, :subject_identity,
  :subject_sha256, :subject_payload, :subject_payload_sha256,
  :authority_proof_kind, :authority_proof_identity,
  :authority_proof_sha256, :authority_set_sha256,
  :expected_count, :fetched_count, :valid_count, :matched_count,
  :promoted_count, :expected_keyset_sha256, :provider_request_payload,
  :provider_request_sha256, :provider_response_payload, :provider_response_sha256,
  :collector_binary_sha256, :provider_sdk_version, :collector_runtime_version,
  :source_timezone, :entitlement_status, :raw_payload_root_sha256,
  :field_value_root_sha256,
  :target_fingerprint_root_sha256, :semantic_sha256, :request_started_at,
  :captured_max_at, :provider_observed_max_at, :published_at, :status,
  :error_message, :created_at
)
"""


def _row_params(run: UpperLimitCaptureRun, row: CapturedUpperLimitRow, *, published_at: datetime) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "stock_code": row.stock_code,
        "trade_date": row.trade_date.isoformat(),
        "k_type": 1,
        "adjust_type": 0,
        "target_row_id": None,
        "field_value_decimal": _decimal_text(row.upper_limit),
        "source_pre_close": _decimal_text(row.pre_close),
        "source_lower_limit": _decimal_text(row.lower_limit),
        "source_is_suspended": row.is_suspended,
        "source_open": None,
        "source_high": None,
        "source_low": None,
        "source_close": None,
        "source_volume_shares": None,
        "source_amount": None,
        "raw_row_text": row.raw_row_text,
        "raw_payload": row.raw_payload,
        "raw_payload_sha256": row.raw_payload_sha256,
        "snapshot_row_sha256": row.snapshot_row_sha256,
        "captured_at": _datetime_text(row.captured_at),
        "provider_observed_at_text": "FIRST_OBSERVED:" + _datetime_text(row.captured_at),
        "provider_observed_at": _datetime_text(row.captured_at),
        "qmt_open": None,
        "qmt_high": None,
        "qmt_low": None,
        "qmt_close": None,
        "qmt_volume_shares": None,
        "qmt_amount": None,
        "qmt_received_at": None,
        "qmt_data_source": None,
        "qmt_batch_id": None,
        "qmt_data_version": None,
        "qmt_quality_status": None,
        "qmt_permission_status": None,
        "target_prewrite_sha256": row.target_key_sha256,
        "target_fact_sha256": row.snapshot_row_sha256,
        "validation_status": "MATCHED",
        "validation_error": "",
        "promoted_at": _datetime_text(published_at),
    }


_ROW_INSERT_SQL = f"""
INSERT INTO {FIELD_CAPTURE_ROW_TABLE} (
  run_id, stock_code, trade_date, k_type, adjust_type, target_row_id,
  field_value_decimal, source_pre_close, source_lower_limit,
  source_is_suspended, source_open, source_high, source_low, source_close,
  source_volume_shares, source_amount, raw_row_text, raw_payload,
  raw_payload_sha256, snapshot_row_sha256, captured_at,
  provider_observed_at_text, provider_observed_at, qmt_open, qmt_high,
  qmt_low, qmt_close, qmt_volume_shares, qmt_amount, qmt_received_at,
  qmt_data_source, qmt_batch_id, qmt_data_version, qmt_quality_status,
  qmt_permission_status, target_prewrite_sha256, target_fact_sha256,
  validation_status, validation_error, promoted_at
) VALUES (
  :run_id, :stock_code, :trade_date, :k_type, :adjust_type, :target_row_id,
  :field_value_decimal, :source_pre_close, :source_lower_limit,
  :source_is_suspended, :source_open, :source_high, :source_low, :source_close,
  :source_volume_shares, :source_amount, :raw_row_text, :raw_payload,
  :raw_payload_sha256, :snapshot_row_sha256, :captured_at,
  :provider_observed_at_text, :provider_observed_at, :qmt_open, :qmt_high,
  :qmt_low, :qmt_close, :qmt_volume_shares, :qmt_amount, :qmt_received_at,
  :qmt_data_source, :qmt_batch_id, :qmt_data_version, :qmt_quality_status,
  :qmt_permission_status, :target_prewrite_sha256, :target_fact_sha256,
  :validation_status, :validation_error, :promoted_at
)
"""


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row._mapping)


def _reconstruct_row(run: Mapping[str, Any], row: Mapping[str, Any]) -> CapturedUpperLimitRow:
    code = _stock_code(row.get("stock_code"))
    session = _exact_date(row.get("trade_date"), field="captured trade_date")
    captured = _local_datetime(row.get("captured_at"), field="captured_at")
    pre_close = _price(row.get("source_pre_close"), field="persisted pre_close")
    upper_limit = _price(row.get("field_value_decimal"), field="persisted upper_limit")
    lower_limit = _price(row.get("source_lower_limit"), field="persisted lower_limit")
    suspended = row.get("source_is_suspended")
    if type(suspended) is not int or suspended != 0 or not upper_limit > pre_close > lower_limit:
        raise _blocked(f"persisted upper-limit values differ for {code} {session}")
    canonical_source = _canonical_source_row(
        stock_code=code,
        trade_date=session,
        pre_close=pre_close,
        upper_limit=upper_limit,
        lower_limit=lower_limit,
        is_suspended=suspended,
    )
    raw_text = _canonical_json(canonical_source)
    raw_payload = _bytes(row.get("raw_payload"), field="raw_payload")
    raw_hash = _sha256(raw_payload)
    response_hash = str(run.get("provider_response_sha256") or "")
    request_hash = str(run.get("provider_request_sha256") or "")
    worker_hash = str(run.get("collector_binary_sha256") or "")
    subject = UpperLimitSubject(
        target_date=_exact_date(run.get("target_date"), field="target_date"),
        stock_codes=(),
        trade_dates=(),
        subject_identity=str(run.get("subject_identity") or ""),
        subject_sha256=str(run.get("subject_sha256") or ""),
        code_set_sha256="",
        trade_dates_sha256="",
        expected_keyset_sha256=str(run.get("expected_keyset_sha256") or ""),
        calendar_batch_id=str(run.get("authority_proof_identity") or ""),
        calendar_manifest_sha256=str(run.get("authority_proof_sha256") or ""),
        calendar_session_set_sha256=str(run.get("authority_set_sha256") or ""),
    )
    semantic = _row_semantic_payload(
        subject=subject,
        source_row=canonical_source,
        captured_at=captured,
        provider_response_sha256=response_hash,
        provider_request_sha256=request_hash,
        worker_sha256=worker_hash,
    )
    target_key = _sha256({
        "schema": "probiga.upper-limit-target-key.v1",
        "subject_sha256": subject.subject_sha256,
        "stock_code": code,
        "trade_date": session.isoformat(),
    })
    if (
        str(row.get("raw_row_text") or "") != raw_text
        or raw_payload != raw_text.encode("utf-8")
        or raw_hash != str(row.get("raw_payload_sha256") or "")
        or _sha256(semantic) != str(row.get("snapshot_row_sha256") or "")
        or str(row.get("target_prewrite_sha256") or "") != target_key
        or str(row.get("target_fact_sha256") or "") != _sha256(semantic)
        or str(row.get("validation_status") or "") != "MATCHED"
        or str(row.get("validation_error") or "") != ""
    ):
        raise _blocked(f"persisted upper-limit row hash differs for {code} {session}")
    return CapturedUpperLimitRow(
        stock_code=code,
        trade_date=session,
        pre_close=pre_close,
        upper_limit=upper_limit,
        lower_limit=lower_limit,
        is_suspended=suspended,
        raw_row_text=raw_text,
        raw_payload=raw_payload,
        raw_payload_sha256=raw_hash,
        snapshot_row_sha256=_sha256(semantic),
        target_key_sha256=target_key,
        captured_at=captured,
    )


def _verify_readback(connection, run: UpperLimitCaptureRun, *, status: str) -> None:
    run_rows = connection.execute(text(
        f"SELECT * FROM {FIELD_CAPTURE_RUN_TABLE} WHERE run_id=:run_id"
    ), {"run_id": run.run_id}).mappings().all()
    if len(run_rows) != 1:
        raise _blocked("upper-limit run readback identity differs")
    persisted = dict(run_rows[0])
    count = len(run.rows)
    response_payload = _bytes(persisted.get("provider_response_payload"), field="response payload")
    request_payload = _bytes(persisted.get("provider_request_payload"), field="request payload")
    persisted_subject_payload = persisted.get("subject_payload")
    if isinstance(persisted_subject_payload, memoryview):
        persisted_subject_payload = persisted_subject_payload.tobytes()
    if (
        str(persisted.get("status") or "") != status
        or any(int(persisted.get(name) or -1) != count for name in (
            "expected_count", "fetched_count", "valid_count", "matched_count"
        ))
        or int(
            persisted.get("promoted_count")
            if persisted.get("promoted_count") is not None
            else -1
        ) != 0
        or str(persisted.get("subject_sha256") or "") != run.subject.subject_sha256
        or (persisted_subject_payload or b"") != run.subject_payload
        or str(persisted.get("subject_payload_sha256") or "")
        != run.subject_payload_sha256
        or str(persisted.get("expected_keyset_sha256") or "") != run.subject.expected_keyset_sha256
        or response_payload != run.provider_response_payload
        or request_payload != run.provider_request_payload
        or _sha256(response_payload) != run.provider_response_sha256
        or _sha256(request_payload) != run.provider_request_sha256
        or str(persisted.get("collector_binary_sha256") or "") != run.worker_sha256
        or str(persisted.get("raw_payload_root_sha256") or "") != run.raw_payload_root_sha256
        or str(persisted.get("field_value_root_sha256") or "") != run.field_value_root_sha256
        or str(persisted.get("target_fingerprint_root_sha256") or "") != run.target_fingerprint_root_sha256
        or str(persisted.get("semantic_sha256") or "") != run.semantic_sha256
    ):
        raise _blocked("upper-limit run readback contract differs")
    persisted_rows = connection.execute(text(
        f"SELECT * FROM {FIELD_CAPTURE_ROW_TABLE} WHERE run_id=:run_id "
        "ORDER BY stock_code, trade_date"
    ), {"run_id": run.run_id}).mappings().all()
    if len(persisted_rows) != count:
        raise _blocked("upper-limit row readback count differs")
    for expected, raw in zip(run.rows, persisted_rows):
        if _reconstruct_row(persisted, raw) != expected:
            raise _blocked(
                f"upper-limit row readback differs for {expected.stock_code} {expected.trade_date}"
            )


def _upper_limit_receipt(run: UpperLimitCaptureRun) -> dict[str, Any]:
    preliminary_receipt = (
        run.subject.subject_identity.split(":", 1)[1]
        if run.subject.subject_identity.startswith("preview:")
        else ""
    )
    return {
        "schema": UPPER_LIMIT_SNAPSHOT_VERSION,
        "status": "COMPLETED",
        "run_id": run.run_id,
        "target_date": run.subject.target_date.isoformat(),
        "decision_at": run.decision_at.isoformat(timespec="seconds"),
        "expected_count": len(run.rows),
        "expected_stock_count": len(run.subject.stock_codes),
        "expected_date_count": len(run.subject.trade_dates),
        "subject_sha256": run.subject.subject_sha256,
        "subject_payload_sha256": run.subject_payload_sha256,
        "expected_keyset_sha256": run.subject.expected_keyset_sha256,
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "target_fingerprint_root_sha256": (
            run.target_fingerprint_root_sha256
        ),
        "semantic_sha256": run.semantic_sha256,
        "collector_build_sha": run.collector_build_sha,
        "preliminary_receipt_sha256": preliminary_receipt,
    }


@contextmanager
def _publication_connection(engine):
    connection = engine.connect()
    mysql = str(getattr(getattr(engine, "dialect", None), "name", "")).lower() == "mysql"
    try:
        if mysql:
            with mysql_named_lock(
                engine,
                MARKET_FIELD_CAPTURE_LOCK_NAME,
                timeout_seconds=0,
                connection=connection,
            ):
                connection.commit()
                with connection.begin():
                    yield connection
        else:
            with connection.begin():
                yield connection
    finally:
        connection.close()


def publish_upper_limit_snapshot(
    engine,
    run: UpperLimitCaptureRun,
    *,
    published_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Atomically append a completed evidence-only capture."""

    published = _local_datetime(
        published_at or _now_shanghai(), field="published_at"
    )
    if not run.captured_at <= published <= run.decision_at:
        raise _blocked("upper-limit publication crossed the decision cutoff")
    if len(run.rows) != UPPER_LIMIT_EXPECTED_STOCK_COUNT * UPPER_LIMIT_EXPECTED_DATE_COUNT:
        raise _blocked("upper-limit publication is not exact 80x21 coverage")
    if (
        not run.subject.calendar_batch_id
        or _SHA64.fullmatch(run.subject.calendar_manifest_sha256) is None
        or _SHA64.fullmatch(run.subject.calendar_session_set_sha256) is None
    ):
        raise _blocked("upper-limit publication lacks immutable calendar authority")
    with _publication_connection(engine) as connection:
        existing = connection.execute(text(f"""
            SELECT run_id, status
            FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE capture_kind=:capture_kind
              AND target_date=:target_date
              AND subject_sha256=:subject_sha256
              AND decision_at=:decision_at
        """), {
            "capture_kind": UPPER_LIMIT_CAPTURE_KIND,
            "target_date": run.subject.target_date.isoformat(),
            "subject_sha256": run.subject.subject_sha256,
            "decision_at": _datetime_text(run.decision_at),
        }).mappings().all()
        if existing:
            if (
                len(existing) != 1
                or str(existing[0].get("status") or "") != "COMPLETED"
            ):
                raise _blocked("upper-limit logical publication is not terminal")
            recovered = replace(run, run_id=str(existing[0]["run_id"]))
            _verify_readback(connection, recovered, status="COMPLETED")
            return _upper_limit_receipt(recovered)
        connection.execute(text(_RUN_INSERT_SQL), _run_params(run, published_at=published))
        params = [_row_params(run, row, published_at=published) for row in run.rows]
        for offset in range(0, len(params), 250):
            connection.execute(text(_ROW_INSERT_SQL), params[offset:offset + 250])
        _verify_readback(connection, run, status="BUILDING")
        terminal = connection.execute(text(
            f"UPDATE {FIELD_CAPTURE_RUN_TABLE} SET status='COMPLETED' "
            "WHERE run_id=:run_id AND status='BUILDING'"
        ), {"run_id": run.run_id})
        if int(getattr(terminal, "rowcount", -1)) != 1:
            raise _blocked("upper-limit terminal transition was not exact")
        _verify_readback(connection, run, status="COMPLETED")
    return _upper_limit_receipt(run)


def recover_completed_upper_limit_receipt(
    engine,
    *,
    subject: UpperLimitSubject,
    decision_at: datetime | str,
    collector_build_sha: str,
) -> dict[str, Any] | None:
    """Recover a committed logical run before making another provider call."""

    cutoff = _local_datetime(decision_at, field="decision_at")
    build_sha = str(collector_build_sha or "").strip().lower()
    if (
        cutoff.microsecond != 0
        or _SHA40.fullmatch(build_sha) is None
        or build_sha == "0" * 40
    ):
        raise _blocked("upper-limit recovery identity is invalid")
    with engine.connect() as connection:
        existing = connection.execute(text(f"""
            SELECT *
            FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE capture_kind=:capture_kind
              AND target_date=:target_date
              AND subject_sha256=:subject_sha256
              AND decision_at=:decision_at
        """), {
            "capture_kind": UPPER_LIMIT_CAPTURE_KIND,
            "target_date": subject.target_date.isoformat(),
            "subject_sha256": subject.subject_sha256,
            "decision_at": _datetime_text(cutoff),
        }).mappings().all()
    if not existing:
        return None
    if len(existing) != 1 or str(existing[0].get("status") or "") != "COMPLETED":
        raise _blocked("upper-limit logical publication is not terminal")
    persisted = dict(existing[0])
    if str(persisted.get("collector_build_sha") or "").lower() != build_sha:
        raise _blocked("upper-limit completed publication build differs")
    preliminary_receipt = (
        subject.subject_identity.split(":", 1)[1]
        if subject.subject_identity.startswith("preview:")
        else ""
    )
    evidence = load_verified_upper_limit_evidence(
        engine,
        run_id=str(persisted.get("run_id") or ""),
        target_date=subject.target_date,
        decision_at=cutoff,
        stock_codes=subject.stock_codes,
        trade_dates=subject.trade_dates,
        preliminary_receipt_sha256=preliminary_receipt,
        expected_collector_build_sha=build_sha,
    )
    if len(evidence) != len(subject.stock_codes):
        raise _blocked("upper-limit completed publication coverage differs")
    return {
        "schema": UPPER_LIMIT_SNAPSHOT_VERSION,
        "status": "COMPLETED",
        "run_id": str(persisted["run_id"]),
        "target_date": subject.target_date.isoformat(),
        "decision_at": cutoff.isoformat(timespec="seconds"),
        "expected_count": int(persisted.get("expected_count") or 0),
        "expected_stock_count": len(subject.stock_codes),
        "expected_date_count": len(subject.trade_dates),
        "subject_sha256": subject.subject_sha256,
        "subject_payload_sha256": str(
            persisted.get("subject_payload_sha256") or ""
        ),
        "expected_keyset_sha256": subject.expected_keyset_sha256,
        "raw_payload_root_sha256": str(
            persisted.get("raw_payload_root_sha256") or ""
        ),
        "field_value_root_sha256": str(
            persisted.get("field_value_root_sha256") or ""
        ),
        "target_fingerprint_root_sha256": str(
            persisted.get("target_fingerprint_root_sha256") or ""
        ),
        "semantic_sha256": str(persisted.get("semantic_sha256") or ""),
        "collector_build_sha": build_sha,
        "preliminary_receipt_sha256": preliminary_receipt,
        "recovered": True,
    }


def _validate_run_contract(
    run: Mapping[str, Any],
    *,
    subject: UpperLimitSubject,
    decision_at: datetime,
    expected_collector_build_sha: str = "",
) -> None:
    response = _bytes(run.get("provider_response_payload"), field="response payload")
    request = _bytes(run.get("provider_request_payload"), field="request payload")
    started = _local_datetime(run.get("request_started_at"), field="request_started_at")
    captured = _local_datetime(run.get("captured_max_at"), field="captured_at")
    published = _local_datetime(run.get("published_at"), field="published_at")
    cutoff = _local_datetime(run.get("decision_at"), field="run decision_at")
    expected_build = str(expected_collector_build_sha or "").strip().lower()
    count = UPPER_LIMIT_EXPECTED_STOCK_COUNT * UPPER_LIMIT_EXPECTED_DATE_COUNT
    if (
        str(run.get("status") or "") != "COMPLETED"
        or str(run.get("schema_version") or "") != UPPER_LIMIT_SNAPSHOT_VERSION
        or _SHA40.fullmatch(str(run.get("collector_build_sha") or "")) is None
        or str(run.get("collector_build_sha") or "") == "0" * 40
        or (
            expected_build
            and str(run.get("collector_build_sha") or "").strip().lower()
            != expected_build
        )
        or str(run.get("capture_kind") or "") != UPPER_LIMIT_CAPTURE_KIND
        or str(run.get("provider") or "") != UPPER_LIMIT_PROVIDER
        or str(run.get("api_path") or "") != UPPER_LIMIT_API_PATH
        or str(run.get("transport_contract") or "") != UPPER_LIMIT_TRANSPORT
        or str(run.get("source_field") or "") != UPPER_LIMIT_SOURCE_FIELD
        or str(run.get("unit") or "") != UPPER_LIMIT_UNIT
        or str(run.get("match_policy") or "") != UPPER_LIMIT_MATCH_POLICY
        or str(run.get("promotion_mode") or "") != UPPER_LIMIT_PROMOTION_MODE
        or str(run.get("promotion_table") or "") != ""
        or str(run.get("promotion_column") or "") != ""
        or str(run.get("subject_kind") or "") != UPPER_LIMIT_SUBJECT_KIND
        or str(run.get("subject_identity") or "") != subject.subject_identity
        or str(run.get("subject_sha256") or "") != subject.subject_sha256
        or str(run.get("authority_proof_kind") or "") != "QMT_TRADE_CALENDAR"
        or str(run.get("authority_proof_identity") or "") != subject.calendar_batch_id
        or str(run.get("authority_proof_sha256") or "")
        != subject.calendar_manifest_sha256
        or str(run.get("authority_set_sha256") or "")
        != subject.calendar_session_set_sha256
        or str(run.get("expected_keyset_sha256") or "") != subject.expected_keyset_sha256
        or any(int(run.get(name) or -1) != count for name in (
            "expected_count", "fetched_count", "valid_count", "matched_count"
        ))
        or int(
            run.get("promoted_count")
            if run.get("promoted_count") is not None
            else -1
        ) != 0
        or _sha256(response) != str(run.get("provider_response_sha256") or "")
        or _sha256(request) != str(run.get("provider_request_sha256") or "")
        or _SHA64.fullmatch(str(run.get("collector_binary_sha256") or "")) is None
        or str(run.get("source_timezone") or "") != "Asia/Shanghai"
        or str(run.get("entitlement_status") or "") != "SUPPORTED"
        or not str(run.get("provider_sdk_version") or "").strip()
        or not str(run.get("collector_runtime_version") or "").strip()
        or not started <= captured <= published <= cutoff
        or cutoff != decision_at
        or _exact_date(run.get("window_start_date"), field="window_start_date") != subject.trade_dates[0]
        or _exact_date(run.get("window_end_date"), field="window_end_date") != subject.target_date
    ):
        raise _blocked("completed upper-limit run contract differs")
    persisted_subject_payload = run.get("subject_payload")
    if isinstance(persisted_subject_payload, memoryview):
        persisted_subject_payload = persisted_subject_payload.tobytes()
    preliminary_identity = (
        subject.subject_identity.split(":", 1)[1]
        if subject.subject_identity.startswith("preview:")
        else ""
    )
    if preliminary_identity:
        try:
            payload_bytes = _bytes(
                persisted_subject_payload, field="preliminary subject payload"
            )
            persisted_preliminary = validate_preliminary_upper_subject_receipt(
                json.loads(payload_bytes)
            )
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
            UpperLimitSnapshotBlocked,
        ) as exc:
            raise _blocked(
                "completed upper-limit preliminary payload is invalid"
            ) from exc
        if (
            _sha256(payload_bytes)
            != str(run.get("subject_payload_sha256") or "")
            or persisted_preliminary["receipt_sha256"] != preliminary_identity
            or persisted_preliminary["trade_date"]
            != subject.target_date.isoformat()
            or persisted_preliminary["decision_at"]
            != decision_at.isoformat(timespec="seconds")
            or (
                expected_build
                and persisted_preliminary["build_sha"] != expected_build
            )
            or sorted(persisted_preliminary["ordered_stock_codes"])
            != list(subject.stock_codes)
        ):
            raise _blocked(
                "completed upper-limit preliminary payload differs"
            )
    elif persisted_subject_payload not in (None, b"") or str(
        run.get("subject_payload_sha256") or ""
    ):
        raise _blocked("diagnostic upper-limit run has unexpected subject payload")


def _validate_persisted_transport_payload(
    run: Mapping[str, Any],
    *,
    subject: UpperLimitSubject,
    rows: Sequence[CapturedUpperLimitRow],
) -> None:
    """Independently bind archived worker bytes to every persisted fact."""

    response_payload = _bytes(
        run.get("provider_response_payload"), field="response payload"
    )
    request_payload = _bytes(
        run.get("provider_request_payload"), field="request payload"
    )
    try:
        response = json.loads(response_payload)
        request = json.loads(request_payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _blocked("persisted MyQuant transport payload is invalid JSON") from exc
    expected_symbols = [_gm_symbol(code) for code in subject.stock_codes]
    expected_request = {
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "end_date": subject.target_date.isoformat(),
        "start_date": subject.trade_dates[0].isoformat(),
        "symbols": expected_symbols,
    }
    if request != expected_request or not isinstance(response, dict):
        raise _blocked("persisted MyQuant request/response identity differs")
    expected_response_keys = {
        "ok", "action", "fields", "columns", "requested_symbols",
        "start_date", "end_date", "request_started_at", "captured_at",
        "timezone", "sdk_version", "python_version", "entitlement_status",
        "rows", "errors",
    }
    if (
        set(response) != expected_response_keys
        or response.get("ok") is not True
        or response.get("action") != UPPER_LIMIT_HISTORY_ACTION
        or response.get("fields") != UPPER_LIMIT_HISTORY_FIELDS
        or response.get("columns") != list(UPPER_LIMIT_HISTORY_COLUMNS)
        or response.get("requested_symbols") != expected_symbols
        or response.get("start_date") != subject.trade_dates[0].isoformat()
        or response.get("end_date") != subject.target_date.isoformat()
        or response.get("timezone") != "Asia/Shanghai"
        or response.get("entitlement_status") != "SUPPORTED"
        or response.get("errors") != {}
        or str(response.get("sdk_version") or "")
        != str(run.get("provider_sdk_version") or "")
        or str(response.get("python_version") or "")
        != str(run.get("collector_runtime_version") or "")
        or _local_datetime(
            response.get("request_started_at"), field="response request_started_at"
        )
        != _local_datetime(run.get("request_started_at"), field="run request_started_at")
        or _local_datetime(response.get("captured_at"), field="response captured_at")
        != _local_datetime(run.get("captured_max_at"), field="run captured_at")
        or not isinstance(response.get("rows"), list)
    ):
        raise _blocked("persisted MyQuant response metadata differs")
    persisted_by_key = {
        (row.stock_code, row.trade_date): row for row in rows
    }
    observed: set[tuple[str, date]] = set()
    for raw in response["rows"]:
        if not isinstance(raw, Mapping):
            raise _blocked("persisted MyQuant response row is invalid")
        code = _gm_symbol_to_code(raw.get("symbol"))
        session = _local_datetime(
            raw.get("trade_date"), field="response trade_date"
        ).date()
        key = (code, session)
        if key in observed or key not in persisted_by_key:
            raise _blocked("persisted MyQuant response keyset differs")
        observed.add(key)
        persisted = persisted_by_key[key]
        if (
            _price(raw.get("pre_close"), field="response pre_close")
            != persisted.pre_close
            or _price(raw.get("upper_limit"), field="response upper_limit")
            != persisted.upper_limit
            or _price(raw.get("lower_limit"), field="response lower_limit")
            != persisted.lower_limit
            or type(raw.get("is_suspended")) is not int
            or int(raw["is_suspended"]) != persisted.is_suspended
        ):
            raise _blocked(
                f"persisted MyQuant response value differs for {code} {session}"
            )
    if observed != set(persisted_by_key):
        raise _blocked("persisted MyQuant response coverage differs")


def load_verified_upper_limit_evidence(
    engine,
    *,
    run_id: str,
    target_date: date | str,
    decision_at: datetime | str,
    stock_codes: Sequence[str],
    trade_dates: Sequence[date | str],
    preliminary_receipt_sha256: str = "",
    expected_collector_build_sha: str = "",
) -> dict[str, dict[str, Any]]:
    """Rebuild every immutable hash and return per-stock Frozen-V4 proof."""

    identity = str(run_id or "").strip().lower()
    if _RUN_ID.fullmatch(identity) is None:
        raise _blocked("upper-limit run id is invalid")
    cutoff = _local_datetime(decision_at, field="decision_at")
    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    normalized_dates = tuple(sorted(
        value if isinstance(value, date) else _exact_date(value, field="trade_date")
        for value in trade_dates
    ))
    with engine.connect() as connection:
        runs = connection.execute(text(
            f"SELECT * FROM {FIELD_CAPTURE_RUN_TABLE} WHERE run_id=:run_id"
        ), {"run_id": identity}).mappings().all()
        if len(runs) != 1:
            raise _blocked("upper-limit completed run is unavailable/ambiguous")
        run = dict(runs[0])
        calendar_batch_id = str(run.get("authority_proof_identity") or "")
        calendar_manifest_sha256 = str(
            run.get("authority_proof_sha256") or ""
        ).lower()
        calendar_session_set_sha256 = str(
            run.get("authority_set_sha256") or ""
        ).lower()
        if (
            str(run.get("authority_proof_kind") or "")
            != "QMT_TRADE_CALENDAR"
            or not calendar_batch_id
            or _SHA64.fullmatch(calendar_manifest_sha256) is None
            or _SHA64.fullmatch(calendar_session_set_sha256) is None
        ):
            raise _blocked("upper-limit calendar authority proof differs")
        if str(getattr(getattr(engine, "dialect", None), "name", "")).lower() == "mysql":
            validate_trade_calendar_immutability(connection)
            calendar = load_trade_calendar_receipt(
                connection,
                start_date=normalized_dates[0].isoformat(),
                end_date=target.isoformat(),
                decision_known_at=cutoff,
                batch_id=calendar_batch_id,
            )
            authoritative_dates = tuple(
                date.fromisoformat(value)
                for value in calendar.sessions
                if value <= target.isoformat()
            )[-UPPER_LIMIT_EXPECTED_DATE_COUNT:]
            if (
                calendar.manifest_hash != calendar_manifest_sha256
                or calendar.session_set_hash != calendar_session_set_sha256
                or authoritative_dates != normalized_dates
            ):
                raise _blocked("upper-limit calendar session authority differs")
        subject = build_upper_limit_subject(
            target_date=target,
            stock_codes=stock_codes,
            trade_dates=normalized_dates,
            calendar_batch_id=calendar_batch_id,
            calendar_manifest_sha256=calendar_manifest_sha256,
            calendar_session_set_sha256=calendar_session_set_sha256,
            preliminary_receipt_sha256=preliminary_receipt_sha256,
        )
        _validate_run_contract(
            run,
            subject=subject,
            decision_at=cutoff,
            expected_collector_build_sha=expected_collector_build_sha,
        )
        raw_rows = connection.execute(text(
            f"SELECT * FROM {FIELD_CAPTURE_ROW_TABLE} WHERE run_id=:run_id "
            "ORDER BY stock_code, trade_date"
        ), {"run_id": identity}).mappings().all()
    if len(raw_rows) != len(subject.stock_codes) * len(subject.trade_dates):
        raise _blocked("completed upper-limit row coverage differs")
    rows = [_reconstruct_row(run, dict(row)) for row in raw_rows]
    _validate_persisted_transport_payload(run, subject=subject, rows=rows)
    expected_keys = {
        (code, session) for code in subject.stock_codes for session in subject.trade_dates
    }
    observed_keys = {(row.stock_code, row.trade_date) for row in rows}
    if observed_keys != expected_keys or len(observed_keys) != len(rows):
        raise _blocked("completed upper-limit keyset differs")
    raw_root = _sha256([
        {"stock_code": row.stock_code, "trade_date": row.trade_date.isoformat(), "raw_payload_sha256": row.raw_payload_sha256}
        for row in rows
    ])
    target_root = _sha256([
        {"stock_code": row.stock_code, "trade_date": row.trade_date.isoformat(), "target_key_sha256": row.target_key_sha256}
        for row in rows
    ])
    semantic_root = _sha256([
        {"stock_code": row.stock_code, "trade_date": row.trade_date.isoformat(), "snapshot_row_sha256": row.snapshot_row_sha256}
        for row in rows
    ])
    value_root = _sha256([
        {"stock_code": row.stock_code, "trade_date": row.trade_date.isoformat(), "upper_limit": _decimal_text(row.upper_limit)}
        for row in rows
    ])
    if (
        raw_root != str(run.get("raw_payload_root_sha256") or "")
        or value_root != str(run.get("field_value_root_sha256") or "")
        or target_root != str(run.get("target_fingerprint_root_sha256") or "")
        or semantic_root != str(run.get("semantic_sha256") or "")
    ):
        raise _blocked("completed upper-limit root hash differs")
    result: dict[str, dict[str, Any]] = {}
    for code in subject.stock_codes:
        stock_rows = [row for row in rows if row.stock_code == code]
        stock_hash = _sha256([
            {"trade_date": row.trade_date.isoformat(), "snapshot_row_sha256": row.snapshot_row_sha256}
            for row in stock_rows
        ])
        proof = build_upper_limit_evidence({
            "status": "PASS",
            "stock_code": code,
            "trade_date": subject.target_date.isoformat(),
            "window_start_date": subject.trade_dates[0].isoformat(),
            "window_end_date": subject.target_date.isoformat(),
            "decision_known_at": _datetime_text(cutoff),
            "captured_at": _datetime_text(_local_datetime(run.get("captured_max_at"), field="captured_at")),
            "source_table": FIELD_CAPTURE_ROW_TABLE,
            "capture_kind": UPPER_LIMIT_CAPTURE_KIND,
            "provider": UPPER_LIMIT_PROVIDER,
            "source_field": UPPER_LIMIT_SOURCE_FIELD,
            "unit": UPPER_LIMIT_UNIT,
            "transport_contract": UPPER_LIMIT_TRANSPORT,
            "entitlement_status": "SUPPORTED",
            "timezone": "Asia/Shanghai",
            "expected_stock_count": len(subject.stock_codes),
            "expected_date_count": len(subject.trade_dates),
            "snapshot_run_id": identity,
            "subject_identity": subject.subject_identity,
            "subject_sha256": subject.subject_sha256,
            "preliminary_receipt_payload_sha256": str(
                run.get("subject_payload_sha256") or ""
            ),
            "code_set_sha256": subject.code_set_sha256,
            "trade_dates_sha256": subject.trade_dates_sha256,
            "calendar_batch_id": subject.calendar_batch_id,
            "calendar_manifest_sha256": subject.calendar_manifest_sha256,
            "calendar_session_set_sha256": subject.calendar_session_set_sha256,
            "expected_keyset_sha256": subject.expected_keyset_sha256,
            "snapshot_semantic_sha256": semantic_root,
            "stock_rows_sha256": stock_hash,
            "provider_response_sha256": str(run.get("provider_response_sha256") or ""),
            "canonical_request_sha256": str(run.get("provider_request_sha256") or ""),
            "worker_sha256": str(run.get("collector_binary_sha256") or ""),
            "sdk_version": str(run.get("provider_sdk_version") or ""),
            "python_version": str(run.get("collector_runtime_version") or ""),
        })
        result[code] = {
            "upper_limits": {
                row.trade_date.isoformat(): row.upper_limit for row in stock_rows
            },
            "upper_limit_evidence_json": proof,
        }
    return result


def load_latest_verified_upper_limit_evidence(
    engine,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    stock_codes: Sequence[str],
    preliminary_receipt_sha256: str = "",
    preliminary_build_sha: str = "",
) -> dict[str, dict[str, Any]]:
    """Resolve the newest convergent run for one exact recommendation set."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    expected_codes = tuple(sorted(_stock_code(code) for code in stock_codes))
    preliminary_receipt = str(
        preliminary_receipt_sha256 or ""
    ).strip().lower()
    preview_build = str(preliminary_build_sha or "").strip().lower()
    if preliminary_receipt and (
        _SHA64.fullmatch(preliminary_receipt) is None
        or _SHA40.fullmatch(preview_build) is None
        or preview_build == "0" * 40
    ):
        return {}
    expected_subject_identity = (
        f"preview:{preliminary_receipt}" if preliminary_receipt else ""
    )
    if (
        len(expected_codes) != UPPER_LIMIT_EXPECTED_STOCK_COUNT
        or len(set(expected_codes)) != UPPER_LIMIT_EXPECTED_STOCK_COUNT
    ):
        return {}
    with engine.connect() as connection:
        candidates = connection.execute(text(f"""
            SELECT run_id, decision_at, published_at, subject_identity,
                   expected_keyset_sha256, field_value_root_sha256
            FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE target_date=:target_date
              AND decision_at=:decision_at
              AND status='COMPLETED'
              AND capture_kind=:capture_kind
              AND provider=:provider
              AND source_field=:source_field
              AND unit=:unit
              AND (:collector_build_sha='' OR collector_build_sha=:collector_build_sha)
            ORDER BY decision_at DESC, published_at DESC, run_id DESC
        """), {
            "target_date": target.isoformat(),
            "decision_at": _datetime_text(cutoff),
            "capture_kind": UPPER_LIMIT_CAPTURE_KIND,
            "provider": UPPER_LIMIT_PROVIDER,
            "source_field": UPPER_LIMIT_SOURCE_FIELD,
            "unit": UPPER_LIMIT_UNIT,
            "collector_build_sha": preview_build,
        }).mappings().all()
        matching: list[tuple[dict[str, Any], tuple[str, ...], tuple[str, ...]]] = []
        for candidate in candidates:
            if (
                expected_subject_identity
                and str(candidate.get("subject_identity") or "")
                != expected_subject_identity
            ):
                continue
            keys = connection.execute(text(
                f"SELECT stock_code, trade_date FROM {FIELD_CAPTURE_ROW_TABLE} "
                "WHERE run_id=:run_id ORDER BY stock_code, trade_date"
            ), {"run_id": candidate["run_id"]}).mappings().all()
            codes = tuple(sorted({
                _stock_code(row["stock_code"]) for row in keys
            }))
            if codes != expected_codes:
                continue
            dates = tuple(sorted({
                _exact_date(row["trade_date"], field="captured trade_date").isoformat()
                for row in keys
            }))
            matching.append((dict(candidate), codes, dates))
    if not matching:
        return {}
    replay_roots = {
        (
            str(candidate.get("expected_keyset_sha256") or ""),
            str(candidate.get("field_value_root_sha256") or ""),
        )
        for candidate, _codes, _dates in matching
    }
    if len(replay_roots) != 1:
        raise _blocked(
            "completed upper-limit snapshot replays disagree on exact values"
        )
    selected, _codes, dates = matching[0]
    return load_verified_upper_limit_evidence(
        engine,
        run_id=str(selected["run_id"]),
        target_date=target,
        decision_at=cutoff,
        stock_codes=expected_codes,
        trade_dates=dates,
        preliminary_receipt_sha256=preliminary_receipt,
        expected_collector_build_sha=preview_build,
    )


__all__ = [
    "CapturedUpperLimitRow",
    "UpperLimitCaptureRun",
    "UpperLimitSnapshotBlocked",
    "UpperLimitSubject",
    "UPPER_LIMIT_CAPTURE_KIND",
    "UPPER_LIMIT_EXPECTED_DATE_COUNT",
    "UPPER_LIMIT_EXPECTED_STOCK_COUNT",
    "build_upper_limit_capture_run",
    "build_upper_limit_subject",
    "collect_upper_limit_snapshot",
    "load_verified_upper_limit_evidence",
    "load_latest_verified_upper_limit_evidence",
    "publish_upper_limit_snapshot",
    "recover_completed_upper_limit_receipt",
]
