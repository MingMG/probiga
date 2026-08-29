# -*- coding: utf-8 -*-
"""Immutable Eastmoney target-session upper/lower-limit quote evidence.

This is a deliberately narrow fallback for the *latest closed session* while
Eastmoney still exposes that session through ``stock/get``.  It does not turn
Eastmoney K-lines into synthetic limit prices and it does not label any field
as QMT.  Provider fields ``f51``/``f52``/``f60`` are retained verbatim and are
accepted only when ``f86`` proves the requested trade date and independent,
attested QMT prices prove the quote identity.

The producer writes a separate evidence-only capture kind.  It therefore can
co-exist with the 21-session MyQuant history contract without pretending that
one current quote supplies historical rows.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from server.common.analysis_pool_receipt import (
    validate_preliminary_upper_subject_receipt,
)
from server.common.mysql_lock import mysql_named_lock
from server.common.qmt_attestation_contract import ATTESTATION_PROTOCOL_VERSION
from server.common.qmt_daily_market_truth import QMT_DAILY_PROVIDER
from server.common.turnover_snapshot_schema import (
    FIELD_CAPTURE_ROW_TABLE,
    FIELD_CAPTURE_RUN_TABLE,
)


EASTMONEY_UPPER_LIMIT_SCHEMA = "probiga.market-field-capture.v1"
EASTMONEY_UPPER_LIMIT_CAPTURE_KIND = "DAILY_UPPER_LIMIT_QUOTE"
EASTMONEY_UPPER_LIMIT_PROVIDER = "eastmoney.push2.quote"
EASTMONEY_UPPER_LIMIT_API_PATH = "/api/qt/stock/get"
EASTMONEY_UPPER_LIMIT_TRANSPORT = "HTTPS_TLS_VERIFIED_DIRECT_V1"
EASTMONEY_UPPER_LIMIT_SOURCE_FIELD = "f51"
EASTMONEY_UPPER_LIMIT_UNIT = "PRICE_CNY"
EASTMONEY_UPPER_LIMIT_MATCH_POLICY = "EXACT_TOP80_TARGET_DAY_QMT_PRICES"
EASTMONEY_UPPER_LIMIT_PROMOTION_MODE = "EVIDENCE_ONLY"
EASTMONEY_UPPER_LIMIT_SUBJECT_KIND = "STRATEGY_RECOMMENDATION_CODESET"
EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT = 80
EASTMONEY_UPPER_LIMIT_HOSTS = (
    "https://push2delay.eastmoney.com",
    "https://33.push2.eastmoney.com",
    "https://63.push2.eastmoney.com",
    "https://81.push2.eastmoney.com",
    "https://90.push2.eastmoney.com",
    "https://push2.eastmoney.com",
    "https://push2his.eastmoney.com",
)
EASTMONEY_UPPER_LIMIT_FIELDS = (
    "f12,f13,f43,f44,f45,f46,f47,f48,f51,f52,f57,f58,f59,f60,f86"
)
MARKET_FIELD_CAPTURE_LOCK_NAME = "probiga:market-field-capture"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODE = re.compile(r"(?:0|3|6)[0-9]{5}")
_RUN_ID = re.compile(r"[0-9a-f]{32}")
_SHA40 = re.compile(r"[0-9a-f]{40}")
_SHA64 = re.compile(r"[0-9a-f]{64}")


class EastmoneyUpperLimitQuoteBlocked(RuntimeError):
    """The target-day provider/QMT identity could not be proven."""


class EastmoneyUpperLimitTransportError(EastmoneyUpperLimitQuoteBlocked):
    """A retryable transport failure before provider facts were accepted."""


def _blocked(message: str) -> EastmoneyUpperLimitQuoteBlocked:
    return EastmoneyUpperLimitQuoteBlocked(f"DATA_BLOCKED: {message}")


def _transport_blocked(message: str) -> EastmoneyUpperLimitTransportError:
    return EastmoneyUpperLimitTransportError(f"DATA_BLOCKED: {message}")


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
        raise _blocked(f"unsupported A-share stock code {value!r}")
    return raw


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
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed


def _datetime_text(value: datetime) -> str:
    return _local_datetime(value, field="datetime").isoformat(
        sep=" ", timespec="microseconds"
    )


def _decimal(value: Any, *, field: str, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise _blocked(f"{field} is not an exact decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise _blocked(f"{field} is not an exact decimal") from exc
    if not result.is_finite() or (nonnegative and result < 0):
        raise _blocked(f"{field} is outside the supported range")
    return result


def _decimal_text(value: Decimal | Any) -> str:
    number = value if isinstance(value, Decimal) else _decimal(
        value, field="decimal", nonnegative=False
    )
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def _bytes(value: Any, *, field: str) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise _blocked(f"{field} is not immutable bytes")


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row._mapping)


def _eastmoney_secid(stock_code: str) -> str:
    code = _stock_code(stock_code)
    return f"{1 if code.startswith('6') else 0}.{code}"


def _http_datetime(value: Any, *, field: str) -> tuple[str, datetime]:
    raw = str(value or "").strip()
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError) as exc:
        raise _blocked(f"{field} is not an RFC HTTP date") from exc
    if parsed.tzinfo is None:
        raise _blocked(f"{field} has no timezone")
    return raw, parsed.astimezone(_SHANGHAI).replace(tzinfo=None)


def _price_from_quote(value: Any, *, precision: int, field: str) -> Decimal:
    raw = _decimal(value, field=field, nonnegative=True)
    if raw != raw.to_integral_value():
        raise _blocked(f"Eastmoney {field} is not an integer quote field")
    result = raw / (Decimal(10) ** precision)
    if result <= 0:
        raise _blocked(f"Eastmoney {field} is not a positive price")
    return result


@dataclass(frozen=True)
class EastmoneyUpperLimitSubject:
    target_date: date
    decision_at: datetime
    stock_codes: tuple[str, ...]
    preliminary_receipt_sha256: str
    subject_identity: str
    subject_payload: bytes
    subject_payload_sha256: str
    subject_sha256: str
    code_set_sha256: str
    expected_keyset_sha256: str


@dataclass(frozen=True)
class QmtUpperLimitQuoteTarget:
    target_row_id: int
    stock_code: str
    trade_date: date
    pre_close: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_shares: Decimal
    amount: Decimal
    received_at: datetime
    data_source: str
    batch_id: str
    data_version: str
    quality_status: str
    permission_status: str
    attestation_id: str
    attestation_run_id: str
    attested_at: datetime
    qmt_fact_sha256: str


@dataclass(frozen=True)
class EastmoneyQuoteHttpResponse:
    stock_code: str
    request_started_at: datetime
    captured_at: datetime
    request_payload: bytes
    request_payload_sha256: str
    raw_payload: bytes
    raw_payload_sha256: str
    http_date_text: str


@dataclass(frozen=True)
class CapturedEastmoneyUpperLimitQuote:
    target: QmtUpperLimitQuoteTarget
    upper_limit: Decimal
    lower_limit: Decimal
    source_pre_close: Decimal
    source_open: Decimal
    source_high: Decimal
    source_low: Decimal
    source_close: Decimal
    source_volume_shares: Decimal
    source_amount: Decimal
    source_trade_at: datetime
    provider_http_date: str
    provider_http_at: datetime
    request_started_at: datetime
    captured_at: datetime
    request_payload: bytes
    request_payload_sha256: str
    raw_payload: bytes
    raw_payload_sha256: str
    raw_row_text: str
    snapshot_row_sha256: str
    target_key_sha256: str


@dataclass(frozen=True)
class EastmoneyUpperLimitQuoteRun:
    run_id: str
    collector_build_sha: str
    collector_binary_sha256: str
    subject: EastmoneyUpperLimitSubject
    decision_at: datetime
    request_started_at: datetime
    captured_at: datetime
    provider_observed_at: datetime
    provider_response_payload: bytes
    provider_response_sha256: str
    provider_request_payload: bytes
    provider_request_sha256: str
    resolved_endpoint: str
    authority_proof_sha256: str
    authority_set_sha256: str
    raw_payload_root_sha256: str
    field_value_root_sha256: str
    target_fingerprint_root_sha256: str
    semantic_sha256: str
    rows: tuple[CapturedEastmoneyUpperLimitQuote, ...]


def build_eastmoney_upper_limit_subject(
    *,
    target_date: date | str,
    decision_at: datetime | str,
    preliminary_receipt: Mapping[str, Any],
    expected_stock_count: int = EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT,
) -> EastmoneyUpperLimitSubject:
    """Freeze the exact preliminary Top80 identity before external calls."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    if cutoff.microsecond != 0:
        raise _blocked("upper-limit quote decision cutoff must be second-exact")
    try:
        receipt = validate_preliminary_upper_subject_receipt(preliminary_receipt)
    except ValueError as exc:
        raise _blocked("preliminary Top80 receipt is invalid") from exc
    codes = tuple(sorted(_stock_code(code) for code in receipt["ordered_stock_codes"]))
    receipt_hash = str(receipt.get("receipt_sha256") or "").strip().lower()
    if (
        len(codes) != int(expected_stock_count)
        or len(set(codes)) != int(expected_stock_count)
        or receipt.get("trade_date") != target.isoformat()
        or receipt.get("decision_at") != cutoff.isoformat(timespec="seconds")
        or _SHA64.fullmatch(receipt_hash) is None
    ):
        raise _blocked("preliminary Top80 receipt identity differs")
    receipt_payload = _canonical_json(receipt).encode("utf-8")
    receipt_payload_hash = _sha256(receipt_payload)
    code_payload = {
        "schema": "probiga.upper-limit-quote-code-set.v1",
        "target_date": target.isoformat(),
        "stock_codes": list(codes),
    }
    code_hash = _sha256(code_payload)
    subject_payload = _canonical_json({
        "schema": "probiga.eastmoney-upper-limit-quote-subject.v1",
        "target_date": target.isoformat(),
        "decision_at": cutoff.isoformat(timespec="seconds"),
        "code_set_sha256": code_hash,
        "preliminary_receipt_sha256": receipt_hash,
        "preliminary_receipt_payload_sha256": receipt_payload_hash,
    }).encode("utf-8")
    subject_hash = _sha256(subject_payload)
    expected_keys = [
        {"stock_code": code, "trade_date": target.isoformat()}
        for code in codes
    ]
    return EastmoneyUpperLimitSubject(
        target_date=target,
        decision_at=cutoff,
        stock_codes=codes,
        preliminary_receipt_sha256=receipt_hash,
        subject_identity=f"preview:{receipt_hash}",
        subject_payload=receipt_payload,
        subject_payload_sha256=receipt_payload_hash,
        subject_sha256=subject_hash,
        code_set_sha256=code_hash,
        expected_keyset_sha256=_sha256(expected_keys),
    )


def _qmt_target_from_row(
    row: Mapping[str, Any],
    *,
    target_date: date,
    decision_at: datetime,
) -> QmtUpperLimitQuoteTarget:
    code = _stock_code(str(row.get("stock_code") or "")[:6])
    session = _exact_date(row.get("trade_date"), field="QMT trade_date")
    received = _local_datetime(row.get("received_at"), field="QMT received_at")
    attested = _local_datetime(row.get("attested_at"), field="QMT attested_at")
    finished = _local_datetime(
        row.get("attestation_finished_at"), field="QMT attestation finished_at"
    )
    values = {
        name: _decimal(row.get(name), field=f"QMT {name}", nonnegative=True)
        for name in (
            "pre_close", "open", "high", "low", "close",
            "volume_shares", "amount",
        )
    }
    attested_values = {
        name: _decimal(row.get(f"attested_{name}"), field=f"attested {name}", nonnegative=True)
        for name in ("open", "high", "low", "close", "volume_shares", "amount")
    }
    source_pre_close = _decimal(
        row.get("attested_pre_close"), field="attested pre_close", nonnegative=True
    )
    data_source = str(row.get("data_source") or "").strip()
    batch_id = str(row.get("batch_id") or "").strip()
    data_version = str(row.get("data_version") or "").strip()
    quality = str(row.get("quality_status") or "").strip()
    permission = str(row.get("permission_status") or "").strip()
    protocol = str(row.get("protocol_version") or "").strip()
    origin = str(row.get("source_pre_close_origin") or "").strip()
    run_provider = str(row.get("attestation_provider") or "").strip()
    attestation_id = str(row.get("attestation_id") or "").strip().lower()
    run_id = str(row.get("attestation_run_id") or "").strip()
    if (
        session != target_date
        or int(row.get("k_type") or -1) != 1
        or int(row.get("adjust_type") if row.get("adjust_type") is not None else -1) != 0
        or data_source != QMT_DAILY_PROVIDER
        or quality != "QMT_ATTESTED"
        or permission != "SUPPORTED"
        or not batch_id
        or not data_version
        or protocol != ATTESTATION_PROTOCOL_VERSION
        or origin != "NATIVE_QMT"
        or run_provider != QMT_DAILY_PROVIDER
        or str(row.get("attestation_status") or "") != "COMPLETED"
        or not run_id
        or _SHA64.fullmatch(attestation_id) is None
        or source_pre_close != values["pre_close"]
        or any(attested_values[name] != values[name] for name in attested_values)
        or max(received, attested, finished) > decision_at
    ):
        raise _blocked(f"attested QMT target contract differs for {code}")
    prices = [values[name] for name in ("pre_close", "open", "high", "low", "close")]
    if (
        any(value <= 0 for value in prices)
        or values["high"] < max(values["open"], values["low"], values["close"])
        or values["low"] > min(values["open"], values["high"], values["close"])
    ):
        raise _blocked(f"attested QMT prices are invalid for {code}")
    proof = {
        "schema": "probiga.qmt-upper-limit-quote-target.v1",
        "target_row_id": int(row.get("target_row_id") or 0),
        "stock_code": code,
        "trade_date": session.isoformat(),
        "pre_close": _decimal_text(values["pre_close"]),
        "open": _decimal_text(values["open"]),
        "high": _decimal_text(values["high"]),
        "low": _decimal_text(values["low"]),
        "close": _decimal_text(values["close"]),
        "volume_shares": _decimal_text(values["volume_shares"]),
        "amount": _decimal_text(values["amount"]),
        "received_at": _datetime_text(received),
        "data_source": data_source,
        "batch_id": batch_id,
        "data_version": data_version,
        "quality_status": quality,
        "permission_status": permission,
        "attestation_id": attestation_id,
        "attestation_run_id": run_id,
        "attested_at": _datetime_text(attested),
    }
    if proof["target_row_id"] <= 0:
        raise _blocked(f"attested QMT target id is invalid for {code}")
    return QmtUpperLimitQuoteTarget(
        target_row_id=proof["target_row_id"],
        stock_code=code,
        trade_date=session,
        pre_close=values["pre_close"],
        open=values["open"],
        high=values["high"],
        low=values["low"],
        close=values["close"],
        volume_shares=values["volume_shares"],
        amount=values["amount"],
        received_at=received,
        data_source=data_source,
        batch_id=batch_id,
        data_version=data_version,
        quality_status=quality,
        permission_status=permission,
        attestation_id=attestation_id,
        attestation_run_id=run_id,
        attested_at=attested,
        qmt_fact_sha256=_sha256(proof),
    )


def freeze_qmt_upper_limit_quote_targets(
    connection,
    *,
    subject: EastmoneyUpperLimitSubject,
) -> tuple[QmtUpperLimitQuoteTarget, ...]:
    """Load the exact Top80 target-day rows with independent QMT attestation."""

    placeholders = ",".join(
        f":stock_code_{index}" for index in range(len(subject.stock_codes))
    )
    rows = connection.execute(text(f"""
        SELECT k.id AS target_row_id, LEFT(k.stock_code, 6) AS stock_code,
               k.trade_date, k.k_type, k.adjust_type, k.pre_close,
               k.`open` AS `open`, k.high AS high, k.low AS low,
               k.`close` AS `close`, k.volume AS volume_shares,
               k.amount AS amount, k.received_at, k.data_source,
               k.batch_id, k.data_version, k.quality_status,
               k.permission_status,
               a.attestation_id, a.run_id AS attestation_run_id,
               a.protocol_version, a.source_pre_close_origin,
               a.source_pre_close AS attested_pre_close,
               a.attested_open AS attested_open,
               a.attested_high AS attested_high,
               a.attested_low AS attested_low,
               a.attested_close AS attested_close,
               a.attested_volume AS attested_volume_shares,
               a.attested_amount AS attested_amount,
               a.created_at AS attested_at,
               r.provider AS attestation_provider,
               r.status AS attestation_status,
               r.finished_at AS attestation_finished_at
        FROM sm_stock_kline AS k
        JOIN qmt_kline_attestation_row AS a
          ON a.target_id=k.id
         AND BINARY a.protocol_version=BINARY :protocol_version
         AND BINARY a.source_data_version=BINARY k.data_version
        JOIN qmt_kline_attestation_run AS r
          ON BINARY r.run_id=BINARY a.run_id
        WHERE k.trade_date=:target_date
          AND k.k_type=1 AND k.adjust_type=0
          AND LEFT(k.stock_code, 6) IN ({placeholders})
        ORDER BY LEFT(k.stock_code, 6), k.id
    """), {
        "protocol_version": ATTESTATION_PROTOCOL_VERSION,
        "target_date": subject.target_date.isoformat(),
        **{
            f"stock_code_{index}": code
            for index, code in enumerate(subject.stock_codes)
        },
    }).fetchall()
    targets = tuple(
        _qmt_target_from_row(
            _mapping(row),
            target_date=subject.target_date,
            decision_at=subject.decision_at,
        )
        for row in rows
    )
    codes = tuple(item.stock_code for item in targets)
    row_ids = tuple(item.target_row_id for item in targets)
    if (
        codes != subject.stock_codes
        or len(set(codes)) != len(subject.stock_codes)
        or len(set(row_ids)) != len(subject.stock_codes)
    ):
        raise _blocked(
            f"attested QMT Top80 coverage {len(targets)}/{len(subject.stock_codes)} differs"
        )
    return targets


def _request_payload(stock_code: str, *, host: str) -> bytes:
    params = {
        "fields": EASTMONEY_UPPER_LIMIT_FIELDS,
        "invt": "2",
        "secid": _eastmoney_secid(stock_code),
    }
    query = urlencode(sorted(params.items()))
    return _canonical_json({
        "schema": "probiga.eastmoney-upper-limit-quote-request.v1",
        "method": "GET",
        "url": f"{host}{EASTMONEY_UPPER_LIMIT_API_PATH}?{query}",
        "headers": {
            "Accept": "application/json,text/plain,*/*",
            "User-Agent": "Mozilla/5.0 ProBigA upper-limit-evidence/1",
        },
    }).encode("utf-8")


def fetch_eastmoney_upper_limit_quote(
    target: QmtUpperLimitQuoteTarget,
    *,
    timeout_seconds: float = 12.0,
    attempts: int = 7,
    hosts: Sequence[str] = EASTMONEY_UPPER_LIMIT_HOSTS,
) -> EastmoneyQuoteHttpResponse:
    """Fetch one quote while retaining exact request/response/HTTP-Date bytes."""

    if not hosts:
        raise _transport_blocked("Eastmoney upper-limit host set is empty")
    last_error: Exception | None = None
    for attempt in range(max(1, int(attempts))):
        host = str(hosts[attempt % len(hosts)]).rstrip("/")
        request_payload = _request_payload(target.stock_code, host=host)
        request = json.loads(request_payload.decode("utf-8"))
        started = datetime.now(_SHANGHAI).replace(tzinfo=None)
        try:
            curl_binary = "curl.exe" if os.name == "nt" else "curl"
            command = [
                curl_binary,
                "--silent",
                "--show-error",
                "--fail",
                "--noproxy",
                "*",
                "--dump-header",
                "-",
                "--output",
                "-",
                "--max-time",
                str(int(math.ceil(max(1.0, float(timeout_seconds))))),
                "--header",
                f"Accept: {request['headers']['Accept']}",
                "--user-agent",
                request["headers"]["User-Agent"],
                request["url"],
            ]
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                timeout=max(1.0, float(timeout_seconds)) + 5.0,
            )
            captured = datetime.now(_SHANGHAI).replace(tzinfo=None)
            if completed.returncode != 0:
                error = completed.stderr.decode("utf-8", errors="replace")[:240]
                raise OSError(f"curl={completed.returncode} {error}")
            raw_http = bytes(completed.stdout)
            separator = b"\r\n\r\n" if b"\r\n\r\n" in raw_http else b"\n\n"
            if separator not in raw_http:
                raise OSError("curl response has no HTTP header boundary")
            header_payload, raw_payload = raw_http.split(separator, 1)
            lines = header_payload.decode("iso-8859-1", errors="strict").splitlines()
            if not lines or re.fullmatch(r"HTTP/\S+ 200(?: .*)?", lines[0].strip()) is None:
                raise OSError("curl response HTTP status differs")
            dates = [
                line.split(":", 1)[1].strip()
                for line in lines[1:]
                if ":" in line and line.split(":", 1)[0].strip().lower() == "date"
            ]
            if len(dates) != 1:
                raise OSError("curl response HTTP Date differs")
            http_date = dates[0]
            if not raw_payload or not http_date:
                raise _blocked(
                    f"Eastmoney quote lacks response/HTTP Date for {target.stock_code}"
                )
            return EastmoneyQuoteHttpResponse(
                stock_code=target.stock_code,
                request_started_at=started,
                captured_at=captured,
                request_payload=request_payload,
                request_payload_sha256=_sha256(request_payload),
                raw_payload=raw_payload,
                raw_payload_sha256=_sha256(raw_payload),
                http_date_text=http_date,
            )
        except EastmoneyUpperLimitQuoteBlocked:
            raise
        except (requests.RequestException, OSError, subprocess.TimeoutExpired) as exc:
            last_error = exc
            if attempt + 1 < max(1, int(attempts)):
                time.sleep(min(1.5, 0.2 * (2**attempt)))
                continue
    raise _transport_blocked(
        f"Eastmoney quote transport failed for {target.stock_code}: {last_error}"
    )


def parse_eastmoney_upper_limit_quote(
    *,
    subject: EastmoneyUpperLimitSubject,
    target: QmtUpperLimitQuoteTarget,
    response: EastmoneyQuoteHttpResponse,
) -> CapturedEastmoneyUpperLimitQuote:
    """Validate provider date/fields against a different, attested QMT source."""

    if response.stock_code != target.stock_code:
        raise _blocked("Eastmoney quote response/request stock identity differs")
    if (
        _sha256(response.request_payload) != response.request_payload_sha256
        or _sha256(response.raw_payload) != response.raw_payload_sha256
    ):
        raise _blocked(f"Eastmoney raw request/response hash differs for {target.stock_code}")
    if (
        target.stock_code not in subject.stock_codes
        or target.trade_date != subject.target_date
        or not response.request_started_at <= response.captured_at <= subject.decision_at
    ):
        raise _blocked(f"Eastmoney quote capture crossed the frozen subject for {target.stock_code}")
    http_text, http_at = _http_datetime(
        response.http_date_text, field="Eastmoney HTTP Date"
    )
    if (
        http_at > subject.decision_at
        or abs((http_at - response.captured_at).total_seconds()) > 600
    ):
        raise _blocked(f"Eastmoney HTTP Date differs from capture time for {target.stock_code}")
    try:
        payload = json.loads(response.raw_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _blocked(f"Eastmoney quote JSON is invalid for {target.stock_code}") from exc
    data = payload.get("data") if isinstance(payload, Mapping) else None
    if (
        not isinstance(payload, Mapping)
        or payload.get("rc") != 0
        or not isinstance(data, Mapping)
        or str(data.get("f57") or "").strip() != target.stock_code
    ):
        raise _blocked(f"Eastmoney quote response identity differs for {target.stock_code}")
    precision_raw = data.get("f59")
    if isinstance(precision_raw, bool):
        raise _blocked(f"Eastmoney quote precision differs for {target.stock_code}")
    try:
        precision = int(precision_raw)
    except (TypeError, ValueError) as exc:
        raise _blocked(f"Eastmoney quote precision differs for {target.stock_code}") from exc
    if precision_raw != precision and str(precision_raw) != str(precision):
        raise _blocked(f"Eastmoney quote precision differs for {target.stock_code}")
    if not 0 <= precision <= 4:
        raise _blocked(f"Eastmoney quote precision differs for {target.stock_code}")
    timestamp_raw = _decimal(data.get("f86"), field="f86", nonnegative=True)
    if timestamp_raw != timestamp_raw.to_integral_value() or timestamp_raw < 1_000_000_000:
        raise _blocked(f"Eastmoney source timestamp differs for {target.stock_code}")
    try:
        source_trade_at = datetime.fromtimestamp(
            int(timestamp_raw), tz=timezone.utc
        ).astimezone(_SHANGHAI).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError) as exc:
        raise _blocked(f"Eastmoney source timestamp differs for {target.stock_code}") from exc
    if (
        source_trade_at.date() != subject.target_date
        or source_trade_at > response.captured_at
    ):
        raise _blocked(
            f"Eastmoney source_trade_date is {source_trade_at.date()}, expected {subject.target_date}"
        )
    source_prices = {
        name: _price_from_quote(data[field], precision=precision, field=field)
        for name, field in (
            ("close", "f43"),
            ("high", "f44"),
            ("low", "f45"),
            ("open", "f46"),
            ("upper_limit", "f51"),
            ("lower_limit", "f52"),
            ("pre_close", "f60"),
        )
    }
    source_volume = _decimal(data.get("f47"), field="f47", nonnegative=True) * 100
    source_amount = _decimal(data.get("f48"), field="f48", nonnegative=True)
    qmt_prices = {
        "pre_close": target.pre_close,
        "open": target.open,
        "high": target.high,
        "low": target.low,
        "close": target.close,
    }
    if any(source_prices[name] != value for name, value in qmt_prices.items()):
        raise _blocked(
            f"Eastmoney/QMT independent price comparison differs for {target.stock_code}"
        )
    # QMT currently stores amount at whole-yuan precision while Eastmoney
    # retains cents and the two transports can differ by one yuan.  Preserve
    # both originals, but bind identity to exact QMT prices and exact volume;
    # amount is deliberately not promoted to an equality authority.
    if source_volume != target.volume_shares:
        raise _blocked(
            f"Eastmoney/QMT independent volume/amount comparison differs for {target.stock_code}"
        )
    if not (
        source_prices["upper_limit"]
        > source_prices["pre_close"]
        > source_prices["lower_limit"]
    ):
        raise _blocked(f"Eastmoney limit/pre-close relationship differs for {target.stock_code}")
    source_row = {
        "schema": "probiga.em-upper-quote-row.v1",
        "provider": EASTMONEY_UPPER_LIMIT_PROVIDER,
        "stock_code": target.stock_code,
        "trade_date": subject.target_date.isoformat(),
        "source_trade_at": _datetime_text(source_trade_at),
        "price_precision": precision,
        "fields": {
            "f43": _decimal_text(source_prices["close"]),
            "f44": _decimal_text(source_prices["high"]),
            "f45": _decimal_text(source_prices["low"]),
            "f46": _decimal_text(source_prices["open"]),
            "f47_shares": _decimal_text(source_volume),
            "f48": _decimal_text(source_amount),
            "f51": _decimal_text(source_prices["upper_limit"]),
            "f52": _decimal_text(source_prices["lower_limit"]),
            "f60": _decimal_text(source_prices["pre_close"]),
            "f86": str(int(timestamp_raw)),
        },
    }
    row_text = _canonical_json(source_row)
    if len(row_text.encode("utf-8")) > 512:
        raise _blocked(f"Eastmoney canonical row exceeds ledger capacity for {target.stock_code}")
    target_key = _sha256({
        "schema": "probiga.eastmoney-upper-limit-target-key.v1",
        "subject_sha256": subject.subject_sha256,
        "stock_code": target.stock_code,
        "trade_date": subject.target_date.isoformat(),
        "target_row_id": target.target_row_id,
    })
    semantic = _row_semantic_payload(
        subject_sha256=subject.subject_sha256,
        target_key_sha256=target_key,
        raw_row_text=row_text,
        raw_payload_sha256=response.raw_payload_sha256,
        request_payload_sha256=response.request_payload_sha256,
        captured_at=response.captured_at,
        provider_http_at=http_at,
        qmt_fact_sha256=target.qmt_fact_sha256,
    )
    return CapturedEastmoneyUpperLimitQuote(
        target=target,
        upper_limit=source_prices["upper_limit"],
        lower_limit=source_prices["lower_limit"],
        source_pre_close=source_prices["pre_close"],
        source_open=source_prices["open"],
        source_high=source_prices["high"],
        source_low=source_prices["low"],
        source_close=source_prices["close"],
        source_volume_shares=source_volume,
        source_amount=source_amount,
        source_trade_at=source_trade_at,
        provider_http_date=http_text,
        provider_http_at=http_at,
        request_started_at=response.request_started_at,
        captured_at=response.captured_at,
        request_payload=response.request_payload,
        request_payload_sha256=response.request_payload_sha256,
        raw_payload=response.raw_payload,
        raw_payload_sha256=response.raw_payload_sha256,
        raw_row_text=row_text,
        snapshot_row_sha256=_sha256(semantic),
        target_key_sha256=target_key,
    )


def _row_semantic_payload(
    *,
    subject_sha256: str,
    target_key_sha256: str,
    raw_row_text: str,
    raw_payload_sha256: str,
    request_payload_sha256: str,
    captured_at: datetime,
    provider_http_at: datetime,
    qmt_fact_sha256: str,
) -> dict[str, Any]:
    return {
        "schema": "probiga.eastmoney-upper-limit-quote-semantic.v1",
        "subject_sha256": subject_sha256,
        "target_key_sha256": target_key_sha256,
        "source_row_sha256": _sha256(raw_row_text),
        "raw_payload_sha256": raw_payload_sha256,
        "request_payload_sha256": request_payload_sha256,
        "captured_at": _datetime_text(captured_at),
        "provider_http_at": _datetime_text(provider_http_at),
        "qmt_fact_sha256": qmt_fact_sha256,
    }


def _collector_binary_sha256() -> str:
    try:
        return _sha256(Path(__file__).read_bytes())
    except OSError as exc:
        raise _blocked("upper-limit quote collector source is unreadable") from exc


def build_eastmoney_upper_limit_quote_run(
    *,
    subject: EastmoneyUpperLimitSubject,
    targets: Sequence[QmtUpperLimitQuoteTarget],
    responses: Sequence[EastmoneyQuoteHttpResponse],
    collector_build_sha: str,
    collector_binary_sha256: str | None = None,
    run_id: str | None = None,
) -> EastmoneyUpperLimitQuoteRun:
    identity = str(run_id or uuid.uuid4().hex).strip().lower()
    build_sha = str(collector_build_sha or "").strip().lower()
    binary_sha = str(
        collector_binary_sha256 or _collector_binary_sha256()
    ).strip().lower()
    if (
        _RUN_ID.fullmatch(identity) is None
        or _SHA40.fullmatch(build_sha) is None
        or build_sha == "0" * 40
        or _SHA64.fullmatch(binary_sha) is None
        or binary_sha == "0" * 64
    ):
        raise _blocked("upper-limit quote run/build identity is invalid")
    ordered_targets = tuple(sorted(targets, key=lambda item: item.stock_code))
    ordered_responses = tuple(sorted(responses, key=lambda item: item.stock_code))
    if (
        tuple(item.stock_code for item in ordered_targets) != subject.stock_codes
        or tuple(item.stock_code for item in ordered_responses) != subject.stock_codes
    ):
        raise _blocked("upper-limit quote target/response keyset differs")
    rows = tuple(
        parse_eastmoney_upper_limit_quote(
            subject=subject, target=target, response=response
        )
        for target, response in zip(ordered_targets, ordered_responses)
    )
    request_envelope = _canonical_json({
        "schema": "probiga.eastmoney-upper-limit-quote-request-set.v1",
        "requests": [
            {
                "stock_code": row.target.stock_code,
                "request_payload_base64": base64.b64encode(
                    row.request_payload
                ).decode("ascii"),
                "request_payload_sha256": row.request_payload_sha256,
            }
            for row in rows
        ],
    }).encode("utf-8")
    response_envelope = _canonical_json({
        "schema": "probiga.eastmoney-upper-limit-quote-response-set.v1",
        "responses": [
            {
                "stock_code": row.target.stock_code,
                "provider_http_date": row.provider_http_date,
                "raw_payload_base64": base64.b64encode(row.raw_payload).decode("ascii"),
                "raw_payload_sha256": row.raw_payload_sha256,
            }
            for row in rows
        ],
    }).encode("utf-8")
    qmt_authority = [
        {
            "stock_code": row.target.stock_code,
            "target_row_id": row.target.target_row_id,
            "attestation_id": row.target.attestation_id,
            "attestation_run_id": row.target.attestation_run_id,
            "qmt_fact_sha256": row.target.qmt_fact_sha256,
        }
        for row in rows
    ]
    authority_proof = _sha256(qmt_authority)
    authority_set = _sha256([
        {"stock_code": item["stock_code"], "attestation_id": item["attestation_id"]}
        for item in qmt_authority
    ])
    raw_root = _sha256([
        {"stock_code": row.target.stock_code, "raw_payload_sha256": row.raw_payload_sha256}
        for row in rows
    ])
    value_root = _sha256([
        {"stock_code": row.target.stock_code, "upper_limit": _decimal_text(row.upper_limit)}
        for row in rows
    ])
    target_root = _sha256([
        {
            "stock_code": row.target.stock_code,
            "target_key_sha256": row.target_key_sha256,
            "qmt_fact_sha256": row.target.qmt_fact_sha256,
        }
        for row in rows
    ])
    semantic_root = _sha256([
        {"stock_code": row.target.stock_code, "snapshot_row_sha256": row.snapshot_row_sha256}
        for row in rows
    ])
    endpoint_hosts: set[str] = set()
    for row in rows:
        try:
            request_url = str(json.loads(row.request_payload.decode("utf-8"))["url"])
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _blocked("Eastmoney exact request URL evidence is invalid") from exc
        parsed_url = urlparse(request_url)
        if parsed_url.scheme != "https" or not parsed_url.hostname:
            raise _blocked("Eastmoney exact request endpoint evidence is invalid")
        endpoint_hosts.add(parsed_url.hostname)
    resolved_endpoint = "|".join(f"{host}:443" for host in sorted(endpoint_hosts))
    if not resolved_endpoint or len(resolved_endpoint) > 160:
        raise _blocked("Eastmoney exact request endpoint set is oversized")
    return EastmoneyUpperLimitQuoteRun(
        run_id=identity,
        collector_build_sha=build_sha,
        collector_binary_sha256=binary_sha,
        subject=subject,
        decision_at=subject.decision_at,
        request_started_at=min(row.request_started_at for row in rows),
        captured_at=max(row.captured_at for row in rows),
        provider_observed_at=max(row.provider_http_at for row in rows),
        provider_response_payload=response_envelope,
        provider_response_sha256=_sha256(response_envelope),
        provider_request_payload=request_envelope,
        provider_request_sha256=_sha256(request_envelope),
        resolved_endpoint=resolved_endpoint,
        authority_proof_sha256=authority_proof,
        authority_set_sha256=authority_set,
        raw_payload_root_sha256=raw_root,
        field_value_root_sha256=value_root,
        target_fingerprint_root_sha256=target_root,
        semantic_sha256=semantic_root,
        rows=rows,
    )


def collect_eastmoney_upper_limit_quote_run(
    *,
    subject: EastmoneyUpperLimitSubject,
    targets: Sequence[QmtUpperLimitQuoteTarget],
    collector_build_sha: str,
    workers: int = 8,
    timeout_seconds: float = 12.0,
    fetcher: Callable[[QmtUpperLimitQuoteTarget], EastmoneyQuoteHttpResponse] | None = None,
    run_id: str | None = None,
) -> EastmoneyUpperLimitQuoteRun:
    """Collect exact Top80 quotes in parallel; any missing/mismatch aborts all."""

    ordered = tuple(sorted(targets, key=lambda item: item.stock_code))
    width = max(1, min(16, int(workers)))
    if fetcher is None:
        def fetcher(item: QmtUpperLimitQuoteTarget) -> EastmoneyQuoteHttpResponse:
            return fetch_eastmoney_upper_limit_quote(
                item, timeout_seconds=timeout_seconds
            )
    if width == 1:
        responses = tuple(fetcher(item) for item in ordered)
    else:
        with ThreadPoolExecutor(max_workers=width) as executor:
            responses = tuple(executor.map(fetcher, ordered))
    return build_eastmoney_upper_limit_quote_run(
        subject=subject,
        targets=ordered,
        responses=responses,
        collector_build_sha=collector_build_sha,
        run_id=run_id,
    )


def _run_params(
    run: EastmoneyUpperLimitQuoteRun, *, published_at: datetime
) -> dict[str, Any]:
    count = len(run.rows)
    return {
        "run_id": run.run_id,
        "schema_version": EASTMONEY_UPPER_LIMIT_SCHEMA,
        "collector_build_sha": run.collector_build_sha,
        "capture_kind": EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
        "target_date": run.subject.target_date.isoformat(),
        "window_start_date": run.subject.target_date.isoformat(),
        "window_end_date": run.subject.target_date.isoformat(),
        "decision_at": _datetime_text(run.decision_at),
        "provider": EASTMONEY_UPPER_LIMIT_PROVIDER,
        "api_path": EASTMONEY_UPPER_LIMIT_API_PATH,
        "transport_contract": EASTMONEY_UPPER_LIMIT_TRANSPORT,
        "resolved_endpoint": run.resolved_endpoint,
        "source_field": EASTMONEY_UPPER_LIMIT_SOURCE_FIELD,
        "unit": EASTMONEY_UPPER_LIMIT_UNIT,
        "match_policy": EASTMONEY_UPPER_LIMIT_MATCH_POLICY,
        "promotion_mode": EASTMONEY_UPPER_LIMIT_PROMOTION_MODE,
        "promotion_table": "",
        "promotion_column": "",
        "k_type": 1,
        "adjust_type": 0,
        "subject_kind": EASTMONEY_UPPER_LIMIT_SUBJECT_KIND,
        "subject_identity": run.subject.subject_identity,
        "subject_sha256": run.subject.subject_sha256,
        "subject_payload": run.subject.subject_payload,
        "subject_payload_sha256": run.subject.subject_payload_sha256,
        "authority_proof_kind": "QMT_KLINE_ATTESTATION_SET",
        "authority_proof_identity": "QMT:" + run.authority_set_sha256[:48],
        "authority_proof_sha256": run.authority_proof_sha256,
        "authority_set_sha256": run.authority_set_sha256,
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
        "collector_binary_sha256": run.collector_binary_sha256,
        "provider_sdk_version": "HTTP_JSON",
        "collector_runtime_version": sys.version.split()[0],
        "source_timezone": "Asia/Shanghai",
        "entitlement_status": "PUBLIC_QUOTE",
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "target_fingerprint_root_sha256": run.target_fingerprint_root_sha256,
        "semantic_sha256": run.semantic_sha256,
        "request_started_at": _datetime_text(run.request_started_at),
        "captured_max_at": _datetime_text(run.captured_at),
        "provider_observed_max_at": _datetime_text(run.provider_observed_at),
        "published_at": _datetime_text(published_at),
        "status": "BUILDING",
        "error_message": "",
        "created_at": _datetime_text(published_at),
    }


def _row_params(
    run: EastmoneyUpperLimitQuoteRun,
    row: CapturedEastmoneyUpperLimitQuote,
    *,
    published_at: datetime,
) -> dict[str, Any]:
    target = row.target
    return {
        "run_id": run.run_id,
        "stock_code": target.stock_code,
        "trade_date": target.trade_date.isoformat(),
        "k_type": 1,
        "adjust_type": 0,
        "target_row_id": target.target_row_id,
        "field_value_decimal": _decimal_text(row.upper_limit),
        "source_pre_close": _decimal_text(row.source_pre_close),
        "source_lower_limit": _decimal_text(row.lower_limit),
        "source_is_suspended": 0,
        "source_open": _decimal_text(row.source_open),
        "source_high": _decimal_text(row.source_high),
        "source_low": _decimal_text(row.source_low),
        "source_close": _decimal_text(row.source_close),
        "source_volume_shares": _decimal_text(row.source_volume_shares),
        "source_amount": _decimal_text(row.source_amount),
        "raw_row_text": row.raw_row_text,
        "raw_payload": row.raw_payload,
        "raw_payload_sha256": row.raw_payload_sha256,
        "snapshot_row_sha256": row.snapshot_row_sha256,
        "captured_at": _datetime_text(row.captured_at),
        "provider_observed_at_text": row.provider_http_date,
        "provider_observed_at": _datetime_text(row.provider_http_at),
        "qmt_open": _decimal_text(target.open),
        "qmt_high": _decimal_text(target.high),
        "qmt_low": _decimal_text(target.low),
        "qmt_close": _decimal_text(target.close),
        "qmt_volume_shares": _decimal_text(target.volume_shares),
        "qmt_amount": _decimal_text(target.amount),
        "qmt_received_at": _datetime_text(target.received_at),
        "qmt_data_source": target.data_source,
        "qmt_batch_id": target.batch_id,
        "qmt_data_version": target.data_version,
        "qmt_quality_status": target.quality_status,
        "qmt_permission_status": target.permission_status,
        "target_prewrite_sha256": target.qmt_fact_sha256,
        "target_fact_sha256": row.snapshot_row_sha256,
        "validation_status": "MATCHED",
        "validation_error": "",
        "promoted_at": _datetime_text(published_at),
    }


def _insert_sql(table_name: str, columns: Sequence[str]) -> str:
    names = ",".join(columns)
    values = ",".join(f":{name}" for name in columns)
    return f"INSERT INTO {table_name} ({names}) VALUES ({values})"


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


def _validate_run_shape(run: EastmoneyUpperLimitQuoteRun) -> None:
    if (
        len(run.rows) != EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT
        or len(run.subject.stock_codes) != EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT
        or tuple(row.target.stock_code for row in run.rows) != run.subject.stock_codes
        or run.subject.target_date != run.rows[-1].target.trade_date
        or run.decision_at != run.subject.decision_at
        or run.captured_at > run.decision_at
        or run.provider_observed_at > run.decision_at
        or not run.resolved_endpoint
        or len(run.resolved_endpoint) > 160
        or _sha256(run.provider_response_payload) != run.provider_response_sha256
        or _sha256(run.provider_request_payload) != run.provider_request_sha256
        or any(
            _SHA64.fullmatch(value) is None or value == "0" * 64
            for value in (
                run.collector_binary_sha256,
                run.authority_proof_sha256,
                run.authority_set_sha256,
                run.raw_payload_root_sha256,
                run.field_value_root_sha256,
                run.target_fingerprint_root_sha256,
                run.semantic_sha256,
            )
        )
    ):
        raise _blocked("upper-limit quote run shape differs")


def _verify_persisted_run(connection, run: EastmoneyUpperLimitQuoteRun, *, status: str) -> None:
    persisted = connection.execute(text(
        f"SELECT * FROM {FIELD_CAPTURE_RUN_TABLE} WHERE run_id=:run_id"
    ), {"run_id": run.run_id}).mappings().all()
    if len(persisted) != 1:
        raise _blocked("upper-limit quote run readback identity differs")
    item = dict(persisted[0])
    expected = _run_params(
        run,
        published_at=_local_datetime(item.get("published_at"), field="published_at"),
    )
    for field in (
        "schema_version", "collector_build_sha", "capture_kind", "provider",
        "api_path", "transport_contract", "source_field", "unit",
        "resolved_endpoint", "match_policy", "promotion_mode", "subject_kind", "subject_identity",
        "subject_sha256", "subject_payload_sha256", "authority_proof_kind",
        "authority_proof_identity", "authority_proof_sha256", "authority_set_sha256",
        "expected_keyset_sha256", "provider_request_sha256",
        "provider_response_sha256", "collector_binary_sha256",
        "raw_payload_root_sha256", "field_value_root_sha256",
        "target_fingerprint_root_sha256", "semantic_sha256",
    ):
        if str(item.get(field) or "") != str(expected[field] or ""):
            raise _blocked(f"upper-limit quote run readback differs: {field}")
    count = len(run.rows)
    if (
        str(item.get("status") or "") != status
        or any(int(item.get(field) or -1) != count for field in (
            "expected_count", "fetched_count", "valid_count", "matched_count"
        ))
        or int(item.get("promoted_count") if item.get("promoted_count") is not None else -1) != 0
        or _bytes(item.get("subject_payload"), field="subject payload")
        != run.subject.subject_payload
        or _bytes(item.get("provider_request_payload"), field="request payload")
        != run.provider_request_payload
        or _bytes(item.get("provider_response_payload"), field="response payload")
        != run.provider_response_payload
    ):
        raise _blocked("upper-limit quote run readback contract differs")
    rows = connection.execute(text(
        f"SELECT * FROM {FIELD_CAPTURE_ROW_TABLE} WHERE run_id=:run_id "
        "ORDER BY stock_code"
    ), {"run_id": run.run_id}).mappings().all()
    if len(rows) != count:
        raise _blocked("upper-limit quote row readback count differs")
    for expected_row, persisted_row in zip(run.rows, rows):
        raw = dict(persisted_row)
        if (
            str(raw.get("stock_code") or "") != expected_row.target.stock_code
            or int(raw.get("target_row_id") or 0) != expected_row.target.target_row_id
            or _bytes(raw.get("raw_payload"), field="row raw payload") != expected_row.raw_payload
            or _sha256(_bytes(raw.get("raw_payload"), field="row raw payload"))
            != str(raw.get("raw_payload_sha256") or "")
            or str(raw.get("snapshot_row_sha256") or "") != expected_row.snapshot_row_sha256
            or str(raw.get("target_prewrite_sha256") or "")
            != expected_row.target.qmt_fact_sha256
            or str(raw.get("validation_status") or "") != "MATCHED"
            or str(raw.get("validation_error") or "")
        ):
            raise _blocked(
                f"upper-limit quote row readback differs for {expected_row.target.stock_code}"
            )


def _receipt(run: EastmoneyUpperLimitQuoteRun) -> dict[str, Any]:
    return {
        "schema": EASTMONEY_UPPER_LIMIT_SCHEMA,
        "status": "COMPLETED",
        "capture_kind": EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
        "provider": EASTMONEY_UPPER_LIMIT_PROVIDER,
        "run_id": run.run_id,
        "target_date": run.subject.target_date.isoformat(),
        "decision_at": run.decision_at.isoformat(timespec="seconds"),
        "expected_count": len(run.rows),
        "matched_count": len(run.rows),
        "source_trade_date_count": len({row.source_trade_at.date() for row in run.rows}),
        "subject_sha256": run.subject.subject_sha256,
        "preliminary_receipt_sha256": run.subject.preliminary_receipt_sha256,
        "expected_keyset_sha256": run.subject.expected_keyset_sha256,
        "authority_proof_sha256": run.authority_proof_sha256,
        "authority_set_sha256": run.authority_set_sha256,
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "target_fingerprint_root_sha256": run.target_fingerprint_root_sha256,
        "semantic_sha256": run.semantic_sha256,
        "collector_build_sha": run.collector_build_sha,
    }


def publish_eastmoney_upper_limit_quote_run(
    engine,
    run: EastmoneyUpperLimitQuoteRun,
    *,
    published_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Atomically append the 80-row target-day evidence-only quote run."""

    _validate_run_shape(run)
    published = _local_datetime(
        published_at or datetime.now(_SHANGHAI), field="published_at"
    )
    if not run.captured_at <= published <= run.decision_at:
        raise _blocked("upper-limit quote publication crossed the decision cutoff")
    with _publication_connection(engine) as connection:
        existing = connection.execute(text(f"""
            SELECT run_id, status
            FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE capture_kind=:capture_kind AND target_date=:target_date
              AND subject_sha256=:subject_sha256 AND decision_at=:decision_at
        """), {
            "capture_kind": EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
            "target_date": run.subject.target_date.isoformat(),
            "subject_sha256": run.subject.subject_sha256,
            "decision_at": _datetime_text(run.decision_at),
        }).mappings().all()
        if existing:
            if len(existing) != 1 or str(existing[0].get("status") or "") != "COMPLETED":
                raise _blocked("upper-limit quote logical publication is not terminal")
            recovered = replace(run, run_id=str(existing[0]["run_id"]))
            _verify_persisted_run(connection, recovered, status="COMPLETED")
            return {**_receipt(recovered), "recovered": True}
        run_params = _run_params(run, published_at=published)
        connection.execute(
            text(_insert_sql(FIELD_CAPTURE_RUN_TABLE, tuple(run_params))), run_params
        )
        row_params = [
            _row_params(run, row, published_at=published) for row in run.rows
        ]
        connection.execute(
            text(_insert_sql(FIELD_CAPTURE_ROW_TABLE, tuple(row_params[0]))),
            row_params,
        )
        _verify_persisted_run(connection, run, status="BUILDING")
        terminal = connection.execute(text(
            f"UPDATE {FIELD_CAPTURE_RUN_TABLE} SET status='COMPLETED' "
            "WHERE run_id=:run_id AND status='BUILDING'"
        ), {"run_id": run.run_id})
        if int(getattr(terminal, "rowcount", -1)) != 1:
            raise _blocked("upper-limit quote terminal transition was not exact")
        _verify_persisted_run(connection, run, status="COMPLETED")
    return _receipt(run)


def _decode_envelope_map(
    payload: bytes,
    *,
    schema: str,
    list_field: str,
) -> dict[str, dict[str, Any]]:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _blocked("upper-limit quote aggregate envelope is invalid") from exc
    items = decoded.get(list_field) if isinstance(decoded, Mapping) else None
    if decoded.get("schema") != schema or not isinstance(items, list):
        raise _blocked("upper-limit quote aggregate envelope identity differs")
    result: dict[str, dict[str, Any]] = {}
    for raw in items:
        if not isinstance(raw, Mapping):
            raise _blocked("upper-limit quote aggregate envelope row differs")
        item = dict(raw)
        code = _stock_code(item.get("stock_code"))
        if code in result:
            raise _blocked("upper-limit quote aggregate envelope has duplicate codes")
        result[code] = item
    return result


def _reconstruct_persisted_run(
    connection,
    *,
    persisted: Mapping[str, Any],
    subject: EastmoneyUpperLimitSubject,
) -> EastmoneyUpperLimitQuoteRun:
    item = dict(persisted)
    response_payload = _bytes(
        item.get("provider_response_payload"), field="provider response payload"
    )
    request_payload = _bytes(
        item.get("provider_request_payload"), field="provider request payload"
    )
    if (
        str(item.get("status") or "") != "COMPLETED"
        or str(item.get("schema_version") or "") != EASTMONEY_UPPER_LIMIT_SCHEMA
        or str(item.get("capture_kind") or "") != EASTMONEY_UPPER_LIMIT_CAPTURE_KIND
        or str(item.get("provider") or "") != EASTMONEY_UPPER_LIMIT_PROVIDER
        or str(item.get("api_path") or "") != EASTMONEY_UPPER_LIMIT_API_PATH
        or str(item.get("transport_contract") or "") != EASTMONEY_UPPER_LIMIT_TRANSPORT
        or str(item.get("source_field") or "") != EASTMONEY_UPPER_LIMIT_SOURCE_FIELD
        or str(item.get("unit") or "") != EASTMONEY_UPPER_LIMIT_UNIT
        or str(item.get("match_policy") or "") != EASTMONEY_UPPER_LIMIT_MATCH_POLICY
        or str(item.get("promotion_mode") or "") != EASTMONEY_UPPER_LIMIT_PROMOTION_MODE
        or str(item.get("subject_sha256") or "") != subject.subject_sha256
        or str(item.get("subject_payload_sha256") or "")
        != subject.subject_payload_sha256
        or _bytes(item.get("subject_payload"), field="subject payload")
        != subject.subject_payload
        or _sha256(response_payload) != str(item.get("provider_response_sha256") or "")
        or _sha256(request_payload) != str(item.get("provider_request_sha256") or "")
        or any(
            int(item.get(field) or -1) != EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT
            for field in ("expected_count", "fetched_count", "valid_count", "matched_count")
        )
        or int(item.get("promoted_count") if item.get("promoted_count") is not None else -1) != 0
    ):
        raise _blocked("persisted upper-limit quote run contract differs")
    targets = freeze_qmt_upper_limit_quote_targets(connection, subject=subject)
    request_map = _decode_envelope_map(
        request_payload,
        schema="probiga.eastmoney-upper-limit-quote-request-set.v1",
        list_field="requests",
    )
    response_map = _decode_envelope_map(
        response_payload,
        schema="probiga.eastmoney-upper-limit-quote-response-set.v1",
        list_field="responses",
    )
    persisted_rows = connection.execute(text(
        f"SELECT * FROM {FIELD_CAPTURE_ROW_TABLE} WHERE run_id=:run_id "
        "ORDER BY stock_code"
    ), {"run_id": str(item.get("run_id") or "")}).mappings().all()
    if (
        set(request_map) != set(subject.stock_codes)
        or set(response_map) != set(subject.stock_codes)
        or len(persisted_rows) != EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT
    ):
        raise _blocked("persisted upper-limit quote aggregate keyset differs")
    row_map = {
        _stock_code(row.get("stock_code")): dict(row) for row in persisted_rows
    }
    if set(row_map) != set(subject.stock_codes):
        raise _blocked("persisted upper-limit quote row keyset differs")
    responses: list[EastmoneyQuoteHttpResponse] = []
    request_started = _local_datetime(
        item.get("request_started_at"), field="request_started_at"
    )
    for code in subject.stock_codes:
        request_item = request_map[code]
        response_item = response_map[code]
        row = row_map[code]
        try:
            exact_request = base64.b64decode(
                str(request_item.get("request_payload_base64") or ""), validate=True
            )
            exact_response = base64.b64decode(
                str(response_item.get("raw_payload_base64") or ""), validate=True
            )
        except (ValueError, TypeError) as exc:
            raise _blocked("persisted upper-limit quote base64 evidence differs") from exc
        request_hash = str(request_item.get("request_payload_sha256") or "")
        response_hash = str(response_item.get("raw_payload_sha256") or "")
        http_date = str(response_item.get("provider_http_date") or "")
        if (
            _sha256(exact_request) != request_hash
            or _sha256(exact_response) != response_hash
            or _bytes(row.get("raw_payload"), field="row raw payload") != exact_response
            or str(row.get("raw_payload_sha256") or "") != response_hash
            or str(row.get("provider_observed_at_text") or "") != http_date
        ):
            raise _blocked(f"persisted upper-limit quote raw evidence differs for {code}")
        responses.append(EastmoneyQuoteHttpResponse(
            stock_code=code,
            request_started_at=request_started,
            captured_at=_local_datetime(row.get("captured_at"), field="captured_at"),
            request_payload=exact_request,
            request_payload_sha256=request_hash,
            raw_payload=exact_response,
            raw_payload_sha256=response_hash,
            http_date_text=http_date,
        ))
    rebuilt = build_eastmoney_upper_limit_quote_run(
        subject=subject,
        targets=targets,
        responses=responses,
        collector_build_sha=str(item.get("collector_build_sha") or ""),
        collector_binary_sha256=str(item.get("collector_binary_sha256") or ""),
        run_id=str(item.get("run_id") or ""),
    )
    _verify_persisted_run(connection, rebuilt, status="COMPLETED")
    return rebuilt


def recover_completed_eastmoney_upper_limit_quote_receipt(
    engine,
    *,
    subject: EastmoneyUpperLimitSubject,
    collector_build_sha: str,
) -> dict[str, Any] | None:
    build_sha = str(collector_build_sha or "").strip().lower()
    if _SHA40.fullmatch(build_sha) is None or build_sha == "0" * 40:
        raise _blocked("upper-limit quote recovery build identity is invalid")
    with engine.connect() as connection:
        rows = connection.execute(text(f"""
            SELECT * FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE capture_kind=:capture_kind AND target_date=:target_date
              AND subject_sha256=:subject_sha256 AND decision_at=:decision_at
        """), {
            "capture_kind": EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
            "target_date": subject.target_date.isoformat(),
            "subject_sha256": subject.subject_sha256,
            "decision_at": _datetime_text(subject.decision_at),
        }).mappings().all()
        if not rows:
            return None
        if len(rows) != 1 or str(rows[0].get("collector_build_sha") or "") != build_sha:
            raise _blocked("upper-limit quote completed publication build differs")
        rebuilt = _reconstruct_persisted_run(
            connection, persisted=dict(rows[0]), subject=subject
        )
    return {**_receipt(rebuilt), "recovered": True}


def load_verified_eastmoney_upper_limit_quotes(
    engine,
    *,
    subject: EastmoneyUpperLimitSubject,
    collector_build_sha: str,
) -> dict[str, dict[str, Any]]:
    """Read target-day quotes after full raw-response and QMT revalidation."""

    build_sha = str(collector_build_sha or "").strip().lower()
    with engine.connect() as connection:
        rows = connection.execute(text(f"""
            SELECT * FROM {FIELD_CAPTURE_RUN_TABLE}
            WHERE capture_kind=:capture_kind AND target_date=:target_date
              AND subject_sha256=:subject_sha256 AND decision_at=:decision_at
              AND status='COMPLETED'
        """), {
            "capture_kind": EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
            "target_date": subject.target_date.isoformat(),
            "subject_sha256": subject.subject_sha256,
            "decision_at": _datetime_text(subject.decision_at),
        }).mappings().all()
        if not rows:
            return {}
        if (
            len(rows) != 1
            or _SHA40.fullmatch(build_sha) is None
            or str(rows[0].get("collector_build_sha") or "") != build_sha
        ):
            raise _blocked("upper-limit quote verified read identity differs")
        rebuilt = _reconstruct_persisted_run(
            connection, persisted=dict(rows[0]), subject=subject
        )
    result: dict[str, dict[str, Any]] = {}
    for row in rebuilt.rows:
        evidence = {
            "schema": "probiga.eastmoney-upper-limit-quote-evidence.v1",
            "status": "PASS",
            "stock_code": row.target.stock_code,
            "trade_date": row.target.trade_date.isoformat(),
            "source_trade_at": row.source_trade_at.isoformat(timespec="seconds"),
            "known_at": row.captured_at.isoformat(timespec="microseconds"),
            "provider_http_date": row.provider_http_date,
            "provider_http_at": row.provider_http_at.isoformat(timespec="seconds"),
            "provider": EASTMONEY_UPPER_LIMIT_PROVIDER,
            "source_fields": {
                "upper_limit": "f51",
                "lower_limit": "f52",
                "pre_close": "f60",
                "source_trade_time": "f86",
            },
            "upper_limit": _decimal_text(row.upper_limit),
            "lower_limit": _decimal_text(row.lower_limit),
            "pre_close": _decimal_text(row.source_pre_close),
            "raw_payload_sha256": row.raw_payload_sha256,
            "request_payload_sha256": row.request_payload_sha256,
            "qmt_fact_sha256": row.target.qmt_fact_sha256,
            "snapshot_run_id": rebuilt.run_id,
            "snapshot_row_sha256": row.snapshot_row_sha256,
            "snapshot_semantic_sha256": rebuilt.semantic_sha256,
            "collector_build_sha": rebuilt.collector_build_sha,
            "preliminary_receipt_sha256": subject.preliminary_receipt_sha256,
        }
        evidence["proof_sha256"] = _sha256(evidence)
        result[row.target.stock_code] = {
            "upper_limit": row.upper_limit,
            "lower_limit": row.lower_limit,
            "pre_close": row.source_pre_close,
            "evidence": evidence,
        }
    return result


__all__ = [
    "EASTMONEY_UPPER_LIMIT_CAPTURE_KIND",
    "EASTMONEY_UPPER_LIMIT_EXPECTED_STOCK_COUNT",
    "EASTMONEY_UPPER_LIMIT_PROVIDER",
    "CapturedEastmoneyUpperLimitQuote",
    "EastmoneyQuoteHttpResponse",
    "EastmoneyUpperLimitQuoteBlocked",
    "EastmoneyUpperLimitQuoteRun",
    "EastmoneyUpperLimitSubject",
    "QmtUpperLimitQuoteTarget",
    "build_eastmoney_upper_limit_quote_run",
    "build_eastmoney_upper_limit_subject",
    "collect_eastmoney_upper_limit_quote_run",
    "fetch_eastmoney_upper_limit_quote",
    "freeze_qmt_upper_limit_quote_targets",
    "load_verified_eastmoney_upper_limit_quotes",
    "parse_eastmoney_upper_limit_quote",
    "publish_eastmoney_upper_limit_quote_run",
    "recover_completed_eastmoney_upper_limit_quote_receipt",
]
