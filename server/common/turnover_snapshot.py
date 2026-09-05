# -*- coding: utf-8 -*-
"""Immutable direct-turnover capture, verification, and NULL-only promotion.

The source contract is deliberately narrow: Eastmoney push2his daily K-line
field ``f61`` in percentage units, cross-checked against one frozen set of
QMT-attested target-session OHLCV rows.  No derived share denominator and no
legacy ``turnover_ratio`` value can satisfy this contract.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import subprocess
import time
import uuid
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from server.common.analysis_pool_receipt import (
    TURNOVER_DIRECT_FORMULA,
    build_turnover_evidence,
)
from server.common.mysql_lock import (
    STOCK_KLINE_FREEZE_LOCK_NAME,
    mysql_named_lock,
)
from server.common.qmt_attestation_contract import expected_stock_set_contract
from server.common.qmt_attestation_contract import (
    validated_no_row_exception_contract,
)
from server.common.qmt_daily_no_row import project_catalog_daily_codes
from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
from server.common.qmt_stock_catalog import (
    load_stock_catalog,
    validate_stock_catalog_immutability,
)
from server.common.qmt_trade_calendar import validate_trade_calendar_immutability
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from server.common.turnover_snapshot_schema import (
    TURNOVER_SNAPSHOT_ROW_TABLE,
    TURNOVER_SNAPSHOT_RUN_TABLE,
)


TURNOVER_SNAPSHOT_VERSION = "probiga.market-field-capture.v1"
TURNOVER_CAPTURE_KIND = "DAILY_TURNOVER_F61"
TURNOVER_MATCH_POLICY = "EXACT_QMT_OHLCV"
TURNOVER_PROMOTION_MODE = "NULL_ONLY_EXACT_KEY"
TURNOVER_DIRECT_TLS_TRANSPORT = "HTTPS_TLS_VERIFIED_DIRECT"
TURNOVER_PINNED_TLS_TRANSPORT = "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1"
DEFAULT_PUSH2HIS_RESOLVE_IP = "61.129.129.48"
TURNOVER_PROVIDER = "eastmoney.push2his.kline"
TURNOVER_API_PATH = "/api/qt/stock/kline/get"
TURNOVER_SOURCE_FIELD = "f61"
TURNOVER_UNIT = "PERCENT"
MIN_TURNOVER_UNIVERSE_COUNT = 3000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODE = re.compile(r"[0-9]{6}")
_SHA40 = re.compile(r"[0-9a-f]{40}")
_SHA64 = re.compile(r"[0-9a-f]{64}")

DEFAULT_EASTMONEY_HOSTS = (
    "https://push2his.eastmoney.com",
    "https://33.push2his.eastmoney.com",
    "https://63.push2his.eastmoney.com",
    "https://81.push2his.eastmoney.com",
    "https://90.push2his.eastmoney.com",
)


def _now_shanghai() -> datetime:
    return datetime.now(_SHANGHAI).replace(tzinfo=None)


class TurnoverSnapshotBlocked(RuntimeError):
    """Raised whenever the immutable evidence contract cannot be proven."""


class TurnoverTransportError(TurnoverSnapshotBlocked):
    """A retryable transport failure before any provider fact was accepted."""


def _blocked(message: str) -> TurnoverSnapshotBlocked:
    return TurnoverSnapshotBlocked(f"DATA_BLOCKED: {message}")


def _transport_blocked(message: str) -> TurnoverTransportError:
    return TurnoverTransportError(f"DATA_BLOCKED: {message}")


def _canonical_decimal(value: Any, *, field: str, nonnegative: bool = True) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise _blocked(f"{field} is not an exact decimal") from exc
    if not result.is_finite() or (nonnegative and result < 0):
        raise _blocked(f"{field} is outside the supported range")
    return result


def _decimal_text(value: Decimal | Any) -> str:
    number = value if isinstance(value, Decimal) else _canonical_decimal(
        value, field="canonical decimal", nonnegative=False
    )
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def _local_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = str(value or "").strip()
        try:
            result = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise _blocked(f"{field} is not an ISO datetime") from exc
    if result.tzinfo is not None:
        result = result.astimezone(_SHANGHAI).replace(tzinfo=None)
    return result


def _datetime_text(value: datetime) -> str:
    return _local_datetime(value, field="datetime").isoformat(
        sep=" ", timespec="microseconds"
    )


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


def _exact_date(value: Any, *, field: str) -> date:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise _blocked(f"{field} is not an exact ISO date") from exc
    if parsed.isoformat() != raw:
        raise _blocked(f"{field} is not an exact ISO date")
    return parsed


def _stock_code(value: Any) -> str:
    raw = str(value or "").strip()
    if _CODE.fullmatch(raw) is None or raw == "000000":
        raise _blocked(f"invalid target stock code {value!r}")
    return raw


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row._mapping)


def _chunks(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    width = max(1, int(size))
    for offset in range(0, len(values), width):
        yield values[offset : offset + width]


@dataclass(frozen=True)
class QmtTurnoverTarget:
    target_row_id: int
    stock_code: str
    trade_date: date
    k_type: int
    adjust_type: int
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
    turnover_ratio: Decimal | None
    prewrite_sha256: str
    ohlcv_sha256: str


@dataclass(frozen=True)
class CapturedTurnoverRow:
    target: QmtTurnoverTarget
    turnover_percent: Decimal
    source_open: Decimal
    source_high: Decimal
    source_low: Decimal
    source_close: Decimal
    source_volume_shares: Decimal
    source_amount: Decimal
    raw_row_text: str
    raw_payload: bytes
    raw_payload_sha256: str
    captured_at: datetime
    provider_http_date: str
    provider_http_at: datetime
    snapshot_row_sha256: str


@dataclass(frozen=True)
class TurnoverCaptureRun:
    run_id: str
    collector_build_sha: str
    collector_binary_sha256: str
    transport_contract: str
    resolved_endpoint: str
    target_date: date
    decision_at: datetime
    request_started_at: datetime
    captured_max_at: datetime
    provider_http_max_at: datetime
    expected_universe_sha256: str
    provider_response_payload: bytes
    provider_response_sha256: str
    raw_payload_root_sha256: str
    field_value_root_sha256: str
    qmt_fingerprint_root_sha256: str
    semantic_sha256: str
    authority_run_id: str
    authority_truth_sha256: str
    authority_stock_set_sha256: str
    rows: tuple[CapturedTurnoverRow, ...]


@dataclass(frozen=True)
class TurnoverUniverseAuthority:
    target_date: date
    decision_at: datetime
    truth_run_id: str
    truth_sha256: str
    stock_set_sha256: str
    expected_codes: tuple[str, ...]


def load_turnover_universe_authority(
    connection,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    require_triggers: bool = True,
) -> TurnoverUniverseAuthority:
    """Load the immutable catalog-bound QMT daily truth for one session."""

    if type(require_triggers) is not bool:
        raise TypeError("require_triggers must be bool")
    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    if dialect == "mysql" and require_triggers:
        validate_stock_catalog_immutability(connection)
        validate_trade_calendar_immutability(connection)
    truth = load_qmt_daily_market_truth(
        connection,
        start_date=target.isoformat(),
        end_date=target.isoformat(),
        decision_known_at=cutoff,
    )
    catalog = load_stock_catalog(
        connection,
        batch_id=truth.catalog_batch_id,
        decision_known_at=cutoff,
    )
    run_manifest = connection.execute(text("""
        SELECT start_date, end_date, tolerance_json
        FROM qmt_kline_attestation_run
        WHERE run_id=:run_id
    """), {"run_id": truth.run_id}).mappings().one_or_none()
    if run_manifest is None:
        raise _blocked("immutable QMT daily universe manifest is unavailable")
    run_start = _exact_date(
        run_manifest.get("start_date"), field="QMT truth start_date"
    )
    run_end = _exact_date(
        run_manifest.get("end_date"), field="QMT truth end_date"
    )
    if (
        run_start.isoformat() != truth.run_start_date
        or run_end.isoformat() != truth.run_end_date
    ):
        raise _blocked("immutable QMT daily universe manifest differs")
    no_row_contract = validated_no_row_exception_contract(
        run_manifest.get("tolerance_json"),
        start_date=truth.run_start_date,
        end_date=truth.run_end_date,
    )
    if bool(no_row_contract) != bool(
        truth.no_row_exception_proof_sha256
    ) or (
        no_row_contract is not None
        and str(no_row_contract.get("proof_sha256") or "")
        != truth.no_row_exception_proof_sha256
    ):
        raise _blocked("immutable QMT no-row authority differs")
    if no_row_contract is not None:
        calendar = load_trade_calendar_receipt(
            connection,
            batch_id=truth.calendar_batch_id,
            start_date=truth.run_start_date,
            end_date=truth.run_end_date,
            decision_known_at=cutoff,
        )
        projected = project_catalog_daily_codes(
            catalog=catalog,
            calendar=calendar,
            start_date=truth.run_start_date,
            end_date=truth.run_end_date,
            contract=no_row_contract,
        )
        codes = tuple(projected.get(target.isoformat()) or ())
    else:
        codes = tuple(catalog.eligible_codes(target.isoformat()))
    stock_set = expected_stock_set_contract(target.isoformat(), codes)
    if (
        truth.requested_sessions != (target.isoformat(),)
        or truth.catalog_manifest_hash != catalog.manifest_hash
        or truth.catalog_member_set_hash != catalog.member_set_hash
        or truth.attested_row_count != len(codes)
        or int(stock_set["stock_count"]) != len(codes)
        or not codes
    ):
        raise _blocked("immutable QMT daily universe authority differs")
    return TurnoverUniverseAuthority(
        target_date=target,
        decision_at=cutoff,
        truth_run_id=str(truth.run_id),
        truth_sha256=str(truth.truth_hash).lower(),
        stock_set_sha256=str(stock_set["stock_set_hash"]).lower(),
        expected_codes=codes,
    )


def _revalidate_replayable_turnover_authority(
    connection,
    *,
    target_date: date,
    decision_at: datetime,
) -> TurnoverUniverseAuthority | None:
    """Revalidate live authority unless its mutable target projection advanced."""

    try:
        return load_turnover_universe_authority(
            connection,
            target_date=target_date,
            decision_at=decision_at,
            require_triggers=False,
        )
    except RuntimeError as exc:
        if str(exc) != "current QMT target rows/attestations are incomplete":
            raise
        # Once QMT advances to the next session, its current attestation
        # projection may no longer replay an older target.  The completed
        # capture remains independently bound below by immutable run/row
        # hashes plus exact historical sm_stock_kline OHLCV/value readback.
        return None


def _validate_universe_authority(
    authority: TurnoverUniverseAuthority,
    *,
    target_date: date,
    decision_at: datetime,
    codes: Sequence[str],
) -> None:
    if (
        authority.target_date != target_date
        or authority.decision_at != decision_at
        or not authority.truth_run_id
        or len(authority.truth_run_id) > 128
        or _SHA64.fullmatch(authority.truth_sha256) is None
        or authority.truth_sha256 == "0" * 64
        or _SHA64.fullmatch(authority.stock_set_sha256) is None
        or authority.stock_set_sha256 == "0" * 64
        or tuple(codes) != authority.expected_codes
        or str(
            expected_stock_set_contract(
                target_date.isoformat(), authority.expected_codes
            )["stock_set_hash"]
        )
        != authority.stock_set_sha256
    ):
        raise _blocked("immutable QMT daily universe authority differs")


def _qmt_ohlcv_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "target_row_id": int(row["id"]),
        "stock_code": _stock_code(row["stock_code"]),
        "trade_date": _exact_date(row["trade_date"], field="QMT trade_date").isoformat(),
        "k_type": int(row["k_type"]),
        "adjust_type": int(row["adjust_type"]),
        "open": _decimal_text(_canonical_decimal(row["open"], field="QMT open")),
        "high": _decimal_text(_canonical_decimal(row["high"], field="QMT high")),
        "low": _decimal_text(_canonical_decimal(row["low"], field="QMT low")),
        "close": _decimal_text(_canonical_decimal(row["close"], field="QMT close")),
        "volume_shares": _decimal_text(
            _canonical_decimal(row["volume"], field="QMT volume")
        ),
        "amount": _decimal_text(_canonical_decimal(row["amount"], field="QMT amount")),
        "received_at": _datetime_text(
            _local_datetime(row["received_at"], field="QMT received_at")
        ),
        "data_source": str(row.get("data_source") or "").strip(),
        "batch_id": str(row.get("batch_id") or "").strip(),
        "data_version": str(row.get("data_version") or "").strip(),
        "quality_status": str(row.get("quality_status") or "").strip(),
        "permission_status": str(row.get("permission_status") or "").strip(),
    }


def _historical_qmt_row_matches_capture(
    row: Mapping[str, Any],
    capture: CapturedTurnoverRow,
) -> bool:
    """Match stable business facts after a later QMT refresh replaces row identity."""

    target = capture.target
    try:
        live_turnover = row.get("turnover_ratio")
        turnover_matches = (
            live_turnover is None
            or str(live_turnover).strip() == ""
            or _canonical_decimal(live_turnover, field="live turnover")
            == capture.turnover_percent
        )
        return bool(
            _stock_code(row.get("stock_code")) == target.stock_code
            and _exact_date(row.get("trade_date"), field="live trade_date")
            == target.trade_date
            and int(row.get("k_type")) == target.k_type
            and int(row.get("adjust_type")) == target.adjust_type
            and _canonical_decimal(row.get("open"), field="live open")
            == target.open
            and _canonical_decimal(row.get("high"), field="live high")
            == target.high
            and _canonical_decimal(row.get("low"), field="live low")
            == target.low
            and _canonical_decimal(row.get("close"), field="live close")
            == target.close
            and _canonical_decimal(row.get("volume"), field="live volume")
            == target.volume_shares
            and _canonical_decimal(row.get("amount"), field="live amount")
            == target.amount
            and str(row.get("data_source") or "").strip()
            == target.data_source
            and str(row.get("quality_status") or "").strip()
            == target.quality_status
            and str(row.get("permission_status") or "").strip()
            == target.permission_status
            and turnover_matches
        )
    except (TypeError, ValueError, TurnoverSnapshotBlocked):
        return False


def _qmt_target_from_row(row: Mapping[str, Any], *, target_date: date, decision_at: datetime) -> QmtTurnoverTarget:
    payload = _qmt_ohlcv_payload(row)
    if (
        payload["trade_date"] != target_date.isoformat()
        or payload["k_type"] != 1
        or payload["adjust_type"] != 0
        or payload["data_source"] != "gj_big_qmt_inner"
        or payload["quality_status"] != "QMT_ATTESTED"
        or payload["permission_status"] != "SUPPORTED"
        or not payload["batch_id"]
        or not payload["data_version"]
    ):
        raise _blocked(f"QMT target contract differs for {payload['stock_code']}")
    received_at = _local_datetime(row["received_at"], field="QMT received_at")
    if received_at > decision_at:
        raise _blocked(f"QMT target was unknown at the decision cutoff: {payload['stock_code']}")
    turnover_value = row.get("turnover_ratio")
    turnover = None
    if turnover_value is not None and str(turnover_value).strip() != "":
        try:
            candidate = Decimal(str(turnover_value))
        except InvalidOperation as exc:
            raise _blocked(f"QMT turnover is malformed for {payload['stock_code']}") from exc
        if not candidate.is_finite():
            raise _blocked(f"QMT turnover is malformed for {payload['stock_code']}")
        turnover = candidate
    if turnover is not None:
        raise _blocked(
            f"NULL-only turnover promotion target is already populated: {payload['stock_code']}"
        )
    prices = [Decimal(payload[name]) for name in ("open", "high", "low", "close")]
    volume = Decimal(payload["volume_shares"])
    amount = Decimal(payload["amount"])
    if (
        any(value <= 0 for value in prices)
        or volume < 0
        or amount < 0
        or prices[1] < max(prices[0], prices[2], prices[3])
        or prices[2] > min(prices[0], prices[1], prices[3])
    ):
        raise _blocked(f"QMT OHLCV is invalid for {payload['stock_code']}")
    prewrite_payload = {**payload, "turnover_ratio": None}
    return QmtTurnoverTarget(
        target_row_id=int(payload["target_row_id"]),
        stock_code=str(payload["stock_code"]),
        trade_date=target_date,
        k_type=1,
        adjust_type=0,
        open=prices[0],
        high=prices[1],
        low=prices[2],
        close=prices[3],
        volume_shares=volume,
        amount=amount,
        received_at=received_at,
        data_source=str(payload["data_source"]),
        batch_id=str(payload["batch_id"]),
        data_version=str(payload["data_version"]),
        quality_status=str(payload["quality_status"]),
        permission_status=str(payload["permission_status"]),
        turnover_ratio=None,
        prewrite_sha256=_sha256(prewrite_payload),
        ohlcv_sha256=_sha256(payload),
    )


_QMT_TARGET_SQL = """
    SELECT id, stock_code, trade_date, k_type, adjust_type,
           `open`, high, low, `close`, volume, amount, turnover_ratio,
           received_at, data_source, batch_id, data_version,
           quality_status, permission_status
    FROM sm_stock_kline
    WHERE trade_date=:target_date AND k_type=1 AND adjust_type=0
    ORDER BY stock_code, id
"""


def freeze_qmt_turnover_targets(
    connection,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    authority: TurnoverUniverseAuthority,
    min_expected_count: int = MIN_TURNOVER_UNIVERSE_COUNT,
    for_update: bool = False,
) -> tuple[QmtTurnoverTarget, ...]:
    """Freeze and fingerprint the exact, full target-session QMT universe."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    suffix = ""
    dialect = str(getattr(getattr(connection, "dialect", None), "name", "")).lower()
    if for_update and dialect == "mysql":
        suffix = " FOR UPDATE"
    rows = connection.execute(
        text(_QMT_TARGET_SQL + suffix), {"target_date": target.isoformat()}
    ).fetchall()
    targets = tuple(
        _qmt_target_from_row(_mapping(row), target_date=target, decision_at=cutoff)
        for row in rows
    )
    minimum = max(1, int(min_expected_count))
    if len(targets) < minimum:
        raise _blocked(
            f"QMT turnover universe coverage {len(targets)} is below {minimum}"
        )
    codes = [item.stock_code for item in targets]
    row_ids = [item.target_row_id for item in targets]
    if len(set(codes)) != len(codes) or len(set(row_ids)) != len(row_ids):
        raise _blocked("QMT turnover universe contains duplicate stock identities")
    if codes != sorted(codes):
        raise _blocked("QMT turnover universe is not deterministically ordered")
    _validate_universe_authority(
        authority,
        target_date=target,
        decision_at=cutoff,
        codes=codes,
    )
    return targets


def expected_universe_sha256(targets: Sequence[QmtTurnoverTarget]) -> str:
    return _sha256([
        {
            "stock_code": row.stock_code,
            "trade_date": row.trade_date.isoformat(),
            "k_type": row.k_type,
            "adjust_type": row.adjust_type,
            "target_row_id": row.target_row_id,
        }
        for row in targets
    ])


def qmt_fingerprint_root_sha256(targets: Sequence[QmtTurnoverTarget]) -> str:
    return _sha256([
        {"stock_code": row.stock_code, "qmt_prewrite_sha256": row.prewrite_sha256}
        for row in targets
    ])


def turnover_capture_input_sha256(
    *,
    targets: Sequence[QmtTurnoverTarget],
    target_date: date | str,
    decision_at: datetime | str,
    collector_build_sha: str,
    collector_binary_sha256: str,
    authority: TurnoverUniverseAuthority,
    transport_contract: str,
    resolved_endpoint: str,
) -> str:
    """Bind resumable per-stock capture state to its exact immutable input."""

    target = (
        target_date
        if isinstance(target_date, date)
        else _exact_date(target_date, field="target_date")
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    codes = [item.stock_code for item in targets]
    _validate_universe_authority(
        authority,
        target_date=target,
        decision_at=cutoff,
        codes=codes,
    )
    return _sha256({
        "schema": "probiga.turnover-capture-input.v1",
        "target_date": target.isoformat(),
        "decision_at": _datetime_text(cutoff),
        "collector_build_sha": str(collector_build_sha).lower(),
        "collector_binary_sha256": str(collector_binary_sha256).lower(),
        "transport_contract": str(transport_contract),
        "resolved_endpoint": str(resolved_endpoint),
        "expected_universe_sha256": expected_universe_sha256(targets),
        "qmt_fingerprint_root_sha256": qmt_fingerprint_root_sha256(targets),
        "authority_run_id": authority.truth_run_id,
        "authority_truth_sha256": authority.truth_sha256,
        "authority_stock_set_sha256": authority.stock_set_sha256,
    })


def serialize_turnover_checkpoint_row(
    row: CapturedTurnoverRow,
) -> dict[str, Any]:
    return {
        "schema": "probiga.turnover-capture-shard.v1",
        "stock_code": row.target.stock_code,
        "target_prewrite_sha256": row.target.prewrite_sha256,
        "target_fact_sha256": row.target.ohlcv_sha256,
        "raw_payload_base64": base64.b64encode(row.raw_payload).decode("ascii"),
        "raw_payload_sha256": row.raw_payload_sha256,
        "captured_at": _datetime_text(row.captured_at),
        "provider_http_date": row.provider_http_date,
        "provider_http_at": _datetime_text(row.provider_http_at),
        "snapshot_row_sha256": row.snapshot_row_sha256,
    }


def restore_turnover_checkpoint_row(
    payload: Mapping[str, Any],
    *,
    target: QmtTurnoverTarget,
    decision_at: datetime | str,
) -> CapturedTurnoverRow:
    """Reparse and verify one locally checkpointed provider response."""

    if (
        payload.get("schema") != "probiga.turnover-capture-shard.v1"
        or str(payload.get("stock_code") or "") != target.stock_code
        or str(payload.get("target_prewrite_sha256") or "")
        != target.prewrite_sha256
        or str(payload.get("target_fact_sha256") or "")
        != target.ohlcv_sha256
    ):
        raise _blocked(f"turnover checkpoint target differs: {target.stock_code}")
    try:
        raw_payload = base64.b64decode(
            str(payload.get("raw_payload_base64") or ""),
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise _blocked(
            f"turnover checkpoint payload is invalid: {target.stock_code}"
        ) from exc
    reconstructed = parse_eastmoney_turnover_response(
        target=target,
        raw_payload=raw_payload,
        provider_http_date=str(payload.get("provider_http_date") or ""),
        captured_at=payload.get("captured_at"),
        decision_at=decision_at,
    )
    if (
        reconstructed.raw_payload_sha256
        != str(payload.get("raw_payload_sha256") or "")
        or reconstructed.provider_http_at
        != _local_datetime(
            payload.get("provider_http_at"),
            field="checkpoint provider_http_at",
        )
        or reconstructed.snapshot_row_sha256
        != str(payload.get("snapshot_row_sha256") or "")
    ):
        raise _blocked(
            f"turnover checkpoint shard hash differs: {target.stock_code}"
        )
    return reconstructed


def _eastmoney_secid(stock_code: str) -> str:
    code = _stock_code(stock_code)
    # Eastmoney market 1 is Shanghai.  Beijing's current 920xxx symbols use
    # market 0; treating every 9-prefix as Shanghai silently drops valid rows.
    return f"{1 if code.startswith('6') else 0}.{code}"


def _eastmoney_params(target: QmtTurnoverTarget) -> dict[str, str]:
    compact_date = target.trade_date.strftime("%Y%m%d")
    return {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "0",
        "secid": _eastmoney_secid(target.stock_code),
        "beg": compact_date,
        "end": compact_date,
    }


def _provider_http_time(value: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError) as exc:
        raise _blocked("Eastmoney HTTP Date is missing or invalid") from exc
    if parsed.tzinfo is None:
        raise _blocked("Eastmoney HTTP Date has no timezone")
    return parsed.astimezone(_SHANGHAI).replace(tzinfo=None)


def _captured_row_payload(
    *,
    target: QmtTurnoverTarget,
    turnover_percent: Decimal,
    source_open: Decimal,
    source_high: Decimal,
    source_low: Decimal,
    source_close: Decimal,
    source_volume_shares: Decimal,
    source_amount: Decimal,
    raw_row_text: str,
    raw_payload_sha256: str,
    captured_at: datetime,
    provider_http_date: str,
    provider_http_at: datetime,
) -> dict[str, Any]:
    return {
        "schema": TURNOVER_SNAPSHOT_VERSION,
        "provider": TURNOVER_PROVIDER,
        "source_field": TURNOVER_SOURCE_FIELD,
        "unit": TURNOVER_UNIT,
        "stock_code": target.stock_code,
        "trade_date": target.trade_date.isoformat(),
        "k_type": target.k_type,
        "adjust_type": target.adjust_type,
        "target_row_id": target.target_row_id,
        "turnover_percent": _decimal_text(turnover_percent),
        "source_open": _decimal_text(source_open),
        "source_high": _decimal_text(source_high),
        "source_low": _decimal_text(source_low),
        "source_close": _decimal_text(source_close),
        "source_volume_shares": _decimal_text(source_volume_shares),
        "source_amount": _decimal_text(source_amount),
        "raw_row_text": raw_row_text,
        "raw_payload_sha256": raw_payload_sha256,
        "captured_at": _datetime_text(captured_at),
        "provider_http_date": provider_http_date,
        "provider_http_at": _datetime_text(provider_http_at),
        "qmt_prewrite_sha256": target.prewrite_sha256,
        "qmt_ohlcv_sha256": target.ohlcv_sha256,
    }


def parse_eastmoney_turnover_response(
    *,
    target: QmtTurnoverTarget,
    raw_payload: bytes,
    provider_http_date: str,
    captured_at: datetime | str,
    decision_at: datetime | str,
) -> CapturedTurnoverRow:
    """Parse one exact f61 response and cross-check its OHLCV against QMT."""

    captured = _local_datetime(captured_at, field="captured_at")
    cutoff = _local_datetime(decision_at, field="decision_at")
    http_at = _provider_http_time(provider_http_date)
    if captured > cutoff or http_at > cutoff:
        raise _blocked(f"turnover source was captured after decision cutoff: {target.stock_code}")
    if not isinstance(raw_payload, bytes) or not raw_payload:
        raise _blocked(f"empty Eastmoney payload for {target.stock_code}")
    try:
        payload = json.loads(raw_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _blocked(f"invalid Eastmoney JSON for {target.stock_code}") from exc
    if not isinstance(payload, dict) or int(payload.get("rc", -1)) != 0:
        raise _blocked(f"Eastmoney response status differs for {target.stock_code}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise _blocked(f"Eastmoney response data is missing for {target.stock_code}")
    if _stock_code(data.get("code")) != target.stock_code:
        raise _blocked(f"Eastmoney response code differs for {target.stock_code}")
    klines = data.get("klines")
    if not isinstance(klines, list) or len(klines) != 1 or not isinstance(klines[0], str):
        raise _blocked(f"Eastmoney target row coverage differs for {target.stock_code}")
    raw_row_text = klines[0]
    if len(raw_row_text) > 512:
        raise _blocked(f"Eastmoney target row is oversized for {target.stock_code}")
    parts = raw_row_text.split(",")
    if len(parts) != 11:
        raise _blocked(f"Eastmoney f51-f61 schema differs for {target.stock_code}")
    source_date = _exact_date(parts[0], field="Eastmoney source date")
    if source_date != target.trade_date:
        raise _blocked(f"Eastmoney source date differs for {target.stock_code}")
    values = [
        _canonical_decimal(
            parts[index],
            field=f"Eastmoney f{51 + index}",
            # f59 (change_pct) and f60 (change) are signed on down days.
            # Every other member of the fixed f52..f61 schema used here is
            # non-negative.  Parsing the whole tuple as non-negative made a
            # legitimate declining stock abort the full-market snapshot.
            nonnegative=index not in {8, 9},
        )
        for index in range(1, 11)
    ]
    source_open, source_close, source_high, source_low = values[:4]
    source_volume_shares = values[4] * Decimal("100")
    source_amount = values[5]
    turnover_percent = values[9]
    if source_volume_shares != source_volume_shares.to_integral_value():
        raise _blocked(f"Eastmoney volume lot conversion is not integral: {target.stock_code}")
    if turnover_percent != turnover_percent.quantize(Decimal("0.01")):
        raise _blocked(f"Eastmoney f61 is not a percentage with 0.01 precision: {target.stock_code}")
    if (
        source_open != target.open
        or source_high != target.high
        or source_low != target.low
        or source_close != target.close
        or source_volume_shares != target.volume_shares
    ):
        raise _blocked(f"Eastmoney/QMT OHLCV fingerprint differs for {target.stock_code}")
    raw_hash = _sha256(raw_payload)
    row_payload = _captured_row_payload(
        target=target,
        turnover_percent=turnover_percent,
        source_open=source_open,
        source_high=source_high,
        source_low=source_low,
        source_close=source_close,
        source_volume_shares=source_volume_shares,
        source_amount=source_amount,
        raw_row_text=raw_row_text,
        raw_payload_sha256=raw_hash,
        captured_at=captured,
        provider_http_date=provider_http_date,
        provider_http_at=http_at,
    )
    return CapturedTurnoverRow(
        target=target,
        turnover_percent=turnover_percent,
        source_open=source_open,
        source_high=source_high,
        source_low=source_low,
        source_close=source_close,
        source_volume_shares=source_volume_shares,
        source_amount=source_amount,
        raw_row_text=raw_row_text,
        raw_payload=raw_payload,
        raw_payload_sha256=raw_hash,
        captured_at=captured,
        provider_http_date=provider_http_date,
        provider_http_at=http_at,
        snapshot_row_sha256=_sha256(row_payload),
    )


class EastmoneyTurnoverCollector:
    """Bounded push2his f61 collector with exact raw-response retention."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        hosts: Iterable[str] = DEFAULT_EASTMONEY_HOSTS,
        timeout_seconds: float = 20.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.session = session or requests.Session()
        if getattr(self.session, "verify", True) is False:
            raise ValueError("formal Eastmoney turnover transport requires TLS verification")
        self.hosts = tuple(str(host).rstrip("/") for host in hosts if str(host).strip())
        if not self.hosts:
            raise ValueError("Eastmoney turnover collector requires at least one host")
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.now = now or _now_shanghai
        self.transport_contract = TURNOVER_DIRECT_TLS_TRANSPORT
        self.resolved_endpoint = ",".join(
            str(urlparse(host).netloc or host) for host in self.hosts
        )[:160]
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/126 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json, text/plain, */*",
        })

    def fetch(
        self,
        target: QmtTurnoverTarget,
        *,
        decision_at: datetime,
    ) -> CapturedTurnoverRow:
        params = _eastmoney_params(target)
        errors: list[str] = []
        for host in self.hosts:
            try:
                response = self.session.get(
                    host + TURNOVER_API_PATH,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                raw_payload = bytes(response.content)
                captured_at = _local_datetime(self.now(), field="captured_at")
                return parse_eastmoney_turnover_response(
                    target=target,
                    raw_payload=raw_payload,
                    provider_http_date=str(response.headers.get("Date") or ""),
                    captured_at=captured_at,
                    decision_at=decision_at,
                )
            except TurnoverSnapshotBlocked:
                # A syntactically successful provider response that violates
                # date/schema/OHLCV semantics is evidence of source drift, not
                # a transport failure to cherry-pick around via another mirror.
                raise
            except Exception as exc:  # each mirror is the same signed source contract
                errors.append(f"{host}:{type(exc).__name__}:{str(exc)[:120]}")
        raise _transport_blocked(
            f"all Eastmoney mirrors failed for {target.stock_code}: {' | '.join(errors)}"
        )


class PinnedCurlEastmoneyTurnoverCollector(EastmoneyTurnoverCollector):
    """TLS-verified hostname request routed to one audited push2his address.

    ``curl --resolve`` preserves the HTTPS hostname for SNI/certificate
    validation while avoiding the host's blocked DNS route.  Deliberately no
    ``--insecure`` or redirect following is permitted.
    """

    def __init__(
        self,
        *,
        host: str = "https://push2his.eastmoney.com",
        resolve_ip: str = DEFAULT_PUSH2HIS_RESOLVE_IP,
        curl_binary: str = "curl.exe",
        timeout_seconds: float = 20.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            hosts=(host,),
            timeout_seconds=timeout_seconds,
            now=now,
        )
        parsed = urlparse(host)
        if parsed.scheme != "https" or not parsed.hostname or parsed.path not in {"", "/"}:
            raise ValueError("pinned Eastmoney host must be one HTTPS origin")
        try:
            normalized_ip = str(ipaddress.ip_address(str(resolve_ip).strip()))
        except ValueError as exc:
            raise ValueError("pinned Eastmoney resolve address is invalid") from exc
        self.host = f"https://{parsed.hostname}"
        self.hostname = parsed.hostname
        self.resolve_ip = normalized_ip
        self.curl_binary = str(curl_binary or "").strip()
        if not self.curl_binary:
            raise ValueError("pinned Eastmoney collector requires curl")
        self.transport_contract = TURNOVER_PINNED_TLS_TRANSPORT
        self.resolved_endpoint = f"{self.hostname}:443:{self.resolve_ip}"

    @staticmethod
    def _split_headers_body(payload: bytes) -> tuple[bytes, bytes]:
        separator = b"\r\n\r\n"
        if separator not in payload:
            separator = b"\n\n"
        if separator not in payload:
            raise _transport_blocked(
                "pinned Eastmoney response has no HTTP header boundary"
            )
        headers, body = payload.split(separator, 1)
        return headers, body

    def fetch(
        self,
        target: QmtTurnoverTarget,
        *,
        decision_at: datetime,
    ) -> CapturedTurnoverRow:
        url = self.host + TURNOVER_API_PATH + "?" + urlencode(
            _eastmoney_params(target)
        )
        command = [
            self.curl_binary,
            "--silent",
            "--show-error",
            "--fail",
            "--noproxy",
            "*",
            "--resolve",
            f"{self.hostname}:443:{self.resolve_ip}",
            "--dump-header",
            "-",
            "--output",
            "-",
            "--max-time",
            str(int(math.ceil(self.timeout_seconds))),
            url,
        ]
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                timeout=self.timeout_seconds + 5.0,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise _transport_blocked(
                f"pinned Eastmoney TLS transport failed for {target.stock_code}: "
                f"{type(exc).__name__}"
            ) from exc
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace")[:300]
            raise _transport_blocked(
                f"pinned Eastmoney TLS request failed for {target.stock_code}: "
                f"curl={completed.returncode} {error}"
            )
        headers, raw_payload = self._split_headers_body(bytes(completed.stdout))
        lines = headers.decode("iso-8859-1", errors="strict").splitlines()
        if not lines or not re.fullmatch(r"HTTP/\S+ 200(?: .*)?", lines[0].strip()):
            raise _transport_blocked(
                f"pinned Eastmoney HTTP status differs for {target.stock_code}"
            )
        date_values = [
            line.split(":", 1)[1].strip()
            for line in lines[1:]
            if ":" in line and line.split(":", 1)[0].strip().lower() == "date"
        ]
        if len(date_values) != 1:
            raise _transport_blocked(
                f"pinned Eastmoney HTTP Date differs for {target.stock_code}"
            )
        return parse_eastmoney_turnover_response(
            target=target,
            raw_payload=raw_payload,
            provider_http_date=date_values[0],
            captured_at=_local_datetime(self.now(), field="captured_at"),
            decision_at=decision_at,
        )


def build_capture_run(
    *,
    targets: Sequence[QmtTurnoverTarget],
    rows: Sequence[CapturedTurnoverRow],
    target_date: date | str,
    decision_at: datetime | str,
    collector_build_sha: str,
    collector_binary_sha256: str,
    authority: TurnoverUniverseAuthority,
    request_started_at: datetime | str,
    transport_contract: str = TURNOVER_DIRECT_TLS_TRANSPORT,
    resolved_endpoint: str = "push2his.eastmoney.com:443",
    run_id: str | None = None,
) -> TurnoverCaptureRun:
    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    started = _local_datetime(request_started_at, field="request_started_at")
    build_sha = str(collector_build_sha or "").strip().lower()
    binary_sha = str(collector_binary_sha256 or "").strip().lower()
    identity = str(run_id or uuid.uuid4().hex).strip().lower()
    transport = str(transport_contract or "").strip()
    endpoint = str(resolved_endpoint or "").strip()
    if _SHA40.fullmatch(build_sha) is None or build_sha == "0" * 40:
        raise _blocked("collector build SHA is unavailable")
    if _SHA64.fullmatch(binary_sha) is None or binary_sha == "0" * 64:
        raise _blocked("collector binary SHA256 is unavailable")
    if re.fullmatch(r"[0-9a-f]{32}", identity) is None:
        raise _blocked("snapshot run identity is invalid")
    if transport not in {
        TURNOVER_DIRECT_TLS_TRANSPORT,
        TURNOVER_PINNED_TLS_TRANSPORT,
    } or not endpoint or len(endpoint) > 160:
        raise _blocked("turnover transport evidence is invalid")
    expected = tuple(targets)
    captured = tuple(rows)
    if not expected or len(expected) != len(captured):
        raise _blocked("turnover capture is not exact full-universe coverage")
    expected_codes = [item.stock_code for item in expected]
    captured_codes = [item.target.stock_code for item in captured]
    if expected_codes != sorted(expected_codes) or captured_codes != expected_codes:
        raise _blocked("turnover capture identities differ from frozen universe")
    _validate_universe_authority(
        authority,
        target_date=target,
        decision_at=cutoff,
        codes=expected_codes,
    )
    for target_row, captured_row in zip(expected, captured):
        if captured_row.target != target_row:
            raise _blocked(f"turnover capture target drifted for {target_row.stock_code}")
        if captured_row.captured_at > cutoff or captured_row.provider_http_at > cutoff:
            raise _blocked(f"turnover capture crossed decision cutoff: {target_row.stock_code}")
        if _sha256(captured_row.raw_payload) != captured_row.raw_payload_sha256:
            raise _blocked(f"turnover raw payload changed for {target_row.stock_code}")
        rebuilt_payload = _captured_row_payload(
            target=target_row,
            turnover_percent=captured_row.turnover_percent,
            source_open=captured_row.source_open,
            source_high=captured_row.source_high,
            source_low=captured_row.source_low,
            source_close=captured_row.source_close,
            source_volume_shares=captured_row.source_volume_shares,
            source_amount=captured_row.source_amount,
            raw_row_text=captured_row.raw_row_text,
            raw_payload_sha256=captured_row.raw_payload_sha256,
            captured_at=captured_row.captured_at,
            provider_http_date=captured_row.provider_http_date,
            provider_http_at=captured_row.provider_http_at,
        )
        if _sha256(rebuilt_payload) != captured_row.snapshot_row_sha256:
            raise _blocked(f"turnover row semantic hash changed for {target_row.stock_code}")
    captured_max = max(item.captured_at for item in captured)
    http_max = max(item.provider_http_at for item in captured)
    if started > captured_max or captured_max > cutoff or http_max > cutoff:
        raise _blocked("turnover capture timestamp order differs")
    raw_root = _sha256([
        {"stock_code": item.target.stock_code, "raw_payload_sha256": item.raw_payload_sha256}
        for item in captured
    ])
    semantic = _sha256([
        {"stock_code": item.target.stock_code, "snapshot_row_sha256": item.snapshot_row_sha256}
        for item in captured
    ])
    value_root = _sha256([
        {
            "stock_code": item.target.stock_code,
            "trade_date": item.target.trade_date.isoformat(),
            "turnover_percent": _decimal_text(item.turnover_percent),
        }
        for item in captured
    ])
    response_manifest = _canonical_json([
        {"stock_code": item.target.stock_code, "raw_payload_sha256": item.raw_payload_sha256}
        for item in captured
    ]).encode("utf-8")
    return TurnoverCaptureRun(
        run_id=identity,
        collector_build_sha=build_sha,
        collector_binary_sha256=binary_sha,
        transport_contract=transport,
        resolved_endpoint=endpoint,
        target_date=target,
        decision_at=cutoff,
        request_started_at=started,
        captured_max_at=captured_max,
        provider_http_max_at=http_max,
        expected_universe_sha256=expected_universe_sha256(expected),
        provider_response_payload=response_manifest,
        provider_response_sha256=_sha256(response_manifest),
        raw_payload_root_sha256=raw_root,
        field_value_root_sha256=value_root,
        qmt_fingerprint_root_sha256=qmt_fingerprint_root_sha256(expected),
        semantic_sha256=semantic,
        authority_run_id=authority.truth_run_id,
        authority_truth_sha256=authority.truth_sha256,
        authority_stock_set_sha256=authority.stock_set_sha256,
        rows=captured,
    )


def collect_turnover_snapshot(
    *,
    targets: Sequence[QmtTurnoverTarget],
    target_date: date | str,
    decision_at: datetime | str,
    collector_build_sha: str,
    collector_binary_sha256: str,
    authority: TurnoverUniverseAuthority,
    collector: EastmoneyTurnoverCollector,
    delay_seconds: float = 1.2,
    batch_every: int = 50,
    batch_pause_seconds: float = 30.0,
    transport_attempts: int = 3,
    transport_backoff_seconds: float = 2.0,
    workers: int = 1,
    sleep: Callable[[float], None] = time.sleep,
    request_started_at: datetime | None = None,
    completed_rows: Mapping[str, CapturedTurnoverRow] | None = None,
    checkpoint_callback: Callable[
        [tuple[CapturedTurnoverRow, ...]], None
    ] | None = None,
) -> TurnoverCaptureRun:
    """Capture every frozen code; partial responses never become a run."""

    cutoff = _local_datetime(decision_at, field="decision_at")
    started = _local_datetime(
        request_started_at or collector.now(), field="request_started_at"
    )
    target_by_code = {item.stock_code: item for item in targets}
    captured_by_code = dict(completed_rows or {})
    if set(captured_by_code) - set(target_by_code):
        raise _blocked("turnover checkpoint contains stocks outside frozen universe")
    for code, captured in captured_by_code.items():
        if captured.target != target_by_code[code]:
            raise _blocked(f"turnover checkpoint target drifted for {code}")
        if (
            captured.captured_at > cutoff
            or captured.provider_http_at > cutoff
            or _sha256(captured.raw_payload) != captured.raw_payload_sha256
        ):
            raise _blocked(f"turnover checkpoint evidence differs for {code}")
    pending_targets = [
        item for item in targets if item.stock_code not in captured_by_code
    ]
    total = len(targets)
    attempts = max(1, min(int(transport_attempts), 5))
    backoff = max(0.0, float(transport_backoff_seconds))
    worker_count = max(1, min(int(workers), 32))

    def fetch_one(target: QmtTurnoverTarget) -> CapturedTurnoverRow:
        for attempt in range(1, attempts + 1):
            try:
                row = collector.fetch(target, decision_at=cutoff)
                if worker_count > 1 and delay_seconds > 0:
                    # Per-worker pacing bounds aggregate request pressure while
                    # still removing the old full-market serial bottleneck.
                    sleep(max(0.0, float(delay_seconds)))
                return row
            except TurnoverTransportError:
                if attempt >= attempts:
                    raise
                sleep(backoff * attempt)
        raise _transport_blocked(
            f"turnover retry state is unreachable for {target.stock_code}"
        )

    def checkpoint() -> None:
        if checkpoint_callback is None:
            return
        checkpoint_callback(tuple(
            captured_by_code[item.stock_code]
            for item in targets
            if item.stock_code in captured_by_code
        ))

    if worker_count == 1:
        for index, target in enumerate(pending_targets, start=1):
            captured_by_code[target.stock_code] = fetch_one(target)
            if (
                index % max(1, int(batch_every or 1)) == 0
                or index == len(pending_targets)
            ):
                checkpoint()
            if index >= len(pending_targets):
                continue
            pause = max(0.0, float(delay_seconds))
            if batch_every > 0 and index % int(batch_every) == 0:
                pause += max(0.0, float(batch_pause_seconds))
            if pause:
                sleep(pause)
    else:
        batch_width = (
            max(worker_count, int(batch_every))
            if int(batch_every) > 0
            else max(1, len(pending_targets))
        )
        for offset in range(0, len(pending_targets), batch_width):
            batch = tuple(pending_targets[offset : offset + batch_width])
            errors: list[Exception] = []
            with ThreadPoolExecutor(
                max_workers=min(worker_count, len(batch)),
                thread_name_prefix="turnover-f61",
            ) as executor:
                futures = {
                    executor.submit(fetch_one, target): target
                    for target in batch
                }
                for future in as_completed(futures):
                    target = futures[future]
                    try:
                        captured_by_code[target.stock_code] = future.result()
                    except Exception as exc:
                        errors.append(exc)
            checkpoint()
            if errors:
                raise errors[0]
            if (
                offset + len(batch) < len(pending_targets)
                and batch_pause_seconds > 0
            ):
                sleep(max(0.0, float(batch_pause_seconds)))
    rows = [captured_by_code[item.stock_code] for item in targets]
    if len(rows) != total:
        raise _blocked("turnover checkpoint did not reach full-universe coverage")
    return build_capture_run(
        targets=targets,
        rows=rows,
        target_date=target_date,
        decision_at=cutoff,
        collector_build_sha=collector_build_sha,
        collector_binary_sha256=collector_binary_sha256,
        authority=authority,
        request_started_at=started,
        transport_contract=collector.transport_contract,
        resolved_endpoint=collector.resolved_endpoint,
    )


def _run_insert_params(run: TurnoverCaptureRun, *, published_at: datetime) -> dict[str, Any]:
    count = len(run.rows)
    return {
        "run_id": run.run_id,
        "schema_version": TURNOVER_SNAPSHOT_VERSION,
        "collector_build_sha": run.collector_build_sha,
        "capture_kind": TURNOVER_CAPTURE_KIND,
        "target_date": run.target_date.isoformat(),
        "window_start_date": run.target_date.isoformat(),
        "window_end_date": run.target_date.isoformat(),
        "decision_at": _datetime_text(run.decision_at),
        "provider": TURNOVER_PROVIDER,
        "api_path": TURNOVER_API_PATH,
        "transport_contract": run.transport_contract,
        "resolved_endpoint": run.resolved_endpoint,
        "source_field": TURNOVER_SOURCE_FIELD,
        "unit": TURNOVER_UNIT,
        "match_policy": TURNOVER_MATCH_POLICY,
        "promotion_mode": TURNOVER_PROMOTION_MODE,
        "promotion_table": "sm_stock_kline",
        "promotion_column": "turnover_ratio",
        "k_type": 1,
        "adjust_type": 0,
        "subject_kind": "QMT_TARGET_UNIVERSE",
        "subject_identity": run.target_date.isoformat(),
        "subject_sha256": run.expected_universe_sha256,
        "authority_proof_kind": "QMT_DAILY_MARKET_TRUTH",
        "authority_proof_identity": run.authority_run_id,
        "authority_proof_sha256": run.authority_truth_sha256,
        "authority_set_sha256": run.authority_stock_set_sha256,
        "expected_count": count,
        "fetched_count": count,
        "valid_count": count,
        "matched_count": count,
        "promoted_count": count,
        "expected_keyset_sha256": run.expected_universe_sha256,
        "provider_response_payload": run.provider_response_payload,
        "provider_response_sha256": run.provider_response_sha256,
        "collector_binary_sha256": run.collector_binary_sha256,
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "target_fingerprint_root_sha256": run.qmt_fingerprint_root_sha256,
        "semantic_sha256": run.semantic_sha256,
        "request_started_at": _datetime_text(run.request_started_at),
        "captured_max_at": _datetime_text(run.captured_max_at),
        "provider_observed_max_at": _datetime_text(run.provider_http_max_at),
        "published_at": _datetime_text(published_at),
        "status": "BUILDING",
        "error_message": "",
        "created_at": _datetime_text(published_at),
    }


_RUN_INSERT_SQL = f"""
    INSERT INTO {TURNOVER_SNAPSHOT_RUN_TABLE} (
      run_id, schema_version, collector_build_sha, capture_kind, target_date,
      window_start_date, window_end_date, decision_at,
      provider, api_path, transport_contract, resolved_endpoint,
      source_field, unit, match_policy, promotion_mode,
      promotion_table, promotion_column, k_type, adjust_type,
      subject_kind, subject_identity, subject_sha256,
      authority_proof_kind, authority_proof_identity,
      authority_proof_sha256, authority_set_sha256,
      expected_count, fetched_count, valid_count, matched_count, promoted_count,
      expected_keyset_sha256, provider_response_payload,
      provider_response_sha256, collector_binary_sha256,
      raw_payload_root_sha256,
      field_value_root_sha256,
      target_fingerprint_root_sha256, semantic_sha256, request_started_at,
      captured_max_at, provider_observed_max_at, published_at, status,
      error_message, created_at
    ) VALUES (
      :run_id, :schema_version, :collector_build_sha, :capture_kind, :target_date,
      :window_start_date, :window_end_date, :decision_at,
      :provider, :api_path, :transport_contract, :resolved_endpoint,
      :source_field, :unit, :match_policy, :promotion_mode,
      :promotion_table, :promotion_column, :k_type, :adjust_type,
      :subject_kind, :subject_identity, :subject_sha256,
      :authority_proof_kind, :authority_proof_identity,
      :authority_proof_sha256, :authority_set_sha256,
      :expected_count, :fetched_count, :valid_count, :matched_count, :promoted_count,
      :expected_keyset_sha256, :provider_response_payload,
      :provider_response_sha256, :collector_binary_sha256,
      :raw_payload_root_sha256,
      :field_value_root_sha256,
      :target_fingerprint_root_sha256, :semantic_sha256, :request_started_at,
      :captured_max_at, :provider_observed_max_at, :published_at, :status,
      :error_message, :created_at
    )
"""


def _row_insert_params(run: TurnoverCaptureRun, row: CapturedTurnoverRow, *, published_at: datetime) -> dict[str, Any]:
    target = row.target
    return {
        "run_id": run.run_id,
        "stock_code": target.stock_code,
        "trade_date": target.trade_date.isoformat(),
        "k_type": target.k_type,
        "adjust_type": target.adjust_type,
        "target_row_id": target.target_row_id,
        "field_value_decimal": _decimal_text(row.turnover_percent),
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
        "target_prewrite_sha256": target.prewrite_sha256,
        "target_fact_sha256": target.ohlcv_sha256,
        "validation_status": "MATCHED",
        "validation_error": "",
        "promoted_at": _datetime_text(published_at),
    }


_ROW_COLUMNS = (
    "run_id", "stock_code", "trade_date", "k_type", "adjust_type",
    "target_row_id", "field_value_decimal", "source_open", "source_high",
    "source_low", "source_close", "source_volume_shares", "source_amount",
    "raw_row_text", "raw_payload", "raw_payload_sha256", "snapshot_row_sha256",
    "captured_at", "provider_observed_at_text", "provider_observed_at", "qmt_open",
    "qmt_high", "qmt_low", "qmt_close", "qmt_volume_shares", "qmt_amount",
    "qmt_received_at", "qmt_data_source", "qmt_batch_id", "qmt_data_version",
    "qmt_quality_status", "qmt_permission_status", "target_prewrite_sha256",
    "target_fact_sha256", "validation_status", "validation_error", "promoted_at",
)
_ROW_INSERT_SQL = (
    f"INSERT INTO {TURNOVER_SNAPSHOT_ROW_TABLE} ("
    + ", ".join(_ROW_COLUMNS)
    + ") VALUES ("
    + ", ".join(f":{column}" for column in _ROW_COLUMNS)
    + ")"
)


def _reconstruct_persisted_capture(
    row: Mapping[str, Any],
    *,
    decision_at: datetime,
) -> CapturedTurnoverRow:
    raw_payload = row.get("raw_payload")
    if isinstance(raw_payload, memoryview):
        raw_payload = raw_payload.tobytes()
    if not isinstance(raw_payload, bytes):
        raise _blocked("persisted turnover raw payload is not binary")
    target = _qmt_target_from_row(
        {
            "id": row.get("target_row_id"),
            "stock_code": row.get("stock_code"),
            "trade_date": row.get("trade_date"),
            "k_type": row.get("k_type"),
            "adjust_type": row.get("adjust_type"),
            "open": row.get("qmt_open"),
            "high": row.get("qmt_high"),
            "low": row.get("qmt_low"),
            "close": row.get("qmt_close"),
            "volume": row.get("qmt_volume_shares"),
            "amount": row.get("qmt_amount"),
            "turnover_ratio": None,
            "received_at": row.get("qmt_received_at"),
            "data_source": row.get("qmt_data_source"),
            "batch_id": row.get("qmt_batch_id"),
            "data_version": row.get("qmt_data_version"),
            "quality_status": row.get("qmt_quality_status"),
            "permission_status": row.get("qmt_permission_status"),
        },
        target_date=_exact_date(row.get("trade_date"), field="snapshot trade_date"),
        decision_at=decision_at,
    )
    reconstructed = parse_eastmoney_turnover_response(
        target=target,
        raw_payload=raw_payload,
        provider_http_date=str(row.get("provider_observed_at_text") or ""),
        captured_at=row.get("captured_at"),
        decision_at=decision_at,
    )
    persisted_turnover = _canonical_decimal(
        row.get("field_value_decimal"), field="persisted field_value_decimal"
    )
    if (
        reconstructed.turnover_percent != persisted_turnover
        or reconstructed.source_open
        != _canonical_decimal(row.get("source_open"), field="persisted source_open")
        or reconstructed.source_high
        != _canonical_decimal(row.get("source_high"), field="persisted source_high")
        or reconstructed.source_low
        != _canonical_decimal(row.get("source_low"), field="persisted source_low")
        or reconstructed.source_close
        != _canonical_decimal(row.get("source_close"), field="persisted source_close")
        or reconstructed.source_volume_shares
        != _canonical_decimal(
            row.get("source_volume_shares"), field="persisted source_volume_shares"
        )
        or reconstructed.source_amount
        != _canonical_decimal(row.get("source_amount"), field="persisted source_amount")
        or reconstructed.provider_http_at
        != _local_datetime(
            row.get("provider_observed_at"), field="persisted provider_observed_at"
        )
        or _local_datetime(row.get("promoted_at"), field="persisted promoted_at")
        > decision_at
        or reconstructed.raw_row_text != str(row.get("raw_row_text") or "")
        or reconstructed.raw_payload_sha256
        != str(row.get("raw_payload_sha256") or "")
        or reconstructed.snapshot_row_sha256
        != str(row.get("snapshot_row_sha256") or "")
        or reconstructed.target.prewrite_sha256
        != str(row.get("target_prewrite_sha256") or "")
        or reconstructed.target.ohlcv_sha256
        != str(row.get("target_fact_sha256") or "")
        or str(row.get("validation_status") or "") != "MATCHED"
        or str(row.get("validation_error") or "")
    ):
        raise _blocked(f"persisted turnover row semantics differ for {target.stock_code}")
    return reconstructed


def _assert_capture_matches_targets(run: TurnoverCaptureRun, targets: Sequence[QmtTurnoverTarget]) -> None:
    if (
        expected_universe_sha256(targets) != run.expected_universe_sha256
        or qmt_fingerprint_root_sha256(targets) != run.qmt_fingerprint_root_sha256
        or len(targets) != len(run.rows)
    ):
        raise _blocked("QMT target universe changed before turnover publication")
    for target, captured in zip(targets, run.rows):
        if target != captured.target:
            raise _blocked(f"QMT target changed before publication: {target.stock_code}")


def _verify_stage_readback(
    connection,
    run: TurnoverCaptureRun,
    *,
    expected_status: str,
) -> None:
    run_row = connection.execute(
        text(f"SELECT * FROM {TURNOVER_SNAPSHOT_RUN_TABLE} WHERE run_id=:run_id"),
        {"run_id": run.run_id},
    ).mappings().all()
    if len(run_row) != 1:
        raise _blocked("turnover run readback identity differs")
    persisted = dict(run_row[0])
    persisted_response = persisted.get("provider_response_payload")
    if isinstance(persisted_response, memoryview):
        persisted_response = persisted_response.tobytes()
    count = len(run.rows)
    if (
        str(persisted.get("status") or "") != expected_status
        or str(persisted.get("schema_version") or "")
        != TURNOVER_SNAPSHOT_VERSION
        or str(persisted.get("collector_build_sha") or "")
        != run.collector_build_sha
        or str(persisted.get("collector_binary_sha256") or "")
        != run.collector_binary_sha256
        or str(persisted.get("capture_kind") or "") != TURNOVER_CAPTURE_KIND
        or _exact_date(persisted.get("target_date"), field="target_date")
        != run.target_date
        or _local_datetime(persisted.get("decision_at"), field="decision_at")
        != run.decision_at
        or str(persisted.get("provider") or "") != TURNOVER_PROVIDER
        or str(persisted.get("api_path") or "") != TURNOVER_API_PATH
        or str(persisted.get("transport_contract") or "")
        != run.transport_contract
        or str(persisted.get("resolved_endpoint") or "")
        != run.resolved_endpoint
        or str(persisted.get("source_field") or "")
        != TURNOVER_SOURCE_FIELD
        or str(persisted.get("unit") or "") != TURNOVER_UNIT
        or str(persisted.get("match_policy") or "")
        != TURNOVER_MATCH_POLICY
        or str(persisted.get("promotion_mode") or "")
        != TURNOVER_PROMOTION_MODE
        or any(int(persisted.get(name) or -1) != count for name in (
            "expected_count", "fetched_count", "valid_count", "matched_count", "promoted_count"
        ))
        or str(persisted.get("expected_keyset_sha256") or "") != run.expected_universe_sha256
        or str(persisted.get("subject_kind") or "") != "QMT_TARGET_UNIVERSE"
        or str(persisted.get("subject_identity") or "") != run.target_date.isoformat()
        or str(persisted.get("subject_sha256") or "") != run.expected_universe_sha256
        or str(persisted.get("authority_proof_kind") or "")
        != "QMT_DAILY_MARKET_TRUTH"
        or str(persisted.get("authority_proof_identity") or "")
        != run.authority_run_id
        or str(persisted.get("authority_proof_sha256") or "")
        != run.authority_truth_sha256
        or str(persisted.get("authority_set_sha256") or "")
        != run.authority_stock_set_sha256
        or not isinstance(persisted_response, bytes)
        or persisted_response != run.provider_response_payload
        or _sha256(persisted_response) != run.provider_response_sha256
        or str(persisted.get("provider_response_sha256") or "")
        != run.provider_response_sha256
        or str(persisted.get("raw_payload_root_sha256") or "") != run.raw_payload_root_sha256
        or str(persisted.get("field_value_root_sha256") or "") != run.field_value_root_sha256
        or str(persisted.get("target_fingerprint_root_sha256") or "") != run.qmt_fingerprint_root_sha256
        or str(persisted.get("semantic_sha256") or "") != run.semantic_sha256
    ):
        raise _blocked("turnover run readback contract differs")
    rows = connection.execute(
        text(
            f"SELECT * FROM {TURNOVER_SNAPSHOT_ROW_TABLE} "
            "WHERE run_id=:run_id ORDER BY stock_code"
        ),
        {"run_id": run.run_id},
    ).mappings().all()
    if len(rows) != count:
        raise _blocked("turnover row readback count differs")
    by_code = {item.target.stock_code: item for item in run.rows}
    for persisted_row in rows:
        code = str(persisted_row.get("stock_code") or "")
        expected = by_code.get(code)
        reconstructed = _reconstruct_persisted_capture(
            persisted_row,
            decision_at=run.decision_at,
        )
        if (
            expected is None
            or reconstructed != expected
        ):
            raise _blocked(f"turnover row readback differs for {code}")


def _verify_kline_promotion_readback(connection, run: TurnoverCaptureRun) -> None:
    rows = connection.execute(
        text(_QMT_TARGET_SQL), {"target_date": run.target_date.isoformat()}
    ).fetchall()
    by_code: dict[str, dict[str, Any]] = {}
    for row in rows:
        mapped = _mapping(row)
        code = _stock_code(mapped["stock_code"])
        if code in by_code:
            raise _blocked("promoted QMT target identities contain duplicates")
        by_code[code] = mapped
    if set(by_code) != {item.target.stock_code for item in run.rows}:
        raise _blocked("promoted QMT target identities differ")
    for item in run.rows:
        current = by_code[item.target.stock_code]
        current_turnover = _canonical_decimal(
            current.get("turnover_ratio"), field="promoted turnover_ratio"
        )
        expected_payload = _qmt_ohlcv_payload(current)
        if (
            current_turnover != item.turnover_percent
            or _sha256(expected_payload) != item.target.ohlcv_sha256
        ):
            raise _blocked(f"NULL-only turnover promotion readback differs for {item.target.stock_code}")


_MYSQL_TURNOVER_PROMOTION_SQL = f"""
    UPDATE sm_stock_kline AS target
    INNER JOIN {TURNOVER_SNAPSHOT_ROW_TABLE} AS captured
      ON captured.run_id=:run_id
     AND captured.target_row_id=target.id
     AND captured.stock_code=target.stock_code
     AND captured.trade_date=target.trade_date
     AND captured.k_type=target.k_type
     AND captured.adjust_type=target.adjust_type
    SET target.turnover_ratio=captured.field_value_decimal
    WHERE target.trade_date=:target_date
      AND target.k_type=1
      AND target.adjust_type=0
      AND target.turnover_ratio IS NULL
      AND captured.validation_status='MATCHED'
"""

_SQLITE_TURNOVER_PROMOTION_SQL = f"""
    UPDATE sm_stock_kline
    SET turnover_ratio=(
      SELECT captured.field_value_decimal
      FROM {TURNOVER_SNAPSHOT_ROW_TABLE} AS captured
      WHERE captured.run_id=:run_id
        AND captured.target_row_id=sm_stock_kline.id
        AND captured.stock_code=sm_stock_kline.stock_code
        AND captured.trade_date=sm_stock_kline.trade_date
        AND captured.k_type=sm_stock_kline.k_type
        AND captured.adjust_type=sm_stock_kline.adjust_type
        AND captured.validation_status='MATCHED'
    )
    WHERE trade_date=:target_date
      AND k_type=1
      AND adjust_type=0
      AND turnover_ratio IS NULL
      AND EXISTS (
        SELECT 1
        FROM {TURNOVER_SNAPSHOT_ROW_TABLE} AS captured
        WHERE captured.run_id=:run_id
          AND captured.target_row_id=sm_stock_kline.id
          AND captured.stock_code=sm_stock_kline.stock_code
          AND captured.trade_date=sm_stock_kline.trade_date
          AND captured.k_type=sm_stock_kline.k_type
          AND captured.adjust_type=sm_stock_kline.adjust_type
          AND captured.validation_status='MATCHED'
      )
"""


def _promote_turnover_rows(connection, run: TurnoverCaptureRun) -> None:
    """Promote the sealed full-market run with one NULL-only set update."""

    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    statement = (
        _MYSQL_TURNOVER_PROMOTION_SQL
        if dialect == "mysql"
        else _SQLITE_TURNOVER_PROMOTION_SQL
    )
    update = connection.execute(text(statement), {
        "run_id": run.run_id,
        "target_date": run.target_date.isoformat(),
    })
    updated_count = int(getattr(update, "rowcount", -1))
    if updated_count < 0:
        raise _blocked("NULL-only turnover promotion rowcount unavailable")
    if updated_count != len(run.rows):
        raise _blocked(
            f"NULL-only turnover promotion changed {updated_count} "
            f"rows, expected {len(run.rows)}"
        )


def _authority_from_run(run: TurnoverCaptureRun) -> TurnoverUniverseAuthority:
    authority = TurnoverUniverseAuthority(
        target_date=run.target_date,
        decision_at=run.decision_at,
        truth_run_id=run.authority_run_id,
        truth_sha256=run.authority_truth_sha256,
        stock_set_sha256=run.authority_stock_set_sha256,
        expected_codes=tuple(item.target.stock_code for item in run.rows),
    )
    _validate_universe_authority(
        authority,
        target_date=run.target_date,
        decision_at=run.decision_at,
        codes=authority.expected_codes,
    )
    return authority


def _turnover_receipt(run: TurnoverCaptureRun) -> dict[str, Any]:
    return {
        "schema": TURNOVER_SNAPSHOT_VERSION,
        "status": "COMPLETED",
        "run_id": run.run_id,
        "target_date": run.target_date.isoformat(),
        "decision_at": run.decision_at.isoformat(timespec="seconds"),
        "expected_count": len(run.rows),
        "promoted_count": len(run.rows),
        "expected_keyset_sha256": run.expected_universe_sha256,
        "target_fingerprint_root_sha256": run.qmt_fingerprint_root_sha256,
        "raw_payload_root_sha256": run.raw_payload_root_sha256,
        "field_value_root_sha256": run.field_value_root_sha256,
        "semantic_sha256": run.semantic_sha256,
        "authority_proof_identity": run.authority_run_id,
        "authority_proof_sha256": run.authority_truth_sha256,
        "authority_set_sha256": run.authority_stock_set_sha256,
        "collector_build_sha": run.collector_build_sha,
        "collector_binary_sha256": run.collector_binary_sha256,
    }


@contextmanager
def _publication_connection(engine):
    connection = engine.connect()
    mysql = str(getattr(getattr(engine, "dialect", None), "name", "")).lower() == "mysql"
    try:
        if mysql:
            with mysql_named_lock(
                engine,
                STOCK_KLINE_FREEZE_LOCK_NAME,
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


def publish_turnover_snapshot(
    engine,
    run: TurnoverCaptureRun,
    *,
    min_expected_count: int = MIN_TURNOVER_UNIVERSE_COUNT,
    published_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Atomically append evidence and fill only NULL target turnover values."""

    published = _local_datetime(
        published_at or _now_shanghai(), field="published_at"
    )
    if published < run.captured_max_at or published > run.decision_at:
        raise _blocked("turnover publication timestamp is outside the decision window")
    with _publication_connection(engine) as connection:
        authority = _authority_from_run(run)
        if str(getattr(connection.dialect, "name", "")).lower() == "mysql":
            live_authority = load_turnover_universe_authority(
                connection,
                target_date=run.target_date,
                decision_at=run.decision_at,
            )
            if live_authority != authority:
                raise _blocked("turnover universe authority changed before publication")
        existing = connection.execute(text(f"""
            SELECT run_id, status
            FROM {TURNOVER_SNAPSHOT_RUN_TABLE}
            WHERE capture_kind=:capture_kind
              AND target_date=:target_date
              AND subject_sha256=:subject_sha256
              AND decision_at=:decision_at
        """), {
            "capture_kind": TURNOVER_CAPTURE_KIND,
            "target_date": run.target_date.isoformat(),
            "subject_sha256": run.expected_universe_sha256,
            "decision_at": _datetime_text(run.decision_at),
        }).mappings().all()
        if existing:
            if len(existing) != 1 or str(existing[0].get("status") or "") != "COMPLETED":
                raise _blocked("turnover logical publication is not terminal")
            recovered = replace(run, run_id=str(existing[0]["run_id"]))
            _verify_stage_readback(
                connection, recovered, expected_status="COMPLETED"
            )
            _verify_kline_promotion_readback(connection, recovered)
            return _turnover_receipt(recovered)
        targets = freeze_qmt_turnover_targets(
            connection,
            target_date=run.target_date,
            decision_at=run.decision_at,
            authority=authority,
            min_expected_count=min_expected_count,
            for_update=True,
        )
        _assert_capture_matches_targets(run, targets)
        connection.execute(text(_RUN_INSERT_SQL), _run_insert_params(run, published_at=published))
        row_params = [
            _row_insert_params(run, row, published_at=published)
            for row in run.rows
        ]
        for batch in _chunks(row_params, 250):
            connection.execute(text(_ROW_INSERT_SQL), list(batch))
        _verify_stage_readback(connection, run, expected_status="BUILDING")
        _promote_turnover_rows(connection, run)
        _verify_kline_promotion_readback(connection, run)
        terminal = connection.execute(
            text(
                f"UPDATE {TURNOVER_SNAPSHOT_RUN_TABLE} SET status='COMPLETED' "
                "WHERE run_id=:run_id AND status='BUILDING'"
            ),
            {"run_id": run.run_id},
        )
        if int(getattr(terminal, "rowcount", -1)) != 1:
            raise _blocked("turnover run terminal transition was not exact")
        _verify_stage_readback(connection, run, expected_status="COMPLETED")
    return _turnover_receipt(run)


def _proof_from_persisted(run: Mapping[str, Any], row: Mapping[str, Any], *, decision_at: datetime) -> str:
    return build_turnover_evidence({
        "status": "PASS",
        "stock_code": _stock_code(row["stock_code"]),
        "trade_date": _exact_date(row["trade_date"], field="snapshot trade_date").isoformat(),
        "decision_known_at": _datetime_text(decision_at),
        "source_table": TURNOVER_SNAPSHOT_ROW_TABLE,
        "formula": TURNOVER_DIRECT_FORMULA,
        "volume": _decimal_text(row["source_volume_shares"]),
        "turnover_ratio": format(
            _canonical_decimal(row["field_value_decimal"], field="snapshot turnover").quantize(Decimal("0.01")),
            ".2f",
        ),
        "provider": str(run["provider"]),
        "transport_contract": str(run["transport_contract"]),
        "resolved_endpoint": str(run["resolved_endpoint"]),
        "source_field": str(run["source_field"]),
        "unit": str(run["unit"]),
        "source_trade_date": _exact_date(row["trade_date"], field="source trade_date").isoformat(),
        "captured_at": _datetime_text(_local_datetime(row["captured_at"], field="captured_at")),
        "provider_http_date": str(row["provider_observed_at_text"]),
        "snapshot_run_id": str(run["run_id"]),
        "collector_build_sha": str(run["collector_build_sha"]),
        "collector_binary_sha256": str(run["collector_binary_sha256"]),
        "authority_proof_kind": str(run["authority_proof_kind"]),
        "authority_proof_identity": str(run["authority_proof_identity"]),
        "authority_proof_sha256": str(run["authority_proof_sha256"]),
        "authority_set_sha256": str(run["authority_set_sha256"]),
        "raw_payload_sha256": str(row["raw_payload_sha256"]),
        "snapshot_row_sha256": str(row["snapshot_row_sha256"]),
        "snapshot_semantic_sha256": str(run["semantic_sha256"]),
        "source_open": _decimal_text(row["source_open"]),
        "source_high": _decimal_text(row["source_high"]),
        "source_low": _decimal_text(row["source_low"]),
        "source_close": _decimal_text(row["source_close"]),
        "source_volume_shares": _decimal_text(row["source_volume_shares"]),
        "qmt_open": _decimal_text(row["qmt_open"]),
        "qmt_high": _decimal_text(row["qmt_high"]),
        "qmt_low": _decimal_text(row["qmt_low"]),
        "qmt_close": _decimal_text(row["qmt_close"]),
        "qmt_volume_shares": _decimal_text(row["qmt_volume_shares"]),
        "qmt_received_at": _datetime_text(_local_datetime(row["qmt_received_at"], field="qmt_received_at")),
        "qmt_data_source": str(row["qmt_data_source"]),
        "qmt_batch_id": str(row["qmt_batch_id"]),
        "qmt_data_version": str(row["qmt_data_version"]),
        "qmt_quality_status": str(row["qmt_quality_status"]),
        "qmt_permission_status": str(row["qmt_permission_status"]),
    })


def load_verified_turnover_evidence(
    engine,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    min_expected_count: int = MIN_TURNOVER_UNIVERSE_COUNT,
) -> dict[str, dict[str, Any]]:
    """Read one completed immutable run and revalidate it against live QMT rows."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    with engine.connect() as connection:
        candidates = connection.execute(text(f"""
            SELECT * FROM {TURNOVER_SNAPSHOT_RUN_TABLE}
            WHERE target_date=:target_date AND decision_at<=:decision_at
              AND status='COMPLETED' AND provider=:provider
              AND source_field=:source_field AND unit=:unit
              AND expected_count=fetched_count
              AND expected_count=valid_count
              AND expected_count=matched_count
              AND expected_count=promoted_count
            ORDER BY decision_at DESC, published_at DESC, run_id DESC
        """), {
            "target_date": target.isoformat(),
            "decision_at": _datetime_text(cutoff),
            "provider": TURNOVER_PROVIDER,
            "source_field": TURNOVER_SOURCE_FIELD,
            "unit": TURNOVER_UNIT,
        }).mappings().all()
        if not candidates:
            return {}
        replay_roots = {
            (
                str(item.get("subject_sha256") or ""),
                str(item.get("expected_keyset_sha256") or ""),
                str(item.get("target_fingerprint_root_sha256") or ""),
                str(item.get("field_value_root_sha256") or ""),
            )
            for item in candidates
        }
        if len(replay_roots) != 1:
            raise _blocked(
                "completed turnover snapshot replays disagree on exact values"
            )
        run = dict(candidates[0])
        run_cutoff = _local_datetime(run["decision_at"], field="snapshot decision_at")
        run_started = _local_datetime(
            run.get("request_started_at"), field="snapshot request_started_at"
        )
        run_captured = _local_datetime(
            run.get("captured_max_at"), field="snapshot captured_max_at"
        )
        run_observed = _local_datetime(
            run.get("provider_observed_max_at"),
            field="snapshot provider_observed_max_at",
        )
        run_published = _local_datetime(
            run.get("published_at"), field="snapshot published_at"
        )
        provider_response = run.get("provider_response_payload")
        if isinstance(provider_response, memoryview):
            provider_response = provider_response.tobytes()
        if (
            run_cutoff > cutoff
            or not (
                run_started <= run_captured <= run_published <= run_cutoff
                and run_observed <= run_cutoff
            )
            or str(run.get("schema_version") or "") != TURNOVER_SNAPSHOT_VERSION
            or _SHA40.fullmatch(str(run.get("collector_build_sha") or "")) is None
            or str(run.get("collector_build_sha") or "") == "0" * 40
            or _SHA64.fullmatch(
                str(run.get("collector_binary_sha256") or "")
            ) is None
            or str(run.get("collector_binary_sha256") or "") == "0" * 64
            or str(run.get("capture_kind") or "") != TURNOVER_CAPTURE_KIND
            or str(run.get("api_path") or "") != TURNOVER_API_PATH
            or str(run.get("transport_contract") or "") not in {
                TURNOVER_DIRECT_TLS_TRANSPORT,
                TURNOVER_PINNED_TLS_TRANSPORT,
            }
            or not str(run.get("resolved_endpoint") or "").strip()
            or str(run.get("match_policy") or "") != TURNOVER_MATCH_POLICY
            or str(run.get("promotion_mode") or "") != TURNOVER_PROMOTION_MODE
            or str(run.get("promotion_table") or "") != "sm_stock_kline"
            or str(run.get("promotion_column") or "") != "turnover_ratio"
            or not isinstance(provider_response, bytes)
            or _sha256(provider_response)
            != str(run.get("provider_response_sha256") or "")
            or _exact_date(run.get("window_start_date"), field="window_start_date")
            != target
            or _exact_date(run.get("window_end_date"), field="window_end_date")
            != target
            or int(run.get("k_type") or -1) != 1
            or int(
                run.get("adjust_type")
                if run.get("adjust_type") is not None
                else -1
            ) != 0
            or str(run.get("subject_kind") or "") != "QMT_TARGET_UNIVERSE"
            or str(run.get("subject_identity") or "") != target.isoformat()
            or str(run.get("subject_sha256") or "")
            != str(run.get("expected_keyset_sha256") or "")
            or str(run.get("authority_proof_kind") or "")
            != "QMT_DAILY_MARKET_TRUTH"
            or not str(run.get("authority_proof_identity") or "").strip()
            or len(str(run.get("authority_proof_identity") or "")) > 128
            or _SHA64.fullmatch(
                str(run.get("authority_proof_sha256") or "")
            ) is None
            or str(run.get("authority_proof_sha256") or "") == "0" * 64
            or _SHA64.fullmatch(
                str(run.get("authority_set_sha256") or "")
            ) is None
            or str(run.get("authority_set_sha256") or "") == "0" * 64
        ):
            raise _blocked("turnover snapshot decision cutoff is in the future")
        live_authority: TurnoverUniverseAuthority | None = None
        if str(getattr(getattr(engine, "dialect", None), "name", "")).lower() == "mysql":
            live_authority = _revalidate_replayable_turnover_authority(
                connection,
                target_date=target,
                decision_at=run_cutoff,
            )
            if live_authority is not None and (
                live_authority.truth_run_id
                != str(run.get("authority_proof_identity") or "")
                or live_authority.truth_sha256
                != str(run.get("authority_proof_sha256") or "")
                or live_authority.stock_set_sha256
                != str(run.get("authority_set_sha256") or "")
            ):
                raise _blocked("turnover universe authority readback differs")
        rows = [dict(row) for row in connection.execute(text(
            f"SELECT * FROM {TURNOVER_SNAPSHOT_ROW_TABLE} "
            "WHERE run_id=:run_id ORDER BY stock_code"
        ), {"run_id": run["run_id"]}).mappings().all()]
        expected_count = int(run["expected_count"])
        if len(rows) != expected_count or expected_count < max(1, int(min_expected_count)):
            raise _blocked("persisted turnover snapshot coverage differs")
        raw_root_items: list[dict[str, str]] = []
        semantic_items: list[dict[str, str]] = []
        field_value_items: list[dict[str, str]] = []
        qmt_root_items: list[dict[str, str]] = []
        universe_items: list[dict[str, Any]] = []
        codes: set[str] = set()
        reconstructed_by_code: dict[str, CapturedTurnoverRow] = {}
        for row in rows:
            code = _stock_code(row["stock_code"])
            if code in codes:
                raise _blocked("persisted turnover snapshot contains duplicates")
            codes.add(code)
            reconstructed = _reconstruct_persisted_capture(
                row,
                decision_at=run_cutoff,
            )
            reconstructed_by_code[code] = reconstructed
            raw_root_items.append({"stock_code": code, "raw_payload_sha256": str(row["raw_payload_sha256"])})
            semantic_items.append({"stock_code": code, "snapshot_row_sha256": str(row["snapshot_row_sha256"])})
            field_value_items.append({
                "stock_code": code,
                "trade_date": _exact_date(
                    row["trade_date"], field="snapshot trade_date"
                ).isoformat(),
                "turnover_percent": _decimal_text(
                    _canonical_decimal(
                        row["field_value_decimal"], field="snapshot turnover"
                    )
                ),
            })
            qmt_root_items.append({"stock_code": code, "qmt_prewrite_sha256": str(row["target_prewrite_sha256"])})
            universe_items.append({
                "stock_code": code,
                "trade_date": _exact_date(row["trade_date"], field="snapshot trade_date").isoformat(),
                "k_type": int(row["k_type"]),
                "adjust_type": int(row["adjust_type"]),
                "target_row_id": int(row["target_row_id"]),
            })
            captured = reconstructed.captured_at
            received = reconstructed.target.received_at
            if captured > run_cutoff or received > run_cutoff:
                raise _blocked(f"persisted turnover snapshot crosses cutoff for {code}")
        if (
            _sha256(raw_root_items) != str(run["raw_payload_root_sha256"])
            or _sha256(field_value_items)
            != str(run["field_value_root_sha256"])
            or _sha256(semantic_items) != str(run["semantic_sha256"])
            or _sha256(qmt_root_items) != str(run["target_fingerprint_root_sha256"])
            or _sha256(universe_items) != str(run["expected_keyset_sha256"])
            or str(
                expected_stock_set_contract(
                    target.isoformat(), sorted(codes)
                )["stock_set_hash"]
            )
            != str(run.get("authority_set_sha256") or "")
            or (
                live_authority is not None
                and tuple(sorted(codes)) != live_authority.expected_codes
            )
        ):
            raise _blocked("persisted turnover snapshot root hash differs")
        current = connection.execute(
            text(_QMT_TARGET_SQL), {"target_date": target.isoformat()}
        ).mappings().all()
        current_by_code: dict[str, dict[str, Any]] = {}
        for current_row in current:
            code = _stock_code(current_row["stock_code"])
            if code in current_by_code:
                raise _blocked("live QMT turnover universe contains duplicates")
            current_by_code[code] = dict(current_row)
        if set(current_by_code) != codes:
            raise _blocked("persisted turnover snapshot no longer covers exact QMT universe")
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            code = _stock_code(row["stock_code"])
            live = current_by_code[code]
            reconstructed = reconstructed_by_code[code]
            if not _historical_qmt_row_matches_capture(live, reconstructed):
                raise _blocked(f"persisted turnover/QMT readback differs for {code}")
            result[code] = {
                "turnover_ratio": _canonical_decimal(row["field_value_decimal"], field="snapshot turnover"),
                "turnover_evidence_json": _proof_from_persisted(run, row, decision_at=cutoff),
            }
        return result


def recover_completed_turnover_receipt(
    engine,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    collector_build_sha: str,
    collector_binary_sha256: str,
    authority: TurnoverUniverseAuthority,
    min_expected_count: int = MIN_TURNOVER_UNIVERSE_COUNT,
) -> dict[str, Any] | None:
    """Reuse verified data, retaining its original collector identity."""

    target = target_date if isinstance(target_date, date) else _exact_date(
        target_date, field="target_date"
    )
    cutoff = _local_datetime(decision_at, field="decision_at")
    build_sha = str(collector_build_sha or "").strip().lower()
    binary_sha = str(collector_binary_sha256 or "").strip().lower()
    _validate_universe_authority(
        authority,
        target_date=target,
        decision_at=cutoff,
        codes=authority.expected_codes,
    )
    evidence = load_verified_turnover_evidence(
        engine, target_date=target, decision_at=cutoff,
        min_expected_count=min_expected_count,
    )
    if not evidence:
        return None
    if set(evidence) != set(authority.expected_codes):
        raise _blocked("completed turnover publication recovery coverage differs")
    proof_run_ids = {
        str(json.loads(item["turnover_evidence_json"])["snapshot_run_id"])
        for item in evidence.values()
    }
    if len(proof_run_ids) != 1:
        raise _blocked("completed turnover publication recovery identity differs")
    with engine.connect() as connection:
        rows = connection.execute(text(f"""
            SELECT * FROM {TURNOVER_SNAPSHOT_RUN_TABLE}
            WHERE target_date=:target_date
              AND decision_at<=:decision_at
              AND status='COMPLETED'
              AND capture_kind=:capture_kind
              AND run_id=:run_id
              AND authority_proof_kind='QMT_DAILY_MARKET_TRUTH'
              AND authority_set_sha256=:authority_set_sha256
        """), {
            "target_date": target.isoformat(),
            "decision_at": _datetime_text(cutoff),
            "capture_kind": TURNOVER_CAPTURE_KIND,
            "run_id": next(iter(proof_run_ids)),
            "authority_set_sha256": authority.stock_set_sha256,
        }).mappings().all()
    if not rows:
        return None
    if len(rows) != 1:
        raise _blocked("completed turnover publication recovery is ambiguous")
    run = dict(rows[0])
    return {
        "schema": TURNOVER_SNAPSHOT_VERSION,
        "status": "COMPLETED",
        "run_id": str(run["run_id"]),
        "target_date": target.isoformat(),
        "decision_at": cutoff.isoformat(timespec="seconds"),
        "expected_count": int(run["expected_count"]),
        "promoted_count": int(run["promoted_count"]),
        "expected_keyset_sha256": str(run["expected_keyset_sha256"]),
        "target_fingerprint_root_sha256": str(
            run["target_fingerprint_root_sha256"]
        ),
        "raw_payload_root_sha256": str(run["raw_payload_root_sha256"]),
        "field_value_root_sha256": str(run["field_value_root_sha256"]),
        "semantic_sha256": str(run["semantic_sha256"]),
        "authority_proof_identity": str(run["authority_proof_identity"]),
        "authority_proof_sha256": str(run["authority_proof_sha256"]),
        "authority_set_sha256": str(run["authority_set_sha256"]),
        "collector_build_sha": str(run["collector_build_sha"]),
        "collector_binary_sha256": str(run["collector_binary_sha256"]),
        "validated_by_build_sha": build_sha,
        "validated_by_binary_sha256": binary_sha,
        "source_decision_at": _datetime_text(run["decision_at"]),
        "recovered": True,
    }


__all__ = [
    "CapturedTurnoverRow",
    "DEFAULT_EASTMONEY_HOSTS",
    "EastmoneyTurnoverCollector",
    "PinnedCurlEastmoneyTurnoverCollector",
    "DEFAULT_PUSH2HIS_RESOLVE_IP",
    "MIN_TURNOVER_UNIVERSE_COUNT",
    "QmtTurnoverTarget",
    "TURNOVER_SNAPSHOT_VERSION",
    "TURNOVER_DIRECT_TLS_TRANSPORT",
    "TURNOVER_PINNED_TLS_TRANSPORT",
    "TurnoverCaptureRun",
    "TurnoverSnapshotBlocked",
    "TurnoverTransportError",
    "TurnoverUniverseAuthority",
    "build_capture_run",
    "collect_turnover_snapshot",
    "expected_universe_sha256",
    "freeze_qmt_turnover_targets",
    "load_turnover_universe_authority",
    "load_verified_turnover_evidence",
    "parse_eastmoney_turnover_response",
    "publish_turnover_snapshot",
    "qmt_fingerprint_root_sha256",
    "recover_completed_turnover_receipt",
    "restore_turnover_checkpoint_row",
    "serialize_turnover_checkpoint_row",
    "turnover_capture_input_sha256",
]
