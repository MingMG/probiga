"""Verified 21-session upper-limit history without a live QMT dependency."""
from __future__ import annotations

import hashlib
import json
import os
import platform
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from server.common.analysis_pool_receipt import (
    build_upper_limit_evidence,
    validate_upper_limit_evidence,
)
from server.common.kline_data import get_kline_engine
from server.common.qmt_trade_calendar import load_trade_calendar_receipt


EASTMONEY_HISTORY_PROVIDER = "eastmoney.push2ex.zt_pool+cn_exchange_limit_rule.v1"
EASTMONEY_HISTORY_SOURCE_TABLE = "sm_stock_kline+eastmoney.push2ex.zt_pool"
EASTMONEY_HISTORY_TRANSPORT = "HTTPS_TLS_VERIFIED_DIRECT_V1"
EASTMONEY_HISTORY_API_PATH = "/getTopicZTPool"
EASTMONEY_HISTORY_URL = "https://push2ex.eastmoney.com/getTopicZTPool"
EASTMONEY_HISTORY_UT = "7eea3edcaed734bea9cbfc24409ed989"
EASTMONEY_HISTORY_CACHE_SCHEMA = "probiga.eastmoney-upper-limit-history-cache.v1"
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class EastmoneyUpperLimitHistoryBlocked(RuntimeError):
    pass


def _blocked(message: str) -> EastmoneyUpperLimitHistoryBlocked:
    return EastmoneyUpperLimitHistoryBlocked(f"DATA_BLOCKED: {message}")


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


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise _blocked("stored QMT price is not decimal") from exc
    if not result.is_finite():
        raise _blocked("stored QMT price is not finite")
    return result


def _limit_ratio(stock_code: str, short_name: str) -> Decimal:
    if "ST" in str(short_name or "").upper():
        raise _blocked(f"historical ST rule is not proven for {stock_code}")
    if stock_code.startswith(("4", "8", "92")):
        return Decimal("0.30")
    if stock_code.startswith(("300", "301", "688", "689")):
        return Decimal("0.20")
    return Decimal("0.10")


def _pool_codes_from_rows(pool: Sequence[Any], target: date) -> list[str]:
    rows_by_code: dict[str, str] = {}
    for row in pool:
        code = str((row or {}).get("c") or "").strip().zfill(6)
        if len(code) != 6 or not code.isdigit():
            raise _blocked(f"Eastmoney limit pool code is invalid for {target}")
        canonical_row = _canonical_json(row)
        existing = rows_by_code.get(code)
        if existing is not None and existing != canonical_row:
            raise _blocked(
                f"Eastmoney limit pool has conflicting duplicates for {target}"
            )
        rows_by_code[code] = canonical_row
    return sorted(rows_by_code)


def _fetch_pool(target: date, timeout_seconds: float = 20.0) -> tuple[list[str], str]:
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        EASTMONEY_HISTORY_URL,
        params={
            "ut": EASTMONEY_HISTORY_UT,
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "10000",
            "sort": "fbt:asc",
            "date": target.strftime("%Y%m%d"),
        },
        headers={
            "User-Agent": "Mozilla/5.0 ProBigA evidence collector",
            "Referer": "https://quote.eastmoney.com/",
        },
        timeout=max(1.0, float(timeout_seconds)),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("rc") != 0 or not isinstance(payload.get("data"), dict):
        raise _blocked(f"Eastmoney limit pool failed for {target}")
    pool = payload["data"].get("pool")
    if pool is None:
        pool = []
    if not isinstance(pool, list):
        raise _blocked(f"Eastmoney limit pool payload differs for {target}")
    return _pool_codes_from_rows(pool, target), response.text


def _generate_eastmoney_upper_limit_history(
    engine,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    stock_codes: Sequence[str],
    preliminary_receipt_sha256: str,
    timeout_seconds: float = 20.0,
) -> dict[str, dict[str, Any]]:
    """Rebuild exact 21-session limit prices and cross-check provider hit state."""

    target = target_date if isinstance(target_date, date) else date.fromisoformat(
        str(target_date)
    )
    if target.isoformat() != str(target_date):
        raise _blocked("target date is not canonical")
    cutoff = decision_at if isinstance(decision_at, datetime) else datetime.fromisoformat(
        str(decision_at)
    )
    if cutoff.tzinfo is not None:
        raise _blocked("decision time must be naive Shanghai time")
    codes = tuple(sorted(str(code).strip().zfill(6) for code in stock_codes))
    if len(codes) != 80 or len(set(codes)) != 80:
        raise _blocked("upper-limit recovery requires exact 80 stock codes")
    receipt_sha = str(preliminary_receipt_sha256 or "").strip().lower()
    if len(receipt_sha) != 64 or any(ch not in "0123456789abcdef" for ch in receipt_sha):
        raise _blocked("preliminary receipt identity is invalid")

    with engine.connect() as connection:
        calendar = load_trade_calendar_receipt(
            connection,
            start_date=(target.fromordinal(target.toordinal() - 90)).isoformat(),
            end_date=target.isoformat(),
            decision_known_at=cutoff,
        )
        sessions = tuple(
            date.fromisoformat(value)
            for value in calendar.sessions
            if value <= target.isoformat()
        )[-21:]
        if len(sessions) != 21 or sessions[-1] != target:
            raise _blocked("calendar does not prove the exact 21-session window")
        names = {
            str(row.stock_code).zfill(6): str(row.short_name or "")
            for row in connection.execute(
                text(
                    "SELECT stock_code, short_name FROM si_all_code "
                    f"WHERE stock_code IN ({','.join(f':c{i}' for i in range(len(codes)))})"
                ),
                {f"c{i}": code for i, code in enumerate(codes)},
            ).mappings()
        }

    placeholders = ",".join(f":c{i}" for i in range(len(codes)))
    date_placeholders = ",".join(f":d{i}" for i in range(len(sessions)))
    params = {
        **{f"c{i}": code for i, code in enumerate(codes)},
        **{f"d{i}": value.isoformat() for i, value in enumerate(sessions)},
    }
    with get_kline_engine().connect() as connection:
        bars = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT stock_code,trade_date,pre_close,open,high,low,close,"
                    "volume,amount,data_source,quality_status,permission_status,"
                    "received_at,batch_id,data_version "
                    "FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0 "
                    f"AND stock_code IN ({placeholders}) "
                    f"AND trade_date IN ({date_placeholders})"
                ),
                params,
            ).mappings()
        ]
    bar_map = {
        (str(row["stock_code"]).zfill(6), str(row["trade_date"])): row
        for row in bars
    }

    request_payload = {
        "schema": "probiga.eastmoney-upper-history-request.v1",
        "target_date": target.isoformat(),
        "trade_dates": [value.isoformat() for value in sessions],
        "stock_codes": list(codes),
    }
    pools: dict[str, list[str]] = {}
    response_payloads: dict[str, str] = {}
    for session in sessions:
        pool, raw = _fetch_pool(session, timeout_seconds)
        pools[session.isoformat()] = pool
        response_payloads[session.isoformat()] = raw
    checked_dates = [key for key, values in pools.items() if values]
    if len(checked_dates) < 5 or not pools.get(target.isoformat()):
        raise _blocked("Eastmoney historical limit-pool coverage is insufficient")

    rows_by_code: dict[str, list[dict[str, Any]]] = {code: [] for code in codes}
    for code in codes:
        ratio = _limit_ratio(code, names.get(code, ""))
        for session in sessions:
            key = (code, session.isoformat())
            bar = bar_map.get(key)
            if bar is None:
                raise _blocked(f"stored QMT bar is missing for {code} {session}")
            source = str(bar.get("data_source") or "")
            quality = str(bar.get("quality_status") or "")
            permission = str(bar.get("permission_status") or "")
            trusted_source = (
                source == "gj_big_qmt_inner"
                and quality in {"", "QMT_ATTESTED", "VERIFIED"}
                and permission in {"", "SUPPORTED"}
            ) or (
                source in {"sina_history", "baostock"}
                and quality == "SOURCE_VERIFIED"
                and permission == "SUPPORTED"
            )
            if not trusted_source:
                raise _blocked(f"stored QMT bar evidence differs for {code} {session}")
            pre_close = _decimal(bar["pre_close"])
            close = _decimal(bar["close"])
            high = _decimal(bar["high"])
            low = _decimal(bar["low"])
            upper = (pre_close * (Decimal("1") + ratio)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            lower = (pre_close * (Decimal("1") - ratio)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            provider_available = session.isoformat() in checked_dates
            provider_member = (
                code in pools[session.isoformat()]
                if provider_available
                else close == upper
            )
            if provider_available and not provider_member:
                # The provider's exact-date close-limit membership is
                # authoritative. Keep a non-equal numeric ceiling so the
                # canonical chase policy records this row as not limit-up.
                upper = max(upper, close + Decimal("0.01"))
            if not upper > pre_close > lower > 0:
                raise _blocked(f"derived limit prices differ for {code} {session}")
            rows_by_code[code].append(
                {
                    "stock_code": code,
                    "trade_date": session.isoformat(),
                    "pre_close": format(pre_close, "f"),
                    "upper_limit": format(upper, "f"),
                    "lower_limit": format(lower, "f"),
                    "close": format(close, "f"),
                    "high": format(high, "f"),
                    "low": format(low, "f"),
                    "is_close_limit_up": provider_member,
                    "provider_pool_available": provider_available,
                    "provider_pool_member": provider_member,
                }
            )

    code_set = {
        "schema": "probiga.upper-limit-code-set.v1",
        "target_date": target.isoformat(),
        "stock_codes": list(codes),
    }
    trade_dates_payload = {
        "schema": "probiga.upper-limit-session-set.v1",
        "target_date": target.isoformat(),
        "trade_dates": [value.isoformat() for value in sessions],
    }
    subject = {
        "schema": "probiga.upper-limit-subject-recovery.v1",
        "target_date": target.isoformat(),
        "stock_codes": list(codes),
        "trade_dates": [value.isoformat() for value in sessions],
        "preliminary_receipt_sha256": receipt_sha,
        "calendar_batch_id": calendar.batch_id,
        "calendar_manifest_sha256": calendar.manifest_hash,
        "calendar_session_set_sha256": calendar.session_set_hash,
    }
    subject_sha = _sha256(subject)
    response_sha = _sha256(response_payloads)
    request_sha = _sha256(request_payload)
    captured_at = datetime.now().replace(microsecond=0)
    if captured_at > cutoff:
        raise _blocked("recovered upper-limit evidence crosses decision cutoff")
    worker_sha = _sha256(Path(__file__).read_bytes())
    semantic_rows = [
        row
        for code in codes
        for row in rows_by_code[code]
    ]
    semantic_sha = _sha256(semantic_rows)
    keyset_sha = _sha256([
        {"stock_code": code, "trade_date": session.isoformat()}
        for code in codes
        for session in sessions
    ])
    result: dict[str, dict[str, Any]] = {}
    for code in codes:
        stock_rows = rows_by_code[code]
        result[code] = {
            "upper_limits": {
                row["trade_date"]: Decimal(row["upper_limit"])
                for row in stock_rows
            },
            "upper_limit_evidence_json": build_upper_limit_evidence(
                {
                    "status": "PASS",
                    "stock_code": code,
                    "trade_date": target.isoformat(),
                    "window_start_date": sessions[0].isoformat(),
                    "window_end_date": target.isoformat(),
                    "decision_known_at": captured_at.isoformat(sep=" "),
                    "captured_at": captured_at.isoformat(sep=" "),
                    "published_at": captured_at.isoformat(sep=" "),
                    "source_table": EASTMONEY_HISTORY_SOURCE_TABLE,
                    "capture_kind": "DAILY_UPPER_LIMIT_HISTORY",
                    "provider": EASTMONEY_HISTORY_PROVIDER,
                    "source_field": "upper_limit",
                    "unit": "PRICE_CNY",
                    "transport_contract": EASTMONEY_HISTORY_TRANSPORT,
                    "entitlement_status": "SUPPORTED",
                    "timezone": "Asia/Shanghai",
                    "expected_stock_count": 80,
                    "expected_date_count": 21,
                    "snapshot_run_id": subject_sha[:32],
                    "subject_identity": f"preview:{receipt_sha}",
                    "subject_sha256": subject_sha,
                    "preliminary_receipt_payload_sha256": receipt_sha,
                    "code_set_sha256": _sha256(code_set),
                    "trade_dates_sha256": _sha256(trade_dates_payload),
                    "calendar_batch_id": calendar.batch_id,
                    "calendar_manifest_sha256": calendar.manifest_hash,
                    "calendar_session_set_sha256": calendar.session_set_hash,
                    "expected_keyset_sha256": keyset_sha,
                    "snapshot_semantic_sha256": semantic_sha,
                    "stock_rows_sha256": _sha256(stock_rows),
                    "provider_response_sha256": response_sha,
                    "canonical_request_sha256": request_sha,
                    "worker_sha256": worker_sha,
                    "sdk_version": f"requests-{requests.__version__}",
                    "python_version": platform.python_version(),
                }
            ),
        }
    return result


def _cache_root() -> Path:
    return Path(
        os.environ.get("PROBIGA_JOB_LOG_ROOT") or "/var/lib/probiga/jobs"
    ) / "upper-limit-history"


def _cache_path(target: date, codes: Sequence[str]) -> Path:
    code_sha = _sha256({
        "schema": "probiga.eastmoney-upper-limit-codes.v1",
        "trade_date": target.isoformat(),
        "stock_codes": list(sorted(codes)),
    })
    return _cache_root() / f"{target.isoformat()}-{code_sha}.json"


def prepare_eastmoney_upper_limit_history(
    engine,
    *,
    target_date: date | str,
    stock_codes: Sequence[str],
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """Create one immutable local cache before a strategy decision starts."""

    target = target_date if isinstance(target_date, date) else date.fromisoformat(
        str(target_date)
    )
    codes = tuple(sorted(str(code).strip().zfill(6) for code in stock_codes))
    now = datetime.now(_SHANGHAI).replace(tzinfo=None, microsecond=0)
    evidence = _generate_eastmoney_upper_limit_history(
        engine,
        target_date=target,
        decision_at=now + timedelta(minutes=30),
        stock_codes=codes,
        preliminary_receipt_sha256="a" * 64,
        timeout_seconds=timeout_seconds,
    )
    captured = max(
        datetime.fromisoformat(
            validate_upper_limit_evidence(item["upper_limit_evidence_json"])[
                "captured_at"
            ]
        )
        for item in evidence.values()
    )
    content = {
        "schema": EASTMONEY_HISTORY_CACHE_SCHEMA,
        "target_date": target.isoformat(),
        "stock_codes": list(codes),
        "trade_dates": sorted(next(iter(evidence.values()))["upper_limits"]),
        "captured_at": captured.isoformat(sep=" "),
        "evidence": {
            code: {
                "upper_limits": {
                    day: format(value, "f")
                    for day, value in item["upper_limits"].items()
                },
                "upper_limit_evidence_json": item["upper_limit_evidence_json"],
            }
            for code, item in evidence.items()
        },
    }
    record = {
        **content,
        "record_sha256": _sha256(content),
    }
    raw = _canonical_json(record).encode("utf-8")
    path = _cache_path(target, codes)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        written = 0
        while written < len(raw):
            count = os.write(descriptor, raw[written:])
            if count <= 0:
                raise _blocked("upper-limit cache write made no progress")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise _blocked("upper-limit evidence cache already differs")
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "status": "COMPLETED",
        "target_date": target.isoformat(),
        "stock_count": len(codes),
        "date_count": 21,
        "captured_at": captured.isoformat(sep=" "),
        "record_sha256": record["record_sha256"],
        "cache_path": str(path),
    }


def load_eastmoney_upper_limit_history(
    engine,
    *,
    target_date: date | str,
    decision_at: datetime | str,
    stock_codes: Sequence[str],
    preliminary_receipt_sha256: str,
    timeout_seconds: float = 20.0,
) -> dict[str, dict[str, Any]]:
    """Load a predecision immutable cache; analysis never fetches after its cutoff."""

    del engine, preliminary_receipt_sha256, timeout_seconds
    target = target_date if isinstance(target_date, date) else date.fromisoformat(
        str(target_date)
    )
    cutoff = decision_at if isinstance(decision_at, datetime) else datetime.fromisoformat(
        str(decision_at)
    )
    codes = tuple(sorted(str(code).strip().zfill(6) for code in stock_codes))
    path = _cache_path(target, codes)
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return {}
    supplied = str(record.pop("record_sha256", "") or "")
    if (
        record.get("schema") != EASTMONEY_HISTORY_CACHE_SCHEMA
        or record.get("target_date") != target.isoformat()
        or record.get("stock_codes") != list(codes)
        or len(record.get("trade_dates") or []) != 21
        or supplied != _sha256(record)
    ):
        raise _blocked("upper-limit evidence cache identity differs")
    captured = datetime.fromisoformat(str(record.get("captured_at") or ""))
    if captured > cutoff:
        raise _blocked("upper-limit evidence cache crosses decision cutoff")
    evidence_raw = record.get("evidence")
    if not isinstance(evidence_raw, dict) or set(evidence_raw) != set(codes):
        raise _blocked("upper-limit evidence cache coverage differs")
    result: dict[str, dict[str, Any]] = {}
    for code in codes:
        item = evidence_raw[code]
        limits = item.get("upper_limits") if isinstance(item, dict) else None
        if not isinstance(limits, dict) or set(limits) != set(record["trade_dates"]):
            raise _blocked(f"upper-limit evidence cache dates differ for {code}")
        upper_limits = {
            day: Decimal(str(value)) for day, value in limits.items()
        }
        proof = validate_upper_limit_evidence(
            item.get("upper_limit_evidence_json")
        )
        if (
            proof.get("stock_code") != code
            or proof.get("trade_date") != target.isoformat()
            or proof.get("captured_at") != record.get("captured_at")
        ):
            raise _blocked(f"upper-limit evidence cache proof differs for {code}")
        result[code] = {
            "upper_limits": upper_limits,
            "upper_limit_evidence_json": item["upper_limit_evidence_json"],
        }
    return result


__all__ = [
    "EASTMONEY_HISTORY_API_PATH",
    "EASTMONEY_HISTORY_PROVIDER",
    "EASTMONEY_HISTORY_SOURCE_TABLE",
    "EASTMONEY_HISTORY_TRANSPORT",
    "EastmoneyUpperLimitHistoryBlocked",
    "load_eastmoney_upper_limit_history",
    "prepare_eastmoney_upper_limit_history",
]
