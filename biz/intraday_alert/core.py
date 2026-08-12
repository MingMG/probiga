# -*- coding: utf-8 -*-
"""Evidence-backed intraday alert pipeline.

The module deliberately separates observation from interpretation and delivery:
QMT is the authority for the full-stock snapshot, public quotes are optional
benchmark evidence, rules are pure functions, and WeCom is only touched while
explicitly running in ``live`` mode.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from integrations.wecom.delivery import deliver_markdown
from server.common.config import get_wecom_webhook
from server.common.mysql_lock import mysql_named_lock

from .schema import (
    BENCHMARK_TABLE,
    OBSERVATION_TABLE,
    OUTBOX_TABLE,
    STATE_TABLE,
    ensure_intraday_alert_tables,
)


LOGGER = logging.getLogger(__name__)
CN_TZ = ZoneInfo("Asia/Shanghai")
LOCK_NAME = "probiga:intraday_market_alert"
CONFIG_VERSION = "intraday_alert_v1"
CONFIG_HASH = hashlib.sha256(CONFIG_VERSION.encode("utf-8")).hexdigest()
MIN_COVERAGE = 0.95
MAX_SOURCE_AGE_SECONDS = 120
COOLDOWN_MINUTES = 15
NORMAL_DAILY_CAP = 7
HARD_DAILY_CAP = 12
ACTIVE_STATES = frozenset({"SUSPECTED", "ENHANCED", "CONFIRMED"})
EMIT_STATES = frozenset({"ENHANCED", "CONFIRMED", "INVALIDATED"})
STATE_RANK = {"SUSPECTED": 1, "ENHANCED": 2, "CONFIRMED": 3}


BENCHMARKS: tuple[dict[str, str], ...] = (
    {"secid": "1.000016", "code": "000016", "name": "上证50", "type": "INDEX"},
    {"secid": "1.000300", "code": "000300", "name": "沪深300", "type": "INDEX"},
    {"secid": "1.000905", "code": "000905", "name": "中证500", "type": "INDEX"},
    {"secid": "1.000852", "code": "000852", "name": "中证1000", "type": "INDEX"},
    {"secid": "0.399303", "code": "399303", "name": "中证2000", "type": "INDEX"},
    {"secid": "0.399006", "code": "399006", "name": "创业板指", "type": "INDEX"},
    {"secid": "1.000688", "code": "000688", "name": "科创50", "type": "INDEX"},
    {"secid": "1.510050", "code": "510050", "name": "上证50ETF", "type": "ETF"},
    {"secid": "1.510300", "code": "510300", "name": "沪深300ETF", "type": "ETF"},
    {"secid": "1.510310", "code": "510310", "name": "沪深300ETF易方达", "type": "ETF"},
    {"secid": "0.159919", "code": "159919", "name": "沪深300ETF", "type": "ETF"},
    {"secid": "1.510500", "code": "510500", "name": "中证500ETF", "type": "ETF"},
    {"secid": "1.512100", "code": "512100", "name": "中证1000ETF", "type": "ETF"},
    {"secid": "0.159915", "code": "159915", "name": "创业板ETF", "type": "ETF"},
    {"secid": "1.510880", "code": "510880", "name": "红利ETF", "type": "ETF"},
)
_BENCHMARK_BY_CODE = {item["code"]: item for item in BENCHMARKS}


class IntradayAlertError(RuntimeError):
    """Base exception whose text is safe for scheduler logs."""


class QualityGateError(IntradayAlertError):
    """Raised when an observation cannot be tied to fresh authoritative data."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class DeliveryDispatchError(IntradayAlertError):
    """A durable outbox delivery failed and should make the process non-zero."""


@dataclass(frozen=True)
class ValidatedSnapshot:
    receipt: dict[str, Any]
    rows: list[dict[str, Any]]
    warnings: tuple[str, ...]


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _as_naive_cn(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CN_TZ).replace(tzinfo=None)
    return parsed


def china_now() -> datetime:
    return datetime.now(CN_TZ).replace(tzinfo=None)


def session_minute(now: datetime) -> int | None:
    """Return a stable minute number for same-time historical comparisons."""

    minute = now.hour * 60 + now.minute
    if 9 * 60 + 30 <= minute <= 11 * 60 + 30:
        return minute - (9 * 60 + 30)
    if 13 * 60 <= minute <= 15 * 60:
        return 121 + minute - 13 * 60
    return None


def make_observation_id(receipt: Mapping[str, Any]) -> str:
    payload = {
        "receipt_id": str(receipt.get("receipt_id") or ""),
        "source_provider": str(receipt.get("source_provider") or ""),
        "source_snapshot_token": str(receipt.get("source_snapshot_token") or ""),
        "source_generated_at": str(receipt.get("source_generated_at") or ""),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _evidence_hash(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _json(event.get("evidence") or {}).encode("utf-8")
    ).hexdigest()


def _normalize_event(candidate: Mapping[str, Any]) -> dict[str, Any]:
    event_type = str(candidate.get("event_type") or candidate.get("type") or "MARKET").upper()
    subject_code = str(candidate.get("subject_code") or candidate.get("code") or "MARKET")
    subject_name = str(candidate.get("subject_name") or candidate.get("name") or "全市场")
    direction = str(candidate.get("direction") or "NEUTRAL").upper()
    target_state = str(candidate.get("target_state") or candidate.get("state") or "SUSPECTED").upper()
    if target_state not in ACTIVE_STATES | {"INVALIDATED"}:
        target_state = "SUSPECTED"
    event_key = str(
        candidate.get("event_key")
        or f"{event_type}:{subject_code}:{direction}"
    )[:220]
    event = dict(candidate)
    event.update(
        {
            "event_key": event_key,
            "event_type": event_type[:48],
            "subject_code": subject_code[:160],
            "subject_name": subject_name[:192],
            "direction": direction[:16],
            "target_state": target_state,
            "severity": max(0, min(9, int(_finite(candidate.get("severity"), 1)))),
            "evidence": dict(candidate.get("evidence") or {}),
        }
    )
    event["evidence_hash"] = _evidence_hash(event)
    return event


def _source_age(now: datetime, value: Any) -> float | None:
    parsed = _as_naive_cn(value)
    return None if parsed is None else (now - parsed).total_seconds()


def validate_qmt_snapshot(
    receipt: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
) -> ValidatedSnapshot:
    """Validate one exact full-market receipt/snapshot pair.

    A parsed full-batch id is binding.  Older receipts that do not contain one
    are accepted only with provider/count/time checks and carry an explicit
    warning, so downstream copy cannot describe the batch as verified.
    """

    item = dict(receipt or {})
    if not item:
        raise QualityGateError("receipt_missing", "QMT 实时全量回执缺失")
    if str(item.get("capture_mode") or "").upper() != "LIVE_FORWARD":
        raise QualityGateError("receipt_not_live", "QMT 回执不是 LIVE_FORWARD")
    if str(item.get("quality_status") or "").upper() != "PASS":
        raise QualityGateError("receipt_not_pass", "QMT 实时全量回执未通过质量校验")
    generated_at = _as_naive_cn(item.get("source_generated_at"))
    published_at = _as_naive_cn(item.get("published_at"))
    if generated_at is None or published_at is None:
        raise QualityGateError("receipt_time_missing", "QMT 回执缺少可解析的数据时间")
    if generated_at.date() != now.date() or published_at.date() != now.date():
        raise QualityGateError("receipt_wrong_date", "QMT 回执不是当日实时数据")
    for label, value in (("source", generated_at), ("published", published_at)):
        age = (now - value).total_seconds()
        if age < -30 or age > MAX_SOURCE_AGE_SECONDS:
            raise QualityGateError("receipt_stale", f"QMT {label} 回执已超过 {MAX_SOURCE_AGE_SECONDS} 秒")
    coverage = _finite(item.get("coverage"), -1)
    if coverage < MIN_COVERAGE:
        raise QualityGateError("receipt_low_coverage", "QMT 全量行情覆盖率低于 95%")
    observed = int(_finite(item.get("observed_count"), 0))
    if observed <= 0:
        raise QualityGateError("receipt_empty", "QMT 回执的实收股票数为零")

    deduplicated: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        code = str(row.get("stock_code") or "").strip()
        if code:
            deduplicated[code] = row
    required_count = math.ceil(observed * MIN_COVERAGE)
    if len(deduplicated) < required_count:
        raise QualityGateError(
            "snapshot_row_shortfall",
            f"当前行情仅 {len(deduplicated)} 只，低于回执实收数的 95%",
        )

    provider = str(item.get("source_provider") or "").strip()
    provider_matches = sum(
        1 for row in deduplicated.values()
        if str(row.get("data_source") or "").strip() == provider
    )
    if provider and provider_matches < required_count:
        raise QualityGateError("snapshot_provider_mismatch", "当前行情与 QMT 回执来源不一致")

    evidence = _json_object(item.get("evidence_json"))
    full_batch_id = str(evidence.get("full_batch_id") or "").strip()
    token = str(item.get("source_snapshot_token") or "").strip()
    batch_counts = Counter(
        str(row.get("batch_id") or "").strip() for row in deduplicated.values()
    )
    batch_counts.pop("", None)
    warnings: list[str] = []
    binding_batch = full_batch_id or (token if token in batch_counts else "")
    if binding_batch:
        if batch_counts.get(binding_batch, 0) < required_count:
            raise QualityGateError("snapshot_batch_mismatch", "当前行情批次与 QMT 全量回执不一致")
    else:
        warnings.append("receipt_batch_unverifiable")

    valid_rows: list[dict[str, Any]] = []
    for row in deduplicated.values():
        price = _finite(row.get("price"), -1)
        change_pct = _finite(row.get("change_pct"), float("nan"))
        amount = _finite(row.get("amount"), -1)
        if price <= 0 or not math.isfinite(change_pct) or amount < 0:
            continue
        # Retain first-day listings but reject obvious parsing/unit failures.
        if abs(change_pct) > 100:
            continue
        normalized = dict(row)
        normalized["stock_code"] = str(row.get("stock_code") or "").zfill(6)
        normalized["price"] = price
        normalized["change_pct"] = change_pct
        normalized["amount"] = amount
        valid_rows.append(normalized)
    if len(valid_rows) < required_count:
        raise QualityGateError("snapshot_invalid_rows", "有效股票行情低于回执实收数的 95%")
    return ValidatedSnapshot(item, valid_rows, tuple(warnings))


def load_validated_snapshot(current_engine: Engine, *, now: datetime) -> ValidatedSnapshot:
    with current_engine.connect() as connection:
        receipt = connection.execute(
            text(
                """
                SELECT receipt_id, source_provider, source_snapshot_token,
                       source_full_file_token, source_generated_at, heartbeat_at,
                       expected_count, observed_count, coverage, published_at,
                       capture_mode, quality_status, evidence_json
                FROM st_qmt_realtime_sync_receipt_v2
                ORDER BY published_at DESC, created_at DESC
                LIMIT 1
                """
            )
        ).mappings().first()
        rows = connection.execute(
            text(
                """
                SELECT stock_code, short_name, price, change_pct, volume, amount,
                       snapshot_at, data_source, source_time, received_at, batch_id,
                       quality_status, permission_status
                FROM sm_stock_current
                """
            )
        ).mappings().all()
    return validate_qmt_snapshot(dict(receipt or {}), [dict(row) for row in rows], now=now)


def compute_market_metrics(
    rows: Sequence[Mapping[str, Any]],
    previous_market: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    returns = [_finite(row.get("change_pct")) for row in rows]
    amounts = [max(0.0, _finite(row.get("amount"))) for row in rows]
    total_amount = float(sum(amounts))
    previous_amount = _finite((previous_market or {}).get("total_amount"), 0)
    return {
        "stock_count": len(rows),
        "median_return_pct": statistics.median(returns) if returns else 0.0,
        "equal_weight_return_pct": statistics.fmean(returns) if returns else 0.0,
        "positive_breadth_pct": (
            sum(1 for value in returns if value > 0) / len(returns) * 100
            if returns else 0.0
        ),
        "total_amount": total_amount,
        "amount_delta": max(0.0, total_amount - previous_amount),
    }


def _eastmoney_time(value: Any, fallback: datetime) -> datetime | None:
    timestamp = int(_finite(value, 0))
    if timestamp > 1_500_000_000:
        return datetime.fromtimestamp(timestamp, CN_TZ).replace(tzinfo=None)
    return None


def fetch_benchmark_quotes(*, now: datetime, timeout: float = 8.0) -> list[dict[str, Any]]:
    """Fetch multiple broad indexes and ETF products from Eastmoney.

    This feed is corroborating evidence only; a failure never weakens the QMT
    full-market quality gate and simply makes benchmark-dependent rules absent.
    """

    endpoint = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "fields": "f12,f14,f2,f3,f5,f6,f18,f124",
        "secids": ",".join(item["secid"] for item in BENCHMARKS),
    }
    with httpx.Client(timeout=timeout, trust_env=False) as client:
        response = client.get(endpoint, params=params)
        response.raise_for_status()
        payload = response.json()
    diff = (payload.get("data") or {}).get("diff") or []
    if isinstance(diff, Mapping):
        diff = list(diff.values())
    quotes: list[dict[str, Any]] = []
    for raw in diff:
        if not isinstance(raw, Mapping):
            continue
        code = str(raw.get("f12") or "").strip().zfill(6)
        metadata = _BENCHMARK_BY_CODE.get(code)
        price = _finite(raw.get("f2"), -1)
        change_pct = _finite(raw.get("f3"), float("nan"))
        amount = _finite(raw.get("f6"), -1)
        if not metadata or price <= 0 or amount < 0 or not math.isfinite(change_pct):
            continue
        quotes.append(
            {
                "instrument_code": code,
                "instrument_name": str(raw.get("f14") or metadata["name"]),
                "instrument_type": metadata["type"],
                "price": price,
                "change_pct": change_pct,
                "amount": amount,
                "source_provider": "eastmoney_ulist",
                "source_time": _eastmoney_time(raw.get("f124"), now),
                "quality_status": "PASS",
            }
        )
    return quotes


def _median(values: Iterable[Any]) -> float | None:
    normalized = [_finite(value, float("nan")) for value in values]
    normalized = [value for value in normalized if math.isfinite(value)]
    return statistics.median(normalized) if normalized else None


def prepare_benchmark_snapshot(
    connection: Connection,
    quotes: Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    observed_at: datetime,
    minute_number: int,
) -> dict[str, Any]:
    snapshot_at = observed_at.replace(second=0, microsecond=0)
    items: list[dict[str, Any]] = []
    for raw in quotes:
        item = dict(raw)
        code = str(item.get("instrument_code") or "").zfill(6)
        source_time = _as_naive_cn(item.get("source_time"))
        source_age = _source_age(observed_at, source_time)
        if (
            str(item.get("quality_status") or "").upper() != "PASS"
            or source_time is None
            or source_time.date() != trade_date
            or source_age is None
            or source_age < -30
            or source_age > MAX_SOURCE_AGE_SECONDS
        ):
            continue
        previous = connection.execute(
            text(
                f"""
                SELECT amount
                FROM {BENCHMARK_TABLE}
                WHERE trade_date = :trade_date
                  AND instrument_code = :code
                  AND snapshot_at < :snapshot_at
                ORDER BY snapshot_at DESC
                LIMIT 1
                """
            ),
            {"trade_date": trade_date, "code": code, "snapshot_at": snapshot_at},
        ).scalar()
        amount = max(0.0, _finite(item.get("amount")))
        amount_delta = max(0.0, amount - _finite(previous, amount)) if previous is not None else 0.0
        historical = [
            _finite(row[0])
            for row in connection.execute(
                text(
                    f"""
                    SELECT amount_delta
                    FROM {BENCHMARK_TABLE}
                    WHERE instrument_code = :code
                      AND trade_date < :trade_date
                      AND session_minute = :session_minute
                      AND amount_delta > 0
                    ORDER BY trade_date DESC
                    LIMIT 20
                    """
                ),
                {"code": code, "trade_date": trade_date, "session_minute": minute_number},
            ).fetchall()
        ]
        short = [
            _finite(row[0])
            for row in connection.execute(
                text(
                    f"""
                    SELECT amount_delta
                    FROM {BENCHMARK_TABLE}
                    WHERE instrument_code = :code
                      AND trade_date = :trade_date
                      AND snapshot_at < :snapshot_at
                      AND amount_delta > 0
                    ORDER BY snapshot_at DESC
                    LIMIT 15
                    """
                ),
                {"code": code, "trade_date": trade_date, "snapshot_at": snapshot_at},
            ).fetchall()
        ]
        historical_baseline = _median(historical) if len(historical) >= 10 else None
        short_baseline = _median(short) if len(short) >= 5 else None
        baseline_type = "unavailable"
        baseline_ratio = None
        baseline_samples = 0
        if historical_baseline and historical_baseline > 0:
            baseline_type = "historical_same_minute"
            baseline_ratio = amount_delta / historical_baseline
            baseline_samples = len(historical)
        elif short_baseline and short_baseline > 0:
            baseline_type = "intraday_short"
            baseline_ratio = amount_delta / short_baseline
            baseline_samples = len(short)
        prepared = {
            **item,
            "instrument_code": code,
            "amount": amount,
            "amount_delta": amount_delta,
            "baseline_type": baseline_type,
            "baseline_ratio": baseline_ratio,
            "amount_ratio": baseline_ratio,
            "baseline_samples": baseline_samples,
        }
        items.append(prepared)
        connection.execute(
            text(
                f"""
                INSERT INTO {BENCHMARK_TABLE} (
                    trade_date, snapshot_at, session_minute, instrument_code,
                    instrument_name, instrument_type, price, change_pct, amount,
                    amount_delta, source_provider, source_time, quality_status,
                    created_at
                ) VALUES (
                    :trade_date, :snapshot_at, :session_minute, :instrument_code,
                    :instrument_name, :instrument_type, :price, :change_pct, :amount,
                    :amount_delta, :source_provider, :source_time, :quality_status,
                    :created_at
                ) ON DUPLICATE KEY UPDATE
                    price=VALUES(price), change_pct=VALUES(change_pct),
                    amount=VALUES(amount), amount_delta=VALUES(amount_delta),
                    source_time=VALUES(source_time), quality_status=VALUES(quality_status)
                """
            ),
            {
                **prepared,
                "trade_date": trade_date,
                "snapshot_at": snapshot_at,
                "session_minute": minute_number,
                "created_at": observed_at,
            },
        )
    return {
        "available": bool(items),
        "source_provider": "eastmoney_ulist" if items else None,
        "items": items,
        "by_code": {item["instrument_code"]: item for item in items},
        "historical_claim_ready": any(
            item.get("baseline_type") == "historical_same_minute" for item in items
        ),
    }


def load_sector_membership(
    engine: Engine,
    *,
    trade_date: date,
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    """Load an immutable, receipt-matched QMT_VALIDATED SW-L1 snapshot."""

    try:
        with engine.connect() as connection:
            run = connection.execute(
                text(
                    """
                    SELECT snapshot_date, source, quality_status,
                           industry_relation_count, captured_at
                    FROM qmt_membership_snapshot_run
                    WHERE snapshot_date <= :trade_date
                      AND source = 'gj_big_qmt_inner'
                      AND quality_status = 'QMT_VALIDATED'
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                    """
                ),
                {"trade_date": trade_date},
            ).mappings().first()
            if not run:
                return {}, {"available": False, "reason": "validated_snapshot_missing"}
            params = {"snapshot_date": run["snapshot_date"], "source": run["source"]}
            actual = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM qmt_industry_member_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND source = :source
                      AND quality_status = 'QMT_VALIDATED'
                    """
                ),
                params,
            ).scalar()
            if int(actual or 0) != int(run["industry_relation_count"] or 0):
                return {}, {"available": False, "reason": "snapshot_receipt_mismatch"}
            rows = connection.execute(
                text(
                    """
                    SELECT stock_code, industry_code, industry_name
                    FROM qmt_industry_member_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND source = :source
                      AND quality_status = 'QMT_VALIDATED'
                      AND industry_type = '申万一级'
                    """
                ),
                params,
            ).mappings().all()
    except Exception as exc:
        LOGGER.warning("intraday sector snapshot unavailable: %s", type(exc).__name__)
        return {}, {"available": False, "reason": "snapshot_query_failed"}
    mapping = {
        str(row["stock_code"]).zfill(6): {
            "industry_code": str(row["industry_code"]),
            "industry_name": str(row["industry_name"]),
        }
        for row in rows
    }
    return mapping, {
        "available": bool(mapping),
        "snapshot_date": str(run["snapshot_date"]),
        "source": str(run["source"]),
        "quality_status": "QMT_VALIDATED",
        "member_count": len(mapping),
    }


def aggregate_sectors(
    rows: Sequence[Mapping[str, Any]],
    membership: Mapping[str, Mapping[str, str]],
    metadata: Mapping[str, Any],
    previous_sector: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not membership or not metadata.get("available"):
        return {**dict(metadata), "available": False, "items": []}
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    names: dict[str, str] = {}
    for row in rows:
        member = membership.get(str(row.get("stock_code") or "").zfill(6))
        if not member:
            continue
        code = str(member["industry_code"])
        names[code] = str(member["industry_name"])
        grouped[code].append(row)
    previous_items = {
        str(item.get("industry_code")): item
        for item in (previous_sector or {}).get("items", [])
        if isinstance(item, Mapping)
    }
    items: list[dict[str, Any]] = []
    for code, members in grouped.items():
        if len(members) < 5:
            continue
        returns = [_finite(row.get("change_pct")) for row in members]
        amount = sum(max(0.0, _finite(row.get("amount"))) for row in members)
        previous_amount = _finite(previous_items.get(code, {}).get("amount"), amount)
        leaders = sorted(
            members,
            key=lambda row: (_finite(row.get("change_pct")), _finite(row.get("amount"))),
            reverse=True,
        )[:3]
        items.append(
            {
                "industry_code": code,
                "industry_name": names[code],
                "code": code,
                "name": names[code],
                "member_count": len(members),
                "median_return_pct": statistics.median(returns),
                "equal_weight_return_pct": statistics.fmean(returns),
                "positive_breadth_pct": sum(value > 0 for value in returns) / len(returns) * 100,
                "amount": amount,
                "amount_delta": max(0.0, amount - previous_amount),
                "leaders": [
                    {
                        "stock_code": str(row.get("stock_code")),
                        "short_name": str(row.get("short_name") or row.get("stock_code")),
                        "change_pct": _finite(row.get("change_pct")),
                        "amount": _finite(row.get("amount")),
                    }
                    for row in leaders
                ],
            }
        )
    items.sort(key=lambda item: item["median_return_pct"], reverse=True)
    return {**dict(metadata), "available": bool(items), "items": items}


def build_key_stocks(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    def compact(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "stock_code": str(row.get("stock_code") or ""),
            "short_name": str(row.get("short_name") or row.get("stock_code") or ""),
            "price": _finite(row.get("price")),
            "change_pct": _finite(row.get("change_pct")),
            "amount": _finite(row.get("amount")),
        }

    ordinary = [row for row in rows if abs(_finite(row.get("change_pct"))) <= 35]
    return {
        "top_turnover": [compact(row) for row in sorted(ordinary, key=lambda r: _finite(r.get("amount")), reverse=True)[:15]],
        "leaders": [compact(row) for row in sorted(ordinary, key=lambda r: (_finite(r.get("change_pct")), _finite(r.get("amount"))), reverse=True)[:10]],
        "laggards": [compact(row) for row in sorted(ordinary, key=lambda r: (_finite(r.get("change_pct")), -_finite(r.get("amount"))))[:10]],
    }


def compute_style(benchmark: Mapping[str, Any], sectors: Mapping[str, Any]) -> dict[str, Any]:
    by_code = benchmark.get("by_code") or {}

    def changes(codes: Sequence[str]) -> list[float]:
        return [
            _finite(by_code[code].get("change_pct"))
            for code in codes if code in by_code
        ]

    large_values = changes(("000016", "000300"))
    small_values = changes(("000852", "399303"))
    tech_values = changes(("399006", "000688"))
    large = statistics.fmean(large_values) if large_values else None
    small = statistics.fmean(small_values) if small_values else None
    tech = statistics.fmean(tech_values) if tech_values else None
    defensive_values = [
        _finite(item.get("median_return_pct"))
        for item in sectors.get("items", [])
        if any(word in str(item.get("industry_name")) for word in ("银行", "公用事业", "煤炭"))
    ]
    defensive = statistics.fmean(defensive_values) if defensive_values else None
    return {
        "available": large is not None and small is not None,
        "large_cap_return_pct": large,
        "small_cap_return_pct": small,
        "large_small_spread_pct": large - small if large is not None and small is not None else None,
        "tech_return_pct": tech,
        "defensive_return_pct": defensive,
        "tech_defensive_spread_pct": tech - defensive if tech is not None and defensive is not None else None,
        "items": (
            [
                {"code": "large_cap", "name": "大盘权重", "pair_group": "large_small", "opposite_code": "small_cap", "return_pct": large},
                {"code": "small_cap", "name": "小盘宽基", "pair_group": "large_small", "opposite_code": "large_cap", "return_pct": small},
            ]
            if large is not None and small is not None else []
        ) + (
            [
                {"code": "technology", "name": "科技成长", "pair_group": "tech_defensive", "opposite_code": "defensive", "return_pct": tech},
                {"code": "defensive", "name": "防御方向", "pair_group": "tech_defensive", "opposite_code": "technology", "return_pct": defensive},
            ]
            if tech is not None and defensive is not None else []
        ),
    }


def _load_previous_observation(connection: Connection, trade_date: date) -> dict[str, Any] | None:
    row = connection.execute(
        text(
            f"""
            SELECT observed_at, market_json, sector_json, key_stock_json,
                   style_json, benchmark_json
            FROM {OBSERVATION_TABLE}
            WHERE trade_date = :trade_date
            ORDER BY observed_at DESC
            LIMIT 1
            """
        ),
        {"trade_date": trade_date},
    ).mappings().first()
    if not row:
        return None
    return {
        "observed_at": row["observed_at"],
        "market": _json_object(row["market_json"]),
        "sectors": _json_object(row["sector_json"]),
        "key_stocks": _json_object(row["key_stock_json"]),
        "style": _json_object(row["style_json"]),
        "benchmarks": _json_object(row["benchmark_json"]),
    }


def _load_history(connection: Connection, trade_date: date, limit: int = 30) -> list[dict[str, Any]]:
    rows = connection.execute(
        text(
            f"""
            SELECT observed_at, session_minute, market_json, sector_json,
                   key_stock_json, style_json, benchmark_json
            FROM {OBSERVATION_TABLE}
            WHERE trade_date = :trade_date
            ORDER BY observed_at DESC
            LIMIT :row_limit
            """
        ),
        {"trade_date": trade_date, "row_limit": limit},
    ).mappings().all()
    history = [
        {
            "observed_at": row["observed_at"],
            "session_minute": int(row["session_minute"]),
            "market": _json_object(row["market_json"]),
            "sectors": _json_object(row["sector_json"]),
            "key_stocks": _json_object(row["key_stock_json"]),
            "style": _json_object(row["style_json"]),
            "benchmarks": _json_object(row["benchmark_json"]),
        }
        for row in rows
    ]
    history.reverse()
    return history


def advance_states(
    previous_states: Mapping[str, Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    emitted_today: int = 0,
    normal_cap: int = NORMAL_DAILY_CAP,
    hard_cap: int = HARD_DAILY_CAP,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pure state transition used by both production persistence and tests."""

    normalized = {_normalize_event(item)["event_key"]: _normalize_event(item) for item in candidates}
    states: list[dict[str, Any]] = []
    emissions: list[dict[str, Any]] = []
    emitted = emitted_today
    all_keys = set(previous_states) | set(normalized)
    for event_key in sorted(all_keys):
        previous = dict(previous_states.get(event_key) or {})
        event = normalized.get(event_key)
        if event is None:
            if not previous or str(previous.get("state")) not in ACTIVE_STATES:
                continue
            miss_count = int(previous.get("miss_count") or 0) + 1
            updated = {**previous, "miss_count": miss_count, "updated_at": now}
            states.append(updated)
            continue

        target = event["target_state"]
        if target == "INVALIDATED":
            if not previous or str(previous.get("state")) not in ACTIVE_STATES:
                continue
            updated = {
                **previous,
                **event,
                "state": "INVALIDATED",
                "miss_count": 0,
                "last_seen_at": now,
                "updated_at": now,
            }
            if previous.get("last_sent_state") in {"ENHANCED", "CONFIRMED"} and emitted < hard_cap:
                emissions.append({**updated, "transition_name": "INVALIDATED"})
                emitted += 1
                updated["last_sent_state"] = "INVALIDATED"
                updated["last_sent_at"] = now
                updated["cooldown_until"] = now + timedelta(minutes=COOLDOWN_MINUTES)
            states.append(updated)
            continue
        if not previous or str(previous.get("state")) == "INVALIDATED":
            cycle = int(previous.get("cycle") or 0) + 1
            updated = {
                **event,
                "cycle": cycle,
                "state": target,
                "hit_count": 1,
                "miss_count": 0,
                "first_seen_at": now,
                "last_seen_at": now,
                "last_sent_state": None,
                "last_sent_at": None,
                "cooldown_until": previous.get("cooldown_until"),
                "updated_at": now,
            }
        else:
            previous_state = str(previous.get("state") or "SUSPECTED")
            if STATE_RANK[target] < STATE_RANK.get(previous_state, 1):
                miss_count = int(previous.get("miss_count") or 0) + 1
                updated = {**previous, "miss_count": miss_count, "updated_at": now}
                states.append(updated)
                continue
            updated = {
                **previous,
                **event,
                "state": target,
                "hit_count": int(previous.get("hit_count") or 0) + 1,
                "miss_count": 0,
                "last_seen_at": now,
                "updated_at": now,
            }

        last_sent_state = str(updated.get("last_sent_state") or "")
        cooldown_until = _as_naive_cn(updated.get("cooldown_until"))
        state_changed = last_sent_state != target
        cooldown_ready = cooldown_until is None or now >= cooldown_until
        upgrade = STATE_RANK.get(target, 0) > STATE_RANK.get(last_sent_state, 0)
        cap = hard_cap if int(event.get("severity") or 0) >= 3 else normal_cap
        if target in {"ENHANCED", "CONFIRMED"} and state_changed and (upgrade or cooldown_ready) and emitted < cap:
            emissions.append({**updated, "transition_name": target})
            emitted += 1
            updated["last_sent_state"] = target
            updated["last_sent_at"] = now
            updated["cooldown_until"] = now + timedelta(minutes=COOLDOWN_MINUTES)
        states.append(updated)
    return states, emissions


def _state_rows(connection: Connection, trade_date: date) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        text(f"SELECT * FROM {STATE_TABLE} WHERE trade_date = :trade_date"),
        {"trade_date": trade_date},
    ).mappings().all()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        item["evidence"] = _json_object(item.pop("evidence_json", None))
        result[str(item["event_key"])] = item
    return result


def _upsert_state(connection: Connection, trade_date: date, state: Mapping[str, Any]) -> None:
    connection.execute(
        text(
            f"""
            INSERT INTO {STATE_TABLE} (
                trade_date, event_key, cycle, event_type, subject_code,
                subject_name, direction, state, severity, hit_count, miss_count,
                first_seen_at, last_seen_at, last_sent_state, last_sent_at,
                cooldown_until, evidence_hash, evidence_json, updated_at
            ) VALUES (
                :trade_date, :event_key, :cycle, :event_type, :subject_code,
                :subject_name, :direction, :state, :severity, :hit_count, :miss_count,
                :first_seen_at, :last_seen_at, :last_sent_state, :last_sent_at,
                :cooldown_until, :evidence_hash, :evidence_json, :updated_at
            ) ON DUPLICATE KEY UPDATE
                cycle=VALUES(cycle), event_type=VALUES(event_type),
                subject_code=VALUES(subject_code), subject_name=VALUES(subject_name),
                direction=VALUES(direction), state=VALUES(state),
                severity=VALUES(severity), hit_count=VALUES(hit_count),
                miss_count=VALUES(miss_count), first_seen_at=VALUES(first_seen_at),
                last_seen_at=VALUES(last_seen_at), last_sent_state=VALUES(last_sent_state),
                last_sent_at=VALUES(last_sent_at), cooldown_until=VALUES(cooldown_until),
                evidence_hash=VALUES(evidence_hash), evidence_json=VALUES(evidence_json),
                updated_at=VALUES(updated_at)
            """
        ),
        {
            "trade_date": trade_date,
            "event_key": state["event_key"],
            "cycle": int(state.get("cycle") or 1),
            "event_type": str(state.get("event_type") or "MARKET")[:48],
            "subject_code": str(state.get("subject_code") or "MARKET")[:160],
            "subject_name": str(state.get("subject_name") or "全市场")[:192],
            "direction": str(state.get("direction") or "NEUTRAL")[:16],
            "state": str(state.get("state") or "SUSPECTED")[:24],
            "severity": int(state.get("severity") or 0),
            "hit_count": int(state.get("hit_count") or 0),
            "miss_count": int(state.get("miss_count") or 0),
            "first_seen_at": state.get("first_seen_at") or state.get("last_seen_at"),
            "last_seen_at": state.get("last_seen_at"),
            "last_sent_state": state.get("last_sent_state"),
            "last_sent_at": state.get("last_sent_at"),
            "cooldown_until": state.get("cooldown_until"),
            "evidence_hash": state.get("evidence_hash") or _evidence_hash(state),
            "evidence_json": _json(state.get("evidence") or {}),
            "updated_at": state.get("updated_at"),
        },
    )


def _render_emission(emission: Mapping[str, Any], observation: Mapping[str, Any]) -> str:
    from .render import render_event

    return str(render_event(dict(emission), dict(observation)))


def _insert_outbox(
    connection: Connection,
    *,
    trade_date: date,
    emission: Mapping[str, Any],
    mode: str,
    content: str,
    now: datetime,
) -> None:
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    outbox_id = hashlib.sha256(
        _json(
            {
                "trade_date": trade_date,
                "event_key": emission["event_key"],
                "cycle": emission.get("cycle"),
                "transition": emission["transition_name"],
                "evidence_hash": emission["evidence_hash"],
                "mode": mode,
            }
        ).encode("utf-8")
    ).hexdigest()
    connection.execute(
        text(
            f"""
            INSERT IGNORE INTO {OUTBOX_TABLE} (
                outbox_id, trade_date, event_key, cycle, transition_name,
                state, mode, status, evidence_hash, evidence_json,
                content_sha256, content_markdown, attempts, next_retry_at,
                claimed_at, delivery_id, error_message, created_at, sent_at,
                updated_at
            ) VALUES (
                :outbox_id, :trade_date, :event_key, :cycle, :transition_name,
                :state, :mode, :status, :evidence_hash, :evidence_json,
                :content_sha256, :content_markdown, 0, :next_retry_at,
                NULL, NULL, NULL, :created_at, NULL, :updated_at
            )
            """
        ),
        {
            "outbox_id": outbox_id,
            "trade_date": trade_date,
            "event_key": emission["event_key"],
            "cycle": int(emission.get("cycle") or 1),
            "transition_name": emission["transition_name"],
            "state": emission.get("state") or emission["transition_name"],
            "mode": mode,
            "status": "SHADOW" if mode == "shadow" else "PENDING",
            "evidence_hash": emission["evidence_hash"],
            "evidence_json": _json(emission.get("evidence") or {}),
            "content_sha256": content_hash,
            "content_markdown": content,
            "next_retry_at": now if mode == "live" else None,
            "created_at": now,
            "updated_at": now,
        },
    )


def _backoff_minutes(attempts: int) -> int:
    return (1, 2, 5, 10, 20, 30)[min(max(0, attempts - 1), 5)]


def drain_outbox(
    engine: Engine,
    *,
    mode: str,
    now: datetime,
    delivery_fn: Callable[..., Any] = deliver_markdown,
    webhook_getter: Callable[..., str] = get_wecom_webhook,
) -> int:
    """Deliver due live rows.  Shadow mode returns before resolving a webhook."""

    if mode != "live":
        return 0
    failures: list[str] = []
    delivered = 0
    # Recover a process that died after claiming but before finalizing.
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                UPDATE {OUTBOX_TABLE}
                SET status='FAILED', next_retry_at=:now,
                    error_message='stale delivery claim recovered', updated_at=:now
                WHERE mode='live' AND status='SENDING'
                  AND claimed_at < :stale_before
                """
            ),
            {"now": now, "stale_before": now - timedelta(minutes=5)},
        )
    while True:
        with engine.begin() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT outbox_id, content_markdown, attempts
                    FROM {OUTBOX_TABLE}
                    WHERE mode='live' AND status IN ('PENDING','FAILED')
                      AND (next_retry_at IS NULL OR next_retry_at <= :now)
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                ),
                {"now": now},
            ).mappings().first()
            if not row:
                break
            outbox_id = str(row["outbox_id"])
            attempts = int(row["attempts"] or 0) + 1
            content = str(row["content_markdown"])
            connection.execute(
                text(
                    f"""
                    UPDATE {OUTBOX_TABLE}
                    SET status='SENDING', attempts=:attempts, claimed_at=:now,
                        updated_at=:now, error_message=NULL
                    WHERE outbox_id=:outbox_id
                    """
                ),
                {"attempts": attempts, "now": now, "outbox_id": outbox_id},
            )
        try:
            webhook = webhook_getter("intraday", required=True)
            result = delivery_fn(
                webhook,
                content,
                engine=engine,
                delivery_kind="intraday_market_alert",
                title="盘中关键节点",
                webhook_kind="intraday",
            )
        except Exception as exc:
            safe_error = f"{type(exc).__name__}: intraday delivery failed"[:512]
            with engine.begin() as connection:
                connection.execute(
                    text(
                        f"""
                        UPDATE {OUTBOX_TABLE}
                        SET status='FAILED', next_retry_at=:next_retry_at,
                            claimed_at=NULL, error_message=:error_message,
                            updated_at=:now
                        WHERE outbox_id=:outbox_id
                        """
                    ),
                    {
                        "next_retry_at": now + timedelta(minutes=_backoff_minutes(attempts)),
                        "error_message": safe_error,
                        "now": now,
                        "outbox_id": outbox_id,
                    },
                )
            failures.append(outbox_id)
            break
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    UPDATE {OUTBOX_TABLE}
                    SET status='SENT', sent_at=:now, claimed_at=NULL,
                        delivery_id=:delivery_id, next_retry_at=NULL,
                        error_message=NULL, updated_at=:now
                    WHERE outbox_id=:outbox_id
                    """
                ),
                {
                    "now": now,
                    "delivery_id": str(getattr(result, "delivery_id", ""))[:36] or None,
                    "outbox_id": outbox_id,
                },
            )
        delivered += 1
    if failures:
        raise DeliveryDispatchError("盘中播报发送失败，已写入 outbox 并安排退避重试")
    return delivered


def _persist_observation(
    connection: Connection,
    *,
    observation_id: str,
    snapshot: ValidatedSnapshot,
    current: Mapping[str, Any],
    now: datetime,
    minute_number: int,
) -> None:
    receipt = snapshot.receipt
    market = current["market"]
    connection.execute(
        text(
            f"""
            INSERT INTO {OBSERVATION_TABLE} (
                observation_id, trade_date, source_snapshot_at, observed_at,
                session_minute, source_provider, source_receipt_id,
                expected_count, observed_count, coverage, median_return_pct,
                equal_weight_return_pct, positive_breadth_pct, total_amount,
                amount_delta, market_json, sector_json, key_stock_json,
                style_json, benchmark_json, quality_status, config_version,
                config_hash, created_at
            ) VALUES (
                :observation_id, :trade_date, :source_snapshot_at, :observed_at,
                :session_minute, :source_provider, :source_receipt_id,
                :expected_count, :observed_count, :coverage, :median_return_pct,
                :equal_weight_return_pct, :positive_breadth_pct, :total_amount,
                :amount_delta, :market_json, :sector_json, :key_stock_json,
                :style_json, :benchmark_json, 'PASS', :config_version,
                :config_hash, :created_at
            )
            """
        ),
        {
            "observation_id": observation_id,
            "trade_date": now.date(),
            "source_snapshot_at": _as_naive_cn(receipt["source_generated_at"]),
            "observed_at": now,
            "session_minute": minute_number,
            "source_provider": str(receipt.get("source_provider")),
            "source_receipt_id": str(receipt.get("receipt_id")),
            "expected_count": int(_finite(receipt.get("expected_count"), 0)),
            "observed_count": int(_finite(receipt.get("observed_count"), 0)),
            "coverage": _finite(receipt.get("coverage")),
            "median_return_pct": market["median_return_pct"],
            "equal_weight_return_pct": market["equal_weight_return_pct"],
            "positive_breadth_pct": market["positive_breadth_pct"],
            "total_amount": market["total_amount"],
            "amount_delta": market["amount_delta"],
            "market_json": _json(market),
            "sector_json": _json(current["sectors"]),
            "key_stock_json": _json(current["key_stocks"]),
            "style_json": _json(current["style"]),
            "benchmark_json": _json(current["benchmarks"]),
            "config_version": CONFIG_VERSION,
            "config_hash": CONFIG_HASH,
            "created_at": now,
        },
    )


def _is_trade_day(engine: Engine, target: date) -> bool:
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT trade_status FROM si_trade_calendar "
                    "WHERE trade_date = :trade_date LIMIT 1"
                ),
                {"trade_date": target},
            ).first()
    except Exception as exc:
        raise QualityGateError("trade_calendar_error", "交易日历查询失败，已关闭播报") from exc
    if row is None or row[0] is None:
        raise QualityGateError("trade_calendar_unknown", "交易日历状态未知，已关闭播报")
    return int(row[0]) == 1


def _evaluate(
    current: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]],
    previous_states: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    from .rules import evaluate_events

    events = evaluate_events(dict(current), list(history), previous_events=previous_states)
    return [dict(event) for event in (events or [])]


def run_intraday_scan(
    main_engine: Engine,
    current_engine: Engine,
    *,
    mode: str = "shadow",
    now: datetime | None = None,
    benchmark_fetcher: Callable[..., list[dict[str, Any]]] = fetch_benchmark_quotes,
) -> dict[str, Any]:
    """Run one source-batch-idempotent scan and optionally drain live outbox."""

    normalized_mode = str(mode or "shadow").strip().lower()
    if normalized_mode not in {"shadow", "live"}:
        raise ValueError("mode must be shadow or live")
    observed_at = _as_naive_cn(now or china_now())
    assert observed_at is not None
    minute_number = session_minute(observed_at)
    if minute_number is None:
        return {"status": "skipped", "reason": "outside_intraday_window", "mode": normalized_mode}
    if not _is_trade_day(main_engine, observed_at.date()):
        return {"status": "skipped", "reason": "non_trading_day", "mode": normalized_mode}

    ensure_intraday_alert_tables(main_engine)
    snapshot = load_validated_snapshot(current_engine, now=observed_at)
    observation_id = make_observation_id(snapshot.receipt)
    warnings = list(snapshot.warnings)
    membership, sector_metadata = load_sector_membership(main_engine, trade_date=observed_at.date())
    try:
        benchmark_quotes = benchmark_fetcher(now=observed_at)
    except Exception as exc:
        LOGGER.warning("intraday benchmark source unavailable: %s", type(exc).__name__)
        benchmark_quotes = []
        warnings.append("benchmark_source_unavailable")

    emissions_count = 0
    duplicate = False
    with mysql_named_lock(main_engine, LOCK_NAME, timeout_seconds=0) as connection:
        try:
            existing = connection.execute(
                text(
                    f"SELECT observation_id FROM {OBSERVATION_TABLE} "
                    "WHERE observation_id = :observation_id LIMIT 1"
                ),
                {"observation_id": observation_id},
            ).first()
            if existing:
                duplicate = True
                connection.commit()
            else:
                previous = _load_previous_observation(connection, observed_at.date())
                market = compute_market_metrics(
                    snapshot.rows,
                    (previous or {}).get("market"),
                )
                benchmarks = prepare_benchmark_snapshot(
                    connection,
                    benchmark_quotes,
                    trade_date=observed_at.date(),
                    observed_at=observed_at,
                    minute_number=minute_number,
                )
                sectors = aggregate_sectors(
                    snapshot.rows,
                    membership,
                    sector_metadata,
                    (previous or {}).get("sectors"),
                )
                current = {
                    "observed_at": observed_at,
                    "source_snapshot_at": _as_naive_cn(snapshot.receipt.get("source_generated_at")),
                    "session_minute": minute_number,
                    "coverage": _finite(snapshot.receipt.get("coverage")),
                    "observed_count": int(_finite(snapshot.receipt.get("observed_count"), 0)),
                    "expected_count": int(_finite(snapshot.receipt.get("expected_count"), 0)),
                    "source_provider": str(snapshot.receipt.get("source_provider") or ""),
                    "market": market,
                    "sectors": sectors,
                    "key_stocks": build_key_stocks(snapshot.rows),
                    "benchmarks": benchmarks,
                    "style": {},
                    "quality": {
                        "status": "PASS",
                        "source_provider": snapshot.receipt.get("source_provider"),
                        "receipt_id": snapshot.receipt.get("receipt_id"),
                        "coverage": _finite(snapshot.receipt.get("coverage")),
                        "batch_verified": "receipt_batch_unverifiable" not in warnings,
                        "warnings": warnings,
                    },
                }
                current["style"] = compute_style(benchmarks, sectors)
                _persist_observation(
                    connection,
                    observation_id=observation_id,
                    snapshot=snapshot,
                    current=current,
                    now=observed_at,
                    minute_number=minute_number,
                )
                history = _load_history(connection, observed_at.date())
                # Exclude the just-persisted current row from the historical window.
                previous_states = _state_rows(connection, observed_at.date())
                history = history[:-1]
                history = [
                    item for item in history
                    if (int(item["session_minute"]) >= 121) == (minute_number >= 121)
                ]
                candidates = _evaluate(current, history, previous_states)
                emitted_today = int(
                    connection.execute(
                        text(
                            f"SELECT COUNT(*) FROM {OUTBOX_TABLE} "
                            "WHERE trade_date=:trade_date AND mode=:mode"
                        ),
                        {"trade_date": observed_at.date(), "mode": normalized_mode},
                    ).scalar()
                    or 0
                )
                states, emissions = advance_states(
                    previous_states,
                    candidates,
                    now=observed_at,
                    emitted_today=emitted_today,
                )
                for state in states:
                    _upsert_state(connection, observed_at.date(), state)
                for emission in emissions:
                    content = _render_emission(emission, current)
                    _insert_outbox(
                        connection,
                        trade_date=observed_at.date(),
                        emission=emission,
                        mode=normalized_mode,
                        content=content,
                        now=observed_at,
                    )
                emissions_count = len(emissions)
                connection.commit()
        except Exception:
            connection.rollback()
            raise

    delivered = drain_outbox(main_engine, mode=normalized_mode, now=observed_at)
    return {
        "status": "duplicate" if duplicate else "ok",
        "mode": normalized_mode,
        "observation_id": observation_id,
        "source_receipt_id": str(snapshot.receipt.get("receipt_id") or ""),
        "emissions": emissions_count,
        "delivered": delivered,
        "warnings": warnings,
    }


__all__ = [
    "BENCHMARKS",
    "DeliveryDispatchError",
    "IntradayAlertError",
    "QualityGateError",
    "ValidatedSnapshot",
    "advance_states",
    "aggregate_sectors",
    "compute_market_metrics",
    "compute_style",
    "drain_outbox",
    "fetch_benchmark_quotes",
    "load_validated_snapshot",
    "make_observation_id",
    "prepare_benchmark_snapshot",
    "run_intraday_scan",
    "session_minute",
    "validate_qmt_snapshot",
]
