"""Tamper-evident, fail-closed coverage evidence for QMT history.

The QMT download APIs returning without an exception is not evidence that a
full market day was captured.  This module independently compares every
session with the immutable stock-catalog universe and produces one canonical
manifest plus one hash-bound entity row per expected (or unexpected) code.

Only ``EXACT`` manifests are strategy eligible.  ``INCOMPLETE`` records are
useful operational evidence, but must never be interpreted as usable history.
Provider limitations are represented explicitly as ``UNAVAILABLE`` manifests;
they are not converted into synthetic market facts.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text


COVERAGE_SCHEMA = "probiga.qmt-history-coverage.v1"
COVERAGE_TABLE = "qmt_history_coverage_manifest"
COVERAGE_ENTITY_TABLE = "qmt_history_coverage_entity"
COVERAGE_TABLE_NAMES = (COVERAGE_TABLE, COVERAGE_ENTITY_TABLE)
COVERAGE_TRIGGER_NAMES = (
    "trg_qmt_history_coverage_no_update",
    "trg_qmt_history_coverage_no_delete",
    "trg_qmt_history_coverage_entity_no_update",
    "trg_qmt_history_coverage_entity_no_delete",
)
COVERAGE_EXACT = "EXACT"
COVERAGE_INCOMPLETE = "INCOMPLETE"
COVERAGE_UNAVAILABLE = "UNAVAILABLE"
STRATEGY_ELIGIBLE_STATUSES = frozenset({COVERAGE_EXACT})
UNAVAILABLE_CAPABILITY_STATUSES = frozenset(
    {"NO_DATA", "NOT_AUTHORIZED", "UNSUPPORTED_CLIENT"}
)

DATASET_STOCK_DAILY = "stock_daily"
DATASET_STOCK_MINUTE = "stock_minute"
SUPPORTED_DATASETS = frozenset(
    {DATASET_STOCK_DAILY, DATASET_STOCK_MINUTE}
)

QMT_MINUTE_GRID_PROFILE = "CN_A_SHARE_QMT_NATIVE_241_V1"
QMT_MINUTE_GRID_PREFIX = QMT_MINUTE_GRID_PROFILE + "_PREFIX_"
QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH = (
    "61f40868016c42b63414c65bb0cc47bf0d30f51a45e188a5198622e8edc298dd"
)
_HASH = re.compile(r"^[0-9a-f]{64}$")
_CODE = re.compile(r"^(0|3|4|6|8|9)[0-9]{5}$")
_IDENTITY = re.compile(r"^[A-Za-z0-9_.:@-]{1,160}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PERIOD_BY_DATASET = {
    DATASET_STOCK_DAILY: "1d",
    DATASET_STOCK_MINUTE: "1m",
}


class QmtHistoryCoverageError(RuntimeError):
    """Coverage evidence is malformed, tampered, or not exact."""


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _iso_date(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        raw = str(value or "").strip()[:10]
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise QmtHistoryCoverageError(
                f"{field} is not an ISO date"
            ) from exc
    return parsed.isoformat()


def _iso_datetime(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace(" ", "T")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise QmtHistoryCoverageError(
                f"{field} is not an ISO datetime"
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0).isoformat(timespec="seconds")


def _normalized_code(value: Any) -> str:
    code = str(value or "").strip().split(".", 1)[0].zfill(6)
    if _CODE.fullmatch(code) is None:
        raise QmtHistoryCoverageError(f"invalid A-share stock code: {value!r}")
    return code


def _normalized_code_set(values: Iterable[Any]) -> list[str]:
    codes = [_normalized_code(value) for value in values]
    if not codes:
        raise QmtHistoryCoverageError("expected stock universe is empty")
    if len(codes) != len(set(codes)):
        raise QmtHistoryCoverageError("expected stock universe contains duplicates")
    return sorted(codes)


def _identity(value: Any, *, field: str, maximum: int = 160) -> str:
    raw = str(value or "").strip()
    if (
        not raw
        or len(raw) > maximum
        or _IDENTITY.fullmatch(raw) is None
    ):
        raise QmtHistoryCoverageError(f"{field} is not a safe identity")
    return raw


def _hash(value: Any, *, field: str) -> str:
    raw = str(value or "").strip().lower()
    if _HASH.fullmatch(raw) is None:
        raise QmtHistoryCoverageError(f"{field} is not a SHA-256 digest")
    return raw


def _finite_decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise QmtHistoryCoverageError(f"{field} is not finite") from exc
    if not result.is_finite():
        raise QmtHistoryCoverageError(f"{field} is not finite")
    return result


def _optional_nonnegative(value: Any, *, field: str) -> Decimal | None:
    if value in (None, ""):
        return None
    result = _finite_decimal(value, field=field)
    if result < 0:
        raise QmtHistoryCoverageError(f"{field} cannot be negative")
    return result


def minute_time_grid(
    profile: str = QMT_MINUTE_GRID_PROFILE,
) -> tuple[str, ...]:
    """Return the exact native-QMT A-share one-minute bar-start grid."""

    if (
        profile != QMT_MINUTE_GRID_PROFILE
        and not str(profile).startswith(QMT_MINUTE_GRID_PREFIX)
    ):
        raise QmtHistoryCoverageError(
            f"unsupported QMT minute grid profile: {profile!r}"
        )
    # Calibrated against a single-batch native Guojin QMT capture
    # (gj_big_qmt_inner, 000001, 2026-07-21).  QMT labels the first
    # afternoon bar 13:01 and includes the 11:30/15:00 endpoints.
    values: list[str] = []
    for start, count in ((time(9, 30), 121), (time(13, 1), 120)):
        cursor = datetime.combine(date(2000, 1, 1), start)
        values.extend(
            (cursor + timedelta(minutes=offset)).strftime("%H:%M:%S")
            for offset in range(count)
        )
    if canonical_digest(values) != QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH:
        raise QmtHistoryCoverageError("native QMT minute grid fixture differs")
    if profile != QMT_MINUTE_GRID_PROFILE:
        suffix = str(profile)[len(QMT_MINUTE_GRID_PREFIX):]
        if len(suffix) != 6 or not suffix.isdigit():
            raise QmtHistoryCoverageError(
                f"unsupported QMT minute grid profile: {profile!r}"
            )
        cutoff = f"{suffix[:2]}:{suffix[2:4]}:{suffix[4:]}"
        if cutoff not in values:
            raise QmtHistoryCoverageError(
                f"unsupported QMT minute prefix cutoff: {cutoff!r}"
            )
        values = values[: values.index(cutoff) + 1]
    return tuple(values)


def minute_grid_profile_for_capture(
    *,
    trade_date: Any,
    captured_at: Any,
) -> str:
    """Freeze a full-session grid or the exact observable intraday prefix."""

    normalized_date = _iso_date(trade_date, field="trade_date")
    normalized_capture = _iso_datetime(captured_at, field="captured_at")
    captured = datetime.fromisoformat(normalized_capture)
    target = date.fromisoformat(normalized_date)
    if target > captured.date():
        raise QmtHistoryCoverageError("minute capture precedes trade date")
    if target < captured.date():
        return QMT_MINUTE_GRID_PROFILE
    full_grid = minute_time_grid()
    cutoff = captured.strftime("%H:%M:%S")
    observable = [value for value in full_grid if value <= cutoff]
    if not observable:
        raise QmtHistoryCoverageError(
            "intraday minute capture precedes the first native bar"
        )
    last_observable = observable[-1]
    if last_observable == full_grid[-1]:
        return QMT_MINUTE_GRID_PROFILE
    return QMT_MINUTE_GRID_PREFIX + last_observable.replace(":", "")


def _row_trade_time(value: Any, *, trade_date: str) -> str:
    normalized = _iso_datetime(value, field="trade_time")
    parsed = datetime.fromisoformat(normalized)
    if parsed.date().isoformat() != trade_date or parsed.second != 0:
        raise QmtHistoryCoverageError(
            "minute row is outside the exact trade date or minute boundary"
        )
    return parsed.strftime("%H:%M:%S")


def _normalize_context(
    *,
    dataset: str,
    trade_date: Any,
    provider: Any,
    run_id: Any,
    catalog_batch_id: Any,
    catalog_manifest_hash: Any,
    calendar_batch_id: Any,
    calendar_manifest_hash: Any,
    source_batch_id: Any,
    captured_at: Any,
) -> dict[str, str]:
    normalized_dataset = str(dataset or "").strip()
    if normalized_dataset not in SUPPORTED_DATASETS:
        raise QmtHistoryCoverageError(
            f"unsupported QMT coverage dataset: {dataset!r}"
        )
    normalized_date = _iso_date(trade_date, field="trade_date")
    normalized_captured_at = _iso_datetime(captured_at, field="captured_at")
    if datetime.fromisoformat(normalized_captured_at).date() < date.fromisoformat(
        normalized_date
    ):
        raise QmtHistoryCoverageError("captured_at precedes trade_date")
    return {
        "schema": COVERAGE_SCHEMA,
        "dataset": normalized_dataset,
        "period": _PERIOD_BY_DATASET[normalized_dataset],
        "trade_date": normalized_date,
        "provider": _identity(provider, field="provider", maximum=32),
        "run_id": _identity(run_id, field="run_id", maximum=64),
        "catalog_batch_id": _identity(
            catalog_batch_id, field="catalog_batch_id", maximum=64
        ),
        "catalog_manifest_hash": _hash(
            catalog_manifest_hash, field="catalog_manifest_hash"
        ),
        "calendar_batch_id": _identity(
            calendar_batch_id, field="calendar_batch_id", maximum=64
        ),
        "calendar_manifest_hash": _hash(
            calendar_manifest_hash, field="calendar_manifest_hash"
        ),
        "source_batch_id": _identity(
            source_batch_id, field="source_batch_id", maximum=64
        ),
        "captured_at": normalized_captured_at,
    }


def _entity_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    canonical = {
        "stock_code": _normalized_code(payload.get("stock_code")),
        "expected_state": str(payload.get("expected_state") or ""),
        "classification": str(payload.get("classification") or ""),
        "bar_count": int(payload.get("bar_count") or 0),
        "time_set_hash": _hash(
            payload.get("time_set_hash"), field="entity.time_set_hash"
        ),
        "first_time": str(payload.get("first_time") or ""),
        "last_time": str(payload.get("last_time") or ""),
        "source_row_hash": _hash(
            payload.get("source_row_hash"), field="entity.source_row_hash"
        ),
    }
    if canonical["expected_state"] not in {
        "TRADED",
        "NO_TRADE",
        "UNEXPECTED",
    }:
        raise QmtHistoryCoverageError("entity expected_state is invalid")
    if canonical["classification"] not in {
        "TRADED",
        "NO_TRADE",
        "MISSING",
        "PARTIAL",
        "UNEXPECTED",
    }:
        raise QmtHistoryCoverageError("entity classification is invalid")
    if canonical["bar_count"] < 0:
        raise QmtHistoryCoverageError("entity bar_count cannot be negative")
    row_hash = canonical_digest(canonical)
    supplied = str(payload.get("row_hash") or "").lower()
    if supplied and supplied != row_hash:
        raise QmtHistoryCoverageError("entity row_hash differs")
    return {**canonical, "row_hash": row_hash}


def _finish_bundle(
    *,
    context: Mapping[str, str],
    expected_codes: Sequence[str],
    expected_traded_codes: Sequence[str],
    actual_traded_codes: Sequence[str],
    no_trade_codes: Sequence[str],
    entities: Sequence[Mapping[str, Any]],
    reasons: Sequence[Mapping[str, Any]],
    grid_profile: str = "",
) -> dict[str, Any]:
    normalized_entities = sorted(
        (_entity_row(row) for row in entities),
        key=lambda row: row["stock_code"],
    )
    if len(normalized_entities) != len(
        {row["stock_code"] for row in normalized_entities}
    ):
        raise QmtHistoryCoverageError("coverage entities contain duplicate codes")
    supplied_reasons = list(reasons)
    # A system clock after 15:00 is not a source-completion receipt.  Until a
    # provider-native end-of-session watermark is independently persisted,
    # same-local-date data is never certified EXACT.  The next calendar day is
    # the earliest historical certification boundary.
    if str(context["captured_at"])[:10] == str(context["trade_date"]):
        supplied_reasons.append(
            {
                "code": "SAME_DAY_SOURCE_NOT_FINAL",
                "stock_code": "",
                "detail": "cross_day_certification_required",
            }
        )
    normalized_reasons = sorted(
        (
            {
                "code": str(row.get("code") or "")[:64],
                "stock_code": str(row.get("stock_code") or "")[:16],
                "detail": str(row.get("detail") or "")[:256],
            }
            for row in supplied_reasons
        ),
        key=lambda row: (row["code"], row["stock_code"], row["detail"]),
    )
    status = COVERAGE_EXACT if not normalized_reasons else COVERAGE_INCOMPLETE
    entity_root = canonical_digest(
        [row["row_hash"] for row in normalized_entities]
    )
    expected = sorted(expected_codes)
    expected_traded = sorted(expected_traded_codes)
    actual_traded = sorted(actual_traded_codes)
    no_trade = sorted(no_trade_codes)
    manifest_core = {
        **dict(context),
        "status": status,
        "strategy_eligible": status in STRATEGY_ELIGIBLE_STATUSES,
        "grid_profile": grid_profile,
        "expected_entity_count": len(expected),
        "entity_count": len(normalized_entities),
        "expected_traded_count": len(expected_traded),
        "actual_traded_count": len(actual_traded),
        "no_trade_count": len(no_trade),
        "bar_count": sum(int(row["bar_count"]) for row in normalized_entities),
        "expected_entity_set_hash": canonical_digest(expected),
        "expected_traded_set_hash": canonical_digest(expected_traded),
        "actual_traded_set_hash": canonical_digest(actual_traded),
        "no_trade_set_hash": canonical_digest(no_trade),
        "entity_root_hash": entity_root,
        "reason_count": len(normalized_reasons),
        "reasons": normalized_reasons,
    }
    manifest_hash = canonical_digest(manifest_core)
    return {
        "manifest": {
            **manifest_core,
            "manifest_hash": manifest_hash,
            "manifest_json": _canonical_json(manifest_core),
        },
        "entities": [
            {**row, "manifest_hash": manifest_hash}
            for row in normalized_entities
        ],
    }


def assess_daily_coverage(
    *,
    expected_codes: Iterable[Any],
    rows: Iterable[Mapping[str, Any]],
    trade_date: Any,
    provider: Any,
    run_id: Any,
    catalog_batch_id: Any,
    catalog_manifest_hash: Any,
    calendar_batch_id: Any,
    calendar_manifest_hash: Any,
    source_batch_id: Any,
    captured_at: Any,
) -> dict[str, Any]:
    """Assess one native-QMT daily session against its exact PIT universe."""

    context = _normalize_context(
        dataset=DATASET_STOCK_DAILY,
        trade_date=trade_date,
        provider=provider,
        run_id=run_id,
        catalog_batch_id=catalog_batch_id,
        catalog_manifest_hash=catalog_manifest_hash,
        calendar_batch_id=calendar_batch_id,
        calendar_manifest_hash=calendar_manifest_hash,
        source_batch_id=source_batch_id,
        captured_at=captured_at,
    )
    expected = _normalized_code_set(expected_codes)
    expected_set = set(expected)
    rows_by_code: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    row_errors: dict[str, list[str]] = defaultdict(list)
    for raw in rows:
        try:
            code = _normalized_code(raw.get("stock_code"))
        except QmtHistoryCoverageError:
            row_errors[""].append("INVALID_STOCK_CODE")
            continue
        rows_by_code[code].append(raw)
        if _iso_date(raw.get("trade_date"), field="row.trade_date") != context[
            "trade_date"
        ]:
            row_errors[code].append("WRONG_TRADE_DATE")
        if str(raw.get("provider") or raw.get("data_source") or "") != context[
            "provider"
        ]:
            row_errors[code].append("WRONG_PROVIDER")
        if str(raw.get("period") or "1d") != "1d":
            row_errors[code].append("WRONG_PERIOD")
        if int(raw.get("k_type") or 1) != 1:
            row_errors[code].append("WRONG_K_TYPE")
        if int(raw.get("adjust_type") or 0) != 0:
            row_errors[code].append("ADJUSTED_DAILY_ROW")
        if str(raw.get("pre_close_origin") or "") != "NATIVE_QMT":
            row_errors[code].append("NON_NATIVE_PRE_CLOSE")
        if str(raw.get("batch_id") or "") != context["source_batch_id"]:
            row_errors[code].append("WRONG_SOURCE_BATCH")
        try:
            prices = [
                _finite_decimal(raw.get(field), field=f"daily[{code}].{field}")
                for field in ("open", "high", "low", "close", "pre_close")
            ]
            if any(value <= 0 for value in prices):
                raise QmtHistoryCoverageError("daily price must be positive")
            if prices[1] < max(prices[0], prices[3]) or prices[2] > min(
                prices[0], prices[3]
            ):
                raise QmtHistoryCoverageError("daily OHLC range differs")
        except QmtHistoryCoverageError:
            row_errors[code].append("INVALID_DAILY_PRICE")
        try:
            if (
                _optional_nonnegative(
                    raw.get("volume"), field=f"daily[{code}].volume"
                )
                is None
                or _optional_nonnegative(
                    raw.get("amount"), field=f"daily[{code}].amount"
                )
                is None
            ):
                raise QmtHistoryCoverageError("daily activity is missing")
        except QmtHistoryCoverageError:
            row_errors[code].append("INVALID_DAILY_ACTIVITY")

    reasons: list[dict[str, str]] = []
    entities: list[dict[str, Any]] = []
    all_codes = sorted(expected_set | set(rows_by_code))
    for code in all_codes:
        observed = rows_by_code.get(code, [])
        classification = "TRADED"
        if code not in expected_set:
            classification = "UNEXPECTED"
            reasons.append(
                {"code": "UNEXPECTED_CODE", "stock_code": code, "detail": ""}
            )
        elif not observed:
            classification = "MISSING"
            reasons.append(
                {"code": "MISSING_CODE", "stock_code": code, "detail": ""}
            )
        elif len(observed) != 1:
            classification = "PARTIAL"
            reasons.append(
                {
                    "code": "DUPLICATE_DAILY_ROW",
                    "stock_code": code,
                    "detail": str(len(observed)),
                }
            )
        if row_errors.get(code):
            classification = "PARTIAL"
            for reason in sorted(set(row_errors[code])):
                reasons.append(
                    {"code": reason, "stock_code": code, "detail": ""}
                )
        source_payload = [
            {
                key: str(row.get(key) if row.get(key) is not None else "")
                for key in (
                    "stock_code",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "pre_close",
                    "pre_close_origin",
                    "provider",
                    "data_source",
                    "batch_id",
                )
            }
            for row in observed
        ]
        source_payload.sort(key=_canonical_json)
        entities.append(
            {
                "stock_code": code,
                "expected_state": (
                    "UNEXPECTED" if code not in expected_set else "TRADED"
                ),
                "classification": classification,
                "bar_count": len(observed),
                "time_set_hash": canonical_digest(
                    [context["trade_date"]] if observed else []
                ),
                "first_time": context["trade_date"] if observed else "",
                "last_time": context["trade_date"] if observed else "",
                "source_row_hash": canonical_digest(source_payload),
            }
        )
    for reason in sorted(set(row_errors.get("", []))):
        reasons.append({"code": reason, "stock_code": "", "detail": ""})
    if not rows_by_code:
        reasons.append(
            {"code": "EMPTY_SOURCE_ROWS", "stock_code": "", "detail": ""}
        )
    actual = sorted(set(rows_by_code) & expected_set)
    return _finish_bundle(
        context=context,
        expected_codes=expected,
        expected_traded_codes=expected,
        actual_traded_codes=actual,
        no_trade_codes=(),
        entities=entities,
        reasons=reasons,
    )


def _daily_trade_classification(
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_codes: Sequence[str],
    trade_date: str,
    provider: str,
    source_batch_id: str,
) -> tuple[set[str], set[str], list[dict[str, str]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    reasons: list[dict[str, str]] = []
    for raw in rows:
        try:
            code = _normalized_code(raw.get("stock_code"))
        except QmtHistoryCoverageError:
            reasons.append(
                {"code": "DAILY_INVALID_CODE", "stock_code": "", "detail": ""}
            )
            continue
        grouped[code].append(raw)
    active: set[str] = set()
    no_trade: set[str] = set()
    for code in expected_codes:
        observed = grouped.get(code, [])
        if len(observed) != 1:
            reasons.append(
                {
                    "code": "DAILY_EVIDENCE_COUNT",
                    "stock_code": code,
                    "detail": str(len(observed)),
                }
            )
            active.add(code)
            continue
        row = observed[0]
        if (
            _iso_date(row.get("trade_date"), field="daily.trade_date")
            != trade_date
            or str(row.get("pre_close_origin") or "") != "NATIVE_QMT"
            or int(row.get("adjust_type") or 0) != 0
            or str(row.get("provider") or row.get("data_source") or "")
            != provider
            or str(row.get("batch_id") or "") != source_batch_id
        ):
            reasons.append(
                {
                    "code": "DAILY_EVIDENCE_NOT_NATIVE_RAW",
                    "stock_code": code,
                    "detail": "",
                }
            )
            active.add(code)
            continue
        try:
            volume = _optional_nonnegative(
                row.get("volume"), field=f"daily[{code}].volume"
            )
            amount = _optional_nonnegative(
                row.get("amount"), field=f"daily[{code}].amount"
            )
        except QmtHistoryCoverageError:
            volume = None
            amount = None
        if volume is None or amount is None:
            reasons.append(
                {
                    "code": "DAILY_ACTIVITY_MISSING",
                    "stock_code": code,
                    "detail": "",
                }
            )
            active.add(code)
        elif volume == 0 and amount == 0:
            no_trade.add(code)
        elif volume > 0 and amount > 0:
            active.add(code)
        else:
            reasons.append(
                {
                    "code": "DAILY_ACTIVITY_CONFLICT",
                    "stock_code": code,
                    "detail": "",
                }
            )
            active.add(code)
    unexpected_daily = sorted(set(grouped) - set(expected_codes))
    for code in unexpected_daily:
        reasons.append(
            {"code": "DAILY_UNEXPECTED_CODE", "stock_code": code, "detail": ""}
        )
    return active, no_trade, reasons


def assess_minute_coverage(
    *,
    expected_codes: Iterable[Any],
    daily_rows: Iterable[Mapping[str, Any]],
    minute_rows: Iterable[Mapping[str, Any]],
    trade_date: Any,
    provider: Any,
    daily_provider: Any,
    run_id: Any,
    catalog_batch_id: Any,
    catalog_manifest_hash: Any,
    calendar_batch_id: Any,
    calendar_manifest_hash: Any,
    source_batch_id: Any,
    daily_source_batch_id: Any,
    captured_at: Any,
    grid_profile: str = QMT_MINUTE_GRID_PROFILE,
) -> dict[str, Any]:
    """Assess exact-date QMT 1m history, including native no-trade proof."""

    context = _normalize_context(
        dataset=DATASET_STOCK_MINUTE,
        trade_date=trade_date,
        provider=provider,
        run_id=run_id,
        catalog_batch_id=catalog_batch_id,
        catalog_manifest_hash=catalog_manifest_hash,
        calendar_batch_id=calendar_batch_id,
        calendar_manifest_hash=calendar_manifest_hash,
        source_batch_id=source_batch_id,
        captured_at=captured_at,
    )
    expected = _normalized_code_set(expected_codes)
    expected_set = set(expected)
    normalized_daily_provider = _identity(
        daily_provider, field="daily_provider", maximum=32
    )
    normalized_daily_source_batch_id = _identity(
        daily_source_batch_id,
        field="daily_source_batch_id",
        maximum=64,
    )
    grid = minute_time_grid(grid_profile)
    grid_set = set(grid)
    grid_hash = canonical_digest(list(grid))
    active, no_trade, reasons = _daily_trade_classification(
        daily_rows,
        expected_codes=expected,
        trade_date=context["trade_date"],
        provider=normalized_daily_provider,
        source_batch_id=normalized_daily_source_batch_id,
    )

    rows_by_code: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    times_by_code: dict[str, list[str]] = defaultdict(list)
    row_errors: dict[str, list[str]] = defaultdict(list)
    for raw in minute_rows:
        try:
            code = _normalized_code(raw.get("stock_code"))
        except QmtHistoryCoverageError:
            row_errors[""].append("INVALID_STOCK_CODE")
            continue
        rows_by_code[code].append(raw)
        try:
            row_time = _row_trade_time(
                raw.get("trade_time"), trade_date=context["trade_date"]
            )
            times_by_code[code].append(row_time)
        except QmtHistoryCoverageError:
            row_errors[code].append("WRONG_TRADE_TIME")
        if str(raw.get("provider") or raw.get("data_source") or "") != context[
            "provider"
        ]:
            row_errors[code].append("WRONG_PROVIDER")
        if str(raw.get("period") or "1m") != "1m":
            row_errors[code].append("WRONG_PERIOD")
        if str(raw.get("batch_id") or "") != context["run_id"]:
            row_errors[code].append("WRONG_CAPTURE_RUN")
        try:
            volume = _optional_nonnegative(
                raw.get("volume"), field=f"minute[{code}].volume"
            )
            amount = _optional_nonnegative(
                raw.get("amount"), field=f"minute[{code}].amount"
            )
            price = _finite_decimal(
                raw.get("price"), field=f"minute[{code}].price"
            )
            if volume is None or amount is None:
                raise QmtHistoryCoverageError("minute activity is missing")
            if price <= 0:
                raise QmtHistoryCoverageError("minute price is not positive")
            if raw.get("avg_price") not in (None, "") and _finite_decimal(
                raw.get("avg_price"), field=f"minute[{code}].avg_price"
            ) <= 0:
                raise QmtHistoryCoverageError(
                    "minute average price is not positive"
                )
        except QmtHistoryCoverageError:
            row_errors[code].append("INVALID_MINUTE_VALUE")

    entities: list[dict[str, Any]] = []
    all_codes = sorted(expected_set | set(rows_by_code))
    for code in all_codes:
        observed = rows_by_code.get(code, [])
        times = times_by_code.get(code, [])
        unique_times = sorted(set(times))
        classification = "TRADED"
        if code not in expected_set:
            classification = "UNEXPECTED"
            reasons.append(
                {"code": "UNEXPECTED_CODE", "stock_code": code, "detail": ""}
            )
        elif code in no_trade:
            if observed:
                classification = "PARTIAL"
                reasons.append(
                    {
                        "code": "NO_TRADE_CODE_HAS_BARS",
                        "stock_code": code,
                        "detail": str(len(observed)),
                    }
                )
            else:
                classification = "NO_TRADE"
        elif not observed:
            classification = "MISSING"
            reasons.append(
                {"code": "MISSING_CODE", "stock_code": code, "detail": ""}
            )
        elif len(times) != len(unique_times):
            classification = "PARTIAL"
            reasons.append(
                {
                    "code": "DUPLICATE_MINUTE_TIME",
                    "stock_code": code,
                    "detail": f"{len(times)}/{len(unique_times)}",
                }
            )
        elif set(unique_times) != grid_set:
            classification = "PARTIAL"
            missing_count = len(grid_set - set(unique_times))
            unexpected_count = len(set(unique_times) - grid_set)
            reasons.append(
                {
                    "code": "MINUTE_GRID_MISMATCH",
                    "stock_code": code,
                    "detail": f"missing={missing_count},unexpected={unexpected_count}",
                }
            )
        if row_errors.get(code):
            classification = "PARTIAL"
            for reason in sorted(set(row_errors[code])):
                reasons.append(
                    {"code": reason, "stock_code": code, "detail": ""}
                )
        source_payload = [
            {
                key: str(row.get(key) if row.get(key) is not None else "")
                for key in (
                    "stock_code",
                    "trade_time",
                    "price",
                    "avg_price",
                    "volume",
                    "amount",
                    "provider",
                    "data_source",
                    "batch_id",
                )
            }
            for row in observed
        ]
        source_payload.sort(
            key=lambda row: (row["trade_time"], _canonical_json(row))
        )
        entities.append(
            {
                "stock_code": code,
                "expected_state": (
                    "UNEXPECTED"
                    if code not in expected_set
                    else "NO_TRADE"
                    if code in no_trade
                    else "TRADED"
                ),
                "classification": classification,
                "bar_count": len(observed),
                "time_set_hash": canonical_digest(unique_times),
                "first_time": unique_times[0] if unique_times else "",
                "last_time": unique_times[-1] if unique_times else "",
                "source_row_hash": canonical_digest(source_payload),
            }
        )
    for reason in sorted(set(row_errors.get("", []))):
        reasons.append({"code": reason, "stock_code": "", "detail": ""})
    if not rows_by_code and active:
        reasons.append(
            {"code": "EMPTY_SOURCE_ROWS", "stock_code": "", "detail": ""}
        )
    actual_traded = sorted(set(rows_by_code) & active)
    bundle = _finish_bundle(
        context=context,
        expected_codes=expected,
        expected_traded_codes=sorted(active),
        actual_traded_codes=actual_traded,
        no_trade_codes=sorted(no_trade),
        entities=entities,
        reasons=reasons,
        grid_profile=grid_profile,
    )
    bundle["manifest"]["minute_grid_hash"] = grid_hash
    # The grid hash is part of the evidence and therefore must be covered by
    # the manifest digest.  Re-finalize after adding the frozen profile root.
    core = {
        key: value
        for key, value in bundle["manifest"].items()
        if key not in {"manifest_hash", "manifest_json"}
    }
    manifest_hash = canonical_digest(core)
    bundle["manifest"] = {
        **core,
        "manifest_hash": manifest_hash,
        "manifest_json": _canonical_json(core),
    }
    bundle["entities"] = [
        {**row, "manifest_hash": manifest_hash}
        for row in bundle["entities"]
    ]
    return bundle


def combine_minute_coverage_partitions(
    *,
    expected_codes: Iterable[Any],
    partitions: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine independently bounded minute partitions into one day proof.

    Full-market minute history is too large to materialize safely in one
    Python object.  Callers may assess bounded code partitions, then combine
    only their entity roots/reasons.  Every partition must bind the exact same
    provider run, catalog, calendar, grid and capture time.
    """

    expected = _normalized_code_set(expected_codes)
    validated: list[tuple[dict[str, Any], Mapping[str, Any]]] = []
    for bundle in partitions:
        manifest = validate_coverage_bundle(bundle)
        if manifest.get("dataset") != DATASET_STOCK_MINUTE:
            raise QmtHistoryCoverageError("minute coverage partition dataset differs")
        validated.append((manifest, bundle))
    if not validated:
        raise QmtHistoryCoverageError("minute coverage partitions are empty")
    first = validated[0][0]
    context_keys = (
        "schema",
        "dataset",
        "period",
        "trade_date",
        "provider",
        "run_id",
        "catalog_batch_id",
        "catalog_manifest_hash",
        "calendar_batch_id",
        "calendar_manifest_hash",
        "source_batch_id",
        "captured_at",
        "grid_profile",
        "minute_grid_hash",
    )
    for manifest, _bundle in validated[1:]:
        if any(manifest.get(key) != first.get(key) for key in context_keys):
            raise QmtHistoryCoverageError("minute coverage partition context differs")

    entities: list[dict[str, Any]] = []
    reasons: list[dict[str, Any]] = []
    for manifest, bundle in validated:
        entities.extend(dict(row) for row in bundle["entities"])
        reasons.extend(dict(row) for row in manifest.get("reasons") or [])
    codes = [str(row.get("stock_code") or "") for row in entities]
    if len(codes) != len(set(codes)):
        raise QmtHistoryCoverageError("minute coverage partitions overlap")
    entity_expected = sorted(
        str(row["stock_code"])
        for row in entities
        if str(row.get("expected_state") or "") != "UNEXPECTED"
    )
    if entity_expected != expected:
        raise QmtHistoryCoverageError(
            "minute coverage partitions do not cover the exact universe"
        )
    expected_traded = sorted(
        str(row["stock_code"])
        for row in entities
        if str(row.get("expected_state") or "") == "TRADED"
    )
    actual_traded = sorted(
        str(row["stock_code"])
        for row in entities
        if str(row.get("expected_state") or "") == "TRADED"
        and int(row.get("bar_count") or 0) > 0
    )
    no_trade = sorted(
        str(row["stock_code"])
        for row in entities
        if str(row.get("expected_state") or "") == "NO_TRADE"
    )
    context = {key: first[key] for key in _normalize_context(
        dataset=first["dataset"],
        trade_date=first["trade_date"],
        provider=first["provider"],
        run_id=first["run_id"],
        catalog_batch_id=first["catalog_batch_id"],
        catalog_manifest_hash=first["catalog_manifest_hash"],
        calendar_batch_id=first["calendar_batch_id"],
        calendar_manifest_hash=first["calendar_manifest_hash"],
        source_batch_id=first["source_batch_id"],
        captured_at=first["captured_at"],
    )}
    combined = _finish_bundle(
        context=context,
        expected_codes=expected,
        expected_traded_codes=expected_traded,
        actual_traded_codes=actual_traded,
        no_trade_codes=no_trade,
        entities=entities,
        reasons=reasons,
        grid_profile=str(first["grid_profile"]),
    )
    combined["manifest"]["minute_grid_hash"] = str(first["minute_grid_hash"])
    core = {
        key: value
        for key, value in combined["manifest"].items()
        if key not in {"manifest_hash", "manifest_json"}
    }
    manifest_hash = canonical_digest(core)
    combined["manifest"] = {
        **core,
        "manifest_hash": manifest_hash,
        "manifest_json": _canonical_json(core),
    }
    combined["entities"] = [
        {**row, "manifest_hash": manifest_hash}
        for row in combined["entities"]
    ]
    validate_coverage_bundle(combined)
    return combined


def unavailable_coverage_bundle(
    *,
    dataset: str,
    trade_date: Any,
    provider: Any,
    capability_key: Any,
    capability_status: Any,
    probed_at: Any,
    reason: Any,
) -> dict[str, Any]:
    """Represent an unavailable licensed/provider dataset without fabricating rows."""

    normalized_dataset = str(dataset or "").strip()
    normalized_status = str(capability_status or "").strip().upper()
    if normalized_status not in UNAVAILABLE_CAPABILITY_STATUSES:
        raise QmtHistoryCoverageError(
            "unavailable coverage requires an explicit provider limitation"
        )
    normalized_date = _iso_date(trade_date, field="trade_date")
    normalized_probe = _iso_datetime(probed_at, field="probed_at")
    empty_hash = canonical_digest([])
    normalized_reason = str(reason or "").strip()[:512]
    if not normalized_reason:
        raise QmtHistoryCoverageError(
            "unavailable coverage requires a provider reason"
        )
    core = {
        "schema": COVERAGE_SCHEMA,
        "dataset": normalized_dataset,
        "period": "",
        "trade_date": normalized_date,
        "provider": _identity(provider, field="provider", maximum=32),
        "status": COVERAGE_UNAVAILABLE,
        "strategy_eligible": False,
        "run_id": "",
        "catalog_batch_id": None,
        "catalog_manifest_hash": None,
        "calendar_batch_id": None,
        "calendar_manifest_hash": None,
        "source_batch_id": "",
        "grid_profile": "",
        "capability_key": _identity(
            capability_key, field="capability_key", maximum=160
        ),
        "capability_status": normalized_status,
        "probed_at": normalized_probe,
        "reason": normalized_reason,
        "expected_entity_count": 0,
        "entity_count": 0,
        "expected_traded_count": 0,
        "actual_traded_count": 0,
        "no_trade_count": 0,
        "bar_count": 0,
        "expected_entity_set_hash": empty_hash,
        "expected_traded_set_hash": empty_hash,
        "actual_traded_set_hash": empty_hash,
        "no_trade_set_hash": empty_hash,
        "entity_root_hash": empty_hash,
        "reason_count": 1,
        "reasons": [
            {
                "code": normalized_status,
                "stock_code": "",
                "detail": normalized_reason[:256],
            }
        ],
        "captured_at": normalized_probe,
    }
    manifest_hash = canonical_digest(core)
    return {
        "manifest": {
            **core,
            "manifest_hash": manifest_hash,
            "manifest_json": _canonical_json(core),
        },
        "entities": [],
    }


def validate_coverage_bundle(bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild every hash/count and return the canonical manifest."""

    if not isinstance(bundle, Mapping):
        raise QmtHistoryCoverageError("coverage bundle must be an object")
    raw_manifest = bundle.get("manifest")
    raw_entities = bundle.get("entities")
    if not isinstance(raw_manifest, Mapping) or not isinstance(raw_entities, list):
        raise QmtHistoryCoverageError("coverage bundle shape differs")
    manifest = dict(raw_manifest)
    if manifest.get("schema") != COVERAGE_SCHEMA:
        raise QmtHistoryCoverageError("coverage schema differs")
    supplied_hash = _hash(
        manifest.pop("manifest_hash", ""), field="manifest_hash"
    )
    supplied_json = str(manifest.pop("manifest_json", ""))
    if supplied_json != _canonical_json(manifest):
        raise QmtHistoryCoverageError("manifest_json differs")
    if canonical_digest(manifest) != supplied_hash:
        raise QmtHistoryCoverageError("manifest_hash differs")
    status = str(manifest.get("status") or "")
    if status == COVERAGE_UNAVAILABLE:
        empty_hash = canonical_digest([])
        if (
            raw_entities
            or manifest.get("strategy_eligible") is not False
            or str(manifest.get("capability_status") or "")
            not in UNAVAILABLE_CAPABILITY_STATUSES
            or int(manifest.get("entity_count") or 0) != 0
            or any(
                int(manifest.get(field) or 0) != 0
                for field in (
                    "expected_entity_count",
                    "expected_traded_count",
                    "actual_traded_count",
                    "no_trade_count",
                    "bar_count",
                )
            )
            or any(
                manifest.get(field) != empty_hash
                for field in (
                    "expected_entity_set_hash",
                    "expected_traded_set_hash",
                    "actual_traded_set_hash",
                    "no_trade_set_hash",
                    "entity_root_hash",
                )
            )
            or manifest.get("catalog_batch_id") is not None
            or manifest.get("catalog_manifest_hash") is not None
            or manifest.get("calendar_batch_id") is not None
            or manifest.get("calendar_manifest_hash") is not None
            or manifest.get("captured_at") != manifest.get("probed_at")
            or int(manifest.get("reason_count") or 0) != 1
            or not isinstance(manifest.get("reasons"), list)
            or len(manifest.get("reasons") or []) != 1
        ):
            raise QmtHistoryCoverageError("unavailable manifest differs")
        return {**manifest, "manifest_hash": supplied_hash}
    if status not in {COVERAGE_EXACT, COVERAGE_INCOMPLETE}:
        raise QmtHistoryCoverageError("coverage status differs")

    normalized_context = _normalize_context(
        dataset=manifest.get("dataset"),
        trade_date=manifest.get("trade_date"),
        provider=manifest.get("provider"),
        run_id=manifest.get("run_id"),
        catalog_batch_id=manifest.get("catalog_batch_id"),
        catalog_manifest_hash=manifest.get("catalog_manifest_hash"),
        calendar_batch_id=manifest.get("calendar_batch_id"),
        calendar_manifest_hash=manifest.get("calendar_manifest_hash"),
        source_batch_id=manifest.get("source_batch_id"),
        captured_at=manifest.get("captured_at"),
    )
    if any(manifest.get(key) != value for key, value in normalized_context.items()):
        raise QmtHistoryCoverageError("coverage context differs")
    if (
        str(manifest.get("status") or "") == COVERAGE_EXACT
        and str(manifest.get("captured_at") or "")[:10]
        == str(manifest.get("trade_date") or "")
    ):
        raise QmtHistoryCoverageError(
            "same-day coverage has no independent source-completion receipt"
        )

    entities = [_entity_row(row) for row in raw_entities]
    codes = [row["stock_code"] for row in entities]
    if len(codes) != len(set(codes)):
        raise QmtHistoryCoverageError("coverage entity identities are duplicate")
    if any(
        str(row.get("manifest_hash") or "") != supplied_hash
        for row in raw_entities
    ):
        raise QmtHistoryCoverageError("coverage entity manifest binding differs")
    if int(manifest.get("entity_count", -1)) != len(entities):
        raise QmtHistoryCoverageError("coverage entity count differs")
    if int(manifest.get("bar_count", -1)) != sum(
        row["bar_count"] for row in entities
    ):
        raise QmtHistoryCoverageError("coverage bar count differs")
    expected_root = canonical_digest(
        [row["row_hash"] for row in sorted(entities, key=lambda row: row["stock_code"])]
    )
    if manifest.get("entity_root_hash") != expected_root:
        raise QmtHistoryCoverageError("coverage entity root differs")
    expected_codes = sorted(
        row["stock_code"]
        for row in entities
        if row["expected_state"] != "UNEXPECTED"
    )
    expected_traded = sorted(
        row["stock_code"]
        for row in entities
        if row["expected_state"] == "TRADED"
    )
    actual_traded = sorted(
        row["stock_code"]
        for row in entities
        if row["expected_state"] == "TRADED" and row["bar_count"] > 0
    )
    no_trade = sorted(
        row["stock_code"]
        for row in entities
        if row["expected_state"] == "NO_TRADE"
    )
    partition_contract = (
        int(manifest.get("expected_entity_count", -1)) == len(expected_codes)
        and int(manifest.get("expected_traded_count", -1))
        == len(expected_traded)
        and int(manifest.get("actual_traded_count", -1))
        == len(actual_traded)
        and int(manifest.get("no_trade_count", -1)) == len(no_trade)
        and manifest.get("expected_entity_set_hash")
        == canonical_digest(expected_codes)
        and manifest.get("expected_traded_set_hash")
        == canonical_digest(expected_traded)
        and manifest.get("actual_traded_set_hash")
        == canonical_digest(actual_traded)
        and manifest.get("no_trade_set_hash") == canonical_digest(no_trade)
    )
    if not partition_contract:
        raise QmtHistoryCoverageError("coverage entity partition differs")
    reason_count = int(manifest.get("reason_count") or 0)
    reasons = manifest.get("reasons")
    if not isinstance(reasons, list) or len(reasons) != reason_count:
        raise QmtHistoryCoverageError("coverage reason count differs")
    exact = reason_count == 0
    if (
        (status == COVERAGE_EXACT) != exact
        or bool(manifest.get("strategy_eligible")) != exact
    ):
        raise QmtHistoryCoverageError("strategy eligibility differs")
    if exact:
        allowed_classes = {"TRADED", "NO_TRADE"}
        if any(row["classification"] not in allowed_classes for row in entities):
            raise QmtHistoryCoverageError("exact manifest contains non-exact entity")
        if int(manifest.get("expected_entity_count") or 0) != len(entities):
            raise QmtHistoryCoverageError("exact expected entity count differs")
        if manifest.get("expected_entity_set_hash") != canonical_digest(
            sorted(codes)
        ):
            raise QmtHistoryCoverageError("exact expected entity set differs")
        if manifest.get("dataset") == DATASET_STOCK_DAILY:
            expected_day = str(manifest.get("trade_date") or "")
            for row in entities:
                if (
                    row["expected_state"] != "TRADED"
                    or row["classification"] != "TRADED"
                    or row["bar_count"] != 1
                    or row["time_set_hash"]
                    != canonical_digest([expected_day])
                    or row["first_time"] != expected_day
                    or row["last_time"] != expected_day
                ):
                    raise QmtHistoryCoverageError(
                        "exact daily entity contract differs"
                    )
        if manifest.get("dataset") == DATASET_STOCK_MINUTE:
            traded = sorted(
                row["stock_code"]
                for row in entities
                if row["classification"] == "TRADED"
            )
            exact_no_trade = sorted(
                row["stock_code"]
                for row in entities
                if row["classification"] == "NO_TRADE"
            )
            if (
                manifest.get("expected_traded_set_hash")
                != canonical_digest(traded)
                or manifest.get("actual_traded_set_hash")
                != canonical_digest(traded)
                or manifest.get("no_trade_set_hash")
                != canonical_digest(exact_no_trade)
                or int(manifest.get("expected_traded_count") or 0)
                != len(traded)
                or int(manifest.get("actual_traded_count") or 0)
                != len(traded)
                or int(manifest.get("no_trade_count") or 0)
                != len(exact_no_trade)
            ):
                raise QmtHistoryCoverageError("exact minute partition differs")
            grid = minute_time_grid(str(manifest.get("grid_profile") or ""))
            if manifest.get("minute_grid_hash") != canonical_digest(list(grid)):
                raise QmtHistoryCoverageError("exact minute grid hash differs")
            for row in entities:
                if row["classification"] == "TRADED" and (
                    row["bar_count"] != len(grid)
                    or row["time_set_hash"] != canonical_digest(list(grid))
                    or row["first_time"] != grid[0]
                    or row["last_time"] != grid[-1]
                ):
                    raise QmtHistoryCoverageError("exact minute entity grid differs")
                if row["classification"] == "NO_TRADE" and (
                    row["bar_count"] != 0
                    or row["time_set_hash"] != canonical_digest([])
                    or row["first_time"]
                    or row["last_time"]
                ):
                    raise QmtHistoryCoverageError("no-trade entity differs")
    return {**manifest, "manifest_hash": supplied_hash}


def require_exact_coverage(bundle: Mapping[str, Any]) -> dict[str, Any]:
    manifest = validate_coverage_bundle(bundle)
    if (
        manifest.get("status") != COVERAGE_EXACT
        or manifest.get("strategy_eligible") is not True
    ):
        reasons = manifest.get("reasons") or []
        raise QmtHistoryCoverageError(
            "QMT history coverage is not exact: "
            + ",".join(str(row.get("code") or "") for row in reasons[:10])
        )
    return manifest


def validate_coverage_authority(
    connection: Any,
    bundle: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind a self-consistent bundle to immutable catalog/calendar receipts.

    The caller-provided hashes are never treated as authority.  Both receipts
    and every member/session are read back from the append-only source tables,
    validated by their native contract, and then compared with the entity
    partition in the bundle.
    """

    manifest = validate_coverage_bundle(bundle)
    if manifest.get("status") == COVERAGE_UNAVAILABLE:
        return manifest
    from server.common.qmt_stock_catalog import load_stock_catalog
    from server.common.qmt_trade_calendar import load_trade_calendar_receipt

    trade_date = str(manifest["trade_date"])
    decision_known_at = str(manifest["captured_at"]).replace("T", " ")
    try:
        catalog = load_stock_catalog(
            connection,
            batch_id=str(manifest["catalog_batch_id"]),
            decision_known_at=decision_known_at,
        )
        calendar = load_trade_calendar_receipt(
            connection,
            start_date=trade_date,
            end_date=trade_date,
            batch_id=str(manifest["calendar_batch_id"]),
            decision_known_at=decision_known_at,
        )
        authoritative_codes = sorted(catalog.eligible_codes(trade_date))
        sessions = calendar.sessions_between(trade_date, trade_date)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise QmtHistoryCoverageError(
            "coverage authority receipt is unavailable or invalid"
        ) from exc
    if catalog.manifest_hash != manifest.get("catalog_manifest_hash"):
        raise QmtHistoryCoverageError("coverage catalog receipt hash differs")
    if calendar.manifest_hash != manifest.get("calendar_manifest_hash"):
        raise QmtHistoryCoverageError("coverage calendar receipt hash differs")
    if sessions != [trade_date]:
        raise QmtHistoryCoverageError(
            "coverage trade date is not an authoritative session"
        )
    entity_codes = sorted(
        _entity_row(row)["stock_code"]
        for row in bundle["entities"]
        if str(row.get("expected_state") or "") != "UNEXPECTED"
    )
    if entity_codes != authoritative_codes:
        raise QmtHistoryCoverageError(
            "coverage expected universe differs from catalog receipt"
        )
    if manifest.get("expected_entity_set_hash") != canonical_digest(
        authoritative_codes
    ):
        raise QmtHistoryCoverageError(
            "coverage expected universe root differs from catalog receipt"
        )
    return manifest


def coverage_table_ddl_statements() -> tuple[str, ...]:
    """Privileged, one-time MySQL table contracts for coverage evidence."""

    return (
        f"""
        CREATE TABLE IF NOT EXISTS {COVERAGE_TABLE} (
            manifest_hash CHAR(64) NOT NULL PRIMARY KEY,
            schema_version VARCHAR(64) NOT NULL,
            dataset VARCHAR(64) NOT NULL,
            period VARCHAR(16) NOT NULL DEFAULT '',
            provider VARCHAR(32) NOT NULL,
            trade_date DATE NOT NULL,
            status VARCHAR(32) NOT NULL,
            strategy_eligible TINYINT(1) NOT NULL,
            run_id VARCHAR(64) NOT NULL DEFAULT '',
            catalog_batch_id VARCHAR(64) NULL,
            catalog_manifest_hash CHAR(64) NULL,
            calendar_batch_id VARCHAR(64) NULL,
            calendar_manifest_hash CHAR(64) NULL,
            source_batch_id VARCHAR(64) NOT NULL DEFAULT '',
            grid_profile VARCHAR(64) NOT NULL DEFAULT '',
            expected_entity_count INT NOT NULL DEFAULT 0,
            entity_count INT NOT NULL DEFAULT 0,
            expected_traded_count INT NOT NULL DEFAULT 0,
            actual_traded_count INT NOT NULL DEFAULT 0,
            no_trade_count INT NOT NULL DEFAULT 0,
            bar_count BIGINT NOT NULL DEFAULT 0,
            expected_entity_set_hash CHAR(64) NOT NULL,
            expected_traded_set_hash CHAR(64) NOT NULL,
            actual_traded_set_hash CHAR(64) NOT NULL,
            no_trade_set_hash CHAR(64) NOT NULL,
            entity_root_hash CHAR(64) NOT NULL,
            reason_count INT NOT NULL DEFAULT 0,
            captured_at DATETIME NOT NULL,
            manifest_json MEDIUMTEXT NOT NULL,
            created_at DATETIME NOT NULL,
            CONSTRAINT fk_qmt_history_coverage_catalog
                FOREIGN KEY (catalog_batch_id)
                REFERENCES qmt_stock_catalog_batch (batch_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            CONSTRAINT fk_qmt_history_coverage_calendar
                FOREIGN KEY (calendar_batch_id)
                REFERENCES qmt_trade_calendar_batch (batch_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            KEY idx_qmt_history_coverage_lookup
                (dataset, trade_date, provider, status, captured_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {COVERAGE_ENTITY_TABLE} (
            manifest_hash CHAR(64) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            expected_state VARCHAR(32) NOT NULL,
            classification VARCHAR(32) NOT NULL,
            bar_count INT NOT NULL,
            time_set_hash CHAR(64) NOT NULL,
            first_time VARCHAR(32) NOT NULL DEFAULT '',
            last_time VARCHAR(32) NOT NULL DEFAULT '',
            source_row_hash CHAR(64) NOT NULL,
            row_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (manifest_hash, stock_code),
            CONSTRAINT fk_qmt_history_coverage_entity_manifest
                FOREIGN KEY (manifest_hash)
                REFERENCES {COVERAGE_TABLE} (manifest_hash)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            KEY idx_qmt_history_coverage_entity_code
                (stock_code, manifest_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    )


def coverage_trigger_ddl_statements() -> tuple[str, ...]:
    return (
        f"""CREATE TRIGGER IF NOT EXISTS trg_qmt_history_coverage_no_update
        BEFORE UPDATE ON {COVERAGE_TABLE} FOR EACH ROW
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='{COVERAGE_TABLE} is append-only'""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_qmt_history_coverage_no_delete
        BEFORE DELETE ON {COVERAGE_TABLE} FOR EACH ROW
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='{COVERAGE_TABLE} is append-only'""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_qmt_history_coverage_entity_no_update
        BEFORE UPDATE ON {COVERAGE_ENTITY_TABLE} FOR EACH ROW
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='{COVERAGE_ENTITY_TABLE} is append-only'""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_qmt_history_coverage_entity_no_delete
        BEFORE DELETE ON {COVERAGE_ENTITY_TABLE} FOR EACH ROW
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='{COVERAGE_ENTITY_TABLE} is append-only'""",
    )


def validate_coverage_schema(
    connection: Any,
    *,
    require_triggers: bool = True,
) -> dict[str, Any]:
    """Read-only physical verification for the primary ``probiga`` schema."""

    table_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
            "(:manifest_table,:entity_table) ORDER BY TABLE_NAME"
        ),
        {
            "manifest_table": COVERAGE_TABLE,
            "entity_table": COVERAGE_ENTITY_TABLE,
        },
    ).mappings().all()
    table_contracts = {
        str(row.get("TABLE_NAME") or row.get("table_name") or ""): {
            "engine": str(row.get("ENGINE") or row.get("engine") or "").lower(),
            "collation": str(
                row.get("TABLE_COLLATION")
                or row.get("table_collation")
                or ""
            ).lower(),
        }
        for row in table_rows
    }
    present_tables = sorted(table_contracts)
    if present_tables != sorted(COVERAGE_TABLE_NAMES):
        raise QmtHistoryCoverageError("coverage table inventory differs")
    if any(
        row != {
            "engine": "innodb",
            "collation": "utf8mb4_unicode_ci",
        }
        for row in table_contracts.values()
    ):
        raise QmtHistoryCoverageError("coverage table engine/collation differs")
    column_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
            "IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION, "
            "CHARACTER_SET_NAME, COLLATION_NAME "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
            "(:manifest_table,:entity_table)"
        ),
        {
            "manifest_table": COVERAGE_TABLE,
            "entity_table": COVERAGE_ENTITY_TABLE,
        },
    ).mappings().all()
    columns = {
        (
            str(row.get("TABLE_NAME") or row.get("table_name") or ""),
            str(row.get("COLUMN_NAME") or row.get("column_name") or ""),
        ): {
            "data_type": str(
                row.get("DATA_TYPE") or row.get("data_type") or ""
            ).lower(),
            "column_type": str(
                row.get("COLUMN_TYPE") or row.get("column_type") or ""
            ).lower(),
            "nullable": str(
                row.get("IS_NULLABLE") or row.get("is_nullable") or ""
            ).upper(),
            "length": (
                int(
                    row.get("CHARACTER_MAXIMUM_LENGTH")
                    or row.get("character_maximum_length")
                )
                if (
                    row.get("CHARACTER_MAXIMUM_LENGTH")
                    or row.get("character_maximum_length")
                )
                is not None
                else None
            ),
            "ordinal": int(
                row.get("ORDINAL_POSITION")
                or row.get("ordinal_position")
                or 0
            ),
            "character_set": str(
                row.get("CHARACTER_SET_NAME")
                or row.get("character_set_name")
                or ""
            ).lower()
            or None,
            "collation": str(
                row.get("COLLATION_NAME")
                or row.get("collation_name")
                or ""
            ).lower()
            or None,
        }
        for row in column_rows
    }
    required_columns = {
        (COVERAGE_TABLE, "manifest_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "schema_version"): ("varchar", "NO", 64),
        (COVERAGE_TABLE, "dataset"): ("varchar", "NO", 64),
        (COVERAGE_TABLE, "period"): ("varchar", "NO", 16),
        (COVERAGE_TABLE, "provider"): ("varchar", "NO", 32),
        (COVERAGE_TABLE, "trade_date"): ("date", "NO", None),
        (COVERAGE_TABLE, "status"): ("varchar", "NO", 32),
        (COVERAGE_TABLE, "strategy_eligible"): ("tinyint", "NO", None),
        (COVERAGE_TABLE, "run_id"): ("varchar", "NO", 64),
        (COVERAGE_TABLE, "catalog_batch_id"): ("varchar", "YES", 64),
        (COVERAGE_TABLE, "catalog_manifest_hash"): ("char", "YES", 64),
        (COVERAGE_TABLE, "calendar_batch_id"): ("varchar", "YES", 64),
        (COVERAGE_TABLE, "calendar_manifest_hash"): ("char", "YES", 64),
        (COVERAGE_TABLE, "source_batch_id"): ("varchar", "NO", 64),
        (COVERAGE_TABLE, "grid_profile"): ("varchar", "NO", 64),
        (COVERAGE_TABLE, "expected_entity_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "entity_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "expected_traded_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "actual_traded_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "no_trade_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "bar_count"): ("bigint", "NO", None),
        (COVERAGE_TABLE, "expected_entity_set_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "expected_traded_set_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "actual_traded_set_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "no_trade_set_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "entity_root_hash"): ("char", "NO", 64),
        (COVERAGE_TABLE, "reason_count"): ("int", "NO", None),
        (COVERAGE_TABLE, "manifest_json"): ("mediumtext", "NO", None),
        (COVERAGE_TABLE, "captured_at"): ("datetime", "NO", None),
        (COVERAGE_TABLE, "created_at"): ("datetime", "NO", None),
        (COVERAGE_ENTITY_TABLE, "manifest_hash"): ("char", "NO", 64),
        (COVERAGE_ENTITY_TABLE, "stock_code"): ("varchar", "NO", 16),
        (COVERAGE_ENTITY_TABLE, "expected_state"): ("varchar", "NO", 32),
        (COVERAGE_ENTITY_TABLE, "classification"): ("varchar", "NO", 32),
        (COVERAGE_ENTITY_TABLE, "bar_count"): ("int", "NO", None),
        (COVERAGE_ENTITY_TABLE, "time_set_hash"): ("char", "NO", 64),
        (COVERAGE_ENTITY_TABLE, "first_time"): ("varchar", "NO", 32),
        (COVERAGE_ENTITY_TABLE, "last_time"): ("varchar", "NO", 32),
        (COVERAGE_ENTITY_TABLE, "source_row_hash"): ("char", "NO", 64),
        (COVERAGE_ENTITY_TABLE, "row_hash"): ("char", "NO", 64),
        (COVERAGE_ENTITY_TABLE, "created_at"): ("datetime", "NO", None),
    }
    if set(columns) != set(required_columns):
        raise QmtHistoryCoverageError("coverage column inventory differs")
    for identity, (data_type, nullable, length) in required_columns.items():
        observed = columns.get(identity)
        if (
            observed is None
            or observed["data_type"] != data_type
            or observed["nullable"] != nullable
            or (length is not None and observed["length"] != length)
        ):
            raise QmtHistoryCoverageError(
                "coverage physical column contract differs"
            )
        if data_type in {"char", "varchar", "mediumtext"}:
            if (
                observed["character_set"] != "utf8mb4"
                or observed["collation"] != "utf8mb4_unicode_ci"
            ):
                raise QmtHistoryCoverageError(
                    "coverage character column collation differs"
                )
        elif (
            observed["character_set"] is not None
            or observed["collation"] is not None
        ):
            raise QmtHistoryCoverageError(
                "coverage non-character column metadata differs"
            )
    expected_column_order = {
        COVERAGE_TABLE: (
            "manifest_hash", "schema_version", "dataset", "period",
            "provider", "trade_date", "status", "strategy_eligible",
            "run_id", "catalog_batch_id", "catalog_manifest_hash",
            "calendar_batch_id", "calendar_manifest_hash", "source_batch_id",
            "grid_profile", "expected_entity_count", "entity_count",
            "expected_traded_count", "actual_traded_count", "no_trade_count",
            "bar_count", "expected_entity_set_hash",
            "expected_traded_set_hash", "actual_traded_set_hash",
            "no_trade_set_hash", "entity_root_hash", "reason_count",
            "captured_at", "manifest_json", "created_at",
        ),
        COVERAGE_ENTITY_TABLE: (
            "manifest_hash", "stock_code", "expected_state", "classification",
            "bar_count", "time_set_hash", "first_time", "last_time",
            "source_row_hash", "row_hash", "created_at",
        ),
    }
    for table_name, expected_order in expected_column_order.items():
        observed_order = tuple(
            name
            for (_table, name), detail in sorted(
                columns.items(), key=lambda item: item[1]["ordinal"]
            )
            if _table == table_name
        )
        if observed_order != expected_order:
            raise QmtHistoryCoverageError("coverage column order differs")

    index_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, "
            "COLUMN_NAME, SUB_PART, INDEX_TYPE "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
            "(:manifest_table,:entity_table) "
            "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
        ),
        {
            "manifest_table": COVERAGE_TABLE,
            "entity_table": COVERAGE_ENTITY_TABLE,
        },
    ).mappings().all()
    index_parts: dict[tuple[str, str], dict[str, Any]] = {}
    for row in index_rows:
        key = (
            str(row.get("TABLE_NAME") or row.get("table_name") or ""),
            str(row.get("INDEX_NAME") or row.get("index_name") or ""),
        )
        item = index_parts.setdefault(
            key,
            {"non_unique": int(row.get("NON_UNIQUE") or row.get("non_unique") or 0), "columns": [], "valid": True},
        )
        item["columns"].append(
            str(row.get("COLUMN_NAME") or row.get("column_name") or "")
        )
        item["valid"] = item["valid"] and (
            (row.get("SUB_PART") if "SUB_PART" in row else row.get("sub_part"))
            is None
            and str(row.get("INDEX_TYPE") or row.get("index_type") or "").upper()
            == "BTREE"
        )
    observed_indexes = {
        key: (value["non_unique"], tuple(value["columns"]))
        for key, value in index_parts.items()
        if value["valid"]
    }
    expected_indexes = {
        (COVERAGE_TABLE, "PRIMARY"): (0, ("manifest_hash",)),
        # MySQL creates these supporting indexes for the two manifest foreign
        # keys.  Their names are bound to the explicit constraint names, so
        # they are part of the physical table contract rather than drift.
        (COVERAGE_TABLE, "fk_qmt_history_coverage_catalog"): (
            1,
            ("catalog_batch_id",),
        ),
        (COVERAGE_TABLE, "fk_qmt_history_coverage_calendar"): (
            1,
            ("calendar_batch_id",),
        ),
        (COVERAGE_TABLE, "idx_qmt_history_coverage_lookup"): (
            1,
            ("dataset", "trade_date", "provider", "status", "captured_at"),
        ),
        (COVERAGE_ENTITY_TABLE, "PRIMARY"): (
            0,
            ("manifest_hash", "stock_code"),
        ),
        (COVERAGE_ENTITY_TABLE, "idx_qmt_history_coverage_entity_code"): (
            1,
            ("stock_code", "manifest_hash"),
        ),
    }
    if observed_indexes != expected_indexes:
        raise QmtHistoryCoverageError("coverage index contract differs")
    foreign_rows = connection.execute(
        text(
            "SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME, "
            "k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, "
            "r.UPDATE_RULE, r.DELETE_RULE "
            "FROM information_schema.KEY_COLUMN_USAGE k "
            "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
            "ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA "
            "AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
            "AND r.TABLE_NAME=k.TABLE_NAME "
            "WHERE k.TABLE_SCHEMA=DATABASE() "
            "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
            "AND k.TABLE_NAME IN (:manifest_table,:entity_table)"
        ),
        {
            "manifest_table": COVERAGE_TABLE,
            "entity_table": COVERAGE_ENTITY_TABLE,
        },
    ).mappings().all()
    foreign_keys = {
        (
            str(row.get("TABLE_NAME") or row.get("table_name") or ""),
            str(
                row.get("CONSTRAINT_NAME")
                or row.get("constraint_name")
                or ""
            ),
            str(row.get("COLUMN_NAME") or row.get("column_name") or ""),
            str(
                row.get("REFERENCED_TABLE_NAME")
                or row.get("referenced_table_name")
                or ""
            ),
            str(
                row.get("REFERENCED_COLUMN_NAME")
                or row.get("referenced_column_name")
                or ""
            ),
            str(row.get("UPDATE_RULE") or row.get("update_rule") or "").upper(),
            str(row.get("DELETE_RULE") or row.get("delete_rule") or "").upper(),
        )
        for row in foreign_rows
    }
    expected_foreign_keys = {
        (
            COVERAGE_TABLE,
            "fk_qmt_history_coverage_catalog",
            "catalog_batch_id",
            "qmt_stock_catalog_batch",
            "batch_id",
            "RESTRICT",
            "RESTRICT",
        ),
        (
            COVERAGE_TABLE,
            "fk_qmt_history_coverage_calendar",
            "calendar_batch_id",
            "qmt_trade_calendar_batch",
            "batch_id",
            "RESTRICT",
            "RESTRICT",
        ),
        (
            COVERAGE_ENTITY_TABLE,
            "fk_qmt_history_coverage_entity_manifest",
            "manifest_hash",
            COVERAGE_TABLE,
            "manifest_hash",
            "RESTRICT",
            "RESTRICT",
        ),
    }
    if foreign_keys != expected_foreign_keys:
        raise QmtHistoryCoverageError("coverage foreign-key contract differs")
    trigger_count = 0
    if require_triggers:
        trigger_rows = connection.execute(
            text(
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, "
                "ACTION_TIMING, ACTION_STATEMENT FROM information_schema.TRIGGERS "
                "WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME IN "
                "(:trigger_0,:trigger_1,:trigger_2,:trigger_3)"
            ),
            {
                f"trigger_{index}": name
                for index, name in enumerate(COVERAGE_TRIGGER_NAMES)
            },
        ).mappings().all()
        observed_names = {
            str(row.get("TRIGGER_NAME") or row.get("trigger_name") or "")
            for row in trigger_rows
        }
        if observed_names != set(COVERAGE_TRIGGER_NAMES):
            raise QmtHistoryCoverageError("coverage trigger inventory differs")
        expected_triggers = {
            COVERAGE_TRIGGER_NAMES[0]: (COVERAGE_TABLE, "UPDATE"),
            COVERAGE_TRIGGER_NAMES[1]: (COVERAGE_TABLE, "DELETE"),
            COVERAGE_TRIGGER_NAMES[2]: (COVERAGE_ENTITY_TABLE, "UPDATE"),
            COVERAGE_TRIGGER_NAMES[3]: (COVERAGE_ENTITY_TABLE, "DELETE"),
        }
        for row in trigger_rows:
            trigger_name = str(
                row.get("TRIGGER_NAME") or row.get("trigger_name") or ""
            )
            expected_table, expected_event = expected_triggers[trigger_name]
            if (
                str(
                    row.get("EVENT_OBJECT_TABLE")
                    or row.get("event_object_table")
                    or ""
                )
                != expected_table
                or
                str(
                    row.get("ACTION_TIMING") or row.get("action_timing") or ""
                ).upper()
                != "BEFORE"
                or str(
                    row.get("EVENT_MANIPULATION")
                    or row.get("event_manipulation")
                    or ""
                ).upper()
                != expected_event
                or "SIGNAL SQLSTATE '45000'"
                not in str(
                    row.get("ACTION_STATEMENT")
                    or row.get("action_statement")
                    or ""
                ).upper()
            ):
                raise QmtHistoryCoverageError("coverage trigger contract differs")
        trigger_count = len(trigger_rows)
    return {
        "database": "probiga",
        "table_names": list(COVERAGE_TABLE_NAMES),
        "table_count": len(present_tables),
        "foreign_key_count": len(expected_foreign_keys),
        "trigger_names": list(COVERAGE_TRIGGER_NAMES),
        "trigger_count": trigger_count,
        "expected_trigger_count": len(COVERAGE_TRIGGER_NAMES),
        "runtime_ddl_required": False,
        "physical_schema_verified": True,
        "physical_seal_verified": bool(require_triggers),
    }


def insert_coverage_bundle(connection: Any, bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Insert one validated bundle idempotently inside the caller transaction."""

    manifest = validate_coverage_authority(connection, bundle)
    manifest_hash = str(manifest["manifest_hash"])
    existing = connection.execute(
        text(
            f"SELECT manifest_json FROM {COVERAGE_TABLE} "
            "WHERE manifest_hash=:manifest_hash"
        ),
        {"manifest_hash": manifest_hash},
    ).mappings().all()
    if existing:
        if len(existing) != 1 or str(existing[0].get("manifest_json") or "") != str(
            bundle["manifest"]["manifest_json"]
        ):
            raise QmtHistoryCoverageError("coverage manifest replay differs")
        persisted_entities = connection.execute(
            text(
                f"""SELECT manifest_hash, stock_code, expected_state,
                    classification, bar_count, time_set_hash, first_time,
                    last_time, source_row_hash, row_hash
                FROM {COVERAGE_ENTITY_TABLE}
                WHERE manifest_hash=:manifest_hash
                ORDER BY stock_code"""
            ),
            {"manifest_hash": manifest_hash},
        ).mappings().all()
        normalized_persisted = [
            {
                **_entity_row(dict(row)),
                "manifest_hash": str(row.get("manifest_hash") or ""),
            }
            for row in persisted_entities
        ]
        normalized_requested = sorted(
            (
                {
                    **_entity_row(dict(row)),
                    "manifest_hash": str(row.get("manifest_hash") or ""),
                }
                for row in bundle["entities"]
            ),
            key=lambda row: row["stock_code"],
        )
        if normalized_persisted != normalized_requested:
            raise QmtHistoryCoverageError("coverage entity replay differs")
        return {"status": "idempotent", "manifest_hash": manifest_hash}

    raw_manifest = dict(bundle["manifest"])
    connection.execute(
        text(
            f"""INSERT INTO {COVERAGE_TABLE} (
                manifest_hash, schema_version, dataset, period, provider,
                trade_date, status, strategy_eligible, run_id,
                catalog_batch_id, catalog_manifest_hash, calendar_batch_id,
                calendar_manifest_hash, source_batch_id, grid_profile,
                expected_entity_count, entity_count, expected_traded_count,
                actual_traded_count, no_trade_count, bar_count,
                expected_entity_set_hash, expected_traded_set_hash,
                actual_traded_set_hash, no_trade_set_hash, entity_root_hash,
                reason_count, captured_at, manifest_json, created_at
            ) VALUES (
                :manifest_hash, :schema_version, :dataset, :period, :provider,
                :trade_date, :status, :strategy_eligible, :run_id,
                :catalog_batch_id, :catalog_manifest_hash, :calendar_batch_id,
                :calendar_manifest_hash, :source_batch_id, :grid_profile,
                :expected_entity_count, :entity_count, :expected_traded_count,
                :actual_traded_count, :no_trade_count, :bar_count,
                :expected_entity_set_hash, :expected_traded_set_hash,
                :actual_traded_set_hash, :no_trade_set_hash, :entity_root_hash,
                :reason_count, :captured_at, :manifest_json, :created_at
            )"""
        ),
        {
            **raw_manifest,
            "schema_version": raw_manifest["schema"],
            "strategy_eligible": 1 if raw_manifest["strategy_eligible"] else 0,
            "created_at": raw_manifest["captured_at"],
        },
    )
    entity_sql = text(
        f"""INSERT INTO {COVERAGE_ENTITY_TABLE} (
            manifest_hash, stock_code, expected_state, classification, bar_count,
            time_set_hash, first_time, last_time, source_row_hash,
            row_hash, created_at
        ) VALUES (
            :manifest_hash, :stock_code, :expected_state, :classification, :bar_count,
            :time_set_hash, :first_time, :last_time, :source_row_hash,
            :row_hash, :created_at
        )"""
    )
    for raw in bundle["entities"]:
        connection.execute(
            entity_sql,
            {**raw, "created_at": raw_manifest["captured_at"]},
        )
    return {"status": "inserted", "manifest_hash": manifest_hash}


__all__ = [
    "COVERAGE_ENTITY_TABLE",
    "COVERAGE_EXACT",
    "COVERAGE_INCOMPLETE",
    "COVERAGE_SCHEMA",
    "COVERAGE_TABLE",
    "COVERAGE_TABLE_NAMES",
    "COVERAGE_TRIGGER_NAMES",
    "COVERAGE_UNAVAILABLE",
    "DATASET_STOCK_DAILY",
    "DATASET_STOCK_MINUTE",
    "QMT_MINUTE_GRID_PROFILE",
    "QMT_MINUTE_GRID_PREFIX",
    "QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH",
    "QmtHistoryCoverageError",
    "assess_daily_coverage",
    "assess_minute_coverage",
    "canonical_digest",
    "combine_minute_coverage_partitions",
    "coverage_table_ddl_statements",
    "coverage_trigger_ddl_statements",
    "insert_coverage_bundle",
    "minute_time_grid",
    "minute_grid_profile_for_capture",
    "require_exact_coverage",
    "unavailable_coverage_bundle",
    "validate_coverage_authority",
    "validate_coverage_bundle",
    "validate_coverage_schema",
]
