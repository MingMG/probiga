"""Authoritative target-date universe and coverage gates for daily stock data.

The immutable QMT catalog, rather than the rows returned by a market-data
provider, defines the expected stock set.  A suspended stock is represented by
a target-date daily bar whose volume and amount are both zero.  Such a stock
may be absent from capital-flow data; every stock that traded must have a
capital-flow row.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from server.common.qmt_stock_catalog import (
    load_target_stock_catalog,
    validate_stock_catalog_runtime_schema,
)


def _code(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("DATA_BLOCKED: empty stock code in daily source")
    code = raw.zfill(6)
    if len(code) != 6 or not code.isdigit():
        raise RuntimeError(f"DATA_BLOCKED: invalid stock code in daily source: {value!r}")
    return code


def _code_set_sha256(codes: Iterable[str]) -> str:
    payload = "\n".join(sorted({_code(code) for code in codes})).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _nonnegative_decimal(value: Any, *, field: str, code: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"DATA_BLOCKED: {field} is not numeric for stock_code={code}"
        ) from exc
    if not number.is_finite() or number < 0:
        raise RuntimeError(
            f"DATA_BLOCKED: {field} must be finite and nonnegative for stock_code={code}"
        )
    return number


@dataclass(frozen=True)
class DailyStockUniverse:
    target_date: str
    catalog_batch_id: str
    catalog_manifest_hash: str
    catalog_member_set_hash: str
    expected_codes: tuple[str, ...]
    expected_code_set_hash: str
    catalog_captured_at: str = ""
    catalog_history_complete_from: str = ""
    catalog_knowledge_mode: str = "SAME_DAY_OR_PRIOR"

    @property
    def expected_count(self) -> int:
        return len(self.expected_codes)


def load_daily_stock_universe(
    engine: Any,
    target_date: str,
    *,
    decision_known_at: datetime | None = None,
) -> DailyStockUniverse:
    """Load and verify one immutable target-date QMT stock catalog."""

    validate_stock_catalog_runtime_schema(engine)
    catalog, codes = load_target_stock_catalog(
        engine,
        target_date=target_date,
        decision_known_at=decision_known_at or datetime.now().replace(microsecond=0),
    )
    normalized = tuple(sorted({_code(code) for code in codes}))
    if len(normalized) != len(codes):
        raise RuntimeError("DATA_BLOCKED: immutable stock catalog contains duplicate codes")
    captured_date = catalog.captured_at[:10]
    retrospective = captured_date > target_date
    effective_range_proven = (
        catalog.history_complete_from <= target_date
        and all(
            str(member.get("list_date") or "").strip()
            and "expire_date" in member
            and str(member.get("instrument_batch_id") or "").strip()
            and str(member.get("instrument_type") or "") == "STOCK"
            for member in catalog.members
        )
    )
    if retrospective and not effective_range_proven:
        raise RuntimeError(
            "DATA_BLOCKED: post-target stock catalog lacks native effective-range proof"
        )
    return DailyStockUniverse(
        target_date=target_date,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        expected_codes=normalized,
        expected_code_set_hash=_code_set_sha256(normalized),
        catalog_captured_at=catalog.captured_at,
        catalog_history_complete_from=catalog.history_complete_from,
        catalog_knowledge_mode=(
            "RETROSPECTIVE_NATIVE_EFFECTIVE_RANGE"
            if retrospective
            else "SAME_DAY_OR_PRIOR"
        ),
    )


def _rows_by_code(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_name: str,
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    duplicates: list[str] = []
    for row in rows:
        code = _code(row.get("stock_code"))
        if code in result:
            duplicates.append(code)
        result[code] = row
    if duplicates:
        raise RuntimeError(
            f"DATA_BLOCKED: {source_name} contains duplicate stock codes: "
            f"count={len(duplicates)}, sample={sorted(set(duplicates))[:20]}"
        )
    return result


def validate_daily_stock_coverage(
    universe: DailyStockUniverse,
    *,
    kline_rows: Iterable[Mapping[str, Any]],
    flow_rows: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Fail closed unless target-date K/flow membership is complete.

    Daily K coverage is exact: QMT supplies zero-volume placeholder bars for
    suspended members, so suspension does not excuse a missing K-line row.
    Capital-flow coverage is exact for the subset that traded; a stock with
    both zero volume and zero amount is the only permitted omission.
    """

    expected = set(universe.expected_codes)
    if not expected:
        raise RuntimeError("DATA_BLOCKED: authoritative stock universe is empty")
    kline = _rows_by_code(kline_rows, source_name="target-date daily K-line")
    kline_codes = set(kline)
    missing_kline = sorted(expected - kline_codes)
    unexpected_kline = sorted(kline_codes - expected)
    if missing_kline or unexpected_kline:
        raise RuntimeError(
            "DATA_BLOCKED: target-date daily K-line set differs from immutable catalog: "
            f"expected={len(expected)}, actual={len(kline_codes)}, "
            f"coverage={len(expected & kline_codes) / len(expected):.6f}, "
            f"missing_sample={missing_kline[:20]}, "
            f"unexpected_sample={unexpected_kline[:20]}, "
            f"catalog_hash={universe.expected_code_set_hash}"
        )

    suspended: set[str] = set()
    traded: set[str] = set()
    for code, row in kline.items():
        volume = _nonnegative_decimal(row.get("volume"), field="volume", code=code)
        amount = _nonnegative_decimal(row.get("amount"), field="amount", code=code)
        (suspended if volume == 0 and amount == 0 else traded).add(code)

    audit: dict[str, Any] = {
        "target_date": universe.target_date,
        "catalog_batch_id": universe.catalog_batch_id,
        "catalog_manifest_hash": universe.catalog_manifest_hash,
        "catalog_member_set_hash": universe.catalog_member_set_hash,
        "catalog_captured_at": universe.catalog_captured_at,
        "catalog_history_complete_from": universe.catalog_history_complete_from,
        "catalog_knowledge_mode": universe.catalog_knowledge_mode,
        "expected_code_set_hash": universe.expected_code_set_hash,
        "expected_count": len(expected),
        "kline_count": len(kline_codes),
        "kline_coverage": 1.0,
        "traded_count": len(traded),
        "suspended_count": len(suspended),
        "suspension_rule": "volume=0 AND amount=0; K required, flow optional",
    }
    if flow_rows is None:
        return audit

    flow = _rows_by_code(flow_rows, source_name="target-date capital flow")
    flow_codes = set(flow)
    unexpected_flow = sorted(flow_codes - expected)
    missing_traded_flow = sorted(traded - flow_codes)
    if unexpected_flow or missing_traded_flow:
        active_coverage = len(traded & flow_codes) / max(len(traded), 1)
        raise RuntimeError(
            "DATA_BLOCKED: target-date K-line/capital-flow sets are misaligned: "
            f"expected={len(expected)}, traded={len(traded)}, flow={len(flow_codes)}, "
            f"traded_flow_coverage={active_coverage:.6f}, "
            f"missing_traded_sample={missing_traded_flow[:20]}, "
            f"unexpected_flow_sample={unexpected_flow[:20]}, "
            f"catalog_hash={universe.expected_code_set_hash}"
        )
    audit.update(
        {
            "flow_count": len(flow_codes),
            "traded_flow_intersection_count": len(traded & flow_codes),
            "traded_flow_coverage": 1.0,
            "suspended_without_flow_count": len(suspended - flow_codes),
        }
    )
    return audit


def rows_from_mappings(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Materialize SQLAlchemy mapping rows for the coverage validator."""

    return [dict(row) for row in rows]


__all__ = [
    "DailyStockUniverse",
    "load_daily_stock_universe",
    "rows_from_mappings",
    "validate_daily_stock_coverage",
]
