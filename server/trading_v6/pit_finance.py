"""Pure, explicit-input point-in-time finance features for Trading V6.

This module performs no database or network I/O.  Every finance record carries
both an announced timestamp and a knowledge timestamp.  A statement is usable
only when both timestamps are no later than the after-close signal timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
from typing import Any, Mapping, Sequence
import weakref


PIT_FINANCE_PROTOCOL = "v6:pit-finance-after-close:v4"
SHANGHAI_OFFSET = timedelta(hours=8)
MAX_ABS_FINANCE_VALUE = 1_000_000.0


class PitFinanceContractError(ValueError):
    """Raised when explicit V6 PIT inputs are incomplete or time-unsafe."""


class _BuildToken:
    __slots__ = ("__weakref__",)


_BUILD_ATTESTATIONS: weakref.WeakKeyDictionary[_BuildToken, str] = (
    weakref.WeakKeyDictionary()
)


@dataclass(frozen=True, slots=True)
class PitFinanceFeature:
    sample_id: str
    instrument_id: str
    signal_at: str
    market_feature_available_at: str
    statement_id: str | None
    report_date: str | None
    notice_at: str | None
    knowledge_at: str | None
    finance_source_manifest_sha256: str
    finance_peer_manifest_sha256: str
    finance_peer_count: int
    quality_percentile: float | None
    cashflow_percentile: float | None
    valuation_percentile: float | None
    asset_liab_ratio_pit: float | None
    net_profit_yoy_gr_pit: float | None
    feature_snapshot_sha256: str
    _build_token: _BuildToken = field(
        default_factory=_BuildToken, init=False, repr=False, compare=False
    )

    @property
    def status(self) -> str:
        values = (
            self.quality_percentile,
            self.cashflow_percentile,
            self.valuation_percentile,
            self.asset_liab_ratio_pit,
            self.net_profit_yoy_gr_pit,
        )
        return (
            "PIT_RESEARCH_FEATURE_READY"
            if all(value is not None for value in values)
            else "DATA_BLOCKED"
        )

    @property
    def lifecycle_status(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def activation_eligible(self) -> bool:
        return False

    def __post_init__(self) -> None:
        _text(self.sample_id, "sample_id")
        _text(self.instrument_id, "instrument_id")
        signal = _after_close_timestamp(self.signal_at, "signal_at")
        market_available = _aware_shanghai_timestamp(
            self.market_feature_available_at,
            "market_feature_available_at",
        )
        if market_available > signal:
            raise PitFinanceContractError(
                "market_feature_available_at exceeds signal_at"
            )
        _sha256(
            self.finance_source_manifest_sha256,
            "finance_source_manifest_sha256",
        )
        _sha256(
            self.finance_peer_manifest_sha256,
            "finance_peer_manifest_sha256",
        )
        if type(self.finance_peer_count) is not int or self.finance_peer_count < 1:
            raise PitFinanceContractError("finance_peer_count must be positive")
        if self.statement_id is None:
            if any(
                value is not None
                for value in (
                    self.report_date,
                    self.notice_at,
                    self.knowledge_at,
                    self.quality_percentile,
                    self.cashflow_percentile,
                    self.valuation_percentile,
                    self.asset_liab_ratio_pit,
                    self.net_profit_yoy_gr_pit,
                )
            ):
                raise PitFinanceContractError(
                    "a missing statement cannot carry PIT finance values"
                )
        else:
            _text(self.statement_id, "statement_id")
            report = _date(self.report_date, "report_date")
            notice = _aware_shanghai_timestamp(self.notice_at, "notice_at")
            knowledge = _aware_shanghai_timestamp(
                self.knowledge_at, "knowledge_at"
            )
            if report > signal.date():
                raise PitFinanceContractError("report_date exceeds signal_at")
            if notice > signal or knowledge > signal:
                raise PitFinanceContractError(
                    "PIT finance publication or knowledge exceeds signal_at"
                )
            if knowledge < notice:
                raise PitFinanceContractError(
                    "knowledge_at precedes the published notice_at"
                )
            for label, value in (
                ("quality_percentile", self.quality_percentile),
                ("cashflow_percentile", self.cashflow_percentile),
                ("valuation_percentile", self.valuation_percentile),
            ):
                if value is not None and not 0.0 < value <= 1.0:
                    raise PitFinanceContractError(f"{label} must be within (0, 1]")
            for label, value in (
                ("asset_liab_ratio_pit", self.asset_liab_ratio_pit),
                ("net_profit_yoy_gr_pit", self.net_profit_yoy_gr_pit),
            ):
                if value is not None:
                    _finite(value, label)
        _sha256(self.feature_snapshot_sha256, "feature_snapshot_sha256")
        expected = _canonical_sha256(_feature_payload(self))
        if self.feature_snapshot_sha256 != expected:
            raise PitFinanceContractError("feature snapshot hash differs")

    def assert_integrity(self) -> None:
        self.__post_init__()
        expected = _BUILD_ATTESTATIONS.get(self._build_token)
        if expected is None:
            raise PitFinanceContractError(
                "PIT feature lacks a process-local builder attestation"
            )
        if expected != self.feature_snapshot_sha256:
            raise PitFinanceContractError("PIT builder attestation differs")

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {
            **_feature_payload(self),
            "feature_snapshot_sha256": self.feature_snapshot_sha256,
            "status": self.status,
            "source_certification_status": "UNVERIFIED_EXPLICIT_INPUT",
            "lifecycle_status": "RESEARCH_ONLY",
            "production_eligible": False,
            "activation_eligible": False,
            "actionable_output_allowed": False,
        }


def build_pit_finance_features(
    market_rows: Sequence[Mapping[str, Any]],
    finance_rows: Sequence[Mapping[str, Any]],
) -> tuple[PitFinanceFeature, ...]:
    """Build deterministic cross-sectional PIT features from explicit rows."""

    markets = _normalize_market_rows(market_rows)
    statements = _normalize_finance_rows(finance_rows)

    output: list[PitFinanceFeature] = []
    signal_times = sorted({row["signal_at"] for row in markets})
    for signal_at in signal_times:
        visible_statements = [
            statement
            for statement in statements
            if statement["notice_at"] <= signal_at
            and statement["knowledge_at"] <= signal_at
        ]
        # Bind only the information that was knowable at this signal.  This
        # preserves the PIT prefix invariant: appending a future disclosure
        # cannot rewrite an already-built historical feature snapshot.
        source_manifest_sha256 = _canonical_sha256(
            [_statement_manifest(row) for row in visible_statements]
        )
        day_rows = [row for row in markets if row["signal_at"] == signal_at]
        if len({row["instrument_id"] for row in day_rows}) != len(day_rows):
            raise PitFinanceContractError(
                "one signal timestamp cannot contain duplicate instruments"
            )
        latest: dict[str, dict[str, Any]] = {}
        for row in day_rows:
            candidates = [
                statement
                for statement in visible_statements
                if statement["instrument_id"] == row["instrument_id"]
            ]
            if candidates:
                latest_key = max(
                    (
                        item["report_date"],
                        item["knowledge_at"],
                        item["notice_at"],
                    )
                    for item in candidates
                )
                latest_candidates = [
                    item
                    for item in candidates
                    if (
                        item["report_date"],
                        item["knowledge_at"],
                        item["notice_at"],
                    )
                    == latest_key
                ]
                economic_payloads = {
                    _canonical_sha256(
                        {
                            key: value
                            for key, value in _statement_manifest(item).items()
                            if key != "statement_id"
                        }
                    )
                    for item in latest_candidates
                }
                if len(economic_payloads) != 1:
                    raise PitFinanceContractError(
                        "conflicting finance statements share one effective timestamp"
                    )
                # Duplicate, economically identical records are harmless; the
                # ID is only a deterministic provenance label, never authority.
                latest[row["instrument_id"]] = min(
                    latest_candidates, key=lambda item: item["statement_id"]
                )

        quality_raw: dict[str, float] = {}
        cashflow_raw: dict[str, float] = {}
        valuation_raw: dict[str, float] = {}
        for row in day_rows:
            if row["eligible_liquid"] is not True:
                continue
            statement = latest.get(row["instrument_id"])
            if statement is None or statement["net_asset_ps"] <= 0:
                continue
            quality_raw[row["instrument_id"]] = (
                statement["roe_wtd"]
                + statement["gross_margin"] * 0.25
                + statement["net_margin"] * 0.25
                - statement["asset_liab_ratio"] * 0.15
            )
            cashflow_raw[row["instrument_id"]] = (
                statement["oper_cf_ps"]
                + statement["cash_flow_ratio"] * 0.1
            )
            valuation_raw[row["instrument_id"]] = (
                row["raw_close"] / statement["net_asset_ps"]
            )

        quality_rank = _percentile_ranks(quality_raw, ascending=True)
        cashflow_rank = _percentile_ranks(cashflow_raw, ascending=True)
        valuation_rank = _percentile_ranks(valuation_raw, ascending=False)
        peer_rows = sorted(
            (
                {
                    "sample_id": row["sample_id"],
                    "instrument_id": row["instrument_id"],
                    "signal_at": _timestamp_text(row["signal_at"]),
                    "market_feature_available_at": _timestamp_text(
                        row["feature_available_at"]
                    ),
                    "raw_close": _stable_float(row["raw_close"]),
                    "eligible_liquid": row["eligible_liquid"],
                    "selected_statement_id": (
                        latest.get(row["instrument_id"]) or {}
                    ).get("statement_id"),
                    "included_in_finance_peer_set": (
                        row["instrument_id"] in quality_raw
                    ),
                }
                for row in day_rows
            ),
            key=lambda item: item["sample_id"],
        )
        peer_manifest_sha256 = _canonical_sha256(peer_rows)
        peer_count = len(quality_raw)
        if peer_count < 1:
            raise PitFinanceContractError(
                "each signal timestamp needs at least one eligible finance peer"
            )
        for row in sorted(day_rows, key=lambda item: item["sample_id"]):
            statement = latest.get(row["instrument_id"])
            values: dict[str, Any] = {
                "protocol": PIT_FINANCE_PROTOCOL,
                "sample_id": row["sample_id"],
                "instrument_id": row["instrument_id"],
                "signal_at": _timestamp_text(signal_at),
                "market_feature_available_at": _timestamp_text(
                    row["feature_available_at"]
                ),
                "statement_id": None,
                "report_date": None,
                "notice_at": None,
                "knowledge_at": None,
                "finance_source_manifest_sha256": source_manifest_sha256,
                "finance_peer_manifest_sha256": peer_manifest_sha256,
                "finance_peer_count": peer_count,
                "quality_percentile": None,
                "cashflow_percentile": None,
                "valuation_percentile": None,
                "asset_liab_ratio_pit": None,
                "net_profit_yoy_gr_pit": None,
            }
            if statement is not None:
                values.update(
                    {
                        "statement_id": statement["statement_id"],
                        "report_date": statement["report_date"].isoformat(),
                        "notice_at": _timestamp_text(statement["notice_at"]),
                        "knowledge_at": _timestamp_text(
                            statement["knowledge_at"]
                        ),
                        "quality_percentile": quality_rank.get(
                            row["instrument_id"]
                        ),
                        "cashflow_percentile": cashflow_rank.get(
                            row["instrument_id"]
                        ),
                        "valuation_percentile": valuation_rank.get(
                            row["instrument_id"]
                        ),
                        "asset_liab_ratio_pit": _stable_float(
                            statement["asset_liab_ratio"]
                        ),
                        "net_profit_yoy_gr_pit": _stable_float(
                            statement["net_profit_yoy_gr"]
                        ),
                    }
                )
            feature = PitFinanceFeature(
                **{key: value for key, value in values.items() if key != "protocol"},
                feature_snapshot_sha256=_canonical_sha256(values),
            )
            _BUILD_ATTESTATIONS[feature._build_token] = feature.feature_snapshot_sha256
            output.append(feature)
    return tuple(output)


def _normalize_market_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
        raise PitFinanceContractError("market_rows must be a non-empty sequence")
    output: list[dict[str, Any]] = []
    sample_ids: set[str] = set()
    for index, raw in enumerate(rows):
        row = _mapping(raw, f"market_rows[{index}]")
        sample_id = _text(row.get("sample_id"), "sample_id")
        if sample_id in sample_ids:
            raise PitFinanceContractError("market sample_id values must be unique")
        sample_ids.add(sample_id)
        signal_at = _after_close_timestamp(row.get("signal_at"), "signal_at")
        available_at = _aware_shanghai_timestamp(
            row.get("feature_available_at"), "feature_available_at"
        )
        if available_at > signal_at:
            raise PitFinanceContractError(
                f"market row {sample_id} uses a feature known after signal_at"
            )
        liquid = row.get("eligible_liquid")
        if type(liquid) is not bool:
            raise PitFinanceContractError("eligible_liquid must be a boolean")
        close = _finite(row.get("raw_close"), "raw_close")
        if not 0.0 < close <= MAX_ABS_FINANCE_VALUE:
            raise PitFinanceContractError("raw_close is outside the safe range")
        output.append(
            {
                "sample_id": sample_id,
                "instrument_id": _text(
                    row.get("instrument_id"), "instrument_id"
                ),
                "signal_at": signal_at,
                "feature_available_at": available_at,
                "raw_close": _stable_float(close),
                "eligible_liquid": liquid,
            }
        )
    return sorted(output, key=lambda item: (item["signal_at"], item["sample_id"]))


def _normalize_finance_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise PitFinanceContractError("finance_rows must be a sequence")
    output: list[dict[str, Any]] = []
    statement_ids: set[str] = set()
    numeric_fields = (
        "net_asset_ps",
        "oper_cf_ps",
        "net_profit_yoy_gr",
        "roe_wtd",
        "gross_margin",
        "net_margin",
        "cash_flow_ratio",
        "asset_liab_ratio",
    )
    for index, raw in enumerate(rows):
        row = _mapping(raw, f"finance_rows[{index}]")
        statement_id = _text(row.get("statement_id"), "statement_id")
        if statement_id in statement_ids:
            raise PitFinanceContractError("finance statement_id values must be unique")
        statement_ids.add(statement_id)
        report_date = _date(row.get("report_date"), "report_date")
        notice_at = _aware_shanghai_timestamp(row.get("notice_at"), "notice_at")
        knowledge_at = _aware_shanghai_timestamp(
            row.get("knowledge_at"), "knowledge_at"
        )
        if report_date > notice_at.date():
            raise PitFinanceContractError("report_date cannot be after notice_at")
        if notice_at > knowledge_at:
            raise PitFinanceContractError("knowledge_at cannot precede notice_at")
        normalized: dict[str, Any] = {
            "statement_id": statement_id,
            "instrument_id": _text(
                row.get("instrument_id"), "instrument_id"
            ),
            "report_date": report_date,
            "notice_at": notice_at,
            "knowledge_at": knowledge_at,
        }
        for field in numeric_fields:
            value = _finite(row.get(field), field)
            if abs(value) > MAX_ABS_FINANCE_VALUE:
                raise PitFinanceContractError(f"{field} is outside the safe range")
            # One canonical numeric representation is used for conflict
            # comparison, manifests, ranking, and feature computation.  This
            # prevents sub-hash-precision values from changing rank outcomes.
            normalized[field] = _stable_float(value)
        output.append(normalized)
    return sorted(
        output,
        key=lambda item: (
            item["instrument_id"],
            item["report_date"],
            item["knowledge_at"],
            item["statement_id"],
        ),
    )


def _percentile_ranks(
    values: Mapping[str, float], *, ascending: bool
) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(
        values.items(),
        key=lambda item: (
            item[1] if ascending else -item[1],
            item[0],
        ),
    )
    result: dict[str, float] = {}
    index = 0
    count = len(ordered)
    while index < count:
        end = index + 1
        while end < count and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = ((index + 1) + end) / 2.0
        percentile = _stable_float(average_rank / count)
        for cursor in range(index, end):
            result[ordered[cursor][0]] = percentile
        index = end
    return result


def _feature_payload(feature: PitFinanceFeature) -> dict[str, Any]:
    return {
        "protocol": PIT_FINANCE_PROTOCOL,
        "sample_id": feature.sample_id,
        "instrument_id": feature.instrument_id,
        "signal_at": feature.signal_at,
        "market_feature_available_at": feature.market_feature_available_at,
        "statement_id": feature.statement_id,
        "report_date": feature.report_date,
        "notice_at": feature.notice_at,
        "knowledge_at": feature.knowledge_at,
        "finance_source_manifest_sha256": feature.finance_source_manifest_sha256,
        "finance_peer_manifest_sha256": feature.finance_peer_manifest_sha256,
        "finance_peer_count": feature.finance_peer_count,
        "quality_percentile": feature.quality_percentile,
        "cashflow_percentile": feature.cashflow_percentile,
        "valuation_percentile": feature.valuation_percentile,
        "asset_liab_ratio_pit": feature.asset_liab_ratio_pit,
        "net_profit_yoy_gr_pit": feature.net_profit_yoy_gr_pit,
    }


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PitFinanceContractError(f"{label} must be an object")
    return value


def _statement_manifest(statement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: (
            value.isoformat(timespec="auto")
            if isinstance(value, datetime)
            else value.isoformat()
            if isinstance(value, date)
            else _stable_float(value)
            if isinstance(value, float)
            else value
        )
        for key, value in statement.items()
    }


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise PitFinanceContractError(f"{label} must be canonical non-empty text")
    return value


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise PitFinanceContractError(f"{label} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise PitFinanceContractError(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise PitFinanceContractError(f"{label} must be finite")
    return result


def _date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        raise PitFinanceContractError(f"{label} must be an ISO date, not datetime")
    try:
        result = value if isinstance(value, date) else date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PitFinanceContractError(f"{label} must be an ISO date") from exc
    return result


def _aware_shanghai_timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PitFinanceContractError(f"{label} must be an ISO timestamp") from exc
    else:
        raise PitFinanceContractError(f"{label} must be an ISO timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise PitFinanceContractError(f"{label} must be timezone-aware")
    if result.utcoffset() != SHANGHAI_OFFSET:
        raise PitFinanceContractError(f"{label} must use the Asia/Shanghai +08:00 offset")
    return result


def _after_close_timestamp(value: Any, label: str) -> datetime:
    result = _aware_shanghai_timestamp(value, label)
    if result.timetz().replace(tzinfo=None) < time(15, 0):
        raise PitFinanceContractError(f"{label} must use an AFTER_CLOSE timestamp")
    return result


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(timespec="auto")


def _stable_float(value: float) -> float:
    return float(f"{float(value):.12g}")


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PitFinanceContractError(f"{label} must be a lowercase SHA-256")
    return value


__all__ = [
    "PIT_FINANCE_PROTOCOL",
    "PitFinanceContractError",
    "PitFinanceFeature",
    "build_pit_finance_features",
]
