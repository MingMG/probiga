"""Canonical, fail-closed BUY gate shared by V2 planning and execution.

The gate deliberately reuses the existing V2 decision signal and intent
evidence.  It does not create another account, order, position, or risk
ledger.  A planner binds one immutable gate receipt into ``evidence_json``;
the executor reconstructs the current receipt from the decision signal and
the latest recommendation state before matching and again before applying a
fill.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from .config import canonical_json_hash


GATE_MODULE = "canonical_execution_buy_gate"
GATE_SCHEMA_VERSION = "v2.execution-buy-gate.v2"
ALLOWED_COMPETITION_STATUSES = frozenset(
    {"ELIGIBLE", "PAPER_TRIAL_ELIGIBLE"}
)
ACTIONABLE_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})
EXECUTABLE_EVENT_RISKS = frozenset({"LOW", "MEDIUM"})
_EVENT_RANK = {
    "": 5,
    "UNKNOWN": 5,
    "DATA_BLOCKED": 5,
    "CRITICAL": 4,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_BLOCKED_QUALITY_MARKERS = (
    "DATA_BLOCKED",
    "SOURCE_STALE",
    "SOURCE_MISSING",
    "SOURCE_ERROR",
    "EVENT_SOURCE_MISSING",
    "NEWS_SOURCE_MISSING",
    "NOTICE_SOURCE_MISSING",
    "UNAVAILABLE",
)
_RECOMMENDATION_REQUIRED_COLUMNS = frozenset(
    {
        "stock_code",
        "pick_date",
        "recommend_status",
        "signal_status",
        "chase_risk_status",
        "ordinary_buy_eligible",
        "event_risk_level",
        "data_quality_score",
        "data_quality_flags",
    }
)
_BUY_GATE_RECEIPT_FIELDS = frozenset(
    {
        "module",
        "schema_version",
        "decision_run_uid",
        "strategy_version",
        "stock_code",
        "context_hash",
        "valid_until",
        "recommendation_data_date",
        "recommend_status",
        "signal_status",
        "chase_risk_status",
        "ordinary_buy_eligible",
        "event_risk_level",
        "source_health_status",
        "gate_hash",
    }
)


@dataclass(frozen=True)
class BuyGateDecision:
    allowed: bool
    reason_code: str = ""
    detail: str = ""


@dataclass(frozen=True)
class BuyGateLoad:
    binding: dict[str, Any] | None
    reason_code: str = ""
    detail: str = ""


def explicit_database_true(value: Any) -> bool:
    """Accept only a real bool or the exact integer database value ``1``."""

    return value is True or (type(value) is int and value == 1)


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if value in {None, ""}:
        return None
    try:
        return datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _datetime_token(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="microseconds")


def _date_token(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return ""


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in {None, ""}:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _json_without_duplicate_keys(value: Any) -> Any:
    def object_pairs(pairs):
        decoded = {}
        for key, item in pairs:
            if key in decoded:
                raise ValueError(f"duplicate JSON key: {key}")
            decoded[key] = item
        return decoded

    return json.loads(str(value), object_pairs_hook=object_pairs)


def _quality_flags(value: Any) -> tuple[str, ...] | None:
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).upper() for item in value)
    if value in {None, ""}:
        return tuple()
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, list):
        return None
    return tuple(str(item).upper() for item in decoded)


def _positive_number(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _worst_event_risk(*values: Any) -> str:
    normalized = [str(value or "").upper() for value in values]
    return max(normalized, key=lambda item: _EVENT_RANK.get(item, 5))


def _gate_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": GATE_SCHEMA_VERSION,
        "decision_run_uid": str(binding.get("decision_run_uid") or ""),
        "strategy_version": str(binding.get("strategy_version") or ""),
        "stock_code": str(binding.get("stock_code") or "").zfill(6),
        "context_hash": str(binding.get("context_hash") or "").lower(),
        "valid_until": str(binding.get("valid_until") or ""),
        "recommendation_data_date": str(
            binding.get("recommendation_data_date") or ""
        ),
        "recommend_status": str(
            binding.get("recommend_status") or "DATA_BLOCKED"
        ).upper(),
        "signal_status": str(
            binding.get("signal_status") or "WATCH"
        ).upper(),
        "chase_risk_status": str(
            binding.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper(),
        "ordinary_buy_eligible": explicit_database_true(
            binding.get("ordinary_buy_eligible")
        ),
        "event_risk_level": str(
            binding.get("event_risk_level") or "DATA_BLOCKED"
        ).upper(),
        "source_health_status": str(
            binding.get("source_health_status") or "DATA_BLOCKED"
        ).upper(),
    }


def build_buy_gate_binding(
    *,
    decision_run_uid: str,
    strategy_version: str,
    stock_code: str,
    context_hash: str,
    valid_until: datetime,
    recommendation_data_date: Any,
    recommend_status: str,
    signal_status: str,
    chase_risk_status: str,
    ordinary_buy_eligible: bool,
    event_risk_level: str,
    source_health_status: str,
) -> dict[str, Any]:
    payload = {
        "module": GATE_MODULE,
        "schema_version": GATE_SCHEMA_VERSION,
        "decision_run_uid": str(decision_run_uid or ""),
        "strategy_version": str(strategy_version or ""),
        "stock_code": str(stock_code or "").zfill(6),
        "context_hash": str(context_hash or "").lower(),
        "valid_until": _datetime_token(valid_until),
        "recommendation_data_date": _date_token(
            recommendation_data_date
        ),
        "recommend_status": str(
            recommend_status or "DATA_BLOCKED"
        ).upper(),
        "signal_status": str(signal_status or "WATCH").upper(),
        "chase_risk_status": str(
            chase_risk_status or "DATA_BLOCKED"
        ).upper(),
        "ordinary_buy_eligible": explicit_database_true(
            ordinary_buy_eligible
        ),
        "event_risk_level": str(
            event_risk_level or "DATA_BLOCKED"
        ).upper(),
        "source_health_status": str(
            source_health_status or "DATA_BLOCKED"
        ).upper(),
    }
    payload["gate_hash"] = canonical_json_hash(_gate_payload(payload))
    return payload


def append_buy_gate_binding(
    evidence: Sequence[Any],
    binding: Mapping[str, Any],
) -> tuple[Any, ...]:
    retained = tuple(
        item
        for item in evidence
        if not (
            isinstance(item, Mapping)
            and str(item.get("module") or "") == GATE_MODULE
        )
    )
    return (*retained, dict(binding))


def bound_buy_gate(evidence_json: Any) -> dict[str, Any] | None:
    def canonical_receipt(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        receipt = dict(value)
        if (
            str(receipt.get("module") or "") != GATE_MODULE
            or not _BUY_GATE_RECEIPT_FIELDS.issubset(receipt)
        ):
            return None
        return receipt

    if isinstance(evidence_json, Mapping):
        return canonical_receipt(evidence_json.get(GATE_MODULE))
    if isinstance(evidence_json, (list, tuple)):
        evidence = list(evidence_json)
    else:
        try:
            evidence = _json_without_duplicate_keys(evidence_json or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if isinstance(evidence, Mapping):
        return canonical_receipt(evidence.get(GATE_MODULE))
    if not isinstance(evidence, list):
        return None
    matches = [
        receipt
        for item in evidence
        if (receipt := canonical_receipt(item)) is not None
    ]
    return matches[0] if len(matches) == 1 else None


def evaluate_buy_gate(
    *,
    now: datetime,
    decision_run_uid: str,
    strategy_version: str,
    stock_code: str,
    bound: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
) -> BuyGateDecision:
    if bound is None or current is None:
        return BuyGateDecision(
            False,
            "BUY_GATE_EVIDENCE_MISSING",
            "bound or current canonical BUY gate evidence is missing",
        )
    for label, value in (("bound", bound), ("current", current)):
        if str(value.get("schema_version") or "") != GATE_SCHEMA_VERSION:
            return BuyGateDecision(
                False,
                "BUY_GATE_EVIDENCE_MISSING",
                f"{label} BUY gate schema is missing or unsupported",
            )
        gate_hash = str(value.get("gate_hash") or "").lower()
        if not _SHA256.fullmatch(gate_hash) or gate_hash != canonical_json_hash(
            _gate_payload(value)
        ):
            return BuyGateDecision(
                False,
                "BUY_GATE_HASH_MISMATCH",
                f"{label} BUY gate hash is invalid",
            )
    identity = {
        "decision_run_uid": str(decision_run_uid or ""),
        "strategy_version": str(strategy_version or ""),
        "stock_code": str(stock_code or "").zfill(6),
    }
    if any(
        str(bound.get(key) or "") != expected
        or str(current.get(key) or "") != expected
        for key, expected in identity.items()
    ):
        return BuyGateDecision(
            False,
            "BUY_GATE_EVIDENCE_MISSING",
            "BUY gate identity differs from its order intent",
        )
    bound_recommendation_date = _date_token(
        bound.get("recommendation_data_date")
    )
    current_recommendation_date = _date_token(
        current.get("recommendation_data_date")
    )
    if (
        not bound_recommendation_date
        or not current_recommendation_date
        or bound_recommendation_date != current_recommendation_date
    ):
        return BuyGateDecision(
            False,
            "BUY_GATE_STALE_RECOMMENDATION",
            "BUY gate recommendation identity/date is missing or changed",
        )
    valid_until = _datetime(current.get("valid_until"))
    bound_valid_until = _datetime(bound.get("valid_until"))
    if valid_until is None or bound_valid_until is None:
        return BuyGateDecision(
            False,
            "BUY_GATE_EVIDENCE_MISSING",
            "BUY gate validity is missing",
        )
    try:
        expired = now > valid_until or now > bound_valid_until
    except TypeError:
        return BuyGateDecision(
            False,
            "BUY_GATE_EVIDENCE_MISSING",
            "BUY gate validity timezone differs from execution time",
        )
    if expired:
        return BuyGateDecision(
            False,
            "BUY_GATE_EXPIRED",
            "BUY gate evidence is no longer valid",
        )
    bound_context = str(bound.get("context_hash") or "").lower()
    current_context = str(current.get("context_hash") or "").lower()
    if (
        not _SHA256.fullmatch(bound_context)
        or not _SHA256.fullmatch(current_context)
        or bound_context != current_context
    ):
        return BuyGateDecision(
            False,
            "BUY_CONTEXT_HASH_MISMATCH",
            "decision context changed after approval",
        )
    if str(current.get("source_health_status") or "").upper() != "PASS":
        return BuyGateDecision(
            False,
            "BUY_GATE_DATA_BLOCKED",
            "current event or decision source health is not PASS",
        )
    event_risk = str(current.get("event_risk_level") or "").upper()
    if event_risk not in EXECUTABLE_EVENT_RISKS:
        return BuyGateDecision(
            False,
            "BUY_GATE_MAJOR_EVENT",
            f"current event risk is {event_risk or 'UNKNOWN'}",
        )
    if str(current.get("recommend_status") or "").upper() != "ALLOW":
        return BuyGateDecision(
            False,
            "BUY_GATE_REVOKED",
            "current recommendation status is not ALLOW",
        )
    if (
        str(current.get("signal_status") or "").upper()
        not in ACTIONABLE_SIGNAL_STATUSES
    ):
        return BuyGateDecision(
            False,
            "BUY_GATE_REVOKED",
            "current signal is not CONFIRM or BUY_READY",
        )
    if (
        str(current.get("chase_risk_status") or "").upper() != "ALLOW"
        or not explicit_database_true(
            current.get("ordinary_buy_eligible")
        )
    ):
        return BuyGateDecision(
            False,
            "BUY_GATE_REVOKED",
            "current chase or ordinary-fill eligibility gate was revoked",
        )
    if str(bound.get("gate_hash") or "").lower() != str(
        current.get("gate_hash") or ""
    ).lower():
        return BuyGateDecision(
            False,
            "BUY_GATE_HASH_MISMATCH",
            "current gate facts differ from the caller-bound receipt",
        )
    return BuyGateDecision(True)


def _table_columns(
    connection: Connection,
    table_name: str,
) -> set[str]:
    inspector = inspect(connection, raiseerr=False)
    if inspector is None or not inspector.has_table(table_name):
        return set()
    return {str(item["name"]) for item in inspector.get_columns(table_name)}


def _current_recommendation(
    connection: Connection,
    *,
    stock_code: str,
    as_of: datetime,
    lock: bool,
) -> dict[str, Any] | None:
    columns = _table_columns(connection, "st_recommended_stocks")
    if not _RECOMMENDATION_REQUIRED_COLUMNS.issubset(columns):
        return None
    lock_clause = (
        " FOR UPDATE"
        if lock and connection.dialect.name.lower() in {"mysql", "mariadb"}
        else ""
    )
    row = connection.execute(
        text(
            "SELECT stock_code, pick_date, recommend_status, signal_status, "
            "chase_risk_status, ordinary_buy_eligible, event_risk_level, "
            "data_quality_score, data_quality_flags "
            "FROM st_recommended_stocks "
            "WHERE stock_code = :stock_code AND pick_date <= :as_of_date "
            "ORDER BY pick_date DESC LIMIT 1" + lock_clause
        ),
        {
            "stock_code": str(stock_code or "").zfill(6),
            "as_of_date": as_of.date(),
        },
    ).mappings().first()
    return dict(row) if row else None


def load_current_buy_gate(
    connection: Connection,
    *,
    decision_run_uid: str,
    strategy_version: str,
    stock_code: str,
    as_of: datetime,
    lock: bool = True,
) -> BuyGateLoad:
    lock_clause = (
        " FOR UPDATE"
        if lock and connection.dialect.name.lower() in {"mysql", "mariadb"}
        else ""
    )
    signal_row = connection.execute(
        text(
            "SELECT s.action, s.competition_status, s.rejection_code, "
            "s.raw_features_json, s.valid_from, s.valid_until, "
            "s.data_snapshot_hash, r.status AS run_status, "
            "r.trade_date AS decision_trade_date, "
            "d.quality_status AS snapshot_quality_status, "
            "d.data_snapshot_hash AS snapshot_data_hash "
            "FROM st_strategy_signal_v2 s "
            "JOIN st_decision_run_v2 r ON r.run_uid = s.run_uid "
            "JOIN st_data_snapshot_v2 d ON d.snapshot_id = r.snapshot_id "
            "WHERE s.run_uid = :run_uid "
            "AND s.strategy_version = :strategy_version "
            "AND s.stock_code = :stock_code" + lock_clause
        ),
        {
            "run_uid": str(decision_run_uid or ""),
            "strategy_version": str(strategy_version or ""),
            "stock_code": str(stock_code or "").zfill(6),
        },
    ).mappings().first()
    if not signal_row:
        return BuyGateLoad(
            None,
            "BUY_GATE_EVIDENCE_MISSING",
            "current strategy signal is missing",
        )
    signal = dict(signal_row)
    recommendation = _current_recommendation(
        connection,
        stock_code=stock_code,
        as_of=as_of,
        lock=lock,
    )
    if recommendation is None:
        return BuyGateLoad(
            None,
            "BUY_GATE_DATA_BLOCKED",
            "current recommendation gate or its required columns are missing",
        )
    decision_trade_date = _date_token(signal.get("decision_trade_date"))
    recommendation_data_date = _date_token(recommendation.get("pick_date"))
    if (
        not decision_trade_date
        or not recommendation_data_date
        or recommendation_data_date != decision_trade_date
    ):
        return BuyGateLoad(
            None,
            "BUY_GATE_STALE_RECOMMENDATION",
            "latest recommendation date is not identical to the decision trade date",
        )
    raw = _json_object(signal.get("raw_features_json"))
    flags = _quality_flags(recommendation.get("data_quality_flags"))
    valid_from = _datetime(signal.get("valid_from"))
    valid_until = _datetime(signal.get("valid_until"))
    try:
        timing_pass = (
            valid_from is not None
            and valid_until is not None
            and valid_from <= as_of <= valid_until
        )
    except TypeError:
        timing_pass = False
    raw_recommend_status = str(
        raw.get("source_recommend_status")
        or raw.get("recommend_status")
        or "DATA_BLOCKED"
    ).upper()
    current_recommend_status = str(
        recommendation.get("recommend_status") or "DATA_BLOCKED"
    ).upper()
    recommend_status = (
        "ALLOW"
        if raw_recommend_status == current_recommend_status == "ALLOW"
        else next(
            (
                value
                for value in (
                    raw_recommend_status,
                    current_recommend_status,
                )
                if value != "ALLOW"
            ),
            "DATA_BLOCKED",
        )
    )
    raw_signal_status = str(
        raw.get("source_signal_status") or "WATCH"
    ).upper()
    current_signal_status = str(
        recommendation.get("signal_status") or "WATCH"
    ).upper()
    signal_status = (
        current_signal_status
        if raw_signal_status in ACTIONABLE_SIGNAL_STATUSES
        and current_signal_status in ACTIONABLE_SIGNAL_STATUSES
        else next(
            (
                value
                for value in (raw_signal_status, current_signal_status)
                if value not in ACTIONABLE_SIGNAL_STATUSES
            ),
            "WATCH",
        )
    )
    source_pass = all(
        (
            str(signal.get("run_status") or "").upper() == "COMPLETED",
            str(signal.get("snapshot_quality_status") or "").upper()
            == "PASS",
            str(signal.get("action") or "").upper() == "BUY",
            str(signal.get("competition_status") or "").upper()
            in ALLOWED_COMPETITION_STATUSES,
            signal.get("rejection_code") in {None, ""},
            raw_recommend_status == "ALLOW",
            current_recommend_status == "ALLOW",
            raw_signal_status in ACTIONABLE_SIGNAL_STATUSES,
            current_signal_status in ACTIONABLE_SIGNAL_STATUSES,
            str(signal.get("data_snapshot_hash") or "").lower()
            == str(signal.get("snapshot_data_hash") or "").lower(),
            _SHA256.fullmatch(
                str(signal.get("data_snapshot_hash") or "").lower()
            )
            is not None,
            _positive_number(raw.get("data_quality_score")),
            _positive_number(recommendation.get("data_quality_score")),
            flags is not None,
            not any(
                marker in flag
                for flag in (flags or ())
                for marker in _BLOCKED_QUALITY_MARKERS
            ),
            timing_pass,
        )
    )
    raw_chase = str(
        raw.get("source_chase_risk_status")
        or raw.get("chase_risk_status")
        or "DATA_BLOCKED"
    ).upper()
    recommendation_chase = str(
        recommendation.get("chase_risk_status") or "DATA_BLOCKED"
    ).upper()
    chase_status = (
        "ALLOW"
        if raw_chase == "ALLOW" and recommendation_chase == "ALLOW"
        else next(
            (
                value
                for value in (raw_chase, recommendation_chase)
                if value != "ALLOW"
            ),
            "DATA_BLOCKED",
        )
    )
    eligible = explicit_database_true(
        raw.get(
            "source_ordinary_buy_eligible",
            raw.get("ordinary_buy_eligible"),
        )
    ) and explicit_database_true(
        recommendation.get("ordinary_buy_eligible")
    )
    event_risk = _worst_event_risk(
        raw.get("event_risk_level") or raw.get("risk_level"),
        recommendation.get("event_risk_level"),
    )
    if valid_until is None:
        return BuyGateLoad(
            None,
            "BUY_GATE_EVIDENCE_MISSING",
            "strategy signal validity is missing",
        )
    binding = build_buy_gate_binding(
        decision_run_uid=decision_run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        context_hash=str(signal.get("data_snapshot_hash") or ""),
        valid_until=valid_until,
        recommendation_data_date=recommendation_data_date,
        recommend_status=recommend_status,
        signal_status=signal_status,
        chase_risk_status=chase_status,
        ordinary_buy_eligible=eligible,
        event_risk_level=event_risk,
        source_health_status="PASS" if source_pass else "DATA_BLOCKED",
    )
    return BuyGateLoad(binding)


__all__ = [
    "ALLOWED_COMPETITION_STATUSES",
    "BuyGateDecision",
    "BuyGateLoad",
    "GATE_MODULE",
    "GATE_SCHEMA_VERSION",
    "append_buy_gate_binding",
    "bound_buy_gate",
    "build_buy_gate_binding",
    "evaluate_buy_gate",
    "explicit_database_true",
    "load_current_buy_gate",
]
