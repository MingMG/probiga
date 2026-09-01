"""Canonical receipt for the mutable daily analysis strategy partition.

The two output tables are intentionally mutable daily projections.  A
scheduled producer therefore records a digest of the complete persisted
business payload in ``st_recommended_run_history`` in the same transaction as
the projection replacement.  Consumers can replay the latest completed
producer instead of incorrectly assuming that an older run still owns the
mutable partition.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.analysis_output_schema import (
    ANALYSIS_COLUMN_CONTRACT,
    RECOMMENDATION_COLUMN_CONTRACT,
)


ANALYSIS_POOL_RECEIPT_SCHEMA = "probiga.analysis-strategy-pool-publication.v1"
TURNOVER_EVIDENCE_SCHEMA = "probiga.chase-turnover-evidence.v1"
UPPER_LIMIT_EVIDENCE_SCHEMA = "probiga.chase-upper-limit-evidence.v1"
PRELIMINARY_UPPER_SUBJECT_SCHEMA = (
    "probiga.analysis-preliminary-upper-subject.v2"
)
TURNOVER_DIRECT_FORMULA = "EASTMONEY_PUSH2HIS_F61_PERCENT"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
ANALYSIS_POOL_PUBLISHER_TASK_TYPES = frozenset({
    "analysis_fast",
    "analysis_morning_strict",
    "analysis_premarket_external",
})
CANONICAL_ANALYSIS_COLUMNS = tuple(
    name
    for name in ANALYSIS_COLUMN_CONTRACT
    if name not in {"id", "created_at", "updated_at"}
)
CANONICAL_RECOMMENDATION_COLUMNS = tuple(
    name
    for name in RECOMMENDATION_COLUMN_CONTRACT
    if name not in {
        "id",
        "created_at",
        "last_check_time",
        # These are two-phase publication controls.  The candidate fields
        # remain hash-bound while activation deliberately changes only these
        # visible hard gates after scheduler postvalidation succeeds.
        "recommend_status",
        "ordinary_buy_eligible",
        "publication_status",
    }
)


def _canonical_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical strategy pool contains non-finite decimal")
        normalized = format(value.normalize(), "f")
        return "0" if normalized in {"-0", ""} else normalized
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical strategy pool contains non-finite float")
        normalized = format(Decimal(str(value)).normalize(), "f")
        return "0" if normalized in {"-0", ""} else normalized
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (str, int, bool)):
        return value
    return str(value)


def _canonical_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    identities: set[str] = set()
    for source in rows:
        item = {
            column: _canonical_scalar(source.get(column))
            for column in columns
        }
        stock_code = str(item.get("stock_code") or "").strip()
        if re.fullmatch(r"[0-9]{6}", stock_code) is None:
            raise ValueError("canonical strategy pool stock identity is invalid")
        if stock_code in identities:
            raise ValueError("canonical strategy pool contains duplicate stock identity")
        identities.add(stock_code)
        canonical.append(item)
    canonical.sort(key=lambda item: str(item["stock_code"]))
    return canonical


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Bind the complete pre-upper recommendation payload.  Only the fields that
# are *owned by the later exact upper-limit recomputation* may be excluded;
# every score, status, price, text explanation, model field and turnover proof
# capable of changing the final recommendation remains in the ordered root.
_PRELIMINARY_UPPER_DERIVED_FIELDS = frozenset(
    {
        "id",
        "created_at",
        "last_check_time",
        "publisher_run_uid",
        "publication_status",
        "upper_limit_evidence_json",
        "chase_risk_status",
        "ordinary_buy_eligible",
        "candidate_ordinary_buy_eligible",
    }
)
_PRELIMINARY_CANDIDATE_FIELDS = (
    "ranking_score",
    *tuple(
        field
        for field in RECOMMENDATION_COLUMN_CONTRACT
        if field not in _PRELIMINARY_UPPER_DERIVED_FIELDS
    ),
)
_PRELIMINARY_CONTEXT_FIELDS = (
    "membership_snapshot_date",
    "membership_snapshot_source",
    "membership_proof_sha256",
    "pit_common_receipt_root_hash",
    "finance_manifest_hash",
    "event_manifest_hash",
    "chase_bar_window_root_sha256",
    "turnover_snapshot_run_id",
    "turnover_snapshot_semantic_sha256",
    "turnover_authority_identity",
    "turnover_authority_sha256",
    "turnover_authority_set_sha256",
    "turnover_collector_build_sha",
    "turnover_collector_binary_sha256",
    "turnover_full_market_count",
    "turnover_full_market_proof_root_sha256",
    "flow_input_root_sha256",
    "flow_input_count",
    "flow_input_min_etl_sync_at",
    "flow_input_max_etl_sync_at",
    "flow_input_decision_at",
)
_PRELIMINARY_GLOBAL_PROOF_FIELDS = tuple(
    field
    for field in _PRELIMINARY_CONTEXT_FIELDS
    if field not in {
        "finance_manifest_hash",
        "event_manifest_hash",
        "chase_bar_window_root_sha256",
    }
)


def build_preliminary_upper_subject_receipt(
    *,
    trade_date: date | str,
    decision_at: datetime | str,
    build_sha: str,
    model_version: str,
    min_score: Any,
    candidates: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Seal the ordered pre-upper top-80 used as the MyQuant subject."""

    target = (
        trade_date.isoformat()
        if isinstance(trade_date, date)
        else str(trade_date or "").strip()
    )
    try:
        if date.fromisoformat(target).isoformat() != target:
            raise ValueError
    except ValueError as exc:
        raise ValueError("preliminary upper subject trade date is invalid") from exc
    if isinstance(decision_at, datetime):
        decision = decision_at
    else:
        raw_decision = str(decision_at or "").strip()
        try:
            decision = datetime.fromisoformat(raw_decision)
        except ValueError as exc:
            raise ValueError(
                "preliminary upper subject decision time is invalid"
            ) from exc
    if decision.tzinfo is not None or decision.microsecond != 0:
        raise ValueError("preliminary upper subject decision time is invalid")
    build = str(build_sha or "").strip().lower()
    model = str(model_version or "").strip()
    if (
        re.fullmatch(r"[0-9a-f]{40}", build) is None
        or build == "0" * 40
        or not model
        or len(model) > 128
    ):
        raise ValueError("preliminary upper subject build/model identity is invalid")
    ranked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rank, source in enumerate(candidates, start=1):
        code = str(source.get("stock_code") or "").strip().zfill(6)
        if re.fullmatch(r"(?:0|3|6)[0-9]{5}", code) is None or code in seen:
            raise ValueError("preliminary upper subject stock identity is invalid")
        seen.add(code)
        ranked.append({
            "rank": rank,
            "stock_code": code,
            **{
                field: _canonical_scalar(source.get(field))
                for field in (
                    *_PRELIMINARY_CANDIDATE_FIELDS,
                    *_PRELIMINARY_CONTEXT_FIELDS,
                )
                if field != "stock_code"
            },
        })
    if len(ranked) != 80:
        raise ValueError("preliminary upper subject requires exactly 80 stocks")
    if any(
        re.fullmatch(
            r"[0-9a-f]{64}",
            str(item.get("chase_bar_window_root_sha256") or ""),
        ) is None
        for item in ranked
    ):
        raise ValueError(
            "preliminary upper subject chase bar proof is incomplete"
        )
    ordered_codes = [item["stock_code"] for item in ranked]
    input_proofs = {
        tuple(item.get(field) for field in _PRELIMINARY_GLOBAL_PROOF_FIELDS)
        for item in ranked
    }
    if len(input_proofs) != 1:
        raise ValueError("preliminary upper subject input proofs are ambiguous")
    input_proof = dict(zip(
        _PRELIMINARY_GLOBAL_PROOF_FIELDS,
        # This zip is deliberately restricted to facts shared by all 80
        # candidates.  Per-stock finance/event manifests remain bound in the
        # ordered candidate root above.
        next(iter(input_proofs)),
    ))
    sha_fields = (
        "membership_proof_sha256",
        "turnover_snapshot_semantic_sha256",
        "turnover_authority_sha256",
        "turnover_authority_set_sha256",
        "turnover_collector_binary_sha256",
        "turnover_full_market_proof_root_sha256",
        "flow_input_root_sha256",
    )
    if (
        any(
            re.fullmatch(r"[0-9a-f]{64}", str(input_proof.get(field) or ""))
            is None
            for field in sha_fields
        )
        or re.fullmatch(
            r"[0-9a-f]{40}",
            str(input_proof.get("turnover_collector_build_sha") or ""),
        ) is None
        or int(input_proof.get("turnover_full_market_count") or 0) < 3000
        or int(input_proof.get("flow_input_count") or 0) < 3000
        or str(input_proof.get("membership_snapshot_date") or "") != target
        or not str(input_proof.get("membership_snapshot_source") or "")
        or not str(input_proof.get("turnover_snapshot_run_id") or "")
        or not str(input_proof.get("turnover_authority_identity") or "")
    ):
        raise ValueError("preliminary upper subject input proof is incomplete")
    flow_min = datetime.fromisoformat(
        str(input_proof.get("flow_input_min_etl_sync_at") or "")
    )
    flow_max = datetime.fromisoformat(
        str(input_proof.get("flow_input_max_etl_sync_at") or "")
    )
    flow_decision = datetime.fromisoformat(
        str(input_proof.get("flow_input_decision_at") or "")
    )
    close_ready = datetime.combine(date.fromisoformat(target), datetime.min.time()).replace(
        hour=15,
        minute=10,
    )
    if (
        any(item.tzinfo is not None for item in (flow_min, flow_max, flow_decision))
        or flow_min < close_ready
        or flow_max > decision
        or flow_decision != decision
    ):
        raise ValueError("preliminary upper subject flow PIT proof differs")
    code_set_sha256 = canonical_sha256({
        "schema": "probiga.analysis-preliminary-upper-code-set.v1",
        "trade_date": target,
        "stock_codes": sorted(ordered_codes),
    })
    ordered_candidate_sha256 = canonical_sha256({
        "schema": "probiga.analysis-preliminary-upper-ranked-candidates.v1",
        "trade_date": target,
        "candidates": ranked,
    })
    core = {
        "schema": PRELIMINARY_UPPER_SUBJECT_SCHEMA,
        "trade_date": target,
        "decision_at": decision.isoformat(timespec="seconds"),
        "build_sha": build,
        "model_version": model,
        "top_n": 80,
        "min_score": _canonical_scalar(min_score),
        "ordered_stock_codes": ordered_codes,
        "code_set_sha256": code_set_sha256,
        "ordered_candidate_sha256": ordered_candidate_sha256,
        "input_proof": input_proof,
        "ranked_candidates": ranked,
    }
    return {**core, "receipt_sha256": canonical_sha256(core)}


def validate_preliminary_upper_subject_receipt(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("preliminary upper subject receipt must be an object")
    expected_keys = {
        "schema",
        "trade_date",
        "decision_at",
        "build_sha",
        "model_version",
        "top_n",
        "min_score",
        "ordered_stock_codes",
        "code_set_sha256",
        "ordered_candidate_sha256",
        "input_proof",
        "ranked_candidates",
        "receipt_sha256",
    }
    if set(value) != expected_keys or value.get("top_n") != 80:
        raise ValueError("preliminary upper subject receipt contract differs")
    candidates = value.get("ranked_candidates")
    if not isinstance(candidates, list):
        raise ValueError("preliminary upper subject candidates are invalid")
    rebuilt = build_preliminary_upper_subject_receipt(
        trade_date=value.get("trade_date"),
        decision_at=value.get("decision_at"),
        build_sha=str(value.get("build_sha") or ""),
        model_version=str(value.get("model_version") or ""),
        min_score=value.get("min_score"),
        candidates=candidates,
    )
    if dict(value) != rebuilt:
        raise ValueError("preliminary upper subject receipt hash/content differs")
    return rebuilt


def build_turnover_evidence(core: Mapping[str, Any]) -> str:
    unsigned = {**dict(core), "schema": TURNOVER_EVIDENCE_SCHEMA}
    unsigned.pop("proof_sha256", None)
    payload = {**unsigned, "proof_sha256": canonical_sha256(unsigned)}
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def validate_turnover_evidence(value: Any) -> dict[str, Any]:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("turnover evidence JSON is invalid") from exc
    supplied_hash = str(payload.get("proof_sha256") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("proof_sha256", None)
    stock_code = str(payload.get("stock_code") or "")
    trade_date = str(payload.get("trade_date") or "")
    try:
        parsed_trade_date = date.fromisoformat(trade_date)
        decision_known_at = datetime.fromisoformat(
            str(payload.get("decision_known_at") or "")
        )
    except ValueError as exc:
        raise ValueError("turnover evidence date is invalid") from exc
    if (
        payload.get("schema") != TURNOVER_EVIDENCE_SCHEMA
        or re.fullmatch(r"[0-9]{6}", stock_code) is None
        or parsed_trade_date.isoformat() != trade_date
        or decision_known_at.tzinfo is not None
        or decision_known_at.date() < parsed_trade_date
        or re.fullmatch(r"[0-9a-f]{64}", supplied_hash) is None
        or supplied_hash != canonical_sha256(unsigned)
        or payload.get("status") not in {"PASS", "DATA_BLOCKED"}
    ):
        raise ValueError("turnover evidence identity differs")
    if payload.get("status") == "DATA_BLOCKED":
        if not str(payload.get("reason") or "").startswith("DATA_BLOCKED:"):
            raise ValueError("turnover DATA_BLOCKED evidence differs")
        return payload
    try:
        volume = Decimal(str(payload.get("volume")))
        turnover = Decimal(str(payload.get("turnover_ratio")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("turnover evidence values are invalid") from exc
    if not volume.is_finite() or volume < 0 or not turnover.is_finite() or turnover < 0:
        raise ValueError("turnover evidence values are invalid")
    try:
        source_trade_date = date.fromisoformat(
            str(payload.get("source_trade_date") or "")
        )
        captured_at = datetime.fromisoformat(
            str(payload.get("captured_at") or "")
        )
        provider_http_at = parsedate_to_datetime(
            str(payload.get("provider_http_date") or "")
        )
        qmt_received_at = datetime.fromisoformat(
            str(payload.get("qmt_received_at") or "")
        )
        source_values = tuple(
            Decimal(str(payload.get(f"source_{name}")))
            for name in ("open", "high", "low", "close", "volume_shares")
        )
        qmt_values = tuple(
            Decimal(str(payload.get(f"qmt_{name}")))
            for name in ("open", "high", "low", "close", "volume_shares")
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("direct turnover snapshot inputs are invalid") from exc
    provider_http_local = (
        provider_http_at.astimezone(_SHANGHAI).replace(tzinfo=None)
        if provider_http_at.tzinfo is not None
        else provider_http_at
    )
    if (
        payload.get("source_table") != "st_market_field_capture_row"
        or payload.get("formula") != TURNOVER_DIRECT_FORMULA
        or payload.get("provider") != "eastmoney.push2his.kline"
        or payload.get("transport_contract") not in {
            "HTTPS_TLS_VERIFIED_DIRECT",
            "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1",
        }
        or not str(payload.get("resolved_endpoint") or "").strip()
        or len(str(payload.get("resolved_endpoint") or "")) > 160
        or payload.get("source_field") != "f61"
        or payload.get("unit") != "PERCENT"
        or source_trade_date != parsed_trade_date
        or captured_at.tzinfo is not None
        or captured_at > decision_known_at
        or provider_http_local.tzinfo is not None
        or provider_http_local > decision_known_at
        or qmt_received_at.tzinfo is not None
        or qmt_received_at > decision_known_at
        or payload.get("qmt_data_source") != "gj_big_qmt_inner"
        or payload.get("qmt_permission_status") != "SUPPORTED"
        or not str(payload.get("qmt_batch_id") or "").strip()
        or not str(payload.get("qmt_data_version") or "").strip()
        or payload.get("qmt_quality_status") != "QMT_ATTESTED"
        or source_values != qmt_values
        or volume != source_values[-1]
        or any(not item.is_finite() or item <= 0 for item in source_values)
        or turnover
        != turnover.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        or re.fullmatch(
            r"[0-9a-f]{32}", str(payload.get("snapshot_run_id") or "")
        ) is None
        or re.fullmatch(
            r"[0-9a-f]{40}", str(payload.get("collector_build_sha") or "")
        ) is None
        or str(payload.get("collector_build_sha") or "") == "0" * 40
        or payload.get("authority_proof_kind") != "QMT_DAILY_MARKET_TRUTH"
        or not str(payload.get("authority_proof_identity") or "").strip()
        or len(str(payload.get("authority_proof_identity") or "")) > 128
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(payload.get(name) or ""))
            is None
            for name in (
                "collector_binary_sha256",
                "authority_proof_sha256",
                "authority_set_sha256",
                "raw_payload_sha256",
                "snapshot_row_sha256",
                "snapshot_semantic_sha256",
            )
        )
        or any(
            str(payload.get(name) or "") == "0" * 64
            for name in (
                "collector_binary_sha256",
                "authority_proof_sha256",
                "authority_set_sha256",
            )
        )
    ):
        raise ValueError("direct turnover snapshot proof differs")
    return payload


def build_upper_limit_evidence(core: Mapping[str, Any]) -> str:
    unsigned = {**dict(core), "schema": UPPER_LIMIT_EVIDENCE_SCHEMA}
    unsigned.pop("proof_sha256", None)
    payload = {**unsigned, "proof_sha256": canonical_sha256(unsigned)}
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def validate_upper_limit_evidence(value: Any) -> dict[str, Any]:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("upper-limit evidence JSON is invalid") from exc
    unsigned = dict(payload)
    supplied_hash = str(unsigned.pop("proof_sha256", "") or "").lower()
    try:
        trade_date = date.fromisoformat(str(payload.get("trade_date") or ""))
        window_start = date.fromisoformat(
            str(payload.get("window_start_date") or payload.get("trade_date") or "")
        )
        window_end = date.fromisoformat(
            str(payload.get("window_end_date") or payload.get("trade_date") or "")
        )
        decision_at = datetime.fromisoformat(
            str(payload.get("decision_known_at") or "")
        )
        captured_at = datetime.fromisoformat(
            str(payload.get("captured_at") or payload.get("decision_known_at") or "")
        )
    except ValueError as exc:
        raise ValueError("upper-limit evidence date is invalid") from exc
    stock_code = str(payload.get("stock_code") or "")
    subject_identity = str(payload.get("subject_identity") or "")
    preliminary_payload_sha = str(
        payload.get("preliminary_receipt_payload_sha256") or ""
    ).lower()
    if (
        payload.get("schema") != UPPER_LIMIT_EVIDENCE_SCHEMA
        or re.fullmatch(r"[0-9]{6}", stock_code) is None
        or trade_date.isoformat() != str(payload.get("trade_date") or "")
        or decision_at.tzinfo is not None
        or captured_at.tzinfo is not None
        or captured_at > decision_at
        or window_start > window_end
        or window_end != trade_date
        or re.fullmatch(r"[0-9a-f]{64}", supplied_hash) is None
        or supplied_hash != canonical_sha256(unsigned)
        or payload.get("status") not in {"PASS", "DATA_BLOCKED"}
    ):
        raise ValueError("upper-limit evidence identity differs")
    if payload.get("status") == "DATA_BLOCKED":
        if not str(payload.get("reason") or "").startswith("DATA_BLOCKED:"):
            raise ValueError("upper-limit DATA_BLOCKED evidence differs")
        return payload
    if (
        payload.get("source_table") != "st_market_field_capture_row"
        or payload.get("capture_kind") != "DAILY_UPPER_LIMIT_HISTORY"
        or payload.get("provider") != "myquant.gm.get_history_instruments"
        or payload.get("source_field") != "upper_limit"
        or payload.get("unit") != "PRICE_CNY"
        or payload.get("transport_contract")
        != "MYQUANT_GM_SDK_FIXED_ACTION_V1"
        or payload.get("entitlement_status") != "SUPPORTED"
        or str(payload.get("timezone") or "") != "Asia/Shanghai"
        or not 1 <= int(payload.get("expected_stock_count") or 0) <= 80
        or int(payload.get("expected_date_count") or 0) != 21
        or re.fullmatch(
            r"[0-9a-f]{32}", str(payload.get("snapshot_run_id") or "")
        )
        is None
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(payload.get(name) or ""))
            is None
            for name in (
                "expected_keyset_sha256",
                "subject_sha256",
                "code_set_sha256",
                "trade_dates_sha256",
                "calendar_manifest_sha256",
                "calendar_session_set_sha256",
                "snapshot_semantic_sha256",
                "stock_rows_sha256",
                "provider_response_sha256",
                "canonical_request_sha256",
                "worker_sha256",
            )
        )
        or not str(payload.get("sdk_version") or "").strip()
        or not str(payload.get("python_version") or "").strip()
        or not str(payload.get("subject_identity") or "").strip()
        or len(str(payload.get("subject_identity") or "")) > 128
        or (
            subject_identity.startswith("preview:")
            and (
                re.fullmatch(r"[0-9a-f]{64}", preliminary_payload_sha)
                is None
                or preliminary_payload_sha == "0" * 64
            )
        )
        or (
            not subject_identity.startswith("preview:")
            and bool(preliminary_payload_sha)
        )
        or not str(payload.get("calendar_batch_id") or "").strip()
        or len(str(payload.get("calendar_batch_id") or "")) > 64
    ):
        raise ValueError("direct upper-limit snapshot proof differs")
    return payload


def explicit_database_true(value: Any) -> bool:
    if value is True or value == 1:
        return True
    return str(value or "").strip().lower() in {"1", "true"}


def is_executable_recommendation(row: Mapping[str, Any]) -> bool:
    """The single four-gate definition used by writer and validator."""

    recommend_status = row.get("candidate_recommend_status")
    if not str(recommend_status or "").strip():
        recommend_status = row.get("recommend_status")
    ordinary_eligible = row.get("candidate_ordinary_buy_eligible")
    if ordinary_eligible is None:
        ordinary_eligible = row.get("ordinary_buy_eligible")
    return bool(
        str(recommend_status or "").strip().upper() == "ALLOW"
        and str(row.get("signal_status") or "").strip().upper()
        in {"BUY_READY", "CONFIRM"}
        and str(row.get("chase_risk_status") or "").strip().upper()
        == "ALLOW"
        and explicit_database_true(ordinary_eligible)
    )


def is_research_only_recommendation(row: Mapping[str, Any]) -> bool:
    """Return whether a persisted candidate is explicitly non-executable."""

    candidate_status = row.get("candidate_recommend_status")
    if not str(candidate_status or "").strip():
        candidate_status = row.get("recommend_status")
    candidate_ordinary = row.get("candidate_ordinary_buy_eligible")
    if candidate_ordinary is None:
        candidate_ordinary = row.get("ordinary_buy_eligible")
    return bool(
        str(candidate_status or "").strip().upper() == "SUSPENDED"
        and not explicit_database_true(candidate_ordinary)
        and not is_executable_recommendation(row)
    )


def research_only_publication_is_safe(manifest: Mapping[str, Any]) -> bool:
    """Accept a visible pool with no order authority only when every pick is sealed research-only."""

    try:
        recommendation_count = int(manifest.get("recommendation_count") or 0)
        executable_count = int(manifest.get("executable_count") or 0)
        research_only_count = int(manifest.get("research_only_count") or 0)
    except (TypeError, ValueError):
        return False
    return bool(
        manifest.get("publication_mode") == "RESEARCH_ONLY"
        and recommendation_count > 0
        and executable_count == 0
        and research_only_count == recommendation_count
    )


def _live_publication_gates_aligned(row: Mapping[str, Any]) -> bool:
    status = str(row.get("publication_status") or "").strip().upper()
    live_recommend = str(
        row.get("recommend_status") or ""
    ).strip().upper()
    candidate_recommend = str(
        row.get("candidate_recommend_status") or ""
    ).strip().upper()
    live_ordinary = explicit_database_true(
        row.get("ordinary_buy_eligible")
    )
    candidate_ordinary = explicit_database_true(
        row.get("candidate_ordinary_buy_eligible")
    )
    if status == "PENDING":
        return live_recommend == "PENDING" and not live_ordinary
    if status == "ACTIVE":
        return (
            bool(candidate_recommend)
            and live_recommend == candidate_recommend
            and live_ordinary == candidate_ordinary
        )
    return False


def build_pool_manifest(
    *,
    trade_date: str,
    analysis_rows: Iterable[Mapping[str, Any]],
    recommendation_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    target = str(trade_date or "").strip()[:10]
    try:
        if date.fromisoformat(target).isoformat() != target:
            raise ValueError
    except ValueError as exc:
        raise ValueError("canonical strategy pool trade_date is invalid") from exc

    analysis = _canonical_rows(
        analysis_rows,
        columns=CANONICAL_ANALYSIS_COLUMNS,
    )
    recommendation_sources = [dict(row) for row in recommendation_rows]
    recommendations = _canonical_rows(
        recommendation_sources,
        columns=CANONICAL_RECOMMENDATION_COLUMNS,
    )
    publisher_run_uids = sorted({
        str(row.get("publisher_run_uid") or "").strip().lower()
        for row in recommendation_sources
        if str(row.get("publisher_run_uid") or "").strip()
    })
    publication_statuses = sorted({
        str(row.get("publication_status") or "").strip().upper()
        for row in recommendation_sources
        if str(row.get("publication_status") or "").strip()
    })
    membership_proofs = sorted(
        {
            (
                str(row.get("membership_snapshot_date") or "")[:10],
                str(row.get("membership_snapshot_source") or "").strip(),
                str(row.get("membership_proof_sha256") or "")
                .strip()
                .lower(),
            )
            for row in recommendation_sources
            if str(row.get("membership_snapshot_date") or "").strip()
            or str(row.get("membership_snapshot_source") or "").strip()
            or str(row.get("membership_proof_sha256") or "").strip()
        }
    )
    turnover_proofs: list[dict[str, str]] = []
    upper_limit_proofs: list[dict[str, str]] = []
    upper_limit_pass_payloads: list[dict[str, Any]] = []
    for row in recommendation_sources:
        evidence = validate_turnover_evidence(
            row.get("turnover_evidence_json")
        )
        upper_evidence = validate_upper_limit_evidence(
            row.get("upper_limit_evidence_json")
        )
        if (
            evidence["stock_code"]
            != str(row.get("stock_code") or "").strip()
            or evidence["trade_date"] != target
            or upper_evidence["stock_code"]
            != str(row.get("stock_code") or "").strip()
            or upper_evidence["trade_date"] != target
            or (
                str(row.get("chase_risk_status") or "").upper()
                in {"ALLOW", "BLOCK"}
                and (
                    evidence["status"] != "PASS"
                    or upper_evidence["status"] != "PASS"
                )
            )
        ):
            raise ValueError("execution evidence differs from recommendation")
        turnover_proofs.append({
            "stock_code": evidence["stock_code"],
            "proof_sha256": evidence["proof_sha256"],
        })
        upper_limit_proofs.append({
            "stock_code": upper_evidence["stock_code"],
            "proof_sha256": upper_evidence["proof_sha256"],
        })
        if upper_evidence["status"] == "PASS":
            upper_limit_pass_payloads.append(upper_evidence)
    turnover_proofs.sort(key=lambda item: item["stock_code"])
    upper_limit_proofs.sort(key=lambda item: item["stock_code"])
    if upper_limit_pass_payloads:
        recommendation_codes = sorted(
            str(row.get("stock_code") or "").strip()
            for row in recommendation_sources
        )
        expected_code_set_sha256 = canonical_sha256({
            "schema": "probiga.upper-limit-code-set.v1",
            "target_date": target,
            "stock_codes": recommendation_codes,
        })
        immutable_run_identities = {
            (
                str(item.get("snapshot_run_id") or ""),
                str(item.get("subject_identity") or ""),
                str(item.get("subject_sha256") or ""),
                str(item.get("code_set_sha256") or ""),
                str(item.get("trade_dates_sha256") or ""),
                str(item.get("calendar_batch_id") or ""),
                str(item.get("calendar_manifest_sha256") or ""),
                str(item.get("calendar_session_set_sha256") or ""),
                str(item.get("expected_keyset_sha256") or ""),
                str(item.get("snapshot_semantic_sha256") or ""),
                str(item.get("provider_response_sha256") or ""),
                str(item.get("canonical_request_sha256") or ""),
                str(item.get("worker_sha256") or ""),
            )
            for item in upper_limit_pass_payloads
        }
        if (
            len(recommendation_codes) != len(set(recommendation_codes))
            or len(immutable_run_identities) != 1
            or any(
                int(item.get("expected_stock_count") or 0)
                != len(recommendation_codes)
                or str(item.get("code_set_sha256") or "")
                != expected_code_set_sha256
                for item in upper_limit_pass_payloads
            )
        ):
            raise ValueError(
                "upper-limit immutable subject differs from recommendation pool"
            )
    analysis_sha256 = canonical_sha256({
        "trade_date": target,
        "rows": analysis,
    })
    recommendation_sha256 = canonical_sha256({
        "trade_date": target,
        "rows": recommendations,
    })
    executable_count = sum(
        1 for row in recommendation_sources
        if is_executable_recommendation(row)
    )
    research_only_count = sum(
        1 for row in recommendation_sources
        if is_research_only_recommendation(row)
    )
    if executable_count > 0:
        publication_mode = "EXECUTABLE"
    elif (
        recommendation_sources
        and research_only_count == len(recommendation_sources)
    ):
        publication_mode = "RESEARCH_ONLY"
    else:
        publication_mode = "INVALID"
    live_gate_alignment = all(
        _live_publication_gates_aligned(row)
        for row in recommendation_sources
    )
    core = {
        "schema": ANALYSIS_POOL_RECEIPT_SCHEMA,
        "trade_date": target,
        "analysis_count": len(analysis),
        "recommendation_count": len(recommendations),
        "executable_count": executable_count,
        "research_only_count": research_only_count,
        "publication_mode": publication_mode,
        "publisher_run_uids": publisher_run_uids,
        "membership_proofs": [
            {
                "snapshot_date": item[0],
                "source": item[1],
                "proof_sha256": item[2],
            }
            for item in membership_proofs
        ],
        "turnover_proofs": turnover_proofs,
        "upper_limit_proofs": upper_limit_proofs,
        "live_gate_alignment": live_gate_alignment,
        "analysis_sha256": analysis_sha256,
        "recommendation_sha256": recommendation_sha256,
    }
    return {
        **core,
        # Live two-phase state is deliberately outside the stable digest.
        # Candidate execution fields and publisher UID remain hash-bound.
        "publication_statuses": publication_statuses,
        "canonical_pool_sha256": canonical_sha256(core),
    }


def read_persisted_pool_manifest(connection: Any, trade_date: str) -> dict[str, Any]:
    analysis_columns = ", ".join(f"`{name}`" for name in CANONICAL_ANALYSIS_COLUMNS)
    recommendation_read_columns = (
        *CANONICAL_RECOMMENDATION_COLUMNS,
        "recommend_status",
        "ordinary_buy_eligible",
        "publication_status",
    )
    recommendation_columns = ", ".join(
        f"`{name}`" for name in recommendation_read_columns
    )
    analysis_rows = connection.execute(
        text(
            f"SELECT {analysis_columns} FROM stock_analysis_result "
            "WHERE analysis_date=:trade_date ORDER BY stock_code"
        ),
        {"trade_date": trade_date},
    ).mappings().all()
    recommendation_rows = connection.execute(
        text(
            f"SELECT {recommendation_columns} FROM st_recommended_stocks "
            "WHERE pick_date=:trade_date ORDER BY stock_code"
        ),
        {"trade_date": trade_date},
    ).mappings().all()
    return build_pool_manifest(
        trade_date=trade_date,
        analysis_rows=analysis_rows,
        recommendation_rows=recommendation_rows,
    )


def build_publication_receipt(
    *,
    manifest: Mapping[str, Any],
    run_uid: str,
    publisher_task_type: str,
    build_sha: str,
    published_at: datetime | str,
) -> dict[str, Any]:
    core = {
        **dict(manifest),
        "run_uid": str(run_uid or "").strip().lower(),
        "publisher_task_type": str(publisher_task_type or "").strip(),
        "build_sha": str(build_sha or "").strip().lower(),
        "published_at": _canonical_scalar(published_at),
    }
    return {**core, "receipt_id": canonical_sha256(core)}


def publication_receipt_is_valid(value: Mapping[str, Any]) -> bool:
    supplied = str(value.get("receipt_id") or "").strip().lower()
    core = dict(value)
    core.pop("receipt_id", None)
    return bool(
        value.get("schema") == ANALYSIS_POOL_RECEIPT_SCHEMA
        and re.fullmatch(r"[0-9a-f]{64}", supplied)
        and supplied == canonical_sha256(core)
    )


__all__ = [
    "ANALYSIS_POOL_RECEIPT_SCHEMA",
    "ANALYSIS_POOL_PUBLISHER_TASK_TYPES",
    "PRELIMINARY_UPPER_SUBJECT_SCHEMA",
    "TURNOVER_DIRECT_FORMULA",
    "TURNOVER_EVIDENCE_SCHEMA",
    "CANONICAL_ANALYSIS_COLUMNS",
    "CANONICAL_RECOMMENDATION_COLUMNS",
    "build_pool_manifest",
    "build_publication_receipt",
    "build_preliminary_upper_subject_receipt",
    "build_turnover_evidence",
    "canonical_sha256",
    "explicit_database_true",
    "is_executable_recommendation",
    "is_research_only_recommendation",
    "publication_receipt_is_valid",
    "research_only_publication_is_safe",
    "read_persisted_pool_manifest",
    "validate_turnover_evidence",
    "validate_preliminary_upper_subject_receipt",
]
