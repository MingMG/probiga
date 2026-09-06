# -*- coding: utf-8 -*-
"""Post-run validation for scheduler tasks that are expected to write data."""
from __future__ import annotations

import json
import hashlib
import os
import re
import shlex
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier, routed_read_engine
from server.common.analysis_pool_receipt import (
    ANALYSIS_POOL_RECEIPT_SCHEMA,
    ANALYSIS_POOL_PUBLISHER_TASK_TYPES,
    canonical_sha256,
    publication_receipt_is_valid,
    read_persisted_pool_manifest,
    research_only_publication_is_safe,
)
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.daily_stock_universe import (
    load_daily_stock_universe,
    validate_daily_stock_coverage,
)
from server.common.finance_coverage import (
    coerce_optional_date,
    finance_disclosure_gate,
    report_period_gate_applies,
)
from server.common.hot_rank_source_contract import (
    HOT_RANK_SINA_TASK_TYPE,
    HOT_RANK_SOURCE_TASK_TYPES,
    SINA_ATTENTION_DATA_BLOCK_REASON,
    basic_receipt_disposition as hot_rank_receipt_disposition,
    parse_hot_rank_receipt,
    validate_persisted_hot_rank_receipt,
)
from server.common.pit_facts import (
    canonical_hash,
    load_finance_atomic_batch_seal,
    load_finance_expected_unavailable,
)
from server.common.qmt_stock_catalog import load_target_stock_catalog
from server.common.release_data_readiness_contract import (
    release_catchup_closed_ready_time,
)


@dataclass(frozen=True)
class TableRequirement:
    table: str
    min_rows: int = 1
    date_col: str | None = None
    target: str = "run_date"
    ready_time: str = "00:00"
    distinct_col: str | None = None
    min_distinct: int = 0
    where_sql: str = ""
    freshness_col: str | None = "etl_sync_at"
    require_fresh: bool = True


@dataclass(frozen=True)
class SchedulerValidationResult:
    checked: bool
    ok: bool
    message: str


TRADING_V3_DECISION_TASK_TYPES = frozenset({
    "trading_v3_close_decision",
    "trading_v3_premarket_review",
})
TRADING_V3_DECISION_RESULT_SCHEMA = (
    "probiga.trading-v3-decision-result.v1"
)
_QMT_STOCK_TASK_DATASETS = {
    "qmt_stock_daily_canonical": "daily",
    "qmt_stock_minute_canonical": "minute",
}
_QMT_INDEX_TASK_DATASETS = {
    "qmt_index_current": "current",
    "qmt_index_kline": "kline",
    "qmt_index_minute": "minute",
}
_EASTMONEY_ALIST_TASK_DATASETS = {
    "alist_daily": "daily",
    "alist_info": "info",
}
_QMT_MINUTE_FLOW_TASK_TYPE = "qmt_stock_minute_flow_canonical"
_QMT_CANONICAL_HISTORY_REPAIR_TASK_TYPE = (
    "qmt_canonical_history_gap_repair"
)
_QMT_CANONICAL_HISTORY_REPAIR_SCHEMA = (
    "probiga.qmt-canonical-history-gap-repair-result.v1"
)
_LINUX_RECENT_DATA_GAP_REPAIR_TASK_TYPE = "linux_recent_data_gap_repair"
_LINUX_RECENT_DATA_GAP_REPAIR_SCHEMA = (
    "probiga.linux-recent-data-gap-repair-result.v1"
)
_NOTICE_PROVIDER_ID = "eastmoney_notice"
_NOTICE_DATA_VERSION = hashlib.sha256(
    b"probiga.eastmoney-notice-exact-association.v2"
).hexdigest()
_NOTICE_QUALITY_STATUS = "SOURCE_IDENTITY_VALIDATED"
_NOTICE_PERMISSION_STATUS = "PUBLIC"
_NOTICE_HISTORY_RESULT_SCHEMA = "probiga.notice-history-repair-result.v1"
_NOTICE_HISTORY_LEDGER_SCHEMA = "probiga.notice-history-repair-ledger.v1"
_NOTICE_HISTORY_TASK_TYPE = "notice_eastmoney_historical_repair"
_NOTICE_HISTORY_DATASET = "notice_eastmoney_full_history"
_NOTICE_HISTORY_UNIVERSE_EVIDENCE = (
    "si_all_code_union_existing_notice_stock_code_frozen_v1"
)
_NOTICE_HISTORY_PAGINATION_EVIDENCE = (
    "eastmoney_exact_stock_total_hits_v1"
)
_NOTICE_HISTORY_REPLACEMENT_SCOPE = "one_stock_full_history"
_CAPITAL_FLOW_BATCH_RESULT_SCHEMA = "probiga.capital-flow-batch-result.v1"
_DIRECT_CAPITAL_FLOW_RESULT_SCHEMA = (
    "probiga.direct-capital-flow-daily-verification.v1"
)
_CAPITAL_FLOW_BATCH_TASK_TYPE = "capital_flow_batch_fast"
_CAPITAL_FLOW_BATCH_DATASET = "stock_capital_flow_daily"
_DIRECT_CAPITAL_FLOW_PROVIDER = "gj_big_qmt_inner"
_DIRECT_CAPITAL_FLOW_VERIFICATION_MODE = "direct_qmt_persisted_read_only"
_CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING = "verified_existing_exact"
_CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR = "historical_exact_fallback_repair"
_CAPITAL_FLOW_EXECUTION_CURRENT_LIVE = "current_live_refresh"
_CAPITAL_FLOW_SOURCE_IDS = frozenset({"east_push2delay", "push2his", "push2hist"})
_CAPITAL_FLOW_LIVE_READY_TIME = time(15, 20)
_DIRECT_CAPITAL_FLOW_READY_TIME = time(15, 40)
_TARGET_TURNOVER_TASK_TYPE = "target_turnover_snapshot"
_UPPER_EVIDENCE_TASK_TYPE = "analysis_upper_evidence_prepare"
_QMT_MEMBERSHIP_TASK_TYPE = "qmt_membership_snapshot"
_QMT_MEMBERSHIP_VERIFICATION_SCHEMA = (
    "probiga.qmt-membership-verification.v1"
)
_QMT_MEMBERSHIP_PUBLICATION_SCHEMA = (
    "probiga.qmt-membership-publication.v1"
)
_FINANCE_EXPECTED_UNAVAILABLE_CODES = frozenset({"002731"})
_FINANCE_AUTHORITATIVE_SOURCES = frozenset({
    "adata.finance.core_index",
    "eastmoney.finance.mainfinadata.direct",
})
_FINANCE_LEGAL_EMPTY_RESOLUTION_TYPE = "STATUTORY_NOT_APPLICABLE"
_FINANCE_LEGAL_EMPTY_REASON = "NEW_LISTING_AFTER_DISCLOSURE_DEADLINE"


def _single_nested_machine_payload(
    output: str | None,
    *,
    schema: str,
) -> Mapping[str, Any] | None:
    """Extract one exact receipt, including a bridge envelope's stdout tail."""

    candidates: list[Mapping[str, Any]] = []
    visited_strings: set[str] = set()

    def visit(value: Any, depth: int = 0) -> None:
        if depth > 4:
            return
        if isinstance(value, Mapping):
            if value.get("schema") == schema:
                candidates.append(value)
            for nested in value.values():
                if isinstance(nested, (Mapping, list, tuple, str)):
                    visit(nested, depth + 1)
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                visit(nested, depth + 1)
            return
        if not isinstance(value, str):
            return
        source = value.strip()
        if not source or source in visited_strings:
            return
        visited_strings.add(source)
        for line in source.splitlines():
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                visit(json.loads(candidate), depth + 1)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

    visit(str(output or ""))
    unique: dict[str, Mapping[str, Any]] = {}
    for payload in candidates:
        key = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        unique[key] = payload
    return next(iter(unique.values())) if len(unique) == 1 else None


def _is_hex(value: Any, length: int) -> bool:
    return re.fullmatch(rf"[0-9a-f]{{{length}}}", str(value or "").lower()) is not None


def _receipt_id_is_valid(payload: Mapping[str, Any]) -> bool:
    supplied = str(payload.get("receipt_id") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("receipt_id", None)
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return supplied == expected


def _etf_forward_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.etf-forward-daily-receipt.v1",
    )


def _dividend_baidu_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.stock-dividend-baidu-receipt.v1",
    )


def _notice_eastmoney_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.notice-sync-result.v1",
    )


def _notice_history_repair_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema=_NOTICE_HISTORY_RESULT_SCHEMA,
    )


def _sim_trade_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.sim-trade-task-result.v1",
    )


def _eastmoney_concept_market_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.eastmoney-concept-market-result.v1",
    )


def _sector_heat_east_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.sector-heat-east-result.v1",
    )


def _ths_hot_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.ths-hot-result.v1",
    )


def _news_sync_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.news-sync-result.v1",
    )


def _capital_flow_batch_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    legacy = _single_nested_machine_payload(
        output,
        schema=_CAPITAL_FLOW_BATCH_RESULT_SCHEMA,
    )
    direct = _single_nested_machine_payload(
        output,
        schema=_DIRECT_CAPITAL_FLOW_RESULT_SCHEMA,
    )
    if legacy is not None and direct is not None:
        return None
    return direct or legacy


def _direct_capital_flow_payload_is_valid(
    payload: Mapping[str, Any],
) -> bool:
    if payload.get("schema") != _DIRECT_CAPITAL_FLOW_RESULT_SCHEMA:
        return False
    source_counts = payload.get("source_counts")
    try:
        target = date.fromisoformat(str(payload.get("trade_date") or ""))
        row_count = int(payload.get("row_count"))
        expected_row_count = int(payload.get("expected_row_count"))
        normalized_source_counts = {
            str(source): int(count)
            for source, count in source_counts.items()
        }
    except (AttributeError, TypeError, ValueError, OverflowError):
        return False
    return bool(
        payload.get("status") == "PASS"
        and payload.get("task_type") == _CAPITAL_FLOW_BATCH_TASK_TYPE
        and payload.get("dataset") == _CAPITAL_FLOW_BATCH_DATASET
        and payload.get("provider") == _DIRECT_CAPITAL_FLOW_PROVIDER
        and target.isoformat() == payload.get("trade_date")
        and payload.get("source_trade_date") == target.isoformat()
        and row_count > 0
        and expected_row_count == row_count
        and normalized_source_counts
        == {_DIRECT_CAPITAL_FLOW_PROVIDER: row_count}
        and _is_hex(payload.get("code_set_sha256"), 64)
        and _is_hex(payload.get("partition_sha256"), 64)
        and payload.get("verification_mode")
        == _DIRECT_CAPITAL_FLOW_VERIFICATION_MODE
        and payload.get("read_only") is True
        and payload.get("network_accessed") is False
        and _machine_timestamp(payload.get("verified_at")) is not None
        and _receipt_id_is_valid(payload)
    )


def _capital_flow_execution_payload(
    payload: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Validate the signed execution-mode evidence before DB postvalidation."""

    execution = payload.get("execution")
    source_counts = payload.get("source_counts")
    if not isinstance(execution, Mapping) or not isinstance(source_counts, Mapping):
        return None
    try:
        row_count = int(payload.get("row_count"))
        existing_count = int(execution.get("existing_row_count"))
        missing_count = int(execution.get("missing_before_count"))
        rows_written = int(execution.get("rows_written"))
        target_count = int(execution.get("target_code_count"))
        live_count = int(execution.get("live_primary_row_count"))
        fallback_requested = int(execution.get("fallback_requested_count"))
        fallback_returned = int(execution.get("fallback_returned_count"))
        normalized_source_counts = {
            str(source): int(count)
            for source, count in source_counts.items()
        }
    except (TypeError, ValueError, OverflowError):
        return None
    mode = str(execution.get("mode") or "")
    target_kind = str(execution.get("target_kind") or "")
    partition_sha256 = str(payload.get("partition_sha256") or "").lower()
    captured_at = str(payload.get("captured_at") or "")
    if (
        row_count <= 0
        or min(
            existing_count,
            missing_count,
            rows_written,
            target_count,
            live_count,
            fallback_requested,
            fallback_returned,
        )
        < 0
        or target_count != row_count
        or execution.get("partition_verified") is not True
        or not _is_hex(partition_sha256, 64)
        or execution.get("partition_sha256") != partition_sha256
        or payload.get("execution_mode") != mode
        or execution.get("captured_at") != captured_at
        or _machine_timestamp(captured_at) is None
        or execution.get("source_counts") != source_counts
        or not normalized_source_counts
        or set(normalized_source_counts) - _CAPITAL_FLOW_SOURCE_IDS
        or any(count <= 0 for count in normalized_source_counts.values())
        or sum(normalized_source_counts.values()) != row_count
        or execution.get("network_accessed")
        is not bool(
            execution.get("live_source_called") is True
            or execution.get("historical_fallback_called") is True
        )
    ):
        return None
    if mode == _CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING:
        valid = (
            target_kind == "historical"
            and execution.get("reuse_verified_existing") is True
            and existing_count == row_count
            and missing_count == 0
            and rows_written == 0
            and execution.get("live_source_called") is False
            and execution.get("historical_fallback_called") is False
            and execution.get("network_accessed") is False
            and live_count == fallback_requested == fallback_returned == 0
            and execution.get("partition_replaced") is False
        )
    elif mode == _CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR:
        valid = (
            target_kind == "historical"
            and execution.get("reuse_verified_existing") is True
            and existing_count + missing_count == row_count
            and missing_count > 0
            and 0 <= rows_written <= missing_count
            and execution.get("live_source_called") is False
            and execution.get("historical_fallback_called") is True
            and execution.get("network_accessed") is True
            and live_count == 0
            and fallback_requested == missing_count
            and fallback_returned == missing_count
            and execution.get("partition_replaced") is False
        )
    elif mode == _CAPITAL_FLOW_EXECUTION_CURRENT_LIVE:
        valid = (
            target_kind == "current"
            and existing_count == 0
            and missing_count == row_count
            and rows_written == row_count
            and execution.get("live_source_called") is True
            and execution.get("network_accessed") is True
            and live_count + fallback_returned == row_count
            and fallback_requested == fallback_returned
            and execution.get("historical_fallback_called")
            is (fallback_requested > 0)
            and execution.get("partition_replaced") is True
        )
    else:
        valid = False
    return execution if valid else None


def _validate_capital_flow_persisted_receipt(
    engine: Engine,
    payload: Mapping[str, Any],
) -> tuple[bool, str]:
    """Recompute exact partition identity; do not trust receipt counts alone."""

    if payload.get("schema") == _DIRECT_CAPITAL_FLOW_RESULT_SCHEMA:
        try:
            from tools.verify_direct_capital_flow_daily import inspect_partition

            target = str(payload.get("trade_date") or "")
            evidence = inspect_partition(engine, target)
        except Exception as exc:
            return False, (
                "direct QMT capital-flow persisted partition validation failed: "
                f"{exc}"
            )
        evidence_fields = (
            "trade_date",
            "row_count",
            "expected_row_count",
            "code_set_sha256",
            "partition_sha256",
            "source_counts",
        )
        if any(payload.get(field) != evidence[field] for field in evidence_fields):
            return False, (
                "direct QMT capital-flow persisted partition identity differs "
                "from receipt"
            )
        return True, (
            "direct QMT capital-flow exact persisted partition verified: "
            f"date={target} rows={evidence['row_count']} "
            f"sha256={evidence['partition_sha256']}"
        )

    try:
        from tools import crawl_realtime_batch as flow

        target = str(payload.get("trade_date") or "")
        target_codes = flow._read_target_traded_flow_codes(engine, target)
        stored = flow._read_existing_flow_partition(engine, target)
        verified = flow._validate_exact_flow_frame(
            stored,
            trade_date=target,
            target_codes=target_codes,
        )
        partition_sha256 = flow._flow_partition_sha256(verified)
        source_counts = {
            str(source): int(count)
            for source, count in sorted(
                verified["data_source"].astype(str).value_counts().items()
            )
        }
    except Exception as exc:
        return False, f"capital-flow persisted partition validation failed: {exc}"
    if (
        len(verified) != int(payload.get("row_count") or 0)
        or partition_sha256 != str(payload.get("partition_sha256") or "").lower()
        or source_counts != payload.get("source_counts")
    ):
        return False, "capital-flow persisted partition identity differs from receipt"
    return True, (
        "capital-flow exact persisted partition verified: "
        f"date={target} rows={len(verified)} sha256={partition_sha256}"
    )


def _daily_analysis_evidence_identity(
    task: Mapping[str, Any],
) -> tuple[str, datetime, str]:
    target = str(task.get("_scheduler_pipeline_target_date") or "").strip()
    cutoff_raw = str(
        task.get("_scheduler_pipeline_decision_at") or ""
    ).strip()
    build_sha = str(
        task.get("_scheduler_expected_build_sha") or ""
    ).strip().lower()
    try:
        target_date = date.fromisoformat(target)
        cutoff = datetime.fromisoformat(cutoff_raw)
    except ValueError as exc:
        raise ValueError(
            "daily analysis evidence scheduler identity is invalid"
        ) from exc
    release_catchup = (
        str(task.get("_trigger_source") or "").strip() == "release_catchup"
    )
    if (
        target_date.isoformat() != target
        or cutoff.tzinfo is not None
        or cutoff.microsecond != 0
        or (
            not release_catchup
            and cutoff.date() != target_date
        )
        or (
            release_catchup
            and cutoff
            < datetime.combine(target_date, time(15, 10))
        )
        or cutoff_raw != cutoff.isoformat(timespec="seconds")
        or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or build_sha == "0" * 40
    ):
        raise ValueError(
            "daily analysis evidence scheduler identity is invalid"
        )
    return target, cutoff, build_sha


def _validate_target_turnover_scheduler_receipt(
    task: Mapping[str, Any],
    *,
    engine: Engine,
    output: str | None,
) -> tuple[bool, str]:
    from server.common.analysis_pool_receipt import validate_turnover_evidence
    from server.common.turnover_snapshot import (
        MIN_TURNOVER_UNIVERSE_COUNT,
        TURNOVER_SNAPSHOT_VERSION,
        load_verified_turnover_evidence,
    )

    target, cutoff, build_sha = _daily_analysis_evidence_identity(task)
    payload = _single_nested_machine_payload(
        output,
        schema=TURNOVER_SNAPSHOT_VERSION,
    )
    if payload is None:
        return False, "target turnover exact machine receipt is missing"
    try:
        expected_count = int(payload.get("expected_count") or 0)
        promoted_count = int(payload.get("promoted_count") or 0)
    except (TypeError, ValueError, OverflowError):
        return False, "target turnover machine counters are invalid"
    if (
        payload.get("status") != "COMPLETED"
        or str(payload.get("target_date") or "") != target
        or str(payload.get("decision_at") or "")
        != cutoff.isoformat(timespec="seconds")
        or str(
            (payload.get("validated_by_build_sha") or payload.get("collector_build_sha") or "")
            if payload.get("recovered") is True
            else payload.get("collector_build_sha") or ""
        ).lower()
        != build_sha
        or expected_count < MIN_TURNOVER_UNIVERSE_COUNT
        or promoted_count != expected_count
        or re.fullmatch(
            r"[0-9a-f]{32}", str(payload.get("run_id") or "")
        ) is None
    ):
        return False, "target turnover machine receipt identity differs"
    try:
        evidence = load_verified_turnover_evidence(
            engine,
            target_date=target,
            decision_at=cutoff,
        )
        proofs = [
            validate_turnover_evidence(item["turnover_evidence_json"])
            for item in evidence.values()
        ]
    except Exception as exc:
        return False, f"target turnover persisted receipt is invalid: {exc}"
    if len(evidence) != expected_count or {
        str(item.get("snapshot_run_id") or "") for item in proofs
    } != {str(payload.get("run_id") or "")} or {
        str(item.get("collector_build_sha") or "").lower()
        for item in proofs
    } != {str(payload.get("collector_build_sha") or "").lower()} or {
        str(item.get("snapshot_semantic_sha256") or "") for item in proofs
    } != {str(payload.get("semantic_sha256") or "")}:
        return False, "target turnover persisted proof differs from receipt"
    return True, (
        "target turnover exact immutable snapshot verified: "
        f"date={target} rows={expected_count} run_id={payload['run_id']}"
    )


def _validate_upper_evidence_scheduler_receipt(
    task: Mapping[str, Any],
    *,
    engine: Engine,
    output: str | None,
) -> tuple[bool, str]:
    from biz.analysis.sync_analysis_fast import (
        prepare_preliminary_upper_subject_receipt,
    )
    from server.common.analysis_pool_receipt import validate_upper_limit_evidence
    from server.common.upper_limit_snapshot import (
        UPPER_LIMIT_EXPECTED_DATE_COUNT,
        UPPER_LIMIT_EXPECTED_STOCK_COUNT,
        UPPER_LIMIT_SNAPSHOT_VERSION,
        load_latest_verified_upper_limit_evidence,
    )

    target, cutoff, build_sha = _daily_analysis_evidence_identity(task)
    payload = _single_nested_machine_payload(
        output,
        schema=UPPER_LIMIT_SNAPSHOT_VERSION,
    )
    if payload is None:
        return False, "upper evidence exact machine receipt is missing"
    preliminary = prepare_preliminary_upper_subject_receipt(
        engine,
        trade_date=target,
        decision_at=cutoff,
        build_sha=build_sha,
        min_score=62.0,
    )
    if (
        payload.get("status") != "COMPLETED"
        or str(payload.get("target_date") or "") != target
        or str(payload.get("decision_at") or "")
        != cutoff.isoformat(timespec="seconds")
        or str(payload.get("collector_build_sha") or "").lower()
        != build_sha
        or str(payload.get("preliminary_receipt_sha256") or "")
        != preliminary["receipt_sha256"]
        or int(payload.get("expected_stock_count") or 0)
        != UPPER_LIMIT_EXPECTED_STOCK_COUNT
        or int(payload.get("expected_date_count") or 0)
        != UPPER_LIMIT_EXPECTED_DATE_COUNT
    ):
        return False, "upper evidence machine receipt identity differs"
    evidence = load_latest_verified_upper_limit_evidence(
        engine,
        target_date=target,
        decision_at=cutoff,
        stock_codes=preliminary["ordered_stock_codes"],
        preliminary_receipt_sha256=preliminary["receipt_sha256"],
        preliminary_build_sha=build_sha,
    )
    try:
        proofs = [
            validate_upper_limit_evidence(item["upper_limit_evidence_json"])
            for item in evidence.values()
        ]
    except Exception as exc:
        return False, f"upper evidence persisted receipt is invalid: {exc}"
    if len(evidence) != UPPER_LIMIT_EXPECTED_STOCK_COUNT or {
        str(item.get("snapshot_run_id") or "") for item in proofs
    } != {str(payload.get("run_id") or "")}:
        return False, "upper evidence persisted proof differs from receipt"
    return True, (
        "upper evidence exact immutable snapshot verified: "
        f"date={target} stocks={len(evidence)} run_id={payload['run_id']} "
        f"preview={preliminary['receipt_sha256']}"
    )


def _qmt_membership_verification_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema=_QMT_MEMBERSHIP_VERIFICATION_SCHEMA,
    )


def _qmt_membership_publication_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema=_QMT_MEMBERSHIP_PUBLICATION_SCHEMA,
    )


def _eastmoney_concept_flow_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.eastmoney-concept-flow-result.v1",
    )


def _result_sha256_is_valid(payload: Mapping[str, Any]) -> bool:
    supplied = str(payload.get("result_sha256") or "").lower()
    if not _is_hex(supplied, 64):
        return False
    unsigned = dict(payload)
    unsigned.pop("result_sha256", None)
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return supplied == hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _machine_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if re.fullmatch(
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?",
        raw,
    ) is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is None else None


def _shanghai_machine_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(hours=8):
        return None
    return parsed.replace(tzinfo=None)


def _notice_code_set_hash(values: Any) -> str:
    normalized = sorted(
        {
            str(value).strip().zfill(6)
            for value in values
            if str(value).strip()
        }
    )
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _notice_canonical_timestamp(value: Any) -> str:
    parsed = _coerce_datetime(value)
    if parsed is None:
        return str(value or "").strip()
    return parsed.replace(microsecond=0).isoformat(
        sep=" ", timespec="seconds"
    )


def _notice_canonical_row(row: Mapping[str, Any]) -> dict[str, Any]:
    notice_date = _coerce_date(row.get("notice_date"))
    return {
        "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
        "art_code": str(row.get("art_code") or "").strip(),
        "notice_date": notice_date.isoformat() if notice_date else "",
        "title": str(row.get("title") or "").strip(),
        "column_name": str(row.get("column_name") or "").strip(),
        "display_time": str(row.get("display_time") or "").strip(),
        "detail_url": str(row.get("detail_url") or "").strip(),
        "association_validated": int(
            row.get("association_validated") or 0
        ),
        "qmt_code": str(row.get("qmt_code") or "").strip().upper(),
        "data_source": str(row.get("data_source") or "").strip(),
        "source_time": _notice_canonical_timestamp(row.get("source_time")),
        "received_at": _notice_canonical_timestamp(row.get("received_at")),
        "batch_id": str(row.get("batch_id") or "").strip(),
        "data_version": str(row.get("data_version") or "").strip(),
        "quality_status": str(row.get("quality_status") or "").strip(),
        "permission_status": str(row.get("permission_status") or "").strip(),
    }


def _notice_row_hash(rows: list[Mapping[str, Any]]) -> str:
    canonical = sorted(
        (_notice_canonical_row(row) for row in rows),
        key=lambda row: (row["stock_code"], row["art_code"]),
    )
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _notice_persisted_manifest_hash(
    rows: list[Mapping[str, Any]],
    *,
    requested_codes: list[str],
) -> str:
    rows_by_code: dict[str, list[Mapping[str, Any]]] = {
        code: [] for code in requested_codes
    }
    for row in rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        rows_by_code.setdefault(code, []).append(row)
    manifest = [
        {
            "stock_code": code,
            "row_count": len(rows_by_code.get(code, [])),
            "row_hash": _notice_row_hash(rows_by_code.get(code, [])),
        }
        for code in sorted(requested_codes)
    ]
    payload = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sim_identity_set_hash(values: Any) -> str:
    normalized = sorted(
        {str(value).strip() for value in values if str(value).strip()}
    )
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _qmt_stock_edge_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.qmt-stock-edge-result.v1",
    )


def _qmt_stock_edge_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _qmt_stock_edge_payload(output)
    if payload is None or return_code is None:
        return "failed"
    expected_dataset = _QMT_STOCK_TASK_DATASETS.get(task_type)
    if (
        expected_dataset is None
        or payload.get("dataset") != expected_dataset
        or payload.get("task_type") != task_type
    ):
        return "failed"
    try:
        from tools.sync_qmt_stock_edge import validate_task_result

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    if disposition == "blocked":
        return "blocked"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if (
        disposition != "complete"
        or not _is_hex(expected_build, 40)
        or expected_build == "0" * 40
        or payload.get("build_sha") != expected_build
    ):
        return "failed"
    return "success"


def _qmt_index_edge_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.qmt-index-edge-result.v1",
    )


def _qmt_index_edge_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _qmt_index_edge_payload(output)
    if payload is None or return_code is None:
        return "failed"
    expected_dataset = _QMT_INDEX_TASK_DATASETS.get(task_type)
    if expected_dataset is None or payload.get("dataset") != expected_dataset:
        return "failed"
    try:
        from tools.sync_qmt_index_edge import validate_task_result

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    if disposition == "blocked":
        return "blocked"
    manifest = payload.get("manifest")
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if (
        disposition != "complete"
        or not isinstance(manifest, Mapping)
        or not _is_hex(expected_build, 40)
        or expected_build == "0" * 40
        or str(manifest.get("build_sha") or "").lower() != expected_build
    ):
        return "failed"
    return "success"


def _eastmoney_alist_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.eastmoney-alist-result.v1",
    )


def _eastmoney_alist_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _eastmoney_alist_payload(output)
    expected_dataset = _EASTMONEY_ALIST_TASK_DATASETS.get(task_type)
    if payload is None or return_code is None or expected_dataset is None:
        return "failed"
    if (
        payload.get("dataset") != expected_dataset
        or payload.get("task_type") != task_type
        or payload.get("executor_owner") != "linux_provider"
        or payload.get("provider") != "eastmoney_datacenter"
    ):
        return "failed"
    try:
        from tools.sync_eastmoney_alist_exact import validate_task_result

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    if disposition == "blocked":
        return "blocked"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    return (
        "success"
        if disposition == "complete"
        and _is_hex(expected_build, 40)
        and expected_build != "0" * 40
        and str(payload.get("build_sha") or "").lower() == expected_build
        else "failed"
    )


def _qmt_minute_flow_payload(output: str | None) -> Mapping[str, Any] | None:
    return _single_nested_machine_payload(
        output,
        schema="probiga.qmt-minute-flow-result.v1",
    )


def _qmt_canonical_history_repair_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    if str(output or "").count(_QMT_CANONICAL_HISTORY_REPAIR_SCHEMA) != 1:
        return None
    return _single_nested_machine_payload(
        output,
        schema=_QMT_CANONICAL_HISTORY_REPAIR_SCHEMA,
    )


def _linux_recent_data_gap_repair_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    if str(output or "").count(_LINUX_RECENT_DATA_GAP_REPAIR_SCHEMA) != 1:
        return None
    return _single_nested_machine_payload(
        output,
        schema=_LINUX_RECENT_DATA_GAP_REPAIR_SCHEMA,
    )


def _qmt_minute_flow_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _qmt_minute_flow_payload(output)
    if payload is None or return_code is None:
        return "failed"
    if (
        task_type != _QMT_MINUTE_FLOW_TASK_TYPE
        or payload.get("task_type") != task_type
        or payload.get("dataset") != "stock_minute_capital_flow"
        or payload.get("executor_owner") != "qmt_windows_edge"
        or payload.get("provider") != "gj_qmt_transactioncount1m"
    ):
        return "failed"
    try:
        from tools.sync_qmt_minute_flow_exact import validate_task_result

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    if disposition == "blocked":
        return "blocked"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    return (
        "success"
        if disposition == "complete"
        and _is_hex(expected_build, 40)
        and expected_build != "0" * 40
        and str(payload.get("build_sha") or "").lower() == expected_build
        else "failed"
    )


def _qmt_canonical_history_repair_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _qmt_canonical_history_repair_payload(output)
    if payload is None or return_code is None:
        return "failed"
    try:
        from tools.repair_qmt_canonical_history_gaps import (
            validate_task_result,
        )

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if (
        not _is_hex(expected_build, 40)
        or expected_build == "0" * 40
        or str(payload.get("build_sha") or "").lower() != expected_build
    ):
        return "failed"
    if disposition == "complete":
        return "success"
    # The repair is interval-driven and explicitly resumable. Persist its
    # incomplete receipt as a retryable scheduler failure, never as success.
    return "failed" if payload.get("retryable") is True else "blocked"


def _linux_recent_data_gap_repair_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _linux_recent_data_gap_repair_payload(output)
    if payload is None or return_code is None:
        return "failed"
    try:
        from tools.repair_linux_recent_data_gaps import validate_task_result

        disposition = validate_task_result(payload, int(return_code))
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if (
        not _is_hex(expected_build, 40)
        or expected_build == "0" * 40
        or str(payload.get("build_sha") or "").lower() != expected_build
    ):
        return "failed"
    if disposition == "complete":
        return "success"
    return "failed" if payload.get("retryable") is True else "blocked"


def _trading_v3_decision_payload(
    output: str | None,
) -> Mapping[str, Any] | None:
    """Extract the one schema-labelled V3 result from mixed process logs."""

    source = str(output or "")
    decoder = json.JSONDecoder()
    candidates: list[Mapping[str, Any]] = []
    for match in re.finditer(r"(?m)^\s*\{", source):
        start = match.start() + len(match.group(0)) - 1
        try:
            payload, _end = decoder.raw_decode(source[start:])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, Mapping)
            and payload.get("schema") == TRADING_V3_DECISION_RESULT_SCHEMA
        ):
            candidates.append(payload)
    return candidates[0] if len(candidates) == 1 else None


def _trading_v3_decision_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _trading_v3_decision_payload(output)
    if payload is None or return_code is None:
        return "failed"
    run_uid = str(payload.get("run_uid") or "")
    trade_date_raw = str(payload.get("trade_date") or "")
    forecast_count = payload.get("forecast_count")
    target_count = payload.get("target_count")
    try:
        trade_date = datetime.strptime(trade_date_raw, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return "failed"
    if (
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or trade_date.isoformat() != trade_date_raw
        or isinstance(forecast_count, bool)
        or not isinstance(forecast_count, int)
        or forecast_count < 0
        or isinstance(target_count, bool)
        or not isinstance(target_count, int)
        or target_count < 0
        or target_count > forecast_count
    ):
        return "failed"
    process_status = str(payload.get("status") or "").lower()
    run_status = str(payload.get("run_status") or "").upper()
    actionable_status = str(
        payload.get("actionable_status") or ""
    ).upper()
    retryable = payload.get("retryable")
    if not isinstance(retryable, bool):
        return "failed"
    if forecast_count == 0:
        return (
            "failed"
            if int(return_code) == 2
            and process_status == "blocked"
            and run_status == "BLOCKED"
            and actionable_status == "DATA_BLOCKED"
            and retryable is True
            else "failed"
        )
    if run_status == "BLOCKED":
        return (
            "failed"
            if int(return_code) == 2
            and process_status == "blocked"
            and actionable_status == "DATA_BLOCKED"
            and retryable is True
            else "failed"
        )
    return (
        "success"
        if int(return_code) == 0
        and process_status == "ok"
        and run_status == "COMPLETED"
        and retryable is False
        and actionable_status
        in {"PAPER_ACTIONABLE", "NO_ACTION", "REPLAY_ONLY"}
        else "failed"
    )


def is_market_closed_skip_output(output: str | None) -> bool:
    """Return True for an intentional non-trading-day task skip.

    A skipped intraday task must not be post-validated against today's empty
    tables.  The previous behavior turned every weekend/holiday skip into a
    false scheduler failure and obscured real pipeline failures.
    """
    text_value = str(output or "")
    normalized = text_value.lower()
    return (
        "Skipped automatically:" in text_value
        or "skipped: market closed" in normalized
        or '"status": "skipped"' in text_value
        and '"reason": "market_closed"' in text_value
    )


def _etf_forward_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _etf_forward_payload(output)
    if payload is None or return_code is None:
        return "failed"
    if int(return_code) != 0 or payload.get("status") != "PASS":
        # QMT data availability can recover after the strategy/client is
        # refreshed. Keep a same-day retry eligible instead of terminally
        # suppressing the forward-data task.
        return "failed"
    try:
        trade_date = date.fromisoformat(str(payload.get("trade_date") or ""))
        market = payload["market_data"]
        forward = payload["forward_ledger"]
        groups = market["groups"]
        database = market["database"]
        universe = market["universe"]
        identity = market["source_identity"]
        expected_build = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").lower()
        universe_hash = str(universe["code_set_hash"])
        group_hashes = database["group_hashes"]
    except (KeyError, TypeError, ValueError):
        return "failed"
    forward_status = str(forward.get("status") or "")
    forward_passed = (
        forward_status == "PASS"
        and forward.get("data_date") == trade_date.isoformat()
        and forward.get("write_status") in {"CREATED", "ALREADY_RECORDED"}
    )
    forward_historical_skip = (
        forward_status == "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED"
        and forward.get("data_date") == trade_date.isoformat()
        and not any(
            key in forward
            for key in (
                "write_status",
                "strategy_version",
                "config_hash",
                "input_hash",
                "signal_type",
            )
        )
    )
    if (
        trade_date.isoformat() != str(payload.get("trade_date"))
        or payload.get("provider") != "gj_big_qmt_inner"
        or payload.get("executor_owner") != "qmt_windows_edge"
        or payload.get("automatic_order_submission") is not False
        or market.get("status") != "PASS"
        or market.get("trade_date") != trade_date.isoformat()
        or int(universe.get("count") or 0) != 14
        or not _is_hex(universe_hash, 64)
        or int(database.get("row_count") or 0) != 28
        or not _is_hex(database.get("row_hash"), 64)
        or not _is_hex(expected_build, 40)
        or expected_build == "0" * 40
        or not (
            identity.get("strategy_build_sha") == expected_build
            or (
                identity.get("compatible_app_build_sha") == expected_build
                and identity.get("strategy_compatibility_status") == "CONTENT_COMPATIBLE"
                and _is_hex(identity.get("strategy_build_sha"), 40)
                and identity.get("strategy_build_sha") != "0" * 40
                and _is_hex(identity.get("strategy_git_blob"), 40)
                and all(_is_hex(identity.get(field), 64) for field in (
                    "strategy_source_sha256", "strategy_artifact_sha256",
                    "strategy_loaded_identity_sha256",
                ))
            )
        )
        or identity.get("strategy_identity_frozen") is not True
        or not (forward_passed or forward_historical_skip)
        or payload.get("automatic_order_submission") is not False
        or not _receipt_id_is_valid(payload)
    ):
        return "failed"
    for dividend_type, adjust_type in (("none", 0), ("front", 1)):
        group = groups.get(dividend_type)
        if (
            not isinstance(group, Mapping)
            or group.get("adjust_type") != adjust_type
            or int(group.get("requested_code_count") or 0) != 14
            or int(group.get("responded_code_count") or 0) != 14
            or group.get("requested_code_set_hash") != universe_hash
            or group.get("responded_code_set_hash") != universe_hash
            or not _is_hex(group.get("source_row_hash"), 64)
            or not str(group.get("request_id") or "")
            or not _is_hex(group_hashes.get(dividend_type), 64)
        ):
            return "failed"
    return "success"


def _dividend_baidu_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _dividend_baidu_payload(output)
    if payload is None or return_code is None:
        return "failed"
    if int(return_code) != 0 or payload.get("status") != "PASS":
        # Provider/coverage blocks are transient acquisition failures. The
        # cron retry window must remain open until an exact full-market batch
        # can be published.
        return "failed"
    try:
        sync_date = date.fromisoformat(str(payload.get("sync_date") or ""))
        collection = payload["collection"]
        catalog = payload["catalog"]
        database = payload["database"]
        identity = payload["source_identity"]
        requested = int(collection["requested_code_count"])
        responded = int(collection["responded_code_count"])
        failures = int(collection["failure_count"])
        nonempty = int(collection["nonempty_code_count"])
        authoritative_empty = int(collection["authoritative_empty_code_count"])
        ratio = float(collection["nonempty_code_ratio"])
        row_count = int(collection["row_count"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    expected_git = str(os.environ.get("PROBIGA_EXPECTED_ADATA_SHA") or "").lower()
    expected_tree = str(
        os.environ.get("PROBIGA_EXPECTED_ADATA_TREE_SHA256") or ""
    ).lower()
    requested_hash = collection.get("requested_code_set_hash")
    if (
        sync_date.isoformat() != str(payload.get("sync_date"))
        or payload.get("provider") != "adata_stock_dividend_baidu"
        or payload.get("executor_owner") != "linux_provider"
        or requested <= 0
        or responded != requested
        or failures != 0
        or nonempty < 0
        or authoritative_empty < 0
        or nonempty + authoritative_empty != requested
        or ratio < 0.2
        or row_count <= 0
        or collection.get("responded_code_set_hash") != requested_hash
        or not _is_hex(requested_hash, 64)
        or not _is_hex(collection.get("response_status_manifest_hash"), 64)
        or not _is_hex(collection.get("nonempty_code_set_hash"), 64)
        or not _is_hex(
            collection.get("authoritative_empty_code_set_hash"), 64
        )
        or not _is_hex(collection.get("row_hash"), 64)
        or not str(catalog.get("batch_id") or "")
        or not _is_hex(catalog.get("manifest_hash"), 64)
        or not _is_hex(catalog.get("member_set_hash"), 64)
        or not str(catalog.get("captured_at") or "")
        or catalog.get("target_code_set_hash") != requested_hash
        or int(database.get("row_count") or 0) != row_count
        or database.get("row_hash") != collection.get("row_hash")
        or int(database.get("scope_code_count") or 0) != requested
        or database.get("scope_code_set_hash") != requested_hash
        or not _is_hex(expected_git, 40)
        or not _is_hex(expected_tree, 64)
        or identity.get("git_sha") != expected_git
        or identity.get("tree_sha256") != expected_tree
        or not _receipt_id_is_valid(payload)
    ):
        return "failed"
    return "success"


def _notice_eastmoney_output_status(
    task: Mapping[str, Any],
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _notice_eastmoney_payload(output)
    if payload is None or return_code is None:
        return "failed"
    try:
        raw_counts = {
            key: payload[key]
            for key in (
                "requested_code_count",
                "succeeded_code_count",
                "nonempty_code_count",
                "authoritative_empty_code_count",
                "failed_code_count",
                "pagination_exhausted_code_count",
                "written_notice_count",
                "replaced_existing_notice_count",
            )
        }
        requested = int(raw_counts["requested_code_count"])
        succeeded = int(raw_counts["succeeded_code_count"])
        nonempty = int(raw_counts["nonempty_code_count"])
        authoritative_empty = int(
            raw_counts["authoritative_empty_code_count"]
        )
        failed = int(raw_counts["failed_code_count"])
        exhausted = int(raw_counts["pagination_exhausted_code_count"])
        written = int(raw_counts["written_notice_count"])
        replaced = int(raw_counts["replaced_existing_notice_count"])
        request_coverage = float(payload["request_coverage"])
        row_coverage = float(payload["row_coverage"])
        minimum_request_coverage = float(
            payload["minimum_request_coverage"]
        )
        minimum_row_coverage = float(payload["minimum_row_coverage"])
        started_at = _machine_timestamp(payload.get("started_at"))
        finished_at = _machine_timestamp(payload.get("finished_at"))
        request_window_start = date.fromisoformat(
            str(payload["request_window_start"])
        )
        request_window_end = date.fromisoformat(
            str(payload["request_window_end"])
        )
        scheduler_target_raw = str(
            task.get("_scheduler_target_trade_date") or ""
        ).strip()
        receipt_target_raw = str(
            payload.get("target_trade_date") or ""
        ).strip()
        target_raw = scheduler_target_raw or receipt_target_raw
        target_trade_date = (
            date.fromisoformat(target_raw)
            if target_raw
            else started_at.date() if started_at is not None else None
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    hashes = (
        payload.get("requested_code_set_hash"),
        payload.get("succeeded_code_set_hash"),
        payload.get("nonempty_code_set_hash"),
        payload.get("authoritative_empty_code_set_hash"),
        payload.get("failed_code_set_hash"),
        payload.get("pagination_exhausted_code_set_hash"),
    )
    expected_empty_hash = _notice_code_set_hash([])
    exact_row_coverage = (
        float(nonempty) / float(requested) if requested > 0 else 0.0
    )
    valid = bool(
        int(return_code) == 0
        and payload.get("status") == "PASS"
        and not any(isinstance(value, bool) for value in raw_counts.values())
        and _is_hex(payload.get("receipt_id"), 32)
        and requested > 0
        and succeeded == requested
        and failed == 0
        and exhausted == requested
        and nonempty >= 0
        and authoritative_empty >= 0
        and nonempty + authoritative_empty == requested
        and written >= nonempty
        and replaced >= 0
        and request_coverage == 1.0
        and abs(row_coverage - exact_row_coverage) <= 1e-12
        and minimum_request_coverage == 1.0
        and minimum_row_coverage == 0.0
        and payload.get("succeeded_code_set_hash")
        == payload.get("requested_code_set_hash")
        and payload.get("failed_code_set_hash") == expected_empty_hash
        and payload.get("pagination_exhausted_code_set_hash")
        == payload.get("requested_code_set_hash")
        and all(_is_hex(value, 64) for value in hashes)
        and payload.get("failure_sample") == []
        and payload.get("pagination_evidence")
        == "eastmoney_exact_stock_total_hits_v1"
        and payload.get("empty_result_evidence")
        == "eastmoney_exact_stock_total_hits_v1"
        and payload.get("sync_mode") == "incremental"
        and payload.get("request_scope_evidence")
        == "eastmoney_stock_date_window_v1"
        and request_window_start <= request_window_end
        and started_at is not None
        and finished_at is not None
        and started_at <= finished_at
        and target_trade_date is not None
        and (
            not scheduler_target_raw
            or date.fromisoformat(scheduler_target_raw).isoformat()
            == scheduler_target_raw
        )
        and (
            not receipt_target_raw
            or date.fromisoformat(receipt_target_raw).isoformat()
            == receipt_target_raw
        )
        and (
            not scheduler_target_raw
            or not receipt_target_raw
            or scheduler_target_raw == receipt_target_raw
        )
        and request_window_start
        == target_trade_date - timedelta(days=45)
        and request_window_end
        == target_trade_date + timedelta(days=1)
        and _is_hex(payload.get("source_manifest_sha256"), 64)
        and _is_hex(payload.get("persisted_manifest_sha256"), 64)
        and _is_hex(payload.get("batch_id"), 64)
        and not isinstance(payload.get("association_validated"), bool)
        and payload.get("association_validated") == 1
        and payload.get("data_source") == _NOTICE_PROVIDER_ID
        and payload.get("data_version") == _NOTICE_DATA_VERSION
        and payload.get("quality_status") == _NOTICE_QUALITY_STATUS
        and payload.get("permission_status") == _NOTICE_PERMISSION_STATUS
        and _result_sha256_is_valid(payload)
    )
    return "success" if valid else "failed"


def _notice_history_repair_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _notice_history_repair_payload(output)
    if payload is None or return_code is None:
        return "failed"
    count_keys = (
        "requested_code_count",
        "completed_code_count",
        "remaining_code_count",
        "processed_code_count_this_run",
    )
    try:
        raw_counts = {key: payload[key] for key in count_keys}
        counts = {key: int(value) for key, value in raw_counts.items()}
        ledger_generation = int(payload.get("ledger_generation"))
        inherited_entry_count = int(payload.get("inherited_entry_count"))
        started_at = _machine_timestamp(payload.get("started_at"))
        finished_at = _machine_timestamp(payload.get("finished_at"))
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    requested = counts["requested_code_count"]
    completed = counts["completed_code_count"]
    remaining = counts["remaining_code_count"]
    processed = counts["processed_code_count_this_run"]
    status = str(payload.get("status") or "")
    retryable = payload.get("retryable")
    ledger_status = str(payload.get("ledger_status") or "")
    failed_code = str(payload.get("failed_code") or "")
    failure_type = str(payload.get("failure_type") or "")
    common_valid = bool(
        not any(isinstance(value, bool) for value in raw_counts.values())
        and payload.get("task_type") == _NOTICE_HISTORY_TASK_TYPE
        and payload.get("dataset") == _NOTICE_HISTORY_DATASET
        and payload.get("executor_owner") == "linux_provider"
        and payload.get("provider") == _NOTICE_PROVIDER_ID
        and isinstance(retryable, bool)
        and _is_hex(payload.get("receipt_id"), 32)
        and requested >= 0
        and 0 <= completed <= requested
        and remaining == requested - completed
        and 0 <= processed <= requested
        and ledger_generation >= 0
        and 0 <= inherited_entry_count <= completed
        and _is_hex(payload.get("requested_code_set_hash"), 64)
        and _is_hex(payload.get("ordered_code_sha256"), 64)
        and _is_hex(payload.get("completed_code_set_hash"), 64)
        and payload.get("ledger_schema") == _NOTICE_HISTORY_LEDGER_SCHEMA
        and payload.get("pagination_evidence")
        == _NOTICE_HISTORY_PAGINATION_EVIDENCE
        and payload.get("replacement_scope")
        == _NOTICE_HISTORY_REPLACEMENT_SCOPE
        and payload.get("universe_evidence")
        == _NOTICE_HISTORY_UNIVERSE_EVIDENCE
        and started_at is not None
        and finished_at is not None
        and started_at <= finished_at
        and _result_sha256_is_valid(payload)
    )
    if not common_valid:
        return "failed"
    has_ledger_proof = bool(
        _is_hex(payload.get("batch_id"), 64)
        and _is_hex(payload.get("ledger_sha256"), 64)
        and _is_hex(payload.get("evidence_chain_sha256"), 64)
    )
    if status == "PASS":
        valid = bool(
            int(return_code) == 0
            and retryable is False
            and requested > 0
            and completed == requested
            and remaining == 0
            and ledger_status == "COMPLETE"
            and ledger_generation >= 1
            and (
                ledger_generation == 1
                and payload.get("parent_ledger_sha256") in {None, ""}
                and inherited_entry_count == 0
                or ledger_generation > 1
                and _is_hex(payload.get("parent_ledger_sha256"), 64)
                and inherited_entry_count > 0
            )
            and has_ledger_proof
            and not failed_code
            and not failure_type
            and not str(payload.get("error") or "")
        )
        return "success" if valid else "failed"
    if status == "PROGRESS":
        # Incomplete shards are an expected, retryable non-success.  They must
        # never create a terminal success row or bypass the PASS postvalidator.
        return "failed"
    if status != "DATA_BLOCKED" or int(return_code) != 2:
        return "failed"
    available_failure = bool(
        requested > 0
        and completed < requested
        and remaining > 0
        and ledger_status == "PROGRESS"
        and has_ledger_proof
        and re.fullmatch(r"\d{6}", failed_code) is not None
        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", failure_type)
        is not None
    )
    unavailable_failure = bool(
        ledger_status == "UNAVAILABLE"
        and ledger_generation == 0
        and inherited_entry_count == 0
        and payload.get("parent_ledger_sha256") in {None, ""}
        and not str(payload.get("batch_id") or "")
        and not str(payload.get("ledger_sha256") or "")
        and not str(payload.get("evidence_chain_sha256") or "")
        and not failed_code
        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", failure_type)
        is not None
        and str(payload.get("error") or "").strip()
    )
    if not (available_failure or unavailable_failure):
        return "failed"
    return "failed" if retryable is True else "blocked"


def _sim_trade_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _sim_trade_payload(output)
    if payload is None or return_code is None:
        return "failed"
    expected_mode = (
        "prepare_signals"
        if task_type == "sim_trade_signal_prepare"
        else "tick"
    )
    try:
        raw_counts = {
            key: payload[key]
            for key in (
                "recommendation_count",
                "total_recommendations",
                "recommendation_code_count",
                "strategy_count",
                "signal_count",
                "allowed_count",
                "rejected_count",
                "signal_identity_count",
                "buy_order_count",
                "buy_fill_count",
                "sell_fill_count",
            )
        }
        trade_date_raw = str(payload["trade_date"])
        parsed_trade_date = date.fromisoformat(trade_date_raw)
        started_at = _machine_timestamp(payload.get("started_at"))
        finished_at = _machine_timestamp(payload.get("finished_at"))
        counts = {
            key: int(value) for key, value in raw_counts.items()
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    if (
        parsed_trade_date.isoformat() != trade_date_raw
        or payload.get("task_mode") != expected_mode
        or not _is_hex(payload.get("receipt_id"), 32)
        or not _result_sha256_is_valid(payload)
        or started_at is None
        or finished_at is None
        or started_at > finished_at
        or any(isinstance(value, bool) for value in raw_counts.values())
        or any(value < 0 for value in counts.values())
        or not _is_hex(payload.get("recommendation_code_set_hash"), 64)
        or not _is_hex(payload.get("signal_identity_hash"), 64)
    ):
        return "failed"

    status = str(payload.get("status") or "").upper()
    if status == "SKIPPED":
        return "success" if int(return_code) == 0 else "failed"
    if status != "PASS" or int(return_code) != 0:
        # Signal preparation failures must remain retryable within the same
        # morning instead of being persisted as a terminal capability block.
        return "failed"
    if expected_mode == "tick":
        return "success"

    signal_date_raw = str(payload.get("signal_date") or "")
    try:
        parsed_signal_date = date.fromisoformat(signal_date_raw)
    except ValueError:
        return "failed"
    expected_decisions = (
        counts["total_recommendations"] * counts["strategy_count"]
    )
    valid_prepare = bool(
        parsed_signal_date.isoformat() == signal_date_raw
        and counts["recommendation_count"]
        == counts["total_recommendations"]
        == counts["recommendation_code_count"]
        and counts["recommendation_count"] > 0
        and counts["strategy_count"] > 0
        and counts["allowed_count"] + counts["rejected_count"]
        == expected_decisions
        and counts["signal_count"]
        == counts["signal_identity_count"]
        == counts["allowed_count"]
    )
    return "success" if valid_prepare else "failed"


_EASTMONEY_CONCEPT_TASK_DATASETS = {
    "eastmoney_concept_current": "current",
    "eastmoney_concept_kline": "kline",
    "eastmoney_concept_minute": "minute",
}


def _canonical_mapping_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _eastmoney_concept_market_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _eastmoney_concept_market_payload(output)
    dataset = _EASTMONEY_CONCEPT_TASK_DATASETS.get(task_type)
    if payload is None or dataset is None or return_code is None:
        return "failed"
    try:
        target = date.fromisoformat(str(payload["target_trade_date"]))
        range_start = date.fromisoformat(str(payload["range_start"]))
        range_end = date.fromisoformat(str(payload["range_end"]))
        directory_count = int(payload["directory_count"])
        open_date_count = int(payload["open_date_count"])
        started_at = _shanghai_machine_timestamp(payload.get("started_at"))
        finished_at = _shanghai_machine_timestamp(payload.get("finished_at"))
        directory = payload["directory"]
        first_source_time = _shanghai_machine_timestamp(
            directory["first_source_time"]
        )
        last_source_time = _shanghai_machine_timestamp(
            directory["last_source_time"]
        )
        dataset_result = payload["dataset_results"][dataset]
        database = payload["db_metrics"][dataset]
        row_count = int(dataset_result["row_count"])
        code_count = int(dataset_result["code_count"])
        date_count = int(dataset_result["date_count"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    expected_rows = directory_count * (240 if dataset == "minute" else 1)
    expected_table = {
        "current": "sm_concept_east_current",
        "kline": "sm_concept_east_kline",
        "minute": "sm_concept_east_minute",
    }[dataset]
    directory_unsigned = dict(directory) if isinstance(directory, Mapping) else {}
    directory_manifest = directory_unsigned.pop("manifest_sha256", None)
    valid = bool(
        int(return_code) == 0
        and payload.get("status") == "PASS"
        and payload.get("provider") == "eastmoney_public_market"
        and payload.get("datasets") == [dataset]
        and payload.get("requested_trade_date")
        in (None, target.isoformat())
        and payload.get("requested_start_date") is None
        and payload.get("requested_end_date") is None
        and payload.get("published") is True
        and _is_hex(payload.get("receipt_id"), 32)
        and _result_sha256_is_valid(payload)
        and started_at is not None
        and finished_at is not None
        and started_at <= finished_at
        and target == range_start == range_end
        and directory_count >= 100
        and open_date_count == 1
        and payload.get("open_dates_sha256")
        == _canonical_mapping_hash([target.isoformat()])
        and isinstance(directory, Mapping)
        and directory.get("schema")
        == "probiga.eastmoney-concept-directory.v1"
        and directory.get("provider") == "eastmoney_public_market"
        and directory.get("pagination_complete") is True
        and int(directory.get("reported_count") or 0) == directory_count
        and int(directory.get("observed_count") or 0) == directory_count
        and int(directory.get("pages_expected") or 0)
        == int(directory.get("pages_fetched") or -1)
        and directory.get("source_dates") == [target.isoformat()]
        and first_source_time is not None
        and last_source_time is not None
        and first_source_time.date() == target
        and last_source_time.date() == target
        and first_source_time.time() >= datetime.strptime(
            "15:00", "%H:%M"
        ).time()
        and first_source_time <= last_source_time
        and last_source_time <= finished_at + timedelta(minutes=5)
        and _is_hex(directory.get("code_set_sha256"), 64)
        and directory_manifest == _canonical_mapping_hash(directory_unsigned)
        and dataset_result.get("dataset") == dataset
        and dataset_result.get("table") == expected_table
        and dataset_result.get("provider") == "eastmoney_public_market"
        and code_count == directory_count
        and date_count == 1
        and row_count == expected_rows
        and dataset_result.get("first_date") == target.isoformat()
        and dataset_result.get("last_date") == target.isoformat()
        and dataset_result.get("code_set_sha256")
        == directory.get("code_set_sha256")
        and _is_hex(dataset_result.get("content_sha256"), 64)
        and int(database.get("row_count") or -1) == row_count
        and int(database.get("code_count") or -1) == code_count
        and int(database.get("date_count") or -1) == date_count
    )
    return "success" if valid else "failed"


def _sector_heat_east_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _sector_heat_east_payload(output)
    if payload is None or return_code is None:
        return "failed"
    try:
        requested = date.fromisoformat(str(payload["requested_date"]))
        data_date = date.fromisoformat(str(payload["data_date"]))
        started_at = _machine_timestamp(payload.get("started_at"))
        finished_at = _machine_timestamp(payload.get("finished_at"))
        evidence = payload["evidence"]
        counts = {
            key: int(evidence[key])
            for key in (
                "raw_count",
                "expected_l1_count",
                "expected_l2_count",
                "l1_count",
                "l2_count",
                "row_count",
            )
        }
        coverage = float(evidence["coverage"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    valid = bool(
        int(return_code) == 0
        and payload.get("status") == "PASS"
        and payload.get("source") == "eastmoney.push2.industry"
        and payload.get("published") is True
        and requested == data_date
        and started_at is not None
        and finished_at is not None
        and started_at <= finished_at
        and _is_hex(payload.get("receipt_id"), 64)
        and _receipt_id_is_valid(payload)
        and counts["raw_count"] >= 128
        and counts["expected_l1_count"] == counts["l1_count"] == 31
        and counts["expected_l2_count"] == counts["l2_count"] == 128
        and counts["row_count"] == 159
        and coverage == 1.0
        and _is_hex(evidence.get("row_hash"), 64)
    )
    return "success" if valid else "failed"


def _ths_hot_output_status(
    task_type: str,
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _ths_hot_payload(output)
    if payload is None or return_code is None:
        return "failed"
    try:
        from server.common.ths_hot_contract import basic_receipt_disposition

        return basic_receipt_disposition(
            payload,
            task_type=task_type,
            return_code=int(return_code),
        )
    except (ImportError, TypeError, ValueError, OverflowError):
        return "failed"


def _news_sync_output_status(
    output: str | None,
    *,
    return_code: int | None,
) -> str:
    payload = _news_sync_payload(output)
    if payload is None or return_code is None:
        return "failed"
    expected_sources = ["cls", "eastmoney", "sina"]
    try:
        started_at = _machine_timestamp(payload.get("batch_started_at"))
        finished_at = _machine_timestamp(payload.get("batch_finished_at"))
        pages = int(payload["requested_pages"])
        evidence = payload["evidence"]
        persisted_count = int(evidence["persisted_count"])
        latest_publish = datetime.fromisoformat(
            str(evidence["latest_publish_time"])
        ).replace(tzinfo=None)
        source_results = payload["source_results"]
    except (KeyError, TypeError, ValueError, OverflowError):
        return "failed"
    if not isinstance(source_results, Mapping):
        return "failed"
    successful: list[str] = []
    nonempty: list[str] = []
    empty: list[str] = []
    failed: list[str] = []
    fetched_total = 0
    for source in expected_sources:
        result = source_results.get(source)
        if not isinstance(result, Mapping):
            return "failed"
        try:
            requested_pages = int(result["requested_pages"])
            fetched_count = int(result["fetched_count"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return "failed"
        if requested_pages != (pages if source == "cls" else max(1, pages // 2)):
            return "failed"
        if fetched_count < 0:
            return "failed"
        status = str(result.get("status") or "")
        outcome = str(result.get("outcome") or "")
        if status == "SUCCESS":
            successful.append(source)
            if outcome == "NONEMPTY" and fetched_count > 0:
                nonempty.append(source)
                fetched_total += fetched_count
            elif outcome == "EMPTY" and fetched_count == 0:
                empty.append(source)
            else:
                return "failed"
        elif status == "FAILED" and fetched_count == 0:
            failed.append(source)
        else:
            return "failed"
    valid = bool(
        int(return_code) == 0
        and payload.get("status") == "PASS"
        and payload.get("attempted_sources") == expected_sources
        and payload.get("successful_sources") == successful
        and payload.get("nonempty_sources") == nonempty
        and payload.get("empty_sources") == empty
        and payload.get("failed_sources") == failed
        and successful
        and nonempty
        and persisted_count == fetched_total > 0
        and pages == 2
        and payload.get("delivery_attempted") is False
        and started_at is not None
        and finished_at is not None
        and started_at <= finished_at
        and latest_publish <= finished_at + timedelta(minutes=5)
        and _is_hex(evidence.get("row_hash"), 64)
        and _is_hex(payload.get("receipt_id"), 64)
        and _receipt_id_is_valid(payload)
    )
    return "success" if valid else "failed"


def scheduler_output_status(
    task: Mapping[str, Any],
    output: str | None,
    *,
    return_code: int | None = None,
) -> str | None:
    """Map a task's machine-readable result to scheduler semantics.

    Level-1 validation returning BLOCK means the validator ran correctly but
    the capability is unavailable.  Persisting that as ``success`` hides a
    production prerequisite; treating it as ``failed`` would cause needless
    same-day retries.  ``blocked`` accurately represents both conditions.
    """
    task_type = str(task.get("task_type") or "").strip()
    if task_type == _QMT_MEMBERSHIP_TASK_TYPE:
        release_verification = (
            str(task.get("_trigger_source") or "").strip()
            == "release_catchup"
        )
        payload = (
            _qmt_membership_verification_payload(output)
            if release_verification
            else _qmt_membership_publication_payload(output)
        )
        if payload is None:
            return "failed"
        if return_code is None:
            return "failed"
        try:
            from tools.sync_bigqmt_reference import (
                validate_membership_publication_receipt,
                validate_membership_verification_receipt,
            )

            disposition = (
                validate_membership_verification_receipt(
                    dict(payload),
                    int(return_code),
                )
                if release_verification
                else validate_membership_publication_receipt(
                    dict(payload),
                    int(return_code),
                )
            )
        except (ImportError, TypeError, ValueError, OverflowError):
            return "failed"
        if disposition == "blocked":
            return "blocked"
        return "success" if disposition == "complete" else "failed"
    if task_type == _CAPITAL_FLOW_BATCH_TASK_TYPE:
        payload = _capital_flow_batch_payload(output)
        if payload is None or return_code is None:
            return "failed"
        if payload.get("schema") == _DIRECT_CAPITAL_FLOW_RESULT_SCHEMA:
            build_sha = str(payload.get("build_sha") or "").strip().lower()
            expected_build = str(
                task.get("_scheduler_expected_build_sha") or ""
            ).strip().lower()
            return (
                "success"
                if int(return_code) == 0
                and _is_hex(build_sha, 40)
                and build_sha != "0" * 40
                and (not expected_build or build_sha == expected_build)
                and _direct_capital_flow_payload_is_valid(payload)
                else "failed"
            )
        try:
            target = date.fromisoformat(str(payload.get("trade_date") or ""))
            row_count = int(payload.get("row_count") or 0)
        except (TypeError, ValueError, OverflowError):
            return "failed"
        build_sha = str(payload.get("build_sha") or "").strip().lower()
        expected_build = str(
            task.get("_scheduler_expected_build_sha") or ""
        ).strip().lower()
        return (
            "success"
            if int(return_code) == 0
            and payload.get("status") == "PASS"
            and payload.get("task_type") == _CAPITAL_FLOW_BATCH_TASK_TYPE
            and payload.get("dataset") == _CAPITAL_FLOW_BATCH_DATASET
            and target.isoformat() == payload.get("trade_date")
            and payload.get("source_trade_date") == target.isoformat()
            and payload.get("source_timestamp_required") is True
            and row_count > 0
            and _is_hex(build_sha, 40)
            and build_sha != "0" * 40
            and (not expected_build or build_sha == expected_build)
            and _capital_flow_execution_payload(payload) is not None
            and _receipt_id_is_valid(payload)
            else "failed"
        )
    if task_type in {
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
    }:
        return _qmt_stock_edge_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type in _EASTMONEY_ALIST_TASK_DATASETS:
        return _eastmoney_alist_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type == _QMT_MINUTE_FLOW_TASK_TYPE:
        return _qmt_minute_flow_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type == _QMT_CANONICAL_HISTORY_REPAIR_TASK_TYPE:
        return _qmt_canonical_history_repair_output_status(
            output,
            return_code=return_code,
        )
    if task_type == _LINUX_RECENT_DATA_GAP_REPAIR_TASK_TYPE:
        return _linux_recent_data_gap_repair_output_status(
            output,
            return_code=return_code,
        )
    if task_type == "etf_forward_daily":
        return _etf_forward_output_status(output, return_code=return_code)
    if task_type == "stock_dividend_baidu":
        return _dividend_baidu_output_status(output, return_code=return_code)
    if task_type == "notice_eastmoney":
        return _notice_eastmoney_output_status(
            task,
            output,
            return_code=return_code,
        )
    if task_type == _NOTICE_HISTORY_TASK_TYPE:
        return _notice_history_repair_output_status(
            output,
            return_code=return_code,
        )
    if task_type in {"sim_trade", "sim_trade_signal_prepare"}:
        return _sim_trade_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type in _EASTMONEY_CONCEPT_TASK_DATASETS:
        return _eastmoney_concept_market_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type in HOT_RANK_SOURCE_TASK_TYPES:
        payload = parse_hot_rank_receipt(output)
        if payload is None or return_code is None:
            return "failed"
        if task_type == HOT_RANK_SINA_TASK_TYPE and (
            payload.get("status") != "DATA_BLOCKED"
            or payload.get("reason") != SINA_ATTENTION_DATA_BLOCK_REASON
        ):
            # Do not let a locally self-signed PASS (or an unrelated block)
            # turn the provider's unverifiable code ordering into readiness.
            return "failed"
        return hot_rank_receipt_disposition(
            payload,
            task_type=task_type,
            return_code=int(return_code),
        )
    if task_type in {"hot_rank_ths", "hot_concept"}:
        return _ths_hot_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type == "sector_heat_east":
        return _sector_heat_east_output_status(
            output,
            return_code=return_code,
        )
    if task_type == "news_sync":
        return _news_sync_output_status(
            output,
            return_code=return_code,
        )
    if task_type in TRADING_V3_DECISION_TASK_TYPES:
        return _trading_v3_decision_output_status(
            output,
            return_code=return_code,
        )
    if task_type == "stock_finance":
        candidates: list[Mapping[str, Any]] = []
        seal_candidates: list[Mapping[str, Any]] = []
        for line in str(output or "").splitlines():
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                payload = json.loads(candidate)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if (
                isinstance(payload, Mapping)
                and payload.get("schema") == "probiga.finance-sync-result.v1"
            ):
                candidates.append(payload)
            if (
                isinstance(payload, Mapping)
                and payload.get("schema")
                == "probiga.finance-atomic-batch-result.v1"
            ):
                seal_candidates.append(payload)
        if len(seal_candidates) == 1 and not candidates and return_code is not None:
            seal = seal_candidates[0]
            try:
                eligible = int(seal.get("eligible_code_count"))
                unavailable = int(seal.get("expected_unavailable_count") or 0)
                catalog_members = int(seal.get("catalog_member_count"))
            except (TypeError, ValueError, OverflowError):
                return "failed"
            return (
                "success"
                if int(return_code) == 0
                and seal.get("status") == "PASS"
                and seal.get("seal_schema")
                == "probiga.pit-finance-atomic-batch.v1"
                and eligible >= 1000
                and catalog_members >= eligible
                and 0 <= unavailable <= eligible
                and all(
                    _is_hex(seal.get(field), 64)
                    for field in (
                        "eligible_code_set_hash",
                        "coverage_root_sha256",
                        "batch_root_sha256",
                        "seal_coverage_id",
                    )
                )
                else "failed"
            )
        if len(candidates) != 1 or return_code is None:
            return "failed"
        payload = candidates[0]
        try:
            requested = int(payload.get("requested_code_count"))
            nonempty = int(payload.get("nonempty_code_count"))
            unavailable = int(
                payload.get("expected_unavailable_code_count") or 0
            )
            resolved = int(
                payload.get("resolved_code_count")
                if payload.get("resolved_code_count") is not None
                else nonempty + unavailable
            )
            written = int(payload.get("written_report_count"))
            failures = int(payload.get("failure_count"))
            coverage = float(payload.get("nonempty_code_coverage"))
            resolution_coverage = float(
                payload.get("resolution_coverage")
                if payload.get("resolution_coverage") is not None
                else coverage
            )
            applicable = int(payload.get("report_period_applicable_code_count"))
            exempt = int(payload.get("new_listing_period_exempt_code_count"))
            unavailable_sample = payload.get(
                "expected_unavailable_code_sample", {}
            )
            minimum_report = datetime.strptime(
                str(payload.get("minimum_report_date")), "%Y-%m-%d"
            ).date()
            disclosure_deadline = datetime.strptime(
                str(payload.get("minimum_report_disclosure_deadline")),
                "%Y-%m-%d",
            ).date()
            oldest_applicable_raw = payload.get(
                "oldest_latest_applicable_report_date"
            )
            oldest_applicable = (
                datetime.strptime(str(oldest_applicable_raw), "%Y-%m-%d").date()
                if oldest_applicable_raw is not None
                else None
            )
        except (TypeError, ValueError, OverflowError):
            return "failed"
        return (
            "success"
            if int(return_code) == 0
            and payload.get("status") == "PASS"
            and requested > 0
            and nonempty + unavailable == requested
            and resolved == requested
            and resolution_coverage == 1.0
            and written >= nonempty
            and failures == 0
            and applicable + exempt + unavailable == requested
            and isinstance(unavailable_sample, Mapping)
            and len(unavailable_sample) == unavailable
            and set(map(str, unavailable_sample))
            <= _FINANCE_EXPECTED_UNAVAILABLE_CODES
            and disclosure_deadline >= minimum_report
            and (
                applicable == 0
                or oldest_applicable is not None
                and oldest_applicable >= minimum_report
            )
            else "failed"
        )
    if task_type == "qmt_announcement_pit":
        candidates: list[Mapping[str, Any]] = []
        for line in str(output or "").splitlines():
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                payload = json.loads(candidate)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if (
                isinstance(payload, Mapping)
                and payload.get("schema")
                == "probiga.qmt-announcement-task-result.v1"
            ):
                candidates.append(payload)
        if len(candidates) != 1 or return_code is None:
            return "failed"
        try:
            if (
                str(task.get("_trigger_source") or "").strip()
                == "release_catchup"
            ):
                from tools.sync_qmt_announcement_pit import (
                    validate_existing_task_result,
                )

                release_target = _release_target_date_from_task(task)
                if release_target is None:
                    return "failed"
                disposition = validate_existing_task_result(
                    dict(candidates[0]),
                    int(return_code),
                    expected_trade_date=release_target.isoformat(),
                    expected_scheduler_run_uid=str(
                        task.get("_scheduler_history_run_uid") or ""
                    ),
                    expected_build_sha=str(
                        task.get("_scheduler_expected_build_sha") or ""
                    ),
                )
            else:
                from server.common.qmt_announcement_pit import (
                    validate_task_result,
                )

                disposition = validate_task_result(
                    candidates[0], int(return_code)
                )
        except (
            ImportError,
            TypeError,
            ValueError,
            OverflowError,
            RuntimeError,
        ):
            return "failed"
        return "success" if disposition == "complete" else "blocked"
    if task_type in {"qmt_index_current", "qmt_index_kline", "qmt_index_minute"}:
        return _qmt_index_edge_output_status(
            task_type,
            output,
            return_code=return_code,
        )
    if task_type == "eastmoney_concept_flow_snapshot":
        payload = _eastmoney_concept_flow_payload(output)
        if payload is None or return_code is None:
            return "failed"
        try:
            from tools.fetch_concept_flow_datacenter import validate_task_result

            disposition = validate_task_result(dict(payload), int(return_code))
        except (ImportError, TypeError, ValueError, OverflowError):
            return "failed"
        return "success" if disposition == "complete" else "blocked"
    if task_type == "strategy_governance_daily":
        # Use the exact same completed/not-due/blocked contract as the deploy
        # validator.  A zero process exit alone is not evidence of a safe
        # governance close: weights, dates and every nested real-order flag
        # still have to pass the canonical validator.
        nonempty_lines = [
            line.strip()
            for line in str(output or "").splitlines()
            if line.strip()
        ]
        if len(nonempty_lines) != 1:
            return "failed"
        try:
            payload = json.loads(nonempty_lines[0])
        except (TypeError, ValueError, json.JSONDecodeError):
            return "failed"
        if return_code is None:
            return "failed"
        try:
            from tools.run_strategy_governance_daily import validate_cli_result

            disposition = validate_cli_result(payload, int(return_code))
        except (ImportError, TypeError, ValueError, OverflowError):
            return "failed"
        if disposition in {"completed", "not_due"}:
            return "success"
        if disposition == "not_ready":
            return "blocked"
        return "failed"
    if task_type == "final_pool_wecom_delivery":
        nonempty_lines = [
            line.strip()
            for line in str(output or "").splitlines()
            if line.strip()
        ]
        if len(nonempty_lines) != 1 or return_code is None:
            return "failed"
        try:
            payload = json.loads(nonempty_lines[0])
            from tools.send_final_pool_wecom import validate_cli_result

            disposition = validate_cli_result(payload, int(return_code))
        except (
            ImportError,
            TypeError,
            ValueError,
            OverflowError,
            json.JSONDecodeError,
        ):
            return "failed"
        if disposition == "completed":
            return "success"
        if disposition == "not_ready":
            return "blocked"
        return "failed"
    if task_type not in {
        "trading_v2_level1_validation",
        "concept_constituent_east",
    }:
        return None
    for line in str(output or "").splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            payload = json.loads(candidate)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        capability_status = str(payload.get("status") or "").upper()
        if capability_status == "PASS":
            return "success"
        if task_type == "concept_constituent_east" and capability_status == "SUCCESS":
            return "success"
        if capability_status == "BLOCK":
            return "blocked"
    return None


TASK_OUTPUT_REQUIREMENTS: dict[str, tuple[TableRequirement, ...]] = {
    "all_code": (
        TableRequirement("si_all_code", min_rows=1000, distinct_col="stock_code", min_distinct=1000, require_fresh=False),
    ),
    "all_index_code": (
        TableRequirement("si_all_index_code", min_rows=50, require_fresh=False),
    ),
    "stock_finance": (
        TableRequirement(
            "st_pit_source_coverage",
            min_rows=1,
            where_sql=(
                "fact_kind = 'finance' AND source IN "
                "('adata.finance.core_index', "
                "'eastmoney.finance.mainfinadata.direct') "
                "AND coverage_status = 'COMPLETE' AND result_count > 0"
            ),
            freshness_col="known_at",
        ),
    ),
    "index_constituent": (
        TableRequirement("si_index_constituent", min_rows=5000, require_fresh=False),
    ),
    "concept_code_east": (
        TableRequirement("si_concept_code_east", min_rows=100, require_fresh=False),
    ),
    "concept_constituent_east": (
        TableRequirement("si_concept_constituent_east", min_rows=1000, require_fresh=False),
    ),
    "stock_relations_qmt": (
        TableRequirement("si_stock_plate_east", min_rows=1000, require_fresh=False),
    ),
    "sector_heat_east": (
        TableRequirement(
            "st_hot_concept_ths_daily",
            min_rows=50,
            date_col="snapshot_date",
            target="output_date",
            distinct_col="concept_code",
            min_distinct=50,
            where_sql="plate_type IN (3, 4)",
        ),
    ),
    "hot_concept": (
        TableRequirement(
            "st_hot_concept_ths_daily",
            min_rows=20,
            date_col="snapshot_date",
            where_sql="plate_type IN (1, 2)",
        ),
    ),
    "hot_rank_ths": (
        TableRequirement("st_hot_rank_ths", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_pop_east": (
        TableRequirement("st_hot_pop_rank_east", min_rows=50, date_col="snapshot_date"),
    ),
    "fetch_hot_rank_xq": (
        TableRequirement("st_hot_rank_xq", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_rank_sina": (
        TableRequirement("st_hot_rank_sina", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_fused": (
        TableRequirement("st_hot_rank_fused", min_rows=20, date_col="snapshot_date"),
    ),
    "hot_fused_3": (
        TableRequirement(
            "st_hot_rank_multi_day",
            min_rows=20,
            date_col="stat_date",
            target="output_date",
            where_sql="stat_days = 3",
        ),
    ),
    "hot_fused_5": (
        TableRequirement(
            "st_hot_rank_multi_day",
            min_rows=20,
            date_col="stat_date",
            target="output_date",
            where_sql="stat_days = 5",
        ),
    ),
    "alist_daily": (
        TableRequirement("st_a_list_daily", min_rows=1, date_col="trade_date"),
    ),
    "alist_info": (
        TableRequirement("st_a_list_info", min_rows=1, date_col="trade_date"),
    ),
    "sync_concept_ths": (
        TableRequirement("si_concept_code_ths", min_rows=100, require_fresh=False),
        TableRequirement("si_concept_constituent_ths", min_rows=50000, require_fresh=False),
    ),
    "capital_flow": (
        TableRequirement(
            "sm_stock_capital_flow_daily",
            min_rows=5000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "capital_flow_batch_fast": (
        TableRequirement(
            "sm_stock_capital_flow_daily",
            min_rows=5000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "stock_current": (
        TableRequirement(
            "sm_stock_current",
            min_rows=3000,
            date_col="snapshot_at",
            distinct_col="stock_code",
            # Suspended/delisted symbols are not present in the live quote
            # universe.  Production currently has 5,280 valid A-share quotes;
            # 5,000 still enforces broad-market coverage without a false fail.
            min_distinct=5000,
        ),
    ),
    "stock_kline": (
        TableRequirement(
            "sm_stock_kline",
            min_rows=3000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=3000,
        ),
    ),
    "stock_minute": (
        TableRequirement(
            "sm_stock_minute",
            min_rows=100000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="stock_code",
            min_distinct=3000,
        ),
    ),
    "stock_minute_flow": (
        TableRequirement(
            "sm_stock_capital_flow_min",
            min_rows=100000,
            date_col="trade_time",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="stock_code",
            min_distinct=4500,
        ),
    ),
    "concept_east_current": (
        TableRequirement(
            "sm_concept_east_current",
            min_rows=100,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_ths_current": (
        TableRequirement(
            "sm_concept_ths_current",
            min_rows=100,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_ths_minute": (
        TableRequirement(
            "sm_concept_ths_minute",
            min_rows=1000,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_flow": (
        TableRequirement(
            "sm_concept_capital_flow_east",
            min_rows=100,
            date_col="snapshot_at",
            ready_time="19:30",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "index_current": (
        TableRequirement(
            "sm_index_current",
            min_rows=50,
            date_col="trade_date",
            target="latest_trade_date",
            distinct_col="index_code",
            min_distinct=50,
        ),
    ),
    "index_kline": (
        TableRequirement(
            "sm_index_kline",
            min_rows=50,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="index_code",
            min_distinct=50,
        ),
    ),
    "index_minute": (
        TableRequirement(
            "sm_index_minute",
            min_rows=1000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="index_code",
            min_distinct=20,
        ),
    ),
    "intraday_realtime": (
        TableRequirement(
            "sm_stock_current",
            min_rows=3000,
            date_col="snapshot_at",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "intraday_minute_kline": (
        TableRequirement(
            "sm_stock_minute",
            # Intraday runs begin shortly after the open.  A fixed full-day
            # row threshold falsely fails early runs even when nearly every
            # stock has already produced bars; coverage is enforced below.
            min_rows=5000,
            date_col="trade_date",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "intraday_minute_flow": (
        TableRequirement(
            "sm_stock_capital_flow_min",
            # The first 09:40 run has only a few bars per stock. Distinct
            # stock coverage is the useful early-session completeness gate.
            min_rows=5000,
            date_col="trade_time",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "market_overview_daily": (
        TableRequirement(
            "sm_market_overview_daily",
            min_rows=1,
            date_col="trade_date",
            target="output_date",
            freshness_col="updated_at",
        ),
    ),
    "strategy_governance_daily": (
        TableRequirement(
            "st_strategy_governance_run",
            min_rows=1,
            date_col="trade_date",
            target="latest_kline_date",
            where_sql="status = 'COMPLETED'",
            freshness_col="finished_at",
        ),
    ),
    "stock_snapshot_daily": (
        TableRequirement(
            "sm_stock_snapshot",
            min_rows=1000,
            date_col="trade_date",
            target="output_date",
            distinct_col="stock_code",
            min_distinct=1000,
        ),
    ),
    "analysis_fast": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="latest_kline_date",
            distinct_col="stock_code",
            min_distinct=1000,
            where_sql="recommend_status IS NOT NULL AND TRIM(recommend_status) <> ''",
            freshness_col="updated_at",
        ),
    ),
    "analysis_morning_strict": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="previous_trade_date",
            distinct_col="stock_code",
            min_distinct=1000,
            where_sql="recommend_status IS NOT NULL AND TRIM(recommend_status) <> ''",
            freshness_col="updated_at",
        ),
    ),
    "analysis_premarket_external": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="previous_trade_date",
            distinct_col="stock_code",
            min_distinct=1000,
            where_sql="recommend_status IS NOT NULL AND TRIM(recommend_status) <> ''",
            freshness_col="updated_at",
        ),
        TableRequirement(
            "st_premarket_theme_forecast_run",
            min_rows=1,
            date_col="session_date",
            target="run_date",
            where_sql="stage = 'PREMARKET_0908' AND status = 'COMPLETED' AND delivery_status = 'SUCCESS'",
            freshness_col="updated_at",
        ),
    ),
}


def _analysis_web_manual_profile_is_exact(task: Mapping[str, Any]) -> bool:
    """Recognize the one fixed no-theme Web recommendation invocation."""
    if str(task.get("task_type") or "").strip() != "analysis_premarket_external":
        return False
    if str(task.get("_trigger_source") or "").strip() != "manual":
        return False
    if str(task.get("_validation_profile") or "").strip() != "analysis_web_manual_v1":
        return False
    try:
        tokens = shlex.split(str(task.get("script_args") or ""), posix=True)
    except ValueError:
        return False
    value_flags = {
        "--date",
        "--top-n",
        "--min-score",
        "--execution-time",
        "--min-kline-coverage",
        "--run-uid",
    }
    bool_flags = {
        "--strict-prev-trade-day",
        "--json",
    }
    values: dict[str, str] = {}
    booleans: set[str] = set()
    index = 0
    while index < len(tokens):
        flag = tokens[index]
        if flag in value_flags:
            if flag in values or index + 1 >= len(tokens):
                return False
            value = tokens[index + 1]
            if value.startswith("--"):
                return False
            values[flag] = value
            index += 2
            continue
        if flag in bool_flags:
            if flag in booleans:
                return False
            booleans.add(flag)
            index += 1
            continue
        return False
    if set(values) != value_flags:
        return False
    if not {"--strict-prev-trade-day", "--json"}.issubset(booleans):
        return False
    if not re.fullmatch(r"[0-9a-f]{32}", values["--run-uid"]):
        return False
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", values["--date"]):
        return False
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        values["--execution-time"],
    ):
        return False
    try:
        top_n = int(values["--top-n"])
        min_score = float(values["--min-score"])
        min_coverage = float(values["--min-kline-coverage"])
    except (TypeError, ValueError):
        return False
    return (
        20 <= top_n <= 200
        and 0.0 <= min_score <= 100.0
        and 0.0 <= min_coverage <= 1.0
    )


def _validate_analysis_strategy_pool(
    engine: Engine,
    *,
    target_date: date,
    started_at: datetime,
    now: datetime,
    scheduler_run_uid: str,
    expected_build_sha: str,
    output: str | None,
) -> tuple[bool, str]:
    """Verify the latest exact-build canonical producer of the daily pool."""

    target = target_date.isoformat()
    run_uid = str(scheduler_run_uid or "").strip().lower()
    build_sha = str(expected_build_sha or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{32}", run_uid) is None:
        return False, "analysis strategy pool scheduler run identity is unavailable"
    if (
        re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or build_sha == "0" * 40
    ):
        return False, "analysis strategy pool build identity is unavailable"
    current_receipt = _single_nested_machine_payload(
        output,
        schema=ANALYSIS_POOL_RECEIPT_SCHEMA,
    )
    if (
        current_receipt is None
        or not publication_receipt_is_valid(current_receipt)
    ):
        return False, "analysis strategy pool exact publication receipt is missing"

    with engine.connect() as connection:
        history_rows = connection.execute(text("""
            SELECT id, run_uid, scheduler_job_id, trade_date, status, build_sha,
                   total, passed, started_at, finished_at,
                   publisher_task_type, canonical_pool_sha256, published_at,
                   executable_count, membership_snapshot_date,
                   membership_snapshot_source, membership_proof_sha256
            FROM st_recommended_run_history
            WHERE run_uid=:run_uid
            LIMIT 2
        """), {"run_uid": run_uid}).mappings().all()
        if len(history_rows) != 1:
            return (
                False,
                "analysis strategy pool recommendation history is unavailable "
                f"or ambiguous: run_uid={run_uid}",
            )
        history = dict(history_rows[0])
        history_started_at = _coerce_datetime(history.get("started_at"))
        history_finished_at = _coerce_datetime(history.get("finished_at"))
        history_published_at = _coerce_datetime(history.get("published_at"))
        try:
            history_total = int(history.get("total"))
            history_passed = int(history.get("passed"))
            history_executable = int(history.get("executable_count"))
        except (TypeError, ValueError):
            history_total = history_passed = history_executable = -1
        current_task_type = str(
            history.get("publisher_task_type") or ""
        ).strip()
        history_membership = {
            "snapshot_date": str(
                history.get("membership_snapshot_date") or ""
            )[:10],
            "source": str(
                history.get("membership_snapshot_source") or ""
            ).strip(),
            "proof_sha256": str(
                history.get("membership_proof_sha256") or ""
            ).strip().lower(),
        }
        if (
            str(history.get("run_uid") or "").strip().lower() != run_uid
            or str(history.get("scheduler_job_id") or "").strip().lower()
            != run_uid
            or str(history.get("trade_date") or "")[:10] != target
            or str(history.get("status") or "").strip().lower() != "done"
            or str(history.get("build_sha") or "").strip().lower() != build_sha
            or current_task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(history.get("canonical_pool_sha256") or "").lower(),
            ) is None
            or history_total <= 0
            or history_passed <= 0
            or history_passed > history_total
            or history_executable < 0
            or history_executable > history_passed
            or (
                history_executable == 0
                and not research_only_publication_is_safe(current_receipt)
            )
            or history_started_at is None
            or history_finished_at is None
            or history_published_at is None
            or history_started_at < started_at - timedelta(minutes=5)
            or history_started_at > now + timedelta(minutes=5)
            or history_finished_at < history_started_at
            or history_published_at < history_started_at
            or history_published_at > history_finished_at
            or history_finished_at > now + timedelta(minutes=5)
            or history_membership["snapshot_date"] != target
            or history_membership["source"] != "gj_big_qmt_inner"
            or re.fullmatch(
                r"[0-9a-f]{64}", history_membership["proof_sha256"]
            ) is None
        ):
            return (
                False,
                "analysis strategy pool recommendation history differs from "
                f"the current scheduler run: run_uid={run_uid} date={target} "
                f"status={history.get('status')} total={history_total} "
                f"passed={history_passed} executable={history_executable}",
            )

        receipt_published_at = _coerce_datetime(
            current_receipt.get("published_at")
        )
        if (
            str(current_receipt.get("run_uid") or "").lower() != run_uid
            or str(current_receipt.get("build_sha") or "").lower() != build_sha
            or str(current_receipt.get("publisher_task_type") or "")
            != current_task_type
            or str(current_receipt.get("trade_date") or "") != target
            or str(current_receipt.get("canonical_pool_sha256") or "").lower()
            != str(history.get("canonical_pool_sha256") or "").lower()
            or int(current_receipt.get("analysis_count") or -1)
            != history_total
            or int(current_receipt.get("recommendation_count") or -1)
            != history_passed
            or int(
                current_receipt.get("executable_count")
                if current_receipt.get("executable_count") is not None
                else -1
            )
            != history_executable
            or receipt_published_at != history_published_at
            or current_receipt.get("membership_proofs")
            != [history_membership]
        ):
            return False, "analysis strategy pool scheduler receipt differs from run history"

        producer_rows = connection.execute(text("""
            SELECT id, run_uid, scheduler_job_id, trade_date, status, build_sha,
                   total, passed, started_at, finished_at,
                   publisher_task_type, canonical_pool_sha256, published_at,
                   executable_count, membership_snapshot_date,
                   membership_snapshot_source, membership_proof_sha256
            FROM st_recommended_run_history
            WHERE trade_date=:trade_date
              AND build_sha=:build_sha
              AND status='done'
              AND published_at IS NOT NULL
            ORDER BY published_at DESC, id DESC
            LIMIT 2
        """), {
            "trade_date": target,
            "build_sha": build_sha,
        }).mappings().all()
        if not producer_rows:
            return False, "analysis strategy pool has no completed canonical producer"
        producer = dict(producer_rows[0])
        producer_uid = str(producer.get("run_uid") or "").strip().lower()
        producer_task_type = str(
            producer.get("publisher_task_type") or ""
        ).strip()
        producer_started_at = _coerce_datetime(producer.get("started_at"))
        producer_finished_at = _coerce_datetime(producer.get("finished_at"))
        producer_published_at = _coerce_datetime(producer.get("published_at"))
        try:
            producer_total = int(producer.get("total"))
            producer_passed = int(producer.get("passed"))
            producer_executable = int(producer.get("executable_count"))
        except (TypeError, ValueError):
            producer_total = producer_passed = producer_executable = -1
        producer_hash = str(
            producer.get("canonical_pool_sha256") or ""
        ).strip().lower()
        producer_membership = {
            "snapshot_date": str(
                producer.get("membership_snapshot_date") or ""
            )[:10],
            "source": str(
                producer.get("membership_snapshot_source") or ""
            ).strip(),
            "proof_sha256": str(
                producer.get("membership_proof_sha256") or ""
            ).strip().lower(),
        }
        if (
            re.fullmatch(r"[0-9a-f]{32}", producer_uid) is None
            or str(producer.get("scheduler_job_id") or "").strip().lower()
            != producer_uid
            or producer_task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
            or re.fullmatch(r"[0-9a-f]{64}", producer_hash) is None
            or producer_total <= 0
            or producer_passed <= 0
            or producer_passed > producer_total
            or producer_executable < 0
            or producer_executable > producer_passed
            or producer_started_at is None
            or producer_finished_at is None
            or producer_published_at is None
            or producer_published_at < producer_started_at
            or producer_published_at > producer_finished_at
            or producer_finished_at > now + timedelta(minutes=5)
            or producer_membership["snapshot_date"] != target
            or producer_membership["source"] != "gj_big_qmt_inner"
            or re.fullmatch(
                r"[0-9a-f]{64}", producer_membership["proof_sha256"]
            ) is None
        ):
            return False, "analysis strategy pool latest producer receipt is invalid"

        from server.engine.strategy_industry_history import (
            resolve_analysis_industry_membership_binding,
        )

        try:
            membership_binding = resolve_analysis_industry_membership_binding(
                engine,
                trade_date=target,
                decision_known_at=now,
            )
        except Exception as exc:
            return (
                False,
                "analysis strategy pool membership proof is unavailable: "
                f"{type(exc).__name__}",
            )
        if (
            membership_binding.get("snapshot_date") != target
            or membership_binding.get("source")
            != producer_membership["source"]
            or membership_binding.get("proof_sha256")
            != producer_membership["proof_sha256"]
        ):
            return False, "analysis strategy pool membership proof differs"
        manifest = read_persisted_pool_manifest(connection, target)
    if (
        int(manifest["analysis_count"]) != producer_total
        or int(manifest["recommendation_count"]) != producer_passed
        or int(manifest["executable_count"]) != producer_executable
        or (
            int(manifest["executable_count"]) == 0
            and not research_only_publication_is_safe(manifest)
        )
        or str(manifest["canonical_pool_sha256"]).lower() != producer_hash
        or manifest.get("publisher_run_uids") != [producer_uid]
        or manifest.get("publication_statuses")
        not in (["PENDING"], ["ACTIVE"])
        or manifest.get("live_gate_alignment") is not True
        or manifest.get("membership_proofs") != [producer_membership]
    ):
        return (
            False,
            "analysis strategy pool mutable partition differs from latest "
            f"canonical producer: producer_run_uid={producer_uid} date={target} "
            f"analysis={manifest['analysis_count']}/{producer_total} "
            f"picks={manifest['recommendation_count']}/{producer_passed} "
            f"executable={manifest['executable_count']}/{producer_executable}",
        )
    return (
        True,
        "analysis strategy pool verified: "
        f"validated_run_uid={run_uid} producer_run_uid={producer_uid} "
        f"producer_task_type={producer_task_type} build={build_sha} "
        f"date={target} analysis={manifest['analysis_count']} "
        f"picks={manifest['recommendation_count']} "
        f"executable={manifest['executable_count']} "
        f"canonical_pool_sha256={producer_hash}",
    )


def validate_scheduler_task_result(
    task: Mapping[str, Any],
    *,
    engine: Engine,
    started_at: datetime | None = None,
    now: datetime | None = None,
    output: str | None = None,
) -> SchedulerValidationResult:
    task_type = str(task.get("task_type") or "").strip()
    requirements = TASK_OUTPUT_REQUIREMENTS.get(task_type)
    exact_v3_receipt = task_type in TRADING_V3_DECISION_TASK_TYPES
    exact_membership_receipt = task_type == _QMT_MEMBERSHIP_TASK_TYPE
    exact_analysis_evidence = task_type in {
        _TARGET_TURNOVER_TASK_TYPE,
        _UPPER_EVIDENCE_TASK_TYPE,
    }
    exact_provider_receipt = task_type in HOT_RANK_SOURCE_TASK_TYPES or task_type in {
        "etf_forward_daily",
        "stock_dividend_baidu",
        "notice_eastmoney",
        _NOTICE_HISTORY_TASK_TYPE,
        "sim_trade_signal_prepare",
        "eastmoney_concept_current",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "hot_rank_ths",
        "hot_concept",
        "sector_heat_east",
        "news_sync",
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
        "qmt_index_current",
        "qmt_index_kline",
        "qmt_index_minute",
        "alist_daily",
        "alist_info",
        "qmt_stock_minute_flow_canonical",
        _QMT_CANONICAL_HISTORY_REPAIR_TASK_TYPE,
        _LINUX_RECENT_DATA_GAP_REPAIR_TASK_TYPE,
        "eastmoney_concept_flow_snapshot",
        _CAPITAL_FLOW_BATCH_TASK_TYPE,
    }
    if task_type == "sector_heat_east":
        # The formal sector receipt replaces the legacy DATE= marker and
        # validates the exact 31+128 fixed provider inventory below.
        requirements = ()
    if task_type in {"hot_rank_ths", "hot_concept"}:
        # The formal receipt verifies the exact provider payload, atomic batch,
        # per-source inventory and persisted row hash below.
        requirements = ()
    if task_type in HOT_RANK_SOURCE_TASK_TYPES:
        # These source-specific receipts prove exact Top100 ranks, date-source
        # capability, one atomic batch and an independent persisted readback.
        requirements = ()
    if task_type in _EASTMONEY_ALIST_TASK_DATASETS:
        # Exact provider receipts support an authoritative empty session and
        # replace the legacy non-empty table threshold.
        requirements = ()
    if _analysis_web_manual_profile_is_exact(task):
        requirements = TASK_OUTPUT_REQUIREMENTS["analysis_morning_strict"]
    if (
        not requirements
        and not exact_v3_receipt
        and not exact_provider_receipt
        and not exact_membership_receipt
        and not exact_analysis_evidence
    ):
        return SchedulerValidationResult(checked=False, ok=True, message="no data validation configured")

    shanghai_now = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    started_at = started_at or shanghai_now
    now = now or shanghai_now
    messages: list[str] = []
    try:
        if task_type == _TARGET_TURNOVER_TASK_TYPE:
            ok, message = _validate_target_turnover_scheduler_receipt(
                task,
                engine=engine,
                output=output,
            )
            return SchedulerValidationResult(
                checked=True,
                ok=ok,
                message=message,
            )
        if task_type == _UPPER_EVIDENCE_TASK_TYPE:
            ok, message = _validate_upper_evidence_scheduler_receipt(
                task,
                engine=engine,
                output=output,
            )
            return SchedulerValidationResult(
                checked=True,
                ok=ok,
                message=message,
            )
        release_target_date = _release_target_date_from_task(task)
        capital_flow_payload = (
            _capital_flow_batch_payload(output)
            if task_type == _CAPITAL_FLOW_BATCH_TASK_TYPE
            else None
        )
        capital_flow_execution = (
            _capital_flow_execution_payload(capital_flow_payload)
            if isinstance(capital_flow_payload, Mapping)
            else None
        )
        direct_flow_receipt = bool(
            isinstance(capital_flow_payload, Mapping)
            and capital_flow_payload.get("schema")
            == _DIRECT_CAPITAL_FLOW_RESULT_SCHEMA
        )
        historical_flow_receipt = bool(
            isinstance(capital_flow_execution, Mapping)
            and capital_flow_execution.get("target_kind") == "historical"
            and capital_flow_execution.get("mode")
            in {
                _CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING,
                _CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR,
            }
            or direct_flow_receipt
            and release_target_date is not None
        )
        for requirement in requirements or ():
            effective_requirement = (
                replace(requirement, require_fresh=False)
                if task_type == _CAPITAL_FLOW_BATCH_TASK_TYPE
                and (historical_flow_receipt or direct_flow_receipt)
                else requirement
            )
            ok, message = _validate_requirement(
                engine,
                effective_requirement,
                started_at=started_at,
                now=now,
                output=output,
                target_date_override=release_target_date,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(checked=True, ok=False, message=message)
        if exact_membership_receipt:
            release_verification = (
                str(task.get("_trigger_source") or "").strip()
                == "release_catchup"
            )
            if release_verification:
                if release_target_date is None:
                    raise ValueError(
                        "qmt_membership_snapshot: release target date is missing"
                    )
                target = release_target_date
                payload = _qmt_membership_verification_payload(output)
                receipt_time_field = "verified_at"
            else:
                raw_target = authoritative_closed_trade_date(
                    engine,
                    now=now,
                    close_ready_time=time(15, 10),
                )
                try:
                    target = date.fromisoformat(raw_target)
                except ValueError as exc:
                    raise ValueError(
                        "qmt_membership_snapshot: authoritative publication "
                        "target is unavailable"
                    ) from exc
                if target.isoformat() != raw_target:
                    raise ValueError(
                        "qmt_membership_snapshot: authoritative publication "
                        "target is invalid"
                    )
                payload = _qmt_membership_publication_payload(output)
                receipt_time_field = "published_at"
            if payload is None:
                raise ValueError(
                    "qmt_membership_snapshot: exact persisted receipt is missing"
                )
            from integrations.bigqmt.membership_snapshot import (
                verify_existing_membership_snapshot,
            )
            from tools.sync_bigqmt_reference import (
                validate_membership_publication_receipt,
                validate_membership_verification_receipt,
            )

            disposition = (
                validate_membership_verification_receipt(
                    dict(payload),
                    0,
                    expected_snapshot_date=target.isoformat(),
                )
                if release_verification
                else validate_membership_publication_receipt(
                    dict(payload),
                    0,
                    expected_snapshot_date=target.isoformat(),
                )
            )
            if disposition != "complete":
                raise ValueError(
                    "qmt_membership_snapshot: exact persisted receipt is incomplete"
                )
            verified_at = _machine_timestamp(payload.get(receipt_time_field))
            if (
                verified_at is None
                or verified_at < started_at - timedelta(minutes=5)
                or verified_at > now + timedelta(minutes=5)
            ):
                raise ValueError(
                    "qmt_membership_snapshot: verification receipt is not fresh"
                )
            proof = verify_existing_membership_snapshot(
                engine,
                snapshot_date=target,
                decision_known_at=now,
            )
            if payload.get("proof") != proof:
                raise ValueError(
                    "qmt_membership_snapshot: persisted exact proof differs from receipt"
                )
            messages.append(
                "qmt_membership_snapshot exact immutable snapshot verified: "
                f"date={target.isoformat()} "
                f"concept_relations={int(proof['concept_relation_count'])} "
                f"industry_relations={int(proof['industry_relation_count'])}"
            )
        if task_type == _CAPITAL_FLOW_BATCH_TASK_TYPE:
            payload = capital_flow_payload
            if payload is None or scheduler_output_status(
                task,
                output,
                return_code=0,
            ) != "success":
                raise ValueError(
                    "capital_flow_batch_fast: exact machine receipt is invalid"
                )
            target_date = date.fromisoformat(str(payload["trade_date"]))
            if (
                release_target_date is not None
                and target_date != release_target_date
            ):
                raise ValueError(
                    "capital_flow_batch_fast: receipt date differs from release target"
                )
            expected_build_sha = str(
                task.get("_scheduler_expected_build_sha") or ""
            ).strip().lower()
            if (
                not _is_hex(expected_build_sha, 40)
                or expected_build_sha == "0" * 40
                or str(payload.get("build_sha") or "").strip().lower()
                != expected_build_sha
            ):
                raise ValueError(
                    "capital_flow_batch_fast: receipt build differs from scheduler"
                )
            if direct_flow_receipt:
                verified_at = _machine_timestamp(payload.get("verified_at"))
                if (
                    verified_at is None
                    or verified_at < started_at - timedelta(minutes=5)
                    or verified_at > now + timedelta(minutes=5)
                ):
                    raise ValueError(
                        "capital_flow_batch_fast: direct QMT receipt is not fresh "
                        "for this scheduler run"
                    )
                mode = str(payload.get("verification_mode") or "")
            else:
                generated_at = _machine_timestamp(payload.get("generated_at"))
                captured_at = _machine_timestamp(payload.get("captured_at"))
                if (
                    generated_at is None
                    or captured_at is None
                    or generated_at < started_at - timedelta(minutes=5)
                    or generated_at > now + timedelta(minutes=5)
                    or captured_at < started_at - timedelta(minutes=5)
                    or captured_at > now + timedelta(minutes=5)
                ):
                    raise ValueError(
                        "capital_flow_batch_fast: receipt is not fresh for this "
                        "scheduler run"
                    )
                mode = str(payload.get("execution_mode") or "")
            trigger_source = str(task.get("_trigger_source") or "").strip()
            if direct_flow_receipt and target_date == now.date():
                if now.time() < _DIRECT_CAPITAL_FLOW_READY_TIME:
                    raise ValueError(
                        "capital_flow_batch_fast: direct QMT target is not close-ready"
                    )
            elif direct_flow_receipt and target_date < now.date():
                if trigger_source != "release_catchup":
                    raise ValueError(
                        "capital_flow_batch_fast: historical direct QMT target "
                        "requires release verification"
                    )
            elif direct_flow_receipt:
                raise ValueError(
                    "capital_flow_batch_fast: receipt target is newer than scheduler date"
                )
            elif target_date == now.date():
                if (
                    now.time() < _CAPITAL_FLOW_LIVE_READY_TIME
                    or mode != _CAPITAL_FLOW_EXECUTION_CURRENT_LIVE
                ):
                    raise ValueError(
                        "capital_flow_batch_fast: latest target requires current live refresh"
                    )
            elif target_date < now.date():
                if (
                    trigger_source != "release_catchup"
                    or mode
                    not in {
                        _CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING,
                        _CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR,
                    }
                ):
                    raise ValueError(
                        "capital_flow_batch_fast: historical target requires release reuse"
                    )
            else:
                raise ValueError(
                    "capital_flow_batch_fast: receipt target is newer than scheduler date"
                )
            persisted_ok, persisted_message = (
                _validate_capital_flow_persisted_receipt(engine, payload)
            )
            if not persisted_ok:
                raise ValueError(persisted_message)
            messages.append(persisted_message)
            messages.append(
                "capital_flow_batch_fast exact partition receipt verified: "
                f"date={target_date.isoformat()} rows={int(payload['row_count'])} "
                f"mode={mode}"
            )
        if task_type in (
            {"capital_flow_batch_fast"}
            | ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        ):
            target_requirement = (requirements or ())[0]
            target_date = release_target_date or _resolve_target_date(
                engine,
                target_requirement,
                started_at=started_at,
                now=now,
                output=output,
            )
            ok, message = _validate_daily_universe_coverage(
                engine,
                task_type=task_type,
                target_date=target_date,
                decision_known_at=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type in ANALYSIS_POOL_PUBLISHER_TASK_TYPES:
            target_requirement = (requirements or ())[0]
            target_date = release_target_date or _resolve_target_date(
                engine,
                target_requirement,
                started_at=started_at,
                now=now,
                output=output,
            )
            # Membership is intentionally verified against the analysis
            # target, not against the membership support task's own cutoff.
            # At 15:10 that support task rolls to today's closed snapshot
            # while a morning analysis can still target the prior session.
            from server.engine.strategy_industry_history import (
                resolve_analysis_industry_membership_binding,
            )

            membership_proof = resolve_analysis_industry_membership_binding(
                engine,
                trade_date=target_date.isoformat(),
                decision_known_at=now,
            )
            messages.append(
                "analysis industry membership snapshot verified: "
                f"date={target_date.isoformat()} "
                f"source_date={membership_proof['source_snapshot_date']} "
                f"proof_mode={membership_proof['proof_mode']} "
                f"industry_relations="
                f"{int(membership_proof['industry_relation_count'])} "
                f"proof_sha256={membership_proof['proof_sha256']}"
            )
            ok, message = _validate_analysis_strategy_pool(
                engine,
                target_date=target_date,
                started_at=started_at,
                now=now,
                scheduler_run_uid=str(
                    task.get("_scheduler_history_run_uid") or ""
                ),
                expected_build_sha=str(
                    task.get("_scheduler_expected_build_sha") or ""
                ),
                output=output,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type in {"stock_snapshot_daily", "market_overview_daily"}:
            target_date = _extract_output_date(output)
            if target_date is None:
                raise ValueError(
                    f"{task_type}: task output is missing one unambiguous "
                    "DATE=YYYY-MM-DD marker"
                )
            if (
                release_target_date is not None
                and target_date != release_target_date
            ):
                raise ValueError(
                    f"{task_type}: output date differs from release target"
                )
            ok, message = _validate_daily_universe_coverage(
                engine,
                task_type=task_type,
                target_date=target_date,
                decision_known_at=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "stock_finance":
            ok, message = _validate_finance_scheduler_coverage(
                engine,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if exact_v3_receipt:
            ok, message = _validate_trading_v3_decision_receipt(
                engine,
                output=output,
                started_at=started_at,
                release_target_date=release_target_date,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "etf_forward_daily":
            ok, message = _validate_etf_forward_receipt(
                engine,
                output=output,
                now=now,
                release_target_date=release_target_date,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "stock_dividend_baidu":
            ok, message = _validate_dividend_baidu_receipt(
                engine,
                output=output,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "notice_eastmoney":
            ok, message = _validate_notice_eastmoney_receipt(
                engine,
                task=task,
                output=output,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == _NOTICE_HISTORY_TASK_TYPE:
            ok, message = _validate_notice_history_repair_receipt(
                task,
                engine=engine,
                output=output,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "sim_trade_signal_prepare":
            ok, message = _validate_sim_trade_prepare_receipt(
                engine,
                output=output,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type in _EASTMONEY_CONCEPT_TASK_DATASETS:
            ok, message = _validate_eastmoney_concept_market_receipt(
                engine,
                task_type=task_type,
                output=output,
                started_at=started_at,
                now=now,
                release_target_date=release_target_date,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "eastmoney_concept_flow_snapshot":
            payload = _eastmoney_concept_flow_payload(output)
            try:
                from server.common.qmt_attestation_contract import canonical_digest
                from tools.fetch_concept_flow_datacenter import validate_task_result

                if payload is None or validate_task_result(
                    dict(payload),
                    0,
                    expected_session=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else ""
                    ),
                ) != "complete":
                    raise ValueError("exact COMPLETE receipt is missing")
                manifest = payload.get("manifest")
                if not isinstance(manifest, Mapping):
                    raise ValueError("result manifest is missing")
                captured_at = _machine_timestamp(manifest.get("captured_at"))
                if (
                    captured_at is None
                    or captured_at < started_at - timedelta(minutes=5)
                    or captured_at > now + timedelta(minutes=5)
                ):
                    raise ValueError("receipt is not fresh for this scheduler run")
                source_date = date.fromisoformat(str(manifest.get("source_date") or ""))
                if (
                    release_target_date is not None
                    and (
                        source_date != release_target_date
                        or payload.get("source_date")
                        != release_target_date.isoformat()
                    )
                ):
                    raise ValueError(
                        "receipt/source date differs from release target"
                    )
                with engine.connect() as connection:
                    rows = connection.execute(text("""
                        SELECT index_code, snapshot_at
                          FROM sm_concept_capital_flow_east
                         ORDER BY index_code
                    """)).mappings().all()
                codes = [str(row.get("index_code") or "").strip() for row in rows]
                dates = {str(row.get("snapshot_at") or "")[:10] for row in rows}
                if (
                    len(rows) != int(manifest.get("row_count") or 0)
                    or len(codes) != len(set(codes))
                    or any(not code for code in codes)
                    or codes != sorted(codes)
                    or dates != {source_date.isoformat()}
                    or canonical_digest(codes) != manifest.get("code_set_hash")
                ):
                    raise ValueError("persisted concept-flow snapshot differs")
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=(
                        "eastmoney_concept_flow_snapshot: persisted exact "
                        f"coverage invalid: {exc}"
                    ),
                )
            messages.append(
                "eastmoney_concept_flow_snapshot exact persisted coverage "
                f"verified: date={source_date.isoformat()} rows={len(rows)}"
            )
        if task_type in {"hot_rank_ths", "hot_concept"}:
            ok, message = _validate_ths_hot_receipt(
                engine,
                task_type=task_type,
                output=output,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == HOT_RANK_SINA_TASK_TYPE:
            return SchedulerValidationResult(
                checked=True,
                ok=False,
                message=(
                    "hot_rank_sina: provider attention semantics are "
                    f"unverifiable ({SINA_ATTENTION_DATA_BLOCK_REASON}); "
                    "PASS/readiness validation is prohibited"
                ),
            )
        if task_type in HOT_RANK_SOURCE_TASK_TYPES:
            payload = parse_hot_rank_receipt(output)
            try:
                if payload is None or hot_rank_receipt_disposition(
                    payload,
                    task_type=task_type,
                    return_code=0,
                ) != "success":
                    raise ValueError("exact PASS receipt is missing")
                inventory = validate_persisted_hot_rank_receipt(
                    engine,
                    payload,
                    started_at,
                    now,
                    expected_target_date=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else None
                    ),
                )
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=(
                        f"{task_type}: exact persisted source receipt invalid: "
                        f"{exc}"
                    ),
                )
            messages.append(
                f"{task_type} exact persisted Top100 verified: "
                f"date={payload['requested_date']} "
                f"rows={int(inventory['row_count'])}"
            )
        if task_type == "sector_heat_east":
            ok, message = _validate_sector_heat_east_receipt(
                engine,
                output=output,
                started_at=started_at,
                now=now,
                release_target_date=release_target_date,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type == "news_sync":
            ok, message = _validate_news_sync_receipt(
                engine,
                output=output,
                started_at=started_at,
                now=now,
            )
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=message,
                )
        if task_type in {
            "qmt_stock_daily_canonical",
            "qmt_stock_minute_canonical",
        }:
            payload = _qmt_stock_edge_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            if (
                payload.get("dataset") != _QMT_STOCK_TASK_DATASETS[task_type]
                or payload.get("task_type") != task_type
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt dataset differs from task identity",
                )
            if (
                release_target_date is not None
                and payload.get("sessions")
                != [release_target_date.isoformat()]
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt session differs from release target",
                )
            try:
                from tools.sync_qmt_stock_edge import validate_persisted_result

                proof = validate_persisted_result(
                    engine,
                    payload,
                    now=now,
                    expected_session=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else ""
                    ),
                )
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact coverage invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted coverage verified: "
                f"sessions={len(proof['sessions'])} rows={proof['row_count']}"
            )
        if task_type in _QMT_INDEX_TASK_DATASETS:
            payload = _qmt_index_edge_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            if payload.get("dataset") != _QMT_INDEX_TASK_DATASETS[task_type]:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt dataset differs from task identity",
                )
            manifest = payload.get("manifest")
            if (
                release_target_date is not None
                and (
                    not isinstance(manifest, Mapping)
                    or manifest.get("sessions")
                    != [release_target_date.isoformat()]
                )
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt session differs from release target",
                )
            captured_at = (
                _machine_timestamp(manifest.get("captured_at"))
                if isinstance(manifest, Mapping)
                else None
            )
            if (
                captured_at is None
                or captured_at < started_at - timedelta(minutes=5)
                or captured_at > now + timedelta(minutes=5)
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt is not fresh for this scheduler run",
                )
            try:
                from tools.sync_qmt_index_edge import validate_persisted_result

                proof = validate_persisted_result(
                    engine,
                    payload,
                    now=now,
                    expected_session=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else ""
                    ),
                )
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact coverage invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted coverage verified: "
                f"sessions={len(proof['sessions'])} rows={proof['row_count']}"
            )
        if task_type in _EASTMONEY_ALIST_TASK_DATASETS:
            payload = _eastmoney_alist_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            if (
                payload.get("dataset")
                != _EASTMONEY_ALIST_TASK_DATASETS[task_type]
                or payload.get("task_type") != task_type
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt dataset differs from task identity",
                )
            if (
                release_target_date is not None
                and payload.get("trade_date")
                != release_target_date.isoformat()
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt session differs from release target",
                )
            finished_at = _shanghai_machine_timestamp(payload.get("finished_at"))
            if (
                finished_at is None
                or finished_at < started_at - timedelta(minutes=5)
                or finished_at > now + timedelta(minutes=5)
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt is not fresh for this scheduler run",
                )
            try:
                from tools.sync_eastmoney_alist_exact import validate_persisted_result

                proof = validate_persisted_result(
                    engine,
                    payload,
                    now=now,
                    expected_session=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else ""
                    ),
                )
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact coverage invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted coverage verified: "
                f"date={proof['trade_date']} rows={proof['row_count']} "
                f"codes={proof['code_count']}"
            )
        if task_type == _QMT_MINUTE_FLOW_TASK_TYPE:
            payload = _qmt_minute_flow_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            if (
                payload.get("task_type") != task_type
                or payload.get("dataset") != "stock_minute_capital_flow"
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt dataset differs from task identity",
                )
            if (
                release_target_date is not None
                and payload.get("trade_date")
                != release_target_date.isoformat()
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt session differs from release target",
                )
            finished_at = _shanghai_machine_timestamp(payload.get("finished_at"))
            if (
                finished_at is None
                or finished_at < started_at - timedelta(minutes=5)
                or finished_at > now + timedelta(minutes=5)
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt is not fresh for this scheduler run",
                )
            try:
                from tools.sync_qmt_minute_flow_exact import validate_persisted_result

                proof = validate_persisted_result(
                    engine,
                    payload,
                    now=now,
                    expected_session=(
                        release_target_date.isoformat()
                        if release_target_date is not None
                        else ""
                    ),
                )
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact coverage invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted coverage verified: "
                f"date={proof['trade_date']} rows={proof['row_count']} "
                f"codes={proof['code_count']}"
            )
        if task_type == _QMT_CANONICAL_HISTORY_REPAIR_TASK_TYPE:
            payload = _qmt_canonical_history_repair_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            receipt_started = _shanghai_machine_timestamp(
                payload.get("started_at")
            )
            receipt_finished = _shanghai_machine_timestamp(
                payload.get("finished_at")
            )
            if (
                receipt_started is None
                or receipt_finished is None
                or receipt_started > receipt_finished
                or receipt_started < started_at - timedelta(minutes=5)
                or receipt_finished > now + timedelta(minutes=5)
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt is not fresh for this scheduler run",
                )
            try:
                from tools.repair_qmt_canonical_history_gaps import (
                    validate_persisted_result,
                )

                proof = validate_persisted_result(engine, payload, now=now)
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact window invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted window verified: "
                f"sessions={len(proof['sessions'])} "
                f"partitions={proof['partition_count']}"
            )
        if task_type == _LINUX_RECENT_DATA_GAP_REPAIR_TASK_TYPE:
            payload = _linux_recent_data_gap_repair_payload(output)
            if payload is None:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: exact machine receipt is missing",
                )
            receipt_started = _shanghai_machine_timestamp(
                payload.get("started_at")
            )
            receipt_finished = _shanghai_machine_timestamp(
                payload.get("finished_at")
            )
            if (
                receipt_started is None
                or receipt_finished is None
                or receipt_started > receipt_finished
                or receipt_started < started_at - timedelta(minutes=5)
                or receipt_finished > now + timedelta(minutes=5)
            ):
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: receipt is not fresh for this scheduler run",
                )
            try:
                from tools.repair_linux_recent_data_gaps import (
                    validate_persisted_result,
                )

                proof = validate_persisted_result(engine, payload, now=now)
            except Exception as exc:
                return SchedulerValidationResult(
                    checked=True,
                    ok=False,
                    message=f"{task_type}: persisted exact window invalid: {exc}",
                )
            messages.append(
                f"{task_type} exact persisted window verified: "
                f"sessions={len(proof['sessions'])} "
                f"partitions={proof['partition_count']}"
            )
    except Exception as exc:  # pylint: disable=broad-except
        return SchedulerValidationResult(checked=True, ok=False, message=f"validation error: {exc}")
    return SchedulerValidationResult(checked=True, ok=True, message="; ".join(messages))


def _fresh_receipt_window(
    payload: Mapping[str, Any],
    *,
    started_at: datetime,
    now: datetime,
) -> tuple[datetime, datetime] | None:
    receipt_started = _machine_timestamp(payload.get("started_at"))
    receipt_finished = _machine_timestamp(payload.get("finished_at"))
    if receipt_started is None or receipt_finished is None:
        return None
    if receipt_started > receipt_finished:
        return None
    if receipt_started < started_at - timedelta(minutes=5):
        return None
    if receipt_started > now + timedelta(minutes=5):
        return None
    if receipt_finished > now + timedelta(minutes=5):
        return None
    return receipt_started, receipt_finished


def _validate_eastmoney_concept_market_receipt(
    engine: Engine,
    *,
    task_type: str,
    output: str | None,
    started_at: datetime,
    now: datetime,
    release_target_date: date | None = None,
) -> tuple[bool, str]:
    payload = _eastmoney_concept_market_payload(output)
    dataset = _EASTMONEY_CONCEPT_TASK_DATASETS.get(task_type)
    if (
        payload is None
        or dataset is None
        or _eastmoney_concept_market_output_status(
            task_type,
            output,
            return_code=0,
        )
        != "success"
    ):
        return False, f"{task_type}: exact PASS receipt is missing or invalid"
    receipt_started = _shanghai_machine_timestamp(payload.get("started_at"))
    receipt_finished = _shanghai_machine_timestamp(payload.get("finished_at"))
    if (
        receipt_started is None
        or receipt_finished is None
        or receipt_started < started_at - timedelta(minutes=5)
        or receipt_started > receipt_finished
        or receipt_finished > now + timedelta(minutes=5)
    ):
        return False, f"{task_type}: receipt execution window is stale or invalid"
    target = date.fromisoformat(str(payload["target_trade_date"]))
    if release_target_date is not None and target != release_target_date:
        return False, f"{task_type}: receipt date differs from release target"
    ready_time = release_catchup_closed_ready_time(task_type).strftime("%H:%M")
    authoritative = _latest_trade_date(
        engine,
        now=now,
        ready_time=ready_time,
    )
    if authoritative is None or target != authoritative:
        return False, f"{task_type}: target is not the authoritative closed session"

    table = {
        "current": "sm_concept_east_current",
        "kline": "sm_concept_east_kline",
        "minute": "sm_concept_east_minute",
    }[dataset]
    numeric_columns = {
        "current": "`open`,price,high,low,volume,amount",
        "kline": "`open`,`close`,high,low,volume,amount",
        "minute": "price,avg_price,volume,amount",
    }[dataset]
    where_sql = ""
    params: dict[str, Any] = {}
    if dataset != "current":
        where_sql = "WHERE trade_date=:target_date"
        params["target_date"] = target.isoformat()
        if dataset == "kline":
            where_sql += " AND k_type=1"
    rows = _read_all(
        engine,
        f"""
        SELECT index_code, trade_date, trade_time, etl_sync_at,
               {numeric_columns}
        FROM {quote_identifier(table)}
        {where_sql}
        ORDER BY index_code, trade_time
        """,
        params,
    )
    expected = payload["dataset_results"][dataset]
    expected_count = int(expected["row_count"])
    if len(rows) != expected_count:
        return False, f"{task_type}: persisted row count differs from receipt"
    codes = sorted(
        {str(row.get("index_code") or "").strip().upper() for row in rows}
    )
    dates = sorted({str(row.get("trade_date") or "")[:10] for row in rows})
    if (
        len(codes) != int(expected["code_count"])
        or dates != [target.isoformat()]
        or _notice_code_set_hash(codes) != expected.get("code_set_sha256")
    ):
        # Both providers deliberately use the same newline-delimited set hash.
        return False, f"{task_type}: persisted code/date set differs from receipt"
    directory = payload["directory"]
    if expected.get("code_set_sha256") != directory.get("code_set_sha256"):
        return False, f"{task_type}: dataset and directory identities differ"
    for row in rows:
        etl_sync_at = _coerce_datetime(row.get("etl_sync_at"))
        trade_time = _coerce_datetime(row.get("trade_time"))
        if (
            etl_sync_at is None
            or etl_sync_at < receipt_started - timedelta(minutes=5)
            or etl_sync_at > receipt_finished + timedelta(minutes=5)
            or trade_time is None
        ):
            return False, f"{task_type}: persisted timestamps are invalid"
        try:
            price = float(
                row.get("price")
                if dataset != "kline"
                else row.get("close")
            )
            volume = float(row.get("volume"))
            amount = float(row.get("amount"))
        except (TypeError, ValueError, OverflowError):
            return False, f"{task_type}: persisted numeric values are malformed"
        if price <= 0 or volume < 0 or amount < 0:
            return False, f"{task_type}: persisted numeric values are out of range"

    if dataset == "current":
        if any(
            _coerce_datetime(row.get("trade_time")).date() != target
            or _coerce_datetime(row.get("trade_time")).hour < 15
            for row in rows
        ):
            return False, f"{task_type}: current source timestamps are not post-close"
    elif dataset == "minute":
        # Eastmoney minute bars are stamped 09:31..11:30 and 13:01..15:00.
        expected_grid: set[str] = set()
        for start_hour, start_minute, end_hour, end_minute in (
            (9, 31, 11, 30),
            (13, 1, 15, 0),
        ):
            cursor = datetime(
                target.year,
                target.month,
                target.day,
                start_hour,
                start_minute,
            )
            stop = datetime(
                target.year,
                target.month,
                target.day,
                end_hour,
                end_minute,
            )
            while cursor <= stop:
                expected_grid.add(cursor.strftime("%H:%M"))
                cursor += timedelta(minutes=1)
        observed_by_code: dict[str, set[str]] = {}
        for row in rows:
            code = str(row.get("index_code") or "").strip().upper()
            observed_by_code.setdefault(code, set()).add(
                _coerce_datetime(row.get("trade_time")).strftime("%H:%M")
            )
        if len(expected_grid) != 240 or any(
            minutes != expected_grid for minutes in observed_by_code.values()
        ):
            return False, f"{task_type}: persisted minute grid is incomplete"
    database = payload["db_metrics"][dataset]
    if (
        int(database["row_count"]) != len(rows)
        or int(database["code_count"]) != len(codes)
        or int(database["date_count"]) != len(dates)
    ):
        return False, f"{task_type}: database metrics differ from receipt"
    return (
        True,
        f"{task_type} exact Eastmoney matrix verified: "
        f"date={target.isoformat()} codes={len(codes)} rows={len(rows)}",
    )


def _validate_ths_hot_receipt(
    engine: Engine,
    *,
    task_type: str,
    output: str | None,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    payload = _ths_hot_payload(output)
    if (
        payload is None
        or _ths_hot_output_status(task_type, output, return_code=0)
        != "success"
    ):
        return False, f"{task_type}: exact PASS receipt is missing or invalid"
    try:
        from server.common.ths_hot_contract import (
            THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE,
            THS_HOT_CONCEPT_TASK_TYPE,
            THS_HOT_RANK_MIN_ROWS,
            THS_HOT_RANK_TASK_TYPE,
            batch_timestamp,
            canonical_hash,
            require_capture_window,
            validate_concept_inventory,
            validate_rank_inventory,
        )

        requested = date.fromisoformat(str(payload.get("requested_date") or ""))
        data_date = date.fromisoformat(str(payload.get("data_date") or ""))
        receipt_started = _machine_timestamp(payload.get("started_at"))
        captured_at = _machine_timestamp(payload.get("captured_at"))
        batch_at = _machine_timestamp(payload.get("batch_at"))
        published_at = _machine_timestamp(payload.get("published_at"))
        if (
            requested != data_date
            or receipt_started is None
            or captured_at is None
            or batch_at is None
            or published_at is None
            or receipt_started < started_at - timedelta(minutes=5)
            or receipt_started > now + timedelta(minutes=5)
            or not (
                receipt_started <= captured_at <= batch_at <= published_at
                <= now + timedelta(minutes=5)
            )
        ):
            raise ValueError("receipt execution window differs")
        require_capture_window(
            engine,
            task_type=task_type,
            requested_date=requested.isoformat(),
            now=receipt_started,
        )
        expected_batch_id = canonical_hash({
            "task_type": task_type,
            "requested_date": requested.isoformat(),
            "started_at": receipt_started.isoformat(sep=" "),
            "batch_at": batch_at.isoformat(sep=" "),
            "provider_payload_sha256": payload.get(
                "provider_payload_sha256"
            ),
        })
        if payload.get("batch_id") != expected_batch_id:
            raise ValueError("receipt batch identity differs")

        if task_type == THS_HOT_RANK_TASK_TYPE:
            rows = _read_all(
                engine,
                """
                SELECT snapshot_date, `rank`, stock_code, short_name,
                       change_pct, hot_value, pop_tag, concept_tag, etl_sync_at
                  FROM st_hot_rank_ths
                 WHERE snapshot_date=:target_date
                 ORDER BY `rank`, stock_code
                """,
                {"target_date": requested.isoformat()},
            )
            actual = validate_rank_inventory(
                rows,
                target_date=requested.isoformat(),
                minimum=THS_HOT_RANK_MIN_ROWS,
            )
            expected_fields = (
                "row_count",
                "provider_payload_sha256",
                "persisted_row_sha256",
                "code_set_sha256",
                "rank_set_sha256",
            )
            inventory_note = f"rows={actual['row_count']}"
        elif task_type == THS_HOT_CONCEPT_TASK_TYPE:
            rows = _read_all(
                engine,
                """
                SELECT snapshot_date, plate_type, `rank`, concept_code,
                       concept_name, change_pct, hot_value, hot_tag, etl_sync_at
                  FROM st_hot_concept_ths_daily
                 WHERE snapshot_date=:target_date AND plate_type IN (1,2)
                 ORDER BY plate_type, `rank`, concept_code
                """,
                {"target_date": requested.isoformat()},
            )
            actual = validate_concept_inventory(
                rows,
                target_date=requested.isoformat(),
                minimum_per_type=THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE,
            )
            expected_fields = (
                "row_count",
                "plate_type_counts",
                "provider_payload_sha256",
                "persisted_row_sha256",
                "identity_set_sha256",
            )
            inventory_note = (
                f"plate_type_counts={actual['plate_type_counts']} "
                f"rows={actual['row_count']}"
            )
        else:
            raise ValueError("unknown THS hot task type")
        if any(payload.get(field) != actual.get(field) for field in expected_fields):
            raise ValueError("persisted inventory/hash differs from receipt")
        persisted_batch = batch_timestamp(rows)
        if persisted_batch != batch_at.isoformat(sep=" "):
            raise ValueError("persisted atomic batch differs from receipt")
    except Exception as exc:
        return False, f"{task_type}: persisted exact THS receipt invalid: {exc}"
    return (
        True,
        f"{task_type} exact THS current snapshot verified: "
        f"date={requested.isoformat()} {inventory_note}",
    )


def _validate_sector_heat_east_receipt(
    engine: Engine,
    *,
    output: str | None,
    started_at: datetime,
    now: datetime,
    release_target_date: date | None = None,
) -> tuple[bool, str]:
    payload = _sector_heat_east_payload(output)
    if (
        payload is None
        or _sector_heat_east_output_status(output, return_code=0)
        != "success"
    ):
        return False, "sector_heat_east: exact PASS receipt is missing or invalid"
    receipt_window = _fresh_receipt_window(
        payload,
        started_at=started_at,
        now=now,
    )
    if receipt_window is None:
        return False, "sector_heat_east: receipt execution window is stale or invalid"
    target = date.fromisoformat(str(payload["data_date"]))
    requested = date.fromisoformat(str(payload["requested_date"]))
    if requested != target:
        return False, "sector_heat_east: receipt requested/data dates differ"
    if release_target_date is not None and target != release_target_date:
        return False, "sector_heat_east: receipt date differs from release target"
    authoritative = _latest_trade_date(
        engine,
        now=now,
        ready_time=release_catchup_closed_ready_time(
            "sector_heat_east"
        ).strftime("%H:%M"),
    )
    if authoritative is None or target != authoritative:
        return False, "sector_heat_east: data date is not the latest closed session"
    rows = _read_all(
        engine,
        """
        SELECT snapshot_date, plate_type, `rank`, concept_code,
               concept_name, change_pct, hot_value, hot_tag
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date=:target_date AND plate_type IN (3,4)
        ORDER BY plate_type, `rank`, concept_code
        """,
        {"target_date": target.isoformat()},
    )
    try:
        from tools.fetch_sector_heat_east_daily import (
            validate_formal_sector_rows,
        )

        actual = validate_formal_sector_rows(
            rows,
            target_date=target.isoformat(),
            raw_count=int(payload["evidence"]["raw_count"]),
        )
    except Exception as exc:
        return False, f"sector_heat_east: persisted fixed inventory invalid: {exc}"
    if actual != dict(payload["evidence"]):
        return False, "sector_heat_east: persisted row hash differs from receipt"
    return (
        True,
        "sector_heat_east fixed inventory verified: "
        f"date={target.isoformat()} l1={actual['l1_count']} "
        f"l2={actual['l2_count']} rows={actual['row_count']}",
    )


def _validate_news_sync_receipt(
    engine: Engine,
    *,
    output: str | None,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    payload = _news_sync_payload(output)
    if payload is None or _news_sync_output_status(output, return_code=0) != "success":
        return False, "news_sync: exact PASS receipt is missing or invalid"
    synthetic_window = {
        "started_at": payload.get("batch_started_at"),
        "finished_at": payload.get("batch_finished_at"),
    }
    receipt_window = _fresh_receipt_window(
        synthetic_window,
        started_at=started_at,
        now=now,
    )
    if receipt_window is None:
        return False, "news_sync: receipt execution window is stale or invalid"
    receipt_started, receipt_finished = receipt_window
    rows = _read_all(
        engine,
        """
        SELECT source, source_id, title, content, publish_time, level,
               stocks, subjects, reading_num, is_top, jpush
        FROM st_news_flash
        WHERE etl_sync_at >= :receipt_started
          AND etl_sync_at <= :receipt_finished
        ORDER BY source, source_id
        """,
        {
            "receipt_started": receipt_started,
            "receipt_finished": receipt_finished,
        },
    )
    if any(
        not str(row.get("title") or "").strip()
        and not str(row.get("content") or "").strip()
        for row in rows
    ):
        return False, "news_sync: persisted batch contains content-free rows"
    try:
        from tools.sync_news_formal import canonical_news_items, news_row_hash

        canonical = canonical_news_items(rows)
    except Exception as exc:
        return False, f"news_sync: persisted batch is malformed: {exc}"
    evidence = payload["evidence"]
    if (
        len(canonical) != int(evidence["persisted_count"])
        or news_row_hash(canonical) != evidence.get("row_hash")
    ):
        return False, "news_sync: persisted batch differs from receipt"
    latest_publish = max(
        datetime.fromisoformat(str(row["publish_time"])) for row in canonical
    ).replace(tzinfo=None)
    if (
        latest_publish.isoformat(timespec="seconds")
        != str(evidence["latest_publish_time"])
        or latest_publish < receipt_started - timedelta(hours=12)
        or latest_publish > receipt_finished + timedelta(minutes=5)
    ):
        return False, "news_sync: latest publication time is stale or invalid"
    return (
        True,
        "news_sync persisted batch verified: "
        f"sources={payload['nonempty_sources']} rows={len(canonical)} "
        f"latest={latest_publish.isoformat(timespec='seconds')}",
    )


def _validate_notice_eastmoney_receipt(
    engine: Engine,
    *,
    task: Mapping[str, Any] | None = None,
    output: str | None,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    payload = _notice_eastmoney_payload(output)
    if (
        payload is None
        or _notice_eastmoney_output_status(
            task or {},
            output,
            return_code=0,
        )
        != "success"
    ):
        return False, "notice_eastmoney: exact PASS receipt is missing or invalid"
    receipt_window = _fresh_receipt_window(
        payload,
        started_at=started_at,
        now=now,
    )
    if receipt_window is None:
        return False, "notice_eastmoney: receipt execution window is stale or invalid"
    receipt_started, receipt_finished = receipt_window
    request_window_start = date.fromisoformat(
        str(payload["request_window_start"])
    )
    request_window_end = date.fromisoformat(
        str(payload["request_window_end"])
    )
    batch_id = str(payload["batch_id"])

    expected_rows = _read_all(
        engine,
        """
        SELECT stock_code
        FROM si_all_code
        ORDER BY stock_code
        """,
    )
    expected_codes = [
        str(row.get("stock_code") or "").strip().zfill(6)
        for row in expected_rows
    ]
    if (
        not expected_codes
        or len(expected_codes) != len(set(expected_codes))
        or any(re.fullmatch(r"\d{6}", code) is None for code in expected_codes)
    ):
        return False, "notice_eastmoney: authoritative stock universe is empty or invalid"
    expected_hash = _notice_code_set_hash(expected_codes)
    if (
        int(payload["requested_code_count"]) != len(expected_codes)
        or payload.get("requested_code_set_hash") != expected_hash
        or payload.get("succeeded_code_set_hash") != expected_hash
    ):
        return False, "notice_eastmoney: requested universe differs from si_all_code"

    fresh_rows = _read_all(
        engine,
        """
        SELECT notice.stock_code, notice.art_code, notice.notice_date,
               notice.title, notice.column_name, notice.display_time,
               notice.detail_url, notice.association_validated,
               notice.etl_sync_at, notice.qmt_code, notice.data_source,
               notice.source_time, notice.received_at, notice.batch_id,
               notice.data_version, notice.quality_status,
               notice.permission_status
          FROM si_notice_eastmoney AS notice
          JOIN si_all_code AS catalog
            ON catalog.stock_code = notice.stock_code
         WHERE notice.notice_date >= :request_window_start
           AND notice.notice_date <= :request_window_end
         ORDER BY notice.stock_code, notice.art_code
        """,
        {
            "request_window_start": request_window_start,
            "request_window_end": request_window_end,
        },
    )
    from integrations.qmt.backend import to_qmt_symbol

    identities: set[tuple[str, str]] = set()
    malformed: list[Mapping[str, Any]] = []
    for row in fresh_rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        art_code = str(row.get("art_code") or "").strip()
        identity = (code, art_code)
        notice_date = _coerce_date(row.get("notice_date"))
        source_time = _coerce_datetime(row.get("source_time"))
        received_at = _coerce_datetime(row.get("received_at"))
        etl_sync_at = _coerce_datetime(row.get("etl_sync_at"))
        association_raw = row.get("association_validated")
        try:
            association_validated = (
                not isinstance(association_raw, bool)
                and int(association_raw or 0) == 1
            )
        except (TypeError, ValueError, OverflowError):
            association_validated = False
        valid = bool(
            code in expected_codes
            and re.fullmatch(r"\d{6}", code) is not None
            and art_code
            and identity not in identities
            and notice_date is not None
            and request_window_start <= notice_date <= request_window_end
            and str(row.get("title") or "").strip()
            and association_validated
            and str(row.get("qmt_code") or "").strip().upper()
            == to_qmt_symbol(code)
            and str(row.get("data_source") or "").strip()
            == _NOTICE_PROVIDER_ID
            and source_time is not None
            and received_at is not None
            and etl_sync_at is not None
            and source_time <= received_at
            and receipt_started <= received_at <= receipt_finished
            and receipt_started <= etl_sync_at <= receipt_finished
            and received_at.replace(microsecond=0)
            == etl_sync_at.replace(microsecond=0)
            and str(row.get("batch_id") or "").strip() == batch_id
            and str(row.get("data_version") or "").strip()
            == _NOTICE_DATA_VERSION
            and str(row.get("quality_status") or "").strip()
            == _NOTICE_QUALITY_STATUS
            and str(row.get("permission_status") or "").strip()
            == _NOTICE_PERMISSION_STATUS
        )
        if not valid:
            malformed.append(row)
        identities.add(identity)
    if malformed:
        return (
            False,
            "notice_eastmoney: persisted window contains invalid association "
            "or provenance",
        )
    written = int(payload["written_notice_count"])
    if len(fresh_rows) != written:
        return (
            False,
            "notice_eastmoney: persisted fresh row count differs from receipt: "
            f"database={len(fresh_rows)} receipt={written}",
        )
    if (
        _notice_persisted_manifest_hash(
            fresh_rows,
            requested_codes=expected_codes,
        )
        != payload.get("persisted_manifest_sha256")
    ):
        return False, "notice_eastmoney: persisted content hash differs from receipt"
    actual_nonempty_codes = sorted(
        {
            str(row.get("stock_code") or "").strip().zfill(6)
            for row in fresh_rows
        }
    )
    expected_set = set(expected_codes)
    if not set(actual_nonempty_codes).issubset(expected_set):
        return False, "notice_eastmoney: fresh rows contain codes outside si_all_code"
    if (
        len(actual_nonempty_codes) != int(payload["nonempty_code_count"])
        or _notice_code_set_hash(actual_nonempty_codes)
        != payload.get("nonempty_code_set_hash")
    ):
        return False, "notice_eastmoney: non-empty code set differs from receipt"
    actual_empty_codes = sorted(expected_set - set(actual_nonempty_codes))
    if (
        len(actual_empty_codes)
        != int(payload["authoritative_empty_code_count"])
        or _notice_code_set_hash(actual_empty_codes)
        != payload.get("authoritative_empty_code_set_hash")
    ):
        return False, "notice_eastmoney: authoritative-empty code set differs from receipt"
    return (
        True,
        "notice_eastmoney full-market receipt verified: "
        f"codes={len(expected_codes)} nonempty={len(actual_nonempty_codes)} "
        f"authoritative_empty={len(actual_empty_codes)} rows={len(fresh_rows)}",
    )


def _notice_history_ledger_path(task: Mapping[str, Any]) -> Path:
    if (
        str(task.get("script_path") or "").replace("\\", "/").strip()
        != "biz/notice/sync_notice_em.py"
    ):
        raise ValueError("notice history task script identity differs")
    try:
        tokens = shlex.split(
            str(task.get("script_args") or ""),
            posix=os.name != "nt",
        )
    except ValueError as exc:
        raise ValueError("notice history task arguments are malformed") from exc

    def option_values(option: str) -> list[str]:
        values: list[str] = []
        for index, token in enumerate(tokens):
            if token == option:
                if index + 1 >= len(tokens) or tokens[index + 1].startswith("--"):
                    raise ValueError(f"notice history task {option} lacks a value")
                values.append(tokens[index + 1])
            elif token.startswith(option + "="):
                values.append(token.split("=", 1)[1])
        return values

    modes = option_values("--mode")
    limits = option_values("--limit")
    paths = option_values("--history-state-file")
    if (
        modes != ["historical-repair"]
        or limits != ["0"]
        or tokens.count("--from-si-all-code") != 1
        or len(paths) != 1
    ):
        raise ValueError("notice history task arguments do not select the full universe")
    path = Path(paths[0])
    if not path.is_absolute():
        raise ValueError("notice history ledger path is not absolute")
    return path


def _notice_history_strict_object(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in notice history ledger: {key}")
        result[key] = value
    return result


def _load_notice_history_ledger(
    task: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    from biz.notice import sync_notice_em

    base_path = _notice_history_ledger_path(task)
    generations = sync_notice_em._load_history_generations(base_path)
    if not generations:
        raise ValueError("notice history ledger generation is missing")
    matches = [
        ledger
        for _path, ledger in generations
        if ledger.get("ledger_sha256") == payload.get("ledger_sha256")
        and ledger.get("batch_id") == payload.get("batch_id")
        and ledger.get("requested_code_set_hash")
        == payload.get("requested_code_set_hash")
        and int(ledger.get("generation") or 1)
        == int(payload.get("ledger_generation") or 0)
    ]
    if len(matches) != 1:
        raise ValueError("notice history receipt does not select one generation")
    selected = matches[0]
    latest_generation = max(
        int(ledger.get("generation") or 1) for _path, ledger in generations
    )
    if int(selected.get("generation") or 1) != latest_generation:
        raise ValueError("notice history receipt does not select the latest generation")
    return selected


def _validate_notice_history_repair_receipt(
    task: Mapping[str, Any],
    *,
    engine: Engine,
    output: str | None,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    payload = _notice_history_repair_payload(output)
    if (
        payload is None
        or _notice_history_repair_output_status(output, return_code=0)
        != "success"
    ):
        return False, "notice history: exact COMPLETE receipt is missing or invalid"
    receipt_window = _fresh_receipt_window(
        payload,
        started_at=started_at,
        now=now,
    )
    if receipt_window is None:
        return False, "notice history: receipt execution window is stale or invalid"
    _receipt_started, receipt_finished = receipt_window
    try:
        ledger = _load_notice_history_ledger(task, payload=payload)
    except Exception as exc:
        return False, f"notice history: protected COMPLETE ledger is invalid: {exc}"
    requested_codes = [str(code) for code in ledger["requested_codes"]]
    entries = list(ledger["completed_entries"])
    entry_by_code = {
        str(entry["stock_code"]): entry for entry in entries
    }
    completed_at = _machine_timestamp(ledger.get("completed_at"))
    created_at = _machine_timestamp(ledger.get("created_at"))
    updated_at = _machine_timestamp(ledger.get("updated_at"))
    captured_times = [
        _machine_timestamp(entry.get("captured_at")) for entry in entries
    ]
    inherited_entry_count = int(ledger.get("inherited_entry_count") or 0)
    expected_ordered_hash = hashlib.sha256(
        "\n".join(requested_codes).encode("ascii")
    ).hexdigest()
    if (
        ledger.get("status") != "COMPLETE"
        or ledger.get("last_failure") is not None
        or int(ledger["next_offset"]) != len(requested_codes)
        or len(entries) != len(requested_codes)
        or list(entry_by_code) != requested_codes
        or int(payload["requested_code_count"]) != len(requested_codes)
        or int(payload["completed_code_count"]) != len(requested_codes)
        or int(payload["remaining_code_count"]) != 0
        or payload.get("batch_id") != ledger.get("batch_id")
        or payload.get("requested_code_set_hash")
        != _notice_code_set_hash(requested_codes)
        or payload.get("ordered_code_sha256") != expected_ordered_hash
        or payload.get("completed_code_set_hash")
        != ledger.get("completed_code_set_hash")
        or payload.get("ledger_sha256") != ledger.get("ledger_sha256")
        or int(payload.get("ledger_generation") or 0)
        != int(ledger.get("generation") or 1)
        or int(payload.get("inherited_entry_count") or 0)
        != inherited_entry_count
        or payload.get("parent_ledger_sha256")
        != ledger.get("parent_ledger_sha256")
        or payload.get("evidence_chain_sha256")
        != ledger.get("evidence_chain_sha256")
        or created_at is None
        or completed_at is None
        or updated_at != completed_at
        or created_at > completed_at
        or completed_at > receipt_finished + timedelta(minutes=5)
        or any(
            captured is None
            or captured > completed_at
            or index >= inherited_entry_count and captured < created_at
            for index, captured in enumerate(captured_times)
        )
    ):
        return False, "notice history: receipt and COMPLETE ledger differ"

    read_engine = routed_read_engine(
        "SELECT stock_code FROM si_notice_eastmoney",
        engine,
    )
    try:
        from integrations.qmt.backend import to_qmt_symbol

        with read_engine.connect() as connection:
            with connection.begin():
                catalog_rows = connection.execute(text("""
                    SELECT stock_code
                      FROM si_all_code
                     ORDER BY stock_code
                """)).mappings().all()
                grouped_rows = connection.execute(text("""
                    SELECT stock_code, qmt_code, COUNT(*) AS row_count
                      FROM si_notice_eastmoney
                     GROUP BY stock_code, qmt_code
                     ORDER BY stock_code, qmt_code
                """)).mappings().all()
                summary = dict(connection.execute(text("""
                    SELECT COUNT(*) AS row_count,
                           SUM(CASE
                               WHEN association_validated IS NULL
                                 OR association_validated <> 1
                                 OR notice_date IS NULL
                                 OR TRIM(COALESCE(art_code, '')) = ''
                                 OR TRIM(COALESCE(title, '')) = ''
                                 OR TRIM(COALESCE(data_source, ''))
                                    <> :data_source
                                 OR source_time IS NULL
                                 OR received_at IS NULL
                                 OR etl_sync_at IS NULL
                                 OR source_time > received_at
                                 OR received_at <> etl_sync_at
                                 OR TRIM(COALESCE(batch_id, '')) = ''
                                 OR TRIM(COALESCE(data_version, ''))
                                    <> :data_version
                                 OR TRIM(COALESCE(quality_status, ''))
                                    <> :quality_status
                                 OR TRIM(COALESCE(permission_status, ''))
                                    <> :permission_status
                               THEN 1 ELSE 0 END) AS invalid_row_count
                      FROM si_notice_eastmoney
                """), {
                    "data_source": _NOTICE_PROVIDER_ID,
                    "data_version": _NOTICE_DATA_VERSION,
                    "quality_status": _NOTICE_QUALITY_STATUS,
                    "permission_status": _NOTICE_PERMISSION_STATUS,
                }).mappings().one())
                duplicate = connection.execute(text("""
                    SELECT stock_code, art_code, COUNT(*) AS identity_count
                      FROM si_notice_eastmoney
                     GROUP BY stock_code, art_code
                    HAVING COUNT(*) <> 1
                     LIMIT 1
                """)).mappings().first()
                batch_rows = connection.execute(text("""
                    SELECT DISTINCT batch_id
                      FROM si_notice_eastmoney
                """)).mappings().all()

                catalog_codes = [
                    str(row.get("stock_code") or "").strip().zfill(6)
                    for row in catalog_rows
                ]
                if (
                    not catalog_codes
                    or catalog_codes != sorted(set(catalog_codes))
                    or any(
                        re.fullmatch(r"\d{6}", code) is None
                        for code in catalog_codes
                    )
                ):
                    return False, "notice history: current catalog is invalid"
                requested_set = set(requested_codes)
                catalog_set = set(catalog_codes)
                if not catalog_set.issubset(requested_set):
                    return False, "notice history: frozen universe misses current catalog codes"

                counts_by_code: dict[str, int] = {}
                current_codes: set[str] = set()
                for row in grouped_rows:
                    code = str(row.get("stock_code") or "").strip().zfill(6)
                    qmt_code = str(row.get("qmt_code") or "").strip().upper()
                    count = int(row.get("row_count") or 0)
                    if (
                        re.fullmatch(r"\d{6}", code) is None
                        or code not in requested_set
                        or qmt_code != to_qmt_symbol(code)
                        or count <= 0
                    ):
                        return False, "notice history: persisted code or QMT identity is invalid"
                    counts_by_code[code] = counts_by_code.get(code, 0) + count
                    current_codes.add(code)
                current_row_count = sum(counts_by_code.values())
                if (
                    current_row_count <= 0
                    or current_row_count != int(summary.get("row_count") or 0)
                    or int(summary.get("invalid_row_count") or 0) != 0
                    or duplicate is not None
                    or any(
                        not _is_hex(row.get("batch_id"), 64)
                        for row in batch_rows
                    )
                ):
                    return False, "notice history: full table provenance or identity is invalid"

                legacy_codes = sorted(requested_set - catalog_set)
                for code in legacy_codes:
                    entry = entry_by_code[code]
                    expected_count = int(entry["persisted_count"])
                    actual_count = counts_by_code.get(code, 0)
                    if actual_count != expected_count:
                        return False, (
                            "notice history: legacy association cleanup differs "
                            f"for {code}"
                        )
                    if expected_count == 0:
                        continue
                    legacy_rows = [
                        dict(row)
                        for row in connection.execute(text("""
                            SELECT stock_code, art_code, notice_date, title,
                                   column_name, display_time, detail_url,
                                   association_validated, qmt_code, data_source,
                                   source_time, received_at, batch_id,
                                   data_version, quality_status,
                                   permission_status
                              FROM si_notice_eastmoney
                             WHERE stock_code=:stock_code
                             ORDER BY stock_code, art_code
                        """), {"stock_code": code}).mappings().all()
                    ]
                    if _notice_row_hash(legacy_rows) != entry.get(
                        "persisted_row_hash"
                    ):
                        return False, (
                            "notice history: legacy persisted content hash "
                            f"differs for {code}"
                        )
    except Exception as exc:
        return False, f"notice history: full-table readback failed: {exc}"

    legacy_empty_codes = {
        code
        for code in requested_codes
        if code not in catalog_set
        and int(entry_by_code[code]["persisted_count"]) == 0
    }
    reconstructed = sorted(catalog_set | current_codes | legacy_empty_codes)
    if (
        set(reconstructed) != set(requested_codes)
        or _notice_code_set_hash(reconstructed)
        != payload.get("requested_code_set_hash")
    ):
        return False, "notice history: reconstructed full-universe hash differs"
    provider_row_count = sum(
        int(entry["persisted_count"]) for entry in entries
    )
    if provider_row_count <= 0:
        return False, "notice history: COMPLETE ledger has no provider rows"
    return (
        True,
        "notice history COMPLETE ledger and full table verified: "
        f"codes={len(requested_codes)} catalog={len(catalog_codes)} "
        f"legacy={len(requested_codes) - len(catalog_codes)} "
        f"rows={current_row_count} ledger={ledger['ledger_sha256']}",
    )


def _validate_sim_trade_prepare_receipt(
    engine: Engine,
    *,
    output: str | None,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    payload = _sim_trade_payload(output)
    if (
        payload is None
        or _sim_trade_output_status(
            "sim_trade_signal_prepare",
            output,
            return_code=0,
        )
        != "success"
    ):
        return False, "sim_trade_signal_prepare: exact receipt is missing or invalid"
    receipt_window = _fresh_receipt_window(
        payload,
        started_at=started_at,
        now=now,
    )
    if receipt_window is None:
        return False, "sim_trade_signal_prepare: receipt execution window is stale or invalid"
    receipt_started, receipt_finished = receipt_window
    trade_date = date.fromisoformat(str(payload["trade_date"]))
    if trade_date != now.date():
        return False, "sim_trade_signal_prepare: receipt trade date is not today"
    calendar_row = _read_one(
        engine,
        """
        SELECT trade_status
        FROM si_trade_calendar
        WHERE trade_date=:trade_date
        LIMIT 1
        """,
        {"trade_date": trade_date.isoformat()},
    )
    is_open = bool(calendar_row and int(calendar_row.get("trade_status") or 0) == 1)
    if str(payload.get("status") or "").upper() == "SKIPPED":
        if is_open:
            return False, "sim_trade_signal_prepare: open trade date cannot be skipped"
        return True, "sim_trade_signal_prepare verified market-closed skip"
    if not is_open:
        return False, "sim_trade_signal_prepare: PASS receipt date is not an open session"

    previous_row = _read_one(
        engine,
        """
        SELECT MAX(trade_date) AS trade_date
        FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date < :trade_date
        """,
        {"trade_date": trade_date.isoformat()},
    )
    previous_open = _coerce_date(previous_row.get("trade_date"))
    signal_date = date.fromisoformat(str(payload.get("signal_date") or ""))
    if previous_open is None or signal_date != previous_open:
        return False, "sim_trade_signal_prepare: signal date is not the previous open session"

    recommendation_rows = _read_all(
        engine,
        """
        SELECT stock_code
        FROM st_recommended_stocks
        WHERE pick_date=:signal_date
          AND (recommend_status IS NULL OR recommend_status='ALLOW')
        ORDER BY stock_code
        """,
        {"signal_date": signal_date.isoformat()},
    )
    recommendation_codes = [
        str(row.get("stock_code") or "").strip().zfill(6)
        for row in recommendation_rows
    ]
    if (
        not recommendation_codes
        or len(recommendation_codes) != len(set(recommendation_codes))
        or any(
            re.fullmatch(r"\d{6}", code) is None
            for code in recommendation_codes
        )
    ):
        return False, "sim_trade_signal_prepare: recommendation identity set is empty or invalid"
    if (
        len(recommendation_codes) != int(payload["recommendation_count"])
        or len(recommendation_codes)
        != int(payload["recommendation_code_count"])
        or _sim_identity_set_hash(recommendation_codes)
        != payload.get("recommendation_code_set_hash")
    ):
        return False, "sim_trade_signal_prepare: recommendation set differs from receipt"

    try:
        from server.engine.sim_trade_engine import STRATEGY_CONFIG
    except ImportError:
        return False, "sim_trade_signal_prepare: strategy catalog is unavailable"
    strategy_keys = set(STRATEGY_CONFIG)
    if int(payload["strategy_count"]) != len(strategy_keys) or not strategy_keys:
        return False, "sim_trade_signal_prepare: strategy catalog differs from receipt"

    signal_rows = _read_all(
        engine,
        """
        SELECT stock_code, strategy_type, updated_at
        FROM st_sim_signal
        WHERE trade_mode='live'
          AND signal_date=:signal_date
          AND trade_date=:trade_date
        ORDER BY stock_code, strategy_type
        """,
        {
            "signal_date": signal_date.isoformat(),
            "trade_date": trade_date.isoformat(),
        },
    )
    identities = [
        f"{str(row.get('stock_code') or '').strip().zfill(6)}:"
        f"{str(row.get('strategy_type') or '').strip()}"
        for row in signal_rows
    ]
    if len(identities) != len(set(identities)):
        return False, "sim_trade_signal_prepare: persisted signal identities are duplicated"
    recommendation_set = set(recommendation_codes)
    for row, identity in zip(signal_rows, identities):
        code, strategy_key = identity.split(":", 1)
        updated_at = _coerce_datetime(row.get("updated_at"))
        if (
            code not in recommendation_set
            or strategy_key not in strategy_keys
            or updated_at is None
            or updated_at < receipt_started - timedelta(minutes=5)
            or updated_at > receipt_finished + timedelta(minutes=5)
        ):
            return False, "sim_trade_signal_prepare: persisted signal scope or freshness is invalid"
    if not (
        len(signal_rows)
        == int(payload["signal_count"])
        == int(payload["signal_identity_count"])
        == int(payload["allowed_count"])
    ) or (
        _sim_identity_set_hash(identities)
        != payload.get("signal_identity_hash")
    ):
        return False, "sim_trade_signal_prepare: persisted signal set differs from receipt"
    expected_decisions = len(recommendation_codes) * len(strategy_keys)
    if (
        int(payload["allowed_count"])
        + int(payload["rejected_count"])
        != expected_decisions
    ):
        return False, "sim_trade_signal_prepare: decision accounting is incomplete"
    return (
        True,
        "sim_trade_signal_prepare exact pool verified: "
        f"recommendations={len(recommendation_codes)} "
        f"strategies={len(strategy_keys)} signals={len(signal_rows)}",
    )


def _validate_etf_forward_receipt(
    engine: Engine,
    *,
    output: str | None,
    now: datetime,
    release_target_date: date | None = None,
) -> tuple[bool, str]:
    payload = _etf_forward_payload(output)
    if payload is None or _etf_forward_output_status(output, return_code=0) != "success":
        return False, "etf_forward_daily: exact PASS receipt is missing or invalid"
    trade_date = str(payload["trade_date"])
    if (
        release_target_date is not None
        and trade_date != release_target_date.isoformat()
    ):
        return False, "etf_forward_daily: receipt date differs from release target"
    try:
        from tools.run_etf_forward_daily import ETF_CLOSE_READY_TIME

        authoritative = authoritative_closed_trade_date(
            engine,
            now=now,
            close_ready_time=ETF_CLOSE_READY_TIME,
        )
    except Exception as exc:
        return False, f"etf_forward_daily: closed-session calendar unavailable: {exc}"
    if not authoritative or trade_date != authoritative:
        return False, (
            "etf_forward_daily: receipt is not for the authoritative latest "
            "closed session"
        )
    market = payload["market_data"]
    expected_database = market["database"]
    try:
        from tools.sync_etf_bigqmt_daily import validate_partition_rows

        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT etf_code,trade_time,trade_date,k_type,adjust_type,
                           `open`,`close`,high,low,volume,amount,pre_close,
                           `change`,change_pct,data_source,validation_source,
                           validation_status,batch_id,data_version,
                           quality_status,permission_status
                      FROM sm_etf_kline
                     WHERE trade_date=:trade_date
                       AND k_type=1 AND adjust_type IN (0,1)
                     ORDER BY adjust_type,etf_code
                    """
                ),
                {"trade_date": trade_date},
            ).mappings().all()
        actual = validate_partition_rows(rows, trade_date=trade_date)
    except Exception as exc:  # fail closed on missing schema or malformed rows
        return False, f"etf_forward_daily: database partition invalid: {exc}"
    if actual != expected_database:
        return False, "etf_forward_daily: database partition differs from receipt"

    forward = payload["forward_ledger"]
    if trade_date != now.date().isoformat():
        if (
            forward.get("status")
            != "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED"
            or forward.get("data_date") != trade_date
        ):
            return False, (
                "etf_forward_daily: prior closed session must not append a "
                "forward observation"
            )
        return (
            True,
            "etf_forward_daily exact 28-row prior closed partition verified: "
            f"{trade_date}",
        )
    if forward.get("status") != "PASS":
        return False, (
            "etf_forward_daily: current closed session lacks its forward observation"
        )
    try:
        with engine.connect() as connection:
            observations = connection.execute(
                text(
                    """
                    SELECT strategy_version,data_date,config_hash,input_hash,
                           signal_type
                      FROM st_etf_forward_observation
                     WHERE strategy_version=:strategy_version
                       AND data_date=:data_date
                    """
                ),
                {
                    "strategy_version": forward["strategy_version"],
                    "data_date": trade_date,
                },
            ).mappings().all()
    except Exception as exc:
        return False, f"etf_forward_daily: forward ledger unavailable: {exc}"
    if len(observations) != 1:
        return False, "etf_forward_daily: exact forward observation is missing or duplicated"
    observation = observations[0]
    if (
        str(observation.get("data_date"))[:10] != trade_date
        or observation.get("config_hash") != forward.get("config_hash")
        or observation.get("input_hash") != forward.get("input_hash")
        or observation.get("signal_type") != forward.get("signal_type")
    ):
        return False, "etf_forward_daily: persisted observation differs from receipt"
    return True, f"etf_forward_daily exact 28-row partition and observation verified: {trade_date}"


def _validate_dividend_baidu_receipt(
    engine: Engine,
    *,
    output: str | None,
    now: datetime,
) -> tuple[bool, str]:
    payload = _dividend_baidu_payload(output)
    if payload is None or _dividend_baidu_output_status(output, return_code=0) != "success":
        return False, "stock_dividend_baidu: exact PASS receipt is missing or invalid"
    sync_date = str(payload["sync_date"])
    if sync_date != now.date().isoformat():
        return False, "stock_dividend_baidu: stale or future receipt date"
    try:
        from biz.stock_market.sync_dividend_baidu import (
            canonical_dividend_rows,
            code_set_hash,
            load_authoritative_universe,
        )

        universe = load_authoritative_universe(
            engine,
            as_of=sync_date,
            known_at=now,
        )
        rows: list[dict[str, Any]] = []
        with engine.connect() as connection:
            for offset in range(0, len(universe.codes), 500):
                chunk = list(universe.codes[offset : offset + 500])
                statement = text(
                    "SELECT stock_code,report_date,dividend_plan,ex_dividend_date "
                    "FROM sm_dividend WHERE stock_code IN :stock_codes "
                    "ORDER BY stock_code,report_date"
                ).bindparams(bindparam("stock_codes", expanding=True))
                rows.extend(
                    dict(row)
                    for row in connection.execute(
                        statement,
                        {"stock_codes": chunk},
                    ).mappings()
                )
        canonical = canonical_dividend_rows(rows)
        canonical_json = json.dumps(
            canonical,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        row_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    except Exception as exc:
        return False, f"stock_dividend_baidu: database scope invalid: {exc}"
    collection = payload["collection"]
    database = payload["database"]
    if (
        len(universe.codes) != int(collection.get("requested_code_count") or 0)
        or code_set_hash(universe.codes) != collection.get("requested_code_set_hash")
        or universe.code_set_hash != payload["catalog"].get("target_code_set_hash")
        or len(canonical) != int(database.get("row_count") or 0)
        or row_hash != database.get("row_hash")
        or row_hash != collection.get("row_hash")
    ):
        return False, "stock_dividend_baidu: persisted scope differs from receipt"
    return True, (
        "stock_dividend_baidu exact authoritative scope verified: "
        f"codes={len(universe.codes)} rows={len(canonical)}"
    )


def _validate_trading_v3_decision_receipt(
    engine: Engine,
    *,
    output: str | None,
    started_at: datetime,
    release_target_date: date | None = None,
) -> tuple[bool, str]:
    """Bind scheduler success/block to one exact persisted decision run."""

    payload = _trading_v3_decision_payload(output)
    if payload is None:
        return False, "trading_v3: exact decision result payload is missing"
    run_uid = str(payload.get("run_uid") or "")
    trade_date_raw = str(payload.get("trade_date") or "")
    forecast_count = payload.get("forecast_count")
    target_count = payload.get("target_count")
    if (
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or not isinstance(forecast_count, int)
        or isinstance(forecast_count, bool)
        or forecast_count < 0
        or not isinstance(target_count, int)
        or isinstance(target_count, bool)
        or target_count < 0
        or target_count > forecast_count
    ):
        return False, "trading_v3: result identity/count contract is invalid"
    if release_target_date is not None:
        expected_decision_at = datetime.combine(
            release_target_date,
            time(16, 5),
        )
        receipt_decision_at = _coerce_datetime(payload.get("decision_at"))
        if (
            trade_date_raw != release_target_date.isoformat()
            or receipt_decision_at != expected_decision_at
            or str(payload.get("mode") or "") != "close"
            or payload.get("execution_enabled") is not False
            or str(payload.get("actionable_status") or "").upper()
            != "REPLAY_ONLY"
            or int(payload.get("paper_order_count") or 0) != 0
            or int(payload.get("real_order_count") or 0) != 0
            or int(payload.get("position_state_updates") or 0) != 0
            or list(payload.get("paper_orders") or [])
        ):
            return False, (
                "trading_v3: release replay date/safety contract differs: "
                f"expected_date={release_target_date.isoformat()} "
                f"output_date={trade_date_raw}"
            )
    rows = _read_all(
        engine,
        """
        SELECT run_uid, trade_date, status, forecast_count, target_count,
               regime_json, portfolio_json, result_hash, completed_at,
               (SELECT COUNT(*) FROM st_alpha_forecast_v3
                WHERE run_uid=:run_uid) AS actual_forecast_count,
               (SELECT COUNT(*) FROM st_target_portfolio_v3
                WHERE run_uid=:run_uid) AS actual_target_count,
               (SELECT COUNT(*) FROM st_theme_signal_v3
                WHERE run_uid=:run_uid) AS actual_theme_signal_count,
               (SELECT COUNT(*) FROM st_trade_hypothesis_v3
                WHERE run_uid=:run_uid) AS actual_hypothesis_count
        FROM st_decision_run_v3
        WHERE run_uid=:run_uid
        """,
        {"run_uid": run_uid},
    )
    if len(rows) != 1:
        return False, (
            "trading_v3: exact persisted run is missing or duplicated: "
            f"run_uid={run_uid} rows={len(rows)}"
        )
    row = rows[0]
    persisted_trade_date = _coerce_date(row.get("trade_date"))
    persisted_status = str(row.get("status") or "").upper()
    persisted_count = int(row.get("forecast_count") or 0)
    persisted_target_count = int(row.get("target_count") or 0)
    actual_forecast_count = int(row.get("actual_forecast_count") or 0)
    actual_target_count = int(row.get("actual_target_count") or 0)
    actual_theme_signal_count = int(
        row.get("actual_theme_signal_count") or 0
    )
    actual_hypothesis_count = int(row.get("actual_hypothesis_count") or 0)
    completed_at = _coerce_datetime(row.get("completed_at"))
    expected_status = str(payload.get("run_status") or "").upper()
    if (
        persisted_trade_date is None
        or persisted_trade_date.isoformat() != trade_date_raw
        or persisted_status != expected_status
        or persisted_count != forecast_count
        or persisted_target_count != target_count
        or actual_forecast_count != forecast_count
        or actual_target_count != target_count
        or completed_at is None
        or completed_at < started_at - timedelta(minutes=5)
    ):
        return False, (
            "trading_v3: persisted receipt differs from process result: "
            f"run_uid={run_uid} output_date={trade_date_raw} "
            f"db_date={persisted_trade_date} output_status={expected_status} "
            f"db_status={persisted_status} output_forecasts={forecast_count} "
            f"db_forecasts={persisted_count} "
            f"actual_forecasts={actual_forecast_count} "
            f"output_targets={target_count} "
            f"db_targets={persisted_target_count} "
            f"actual_targets={actual_target_count} "
            f"completed_at={completed_at}"
        )
    if persisted_status == "COMPLETED" and persisted_count <= 0:
        return False, (
            "trading_v3: COMPLETED run has an empty forecast ledger: "
            f"run_uid={run_uid}"
        )
    if persisted_count == 0 and persisted_status != "BLOCKED":
        return False, (
            "trading_v3: empty forecast ledger is not BLOCKED: "
            f"run_uid={run_uid} status={persisted_status}"
        )
    try:
        from server.trading_v3.decision_truth import (
            DECISION_INTEGRITY_SCHEMA_VERSION,
            FORECAST_LEDGER_SQL_COLUMNS,
            canonical_forecast_ledger,
            canonical_hash,
            canonical_target_ledger,
            decision_result_hash,
        )

        def _json_object(value: Any) -> dict[str, Any]:
            if isinstance(value, Mapping):
                return dict(value)
            parsed = json.loads(str(value or "{}"))
            if not isinstance(parsed, Mapping):
                raise ValueError("persisted JSON is not an object")
            return dict(parsed)

        portfolio = _json_object(row.get("portfolio_json"))
        regime = _json_object(row.get("regime_json"))
        integrity = dict(portfolio.get("decision_integrity") or {})
        forecast_rows = _read_all(
            engine,
            f"""
            SELECT {FORECAST_LEDGER_SQL_COLUMNS}
            FROM st_alpha_forecast_v3
            WHERE run_uid=:run_uid
            ORDER BY rank_no, stock_code, strategy_key, forecast_id
            """,
            {"run_uid": run_uid},
        )
        target_rows = _read_all(
            engine,
            """
            SELECT *
            FROM st_target_portfolio_v3
            WHERE run_uid=:run_uid
            ORDER BY rank_no, stock_code
            """,
            {"run_uid": run_uid},
        )
        if persisted_trade_date is None:
            raise ValueError("persisted trade date is missing")
        target_ledger = canonical_target_ledger(
            target_rows,
            run_uid=run_uid,
            trade_date=persisted_trade_date,
            persisted=True,
        )
        forecast_ledger = canonical_forecast_ledger(forecast_rows)
        integrity_checks = (
            str(integrity.get("schema_version") or "")
            == DECISION_INTEGRITY_SCHEMA_VERSION,
            int(integrity.get("forecast_count") or 0)
            == actual_forecast_count,
            len(forecast_rows) == actual_forecast_count,
            canonical_hash(forecast_ledger)
            == str(integrity.get("forecast_ledger_hash") or ""),
            int(integrity.get("target_count") or 0)
            == actual_target_count,
            int(integrity.get("persisted_theme_signal_count") or 0)
            == actual_theme_signal_count,
            int(integrity.get("hypothesis_count") or 0)
            == actual_hypothesis_count,
            canonical_hash(target_ledger)
            == str(integrity.get("target_ledger_hash") or ""),
            decision_result_hash(
                regime=regime,
                portfolio=portfolio,
                forecast_count=actual_forecast_count,
                theme_signal_count=int(
                    integrity.get("raw_theme_signal_count") or 0
                ),
                hypothesis_count=actual_hypothesis_count,
            )
            == str(row.get("result_hash") or ""),
        )
    except (
        ArithmeticError,
        AttributeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        return False, (
            "trading_v3: persisted decision integrity is unreadable: "
            f"run_uid={run_uid} error={exc}"
        )
    if not all(integrity_checks):
        return False, (
            "trading_v3: persisted decision integrity or forecast/target ledger "
            f"differs from receipt: run_uid={run_uid}"
        )
    return True, (
        "trading_v3 exact decision receipt verified: "
        f"run_uid={run_uid} trade_date={trade_date_raw} "
        f"forecast_count={persisted_count} "
        f"target_count={persisted_target_count} status={persisted_status}"
    )


def _validate_daily_universe_coverage(
    engine: Engine,
    *,
    task_type: str,
    target_date: date,
    decision_known_at: datetime,
) -> tuple[bool, str]:
    """Reconcile derived daily outputs with one immutable catalog manifest."""

    target = target_date.isoformat()
    universe = load_daily_stock_universe(
        engine,
        target,
        decision_known_at=decision_known_at,
    )
    kline_rows = _read_all(
        engine,
        """
        SELECT stock_code, volume, amount
        FROM sm_stock_kline
        WHERE trade_date=:target_date AND k_type=1 AND adjust_type=0
        ORDER BY stock_code
        """,
        {"target_date": target},
    )
    if task_type == "market_overview_daily":
        audit = validate_daily_stock_coverage(
            universe,
            kline_rows=kline_rows,
        )
        overview = _read_one(
            engine,
            """
            SELECT total
            FROM sm_market_overview_daily
            WHERE trade_date=:target_date
            """,
            {"target_date": target},
        )
        total = int(overview.get("total") or 0)
        if total != universe.expected_count:
            return (
                False,
                "sm_market_overview_daily catalog coverage differs: "
                f"date={target} total={total} expected={universe.expected_count} "
                f"catalog_hash={universe.expected_code_set_hash}",
            )
        return (
            True,
            "sm_market_overview_daily catalog coverage verified: "
            f"date={target} count={audit['kline_count']} "
            f"catalog_hash={universe.expected_code_set_hash}",
        )

    if task_type == _CAPITAL_FLOW_BATCH_TASK_TYPE:
        # The exact supported-market flow partition was already verified by
        # _validate_capital_flow_persisted_receipt. Keep the independent daily
        # K/catalog check, without demanding unsupported BSE flow here.
        audit = validate_daily_stock_coverage(universe, kline_rows=kline_rows)
        return True, (
            "capital_flow_batch_fast daily K/catalog verified; "
            "capital-flow scope=SH/SZ supported traded codes; "
            f"date={target} kline={audit['kline_count']} "
            f"catalog_hash={universe.expected_code_set_hash}"
        )

    flow_rows = _read_all(
        engine,
        """
        SELECT stock_code
        FROM sm_stock_capital_flow_daily
        WHERE trade_date=:target_date
        ORDER BY stock_code
        """,
        {"target_date": target},
    )
    audit = validate_daily_stock_coverage(
        universe,
        kline_rows=kline_rows,
        flow_rows=flow_rows,
    )
    if task_type in {
        "capital_flow_batch_fast",
        "analysis_fast",
        "analysis_morning_strict",
        "analysis_premarket_external",
    }:
        return (
            True,
            f"{task_type} exact capital-flow/catalog coverage verified: "
            f"date={target} expected={audit['expected_count']} "
            f"traded={audit['traded_count']} flow={audit['flow_count']} "
            f"catalog_hash={universe.expected_code_set_hash}",
        )
    snapshot_rows = _read_all(
        engine,
        """
        SELECT stock_code, volume, amount
        FROM sm_stock_snapshot
        WHERE trade_date=:target_date
        ORDER BY stock_code
        """,
        {"target_date": target},
    )
    validate_daily_stock_coverage(universe, kline_rows=snapshot_rows)
    return (
        True,
        "sm_stock_snapshot source/catalog coverage verified: "
        f"date={target} expected={audit['expected_count']} "
        f"traded={audit['traded_count']} suspended={audit['suspended_count']} "
        f"flow={audit['flow_count']} catalog_hash={universe.expected_code_set_hash}",
    )


def _minimum_expected_finance_report_date(as_of: date) -> date:
    return finance_disclosure_gate(as_of).minimum_report_date


def _finance_empty_source_receipt_valid(
    value: Any,
    *,
    stock_code: str,
    source: str,
    known_at: datetime,
) -> bool:
    if not isinstance(value, Mapping):
        return False
    sweeps = value.get("sweeps")
    try:
        sweep_count = int(value.get("stable_sweep_count"))
    except (TypeError, ValueError, OverflowError):
        return False
    stable_hash = str(value.get("stable_content_sha256") or "")
    expected_stable_hash = canonical_hash({
        "schema": "probiga.eastmoney-finance-issuer-response.v1",
        "stock_code": stock_code,
        "rows": [],
    })
    captured_at = _coerce_datetime(value.get("captured_at"))
    if (
        value.get("schema")
        != "probiga.eastmoney-finance-issuer-capture.v1"
        or str(value.get("source") or "") != source
        or value.get("endpoint")
        != "https://datacenter.eastmoney.com/securities/api/data/get"
        or str(value.get("stock_code") or "").zfill(6) != stock_code
        or value.get("stability_status") != "STABLE_DOUBLE_SWEEP"
        or sweep_count != 2
        or stable_hash != expected_stable_hash
        or captured_at is None
        or captured_at > known_at
        or not isinstance(sweeps, list)
        or len(sweeps) != sweep_count
    ):
        return False
    for sweep_no, sweep in enumerate(sweeps, start=1):
        if not isinstance(sweep, Mapping):
            return False
        try:
            page_count = int(sweep.get("page_count"))
            total_count = int(sweep.get("total_count"))
            row_count = int(sweep.get("row_count"))
        except (TypeError, ValueError, OverflowError):
            return False
        raw_hashes = sweep.get("page_raw_sha256")
        content_hashes = sweep.get("page_content_sha256")
        page_row_counts = sweep.get("page_row_counts")
        if (
            int(sweep.get("sweep_no") or 0) != sweep_no
            or page_count < 1
            or total_count != 0
            or row_count != 0
            or str(sweep.get("content_sha256") or "") != stable_hash
            or not isinstance(raw_hashes, list)
            or not isinstance(content_hashes, list)
            or not isinstance(page_row_counts, list)
            or len(raw_hashes) != page_count
            or len(content_hashes) != page_count
            or len(page_row_counts) != page_count
            or any(not _is_hex(item, 64) for item in raw_hashes)
            or any(not _is_hex(item, 64) for item in content_hashes)
            or any(type(item) is not int or item != 0 for item in page_row_counts)
        ):
            return False
    return True


def _finance_catalog_bound_legal_empty_resolutions(
    engine: Engine,
    *,
    expected: Mapping[str, date | None],
    codes: set[str],
    target: date,
    gate: Any,
    known_after: datetime,
    now: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Resolve only a fresh, catalog-bound statutory no-data disposition."""

    if not codes:
        return {}, {}
    rows = _read_all(
        engine,
        """
        SELECT coverage_id, stock_code, window_start, window_end,
               known_at, received_at, covered_through_at, watermark_kind,
               watermark_hash, coverage_status, result_count,
               source_response_hash, fact_set_hash, revision_no, source,
               batch_id, payload_json
        FROM st_pit_source_coverage
        WHERE fact_kind='finance'
          AND source IN (
              'adata.finance.core_index',
              'eastmoney.finance.mainfinadata.direct'
          )
          AND coverage_status='COMPLETE'
          AND result_count=0
          AND known_at >= :known_after
          AND known_at <= :now
        ORDER BY stock_code, known_at DESC, revision_no DESC, coverage_id DESC
        """,
        {"known_after": known_after, "now": now},
    )
    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    catalog_cache: dict[str, tuple[Any, set[str]]] = {}
    seen: set[str] = set()
    empty_source_hash = canonical_hash({
        "schema": "probiga.pit-finance-source-response.v1",
        "rows": [],
    })
    empty_fact_hash = canonical_hash({
        "schema": "probiga.pit-finance-fact-set.v1",
        "bindings": [],
    })
    for row in rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if code not in codes or code in seen:
            continue
        seen.add(code)
        reason = "FINANCE_LEGAL_EMPTY_RECEIPT_INVALID"
        try:
            payload = json.loads(str(row.get("payload_json") or ""))
            if not isinstance(payload, Mapping):
                raise ValueError(reason)
            source = str(row.get("source") or "")
            known_at = _coerce_datetime(row.get("known_at"))
            received_at = _coerce_datetime(row.get("received_at"))
            covered_at = _coerce_datetime(row.get("covered_through_at"))
            watermark = payload.get("watermark")
            evidence = (
                watermark.get("evidence")
                if isinstance(watermark, Mapping)
                else None
            )
            source_receipt = (
                evidence.get("source_receipt")
                if isinstance(evidence, Mapping)
                else None
            )
            timestamp_guard = (
                evidence.get("source_timestamp_guard")
                if isinstance(evidence, Mapping)
                else None
            )
            source_response_hash = str(
                row.get("source_response_hash") or ""
            )
            fact_set_hash = str(row.get("fact_set_hash") or "")
            if (
                source not in _FINANCE_AUTHORITATIVE_SOURCES
                or row.get("coverage_status") != "COMPLETE"
                or int(row.get("result_count")) != 0
                or _coerce_date(row.get("window_start")) != date(1900, 1, 1)
                or _coerce_date(row.get("window_end")) != target
                or known_at is None
                or received_at is None
                or covered_at is None
                or not known_after <= known_at <= now
                or received_at > known_at
                or covered_at != known_at
                or row.get("watermark_kind") != "CAPTURED_AT"
                or not _is_hex(row.get("coverage_id"), 64)
                or source_response_hash != empty_source_hash
                or fact_set_hash != empty_fact_hash
                or payload.get("schema")
                != "probiga.pit-source-coverage-payload.v1"
                or payload.get("fact_kind") != "finance"
                or str(payload.get("stock_code") or "").zfill(6) != code
                or _coerce_date(payload.get("window_start"))
                != date(1900, 1, 1)
                or _coerce_date(payload.get("window_end")) != target
                or _coerce_datetime(payload.get("known_at")) != known_at
                or _coerce_datetime(payload.get("received_at")) != received_at
                or _coerce_datetime(payload.get("covered_through_at"))
                != covered_at
                or int(payload.get("result_count")) != 0
                or payload.get("source_rows") != []
                or payload.get("fact_bindings") != []
                or payload.get("source_response_hash") != source_response_hash
                or payload.get("fact_set_hash") != fact_set_hash
                or not isinstance(watermark, Mapping)
                or watermark.get("schema")
                != "probiga.pit-source-watermark.v1"
                or watermark.get("kind") != "CAPTURED_AT"
                or _coerce_datetime(watermark.get("covered_through_at"))
                != covered_at
                or watermark.get("source_response_hash")
                != source_response_hash
                or canonical_hash(dict(watermark))
                != str(row.get("watermark_hash") or "")
                or not isinstance(evidence, Mapping)
                or evidence.get("provider") != source
                or evidence.get("capture")
                != "stable_eastmoney_result_set"
                or evidence.get("resolution_type")
                != _FINANCE_LEGAL_EMPTY_RESOLUTION_TYPE
                or evidence.get("reason_code")
                != _FINANCE_LEGAL_EMPTY_REASON
                or str(evidence.get("stock_code") or "").zfill(6) != code
                or _coerce_date(evidence.get("listing_date"))
                != expected.get(code)
                or _coerce_date(evidence.get("disclosure_deadline"))
                != gate.disclosure_deadline
                or _coerce_date(evidence.get("as_of_date")) != target
                or not isinstance(timestamp_guard, Mapping)
                or timestamp_guard.get("status") != "PASS"
                or _coerce_date(timestamp_guard.get("as_of_date")) != target
                or _coerce_datetime(timestamp_guard.get("captured_at"))
                != known_at
                or timestamp_guard.get("maximum_notice_date") is not None
                or timestamp_guard.get("maximum_update_date") is not None
                or not _finance_empty_source_receipt_valid(
                    source_receipt,
                    stock_code=code,
                    source=source,
                    known_at=known_at,
                )
            ):
                raise ValueError(reason)
            listing_date = expected.get(code)
            if (
                listing_date is None
                or listing_date <= gate.disclosure_deadline
                or listing_date > target
            ):
                raise ValueError(reason)
            catalog_batch_id = str(evidence.get("catalog_batch_id") or "")
            if catalog_batch_id not in catalog_cache:
                catalog, eligible_codes = load_target_stock_catalog(
                    engine,
                    target_date=target.isoformat(),
                    decision_known_at=now,
                    batch_id=catalog_batch_id,
                )
                catalog_cache[catalog_batch_id] = (
                    catalog,
                    set(eligible_codes),
                )
            catalog, eligible_codes = catalog_cache[catalog_batch_id]
            catalog_members = [
                item
                for item in catalog.members
                if str(item.get("stock_code") or "").zfill(6) == code
            ]
            if (
                catalog.batch_id != catalog_batch_id
                or catalog.manifest_hash
                != evidence.get("catalog_manifest_hash")
                or catalog.member_set_hash
                != evidence.get("catalog_member_set_hash")
                or catalog.member_count
                != int(evidence.get("catalog_member_count") or 0)
                or code not in eligible_codes
                or len(catalog_members) != 1
                or _coerce_date(catalog_members[0].get("list_date"))
                != listing_date
            ):
                raise ValueError(reason)
        except Exception as exc:
            invalid[code] = f"{reason}:{type(exc).__name__}"
            continue
        available[code] = {
            "coverage_id": str(row.get("coverage_id")),
            "source": source,
            "resolution_type": _FINANCE_LEGAL_EMPTY_RESOLUTION_TYPE,
            "reason_code": _FINANCE_LEGAL_EMPTY_REASON,
            "catalog_batch_id": catalog_batch_id,
            "known_at": known_at.isoformat(),
        }
    return available, invalid


def _validate_finance_scheduler_coverage(
    engine: Engine,
    *,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    """Require a non-empty fresh PIT receipt and a current period per stock."""

    expected_rows = _read_all(
        engine,
        """
        SELECT stock_code, list_date
        FROM si_all_code
        WHERE stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$'
        ORDER BY stock_code
        """,
    )
    expected = {
        str(row.get("stock_code") or "").strip().zfill(6):
            coerce_optional_date(row.get("list_date"))
        for row in expected_rows
        if str(row.get("stock_code") or "").strip()
    }
    if not expected:
        return False, "stock_finance: authoritative stock universe is empty"
    fresh_after = started_at - timedelta(minutes=5)
    receipt_rows = _read_all(
        engine,
        """
        SELECT stock_code, MAX(known_at) AS latest_known_at,
               MAX(result_count) AS max_result_count
        FROM st_pit_source_coverage
        WHERE fact_kind='finance'
          AND source IN (
              'adata.finance.core_index',
              'eastmoney.finance.mainfinadata.direct'
          )
          AND coverage_status='COMPLETE'
          AND result_count > 0
          AND known_at >= :fresh_after
        GROUP BY stock_code
        """,
        {"fresh_after": fresh_after},
    )
    receipt_codes = {
        str(row.get("stock_code") or "").strip().zfill(6)
        for row in receipt_rows
    }
    expected_codes = set(expected)
    fresh_after = started_at - timedelta(minutes=5)
    try:
        atomic_seal = load_finance_atomic_batch_seal(
            engine,
            codes=sorted(expected_codes),
            decision_at=now,
            as_of_date=now.date(),
        )
    except Exception:
        atomic_seal = {}
    seal_completed_at = _coerce_datetime(
        atomic_seal.get("completed_known_at") if atomic_seal else None
    )
    if (
        atomic_seal
        and seal_completed_at is not None
        and fresh_after <= seal_completed_at <= now
        and int(atomic_seal.get("eligible_code_count") or 0) == len(expected_codes)
    ):
        return (
            True,
            "stock_finance existing full-market PIT seal verified: "
            f"codes={len(expected_codes)} "
            f"expected_unavailable={int(atomic_seal.get('expected_unavailable_count') or 0)} "
            f"coverage_root={atomic_seal.get('coverage_root_sha256')}",
        )
    gate = finance_disclosure_gate(now.date())
    minimum = gate.minimum_report_date
    initially_missing = expected_codes - receipt_codes
    legal_empty, invalid_legal_empty = (
        _finance_catalog_bound_legal_empty_resolutions(
            engine,
            expected=expected,
            codes=(
                initially_missing - _FINANCE_EXPECTED_UNAVAILABLE_CODES
            ),
            target=now.date(),
            gate=gate,
            known_after=fresh_after,
            now=now,
        )
    )
    if invalid_legal_empty:
        return (
            False,
            "stock_finance legal-empty resolution is invalid: "
            f"{invalid_legal_empty}",
        )
    legal_empty_codes = set(legal_empty)
    unsupported_missing = sorted(
        initially_missing
        - _FINANCE_EXPECTED_UNAVAILABLE_CODES
        - legal_empty_codes
    )
    unavailable: dict[str, dict[str, Any]] = {}
    invalid_unavailable: dict[str, str] = {}
    if initially_missing & _FINANCE_EXPECTED_UNAVAILABLE_CODES:
        try:
            unavailable, invalid_unavailable = load_finance_expected_unavailable(
                engine,
                codes=sorted(
                    initially_missing & _FINANCE_EXPECTED_UNAVAILABLE_CODES
                ),
                decision_at=now,
                expected_report_date=minimum,
                known_after=fresh_after,
            )
        except Exception as exc:
            return (
                False,
                "stock_finance expected-unavailable validation failed: "
                f"{exc}",
            )
    if invalid_unavailable:
        return (
            False,
            "stock_finance expected-unavailable receipt is invalid: "
            f"{invalid_unavailable}",
        )
    unavailable_codes = set(unavailable)
    resolved_codes = receipt_codes | unavailable_codes | legal_empty_codes
    missing_receipts = sorted(expected_codes - resolved_codes)
    unexpected_receipts = sorted(receipt_codes - expected_codes)
    if unsupported_missing or missing_receipts or unexpected_receipts:
        return (
            False,
            "stock_finance fresh PIT resolution coverage differs: "
            f"expected={len(expected)} actual={len(resolved_codes)} "
            f"complete={len(receipt_codes)} "
            f"expected_unavailable={len(unavailable_codes)} "
            f"legal_empty={len(legal_empty_codes)} "
            f"coverage={len(expected_codes & resolved_codes) / len(expected):.6f} "
            f"missing_sample={missing_receipts[:20]} "
            f"unexpected_sample={unexpected_receipts[:20]}",
        )

    latest_rows = _read_all(
        engine,
        """
        SELECT code.stock_code, code.list_date,
               MAX(finance.report_date) AS latest_report_date
        FROM si_all_code AS code
        LEFT JOIN si_stock_finance AS finance
          ON finance.stock_code=code.stock_code
        WHERE code.stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$'
        GROUP BY code.stock_code
        ORDER BY code.stock_code
        """,
    )
    stale: list[tuple[str, str]] = []
    observed: set[str] = set()
    exempt_count = 0
    for row in latest_rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if code not in expected_codes:
            continue
        observed.add(code)
        if code in unavailable_codes:
            continue
        latest = _coerce_date(row.get("latest_report_date"))
        listing_date = expected[code]
        applies = report_period_gate_applies(listing_date, gate)
        if not applies:
            exempt_count += 1
        if applies and (latest is None or latest < minimum):
            stale.append((code, latest.isoformat() if latest else "NULL"))
    for code in sorted(expected_codes - observed):
        stale.append((code, "NULL"))
    if stale:
        return (
            False,
            "stock_finance latest report period is incomplete: "
            f"minimum={minimum.isoformat()} stale_count={len(stale)} "
            f"sample={stale[:20]}",
        )
    return (
        True,
        "stock_finance full-market PIT receipts verified: "
        f"codes={len(expected)} complete={len(receipt_codes)} "
        f"expected_unavailable={len(unavailable_codes)} "
        f"legal_empty={len(legal_empty_codes)} coverage=1.000000 "
        f"minimum_report_date={minimum.isoformat()} "
        f"new_listing_period_exempt={exempt_count}",
    )


def _validate_requirement(
    engine: Engine,
    requirement: TableRequirement,
    *,
    started_at: datetime,
    now: datetime,
    output: str | None = None,
    target_date_override: date | None = None,
) -> tuple[bool, str]:
    columns = _table_columns(engine, requirement.table)
    if not columns:
        return False, f"{requirement.table}: target table does not exist"

    target_date = target_date_override or _resolve_target_date(
        engine,
        requirement,
        started_at=started_at,
        now=now,
        output=output,
    )
    where_parts: list[str] = []
    params: dict[str, Any] = {}
    if requirement.where_sql:
        where_parts.append(f"({requirement.where_sql})")
    if requirement.date_col:
        if requirement.date_col not in columns:
            return False, f"{requirement.table}: date column {requirement.date_col} does not exist"
        start_date = target_date.isoformat()
        end_date = (target_date + timedelta(days=1)).isoformat()
        where_parts.append(f"{quote_identifier(requirement.date_col)} >= :target_start")
        where_parts.append(f"{quote_identifier(requirement.date_col)} < :target_end")
        params.update({"target_start": start_date, "target_end": end_date})

    select_parts = ["COUNT(*) AS row_count"]
    if requirement.distinct_col:
        if requirement.distinct_col not in columns:
            return False, f"{requirement.table}: distinct column {requirement.distinct_col} does not exist"
        select_parts.append(f"COUNT(DISTINCT {quote_identifier(requirement.distinct_col)}) AS distinct_count")
    freshness_col = requirement.freshness_col if requirement.freshness_col in columns else None
    if freshness_col:
        select_parts.append(f"MAX({quote_identifier(freshness_col)}) AS max_freshness")
    if requirement.date_col:
        select_parts.append(f"MAX({quote_identifier(requirement.date_col)}) AS max_data_time")

    where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    row = _read_one(
        engine,
        f"SELECT {', '.join(select_parts)} FROM {quote_identifier(requirement.table)}{where_sql}",
        params,
    )
    row_count = int(row.get("row_count") or 0)
    if row_count < requirement.min_rows:
        date_note = f" for {target_date.isoformat()}" if requirement.date_col else ""
        return (
            False,
            f"{requirement.table}{date_note}: only {row_count} rows, expected >= {requirement.min_rows}",
        )

    distinct_count = int(row.get("distinct_count") or 0)
    if requirement.distinct_col and distinct_count < requirement.min_distinct:
        date_note = f" for {target_date.isoformat()}" if requirement.date_col else ""
        return (
            False,
            f"{requirement.table}{date_note}: only {distinct_count} distinct {requirement.distinct_col}, "
            f"expected >= {requirement.min_distinct}",
        )

    if requirement.require_fresh and freshness_col:
        max_freshness = _coerce_datetime(row.get("max_freshness"))
        fresh_after = started_at - timedelta(minutes=5)
        if not max_freshness or max_freshness < fresh_after:
            return (
                False,
                f"{requirement.table}: data exists but was not refreshed by this run "
                f"(max {freshness_col}={row.get('max_freshness')})",
            )

    target_note = f" date={target_date.isoformat()}" if requirement.date_col else ""
    distinct_note = f" distinct_{requirement.distinct_col}={distinct_count}" if requirement.distinct_col else ""
    return True, f"{requirement.table}{target_note} rows={row_count}{distinct_note}"


def _release_target_date_from_task(task: Mapping[str, Any]) -> date | None:
    """Read the scheduler-bound release target; reject unaudited injection."""

    raw = str(task.get("_release_target_date") or "").strip()
    if not raw:
        return None
    if str(task.get("_trigger_source") or "").strip() != "release_catchup":
        raise ValueError("release validation target has no catch-up authority")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("release validation target date is invalid") from exc
    if parsed.isoformat() != raw:
        raise ValueError("release validation target date is invalid")
    return parsed


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    # The information_schema query does not contain the target table in its
    # FROM clause, so route explicitly before inspecting external tables.
    metadata_engine = routed_read_engine(
        f"SELECT * FROM {quote_identifier(table_name)}",
        engine,
    )
    rows = _read_all(
        metadata_engine,
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        """,
        {"table_name": table_name},
    )
    return {str(row.get("COLUMN_NAME")) for row in rows}


def _read_one(engine: Engine, sql: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    rows = _read_all(engine, sql, params)
    return rows[0] if rows else {}


def _read_all(engine: Engine, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    read_engine = routed_read_engine(sql, engine)
    with read_engine.connect() as conn:
        result = conn.execute(text(sql), dict(params or {}))
        return [dict(row) for row in result.mappings().all()]


def _resolve_target_date(
    engine: Engine,
    requirement: TableRequirement,
    *,
    started_at: datetime,
    now: datetime,
    output: str | None = None,
) -> date:
    if requirement.target == "output_date":
        output_date = _extract_output_date(output)
        if output_date is None:
            raise ValueError(
                f"{requirement.table}: task output is missing one unambiguous DATE=YYYY-MM-DD marker"
            )
        return output_date
    if requirement.target == "latest_trade_date":
        return _latest_trade_date(engine, now=now, ready_time=requirement.ready_time) or started_at.date()
    if requirement.target == "previous_trade_date":
        return _previous_trade_date(engine, ref_date=started_at.date()) or started_at.date()
    if requirement.target == "latest_kline_date":
        return _latest_kline_date(engine) or _latest_trade_date(engine, now=now, ready_time=requirement.ready_time) or started_at.date()
    return started_at.date()


def _extract_output_date(output: str | None) -> date | None:
    """Return the one exact data-date marker emitted by a provider task."""

    candidates = {
        match.group(1)
        for line in str(output or "").splitlines()
        if (match := re.fullmatch(r"\s*DATE=(\d{4}-\d{2}-\d{2})\s*", line))
    }
    if len(candidates) != 1:
        return None
    candidate = next(iter(candidates))
    try:
        parsed = datetime.strptime(candidate, "%Y-%m-%d").date()
    except ValueError:
        return None
    return parsed if parsed.isoformat() == candidate else None


def _latest_trade_date(engine: Engine, *, now: datetime, ready_time: str) -> date | None:
    comparator = "<=" if _time_reached(now, ready_time) else "<"
    row = _read_one(
        engine,
        f"""
        SELECT MAX(trade_date) AS trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date {comparator} :today
        """,
        {"today": now.date().isoformat()},
    )
    return _coerce_date(row.get("trade_date"))


def _previous_trade_date(engine: Engine, *, ref_date: date) -> date | None:
    row = _read_one(
        engine,
        """
        SELECT MAX(trade_date) AS trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date < :ref_date
        """,
        {"ref_date": ref_date.isoformat()},
    )
    return _coerce_date(row.get("trade_date")) or _latest_kline_date(engine)


def _latest_kline_date(engine: Engine) -> date | None:
    row = _read_one(
        engine,
        """
        SELECT MAX(trade_date) AS trade_date
        FROM sm_stock_kline
        WHERE k_type = 1
        """,
    )
    return _coerce_date(row.get("trade_date"))


def _time_reached(now: datetime, hhmm: str) -> bool:
    try:
        hour, minute = str(hhmm or "00:00").split(":", 1)
        return now.hour * 60 + now.minute >= int(hour) * 60 + int(minute)
    except Exception:
        return True


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = _coerce_datetime(value)
    return parsed.date() if parsed else None


def _coerce_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text_value = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value[: len(fmt)], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None
