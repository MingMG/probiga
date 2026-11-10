"""Fail-closed full-market announcement point-in-time ingestion.

QMT ``announcement`` remains the primary source.  An explicitly identified
fallback provider may be used only after the primary source returned a frozen
unavailability code.  Regardless of provider, a run is published only after
every member of one immutable QMT stock-catalog batch has been downloaded and
read at the same request cutoff.  Per-code checkpoints are staging data only;
one database transaction makes the completed batch visible.

This module is deliberately DML-only.  Deployment owns all PIT and catalog
DDL/triggers; a runtime run refuses an absent or drifted schema.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from itertools import groupby, islice
import json
import logging
import math
import os
from pathlib import Path
import re
import socket
import tempfile
import threading
from time import monotonic
from types import MappingProxyType
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from integrations.qmt.runtime import connect_xtdata


SHANGHAI = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)
QMT_ANNOUNCEMENT_SOURCE = "qmt.announcement"
CNINFO_ANNOUNCEMENT_SOURCE = "cninfo.announcement"
EASTMONEY_ANNOUNCEMENT_SOURCE = "eastmoney.notice"
AUTHORITATIVE_ANNOUNCEMENT_SOURCES = (
    QMT_ANNOUNCEMENT_SOURCE,
    CNINFO_ANNOUNCEMENT_SOURCE,
    EASTMONEY_ANNOUNCEMENT_SOURCE,
)
ANNOUNCEMENT_FALLBACK_REASON_CODES = frozenset({
    "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
    "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
    "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_API_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT",
})
_QMT_ANNOUNCEMENT_PERMISSION_MARKERS = (
    "no_permission",
    "no permission",
    "announcement permission denied",
    "announcement has no permission",
    "公告无权限",
    "没有公告权限",
)
QMT_ANNOUNCEMENT_PERIOD = "announcement"
QMT_ANNOUNCEMENT_BATCH_SCHEMA = "probiga.qmt-announcement-batch.v1"
QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA = (
    "probiga.qmt-announcement-historical-reconstruction-batch.v2"
)
QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA = (
    "probiga.qmt-announcement-historical-reconstruction.v2"
)
QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V2_SCHEMA = (
    "probiga.qmt-announcement-reconstruction-authority.v2"
)
QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA = (
    "probiga.qmt-announcement-reconstruction-authority.v3"
)
QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE = (
    "QMT_DAILY_TRUTH_STOCK_CATALOG_RECONCILIATION"
)
QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN = "SCHEDULER_HISTORY_RECOVERY"
QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN = "MANUAL_RESEARCH_RECOVERY"
QMT_ANNOUNCEMENT_RECONSTRUCTION_ORIGINS = frozenset({
    QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
    QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN,
})
QMT_ANNOUNCEMENT_CHECKPOINT_SCHEMA = (
    "probiga.qmt-announcement-checkpoint.v2"
)
QMT_ANNOUNCEMENT_RESULT_SCHEMA = "probiga.qmt-announcement-code-result.v2"
QMT_ANNOUNCEMENT_PREPARED_SCHEMA = "probiga.qmt-announcement-prepared.v1"
QMT_ANNOUNCEMENT_TASK_SCHEMA = "probiga.qmt-announcement-task-result.v1"
CNINFO_PROVIDER_RECEIPT_SCHEMA = (
    "probiga.cninfo-announcement-provider-receipt.v3"
)
HISTORICAL_SOURCE_EVENT_CONTRACT = "OFFICIAL_ANNOUNCEMENT_ID_PUBLISHED_AT_RAW_SOURCE_ROW_SHA256_V1"
CNINFO_MAX_PAGES_PER_STOCK = 200
CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS = 8
CNINFO_DATE_SHARD_SPLIT_VERSION = "MIDPOINT_INCLUSIVE_V1"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_OVERLAP_DAYS = 3
DEFAULT_BATCH_SIZE = 100
# The provider is queried per issuer. Keep a full-market capture resumable and
# reserve a separate window for the final anchor, validation and atomic write.
HISTORICAL_RECONSTRUCTION_BUDGET = timedelta(hours=2)
HISTORICAL_RECONSTRUCTION_PUBLISH_RESERVE = timedelta(minutes=15)
HISTORICAL_CAPTURE_DEADLINE = "QMT_ANNOUNCEMENT_RECONSTRUCTION_CAPTURE_DEADLINE"
HISTORICAL_PUBLISH_DEADLINE = "QMT_ANNOUNCEMENT_RECONSTRUCTION_PUBLISH_DEADLINE"
PUBLISH_COMMIT_UNKNOWN = "QMT_ANNOUNCEMENT_PUBLISH_COMMIT_UNKNOWN"
VALIDATION_READ_BATCH_SIZE = 40
VALIDATION_READ_PAYLOAD_BYTES = 4 * 1024 * 1024
VALIDATION_INVENTORY_CODE_LIMIT = 400
LIVE_ANNOUNCEMENT_BATCH_QUERY_TIMEOUT_MS = 60_000
LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS = 15 * 60
LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE = "QMT_ANNOUNCEMENT_LIVE_BATCH_VALIDATION_DEADLINE"
LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE = "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE"
LIVE_ANNOUNCEMENT_BATCH_REPLY_FAILED = "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED"
_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}$")
_QMT_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}\.(?:SH|SZ|BJ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXACT_COMPACT_RE = re.compile(r"^\d{14}(?:\.\d{1,6})?$")
_EXACT_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?$"
)
_PUBLICATION_FIELDS = (
    "publish_time", "publishTime", "published_at", "publishedAt",
    "announcement_time", "announcementTime", "display_time", "pub_time",
    "pubTime", "datetime", "time",
)
_TITLE_FIELDS = (
    "title", "announcement_title", "announcementTitle", "name", "subject",
    "主题", "标题",
)
# Some full-QMT builds accept ``period="announcement"`` but return their
# ordinary daily-bar frame instead.  This exact native schema was observed
# through the signed BigQMT announcement transport; it proves that the
# announcement API is unavailable, not that an announcement has a bad time.
# Keep the fingerprint exact so malformed announcement rows do not authorize
# switching providers.
_QMT_NATIVE_DAILY_BAR_FIELDS = frozenset({
    "time", "stime", "open", "high", "low", "close", "volume", "amount",
    "settelementPrice", "openInterest", "preClose", "suspendFlag",
})
_EVENT_ID_FIELDS = (
    "announcement_id", "announcementId", "art_code", "artCode", "id",
    "info_id", "infoId", "url", "detail_url",
)
_AUDIT_FIELDS = frozenset(
    _PUBLICATION_FIELDS
    + _TITLE_FIELDS
    + _EVENT_ID_FIELDS
    + (
        "stock_code", "stockCode", "code", "instrument", "category",
        "type", "source", "url", "detail_url", "证券", "主题", "标题",
        "摘要", "格式", "内容", "级别", "类型 0-其他 1-财报类",
        "provider", "provider_data_version", "source_row_hash",
    )
)


class QMTAnnouncementBlocked(RuntimeError):
    """A source/schema/completeness condition that must fail closed."""

    def __init__(self, reason_code: str, detail: str = "") -> None:
        self.reason_code = str(reason_code or "QMT_ANNOUNCEMENT_DATA_BLOCKED")
        self.detail = str(detail or "")[:1000]
        message = self.reason_code
        if self.detail:
            message += f":{self.detail}"
        super().__init__(message)


@dataclass(frozen=True)
class HistoricalAnnouncementSelection:
    """Validated historical batch rows and their scope-head status at the decision time."""

    engine: Engine
    decision_at: datetime
    window_start: date
    window_end: date
    batch: Mapping[str, Any]
    members: Mapping[str, tuple[Mapping[str, Any], bool]]


@dataclass(frozen=True)
class AnnouncementCatalog:
    batch_id: str
    manifest_hash: str
    member_set_hash: str
    codes: tuple[str, ...]
    qmt_by_code: Mapping[str, str]


@dataclass(frozen=True)
class HistoricalReconstructionContext:
    """Immutable authority and execution identity for one post-hoc rebuild."""

    target_trade_date: date
    source_query_cutoff_at: datetime
    reconstruction_started_at: datetime
    scheduler_run_uid: str
    build_sha: str
    authority: Mapping[str, Any]
    execution_origin: str = QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN


def _validate_reconstruction_context(
    value: HistoricalReconstructionContext,
    *,
    catalog: AnnouncementCatalog,
) -> dict[str, Any]:
    target = value.target_trade_date
    cutoff = _dt(value.source_query_cutoff_at)
    started = _dt(value.reconstruction_started_at)
    run_uid = str(value.scheduler_run_uid or "").strip().lower()
    build_sha = str(value.build_sha or "").strip().lower()
    execution_origin = str(value.execution_origin or "").strip().upper()
    authority = _json_safe(dict(value.authority or {}))
    if (
        cutoff.date() != target
        or cutoff.time() != datetime.max.time()
        or started <= cutoff
        or re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or build_sha == "0" * 40
        or execution_origin not in QMT_ANNOUNCEMENT_RECONSTRUCTION_ORIGINS
        or not isinstance(authority, dict)
        or authority.get("schema") not in {
            QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V2_SCHEMA,
            QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA,
        }
        or authority.get("target_trade_date") != target.isoformat()
        or authority.get("catalog_batch_id") != catalog.batch_id
        or authority.get("catalog_manifest_hash") != catalog.manifest_hash
        or authority.get("catalog_member_set_hash") != catalog.member_set_hash
        or authority.get("catalog_member_count") != len(catalog.codes)
        or authority.get("catalog_codes_sha256")
        != canonical_hash(list(catalog.codes))
        or not _reconstruction_execution_origin_matches(
            authority,
            execution_origin=execution_origin,
        )
        or not _reconstruction_authority_matches(
            authority,
            target_trade_date=target,
            catalog=catalog,
        )
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_INVALID"
        )
    return {
        "target_trade_date": target.isoformat(),
        "source_query_cutoff_at": _dt_text(cutoff),
        "reconstruction_started_at": _dt_text(started),
        "scheduler_run_uid": run_uid,
        "build_sha": build_sha,
        "execution_origin": execution_origin,
        "authority": authority,
    }


def _shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(SHANGHAI).replace(tzinfo=None)


def _shanghai_now() -> datetime:
    return datetime.now(SHANGHAI)


def _dt(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("exact datetime is required")
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return _shanghai_naive(value)


def _dt_text(value: datetime) -> str:
    return _shanghai_naive(value).strftime("%Y-%m-%dT%H:%M:%S.%f")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else None
    if isinstance(value, datetime):
        return _dt_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_safe(item())
        except (TypeError, ValueError):
            pass
    try:
        if value != value:
            return None
    except Exception:
        pass
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        _json_safe(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _reconstruction_authority_matches(
    authority: Mapping[str, Any],
    *,
    target_trade_date: date,
    catalog: AnnouncementCatalog,
    known_at: datetime | None = None,
) -> bool:
    """Recompute every nested authority seal and cross-bind its catalog."""

    if not isinstance(authority, Mapping):
        return False
    truth = authority.get("qmt_daily_truth")
    reconciliation = authority.get("reconciliation")
    if not all(isinstance(value, Mapping) for value in (truth, reconciliation)):
        return False
    truth = dict(truth)
    reconciliation = dict(reconciliation)
    target = target_trade_date.isoformat()
    no_trade_codes = reconciliation.get("native_no_trade_codes")
    exclusions = reconciliation.get("excluded_from_prior")
    additions = reconciliation.get("added_to_prior")
    if (
        not isinstance(no_trade_codes, list)
        or not isinstance(exclusions, list)
        or not isinstance(additions, list)
        or no_trade_codes != sorted(set(no_trade_codes))
        or any(_CODE_RE.fullmatch(str(code)) is None for code in no_trade_codes)
        or any(
            not isinstance(item, Mapping)
            or _CODE_RE.fullmatch(str(item.get("stock_code") or "")) is None
            or item.get("reason") not in {
                "LISTED_AFTER_TARGET", "EXPIRED_BEFORE_TARGET",
            }
            for item in exclusions
        )
    ):
        return False
    try:
        attested_count = int(reconciliation.get("attested_daily_count"))
        no_trade_count = int(reconciliation.get("native_no_trade_count"))
        prior_count = int(reconciliation.get("prior_eligible_count"))
    except (TypeError, ValueError, OverflowError):
        return False
    try:
        return bool(
            authority.get("qmt_daily_truth_sha256") == canonical_hash(truth)
            and _reconstruction_stock_scope_matches(
                authority,
                target_trade_date=target_trade_date,
                known_at=known_at,
            )
            and authority.get("reconciliation_sha256")
            == canonical_hash(reconciliation)
            and truth.get("schema")
            == "probiga.qmt-daily-market-consumer-truth.v1"
            and truth.get("requested_sessions") == [target]
            and truth.get("catalog_batch_id") == catalog.batch_id
            and truth.get("catalog_manifest_hash") == catalog.manifest_hash
            and truth.get("catalog_member_set_hash") == catalog.member_set_hash
            and int(truth.get("attested_row_count") or -1) == attested_count
            and reconciliation.get("schema")
            == "probiga.qmt-announcement-catalog-reconciliation.v2"
            and reconciliation.get("target_trade_date") == target
            and reconciliation.get("authority_catalog_batch_id")
            == catalog.batch_id
            and reconciliation.get("authority_catalog_manifest_hash")
            == catalog.manifest_hash
            and int(reconciliation.get("authority_eligible_count") or -1)
            == len(catalog.codes)
            and prior_count - len(exclusions) + len(additions) == len(catalog.codes)
            and all(item["stock_code"] in catalog.codes and catalog.qmt_by_code.get(item["stock_code"]) == item["qmt_code"]
                    for item in additions)
            and attested_count > 0
            and no_trade_count == len(no_trade_codes)
            and attested_count + no_trade_count == len(catalog.codes)
            and reconciliation.get("native_no_trade_codes_sha256")
            == canonical_hash(no_trade_codes)
        )
    except (TypeError, ValueError, OverflowError):
        return False


def _reconstruction_stock_scope_matches(
    authority: Mapping[str, Any],
    *,
    target_trade_date: date,
    known_at: datetime | None = None,
) -> bool:
    """Preserve legacy membership seals while defining the v3 stock scope."""

    schema = authority.get("schema")
    if schema == QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V2_SCHEMA:
        membership = authority.get("membership_snapshot")
        reconciliation = authority.get("reconciliation")
        if not isinstance(membership, Mapping) or not isinstance(reconciliation, Mapping):
            return False
        membership = dict(membership)
        return bool(
            authority.get("membership_snapshot_sha256")
            == canonical_hash(membership)
            and membership.get("snapshot_date")
            == target_trade_date.isoformat()
            and membership.get("quality_status") == "QMT_VALIDATED"
            and reconciliation.get("added_to_prior") == []
        )
    if schema == QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA:
        return bool(
            authority.get("stock_scope_authority")
            == QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE
            and authority.get("execution_origin")
            in QMT_ANNOUNCEMENT_RECONSTRUCTION_ORIGINS
            and "membership_snapshot" not in authority
            and "membership_snapshot_sha256" not in authority
            and _reconstruction_additions_valid(authority, target_trade_date=target_trade_date, known_at=known_at)
        )
    return False


def _reconstruction_additions_valid(authority, *, target_trade_date: date, known_at: datetime | None) -> bool:
    """Permit only a proven target-day IPO added to the native traded scope."""
    reconciliation = authority.get("reconciliation")
    if not isinstance(reconciliation, Mapping):
        return False
    additions = reconciliation.get("added_to_prior")
    if additions == []:
        return True
    from server.common.official_listing_evidence import validate_official_listing_evidence
    from server.common.qmt_datadir_daily_coverage import canonical_bar

    truth = authority.get("qmt_daily_truth") or {}
    if (not isinstance(additions, list) or len(additions) > 8 or truth.get("provider") != "qmt_datadir"
            or not _SHA256_RE.fullmatch(str(truth.get("truth_hash") or ""))):
        return False
    target = target_trade_date.isoformat()
    prelisting = (target_trade_date - timedelta(days=1)).isoformat()
    codes = []
    try:
        for addition in additions:
            code = addition["stock_code"]
            qmt_code = addition["qmt_code"]
            disclosure = validate_official_listing_evidence(addition["listing_disclosure"], stock_code=code, target_date=prelisting)
            captured = _dt(disclosure["captured_at"])
            entity = addition["native_daily_entity"]
            bar = entity["evidence"]
            if (not _CODE_RE.fullmatch(code) or not _QMT_CODE_RE.fullmatch(qmt_code) or qmt_code[:6] != code
                    or addition["reason"] != "OFFICIAL_TARGET_DAY_LISTING_WITH_NATIVE_TRADE"
                    or addition["list_date"] != target or disclosure["listing_date"] != target
                    or captured.date().isoformat() < disclosure["publication_date"] or (known_at is not None and captured > known_at)
                    or len(disclosure["source_text"].encode("utf-8")) > 8192
                    or entity["stock_code"] != code or entity["classification"] != "TRADED"
                    or entity["row_hash"] != canonical_hash({"stock_code": code, "classification": "TRADED", "evidence": bar})
                    or bar["stock_code"] != code or bar["trade_date"] != target or canonical_bar(bar) != bar
                    or not (Decimal(bar["volume"]) > 0 or Decimal(bar["amount"]) > 0)):
                return False
            codes.append(code)
        excluded = {entry["stock_code"] for entry in reconciliation["excluded_from_prior"]}
        unavailable = set(reconciliation["native_no_trade_codes"])
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False
    return codes == sorted(set(codes)) and not set(codes) & (excluded | unavailable)


def _reconstruction_execution_origin_matches(
    authority: Mapping[str, Any],
    *,
    execution_origin: str,
) -> bool:
    origin = str(execution_origin or "").strip().upper()
    schema = authority.get("schema")
    if schema == QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V2_SCHEMA:
        return bool(
            origin == QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN
            and "execution_origin" not in authority
        )
    if schema == QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA:
        return bool(
            origin in QMT_ANNOUNCEMENT_RECONSTRUCTION_ORIGINS
            and authority.get("execution_origin") == origin
        )
    return False


def _persisted_reconstruction_execution_origin_matches(
    provenance: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> bool:
    if not isinstance(provenance, Mapping) or not isinstance(authority, Mapping):
        return False
    raw_origin = str(provenance.get("execution_origin") or "").strip().upper()
    if authority.get("schema") == (
        QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V2_SCHEMA
    ):
        return bool(
            raw_origin in {"", QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN}
            and "execution_origin" not in authority
        )
    return _reconstruction_execution_origin_matches(
        authority,
        execution_origin=raw_origin,
    )


def _fallback_receipt_valid(
    receipt: Any,
    *,
    source: str,
    stock_code: str,
    qmt_code: str,
    requested_start_time: str,
    requested_end_time: str,
    result_count: int,
    catalog_codes: Sequence[str],
    staged: bool = False,
) -> bool:
    """Validate source-specific proof without weakening common PIT checks."""

    if not isinstance(receipt, Mapping):
        return False
    try:
        page_count = int(receipt.get("page_count"))
        expected_pages = int(receipt.get("expected_pages"))
        count = int(receipt.get("result_count"))
    except (TypeError, ValueError, OverflowError):
        return False
    if (
        receipt.get("schema")
        != (
            CNINFO_PROVIDER_RECEIPT_SCHEMA
            if source == CNINFO_ANNOUNCEMENT_SOURCE
            else "probiga.announcement-provider-receipt.v1"
        )
        or receipt.get("status") != "COMPLETE"
        or receipt.get("source") != source
        or str(receipt.get("stock_code") or "") != stock_code
        or str(receipt.get("qmt_code") or "") != qmt_code
        or receipt.get("requested_start_time") != requested_start_time
        or receipt.get("requested_end_time") != requested_end_time
        or receipt.get("exhausted") is not True
        or isinstance(receipt.get("result_count"), bool)
        or count != result_count
        or page_count < 1
        or expected_pages < 0
        or not _SHA256_RE.fullmatch(
            str(receipt.get("provider_payload_sha256") or "")
        )
    ):
        return False
    if source != CNINFO_ANNOUNCEMENT_SOURCE:
        return True
    try:
        provider_total = int(receipt.get("provider_total_record_count"))
        reported_pages = int(receipt.get("provider_reported_totalpages"))
        max_pages = int(receipt.get("max_pages_per_stock"))
    except (TypeError, ValueError, OverflowError):
        return False
    from server.common.pit_facts import _cninfo_cutoff_partition_valid

    if not _cninfo_cutoff_partition_valid(
        receipt, requested_end_time=requested_end_time,
        raw_count=provider_total, retained_count=result_count,
    ):
        return False
    page_hashes = receipt.get("page_sha256")
    master_start = str(receipt.get("security_master_sha256") or "")
    first_anchor = str(receipt.get("first_page_anchor_sha256") or "")
    last_anchor = str(receipt.get("last_page_anchor_sha256") or "")
    first_page_recheck = str(
        receipt.get("first_page_recheck_sha256") or ""
    )
    pagination_mode = str(receipt.get("pagination_mode") or "")
    quality_status = str(receipt.get("quality_status") or "")
    date_shard_manifest = receipt.get("date_shard_manifest")
    date_shard_manifest_hash = str(
        receipt.get("date_shard_manifest_sha256") or ""
    )
    date_shard_recheck_hash = str(
        receipt.get("date_shard_recheck_sha256") or ""
    )
    complete_round_hashes = receipt.get(
        "pagination_complete_round_sha256"
    )
    complete_round_attempts = receipt.get(
        "pagination_complete_round_attempts"
    )
    split_version = str(receipt.get("date_shard_split_version") or "")
    try:
        date_shard_count = int(receipt.get("date_shard_count") or 0)
        pagination_round_count = int(
            receipt.get("pagination_round_count") or 0
        )
        pagination_query_count = int(
            receipt.get("pagination_query_count") or 0
        )
        pagination_attempt_count = int(
            receipt.get("pagination_attempt_count") or 0
        )
        pagination_invalid_round_count = int(
            receipt.get("pagination_invalid_round_count") or 0
        )
    except (TypeError, ValueError, OverflowError):
        return False
    directory_raw_hash = str(receipt.get("directory_raw_sha256") or "")
    directory_manifest_hash = str(
        receipt.get("directory_manifest_hash") or ""
    )
    directory_member_set_hash = str(
        receipt.get("directory_member_set_hash") or ""
    )
    pagination_payload = {
        "pagination_mode": pagination_mode,
        "total_record_count": provider_total,
        "reported_totalpages": reported_pages,
        "page_count": page_count,
        "page_sha256": page_hashes,
        "last_page_has_more": False,
        "first_page_sha256": first_anchor,
        "last_page_sha256": last_anchor,
        "first_page_recheck_sha256": first_page_recheck,
    }
    if "excluded_after_cutoff" in receipt:
        pagination_payload["excluded_after_cutoff"] = receipt["excluded_after_cutoff"]
    shard_mode = pagination_mode == (
        "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
    )
    if shard_mode:
        pagination_payload.update({
            "date_shard_manifest_sha256": date_shard_manifest_hash,
            "date_shard_recheck_sha256": date_shard_recheck_hash,
            "pagination_round_count": pagination_round_count,
            "pagination_query_count": pagination_query_count,
            "pagination_attempt_count": pagination_attempt_count,
            "pagination_invalid_round_count": (
                pagination_invalid_round_count
            ),
            "pagination_complete_round_sha256": complete_round_hashes,
            "pagination_complete_round_attempts": complete_round_attempts,
            "date_shard_split_version": split_version,
        })
    expected_pagination_hash = canonical_hash(pagination_payload)
    common_pagination_valid = bool(
        expected_pages == page_count
        and max_pages == CNINFO_MAX_PAGES_PER_STOCK
        and 1 <= page_count <= max_pages
        and reported_pages == provider_total // 30
        and isinstance(page_hashes, list)
        and len(page_hashes) == page_count
        and all(
            _SHA256_RE.fullmatch(str(value or ""))
            for value in page_hashes
        )
        and _SHA256_RE.fullmatch(first_anchor)
        and first_anchor == str(page_hashes[0])
        and _SHA256_RE.fullmatch(last_anchor)
        and last_anchor == str(page_hashes[-1])
        and receipt.get("last_page_has_more") is False
        and receipt.get("pagination_sha256") == expected_pagination_hash
    )
    # Preserve already-staged v3 single-page receipts.  Previous multi-page
    # first-page-only receipts are deliberately rejected and must be recaptured
    # under the stable-sweep proof below.
    single_page_proof_valid = bool(
        pagination_mode == "EXACT_STOCK_SINGLE_PAGE"
        and quality_status == "EXACT_STOCK_PAGINATION_EXHAUSTED"
        and page_count == expected_pages == 1
        and 0 <= provider_total <= 30
        and first_page_recheck == ""
        and date_shard_manifest in (None, [])
        and date_shard_count == 0
        and date_shard_manifest_hash == ""
        and date_shard_recheck_hash == ""
        and pagination_round_count in {0, 1}
        and pagination_query_count in {0, 1}
        and pagination_attempt_count in {0, 1}
        and pagination_invalid_round_count == 0
        and complete_round_hashes in (None, [])
        and complete_round_attempts in (None, [])
        and split_version == ""
    )
    shard_proof_valid = False
    if shard_mode:
        try:
            requested_start_date = datetime.strptime(
                requested_start_time[:8], "%Y%m%d"
            ).date()
            requested_end_date = datetime.strptime(
                requested_end_time[:8], "%Y%m%d"
            ).date()
        except (TypeError, ValueError):
            return False
        if isinstance(date_shard_manifest, list) and date_shard_manifest:
            cursor = requested_start_date
            shard_total = 0
            tree_query_count = 2 * len(date_shard_manifest) - 1
            shard_hashes: list[str] = []
            normalized_shards: list[dict[str, Any]] = []
            for raw_shard in date_shard_manifest:
                if not isinstance(raw_shard, Mapping) or set(raw_shard) != {
                    "window_start",
                    "window_end",
                    "capture_mode",
                    "result_count",
                    "provider_reported_totalpages",
                    "provider_page_count",
                    "page_sha256",
                    "last_page_has_more",
                }:
                    normalized_shards = []
                    break
                try:
                    shard_start = datetime.strptime(
                        str(raw_shard.get("window_start") or ""), "%Y-%m-%d"
                    ).date()
                    shard_end = datetime.strptime(
                        str(raw_shard.get("window_end") or ""), "%Y-%m-%d"
                    ).date()
                    shard_count = int(raw_shard.get("result_count"))
                    shard_reported_pages = int(
                        raw_shard.get("provider_reported_totalpages")
                    )
                    shard_page_count = int(
                        raw_shard.get("provider_page_count")
                    )
                except (TypeError, ValueError, OverflowError):
                    normalized_shards = []
                    break
                capture_mode = str(raw_shard.get("capture_mode") or "")
                shard_hash = str(raw_shard.get("page_sha256") or "")
                expected_physical_pages = max(
                    1, (shard_count + 29) // 30
                )
                if (
                    shard_start != cursor
                    or shard_start > shard_end
                    or shard_end > requested_end_date
                    or shard_count < 0
                    or shard_reported_pages != shard_count // 30
                    or shard_page_count != expected_physical_pages
                    or shard_page_count > max_pages
                    or _SHA256_RE.fullmatch(shard_hash) is None
                    or raw_shard.get("last_page_has_more") is not False
                    or (
                        capture_mode == "SINGLE_PAGE"
                        and shard_count > 30
                    )
                    or (
                        capture_mode == "DENSE_DAY_COMPLETE_SWEEP"
                        and not (
                            shard_start == shard_end
                            and shard_count > 30
                            and shard_page_count > 1
                        )
                    )
                    or capture_mode not in {
                        "SINGLE_PAGE", "DENSE_DAY_COMPLETE_SWEEP"
                    }
                ):
                    normalized_shards = []
                    break
                normalized = {
                    "window_start": shard_start.isoformat(),
                    "window_end": shard_end.isoformat(),
                    "capture_mode": capture_mode,
                    "result_count": shard_count,
                    "provider_reported_totalpages": shard_reported_pages,
                    "provider_page_count": shard_page_count,
                    "page_sha256": shard_hash,
                    "last_page_has_more": False,
                }
                normalized_shards.append(normalized)
                shard_hashes.append(shard_hash)
                shard_total += shard_count
                tree_query_count += shard_page_count - 1
                cursor = shard_end + timedelta(days=1)
            expected_shard_hash = canonical_hash({
                "schema": "probiga.cninfo-date-shard-manifest.v1",
                "stock_code": stock_code,
                "requested_start_time": requested_start_time,
                "requested_end_time": requested_end_time,
                "shards": normalized_shards,
            })
            round_hashes_valid = bool(
                isinstance(complete_round_hashes, list)
                and len(complete_round_hashes) == pagination_round_count
                and all(
                    _SHA256_RE.fullmatch(str(value or ""))
                    for value in complete_round_hashes
                )
                and pagination_round_count >= 2
                and complete_round_hashes[-1] == complete_round_hashes[-2]
            )
            round_attempts_valid = bool(
                isinstance(complete_round_attempts, list)
                and len(complete_round_attempts) == pagination_round_count
                and all(
                    isinstance(value, int) and not isinstance(value, bool)
                    for value in complete_round_attempts
                )
                and complete_round_attempts
                == sorted(set(complete_round_attempts))
                and complete_round_attempts[-1] == pagination_attempt_count
                and complete_round_attempts[-2]
                == pagination_attempt_count - 1
            )
            shard_proof_valid = bool(
                normalized_shards
                and cursor == requested_end_date + timedelta(days=1)
                and normalized_shards == date_shard_manifest
                and date_shard_count == len(normalized_shards)
                and page_count == date_shard_count
                and expected_pages == date_shard_count
                and page_hashes == shard_hashes
                and shard_total == provider_total
                and date_shard_manifest_hash == expected_shard_hash
                and date_shard_recheck_hash == expected_shard_hash
                and split_version == CNINFO_DATE_SHARD_SPLIT_VERSION
                and quality_status
                == "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
                and 2 <= pagination_round_count
                <= CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS
                and 2 <= pagination_attempt_count
                <= CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS
                and 0 <= pagination_invalid_round_count
                < pagination_attempt_count
                and pagination_attempt_count
                == pagination_round_count + pagination_invalid_round_count
                and round_hashes_valid
                and round_attempts_valid
                and 2 * tree_query_count <= pagination_query_count
                <= max_pages
                and first_page_recheck == ""
            )
    if not (
        str(receipt.get("org_id") or "").strip()
        and receipt.get("query_stock_identity")
        == f"{stock_code},{receipt.get('org_id')}"
        and receipt.get("permission_status") == "PUBLIC"
        and common_pagination_valid
        and (single_page_proof_valid or shard_proof_valid)
        and _SHA256_RE.fullmatch(master_start)
        and _SHA256_RE.fullmatch(directory_raw_hash)
        and directory_manifest_hash == master_start
        and _SHA256_RE.fullmatch(directory_member_set_hash)
    ):
        return False
    if staged:
        # Reuse still requires a fresh, matching first security-master anchor.
        # The final second anchor and whole-catalog proof are sealed later.
        return True
    try:
        master_count = int(receipt.get("security_master_member_count"))
        requested_count = int(receipt.get("requested_catalog_member_count"))
        unrequested_count = int(receipt.get("security_master_unrequested_count"))
        missing_count = int(receipt.get("security_master_missing_requested_count"))
        directory_count = int(receipt.get("directory_member_count"))
        directory_coverage = int(receipt.get("directory_catalog_coverage_count"))
        directory_missing = int(receipt.get("directory_catalog_missing_count"))
        directory_extra = int(receipt.get("directory_catalog_extra_count"))
    except (TypeError, ValueError, OverflowError):
        return False
    master_end = str(receipt.get("security_master_end_sha256") or "")
    normalized_catalog = sorted(str(code).zfill(6) for code in catalog_codes)
    expected_directory_attestation = canonical_hash({
        "directory_raw_sha256": directory_raw_hash,
        "directory_manifest_hash": directory_manifest_hash,
        "directory_member_set_hash": directory_member_set_hash,
        "directory_member_count": directory_count,
        "security_master_start_sha256": master_start,
        "security_master_end_sha256": master_end,
        "requested_catalog_member_count": requested_count,
        "requested_catalog_member_set_sha256": receipt.get("requested_catalog_member_set_sha256"),
        "directory_catalog_missing_count": directory_missing,
    })
    return bool(
        master_start == master_end
        and receipt.get("directory_attestation_sha256") == expected_directory_attestation
        and master_count >= len(normalized_catalog)
        and directory_count == master_count
        and directory_coverage == len(normalized_catalog)
        and directory_missing == 0
        and directory_extra == directory_count - directory_coverage
        and requested_count == len(normalized_catalog)
        and unrequested_count == master_count - requested_count
        and missing_count == 0
        and receipt.get("requested_catalog_member_set_sha256")
        == canonical_hash(normalized_catalog)
    )


def parse_qmt_publication_time(value: Any) -> datetime:
    """Parse an exact QMT publication instant; date-only values are rejected."""

    if isinstance(value, datetime):
        parsed = _shanghai_naive(value)
    else:
        # pandas Timestamp and numpy datetime64 expose ``to_pydatetime``.
        converter = getattr(value, "to_pydatetime", None)
        if callable(converter):
            parsed = _shanghai_naive(converter())
        elif callable(getattr(value, "item", None)):
            # Official announcement DataFrames expose ``time`` as numpy int64
            # epoch milliseconds.  Normalize the scalar without converting it
            # through an imprecise display string.
            scalar = value.item()
            if scalar is value:
                raise ValueError(
                    "QMT announcement publication scalar cannot be normalized"
                )
            return parse_qmt_publication_time(scalar)
        elif isinstance(value, (int, float, Decimal)) and not isinstance(
            value, bool
        ):
            numeric = float(value)
            if not math.isfinite(numeric):
                raise ValueError("QMT announcement publication time is not finite")
            integer = str(int(numeric))
            if len(integer) == 14 and integer.startswith(("19", "20")):
                parsed = datetime.strptime(integer, "%Y%m%d%H%M%S")
            elif 1_000_000_000 <= numeric < 10_000_000_000:
                parsed = datetime.fromtimestamp(numeric, tz=timezone.utc).astimezone(
                    SHANGHAI
                ).replace(tzinfo=None)
            elif 1_000_000_000_000 <= numeric < 10_000_000_000_000:
                parsed = datetime.fromtimestamp(
                    numeric / 1000.0, tz=timezone.utc
                ).astimezone(SHANGHAI).replace(tzinfo=None)
            else:
                raise ValueError("QMT announcement numeric publication time is invalid")
        else:
            raw = str(value or "").strip()
            if _EXACT_COMPACT_RE.fullmatch(raw):
                base, _, fraction = raw.partition(".")
                parsed = datetime.strptime(base, "%Y%m%d%H%M%S")
                if fraction:
                    parsed = parsed.replace(
                        microsecond=int(fraction.ljust(6, "0")[:6])
                    )
            elif _EXACT_ISO_RE.fullmatch(raw):
                parsed = _shanghai_naive(
                    datetime.fromisoformat(raw.replace("Z", "+00:00"))
                )
            else:
                raise ValueError(
                    "QMT announcement publication time is not an exact timestamp"
                )
    since_midnight = parsed - datetime.combine(parsed.date(), datetime.min.time())
    if since_midnight < timedelta(minutes=1):
        # The official announcement example encodes its daily records as epoch
        # milliseconds a few seconds after midnight (for example
        # 1720195215674 -> 00:00:15.674 Shanghai).  That is a date/sequence
        # marker, not proven exchange publication time, and must not be promoted
        # to VERIFIED_EXACT PIT evidence.
        raise ValueError(
            "QMT announcement near-midnight date marker is not an exact "
            "publication timestamp"
        )
    return parsed


def _frame_records(frame: Any) -> list[tuple[Any, dict[str, Any]]]:
    if frame is None:
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RESPONSE_MISSING_FRAME")
    empty = getattr(frame, "empty", None)
    if empty is True:
        return []
    iterator = getattr(frame, "iterrows", None)
    if not callable(iterator):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RESPONSE_NOT_DATAFRAME",
            type(frame).__name__,
        )
    return [(index, dict(row)) for index, row in iterator()]


def _publication_from_record(index: Any, row: Mapping[str, Any]) -> datetime:
    parsed: list[datetime] = []
    for field in _PUBLICATION_FIELDS:
        raw = row.get(field)
        if raw is None or str(raw).strip() == "":
            continue
        parsed.append(parse_qmt_publication_time(raw))
    index_name = str(getattr(index, "name", "") or "").lower()
    if not parsed and (
        isinstance(index, datetime)
        or callable(getattr(index, "to_pydatetime", None))
        or (
            isinstance(index, (int, float, Decimal))
            and not isinstance(index, bool)
            and abs(float(index)) >= 1_000_000_000
        )
        or _EXACT_COMPACT_RE.fullmatch(str(index or "").strip()) is not None
        or _EXACT_ISO_RE.fullmatch(str(index or "").strip()) is not None
        or index_name in {"time", "datetime", "publish_time", "published_at"}
    ):
        parsed.append(parse_qmt_publication_time(index))
    if not parsed:
        raise ValueError("QMT announcement row has no exact publication timestamp")
    first = parsed[0]
    if any(item != first for item in parsed[1:]):
        raise ValueError("QMT announcement publication timestamp fields conflict")
    return first


def _first_text(row: Mapping[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = row.get(field)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _bounded_audit_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in sorted(str(item) for item in row if str(item) in _AUDIT_FIELDS):
        value = _json_safe(row.get(key))
        if isinstance(value, str):
            value = value[:4096]
        result[key] = value
    return result


def _provider_publication_date(row: Mapping[str, Any]) -> date:
    """Read an explicit provider date marker without inventing a time."""

    for field in (*_PUBLICATION_FIELDS, "notice_date", "art_date"):
        raw = row.get(field)
        if raw is None or str(raw).strip() == "":
            continue
        if isinstance(raw, (int, float, Decimal)) and not isinstance(raw, bool):
            numeric = float(raw)
            if 1_000_000_000_000 <= numeric < 10_000_000_000_000:
                return datetime.fromtimestamp(
                    numeric / 1000.0, tz=timezone.utc
                ).astimezone(SHANGHAI).date()
        text_value = str(raw).strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text_value):
            return date.fromisoformat(text_value)
        if re.fullmatch(r"\d{8}", text_value):
            return datetime.strptime(text_value, "%Y%m%d").date()
        try:
            return _dt(text_value).date()
        except (TypeError, ValueError):
            continue
    raise ValueError("announcement row has no provider publication date")


def _validated_source_publication_evidence(
    row: Mapping[str, Any],
) -> tuple[date, datetime | None]:
    """Validate a stored exact timestamp or its explicitly date-only evidence."""

    if not isinstance(row, Mapping):
        raise ValueError("announcement source row is invalid")
    raw_event_date = str(row.get("event_date") or "").strip()
    try:
        event_date = date.fromisoformat(raw_event_date)
    except ValueError as exc:
        raise ValueError("announcement event date is invalid") from exc
    if event_date.isoformat() != raw_event_date:
        raise ValueError("announcement event date is not canonical")
    raw_published_at = row.get("published_at")
    if raw_published_at is None or not str(raw_published_at).strip():
        return event_date, None
    published_at = _dt(raw_published_at)
    if published_at.date() != event_date:
        raise ValueError("announcement publication date differs")
    return event_date, published_at


def _source_publication_within_window(
    row: Mapping[str, Any],
    *,
    window_start: date,
    fact_cutoff_at: datetime,
) -> bool:
    try:
        event_date, published_at = _validated_source_publication_evidence(row)
    except (TypeError, ValueError):
        return False
    if not window_start <= event_date <= fact_cutoff_at.date():
        return False
    if published_at is not None:
        return published_at <= fact_cutoff_at
    return datetime.combine(event_date, datetime.max.time()) <= fact_cutoff_at


def parse_qmt_announcement_frame(
    *,
    stock_code: str,
    qmt_code: str,
    frame: Any,
    fact_cutoff_at: datetime,
    window_start: date,
    source: str = QMT_ANNOUNCEMENT_SOURCE,
) -> list[dict[str, Any]]:
    """Normalize one source-identified ``stock -> DataFrame`` response."""

    code = str(stock_code or "").zfill(6)
    instrument = str(qmt_code or "").upper()
    if not _CODE_RE.fullmatch(code) or not _QMT_CODE_RE.fullmatch(instrument):
        raise ValueError("QMT announcement stock identity is invalid")
    if instrument[:6] != code:
        raise ValueError("QMT announcement stock/QMT identities differ")
    cutoff = _dt(fact_cutoff_at)
    source_name = str(source or "").strip()
    if source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
        raise ValueError("announcement source identity is invalid")
    records = _frame_records(frame)
    if (
        source_name == QMT_ANNOUNCEMENT_SOURCE
        and records
        and all(set(row) == _QMT_NATIVE_DAILY_BAR_FIELDS for _, row in records)
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_API_UNAVAILABLE",
            f"{instrument}:announcement_period_returned_native_daily_bars",
        )
    events: dict[str, dict[str, Any]] = {}
    for index, raw_row in records:
        row = {str(key): _json_safe(value) for key, value in raw_row.items()}
        published: datetime | None
        try:
            published = _publication_from_record(index, raw_row)
        except ValueError:
            if source_name == QMT_ANNOUNCEMENT_SOURCE:
                raise
            published = None
        event_date = (
            published.date()
            if published is not None
            else _provider_publication_date(raw_row)
        )
        if (published is not None and published > cutoff) or event_date > cutoff.date():
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_FUTURE_PUBLICATION",
                f"{instrument}:{event_date.isoformat()}>{_dt_text(cutoff)}",
            )
        if event_date < window_start:
            continue
        source_code = _first_text(
            raw_row, ("stock_code", "stockCode", "code", "instrument")
        ).upper()
        if source_code:
            normalized_source = source_code.split(".", 1)[0].zfill(6)
            if normalized_source != code:
                raise ValueError("QMT announcement row stock identity differs")
        title = _first_text(raw_row, _TITLE_FIELDS)
        if not title:
            raise ValueError("QMT announcement title is missing")
        source_event_id = _first_text(raw_row, _EVENT_ID_FIELDS)
        identity_material = {
            "schema": "probiga.qmt-announcement-event-identity.v1",
            "source": str(source or "").strip(),
            "qmt_code": instrument,
            "source_event_id": source_event_id,
            "published_at": (
                _dt_text(published) if published is not None
                else event_date.isoformat()
            ),
            "title": title,
        }
        event_prefix = {
            QMT_ANNOUNCEMENT_SOURCE: "qmt",
            CNINFO_ANNOUNCEMENT_SOURCE: "cninfo",
            EASTMONEY_ANNOUNCEMENT_SOURCE: "eastmoney",
        }[source_name]
        event_key = (
            f"{event_prefix}:{source_event_id}"[:160]
            if source_event_id
            else f"{event_prefix}:{canonical_hash(identity_material)}"[:160]
        )
        provider_source_row_hash = _first_text(
            raw_row, ("source_row_hash",)
        )
        if not (
            source_name != QMT_ANNOUNCEMENT_SOURCE
            and _SHA256_RE.fullmatch(provider_source_row_hash)
        ):
            provider_source_row_hash = canonical_hash(row)
        payload = {
            "event_key": event_key,
            "stock_code": code,
            "qmt_code": instrument,
            "event_date": event_date.isoformat(),
            "published_at": _dt_text(published) if published is not None else None,
            "source_published_text": event_date.isoformat(),
            "title": title[:1024],
            "source_event_id": source_event_id[:512],
            # Commit to the complete source row without duplicating a possibly
            # huge announcement body into both the revision and coverage
            # ledgers.  The bounded fields are sufficient for operator audit.
            "source_row_hash": provider_source_row_hash,
            "source_fields": _bounded_audit_fields(raw_row),
        }
        previous = events.get(event_key)
        if previous is not None and previous != payload:
            raise ValueError("QMT announcement event identity has conflicting rows")
        events[event_key] = payload
    return sorted(
        events.values(),
        key=lambda item: (str(item["published_at"]), str(item["event_key"])),
    )


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class AnnouncementCheckpoint:
    """Hash-bound per-code staging that cannot combine different cutoffs."""

    def __init__(self, root: Path, manifest: Mapping[str, Any]) -> None:
        self.root = root
        self.manifest = dict(manifest)

    @classmethod
    def open(
        cls,
        root: Path,
        *,
        batch_id: str,
        fact_cutoff_at: datetime,
        window_start: date,
        window_end: date,
        catalog: AnnouncementCatalog,
        resume: bool,
        source: str = QMT_ANNOUNCEMENT_SOURCE,
        fallback_reason: str = "",
        coverage_target_date: date | str | None = None,
        coverage_window_start: date | str | None = None,
        manifest_extra: Mapping[str, Any] | None = None,
    ) -> "AnnouncementCheckpoint":
        directory = root / batch_id
        target_date = (
            window_end.isoformat()
            if coverage_target_date is None
            else date.fromisoformat(str(coverage_target_date)[:10]).isoformat()
        )
        expected = {
            "schema": QMT_ANNOUNCEMENT_CHECKPOINT_SCHEMA,
            "batch_id": batch_id,
            "fact_cutoff_at": _dt_text(fact_cutoff_at),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "stock_codes": list(catalog.codes),
            "qmt_by_code": dict(catalog.qmt_by_code),
            "source": str(source or "").strip(),
            "primary_source": QMT_ANNOUNCEMENT_SOURCE,
            "fallback_reason": str(fallback_reason or "").strip(),
            "coverage_target_date": target_date,
            "coverage_window_start": (
                window_start.isoformat()
                if coverage_window_start is None
                else date.fromisoformat(
                    str(coverage_window_start)[:10]
                ).isoformat()
            ),
        }
        extra = _json_safe(dict(manifest_extra or {}))
        if not isinstance(extra, dict) or set(expected).intersection(extra):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_EXTRA_INVALID"
            )
        expected.update(extra)
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            if not resume:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_EXISTS", batch_id
                )
            try:
                observed = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_INVALID", type(exc).__name__
                ) from exc
            if observed != expected:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_MIXED_CUTOFF_OR_CATALOG"
                )
        else:
            _atomic_write(manifest_path, expected)
        return cls(directory, expected)

    def _result_path(self, stock_code: str) -> Path:
        return self.root / "results" / f"{stock_code}.json"

    def _load_result(self, stock_code: str) -> dict[str, Any] | None:
        path = self._result_path(stock_code)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_INVALID",
                f"{stock_code}:{type(exc).__name__}",
            ) from exc
        if not isinstance(payload, dict):
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_INVALID", stock_code)
        unsigned = dict(payload)
        payload_hash = str(unsigned.pop("payload_hash", ""))
        expected = {
            "schema": QMT_ANNOUNCEMENT_RESULT_SCHEMA,
            "batch_id": self.manifest["batch_id"],
            "fact_cutoff_at": self.manifest["fact_cutoff_at"],
            "catalog_batch_id": self.manifest["catalog_batch_id"],
            "stock_code": stock_code,
            "qmt_code": self.manifest["qmt_by_code"][stock_code],
        }
        if (
            any(unsigned.get(key) != value for key, value in expected.items())
            or not isinstance(unsigned.get("events"), list)
            or any(not isinstance(event, dict) for event in unsigned["events"])
            or not _SHA256_RE.fullmatch(payload_hash)
            or canonical_hash(unsigned) != payload_hash
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_MIXED_OR_TAMPERED",
                stock_code,
            )
        return unsigned

    def load(self, stock_code: str) -> list[dict[str, Any]] | None:
        result = self._load_result(stock_code)
        return None if result is None else [dict(item) for item in result["events"]]

    def save(
        self,
        stock_code: str,
        events: Sequence[Mapping[str, Any]],
        *,
        provider_receipt: Mapping[str, Any] | None = None,
    ) -> None:
        unsigned = {
            "schema": QMT_ANNOUNCEMENT_RESULT_SCHEMA,
            "batch_id": self.manifest["batch_id"],
            "fact_cutoff_at": self.manifest["fact_cutoff_at"],
            "catalog_batch_id": self.manifest["catalog_batch_id"],
            "stock_code": stock_code,
            "qmt_code": self.manifest["qmt_by_code"][stock_code],
            "events": [dict(item) for item in events],
        }
        if provider_receipt is not None:
            unsigned["provider_receipt"] = dict(provider_receipt)
        _atomic_write(
            self._result_path(stock_code),
            {**unsigned, "payload_hash": canonical_hash(unsigned)},
        )

    def load_provider_receipt(
        self, stock_code: str
    ) -> dict[str, Any] | None:
        result = self._load_result(stock_code)
        if result is None:
            return None
        receipt = result.get("provider_receipt")
        if receipt is None:
            return None
        if not isinstance(receipt, Mapping):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_MIXED_OR_TAMPERED",
                stock_code,
            )
        return dict(receipt)

    def quarantine_result(self, stock_code: str, *, reason: str) -> Path:
        """Move one obsolete staged proof aside so that it is recaptured."""

        if stock_code not in self.manifest["qmt_by_code"]:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_INVALID", stock_code
            )
        path = self._result_path(stock_code)
        if not path.exists():
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_INVALID", stock_code
            )
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
            safe_reason = re.sub(r"[^a-z0-9-]+", "-", reason.lower()).strip("-")
            if not safe_reason:
                safe_reason = "obsolete"
            destination = (
                self.root / "invalidated-results"
                / f"{stock_code}.{safe_reason}.{digest}.json"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                destination.unlink()
            os.replace(path, destination)
        except OSError as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_INVALID",
                f"{stock_code}:{type(exc).__name__}",
            ) from exc
        return destination

    def diagnose(self, reason_code: str, detail: str) -> None:
        _atomic_write(
            self.root / "failure.json",
            {
                "schema": "probiga.qmt-announcement-failure.v1",
                "batch_id": self.manifest["batch_id"],
                "fact_cutoff_at": self.manifest["fact_cutoff_at"],
                "reason_code": str(reason_code),
                "detail": str(detail)[:1000],
            },
        )

    def prepare_publish(
        self, *, batch_root_hash: str, received_at: datetime, publication_batch_id: str = ""
    ) -> None:
        unsigned = {
            "schema": QMT_ANNOUNCEMENT_PREPARED_SCHEMA,
            "batch_id": self.manifest["batch_id"],
            "fact_cutoff_at": self.manifest["fact_cutoff_at"],
            "received_at": _dt_text(received_at),
            "batch_root_hash": str(batch_root_hash),
        }
        if publication_batch_id:
            unsigned["publication_batch_id"] = publication_batch_id
        _atomic_write(
            self.root / "prepared.json",
            {**unsigned, "payload_hash": canonical_hash(unsigned)},
        )

    def load_prepared_publish(self) -> tuple[datetime, str] | None:
        path = self.root / "prepared.json"
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_PREPARED_CHECKPOINT_INVALID",
                type(exc).__name__,
            ) from exc
        unsigned = dict(payload)
        payload_hash = str(unsigned.pop("payload_hash", ""))
        received_text = str(unsigned.get("received_at") or "")
        root_hash = str(unsigned.get("batch_root_hash") or "")
        if (
            unsigned.get("schema") != QMT_ANNOUNCEMENT_PREPARED_SCHEMA
            or unsigned.get("batch_id") != self.manifest["batch_id"]
            or unsigned.get("fact_cutoff_at")
            != self.manifest["fact_cutoff_at"]
            or not _SHA256_RE.fullmatch(root_hash)
            or not _SHA256_RE.fullmatch(payload_hash)
            or canonical_hash(unsigned) != payload_hash
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_PREPARED_CHECKPOINT_TAMPERED"
            )
        try:
            received_at = _dt(received_text)
        except (TypeError, ValueError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_PREPARED_CHECKPOINT_INVALID_TIME"
            ) from exc
        return received_at, root_hash

    def mark_complete(self, *, batch_root_hash: str, received_at: datetime, publication_batch_id: str = "") -> None:
        _atomic_write(
            self.root / "complete.json",
            {
                "schema": "probiga.qmt-announcement-checkpoint-complete.v1",
                "batch_id": self.manifest["batch_id"],
                "fact_cutoff_at": self.manifest["fact_cutoff_at"],
                "received_at": _dt_text(received_at),
                "batch_root_hash": batch_root_hash,
                **({"publication_batch_id": publication_batch_id} if publication_batch_id else {}),
            },
        )

    def load_complete(self) -> dict[str, list[dict[str, Any]]]:
        results: dict[str, list[dict[str, Any]]] = {}
        for code in self.manifest["stock_codes"]:
            value = self.load(str(code))
            if value is None:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_INCOMPLETE", str(code)
                )
            results[str(code)] = value
        return results


def _load_catalog(engine: Engine, fact_cutoff_at: datetime) -> AnnouncementCatalog:
    from server.common.qmt_stock_catalog import load_target_stock_catalog

    catalog, codes = load_target_stock_catalog(
        engine,
        target_date=fact_cutoff_at.date().isoformat(),
        decision_known_at=fact_cutoff_at,
    )
    code_set = set(codes)
    qmt_by_code = {
        str(member["stock_code"]): str(member["qmt_code"])
        for member in catalog.members
        if str(member["stock_code"]) in code_set
    }
    normalized = tuple(sorted(codes))
    if set(qmt_by_code) != set(normalized):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CATALOG_MAPPING_INCOMPLETE"
        )
    return AnnouncementCatalog(
        batch_id=catalog.batch_id,
        manifest_hash=catalog.manifest_hash,
        member_set_hash=catalog.member_set_hash,
        codes=normalized,
        qmt_by_code=qmt_by_code,
    )


def _cninfo_directory_identity(value: Any) -> dict[str, str] | None:
    if not isinstance(value, Mapping):
        return None
    identity = {
        field: str(value.get(field) or "")
        for field in (
            "directory_raw_sha256", "directory_manifest_hash", "directory_member_set_hash",
        )
    }
    return identity if all(_SHA256_RE.fullmatch(item) for item in identity.values()) else None


def _find_resumable_checkpoint(
    engine: Engine,
    *,
    checkpoint_root: Path,
    observed_at: datetime,
    window_days: int,
    source: str,
    fallback_reason: str,
    coverage_target_date: date | str | None,
    coverage_window_start: date,
    capture_window_start: date,
    cninfo_directory: dict[str, str] | None = None,
) -> tuple[datetime, AnnouncementCatalog, dict[str, str] | None] | None:
    candidates: list[tuple[int, datetime, Path, dict[str, Any], str | None]] = []
    try:
        manifests = list(Path(checkpoint_root).glob("*/manifest.json"))
    except OSError:
        return None
    for path in manifests:
        if (path.parent / "complete.json").exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            cutoff = _dt(payload.get("fact_cutoff_at"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            payload.get("schema") != QMT_ANNOUNCEMENT_CHECKPOINT_SCHEMA
            or observed_at < cutoff
            or payload.get("source") != source
            or payload.get("fallback_reason") != fallback_reason
        ):
            continue
        known_codes = set(payload.get("stock_codes") or ())
        try:
            staged_codes = {
                result_path.stem
                for result_path in (path.parent / "results").glob("*.json")
                if result_path.stem in known_codes
            }
        except OSError:
            staged_codes = set()
        candidates.append((len(staged_codes), cutoff, path, payload, min(staged_codes, default=None)))
    # Prefer the valid capture with the most already-proven members.  A later
    # retry may have created a fresh cutoff before failing; choosing it merely
    # because it is newer would discard safe staging and repeat source work.
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    for _staged_count, cutoff, path, payload, first_staged_code in candidates:
        try:
            catalog = _load_catalog(engine, cutoff)
        except Exception:
            continue
        try:
            target_date = (
                cutoff.date()
                if coverage_target_date is None
                else date.fromisoformat(str(coverage_target_date)[:10])
            )
        except ValueError:
            continue
        expected_coverage_start = (
            target_date - timedelta(days=int(window_days))
        )
        if expected_coverage_start != coverage_window_start:
            continue
        expected_start = capture_window_start.isoformat()
        source_prefix = {
            QMT_ANNOUNCEMENT_SOURCE: "qmt-ann",
            CNINFO_ANNOUNCEMENT_SOURCE: "cninfo-ann",
            EASTMONEY_ANNOUNCEMENT_SOURCE: "em-ann",
        }.get(source)
        if source_prefix is None:
            continue
        expected_seed = {
            "schema": "probiga.qmt-announcement-batch-id.v2",
            "source": source,
            "primary_source": QMT_ANNOUNCEMENT_SOURCE,
            "fallback_reason": fallback_reason,
            "fact_cutoff_at": _dt_text(cutoff),
            "coverage_target_date": target_date.isoformat(),
            "window_start": expected_coverage_start.isoformat(),
            "capture_window_start": expected_start,
            "window_end": cutoff.date().isoformat(),
            "catalog_batch_id": catalog.batch_id,
            "catalog_member_set_hash": catalog.member_set_hash,
        }
        bound_directory = None
        if cninfo_directory is not None and "cninfo_directory_identity" in payload:
            bound_directory = _cninfo_directory_identity(payload["cninfo_directory_identity"])
            if bound_directory is None:
                continue
            expected_seed["cninfo_directory_identity"] = bound_directory
        expected_batch_id = (
            f"{source_prefix}-{cutoff.strftime('%Y%m%dT%H%M%S')}-"
            f"{canonical_hash(expected_seed)[:16]}"
        )
        if (
            payload.get("batch_id") == expected_batch_id
            and path.parent.name == expected_batch_id
            and payload.get("window_start") == expected_start
            and payload.get("window_end") == cutoff.date().isoformat()
            and payload.get("catalog_batch_id") == catalog.batch_id
            and payload.get("catalog_manifest_hash") == catalog.manifest_hash
            and payload.get("catalog_member_set_hash") == catalog.member_set_hash
            and payload.get("stock_codes") == list(catalog.codes)
            and payload.get("qmt_by_code") == dict(catalog.qmt_by_code)
            and payload.get("source") == source
            and payload.get("primary_source") == QMT_ANNOUNCEMENT_SOURCE
            and payload.get("fallback_reason") == fallback_reason
            and payload.get("coverage_target_date") == target_date.isoformat()
            and payload.get("coverage_window_start")
            == expected_coverage_start.isoformat()
        ):
            if cninfo_directory is not None:
                if bound_directory is not None:
                    if bound_directory != cninfo_directory:
                        continue
                else:
                    # Existing manifests did not bind the directory. Authenticate
                    # one staged member before treating its directory as the anchor.
                    if first_staged_code is None:
                        continue
                    receipt = AnnouncementCheckpoint(path.parent, payload).load_provider_receipt(first_staged_code)
                    staged_directory = _cninfo_directory_identity(receipt)
                    if (
                        staged_directory is None
                        or receipt.get("security_master_sha256") != staged_directory["directory_manifest_hash"]
                        or receipt.get("source") != source
                        or receipt.get("stock_code") != first_staged_code
                        or receipt.get("qmt_code") != catalog.qmt_by_code[first_staged_code]
                    ):
                        raise QMTAnnouncementBlocked(
                            "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", first_staged_code,
                        )
                    if staged_directory != cninfo_directory:
                        continue
            return cutoff, catalog, bound_directory
    return None


def _load_incremental_announcement_baseline(
    engine: Engine,
    *,
    catalog: AnnouncementCatalog,
    source: str,
    fact_cutoff_at: datetime,
    coverage_window_start: date,
    capture_window_start: date,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]] | None:
    """Load one previously validated batch as the immutable daily baseline."""

    if capture_window_start <= coverage_window_start or not catalog.codes:
        return {}, None
    try:
        with engine.connect() as connection:
            candidates = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT batch_id, known_at, payload_json "
                        "FROM st_pit_source_coverage "
                        "WHERE fact_kind='event' AND source=:source "
                        "AND stock_code=:stock_code "
                        "AND coverage_status='COMPLETE' "
                        "AND known_at<:fact_cutoff_at "
                        "AND window_start<=:coverage_window_start "
                        "AND window_end>=:capture_window_start "
                        "ORDER BY known_at DESC, batch_id DESC LIMIT 8"
                    ),
                    {
                        "source": source,
                        "stock_code": catalog.codes[0],
                        "fact_cutoff_at": fact_cutoff_at,
                        "coverage_window_start": coverage_window_start,
                        "capture_window_start": capture_window_start,
                    },
                ).mappings()
            ]
        for candidate in candidates:
            try:
                payload = json.loads(str(candidate.get("payload_json") or ""))
                watermark = payload["watermark"]
                evidence = watermark["evidence"]
                prior_received = _dt(evidence["received_at"])
                prior_cutoff = _dt(evidence["fact_cutoff_at"])
                prior_start = date.fromisoformat(str(evidence["window_start"])[:10])
                prior_end = date.fromisoformat(str(evidence["window_end"])[:10])
                proof = validate_complete_qmt_announcement_batch(
                    engine,
                    codes=[catalog.codes[0]],
                    decision_at=prior_received,
                    fact_cutoff_at=prior_cutoff,
                    window_start=max(coverage_window_start, prior_start),
                    window_end=min(capture_window_start, prior_end),
                    source=source,
                )
                batch_id = str(candidate["batch_id"])
                if str(proof.get("batch_id") or "") != batch_id:
                    continue
                with engine.connect() as connection:
                    rows = [
                        dict(row)
                        for row in connection.execute(
                            text(
                                "SELECT stock_code, payload_json "
                                "FROM st_pit_source_coverage "
                                "WHERE fact_kind='event' AND source=:source "
                                "AND batch_id=:batch_id ORDER BY stock_code"
                            ),
                            {"source": source, "batch_id": batch_id},
                        ).mappings()
                    ]
                if [str(row["stock_code"]).zfill(6) for row in rows] != list(
                    catalog.codes
                ):
                    # Catalog additions/removals require a complete rebuild so
                    # a newly listed symbol never inherits an empty history.
                    continue
                baseline: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    code = str(row["stock_code"]).zfill(6)
                    row_payload = json.loads(str(row.get("payload_json") or ""))
                    source_rows = row_payload.get("source_rows")
                    if not isinstance(source_rows, list):
                        raise ValueError("baseline source rows are unavailable")
                    baseline[code] = [
                        dict(item)
                        for item in source_rows
                        if (
                            isinstance(item, Mapping)
                            and coverage_window_start
                            <= _validated_source_publication_evidence(item)[0]
                            < capture_window_start
                        )
                    ]
                return baseline, {
                    "parent_batch_id": batch_id,
                    "parent_batch_root_hash": str(
                        proof.get("batch_root_hash") or ""
                    ),
                    "parent_received_at": _dt_text(prior_received),
                }
            except (KeyError, TypeError, ValueError, QMTAnnouncementBlocked):
                continue
    except Exception:
        return {}, None
    return {}, None


def _merge_announcement_results(
    *,
    catalog: AnnouncementCatalog,
    baseline: Mapping[str, Sequence[Mapping[str, Any]]],
    delta: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for code in catalog.codes:
        by_event: dict[str, dict[str, Any]] = {}
        for item in [*baseline.get(code, ()), *delta.get(code, ())]:
            normalized = dict(item)
            key = str(normalized.get("event_key") or canonical_hash(normalized))
            by_event[key] = normalized
        merged[code] = sorted(by_event.values(), key=canonical_json)
    return merged


def _qmt_time(value: datetime) -> str:
    return _shanghai_naive(value).strftime("%Y%m%d%H%M%S")


def _explicit_qmt_unavailability_reason(exc: BaseException) -> str:
    """Return a fallback-eligible reason only for frozen broker evidence."""

    frozen = str(getattr(exc, "reason_code", "") or "").strip()
    if frozen in ANNOUNCEMENT_FALLBACK_REASON_CODES:
        return frozen
    module_name = str(getattr(exc, "name", "") or "").lower()
    if isinstance(exc, (ImportError, ModuleNotFoundError)) and (
        module_name.startswith("xtquant")
        or "xtquant" in str(exc).lower()
    ):
        return "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"
    message = str(exc).strip().lower()
    # This is a frozen failure emitted by the signed Big QMT bridge when the
    # broker terminal's own announcement implementation imports pandas from
    # ``_PyContextInfo.get_market_data_ex`` but that terminal runtime does not
    # contain pandas.  Keep the complete fingerprint deliberately narrow: an
    # arbitrary local ModuleNotFoundError, or even another Big QMT dependency
    # error, must never authorize a fallback provider.
    if (
        "big qmt announcement failed:" in message
        and "_pycontextinfo.py" in message
        and "get_market_data_ex" in message
        and "modulenotfounderror" in message
        and (
            "no module named 'pandas'" in message
            or 'no module named "pandas"' in message
        )
    ):
        return "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"
    if any(marker in message for marker in _QMT_ANNOUNCEMENT_PERMISSION_MARKERS):
        return "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED"
    return ""


def _connect_announcement_transport(
    adapter: Any, *, source: str = QMT_ANNOUNCEMENT_SOURCE,
) -> None:
    """Connect spool adapters once; retain native xtdata's port candidates."""

    connector = getattr(adapter, "connect_announcement_transport", None)
    if callable(connector):
        # Explicit adapters classify failures at their actual transport calls.
        # Their identity validation may also time out and must remain closed.
        connector()
        return
    if not callable(getattr(adapter, "connect", None)):
        if source != QMT_ANNOUNCEMENT_SOURCE:
            raise RuntimeError("announcement provider connection API is unavailable")
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_API_UNAVAILABLE", "connect"
        )
    try:
        connect_xtdata(adapter)
    except TimeoutError as exc:
        if source != QMT_ANNOUNCEMENT_SOURCE:
            raise
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT", "connect:TimeoutError"
        ) from exc


def _download_and_read(
    xtdata: Any,
    *,
    checkpoint: AnnouncementCheckpoint,
    catalog: AnnouncementCatalog,
    fact_cutoff_at: datetime,
    window_start: date,
    batch_size: int,
    source: str = QMT_ANNOUNCEMENT_SOURCE,
    before_fetch: Callable[[str], None] | None = None,
    after_save: Callable[[str], None] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    downloader = getattr(xtdata, "download_history_data", None)
    reader = getattr(xtdata, "get_market_data_ex", None)
    if not callable(downloader) or not callable(reader):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_API_UNAVAILABLE",
            "download_history_data/get_market_data_ex",
        )
    pending = [code for code in catalog.codes if checkpoint.load(code) is None]
    start_time = datetime.combine(window_start, datetime.min.time())
    checkpoint_batch_size = int(
        getattr(xtdata, "checkpoint_batch_size", batch_size)
    )
    effective_batch_size = min(
        max(1, int(batch_size)), max(1, checkpoint_batch_size)
    )
    failed_chunks = []
    for offset in range(0, len(pending), effective_batch_size):
        try:
            code_chunk = pending[offset:offset + effective_batch_size]
            qmt_chunk = [catalog.qmt_by_code[code] for code in code_chunk]
            operation = "download_history_data"
            try:
                for qmt_code in qmt_chunk:
                    if before_fetch is not None:
                        before_fetch(qmt_code)
                    downloader(
                        qmt_code,
                        period=QMT_ANNOUNCEMENT_PERIOD,
                        start_time=_qmt_time(start_time),
                        end_time=_qmt_time(fact_cutoff_at),
                    )
                operation = "get_market_data_ex"
                response = reader(
                    field_list=[],
                    stock_list=qmt_chunk,
                    period=QMT_ANNOUNCEMENT_PERIOD,
                    start_time=_qmt_time(start_time),
                    end_time=_qmt_time(fact_cutoff_at),
                    count=-1,
                    dividend_type="none",
                    fill_data=False,
                )
            except Exception as exc:
                if source == QMT_ANNOUNCEMENT_SOURCE and isinstance(exc, TimeoutError):
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT",
                        f"{operation}:TimeoutError",
                    ) from exc
                reason_code = str(getattr(exc, "reason_code", "") or "")
                if source != QMT_ANNOUNCEMENT_SOURCE and reason_code:
                    raise QMTAnnouncementBlocked(
                        reason_code,
                        str(getattr(exc, "detail", "") or type(exc).__name__),
                    ) from exc
                reason_code = _explicit_qmt_unavailability_reason(exc)
                if reason_code:
                    raise QMTAnnouncementBlocked(
                        reason_code,
                        str(getattr(exc, "detail", "") or type(exc).__name__),
                    ) from exc
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CAPTURE_RUNTIME_FAILED",
                    type(exc).__name__,
                ) from exc
            if not isinstance(response, Mapping):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_NOT_STOCK_MAP"
                )
            observed_keys = {str(key).upper() for key in response}
            if len(observed_keys) != len(response):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_DUPLICATE_STOCK"
                )
            missing = sorted(set(qmt_chunk) - observed_keys)
            if missing:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_MISSING_STOCK",
                    ",".join(missing[:10]),
                )
            unexpected = sorted(observed_keys - set(qmt_chunk))
            if unexpected:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_UNEXPECTED_STOCK",
                    ",".join(unexpected[:10]),
                )
            for code in code_chunk:
                qmt_code = catalog.qmt_by_code[code]
                frame = next(
                    value for key, value in response.items()
                    if str(key).upper() == qmt_code
                )
                try:
                    events = parse_qmt_announcement_frame(
                        stock_code=code,
                        qmt_code=qmt_code,
                        frame=frame,
                        fact_cutoff_at=fact_cutoff_at,
                        window_start=window_start,
                        source=source,
                    )
                except QMTAnnouncementBlocked:
                    raise
                except Exception as exc:
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_ROW_INVALID",
                        f"{qmt_code}:{type(exc).__name__}",
                    ) from exc
                provider_receipt = None
                if source != QMT_ANNOUNCEMENT_SOURCE:
                    staged_reader = getattr(
                        xtdata, "staged_capture_receipts", None
                    )
                    staged = staged_reader() if callable(staged_reader) else None
                    if not isinstance(staged, Mapping) or not isinstance(
                        staged.get(code), Mapping
                    ):
                        raise QMTAnnouncementBlocked(
                            "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                        )
                    provider_receipt = dict(staged[code])
                checkpoint.save(
                    code, events, provider_receipt=provider_receipt
                )
                if after_save is not None:
                    after_save(code)
        except QMTAnnouncementBlocked as exc:
            if exc.reason_code in ANNOUNCEMENT_FALLBACK_REASON_CODES or (before_fetch is not None
                    and exc.reason_code in {HISTORICAL_CAPTURE_DEADLINE, "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"}):
                raise
            failed_chunks.append(exc)
            checkpoint.diagnose(exc.reason_code, exc.detail)
    if failed_chunks:
        raise failed_chunks[0]
    return checkpoint.load_complete()


def _source_response_hash(events: Sequence[Mapping[str, Any]]) -> str:
    from server.common.pit_facts import canonical_hash as pit_hash

    normalized = sorted((dict(item) for item in events), key=canonical_json)
    return pit_hash(
        {
            "schema": "probiga.pit-event-source-response.v1",
            "rows": normalized,
        }
    )


def build_batch_root(
    *,
    batch_id: str,
    fact_cutoff_at: datetime,
    received_at: datetime,
    window_start: date,
    window_end: date,
    catalog: AnnouncementCatalog,
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    source: str = QMT_ANNOUNCEMENT_SOURCE,
    provider_receipts: Mapping[str, Mapping[str, Any]] | None = None,
    reconstruction_provenance: Mapping[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    receipts = provider_receipts or {}
    entries = []
    for code in catalog.codes:
        entry = {
            "stock_code": code,
            "source_response_hash": _source_response_hash(results[code]),
            "result_count": len(results[code]),
        }
        if source != QMT_ANNOUNCEMENT_SOURCE:
            receipt = receipts.get(code)
            if not isinstance(receipt, Mapping):
                raise ValueError("fallback provider receipt is missing")
            entry["provider_receipt_hash"] = canonical_hash(dict(receipt))
        entries.append(entry)
    reconstruction = dict(reconstruction_provenance or {})
    payload = {
        "schema": (
            QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA
            if reconstruction else QMT_ANNOUNCEMENT_BATCH_SCHEMA
        ),
        "source": source,
        "batch_id": batch_id,
        "fact_cutoff_at": _dt_text(fact_cutoff_at),
        "decision_at": _dt_text(received_at),
        "received_at": _dt_text(received_at),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": len(catalog.codes),
        "entries": entries,
    }
    if reconstruction:
        payload["reconstruction_provenance"] = reconstruction
    return canonical_hash(payload), entries


def _batch_root_from_entries(
    *,
    batch_id: str,
    fact_cutoff_at: datetime,
    received_at: datetime,
    window_start: date,
    window_end: date,
    catalog_batch_id: str,
    catalog_manifest_hash: str,
    catalog_member_set_hash: str,
    entries: Sequence[Mapping[str, Any]],
    source: str = QMT_ANNOUNCEMENT_SOURCE,
    reconstruction_provenance: Mapping[str, Any] | None = None,
) -> str:
    normalized_entries = []
    for item in entries:
        normalized = {
                "stock_code": str(item.get("stock_code") or "").zfill(6),
                "source_response_hash": str(
                    item.get("source_response_hash") or ""
                ),
                "result_count": int(item.get("result_count") or 0),
            }
        if source != QMT_ANNOUNCEMENT_SOURCE:
            normalized["provider_receipt_hash"] = str(
                item.get("provider_receipt_hash") or ""
            )
        normalized_entries.append(normalized)
    normalized_entries.sort(key=lambda item: item["stock_code"])
    reconstruction = dict(reconstruction_provenance or {})
    payload = {
        "schema": (
            QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA
            if reconstruction else QMT_ANNOUNCEMENT_BATCH_SCHEMA
        ),
        "source": source,
        "batch_id": batch_id,
        "fact_cutoff_at": _dt_text(fact_cutoff_at),
        "decision_at": _dt_text(received_at),
        "received_at": _dt_text(received_at),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "catalog_batch_id": catalog_batch_id,
        "catalog_manifest_hash": catalog_manifest_hash,
        "catalog_member_set_hash": catalog_member_set_hash,
        "catalog_member_count": len(normalized_entries),
        "entries": normalized_entries,
    }
    if reconstruction:
        payload["reconstruction_provenance"] = reconstruction
    return canonical_hash(payload)


def _live_announcement_payload_evidence(row: Mapping[str, Any]) -> tuple[dict, dict]:
    raw = str(row.get("payload_json") or "")
    payload = json.loads(raw)
    if canonical_json(payload) != raw:
        raise ValueError("coverage payload is not canonical")
    watermark = payload.get("watermark")
    evidence = watermark.get("evidence") if isinstance(watermark, dict) else None
    if (
        not isinstance(evidence, dict)
        or row.get("watermark_kind") != "QUERY_CUTOFF"
        or watermark.get("kind") != "QUERY_CUTOFF"
    ):
        raise ValueError("batch lacks query-cutoff evidence")
    return payload, evidence


def _live_announcement_read_hint(*, mysql: bool, deadline: float, stage: str) -> str:
    remaining_ms = int((deadline - monotonic()) * 1_000)
    if remaining_ms <= 0:
        raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE, stage)
    return (
        f"/*+ MAX_EXECUTION_TIME({min(remaining_ms, LIVE_ANNOUNCEMENT_BATCH_QUERY_TIMEOUT_MS)}) */ "
        if mysql else ""
    )


@contextmanager
def _live_announcement_reply_budget(connection: Any, *, deadline: float, stage: str):
    """Bound execute and the full reply on this checked-out MySQL connection."""
    started = monotonic()
    remaining = deadline - started
    if remaining <= 0:
        raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE, stage)
    if connection.dialect.name != "mysql":
        yield
        return
    dbapi = getattr(getattr(connection, "connection", None), "dbapi_connection", None)
    original = {
        name: getattr(dbapi, name) for name in ("_read_timeout", "_write_timeout")
        if hasattr(dbapi, name)
    }
    call_limit = min(
        remaining, LIVE_ANNOUNCEMENT_BATCH_QUERY_TIMEOUT_MS / 1_000,
        *(value for value in original.values() if value is not None),
    )
    call_deadline = started + call_limit
    expired = threading.Event()
    sock = getattr(dbapi, "_sock", None)

    def abort_reply() -> None:
        expired.set()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        # On Windows shutdown alone may not interrupt a timed receive.
        # PyMySQL's buffered makefile also defers a normal socket close.
        try:
            descriptor = sock.detach()
            if descriptor >= 0:
                socket.close(descriptor)
        except OSError:
            pass

    def check_reply(exc: Exception | None = None) -> None:
        observed = monotonic()
        if expired.is_set() or observed >= call_deadline:
            if sock is not None and not getattr(connection, "invalidated", False):
                if not expired.is_set():
                    abort_reply()
                try:
                    connection.invalidate()
                except SQLAlchemyError:
                    logger.warning("Announcement reply connection invalidation failed at %s", stage)
            reason = (
                LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE if observed >= deadline
                else LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE
            )
            raise QMTAnnouncementBlocked(reason, stage) from exc
        errno = getattr(getattr(exc, "orig", None), "args", ())
        if errno and errno[0] == 3024:
            raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE, stage) from exc
        if getattr(exc, "connection_invalidated", False) or getattr(connection, "invalidated", False):
            raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_REPLY_FAILED, stage) from exc

    for name in original:
        setattr(dbapi, name, call_limit)
    watchdog = None
    if sock is not None:
        watchdog = threading.Timer(max(0.0, call_deadline - monotonic()), abort_reply)
        watchdog.daemon = True
        watchdog.start()
    try:
        try:
            yield
        finally:
            if watchdog is not None:
                watchdog.cancel()
                watchdog.join()
            for name, value in original.items():
                setattr(dbapi, name, value)
    except Exception as exc:
        check_reply(exc)
        raise
    check_reply()


def _bounded_announcement_payload_keys(
    keys: Sequence[tuple[str, int]], *, max_rows: int = VALIDATION_READ_BATCH_SIZE,
) -> Iterator[list[tuple[str, int]]]:
    """Keep payload reads small, isolating any indivisible oversized row."""
    if max_rows <= 0:
        raise ValueError("announcement payload batch row limit must be positive")
    batch: list[tuple[str, int]] = []
    batch_bytes = 0
    for key in keys:
        _, payload_bytes = key
        if type(payload_bytes) is not int or payload_bytes <= 0:
            raise ValueError("announcement payload byte size differs")
        if batch and (
            len(batch) == max_rows
            or batch_bytes + payload_bytes > VALIDATION_READ_PAYLOAD_BYTES
        ):
            yield batch
            batch = []
            batch_bytes = 0
        if payload_bytes > VALIDATION_READ_PAYLOAD_BYTES:
            # The canonical payload must still be read and verified in full.
            yield [key]
        else:
            batch.append(key)
            batch_bytes += payload_bytes
    if batch:
        yield batch


def _read_live_announcement_payload_sizes(
    connection: Any, *, source: str, batch_id: str | None = None,
    coverage_ids: Sequence[str], deadline: float, stage: str,
) -> dict[str, int]:
    mysql = connection.dialect.name == "mysql"
    timeout_hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
    index_hint = " FORCE INDEX (PRIMARY)" if mysql else ""
    batch_filter = " AND batch_id=:batch_id" if batch_id is not None else ""
    payload_bytes = "OCTET_LENGTH(payload_json)" if mysql else "LENGTH(CAST(payload_json AS BLOB))"
    statement = text(
        f"SELECT {timeout_hint}coverage_id, {payload_bytes} FROM st_pit_source_coverage{index_hint} "
        f"WHERE fact_kind='event' AND source=:source{batch_filter} AND coverage_id IN :coverage_ids"
    ).bindparams(bindparam("coverage_ids", expanding=True))
    params: dict[str, Any] = {"source": source, "coverage_ids": coverage_ids}
    if batch_id is not None:
        params["batch_id"] = batch_id
    with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
        rows = connection.execute(statement, params).tuples().all()
    sizes = dict(rows)
    if len(rows) != len(coverage_ids) or sizes.keys() != set(coverage_ids):
        raise ValueError("batch coverage sizes differ from inventory")
    return sizes


def _read_live_announcement_payload_rows(
    connection: Any, *, source: str, batch_id: str,
    coverage_ids: Sequence[str], deadline: float, stage: str,
) -> list[dict[str, Any]]:
    from server.common.pit_facts import _coverage_payload_columns, _coverage_payload_row

    mysql = connection.dialect.name == "mysql"
    timeout_hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
    index_hint = " FORCE INDEX (PRIMARY)" if mysql else ""
    statement = text(
        f"SELECT {timeout_hint}{_coverage_payload_columns(mysql)} FROM st_pit_source_coverage{index_hint} "
        "WHERE fact_kind='event' AND source=:source AND batch_id=:batch_id AND coverage_id IN :coverage_ids"
    ).bindparams(bindparam("coverage_ids", expanding=True)).execution_options(stream_results=True)
    with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
        with connection.execute(statement, {
            "source": source, "batch_id": batch_id, "coverage_ids": coverage_ids,
        }) as result:
            packed_rows = result.mappings().all()
    return [
        _coverage_payload_row(row, mysql=mysql, stage="announcement_live_batch")
        for row in packed_rows
    ]


def _load_live_announcement_batch_inventory(
    connection: Any,
    *,
    source: str,
    batch_id: str,
    catalog_codes: Sequence[str],
    deadline: float,
) -> list[tuple[str, str]]:
    """Read the exact ordered membership from the caller's batch snapshot."""
    mysql = connection.dialect.name == "mysql"
    codes = sorted(catalog_codes)
    inventory_limit = VALIDATION_INVENTORY_CODE_LIMIT if mysql else max(1, len(codes))
    index_hint = " FORCE INDEX (idx_pit_coverage_decision)" if mysql else ""
    keys = []
    for offset in range(0, max(1, len(codes)), inventory_limit):
        end = offset + inventory_limit
        expected_codes = codes[offset:end]
        params = {"source": source, "batch_id": batch_id, "inventory_limit": len(expected_codes) + 1}
        bounds = ""
        # The first/last ranges have open ends. Every stored code belongs to
        # exactly one range, including unknown codes between catalog members.
        # No requested-code or time filter can hide extra batch rows.
        if offset:
            bounds += " AND stock_code>=:lower_code"
            params["lower_code"] = codes[offset]
        if end < len(codes):
            bounds += " AND stock_code<:upper_code"
            params["upper_code"] = codes[end]
        stage = f"batch-inventory:{offset}" if mysql else "batch-inventory"
        timeout_hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
            range_keys = connection.execute(text(
                f"SELECT {timeout_hint}coverage_id, stock_code FROM st_pit_source_coverage{index_hint} "
                "WHERE fact_kind='event' AND source=:source AND batch_id=:batch_id"
                f"{bounds} ORDER BY stock_code LIMIT :inventory_limit"
            ), params).tuples().all()
        if [str(code).zfill(6) for _, code in range_keys] != expected_codes:
            raise ValueError("batch stock set differs from catalog")
        keys.extend((coverage_id, code) for coverage_id, code in range_keys)
    return keys


def _load_live_announcement_batch_rows(
    connection: Any,
    *,
    source: str,
    batch_id: str,
    catalog_codes: Sequence[str],
    deadline: float,
) -> Iterator[dict[str, Any]]:
    """Prove complete batch membership before bounded primary-key payload reads."""
    keys = _load_live_announcement_batch_inventory(
        connection, source=source, batch_id=batch_id, catalog_codes=catalog_codes, deadline=deadline,
    )
    for offset in range(0, len(keys), VALIDATION_READ_BATCH_SIZE):
        expected = keys[offset : offset + VALIDATION_READ_BATCH_SIZE]
        codes_by_id = dict(expected)
        sizes = _read_live_announcement_payload_sizes(
            connection, source=source, batch_id=batch_id, coverage_ids=list(codes_by_id),
            deadline=deadline, stage=f"batch-payload-size:{offset}",
        )
        sized_keys = [(coverage_id, sizes[coverage_id]) for coverage_id, _ in expected]
        payload_offset = offset
        for payload_keys in _bounded_announcement_payload_keys(sized_keys):
            rows = _read_live_announcement_payload_rows(
                connection, source=source, batch_id=batch_id,
                coverage_ids=[coverage_id for coverage_id, _ in payload_keys],
                deadline=deadline, stage=f"batch-payload:{payload_offset}",
            )
            for (coverage_id, payload_bytes), row in zip(
                payload_keys, sorted(rows, key=lambda row: row["stock_code"]), strict=True,
            ):
                if (
                    coverage_id != row["coverage_id"]
                    or codes_by_id[coverage_id] != row["stock_code"]
                    or payload_bytes != len(row["payload_json"].encode("utf-8"))
                ):
                    raise ValueError("batch coverage row differs from inventory")
                yield row
            payload_offset += len(payload_keys)


def validate_complete_qmt_announcement_batch(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    window_start: date | str,
    window_end: date | str,
    fact_cutoff_at: datetime | str | None = None,
    source: str = QMT_ANNOUNCEMENT_SOURCE,
) -> dict[str, Any]:
    """Prove one atomic, catalog-exact QMT event coverage batch.

    The caller's code set may be a subset (for one screen), but the persisted
    batch itself must contain exactly every catalog-eligible A-share, including
    Beijing instruments.  A partial batch, mixed cutoff or stale batch is never
    accepted.
    """

    from server.common.qmt_stock_catalog import load_stock_catalog

    source_name = str(source or "").strip()
    if source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_SOURCE_INVALID", source_name
        )
    requested_codes = sorted(
        {
            str(code).strip().zfill(6)
            for code in codes
            if str(code).strip()
        }
    )
    if not requested_codes or any(
        not _CODE_RE.fullmatch(code) for code in requested_codes
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_REQUIRED_SCOPE_INVALID"
        )
    decision = _dt(decision_at)
    requested_cutoff = _dt(fact_cutoff_at) if fact_cutoff_at is not None else None
    start = date.fromisoformat(str(window_start)[:10])
    end = date.fromisoformat(str(window_end)[:10])
    if start > end:
        raise ValueError("QMT announcement validation window is invalid")
    deadline = monotonic() + LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    try:
        with engine.connect() as connection:
            mysql = connection.dialect.name == "mysql"
            if mysql:
                with _live_announcement_reply_budget(connection, deadline=deadline, stage="snapshot-isolation"):
                    if str(connection.get_isolation_level()).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                        connection = connection.execution_options(isolation_level="REPEATABLE READ")
            timeout_hint = _live_announcement_read_hint(
                mysql=mysql, deadline=deadline, stage="candidate-discovery",
            )
            statement = text(
                f"SELECT {timeout_hint}batch_id, stock_code, known_at FROM st_pit_source_coverage "
                "WHERE fact_kind='event' AND source=:source "
                "AND watermark_kind='QUERY_CUTOFF' "
                "AND stock_code IN :codes AND known_at<=:decision_at "
                "AND received_at<=:decision_at AND coverage_status='COMPLETE' "
                "AND window_start<=:window_start AND window_end>=:window_end "
                "ORDER BY known_at DESC, batch_id DESC"
            ).bindparams(bindparam("codes", expanding=True))
            with _live_announcement_reply_budget(connection, deadline=deadline, stage="candidate-discovery"):
                required_rows = connection.execute(statement, {
                    "source": source_name,
                    "codes": requested_codes,
                    "decision_at": decision,
                    "window_start": start,
                    "window_end": end,
                }).mappings().all()
            _live_announcement_read_hint(mysql=False, deadline=deadline, stage="candidate-discovery")
            by_batch: dict[str, dict[str, dict[str, Any]]] = {}
            for row in required_rows:
                batch_id = str(row.get("batch_id") or "")
                code = str(row.get("stock_code") or "").zfill(6)
                existing = by_batch.setdefault(batch_id, {}).get(code)
                if existing is None or _dt(row.get("known_at")) > _dt(
                    existing.get("known_at")
                ):
                    by_batch[batch_id][code] = row
            candidates = [
                batch_id for batch_id, rows in by_batch.items()
                if set(rows) == set(requested_codes)
            ]
            candidates.sort(
                key=lambda batch_id: max(
                    _dt(row.get("known_at"))
                    for row in by_batch[batch_id].values()
                ),
                reverse=True,
            )
            failure_codes: list[str] = []
            for candidate in candidates:
                try:
                    timeout_hint = _live_announcement_read_hint(
                        mysql=mysql, deadline=deadline, stage="first-member-inventory",
                    )
                    with _live_announcement_reply_budget(connection, deadline=deadline, stage="first-member-inventory"):
                        first_id = connection.execute(text(
                            f"SELECT {timeout_hint}coverage_id FROM st_pit_source_coverage "
                            "WHERE fact_kind='event' AND source=:source AND batch_id=:batch_id "
                            "ORDER BY stock_code LIMIT 1"
                        ), {"source": source_name, "batch_id": candidate}).scalar_one_or_none()
                    if first_id is None:
                        raise ValueError("batch has no coverage rows")
                    first_rows = _read_live_announcement_payload_rows(
                        connection, source=source_name, batch_id=candidate, coverage_ids=[first_id],
                        deadline=deadline, stage="first-member-payload",
                    )
                    if len(first_rows) != 1 or first_rows[0]["coverage_id"] != first_id:
                        raise ValueError("batch first coverage row differs from inventory")
                    _, first_evidence = _live_announcement_payload_evidence(first_rows[0])
                    incremental_proof = first_evidence.get(
                        "incremental_proof"
                    )
                    if not isinstance(incremental_proof, dict):
                        raise ValueError("batch incremental proof is missing")
                    incremental_core = {
                        key: value
                        for key, value in incremental_proof.items()
                        if key != "chain_root_hash"
                    }
                    if (
                        incremental_proof.get("schema")
                        != "probiga.qmt-announcement-incremental-chain.v1"
                        or incremental_proof.get("mode")
                        not in {"FULL_BASELINE", "DELTA_OVER_PARENT"}
                        or not _SHA256_RE.fullmatch(str(
                            incremental_proof.get("delta_root_hash") or ""
                        ))
                        or not _SHA256_RE.fullmatch(str(
                            incremental_proof.get("result_batch_root_hash")
                            or ""
                        ))
                        or canonical_hash(incremental_core)
                        != incremental_proof.get("chain_root_hash")
                    ):
                        raise ValueError("batch incremental proof differs")
                    event_cutoff = _dt(first_evidence.get("fact_cutoff_at"))
                    received = _dt(first_evidence.get("received_at"))
                    evidence_decision = _dt(first_evidence.get("decision_at"))
                    if (
                        evidence_decision != received
                        or received > decision
                        or received < event_cutoff
                        or decision - event_cutoff < timedelta(0)
                        or (
                            requested_cutoff is not None
                            and event_cutoff < requested_cutoff
                        )
                    ):
                        raise ValueError("batch cutoff/receipt knowledge differs")
                    catalog_batch_id = str(
                        first_evidence.get("catalog_batch_id") or ""
                    )
                    with _live_announcement_reply_budget(connection, deadline=deadline, stage="stock-catalog"):
                        catalog = load_stock_catalog(
                            connection, batch_id=catalog_batch_id, decision_known_at=event_cutoff,
                        )
                    catalog_codes = catalog.eligible_codes(
                        event_cutoff.date().isoformat()
                    )
                    catalog_qmt_by_code = {
                        str(member["stock_code"]).zfill(6): str(
                            member["qmt_code"]
                        ).upper()
                        for member in catalog.members
                    }
                    if not set(requested_codes).issubset(catalog_codes):
                        raise ValueError("required scope is absent from batch")
                    persisted_start = date.fromisoformat(
                        str(first_evidence.get("window_start"))[:10]
                    )
                    persisted_end = date.fromisoformat(
                        str(first_evidence.get("window_end"))[:10]
                    )
                    capture_start = date.fromisoformat(str(
                        incremental_proof.get("capture_window_start") or ""
                    )[:10])
                    requested_start_time = _qmt_time(datetime.combine(
                        capture_start, datetime.min.time()
                    ))
                    requested_end_time = _qmt_time(event_cutoff)
                    entries: list[dict[str, Any]] = []
                    cninfo_directory_anchors: set[tuple[Any, ...]] = set()
                    root = str(first_evidence.get("global_batch_root_hash") or "")
                    for row in _load_live_announcement_batch_rows(
                        connection, source=source_name, batch_id=candidate,
                        catalog_codes=catalog_codes, deadline=deadline,
                    ):
                        payload, evidence = _live_announcement_payload_evidence(row)
                        provider_receipt = evidence.get("provider_receipt")
                        provider_receipt_hash = str(
                            evidence.get("provider_receipt_hash") or ""
                        )
                        fallback_evidence_valid = bool(
                            source_name == QMT_ANNOUNCEMENT_SOURCE
                            or (
                                source_name in {
                                    CNINFO_ANNOUNCEMENT_SOURCE,
                                    EASTMONEY_ANNOUNCEMENT_SOURCE,
                                }
                                and evidence.get("primary_provider")
                                == QMT_ANNOUNCEMENT_SOURCE
                                and str(evidence.get("fallback_reason") or "")
                                in ANNOUNCEMENT_FALLBACK_REASON_CODES
                                and isinstance(provider_receipt, dict)
                                and canonical_hash(provider_receipt)
                                == provider_receipt_hash
                                and provider_receipt.get("schema")
                                == (
                                    CNINFO_PROVIDER_RECEIPT_SCHEMA
                                    if source_name
                                    == CNINFO_ANNOUNCEMENT_SOURCE
                                    else "probiga.announcement-provider-receipt.v1"
                                )
                                and provider_receipt.get("status") == "COMPLETE"
                                and provider_receipt.get("source") == source_name
                                and str(provider_receipt.get("stock_code") or "")
                                == str(row["stock_code"]).zfill(6)
                                and provider_receipt.get("exhausted") is True
                                and int(provider_receipt.get("result_count") or 0)
                                == int(row.get("result_count") or 0)
                                and _fallback_receipt_valid(
                                    provider_receipt,
                                    source=source_name,
                                    stock_code=str(row["stock_code"]).zfill(6),
                                    qmt_code=catalog_qmt_by_code[
                                        str(row["stock_code"]).zfill(6)
                                    ],
                                    requested_start_time=requested_start_time,
                                    requested_end_time=requested_end_time,
                                    result_count=int(
                                        row.get("result_count") or 0
                                    ),
                                    catalog_codes=catalog_codes,
                                )
                            )
                        )
                        if (
                            str(evidence.get("provider") or "") != source_name
                            or not fallback_evidence_valid
                            or str(
                                evidence.get("global_batch_root_hash") or ""
                            ) != root
                            or str(evidence.get("catalog_batch_id") or "")
                            != catalog.batch_id
                            or str(evidence.get("catalog_manifest_hash") or "")
                            != catalog.manifest_hash
                            or str(evidence.get("catalog_member_set_hash") or "")
                            != catalog.member_set_hash
                            or int(evidence.get("catalog_member_count") or 0)
                            != len(catalog_codes)
                            or _dt(evidence.get("fact_cutoff_at")) != event_cutoff
                            or _dt(evidence.get("decision_at")) != received
                            or _dt(evidence.get("received_at")) != received
                            or _dt(row.get("covered_through_at")) != event_cutoff
                            or _dt(row.get("known_at")) != received
                            or _dt(row.get("received_at")) != received
                            or date.fromisoformat(str(row.get("window_start"))[:10])
                            != date.fromisoformat(
                                str(first_evidence.get("window_start"))[:10]
                            )
                            or date.fromisoformat(str(row.get("window_end"))[:10])
                            != date.fromisoformat(
                                str(first_evidence.get("window_end"))[:10]
                            )
                            or str(evidence.get("source_response_hash") or "")
                            != str(row.get("source_response_hash") or "")
                            or evidence.get("incremental_proof")
                            != incremental_proof
                            or int(payload.get("result_count") or 0)
                            != int(row.get("result_count") or 0)
                        ):
                            raise ValueError("batch per-code evidence differs")
                        entry = {
                            "stock_code": str(row["stock_code"]).zfill(6),
                            "source_response_hash": str(
                                row.get("source_response_hash") or ""
                            ),
                            "result_count": int(row.get("result_count") or 0),
                        }
                        if source_name != QMT_ANNOUNCEMENT_SOURCE:
                            entry["provider_receipt_hash"] = provider_receipt_hash
                            if source_name == CNINFO_ANNOUNCEMENT_SOURCE:
                                cninfo_directory_anchors.add((
                                    provider_receipt.get(
                                        "directory_raw_sha256"
                                    ),
                                    provider_receipt.get(
                                        "directory_manifest_hash"
                                    ),
                                    provider_receipt.get(
                                        "directory_member_set_hash"
                                    ),
                                    provider_receipt.get(
                                        "directory_member_count"
                                    ),
                                    provider_receipt.get(
                                        "requested_catalog_member_set_sha256"
                                    ),
                                ))
                        entries.append(entry)
                    if (
                        source_name == CNINFO_ANNOUNCEMENT_SOURCE
                        and len(cninfo_directory_anchors) != 1
                    ):
                        raise ValueError("batch CNInfo directory anchors differ")
                    expected_root = _batch_root_from_entries(
                        batch_id=candidate,
                        fact_cutoff_at=event_cutoff,
                        received_at=received,
                        window_start=persisted_start,
                        window_end=persisted_end,
                        catalog_batch_id=catalog.batch_id,
                        catalog_manifest_hash=catalog.manifest_hash,
                        catalog_member_set_hash=catalog.member_set_hash,
                        entries=entries,
                        source=source_name,
                    )
                    if not _SHA256_RE.fullmatch(root) or root != expected_root:
                        raise ValueError("batch global root differs")
                    if (
                        incremental_proof.get("coverage_window_start")
                        != persisted_start.isoformat()
                        or incremental_proof.get("window_end")
                        != persisted_end.isoformat()
                        or incremental_proof.get("result_batch_root_hash")
                        != root
                        or not persisted_start <= capture_start <= persisted_end
                    ):
                        raise ValueError("batch incremental window differs")
                    if incremental_proof.get("mode") == "FULL_BASELINE":
                        if (
                            capture_start != persisted_start
                            or incremental_proof.get("parent_batch_id") != ""
                            or incremental_proof.get("parent_batch_root_hash")
                            != ""
                            or incremental_proof.get("parent_received_at") != ""
                        ):
                            raise ValueError("full baseline proof differs")
                    else:
                        parent_batch_id = str(
                            incremental_proof.get("parent_batch_id") or ""
                        )
                        parent_root = str(
                            incremental_proof.get("parent_batch_root_hash")
                            or ""
                        )
                        parent_received = _dt(
                            incremental_proof.get("parent_received_at")
                        )
                        timeout_hint = _live_announcement_read_hint(
                            mysql=mysql, deadline=deadline, stage="incremental-parent",
                        )
                        with _live_announcement_reply_budget(connection, deadline=deadline, stage="incremental-parent"):
                            parent_row = connection.execute(
                                text(
                                    f"SELECT {timeout_hint}payload_json FROM "
                                    "st_pit_source_coverage "
                                    "WHERE fact_kind='event' AND source=:source "
                                    "AND batch_id=:batch_id LIMIT 1"
                                ),
                                {"source": source_name, "batch_id": parent_batch_id},
                            ).mappings().first()
                        if (
                            capture_start == persisted_start
                            or not parent_batch_id
                            or not _SHA256_RE.fullmatch(parent_root)
                            or parent_received >= received
                            or parent_row is None
                        ):
                            raise ValueError("incremental parent proof differs")
                        parent_payload = json.loads(
                            str(parent_row.get("payload_json") or "")
                        )
                        parent_evidence = parent_payload.get(
                            "watermark", {}
                        ).get("evidence", {})
                        if parent_evidence.get(
                            "global_batch_root_hash"
                        ) != parent_root:
                            raise ValueError("incremental parent root differs")
                    if (
                        source_name == QMT_ANNOUNCEMENT_SOURCE
                        and len(catalog_codes) >= 100
                        and sum(
                            int(entry.get("result_count") or 0)
                            for entry in entries
                        ) == 0
                    ):
                        # Older publishers could persist the broker's
                        # permission-failure shape as one COMPLETE-but-empty
                        # row per stock.  The live publisher rejects that
                        # shape before writing; the strict reader must apply
                        # the same rule so a legacy batch cannot silently
                        # become strategy evidence.  Check it only after the
                        # immutable root is proven, so corrupt QMT evidence is
                        # never hidden behind a fallback source.
                        raise QMTAnnouncementBlocked(
                            "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
                            candidate,
                        )
                    _live_announcement_read_hint(mysql=False, deadline=deadline, stage="batch-verified")
                    return {
                        "schema": QMT_ANNOUNCEMENT_BATCH_SCHEMA,
                        "status": "COMPLETE",
                        "source": source_name,
                        "primary_source": str(
                            first_evidence.get("primary_provider") or ""
                        ),
                        "fallback_reason": str(
                            first_evidence.get("fallback_reason") or ""
                        ),
                        "batch_id": candidate,
                        "batch_root_hash": root,
                        "incremental_chain_root_hash": incremental_proof[
                            "chain_root_hash"
                        ],
                        "parent_batch_id": incremental_proof[
                            "parent_batch_id"
                        ],
                        "parent_batch_root_hash": incremental_proof[
                            "parent_batch_root_hash"
                        ],
                        "fact_cutoff_at": _dt_text(event_cutoff),
                        "decision_at": _dt_text(received),
                        "received_at": _dt_text(received),
                        "window_start": persisted_start.isoformat(),
                        "capture_window_start": capture_start.isoformat(),
                        "window_end": persisted_end.isoformat(),
                        "catalog_batch_id": catalog.batch_id,
                        "catalog_manifest_hash": catalog.manifest_hash,
                        "catalog_member_set_hash": catalog.member_set_hash,
                        "catalog_member_count": len(catalog_codes),
                    }
                except QMTAnnouncementBlocked as exc:
                    if exc.reason_code in {
                        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE,
                        LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE,
                        LIVE_ANNOUNCEMENT_BATCH_REPLY_FAILED,
                    }:
                        raise
                    _live_announcement_read_hint(mysql=False, deadline=deadline, stage="candidate-read")
                    failure_codes.append(exc.reason_code)
                except Exception as exc:
                    _live_announcement_read_hint(mysql=False, deadline=deadline, stage="candidate-read")
                    failure_codes.append(type(exc).__name__)
            if failure_codes and set(failure_codes) == {
                "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN"
            }:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
                    "legacy-complete-batch",
                )
            detail = ",".join(failure_codes[:10]) or "no-common-batch"
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND", detail
            )
    except QMTAnnouncementBlocked:
        raise
    except Exception as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_VALIDATION_FAILED", type(exc).__name__
        ) from exc


def validate_complete_historical_reconstruction_batch(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    window_start: date | str,
    window_end: date | str,
    expected_trade_date: date | str | None = None,
    capture_historical_selection: Callable[[HistoricalAnnouncementSelection], None] | None = None,
) -> dict[str, Any]:
    """Validate one exact post-hoc CNINFO batch and its actual knowledge time."""

    from server.common.pit_facts import (
        _coverage_payload_columns,
        _coverage_payload_row,
        _validate_coverage_chain,
    )
    from server.common.qmt_stock_catalog import load_stock_catalog

    requested = sorted({
        str(code).strip().zfill(6) for code in codes if str(code).strip()
    })
    decision = _dt(decision_at)
    start = date.fromisoformat(str(window_start)[:10])
    end = date.fromisoformat(str(window_end)[:10])
    target = date.fromisoformat(
        str(expected_trade_date or end)[:10]
    )
    if (
        not requested
        or any(_CODE_RE.fullmatch(code) is None for code in requested)
        or start > end
        or end != target
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_SCOPE_INVALID"
        )
    try:
        with engine.connect() as connection:
            dialect = str(connection.dialect.name or "").lower()
            if dialect == "mysql" and str(
                connection.get_isolation_level() or ""
            ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError(
                    "announcement reconstruction requires a repeatable-read snapshot"
                )
            mysql = dialect == "mysql"
            payload_columns = _coverage_payload_columns(mysql)
            statement = text(
                f"SELECT {payload_columns} FROM st_pit_source_coverage "
                "WHERE fact_kind='event' AND source=:source "
                "AND stock_code IN :codes AND known_at<=:decision_at "
                "AND received_at<=:decision_at "
                "ORDER BY stock_code, scope_hash, revision_no"
            ).bindparams(bindparam("codes", expanding=True)).execution_options(stream_results=True)
            # Every valid candidate must cover this required stock. If its
            # historical row is absent, reading unrelated payload chains for
            # the entire universe cannot establish a complete batch.
            first_code_has_coverage = connection.execute(
                text(
                    "SELECT /*+ MAX_EXECUTION_TIME(2500) */ 1 "
                    "FROM st_pit_source_coverage "
                    "WHERE fact_kind='event' AND source=:source "
                    "AND stock_code=:stock_code "
                    "AND watermark_kind='HISTORICAL_RECONSTRUCTION' "
                    "AND coverage_status='COMPLETE' "
                    "AND window_start<=:window_start AND window_end>=:window_end "
                    "AND known_at<=:decision_at AND received_at<=:decision_at "
                    "LIMIT 1"
                ),
                {
                    "source": CNINFO_ANNOUNCEMENT_SOURCE,
                    "stock_code": requested[0],
                    "window_start": start,
                    "window_end": end,
                    "decision_at": decision,
                },
            ).scalar_one_or_none()
            if first_code_has_coverage is None:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND", "no-common-batch"
                )
            by_batch: dict[str, dict[str, datetime]] = {}
            scope_heads: dict[str, str] = {}
            candidate_links: dict[str, dict[str, tuple[str, str]]] = {}
            for offset in range(0, len(requested), VALIDATION_READ_BATCH_SIZE):
                code_batch = requested[
                    offset : offset + VALIDATION_READ_BATCH_SIZE
                ]
                with connection.execute(
                    statement,
                    {
                        "source": CNINFO_ANNOUNCEMENT_SOURCE,
                        "codes": code_batch,
                        "decision_at": decision,
                    },
                ) as rows:
                    for _, scope_rows in groupby(
                        rows.mappings(),
                        key=lambda row: (row["stock_code"], row["scope_hash"]),
                    ):
                        chain = [
                            _coverage_payload_row(row, mysql=mysql, stage="announcement_history")
                            for row in scope_rows
                        ]
                        _validate_coverage_chain(chain)
                        if capture_historical_selection is not None:
                            head = chain[-1]
                            scope_heads[str(head["stock_code"]).zfill(6)] = str(head["coverage_id"])
                        # A later live/query-cutoff revision must not hide an
                        # earlier immutable reconstruction for this exact target.
                        for candidate in chain:
                            if (
                                candidate.get("watermark_kind")
                                == "HISTORICAL_RECONSTRUCTION"
                                and str(candidate.get("coverage_status") or "")
                                == "COMPLETE"
                                and date.fromisoformat(
                                    str(candidate.get("window_start"))[:10]
                                ) <= start
                                and date.fromisoformat(
                                    str(candidate.get("window_end"))[:10]
                                ) >= end
                            ):
                                batch_id = str(candidate.get("batch_id") or "")
                                code = str(candidate.get("stock_code") or "").zfill(6)
                                by_batch.setdefault(batch_id, {})[code] = _dt(
                                    candidate.get("known_at")
                                )
                                if capture_historical_selection is not None:
                                    candidate_links.setdefault(batch_id, {})[code] = (
                                        str(candidate["coverage_id"]),
                                        str(candidate["scope_hash"]),
                                    )
            candidates = [
                batch_id for batch_id, batch_codes in by_batch.items()
                if set(batch_codes) == set(requested)
            ]
            candidates.sort(
                key=lambda batch_id: max(by_batch[batch_id].values()),
                reverse=True,
            )
            failure_codes: list[str] = []
            for batch_id in candidates:
                try:
                    batch_rows = [
                        dict(row)
                        for row in connection.execute(
                            text(
                                "SELECT stock_code, window_start, window_end, "
                                "result_count FROM st_pit_source_coverage "
                                "WHERE fact_kind='event' AND source=:source "
                                "AND batch_id=:batch_id ORDER BY stock_code"
                            ),
                            {
                                "source": CNINFO_ANNOUNCEMENT_SOURCE,
                                "batch_id": batch_id,
                            },
                        ).mappings()
                    ]
                    if not batch_rows:
                        raise ValueError("reconstruction batch is empty")
                    first_row = connection.execute(
                        text(
                            "SELECT payload_json FROM st_pit_source_coverage "
                            "WHERE fact_kind='event' AND source=:source "
                            "AND batch_id=:batch_id AND stock_code=:stock_code LIMIT 1"
                        ),
                        {
                            "source": CNINFO_ANNOUNCEMENT_SOURCE,
                            "batch_id": batch_id,
                            "stock_code": batch_rows[0]["stock_code"],
                        },
                    ).mappings().first()
                    if first_row is None:
                        raise ValueError("reconstruction batch is empty")
                    first_raw = str(first_row.get("payload_json") or "")
                    first_payload = json.loads(first_raw)
                    if canonical_json(first_payload) != first_raw:
                        raise ValueError("coverage payload is not canonical")
                    first_evidence = first_payload.get("watermark", {}).get(
                        "evidence"
                    )
                    if not isinstance(first_evidence, dict):
                        raise ValueError("reconstruction evidence is absent")
                    batch_starts = {
                        date.fromisoformat(str(row.get("window_start"))[:10])
                        for row in batch_rows
                    }
                    batch_ends = {
                        date.fromisoformat(str(row.get("window_end"))[:10])
                        for row in batch_rows
                    }
                    if len(batch_starts) != 1 or len(batch_ends) != 1:
                        raise ValueError("reconstruction batch window differs")
                    batch_start = next(iter(batch_starts))
                    batch_end = next(iter(batch_ends))
                    if batch_start > start or batch_end != end:
                        raise ValueError("reconstruction batch does not cover request")
                    provenance = first_evidence.get(
                        "reconstruction_provenance"
                    )
                    if not isinstance(provenance, dict):
                        raise ValueError("reconstruction provenance is absent")
                    del first_raw, first_payload, first_evidence, first_row
                    provenance_core = {
                        key: value
                        for key, value in provenance.items()
                        if key != "reconstruction_sha256"
                    }
                    reconstructed = _dt(provenance.get("reconstructed_at"))
                    cutoff = _dt(provenance.get("source_query_cutoff_at"))
                    authority = provenance.get("authority")
                    if (
                        provenance.get("schema")
                        != QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA
                        or provenance.get("mode")
                        != "HISTORICAL_RECONSTRUCTION"
                        or canonical_hash(provenance_core)
                        != provenance.get("reconstruction_sha256")
                        or provenance.get("target_trade_date")
                        != target.isoformat()
                        or provenance.get("query_window_start")
                        != batch_start.isoformat()
                        or provenance.get("query_window_end")
                        != batch_end.isoformat()
                        or cutoff.date() != target
                        or cutoff.time() != datetime.max.time()
                        or reconstructed > decision
                        or _dt(provenance.get("known_at")) != reconstructed
                        or provenance.get("provider")
                        != CNINFO_ANNOUNCEMENT_SOURCE
                        or provenance.get("source")
                        != CNINFO_ANNOUNCEMENT_SOURCE
                        or re.fullmatch(
                            r"[0-9a-f]{32}",
                            str(provenance.get("scheduler_run_uid") or ""),
                        ) is None
                        or re.fullmatch(
                            r"[0-9a-f]{40}",
                            str(provenance.get("build_sha") or ""),
                        ) is None
                        or provenance.get("build_sha") == "0" * 40
                        or provenance.get("zero_result_receipts_complete")
                        is not True
                        or provenance.get("automatic_real_order_submission")
                        is not False
                        or provenance.get("real_order_authority") is not False
                        or not isinstance(authority, dict)
                        or not _persisted_reconstruction_execution_origin_matches(
                            provenance,
                            authority,
                        )
                    ):
                        raise ValueError("reconstruction provenance differs")
                    catalog = load_stock_catalog(
                        connection,
                        batch_id=str(provenance.get("catalog_batch_id") or ""),
                        decision_known_at=decision,
                    )
                    catalog_codes = catalog.eligible_codes(target.isoformat())
                    row_codes = [
                        str(row.get("stock_code") or "").zfill(6)
                        for row in batch_rows
                    ]
                    if (
                        row_codes != sorted(catalog_codes)
                        or not set(requested).issubset(row_codes)
                        or provenance.get("catalog_batch_id") != catalog.batch_id
                        or provenance.get("catalog_manifest_hash")
                        != catalog.manifest_hash
                        or provenance.get("catalog_member_set_hash")
                        != catalog.member_set_hash
                        or provenance.get("catalog_member_count")
                        != len(catalog_codes)
                        or provenance.get("catalog_codes_sha256")
                        != canonical_hash(catalog_codes)
                        or authority.get("catalog_batch_id") != catalog.batch_id
                        or authority.get("catalog_manifest_hash")
                        != catalog.manifest_hash
                        or authority.get("catalog_member_set_hash")
                        != catalog.member_set_hash
                        or authority.get("catalog_member_count")
                        != len(catalog_codes)
                    ):
                        raise ValueError("reconstruction catalog differs")
                    qmt_by_code = {
                        str(member["stock_code"]).zfill(6): str(
                            member["qmt_code"]
                        ).upper()
                        for member in catalog.members
                    }
                    authority_catalog = AnnouncementCatalog(
                        batch_id=catalog.batch_id,
                        manifest_hash=catalog.manifest_hash,
                        member_set_hash=catalog.member_set_hash,
                        codes=tuple(catalog_codes),
                        qmt_by_code=qmt_by_code,
                    )
                    if not _reconstruction_authority_matches(
                        authority,
                        target_trade_date=target,
                        catalog=authority_catalog,
                        known_at=reconstructed,
                    ):
                        raise ValueError("reconstruction authority differs")
                    additions = authority["reconciliation"]["added_to_prior"]
                    if additions:
                        from server.common.qmt_datadir_daily_coverage import load_daily_manifest

                        truth = authority["qmt_daily_truth"]
                        native = load_daily_manifest(connection, target.isoformat(), decision_known_at=reconstructed,
                                                     manifest_hash=truth["truth_hash"], verify_rows=False)
                        entities = {item["stock_code"]: item for item in (native or {}).get("entities", ())}
                        if any(entities.get(item["stock_code"]) != item["native_daily_entity"] for item in additions):
                            raise ValueError("reconstruction added listing native trade proof differs")
                    requested_start = _qmt_time(
                        datetime.combine(batch_start, datetime.min.time())
                    )
                    requested_end = _qmt_time(cutoff)
                    entries: list[dict[str, Any]] = []
                    roots: set[str] = set()
                    incremental_hashes: set[str] = set()
                    shared_incremental: dict[str, Any] | None = None
                    payload_row_count = 0
                    members: dict[str, tuple[Mapping[str, Any], bool]] = {}
                    security_master = provenance.get("security_master")
                    if not isinstance(security_master, dict):
                        raise ValueError("security master proof is absent")
                    master_started = _dt(security_master.get("started_at"))
                    master_ended = _dt(security_master.get("ended_at"))
                    reconstruction_started = _dt(
                        provenance.get("reconstruction_started_at")
                    )
                    if not (
                        reconstruction_started <= master_started
                        <= master_ended <= reconstructed
                    ):
                        raise ValueError("security master timing differs")
                    batch_payload_statement = text(
                        f"SELECT {payload_columns} FROM st_pit_source_coverage "
                        "WHERE fact_kind='event' AND source=:source "
                        "AND batch_id=:batch_id AND stock_code IN :codes "
                        "ORDER BY stock_code"
                    ).bindparams(bindparam("codes", expanding=True))
                    for offset in range(
                        0,
                        len(catalog_codes),
                        VALIDATION_READ_BATCH_SIZE,
                    ):
                        code_batch = catalog_codes[
                            offset : offset + VALIDATION_READ_BATCH_SIZE
                        ]
                        for raw_row in connection.execute(
                            batch_payload_statement,
                            {
                                "source": CNINFO_ANNOUNCEMENT_SOURCE,
                                "batch_id": batch_id,
                                "codes": code_batch,
                            },
                        ).mappings():
                            row = _coverage_payload_row(raw_row, mysql=mysql, stage="announcement_history")
                            raw = str(row.get("payload_json") or "")
                            payload = json.loads(raw)
                            if canonical_json(payload) != raw:
                                raise ValueError("coverage payload is not canonical")
                            evidence = payload.get("watermark", {}).get("evidence")
                            if not isinstance(evidence, dict):
                                raise ValueError("reconstruction evidence is absent")
                            code = str(row.get("stock_code") or "").zfill(6)
                            receipt = evidence.get("provider_receipt")
                            receipt_hash = str(
                                evidence.get("provider_receipt_hash") or ""
                            )
                            incremental = evidence.get("incremental_proof")
                            source_rows = payload.get("source_rows")
                            if not isinstance(receipt, dict):
                                raise ValueError("provider receipt is absent")
                            receipt_captured = _dt(receipt.get("captured_at"))
                            if (
                                evidence.get("reconstruction_provenance") != provenance
                                or _dt(row.get("known_at")) != reconstructed
                                or _dt(row.get("received_at")) != reconstructed
                                or _dt(row.get("covered_through_at")) != cutoff
                                or canonical_hash(receipt) != receipt_hash
                                or not isinstance(source_rows, list)
                                or not master_started <= receipt_captured <= master_ended
                                or receipt.get("security_master_started_at")
                                != security_master.get("started_at")
                                or receipt.get("security_master_ended_at")
                                != security_master.get("ended_at")
                                or receipt.get("directory_raw_sha256")
                                != security_master.get("directory_raw_sha256")
                                or receipt.get("directory_manifest_hash")
                                != security_master.get("directory_manifest_hash")
                                or receipt.get("directory_member_set_hash")
                                != security_master.get("directory_member_set_hash")
                                or receipt.get("directory_member_count")
                                != security_master.get("directory_member_count")
                                or receipt.get("directory_attestation_sha256")
                                != security_master.get("directory_attestation_sha256")
                                or receipt.get("requested_catalog_member_set_sha256")
                                != security_master.get(
                                    "requested_catalog_member_set_sha256"
                                )
                                or any(
                                    not isinstance(item, dict)
                                    or not str(item.get("source_event_id") or "")
                                    or not _SHA256_RE.fullmatch(
                                        str(item.get("source_row_hash") or "")
                                    )
                                    or not _source_publication_within_window(
                                        item,
                                        window_start=batch_start,
                                        fact_cutoff_at=cutoff,
                                    )
                                    for item in source_rows
                                )
                                or not _fallback_receipt_valid(
                                    receipt,
                                    source=CNINFO_ANNOUNCEMENT_SOURCE,
                                    stock_code=code,
                                    qmt_code=qmt_by_code[code],
                                    requested_start_time=requested_start,
                                    requested_end_time=requested_end,
                                    result_count=int(row.get("result_count") or 0),
                                    catalog_codes=catalog_codes,
                                )
                                or not isinstance(incremental, dict)
                            ):
                                raise ValueError("reconstruction member differs")
                            roots.add(
                                str(evidence.get("global_batch_root_hash") or "")
                            )
                            incremental_hashes.add(canonical_hash(incremental))
                            if shared_incremental is None:
                                shared_incremental = incremental
                            entries.append({
                                "stock_code": code,
                                "source_response_hash": str(
                                    row.get("source_response_hash") or ""
                                ),
                                "result_count": int(row.get("result_count") or 0),
                                "provider_receipt_hash": receipt_hash,
                            })
                            if code in scope_heads:
                                coverage_id = str(row["coverage_id"])
                                if candidate_links[batch_id].get(code) != (
                                    coverage_id, str(row["scope_hash"]),
                                ):
                                    raise ValueError("reconstruction selection coverage identity differs")
                                members[code] = (
                                    MappingProxyType({
                                        key: value for key, value in row.items()
                                        if key != "payload_json"
                                    }),
                                    coverage_id == scope_heads[code],
                                )
                            payload_row_count += 1
                    if (
                        payload_row_count != len(batch_rows)
                        or len(roots) != 1
                        or len(incremental_hashes) != 1
                        or shared_incremental is None
                    ):
                        raise ValueError("reconstruction batch roots differ")
                    root = next(iter(roots))
                    incremental = shared_incremental
                    expected_root = _batch_root_from_entries(
                        batch_id=batch_id,
                        fact_cutoff_at=cutoff,
                        received_at=reconstructed,
                        window_start=batch_start,
                        window_end=batch_end,
                        catalog_batch_id=catalog.batch_id,
                        catalog_manifest_hash=catalog.manifest_hash,
                        catalog_member_set_hash=catalog.member_set_hash,
                        entries=entries,
                        source=CNINFO_ANNOUNCEMENT_SOURCE,
                        reconstruction_provenance=provenance,
                    )
                    if (
                        not _SHA256_RE.fullmatch(root)
                        or root != expected_root
                        or incremental.get("mode") != "FULL_BASELINE"
                        or incremental.get("coverage_window_start")
                        != batch_start.isoformat()
                        or incremental.get("capture_window_start")
                        != batch_start.isoformat()
                        or incremental.get("window_end") != batch_end.isoformat()
                        or incremental.get("parent_batch_id") != ""
                        or incremental.get("parent_batch_root_hash") != ""
                        or incremental.get("parent_received_at") != ""
                        or incremental.get("result_batch_root_hash") != root
                        or canonical_hash({
                            key: value
                            for key, value in incremental.items()
                            if key != "chain_root_hash"
                        }) != incremental.get("chain_root_hash")
                    ):
                        raise ValueError("reconstruction batch root differs")
                    if capture_historical_selection is not None and len(members) != len(requested):
                        raise ValueError("reconstruction selection does not cover requested stocks")
                    batch = {
                        "schema": QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA,
                        "status": "COMPLETE",
                        "mode": "HISTORICAL_RECONSTRUCTION",
                        "source": CNINFO_ANNOUNCEMENT_SOURCE,
                        "primary_source": QMT_ANNOUNCEMENT_SOURCE,
                        "fallback_reason": (
                            "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED"
                        ),
                        "batch_id": batch_id,
                        "batch_root_hash": root,
                        "incremental_chain_root_hash": incremental[
                            "chain_root_hash"
                        ],
                        "fact_cutoff_at": _dt_text(cutoff),
                        "source_query_cutoff_at": _dt_text(cutoff),
                        "decision_at": _dt_text(reconstructed),
                        "received_at": _dt_text(reconstructed),
                        "reconstructed_at": _dt_text(reconstructed),
                        "window_start": batch_start.isoformat(),
                        "capture_window_start": batch_start.isoformat(),
                        "window_end": batch_end.isoformat(),
                        "catalog_batch_id": catalog.batch_id,
                        "catalog_manifest_hash": catalog.manifest_hash,
                        "catalog_member_set_hash": catalog.member_set_hash,
                        "catalog_member_count": len(catalog_codes),
                        "coverage_count": len(batch_rows),
                        "event_count": sum(
                            int(row.get("result_count") or 0)
                            for row in batch_rows
                        ),
                        "empty_stock_count": sum(
                            int(row.get("result_count") or 0) == 0
                            for row in batch_rows
                        ),
                        "reconstruction_provenance": provenance,
                        "reconstruction_sha256": provenance[
                            "reconstruction_sha256"
                        ],
                    }
                except SQLAlchemyError:
                    # Transport failure is not evidence that the batch is absent.
                    raise
                except Exception as exc:
                    failure_codes.append(type(exc).__name__)
                else:
                    if capture_historical_selection is not None:
                        capture_historical_selection(HistoricalAnnouncementSelection(
                            engine=engine,
                            decision_at=decision,
                            window_start=start,
                            window_end=end,
                            batch=MappingProxyType(deepcopy(batch)),
                            members=MappingProxyType(members),
                        ))
                    return batch
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND",
                ",".join(failure_codes[:10]) or "no-common-batch",
            )
    except QMTAnnouncementBlocked:
        raise
    except Exception as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_VALIDATION_FAILED", type(exc).__name__
        ) from exc


def validate_complete_announcement_batch(
    engine: Engine,
    *,
    capture_historical_selection: Callable[[HistoricalAnnouncementSelection], None] | None = None,
    capture_live_reuse: Callable[[], None] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Prefer QMT and use a fully proven fallback batch only when absent.

    A malformed or tampered QMT candidate is never hidden by a fallback.  The
    fallback search starts only when there is no common QMT batch, or when all
    otherwise-valid QMT candidates are the isolated legacy full-market-empty
    failure shape rejected by the current publisher.
    """

    try:
        proof = validate_complete_qmt_announcement_batch(
            engine, source=QMT_ANNOUNCEMENT_SOURCE, **kwargs
        )
    except QMTAnnouncementBlocked as exc:
        qmt_absent = (
            exc.reason_code == "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
            and exc.detail == "no-common-batch"
        )
        legacy_all_empty = (
            exc.reason_code
            == "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN"
            and exc.detail == "legacy-complete-batch"
        )
        if not (qmt_absent or legacy_all_empty):
            raise
        if legacy_all_empty:
            # The downstream cutoff can reject a legacy QMT candidate before
            # the all-empty check. Keep that case on the explicit T validation.
            capture_historical_selection = None
            capture_live_reuse = None
    else:
        if capture_live_reuse is not None:
            capture_live_reuse()
        return proof
    for fallback_source in (
        CNINFO_ANNOUNCEMENT_SOURCE,
        EASTMONEY_ANNOUNCEMENT_SOURCE,
    ):
        try:
            proof = validate_complete_qmt_announcement_batch(
                engine, source=fallback_source, **kwargs
            )
        except QMTAnnouncementBlocked as exc:
            if not (
                exc.reason_code
                == "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
                and exc.detail == "no-common-batch"
            ):
                raise
            if fallback_source == CNINFO_ANNOUNCEMENT_SOURCE:
                try:
                    return validate_complete_historical_reconstruction_batch(
                        engine,
                        codes=kwargs.get("codes") or (),
                        decision_at=kwargs.get("decision_at"),
                        window_start=kwargs.get("window_start"),
                        window_end=kwargs.get("window_end"),
                        expected_trade_date=kwargs.get("window_end"),
                        capture_historical_selection=capture_historical_selection,
                    )
                except QMTAnnouncementBlocked as historical_exc:
                    if not (
                        historical_exc.reason_code
                        == "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
                        and historical_exc.detail == "no-common-batch"
                    ):
                        raise
        else:
            if capture_live_reuse is not None:
                capture_live_reuse()
            return proof
    raise QMTAnnouncementBlocked(
        "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND", "no-common-batch"
    )


def _assert_pit_fact_schema_prepared(engine: Engine) -> None:
    """Fail before any provider I/O when the immutable PIT sink is unavailable."""

    from server.common.pit_facts import (
        PIT_FACT_TRIGGER_STATEMENTS,
        pit_fact_schema_health,
    )

    production = (
        os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
        == "production"
    )
    if production:
        connection_scope = engine.connect()
    else:
        connection_scope = nullcontext(engine)
    with connection_scope as connection:
        health = pit_fact_schema_health(connection)
        confirmed_health: dict[str, Any] | None = None
        seal: dict[str, Any] = {}
        if production:
            try:
                from server.engine.strategy_governance import (
                    validate_privileged_trigger_migration_seal,
                )

                seal = validate_privileged_trigger_migration_seal(connection)
                confirmed_health = pit_fact_schema_health(connection)
            except Exception:
                seal = {}
    if health.get("valid") and not production:
        return

    # Runtime metadata may expose all triggers or hide all of them.  Permission
    # enumeration is not a health gate, so accept either exact physical shape
    # only when the same runtime-readable, build-bound migration seal used by
    # strategy governance validates.  Partial visibility, table/column drift,
    # or a stale build still fail closed before any provider request.
    seal_valid = False
    exact_visible = (
        health.get("schema") == "probiga.pit-fact-schema-health.v1"
        and health.get("status") == "HEALTHY"
        and health.get("valid") is True
        and int(health.get("table_count") or 0) == 3
        and not health.get("missing_tables")
        and not health.get("missing_columns")
        and int(health.get("trigger_count") or 0)
        == len(PIT_FACT_TRIGGER_STATEMENTS)
        and not health.get("missing_triggers")
    )
    exact_hidden = (
        health.get("schema") == "probiga.pit-fact-schema-health.v1"
        and health.get("status") == "NOT_READY"
        and health.get("valid") is False
        and int(health.get("table_count") or 0) == 3
        and not health.get("missing_tables")
        and not health.get("missing_columns")
        and int(health.get("trigger_count") or 0) == 0
        and set(health.get("missing_triggers") or ())
        == set(PIT_FACT_TRIGGER_STATEMENTS)
    )
    if (
        production
        and (exact_visible or exact_hidden)
    ):
        try:
            from server.engine.strategy_governance import (
                PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH,
                validate_privileged_trigger_seal_payload,
            )
            from server.common.component_release import runtime_contract_build_sha
            validate_privileged_trigger_seal_payload(
                seal,
                expected_build_sha=runtime_contract_build_sha(),
            )
            seal_valid = bool(
                confirmed_health == health
                and health.get("contract_hash")
                == PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH
            )
        except Exception:
            seal_valid = False
    if not seal_valid:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_PIT_SCHEMA_NOT_PREPARED",
            str(health.get("status") or "NOT_READY"),
        )


def assert_announcement_pit_schema_prepared(engine: Engine) -> None:
    """Public read-only preflight shared by authoritative event collectors."""

    _assert_pit_fact_schema_prepared(engine)


def _publish_error_detail(
    phase: str, exc: Exception, *, connection: Any = None, commit_state: str,
) -> str:
    original = getattr(exc, "orig", None)
    errno = getattr(original, "errno", None)
    if type(errno) is not int:
        args = getattr(original, "args", ())
        errno = args[0] if args else None
    errno = errno if type(errno) is int and 0 < errno <= 65535 else "unknown"
    invalidated = bool(
        getattr(exc, "connection_invalidated", False)
        or getattr(connection, "invalidated", False)
    )
    return (
        f"db_phase={phase};error_type={type(exc).__name__};mysql_errno={errno};"
        f"connection_invalidated={str(invalidated).lower()};commit_state={commit_state}"
    )


def _finish_publish_connection(
    connection: Any, *, lock_acquired: bool, lock_uncertain: bool, commit_state: str,
) -> None:
    phase = "rollback"
    try:
        if connection.invalidated:
            return
        if lock_uncertain:
            phase = "invalidate_uncertain_lock"
            connection.invalidate()
            return
        if connection.in_transaction():
            connection.rollback()
        if lock_acquired and not connection.invalidated:
            phase = "release_lock"
            released = connection.execute(
                text("SELECT RELEASE_LOCK('probiga:qmt-announcement-pit')")
            ).scalar_one()
            if released != 1:
                raise RuntimeError("publisher lock release was not confirmed")
            phase = "release_lock_transaction_end"
            connection.commit()
    except Exception as exc:
        logger.warning("Announcement publisher cleanup: %s", _publish_error_detail(
            phase, exc, connection=connection, commit_state=commit_state,
        ))
        try:
            # Never return a physical session that may still own the lock.
            connection.invalidate()
        except Exception as invalidate_exc:
            logger.warning("Announcement publisher cleanup: %s", _publish_error_detail(
                "invalidate", invalidate_exc, connection=connection, commit_state=commit_state,
            ))
    finally:
        try:
            connection.close()
        except Exception as close_exc:
            logger.warning("Announcement publisher cleanup: %s", _publish_error_detail(
                "close", close_exc, connection=connection, commit_state=commit_state,
            ))


def _publish_batch(
    engine: Engine,
    *,
    batch_id: str,
    batch_root_hash: str,
    entries: Sequence[Mapping[str, Any]],
    fact_cutoff_at: datetime,
    received_at: datetime,
    window_start: date,
    window_end: date,
    catalog: AnnouncementCatalog,
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    source: str = QMT_ANNOUNCEMENT_SOURCE,
    fallback_reason: str = "",
    provider_receipts: Mapping[str, Mapping[str, Any]] | None = None,
    incremental_proof: Mapping[str, Any] | None = None,
    reconstruction_provenance: Mapping[str, Any] | None = None,
    deadline_at: datetime | None = None,
    deadline_monotonic: float | None = None,
    now_fn: Callable[[], datetime] = datetime.now,
) -> tuple[list[str], datetime]:
    from server.common.pit_facts import (
        append_event_revisions,
        append_source_coverages,
    )

    # Keep the transactional boundary guarded as a defence-in-depth recheck;
    # synchronize_qmt_announcements performs the same check before network I/O.
    try:
        _assert_pit_fact_schema_prepared(engine)
    except SQLAlchemyError as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_PUBLISH_DATABASE_ERROR",
            _publish_error_detail("schema_preflight", exc, commit_state="not_committed"),
        ) from None
    response_hashes = {
        str(item["stock_code"]): str(item["source_response_hash"])
        for item in entries
    }
    coverage_ids: list[str] = []
    source_name = str(source or "").strip()
    fallback_code = str(fallback_reason or "").strip()
    reconstruction = dict(reconstruction_provenance or {})
    reconstruction_mode = bool(reconstruction)
    if source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_SOURCE_INVALID", source_name
        )
    if source_name != QMT_ANNOUNCEMENT_SOURCE and not (
        fallback_code in ANNOUNCEMENT_FALLBACK_REASON_CODES
        or (
            reconstruction_mode
            and fallback_code
            == "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED"
        )
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_FALLBACK_REASON_INVALID", fallback_code
        )
    receipts = provider_receipts or {}
    if reconstruction_mode:
        if source_name != CNINFO_ANNOUNCEMENT_SOURCE:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_SOURCE_INVALID"
            )
        expected_reconstruction_hash = str(
            reconstruction.get("reconstruction_sha256") or ""
        )
        reconstruction_core = {
            key: value
            for key, value in reconstruction.items()
            if key != "reconstruction_sha256"
        }
        try:
            reconstruction_target = date.fromisoformat(str(
                reconstruction.get("target_trade_date") or ""
            ))
            reconstruction_started = _dt(
                reconstruction.get("reconstruction_started_at")
            )
            reconstructed_at = _dt(reconstruction.get("reconstructed_at"))
            security_master = reconstruction.get("security_master")
            if not isinstance(security_master, Mapping):
                raise ValueError("security master proof is absent")
            master_started = _dt(security_master.get("started_at"))
            master_ended = _dt(security_master.get("ended_at"))
        except (TypeError, ValueError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_PROVENANCE_INVALID"
            ) from exc
        if (
            reconstruction.get("schema")
            != QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA
            or reconstruction.get("mode") != "HISTORICAL_RECONSTRUCTION"
            or canonical_hash(reconstruction_core)
            != expected_reconstruction_hash
            or _dt(reconstruction.get("source_query_cutoff_at"))
            != fact_cutoff_at
            or _dt(reconstruction.get("reconstructed_at")) != received_at
            or _dt(reconstruction.get("known_at")) != received_at
            or reconstruction_target != window_end
            or reconstruction.get("provider") != CNINFO_ANNOUNCEMENT_SOURCE
            or reconstruction.get("source") != CNINFO_ANNOUNCEMENT_SOURCE
            or re.fullmatch(
                r"[0-9a-f]{32}",
                str(reconstruction.get("scheduler_run_uid") or ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(reconstruction.get("build_sha") or ""),
            ) is None
            or reconstruction.get("build_sha") == "0" * 40
            or reconstruction.get("automatic_real_order_submission") is not False
            or reconstruction.get("real_order_authority") is not False
            or not _persisted_reconstruction_execution_origin_matches(
                reconstruction,
                reconstruction.get("authority") or {},
            )
            or not reconstruction_started <= master_started
            <= master_ended <= reconstructed_at
            or not _reconstruction_authority_matches(
                reconstruction.get("authority") or {},
                target_trade_date=reconstruction_target,
                catalog=catalog,
                known_at=reconstructed_at,
            )
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_PROVENANCE_INVALID"
            )
    incremental = dict(incremental_proof or {})
    if not incremental:
        incremental = {
            "schema": "probiga.qmt-announcement-incremental-chain.v1",
            "mode": "FULL_BASELINE",
            "coverage_window_start": window_start.isoformat(),
            "capture_window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "parent_batch_id": "",
            "parent_batch_root_hash": "",
            "parent_received_at": "",
            "delta_root_hash": batch_root_hash,
            "result_batch_root_hash": batch_root_hash,
        }
        incremental["chain_root_hash"] = canonical_hash(incremental)
    if (
        incremental.get("schema")
        != "probiga.qmt-announcement-incremental-chain.v1"
        or not _SHA256_RE.fullmatch(
            str(incremental.get("chain_root_hash") or "")
        )
        or canonical_hash({
            key: value
            for key, value in incremental.items()
            if key != "chain_root_hash"
        })
        != incremental.get("chain_root_hash")
        or incremental.get("result_batch_root_hash") != batch_root_hash
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_INCREMENTAL_PROOF_INVALID"
        )
    if source_name != QMT_ANNOUNCEMENT_SOURCE and set(receipts) != set(
        catalog.codes
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_FALLBACK_RECEIPTS_INCOMPLETE"
        )
    if reconstruction_mode:
        security_master = reconstruction["security_master"]
        for code in catalog.codes:
            receipt = receipts.get(code)
            if not isinstance(receipt, Mapping):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPT_INVALID", code
                )
            try:
                receipt_captured = _dt(receipt.get("captured_at"))
            except (TypeError, ValueError) as exc:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPT_INVALID", code
                ) from exc
            if (
                not master_started <= receipt_captured <= master_ended
                or receipt.get("security_master_started_at")
                != security_master.get("started_at")
                or receipt.get("security_master_ended_at")
                != security_master.get("ended_at")
                or receipt.get("directory_raw_sha256")
                != security_master.get("directory_raw_sha256")
                or receipt.get("directory_manifest_hash")
                != security_master.get("directory_manifest_hash")
                or receipt.get("directory_member_set_hash")
                != security_master.get("directory_member_set_hash")
                or receipt.get("directory_member_count")
                != security_master.get("directory_member_count")
                or receipt.get("directory_attestation_sha256")
                != security_master.get("directory_attestation_sha256")
                or receipt.get("requested_catalog_member_set_sha256")
                != security_master.get("requested_catalog_member_set_sha256")
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPT_INVALID", code
                )

    def check_deadline(stage: str) -> datetime:
        observed = _dt(now_fn())
        if ((deadline_at is not None and observed > _dt(deadline_at))
                or (deadline_monotonic is not None and monotonic() >= deadline_monotonic)):
            raise QMTAnnouncementBlocked(
                HISTORICAL_PUBLISH_DEADLINE if reconstruction_mode else "QMT_ANNOUNCEMENT_CAPTURE_CLOCK_REVERSED", stage
            )
        return observed

    def checked_events(codes: Sequence[str], offset: int) -> Iterator[Mapping[str, Any]]:
        for index, code in enumerate(codes, start=offset):
            if deadline_monotonic is not None or index % 100 == 0:
                check_deadline(f"db-write:{index}")
            for item_index, item in enumerate(results[code]):
                if deadline_monotonic is not None and item_index % 25 == 0:
                    check_deadline(f"db-event:{index}:{item_index}")
                yield item

    def bindings_by_stock(connection: Any) -> Iterator[tuple[str, list[dict[str, str]]]]:
        nonlocal db_phase
        for offset in range(0, len(catalog.codes), DEFAULT_BATCH_SIZE):
            codes = catalog.codes[offset:offset + DEFAULT_BATCH_SIZE]
            db_phase = "event_revision_write"
            event_receipts = iter(append_event_revisions(
                connection, checked_events(codes, offset),
                known_at=received_at, received_at=received_at,
                source=source_name, batch_id=batch_id,
            ))
            db_phase = "coverage_write"
            for code in codes:
                yield code, [
                    {
                        "revision_id": receipt.revision_id,
                        "content_hash": receipt.content_hash,
                    }
                    for receipt in islice(event_receipts, len(results[code]))
                ]

    def coverage_arguments(connection: Any) -> Iterator[dict[str, Any]]:
        for code, bindings in bindings_by_stock(connection):
            provider_receipt = (
                dict(receipts[code])
                if source_name != QMT_ANNOUNCEMENT_SOURCE
                else None
            )
            yield dict(
                fact_kind="event",
                stock_code=code,
                window_start=window_start,
                window_end=window_end,
                known_at=received_at,
                received_at=received_at,
                covered_through_at=fact_cutoff_at,
                watermark_kind=(
                    "HISTORICAL_RECONSTRUCTION"
                    if reconstruction_mode else "QUERY_CUTOFF"
                ),
                watermark_evidence={
                    "provider": source_name,
                    "period": QMT_ANNOUNCEMENT_PERIOD,
                    "primary_provider": QMT_ANNOUNCEMENT_SOURCE,
                    "fallback_reason": fallback_code,
                    "provider_receipt": provider_receipt,
                    "provider_receipt_hash": (
                        canonical_hash(provider_receipt)
                        if provider_receipt is not None else ""
                    ),
                    "fact_cutoff_at": _dt_text(fact_cutoff_at),
                    "decision_at": _dt_text(received_at),
                    "received_at": _dt_text(received_at),
                    "query_end_time": _qmt_time(fact_cutoff_at),
                    "source_response_hash": response_hashes[code],
                    "global_batch_root_hash": batch_root_hash,
                    "catalog_batch_id": catalog.batch_id,
                    "catalog_manifest_hash": catalog.manifest_hash,
                    "catalog_member_set_hash": catalog.member_set_hash,
                    "catalog_member_count": len(catalog.codes),
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "incremental_proof": incremental,
                    **(
                        {"reconstruction_provenance": reconstruction}
                        if reconstruction_mode else {}
                    ),
                },
                source_rows=results[code],
                fact_bindings=bindings,
                source=source_name,
                batch_id=batch_id,
            )

    def write_rows(connection: Any) -> None:
        coverages = append_source_coverages(connection, coverage_arguments(connection))
        for code, coverage in zip(catalog.codes, coverages, strict=True):
            if coverage.source_response_hash != response_hashes[code]:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_HASH_DRIFT", code
                )
            coverage_ids.append(coverage.coverage_id)

    check_deadline("db-publish-start")
    db_phase = "connect"
    commit_state = "not_committed"
    connection = None
    lock_acquired = False
    lock_uncertain = False
    try:
        connection = engine.connect()
        if connection.dialect.name == "mysql":
            db_phase = "acquire_lock"
            lock_uncertain = True
            acquired = connection.execute(
                text("SELECT GET_LOCK('probiga:qmt-announcement-pit', 0)")
            ).scalar_one()
            lock_acquired = int(acquired or 0) == 1
            lock_uncertain = False
            if not lock_acquired:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_PUBLISHER_ALREADY_RUNNING"
                )
            # GET_LOCK is connection-scoped, not transactional.  End the
            # implicit SELECT transaction before starting the atomic DML unit.
            db_phase = "lock_transaction_end"
            connection.commit()
        db_phase = "transaction_begin"
        transaction = connection.begin()
        db_phase = "coverage_write"
        write_rows(connection)
        # This check is still inside the transaction: exceeding the total SLA
        # rolls every PIT row back before commit.
        publish_checked_at = check_deadline("db-precommit")
        db_phase = "commit"
        commit_state = "unknown"
        transaction.commit()
        commit_state = "committed"
    except Exception as exc:
        if not isinstance(exc, SQLAlchemyError) and commit_state != "unknown":
            raise
        if getattr(exc, "connection_invalidated", False):
            lock_uncertain = True
        raise QMTAnnouncementBlocked(
            PUBLISH_COMMIT_UNKNOWN if commit_state == "unknown"
            else "QMT_ANNOUNCEMENT_PUBLISH_DATABASE_ERROR",
            _publish_error_detail(db_phase, exc, connection=connection, commit_state=commit_state),
        ) from None
    finally:
        if connection is not None:
            _finish_publish_connection(
                connection, lock_acquired=lock_acquired, lock_uncertain=lock_uncertain,
                commit_state=commit_state,
            )
    # Never raise a deadline error after the atomic commit: doing so would
    # falsely report ``database_writes=false`` and invite a duplicate rebuild.
    # The final authoritative deadline check happened inside the transaction,
    # immediately before commit; return that sealed publication timestamp.
    return coverage_ids, publish_checked_at


def synchronize_qmt_announcements(
    engine: Engine,
    *,
    xtdata: Any,
    checkpoint_root: Path,
    now_fn: Callable[[], datetime] = datetime.now,
    window_days: int = DEFAULT_WINDOW_DAYS,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    resume: bool = True,
    coverage_target_date: date | str | None = None,
    source: str = QMT_ANNOUNCEMENT_SOURCE,
    fallback_reason: str = "",
    capture_fact_cutoff_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Capture and atomically publish one exact full-catalog source batch."""

    if not 20 <= int(window_days) <= 3660:
        raise ValueError("QMT announcement window_days must be 20..3660")
    if not 1 <= int(overlap_days) <= min(10, int(window_days)):
        raise ValueError("QMT announcement overlap_days must be 1..10")
    if not 1 <= int(batch_size) <= 500:
        raise ValueError("QMT announcement batch_size must be 1..500")
    source_name = str(source or "").strip()
    fallback_code = str(fallback_reason or "").strip()
    if source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
        raise ValueError("announcement source identity is invalid")
    if source_name != QMT_ANNOUNCEMENT_SOURCE:
        if fallback_code not in ANNOUNCEMENT_FALLBACK_REASON_CODES:
            raise ValueError("announcement fallback reason is not eligible")
    # The fallback can take many minutes across the full stock catalog.  A
    # missing/drifted append-only schema can never be repaired by capture, so
    # reject it here instead of wasting the provider window and retry budget.
    _assert_pit_fact_schema_prepared(engine)
    observed_at = _dt(
        capture_fact_cutoff_at
        if capture_fact_cutoff_at is not None
        else now_fn()
    )
    fact_cutoff = observed_at
    catalog = _load_catalog(engine, fact_cutoff)
    if coverage_target_date is None:
        coverage_target = fact_cutoff.date()
    else:
        try:
            coverage_target = date.fromisoformat(str(coverage_target_date)[:10])
        except ValueError as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_COVERAGE_TARGET_INVALID"
            ) from exc
    if coverage_target > fact_cutoff.date():
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_COVERAGE_TARGET_INVALID", "future"
        )
    coverage_window_start = coverage_target - timedelta(days=int(window_days))
    preferred_capture_start = max(
        coverage_window_start,
        coverage_target - timedelta(days=int(overlap_days)),
    )
    baseline_results, parent_proof = _load_incremental_announcement_baseline(
        engine,
        catalog=catalog,
        source=source_name,
        fact_cutoff_at=fact_cutoff,
        coverage_window_start=coverage_window_start,
        capture_window_start=preferred_capture_start,
    )
    capture_window_start = (
        preferred_capture_start
        if parent_proof is not None
        else coverage_window_start
    )
    cninfo_directory = None
    if source_name == CNINFO_ANNOUNCEMENT_SOURCE:
        _connect_announcement_transport(xtdata, source=source_name)
        read_directory = getattr(xtdata, "capture_directory_identity", None)
        if callable(read_directory):
            cninfo_directory = _cninfo_directory_identity(read_directory())
        if cninfo_directory is None:
            raise QMTAnnouncementBlocked("ANNOUNCEMENT_FALLBACK_DIRECTORY_IDENTITY_UNAVAILABLE")
    bound_directory = cninfo_directory
    if resume:
        resumable = _find_resumable_checkpoint(
            engine,
            checkpoint_root=Path(checkpoint_root),
            observed_at=observed_at,
            window_days=int(window_days),
            source=source_name,
            fallback_reason=fallback_code,
            coverage_target_date=coverage_target_date,
            coverage_window_start=coverage_window_start,
            capture_window_start=capture_window_start,
            cninfo_directory=cninfo_directory,
        )
        if resumable is not None:
            fact_cutoff, catalog, bound_directory = resumable
            baseline_results, parent_proof = (
                _load_incremental_announcement_baseline(
                    engine,
                    catalog=catalog,
                    source=source_name,
                    fact_cutoff_at=fact_cutoff,
                    coverage_window_start=coverage_window_start,
                    capture_window_start=preferred_capture_start,
                )
            )
            capture_window_start = (
                preferred_capture_start
                if parent_proof is not None
                else coverage_window_start
            )
    window_end = fact_cutoff.date()
    if coverage_target > window_end:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_COVERAGE_TARGET_INVALID", "future"
        )
    window_start = coverage_window_start
    seed = {
        "schema": "probiga.qmt-announcement-batch-id.v2",
        "source": source_name,
        "primary_source": QMT_ANNOUNCEMENT_SOURCE,
        "fallback_reason": fallback_code,
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "coverage_target_date": coverage_target.isoformat(),
        "window_start": window_start.isoformat(),
        "capture_window_start": capture_window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    directory_manifest = (
        {"cninfo_directory_identity": bound_directory} if bound_directory is not None else {}
    )
    seed.update(directory_manifest)
    batch_prefix = {
        QMT_ANNOUNCEMENT_SOURCE: "qmt-ann",
        CNINFO_ANNOUNCEMENT_SOURCE: "cninfo-ann",
        EASTMONEY_ANNOUNCEMENT_SOURCE: "em-ann",
    }[source_name]
    batch_id = (
        f"{batch_prefix}-"
        f"{fact_cutoff.strftime('%Y%m%dT%H%M%S')}-"
        f"{canonical_hash(seed)[:16]}"
    )
    checkpoint = AnnouncementCheckpoint.open(
        Path(checkpoint_root),
        batch_id=batch_id,
        fact_cutoff_at=fact_cutoff,
        window_start=capture_window_start,
        window_end=window_end,
        catalog=catalog,
        resume=resume,
        source=source_name,
        fallback_reason=fallback_code,
        coverage_target_date=coverage_target,
        coverage_window_start=window_start,
        manifest_extra=directory_manifest,
    )
    try:
        if cninfo_directory is None:
            _connect_announcement_transport(xtdata, source=source_name)
        if source_name != QMT_ANNOUNCEMENT_SOURCE:
            restored_receipts: dict[str, dict[str, Any]] = {}
            for code in catalog.codes:
                staged_events = checkpoint.load(code)
                if staged_events is None:
                    continue
                receipt = checkpoint.load_provider_receipt(code)
                if receipt is None:
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_FALLBACK_CHECKPOINT_RECEIPT_MISSING",
                        code,
                    )
                if (
                    source_name == CNINFO_ANNOUNCEMENT_SOURCE
                    and receipt.get("pagination_mode")
                    == "EXACT_STOCK_MULTI_PAGE_FIRST_RECHECK"
                ):
                    # That historical proof only replayed page one.  Preserve
                    # the artifact for audit, remove it from the active result
                    # set, and recapture this member with stable full sweeps.
                    checkpoint.quarantine_result(
                        code, reason="legacy-multi-page-proof"
                    )
                    continue
                restored_receipts[code] = receipt
            if restored_receipts:
                restorer = getattr(
                    xtdata, "restore_capture_receipts", None
                )
                if not callable(restorer):
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_FALLBACK_RESUME_UNSUPPORTED"
                    )
                restorer(restored_receipts)
        delta_results = _download_and_read(
            xtdata,
            checkpoint=checkpoint,
            catalog=catalog,
            fact_cutoff_at=fact_cutoff,
            window_start=capture_window_start,
            batch_size=int(batch_size),
            source=source_name,
        )
        provider_receipts: dict[str, dict[str, Any]] = {}
        cninfo_directory_anchors: set[tuple[Any, ...]] = set()
        if source_name != QMT_ANNOUNCEMENT_SOURCE:
            receipt_reader = getattr(xtdata, "capture_receipts", None)
            raw_receipts = receipt_reader() if callable(receipt_reader) else None
            if not isinstance(raw_receipts, Mapping) or set(raw_receipts) != set(
                catalog.codes
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_FALLBACK_RECEIPTS_INCOMPLETE"
                )
            requested_start = _qmt_time(
                datetime.combine(capture_window_start, datetime.min.time())
            )
            requested_end = _qmt_time(fact_cutoff)
            for code in catalog.codes:
                raw_receipt = raw_receipts.get(code)
                if not isinstance(raw_receipt, Mapping):
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                    )
                receipt = dict(raw_receipt)
                try:
                    captured_at = _dt(receipt.get("captured_at"))
                    result_count = int(receipt.get("result_count"))
                except (TypeError, ValueError, OverflowError) as exc:
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                    ) from exc
                if (
                    receipt.get("schema")
                    != (
                        CNINFO_PROVIDER_RECEIPT_SCHEMA
                        if source_name == CNINFO_ANNOUNCEMENT_SOURCE
                        else "probiga.announcement-provider-receipt.v1"
                    )
                    or receipt.get("status") != "COMPLETE"
                    or receipt.get("source") != source_name
                    or str(receipt.get("stock_code") or "") != code
                    or str(receipt.get("qmt_code") or "")
                    != catalog.qmt_by_code[code]
                    or receipt.get("requested_start_time") != requested_start
                    or receipt.get("requested_end_time") != requested_end
                    or receipt.get("exhausted") is not True
                    or isinstance(receipt.get("result_count"), bool)
                    or result_count != len(delta_results[code])
                    or captured_at < fact_cutoff
                    or not _SHA256_RE.fullmatch(
                        str(receipt.get("provider_payload_sha256") or "")
                    )
                    or not _fallback_receipt_valid(
                        receipt,
                        source=source_name,
                        stock_code=code,
                        qmt_code=catalog.qmt_by_code[code],
                        requested_start_time=requested_start,
                        requested_end_time=requested_end,
                        result_count=len(delta_results[code]),
                        catalog_codes=catalog.codes,
                    )
                ):
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                    )
                provider_receipts[code] = receipt
                if source_name == CNINFO_ANNOUNCEMENT_SOURCE:
                    cninfo_directory_anchors.add((
                        receipt.get("directory_raw_sha256"),
                        receipt.get("directory_manifest_hash"),
                        receipt.get("directory_member_set_hash"),
                        receipt.get("directory_member_count"),
                        receipt.get("requested_catalog_member_set_sha256"),
                    ))
            if (
                source_name == CNINFO_ANNOUNCEMENT_SOURCE
                and len(cninfo_directory_anchors) != 1
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_FALLBACK_DIRECTORY_DRIFT"
                )
        if (
            source_name == QMT_ANNOUNCEMENT_SOURCE
            and len(catalog.codes) >= 100
            and not any(delta_results.values())
        ):
            # QMT permission failures can present as one empty DataFrame per
            # requested symbol instead of raising.  A 30-day full A-share
            # market with zero announcements is not authoritative evidence;
            # keep every event-dependent strategy DATA_BLOCKED.
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN"
            )
        delta_root_hash = canonical_hash({
            "schema": "probiga.qmt-announcement-delta.v1",
            "source": source_name,
            "fact_cutoff_at": _dt_text(fact_cutoff),
            "capture_window_start": capture_window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "catalog_batch_id": catalog.batch_id,
            "entries": [
                {
                    "stock_code": code,
                    "source_response_hash": _source_response_hash(
                        delta_results[code]
                    ),
                    "result_count": len(delta_results[code]),
                }
                for code in catalog.codes
            ],
        })
        results = _merge_announcement_results(
            catalog=catalog,
            baseline=baseline_results,
            delta=delta_results,
        )
        prepared = checkpoint.load_prepared_publish()
        received_at = prepared[0] if prepared is not None else _dt(now_fn())
        if source_name != QMT_ANNOUNCEMENT_SOURCE and any(
            _dt(receipt["captured_at"]) > received_at
            for receipt in provider_receipts.values()
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID",
                "captured-after-received",
            )
        elapsed = received_at - fact_cutoff
        if elapsed < timedelta(0):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CAPTURE_CLOCK_REVERSED",
                str(int(elapsed.total_seconds())),
            )
        batch_root, entries = build_batch_root(
            batch_id=batch_id,
            fact_cutoff_at=fact_cutoff,
            received_at=received_at,
            window_start=window_start,
            window_end=window_end,
            catalog=catalog,
            results=results,
            source=source_name,
            provider_receipts=provider_receipts,
        )
        incremental_proof = {
            "schema": "probiga.qmt-announcement-incremental-chain.v1",
            "mode": (
                "DELTA_OVER_PARENT"
                if parent_proof is not None
                else "FULL_BASELINE"
            ),
            "coverage_window_start": window_start.isoformat(),
            "capture_window_start": capture_window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "parent_batch_id": str(
                (parent_proof or {}).get("parent_batch_id") or ""
            ),
            "parent_batch_root_hash": str(
                (parent_proof or {}).get("parent_batch_root_hash") or ""
            ),
            "parent_received_at": str(
                (parent_proof or {}).get("parent_received_at") or ""
            ),
            "delta_root_hash": delta_root_hash,
            "result_batch_root_hash": batch_root,
        }
        incremental_proof["chain_root_hash"] = canonical_hash(
            incremental_proof
        )
        if prepared is not None:
            if prepared[1] != batch_root:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_PREPARED_ROOT_MISMATCH"
                )
        else:
            # Freeze E and the global root before entering the database
            # transaction.  If the DB commit succeeds but the final local
            # marker fails, a retry replays byte-identical evidence and the
            # append-only ledgers remain idempotent.
            checkpoint.prepare_publish(
                batch_root_hash=batch_root,
                received_at=received_at,
            )
        coverage_ids, _publish_completed_at = _publish_batch(
            engine,
            batch_id=batch_id,
            batch_root_hash=batch_root,
            entries=entries,
            fact_cutoff_at=fact_cutoff,
            received_at=received_at,
            window_start=window_start,
            window_end=window_end,
            catalog=catalog,
            results=results,
            source=source_name,
            fallback_reason=fallback_code,
            provider_receipts=provider_receipts,
            incremental_proof=incremental_proof,
            now_fn=now_fn,
        )
        try:
            checkpoint.mark_complete(
                batch_root_hash=batch_root, received_at=received_at
            )
        except OSError:
            # The immutable database transaction is the authority.  A failed
            # local marker can only cause an idempotent replay of the same T.
            pass
        return {
            "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
            "status": "COMPLETE",
            "reason_code": (
                "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE"
                if source_name == QMT_ANNOUNCEMENT_SOURCE
                else "ANNOUNCEMENT_FALLBACK_FULL_MARKET_COMPLETE"
            ),
            "source": source_name,
            "primary_source": QMT_ANNOUNCEMENT_SOURCE,
            "fallback_reason": fallback_code,
            "batch_id": batch_id,
            "batch_root_hash": batch_root,
            "incremental_chain_root_hash": incremental_proof[
                "chain_root_hash"
            ],
            "parent_batch_id": incremental_proof["parent_batch_id"],
            "parent_batch_root_hash": incremental_proof[
                "parent_batch_root_hash"
            ],
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "stock_count": len(catalog.codes),
            "coverage_count": len(coverage_ids),
            "event_count": sum(len(value) for value in results.values()),
            "empty_stock_count": sum(not value for value in results.values()),
            "fact_cutoff_at": _dt_text(fact_cutoff),
            "decision_at": _dt_text(received_at),
            "received_at": _dt_text(received_at),
            # Match the immutable receipt's T/E interval. The full capture
            # and publication deadline is enforced inside the transaction.
            "capture_seconds": int(
                (received_at - fact_cutoff).total_seconds()
            ),
            "window_start": window_start.isoformat(),
            "capture_window_start": capture_window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except QMTAnnouncementBlocked as exc:
        try:
            checkpoint.diagnose(exc.reason_code, exc.detail)
        except OSError as diagnostic_exc:
            logger.warning("Announcement checkpoint diagnostic unavailable: %s", type(diagnostic_exc).__name__)
        received = _dt(now_fn())
        return {
            "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
            "status": "DATA_BLOCKED",
            "reason_code": exc.reason_code,
            "detail": exc.detail,
            **({"database_writes": None} if exc.reason_code == PUBLISH_COMMIT_UNKNOWN else {}),
            "source": source_name,
            "primary_source": QMT_ANNOUNCEMENT_SOURCE,
            "fallback_reason": fallback_code,
            "batch_id": batch_id,
            "batch_root_hash": "",
            "incremental_chain_root_hash": "",
            "parent_batch_id": str(
                (parent_proof or {}).get("parent_batch_id") or ""
            ),
            "parent_batch_root_hash": str(
                (parent_proof or {}).get("parent_batch_root_hash") or ""
            ),
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "stock_count": len(catalog.codes),
            "coverage_count": 0,
            "event_count": 0,
            "empty_stock_count": 0,
            "fact_cutoff_at": _dt_text(fact_cutoff),
            "decision_at": _dt_text(received),
            "received_at": _dt_text(received),
            "capture_seconds": max(
                0, int((received - fact_cutoff).total_seconds())
            ),
            "window_start": window_start.isoformat(),
            "capture_window_start": capture_window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }


def synchronize_historical_cninfo_announcements(
    engine: Engine,
    *,
    adapter: Any,
    checkpoint_root: Path,
    catalog: AnnouncementCatalog,
    context: HistoricalReconstructionContext,
    now_fn: Callable[[], datetime] = _shanghai_now,
    window_days: int = DEFAULT_WINDOW_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress=None,
) -> dict[str, Any]:
    """Reconstruct one missing target batch without relabelling its knowledge time.

    The provider query is bounded by the target-day cutoff, while every event
    and empty receipt becomes known only at the actual reconstruction time.
    Validated shards survive a bounded attempt and are reused on fenced retry.
    """

    if not 20 <= int(window_days) <= 3660:
        raise ValueError("QMT announcement window_days must be 20..3660")
    if not 1 <= int(batch_size) <= 500:
        raise ValueError("QMT announcement batch_size must be 1..500")
    _assert_pit_fact_schema_prepared(engine)
    identity = _validate_reconstruction_context(context, catalog=catalog)
    target = context.target_trade_date
    source_cutoff = _dt(context.source_query_cutoff_at)
    started_at = _dt(context.reconstruction_started_at)
    observed_at = _dt(now_fn())
    if observed_at < started_at:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_CLOCK_INVALID"
        )
    publish_deadline = started_at + HISTORICAL_RECONSTRUCTION_BUDGET
    capture_deadline = publish_deadline - HISTORICAL_RECONSTRUCTION_PUBLISH_RESERVE
    time_observed = monotonic()
    publish_monotonic = time_observed + (publish_deadline - observed_at).total_seconds()
    capture_monotonic = time_observed + (capture_deadline - observed_at).total_seconds()

    def check_capture_deadline(unit: str) -> None:
        if monotonic() >= capture_monotonic:
            raise QMTAnnouncementBlocked(HISTORICAL_CAPTURE_DEADLINE, unit)

    check_capture_deadline("checkpoint")
    binder = getattr(adapter, "bind_capture_deadline", None)
    if callable(binder):
        binder(fact_cutoff_at=started_at, max_capture_delay=capture_deadline - started_at)
    window_start = target - timedelta(days=int(window_days))
    window_end = target
    requested_start = _qmt_time(datetime.combine(window_start, datetime.min.time()))
    requested_end = _qmt_time(source_cutoff)
    scope = {
        "schema": "probiga.cninfo-announcement-query-scope.v1",
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "target_trade_date": target.isoformat(),
        "source_query_cutoff_at": _dt_text(source_cutoff),
        "query_window_start": window_start.isoformat(),
        "query_window_end": window_end.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "instruments": [[code, catalog.qmt_by_code[code]] for code in catalog.codes],
        "provider_receipt_schema": CNINFO_PROVIDER_RECEIPT_SCHEMA,
        "source_event_contract": HISTORICAL_SOURCE_EVENT_CONTRACT,
    }
    query_scope_sha256 = canonical_hash(scope)
    checkpoint_id = (
        f"cninfo-ann-reconstruction-scope-{target.strftime('%Y%m%d')}-"
        f"{query_scope_sha256[:32]}"
    )
    checkpoint = AnnouncementCheckpoint.open(
        Path(checkpoint_root),
        batch_id=checkpoint_id,
        fact_cutoff_at=source_cutoff,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        resume=True,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason="QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED",
        coverage_target_date=target,
        coverage_window_start=window_start,
        manifest_extra={
            "capture_mode": "HISTORICAL_RECONSTRUCTION",
            "source_query_scope_sha256": query_scope_sha256,
        },
    )
    _connect_announcement_transport(adapter, source=CNINFO_ANNOUNCEMENT_SOURCE)
    staged_receipts = {}
    for code in catalog.codes:
        check_capture_deadline(f"restore:{code}")
        result = checkpoint._load_result(code)
        if result is None:
            continue
        receipt = result.get("provider_receipt")
        if not _fallback_receipt_valid(
            receipt, source=CNINFO_ANNOUNCEMENT_SOURCE, stock_code=code,
            qmt_code=catalog.qmt_by_code[code], requested_start_time=requested_start,
            requested_end_time=requested_end, result_count=len(result["events"]),
            catalog_codes=catalog.codes, staged=True,
        ):
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_STAGED_RECEIPT_INVALID", code)
        try:
            master_at = _dt(receipt.get("security_master_started_at"))
            captured_at = _dt(receipt.get("captured_at"))
            event_keys = set()
            for event in result["events"]:
                event_date = date.fromisoformat(str(event.get("event_date") or ""))
                published = _dt(event["published_at"]) if event.get("published_at") else None
                event_key = str(event.get("event_key") or "")
                if (event.get("stock_code") != code or event.get("qmt_code") != catalog.qmt_by_code[code]
                        or not event_key.startswith("cninfo:") or event_key in event_keys
                        or not _SHA256_RE.fullmatch(str(event.get("source_row_hash") or ""))
                        or not window_start <= event_date <= window_end
                        or (published is not None and (published.date() != event_date or published > source_cutoff))):
                    raise ValueError("staged announcement source scope differs")
                event_keys.add(event_key)
        except (TypeError, ValueError) as exc:
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_STAGED_RECEIPT_INVALID", code) from exc
        if not source_cutoff < master_at <= captured_at <= observed_at:
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_STAGED_RECEIPT_INVALID", code)
        staged_receipts[code] = receipt
    if staged_receipts:
        restorer = getattr(adapter, "restore_capture_receipts", None)
        if not callable(restorer):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_RESUME_UNSUPPORTED"
            )
        restorer(staged_receipts)
    completed_units = len(staged_receipts)
    resumed_units = completed_units
    progress_started = last_report = monotonic()
    if progress is not None:
        progress.checkpoint = {"checkpoint_id": checkpoint_id, "source_query_scope_sha256": query_scope_sha256,
                               "source": CNINFO_ANNOUNCEMENT_SOURCE, "target_date": target.isoformat()}
        progress.emit("source_checkpoint", completed_units=completed_units,
                      resumed_units=resumed_units,
                      capture_deadline_at=_dt_text(capture_deadline), publish_deadline_at=_dt_text(publish_deadline))

    def report_saved_stock(code: str) -> None:
        nonlocal completed_units, last_report
        completed_units += 1
        current = monotonic()
        if (progress is not None and (completed_units % 50 == 0 or completed_units == len(catalog.codes)
                                      or current - last_report >= 30)):
            rate = (completed_units - resumed_units) / max(current - progress_started, 0.001)
            progress.emit("source_checkpoint", completed_units=completed_units, current_unit=code,
                          committed_units_per_second=round(rate, 3))
            last_report = current

    try:
        results = _download_and_read(
            adapter,
            checkpoint=checkpoint,
            catalog=catalog,
            fact_cutoff_at=source_cutoff,
            window_start=window_start,
            batch_size=int(batch_size),
            source=CNINFO_ANNOUNCEMENT_SOURCE,
            before_fetch=check_capture_deadline,
            after_save=report_saved_stock,
        )
    except Exception:
        if progress is not None:
            progress.emit("source_checkpoint", status="FAILED", completed_units=completed_units)
        raise
    check_capture_deadline("security-master-end")
    if progress is not None:
        progress.emit("source_checkpoint", status="COMPLETE", completed_units=completed_units)
    receipt_reader = getattr(adapter, "capture_receipts", None)
    raw_receipts = receipt_reader() if callable(receipt_reader) else None
    if not isinstance(raw_receipts, Mapping) or set(raw_receipts) != set(
        catalog.codes
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPTS_INCOMPLETE"
        )
    receipts = {str(code): dict(value) for code, value in raw_receipts.items()}
    directory_anchors: set[tuple[Any, ...]] = set()
    captured_times: list[datetime] = []
    for code in catalog.codes:
        receipt = receipts[code]
        try:
            captured_at = _dt(receipt.get("captured_at"))
            security_master_started = _dt(
                receipt.get("security_master_started_at")
            )
            security_master_ended = _dt(
                receipt.get("security_master_ended_at")
            )
        except (TypeError, ValueError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPT_INVALID", code
            ) from exc
        if (
            not source_cutoff < security_master_started
            <= captured_at <= security_master_ended
            or not _fallback_receipt_valid(
                receipt,
                source=CNINFO_ANNOUNCEMENT_SOURCE,
                stock_code=code,
                qmt_code=catalog.qmt_by_code[code],
                requested_start_time=requested_start,
                requested_end_time=requested_end,
                result_count=len(results[code]),
                catalog_codes=catalog.codes,
            )
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_RECEIPT_INVALID", code
            )
        captured_times.append(captured_at)
        directory_anchors.add((
            receipt.get("directory_raw_sha256"),
            receipt.get("directory_manifest_hash"),
            receipt.get("directory_member_set_hash"),
            receipt.get("directory_member_count"),
            receipt.get("security_master_started_at"),
            receipt.get("security_master_ended_at"),
            receipt.get("directory_attestation_sha256"),
            receipt.get("requested_catalog_member_set_sha256"),
        ))
    reconstruction_anchor_starts = [
        _dt(receipt.get("security_master_started_at"))
        for receipt in receipts.values()
    ]
    effective_started_at = min([started_at, *reconstruction_anchor_starts])
    reconstructed_at = _dt(now_fn())
    if (
        len(directory_anchors) != 1
        or reconstructed_at < max(captured_times)
        or reconstructed_at < _dt(list(directory_anchors)[0][5])
        or effective_started_at <= source_cutoff
        or reconstructed_at < effective_started_at
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_ENVELOPE_INVALID"
        )
    directory_anchor = list(directory_anchors)[0]
    if progress is not None:
        progress.emit("source_capture", status="COMPLETE", completed_units=completed_units)
    provenance_core = {
        "schema": QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA,
        "mode": "HISTORICAL_RECONSTRUCTION",
        "target_trade_date": target.isoformat(),
        "query_window_start": window_start.isoformat(),
        "query_window_end": window_end.isoformat(),
        "source_query_cutoff_at": _dt_text(source_cutoff),
        "source_query_scope_sha256": query_scope_sha256,
        "reconstruction_started_at": _dt_text(effective_started_at),
        "reconstructed_at": _dt_text(reconstructed_at),
        "known_at": _dt_text(reconstructed_at),
        "provider": CNINFO_ANNOUNCEMENT_SOURCE,
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "scheduler_run_uid": identity["scheduler_run_uid"],
        "build_sha": identity["build_sha"],
        "execution_origin": identity["execution_origin"],
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": len(catalog.codes),
        "catalog_codes_sha256": canonical_hash(list(catalog.codes)),
        "authority": identity["authority"],
        "security_master": {
            "directory_raw_sha256": directory_anchor[0],
            "directory_manifest_hash": directory_anchor[1],
            "directory_member_set_hash": directory_anchor[2],
            "directory_member_count": directory_anchor[3],
            "started_at": directory_anchor[4],
            "ended_at": directory_anchor[5],
            "directory_attestation_sha256": directory_anchor[6],
            "requested_catalog_member_set_sha256": directory_anchor[7],
        },
        "event_content_contract": HISTORICAL_SOURCE_EVENT_CONTRACT,
        "zero_result_receipts_complete": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    provenance = {
        **provenance_core,
        "reconstruction_sha256": canonical_hash(provenance_core),
    }
    delta_root = canonical_hash({
        "schema": "probiga.qmt-announcement-reconstruction-delta.v2",
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "source_query_cutoff_at": _dt_text(source_cutoff),
        "catalog_batch_id": catalog.batch_id,
        "entries": [
            {
                "stock_code": code,
                "source_response_hash": _source_response_hash(results[code]),
                "provider_receipt_hash": canonical_hash(receipts[code]),
                "result_count": len(results[code]),
            }
            for code in catalog.codes
        ],
    })
    publication_identity = canonical_hash({
        "schema": "probiga.cninfo-announcement-publication-id.v1",
        "source_query_scope_sha256": query_scope_sha256,
        "reconstruction_sha256": provenance["reconstruction_sha256"],
        "delta_root_sha256": delta_root,
    })
    batch_id = f"cninfo-ann-reconstruction-{target.strftime('%Y%m%d')}-{publication_identity[:32]}"
    batch_root, entries = build_batch_root(
        batch_id=batch_id,
        fact_cutoff_at=source_cutoff,
        received_at=reconstructed_at,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        results=results,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        provider_receipts=receipts,
        reconstruction_provenance=provenance,
    )
    incremental = {
        "schema": "probiga.qmt-announcement-incremental-chain.v1",
        "mode": "FULL_BASELINE",
        "coverage_window_start": window_start.isoformat(),
        "capture_window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "parent_batch_id": "",
        "parent_batch_root_hash": "",
        "parent_received_at": "",
        "delta_root_hash": delta_root,
        "result_batch_root_hash": batch_root,
    }
    incremental["chain_root_hash"] = canonical_hash(incremental)
    checkpoint.prepare_publish(
        batch_root_hash=batch_root, received_at=reconstructed_at, publication_batch_id=batch_id
    )
    if progress is not None:
        progress.emit("database_publish", completed_units=completed_units)
    coverage_ids, completed_at = _publish_batch(
        engine,
        batch_id=batch_id,
        batch_root_hash=batch_root,
        entries=entries,
        fact_cutoff_at=source_cutoff,
        received_at=reconstructed_at,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        results=results,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason="QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED",
        provider_receipts=receipts,
        incremental_proof=incremental,
        reconstruction_provenance=provenance,
        deadline_at=publish_deadline,
        deadline_monotonic=publish_monotonic,
        now_fn=now_fn,
    )
    try:
        checkpoint.mark_complete(
            batch_root_hash=batch_root, received_at=reconstructed_at, publication_batch_id=batch_id
        )
    except OSError:
        pass
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "COMPLETE",
        "reason_code": "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_COMPLETE",
        "detail": "",
        "mode": "HISTORICAL_RECONSTRUCTION",
        "trade_date": target.isoformat(),
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "primary_source": QMT_ANNOUNCEMENT_SOURCE,
        "fallback_reason": "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED",
        "funding_eligible": True,
        "batch_id": batch_id,
        "batch_root_hash": batch_root,
        "incremental_chain_root_hash": incremental["chain_root_hash"],
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "stock_count": len(catalog.codes),
        "coverage_count": len(coverage_ids),
        "event_count": sum(len(value) for value in results.values()),
        "empty_stock_count": sum(not value for value in results.values()),
        "fact_cutoff_at": _dt_text(source_cutoff),
        "source_query_cutoff_at": _dt_text(source_cutoff),
        "decision_at": _dt_text(reconstructed_at),
        "received_at": _dt_text(reconstructed_at),
        "reconstructed_at": _dt_text(reconstructed_at),
        "capture_seconds": max(
            0, int((completed_at - effective_started_at).total_seconds())
        ),
        "window_start": window_start.isoformat(),
        "capture_window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "reconstruction_provenance": provenance,
        "reconstruction_sha256": provenance["reconstruction_sha256"],
        "validation_run_uid": identity["scheduler_run_uid"],
        "validation_build_sha": identity["build_sha"],
        "execution_origin": identity["execution_origin"],
        **(
            {"research_run_uid": identity["scheduler_run_uid"]}
            if identity["execution_origin"]
            == QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
            else {}
        ),
        "database_writes": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def validate_task_result(payload: Any, process_exit: int) -> str:
    """Validate the single-line scheduler result and map its disposition."""

    if not isinstance(payload, dict) or payload.get("schema") != (
        QMT_ANNOUNCEMENT_TASK_SCHEMA
    ):
        raise ValueError("QMT announcement task result schema differs")
    if (
        payload.get("automatic_real_order_submission") is not False
        or payload.get("real_order_authority") is not False
        or not isinstance(payload.get("reason_code"), str)
        or not payload["reason_code"]
        or not isinstance(payload.get("stock_count"), int)
        or not isinstance(payload.get("coverage_count"), int)
        or not isinstance(payload.get("capture_seconds"), int)
        or payload["capture_seconds"] < 0
    ):
        raise ValueError("QMT announcement task safety/counter fields differ")
    try:
        cutoff = _dt(payload.get("fact_cutoff_at"))
        decision = _dt(payload.get("decision_at"))
        received = _dt(payload.get("received_at"))
    except (TypeError, ValueError) as exc:
        raise ValueError("QMT announcement task timestamps are invalid") from exc
    reconstruction_mode = payload.get("mode") in {
        "HISTORICAL_RECONSTRUCTION",
        "HISTORICAL_RECONSTRUCTION_EXISTING",
    }
    if decision != received or received < cutoff:
        raise ValueError("QMT announcement task T/E timestamps differ")
    if reconstruction_mode:
        provenance = payload.get("reconstruction_provenance")
        if not isinstance(provenance, dict):
            raise ValueError("announcement reconstruction provenance is absent")
        core = {
            key: value
            for key, value in provenance.items()
            if key != "reconstruction_sha256"
        }
        try:
            target = date.fromisoformat(str(payload.get("trade_date") or ""))
            reconstruction_started = _dt(
                provenance.get("reconstruction_started_at")
            )
            security_master = provenance.get("security_master")
            if not isinstance(security_master, Mapping):
                raise ValueError("security master proof is absent")
            master_started = _dt(security_master.get("started_at"))
            master_ended = _dt(security_master.get("ended_at"))
            authority = provenance.get("authority")
            if not isinstance(authority, Mapping):
                raise ValueError("reconstruction authority is absent")
            execution_origin = str(
                provenance.get("execution_origin") or ""
            ).strip().upper()
            truth = authority.get("qmt_daily_truth")
            reconciliation = authority.get("reconciliation")
            if not all(isinstance(value, Mapping) for value in (
                truth, reconciliation,
            )):
                raise ValueError("nested reconstruction authority is absent")
            truth = dict(truth)
            reconciliation = dict(reconciliation)
            no_trade_codes = reconciliation.get("native_no_trade_codes")
            exclusions = reconciliation.get("excluded_from_prior")
            additions = reconciliation.get("added_to_prior")
            if (
                not isinstance(no_trade_codes, list)
                or not isinstance(exclusions, list)
                or not isinstance(additions, list)
                or no_trade_codes != sorted(set(no_trade_codes))
                or any(
                    _CODE_RE.fullmatch(str(code)) is None
                    for code in no_trade_codes
                )
                or any(
                    not isinstance(item, Mapping)
                    or _CODE_RE.fullmatch(
                        str(item.get("stock_code") or "")
                    ) is None
                    or item.get("reason") not in {
                        "LISTED_AFTER_TARGET", "EXPIRED_BEFORE_TARGET",
                    }
                    for item in exclusions
                )
            ):
                raise ValueError("native no-trade identity is absent")
            attested_count = int(reconciliation.get("attested_daily_count"))
            no_trade_count = int(reconciliation.get("native_no_trade_count"))
            prior_count = int(reconciliation.get("prior_eligible_count"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "announcement reconstruction provenance is invalid"
            ) from exc
        if (
            provenance.get("schema")
            != QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA
            or provenance.get("mode") != "HISTORICAL_RECONSTRUCTION"
            or provenance.get("reconstruction_sha256")
            != canonical_hash(core)
            or payload.get("reconstruction_sha256")
            != provenance.get("reconstruction_sha256")
            or _dt(provenance.get("source_query_cutoff_at")) != cutoff
            or _dt(provenance.get("reconstructed_at")) != received
            or _dt(provenance.get("known_at")) != received
            or cutoff.date() != target
            or cutoff.time() != datetime.max.time()
            or received <= cutoff
            or provenance.get("provider") != CNINFO_ANNOUNCEMENT_SOURCE
            or provenance.get("source") != CNINFO_ANNOUNCEMENT_SOURCE
            or re.fullmatch(
                r"[0-9a-f]{32}",
                str(provenance.get("scheduler_run_uid") or ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(provenance.get("build_sha") or ""),
            ) is None
            or provenance.get("build_sha") == "0" * 40
            or provenance.get("automatic_real_order_submission") is not False
            or provenance.get("real_order_authority") is not False
            or not _persisted_reconstruction_execution_origin_matches(
                provenance,
                authority,
            )
            or (
                execution_origin
                and payload.get("execution_origin") != execution_origin
            )
            or (
                execution_origin == QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
                and payload.get("research_run_uid")
                != provenance.get("scheduler_run_uid")
            )
            or (
                execution_origin
                != QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
                and "research_run_uid" in payload
            )
            or not reconstruction_started <= master_started
            <= master_ended <= received
            or authority.get("qmt_daily_truth_sha256")
            != canonical_hash(truth)
            or not _reconstruction_stock_scope_matches(
                authority,
                target_trade_date=target,
                known_at=received,
            )
            or authority.get("reconciliation_sha256")
            != canonical_hash(reconciliation)
            or authority.get("target_trade_date") != target.isoformat()
            or authority.get("catalog_batch_id")
            != provenance.get("catalog_batch_id")
            or authority.get("catalog_manifest_hash")
            != provenance.get("catalog_manifest_hash")
            or authority.get("catalog_member_set_hash")
            != provenance.get("catalog_member_set_hash")
            or int(authority.get("catalog_member_count") or -1)
            != int(provenance.get("catalog_member_count") or -2)
            or not _SHA256_RE.fullmatch(str(
                authority.get("catalog_codes_sha256") or ""
            ))
            or authority.get("catalog_codes_sha256")
            != provenance.get("catalog_codes_sha256")
            or truth.get("schema")
            != "probiga.qmt-daily-market-consumer-truth.v1"
            or truth.get("requested_sessions") != [target.isoformat()]
            or truth.get("catalog_batch_id")
            != provenance.get("catalog_batch_id")
            or truth.get("catalog_manifest_hash")
            != provenance.get("catalog_manifest_hash")
            or truth.get("catalog_member_set_hash")
            != provenance.get("catalog_member_set_hash")
            or int(truth.get("attested_row_count") or -1) != attested_count
            or reconciliation.get("schema")
            != "probiga.qmt-announcement-catalog-reconciliation.v2"
            or reconciliation.get("target_trade_date") != target.isoformat()
            or reconciliation.get("authority_catalog_batch_id")
            != provenance.get("catalog_batch_id")
            or reconciliation.get("authority_catalog_manifest_hash")
            != provenance.get("catalog_manifest_hash")
            or int(reconciliation.get("authority_eligible_count") or -1)
            != int(provenance.get("catalog_member_count") or -2)
            or prior_count - len(exclusions) + len(additions)
            != int(provenance.get("catalog_member_count") or -2)
            or attested_count <= 0
            or no_trade_count != len(no_trade_codes)
            or attested_count + no_trade_count
            != int(provenance.get("catalog_member_count") or -2)
            or reconciliation.get("native_no_trade_codes_sha256")
            != canonical_hash(no_trade_codes)
            or payload.get("trade_date")
            != provenance.get("target_trade_date")
            or payload.get("source_query_cutoff_at")
            != provenance.get("source_query_cutoff_at")
            or payload.get("reconstructed_at")
            != provenance.get("reconstructed_at")
            or payload.get("database_writes")
            is not (payload.get("mode") == "HISTORICAL_RECONSTRUCTION")
        ):
            raise ValueError("announcement reconstruction provenance differs")
    elif int((received - cutoff).total_seconds()) != int(
        payload["capture_seconds"]
    ):
        raise ValueError("QMT announcement task T/E timestamps differ")
    status = payload.get("status")
    source_name = str(
        payload.get("source") or QMT_ANNOUNCEMENT_SOURCE
    ).strip()
    fallback_code = str(payload.get("fallback_reason") or "").strip()
    if source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
        raise ValueError("announcement task source differs")
    if source_name != QMT_ANNOUNCEMENT_SOURCE and (
        (
            fallback_code
            != "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED"
            if reconstruction_mode
            else fallback_code not in ANNOUNCEMENT_FALLBACK_REASON_CODES
        )
        or payload.get("primary_source") != QMT_ANNOUNCEMENT_SOURCE
    ):
        raise ValueError("announcement task fallback evidence differs")
    if status == "COMPLETE":
        batch_prefix = {
            QMT_ANNOUNCEMENT_SOURCE: "qmt-ann-",
            CNINFO_ANNOUNCEMENT_SOURCE: "cninfo-ann-",
            EASTMONEY_ANNOUNCEMENT_SOURCE: "em-ann-",
        }[source_name]
        if (
            process_exit != 0
            or payload["stock_count"] <= 0
            or payload["coverage_count"] != payload["stock_count"]
            or not _SHA256_RE.fullmatch(str(payload.get("batch_root_hash") or ""))
            or not str(payload.get("batch_id") or "").startswith(batch_prefix)
        ):
            raise ValueError("QMT announcement COMPLETE result differs")
        if reconstruction_mode and (
            source_name != CNINFO_ANNOUNCEMENT_SOURCE
            or payload.get("reason_code") not in {
                "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_COMPLETE",
                "QMT_ANNOUNCEMENT_EXISTING_HISTORICAL_RECONSTRUCTION_COMPLETE",
            }
        ):
            raise ValueError("QMT announcement reconstruction result differs")
        return "complete"
    if status == "DATA_BLOCKED":
        if (
            process_exit != 2
            or payload.get("batch_root_hash") != ""
            or payload.get("coverage_count") != 0
        ):
            raise ValueError("QMT announcement DATA_BLOCKED result differs")
        return "data_blocked"
    raise ValueError("QMT announcement task status is unknown")


__all__ = [
    "ANNOUNCEMENT_FALLBACK_REASON_CODES",
    "AUTHORITATIVE_ANNOUNCEMENT_SOURCES",
    "AnnouncementCatalog", "AnnouncementCheckpoint", "DEFAULT_BATCH_SIZE",
    "HistoricalReconstructionContext",
    "EASTMONEY_ANNOUNCEMENT_SOURCE",
    "CNINFO_ANNOUNCEMENT_SOURCE",
    "DEFAULT_WINDOW_DAYS", "QMTAnnouncementBlocked",
    "QMT_ANNOUNCEMENT_BATCH_SCHEMA", "QMT_ANNOUNCEMENT_PERIOD",
    "QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA",
    "QMT_ANNOUNCEMENT_RECONSTRUCTION_SCHEMA",
    "QMT_ANNOUNCEMENT_SOURCE", "QMT_ANNOUNCEMENT_TASK_SCHEMA",
    "assert_announcement_pit_schema_prepared",
    "build_batch_root", "canonical_hash", "canonical_json",
    "parse_qmt_announcement_frame", "parse_qmt_publication_time",
    "synchronize_qmt_announcements", "validate_complete_announcement_batch",
    "synchronize_historical_cninfo_announcements",
    "HistoricalAnnouncementSelection", "validate_complete_historical_reconstruction_batch",
    "validate_complete_qmt_announcement_batch",
    "validate_task_result",
]
