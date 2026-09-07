"""Immutable point-in-time finance and issuer-event facts.

The legacy ``si_stock_finance`` and ``si_notice_eastmoney`` tables are mutable
display caches.  They are deliberately never read by this module.  Strategy
features must use the append-only revisions below and prove both source
publication time and local knowledge time were available at ``decision_at``.

All timestamps are stored as naive ``Asia/Shanghai`` wall-clock values because
the production MySQL schema uses ``DATETIME(6)``.  Public helpers accept aware
timestamps and normalize them before comparison.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import itertools
import json
import math
import re
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError

from server.common.finance_coverage import (
    finance_disclosure_gate,
    report_period_gate_applies,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
FINANCE_REVISION_TABLE = "st_pit_finance_revision"
EVENT_REVISION_TABLE = "st_pit_event_revision"
SOURCE_COVERAGE_TABLE = "st_pit_source_coverage"
FINANCE_EXPECTED_UNAVAILABLE_STATUS = "EXPECTED_UNAVAILABLE"
CNINFO_FINANCE_NONFILING_SOURCE = "cninfo.finance.nonfiling"
FINANCE_ATOMIC_BATCH_SOURCE = "probiga.finance.atomic_batch"
FINANCE_ATOMIC_BATCH_CODE = "000000"
FINANCE_ATOMIC_BATCH_SCHEMA = "probiga.pit-finance-atomic-batch.v1"
FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA = (
    "probiga.pit-finance-atomic-batch.v2"
)
FINANCE_ATOMIC_BATCH_HISTORY_LIMIT = 20
FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT = 100
COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT = 100
FINANCE_INCREMENTAL_DISCOVERY_SOURCE = "eastmoney.finance.global_discovery"
FINANCE_INCREMENTAL_DISCOVERY_CODE = "000000"
FINANCE_INCREMENTAL_DISCOVERY_SCHEMA = (
    "probiga.pit-finance-incremental-discovery.v1"
)
AUTHORITATIVE_FINANCE_SOURCES = frozenset({
    "adata.finance.core_index",
    "eastmoney.finance.mainfinadata.direct",
})
FINANCE_NONFILING_REASON_CODES = frozenset({
    "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED",
})
QMT_EVENT_SOURCE = "qmt.announcement"
CNINFO_EVENT_SOURCE = "cninfo.announcement"
EASTMONEY_EVENT_SOURCE = "eastmoney.notice"
AUTHORITATIVE_EVENT_SOURCES = frozenset(
    {QMT_EVENT_SOURCE, CNINFO_EVENT_SOURCE, EASTMONEY_EVENT_SOURCE}
)
EVENT_FALLBACK_REASON_CODES = frozenset({
    "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
    "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
    "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED",
})
PIT_FACT_TABLE_NAMES = frozenset(
    {FINANCE_REVISION_TABLE, EVENT_REVISION_TABLE, SOURCE_COVERAGE_TABLE}
)
TIME_VERIFIED = "VERIFIED_EXACT"
TIME_UNVERIFIED = "TIME_UNVERIFIED"
PIT_AVAILABLE = "AVAILABLE"
PIT_NO_ROWS = "NO_ROWS"
PIT_DATA_BLOCKED = "DATA_BLOCKED"
PIT_SCHEMA_UNAVAILABLE = "SCHEMA_UNAVAILABLE"
MAX_LIVE_CAPTURE_DELAY = timedelta(minutes=30)
CNINFO_MAX_PAGES_PER_STOCK = 200
CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS = 8
CNINFO_DATE_SHARD_SPLIT_VERSION = "MIDPOINT_INCLUSIVE_V1"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_EXACT_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
_LOCAL_INGESTION_FIELDS = frozenset(
    {
        "etl_sync_at",
        "sync_at",
        "sync_time",
        "ingested_at",
        "received_at",
        "known_at",
        "created_at",
        "updated_at",
    }
)


PIT_FACT_TABLE_DDLS: dict[str, str] = {
    FINANCE_REVISION_TABLE: f"""
        CREATE TABLE IF NOT EXISTS {FINANCE_REVISION_TABLE} (
            revision_id CHAR(64) NOT NULL,
            identity_hash CHAR(64) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            report_date DATE NOT NULL,
            report_type VARCHAR(64) NOT NULL DEFAULT '',
            published_at DATETIME(6) NULL,
            source_published_text VARCHAR(128) NOT NULL DEFAULT '',
            publication_time_status VARCHAR(32) NOT NULL,
            known_at DATETIME(6) NOT NULL,
            received_at DATETIME(6) NOT NULL,
            revision_no INT UNSIGNED NOT NULL,
            supersedes_revision_id CHAR(64) NULL,
            source VARCHAR(64) NOT NULL,
            batch_id VARCHAR(128) NOT NULL,
            content_hash CHAR(64) NOT NULL,
            revision_fingerprint_hash CHAR(64) NOT NULL,
            payload_json LONGTEXT NOT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (revision_id),
            UNIQUE KEY uk_pit_finance_identity_revision
                (identity_hash, revision_no),
            UNIQUE KEY uk_pit_finance_identity_fingerprint
                (identity_hash, revision_fingerprint_hash),
            KEY idx_pit_finance_decision
                (stock_code, report_date, known_at, published_at),
            KEY idx_pit_finance_supersedes (supersedes_revision_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    EVENT_REVISION_TABLE: f"""
        CREATE TABLE IF NOT EXISTS {EVENT_REVISION_TABLE} (
            revision_id CHAR(64) NOT NULL,
            identity_hash CHAR(64) NOT NULL,
            event_key VARCHAR(160) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            event_date DATE NULL,
            published_at DATETIME(6) NULL,
            source_published_text VARCHAR(128) NOT NULL DEFAULT '',
            publication_time_status VARCHAR(32) NOT NULL,
            known_at DATETIME(6) NOT NULL,
            received_at DATETIME(6) NOT NULL,
            revision_no INT UNSIGNED NOT NULL,
            supersedes_revision_id CHAR(64) NULL,
            source VARCHAR(64) NOT NULL,
            batch_id VARCHAR(128) NOT NULL,
            content_hash CHAR(64) NOT NULL,
            revision_fingerprint_hash CHAR(64) NOT NULL,
            payload_json LONGTEXT NOT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (revision_id),
            UNIQUE KEY uk_pit_event_identity_revision
                (identity_hash, revision_no),
            UNIQUE KEY uk_pit_event_identity_fingerprint
                (identity_hash, revision_fingerprint_hash),
            KEY idx_pit_event_decision
                (stock_code, event_date, known_at, published_at),
            KEY idx_pit_event_supersedes (supersedes_revision_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    SOURCE_COVERAGE_TABLE: f"""
        CREATE TABLE IF NOT EXISTS {SOURCE_COVERAGE_TABLE} (
            coverage_id CHAR(64) NOT NULL,
            scope_hash CHAR(64) NOT NULL,
            fact_kind VARCHAR(16) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            window_start DATE NOT NULL,
            window_end DATE NOT NULL,
            known_at DATETIME(6) NOT NULL,
            received_at DATETIME(6) NOT NULL,
            covered_through_at DATETIME(6) NOT NULL,
            watermark_kind VARCHAR(32) NOT NULL,
            watermark_hash CHAR(64) NOT NULL,
            coverage_status VARCHAR(32) NOT NULL,
            result_count INT UNSIGNED NOT NULL,
            source_response_hash CHAR(64) NOT NULL,
            fact_set_hash CHAR(64) NOT NULL,
            revision_no INT UNSIGNED NOT NULL,
            supersedes_coverage_id CHAR(64) NULL,
            source VARCHAR(64) NOT NULL,
            batch_id VARCHAR(128) NOT NULL,
            coverage_fingerprint_hash CHAR(64) NOT NULL,
            payload_json LONGTEXT NOT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (coverage_id),
            UNIQUE KEY uk_pit_coverage_scope_revision
                (scope_hash, revision_no),
            UNIQUE KEY uk_pit_coverage_scope_fingerprint
                (scope_hash, coverage_fingerprint_hash),
            KEY idx_pit_coverage_decision
                (fact_kind, stock_code, known_at, window_start, window_end),
            KEY idx_pit_coverage_supersedes (supersedes_coverage_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
}


PIT_FACT_TRIGGER_STATEMENTS: dict[str, str] = {
    "trg_pit_finance_revision_immutable_bu": f"""
        CREATE TRIGGER trg_pit_finance_revision_immutable_bu
        BEFORE UPDATE ON {FINANCE_REVISION_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT finance revisions are append-only'
    """,
    "trg_pit_finance_revision_immutable_bd": f"""
        CREATE TRIGGER trg_pit_finance_revision_immutable_bd
        BEFORE DELETE ON {FINANCE_REVISION_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT finance revisions are append-only'
    """,
    "trg_pit_event_revision_immutable_bu": f"""
        CREATE TRIGGER trg_pit_event_revision_immutable_bu
        BEFORE UPDATE ON {EVENT_REVISION_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT event revisions are append-only'
    """,
    "trg_pit_event_revision_immutable_bd": f"""
        CREATE TRIGGER trg_pit_event_revision_immutable_bd
        BEFORE DELETE ON {EVENT_REVISION_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT event revisions are append-only'
    """,
    "trg_pit_source_coverage_immutable_bu": f"""
        CREATE TRIGGER trg_pit_source_coverage_immutable_bu
        BEFORE UPDATE ON {SOURCE_COVERAGE_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT source coverage receipts are append-only'
    """,
    "trg_pit_source_coverage_immutable_bd": f"""
        CREATE TRIGGER trg_pit_source_coverage_immutable_bd
        BEFORE DELETE ON {SOURCE_COVERAGE_TABLE}
        FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'PIT source coverage receipts are append-only'
    """,
}


_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    FINANCE_REVISION_TABLE: frozenset(
        {
            "revision_id", "identity_hash", "stock_code", "report_date",
            "report_type", "published_at", "source_published_text",
            "publication_time_status", "known_at", "received_at",
            "revision_no", "supersedes_revision_id", "source", "batch_id",
            "content_hash", "revision_fingerprint_hash", "payload_json",
            "created_at",
        }
    ),
    EVENT_REVISION_TABLE: frozenset(
        {
            "revision_id", "identity_hash", "event_key", "stock_code",
            "event_date", "published_at", "source_published_text",
            "publication_time_status", "known_at", "received_at",
            "revision_no", "supersedes_revision_id", "source", "batch_id",
            "content_hash", "revision_fingerprint_hash", "payload_json",
            "created_at",
        }
    ),
    SOURCE_COVERAGE_TABLE: frozenset(
        {
            "coverage_id", "scope_hash", "fact_kind", "stock_code",
            "window_start", "window_end", "known_at", "received_at",
            "covered_through_at", "watermark_kind", "watermark_hash",
            "coverage_status", "result_count", "source_response_hash",
            "fact_set_hash", "revision_no", "supersedes_coverage_id",
            "source", "batch_id", "coverage_fingerprint_hash",
            "payload_json", "created_at",
        }
    ),
}


@dataclass(frozen=True)
class PITRevisionReceipt:
    revision_id: str
    content_hash: str
    revision_no: int
    supersedes_revision_id: str | None
    publication_time_status: str
    idempotent: bool


@dataclass(frozen=True)
class PITCoverageReceipt:
    coverage_id: str
    source_response_hash: str
    fact_set_hash: str
    revision_no: int
    supersedes_coverage_id: str | None
    covered_through_at: str
    idempotent: bool


@dataclass
class PITFactBatch:
    facts: dict[str, Any] = field(default_factory=dict)
    coverage_by_code: dict[str, dict[str, Any]] = field(default_factory=dict)
    status_by_code: dict[str, str] = field(default_factory=dict)
    reason_by_code: dict[str, str] = field(default_factory=dict)
    manifest_hash: str = ""
    decision_at: str = ""
    fact_cutoff_at: str = ""
    table_name: str = ""
    global_status: str = PIT_AVAILABLE

    def status_for(self, stock_code: str) -> str:
        return self.status_by_code.get(str(stock_code).zfill(6), self.global_status)

    def reason_for(self, stock_code: str) -> str:
        return self.reason_by_code.get(str(stock_code).zfill(6), "")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else None
    if isinstance(value, datetime):
        return _dt_text(_to_shanghai_naive(value))
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
        if value != value:  # pandas/numpy NaN without importing either package
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


def _fallback_event_receipt_valid(
    evidence: Mapping[str, Any],
    *,
    source: str,
    stock_code: str,
    result_count: int,
) -> bool:
    """Recheck fallback evidence at the immutable coverage write boundary."""

    if source == QMT_EVENT_SOURCE:
        return True
    receipt = evidence.get("provider_receipt")
    if not isinstance(receipt, Mapping) or canonical_hash(receipt) != str(
        evidence.get("provider_receipt_hash") or ""
    ):
        return False
    schema = (
        "probiga.cninfo-announcement-provider-receipt.v3"
        if source == CNINFO_EVENT_SOURCE
        else "probiga.announcement-provider-receipt.v1"
    )
    qmt_code = str(receipt.get("qmt_code") or "")
    requested_start = (
        str(evidence.get("window_start") or "").replace("-", "")
        + "000000"
    )
    if (
        receipt.get("schema") != schema
        or receipt.get("status") != "COMPLETE"
        or receipt.get("source") != source
        or str(receipt.get("stock_code") or "") != stock_code
        or re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", qmt_code) is None
        or qmt_code[:6] != stock_code
        or receipt.get("requested_start_time") != requested_start
        or receipt.get("requested_end_time")
        != evidence.get("query_end_time")
        or receipt.get("exhausted") is not True
        or type(receipt.get("result_count")) is not int
        or int(receipt["result_count"]) != result_count
        or not _HASH_RE.fullmatch(
            str(receipt.get("provider_payload_sha256") or "")
        )
    ):
        return False
    if source != CNINFO_EVENT_SOURCE:
        return True
    try:
        total = int(receipt.get("provider_total_record_count"))
        page_count = int(receipt.get("page_count"))
        expected_pages = int(receipt.get("expected_pages"))
        reported_pages = int(receipt.get("provider_reported_totalpages"))
        max_pages = int(receipt.get("max_pages_per_stock"))
        directory_count = int(receipt.get("directory_member_count"))
        requested_count = int(receipt.get("requested_catalog_member_count"))
        coverage_count = int(
            receipt.get("directory_catalog_coverage_count")
        )
        missing_count = int(
            receipt.get("directory_catalog_missing_count")
        )
        extra_count = int(receipt.get("directory_catalog_extra_count"))
    except (TypeError, ValueError, OverflowError):
        return False
    page_hashes = receipt.get("page_sha256")
    first_anchor = str(receipt.get("first_page_anchor_sha256") or "")
    last_anchor = str(receipt.get("last_page_anchor_sha256") or "")
    first_page_recheck = str(
        receipt.get("first_page_recheck_sha256") or ""
    )
    pagination_mode = str(receipt.get("pagination_mode") or "")
    master_start = str(receipt.get("security_master_sha256") or "")
    master_end = str(receipt.get("security_master_end_sha256") or "")
    directory_raw = str(receipt.get("directory_raw_sha256") or "")
    directory_manifest = str(receipt.get("directory_manifest_hash") or "")
    directory_set = str(receipt.get("directory_member_set_hash") or "")
    requested_set = str(
        receipt.get("requested_catalog_member_set_sha256") or ""
    )
    pagination_payload = {
        "pagination_mode": pagination_mode,
        "total_record_count": total,
        "reported_totalpages": reported_pages,
        "page_count": page_count,
        "page_sha256": page_hashes,
        "last_page_has_more": False,
        "first_page_sha256": first_anchor,
        "last_page_sha256": last_anchor,
        "first_page_recheck_sha256": first_page_recheck,
    }
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
    shard_mode = (
        pagination_mode == "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
    )
    if shard_mode:
        pagination_payload.update({
            "date_shard_manifest_sha256": date_shard_manifest_hash,
            "date_shard_recheck_sha256": date_shard_recheck_hash,
            "pagination_round_count": pagination_round_count,
            "pagination_query_count": pagination_query_count,
            "pagination_attempt_count": pagination_attempt_count,
            "pagination_invalid_round_count": pagination_invalid_round_count,
            "pagination_complete_round_sha256": complete_round_hashes,
            "pagination_complete_round_attempts": complete_round_attempts,
            "date_shard_split_version": split_version,
        })
    expected_pagination = canonical_hash(pagination_payload)
    expected_directory_attestation = canonical_hash({
        "directory_raw_sha256": directory_raw,
        "directory_manifest_hash": directory_manifest,
        "directory_member_set_hash": directory_set,
        "directory_member_count": directory_count,
        "security_master_start_sha256": master_start,
        "security_master_end_sha256": master_end,
        "requested_catalog_member_count": requested_count,
        "requested_catalog_member_set_sha256": requested_set,
        "directory_catalog_missing_count": missing_count,
    })
    catalog_count = int(evidence.get("catalog_member_count") or 0)
    common_pagination_valid = bool(
        total == result_count
        and expected_pages == page_count
        and max_pages == CNINFO_MAX_PAGES_PER_STOCK
        and 1 <= page_count <= max_pages
        and reported_pages == total // 30
        and isinstance(page_hashes, list)
        and len(page_hashes) == page_count
        and all(_HASH_RE.fullmatch(str(value or "")) for value in page_hashes)
        and _HASH_RE.fullmatch(first_anchor)
        and first_anchor == str(page_hashes[0])
        and _HASH_RE.fullmatch(last_anchor)
        and last_anchor == str(page_hashes[-1])
        and receipt.get("last_page_has_more") is False
        and receipt.get("pagination_sha256") == expected_pagination
    )
    single_page_proof_valid = bool(
        pagination_mode == "EXACT_STOCK_SINGLE_PAGE"
        and receipt.get("quality_status")
        == "EXACT_STOCK_PAGINATION_EXHAUSTED"
        and page_count == expected_pages == 1
        and 0 <= total <= 30
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
                requested_start[:8], "%Y%m%d"
            ).date()
            requested_end_date = datetime.strptime(
                str(evidence.get("query_end_time") or "")[:8], "%Y%m%d"
            ).date()
        except (TypeError, ValueError):
            return False
        if isinstance(date_shard_manifest, list) and date_shard_manifest:
            cursor = requested_start_date
            shard_total = 0
            tree_query_count = 2 * len(date_shard_manifest) - 1
            shard_hashes: list[str] = []
            normalized_shards: list[dict[str, Any]] = []
            required_shard_fields = {
                "window_start", "window_end", "capture_mode", "result_count",
                "provider_reported_totalpages", "provider_page_count",
                "page_sha256", "last_page_has_more",
            }
            for raw_shard in date_shard_manifest:
                if (
                    not isinstance(raw_shard, Mapping)
                    or set(raw_shard) != required_shard_fields
                ):
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
                expected_physical_pages = max(1, (shard_count + 29) // 30)
                if (
                    shard_start != cursor
                    or shard_start > shard_end
                    or shard_end > requested_end_date
                    or shard_count < 0
                    or shard_reported_pages != shard_count // 30
                    or shard_page_count != expected_physical_pages
                    or shard_page_count > max_pages
                    or _HASH_RE.fullmatch(shard_hash) is None
                    or raw_shard.get("last_page_has_more") is not False
                    or (capture_mode == "SINGLE_PAGE" and shard_count > 30)
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
                normalized_shards.append({
                    "window_start": shard_start.isoformat(),
                    "window_end": shard_end.isoformat(),
                    "capture_mode": capture_mode,
                    "result_count": shard_count,
                    "provider_reported_totalpages": shard_reported_pages,
                    "provider_page_count": shard_page_count,
                    "page_sha256": shard_hash,
                    "last_page_has_more": False,
                })
                shard_hashes.append(shard_hash)
                shard_total += shard_count
                tree_query_count += shard_page_count - 1
                cursor = shard_end + timedelta(days=1)
            expected_shard_hash = canonical_hash({
                "schema": "probiga.cninfo-date-shard-manifest.v1",
                "stock_code": stock_code,
                "requested_start_time": requested_start,
                "requested_end_time": evidence.get("query_end_time"),
                "shards": normalized_shards,
            })
            round_hashes_valid = bool(
                isinstance(complete_round_hashes, list)
                and len(complete_round_hashes) == pagination_round_count
                and all(
                    _HASH_RE.fullmatch(str(value or ""))
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
                and complete_round_attempts == sorted(set(complete_round_attempts))
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
                and shard_total == total == result_count
                and date_shard_manifest_hash == expected_shard_hash
                and date_shard_recheck_hash == expected_shard_hash
                and split_version == CNINFO_DATE_SHARD_SPLIT_VERSION
                and receipt.get("quality_status")
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
                and 2 * tree_query_count <= pagination_query_count <= max_pages
                and first_page_recheck == ""
            )
    return bool(
        str(receipt.get("org_id") or "").strip()
        and receipt.get("query_stock_identity")
        == f"{stock_code},{receipt.get('org_id')}"
        and receipt.get("permission_status") == "PUBLIC"
        and common_pagination_valid
        and (single_page_proof_valid or shard_proof_valid)
        and _HASH_RE.fullmatch(master_start)
        and master_start == master_end == directory_manifest
        and all(_HASH_RE.fullmatch(value) for value in (
            directory_raw, directory_manifest, directory_set, requested_set
        ))
        and receipt.get("directory_attestation_sha256")
        == expected_directory_attestation
        and requested_count == coverage_count == catalog_count
        and missing_count == 0
        and directory_count >= requested_count
        and extra_count == directory_count - requested_count
    )


def _source_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    """Remove local clock fields that are evidence metadata, not source content.

    Ingestion timestamps live in dedicated immutable columns.  Including them
    in ``content_hash`` would manufacture a new revision on every idempotent
    synchronizer run even when the upstream fact did not change.
    """

    return {
        str(key): value
        for key, value in row.items()
        if str(key).strip().lower() not in _LOCAL_INGESTION_FIELDS
    }


def _dt_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f")


def _to_shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(SHANGHAI).replace(tzinfo=None)


def normalize_decision_at(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _to_shanghai_naive(value)
    raw = str(value or "").strip()
    if not raw or len(raw) <= 10:
        raise ValueError("decision_at must contain an exact Asia/Shanghai time")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return _to_shanghai_naive(parsed)


def _fact_and_decision_times(
    *,
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None,
) -> tuple[datetime, datetime]:
    decision = normalize_decision_at(decision_at)
    fact_cutoff = normalize_decision_at(
        fact_cutoff_at if fact_cutoff_at is not None else decision
    )
    if fact_cutoff > decision:
        raise ValueError("fact_cutoff_at cannot be later than decision_at")
    return fact_cutoff, decision


def _live_capture_allowed(
    *, fact_cutoff_at: datetime, decision_at: datetime, known_at: datetime,
) -> bool:
    """Allow post-cutoff receipt knowledge only in one bounded live run."""

    if known_at <= fact_cutoff_at:
        return True
    return (
        timedelta(0) <= known_at - fact_cutoff_at <= MAX_LIVE_CAPTURE_DELAY
        and timedelta(0)
        <= decision_at - fact_cutoff_at
        <= MAX_LIVE_CAPTURE_DELAY
    )


def normalize_published_at(value: Any) -> tuple[datetime | None, str, str]:
    """Return exact Shanghai time or a fail-closed TIME_UNVERIFIED marker."""

    source_text = str(value or "").strip()[:128]
    if isinstance(value, str) and _EXACT_DATETIME_RE.fullmatch(value.strip()):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            normalized = _to_shanghai_naive(parsed)
            # Several upstream date-only feeds serialize an unknown time as
            # ``00:00:00``.  Treating that sentinel as an exact publication
            # instant would let an after-close disclosure leak into the same
            # day's decision.  A real midnight publication therefore remains
            # conservatively blocked unless the source later supplies a
            # distinguishable exact timestamp.
            if (
                normalized.hour
                == normalized.minute
                == normalized.second
                == normalized.microsecond
                == 0
            ):
                return None, TIME_UNVERIFIED, source_text
            return normalized, TIME_VERIFIED, source_text
        except ValueError:
            pass
    if isinstance(value, datetime) and not (
        value.hour == value.minute == value.second == value.microsecond == 0
    ):
        return _to_shanghai_naive(value), TIME_VERIFIED, source_text
    return None, TIME_UNVERIFIED, source_text


def _date_value(value: Any, *, required: bool) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()[:10]
    if raw:
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
    if required:
        raise ValueError("required PIT fact date is missing or invalid")
    return None


def _source_publication_date(row: Mapping[str, Any]) -> date | None:
    value = row.get("source_published_text")
    return _date_value(value, required=False)


def _connection(target: Engine | Connection) -> tuple[Connection, bool]:
    if isinstance(target, Connection):
        return target, False
    return target.connect(), True


def _mapping(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    mapping = getattr(row, "_mapping", row)
    return dict(mapping)


def _latest_revision(
    connection: Connection,
    table_name: str,
    identity_hash: str,
) -> dict[str, Any] | None:
    lock = " FOR UPDATE" if connection.dialect.name == "mysql" else ""
    row = connection.execute(
        text(
            f"SELECT revision_id, content_hash, revision_no, "
            f"supersedes_revision_id, publication_time_status, published_at, "
            f"source_published_text, revision_fingerprint_hash "
            f"FROM {table_name} WHERE identity_hash=:identity_hash "
            f"ORDER BY revision_no DESC LIMIT 1{lock}"
        ),
        {"identity_hash": identity_hash},
    ).first()
    return _mapping(row)


def _existing_fingerprint(
    connection: Connection,
    table_name: str,
    identity_hash: str,
    fingerprint: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        text(
            f"SELECT revision_id, content_hash, revision_no, "
            f"supersedes_revision_id, publication_time_status "
            f"FROM {table_name} WHERE identity_hash=:identity_hash "
            "AND revision_fingerprint_hash=:fingerprint LIMIT 1"
        ),
        {"identity_hash": identity_hash, "fingerprint": fingerprint},
    ).first()
    return _mapping(row)


def _receipt(row: Mapping[str, Any], *, idempotent: bool) -> PITRevisionReceipt:
    return PITRevisionReceipt(
        revision_id=str(row.get("revision_id") or ""),
        content_hash=str(row.get("content_hash") or ""),
        revision_no=int(row.get("revision_no") or 0),
        supersedes_revision_id=(
            str(row.get("supersedes_revision_id"))
            if row.get("supersedes_revision_id") else None
        ),
        publication_time_status=str(row.get("publication_time_status") or ""),
        idempotent=idempotent,
    )


def _append_revision(
    connection: Connection,
    *,
    table_name: str,
    fact_kind: str,
    identity_payload: Mapping[str, Any],
    row_columns: Mapping[str, Any],
    payload: Mapping[str, Any],
    published_value: Any,
    known_at: datetime | str,
    received_at: datetime | str | None,
    source: str,
    batch_id: str,
) -> PITRevisionReceipt:
    known = normalize_decision_at(known_at)
    received = normalize_decision_at(received_at or known)
    if received > known:
        known = received
    published, time_status, source_published_text = normalize_published_at(
        published_value
    )
    normalized_payload = _json_safe(dict(payload))
    content_hash = canonical_hash(
        {"schema": f"probiga.pit-{fact_kind}-payload.v1", "payload": normalized_payload}
    )
    identity_hash = canonical_hash(
        {"schema": f"probiga.pit-{fact_kind}-identity.v1", **identity_payload}
    )
    latest = _latest_revision(connection, table_name, identity_hash)
    latest_published = (
        _dt_text(_row_datetime(latest["published_at"]))
        if latest and latest.get("published_at") is not None
        else None
    )
    if latest is not None and (
        str(latest.get("content_hash") or "") == content_hash
        and str(latest.get("publication_time_status") or "") == time_status
        and latest_published == (_dt_text(published) if published else None)
        and str(latest.get("source_published_text") or "")
        == source_published_text
    ):
        return _receipt(latest, idempotent=True)
    revision_no = int((latest or {}).get("revision_no") or 0) + 1
    supersedes = str((latest or {}).get("revision_id") or "") or None
    fingerprint = canonical_hash(
        {
            "schema": f"probiga.pit-{fact_kind}-revision-fingerprint.v2",
            "identity_hash": identity_hash,
            "content_hash": content_hash,
            "publication_time_status": time_status,
            "published_at": _dt_text(published) if published else None,
            "source_published_text": source_published_text,
            "supersedes_revision_id": supersedes,
        }
    )
    existing = _existing_fingerprint(
        connection, table_name, identity_hash, fingerprint
    )
    if existing is not None:
        return _receipt(existing, idempotent=True)
    revision_id = canonical_hash(
        {
            "schema": f"probiga.pit-{fact_kind}-revision-id.v1",
            "identity_hash": identity_hash,
            "revision_fingerprint_hash": fingerprint,
        }
    )
    values = {
        "revision_id": revision_id,
        "identity_hash": identity_hash,
        **dict(row_columns),
        "published_at": published,
        "source_published_text": source_published_text,
        "publication_time_status": time_status,
        "known_at": known,
        "received_at": received,
        "revision_no": revision_no,
        "supersedes_revision_id": supersedes,
        "source": str(source or "UNKNOWN")[:64],
        "batch_id": str(batch_id or canonical_hash({"source": source, "known": _dt_text(known)}))[:128],
        "content_hash": content_hash,
        "revision_fingerprint_hash": fingerprint,
        "payload_json": canonical_json(normalized_payload),
        "created_at": known,
    }
    columns = list(values)
    statement = text(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
        f"({', '.join(':' + column for column in columns)})"
    )
    try:
        connection.execute(statement, values)
    except IntegrityError:
        existing = _existing_fingerprint(
            connection, table_name, identity_hash, fingerprint
        )
        if existing is None:
            raise
        return _receipt(existing, idempotent=True)
    return PITRevisionReceipt(
        revision_id=revision_id,
        content_hash=content_hash,
        revision_no=revision_no,
        supersedes_revision_id=supersedes,
        publication_time_status=time_status,
        idempotent=False,
    )


def append_finance_revision(
    target: Engine | Connection,
    row: Mapping[str, Any],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str = "adata.finance.core_index",
    batch_id: str = "",
) -> PITRevisionReceipt:
    code = str(row.get("stock_code") or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("finance PIT revision requires a six-digit stock code")
    report_date = _date_value(row.get("report_date"), required=True)
    report_type = str(row.get("report_type") or "").strip()[:64]
    connection, close = _connection(target)
    transaction = None
    if close:
        transaction = connection.begin()
    try:
        receipt = _append_revision(
            connection,
            table_name=FINANCE_REVISION_TABLE,
            fact_kind="finance",
            identity_payload={
                "stock_code": code,
                "report_date": report_date.isoformat(),
                "report_type": report_type,
            },
            row_columns={
                "stock_code": code,
                "report_date": report_date,
                "report_type": report_type,
            },
            payload=_source_payload(row),
            published_value=row.get("published_at", row.get("notice_date")),
            known_at=known_at,
            received_at=received_at,
            source=source,
            batch_id=batch_id,
        )
        if transaction is not None:
            transaction.commit()
        return receipt
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


def append_event_revision(
    target: Engine | Connection,
    row: Mapping[str, Any],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str = "eastmoney.notice",
    batch_id: str = "",
) -> PITRevisionReceipt:
    code = str(row.get("stock_code") or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("event PIT revision requires a six-digit stock code")
    event_key = str(
        row.get("event_key") or row.get("art_code") or ""
    ).strip()[:160]
    if not event_key:
        raise ValueError("event PIT revision requires a stable source event key")
    published_value = row.get("published_at", row.get("display_time"))
    event_date = _date_value(
        row.get("event_date", row.get("notice_date")), required=False
    )
    if event_date is None:
        event_date = _date_value(published_value, required=False)
    connection, close = _connection(target)
    transaction = None
    if close:
        transaction = connection.begin()
    try:
        receipt = _append_revision(
            connection,
            table_name=EVENT_REVISION_TABLE,
            fact_kind="event",
            identity_payload={
                "source": str(source or "UNKNOWN")[:64],
                "event_key": event_key,
                "stock_code": code,
            },
            row_columns={
                "event_key": event_key,
                "stock_code": code,
                "event_date": event_date,
            },
            payload=_source_payload(row),
            published_value=published_value,
            known_at=known_at,
            received_at=received_at,
            source=source,
            batch_id=batch_id,
        )
        if transaction is not None:
            transaction.commit()
        return receipt
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


def _coverage_receipt(
    row: Mapping[str, Any], *, idempotent: bool
) -> PITCoverageReceipt:
    return PITCoverageReceipt(
        coverage_id=str(row.get("coverage_id") or ""),
        source_response_hash=str(row.get("source_response_hash") or ""),
        fact_set_hash=str(row.get("fact_set_hash") or ""),
        revision_no=int(row.get("revision_no") or 0),
        supersedes_coverage_id=(
            str(row.get("supersedes_coverage_id"))
            if row.get("supersedes_coverage_id")
            else None
        ),
        covered_through_at=str(row.get("covered_through_at") or ""),
        idempotent=idempotent,
    )


def append_source_coverage(
    target: Engine | Connection,
    *,
    fact_kind: str,
    stock_code: str,
    window_start: date | str,
    window_end: date | str,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    covered_through_at: datetime | str,
    watermark_kind: str,
    watermark_evidence: Mapping[str, Any],
    source_rows: Iterable[Mapping[str, Any]],
    fact_bindings: Iterable[Mapping[str, Any]],
    source: str,
    batch_id: str,
) -> PITCoverageReceipt:
    """Append a hash-bound successful source-query coverage receipt.

    A receipt is never a generic freshness assertion.  Its immutable source
    response, fact bindings and high-watermark are bound together, and an
    empty result may only be consumed for decisions at or before that exact
    watermark.  Callers must not invoke this helper after a failed/partial
    fetch.
    """

    kind = str(fact_kind or "").strip().lower()
    if kind not in {"finance", "event"}:
        raise ValueError("coverage fact_kind must be finance or event")
    code = str(stock_code or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("coverage receipt requires a six-digit stock code")
    start = _date_value(window_start, required=True)
    end = _date_value(window_end, required=True)
    if start is None or end is None or start > end:
        raise ValueError("coverage window is invalid")
    known = normalize_decision_at(known_at)
    received = normalize_decision_at(received_at or known)
    if received > known:
        known = received
    covered = normalize_decision_at(covered_through_at)
    watermark_type = str(watermark_kind or "").strip().upper()
    if watermark_type not in {
        "CAPTURED_AT", "SOURCE_SERVER_TIME", "QUERY_CUTOFF",
        "HISTORICAL_RECONSTRUCTION",
    }:
        raise ValueError("coverage watermark kind is not authoritative")
    if covered > known:
        raise ValueError("coverage watermark cannot be later than local knowledge")
    if watermark_type == "CAPTURED_AT" and covered != known:
        raise ValueError("captured-at watermark must equal local knowledge time")
    if not isinstance(watermark_evidence, Mapping) or not watermark_evidence:
        raise ValueError("coverage watermark evidence is required")
    normalized_watermark_evidence = _json_safe(dict(watermark_evidence))

    normalized_rows = [_json_safe(_source_payload(row)) for row in source_rows]
    normalized_rows.sort(key=canonical_json)
    normalized_bindings = [_json_safe(dict(row)) for row in fact_bindings]
    normalized_bindings.sort(key=canonical_json)
    if len(normalized_rows) != len(normalized_bindings):
        raise ValueError("coverage response rows and fact bindings differ")
    for binding in normalized_bindings:
        if not isinstance(binding, dict):
            raise ValueError("coverage fact binding is malformed")
        if not _HASH_RE.fullmatch(str(binding.get("revision_id") or "")):
            raise ValueError("coverage fact binding revision_id is invalid")
        if not _HASH_RE.fullmatch(str(binding.get("content_hash") or "")):
            raise ValueError("coverage fact binding content_hash is invalid")

    source_response_hash = canonical_hash(
        {
            "schema": f"probiga.pit-{kind}-source-response.v1",
            "rows": normalized_rows,
        }
    )
    fact_set_hash = canonical_hash(
        {
            "schema": f"probiga.pit-{kind}-fact-set.v1",
            "bindings": normalized_bindings,
        }
    )
    source_name = str(source or "UNKNOWN")[:64]
    if watermark_type == "SOURCE_SERVER_TIME":
        source_server_at = normalize_decision_at(
            str((normalized_watermark_evidence or {}).get("source_server_at") or "")
        )
        response_binding = str(
            (normalized_watermark_evidence or {}).get("source_response_hash")
            or ""
        )
        if source_server_at != covered:
            raise ValueError("source-server watermark differs from response time")
        if response_binding != source_response_hash:
            raise ValueError("source-server watermark is not bound to response")
    elif watermark_type in {"QUERY_CUTOFF", "HISTORICAL_RECONSTRUCTION"}:
        if not isinstance(normalized_watermark_evidence, dict):
            raise ValueError("query-cutoff watermark evidence is malformed")
        evidence_cutoff = normalize_decision_at(
            str(normalized_watermark_evidence.get("fact_cutoff_at") or "")
        )
        evidence_received = normalize_decision_at(
            str(normalized_watermark_evidence.get("received_at") or "")
        )
        evidence_decision = normalize_decision_at(
            str(normalized_watermark_evidence.get("decision_at") or "")
        )
        if (
            evidence_cutoff != covered
            or evidence_received != known
            or evidence_decision != known
        ):
            raise ValueError("query-cutoff evidence timestamps differ")
        historical = watermark_type == "HISTORICAL_RECONSTRUCTION"
        if not historical and not (
            timedelta(0) <= known - covered <= MAX_LIVE_CAPTURE_DELAY
        ):
            raise ValueError("query-cutoff capture exceeded the live bound")
        if (
            str(normalized_watermark_evidence.get("source_response_hash") or "")
            != source_response_hash
        ):
            raise ValueError("query-cutoff is not bound to the source response")
        query_end = str(
            normalized_watermark_evidence.get("query_end_time") or ""
        )
        if query_end != covered.strftime("%Y%m%d%H%M%S"):
            raise ValueError("query-cutoff end time differs")
        required_hashes = (
            "global_batch_root_hash", "catalog_manifest_hash",
            "catalog_member_set_hash",
        )
        if any(
            not _HASH_RE.fullmatch(
                str(normalized_watermark_evidence.get(field) or "")
            )
            for field in required_hashes
        ):
            raise ValueError("query-cutoff batch/catalog hash is invalid")
        provider_name = str(
            normalized_watermark_evidence.get("provider") or ""
        )
        fallback_valid = bool(
            source_name == QMT_EVENT_SOURCE
            or (
                source_name in {CNINFO_EVENT_SOURCE, EASTMONEY_EVENT_SOURCE}
                and normalized_watermark_evidence.get("primary_provider")
                == QMT_EVENT_SOURCE
                and str(
                    normalized_watermark_evidence.get("fallback_reason") or ""
                ) in EVENT_FALLBACK_REASON_CODES
            )
        )
        fallback_receipt_valid = _fallback_event_receipt_valid(
            normalized_watermark_evidence,
            source=source_name,
            stock_code=code,
            result_count=len(normalized_rows),
        )
        if (
            provider_name != source_name
            or source_name not in AUTHORITATIVE_EVENT_SOURCES
            or not fallback_valid
            or not fallback_receipt_valid
            or str(normalized_watermark_evidence.get("period") or "")
            != "announcement"
            or not str(
                normalized_watermark_evidence.get("catalog_batch_id") or ""
            )
            or not isinstance(
                normalized_watermark_evidence.get("catalog_member_count"), int
            )
            or int(normalized_watermark_evidence["catalog_member_count"]) <= 0
        ):
            raise ValueError("query-cutoff provider/catalog evidence differs")
        if historical:
            reconstruction = normalized_watermark_evidence.get(
                "reconstruction_provenance"
            )
            if not isinstance(reconstruction, dict):
                raise ValueError("historical reconstruction evidence is absent")
            reconstruction_core = {
                key: value
                for key, value in reconstruction.items()
                if key != "reconstruction_sha256"
            }
            if (
                kind != "event"
                or source_name != CNINFO_EVENT_SOURCE
                or reconstruction.get("schema")
                != "probiga.qmt-announcement-historical-reconstruction.v2"
                or reconstruction.get("mode") != "HISTORICAL_RECONSTRUCTION"
                or canonical_hash(reconstruction_core)
                != reconstruction.get("reconstruction_sha256")
                or normalize_decision_at(
                    reconstruction.get("source_query_cutoff_at")
                ) != covered
                or normalize_decision_at(
                    reconstruction.get("reconstructed_at")
                ) != known
                or normalize_decision_at(reconstruction.get("known_at"))
                != known
                or reconstruction.get("provider") != source_name
                or reconstruction.get("source") != source_name
                or reconstruction.get("automatic_real_order_submission")
                is not False
                or reconstruction.get("real_order_authority") is not False
            ):
                raise ValueError("historical reconstruction evidence differs")
    elif isinstance(normalized_watermark_evidence, dict):
        # The local captured-at watermark is produced inside this immutable
        # receipt, rather than trusting a caller-supplied future timestamp.
        normalized_watermark_evidence = {
            **normalized_watermark_evidence,
            "captured_at": _dt_text(known),
        }
    watermark_payload = {
        "schema": "probiga.pit-source-watermark.v1",
        "kind": watermark_type,
        "covered_through_at": _dt_text(covered),
        "source_response_hash": source_response_hash,
        "evidence": normalized_watermark_evidence,
    }
    watermark_hash = canonical_hash(watermark_payload)
    if watermark_type in {"QUERY_CUTOFF", "HISTORICAL_RECONSTRUCTION"} and (
        kind != "event" or source_name not in AUTHORITATIVE_EVENT_SOURCES
    ):
        raise ValueError("query-cutoff is reserved for authoritative announcements")
    scope_hash = canonical_hash(
        {
            "schema": "probiga.pit-source-coverage-scope.v1",
            "fact_kind": kind,
            "stock_code": code,
            "source": source_name,
        }
    )
    connection, close = _connection(target)
    transaction = connection.begin() if close else None
    try:
        lock = " FOR UPDATE" if connection.dialect.name == "mysql" else ""
        latest = _mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
                    "WHERE scope_hash=:scope_hash "
                    f"ORDER BY revision_no DESC LIMIT 1{lock}"
                ),
                {"scope_hash": scope_hash},
            ).first()
        )
        revision_no = int((latest or {}).get("revision_no") or 0) + 1
        supersedes = str((latest or {}).get("coverage_id") or "") or None
        payload = {
            "schema": "probiga.pit-source-coverage-payload.v1",
            "fact_kind": kind,
            "stock_code": code,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "known_at": _dt_text(known),
            "received_at": _dt_text(received),
            "covered_through_at": _dt_text(covered),
            "watermark": watermark_payload,
            "result_count": len(normalized_rows),
            "source_response_hash": source_response_hash,
            "fact_set_hash": fact_set_hash,
            "source_rows": normalized_rows,
            "fact_bindings": normalized_bindings,
        }
        if latest is not None and str(latest.get("payload_json") or "") == canonical_json(payload):
            if transaction is not None:
                transaction.commit()
            return _coverage_receipt(latest, idempotent=True)
        fingerprint = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-fingerprint.v1",
                "scope_hash": scope_hash,
                "payload": payload,
                "supersedes_coverage_id": supersedes,
            }
        )
        existing = _mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
                    "WHERE scope_hash=:scope_hash "
                    "AND coverage_fingerprint_hash=:fingerprint LIMIT 1"
                ),
                {"scope_hash": scope_hash, "fingerprint": fingerprint},
            ).first()
        )
        if existing is not None:
            if transaction is not None:
                transaction.commit()
            return _coverage_receipt(existing, idempotent=True)
        coverage_id = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-id.v1",
                "scope_hash": scope_hash,
                "coverage_fingerprint_hash": fingerprint,
            }
        )
        values = {
            "coverage_id": coverage_id,
            "scope_hash": scope_hash,
            "fact_kind": kind,
            "stock_code": code,
            "window_start": start,
            "window_end": end,
            "known_at": known,
            "received_at": received,
            "covered_through_at": covered,
            "watermark_kind": watermark_type,
            "watermark_hash": watermark_hash,
            "coverage_status": "COMPLETE",
            "result_count": len(normalized_rows),
            "source_response_hash": source_response_hash,
            "fact_set_hash": fact_set_hash,
            "revision_no": revision_no,
            "supersedes_coverage_id": supersedes,
            "source": source_name,
            "batch_id": str(batch_id or "")[:128],
            "coverage_fingerprint_hash": fingerprint,
            "payload_json": canonical_json(payload),
            "created_at": known,
        }
        columns = list(values)
        connection.execute(
            text(
                f"INSERT INTO {SOURCE_COVERAGE_TABLE} "
                f"({', '.join(columns)}) VALUES "
                f"({', '.join(':' + column for column in columns)})"
            ),
            values,
        )
        if transaction is not None:
            transaction.commit()
        return PITCoverageReceipt(
            coverage_id=coverage_id,
            source_response_hash=source_response_hash,
            fact_set_hash=fact_set_hash,
            revision_no=revision_no,
            supersedes_coverage_id=supersedes,
            covered_through_at=_dt_text(covered),
            idempotent=False,
        )
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


def append_finance_expected_unavailable(
    target: Engine | Connection,
    *,
    stock_code: str,
    expected_report_date: date | str,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    official_evidence: Mapping[str, Any],
    source: str = CNINFO_FINANCE_NONFILING_SOURCE,
    batch_id: str,
) -> PITCoverageReceipt:
    """Append a short-lived official non-filing disposition.

    This is deliberately not a COMPLETE source-coverage receipt and carries no
    finance fact binding.  It only proves that the required report does not
    legally exist yet, so governance can distinguish expected-unavailable from
    a fetch failure and retry on the recorded date.
    """

    code = str(stock_code or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("finance non-filing receipt requires a six-digit code")
    report_date = _date_value(expected_report_date, required=True)
    if report_date is None:
        raise ValueError("finance non-filing receipt requires a report date")
    known = normalize_decision_at(known_at)
    received = normalize_decision_at(received_at or known)
    if received > known:
        raise ValueError("finance non-filing receipt was received in the future")
    source_name = str(source or "")[:64]
    if source_name != CNINFO_FINANCE_NONFILING_SOURCE:
        raise ValueError("finance non-filing source is not authoritative")
    if not isinstance(official_evidence, Mapping):
        raise ValueError("finance non-filing official evidence is required")
    evidence = _json_safe(dict(official_evidence))
    if (
        str(evidence.get("source") or "") != source_name
        or str(evidence.get("stock_code") or "").zfill(6) != code
        or str(evidence.get("expected_report_date") or "")
        != report_date.isoformat()
        or str(evidence.get("reason_code") or "")
        not in FINANCE_NONFILING_REASON_CODES
    ):
        raise ValueError("finance non-filing evidence identity differs")
    title = str(evidence.get("announcement_title") or "")
    if not (
        "未在规定期限内披露定期报告" in title
        or "无法在法定期限内披露定期报告" in title
        or ("无法在规定期限内披露" in title and "年度报告" in title)
    ):
        raise ValueError("finance non-filing announcement title is not dispositive")
    announcement_id = str(evidence.get("announcement_id") or "")
    announcement_url = str(evidence.get("announcement_url") or "")
    if (
        not re.fullmatch(r"\d+", announcement_id)
        or not re.fullmatch(
            rf"https://static\.cninfo\.com\.cn/finalpage/"
            rf"\d{{4}}-\d{{2}}-\d{{2}}/{re.escape(announcement_id)}\.PDF",
            announcement_url,
        )
        or any(
            not _HASH_RE.fullmatch(str(evidence.get(field) or ""))
            for field in (
                "announcement_document_sha256",
                "catalog_response_sha256",
            )
        )
    ):
        raise ValueError("finance non-filing official document binding differs")
    published = normalize_decision_at(
        str(evidence.get("announcement_published_at") or "")
    )
    valid_from = _date_value(evidence.get("valid_from"), required=True)
    valid_until = _date_value(evidence.get("valid_until"), required=True)
    next_retry = _date_value(evidence.get("next_retry_date"), required=True)
    if (
        valid_from is None
        or valid_until is None
        or next_retry is None
        or valid_from != published.date()
        or valid_until < known.date()
        or valid_until > valid_from + timedelta(days=7)
        or not (known.date() <= next_retry <= valid_until)
    ):
        raise ValueError("finance non-filing evidence validity differs")

    response_payload = {
        "schema": "probiga.pit-finance-expected-unavailable-response.v1",
        "official_evidence": evidence,
    }
    source_response_hash = canonical_hash(response_payload)
    fact_set_hash = canonical_hash({
        "schema": "probiga.pit-finance-expected-unavailable-fact-set.v1",
        "bindings": [],
    })
    watermark_payload = {
        "schema": "probiga.pit-finance-expected-unavailable-watermark.v1",
        "kind": "REGULATORY_NONFILING",
        "known_at": _dt_text(known),
        "valid_from": valid_from.isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_retry_date": next_retry.isoformat(),
        "source_response_hash": source_response_hash,
    }
    watermark_hash = canonical_hash(watermark_payload)
    payload = {
        "schema": "probiga.pit-finance-expected-unavailable-payload.v1",
        "fact_kind": "finance",
        "stock_code": code,
        "expected_report_date": report_date.isoformat(),
        "known_at": _dt_text(known),
        "received_at": _dt_text(received),
        "coverage_status": FINANCE_EXPECTED_UNAVAILABLE_STATUS,
        "reason_code": str(evidence["reason_code"]),
        "source": source_name,
        "official_evidence": evidence,
        "source_response_hash": source_response_hash,
        "fact_set_hash": fact_set_hash,
        "watermark": watermark_payload,
    }
    scope_hash = canonical_hash({
        "schema": "probiga.pit-source-coverage-scope.v1",
        "fact_kind": "finance",
        "stock_code": code,
        "source": source_name,
    })
    connection, close = _connection(target)
    transaction = connection.begin() if close else None
    try:
        lock = " FOR UPDATE" if connection.dialect.name == "mysql" else ""
        latest = _mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
                    "WHERE scope_hash=:scope_hash "
                    f"ORDER BY revision_no DESC LIMIT 1{lock}"
                ),
                {"scope_hash": scope_hash},
            ).first()
        )
        if latest is not None and (
            str(latest.get("coverage_status") or "")
            != FINANCE_EXPECTED_UNAVAILABLE_STATUS
        ):
            raise ValueError("finance non-filing scope contains another disposition")
        if (
            latest is not None
            and str(latest.get("payload_json") or "") == canonical_json(payload)
        ):
            if transaction is not None:
                transaction.commit()
            return _coverage_receipt(latest, idempotent=True)
        revision_no = int((latest or {}).get("revision_no") or 0) + 1
        supersedes = str((latest or {}).get("coverage_id") or "") or None
        fingerprint = canonical_hash({
            "schema": "probiga.pit-source-coverage-fingerprint.v1",
            "scope_hash": scope_hash,
            "payload": payload,
            "supersedes_coverage_id": supersedes,
        })
        coverage_id = canonical_hash({
            "schema": "probiga.pit-source-coverage-id.v1",
            "scope_hash": scope_hash,
            "coverage_fingerprint_hash": fingerprint,
        })
        values = {
            "coverage_id": coverage_id,
            "scope_hash": scope_hash,
            "fact_kind": "finance",
            "stock_code": code,
            "window_start": report_date,
            "window_end": report_date,
            "known_at": known,
            "received_at": received,
            "covered_through_at": known,
            "watermark_kind": "REGULATORY_NONFILING",
            "watermark_hash": watermark_hash,
            "coverage_status": FINANCE_EXPECTED_UNAVAILABLE_STATUS,
            "result_count": 0,
            "source_response_hash": source_response_hash,
            "fact_set_hash": fact_set_hash,
            "revision_no": revision_no,
            "supersedes_coverage_id": supersedes,
            "source": source_name,
            "batch_id": str(batch_id or "")[:128],
            "coverage_fingerprint_hash": fingerprint,
            "payload_json": canonical_json(payload),
            "created_at": known,
        }
        columns = list(values)
        connection.execute(
            text(
                f"INSERT INTO {SOURCE_COVERAGE_TABLE} "
                f"({', '.join(columns)}) VALUES "
                f"({', '.join(':' + column for column in columns)})"
            ),
            values,
        )
        if transaction is not None:
            transaction.commit()
        return PITCoverageReceipt(
            coverage_id=coverage_id,
            source_response_hash=source_response_hash,
            fact_set_hash=fact_set_hash,
            revision_no=revision_no,
            supersedes_coverage_id=supersedes,
            covered_through_at=_dt_text(known),
            idempotent=False,
        )
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


def _parse_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("payload_json")
    try:
        payload = json.loads(str(raw or ""))
    except (TypeError, ValueError) as exc:
        raise ValueError("PIT revision payload is not canonical JSON") from exc
    if not isinstance(payload, dict) or canonical_json(payload) != str(raw):
        raise ValueError("PIT revision payload is not canonical JSON")
    return payload


def _row_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _to_shanghai_naive(value)
    return normalize_decision_at(str(value or ""))


def _has_source_publication_date(row: Mapping[str, Any]) -> bool:
    return _date_value(row.get("source_published_text"), required=False) is not None


def _validate_chain(rows: list[dict[str, Any]], fact_kind: str) -> None:
    previous_id: str | None = None
    previous_known: datetime | None = None
    for expected_no, row in enumerate(rows, 1):
        if int(row.get("revision_no") or 0) != expected_no:
            raise ValueError("PIT revision chain has a sequence gap")
        supersedes = str(row.get("supersedes_revision_id") or "") or None
        if supersedes != previous_id:
            raise ValueError("PIT revision supersedes chain is broken")
        known = _row_datetime(row.get("known_at"))
        received = _row_datetime(row.get("received_at"))
        if received > known or (previous_known is not None and known < previous_known):
            raise ValueError("PIT revision knowledge-time chain is invalid")
        payload = _parse_payload(row)
        expected_content = canonical_hash(
            {"schema": f"probiga.pit-{fact_kind}-payload.v1", "payload": payload}
        )
        if str(row.get("content_hash") or "") != expected_content:
            raise ValueError("PIT revision content hash differs")
        identity_payload = (
            {
                "stock_code": str(row.get("stock_code") or ""),
                "report_date": _date_value(row.get("report_date"), required=True).isoformat(),
                "report_type": str(row.get("report_type") or ""),
            }
            if fact_kind == "finance"
            else {
                "source": str(row.get("source") or "")[:64],
                "event_key": str(row.get("event_key") or ""),
                "stock_code": str(row.get("stock_code") or ""),
            }
        )
        expected_identity = canonical_hash(
            {"schema": f"probiga.pit-{fact_kind}-identity.v1", **identity_payload}
        )
        if str(row.get("identity_hash") or "") != expected_identity:
            raise ValueError("PIT revision identity hash differs")
        published = row.get("published_at")
        published_dt = _row_datetime(published) if published is not None else None
        expected_fingerprint = canonical_hash(
            {
                "schema": f"probiga.pit-{fact_kind}-revision-fingerprint.v2",
                "identity_hash": expected_identity,
                "content_hash": expected_content,
                "publication_time_status": str(row.get("publication_time_status") or ""),
                "published_at": _dt_text(published_dt) if published_dt else None,
                "source_published_text": str(row.get("source_published_text") or ""),
                "supersedes_revision_id": supersedes,
            }
        )
        if str(row.get("revision_fingerprint_hash") or "") != expected_fingerprint:
            raise ValueError("PIT revision fingerprint differs")
        expected_revision_id = canonical_hash(
            {
                "schema": f"probiga.pit-{fact_kind}-revision-id.v1",
                "identity_hash": expected_identity,
                "revision_fingerprint_hash": expected_fingerprint,
            }
        )
        if str(row.get("revision_id") or "") != expected_revision_id:
            raise ValueError("PIT revision id differs")
        previous_id = expected_revision_id
        previous_known = known


def _latest_revision_for_fact_cutoff(
    rows: list[dict[str, Any]],
    *,
    fact_kind: str,
    fact_cutoff_at: datetime,
    decision_at: datetime,
) -> tuple[dict[str, Any] | None, str]:
    """Select the latest revision eligible for market cutoff T and decision E."""

    _validate_chain(rows, fact_kind)
    eligible: dict[str, Any] | None = None
    reason = ""
    previous_publication: datetime | None = None
    previous_source_date: date | None = None
    for index, row in enumerate(rows):
        known = _row_datetime(row.get("known_at"))
        exact = (
            row.get("publication_time_status") == TIME_VERIFIED
            and row.get("published_at") is not None
        )
        source_date = _source_publication_date(row)
        if exact:
            published = _row_datetime(row.get("published_at"))
            if published > fact_cutoff_at:
                reason = (
                    f"PIT_{fact_kind.upper()}_PUBLISHED_AFTER_DECISION"
                    if fact_cutoff_at == decision_at
                    else f"PIT_{fact_kind.upper()}_PUBLISHED_AFTER_FACT_CUTOFF"
                )
            elif (
                index > 0
                and known > fact_cutoff_at
                and previous_publication is not None
                and published <= previous_publication
            ):
                # Changed content reusing the original publication timestamp is
                # not a historically published amendment.  Keep the prior
                # revision for T; never rewrite the prefix from a later fetch.
                reason = (
                    f"PIT_{fact_kind.upper()}_REVISION_PUBLICATION_NOT_ADVANCED"
                )
            else:
                eligible = row
                reason = ""
            previous_publication = published
            previous_source_date = published.date()
            continue

        if source_date is None:
            reason = f"PIT_{fact_kind.upper()}_PUBLICATION_DATE_MISSING"
        elif source_date > fact_cutoff_at.date():
            reason = (
                f"PIT_{fact_kind.upper()}_PUBLISHED_AFTER_DECISION"
                if fact_cutoff_at == decision_at
                else f"PIT_{fact_kind.upper()}_PUBLISHED_AFTER_FACT_CUTOFF"
            )
        elif not _live_capture_allowed(
            fact_cutoff_at=fact_cutoff_at,
            decision_at=decision_at,
            known_at=known,
        ):
            reason = f"PIT_{fact_kind.upper()}_LIVE_CAPTURE_WINDOW_EXCEEDED"
        elif (
            index > 0
            and known > fact_cutoff_at
            and previous_source_date is not None
            and source_date <= previous_source_date
        ):
            reason = (
                f"PIT_{fact_kind.upper()}_REVISION_PUBLICATION_NOT_ADVANCED"
            )
        else:
            eligible = row
            reason = ""
        previous_source_date = source_date
        previous_publication = None
    return eligible, reason


def _query_revisions(
    engine: Engine | Connection,
    *,
    table_name: str,
    codes: list[str],
    decision_at: datetime,
    start_date: date | None,
    end_date: date,
    source: str = "",
    batch_id: str = "",
) -> list[dict[str, Any]]:
    if table_name == EVENT_REVISION_TABLE:
        identity_params: dict[str, Any] = {
            "codes": codes,
            "decision_at": decision_at,
            "end_date": end_date,
        }
        source_filter = ""
        if source:
            source_filter += " AND source=:source"
            identity_params["source"] = source
        # Do not filter revisions by coverage batch.  An unchanged event is
        # idempotently bound into a later full-market coverage response while
        # retaining the immutable batch_id of its first revision.
        # Exact publication date controls visibility windows.  Eastmoney may
        # assign tomorrow's business ``notice_date`` to an announcement that
        # was actually published tonight; filtering on event_date would hide
        # a risk fact that the system already knew.  Unverified rows retain
        # event_date only for retrieval and remain fail-closed at selection.
        date_window = "fact_date<=:end_date"
        if start_date is not None:
            date_window += " AND fact_date>=:start_date"
            identity_params["start_date"] = start_date
        identity_statement = text(
            f"SELECT identity_hash FROM ("
            f"SELECT identity_hash, "
            f"COALESCE(DATE(published_at), event_date) AS fact_date, "
            f"ROW_NUMBER() OVER (PARTITION BY identity_hash "
            f"ORDER BY revision_no DESC) AS rn "
            f"FROM {EVENT_REVISION_TABLE} WHERE stock_code IN :codes "
            f"AND known_at<=:decision_at AND received_at<=:decision_at"
            f"{source_filter}"
            f") selected WHERE rn=1 AND (fact_date IS NULL OR "
            f"({date_window})) ORDER BY identity_hash"
        ).bindparams(bindparam("codes", expanding=True))
        with (nullcontext(engine) if isinstance(engine, Connection) else engine.connect()) as connection:
            identities = [
                str(row[0])
                for row in connection.execute(
                    identity_statement, identity_params
                ).fetchall()
            ]
            if not identities:
                return []
            statement = text(
                f"SELECT * FROM {EVENT_REVISION_TABLE} "
                f"WHERE identity_hash IN :identities "
                f"AND known_at<=:decision_at AND received_at<=:decision_at "
                f"{source_filter} "
                f"ORDER BY stock_code, identity_hash, revision_no"
            ).bindparams(bindparam("identities", expanding=True))
            return [
                dict(row)
                for row in connection.execute(
                    statement,
                    {
                        "identities": identities,
                        "decision_at": decision_at,
                        **(
                            {"source": source} if source else {}
                        ),
                    },
                ).mappings()
            ]
    date_column = "report_date" if table_name == FINANCE_REVISION_TABLE else "event_date"
    date_filter = f"AND {date_column}<=:end_date "
    params: dict[str, Any] = {
        "codes": codes,
        "decision_at": decision_at,
        "end_date": end_date,
    }
    if start_date is not None:
        date_filter += f"AND {date_column}>=:start_date "
        params["start_date"] = start_date
    statement = text(
        f"SELECT * FROM {table_name} WHERE stock_code IN :codes "
        f"AND known_at<=:decision_at AND received_at<=:decision_at "
        f"{date_filter}ORDER BY stock_code, identity_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    with (nullcontext(engine) if isinstance(engine, Connection) else engine.connect()) as connection:
        return [dict(row) for row in connection.execute(statement, params).mappings()]


def _blocked_batch(
    *, table_name: str, codes: list[str], decision_at: datetime, reason: str,
    fact_cutoff_at: datetime | None = None,
    status: str = PIT_SCHEMA_UNAVAILABLE,
) -> PITFactBatch:
    return PITFactBatch(
        facts={},
        status_by_code={code: status for code in codes},
        reason_by_code={code: reason for code in codes},
        manifest_hash=canonical_hash(
            {
                "table": table_name,
                "fact_cutoff_at": _dt_text(fact_cutoff_at or decision_at),
                "decision_at": _dt_text(decision_at),
                "reason": reason,
            }
        ),
        decision_at=_dt_text(decision_at),
        fact_cutoff_at=_dt_text(fact_cutoff_at or decision_at),
        table_name=table_name,
        global_status=status,
    )


def _validate_finance_expected_unavailable_row(
    row: Mapping[str, Any],
    *,
    supersedes: str | None,
) -> None:
    if (
        str(row.get("fact_kind") or "") != "finance"
        or str(row.get("coverage_status") or "")
        != FINANCE_EXPECTED_UNAVAILABLE_STATUS
        or str(row.get("source") or "") != CNINFO_FINANCE_NONFILING_SOURCE
        or str(row.get("watermark_kind") or "") != "REGULATORY_NONFILING"
        or int(row.get("result_count") or 0) != 0
    ):
        raise ValueError("finance expected-unavailable row identity differs")
    payload = _parse_payload(row)
    if payload.get("schema") != "probiga.pit-finance-expected-unavailable-payload.v1":
        raise ValueError("finance expected-unavailable payload schema differs")
    code = str(row.get("stock_code") or "").zfill(6)
    report_date = _date_value(payload.get("expected_report_date"), required=True)
    known = _row_datetime(row.get("known_at"))
    received = _row_datetime(row.get("received_at"))
    if (
        report_date is None
        or _date_value(row.get("window_start"), required=True) != report_date
        or _date_value(row.get("window_end"), required=True) != report_date
        or str(payload.get("fact_kind") or "") != "finance"
        or str(payload.get("stock_code") or "").zfill(6) != code
        or str(payload.get("known_at") or "") != _dt_text(known)
        or str(payload.get("received_at") or "") != _dt_text(received)
        or str(payload.get("coverage_status") or "")
        != FINANCE_EXPECTED_UNAVAILABLE_STATUS
        or str(payload.get("source") or "") != CNINFO_FINANCE_NONFILING_SOURCE
        or str(payload.get("reason_code") or "")
        not in FINANCE_NONFILING_REASON_CODES
    ):
        raise ValueError("finance expected-unavailable payload identity differs")
    evidence = payload.get("official_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("finance expected-unavailable evidence is malformed")
    title = str(evidence.get("announcement_title") or "")
    announcement_id = str(evidence.get("announcement_id") or "")
    if (
        str(evidence.get("source") or "") != CNINFO_FINANCE_NONFILING_SOURCE
        or str(evidence.get("stock_code") or "").zfill(6) != code
        or str(evidence.get("expected_report_date") or "")
        != report_date.isoformat()
        or str(evidence.get("reason_code") or "")
        not in FINANCE_NONFILING_REASON_CODES
        or not (
            "未在规定期限内披露定期报告" in title
            or "无法在法定期限内披露定期报告" in title
            or ("无法在规定期限内披露" in title and "年度报告" in title)
        )
        or not re.fullmatch(r"\d+", announcement_id)
        or not re.fullmatch(
            rf"https://static\.cninfo\.com\.cn/finalpage/"
            rf"\d{{4}}-\d{{2}}-\d{{2}}/{re.escape(announcement_id)}\.PDF",
            str(evidence.get("announcement_url") or ""),
        )
        or any(
            not _HASH_RE.fullmatch(str(evidence.get(field) or ""))
            for field in (
                "announcement_document_sha256",
                "catalog_response_sha256",
            )
        )
    ):
        raise ValueError("finance expected-unavailable official evidence differs")
    published = normalize_decision_at(
        str(evidence.get("announcement_published_at") or "")
    )
    valid_from = _date_value(evidence.get("valid_from"), required=True)
    valid_until = _date_value(evidence.get("valid_until"), required=True)
    next_retry = _date_value(evidence.get("next_retry_date"), required=True)
    if (
        valid_from is None
        or valid_until is None
        or next_retry is None
        or valid_from != published.date()
        or valid_until < known.date()
        or valid_until > valid_from + timedelta(days=7)
        or not (known.date() <= next_retry <= valid_until)
    ):
        raise ValueError("finance expected-unavailable validity differs")
    source_response_hash = canonical_hash({
        "schema": "probiga.pit-finance-expected-unavailable-response.v1",
        "official_evidence": evidence,
    })
    fact_set_hash = canonical_hash({
        "schema": "probiga.pit-finance-expected-unavailable-fact-set.v1",
        "bindings": [],
    })
    if (
        str(payload.get("source_response_hash") or "") != source_response_hash
        or str(row.get("source_response_hash") or "") != source_response_hash
        or str(payload.get("fact_set_hash") or "") != fact_set_hash
        or str(row.get("fact_set_hash") or "") != fact_set_hash
    ):
        raise ValueError("finance expected-unavailable response binding differs")
    watermark = payload.get("watermark")
    expected_watermark = {
        "schema": "probiga.pit-finance-expected-unavailable-watermark.v1",
        "kind": "REGULATORY_NONFILING",
        "known_at": _dt_text(known),
        "valid_from": valid_from.isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_retry_date": next_retry.isoformat(),
        "source_response_hash": source_response_hash,
    }
    if (
        watermark != expected_watermark
        or str(row.get("watermark_hash") or "") != canonical_hash(expected_watermark)
        or _row_datetime(row.get("covered_through_at")) != known
    ):
        raise ValueError("finance expected-unavailable watermark differs")
    scope_hash = canonical_hash({
        "schema": "probiga.pit-source-coverage-scope.v1",
        "fact_kind": "finance",
        "stock_code": code,
        "source": CNINFO_FINANCE_NONFILING_SOURCE,
    })
    fingerprint = canonical_hash({
        "schema": "probiga.pit-source-coverage-fingerprint.v1",
        "scope_hash": scope_hash,
        "payload": payload,
        "supersedes_coverage_id": supersedes,
    })
    coverage_id = canonical_hash({
        "schema": "probiga.pit-source-coverage-id.v1",
        "scope_hash": scope_hash,
        "coverage_fingerprint_hash": fingerprint,
    })
    if (
        str(row.get("scope_hash") or "") != scope_hash
        or str(row.get("coverage_fingerprint_hash") or "") != fingerprint
        or str(row.get("coverage_id") or "") != coverage_id
    ):
        raise ValueError("finance expected-unavailable row hash differs")


def _validate_coverage_chain(rows: Iterable[dict[str, Any]]) -> None:
    previous_id: str | None = None
    previous_known: datetime | None = None
    for expected_no, row in enumerate(rows, 1):
        if int(row.get("revision_no") or 0) != expected_no:
            raise ValueError("PIT coverage chain has a sequence gap")
        supersedes = str(row.get("supersedes_coverage_id") or "") or None
        if supersedes != previous_id:
            raise ValueError("PIT coverage supersedes chain is broken")
        known = _row_datetime(row.get("known_at"))
        received = _row_datetime(row.get("received_at"))
        covered = _row_datetime(row.get("covered_through_at"))
        if (
            received > known
            or covered > known
            or (previous_known is not None and known < previous_known)
        ):
            raise ValueError("PIT coverage knowledge/watermark chain is invalid")
        if (
            str(row.get("coverage_status") or "")
            == FINANCE_EXPECTED_UNAVAILABLE_STATUS
        ):
            _validate_finance_expected_unavailable_row(
                row,
                supersedes=supersedes,
            )
            previous_id = str(row.get("coverage_id") or "")
            previous_known = known
            continue
        if str(row.get("coverage_status") or "") != "COMPLETE":
            raise ValueError("PIT coverage disposition is invalid")
        payload = _parse_payload(row)
        if payload.get("schema") != "probiga.pit-source-coverage-payload.v1":
            raise ValueError("PIT coverage payload schema differs")
        expected_payload_fields = {
            "fact_kind": str(row.get("fact_kind") or ""),
            "stock_code": str(row.get("stock_code") or ""),
            "window_start": _date_value(
                row.get("window_start"), required=True
            ).isoformat(),
            "window_end": _date_value(
                row.get("window_end"), required=True
            ).isoformat(),
            "known_at": _dt_text(known),
            "received_at": _dt_text(received),
            "covered_through_at": _dt_text(covered),
            "result_count": int(row.get("result_count") or 0),
        }
        if any(
            payload.get(key) != value
            for key, value in expected_payload_fields.items()
        ):
            raise ValueError("PIT coverage payload differs from stored columns")
        source_rows = payload.get("source_rows")
        bindings = payload.get("fact_bindings")
        if not isinstance(source_rows, list) or not isinstance(bindings, list):
            raise ValueError("PIT coverage response/bindings are malformed")
        if (
            int(row.get("result_count") or 0) != len(bindings)
            or len(source_rows) != len(bindings)
        ):
            raise ValueError("PIT coverage result count differs")
        expected_response_hash = canonical_hash(
            {
                "schema": (
                    f"probiga.pit-{row.get('fact_kind')}-source-response.v1"
                ),
                "rows": source_rows,
            }
        )
        if (
            str(payload.get("source_response_hash") or "")
            != expected_response_hash
            or str(row.get("source_response_hash") or "")
            != expected_response_hash
        ):
            raise ValueError("PIT coverage response hash differs")
        expected_fact_set_hash = canonical_hash(
            {
                "schema": f"probiga.pit-{row.get('fact_kind')}-fact-set.v1",
                "bindings": bindings,
            }
        )
        if (
            str(row.get("fact_set_hash") or "") != expected_fact_set_hash
            or str(payload.get("fact_set_hash") or "") != expected_fact_set_hash
        ):
            raise ValueError("PIT coverage fact-set hash differs")
        watermark = payload.get("watermark")
        if not isinstance(watermark, dict):
            raise ValueError("PIT coverage watermark payload is malformed")
        if (
            watermark.get("schema")
            != "probiga.pit-source-watermark.v1"
            or str(watermark.get("kind") or "")
            != str(row.get("watermark_kind") or "")
            or str(watermark.get("covered_through_at") or "")
            != _dt_text(covered)
            or str(watermark.get("source_response_hash") or "")
            != expected_response_hash
        ):
            raise ValueError("PIT coverage watermark binding differs")
        watermark_kind = str(row.get("watermark_kind") or "")
        evidence = watermark.get("evidence")
        if not isinstance(evidence, dict):
            raise ValueError("PIT coverage watermark evidence is malformed")
        if watermark_kind == "CAPTURED_AT":
            if (
                _row_datetime(evidence.get("captured_at")) != known
                or covered != known
            ):
                raise ValueError("captured-at coverage watermark differs")
        elif watermark_kind == "SOURCE_SERVER_TIME":
            if (
                _row_datetime(evidence.get("source_server_at")) != covered
                or str(evidence.get("source_response_hash") or "")
                != expected_response_hash
            ):
                raise ValueError("source-server coverage watermark differs")
        elif watermark_kind in {
            "QUERY_CUTOFF", "HISTORICAL_RECONSTRUCTION",
        }:
            required_hashes = (
                "global_batch_root_hash", "catalog_manifest_hash",
                "catalog_member_set_hash",
            )
            row_source = str(row.get("source") or "")
            fallback_valid = bool(
                row_source == QMT_EVENT_SOURCE
                or (
                    row_source in {CNINFO_EVENT_SOURCE, EASTMONEY_EVENT_SOURCE}
                    and evidence.get("primary_provider") == QMT_EVENT_SOURCE
                    and str(evidence.get("fallback_reason") or "")
                    in EVENT_FALLBACK_REASON_CODES
                )
            )
            fallback_receipt_valid = _fallback_event_receipt_valid(
                evidence,
                source=row_source,
                stock_code=str(row.get("stock_code") or "").zfill(6),
                result_count=len(source_rows),
            )
            historical = watermark_kind == "HISTORICAL_RECONSTRUCTION"
            reconstruction = evidence.get("reconstruction_provenance")
            reconstruction_valid = True
            if historical:
                reconstruction_valid = bool(
                    isinstance(reconstruction, dict)
                    and reconstruction.get("schema")
                    == "probiga.qmt-announcement-historical-reconstruction.v2"
                    and reconstruction.get("mode")
                    == "HISTORICAL_RECONSTRUCTION"
                    and canonical_hash({
                        key: value
                        for key, value in reconstruction.items()
                        if key != "reconstruction_sha256"
                    }) == reconstruction.get("reconstruction_sha256")
                    and _row_datetime(
                        reconstruction.get("source_query_cutoff_at")
                    ) == covered
                    and _row_datetime(reconstruction.get("reconstructed_at"))
                    == known
                    and _row_datetime(reconstruction.get("known_at")) == known
                    and reconstruction.get("provider") == row_source
                    and reconstruction.get("source") == row_source
                    and reconstruction.get("automatic_real_order_submission")
                    is False
                    and reconstruction.get("real_order_authority") is False
                )
            if (
                str(row.get("fact_kind") or "") != "event"
                or row_source not in AUTHORITATIVE_EVENT_SOURCES
                or str(evidence.get("provider") or "") != row_source
                or not fallback_valid
                or not fallback_receipt_valid
                or str(evidence.get("period") or "") != "announcement"
                or _row_datetime(evidence.get("fact_cutoff_at")) != covered
                or _row_datetime(evidence.get("decision_at")) != known
                or _row_datetime(evidence.get("received_at")) != known
                or str(evidence.get("query_end_time") or "")
                != covered.strftime("%Y%m%d%H%M%S")
                or str(evidence.get("source_response_hash") or "")
                != expected_response_hash
                or (not historical and not (
                    timedelta(0)
                    <= known - covered
                    <= MAX_LIVE_CAPTURE_DELAY
                ))
                or (historical and row_source != CNINFO_EVENT_SOURCE)
                or not reconstruction_valid
                or any(
                    not _HASH_RE.fullmatch(str(evidence.get(field) or ""))
                    for field in required_hashes
                )
                or not str(evidence.get("catalog_batch_id") or "")
                or not isinstance(evidence.get("catalog_member_count"), int)
                or int(evidence.get("catalog_member_count") or 0) <= 0
            ):
                raise ValueError("query-cutoff coverage watermark differs")
        else:
            raise ValueError("PIT coverage watermark kind is invalid")
        expected_watermark_hash = canonical_hash(watermark)
        if str(row.get("watermark_hash") or "") != expected_watermark_hash:
            raise ValueError("PIT coverage watermark hash differs")
        expected_scope = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-scope.v1",
                "fact_kind": str(row.get("fact_kind") or ""),
                "stock_code": str(row.get("stock_code") or ""),
                "source": str(row.get("source") or "")[:64],
            }
        )
        if str(row.get("scope_hash") or "") != expected_scope:
            raise ValueError("PIT coverage scope hash differs")
        fingerprint = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-fingerprint.v1",
                "scope_hash": expected_scope,
                "payload": payload,
                "supersedes_coverage_id": supersedes,
            }
        )
        if str(row.get("coverage_fingerprint_hash") or "") != fingerprint:
            raise ValueError("PIT coverage fingerprint differs")
        expected_id = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-id.v1",
                "scope_hash": expected_scope,
                "coverage_fingerprint_hash": fingerprint,
            }
        )
        if str(row.get("coverage_id") or "") != expected_id:
            raise ValueError("PIT coverage id differs")
        previous_id = expected_id
        previous_known = known


def _authoritative_empty_coverage(
    engine: Engine,
    *,
    fact_kind: str,
    codes: list[str],
    fact_cutoff_at: datetime,
    decision_at: datetime,
    start_date: date,
    end_date: date,
    source: str = "",
    batch_id: str = "",
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Resolve complete zero-result receipts without crossing their watermark."""

    source_filter = ""
    params: dict[str, Any] = {
        "fact_kind": fact_kind,
        "codes": codes,
        "decision_at": decision_at,
    }
    if source:
        source_filter += " AND source=:source"
        params["source"] = source
    # Coverage-chain validation needs every prior revision in the source
    # scope.  The requested completed batch is checked after the full chain is
    # validated; SQL-filtering it here would manufacture a revision gap.
    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        "WHERE fact_kind=:fact_kind AND stock_code IN :codes "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        f"{source_filter} "
        "ORDER BY stock_code, scope_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    def code_chains():
        batch_size = max(1, int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT))
        with engine.connect() as connection:
            if connection.dialect.name == "mysql" and str(
                connection.get_isolation_level() or ""
            ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError("empty coverage requires a repeatable-read snapshot")
            with connection.begin():
                for offset in range(0, len(codes), batch_size):
                    code_batch = codes[offset:offset + batch_size]
                    by_code_scope: dict[
                        str, dict[str, list[dict[str, Any]]]
                    ] = defaultdict(lambda: defaultdict(list))
                    for raw in connection.execute(
                        statement, {**params, "codes": code_batch}
                    ).mappings():
                        row = dict(raw)
                        by_code_scope[str(row.get("stock_code") or "").zfill(6)][
                            str(row.get("scope_hash") or "")
                        ].append(row)
                    for code in code_batch:
                        yield code, by_code_scope.get(code, {})
                    by_code_scope.clear()

    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    for code, scopes in code_chains():
        candidates: list[dict[str, Any]] = []
        for chain in scopes.values():
            try:
                _validate_coverage_chain(chain)
            except ValueError as exc:
                invalid[code] = f"PIT_{fact_kind.upper()}_BAD_COVERAGE_CHAIN:{exc}"
                continue
            latest = chain[-1]
            if (
                str(latest.get("coverage_status") or "") == "COMPLETE"
                and int(latest.get("result_count") or 0) == 0
                and _date_value(latest.get("window_start"), required=True)
                <= start_date
                and _date_value(latest.get("window_end"), required=True)
                >= end_date
                and fact_cutoff_at
                <= _row_datetime(latest.get("covered_through_at"))
                and (
                    _live_capture_allowed(
                        fact_cutoff_at=fact_cutoff_at,
                        decision_at=decision_at,
                        known_at=_row_datetime(latest.get("known_at")),
                    )
                    or (
                        bool(batch_id)
                        and fact_kind == "event"
                        and str(latest.get("batch_id") or "") == batch_id
                        and str(latest.get("watermark_kind") or "")
                        == "HISTORICAL_RECONSTRUCTION"
                        and _row_datetime(latest.get("known_at"))
                        <= decision_at
                    )
                )
                and (
                    not batch_id
                    or str(latest.get("batch_id") or "") == batch_id
                )
            ):
                candidates.append(latest)
        if candidates:
            candidates.sort(
                key=lambda row: (
                    _row_datetime(row.get("known_at")),
                    int(row.get("revision_no") or 0),
                )
            )
            selected = candidates[-1]
            available[code] = {
                "coverage_id": str(selected.get("coverage_id") or ""),
                "coverage_source": str(selected.get("source") or ""),
                "coverage_response_hash": str(
                    selected.get("source_response_hash") or ""
                ),
                "coverage_watermark_hash": str(
                    selected.get("watermark_hash") or ""
                ),
                "covered_through_at": _dt_text(
                    _row_datetime(selected.get("covered_through_at"))
                ),
                "coverage_known_at": _dt_text(
                    _row_datetime(selected.get("known_at"))
                ),
                "coverage_received_at": _dt_text(
                    _row_datetime(selected.get("received_at"))
                ),
                "coverage_batch_id": str(selected.get("batch_id") or ""),
                "fact_cutoff_at": _dt_text(fact_cutoff_at),
                "decision_at": _dt_text(decision_at),
            }
    return available, invalid


def load_finance_expected_unavailable(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    expected_report_date: date | str | None = None,
    known_after: datetime | str | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Resolve valid official non-filing dispositions for governance."""

    normalized_codes = sorted({
        str(code).strip().zfill(6)
        for code in codes
        if str(code).strip()
    })
    if not normalized_codes:
        return {}, {}
    decision = normalize_decision_at(decision_at)
    report_date = _date_value(expected_report_date, required=False)
    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        "WHERE fact_kind='finance' AND source=:source "
        "AND stock_code IN :codes "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        "ORDER BY stock_code, scope_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                {
                    "source": CNINFO_FINANCE_NONFILING_SOURCE,
                    "codes": normalized_codes,
                    "decision_at": decision,
                },
            ).mappings()
        ]
    by_code_scope: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        by_code_scope[str(row.get("stock_code") or "").zfill(6)][
            str(row.get("scope_hash") or "")
        ].append(row)
    minimum_known = (
        normalize_decision_at(known_after) if known_after is not None else None
    )
    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    for code in normalized_codes:
        candidates: list[dict[str, Any]] = []
        for chain in by_code_scope.get(code, {}).values():
            try:
                _validate_coverage_chain(chain)
            except ValueError as exc:
                invalid[code] = f"PIT_FINANCE_BAD_UNAVAILABLE_CHAIN:{exc}"
                continue
            latest = chain[-1]
            payload = _parse_payload(latest)
            evidence = payload.get("official_evidence")
            if not isinstance(evidence, dict):
                invalid[code] = "PIT_FINANCE_UNAVAILABLE_EVIDENCE_MALFORMED"
                continue
            known = _row_datetime(latest.get("known_at"))
            valid_from = _date_value(evidence.get("valid_from"), required=True)
            valid_until = _date_value(evidence.get("valid_until"), required=True)
            if (
                str(latest.get("coverage_status") or "")
                == FINANCE_EXPECTED_UNAVAILABLE_STATUS
                and (
                    report_date is None
                    or str(payload.get("expected_report_date") or "")
                    == report_date.isoformat()
                )
                and valid_from is not None
                and valid_until is not None
                and valid_from <= decision.date() <= valid_until
                and (minimum_known is None or known >= minimum_known)
            ):
                candidates.append(latest)
        if candidates:
            candidates.sort(
                key=lambda row: (
                    _row_datetime(row.get("known_at")),
                    int(row.get("revision_no") or 0),
                )
            )
            selected = candidates[-1]
            payload = _parse_payload(selected)
            evidence = dict(payload["official_evidence"])
            available[code] = {
                "coverage_status": FINANCE_EXPECTED_UNAVAILABLE_STATUS,
                "reason_code": str(payload.get("reason_code") or ""),
                "source": str(selected.get("source") or ""),
                "disposition_id": str(selected.get("coverage_id") or ""),
                "coverage_id": str(selected.get("coverage_id") or ""),
                "coverage_source": str(selected.get("source") or ""),
                "source_response_hash": str(
                    selected.get("source_response_hash") or ""
                ),
                "coverage_response_hash": str(
                    selected.get("source_response_hash") or ""
                ),
                "watermark_hash": str(selected.get("watermark_hash") or ""),
                "coverage_watermark_hash": str(
                    selected.get("watermark_hash") or ""
                ),
                "known_at": _dt_text(_row_datetime(selected.get("known_at"))),
                "covered_through_at": _dt_text(
                    _row_datetime(selected.get("covered_through_at"))
                ),
                "coverage_known_at": _dt_text(
                    _row_datetime(selected.get("known_at"))
                ),
                "coverage_received_at": _dt_text(
                    _row_datetime(selected.get("received_at"))
                ),
                "coverage_batch_id": str(selected.get("batch_id") or ""),
                "valid_from": str(evidence.get("valid_from") or ""),
                "valid_until": str(evidence.get("valid_until") or ""),
                "next_retry_date": str(evidence.get("next_retry_date") or ""),
                "announcement_id": str(evidence.get("announcement_id") or ""),
                "announcement_url": str(evidence.get("announcement_url") or ""),
                "announcement_document_sha256": str(
                    evidence.get("announcement_document_sha256") or ""
                ),
            }
    return available, invalid


def _finance_discovery_event_set_hash(events: list[dict[str, Any]]) -> str:
    return canonical_hash({
        "schema": "probiga.pit-finance-discovery-event-set.v1",
        "events": events,
    })


def _finance_discovery_universe_hash(codes: list[str]) -> str:
    return canonical_hash({
        "schema": "probiga.pit-finance-discovery-universe.v1",
        "codes": codes,
    })


def _validate_finance_incremental_discovery(
    row: Mapping[str, Any],
    *,
    codes: list[str],
    as_of_date: date,
) -> dict[str, Any]:
    """Validate one immutable, stable, exact-date EastMoney change sweep."""

    if (
        str(row.get("source") or "") != FINANCE_INCREMENTAL_DISCOVERY_SOURCE
        or str(row.get("stock_code") or "").zfill(6)
        != FINANCE_INCREMENTAL_DISCOVERY_CODE
        or str(row.get("coverage_status") or "") != "COMPLETE"
        or int(row.get("result_count") or 0) != 0
    ):
        raise ValueError("finance incremental discovery receipt identity differs")
    start = _date_value(row.get("window_start"), required=True)
    end = _date_value(row.get("window_end"), required=True)
    if start is None or end is None or start > end or end < as_of_date:
        raise ValueError("finance incremental discovery window differs")
    payload = _parse_payload(row)
    if payload.get("source_rows") != [] or payload.get("fact_bindings") != []:
        raise ValueError("finance incremental discovery must not manufacture facts")
    watermark = payload.get("watermark")
    evidence = (
        watermark.get("evidence") if isinstance(watermark, dict) else None
    )
    if not isinstance(evidence, dict):
        raise ValueError("finance incremental discovery evidence is malformed")
    expected_codes = sorted({str(code).zfill(6) for code in codes})
    fields = evidence.get("query_fields")
    events = evidence.get("events")
    sweeps = evidence.get("sweeps")
    if (
        evidence.get("schema") != FINANCE_INCREMENTAL_DISCOVERY_SCHEMA
        or evidence.get("source") != FINANCE_INCREMENTAL_DISCOVERY_SOURCE
        or evidence.get("query_mode") != "EXACT_DATE"
        or fields != ["NOTICE_DATE", "UPDATE_DATE"]
        or str(evidence.get("window_start") or "") != start.isoformat()
        or str(evidence.get("window_end") or "") != end.isoformat()
        or int(evidence.get("universe_code_count") or 0) != len(expected_codes)
        or evidence.get("universe_code_set_sha256")
        != _finance_discovery_universe_hash(expected_codes)
        or not isinstance(events, list)
        or not isinstance(sweeps, list)
        or len(sweeps) != 2
        or int(evidence.get("stable_sweep_count") or 0) != 2
        or evidence.get("stability_status") != "STABLE_DOUBLE_SWEEP"
    ):
        raise ValueError("finance incremental discovery contract differs")

    normalized_events: list[dict[str, Any]] = []
    changed_dates: dict[str, set[date]] = defaultdict(set)
    seen_event_keys: set[tuple[str, str, str, str, str]] = set()
    for item in events:
        if not isinstance(item, dict):
            raise ValueError("finance incremental discovery event is malformed")
        query_field = str(item.get("query_field") or "")
        query_date = _date_value(item.get("query_date"), required=True)
        source_code = str(item.get("source_security_code") or "")
        code_raw = str(item.get("stock_code") or "")
        code = code_raw.zfill(6) if code_raw else ""
        report_date = _date_value(item.get("report_date"), required=True)
        report_type = str(item.get("report_type") or "")
        row_hash = str(item.get("row_content_sha256") or "")
        if (
            query_field not in fields
            or query_date is None
            or not (start <= query_date <= end)
            or not re.fullmatch(r"[A-Za-z0-9]{1,16}", source_code)
            or (code and not re.fullmatch(r"\d{6}", code))
            or report_date is None
            or not report_type
            or not _HASH_RE.fullmatch(row_hash)
        ):
            raise ValueError("finance incremental discovery event fields differ")
        source_date = _date_value(
            item.get(
                "notice_date" if query_field == "NOTICE_DATE" else "update_date"
            ),
            required=True,
        )
        if source_date != query_date:
            raise ValueError("finance incremental discovery is not exact-date")
        key = (
            query_field,
            query_date.isoformat(),
            source_code,
            report_date.isoformat(),
            report_type,
        )
        if key in seen_event_keys:
            raise ValueError("finance incremental discovery contains duplicates")
        seen_event_keys.add(key)
        normalized = dict(item)
        normalized_events.append(normalized)
        if code in expected_codes:
            changed_dates[code].add(query_date)
    normalized_events.sort(key=canonical_json)
    changed_codes = sorted(changed_dates)
    event_set_hash = _finance_discovery_event_set_hash(normalized_events)
    if (
        events != normalized_events
        or int(evidence.get("event_count") or 0) != len(normalized_events)
        or evidence.get("event_set_sha256") != event_set_hash
        or evidence.get("changed_codes") != changed_codes
        or evidence.get("changed_code_set_sha256")
        != canonical_hash({
            "schema": "probiga.pit-finance-discovery-changed-code-set.v1",
            "codes": changed_codes,
        })
    ):
        raise ValueError("finance incremental discovery event root differs")

    expected_queries = {
        (field, (start + timedelta(days=offset)).isoformat())
        for offset in range((end - start).days + 1)
        for field in fields
    }
    stable_roots: list[str] = []
    for ordinal, sweep in enumerate(sweeps, 1):
        if not isinstance(sweep, dict):
            raise ValueError("finance incremental discovery sweep is malformed")
        queries = sweep.get("queries")
        if not isinstance(queries, list):
            raise ValueError("finance incremental discovery query manifest is missing")
        observed_queries: set[tuple[str, str]] = set()
        query_rows = 0
        page_rows = 0
        for query in queries:
            if not isinstance(query, dict):
                raise ValueError("finance incremental discovery query is malformed")
            field = str(query.get("query_field") or "")
            query_date = str(query.get("query_date") or "")
            pair = (field, query_date)
            page_hashes = query.get("page_content_sha256")
            page_raw_hashes = query.get("page_raw_sha256")
            page_row_counts = query.get("page_row_counts")
            page_size = int(query.get("page_size") or 0)
            total_count = int(query.get("total_count") or 0)
            expected_page_count = (
                max(1, math.ceil(total_count / page_size))
                if page_size > 0
                else 0
            )
            if (
                pair not in expected_queries
                or pair in observed_queries
                or not isinstance(page_hashes, list)
                or not page_hashes
                or not all(_HASH_RE.fullmatch(str(value or "")) for value in page_hashes)
                or not isinstance(page_raw_hashes, list)
                or len(page_raw_hashes) != len(page_hashes)
                or not all(
                    _HASH_RE.fullmatch(str(value or ""))
                    for value in page_raw_hashes
                )
                or not isinstance(page_row_counts, list)
                or len(page_row_counts) != len(page_hashes)
                or any(not isinstance(value, int) or value < 0 for value in page_row_counts)
                or page_size <= 0
                or any(value > page_size for value in page_row_counts)
                or int(query.get("page_count") or 0) != len(page_hashes)
                or len(page_hashes) != expected_page_count
                or int(query.get("row_count") or 0) != sum(page_row_counts)
                or total_count != sum(page_row_counts)
            ):
                raise ValueError("finance incremental discovery pagination differs")
            query_events = [
                event for event in normalized_events
                if event["query_field"] == field
                and event["query_date"] == query_date
            ]
            expected_query_hash = canonical_hash({
                "schema": "probiga.pit-finance-discovery-query-result.v1",
                "query_field": field,
                "query_date": query_date,
                "events": query_events,
            })
            if (
                len(query_events) != int(query.get("row_count") or 0)
                or query.get("content_sha256") != expected_query_hash
            ):
                raise ValueError("finance incremental discovery query root differs")
            observed_queries.add(pair)
            query_rows += len(query_events)
            page_rows += len(page_hashes)
        queries_sorted = sorted(
            queries,
            key=lambda item: (item["query_date"], item["query_field"]),
        )
        sweep_root = canonical_hash({
            "schema": "probiga.pit-finance-discovery-sweep.v1",
            "queries": queries_sorted,
        })
        if (
            observed_queries != expected_queries
            or queries != queries_sorted
            or int(sweep.get("sweep_no") or 0) != ordinal
            or int(sweep.get("query_count") or 0) != len(queries)
            or int(sweep.get("page_count") or 0) != page_rows
            or int(sweep.get("row_count") or 0) != query_rows
            or sweep.get("content_sha256") != sweep_root
        ):
            raise ValueError("finance incremental discovery sweep root differs")
        started = normalize_decision_at(str(sweep.get("started_at") or ""))
        completed = normalize_decision_at(str(sweep.get("completed_at") or ""))
        if completed < started or completed > _row_datetime(row.get("known_at")):
            raise ValueError("finance incremental discovery capture time differs")
        stable_roots.append(sweep_root)
    if (
        len(set(stable_roots)) != 1
        or evidence.get("stable_content_sha256") != stable_roots[0]
    ):
        raise ValueError("finance incremental discovery sweeps are unstable")
    return {
        "coverage_id": str(row.get("coverage_id") or ""),
        "source_response_hash": str(row.get("source_response_hash") or ""),
        "watermark_hash": str(row.get("watermark_hash") or ""),
        "known_at": _dt_text(_row_datetime(row.get("known_at"))),
        "window_start": start,
        "window_end": end,
        "event_set_sha256": event_set_hash,
        "changed_code_set_sha256": str(
            evidence.get("changed_code_set_sha256") or ""
        ),
        "changed_codes": changed_codes,
        "changed_dates_by_code": {
            code: sorted(values) for code, values in changed_dates.items()
        },
    }


def load_finance_incremental_discovery(
    engine: Engine,
    *,
    coverage_id: str,
    codes: Iterable[str],
    decision_at: datetime | str,
    as_of_date: date | str,
) -> dict[str, Any]:
    """Load and cryptographically revalidate one exact discovery receipt."""

    receipt_id = str(coverage_id or "")
    if not _HASH_RE.fullmatch(receipt_id):
        raise ValueError("finance incremental discovery coverage id is invalid")
    decision = normalize_decision_at(decision_at)
    target = _date_value(as_of_date, required=True)
    normalized_codes = sorted({
        str(code).strip().zfill(6) for code in codes if str(code).strip()
    })
    if target is None or not normalized_codes:
        raise ValueError("finance incremental discovery scope is empty")
    statement = text(
        f"SELECT candidate.* FROM {SOURCE_COVERAGE_TABLE} AS candidate "
        f"JOIN {SOURCE_COVERAGE_TABLE} AS selected "
        "ON selected.scope_hash=candidate.scope_hash "
        "WHERE selected.coverage_id=:coverage_id "
        "AND candidate.known_at<=:decision_at "
        "AND candidate.received_at<=:decision_at "
        "ORDER BY candidate.revision_no"
    )
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                {"coverage_id": receipt_id, "decision_at": decision},
            ).mappings()
        ]
    if not rows:
        raise ValueError("finance incremental discovery receipt is unavailable")
    _validate_coverage_chain(rows)
    selected = next(
        (row for row in rows if str(row.get("coverage_id") or "") == receipt_id),
        None,
    )
    if selected is None:
        raise ValueError("finance incremental discovery receipt postdates decision")
    return _validate_finance_incremental_discovery(
        selected,
        codes=normalized_codes,
        as_of_date=target,
    )


def _finance_incremental_proves_unchanged(
    discovery: Mapping[str, Any] | None,
    *,
    stock_code: str,
    prior_window_end: date,
    target: date,
) -> bool:
    if prior_window_end >= target:
        return True
    if not discovery:
        return False
    start = discovery.get("window_start")
    end = discovery.get("window_end")
    if not isinstance(start, date) or not isinstance(end, date):
        return False
    required_start = prior_window_end + timedelta(days=1)
    if start > required_start or end < target:
        return False
    changed_dates = discovery.get("changed_dates_by_code") or {}
    return not any(
        required_start <= changed_date <= target
        for changed_date in changed_dates.get(stock_code, ())
    )


def _finance_target_timestamp_guard_valid(
    row: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    target: date,
) -> bool:
    """Reject late mutable captures unless source dates prove target safety."""

    if _row_datetime(row.get("known_at")).date() <= target:
        return True
    watermark = payload.get("watermark")
    evidence = (
        watermark.get("evidence") if isinstance(watermark, dict) else None
    )
    guard = evidence.get("source_timestamp_guard") if isinstance(evidence, dict) else None
    if (
        not isinstance(guard, dict)
        or guard.get("status") != "PASS"
        or guard.get("as_of_date") != target.isoformat()
    ):
        return False
    source_rows = payload.get("source_rows")
    if not isinstance(source_rows, list) or not source_rows:
        return False
    for source_row in source_rows:
        if not isinstance(source_row, dict):
            return False
        for field in ("notice_date", "source_update_date"):
            value = _date_value(source_row.get(field), required=False)
            if value is not None and value > target:
                return False
    return True


def _finance_new_listing_empty_valid(
    payload: Mapping[str, Any],
    *,
    stock_code: str,
    listing_date: date | None,
    gate: Any,
    target: date,
    catalog_binding: Mapping[str, Any],
) -> bool:
    if listing_date is None or listing_date <= gate.disclosure_deadline:
        return False
    watermark = payload.get("watermark")
    evidence = (
        watermark.get("evidence") if isinstance(watermark, dict) else None
    )
    receipt = evidence.get("source_receipt") if isinstance(evidence, dict) else None
    guard = (
        evidence.get("source_timestamp_guard")
        if isinstance(evidence, dict)
        else None
    )
    return bool(
        isinstance(receipt, dict)
        and isinstance(guard, dict)
        and evidence.get("resolution_type") == "STATUTORY_NOT_APPLICABLE"
        and evidence.get("reason_code")
        == "NEW_LISTING_AFTER_DISCLOSURE_DEADLINE"
        and evidence.get("stock_code") == stock_code
        and evidence.get("listing_date") == listing_date.isoformat()
        and evidence.get("disclosure_deadline")
        == gate.disclosure_deadline.isoformat()
        and evidence.get("as_of_date") == target.isoformat()
        and evidence.get("catalog_batch_id")
        == catalog_binding.get("catalog_batch_id")
        and evidence.get("catalog_manifest_hash")
        == catalog_binding.get("catalog_manifest_hash")
        and evidence.get("catalog_member_set_hash")
        == catalog_binding.get("catalog_member_set_hash")
        and int(evidence.get("catalog_member_count") or 0)
        == int(catalog_binding.get("catalog_member_count") or 0)
        and receipt.get("stock_code") == stock_code
        and receipt.get("stability_status") == "STABLE_DOUBLE_SWEEP"
        and int(receipt.get("stable_sweep_count") or 0) == 2
        and _HASH_RE.fullmatch(str(receipt.get("stable_content_sha256") or ""))
        and guard.get("status") == "PASS"
        and guard.get("as_of_date") == target.isoformat()
    )


def _select_finance_atomic_batch_members(
    engine: Engine,
    *,
    codes: list[str],
    listing_dates: Mapping[str, date | None],
    completed_known_at: datetime,
    as_of_date: date,
    incremental_discovery: Mapping[str, Any] | None = None,
    catalog_binding: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], datetime]:
    """Select one strict finance disposition per catalog member.

    Finance coverage payloads contain the provider's full historical rows.  A
    full-catalog query currently exceeds 500 MB in production, so materialize
    and validate one bounded code chunk at a time inside one repeatable-read
    snapshot.  Only the compact fields consumed by the atomic seal survive a
    chunk; raw payload JSON is never retained for the full universe.
    """

    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        "WHERE fact_kind='finance' AND stock_code IN :codes "
        "AND known_at<=:completed_known_at "
        "AND received_at<=:completed_known_at "
        "ORDER BY stock_code, scope_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    gate = finance_disclosure_gate(as_of_date)
    selected: list[dict[str, Any]] = []
    source_cutoff: datetime | None = None
    batch_size = max(1, int(FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT))
    with engine.connect() as connection:
        dialect = str(
            getattr(getattr(connection, "dialect", None), "name", "") or ""
        ).lower()
        if dialect == "mysql":
            isolation = str(connection.get_isolation_level() or "").upper()
            if isolation not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError(
                    "finance atomic batch requires a repeatable-read snapshot"
                )
        with connection.begin():
            for offset in range(0, len(codes), batch_size):
                code_batch = codes[offset : offset + batch_size]
                raw_rows = [
                    dict(row)
                    for row in connection.execute(
                        statement,
                        {
                            "codes": code_batch,
                            "completed_known_at": completed_known_at,
                        },
                    ).mappings()
                ]
                by_code_scope: dict[
                    str, dict[str, list[dict[str, Any]]]
                ] = defaultdict(lambda: defaultdict(list))
                for row in raw_rows:
                    by_code_scope[
                        str(row.get("stock_code") or "").zfill(6)
                    ][str(row.get("scope_hash") or "")].append(row)

                for code in code_batch:
                    complete_candidates: list[dict[str, Any]] = []
                    unavailable_candidates: list[dict[str, Any]] = []
                    for chain in by_code_scope.get(code, {}).values():
                        _validate_coverage_chain(chain)
                        latest = chain[-1]
                        status = str(latest.get("coverage_status") or "")
                        payload = _parse_payload(latest)
                        if status == FINANCE_EXPECTED_UNAVAILABLE_STATUS:
                            evidence = payload.get("official_evidence")
                            if not isinstance(evidence, dict):
                                continue
                            valid_from = _date_value(
                                evidence.get("valid_from"), required=True
                            )
                            valid_until = _date_value(
                                evidence.get("valid_until"), required=True
                            )
                            if (
                                str(
                                    payload.get("expected_report_date") or ""
                                )
                                == gate.minimum_report_date.isoformat()
                                and valid_from is not None
                                and valid_until is not None
                                and valid_from
                                <= completed_known_at.date()
                                <= valid_until
                            ):
                                unavailable_candidates.append(latest)
                            continue
                        source_rows = payload.get("source_rows")
                        coverage_window_end = _date_value(
                            latest.get("window_end"), required=True
                        )
                        listing_date = listing_dates.get(code)
                        legal_new_listing_empty = bool(
                            isinstance(source_rows, list)
                            and not source_rows
                            and _finance_new_listing_empty_valid(
                                payload,
                                stock_code=code,
                                listing_date=listing_date,
                                gate=gate,
                                target=as_of_date,
                                catalog_binding=catalog_binding or {},
                            )
                        )
                        if (
                            status != "COMPLETE"
                            or str(latest.get("source") or "")
                            not in AUTHORITATIVE_FINANCE_SOURCES
                            or not isinstance(source_rows, list)
                            or (not source_rows and not legal_new_listing_empty)
                            or _date_value(
                                latest.get("window_start"), required=True
                            )
                            > date(1900, 1, 1)
                            or coverage_window_end is None
                            or not _finance_incremental_proves_unchanged(
                                incremental_discovery,
                                stock_code=code,
                                prior_window_end=coverage_window_end,
                                target=as_of_date,
                            )
                            or (
                                not legal_new_listing_empty
                                and not _finance_target_timestamp_guard_valid(
                                    latest,
                                    payload,
                                    target=as_of_date,
                                )
                            )
                        ):
                            continue
                        if legal_new_listing_empty:
                            accepted = dict(latest)
                            accepted["_legal_no_data_binding"] = {
                                "resolution_type": "STATUTORY_NOT_APPLICABLE",
                                "reason_code": (
                                    "NEW_LISTING_AFTER_DISCLOSURE_DEADLINE"
                                ),
                                "listing_date": listing_date.isoformat(),
                                "disclosure_deadline": (
                                    gate.disclosure_deadline.isoformat()
                                ),
                            }
                            complete_candidates.append(accepted)
                            continue
                        report_dates = [
                            _date_value(
                                item.get("report_date"), required=True
                            )
                            for item in source_rows
                            if isinstance(item, dict)
                            and str(item.get("stock_code") or "").zfill(6)
                            == code
                        ]
                        if (
                            len(report_dates) != len(source_rows)
                            or not report_dates
                        ):
                            continue
                        gate_applies = (
                            listing_date is None
                            or listing_date <= gate.disclosure_deadline
                        )
                        if (
                            gate_applies
                            and max(report_dates) < gate.minimum_report_date
                        ):
                            continue
                        accepted = dict(latest)
                        if coverage_window_end < as_of_date:
                            accepted["_incremental_discovery_binding"] = {
                                "coverage_id": str(
                                    incremental_discovery.get("coverage_id") or ""
                                ),
                                "source_response_hash": str(
                                    incremental_discovery.get(
                                        "source_response_hash"
                                    )
                                    or ""
                                ),
                                "watermark_hash": str(
                                    incremental_discovery.get("watermark_hash") or ""
                                ),
                                "window_start": incremental_discovery[
                                    "window_start"
                                ].isoformat(),
                                "window_end": incremental_discovery[
                                    "window_end"
                                ].isoformat(),
                                "event_set_sha256": str(
                                    incremental_discovery.get(
                                        "event_set_sha256"
                                    )
                                    or ""
                                ),
                            }
                        complete_candidates.append(accepted)

                    candidates = (
                        unavailable_candidates or complete_candidates
                    )
                    if not candidates:
                        raise ValueError(
                            "finance atomic batch has no valid disposition "
                            f"for {code}"
                        )
                    candidates.sort(
                        key=lambda row: (
                            _row_datetime(row.get("known_at")),
                            int(row.get("revision_no") or 0),
                        )
                    )
                    chosen = candidates[-1]
                    payload = _parse_payload(chosen)
                    compact = {
                        key: value
                        for key, value in chosen.items()
                        if key != "payload_json"
                    }
                    known = _row_datetime(chosen.get("known_at"))
                    if (
                        str(chosen.get("coverage_status") or "")
                        == FINANCE_EXPECTED_UNAVAILABLE_STATUS
                    ):
                        evidence = payload.get("official_evidence")
                        compact["_expected_report_date"] = str(
                            payload.get("expected_report_date") or ""
                        )
                        compact["_reason_code"] = str(
                            payload.get("reason_code") or ""
                        )
                        compact["_official_evidence"] = dict(
                            evidence or {}
                        )
                        compact["_source_publication_at"] = (
                            normalize_decision_at(
                                str(
                                    (evidence or {}).get(
                                        "announcement_published_at"
                                    )
                                    or ""
                                )
                            )
                        )
                    elif chosen.get("_legal_no_data_binding"):
                        # The QMT catalog listing date and a stable empty
                        # primary response prove that no required period can
                        # legally exist; no finance fact is manufactured.
                        pass
                    else:
                        source_rows = [
                            dict(source_row)
                            for source_row in payload.get("source_rows") or ()
                            if isinstance(source_row, dict)
                        ]
                        report_dates = sorted(
                            {
                                _date_value(
                                    source_row.get("report_date"),
                                    required=True,
                                )
                                for source_row in source_rows
                            },
                            reverse=True,
                        )[:FINANCE_ATOMIC_BATCH_HISTORY_LIMIT]
                        strategy_rows = [
                            source_row
                            for source_row in source_rows
                            if _date_value(
                                source_row.get("report_date"), required=True
                            )
                            in report_dates
                        ]
                        if not strategy_rows:
                            raise ValueError(
                                "finance strategy prefix is empty"
                            )
                        publication_instants: list[datetime] = []
                        for source_row in strategy_rows:
                            exact_published = source_row.get("published_at")
                            if exact_published not in (None, ""):
                                publication_instants.append(
                                    normalize_decision_at(exact_published)
                                )
                                continue
                            publication_date = _date_value(
                                source_row.get("notice_date"), required=True
                            )
                            publication_instants.append(datetime.combine(
                                publication_date,
                                datetime.max.time(),
                            ).replace(microsecond=0))
                        compact["_latest_publication_at"] = max(
                            publication_instants
                        )
                        compact["_strategy_prefix_binding"] = {
                            "history_limit": (
                                FINANCE_ATOMIC_BATCH_HISTORY_LIMIT
                            ),
                            "report_date_count": len(report_dates),
                            "content_row_count": len(strategy_rows),
                            "latest_report_date": (
                                report_dates[0].isoformat()
                            ),
                            "oldest_report_date": (
                                report_dates[-1].isoformat()
                            ),
                            "content_root_sha256": canonical_hash(
                                {
                                    "schema": (
                                        "probiga.pit-finance-strategy-"
                                        "prefix.v1"
                                    ),
                                    "rows": sorted(
                                        strategy_rows,
                                        key=canonical_json,
                                    ),
                                }
                            ),
                        }
                    member_source_cutoff = known
                    if compact.get("_incremental_discovery_binding"):
                        # The original full-history receipt remains immutable.
                        # A catalog-wide, stable discovery receipt can extend
                        # the time through which that exact member is proven
                        # unchanged without relabelling its local known_at.
                        discovery_known = _row_datetime(
                            (incremental_discovery or {}).get("known_at")
                        )
                        member_source_cutoff = max(known, discovery_known)
                    source_cutoff = (
                        member_source_cutoff
                        if source_cutoff is None
                        else min(source_cutoff, member_source_cutoff)
                    )
                    selected.append(compact)

    if source_cutoff is None:
        raise ValueError("finance atomic batch catalog scope is empty")
    for row in selected:
        if (
            str(row.get("coverage_status") or "")
            == FINANCE_EXPECTED_UNAVAILABLE_STATUS
        ):
            if row.pop("_source_publication_at") > source_cutoff:
                raise ValueError(
                    "finance non-filing evidence postdates batch source cutoff"
                )
            continue
        if row.get("_legal_no_data_binding"):
            continue
        # Date-only provider values are conservatively interpreted as the end
        # of that day.  Exact announcement bindings may prove same-day order.
        if row.pop("_latest_publication_at") >= source_cutoff:
            raise ValueError(
                "finance content postdates batch source cutoff"
            )
    return selected, source_cutoff


def _finance_atomic_member(row: Mapping[str, Any]) -> dict[str, Any]:
    expected_report_date, reason_code = _finance_unavailable_metadata(row)
    unavailable_evidence = _finance_unavailable_evidence(row)
    return {
        "stock_code": str(row.get("stock_code") or "").zfill(6),
        "coverage_id": str(row.get("coverage_id") or ""),
        "scope_hash": str(row.get("scope_hash") or ""),
        "coverage_status": str(row.get("coverage_status") or ""),
        "source": str(row.get("source") or ""),
        "known_at": _dt_text(_row_datetime(row.get("known_at"))),
        "covered_through_at": _dt_text(
            _row_datetime(row.get("covered_through_at"))
        ),
        "source_response_hash": str(
            row.get("source_response_hash") or ""
        ),
        "fact_set_hash": str(row.get("fact_set_hash") or ""),
        "watermark_hash": str(row.get("watermark_hash") or ""),
        "expected_report_date": expected_report_date,
        "reason_code": reason_code,
        "incremental_discovery_binding": dict(
            row.get("_incremental_discovery_binding") or {}
        ),
        "legal_no_data_binding": dict(
            row.get("_legal_no_data_binding") or {}
        ),
        "valid_until": str(unavailable_evidence.get("valid_until") or ""),
        "next_retry_date": str(
            unavailable_evidence.get("next_retry_date") or ""
        ),
        "strategy_prefix_binding": dict(
            row.get("_strategy_prefix_binding") or {}
        ),
    }


def _finance_unavailable_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    if (
        str(row.get("coverage_status") or "")
        != FINANCE_EXPECTED_UNAVAILABLE_STATUS
    ):
        return {}
    embedded = row.get("_official_evidence")
    if isinstance(embedded, Mapping):
        return dict(embedded)
    payload = _parse_payload(row)
    evidence = payload.get("official_evidence")
    return dict(evidence) if isinstance(evidence, Mapping) else {}


def _finance_unavailable_metadata(
    row: Mapping[str, Any],
) -> tuple[str, str]:
    if (
        str(row.get("coverage_status") or "")
        != FINANCE_EXPECTED_UNAVAILABLE_STATUS
    ):
        return "", ""
    if "_expected_report_date" in row or "_reason_code" in row:
        return (
            str(row.get("_expected_report_date") or ""),
            str(row.get("_reason_code") or ""),
        )
    payload = _parse_payload(row)
    return (
        str(payload.get("expected_report_date") or ""),
        str(payload.get("reason_code") or ""),
    )


def append_finance_atomic_batch_seal(
    engine: Engine,
    *,
    as_of_date: date | str,
    completed_known_at: datetime | str | None = None,
    incremental_discovery_coverage_id: str = "",
    changed_codes: Iterable[str] | None = None,
    provider_contract_version: str = "",
) -> dict[str, Any]:
    """Seal one finance baseline plus its changed stock dispositions.

    ``changed_codes=None`` remains the explicit historical/full-repair path.
    Passing a collection creates a v2 delta-over-parent seal.  The immutable
    parent member set is reused, while new, stale, expired, and explicitly
    changed members alone are selected from the per-stock coverage ledger.
    """

    target = _date_value(as_of_date, required=True)
    if target is None:
        raise ValueError("finance atomic batch as-of date is required")
    completed = normalize_decision_at(
        completed_known_at or datetime.now(SHANGHAI)
    )
    from server.common.qmt_stock_catalog import load_stock_catalog

    with engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at=completed,
        )
    codes = catalog.eligible_codes(target.isoformat())
    if not codes:
        raise ValueError("finance atomic batch catalog scope is empty")
    listing_dates = {
        str(item.get("stock_code") or "").zfill(6): _date_value(
            item.get("list_date"), required=False
        )
        for item in catalog.members
    }
    catalog_binding = {
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": catalog.member_count,
    }
    incremental_discovery: dict[str, Any] = {}
    if incremental_discovery_coverage_id:
        incremental_discovery = load_finance_incremental_discovery(
            engine,
            coverage_id=incremental_discovery_coverage_id,
            codes=codes,
            decision_at=completed,
            as_of_date=target,
        )
    prior_row: dict[str, Any] = {}
    prior_evidence: dict[str, Any] = {}
    prior_members: dict[str, dict[str, Any]] = {}
    if changed_codes is not None:
        prior_row, prior_evidence, prior_members, _ = (
            _load_latest_finance_atomic_seal_evidence(
                engine,
                decision_at=completed,
            )
        )

    requested_changes = {
        str(code).strip().zfill(6)
        for code in (changed_codes or ())
        if str(code).strip()
    }
    unknown_changes = requested_changes - set(codes)
    if unknown_changes:
        raise ValueError(
            "finance atomic delta contains codes outside current catalog: "
            + ",".join(sorted(unknown_changes)[:20])
        )
    effective_changes = set(codes) if not prior_members else set(requested_changes)
    effective_changes.update(set(codes) - set(prior_members))
    effective_changes.update(
        str(code).zfill(6)
        for code in (incremental_discovery.get("changed_dates_by_code") or {})
        if str(code).zfill(6) in codes
    )
    gate = finance_disclosure_gate(target)
    for code in set(codes) & set(prior_members):
        member = prior_members[code]
        listing_date = listing_dates.get(code)
        if (
            str(member.get("coverage_status") or "")
            == FINANCE_EXPECTED_UNAVAILABLE_STATUS
        ):
            valid_until = _date_value(
                member.get("valid_until"), required=False
            )
            next_retry = _date_value(
                member.get("next_retry_date"), required=False
            )
            if (
                str(member.get("expected_report_date") or "")
                != gate.minimum_report_date.isoformat()
                or valid_until is None
                or valid_until < target
                or next_retry is None
                or next_retry <= target
            ):
                effective_changes.add(code)
            continue
        prefix = member.get("strategy_prefix_binding")
        latest_report = _date_value(
            prefix.get("latest_report_date")
            if isinstance(prefix, Mapping)
            else None,
            required=False,
        )
        if (
            latest_report is None
            or (
                report_period_gate_applies(listing_date, gate)
                and latest_report < gate.minimum_report_date
            )
        ):
            effective_changes.add(code)

    changed = sorted(effective_changes)
    selected: list[dict[str, Any]] = []
    delta_source_cutoff: datetime | None = None
    if changed:
        selected, delta_source_cutoff = _select_finance_atomic_batch_members(
            engine,
            codes=changed,
            listing_dates=listing_dates,
            completed_known_at=completed,
            as_of_date=target,
            incremental_discovery=incremental_discovery,
            catalog_binding=catalog_binding,
        )
    delta_members = sorted(
        [_finance_atomic_member(row) for row in selected],
        key=lambda item: item["stock_code"],
    )
    member_map = {
        code: dict(member)
        for code, member in prior_members.items()
        if code in codes and code not in effective_changes
    }
    member_map.update({item["stock_code"]: item for item in delta_members})
    if set(member_map) != set(codes):
        missing = sorted(set(codes) - set(member_map))
        raise ValueError(
            "finance atomic batch has no reusable disposition for: "
            + ",".join(missing[:20])
        )
    members = [member_map[code] for code in codes]
    prior_source_cutoff = _date_time_value(
        prior_evidence.get("source_cutoff_at"), required=False
    )
    source_cutoffs = [
        value
        for value in (prior_source_cutoff, delta_source_cutoff)
        if value is not None
    ]
    if not source_cutoffs:
        raise ValueError("finance atomic batch source cutoff is unavailable")
    source_cutoff = min(source_cutoffs)
    code_set_hash = canonical_hash({
        "schema": "probiga.pit-finance-atomic-code-set.v1",
        "codes": codes,
    })
    coverage_root = canonical_hash({
        "schema": "probiga.pit-finance-atomic-coverage-set.v1",
        "members": members,
    })
    unavailable_members = [
        item
        for item in members
        if item["coverage_status"] == FINANCE_EXPECTED_UNAVAILABLE_STATUS
    ]
    parent_coverage_id = str(prior_row.get("coverage_id") or "")
    parent_root = str(prior_evidence.get("batch_root_sha256") or "")
    delta_root = canonical_hash({
        "schema": "probiga.pit-finance-atomic-delta.v1",
        "parent_batch_root_sha256": parent_root,
        "as_of_date": target.isoformat(),
        "changed_codes": changed,
        "members": delta_members,
    })
    core = {
        "schema": FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA,
        "seal_mode": "DELTA_OVER_PARENT" if prior_members else "FULL_BASELINE",
        "as_of_date": target.isoformat(),
        "minimum_report_date": finance_disclosure_gate(
            target
        ).minimum_report_date.isoformat(),
        "provider_contract_version": str(provider_contract_version or ""),
        "source_cutoff_at": _dt_text(source_cutoff),
        "completed_known_at": _dt_text(completed),
        "catalog_batch_id": catalog.batch_id,
        "catalog_captured_at": catalog.captured_at,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": catalog.member_count,
        "eligible_code_count": len(codes),
        "eligible_code_set_hash": code_set_hash,
        "coverage_root_sha256": coverage_root,
        "expected_unavailable_count": len(unavailable_members),
        "expected_unavailable_root_sha256": canonical_hash({
            "schema": "probiga.pit-finance-atomic-unavailable-set.v1",
            "members": unavailable_members,
        }),
        "incremental_discovery_binding": (
            {
                key: (
                    value.isoformat() if isinstance(value, date) else value
                )
                for key, value in incremental_discovery.items()
                if key not in {"changed_dates_by_code"}
            }
            if incremental_discovery
            else {}
        ),
        "parent_seal_coverage_id": parent_coverage_id,
        "parent_batch_root_sha256": parent_root,
        "changed_code_count": len(changed),
        "changed_codes": changed,
        "delta_root_sha256": delta_root,
        "delta_members": delta_members,
        "members": members,
    }
    batch_root = canonical_hash(core)
    evidence = {**core, "batch_root_sha256": batch_root}
    receipt = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=FINANCE_ATOMIC_BATCH_CODE,
        window_start="1900-01-01",
        window_end=target,
        known_at=completed,
        received_at=completed,
        covered_through_at=completed,
        watermark_kind="CAPTURED_AT",
        watermark_evidence=evidence,
        source_rows=[],
        fact_bindings=[],
        source=FINANCE_ATOMIC_BATCH_SOURCE,
        batch_id=f"finance-atomic-{batch_root[:32]}",
    )
    return {
        **{
            key: value
            for key, value in core.items()
            if key not in {"members", "delta_members", "changed_codes"}
        },
        "batch_root_sha256": batch_root,
        "seal_coverage_id": receipt.coverage_id,
        "idempotent": receipt.idempotent,
    }


def _date_time_value(
    value: Any,
    *,
    required: bool,
) -> datetime | None:
    if value in (None, "") and not required:
        return None
    try:
        return normalize_decision_at(value)
    except (TypeError, ValueError):
        if required:
            raise
        return None


def _load_latest_finance_atomic_seal_evidence(
    engine: Engine,
    *,
    decision_at: datetime | str,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    decision = normalize_decision_at(decision_at)
    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        "WHERE fact_kind='finance' AND stock_code=:seal_code "
        "AND source=:seal_source AND known_at<=:decision_at "
        "AND received_at<=:decision_at "
        "AND (:has_cursor=0 OR scope_hash>:after_scope "
        "OR (scope_hash=:after_scope AND revision_no>:after_revision)) "
        "ORDER BY scope_hash, revision_no LIMIT 1"
    )
    seal_rows: list[dict[str, Any]] = []
    latest_row: dict[str, Any] = {}
    with engine.connect() as connection:
        if connection.dialect.name == "mysql" and str(
            connection.get_isolation_level() or ""
        ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
            raise ValueError("finance seal history requires a repeatable-read snapshot")

        def history_rows() -> Iterable[dict[str, Any]]:
            nonlocal latest_row
            parameters = {
                "seal_code": FINANCE_ATOMIC_BATCH_CODE,
                "seal_source": FINANCE_ATOMIC_BATCH_SOURCE,
                "decision_at": decision,
                "has_cursor": 0,
                "after_scope": "",
                "after_revision": 0,
            }
            while True:
                try:
                    raw = connection.execute(statement, parameters).mappings().first()
                except OperationalError as exc:
                    exc._pit_read_stage = "finance_seal_history"
                    raise
                if raw is None:
                    return
                current = dict(raw)
                # The consumer validates every original payload and chain link.
                # Keeping all full-catalog seal JSONs used over 226 MB before
                # parsing; retain only the latest payload and parent root index.
                yield current
                payload = _parse_payload(current)
                watermark = payload.get("watermark")
                evidence = watermark.get("evidence") if isinstance(watermark, dict) else None
                seal_rows.append({
                    "coverage_id": current.get("coverage_id"),
                    "batch_root_sha256": (
                        evidence.get("batch_root_sha256")
                        if isinstance(evidence, dict) else None
                    ),
                })
                latest_row = current
                parameters.update({
                    "has_cursor": 1,
                    "after_scope": current["scope_hash"],
                    "after_revision": current["revision_no"],
                })
                del payload, watermark, evidence, raw, current

        with connection.begin():
            _validate_coverage_chain(history_rows())
    if not seal_rows:
        return {}, {}, {}, []
    row = latest_row
    payload = _parse_payload(row)
    watermark = payload.get("watermark")
    evidence = (
        watermark.get("evidence")
        if isinstance(watermark, dict)
        else None
    )
    if not isinstance(evidence, dict):
        raise ValueError("finance atomic batch seal evidence is malformed")
    members = evidence.get("members")
    if not isinstance(members, list) or not members:
        raise ValueError("finance atomic batch seal members are unavailable")
    core = {
        key: value
        for key, value in evidence.items()
        if key not in {"batch_root_sha256", "captured_at"}
    }
    schema = str(evidence.get("schema") or "")
    if (
        schema
        not in {
            FINANCE_ATOMIC_BATCH_SCHEMA,
            FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA,
        }
        or evidence.get("batch_root_sha256") != canonical_hash(core)
        or str(row.get("coverage_status") or "") != "COMPLETE"
        or int(row.get("result_count") or 0) != 0
        or _row_datetime(evidence.get("completed_known_at"))
        != _row_datetime(row.get("known_at"))
        or _row_datetime(evidence.get("source_cutoff_at"))
        > _row_datetime(row.get("known_at"))
        or int(evidence.get("eligible_code_count") or 0) != len(members)
        or evidence.get("coverage_root_sha256") != canonical_hash({
            "schema": "probiga.pit-finance-atomic-coverage-set.v1",
            "members": members,
        })
    ):
        raise ValueError("finance atomic batch seal root differs")
    member_map = {
        str(item.get("stock_code") or "").zfill(6): dict(item)
        for item in members
        if isinstance(item, dict)
    }
    if len(member_map) != len(members):
        raise ValueError("finance atomic batch seal code scope differs")
    if schema == FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA:
        changed_codes = evidence.get("changed_codes")
        delta_members = evidence.get("delta_members")
        if (
            not isinstance(changed_codes, list)
            or changed_codes != sorted(set(changed_codes))
            or not isinstance(delta_members, list)
            or [
                str(item.get("stock_code") or "").zfill(6)
                for item in delta_members
                if isinstance(item, dict)
            ]
            != changed_codes
            or int(evidence.get("changed_code_count") or 0)
            != len(changed_codes)
            or evidence.get("delta_root_sha256") != canonical_hash({
                "schema": "probiga.pit-finance-atomic-delta.v1",
                "parent_batch_root_sha256": str(
                    evidence.get("parent_batch_root_sha256") or ""
                ),
                "as_of_date": str(evidence.get("as_of_date") or ""),
                "changed_codes": changed_codes,
                "members": delta_members,
            })
        ):
            raise ValueError("finance atomic batch delta root differs")
        parent_id = str(evidence.get("parent_seal_coverage_id") or "")
        parent_root = str(evidence.get("parent_batch_root_sha256") or "")
        if bool(parent_id) != bool(parent_root):
            raise ValueError("finance atomic batch parent identity differs")
        if parent_id:
            parent_rows = [
                item
                for item in seal_rows[:-1]
                if str(item.get("coverage_id") or "") == parent_id
            ]
            if len(parent_rows) != 1:
                raise ValueError("finance atomic batch parent is unavailable")
            if str(parent_rows[0].get("batch_root_sha256") or "") != parent_root:
                raise ValueError("finance atomic batch parent root differs")
    return row, evidence, member_map, seal_rows


def load_latest_finance_atomic_batch_baseline(
    engine: Engine,
    *,
    decision_at: datetime | str,
) -> dict[str, Any]:
    """Return the latest self-hashed member baseline without a market rescan."""

    row, evidence, member_map, _ = _load_latest_finance_atomic_seal_evidence(
        engine,
        decision_at=decision_at,
    )
    if not row:
        return {}
    return {
        **{
            key: value
            for key, value in evidence.items()
            if key not in {"members", "delta_members", "changed_codes"}
        },
        "seal_coverage_id": str(row.get("coverage_id") or ""),
        "members": member_map,
    }


def _finance_atomic_row_from_member(
    member: Mapping[str, Any],
) -> dict[str, Any]:
    """Rehydrate the compact fields consumed by the sealed strategy reader."""

    return {
        "stock_code": str(member.get("stock_code") or "").zfill(6),
        "coverage_id": str(member.get("coverage_id") or ""),
        "scope_hash": str(member.get("scope_hash") or ""),
        "coverage_status": str(member.get("coverage_status") or ""),
        "source": str(member.get("source") or ""),
        "known_at": str(member.get("known_at") or ""),
        "received_at": str(member.get("known_at") or ""),
        "covered_through_at": str(member.get("covered_through_at") or ""),
        "source_response_hash": str(
            member.get("source_response_hash") or ""
        ),
        "fact_set_hash": str(member.get("fact_set_hash") or ""),
        "watermark_hash": str(member.get("watermark_hash") or ""),
        "_expected_report_date": str(
            member.get("expected_report_date") or ""
        ),
        "_reason_code": str(member.get("reason_code") or ""),
        "_strategy_prefix_binding": dict(
            member.get("strategy_prefix_binding") or {}
        ),
    }


def load_finance_atomic_batch_seal(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    as_of_date: date | str,
) -> dict[str, Any]:
    """Validate and resolve the latest immutable full-catalog finance seal."""

    requested = sorted({
        str(code).strip().zfill(6)
        for code in codes
        if str(code).strip()
    })
    decision = normalize_decision_at(decision_at)
    target = _date_value(as_of_date, required=True)
    if target is None or not requested:
        return {}
    row, evidence, member_map, _ = _load_latest_finance_atomic_seal_evidence(
        engine,
        decision_at=decision,
    )
    if not row:
        return {}
    if (
        str(row.get("coverage_status") or "") != "COMPLETE"
        or int(row.get("result_count") or 0) != 0
        or _date_value(row.get("window_start"), required=True)
        > date(1900, 1, 1)
        or _date_value(row.get("window_end"), required=True) < target
    ):
        return {}
    members = evidence.get("members")
    if len(member_map) != len(members) or not set(requested) <= set(member_map):
        raise ValueError("finance atomic batch seal code scope differs")

    from server.common.qmt_stock_catalog import load_stock_catalog

    with engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at=decision,
            batch_id=str(evidence.get("catalog_batch_id") or ""),
        )
    sealed_as_of = _date_value(evidence.get("as_of_date"), required=True)
    completed = _row_datetime(evidence.get("completed_known_at"))
    if sealed_as_of is None:
        raise ValueError("finance atomic batch seal date is invalid")
    catalog_codes = catalog.eligible_codes(sealed_as_of.isoformat())
    if (
        catalog.manifest_hash != evidence.get("catalog_manifest_hash")
        or catalog.member_set_hash != evidence.get("catalog_member_set_hash")
        or catalog.member_count != int(evidence.get("catalog_member_count") or 0)
        or catalog_codes != sorted(member_map)
        or canonical_hash({
            "schema": "probiga.pit-finance-atomic-code-set.v1",
            "codes": catalog_codes,
        }) != evidence.get("eligible_code_set_hash")
    ):
        raise ValueError("finance atomic batch catalog proof differs")
    listing_dates = {
        str(item.get("stock_code") or "").zfill(6): _date_value(
            item.get("list_date"), required=False
        )
        for item in catalog.members
    }
    catalog_binding = {
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": catalog.member_count,
    }
    incremental_discovery: dict[str, Any] = {}
    embedded_discovery = evidence.get("incremental_discovery_binding")
    if embedded_discovery:
        if not isinstance(embedded_discovery, dict):
            raise ValueError("finance atomic incremental discovery is malformed")
        incremental_discovery = load_finance_incremental_discovery(
            engine,
            coverage_id=str(embedded_discovery.get("coverage_id") or ""),
            codes=catalog_codes,
            decision_at=completed,
            as_of_date=sealed_as_of,
        )
        rebuilt_discovery = {
            key: value.isoformat() if isinstance(value, date) else value
            for key, value in incremental_discovery.items()
            if key not in {"changed_dates_by_code"}
        }
        if rebuilt_discovery != embedded_discovery:
            raise ValueError("finance atomic incremental discovery proof differs")
    schema = str(evidence.get("schema") or "")
    validation_codes = (
        catalog_codes
        if schema == FINANCE_ATOMIC_BATCH_SCHEMA
        else [str(code).zfill(6) for code in evidence.get("changed_codes") or ()]
    )
    selected: list[dict[str, Any]] = []
    delta_source_cutoff: datetime | None = None
    if validation_codes:
        selected, delta_source_cutoff = _select_finance_atomic_batch_members(
            engine,
            codes=validation_codes,
            listing_dates=listing_dates,
            completed_known_at=completed,
            as_of_date=sealed_as_of,
            incremental_discovery=incremental_discovery,
            catalog_binding=catalog_binding,
        )
    selected_map = {
        str(item.get("stock_code") or "").zfill(6): item
        for item in selected
    }
    rebuilt_members = sorted(
        [_finance_atomic_member(item) for item in selected_map.values()],
        key=lambda item: item["stock_code"],
    )
    expected_members = (
        members
        if schema == FINANCE_ATOMIC_BATCH_SCHEMA
        else evidence.get("delta_members")
    )
    source_cutoff_matches = True
    if schema == FINANCE_ATOMIC_BATCH_SCHEMA:
        source_cutoff_matches = (
            delta_source_cutoff is not None
            and _dt_text(delta_source_cutoff) == evidence.get("source_cutoff_at")
        )
    elif delta_source_cutoff is not None:
        parent_cutoff = _date_time_value(
            evidence.get("source_cutoff_at"), required=True
        )
        source_cutoff_matches = delta_source_cutoff >= parent_cutoff
    if (
        rebuilt_members != expected_members
        or not source_cutoff_matches
        or finance_disclosure_gate(sealed_as_of).minimum_report_date.isoformat()
        != evidence.get("minimum_report_date")
    ):
        raise ValueError("finance atomic batch member proof differs")
    sealed_rows = {
        code: _finance_atomic_row_from_member(member)
        for code, member in member_map.items()
    }
    sealed_rows.update(selected_map)
    return {
        "seal_coverage_id": str(row.get("coverage_id") or ""),
        "batch_root_sha256": str(evidence.get("batch_root_sha256") or ""),
        "source_cutoff_at": str(evidence.get("source_cutoff_at") or ""),
        "completed_known_at": str(evidence.get("completed_known_at") or ""),
        "as_of_date": sealed_as_of.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "eligible_code_count": len(catalog_codes),
        "members": {code: member_map[code] for code in requested},
        "rows": {code: sealed_rows[code] for code in requested},
        "schema": schema,
        "changed_code_count": int(evidence.get("changed_code_count") or 0),
        "parent_batch_root_sha256": str(
            evidence.get("parent_batch_root_sha256") or ""
        ),
        "delta_root_sha256": str(evidence.get("delta_root_sha256") or ""),
    }


def resolve_common_fact_cutoff(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    finance_start_date: date | str,
    finance_end_date: date | str,
    event_start_date: date | str,
    event_end_date: date | str,
    require_qmt_event_batch: bool = False,
) -> dict[str, Any]:
    """Resolve one immutable COMPLETE-coverage cutoff (T) for a live batch (E)."""

    normalized_codes = sorted(
        {str(code).strip().zfill(6) for code in codes if str(code).strip()}
    )
    decision = normalize_decision_at(decision_at)
    windows = {
        "finance": (
            _date_value(finance_start_date, required=True),
            _date_value(finance_end_date, required=True),
        ),
        "event": (
            _date_value(event_start_date, required=True),
            _date_value(event_end_date, required=True),
        ),
    }
    blocked = {
        "status": PIT_DATA_BLOCKED,
        "fact_cutoff_at": "",
        "decision_at": _dt_text(decision),
        "receipt_root_hash": "",
        "receipts": [],
    }
    if not normalized_codes:
        return {**blocked, "reason": "PIT_COMMON_CUTOFF_EMPTY_SCOPE"}
    for kind, (start, end) in windows.items():
        if start is None or end is None or start > end:
            raise ValueError(f"{kind} coverage window is invalid")
    try:
        finance_batch = load_finance_atomic_batch_seal(
            engine,
            codes=normalized_codes,
            decision_at=decision,
            as_of_date=windows["finance"][1],
        )
    except Exception as exc:
        diagnostic = type(exc).__name__
        if isinstance(exc, OperationalError):
            args = getattr(getattr(exc, "orig", None), "args", ())
            if args and isinstance(args[0], int):
                diagnostic += f":errno={args[0]}"
            stage = getattr(exc, "_pit_read_stage", "")
            if stage == "finance_seal_history":
                diagnostic += f":stage={stage}"
        return {
            **blocked,
            "reason": "PIT_FINANCE_ATOMIC_BATCH_INVALID:"
            f"{diagnostic}",
        }
    event_batch: dict[str, Any] = {}
    if require_qmt_event_batch:
        try:
            from server.common.qmt_announcement_pit import (
                validate_complete_announcement_batch,
            )

            event_batch = validate_complete_announcement_batch(
                engine,
                codes=normalized_codes,
                decision_at=decision,
                window_start=windows["event"][0],
                window_end=windows["event"][1],
            )
        except Exception as exc:
            reason_code = str(getattr(exc, "reason_code", "") or "")
            return {
                **blocked,
                "reason": reason_code
                or f"PIT_QMT_EVENT_BATCH_UNAVAILABLE:{type(exc).__name__}",
            }
    event_filter = ""
    query_params: dict[str, Any] = {
        "codes": normalized_codes,
        "decision_at": decision,
    }
    if event_batch:
        event_filter = (
            "AND (fact_kind='finance' OR "
            "source=:event_source) "
        )
        query_params.update(
            {
                "event_source": str(event_batch.get("source") or ""),
            }
        )
    # The validated finance seal already supplies every selected finance row.
    # Loading their full historical payloads again can exceed 500 MB, and those
    # candidates are never used below. Unsealed finance still needs this scan.
    fact_kind_filter = (
        "fact_kind='event'"
        if finance_batch
        else "fact_kind IN ('finance','event')"
    )
    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        f"WHERE {fact_kind_filter} AND stock_code IN :codes "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        f"{event_filter}"
        "ORDER BY fact_kind, stock_code, scope_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    read_errors: list[str] = []

    def coverage_chains():
        # A code stays in one chunk, so every revision of every scope is still
        # validated together. The snapshot matches the former single SELECT.
        batch_size = max(1, int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT))
        try:
            with engine.connect() as connection:
                if connection.dialect.name == "mysql" and str(
                    connection.get_isolation_level() or ""
                ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                    raise ValueError("common cutoff requires a repeatable-read snapshot")
                with connection.begin():
                    for offset in range(0, len(normalized_codes), batch_size):
                        parameters = {
                            **query_params,
                            "codes": normalized_codes[offset:offset + batch_size],
                        }
                        chains: dict[
                            tuple[str, str, str], list[dict[str, Any]]
                        ] = defaultdict(list)
                        for raw in connection.execute(statement, parameters).mappings():
                            row = dict(raw)
                            chains[(
                                str(row.get("fact_kind") or "").lower(),
                                str(row.get("stock_code") or "").zfill(6),
                                str(row.get("scope_hash") or ""),
                            )].append(row)
                        yield from chains.items()
                        chains.clear()
        except Exception as exc:
            read_errors.append(type(exc).__name__)

    candidates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    bad: set[tuple[str, str]] = set()
    required_finance_report = finance_disclosure_gate(
        windows["finance"][1]
    ).minimum_report_date
    for (kind, code, _scope), chain in coverage_chains():
        try:
            _validate_coverage_chain(chain)
        except ValueError:
            bad.add((kind, code))
            continue
        latest = chain[-1]
        if kind == "event" and event_batch:
            sealed_batch_id = str(event_batch.get("batch_id") or "")
            sealed_rows = [
                row for row in chain
                if str(row.get("batch_id") or "") == sealed_batch_id
            ]
            if not sealed_rows:
                continue
            # The whole revision chain is validated above.  Select the row
            # sealed by the authoritative event batch rather than allowing a
            # later, valid live revision in the same scope to hide it.
            latest = sealed_rows[-1]
        start, end = windows[kind]
        complete_window = (
            str(latest.get("coverage_status") or "") == "COMPLETE"
            and (
                kind != "event"
                or not event_batch
                or str(latest.get("batch_id") or "")
                == str(event_batch.get("batch_id") or "")
            )
            and _date_value(latest.get("window_start"), required=True) <= start
            and _date_value(latest.get("window_end"), required=True) >= end
        )
        expected_unavailable = False
        if (
            kind == "finance"
            and str(latest.get("coverage_status") or "")
            == FINANCE_EXPECTED_UNAVAILABLE_STATUS
        ):
            payload = _parse_payload(latest)
            evidence = payload.get("official_evidence")
            if isinstance(evidence, dict):
                valid_from = _date_value(
                    evidence.get("valid_from"), required=True
                )
                valid_until = _date_value(
                    evidence.get("valid_until"), required=True
                )
                expected_unavailable = bool(
                    str(payload.get("expected_report_date") or "")
                    == required_finance_report.isoformat()
                    and valid_from is not None
                    and valid_until is not None
                    and valid_from <= decision.date() <= valid_until
                )
        if complete_window or expected_unavailable:
            compact = {
                key: value for key, value in latest.items() if key != "payload_json"
            }
            if str(latest.get("coverage_status") or "") == FINANCE_EXPECTED_UNAVAILABLE_STATUS:
                compact["_expected_report_date"], compact["_reason_code"] = (
                    _finance_unavailable_metadata(latest)
                )
            candidates[(kind, code)].append(compact)
    if read_errors:
        return {
            **blocked,
            "reason": "PIT_COMMON_CUTOFF_SCHEMA_UNAVAILABLE:" + read_errors[0],
        }
    selected: list[dict[str, Any]] = []
    missing: list[str] = []
    for kind in ("finance", "event"):
        for code in normalized_codes:
            if kind == "finance" and finance_batch:
                sealed = dict(finance_batch["rows"][code])
                sealed["_effective_covered_through_at"] = (
                    finance_batch["completed_known_at"]
                )
                sealed["_finance_batch_seal_id"] = finance_batch[
                    "seal_coverage_id"
                ]
                selected.append(sealed)
                continue
            rows_for_requirement = candidates.get((kind, code), [])
            if not rows_for_requirement:
                suffix = "BAD_CHAIN" if (kind, code) in bad else "MISSING"
                missing.append(f"{kind}:{code}:{suffix}")
                continue
            rows_for_requirement.sort(
                key=lambda row: (
                    _row_datetime(row.get("covered_through_at")),
                    _row_datetime(row.get("known_at")),
                    int(row.get("revision_no") or 0),
                )
            )
            selected.append(rows_for_requirement[-1])
    if missing:
        return {
            **blocked,
            "reason": "PIT_COMMON_CUTOFF_INCOMPLETE:" + ",".join(missing),
        }
    fact_cutoff = min(
        _row_datetime(
            row.get("_effective_covered_through_at")
            or row.get("covered_through_at")
        )
        for row in selected
    )
    retrospective_batch_id = (
        str(event_batch.get("batch_id") or "")
        if event_batch.get("mode") == "HISTORICAL_RECONSTRUCTION"
        else ""
    )
    if any(
        not row.get("_finance_batch_seal_id")
        and not (
            retrospective_batch_id
            and str(row.get("fact_kind") or "") == "event"
            and str(row.get("batch_id") or "") == retrospective_batch_id
            and str(row.get("watermark_kind") or "")
            == "HISTORICAL_RECONSTRUCTION"
            and _row_datetime(row.get("known_at")) <= decision
        )
        and not _live_capture_allowed(
                fact_cutoff_at=fact_cutoff,
                decision_at=decision,
                known_at=_row_datetime(row.get("known_at")),
            )
        for row in selected
    ):
        return {**blocked, "reason": "PIT_COMMON_CUTOFF_STALE_OR_BACKFILL"}
    receipts = sorted(
        [
            {
                "fact_kind": str(row.get("fact_kind") or ""),
                "stock_code": str(row.get("stock_code") or "").zfill(6),
                "coverage_id": str(row.get("coverage_id") or ""),
                "coverage_status": str(row.get("coverage_status") or ""),
                "source": str(row.get("source") or ""),
                "watermark_kind": str(row.get("watermark_kind") or ""),
                "watermark_hash": str(row.get("watermark_hash") or ""),
                "source_response_hash": str(row.get("source_response_hash") or ""),
                "batch_id": str(row.get("batch_id") or ""),
                "known_at": _dt_text(_row_datetime(row.get("known_at"))),
                "covered_through_at": _dt_text(
                    _row_datetime(row.get("covered_through_at"))
                ),
                "effective_covered_through_at": _dt_text(
                    _row_datetime(
                        row.get("_effective_covered_through_at")
                        or row.get("covered_through_at")
                    )
                ),
                "finance_batch_seal_id": str(
                    row.get("_finance_batch_seal_id") or ""
                ),
                "expected_report_date": _finance_unavailable_metadata(row)[0],
                "reason_code": _finance_unavailable_metadata(row)[1],
            }
            for row in selected
        ],
        key=lambda item: (item["fact_kind"], item["stock_code"]),
    )
    root_payload = {
        "schema": "probiga.pit-common-fact-cutoff.v1",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "decision_at": _dt_text(decision),
        "windows": {
            kind: {"start": start.isoformat(), "end": end.isoformat()}
            for kind, (start, end) in windows.items()
        },
        "receipts": receipts,
    }
    if event_batch:
        root_payload["qmt_event_batch"] = event_batch
    if finance_batch:
        root_payload["finance_atomic_batch"] = {
            key: value
            for key, value in finance_batch.items()
            if key not in {"members", "rows"}
        }
    result = {
        "status": PIT_AVAILABLE,
        "reason": "",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "decision_at": _dt_text(decision),
        "receipt_root_hash": canonical_hash(root_payload),
        "receipts": receipts,
    }
    if event_batch:
        result["qmt_event_batch"] = event_batch
    if finance_batch:
        result["finance_atomic_batch"] = {
            key: value
            for key, value in finance_batch.items()
            if key not in {"members", "rows"}
        }
    return result


def load_finance_facts(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None = None,
    as_of_date: date | str | None = None,
) -> PITFactBatch:
    normalized_codes = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    fact_cutoff, decision = _fact_and_decision_times(
        decision_at=decision_at, fact_cutoff_at=fact_cutoff_at,
    )
    end_date = _date_value(as_of_date, required=False) or fact_cutoff.date()
    if not normalized_codes:
        return PITFactBatch(
            manifest_hash=canonical_hash([]), decision_at=_dt_text(decision),
            fact_cutoff_at=_dt_text(fact_cutoff),
            table_name=FINANCE_REVISION_TABLE,
        )
    revision_errors: list[str] = []

    def revision_groups():
        # One historical finance scan held 264k rows (214 MiB of JSON) and
        # exceeded 980 MiB RSS. Keep all report/revision identities for a code
        # together, process it unchanged, then release the code chunk.
        batch_size = max(1, int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT))
        try:
            with engine.connect() as connection:
                if connection.dialect.name == "mysql" and str(
                    connection.get_isolation_level() or ""
                ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                    raise ValueError("finance revisions require a repeatable-read snapshot")
                with connection.begin():
                    for offset in range(0, len(normalized_codes), batch_size):
                        code_batch = normalized_codes[offset:offset + batch_size]
                        rows = _query_revisions(
                            connection, table_name=FINANCE_REVISION_TABLE,
                            codes=code_batch, decision_at=decision,
                            start_date=None, end_date=end_date,
                        )
                        by_code_identity: dict[
                            str, dict[str, list[dict[str, Any]]]
                        ] = defaultdict(lambda: defaultdict(list))
                        for row in rows:
                            by_code_identity[str(row.get("stock_code") or "").zfill(6)][
                                str(row.get("identity_hash") or "")
                            ].append(row)
                        for code in code_batch:
                            yield code, by_code_identity.get(code, {})
                        rows.clear()
                        by_code_identity.clear()
        except Exception as exc:
            revision_errors.append(type(exc).__name__)

    groups = revision_groups()
    first_group = next(groups, None)
    if revision_errors:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason="PIT_FINANCE_SCHEMA_UNAVAILABLE:" + revision_errors[0],
        )
    try:
        empty_coverage, coverage_errors = _authoritative_empty_coverage(
            engine,
            fact_kind="finance",
            codes=normalized_codes,
            fact_cutoff_at=fact_cutoff,
            decision_at=decision,
            start_date=date(1900, 1, 1),
            end_date=end_date,
        )
    except Exception as exc:
        empty_coverage = {}
        coverage_errors = {
            code: f"PIT_FINANCE_COVERAGE_SCHEMA_UNAVAILABLE:{type(exc).__name__}"
            for code in normalized_codes
        }
    required_report_date = finance_disclosure_gate(end_date).minimum_report_date
    try:
        expected_unavailable, unavailable_errors = (
            load_finance_expected_unavailable(
                engine,
                codes=normalized_codes,
                decision_at=decision,
                expected_report_date=required_report_date,
            )
        )
    except Exception as exc:
        expected_unavailable = {}
        unavailable_errors = {
            code: (
                "PIT_FINANCE_UNAVAILABLE_SCHEMA_UNAVAILABLE:"
                f"{type(exc).__name__}"
            )
            for code in normalized_codes
        }
    coverage_errors.update(unavailable_errors)
    resolved_coverage = {
        **empty_coverage,
        **expected_unavailable,
    }
    facts: dict[str, dict[str, Any]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code, identities in itertools.chain((first_group,), groups):
        if not identities:
            if code in expected_unavailable:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            elif code in empty_coverage:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_AUTHORITATIVE_EMPTY"
            elif code in coverage_errors and "BAD_COVERAGE_CHAIN" in coverage_errors[code]:
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = coverage_errors[code]
            else:
                statuses[code] = PIT_NO_ROWS
                reasons[code] = "PIT_FINANCE_NO_ROWS"
            continue
        latest_by_identity: list[dict[str, Any]] = []
        selection_reasons: list[str] = []
        try:
            for chain in identities.values():
                selected, selection_reason = _latest_revision_for_fact_cutoff(
                    chain,
                    fact_kind="finance",
                    fact_cutoff_at=fact_cutoff,
                    decision_at=decision,
                )
                if selected is not None:
                    latest_by_identity.append(selected)
                elif selection_reason:
                    selection_reasons.append(selection_reason)
        except ValueError as exc:
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = f"PIT_FINANCE_BAD_CHAIN:{exc}"
            continue
        if not latest_by_identity:
            if code in expected_unavailable:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            elif code in empty_coverage:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_AUTHORITATIVE_EMPTY"
            elif selection_reasons:
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = sorted(selection_reasons)[0]
            else:
                statuses[code] = PIT_NO_ROWS
                reasons[code] = "PIT_FINANCE_NO_ROWS_AT_FACT_CUTOFF"
            continue
        newest_report = max(
            _date_value(row.get("report_date"), required=True)
            for row in latest_by_identity
        )
        if (
            code in expected_unavailable
            and newest_report < required_report_date
        ):
            statuses[code] = PIT_AVAILABLE
            reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            continue
        candidates = [
            row for row in latest_by_identity
            if _date_value(row.get("report_date"), required=True) == newest_report
        ]
        candidates.sort(
            key=lambda row: (
                (
                    _row_datetime(row["published_at"])
                    if row.get("published_at") is not None
                    else _row_datetime(row["known_at"])
                ),
                _row_datetime(row["known_at"]), int(row.get("revision_no") or 0),
            )
        )
        selected = candidates[-1]
        payload = _parse_payload(selected)
        exact_publication = bool(
            selected.get("publication_time_status") == TIME_VERIFIED
            and selected.get("published_at") is not None
        )
        published_text = (
            _dt_text(_row_datetime(selected["published_at"]))
            if exact_publication
            else str(selected.get("source_published_text") or "")
        )
        known_text = _dt_text(_row_datetime(selected["known_at"]))
        payload.update(
            {
                "finance_report_date": newest_report.isoformat(),
                "finance_published_at": published_text,
                "finance_known_at": known_text,
                "finance_publication_precision": (
                    "EXACT_TIME" if exact_publication else "DATE_ONLY"
                ),
                "finance_publication_time_verified": exact_publication,
                "finance_observed_available_at": known_text,
                "finance_fact_cutoff_at": _dt_text(fact_cutoff),
                "finance_decision_at": _dt_text(decision),
                "finance_publication_reason": (
                    "" if exact_publication
                    else "DATE_ONLY_USABLE_FROM_RECEIVED_AT"
                ),
                # Compatibility names consumed by the frozen production V6
                # PIT advisory.  They carry the same exact timestamps; no
                # date-only value is manufactured here.
                "finance_notice_date": published_text,
                "finance_knowledge_at": known_text,
                "finance_revision_id": str(selected["revision_id"]),
                "finance_content_hash": str(selected["content_hash"]),
                "finance_source": str(selected.get("source") or ""),
                "finance_pit_verified": True,
            }
        )
        facts[code] = payload
        statuses[code] = PIT_AVAILABLE
        reasons[code] = ""
    if revision_errors:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason="PIT_FINANCE_SCHEMA_UNAVAILABLE:" + revision_errors[0],
        )
    manifest = {
        "schema": "probiga.pit-finance-selection.v1",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "decision_at": _dt_text(decision),
        "as_of_date": end_date.isoformat(),
        "facts": {
            code: {
                "status": statuses.get(code),
                "reason": reasons.get(code),
                "revision_id": (facts.get(code) or {}).get("finance_revision_id"),
                "content_hash": (facts.get(code) or {}).get("finance_content_hash"),
                "coverage": resolved_coverage.get(code),
            }
            for code in normalized_codes
        },
    }
    return PITFactBatch(
        facts=facts, coverage_by_code=resolved_coverage,
        status_by_code=statuses, reason_by_code=reasons,
        manifest_hash=canonical_hash(manifest), decision_at=_dt_text(decision),
        fact_cutoff_at=_dt_text(fact_cutoff),
        table_name=FINANCE_REVISION_TABLE,
        global_status=PIT_AVAILABLE,
    )


def load_finance_history_facts(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None = None,
    as_of_date: date | str | None = None,
    limit_per_code: int = 20,
) -> PITFactBatch:
    """Return the immutable as-known finance prefix for each requested code.

    One revision is selected for every report identity as of ``decision_at``;
    reports sharing a report date are then resolved by publication time,
    knowledge time and revision number.  Any unverifiable identity blocks the
    complete finance feature for that code instead of silently returning a
    deceptively clean partial history.
    """

    normalized_codes = sorted(
        {
            str(code).strip().zfill(6)
            for code in codes
            if str(code).strip()
        }
    )
    fact_cutoff, decision = _fact_and_decision_times(
        decision_at=decision_at, fact_cutoff_at=fact_cutoff_at,
    )
    end_date = _date_value(as_of_date, required=False) or fact_cutoff.date()
    if not normalized_codes:
        return PITFactBatch(
            facts={},
            manifest_hash=canonical_hash([]),
            decision_at=_dt_text(decision),
            fact_cutoff_at=_dt_text(fact_cutoff),
            table_name=FINANCE_REVISION_TABLE,
        )
    try:
        rows = _query_revisions(
            engine,
            table_name=FINANCE_REVISION_TABLE,
            codes=normalized_codes,
            decision_at=decision,
            start_date=None,
            end_date=end_date,
        )
    except Exception as exc:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE,
            codes=normalized_codes,
            decision_at=decision,
            fact_cutoff_at=fact_cutoff,
            reason=f"PIT_FINANCE_SCHEMA_UNAVAILABLE:{type(exc).__name__}",
        )

    by_code_identity: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        by_code_identity[str(row.get("stock_code") or "").zfill(6)][
            str(row.get("identity_hash") or "")
        ].append(row)

    try:
        empty_coverage, coverage_errors = _authoritative_empty_coverage(
            engine,
            fact_kind="finance",
            codes=normalized_codes,
            decision_at=decision,
            start_date=date(1900, 1, 1),
            end_date=end_date,
        )
    except Exception as exc:
        empty_coverage = {}
        coverage_errors = {
            code: f"PIT_FINANCE_COVERAGE_SCHEMA_UNAVAILABLE:{type(exc).__name__}"
            for code in normalized_codes
        }

    required_report_date = finance_disclosure_gate(
        end_date
    ).minimum_report_date
    try:
        expected_unavailable, unavailable_errors = (
            load_finance_expected_unavailable(
                engine,
                codes=normalized_codes,
                decision_at=decision,
                expected_report_date=required_report_date,
            )
        )
    except Exception as exc:
        expected_unavailable = {}
        unavailable_errors = {
            code: (
                "PIT_FINANCE_UNAVAILABLE_SCHEMA_UNAVAILABLE:"
                f"{type(exc).__name__}"
            )
            for code in normalized_codes
        }
    coverage_errors.update(unavailable_errors)
    resolved_coverage = {**empty_coverage, **expected_unavailable}

    facts: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code in normalized_codes:
        identities = by_code_identity.get(code, {})
        if not identities:
            facts[code] = []
            if code in expected_unavailable:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            elif code in empty_coverage:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_AUTHORITATIVE_EMPTY"
            elif code in coverage_errors and "BAD_COVERAGE_CHAIN" in coverage_errors[code]:
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = coverage_errors[code]
            else:
                statuses[code] = PIT_NO_ROWS
                reasons[code] = "PIT_FINANCE_NO_ROWS"
            continue
        selected_by_report_date: dict[date, dict[str, Any]] = {}
        blocked_reason = ""
        try:
            for chain in identities.values():
                selected, selection_reason = _latest_revision_for_fact_cutoff(
                    chain,
                    fact_kind="finance",
                    fact_cutoff_at=fact_cutoff,
                    decision_at=decision,
                )
                if selected is None:
                    if selection_reason:
                        blocked_reason = selection_reason
                    continue
                report_date = _date_value(
                    selected.get("report_date"), required=True
                )
                previous = selected_by_report_date.get(report_date)
                selection_key = (
                    (
                        _row_datetime(selected["published_at"])
                        if selected.get("published_at") is not None
                        else _row_datetime(selected["known_at"])
                    ),
                    _row_datetime(selected["known_at"]),
                    int(selected.get("revision_no") or 0),
                    str(selected.get("revision_id") or ""),
                )
                if previous is None or selection_key > (
                    (
                        _row_datetime(previous["published_at"])
                        if previous.get("published_at") is not None
                        else _row_datetime(previous["known_at"])
                    ),
                    _row_datetime(previous["known_at"]),
                    int(previous.get("revision_no") or 0),
                    str(previous.get("revision_id") or ""),
                ):
                    selected_by_report_date[report_date] = selected
        except ValueError as exc:
            blocked_reason = f"PIT_FINANCE_BAD_CHAIN:{exc}"
        if blocked_reason:
            facts[code] = []
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = blocked_reason
            continue

        if (
            code in expected_unavailable
            and (
                not selected_by_report_date
                or max(selected_by_report_date) < required_report_date
            )
        ):
            facts[code] = []
            statuses[code] = PIT_AVAILABLE
            reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            continue

        selected_facts: list[dict[str, Any]] = []
        for report_date, selected in sorted(
            selected_by_report_date.items(), reverse=True
        )[: max(1, int(limit_per_code))]:
            payload = _parse_payload(selected)
            exact_publication = bool(
                selected.get("publication_time_status") == TIME_VERIFIED
                and selected.get("published_at") is not None
            )
            published_text = (
                _dt_text(_row_datetime(selected["published_at"]))
                if exact_publication
                else str(selected.get("source_published_text") or "")
            )
            known_text = _dt_text(_row_datetime(selected["known_at"]))
            payload.update(
                {
                    "finance_report_date": report_date.isoformat(),
                    "finance_published_at": published_text,
                    "finance_known_at": known_text,
                    "finance_publication_precision": (
                        "EXACT_TIME" if exact_publication else "DATE_ONLY"
                    ),
                    "finance_publication_time_verified": exact_publication,
                    "finance_observed_available_at": known_text,
                    "finance_fact_cutoff_at": _dt_text(fact_cutoff),
                    "finance_decision_at": _dt_text(decision),
                    "finance_publication_reason": (
                        "" if exact_publication
                        else "DATE_ONLY_USABLE_FROM_RECEIVED_AT"
                    ),
                    "finance_notice_date": published_text,
                    "finance_knowledge_at": known_text,
                    "finance_revision_id": str(selected["revision_id"]),
                    "finance_content_hash": str(selected["content_hash"]),
                    "finance_source": str(selected.get("source") or ""),
                    "finance_pit_verified": True,
                }
            )
            selected_facts.append(payload)
        facts[code] = selected_facts
        statuses[code] = PIT_AVAILABLE if selected_facts else PIT_NO_ROWS
        reasons[code] = "" if selected_facts else "PIT_FINANCE_NO_ROWS"

    manifest = {
        "schema": "probiga.pit-finance-history-selection.v1",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "decision_at": _dt_text(decision),
        "as_of_date": end_date.isoformat(),
        "facts": {
            code: {
                "status": statuses.get(code),
                "reason": reasons.get(code),
                "revision_ids": [
                    item.get("finance_revision_id")
                    for item in facts.get(code, [])
                ],
                "coverage": resolved_coverage.get(code),
            }
            for code in normalized_codes
        },
    }
    return PITFactBatch(
        facts=facts,
        coverage_by_code=resolved_coverage,
        status_by_code=statuses,
        reason_by_code=reasons,
        manifest_hash=canonical_hash(manifest),
        decision_at=_dt_text(decision),
        fact_cutoff_at=_dt_text(fact_cutoff),
        table_name=FINANCE_REVISION_TABLE,
        global_status=PIT_AVAILABLE,
    )


def load_event_facts(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    require_qmt_complete_batch: bool = False,
) -> PITFactBatch:
    normalized_codes = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    fact_cutoff, decision = _fact_and_decision_times(
        decision_at=decision_at, fact_cutoff_at=fact_cutoff_at,
    )
    start = _date_value(start_date, required=False)
    end = _date_value(end_date, required=False) or fact_cutoff.date()
    if not normalized_codes:
        return PITFactBatch(
            facts={}, manifest_hash=canonical_hash([]),
            decision_at=_dt_text(decision),
            fact_cutoff_at=_dt_text(fact_cutoff),
            table_name=EVENT_REVISION_TABLE,
        )
    event_batch: dict[str, Any] = {}
    if require_qmt_complete_batch:
        try:
            from server.common.qmt_announcement_pit import (
                validate_complete_announcement_batch,
            )

            event_batch = validate_complete_announcement_batch(
                engine,
                codes=normalized_codes,
                decision_at=decision,
                fact_cutoff_at=fact_cutoff,
                window_start=start or date(1900, 1, 1),
                window_end=end,
            )
        except Exception as exc:
            reason = str(getattr(exc, "reason_code", "") or "") or (
                f"PIT_QMT_EVENT_BATCH_UNAVAILABLE:{type(exc).__name__}"
            )
            return _blocked_batch(
                table_name=EVENT_REVISION_TABLE,
                codes=normalized_codes,
                decision_at=decision,
                fact_cutoff_at=fact_cutoff,
                reason=reason,
                status=PIT_DATA_BLOCKED,
            )
    try:
        rows = _query_revisions(
            engine, table_name=EVENT_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, start_date=start, end_date=end,
            source=(str(event_batch.get("source") or "") if event_batch else ""),
            batch_id=str(event_batch.get("batch_id") or ""),
        )
    except Exception as exc:
        return _blocked_batch(
            table_name=EVENT_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision,
            fact_cutoff_at=fact_cutoff,
            reason=f"PIT_EVENT_SCHEMA_UNAVAILABLE:{type(exc).__name__}",
        )

    by_code_identity: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        by_code_identity[str(row.get("stock_code") or "").zfill(6)][
            str(row.get("identity_hash") or "")
        ].append(row)
    coverage_start = start or date(1900, 1, 1)
    try:
        empty_coverage, coverage_errors = _authoritative_empty_coverage(
            engine,
            fact_kind="event",
            codes=normalized_codes,
            fact_cutoff_at=fact_cutoff,
            decision_at=decision,
            start_date=coverage_start,
            end_date=end,
            source=(str(event_batch.get("source") or "") if event_batch else ""),
            batch_id=str(event_batch.get("batch_id") or ""),
        )
    except Exception as exc:
        empty_coverage = {}
        coverage_errors = {
            code: f"PIT_EVENT_COVERAGE_SCHEMA_UNAVAILABLE:{type(exc).__name__}"
            for code in normalized_codes
        }
    facts: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code in normalized_codes:
        identities = by_code_identity.get(code, {})
        if not identities:
            facts[code] = []
            if code in empty_coverage:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_EVENT_AUTHORITATIVE_EMPTY"
            elif code in coverage_errors and "BAD_COVERAGE_CHAIN" in coverage_errors[code]:
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = coverage_errors[code]
            else:
                statuses[code] = PIT_NO_ROWS
                reasons[code] = "PIT_EVENT_NO_ROWS"
            continue
        selected_rows: list[dict[str, Any]] = []
        blocked_reasons: list[str] = []
        try:
            for chain in identities.values():
                selected, selection_reason = _latest_revision_for_fact_cutoff(
                    chain,
                    fact_kind="event",
                    fact_cutoff_at=fact_cutoff,
                    decision_at=decision,
                )
                if selected is None:
                    if selection_reason:
                        blocked_reasons.append(selection_reason)
                    continue
                payload = _parse_payload(selected)
                exact_publication = bool(
                    selected.get("publication_time_status") == TIME_VERIFIED
                    and selected.get("published_at") is not None
                )
                published_text = (
                    _dt_text(_row_datetime(selected["published_at"]))
                    if exact_publication
                    else str(selected.get("source_published_text") or "")
                )
                known_text = _dt_text(_row_datetime(selected["known_at"]))
                payload.update(
                    {
                        "event_published_at": published_text,
                        "event_known_at": known_text,
                        "event_publication_precision": (
                            "EXACT_TIME" if exact_publication else "DATE_ONLY"
                        ),
                        "event_publication_time_verified": exact_publication,
                        "event_observed_available_at": known_text,
                        "event_fact_cutoff_at": _dt_text(fact_cutoff),
                        "event_decision_at": _dt_text(decision),
                        "event_publication_reason": (
                            "" if exact_publication
                            else "DATE_ONLY_USABLE_FROM_RECEIVED_AT"
                        ),
                        "event_revision_id": str(selected["revision_id"]),
                        "event_content_hash": str(selected["content_hash"]),
                        "event_source": str(selected.get("source") or ""),
                        "event_pit_verified": True,
                    }
                )
                selected_rows.append(payload)
        except ValueError as exc:
            blocked_reasons = [f"PIT_EVENT_BAD_CHAIN:{exc}"]
        hard_block_reasons = [
            reason
            for reason in blocked_reasons
            if "PUBLISHED_AFTER_" not in reason
        ]
        if hard_block_reasons:
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = sorted(hard_block_reasons)[0]
            continue
        selected_rows.sort(
            key=lambda item: (
                str(item.get("event_published_at") or ""),
                str(item.get("event_revision_id") or ""),
            ),
            reverse=True,
        )
        facts[code] = selected_rows
        if selected_rows:
            statuses[code] = PIT_AVAILABLE
            reasons[code] = ""
        elif code in empty_coverage:
            statuses[code] = PIT_AVAILABLE
            reasons[code] = "PIT_EVENT_AUTHORITATIVE_EMPTY"
        elif blocked_reasons:
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = sorted(blocked_reasons)[0]
        else:
            statuses[code] = PIT_NO_ROWS
            reasons[code] = "PIT_EVENT_NO_ROWS_AT_FACT_CUTOFF"
    manifest = {
        "schema": "probiga.pit-event-selection.v1",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "decision_at": _dt_text(decision),
        "start_date": start.isoformat() if start else None,
        "end_date": end.isoformat(),
        "facts": {
            code: {
                "status": statuses.get(code),
                "reason": reasons.get(code),
                "revision_ids": [
                    item.get("event_revision_id")
                    for item in facts.get(code, [])
                ],
                "coverage": empty_coverage.get(code),
            }
            for code in normalized_codes
        },
    }
    if event_batch:
        manifest["qmt_event_batch"] = event_batch
    return PITFactBatch(
        facts=facts, coverage_by_code=empty_coverage,
        status_by_code=statuses, reason_by_code=reasons,
        manifest_hash=canonical_hash(manifest), decision_at=_dt_text(decision),
        fact_cutoff_at=_dt_text(fact_cutoff),
        table_name=EVENT_REVISION_TABLE,
        global_status=PIT_AVAILABLE,
    )


def _sqlite_table_ddl(table_name: str) -> str:
    if table_name == SOURCE_COVERAGE_TABLE:
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                coverage_id TEXT PRIMARY KEY,
                scope_hash TEXT NOT NULL,
                fact_kind TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                known_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                covered_through_at TEXT NOT NULL,
                watermark_kind TEXT NOT NULL,
                watermark_hash TEXT NOT NULL,
                coverage_status TEXT NOT NULL,
                result_count INTEGER NOT NULL,
                source_response_hash TEXT NOT NULL,
                fact_set_hash TEXT NOT NULL,
                revision_no INTEGER NOT NULL,
                supersedes_coverage_id TEXT NULL,
                source TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                coverage_fingerprint_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scope_hash, revision_no),
                UNIQUE (scope_hash, coverage_fingerprint_hash)
            )
        """
    if table_name == FINANCE_REVISION_TABLE:
        extra = "report_date TEXT NOT NULL, report_type TEXT NOT NULL DEFAULT ''"
    else:
        extra = "event_key TEXT NOT NULL, event_date TEXT NULL"
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            revision_id TEXT PRIMARY KEY,
            identity_hash TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            {extra},
            published_at TEXT NULL,
            source_published_text TEXT NOT NULL DEFAULT '',
            publication_time_status TEXT NOT NULL,
            known_at TEXT NOT NULL,
            received_at TEXT NOT NULL,
            revision_no INTEGER NOT NULL,
            supersedes_revision_id TEXT NULL,
            source TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            revision_fingerprint_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (identity_hash, revision_no),
            UNIQUE (identity_hash, revision_fingerprint_hash)
        )
    """


def _sqlite_trigger_statement(name: str, mysql_statement: str) -> str:
    event = "UPDATE" if name.endswith("_bu") else "DELETE"
    if "finance" in name:
        table_name = FINANCE_REVISION_TABLE
    elif "event" in name:
        table_name = EVENT_REVISION_TABLE
    else:
        table_name = SOURCE_COVERAGE_TABLE
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name} BEFORE {event} ON {table_name} "
        "BEGIN SELECT RAISE(ABORT, 'PIT revisions are append-only'); END"
    )


def pit_fact_schema_health(engine: Engine | Connection) -> dict[str, Any]:
    connection, close = _connection(engine)
    try:
        inspector = inspect(connection)
        observed_tables = set(inspector.get_table_names())
        missing_tables = sorted(PIT_FACT_TABLE_NAMES - observed_tables)
        missing_columns: dict[str, list[str]] = {}
        for table_name in sorted(PIT_FACT_TABLE_NAMES & observed_tables):
            observed = {str(item["name"]) for item in inspector.get_columns(table_name)}
            missing = sorted(_REQUIRED_COLUMNS[table_name] - observed)
            if missing:
                missing_columns[table_name] = missing
        if connection.dialect.name == "sqlite":
            trigger_rows = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='trigger'")
            ).fetchall()
            observed_triggers = {str(row[0]) for row in trigger_rows}
        else:
            trigger_rows = connection.execute(
                text(
                    "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA=DATABASE()"
                )
            ).fetchall()
            observed_triggers = {str(row[0]) for row in trigger_rows}
        missing_triggers = sorted(
            set(PIT_FACT_TRIGGER_STATEMENTS) - observed_triggers
        )
        valid = not missing_tables and not missing_columns and not missing_triggers
        return {
            "schema": "probiga.pit-fact-schema-health.v1",
            "status": "HEALTHY" if valid else "NOT_READY",
            "valid": valid,
            "table_count": len(PIT_FACT_TABLE_NAMES & observed_tables),
            "trigger_count": len(set(PIT_FACT_TRIGGER_STATEMENTS) & observed_triggers),
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
            "missing_triggers": missing_triggers,
            "contract_hash": canonical_hash(
                {
                    "tables": PIT_FACT_TABLE_DDLS,
                    "triggers": PIT_FACT_TRIGGER_STATEMENTS,
                    "required_columns": {
                        key: sorted(value) for key, value in _REQUIRED_COLUMNS.items()
                    },
                }
            ),
        }
    finally:
        if close:
            connection.close()


def ensure_pit_fact_schema(
    engine: Engine,
    *,
    trigger_ddl_executor: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    dialect = engine.dialect.name
    with engine.begin() as connection:
        for table_name in sorted(PIT_FACT_TABLE_NAMES):
            ddl = (
                _sqlite_table_ddl(table_name)
                if dialect == "sqlite"
                else PIT_FACT_TABLE_DDLS[table_name]
            )
            connection.execute(text(ddl))
    health = pit_fact_schema_health(engine)
    missing = list(health["missing_triggers"])
    for name in missing:
        statement = PIT_FACT_TRIGGER_STATEMENTS[name]
        if dialect == "sqlite":
            with engine.begin() as connection:
                connection.execute(text(_sqlite_trigger_statement(name, statement)))
        elif trigger_ddl_executor is not None:
            trigger_ddl_executor(statement)
        else:
            with engine.begin() as connection:
                connection.execute(text(statement))
    health = pit_fact_schema_health(engine)
    if not health["valid"]:
        raise RuntimeError(f"PIT fact schema is not healthy: {health}")
    return health


def preflight_pit_fact_schema(engine: Engine | Connection) -> dict[str, Any]:
    """Allow a wholly absent schema, but reject partial/drifted installations."""

    health = pit_fact_schema_health(engine)
    if health["valid"]:
        return health
    if set(health["missing_tables"]) == set(PIT_FACT_TABLE_NAMES):
        return {**health, "status": "WOULD_CREATE"}
    raise RuntimeError(f"partial PIT fact schema requires fenced repair: {health}")


__all__ = [
    "CNINFO_FINANCE_NONFILING_SOURCE",
    "EVENT_REVISION_TABLE", "FINANCE_REVISION_TABLE", "SOURCE_COVERAGE_TABLE",
    "FINANCE_ATOMIC_BATCH_CODE", "FINANCE_ATOMIC_BATCH_HISTORY_LIMIT",
    "FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA",
    "FINANCE_ATOMIC_BATCH_SCHEMA", "FINANCE_ATOMIC_BATCH_SOURCE",
    "FINANCE_INCREMENTAL_DISCOVERY_CODE",
    "FINANCE_INCREMENTAL_DISCOVERY_SCHEMA",
    "FINANCE_INCREMENTAL_DISCOVERY_SOURCE",
    "FINANCE_EXPECTED_UNAVAILABLE_STATUS",
    "PIT_AVAILABLE",
    "PIT_DATA_BLOCKED", "PIT_FACT_TABLE_DDLS", "PIT_FACT_TABLE_NAMES",
    "PIT_FACT_TRIGGER_STATEMENTS", "PIT_NO_ROWS", "PIT_SCHEMA_UNAVAILABLE",
    "PITCoverageReceipt", "PITFactBatch", "PITRevisionReceipt",
    "TIME_UNVERIFIED", "TIME_VERIFIED", "append_event_revision",
    "append_finance_atomic_batch_seal", "append_finance_expected_unavailable",
    "append_finance_revision",
    "append_source_coverage", "canonical_hash",
    "canonical_json", "ensure_pit_fact_schema", "load_event_facts",
    "load_finance_atomic_batch_seal", "load_finance_expected_unavailable",
    "load_finance_incremental_discovery",
    "load_latest_finance_atomic_batch_baseline",
    "load_finance_facts",
    "load_finance_history_facts",
    "normalize_decision_at", "normalize_published_at",
    "pit_fact_schema_health", "preflight_pit_fact_schema",
    "resolve_common_fact_cutoff",
]
