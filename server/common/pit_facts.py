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
from itertools import groupby
import json
import logging
import math
from pathlib import Path
import re
from time import monotonic
from types import MappingProxyType
import zlib
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

from server.common.finance_coverage import (
    finance_disclosure_gate,
    report_period_gate_applies,
)
from server.common.finance_nonfiling_evidence import (
    EVIDENCE_SCHEMA as FINANCE_NONFILING_BODY_SCHEMA,
    validate_document_body_evidence,
)
from server.common.pit_event_coverage_checkpoint import EventCoverageCheckpoint

if TYPE_CHECKING:
    from server.common.daily_delivery_control import FrozenInitialAnnouncement, FrozenPremarketAnnouncement
    from server.common.qmt_announcement_pit import HistoricalAnnouncementSelection

logger = logging.getLogger(__name__)


SHANGHAI = ZoneInfo("Asia/Shanghai")
FINANCE_REVISION_TABLE = "st_pit_finance_revision"
EVENT_REVISION_TABLE = "st_pit_event_revision"
SOURCE_COVERAGE_TABLE = "st_pit_source_coverage"
FINANCE_EXPECTED_UNAVAILABLE_STATUS = "EXPECTED_UNAVAILABLE"
FINANCE_DATA_EXCLUDED_STATUS = "DATA_EXCLUDED"
CNINFO_FINANCE_NONFILING_SOURCE = "cninfo.finance.nonfiling"
FINANCE_ATOMIC_BATCH_SOURCE = "probiga.finance.atomic_batch"
FINANCE_ATOMIC_BATCH_CODE = "000000"
FINANCE_ATOMIC_BATCH_SCHEMA = "probiga.pit-finance-atomic-batch.v1"
FINANCE_ATOMIC_BATCH_INCREMENTAL_SCHEMA = (
    "probiga.pit-finance-atomic-batch.v2"
)
FINANCE_ATOMIC_BATCH_HISTORY_LIMIT = 20
FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT = 100
COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT = 500
FINANCE_REVISION_MYSQL_CODE_LIMIT = 40
FINANCE_REVISION_MYSQL_PAYLOAD_ROW_LIMIT = 480
FINANCE_REVISION_MYSQL_DEADLINE_SECONDS = 30 * 60
LIVE_EVENT_COVERAGE_READ_DEADLINE_SECONDS = 60 * 60
EMPTY_COVERAGE_MYSQL_CODE_LIMIT = 40
FINANCE_NONFILING_MYSQL_CODE_LIMIT = 40
FINANCE_NONFILING_MYSQL_QUERY_TIMEOUT_MS = 60_000
FINANCE_NONFILING_MYSQL_DEADLINE_SECONDS = 30 * 60
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
    "QMT_ANNOUNCEMENT_API_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT",
    "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED",
})
EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR = "EXPOSURE_MONITOR"
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
_REVISION_METADATA_COLUMNS = (
    "revision_id", "content_hash", "revision_no", "supersedes_revision_id",
    "publication_time_status", "published_at", "source_published_text",
    "revision_fingerprint_hash",
)
_PIT_WRITE_BATCH_ROWS = 100
_PIT_WRITE_BATCH_BYTES = 480_000


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


@dataclass(frozen=True)
class SealedFinanceInputs:
    engine: Engine
    decision_at: datetime
    fact_cutoff_at: datetime
    as_of_date: date
    seal_id: str
    batch_root_sha256: str
    members: Mapping[str, Mapping[str, Any]]
    exclusions: Mapping[str, Mapping[str, Any]]


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


def _cninfo_cutoff_partition_valid(
    receipt: Mapping[str, Any], *, requested_end_time: str,
    raw_count: int, retained_count: int,
) -> bool:
    """Prove the exact-time partition of one exhaustive CNINFO date query."""
    if "excluded_after_cutoff" not in receipt:
        return raw_count == retained_count
    excluded = receipt["excluded_after_cutoff"]
    if (
        not isinstance(excluded, list)
        or not excluded
        or retained_count < 0
        or raw_count != retained_count + len(excluded)
        or raw_count > CNINFO_MAX_PAGES_PER_STOCK * 30
        or re.fullmatch(r"[0-9]{14}", requested_end_time) is None
        or requested_end_time.endswith("235959")
    ):
        return False
    try:
        cutoff = datetime.strptime(requested_end_time, "%Y%m%d%H%M%S")
    except ValueError:
        return False
    seen: set[str] = set()
    for row in excluded:
        if not isinstance(row, Mapping) or set(row) != {
            "source_event_id", "published_at", "source_row_hash",
        }:
            return False
        event_id, published_text, raw_hash = (
            row["source_event_id"], row["published_at"], row["source_row_hash"],
        )
        if (
            not isinstance(event_id, str) or not 1 <= len(event_id) <= 512
            or event_id != event_id.strip() or event_id in seen
            or not isinstance(raw_hash, str) or not _HASH_RE.fullmatch(raw_hash)
            or not isinstance(published_text, str)
        ):
            return False
        try:
            published_at = datetime.fromisoformat(published_text)
        except ValueError:
            return False
        if (
            published_at.tzinfo is not None
            or published_at.isoformat(sep=" ", timespec="microseconds") != published_text
            or published_at.microsecond % 1000
            or published_at.date() != cutoff.date()
            or published_at <= cutoff
        ):
            return False
        seen.add(event_id)
    return True


def _cninfo_shard_manifest_details(
    manifest: Any, *, stock_code: str, requested_start_time: str,
    requested_end_time: str, max_pages: int,
) -> tuple[int, int, list[str], str] | None:
    """Validate an exhausted, contiguous raw date tree and derive its audit totals."""
    if not isinstance(manifest, list) or not 1 <= len(manifest) <= max_pages:
        return None
    try:
        start_date = datetime.strptime(requested_start_time[:8], "%Y%m%d").date()
        end_date = datetime.strptime(requested_end_time[:8], "%Y%m%d").date()
    except (TypeError, ValueError):
        return None
    cursor = start_date.toordinal()
    total = 0
    query_count = 2 * len(manifest) - 1
    page_hashes: list[str] = []
    for leaf in manifest:
        if not isinstance(leaf, Mapping) or set(leaf) != {
            "window_start", "window_end", "capture_mode", "result_count",
            "provider_reported_totalpages", "provider_page_count",
            "page_sha256", "last_page_has_more",
        }:
            return None
        try:
            leaf_start = date.fromisoformat(leaf["window_start"])
            leaf_end = date.fromisoformat(leaf["window_end"])
        except (TypeError, ValueError):
            return None
        count = leaf["result_count"]
        pages = leaf["provider_page_count"]
        reported_pages = leaf["provider_reported_totalpages"]
        page_hash = leaf["page_sha256"]
        if (
            leaf["window_start"] != leaf_start.isoformat()
            or leaf["window_end"] != leaf_end.isoformat()
            or leaf_start.toordinal() != cursor
            or not leaf_start <= leaf_end <= end_date
            or type(count) is not int or count < 0
            or type(reported_pages) is not int or reported_pages != count // 30
            or type(pages) is not int or pages != max(1, (count + 29) // 30)
            or pages > max_pages
            or leaf["last_page_has_more"] is not False
            or leaf["capture_mode"] != ("SINGLE_PAGE" if pages == 1 else "DENSE_DAY_COMPLETE_SWEEP")
            or (pages > 1 and leaf_start != leaf_end)
            or not isinstance(page_hash, str) or not _HASH_RE.fullmatch(page_hash)
        ):
            return None
        cursor = leaf_end.toordinal() + 1
        total += count
        query_count += pages - 1
        page_hashes.append(page_hash)
    if cursor != end_date.toordinal() + 1:
        return None
    manifest_hash = canonical_hash({
        "schema": "probiga.cninfo-date-shard-manifest.v1",
        "stock_code": stock_code,
        "requested_start_time": requested_start_time,
        "requested_end_time": requested_end_time,
        "shards": manifest,
    })
    return total, query_count, page_hashes, manifest_hash


def _cninfo_shard_rounds_valid(
    receipt: Mapping[str, Any], *, round_count: int, query_count: int,
    final_tree_queries: int, retained_count: int, raw_count: int, max_pages: int,
) -> bool:
    raw_hashes = receipt.get("pagination_complete_round_sha256")
    if (
        round_count < 2
        or not isinstance(raw_hashes, list)
        or len(raw_hashes) != round_count
        or not all(isinstance(value, str) and _HASH_RE.fullmatch(value) for value in raw_hashes)
        or query_count > max_pages
    ):
        return False
    if "pagination_frozen_proof" not in receipt:
        # These immutable receipts attested an identical full raw result set.
        return (
            raw_hashes[-1] == raw_hashes[-2]
            and receipt.get("date_shard_recheck_sha256") == receipt.get("date_shard_manifest_sha256")
            and receipt.get("quality_status") == "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
            and 2 * final_tree_queries <= query_count
        )

    proof = receipt["pagination_frozen_proof"]
    if (
        not isinstance(proof, Mapping)
        or set(proof) != {"schema", "complete_rounds"}
        or proof.get("schema") != "probiga.cninfo-frozen-pagination.v1"
        or receipt.get("quality_status") != "EXACT_STOCK_DATE_SHARDS_FROZEN_CUTOFF_DOUBLE_ATTESTED"
        or receipt.get("data_version") != canonical_hash("probiga.cninfo-per-stock-announcement.v2")
    ):
        return False
    rounds = proof["complete_rounds"]
    if not isinstance(rounds, list) or len(rounds) != round_count:
        return False

    start = receipt["requested_start_time"]
    end = receipt["requested_end_time"]
    complete_queries = 0
    for sweep in rounds:
        if not isinstance(sweep, Mapping) or set(sweep) != {
            "raw_manifest", "raw_manifest_sha256", "raw_count",
            "query_count", "frozen_sha256", "retained_count",
        }:
            return False
        manifest = sweep["raw_manifest"]
        total, frozen_count, queries, frozen_hash = (
            sweep["raw_count"], sweep["retained_count"],
            sweep["query_count"], sweep["frozen_sha256"],
        )
        if (
            any(type(value) is not int for value in (total, frozen_count, queries))
            or not 0 <= frozen_count <= total <= max_pages * 30
            or not 1 <= queries <= max_pages
            or (end.endswith("235959") and frozen_count != total)
            or not isinstance(frozen_hash, str) or not _HASH_RE.fullmatch(frozen_hash)
        ):
            return False
        details = _cninfo_shard_manifest_details(
            manifest, stock_code=receipt["stock_code"], requested_start_time=start,
            requested_end_time=end, max_pages=max_pages,
        )
        if details is None or (details[0], details[1], details[3]) != (
            total, queries, sweep["raw_manifest_sha256"],
        ):
            return False
        complete_queries += queries
    prior, final = rounds[-2:]
    invalid_round_count = receipt["pagination_invalid_round_count"]
    return (
        prior["frozen_sha256"] == final["frozen_sha256"]
        and prior["retained_count"] == final["retained_count"] == retained_count
        and prior["raw_manifest_sha256"] == receipt.get("date_shard_recheck_sha256")
        and final["raw_manifest_sha256"] == receipt.get("date_shard_manifest_sha256")
        and final["raw_count"] == raw_count
        and final["query_count"] == final_tree_queries
        and complete_queries + invalid_round_count <= query_count
        and (invalid_round_count > 0 or complete_queries == query_count)
    )


def _fallback_announcement_delta_parts(
    evidence: Mapping[str, Any], source_rows: Iterable[Mapping[str, Any]],
    *, stock_code: str,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Validate the exact source window and split a merged fallback response."""
    chain = evidence.get("incremental_proof")
    if not isinstance(chain, Mapping) or chain.get("mode") != "DELTA_OVER_PARENT":
        raise ValueError("fallback announcement delta proof is missing")
    coverage_start = date.fromisoformat(chain["coverage_window_start"])
    capture_start = date.fromisoformat(chain["capture_window_start"])
    cutoff = normalize_decision_at(evidence["fact_cutoff_at"])
    if (
        chain.get("schema") != "probiga.qmt-announcement-incremental-chain.v1"
        or coverage_start.isoformat() != evidence.get("window_start")
        or capture_start.isoformat() != chain.get("capture_window_start")
        or cutoff.date().isoformat() != chain.get("window_end")
        or chain.get("window_end") != evidence.get("window_end")
        or cutoff.strftime("%Y%m%d%H%M%S") != evidence.get("query_end_time")
        or not coverage_start < capture_start <= cutoff.date()
        or not chain.get("parent_batch_id")
        or not all(_HASH_RE.fullmatch(str(chain.get(field) or "")) for field in (
            "parent_batch_root_hash", "delta_root_hash", "prefix_root_hash",
        ))
        or normalize_decision_at(chain["parent_received_at"]) >= cutoff
        or chain.get("result_batch_root_hash") != evidence.get("global_batch_root_hash")
        or canonical_hash({key: value for key, value in chain.items() if key != "chain_root_hash"})
        != chain.get("chain_root_hash")
    ):
        raise ValueError("fallback announcement delta evidence differs")
    start_at = datetime.combine(coverage_start, datetime.min.time())
    capture_at = datetime.combine(capture_start, datetime.min.time())
    prefix: list[Mapping[str, Any]] = []
    delta: list[Mapping[str, Any]] = []
    seen = set()
    for row in source_rows:
        if not isinstance(row, Mapping) or row.get("stock_code") != stock_code:
            raise ValueError("fallback announcement delta stock differs")
        event_key = row.get("event_key")
        raw_published_at = row.get("published_at")
        published_at, _, _ = normalize_published_at(raw_published_at)
        if (
            not isinstance(event_key, str) or not event_key or event_key in seen
            or published_at is None or raw_published_at != _dt_text(published_at)
            or published_at.date().isoformat() != row.get("event_date")
            or not start_at <= published_at <= cutoff
        ):
            raise ValueError("fallback announcement delta publication differs")
        seen.add(event_key)
        (prefix if published_at < capture_at else delta).append(row)
    return prefix, delta


def _fallback_event_receipt_valid(
    evidence: Mapping[str, Any],
    *,
    source: str,
    stock_code: str,
    result_count: int,
    source_rows: list[Mapping[str, Any]] | None = None,
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
    requested_start_date = str(evidence.get("window_start") or "")
    chain = evidence.get("incremental_proof")
    if isinstance(chain, Mapping) and chain.get("mode") == "DELTA_OVER_PARENT":
        if source_rows is None or len(source_rows) != result_count:
            return False
        try:
            _, delta = _fallback_announcement_delta_parts(evidence, source_rows, stock_code=stock_code)
        except (KeyError, TypeError, ValueError):
            return False
        requested_start_date = chain["capture_window_start"]
        result_count = len(delta)
    requested_start = requested_start_date.replace("-", "") + "000000"
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
    if not _cninfo_cutoff_partition_valid(
        receipt, requested_end_time=str(evidence.get("query_end_time") or ""),
        raw_count=total, retained_count=result_count,
    ):
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
    if "excluded_after_cutoff" in receipt:
        pagination_payload["excluded_after_cutoff"] = receipt["excluded_after_cutoff"]
    if "pagination_frozen_proof" in receipt:
        pagination_payload["pagination_frozen_proof"] = receipt["pagination_frozen_proof"]
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
        expected_pages == page_count
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
        and "pagination_frozen_proof" not in receipt
    )
    shard_proof_valid = False
    if shard_mode:
        details = _cninfo_shard_manifest_details(
            date_shard_manifest, stock_code=stock_code,
            requested_start_time=requested_start,
            requested_end_time=str(evidence.get("query_end_time") or ""),
            max_pages=max_pages,
        )
        if details is not None:
            shard_total, tree_query_count, shard_hashes, expected_shard_hash = details
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
                date_shard_count == len(shard_hashes)
                and page_count == date_shard_count
                and expected_pages == date_shard_count
                and page_hashes == shard_hashes
                and shard_total == total
                and date_shard_manifest_hash == expected_shard_hash
                and split_version == CNINFO_DATE_SHARD_SPLIT_VERSION
                and 2 <= pagination_round_count
                <= CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS
                and 2 <= pagination_attempt_count
                <= CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS
                and 0 <= pagination_invalid_round_count
                < pagination_attempt_count
                and pagination_attempt_count
                == pagination_round_count + pagination_invalid_round_count
                and round_attempts_valid
                and _cninfo_shard_rounds_valid(
                    receipt, round_count=pagination_round_count, query_count=pagination_query_count,
                    final_tree_queries=tree_query_count, retained_count=result_count,
                    raw_count=total, max_pages=max_pages,
                )
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
    """Bound ambiguous date-only facts independently of job runtime."""

    if known_at > decision_at:
        return False
    if known_at <= fact_cutoff_at:
        return True
    return (
        timedelta(0) <= known_at - fact_cutoff_at <= MAX_LIVE_CAPTURE_DELAY
    )


def _verified_cninfo_coverage_known(
    row: Mapping[str, Any],
    batch: Mapping[str, Any] | None,
    *,
    fact_cutoff_at: datetime,
    decision_at: datetime,
    completion_known_at: datetime | None,
) -> bool:
    """Use exact CNInfo evidence only after its actual immutable receipt."""
    if (
        not batch
        or completion_known_at is None
        or batch.get("status") != "COMPLETE"
        or batch.get("source") != CNINFO_EVENT_SOURCE
        or row.get("source") != CNINFO_EVENT_SOURCE
        or row.get("fact_kind") != "event"
        or row.get("watermark_kind") != "QUERY_CUTOFF"
        or row.get("coverage_status") != "COMPLETE"
        or not batch.get("batch_id")
        or row.get("batch_id") != batch["batch_id"]
        or not _HASH_RE.fullmatch(str(batch.get("batch_root_hash") or ""))
    ):
        return False
    known = _row_datetime(row.get("known_at"))
    received = _row_datetime(row.get("received_at"))
    return (
        known == received == _row_datetime(batch.get("received_at"))
        and fact_cutoff_at <= _row_datetime(batch.get("fact_cutoff_at"))
        == _row_datetime(row.get("covered_through_at"))
        <= known <= completion_known_at <= decision_at
    )


def _late_cninfo_completion(
    engine: Engine, batch: Mapping[str, Any], *,
    fact_cutoff_at: datetime, decision_at: datetime,
) -> tuple[bool, datetime | None]:
    if (
        not batch
        or batch.get("source") != CNINFO_EVENT_SOURCE
        or batch.get("schema") != "probiga.qmt-announcement-batch.v1"
    ):
        return False, None
    if _live_capture_allowed(
        fact_cutoff_at=fact_cutoff_at,
        decision_at=decision_at,
        known_at=_row_datetime(batch.get("received_at")),
    ):
        return False, None
    from server.common.daily_delivery_control import load_completed_cninfo_announcement_at

    try:
        return True, load_completed_cninfo_announcement_at(engine, batch=batch, decision_at=decision_at)
    except (ValueError, TypeError, RuntimeError, SQLAlchemyError):
        return True, None


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


def _latest_finance_revisions(
    connection: Connection,
    identity_hashes: Iterable[str],
) -> dict[str, dict[str, Any] | None]:
    return _latest_revisions(connection, FINANCE_REVISION_TABLE, identity_hashes)


def _latest_revisions(
    connection: Connection, table_name: str, identity_hashes: Iterable[str],
) -> dict[str, dict[str, Any] | None]:
    identities = sorted(set(identity_hashes))
    latest: dict[str, dict[str, Any] | None] = dict.fromkeys(identities)
    mysql = connection.dialect.name == "mysql"
    # Search the identity prefix itself, including absent identities.  On MySQL
    # this retains the single-row writer's next-key/gap locking under the same
    # transaction isolation, rather than locking only IDs found by a subquery.
    index_name = {
        FINANCE_REVISION_TABLE: "uk_pit_finance_identity_revision",
        EVENT_REVISION_TABLE: "uk_pit_event_identity_revision",
    }[table_name]
    index_hint = f" FORCE INDEX ({index_name})" if mysql else ""
    lock = " FOR UPDATE" if mysql else ""
    statement = text(
        f"SELECT identity_hash, {', '.join(_REVISION_METADATA_COLUMNS)} "
        f"FROM {table_name}{index_hint} "
        "WHERE identity_hash IN :identity_hashes "
        f"ORDER BY identity_hash, revision_no DESC{lock}"
    ).bindparams(bindparam("identity_hashes", expanding=True))
    for offset in range(0, len(identities), _PIT_WRITE_BATCH_ROWS):
        rows = connection.execute(
            statement, {"identity_hashes": identities[offset:offset + _PIT_WRITE_BATCH_ROWS]}
        ).mappings()
        for row in rows:
            identity_hash = str(row["identity_hash"])
            if latest[identity_hash] is None:
                latest[identity_hash] = dict(row)
    return latest


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


def _existing_scoped_fingerprints(
    connection: Connection,
    *,
    table_name: str,
    scope_column: str,
    fingerprint_column: str,
    columns: str,
    fingerprints: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    if not fingerprints:
        return {}
    predicates = []
    parameters = {}
    for index, (scope, fingerprint) in enumerate(fingerprints.items()):
        predicates.append(
            f"({scope_column}=:scope{index} AND {fingerprint_column}=:fingerprint{index})"
        )
        parameters[f"scope{index}"] = scope
        parameters[f"fingerprint{index}"] = fingerprint
    rows = connection.execute(
        text(f"SELECT {columns} FROM {table_name} WHERE " + " OR ".join(predicates)),
        parameters,
    ).mappings()
    return {str(row[scope_column]): dict(row) for row in rows}


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
    preloaded_latest: dict[str, dict[str, Any] | None] | None = None,
) -> PITRevisionReceipt:
    identity_hash = canonical_hash(
        {"schema": f"probiga.pit-{fact_kind}-identity.v1", **identity_payload}
    )
    latest = (
        _latest_revision(connection, table_name, identity_hash)
        if preloaded_latest is None else preloaded_latest[identity_hash]
    )
    values, planned_receipt = _plan_revision(
        fact_kind=fact_kind, identity_hash=identity_hash, latest=latest,
        row_columns=row_columns, payload=payload, published_value=published_value,
        known_at=known_at, received_at=received_at, source=source, batch_id=batch_id,
    )
    if values is None:
        return planned_receipt
    fingerprint = values["revision_fingerprint_hash"]
    existing = _existing_fingerprint(connection, table_name, identity_hash, fingerprint)
    if existing is None:
        try:
            _insert_pit_rows(connection, table_name, [values])
        except IntegrityError:
            existing = _existing_fingerprint(
                connection, table_name, identity_hash, fingerprint
            )
            if existing is None:
                raise
    if existing is not None:
        if preloaded_latest is not None and (
            int(existing.get("revision_no") or 0)
            > int((latest or {}).get("revision_no") or 0)
        ):
            preloaded_latest[identity_hash] = {
                **existing, "published_at": values["published_at"],
                "source_published_text": values["source_published_text"],
            }
        return _receipt(existing, idempotent=True)
    if preloaded_latest is not None:
        preloaded_latest[identity_hash] = {
            column: values[column] for column in _REVISION_METADATA_COLUMNS
        }
    return planned_receipt


def _plan_revision(
    *,
    fact_kind: str,
    identity_hash: str,
    latest: Mapping[str, Any] | None,
    row_columns: Mapping[str, Any],
    payload: Mapping[str, Any],
    published_value: Any,
    known_at: datetime | str,
    received_at: datetime | str | None,
    source: str,
    batch_id: str,
) -> tuple[dict[str, Any] | None, PITRevisionReceipt]:
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
        return None, _receipt(latest, idempotent=True)
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
    return values, _receipt(values, idempotent=False)


def _insert_pit_rows(
    connection: Connection, table_name: str, rows: list[dict[str, Any]],
) -> None:
    columns = list(rows[0])
    statement = text(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
        f"({', '.join(':' + column for column in columns)})"
    )
    connection.execute(statement, rows if len(rows) > 1 else rows[0])


def _finance_revision_arguments(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    code = str(row.get("stock_code") or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("finance PIT revision requires a six-digit stock code")
    report_date = _date_value(row.get("report_date"), required=True)
    report_type = str(row.get("report_type") or "").strip()[:64]
    return {
        "table_name": FINANCE_REVISION_TABLE,
        "fact_kind": "finance",
        "identity_payload": {
            "stock_code": code,
            "report_date": report_date.isoformat(),
            "report_type": report_type,
        },
        "row_columns": {
            "stock_code": code,
            "report_date": report_date,
            "report_type": report_type,
        },
        "payload": _source_payload(row),
        "published_value": row.get("published_at", row.get("notice_date")),
    }


def append_finance_revision(
    target: Engine | Connection,
    row: Mapping[str, Any],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str = "adata.finance.core_index",
    batch_id: str = "",
) -> PITRevisionReceipt:
    arguments = _finance_revision_arguments(row)
    connection, close = _connection(target)
    transaction = None
    if close:
        transaction = connection.begin()
    try:
        receipt = _append_revision(
            connection,
            **arguments,
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


def append_finance_revisions(
    target: Engine | Connection,
    rows: Iterable[Mapping[str, Any]],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str = "adata.finance.core_index",
    batch_id: str = "",
) -> list[PITRevisionReceipt]:
    """Append one issuer's reports atomically with bounded metadata reads."""

    arguments = [_finance_revision_arguments(row) for row in rows]
    if not arguments:
        return []
    if len({item["row_columns"]["stock_code"] for item in arguments}) != 1:
        raise ValueError("finance PIT revision batch requires one stock code")
    identities = [
        canonical_hash({
            "schema": "probiga.pit-finance-identity.v1",
            **item["identity_payload"],
        })
        for item in arguments
    ]
    connection, close = _connection(target)
    transaction = connection.begin() if close else None
    try:
        latest = _latest_finance_revisions(connection, identities)
        receipts = [
            _append_revision(
                connection,
                **item,
                known_at=known_at,
                received_at=received_at,
                source=source,
                batch_id=batch_id,
                preloaded_latest=latest,
            )
            for item in arguments
        ]
        if transaction is not None:
            transaction.commit()
        return receipts
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


def _event_revision_arguments(row: Mapping[str, Any], source: str) -> dict[str, Any]:
    code = str(row.get("stock_code") or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("event PIT revision requires a six-digit stock code")
    event_key = str(
        row.get("event_key") or row.get("art_code") or ""
    ).strip()[:160]
    if not event_key:
        raise ValueError("event PIT revision requires a stable source event key")
    published_value = row.get("published_at", row.get("display_time"))
    if published_value is None:
        # The canonical announcement parser deliberately leaves exact time
        # unknown for date-only disclosures. Preserve its actual source date
        # without promoting an unverified timestamp or using a business date.
        source_date = str(row.get("source_published_text") or "")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", source_date):
            published_value = source_date
    event_date = _date_value(
        row.get("event_date", row.get("notice_date")), required=False
    )
    if event_date is None:
        event_date = _date_value(published_value, required=False)
    return {
        "identity_payload": {
            "source": str(source or "UNKNOWN")[:64],
            "event_key": event_key,
            "stock_code": code,
        },
        "row_columns": {
            "event_key": event_key,
            "stock_code": code,
            "event_date": event_date,
        },
        "payload": _source_payload(row),
        "published_value": published_value,
    }


def append_event_revision(
    target: Engine | Connection,
    row: Mapping[str, Any],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str = "eastmoney.notice",
    batch_id: str = "",
) -> PITRevisionReceipt:
    arguments = _event_revision_arguments(row, source)
    connection, close = _connection(target)
    transaction = None
    if close:
        transaction = connection.begin()
    try:
        receipt = _append_revision(
            connection,
            table_name=EVENT_REVISION_TABLE,
            fact_kind="event",
            **arguments,
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


def append_event_revisions(
    connection: Connection,
    rows: Iterable[Mapping[str, Any]],
    *,
    known_at: datetime | str,
    received_at: datetime | str | None = None,
    source: str,
    batch_id: str,
) -> list[PITRevisionReceipt]:
    """Append distinct events within the caller's locked publication transaction."""
    if not connection.in_transaction():
        raise ValueError("event revision batch requires a publication transaction")
    receipts: list[PITRevisionReceipt] = []
    arguments: dict[str, dict[str, Any]] = {}
    identities: set[str] = set()
    batch_bytes = 0

    def write_chunk() -> None:
        latest = _latest_revisions(connection, EVENT_REVISION_TABLE, arguments)
        plans = {
            identity: _plan_revision(
                fact_kind="event", identity_hash=identity, latest=latest[identity],
                **item, known_at=known_at, received_at=received_at,
                source=source, batch_id=batch_id,
            )
            for identity, item in arguments.items()
        }
        existing = _existing_scoped_fingerprints(
            connection,
            table_name=EVENT_REVISION_TABLE,
            scope_column="identity_hash",
            fingerprint_column="revision_fingerprint_hash",
            columns="identity_hash, " + ", ".join(_REVISION_METADATA_COLUMNS),
            fingerprints={
                identity: values["revision_fingerprint_hash"]
                for identity, (values, _) in plans.items() if values is not None
            },
        )
        inserts = [
            values for identity, (values, _) in sorted(plans.items())
            if values is not None and identity not in existing
        ]
        if inserts:
            _insert_pit_rows(connection, EVENT_REVISION_TABLE, inserts)
        receipts.extend(
            _receipt(existing[identity], idempotent=True)
            if identity in existing else receipt
            for identity, (_, receipt) in plans.items()
        )

    for row in rows:
        item = _event_revision_arguments(row, source)
        identity = canonical_hash({
            "schema": "probiga.pit-event-identity.v1", **item.pop("identity_payload"),
        })
        if identity in identities:
            raise ValueError("event revision batch requires distinct identities")
        identities.add(identity)
        row_bytes = len(canonical_json(item["payload"]).encode("utf-8")) + 1_024
        if arguments and (
            len(arguments) == _PIT_WRITE_BATCH_ROWS
            or batch_bytes + row_bytes > _PIT_WRITE_BATCH_BYTES
        ):
            write_chunk()
            arguments.clear()
            batch_bytes = 0
        arguments[identity] = item
        batch_bytes += row_bytes
    if arguments:
        write_chunk()
    return receipts


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


def _source_coverage_scope_hash(fact_kind: str, stock_code: str, source: str) -> str:
    return canonical_hash({
        "schema": "probiga.pit-source-coverage-scope.v1",
        "fact_kind": fact_kind,
        "stock_code": stock_code,
        "source": source,
    })


def _prepare_source_coverage(
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
) -> tuple[dict[str, Any], dict[str, Any]]:

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
            timedelta(0) <= known - covered
        ):
            raise ValueError("query-cutoff knowledge clock precedes its source cutoff")
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
        capture_purpose = str(
            normalized_watermark_evidence.get("capture_purpose") or ""
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
            or (
                source_name == CNINFO_EVENT_SOURCE
                and capture_purpose == EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR
                and normalized_watermark_evidence.get("primary_provider")
                == CNINFO_EVENT_SOURCE
                and not str(
                    normalized_watermark_evidence.get("fallback_reason") or ""
                )
            )
        )
        fallback_receipt_valid = _fallback_event_receipt_valid(
            normalized_watermark_evidence,
            source=source_name,
            stock_code=code,
            result_count=len(normalized_rows),
            source_rows=normalized_rows,
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
    scope_hash = _source_coverage_scope_hash(kind, code, source_name)
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
    return payload, {
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
        "source": source_name,
        "batch_id": str(batch_id or "")[:128],
        "payload_json": canonical_json(payload),
        "created_at": known,
    }


_SOURCE_COVERAGE_RECEIPT_COLUMNS = (
    "coverage_id", "scope_hash", "source_response_hash", "fact_set_hash",
    "revision_no", "supersedes_coverage_id", "covered_through_at",
)


def _write_source_coverage_chunk(
    connection: Connection,
    prepared: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[PITCoverageReceipt]:
    by_scope = {row["scope_hash"]: row for _, row in prepared}
    if len(by_scope) != len(prepared):
        raise ValueError("coverage batch requires distinct scopes")
    latest: dict[str, dict[str, Any]] = {}
    mysql = connection.dialect.name == "mysql"
    lock = " FOR UPDATE" if mysql else ""
    columns = ", ".join(_SOURCE_COVERAGE_RECEIPT_COLUMNS)
    single = len(prepared) == 1
    scopes = sorted(by_scope)
    if mysql and not single:
        # Lock each scope's newest revision (or the missing prefix) without
        # scanning old revisions or sorting a mixed-direction index range.
        # Both ORDER BY columns must use the backward index scan.
        latest_query = text(
            f"SELECT latest.* FROM JSON_TABLE(:scope_json, '$[*]' "
            "COLUMNS (scope_hash CHAR(64) PATH '$')) AS requested "
            f"JOIN LATERAL (SELECT {columns} FROM {SOURCE_COVERAGE_TABLE} "
            "FORCE INDEX (uk_pit_coverage_scope_revision) "
            "WHERE scope_hash = requested.scope_hash COLLATE utf8mb4_bin "
            "ORDER BY scope_hash DESC, revision_no DESC LIMIT 1 FOR UPDATE"
            ") AS latest ON TRUE"
        )
        latest_parameters = {"scope_json": json.dumps(scopes)}
    else:
        index = " FORCE INDEX (uk_pit_coverage_scope_revision)" if mysql else ""
        limit = " LIMIT 1" if single else ""
        latest_query = text(
            f"SELECT {columns} FROM {SOURCE_COVERAGE_TABLE}{index} "
            "WHERE scope_hash IN :scopes ORDER BY scope_hash, revision_no DESC"
            f"{limit}{lock}"
        ).bindparams(bindparam("scopes", expanding=True))
        latest_parameters = {"scopes": scopes}
    with connection.execute(
        latest_query.execution_options(stream_results=True), latest_parameters,
    ) as result:
        for row in result.mappings():
            latest.setdefault(str(row["scope_hash"]), dict(row))

    matching_scopes: set[str] = set()
    # These values are part of the canonical payload. A mismatch rules out an
    # exact replay without transferring the previous response body.
    possible_replays = [
        row["coverage_id"] for scope, row in latest.items()
        if _row_datetime(row["covered_through_at"]) == by_scope[scope]["covered_through_at"]
        and row["source_response_hash"] == by_scope[scope]["source_response_hash"]
        and row["fact_set_hash"] == by_scope[scope]["fact_set_hash"]
    ]
    if possible_replays:
        # Matching metadata is only a prefilter. Compare the actual payload
        # bytes, including the capture evidence and original timestamps.
        latest_payload_query = text(
            f"SELECT scope_hash, payload_json FROM {SOURCE_COVERAGE_TABLE} "
            f"WHERE coverage_id IN :ids{lock}"
        ).bindparams(bindparam("ids", expanding=True))
        with connection.execute(
            latest_payload_query.execution_options(stream_results=True),
            {"ids": possible_replays},
        ) as result:
            for row in result.mappings():
                scope = str(row["scope_hash"])
                if str(row["payload_json"] or "") == by_scope[scope]["payload_json"]:
                    matching_scopes.add(scope)

    pending: dict[str, dict[str, Any]] = {}
    for payload, row in prepared:
        scope = row["scope_hash"]
        if scope in matching_scopes:
            continue
        previous = latest.get(scope) or {}
        supersedes = str(previous.get("coverage_id") or "") or None
        fingerprint = canonical_hash(
            {
                "schema": "probiga.pit-source-coverage-fingerprint.v1",
                "scope_hash": scope,
                "payload": payload,
                "supersedes_coverage_id": supersedes,
            }
        )
        pending[scope] = {
            **row,
            "coverage_id": canonical_hash(
                {
                    "schema": "probiga.pit-source-coverage-id.v1",
                    "scope_hash": scope,
                    "coverage_fingerprint_hash": fingerprint,
                }
            ),
            "revision_no": int(previous.get("revision_no") or 0) + 1,
            "supersedes_coverage_id": supersedes,
            "coverage_fingerprint_hash": fingerprint,
        }

    existing = _existing_scoped_fingerprints(
        connection,
        table_name=SOURCE_COVERAGE_TABLE,
        scope_column="scope_hash",
        fingerprint_column="coverage_fingerprint_hash",
        columns=columns,
        fingerprints={
            scope: row["coverage_fingerprint_hash"] for scope, row in pending.items()
        },
    )
    inserts = [pending[scope] for scope in sorted(pending) if scope not in existing]
    if inserts:
        _insert_pit_rows(connection, SOURCE_COVERAGE_TABLE, inserts)

    receipts = []
    for _, row in prepared:
        scope = row["scope_hash"]
        if scope in matching_scopes:
            receipts.append(_coverage_receipt(latest[scope], idempotent=True))
        elif scope in existing:
            receipts.append(_coverage_receipt(existing[scope], idempotent=True))
        else:
            receipts.append(_coverage_receipt({
                **pending[scope],
                "covered_through_at": _dt_text(pending[scope]["covered_through_at"]),
            }, idempotent=False))
    return receipts


def append_source_coverages(
    target: Engine | Connection,
    receipts: Iterable[Mapping[str, Any]],
) -> list[PITCoverageReceipt]:
    """Append validated source-query receipts atomically in bounded SQL batches.

    Callers supply one successful source response per scope, including proven
    empty responses. An existing Connection retains its enclosing transaction.
    """
    connection, close = _connection(target)
    transaction = connection.begin() if close else None
    output: list[PITCoverageReceipt] = []
    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    scopes: set[str] = set()
    batch_bytes = 0
    try:
        for arguments in receipts:
            payload, row = _prepare_source_coverage(**arguments)
            scope = row["scope_hash"]
            if scope in scopes:
                raise ValueError("coverage batch requires distinct scopes")
            scopes.add(scope)
            # PyMySQL bounds the escaped multi-row SQL as well. Keep the
            # parameter batches small before passing them to the driver.
            row_bytes = len(row["payload_json"].encode("utf-8")) + 1_024
            if prepared and (
                len(prepared) == _PIT_WRITE_BATCH_ROWS
                or batch_bytes + row_bytes > _PIT_WRITE_BATCH_BYTES
            ):
                output.extend(_write_source_coverage_chunk(connection, prepared))
                prepared.clear()
                batch_bytes = 0
            prepared.append((payload, row))
            batch_bytes += row_bytes
        if prepared:
            output.extend(_write_source_coverage_chunk(connection, prepared))
        if transaction is not None:
            transaction.commit()
        return output
    except Exception:
        if transaction is not None:
            transaction.rollback()
        raise
    finally:
        if close:
            connection.close()


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
    """Append one immutable successful source-query coverage receipt."""
    return append_source_coverages(target, ({
        "fact_kind": fact_kind,
        "stock_code": stock_code,
        "window_start": window_start,
        "window_end": window_end,
        "known_at": known_at,
        "received_at": received_at,
        "covered_through_at": covered_through_at,
        "watermark_kind": watermark_kind,
        "watermark_evidence": watermark_evidence,
        "source_rows": source_rows,
        "fact_bindings": fact_bindings,
        "source": source,
        "batch_id": batch_id,
    },))[0]


def _validate_nonfiling_evidence_version(
    evidence: Mapping[str, Any], *, code: str, report_date: date,
    known: datetime, historical_audit: bool,
) -> None:
    """Keep immutable original audits readable; all new writes require PDF proof."""
    version = evidence.get("evidence_schema")
    if version is None and historical_audit:
        # These already sealed facts retain their original hash and seven-day
        # validity rules. This branch is never available to a new append.
        title = str(evidence.get("announcement_title") or "")
        if not (
            "未在规定期限内披露定期报告" in title
            or "无法在法定期限内披露定期报告" in title
            or ("无法在规定期限内披露" in title and "年度报告" in title)
        ):
            raise ValueError("finance non-filing historical title is not dispositive")
        return
    if version != FINANCE_NONFILING_BODY_SCHEMA:
        raise ValueError("finance non-filing body evidence version differs")
    validate_document_body_evidence(evidence, code, report_date.isoformat())
    catalog = evidence.get("catalog_identity")
    if not isinstance(catalog, dict):
        raise ValueError("finance non-filing complete catalogue is required")
    window_start = _date_value(catalog.get("window_start"), required=True)
    window_end = _date_value(catalog.get("window_end"), required=True)
    published = normalize_decision_at(str(evidence.get("announcement_published_at") or ""))
    if (
        catalog.get("schema") != "probiga.cninfo-nonfiling-catalogue.v1"
        or catalog.get("stock_code") != code
        or not re.fullmatch(r"\d+", str(catalog.get("org_id") or ""))
        or catalog.get("complete") is not True
        or type(catalog.get("page_count")) is not int
        or not 1 <= catalog["page_count"] <= 20
        or type(catalog.get("row_count")) is not int
        or not 1 <= catalog["row_count"] <= catalog["page_count"] * 30
        or window_start is None or window_end is None
        or not window_start <= published.date() <= known.date() <= window_end
        or published > known
        or str(evidence.get("announcement_url") or "") != (
            "https://static.cninfo.com.cn/finalpage/"
            f"{published.date().isoformat()}/{evidence.get('announcement_id')}.PDF"
        )
    ):
        raise ValueError("finance non-filing catalogue or publication binding differs")


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
    _validate_nonfiling_evidence_version(
        evidence, code=code, report_date=report_date, known=known,
        historical_audit=False,
    )
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


def _signed_cninfo_clock_correction(
    previous: Mapping[str, Any], current: Mapping[str, Any],
    *, received: datetime | None, decision: datetime,
) -> bool:
    if (
        received is None or not _row_datetime(current["known_at"]) <= received <= decision
        or previous.get("source") != CNINFO_EVENT_SOURCE or current.get("source") != CNINFO_EVENT_SOURCE
        or previous.get("publication_time_status") != TIME_VERIFIED or previous.get("published_at") is None
    ):
        return False
    before, after = (_row_datetime(item["published_at"]) for item in (previous, current))
    if before.date() != after.date() or not timedelta(0) < before - after <= timedelta(seconds=1):
        return False
    old, new = (_parse_payload(item) for item in (previous, current))
    event_id = str(new.get("source_event_id") or "")
    if not re.fullmatch(r"\d{6,20}", event_id) or new.get("event_key") != f"cninfo:{event_id}":
        return False
    ignore = {"published_at", "source_row_hash", "source_fields"}
    if {k: v for k, v in old.items() if k not in ignore} != {k: v for k, v in new.items() if k not in ignore}:
        return False
    time_fields = {"announcementTime", "published_at", "source_row_hash"}
    retained = []
    for payload, published in ((old, before), (new, after)):
        fields = payload.get("source_fields")
        if not isinstance(fields, dict):
            return False
        raw_time = fields.get("announcementTime")
        url = str(fields.get("detail_url") or "")
        if (
            type(raw_time) is not int
            or raw_time != round(published.replace(tzinfo=SHANGHAI).timestamp() * 1000)
            or fields.get("provider") != CNINFO_EVENT_SOURCE or fields.get("announcement_id") != event_id
            or not re.fullmatch(r"https://static\.cninfo\.com\.cn/finalpage/\d{4}-\d{2}-\d{2}/" + event_id + r"\.pdf", url, re.IGNORECASE)
            or not _HASH_RE.fullmatch(str(payload.get("source_row_hash") or ""))
            or fields.get("source_row_hash") != payload.get("source_row_hash")
            or _row_datetime(payload.get("published_at")) != published
            or _row_datetime(fields.get("published_at")) != published
        ):
            return False
        retained.append({k: v for k, v in fields.items() if k not in time_fields})
    return retained[0] == retained[1]


def _latest_revision_for_fact_cutoff(
    rows: list[dict[str, Any]],
    *,
    fact_kind: str,
    fact_cutoff_at: datetime,
    decision_at: datetime,
    signed_event_received_at: datetime | None = None,
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
                and not (
                    fact_kind == "event" and signed_event_received_at is not None
                    and eligible is rows[index - 1]
                    and _signed_cninfo_clock_correction(
                        rows[index - 1], row, received=signed_event_received_at, decision=decision_at,
                    )
                )
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
        elif not (
            _live_capture_allowed(
                fact_cutoff_at=fact_cutoff_at, decision_at=decision_at, known_at=known,
            )
            or (
                # A signed CNINFO day-end reconstruction proves the provider's
                # date and the actual knowledge time, without inventing a time.
                fact_kind == "event"
                and row.get("source") == CNINFO_EVENT_SOURCE
                and signed_event_received_at is not None
                and datetime.combine(source_date, datetime.max.time()) <= fact_cutoff_at
                and known <= signed_event_received_at <= decision_at
            )
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


def _read_pit_payload_rows(
    connection: Connection, *, table_name: str, statement: Any,
    parameters: Mapping[str, Any], deadline: float, stage_prefix: str,
    omit_nonempty_coverage_for: set[str] | None = None,
) -> list[dict[str, Any]]:
    from server.common.qmt_announcement_pit import (
        _bounded_announcement_payload_keys, _live_announcement_read_hint,
        _live_announcement_reply_budget,
    )

    def read(query, values, stage):
        try:
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
                with connection.execute(query.execution_options(stream_results=True), values) as result:
                    return [dict(row) for row in result.mappings()]
        except Exception as exc:
            setattr(exc, "_pit_read_stage", stage)
            raise

    def invalid(message, stage):
        exc = ValueError(message)
        setattr(exc, "_pit_read_stage", stage)
        return exc

    id_column = {
        EVENT_REVISION_TABLE: "revision_id", FINANCE_REVISION_TABLE: "revision_id",
        SOURCE_COVERAGE_TABLE: "coverage_id",
    }.get(table_name)
    if id_column is None:
        raise ValueError("PIT payload table differs")
    inventory_stage = stage_prefix + "-inventory"
    try:
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=inventory_stage):
            if str(connection.get_isolation_level() or "").upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError("PIT payload reads require a repeatable-read snapshot")
    except Exception as exc:
        setattr(exc, "_pit_read_stage", inventory_stage)
        raise
    inventory = read(statement, parameters, inventory_stage)
    expected = {row[id_column]: row for row in inventory}
    inventory_columns = {id_column}
    if omit_nonempty_coverage_for:
        if table_name != SOURCE_COVERAGE_TABLE:
            raise ValueError("finance coverage filtering requires source coverage")
        inventory_columns.update({"stock_code", "scope_hash", "revision_no", "coverage_status", "result_count"})
    if (len(expected) != len(inventory)
            or any(set(row) != inventory_columns for row in inventory)
            or any(not _HASH_RE.fullmatch(str(key)) for key in expected)):
        raise invalid("PIT inventory identity differs", inventory_stage)
    if omit_nonempty_coverage_for:
        def stock_scope(row):
            return row["stock_code"], row["scope_hash"]

        ordered_ids = []
        for (code, _), scope_rows in groupby(inventory, key=stock_scope):
            chain = list(scope_rows)
            head = chain[-1]
            # Only an eligible empty head can affect a stock that has revision
            # identities. For those heads the complete predecessor chain is
            # still needed to detect corruption before accepting the proof.
            if code not in omit_nonempty_coverage_for or (
                head["coverage_status"] == "COMPLETE" and head["result_count"] == 0
            ):
                ordered_ids.extend(row[id_column] for row in chain)
    else:
        ordered_ids = list(expected)
    packed_by_id = {}
    metadata_columns = ",".join(sorted(_REQUIRED_COLUMNS[table_name] - {"payload_json"}))
    for offset in range(0, len(ordered_ids), FINANCE_REVISION_MYSQL_PAYLOAD_ROW_LIMIT):
        revision_ids = ordered_ids[offset:offset + FINANCE_REVISION_MYSQL_PAYLOAD_ROW_LIMIT]
        stage = f"{stage_prefix}-size:{offset}"
        hint = _live_announcement_read_hint(mysql=True, deadline=deadline, stage=stage)
        sizes_query = text(
            f"SELECT {hint}{id_column},OCTET_LENGTH(payload_json) AS payload_json_bytes "
            f"FROM {table_name} FORCE INDEX (PRIMARY) WHERE {id_column} IN :row_ids"
        ).bindparams(bindparam("row_ids", expanding=True)).execution_options(stream_results=True)
        size_rows = read(sizes_query, {"row_ids": revision_ids}, stage)
        sizes = {row[id_column]: row["payload_json_bytes"] for row in size_rows}
        if (
            len(size_rows) != len(revision_ids) or sizes.keys() != set(revision_ids)
            or any(type(size) is not int or size <= 0 for size in sizes.values())
        ):
            raise invalid("PIT payload size inventory differs", stage)
        for batch in _bounded_announcement_payload_keys(
            [(key, sizes[key]) for key in revision_ids], max_rows=FINANCE_REVISION_MYSQL_PAYLOAD_ROW_LIMIT,
        ):
            batch_ids = [key for key, _ in batch]
            stage = f"{stage_prefix}-payload:{offset}"
            hint = _live_announcement_read_hint(mysql=True, deadline=deadline, stage=stage)
            payload_query = text(
                f"SELECT {hint}{metadata_columns},OCTET_LENGTH(payload_json) AS payload_json_bytes,"
                "COMPRESS(payload_json) AS payload_json_compressed "
                f"FROM {table_name} FORCE INDEX (PRIMARY) WHERE {id_column} IN :row_ids"
            ).bindparams(bindparam("row_ids", expanding=True)).execution_options(stream_results=True)
            packed = read(payload_query, {"row_ids": batch_ids}, stage)
            actual = {row[id_column]: row for row in packed}
            if len(packed) != len(batch_ids) or actual.keys() != set(batch_ids):
                raise invalid("PIT payload membership differs", stage)
            for key, payload_bytes in batch:
                if actual[key]["payload_json_bytes"] != payload_bytes:
                    raise invalid("PIT payload size differs", stage)
                if omit_nonempty_coverage_for and any(
                    actual[key][column] != expected[key][column] for column in inventory_columns
                ):
                    raise invalid("finance coverage payload inventory differs", stage)
                packed_by_id[key] = actual[key]
    return [packed_by_id[key] for key in ordered_ids]


def _query_event_revisions(
    connection: Connection, *, codes: list[str], decision_at: datetime,
    start_date: date | None, end_date: date, source: str, read_deadline: float | None,
) -> list[dict[str, Any]]:
    from server.common.qmt_announcement_pit import (
        VALIDATION_READ_BATCH_SIZE, _live_announcement_read_hint, _live_announcement_reply_budget,
    )

    mysql = connection.dialect.name == "mysql"
    deadline = read_deadline if read_deadline is not None else monotonic() + 30 * 60

    def read_rows(statement, parameters, stage):
        try:
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
                with connection.execute(statement.execution_options(stream_results=True), parameters) as result:
                    return [dict(row) for row in result.mappings()]
        except Exception as exc:
            setattr(exc, "_pit_read_stage", stage)
            raise

    def fail(message, stage):
        exc = ValueError(message)
        setattr(exc, "_pit_read_stage", stage)
        raise exc

    isolation_stage = "event-revision-snapshot"
    if mysql:
        try:
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=isolation_stage):
                if str(connection.get_isolation_level() or "").upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                    raise ValueError("PIT event revisions require a repeatable-read snapshot")
        except Exception as exc:
            setattr(exc, "_pit_read_stage", isolation_stage)
            raise

    visibility = "known_at<=:decision_at AND received_at<=:decision_at"
    visibility_params: dict[str, Any] = {"decision_at": decision_at}
    if source:
        visibility += " AND source=:source"
        visibility_params["source"] = source
    date_window = "fact_date<=:end_date"
    window_params = {"end_date": end_date}
    if start_date is not None:
        date_window += " AND fact_date>=:start_date"
        window_params["start_date"] = start_date

    stocks = sorted(set(codes))
    seen_identities: set[str] = set()
    rows = []
    identity_offset = 0
    # Each identity includes its stock. Pick each head at E before filtering
    # by publication day, then read every visible predecessor in the same
    # snapshot. An unchanged revision can be bound into a later coverage batch.
    transaction_open = isinstance(connection, Connection) and connection.in_transaction()
    with (nullcontext() if transaction_open else connection.begin()):
        for stock_offset in range(0, len(stocks), VALIDATION_READ_BATCH_SIZE):
            stock_batch = stocks[stock_offset:stock_offset + VALIDATION_READ_BATCH_SIZE]
            stock_set = set(stock_batch)
            stage = f"event-revision-identities:{stock_offset}"
            hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
            identity_query = text(
                f"SELECT {hint}identity_hash,stock_code FROM ("
                "SELECT identity_hash,stock_code,COALESCE(DATE(published_at),event_date) AS fact_date, "
                "ROW_NUMBER() OVER (PARTITION BY identity_hash ORDER BY revision_no DESC) AS rn "
                f"FROM {EVENT_REVISION_TABLE} WHERE stock_code IN :codes AND {visibility}"
                f") selected WHERE rn=1 AND (fact_date IS NULL OR ({date_window})) ORDER BY identity_hash"
            ).bindparams(bindparam("codes", expanding=True))
            inventory = read_rows(
                identity_query, {**visibility_params, **window_params, "codes": stock_batch}, stage,
            )
            if any(set(row) != {"identity_hash", "stock_code"} for row in inventory):
                fail("PIT event revision identity inventory differs", stage)
            by_identity = {row["identity_hash"]: row["stock_code"] for row in inventory}
            if (
                len(by_identity) != len(inventory) or seen_identities.intersection(by_identity)
                or any(not _HASH_RE.fullmatch(str(key)) or code not in stock_set for key, code in by_identity.items())
            ):
                fail("PIT event revision identity inventory differs", stage)
            seen_identities.update(by_identity)
            identities = list(by_identity)
            for offset in range(0, len(identities), 400):
                identity_batch = identities[offset:offset + 400]
                stage = f"event-revision-history:{identity_offset}"
                identity_offset += len(identity_batch)
                columns = "revision_id" if mysql else "*"
                hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=f"{stage}-inventory")
                history_query = text(
                    f"SELECT {hint}{columns} FROM {EVENT_REVISION_TABLE} "
                    f"WHERE identity_hash IN :identities AND {visibility} "
                    "ORDER BY stock_code, identity_hash, revision_no"
                ).bindparams(bindparam("identities", expanding=True))
                parameters = {**visibility_params, "identities": identity_batch}
                if mysql:
                    history = _read_pit_payload_rows(
                        connection, table_name=EVENT_REVISION_TABLE, statement=history_query,
                        parameters=parameters, deadline=deadline, stage_prefix=stage,
                    )
                else:
                    history = read_rows(history_query, parameters, f"{stage}-inventory")
                observed = {row["identity_hash"] for row in history}
                if observed != set(identity_batch) or any(
                    row["stock_code"] != by_identity.get(row["identity_hash"]) for row in history
                ):
                    fail("PIT event revision history identity or stock differs", stage)
                for row in history:
                    try:
                        rows.append(_coverage_payload_row(row, mysql=mysql, stage=f"{stage}-decode"))
                    except ValueError as exc:
                        setattr(exc, "_pit_read_stage", f"{stage}-decode")
                        raise
    rows.sort(key=lambda row: (row["stock_code"], row["identity_hash"], row["revision_no"]))
    _live_announcement_read_hint(mysql=False, deadline=deadline, stage="event-revision-complete")
    return rows


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
    read_deadline: float | None = None,
) -> list[dict[str, Any]]:
    if table_name == EVENT_REVISION_TABLE:
        with (nullcontext(engine) if isinstance(engine, Connection) else engine.connect()) as connection:
            return _query_event_revisions(
                connection, codes=codes, decision_at=decision_at, start_date=start_date,
                end_date=end_date, source=source, read_deadline=read_deadline,
            )
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
    with (nullcontext(engine) if isinstance(engine, Connection) else engine.connect()) as connection:
        mysql_finance = connection.dialect.name == "mysql" and table_name == FINANCE_REVISION_TABLE
        columns, hint = "*", ""
        if mysql_finance:
            from server.common.qmt_announcement_pit import _live_announcement_read_hint

            if read_deadline is None:
                read_deadline = monotonic() + FINANCE_REVISION_MYSQL_DEADLINE_SECONDS
            columns = "revision_id"
            hint = _live_announcement_read_hint(
                mysql=True, deadline=read_deadline, stage="finance-revision-inventory",
            )
        statement = text(
            f"SELECT {hint}{columns} FROM {table_name} WHERE stock_code IN :codes "
            f"AND known_at<=:decision_at AND received_at<=:decision_at "
            f"{date_filter}ORDER BY stock_code, identity_hash, revision_no"
        ).bindparams(bindparam("codes", expanding=True))
        if mysql_finance:
            packed = _read_pit_payload_rows(
                connection, table_name=FINANCE_REVISION_TABLE, statement=statement, parameters=params,
                deadline=read_deadline, stage_prefix="finance-revision",
            )
            try:
                return [_coverage_payload_row(row, mysql=True, stage="finance-revision-decode") for row in packed]
            except ValueError as exc:
                setattr(exc, "_pit_read_stage", "finance-revision-decode")
                raise
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
    _validate_nonfiling_evidence_version(
        evidence, code=code, report_date=report_date, known=known,
        historical_audit=True,
    )
    announcement_id = str(evidence.get("announcement_id") or "")
    if (
        str(evidence.get("source") or "") != CNINFO_FINANCE_NONFILING_SOURCE
        or str(evidence.get("stock_code") or "").zfill(6) != code
        or str(evidence.get("expected_report_date") or "")
        != report_date.isoformat()
        or str(evidence.get("reason_code") or "")
        not in FINANCE_NONFILING_REASON_CODES
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


def _validate_coverage_chain(
    rows: Iterable[dict[str, Any]], *, first_revision: int = 1,
    previous_id: str | None = None, previous_known: datetime | None = None,
) -> None:
    """Validate payloads; a noninitial cursor requires a verified link prefix."""
    for expected_no, row in enumerate(rows, first_revision):
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
                or (
                    row_source == CNINFO_EVENT_SOURCE
                    and evidence.get("capture_purpose")
                    == EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR
                    and evidence.get("primary_provider") == CNINFO_EVENT_SOURCE
                    and not str(evidence.get("fallback_reason") or "")
                )
            )
            fallback_receipt_valid = _fallback_event_receipt_valid(
                evidence,
                source=row_source,
                stock_code=str(row.get("stock_code") or "").zfill(6),
                result_count=len(source_rows),
                source_rows=source_rows,
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
        expected_scope = _source_coverage_scope_hash(
            str(row.get("fact_kind") or ""), str(row.get("stock_code") or ""),
            str(row.get("source") or "")[:64],
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


def _empty_coverage_receipt(
    row: Mapping[str, Any],
    *,
    fact_cutoff_at: datetime,
    decision_at: datetime,
) -> dict[str, Any]:
    return {
        "coverage_id": str(row.get("coverage_id") or ""),
        "coverage_source": str(row.get("source") or ""),
        "coverage_response_hash": str(row.get("source_response_hash") or ""),
        "coverage_watermark_hash": str(row.get("watermark_hash") or ""),
        "covered_through_at": _dt_text(_row_datetime(row.get("covered_through_at"))),
        "coverage_known_at": _dt_text(_row_datetime(row.get("known_at"))),
        "coverage_received_at": _dt_text(_row_datetime(row.get("received_at"))),
        "coverage_batch_id": str(row.get("batch_id") or ""),
        "fact_cutoff_at": _dt_text(fact_cutoff_at),
        "decision_at": _dt_text(decision_at),
    }


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
    verified_event_batch: Mapping[str, Any] | None = None,
    event_completion_at: datetime | None = None,
    read_deadline: float | None = None,
    codes_with_revisions: set[str] | None = None,
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

    def code_chains():
        with engine.connect() as connection:
            mysql = connection.dialect.name == "mysql"
            indexed_event = bool(
                mysql and fact_kind == "event" and batch_id and source in AUTHORITATIVE_EVENT_SOURCES
            )
            indexed_finance = mysql and fact_kind == "finance"
            omit_nonempty_for = codes_with_revisions if indexed_finance else None
            snapshot_budget = nullcontext()
            if indexed_event:
                from server.common.qmt_announcement_pit import (
                    _live_announcement_read_hint, _live_announcement_reply_budget,
                )

                deadline = monotonic() + LIVE_EVENT_COVERAGE_READ_DEADLINE_SECONDS
                snapshot_budget = _live_announcement_reply_budget(
                    connection, deadline=deadline, stage="event-empty-coverage-isolation",
                )
            elif indexed_finance:
                from server.common.qmt_announcement_pit import _live_announcement_read_hint

                deadline = read_deadline if read_deadline is not None else (
                    monotonic() + FINANCE_REVISION_MYSQL_DEADLINE_SECONDS
                )
            with snapshot_budget:
                if mysql and str(
                    connection.get_isolation_level() or ""
                ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                    raise ValueError("empty coverage requires a repeatable-read snapshot")
            # Keep each code's full chain in one chunk and limit the number of
            # codes transferred together on the remote MySQL connection.
            batch_size = max(1, int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT))
            if mysql:
                batch_size = min(batch_size, EMPTY_COVERAGE_MYSQL_CODE_LIMIT)
            if omit_nonempty_for:
                columns = "coverage_id,stock_code,scope_hash,revision_no,coverage_status,result_count"
            elif indexed_finance:
                columns = "coverage_id"
            else:
                columns = _coverage_payload_columns(mysql)
            hint = _live_announcement_read_hint(
                mysql=True, deadline=deadline, stage="finance-empty-coverage-inventory",
            ) if indexed_finance else ""
            statement = text(
                f"SELECT {hint}{columns} FROM {SOURCE_COVERAGE_TABLE} "
                "WHERE fact_kind=:fact_kind AND stock_code IN :codes "
                "AND known_at<=:decision_at AND received_at<=:decision_at "
                f"{source_filter} "
                "ORDER BY stock_code, scope_hash, revision_no"
            ).bindparams(bindparam("codes", expanding=True)).execution_options(
                stream_results=True,
            )
            with connection.begin():
                for offset in range(0, len(codes), batch_size):
                    code_batch = codes[offset:offset + batch_size]
                    by_code_scope: dict[
                        str, dict[str, list[dict[str, Any]]]
                    ] = defaultdict(lambda: defaultdict(list))
                    if indexed_event:
                        rows = nullcontext(_read_indexed_event_coverage_rows(
                            connection, codes=code_batch, source=source, decision_at=decision_at,
                            deadline=deadline, stage=f"event-empty-coverage:{offset}",
                        ))
                    elif indexed_finance:
                        rows = nullcontext(_read_pit_payload_rows(
                            connection, table_name=SOURCE_COVERAGE_TABLE, statement=statement,
                            parameters={**params, "codes": code_batch}, deadline=deadline,
                            stage_prefix="finance-empty-coverage",
                            omit_nonempty_coverage_for=omit_nonempty_for,
                        ))
                    else:
                        rows = connection.execute(statement, {**params, "codes": code_batch})
                    with rows as result:
                        for raw in result if indexed_event or indexed_finance else result.mappings():
                            row = dict(raw)
                            by_code_scope[str(row.get("stock_code") or "").zfill(6)][
                                str(row.get("scope_hash") or "")
                            ].append(row)
                    for code in code_batch:
                        yield code, by_code_scope.get(code, {}), mysql
                    by_code_scope.clear()
                if indexed_event:
                    _live_announcement_read_hint(
                        mysql=False, deadline=deadline, stage="event-empty-coverage-complete",
                    )

    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    for code, scopes, mysql in code_chains():
        candidates: list[dict[str, Any]] = []
        for chain in scopes.values():
            try:
                if mysql:
                    chain = [
                        _coverage_payload_row(
                            row, mysql=True, stage=f"{fact_kind}_empty_coverage",
                        )
                        for row in chain
                    ]
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
                    or _verified_cninfo_coverage_known(
                        latest, verified_event_batch,
                        fact_cutoff_at=fact_cutoff_at, decision_at=decision_at,
                        completion_known_at=event_completion_at,
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
            available[code] = _empty_coverage_receipt(
                candidates[-1],
                fact_cutoff_at=fact_cutoff_at,
                decision_at=decision_at,
            )
    return available, invalid


def load_finance_expected_unavailable(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    expected_report_date: date | str | None = None,
    known_after: datetime | str | None = None,
    read_deadline: float | None = None,
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

    def code_chains():
        deadline = read_deadline if read_deadline is not None else (
            monotonic() + FINANCE_NONFILING_MYSQL_DEADLINE_SECONDS
        )
        with engine.connect() as connection:
            mysql = connection.dialect.name == "mysql"
            batch_size = len(normalized_codes)
            timeout_hint = ""
            if mysql:
                batch_size = FINANCE_NONFILING_MYSQL_CODE_LIMIT
                timeout_hint = (
                    f"/*+ MAX_EXECUTION_TIME({FINANCE_NONFILING_MYSQL_QUERY_TIMEOUT_MS}) */ "
                )
                if str(connection.get_isolation_level()).upper() != "SERIALIZABLE":
                    connection = connection.execution_options(
                        isolation_level="REPEATABLE READ"
                    )
            columns = (
                "coverage_id"
                if mysql else _coverage_payload_columns(False)
            )
            statement = text(
                f"SELECT {timeout_hint}{columns} "
                f"FROM {SOURCE_COVERAGE_TABLE} "
                "WHERE fact_kind='finance' AND source=:source "
                "AND stock_code IN :codes "
                "AND known_at<=:decision_at AND received_at<=:decision_at "
                "ORDER BY stock_code, scope_hash, revision_no"
            ).bindparams(bindparam("codes", expanding=True)).execution_options(
                stream_results=True,
            )
            # Each symbol's full chain belongs to one query and all queries
            # share the same snapshot, including across batch boundaries.
            with connection.begin():
                for offset in range(0, len(normalized_codes), batch_size):
                    if mysql and monotonic() >= deadline:
                        raise TimeoutError(
                            f"finance_nonfiling: read deadline after {offset} codes"
                        )
                    parameters = {
                        "source": CNINFO_FINANCE_NONFILING_SOURCE,
                        "codes": normalized_codes[offset:offset + batch_size],
                        "decision_at": decision,
                    }
                    rows = (
                        nullcontext(_read_pit_payload_rows(
                            connection, table_name=SOURCE_COVERAGE_TABLE, statement=statement,
                            parameters=parameters, deadline=deadline, stage_prefix="finance-nonfiling",
                        ))
                        if mysql else connection.execute(statement, parameters)
                    )
                    with rows as result:
                        for code, code_rows in groupby(
                            result if mysql else result.mappings(),
                            key=lambda row: str(row["stock_code"] or "").zfill(6),
                        ):
                            by_scope: dict[str, list[dict[str, Any]]] = defaultdict(list)
                            for row in code_rows:
                                by_scope[str(row["scope_hash"] or "")].append(dict(row))
                            yield code, by_scope, mysql
    minimum_known = (
        normalize_decision_at(known_after) if known_after is not None else None
    )
    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    for code, scopes, mysql in code_chains():
        candidates: list[dict[str, Any]] = []
        for chain in scopes.values():
            try:
                if mysql:
                    chain = [
                        _coverage_payload_row(row, mysql=True, stage="finance_nonfiling")
                        for row in chain
                    ]
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
    with engine.connect() as connection:
        selected = connection.execute(text(
            f"SELECT * FROM {SOURCE_COVERAGE_TABLE} WHERE coverage_id=:coverage_id "
            "AND known_at<=:decision_at AND received_at<=:decision_at"
        ), {"coverage_id": receipt_id, "decision_at": decision}).mappings().first()
        if selected is None:
            raise ValueError("finance incremental discovery receipt is unavailable or postdates decision")
        selected = dict(selected)
        # Prior payloads were checked when appended and are immutable. Inspect
        # their links and clocks, not every old source response on each resume.
        links = connection.execution_options(stream_results=True).execute(text(
            "SELECT coverage_id,scope_hash,coverage_fingerprint_hash,revision_no,"
            "supersedes_coverage_id,known_at,received_at,covered_through_at,coverage_status "
            f"FROM {SOURCE_COVERAGE_TABLE} WHERE scope_hash=:scope_hash "
            "AND revision_no<=:revision_no AND known_at<=:decision_at "
            "AND received_at<=:decision_at ORDER BY revision_no"
        ), {"scope_hash": selected["scope_hash"], "revision_no": selected["revision_no"],
            "decision_at": decision}).mappings()
        previous_id, previous_known, count = None, None, 0
        predecessor_id, predecessor_known = None, None
        for count, row in enumerate(links, 1):
            known = _row_datetime(row["known_at"])
            if (int(row["revision_no"]) != count
                    or (str(row["supersedes_coverage_id"] or "") or None) != previous_id
                    or _row_datetime(row["received_at"]) > known
                    or _row_datetime(row["covered_through_at"]) > known
                    or (previous_known is not None and known < previous_known)
                    or str(row["coverage_status"]) != "COMPLETE"
                    or str(row["coverage_id"]) != canonical_hash({
                        "schema": "probiga.pit-source-coverage-id.v1",
                        "scope_hash": str(row["scope_hash"]),
                        "coverage_fingerprint_hash": str(row["coverage_fingerprint_hash"]),
                    })):
                raise ValueError("finance incremental discovery coverage links differ")
            if str(row["coverage_id"]) == receipt_id:
                predecessor_id, predecessor_known = previous_id, previous_known
            previous_id, previous_known = str(row["coverage_id"]), known
        if count != int(selected["revision_no"]) or previous_id != receipt_id:
            raise ValueError("finance incremental discovery coverage links are incomplete")
    _validate_coverage_chain([selected], first_revision=int(selected["revision_no"]),
                             previous_id=predecessor_id, previous_known=predecessor_known)
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


def _finance_strategy_prefix_binding(source_rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = [dict(row) for row in source_rows if isinstance(row, dict)]
    report_dates = sorted({_date_value(row.get("report_date"), required=True) for row in rows},
                          reverse=True)[:FINANCE_ATOMIC_BATCH_HISTORY_LIMIT]
    strategy_rows = [row for row in rows if _date_value(row.get("report_date"), required=True) in report_dates]
    if not strategy_rows:
        raise ValueError("finance strategy prefix is empty")
    return strategy_rows, {
        "history_limit": FINANCE_ATOMIC_BATCH_HISTORY_LIMIT,
        "report_date_count": len(report_dates),
        "content_row_count": len(strategy_rows),
        "latest_report_date": report_dates[0].isoformat(),
        "oldest_report_date": report_dates[-1].isoformat(),
        "content_root_sha256": canonical_hash({
            "schema": "probiga.pit-finance-strategy-prefix.v1",
            "rows": sorted(strategy_rows, key=canonical_json),
        }),
    }


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

    Each immutable coverage already contains a complete provider snapshot.
    Load the latest visible record per source scope and its predecessor anchor,
    in bounded code chunks inside one repeatable-read snapshot. Historical
    payload replay belongs to the full audit reader, not daily batch assembly.
    """

    gate = finance_disclosure_gate(as_of_date)
    selected: list[dict[str, Any]] = []
    source_cutoff: datetime | None = None
    batch_size = max(1, int(FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT))
    with engine.connect() as connection:
        dialect = str(
            getattr(getattr(connection, "dialect", None), "name", "") or ""
        ).lower()
        mysql = dialect == "mysql"
        if mysql:
            isolation = str(connection.get_isolation_level() or "").upper()
            if isolation not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError(
                    "finance atomic batch requires a repeatable-read snapshot"
                )
        statement = text(
            f"SELECT {_coverage_payload_columns(mysql, table_alias='selected')},"
            "parent.coverage_id AS _parent_id,parent.scope_hash AS _parent_scope,"
            "parent.revision_no AS _parent_revision,parent.known_at AS _parent_known,"
            "parent.received_at AS _parent_received,parent.covered_through_at AS _parent_covered,"
            "parent.coverage_status AS _parent_status,parent.coverage_fingerprint_hash AS _parent_fingerprint "
            f"FROM {SOURCE_COVERAGE_TABLE} AS selected JOIN ("
            f"SELECT scope_hash,MAX(revision_no) AS revision_no FROM {SOURCE_COVERAGE_TABLE} "
            "WHERE fact_kind='finance' AND stock_code IN :codes "
            "AND known_at<=:completed_known_at AND received_at<=:completed_known_at "
            "GROUP BY scope_hash) AS heads "
            "ON selected.scope_hash=heads.scope_hash AND selected.revision_no=heads.revision_no "
            f"LEFT JOIN {SOURCE_COVERAGE_TABLE} AS parent ON parent.coverage_id=selected.supersedes_coverage_id "
            "ORDER BY selected.stock_code,selected.scope_hash"
        ).bindparams(bindparam("codes", expanding=True))
        with connection.begin():
            for offset in range(0, len(codes), batch_size):
                code_batch = codes[offset : offset + batch_size]
                raw_rows, predecessors = [], {}
                for raw in connection.execute(
                    statement,
                    {"codes": code_batch, "completed_known_at": completed_known_at},
                ).mappings():
                    row = _coverage_payload_row(raw, mysql=mysql)
                    parent = {key: row.pop("_parent_" + key) for key in (
                        "id", "scope", "revision", "known", "received", "covered", "status", "fingerprint",
                    )}
                    revision = int(row["revision_no"])
                    if revision < 1:
                        raise ValueError("finance coverage revision is invalid")
                    previous_id, previous_known = None, None
                    if revision > 1:
                        previous_id = str(parent["id"] or "")
                        previous_known = _row_datetime(parent["known"])
                        if (parent["scope"] != row["scope_hash"]
                                or int(parent["revision"] or 0) != revision - 1
                                or previous_known > _row_datetime(row["known_at"])
                                or _row_datetime(parent["received"]) > previous_known
                                or _row_datetime(parent["covered"]) > previous_known
                                or parent["status"] not in {"COMPLETE", FINANCE_EXPECTED_UNAVAILABLE_STATUS}
                                or not _HASH_RE.fullmatch(str(parent["fingerprint"] or ""))
                                or previous_id != canonical_hash({
                                    "schema": "probiga.pit-source-coverage-id.v1",
                                    "scope_hash": str(parent["scope"]),
                                    "coverage_fingerprint_hash": str(parent["fingerprint"]),
                                })):
                            raise ValueError("finance coverage predecessor differs")
                    predecessors[str(row["coverage_id"])] = (previous_id, previous_known)
                    raw_rows.append(row)
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
                        latest = chain[-1]
                        previous_id, previous_known = predecessors[str(latest["coverage_id"])]
                        _validate_coverage_chain(chain, first_revision=int(latest["revision_no"]),
                                                 previous_id=previous_id, previous_known=previous_known)
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
                        strategy_rows, prefix = _finance_strategy_prefix_binding(
                            payload.get("source_rows") or (),
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
                        compact["_strategy_prefix_binding"] = prefix
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
    if row.get("coverage_status") == FINANCE_DATA_EXCLUDED_STATUS:
        return "", str(row.get("_reason_code") or row.get("reason_code") or "")
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


def build_finance_data_exclusion(
    *,
    stock_code: str,
    target_date: date | str,
    observed_at: datetime | str,
    source: str,
    reason_code: str,
    error_detail: str,
) -> dict[str, Any]:
    """Bind an observed stock-local failure without asserting source coverage."""

    code = str(stock_code or "").strip().zfill(6)
    target = _date_value(target_date, required=True)
    observed = normalize_decision_at(observed_at)
    reason = str(reason_code or "")
    if (
        not re.fullmatch(r"\d{6}", code)
        or target is None
        or target > observed.date()
        or reason not in {
            "FINANCE_FETCH_FAILED", "FINANCE_RESPONSE_INVALID",
            "FINANCE_NONFILING_EVIDENCE_UNAVAILABLE",
        }
        or not str(source or "").strip()
        or not str(error_detail or "").strip()
    ):
        raise ValueError("finance data exclusion identity is invalid")
    core = {
        "schema": "probiga.finance-data-exclusion.v1",
        "stock_code": code,
        "target_date": target.isoformat(),
        "observed_at": _dt_text(observed),
        "source": str(source).strip()[:64],
        "reason_code": reason,
        "error_detail": str(error_detail).strip()[:1000],
    }
    return {**core, "disposition_sha256": canonical_hash(core)}


def _finance_exclusion_member(
    exclusion: Mapping[str, Any], *, target: date, completed: datetime,
) -> dict[str, Any]:
    evidence = dict(exclusion)
    rebuilt = build_finance_data_exclusion(**{
        key: evidence.get(key)
        for key in (
            "stock_code", "target_date", "observed_at", "source",
            "reason_code", "error_detail",
        )
    })
    if (
        evidence != rebuilt
        or evidence["target_date"] != target.isoformat()
        or normalize_decision_at(evidence["observed_at"]) > completed
    ):
        raise ValueError("finance data exclusion proof differs")
    return {
        "stock_code": evidence["stock_code"],
        "coverage_status": FINANCE_DATA_EXCLUDED_STATUS,
        "source": evidence["source"],
        "known_at": evidence["observed_at"],
        "covered_through_at": evidence["observed_at"],
        "reason_code": evidence["reason_code"],
        "data_exclusion": evidence,
    }


def _finance_exclusion_summary(
    members: Mapping[str, Mapping[str, Any]], *, target: date,
    completed: datetime,
) -> dict[str, Any]:
    exclusions = []
    for code, member in sorted(members.items()):
        if member.get("coverage_status") != FINANCE_DATA_EXCLUDED_STATUS:
            continue
        evidence = member.get("data_exclusion")
        if not isinstance(evidence, Mapping) or dict(member) != (
            _finance_exclusion_member(evidence, target=target, completed=completed)
        ) or evidence.get("stock_code") != code:
            raise ValueError("finance excluded member differs from its failure proof")
        exclusions.append(dict(evidence))
    return {
        "data_excluded_count": len(exclusions),
        "available_code_count": len(members) - len(exclusions),
        "data_excluded_codes": [item["stock_code"] for item in exclusions],
        "data_excluded_reasons": {
            item["stock_code"]: item["reason_code"] for item in exclusions
        },
        "data_exclusions": exclusions,
    }


def append_finance_atomic_batch_seal(
    engine: Engine,
    *,
    as_of_date: date | str,
    completed_known_at: datetime | str | None = None,
    incremental_discovery_coverage_id: str = "",
    changed_codes: Iterable[str] | None = None,
    provider_contract_version: str = "",
    data_exclusions: Iterable[Mapping[str, Any]] = (),
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
    excluded_members: dict[str, dict[str, Any]] = {}
    for exclusion in data_exclusions:
        member = _finance_exclusion_member(
            exclusion, target=target, completed=completed,
        )
        code = member["stock_code"]
        if code not in codes or code in excluded_members:
            raise ValueError("finance exclusions duplicate or exceed catalog scope")
        excluded_members[code] = member
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
    effective_changes.update(excluded_members)
    prior_discovery = prior_evidence.get("incremental_discovery_binding") or {}
    same_discovery_content = bool(
        prior_members
        and prior_evidence.get("as_of_date") == target.isoformat()
        and prior_evidence.get("catalog_member_set_hash") == catalog.member_set_hash
        and prior_evidence.get("provider_contract_version") == str(provider_contract_version or "")
        and _HASH_RE.fullmatch(str(incremental_discovery.get("event_set_sha256") or ""))
        and isinstance(prior_discovery, Mapping)
        and all(str(prior_discovery.get(field) or "") == str(incremental_discovery.get(field) or "")
                for field in ("window_start", "window_end", "event_set_sha256", "changed_code_set_sha256"))
    )
    # A later observation of the same source contents is not another change
    # to every stock already incorporated into this same-day snapshot.
    if not same_discovery_content:
        effective_changes.update(
            str(code).zfill(6)
            for code in (incremental_discovery.get("changed_dates_by_code") or {})
            if str(code).zfill(6) in codes
        )
    gate = finance_disclosure_gate(target)
    for code in set(codes) & set(prior_members):
        member = prior_members[code]
        listing_date = listing_dates.get(code)
        if member.get("coverage_status") == FINANCE_DATA_EXCLUDED_STATUS:
            # A failed observation is never reused as proof of unchanged data.
            effective_changes.add(code)
            continue
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
    source_changed = sorted(set(changed) - set(excluded_members))
    if source_changed:
        if changed_codes is None:
            prior_exclusions = load_finance_data_exclusions(
                engine, codes=source_changed, decision_at=completed, as_of_date=target,
            )
        else:
            # The parent payload and its exclusions were validated above in
            # this operation. Derive the same result without rereading every
            # historical full-catalog seal a second time.
            prior_baseline = {}
            if prior_evidence.get("as_of_date") == target.isoformat():
                prior_baseline = {
                    **prior_evidence,
                    **_finance_exclusion_summary(prior_members,
                        target=target,
                        completed=_row_datetime(prior_evidence.get("completed_known_at"))),
                    "seal_coverage_id": str(prior_row.get("coverage_id") or ""),
                }
            prior_exclusions = _finance_data_exclusions_from_baseline(
                prior_baseline, codes=source_changed, target=target,
            )
        selected, delta_source_cutoff = _select_finance_atomic_batch_members(
            engine,
            codes=source_changed,
            listing_dates=listing_dates,
            completed_known_at=completed,
            as_of_date=target,
            incremental_discovery=incremental_discovery,
            catalog_binding=catalog_binding,
        )
        for row in selected:
            prior = prior_exclusions.get(str(row.get("stock_code") or ""), {})
            if (
                prior
                and _row_datetime(row.get("known_at"))
                <= _row_datetime(prior.get("observed_at"))
            ):
                raise ValueError("finance excluded member requires a fresh source observation")
    delta_members = sorted(
        [_finance_atomic_member(row) for row in selected]
        + list(excluded_members.values()),
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
        if len(excluded_members) != len(codes):
            raise ValueError("finance atomic batch source cutoff is unavailable")
        # No source facts exist in an all-excluded batch. This is only the
        # disposition observation cutoff; consumers receive zero available codes.
        source_cutoffs = [completed]
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
        **_finance_exclusion_summary(member_map, target=target, completed=completed),
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


def _decode_coverage_payload(
    packed: object, *, expected_bytes: int, stage: str = "finance_seal_history",
) -> str:
    """Decode one MySQL COMPRESS frame within that exact stored payload size.

    The database's OCTET_LENGTH from the same row is the inflation bound; no
    smaller business limit is imposed on existing valid full-catalog seals.
    """
    if (type(expected_bytes) is not int or expected_bytes <= 0
            or not isinstance(packed, (bytes, bytearray, memoryview))):
        raise ValueError(f"{stage}: compressed payload type/size differs")
    packed = bytes(packed)
    if len(packed) <= 4 or int.from_bytes(packed[:4], "little") != expected_bytes:
        raise ValueError(f"{stage}: compressed payload length differs")
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(packed[4:], expected_bytes + 1)
    except zlib.error as exc:
        raise ValueError(f"{stage}: invalid MySQL zlib payload") from exc
    if (len(raw) != expected_bytes or decoder.unconsumed_tail or not decoder.eof):
        raise ValueError(f"{stage}: compressed payload inflation/boundary differs")
    tail = decoder.unused_data
    if (tail not in (b"", b".") or (tail == b"." and packed[-2] != 0x20)
            or (not tail and packed[-1] == 0x20)):
        raise ValueError(f"{stage}: invalid MySQL compressed trailing bytes")
    try:
        return raw.decode("utf-8")
    except UnicodeError as exc:
        raise ValueError(f"{stage}: compressed payload is not UTF-8") from exc


def _coverage_payload_columns(mysql: bool, *, table_alias: str = "") -> str:
    prefix = table_alias + "." if table_alias else ""
    if not mysql:
        return prefix + "*"
    metadata = sorted(_REQUIRED_COLUMNS[SOURCE_COVERAGE_TABLE] - {"payload_json"})
    return (
        ",".join(prefix + column for column in metadata)
        + f",OCTET_LENGTH({prefix}payload_json) AS payload_json_bytes,"
        + f"COMPRESS({prefix}payload_json) AS payload_json_compressed"
    )


def _coverage_payload_row(
    row: Mapping[str, Any], *, mysql: bool, stage: str = "finance_seal_history",
) -> dict[str, Any]:
    current = dict(row)
    if mysql:
        current["payload_json"] = _decode_coverage_payload(
            current.pop("payload_json_compressed"),
            expected_bytes=current.pop("payload_json_bytes"), stage=stage,
        )
    return current


def _read_indexed_event_coverage_inventory(
    connection: Connection, *, codes: list[str], source: str, decision_at: datetime,
    deadline: float, stage: str, include_metadata: bool = False,
) -> dict[str, dict[str, Any]]:
    from server.common.qmt_announcement_pit import (
        _live_announcement_read_hint,
        _live_announcement_reply_budget,
    )

    code_by_scope = {_source_coverage_scope_hash("event", code, source): code for code in codes}
    inventory_stage = f"{stage}:inventory"
    hint = _live_announcement_read_hint(mysql=True, deadline=deadline, stage=inventory_stage)
    columns = (
        ",".join(sorted(_REQUIRED_COLUMNS[SOURCE_COVERAGE_TABLE] - {"payload_json"}))
        if include_metadata else "coverage_id, stock_code, scope_hash, revision_no"
    )
    inventory = text(
        f"SELECT {hint}{columns} "
        f"FROM {SOURCE_COVERAGE_TABLE} FORCE INDEX (uk_pit_coverage_scope_revision) "
        "WHERE scope_hash IN :scope_hashes AND fact_kind='event' AND source=:source "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        "ORDER BY scope_hash, revision_no"
    ).bindparams(bindparam("scope_hashes", expanding=True)).execution_options(stream_results=True)
    expected = {}
    with _live_announcement_reply_budget(connection, deadline=deadline, stage=inventory_stage):
        with connection.execute(inventory, {
            "scope_hashes": sorted(code_by_scope), "source": source, "decision_at": decision_at,
        }) as rows:
            for row in rows.mappings():
                coverage_id, code, scope, revision = (
                    row["coverage_id"], row["stock_code"], row["scope_hash"], row["revision_no"],
                )
                if (not _HASH_RE.fullmatch(str(coverage_id)) or coverage_id in expected
                        or code_by_scope.get(scope) != code
                        or type(revision) is not int or revision <= 0):
                    raise ValueError("event coverage inventory identity differs")
                expected[coverage_id] = dict(row)
    return expected


def _read_indexed_event_coverage_payloads(
    connection: Connection, *, expected: Mapping[str, Mapping[str, Any]],
    source: str, deadline: float, stage: str,
) -> Iterable[dict[str, Any]]:
    from server.common.qmt_announcement_pit import (
        VALIDATION_READ_BATCH_SIZE,
        _bounded_announcement_payload_keys,
        _live_announcement_read_hint,
        _live_announcement_reply_budget,
        _read_live_announcement_payload_sizes,
    )

    ordered_ids = list(expected)
    payload_offset = 0
    for offset in range(0, len(ordered_ids), VALIDATION_READ_BATCH_SIZE):
        ids = ordered_ids[offset:offset + VALIDATION_READ_BATCH_SIZE]
        sizes = _read_live_announcement_payload_sizes(
            connection, source=source, coverage_ids=ids, deadline=deadline,
            stage=f"{stage}:size:{offset}",
        )
        for batch in _bounded_announcement_payload_keys([(key, sizes[key]) for key in ids]):
            payload_stage = f"{stage}:payload:{payload_offset}"
            hint = _live_announcement_read_hint(mysql=True, deadline=deadline, stage=payload_stage)
            payload = text(
                f"SELECT {hint}{_coverage_payload_columns(True)} "
                f"FROM {SOURCE_COVERAGE_TABLE} FORCE INDEX (PRIMARY) "
                "WHERE coverage_id IN :coverage_ids"
            ).bindparams(bindparam("coverage_ids", expanding=True)).execution_options(stream_results=True)
            batch_ids = [coverage_id for coverage_id, _ in batch]
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=payload_stage):
                with connection.execute(payload, {"coverage_ids": batch_ids}) as rows:
                    packed_rows = rows.mappings().all()
            actual = {row["coverage_id"]: row for row in packed_rows}
            if len(packed_rows) != len(batch_ids) or actual.keys() != set(batch_ids):
                raise ValueError("event coverage payload membership differs from inventory")
            for coverage_id, payload_bytes in batch:
                row = actual[coverage_id]
                if (any(row.get(key) != value for key, value in expected[coverage_id].items())
                        or row["payload_json_bytes"] != payload_bytes
                        or row["fact_kind"] != "event" or row["source"] != source):
                    raise ValueError("event coverage payload identity differs from inventory")
                yield dict(row)
            payload_offset += len(batch_ids)


def _read_indexed_event_coverage_rows(
    connection: Connection, *, codes: list[str], source: str, decision_at: datetime,
    deadline: float, stage: str,
) -> Iterable[dict[str, Any]]:
    """Read each complete source scope through its index and bounded PK replies."""
    expected = _read_indexed_event_coverage_inventory(
        connection, codes=codes, source=source, decision_at=decision_at, deadline=deadline, stage=stage,
    )
    yield from _read_indexed_event_coverage_payloads(
        connection, expected=expected, source=source, deadline=deadline, stage=stage,
    )


def _read_verified_event_coverage_chunk(
    connection: Connection, *, codes: list[str], source: str, decision_at: datetime,
    deadline: float, stage: str, checkpoint: EventCoverageCheckpoint,
) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    """Validate only the suffix after a matching, previously verified prefix."""
    inventory = _read_indexed_event_coverage_inventory(
        connection, codes=codes, source=source, decision_at=decision_at, deadline=deadline,
        stage=stage, include_metadata=True,
    )
    scopes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in inventory.values():
        scopes[row["scope_hash"]].append(row)
    prior = checkpoint.load(list(scopes))
    prefixes, pending = {}, {}
    for scope, rows in scopes.items():
        count, digest = prior.get(scope, (0, ""))
        if type(count) is not int or not 0 < count <= len(rows) or canonical_hash(rows[:count]) != digest:
            count = 0
        prefixes[scope] = count
        pending.update((row["coverage_id"], row) for row in rows[count:])

    packed = {
        row["coverage_id"]: row
        for row in _read_indexed_event_coverage_payloads(
            connection, expected=pending, source=source, deadline=deadline, stage=stage,
        )
    }
    valid, invalid, verified = {}, set(), []
    for scope, rows in scopes.items():
        count = prefixes[scope]
        head = rows[count - 1] if count else None
        suffix = rows[count:]
        try:
            _validate_coverage_chain(
                (
                    _coverage_payload_row(
                        packed.pop(row["coverage_id"]), mysql=True, stage="common_cutoff_event_coverage",
                    )
                    for row in suffix
                ),
                first_revision=count + 1,
                previous_id=head["coverage_id"] if head else None,
                previous_known=_row_datetime(head["known_at"]) if head else None,
            )
        except ValueError:
            invalid.add(rows[0]["stock_code"])
            continue
        valid[scope] = rows
        if suffix:
            verified.append((scope, len(rows), canonical_hash(rows)))
    checkpoint.save(verified)
    return valid, invalid


def _load_latest_finance_atomic_seal_evidence(
    engine: Engine,
    *,
    decision_at: datetime | str,
    seal_coverage_id: str = "",
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    decision = normalize_decision_at(decision_at)
    seal_rows: list[dict[str, Any]] = []
    latest_row: dict[str, Any] = {}
    with engine.connect() as connection:
        if connection.dialect.name == "mysql" and str(
            connection.get_isolation_level() or ""
        ).upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
            raise ValueError("finance seal history requires a repeatable-read snapshot")

        # Earlier full-catalog payloads are immutable and were checked when
        # appended. Recheck their links, then load only this snapshot and its
        # explicit parent. Full-history payload auditing remains a separate job.
        index_hint = (
            " FORCE INDEX (idx_pit_coverage_decision)"
            if connection.dialect.name == "mysql"
            else ""
        )
        key_statement = text(
            f"SELECT coverage_id, scope_hash, revision_no "
            f"FROM {SOURCE_COVERAGE_TABLE}{index_hint} "
            "WHERE fact_kind='finance' AND stock_code=:seal_code "
            "AND source=:seal_source AND known_at<=:decision_at "
            "AND received_at<=:decision_at "
            "AND (:seal_id='' OR coverage_id=:seal_id) "
            "ORDER BY known_at DESC, revision_no DESC LIMIT 1"
        )
        mysql = connection.dialect.name == "mysql"
        payload_statement = text(
            f"SELECT {_coverage_payload_columns(mysql)} FROM {SOURCE_COVERAGE_TABLE} "
            "WHERE coverage_id=:coverage_id"
        )

        with connection.begin():
            try:
                key_row = connection.execute(key_statement, {
                    "seal_code": FINANCE_ATOMIC_BATCH_CODE,
                    "seal_source": FINANCE_ATOMIC_BATCH_SOURCE,
                    "decision_at": decision,
                    "seal_id": seal_coverage_id,
                }).mappings().first()
                if key_row is None:
                    if seal_coverage_id:
                        raise ValueError("requested finance atomic seal is unavailable at decision time")
                    return {}, {}, {}, []
                raw = connection.execute(payload_statement, {
                    "coverage_id": key_row["coverage_id"],
                }).mappings().first()
                if raw is None:
                    raise ValueError("finance seal disappeared inside repeatable-read snapshot")
                latest_row = _coverage_payload_row(raw, mysql=mysql)
                links = connection.execute(text(
                    "SELECT coverage_id,scope_hash,coverage_fingerprint_hash,revision_no,"
                    "supersedes_coverage_id,known_at,received_at,covered_through_at,"
                    "fact_kind,stock_code,source,coverage_status,result_count "
                    f"FROM {SOURCE_COVERAGE_TABLE} WHERE scope_hash=:scope_hash "
                    "AND revision_no<=:revision_no ORDER BY revision_no"
                ), {"scope_hash": latest_row["scope_hash"],
                    "revision_no": latest_row["revision_no"]}).mappings()
                anchors = {}
                previous_id, previous_known, count = None, None, 0
                for count, link in enumerate(links, 1):
                    known = _row_datetime(link["known_at"])
                    if (int(link["revision_no"]) != count
                            or (str(link["supersedes_coverage_id"] or "") or None) != previous_id
                            or _row_datetime(link["received_at"]) > known
                            or _row_datetime(link["covered_through_at"]) > known
                            or known > decision
                            or (previous_known is not None and known < previous_known)
                            or link["fact_kind"] != "finance"
                            or link["stock_code"] != FINANCE_ATOMIC_BATCH_CODE
                            or link["source"] != FINANCE_ATOMIC_BATCH_SOURCE
                            or link["coverage_status"] != "COMPLETE"
                            or int(link["result_count"]) != 0
                            or link["coverage_id"] != canonical_hash({
                                "schema": "probiga.pit-source-coverage-id.v1",
                                "scope_hash": str(link["scope_hash"]),
                                "coverage_fingerprint_hash": str(link["coverage_fingerprint_hash"]),
                            })):
                        raise ValueError("finance atomic seal coverage links differ")
                    ident = str(link["coverage_id"])
                    anchors[ident] = (count, previous_id, previous_known)
                    seal_rows.append({"coverage_id": ident, "batch_root_sha256": None})
                    previous_id, previous_known = ident, known
                if count != int(latest_row["revision_no"]) or previous_id != latest_row["coverage_id"]:
                    raise ValueError("finance atomic seal coverage links are incomplete")

                def validate_snapshot(snapshot):
                    revision, parent_id, parent_known = anchors[str(snapshot["coverage_id"])]
                    _validate_coverage_chain([snapshot], first_revision=revision,
                                             previous_id=parent_id, previous_known=parent_known)
                    payload = _parse_payload(snapshot)
                    watermark = payload.get("watermark")
                    evidence = watermark.get("evidence") if isinstance(watermark, dict) else None
                    if not isinstance(evidence, dict):
                        raise ValueError("finance atomic batch seal evidence is malformed")
                    return evidence

                current_evidence = validate_snapshot(latest_row)
                seal_rows[-1]["batch_root_sha256"] = current_evidence.get("batch_root_sha256")
                parent_id = str(current_evidence.get("parent_seal_coverage_id") or "")
                if parent_id:
                    if parent_id not in anchors or parent_id == latest_row["coverage_id"]:
                        raise ValueError("finance atomic batch parent is unavailable")
                    parent = connection.execute(payload_statement, {"coverage_id": parent_id}).mappings().first()
                    if parent is None:
                        raise ValueError("finance atomic batch parent is unavailable")
                    parent_evidence = validate_snapshot(_coverage_payload_row(parent, mysql=mysql))
                    seal_rows[anchors[parent_id][0] - 1]["batch_root_sha256"] = parent_evidence.get("batch_root_sha256")
            except OperationalError as exc:
                exc._pit_read_stage = "finance_seal_history"
                raise
    if not seal_rows:
        return {}, {}, {}, []
    row = latest_row
    if seal_coverage_id and row.get("coverage_id") != seal_coverage_id:
        raise ValueError("requested finance atomic seal is unavailable at decision time")
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
    _finance_exclusion_summary(
        member_map,
        target=_date_value(evidence.get("as_of_date"), required=True),
        completed=_row_datetime(evidence.get("completed_known_at")),
    )
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
        **_finance_exclusion_summary(
            member_map,
            target=_date_value(evidence.get("as_of_date"), required=True),
            completed=_row_datetime(evidence.get("completed_known_at")),
        ),
    }


def _finance_atomic_row_from_member(
    member: Mapping[str, Any],
) -> dict[str, Any]:
    """Rehydrate the compact fields consumed by the sealed strategy reader."""

    return {
        "fact_kind": "finance",
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
        "_data_exclusion": dict(member.get("data_exclusion") or {}),
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
    seal_coverage_id: str = "",
    verify_source_rows: bool = True,
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
    if not verify_source_rows and not seal_coverage_id:
        raise ValueError("finance receipt-only validation requires an exact seal ID")
    selection = {"seal_coverage_id": seal_coverage_id} if seal_coverage_id else {}
    row, evidence, member_map, _ = _load_latest_finance_atomic_seal_evidence(
        engine, decision_at=decision, **selection,
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
    excluded_summary = _finance_exclusion_summary(
        member_map, target=sealed_as_of, completed=completed,
    )
    excluded_codes = set(excluded_summary["data_excluded_codes"])
    source_validation_codes = [
        code for code in validation_codes if code not in excluded_codes
    ]
    selected: list[dict[str, Any]] = []
    delta_source_cutoff: datetime | None = None
    selected_map = {}
    if verify_source_rows:
        if source_validation_codes:
            selected, delta_source_cutoff = _select_finance_atomic_batch_members(
                engine,
                codes=source_validation_codes,
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
            [_finance_atomic_member(item) for item in selected_map.values()]
            + [member_map[code] for code in validation_codes if code in excluded_codes],
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
        ):
            raise ValueError("finance atomic batch member proof differs")
    if finance_disclosure_gate(sealed_as_of).minimum_report_date.isoformat() != evidence.get("minimum_report_date"):
        raise ValueError("finance atomic batch disclosure period differs")
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
        **excluded_summary,
        "members": {code: member_map[code] for code in requested},
        "rows": {code: sealed_rows[code] for code in requested},
        "schema": schema,
        "changed_code_count": int(evidence.get("changed_code_count") or 0),
        "parent_batch_root_sha256": str(
            evidence.get("parent_batch_root_sha256") or ""
        ),
        "delta_root_sha256": str(evidence.get("delta_root_sha256") or ""),
    }


@dataclass(frozen=True)
class _LiveEventSelection:
    engine: Engine
    decision_at: datetime
    window_start: date
    window_end: date
    batch: Mapping[str, Any]
    members: Mapping[str, tuple[Mapping[str, Any], bool]]


@dataclass(frozen=True)
class _SignedLiveEventSelection(_LiveEventSelection):
    completion_at: datetime
    bindings: Mapping[str, tuple[Mapping[str, str], ...]]


@dataclass(frozen=True)
class HistoricalEventRead:
    """In-run authoritative event proof and the exact cutoff it resolved."""

    selection: HistoricalAnnouncementSelection | _LiveEventSelection
    fact_cutoff_at: datetime
    signed_historical_bindings: Mapping[str, tuple[Mapping[str, str], ...]] | None = None


def _announcement_batch_read_reason(error: Exception) -> str:
    from server.common.qmt_announcement_pit import (
        LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE,
        LIVE_ANNOUNCEMENT_BATCH_REPLY_FAILED,
        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE,
    )

    reason = str(getattr(error, "reason_code", "") or "")
    detail = str(getattr(error, "detail", "") or "")
    if reason in {
        LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE,
        LIVE_ANNOUNCEMENT_BATCH_REPLY_FAILED,
        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE,
    } and re.fullmatch(
        r"snapshot-isolation|candidate-discovery(?::[0-9]{1,8})?|first-member-(?:inventory|payload)|"
        r"stock-catalog|batch-verified|batch-inventory(?::[0-9]{1,8})?|"
        r"batch-payload(?:-size)?:[0-9]{1,8}|"
        r"incremental-parent|candidate-read|signed-historical-(?:isolation|catalog|native|inventory|"
        r"(?:chain|size|payload|bindings):[0-9]{1,8}(?::inventory)?)",
        detail,
    ):
        return f"{reason}:{detail}"
    return reason or f"PIT_QMT_EVENT_BATCH_UNAVAILABLE:{type(error).__name__}"


def _bind_signed_live_event_selection(
    engine: Engine, receipt: Mapping[str, Any], *, codes: list[str],
    decision: datetime, cutoff: datetime, completion: datetime,
    window_start: date, window_end: date, phase: str,
) -> _SignedLiveEventSelection:
    from server.common.qmt_announcement_pit import (
        QMT_ANNOUNCEMENT_TASK_SCHEMA,
        validate_complete_qmt_announcement_batch,
    )

    source = receipt["source"]
    if (
        receipt["schema"] != QMT_ANNOUNCEMENT_TASK_SCHEMA or receipt["status"] != "COMPLETE"
        or source not in {QMT_EVENT_SOURCE, CNINFO_EVENT_SOURCE}
        or not receipt["batch_id"] or not _HASH_RE.fullmatch(str(receipt["batch_root_hash"]))
        or not _HASH_RE.fullmatch(str(receipt["stage_evidence_sha256"]))
        or not _HASH_RE.fullmatch(str(receipt["stage_attempt_uid"]))
    ):
        raise ValueError("signed announcement stage identity differs")
    captured: list[dict[str, tuple[dict[str, Any], list[dict[str, str]]]]] = []
    batch = validate_complete_qmt_announcement_batch(
        engine, codes=codes, decision_at=decision, fact_cutoff_at=cutoff,
        window_start=window_start, window_end=window_end, source=source,
        _required_batch_id=receipt["batch_id"], _capture_bound_members=captured.append,
    )
    if len(captured) != 1 or any(
        batch.get(field) != receipt[field] for field in (
            "batch_id", "batch_root_hash", "source", "fact_cutoff_at", "received_at",
            "window_start", "window_end", "capture_window_start", "incremental_chain_root_hash",
            "parent_batch_id", "parent_batch_root_hash", "catalog_batch_id",
            "catalog_manifest_hash", "catalog_member_set_hash", "fallback_reason",
        )
    ) or batch.get("catalog_member_count") != receipt.get("stock_count"):
        raise ValueError("announcement durable batch differs from its signed stage receipt")
    members = captured[0]
    return _SignedLiveEventSelection(
        engine=engine, decision_at=decision, window_start=window_start, window_end=window_end,
        batch=MappingProxyType({
            **batch, f"{phase}_stage": {
                "attempt_uid": receipt["stage_attempt_uid"],
                "evidence_sha256": receipt["stage_evidence_sha256"],
                "completed_at": _dt_text(completion),
            },
        }),
        members=MappingProxyType({
            code: (MappingProxyType(row), True) for code, (row, _) in members.items()
        }),
        completion_at=completion,
        bindings=MappingProxyType({
            code: tuple(MappingProxyType(binding) for binding in bindings)
            for code, (_, bindings) in members.items()
        }),
    )


def _load_certified_initial_event_selection(
    engine: Engine, required: FrozenInitialAnnouncement, *, codes: list[str],
    decision: datetime, data_date: date, window_start: date, window_end: date,
    event_progress: Callable[[int, int], None] | None = None, binding_units: int = 0,
) -> _SignedLiveEventSelection | HistoricalAnnouncementSelection:
    from server.common.authoritative_market_clock import STRATEGY_POOL_INITIAL_TIME
    from server.common.daily_delivery_control import (
        FrozenInitialAnnouncement, signed_initial_announcement_window_matches,
    )

    if not isinstance(required, FrozenInitialAnnouncement):
        raise ValueError("initial announcement requires a verified stage receipt")
    receipt = required.receipt
    cutoff = _row_datetime(receipt["fact_cutoff_at"])
    received = _row_datetime(receipt["received_at"])
    completion = _row_datetime(receipt["stage_finished_at"])
    if (
        not signed_initial_announcement_window_matches(receipt, data_date)
        or not data_date == cutoff.date() == window_end
        or decision < datetime.combine(data_date, STRATEGY_POOL_INITIAL_TIME)
        or not cutoff <= received <= completion <= decision
    ):
        raise ValueError("initial announcement source or completion scope differs")
    if receipt.get("mode") in {"HISTORICAL_RECONSTRUCTION", "HISTORICAL_RECONSTRUCTION_EXISTING"}:
        return _bind_signed_historical_event_selection(
            engine, receipt, codes=codes, decision=decision, received=received, completion=completion,
            window_start=window_start, window_end=window_end, event_progress=event_progress,
            binding_units=binding_units,
        )
    return _bind_signed_live_event_selection(
        engine, receipt, codes=codes, decision=decision, cutoff=cutoff, completion=completion,
        window_start=window_start, window_end=window_end, phase="initial",
    )


def _read_signed_historical_coverage_links(
    connection: Connection, *, exact_ids: Mapping[str, str], received: datetime,
    deadline: float, offset: int,
) -> dict[str, tuple[Mapping[str, Any] | None, bool]]:
    """Verify coverage links and the original frontier without reading other batches' payloads."""
    from server.common.qmt_announcement_pit import _live_announcement_read_hint, _live_announcement_reply_budget

    mysql = connection.dialect.name == "mysql"
    stage = f"signed-historical-chain:{offset}"
    hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
    index = " FORCE INDEX (uk_pit_coverage_scope_revision)" if mysql else ""
    code_by_scope = {_source_coverage_scope_hash("event", code, CNINFO_EVENT_SOURCE): code for code in exact_ids}
    statement = text(
        f"SELECT {hint}coverage_id,scope_hash,stock_code,revision_no,supersedes_coverage_id,"
        "known_at,received_at,covered_through_at,coverage_status,coverage_fingerprint_hash "
        f"FROM {SOURCE_COVERAGE_TABLE}{index} WHERE scope_hash IN :scopes "
        "AND fact_kind='event' AND source=:source AND known_at<=:received AND received_at<=:received "
        "ORDER BY scope_hash,revision_no"
    ).bindparams(bindparam("scopes", expanding=True)).execution_options(stream_results=True)
    links = {}
    with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
        with connection.execute(statement, {
            "scopes": sorted(code_by_scope), "source": CNINFO_EVENT_SOURCE, "received": received,
        }) as result:
            for scope, chain in groupby(result.mappings(), key=lambda row: row["scope_hash"]):
                code = code_by_scope[scope]
                selected_id = exact_ids[code]
                previous = predecessor = None
                for revision, row in enumerate(chain, start=1):
                    fingerprint = row["coverage_fingerprint_hash"]
                    known = _row_datetime(row["known_at"])
                    if (
                        row["stock_code"] != code or row["revision_no"] != revision
                        or (row["supersedes_coverage_id"] or None) != (previous["coverage_id"] if previous else None)
                        or (previous is not None and known < _row_datetime(previous["known_at"]))
                        or not _row_datetime(row["received_at"]) <= known <= received
                        or _row_datetime(row["covered_through_at"]) > known
                        or row["coverage_status"] != "COMPLETE"
                        or not _HASH_RE.fullmatch(str(fingerprint or ""))
                        or row["coverage_id"] != canonical_hash({
                            "schema": "probiga.pit-source-coverage-id.v1", "scope_hash": scope,
                            "coverage_fingerprint_hash": fingerprint,
                        })
                    ):
                        raise ValueError("historical announcement coverage chain differs")
                    if row["coverage_id"] == selected_id:
                        predecessor = previous
                        links[selected_id] = (predecessor, False)
                    previous = row
                if selected_id in links and previous is not None and previous["coverage_id"] == selected_id:
                    links[selected_id] = (predecessor, True)
    if links.keys() != set(exact_ids.values()):
        raise ValueError("historical announcement selected coverage is unavailable at its original frontier")
    return links


def _bind_signed_historical_event_selection(
    engine: Engine, receipt: Mapping[str, Any], *, codes: list[str],
    decision: datetime, received: datetime, completion: datetime,
    window_start: date, window_end: date,
    event_progress: Callable[[int, int], None] | None = None, binding_units: int = 0,
) -> HistoricalAnnouncementSelection:
    from server.common.pit_source_coverage_index import PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME
    from server.common.qmt_announcement_pit import (
        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS, VALIDATION_READ_BATCH_SIZE,
        AnnouncementCatalog, HistoricalAnnouncementSelection,
        QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA, QMT_ANNOUNCEMENT_TASK_SCHEMA,
        _batch_root_from_entries, _bounded_announcement_payload_keys, _fallback_receipt_valid,
        _live_announcement_read_hint, _live_announcement_reply_budget, _qmt_time,
        _read_live_announcement_payload_rows, _read_live_announcement_payload_sizes,
        _reconstruction_authority_matches, _source_publication_within_window, validate_task_result,
    )
    from server.common.qmt_stock_catalog import load_stock_catalog

    if (
        receipt.get("schema") != QMT_ANNOUNCEMENT_TASK_SCHEMA
        or receipt.get("status") != "COMPLETE"
        or receipt.get("source") != CNINFO_EVENT_SOURCE
        or receipt.get("mode") not in {"HISTORICAL_RECONSTRUCTION", "HISTORICAL_RECONSTRUCTION_EXISTING"}
        or not _HASH_RE.fullmatch(str(receipt.get("stage_evidence_sha256") or ""))
        or not _HASH_RE.fullmatch(str(receipt.get("stage_attempt_uid") or ""))
        or validate_task_result(dict(receipt), 0) != "complete"
    ):
        raise ValueError("signed historical announcement stage identity differs")
    source = CNINFO_EVENT_SOURCE
    batch_id = receipt["batch_id"]
    root = receipt["batch_root_hash"]
    start = date.fromisoformat(receipt["window_start"])
    cutoff = _row_datetime(receipt["source_query_cutoff_at"])
    provenance = receipt["reconstruction_provenance"]
    authority = provenance["authority"]
    if (
        not codes or not start <= window_start <= window_end
        or receipt["window_end"] != window_end.isoformat()
        or receipt.get("capture_window_start", receipt["window_start"]) != start.isoformat()
        or provenance.get("query_window_start") != start.isoformat()
        or provenance.get("query_window_end") != window_end.isoformat()
        or _row_datetime(receipt["reconstructed_at"]) != received
        or provenance.get("zero_result_receipts_complete") is not True
    ):
        raise ValueError("historical announcement durable batch differs from its signed stage receipt")

    deadline = monotonic() + LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    entries = []
    members = {}
    requested = set(codes)
    incremental_hashes = set()
    master = provenance["security_master"]
    master_started = _row_datetime(master["started_at"])
    master_ended = _row_datetime(master["ended_at"])
    requested_start = _qmt_time(datetime.combine(start, datetime.min.time()))
    requested_end = _qmt_time(cutoff)
    with engine.connect() as connection:
        mysql = connection.dialect.name == "mysql"
        with _live_announcement_reply_budget(connection, deadline=deadline, stage="signed-historical-isolation"):
            if mysql and str(connection.get_isolation_level() or "").upper() not in {"REPEATABLE READ", "SERIALIZABLE"}:
                raise ValueError("signed historical announcement requires a repeatable-read snapshot")
        with _live_announcement_reply_budget(connection, deadline=deadline, stage="signed-historical-catalog"):
            catalog = load_stock_catalog(connection, batch_id=receipt["catalog_batch_id"], decision_known_at=received)
        catalog_codes = catalog.eligible_codes(window_end.isoformat())
        qmt_by_code = {member["stock_code"]: member["qmt_code"] for member in catalog.members}
        if (
            catalog.manifest_hash != receipt["catalog_manifest_hash"]
            or catalog.member_set_hash != receipt["catalog_member_set_hash"]
            or len(catalog_codes) != receipt["stock_count"]
            or canonical_hash(catalog_codes) != provenance.get("catalog_codes_sha256")
            or not requested.issubset(catalog_codes)
            or not _reconstruction_authority_matches(
                authority, target_trade_date=window_end, known_at=received,
                catalog=AnnouncementCatalog(catalog.batch_id, catalog.manifest_hash,
                                            catalog.member_set_hash, tuple(catalog_codes), qmt_by_code),
            )
        ):
            raise ValueError("historical announcement reconstruction catalog differs")
        additions = authority["reconciliation"]["added_to_prior"]
        if additions:
            from server.common.qmt_datadir_daily_coverage import load_daily_manifest

            with _live_announcement_reply_budget(connection, deadline=deadline, stage="signed-historical-native"):
                native = load_daily_manifest(
                    connection, window_end.isoformat(), decision_known_at=received,
                    manifest_hash=authority["qmt_daily_truth"]["truth_hash"], verify_rows=False,
                )
            entities = {item["stock_code"]: item for item in (native or {}).get("entities", ())}
            if any(entities.get(item["stock_code"]) != item["native_daily_entity"] for item in additions):
                raise ValueError("historical announcement added listing native trade proof differs")

        stage = "signed-historical-inventory"
        hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
        index = f" FORCE INDEX ({PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME})" if mysql else ""
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
            inventory = connection.execute(text(
                f"SELECT {hint}stock_code,coverage_id FROM {SOURCE_COVERAGE_TABLE}{index} "
                "WHERE fact_kind='event' AND source=:source AND batch_id=:batch_id "
                "ORDER BY stock_code LIMIT :row_limit"
            ), {"source": source, "batch_id": batch_id, "row_limit": len(catalog_codes) + 1}).tuples().all()
        if [row[0] for row in inventory] != sorted(catalog_codes):
            raise ValueError("historical announcement durable batch differs from its signed stage receipt")

        total = len(inventory) + binding_units
        if event_progress is not None:
            event_progress(0, total)
        for offset in range(0, len(inventory), VALIDATION_READ_BATCH_SIZE):
            stock_batch = dict(inventory[offset:offset + VALIDATION_READ_BATCH_SIZE])
            links = _read_signed_historical_coverage_links(
                connection, exact_ids=stock_batch, received=received, deadline=deadline, offset=offset,
            )
            sizes = _read_live_announcement_payload_sizes(
                connection, source=source, batch_id=batch_id, coverage_ids=list(stock_batch.values()),
                deadline=deadline, stage=f"signed-historical-size:{offset}",
            )
            for keys in _bounded_announcement_payload_keys(list(sizes.items())):
                selected_ids = [key for key, _ in keys]
                rows = _read_live_announcement_payload_rows(
                    connection, source=source, batch_id=batch_id, coverage_ids=selected_ids,
                    deadline=deadline, stage=f"signed-historical-payload:{len(entries)}",
                )
                if len(rows) != len(selected_ids) or {row["coverage_id"] for row in rows} != set(selected_ids):
                    raise ValueError("historical announcement durable coverage membership differs")
                for row in rows:
                    code = row["stock_code"]
                    previous, head = links[row["coverage_id"]]
                    _validate_coverage_chain(
                        (row,), first_revision=row["revision_no"],
                        previous_id=previous["coverage_id"] if previous else None,
                        previous_known=_row_datetime(previous["known_at"]) if previous else None,
                    )
                    payload = json.loads(row.pop("payload_json"))  # Canonical bytes were checked above.
                    evidence = payload["watermark"]["evidence"]
                    provider = evidence["provider_receipt"]
                    incremental = evidence["incremental_proof"]
                    count = row["result_count"]
                    if evidence["global_batch_root_hash"] != root:
                        raise ValueError("historical announcement durable batch differs from its signed stage receipt")
                    if (
                        row["watermark_kind"] != "HISTORICAL_RECONSTRUCTION"
                        or _date_value(row["window_start"], required=True) != start
                        or _date_value(row["window_end"], required=True) != window_end
                        or _row_datetime(row["covered_through_at"]) != cutoff
                        or _row_datetime(row["known_at"]) != received
                        or _row_datetime(row["received_at"]) != received
                        or evidence["reconstruction_provenance"] != provenance
                        or not master_started <= _row_datetime(provider["captured_at"]) <= master_ended
                        or any(provider.get(key) != master.get(master_key) for key, master_key in (
                            ("security_master_started_at", "started_at"), ("security_master_ended_at", "ended_at"),
                            *((field, field) for field in (
                                "directory_raw_sha256", "directory_manifest_hash", "directory_member_set_hash",
                                "directory_member_count", "directory_attestation_sha256", "requested_catalog_member_set_sha256",
                            )),
                        ))
                        or not _fallback_receipt_valid(
                            provider, source=source, stock_code=code, qmt_code=qmt_by_code[code],
                            requested_start_time=requested_start, requested_end_time=requested_end,
                            result_count=count, catalog_codes=catalog_codes,
                        )
                        or any(not item.get("source_event_id") or not _HASH_RE.fullmatch(str(item.get("source_row_hash") or ""))
                               or not _source_publication_within_window(item, window_start=start, fact_cutoff_at=cutoff)
                               for item in payload["source_rows"])
                        or incremental.get("mode") != "FULL_BASELINE"
                        or incremental.get("coverage_window_start") != start.isoformat()
                        or incremental.get("capture_window_start") != start.isoformat()
                        or incremental.get("window_end") != window_end.isoformat()
                        or any(incremental.get(field) != "" for field in ("parent_batch_id", "parent_batch_root_hash", "parent_received_at"))
                        or incremental.get("result_batch_root_hash") != root
                        or canonical_hash({key: value for key, value in incremental.items() if key != "chain_root_hash"})
                        != incremental.get("chain_root_hash")
                    ):
                        raise ValueError("historical announcement durable member differs")
                    incremental_hashes.add(incremental["chain_root_hash"])
                    entries.append({"stock_code": code, "source_response_hash": row["source_response_hash"],
                                    "result_count": count, "provider_receipt_hash": evidence["provider_receipt_hash"]})
                    if code in requested:
                        members[code] = (MappingProxyType(row), head)
                if event_progress is not None:
                    event_progress(len(entries), total)

    _live_announcement_read_hint(mysql=False, deadline=deadline, stage="batch-verified")
    if (
        set(members) != requested or len(entries) != receipt["coverage_count"]
        or sum(item["result_count"] for item in entries) != receipt["event_count"]
        or sum(not item["result_count"] for item in entries) != receipt["empty_stock_count"]
        or len(incremental_hashes) != 1
        or ("incremental_chain_root_hash" in receipt and incremental_hashes != {receipt["incremental_chain_root_hash"]})
        or _batch_root_from_entries(
            batch_id=batch_id, fact_cutoff_at=cutoff, received_at=received, window_start=start, window_end=window_end,
            catalog_batch_id=catalog.batch_id, catalog_manifest_hash=catalog.manifest_hash,
            catalog_member_set_hash=catalog.member_set_hash, entries=entries, source=source,
            reconstruction_provenance=provenance,
        ) != root
    ):
        raise ValueError("historical announcement durable batch differs from its signed stage receipt")

    return HistoricalAnnouncementSelection(
        engine=engine, decision_at=decision, window_start=window_start, window_end=window_end,
        batch=MappingProxyType({
            **{field: receipt[field] for field in (
                "status", "source", "primary_source", "fallback_reason", "batch_id", "batch_root_hash",
                "window_start", "window_end", "catalog_batch_id", "catalog_manifest_hash", "catalog_member_set_hash",
                "coverage_count", "event_count", "empty_stock_count", "reconstruction_provenance", "reconstruction_sha256",
            )},
            "schema": QMT_ANNOUNCEMENT_RECONSTRUCTION_BATCH_SCHEMA, "mode": "HISTORICAL_RECONSTRUCTION",
            "capture_window_start": start.isoformat(), "catalog_member_count": len(catalog_codes),
            "incremental_chain_root_hash": next(iter(incremental_hashes)),
            "fact_cutoff_at": _dt_text(cutoff), "source_query_cutoff_at": _dt_text(cutoff),
            "decision_at": _dt_text(received), "received_at": _dt_text(received), "reconstructed_at": _dt_text(received),
            "initial_stage": {
                "attempt_uid": receipt["stage_attempt_uid"],
                "evidence_sha256": receipt["stage_evidence_sha256"],
                "completed_at": _dt_text(completion),
            },
        }),
        members=MappingProxyType(members),
    )


def _load_certified_premarket_event_selection(
    engine: Engine, required: FrozenPremarketAnnouncement, *, codes: list[str],
    decision: datetime, data_date: date, window_start: date, window_end: date,
) -> _SignedLiveEventSelection:
    from server.common.daily_delivery_control import FrozenPremarketAnnouncement

    if not isinstance(required, FrozenPremarketAnnouncement):
        raise ValueError("premarket announcement requires a verified stage receipt")
    receipt = required.receipt
    stage = receipt["premarket_source"]
    cutoff = _row_datetime(receipt["fact_cutoff_at"])
    received = _row_datetime(receipt["received_at"])
    published = _row_datetime(receipt["publish_completed_at"])
    completion = _row_datetime(receipt["stage_finished_at"])
    if (
        stage["data_trade_date"] != data_date.isoformat()
        or receipt["coverage_target_date"] != data_date.isoformat()
        or stage["execution_session_date"] != window_end.isoformat()
        or receipt["window_end"] != window_end.isoformat()
        or not data_date < cutoff.date() == window_end == decision.date()
        or cutoff != _row_datetime(stage["source_cutoff_at"])
        or decision != _row_datetime(stage["decision_cutoff_at"])
        or not cutoff <= received <= published <= completion <= decision
        or receipt["parent_batch_id"] != stage["parent_batch_id"]
        or receipt["parent_batch_root_hash"] != stage["parent_batch_root_hash"]
        or receipt["source"] != stage["parent_source"]
    ):
        raise ValueError("premarket announcement source or completion scope differs")
    return _bind_signed_live_event_selection(
        engine, receipt, codes=codes, decision=decision, cutoff=cutoff, completion=completion,
        window_start=window_start, window_end=window_end, phase="premarket",
    )


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
    include_event_facts: bool = True,
    capture_historical_selection: Callable[[HistoricalEventRead], None] | None = None,
    capture_finance_selection: Callable[[SealedFinanceInputs], None] | None = None,
    event_checkpoint_path: Path | None = None,
    event_progress: Callable[[int, int], None] | None = None,
    required_premarket_announcement: FrozenPremarketAnnouncement | None = None,
    required_initial_announcement: FrozenInitialAnnouncement | None = None,
    _dataset_cutoffs: bool = False,
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
    if required_initial_announcement is not None and required_premarket_announcement is not None:
        return {**blocked, "reason": "PIT_SIGNED_ANNOUNCEMENT_SCOPE_CONFLICT"}
    signed_source = required_initial_announcement
    signed_phase = "INITIAL"
    read_signed_selection = _load_certified_initial_event_selection
    if required_premarket_announcement is not None:
        signed_source = required_premarket_announcement
        signed_phase = "PREMARKET"
        read_signed_selection = _load_certified_premarket_event_selection
    if signed_source is not None and not (_dataset_cutoffs and require_qmt_event_batch and include_event_facts):
        return {**blocked, "reason": f"PIT_{signed_phase}_ANNOUNCEMENT_SCOPE_REQUIRED"}
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
    historical_selections: list[HistoricalAnnouncementSelection] = []
    reusable_live_batch: list[bool] = []
    signed_selection = signed_historical_bindings = None
    if signed_source is not None:
        try:
            initial_progress = (
                {"event_progress": event_progress,
                 "binding_units": len(normalized_codes) if capture_historical_selection is not None else 0}
                if required_initial_announcement is not None else {}
            )
            signed_selection = read_signed_selection(
                engine, signed_source, codes=normalized_codes, decision=decision,
                data_date=windows["finance"][1], window_start=windows["event"][0], window_end=windows["event"][1],
                **initial_progress,
            )
            event_batch = dict(signed_selection.batch)
            if capture_historical_selection is not None and not isinstance(signed_selection, _SignedLiveEventSelection):
                signed_historical_bindings = _read_signed_historical_event_bindings(
                    engine, signed_selection, event_progress=event_progress,
                )
        except Exception as exc:
            return {**blocked, "reason": f"PIT_{signed_phase}_ANNOUNCEMENT_INVALID:" + _announcement_batch_read_reason(exc)}
    elif require_qmt_event_batch and include_event_facts:
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
                capture_historical_selection=(
                    historical_selections.append if capture_historical_selection else None
                ),
                capture_live_reuse=(
                    (lambda: reusable_live_batch.append(True)) if capture_historical_selection else None
                ),
            )
        except Exception as exc:
            return {
                **blocked,
                "reason": _announcement_batch_read_reason(exc),
            }
    historical_selection = next(iter(historical_selections), None)
    exact_event_selection = signed_selection or historical_selection
    signed_live_selection = signed_selection if isinstance(signed_selection, _SignedLiveEventSelection) else None
    scan_event = include_event_facts and exact_event_selection is None
    coverage_progress = event_progress if signed_selection is None or signed_live_selection is not None else None
    event_filter = ""
    query_params: dict[str, Any] = {
        "codes": normalized_codes,
        "decision_at": decision,
    }
    if event_batch and scan_event:
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
        "fact_kind='finance'"
        if not scan_event
        else (
            "fact_kind='event'"
            if finance_batch
            else "fact_kind IN ('finance','event')"
        )
    )
    coverage_scope_sql = (
        f"FROM {SOURCE_COVERAGE_TABLE} "
        f"WHERE {fact_kind_filter} AND stock_code IN :codes "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        f"{event_filter}"
        "ORDER BY fact_kind, stock_code, scope_hash, revision_no"
    )
    statement = text("SELECT * " + coverage_scope_sql).bindparams(bindparam("codes", expanding=True))
    read_errors: list[str] = []
    bad: set[tuple[str, str]] = set()

    def coverage_chains():
        # A code stays in one chunk, so every revision of every scope is still
        # validated together. The snapshot matches the former single SELECT.
        batch_size = max(1, int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT))
        compressed = False
        try:
            with engine.connect() as connection:
                mysql = connection.dialect.name == "mysql"
                compressed = bool(
                    mysql and scan_event and finance_batch and event_batch
                    and event_batch.get("mode") != "HISTORICAL_RECONSTRUCTION"
                )
                snapshot_budget = nullcontext()
                if compressed:
                    from server.common.qmt_announcement_pit import (
                        VALIDATION_READ_BATCH_SIZE,
                        _live_announcement_read_hint,
                        _live_announcement_reply_budget,
                    )

                    batch_size = min(batch_size, VALIDATION_READ_BATCH_SIZE)
                    deadline = monotonic() + LIVE_EVENT_COVERAGE_READ_DEADLINE_SECONDS
                    snapshot_budget = _live_announcement_reply_budget(
                        connection, deadline=deadline, stage="common-cutoff-isolation",
                    )
                with snapshot_budget:
                    if mysql and str(connection.get_isolation_level() or "").upper() not in {
                        "REPEATABLE READ", "SERIALIZABLE",
                    }:
                        raise ValueError("common cutoff requires a repeatable-read snapshot")
                with connection.begin():
                    checkpoint = None
                    if compressed and event_checkpoint_path is not None:
                        from server.engine.strategy_governance import validate_privileged_trigger_migration_seal

                        # A checkpoint stands in only for payloads protected by
                        # the attested append-only schema on this database.
                        with _live_announcement_reply_budget(
                            connection, deadline=deadline, stage="common-cutoff-checkpoint-identity",
                        ):
                            try:
                                seal = validate_privileged_trigger_migration_seal(connection)
                            except RuntimeError:
                                logger.warning("Event coverage checkpoint identity is unavailable; verifying the full input")
                            else:
                                identity = {
                                    "version": EventCoverageCheckpoint.VERSION,
                                    "server": seal["trigger_inventory_server_uuid"],
                                    "database": seal["trigger_inventory_seal_database"],
                                    "contract": seal["pit_fact_schema_contract_hash"],
                                    "seal": seal["trigger_inventory_table_comment"],
                                }
                                checkpoint = EventCoverageCheckpoint(event_checkpoint_path, canonical_hash(identity))
                    if coverage_progress is not None:
                        coverage_progress(0, len(normalized_codes))
                    for offset in range(0, len(normalized_codes), batch_size):
                        parameters = {
                            **query_params,
                            "codes": normalized_codes[offset:offset + batch_size],
                        }
                        stage = f"common-cutoff-coverage:{offset}"
                        if checkpoint is not None:
                            verified, invalid = _read_verified_event_coverage_chunk(
                                connection, codes=parameters["codes"], source=parameters["event_source"],
                                decision_at=decision, deadline=deadline, stage=stage, checkpoint=checkpoint,
                            )
                            bad.update(("event", code) for code in invalid)
                            for scope, chain in verified.items():
                                yield ("event", chain[0]["stock_code"], scope), chain, False, True
                            if coverage_progress is not None:
                                coverage_progress(offset + len(parameters["codes"]), len(normalized_codes))
                            continue
                        chains: dict[
                            tuple[str, str, str], list[dict[str, Any]]
                        ] = defaultdict(list)
                        rows = (
                            nullcontext(_read_indexed_event_coverage_rows(
                                connection, codes=parameters["codes"], source=parameters["event_source"],
                                decision_at=decision, deadline=deadline, stage=stage,
                            ))
                            if compressed else connection.execute(statement, parameters)
                        )
                        with rows as result:
                            for raw in result if compressed else result.mappings():
                                row = dict(raw)
                                chains[(
                                    str(row.get("fact_kind") or "").lower(),
                                    str(row.get("stock_code") or "").zfill(6),
                                    str(row.get("scope_hash") or ""),
                                )].append(row)
                        for key, chain in chains.items():
                            yield key, chain, compressed, False
                        if coverage_progress is not None:
                            coverage_progress(offset + len(parameters["codes"]), len(normalized_codes))
                    if compressed:
                        _live_announcement_read_hint(
                            mysql=False, deadline=deadline, stage="common-cutoff-coverage-complete",
                        )
        except Exception as exc:
            reason = str(getattr(exc, "reason_code", "") or "")
            detail = str(getattr(exc, "detail", "") or "")
            read_errors.append(f"{reason}:{detail}" if compressed and reason else type(exc).__name__)

    candidates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    if exact_event_selection is not None:
        # The exact validator already checked the chosen batch and bound members.
        for code, (row, _) in exact_event_selection.members.items():
            candidates[("event", code)].append(dict(row))
    live_scope_heads: set[str] = set()
    required_finance_report = finance_disclosure_gate(
        windows["finance"][1]
    ).minimum_report_date
    chains = () if exact_event_selection is not None and finance_batch else coverage_chains()
    for (kind, code, _scope), chain, compressed, verified in chains:
        try:
            if compressed:
                chain = [
                    _coverage_payload_row(row, mysql=True, stage="common_cutoff_event_coverage")
                    for row in chain
                ]
            if not verified:
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
            if reusable_live_batch and latest is chain[-1]:
                live_scope_heads.add(str(latest["coverage_id"]))
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
    required_kinds = ("finance", "event") if include_event_facts else ("finance",)
    for kind in required_kinds:
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
    def covered_through(row: Mapping[str, Any]) -> datetime:
        return _row_datetime(
            row.get("_effective_covered_through_at") or row.get("covered_through_at")
        )

    source_cutoffs = {
        kind: min(covered_through(row) for row in selected if row["fact_kind"] == kind)
        for kind in required_kinds
    }
    fact_cutoff = min(source_cutoffs.values())

    def cutoff_for(row: Mapping[str, Any]) -> datetime:
        return source_cutoffs[row["fact_kind"]] if _dataset_cutoffs else fact_cutoff

    event_completion_at = None
    if signed_live_selection is None:
        _, event_completion_at = _late_cninfo_completion(
            engine, event_batch, fact_cutoff_at=source_cutoffs.get("event", fact_cutoff)
            if _dataset_cutoffs else fact_cutoff, decision_at=decision,
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
        and not (
            signed_live_selection is not None
            and row.get("fact_kind") == "event"
            and row.get("batch_id") == event_batch["batch_id"]
            and cutoff_for(row) == _row_datetime(event_batch["fact_cutoff_at"])
            <= _row_datetime(row["known_at"]) <= signed_live_selection.completion_at <= decision
        )
        and not _verified_cninfo_coverage_known(
            row, event_batch, fact_cutoff_at=cutoff_for(row), decision_at=decision,
            completion_known_at=event_completion_at,
        )
        and not _live_capture_allowed(
                fact_cutoff_at=cutoff_for(row),
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
    if _dataset_cutoffs:
        root_payload["schema"] = "probiga.strategy-pit-source-cutoffs.v1"
        root_payload["source_cutoff_at"] = {
            kind: _dt_text(cutoff) for kind, cutoff in source_cutoffs.items()
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
    if _dataset_cutoffs:
        result["source_cutoff_at"] = root_payload["source_cutoff_at"]
    if event_batch:
        result["qmt_event_batch"] = event_batch
    if finance_batch:
        result["finance_atomic_batch"] = {
            key: value
            for key, value in finance_batch.items()
            if key not in {"members", "rows"}
        }
        result.update({
            key: finance_batch[key]
            for key in (
                "data_excluded_count", "available_code_count",
                "data_excluded_codes", "data_excluded_reasons", "data_exclusions",
            )
        })
        if capture_finance_selection is not None and _dataset_cutoffs:
            exclusions = _finance_data_exclusions_from_baseline(
                finance_batch, codes=normalized_codes, target=windows["finance"][1],
            )
            capture_finance_selection(SealedFinanceInputs(
                engine=engine, decision_at=decision, fact_cutoff_at=source_cutoffs["finance"],
                as_of_date=windows["finance"][1], seal_id=finance_batch["seal_coverage_id"],
                batch_root_sha256=finance_batch["batch_root_sha256"],
                members=MappingProxyType({code: MappingProxyType(member) for code, member in finance_batch["members"].items()}),
                exclusions=MappingProxyType({code: MappingProxyType(item) for code, item in exclusions.items()}),
            ))
    if capture_historical_selection is not None:
        event_selection = exact_event_selection
        if event_selection is None and reusable_live_batch:
            event_selection = _LiveEventSelection(
                engine, decision, windows["event"][0], windows["event"][1],
                MappingProxyType(dict(event_batch)), MappingProxyType({
                    str(row["stock_code"]): (
                        MappingProxyType(dict(row)), str(row["coverage_id"]) in live_scope_heads,
                    )
                    for row in selected if row["fact_kind"] == "event"
                }),
            )
        if event_selection is not None:
            event_cutoff = source_cutoffs["event"] if _dataset_cutoffs else fact_cutoff
            capture_historical_selection(HistoricalEventRead(
                event_selection, event_cutoff, signed_historical_bindings,
            ))
    return result



def resolve_strategy_fact_cutoffs(engine: Engine, **inputs: Any) -> dict[str, Any]:
    """Seal each consumed source at its own proven frontier under one decision time."""
    return resolve_common_fact_cutoff(engine, _dataset_cutoffs=True, **inputs)


def load_finance_data_exclusions(
    engine: Engine, *, codes: Iterable[str], decision_at: datetime | str,
    as_of_date: date | str,
) -> dict[str, dict[str, Any]]:
    """Read the current sealed exclusions without materializing finance payloads."""
    decision = normalize_decision_at(decision_at)
    target = _date_value(as_of_date, required=True)
    baseline = load_latest_finance_atomic_batch_baseline(engine, decision_at=decision)
    return _finance_data_exclusions_from_baseline(baseline, codes=codes, target=target)


def _finance_data_exclusions_from_baseline(
    baseline: Mapping[str, Any], *, codes: Iterable[str], target: date,
) -> dict[str, dict[str, Any]]:
    """Project exclusions from an already validated exact-date baseline."""
    if not baseline or baseline.get("as_of_date") != target.isoformat():
        return {}
    requested = {str(code).strip().zfill(6) for code in codes}
    return {
        item["stock_code"]: {
            **item,
            "coverage_status": FINANCE_DATA_EXCLUDED_STATUS,
            "batch_root_sha256": baseline["batch_root_sha256"],
            "seal_coverage_id": baseline["seal_coverage_id"],
        }
        for item in baseline["data_exclusions"]
        if item["stock_code"] in requested
    }


def _finance_feature_fact(
    selected: Mapping[str, Any], *, fact_cutoff: datetime, decision: datetime,
) -> dict[str, Any]:
    newest_report = _date_value(selected.get("report_date"), required=True)
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
    return payload


def load_finance_facts(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None = None,
    as_of_date: date | str | None = None,
    read_progress: Callable[[int, int], None] | None = None,
    sealed_finance_inputs: SealedFinanceInputs | None = None,
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
    if sealed_finance_inputs is not None:
        from server.common.pit_sealed_finance_read import load_sealed_finance_facts

        return load_sealed_finance_facts(
            engine, selection=sealed_finance_inputs, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff, as_of_date=end_date,
            read_progress=read_progress,
        )
    try:
        data_exclusions = load_finance_data_exclusions(
            engine, codes=normalized_codes, decision_at=decision, as_of_date=end_date,
        )
    except Exception as exc:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason=(
                "PIT_FINANCE_SCHEMA_UNAVAILABLE:" if isinstance(exc, OperationalError)
                else "PIT_FINANCE_EXCLUSION_MANIFEST_INVALID:"
            ) + type(exc).__name__,
        )
    read_deadline = monotonic() + FINANCE_REVISION_MYSQL_DEADLINE_SECONDS
    revision_errors: list[str] = []

    def read_failure(exc: Exception, *, default_stage: str = "", override_reason: str = "") -> str:
        diagnostic = type(exc).__name__
        stage = getattr(exc, "_pit_read_stage", "") or getattr(exc, "detail", "")
        if not re.fullmatch(
            r"finance-(revision|empty-coverage|nonfiling)-(inventory|decode|size:[0-9]+|payload:[0-9]+)",
            str(stage),
        ):
            stage = default_stage
        if stage:
            diagnostic += f":stage={stage}"
        reason = override_reason or str(getattr(exc, "reason_code", "") or "")
        if reason in {
            "QMT_ANNOUNCEMENT_LIVE_BATCH_VALIDATION_DEADLINE",
            "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE",
            "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED",
        }:
            diagnostic += f":reason={reason}"
        return diagnostic

    def shared_deadline_failure(stage: str, error: Exception | None) -> PITFactBatch | None:
        deadline_reason = "QMT_ANNOUNCEMENT_LIVE_BATCH_VALIDATION_DEADLINE"
        if monotonic() < read_deadline and getattr(error, "reason_code", "") != deadline_reason:
            return None
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason="PIT_FINANCE_SCHEMA_UNAVAILABLE:" + read_failure(
                error if error is not None else TimeoutError(),
                default_stage=stage, override_reason=deadline_reason,
            ),
        )

    def revision_groups():
        # One historical finance scan held 264k rows (214 MiB of JSON) and
        # exceeded 980 MiB RSS. Keep all report/revision identities for a code
        # together, process it unchanged, then release the code chunk.
        batch_size = max(1, min(int(COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT), FINANCE_REVISION_MYSQL_CODE_LIMIT))
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
                            read_deadline=read_deadline,
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
                        if read_progress is not None:
                            read_progress(offset + len(code_batch), len(normalized_codes))
        except Exception as exc:
            revision_errors.append(read_failure(exc))

    codes_with_revisions: set[str] = set()
    selected_by_code: dict[str, dict[str, Any]] = {}
    ineligible_revision_reasons: dict[str, str] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code, identities in revision_groups():
        if identities:
            codes_with_revisions.add(code)
        if code in data_exclusions:
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = (
                "PIT_FINANCE_DATA_EXCLUDED:" + data_exclusions[code]["reason_code"]
            )
            continue
        if not identities:
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
            if selection_reasons:
                ineligible_revision_reasons[code] = min(selection_reasons)
            continue
        newest_report = max(
            _date_value(row.get("report_date"), required=True)
            for row in latest_by_identity
        )
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
        selected_by_code[code] = candidates[-1]
    if revision_errors:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason="PIT_FINANCE_SCHEMA_UNAVAILABLE:" + revision_errors[0],
        )

    # The revision iterator has released its snapshot before these reads, even
    # when the pool has just one connection.
    coverage_error = None
    try:
        empty_coverage, coverage_errors = _authoritative_empty_coverage(
            engine,
            fact_kind="finance",
            codes=normalized_codes,
            fact_cutoff_at=fact_cutoff,
            decision_at=decision,
            start_date=date(1900, 1, 1),
            end_date=end_date,
            read_deadline=read_deadline,
            codes_with_revisions=codes_with_revisions,
        )
    except Exception as exc:
        coverage_error = exc
        empty_coverage = {}
        coverage_errors = {
            code: f"PIT_FINANCE_COVERAGE_SCHEMA_UNAVAILABLE:{read_failure(exc)}"
            for code in normalized_codes
        }
    deadline_failure = shared_deadline_failure("finance-empty-coverage", coverage_error)
    if deadline_failure is not None:
        return deadline_failure
    required_report_date = finance_disclosure_gate(end_date).minimum_report_date
    nonfiling_error = None
    try:
        expected_unavailable, unavailable_errors = (
            load_finance_expected_unavailable(
                engine,
                codes=normalized_codes,
                decision_at=decision,
                expected_report_date=required_report_date,
                read_deadline=read_deadline,
            )
        )
    except Exception as exc:
        nonfiling_error = exc
        expected_unavailable = {}
        unavailable_errors = {
            code: (
                "PIT_FINANCE_UNAVAILABLE_SCHEMA_UNAVAILABLE:"
                f"{read_failure(exc)}"
            )
            for code in normalized_codes
        }
    deadline_failure = shared_deadline_failure("finance-nonfiling", nonfiling_error)
    if deadline_failure is not None:
        return deadline_failure
    coverage_errors.update(unavailable_errors)
    resolved_coverage = {**empty_coverage, **expected_unavailable, **data_exclusions}

    facts: dict[str, dict[str, Any]] = {}
    for code in normalized_codes:
        if code in statuses:
            continue
        selected = selected_by_code.get(code)
        if selected is None:
            if code in expected_unavailable:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            elif code in empty_coverage:
                statuses[code] = PIT_AVAILABLE
                reasons[code] = "PIT_FINANCE_AUTHORITATIVE_EMPTY"
            elif code in ineligible_revision_reasons:
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = ineligible_revision_reasons[code]
            elif code not in codes_with_revisions and "BAD_COVERAGE_CHAIN" in coverage_errors.get(code, ""):
                statuses[code] = PIT_DATA_BLOCKED
                reasons[code] = coverage_errors[code]
            else:
                statuses[code] = PIT_NO_ROWS
                reasons[code] = (
                    "PIT_FINANCE_NO_ROWS_AT_FACT_CUTOFF" if code in codes_with_revisions
                    else "PIT_FINANCE_NO_ROWS"
                )
            continue
        newest_report = _date_value(selected.get("report_date"), required=True)
        if code in expected_unavailable and newest_report < required_report_date:
            statuses[code] = PIT_AVAILABLE
            reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
            continue
        facts[code] = _finance_feature_fact(selected, fact_cutoff=fact_cutoff, decision=decision)
        statuses[code] = PIT_AVAILABLE
        reasons[code] = ""
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
        data_exclusions = load_finance_data_exclusions(
            engine, codes=normalized_codes, decision_at=decision, as_of_date=end_date,
        )
    except Exception as exc:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, fact_cutoff_at=fact_cutoff,
            reason=(
                "PIT_FINANCE_SCHEMA_UNAVAILABLE:" if isinstance(exc, OperationalError)
                else "PIT_FINANCE_EXCLUSION_MANIFEST_INVALID:"
            ) + type(exc).__name__,
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
    resolved_coverage = {**empty_coverage, **expected_unavailable, **data_exclusions}

    facts: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code in normalized_codes:
        if code in data_exclusions:
            facts[code] = []
            statuses[code] = PIT_DATA_BLOCKED
            reasons[code] = (
                "PIT_FINANCE_DATA_EXCLUDED:" + data_exclusions[code]["reason_code"]
            )
            continue
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


def _read_signed_historical_event_bindings(
    engine: Engine, selection: HistoricalAnnouncementSelection,
    *, event_progress: Callable[[int, int], None] | None = None,
) -> Mapping[str, tuple[Mapping[str, str], ...]]:
    from server.common.qmt_announcement_pit import (
        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS, VALIDATION_READ_BATCH_SIZE,
        _live_announcement_read_hint, _live_announcement_reply_budget,
    )

    by_id = {row["coverage_id"]: (code, row) for code, (row, _) in selection.members.items()}
    if len(by_id) != len(selection.members):
        raise ValueError("historical announcement bound coverage identity is duplicated")
    deadline = monotonic() + LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    result = {}
    coverage_count = selection.batch["coverage_count"]
    with engine.connect() as connection:
        mysql = connection.dialect.name == "mysql"
        ids = sorted(by_id)
        for offset in range(0, len(ids), VALIDATION_READ_BATCH_SIZE):
            selected_ids = ids[offset:offset + VALIDATION_READ_BATCH_SIZE]
            stage = f"signed-historical-bindings:{offset}"
            hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
            statement = text(
                f"SELECT {hint}coverage_id, JSON_EXTRACT(payload_json, '$.fact_bindings') AS bindings "
                f"FROM {SOURCE_COVERAGE_TABLE} WHERE coverage_id IN :ids"
            ).bindparams(bindparam("ids", expanding=True))
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
                rows = connection.execute(statement, {"ids": selected_ids}).mappings().all()
            if len(rows) != len(selected_ids) or {row["coverage_id"] for row in rows} != set(selected_ids):
                raise ValueError("historical announcement bound coverage is unavailable")
            for row in rows:
                code, expected = by_id[row["coverage_id"]]
                bindings = json.loads(row["bindings"])
                if (
                    not isinstance(bindings, list) or len(bindings) != expected["result_count"]
                    or any(not isinstance(item, dict) or set(item) != {"revision_id", "content_hash"}
                           or any(not isinstance(value, str) or not _HASH_RE.fullmatch(value)
                                  for value in item.values())
                           for item in bindings)
                    or canonical_hash({"schema": "probiga.pit-event-fact-set.v1", "bindings": bindings})
                    != expected["fact_set_hash"]
                ):
                    raise ValueError("historical announcement bound fact set differs")
                result[code] = tuple(MappingProxyType(item) for item in bindings)
            if event_progress is not None:
                event_progress(coverage_count + len(result), coverage_count + len(ids))
    return MappingProxyType(result)


def _query_signed_bound_event_revisions(
    engine: Engine, selection: _SignedLiveEventSelection | HistoricalAnnouncementSelection,
    bindings_by_code: Mapping[str, tuple[Mapping[str, str], ...]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Resolve only identities committed to the exact signed coverage receipt."""
    from server.common.qmt_announcement_pit import (
        LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS, _live_announcement_reply_budget,
    )

    deadline = monotonic() + LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    expected = {}
    for code, bindings in bindings_by_code.items():
        for binding in bindings:
            revision_id = binding["revision_id"]
            if revision_id in expected:
                raise ValueError("signed announcement fact binding is duplicated")
            expected[revision_id] = (code, binding["content_hash"])
    if not expected:
        return [], {}
    selected = {}
    histories = []
    params = {
        "known": _row_datetime(selection.batch["received_at"]),
        "source": selection.batch["source"],
    }
    with engine.connect() as connection:
        bound_statement = text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(15000) */ revision_id,content_hash,identity_hash,
                   stock_code,event_date,published_at,revision_no
            FROM {EVENT_REVISION_TABLE}
            WHERE revision_id IN :ids AND source=:source
              AND known_at<=:known AND received_at<=:known
        """).bindparams(bindparam("ids", expanding=True))
        ids = sorted(expected)
        limit = COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT
        for offset in range(0, len(ids), limit):
            requested = ids[offset:offset + limit]
            with _live_announcement_reply_budget(connection, deadline=deadline, stage="signed-announcement-bound-revisions"):
                observed = connection.execute(
                    bound_statement, {**params, "ids": requested},
                ).mappings().all()
            if {item["revision_id"] for item in observed} != set(requested):
                raise ValueError("signed announcement bound revision is missing")
            for row in observed:
                if (row["stock_code"], row["content_hash"]) != expected[row["revision_id"]]:
                    raise ValueError("signed announcement bound content differs")
                day = (
                    _row_datetime(row["published_at"]).date()
                    if row["published_at"] is not None else _date_value(row["event_date"], required=True)
                )
                if selection.window_start <= day <= selection.window_end:
                    identity = row["identity_hash"]
                    if identity in selected:
                        raise ValueError("signed announcement identity has conflicting bindings")
                    selected[identity] = row["revision_id"], int(row["revision_no"])
        history_statement = text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM {EVENT_REVISION_TABLE}
            WHERE identity_hash IN :ids AND source=:source
              AND known_at<=:known AND received_at<=:known
            ORDER BY stock_code,identity_hash,revision_no
        """).bindparams(bindparam("ids", expanding=True))
        identities = sorted(selected)
        for offset in range(0, len(identities), limit):
            requested = identities[offset:offset + limit]
            with _live_announcement_reply_budget(connection, deadline=deadline, stage="signed-announcement-bound-history"):
                for row in connection.execute(history_statement, {**params, "ids": requested}).mappings():
                    if row["revision_no"] <= selected[row["identity_hash"]][1]:
                        histories.append(dict(row))
    if set(selected) != {row["identity_hash"] for row in histories}:
        raise ValueError("signed announcement bound revision history is missing")
    return histories, {identity: binding[0] for identity, binding in selected.items()}


def load_event_facts(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    require_qmt_complete_batch: bool = False,
    historical_selection: HistoricalEventRead | None = None,
) -> PITFactBatch:
    normalized_codes = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    fact_cutoff, decision = _fact_and_decision_times(
        decision_at=decision_at, fact_cutoff_at=fact_cutoff_at,
    )
    start = _date_value(start_date, required=False)
    end = _date_value(end_date, required=False) or fact_cutoff.date()
    selection = (
        historical_selection.selection
        if require_qmt_complete_batch and historical_selection is not None
        else None
    )
    if not normalized_codes and selection is None:
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

            if selection is None:
                event_batch = validate_complete_announcement_batch(
                    engine,
                    codes=normalized_codes,
                    decision_at=decision,
                    fact_cutoff_at=fact_cutoff,
                    window_start=start or date(1900, 1, 1),
                    window_end=end,
                )
            elif (
                normalized_codes
                and selection.engine is engine
                and selection.decision_at == decision
                and selection.window_start == (start or date(1900, 1, 1))
                and selection.window_end == end
                and set(selection.members) == set(normalized_codes)
                and historical_selection.fact_cutoff_at == fact_cutoff
                and fact_cutoff <= _row_datetime(selection.batch["fact_cutoff_at"])
            ):
                event_batch = dict(selection.batch)
            else:
                return _blocked_batch(
                    table_name=EVENT_REVISION_TABLE,
                    codes=normalized_codes,
                    decision_at=decision,
                    fact_cutoff_at=fact_cutoff,
                    reason=("PIT_EVENT_VALIDATED_LIVE_SCOPE_MISMATCH"
                            if isinstance(selection, _LiveEventSelection)
                            else "PIT_EVENT_VALIDATED_HISTORY_SCOPE_MISMATCH"),
                    status=PIT_DATA_BLOCKED,
                )
        except Exception as exc:
            return _blocked_batch(
                table_name=EVENT_REVISION_TABLE,
                codes=normalized_codes,
                decision_at=decision,
                fact_cutoff_at=fact_cutoff,
                reason=_announcement_batch_read_reason(exc),
                status=PIT_DATA_BLOCKED,
            )
    event_completion_at = None
    if selection is None:
        requires_completion, event_completion_at = _late_cninfo_completion(
            engine, event_batch, fact_cutoff_at=fact_cutoff, decision_at=decision,
        )
        if requires_completion and event_completion_at is None:
            return _blocked_batch(
                table_name=EVENT_REVISION_TABLE,
                codes=normalized_codes,
                decision_at=decision,
                fact_cutoff_at=fact_cutoff,
                reason="PIT_EVENT_LATE_PUBLICATION_NOT_COMPLETED_AT_CUTOFF",
                status=PIT_DATA_BLOCKED,
            )
    bound_ids = {}
    signed_bindings = historical_selection.signed_historical_bindings if selection is not None else None
    if isinstance(selection, _SignedLiveEventSelection):
        signed_bindings = selection.bindings
    signed_event_received_at = None
    try:
        if signed_bindings is not None:
            if set(signed_bindings) != set(normalized_codes):
                raise ValueError("signed announcement bound stock scope differs")
            if (
                not isinstance(selection, _SignedLiveEventSelection)
                and event_batch.get("mode") == "HISTORICAL_RECONSTRUCTION"
                and event_batch.get("source") == CNINFO_EVENT_SOURCE
            ):
                received = _row_datetime(event_batch.get("received_at"))
                completed = _row_datetime(event_batch["initial_stage"]["completed_at"])
                if not fact_cutoff <= received <= completed <= decision:
                    raise ValueError("signed historical announcement knowledge frontier differs")
                signed_event_received_at = received
            rows, bound_ids = _query_signed_bound_event_revisions(engine, selection, signed_bindings)
        else:
            rows = _query_revisions(
                engine, table_name=EVENT_REVISION_TABLE, codes=normalized_codes,
                decision_at=decision, start_date=start, end_date=end,
                source=(str(event_batch.get("source") or "") if event_batch else ""),
                batch_id=str(event_batch.get("batch_id") or ""),
            )
    except Exception as exc:
        diagnostic = type(exc).__name__
        stage = str(getattr(exc, "_pit_read_stage", "") or getattr(exc, "detail", ""))
        if re.fullmatch(
            r"event-revision-(?:snapshot|complete|identities:[0-9]+|"
            r"history:[0-9]+(?:-(?:inventory|decode|size:[0-9]+|payload:[0-9]+))?)",
            stage,
        ):
            diagnostic += f":stage={stage}"
        reason = str(getattr(exc, "reason_code", "") or "")
        if reason in {
            "QMT_ANNOUNCEMENT_LIVE_BATCH_VALIDATION_DEADLINE",
            "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE",
            "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED",
        }:
            diagnostic += f":reason={reason}"
        return _blocked_batch(
            table_name=EVENT_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision,
            fact_cutoff_at=fact_cutoff,
            reason=f"PIT_EVENT_SCHEMA_UNAVAILABLE:{diagnostic}",
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
        if selection is None:
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
                verified_event_batch=event_batch,
                event_completion_at=event_completion_at,
            )
        else:
            coverage_errors = {}
            empty_coverage = {
                code: _empty_coverage_receipt(
                    row, fact_cutoff_at=fact_cutoff, decision_at=decision,
                )
                for code, (row, is_scope_head) in selection.members.items()
                if is_scope_head and (
                    (not by_code_identity.get(code) if signed_bindings is not None
                     else int(row["result_count"]) == 0)
                )
            }
    except Exception as exc:
        empty_coverage = {}
        reason = str(getattr(exc, "reason_code", "") or "")
        detail = str(getattr(exc, "detail", "") or "")
        diagnostic = f"{reason}:{detail}" if reason else type(exc).__name__
        coverage_errors = {
            code: f"PIT_EVENT_COVERAGE_SCHEMA_UNAVAILABLE:{diagnostic}"
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
            for identity, chain in identities.items():
                selected, selection_reason = _latest_revision_for_fact_cutoff(
                    chain,
                    fact_kind="event",
                    fact_cutoff_at=fact_cutoff,
                    decision_at=decision,
                    signed_event_received_at=signed_event_received_at,
                )
                if signed_bindings is not None and (
                    selected is None or selected["revision_id"] != bound_ids.get(identity)
                ):
                    blocked_reasons.append("PIT_EVENT_BOUND_REVISION_NOT_ELIGIBLE")
                    continue
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
    "EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR",
    "FINANCE_EXPECTED_UNAVAILABLE_STATUS",
    "FINANCE_DATA_EXCLUDED_STATUS", "build_finance_data_exclusion",
    "load_finance_data_exclusions",
    "PIT_AVAILABLE",
    "PIT_DATA_BLOCKED", "PIT_FACT_TABLE_DDLS", "PIT_FACT_TABLE_NAMES",
    "PIT_FACT_TRIGGER_STATEMENTS", "PIT_NO_ROWS", "PIT_SCHEMA_UNAVAILABLE",
    "PITCoverageReceipt", "PITFactBatch", "PITRevisionReceipt",
    "TIME_UNVERIFIED", "TIME_VERIFIED", "append_event_revision", "append_event_revisions",
    "append_finance_atomic_batch_seal", "append_finance_expected_unavailable",
    "append_finance_revision", "append_finance_revisions",
    "append_source_coverage", "append_source_coverages", "canonical_hash",
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
