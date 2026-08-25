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
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import math
import re
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError


SHANGHAI = ZoneInfo("Asia/Shanghai")
FINANCE_REVISION_TABLE = "st_pit_finance_revision"
EVENT_REVISION_TABLE = "st_pit_event_revision"
SOURCE_COVERAGE_TABLE = "st_pit_source_coverage"
QMT_EVENT_SOURCE = "qmt.announcement"
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
    elif watermark_type == "QUERY_CUTOFF":
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
        if not (
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
        if (
            str(normalized_watermark_evidence.get("provider") or "")
            != "qmt.announcement"
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
    source_name = str(source or "UNKNOWN")[:64]
    if watermark_type == "QUERY_CUTOFF" and (
        kind != "event" or source_name != "qmt.announcement"
    ):
        raise ValueError("query-cutoff is reserved for QMT announcements")
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
    engine: Engine,
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
        with engine.connect() as connection:
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
    with engine.connect() as connection:
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


def _validate_coverage_chain(rows: list[dict[str, Any]]) -> None:
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
            or str(row.get("coverage_status") or "") != "COMPLETE"
        ):
            raise ValueError("PIT coverage knowledge/watermark chain is invalid")
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
        elif watermark_kind == "QUERY_CUTOFF":
            required_hashes = (
                "global_batch_root_hash", "catalog_manifest_hash",
                "catalog_member_set_hash",
            )
            if (
                str(row.get("fact_kind") or "") != "event"
                or str(row.get("source") or "") != "qmt.announcement"
                or str(evidence.get("provider") or "")
                != "qmt.announcement"
                or str(evidence.get("period") or "") != "announcement"
                or _row_datetime(evidence.get("fact_cutoff_at")) != covered
                or _row_datetime(evidence.get("decision_at")) != known
                or _row_datetime(evidence.get("received_at")) != known
                or str(evidence.get("query_end_time") or "")
                != covered.strftime("%Y%m%d%H%M%S")
                or str(evidence.get("source_response_hash") or "")
                != expected_response_hash
                or not (
                    timedelta(0)
                    <= known - covered
                    <= MAX_LIVE_CAPTURE_DELAY
                )
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
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                params,
            ).mappings()
        ]
    by_code_scope: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        by_code_scope[str(row.get("stock_code") or "").zfill(6)][
            str(row.get("scope_hash") or "")
        ].append(row)
    available: dict[str, dict[str, Any]] = {}
    invalid: dict[str, str] = {}
    for code in codes:
        candidates: list[dict[str, Any]] = []
        for chain in by_code_scope.get(code, {}).values():
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
                and _live_capture_allowed(
                    fact_cutoff_at=fact_cutoff_at,
                    decision_at=decision_at,
                    known_at=_row_datetime(latest.get("known_at")),
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
    event_batch: dict[str, Any] = {}
    if require_qmt_event_batch:
        try:
            from server.common.qmt_announcement_pit import (
                validate_complete_qmt_announcement_batch,
            )

            event_batch = validate_complete_qmt_announcement_batch(
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
                "event_source": "qmt.announcement",
            }
        )
    statement = text(
        f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
        "WHERE fact_kind IN ('finance','event') AND stock_code IN :codes "
        "AND known_at<=:decision_at AND received_at<=:decision_at "
        f"{event_filter}"
        "ORDER BY fact_kind, stock_code, scope_hash, revision_no"
    ).bindparams(bindparam("codes", expanding=True))
    try:
        with engine.connect() as connection:
            rows = [
                dict(row)
                for row in connection.execute(
                    statement,
                    query_params,
                ).mappings()
            ]
    except Exception as exc:
        return {
            **blocked,
            "reason": "PIT_COMMON_CUTOFF_SCHEMA_UNAVAILABLE:"
            f"{type(exc).__name__}",
        }
    chains: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        chains[(
            str(row.get("fact_kind") or "").lower(),
            str(row.get("stock_code") or "").zfill(6),
            str(row.get("scope_hash") or ""),
        )].append(row)
    candidates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    bad: set[tuple[str, str]] = set()
    for (kind, code, _scope), chain in chains.items():
        try:
            _validate_coverage_chain(chain)
        except ValueError:
            bad.add((kind, code))
            continue
        latest = chain[-1]
        start, end = windows[kind]
        if (
            str(latest.get("coverage_status") or "") == "COMPLETE"
            and (
                kind != "event"
                or not event_batch
                or str(latest.get("batch_id") or "")
                == str(event_batch.get("batch_id") or "")
            )
            and _date_value(latest.get("window_start"), required=True) <= start
            and _date_value(latest.get("window_end"), required=True) >= end
        ):
            candidates[(kind, code)].append(latest)
    selected: list[dict[str, Any]] = []
    missing: list[str] = []
    for kind in ("finance", "event"):
        for code in normalized_codes:
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
        _row_datetime(row.get("covered_through_at")) for row in selected
    )
    if any(
        not _live_capture_allowed(
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
                "watermark_hash": str(row.get("watermark_hash") or ""),
                "source_response_hash": str(row.get("source_response_hash") or ""),
                "batch_id": str(row.get("batch_id") or ""),
                "known_at": _dt_text(_row_datetime(row.get("known_at"))),
                "covered_through_at": _dt_text(
                    _row_datetime(row.get("covered_through_at"))
                ),
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
    try:
        rows = _query_revisions(
            engine, table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
            decision_at=decision, start_date=None, end_date=end_date,
        )
    except Exception as exc:
        return _blocked_batch(
            table_name=FINANCE_REVISION_TABLE, codes=normalized_codes,
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
    facts: dict[str, dict[str, Any]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code in normalized_codes:
        identities = by_code_identity.get(code, {})
        if not identities:
            if code in empty_coverage:
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
            if code in empty_coverage:
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
                "coverage": empty_coverage.get(code),
            }
            for code in normalized_codes
        },
    }
    return PITFactBatch(
        facts=facts, coverage_by_code=empty_coverage,
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

    facts: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for code in normalized_codes:
        identities = by_code_identity.get(code, {})
        if not identities:
            facts[code] = []
            if code in empty_coverage:
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
                "coverage": empty_coverage.get(code),
            }
            for code in normalized_codes
        },
    }
    return PITFactBatch(
        facts=facts,
        coverage_by_code=empty_coverage,
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
                validate_complete_qmt_announcement_batch,
            )

            event_batch = validate_complete_qmt_announcement_batch(
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
            source=(QMT_EVENT_SOURCE if event_batch else ""),
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
            source=(QMT_EVENT_SOURCE if event_batch else ""),
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
    "EVENT_REVISION_TABLE", "FINANCE_REVISION_TABLE", "SOURCE_COVERAGE_TABLE",
    "PIT_AVAILABLE",
    "PIT_DATA_BLOCKED", "PIT_FACT_TABLE_DDLS", "PIT_FACT_TABLE_NAMES",
    "PIT_FACT_TRIGGER_STATEMENTS", "PIT_NO_ROWS", "PIT_SCHEMA_UNAVAILABLE",
    "PITCoverageReceipt", "PITFactBatch", "PITRevisionReceipt",
    "TIME_UNVERIFIED", "TIME_VERIFIED", "append_event_revision",
    "append_finance_revision", "append_source_coverage", "canonical_hash",
    "canonical_json", "ensure_pit_fact_schema", "load_event_facts",
    "load_finance_facts", "load_finance_history_facts",
    "normalize_decision_at", "normalize_published_at",
    "pit_fact_schema_health", "preflight_pit_fact_schema",
    "resolve_common_fact_cutoff",
]
