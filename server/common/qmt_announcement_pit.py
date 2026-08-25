"""Fail-closed full-market QMT announcement point-in-time ingestion.

The official QMT ``announcement`` period is the only strategy-authoritative
announcement source.  Eastmoney remains a display cache.  A source run is
published only after every member of one immutable QMT stock-catalog batch has
been downloaded and read at the same request cutoff.  Per-code checkpoints are
staging data only; one database transaction makes the completed batch visible.

This module is deliberately DML-only.  Deployment owns all PIT and catalog
DDL/triggers; a runtime run refuses an absent or drifted schema.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


SHANGHAI = ZoneInfo("Asia/Shanghai")
QMT_ANNOUNCEMENT_SOURCE = "qmt.announcement"
QMT_ANNOUNCEMENT_PERIOD = "announcement"
QMT_ANNOUNCEMENT_BATCH_SCHEMA = "probiga.qmt-announcement-batch.v1"
QMT_ANNOUNCEMENT_CHECKPOINT_SCHEMA = (
    "probiga.qmt-announcement-checkpoint.v1"
)
QMT_ANNOUNCEMENT_RESULT_SCHEMA = "probiga.qmt-announcement-code-result.v1"
QMT_ANNOUNCEMENT_PREPARED_SCHEMA = "probiga.qmt-announcement-prepared.v1"
QMT_ANNOUNCEMENT_TASK_SCHEMA = "probiga.qmt-announcement-task-result.v1"
MAX_CAPTURE_DELAY = timedelta(minutes=30)
DEFAULT_WINDOW_DAYS = 30
DEFAULT_BATCH_SIZE = 100
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
class AnnouncementCatalog:
    batch_id: str
    manifest_hash: str
    member_set_hash: str
    codes: tuple[str, ...]
    qmt_by_code: Mapping[str, str]


def _shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(SHANGHAI).replace(tzinfo=None)


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


def parse_qmt_announcement_frame(
    *,
    stock_code: str,
    qmt_code: str,
    frame: Any,
    fact_cutoff_at: datetime,
    window_start: date,
) -> list[dict[str, Any]]:
    """Normalize one official ``stock -> DataFrame`` response."""

    code = str(stock_code or "").zfill(6)
    instrument = str(qmt_code or "").upper()
    if not _CODE_RE.fullmatch(code) or not _QMT_CODE_RE.fullmatch(instrument):
        raise ValueError("QMT announcement stock identity is invalid")
    if instrument[:6] != code:
        raise ValueError("QMT announcement stock/QMT identities differ")
    cutoff = _dt(fact_cutoff_at)
    events: dict[str, dict[str, Any]] = {}
    for index, raw_row in _frame_records(frame):
        row = {str(key): _json_safe(value) for key, value in raw_row.items()}
        published = _publication_from_record(index, raw_row)
        if published > cutoff:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_FUTURE_PUBLICATION",
                f"{instrument}:{_dt_text(published)}>{_dt_text(cutoff)}",
            )
        if published.date() < window_start:
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
            "qmt_code": instrument,
            "source_event_id": source_event_id,
            "published_at": _dt_text(published),
            "title": title,
        }
        event_key = (
            f"qmt:{source_event_id}"[:160]
            if source_event_id
            else f"qmt:{canonical_hash(identity_material)}"[:160]
        )
        payload = {
            "event_key": event_key,
            "stock_code": code,
            "qmt_code": instrument,
            "event_date": published.date().isoformat(),
            "published_at": _dt_text(published),
            "title": title[:1024],
            "source_event_id": source_event_id[:512],
            # Commit to the complete source row without duplicating a possibly
            # huge announcement body into both the revision and coverage
            # ledgers.  The bounded fields are sufficient for operator audit.
            "source_row_hash": canonical_hash(row),
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
    ) -> "AnnouncementCheckpoint":
        directory = root / batch_id
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
        }
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

    def load(self, stock_code: str) -> list[dict[str, Any]] | None:
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
            or not _SHA256_RE.fullmatch(payload_hash)
            or canonical_hash(unsigned) != payload_hash
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_MIXED_OR_TAMPERED",
                stock_code,
            )
        return [dict(item) for item in unsigned["events"]]

    def save(self, stock_code: str, events: Sequence[Mapping[str, Any]]) -> None:
        unsigned = {
            "schema": QMT_ANNOUNCEMENT_RESULT_SCHEMA,
            "batch_id": self.manifest["batch_id"],
            "fact_cutoff_at": self.manifest["fact_cutoff_at"],
            "catalog_batch_id": self.manifest["catalog_batch_id"],
            "stock_code": stock_code,
            "qmt_code": self.manifest["qmt_by_code"][stock_code],
            "events": [dict(item) for item in events],
        }
        _atomic_write(
            self._result_path(stock_code),
            {**unsigned, "payload_hash": canonical_hash(unsigned)},
        )

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
        self, *, batch_root_hash: str, received_at: datetime
    ) -> None:
        unsigned = {
            "schema": QMT_ANNOUNCEMENT_PREPARED_SCHEMA,
            "batch_id": self.manifest["batch_id"],
            "fact_cutoff_at": self.manifest["fact_cutoff_at"],
            "received_at": _dt_text(received_at),
            "batch_root_hash": str(batch_root_hash),
        }
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

    def mark_complete(self, *, batch_root_hash: str, received_at: datetime) -> None:
        _atomic_write(
            self.root / "complete.json",
            {
                "schema": "probiga.qmt-announcement-checkpoint-complete.v1",
                "batch_id": self.manifest["batch_id"],
                "fact_cutoff_at": self.manifest["fact_cutoff_at"],
                "received_at": _dt_text(received_at),
                "batch_root_hash": batch_root_hash,
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


def _find_resumable_checkpoint(
    engine: Engine,
    *,
    checkpoint_root: Path,
    observed_at: datetime,
    window_days: int,
) -> tuple[datetime, AnnouncementCatalog] | None:
    candidates: list[tuple[datetime, Path, dict[str, Any]]] = []
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
            or observed_at - cutoff > MAX_CAPTURE_DELAY
        ):
            continue
        candidates.append((cutoff, path, payload))
    candidates.sort(key=lambda item: item[0], reverse=True)
    for cutoff, _path, payload in candidates:
        try:
            catalog = _load_catalog(engine, cutoff)
        except Exception:
            continue
        expected_start = (
            cutoff.date() - timedelta(days=int(window_days))
        ).isoformat()
        if (
            payload.get("batch_id")
            and payload.get("window_start") == expected_start
            and payload.get("window_end") == cutoff.date().isoformat()
            and payload.get("catalog_batch_id") == catalog.batch_id
            and payload.get("catalog_manifest_hash") == catalog.manifest_hash
            and payload.get("catalog_member_set_hash") == catalog.member_set_hash
            and payload.get("stock_codes") == list(catalog.codes)
            and payload.get("qmt_by_code") == dict(catalog.qmt_by_code)
        ):
            return cutoff, catalog
    return None


def _qmt_time(value: datetime) -> str:
    return _shanghai_naive(value).strftime("%Y%m%d%H%M%S")


def _download_and_read(
    xtdata: Any,
    *,
    checkpoint: AnnouncementCheckpoint,
    catalog: AnnouncementCatalog,
    fact_cutoff_at: datetime,
    window_start: date,
    batch_size: int,
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
    for offset in range(0, len(pending), max(1, batch_size)):
        code_chunk = pending[offset:offset + max(1, batch_size)]
        qmt_chunk = [catalog.qmt_by_code[code] for code in code_chunk]
        try:
            for qmt_code in qmt_chunk:
                downloader(
                    qmt_code,
                    period=QMT_ANNOUNCEMENT_PERIOD,
                    start_time=_qmt_time(start_time),
                    end_time=_qmt_time(fact_cutoff_at),
                )
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
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
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
                )
            except QMTAnnouncementBlocked:
                raise
            except Exception as exc:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_ROW_INVALID",
                    f"{qmt_code}:{type(exc).__name__}",
                ) from exc
            checkpoint.save(code, events)
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
) -> tuple[str, list[dict[str, Any]]]:
    entries = [
        {
            "stock_code": code,
            "source_response_hash": _source_response_hash(results[code]),
            "result_count": len(results[code]),
        }
        for code in catalog.codes
    ]
    payload = {
        "schema": QMT_ANNOUNCEMENT_BATCH_SCHEMA,
        "source": QMT_ANNOUNCEMENT_SOURCE,
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
) -> str:
    normalized_entries = sorted(
        (
            {
                "stock_code": str(item.get("stock_code") or "").zfill(6),
                "source_response_hash": str(
                    item.get("source_response_hash") or ""
                ),
                "result_count": int(item.get("result_count") or 0),
            }
            for item in entries
        ),
        key=lambda item: item["stock_code"],
    )
    payload = {
        "schema": QMT_ANNOUNCEMENT_BATCH_SCHEMA,
        "source": QMT_ANNOUNCEMENT_SOURCE,
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
    return canonical_hash(payload)


def validate_complete_qmt_announcement_batch(
    engine: Engine,
    *,
    codes: Iterable[str],
    decision_at: datetime | str,
    window_start: date | str,
    window_end: date | str,
    fact_cutoff_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Prove one atomic, catalog-exact QMT event coverage batch.

    The caller's code set may be a subset (for one screen), but the persisted
    batch itself must contain exactly every catalog-eligible A-share, including
    Beijing instruments.  A partial batch, mixed cutoff or stale batch is never
    accepted.
    """

    from server.common.qmt_stock_catalog import load_stock_catalog

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
    statement = text(
        "SELECT * FROM st_pit_source_coverage "
        "WHERE fact_kind='event' AND source=:source "
        "AND stock_code IN :codes AND known_at<=:decision_at "
        "AND received_at<=:decision_at AND coverage_status='COMPLETE' "
        "AND window_start<=:window_start AND window_end>=:window_end "
        "ORDER BY known_at DESC, batch_id DESC"
    ).bindparams(bindparam("codes", expanding=True))
    try:
        with engine.connect() as connection:
            required_rows = [
                dict(row)
                for row in connection.execute(
                    statement,
                    {
                        "source": QMT_ANNOUNCEMENT_SOURCE,
                        "codes": requested_codes,
                        "decision_at": decision,
                        "window_start": start,
                        "window_end": end,
                    },
                ).mappings()
            ]
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
                    rows = [
                        dict(row)
                        for row in connection.execute(
                            text(
                                "SELECT * FROM st_pit_source_coverage "
                                "WHERE fact_kind='event' AND source=:source "
                                "AND batch_id=:batch_id ORDER BY stock_code"
                            ),
                            {
                                "source": QMT_ANNOUNCEMENT_SOURCE,
                                "batch_id": candidate,
                            },
                        ).mappings()
                    ]
                    if not rows:
                        raise ValueError("batch has no coverage rows")
                    if len({str(row["stock_code"]) for row in rows}) != len(rows):
                        raise ValueError("batch contains duplicate stock coverage")
                    payloads = []
                    for row in rows:
                        raw = str(row.get("payload_json") or "")
                        payload = json.loads(raw)
                        if canonical_json(payload) != raw:
                            raise ValueError("coverage payload is not canonical")
                        watermark = payload.get("watermark")
                        evidence = (
                            watermark.get("evidence")
                            if isinstance(watermark, dict)
                            else None
                        )
                        if (
                            not isinstance(evidence, dict)
                            or row.get("watermark_kind") != "QUERY_CUTOFF"
                            or watermark.get("kind") != "QUERY_CUTOFF"
                        ):
                            raise ValueError("batch lacks query-cutoff evidence")
                        payloads.append((row, payload, evidence))
                    first_evidence = payloads[0][2]
                    event_cutoff = _dt(first_evidence.get("fact_cutoff_at"))
                    received = _dt(first_evidence.get("received_at"))
                    evidence_decision = _dt(first_evidence.get("decision_at"))
                    if (
                        evidence_decision != received
                        or received > decision
                        or received < event_cutoff
                        or received - event_cutoff > MAX_CAPTURE_DELAY
                        or decision - event_cutoff < timedelta(0)
                        or decision - event_cutoff > MAX_CAPTURE_DELAY
                        or (
                            requested_cutoff is not None
                            and event_cutoff < requested_cutoff
                        )
                    ):
                        raise ValueError("batch cutoff/decision freshness differs")
                    catalog_batch_id = str(
                        first_evidence.get("catalog_batch_id") or ""
                    )
                    catalog = load_stock_catalog(
                        connection,
                        batch_id=catalog_batch_id,
                        decision_known_at=event_cutoff,
                    )
                    catalog_codes = catalog.eligible_codes(
                        event_cutoff.date().isoformat()
                    )
                    row_codes = [str(row["stock_code"]).zfill(6) for row in rows]
                    if row_codes != sorted(catalog_codes):
                        raise ValueError("batch stock set differs from catalog")
                    if not set(requested_codes).issubset(row_codes):
                        raise ValueError("required scope is absent from batch")
                    entries: list[dict[str, Any]] = []
                    root = str(first_evidence.get("global_batch_root_hash") or "")
                    for row, payload, evidence in payloads:
                        if (
                            str(evidence.get("global_batch_root_hash") or "") != root
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
                            or int(payload.get("result_count") or 0)
                            != int(row.get("result_count") or 0)
                        ):
                            raise ValueError("batch per-code evidence differs")
                        entries.append(
                            {
                                "stock_code": str(row["stock_code"]).zfill(6),
                                "source_response_hash": str(
                                    row.get("source_response_hash") or ""
                                ),
                                "result_count": int(row.get("result_count") or 0),
                            }
                        )
                    persisted_start = date.fromisoformat(
                        str(first_evidence.get("window_start"))[:10]
                    )
                    persisted_end = date.fromisoformat(
                        str(first_evidence.get("window_end"))[:10]
                    )
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
                    )
                    if not _SHA256_RE.fullmatch(root) or root != expected_root:
                        raise ValueError("batch global root differs")
                    return {
                        "schema": QMT_ANNOUNCEMENT_BATCH_SCHEMA,
                        "status": "COMPLETE",
                        "batch_id": candidate,
                        "batch_root_hash": root,
                        "fact_cutoff_at": _dt_text(event_cutoff),
                        "decision_at": _dt_text(received),
                        "received_at": _dt_text(received),
                        "window_start": persisted_start.isoformat(),
                        "window_end": persisted_end.isoformat(),
                        "catalog_batch_id": catalog.batch_id,
                        "catalog_manifest_hash": catalog.manifest_hash,
                        "catalog_member_set_hash": catalog.member_set_hash,
                        "catalog_member_count": len(catalog_codes),
                    }
                except Exception as exc:
                    failure_codes.append(type(exc).__name__)
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
) -> list[str]:
    from server.common.pit_facts import (
        append_event_revision,
        append_source_coverage,
        pit_fact_schema_health,
    )

    health = pit_fact_schema_health(engine)
    if not health.get("valid"):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_PIT_SCHEMA_NOT_PREPARED",
            str(health.get("status") or "NOT_READY"),
        )
    response_hashes = {
        str(item["stock_code"]): str(item["source_response_hash"])
        for item in entries
    }
    coverage_ids: list[str] = []

    def write_rows(connection: Any) -> None:
        for code in catalog.codes:
            source_rows = [dict(item) for item in results[code]]
            bindings: list[dict[str, Any]] = []
            for item in source_rows:
                receipt = append_event_revision(
                    connection,
                    item,
                    known_at=received_at,
                    received_at=received_at,
                    source=QMT_ANNOUNCEMENT_SOURCE,
                    batch_id=batch_id,
                )
                bindings.append(
                    {
                        "revision_id": receipt.revision_id,
                        "content_hash": receipt.content_hash,
                    }
                )
            coverage = append_source_coverage(
                connection,
                fact_kind="event",
                stock_code=code,
                window_start=window_start,
                window_end=window_end,
                known_at=received_at,
                received_at=received_at,
                covered_through_at=fact_cutoff_at,
                watermark_kind="QUERY_CUTOFF",
                watermark_evidence={
                    "provider": QMT_ANNOUNCEMENT_SOURCE,
                    "period": QMT_ANNOUNCEMENT_PERIOD,
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
                },
                source_rows=source_rows,
                fact_bindings=bindings,
                source=QMT_ANNOUNCEMENT_SOURCE,
                batch_id=batch_id,
            )
            if coverage.source_response_hash != response_hashes[code]:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_RESPONSE_HASH_DRIFT", code
                )
            coverage_ids.append(coverage.coverage_id)

    with engine.connect() as connection:
        lock_acquired = False
        if connection.dialect.name == "mysql":
            acquired = connection.execute(
                text("SELECT GET_LOCK('probiga:qmt-announcement-pit', 0)")
            ).scalar_one()
            if int(acquired or 0) != 1:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_PUBLISHER_ALREADY_RUNNING"
                )
            lock_acquired = True
            # GET_LOCK is connection-scoped, not transactional.  End the
            # implicit SELECT transaction before starting the atomic DML unit.
            connection.commit()
        try:
            with connection.begin():
                write_rows(connection)
        finally:
            if lock_acquired:
                try:
                    if connection.in_transaction():
                        connection.rollback()
                    connection.execute(
                        text("SELECT RELEASE_LOCK('probiga:qmt-announcement-pit')")
                    )
                    connection.commit()
                except Exception:
                    # Never return a pooled session that may still own the
                    # publisher lock.
                    connection.invalidate()
                    raise
    return coverage_ids


def synchronize_qmt_announcements(
    engine: Engine,
    *,
    xtdata: Any,
    checkpoint_root: Path,
    now_fn: Callable[[], datetime] = datetime.now,
    window_days: int = DEFAULT_WINDOW_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_capture_delay: timedelta = MAX_CAPTURE_DELAY,
    resume: bool = True,
) -> dict[str, Any]:
    """Capture and atomically publish one exact full-catalog QMT batch."""

    if not 20 <= int(window_days) <= 3660:
        raise ValueError("QMT announcement window_days must be 20..3660")
    if not 1 <= int(batch_size) <= 500:
        raise ValueError("QMT announcement batch_size must be 1..500")
    observed_at = _dt(now_fn())
    fact_cutoff = observed_at
    catalog = _load_catalog(engine, fact_cutoff)
    if resume:
        resumable = _find_resumable_checkpoint(
            engine,
            checkpoint_root=Path(checkpoint_root),
            observed_at=observed_at,
            window_days=int(window_days),
        )
        if resumable is not None:
            fact_cutoff, catalog = resumable
    window_end = fact_cutoff.date()
    window_start = window_end - timedelta(days=int(window_days))
    seed = {
        "schema": "probiga.qmt-announcement-batch-id.v1",
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    batch_id = (
        f"qmt-ann-{fact_cutoff.strftime('%Y%m%dT%H%M%S')}-"
        f"{canonical_hash(seed)[:16]}"
    )
    checkpoint = AnnouncementCheckpoint.open(
        Path(checkpoint_root),
        batch_id=batch_id,
        fact_cutoff_at=fact_cutoff,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        resume=resume,
    )
    try:
        connector = getattr(xtdata, "connect", None)
        if callable(connector):
            connector()
        results = _download_and_read(
            xtdata,
            checkpoint=checkpoint,
            catalog=catalog,
            fact_cutoff_at=fact_cutoff,
            window_start=window_start,
            batch_size=int(batch_size),
        )
        if len(catalog.codes) >= 100 and not any(results.values()):
            # QMT permission failures can present as one empty DataFrame per
            # requested symbol instead of raising.  A 30-day full A-share
            # market with zero announcements is not authoritative evidence;
            # keep every event-dependent strategy DATA_BLOCKED.
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN"
            )
        prepared = checkpoint.load_prepared_publish()
        received_at = prepared[0] if prepared is not None else _dt(now_fn())
        elapsed = received_at - fact_cutoff
        if elapsed < timedelta(0) or elapsed > max_capture_delay:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CAPTURE_EXCEEDED_30_MINUTES",
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
        coverage_ids = _publish_batch(
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
            "reason_code": "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE",
            "batch_id": batch_id,
            "batch_root_hash": batch_root,
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
            "capture_seconds": int(elapsed.total_seconds()),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except QMTAnnouncementBlocked as exc:
        checkpoint.diagnose(exc.reason_code, exc.detail)
        received = _dt(now_fn())
        return {
            "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
            "status": "DATA_BLOCKED",
            "reason_code": exc.reason_code,
            "detail": exc.detail,
            "batch_id": batch_id,
            "batch_root_hash": "",
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
            "window_end": window_end.isoformat(),
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
    if (
        decision != received
        or received < cutoff
        or int((received - cutoff).total_seconds())
        != int(payload["capture_seconds"])
    ):
        raise ValueError("QMT announcement task T/E timestamps differ")
    status = payload.get("status")
    if status == "COMPLETE":
        if (
            process_exit != 0
            or payload["stock_count"] <= 0
            or payload["coverage_count"] != payload["stock_count"]
            or payload["capture_seconds"] > int(MAX_CAPTURE_DELAY.total_seconds())
            or not _SHA256_RE.fullmatch(str(payload.get("batch_root_hash") or ""))
            or not str(payload.get("batch_id") or "").startswith("qmt-ann-")
        ):
            raise ValueError("QMT announcement COMPLETE result differs")
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
    "AnnouncementCatalog", "AnnouncementCheckpoint", "DEFAULT_BATCH_SIZE",
    "DEFAULT_WINDOW_DAYS", "MAX_CAPTURE_DELAY", "QMTAnnouncementBlocked",
    "QMT_ANNOUNCEMENT_BATCH_SCHEMA", "QMT_ANNOUNCEMENT_PERIOD",
    "QMT_ANNOUNCEMENT_SOURCE", "QMT_ANNOUNCEMENT_TASK_SCHEMA",
    "build_batch_root", "canonical_hash", "canonical_json",
    "parse_qmt_announcement_frame", "parse_qmt_publication_time",
    "synchronize_qmt_announcements", "validate_complete_qmt_announcement_batch",
    "validate_task_result",
]
