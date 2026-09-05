#!/usr/bin/env python3
"""Repair recent Linux/provider/derived data gaps without inventing history.

The Windows QMT edge owns canonical stock/index/minute history.  This job starts
after that history is available and repairs only data that the Linux host can
reconstruct from date-bound source evidence.  It deliberately separates three
classes of data:

* exact historical partitions (source-labelled Eastmoney daily flow, sector heat,
  Eastmoney A-list, market overview);
* conditionally replayable derived partitions (analysis and Trading V3), which
  remain ``DATA_BLOCKED`` until their point-in-time facts are provable; and
* observation-time/current snapshots (hot ranks, news, finance coverage,
  dividends, concept current/minute, stock snapshot and simulation activation),
  which must be refreshed by their normal latest/full tasks and are never
  stamped onto an older date by this repair job.

Every run is bounded and resumable.  Exact partitions are read before any
publisher is called, publishers are atomic, every successful write is read
back, and the single machine receipt binds the immutable calendar, plan,
attempts and final canonical partition proofs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.daily_stock_universe import (
    DailyStockUniverse,
    load_daily_stock_universe,
    validate_daily_stock_coverage,
)
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.mysql_lock import (
    CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
    mysql_named_lock,
)
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)
from tools.env_config import load_project_env


RESULT_SCHEMA = "probiga.linux-recent-data-gap-repair-result.v1"
TASK_TYPE = "linux_recent_data_gap_repair"
EXECUTOR_OWNER = "linux_provider"
PROVIDER = "canonical_provider_and_derived"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")
DAILY_FLOW_PROVIDER_PREFIXES = frozenset({"00", "30", "60", "68"})
DAILY_FLOW_HISTORICAL_SOURCES = frozenset({"push2hist", "baidu"})
CLOSED_READY_TIME = time(18, 0)
LEDGER_SCHEMA = "probiga.linux-recent-data-gap-repair-ledger.v1"
DEFAULT_STATE_FILE = Path(
    "/var/lib/probiga/jobs/linux-recent-data-gap-repair-v1.json"
)
# Mirrored from the scheduler's bounded replay-evidence contract.  One result
# is emitted as one JSON line, so it must fit without truncation.
SCHEDULER_REPLAY_RECEIPT_LIMIT_BYTES = 24_000

# Date-major order is intentional.  Analysis on D+1 consumes earlier
# recommendation/performance evidence, so finishing one date before advancing
# preserves the same dependency ordering as the daily production pipeline.
HISTORICAL_DATASET_ORDER = (
    "stock_daily_flow",
    "sector_heat",
    "alist_daily",
    "alist_info",
    "market_overview",
    # These two replay products remain available only when explicitly
    # requested. Missing point-in-time finance/announcement observations
    # cannot be reconstructed honestly after their decision cutoff, so they
    # must not make the default release catch-up permanently non-convergent.
    "analysis_recommendations",
    "trading_v3_replay",
)
LATEST_MATERIALIZATION_ORDER = (
    "concept_kline",
    "stock_snapshot",
)
DATASET_ORDER = HISTORICAL_DATASET_ORDER + LATEST_MATERIALIZATION_ORDER
DEFAULT_DATASET_ORDER = tuple(
    dataset
    for dataset in DATASET_ORDER
    if dataset not in {"analysis_recommendations", "trading_v3_replay"}
)

DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "concept_kline": (),
    "stock_daily_flow": (),
    "sector_heat": (),
    "alist_daily": (),
    "alist_info": ("alist_daily",),
    "market_overview": (),
    # The legacy analysis consumes same-day flow and sector evidence.  Its
    # publisher performs an additional PIT finance/announcement cutoff gate.
    "analysis_recommendations": ("stock_daily_flow", "sector_heat"),
    # Keep both visible stock pools in dependency order.  A replay is always
    # execution-disabled and cannot materialize paper or real orders.
    "trading_v3_replay": ("analysis_recommendations",),
    # Eastmoney's live concept directory can prove only the latest closed
    # session; older directory membership must never be reconstructed from
    # today's f124 response. sm_stock_snapshot is also replace-all current
    # state. Only the latest closed date belongs in the plan for both.
    "stock_snapshot": ("stock_daily_flow",),
}


# This matrix is part of the signed receipt.  It prevents a later operator from
# mistaking a current/full refresh for an admissible historical replay.
CAPABILITY_POLICY: dict[str, dict[str, Any]] = {
    "concept_kline": {
        "mode": "LATEST_CLOSED_ONLY_WITH_EXACT_DIRECTORY",
        "safe": True,
        "reason": (
            "daily bars are historical, but the provider exposes only a live "
            "directory; only the latest closed partition can be proved exactly"
        ),
    },
    "stock_daily_flow": {
        "mode": "EXACT_HISTORICAL",
        "safe": True,
        "reason": (
            "target-date historical provider rows for every traded Shanghai, "
            "Shenzhen, ChiNext and STAR Market stock; Beijing flow is unavailable. "
            "Automatic repair uses Eastmoney only; complete existing Baidu "
            "partitions retain their own source without claiming equivalence"
        ),
    },
    "sector_heat": {
        "mode": "EXACT_HISTORICAL",
        "safe": True,
        "reason": "date-specific Eastmoney industry K-line and fixed L1/L2 inventory",
    },
    "alist_daily": {
        "mode": "EXACT_HISTORICAL",
        "safe": True,
        "reason": "date-filtered complete Eastmoney report plus immutable QMT catalog",
    },
    "alist_info": {
        "mode": "EXACT_HISTORICAL",
        "safe": True,
        "reason": "date-filtered complete details after exact daily-report prerequisite",
    },
    "market_overview": {
        "mode": "EXACT_HISTORICAL_DERIVED",
        "safe": True,
        "reason": "deterministic aggregate of exact target-date canonical daily K-lines",
    },
    "analysis_recommendations": {
        "mode": "CANONICAL_ANALYSIS_PUBLISHER_ONLY",
        "safe": False,
        "reason": (
            "the mutable strategy partition may be written only by a scheduler-"
            "bound canonical analysis producer and its activation transaction"
        ),
    },
    "trading_v3_replay": {
        "mode": "PIT_CONDITIONAL_REPLAY_ONLY",
        "safe": True,
        "reason": "historical as-of is execution-disabled and requires nonempty exact forecasts",
    },
    "stock_snapshot": {
        "mode": "LATEST_CLOSED_MATERIALIZATION_ONLY",
        "safe": True,
        "reason": "sm_stock_snapshot is replace-all current state, not a historical ledger",
    },
    "sim_signal_prepare": {
        "mode": "NEXT_SESSION_MATERIALIZATION_ONLY",
        "safe": False,
        "reason": "signals belong to the next/current trading session and are not historical facts",
    },
    "concept_current": {
        "mode": "LATEST_SNAPSHOT_ONLY",
        "safe": False,
        "reason": "Eastmoney f124 is observation-time evidence",
    },
    "concept_minute": {
        "mode": "LATEST_SESSION_ONLY",
        "safe": False,
        "reason": "the provider endpoint exposes only the latest rolling minute window",
    },
    "concept_flow_snapshot": {
        "mode": "LATEST_SNAPSHOT_ONLY",
        "safe": False,
        "reason": "provider observation cannot be relabelled as an older snapshot",
    },
    "hot_ths": {
        "mode": "LATEST_SNAPSHOT_ONLY",
        "safe": False,
        "reason": "THS hot endpoints accept no provider-side historical date",
    },
    "hot_fused": {
        "mode": "DERIVE_ONLY_FROM_EXISTING_EXACT_DATE_SOURCES",
        "safe": False,
        "reason": "missing observation-time source ranks cannot be reconstructed",
    },
    "news": {
        "mode": "LATEST_ROLLING_APPEND_ONLY",
        "safe": False,
        "reason": "current rolling pages do not prove historical completeness or received-at time",
    },
    "finance": {
        "mode": "CURRENT_FULL_UNIVERSE",
        "safe": False,
        "reason": "past report rows may be filled, but old knowledge/received-at cannot be recreated",
    },
    "dividend": {
        "mode": "CURRENT_FULL_UNIVERSE_SNAPSHOT",
        "safe": False,
        "reason": "full provider history is collected now and cannot establish old PIT knowledge",
    },
}


class LinuxGapRepairBlocked(RuntimeError):
    """Fail-closed repair condition with an explicit retry disposition."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = bool(retryable)


@dataclass(frozen=True, order=True)
class PartitionRef:
    trade_date: str
    dataset: str

    @property
    def partition_id(self) -> str:
        return f"{self.dataset}:{self.trade_date}"


@dataclass(frozen=True)
class AuthorityWindow:
    sessions: tuple[str, ...]
    batch_id: str
    manifest_hash: str
    source_session_set_hash: str

    def receipt(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "manifest_hash": self.manifest_hash,
            "source_session_set_hash": self.source_session_set_hash,
            "selected_session_set_hash": _digest(list(self.sessions)),
        }


class ProofLedger:
    """Small no-follow, atomic, hash-bound proof ledger for resumability."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.entries: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _payload(entries: Mapping[str, Mapping[str, Any]], updated_at: datetime) -> dict[str, Any]:
        normalized = {
            str(partition_id): dict(entries[partition_id])
            for partition_id in sorted(entries)
        }
        payload = {
            "schema": LEDGER_SCHEMA,
            "task_type": TASK_TYPE,
            "updated_at": _timestamp(updated_at),
            "entries": normalized,
            "entry_count": len(normalized),
            "entry_root_sha256": _partition_root(normalized),
        }
        payload["ledger_sha256"] = _digest(payload)
        return payload

    def load(self) -> dict[str, dict[str, Any]]:
        if self.path.is_symlink():
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state file must not be a symlink",
                retryable=False,
            )
        if not self.path.exists():
            self.entries = {}
            return {}
        metadata = self.path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state file is not a regular no-follow file",
                retryable=False,
            )
        if os.name != "nt" and stat.S_IMODE(metadata.st_mode) != 0o600:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state file mode must be 0600",
                retryable=False,
            )
        if metadata.st_size <= 0 or metadata.st_size > 1_000_000:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state file size is invalid",
                retryable=False,
            )
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state file is unreadable",
                retryable=False,
            ) from exc
        if not isinstance(payload, Mapping):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state payload is malformed",
                retryable=False,
            )
        unsigned = dict(payload)
        supplied = unsigned.pop("ledger_sha256", None)
        entries = payload.get("entries")
        if (
            supplied != _digest(unsigned)
            or payload.get("schema") != LEDGER_SCHEMA
            or payload.get("task_type") != TASK_TYPE
            or not isinstance(entries, Mapping)
            or int(payload.get("entry_count") or -1) != len(entries)
            or payload.get("entry_root_sha256") != _partition_root(entries)
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state hash/counters differ",
                retryable=False,
            )
        self.entries = {
            str(partition_id): dict(proof)
            for partition_id, proof in entries.items()
            if isinstance(proof, Mapping)
        }
        if len(self.entries) != len(entries):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state proof is malformed",
                retryable=False,
            )
        for partition_id, proof in self.entries.items():
            try:
                dataset, trade_date = partition_id.split(":", 1)
                partition = PartitionRef(_iso_date(trade_date), dataset)
                if partition.partition_id != partition_id or dataset not in DATASET_ORDER:
                    raise ValueError("partition identity differs")
                _validate_partition_proof(partition, proof)
            except Exception as exc:
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: Linux repair state partition proof differs",
                    retryable=False,
                ) from exc
        return dict(self.entries)

    def record(
        self,
        partition: PartitionRef,
        proof: Mapping[str, Any],
        *,
        now: datetime,
    ) -> None:
        verified = _validate_partition_proof(partition, proof)
        parent = self.path.parent
        if not parent.exists():
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state directory is not provisioned",
                retryable=False,
            )
        parent_metadata = parent.lstat()
        if stat.S_ISLNK(parent_metadata.st_mode) or not stat.S_ISDIR(parent_metadata.st_mode):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Linux repair state directory is not a real directory",
                retryable=False,
            )
        entries = dict(self.entries)
        entries[partition.partition_id] = verified
        encoded = (_canonical_json(self._payload(entries, now)) + "\n").encode("utf-8")
        temporary = parent / f".{self.path.name}.{os.getpid()}.{hashlib.sha256(encoded).hexdigest()[:12]}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = None
        try:
            descriptor = os.open(temporary, flags, 0o600)
            os.write(descriptor, encoded)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, self.path)
            if os.name != "nt":
                os.chmod(self.path, 0o600, follow_symlinks=False)
            if os.name != "nt":
                directory_descriptor = os.open(parent, os.O_RDONLY)
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        self.entries = entries


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _signed(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["result_sha256"] = _digest(result)
    return result


def _iso_date(value: Any, *, field: str = "trade_date") -> str:
    raw = str(value or "").strip()[:10]
    try:
        normalized = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise LinuxGapRepairBlocked(
            f"DATA_BLOCKED: {field} is invalid",
            retryable=False,
        ) from exc
    if raw != normalized:
        raise LinuxGapRepairBlocked(
            f"DATA_BLOCKED: {field} is invalid",
            retryable=False,
        )
    return normalized


def _as_shanghai(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI, microsecond=0)
    return value.astimezone(SHANGHAI).replace(microsecond=0)


def _timestamp(value: datetime) -> str:
    return _as_shanghai(value).isoformat(timespec="seconds")


def _parse_timestamp(value: Any, *, field: str) -> datetime:
    try:
        return _as_shanghai(
            datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise ValueError(f"{field} is not an ISO timestamp") from exc


def _build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if SHA40.fullmatch(value) is None or value == "0" * 40:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: exact Linux repair build SHA is unavailable",
            retryable=False,
        )
    return value


def validate_executor(
    *,
    platform_name: str | None = None,
    executor_role: str | None = None,
    task_type: str | None = None,
) -> None:
    platform = os.name if platform_name is None else str(platform_name)
    role = (
        str(os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or "").strip()
        if executor_role is None
        else str(executor_role).strip()
    )
    observed_task = (
        str(os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or "").strip()
        if task_type is None
        else str(task_type).strip()
    )
    if platform == "nt" or role != EXECUTOR_OWNER:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: recent provider/derived repair is Linux-provider-only",
            retryable=False,
        )
    if observed_task and observed_task != TASK_TYPE:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: scheduler task identity differs from Linux gap repair",
            retryable=False,
        )


def load_recent_closed_window(
    engine: Any,
    *,
    now: datetime,
    lookback_sessions: int,
    batch_id: str | None = None,
) -> AuthorityWindow:
    count = int(lookback_sessions)
    if count <= 0 or count > 60:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: lookback session count is outside 1..60",
            retryable=False,
        )
    current = _as_shanghai(now)
    cutoff = current.date()
    if current.time().replace(tzinfo=None) < CLOSED_READY_TIME:
        cutoff -= timedelta(days=1)
    start = cutoff - timedelta(days=max(45, count * 4 + 20))
    validate_trade_calendar_runtime_schema(engine)
    try:
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start.isoformat(),
                end_date=cutoff.isoformat(),
                decision_known_at=current.replace(tzinfo=None),
                batch_id=batch_id,
            )
    except Exception as exc:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: immutable QMT calendar does not cover Linux repair window"
        ) from exc
    sessions = tuple(
        receipt.sessions_between(start.isoformat(), cutoff.isoformat())[-count:]
    )
    if len(sessions) != count:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: immutable QMT calendar lacks requested closed sessions"
        )
    return AuthorityWindow(
        sessions=sessions,
        batch_id=str(receipt.batch_id),
        manifest_hash=str(receipt.manifest_hash),
        source_session_set_hash=str(receipt.session_set_hash),
    )


def _normalize_datasets(values: Iterable[str]) -> tuple[str, ...]:
    requested = {str(value or "").strip() for value in values}
    unknown = sorted(requested - set(DATASET_ORDER))
    if unknown:
        raise ValueError(f"unsupported Linux repair datasets: {unknown}")
    pending = list(requested)
    while pending:
        dataset = pending.pop()
        for dependency in DEPENDENCIES[dataset]:
            if dependency not in requested:
                requested.add(dependency)
                pending.append(dependency)
    return tuple(dataset for dataset in DATASET_ORDER if dataset in requested)


def build_plan(
    window: AuthorityWindow,
    datasets: Sequence[str],
) -> list[PartitionRef]:
    selected = _normalize_datasets(datasets)
    plan: list[PartitionRef] = []
    for trade_date in window.sessions:
        for dataset in HISTORICAL_DATASET_ORDER:
            if dataset in selected:
                plan.append(PartitionRef(trade_date, dataset))
    latest = window.sessions[-1]
    for dataset in LATEST_MATERIALIZATION_ORDER:
        if dataset in selected:
            plan.append(PartitionRef(latest, dataset))
    return plan


def _canonical_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float):
        return format(Decimal(str(value)).normalize(), "f")
    return str(value)


def _canonical_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    columns: Sequence[str],
) -> list[dict[str, Any]]:
    return [
        {column: _canonical_scalar(row.get(column)) for column in columns}
        for row in rows
    ]


def _mapping_rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


def _code_set_hash(codes: Iterable[Any]) -> str:
    normalized = sorted({str(value or "").strip().zfill(6) for value in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _proof(
    partition: PartitionRef,
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str],
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    canonical = _canonical_rows(rows, columns=columns)
    if not canonical:
        raise LinuxGapRepairBlocked(
            f"DATA_BLOCKED: {partition.partition_id} is empty"
        )
    return {
        "dataset": partition.dataset,
        "trade_date": partition.trade_date,
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
        "authority": dict(authority),
        "authority_sha256": _digest(authority),
    }


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value)).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LinuxGapRepairBlocked(
            f"DATA_BLOCKED: {field} is not numeric"
        ) from exc
    if not result.is_finite():
        raise LinuxGapRepairBlocked(f"DATA_BLOCKED: {field} is not finite")
    return result


@dataclass(frozen=True)
class DailyContext:
    universe: DailyStockUniverse
    kline_rows: tuple[dict[str, Any], ...]

    @property
    def traded_codes(self) -> tuple[str, ...]:
        result: list[str] = []
        for row in self.kline_rows:
            volume = _decimal(row.get("volume"), field="daily volume")
            amount = _decimal(row.get("amount"), field="daily amount")
            if volume != 0 or amount != 0:
                result.append(str(row["stock_code"]).zfill(6))
        return tuple(sorted(result))


class ProductionPartitionInspector:
    """Pure-SELECT validation for persisted provider/derived partitions."""

    def __init__(
        self,
        primary_engine: Any,
        history_engine: Any,
        *,
        decision_time: datetime,
        expected_build_sha: str,
        prior_proofs: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self.primary_engine = primary_engine
        self.history_engine = history_engine
        self.decision_time = _as_shanghai(decision_time)
        self.expected_build_sha = expected_build_sha
        self._daily_cache: dict[str, DailyContext] = {}
        self._runtime_concept_receipts: dict[str, dict[str, Any]] = {}
        self._prior_proofs: dict[str, dict[str, Any]] | None = (
            {
                str(partition_id): dict(proof)
                for partition_id, proof in (prior_proofs or {}).items()
            }
            if prior_proofs is not None
            else None
        )

    def record_concept_receipt(
        self,
        partition: PartitionRef,
        receipt: Mapping[str, Any],
    ) -> None:
        self._runtime_concept_receipts[partition.partition_id] = dict(receipt)

    def _load_prior_proofs(self) -> dict[str, dict[str, Any]]:
        if self._prior_proofs is not None:
            return self._prior_proofs
        # Scheduler history deliberately retains only proof hashes so that its
        # replayable machine receipt stays below 24 KB.  Full concept-directory
        # authority is available only from the no-follow 0600 proof ledger.
        self._prior_proofs = {}
        return self._prior_proofs

    def __call__(self, partition: PartitionRef) -> dict[str, Any]:
        method = getattr(self, f"_{partition.dataset}", None)
        if method is None:
            raise LinuxGapRepairBlocked(
                f"DATA_BLOCKED: no inspector for {partition.dataset}",
                retryable=False,
            )
        return method(partition)

    def _daily(self, trade_date: str) -> DailyContext:
        cached = self._daily_cache.get(trade_date)
        if cached is not None:
            return cached
        universe = load_daily_stock_universe(
            self.primary_engine,
            trade_date,
            decision_known_at=self.decision_time.replace(tzinfo=None),
        )
        with self.history_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT stock_code,volume,amount FROM sm_stock_kline "
                        "WHERE trade_date=:trade_date AND k_type=1 AND adjust_type=0 "
                        "ORDER BY stock_code"
                    ),
                    {"trade_date": trade_date},
                )
            )
        validate_daily_stock_coverage(universe, kline_rows=rows)
        context = DailyContext(universe=universe, kline_rows=tuple(rows))
        self._daily_cache[trade_date] = context
        return context

    @staticmethod
    def _daily_authority(context: DailyContext) -> dict[str, Any]:
        universe = context.universe
        return {
            "catalog_batch_id": universe.catalog_batch_id,
            "catalog_manifest_hash": universe.catalog_manifest_hash,
            "catalog_member_set_hash": universe.catalog_member_set_hash,
            "catalog_captured_at": universe.catalog_captured_at,
            "catalog_history_complete_from": universe.catalog_history_complete_from,
            "catalog_knowledge_mode": universe.catalog_knowledge_mode,
            "expected_code_count": universe.expected_count,
            "expected_code_set_hash": universe.expected_code_set_hash,
        }

    def _stock_daily_flow(self, partition: PartitionRef) -> dict[str, Any]:
        context = self._daily(partition.trade_date)
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT stock_code,trade_date,main_net_inflow,max_net_inflow,"
                        "lg_net_inflow,mid_net_inflow,sm_net_inflow,data_source "
                        "FROM sm_stock_capital_flow_daily "
                        "WHERE trade_date=:trade_date ORDER BY stock_code"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
        validate_daily_stock_coverage(
            context.universe,
            kline_rows=context.kline_rows,
        )
        expected_codes = tuple(
            code
            for code in context.traded_codes
            if code[:2] in DAILY_FLOW_PROVIDER_PREFIXES
        )
        # Older all-market partitions may also contain Beijing or no-trade
        # rows. Preserve them, but do not include them in this provider's
        # supported traded-universe proof.
        expected_set = set(expected_codes)
        outside_expected_count = sum(
            str(row.get("stock_code") or "").zfill(6) not in expected_set
            for row in rows
        )
        rows = [
            row for row in rows
            if str(row.get("stock_code") or "").zfill(6) in expected_set
        ]
        codes = [str(row.get("stock_code") or "").zfill(6) for row in rows]
        if tuple(codes) != expected_codes:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: daily-flow partition differs from the exact "
                "provider-supported traded universe"
            )
        for row in rows:
            maximum = _decimal(row.get("max_net_inflow"), field="max_net_inflow")
            large = _decimal(row.get("lg_net_inflow"), field="lg_net_inflow")
            main = _decimal(row.get("main_net_inflow"), field="main_net_inflow")
            middle = _decimal(row.get("mid_net_inflow"), field="mid_net_inflow")
            small = _decimal(row.get("sm_net_inflow"), field="sm_net_inflow")
            tolerance = max(
                max(abs(value) for value in (main, maximum, large, middle, small))
                * Decimal("0.001"),
                Decimal("1000000"),
            )
            if (
                abs(main - maximum - large) > tolerance
                or abs(main + middle + small) > tolerance
            ):
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: daily-flow bucket accounting differs"
                )
            if (
                str(row.get("data_source") or "").strip().lower()
                not in DAILY_FLOW_HISTORICAL_SOURCES
            ):
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: daily-flow historical provider differs"
                )
        sources = {str(row["data_source"]).strip().lower() for row in rows}
        if len(sources) > 1:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: daily-flow partition mixes provider bucket semantics",
                retryable=False,
            )
        authority = {
            **self._daily_authority(context),
            "provider_supported_traded_code_count": len(expected_codes),
            "provider_supported_traded_code_set_hash": _code_set_hash(expected_codes),
            "provider_supported_prefixes": sorted(DAILY_FLOW_PROVIDER_PREFIXES),
            "excluded_beijing_traded_code_count": (
                len(context.traded_codes) - len(expected_codes)
            ),
            "historical_sources": sorted(DAILY_FLOW_HISTORICAL_SOURCES),
            "observed_sources": sorted(sources),
            "outside_expected_row_count": outside_expected_count,
        }
        return _proof(
            partition,
            rows,
            columns=(
                "stock_code",
                "trade_date",
                "main_net_inflow",
                "max_net_inflow",
                "lg_net_inflow",
                "mid_net_inflow",
                "sm_net_inflow",
                "data_source",
            ),
            authority=authority,
        )

    def _market_overview(self, partition: PartitionRef) -> dict[str, Any]:
        context = self._daily(partition.trade_date)
        columns = (
            "trade_date",
            "up_cnt",
            "down_cnt",
            "sideline_cnt",
            "total",
            "total_amount",
            "small_up_cnt",
            "small_total",
            "small_avg_chg",
            "source_table",
            "quality_status",
        )
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT " + ",".join(columns) + " "
                        "FROM sm_market_overview_daily "
                        "WHERE trade_date=:trade_date"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
        if (
            len(rows) != 1
            or int(rows[0].get("total") or 0) != context.universe.expected_count
            or str(rows[0].get("source_table") or "") != "sm_stock_kline"
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: market overview is absent or differs from daily universe"
            )
        return _proof(
            partition,
            rows,
            columns=columns,
            authority=self._daily_authority(context),
        )

    def _stock_snapshot(self, partition: PartitionRef) -> dict[str, Any]:
        context = self._daily(partition.trade_date)
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT stock_code,trade_date,price,close,main_net_inflow,"
                        "max_net_inflow,lg_net_inflow,mid_net_inflow,sm_net_inflow "
                        "FROM sm_stock_snapshot ORDER BY stock_code"
                    )
                )
            )
        codes = [str(row.get("stock_code") or "").zfill(6) for row in rows]
        dates = {str(row.get("trade_date") or "")[:10] for row in rows}
        if (
            tuple(codes) != context.universe.expected_codes
            or dates != {partition.trade_date}
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: current stock snapshot differs from latest closed universe"
            )
        return _proof(
            partition,
            rows,
            columns=(
                "stock_code",
                "trade_date",
                "price",
                "close",
                "main_net_inflow",
                "max_net_inflow",
                "lg_net_inflow",
                "mid_net_inflow",
                "sm_net_inflow",
            ),
            authority=self._daily_authority(context),
        )

    def _analysis_recommendations(self, partition: PartitionRef) -> dict[str, Any]:
        context = self._daily(partition.trade_date)
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT stock_code,analysis_date,recommend_status,"
                        "data_quality_score,data_quality_flags,model_version "
                        "FROM stock_analysis_result "
                        "WHERE analysis_date=:trade_date ORDER BY stock_code"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
            recommendations = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT stock_code,pick_date,recommend_status,"
                        "final_trade_score,primary_strategy,model_version "
                        "FROM st_recommended_stocks "
                        "WHERE pick_date=:trade_date ORDER BY stock_code"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
        codes = tuple(str(row.get("stock_code") or "").zfill(6) for row in rows)
        blocked_flags = [
            str(row.get("stock_code") or "")
            for row in rows
            if "data_blocked" in str(row.get("data_quality_flags") or "").lower()
            or not str(row.get("recommend_status") or "").strip()
        ]
        if codes != context.universe.expected_codes or blocked_flags:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: analysis lacks exact PIT/full-universe output"
            )
        recommendation_codes = [
            str(row.get("stock_code") or "").zfill(6) for row in recommendations
        ]
        if len(recommendation_codes) != len(set(recommendation_codes)):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: recommendation output has duplicate stock identities"
            )
        combined = [
            {
                "scope": "analysis",
                **row,
            }
            for row in rows
        ] + [
            {
                "scope": "recommendation",
                **row,
            }
            for row in recommendations
        ]
        return _proof(
            partition,
            combined,
            columns=(
                "scope",
                "stock_code",
                "analysis_date",
                "pick_date",
                "recommend_status",
                "data_quality_score",
                "data_quality_flags",
                "final_trade_score",
                "primary_strategy",
                "model_version",
            ),
            authority={
                **self._daily_authority(context),
                "analysis_code_count": len(rows),
                "recommendation_count": len(recommendations),
                "recommendation_code_set_hash": _code_set_hash(recommendation_codes),
                "pit_status": "AVAILABLE",
            },
        )

    def _trading_v3_replay(self, partition: PartitionRef) -> dict[str, Any]:
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT run_uid,trade_date,requested_as_of,decision_at,mode,"
                        "model_version,status,forecast_count,target_count,"
                        "data_snapshot_hash,result_hash,portfolio_json "
                        "FROM st_decision_run_v3 "
                        "WHERE requested_as_of=:trade_date AND mode='close' "
                        "ORDER BY decision_at DESC LIMIT 1"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
        if len(rows) != 1:
            raise LinuxGapRepairBlocked("DATA_BLOCKED: Trading V3 replay is missing")
        row = rows[0]
        try:
            portfolio = json.loads(str(row.get("portfolio_json") or "{}"))
        except json.JSONDecodeError as exc:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Trading V3 portfolio receipt is malformed",
                retryable=False,
            ) from exc
        truth = portfolio.get("decision_truth") if isinstance(portfolio, Mapping) else {}
        if (
            str(row.get("status") or "").upper() != "COMPLETED"
            or int(row.get("forecast_count") or 0) <= 0
            or str((truth or {}).get("actionable_status") or "").upper()
            != "REPLAY_ONLY"
            or str((truth or {}).get("paper_order_authority") or "").upper() != "NONE"
            or str((truth or {}).get("order_authority") or "").lower() not in {"false", "0"}
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Trading V3 historical result is not a completed replay-only run"
            )
        return _proof(
            partition,
            rows,
            columns=(
                "run_uid",
                "trade_date",
                "requested_as_of",
                "decision_at",
                "mode",
                "model_version",
                "status",
                "forecast_count",
                "target_count",
                "data_snapshot_hash",
                "result_hash",
                "portfolio_json",
            ),
            authority={
                "execution_enabled": False,
                "actionable_status": "REPLAY_ONLY",
                "automatic_order_submission": False,
            },
        )

    def _concept_kline(self, partition: PartitionRef) -> dict[str, Any]:
        # The table does not carry a target-date directory receipt.  A bare row
        # count would silently accept a current directory stamped onto history.
        # Exact runs are therefore recognized only through a successful,
        # hash-bound scheduler receipt for this exact target date.
        from tools import sync_eastmoney_concept_market as concept

        receipt = self._runtime_concept_receipts.get(partition.partition_id)
        dataset: Mapping[str, Any] = {}
        directory: Mapping[str, Any] = {}
        prior = self._load_prior_proofs().get(partition.partition_id)
        if receipt is not None:
            dataset = dict(receipt.get("dataset_results") or {}).get("kline") or {}
            directory = receipt.get("directory") or {}
        elif prior is None:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: exact target-date concept directory receipt is unavailable"
            )
        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        "SELECT index_code,trade_date,k_type,open,close,high,low,"
                        "volume,amount,change,change_pct "
                        "FROM sm_concept_east_kline "
                        "WHERE trade_date=:trade_date AND k_type=1 "
                        "ORDER BY index_code"
                    ),
                    {"trade_date": partition.trade_date},
                )
            )
        codes = [str(row.get("index_code") or "") for row in rows]
        if receipt is not None:
            if (
                int(dataset.get("row_count") or 0) != len(rows)
                or int(dataset.get("code_count") or 0) != len(codes)
                or dataset.get("code_set_sha256") != concept._code_set_hash(codes)
                or dataset.get("code_set_sha256") != directory.get("code_set_sha256")
            ):
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: persisted concept K-line differs from exact directory receipt"
                )
            authority = {
                "source_receipt_sha256": receipt["result_sha256"],
                "directory_manifest_sha256": directory.get("manifest_sha256"),
                "directory_code_set_sha256": directory.get("code_set_sha256"),
            }
        else:
            assert prior is not None
            authority = dict(prior.get("authority") or {})
            if (
                SHA64.fullmatch(str(authority.get("source_receipt_sha256") or ""))
                is None
                or SHA64.fullmatch(
                    str(authority.get("directory_manifest_sha256") or "")
                )
                is None
                or authority.get("directory_code_set_sha256")
                != concept._code_set_hash(codes)
            ):
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: prior concept directory authority differs",
                    retryable=False,
                )
        current = _proof(
            partition,
            rows,
            columns=(
                "index_code",
                "trade_date",
                "k_type",
                "open",
                "close",
                "high",
                "low",
                "volume",
                "amount",
                "change",
                "change_pct",
            ),
            authority=authority,
        )
        if prior is not None and _digest(current) != _digest(prior):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: persisted concept partition differs from prior exact proof"
            )
        return current

    def _sector_heat(self, partition: PartitionRef) -> dict[str, Any]:
        from tools.fetch_sector_heat_east_daily import (
            _select_sector_rows,
            validate_formal_sector_rows,
        )

        with self.primary_engine.connect() as connection:
            rows = _select_sector_rows(connection, partition.trade_date)
        evidence = validate_formal_sector_rows(
            rows,
            target_date=partition.trade_date,
            raw_count=len([row for row in rows if int(row["plate_type"]) == 4]),
        )
        return _proof(
            partition,
            rows,
            columns=(
                "snapshot_date",
                "plate_type",
                "rank",
                "concept_code",
                "concept_name",
                "change_pct",
                "hot_value",
                "hot_tag",
            ),
            authority={
                "provider": "eastmoney",
                "fixed_inventory_hash": evidence["row_hash"],
                "l1_count": evidence["l1_count"],
                "l2_count": evidence["l2_count"],
            },
        )

    def _alist(self, partition: PartitionRef, dataset: str) -> dict[str, Any]:
        from tools import sync_eastmoney_alist_exact as alist

        with self.primary_engine.connect() as connection:
            rows = alist._read_partition(
                connection,
                dataset=dataset,
                trade_date=partition.trade_date,
            )
        proof = alist.partition_proof(rows, dataset=dataset)
        if dataset == "daily" and int(proof.get("row_count") or 0) == 0:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: empty A-list daily partition needs its authoritative-empty receipt"
            )
        return {
            "dataset": partition.dataset,
            "trade_date": partition.trade_date,
            "row_count": max(1, int(proof.get("row_count") or 0)),
            "row_hash": str(proof.get("row_hash") or _digest([])),
            "authority": {
                "provider": alist.PROVIDER_ID,
                "database_proof": proof,
            },
            "authority_sha256": _digest(proof),
        }

    def _alist_daily(self, partition: PartitionRef) -> dict[str, Any]:
        return self._alist(partition, "daily")

    def _alist_info(self, partition: PartitionRef) -> dict[str, Any]:
        return self._alist(partition, "info")


def _daily_flow_rows_from_minute_close(
    rows: Sequence[Mapping[str, Any]],
    *,
    trade_date: str,
    traded_codes: Sequence[str],
    etl_sync_at: datetime,
) -> list[dict[str, Any]]:
    """Normalize exact native-QMT 15:00 cumulative rows into daily rows."""

    expected = tuple(sorted(str(code).zfill(6) for code in traded_codes))
    if len(expected) != len(set(expected)) or not expected:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: exact daily-flow traded universe is empty or duplicated"
        )
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        code = str(raw.get("stock_code") or "").strip().zfill(6)
        trade_time = str(raw.get("trade_time") or "").replace("T", " ")[:19]
        if trade_time != f"{trade_date} 15:00:00":
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: daily-flow source is not the exact 15:00 cumulative row"
            )
        maximum = _decimal(raw.get("max_net_inflow"), field="max_net_inflow")
        large = _decimal(raw.get("lg_net_inflow"), field="lg_net_inflow")
        middle = _decimal(raw.get("mid_net_inflow"), field="mid_net_inflow")
        small = _decimal(raw.get("sm_net_inflow"), field="sm_net_inflow")
        main = _decimal(raw.get("main_net_inflow"), field="main_net_inflow")
        if main != maximum + large or abs(main + middle + small) > Decimal("0.01"):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: native-QMT close flow accounting differs"
            )
        if (
            str(raw.get("data_source") or "") != "gj_qmt_transactioncount1m"
            or str(raw.get("quality_status") or "") != "QMT_NATIVE_EXACT"
            or str(raw.get("permission_status") or "") != "SUPPORTED"
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: minute-flow close lacks native exact provenance"
            )
        normalized.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                "main_net_inflow": main,
                "max_net_inflow": maximum,
                "lg_net_inflow": large,
                "mid_net_inflow": middle,
                "sm_net_inflow": small,
                "etl_sync_at": etl_sync_at.replace(tzinfo=None, microsecond=0),
                "data_source": "gj_qmt_transactioncount1m_close",
            }
        )
    normalized.sort(key=lambda row: row["stock_code"])
    actual = tuple(row["stock_code"] for row in normalized)
    if actual != expected:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: exact minute-flow close set differs from traded universe"
        )
    return normalized


def _daily_flow_database_rows(connection: Any, trade_date: str) -> list[dict[str, Any]]:
    return _mapping_rows(
        connection.execute(
            text(
                "SELECT stock_code,trade_date,main_net_inflow,max_net_inflow,"
                "lg_net_inflow,mid_net_inflow,sm_net_inflow,data_source "
                "FROM sm_stock_capital_flow_daily "
                "WHERE trade_date=:trade_date ORDER BY stock_code"
            ),
            {"trade_date": trade_date},
        )
    )


def publish_daily_flow_from_exact_minute(
    primary_engine: Any,
    minute_engine: Any,
    *,
    trade_date: str,
    now: datetime,
) -> dict[str, Any]:
    """Atomically replace one daily-flow partition from exact QMT minute truth."""

    from tools import sync_qmt_minute_flow_exact as minute_flow

    current = _as_shanghai(now)
    universe = minute_flow.load_flow_universe(
        primary_engine,
        trade_date=trade_date,
        now=current,
    )
    with minute_engine.connect() as connection:
        close_rows = _mapping_rows(
            connection.execute(
                text(
                    "SELECT stock_code,trade_time,main_net_inflow,max_net_inflow,"
                    "lg_net_inflow,mid_net_inflow,sm_net_inflow,data_source,"
                    "quality_status,permission_status "
                    "FROM sm_stock_capital_flow_min "
                    "WHERE trade_time=:close_time ORDER BY stock_code"
                ),
                {"close_time": f"{trade_date} 15:00:00"},
            )
        )
    daily_rows = _daily_flow_rows_from_minute_close(
        close_rows,
        trade_date=trade_date,
        traded_codes=tuple(universe.qmt_by_stock),
        etl_sync_at=current,
    )
    stable_columns = (
        "stock_code",
        "trade_date",
        "main_net_inflow",
        "max_net_inflow",
        "lg_net_inflow",
        "mid_net_inflow",
        "sm_net_inflow",
        "data_source",
    )
    expected_rows = _canonical_rows(daily_rows, columns=stable_columns)
    close_columns = (
        "stock_code",
        "trade_time",
        "main_net_inflow",
        "max_net_inflow",
        "lg_net_inflow",
        "mid_net_inflow",
        "sm_net_inflow",
        "data_source",
        "quality_status",
        "permission_status",
    )
    canonical_close_rows = _canonical_rows(close_rows, columns=close_columns)
    minute_close_proof = {
        "dataset": "stock_minute_capital_flow",
        "trade_date": trade_date,
        "close_time": f"{trade_date} 15:00:00",
        "row_count": len(canonical_close_rows),
        "code_count": len(daily_rows),
        "code_set_hash": _code_set_hash(
            row["stock_code"] for row in daily_rows
        ),
        "row_hash": _digest(canonical_close_rows),
        "data_source": "gj_qmt_transactioncount1m",
        "quality_status": "QMT_NATIVE_EXACT",
        "permission_status": "SUPPORTED",
    }
    insert = text(
        "INSERT INTO sm_stock_capital_flow_daily "
        "(stock_code,trade_date,main_net_inflow,max_net_inflow,lg_net_inflow,"
        "mid_net_inflow,sm_net_inflow,etl_sync_at,data_source) VALUES "
        "(:stock_code,:trade_date,:main_net_inflow,:max_net_inflow,:lg_net_inflow,"
        ":mid_net_inflow,:sm_net_inflow,:etl_sync_at,:data_source)"
    )
    with mysql_named_lock(
        primary_engine,
        CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        timeout_seconds=30,
    ) as connection:
        if connection.in_transaction():
            connection.commit()
        with connection.begin():
            connection.execute(
                text(
                    "DELETE FROM sm_stock_capital_flow_daily "
                    "WHERE trade_date=:trade_date"
                ),
                {"trade_date": trade_date},
            )
            for offset in range(0, len(daily_rows), 1000):
                connection.execute(insert, daily_rows[offset : offset + 1000])
            transaction_rows = _canonical_rows(
                _daily_flow_database_rows(connection, trade_date),
                columns=stable_columns,
            )
            if transaction_rows != expected_rows:
                raise LinuxGapRepairBlocked(
                    "DATA_BLOCKED: daily-flow transaction readback differs"
                )
    with primary_engine.connect() as connection:
        committed_rows = _canonical_rows(
            _daily_flow_database_rows(connection, trade_date),
            columns=stable_columns,
        )
    if committed_rows != expected_rows:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: committed daily-flow readback differs"
        )
    return {
        "source_schema": minute_flow.RESULT_SCHEMA,
        "source_status": "PASS",
        "source_receipt_sha256": _digest(
            {
                "trade_date": trade_date,
                "universe": universe.receipt(),
                "minute_close_database": minute_close_proof,
                "daily_row_hash": _digest(expected_rows),
            }
        ),
        "automatic_order_submission": False,
    }


@contextmanager
def _inner_task(task_type: str):
    key = "PROBIGA_SCHEDULER_TASK_TYPE"
    previous = os.environ.get(key)
    os.environ[key] = task_type
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


class ProductionPartitionPublisher:
    """Adapters around exact publishers; no historical current-snapshot fallback."""

    def __init__(
        self,
        primary_engine: Any,
        history_engine: Any,
        minute_engine: Any,
        *,
        expected_build_sha: str,
        now: datetime,
        concept_receipt_sink: (
            Callable[[PartitionRef, Mapping[str, Any]], None] | None
        ) = None,
        flow_evidence_root: Path | None = None,
    ) -> None:
        self.primary_engine = primary_engine
        self.history_engine = history_engine
        self.minute_engine = minute_engine
        self.expected_build_sha = expected_build_sha
        self.now = _as_shanghai(now)
        self.concept_receipt_sink = concept_receipt_sink
        self.flow_evidence_root = flow_evidence_root or DEFAULT_STATE_FILE.parent

    def __call__(self, partition: PartitionRef) -> dict[str, Any]:
        method = getattr(self, f"_{partition.dataset}", None)
        if method is None:
            raise LinuxGapRepairBlocked(
                f"DATA_BLOCKED: no exact publisher for {partition.dataset}",
                retryable=False,
            )
        result = dict(method(partition))
        if (
            SHA64.fullmatch(str(result.get("source_receipt_sha256") or "")) is None
            or result.get("automatic_order_submission") is not False
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: provider summary is not hash-bound/non-ordering",
                retryable=False,
            )
        return result

    def _stock_daily_flow(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import backfill_screener_history_inputs as backfill

        inspector = ProductionPartitionInspector(
            self.primary_engine,
            self.history_engine,
            decision_time=self.now,
            expected_build_sha=self.expected_build_sha,
        )
        # Unlike the provider response, the independently validated catalog
        # and complete daily bars are authority for the expected stock set.
        context = inspector._daily(partition.trade_date)
        expected_codes = {
            code for code in context.traded_codes
            if code[:2] in DAILY_FLOW_PROVIDER_PREFIXES
        }
        if not expected_codes:
            raise LinuxGapRepairBlocked("DATA_BLOCKED: daily-flow traded universe is empty")
        try:
            proof = inspector(partition)
        except LinuxGapRepairBlocked:
            proof = None
        if proof is not None:
            return {
                "source_schema": "probiga.existing-daily-flow.v1",
                "source_status": "PASS",
                "source_receipt_sha256": _digest(proof),
                "automatic_order_submission": False,
                "reused_existing": True,
            }
        expected, _missing, _invalid, _targets, existing = backfill._flow_gap_plan(
            self.primary_engine, self.history_engine,
            partition.trade_date, partition.trade_date,
        )
        if expected != {(code, partition.trade_date) for code in expected_codes}:
            raise LinuxGapRepairBlocked("DATA_BLOCKED: provider gap plan differs from daily universe")
        sources = {
            str(row.get("data_source") or "").strip().lower()
            for key, row in existing.items() if key in expected
        }
        if sources - {backfill.FLOW_SOURCE}:
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: partial daily-flow partition requires an explicit same-provider repair; "
                "Eastmoney cannot be mixed with existing provider semantics",
                retryable=False,
            )
        # This root is the already provisioned/verified job-ledger directory.
        # The reused writer saves preimages before correcting invalid rows,
        # uses the shared flow freeze lock, and keeps successful partial fetches
        # so the next attempt requests only the remaining gaps.
        evidence_dir = Path(tempfile.mkdtemp(
            prefix=f"flow-{partition.trade_date}-", dir=self.flow_evidence_root,
        ))
        report = backfill.backfill_flow(
            self.primary_engine, self.history_engine,
            partition.trade_date, partition.trade_date,
            workers=8, evidence_dir=evidence_dir, dry_run=False, baidu_only=False,
            required_existing_source=backfill.FLOW_SOURCE,
        )
        (evidence_dir / "manifest.json").write_text(
            _canonical_json(report) + "\n", encoding="utf-8",
        )
        if (
            report.get("status") != "COMPLETE"
            or report.get("unresolved_pair_count")
            or report.get("unresolved_prerequisite_count")
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: historical Eastmoney daily-flow gaps remain; "
                f"see {evidence_dir / 'manifest.json'}"
            )
        # Do not promote a provider's successful HTTP response into a PASS.
        # Verify committed values and the exact independent stock set again.
        proof = ProductionPartitionInspector(
            self.primary_engine, self.history_engine,
            decision_time=self.now, expected_build_sha=self.expected_build_sha,
        )(partition)
        return {
            "source_schema": "probiga.historical-eastmoney-daily-flow.v1",
            "source_status": "PASS",
            "source_receipt_sha256": _digest({"report": report, "proof": proof}),
            "automatic_order_submission": False,
        }

    def _market_overview(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import refresh_market_overview_daily as overview

        universe = load_daily_stock_universe(
            self.primary_engine,
            partition.trade_date,
            decision_known_at=self.now.replace(tzinfo=None),
        )
        with self.history_engine.connect() as kline_connection:
            with self.primary_engine.begin() as connection:
                result = overview.refresh_one(
                    connection,
                    kline_connection,
                    partition.trade_date,
                    universe=universe,
                )
        return {
            "source_schema": "probiga.market-overview-derived.v1",
            "source_status": "PASS",
            "source_receipt_sha256": _digest(result),
            "automatic_order_submission": False,
        }

    def _stock_snapshot(self, partition: PartitionRef) -> dict[str, Any]:
        from biz.stock_market import sync_stock_snapshot as snapshot

        frame = snapshot.fetch_snapshot(self.primary_engine, partition.trade_date)
        snapshot.write_snapshot(self.primary_engine, frame)
        return {
            "source_schema": "probiga.stock-snapshot-materialization.v1",
            "source_status": "PASS",
            "source_receipt_sha256": _digest(
                {
                    "trade_date": partition.trade_date,
                    "row_count": len(frame),
                    "coverage": frame.attrs.get("coverage_audit") or {},
                }
            ),
            "automatic_order_submission": False,
        }

    def _sector_heat(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import fetch_sector_heat_east_daily as sector

        receipt = sector.fetch_sector_heat_east_daily(
            partition.trade_date,
            formal=True,
            engine=self.primary_engine,
            now=self.now.replace(tzinfo=None),
        )
        if (
            receipt.get("status") != "PASS"
            or receipt.get("data_date") != partition.trade_date
            or SHA64.fullmatch(str(receipt.get("receipt_id") or "")) is None
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: exact historical sector receipt differs"
            )
        return {
            "source_schema": sector.FORMAL_RESULT_SCHEMA,
            "source_status": "PASS",
            "source_receipt_sha256": receipt["receipt_id"],
            "automatic_order_submission": False,
        }

    def _alist(self, partition: PartitionRef, dataset: str) -> dict[str, Any]:
        from tools import sync_eastmoney_alist_exact as alist

        with _inner_task(alist.TASK_TYPES[dataset]):
            receipt = alist.run_sync(
                self.primary_engine,
                dataset=dataset,
                trade_date=partition.trade_date,
                expected_build_sha=self.expected_build_sha,
                apply=True,
                now=self.now,
            )
        if (
            alist.validate_task_result(receipt, 0) != "complete"
            or receipt.get("trade_date") != partition.trade_date
        ):
            raise LinuxGapRepairBlocked("DATA_BLOCKED: exact A-list receipt differs")
        return {
            "source_schema": alist.RESULT_SCHEMA,
            "source_status": "PASS",
            "source_receipt_sha256": receipt["receipt_id"],
            "automatic_order_submission": False,
        }

    def _alist_daily(self, partition: PartitionRef) -> dict[str, Any]:
        return self._alist(partition, "daily")

    def _alist_info(self, partition: PartitionRef) -> dict[str, Any]:
        return self._alist(partition, "info")

    def _concept_kline(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import sync_eastmoney_concept_market as concept

        # The live directory must itself claim this exact target session.  This
        # allows the latest closed session to be repaired, while older gaps are
        # truthfully blocked instead of reusing today's membership.
        result = concept.run_publisher(
            self.primary_engine,
            concept.EastmoneyConceptProvider(),
            datasets=("kline",),
            trade_date=partition.trade_date,
            workers=concept.DEFAULT_WORKERS,
            dry_run=False,
            now=self.now,
        )
        receipt = concept.build_receipt(
            status="PASS",
            datasets=("kline",),
            started_at=self.now,
            finished_at=datetime.now(SHANGHAI).replace(microsecond=0),
            result=result,
            requested_trade_date=partition.trade_date,
        )
        if self.concept_receipt_sink is not None:
            self.concept_receipt_sink(partition, receipt)
        return {
            "source_schema": concept.RECEIPT_SCHEMA,
            "source_status": "PASS",
            "source_receipt_sha256": receipt["result_sha256"],
            "automatic_order_submission": False,
        }

    def _analysis_recommendations(self, partition: PartitionRef) -> dict[str, Any]:
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: analysis partition repair is delegated to the "
            "scheduler-bound canonical analysis publisher",
            retryable=False,
        )

    def _trading_v3_replay(self, partition: PartitionRef) -> dict[str, Any]:
        from server.trading_v3.decision_worker import run_daily_decision_v3

        decision_at = datetime.combine(
            date.fromisoformat(partition.trade_date),
            time(16, 5),
        )
        result = run_daily_decision_v3(
            self.primary_engine,
            as_of=date.fromisoformat(partition.trade_date),
            decision_at=decision_at,
            mode="close",
            universe_limit=1200,
            per_sleeve_limit=300,
            kline_engine=self.history_engine,
            execution_enabled=False,
        )
        if (
            result.get("status") != "ok"
            or result.get("run_status") != "COMPLETED"
            or result.get("actionable_status") != "REPLAY_ONLY"
            or int(result.get("paper_order_count") or 0) != 0
            or result.get("real_trading_enabled") is not False
        ):
            raise LinuxGapRepairBlocked(
                "DATA_BLOCKED: Trading V3 historical replay is not complete/non-ordering"
            )
        return {
            "source_schema": result["schema"],
            "source_status": "PASS",
            "source_receipt_sha256": _digest(result),
            "automatic_order_submission": False,
        }


def _inspection_failure(exc: BaseException) -> dict[str, Any]:
    return {
        "error_type": type(exc).__name__,
        "error_sha256": _digest(str(exc)),
        "retryable": bool(getattr(exc, "retryable", True)),
    }


def _validate_partition_proof(
    partition: PartitionRef,
    proof: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(proof)
    authority = result.get("authority")
    if (
        result.get("dataset") != partition.dataset
        or result.get("trade_date") != partition.trade_date
        or int(result.get("row_count") or 0) <= 0
        or SHA64.fullmatch(str(result.get("row_hash") or "")) is None
        or not isinstance(authority, Mapping)
        or SHA64.fullmatch(str(result.get("authority_sha256") or "")) is None
        or result.get("authority_sha256") != _digest(authority)
    ):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: partition proof identity/hash differs",
            retryable=False,
        )
    return result


def _inspect_all(
    plan: Sequence[PartitionRef],
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    exact: dict[str, dict[str, Any]] = {}
    missing: dict[str, dict[str, Any]] = {}
    for partition in plan:
        try:
            exact[partition.partition_id] = _validate_partition_proof(
                partition,
                inspect_partition(partition),
            )
        except Exception as exc:  # noqa: BLE001 - failed proof is a gap
            missing[partition.partition_id] = _inspection_failure(exc)
    return exact, missing


def _partition_proof_hashes(
    exact: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    return {
        partition_id: _digest(exact[partition_id])
        for partition_id in sorted(exact)
    }


def _partition_proof_hash_root(proof_hashes: Mapping[str, str]) -> str:
    return _digest(
        [
            {
                "partition_id": partition_id,
                "proof_sha256": proof_hashes[partition_id],
            }
            for partition_id in sorted(proof_hashes)
        ]
    )


def _partition_root(exact: Mapping[str, Mapping[str, Any]]) -> str:
    return _partition_proof_hash_root(_partition_proof_hashes(exact))


def _dependency_ids(partition: PartitionRef) -> list[str]:
    return [
        PartitionRef(partition.trade_date, dataset).partition_id
        for dataset in DEPENDENCIES[partition.dataset]
    ]


def repair_recent_partitions(
    *,
    expected_build_sha: str,
    datasets: Sequence[str],
    lookback_sessions: int,
    max_repairs_per_run: int,
    apply: bool,
    now: datetime,
    window: AuthorityWindow,
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]],
    publish_partition: Callable[[PartitionRef], Mapping[str, Any]],
    persist_repaired_proof: (
        Callable[[PartitionRef, Mapping[str, Any]], None] | None
    ) = None,
) -> dict[str, Any]:
    selected = _normalize_datasets(datasets)
    if not selected:
        raise ValueError("at least one Linux repair dataset is required")
    if len(window.sessions) != int(lookback_sessions):
        raise ValueError("calendar window size differs from requested lookback")
    if SHA40.fullmatch(str(expected_build_sha)) is None:
        raise ValueError("expected build SHA must be lower SHA-1")
    budget = int(max_repairs_per_run)
    if budget <= 0:
        raise ValueError("max repairs per run must be positive")

    started_at = _as_shanghai(now)
    plan = build_plan(window, selected)
    plan_ids = [partition.partition_id for partition in plan]
    initial_exact, initial_missing = _inspect_all(plan, inspect_partition)
    current_exact = dict(initial_exact)
    attempts: list[dict[str, Any]] = []
    publish_attempt_count = 0
    persistence_failures: dict[str, dict[str, Any]] = {}

    if apply:
        for partition in plan:
            partition_id = partition.partition_id
            if partition_id not in initial_missing:
                continue
            dependencies = _dependency_ids(partition)
            unsatisfied = sorted(
                dependency for dependency in dependencies if dependency not in current_exact
            )
            if unsatisfied:
                attempts.append(
                    {
                        "partition_id": partition_id,
                        "status": "DEPENDENCY_BLOCKED",
                        "dependency_partition_ids": unsatisfied,
                        "retryable": True,
                    }
                )
                continue
            if publish_attempt_count >= budget:
                break
            publish_attempt_count += 1
            attempt: dict[str, Any] = {
                "partition_id": partition_id,
                "status": "DATA_BLOCKED",
                "before": initial_missing[partition_id],
                "retryable": True,
            }
            try:
                source = dict(publish_partition(partition))
                if (
                    SHA64.fullmatch(
                        str(source.get("source_receipt_sha256") or "")
                    )
                    is None
                    or source.get("automatic_order_submission") is not False
                ):
                    raise LinuxGapRepairBlocked(
                        "DATA_BLOCKED: source publication receipt differs",
                        retryable=False,
                    )
                proof = _validate_partition_proof(
                    partition,
                    inspect_partition(partition),
                )
                if persist_repaired_proof is not None:
                    try:
                        persist_repaired_proof(partition, proof)
                    except Exception as exc:
                        persistence_failures[partition_id] = _inspection_failure(exc)
                        raise LinuxGapRepairBlocked(
                            "DATA_BLOCKED: repaired partition proof was not persisted",
                            retryable=False,
                        ) from exc
                current_exact[partition_id] = proof
                attempt.update(
                    status="REPAIRED",
                    source_receipt_sha256=source["source_receipt_sha256"],
                    canonical_proof_sha256=_digest(proof),
                    retryable=False,
                )
            except Exception as exc:  # noqa: BLE001 - fail closed and resume
                attempt.update(_inspection_failure(exc))
            attempts.append(attempt)

    final_exact, final_missing = _inspect_all(plan, inspect_partition)
    for partition_id, failure in persistence_failures.items():
        final_exact.pop(partition_id, None)
        final_missing[partition_id] = failure
    terminal_attempt_ids = {
        str(item.get("partition_id") or "")
        for item in attempts
        if item.get("status") == "DATA_BLOCKED"
        and item.get("retryable") is False
    }
    for partition_id in terminal_attempt_ids & set(final_missing):
        final_missing[partition_id]["retryable"] = False
    remaining_ids = sorted(final_missing)
    final_proof_hashes = _partition_proof_hashes(final_exact)
    complete = not remaining_ids
    remaining_retryable = any(
        bool(item.get("retryable", True)) for item in final_missing.values()
    )
    # A dependency-blocked partition can become runnable after its upstream is
    # repaired in another bounded run even if its current inspection error was
    # otherwise terminal.
    if any(item.get("status") == "DEPENDENCY_BLOCKED" for item in attempts):
        remaining_retryable = True
    if complete:
        blocked_reason = None
    elif not apply:
        blocked_reason = "dry_run_missing_partitions"
    elif publish_attempt_count >= budget:
        blocked_reason = "repair_budget_exhausted"
        remaining_retryable = True
    elif any(item.get("status") == "DEPENDENCY_BLOCKED" for item in attempts):
        blocked_reason = "dependency_data_blocked"
    elif remaining_retryable:
        blocked_reason = "provider_or_pit_data_blocked"
    else:
        blocked_reason = "historical_reconstruction_not_provable"

    capability = {
        name: dict(CAPABILITY_POLICY[name])
        for name in sorted(CAPABILITY_POLICY)
    }
    finished_at = max(datetime.now(SHANGHAI).replace(microsecond=0), started_at)
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "COMPLETE" if complete else "DATA_BLOCKED",
            "task_type": TASK_TYPE,
            "executor_owner": EXECUTOR_OWNER,
            "provider": PROVIDER,
            "build_sha": expected_build_sha,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(finished_at),
            "apply": bool(apply),
            "retryable": False if complete else remaining_retryable,
            "lookback_sessions": int(lookback_sessions),
            "sessions": list(window.sessions),
            "session_set_sha256": _digest(list(window.sessions)),
            "calendar": window.receipt(),
            "datasets": list(selected),
            "plan_partition_count": len(plan),
            "plan_partition_ids": plan_ids,
            "plan_partition_root_sha256": _digest(plan_ids),
            "exact_before_count": len(initial_exact),
            "candidate_before_count": len(initial_missing),
            "candidate_before_ids": sorted(initial_missing),
            "publish_attempt_count": publish_attempt_count,
            "attempt_record_count": len(attempts),
            "repaired_count": sum(
                item.get("status") == "REPAIRED" for item in attempts
            ),
            "max_repairs_per_run": budget,
            "attempts": attempts,
            "exact_after_count": len(final_exact),
            "exact_partition_root_sha256": _partition_proof_hash_root(
                final_proof_hashes
            ),
            "exact_partition_proof_hashes": final_proof_hashes,
            "remaining_count": len(remaining_ids),
            "remaining_partition_ids": remaining_ids,
            "remaining_failures": final_missing,
            "blocked_reason": blocked_reason,
            "capability_policy": capability,
            "capability_policy_sha256": _digest(capability),
            "automatic_order_submission": False,
        }
    )


_PRECONDITION_FAILURE_FIELDS = frozenset(
    {
        "schema",
        "status",
        "task_type",
        "executor_owner",
        "provider",
        "build_sha",
        "started_at",
        "finished_at",
        "apply",
        "retryable",
        "lookback_sessions",
        "sessions",
        "session_set_sha256",
        "calendar",
        "datasets",
        "plan_partition_count",
        "plan_partition_ids",
        "plan_partition_root_sha256",
        "exact_before_count",
        "candidate_before_count",
        "candidate_before_ids",
        "publish_attempt_count",
        "attempt_record_count",
        "repaired_count",
        "max_repairs_per_run",
        "attempts",
        "exact_after_count",
        "exact_partition_root_sha256",
        "exact_partition_proof_hashes",
        "remaining_count",
        "remaining_partition_ids",
        "remaining_failures",
        "blocked_reason",
        "error_type",
        "error_sha256",
        "capability_policy",
        "capability_policy_sha256",
        "automatic_order_submission",
        "result_sha256",
    }
)


def _validate_precondition_failure_result(
    payload: Mapping[str, Any],
    return_code: int,
) -> str | None:
    """Validate the zero-plan envelope emitted before a repair window exists."""

    if payload.get("blocked_reason") != "repair_precondition_data_blocked":
        return None
    datasets = payload.get("datasets")
    try:
        selected = _normalize_datasets(datasets if isinstance(datasets, list) else ())
        started_at = _parse_timestamp(payload.get("started_at"), field="started_at")
        finished_at = _parse_timestamp(payload.get("finished_at"), field="finished_at")
    except (TypeError, ValueError) as exc:
        raise ValueError("Linux gap repair precondition result is invalid") from exc

    build_sha = payload.get("build_sha")
    capability = {
        name: dict(CAPABILITY_POLICY[name]) for name in sorted(CAPABILITY_POLICY)
    }
    empty_root = _digest([])
    valid = bool(
        set(payload) == _PRECONDITION_FAILURE_FIELDS
        and payload.get("status") == "DATA_BLOCKED"
        and int(return_code) == 2
        and payload.get("task_type") == TASK_TYPE
        and payload.get("executor_owner") == EXECUTOR_OWNER
        and payload.get("provider") == PROVIDER
        and payload.get("automatic_order_submission") is False
        and isinstance(payload.get("apply"), bool)
        and isinstance(payload.get("retryable"), bool)
        and type(payload.get("lookback_sessions")) is int
        and type(payload.get("max_repairs_per_run")) is int
        and isinstance(datasets, list)
        and bool(selected)
        and datasets == list(selected)
        and (
            build_sha is None
            or (
                isinstance(build_sha, str)
                and SHA40.fullmatch(build_sha) is not None
                and build_sha != "0" * 40
            )
        )
        and started_at <= finished_at
        and payload.get("sessions") == []
        and payload.get("session_set_sha256") == empty_root
        and payload.get("calendar") is None
        and payload.get("plan_partition_count") == 0
        and payload.get("plan_partition_ids") == []
        and payload.get("plan_partition_root_sha256") == empty_root
        and payload.get("exact_before_count") == 0
        and payload.get("candidate_before_count") == 0
        and payload.get("candidate_before_ids") == []
        and payload.get("publish_attempt_count") == 0
        and payload.get("attempt_record_count") == 0
        and payload.get("repaired_count") == 0
        and payload.get("attempts") == []
        and payload.get("exact_after_count") == 0
        and payload.get("exact_partition_root_sha256") == empty_root
        and payload.get("exact_partition_proof_hashes") == {}
        and payload.get("remaining_count") == 0
        and payload.get("remaining_partition_ids") == []
        and payload.get("remaining_failures") == {}
        and re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]{0,127}",
            str(payload.get("error_type") or ""),
        )
        is not None
        and SHA64.fullmatch(str(payload.get("error_sha256") or "")) is not None
        and payload.get("capability_policy") == capability
        and payload.get("capability_policy_sha256") == _digest(capability)
    )
    if not valid:
        raise ValueError("Linux gap repair precondition result contract differs")
    return "blocked"


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        raise ValueError("Linux gap repair result schema differs")
    unsigned = dict(payload)
    supplied = unsigned.pop("result_sha256", None)
    if supplied != _digest(unsigned):
        raise ValueError("Linux gap repair result hash differs")
    if (
        len(_canonical_json(payload).encode("utf-8"))
        > SCHEDULER_REPLAY_RECEIPT_LIMIT_BYTES
    ):
        raise ValueError("Linux gap repair result exceeds scheduler receipt budget")
    precondition_disposition = _validate_precondition_failure_result(
        payload,
        return_code,
    )
    if precondition_disposition is not None:
        return precondition_disposition
    try:
        selected = _normalize_datasets(payload["datasets"])
        sessions = [_iso_date(value) for value in payload["sessions"]]
        plan_ids = list(payload["plan_partition_ids"])
        plan_count = int(payload["plan_partition_count"])
        exact_before = int(payload["exact_before_count"])
        candidate_before = int(payload["candidate_before_count"])
        exact_after = int(payload["exact_after_count"])
        remaining = int(payload["remaining_count"])
        attempts = list(payload["attempts"])
        publish_count = int(payload["publish_attempt_count"])
        repaired = int(payload["repaired_count"])
        budget = int(payload["max_repairs_per_run"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Linux gap repair counters are invalid") from exc
    window = AuthorityWindow(
        sessions=tuple(sessions),
        batch_id=str((payload.get("calendar") or {}).get("batch_id") or ""),
        manifest_hash=str((payload.get("calendar") or {}).get("manifest_hash") or ""),
        source_session_set_hash=str(
            (payload.get("calendar") or {}).get("source_session_set_hash") or ""
        ),
    )
    expected_plan = [item.partition_id for item in build_plan(window, selected)]
    capability = payload.get("capability_policy")
    proof_hashes = payload.get("exact_partition_proof_hashes")
    valid = bool(
        payload.get("task_type") == TASK_TYPE
        and payload.get("executor_owner") == EXECUTOR_OWNER
        and payload.get("provider") == PROVIDER
        and payload.get("automatic_order_submission") is False
        and isinstance(payload.get("apply"), bool)
        and sessions == sorted(set(sessions))
        and int(payload.get("lookback_sessions") or 0) == len(sessions)
        and payload.get("session_set_sha256") == _digest(sessions)
        and plan_ids == expected_plan
        and plan_count == len(expected_plan)
        and payload.get("plan_partition_root_sha256") == _digest(plan_ids)
        and exact_before + candidate_before == plan_count
        and exact_after + remaining == plan_count
        and isinstance(proof_hashes, Mapping)
        and set(proof_hashes or {})
        == set(plan_ids) - set(payload.get("remaining_partition_ids") or [])
        and all(
            isinstance(proof_hash, str)
            and SHA64.fullmatch(proof_hash) is not None
            for proof_hash in (proof_hashes or {}).values()
        )
        and payload.get("exact_partition_root_sha256")
        == _partition_proof_hash_root(proof_hashes or {})
        and 0 <= publish_count <= budget
        and int(payload.get("attempt_record_count") or 0) == len(attempts)
        and repaired
        == sum(item.get("status") == "REPAIRED" for item in attempts)
        and list(payload.get("candidate_before_ids") or [])
        == sorted(set(payload.get("candidate_before_ids") or []))
        and list(payload.get("remaining_partition_ids") or [])
        == sorted(set(payload.get("remaining_partition_ids") or []))
        and set(payload.get("candidate_before_ids") or []).issubset(plan_ids)
        and set(payload.get("remaining_partition_ids") or []).issubset(plan_ids)
        and isinstance(capability, Mapping)
        and payload.get("capability_policy_sha256") == _digest(capability)
        and capability == {
            name: dict(CAPABILITY_POLICY[name]) for name in sorted(CAPABILITY_POLICY)
        }
        and SHA64.fullmatch(
            str(payload.get("exact_partition_root_sha256") or "")
        )
        is not None
        and _parse_timestamp(payload.get("started_at"), field="started_at")
        <= _parse_timestamp(payload.get("finished_at"), field="finished_at")
    )
    calendar = payload.get("calendar") or {}
    if sessions and (
        SHA40.fullmatch(str(payload.get("build_sha") or "")) is None
        or not str(calendar.get("batch_id") or "")
        or any(
            SHA64.fullmatch(str(calendar.get(field) or "")) is None
            for field in (
                "manifest_hash",
                "source_session_set_hash",
                "selected_session_set_hash",
            )
        )
        or calendar.get("selected_session_set_hash") != _digest(sessions)
    ):
        valid = False
    for attempt in attempts:
        if (
            not isinstance(attempt, Mapping)
            or attempt.get("partition_id") not in plan_ids
            or attempt.get("status")
            not in {"REPAIRED", "DATA_BLOCKED", "DEPENDENCY_BLOCKED"}
            or not isinstance(attempt.get("retryable"), bool)
        ):
            valid = False
            break
        if attempt.get("status") == "REPAIRED" and (
            SHA64.fullmatch(str(attempt.get("source_receipt_sha256") or ""))
            is None
            or SHA64.fullmatch(str(attempt.get("canonical_proof_sha256") or ""))
            is None
            or (proof_hashes or {}).get(attempt.get("partition_id"))
            != attempt.get("canonical_proof_sha256")
        ):
            valid = False
            break
    if not valid:
        raise ValueError("Linux gap repair result contract differs")
    status = str(payload.get("status") or "")
    if status == "COMPLETE":
        if (
            int(return_code) != 0
            or payload.get("retryable") is not False
            or remaining != 0
            or payload.get("blocked_reason") is not None
        ):
            raise ValueError("Linux gap repair COMPLETE disposition differs")
        return "complete"
    if status == "DATA_BLOCKED":
        if int(return_code) != 2 or remaining <= 0 or not payload.get("blocked_reason"):
            raise ValueError("Linux gap repair DATA_BLOCKED disposition differs")
        return "blocked"
    raise ValueError("Linux gap repair status is invalid")


def parse_result(output: str | None) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for line in str(output or "").splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("schema") == RESULT_SCHEMA:
            matches.append(value)
    if len(matches) != 1:
        raise ValueError("Linux gap repair output must contain exactly one result")
    return matches[0]


def scheduler_output_status(output: str | None, *, return_code: int) -> str:
    try:
        payload = parse_result(output)
        disposition = validate_task_result(payload, return_code)
    except (TypeError, ValueError):
        return "failed"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if expected_build and payload.get("build_sha") != expected_build:
        return "failed"
    if disposition == "complete":
        return "success"
    return "failed" if payload.get("retryable") is True else "blocked"


def validate_persisted_result(
    primary_engine: Any,
    payload: Mapping[str, Any],
    *,
    history_engine: Any | None = None,
    now: datetime | None = None,
    state_file: Path = DEFAULT_STATE_FILE,
    window_loader: Callable[..., AuthorityWindow] = load_recent_closed_window,
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Pure-read replay of one COMPLETE receipt against persisted partitions."""

    if validate_task_result(payload, 0) != "complete":
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap repair receipt is not COMPLETE"
        )
    checked_at = _as_shanghai(now or datetime.now(SHANGHAI))
    finished_at = _parse_timestamp(payload.get("finished_at"), field="finished_at")
    if finished_at > checked_at + timedelta(minutes=5):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap repair receipt is from the future"
        )
    environment_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if environment_sha and environment_sha != payload.get("build_sha"):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap repair build receipt is stale"
        )

    calendar = payload.get("calendar")
    if not isinstance(calendar, Mapping):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap repair calendar receipt is missing"
        )
    window = window_loader(
        primary_engine,
        now=finished_at,
        lookback_sessions=int(payload["lookback_sessions"]),
        batch_id=str(calendar.get("batch_id") or ""),
    )
    if (
        list(window.sessions) != list(payload.get("sessions") or [])
        or window.receipt() != dict(calendar)
    ):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap repair calendar replay differs"
        )

    owned_history = None
    if inspect_partition is None:
        owned_history = history_engine or get_kline_engine()
        prior_proofs = ProofLedger(state_file).load()
        inspect_partition = ProductionPartitionInspector(
            primary_engine,
            owned_history,
            decision_time=finished_at,
            expected_build_sha=str(payload["build_sha"]),
            prior_proofs=prior_proofs,
        )
    try:
        plan = build_plan(window, _normalize_datasets(payload["datasets"]))
        exact, missing = _inspect_all(plan, inspect_partition)
    finally:
        if history_engine is None and owned_history is not None:
            dispose = getattr(owned_history, "dispose", None)
            if callable(dispose):
                dispose()
    proof_hashes = {
        partition_id: _digest(proof)
        for partition_id, proof in sorted(exact.items())
    }
    if (
        missing
        or len(exact) != int(payload["plan_partition_count"])
        or proof_hashes != dict(payload["exact_partition_proof_hashes"])
        or _partition_proof_hash_root(proof_hashes)
        != payload.get("exact_partition_root_sha256")
    ):
        raise LinuxGapRepairBlocked(
            "DATA_BLOCKED: Linux recent-gap persisted window differs from receipt"
        )
    return {
        "status": "COMPLETE",
        "task_type": TASK_TYPE,
        "build_sha": payload["build_sha"],
        "sessions": list(window.sessions),
        "partition_count": len(exact),
        "exact_partition_root_sha256": payload["exact_partition_root_sha256"],
        "checked_at": _timestamp(checked_at),
    }


def _failure_result(
    *,
    datasets: Sequence[str],
    lookback_sessions: int,
    max_repairs_per_run: int,
    apply: bool,
    build_sha: str,
    started_at: datetime,
    error: BaseException,
) -> dict[str, Any]:
    capability = {
        name: dict(CAPABILITY_POLICY[name])
        for name in sorted(CAPABILITY_POLICY)
    }
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "task_type": TASK_TYPE,
            "executor_owner": EXECUTOR_OWNER,
            "provider": PROVIDER,
            "build_sha": build_sha or None,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(datetime.now(SHANGHAI)),
            "apply": bool(apply),
            "retryable": bool(getattr(error, "retryable", True)),
            "lookback_sessions": int(lookback_sessions),
            "sessions": [],
            "session_set_sha256": _digest([]),
            "calendar": None,
            "datasets": list(_normalize_datasets(datasets)),
            "plan_partition_count": 0,
            "plan_partition_ids": [],
            "plan_partition_root_sha256": _digest([]),
            "exact_before_count": 0,
            "candidate_before_count": 0,
            "candidate_before_ids": [],
            "publish_attempt_count": 0,
            "attempt_record_count": 0,
            "repaired_count": 0,
            "max_repairs_per_run": int(max_repairs_per_run),
            "attempts": [],
            "exact_after_count": 0,
            "exact_partition_root_sha256": _digest([]),
            "exact_partition_proof_hashes": {},
            "remaining_count": 0,
            "remaining_partition_ids": [],
            "remaining_failures": {},
            "blocked_reason": "repair_precondition_data_blocked",
            "error_type": type(error).__name__,
            "error_sha256": _digest(str(error)),
            "capability_policy": capability,
            "capability_policy_sha256": _digest(capability),
            "automatic_order_submission": False,
        }
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=DATASET_ORDER,
        default=[],
        help="Repeat to limit scope; default covers every safe/conditional repair dataset.",
    )
    parser.add_argument("--lookback-sessions", type=int, default=5)
    parser.add_argument("--max-repairs-per-run", type=int, default=20)
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help="Pre-provisioned persistent proof ledger path.",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    datasets = args.dataset or list(DEFAULT_DATASET_ORDER)
    started_at = datetime.now(SHANGHAI).replace(microsecond=0)
    build_sha = ""
    primary = None
    history = None
    minute = None
    ledger = ProofLedger(args.state_file)
    try:
        load_project_env()
        build_sha = _build_sha(args.expected_build_sha)
        validate_executor()
        primary = create_batch_engine(future=True)
        history = get_kline_engine()
        minute = get_minute_engine()
        prior_proofs = ledger.load()
        window = load_recent_closed_window(
            primary,
            now=started_at,
            lookback_sessions=args.lookback_sessions,
        )
        inspector = ProductionPartitionInspector(
            primary,
            history,
            decision_time=started_at,
            expected_build_sha=build_sha,
            prior_proofs=prior_proofs,
        )
        publisher = ProductionPartitionPublisher(
            primary,
            history,
            minute,
            expected_build_sha=build_sha,
            now=started_at,
            concept_receipt_sink=inspector.record_concept_receipt,
            flow_evidence_root=args.state_file.parent,
        )
        result = repair_recent_partitions(
            expected_build_sha=build_sha,
            datasets=datasets,
            lookback_sessions=args.lookback_sessions,
            max_repairs_per_run=args.max_repairs_per_run,
            apply=args.apply,
            now=started_at,
            window=window,
            inspect_partition=inspector,
            publish_partition=publisher,
            persist_repaired_proof=lambda partition, proof: ledger.record(
                partition,
                proof,
                now=started_at,
            ),
        )
    except Exception as exc:  # noqa: BLE001 - machine-readable failure
        result = _failure_result(
            datasets=datasets,
            lookback_sessions=args.lookback_sessions,
            max_repairs_per_run=args.max_repairs_per_run,
            apply=args.apply,
            build_sha=build_sha,
            started_at=started_at,
            error=exc,
        )
    finally:
        if minute is not None and minute is not primary and minute is not history:
            minute.dispose()
        if history is not None and history is not primary:
            history.dispose()
        if primary is not None:
            primary.dispose()
    print(_canonical_json(result), flush=True)
    return 0 if result.get("status") == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
