"""Bulk import of the QMT local bar cache into the canonical history tables.

The cache is read once per instrument and staged in MySQL, then published one
trading session at a time.  Reading it per session instead would re-read the
whole ~16 GiB cache once per date, so the harvest loop is file-major and the
publish loop is date-major.

``sm_stock_minute`` has no unique business key, so a session is published as a
date-scoped replace: the previous ``qmt_datadir`` rows for that date are
removed, the staged rows are inserted, and only then are superseded rows from
another source removed.  A crash therefore never leaves a session empty.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from integrations.qmt.datadir import (
    CST,
    MARKETS,
    MINUTE_PERIOD,
    load_dat,
    iter_minute_rows,
    select_records,
)
from server.common.auxiliary_runtime_schema import (
    validate_qmt_datadir_import_runtime_schema,
)
from server.common.qmt_index_minute_grid import CORE_GRID
from server.common.qmt_stock_catalog import load_stock_catalog

DATASET = "stock_minute"
SOURCE = "qmt_datadir"
DATA_VERSION = "qmt_datadir_v1"
QUALITY_STATUS = "VERIFIED"
PERMISSION_STATUS = "SUPPORTED"

TARGET_TABLE = "sm_stock_minute"
STAGE_TABLE = "qmt_datadir_minute_stage"
RECEIPT_TABLE = "qmt_datadir_import_partition"

STATUS_RUNNING = "RUNNING"
STATUS_COMPLETE = "COMPLETE"
STATUS_FAILED = "FAILED"

DEFAULT_CHUNK_ROWS = 5000
DEFAULT_SESSIONS_PER_BATCH = 20
# A session is only complete when the cache holds (nearly) every instrument the
# catalog proves was listed that day.  The production publisher covers 5563 of
# 5575 eligible codes on 2026-10-14 (99.8%), so a 95% floor rejects a cache that
# only holds a fragment of the day without touching a healthy one.
DEFAULT_MIN_COVERAGE = 0.95
REASON_THIN_CACHE = "insufficient_code_coverage"

MINUTE_COLUMNS = (
    "stock_code",
    "trade_time",
    "trade_date",
    "price",
    "`change`",
    "change_pct",
    "volume",
    "amount",
    "etl_sync_at",
    "qmt_code",
    "data_source",
    "source_time",
    "received_at",
    "batch_id",
    "data_version",
    "quality_status",
    "permission_status",
)
MINUTE_STAGE_COLUMNS = (
    "run_id",
    "stock_code",
    "trade_time",
    "trade_date",
    "price",
    "`change`",
    "change_pct",
    "volume",
    "amount",
    "qmt_code",
)
MINUTE_ROW_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
MINUTE_STAGE_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


@dataclass(frozen=True)
class UniverseMember:
    """A stock the QMT catalog proves was listed during the import window."""

    qmt_code: str
    market: str
    code: str


@dataclass(frozen=True)
class Universe:
    """The import window's stocks, with the catalog's per-day listing proof."""

    members: dict[str, UniverseMember]
    listed_on: dict[str, tuple[str, str | None]]

    def __len__(self) -> int:
        return len(self.members)

    def __iter__(self):
        return iter(self.members.values())

    def ordered(self) -> list["UniverseMember"]:
        return [self.members[qmt_code] for qmt_code in sorted(self.members)]

    def eligible_codes(self, session: dt.date) -> int:
        """Number of stocks the catalog proves were listed on ``session``."""
        day = session.isoformat()
        return sum(
            1
            for listed, expired in self.listed_on.values()
            if listed <= day and (expired is None or day <= expired)
        )


def coverage_shortfall(
    universe: Universe, session: dt.date, imported_codes: int,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> int | None:
    """Return the eligible-code count when the cache holds too few instruments.

    ``None`` means the session clears the coverage floor and may be published.
    """
    eligible = universe.eligible_codes(session)
    if eligible == 0:
        return None
    if imported_codes >= math.ceil(eligible * min_coverage):
        return None
    return eligible


@dataclass(frozen=True)
class PartitionPlan:
    session: dt.date
    existing_sources: tuple[str, ...]
    action: str

    def as_dict(self) -> dict:
        return {
            "session": self.session.isoformat(),
            "existing_sources": list(self.existing_sources),
            "action": self.action,
        }


@dataclass(frozen=True)
class PartitionResult:
    session: dt.date
    status: str
    row_count: int
    code_count: int
    content_sha256: str
    readback_rows: int
    readback_codes: int

    def as_dict(self) -> dict:
        return dataclasses.asdict(self) | {"session": self.session.isoformat()}


def require_schema(engine: Engine) -> None:
    """Fail early when the release migration has not provisioned the tables.

    The runtime account has no ``CREATE`` privilege, so the manifest for
    ``sm_stock_minute`` is created by ``privileged_migrate_*`` during release.
    """
    try:
        validate_qmt_datadir_import_runtime_schema(engine)
    except Exception as exc:  # noqa: BLE001 - re-raised with the operator action
        raise RuntimeError(
            "QMT datadir importer tables are missing; run the privileged "
            "auxiliary runtime schema migration before importing"
        ) from exc


def load_universe(
    engine: Engine,
    *,
    start: dt.date,
    end: dt.date,
    decision_known_at: dt.datetime | None = None,
) -> "Universe":
    """Return the catalog-proven stock universe for the import window.

    The catalog also lists instruments that expired before the window, and the
    datadir holds indices and funds under the same numeric namespaces.  Only
    ``qmt_code`` values the stock catalog proves were listed inside the window
    are imported, and each session carries the number of codes the catalog
    proves were listed on that day so a fragment of a day can be rejected.
    """
    known_at = decision_known_at or dt.datetime.now(CST)
    with engine.connect() as conn:
        batch = load_stock_catalog(conn, decision_known_at=known_at)
    members: dict[str, UniverseMember] = {}
    listed_on: dict[str, tuple[str, str | None]] = {}
    start_iso, end_iso = start.isoformat(), end.isoformat()
    for member in batch.members:
        qmt_code = str(member["qmt_code"])
        # QMT codes are "<code>.<market>", for example 600000.SH.
        code, _, market = qmt_code.partition(".")
        if market not in MARKETS or not code:
            continue
        listed = str(member.get("list_date") or "1970-01-01")
        expired = member.get("expire_date")
        if listed > end_iso:
            continue
        if expired not in (None, "") and str(expired) < start_iso:
            continue
        members[qmt_code] = UniverseMember(qmt_code=qmt_code, market=market, code=code)
        listed_on[qmt_code] = (listed, None if expired in (None, "") else str(expired))
    return Universe(members=members, listed_on=listed_on)


def existing_sources(engine: Engine, sessions: Sequence[dt.date]) -> dict[dt.date, list[str]]:
    """Return which sources already hold rows for each session.

    ``sm_stock_minute`` is 125M+ rows, so the plan uses one index-only
    ``GROUP BY`` over the requested window instead of a counted scan per
    session; row totals are proven at publish time by the receipt readback.
    """
    if not sessions:
        return {}
    low, high = min(sessions), max(sessions)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT trade_date, data_source FROM `{TARGET_TABLE}` "
                "WHERE trade_date BETWEEN :low AND :high "
                "GROUP BY trade_date, data_source"
            ),
            {"low": low, "high": high},
        ).fetchall()
    found: dict[dt.date, list[str]] = {}
    for trade_date, source in rows:
        found.setdefault(trade_date, []).append(str(source))
    return {session: sorted(found.get(session, [])) for session in sessions}


def plan(
    existing: dict[dt.date, Sequence[str]],
    sessions: Sequence[dt.date],
    *,
    replace_existing: bool,
    already_complete: set[dt.date] | None = None,
) -> list[PartitionPlan]:
    """Decide per session whether to import, replace or skip."""
    complete = already_complete or set()
    plans: list[PartitionPlan] = []
    for session in sessions:
        sources = tuple(existing.get(session, ()))
        has_other = any(source != SOURCE for source in sources)
        if session in complete:
            action = "SKIP_COMPLETE"
        elif not sources:
            action = "IMPORT"
        elif not has_other:
            action = "IMPORT"
        elif replace_existing:
            action = "REPLACE"
        else:
            action = "SKIP_EXISTING"
        plans.append(
            PartitionPlan(session=session, existing_sources=sources, action=action)
        )
    return plans


def completed_sessions(engine: Engine) -> set[dt.date]:
    """Return sessions whose receipt says the import finished."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT trade_date FROM `{RECEIPT_TABLE}` "
                    "WHERE dataset=:d AND status=:s"
                ),
                {"d": DATASET, "s": STATUS_COMPLETE},
            ).fetchall()
    except Exception:  # table is created on first apply
        return set()
    return {row[0] for row in rows}


def _insert_stage(conn: Connection, chunk: list[tuple]) -> None:
    if not chunk:
        return
    sql = (
        f"INSERT INTO `{STAGE_TABLE}` ({','.join(MINUTE_STAGE_COLUMNS)}) VALUES "
        + ",".join([MINUTE_STAGE_TEMPLATE] * len(chunk))
    )
    conn.exec_driver_sql(
        sql, tuple(value for row in chunk for value in row)
    )
    # Staging is a bulk landing area, not a transaction: committing per chunk
    # keeps one session batch from holding ~27M rows of undo log open.
    conn.commit()


def harvest(
    conn: Connection,
    *,
    root: Path,
    universe: Sequence[UniverseMember] | Universe,
    sessions: Sequence[dt.date],
    run_id: str,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
) -> dict[dt.date, dict]:
    """Read every instrument once and stage its rows for ``sessions``.

    Returns ``{session: {"rows", "codes", "digest"}}`` where the digest binds the
    exact decoded bytes that produced the session.
    """
    stats: dict[dt.date, dict] = {
        session: {"rows": 0, "codes": [], "digest": hashlib.sha256()}
        for session in sessions
    }
    chunk: list[tuple] = []
    missing = 0
    for member in universe:
        dat = load_dat(root, member.market, MINUTE_PERIOD, member.code)
        if dat is None:
            missing += 1
            continue
        selected = select_records(dat, sessions)
        if len(selected) == 0:
            continue
        selected_digest = hashlib.sha256(selected.tobytes()).digest()
        per_session: dict[dt.date, int] = {}
        for session, row in iter_minute_rows(dat, selected, CORE_GRID):
            chunk.append((run_id, *row))
            per_session[session] = per_session.get(session, 0) + 1
        for session in per_session:
            entry = stats[session]
            entry["codes"].append(member.code)
            entry["digest"].update(member.qmt_code.encode())
            entry["digest"].update(selected_digest)
        for session, count in per_session.items():
            stats[session]["rows"] += count
        if len(chunk) >= chunk_rows:
            _insert_stage(conn, chunk)
            chunk = []
    _insert_stage(conn, chunk)
    for entry in stats.values():
        entry["codes"] = sorted(entry["codes"])
        entry["digest"] = entry["digest"].hexdigest()
    stats["_missing_files"] = missing
    return stats


def write_receipt(
    conn: Connection,
    *,
    session: dt.date,
    root: str,
    status: str,
    run_id: str,
    code_count: int = 0,
    row_count: int = 0,
    content_sha256: str = "",
    detail: dict | None = None,
    finished: bool = False,
) -> None:
    conn.execute(
        text(f"""
            INSERT INTO `{RECEIPT_TABLE}`
              (dataset, trade_date, source_root, status, code_count, row_count,
               content_sha256, run_id, started_at, finished_at, detail_json)
            VALUES (:dataset, :d, :root, :status, :codes, :rows, :sha, :run_id,
                    NOW(), {"NOW()" if finished else "NULL"}, :detail)
            ON DUPLICATE KEY UPDATE
              source_root=VALUES(source_root), status=VALUES(status),
              code_count=VALUES(code_count), row_count=VALUES(row_count),
              content_sha256=VALUES(content_sha256), run_id=VALUES(run_id),
              finished_at=VALUES(finished_at), detail_json=VALUES(detail_json)
        """),
        {
            "dataset": DATASET,
            "d": session,
            "root": root,
            "status": status,
            "codes": code_count,
            "rows": row_count,
            "sha": content_sha256,
            "run_id": run_id,
            "detail": json.dumps(detail or {}, ensure_ascii=False, default=str),
        },
    )


def publish_session(
    conn: Connection,
    *,
    session: dt.date,
    run_id: str,
    batch_id: str,
    replace_sources: Sequence[str],
) -> None:
    """Replace one session's rows: remove ours, insert staged, drop superseded."""
    conn.execute(
        text(f"DELETE FROM `{TARGET_TABLE}` WHERE trade_date=:d AND data_source=:s"),
        {"d": session, "s": SOURCE},
    )
    conn.execute(
        text(f"""
            INSERT INTO `{TARGET_TABLE}` ({','.join(MINUTE_COLUMNS)})
            SELECT stock_code, trade_time, trade_date, price, `change`, change_pct,
                   volume, amount, NOW(), qmt_code, :source, trade_time, NOW(),
                   :batch_id, :version, :quality, :permission
            FROM `{STAGE_TABLE}`
            WHERE run_id=:run_id AND trade_date=:d
            ORDER BY stock_code, trade_time
        """),
        {
            "source": SOURCE,
            "batch_id": batch_id,
            "version": DATA_VERSION,
            "quality": QUALITY_STATUS,
            "permission": PERMISSION_STATUS,
            "run_id": run_id,
            "d": session,
        },
    )
    superseded = [source for source in replace_sources if source != SOURCE]
    if superseded:
        statement = text(
            f"DELETE FROM `{TARGET_TABLE}` WHERE trade_date=:d "
            "AND data_source IN :sources"
        ).bindparams(bindparam("sources", expanding=True))
        conn.execute(statement, {"d": session, "sources": superseded})


def readback_session(conn: Connection, session: dt.date) -> tuple[int, int]:
    """Return the durable row and code counts the target now holds for us."""
    row = conn.execute(
        text(
            f"SELECT COUNT(*), COUNT(DISTINCT stock_code) FROM `{TARGET_TABLE}` "
            "WHERE trade_date=:d AND data_source=:s"
        ),
        {"d": session, "s": SOURCE},
    ).fetchone()
    return int(row[0]), int(row[1])


def drop_staged(conn: Connection, run_id: str) -> None:
    conn.execute(text(f"DELETE FROM `{STAGE_TABLE}` WHERE run_id=:r"), {"r": run_id})


def new_run_id(now: dt.datetime | None = None) -> str:
    moment = now or dt.datetime.now(CST)
    return f"qdd_{moment.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def new_batch_id(session: dt.date, run_id: str) -> str:
    return f"qmt_dat_{session.strftime('%Y%m%d')}_{run_id}"

