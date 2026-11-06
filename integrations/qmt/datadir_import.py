"""Bulk import of the QMT local bar cache into the canonical history tables.

The cache is read once per instrument and staged in MySQL, then published one
trading session at a time.  Reading it per session instead would re-read the
whole ~16 GiB cache once per date, so the harvest loop is file-major and the
publish loop is date-major.

``sm_stock_minute`` has no unique business key. Publication reconciles the
staged session in one transaction, keeping exact existing bars and replacing
only differences and explicitly superseded sources.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import re
import time
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
    QMT_DATADIR_STAGE_INDEX_NAME,
    validate_qmt_datadir_import_runtime_schema,
)
from server.common.qmt_datadir_daily_coverage import MANIFEST_TABLE, SCHEMA as DAILY_MANIFEST_SCHEMA
from server.common.qmt_index_minute_grid import CORE_GRID
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_datadir_progress import MinuteImportProgress

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


def load_minute_partition_receipts(
    engine: Engine, sessions: Sequence[dt.date],
) -> dict[dt.date, dict]:
    """Read requested receipts and current daily identities in one database snapshot."""
    if not sessions:
        return {}
    cutoff = dt.datetime.now(CST).replace(tzinfo=None)
    query = text(f"""
        SELECT r.dataset,r.trade_date,r.source_root,r.status,r.code_count,r.row_count,
               r.content_sha256,r.run_id,r.detail_json,
               (SELECT CASE WHEN m.strategy_eligible=1 THEN m.manifest_hash END
                FROM {MANIFEST_TABLE} m
                WHERE m.dataset='stock_daily' AND m.trade_date=r.trade_date
                  AND m.provider=:provider AND m.status='EXACT'
                  AND m.schema_version=:daily_schema AND m.captured_at<=:cutoff
                ORDER BY m.captured_at DESC,m.manifest_hash DESC LIMIT 1
               ) AS current_daily_manifest_hash
        FROM `{RECEIPT_TABLE}` r
        WHERE r.dataset=:dataset AND r.trade_date IN :sessions
    """).bindparams(bindparam("sessions", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"dataset": DATASET, "provider": SOURCE,
            "daily_schema": DAILY_MANIFEST_SCHEMA, "cutoff": cutoff, "sessions": sessions}).mappings().all()
    receipts = {}
    for row in rows:
        receipt = dict(row)
        try:
            detail = json.loads(receipt.pop("detail_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            detail = {}
        receipt["detail"] = detail if isinstance(detail, dict) else {}
        receipts[dt.date.fromisoformat(str(receipt["trade_date"]))] = receipt
    return receipts


def is_current_minute_receipt(receipt: dict) -> bool:
    """Accept sealed minute rows only while their daily authority is current."""
    detail = receipt["detail"]
    daily_hash = detail.get("full_market_manifest_hash")
    return (
        receipt["status"] == STATUS_COMPLETE
        and detail.get("minute_grid_count") == len(CORE_GRID)
        and detail.get("bad_grid_codes") == 0
        and isinstance(detail.get("row_hash"), str)
        and len(detail["row_hash"]) == 64
        and detail.get("coverage") == 1.0
        and isinstance(daily_hash, str)
        and len(daily_hash) == 64
        and daily_hash == receipt["current_daily_manifest_hash"]
        and len(str(detail.get("expected_traded_code_set_sha256") or "")) == 64
    )


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
    codes_by_session: dict[dt.date, set[str]] | None = None,
) -> dict[dt.date, dict]:
    """Read every instrument once and stage its rows for ``sessions``.

    Returns ``{session: {"rows", "codes", "digest"}}`` where the digest binds the
    exact decoded bytes that produced the session.
    """
    stats: dict[dt.date, dict] = {
        session: {
            "rows": 0,
            "codes": [],
            "code_rows": {},
            "digest": hashlib.sha256(),
        }
        for session in sessions
    }
    chunk: list[tuple] = []
    missing = 0
    total = (sum(len(codes_by_session[day]) * len(CORE_GRID) for day in sessions)
             if codes_by_session is not None else len(universe) * len(sessions) * len(CORE_GRID))
    progress = MinuteImportProgress(run_id=run_id, total_units=total,
        operation="harvest", days=sessions)
    progress.emit("source_read", total_securities=len(universe))
    source_seconds, stage_seconds, committed, next_report = 0.0, 0.0, 0, 50000

    def commit_chunk(values, current):
        nonlocal stage_seconds, committed, next_report
        if not values:
            return
        progress.emit("stage_commit", current_unit=current, batch_rows=len(values),
            source_read_seconds=round(source_seconds, 6), stage_commit_seconds=round(stage_seconds, 6))
        started = time.monotonic()
        try:
            _insert_stage(conn, values)
        except Exception as exc:
            progress.emit("stage_commit", status="FAILED", current_unit=current,
                batch_rows=len(values), phase_seconds=round(time.monotonic() - started, 6),
                error_type=type(exc).__name__, console=True)
            raise
        stage_seconds += time.monotonic() - started
        committed += len(values)
        progress.emit("stage_commit", status="COMPLETE", completed_units=committed, current_unit=current,
            batch_rows=len(values), source_read_seconds=round(source_seconds, 6),
            stage_commit_seconds=round(stage_seconds, 6))
        if committed >= next_report:
            progress.emit("source_read", completed_units=committed, current_unit=current,
                source_read_seconds=round(source_seconds, 6), stage_commit_seconds=round(stage_seconds, 6))
            next_report = (committed // 50000 + 1) * 50000

    for member in universe:
        source_started = time.monotonic()
        dat = load_dat(root, member.market, MINUTE_PERIOD, member.code)
        if dat is None:
            missing += 1
            source_seconds += time.monotonic() - source_started
            continue
        selected = select_records(dat, sessions)
        if len(selected) == 0:
            source_seconds += time.monotonic() - source_started
            continue
        selected_digest = hashlib.sha256(selected.tobytes()).digest()
        per_session: dict[dt.date, int] = {}
        for session, row in iter_minute_rows(dat, selected, CORE_GRID):
            if codes_by_session is not None and member.code not in codes_by_session[session]:
                continue
            chunk.append((run_id, *row))
            per_session[session] = per_session.get(session, 0) + 1
        for session in per_session:
            entry = stats[session]
            entry["codes"].append(member.code)
            entry["digest"].update(member.qmt_code.encode())
            entry["digest"].update(selected_digest)
        for session, count in per_session.items():
            stats[session]["rows"] += count
            stats[session]["code_rows"][member.code] = count
        source_seconds += time.monotonic() - source_started
        if len(chunk) >= chunk_rows:
            commit_chunk(chunk, member.code)
            chunk = []
    commit_chunk(chunk, "final_chunk")
    for entry in stats.values():
        entry["codes"] = sorted(entry["codes"])
        entry["code_rows"] = dict(sorted(entry["code_rows"].items()))
        entry["digest"] = entry["digest"].hexdigest()
    stats["_missing_files"] = missing
    progress.emit("harvest_complete", status="COMPLETE", completed_units=committed,
        source_read_seconds=round(source_seconds, 6), stage_commit_seconds=round(stage_seconds, 6),
        missing_files=missing)
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
    batch_codes: int = 50,
    deadline_seconds: float = 900,
    progress=None,
) -> None:
    """Reconcile bounded stock batches inside the caller's one atomic transaction."""
    if batch_codes < 1 or deadline_seconds <= 0:
        raise ValueError("minute publication batch/deadline is invalid")
    sources = sorted({SOURCE, *replace_sources})
    mysql = conn.dialect.name == "mysql"
    stage_index = f" FORCE INDEX (`{QMT_DATADIR_STAGE_INDEX_NAME}`)" if mysql else ""
    target_index = " FORCE INDEX (idx_smm_date_source_code_time)" if mysql else ""
    stock_encodings = {}
    if mysql:
        stock_encodings = {
            table: (charset, collation)
            for table, charset, collation in conn.execute(text("""
                SELECT TABLE_NAME, CHARACTER_SET_NAME, COLLATION_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE() AND COLUMN_NAME='stock_code'
                  AND TABLE_NAME IN (:target, :stage)
            """), {"target": TARGET_TABLE, "stage": STAGE_TABLE})
        }
        if set(stock_encodings) != {TARGET_TABLE, STAGE_TABLE} or any(
            not isinstance(charset, str) or not re.fullmatch(r"[a-z0-9_]+", charset)
            or not isinstance(collation, str) or not re.fullmatch(r"[a-z0-9_]+", collation)
            or not collation.startswith(charset + "_")
            for charset, collation in stock_encodings.values()
        ):
            raise RuntimeError("minute_publication: stock code collation metadata differs")
    staged_counts = {str(code): int(count) for code, count in conn.execute(text(
        f"SELECT stock_code,COUNT(*) FROM `{STAGE_TABLE}`{stage_index} "
        "WHERE run_id=:r AND trade_date=:d GROUP BY stock_code"
    ), {"r": run_id, "d": session})}
    codes = set(staged_counts)
    current_codes = text(
        f"SELECT DISTINCT stock_code FROM `{TARGET_TABLE}` "
        "WHERE trade_date=:d AND data_source IN :sources"
    ).bindparams(bindparam("sources", expanding=True))
    existing_codes = {
        str(row[0])
        for row in conn.execute(current_codes, {"d": session, "sources": sources})
    }
    codes.update(existing_codes)
    ordered = sorted(codes)
    deadline = time.monotonic() + deadline_seconds

    def exact_text(expression):
        return f"CAST({expression} AS BINARY)" if mysql else f"{expression} COLLATE BINARY"

    def indexed_stock_match(indexed_alias, indexed_table, outer_alias, outer_table):
        outer = f"{outer_alias}.stock_code"
        if mysql:
            charset, collation = stock_encodings[indexed_table]
            outer_charset, outer_collation = stock_encodings[outer_table]
            if charset != outer_charset:
                outer = f"CONVERT({outer} USING {charset})"
            if collation != outer_collation:
                outer = f"{outer} COLLATE {collation}"
        return f"{indexed_alias}.stock_code={outer}"

    identity = f"""
        e.trade_date=s.trade_date
        AND e.trade_time=s.trade_time
        AND {exact_text('e.stock_code')}={exact_text('s.stock_code')}
    """
    numeric_match = " AND ".join(
        f"(e.`{field}`=s.`{field}` OR (e.`{field}` IS NULL AND s.`{field}` IS NULL))"
        for field in ("price", "change", "change_pct", "volume", "amount")
    )
    # The physical key is id, so retain only the first exact row per bar.
    # Select only victim IDs over the wire and delete by their primary keys.
    victims = text(f"""
        SELECT id FROM (
            SELECT id, is_exact, ROW_NUMBER() OVER (
                PARTITION BY stock_code, trade_time, is_exact ORDER BY id
            ) AS identity_rank
            FROM (
                SELECT e.id, e.stock_code, e.trade_time, CASE WHEN
                    {exact_text('e.data_source')}={exact_text(':source')}
                    AND {exact_text('e.data_version')}={exact_text(':version')}
                    AND {exact_text('e.quality_status')}={exact_text(':quality')}
                    AND {exact_text('e.permission_status')}={exact_text(':permission')}
                    AND EXISTS (
                        SELECT 1 FROM `{STAGE_TABLE}` AS s{stage_index}
                        WHERE s.run_id=:run_id AND s.trade_date=:d AND {identity}
                          AND {indexed_stock_match('s', STAGE_TABLE, 'e', TARGET_TABLE)}
                          AND e.source_time=s.trade_time
                          AND {exact_text('e.qmt_code')}={exact_text('s.qmt_code')}
                          AND {numeric_match}
                    )
                    THEN 1 ELSE 0 END AS is_exact
                FROM `{TARGET_TABLE}` AS e{target_index}
                WHERE e.trade_date=:d AND e.data_source IN :sources AND e.stock_code IN :codes
                  AND (e.data_source=:source OR {exact_text('e.data_source')} IN :sources)
            ) AS compared
        ) AS ranked
        WHERE is_exact=0 OR identity_rank>1
    """).bindparams(bindparam("sources", expanding=True), bindparam("codes", expanding=True))
    delete = text(
        f"DELETE FROM `{TARGET_TABLE}` WHERE id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    # The caller holds the minute writer lock and one transaction throughout.
    # An empty target scope needs neither reconciliation nor an anti-join.
    target_join = f"""
            LEFT JOIN `{TARGET_TABLE}` AS e{target_index}
              ON e.data_source=:source AND {identity}
             AND {indexed_stock_match('e', TARGET_TABLE, 's', STAGE_TABLE)}
             AND {exact_text('e.data_source')}={exact_text(':source')}
    """ if existing_codes else ""
    missing_filter = "AND e.id IS NULL" if existing_codes else ""
    insert = text(f"""
            INSERT INTO `{TARGET_TABLE}` ({','.join(MINUTE_COLUMNS)})
            SELECT s.stock_code, s.trade_time, s.trade_date, s.price, s.`change`, s.change_pct,
                   s.volume, s.amount, NOW(), s.qmt_code, :source, s.trade_time, NOW(),
                   :batch_id, :version, :quality, :permission
            FROM `{STAGE_TABLE}` AS s{stage_index}
            {target_join}
            WHERE s.run_id=:run_id AND s.trade_date=:d AND s.stock_code IN :codes
              {missing_filter}
            ORDER BY s.stock_code, s.trade_time
        """).bindparams(bindparam("codes", expanding=True))
    processed_rows = inserted_rows = deleted_rows = 0
    for offset in range(0, len(ordered), batch_codes):
        if time.monotonic() >= deadline:
            raise TimeoutError("minute_publication: total deadline exceeded")
        batch = ordered[offset:offset + batch_codes]
        params = {
            "source": SOURCE,
            "sources": sources,
            "batch_id": batch_id,
            "version": DATA_VERSION,
            "quality": QUALITY_STATUS,
            "permission": PERMISSION_STATUS,
            "run_id": run_id,
            "d": session,
            "codes": batch,
        }
        ids = conn.execute(victims, params).scalars().all() if existing_codes else []
        for start in range(0, len(ids), 500):
            if time.monotonic() >= deadline:
                raise TimeoutError("minute_publication: total deadline exceeded")
            deleted = conn.execute(delete, {"ids": ids[start:start + 500]})
            deleted_rows += max(0, int(deleted.rowcount or 0))
        if time.monotonic() >= deadline:
            raise TimeoutError("minute_publication: total deadline exceeded")
        inserted = conn.execute(insert, params)
        processed_rows += sum(staged_counts.get(code, 0) for code in batch)
        inserted_rows += max(0, int(inserted.rowcount or 0))
        if progress is not None:
            progress.emit("server_publication", current_unit=batch[-1], completed_units=processed_rows,
                inserted_rows=inserted_rows, deleted_rows=deleted_rows,
                completed_securities=min(offset + len(batch), len(ordered)),
                total_securities=len(ordered), budget_seconds=deadline_seconds,
                budget_remaining_seconds=round(max(0.0, deadline - time.monotonic()), 6))
    if time.monotonic() >= deadline:
        raise TimeoutError("minute_publication: total deadline exceeded")


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


def drop_staged(conn: Connection, run_id: str, *, session: dt.date | None = None) -> None:
    predicate = " AND trade_date=:d" if session is not None else ""
    conn.execute(text(f"DELETE FROM `{STAGE_TABLE}` WHERE run_id=:r{predicate}"),
                 {"r": run_id, "d": session})


def new_run_id(now: dt.datetime | None = None) -> str:
    moment = now or dt.datetime.now(CST)
    return f"qdd_{moment.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def new_batch_id(session: dt.date, run_id: str) -> str:
    return f"qmt_dat_{session.strftime('%Y%m%d')}_{run_id}"

