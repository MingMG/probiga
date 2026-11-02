#!/usr/bin/env python3
"""Import the Guojin QMT local bar cache into the canonical minute history.

The cache under ``<datadir>/<MARKET>/60`` stays readable after QMT restricted
its built-in Python runtime, so this tool is the post-restriction acquisition
path for historical minutes.  It never talks to QMT.

Dry run (the default) prints the per-session plan.  ``--apply`` stages each
batch in MySQL and publishes one session at a time, writing a durable receipt
per session before and after the write.  ``--datadir-root`` may be repeated:
each root is harvested in the given order and a session is published from the
first root whose cache proves coverage.

Examples::

    python tools/import_qmt_datadir_history.py --start 2025-11-10 --end 2026-10-16
    python tools/import_qmt_datadir_history.py --start 2026-07-06 --end 2026-07-17 \
        --replace-existing --apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
from contextlib import contextmanager
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.datadir import CST, MINUTE_PERIOD, load_dat, iter_minute_rows, select_records  # noqa: E402
from integrations.qmt.datadir_import import (  # noqa: E402
    DEFAULT_CHUNK_ROWS,
    DEFAULT_SESSIONS_PER_BATCH,
    DATASET,
    SOURCE,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_RUNNING,
    require_schema,
    completed_sessions,
    drop_staged,
    existing_sources,
    harvest,
    load_universe,
    new_batch_id,
    new_run_id,
    plan,
    publish_session,
    write_receipt,
)
from server.common.config import get_minute_mysql_url  # noqa: E402
from server.common.engine_factory import create_pooled_engine  # noqa: E402
from server.common.qmt_index_minute_grid import CORE_GRID  # noqa: E402
from server.common.authoritative_market_clock import (  # noqa: E402
    authoritative_closed_trade_date,
)
from server.common.qmt_trade_calendar import load_trade_calendar_receipt  # noqa: E402
from server.common.qmt_datadir_result import (  # noqa: E402
    build_result,
    build_unavailable_result,
    load_receipt,
    partition_from_receipt,
)
from server.common.qmt_datadir_minute_coverage import (  # noqa: E402
    trade_scope, validate_code_scope, validate_matrix, persisted_matrix, staged_matrix,
)
from server.common.mysql_lock import mysql_named_lock, STOCK_MINUTE_FREEZE_LOCK_NAME  # noqa: E402
from server.common.qmt_attestation_contract import canonical_digest  # noqa: E402
from server.common.qmt_datadir_progress import MinuteImportProgress  # noqa: E402
from tools.env_config import load_project_env  # noqa: E402

DEFAULT_DATADIRS = (
    r"D:\国金证券QMT交易端\userdata_mini\datadir",
    r"D:\国金证券QMT交易端\datadir",
)
ACTIONABLE = {"IMPORT", "REPLACE"}


@contextmanager
def _minute_write_connection(engine):
    """Keep the existing minute freeze domain on the actual write connection."""
    if engine.dialect.name == "sqlite":
        with engine.connect() as connection:
            yield connection
        return
    with mysql_named_lock(engine, STOCK_MINUTE_FREEZE_LOCK_NAME) as connection:
        # GET_LOCK starts SQLAlchemy's implicit transaction, not business DML.
        connection.commit()
        yield connection


def _completion_detail(fingerprint):
    return {**{key: value for key, value in fingerprint.items() if key != "business_row_hash"},
            "readback_rows": fingerprint["row_count"],
            "readback_codes": fingerprint["code_count"]}


def _source_matrix(root, *, universe, session, scope, run_id=""):
    """Read native bytes once for content comparison; never land them in SQL."""
    digest = hashlib.sha256()
    fields = ("stock_code", "trade_time", "trade_date", "price", "change",
              "change_pct", "volume", "amount", "qmt_code")
    stored_scale = Decimal("0.000001")
    expected = set(scope["codes"])
    def rows():
        for member in universe:
            if member.code not in expected:
                continue
            dat = load_dat(root, member.market, MINUTE_PERIOD, member.code)
            if dat is None:
                continue
            selected = select_records(dat, [session])
            if not len(selected):
                continue
            digest.update(member.qmt_code.encode())
            digest.update(hashlib.sha256(selected.tobytes()).digest())
            for _, native in iter_minute_rows(dat, selected, CORE_GRID):
                row = dict(zip(fields, native))
                # Only the calculated percentage can exceed the stored scale.
                # MySQL DECIMAL(50,6) rounds ties away from zero and stores
                # unsigned zero. Keep the raw DAT digest and other values.
                pct = row["change_pct"]
                if pct.is_finite():
                    stored_pct = pct.quantize(stored_scale, rounding=ROUND_HALF_UP)
                    row["change_pct"] = stored_pct if stored_pct else stored_pct.copy_abs()
                yield {**row, "source_time": row["trade_time"], "data_source": SOURCE,
                    "batch_id": "native-content-comparison", "data_version": "qmt_datadir_v1",
                    "quality_status": "VERIFIED", "permission_status": "SUPPORTED"}
    progress = MinuteImportProgress(run_id=run_id, total_units=len(scope["codes"]) * len(CORE_GRID),
                                    operation="source_content_readback", days=[session])
    actual = validate_matrix(rows(), day=session.isoformat(), scope=scope, progress=progress,
                             include_business_fingerprint=True)
    return {"content_sha256": digest.hexdigest(), "business_row_hash": actual["business_row_hash"]}


def _prior_completed_partitions(connection, session, receipt):
    """Read existing publication provenance, including overwritten cursors."""
    from server.common.scheduler_validation import _qmt_datadir_payload

    candidates = []
    if receipt is not None and receipt.get("status") == STATUS_COMPLETE:
        candidates.append(partition_from_receipt(receipt))
    rows = connection.execute(text(
        "SELECT build_sha,output FROM st_scheduled_task_history "
        "WHERE task_type='qmt_stock_minute_canonical' AND status<>'running' "
        "AND finished_at IS NOT NULL AND exit_code=0 AND run_at>=:day "
        "ORDER BY id DESC LIMIT 20"
    ), {"day": session}).mappings()
    day = session.isoformat()
    for row in rows:
        payload = _qmt_datadir_payload(row["output"])
        if (not isinstance(payload, dict) or payload.get("status") != "PASS"
                or payload.get("dataset") != DATASET or payload.get("provider") != SOURCE
                or payload.get("task_type") != "qmt_stock_minute_canonical"
                or payload.get("execution") != {"source_kind": "local_qmt_datadir", "network_accessed": False}
                or payload.get("build_sha") != row["build_sha"]
                or payload.get("receipt_id") != canonical_digest(
                    {key: value for key, value in payload.items() if key != "receipt_id"})):
            continue
        for partition in payload.get("partitions", []):
            if partition.get("trade_date") == day and partition.get("status") == STATUS_COMPLETE:
                candidates.append(dict(partition))
    valid = []
    for item in candidates:
        try:
            rows, codes = int(item["row_count"]), int(item["code_count"])
            if (item.get("source_root") and item.get("run_id")
                    and rows == codes * len(CORE_GRID) and codes > 0
                    and int(item["readback_rows"]) == rows
                    and int(item["readback_codes"]) == codes
                    and all(len(str(item.get(key) or "")) == 64
                            and set(str(item[key])) <= set("0123456789abcdef")
                            for key in ("content_sha256", "row_hash"))):
                valid.append({**item, "row_count": rows, "code_count": codes})
        except (KeyError, TypeError, ValueError):
            continue
    return valid


def _reuse_existing_partition(engine, *, session, scope, datadir_roots=(), universe=()):
    """Rebind proven existing data to today's valid scope without rewriting rows."""
    receipt = load_receipt(engine, dataset=DATASET, trade_date=session.isoformat())
    with _minute_write_connection(engine) as connection:
        with connection.begin():
            candidates = _prior_completed_partitions(connection, session, receipt)
            candidates = [item for item in candidates
                          if item["code_count"] == len(scope["codes"])]
            if not candidates and not datadir_roots:
                return None
            # Do not trust an old threshold or an old row hash on its own:
            # verify the full current grid, stock set and daily close anchors.
            try:
                fingerprint = persisted_matrix(connection, session.isoformat(), scope=scope,
                    run_id=candidates[0]["run_id"] if candidates else receipt.get("run_id", "") if receipt else "",
                    operation="existing_partition_readback", include_business_fingerprint=bool(datadir_roots))
            except ValueError:
                return None
            original = next((item for item in candidates
                             if item["row_hash"] == fingerprint["row_hash"]
                             and item["row_count"] == fingerprint["row_count"]), None)
            if original is None:
                for root in datadir_roots:
                    try:
                        source = _source_matrix(root, universe=universe, session=session, scope=scope,
                            run_id=receipt.get("run_id", "") if receipt else "")
                    except ValueError:
                        continue
                    if source["business_row_hash"] == fingerprint["business_row_hash"]:
                        original = {"source_root": str(root), "run_id": new_run_id(),
                                    "content_sha256": source["content_sha256"]}
                        break
            if original is None:
                return None
            trade_scope(connection, session.isoformat(), manifest_hash=scope["full_market_manifest_hash"])
            write_receipt(connection, session=session, root=original["source_root"],
                status=STATUS_COMPLETE, run_id=original["run_id"],
                row_count=fingerprint["row_count"], code_count=fingerprint["code_count"],
                content_sha256=original["content_sha256"], finished=True,
                detail={**_completion_detail(fingerprint), "reused_existing_partition": True})
    print(f"{session} REUSED rows={fingerprint['row_count']} codes={fingerprint['code_count']}", flush=True)
    return {"session": session.isoformat(), "status": STATUS_COMPLETE,
            "rows": fingerprint["row_count"], "codes": fingerprint["code_count"],
            "run_id": original["run_id"], "reused_existing_partition": True}


def _resume_staged_partition(engine, *, session, scope, replace, replace_sources):
    receipt = load_receipt(engine, dataset=DATASET, trade_date=session.isoformat())
    if (receipt is None or receipt.get("status") not in {STATUS_FAILED, STATUS_RUNNING}
            or not receipt.get("detail", {}).get("staged_rows_retained")
            or int(receipt["code_count"]) != len(scope["codes"])
            or int(receipt["row_count"]) != len(scope["codes"]) * len(CORE_GRID)):
        return None
    with engine.connect() as connection:
        try:
            fingerprint = staged_matrix(connection, session.isoformat(), scope=scope,
                                        run_id=receipt["run_id"])
        except ValueError:
            return None
    entry = dict(rows=fingerprint["row_count"], codes=list(scope["codes"]),
        code_rows={code: len(CORE_GRID) for code in scope["codes"]}, digest=receipt["content_sha256"])
    return _publish_one(engine, session=session, entry=entry, run_id=receipt["run_id"],
        root=Path(receipt["source_root"]), replace=replace, replace_sources=replace_sources, scope=scope)


def _sessions(engine, start: dt.date, end: dt.date) -> list[dt.date]:
    with engine.connect() as conn:
        receipt = load_trade_calendar_receipt(
            conn,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            decision_known_at=dt.datetime.now(CST),
        )
    return [dt.date.fromisoformat(day) for day in receipt.sessions_between(
        start.isoformat(), end.isoformat())]


def _batched(items: list, size: int):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _build_sha() -> str:
    return str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()


def _minute_grid_shortfall(entry: dict) -> int:
    expected = len(CORE_GRID)
    return sum(
        1
        for count in entry["code_rows"].values()
        if int(count) != expected
    )


def _task_result(
    engine,
    *,
    sessions: list[dt.date],
) -> dict:
    partitions = []
    for session in sessions:
        receipt = load_receipt(
            engine,
            dataset=DATASET,
            trade_date=session.isoformat(),
        )
        if receipt is not None:
            partitions.append(partition_from_receipt(receipt))
    return build_result(
        dataset=DATASET,
        sessions=[session.isoformat() for session in sessions],
        partitions=partitions,
        build_sha=_build_sha(),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--latest-session", action="store_true")
    date_group.add_argument("--start", help="first trading session (inclusive)")
    parser.add_argument("--end", help="last trading session (inclusive)")
    parser.add_argument(
        "--datadir-root",
        action="append",
        default=None,
        help="cache root to harvest; repeat to name several, tried in order",
    )
    parser.add_argument("--apply", action="store_true", help="write to the database")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="replace sessions that already hold another source's rows",
    )
    parser.add_argument("--batch-sessions", type=int, default=DEFAULT_SESSIONS_PER_BATCH)
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_pooled_engine(get_minute_mysql_url(), pool_size=2, max_overflow=0,
        pool_timeout=10, connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 45})
    if args.latest_session:
        latest = authoritative_closed_trade_date(engine)
        start = end = dt.date.fromisoformat(latest)
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required together")
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    if end < start:
        parser.error("--end must not precede --start")
    datadir_roots = [
        Path(item)
        for item in (args.datadir_root or DEFAULT_DATADIRS)
    ]
    missing_roots = [str(root) for root in datadir_roots if not root.is_dir()]
    if missing_roots:
        parser.error(f"datadir root does not exist: {', '.join(missing_roots)}")

    sessions = _sessions(engine, start, end)
    if not sessions:
        parser.error("the QMT calendar has no session in the requested range")

    universe = load_universe(engine, start=start, end=end)
    plans = plan(
        existing_sources(engine, sessions),
        sessions,
        replace_existing=args.replace_existing,
        already_complete=completed_sessions(engine),
    )
    report = {
        "datadir_roots": [str(root) for root in datadir_roots],
        "universe_codes": len(universe),
        "sessions": [item.as_dict() for item in plans],
        "applied": bool(args.apply),
    }
    if not args.json:
        counts: dict[str, int] = {}
        for item in plans:
            counts[item.action] = counts.get(item.action, 0) + 1
        print(f"datadir={','.join(str(root) for root in datadir_roots)} "
              f"universe={len(universe)} sessions={len(plans)}")
        print("plan: " + " ".join(f"{key}={value}" for key, value in sorted(counts.items())))
        for item in plans:
            sources = ",".join(item.existing_sources)
            print(f"  {item.session} {item.action:<14}"
                  + (f" [{sources}]" if sources else ""))

    if not args.apply:
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=1))
        return 0

    todo = [item for item in plans if item.action in ACTIONABLE]
    if not todo:
        print("nothing to import")
        if args.json:
            print(json.dumps(
                _task_result(engine, sessions=sessions),
                ensure_ascii=False,
                separators=(",", ":"),
            ))
        return 0

    require_schema(engine)
    replace_sources = sorted({
        source
        for item in todo if item.action == "REPLACE"
        for source in item.existing_sources
    })
    ordered = universe.ordered()
    try:
        scopes = {item.session: trade_scope(engine, item.session.isoformat()) for item in todo}
    except ValueError as exc:
        print(json.dumps(build_unavailable_result(dataset=DATASET,
            sessions=[item.session.isoformat() for item in todo],
            reason=str(exc), build_sha=_build_sha()), ensure_ascii=False), flush=True)
        return 3
    results: list[dict] = []
    published: dict[dt.date, dict] = {}
    for item in todo:
        outcome = _reuse_existing_partition(engine, session=item.session, scope=scopes[item.session],
            datadir_roots=datadir_roots, universe=ordered)
        if outcome is None:
            outcome = _resume_staged_partition(engine, session=item.session, scope=scopes[item.session],
                replace=item.action == "REPLACE", replace_sources=replace_sources)
        if outcome is not None:
            published[item.session] = outcome
            results.append(outcome)
    # The client keeps only its own recent window, so an older snapshot may
    # hold the fuller cache for the same date.  Roots are tried in the given
    # order and a session is published from the first root that proves
    # coverage; a thin root leaves the date open for the next one.
    shortfalls: dict[dt.date, dict] = {}

    for root in datadir_roots:
        pending = [item for item in todo if item.session not in published]
        if not pending:
            break
        for group in _batched(pending, max(1, args.batch_sessions)):
            run_id = new_run_id()
            wanted = [item.session for item in group]
            with engine.connect() as conn:
                stats = harvest(
                    conn,
                    root=root,
                    universe=ordered,
                    sessions=wanted,
                    run_id=run_id,
                    chunk_rows=args.chunk_rows,
                    codes_by_session={day: set(scopes[day]["codes"]) for day in wanted},
                )
            for item in group:
                session = item.session
                entry = stats[session]
                if entry["rows"] == 0:
                    shortfalls[session] = {"run_id": run_id, "root": str(root),
                                           "reason": "no_rows_in_datadir",
                                           "imported_codes": 0, "eligible_codes":
                                           universe.eligible_codes(session)}
                    continue
                bad_grid_codes = _minute_grid_shortfall(entry)
                if bad_grid_codes:
                    shortfalls[session] = {
                        "run_id": run_id,
                        "root": str(root),
                        "reason": "incomplete_minute_grid",
                        "rows": entry["rows"],
                        "imported_codes": len(entry["codes"]),
                        "eligible_codes": universe.eligible_codes(session),
                        "digest": entry["digest"],
                        "bad_grid_codes": bad_grid_codes,
                    }
                    continue
                try:
                    validate_code_scope(entry, scopes[session])
                except ValueError as exc:
                    shortfalls[session] = {
                        "run_id": run_id, "root": str(root),
                        "reason": str(exc), "rows": entry["rows"],
                        "imported_codes": len(entry["codes"]),
                        "eligible_codes": scopes[session]["eligible_code_count"], "digest": entry["digest"],
                    }
                    continue
                outcome = _publish_one(
                    engine, session=session, entry=entry, run_id=run_id, root=root,
                    replace=item.action == "REPLACE", replace_sources=replace_sources,
                    scope=scopes[session],
                )
                published[session] = outcome
                results.append(outcome)
            with engine.begin() as conn:
                for item in group:
                    if published.get(item.session, {}).get("status") == STATUS_COMPLETE:
                        drop_staged(conn, run_id, session=item.session)

    failures = 0
    for item in todo:
        session = item.session
        if session in published:
            if published[session]["status"] != STATUS_COMPLETE:
                failures += 1
            continue
        failure = shortfalls.get(session, {})
        failures += 1
        with engine.begin() as conn:
            write_receipt(
                conn, session=session, root=failure.get("root", ""),
                status=STATUS_FAILED, run_id=failure.get("run_id", ""),
                row_count=failure.get("rows", 0),
                code_count=failure.get("imported_codes", 0),
                content_sha256=failure.get("digest", ""), finished=True,
                detail={"reason": failure.get("reason", "not_attempted"),
                        "eligible_codes": failure.get("eligible_codes", 0),
                        "imported_codes": failure.get("imported_codes", 0),
                        "bad_grid_codes": failure.get("bad_grid_codes", 0)},
            )
        print(f"{session} FAILED {failure.get('reason', 'not_attempted')} "
              f"imported_codes={failure.get('imported_codes', 0)} "
              f"eligible_codes={failure.get('eligible_codes', 0)}")
        results.append({"session": session.isoformat(), "status": STATUS_FAILED,
                        "reason": failure.get("reason", "not_attempted"),
                        "root": failure.get("root", ""),
                        "imported_codes": failure.get("imported_codes", 0),
                        "eligible_codes": failure.get("eligible_codes", 0)})

    report["results"] = results
    report["failures"] = failures
    if args.json:
        print(json.dumps(
            _task_result(engine, sessions=sessions),
            ensure_ascii=False,
            separators=(",", ":"),
        ))
    return 1 if failures else 0


def _publish_one(
    engine,
    *,
    session,
    entry,
    run_id,
    root,
    replace,
    replace_sources,
    scope,
):
    """Stage-to-canonical publication of one session, proven by readback."""
    progress = MinuteImportProgress(run_id=run_id, total_units=entry["rows"],
        operation="publication", days=[session])
    try:
        with _minute_write_connection(engine) as conn:
            with conn.begin():
                write_receipt(
                    conn, session=session, root=str(root), status=STATUS_RUNNING,
                    run_id=run_id, row_count=entry["rows"], code_count=len(entry["codes"]),
                    content_sha256=entry["digest"], detail={"staged_rows_retained": True},
                )
            with conn.begin():
                outcome = _publish_locked(conn, session=session, entry=entry, run_id=run_id,
                    root=root, replace=replace, replace_sources=replace_sources,
                    scope=scope, progress=progress)
        progress.emit("canonical_commit", status="COMPLETE", completed_units=outcome["rows"])
        print(f"{session} COMPLETE rows={outcome['rows']} codes={outcome['codes']} root={root}")
        return outcome
    except Exception as exc:  # noqa: BLE001 - the receipt records the cause
        progress.emit("publication", status="FAILED", error_type=type(exc).__name__)
        with engine.begin() as conn:
            write_receipt(
                conn, session=session, root=str(root),
                status=STATUS_FAILED, run_id=run_id,
                row_count=entry["rows"], code_count=len(entry["codes"]),
                content_sha256=entry["digest"], finished=True,
                detail={"error": f"{type(exc).__name__}: {exc}", "staged_rows_retained": True},
            )
        print(f"{session} FAILED {type(exc).__name__}: {exc}")
        return {"session": session.isoformat(), "status": STATUS_FAILED,
                "root": str(root), "error": str(exc)}


def _publish_locked(conn, *, session, entry, run_id, root, replace, replace_sources, scope, progress):
    # Recheck the frozen daily scope before replacing any live minute.
    trade_scope(conn, session.isoformat(), manifest_hash=scope["full_market_manifest_hash"])
    published_started = time.monotonic()
    progress.emit("server_publication")
    publish_session(conn, session=session, run_id=run_id,
        batch_id=new_batch_id(session, run_id),
        replace_sources=replace_sources if replace else (), progress=progress)
    progress.emit("server_publication", status="COMPLETE",
        phase_seconds=round(time.monotonic()-published_started, 6))
    fingerprint = persisted_matrix(conn, session.isoformat(), scope=scope,
        run_id=run_id, operation="publication_readback")
    rows, codes = fingerprint["row_count"], fingerprint["code_count"]
    if rows != entry["rows"] or codes != len(entry["codes"]):
        raise ValueError("exact minute matrix differs from harvested partition")
    write_receipt(conn, session=session, root=str(root), status=STATUS_COMPLETE,
        run_id=run_id, row_count=rows, code_count=codes,
        content_sha256=entry["digest"], finished=True, detail=_completion_detail(fingerprint))
    return {"session": session.isoformat(), "status": STATUS_COMPLETE,
            "rows": rows, "codes": codes, "root": str(root)}


if __name__ == "__main__":
    raise SystemExit(main())
