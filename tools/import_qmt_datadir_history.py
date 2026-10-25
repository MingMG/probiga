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
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.datadir import CST  # noqa: E402
from integrations.qmt.datadir_import import (  # noqa: E402
    DEFAULT_CHUNK_ROWS,
    DEFAULT_MIN_COVERAGE,
    DEFAULT_SESSIONS_PER_BATCH,
    REASON_THIN_CACHE,
    SOURCE,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_RUNNING,
    coverage_shortfall,
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
    readback_session,
    write_receipt,
)
from server.common.minute_data import get_minute_engine  # noqa: E402
from server.common.qmt_trade_calendar import load_trade_calendar_receipt  # noqa: E402
from tools.env_config import load_project_env  # noqa: E402

DEFAULT_DATADIRS = (
    r"D:\国金证券QMT交易端\userdata_mini\datadir",
    r"D:\国金证券QMT交易端\datadir",
)
ACTIONABLE = {"IMPORT", "REPLACE"}


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, help="first trading session (inclusive)")
    parser.add_argument("--end", required=True, help="last trading session (inclusive)")
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
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=DEFAULT_MIN_COVERAGE,
        help="fraction of the day's catalog-eligible codes the cache must hold",
    )
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    args = parser.parse_args(argv)

    load_project_env()
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

    engine = get_minute_engine()
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
            print(json.dumps(report, ensure_ascii=False, indent=1))
        return 0

    require_schema(engine)
    replace_sources = sorted({
        source
        for item in todo if item.action == "REPLACE"
        for source in item.existing_sources
    })
    ordered = universe.ordered()
    results: list[dict] = []
    published: dict[dt.date, dict] = {}
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
                shortfall = coverage_shortfall(universe, session, len(entry["codes"]),
                                               args.min_coverage)
                if shortfall is not None:
                    # A cached fragment of a day is a gap, not a session:
                    # publishing it would make the date look covered while
                    # most instruments are missing from it.
                    shortfalls[session] = {
                        "run_id": run_id, "root": str(root),
                        "reason": REASON_THIN_CACHE, "rows": entry["rows"],
                        "imported_codes": len(entry["codes"]),
                        "eligible_codes": shortfall, "digest": entry["digest"],
                    }
                    continue
                outcome = _publish_one(
                    engine, session=session, entry=entry, run_id=run_id, root=root,
                    replace=item.action == "REPLACE", replace_sources=replace_sources,
                )
                published[session] = outcome
                results.append(outcome)
            with engine.begin() as conn:
                drop_staged(conn, run_id)

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
                        "imported_codes": failure.get("imported_codes", 0)},
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
        print(json.dumps(report, ensure_ascii=False, indent=1))
    return 1 if failures else 0


def _publish_one(engine, *, session, entry, run_id, root, replace, replace_sources):
    """Stage-to-canonical publication of one session, proven by readback."""
    with engine.begin() as conn:
        write_receipt(
            conn, session=session, root=str(root), status=STATUS_RUNNING,
            run_id=run_id, row_count=entry["rows"], code_count=len(entry["codes"]),
            content_sha256=entry["digest"],
        )
    try:
        with engine.begin() as conn:
            publish_session(
                conn,
                session=session,
                run_id=run_id,
                batch_id=new_batch_id(session, run_id),
                replace_sources=replace_sources if replace else (),
            )
        with engine.begin() as conn:
            rows, codes = readback_session(conn, session)
            ok = rows == entry["rows"] and codes == len(entry["codes"])
            write_receipt(
                conn, session=session, root=str(root),
                status=STATUS_COMPLETE if ok else STATUS_FAILED, run_id=run_id,
                row_count=entry["rows"], code_count=len(entry["codes"]),
                content_sha256=entry["digest"], finished=True,
                detail={"readback_rows": rows, "readback_codes": codes},
            )
    except Exception as exc:  # noqa: BLE001 - the receipt records the cause
        with engine.begin() as conn:
            write_receipt(
                conn, session=session, root=str(root),
                status=STATUS_FAILED, run_id=run_id,
                row_count=entry["rows"], code_count=len(entry["codes"]),
                content_sha256=entry["digest"], finished=True,
                detail={"error": f"{type(exc).__name__}: {exc}"},
            )
        print(f"{session} FAILED {type(exc).__name__}: {exc}")
        return {"session": session.isoformat(), "status": STATUS_FAILED,
                "root": str(root), "error": str(exc)}
    if ok:
        print(f"{session} COMPLETE rows={rows} codes={codes} root={root}")
        return {"session": session.isoformat(), "status": STATUS_COMPLETE,
                "rows": rows, "codes": codes, "root": str(root)}
    print(f"{session} FAILED expected rows={entry['rows']} codes={len(entry['codes'])} "
          f"readback rows={rows} codes={codes}")
    return {"session": session.isoformat(), "status": STATUS_FAILED,
            "rows": rows, "codes": codes, "root": str(root),
            "expected_rows": entry["rows"],
            "expected_codes": len(entry["codes"])}


if __name__ == "__main__":
    raise SystemExit(main())
