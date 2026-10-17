"""Offline import of the QMT client's own native bar files.

The import is source-inventory driven: a code-day is written only when the
source session is complete against the canonical clock. Dates the minute table
already covers are left alone unless replacement is explicitly requested;
daily rows are compared per security. Nothing starts QMT, downloads history or
calls the bridge.

Usage::

    python -m acquisition.local_dat_import \
        --config E:/ProBigA-direct-acquisition-data/direct-acquisition.json \
        --dataset stock_minute_local \
        --datadir "D:/国金证券QMT交易端/userdata_mini/datadir" \
        --start 2025-10-17 --end 2026-07-17 \
        --report minute-import.json --execute

Without ``--execute`` the same inventory is computed and the report says what
would be written; no row and no partition state is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from .config import Config
from .datasets import get_spec
from .minute_grid import proof as minute_grid_proof
from .models import NormalizedBatch, NormalizedUnit, WorkUnit
from .sources.local_dat import (DatFormatError, TIMEFRAME_DIR, bars_to_rows,
                                read_bars, session_coverage)
from .store import StaleRequest, Store, safe_error

SHANGHAI = ZoneInfo("Asia/Shanghai")
LOCAL_SOURCE_METHOD = "local_dat"
DATASETS = ("stock_daily_local", "stock_minute_local")
_REPORT_ERRORS = 200


def _sha(value) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str,
                         allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request_id(spec, code, units) -> str:
    parts = []
    for result in units:
        if spec.period == "1m":
            content = result.detail["minute_grid_proof"]["content_sha256"]
        else:
            content = result.detail["content_sha256"]
        parts.append((result.unit.target_date, content))
    return _sha({"dataset": spec.name, "source": spec.source, "code": code,
                 "period": spec.period, "parts": sorted(parts)})


def existing_coverage(store, spec, days):
    """What the database already covers inside the requested dates.

    A per-code bar count for one minute date exceeds the production
    statement budget (measured over 120 s on 2026-10-17), so minute coverage
    uses the table's indexed date bounds: a date inside them is left alone
    unless replacement is explicitly requested. Daily rows are small enough
    to read the exact code set per date.
    """
    table = store.table(spec.table)
    coverage = {"dates": set(), "codes": {}}
    if not days:
        return coverage
    with store.engine.connect() as conn:
        if spec.period == "1m":
            first, last = conn.execute(
                select(func.min(table.c.trade_date), func.max(table.c.trade_date))).one()
            if first is not None:
                coverage["dates"] = {day for day in days if first <= day <= last}
            return coverage
        for day in days:
            rows = conn.execute(select(table.c[spec.code_column]).where(
                table.c.trade_date == day, table.c.k_type == 1, table.c.adjust_type == 0))
            coverage["codes"][day] = {str(code).zfill(6) for (code,) in rows}
    return coverage


def _write_batch(store, spec, batch, *, reclaim):
    units = [result.unit for result in batch.units]
    try:
        store.begin_request(units, batch.request_id, batch.received_at)
    except StaleRequest:
        if not reclaim:
            raise
        store.reclaim_stale_running(spec, units[0].code, batch.received_at)
        store.begin_request(units, batch.request_id, batch.received_at)
    try:
        return store.write_partitions(spec, batch)
    except StaleRequest:
        if not reclaim:
            raise
        store.reclaim_stale_running(spec, units[0].code, batch.received_at)
        store.begin_request(units, batch.request_id, batch.received_at)
        return store.write_partitions(spec, batch)
    except Exception as exc:
        store.fail_request(units, batch.request_id, safe_error(exc), batch.received_at)
        raise


def run(args):
    spec = get_spec(args.dataset)
    if spec.name not in DATASETS:
        raise ValueError(f"not an offline native-file dataset: {spec.name}")
    date_range_s, date_range_e = args.start, args.end
    today = date.today()
    start = date.fromisoformat(date_range_s) if date_range_s else today - timedelta(days=366)
    end = date.fromisoformat(date_range_e) if date_range_e else today
    if start > end:
        raise ValueError("start follows end")
    datadir = Path(args.datadir).resolve()
    if not datadir.is_dir():
        raise ValueError(f"datadir does not exist: {datadir}")
    datadirs = [str(datadir)]
    for extra in args.datadir_all or []:
        candidate = Path(extra).resolve()
        if not candidate.is_dir():
            raise ValueError(f"datadir does not exist: {candidate}")
        datadirs.append(str(candidate))
    code_filter = {item.strip() for item in (args.codes or "").split(",") if item.strip()}
    timeframe = "1m" if spec.period == "1m" else "1d"

    config = Config.load(args.config)
    if args.execute:
        config.require_writes()
    engine = config.engine(spec.database)
    store = Store(engine)
    store.validate_spec(spec)
    started = datetime.now(SHANGHAI)
    clock = time.monotonic()
    report = {
        "status": "running", "dataset": spec.name, "datadirs": datadirs,
        "state_dir_source": str(Path(args.config).resolve()),
        "range": [start.isoformat(), end.isoformat()], "execute": bool(args.execute),
        "replace_existing": bool(args.replace_existing),
        "started_at": started.isoformat(timespec="seconds"),
        "catalog_codes": 0, "codes_seen": 0, "codes_written": 0,
        "codes_missing_file": 0, "codes_not_in_catalog": 0,
        "partitions": {"planned": 0, "imported": 0, "replayed": 0,
                       "existing_complete": 0, "source_partial": 0,
                       "source_extended": 0, "errors": 0},
        "rows_planned": 0, "rows_written": 0,
        "dates": defaultdict(lambda: {"source_full": 0, "existing_complete": 0,
                                      "partial": 0, "imported": 0, "rows": 0}),
        "errors": [],
    }
    error_count = 0

    def record_error(code, day, message):
        nonlocal error_count
        error_count += 1
        if len(report["errors"]) < _REPORT_ERRORS:
            report["errors"].append({"code": code, "date": day, "error": message})

    try:
        catalog = store.catalog("stock")
        report["catalog_codes"] = len(catalog)
        selected = []
        for qmt_code in catalog:
            bare = qmt_code.split(".")[0]
            if code_filter and bare not in code_filter and qmt_code not in code_filter:
                continue
            selected.append(qmt_code)
        if args.limit_codes:
            selected = selected[:args.limit_codes]
        days = [start + timedelta(days=offset)
                for offset in range((end - start).days + 1)]
        coverage_db = existing_coverage(store, spec, days)

        for position, qmt_code in enumerate(selected, 1):
            bare = qmt_code.split(".")[0]
            market = qmt_code.rsplit(".", 1)[1]
            path = None
            for root in datadirs:
                candidate = Path(root) / market / TIMEFRAME_DIR[timeframe] / f"{bare}.DAT"
                if candidate.is_file():
                    path = candidate
                    break
            if path is None:
                report["codes_missing_file"] += 1
                continue
            try:
                frame = read_bars(path, timeframe)
                frame = frame[(frame["trade_date"] >= start) & (frame["trade_date"] <= end)]
                if frame.empty:
                    continue
                report["codes_seen"] += 1
                coverage = ({item.session: item for item in session_coverage(frame)}
                            if spec.period == "1m" else {})
                units = []
                for raw_day, group in frame.groupby("trade_date", sort=True):
                    day_text = str(raw_day)
                    stats = report["dates"][day_text]
                    if spec.period == "1m":
                        item = coverage.get(day_text)
                        if item is None or item.status != "full":
                            state = item.status if item else "missing"
                            report["partitions"]["source_partial"] += 1
                            stats["partial"] += 1
                            if state == "extended":
                                report["partitions"]["source_extended"] += 1
                            continue
                    stats["source_full"] += 1
                    if spec.period == "1m":
                        already = raw_day in coverage_db["dates"]
                    else:
                        already = bare in coverage_db["codes"].get(raw_day, set())
                    if already and not args.replace_existing:
                        report["partitions"]["existing_complete"] += 1
                        stats["existing_complete"] += 1
                        continue
                    rows = bars_to_rows(group, timeframe, qmt_code)
                    if not rows:
                        continue
                    unit = WorkUnit(spec.name, spec.source, day_text, qmt_code,
                                    spec.period, "none")
                    detail = {"traded": True, "source_method": LOCAL_SOURCE_METHOD,
                              "source_path": str(path)}
                    if spec.period == "1m":
                        detail.update(
                            minute_grid_proof=minute_grid_proof(spec, unit, rows),
                            missing_expected_rows=0, out_of_scope_rows=0)
                    else:
                        detail.update(content_sha256=_sha(rows), sessions=1)
                    units.append(NormalizedUnit(unit=unit, status="complete",
                                                rows=rows, detail=detail))
                if not units:
                    continue
                report["partitions"]["planned"] += len(units)
                report["rows_planned"] += sum(len(item.rows) for item in units)
                if args.execute:
                    batch = NormalizedBatch(request_id=_request_id(spec, qmt_code, units),
                                            units=units,
                                            received_at=datetime.now(SHANGHAI))
                    counts = _write_batch(store, spec, batch, reclaim=args.reclaim_stale)
                    report["partitions"]["imported"] += counts["complete"]
                    report["partitions"]["replayed"] += counts["replayed"]
                    report["rows_written"] += counts["written_rows"]
                    report["codes_written"] += 1
                    for item in units:
                        day_text = item.unit.target_date
                        report["dates"][day_text]["imported"] += 1
                        report["dates"][day_text]["rows"] += len(item.rows)
                else:
                    report["codes_written"] += 1
                elapsed = time.monotonic() - clock
                rate = report["codes_written"] / elapsed if elapsed > 0 else 0
                print(f"[{position}/{len(selected)}] {qmt_code} {path.name} "
                      f"partitions={len(units)} rows={sum(len(u.rows) for u in units)} "
                      f"rate={rate:.1f} codes/s", file=sys.stderr, flush=True)
            except (DatFormatError, OSError, ValueError, KeyError) as exc:
                record_error(qmt_code, "", f"{type(exc).__name__}: {exc}")
                print(f"[{position}/{len(selected)}] {qmt_code} ERROR {type(exc).__name__}",
                      file=sys.stderr, flush=True)

        report["status"] = ("error" if error_count else
                            "planned" if not args.execute else "complete")
        if not error_count and report["partitions"]["source_partial"]:
            report["status"] = "partial" if args.execute else "planned_partial"
    except Exception:
        report["status"] = "error"
        raise
    finally:
        report["partitions"]["errors"] = error_count
        report["finished_at"] = datetime.now(SHANGHAI).isoformat(timespec="seconds")
        report["elapsed_seconds"] = round(time.monotonic() - clock, 1)
        report["dates"] = dict(sorted(report["dates"].items()))
        engine.dispose()

    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.report:
        Path(args.report).write_text(payload + "\n", encoding="utf-8")
        print(f"wrote {args.report}", file=sys.stderr)
    else:
        print(payload)
    print(f"status={report['status']} planned={report['partitions']['planned']} "
          f"imported={report['partitions']['imported']} "
          f"existing={report['partitions']['existing_complete']} "
          f"partial={report['partitions']['source_partial']} "
          f"errors={error_count} rows={report['rows_written'] or report['rows_planned']}",
          file=sys.stderr)
    return 2 if error_count else 0


def parser():
    root = argparse.ArgumentParser(
        description="Import native QMT .DAT bars into the direct acquisition tables")
    root.add_argument("--config", required=True,
                      help="explicit protected direct-acquisition configuration JSON")
    root.add_argument("--dataset", required=True, choices=DATASETS)
    root.add_argument("--datadir", required=True, help="QMT datadir holding SH/SZ/BJ")
    root.add_argument("--datadir-all", nargs="+", help="ordered datadir fallbacks")
    root.add_argument("--start", help="first date, YYYY-MM-DD (default: 366 days ago)")
    root.add_argument("--end", help="last date, YYYY-MM-DD (default: today)")
    root.add_argument("--codes", help="comma-separated bare or qualified symbols")
    root.add_argument("--limit-codes", type=int, help="bounded canary over the catalog")
    root.add_argument("--replace-existing", action="store_true",
                      help="replace database partitions that already hold a complete grid")
    root.add_argument("--reclaim-stale", action="store_true",
                      help="recover orphaned running rows while holding the dataset lock")
    root.add_argument("--execute", action="store_true",
                      help="write; without it the run only plans and reports")
    root.add_argument("--report", help="write the JSON report to this path")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        print(json.dumps({"status": "interrupted", "error": "KeyboardInterrupt"}))
        return 130
    except Exception as exc:
        # SQL and provider errors can embed credentials or values. The class
        # name and a safe code are enough for a bounded CLI failure.
        print(json.dumps({"status": "error", "error": type(exc).__name__,
                          "message": safe_error(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
