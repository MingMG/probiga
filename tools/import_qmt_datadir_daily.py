#!/usr/bin/env python3
"""Import QMT local daily bars into the canonical stock daily table."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sys
import uuid
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.datadir import (  # noqa: E402
    CST,
    DAILY_PERIOD,
    iter_daily_rows,
    load_dat,
    session_slice,
)
from integrations.qmt.datadir_import import (  # noqa: E402
    load_universe,
    new_run_id,
)
from server.common.authoritative_market_clock import (  # noqa: E402
    authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.qmt_trade_calendar import (  # noqa: E402
    load_trade_calendar_receipt,
)
from tools.env_config import load_project_env  # noqa: E402

DATASET = "stock_daily"
SOURCE = "qmt_datadir"
TARGET_TABLE = "sm_stock_kline"
RECEIPT_TABLE = "qmt_datadir_import_partition"
MIN_COVERAGE = 0.95


def _sessions(engine, start: dt.date, end: dt.date) -> list[dt.date]:
    with engine.connect() as connection:
        receipt = load_trade_calendar_receipt(
            connection,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            decision_known_at=dt.datetime.now(CST),
        )
    return [
        dt.date.fromisoformat(day)
        for day in receipt.sessions_between(
            start.isoformat(), end.isoformat()
        )
    ]


def _existing_sources(engine, session: dt.date) -> list[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"SELECT DISTINCT data_source FROM `{TARGET_TABLE}` "
                "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
            ),
            {"d": session},
        ).fetchall()
    return sorted(str(row[0]) for row in rows if row[0])


def _short_names(engine) -> dict[str, str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT stock_code, short_name FROM si_all_code")
        ).fetchall()
    return {
        str(row[0]).zfill(6): str(row[1] or "")
        for row in rows
    }


def _harvest(root: Path, universe, session: dt.date) -> dict:
    rows = []
    codes = set()
    digest = hashlib.sha256()
    for member in universe.ordered():
        dat = load_dat(root, member.market, DAILY_PERIOD, member.code)
        if dat is None:
            continue
        selected = session_slice(dat.records, session)
        if not len(selected):
            continue
        digest.update(member.qmt_code.encode("ascii"))
        digest.update(selected.tobytes())
        for _, row in iter_daily_rows(dat, [session]):
            rows.append(row)
            codes.add(row[0])
    return {
        "rows": rows,
        "codes": codes,
        "digest": digest.hexdigest(),
    }


def _publish(
    engine,
    *,
    session: dt.date,
    rows: list[tuple],
    covered_codes: set[str],
    short_names: dict[str, str],
    run_id: str,
    source_root: Path,
    content_sha256: str,
) -> dict:
    now = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    batch_id = f"qmt_dat_daily_{session:%Y%m%d}_{run_id}"
    statement = text(
        f"""
        INSERT INTO `{TARGET_TABLE}`
          (stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
           open, close, high, low, volume, amount, change, change_pct,
           pre_close, etl_sync_at, qmt_code, data_source, source_time,
           received_at, batch_id, data_version, quality_status,
           permission_status)
        VALUES
          (:stock_code, :short_name, :trade_time, :trade_date, 1, 0,
           :open, :close, :high, :low, :volume, :amount, :change,
           :change_pct, :pre_close, :etl_sync_at, :qmt_code, :data_source,
           :source_time, :received_at, :batch_id, :data_version,
           'VERIFIED', 'SUPPORTED')
        ON DUPLICATE KEY UPDATE
          short_name=VALUES(short_name), trade_time=VALUES(trade_time),
          open=VALUES(open), close=VALUES(close), high=VALUES(high),
          low=VALUES(low), volume=VALUES(volume), amount=VALUES(amount),
          change=VALUES(change), change_pct=VALUES(change_pct),
          pre_close=VALUES(pre_close), etl_sync_at=VALUES(etl_sync_at),
          qmt_code=VALUES(qmt_code), data_source=VALUES(data_source),
          source_time=VALUES(source_time), received_at=VALUES(received_at),
          batch_id=VALUES(batch_id), data_version=VALUES(data_version),
          quality_status=VALUES(quality_status),
          permission_status=VALUES(permission_status)
        """
    )
    payload = []
    for (
        code,
        _native_time,
        _trade_date,
        open_price,
        close_price,
        high,
        low,
        volume,
        amount,
        pre_close,
        change,
        change_pct,
    ) in rows:
        market = (
            "SH" if code.startswith("6")
            else "BJ" if code.startswith(("4", "8", "9"))
            else "SZ"
        )
        source_time = dt.datetime.combine(session, dt.time(15, 0))
        payload.append({
            "stock_code": code,
            "short_name": short_names.get(code, ""),
            "trade_time": source_time,
            "trade_date": session,
            "open": open_price,
            "close": close_price,
            "high": high,
            "low": low,
            "volume": volume,
            "amount": amount,
            "change": change,
            "change_pct": change_pct,
            "pre_close": pre_close,
            "etl_sync_at": now,
            "qmt_code": f"{code}.{market}",
            "data_source": SOURCE,
            "source_time": source_time,
            "received_at": now,
            "batch_id": batch_id,
            "data_version": hashlib.sha256(
                (
                    f"{code}|{session}|{open_price}|{close_price}|"
                    f"{high}|{low}|{volume}|{amount}|{pre_close}"
                ).encode("utf-8")
            ).hexdigest()[:32],
        })
    with engine.begin() as connection:
        for offset in range(0, len(payload), 1000):
            connection.execute(statement, payload[offset:offset + 1000])
        connection.execute(
            text(
                f"DELETE FROM `{TARGET_TABLE}` "
                "WHERE trade_date=:d AND k_type=1 AND adjust_type=0 "
                "AND data_source<>:source"
            ),
            {"d": session, "source": SOURCE},
        )
        connection.execute(
            text(
                f"""
                INSERT INTO `{RECEIPT_TABLE}`
                  (dataset, trade_date, source_root, status, code_count,
                   row_count, content_sha256, run_id, started_at, finished_at,
                   detail_json)
                VALUES
                  (:dataset, :d, :root, 'COMPLETE', :codes, :rows, :sha,
                   :run_id, NOW(), NOW(), :detail)
                ON DUPLICATE KEY UPDATE
                  source_root=VALUES(source_root), status=VALUES(status),
                  code_count=VALUES(code_count), row_count=VALUES(row_count),
                  content_sha256=VALUES(content_sha256),
                  run_id=VALUES(run_id), finished_at=VALUES(finished_at),
                  detail_json=VALUES(detail_json)
                """
            ),
            {
                "dataset": DATASET,
                "d": session,
                "root": str(source_root),
                "codes": len(covered_codes),
                "rows": len(rows),
                "sha": content_sha256,
                "run_id": run_id,
                "detail": "{}",
            },
        )
    with engine.connect() as connection:
        durable = connection.execute(
            text(
                f"SELECT COUNT(*), COUNT(DISTINCT stock_code) "
                f"FROM `{TARGET_TABLE}` WHERE trade_date=:d "
                "AND k_type=1 AND adjust_type=0 AND data_source=:source"
            ),
            {"d": session, "source": SOURCE},
        ).one()
    return {
        "row_count": len(rows),
        "code_count": len(covered_codes),
        "readback_rows": int(durable[0]),
        "readback_codes": int(durable[1]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--latest-session", action="store_true")
    date_group.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument(
        "--datadir-root",
        default=r"D:\国金证券QMT交易端\datadir",
    )
    parser.add_argument("--replace-existing", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    load_project_env()
    engine = create_batch_engine()
    if args.latest_session:
        start = end = dt.date.fromisoformat(
            authoritative_closed_trade_date(engine)
        )
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required together")
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    root = Path(args.datadir_root)
    if not root.is_dir():
        parser.error(f"datadir root does not exist: {root}")
    sessions = _sessions(engine, start, end)
    universe = load_universe(engine, start=start, end=end)
    short_names = _short_names(engine)
    results = []
    failures = 0
    for session in sessions:
        sources = _existing_sources(engine, session)
        if sources and not args.replace_existing:
            results.append({
                "session": session.isoformat(),
                "status": "SKIP_EXISTING",
                "sources": sources,
            })
            continue
        harvested = _harvest(root, universe, session)
        eligible = universe.eligible_codes(session)
        required = math.ceil(eligible * MIN_COVERAGE)
        if len(harvested["codes"]) < required:
            failures += 1
            results.append({
                "session": session.isoformat(),
                "status": "FAILED",
                "reason": "insufficient_code_coverage",
                "codes": len(harvested["codes"]),
                "eligible": eligible,
                "required": required,
            })
            continue
        if not args.apply:
            results.append({
                "session": session.isoformat(),
                "status": "READY",
                "rows": len(harvested["rows"]),
                "codes": len(harvested["codes"]),
            })
            continue
        published = _publish(
            engine,
            session=session,
            rows=harvested["rows"],
            covered_codes=harvested["codes"],
            short_names=short_names,
            run_id=new_run_id(),
            source_root=root,
            content_sha256=harvested["digest"],
        )
        results.append({
            "session": session.isoformat(),
            "status": "COMPLETE",
            **published,
        })
    payload = {
        "schema": "probiga.qmt-datadir-daily-import.v1",
        "source": SOURCE,
        "datadir_root": str(root),
        "applied": bool(args.apply),
        "failures": failures,
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
