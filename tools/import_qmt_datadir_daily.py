#!/usr/bin/env python3
"""Import QMT local daily bars into the canonical stock daily table."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
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
from server.common.qmt_datadir_result import (  # noqa: E402
    build_result,
    load_receipt,
    partition_from_receipt,
    persisted_fingerprint,
)
from tools.env_config import load_project_env  # noqa: E402

DATASET = "stock_daily"
SOURCE = "qmt_datadir"
TARGET_TABLE = "sm_stock_kline"
RECEIPT_TABLE = "qmt_datadir_import_partition"
MIN_COVERAGE = 0.95


def _build_sha() -> str:
    return str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()


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
    eligible_code_count: int,
) -> dict:
    now = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    batch_id = f"qmt_dat_daily_{session:%Y%m%d}_{run_id}"
    columns = (
        "stock_code, short_name, trade_time, trade_date, k_type, adjust_type, "
        "open, close, high, low, volume, amount, `change`, change_pct, "
        "pre_close, etl_sync_at, qmt_code, data_source, source_time, "
        "received_at, batch_id, data_version, quality_status, permission_status"
    )
    stage_columns = (
        "stage.stock_code, stage.short_name, stage.trade_time, stage.trade_date, "
        "stage.k_type, stage.adjust_type, stage.`open`, stage.`close`, "
        "stage.high, stage.low, stage.volume, stage.amount, stage.`change`, "
        "stage.change_pct, stage.pre_close, stage.etl_sync_at, "
        "stage.qmt_code, stage.data_source, stage.source_time, "
        "stage.received_at, stage.batch_id, stage.data_version, "
        "stage.quality_status, stage.permission_status"
    )
    payload: list[tuple] = []
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
        payload.append((
            code,
            short_names.get(code, ""),
            source_time,
            session,
            1,
            0,
            open_price,
            close_price,
            high,
            low,
            volume,
            amount,
            change,
            change_pct,
            pre_close,
            now,
            f"{code}.{market}",
            SOURCE,
            source_time,
            now,
            batch_id,
            hashlib.sha256(
                (
                    f"{code}|{session}|{open_price}|{close_price}|"
                    f"{high}|{low}|{volume}|{amount}|{pre_close}"
                ).encode("utf-8")
            ).hexdigest()[:32],
            "VERIFIED",
            "SUPPORTED",
        ))
    row_template = "(" + ",".join(["%s"] * 24) + ")"
    temp_table = f"tmp_qmt_daily_{run_id}"
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"""
            CREATE TEMPORARY TABLE `{temp_table}` (
              stock_code VARCHAR(16) NOT NULL,
              short_name VARCHAR(64) NULL,
              trade_time DATETIME NOT NULL,
              trade_date DATE NOT NULL,
              k_type INT NOT NULL,
              adjust_type INT NOT NULL,
              `open` DECIMAL(50,6) NULL,
              `close` DECIMAL(50,6) NULL,
              high DECIMAL(50,6) NULL,
              low DECIMAL(50,6) NULL,
              volume DECIMAL(50,6) NULL,
              amount DECIMAL(50,6) NULL,
              `change` DECIMAL(50,6) NULL,
              change_pct DECIMAL(50,6) NULL,
              pre_close DECIMAL(50,6) NULL,
              etl_sync_at DATETIME NOT NULL,
              qmt_code VARCHAR(32) NULL,
              data_source VARCHAR(32) NOT NULL,
              source_time DATETIME NULL,
              received_at DATETIME NOT NULL,
              batch_id VARCHAR(64) NOT NULL,
              data_version VARCHAR(64) NOT NULL,
              quality_status VARCHAR(32) NOT NULL,
              permission_status VARCHAR(32) NOT NULL
            ) ENGINE=InnoDB
            """
        )
        insert_stage = (
            f"INSERT INTO `{temp_table}` ({columns}) VALUES "
        )
        for offset in range(0, len(payload), 500):
            chunk = payload[offset:offset + 500]
            statement = insert_stage + ",".join(
                [row_template] * len(chunk)
            )
            connection.exec_driver_sql(
                statement,
                tuple(value for row in chunk for value in row),
            )
        connection.execute(
            text(
            f"""
            UPDATE `{TARGET_TABLE}` AS target
            JOIN `{temp_table}` AS stage
              ON stage.stock_code=target.stock_code
             AND stage.trade_date=target.trade_date
             AND stage.k_type=target.k_type
             AND stage.adjust_type=target.adjust_type
            SET target.short_name=stage.short_name,
                target.trade_time=stage.trade_time,
                target.`open`=stage.`open`,
                target.`close`=stage.`close`,
                target.high=stage.high,
                target.low=stage.low,
                target.volume=stage.volume,
                target.amount=stage.amount,
                target.`change`=stage.`change`,
                target.change_pct=stage.change_pct,
                target.pre_close=stage.pre_close,
                target.etl_sync_at=stage.etl_sync_at,
                target.qmt_code=stage.qmt_code,
                target.data_source=stage.data_source,
                target.source_time=stage.source_time,
                target.received_at=stage.received_at,
                target.batch_id=stage.batch_id,
                target.data_version=stage.data_version,
                target.quality_status=stage.quality_status,
                target.permission_status=stage.permission_status
            WHERE target.trade_date=:d
              AND target.k_type=1
              AND target.adjust_type=0
            """
            ),
            {"d": session},
        )
        connection.exec_driver_sql(
            f"""
            INSERT INTO `{TARGET_TABLE}` ({columns})
            SELECT {stage_columns}
            FROM `{temp_table}` AS stage
            LEFT JOIN `{TARGET_TABLE}` AS target
              ON target.stock_code=stage.stock_code
             AND target.trade_date=stage.trade_date
             AND target.k_type=stage.k_type
             AND target.adjust_type=stage.adjust_type
            WHERE target.id IS NULL
            """
        )
        connection.execute(
            text(
            f"""
            DELETE target
            FROM `{TARGET_TABLE}` AS target
            LEFT JOIN `{temp_table}` AS stage
              ON stage.stock_code=target.stock_code
             AND stage.trade_date=target.trade_date
             AND stage.k_type=target.k_type
             AND stage.adjust_type=target.adjust_type
            WHERE target.trade_date=:d
              AND target.k_type=1
              AND target.adjust_type=0
              AND stage.stock_code IS NULL
            """
            ),
            {"d": session},
        )
        fingerprint = persisted_fingerprint(
            connection,
            dataset=DATASET,
            trade_date=session.isoformat(),
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
                "detail": json.dumps(
                    {
                        "readback_rows": fingerprint["row_count"],
                        "readback_codes": fingerprint["code_count"],
                        "row_hash": fingerprint["row_hash"],
                        "eligible_code_count": int(eligible_code_count),
                        "coverage": (
                            len(covered_codes) / int(eligible_code_count)
                            if int(eligible_code_count) else 0.0
                        ),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
        )
    return {
        "row_count": len(rows),
        "code_count": len(covered_codes),
        "readback_rows": int(fingerprint["row_count"]),
        "readback_codes": int(fingerprint["code_count"]),
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
        run_id = new_run_id()
        published = _publish(
            engine,
            session=session,
            rows=harvested["rows"],
            covered_codes=harvested["codes"],
            short_names=short_names,
            run_id=run_id,
            source_root=root,
            content_sha256=harvested["digest"],
            eligible_code_count=eligible,
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
    if not args.apply:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0 if not failures else 1
    task_result = _task_result(engine, sessions=sessions)
    print(json.dumps(task_result, ensure_ascii=False, separators=(",", ":")))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
