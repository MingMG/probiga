#!/usr/bin/env python3
"""Publish index daily or minute bars from the QMT local datadir cache."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.datadir import (  # noqa: E402
    CST,
    DAILY_PERIOD,
    MINUTE_PERIOD,
    iter_index_daily_rows,
    iter_index_minute_rows,
    load_dat,
    session_slice,
)
from server.common.authoritative_market_clock import (  # noqa: E402
    authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine, replace_table_rows  # noqa: E402
from server.common.kline_data import get_kline_engine  # noqa: E402
from server.common.qmt_datadir_result import (  # noqa: E402
    build_result,
    build_unavailable_result,
    load_receipt,
    partition_from_receipt,
    persisted_fingerprint,
)
from server.common.qmt_index_minute_grid import index_minute_grids  # noqa: E402
from server.common.qmt_trade_calendar import (  # noqa: E402
    load_trade_calendar_receipt,
)
from integrations.qmt.info import to_qmt_index_symbol  # noqa: E402
from tools.env_config import load_project_env  # noqa: E402
from tools.sync_qmt_index_edge import (  # noqa: E402
    MIN_FORMAL_INDEX_COUNT,
    IndexCatalogMember,
    expected_codes_by_session,
)


DATASETS = {
    "kline": {
        "dataset": "index_daily",
        "period": DAILY_PERIOD,
        "table": "sm_index_kline",
    },
    "minute": {
        "dataset": "index_minute",
        "period": MINUTE_PERIOD,
        "table": "sm_index_minute",
    },
}
DEFAULT_ROOT = Path(r"D:\国金证券QMT交易端\datadir")
MIN_COVERAGE = 0.95
SOURCE = "qmt_datadir"
QMT_PROVIDERS = {"qmt", "gj_qmt", "gj_big_qmt_inner"}


def _build_sha() -> str:
    return str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()


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
        for day in receipt.sessions_between(start.isoformat(), end.isoformat())
    ]


def _load_catalog(engine) -> list[IndexCatalogMember]:
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT index_code,name,source FROM si_all_index_code "
                    "ORDER BY index_code"
                )
            ).mappings()
        ]
        detail_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT qmt_code,stock_code,short_name,list_date,expire_date,"
                    "batch_id,data_source,permission_status "
                    "FROM qmt_instrument_detail"
                )
            ).mappings()
        ]
    if len(rows) < MIN_FORMAL_INDEX_COUNT:
        raise RuntimeError("QMT formal index catalog is unexpectedly small")
    details = {
        str(row.get("qmt_code") or "").upper(): row
        for row in detail_rows
    }
    members: list[IndexCatalogMember] = []
    seen: set[str] = set()
    for row in rows:
        code = str(row.get("index_code") or "").strip().zfill(6)
        if code.startswith("395"):
            continue
        symbol = to_qmt_index_symbol(code)
        if (
            len(code) != 6
            or not code.isdigit()
            or code in seen
            or symbol is None
            or str(row.get("source") or "").strip().lower()
            not in QMT_PROVIDERS
        ):
            raise RuntimeError("QMT formal index catalog identity is invalid")
        detail = details.get(symbol)
        if detail is not None:
            if (
                str(detail.get("stock_code") or "").zfill(6) != code
                or str(detail.get("data_source") or "").strip().lower()
                not in QMT_PROVIDERS
                or str(detail.get("permission_status") or "") != "SUPPORTED"
            ):
                raise RuntimeError("QMT index instrument identity is invalid")
        seen.add(code)
        list_date = (
            str(detail.get("list_date") or "")[:10]
            if detail is not None and detail.get("list_date") not in (None, "")
            else None
        )
        expire_date = (
            str(detail.get("expire_date") or "")[:10]
            if detail is not None and detail.get("expire_date") not in (None, "")
            else None
        )
        members.append(IndexCatalogMember(
            index_code=code,
            qmt_code=symbol,
            name=(
                str(detail.get("short_name") or "").strip()
                if detail is not None
                else str(row.get("name") or "").strip()
            ),
            list_date=list_date,
            expire_date=expire_date,
            batch_id=(
                str(detail.get("batch_id") or "")
                if detail is not None else ""
            ),
        ))
    if len(members) < MIN_FORMAL_INDEX_COUNT:
        raise RuntimeError("QMT price-index catalog is unexpectedly small")
    return members


def _harvest(
    root: Path,
    *,
    dataset: str,
    catalog,
    sessions: list[dt.date],
) -> dict:
    expected = expected_codes_by_session(
        catalog,
        [session.isoformat() for session in sessions],
    )
    by_code = {member.index_code: member for member in catalog}
    rows: list[dict] = []
    covered: dict[str, set[str]] = {
        session.isoformat(): set() for session in sessions
    }
    bad_grids: list[dict] = []
    digest = hashlib.sha256()
    for session in sessions:
        day = session.isoformat()
        for code in expected[day]:
            member = by_code[code]
            _, market = member.qmt_code.split(".", 1)
            period = DATASETS[dataset]["period"]
            dat = load_dat(root, market, period, code)
            if dat is None:
                continue
            selected = session_slice(dat.records, session)
            if not len(selected):
                continue
            digest.update(member.qmt_code.encode("ascii"))
            digest.update(selected.tobytes())
            if dataset == "kline":
                session_rows = [
                    row for _, row in iter_index_daily_rows(dat, [session])
                ]
                if len(session_rows) != 1:
                    bad_grids.append({
                        "trade_date": day,
                        "index_code": code,
                        "required_rows": 1,
                        "observed_rows": len(session_rows),
                    })
                    continue
                (
                    _,
                    trade_time,
                    trade_date,
                    open_price,
                    close,
                    high,
                    low,
                    volume,
                    amount,
                    _pre_close,
                    change,
                    change_pct,
                    qmt_code,
                ) = session_rows[0]
                rows.append({
                    "index_code": code,
                    "trade_time": trade_time,
                    "trade_date": trade_date,
                    "k_type": 1,
                    "open": open_price,
                    "close": close,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "amount": amount,
                    "change": change,
                    "change_pct": change_pct,
                    "etl_sync_at": dt.datetime.now(CST).replace(tzinfo=None),
                    "_qmt_code": qmt_code,
                })
            else:
                required, _allowed = index_minute_grids(member.qmt_code)
                session_rows = [
                    row
                    for _, row in iter_index_minute_rows(
                        dat,
                        selected,
                        frozenset(required),
                    )
                ]
                if len(session_rows) != len(required):
                    bad_grids.append({
                        "trade_date": day,
                        "index_code": code,
                        "required_rows": len(required),
                        "observed_rows": len(session_rows),
                    })
                    continue
                for row in session_rows:
                    (
                        _,
                        trade_time,
                        trade_date,
                        price,
                        volume,
                        amount,
                        change,
                        change_pct,
                        qmt_code,
                    ) = row
                    rows.append({
                        "index_code": code,
                        "trade_time": trade_time,
                        "trade_date": trade_date,
                        "price": price,
                        "avg_price": None,
                        "change": change,
                        "change_pct": change_pct,
                        "volume": volume,
                        "amount": amount,
                        "snapshot_at": trade_time,
                        "etl_sync_at": dt.datetime.now(CST).replace(tzinfo=None),
                        "_qmt_code": qmt_code,
                    })
            covered[day].add(code)
    eligible = {
        day: len(codes)
        for day, codes in expected.items()
    }
    return {
        "rows": rows,
        "covered": covered,
        "eligible": eligible,
        "bad_grids": bad_grids,
        "digest": digest.hexdigest(),
    }


def _write_receipt(
    connection,
    *,
    dataset: str,
    session: dt.date,
    root: Path,
    run_id: str,
    content_sha256: str,
    eligible_code_count: int,
    code_count: int,
    row_count: int,
    row_hash: str,
) -> None:
    detail = {
        "readback_rows": row_count,
        "readback_codes": code_count,
        "row_hash": row_hash,
        "eligible_code_count": eligible_code_count,
        "coverage": code_count / eligible_code_count if eligible_code_count else 0.0,
    }
    connection.execute(
        text(
            """
            INSERT INTO qmt_datadir_import_partition
              (dataset,trade_date,source_root,status,code_count,row_count,
               content_sha256,run_id,started_at,finished_at,detail_json)
            VALUES
              (:dataset,:trade_date,:source_root,'COMPLETE',:code_count,:row_count,
               :content_sha256,:run_id,NOW(),NOW(),:detail_json)
            ON DUPLICATE KEY UPDATE
              source_root=VALUES(source_root),status=VALUES(status),
              code_count=VALUES(code_count),row_count=VALUES(row_count),
              content_sha256=VALUES(content_sha256),run_id=VALUES(run_id),
              finished_at=VALUES(finished_at),detail_json=VALUES(detail_json)
            """
        ),
        {
            "dataset": dataset,
            "trade_date": session,
            "source_root": str(root),
            "code_count": code_count,
            "row_count": row_count,
            "content_sha256": content_sha256,
            "run_id": run_id,
            "detail_json": json.dumps(
                detail,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        },
    )


def _task_result(engine, *, dataset: str, sessions: list[dt.date]) -> dict:
    partitions = []
    for session in sessions:
        receipt = load_receipt(
            engine,
            dataset=dataset,
            trade_date=session.isoformat(),
        )
        if receipt is not None:
            partitions.append(partition_from_receipt(receipt))
    return build_result(
        dataset=dataset,
        sessions=[session.isoformat() for session in sessions],
        partitions=partitions,
        build_sha=_build_sha(),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(DATASETS), required=True)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--latest-session", action="store_true")
    date_group.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--datadir-root", default=str(DEFAULT_ROOT))
    parser.add_argument("--min-coverage", type=float, default=MIN_COVERAGE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_batch_engine()
    history = get_kline_engine()
    dataset = DATASETS[args.dataset]["dataset"]
    if args.latest_session:
        latest = authoritative_closed_trade_date(engine)
        start = end = dt.date.fromisoformat(latest)
    else:
        if not args.end_date:
            parser.error("--end-date is required with --start-date")
        start = dt.date.fromisoformat(args.start_date)
        end = dt.date.fromisoformat(args.end_date)
    sessions = _sessions(engine, start, end)
    if not sessions:
        parser.error("no QMT calendar session in the requested range")
    root = Path(args.datadir_root)
    if not root.is_dir():
        payload = build_unavailable_result(
            dataset=dataset,
            sessions=[session.isoformat() for session in sessions],
            reason="qmt_datadir_root_missing",
            build_sha=_build_sha(),
        )
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 2

    catalog = _load_catalog(engine)
    harvested = _harvest(
        root,
        dataset=args.dataset,
        catalog=catalog,
        sessions=sessions,
    )
    for session in sessions:
        day = session.isoformat()
        required = math.ceil(harvested["eligible"][day] * args.min_coverage)
        if (
            harvested["bad_grids"]
            or len(harvested["covered"][day]) < required
        ):
            reason = (
                "incomplete_index_minute_grid"
                if any(item["trade_date"] == day for item in harvested["bad_grids"])
                else "insufficient_index_code_coverage"
            )
            payload = build_unavailable_result(
                dataset=dataset,
                sessions=[session.isoformat()],
                reason=reason,
                build_sha=_build_sha(),
            )
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            return 2

    if not args.apply:
        print(json.dumps({
            "status": "READY",
            "dataset": dataset,
            "sessions": [session.isoformat() for session in sessions],
            "rows": len(harvested["rows"]),
        }, ensure_ascii=False, separators=(",", ":")))
        return 0

    frame = pd.DataFrame(harvested["rows"]).drop(columns=["_qmt_code"])
    table = DATASETS[args.dataset]["table"]
    if args.dataset == "kline":
        where = "trade_date BETWEEN :start_date AND :end_date AND k_type=1"
    else:
        where = "trade_date BETWEEN :start_date AND :end_date"
    replace_table_rows(
        frame,
        table,
        history,
        where_sql=where,
        params={
            "start_date": min(sessions),
            "end_date": max(sessions),
        },
        chunksize=2000,
        method="multi",
    )
    run_id = f"qid_{dt.datetime.now(CST):%Y%m%d%H%M%S}"
    for session in sessions:
        day = session.isoformat()
        fingerprint = persisted_fingerprint(
            history,
            dataset=dataset,
            trade_date=day,
        )
        with engine.begin() as connection:
            _write_receipt(
                connection,
                dataset=dataset,
                session=session,
                root=root,
                run_id=run_id,
                content_sha256=harvested["digest"],
                eligible_code_count=harvested["eligible"][day],
                code_count=fingerprint["code_count"],
                row_count=fingerprint["row_count"],
                row_hash=fingerprint["row_hash"],
            )
    payload = _task_result(engine, dataset=dataset, sessions=sessions)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
