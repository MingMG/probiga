#!/usr/bin/env python3
from __future__ import annotations

"""Validate and publish standard-QMT reference data to existing si_* tables."""

import argparse
import json
import sys
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.reference import (
    PROVIDER_ID,
    fetch_all_index_codes,
    fetch_all_stock_codes,
    fetch_index_constituents,
    fetch_sector_datasets,
)
from integrations.bigqmt.membership_snapshot import (
    ensure_membership_snapshot_tables,
    publish_membership_snapshot,
)
from server.common.sql_reader import read_sql_rows
from server.common.batch_db import write_frame
from tools.env_config import create_tool_engine, load_project_env


TARGET_COLUMNS = {
    "si_all_code": ["stock_code", "short_name", "exchange", "list_date", "etl_sync_at"],
    "si_all_index_code": ["index_code", "concept_code", "name", "source", "etl_sync_at"],
    "si_index_constituent": ["index_code", "stock_code", "short_name", "etl_sync_at"],
    "si_concept_code_east": ["concept_code", "index_code", "name", "source", "etl_sync_at"],
    "si_concept_constituent_east": ["concept_code", "stock_code", "short_name", "etl_sync_at"],
    "si_industry_sw": ["stock_code", "sw_code", "industry_name", "industry_type", "source", "etl_sync_at"],
    "si_stock_concept_east": ["stock_code", "concept_code", "name", "source", "reason", "etl_sync_at"],
    "si_stock_plate_east": ["stock_code", "plate_code", "plate_name", "plate_type", "source", "etl_sync_at"],
}


def _clean(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.reindex(columns=columns).copy()
    return out.astype(object).where(pd.notna(out), None)


def fetch_and_validate(
    engine,
    *,
    force_reference_refresh: bool = False,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    print("[1/4] standard QMT stock universe", flush=True)
    stocks = fetch_all_stock_codes()
    print(f"stock rows={len(stocks)}", flush=True)

    print("[2/4] standard QMT index universe", flush=True)
    indexes = fetch_all_index_codes(engine=engine)
    print(f"index rows={len(indexes)}", flush=True)

    print("[3/4] standard QMT index constituents", flush=True)
    index_members = fetch_index_constituents(indexes["index_code"].tolist())
    print(
        f"index member rows={len(index_members)} indexes={index_members['index_code'].nunique() if not index_members.empty else 0}",
        flush=True,
    )

    print("[4/4] standard QMT concepts and industries", flush=True)
    sectors = fetch_sector_datasets(force_refresh=force_reference_refresh)
    for name, frame in sectors.items():
        print(f"{name} rows={len(frame)}", flush=True)

    stock_codes = set(stocks["stock_code"].astype(str).str.zfill(6))
    index_codes = set(indexes["index_code"].astype(str).str.zfill(6))
    name_map = stocks.set_index("stock_code")["short_name"].fillna("").astype(str).to_dict()
    index_members = index_members.copy()
    index_members["index_code"] = index_members["index_code"].astype(str).str.zfill(6)
    index_members["stock_code"] = index_members["stock_code"].astype(str).str.zfill(6)
    index_members = index_members[
        index_members["index_code"].isin(index_codes) & index_members["stock_code"].isin(stock_codes)
    ].drop_duplicates(subset=["index_code", "stock_code"], keep="last")
    index_members["short_name"] = index_members["stock_code"].map(name_map).fillna("")

    catalog = sectors["concept_catalog"].drop_duplicates(subset=["concept_code"], keep="last").copy()
    concept_codes = set(catalog["concept_code"].astype(str))
    concept_members = sectors["concept_constituents"].copy()
    concept_members["stock_code"] = concept_members["stock_code"].astype(str).str.zfill(6)
    concept_members = concept_members[
        concept_members["concept_code"].astype(str).isin(concept_codes)
        & concept_members["stock_code"].isin(stock_codes)
    ].drop_duplicates(subset=["concept_code", "stock_code"], keep="last")
    concept_members["short_name"] = concept_members["stock_code"].map(name_map).fillna("")

    industry = sectors["industry_sw"].copy()
    concepts = sectors["stock_concepts"].copy()
    plates = sectors["stock_plates"].copy()
    for frame in (industry, concepts, plates):
        frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
        frame.drop(frame.index[~frame["stock_code"].isin(stock_codes)], inplace=True)

    counts = {
        "stocks": int(len(stocks)),
        "indexes": int(len(indexes)),
        "index_members": int(len(index_members)),
        "index_member_indexes": int(index_members["index_code"].nunique()),
        "concepts": int(len(catalog)),
        "concept_members": int(len(concept_members)),
        "industry_relations": int(len(industry)),
        "industry_stocks": int(industry["stock_code"].nunique()),
        "stock_concepts": int(len(concepts)),
        "concept_stocks": int(concepts["stock_code"].nunique()),
        "stock_plates": int(len(plates)),
        "plate_stocks": int(plates["stock_code"].nunique()),
        "source": PROVIDER_ID,
    }
    minimums = {
        "stocks": 5000,
        "indexes": 500,
        "index_members": 5000,
        "index_member_indexes": 100,
        "concepts": 500,
        "concept_members": 30000,
        "industry_relations": 5000,
        "industry_stocks": 4500,
        "stock_concepts": 30000,
        "concept_stocks": 3000,
        "stock_plates": 40000,
        "plate_stocks": 4500,
    }
    failures = [f"{key}={counts[key]} < {minimum}" for key, minimum in minimums.items() if counts[key] < minimum]
    if failures:
        raise RuntimeError("standard QMT reference validation failed: " + "; ".join(failures))

    now = datetime.now().replace(microsecond=0)
    frames = {
        "si_all_code": stocks,
        "si_all_index_code": indexes,
        "si_index_constituent": index_members,
        "si_concept_code_east": catalog,
        "si_concept_constituent_east": concept_members,
        "si_industry_sw": industry,
        "si_stock_concept_east": concepts,
        "si_stock_plate_east": plates,
    }
    for table_name, frame in frames.items():
        frame["etl_sync_at"] = now
        frames[table_name] = _clean(frame, TARGET_COLUMNS[table_name])
    return frames, counts


def resolve_snapshot_date(engine, requested: str = "") -> date:
    if requested:
        return date.fromisoformat(str(requested)[:10])
    now = datetime.now()
    upper = now.date() if now.time() >= time(15, 10) else date.fromordinal(now.date().toordinal() - 1)
    rows = read_sql_rows(
        engine,
        """
        SELECT MAX(trade_date) AS snapshot_date
        FROM si_trade_calendar
        WHERE trade_status = 1 AND trade_date <= :upper
        """,
        {"upper": upper},
        context="qmt_membership_snapshot_date",
    )
    value = (rows[0] if rows else {}).get("snapshot_date")
    if value is None:
        raise RuntimeError("cannot resolve completed trading date for QMT membership snapshot")
    return pd.Timestamp(value).date()


def publish(
    engine,
    frames: dict[str, pd.DataFrame],
    *,
    snapshot_date: date,
) -> dict[str, object]:
    delete_order = [
        "si_index_constituent",
        "si_concept_constituent_east",
        "si_industry_sw",
        "si_stock_concept_east",
        "si_stock_plate_east",
        "si_concept_code_east",
        "si_all_index_code",
        "si_all_code",
    ]
    insert_order = [
        "si_all_code",
        "si_all_index_code",
        "si_concept_code_east",
        "si_index_constituent",
        "si_concept_constituent_east",
        "si_industry_sw",
        "si_stock_concept_east",
        "si_stock_plate_east",
    ]
    ensure_membership_snapshot_tables(engine)
    with engine.begin() as conn:
        for table_name in delete_order:
            conn.execute(text(f"DELETE FROM `{table_name}`"))
        for table_name in insert_order:
            frame = frames[table_name]
            written = write_frame(
                frame,
                table_name,
                conn,
                if_exists="append",
                index=False,
                chunksize=2000 if len(frame) > 10000 else 1000,
                method="multi",
            )
            if int(written) != len(frame):
                raise RuntimeError(f"{table_name} write mismatch: {written}/{len(frame)}")
            print(f"published {table_name}: {written}", flush=True)
        snapshot = publish_membership_snapshot(
            conn,
            frames,
            snapshot_date=snapshot_date,
        )
        print(
            f"published membership snapshot {snapshot_date}: "
            f"concept={snapshot['concept_relations']} "
            f"industry={snapshot['industry_relations']}",
            flush=True,
        )
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="publish after all validation passes")
    parser.add_argument("--snapshot-date", default="")
    parser.add_argument(
        "--force-reference-refresh",
        action="store_true",
        help="force a fresh QMT sector pull instead of using the bridge cache",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--promote-production",
        action="store_true",
        help=(
            "after an immutable QMT snapshot is published, copy that exact "
            "hash-verified snapshot to the production database"
        ),
    )
    args = parser.parse_args()
    load_project_env()
    engine = create_tool_engine(pool_pre_ping=True)
    frames, counts = fetch_and_validate(
        engine,
        force_reference_refresh=args.force_reference_refresh,
    )
    result: dict[str, object] = {"status": "validated", "counts": counts, "applied": False}
    if args.apply:
        snapshot_date = resolve_snapshot_date(engine, args.snapshot_date)
        snapshot = publish(engine, frames, snapshot_date=snapshot_date)
        result.update({
            "status": "success",
            "applied": True,
            "membership_snapshot": snapshot,
        })
        if args.promote_production:
            from tools.promote_qmt_membership_to_production import (
                promote_to_production,
            )

            result["production_promotion"] = promote_to_production(
                snapshot_date=snapshot_date,
            )
    print(json.dumps(result, ensure_ascii=False, default=str) if args.json else result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
