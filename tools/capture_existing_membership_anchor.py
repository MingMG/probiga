#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Capture the existing current membership tables as an honest legacy anchor.

This does not pretend to reconstruct historical membership. The snapshot is
labelled OBSERVED_CURRENT_ONLY and uses the latest common ETL date of the
current concept and industry tables.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.membership_snapshot import (
    ensure_membership_snapshot_tables,
    publish_membership_snapshot,
)
from server.common.sql_reader import read_sql_rows
from tools.env_config import create_tool_engine, load_project_env


def _frame(engine, sql: str, context: str) -> pd.DataFrame:
    return pd.DataFrame(read_sql_rows(engine, sql, context=context))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default="")
    args = parser.parse_args()
    load_project_env()
    engine = create_tool_engine(pool_pre_ping=True)
    try:
        frames = {
            "si_all_code": _frame(
                engine,
                "SELECT stock_code, short_name FROM si_all_code",
                "membership_anchor_stocks",
            ),
            "si_concept_code_east": _frame(
                engine,
                "SELECT concept_code, name FROM si_concept_code_east",
                "membership_anchor_concepts",
            ),
            "si_concept_constituent_east": _frame(
                engine,
                """
                SELECT concept_code, stock_code, short_name
                FROM si_concept_constituent_east
                """,
                "membership_anchor_concept_members",
            ),
            "si_industry_sw": _frame(
                engine,
                """
                SELECT stock_code, sw_code, industry_name, industry_type
                FROM si_industry_sw
                """,
                "membership_anchor_industry_members",
            ),
        }
        if frames["si_concept_constituent_east"].empty:
            frames["si_concept_constituent_east"] = _frame(
                engine,
                """
                SELECT s.concept_code, s.stock_code,
                       COALESCE(c.short_name, '') AS short_name
                FROM si_stock_concept_east s
                LEFT JOIN si_all_code c ON c.stock_code = s.stock_code
                WHERE s.concept_code IS NOT NULL AND s.concept_code <> ''
                """,
                "membership_anchor_concept_members_reverse",
            )
        if any(frame.empty for frame in frames.values()):
            raise RuntimeError("current membership tables are incomplete; legacy anchor refused")
        if args.snapshot_date:
            snapshot_date = date.fromisoformat(args.snapshot_date[:10])
        else:
            rows = read_sql_rows(
                engine,
                """
                SELECT LEAST(
                    COALESCE(
                        (SELECT MAX(DATE(etl_sync_at)) FROM si_concept_constituent_east),
                        (SELECT MAX(DATE(etl_sync_at)) FROM si_stock_concept_east)
                    ),
                    (SELECT MAX(DATE(etl_sync_at)) FROM si_industry_sw)
                ) AS snapshot_date
                """,
                context="membership_anchor_date",
            )
            value = (rows[0] if rows else {}).get("snapshot_date")
            if value is None:
                raise RuntimeError("legacy membership ETL date is unavailable")
            snapshot_date = pd.Timestamp(value).date()
        ensure_membership_snapshot_tables(engine)
        with engine.begin() as connection:
            result = publish_membership_snapshot(
                connection,
                frames,
                snapshot_date=snapshot_date,
                source="legacy_current_anchor",
                quality_status="OBSERVED_CURRENT_ONLY",
                capture_mode="existing_current_tables_anchor",
            )
        print(json.dumps(result, ensure_ascii=False, default=str))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
