#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Restore the canonical current concept-member table from BigQMT reverse rows."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env


def repair(engine, *, apply: bool) -> dict[str, object]:
    with engine.begin() as connection:
        target_count = int(
            connection.execute(
                text("SELECT COUNT(*) FROM si_concept_constituent_east")
            ).scalar()
            or 0
        )
        source = connection.execute(
            text(
                """
                SELECT COUNT(*) AS relation_count,
                       COUNT(DISTINCT concept_code) AS concept_count,
                       COUNT(DISTINCT stock_code) AS stock_count,
                       MIN(etl_sync_at) AS source_time,
                       MAX(etl_sync_at) AS source_time_max
                FROM si_stock_concept_east
                WHERE source='gj_big_qmt_inner'
                  AND concept_code IS NOT NULL AND concept_code <> ''
                  AND stock_code REGEXP '^(0|3|4|6|8|9)'
                """
            )
        ).mappings().one()
        source_count = int(source["relation_count"] or 0)
        if (
            source_count < 30000
            or int(source["concept_count"] or 0) < 500
            or int(source["stock_count"] or 0) < 3000
        ):
            raise RuntimeError(f"BigQMT reverse concept rows failed quality gate: {dict(source)}")
        if target_count:
            return {
                "status": "not_needed",
                "target_rows": target_count,
                "source_rows": source_count,
                "source": "gj_big_qmt_inner",
            }
        if not apply:
            return {
                "status": "ready",
                "target_rows": 0,
                "source_rows": source_count,
                "source": "gj_big_qmt_inner",
            }
        inserted = int(
            connection.execute(
                text(
                    """
                    INSERT INTO si_concept_constituent_east
                    (concept_code, stock_code, short_name, etl_sync_at)
                    SELECT s.concept_code, s.stock_code,
                           COALESCE(c.short_name, ''),
                           s.etl_sync_at
                    FROM si_stock_concept_east s
                    LEFT JOIN si_all_code c ON c.stock_code=s.stock_code
                    WHERE s.source='gj_big_qmt_inner'
                      AND s.concept_code IS NOT NULL AND s.concept_code <> ''
                      AND s.stock_code REGEXP '^(0|3|4|6|8|9)'
                    GROUP BY s.concept_code, s.stock_code,
                             c.short_name, s.etl_sync_at
                    """
                )
            ).rowcount
            or 0
        )
        after = int(
            connection.execute(
                text("SELECT COUNT(*) FROM si_concept_constituent_east")
            ).scalar()
            or 0
        )
        if after != source_count or inserted != source_count:
            raise RuntimeError(
                f"canonical concept restore mismatch: inserted={inserted} "
                f"after={after} source={source_count}"
            )
        return {
            "status": "repaired",
            "target_rows_before": 0,
            "inserted_rows": inserted,
            "target_rows_after": after,
            "concept_count": int(source["concept_count"] or 0),
            "stock_count": int(source["stock_count"] or 0),
            "source": "gj_big_qmt_inner",
            "source_time": str(source["source_time"] or ""),
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_project_env()
    engine = create_tool_engine(pool_pre_ping=True)
    try:
        print(json.dumps(repair(engine, apply=args.apply), ensure_ascii=False))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
