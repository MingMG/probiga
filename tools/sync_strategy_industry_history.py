#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Append today's industry facts to the strategy-governance history ledger.

The mutable ``si_industry_sw`` table is only a source for a prospective daily
capture.  This tool deliberately refuses historical backfills: a current
overwrite can never be presented as proof of what the industry mapping was on
an earlier trade date.  Governance reads only the append-only history table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def build_history_rows(
    source_rows: Iterable[Mapping[str, Any]], *, trade_date: str,
) -> tuple[str, list[dict[str, Any]]]:
    target = date.fromisoformat(str(trade_date)).isoformat()
    if target != date.today().isoformat():
        raise ValueError("行业历史只允许当日前瞻冻结，禁止用当前覆盖表回填历史")
    cutoff = (date.fromisoformat(target) + timedelta(days=1)).isoformat() + "T00:00:00"
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in source_rows:
        code = str(raw.get("stock_code") or "").strip().zfill(6)
        name = str(raw.get("industry_name") or "").strip()
        industry_type = str(raw.get("industry_type") or "").strip()
        source_id = str(raw.get("id") or "").strip()
        source_system = str(raw.get("source") or "si_industry_sw").strip()
        source_time = str(raw.get("etl_sync_at") or "").strip().replace(" ", "T")
        if (
            not re.fullmatch(r"[0-9]{6}", code)
            or code in seen or not name or not industry_type
            or not source_id or not source_system or not source_time
        ):
            raise ValueError("行业源存在重复或不完整的证券事实")
        try:
            parsed = datetime.fromisoformat(source_time.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("行业源同步时间无效") from exc
        normalized_time = parsed.replace(tzinfo=None).isoformat(timespec="seconds")
        if normalized_time >= cutoff:
            raise ValueError("行业源事实晚于当日冻结cutoff")
        seen.add(code)
        normalized.append({
            "stock_code": code,
            "industry_name": name,
            "industry_type": industry_type,
            "source_system": source_system,
            "source_fact_id": f"{target}:{source_id}",
            "source_effective_at": normalized_time,
            "source_etl_sync_at": normalized_time,
        })
    normalized.sort(key=lambda row: row["stock_code"])
    if not normalized:
        raise ValueError("行业源没有可冻结事实")
    snapshot_id = _digest({
        "schema": "probiga.strategy-industry-history-capture.v1",
        "trade_date": target,
        "as_of_exclusive": cutoff,
        "facts": normalized,
    })
    rows = []
    for item in normalized:
        payload = {
            "snapshot_id": snapshot_id,
            "trade_date": target,
            "as_of_exclusive": cutoff,
            **item,
        }
        rows.append({**payload, "row_hash": _digest(payload)})
    return snapshot_id, rows


def capture_industry_history(engine, *, trade_date: str) -> dict[str, Any]:
    """Capture one exact, append-only daily snapshot without printing output."""

    target = date.fromisoformat(str(trade_date)).isoformat()
    with engine.connect() as connection:
        source_rows = [dict(row) for row in connection.execute(text("""
            SELECT id, stock_code, industry_name, industry_type, source,
                   etl_sync_at
            FROM si_industry_sw
            WHERE industry_type IN ('L1','一级行业','申万一级','SW2021')
            ORDER BY stock_code, etl_sync_at DESC, id DESC
        """)).mappings().all()]
        source_row_count = int(connection.execute(text("""
            SELECT COUNT(*)
            FROM si_industry_sw
            WHERE industry_type IN ('L1','一级行业','申万一级','SW2021')
        """)).scalar() or 0)
    if source_row_count != len(source_rows):
        raise RuntimeError(
            "行业源读取不完整："
            f"源行数{source_row_count}，已加载{len(source_rows)}"
        )
    newest: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        newest.setdefault(str(row.get("stock_code") or "").zfill(6), row)
    snapshot_id, rows = build_history_rows(newest.values(), trade_date=target)
    report: dict[str, Any] = {
        "status": "COMPLETED",
        "trade_date": target,
        "snapshot_id": snapshot_id,
        "source_row_count": source_row_count,
        "selected_row_count": len(rows),
        "row_count": len(rows),
        "historical_backfill_allowed": False,
    }
    with engine.begin() as connection:
        existing = connection.execute(text(
            "SELECT DISTINCT snapshot_id FROM st_strategy_industry_history "
            "WHERE trade_date=:trade_date FOR UPDATE"
        ), {"trade_date": target}).scalars().all()
        if existing:
            if existing != [snapshot_id]:
                raise RuntimeError("当日行业历史已存在不同snapshot_id，拒绝覆盖")
            count = connection.execute(text(
                "SELECT COUNT(*) FROM st_strategy_industry_history "
                "WHERE trade_date=:trade_date AND snapshot_id=:snapshot_id"
            ), {
                "trade_date": target,
                "snapshot_id": snapshot_id,
            }).scalar()
            if int(count or 0) != len(rows):
                raise RuntimeError("当日行业历史已存在但行数不完整")
            report["idempotent_replay"] = True
        else:
            for row in rows:
                connection.execute(text("""
                    INSERT INTO st_strategy_industry_history
                    (snapshot_id, trade_date, as_of_exclusive, stock_code,
                     industry_name, industry_type, source_system,
                     source_fact_id, source_effective_at,
                     source_etl_sync_at, row_hash)
                    VALUES
                    (:snapshot_id, :trade_date, :as_of_exclusive,
                     :stock_code, :industry_name, :industry_type,
                     :source_system, :source_fact_id,
                     :source_effective_at, :source_etl_sync_at, :row_hash)
                """), row)
            report["idempotent_replay"] = False
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    load_project_env(ROOT)
    engine = create_tool_engine()
    if args.apply:
        report = capture_industry_history(engine, trade_date=args.trade_date)
    else:
        with engine.connect() as connection:
            source_rows = [dict(row) for row in connection.execute(text("""
                SELECT id, stock_code, industry_name, industry_type, source,
                       etl_sync_at
                FROM si_industry_sw
                WHERE industry_type IN ('L1','一级行业','申万一级','SW2021')
                ORDER BY stock_code, etl_sync_at DESC, id DESC
            """)).mappings().all()]
        newest: dict[str, dict[str, Any]] = {}
        for row in source_rows:
            newest.setdefault(str(row.get("stock_code") or "").zfill(6), row)
        snapshot_id, rows = build_history_rows(
            newest.values(), trade_date=args.trade_date,
        )
        report = {
            "status": "PREVIEW",
            "trade_date": args.trade_date,
            "snapshot_id": snapshot_id,
            "source_row_count": len(source_rows),
            "selected_row_count": len(rows),
            "row_count": len(rows),
            "historical_backfill_allowed": False,
        }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
