#!/usr/bin/env python3
from env_config import create_tool_engine, resolve_tool_mysql_url
# -*- coding: utf-8 -*-
"""
融合东财人气榜 + 同花顺热股 → 统一榜单，支持单日和 N 天统计。

数据源：
  - st_hot_pop_rank_east（东财人气榜TOP100）
  - st_hot_rank_ths（同花顺热股TOP100）

新浪 ``Market_Center.getHQNodeData`` 不提供可验证的关注度字段，旧数据是
证券代码序列而非热度榜，永久不得参与融合。
旧雪球 ``type=10`` 全球榜过滤后也曾写入过无法区分的伪 A 股批次；在正式
scheduler receipt 能独立绑定 XQ 数据库批次之前，雪球同样不得参与融合。

输出表：
  - st_hot_rank_fused：单日融合Top100
  - st_hot_rank_multi_day：多日持续上榜统计（--days 3 / --days 5）
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from server.common.batch_db import read_frame, replace_table_rows
from server.common.auxiliary_runtime_schema import (
    validate_hot_rank_fusion_runtime_schema,
)
from server.common.hot_rank_source_contract import (
    HOT_POP_EAST_TASK_TYPE,
    validate_rank_inventory as validate_exact_rank_inventory,
)
from server.common.ths_hot_contract import (
    validate_rank_inventory as validate_ths_rank_inventory,
)

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)


_HOT_RANK_SOURCE_TABLES = (
    ("east", "st_hot_pop_rank_east"),
    ("ths", "st_hot_rank_ths"),
)


def _warn(message: str, exc: Exception) -> None:
    print(f"[WARN] {message}: {exc}", file=sys.stderr)


def _score_from_rank(rank) -> float:
    if rank is None or pd.isna(rank):
        return 0.0
    return max(0.0, 101.0 - float(rank))


def source_tag(flag: str) -> str:
    mapping = {
        "all": "4源",
        "east_ths_xq": "东财+同花顺+雪球",
        "east_ths_sina": "东财+同花顺+新浪",
        "east_xq_sina": "东财+雪球+新浪",
        "ths_xq_sina": "同花顺+雪球+新浪",
        "both": "东财+同花顺",
        "east_xq": "东财+雪球",
        "east_sina": "东财+新浪",
        "ths_xq": "同花顺+雪球",
        "ths_sina": "同花顺+新浪",
        "xq_sina": "雪球+新浪",
        "east_only": "仅东财",
        "ths_only": "仅同花顺",
        "xq_only": "仅雪球",
        "sina_only": "仅新浪",
    }
    return mapping.get(flag, flag)


def _load_industry_map(engine) -> dict[str, str]:
    mapping: dict[str, str] = {}
    try:
        df = read_frame(
            text("SELECT stock_code, plate_name FROM si_stock_plate_east WHERE plate_type = '行业'"),
            engine
        )
        for _, row in df.iterrows():
            code = str(row["stock_code"]).strip()
            if code and code not in mapping:
                mapping[code] = str(row["plate_name"])
    except Exception as e:
        _warn("failed to load east industry map", e)
    if not mapping:
        try:
            df = read_frame(
                text("SELECT stock_code, industry_name, industry_type FROM si_industry_sw WHERE industry_name IS NOT NULL"),
                engine
            )
            if not df.empty:
                df["priority"] = df["industry_type"].apply(lambda t: 0 if t == "申万一级" else 1)
                df = df.sort_values("priority")
                for _, row in df.iterrows():
                    code = str(row["stock_code"]).strip()
                    if code and code not in mapping:
                        mapping[code] = str(row["industry_name"])
        except Exception as e:
            _warn("failed to load SW industry map", e)
    print(f"  [板块] 加载了 {len(mapping)} 个股的行业映射")
    return mapping


def _filter_hs_a(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "stock_code" in df.columns:
        df = df[
            df["stock_code"].astype(str).str.match(r"^(0|3|4|6|8|9)")
        ]
    return df


def _attach_industry(df: pd.DataFrame, industry_map: dict[str, str]) -> pd.DataFrame:
    if "industry_name" not in df.columns:
        df["industry_name"] = None
    df["industry_name"] = df["stock_code"].map(lambda c: industry_map.get(str(c).strip()))
    return df


def _read_day_data(engine, dt: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    east = read_frame(
        text("SELECT * FROM st_hot_pop_rank_east WHERE snapshot_date = :d ORDER BY `rank`"),
        engine, params={"d": dt}
    )
    ths = read_frame(
        text("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY `rank`"),
        engine, params={"d": dt}
    )
    east = _filter_hs_a(east)
    ths = _filter_hs_a(ths)
    # XQ and Sina are deliberately not queried.  Empty compatibility slots
    # keep the existing internal tuple/schema stable while making old
    # unproven batches impossible to fuse.
    return east, ths, pd.DataFrame(), pd.DataFrame()


def _trusted_same_day_sources(
    snapshot_date: str,
    east: pd.DataFrame,
    ths: pd.DataFrame,
    xq: pd.DataFrame,
    sina: pd.DataFrame,
) -> tuple[
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
    tuple[str, ...],
]:
    """Return only exact-date sources whose complete inventory is valid."""

    sources = {
        "east": east,
        "ths": ths,
    }
    trusted: dict[str, pd.DataFrame] = {
        "east": pd.DataFrame(),
        "ths": pd.DataFrame(),
    }
    available: list[str] = []
    for source, frame in sources.items():
        if frame.empty:
            continue
        if "snapshot_date" not in frame.columns:
            raise RuntimeError(
                "DATA_BLOCKED: "
                f"{source} hot-rank rows have no snapshot_date for {snapshot_date}"
            )
        observed_dates = {
            str(value)[:10]
            for value in frame["snapshot_date"].dropna().tolist()
        }
        if observed_dates != {snapshot_date}:
            raise RuntimeError(
                "DATA_BLOCKED: "
                f"{source} hot-rank date mismatch for {snapshot_date}: "
                f"observed={sorted(observed_dates)}"
            )
        try:
            records = frame.to_dict(orient="records")
            if source == "east":
                validate_exact_rank_inventory(
                    records,
                    task_type=HOT_POP_EAST_TASK_TYPE,
                    target_date=snapshot_date,
                )
            else:
                validate_ths_rank_inventory(
                    records,
                    target_date=snapshot_date,
                )
        except (KeyError, TypeError, ValueError, RuntimeError) as exc:
            _warn(
                f"DATA_BLOCKED: excluded invalid {source} hot-rank inventory "
                f"for {snapshot_date}",
                exc,
            )
            continue
        trusted[source] = frame
        available.append(source)
    return (
        (trusted["east"], trusted["ths"], pd.DataFrame(), pd.DataFrame()),
        tuple(available),
    )


def _exact_date_sources(
    snapshot_date: str,
    east: pd.DataFrame,
    ths: pd.DataFrame,
    xq: pd.DataFrame,
    sina: pd.DataFrame,
) -> tuple[str, ...]:
    _trusted, available = _trusted_same_day_sources(
        snapshot_date,
        east,
        ths,
        xq,
        sina,
    )
    return available


def _require_trusted_same_day_sources(
    snapshot_date: str,
    east: pd.DataFrame,
    ths: pd.DataFrame,
    xq: pd.DataFrame,
    sina: pd.DataFrame,
) -> tuple[
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
    tuple[str, ...],
]:
    trusted, available = _trusted_same_day_sources(
        snapshot_date,
        east,
        ths,
        xq,
        sina,
    )
    if len(available) < 2:
        raise RuntimeError(
            "DATA_BLOCKED: hot-rank fusion requires at least two exact-date "
            "complete trusted sources: "
            f"snapshot_date={snapshot_date}, available={available}"
        )
    return trusted, available


def _require_same_day_sources(
    snapshot_date: str,
    east: pd.DataFrame,
    ths: pd.DataFrame,
    xq: pd.DataFrame,
    sina: pd.DataFrame,
) -> tuple[str, ...]:
    _trusted, available = _require_trusted_same_day_sources(
        snapshot_date,
        east,
        ths,
        xq,
        sina,
    )
    return available


def _candidate_snapshot_dates(engine, end_date: str) -> list[str]:
    """Return source-backed open-market dates up to the requested upper bound."""

    candidates: set[str] = set()
    for source, table_name in _HOT_RANK_SOURCE_TABLES:
        try:
            frame = read_frame(
                text(
                    f"SELECT DISTINCT snapshot_date FROM `{table_name}` "
                    "WHERE snapshot_date <= :end_date "
                    "ORDER BY snapshot_date DESC"
                ),
                engine,
                params={"end_date": end_date},
            )
        except Exception as exc:
            _warn(f"failed to load {source} hot-rank date index", exc)
            continue
        if frame.empty:
            continue
        if "snapshot_date" not in frame.columns:
            raise RuntimeError(
                "DATA_BLOCKED: hot-rank date index has no snapshot_date: "
                f"source={source}"
            )
        for value in frame["snapshot_date"].dropna().tolist():
            candidate = str(value)[:10]
            try:
                parsed = datetime.strptime(candidate, "%Y-%m-%d").date()
            except ValueError as exc:
                raise RuntimeError(
                    "DATA_BLOCKED: hot-rank date index contains an invalid date: "
                    f"source={source}, value={candidate}"
                ) from exc
            if parsed.isoformat() != candidate:
                raise RuntimeError(
                    "DATA_BLOCKED: hot-rank date index contains a non-canonical date: "
                    f"source={source}, value={candidate}"
                )
            if candidate <= end_date:
                candidates.add(candidate)

    try:
        calendar = read_frame(
            text(
                "SELECT trade_date FROM si_trade_calendar "
                "WHERE trade_status = 1 AND trade_date <= :end_date "
                "ORDER BY trade_date DESC"
            ),
            engine,
            params={"end_date": end_date},
        )
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative trading calendar is unavailable for "
            "hot-rank multi-day fusion"
        ) from exc
    if calendar.empty or "trade_date" not in calendar.columns:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative trading calendar has no open dates for "
            f"hot-rank multi-day fusion through {end_date}"
        )
    trading_dates = {
        str(value)[:10]
        for value in calendar["trade_date"].dropna().tolist()
    }
    return sorted(candidates.intersection(trading_dates))


def _select_multi_day_window(
    engine,
    requested_end_date: str,
    num_days: int,
) -> list[
    tuple[
        str,
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
        tuple[str, ...],
    ]
]:
    """Select the latest N exact-date snapshots backed by at least two sources.

    ``requested_end_date`` is an inclusive upper bound, not a date to stamp on
    older data.  The last selected source date becomes the persisted
    ``stat_date``.  This lets a weekend request resolve to Friday and a Monday
    request use Monday/Friday/Thursday while keeping the two-source gate on
    every observation day.
    """

    if num_days < 1:
        raise ValueError("num_days must be positive")
    requested = datetime.strptime(requested_end_date, "%Y-%m-%d").date()
    if requested.isoformat() != requested_end_date:
        raise ValueError("requested_end_date must be canonical YYYY-MM-DD")

    selected_desc: list[
        tuple[
            str,
            tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
            tuple[str, ...],
        ]
    ] = []
    for snapshot_date in reversed(
        _candidate_snapshot_dates(engine, requested_end_date)
    ):
        frames = _read_day_data(engine, snapshot_date)
        trusted_frames, available = _trusted_same_day_sources(
            snapshot_date,
            *frames,
        )
        if len(available) < 2:
            print(
                "  [跳过] "
                f"{snapshot_date} 同日独立来源不足2个: {list(available)}"
            )
            continue
        # Keep the same fail-closed contract as single-day fusion.  Calling
        # the public guard here also makes future source-policy changes apply
        # to every selected multi-day observation.
        _require_same_day_sources(snapshot_date, *trusted_frames)
        selected_desc.append((snapshot_date, trusted_frames, available))
        if len(selected_desc) == num_days:
            break

    if len(selected_desc) < num_days:
        raise RuntimeError(
            "DATA_BLOCKED: hot-rank multi-day fusion has too few exact-date "
            "two-source snapshots: "
            f"requested_end_date={requested_end_date}, required={num_days}, "
            f"available={len(selected_desc)}"
        )
    selected_desc.reverse()
    return selected_desc


def _fuse_single_day(east_df: pd.DataFrame, ths_df: pd.DataFrame, xq_df: pd.DataFrame, sina_df: pd.DataFrame) -> pd.DataFrame:
    stock_map: dict[str, dict] = {}

    def _get(code: str) -> dict:
        if code not in stock_map:
            stock_map[code] = {
                "stock_code": code, "short_name": "", "change_pct": None,
                "east_rank": None, "ths_rank": None, "xq_rank": None, "sina_rank": None,
                "east_score": 0.0, "ths_score": 0.0, "xq_score": 0.0, "sina_score": 0.0,
                "total_score": 0.0, "sources": set(),
            }
        return stock_map[code]

    for _, row in east_df.iterrows():
        code = str(row["stock_code"]).strip()
        r = _get(code)
        r["east_rank"] = int(row["rank"])
        r["east_score"] = _score_from_rank(row["rank"])
        r["sources"].add("east")
        if not r["short_name"]:
            r["short_name"] = str(row.get("short_name", "") or "")
        if r["change_pct"] is None and pd.notna(row.get("change_pct")):
            r["change_pct"] = row["change_pct"]

    for _, row in ths_df.iterrows():
        code = str(row["stock_code"]).strip()
        r = _get(code)
        r["ths_rank"] = int(row["rank"])
        r["ths_score"] = _score_from_rank(row["rank"])
        r["sources"].add("ths")
        if not r["short_name"]:
            r["short_name"] = str(row.get("short_name", "") or "")
        if r["change_pct"] is None and pd.notna(row.get("change_pct")):
            r["change_pct"] = row["change_pct"]

    for r in stock_map.values():
        r["total_score"] = r["east_score"] + r["ths_score"]
        srcs = r["sources"]
        if len(srcs) == 2:
            r["source_flag"] = "both"
        else:
            if "east" in srcs:
                r["source_flag"] = "east_only"
            else:
                r["source_flag"] = "ths_only"
        del r["sources"]

    df = pd.DataFrame(list(stock_map.values()))
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df


def run_single_day(engine, snapshot_date: str, top_n: int, save: bool):
    print(f"\n{'='*70}")
    print(f"  单日融合榜单：东财人气榜 × 同花顺热股 → 统一 Top{top_n}")
    print(f"  快照日期: {snapshot_date}")
    print(f"{'='*70}")

    raw_frames = _read_day_data(engine, snapshot_date)
    trusted_frames, available_sources = _require_trusted_same_day_sources(
        snapshot_date,
        *raw_frames,
    )
    east_df, ths_df, xq_df, sina_df = trusted_frames
    print(f"  同日可用来源: {', '.join(available_sources)}")

    print(f"  东财人气榜: {len(east_df)} 条")
    print(f"  同花顺热股: {len(ths_df)} 条")

    result_df = _fuse_single_day(east_df, ths_df, xq_df, sina_df)
    result_df["fused_rank"] = range(1, len(result_df) + 1)
    result_df["snapshot_date"] = snapshot_date
    result_df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    industry_map = _load_industry_map(engine)
    _attach_industry(result_df, industry_map)

    top_df = result_df.head(top_n).copy()

    print(f"\n  {'═'*78}")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<14} {'涨跌幅':>8} {'东财排名':>8} {'同花顺':>8} {'综合分':>8} {'来源'}")
    print(f"  {'═'*78}")
    for _, r in top_df.iterrows():
        src = source_tag(r["source_flag"])
        east_r = f"{int(r['east_rank'])}" if pd.notna(r["east_rank"]) else "-"
        ths_r = f"{int(r['ths_rank'])}" if pd.notna(r["ths_rank"]) else "-"
        chg = f"{r['change_pct']:.2f}%" if pd.notna(r["change_pct"]) else "-"
        print(f"  {int(r['fused_rank']):>4} {r['stock_code']:<10} {r['short_name']:<14} {chg:>8} {east_r:>8} {ths_r:>8} {r['total_score']:>8.2f} {src}")

    both = len(top_df[top_df["source_flag"] == "both"])
    east_only = len(top_df[top_df["source_flag"] == "east_only"])
    ths_only = len(top_df[top_df["source_flag"] == "ths_only"])
    print(
        f"\n  融合统计: 东财+同花顺 {both} | "
        f"仅东财 {east_only} | 仅同花顺 {ths_only}"
    )

    if save:
        validate_hot_rank_fusion_runtime_schema(
            engine,
            tables=("st_hot_rank_fused",),
        )
        replace_table_rows(
            top_df,
            "st_hot_rank_fused",
            engine,
            where_sql="snapshot_date = :snapshot_date",
            params={"snapshot_date": snapshot_date},
            chunksize=500,
            method="multi",
        )
        print(f"  已写入 st_hot_rank_fused，共 {len(top_df)} 行")


def run_multi_day(engine, end_date: str, num_days: int, top_n: int, save: bool):
    print(f"\n{'='*70}")
    print(f"  ★ 多日持续上榜统计：近 {num_days} 个合规快照日强势股追踪 ★")
    print(f"  请求截止上限: {end_date}")
    print(f"{'='*70}")

    window = _select_multi_day_window(engine, end_date, num_days)
    date_list = [item[0] for item in window]
    stat_date = date_list[-1]

    print(f"  实际统计日: {stat_date}")
    print(f"  合规日期序列: {', '.join(date_list)}")

    stock_days: dict[str, dict] = defaultdict(lambda: {
        "appear_days": 0, "east_ranks": [], "ths_ranks": [], "xq_ranks": [], "sina_ranks": [],
        "scores": [], "change_pcts": [], "short_name": "",
    })

    for dt, frames, available_sources in window:
        east_df, ths_df, xq_df, sina_df = frames
        print(f"  {dt} 同日可用来源: {', '.join(available_sources)}")
        day_codes = set()
        for _, row in east_df.iterrows():
            code = str(row["stock_code"]).strip()
            stock_days[code]["appear_days"] += 1
            stock_days[code]["east_ranks"].append(int(row["rank"]))
            stock_days[code]["short_name"] = str(row.get("short_name", "") or "")
            stock_days[code]["scores"].append(_score_from_rank(row["rank"]))
            if pd.notna(row.get("change_pct")):
                stock_days[code]["change_pcts"].append(float(row["change_pct"]))
            day_codes.add(code)
        for _, row in ths_df.iterrows():
            code = str(row["stock_code"]).strip()
            if code not in day_codes:
                stock_days[code]["appear_days"] += 1
                if not stock_days[code]["short_name"]:
                    stock_days[code]["short_name"] = str(row.get("short_name", "") or "")
                day_codes.add(code)
            stock_days[code]["ths_ranks"].append(int(row["rank"]))
            stock_days[code]["scores"].append(_score_from_rank(row["rank"]))
            if pd.notna(row.get("change_pct")):
                stock_days[code]["change_pcts"].append(float(row["change_pct"]))
    if not stock_days:
        raise RuntimeError(
            "DATA_BLOCKED: selected hot-rank multi-day window has no stock rows"
        )

    last_east, last_ths, last_xq, last_sina = window[-1][1]
    last_east_map = {str(r["stock_code"]).strip(): int(r["rank"]) for _, r in last_east.iterrows()}
    last_ths_map = {str(r["stock_code"]).strip(): int(r["rank"]) for _, r in last_ths.iterrows()}
    last_xq_map = {str(r["stock_code"]).strip(): int(r["rank"]) for _, r in last_xq.iterrows()}
    last_sina_map = {str(r["stock_code"]).strip(): int(r["rank"]) for _, r in last_sina.iterrows()}

    rows = []
    for code, info in stock_days.items():
        avg_east = np.mean(info["east_ranks"]) if info["east_ranks"] else None
        avg_ths = np.mean(info["ths_ranks"]) if info["ths_ranks"] else None
        avg_xq = np.mean(info["xq_ranks"]) if info["xq_ranks"] else None
        avg_sina = np.mean(info["sina_ranks"]) if info["sina_ranks"] else None
        avg_score = np.mean(info["scores"]) if info["scores"] else 0
        avg_chg = np.mean(info["change_pcts"]) if info["change_pcts"] else None

        srcs = set()
        if info["east_ranks"]:
            srcs.add("east")
        if info["ths_ranks"]:
            srcs.add("ths")
        if len(srcs) == 2:
            src = "both"
        else:
            if "east" in srcs:
                src = "east_only"
            else:
                src = "ths_only"

        rows.append({
            "stock_code": code,
            "short_name": info["short_name"],
            "appear_days": info["appear_days"],
            "continuity_rate": round(info["appear_days"] / num_days * 100, 2),
            "avg_east_rank": round(avg_east, 2) if avg_east else None,
            "avg_ths_rank": round(avg_ths, 2) if avg_ths else None,
            "avg_xq_rank": round(avg_xq, 2) if avg_xq else None,
            "avg_sina_rank": round(avg_sina, 2) if avg_sina else None,
            "last_east_rank": last_east_map.get(code),
            "last_ths_rank": last_ths_map.get(code),
            "last_xq_rank": last_xq_map.get(code),
            "last_sina_rank": last_sina_map.get(code),
            "avg_total_score": round(avg_score, 2),
            "avg_change_pct": round(avg_chg, 4) if avg_chg else None,
            "source_flag": src,
        })

    result_df = pd.DataFrame(rows)
    result_df = result_df.sort_values(["appear_days", "avg_total_score"], ascending=[False, False]).reset_index(drop=True)
    result_df["fused_rank"] = range(1, len(result_df) + 1)

    industry_map = _load_industry_map(engine)
    _attach_industry(result_df, industry_map)

    top_df = result_df.head(top_n).copy()

    print(f"\n  {'═'*95}")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<12} {'出现/总':>8} {'频率':>6} {'均东财':>7} {'均同花':>7} {'最新东':>6} {'最新同':>6} {'均涨跌':>8}")
    print(f"  {'═'*95}")
    for _, r in top_df.iterrows():
        app = f"{int(r['appear_days'])}/{num_days}"
        freq = f"{r['continuity_rate']:.0f}%"
        ae = f"{r['avg_east_rank']:.1f}" if pd.notna(r["avg_east_rank"]) else "-"
        at = f"{r['avg_ths_rank']:.1f}" if pd.notna(r["avg_ths_rank"]) else "-"
        le = f"{int(r['last_east_rank'])}" if pd.notna(r["last_east_rank"]) else "-"
        lt = f"{int(r['last_ths_rank'])}" if pd.notna(r["last_ths_rank"]) else "-"
        chg = f"{r['avg_change_pct']:.2f}%" if pd.notna(r["avg_change_pct"]) else "-"
        print(f"  {int(r['fused_rank']):>4} {r['stock_code']:<10} {r['short_name']:<12} {app:>8} {freq:>6} {ae:>7} {at:>7} {le:>6} {lt:>6} {chg:>8}")

    full_cover = len(top_df[top_df["appear_days"] == num_days])
    appear_ge_half = len(top_df[top_df["appear_days"] >= (num_days + 1) // 2])
    print(
        f"\n  多日统计: 全部{num_days}个合规日均上榜 {full_cover} 只 | "
        f"半数以上 {appear_ge_half} 只"
    )

    if save:
        validate_hot_rank_fusion_runtime_schema(
            engine,
            tables=("st_hot_rank_multi_day",),
        )
        top_df["stat_date"] = stat_date
        top_df["stat_days"] = num_days
        top_df["etl_sync_at"] = datetime.now().replace(microsecond=0)
        replace_table_rows(
            top_df,
            "st_hot_rank_multi_day",
            engine,
            where_sql="stat_date = :stat_date AND stat_days = :stat_days",
            params={"stat_date": stat_date, "stat_days": num_days},
            chunksize=500,
            method="multi",
        )
        print(f"  已写入 st_hot_rank_multi_day，共 {len(top_df)} 行")
    print(f"DATE={stat_date}")
    return {
        "requested_end_date": end_date,
        "stat_date": stat_date,
        "snapshot_dates": date_list,
        "rows": len(top_df),
    }


def main():
    parser = argparse.ArgumentParser(description="融合东财人气榜 + 同花顺热股 → 统一榜单")
    parser.add_argument("date", nargs="?", help="快照/截止日期，格式：YYYY-MM-DD，默认今天")
    parser.add_argument("--top", type=int, default=100, help="输出前N名，默认100")
    parser.add_argument("--days", type=int, default=0,
                        help=(
                            "多日统计模式：统计截止上限之前最近 N 个同日双源"
                            "合规交易日。例：--days 3, --days 5。默认0=单日"
                        ))
    parser.add_argument("--no-save", action="store_true", help="不写入数据库，仅打印")
    args = parser.parse_args()

    snapshot_date = args.date or datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(snapshot_date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {snapshot_date}")
        return

    engine = create_tool_engine(resolve_tool_mysql_url())

    if args.days > 1:
        run_multi_day(engine, snapshot_date, args.days, args.top, save=not args.no_save)
    else:
        run_single_day(engine, snapshot_date, args.top, save=not args.no_save)


if __name__ == "__main__":
    main()
