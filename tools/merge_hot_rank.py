#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
融合东财人气榜 + 同花顺热股 + 雪球热股 + 新浪热股 → 统一榜单，支持单日和 N 天统计。

数据源：
  - st_hot_pop_rank_east（东财人气榜TOP100）
  - st_hot_rank_ths（同花顺热股TOP100）
  - st_hot_rank_xq（雪球热股TOP100）
  - st_hot_rank_sina（新浪热股TOP100）

输出表：
  - st_hot_rank_fused：单日融合Top100
  - st_hot_rank_multi_day：多日持续上榜统计（--days 3 / --days 5）
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _load_sql(file_name: str) -> str:
    p = ROOT / "tools" / file_name
    if p.exists():
        return p.read_text(encoding="utf-8")
    return ""


def run_ddl(engine, file_name: str):
    sql = _load_sql(file_name)
    if not sql:
        print(f"  SQL文件 {file_name} 未找到，跳过自动建表")
        return
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s and not s.startswith("--"):
                try:
                    conn.execute(text(s))
                except Exception as e:
                    if "already exists" in str(e) or "Duplicate" in str(e):
                        pass
                    else:
                        print(f"  执行SQL警告（可忽略）: {e}")
    print(f"  已执行建表SQL: {file_name}")


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
        df = pd.read_sql(
            text("SELECT stock_code, plate_name FROM si_stock_plate_east WHERE plate_type = '行业'"),
            engine
        )
        for _, row in df.iterrows():
            code = str(row["stock_code"]).strip()
            if code and code not in mapping:
                mapping[code] = str(row["plate_name"])
    except Exception as e:
        pass
    if not mapping:
        try:
            df = pd.read_sql(
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
            pass
    print(f"  [板块] 加载了 {len(mapping)} 个股的行业映射")
    return mapping


def _filter_hs_a(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "stock_code" in df.columns:
        df = df[df["stock_code"].astype(str).str.match(r"^(0|6|3)")]
    return df


def _attach_industry(df: pd.DataFrame, industry_map: dict[str, str]) -> pd.DataFrame:
    if "industry_name" not in df.columns:
        df["industry_name"] = None
    df["industry_name"] = df["stock_code"].map(lambda c: industry_map.get(str(c).strip()))
    return df


def _read_day_data(engine, dt: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    east = pd.read_sql(
        text("SELECT * FROM st_hot_pop_rank_east WHERE snapshot_date = :d ORDER BY rank"),
        engine, params={"d": dt}
    )
    if east.empty:
        fallback = pd.read_sql(
            text("SELECT * FROM st_hot_pop_rank_east WHERE snapshot_date <= :d ORDER BY snapshot_date DESC, rank LIMIT 200"),
            engine, params={"d": dt}
        )
        if not fallback.empty:
            fb_date = fallback.iloc[0]["snapshot_date"]
            east = fallback[fallback["snapshot_date"] == fb_date].copy()
            print(f"  [兜底] 东财当日({dt})无数据，使用 {fb_date} 的 {len(east)} 条数据")
    ths = pd.read_sql(
        text("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY rank"),
        engine, params={"d": dt}
    )
    try:
        xq = pd.read_sql(
            text("SELECT * FROM st_hot_rank_xq WHERE snapshot_date = :d ORDER BY `rank`"),
            engine, params={"d": dt}
        )
    except Exception:
        xq = pd.DataFrame()
    try:
        sina = pd.read_sql(
            text("SELECT * FROM st_hot_rank_sina WHERE snapshot_date = :d ORDER BY `rank`"),
            engine, params={"d": dt}
        )
    except Exception:
        sina = pd.DataFrame()
    east = _filter_hs_a(east)
    ths = _filter_hs_a(ths)
    xq = _filter_hs_a(xq)
    sina = _filter_hs_a(sina)
    return east, ths, xq, sina


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

    for _, row in xq_df.iterrows():
        code = str(row["stock_code"]).strip()
        r = _get(code)
        r["xq_rank"] = int(row["rank"])
        r["xq_score"] = _score_from_rank(row["rank"])
        r["sources"].add("xq")
        if not r["short_name"]:
            r["short_name"] = str(row.get("short_name", "") or "")
        if r["change_pct"] is None and pd.notna(row.get("percent")):
            r["change_pct"] = row["percent"]

    for _, row in sina_df.iterrows():
        code = str(row["stock_code"]).strip()
        r = _get(code)
        r["sina_rank"] = int(row["rank"])
        r["sina_score"] = _score_from_rank(row["rank"])
        r["sources"].add("sina")
        if not r["short_name"]:
            r["short_name"] = str(row.get("short_name", "") or "")
        if r["change_pct"] is None and pd.notna(row.get("change_pct")):
            r["change_pct"] = row["change_pct"]

    for r in stock_map.values():
        r["total_score"] = r["east_score"] + r["ths_score"] + r["xq_score"] + r["sina_score"]
        srcs = r["sources"]
        if len(srcs) == 4:
            r["source_flag"] = "all"
        elif len(srcs) == 3:
            if "east" in srcs and "ths" in srcs and "xq" in srcs:
                r["source_flag"] = "east_ths_xq"
            elif "east" in srcs and "ths" in srcs and "sina" in srcs:
                r["source_flag"] = "east_ths_sina"
            elif "east" in srcs and "xq" in srcs and "sina" in srcs:
                r["source_flag"] = "east_xq_sina"
            else:
                r["source_flag"] = "ths_xq_sina"
        elif len(srcs) == 2:
            if "east" in srcs and "ths" in srcs:
                r["source_flag"] = "both"
            elif "east" in srcs and "xq" in srcs:
                r["source_flag"] = "east_xq"
            elif "east" in srcs and "sina" in srcs:
                r["source_flag"] = "east_sina"
            elif "ths" in srcs and "xq" in srcs:
                r["source_flag"] = "ths_xq"
            elif "ths" in srcs and "sina" in srcs:
                r["source_flag"] = "ths_sina"
            else:
                r["source_flag"] = "xq_sina"
        else:
            if "east" in srcs:
                r["source_flag"] = "east_only"
            elif "ths" in srcs:
                r["source_flag"] = "ths_only"
            elif "xq" in srcs:
                r["source_flag"] = "xq_only"
            else:
                r["source_flag"] = "sina_only"
        del r["sources"]

    df = pd.DataFrame(list(stock_map.values()))
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df


def run_single_day(engine, snapshot_date: str, top_n: int, save: bool):
    print(f"\n{'='*70}")
    print(f"  单日融合榜单：东财人气榜 × 同花顺热股 × 雪球热股  → 统一 Top{top_n}")
    print(f"  快照日期: {snapshot_date}")
    print(f"{'='*70}")

    east_df, ths_df, xq_df, sina_df = _read_day_data(engine, snapshot_date)
    if east_df.empty and ths_df.empty and xq_df.empty and sina_df.empty:
        print("  所有数据源均无数据，请先执行 fetch 脚本获取数据。")
        return

    print(f"  东财人气榜: {len(east_df)} 条")
    print(f"  同花顺热股: {len(ths_df)} 条")
    print(f"  雪球热股: {len(xq_df)} 条")
    print(f"  新浪热股: {len(sina_df)} 条")

    result_df = _fuse_single_day(east_df, ths_df, xq_df, sina_df)
    result_df["fused_rank"] = range(1, len(result_df) + 1)
    result_df["snapshot_date"] = snapshot_date
    result_df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    industry_map = _load_industry_map(engine)
    _attach_industry(result_df, industry_map)

    top_df = result_df.head(top_n).copy()

    print(f"\n  {'═'*78}")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<14} {'涨跌幅':>8} {'东财排名':>8} {'同花顺':>8} {'雪球':>6} {'综合分':>8} {'来源'}")
    print(f"  {'═'*78}")
    for _, r in top_df.iterrows():
        src = source_tag(r["source_flag"])
        east_r = f"{int(r['east_rank'])}" if pd.notna(r["east_rank"]) else "-"
        ths_r = f"{int(r['ths_rank'])}" if pd.notna(r["ths_rank"]) else "-"
        xq_r = f"{int(r['xq_rank'])}" if pd.notna(r["xq_rank"]) else "-"
        chg = f"{r['change_pct']:.2f}%" if pd.notna(r["change_pct"]) else "-"
        print(f"  {int(r['fused_rank']):>4} {r['stock_code']:<10} {r['short_name']:<14} {chg:>8} {east_r:>8} {ths_r:>8} {xq_r:>6} {r['total_score']:>8.2f} {src}")

    both = len(top_df[top_df["source_flag"] == "both"])
    all_three = len(top_df[top_df["source_flag"].isin(["east_ths_xq", "east_ths_sina", "east_xq_sina", "ths_xq_sina"])])
    all_four = len(top_df[top_df["source_flag"] == "all"])
    east_xq = len(top_df[top_df["source_flag"] == "east_xq"])
    ths_xq = len(top_df[top_df["source_flag"] == "ths_xq"])
    east_sina = len(top_df[top_df["source_flag"] == "east_sina"])
    ths_sina = len(top_df[top_df["source_flag"] == "ths_sina"])
    xq_sina = len(top_df[top_df["source_flag"] == "xq_sina"])
    east_only = len(top_df[top_df["source_flag"] == "east_only"])
    ths_only = len(top_df[top_df["source_flag"] == "ths_only"])
    xq_only = len(top_df[top_df["source_flag"] == "xq_only"])
    sina_only = len(top_df[top_df["source_flag"] == "sina_only"])
    print(f"\n  融合统计: 4源 {all_four} | 3源 {all_three} | 东财+同花顺 {both} | "
          f"东财+雪球 {east_xq} | 东财+新浪 {east_sina} | 同花顺+雪球 {ths_xq} | "
          f"同花顺+新浪 {ths_sina} | 雪球+新浪 {xq_sina} | "
          f"仅东财 {east_only} | 仅同花顺 {ths_only} | 仅雪球 {xq_only} | 仅新浪 {sina_only}")

    if save:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM `st_hot_rank_fused` WHERE `snapshot_date` = :d"), {"d": snapshot_date})
        top_df.to_sql("st_hot_rank_fused", engine, if_exists="append", index=False, chunksize=500, method="multi")
        print(f"  已写入 st_hot_rank_fused，共 {len(top_df)} 行")


def run_multi_day(engine, end_date: str, num_days: int, top_n: int, save: bool):
    print(f"\n{'='*70}")
    print(f"  ★ 多日持续上榜统计：近 {num_days} 天强势股追踪（东财+同花顺+雪球）★")
    print(f"  截止日期: {end_date}，统计区间: 近{num_days}天")
    print(f"{'='*70}")

    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(num_days)]
    date_list.reverse()

    print(f"  扫描日期: {date_list[0]} ~ {date_list[-1]} 共 {num_days} 天")

    stock_days: dict[str, dict] = defaultdict(lambda: {
        "appear_days": 0, "east_ranks": [], "ths_ranks": [], "xq_ranks": [], "sina_ranks": [],
        "scores": [], "change_pcts": [], "short_name": "",
    })

    for dt in date_list:
        east_df, ths_df, xq_df, sina_df = _read_day_data(engine, dt)
        if east_df.empty and ths_df.empty and xq_df.empty and sina_df.empty:
            continue
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
        for _, row in xq_df.iterrows():
            code = str(row["stock_code"]).strip()
            if code not in day_codes:
                stock_days[code]["appear_days"] += 1
                if not stock_days[code]["short_name"]:
                    stock_days[code]["short_name"] = str(row.get("short_name", "") or "")
            stock_days[code]["xq_ranks"].append(int(row["rank"]))
            stock_days[code]["scores"].append(_score_from_rank(row["rank"]))
            chg_val = row.get("percent") if pd.notna(row.get("percent")) else row.get("change_pct")
            if chg_val is not None and pd.notna(chg_val):
                stock_days[code]["change_pcts"].append(float(chg_val))
        for _, row in sina_df.iterrows():
            code = str(row["stock_code"]).strip()
            if code not in day_codes:
                stock_days[code]["appear_days"] += 1
                if not stock_days[code]["short_name"]:
                    stock_days[code]["short_name"] = str(row.get("short_name", "") or "")
            stock_days[code]["sina_ranks"].append(int(row["rank"]))
            stock_days[code]["scores"].append(_score_from_rank(row["rank"]))
            if pd.notna(row.get("change_pct")):
                stock_days[code]["change_pcts"].append(float(row["change_pct"]))

    if not stock_days:
        print("  区间内无数据")
        return

    last_east, last_ths, last_xq, last_sina = _read_day_data(engine, date_list[-1])
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
        if info["xq_ranks"]:
            srcs.add("xq")
        if info["sina_ranks"]:
            srcs.add("sina")
        if len(srcs) == 4:
            src = "all"
        elif len(srcs) == 3:
            if "east" in srcs and "ths" in srcs and "xq" in srcs:
                src = "east_ths_xq"
            elif "east" in srcs and "ths" in srcs and "sina" in srcs:
                src = "east_ths_sina"
            elif "east" in srcs and "xq" in srcs and "sina" in srcs:
                src = "east_xq_sina"
            else:
                src = "ths_xq_sina"
        elif len(srcs) == 2:
            if "east" in srcs and "ths" in srcs:
                src = "both"
            elif "east" in srcs and "xq" in srcs:
                src = "east_xq"
            elif "east" in srcs and "sina" in srcs:
                src = "east_sina"
            elif "ths" in srcs and "xq" in srcs:
                src = "ths_xq"
            elif "ths" in srcs and "sina" in srcs:
                src = "ths_sina"
            else:
                src = "xq_sina"
        else:
            if "east" in srcs:
                src = "east_only"
            elif "ths" in srcs:
                src = "ths_only"
            elif "xq" in srcs:
                src = "xq_only"
            else:
                src = "sina_only"

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
    print(f"  {'排名':>4} {'代码':<10} {'名称':<12} {'出现/总':>8} {'频率':>6} {'均东财':>7} {'均同花':>7} {'均雪球':>7} {'最新东':>6} {'最新同':>6} {'最新雪':>6} {'均涨跌':>8}")
    print(f"  {'═'*95}")
    for _, r in top_df.iterrows():
        app = f"{int(r['appear_days'])}/{num_days}"
        freq = f"{r['continuity_rate']:.0f}%"
        ae = f"{r['avg_east_rank']:.1f}" if pd.notna(r["avg_east_rank"]) else "-"
        at = f"{r['avg_ths_rank']:.1f}" if pd.notna(r["avg_ths_rank"]) else "-"
        ax = f"{r['avg_xq_rank']:.1f}" if pd.notna(r["avg_xq_rank"]) else "-"
        le = f"{int(r['last_east_rank'])}" if pd.notna(r["last_east_rank"]) else "-"
        lt = f"{int(r['last_ths_rank'])}" if pd.notna(r["last_ths_rank"]) else "-"
        lx = f"{int(r['last_xq_rank'])}" if pd.notna(r["last_xq_rank"]) else "-"
        chg = f"{r['avg_change_pct']:.2f}%" if pd.notna(r["avg_change_pct"]) else "-"
        print(f"  {int(r['fused_rank']):>4} {r['stock_code']:<10} {r['short_name']:<12} {app:>8} {freq:>6} {ae:>7} {at:>7} {ax:>7} {le:>6} {lt:>6} {lx:>6} {chg:>8}")

    full_cover = len(top_df[top_df["appear_days"] == num_days])
    appear_ge_half = len(top_df[top_df["appear_days"] >= (num_days + 1) // 2])
    print(f"\n  多日统计: 全部{num_days}天均上榜 {full_cover} 只 | 半数以上 {appear_ge_half} 只")

    if save:
        top_df["stat_date"] = end_date
        top_df["stat_days"] = num_days
        top_df["etl_sync_at"] = datetime.now().replace(microsecond=0)
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM `st_hot_rank_multi_day` WHERE `stat_date` = :d AND `stat_days` = :n"),
                {"d": end_date, "n": num_days}
            )
        top_df.to_sql("st_hot_rank_multi_day", engine, if_exists="append", index=False, chunksize=500, method="multi")
        print(f"  已写入 st_hot_rank_multi_day，共 {len(top_df)} 行")


def main():
    parser = argparse.ArgumentParser(description="融合东财人气榜 + 同花顺热股 + 雪球热股 → 统一榜单")
    parser.add_argument("date", nargs="?", help="快照/截止日期，格式：YYYY-MM-DD，默认今天")
    parser.add_argument("--top", type=int, default=100, help="输出前N名，默认100")
    parser.add_argument("--days", type=int, default=0,
                        help="多日统计模式：统计近 N 天持续上榜的股票。例：--days 3, --days 5。默认0=单日")
    parser.add_argument("--no-save", action="store_true", help="不写入数据库，仅打印")
    args = parser.parse_args()

    snapshot_date = args.date or datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(snapshot_date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {snapshot_date}")
        return

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    run_ddl(engine, "02_hot_rank_extra_tables.sql")

    if args.days > 1:
        run_multi_day(engine, snapshot_date, args.days, args.top, save=not args.no_save)
    else:
        run_single_day(engine, snapshot_date, args.top, save=not args.no_save)


if __name__ == "__main__":
    main()