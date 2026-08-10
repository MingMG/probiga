#!/usr/bin/env python3
from env_config import create_tool_engine, resolve_tool_mysql_url
# -*- coding: utf-8 -*-
"""
热门数据统计：从 st_hot_concept_ths_daily、st_hot_rank_ths、st_hot_pop_rank_east
汇总统计结果并打印，可将结果写入 st_hot_stats 表。
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import read_frame, write_frame

def _ensure_stats_table(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS `st_hot_stats` (
      `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
      `stat_date` DATE NOT NULL COMMENT '统计日期',
      `stat_type` VARCHAR(64) NOT NULL COMMENT '统计类型',
      `stat_name` VARCHAR(256) NOT NULL COMMENT '统计项名称',
      `stat_value` DECIMAL(50,6) DEFAULT NULL COMMENT '统计值',
      `stat_desc` VARCHAR(1024) DEFAULT NULL COMMENT '统计说明',
      `etl_sync_at` DATETIME NOT NULL COMMENT '同步写入时间',
      PRIMARY KEY (`id`),
      KEY `idx_stats_date` (`stat_date`),
      KEY `idx_stats_type` (`stat_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='热门数据统计汇总'
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("已确保 st_hot_stats 表存在")


def stats_hot_concept_ths(engine, stat_date: str, save: bool):
    print(f"\n{'='*60}")
    print(f"【同花顺热门概念/行业TOP20】统计日期: {stat_date}")
    print(f"{'='*60}")

    q = text("SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d ORDER BY plate_type, `rank`")
    df = read_frame(q, engine, params={"d": stat_date})
    if df.empty:
        print(f"  {stat_date} 无数据")
        return

    concept = df[df["plate_type"] == 1]
    industry = df[df["plate_type"] == 2]

    print(f"\n  概念板块 TOP20:")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<20} {'涨跌幅(%)':>10} {'热度值':>10}")
    print(f"  {'-'*56}")
    for _, r in concept.iterrows():
        print(f"  {int(r['rank']):>4} {r['concept_code']:<10} {r['concept_name']:<20} {r['change_pct'] or 0:>10.2f} {r['hot_value'] or 0:>10.2f}")

    print(f"\n  行业板块 TOP20:")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<20} {'涨跌幅(%)':>10} {'热度值':>10}")
    print(f"  {'-'*56}")
    for _, r in industry.iterrows():
        print(f"  {int(r['rank']):>4} {r['concept_code']:<10} {r['concept_name']:<20} {r['change_pct'] or 0:>10.2f} {r['hot_value'] or 0:>10.2f}")

    if save:
        now = datetime.now().replace(microsecond=0)
        rows = []
        for _, r in df.iterrows():
            label = "概念板块" if r["plate_type"] == 1 else "行业板块"
            rows.append({"stat_date": stat_date, "stat_type": f"同花顺热门{label}TOP20", "stat_name": r["concept_name"], "stat_value": r["hot_value"], "stat_desc": f"排名{int(r['rank'])} 涨跌幅{r['change_pct']:.2f}%", "etl_sync_at": now})
        write_frame(pd.DataFrame(rows), "st_hot_stats", engine, if_exists="append", index=False, chunksize=500, method="multi")
        print(f"\n  已保存 {len(rows)} 条到 st_hot_stats")


def stats_hot_rank_ths(engine, stat_date: str, save: bool):
    print(f"\n{'='*60}")
    print(f"【同花顺热股TOP100】统计日期: {stat_date}")
    print(f"{'='*60}")

    q = text("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY `rank`")
    df = read_frame(q, engine, params={"d": stat_date})
    if df.empty:
        print(f"  {stat_date} 无数据")
        return

    print(f"\n  热股 TOP100:")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<16} {'涨跌幅(%)':>10} {'热度值':>10} {'人气标签':<12}")
    print(f"  {'-'*66}")
    for _, r in df.iterrows():
        print(f"  {int(r['rank']):>4} {r['stock_code']:<10} {r['short_name']:<16} {r['change_pct'] or 0:>10.2f} {r['hot_value'] or 0:>10.2f} {str(r.get('pop_tag','') or ''):<12}")

    top10 = df.head(10)["short_name"].tolist()
    up_count = len(df[df["change_pct"] > 0]) if "change_pct" in df.columns else 0
    down_count = len(df[df["change_pct"] < 0]) if "change_pct" in df.columns else 0
    print(f"\n  汇总: TOP10: {', '.join(top10)}")
    print(f"        上涨: {up_count} 只, 下跌: {down_count} 只")

    if save:
        now = datetime.now().replace(microsecond=0)
        rows = [{"stat_date": stat_date, "stat_type": "同花顺热股TOP100-涨跌统计", "stat_name": "上涨家数", "stat_value": up_count, "stat_desc": f"下跌家数: {down_count}", "etl_sync_at": now}]
        for _, r in df.iterrows():
            rows.append({"stat_date": stat_date, "stat_type": "同花顺热股TOP100", "stat_name": r["short_name"], "stat_value": r["hot_value"], "stat_desc": f"排名{int(r['rank'])} 涨跌幅{r['change_pct']:.2f}%", "etl_sync_at": now})
        write_frame(pd.DataFrame(rows), "st_hot_stats", engine, if_exists="append", index=False, chunksize=500, method="multi")
        print(f"  已保存 {len(rows)} 条到 st_hot_stats")


def stats_hot_pop_rank_east(engine, stat_date: str, save: bool):
    print(f"\n{'='*60}")
    print(f"【东财人气榜TOP100】统计日期: {stat_date}")
    print(f"{'='*60}")

    q = text("SELECT * FROM st_hot_pop_rank_east WHERE snapshot_date = :d ORDER BY `rank`")
    df = read_frame(q, engine, params={"d": stat_date})
    if df.empty:
        print(f"  {stat_date} 无数据")
        return

    print(f"\n  人气榜 TOP100:")
    print(f"  {'排名':>4} {'代码':<10} {'名称':<16} {'最新价':>10} {'涨跌幅(%)':>10}")
    print(f"  {'-'*52}")
    for _, r in df.iterrows():
        print(f"  {int(r['rank']):>4} {r['stock_code']:<10} {r['short_name']:<16} {r['price'] or 0:>10.2f} {r['change_pct'] or 0:>10.2f}")

    top10 = df.head(10)["short_name"].tolist()
    up_count = len(df[df["change_pct"] > 0]) if "change_pct" in df.columns else 0
    down_count = len(df[df["change_pct"] < 0]) if "change_pct" in df.columns else 0
    print(f"\n  汇总: TOP10: {', '.join(top10)}")
    print(f"        上涨: {up_count} 只, 下跌: {down_count} 只")

    if save:
        now = datetime.now().replace(microsecond=0)
        rows = [{"stat_date": stat_date, "stat_type": "东财人气榜TOP100-涨跌统计", "stat_name": "上涨家数", "stat_value": up_count, "stat_desc": f"下跌家数: {down_count}", "etl_sync_at": now}]
        for _, r in df.iterrows():
            rows.append({"stat_date": stat_date, "stat_type": "东财人气榜TOP100", "stat_name": r["short_name"], "stat_value": r["change_pct"], "stat_desc": f"排名{int(r['rank'])} 价格{r['price']:.2f}", "etl_sync_at": now})
        write_frame(pd.DataFrame(rows), "st_hot_stats", engine, if_exists="append", index=False, chunksize=500, method="multi")
        print(f"  已保存 {len(rows)} 条到 st_hot_stats")


def stats_recent_summary(engine, days: int):
    print(f"\n{'='*60}")
    print(f"【最近 {days} 天数据概况】")
    print(f"{'='*60}")

    print(f"\n  st_hot_concept_ths_daily 各日期记录数:")
    q = text("SELECT snapshot_date, COUNT(*) as cnt FROM st_hot_concept_ths_daily WHERE snapshot_date >= DATE_SUB(CURDATE(), INTERVAL :d DAY) GROUP BY snapshot_date ORDER BY snapshot_date DESC")
    df = read_frame(q, engine, params={"d": days})
    if not df.empty:
        for _, r in df.iterrows():
            print(f"    {r['snapshot_date']}: {r['cnt']} 条")
    else:
        print(f"    无数据")

    print(f"\n  st_hot_rank_ths 各日期记录数:")
    q = text("SELECT snapshot_date, COUNT(*) as cnt FROM st_hot_rank_ths WHERE snapshot_date >= DATE_SUB(CURDATE(), INTERVAL :d DAY) GROUP BY snapshot_date ORDER BY snapshot_date DESC")
    df = read_frame(q, engine, params={"d": days})
    if not df.empty:
        for _, r in df.iterrows():
            print(f"    {r['snapshot_date']}: {r['cnt']} 条")
    else:
        print(f"    无数据")

    print(f"\n  st_hot_pop_rank_east 各日期记录数:")
    q = text("SELECT snapshot_date, COUNT(*) as cnt FROM st_hot_pop_rank_east WHERE snapshot_date >= DATE_SUB(CURDATE(), INTERVAL :d DAY) GROUP BY snapshot_date ORDER BY snapshot_date DESC")
    df = read_frame(q, engine, params={"d": days})
    if not df.empty:
        for _, r in df.iterrows():
            print(f"    {r['snapshot_date']}: {r['cnt']} 条")
    else:
        print(f"    无数据")


def main():
    parser = argparse.ArgumentParser(description="热门数据统计")
    parser.add_argument("date", nargs="?", help="统计日期，格式：YYYY-MM-DD，默认今天")
    parser.add_argument("--save", action="store_true", help="将统计结果写入 st_hot_stats 表")
    parser.add_argument("--recent", type=int, default=0, help="查看最近 N 天数据概况（与日期参数互斥）")
    args = parser.parse_args()

    engine = create_tool_engine(resolve_tool_mysql_url())
    _ensure_stats_table(engine)

    if args.recent > 0:
        stats_recent_summary(engine, args.recent)
        return

    stat_date = args.date or datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(stat_date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {stat_date}")
        return

    stats_hot_concept_ths(engine, stat_date, args.save)
    stats_hot_rank_ths(engine, stat_date, args.save)
    stats_hot_pop_rank_east(engine, stat_date, args.save)


if __name__ == "__main__":
    main()
