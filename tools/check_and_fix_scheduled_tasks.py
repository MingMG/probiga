#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查并修复定时任务配置"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

# 需要确保存在的任务列表
REQUIRED_TASKS = [
    # (task_name, task_type, script_path, script_args, cron_time, sort_order)
    ('东财板块热度', 'sector_heat_east', 'tools/fetch_sector_heat_east_daily.py', '', '17:08', 10),
    ('同花顺热门概念', 'hot_concept', 'tools/fetch_hot_concept_ths_daily.py', '', '17:10', 11),
    ('同花顺热股TOP100', 'hot_rank_ths', 'tools/fetch_hot_rank_ths.py', '', '17:12', 12),
    ('东财人气榜TOP100', 'hot_pop_east', 'tools/fetch_hot_pop_rank_east.py', '', '17:14', 13),
    ('新浪热股TOP100', 'hot_rank_sina', 'tools/fetch_hot_rank_sina.py', '', '17:16', 14),
    ('融合榜单(当天)', 'hot_fused', 'tools/merge_hot_rank.py', '--top 100', '17:20', 15),
    ('融合榜单(3天)', 'hot_fused_3', 'tools/merge_hot_rank.py', '--top 100 --days 3', '17:22', 16),
    ('融合榜单(5天)', 'hot_fused_5', 'tools/merge_hot_rank.py', '--top 100 --days 5', '17:24', 17),
    ('龙虎榜列表', 'alist_daily', 'tools/run_single_table.py', 'st_a_list_daily', '17:40', 20),
    ('龙虎榜明细', 'alist_info', 'tools/run_single_table.py', 'st_a_list_info', '17:45', 21),
    ('个股资金流向(全量)', 'capital_flow', 'tools/fetch_sm_stock_capital_flow_daily.py', '', '17:30', 30),
    ('概念资金流向', 'concept_flow', 'tools/run_single_table.py', 'sm_concept_capital_flow_east', '19:30', 54),
]


def main():
    mysql_url = os.environ.get("MYSQL_URL") or DEFAULT_MYSQL_URL
    engine = create_engine(mysql_url, pool_pre_ping=True)

    print("=" * 60)
    print("检查定时任务配置")
    print("=" * 60)

    # 获取数据库中现有的任务
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT task_type, task_name, cron_time, sort_order
            FROM st_scheduled_tasks
            ORDER BY sort_order
        """))
        existing_tasks = {row[0]: row for row in result}

    print(f"\n数据库中现有任务数量: {len(existing_tasks)}")

    # 检查每个必需的任务
    missing_tasks = []
    for task_name, task_type, script_path, script_args, cron_time, sort_order in REQUIRED_TASKS:
        if task_type in existing_tasks:
            row = existing_tasks[task_type]
            print(f"✓ {task_name} ({task_type}) - 执行时间: {row[2]}")
        else:
            print(f"✗ {task_name} ({task_type}) - 缺失!")
            missing_tasks.append((task_name, task_type, script_path, script_args, cron_time, sort_order))

    # 添加缺失的任务
    if missing_tasks:
        print(f"\n需要添加 {len(missing_tasks)} 个任务:")
        with engine.connect() as conn:
            for task_name, task_type, script_path, script_args, cron_time, sort_order in missing_tasks:
                conn.execute(text("""
                    INSERT INTO st_scheduled_tasks
                        (task_name, task_type, script_path, script_args, cron_time, date_param, date_param_desc, sort_order, etl_sync_at)
                    VALUES
                        (:name, :type, :path, :args, :cron, '', '空=当天, 或 YYYY-MM-DD', :order, NOW())
                """), {
                    "name": task_name,
                    "type": task_type,
                    "path": script_path,
                    "args": script_args,
                    "cron": cron_time,
                    "order": sort_order
                })
                print(f"  + 已添加: {task_name}")
            conn.commit()
        print("\n✓ 所有缺失任务已添加完成")
    else:
        print("\n✓ 所有必需任务都已存在")

    # 显示完整的热门数据任务列表
    print("\n" + "=" * 60)
    print("热门数据定时任务列表")
    print("=" * 60)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT task_name, task_type, cron_time, sort_order
            FROM st_scheduled_tasks
            WHERE sort_order BETWEEN 10 AND 59
            ORDER BY sort_order
        """))
        for row in result:
            print(f"  {row[3]:>2}. [{row[2]}] {row[0]} ({row[1]})")


if __name__ == "__main__":
    main()
