#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Check and repair required scheduler task definitions."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.scheduler_tasks import upsert_scheduler_task

DATE_PARAM_DESC = "空=当天，或 YYYY-MM-DD"

# (task_name, task_type, script_path, script_args, cron_time, sort_order)
REQUIRED_TASKS = [
    ("股票代码列表", "all_code", "tools/run_single_table.py", "si_all_code", "05:00", 1),
    ("指数代码列表", "all_index_code", "tools/run_single_table.py", "si_all_index_code", "05:02", 2),
    ("指数成分股", "index_constituent", "tools/run_single_table.py", "si_index_constituent", "05:05", 3),
    ("概念目录(QMT)", "concept_code_east", "tools/run_single_table.py", "si_concept_code_east", "05:10", 4),
    (
        "概念成分股(QMT)",
        "concept_constituent_east",
        "tools/run_single_table.py",
        "si_concept_constituent_east",
        "05:15",
        5,
    ),
    (
        "个股行业/概念/板块归属(QMT)",
        "stock_relations_qmt",
        "tools/run_single_table.py",
        "si_stock_plate_east",
        "05:20",
        6,
    ),
    ("东方财富板块热度", "sector_heat_east", "tools/fetch_sector_heat_east_daily.py", "", "17:08", 10),
    ("同花顺热门概念", "hot_concept", "tools/fetch_hot_concept_ths_daily.py", "", "17:10", 11),
    ("同花顺热股TOP100", "hot_rank_ths", "tools/fetch_hot_rank_ths.py", "", "17:12", 12),
    ("东方财富人气榜TOP100", "hot_pop_east", "tools/fetch_hot_pop_rank_east.py", "", "17:14", 13),
    ("新浪热股TOP100", "hot_rank_sina", "tools/fetch_hot_rank_sina.py", "", "17:16", 14),
    ("融合榜单(当天)", "hot_fused", "tools/merge_hot_rank.py", "--top 100", "17:20", 15),
    ("融合榜单(3天)", "hot_fused_3", "tools/merge_hot_rank.py", "--top 100 --days 3", "17:22", 16),
    ("融合榜单(5天)", "hot_fused_5", "tools/merge_hot_rank.py", "--top 100 --days 5", "17:24", 17),
    ("龙虎榜列表", "alist_daily", "tools/run_single_table.py", "st_a_list_daily", "17:40", 20),
    ("龙虎榜明细", "alist_info", "tools/run_single_table.py", "st_a_list_info", "17:45", 21),
    (
        "个股资金流向(全量)",
        "capital_flow",
        "tools/crawl_realtime_batch.py",
        "--only flow --min-coverage 0.70 --json",
        "17:30",
        30,
    ),
    ("概念资金流向", "concept_flow", "tools/fetch_concept_flow_datacenter.py", "", "19:30", 54),
    ("A股早报推送", "news_daily", "biz/early_briefing/generate.py", "", "08:30", 89),
    ("A股晚报推送", "evening_review", "biz/evening_review/generate.py", "", "20:00", 90),
]


def _task_payload(task: tuple[str, str, str, str, str, int]) -> dict[str, object]:
    task_name, task_type, script_path, script_args, cron_time, sort_order = task
    return {
        "task_name": task_name,
        "task_type": task_type,
        "script_path": script_path,
        "script_args": script_args,
        "cron_time": cron_time,
        "enabled": 1,
        "date_param": "",
        "date_param_desc": DATE_PARAM_DESC,
        "sort_order": sort_order,
    }


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())

    print("=" * 60)
    print("检查定时任务配置")
    print("=" * 60)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, task_type, task_name, script_path, script_args, cron_time, sort_order, enabled
                FROM st_scheduled_tasks
                ORDER BY sort_order
                """
            )
        ).fetchall()
    existing_tasks = {row[1]: row for row in rows}
    print(f"\n数据库中现有任务数量: {len(existing_tasks)}")

    missing_tasks: list[tuple[str, str, str, str, str, int]] = []
    repair_tasks: list[tuple[str, str, str, str, str, int]] = []
    for task in REQUIRED_TASKS:
        task_name, task_type, script_path, script_args, cron_time, sort_order = task
        row = existing_tasks.get(task_type)
        if row is None:
            print(f"! {task_name} ({task_type}) - 缺失")
            missing_tasks.append(task)
            continue

        needs_repair = (
            row[2] != task_name
            or row[3] != script_path
            or (row[4] or "") != script_args
            or row[5] != cron_time
            or row[6] != sort_order
            or int(row[7] or 0) != 1
        )
        if needs_repair:
            print(f"~ {task_name} ({task_type}) - 需要修复，当前时间 {row[5]}")
            repair_tasks.append(task)
        else:
            print(f"+ {task_name} ({task_type}) - 执行时间: {row[5]}")

    if repair_tasks or missing_tasks:
        print(f"\n需要修复 {len(repair_tasks)} 个任务，添加 {len(missing_tasks)} 个任务")
        for task in repair_tasks + missing_tasks:
            payload = _task_payload(task)
            result = upsert_scheduler_task(
                engine,
                payload,
                lookup_where="task_type = :task_type",
                lookup_params={"task_type": payload["task_type"]},
                update_exclude={"task_type"},
            )
            marker = "~" if result["action"] == "updated" else "+"
            print(f"  {marker} {payload['task_name']} ({result['action']}, id={result['id']})")
        print("\n定时任务已修复完成")
    else:
        print("\n所有必需任务都已存在")

    print("\n" + "=" * 60)
    print("热门数据定时任务列表")
    print("=" * 60)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT task_name, task_type, cron_time, sort_order
                FROM st_scheduled_tasks
                WHERE sort_order BETWEEN 10 AND 59
                ORDER BY sort_order
                """
            )
        ).fetchall()
    for row in rows:
        print(f"  {row[3]:>2}. [{row[2]}] {row[0]} ({row[1]})")


if __name__ == "__main__":
    main()
