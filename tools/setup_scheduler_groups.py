#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Initialize scheduler task groups and a small set of supplemental tasks."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.scheduler_tasks import ensure_scheduler_columns, table_columns, upsert_scheduler_task

GROUP_RULES = [
    ("复盘数据", ["热股榜", "人气榜", "热股", "融合", "强势股", "fetch_hot_rank", "fetch_hot_pop", "merge_hot_rank"]),
    ("概念行业", ["概念", "行业", "concept", "industry"]),
    ("资金流向", ["资金", "capital", "flow"]),
    ("龙虎榜", ["龙虎榜", "a_list", "alist"]),
    ("系统管理", ["同步", "sync", "system", "backup"]),
]


def ensure_group_column(engine) -> None:
    before = table_columns(engine)
    ensure_scheduler_columns(engine)
    if "group_name" in before:
        print("  [SKIP] group_name 列已存在")
    else:
        print("  [OK] 添加 group_name 列")


def get_all_tasks(engine) -> list[dict[str, object]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, task_name, script_path
                FROM st_scheduled_tasks
                ORDER BY sort_order
                """
            )
        ).fetchall()
    return [dict(row._mapping) for row in rows]


def classify_task(task_name: str | None, script_path: str | None) -> str:
    combined = f"{task_name or ''} {script_path or ''}"
    for group_name, keywords in GROUP_RULES:
        if any(keyword in combined for keyword in keywords):
            return group_name
    return "其他"


def assign_groups(engine) -> None:
    for task in get_all_tasks(engine):
        group = classify_task(task.get("task_name"), task.get("script_path"))
        upsert_scheduler_task(
            engine,
            {"group_name": group},
            lookup_where="id = :id",
            lookup_params={"id": task["id"]},
            allowed_columns={"group_name"},
        )
        print(f"  [{group}] {task['task_name']}")


def get_max_sort(engine) -> int:
    with engine.connect() as conn:
        value = conn.execute(text("SELECT COALESCE(MAX(sort_order), 0) FROM st_scheduled_tasks")).scalar()
    return int(value or 0)


def upsert_xq_task(engine) -> None:
    payload = {
        "task_name": "雪球热股榜TOP100",
        "task_type": "hot_rank_xq",
        "group_name": "复盘数据",
        "script_path": "tools/fetch_hot_rank_xq.py",
        "script_args": "",
        "cron_time": "17:10",
        "enabled": 1,
        "sort_order": get_max_sort(engine) + 1,
        "date_param": "",
    }
    result = upsert_scheduler_task(
        engine,
        payload,
        lookup_where="script_path LIKE :script_path",
        lookup_params={"script_path": "%fetch_hot_rank_xq%"},
        update_exclude={"script_path", "sort_order"},
    )
    print(f"  [OK] 雪球热股榜任务已{result['action']}: id={result['id']}")


def upsert_concept_task(engine) -> None:
    payload = {
        "task_name": "同花顺概念成分股同步",
        "task_type": "concept_constituent_ths",
        "group_name": "概念行业",
        "script_path": "tools/sync_concept_ths.py",
        "script_args": "",
        "cron_time": "06:00",
        "enabled": 1,
        "sort_order": get_max_sort(engine) + 1,
        "date_param": "",
    }
    result = upsert_scheduler_task(
        engine,
        payload,
        lookup_where="script_path LIKE :script_path",
        lookup_params={"script_path": "%sync_concept_ths%"},
        update_exclude={"script_path", "sort_order"},
    )
    print(f"  [OK] 同花顺概念成分股同步任务已{result['action']}: id={result['id']}")


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())

    print("=" * 60)
    print("  调度任务分组 + 新任务初始化")
    print("=" * 60)

    print("\n[1/4] 检查 group_name 列...")
    ensure_group_column(engine)

    print("\n[2/4] 写入雪球热股任务...")
    upsert_xq_task(engine)

    print("\n[3/4] 写入概念成分股同步任务...")
    upsert_concept_task(engine)

    print("\n[4/4] 自动分组...")
    assign_groups(engine)

    print("\nDone!")


if __name__ == "__main__":
    main()
