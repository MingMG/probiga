#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime

from sqlalchemy import create_engine, text

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
mysql_url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
engine = create_engine(mysql_url, pool_pre_ping=True)

GROUP_RULES = [
    ("复盘数据", ["热股榜", "人气榜", "热股", "融合", "强势股", "fetch_hot_rank", "fetch_hot_pop", "merge_hot_rank"]),
    ("概念行业", ["概念", "行业", "concept", "industry"]),
    ("资金流向", ["资金", "capital", "flow"]),
    ("龙虎榜",   ["龙虎榜", "a_list", "alist"]),
    ("系统管理", ["同步", "sync", "system", "backup"]),
]

def ensure_group_column(engine):
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE `st_scheduled_tasks` ADD COLUMN `group_name` VARCHAR(32) DEFAULT '其他' COMMENT '分组名称' AFTER `task_name`"))
            print("  [OK] 添加 group_name 列")
        except Exception as e:
            if "Duplicate column" in str(e):
                print("  [SKIP] group_name 列已存在")
            else:
                raise

def get_all_tasks(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, task_name, script_path FROM st_scheduled_tasks ORDER BY sort_order"))
        return [dict(row._mapping) for row in result]

def classify_task(task_name, script_path):
    combined = (task_name or "") + " " + (script_path or "")
    for group_name, keywords in GROUP_RULES:
        for kw in keywords:
            if kw in combined:
                return group_name
    return "其他"

def assign_groups(engine):
    tasks = get_all_tasks(engine)
    for t in tasks:
        group = classify_task(t["task_name"], t.get("script_path", ""))
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE st_scheduled_tasks SET group_name = :g WHERE id = :id"),
                {"g": group, "id": t["id"]}
            )
        print(f"  [{group}] {t['task_name']}")

def get_max_sort(engine):
    with engine.connect() as conn:
        r = conn.execute(text("SELECT COALESCE(MAX(sort_order), 0) FROM st_scheduled_tasks"))
        return r.scalar()

def insert_xq_task(engine):
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT COUNT(*) FROM st_scheduled_tasks WHERE script_path LIKE :p"),
            {"p": "%fetch_hot_rank_xq%"}
        ).scalar()
    if exists:
        print("  [SKIP] 雪球热股任务已存在")
        return

    max_sort = get_max_sort(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO st_scheduled_tasks
            (task_name, group_name, script_path, script_args, cron_time, enabled, sort_order, date_param, created_at, updated_at)
            VALUES (:name, :grp, :script, :args, :cron, :enabled, :sort, :dp, NOW(), NOW())
        """), {
            "name": "雪球热股榜TOP100",
            "grp": "复盘数据",
            "script": "tools/fetch_hot_rank_xq.py",
            "args": "",
            "cron": "17:10",
            "enabled": 1,
            "sort": max_sort + 1,
            "dp": "",
        })
    print("  [OK] 插入雪球热股榜定时任务")

def insert_concept_task(engine):
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT COUNT(*) FROM st_scheduled_tasks WHERE script_path LIKE :p"),
            {"p": "%sync_concept_ths%"}
        ).scalar()
    if exists:
        print("  [SKIP] 概念成分股任务已存在")
        return

    max_sort = get_max_sort(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO st_scheduled_tasks
            (task_name, group_name, script_path, script_args, cron_time, enabled, sort_order, date_param, created_at, updated_at)
            VALUES (:name, :grp, :script, :args, :cron, :enabled, :sort, :dp, NOW(), NOW())
        """), {
            "name": "同花顺概念成分股同步",
            "grp": "概念行业",
            "script": "tools/sync_concept_ths.py",
            "args": "",
            "cron": "06:00",
            "enabled": 1,
            "sort": max_sort + 1,
            "dp": "",
        })
    print("  [OK] 插入同花顺概念成分股同步任务（每天06:00）")

print("=" * 60)
print("  调度任务分组 + 新任务初始化")
print("=" * 60)

print("\n[1/4] 添加 group_name 列...")
ensure_group_column(engine)

print("\n[2/4] 插入雪球热股任务...")
insert_xq_task(engine)

print("\n[3/4] 插入概念成分股同步任务...")
insert_concept_task(engine)

print("\n[4/4] 自动分组...")
assign_groups(engine)

print("\nDone!")
