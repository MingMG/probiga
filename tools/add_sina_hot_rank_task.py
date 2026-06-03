#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""添加新浪热股定时任务到数据库"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"


def main():
    mysql_url = os.environ.get("MYSQL_URL") or DEFAULT_MYSQL_URL
    engine = create_engine(mysql_url, pool_pre_ping=True)

    # 检查是否已存在新浪热股任务
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM st_scheduled_tasks WHERE task_type = 'hot_rank_sina'"))
        count = result.scalar()
        print(f"当前新浪热股任务数量: {count}")

        if count == 0:
            # 插入新浪热股任务
            conn.execute(text("""
                INSERT INTO st_scheduled_tasks
                    (task_name, task_type, script_path, script_args, cron_time, date_param, date_param_desc, sort_order, etl_sync_at)
                VALUES
                    ('新浪热股TOP100', 'hot_rank_sina', 'tools/fetch_hot_rank_sina.py', '', '17:16', '', '空=当天, 或 YYYY-MM-DD', 14, NOW())
            """))
            conn.commit()
            print("✓ 已成功添加新浪热股定时任务")
        else:
            print("✓ 新浪热股定时任务已存在，无需重复添加")

    # 显示当前所有热门数据任务
    print("\n当前热门数据定时任务:")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT task_name, task_type, cron_time, sort_order
            FROM st_scheduled_tasks
            WHERE sort_order BETWEEN 10 AND 19
            ORDER BY sort_order
        """))
        for row in result:
            print(f"  {row[3]:>2}. [{row[2]}] {row[0]} ({row[1]})")


if __name__ == "__main__":
    main()
