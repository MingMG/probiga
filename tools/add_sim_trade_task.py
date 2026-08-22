#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""添加模拟交易定时任务到数据库（盘中每1分钟扫描）"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from tools.env_config import create_tool_engine


def main():
    engine = create_tool_engine()

    with engine.connect() as conn:
        # 检查是否已存在
        result = conn.execute(text(
            "SELECT COUNT(*) FROM st_scheduled_tasks WHERE script_path = :p"
        ), {"p": "biz/analysis/sync_sim_trade.py"})
        count = result.scalar()

        if count > 0:
            print("[OK] 模拟交易任务已存在，更新间隔为1分钟")
            conn.execute(text(
                "UPDATE st_scheduled_tasks SET interval_minutes = 1, enabled = 1, task_name = :n WHERE script_path = :p"
            ), {"n": "模拟交易扫描(1min)", "p": "biz/analysis/sync_sim_trade.py"})
        else:
            print("[OK] 添加模拟交易定时任务")
            conn.execute(text("""
                INSERT INTO st_scheduled_tasks
                    (task_name, task_type, script_path, script_args, cron_time, interval_minutes,
                     date_param, date_param_desc, sort_order, enabled, etl_sync_at)
                VALUES
                    ('模拟交易扫描(1min)', 'sim_trade', 'biz/analysis/sync_sim_trade.py',
                     '', '09:31', 1, '', '', 99, 1, NOW())
            """))
        conn.commit()

        # 显示结果
        result = conn.execute(text(
            "SELECT id, task_name, interval_minutes, enabled FROM st_scheduled_tasks WHERE script_path = :p"
        ), {"p": "biz/analysis/sync_sim_trade.py"})
        row = result.fetchone()
        if row:
            print(f"  ID: {row[0]}, 名称: {row[1]}, 间隔: {row[2]}分钟, 启用: {row[3]}")


if __name__ == "__main__":
    main()
