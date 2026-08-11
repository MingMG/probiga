#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为 sm_stock_kline 增加 short_name（股票简称），并从 si_all_code 回填已有行。

可重复执行：已有列则跳过 ALTER，仍会执行一次 UPDATE 同步简称。

用法（在项目根或任意目录）::
  python tools/add_sm_stock_kline_short_name.py

连接串与 sync_stock_market 一致，默认::
  Set MYSQL_URL to your target MySQL connection string before running.
"""
from __future__ import annotations

import sys
from env_config import create_tool_engine, resolve_tool_mysql_url


def main() -> None:
    try:
        from sqlalchemy import text
    except ImportError as e:
        print("请先安装: pip install sqlalchemy pymysql", file=sys.stderr)
        raise SystemExit(1) from e

    url = resolve_tool_mysql_url()
    engine = create_tool_engine(url)

    def col_exists(conn) -> bool:
        r = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'sm_stock_kline'
                  AND column_name = 'short_name'
                """
            )
        ).scalar()
        return int(r or 0) > 0

    with engine.connect() as conn:
        db = conn.execute(text("SELECT DATABASE()")).scalar()
        print(f"当前库: {db}")

    with engine.begin() as conn:
        if col_exists(conn):
            print("列 short_name 已存在，跳过 ALTER。")
        else:
            conn.execute(
                text(
                    """
                    ALTER TABLE `sm_stock_kline`
                    ADD COLUMN `short_name` VARCHAR(128) NOT NULL DEFAULT ''
                    COMMENT '股票简称（来自 si_all_code）'
                    AFTER `stock_code`
                    """
                )
            )
            print("已执行 ALTER TABLE，增加列 short_name。")

    with engine.begin() as conn:
        r = conn.execute(
            text(
                """
                UPDATE `sm_stock_kline` k
                INNER JOIN `si_all_code` s ON k.stock_code = s.stock_code
                SET k.short_name = s.short_name
                """
            )
        )
        # SQLAlchemy 2 rowcount
        n = r.rowcount if r.rowcount is not None else -1
        print(f"已执行 UPDATE 回填 short_name（受影响行数约: {n}，-1 表示驱动未返回）。")

    print("完成。可用: DESCRIBE sm_stock_kline; 或 SHOW COLUMNS FROM sm_stock_kline LIKE 'short_name';")


if __name__ == "__main__":
    main()
