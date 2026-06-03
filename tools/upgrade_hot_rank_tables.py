#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from sqlalchemy import create_engine, text

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
mysql_url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)

engine = create_engine(mysql_url, pool_pre_ping=True)

ALTERS = [
    "ALTER TABLE `st_hot_rank_fused` ADD COLUMN `xq_rank` INT DEFAULT NULL COMMENT '雪球热股排名' AFTER `ths_rank`",
    "ALTER TABLE `st_hot_rank_fused` ADD COLUMN `xq_score` DECIMAL(10,4) DEFAULT 0 COMMENT '雪球得分' AFTER `ths_score`",
    "ALTER TABLE `st_hot_rank_multi_day` ADD COLUMN `avg_xq_rank` DECIMAL(10,2) DEFAULT NULL COMMENT '雪球平均排名' AFTER `avg_ths_rank`",
    "ALTER TABLE `st_hot_rank_multi_day` ADD COLUMN `last_xq_rank` INT DEFAULT NULL COMMENT '最后一天雪球排名' AFTER `last_ths_rank`",
]

with engine.begin() as conn:
    for sql in ALTERS:
        try:
            conn.execute(text(sql))
            print(f"  [OK] {sql}")
        except Exception as e:
            err = str(e)
            if "Duplicate column" in err or "duplicate column" in err:
                print(f"  [SKIP] 列已存在: {sql}")
            else:
                print(f"  [ERR] {err}")

print("\nDone!")
