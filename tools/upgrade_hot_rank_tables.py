#!/usr/bin/env python3
from env_config import create_tool_engine, resolve_tool_mysql_url
# -*- coding: utf-8 -*-
from sqlalchemy import text

ALTERS = [
    "ALTER TABLE `st_hot_rank_fused` ADD COLUMN `xq_rank` INT DEFAULT NULL COMMENT '雪球热股排名' AFTER `ths_rank`",
    "ALTER TABLE `st_hot_rank_fused` ADD COLUMN `xq_score` DECIMAL(10,4) DEFAULT 0 COMMENT '雪球得分' AFTER `ths_score`",
    "ALTER TABLE `st_hot_rank_multi_day` ADD COLUMN `avg_xq_rank` DECIMAL(10,2) DEFAULT NULL COMMENT '雪球平均排名' AFTER `avg_ths_rank`",
    "ALTER TABLE `st_hot_rank_multi_day` ADD COLUMN `last_xq_rank` INT DEFAULT NULL COMMENT '最后一天雪球排名' AFTER `last_ths_rank`",
]

def main():
    engine = create_tool_engine(resolve_tool_mysql_url())
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


if __name__ == "__main__":
    main()
