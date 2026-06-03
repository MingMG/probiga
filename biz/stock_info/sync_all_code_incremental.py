# -*- coding: utf-8 -*-
"""
``si_all_code`` 增量同步（按主键 ``stock_code`` 插入或更新），**不执行 TRUNCATE**。

适用：全市场代码表已存在，需要定期合并 **新股、更名、上市日期修正** 等来自 ``stock.info.all_code()`` 的变化。

用法（仓库根目录）::

    python -m biz.stock_info.sync_all_code_incremental

环境变量 ``MYSQL_URL`` 与全量脚本相同；可选复用 ``biz.stock_info.sync_stock_info`` 里的 HTTP 重试逻辑。

说明：
  - **其它表**（股本、概念、指数等）增量策略各不相同；当前仓库仍以 ``sync_stock_info`` 全量为主。
  - 若需 **删除** 已退市且不再出现在 ``all_code()`` 中的代码，可另做「对账删除」任务（本脚本默认不做，避免误删）。
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _fetch_all_code_df():
    try:
        from biz.stock_info.sync_stock_info import retry_remote
    except ImportError:
        retry_remote = lambda fn, *a, **k: fn(*a, **k)  # noqa: E731

    from adata.stock.info import info

    return retry_remote(info.all_code)


def upsert_si_all_code(engine, df: pd.DataFrame) -> int:
    ts = datetime.now().replace(microsecond=0)
    sql = text(
        """
        INSERT INTO si_all_code (stock_code, short_name, exchange, list_date, etl_sync_at)
        VALUES (:stock_code, :short_name, :exchange, :list_date, :etl_sync_at)
        ON DUPLICATE KEY UPDATE
            short_name = VALUES(short_name),
            exchange = VALUES(exchange),
            list_date = VALUES(list_date),
            etl_sync_at = VALUES(etl_sync_at)
        """
    )
    n = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            ld = row.get("list_date")
            if pd.isna(ld) or ld is None:
                ld_sql = None
            else:
                ld_sql = pd.Timestamp(ld).date() if hasattr(ld, "date") else ld
            conn.execute(
                sql,
                {
                    "stock_code": str(row["stock_code"]).zfill(6),
                    "short_name": str(row.get("short_name") or "").strip(),
                    "exchange": None if pd.isna(row.get("exchange")) else str(row["exchange"]),
                    "list_date": ld_sql,
                    "etl_sync_at": ts,
                },
            )
            n += 1
            if n % 1000 == 0:
                logger.info("已 upsert %s 行…", n)
    return n


def main() -> None:
    logger.info("拉取 all_code() …")
    df = _fetch_all_code_df()
    if df is None or df.empty:
        logger.error("未获取到代码表数据。")
        sys.exit(1)
    url = _mysql_url()
    engine = create_engine(url, pool_pre_ping=True, future=True)
    cnt = upsert_si_all_code(engine, df)
    logger.info("完成：si_all_code 增量 upsert 共 %s 行（含新增与更新）。", cnt)


if __name__ == "__main__":
    main()
