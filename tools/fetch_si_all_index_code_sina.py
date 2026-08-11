#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仅用「新浪财经」写入 ``si_all_index_code``（不访问东财 push2）。

适合东财长期 RemoteDisconnected 的环境。列与 stock.info.all_index_code 一致。

用法（仓库根）::

  python tools/fetch_si_all_index_code_sina.py

可选环境变量（与 sync_stock_info 一致）::
  MYSQL_URL
  SI_SKIP_DDL=1              跳过建表
  SI_INDEX_SINA_NODE         默认 hs_s
  SI_INDEX_SINA_PAGE_SLEEP   分页间隔秒
  SI_INDEX_SINA_MAX_PAGES    最大页数保护，默认 500
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.process_env import temporary_env


def main() -> int:
    with temporary_env(
        {
            "SI_SKIP_GLOBAL_TRUNCATE": "1",
            "SI_SYNC_SKIP_ALL_CODE": "1",
            "SI_INDEX_PRIMARY": "sina",
        },
        overwrite=False,
    ):
        return _main()


def _main() -> int:

    from server.common.batch_db import create_batch_engine

    from biz.stock_info.sync_stock_info import (
        _clean_object_df,
        df_to_table,
        fetch_all_index_code_sina,
        run_ddl,
        truncate_only,
        _now,
    )

    eng = create_batch_engine()
    run_ddl(eng)
    ts = _now()
    df = fetch_all_index_code_sina()
    df = _clean_object_df(df)
    df["etl_sync_at"] = ts
    truncate_only(eng, "si_all_index_code")
    df_to_table(eng, df, "si_all_index_code")
    print(f"已写入 si_all_index_code：{len(df)} 条（来源：新浪财经）", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
