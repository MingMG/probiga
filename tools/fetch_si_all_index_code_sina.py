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

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    os.environ.setdefault("SI_SKIP_GLOBAL_TRUNCATE", "1")
    os.environ.setdefault("SI_SYNC_SKIP_ALL_CODE", "1")
    os.environ.setdefault("SI_INDEX_PRIMARY", "sina")

    from biz.stock_info.sync_stock_info import (
        PartialSnapshotPublished,
        sync_all_index_code,
    )
    from server.common.batch_db import create_batch_engine
    from server.common.auxiliary_runtime_schema import (
        validate_si_all_index_code_runtime_schema,
    )

    eng = create_batch_engine()
    validate_si_all_index_code_runtime_schema(eng)
    try:
        df = sync_all_index_code(eng, object())
    except PartialSnapshotPublished as exc:
        print(str(exc), file=sys.stderr, flush=True)
        return exc.exit_code
    print(
        f"status=complete exit_code=0 si_all_index_code={len(df)} source=新浪财经",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
