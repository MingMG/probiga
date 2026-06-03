#!/usr/bin/env bash
# ============================================================================
# ProBigA 本地数据拉取脚本（精简版 - 只拉看板需要的关键表）
# 在本地 Windows PowerShell 执行:
#   cd E:\My Code\ProBigA
#   .\venv\Scripts\activate
#   $env:MYSQL_URL = "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4"
#   $env:SM_MAX_STOCKS = "200"
#   $env:SM_HTTP_RETRIES = "3"
#   $env:SM_REQUEST_SLEEP = "0.3"
#   python tools/pull_local.py
# ============================================================================
"""
本地数据拉取脚本 - 跳过 K线（5年数据太长）、跳过东财接口（服务器被封），只拉同花顺/新浪数据
"""

import os, sys, time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "adata"))

os.environ.setdefault("SM_MAX_STOCKS", "200")
os.environ.setdefault("SM_HTTP_RETRIES", "3")
os.environ.setdefault("SM_REQUEST_SLEEP", "0.3")

STEPS = [
    ("1/8 指数代码", "python tools/run_single_table.py si_all_index_code"),
    ("2/8 指数成分", "python tools/run_single_table.py si_index_constituent"),
    ("3/8 龙虎榜列表", "python tools/run_single_table.py st_a_list_daily"),
    ("4/8 龙虎榜明细", "python tools/run_single_table.py st_a_list_info"),
    ("5/8 个股行情(200只/批)", "python tools/run_single_table.py sm_stock_current"),
    ("6/8 个股分红", "python tools/run_single_table.py sm_dividend"),
    ("7/8 同花顺概念行情", "python tools/run_single_table.py sm_concept_ths_current"),
    ("8/8 指数行情", "python tools/run_single_table.py sm_index_current"),
]

START = time.time()
for i, (label, cmd) in enumerate(STEPS, 1):
    t0 = time.time()
    print(f"\n{'='*50}\n [{label}] {cmd}\n{'='*50}", flush=True)
    rc = os.system(f"cd {ROOT} && {cmd}")
    elapsed = int(time.time() - t0)
    print(f" [{label}] 完成, 返回码={rc}, 耗时={elapsed}s", flush=True)

print(f"\n\n 全部完成! 总耗时: {int(time.time() - START)}s", flush=True)
print(f"\n 接下来: 热门概念 + 热股 + 融合榜单 + 资金流向 在调度管理里单独跑", flush=True)
