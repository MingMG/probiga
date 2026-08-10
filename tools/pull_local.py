#!/usr/bin/env python3
# ============================================================================
# ProBigA 本地数据拉取脚本（精简版 - 只拉看板需要的关键表）
# 在本地 Windows PowerShell 执行:
#   cd <repo-root>
#   .\.venv\Scripts\Activate.ps1
#   Set MYSQL_URL to your target MySQL connection string before running.
#   $env:SM_MAX_STOCKS = "200"
#   $env:SM_HTTP_RETRIES = "3"
#   $env:SM_REQUEST_SLEEP = "0.3"
#   python tools/pull_local.py
# ============================================================================
"""
本地数据拉取脚本 - 跳过 K线（5年数据太长）、跳过东财接口（服务器被封），只拉同花顺/新浪数据
"""

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.process_env import build_child_env, child_process_timeout

STEPS = [
    ("1/8 指数代码", "si_all_index_code"),
    ("2/8 指数成分", "si_index_constituent"),
    ("3/8 龙虎榜列表", "st_a_list_daily"),
    ("4/8 龙虎榜明细", "st_a_list_info"),
    ("5/8 个股行情(200只/批)", "sm_stock_current"),
    ("6/8 个股分红", "sm_dividend"),
    ("7/8 同花顺概念行情", "sm_concept_ths_current"),
    ("8/8 指数行情", "sm_index_current"),
]


def main() -> int:
    base_env = {
        "SM_MAX_STOCKS": "200",
        "SM_HTTP_RETRIES": "3",
        "SM_REQUEST_SLEEP": "0.3",
    }

    start = time.time()
    script = ROOT / "tools" / "run_single_table.py"
    failures: list[tuple[str, int]] = []

    for i, (label, table_name) in enumerate(STEPS, 1):
        t0 = time.time()
        cmd = [sys.executable, str(script), table_name]
        print(f"\n{'='*50}\n [{label}] {cmd}\n{'='*50}", flush=True)
        child_env = build_child_env(ROOT)
        for key, value in base_env.items():
            child_env.setdefault(key, value)
        timeout = child_process_timeout(30 * 60, env_name="PROBIGA_PULL_LOCAL_STEP_TIMEOUT")
        try:
            rc = subprocess.run(cmd, cwd=ROOT, env=child_env, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            print(f" [{label}] TIMEOUT after {timeout}s", flush=True)
            rc = 124
        elapsed = int(time.time() - t0)
        print(f" [{label}] 完成, 返回码={rc}, 耗时={elapsed}s", flush=True)
        if rc != 0:
            failures.append((label, rc))

    print(f"\n\n 全部完成! 总耗时: {int(time.time() - start)}s", flush=True)
    print(f"\n 接下来: 热门概念 + 热股 + 融合榜单 + 资金流向 在调度管理里单独跑", flush=True)
    if failures:
        print("\n[WARN] failed local pull steps:", flush=True)
        for label, rc in failures:
            print(f"  - {label}: rc={rc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
