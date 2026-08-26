#!/usr/bin/env python3
"""
ProBigA 历史数据拉取（2024-01-01 起，不含K线）
用法：python tools/pull_all.py
"""
import os, sys, subprocess
from datetime import datetime, timedelta

# 设环境变量
from tools.env_config import resolve_tool_mysql_url

resolve_tool_mysql_url()
os.environ["SM_MAX_STOCKS"] = "200"
os.environ["SM_HTTP_RETRIES"] = "3"
os.environ["SM_REQUEST_SLEEP"] = "0.5"
os.environ["SE_SKIP_GLOBAL_TRUNCATE"] = "1"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

def run(cmd, capture=False):
    """执行命令，忽略错误继续"""
    try:
        subprocess.run(cmd, capture_output=capture, cwd=ROOT, timeout=None)
    except:
        pass

today = datetime.now().strftime("%Y-%m-%d")

print("=" * 50)
print("ProBigA 历史数据拉取（2024-01-01 起）")
print("=" * 50)

# ==== 1/6 基础数据 ====
print("\n[1/6] 基础数据...")
run([PY, "tools/run_single_table.py", "si_all_index_code"])
run([PY, "tools/run_single_table.py", "si_index_constituent"])
run([PY, "tools/run_single_table.py", "si_concept_constituent_east"])
print("[1/6] ✅")

# ==== 2/6 逐日热门数据 ====
print("\n[2/6] 逐日热门数据历史批量入口已禁用")
print("THS/Sina/XQ 只有当前快照；东财历史榜须按目标日期通过正式脚本单独恢复。")
start = datetime(2024, 1, 1)
end = datetime.now()
total = (end - start).days + 1
print("[2/6] ✅ 安全跳过（未生成融合榜）")

# ==== 3/6 逐日龙虎榜 ====
print("\n[3/6] 逐日龙虎榜（2024-01-01 ~ 今天）...")
d = start
day = 0
while d <= end:
    date_str = d.strftime("%Y-%m-%d")
    day += 1
    if day % 100 == 0 or day <= 3:
        print(f"  进度 {day}/{total}  ({date_str})", flush=True)
    os.environ["SE_A_LIST_DATE"] = date_str
    run([PY, "tools/run_single_table.py", "st_a_list_daily"], capture=True)
    d += timedelta(days=1)
print("[3/6] ✅")

# 龙虎榜明细（最近30天）
print("\n  龙虎榜明细（最近30天）...")
d = end - timedelta(days=30)
while d <= end:
    date_str = d.strftime("%Y-%m-%d")
    os.environ["SE_A_LIST_DATE"] = date_str
    run([PY, "tools/run_single_table.py", "st_a_list_info"], capture=True)
    d += timedelta(days=1)
print("  明细 ✅")

# ==== 4/6 资金流向（120天）====
print("\n[4/6] 资金流向（最近120天）...")
start_flow = end - timedelta(days=120)
d = start_flow
day = 0
flow_total = (end - start_flow).days + 1
while d <= end:
    date_str = d.strftime("%Y-%m-%d")
    day += 1
    if day % 30 == 0 or day <= 3:
        print(f"  进度 {day}/{flow_total}  ({date_str})", flush=True)
    run([PY, "tools/fetch_sm_stock_capital_flow_daily.py", date_str], capture=True)
    d += timedelta(days=1)
print("[4/6] ✅")

# ==== 5/6 分红 ====
print("\n[5/6] 个股分红...")
run([PY, "tools/run_single_table.py", "sm_dividend"])
print("[5/6] ✅")

# ==== 6/6 当天快照 ====
print("\n[6/6] 当天行情快照...")
run([PY, "tools/fetch_sector_heat_east_daily.py", today])
run([PY, "tools/fetch_hot_rank_ths.py", today])
run([PY, "tools/fetch_hot_concept_ths_daily.py", today])
run([PY, "tools/fetch_hot_pop_rank_east.py", today])
# Legacy bulk recovery deliberately never publishes fused data.  The formal
# multi-source scheduler owns all fused output.
run([PY, "tools/run_single_table.py", "sm_stock_current"])
run([PY, "tools/run_single_table.py", "sm_concept_ths_current"])
run([PY, "tools/run_single_table.py", "sm_index_current"])
print("[6/6] ✅")

print("\n" + "=" * 50)
print("🎉 全部完成！")
print("=" * 50)
print("\n查看数据量命令：")
print('python -c "from sqlalchemy import create_engine,text; e=create_engine()"')
