#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import sys
from datetime import datetime


def run(script, args=[]):
    cmd = [sys.executable, script] + args
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print('='*60)
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode

today = datetime.now().strftime("%Y-%m-%d")
date = today
if len(sys.argv) > 1:
    date = sys.argv[1]

if date != today:
    print(
        "雪球是当前快照源，legacy 聚合入口拒绝历史日期；未生成融合榜。",
        file=sys.stderr,
    )
    raise SystemExit(2)

skip_concept = "--skip-concept" in sys.argv

print(f"日期: {date}")

print("\n[1/6] 升级数据库表结构...")
ret1 = run("tools/upgrade_hot_rank_tables.py")

print("\n[2/6] 初始化调度任务分组 + 雪球任务...")
ret2 = run("tools/setup_scheduler_groups.py")

print("\n[3/6] 获取雪球热股TOP100...")
ret3 = run("tools/fetch_hot_rank_xq.py", [date])
if ret3 != 0:
    print("雪球精确Top100未通过，停止后续融合，未写 fused。", file=sys.stderr)
    raise SystemExit(1)

print("\n[4/6] 融合三榜（东财+同花顺+雪球）...")
ret4 = run("tools/merge_hot_rank.py", [date])

print("\n[5/6] 融合近3天强势股...")
ret5 = run("tools/merge_hot_rank.py", [date, "--days", "3"])

if skip_concept:
    print("\n[6/6] 同步同花顺概念成分股... 跳过(--skip-concept)")
    ret6 = 0
else:
    print("\n[6/6] 同步同花顺概念成分股（耗时较长，请耐心等待）...")
    ret6 = run("tools/sync_concept_ths.py")

print(f"\n\n结果:")
print(f"  表升级: {'OK' if ret1==0 else 'FAIL'}")
print(f"  调度分组: {'OK' if ret2==0 else 'FAIL'}")
print(f"  雪球TOP100: {'OK' if ret3==0 else 'FAIL'}")
print(f"  单日融合: {'OK' if ret4==0 else 'FAIL'}")
print(f"  近3天融合: {'OK' if ret5==0 else 'FAIL'}")
print(f"  概念成分股: {'OK' if ret6==0 else 'FAIL'}")
