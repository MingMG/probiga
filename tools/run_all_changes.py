#!/usr/bin/env python3
from env_config import resolve_tool_mysql_url
# -*- coding: utf-8 -*-
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.process_env import build_child_env, child_process_timeout

BASE_ENV: dict[str, str] = {}


def run(script, args=None) -> int:
    if args is None:
        args = []
    cmd = [sys.executable, script] + args
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print('='*60)
    child_env = build_child_env(ROOT)
    child_env.update(BASE_ENV)
    timeout = child_process_timeout(45 * 60, env_name="PROBIGA_RUN_ALL_CHANGES_STEP_TIMEOUT")
    try:
        result = subprocess.run(cmd, capture_output=False, cwd=ROOT, env=child_env, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT] {' '.join(cmd)} exceeded {timeout}s", file=sys.stderr, flush=True)
        return 124
    return result.returncode


def main() -> int:
    BASE_ENV.clear()
    BASE_ENV["MYSQL_URL"] = resolve_tool_mysql_url()

    date = datetime.now().strftime("%Y-%m-%d")
    if len(sys.argv) > 1:
        date = sys.argv[1]

    skip_concept = "--skip-concept" in sys.argv

    print(f"日期: {date}")

    print("\n[1/6] 升级数据库表结构...")
    ret1 = run("tools/upgrade_hot_rank_tables.py")

    print("\n[2/6] 初始化调度任务分组 + 雪球任务...")
    ret2 = run("tools/setup_scheduler_groups.py")

    print("\n[3/6] 获取雪球热股TOP100...")
    ret3 = run("tools/fetch_hot_rank_xq.py", [date])

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
    return 0 if all(ret == 0 for ret in (ret1, ret2, ret3, ret4, ret5, ret6)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
