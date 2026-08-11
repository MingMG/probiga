#!/usr/bin/env python3
from env_config import resolve_tool_mysql_url
"""
ProBigA 历史数据拉取（2024-01-01 起，不含K线）
用法：python tools/pull_all.py
"""
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)
PY = sys.executable
FAILURES: list[tuple[list[str], int, str]] = []
BASE_ENV: dict[str, str] = {}

from server.common.process_env import build_child_env, child_process_timeout


def run(cmd, capture=False, extra_env: dict[str, str] | None = None) -> int:
    """执行命令，忽略错误继续"""
    try:
        child_env = build_child_env(ROOT)
        child_env.update(BASE_ENV)
        if extra_env:
            child_env.update(extra_env)
        timeout = child_process_timeout(30 * 60, env_name="PROBIGA_PULL_ALL_STEP_TIMEOUT")
        result = subprocess.run(cmd, capture_output=capture, cwd=ROOT, env=child_env, timeout=timeout)
        if result.returncode != 0:
            print(f"[WARN] command failed ({result.returncode}): {' '.join(map(str, cmd))}", flush=True)
            output = ""
            if capture:
                stdout = result.stdout or b""
                stderr = result.stderr or b""
                if isinstance(stdout, bytes):
                    stdout = stdout.decode("utf-8", errors="replace")
                if isinstance(stderr, bytes):
                    stderr = stderr.decode("utf-8", errors="replace")
                output = (str(stdout) + str(stderr)).strip()[-600:]
            FAILURES.append((list(map(str, cmd)), int(result.returncode), output))
        return int(result.returncode)
    except subprocess.TimeoutExpired as exc:
        timeout = child_process_timeout(30 * 60, env_name="PROBIGA_PULL_ALL_STEP_TIMEOUT")
        print(f"[WARN] command timeout after {timeout}s: {' '.join(map(str, cmd))}", flush=True)
        output = ""
        if capture:
            stdout = exc.stdout or b""
            stderr = exc.stderr or b""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            output = (str(stdout) + str(stderr)).strip()[-600:]
        FAILURES.append((list(map(str, cmd)), 124, output))
        return 124
    except Exception as exc:
        print(f"[WARN] command error: {' '.join(map(str, cmd))}: {exc}", flush=True)
        FAILURES.append((list(map(str, cmd)), -1, str(exc)))
        return -1


def main() -> int:
    # 设环境变量
    BASE_ENV.clear()
    BASE_ENV.update(
        {
            "MYSQL_URL": resolve_tool_mysql_url(),
            "SM_MAX_STOCKS": "200",
            "SM_HTTP_RETRIES": "3",
            "SM_REQUEST_SLEEP": "0.5",
            "SE_SKIP_GLOBAL_TRUNCATE": "1",
        }
    )

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
    print("\n[2/6] 逐日热门数据（2024-01-01 ~ 今天）...")
    start = datetime(2024, 1, 1)
    end = datetime.now()
    total = (end - start).days + 1
    d = start
    day = 0
    while d <= end:
        date_str = d.strftime("%Y-%m-%d")
        day += 1
        if day % 100 == 0 or day <= 3:
            print(f"  进度 {day}/{total}  ({date_str})", flush=True)
        run([PY, "tools/fetch_hot_rank_ths.py", date_str], capture=True)
        run([PY, "tools/fetch_hot_concept_ths_daily.py", date_str], capture=True)
        run([PY, "tools/fetch_hot_pop_rank_east.py", date_str], capture=True)
        run([PY, "tools/merge_hot_rank.py", date_str, "--top", "100"], capture=True)
        run([PY, "tools/merge_hot_rank.py", date_str, "--top", "100", "--days", "3"], capture=True)
        run([PY, "tools/merge_hot_rank.py", date_str, "--top", "100", "--days", "5"], capture=True)
        d += timedelta(days=1)
    print("[2/6] ✅")

    # ==== 3/6 逐日龙虎榜 ====
    print("\n[3/6] 逐日龙虎榜（2024-01-01 ~ 今天）...")
    d = start
    day = 0
    while d <= end:
        date_str = d.strftime("%Y-%m-%d")
        day += 1
        if day % 100 == 0 or day <= 3:
            print(f"  进度 {day}/{total}  ({date_str})", flush=True)
        run([PY, "tools/run_single_table.py", "st_a_list_daily"], capture=True, extra_env={"SE_A_LIST_DATE": date_str})
        d += timedelta(days=1)
    print("[3/6] ✅")

    # 龙虎榜明细（最近30天）
    print("\n  龙虎榜明细（最近30天）...")
    d = end - timedelta(days=30)
    while d <= end:
        date_str = d.strftime("%Y-%m-%d")
        run([PY, "tools/run_single_table.py", "st_a_list_info"], capture=True, extra_env={"SE_A_LIST_DATE": date_str})
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
    run([PY, "tools/merge_hot_rank.py", today, "--top", "100"])
    run([PY, "tools/merge_hot_rank.py", today, "--top", "100", "--days", "3"])
    run([PY, "tools/merge_hot_rank.py", today, "--top", "100", "--days", "5"])
    run([PY, "tools/run_single_table.py", "sm_stock_current"])
    run([PY, "tools/run_single_table.py", "sm_concept_ths_current"])
    run([PY, "tools/run_single_table.py", "sm_index_current"])
    print("[6/6] ✅")

    print("\n" + "=" * 50)
    print("🎉 全部完成！")
    print("=" * 50)
    print("\n查看数据量命令：")
    print('python -c "from tools.env_config import create_tool_engine; e=create_tool_engine(); print(e.url)"')
    if FAILURES:
        print(f"\n[WARN] failed commands: {len(FAILURES)}", flush=True)
        for cmd, rc, output in FAILURES[:30]:
            print(f"  - rc={rc} {' '.join(cmd)}", flush=True)
            if output:
                print(f"    {output}", flush=True)
        if len(FAILURES) > 30:
            print(f"  ... +{len(FAILURES) - 30} more", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
