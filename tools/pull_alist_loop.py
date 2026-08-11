# 逐日龙虎榜
from datetime import datetime, timedelta
import subprocess
import sys
from sys import executable as py
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.process_env import build_child_env, child_process_timeout


def _output_tail(proc: subprocess.CompletedProcess, limit: int = 600) -> str:
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    return output[-limit:] if output else ""


def main() -> int:
    s = datetime(2024, 1, 1)
    e = datetime.now()
    total = int((e - s).days) + 1
    d = s
    day = 0
    failures: list[tuple[str, int, str]] = []
    while d <= e:
        date_str = d.strftime("%Y-%m-%d")
        day += 1
        if day % 100 == 0:
            print(f"  进度: {day}/{total} ({date_str})", flush=True)
        child_env = build_child_env(ROOT)
        child_env["SE_A_LIST_DATE"] = date_str
        cmd = [py, "tools/run_single_table.py", "st_a_list_daily"]
        timeout = child_process_timeout(30 * 60, env_name="PROBIGA_PULL_LOOP_STEP_TIMEOUT")
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=ROOT,
                env=child_env,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            failures.append((date_str, 124, (str(exc.stdout or "") + str(exc.stderr or "")).strip()[-600:]))
            d += timedelta(days=1)
            continue
        if proc.returncode != 0:
            failures.append((date_str, int(proc.returncode), _output_tail(proc)))
        d += timedelta(days=1)
    print(f"✅ 龙虎榜完成: {total} 天", flush=True)
    if failures:
        print(f"[WARN] failed dates: {len(failures)}", flush=True)
        for date_str, rc, output in failures[:20]:
            print(f"  - {date_str}: rc={rc} {output}", flush=True)
        if len(failures) > 20:
            print(f"  ... +{len(failures) - 20} more", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
