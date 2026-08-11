# 逐日热股+概念+人气+融合（快循环版）
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


def _run_step(args: list[str], date_str: str, label: str) -> tuple[int, str]:
    timeout = child_process_timeout(30 * 60, env_name="PROBIGA_PULL_LOOP_STEP_TIMEOUT")
    try:
        proc = subprocess.run(
            [py, *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=ROOT,
            env=build_child_env(ROOT),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return 124, (str(exc.stdout or "") + str(exc.stderr or "")).strip()[-600:]
    return int(proc.returncode), _output_tail(proc)


def main() -> int:
    s = datetime(2024, 1, 1)
    e = datetime.now()
    total = int((e - s).days) + 1
    d = s
    day = 0
    failures: list[tuple[str, str, int, str]] = []
    steps = [
        ("ths_rank", ["tools/fetch_hot_rank_ths.py"]),
        ("ths_concept", ["tools/fetch_hot_concept_ths_daily.py"]),
        ("east_pop", ["tools/fetch_hot_pop_rank_east.py"]),
        ("merge_1d", ["tools/merge_hot_rank.py", "--top", "100"]),
        ("merge_3d", ["tools/merge_hot_rank.py", "--top", "100", "--days", "3"]),
        ("merge_5d", ["tools/merge_hot_rank.py", "--top", "100", "--days", "5"]),
    ]
    while d <= e:
        date_str = d.strftime("%Y-%m-%d")
        day += 1
        if day % 100 == 0:
            print(f"  进度: {day}/{total} ({date_str})", flush=True)
        for label, base_args in steps:
            rc, output = _run_step([base_args[0], date_str, *base_args[1:]], date_str, label)
            if rc != 0:
                failures.append((date_str, label, rc, output))
        d += timedelta(days=1)
    print(f"✅ 逐日热门完成: {total} 天", flush=True)
    if failures:
        print(f"[WARN] failed steps: {len(failures)}", flush=True)
        for date_str, label, rc, output in failures[:30]:
            print(f"  - {date_str} {label}: rc={rc} {output}", flush=True)
        if len(failures) > 30:
            print(f"  ... +{len(failures) - 30} more", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
