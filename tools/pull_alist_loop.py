# 逐日龙虎榜
from datetime import datetime, timedelta
from subprocess import run
from sys import executable as py
import os

s = datetime(2024, 1, 1)
e = datetime.now()
total = int((e - s).days) + 1
d = s
day = 0
while d <= e:
    date_str = d.strftime("%Y-%m-%d")
    day += 1
    if day % 100 == 0:
        print(f"  进度: {day}/{total} ({date_str})", flush=True)
    child_env = os.environ.copy()
    child_env["SE_A_LIST_DATE"] = date_str
    run([py, "tools/run_single_table.py", "st_a_list_daily"], capture_output=True, env=child_env)
    d += timedelta(days=1)
print(f"✅ 龙虎榜完成: {total} 天", flush=True)
