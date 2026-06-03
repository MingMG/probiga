# 逐日热股+概念+人气+融合（快循环版）
from datetime import datetime, timedelta
from subprocess import run
from sys import executable as py, stdout
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
    run([py, "tools/fetch_hot_rank_ths.py", date_str], capture_output=True)
    run([py, "tools/fetch_hot_concept_ths_daily.py", date_str], capture_output=True)
    run([py, "tools/fetch_hot_pop_rank_east.py", date_str], capture_output=True)
    run([py, "tools/merge_hot_rank.py", date_str, "--top", "100"], capture_output=True)
    run([py, "tools/merge_hot_rank.py", date_str, "--top", "100", "--days", "3"], capture_output=True)
    run([py, "tools/merge_hot_rank.py", date_str, "--top", "100", "--days", "5"], capture_output=True)
    d += timedelta(days=1)
print(f"✅ 逐日热门完成: {total} 天", flush=True)
