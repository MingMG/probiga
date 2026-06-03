# 逐日资金流向
from datetime import datetime, timedelta
from subprocess import run
from sys import executable as py
import os

s = datetime.now() - timedelta(days=120)
e = datetime.now()
total = int((e - s).days) + 1
d = s
day = 0
while d <= e:
    date_str = d.strftime("%Y-%m-%d")
    day += 1
    if day % 30 == 0:
        print(f"  进度: {day}/{total} ({date_str})", flush=True)
    run([py, "tools/fetch_sm_stock_capital_flow_daily.py", date_str], capture_output=True)
    d += timedelta(days=1)
print(f"✅ 资金流向完成: {total} 天", flush=True)
