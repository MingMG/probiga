#!/usr/bin/env python3
"""等待东财API解封后，自动同步落后的资金流向数据。"""
import subprocess
import sys
import time
import requests

MAX_WAIT = 3600
CHECK_INTERVAL = 60

def is_east_available():
    try:
        url = 'https://push2.eastmoney.com/api/qt/stock/fflow/daykline/get'
        params = {'secid': '0.000001', 'fields1': 'f1', 'lmt': '0', 'klt': '101'}
        r = requests.get(url, params=params, timeout=10, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        return r.status_code == 200 and len(r.text) > 100
    except Exception:
        return False

def main():
    start = time.time()
    print("等待东财API解封...", flush=True)
    
    while time.time() - start < MAX_WAIT:
        if is_east_available():
            print("东财API已恢复!", flush=True)
            break
        elapsed = int(time.time() - start)
        print(f"  仍被封禁... ({elapsed}s)", flush=True)
        time.sleep(CHECK_INTERVAL)
    else:
        print("等待超时，东财API仍未恢复", flush=True)
        return 1
    
    from tools.env_config import create_tool_engine
    engine = create_tool_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute('SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily')
    latest = cur.fetchone()[0]
    cur.close()
    conn.close()
    engine.dispose()
    
    import datetime
    today = datetime.date.today()
    dates = []
    d = latest + datetime.timedelta(days=1)
    while d <= today:
        if d.weekday() < 5:
            dates.append(d.strftime('%Y-%m-%d'))
        d += datetime.timedelta(days=1)
    
    if not dates:
        print("资金流向数据已是最新", flush=True)
        return 0
    
    print(f"需要同步的日期: {dates}", flush=True)
    
    for date in dates:
        print(f"\n=== 同步 {date} ===", flush=True)
        env = {
            'SM_MAX_WORKERS': '1',
            'SM_REQUEST_SLEEP': '0.5',
            'SM_HTTP_RETRIES': '8',
            'SM_HTTP_BACKOFF': '5',
        }
        import os
        e = os.environ.copy()
        e.update(env)
        
        cmd = [
            sys.executable, '-m', 'biz.stock_market.sync_stock_market',
            '--only', 'stock_flow_daily',
            f'--flow-date={date}',
            '--limit', '-1',
        ]
        
        rc = subprocess.call(cmd, cwd='/opt/ProBigA', env=e)
        if rc != 0:
            print(f"  {date} 失败 (rc={rc})", flush=True)
            time.sleep(30)
        else:
            print(f"  {date} 完成", flush=True)
        
        time.sleep(10)
    
    print("\n所有资金流向数据同步完成!", flush=True)
    return 0

if __name__ == '__main__':
    sys.exit(main())
