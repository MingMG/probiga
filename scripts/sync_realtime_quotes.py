#!/usr/bin/env python3
"""
盘中实时行情同步脚本
每分钟执行一次，同步全量股票实时行情到数据库
"""
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, '/opt/ProBigA')
os.environ.setdefault('MYSQL_URL', 'mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4')

from biz.stock_market.realtime_quotes import fetch_list_market_current
from sqlalchemy import create_engine, text
import pandas as pd

def get_engine():
    url = os.environ.get('MYSQL_URL')
    return create_engine(url, pool_pre_ping=True, future=True)

def is_trading_time():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    hour, minute = now.hour, now.minute
    current = hour * 100 + minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)

def sync_realtime():
    if not is_trading_time():
        print(f"[{datetime.now()}] 非交易时间，跳过同步")
        return 0
    
    engine = get_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT stock_code FROM si_all_code WHERE stock_code REGEXP '^[0-9]{6}$'"))
        all_codes = [str(row[0]).zfill(6) for row in result]
    
    if not all_codes:
        print(f"[{datetime.now()}] 无股票代码")
        return 0
    
    batch_size = 500
    total_synced = 0
    
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i+batch_size]
        try:
            df = fetch_list_market_current(batch)
            if not df.empty:
                ts = datetime.now().replace(microsecond=0)
                df["snapshot_at"] = ts
                df.to_sql(
                    "sm_rt_quote_snapshot",
                    engine,
                    if_exists="append",
                    index=False,
                    chunksize=500,
                    method="multi"
                )
                total_synced += len(df)
        except Exception as e:
            print(f"[{datetime.now()}] 批次 {i//batch_size + 1} 同步失败: {e}")
            continue
    
    print(f"[{datetime.now()}] 同步完成，共 {total_synced} 条记录")
    return total_synced

if __name__ == "__main__":
    sync_realtime()
