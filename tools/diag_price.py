#!/usr/bin/env python3
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4')
with engine.connect() as c:
    a = c.execute(text("SELECT COUNT(*) FROM sm_stock_current")).scalar()
    b = c.execute(text("SELECT COUNT(*) FROM sm_stock_kline")).scalar()
    print(f"sm_stock_current: {a}")
    print(f"sm_stock_kline: {b}")
    if b > 0:
        r = c.execute(text("SELECT stock_code, close, change_pct, trade_date FROM sm_stock_kline ORDER BY trade_date DESC LIMIT 5")).fetchall()
        for x in r:
            print(f"  {x[0]} close={x[1]} chg={x[2]} date={x[3]}")
