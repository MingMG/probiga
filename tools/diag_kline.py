#!/usr/bin/env python3
from sqlalchemy import create_engine, text
engine = create_engine('mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4')
with engine.connect() as c:
    cols = c.execute(text("SHOW COLUMNS FROM sm_stock_kline")).fetchall()
    print("sm_stock_kline columns:")
    for col in cols:
        print(f"  {col[0]}  {col[1]}")
    r = c.execute(text("SELECT * FROM sm_stock_kline WHERE stock_code='000001' ORDER BY trade_date DESC LIMIT 3")).fetchall()
    print("\nSample 000001:")
    col_names = [col[0] for col in cols]
    for row in r:
        for i, v in enumerate(row):
            print(f"  {col_names[i]}={v}")
        print()
