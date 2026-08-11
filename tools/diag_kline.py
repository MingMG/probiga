#!/usr/bin/env python3
from env_config import create_tool_engine
from sqlalchemy import text


def main() -> None:
    engine = create_tool_engine()
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


if __name__ == "__main__":
    main()
