#!/usr/bin/env python3
"""导出 probiga 数据库到 SQL 文件"""
import numpy as np
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4", pool_pre_ping=True)

with engine.connect() as c:
    tables = [r[0] for r in c.execute(text("SHOW TABLES")).fetchall()]

print(f"共 {len(tables)} 张表")

with open("E:/probiga_dump.sql", "w", encoding="utf-8") as f:
    f.write("CREATE DATABASE IF NOT EXISTS probiga DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n")
    f.write("USE probiga;\n\n")
    f.write("SET NAMES utf8mb4;\n\n")

    for tbl in tables:
        print(f"导出表: {tbl}")
        try:
            df = pd.read_sql(text(f"SELECT * FROM {tbl}"), engine)
        except Exception as e:
            print(f"  {tbl} 读取数据失败: {e}")
            continue
        if df.empty:
            print(f"  {tbl} 空表，跳过")
            continue

        with engine.connect() as c:
            ddl_row = c.execute(text(f"SHOW CREATE TABLE `{tbl}`")).fetchone()
            if not ddl_row:
                print(f"  {tbl} 无建表语句")
                continue
            ddl = ddl_row[1]

        f.write(f"\nDROP TABLE IF EXISTS `{tbl}`;\n")
        f.write(ddl + ";\n\n")

        batch_size = 100
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start+batch_size]
            for _, row in batch.iterrows():
                cols = ",".join([f"`{c}`" for c in df.columns])
                vals = []
                for v in row:
                    if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        vals.append(f"'{s}'")
                f.write(f"INSERT INTO `{tbl}` ({cols}) VALUES ({','.join(vals)});\n")
        print(f"  {tbl} 完成: {len(df)} 行")

print("导出完成: E:/probiga_dump.sql")
