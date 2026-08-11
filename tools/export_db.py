#!/usr/bin/env python3
from env_config import create_tool_engine
"""导出 probiga 数据库到 SQL 文件"""
import argparse
from pathlib import Path

import numpy as np
from sqlalchemy import text
from server.common.batch_db import quote_identifier, read_frame

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "exports" / "probiga_dump.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export the configured probiga database to a SQL file.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output .sql path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = args.output.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_tool_engine()

    with engine.connect() as c:
        tables = [r[0] for r in c.execute(text("SHOW TABLES")).fetchall()]

    print(f"共 {len(tables)} 张表")

    with output_path.open("w", encoding="utf-8") as f:
        f.write("CREATE DATABASE IF NOT EXISTS probiga DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n")
        f.write("USE probiga;\n\n")
        f.write("SET NAMES utf8mb4;\n\n")

        for tbl in tables:
            print(f"导出表: {tbl}")
            try:
                quoted_table = quote_identifier(str(tbl))
                df = read_frame(text(f"SELECT * FROM {quoted_table}"), engine)
            except Exception as e:
                print(f"  {tbl} 读取数据失败: {e}")
                continue
            if df.empty:
                print(f"  {tbl} 空表，跳过")
                continue

            with engine.connect() as c:
                ddl_row = c.execute(text(f"SHOW CREATE TABLE {quoted_table}")).fetchone()
                if not ddl_row:
                    print(f"  {tbl} 无建表语句")
                    continue
                ddl = ddl_row[1]

            f.write(f"\nDROP TABLE IF EXISTS {quoted_table};\n")
            f.write(ddl + ";\n\n")

            batch_size = 100
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start:start+batch_size]
                for _, row in batch.iterrows():
                    cols = ",".join([quote_identifier(str(c)) for c in df.columns])
                    vals = []
                    for v in row:
                        if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                            vals.append("NULL")
                        elif isinstance(v, (int, float)):
                            vals.append(str(v))
                        else:
                            s = str(v).replace("\\", "\\\\").replace("'", "\\'")
                            vals.append(f"'{s}'")
                    f.write(f"INSERT INTO {quoted_table} ({cols}) VALUES ({','.join(vals)});\n")
            print(f"  {tbl} 完成: {len(df)} 行")

    print(f"导出完成: {output_path}")


if __name__ == "__main__":
    main()
