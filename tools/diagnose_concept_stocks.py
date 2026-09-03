#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine

engine = create_tool_engine()

with engine.connect() as conn:
    print("=" * 60)
    print("  诊断概念成分股问题")
    print("=" * 60)

    # 1. 检查概念热度表有没有数据
    cnt = conn.execute(text("SELECT COUNT(*) FROM st_hot_concept_ths_daily")).scalar()
    print(f"\n[1] st_hot_concept_ths_daily: {cnt} 行")

    # 2. 取一个最近的概念代码
    row = conn.execute(text(
        "SELECT concept_code, concept_name, plate_type FROM st_hot_concept_ths_daily ORDER BY snapshot_date DESC LIMIT 1"
    ).execution_options(autocommit=True)).fetchone()
    if row:
        print(f"    最新概念示例: code={row[0]}, name={row[1]}, type={row[2]}")
        sample_code = row[0]
    else:
        print("    无数据！")
        sample_code = None

    # 3. 检查概念代码映射表
    cnt2 = conn.execute(text("SELECT COUNT(*) FROM si_concept_code_ths")).scalar()
    print(f"\n[2] si_concept_code_ths: {cnt2} 行")

    if sample_code:
        rows = conn.execute(text(
            "SELECT concept_code, index_code, name FROM si_concept_code_ths WHERE concept_code = :c LIMIT 5"
        ), {"c": sample_code}).fetchall()
        if rows:
            for r in rows:
                print(f"    匹配: concept_code={r[0]}, index_code={r[1]}, name={r[2]}")
        else:
            print(f"    concept_code={sample_code} 在映射表中找不到！")
            # 尝试模糊匹配
            rows2 = conn.execute(text(
                "SELECT concept_code, index_code, name FROM si_concept_code_ths WHERE concept_code LIKE :c LIMIT 5"
            ), {"c": "%" + sample_code[:4] + "%"}).fetchall()
            if rows2:
                print(f"    模糊匹配(前4位)找到 {len(rows2)} 条:")
                for r in rows2:
                    print(f"      concept_code={r[0]}, index_code={r[1]}, name={r[2]}")

    # 4. 检查成分股表
    cnt3 = conn.execute(text("SELECT COUNT(*) FROM si_concept_constituent_ths")).scalar()
    print(f"\n[3] si_concept_constituent_ths: {cnt3} 行")

    if sample_code:
        # 直接用 concept_code 查
        cnt_direct = conn.execute(text(
            "SELECT COUNT(*) FROM si_concept_constituent_ths WHERE query_key = :c"
        ), {"c": sample_code}).scalar()
        print(f"    直接查 query_key='{sample_code}': {cnt_direct} 条")

        # 用 index_code 查
        idx_row = conn.execute(text(
            "SELECT index_code FROM si_concept_code_ths WHERE concept_code = :c LIMIT 1"
        ), {"c": sample_code}).fetchone()
        if idx_row and idx_row[0]:
            idx_code = idx_row[0]
            cnt_idx = conn.execute(text(
                "SELECT COUNT(*) FROM si_concept_constituent_ths WHERE query_key = :c"
            ), {"c": idx_code}).scalar()
            print(f"    用 index_code='{idx_code}' 查: {cnt_idx} 条")

    # 5. 检查 query_key 分布
    rows_dist = conn.execute(text(
        "SELECT query_type, "
        "COUNT(DISTINCT query_key) AS distinct_query_key_count, "
        "COUNT(*) AS row_count "
        "FROM si_concept_constituent_ths GROUP BY query_type"
    )).fetchall()
    print(f"\n[4] 成分股 query_key 分布:")
    for r in rows_dist:
        print(f"    {r[0]}: {r[1]} 个key, {r[2]} 条记录")

    # 6. 看几条 sample
    rows_sample = conn.execute(text(
        "SELECT query_type, query_key, stock_code, short_name FROM si_concept_constituent_ths LIMIT 5"
    )).fetchall()
    print(f"\n[5] 成分股前5条:")
    for r in rows_sample:
        print(f"    type={r[0]}, key={r[1]}, code={r[2]}, name={r[3]}")

    # 7. sm_stock_current 有数据吗
    cnt4 = conn.execute(text("SELECT COUNT(*) FROM sm_stock_current")).scalar()
    print(f"\n[6] sm_stock_current: {cnt4} 行")

print("\nDone!")
