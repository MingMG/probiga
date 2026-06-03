# -*- coding: utf-8 -*-
"""构建统一的个股-概念映射表 si_stock_concept_map

数据源：si_concept_constituent_ths + si_concept_code_ths
规则：
1. 合并 index_code 和 concept_code 两种映射
2. 过滤非行业/主题类概念（融资融券、深股通等）
3. 去重后写入 si_stock_concept_map
"""
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

DB_URL = 'mysql+pymysql://root:ProBigA%4070966@47.113.123.190:3306/probiga'

# 需要过滤的概念关键词
FILTER_KEYWORDS = (
    "融资融券", "深股通", "沪股通", "陆股通", "北向资金", "QFII",
    "年报", "一季报", "半年报", "三季报", "季报", "预增", "预减", "预亏", "预盈",
    "ST板块", "退市", "风险警示",
    "两融", "转融通", "MSCI", "标普", "富时", "中证",
    "涨跌停", "龙虎榜", "大宗交易",
    "同花顺漂亮", "证金持股", "超级品牌",
)


def build_concept_map():
    engine = create_engine(DB_URL)

    print("[1/4] 读取概念映射数据...")
    df = pd.read_sql(text("""
        SELECT ct.stock_code, ct.short_name, ct.query_type, ct.query_key,
               cc.name AS concept_name, cc.index_code, cc.concept_code
        FROM si_concept_constituent_ths ct
        JOIN si_concept_code_ths cc
            ON (ct.query_type = 'index_code' AND ct.query_key = cc.index_code)
            OR (ct.query_type = 'concept_code' AND ct.query_key = cc.concept_code)
    """), engine)
    print(f"  原始数据: {len(df)} 条")

    print("[2/4] 过滤非行业/主题类概念...")
    mask = df['concept_name'].apply(lambda n: not any(kw in str(n) for kw in FILTER_KEYWORDS))
    df = df[mask].copy()
    print(f"  过滤后: {len(df)} 条")

    print("[3/4] 去重并构建最终数据...")
    df['_stock_code'] = df['stock_code']
    df['_concept_name'] = df['concept_name']
    df = df.drop_duplicates(subset=['_stock_code', '_concept_name'])
    result = df[['stock_code', 'short_name', 'concept_name']].copy()
    result = result.rename(columns={'concept_name': 'name'})
    result['source'] = 'ths'
    result['etl_sync_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = result.sort_values(['stock_code', 'name']).reset_index(drop=True)
    print(f"  最终数据: {len(result)} 条")

    print("[4/4] 写入数据库...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE si_stock_concept_map"))
        conn.commit()

    result.to_sql('si_stock_concept_map', engine, if_exists='append', index=False, chunksize=200)

    stats = pd.read_sql(text("""
        SELECT 
            COUNT(*) AS total,
            COUNT(DISTINCT stock_code) AS stock_count,
            COUNT(DISTINCT name) AS concept_count
        FROM si_stock_concept_map
    """), engine)

    print(f"\n=== 构建完成 ===")
    print(f"  总映射数: {stats.iloc[0]['total']}")
    print(f"  覆盖个股: {stats.iloc[0]['stock_count']}")
    print(f"  概念数量: {stats.iloc[0]['concept_count']}")


if __name__ == '__main__':
    build_concept_map()
