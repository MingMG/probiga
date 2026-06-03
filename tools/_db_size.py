from sqlalchemy import create_engine, text
e = create_engine("mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4", pool_pre_ping=True)
with e.connect() as c:
    r = c.execute(text("""
        SELECT table_name, ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
        FROM information_schema.tables
        WHERE table_schema = 'probiga'
        ORDER BY size_mb DESC
        LIMIT 15
    """)).fetchall()
    total = c.execute(text("""
        SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2)
        FROM information_schema.tables
        WHERE table_schema = 'probiga'
    """)).scalar()
    print("=== 最大表 TOP15 ===")
    for t, s in r:
        print(f"  {t:<35} {s:>8.2f} MB")
    print(f"  ---")
    print(f"  数据库总计: {total:.2f} MB")
