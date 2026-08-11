from env_config import create_tool_engine
from sqlalchemy import text


def main() -> None:
    engine = create_tool_engine()
    with engine.connect() as c:
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


if __name__ == "__main__":
    main()
