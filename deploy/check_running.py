from env_config import create_tool_engine, resolve_tool_mysql_url
from sqlalchemy import text


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, task_name, last_run_status FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id")).fetchall()
        print("Running tasks: %d" % len(rows))
        for r in rows:
            print("  ID=%d %s" % (r[0], r[1]))
        rows2 = conn.execute(text("SELECT id, task_name, last_run_status FROM st_scheduled_tasks WHERE last_run_status='success' ORDER BY id")).fetchall()
        print("\nCompleted tasks: %d" % len(rows2))
        for r in rows2:
            print("  ID=%d %s" % (r[0], r[1]))


if __name__ == "__main__":
    main()
