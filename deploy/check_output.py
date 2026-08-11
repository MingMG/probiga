from env_config import create_tool_engine, resolve_tool_mysql_url
from sqlalchemy import text


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, task_name, last_run_status, LEFT(last_run_output, 200) AS output_tail "
            "FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id"
        )).fetchall()
        for r in rows:
            print("ID=%d | %s | output: %s" % (r[0], r[1], (r[3] or '')[:200]))
            print()


if __name__ == "__main__":
    main()
