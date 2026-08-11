from env_config import create_tool_engine, resolve_tool_mysql_url
from sqlalchemy import text

from server.common.scheduler_tasks import update_scheduler_tasks


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())
    reset_count = update_scheduler_tasks(
        engine,
        {"last_run_status": "", "last_run_output": ""},
        lookup_where="last_run_status = :status",
        lookup_params={"status": "running"},
    )
    print("Reset %d zombie tasks" % reset_count)

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, task_name, last_run_status FROM st_scheduled_tasks ORDER BY id"
        )).fetchall()
        for r in rows:
            print("  ID=%-2d | status=%-10s | %s" % (r[0], r[2] or '(empty)', r[1]))


if __name__ == "__main__":
    main()
