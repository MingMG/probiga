import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from tools.env_config import create_tool_engine

engine = create_tool_engine()
with engine.begin() as conn:
    # Reset all stuck "running" tasks
    result = conn.execute(text(
        "UPDATE st_scheduled_tasks SET last_run_status='', last_run_output='' "
        "WHERE last_run_status='running'"
    ))
    print("Reset %d zombie tasks" % result.rowcount)

    # Verify
    rows = conn.execute(text(
        "SELECT id, task_name, last_run_status FROM st_scheduled_tasks ORDER BY id"
    )).fetchall()
    for r in rows:
        print("  ID=%-2d | status=%-10s | %s" % (r[0], r[2] or '(empty)', r[1]))
