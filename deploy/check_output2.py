import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from tools.env_config import create_tool_engine

engine = create_tool_engine()
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT id, task_name, last_run_status, last_run_duration "
        "FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id"
    )).fetchall()
    for r in rows:
        print("ID=%d | %s | status=%s | duration=%s" % (r[0], r[1], r[2], r[3]))
