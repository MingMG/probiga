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
        "SELECT id, task_name, last_run_status, "
        "LEFT(last_run_output, 200) AS output_preview "
        "FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id"
    )).fetchall()
    for r in rows:
        print("ID=%d | %s | output: %s" % (r[0], r[1], (r[3] or '')[:200]))
        print()
