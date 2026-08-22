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
        "SELECT id, task_name, script_path, cron_time, enabled, last_run_status, "
        "DATE_FORMAT(last_run_at, '%%m-%%d %%H:%%i') as last_run, "
        "DATE_FORMAT(last_triggered_at, '%%m-%%d %%H:%%i') as last_trig "
        "FROM st_scheduled_tasks ORDER BY sort_order"
    )).fetchall()
    print("%-2s %-28s %-40s %-6s %-3s %-10s %-12s %-12s" % ("ID", "TASK", "SCRIPT", "CRON", "EN", "STATUS", "LAST_RUN", "LAST_TRIG"))
    print("-" * 120)
    for r in rows:
        print("%-2d %-28s %-40s %-6s %-3s %-10s %-12s %-12s" % (
            r[0], r[1][:28], (r[2] or '')[:40], r[3] or '', r[4],
            r[5] or '', r[6] or '', r[7] or ''))
