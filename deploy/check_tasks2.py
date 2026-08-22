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
        "SELECT id, task_name, script_path, script_args, cron_time, enabled, last_run_status "
        "FROM st_scheduled_tasks ORDER BY sort_order"
    )).fetchall()
    for r in rows:
        args = r[3] or ''
        print("ID=%-2d | %-20s | %-35s | args=%-30s | cron=%s | en=%s | %s" % (
            r[0], r[1][:20], (r[2] or '')[:35], args[:30], r[4], r[5], r[6] or ''))

    # Check sm_stock_minute sample
    print("\n--- sm_stock_minute sample ---")
    rows2 = conn.execute(text("SELECT COUNT(*) as cnt, MIN(trade_time), MAX(trade_time) FROM sm_stock_minute")).fetchone()
    print("Rows: %s, Time range: %s ~ %s" % (rows2[0], rows2[1], rows2[2]))

    rows3 = conn.execute(text("SELECT stock_code, COUNT(*) as cnt FROM sm_stock_minute GROUP BY stock_code ORDER BY cnt DESC LIMIT 10")).fetchall()
    for r in rows3:
        print("  %s: %d rows" % (r[0], r[1]))
