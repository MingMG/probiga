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
        "SELECT table_name, table_rows, ROUND(data_length/1024/1024,2) as data_mb, update_time "
        "FROM information_schema.tables WHERE table_schema='probiga' ORDER BY table_name"
    )).fetchall()
    total = 0
    print("%-40s %10s %8s  %s" % ("TABLE", "ROWS", "SIZE_MB", "UPDATE_TIME"))
    print("-" * 80)
    for r in rows:
        print("%-40s %10d %8.2f  %s" % (r[0], r[1] or 0, r[2] or 0, r[3]))
        total += (r[1] or 0)
    print("\nTotal: ~%d rows" % total)
