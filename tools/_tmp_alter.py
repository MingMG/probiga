from env_config import create_tool_engine

engine = create_tool_engine()
conn = engine.raw_connection()
cur = conn.cursor()
try:
    cur.execute("ALTER TABLE st_scheduled_tasks ADD COLUMN interval_minutes INT DEFAULT 0 AFTER cron_time")
    print("interval_minutes added")
except Exception as e:
    print(f"interval_minutes: {e}")
try:
    cur.execute("ALTER TABLE st_scheduled_tasks ADD COLUMN last_triggered_at DATETIME DEFAULT NULL AFTER last_run_at")
    print("last_triggered_at added")
except Exception as e:
    print(f"last_triggered_at: {e}")
conn.commit()
cur.execute("DESCRIBE st_scheduled_tasks")
for row in cur.fetchall():
    print(row)
cur.close()
conn.close()
engine.dispose()
