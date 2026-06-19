from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4")
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, task_name, last_run_status FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id")).fetchall()
    print("Running tasks: %d" % len(rows))
    for r in rows:
        print("  ID=%d %s" % (r[0], r[1]))
    rows2 = conn.execute(text("SELECT id, task_name, last_run_status FROM st_scheduled_tasks WHERE last_run_status='success' ORDER BY id")).fetchall()
    print("\nCompleted tasks: %d" % len(rows2))
    for r in rows2:
        print("  ID=%d %s" % (r[0], r[1]))
