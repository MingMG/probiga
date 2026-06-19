from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4")
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT id, task_name, last_run_status, last_run_duration "
        "FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id"
    )).fetchall()
    for r in rows:
        print("ID=%d | %s | status=%s | duration=%s" % (r[0], r[1], r[2], r[3]))
