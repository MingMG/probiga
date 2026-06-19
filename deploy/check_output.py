from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4")
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT id, task_name, last_run_status, LEFT(last_run_output, 200) as out "
        "FROM st_scheduled_tasks WHERE last_run_status='running' ORDER BY id"
    )).fetchall()
    for r in rows:
        print("ID=%d | %s | output: %s" % (r[0], r[1], (r[3] or '')[:200]))
        print()
