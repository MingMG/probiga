from contextlib import contextmanager

from sqlalchemy import text

from server.api import scheduler_runtime as runtime
from test_scheduler_terminal_persistence import case, disconnect_begin, finish, rows


def test_database_disconnect_does_not_leave_shutdown_waiting_forever(case,monkeypatch):
    case.start(task_type="qmt_local_history_2024",stage=False)
    with case.engine.begin() as conn:
        conn.execute(text("INSERT INTO st_scheduled_task_history "
            "(run_uid,task_id,scheduler_instance_id,status) VALUES "
            "('foreign',56,'other-owner','running')"))
    monkeypatch.setattr(runtime,"_running_history_uids",{127:case.uid})
    clock=[100.0]
    monkeypatch.setattr(runtime.time,"monotonic",lambda:clock[0])
    monkeypatch.setattr(runtime,"get_engine",lambda:case.engine)
    begin=disconnect_begin(monkeypatch,case.engine)
    finish(case,status="stopped",exit_code=0xFFFFFFFF,
           output="confirmed stopped; token=secret-value")
    assert case.uid in runtime._pending_terminal_writes
    assert "secret-value" not in runtime._pending_terminal_writes[case.uid]["output"]
    monkeypatch.setattr(case.engine,"begin",begin)
    assert not runtime._owned_shutdown_runs_are_terminal({127:case.uid})
    assert case.uid in runtime._pending_terminal_writes
    runtime._running_history_uids.clear()
    clock[0]+=6
    assert runtime._owned_shutdown_runs_are_terminal({127:case.uid})
    assert runtime._pending_terminal_writes=={}
    assert rows(case)[0]["status"]=="stopped"
    assert rows(case)[0]["exit_code"]==-1
    with case.engine.connect() as conn:
        assert conn.execute(text("SELECT status FROM st_scheduled_task_history WHERE run_uid='foreign'")).scalar()=="running"


def test_non_database_error_is_not_replayed(case,monkeypatch):
    case.start(task_type="qmt_local_history_2024",stage=False)
    @contextmanager
    def invalid():
        raise ValueError("invalid terminal contract")
        yield
    monkeypatch.setattr(case.engine,"begin",invalid)
    finish(case,status="failed",exit_code=1,output="invalid")
    assert runtime._pending_terminal_writes=={}
    assert runtime._terminal_journal().read(case.uid,"REJECTED") is not None
    runtime._retry_pending_terminal_writes(case.engine)
    assert rows(case)[0]["status"]=="running"
