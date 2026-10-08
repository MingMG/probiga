"""Real SQLite daily DDL and ordinary-user job-log recovery, never production."""
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.api import scheduler_runtime as runtime
from server.common import daily_delivery_control as daily
from server.common.scheduler_tasks import claim_scheduler_task_run


@pytest.fixture
def case(tmp_path, monkeypatch):
    now = datetime(2026, 10, 9, 8, 0, 0)
    monkeypatch.setattr(runtime, "_now_shanghai_naive", lambda: now)
    monkeypatch.setattr(daily, "_control_now", lambda: now)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    jobs = tmp_path / "jobs"
    jobs.mkdir(mode=0o700)
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(jobs))
    for name in ("_pending_terminal_writes", "_terminal_run_identities",
                 "_terminal_stage_identities", "_terminal_session_identities",
                 "_terminal_session_expectations", "_running_history_uids", "_running_procs"):
        monkeypatch.setattr(runtime, name, {})
    monkeypatch.setattr(runtime, "_terminal_worker_exited", set())
    monkeypatch.setattr(runtime, "_last_terminal_retry", 0.0)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    event.listen(engine, "connect", lambda db, _: db.create_function("NOW", 0, lambda: now.isoformat(" ")))
    daily.privileged_migrate_daily_delivery_schema(engine)
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE st_scheduled_tasks (
            id INTEGER PRIMARY KEY,task_name TEXT,task_type TEXT,enabled INTEGER,
            last_run_status TEXT,last_run_at DATETIME,last_triggered_at DATETIME,
            last_run_output TEXT,last_run_duration INTEGER,updated_at DATETIME)"""))
        conn.execute(text("""CREATE TABLE st_scheduled_task_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,run_uid TEXT UNIQUE NOT NULL,
            task_id INTEGER,task_name TEXT,task_type TEXT,run_at DATETIME,
            finished_at DATETIME,status TEXT,duration INTEGER,exit_code INTEGER,
            host_name TEXT,scheduler_instance_id TEXT,build_sha TEXT,trigger_source TEXT,output TEXT)"""))
    data = SimpleNamespace(engine=engine, now=now, jobs=jobs, uid="a"*32,
                           task_id=127, task_type="qmt_announcement_pit")

    def start(*, task_type="qmt_announcement_pit", uid="a"*32, stage=True):
        data.uid, data.task_type = uid, task_type
        identity = dict(run_uid=uid,task_id=127,task_name=task_type,task_type=task_type,
            run_at=now.isoformat(),host_name="MODEL-WIN",
            scheduler_instance_id=runtime._scheduler_instance_id,build_sha="b"*40,
            trigger_source="scheduled")
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO st_scheduled_tasks VALUES "
                "(127,:task_type,:task_type,1,'running',:now,:now,'OLD COMPLETE',99,:now)"),
                {"task_type":task_type,"now":now})
            conn.execute(text("INSERT INTO st_scheduled_task_history "
                "(run_uid,task_id,task_name,task_type,run_at,status,host_name,scheduler_instance_id,build_sha,trigger_source) "
                "VALUES (:run_uid,:task_id,:task_name,:task_type,:run_at,'running',:host_name,:scheduler_instance_id,:build_sha,:trigger_source)"),
                {**identity,"run_at":now})
        runtime._terminal_run_identities[uid] = identity
        runtime._terminal_stage_identities[uid] = None
        if stage:
            attempt = daily.start_daily_stage_attempt(engine,scheduler_run_uid=uid,
                stage_name=task_type,trade_date="2026-09-30",release_id="b"*40,
                strategy_release_id="c"*64,lease_owner=runtime._scheduler_instance_id,
                lease_seconds=90,preserve_session_status=True)
            runtime._terminal_stage_identities[uid] = runtime._terminal_stage_identity(attempt)
            with engine.connect() as conn:
                runtime._terminal_session_identities[uid] = runtime._terminal_session_identity(
                    runtime._terminal_session_row(conn,attempt["session_uid"]))
            runtime._terminal_session_expectations[uid] = dict(trade_date="2026-09-30",
                release_id="b"*40,strategy_release_id="c"*64)
        return data

    data.start = start
    yield data
    engine.dispose()


def finish(case, **overrides):
    runtime._task_history_finish(case.engine,case.uid,**dict(
        dict(status="success",duration=61,exit_code=0,output="ORIGINAL RESULT",
             task_type=case.task_type),**overrides))


def rows(case):
    with case.engine.connect() as conn:
        history = dict(conn.execute(text("SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid"),{"uid":case.uid}).mappings().one())
        stage = conn.execute(text(f"SELECT * FROM {daily.ATTEMPT_TABLE} WHERE scheduler_run_uid=:uid"),{"uid":case.uid}).mappings().first()
        task = dict(conn.execute(text("SELECT * FROM st_scheduled_tasks WHERE id=127")).mappings().one())
    return history,dict(stage) if stage else None,task


def disconnect_begin(monkeypatch, engine):
    original = engine.begin
    @contextmanager
    def broken():
        raise OperationalError("terminal MODEL",{},ConnectionError("MySQL 2013 MODEL"))
        yield
    monkeypatch.setattr(engine,"begin",broken)
    return original


@pytest.mark.parametrize("status", ["success","failed","blocked","timeout","stopped"])
def test_daily_db_failure_preserves_first_outcome_then_atomic_retry(case,monkeypatch,status):
    case.start()
    original = disconnect_begin(monkeypatch,case.engine)
    finish(case,status=status,output="ORIGINAL token=secret-value",exit_code=0xFFFFFFFF)
    history,stage,task = rows(case)
    assert (history["status"],stage["status"],task["last_run_status"]) == ("running","RUNNING","running")
    journal = runtime._terminal_journal()
    observed = journal.read(case.uid,"OBSERVED")
    assert b"secret-value" not in observed
    assert "secret-value" not in runtime._pending_terminal_writes[case.uid]["output"]
    # A DB error is not a second child failure.
    finish(case,status="failed",output="MUST NOT REPLACE")
    assert journal.read(case.uid,"OBSERVED") == observed
    monkeypatch.setattr(case.engine,"begin",original)
    runtime._retry_pending_terminal_writes(case.engine)
    history,stage,task = rows(case)
    assert history["status"] == task["last_run_status"] == status
    assert stage["status"] == {"success":"SUCCESS","failed":"FAILED","blocked":"BLOCKED","timeout":"TIMEOUT","stopped":"STOPPED"}[status]
    assert history["exit_code"] == -1
    assert "ORIGINAL" in history["output"] and "MUST NOT REPLACE" not in history["output"]
    assert journal.read(case.uid,"COMMITTED") is not None
    assert case.uid not in runtime._pending_terminal_writes


def test_unknown_commit_readback_never_repeats_stage_or_publication(case,monkeypatch):
    case.start()
    begin = case.engine.begin
    @contextmanager
    def commit_then_disconnect():
        with begin() as conn:
            yield conn
        if rows(case)[0]["status"] != "running":
            raise OperationalError("COMMIT MODEL",{},ConnectionError("response lost"))
    monkeypatch.setattr(case.engine,"begin",commit_then_disconnect)
    finish(case)
    assert rows(case)[0]["status"] == "success"
    assert case.uid in runtime._pending_terminal_writes
    monkeypatch.setattr(case.engine,"begin",begin)
    monkeypatch.setattr(runtime,"finish_daily_stage_attempt",lambda *a,**k:pytest.fail("repeated stage side effect"))
    monkeypatch.setattr(runtime,"_daily_delivery_runtime_health",lambda *a,**k:pytest.fail("health before exact readback"))
    runtime._retry_pending_terminal_writes(case.engine)
    assert case.uid not in runtime._pending_terminal_writes
    assert runtime._terminal_journal().read(case.uid,"COMMITTED") is not None


def test_restart_replays_original_old_owner_not_new_owner(case,monkeypatch):
    case.start()
    begin = disconnect_begin(monkeypatch,case.engine)
    finish(case)
    original = runtime._terminal_journal().read(case.uid,"OBSERVED")
    monkeypatch.setattr(case.engine,"begin",begin)
    runtime._pending_terminal_writes.clear()
    runtime._terminal_run_identities.clear()
    runtime._terminal_stage_identities.clear()
    runtime._terminal_session_identities.clear()
    runtime._terminal_session_expectations.clear()
    monkeypatch.setattr(runtime,"_scheduler_instance_id","NEW-DAEMON")
    runtime._retry_pending_terminal_writes(case.engine)
    assert rows(case)[0]["status"] == "success"
    assert runtime._terminal_journal().read(case.uid,"OBSERVED") == original


def test_complete_cached_start_survives_total_database_outage_then_restart(case,monkeypatch):
    case.start()
    connect,begin=case.engine.connect,case.engine.begin
    def disconnected(*a,**k):
        raise OperationalError("MODEL all reads and writes",{},ConnectionError("MySQL 2013 MODEL"))
    monkeypatch.setattr(case.engine,"connect",disconnected)
    monkeypatch.setattr(case.engine,"begin",disconnected)
    finish(case,output="ORIGINAL RESULT token=secret-value")
    journal=runtime._terminal_journal()
    original=journal.read(case.uid,"OBSERVED")
    assert json.loads(original)["outcome"]["status"]=="success"
    assert b"secret-value" not in original
    assert runtime._pending_terminal_writes[case.uid]["_durable"]
    # The first outcome, not a newly invented daemon result, survives restart.
    for name in ("_pending_terminal_writes","_terminal_run_identities",
                 "_terminal_stage_identities","_terminal_session_identities",
                 "_terminal_session_expectations"):
        getattr(runtime,name).clear()
    monkeypatch.setattr(runtime,"_scheduler_instance_id","RESTARTED-DAEMON")
    monkeypatch.setattr(case.engine,"connect",connect)
    monkeypatch.setattr(case.engine,"begin",begin)
    runtime._retry_pending_terminal_writes(case.engine)
    assert rows(case)[0]["status"]=="success" and rows(case)[1]["status"]=="SUCCESS"
    assert journal.read(case.uid,"OBSERVED")==original
    assert journal.read(case.uid,"COMMITTED") is not None


def test_unknown_start_stage_during_read_outage_does_not_invent_absence(case,monkeypatch):
    case.start()
    runtime._terminal_stage_identities[case.uid]=None
    runtime._terminal_session_identities[case.uid]=None
    def disconnected(*a,**k):
        raise OperationalError("MODEL unknown start read",{},ConnectionError("MySQL 2013 MODEL"))
    monkeypatch.setattr(case.engine,"connect",disconnected)
    monkeypatch.setattr(case.engine,"begin",disconnected)
    finish(case)
    assert not (case.jobs/"scheduler-terminal"/case.uid/"OBSERVED").exists()
    assert not runtime._pending_terminal_writes[case.uid]["_durable"]
    assert runtime._pending_terminal_writes[case.uid]["output"]=="ORIGINAL RESULT"


def test_old_owner_without_original_cannot_invent_terminal(case,monkeypatch):
    case.start()
    runtime._terminal_run_identities.clear()
    runtime._terminal_stage_identities.clear()
    monkeypatch.setattr(runtime,"_scheduler_instance_id","NEW-DAEMON")
    finish(case)
    assert rows(case)[0]["status"] == "running"
    assert not (case.jobs/"scheduler-terminal"/case.uid/"OBSERVED").exists()


def test_stage_start_commit_unknown_reads_actual_stage_before_first_observation(case,monkeypatch):
    case.start()
    runtime._terminal_stage_identities[case.uid] = None
    runtime._terminal_session_identities[case.uid] = None
    finish(case)
    value = json.loads(runtime._terminal_journal().read(case.uid,"OBSERVED"))
    assert value["stage_identity"]["attempt_uid"] == rows(case)[1]["attempt_uid"]
    assert rows(case)[1]["status"] == "SUCCESS"


@pytest.mark.parametrize("field,value", [
    ("lease_owner","FOREIGN"),("fencing_token",999),("attempt_uid","f"*32),
    ("session_uid","f"*32),("stage_name","foreign"),("shard_id","foreign")])
def test_changed_original_stage_is_pending_not_overwritten(case,monkeypatch,field,value):
    case.start()
    begin = disconnect_begin(monkeypatch,case.engine)
    finish(case)
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text(f"UPDATE {daily.ATTEMPT_TABLE} SET {field}=:value WHERE scheduler_run_uid=:uid"),{"value":value,"uid":case.uid})
    finish(case)
    assert rows(case)[0]["status"] == "running"
    assert rows(case)[1]["status"] == "RUNNING"
    assert runtime._terminal_journal().read(case.uid,"FAILED_FINALIZATION") is None


def test_same_second_new_uid_does_not_alias_current_claim(case,monkeypatch):
    case.start()
    begin = disconnect_begin(monkeypatch,case.engine)
    finish(case)
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text("INSERT INTO st_scheduled_task_history "
            "(run_uid,task_id,task_name,task_type,run_at,status,host_name,scheduler_instance_id,build_sha,trigger_source) "
            "SELECT :next,task_id,task_name,task_type,run_at,'running',host_name,scheduler_instance_id,build_sha,trigger_source "
            "FROM st_scheduled_task_history WHERE run_uid=:uid"),{"next":"b"*32,"uid":case.uid})
    finish(case)
    assert rows(case)[0]["status"] == "running" and rows(case)[2]["last_run_status"] == "running"


def test_expired_original_lease_preserves_child_success_and_finalizes_failed(case,monkeypatch):
    case.start()
    begin = disconnect_begin(monkeypatch,case.engine)
    finish(case)
    journal = runtime._terminal_journal()
    original = journal.read(case.uid,"OBSERVED")
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text(f"UPDATE {daily.ATTEMPT_TABLE} SET lease_until=:expired"),{"expired":case.now-timedelta(seconds=1)})
    finish(case)
    history,stage,task = rows(case)
    assert journal.read(case.uid,"OBSERVED") == original
    assert json.loads(original)["outcome"]["status"] == "success"
    assert json.loads(original)["outcome"]["exit_code"] == 0
    assert history["status"] == task["last_run_status"] == "failed"
    assert history["exit_code"] is None and stage["status"] == "FAILED"
    assert "publication lease expired" in history["output"]
    assert journal.read(case.uid,"FAILED_FINALIZATION") is not None


def test_superseding_fence_never_gets_expired_failed_finalization(case,monkeypatch):
    case.start()
    begin = disconnect_begin(monkeypatch,case.engine)
    finish(case)
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text(f"UPDATE {daily.ATTEMPT_TABLE} SET lease_until=:expired"),{"expired":case.now-timedelta(seconds=1)})
    daily.start_daily_stage_attempt(case.engine,scheduler_run_uid="b"*32,
        stage_name=case.task_type,trade_date="2026-09-30",release_id="b"*40,
        strategy_release_id="c"*64,lease_owner="NEW-OWNER",lease_seconds=90)
    finish(case)
    assert rows(case)[0]["status"] == "running" and rows(case)[1]["status"] == "SUPERSEDED"
    assert runtime._terminal_journal().read(case.uid,"FAILED_FINALIZATION") is None


@pytest.mark.parametrize("missing",["OBSERVED","prepared"])
def test_committed_downstream_does_not_rebuild_missing_predecessor(case,missing):
    case.start()
    finish(case)
    directory=case.jobs/"scheduler-terminal"/case.uid
    path=directory/"OBSERVED" if missing=="OBSERVED" else next(directory.glob("PREPARED.*"))
    path.unlink()  # Isolated fault injection only, never production cleanup.
    finish(case)
    assert not path.exists()
    assert rows(case)[0]["status"] == "success"
    assert case.uid in runtime._pending_terminal_writes


def test_projection_cas_failure_rolls_back_daily_and_audit(case,monkeypatch):
    case.start()
    def fail(*a,**k):
        raise runtime._TerminalPersistencePending("MODEL lost projection CAS")
    monkeypatch.setattr(runtime,"_terminal_update_projection",fail)
    finish(case)
    assert (rows(case)[0]["status"],rows(case)[1]["status"]) == ("running","RUNNING")


def test_claim_clears_previous_output_not_history(case):
    case.start(task_type="model_task",stage=False)
    with case.engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_tasks SET last_run_status='success'"))
    assert claim_scheduler_task_run(case.engine,127)
    history,_,task=rows(case)
    assert task["last_run_status"] == "running" and task["last_run_output"] is None
    assert task["last_run_duration"] is None and history["status"] == "running"


def test_raw_retention_failure_keeps_worker_ownership(case,monkeypatch):
    case.start()
    runtime._running_history_uids[127]=case.uid
    runtime._running_task_ids.add(127)
    monkeypatch.setattr(runtime,"_task_lane_semaphore",lambda row:__import__("contextlib").nullcontext())
    monkeypatch.setattr(runtime,"_run_task",lambda *a:finish(case))
    monkeypatch.setattr(runtime,"_terminal_journal",lambda:(_ for _ in ()).throw(OSError("MODEL fsync failed")))
    runtime._run_task_async({"id":127,"_history_run_uid":case.uid},case.jobs,case.engine)
    assert runtime._running_history_uids[127] == case.uid
    assert case.uid in runtime._terminal_worker_exited
    assert not runtime._pending_terminal_writes[case.uid]["_durable"]
    runtime._running_task_ids.discard(127)


@pytest.mark.parametrize("path",["outer","script_policy","missing_script"])
def test_error_paths_do_not_publish_summary_before_terminal_transaction(case,monkeypatch,path):
    case.start(task_type="model_task",stage=False)
    row=dict(id=127,task_name="model_task",task_type=case.task_type,
             script_path="tools/model.py",_history_started=True,_history_run_uid=case.uid)
    disconnect_begin(monkeypatch,case.engine)
    monkeypatch.setattr(runtime,"_scheduler_build_commit_sha",lambda:"b"*40)
    if path=="outer":
        monkeypatch.setattr(runtime,"_run_task_impl",lambda *a,**k:(_ for _ in ()).throw(ValueError("MODEL launch")))
        runtime._run_task(row,case.jobs,case.engine)
    else:
        if path=="script_policy":
            monkeypatch.setattr(runtime,"resolve_scheduler_script",lambda *a:(_ for _ in ()).throw(runtime.SchedulerScriptPolicyError("MODEL denied")))
        else:
            monkeypatch.setattr(runtime,"resolve_scheduler_script",lambda *a:case.jobs/"missing.py")
        runtime._run_task_impl(row,case.jobs,case.engine,history_run_uid=case.uid)
    assert rows(case)[0]["status"]==rows(case)[2]["last_run_status"]=="running"
    assert json.loads(runtime._terminal_journal().read(case.uid,"OBSERVED"))["outcome"]["status"]=="failed"


@pytest.mark.parametrize("timeout",[False,True])
def test_normal_and_timeout_outputs_survive_terminal_database_failure(case,monkeypatch,timeout):
    from server.common.scheduler_validation import SchedulerValidationResult
    case.start(task_type="model_task",stage=False)
    row=dict(id=127,task_name="model_task",task_type=case.task_type,script_path="tools/model.py")
    script=case.jobs/"model.py"
    script.touch()
    monkeypatch.setattr(runtime,"_scheduler_build_commit_sha",lambda:"b"*40)
    monkeypatch.setattr(runtime,"resolve_scheduler_script",lambda *a:script)
    monkeypatch.setattr(runtime,"_task_dispatch_date",lambda *a,**k:"2026-09-30")
    monkeypatch.setattr(runtime,"_task_argument_row",lambda row,**k:row)
    monkeypatch.setattr(runtime,"_bind_release_validation_target",lambda row,*a,**k:row)
    monkeypatch.setattr(runtime,"_build_task_args",lambda *a:[])
    monkeypatch.setattr(runtime,"build_child_env",lambda *a,**k:{})
    monkeypatch.setattr(runtime,"_try_revalidate_existing_notice_receipt",lambda *a,**k:False)
    monkeypatch.setattr(runtime,"_task_timeout_minutes",lambda *a,**k:1)
    monkeypatch.setattr(runtime,"_terminate_process",lambda proc:None)
    monkeypatch.setattr(runtime,"scheduler_output_status",lambda *a,**k:None)
    monkeypatch.setattr(runtime,"validate_scheduler_task_result",lambda *a,**k:SchedulerValidationResult(checked=False,ok=False,message="MODEL"))
    calls=[]
    class Process:
        returncode=0
        def communicate(self,**kwargs):
            calls.append(kwargs)
            if timeout and len(calls)==1:
                raise runtime.subprocess.TimeoutExpired("MODEL",1)
            return "ORIGINAL STDOUT","ORIGINAL STDERR"
    def spawn(*a,**k):
        disconnect_begin(monkeypatch,case.engine)
        return Process()
    monkeypatch.setattr(runtime.subprocess,"Popen",spawn)
    runtime._run_task_impl(row,case.jobs,case.engine,history_run_uid=case.uid)
    intent=json.loads(runtime._terminal_journal().read(case.uid,"OBSERVED"))
    assert intent["outcome"]["status"]==("timeout" if timeout else "success")
    assert "ORIGINAL STDOUT" in intent["outcome"]["output"]
    assert "ORIGINAL STDERR" in intent["outcome"]["output"]
    assert rows(case)[0]["status"]==rows(case)[2]["last_run_status"]=="running"


def test_late_terminal_wall_clock_does_not_replace_original_claim_stamp(case,monkeypatch):
    case.start()
    monkeypatch.setattr(runtime,"_now_shanghai_naive",lambda:case.now+timedelta(seconds=10))
    finish(case)
    history,stage,task=rows(case)
    assert history["status"]=="success" and stage["status"]=="SUCCESS"
    assert runtime._coerce_datetime(history["run_at"])==case.now
    assert runtime._coerce_datetime(history["finished_at"])==case.now+timedelta(seconds=10)


def test_normal_long_validation_refresh_happens_after_original_durable(case,monkeypatch):
    case.start()
    late=case.now+timedelta(seconds=120)
    monkeypatch.setattr(runtime,"_now_shanghai_naive",lambda:late)
    monkeypatch.setattr(daily,"_control_now",lambda:late)
    original=runtime._refresh_daily_stage_lease_for_publication
    called=[]
    def refresh(*a,**k):
        assert runtime._terminal_journal().read(case.uid,"OBSERVED") is not None
        called.append(True)
        return original(*a,**k)
    monkeypatch.setattr(runtime,"_refresh_daily_stage_lease_for_publication",refresh)
    finish(case)
    assert called==[True]
    assert rows(case)[0]["status"]=="success" and rows(case)[1]["status"]=="SUCCESS"
    assert runtime._terminal_journal().read(case.uid,"FAILED_FINALIZATION") is None


@pytest.mark.parametrize("field,value",[
    ("id",99),("run_id","foreign"),("trade_date","2026-09-29"),
    ("release_id","d"*40),("strategy_release_id","d"*64),
    ("started_at","2026-10-09 07:00:00")])
def test_session_immutable_drift_never_commits_old_observation(case,monkeypatch,field,value):
    case.start()
    begin=disconnect_begin(monkeypatch,case.engine)
    finish(case)
    original=runtime._terminal_journal().read(case.uid,"OBSERVED")
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text(f"UPDATE {daily.SESSION_TABLE} SET {field}=:value"),{"value":value})
    finish(case)
    assert rows(case)[0]["status"]=="running" and rows(case)[1]["status"]=="RUNNING"
    assert runtime._terminal_journal().read(case.uid,"OBSERVED")==original
    assert runtime._terminal_journal().read(case.uid,"COMMITTED") is None


def test_session_mutable_progress_is_not_an_identity_drift(case,monkeypatch):
    case.start()
    begin=disconnect_begin(monkeypatch,case.engine)
    finish(case)
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text(f"UPDATE {daily.SESSION_TABLE} SET latest_generation=7,updated_at=:later"),{"later":case.now+timedelta(seconds=1)})
    finish(case)
    assert rows(case)[0]["status"]=="success"


def test_missing_original_history_never_creates_failed_finalization(case,monkeypatch):
    case.start()
    begin=disconnect_begin(monkeypatch,case.engine)
    finish(case)
    journal=runtime._terminal_journal()
    original=journal.read(case.uid,"OBSERVED")
    monkeypatch.setattr(case.engine,"begin",begin)
    with case.engine.begin() as conn:
        conn.execute(text("DELETE FROM st_scheduled_task_history WHERE run_uid=:uid"),{"uid":case.uid})
    finish(case)
    assert journal.read(case.uid,"OBSERVED")==original
    assert journal.read(case.uid,"REJECTED") is None
    assert journal.read(case.uid,"FAILED_FINALIZATION") is None
    assert case.uid in runtime._pending_terminal_writes


def test_unknown_activation_commit_reads_original_run_after_live_pool_changes(case,monkeypatch):
    case.start(task_type=sorted(runtime.ANALYSIS_POOL_PUBLISHER_TASK_TYPES)[0])
    with case.engine.begin() as conn:
        conn.execute(text("CREATE TABLE st_recommended_run_history "
                          "(run_uid TEXT PRIMARY KEY,publication_state TEXT,output_dataset_id TEXT)"))
        conn.execute(text("CREATE TABLE st_recommended_stocks (stock_code TEXT,run_uid TEXT,pool_state TEXT)"))
    activated=[]
    def activate(connection,*,run_uid,**kwargs):
        activated.append(run_uid)
        connection.execute(text("INSERT INTO st_recommended_run_history VALUES (:uid,'SUCCESS',:uid)"),{"uid":run_uid})
        connection.execute(text("INSERT INTO st_recommended_stocks VALUES ('MODEL',:uid,'ACTIVE')"),{"uid":run_uid})
        return {"schema":"MODEL-activation-receipt","run_uid":run_uid}
    monkeypatch.setattr(runtime,"_activate_analysis_strategy_pool",activate)
    begin=case.engine.begin
    @contextmanager
    def commit_then_disconnect():
        with begin() as conn:
            yield conn
        if rows(case)[0]["status"]!="running":
            raise OperationalError("COMMIT MODEL",{},ConnectionError("response lost"))
    monkeypatch.setattr(case.engine,"begin",commit_then_disconnect)
    finish(case)
    assert activated==[case.uid] and rows(case)[0]["status"]=="success"
    original_output=rows(case)[0]["output"]
    assert "MODEL-activation-receipt" in original_output
    assert case.uid in runtime._pending_terminal_writes
    monkeypatch.setattr(case.engine,"begin",begin)
    # A subsequent legal publisher can replace its date-scoped ACTIVE rows.
    with begin() as conn:
        conn.execute(text("DELETE FROM st_recommended_stocks"))
        conn.execute(text("INSERT INTO st_recommended_stocks VALUES ('LATER',:uid,'ACTIVE')"),{"uid":"d"*32})
    monkeypatch.setattr(runtime,"_activate_analysis_strategy_pool",lambda *a,**k:pytest.fail("repeated activation"))
    runtime._retry_pending_terminal_writes(case.engine)
    assert case.uid not in runtime._pending_terminal_writes
    assert runtime._terminal_journal().read(case.uid,"COMMITTED") is not None
    assert rows(case)[0]["output"]==original_output
