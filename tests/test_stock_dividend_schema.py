from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import CHAR, VARCHAR, DATE, DATETIME, LONGTEXT, BIGINT

from server.common import stock_dividend_schema as s


class Inspector:
    def __init__(self):
        types = {"CHAR": CHAR, "VARCHAR": VARCHAR, "DATE": DATE, "DATETIME": DATETIME, "LONGTEXT": LONGTEXT, "BIGINT": BIGINT}
        self.columns = {"sm_dividend": [{"name": n, "type": VARCHAR(64), "nullable": True} for n in s.BASE_COLUMNS]}
        for table, contracts in s.TYPE_CONTRACTS.items():
            self.columns.setdefault(table, [])
            for name, (kind, length, nullable) in contracts.items():
                self.columns[table].append({"name": name, "type": types[kind](length) if length else types[kind](), "nullable": nullable})
        self.unique = [("event_id",)]

    def has_table(self, table):
        return table in self.columns

    def get_columns(self, table):
        return self.columns[table]

    def get_indexes(self, table):
        return [{"column_names": cols, "unique": True} for cols in self.unique]

    def get_unique_constraints(self, table):
        return []

    def get_pk_constraint(self, table):
        return {"constrained_columns": list(s.AUDIT_PRIMARY_KEYS[table])}


def test_exact_storage_contract_passes_without_database_writes(monkeypatch):
    reader = Inspector()
    monkeypatch.setattr(s, "inspect", lambda engine: reader)
    assert s.validate_stock_dividend_schema(object())["status"] == "PASS"


@pytest.mark.parametrize("table,column,change", [
    ("sm_dividend", "event_id", {"type": CHAR(8)}),
    ("sm_dividend", "report_period", {"type": VARCHAR(10)}),
    ("sm_dividend", "assign_progress", {"type": VARCHAR(8)}),
    ("sm_dividend", "source_payload_json", {"type": VARCHAR(2048)}),
    ("sm_dividend_source_revision", "source_hash", {"nullable": True}),
    ("sm_dividend_source_snapshot", "manifest_json", {"type": VARCHAR(2048)}),
    ("sm_dividend_source_snapshot", "observed_at", {"nullable": True}),
])
def test_present_but_wrong_type_length_or_nullability_fails_closed(monkeypatch, table, column, change):
    reader = Inspector()
    next(row for row in reader.columns[table] if row["name"] == column).update(change)
    monkeypatch.setattr(s, "inspect", lambda engine: reader)
    with pytest.raises(RuntimeError, match="STORAGE_CONTRACT_DRIFT"):
        s.inspect_stock_dividend_schema(object())


def test_legacy_unique_is_not_silently_dropped(monkeypatch):
    reader = Inspector()
    reader.unique.append(("stock_code", "report_date"))
    monkeypatch.setattr(s, "inspect", lambda engine: reader)
    with pytest.raises(RuntimeError, match="LEGACY_UNIQUE_COLLAPSES"):
        s.inspect_stock_dividend_schema(object())


def test_migration_report_never_applies_or_hides_existing_type_drift(monkeypatch):
    reader = Inspector()
    reader.columns["sm_dividend"] = [row for row in reader.columns["sm_dividend"] if row["name"] != "event_id"]
    reader.columns.pop("sm_dividend_source_revision")
    reader.unique = []
    monkeypatch.setattr(s, "inspect", lambda engine: reader)
    plan = s.inspect_stock_dividend_schema(object())
    assert plan["status"] == "MIGRATION_REQUIRED"
    assert plan["missing_event_columns"] == ["event_id"]
    assert plan["missing_audit_tables"] == ["sm_dividend_source_revision"]


def task_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.exec_driver_sql("CREATE TABLE st_scheduled_tasks (id INTEGER PRIMARY KEY,task_name TEXT,task_type TEXT,script_path TEXT,script_args TEXT,enabled INTEGER,cron_time TEXT,last_run_status TEXT,last_run_at TEXT,last_run_duration INTEGER,last_run_output TEXT,last_triggered_at TEXT)")
        c.exec_driver_sql("CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY,task_id INTEGER,task_type TEXT,status TEXT,output TEXT,run_uid TEXT,host_name TEXT,scheduler_instance_id TEXT,build_sha TEXT)")
        c.exec_driver_sql("CREATE TABLE sm_dividend_task_cutover_audit (task_id INTEGER PRIMARY KEY,old_identity_hash TEXT,old_identity_json TEXT,old_projection_json TEXT,new_identity_json TEXT,migrated_at DATETIME)")
        c.execute(text("INSERT INTO st_scheduled_tasks VALUES (47,'old',:type,:script,'old args',0,'21:31','failed','2026-09-11 22:00:00',5,'old provider error','2026-09-11 22:00:00')"), {"type": s.OLD_TASK_TYPE, "script": s.OLD_SCRIPT})
        c.execute(text("INSERT INTO st_scheduled_task_history (id,task_id,task_type,status,output) VALUES (1,47,:type,'failed','old provider error')"), {"type": s.OLD_TASK_TYPE})
    return engine


@pytest.mark.parametrize("enabled", [0, 1])
def test_fenced_identity_cutover_keeps_task_id_schedule_history_and_retains_projection(enabled):
    db = task_engine()
    with db.begin() as c:
        c.execute(text("UPDATE st_scheduled_tasks SET enabled=:enabled"), {"enabled": enabled})
    assert s.inspect_stock_dividend_task_identity(db) == {"status": "MIGRATE", "task_id": 47}
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_task_cutover_audit")).scalar() == 0
    assert s.migrate_stock_dividend_task_identity(db)["task_id"] == 47
    with db.connect() as c:
        task = dict(c.execute(text("SELECT * FROM st_scheduled_tasks")).mappings().one())
        history = dict(c.execute(text("SELECT * FROM st_scheduled_task_history")).mappings().one())
        audit = dict(c.execute(text("SELECT * FROM sm_dividend_task_cutover_audit")).mappings().one())
    assert task["id"] == 47 and task["enabled"] == enabled and task["cron_time"] == "21:31"
    assert task["last_triggered_at"] == "2026-09-11 22:00:00"
    assert all(task[name] is None for name in s.PROJECTION_COLUMNS)
    assert all(task[key] == value for key, value in s.NEW_IDENTITY.items())
    assert history["task_type"] == s.OLD_TASK_TYPE and history["output"] == "old provider error"
    assert s.json.loads(audit["old_projection_json"])["last_run_output"] == "old provider error"
    assert s.migrate_stock_dividend_task_identity(db)["status"] == "PASS"
    from server.common.scheduler_task_retirement import retire_superseded_provider_tasks
    assert retire_superseded_provider_tasks(db)["retired_tasks"] == []
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM st_scheduled_tasks")).scalar() == 1
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_task_cutover_audit")).scalar() == 1
        assert c.execute(text("SELECT enabled FROM st_scheduled_tasks WHERE id=47")).scalar() == enabled


@pytest.mark.parametrize("kind", ["history", "projection", "duplicate", "wrong_script"])
def test_task_cutover_ambiguity_and_undrained_owners_leave_everything_unchanged(kind):
    db = task_engine()
    with db.begin() as c:
        if kind == "history":
            c.execute(text("UPDATE st_scheduled_task_history SET status='running'"))
        elif kind == "projection":
            c.execute(text("UPDATE st_scheduled_tasks SET last_run_status='running'"))
        elif kind == "duplicate":
            c.execute(text("INSERT INTO st_scheduled_tasks (id,task_type,script_path) VALUES (48,:t,:p)"), {"t": s.NEW_IDENTITY["task_type"], "p": s.NEW_IDENTITY["script_path"]})
        else:
            c.execute(text("UPDATE st_scheduled_tasks SET script_path='other.py'"))
    with pytest.raises(RuntimeError):
        s.migrate_stock_dividend_task_identity(db)
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_dividend_task_cutover_audit")).scalar() == 0
        assert c.execute(text("SELECT task_type FROM st_scheduled_tasks WHERE id=47")).scalar() == s.OLD_TASK_TYPE


def test_only_exact_absent_old_owner_allows_identity_cutover_without_completing_history(monkeypatch):
    db = task_engine()
    with db.begin() as c:
        c.execute(text("UPDATE st_scheduled_tasks SET last_run_status='running'"))
        c.execute(text("UPDATE st_scheduled_task_history SET status='running',run_uid=:r,host_name='host',scheduler_instance_id='host-123',build_sha=:b"), {"r":"a"*32,"b":"b"*40})
    evidence = {"owner_absent": True, "history_status_unchanged": True, "run_uid":"a"*32}
    calls = []
    monkeypatch.setattr(s, "_dead_scheduler_owner_evidence", lambda h: calls.append(h) or evidence)
    result = s.migrate_stock_dividend_task_identity(db)
    assert result["task_id"] == 47 and len(calls) == 1
    with db.connect() as c:
        assert c.execute(text("SELECT status FROM st_scheduled_task_history")).scalar() == "running"
        assert c.execute(text("SELECT last_run_status FROM st_scheduled_tasks")).scalar() is None
        audit = s.json.loads(c.execute(text("SELECT old_projection_json FROM sm_dividend_task_cutover_audit")).scalar())
    assert audit["last_run_status"] == "running" and audit["running_owner_evidence"] == evidence


@pytest.mark.parametrize("condition", ["absent", "alive", "foreign_host", "invalid_pid", "permission_unknown", "proc_alive"])
def test_dead_owner_evidence_requires_both_local_pid_and_proc_absence(monkeypatch, condition):
    from server.api import scheduler_runtime
    monkeypatch.setattr(s, "os", SimpleNamespace(name="posix"))
    monkeypatch.setattr(s, "gethostname", lambda: "host")
    monkeypatch.setattr(scheduler_runtime, "_owner_pid_is_absent", lambda *a, **k: condition != "alive")
    history = {"host_name": "host", "scheduler_instance_id": "host-123", "task_type": s.OLD_TASK_TYPE,
               "run_uid": "a"*32, "build_sha": "b"*40}
    if condition == "foreign_host": history["host_name"] = "other"
    if condition == "invalid_pid": history["scheduler_instance_id"] = "host-0"
    def stat(path):
        if path == "/proc/self/status": return object()
        if condition == "permission_unknown": raise PermissionError(13, "denied", path)
        if condition == "proc_alive": return object()
        raise FileNotFoundError(2, "absent", path)
    monkeypatch.setattr(s, "Path", lambda path: SimpleNamespace(stat=lambda: stat(path)))
    if condition == "absent":
        assert s._dead_scheduler_owner_evidence(history)["owner_absent"] is True
    else:
        with pytest.raises(RuntimeError, match="NOT_PROVEN_ABSENT"):
            s._dead_scheduler_owner_evidence(history)
