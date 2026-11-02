"""Release only changed contracts; preserve untouched production task state."""
from copy import deepcopy
import json

import pytest
from sqlalchemy import create_engine, event, text

from tools import ensure_quality_gate as gate
from tools import scheduler_task_release as release


PREVIOUS = "a2edb2a60e415e9379cf2b33a6ac2bd759ef6149"
TARGET = "921b76c6d24e260ea26042fb0fd58decafba6709"


@pytest.fixture
def database(tmp_path):
    engine = create_engine("sqlite+pysqlite:///" + str(tmp_path / "scheduler.sqlite"))
    @event.listens_for(engine, "connect")
    def clock(connection, _record):
        connection.create_function("NOW", 0, lambda: "2026-11-02 23:55:00")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE st_scheduled_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_name TEXT NOT NULL,
            task_type TEXT NOT NULL UNIQUE, group_name TEXT, script_path TEXT,
            script_args TEXT, date_param_desc TEXT, cron_time TEXT,
            interval_minutes INTEGER, enabled INTEGER, description TEXT,
            sort_order INTEGER, date_param TEXT, updated_at DATETIME,
            created_at DATETIME, etl_sync_at DATETIME, last_triggered_at DATETIME,
            last_run_output TEXT, last_run_duration INTEGER, last_run_status TEXT,
            last_run_at DATETIME)"""))
        conn.execute(text("""CREATE TABLE st_scheduled_task_history (
            id INTEGER PRIMARY KEY, task_id INTEGER, run_uid TEXT,
            status TEXT, output TEXT)"""))
        conn.execute(text("INSERT INTO st_scheduled_task_history VALUES "
                          "(80238,29,'b2754bf13e8b4f97a6381682bc10c858','success','immutable audit')"))
    yield engine
    engine.dispose()


def _tasks():
    return [{key: deepcopy(task[key]) for key in gate.TASK_PAYLOAD_COLUMNS}
            for task in gate.TASKS]


def _task(tasks, task_type):
    return next(task for task in tasks if task["task_type"] == task_type)


def _insert(engine, task, identity):
    row = {key: value for key, value in task.items() if key in gate.TASK_PAYLOAD_COLUMNS}
    row.update(id=identity, date_param_desc="frozen runtime description",
        created_at="2026-05-11 00:00:00", updated_at="2026-11-02 08:35:01",
        etl_sync_at="2026-05-11 00:37:42", last_triggered_at="2026-11-02 08:30:33",
        last_run_at="2026-11-02 08:30:33", last_run_status="success",
        last_run_duration=266, last_run_output="\n---STDERR---\n")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO st_scheduled_tasks (" + ",".join(row) + ") VALUES (" +
                          ",".join(":" + key for key in row) + ")"), row)


def _seed(engine, *, finance=None, briefing=None):
    tasks = _tasks()
    for index, task in enumerate(task for task in tasks
                                 if task["task_type"] in gate.REVIEW_DELIVERY_TASK_TYPES):
        if task["task_type"] == "news_daily":
            candidate = deepcopy(briefing or task)
            candidate.update(enabled=0, script_args="--push")
            _insert(engine, candidate, 29)
        else:
            _insert(engine, task, 201 + index)
    _insert(engine, finance or _task(tasks, "stock_finance"), 123)


def _row_bytes(engine, task_type="news_daily"):
    with engine.connect() as conn:
        row = dict(conn.execute(text("SELECT * FROM st_scheduled_tasks WHERE task_type=:kind"),
                                {"kind": task_type}).mappings().one())
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _history(engine):
    with engine.connect() as conn:
        return [tuple(row) for row in conn.execute(text("SELECT * FROM st_scheduled_task_history ORDER BY id"))]


def _mutations(engine):
    statements = []
    @event.listens_for(engine, "before_cursor_execute")
    def observe(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().split(None, 1)[0].upper() in {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "REPLACE"}:
            statements.append(statement)
    return statements


def test_identical_all_task_contracts_are_noop_through_prepare_activate_validate_rollback(database):
    _seed(database)
    before, history = _row_bytes(database), _history(database)
    plan = release.build_plan(_tasks(), _tasks(), previous_sha=PREVIOUS, target_sha=TARGET)
    assert plan["changed_task_types"] == plan["new_task_types"] == plan["retired_task_types"] == []
    assert plan["upsert_task_types"] == []
    mutations = _mutations(database)
    snapshot = release.capture_before(database, plan)
    assert release.apply_plan(database, plan)["status"] == "NOOP"
    release.validate_applied_plan(database, plan)
    release.restore_before(database, plan, snapshot)
    assert mutations == []
    assert _row_bytes(database) == before
    assert _history(database) == history


def test_changed_finance_contract_never_rewrites_or_globally_validates_unchanged_briefing(database):
    target, previous = _tasks(), _tasks()
    _task(previous, "stock_finance")["script_args"] = "--old-finance-contract"
    _seed(database, finance=_task(previous, "stock_finance"))
    before, history = _row_bytes(database), _history(database)
    plan = release.build_plan(previous, target, previous_sha=PREVIOUS, target_sha=TARGET)
    assert plan["upsert_task_types"] == ["stock_finance"]
    release.capture_before(database, plan)
    release.apply_plan(database, plan)
    release.validate_applied_plan(database, plan)
    assert _row_bytes(database) == before
    assert _history(database) == history
    assert gate.validate_managed_task_contracts(database, task_types={"stock_finance"}) == {"stock_finance": "validated"}
    # The manual full audit remains strict. The release was scoped by real
    # contract content; it did not redefine disabled/--push as globally valid.
    with pytest.raises(RuntimeError, match="news_daily drifted fields: enabled, script_args"):
        gate.validate_review_delivery(database)


@pytest.mark.parametrize("field,value", [("enabled", 0), ("script_args", "--push")])
def test_genuinely_changed_review_contract_is_migrated_then_strictly_validated(database, field, value):
    target, previous = _tasks(), _tasks()
    _task(previous, "news_daily")["cron_time"] = "08:29"
    _seed(database, briefing=_task(previous, "news_daily"))
    history = _history(database)
    plan = release.build_plan(previous, target, previous_sha=PREVIOUS, target_sha=TARGET)
    assert plan["upsert_task_types"] == ["news_daily"]
    release.capture_before(database, plan)
    release.apply_plan(database, plan)
    release.validate_applied_plan(database, plan)
    with database.connect() as conn:
        actual = dict(conn.execute(text("SELECT * FROM st_scheduled_tasks WHERE id=29")).mappings().one())
    expected = _task(target, "news_daily")
    assert {key: actual[key] for key in gate.TASK_PAYLOAD_COLUMNS} == {
        key: expected[key] for key in gate.TASK_PAYLOAD_COLUMNS}
    assert _history(database) == history
    with database.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_tasks SET " + field + "=:value WHERE id=29"), {"value": value})
    with pytest.raises(RuntimeError, match="news_daily drifted fields: " + field):
        release.validate_applied_plan(database, plan)


def test_readonly_global_deadlines_allow_real_bash_scheduler_capture_apply_validate_restore(tmp_path):
    """Exercise actual shell wrappers and Python NOOP plans without a database."""
    from pathlib import Path
    import re
    import shlex
    import shutil
    import site
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    bash = shutil.which("bash") or str(Path(r"C:\Program Files\Git\bin\bash.exe"))
    if not Path(bash).is_file():
        pytest.skip("Bash required for the production readonly-variable regression")
    source = (root / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    names = (
        "controlled_guard_run_service_gate_with_deadline",
        "run_prepared_scheduler_task_release",
        "capture_and_apply_scheduler_task_release",
        "validate_prepared_scheduler_task_release",
        "controlled_guard_restore_scheduler_task_plan",
    )
    functions = []
    for name in names:
        match = re.search(rf"(?ms)^{name}\(\) \{{\n(.*?)^\}}$", source)
        assert match is not None
        body = match.group(0)
        if name == "controlled_guard_run_service_gate_with_deadline":
            # Run as the test account; timeout still owns the real child.
            body = body.replace('test -x /usr/bin/sudo || return 1', 'true')
            body = body.replace('/usr/bin/sudo -u "$service_user" ', '')
            body = body.replace('/usr/bin/timeout --signal=TERM', 'record_timeout --signal=TERM')
        functions.append(body)

    target, previous = "b" * 40, "a" * 40
    code = tmp_path / "code" / target
    artifacts = tmp_path / "artifacts" / target
    venv = tmp_path / "venvs" / target
    (code / "tools").mkdir(parents=True)
    artifacts.mkdir(parents=True)
    (venv / "bin").mkdir(parents=True)
    helper = root / "tools/scheduler_task_release.py"
    shutil.copyfile(helper, code / "tools/scheduler_task_release.py")
    (venv / ".adata.gitsha").write_text("c" * 40, encoding="utf-8")
    (venv / ".adata.tree.sha256").write_text("d" * 64, encoding="utf-8")
    q = lambda value: shlex.quote(str(value).replace("\\", "/"))
    # Delegate the fixture's immutable venv to the installed test interpreter;
    # keep the production arguments and env-isolation boundary intact.
    (venv / "bin/python").write_text(
        "#!/usr/bin/bash\nexport PYTHONUSERBASE=" + q(site.getuserbase()) + "\nexec "
        + q(sys.executable) + " -P " + q(helper) + ' "${@:3}"\n', encoding="utf-8", newline="\n")
    contracts = [{key: task[key] for key in release.FIELDS} for task in gate.TASKS]
    plan = release.build_plan(contracts, contracts, previous_sha=previous, target_sha=target)
    plan_path = artifacts / f"scheduler-task-release-{previous}.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    before_path = artifacts / f"scheduler-task-before-{previous}.json"
    trace = tmp_path / "timeout-trace"
    harness = f'''set -Eeuo pipefail
readonly CONTROLLED_DATABASE_GATE_TIMEOUT=30m
readonly CONTROLLED_DATABASE_GATE_KILL_AFTER=30s
SERVICE_USER=test-user
EXPECTED_SHA={target}
PREPARED_CODE_ROOT={q(code)}
CODE_RELEASE_ROOT={q(tmp_path / 'code')}
RELEASE_VENV_ROOT={q(tmp_path / 'venvs')}
RELEASE_ARTIFACT_ROOT={q(tmp_path / 'artifacts')}
ADATA_RUNTIME_ROOT={q(tmp_path / 'adata')}
ADATA_SOURCE=/unused-adata
EXPECTED_ADATA_SHA={'c' * 40}
EXPECTED_ADATA_TREE_SHA256={'d' * 64}
BOOTSTRAP_PYTHON={q(sys.executable)}
SCHEDULER_TASK_PLAN_FILE={q(plan_path)}
SCHEDULER_TASK_BEFORE_FILE={q(before_path)}
RECEIPT_ID=readonly-deadline-regression
TRACE={q(trace)}
activation_snapshot_old_release() {{ printf '%s\\n' {previous}; }}
controlled_guard_assert_file() {{ test -f "$1" && test ! -L "$1"; }}
git() {{ printf '%s\\n' "$EXPECTED_SHA"; }}
sync() {{ return 0; }}
install() {{
  local -a args=()
  while [ "$#" -gt 0 ]; do
    case "$1" in -o|-g) shift 2 ;; *) args+=("$1"); shift ;; esac
  done
  command install "${{args[@]}}"
}}
record_timeout() {{
  printf '%s,%s,%s\\n' "$1" "$2" "$3" >> "$TRACE"
  /usr/bin/timeout "$@"
}}
{chr(10).join(functions)}
chmod +x "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python"
capture_and_apply_scheduler_task_release
validate_prepared_scheduler_task_release
controlled_guard_restore_scheduler_task_plan "$EXPECTED_SHA"
test "$CONTROLLED_DATABASE_GATE_TIMEOUT" = 30m
test "$CONTROLLED_DATABASE_GATE_KILL_AFTER" = 30s
printf 'READONLY_GLOBALS_PRESERVED\\n'
'''
    harness_path = tmp_path / "readonly-scheduler-release.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run([bash, str(harness_path)], capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=40)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "readonly variable" not in completed.stderr
    assert "READONLY_GLOBALS_PRESERVED" in completed.stdout
    assert '"status":"NOOP"' in completed.stdout
    assert json.loads(before_path.read_text(encoding="utf-8"))["rows"] == {}
    assert trace.read_text(encoding="utf-8").splitlines() == [
        "--signal=TERM,--kill-after=5s,90s"] * 4
