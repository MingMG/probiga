"""Trust boundaries of a content-scoped scheduler release and its rollback."""
from copy import deepcopy

import pytest

from tools import ensure_quality_gate as gate
from tools import scheduler_task_release as release
from sqlalchemy import create_engine, event, text


PREVIOUS = "a" * 40
TARGET = "b" * 40


def _task(task_type):
    return deepcopy(next(task for task in gate.TASKS if task["task_type"] == task_type))


def _plan(before, after):
    return release.build_plan(before, after, previous_sha=PREVIOUS, target_sha=TARGET)


class NoDatabase:
    def __getattribute__(self, name):
        raise AssertionError("empty or invalid plan attempted database access: " + name)


def test_unrelated_repository_revision_does_not_release_unchanged_notifications():
    review, notice = _task("daily_review"), _task("notice_eastmoney")
    plan = _plan([review, notice], [deepcopy(notice), deepcopy(review)])
    assert plan["previous_sha"] == PREVIOUS and plan["target_sha"] == TARGET
    assert plan["base_task_sha256"] == plan["target_task_sha256"]
    for key in ("changed_task_types", "new_task_types", "retired_task_types", "upsert_task_types"):
        assert plan[key] == []


def test_empty_plan_and_rollback_do_not_even_inspect_database_schema():
    contracts = _all_contracts()
    plan = _plan(contracts, deepcopy(contracts))
    engine = NoDatabase()
    before = release.capture_before(engine, plan)
    release.apply_plan(engine, plan)
    release.validate_applied_plan(engine, plan)
    release.restore_before(engine, plan, before)


def test_empty_plan_still_requires_the_actual_target_source_contract():
    plan = _plan([_task("daily_review")], [_task("daily_review")])
    with pytest.raises(ValueError, match="TARGET_SOURCE_CONTRACT"):
        release.apply_plan(NoDatabase(), plan)


def test_rollback_before_image_cannot_be_rebound_to_another_release_plan():
    contracts = _all_contracts()
    plan = _plan(contracts, deepcopy(contracts))
    before = release.capture_before(NoDatabase(), plan)
    other = release.build_plan(contracts, contracts, previous_sha="c" * 40, target_sha=TARGET)
    with pytest.raises(ValueError, match="BEFORE_IMAGE"):
        release.restore_before(NoDatabase(), other, before)


def test_actual_executor_build_cannot_consume_another_targets_valid_plan(monkeypatch):
    contracts = _all_contracts()
    plan = _plan(contracts, deepcopy(contracts))
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", "c" * 40)
    with pytest.raises(ValueError, match="TARGET_BUILD_DIFFERS"):
        release.apply_plan(NoDatabase(), plan)


def test_changed_new_and_retired_are_disjoint_exact_content_sets():
    review, notice, minute = (_task(kind) for kind in (
        "daily_review", "notice_eastmoney", "qmt_stock_minute_canonical"))
    changed = {**notice, "script_args": notice["script_args"] + " --json"}
    created = {**minute, "task_name": "new exact publisher", "task_type": "new_exact_publisher"}
    plan = _plan([review, notice, minute], [review, changed, created])
    assert plan["changed_task_types"] == ["notice_eastmoney"]
    assert plan["new_task_types"] == ["new_exact_publisher"]
    assert plan["retired_task_types"] == ["qmt_stock_minute_canonical"]
    assert set(plan["upsert_task_types"]) == {"notice_eastmoney", "new_exact_publisher"}
    assert "daily_review" not in plan["upsert_task_types"]
    assert set(plan["base_tasks"]) == {"daily_review", "notice_eastmoney", "qmt_stock_minute_canonical"}


def test_definition_order_does_not_change_the_plan_identity():
    before = [_task("daily_review"), _task("notice_eastmoney")]
    after = [before[0], {**before[1], "cron_time": "20:01"}]
    forward = _plan(before, after)
    reverse = _plan(list(reversed(before)), list(reversed(after)))
    assert forward == reverse


@pytest.mark.parametrize("bad", ["", "b" * 39, "g" * 40, "0" * 40, "../main"])
@pytest.mark.parametrize("field", ["previous_sha", "target_sha"])
def test_release_requires_real_immutable_build_identities_before_planning(field, bad):
    identities = {"previous_sha": PREVIOUS, "target_sha": TARGET, field: bad}
    with pytest.raises((ValueError, RuntimeError)):
        release.build_plan([_task("daily_review")], [_task("daily_review")], **identities)


def test_conflicting_duplicate_task_type_cannot_silently_overwrite_one_contract():
    review = _task("daily_review")
    conflict = {**review, "script_args": "--push"}
    if conflict == review:
        conflict["script_args"] = "--no-push"
    with pytest.raises((ValueError, RuntimeError)):
        _plan([review], [review, conflict])


@pytest.mark.parametrize("key,value", [
    ("upsert_task_types", None),
    ("upsert_task_types", ["daily_review"]),
    ("changed_task_types", ["daily_review"]),
    ("target_sha", "c" * 40),
    ("plan_sha256", "0" * 64),
])
def test_tampered_empty_selection_fails_before_an_all_tasks_api_can_be_called(key, value):
    plan = _plan([_task("daily_review")], [_task("daily_review")])
    plan[key] = value
    with pytest.raises((ValueError, RuntimeError)):
        release.apply_plan(NoDatabase(), plan)


def test_changed_contract_content_is_covered_by_the_plan_digest():
    review = _task("daily_review")
    plan = _plan([review], [{**review, "script_args": "--target-date 2026-11-02"}])
    plan["target_tasks"]["daily_review"]["script_args"] = "--push"
    with pytest.raises((ValueError, RuntimeError)):
        release.validate_applied_plan(NoDatabase(), plan)


@pytest.fixture
def scheduler_database(tmp_path):
    engine = create_engine("sqlite+pysqlite:///" + str(tmp_path / "rollback.sqlite"))
    @event.listens_for(engine, "connect")
    def clock(connection, _record):
        connection.create_function("NOW", 0, lambda: "2026-11-03 00:10:00")
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE st_scheduled_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_name TEXT NOT NULL,
            task_type TEXT NOT NULL UNIQUE, group_name TEXT, script_path TEXT,
            script_args TEXT, date_param_desc TEXT, cron_time TEXT,
            interval_minutes INTEGER, enabled INTEGER, description TEXT,
            sort_order INTEGER, date_param TEXT, updated_at DATETIME,
            created_at DATETIME, etl_sync_at DATETIME, last_triggered_at DATETIME,
            last_run_output TEXT, last_run_duration INTEGER, last_run_status TEXT,
            last_run_at DATETIME)"""))
    yield engine
    engine.dispose()


def _all_contracts():
    return [{key: deepcopy(task[key]) for key in release.FIELDS} for task in gate.TASKS]


def test_paused_qmt_contract_can_roll_back_without_rewinding_new_runtime_audit(scheduler_database):
    target = _all_contracts()
    previous = deepcopy(target)
    old = next(task for task in previous if task["task_type"] == "qmt_stock_minute_canonical")
    old["script_args"] = "--old-exact-contract"
    paused = {**old, "enabled": 0, "id": 112}
    with scheduler_database.begin() as connection:
        connection.execute(text("INSERT INTO st_scheduled_tasks (" + ",".join(paused)
            + ") VALUES (" + ",".join(":" + key for key in paused) + ")"), paused)
    plan = _plan(previous, target)
    before = release.capture_before(scheduler_database, plan)
    release.apply_plan(scheduler_database, plan)
    with scheduler_database.begin() as connection:
        connection.execute(text("UPDATE st_scheduled_tasks SET last_run_status='failed',"
                                "last_run_output='new immutable execution outcome' WHERE id=112"))
    release.restore_before(scheduler_database, plan, before)
    release.restore_before(scheduler_database, plan, before)
    with scheduler_database.connect() as connection:
        row = dict(connection.execute(text("SELECT * FROM st_scheduled_tasks WHERE id=112")).mappings().one())
    assert row["enabled"] == 0 and row["script_args"] == "--old-exact-contract"
    assert row["last_run_status"] == "failed"
    assert row["last_run_output"] == "new immutable execution outcome"


def test_new_task_rollback_disables_once_and_is_safe_to_repeat(scheduler_database):
    target = _all_contracts()
    previous = [task for task in target if task["task_type"] != "stock_finance"]
    plan = _plan(previous, target)
    before = release.capture_before(scheduler_database, plan)
    release.apply_plan(scheduler_database, plan)
    with scheduler_database.connect() as connection:
        identity = connection.execute(text("SELECT id FROM st_scheduled_tasks WHERE task_type='stock_finance'")).scalar_one()
    release.restore_before(scheduler_database, plan, before)
    release.restore_before(scheduler_database, plan, before)
    with scheduler_database.connect() as connection:
        row = dict(connection.execute(text("SELECT id,enabled FROM st_scheduled_tasks WHERE task_type='stock_finance'")).mappings().one())
    assert row == {"id": identity, "enabled": 0}


@pytest.mark.parametrize("external_drift", [False, True])
def test_retirement_only_disables_the_exact_old_contract_and_can_be_undone(
    scheduler_database, monkeypatch, external_drift,
):
    previous = _all_contracts()
    target = [task for task in previous if task["task_type"] != "stock_finance"]
    original = next(task for task in previous if task["task_type"] == "stock_finance")
    row = {**original, "id": 123}
    if external_drift:
        row["script_args"] = "--preserve-external-custom-contract"
    with scheduler_database.begin() as connection:
        connection.execute(text("INSERT INTO st_scheduled_tasks (" + ",".join(row)
            + ") VALUES (" + ",".join(":" + key for key in row) + ")"), row)
    monkeypatch.setattr(gate, "TASKS", target)
    plan = _plan(previous, target)
    before = release.capture_before(scheduler_database, plan)
    if external_drift:
        with pytest.raises(ValueError, match="RETIRED_IDENTITY_UNPROVEN"):
            release.apply_plan(scheduler_database, plan)
    else:
        release.apply_plan(scheduler_database, plan)
        with scheduler_database.connect() as connection:
            assert connection.execute(text("SELECT enabled FROM st_scheduled_tasks WHERE id=123")).scalar_one() == 0
        release.restore_before(scheduler_database, plan, before)
        release.restore_before(scheduler_database, plan, before)
    with scheduler_database.connect() as connection:
        actual = dict(connection.execute(text("SELECT * FROM st_scheduled_tasks WHERE id=123")).mappings().one())
    assert actual["enabled"] == original["enabled"]
    assert actual["script_args"] == row["script_args"]
