from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from server.common import scheduler_tasks


def _complete_columns() -> set[str]:
    return set(scheduler_tasks.SCHEDULER_REQUIRED_COLUMNS)


def test_runtime_ensure_is_read_only_and_returns_existing_surface():
    engine = MagicMock()
    columns = _complete_columns() | {"legacy_extra"}
    with patch.object(scheduler_tasks, "table_columns", return_value=columns):
        assert scheduler_tasks.ensure_scheduler_columns(engine) == columns

    engine.begin.assert_not_called()
    source = inspect.getsource(scheduler_tasks.ensure_scheduler_columns).upper()
    assert "ALTER TABLE" not in source
    assert "CREATE TABLE" not in source


def test_runtime_ensure_fails_closed_when_release_migration_was_not_run():
    engine = MagicMock()
    columns = _complete_columns() - {"last_run_output"}
    with patch.object(scheduler_tasks, "table_columns", return_value=columns):
        with pytest.raises(RuntimeError, match="missing_columns"):
            scheduler_tasks.ensure_scheduler_columns(engine)
    engine.begin.assert_not_called()


def test_privileged_migrator_adds_only_missing_extension_columns():
    engine = MagicMock()
    before = _complete_columns() - {"last_run_output", "description"}
    after = _complete_columns()
    with patch.object(
        scheduler_tasks,
        "table_columns",
        side_effect=[before, after],
    ):
        result = scheduler_tasks.privileged_migrate_scheduler_task_columns(engine)

    sql = "\n".join(
        str(call.args[0])
        for call in engine.begin.return_value.__enter__.return_value.execute.call_args_list
    ).upper()
    assert sql.count("ALTER TABLE") == 2
    assert "LAST_RUN_OUTPUT" in sql
    assert "DESCRIPTION" in sql
    assert result["added_columns"] == ("description", "last_run_output")
    assert result["required_surface_verified"] is True


def test_privileged_migrator_never_fabricates_missing_base_schema():
    engine = MagicMock()
    with patch.object(
        scheduler_tasks,
        "table_columns",
        return_value=_complete_columns() - {"id"},
    ):
        with pytest.raises(RuntimeError, match="base schema is incompatible"):
            scheduler_tasks.privileged_migrate_scheduler_task_columns(engine)
    engine.begin.assert_not_called()


def test_custom_definition_rejects_sql_fragments():
    with pytest.raises(ValueError, match="unsafe scheduler column definition"):
        scheduler_tasks._scheduler_column_definitions(
            {"unsafe_column": "TEXT; DROP TABLE st_scheduled_tasks"}
        )
