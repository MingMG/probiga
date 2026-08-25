from __future__ import annotations

import inspect

from server.common import hot_rank_schema
from tools import (
    fetch_hot_pop_rank_east,
    fetch_hot_rank_sina,
    fetch_hot_rank_ths,
    fetch_hot_rank_xq,
)


def _source(function) -> str:
    return inspect.getsource(function).upper()


def test_all_hot_rank_runtime_guards_are_read_only():
    for function in (
        fetch_hot_rank_ths._ensure_snapshot_date_column,
        fetch_hot_pop_rank_east._ensure_snapshot_date_column,
        fetch_hot_rank_xq._ensure_snapshot_date_column,
        fetch_hot_rank_sina._run_ddl,
    ):
        source = _source(function)
        assert "VALIDATE_HOT_RANK_RUNTIME_SCHEMA" in source
        assert "CREATE TABLE" not in source
        assert "ALTER TABLE" not in source


def test_xueqiu_runtime_fetch_never_creates_or_alters_tables():
    source = _source(fetch_hot_rank_xq.fetch_hot_rank_xq)
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "REPLACE_TABLE_ROWS" in source


def test_other_rank_writers_replace_each_date_atomically():
    for function in (
        fetch_hot_rank_ths.fetch_hot_rank_ths,
        fetch_hot_pop_rank_east.fetch_hot_pop_rank_east,
        fetch_hot_rank_sina.fetch_hot_rank_sina,
    ):
        source = _source(function)
        assert "REPLACE_TABLE_ROWS" in source
        assert "TRUNCATE TABLE" not in source


def test_release_migration_is_the_only_hot_rank_schema_writer():
    source = _source(hot_rank_schema.privileged_migrate_hot_rank_schema)
    assert "CREATE" in source
    assert "ALTER TABLE" in source
    assert set(hot_rank_schema.HOT_RANK_REQUIRED_COLUMNS) == {
        "st_hot_rank_ths",
        "st_hot_pop_rank_east",
        "st_hot_rank_xq",
        "st_hot_rank_sina",
    }


def test_runtime_validator_has_no_schema_mutation():
    source = _source(hot_rank_schema.validate_hot_rank_runtime_schema)
    assert "VALIDATE_REQUIRED_TABLE_SURFACE" in source
    assert "CREATE" not in source
    assert "ALTER" not in source
