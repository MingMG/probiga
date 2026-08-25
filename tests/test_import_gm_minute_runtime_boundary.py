from __future__ import annotations

import inspect

from tools import import_gm_minute


class _Cursor:
    def __init__(self, *, engine="InnoDB", columns=()):
        self.engine = engine
        self.columns = list(columns)
        self.query_count = 0

    def execute(self, sql, params):
        upper = str(sql).upper()
        assert not any(token in upper for token in (
            "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "TRUNCATE TABLE",
        ))
        assert params == ("sm_stock_minute_gm",)
        self.query_count += 1

    def fetchone(self):
        return None if self.engine is None else (self.engine,)

    def fetchall(self):
        return [(column,) for column in self.columns]


def test_import_runtime_guard_is_read_only_and_accepts_complete_surface():
    cursor = _Cursor(columns=import_gm_minute._REQUIRED_COLUMNS)
    import_gm_minute._validate_runtime_table(cursor)
    assert cursor.query_count == 2


def test_import_source_contains_no_persistent_ddl_executor():
    source = inspect.getsource(import_gm_minute)
    assert "def _run_ddl" not in source
    assert "DDL_PATH" not in source
