from __future__ import annotations

import inspect

from scripts import build_concept_map


def test_concept_map_replacement_is_atomic_and_has_no_destructive_ddl():
    source = inspect.getsource(build_concept_map.build_concept_map)
    upper = source.upper()

    assert "REPLACE_TABLE_ROWS" in upper
    assert "TRUNCATE TABLE" not in upper
    assert "CREATE TABLE" not in upper
    assert "DROP TABLE" not in upper
    assert "ALTER TABLE" not in upper
