# -*- coding: utf-8 -*-
import sys
from unittest.mock import patch

import pandas as pd

from tools import export_db


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def execute(self, sql):
        statement = str(sql)
        if "SHOW TABLES" in statement:
            return _FakeResult([("stock_table",)])
        if "SHOW CREATE TABLE" in statement:
            return _FakeResult([("stock_table", "CREATE TABLE `stock_table` (`txt` text)")])
        raise AssertionError(f"unexpected sql: {statement}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def connect(self):
        return _FakeConnection()


def test_export_db_uses_configurable_output_and_quoted_names(tmp_path):
    output = tmp_path / "dump.sql"
    frame = pd.DataFrame([{"id": 1, "txt": "a'b"}])

    with patch.object(sys, "argv", ["export_db.py", "--output", str(output)]), patch(
        "tools.export_db.create_tool_engine",
        return_value=_FakeEngine(),
    ), patch("tools.export_db.read_frame", return_value=frame):
        export_db.main()

    text = output.read_text(encoding="utf-8")
    assert "INSERT INTO `stock_table` (`id`,`txt`) VALUES (1,'a\\'b');" in text
