"""Path provisioning must preserve SQLite's process-wide POSIX locks."""
import os
import sqlite3
import subprocess
import sys

import pytest

from server.common.sector_heatmap_store import _path


@pytest.mark.skipif(os.name != "posix", reason="requires real POSIX SQLite locking")
def test_existing_store_path_preserves_active_transaction_lock(tmp_path):
    tmp_path.chmod(0o700)
    path = _path(create=True, root=tmp_path)
    child = """
import sqlite3, sys
db = sqlite3.connect(sys.argv[1], timeout=.05)
try:
    db.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as exc:
    assert str(exc) == 'database is locked'
    print('LOCKED')
else:
    print('SECOND_WRITER')
finally:
    db.close()
"""
    def writer():
        return subprocess.run([sys.executable, "-c", child, str(path)],
                              capture_output=True, text=True, timeout=3, check=True).stdout.strip()

    db = sqlite3.connect(path)
    try:
        db.execute("CREATE TABLE value (n INTEGER)")
        db.commit()
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO value VALUES (1)")
        assert writer() == "LOCKED"
        assert _path(create=True, root=tmp_path) == path
        assert writer() == "LOCKED"
        db.commit()
        assert writer() == "SECOND_WRITER"
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("SELECT n FROM value").fetchall() == [(1,)]
    finally:
        db.close()
