from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from tools.sanitize_mysql55_dump_for_mysql84 import (
    sanitize_dump,
    sanitize_dump_line,
    split_large_insert_line,
)


def test_sanitize_dump_line_only_removes_exact_mode_from_versioned_statement():
    source = (
        b"/*!50003 SET sql_mode = "
        b"'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;\r\n"
    )

    sanitized, removed = sanitize_dump_line(source)

    assert removed == 1
    assert sanitized == (
        b"/*!50003 SET sql_mode = "
        b"'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;\r\n"
    )


def test_sanitize_dump_line_supports_plain_session_statement_case_insensitively():
    source = b" set SESSION SQL_MODE = 'no_auto_create_user,STRICT_TRANS_TABLES';\n"

    sanitized, removed = sanitize_dump_line(source)

    assert removed == 1
    assert sanitized == b" set SESSION SQL_MODE = 'STRICT_TRANS_TABLES';\n"


@pytest.mark.parametrize(
    "line",
    (
        b"INSERT INTO audit_log VALUES ('NO_AUTO_CREATE_USER');\n",
        b"-- SET sql_mode = 'NO_AUTO_CREATE_USER';\n",
        b"SET @note = 'NO_AUTO_CREATE_USER';\n",
        b"/*!50003 SET sql_mode = 'STRICT_TRANS_TABLES' */ ;\n",
    ),
)
def test_sanitize_dump_line_never_changes_unrelated_content(line):
    assert sanitize_dump_line(line) == (line, 0)


def test_stream_sanitize_preserves_unrelated_bytes_and_reports_hashes(tmp_path: Path):
    source = tmp_path / "source.sql"
    output = tmp_path / "target.sql"
    payload = (
        b"SET NAMES utf8mb4;\n"
        b"/*!50003 SET sql_mode = 'NO_AUTO_CREATE_USER,STRICT_TRANS_TABLES' */ ;\n"
        b"INSERT INTO t VALUES (0x00FF, 'NO_AUTO_CREATE_USER');\n"
        b"SET sql_mode='NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER';\n"
    )
    expected = (
        b"SET NAMES utf8mb4;\n"
        b"/*!50003 SET sql_mode = 'STRICT_TRANS_TABLES' */ ;\n"
        b"INSERT INTO t VALUES (0x00FF, 'NO_AUTO_CREATE_USER');\n"
        b"SET sql_mode='NO_ENGINE_SUBSTITUTION';\n"
    )
    source.write_bytes(payload)

    report = sanitize_dump(source, output, expected_changed_statements=2)

    assert source.read_bytes() == payload
    assert output.read_bytes() == expected
    assert report.changed_statements == 2
    assert report.removed_tokens == 2
    assert report.source_bytes == len(payload)
    assert report.output_bytes == len(expected)
    assert report.source_sha256 == hashlib.sha256(payload).hexdigest()
    assert report.output_sha256 == hashlib.sha256(expected).hexdigest()


def test_expected_count_failure_does_not_publish_partial_output(tmp_path: Path):
    source = tmp_path / "source.sql"
    output = tmp_path / "target.sql"
    source.write_bytes(b"SET sql_mode='NO_AUTO_CREATE_USER';\n")

    with pytest.raises(ValueError, match="expected 2, got 1"):
        sanitize_dump(source, output, expected_changed_statements=2)

    assert not output.exists()
    assert list(tmp_path.glob(".*.partial-*")) == []


def test_source_cannot_be_overwritten(tmp_path: Path):
    source = tmp_path / "source.sql"
    source.write_bytes(b"SELECT 1;\n")

    with pytest.raises(ValueError, match="different files"):
        sanitize_dump(source, source, overwrite=True)


def test_large_insert_is_split_only_at_tuple_boundaries():
    line = b"INSERT INTO t VALUES (1,'a,b'),(2,'escaped\\\'quote'),(3,4);\n"

    pieces, chunks = split_large_insert_line(line, max_bytes=45)

    assert chunks == len(pieces) == 3
    assert all(piece.startswith(b"INSERT INTO t VALUES ") for piece in pieces)
    assert all(piece.endswith(b";\n") for piece in pieces)
    assert b"(1,'a,b')" in pieces[0]
    assert b"(2,'escaped\\\'quote')" in pieces[1]
    assert b"(3,4)" in pieces[2]


def test_small_insert_is_byte_identical_with_default_split_policy():
    line = b"INSERT INTO t VALUES (1),(2);\n"

    pieces, chunks = split_large_insert_line(line, max_bytes=256 * 1024)

    assert pieces == [line]
    assert chunks == 0


def test_fast_path_falls_back_when_delimiter_text_is_quoted():
    line = b"INSERT INTO t VALUES (1,'literal ),( marker'),(2,'ok');\n"

    pieces, chunks = split_large_insert_line(line, max_bytes=40)

    assert chunks == len(pieces) == 2
    assert b"literal ),( marker" in pieces[0]
    assert b"(2,'ok')" in pieces[1]
