from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
ACTIVE_SQL_ROOTS = ("biz", "server", "integrations", "tools", "scripts", "deploy")

# migrations_v4.py contains checksum-frozen migration statements. Compatibility
# for those statements is provided by the migration runner, not by rewriting the
# canonical SQL text.
FROZEN_SQL_FILES = {Path("server/db/migrations_v4.py")}

# The intersection of MySQL 8.4 INFORMATION_SCHEMA.KEYWORDS (RESERVED = 1) and
# the real 5.5 business schemas contains CHANGE and RANK. RANK is covered by its
# own compatibility audit; this test owns the remaining collision.
MYSQL84_RESERVED_SCHEMA_IDENTIFIERS = ("change",)
MYSQL84_RESERVED_ALIAS_IDENTIFIERS = ("keys", "rows")

SQL_STATEMENT_START = re.compile(
    r"(?im)^\s*(?:"
    r"WITH\b|SELECT\b|INSERT\s+INTO\b|REPLACE\s+INTO\b|UPDATE\b|"
    r"DELETE\s+FROM\b|CREATE\s+TABLE\b|ALTER\s+TABLE\b|"
    r"CHANGE\s+(?:MASTER|REPLICATION)\b"
    r")"
)
BIND_PARAMETER = re.compile(r"(?<!:):[A-Za-z_]\w*")
ALTER_TABLE_HEAD = re.compile(
    r"(?is)^\s*ALTER\s+TABLE\s+"
    r"(?:[A-Za-z_]\w*|Q)(?:\s*\.\s*(?:[A-Za-z_]\w*|Q))?"
)


def _blank(chars: list[str], start: int, end: int, marker: str | None = None) -> None:
    for index in range(start, end):
        chars[index] = " "
    if marker is not None and start < end:
        chars[start] = marker


def _mask_non_identifiers(sql: str) -> str:
    """Mask comments, values, quoted identifiers and bind parameters.

    A quoted identifier is represented by one ``Q`` token so an
    ``ALTER TABLE `schema`.`table` CHANGE ...`` clause can still be recognized
    as SQL syntax. Its contents remain unavailable to the reserved-word scan.
    """

    chars = list(sql)
    index = 0
    length = len(sql)
    while index < length:
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            end = length if end < 0 else end + 2
            _blank(chars, index, end)
            index = end
            continue

        if sql.startswith("--", index) or sql[index] == "#":
            end = sql.find("\n", index + 1)
            end = length if end < 0 else end
            _blank(chars, index, end)
            index = end
            continue

        quote = sql[index]
        if quote in {"'", '"', "`"}:
            end = index + 1
            while end < length:
                if sql[end] == "\\" and end + 1 < length:
                    end += 2
                    continue
                if sql[end] == quote:
                    if end + 1 < length and sql[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            _blank(chars, index, end, "Q" if quote == "`" else None)
            index = end
            continue

        index += 1

    masked = "".join(chars)
    return BIND_PARAMETER.sub(lambda match: " " * len(match.group(0)), masked)


def _is_change_syntax_keyword(masked_sql: str, offset: int) -> bool:
    before = masked_sql[:offset].rsplit(";", 1)[-1]
    after = masked_sql[offset + len("change") :]

    # Top-level replication syntax: CHANGE MASTER / CHANGE REPLICATION ...
    if not before.strip() and re.match(r"(?is)\s+(?:MASTER|REPLICATION)\b", after):
        return True

    # ALTER TABLE t CHANGE [COLUMN] old_name new_name ... . A CHANGE token at
    # the beginning of a later comma-delimited ALTER clause is also syntax.
    head = ALTER_TABLE_HEAD.match(before)
    if head is None:
        return False
    alter_prefix = before[head.end() :]
    return not alter_prefix.strip() or alter_prefix.rstrip().endswith(",")


def _unquoted_identifier_offsets(sql: str, identifier: str) -> list[int]:
    masked = _mask_non_identifiers(sql)
    word = re.compile(rf"(?i)\b{re.escape(identifier)}\b")
    return [
        match.start()
        for match in word.finditer(masked)
        if not (
            identifier.casefold() == "change"
            and _is_change_syntax_keyword(masked, match.start())
        )
    ]


def _unquoted_alias_offsets(sql: str, identifier: str) -> list[int]:
    masked = _mask_non_identifiers(sql)
    alias = re.compile(rf"(?i)\bAS\s+({re.escape(identifier)})\b")
    return [match.start(1) for match in alias.finditer(masked)]


def _joined_string_value(node: ast.JoinedStr) -> str:
    return "".join(
        value.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str)
        else "__expression__"
        for value in node.values
    )


def _python_sql_literals(path: Path) -> Iterator[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    seen: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
        elif isinstance(node, ast.JoinedStr):
            value = _joined_string_value(node)
        else:
            continue
        item = (node.lineno, value)
        if item in seen:
            continue
        seen.add(item)
        if SQL_STATEMENT_START.search(_mask_non_identifiers(value)):
            yield item


def _active_sql() -> Iterator[tuple[Path, int, str]]:
    for root_name in ACTIVE_SQL_ROOTS:
        root = ROOT / root_name
        for path in sorted(root.rglob("*.py")):
            relative = path.relative_to(ROOT)
            if relative in FROZEN_SQL_FILES:
                continue
            for line, sql in _python_sql_literals(path):
                yield relative, line, sql
        for path in sorted(root.rglob("*.sql")):
            relative = path.relative_to(ROOT)
            if relative in FROZEN_SQL_FILES:
                continue
            sql = path.read_text(encoding="utf-8")
            if SQL_STATEMENT_START.search(_mask_non_identifiers(sql)):
                yield relative, 1, sql


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT `change` FROM prices",
        "INSERT INTO prices (`change`) VALUES (:change)",
        "SELECT 'change' AS label FROM prices",
        "ALTER TABLE prices CHANGE COLUMN old_value new_value DECIMAL(18, 6)",
        "ALTER TABLE `probiga`.`prices` ALGORITHM=INPLACE, CHANGE old_value new_value INT",
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST='change'",
    ),
)
def test_reserved_word_scanner_allows_quoted_values_and_change_syntax(sql: str) -> None:
    assert _unquoted_identifier_offsets(sql, "change") == []


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT change FROM prices",
        "SELECT p.change FROM prices AS p",
        "SELECT 1 AS change FROM prices",
        "CREATE TABLE prices (change DECIMAL(18, 6))",
        "ALTER TABLE prices ADD COLUMN change DECIMAL(18, 6)",
        "ALTER TABLE prices CHANGE change new_value DECIMAL(18, 6)",
        "UPDATE prices SET change = :change",
    ),
)
def test_reserved_word_scanner_rejects_unquoted_change_identifiers(sql: str) -> None:
    assert _unquoted_identifier_offsets(sql, "change")


def test_reserved_word_scanner_distinguishes_aliases_from_rows_syntax() -> None:
    valid = (
        "SELECT 1 AS `keys`, 2 AS `rows`",
        "SELECT SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM prices",
    )
    invalid = "SELECT COUNT(DISTINCT query_key) AS keys, COUNT(*) AS rows FROM samples"

    for sql in valid:
        for identifier in MYSQL84_RESERVED_ALIAS_IDENTIFIERS:
            assert _unquoted_alias_offsets(sql, identifier) == []
    for identifier in MYSQL84_RESERVED_ALIAS_IDENTIFIERS:
        assert _unquoted_alias_offsets(invalid, identifier)


def test_active_business_sql_quotes_mysql84_reserved_identifiers() -> None:
    violations: list[str] = []
    for path, line, sql in _active_sql():
        for identifier in MYSQL84_RESERVED_SCHEMA_IDENTIFIERS:
            offsets = _unquoted_identifier_offsets(sql, identifier)
            if offsets:
                snippet = " ".join(sql.split())[:240]
                violations.append(
                    f"{path}:{line}: unquoted {identifier!r} at offsets {offsets}: {snippet}"
                )
        for identifier in MYSQL84_RESERVED_ALIAS_IDENTIFIERS:
            offsets = _unquoted_alias_offsets(sql, identifier)
            if offsets:
                snippet = " ".join(sql.split())[:240]
                violations.append(
                    f"{path}:{line}: unquoted alias {identifier!r} at offsets {offsets}: {snippet}"
                )

    assert violations == [], "\n" + "\n".join(violations)
