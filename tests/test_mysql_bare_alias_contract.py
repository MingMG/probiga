import ast
import hashlib
import re
import subprocess
from pathlib import Path
from typing import NamedTuple


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE_SUFFIXES = {".py", ".ps1", ".sh", ".sql"}

# Frozen from Oracle's MySQL 8.4 keyword inventory.  The manual marks 262
# words with (R) and separately states that _FILENAME is reserved:
# https://dev.mysql.com/doc/refman/8.4/en/keywords.html
MYSQL_84_RESERVED_WORDS = frozenset(
    {
        "_filename", "accessible", "add", "all", "alter", "analyze",
        "and", "as", "asc", "asensitive", "before", "between",
        "bigint", "binary", "blob", "both", "by", "call",
        "cascade", "case", "change", "char", "character", "check",
        "collate", "column", "condition", "constraint", "continue", "convert",
        "create", "cross", "cube", "cume_dist", "current_date", "current_time",
        "current_timestamp", "current_user", "cursor", "database", "databases", "day_hour",
        "day_microsecond", "day_minute", "day_second", "dec", "decimal", "declare",
        "default", "delayed", "delete", "dense_rank", "desc", "describe",
        "deterministic", "distinct", "distinctrow", "div", "double", "drop",
        "dual", "each", "else", "elseif", "empty", "enclosed",
        "escaped", "except", "exists", "exit", "explain", "false",
        "fetch", "first_value", "float", "float4", "float8", "for",
        "force", "foreign", "from", "fulltext", "function", "generated",
        "get", "grant", "group", "grouping", "groups", "having",
        "high_priority", "hour_microsecond", "hour_minute", "hour_second", "if", "ignore",
        "in", "index", "infile", "inner", "inout", "insensitive",
        "insert", "int", "int1", "int2", "int3", "int4",
        "int8", "integer", "intersect", "interval", "into", "io_after_gtids",
        "io_before_gtids", "is", "iterate", "join", "json_table", "key",
        "keys", "kill", "lag", "last_value", "lateral", "lead",
        "leading", "leave", "left", "like", "limit", "linear",
        "lines", "load", "localtime", "localtimestamp", "lock", "long",
        "longblob", "longtext", "loop", "low_priority", "match", "maxvalue",
        "mediumblob", "mediumint", "mediumtext", "middleint", "minute_microsecond", "minute_second",
        "mod", "modifies", "natural", "no_write_to_binlog", "not", "nth_value",
        "ntile", "null", "numeric", "of", "on", "optimize",
        "optimizer_costs", "option", "optionally", "or", "order", "out",
        "outer", "outfile", "over", "partition", "percent_rank", "precision",
        "primary", "procedure", "purge", "qualify", "range", "rank",
        "read", "read_write", "reads", "real", "recursive", "references",
        "regexp", "release", "rename", "repeat", "replace", "require",
        "resignal", "restrict", "return", "revoke", "right", "rlike",
        "row", "row_number", "rows", "schema", "schemas", "second_microsecond",
        "select", "sensitive", "separator", "set", "show", "signal",
        "smallint", "spatial", "specific", "sql", "sql_big_result", "sql_calc_found_rows",
        "sql_small_result", "sqlexception", "sqlstate", "sqlwarning", "ssl", "starting",
        "stored", "straight_join", "system", "table", "tablesample", "terminated",
        "then", "tinyblob", "tinyint", "tinytext", "to", "trailing",
        "trigger", "true", "undo", "union", "unique", "unlock",
        "unsigned", "update", "usage", "use", "using", "utc_date",
        "utc_time", "utc_timestamp", "values", "varbinary", "varchar", "varcharacter",
        "varying", "virtual", "when", "where", "while", "window",
        "with", "write", "xor", "year_month", "zerofill",
    }
)

# SESSION_USER is a built-in identity function rather than an (R) word in the
# upstream inventory.  It remains forbidden as a bare alias because it is the
# companion identity name implicated by the production MySQL 8.4 failure.
FORBIDDEN_BARE_ALIASES = MYSQL_84_RESERVED_WORDS | {"session_user"}
SQL_STATEMENT_STARTERS = {
    "create",
    "delete",
    "insert",
    "replace",
    "select",
    "update",
}
QUERY_EXPRESSION_STARTERS = {"select", "table", "values", "with"}

SQL_TOKEN_PATTERN = re.compile(
    r"""
    (?P<space>\s+)
    |(?P<block_comment>/\*.*?\*/)
    |(?P<dash_comment>--(?=\s|$)[^\r\n]*)
    |(?P<hash_comment>\#[^\r\n]*)
    |(?P<single_quote>'(?:''|\\.|[^'\\])*')
    |(?P<double_quote>"(?:""|\\.|[^"\\])*")
    |(?P<backtick>`(?:``|\\.|[^`\\])*`)
    |(?P<word>[A-Za-z_][A-Za-z0-9_$]*)
    |(?P<symbol>[(),;])
    |(?P<other>.)
    """,
    re.DOTALL | re.VERBOSE,
)

SCRIPT_STRING_PATTERN = re.compile(
    r"""
    (?P<ps_here_single>@'\r?\n(?P<ps_here_single_body>.*?)\r?\n'@)
    |(?P<ps_here_double>@"\r?\n(?P<ps_here_double_body>.*?)\r?\n"@)
    |(?P<single>'(?P<single_body>(?:''|\\.|[^'\\])*)')
    |(?P<double>"(?P<double_body>(?:""|\\.|`.|[^"\\`])*)")
    """,
    re.DOTALL | re.MULTILINE | re.VERBOSE,
)


class _SqlToken(NamedTuple):
    kind: str
    value: str
    start: int
    starts_line: bool


class _BareAlias(NamedTuple):
    alias: str
    start: int


def _tracked_production_sources() -> list[Path]:
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "-z",
            "--",
            "*.py",
            "*.ps1",
            "*.sh",
            "*.sql",
        ],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
    )
    relative_paths = result.stdout.decode("utf-8").split("\0")
    return [
        REPOSITORY_ROOT / relative_path
        for relative_path in relative_paths
        if relative_path
        and Path(relative_path).suffix.casefold() in PRODUCTION_SOURCE_SUFFIXES
        and Path(relative_path).parts[0].casefold() != "tests"
    ]


def _string_expression(node: ast.AST) -> tuple[str, bool]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value, True
    if isinstance(node, ast.FormattedValue):
        return " __PY_EXPR__ ", False
    if isinstance(node, ast.JoinedStr):
        parts = [_string_expression(value) for value in node.values]
        return "".join(text for text, _ in parts), any(found for _, found in parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left_text, left_found = _string_expression(node.left)
        right_text, right_found = _string_expression(node.right)
        return left_text + right_text, left_found or right_found
    return " __PY_EXPR__ ", False


class _SourceStringVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.docstring_node_ids: set[int] = set()
        self.strings: set[tuple[int, str]] = set()

    def _visit_scope(self, node: ast.AST) -> None:
        body = getattr(node, "body", [])
        if body and isinstance(body[0], ast.Expr):
            value = body[0].value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                self.docstring_node_ids.add(id(value))
        self.generic_visit(node)

    visit_Module = _visit_scope
    visit_ClassDef = _visit_scope
    visit_FunctionDef = _visit_scope
    visit_AsyncFunctionDef = _visit_scope

    def _record(self, node: ast.AST) -> None:
        if id(node) in self.docstring_node_ids:
            return
        text, contains_literal = _string_expression(node)
        if contains_literal and text:
            self.strings.add((node.lineno, text))

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self._record(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        self._record(node)
        for value in node.values:
            if isinstance(value, ast.FormattedValue):
                self.visit(value.value)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        if isinstance(node.op, ast.Add):
            self._record(node)
        self.generic_visit(node)


def _python_source_strings(path: Path) -> list[tuple[int, str]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    visitor = _SourceStringVisitor()
    visitor.visit(tree)
    return sorted(visitor.strings)


def _script_source_strings(path: Path) -> list[tuple[int, str]]:
    source = path.read_text(encoding="utf-8")
    masked = list(source)
    strings: set[tuple[int, str]] = set()
    for match in SCRIPT_STRING_PATTERN.finditer(source):
        for group_name in (
            "ps_here_single_body",
            "ps_here_double_body",
            "single_body",
            "double_body",
        ):
            body = match.group(group_name)
            if body is None:
                continue
            body_start = match.start(group_name)
            strings.add((1 + source[:body_start].count("\n"), body))
            break
        for index in range(match.start(), match.end()):
            if masked[index] not in "\r\n":
                masked[index] = " "

    unquoted_source = "".join(masked)
    unquoted_source = re.sub(r"(?s)<\#.*?\#>", "", unquoted_source)
    unquoted_source = re.sub(r"(?m)^[ \t]*\#.*$", "", unquoted_source)
    strings.add((1, unquoted_source))
    return sorted(strings)


def _searchable_source_text(path: Path) -> list[tuple[int, str]]:
    suffix = path.suffix.casefold()
    if suffix == ".py":
        return _python_source_strings(path)
    if suffix in {".ps1", ".sh"}:
        return _script_source_strings(path)
    return [(1, path.read_text(encoding="utf-8"))]


def _sql_tokens(text: str) -> list[_SqlToken]:
    tokens: list[_SqlToken] = []
    line_has_code = False
    for match in SQL_TOKEN_PATTERN.finditer(text):
        kind = match.lastgroup or "other"
        value = match.group()
        if kind in {"space", "block_comment", "dash_comment", "hash_comment"}:
            if "\n" in value or "\r" in value:
                line_has_code = bool(value.rsplit("\n", 1)[-1].strip())
            continue
        starts_line = not line_has_code
        line_has_code = True
        if kind != "other":
            tokens.append(_SqlToken(kind, value, match.start(), starts_line))
    return tokens


def _is_create_query_as(alias: str, top_level_words: list[str]) -> bool:
    if alias not in QUERY_EXPRESSION_STARTERS:
        return False
    if not top_level_words or top_level_words[0] != "create":
        return False
    return "table" in top_level_words[1:] or "view" in top_level_words[1:]


def _bare_alias_matches(text: str) -> list[_BareAlias]:
    tokens = _sql_tokens(text)
    matches: list[_BareAlias] = []
    sql_context = False
    statement_has_code = False
    parenthesis_stack: list[bool] = []
    top_level_words: list[str] = []
    previous_token: _SqlToken | None = None

    for index, token in enumerate(tokens):
        if token.kind == "symbol" and token.value == ";":
            sql_context = False
            statement_has_code = False
            parenthesis_stack.clear()
            top_level_words.clear()
            previous_token = token
            continue

        if token.kind == "symbol" and token.value == "(":
            parenthesis_stack.append(
                bool(
                    previous_token
                    and previous_token.kind == "word"
                    and previous_token.value.casefold() == "cast"
                )
            )
            statement_has_code = True
            previous_token = token
            continue
        if token.kind == "symbol" and token.value == ")":
            if parenthesis_stack:
                parenthesis_stack.pop()
            statement_has_code = True
            previous_token = token
            continue

        if token.kind == "word":
            word = token.value.casefold()
            if word in SQL_STATEMENT_STARTERS and (
                not statement_has_code or token.starts_line
            ):
                sql_context = True
            if not parenthesis_stack:
                top_level_words.append(word)

            if word == "as" and sql_context and index + 1 < len(tokens):
                alias_token = tokens[index + 1]
                if alias_token.kind == "word":
                    alias = alias_token.value.casefold()
                    if (
                        alias in FORBIDDEN_BARE_ALIASES
                        and not (parenthesis_stack and parenthesis_stack[-1])
                        and not _is_create_query_as(alias, top_level_words)
                    ):
                        matches.append(_BareAlias(alias, token.start))

        statement_has_code = True
        previous_token = token

    return matches


def _bare_aliases(text: str) -> list[str]:
    return [match.alias for match in _bare_alias_matches(text)]


def test_mysql_84_reserved_word_inventory_is_frozen_and_complete():
    assert len(MYSQL_84_RESERVED_WORDS) == 263
    inventory = "\n".join(sorted(MYSQL_84_RESERVED_WORDS)).encode("ascii")
    assert hashlib.sha256(inventory).hexdigest() == (
        "64592c091646f3bdbd098cb38f8c70b1f1faf4a0d7dc0d17d4e1b1307523593b"
    )
    assert "session_user" not in MYSQL_84_RESERVED_WORDS
    assert "session_user" in FORBIDDEN_BARE_ALIASES


def test_bare_alias_detector_covers_every_reserved_word_case_insensitively():
    for alias in sorted(FORBIDDEN_BARE_ALIASES):
        assert _bare_aliases(f"SeLeCt 1 aS {alias.upper()}") == [alias]


def test_bare_alias_detector_allows_quoted_aliases():
    assert _bare_aliases(
        "SELECT 1 AS `out`, 2 AS \"rows\", 3 AS 'order'"
    ) == []


def test_bare_alias_detector_ignores_non_alias_as_and_ordinary_english():
    assert _bare_aliases(
        "SELECT CAST(total AS DECIMAL(20, 2)) AS safe_total"
    ) == []
    assert _bare_aliases(
        "SELECT CAST((SELECT amount AS order) AS DECIMAL(20, 2))"
    ) == ["order"]
    assert _bare_aliases(
        "CREATE TABLE archive AS SELECT * FROM source"
    ) == []
    assert _bare_aliases(
        "CREATE VIEW archive AS WITH rows_cte AS (SELECT 1) SELECT * FROM rows_cte"
    ) == []
    assert _bare_aliases("Return the latest value used as index cutoff.") == []
    assert _bare_aliases("Treat child text as SQL/data, not executable input.") == []
    assert _bare_aliases("SELECT 'ordinary words AS rows' AS safe_label") == []


def test_bare_alias_detector_still_checks_table_and_projection_aliases():
    assert _bare_aliases("UPDATE positions AS range SET active = 0") == ["range"]
    assert _bare_aliases(
        "CREATE TABLE archive AS SELECT amount AS order FROM ledger"
    ) == ["order"]


def test_tracked_production_sql_has_no_mysql_84_unsafe_bare_aliases():
    offenders: set[str] = set()
    for path in _tracked_production_sources():
        for line_number, text in _searchable_source_text(path):
            for match in _bare_alias_matches(text):
                match_line = line_number + text[: match.start].count("\n")
                relative_path = path.relative_to(REPOSITORY_ROOT).as_posix()
                offenders.add(f"{relative_path}:{match_line}: AS {match.alias}")

    assert not offenders, (
        "MySQL 8.4-unsafe bare SQL aliases found:\n"
        + "\n".join(sorted(offenders))
    )
