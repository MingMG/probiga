from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUSINESS_SOURCE_ROOTS = (
    "adata",
    "biz",
    "deploy",
    "integrations",
    "scripts",
    "server",
    "strategies",
    "tools",
)
SQL_PREFIX = re.compile(
    r"^\s*(?:SELECT|INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|WITH|"
    r"ORDER\s+BY|GROUP\s+BY|HAVING|PARTITION\s+BY)\b",
    re.IGNORECASE | re.DOTALL,
)
UNQUOTED_RANK = re.compile(
    r"(?<![\x60A-Za-z0-9_:])rank(?![\x60A-Za-z0-9_:])",
    re.IGNORECASE,
)
SQL_SINGLE_QUOTED_LITERAL = re.compile(r"'(?:''|\\.|[^'])*'", re.DOTALL)
SQL_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
SQL_LINE_COMMENT = re.compile(r"(?:--|#)[^\r\n]*")


def _business_python_paths() -> list[Path]:
    paths = [path for path in ROOT.glob("*.py") if path.is_file()]
    for root_name in BUSINESS_SOURCE_ROOTS:
        source_root = ROOT / root_name
        if source_root.exists():
            paths.extend(source_root.rglob("*.py"))
    return sorted(set(paths))


def _business_sql_paths() -> list[Path]:
    paths: list[Path] = []
    for root_name in BUSINESS_SOURCE_ROOTS:
        source_root = ROOT / root_name
        if source_root.exists():
            paths.extend(source_root.rglob("*.sql"))
    return sorted(set(paths))


def _sql_code(value: str) -> str:
    without_literals = SQL_SINGLE_QUOTED_LITERAL.sub("''", value)
    without_blocks = SQL_BLOCK_COMMENT.sub(" ", without_literals)
    return SQL_LINE_COMMENT.sub(" ", without_blocks)


def _has_unsafe_rank(value: str) -> bool:
    code = _sql_code(value)
    for match in UNQUOTED_RANK.finditer(code):
        if not re.match(r"\s*\(", code[match.end():]):
            return True
    return False


def _literal_value(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            item.value
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
            else "{expr}"
            for item in node.values
        )
    return None


def _embedded_python_trees(tree: ast.AST):
    yield "", tree
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        source = node.value.lstrip()
        if not source.startswith(("from __future__ import", "import ")):
            continue
        try:
            embedded = ast.parse(node.value)
        except SyntaxError:
            continue
        yield f":embedded@{node.lineno}", embedded


def _unsafe_rank_references(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    try:
        display_path = path.relative_to(ROOT)
    except ValueError:
        display_path = path
    findings: list[str] = []
    seen: set[tuple[str, int, str]] = set()
    for label, candidate_tree in _embedded_python_trees(tree):
        for node in ast.walk(candidate_tree):
            value = _literal_value(node)
            if value is None:
                continue
            code = _sql_code(value)
            if not SQL_PREFIX.search(code):
                continue
            if _has_unsafe_rank(code):
                compact = " ".join(value.split())[:240]
                key = (label, node.lineno, compact)
                if key not in seen:
                    seen.add(key)
                    findings.append(f"{display_path}{label}:{node.lineno}: {compact}")
    return findings


def test_business_sql_quotes_mysql84_reserved_rank_identifier():
    findings = [
        finding
        for path in _business_python_paths()
        for finding in _unsafe_rank_references(path)
    ]
    findings.extend(
        str(path.relative_to(ROOT))
        for path in _business_sql_paths()
        if _has_unsafe_rank(path.read_text(encoding="utf-8"))
    )
    assert not findings, "unquoted MySQL 8.4 reserved identifier rank:\n" + "\n".join(findings)


def test_rank_scanner_distinguishes_columns_from_safe_rank_uses(tmp_path):
    unsafe = tmp_path / "unsafe.py"
    unsafe.write_text('SQL = "SELECT rank FROM scores ORDER BY scores.rank"\n', encoding="utf-8")
    assert _unsafe_rank_references(unsafe)

    safe = tmp_path / "safe.py"
    safe.write_text(
        'SQL = "SELECT `rank`, RANK() OVER (ORDER BY score), :rank, \'hot rank label\' FROM scores"\n',
        encoding="utf-8",
    )
    assert _unsafe_rank_references(safe) == []


def test_production_acceptance_audit_quotes_dynamic_rank_column():
    source = (ROOT / "tools" / "production_acceptance_audit.py").read_text(encoding="utf-8")
    assert "rank_identifier = quote_identifier(rank_col)" in source
    assert "SUM({rank_identifier} IS NULL OR {rank_identifier} < 1 OR {rank_identifier} > 100)" in source
    assert "SUM({rank_col} IS NULL OR {rank_col} < 1 OR {rank_col} > 100)" not in source
