import ast
import re
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE_SUFFIXES = {".py", ".ps1", ".sh", ".sql"}

# CURRENT_USER, OUT, KEYS, and ROWS are reserved by MySQL 8.4.  SESSION_USER
# is included as an explicit repository contract because it is the companion
# identity alias that caused the same production parser failure.
FORBIDDEN_BARE_ALIASES = (
    "current_user",
    "session_user",
    "out",
    "keys",
    "rows",
)
BARE_ALIAS_PATTERN = re.compile(
    rf"\bAS\s+(?P<alias>{'|'.join(FORBIDDEN_BARE_ALIASES)})\b",
    re.IGNORECASE,
)


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


def _searchable_source_text(path: Path) -> list[tuple[int, str]]:
    source = path.read_text(encoding="utf-8")
    if path.suffix.casefold() != ".py":
        return [(1, source)]
    tree = ast.parse(source, filename=str(path))
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _bare_aliases(text: str) -> list[str]:
    return [
        match.group("alias").casefold()
        for match in BARE_ALIAS_PATTERN.finditer(text)
    ]


def test_bare_alias_detector_is_case_insensitive_and_allows_quoted_aliases():
    assert _bare_aliases("SELECT 1 As OuT") == ["out"]
    assert _bare_aliases("SELECT 1 AS `out`, 2 AS \"rows\"") == []


def test_tracked_production_sql_has_no_dangerous_bare_aliases():
    offenders: list[str] = []
    for path in _tracked_production_sources():
        for line_number, text in _searchable_source_text(path):
            for match in BARE_ALIAS_PATTERN.finditer(text):
                alias = match.group("alias").casefold()
                match_line = line_number + text[: match.start()].count("\n")
                relative_path = path.relative_to(REPOSITORY_ROOT).as_posix()
                offenders.append(f"{relative_path}:{match_line}: AS {alias}")

    assert not offenders, (
        "MySQL 8.4-unsafe bare SQL aliases found:\n" + "\n".join(offenders)
    )
