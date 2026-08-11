import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ACTIVE_SQL_ROOTS = (
    "biz",
    "deploy",
    "integrations",
    "scripts",
    "server",
    "strategies",
    "tools",
)
ZERO_DATE_AUDIT_MARKER = "mysql84-zero-date-audit-only"
ZERO_DATE_AUDIT_EXCEPTIONS = {
    "server/db/mysql84_datetime_defaults.py": 2,
    "tools/mysql55_to_mysql84_data_manifest.py": 2,
}


def _active_sql_source_files() -> list[Path]:
    files: list[Path] = []
    for relative_root in ACTIVE_SQL_ROOTS:
        root = ROOT / relative_root
        if not root.exists():
            continue
        files.extend(root.rglob("*.py"))
        files.extend(root.rglob("*.sql"))
    return files


def test_active_sql_has_no_zero_date_literals() -> None:
    zero_date = re.compile(
        r"(?<!\d)(?:0000-\d{2}-\d{2}|\d{4}-00-\d{2}|\d{4}-\d{2}-00)(?!\d)"
    )
    offenders = []
    marked_audit_literals = {path: 0 for path in ZERO_DATE_AUDIT_EXCEPTIONS}
    for path in _active_sql_source_files():
        text = path.read_text(encoding="utf-8-sig")
        relative = path.relative_to(ROOT).as_posix()
        for match in zero_date.finditer(text):
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_end = text.find("\n", match.end())
            if line_end < 0:
                line_end = len(text)
            same_line = text[line_start:line_end]
            if relative in ZERO_DATE_AUDIT_EXCEPTIONS and ZERO_DATE_AUDIT_MARKER in same_line:
                marked_audit_literals[relative] += 1
            else:
                offenders.append(relative)
    assert offenders == []
    assert marked_audit_literals == ZERO_DATE_AUDIT_EXCEPTIONS


def test_zero_date_audit_marker_is_confined_to_authorized_auditors() -> None:
    marked_files = []
    for path in _active_sql_source_files():
        text = path.read_text(encoding="utf-8-sig")
        if ZERO_DATE_AUDIT_MARKER in text:
            marked_files.append(path.relative_to(ROOT).as_posix())
    assert marked_files == list(ZERO_DATE_AUDIT_EXCEPTIONS)


def test_kline_insert_selects_guard_percent_denominators() -> None:
    source_paths = (
        ROOT / "tools" / "build_daily_kline_from_intraday.py",
        ROOT / "tools" / "promote_qmt_local_history_to_business.py",
    )
    combined = "\n".join(
        path.read_text(encoding="utf-8-sig") for path in source_paths
    )

    unsafe_denominator = re.compile(
        r"/\s*\(\s*1\s*\+\s*(?:c|lm)\.change_pct\s*/\s*100\s*\)"
    )
    assert unsafe_denominator.search(combined) is None
    assert combined.count(
        "/ NULLIF(1 + c.change_pct / 100, 0)"
    ) == 2
    assert (
        combined.count("/ NULLIF(1 + lm.change_pct / 100, 0)")
        == 1
    )


def test_jq_writes_do_not_use_legacy_zero_date_defaults() -> None:
    source = (ROOT / "server" / "api" / "routers" / "hot_data.py").read_text(
        encoding="utf-8-sig"
    )

    assert "pick_date, created_at" in source
    assert "VALUES (:s, :c, :n, :sc, :r, :d, NOW())" in source
    assert "created_at, updated_at" in source
    assert "VALUES (:s, :desc, :d, NOW(), NOW())" in source
    assert "updated_at = NOW()" in source
