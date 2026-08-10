from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
OWNED_ROOTS = (ROOT / "server/trading_v6", ROOT / "tools/trading_v6")
FORBIDDEN = (
    "server.trading_v2", "server.trading_v3", "server.trading_v4",
    "server.trading_v5", "server.common", "sqlalchemy", "pymysql",
    "integrations", "tools.research_trading_v4",
)


def _owned_files():
    return sorted(path for root in OWNED_ROOTS for path in root.rglob("*.py"))


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue
            yield node.module or ""
        elif isinstance(node, ast.Call):
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            if name in {"__import__", "import_module", "eval", "exec"}:
                for argument in node.args:
                    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                        yield argument.value


def test_v6_has_no_cross_version_database_or_order_runtime_dependency() -> None:
    files = _owned_files()
    assert files
    for path in files:
        for imported in _imports(path):
            assert not imported.startswith(FORBIDDEN), (path, imported)
        text = path.read_text(encoding="utf-8").lower()
        for token in (
            "create_engine(", "pymysql.connect(", "submit_order(",
            "real_order_submission = true", "paper_orders_allowed = true",
        ):
            assert token not in text, (path, token)


def test_older_version_packages_do_not_import_v6() -> None:
    for version in ("trading_v2", "trading_v3", "trading_v4", "trading_v5"):
        root = ROOT / "server" / version
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            assert "trading_v6" not in path.read_text(
                encoding="utf-8", errors="ignore"
            ), path


def test_v6_modules_import_with_site_packages_disabled_from_unrelated_cwd(tmp_path) -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = ""
    code = (
        "import sys;"
        f"sys.path.insert(0, {str(ROOT)!r});"
        "import server.trading_v6.evidence;"
        "import server.trading_v6.models;"
        "import server.trading_v6.pit_finance;"
        "print('V6_IMPORT_OK')"
    )
    process = subprocess.run(
        [sys.executable, "-S", "-c", code], cwd=tmp_path, env=environment,
        capture_output=True, text=True, check=False,
    )
    assert process.returncode == 0, process.stderr
    assert process.stdout.strip() == "V6_IMPORT_OK"
