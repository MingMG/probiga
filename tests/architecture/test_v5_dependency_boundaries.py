from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OWNED_ROOTS = (ROOT / "server/trading_v5", ROOT / "tools/trading_v5")
FORBIDDEN_PREFIXES = (
    "server.trading_v2",
    "server.trading_v3",
    "server.trading_v4",
    "server.trading_v6",
    "server.common",
    "tools.research_trading_v4",
)


def _module_context(path: Path) -> tuple[str, ...]:
    relative = path.relative_to(ROOT).with_suffix("")
    parts = relative.parts
    return tuple(parts[:-1] if parts[-1] == "__init__" else parts[:-1])


def _resolved_from_import(path: Path, node: ast.ImportFrom) -> list[str]:
    package = list(_module_context(path))
    if node.level:
        remove = node.level - 1
        if remove > len(package):
            return ["INVALID_RELATIVE_IMPORT"]
        package = package[: len(package) - remove]
    elif node.module:
        package = []
    module_parts = node.module.split(".") if node.module else []
    base = ".".join([*package, *module_parts])
    result = [base] if base else []
    result.extend(
        f"{base}.{alias.name}" if base else alias.name
        for alias in node.names
        if alias.name != "*"
    )
    return result


def _imports(path: Path, *, enforce_no_dynamic_imports: bool = False) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: list[str] = []
    importlib_aliases: set[str] = set()
    import_function_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.extend(alias.name for alias in node.names)
            importlib_aliases.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "importlib"
            )
        elif isinstance(node, ast.ImportFrom):
            result.extend(_resolved_from_import(path, node))
            if node.module == "importlib":
                import_function_aliases.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name in {"import_module", "__import__"}
                )
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if enforce_no_dynamic_imports:
                assert node.func.id not in {
                    "__import__",
                    "eval",
                    "exec",
                    *import_function_aliases,
                }, (
                    path,
                    node.func.id,
                )
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if enforce_no_dynamic_imports:
                assert not (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id in importlib_aliases
                ), (path, "dynamic importlib call")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if enforce_no_dynamic_imports:
                assert not node.value.startswith(FORBIDDEN_PREFIXES), (
                    path,
                    node.value,
                )
    return result


def test_v5_owned_python_has_no_legacy_runtime_or_dynamic_imports() -> None:
    files = sorted(path for root in OWNED_ROOTS for path in root.rglob("*.py"))
    assert files
    for path in files:
        for imported in _imports(path, enforce_no_dynamic_imports=True):
            assert not imported.startswith(FORBIDDEN_PREFIXES), (path, imported)


def test_older_runtime_packages_do_not_import_v5() -> None:
    for directory in (
        ROOT / "server/trading_v2",
        ROOT / "server/trading_v3",
        ROOT / "server/trading_v4",
    ):
        for path in directory.rglob("*.py"):
            assert all(
                not imported.startswith("server.trading_v5")
                for imported in _imports(path)
            ), path
