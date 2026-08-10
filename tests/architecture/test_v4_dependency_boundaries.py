"""Executable clean-room dependency rules for Trading V4."""

from __future__ import annotations

import ast
import importlib.util
import sys
from dataclasses import fields, is_dataclass
from pathlib import Path

from server.trading_v4.domain import (
    FORBIDDEN_LEGACY_FIELDS,
    DecisionContext,
    DecisionInput,
    FeatureVector,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
V4_ROOT = PROJECT_ROOT / "server" / "trading_v4"
TRADING_CORE_ROOT = PROJECT_ROOT / "server" / "trading_core"
FORBIDDEN_LEGACY_MODULES = (
    "server.trading_v2",
    "server.trading_v3",
    "server.evaluation",
)
FORBIDDEN_LEGACY_TABLES = frozenset(
    {
        "st_alpha_forecast_v3",
        "st_decision_run_v2",
        "st_decision_run_v3",
        "st_execution_plan_v3",
        "st_portfolio_plan_v2",
        "st_position_state_v3",
        "st_shadow_portfolio_v3",
        "st_strategy_signal_v2",
        "st_target_portfolio_v3",
        "st_theme_signal_v3",
        "st_trade_hypothesis_v3",
    }
)
FORBIDDEN_AMBIENT_ROOTS = {
    "httpx",
    "os",
    "pymysql",
    "random",
    "requests",
    "secrets",
    "socket",
    "sqlalchemy",
    "subprocess",
    "time",
    "uuid",
}
FORBIDDEN_DYNAMIC_MODULE_ROOTS = frozenset(
    {"builtins", "importlib", "pkgutil", "runpy"}
)
FORBIDDEN_DYNAMIC_CALLS = frozenset(
    {
        "__import__",
        "eval",
        "exec",
        "exec_module",
        "find_loader",
        "import_module",
        "load_module",
        "run_module",
        "run_path",
    }
)


def _python_files(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts))


def _module_and_package(path: Path) -> tuple[str, str]:
    relative = path.relative_to(PROJECT_ROOT).with_suffix("")
    parts = list(relative.parts)
    is_package = parts[-1] == "__init__"
    if is_package:
        parts.pop()
    module = ".".join(parts)
    package = module if is_package else module.rpartition(".")[0]
    return module, package


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    _, package = _module_and_package(path)
    return _resolved_imports(tree, package)


def _resolved_imports(tree: ast.AST, package: str) -> tuple[str, ...]:
    """Resolve both ImportFrom bases and their potentially-module aliases."""

    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base_module = importlib.util.resolve_name(
                    "." * node.level + (node.module or ""),
                    package,
                )
            else:
                base_module = node.module or ""
            found.append(base_module)
            found.extend(
                f"{base_module}.{alias.name}" if base_module else alias.name
                for alias in node.names
                if alias.name != "*"
            )
    return tuple(found)


def _static_string(node: ast.AST) -> str | None:
    """Evaluate only compile-time string concatenation, never names/calls."""

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left)
        right = _static_string(node.right)
        return None if left is None or right is None else left + right
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if (
                isinstance(value, ast.FormattedValue)
                and value.conversion == -1
                and value.format_spec is None
            ):
                rendered = _static_string(value.value)
                if rendered is not None:
                    parts.append(rendered)
                    continue
            return None
        return "".join(parts)
    return None


def _static_strings(tree: ast.AST) -> tuple[tuple[int, str], ...]:
    values = {
        (node.lineno, rendered)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr))
        and (rendered := _static_string(node)) is not None
    }
    return tuple(sorted(values))


def _dynamic_code_violations(tree: ast.AST) -> tuple[tuple[int, str], ...]:
    violations: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root in FORBIDDEN_DYNAMIC_MODULE_ROOTS:
                    violations.add((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".", 1)[0]
            if root in FORBIDDEN_DYNAMIC_MODULE_ROOTS:
                violations.add((node.lineno, node.module or root))
        elif isinstance(node, ast.Call):
            function = node.func
            if (
                isinstance(function, ast.Name)
                and function.id in FORBIDDEN_DYNAMIC_CALLS
            ):
                violations.add((node.lineno, function.id))
            elif (
                isinstance(function, ast.Attribute)
                and function.attr in FORBIDDEN_DYNAMIC_CALLS
            ):
                violations.add((node.lineno, function.attr))
        rendered = _static_string(node)
        if rendered in {"__import__", "eval", "exec"}:
            violations.add((node.lineno, rendered))
    return tuple(sorted(violations))


def test_import_scanner_resolves_importfrom_alias_modules():
    tree = ast.parse(
        "from ... import trading_v3\n"
        "from server import trading_v4\n"
    )
    imports = _resolved_imports(tree, "server.trading_v4.kernel")
    assert "server.trading_v3" in imports
    assert "server.trading_v4" in imports


def test_dynamic_guard_rejects_import_alias_and_builtins_lookup():
    tree = ast.parse(
        "from importlib import import_module as load\n"
        "load('server.trading_v3')\n"
        "loader = getattr(__builtins__, '__' + 'import__')\n"
        "runner = getattr(__builtins__, f\"{'ev' + 'al'}\")\n"
    )
    violations = _dynamic_code_violations(tree)
    assert any(name == "importlib" for _, name in violations)
    assert any(name == "__import__" for _, name in violations)
    assert any(name == "eval" for _, name in violations)


def test_static_string_scanner_reassembles_split_legacy_table_names():
    tree = ast.parse(
        "SQL = 'SELECT * FROM ' + 'st_position_' + 'state_v3'\n"
    )
    assert any(
        "st_position_state_v3" in value
        for _, value in _static_strings(tree)
    )


def test_v4_has_no_legacy_runtime_imports():
    violations: list[str] = []
    for path in _python_files(V4_ROOT):
        for module in _imports(path):
            if module.startswith(FORBIDDEN_LEGACY_MODULES):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
    assert not violations, "legacy imports crossed the V4 boundary:\n" + "\n".join(violations)


def test_trading_core_has_no_strategy_runtime_dependencies():
    forbidden = (
        "server.evaluation",
        "server.trading_v2",
        "server.trading_v3",
        "server.trading_v4",
    )
    violations = [
        f"{path.relative_to(PROJECT_ROOT)} -> {module}"
        for path in _python_files(TRADING_CORE_ROOT)
        for module in _imports(path)
        if module.startswith(forbidden)
    ]
    assert not violations, "strategy import crossed into trading_core:\n" + "\n".join(
        violations
    )


def test_v4_domain_has_no_ambient_io_or_nondeterminism_imports():
    violations: list[str] = []
    for path in _python_files(V4_ROOT / "domain"):
        for module in _imports(path):
            root = module.split(".", 1)[0]
            if root in FORBIDDEN_AMBIENT_ROOTS:
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
            elif root == "server" and not module.startswith("server.trading_v4.domain"):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
            elif root not in sys.stdlib_module_names and root != "server":
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
    assert not violations, "impure domain imports detected:\n" + "\n".join(violations)


def test_v4_kernel_has_no_ambient_io_or_nondeterminism_imports():
    violations: list[str] = []
    allowed_server_modules = (
        "server.trading_v4.domain",
        "server.trading_v4.kernel",
    )
    for path in _python_files(V4_ROOT / "kernel"):
        for module in _imports(path):
            root = module.split(".", 1)[0]
            if root in FORBIDDEN_AMBIENT_ROOTS:
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
            elif root == "server" and not module.startswith(
                allowed_server_modules
            ):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
            elif root not in sys.stdlib_module_names and root != "server":
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
    assert not violations, "impure kernel imports detected:\n" + "\n".join(
        violations
    )


def test_v4_ports_are_contract_only_dependencies():
    violations: list[str] = []
    for path in _python_files(V4_ROOT / "ports"):
        for module in _imports(path):
            root = module.split(".", 1)[0]
            if root == "server" and not module.startswith(
                (
                    "server.trading_v4.domain",
                    "server.trading_v4.ports",
                )
            ):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
            elif root not in sys.stdlib_module_names and root != "server":
                violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {module}")
    assert not violations, "implementation dependency found in a V4 port:\n" + "\n".join(violations)


def test_v4_input_contracts_do_not_define_legacy_opinion_fields():
    contract_types = (DecisionContext, DecisionInput, FeatureVector)
    actual_fields = {
        item.name.casefold()
        for contract_type in contract_types
        if is_dataclass(contract_type)
        for item in fields(contract_type)
    }
    assert actual_fields.isdisjoint(FORBIDDEN_LEGACY_FIELDS)


def test_v4_does_not_use_dynamic_import_or_runtime_code_execution():
    violations: list[str] = []
    for path in _python_files(V4_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(
            f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {name}"
            for line_number, name in _dynamic_code_violations(tree)
        )
    assert not violations, "dynamic imports/code execution detected:\n" + "\n".join(
        violations
    )


def test_v4_sql_does_not_read_or_write_legacy_decision_tables():
    violations: list[str] = []
    for path in _python_files(V4_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for line_number, value in _static_strings(tree):
            lowered = value.casefold()
            for table in FORBIDDEN_LEGACY_TABLES:
                if table in lowered:
                    violations.append(
                        f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {table}"
                    )
    assert not violations, "legacy decision table access detected:\n" + "\n".join(
        violations
    )


def test_legacy_decision_runtimes_do_not_import_v4_during_clean_room_build():
    legacy_roots = (
        PROJECT_ROOT / "server" / "trading_v2",
        PROJECT_ROOT / "server" / "trading_v3",
    )
    violations = [
        f"{path.relative_to(PROJECT_ROOT)} -> {module}"
        for legacy_root in legacy_roots
        for path in _python_files(legacy_root)
        for module in _imports(path)
        if module.startswith("server.trading_v4")
    ]
    assert not violations, (
        "a legacy runtime imported the clean-room runtime:\n"
        + "\n".join(violations)
    )
