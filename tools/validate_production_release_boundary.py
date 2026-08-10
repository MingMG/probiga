#!/usr/bin/env python3
"""Validate the non-database production boundary for Trading V4/V5/V6.

Passing means the three research generations remain byte-consistent and
unable to enter API, scheduler, action, or order paths.  It never means that a
model, strategy, paper trial, or production activation has passed.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.trading_v4.release_integrity import validate_v4_release
from tools.trading_v5.release_integrity import validate_v5_release
from tools.trading_v6.release_integrity import validate_v6_release


class ProductionBoundaryError(ValueError):
    """Raised when a research generation can leak into production."""


RUNTIME_CODE_ROOTS = (
    "server",
    "biz",
    "integrations",
    "tools",
)
RUNTIME_SCAN_EXCLUDED_PREFIXES = (
    "server/trading_v4/",
    "server/trading_v5/",
    "server/trading_v6/",
    "tools/trading_v4/",
    "tools/trading_v5/",
    "tools/trading_v6/",
    # These are explicitly isolated V4 forward-data adapters, not application
    # routes, schedulers, workers, or order-entry code.
    "server/integrations/v4_pit_sources/",
)
RUNTIME_SCAN_EXCLUDED_PATHS = {
    # This guard necessarily contains the forbidden names as policy data and
    # is exercised directly by _validate_scheduler_policy below.
    "server/common/scheduler_script_policy.py",
    # The validator necessarily imports each release validator.  It is a CI
    # gate, not an application or scheduler entry point.
    "tools/validate_production_release_boundary.py",
    # This explicitly named, scheduler-denied database acceptance harness is
    # kept for the deferred migration work and is not a production job.
    "tools/trading_v4_mysql_acceptance.py",
}
FORBIDDEN_IMPORT_PREFIXES = (
    "server.trading_v4",
    "server.trading_v5",
    "server.trading_v6",
    "tools.trading_v4",
    "tools.trading_v5",
    "tools.trading_v6",
)
PROTECTED_RELEASE_PATHS = (
    "server",
    "biz",
    "integrations",
    "tools",
    "scripts",
    "strategies",
    "versions",
    "artifacts/trading_v4",
    "artifacts/trading_v5",
    "artifacts/trading_v6",
    ".github",
    "deploy",
    "requirements-platform.txt",
    ".gitattributes",
    ".gitignore",
    "sitecustomize.py",
    "usercustomize.py",
    ":(top,glob)*.py",
    ":(top,glob)*.pyw",
    ":(top,glob)*.pyd",
    ":(top,glob)*.so",
    ":(top,glob)*/__init__.py",
    ":(top,glob)*/__init__*.pyc",
    ":(top,glob)*/__init__*.pyd",
    ":(top,glob)*/__init__*.so",
)
BYTECODE_SCAN_ROOTS = (
    "server",
    "biz",
    "integrations",
    "tools",
    "scripts",
    "strategies",
    "versions",
)
MUTABLE_RUNTIME_ROOTS = (
    "data",
    "runtime",
    "logs",
    "output",
    "outputs",
    "cache",
    ".cache",
    ".tmp",
    "tmp",
)
MUTABLE_RUNTIME_TRACKED_ALLOWLIST = frozenset({"data/.gitkeep"})


def validate_production_boundary(
    *,
    require_git_anchor: bool = False,
    expected_git_sha: str | None = None,
) -> dict[str, Any]:
    _validate_mutable_runtime_roots_untracked()
    releases = (
        validate_v4_release(),
        validate_v5_release(),
        validate_v6_release(),
    )
    for release in releases:
        document = release.document
        for field in (
            "activation_eligible",
            "paper_eligible",
            "production_eligible",
        ):
            if document.get(field) is not False:
                raise ProductionBoundaryError(
                    f"{document['release_id']} escaped closed field {field}"
                )
        if (
            document.get("lifecycle_status") != "RESEARCH_ONLY"
            or document.get("release_decision") != "BLOCK"
            or document.get("api_prefix") is not None
            or document.get("task_namespace") is not None
        ):
            raise ProductionBoundaryError(
                f"{document['release_id']} escaped the research-only boundary"
            )
        if (
            document.get("database_status") != "DEFERRED_MIGRATION_IN_PROGRESS"
            or document.get("database_tests_run") is not False
            or document.get("database_counts_as_pass") is not False
        ):
            raise ProductionBoundaryError(
                f"{document['release_id']} misstates deferred database evidence"
            )
        _validate_runtime_boundary(document)

    _validate_current_production_route()
    _validate_scheduler_policy()
    scanned_files = _validate_no_production_imports()
    tracked = _git_delivery_status(releases, expected_git_sha)
    if require_git_anchor and not tracked["ready"]:
        raise ProductionBoundaryError(tracked["reason"])
    return {
        "schema_version": "probiga.production-release-boundary.v1",
        "deployment_safety": "PASS",
        "activation_readiness": "BLOCK",
        "database": {
            "status": "DEFERRED_MIGRATION_IN_PROGRESS",
            "tests_run": False,
            "counts_as_pass": False,
        },
        "current_decision_route": "V3_ONLY_PAPER_TRIAL",
        "research_releases": [
            {
                "release_id": item.document["release_id"],
                "manifest_sha256": item.manifest_sha256,
                "integrity_status": item.status,
                "activation_eligible": False,
            }
            for item in releases
        ],
        "production_python_files_scanned": scanned_files,
        "git_delivery_anchor": tracked,
        "meaning": (
            "PASS only proves fail-closed deployment boundaries; all V4/V5/V6 "
            "activation remains BLOCK"
        ),
    }


def _validate_current_production_route() -> None:
    v3 = _strict_json(ROOT / "strategies/trading_v3.json")
    if v3.get("lifecycle_status") != "PAPER_TRIAL":
        raise ProductionBoundaryError("current Trading V3 lifecycle is not PAPER_TRIAL")
    if v3.get("automatic_real_order_submission") is not False:
        raise ProductionBoundaryError("Trading V3 real-order submission is open")
    routes = _mapping(v3.get("production_routes"), "V3 production_routes")
    expected = {
        "decision_engine": "V3_ONLY",
        "legacy_v2_entry_enabled": False,
        "validated_intraday_entry_enabled": False,
    }
    if routes != expected:
        raise ProductionBoundaryError("current production decision routing drifted")
    discovery = _mapping(v3.get("paper_discovery"), "V3 paper_discovery")
    if discovery.get("real_order_allowed") is not False:
        raise ProductionBoundaryError("Trading V3 discovery allows real orders")
    paper = _mapping(v3.get("paper_execution"), "V3 paper_execution")
    if paper.get("real_order_allowed") is not False:
        raise ProductionBoundaryError("Trading V3 paper path allows real orders")


def _validate_runtime_boundary(document: Mapping[str, Any]) -> None:
    """Independently verify the release runtime remains non-actionable."""

    runtime_path = ROOT / str(document.get("config_path", ""))
    runtime = _strict_json(runtime_path)
    expected_hash = document.get("config_sha256")
    if (
        not isinstance(expected_hash, str)
        or hashlib.sha256(runtime_path.read_bytes()).hexdigest() != expected_hash
    ):
        raise ProductionBoundaryError(
            f"{document['release_id']} runtime digest differs from its manifest"
        )
    expected_identity = {
        "system": document["system"],
        "release_id": document["release_id"],
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "api_prefix": None,
        "task_namespace": None,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
    }
    for name, expected in expected_identity.items():
        if runtime.get(name) != expected or type(runtime.get(name)) is not type(expected):
            raise ProductionBoundaryError(
                f"{document['release_id']} runtime escaped field {name}"
            )
    system = document["system"]
    if system == "trading_v4":
        evidence_gates = _mapping(
            runtime.get("evidence_gates"),
            "runtime evidence_gates",
        )
        database = _mapping(
            evidence_gates.get("database"),
            "runtime database",
        )
    else:
        database = _mapping(runtime.get("database"), "runtime database")
    expected_database = {
        "status": "DEFERRED_MIGRATION_IN_PROGRESS",
        "tests_run": False,
        "counts_as_pass": False,
    }
    if system == "trading_v4":
        expected_database["migration_head_recorded_only"] = (
            "20260804_007_v4_factor_lineage"
        )
    if database != expected_database:
        raise ProductionBoundaryError(
            f"{document['release_id']} runtime misstates database evidence"
        )
    execution = _mapping(
        runtime.get("execution_boundary"),
        "runtime execution_boundary",
    )
    expected_execution = {
        "trading_v4": {
            "expected_return_estimation": False,
            "probability_estimation": False,
            "forecast_emission": False,
            "action_emission": False,
            "execution_intent_emission": False,
            "paper_order_submission": False,
            "real_order_submission": False,
            "v2_or_v3_runtime_write": False,
        },
        "trading_v5": {
            "actionable_output_allowed": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
            "order_intents_allowed": False,
            "v2_v3_v4_runtime_imports_allowed": False,
        },
        "trading_v6": {
            "actionable_output_allowed": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
            "order_intents_allowed": False,
            "v2_v3_v4_v5_runtime_imports_allowed": False,
        },
    }.get(system)
    if expected_execution is None or execution != expected_execution:
        raise ProductionBoundaryError(
            f"{document['release_id']} runtime execution boundary is open"
        )


def _validate_scheduler_policy() -> None:
    from server.common.scheduler_script_policy import (
        SchedulerScriptPolicyError,
        resolve_scheduler_script,
    )

    blocked = (
        "tools/trading_v4/run_research.py",
        "server/trading_v5/models.py",
        "tools/research_trading_v6_campaign.py",
        "../tools/job.py",
    )
    for relative_path in blocked:
        try:
            resolve_scheduler_script(ROOT, relative_path)
        except SchedulerScriptPolicyError:
            continue
        raise ProductionBoundaryError(
            f"scheduler path policy accepted forbidden path {relative_path}"
        )


def _validate_no_production_imports() -> int:
    count = 0
    violations: list[str] = []
    for relative_root in RUNTIME_CODE_ROOTS:
        for path in sorted((ROOT / relative_root).rglob("*.py")):
            relative_path = path.relative_to(ROOT).as_posix()
            if relative_path in RUNTIME_SCAN_EXCLUDED_PATHS:
                continue
            if any(
                relative_path.startswith(prefix)
                for prefix in RUNTIME_SCAN_EXCLUDED_PREFIXES
            ):
                continue
            count += 1
            try:
                source = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError, SyntaxError) as exc:
                raise ProductionBoundaryError(
                    f"cannot inspect production source {path.relative_to(ROOT)}: {exc}"
                ) from exc
            module_name, is_package = _module_identity(relative_path)
            try:
                names = _forbidden_imports_from_source(
                    source,
                    module_name=module_name,
                    is_package=is_package,
                )
            except SyntaxError as exc:
                raise ProductionBoundaryError(
                    f"cannot inspect production source {relative_path}: {exc}"
                ) from exc
            violations.extend(f"{relative_path} -> {name}" for name in names)
            mutable_adata = _mutable_adata_path_injections_from_source(source)
            violations.extend(
                f"{relative_path} -> mutable adata path injection"
                for _item in mutable_adata
            )
    if violations:
        raise ProductionBoundaryError(
            "research packages entered production code: " + "; ".join(violations)
        )
    return count


def _module_identity(relative_path: str) -> tuple[str, bool]:
    path = Path(relative_path)
    parts = list(path.with_suffix("").parts)
    is_package = bool(parts and parts[-1] == "__init__")
    if is_package:
        parts.pop()
    return ".".join(parts), is_package


def _forbidden_imports_from_source(
    source: str,
    *,
    module_name: str,
    is_package: bool = False,
) -> tuple[str, ...]:
    """Return static and constant dynamic imports into V4/V5/V6 packages."""

    tree = ast.parse(source)
    names: set[str] = set()
    importlib_names = {"importlib"}
    dynamic_loader_names = {"__import__"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
                if alias.name == "importlib":
                    importlib_names.add(alias.asname or "importlib")
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_import_from(node, module_name, is_package)
            if base:
                names.add(base)
            for alias in node.names:
                if alias.name != "*":
                    names.add(f"{base}.{alias.name}" if base else alias.name)
                if base == "importlib" and alias.name == "import_module":
                    dynamic_loader_names.add(alias.asname or alias.name)
        if isinstance(node, (ast.Constant, ast.BinOp)):
            static_value = _constant_string(node)
            if static_value:
                names.add(static_value)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        function = node.func
        is_loader = (
            isinstance(function, ast.Name)
            and function.id in dynamic_loader_names
        ) or (
            isinstance(function, ast.Attribute)
            and function.attr == "import_module"
            and isinstance(function.value, ast.Name)
            and function.value.id in importlib_names
        )
        if is_loader:
            value = _constant_string(node.args[0])
            if value:
                names.add(value)

    return tuple(sorted(
        name
        for name in names
        if any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in FORBIDDEN_IMPORT_PREFIXES
        )
    ))


def _mutable_adata_path_injections_from_source(source: str) -> tuple[str, ...]:
    """Find direct attempts to put the ignored nested checkout on sys.path."""
    tree = ast.parse(source)
    findings: set[str] = set()
    imports_adata = False
    mutates_sys_path = False
    uses_verified_resolver = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports_adata = imports_adata or any(
                alias.name == "adata" or alias.name.startswith("adata.")
                for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom):
            imports_adata = imports_adata or bool(
                node.module
                and (node.module == "adata" or node.module.startswith("adata."))
            )
        elif isinstance(node, ast.Name) and node.id == "ensure_adata_import_path":
            uses_verified_resolver = True
        if not isinstance(node, ast.Call):
            continue
        rendered = ast.unparse(node)
        if "sys.path" in rendered and any(
            marker in rendered
            for marker in (".insert(", ".append(", ".extend(")
        ):
            mutates_sys_path = True
        constants = {
            child.value
            for child in ast.walk(node)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        }
        if "adata" not in constants:
            continue
        if "sys.path" in rendered and any(
            marker in rendered
            for marker in (".insert(", ".append(", ".extend(")
        ):
            findings.add(rendered)
        if any(keyword.arg == "extra_python_paths" for keyword in node.keywords):
            findings.add(rendered)
    if imports_adata and mutates_sys_path and not uses_verified_resolver:
        findings.add("adata import follows an unverified sys.path mutation")
    return tuple(sorted(findings))


def _resolve_import_from(
    node: ast.ImportFrom,
    module_name: str,
    is_package: bool,
) -> str:
    if not node.level:
        return node.module or ""
    package = module_name if is_package else module_name.rpartition(".")[0]
    relative = "." * node.level + (node.module or "")
    try:
        return importlib.util.resolve_name(relative, package)
    except (ImportError, ValueError):
        return relative


def _constant_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_string(node.left)
        right = _constant_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _validate_mutable_runtime_roots_untracked() -> tuple[str, ...]:
    """Reject live Git-tracked files in mutable runtime/output roots.

    A local migration may leave an unstaged deletion in the index until the
    cleanup commit is created.  Such paths are excluded only when Git itself
    reports them deleted relative to ``HEAD``; a clean CI checkout therefore
    cannot use this exception.
    """
    tracked = {
        path
        for path in _git(
            "ls-files",
            "-z",
            "--",
            *MUTABLE_RUNTIME_ROOTS,
        ).split("\0")
        if path
    }
    deleted = {
        path
        for path in _git(
            "diff",
            "--name-only",
            "--diff-filter=D",
            "-z",
            "HEAD",
            "--",
            *MUTABLE_RUNTIME_ROOTS,
        ).split("\0")
        if path
    }
    violations = tuple(
        sorted(
            tracked
            - MUTABLE_RUNTIME_TRACKED_ALLOWLIST
            - deleted
        )
    )
    if violations:
        raise ProductionBoundaryError(
            "mutable runtime/output roots contain Git-tracked files: "
            f"{list(violations[:8])}"
        )
    return tuple(sorted(tracked - deleted))


def _git_delivery_status(releases: Sequence[Any], expected_sha: str | None) -> dict[str, Any]:
    try:
        head = _git("rev-parse", "HEAD")
        if expected_sha and head != expected_sha:
            return {
                "ready": False,
                "head": head,
                "reason": "checked-out HEAD differs from the workflow commit",
            }
        required = _required_release_files(releases)
        tracked = set(_git("ls-files", "--", *sorted(required)).splitlines())
        missing = sorted(required - tracked)
        if missing:
            return {
                "ready": False,
                "head": head,
                "reason": f"release files are not Git-tracked: {missing[:8]}",
            }
        if _git(
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            *PROTECTED_RELEASE_PATHS,
        ):
            return {
                "ready": False,
                "head": head,
                "reason": "protected release files differ from Git HEAD",
            }
        untracked_code = _git(
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            "server",
            "biz",
            "integrations",
            "tools",
            "scripts",
            "sitecustomize.py",
            "usercustomize.py",
        ).splitlines()
        unsafe = sorted(
            path for path in untracked_code
            if path.lower().endswith((".py", ".pyw", ".pyc", ".pyd", ".so"))
        )
        if unsafe:
            return {
                "ready": False,
                "head": head,
                "reason": f"untracked executable code exists: {unsafe[:8]}",
            }
        root_shadows = _untracked_root_shadow_files()
        if root_shadows:
            return {
                "ready": False,
                "head": head,
                "reason": f"untracked root import shadows exist: {root_shadows[:8]}",
            }
        return {"ready": True, "head": head, "reason": None}
    except (OSError, subprocess.CalledProcessError) as exc:
        return {"ready": False, "head": None, "reason": f"Git check failed: {exc}"}


def _required_release_files(releases: Sequence[Any]) -> set[str]:
    required: set[str] = set()
    for release in releases:
        document = release.document
        required.update(document["source_files"])
        required.update(document["config_files"])
        required.update(document["test_files"])
        required.add(document["test_manifest_path"])
        required.add(
            f"versions/{document['system']}/releases/"
            f"{document['release_id']}/manifest.json"
        )
        historical = document.get("historical_evidence")
        if isinstance(historical, Mapping):
            required.add(historical["path"])
        for item in (document.get("historical_evidence_files") or {}).values():
            required.add(item["path"])
        for item in (document.get("historical_dependency_files") or {}).values():
            required.add(item["path"])
    return required


def _untracked_root_shadow_files() -> list[str]:
    tracked = set(
        _git(
            "ls-files",
            "--",
            ":(top,glob)*.py",
            ":(top,glob)*.pyw",
            ":(top,glob)*.pyc",
            ":(top,glob)*.pyd",
            ":(top,glob)*.so",
            ":(top,glob)*/__init__.py",
            ":(top,glob)*/__init__*.pyc",
            ":(top,glob)*/__init__*.pyd",
            ":(top,glob)*/__init__*.so",
        ).splitlines()
    )
    suffixes = (".py", ".pyw", ".pyc", ".pyd", ".so")
    candidates: set[str] = set()
    for child in ROOT.iterdir():
        if child.is_file() and child.suffix.lower() in suffixes:
            candidates.add(child.name)
        elif child.is_dir():
            init_file = child / "__init__.py"
            if init_file.is_file():
                candidates.add(init_file.relative_to(ROOT).as_posix())
            for pattern in ("*.pyc", "__init__*.pyd", "__init__*.so"):
                candidates.update(
                    item.relative_to(ROOT).as_posix()
                    for item in child.glob(pattern)
                )
    for root_name in BYTECODE_SCAN_ROOTS:
        code_root = ROOT / root_name
        if not code_root.is_dir():
            continue
        for pattern in ("*.pyc", "*.pyo"):
            candidates.update(
                item.relative_to(ROOT).as_posix()
                for item in code_root.rglob(pattern)
            )
    return sorted(candidates - tracked)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True,
        text=True, encoding="utf-8", errors="strict",
    ).stdout.strip()


def _strict_json(path: Path) -> Mapping[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ProductionBoundaryError(f"duplicate JSON key in {path}: {key}")
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise ProductionBoundaryError(f"non-finite JSON value in {path}: {value}")

    try:
        return _mapping(
            json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=pairs,
                parse_constant=constant,
            ),
            str(path),
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionBoundaryError(f"invalid strict JSON: {path}") from exc


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProductionBoundaryError(f"{label} must be an object")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-git-anchor", action="store_true")
    parser.add_argument("--expected-git-sha")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = validate_production_boundary(
            require_git_anchor=args.require_git_anchor,
            expected_git_sha=args.expected_git_sha,
        )
    except (OSError, ValueError) as exc:
        print(f"production_release_boundary=FAILED error={exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, allow_nan=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
