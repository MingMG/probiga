"""Static activation guard for the legacy direct V2-to-V3 synchronization."""
from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


LEGACY_DIRECT_SYNC_NAME = "_sync_v3_execution_plan_states"
OUTBOX_RUNTIME_ENABLED = False
PRODUCTION_ACTIVATION_ALLOWED = False


class LegacyDirectSyncStillActiveError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class LegacyDirectSyncStatus:
    module_path: str
    function_definitions: int
    import_sites: int
    direct_call_sites: int
    referenced_files: tuple[str, ...]
    outbox_runtime_enabled: bool
    production_activation_allowed: bool


def inspect_legacy_direct_sync(
    module_path: str | Path | None = None,
) -> LegacyDirectSyncStatus:
    target = (
        Path(module_path)
        if module_path is not None
        else Path(__file__).resolve().parents[2]
    )
    paths = (
        (target,)
        if target.is_file()
        else tuple(sorted(target.rglob("*.py"), key=lambda item: item.as_posix()))
    )
    if not paths:
        raise LegacyDirectSyncStillActiveError(
            "legacy direct-sync inventory target contains no Python files"
        )
    definitions = 0
    imports = 0
    calls = 0
    referenced: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_aliases = {LEGACY_DIRECT_SYNC_NAME}
        file_definitions = 0
        file_imports = 0
        file_calls = 0
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == LEGACY_DIRECT_SYNC_NAME
            ):
                file_definitions += 1
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == LEGACY_DIRECT_SYNC_NAME:
                        file_imports += 1
                        imported_aliases.add(alias.asname or alias.name)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if (
                isinstance(node.func, ast.Name)
                and node.func.id in imported_aliases
            ) or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == LEGACY_DIRECT_SYNC_NAME
            ):
                file_calls += 1
        if file_definitions or file_imports or file_calls:
            referenced.append(str(path.resolve()))
        definitions += file_definitions
        imports += file_imports
        calls += file_calls
    return LegacyDirectSyncStatus(
        module_path=str(target.resolve()),
        function_definitions=definitions,
        import_sites=imports,
        direct_call_sites=calls,
        referenced_files=tuple(referenced),
        outbox_runtime_enabled=OUTBOX_RUNTIME_ENABLED,
        production_activation_allowed=PRODUCTION_ACTIVATION_ALLOWED,
    )


def require_outbox_replacement_safe(
    module_path: str | Path | None = None,
) -> LegacyDirectSyncStatus:
    status = inspect_legacy_direct_sync(module_path)
    if (
        status.function_definitions
        or status.import_sites
        or status.direct_call_sites
    ):
        raise LegacyDirectSyncStillActiveError(
            "legacy direct V2-to-V3 synchronization is still present; "
            "outbox runtime activation remains blocked"
        )
    if not status.outbox_runtime_enabled:
        raise LegacyDirectSyncStillActiveError(
            "outbox runtime remains explicitly disabled pending acceptance"
        )
    if status.production_activation_allowed:
        raise LegacyDirectSyncStillActiveError(
            "projection replacement may not enable production actions"
        )
    return status


__all__ = [
    "LEGACY_DIRECT_SYNC_NAME",
    "OUTBOX_RUNTIME_ENABLED",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "LegacyDirectSyncStatus",
    "LegacyDirectSyncStillActiveError",
    "inspect_legacy_direct_sync",
    "require_outbox_replacement_safe",
]
