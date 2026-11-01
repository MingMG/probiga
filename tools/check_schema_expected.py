#!/usr/bin/env python3
"""Read-only schema-surface guard for the fast production release path.

The runtime schema bundle already owns the code-side table and column
contracts.  This tool runs those validators without invoking migrations,
recovery planners, seeds, or task changes, so a missed DDL classification is
detected before Linux activation.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable
from typing import Any


SCHEMA = "probiga.release-schema-guard.v1"
EXIT_MISMATCH = 2
EXIT_UNAVAILABLE = 3


class SchemaGuardError(RuntimeError):
    """A fixed, non-sensitive schema guard failure."""


def _bundle_validators() -> tuple[tuple[str, Callable[[Any], Any]], ...]:
    from server.common import production_runtime_schema_bundle

    return production_runtime_schema_bundle._VALIDATORS


def collect_schema_guard(
    engine: Any,
    *,
    validators: Iterable[tuple[str, Callable[[Any], Any]]] | None = None,
) -> dict[str, object]:
    """Run every code-owned runtime schema validator without changing state."""
    from server.common import production_runtime_schema_bundle as bundle

    entries = tuple(validators if validators is not None else _bundle_validators())
    if not entries:
        raise SchemaGuardError("required_schema_inventory_empty")
    if len({name for name, _ in entries}) != len(entries):
        raise SchemaGuardError("required_schema_inventory_duplicate")

    checks: dict[str, dict[str, object]] = {}
    for name, validator in entries:
        if not isinstance(name, str) or not name or not callable(validator):
            raise SchemaGuardError("required_schema_inventory_invalid")
        try:
            validator(engine)
        except Exception as exc:
            checks[name] = {
                "status": "MIGRATION_REQUIRED",
                "error_type": type(exc).__name__,
                "read_only": True,
            }
        else:
            checks[name] = {
                "status": "READY",
                "read_only": True,
            }

    metadata = bundle._contract_metadata()
    mismatch_count = sum(
        item["status"] != "READY" for item in checks.values()
    )
    return {
        "schema": SCHEMA,
        "status": "READY" if mismatch_count == 0 else "MIGRATION_REQUIRED",
        "bundle_schema": metadata["schema"],
        "contract_sha256": metadata["contract_hash"],
        "check_count": len(checks),
        "mismatch_count": mismatch_count,
        "checks": checks,
        "read_only": True,
    }


def _open_read_only_boundary() -> Any:
    from tools import prepare_strategy_governance_schema as boundary_policy

    return boundary_policy._open_boundary(
        include_migrator=True,
        expected_trust=0,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--print-required-columns",
        action="store_true",
        help="print the same code-owned required surface inventory as JSON",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the schema guard result as JSON (the default format)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    boundary = None
    try:
        boundary = _open_read_only_boundary()
        # Runtime has SELECT but no TRIGGER privilege, so MySQL hides trigger
        # metadata from information_schema. The read-only migrator connection
        # is required to validate the append-only trigger contracts.
        engine = boundary.migrator_engine
        if engine is None:
            raise SchemaGuardError("required_schema_inventory_unavailable")
        result = collect_schema_guard(engine)
    except SchemaGuardError as exc:
        print(json.dumps({
            "schema": SCHEMA,
            "status": "UNAVAILABLE",
            "error_code": str(exc),
            "read_only": True,
        }, sort_keys=True))
        return EXIT_UNAVAILABLE
    except Exception as exc:
        # Database diagnostics can contain credentials or topology. Preserve
        # only a fixed status and exception type for the release operator.
        print(json.dumps({
            "schema": SCHEMA,
            "status": "UNAVAILABLE",
            "error_code": "schema_guard_database_unavailable",
            "error_type": type(exc).__name__,
            "read_only": True,
        }, sort_keys=True))
        return EXIT_UNAVAILABLE
    finally:
        if boundary is not None:
            engines = {
                id(candidate): candidate
                for candidate in (
                    boundary.runtime_engine,
                    boundary.migrator_engine,
                )
                if candidate is not None
            }
            for engine in engines.values():
                try:
                    engine.dispose()
                except Exception:
                    pass

    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "READY" else EXIT_MISMATCH


if __name__ == "__main__":
    raise SystemExit(main())
