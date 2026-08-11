#!/usr/bin/env python3
"""Render (never execute) the frozen MySQL 5.7 V4 runtime grant plan."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.integrations.v4_database_roles import (
    ROLE_MANIFEST_HASH,
    ROLE_MANIFEST_VERSION,
    V4RuntimeDatabaseRole,
    V4RuntimeRoleContractError,
    render_mysql57_grant_plan,
)


_TEST_DATABASE = re.compile(r"^[A-Za-z0-9_]*_v4_(?:test|ci)[A-Za-z0-9_]*$")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise V4RuntimeRoleContractError(
                f"principal JSON contains duplicate key: {key}"
            )
        result[key] = value
    return result


def load_principals(path: str | Path) -> dict[V4RuntimeDatabaseRole, tuple[str, str]]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        value = json.loads(raw, object_pairs_hook=_pairs)
    except V4RuntimeRoleContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise V4RuntimeRoleContractError(
            "unable to read strict runtime-principal JSON"
        ) from exc
    if not isinstance(value, Mapping) or set(value) != {
        role.value for role in V4RuntimeDatabaseRole
    }:
        raise V4RuntimeRoleContractError(
            "principal JSON must contain exactly the five runtime roles"
        )
    result: dict[V4RuntimeDatabaseRole, tuple[str, str]] = {}
    for role in V4RuntimeDatabaseRole:
        item = value[role.value]
        if not isinstance(item, Mapping) or set(item) != {"user", "host"}:
            raise V4RuntimeRoleContractError(
                f"principal {role.value} must contain exactly user and host"
            )
        if type(item["user"]) is not str or type(item["host"]) is not str:
            raise V4RuntimeRoleContractError("principal values must be strings")
        result[role] = (item["user"], item["host"])
    return result


def render_report(*, database: str, principal_path: str | Path) -> dict[str, Any]:
    if _TEST_DATABASE.fullmatch(database) is None:
        raise V4RuntimeRoleContractError(
            "grant rendering requires an explicit *_v4_test* or *_v4_ci* database"
        )
    statements = render_mysql57_grant_plan(
        database=database,
        principals=load_principals(principal_path),
    )
    return {
        "status": "DBA_REVIEW_REQUIRED",
        "database": database,
        "manifest_version": ROLE_MANIFEST_VERSION,
        "manifest_hash": ROLE_MANIFEST_HASH,
        "statements": statements,
        "statement_count": len(statements),
        "contains_passwords": False,
        "clears_mysql57_proxy_privileges": True,
        "requires_post_grant_runtime_audit": True,
        "executed": False,
        "production_activation_allowed": False,
        "actionable_output_allowed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render but never execute the frozen V4 runtime grant plan"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--principals", required=True)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            render_report(
                database=args.database,
                principal_path=args.principals,
            ),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
