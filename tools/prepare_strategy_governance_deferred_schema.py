#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare the governance base schema while trigger installation is deferred.

This tool intentionally uses the application's current ``MYSQL_URL``.  It
does not read the trigger-administrator or migrator option files and it never
executes trigger DDL.  The full trigger phase remains a separate production
maintenance operation.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.pool import NullPool


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env  # noqa: E402
from server.common.strategy_governance_mode import (  # noqa: E402
    StrategyGovernanceMode,
    get_strategy_governance_mode,
)


MODE = "DEFERRED_DB_BASE_SCHEMA"
DATABASE_NAME = "probiga"


class DeferredBaseSchemaError(RuntimeError):
    """Safe public failure for the explicitly deferred schema phase."""


def _governance_api():
    # Import only after project configuration has been loaded by the caller.
    from server.engine.strategy_governance import (
        GOVERNANCE_TABLE_NAMES,
        ensure_strategy_governance_tables,
        seed_governance_registry,
        validate_deferred_governance_base_schema,
        validate_deferred_governance_trigger_inventory,
    )
    from server.engine.dynamic_shadow_ledger_schema import (
        DYNAMIC_SHADOW_LEDGER_TABLE_NAMES,
    )
    from server.engine.strategy_funding_checkpoint import (
        FUNDING_CHECKPOINT_TABLE_NAME,
        FUNDING_DAILY_FACT_TABLE_NAME,
    )
    from server.common.production_runtime_schema_bundle import (
        privileged_migrate_runtime_schema_bundle,
        validate_runtime_schema_bundle,
    )

    return {
        "core_tables": tuple(GOVERNANCE_TABLE_NAMES),
        "dynamic_tables": tuple(DYNAMIC_SHADOW_LEDGER_TABLE_NAMES),
        "funding_tables": (
            FUNDING_DAILY_FACT_TABLE_NAME,
            FUNDING_CHECKPOINT_TABLE_NAME,
        ),
        "ensure": ensure_strategy_governance_tables,
        "seed": seed_governance_registry,
        "validate": validate_deferred_governance_base_schema,
        "validate_triggers": validate_deferred_governance_trigger_inventory,
        "migrate_runtime_bundle": privileged_migrate_runtime_schema_bundle,
        "validate_runtime_bundle": validate_runtime_schema_bundle,
    }


def _new_engine():
    return create_tool_engine(future=True, poolclass=NullPool)


def _identity(engine) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT DATABASE() AS database_name, "
            "CURRENT_USER() AS authenticated_user, VERSION() AS mysql_version, "
            "@@GLOBAL.read_only AS global_read_only"
        )).mappings().one()
    database_name = str(row.get("database_name") or "")
    authenticated_user = str(row.get("authenticated_user") or "")
    mysql_version = str(row.get("mysql_version") or "")
    if database_name != DATABASE_NAME:
        raise DeferredBaseSchemaError(
            "current primary database is not the probiga schema"
        )
    if not authenticated_user or not mysql_version:
        raise DeferredBaseSchemaError(
            "current primary database identity is incomplete"
        )
    return {
        "database_name": database_name,
        "mysql_version": mysql_version,
        "runtime_identity_verified": True,
        "database_read_only": bool(int(row.get("global_read_only") or 0)),
    }


def _preflight(engine, api: dict[str, Any]) -> dict[str, Any]:
    table_names = (
        tuple(api["core_tables"])
        + tuple(api["dynamic_tables"])
        + tuple(api["funding_tables"])
    )
    params = {
        f"table_{index}": name
        for index, name in enumerate(table_names)
    }
    placeholders = ", ".join(f":table_{index}" for index in range(len(params)))
    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ("
            f"{placeholders}) ORDER BY BINARY TABLE_NAME"
        ), params).mappings().all()
        trigger_detail = api["validate_triggers"](connection)
    existing_tables = sorted(
        str(row.get("table_name") or "") for row in rows
    )
    return {
        "read_only": True,
        "existing_table_count": len(existing_tables),
        "expected_table_count": len(table_names),
        "missing_table_count": len(table_names) - len(existing_tables),
        "existing_table_names": existing_tables,
        "installed_trigger_count": int(
            trigger_detail["installed_trigger_count"]
        ),
        "missing_trigger_count": int(
            trigger_detail["missing_trigger_count"]
        ),
        "trigger_metadata_valid": bool(
            trigger_detail["installed_trigger_metadata_valid"]
        ),
    }


def _verified_payload(engine, api: dict[str, Any], *, action: str) -> dict[str, Any]:
    identity = _identity(engine)
    detail = api["validate"](engine)
    runtime_bundle = api["validate_runtime_bundle"](engine)
    missing_trigger_count = int(detail.get("missing_trigger_count") or 0)
    if missing_trigger_count <= 0:
        raise DeferredBaseSchemaError(
            "deferred base schema must retain a positive trigger gap"
        )
    return {
        "status": "ok",
        "mode": MODE,
        "action": action,
        "schema_ready_without_triggers": True,
        "missing_trigger_count": missing_trigger_count,
        "trigger_installation_deferred": True,
        "trigger_installation_asserted": False,
        "database_triggers_installed": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "identity": identity,
        "runtime_schema_bundle_validation": runtime_bundle,
        **detail,
    }


def prepare_deferred_base_schema(
    *,
    apply: bool,
    writers_fenced: bool,
) -> dict[str, Any]:
    if type(apply) is not bool or type(writers_fenced) is not bool:
        raise TypeError("apply and writers_fenced must be bool")
    if not apply and writers_fenced:
        raise DeferredBaseSchemaError(
            "read-only verification does not accept a writer-fence assertion"
        )
    if apply and not writers_fenced:
        raise DeferredBaseSchemaError(
            "base-schema writes require the verified writer fence"
        )

    load_project_env()
    if get_strategy_governance_mode() is not StrategyGovernanceMode.DEFERRED_DB:
        raise DeferredBaseSchemaError(
            "base-schema-only preparation requires DEFERRED_DB mode"
        )
    api = _governance_api()
    engine = _new_engine()
    try:
        identity = _identity(engine)
        if apply and bool(identity["database_read_only"]):
            raise DeferredBaseSchemaError(
                "current primary database is read-only"
            )
        if not apply:
            return _verified_payload(engine, api, action="verify")
        preflight = _preflight(engine, api)
        runtime_bundle = api["migrate_runtime_bundle"](engine)
        api["ensure"](
            engine=engine,
            writers_fenced=True,
            defer_triggers=True,
        )
        api["seed"](engine=engine)
    finally:
        engine.dispose()

    # A fresh pool/connection proves that the committed structures, seed rows
    # and version markers are visible independently from the writer session.
    verify_engine = _new_engine()
    try:
        result = _verified_payload(verify_engine, api, action="apply")
    finally:
        verify_engine.dispose()
    result["preflight"] = preflight
    result["runtime_schema_bundle"] = runtime_bundle
    result["fresh_connection_verified"] = True
    return result


def _failure_payload(exc: BaseException, *, action: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "mode": MODE,
        "action": action,
        "schema_ready_without_triggers": False,
        "reason": (
            f"{type(exc).__name__}: deferred base schema failed closed"
        ),
        "trigger_installation_deferred": True,
        "trigger_installation_asserted": False,
        "database_triggers_installed": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--apply", action="store_true")
    action.add_argument("--verify", action="store_true")
    parser.add_argument(
        "--writers-fenced",
        action="store_true",
        help="required for --apply after every application writer is drained",
    )
    args = parser.parse_args(argv)
    action_name = "apply" if args.apply else "verify"
    try:
        result = prepare_deferred_base_schema(
            apply=bool(args.apply),
            writers_fenced=bool(args.writers_fenced),
        )
    except Exception as exc:
        print(json.dumps(
            _failure_payload(exc, action=action_name),
            ensure_ascii=False,
        ))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
