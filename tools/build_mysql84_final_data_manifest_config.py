#!/usr/bin/env python3
"""Build a fact-pinned final MySQL 5.5 -> 8.4 data-manifest policy.

Every base table receives an exact row count and primary-key boundary check.
Small tables and business-authority tables receive full ordered logical
SHA-256 coverage. Large market/history tables with a single integer primary key
receive deterministic primary-key window hashes; other large primary-key
layouts receive a deterministic CRC-selected sample. All date/time columns
receive min/max/null boundaries. The result contains no credentials.  A fresh
target must match the source catalogue exactly; the one explicitly supported
exception is the already-applied, repository-pinned V2/V3/V4 target schema.
That target is compared through the complete source-table/source-column
projection while its full post-migration catalogue is separately pinned.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mysql55_to_mysql84_data_manifest import (  # noqa: E402
    KNOWN_MYSQL84_ZERO_DATE_COLUMNS,
    _connect,
    inspect_identity,
    load_catalog,
)


SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
SOURCE_VERSION = "5.5.20-log"
TARGET_VERSION = "8.4.11"
FULL_HASH_MAX_BYTES = 16 * 1024 * 1024
CRC_SAMPLE_MODULUS = 4096
_INTEGER_TYPES = frozenset({"tinyint", "smallint", "mediumint", "int", "integer", "bigint"})
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_AUTHORITY_TOKEN_RE = re.compile(
    r"(?:^|_)(?:account|cash|decision|execution|hypothesis|intent|ledger|order|"
    r"portfolio|position|risk|trade)(?:_|$)",
    re.IGNORECASE,
)
_DATE_TYPES = frozenset({"date", "datetime", "timestamp"})

# This is the exact forward-only schema delta produced by the reviewed
# 2026-08-03/04 V2/V3/V4 migrations.  Keeping the list here deliberately makes
# a later or unrelated target addition fail closed instead of silently turning
# "target is a superset" into a broad exception.
KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES = frozenset(
    {
        "probiga.schema_migration_v2_maintenance_fence",
        "probiga.schema_migration_v3_progress",
        "probiga.schema_migration_v4",
        "probiga.st_cash_event_binding_v2",
        "probiga.st_counterfactual_queue_v3",
        "probiga.st_counterfactual_learning_run_v3",
        "probiga.st_calibration_gate_v3",
        "probiga.st_data_source_certification_v4",
        "probiga.st_decision_channel_head_v4",
        "probiga.st_decision_context_v4",
        "probiga.st_decision_run_v4",
        "probiga.st_entity_feature_snapshot_v4",
        "probiga.st_execution_authority_attestation_v2",
        "probiga.st_execution_authority_key_revocation_v2",
        "probiga.st_execution_authority_receipt_revocation_v2",
        "probiga.st_execution_authority_receipt_v2",
        "probiga.st_execution_authority_trust_key_v2",
        "probiga.st_execution_plan_binding_v3",
        "probiga.st_execution_projection_dead_letter_reconciliation_v3",
        "probiga.st_execution_projection_head_v3",
        "probiga.st_execution_projection_inbox_v3",
        "probiga.st_execution_projection_order_baseline_v3",
        "probiga.st_execution_projection_outbox_v2",
        "probiga.st_execution_projection_worker_checkpoint_v3",
        "probiga.st_factor_definition_v4",
        "probiga.st_fill_accounting_outcome_finalization_v2",
        "probiga.st_fill_accounting_outcome_v2",
        "probiga.st_fill_execution_evidence_v2",
        "probiga.st_forward_trade_evidence_v3",
        "probiga.st_horizon_forecast_contract_v3",
        "probiga.st_horizon_model_artifact_v3",
        "probiga.st_horizon_outcome_v3",
        "probiga.st_job_claim_token_v4",
        "probiga.st_job_run_v4",
        "probiga.st_lot_transition_evidence_v2",
        "probiga.st_market_calendar_evidence_v2",
        "probiga.st_order_transition_v2",
        "probiga.st_quote_receipt_evidence_v2",
        "probiga.st_runtime_control_transition_v4",
        "probiga.st_runtime_control_v4",
        "probiga.st_shadow_portfolio_v3",
        "probiga.st_shadow_release_v3",
        "probiga.st_source_watermark_v4",
        "probiga.st_theme_signal_v3",
    }
)
KNOWN_POST_MIGRATION_EXTENDED_COLUMNS = {
    "probiga.schema_migration_v3": ("statement_count",),
    "probiga.st_counterfactual_v3": (
        "evidence_kind",
        "selection_status",
        "execution_status",
        "protocol_version",
    ),
    "probiga.st_decision_run_v3": (
        "requested_as_of",
        "config_hash",
        "code_commit_sha",
        "calibration_set_hash",
    ),
    "probiga.st_news_flash": ("first_seen_at",),
    "probiga.st_opportunity_recall_v3": (
        "strategy_key",
        "evidence_kind",
        "protocol_version",
    ),
    "probiga.st_target_portfolio_v3": (
        "theme_codes_json",
        "primary_strategy_key",
        "primary_forecast_id",
        "attribution_snapshot_hash",
    ),
}


class ConfigBuildError(RuntimeError):
    """Live identity/catalogue facts cannot safely produce a final policy."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def classify_hash_mode(table_ref: str, *, allocated_bytes: int) -> str:
    table = table_ref.split(".", 1)[1]
    if _AUTHORITY_TOKEN_RE.search(table) or allocated_bytes <= FULL_HASH_MAX_BYTES:
        return "full_ordered_sha256"
    return "deterministic_pk_windows_sha256"


def _supports_pk_windows(table_item: Mapping[str, Any], primary_key: Sequence[str]) -> bool:
    if len(primary_key) != 1:
        return False
    column_types = {
        str(column.get("name")): str(column.get("data_type", "")).lower()
        for column in table_item.get("columns", [])
        if isinstance(column, Mapping)
    }
    return column_types.get(primary_key[0]) in _INTEGER_TYPES


def _table_sizes(connection) -> dict[str, int]:
    placeholders = ",".join("%s" for _ in SCHEMAS)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_schema, table_name, COALESCE(data_length,0), "
            "COALESCE(index_length,0) FROM information_schema.tables "
            f"WHERE table_schema IN ({placeholders}) AND table_type='BASE TABLE'",
            SCHEMAS,
        )
        rows = cursor.fetchall()
    return {
        f"{schema}.{table}": int(data_length) + int(index_length)
        for schema, table, data_length, index_length in rows
    }


def _catalog_signature(catalog: Mapping[str, Any]) -> dict[str, Any]:
    tables = catalog.get("tables")
    if not isinstance(tables, Mapping):
        raise ConfigBuildError("catalogue tables are missing")
    return {
        ref: {
            "table_type": item.get("table_type"),
            "engine": item.get("engine"),
            "columns": [
                {
                    "name": column.get("name"),
                    "ordinal": column.get("ordinal"),
                    "data_type": column.get("data_type"),
                    "nullable": column.get("nullable"),
                }
                for column in item.get("columns", [])
            ],
            "primary_key": list(item.get("primary_key", [])),
        }
        for ref, item in sorted(tables.items())
    }


def _catalog_signature_sha256(catalog: Mapping[str, Any]) -> str:
    payload = {
        "schemas": list(catalog.get("schemas", [])),
        "tables": _catalog_signature(catalog),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _catalog_relationship(
    source_catalog: Mapping[str, Any],
    target_catalog: Mapping[str, Any],
    *,
    allowed_target_only: frozenset[str] = KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES,
    allowed_extended_columns: Mapping[str, Sequence[str]] = (
        KNOWN_POST_MIGRATION_EXTENDED_COLUMNS
    ),
) -> dict[str, Any]:
    source_signature = _catalog_signature(source_catalog)
    target_signature = _catalog_signature(target_catalog)
    if list(source_catalog.get("schemas", [])) != list(target_catalog.get("schemas", [])):
        raise ConfigBuildError("source and target schema inventories differ")
    source_only = set(source_signature) - set(target_signature)
    if source_only:
        raise ConfigBuildError(f"target is missing source tables: {sorted(source_only)}")

    target_only = frozenset(set(target_signature) - set(source_signature))
    extended_columns: dict[str, tuple[str, ...]] = {}
    for table_ref, source_item in source_signature.items():
        target_item = target_signature[table_ref]
        for field in ("table_type", "engine", "primary_key"):
            if source_item[field] != target_item[field]:
                raise ConfigBuildError(
                    f"target changed source {field} for {table_ref}"
                )
        source_columns = {str(item["name"]): item for item in source_item["columns"]}
        target_columns = {str(item["name"]): item for item in target_item["columns"]}
        missing_columns = sorted(set(source_columns) - set(target_columns))
        if missing_columns:
            raise ConfigBuildError(
                f"target is missing source columns for {table_ref}: {missing_columns}"
            )
        for column_name, source_column in source_columns.items():
            target_column = target_columns[column_name]
            for field in ("data_type", "nullable"):
                if source_column[field] != target_column[field]:
                    raise ConfigBuildError(
                        f"target changed source column {field} for "
                        f"{table_ref}.{column_name}"
                    )
        extras = tuple(
            str(item["name"])
            for item in target_item["columns"]
            if str(item["name"]) not in source_columns
        )
        if extras:
            extended_columns[table_ref] = extras

    if not target_only and not extended_columns:
        mode = "exact"
    else:
        if target_only != allowed_target_only:
            raise ConfigBuildError(
                "target-only tables are not the exact reviewed V2/V3/V4 set"
            )
        expected_columns = {
            table: tuple(columns)
            for table, columns in allowed_extended_columns.items()
        }
        if extended_columns != expected_columns:
            raise ConfigBuildError(
                "target-added columns are not the exact reviewed V2/V3/V4 set"
            )
        mode = "reviewed_v2_v3_v4_source_projection"

    return {
        "mode": mode,
        "source_catalog_sha256": _catalog_signature_sha256(source_catalog),
        "target_catalog_sha256": _catalog_signature_sha256(target_catalog),
        "source_table_count": len(source_signature),
        "target_table_count": len(target_signature),
        "target_only_tables": sorted(target_only),
        "target_extended_columns": {
            table: list(columns) for table, columns in sorted(extended_columns.items())
        },
    }


def build_policy(
    *,
    source_identity: Mapping[str, Any],
    target_identity: Mapping[str, Any],
    source_catalog: Mapping[str, Any],
    target_catalog: Mapping[str, Any],
    table_sizes: Mapping[str, int],
    max_workers: int,
) -> dict[str, Any]:
    if source_identity.get("version") != SOURCE_VERSION or source_identity.get("port") != 3306:
        raise ConfigBuildError("source is not the binlog-enabled MySQL 5.5.20 production endpoint")
    if source_identity.get("server_uuid") is not None:
        raise ConfigBuildError("legacy source unexpectedly exposes a server UUID")
    if source_identity.get("ssl_cipher"):
        raise ConfigBuildError("legacy source identity unexpectedly claims TLS")
    target_uuid = str(target_identity.get("server_uuid") or "").lower()
    target_port = int(target_identity.get("port") or 0)
    if (
        target_identity.get("version") != TARGET_VERSION
        or _UUID_RE.fullmatch(target_uuid) is None
        or target_port == 3306
        or not target_identity.get("ssl_cipher")
    ):
        raise ConfigBuildError("target is not an isolated TLS Oracle MySQL 8.4.11 endpoint")
    source_signature = _catalog_signature(source_catalog)
    catalog_comparison = _catalog_relationship(source_catalog, target_catalog)
    if set(table_sizes) != set(source_signature):
        raise ConfigBuildError("source table-size inventory differs from its catalogue")
    if not 1 <= max_workers <= 8:
        raise ConfigBuildError("max workers must be in 1..8")

    hashes: dict[str, Any] = {}
    date_columns: dict[str, list[str]] = {}
    full_hash_count = 0
    window_hash_count = 0
    crc_sample_hash_count = 0
    no_pk_tables: list[str] = []
    for table_ref, item in source_signature.items():
        primary_key = list(item["primary_key"])
        if primary_key:
            mode = classify_hash_mode(
                table_ref, allocated_bytes=int(table_sizes[table_ref])
            )
            if mode == "deterministic_pk_windows_sha256" and not _supports_pk_windows(
                item, primary_key
            ):
                mode = "deterministic_sample_sha256"
            spec: dict[str, Any] = {
                "mode": mode,
                "key_columns": primary_key,
                "columns": "*",
                "chunk_rows": 10_000,
            }
            if mode == "deterministic_pk_windows_sha256":
                spec["window_count"] = 64
                spec["window_rows"] = 256
                window_hash_count += 1
            elif mode == "deterministic_sample_sha256":
                # One remainder means one server-side scan.  The modulus keeps
                # the streamed result bounded even for the largest minute table.
                spec["sample_modulus"] = CRC_SAMPLE_MODULUS
                spec["sample_remainders"] = [0]
                crc_sample_hash_count += 1
            else:
                full_hash_count += 1
            hashes[table_ref] = spec
        else:
            no_pk_tables.append(table_ref)
        selected_dates = [
            str(column["name"])
            for column in item["columns"]
            if str(column["data_type"]).lower() in _DATE_TYPES
        ]
        if selected_dates:
            date_columns[table_ref] = selected_dates

    policy = {
        "format_version": 1,
        "schemas": list(SCHEMAS),
        "endpoints": {
            "source": {
                "version": SOURCE_VERSION,
                "port": 3306,
                "server_uuid": None,
                "legacy_identity_sha256": source_identity["legacy_identity_sha256"],
                "require_tls": False,
            },
            "target": {
                "version": TARGET_VERSION,
                "port": target_port,
                "server_uuid": target_uuid,
                "legacy_identity_sha256": None,
                "require_tls": True,
            },
        },
        "execution": {"max_workers": max_workers},
        "counts": {"mode": "all", "tables": []},
        "boundaries": {
            "primary_key_mode": "all",
            "primary_key_tables": [],
            "date_columns": date_columns,
        },
        "aggregates": {},
        "hashes": hashes,
        "legacy_zero_date_columns": list(KNOWN_MYSQL84_ZERO_DATE_COLUMNS),
        "catalog_comparison": catalog_comparison,
        "build_evidence": {
            "tool": "build_mysql84_final_data_manifest_config",
            "built_at_utc": _utc_now(),
            "base_table_count": len(source_signature),
            "full_hash_table_count": full_hash_count,
            "pk_window_hash_table_count": window_hash_count,
            "crc_sample_hash_table_count": crc_sample_hash_count,
            "no_primary_key_tables": no_pk_tables,
            "date_boundary_table_count": len(date_columns),
            "credentials_in_config": False,
            "catalog_comparison_mode": catalog_comparison["mode"],
        },
    }
    return policy


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    if not path.is_absolute() or path.exists():
        raise ConfigBuildError("output path must be absolute and new")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--target-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    expected_uuid = str(args.expected_target_uuid).strip().lower()
    if _UUID_RE.fullmatch(expected_uuid) is None:
        raise ConfigBuildError("expected target UUID is invalid")
    if args.expected_target_port == 3306 or not 1 <= args.expected_target_port <= 65535:
        raise ConfigBuildError("expected target port must be isolated and valid")
    expected_datadir = args.expected_target_datadir.expanduser().resolve(strict=True)
    source = _connect(
        args.source_option_file,
        ssl_ca=None,
        require_tls=False,
        read_timeout=300,
    )
    target = _connect(
        args.target_option_file,
        ssl_ca=args.target_ssl_ca,
        require_tls=True,
        read_timeout=300,
    )
    try:
        source_identity = inspect_identity(source)
        target_identity = inspect_identity(target)
        with target.cursor() as cursor:
            cursor.execute("SELECT @@datadir")
            datadir_row = cursor.fetchone()
        target_datadir = Path(str(datadir_row[0]).replace("/", os.sep)).resolve()
        if target_identity.get("server_uuid") != expected_uuid:
            raise ConfigBuildError("target UUID differs from the explicit expectation")
        if target_identity.get("port") != args.expected_target_port:
            raise ConfigBuildError("target port differs from the explicit expectation")
        if os.path.normcase(str(target_datadir)) != os.path.normcase(str(expected_datadir)):
            raise ConfigBuildError("target datadir differs from the explicit expectation")
        source_catalog = load_catalog(source, SCHEMAS)
        target_catalog = load_catalog(target, SCHEMAS)
        sizes = _table_sizes(source)
    finally:
        source.close()
        target.close()
    policy = build_policy(
        source_identity=source_identity,
        target_identity=target_identity,
        source_catalog=source_catalog,
        target_catalog=target_catalog,
        table_sizes=sizes,
        max_workers=args.max_workers,
    )
    _atomic_json(args.output.expanduser().resolve(strict=False), policy)
    return {
        "status": "success",
        "output": str(args.output.expanduser().resolve()),
        "output_sha256": hashlib.sha256(
            args.output.expanduser().resolve().read_bytes()
        ).hexdigest(),
        "build_evidence": policy["build_evidence"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run(args)
    except (ConfigBuildError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
