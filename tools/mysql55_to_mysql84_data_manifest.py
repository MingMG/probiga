#!/usr/bin/env python3
"""Build and compare bounded-memory data manifests for the MySQL 5.5 -> 8.4 move.

The tool is deliberately separate from the schema semantic audit.  It records
the exact server identity and schema catalogue before it reads business data,
then evaluates only checks explicitly selected in a JSON configuration:

* exact ``COUNT(*)`` checks (all tables, a selected set, or disabled);
* primary-key/date boundaries and configured numeric aggregates;
* ordered, client-side SHA-256 over every logical row of selected tables; or
* deterministic CRC32-selected samples which are then hashed with SHA-256.

All row reads are streamed.  Sampling is useful evidence, but is explicitly
not treated as proof that unobserved rows match.  Even a full logical hash is
not a physical-backup checksum.  Credentials are accepted only through a
MySQL ``[client]`` option file and are never written to a report.

Operationally important consistency rule: a live ``consistent_snapshot``
source capture is single-connection.  Limited parallelism is permitted only
after the operator records that source writes are frozen, or for a quiescent
restored target.
"""

from __future__ import annotations

import argparse
import configparser
import datetime as dt
import decimal
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pymysql
from pymysql.cursors import SSCursor


FORMAT_NAME = "probiga.mysql55_to_mysql84.data_manifest"
FORMAT_VERSION = 1
REPORT_NAME = "probiga.mysql55_to_mysql84.data_comparison"
CHECKPOINT_NAME = "probiga.mysql55_to_mysql84.data_manifest_checkpoint"
DOCUMENT_DIGEST_FIELD = "document_sha256"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SUPPORTED_AGGREGATES = frozenset({"min", "max", "sum", "avg", "count_nonnull"})
_SUPPORTED_DATE_TYPES = frozenset({"date", "datetime", "timestamp", "year"})

# These defaults were found on the 5.5 source during the upgrade preflight.
# The stored-row audit found zero legacy zero values, but the old defaults are
# still a separate 8.4 safety concern and must not disappear inside a generic
# schema/data "match" result.
KNOWN_MYSQL84_ZERO_DATE_COLUMNS = (
    "probiga.jq_strategy_meta.created_at",
    "probiga.jq_strategy_meta.updated_at",
    "probiga.jq_strategy_picks.created_at",
    "probiga.st_daily_review.etl_sync_at",
    "probiga.st_portfolio_analysis_log.created_at",
    "probiga.st_portfolio_trans_log.created_at",
    "probiga.st_recommended_stocks.created_at",
    "probiga.st_user_portfolio.etl_sync_at",
)

REVIEWED_POST_MIGRATION_MISMATCHES = frozenset(
    {
        ("exact_count", "probiga.schema_migration_v2"),
        ("boundary", "probiga.schema_migration_v2"),
        ("full_logical_sha256", "probiga.schema_migration_v2"),
        ("exact_count", "probiga.schema_migration_v3"),
        ("boundary", "probiga.schema_migration_v3"),
        ("full_logical_sha256", "probiga.schema_migration_v3"),
        ("full_logical_sha256", "probiga.st_model_registry_v3"),
        ("full_logical_sha256", "probiga.st_scheduled_tasks"),
        ("boundary", "probiga.st_strategy_version_v2"),
        ("full_logical_sha256", "probiga.st_strategy_version_v2"),
    }
)

REVIEWED_V2_SOURCE_VERSIONS = frozenset(
    {
        "20260725_001_trading_v2_core",
        "20260725_002_trading_v2_jobs_and_lifecycle",
        "20260725_003_trading_v2_execution_research_ops",
        "20260725_004_trading_v2_etf_truth_and_forward",
        "20260725_005_trading_v2_theme_risk_chain",
        "20260726_006_real_trading_hard_guard",
        "20260726_007_market_regime_transition_state",
        "20260727_008_intraday_dynamic_activation",
        "20260730_009_public_quote_failover",
        "20260730_010_qmt_end_to_end_health",
    }
)

REVIEWED_V2_TARGET_ONLY_VERSIONS = frozenset(
    {
        "20260803_011_v2_execution_evidence_bindings",
        "20260803_012_v2_execution_evidence_guards",
        "20260803_013_v2_execution_evidence_natural_keys",
        "20260803_014_v2_execution_authority_attestations",
        "20260803_015_v2_accounting_outcome_evidence",
    }
)

REVIEWED_V3_SOURCE_VERSIONS = frozenset(
    {
        "20260728_001_trading_v3_core",
        "20260729_002_restore_real_trading_hard_guard",
        "20260730_003_add_forecast_feature_snapshot",
        "20260730_004_trade_hypothesis_ledger",
    }
)

REVIEWED_V3_TARGET_ONLY_VERSIONS = frozenset(
    {
        "20260730_005_decision_provenance",
        "20260730_006_target_theme_exposure",
        "20260730_007_retire_legacy_models",
        "20260730_008_disable_legacy_entry_routes",
        "20260730_009_suspend_legacy_entry_strategies",
        "20260730_010_cancel_legacy_entry_orders",
        "20260801_001_block_real_execution_plans",
        "20260801_002_repair_counterfactual_attribution",
        "20260801_003_unify_forward_execution_evidence",
        "20260801_004_tag_opportunity_recall_evidence",
        "20260801_005_freeze_sample_ownership",
        "20260801_006_counterfactual_backlog_queue",
        "20260802_001_shadow_portfolio_evidence_isolation",
        "20260802_002_generic_theme_signal_ledger",
        "20260802_003_news_point_in_time_knowledge",
        "20260803_001_v3_execution_projection_subscriber",
        "20260804_000_shadow_intelligence_runtime",
        "20260804_001_v3_execution_projection_outbox",
    }
)


class ManifestError(RuntimeError):
    """Fail-closed configuration, identity, catalogue, or comparison error."""


@dataclass(frozen=True)
class EndpointExpectation:
    version: str
    port: int
    server_uuid: str | None
    legacy_identity_sha256: str | None
    require_tls: bool


@dataclass(frozen=True)
class CountPolicy:
    mode: str
    tables: tuple[str, ...]


@dataclass(frozen=True)
class BoundaryPolicy:
    primary_key_mode: str
    primary_key_tables: tuple[str, ...]
    date_columns: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class AggregateSpec:
    column: str
    functions: tuple[str, ...]
    absolute_tolerance: str


@dataclass(frozen=True)
class HashSpec:
    mode: str
    key_columns: tuple[str, ...]
    columns: tuple[str, ...] | None
    chunk_rows: int
    sample_modulus: int | None
    sample_remainders: tuple[int, ...]
    window_count: int | None
    window_rows: int | None


@dataclass(frozen=True)
class AuditConfig:
    schemas: tuple[str, ...]
    source: EndpointExpectation
    target: EndpointExpectation
    max_workers: int
    counts: CountPolicy
    boundaries: BoundaryPolicy
    aggregates: dict[str, tuple[AggregateSpec, ...]]
    hashes: dict[str, HashSpec]
    legacy_zero_date_columns: tuple[str, ...]
    raw: dict[str, Any]
    sha256: str


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def seal_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON document with a self-verifying canonical SHA-256."""

    document = dict(payload)
    document.pop(DOCUMENT_DIGEST_FIELD, None)
    document[DOCUMENT_DIGEST_FIELD] = _sha256_json(document)
    return document


def verify_document(document: Mapping[str, Any]) -> None:
    claimed = str(document.get(DOCUMENT_DIGEST_FIELD, "")).lower()
    if not _SHA256_RE.fullmatch(claimed):
        raise ManifestError("document is missing a valid document_sha256")
    unsigned = dict(document)
    unsigned.pop(DOCUMENT_DIGEST_FIELD, None)
    actual = _sha256_json(unsigned)
    if actual != claimed:
        raise ManifestError("document_sha256 mismatch; the JSON was changed or truncated")


def atomic_write_json(path: Path, document: Mapping[str, Any], *, overwrite: bool = False) -> None:
    """Write JSON via fsync + same-directory atomic replace, with mode 0600."""

    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not overwrite:
        raise ManifestError(f"refusing to overwrite existing output: {destination}")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    temporary = Path(temporary_name)
    try:
        try:
            os.fchmod(fd, 0o600)
        except (AttributeError, OSError):
            pass
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(document, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if destination.exists() and not overwrite:
            raise ManifestError(f"refusing to overwrite existing output: {destination}")
        os.replace(temporary, destination)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def load_sealed_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    with resolved.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ManifestError(f"JSON root must be an object: {resolved}")
    verify_document(value)
    return value


def _require_object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ManifestError(f"{name} must be a JSON object")
    return dict(value)


def _require_list(value: object, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ManifestError(f"{name} must be a JSON array")
    return list(value)


def _identifier(value: object, name: str) -> str:
    text = str(value)
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ManifestError(f"{name} is not a safe MySQL identifier: {text!r}")
    return text


def _table_ref(value: object, schemas: Sequence[str], name: str) -> str:
    text = str(value)
    pieces = text.split(".")
    if len(pieces) != 2:
        raise ManifestError(f"{name} must use schema.table form: {text!r}")
    schema = _identifier(pieces[0], name)
    table = _identifier(pieces[1], name)
    if schema not in schemas:
        raise ManifestError(f"{name} references an unconfigured schema: {schema}")
    return f"{schema}.{table}"


def _column_ref(value: object, schemas: Sequence[str], name: str) -> str:
    text = str(value)
    pieces = text.split(".")
    if len(pieces) != 3:
        raise ManifestError(f"{name} must use schema.table.column form: {text!r}")
    table = _table_ref(".".join(pieces[:2]), schemas, name)
    column = _identifier(pieces[2], name)
    return f"{table}.{column}"


def _unique_strings(values: Sequence[object], name: str) -> tuple[str, ...]:
    result = tuple(str(value) for value in values)
    if len(set(result)) != len(result):
        raise ManifestError(f"{name} contains duplicates")
    return result


def _parse_endpoint(value: object, name: str, *, target: bool) -> EndpointExpectation:
    item = _require_object(value, name)
    version = str(item.get("version", "")).strip()
    if not version:
        raise ManifestError(f"{name}.version must be exact and non-empty")
    try:
        port = int(item.get("port"))
    except (TypeError, ValueError) as exc:
        raise ManifestError(f"{name}.port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ManifestError(f"{name}.port is outside 1..65535")

    raw_uuid = item.get("server_uuid")
    server_uuid = None if raw_uuid is None else str(raw_uuid).strip().lower()
    if server_uuid is not None and not _UUID_RE.fullmatch(server_uuid):
        raise ManifestError(f"{name}.server_uuid must be an exact UUID or null")
    raw_legacy = item.get("legacy_identity_sha256")
    legacy = None if raw_legacy is None else str(raw_legacy).strip().lower()
    if legacy is not None and not _SHA256_RE.fullmatch(legacy):
        raise ManifestError(f"{name}.legacy_identity_sha256 must be an exact SHA-256 or null")
    require_tls = item.get("require_tls")
    if not isinstance(require_tls, bool):
        raise ManifestError(f"{name}.require_tls must be true or false")

    if server_uuid is None and legacy is None:
        raise ManifestError(
            f"{name} must pin server_uuid, or pin legacy_identity_sha256 for a server without UUID"
        )
    if target and (server_uuid is None or not require_tls):
        raise ManifestError("endpoints.target must pin server_uuid and require TLS")
    return EndpointExpectation(version, port, server_uuid, legacy, require_tls)


def load_config(path: Path) -> AuditConfig:
    resolved = path.expanduser().resolve(strict=True)
    with resolved.open("r", encoding="utf-8") as stream:
        raw_value = json.load(stream)
    raw = _require_object(raw_value, "config")
    if raw.get("format_version") != FORMAT_VERSION:
        raise ManifestError(f"config.format_version must be {FORMAT_VERSION}")

    raw_schemas = _require_list(raw.get("schemas"), "schemas")
    schemas = tuple(_identifier(value, "schemas[]") for value in raw_schemas)
    if not schemas or len(set(schemas)) != len(schemas):
        raise ManifestError("schemas must be a non-empty list without duplicates")

    endpoints = _require_object(raw.get("endpoints"), "endpoints")
    source = _parse_endpoint(endpoints.get("source"), "endpoints.source", target=False)
    target = _parse_endpoint(endpoints.get("target"), "endpoints.target", target=True)
    if source.server_uuid is not None and source.server_uuid == target.server_uuid:
        raise ManifestError("source and target server_uuid must be different")

    execution = _require_object(raw.get("execution"), "execution")
    try:
        max_workers = int(execution.get("max_workers"))
    except (TypeError, ValueError) as exc:
        raise ManifestError("execution.max_workers must be an integer") from exc
    if not 1 <= max_workers <= 8:
        raise ManifestError("execution.max_workers must be in 1..8")

    counts_value = _require_object(raw.get("counts"), "counts")
    count_mode = str(counts_value.get("mode", "")).lower()
    if count_mode not in {"all", "selected", "none"}:
        raise ManifestError("counts.mode must be all, selected, or none")
    count_tables = tuple(
        _table_ref(value, schemas, "counts.tables[]")
        for value in _require_list(counts_value.get("tables", []), "counts.tables")
    )
    if len(set(count_tables)) != len(count_tables):
        raise ManifestError("counts.tables contains duplicates")
    if count_mode == "selected" and not count_tables:
        raise ManifestError("counts.tables cannot be empty when counts.mode=selected")
    if count_mode != "selected" and count_tables:
        raise ManifestError("counts.tables is only valid when counts.mode=selected")
    counts = CountPolicy(count_mode, count_tables)

    boundaries_value = _require_object(raw.get("boundaries"), "boundaries")
    pk_mode = str(boundaries_value.get("primary_key_mode", "")).lower()
    if pk_mode not in {"all", "selected", "none"}:
        raise ManifestError("boundaries.primary_key_mode must be all, selected, or none")
    pk_tables = tuple(
        _table_ref(value, schemas, "boundaries.primary_key_tables[]")
        for value in _require_list(
            boundaries_value.get("primary_key_tables", []), "boundaries.primary_key_tables"
        )
    )
    if len(set(pk_tables)) != len(pk_tables):
        raise ManifestError("boundaries.primary_key_tables contains duplicates")
    if pk_mode == "selected" and not pk_tables:
        raise ManifestError("primary_key_tables cannot be empty for selected mode")
    if pk_mode != "selected" and pk_tables:
        raise ManifestError("primary_key_tables is only valid for selected mode")
    date_columns_value = _require_object(boundaries_value.get("date_columns", {}), "date_columns")
    date_columns: dict[str, tuple[str, ...]] = {}
    for raw_table, raw_columns in date_columns_value.items():
        table = _table_ref(raw_table, schemas, "boundaries.date_columns key")
        columns = tuple(
            _identifier(value, f"boundaries.date_columns.{table}[]")
            for value in _require_list(raw_columns, f"boundaries.date_columns.{table}")
        )
        if not columns or len(set(columns)) != len(columns):
            raise ManifestError(f"date columns for {table} must be non-empty and unique")
        date_columns[table] = columns
    boundaries = BoundaryPolicy(pk_mode, pk_tables, date_columns)

    aggregates_value = _require_object(raw.get("aggregates", {}), "aggregates")
    aggregates: dict[str, tuple[AggregateSpec, ...]] = {}
    for raw_table, raw_specs in aggregates_value.items():
        table = _table_ref(raw_table, schemas, "aggregates key")
        specs: list[AggregateSpec] = []
        seen_columns: set[str] = set()
        for index, raw_spec in enumerate(_require_list(raw_specs, f"aggregates.{table}")):
            spec = _require_object(raw_spec, f"aggregates.{table}[{index}]")
            column = _identifier(spec.get("column"), f"aggregates.{table}[{index}].column")
            if column in seen_columns:
                raise ManifestError(f"duplicate aggregate column for {table}: {column}")
            seen_columns.add(column)
            functions = tuple(
                str(value).lower()
                for value in _require_list(spec.get("functions"), "aggregate functions")
            )
            if not functions or len(set(functions)) != len(functions):
                raise ManifestError(f"aggregate functions for {table}.{column} must be unique")
            unknown = set(functions) - _SUPPORTED_AGGREGATES
            if unknown:
                raise ManifestError(f"unsupported aggregate functions: {sorted(unknown)}")
            tolerance_text = str(spec.get("absolute_tolerance", "0"))
            try:
                tolerance = decimal.Decimal(tolerance_text)
            except decimal.InvalidOperation as exc:
                raise ManifestError("aggregate absolute_tolerance must be a decimal") from exc
            if not tolerance.is_finite() or tolerance < 0:
                raise ManifestError("aggregate absolute_tolerance must be finite and non-negative")
            specs.append(AggregateSpec(column, functions, format(tolerance, "f")))
        if not specs:
            raise ManifestError(f"aggregates.{table} cannot be empty")
        aggregates[table] = tuple(specs)

    hashes_value = _require_object(raw.get("hashes", {}), "hashes")
    hashes: dict[str, HashSpec] = {}
    for raw_table, raw_spec in hashes_value.items():
        table = _table_ref(raw_table, schemas, "hashes key")
        spec = _require_object(raw_spec, f"hashes.{table}")
        mode = str(spec.get("mode", "")).lower()
        if mode not in {
            "full_ordered_sha256",
            "deterministic_sample_sha256",
            "deterministic_pk_windows_sha256",
        }:
            raise ManifestError(
                f"hashes.{table}.mode must be full_ordered_sha256, "
                "deterministic_sample_sha256, or deterministic_pk_windows_sha256"
            )
        key_columns = tuple(
            _identifier(value, f"hashes.{table}.key_columns[]")
            for value in _require_list(spec.get("key_columns"), "key_columns")
        )
        if not key_columns or len(set(key_columns)) != len(key_columns):
            raise ManifestError(f"hash key_columns for {table} must be non-empty and unique")
        raw_columns = spec.get("columns")
        if raw_columns == "*":
            columns = None
        else:
            columns = tuple(
                _identifier(value, f"hashes.{table}.columns[]")
                for value in _require_list(raw_columns, f"hashes.{table}.columns")
            )
            if not columns or len(set(columns)) != len(columns):
                raise ManifestError(f"hash columns for {table} must be non-empty and unique")
        try:
            chunk_rows = int(spec.get("chunk_rows"))
        except (TypeError, ValueError) as exc:
            raise ManifestError(f"hashes.{table}.chunk_rows must be an integer") from exc
        if not 1_000 <= chunk_rows <= 1_000_000:
            raise ManifestError(f"hashes.{table}.chunk_rows must be in 1000..1000000")

        modulus: int | None = None
        remainders: tuple[int, ...] = ()
        window_count: int | None = None
        window_rows: int | None = None
        if mode == "deterministic_sample_sha256":
            try:
                modulus = int(spec.get("sample_modulus"))
            except (TypeError, ValueError) as exc:
                raise ManifestError(f"hashes.{table}.sample_modulus must be an integer") from exc
            if not 2 <= modulus <= 1_000_000:
                raise ManifestError(f"hashes.{table}.sample_modulus must be in 2..1000000")
            remainders = tuple(
                int(value)
                for value in _require_list(
                    spec.get("sample_remainders"), f"hashes.{table}.sample_remainders"
                )
            )
            if not remainders or len(set(remainders)) != len(remainders):
                raise ManifestError(f"sample remainders for {table} must be non-empty and unique")
            if any(value < 0 or value >= modulus for value in remainders):
                raise ManifestError(f"sample remainders for {table} must be in 0..modulus-1")
            remainders = tuple(sorted(remainders))
            if "window_count" in spec or "window_rows" in spec:
                raise ManifestError(f"window fields are invalid for CRC sample on {table}")
        elif mode == "deterministic_pk_windows_sha256":
            try:
                window_count = int(spec.get("window_count"))
                window_rows = int(spec.get("window_rows"))
            except (TypeError, ValueError) as exc:
                raise ManifestError(f"hashes.{table} window fields must be integers") from exc
            if not 2 <= window_count <= 10_000:
                raise ManifestError(f"hashes.{table}.window_count must be in 2..10000")
            if not 1 <= window_rows <= 100_000:
                raise ManifestError(f"hashes.{table}.window_rows must be in 1..100000")
            if "sample_modulus" in spec or "sample_remainders" in spec:
                raise ManifestError(f"CRC sample fields are invalid for PK windows on {table}")
        elif "sample_modulus" in spec or "sample_remainders" in spec:
            raise ManifestError(f"sample fields are invalid for full hash on {table}")
        elif "window_count" in spec or "window_rows" in spec:
            raise ManifestError(f"window fields are invalid for full hash on {table}")
        hashes[table] = HashSpec(
            mode,
            key_columns,
            columns,
            chunk_rows,
            modulus,
            remainders,
            window_count,
            window_rows,
        )

    if "legacy_zero_date_columns" not in raw:
        raise ManifestError(
            "legacy_zero_date_columns must be explicit; use the known ProBigA list or [] only for isolation tests"
        )
    legacy_columns = tuple(
        _column_ref(value, schemas, "legacy_zero_date_columns[]")
        for value in _require_list(raw.get("legacy_zero_date_columns"), "legacy_zero_date_columns")
    )
    if len(set(legacy_columns)) != len(legacy_columns):
        raise ManifestError("legacy_zero_date_columns contains duplicates")

    return AuditConfig(
        schemas=schemas,
        source=source,
        target=target,
        max_workers=max_workers,
        counts=counts,
        boundaries=boundaries,
        aggregates=aggregates,
        hashes=hashes,
        legacy_zero_date_columns=legacy_columns,
        raw=raw,
        sha256=_sha256_json(raw),
    )


def _strip_option_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def read_client_options(path: Path) -> dict[str, Any]:
    """Read credentials solely from a MySQL option file.

    The returned mapping is used only to establish a connection.  It must not
    be logged, serialized, or attached to raised errors by callers.  On POSIX,
    group/other-readable files are rejected.  Windows deployments must protect
    the file DACL (the upgrade runbook creates Administrator + SYSTEM ACLs).
    """

    resolved = path.expanduser().resolve(strict=True)
    if os.name != "nt" and resolved.stat().st_mode & 0o077:
        raise ManifestError("client option file must not be accessible by group/other users")
    parser = configparser.RawConfigParser(interpolation=None, strict=False)
    with resolved.open("r", encoding="utf-8-sig") as stream:
        parser.read_file(stream)
    if not parser.has_section("client"):
        raise ManifestError("client option file is missing [client]")

    def option(name: str, default: str = "") -> str:
        return _strip_option_value(parser.get("client", name, fallback=default))

    host = option("host", "127.0.0.1")
    user = option("user")
    password = option("password")
    try:
        port = int(option("port", "3306"))
    except ValueError as exc:
        raise ManifestError("client option file contains an invalid port") from exc
    if not host or not user or not password or not 1 <= port <= 65535:
        raise ManifestError("client option file has incomplete connection fields")
    return {"host": host, "port": port, "user": user, "password": password}


def _connect(
    option_file: Path,
    *,
    ssl_ca: Path | None,
    require_tls: bool,
    read_timeout: int,
    streaming: bool = False,
) -> pymysql.Connection:
    if require_tls and ssl_ca is None:
        raise ManifestError("a CA file is mandatory for the TLS-required target")
    options = read_client_options(option_file)
    kwargs: dict[str, Any] = {
        **options,
        "charset": "utf8mb4",
        "autocommit": False,
        "connect_timeout": 15,
        "read_timeout": read_timeout,
        "write_timeout": 60,
    }
    if streaming:
        kwargs["cursorclass"] = SSCursor
    if ssl_ca is not None:
        kwargs["ssl"] = {
            "ca": str(ssl_ca.expanduser().resolve(strict=True)),
            "check_hostname": False,
        }
    try:
        return pymysql.connect(**kwargs)
    finally:
        # Do not let connection dictionaries survive longer than necessary.
        options.clear()
        kwargs.pop("password", None)


def _legacy_identity_sha256(
    *, hostname: object, port: object, server_id: object, datadir: object
) -> str:
    return _sha256_json(
        {
            "hostname": str(hostname),
            "port": int(port),
            "server_id": int(server_id),
            "datadir": str(datadir),
        }
    )


def inspect_identity(connection: pymysql.Connection) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT @@version, @@version_comment, @@port, @@hostname, @@server_id, @@datadir"
        )
        row = cursor.fetchone()
        if row is None:
            raise ManifestError("server identity query returned no row")
        version, version_comment, port, hostname, server_id, datadir = row
        try:
            cursor.execute("SELECT @@server_uuid")
            uuid_row = cursor.fetchone()
            server_uuid = str(uuid_row[0]).strip().lower() if uuid_row else None
        except pymysql.MySQLError as exc:
            if exc.args and int(exc.args[0]) == 1193:
                server_uuid = None
            else:
                raise
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        ssl_row = cursor.fetchone()
    return {
        "version": str(version),
        "version_comment": str(version_comment),
        "port": int(port),
        "server_uuid": server_uuid,
        "legacy_identity_sha256": _legacy_identity_sha256(
            hostname=hostname,
            port=port,
            server_id=server_id,
            datadir=datadir,
        ),
        "server_id": int(server_id),
        "hostname_sha256": hashlib.sha256(str(hostname).encode("utf-8")).hexdigest(),
        "ssl_cipher": str(ssl_row[1] if ssl_row else ""),
    }


def validate_identity(
    identity: Mapping[str, Any], expectation: EndpointExpectation, *, role: str
) -> None:
    if identity.get("version") != expectation.version:
        raise ManifestError(
            f"{role} version mismatch: expected {expectation.version!r}, got {identity.get('version')!r}"
        )
    if identity.get("port") != expectation.port:
        raise ManifestError(
            f"{role} port mismatch: expected {expectation.port}, got {identity.get('port')!r}"
        )
    actual_uuid = identity.get("server_uuid")
    if expectation.server_uuid is not None:
        if actual_uuid != expectation.server_uuid:
            raise ManifestError(f"{role} server_uuid mismatch")
    else:
        if actual_uuid is not None:
            raise ManifestError(f"{role} unexpectedly exposes server_uuid; pin it in config")
        if identity.get("legacy_identity_sha256") != expectation.legacy_identity_sha256:
            raise ManifestError(f"{role} legacy identity fingerprint mismatch")
    if expectation.require_tls and not identity.get("ssl_cipher"):
        raise ManifestError(f"{role} connection is not using TLS")


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ManifestError(f"unsafe identifier: {value!r}")
    return f"`{value}`"


def _quote_table(table_ref: str) -> str:
    schema, table = table_ref.split(".")
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _sql_placeholders(count: int) -> str:
    if count <= 0:
        raise ManifestError("at least one schema is required")
    return ",".join(["%s"] * count)


def load_catalog(connection: pymysql.Connection, schemas: Sequence[str]) -> dict[str, Any]:
    """Read a compact logical catalogue used to fail closed before table scans."""

    placeholders = _sql_placeholders(len(schemas))
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT schema_name FROM information_schema.schemata "
            f"WHERE schema_name IN ({placeholders}) ORDER BY schema_name",
            tuple(schemas),
        )
        present_schemas = tuple(str(row[0]) for row in cursor.fetchall())
        missing = sorted(set(schemas) - set(present_schemas))
        if missing:
            raise ManifestError(f"configured schemas are missing: {missing}")

        cursor.execute(
            f"SELECT table_schema, table_name, table_type, COALESCE(engine, '<NULL>') "
            f"FROM information_schema.tables WHERE table_schema IN ({placeholders}) "
            f"ORDER BY table_schema, table_name",
            tuple(schemas),
        )
        table_rows = cursor.fetchall()

        cursor.execute(
            f"SELECT table_schema, table_name, column_name, ordinal_position, data_type, "
            f"is_nullable, column_default "
            f"FROM information_schema.columns WHERE table_schema IN ({placeholders}) "
            f"ORDER BY table_schema, table_name, ordinal_position",
            tuple(schemas),
        )
        column_rows = cursor.fetchall()

        cursor.execute(
            f"SELECT table_schema, table_name, column_name, ordinal_position "
            f"FROM information_schema.key_column_usage "
            f"WHERE constraint_name='PRIMARY' AND table_schema IN ({placeholders}) "
            f"ORDER BY table_schema, table_name, ordinal_position",
            tuple(schemas),
        )
        pk_rows = cursor.fetchall()

    tables: dict[str, dict[str, Any]] = {}
    for schema, table, table_type, engine in table_rows:
        ref = f"{schema}.{table}"
        tables[ref] = {
            "table_type": str(table_type),
            "engine": str(engine),
            "columns": [],
            "primary_key": [],
        }
    for schema, table, column, ordinal, data_type, nullable, default in column_rows:
        ref = f"{schema}.{table}"
        if ref not in tables:
            raise ManifestError(f"column metadata references unknown table: {ref}")
        tables[ref]["columns"].append(
            {
                "name": str(column),
                "ordinal": int(ordinal),
                "data_type": str(data_type).lower(),
                "nullable": str(nullable).upper() == "YES",
                # Defaults are needed for the zero-date risk section but are
                # deliberately excluded from the catalogue digest below.
                "default": _json_value(default),
            }
        )
    for schema, table, column, _ordinal in pk_rows:
        ref = f"{schema}.{table}"
        if ref not in tables:
            raise ManifestError(f"primary-key metadata references unknown table: {ref}")
        tables[ref]["primary_key"].append(str(column))

    digest_catalog = {
        "schemas": list(schemas),
        "tables": {
            ref: {
                "table_type": item["table_type"],
                "engine": item["engine"],
                "columns": [
                    {
                        "name": column["name"],
                        "ordinal": column["ordinal"],
                        "data_type": column["data_type"],
                        "nullable": column["nullable"],
                    }
                    for column in item["columns"]
                ],
                "primary_key": list(item["primary_key"]),
            }
            for ref, item in sorted(tables.items())
        },
    }
    return {
        "schemas": list(schemas),
        "tables": tables,
        "catalog_sha256": _sha256_json(digest_catalog),
    }


def _catalog_for_comparison(catalog: Mapping[str, Any]) -> dict[str, Any]:
    """Drop defaults, which are audited separately and may be repaired on 8.4."""

    tables: dict[str, Any] = {}
    for ref, raw_item in sorted(_require_object(catalog.get("tables"), "catalog.tables").items()):
        item = _require_object(raw_item, f"catalog.tables.{ref}")
        tables[ref] = {
            "table_type": item.get("table_type"),
            "engine": item.get("engine"),
            "columns": [
                {
                    "name": column.get("name"),
                    "ordinal": column.get("ordinal"),
                    "data_type": column.get("data_type"),
                    "nullable": column.get("nullable"),
                }
                for column in _require_list(item.get("columns"), f"catalog.tables.{ref}.columns")
            ],
            "primary_key": list(item.get("primary_key", [])),
        }
    return {"schemas": list(catalog.get("schemas", [])), "tables": tables}


def _assert_catalog_matches(source: Mapping[str, Any], target: Mapping[str, Any]) -> None:
    if _catalog_for_comparison(source) != _catalog_for_comparison(target):
        raise ManifestError("target logical table/column/primary-key catalogue differs from source")


def _catalog_signature_sha256(catalog: Mapping[str, Any]) -> str:
    return _sha256_json(_catalog_for_comparison(catalog))


def _catalog_comparison_policy(config: AuditConfig) -> dict[str, Any]:
    raw_value = config.raw.get("catalog_comparison")
    if raw_value is None:
        return {"mode": "exact"}
    policy = _require_object(raw_value, "catalog_comparison")
    mode = str(policy.get("mode", ""))
    if mode not in {"exact", "reviewed_v2_v3_v4_source_projection"}:
        raise ManifestError("catalog_comparison.mode is unsupported")
    for name in ("source_catalog_sha256", "target_catalog_sha256"):
        value = str(policy.get(name, "")).lower()
        if not _SHA256_RE.fullmatch(value):
            raise ManifestError(f"catalog_comparison.{name} must be a SHA-256")
    target_only = _require_list(
        policy.get("target_only_tables", []),
        "catalog_comparison.target_only_tables",
    )
    if target_only != sorted(target_only) or len(set(target_only)) != len(target_only):
        raise ManifestError("catalog_comparison.target_only_tables must be sorted and unique")
    for value in target_only:
        _table_ref(value, config.schemas, "catalog_comparison.target_only_tables[]")
    extended = _require_object(
        policy.get("target_extended_columns", {}),
        "catalog_comparison.target_extended_columns",
    )
    for table, columns in extended.items():
        _table_ref(table, config.schemas, "catalog_comparison.target_extended_columns key")
        names = _require_list(columns, f"catalog_comparison.target_extended_columns.{table}")
        if not names or len(set(names)) != len(names):
            raise ManifestError(f"catalog_comparison extended columns for {table} are invalid")
        for column in names:
            _identifier(column, f"catalog_comparison.target_extended_columns.{table}[]")
    if mode == "exact" and (target_only or extended):
        raise ManifestError("exact catalog comparison cannot declare target additions")
    return policy


def _project_target_catalog(
    source_catalog: Mapping[str, Any],
    target_catalog: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if list(source_catalog.get("schemas", [])) != list(target_catalog.get("schemas", [])):
        raise ManifestError("target schema inventory differs from source")
    source_tables = _require_object(source_catalog.get("tables"), "source catalog.tables")
    target_tables = _require_object(target_catalog.get("tables"), "target catalog.tables")
    source_only = sorted(set(source_tables) - set(target_tables))
    if source_only:
        raise ManifestError(f"target is missing source tables: {source_only}")

    projected_tables: dict[str, Any] = {}
    extended_columns: dict[str, list[str]] = {}
    for table_ref, raw_source_item in sorted(source_tables.items()):
        source_item = _require_object(raw_source_item, f"source catalog.tables.{table_ref}")
        target_item = _require_object(
            target_tables[table_ref], f"target catalog.tables.{table_ref}"
        )
        for field in ("table_type", "engine", "primary_key"):
            if source_item.get(field) != target_item.get(field):
                raise ManifestError(f"target changed source {field} for {table_ref}")
        source_columns = _require_list(
            source_item.get("columns"), f"source catalog.tables.{table_ref}.columns"
        )
        target_columns = _require_list(
            target_item.get("columns"), f"target catalog.tables.{table_ref}.columns"
        )
        target_by_name = {
            str(_require_object(item, "target column").get("name")): _require_object(
                item, "target column"
            )
            for item in target_columns
        }
        projected_columns: list[dict[str, Any]] = []
        source_names: set[str] = set()
        for raw_source_column in source_columns:
            source_column = _require_object(raw_source_column, "source column")
            name = str(source_column.get("name"))
            source_names.add(name)
            if name not in target_by_name:
                raise ManifestError(f"target is missing source column {table_ref}.{name}")
            target_column = target_by_name[name]
            for field in ("data_type", "nullable"):
                if source_column.get(field) != target_column.get(field):
                    raise ManifestError(
                        f"target changed source column {field} for {table_ref}.{name}"
                    )
            projected_columns.append(
                {
                    **source_column,
                    # Defaults are intentionally allowed to be repaired on 8.4.
                    "default": target_column.get("default"),
                }
            )
        extras = [
            str(_require_object(item, "target column").get("name"))
            for item in target_columns
            if str(_require_object(item, "target column").get("name")) not in source_names
        ]
        if extras:
            extended_columns[table_ref] = extras
        projected_tables[table_ref] = {
            "table_type": source_item.get("table_type"),
            "engine": source_item.get("engine"),
            "columns": projected_columns,
            "primary_key": list(source_item.get("primary_key", [])),
        }

    projected = {
        "schemas": list(source_catalog.get("schemas", [])),
        "tables": projected_tables,
    }
    projected["catalog_sha256"] = _catalog_signature_sha256(projected)
    attestation = {
        "mode": "reviewed_v2_v3_v4_source_projection",
        "source_catalog_sha256": _catalog_signature_sha256(source_catalog),
        "target_catalog_sha256": _catalog_signature_sha256(target_catalog),
        "source_table_count": len(source_tables),
        "target_table_count": len(target_tables),
        "target_only_tables": sorted(set(target_tables) - set(source_tables)),
        "target_extended_columns": extended_columns,
    }
    return projected, attestation


def _json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bytes):
        return {"type": "bytes", "hex": value.hex()}
    if isinstance(value, decimal.Decimal):
        return {"type": "decimal", "value": format(value, "f")}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            return {"type": "float", "value": value.hex()}
        return {"type": "float", "value": repr(value)}
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return {"type": value.__class__.__name__, "value": value.isoformat()}
    return {"type": "string", "value": str(value)}


def _column_map(table_catalog: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item["name"]): dict(item)
        for item in _require_list(table_catalog.get("columns"), "table columns")
    }


def _base_tables(catalog: Mapping[str, Any]) -> tuple[str, ...]:
    tables = _require_object(catalog.get("tables"), "catalog.tables")
    return tuple(
        ref
        for ref, raw_item in sorted(tables.items())
        if str(_require_object(raw_item, f"catalog.tables.{ref}").get("table_type")).upper()
        == "BASE TABLE"
    )


def validate_config_against_catalog(config: AuditConfig, catalog: Mapping[str, Any]) -> None:
    tables = _require_object(catalog.get("tables"), "catalog.tables")
    base_tables = set(_base_tables(catalog))

    referenced_tables: set[str] = set(config.counts.tables)
    referenced_tables.update(config.boundaries.primary_key_tables)
    referenced_tables.update(config.boundaries.date_columns)
    referenced_tables.update(config.aggregates)
    referenced_tables.update(config.hashes)
    referenced_tables.update(".".join(value.split(".")[:2]) for value in config.legacy_zero_date_columns)
    missing = sorted(referenced_tables - set(tables))
    if missing:
        raise ManifestError(f"configuration references missing tables: {missing}")
    non_base = sorted(referenced_tables - base_tables)
    if non_base:
        raise ManifestError(f"data checks may only reference BASE TABLE objects: {non_base}")

    for table, columns in config.boundaries.date_columns.items():
        column_map = _column_map(_require_object(tables[table], f"catalog.tables.{table}"))
        for column in columns:
            if column not in column_map:
                raise ManifestError(f"configured date boundary column is missing: {table}.{column}")
            if column_map[column]["data_type"] not in _SUPPORTED_DATE_TYPES:
                raise ManifestError(
                    f"configured date boundary is not a date-like column: {table}.{column}"
                )

    numeric_types = {
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "bit",
    }
    for table, specs in config.aggregates.items():
        column_map = _column_map(_require_object(tables[table], f"catalog.tables.{table}"))
        for spec in specs:
            if spec.column not in column_map:
                raise ManifestError(f"aggregate column is missing: {table}.{spec.column}")
            if column_map[spec.column]["data_type"] not in numeric_types:
                raise ManifestError(f"aggregate column is not numeric: {table}.{spec.column}")

    for table, spec in config.hashes.items():
        table_item = _require_object(tables[table], f"catalog.tables.{table}")
        column_map = _column_map(table_item)
        actual_pk = tuple(str(value) for value in table_item.get("primary_key", []))
        if spec.key_columns != actual_pk:
            raise ManifestError(
                f"hash key_columns for {table} must exactly match PRIMARY KEY order; "
                f"configured={spec.key_columns!r}, actual={actual_pk!r}"
            )
        resolved_columns = tuple(column_map) if spec.columns is None else spec.columns
        missing_columns = sorted(set(resolved_columns) - set(column_map))
        if missing_columns:
            raise ManifestError(f"hash columns are missing on {table}: {missing_columns}")
        missing_keys = sorted(set(spec.key_columns) - set(resolved_columns))
        if missing_keys:
            raise ManifestError(f"hash columns must include key columns on {table}: {missing_keys}")
        if spec.mode == "deterministic_pk_windows_sha256":
            if len(spec.key_columns) != 1:
                raise ManifestError(f"PK-window sampling requires a single-column primary key: {table}")
            key_type = column_map[spec.key_columns[0]]["data_type"]
            if key_type not in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}:
                raise ManifestError(f"PK-window sampling requires an integer primary key: {table}")
            if table not in _selected_pk_boundary_tables(config, catalog):
                raise ManifestError(
                    f"PK-window sampling requires primary-key boundaries for {table}"
                )

    for ref in config.legacy_zero_date_columns:
        schema, table, column = ref.split(".")
        table_ref = f"{schema}.{table}"
        column_map = _column_map(_require_object(tables[table_ref], f"catalog.tables.{table_ref}"))
        if column not in column_map:
            raise ManifestError(f"legacy zero-date column is missing: {ref}")
        if column_map[column]["data_type"] not in {"date", "datetime", "timestamp"}:
            raise ManifestError(f"legacy zero-date check is not date/datetime/timestamp: {ref}")


def _selected_count_tables(config: AuditConfig, catalog: Mapping[str, Any]) -> set[str]:
    if config.counts.mode == "all":
        return set(_base_tables(catalog))
    if config.counts.mode == "selected":
        return set(config.counts.tables)
    return set()


def _selected_pk_boundary_tables(config: AuditConfig, catalog: Mapping[str, Any]) -> set[str]:
    if config.boundaries.primary_key_mode == "all":
        return set(_base_tables(catalog))
    if config.boundaries.primary_key_mode == "selected":
        return set(config.boundaries.primary_key_tables)
    return set()


def build_table_plans(config: AuditConfig, catalog: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    validate_config_against_catalog(config, catalog)
    tables = _require_object(catalog.get("tables"), "catalog.tables")
    count_tables = _selected_count_tables(config, catalog)
    pk_boundary_tables = _selected_pk_boundary_tables(config, catalog)
    needed = set(count_tables) | pk_boundary_tables
    needed |= set(config.boundaries.date_columns)
    needed |= set(config.aggregates)
    needed |= set(config.hashes)
    needed |= {".".join(value.split(".")[:2]) for value in config.legacy_zero_date_columns}

    plans: dict[str, dict[str, Any]] = {}
    for table in sorted(needed):
        table_item = _require_object(tables[table], f"catalog.tables.{table}")
        boundary_columns: list[str] = []
        if table in pk_boundary_tables:
            boundary_columns.extend(str(value) for value in table_item.get("primary_key", []))
        for column in config.boundaries.date_columns.get(table, ()):
            if column not in boundary_columns:
                boundary_columns.append(column)
        zero_columns = tuple(
            value.rsplit(".", 1)[1]
            for value in config.legacy_zero_date_columns
            if value.startswith(f"{table}.")
        )
        plans[table] = {
            "count": table in count_tables,
            "boundary_columns": tuple(boundary_columns),
            "aggregates": config.aggregates.get(table, ()),
            "hash": config.hashes.get(table),
            "legacy_zero_columns": zero_columns,
        }
    return plans


def _cell_bytes(value: object) -> bytes:
    if value is None:
        payload = b""
        tag = b"N"
    elif isinstance(value, bytes):
        payload = value
        tag = b"B"
    elif isinstance(value, str):
        payload = value.encode("utf-8")
        tag = b"S"
    elif isinstance(value, bool):
        payload = b"1" if value else b"0"
        tag = b"Z"
    elif isinstance(value, int):
        payload = str(value).encode("ascii")
        tag = b"I"
    elif isinstance(value, decimal.Decimal):
        payload = format(value, "f").encode("ascii")
        tag = b"D"
    elif isinstance(value, float):
        payload = value.hex().encode("ascii")
        tag = b"F"
    elif isinstance(value, dt.datetime):
        payload = value.isoformat(sep=" ", timespec="microseconds").encode("ascii")
        tag = b"T"
    elif isinstance(value, dt.date):
        payload = value.isoformat().encode("ascii")
        tag = b"A"
    elif isinstance(value, dt.time):
        payload = value.isoformat(timespec="microseconds").encode("ascii")
        tag = b"M"
    else:
        payload = str(value).encode("utf-8")
        tag = b"O"
    return tag + str(len(payload)).encode("ascii") + b":" + payload


def canonical_row_bytes(row: Sequence[object]) -> bytes:
    encoded = bytearray()
    encoded.extend(str(len(row)).encode("ascii"))
    encoded.extend(b"|")
    for value in row:
        cell = _cell_bytes(value)
        encoded.extend(str(len(cell)).encode("ascii"))
        encoded.extend(b":")
        encoded.extend(cell)
    encoded.extend(b"\n")
    return bytes(encoded)


def hash_rows_in_chunks(
    rows: Iterable[Sequence[object]],
    *,
    key_width: int,
    chunk_rows: int,
    max_recorded_chunks: int = 4096,
) -> dict[str, Any]:
    """Pure bounded-memory ordered SHA-256 helper used by the streaming reader."""

    if max_recorded_chunks < 2:
        raise ManifestError("max_recorded_chunks must be at least 2")
    overall = hashlib.sha256()
    chunk_ledger = hashlib.sha256()
    chunk_digest = hashlib.sha256()
    first_limit = max_recorded_chunks // 2
    tail_limit = max_recorded_chunks - first_limit
    first_chunks: list[dict[str, Any]] = []
    tail_chunks: deque[dict[str, Any]] = deque(maxlen=tail_limit)
    total_chunks = 0
    row_count = 0
    chunk_count = 0
    first_key: list[object] | None = None
    last_key: list[object] | None = None

    def record_chunk(chunk: dict[str, Any]) -> None:
        nonlocal total_chunks
        chunk_ledger.update(_canonical_json_bytes(chunk))
        chunk_ledger.update(b"\n")
        if total_chunks < first_limit:
            first_chunks.append(chunk)
        else:
            tail_chunks.append(chunk)
        total_chunks += 1

    for row in rows:
        if len(row) < key_width:
            raise ManifestError("hash row has fewer values than key_width")
        payload = canonical_row_bytes(row)
        overall.update(payload)
        chunk_digest.update(payload)
        key = [_json_value(value) for value in row[:key_width]]
        if first_key is None:
            first_key = key
        last_key = key
        row_count += 1
        chunk_count += 1
        if chunk_count == chunk_rows:
            record_chunk(
                {
                    "ordinal": total_chunks,
                    "row_count": chunk_count,
                    "first_key": first_key,
                    "last_key": last_key,
                    "sha256": chunk_digest.hexdigest(),
                }
            )
            chunk_digest = hashlib.sha256()
            chunk_count = 0
            first_key = None
            last_key = None
    if chunk_count:
        record_chunk(
            {
                "ordinal": total_chunks,
                "row_count": chunk_count,
                "first_key": first_key,
                "last_key": last_key,
                "sha256": chunk_digest.hexdigest(),
            }
        )
    recorded_chunks = first_chunks + list(tail_chunks)
    return {
        "row_count": row_count,
        "overall_sha256": overall.hexdigest(),
        "chunk_count": total_chunks,
        "chunk_ledger_sha256": chunk_ledger.hexdigest(),
        "chunks_truncated": total_chunks > len(recorded_chunks),
        "recorded_chunk_strategy": "all" if total_chunks <= max_recorded_chunks else "first_and_last",
        "chunks": recorded_chunks,
    }


def _sample_crc_expression(key_columns: Sequence[str]) -> str:
    """Build an unambiguous deterministic selector; CRC32 is selection only."""

    pieces: list[str] = []
    for column in key_columns:
        quoted = _quote_identifier(column)
        hex_value = f"HEX(CAST({quoted} AS BINARY))"
        pieces.append(f"LPAD(CHAR_LENGTH({hex_value}),10,'0')")
        pieces.append("':'")
        pieces.append(hex_value)
        pieces.append("';'")
    return f"CRC32(CONCAT({','.join(pieces)}))"


def _resolved_hash_columns(spec: HashSpec, table_catalog: Mapping[str, Any]) -> tuple[str, ...]:
    if spec.columns is not None:
        return spec.columns
    return tuple(
        str(column["name"])
        for column in _require_list(table_catalog.get("columns"), "table columns")
    )


def _hash_query(
    table: str,
    spec: HashSpec,
    table_catalog: Mapping[str, Any],
    *,
    remainder: int | None,
) -> tuple[str, tuple[object, ...], tuple[str, ...]]:
    columns = _resolved_hash_columns(spec, table_catalog)
    # Key values are returned first for chunk boundaries, then the configured
    # logical row.  Repeating key values inside the logical row is intentional.
    select_columns = (*spec.key_columns, *columns)
    select_sql = ",".join(_quote_identifier(value) for value in select_columns)
    order_sql = ",".join(_quote_identifier(value) for value in spec.key_columns)
    where_sql = ""
    params: tuple[object, ...] = ()
    if spec.mode == "deterministic_sample_sha256":
        if remainder is None or spec.sample_modulus is None:
            raise ManifestError("sample hash query requires a remainder")
        selector = _sample_crc_expression(spec.key_columns)
        where_sql = f" WHERE MOD({selector}, %s) = %s"
        params = (spec.sample_modulus, remainder)
    query = f"SELECT {select_sql} FROM {_quote_table(table)}{where_sql} ORDER BY {order_sql}"
    return query, params, columns


def deterministic_integer_anchors(minimum: int, maximum: int, window_count: int) -> tuple[int, ...]:
    if minimum > maximum:
        raise ManifestError("primary-key minimum cannot exceed maximum")
    if window_count < 2:
        raise ManifestError("window_count must be at least 2")
    if minimum == maximum:
        return (minimum,)
    span = maximum - minimum
    return tuple(
        sorted(
            {
                minimum + (span * index) // (window_count - 1)
                for index in range(window_count)
            }
        )
    )


def _window_hash_query(
    table: str,
    spec: HashSpec,
    table_catalog: Mapping[str, Any],
) -> tuple[str, tuple[str, ...]]:
    if len(spec.key_columns) != 1 or spec.window_rows is None:
        raise ManifestError("invalid PK-window hash specification")
    columns = _resolved_hash_columns(spec, table_catalog)
    select_columns = (*spec.key_columns, *columns)
    select_sql = ",".join(_quote_identifier(value) for value in select_columns)
    key = _quote_identifier(spec.key_columns[0])
    # LIMIT is a validated integer literal.  The lower bound is parameterized
    # and uses the PRIMARY KEY for an index-range read rather than a full scan.
    query = (
        f"SELECT {select_sql} FROM {_quote_table(table)} "
        f"WHERE {key} >= %s ORDER BY {key} LIMIT {spec.window_rows}"
    )
    return query, columns


def _catalog_column(table_catalog: Mapping[str, Any], column_name: str) -> dict[str, Any]:
    columns = _column_map(table_catalog)
    try:
        return columns[column_name]
    except KeyError as exc:
        raise ManifestError(f"catalogue column disappeared: {column_name}") from exc


def _value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict) and "value" in value:
        return str(value["value"])
    return str(value)


def _as_integer_value(value: object, name: str) -> int:
    if not isinstance(value, dict) or value.get("type") != "int":
        raise ManifestError(f"{name} is not an integer boundary")
    try:
        return int(str(value.get("value")))
    except ValueError as exc:
        raise ManifestError(f"{name} is not an integer boundary") from exc


def _is_zero_date_default(value: object) -> bool:
    text = _value_text(value)
    return bool(text and text.startswith("0000-00-00"))  # mysql84-zero-date-audit-only


def _metric_query(
    table: str, plan: Mapping[str, Any]
) -> tuple[str | None, tuple[tuple[str, ...], ...]]:
    expressions: list[str] = []
    labels: list[tuple[str, ...]] = []
    if plan.get("count"):
        expressions.append("COUNT(*)")
        labels.append(("count",))
    for column in plan.get("boundary_columns", ()):
        quoted = _quote_identifier(str(column))
        expressions.extend((f"MIN({quoted})", f"MAX({quoted})"))
        labels.extend((("boundary", str(column), "min"), ("boundary", str(column), "max")))
    for raw_spec in plan.get("aggregates", ()):
        if not isinstance(raw_spec, AggregateSpec):
            raise ManifestError("invalid aggregate plan")
        quoted = _quote_identifier(raw_spec.column)
        for function in raw_spec.functions:
            if function == "count_nonnull":
                expression = f"COUNT({quoted})"
            else:
                expression = f"{function.upper()}({quoted})"
            expressions.append(expression)
            labels.append(("aggregate", raw_spec.column, function, raw_spec.absolute_tolerance))
    for column in plan.get("legacy_zero_columns", ()):
        quoted = _quote_identifier(str(column))
        expressions.append(
            "COALESCE(SUM(CASE WHEN CAST("
            f"{quoted} AS CHAR) LIKE '0000-00-00%' THEN 1 ELSE 0 END),0)"  # mysql84-zero-date-audit-only
        )
        labels.append(("legacy_zero", str(column)))
    if not expressions:
        return None, ()
    return f"SELECT {','.join(expressions)} FROM {_quote_table(table)}", tuple(labels)


def _capture_metrics(
    connection: pymysql.Connection,
    table: str,
    plan: Mapping[str, Any],
    table_catalog: Mapping[str, Any],
) -> dict[str, Any]:
    query, labels = _metric_query(table, plan)
    result: dict[str, Any] = {
        "exact_count": None,
        "boundaries": {},
        "aggregates": {},
        "legacy_zero_dates": {},
    }
    if query is None:
        return result
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    if row is None or len(row) != len(labels):
        raise ManifestError(f"metric query returned an invalid row for {table}")
    for label, value in zip(labels, row, strict=True):
        if label[0] == "count":
            result["exact_count"] = int(value)
        elif label[0] == "boundary":
            _kind, column, side = label
            result["boundaries"].setdefault(column, {})[side] = _json_value(value)
        elif label[0] == "aggregate":
            _kind, column, function, tolerance = label
            result["aggregates"].setdefault(column, {"absolute_tolerance": tolerance})[
                function
            ] = _json_value(value)
        elif label[0] == "legacy_zero":
            _kind, column = label
            default = _catalog_column(table_catalog, column).get("default")
            result["legacy_zero_dates"][column] = {
                "risk": "mysql84_unsafe_zero_date_default",
                "column_default": default,
                "default_is_zero_date": _is_zero_date_default(default),
                "stored_zero_count": int(value),
            }
        else:
            raise ManifestError(f"unknown metric label for {table}: {label}")
    return result


def _capture_hash(
    connection: pymysql.Connection,
    table: str,
    spec: HashSpec,
    table_catalog: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    columns = _resolved_hash_columns(spec, table_catalog)
    common = {
        "mode": spec.mode,
        "key_columns": list(spec.key_columns),
        "columns": list(columns),
        "chunk_rows": spec.chunk_rows,
        "selector": None,
        "evidence_strength": (
            "full_logical_rows_client_sha256"
            if spec.mode == "full_ordered_sha256"
            else (
                "deterministic_crc_selected_sample_only_not_full_proof"
                if spec.mode == "deterministic_sample_sha256"
                else "deterministic_pk_window_sample_only_not_full_proof"
            )
        ),
    }
    if spec.mode == "full_ordered_sha256":
        query, params, _ = _hash_query(table, spec, table_catalog, remainder=None)
        with connection.cursor(SSCursor) as cursor:
            cursor.execute(query, params)
            hashed = hash_rows_in_chunks(
                cursor, key_width=len(spec.key_columns), chunk_rows=spec.chunk_rows
            )
        return {**common, **hashed}

    if spec.mode == "deterministic_pk_windows_sha256":
        if spec.window_count is None or spec.window_rows is None:
            raise ManifestError(f"invalid PK-window configuration for {table}")
        key = spec.key_columns[0]
        boundaries = _require_object(metrics.get("boundaries"), f"{table} boundaries")
        key_boundary = _require_object(boundaries.get(key), f"{table}.{key} boundary")
        minimum_value = key_boundary.get("min")
        maximum_value = key_boundary.get("max")
        if minimum_value is None and maximum_value is None:
            anchors: tuple[int, ...] = ()
        else:
            minimum = _as_integer_value(minimum_value, f"{table}.{key}.min")
            maximum = _as_integer_value(maximum_value, f"{table}.{key}.max")
            anchors = deterministic_integer_anchors(minimum, maximum, spec.window_count)
        query, _ = _window_hash_query(table, spec, table_catalog)
        windows: list[dict[str, Any]] = []
        for anchor in anchors:
            with connection.cursor(SSCursor) as cursor:
                cursor.execute(query, (anchor,))
                hashed = hash_rows_in_chunks(
                    cursor, key_width=len(spec.key_columns), chunk_rows=spec.chunk_rows
                )
            windows.append({"anchor": str(anchor), **hashed})
        return {
            **common,
            "selector": {
                "algorithm": "evenly_spaced_integer_primary_key_windows",
                "window_count_requested": spec.window_count,
                "window_rows": spec.window_rows,
                "anchors": [str(value) for value in anchors],
                "warning": "Index-window samples are partial evidence and do not prove full equality.",
            },
            "sampled_row_reads": sum(int(window["row_count"]) for window in windows),
            "windows": windows,
        }

    buckets: list[dict[str, Any]] = []
    for remainder in spec.sample_remainders:
        query, params, _ = _hash_query(table, spec, table_catalog, remainder=remainder)
        with connection.cursor(SSCursor) as cursor:
            cursor.execute(query, params)
            hashed = hash_rows_in_chunks(
                cursor, key_width=len(spec.key_columns), chunk_rows=spec.chunk_rows
            )
        buckets.append({"remainder": remainder, **hashed})
    return {
        **common,
        "selector": {
            "algorithm": "mysql_crc32_of_length_prefixed_primary_key",
            "modulus": spec.sample_modulus,
            "remainders": list(spec.sample_remainders),
            "warning": "CRC32 selects rows only; matching sampled SHA-256 values do not prove full equality.",
        },
        "sampled_row_count": sum(int(bucket["row_count"]) for bucket in buckets),
        "buckets": buckets,
    }


def capture_table(
    connection: pymysql.Connection,
    table: str,
    plan: Mapping[str, Any],
    table_catalog: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _capture_metrics(connection, table, plan, table_catalog)
    spec = plan.get("hash")
    if spec is not None:
        if not isinstance(spec, HashSpec):
            raise ManifestError(f"invalid hash plan for {table}")
        metrics["hash"] = _capture_hash(connection, table, spec, table_catalog, metrics)
        if spec.mode == "full_ordered_sha256" and metrics["exact_count"] is not None:
            if int(metrics["exact_count"]) != int(metrics["hash"]["row_count"]):
                raise ManifestError(
                    f"{table} changed between COUNT and full hash inside the asserted snapshot"
                )
    else:
        metrics["hash"] = None
    return metrics


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def parse_timestamp(value: str, name: str) -> str:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ManifestError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ManifestError(f"{name} must include a timezone offset")
    return parsed.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


def validate_snapshot_arguments(
    *,
    mode: str,
    snapshot_id: str,
    workers: int,
    ddl_frozen: bool,
    writes_frozen: bool,
    writes_frozen_at: str | None,
    restore_artifact_sha256: str | None,
) -> dict[str, Any]:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"initial_consistent_snapshot", "cutover_writes_frozen"}:
        raise ManifestError(
            "snapshot mode must be initial_consistent_snapshot or cutover_writes_frozen"
        )
    label = snapshot_id.strip()
    if not label or len(label) > 200:
        raise ManifestError("snapshot_id must be non-empty and at most 200 characters")
    if not ddl_frozen:
        raise ManifestError("--assert-ddl-frozen is required for a stable catalogue")
    if normalized_mode == "initial_consistent_snapshot":
        if workers != 1:
            raise ManifestError("a live consistent snapshot must use exactly one connection")
        if writes_frozen or writes_frozen_at is not None or restore_artifact_sha256 is not None:
            raise ManifestError("write-freeze/artifact fields are invalid for an initial snapshot")
        return {
            "id": label,
            "mode": normalized_mode,
            "ddl_frozen_asserted": True,
            "writes_frozen_asserted": False,
            "writes_frozen_at": None,
            "restore_artifact_sha256": None,
            "eligible_for_final_cutover_comparison": False,
        }

    if not writes_frozen or writes_frozen_at is None:
        raise ManifestError(
            "cutover mode requires --assert-writes-frozen and --writes-frozen-at"
        )
    frozen_at = parse_timestamp(writes_frozen_at, "writes_frozen_at")
    artifact = str(restore_artifact_sha256 or "").lower()
    if not _SHA256_RE.fullmatch(artifact):
        raise ManifestError("cutover mode requires the exact restore artifact SHA-256")
    return {
        "id": label,
        "mode": normalized_mode,
        "ddl_frozen_asserted": True,
        "writes_frozen_asserted": True,
        "writes_frozen_at": frozen_at,
        "restore_artifact_sha256": artifact,
        "eligible_for_final_cutover_comparison": True,
    }


def _start_consistent_snapshot(connection: pymysql.Connection) -> str:
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    return _utc_now()


def _rollback_close(connection: pymysql.Connection) -> None:
    try:
        connection.rollback()
    finally:
        connection.close()


def _capture_one_with_new_connection(
    *,
    option_file: Path,
    ssl_ca: Path | None,
    expectation: EndpointExpectation,
    role: str,
    read_timeout: int,
    table: str,
    plan: Mapping[str, Any],
    table_catalog: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    connection = _connect(
        option_file,
        ssl_ca=ssl_ca,
        require_tls=expectation.require_tls,
        read_timeout=read_timeout,
    )
    try:
        identity = inspect_identity(connection)
        validate_identity(identity, expectation, role=role)
        _start_consistent_snapshot(connection)
        return table, capture_table(connection, table, plan, table_catalog)
    finally:
        _rollback_close(connection)


def _capture_all_tables(
    *,
    connection: pymysql.Connection | None,
    option_file: Path,
    ssl_ca: Path | None,
    expectation: EndpointExpectation,
    role: str,
    read_timeout: int,
    workers: int,
    plans: Mapping[str, Mapping[str, Any]],
    catalog: Mapping[str, Any],
    existing_measurements: Mapping[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    table_catalogs = _require_object(catalog.get("tables"), "catalog.tables")
    measurements: dict[str, Any] = dict(existing_measurements or {})
    unexpected = sorted(set(measurements) - set(plans))
    if unexpected:
        raise ManifestError(f"checkpoint contains tables outside the current plan: {unexpected}")
    pending_plans = {table: plan for table, plan in plans.items() if table not in measurements}
    if workers == 1:
        if connection is None:
            raise ManifestError("single-worker capture requires the established connection")
        transaction_started_at = _start_consistent_snapshot(connection)
        for table, plan in sorted(pending_plans.items()):
            print(f"[{role}] scanning configured checks for {table}", file=sys.stderr, flush=True)
            measurements[table] = capture_table(
                connection,
                table,
                plan,
                _require_object(table_catalogs[table], f"catalog.tables.{table}"),
            )
            if progress_callback is not None:
                progress_callback(dict(sorted(measurements.items())))
        return measurements, transaction_started_at

    # Multiple connections cannot share a MySQL snapshot.  The caller permits
    # this branch only after source writes are frozen or for a quiescent target.
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mysql-manifest") as executor:
        futures = {
            executor.submit(
                _capture_one_with_new_connection,
                option_file=option_file,
                ssl_ca=ssl_ca,
                expectation=expectation,
                role=role,
                read_timeout=read_timeout,
                table=table,
                plan=plan,
                table_catalog=_require_object(table_catalogs[table], f"catalog.tables.{table}"),
            ): table
            for table, plan in sorted(pending_plans.items())
        }
        for future in as_completed(futures):
            table, result = future.result()
            measurements[table] = result
            if progress_callback is not None:
                progress_callback(dict(sorted(measurements.items())))
            print(f"[{role}] completed configured checks for {table}", file=sys.stderr, flush=True)
    return dict(sorted(measurements.items())), None


def _critical_identity(identity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: identity.get(key)
        for key in ("version", "port", "server_uuid", "legacy_identity_sha256")
    }


def _checkpoint_payload(
    *,
    role: str,
    config: AuditConfig,
    identity: Mapping[str, Any],
    catalog: Mapping[str, Any],
    context: Mapping[str, Any],
    measurements: Mapping[str, Any],
    complete: bool,
) -> dict[str, Any]:
    return seal_document(
        {
            "format": CHECKPOINT_NAME,
            "format_version": FORMAT_VERSION,
            "role": role,
            "updated_at": _utc_now(),
            "config_sha256": config.sha256,
            "schemas": list(config.schemas),
            "endpoint": dict(identity),
            "catalog": dict(catalog),
            "context": dict(context),
            "measurements": dict(sorted(measurements.items())),
            "complete": complete,
            "resume_scope": "completed tables only; an interrupted in-flight table is rescanned",
        }
    )


def _load_checkpoint(
    *,
    path: Path,
    role: str,
    config: AuditConfig,
    identity: Mapping[str, Any],
    catalog: Mapping[str, Any],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    checkpoint = load_sealed_json(path)
    if checkpoint.get("format") != CHECKPOINT_NAME or checkpoint.get("format_version") != FORMAT_VERSION:
        raise ManifestError("unsupported checkpoint format")
    if checkpoint.get("role") != role:
        raise ManifestError("checkpoint role differs from this capture")
    if checkpoint.get("config_sha256") != config.sha256:
        raise ManifestError("checkpoint config differs from this capture")
    if checkpoint.get("schemas") != list(config.schemas):
        raise ManifestError("checkpoint schema list differs from this capture")
    if _critical_identity(_require_object(checkpoint.get("endpoint"), "checkpoint.endpoint")) != (
        _critical_identity(identity)
    ):
        raise ManifestError("checkpoint endpoint identity differs from the connected server")
    checkpoint_catalog = _require_object(checkpoint.get("catalog"), "checkpoint.catalog")
    if checkpoint_catalog != catalog:
        raise ManifestError("checkpoint catalogue differs from the connected server")
    if _require_object(checkpoint.get("context"), "checkpoint.context") != dict(context):
        raise ManifestError("checkpoint snapshot/provenance context differs from this capture")
    return _require_object(checkpoint.get("measurements"), "checkpoint.measurements")


def _checkpoint_callback(
    *,
    path: Path | None,
    role: str,
    config: AuditConfig,
    identity: Mapping[str, Any],
    catalog: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Callable[[dict[str, Any]], None] | None:
    if path is None:
        return None

    def write(measurements: dict[str, Any]) -> None:
        atomic_write_json(
            path,
            _checkpoint_payload(
                role=role,
                config=config,
                identity=identity,
                catalog=catalog,
                context=context,
                measurements=measurements,
                complete=False,
            ),
            overwrite=True,
        )

    return write


def _manifest_header(*, role: str, config: AuditConfig) -> dict[str, Any]:
    return {
        "format": FORMAT_NAME,
        "format_version": FORMAT_VERSION,
        "role": role,
        "config_sha256": config.sha256,
        "schemas": list(config.schemas),
    }


def _coverage_summary(
    config: AuditConfig, catalog: Mapping[str, Any], measurements: Mapping[str, Any]
) -> dict[str, Any]:
    base_tables = set(_base_tables(catalog))
    exact_count_tables = {
        table
        for table, raw in measurements.items()
        if _require_object(raw, f"measurements.{table}").get("exact_count") is not None
    }
    full_hash_tables: set[str] = set()
    full_hash_all_columns: set[str] = set()
    sampled_tables: set[str] = set()
    table_catalogs = _require_object(catalog.get("tables"), "catalog.tables")
    for table, raw in measurements.items():
        item = _require_object(raw, f"measurements.{table}")
        raw_hash = item.get("hash")
        if raw_hash is None:
            continue
        hash_result = _require_object(raw_hash, f"measurements.{table}.hash")
        if hash_result.get("mode") == "full_ordered_sha256":
            full_hash_tables.add(table)
            actual_columns = tuple(
                str(column["name"])
                for column in _require_list(
                    _require_object(table_catalogs[table], f"catalog.tables.{table}").get("columns"),
                    "catalog columns",
                )
            )
            if tuple(hash_result.get("columns", [])) == actual_columns:
                full_hash_all_columns.add(table)
        elif hash_result.get("mode") == "deterministic_sample_sha256":
            sampled_tables.add(table)
    return {
        "base_table_count": len(base_tables),
        "exact_count_table_count": len(exact_count_tables),
        "exact_counts_cover_all_base_tables": exact_count_tables == base_tables,
        "full_hash_table_count": len(full_hash_tables),
        "full_hash_all_columns_table_count": len(full_hash_all_columns),
        "full_logical_sha256_covers_all_base_tables": full_hash_all_columns == base_tables,
        "sampled_table_count": len(sampled_tables),
        "legacy_zero_date_check_count": len(config.legacy_zero_date_columns),
        "claims": {
            "sample_or_crc_proves_full_equality": False,
            "full_logical_hash_is_physical_backup_checksum": False,
            "note": (
                "Samples are partial evidence only. A full ordered SHA-256 covers logical values "
                "read through the connector in the asserted snapshot, not physical backup bytes."
            ),
        },
    }


def capture_source_manifest(
    *,
    config: AuditConfig,
    option_file: Path,
    ssl_ca: Path | None,
    workers: int,
    read_timeout: int,
    snapshot: dict[str, Any],
    checkpoint_path: Path | None = None,
    resume: bool = False,
) -> dict[str, Any]:
    if not 1 <= workers <= config.max_workers:
        raise ManifestError(f"workers must be in 1..{config.max_workers}")
    if snapshot["mode"] == "initial_consistent_snapshot" and workers != 1:
        raise ManifestError("initial consistent source capture cannot use parallel connections")
    if snapshot["mode"] == "initial_consistent_snapshot" and (checkpoint_path or resume):
        raise ManifestError(
            "live consistent snapshots cannot resume across connections; checkpoints require cutover write freeze"
        )
    if resume and checkpoint_path is None:
        raise ManifestError("--resume requires --checkpoint")
    if snapshot["mode"] == "cutover_writes_frozen":
        if config.counts.mode != "all":
            raise ManifestError("final cutover source manifest requires exact COUNT on all base tables")
        if set(config.legacy_zero_date_columns) != set(KNOWN_MYSQL84_ZERO_DATE_COLUMNS):
            raise ManifestError(
                "final cutover config must include the complete known ProBigA zero-date risk list"
            )

    started_at = _utc_now()
    if snapshot.get("writes_frozen_at"):
        frozen_at = dt.datetime.fromisoformat(str(snapshot["writes_frozen_at"]))
        started = dt.datetime.fromisoformat(started_at)
        if frozen_at > started:
            raise ManifestError("writes_frozen_at cannot be later than manifest capture start")

    connection = _connect(
        option_file,
        ssl_ca=ssl_ca,
        require_tls=config.source.require_tls,
        read_timeout=read_timeout,
    )
    try:
        identity = inspect_identity(connection)
        validate_identity(identity, config.source, role="source")
        catalog = load_catalog(connection, config.schemas)
        catalog_policy = _catalog_comparison_policy(config)
        expected_source_catalog = catalog_policy.get("source_catalog_sha256")
        if expected_source_catalog is not None and _catalog_signature_sha256(
            catalog
        ) != str(expected_source_catalog).lower():
            raise ManifestError("live source catalogue differs from the policy-builder snapshot")
        plans = build_table_plans(config, catalog)
        checkpoint_context = {
            key: snapshot.get(key)
            for key in (
                "id",
                "mode",
                "writes_frozen_at",
                "restore_artifact_sha256",
                "eligible_for_final_cutover_comparison",
            )
        }
        existing_measurements: dict[str, Any] = {}
        if resume:
            existing_measurements = _load_checkpoint(
                path=checkpoint_path,
                role="source",
                config=config,
                identity=identity,
                catalog=catalog,
                context=checkpoint_context,
            )
        elif checkpoint_path is not None and checkpoint_path.expanduser().resolve().exists():
            raise ManifestError("checkpoint already exists; pass --resume or choose a new path")
        callback = _checkpoint_callback(
            path=checkpoint_path,
            role="source",
            config=config,
            identity=identity,
            catalog=catalog,
            context=checkpoint_context,
        )
        if callback is not None and not existing_measurements:
            callback({})
        measurements, transaction_started_at = _capture_all_tables(
            connection=connection if workers == 1 else None,
            option_file=option_file,
            ssl_ca=ssl_ca,
            expectation=config.source,
            role="source",
            read_timeout=read_timeout,
            workers=workers,
            plans=plans,
            catalog=catalog,
            existing_measurements=existing_measurements,
            progress_callback=callback,
        )
    finally:
        _rollback_close(connection)

    finished_at = _utc_now()
    snapshot_record = dict(snapshot)
    snapshot_record.update(
        {
            "manifest_capture_started_at": started_at,
            "manifest_capture_finished_at": finished_at,
            "single_transaction_started_at": transaction_started_at,
            "parallel_connection_count": workers,
            "resumed_from_checkpoint": resume,
            "resumed_completed_table_count": len(existing_measurements),
            "consistency_note": (
                "Completed tables came from checkpoint transactions and remaining tables were rescanned "
                "while operator-asserted writes remained frozen."
                if resume
                else (
                    "One repeatable-read consistent transaction was used."
                    if workers == 1
                    else "Connections used independent snapshots while operator-asserted writes were frozen."
                )
            ),
        }
    )
    payload = {
        **_manifest_header(role="source", config=config),
        "created_at": finished_at,
        "endpoint": identity,
        "snapshot": snapshot_record,
        "catalog": catalog,
        "measurements": measurements,
        "coverage": _coverage_summary(config, catalog, measurements),
    }
    if checkpoint_path is not None:
        atomic_write_json(
            checkpoint_path,
            _checkpoint_payload(
                role="source",
                config=config,
                identity=identity,
                catalog=catalog,
                context=checkpoint_context,
                measurements=measurements,
                complete=True,
            ),
            overwrite=True,
        )
    return seal_document(payload)


def _validate_manifest_shape(document: Mapping[str, Any], *, role: str) -> None:
    verify_document(document)
    if document.get("format") != FORMAT_NAME or document.get("format_version") != FORMAT_VERSION:
        raise ManifestError("unsupported data manifest format")
    if document.get("role") != role:
        raise ManifestError(f"expected a {role} manifest")
    schemas = document.get("schemas")
    if not isinstance(schemas, list) or not schemas:
        raise ManifestError("manifest schemas are missing")
    if not isinstance(document.get("catalog"), dict) or not isinstance(
        document.get("measurements"), dict
    ):
        raise ManifestError("manifest catalogue or measurements are missing")


def capture_target_manifest(
    *,
    config: AuditConfig,
    option_file: Path,
    ssl_ca: Path,
    workers: int,
    read_timeout: int,
    source_manifest: Mapping[str, Any],
    target_quiescent: bool,
    restored_artifact_sha256: str | None = None,
    checkpoint_path: Path | None = None,
    resume: bool = False,
) -> dict[str, Any]:
    _validate_manifest_shape(source_manifest, role="source")
    if not target_quiescent:
        raise ManifestError("--assert-target-quiescent is required")
    if not 1 <= workers <= config.max_workers:
        raise ManifestError(f"workers must be in 1..{config.max_workers}")
    if resume and checkpoint_path is None:
        raise ManifestError("--resume requires --checkpoint")
    if source_manifest.get("config_sha256") != config.sha256:
        raise ManifestError("target config differs from the sealed source manifest config")
    if source_manifest.get("schemas") != list(config.schemas):
        raise ManifestError("target schema list differs from the source manifest")
    source_snapshot = _require_object(source_manifest.get("snapshot"), "source snapshot")
    expected_artifact = source_snapshot.get("restore_artifact_sha256")
    observed_artifact = (
        None if restored_artifact_sha256 is None else restored_artifact_sha256.strip().lower()
    )
    if expected_artifact is None:
        if observed_artifact is not None:
            raise ManifestError("initial snapshot has no restore artifact SHA-256 to attest")
    elif observed_artifact != expected_artifact:
        raise ManifestError("restored artifact SHA-256 differs from the sealed source manifest")

    started_at = _utc_now()
    connection = _connect(
        option_file,
        ssl_ca=ssl_ca,
        require_tls=config.target.require_tls,
        read_timeout=read_timeout,
    )
    try:
        identity = inspect_identity(connection)
        validate_identity(identity, config.target, role="target")
        source_identity = _require_object(source_manifest.get("endpoint"), "source endpoint")
        if identity.get("server_uuid") and identity.get("server_uuid") == source_identity.get(
            "server_uuid"
        ):
            raise ManifestError("source and target resolve to the same server_uuid")
        if identity.get("legacy_identity_sha256") == source_identity.get("legacy_identity_sha256"):
            raise ManifestError("source and target resolve to the same legacy endpoint fingerprint")
        observed_catalog = load_catalog(connection, config.schemas)
        source_catalog = _require_object(source_manifest.get("catalog"), "source catalog")
        catalog_policy = _catalog_comparison_policy(config)
        if catalog_policy["mode"] == "reviewed_v2_v3_v4_source_projection":
            if _catalog_signature_sha256(source_catalog) != str(
                catalog_policy["source_catalog_sha256"]
            ).lower():
                raise ManifestError("sealed source catalogue differs from projection policy")
            if _catalog_signature_sha256(observed_catalog) != str(
                catalog_policy["target_catalog_sha256"]
            ).lower():
                raise ManifestError("live target catalogue differs from the policy-builder snapshot")
            catalog, catalog_attestation = _project_target_catalog(
                source_catalog, observed_catalog
            )
            if catalog_attestation["target_only_tables"] != list(
                catalog_policy.get("target_only_tables", [])
            ) or catalog_attestation["target_extended_columns"] != dict(
                catalog_policy.get("target_extended_columns", {})
            ):
                raise ManifestError("live target additions differ from the reviewed V2/V3/V4 set")
        else:
            _assert_catalog_matches(source_catalog, observed_catalog)
            expected_target_catalog = catalog_policy.get("target_catalog_sha256")
            if expected_target_catalog is not None and _catalog_signature_sha256(
                observed_catalog
            ) != str(expected_target_catalog).lower():
                raise ManifestError("live target catalogue differs from the policy-builder snapshot")
            catalog = observed_catalog
            catalog_attestation = {
                "mode": "exact",
                "source_catalog_sha256": _catalog_signature_sha256(source_catalog),
                "target_catalog_sha256": _catalog_signature_sha256(observed_catalog),
                "source_table_count": len(_base_tables(source_catalog)),
                "target_table_count": len(_base_tables(observed_catalog)),
                "target_only_tables": [],
                "target_extended_columns": {},
            }
        plans = build_table_plans(config, catalog)
        checkpoint_context = {
            "source_manifest_sha256": source_manifest[DOCUMENT_DIGEST_FIELD],
            "source_snapshot_id": _require_object(
                source_manifest.get("snapshot"), "source snapshot"
            ).get("id"),
            "restore_artifact_sha256": observed_artifact,
            "observed_target_catalog_sha256": catalog_attestation[
                "target_catalog_sha256"
            ],
        }
        existing_measurements: dict[str, Any] = {}
        if resume:
            existing_measurements = _load_checkpoint(
                path=checkpoint_path,
                role="target",
                config=config,
                identity=identity,
                catalog=catalog,
                context=checkpoint_context,
            )
        elif checkpoint_path is not None and checkpoint_path.expanduser().resolve().exists():
            raise ManifestError("checkpoint already exists; pass --resume or choose a new path")
        callback = _checkpoint_callback(
            path=checkpoint_path,
            role="target",
            config=config,
            identity=identity,
            catalog=catalog,
            context=checkpoint_context,
        )
        if callback is not None and not existing_measurements:
            callback({})
        measurements, transaction_started_at = _capture_all_tables(
            connection=connection if workers == 1 else None,
            option_file=option_file,
            ssl_ca=ssl_ca,
            expectation=config.target,
            role="target",
            read_timeout=read_timeout,
            workers=workers,
            plans=plans,
            catalog=catalog,
            existing_measurements=existing_measurements,
            progress_callback=callback,
        )
    finally:
        _rollback_close(connection)

    finished_at = _utc_now()
    payload = {
        **_manifest_header(role="target", config=config),
        "created_at": finished_at,
        "endpoint": identity,
        "restored_from": {
            "source_manifest_sha256": source_manifest[DOCUMENT_DIGEST_FIELD],
            "source_snapshot_id": _require_object(
                source_manifest.get("snapshot"), "source snapshot"
            ).get("id"),
            "restore_artifact_sha256": _require_object(
                source_manifest.get("snapshot"), "source snapshot"
            ).get("restore_artifact_sha256"),
        },
        "capture": {
            "target_quiescent_asserted": True,
            "capture_started_at": started_at,
            "capture_finished_at": finished_at,
            "single_transaction_started_at": transaction_started_at,
            "parallel_connection_count": workers,
            "resumed_from_checkpoint": resume,
            "resumed_completed_table_count": len(existing_measurements),
        },
        "catalog": catalog,
        "observed_target_catalog": catalog_attestation,
        "measurements": measurements,
        "coverage": _coverage_summary(config, catalog, measurements),
    }
    if checkpoint_path is not None:
        atomic_write_json(
            checkpoint_path,
            _checkpoint_payload(
                role="target",
                config=config,
                identity=identity,
                catalog=catalog,
                context=checkpoint_context,
                measurements=measurements,
                complete=True,
            ),
            overwrite=True,
        )
    return seal_document(payload)


def _as_decimal(value: object) -> decimal.Decimal | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return None
    if value.get("type") not in {"int", "decimal", "float"}:
        return None
    try:
        parsed = decimal.Decimal(str(value.get("value")))
    except decimal.InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _aggregate_values_match(source: object, target: object, tolerance: str) -> bool:
    if source == target:
        return True
    left = _as_decimal(source)
    right = _as_decimal(target)
    if left is None or right is None:
        return False
    return abs(left - right) <= decimal.Decimal(tolerance)


def _append_mismatch(
    mismatches: list[dict[str, Any]], kind: str, subject: str, detail: str
) -> None:
    mismatches.append({"kind": kind, "subject": subject, "detail": detail})


def _manifest_endpoint_expectation(
    manifest: Mapping[str, Any], *, role: str
) -> EndpointExpectation:
    endpoint = _require_object(manifest.get("endpoint"), f"{role}.endpoint")
    return EndpointExpectation(
        version=str(endpoint.get("version") or ""),
        port=int(endpoint.get("port") or 0),
        server_uuid=(
            str(endpoint.get("server_uuid") or "").lower()
            if endpoint.get("server_uuid") is not None
            else None
        ),
        legacy_identity_sha256=(
            str(endpoint.get("legacy_identity_sha256") or "").lower()
            if endpoint.get("legacy_identity_sha256") is not None
            else None
        ),
        require_tls=role == "target",
    )


def _catalog_table_rows(
    connection: pymysql.Connection,
    *,
    catalog: Mapping[str, Any],
    table: str,
) -> tuple[tuple[str, ...], tuple[str, ...], dict[tuple[object, ...], tuple[object, ...]]]:
    tables = _require_object(catalog.get("tables"), "catalog.tables")
    table_catalog = _require_object(tables.get(table), f"catalog.tables.{table}")
    columns = tuple(
        str(item["name"])
        for item in _require_list(table_catalog.get("columns"), f"{table}.columns")
    )
    primary_key = tuple(str(value) for value in table_catalog.get("primary_key", ()))
    if not columns or not primary_key:
        raise ManifestError(f"reviewed transition table lacks columns/primary key: {table}")
    column_indexes = {name: index for index, name in enumerate(columns)}
    if not set(primary_key).issubset(column_indexes):
        raise ManifestError(f"reviewed transition primary key is absent from columns: {table}")
    query = (
        "SELECT "
        + ",".join(_quote_identifier(column) for column in columns)
        + f" FROM {_quote_table(table)} ORDER BY "
        + ",".join(_quote_identifier(column) for column in primary_key)
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        fetched = tuple(tuple(row) for row in cursor.fetchall())
    rows: dict[tuple[object, ...], tuple[object, ...]] = {}
    for row in fetched:
        key = tuple(row[column_indexes[column]] for column in primary_key)
        if key in rows:
            raise ManifestError(f"duplicate primary key in reviewed transition table: {table}")
        rows[key] = row
    return columns, primary_key, rows


def _row_digest(row: Sequence[object]) -> str:
    return hashlib.sha256(canonical_row_bytes(row)).hexdigest()


def _require_rows_equal_except(
    *,
    table: str,
    key: tuple[object, ...],
    columns: Sequence[str],
    source_row: Sequence[object],
    target_row: Sequence[object],
    allowed_columns: frozenset[str],
) -> None:
    if len(source_row) != len(target_row) or len(columns) != len(source_row):
        raise ManifestError(f"reviewed transition row shape differs: {table} {key!r}")
    for index, column in enumerate(columns):
        if column not in allowed_columns and canonical_row_bytes(
            (source_row[index],)
        ) != canonical_row_bytes((target_row[index],)):
            raise ManifestError(
                f"unreviewed value changed in transition table: {table} {key!r} {column}"
            )


def attest_reviewed_post_migration_transitions(
    *,
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    source_connection: pymysql.Connection,
    target_connection: pymysql.Connection,
    raw_mismatches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Prove the exact V2/V3/V4 mutations applied after the base data copy.

    This gate is intentionally narrow. It accepts only the known migration-ledger
    additions and four reviewed lifecycle/scheduler transitions. Every other
    projected column and every other row in those tables must remain identical.
    """

    mismatch_signature = frozenset(
        (str(item.get("kind") or ""), str(item.get("subject") or ""))
        for item in raw_mismatches
    )
    if mismatch_signature != REVIEWED_POST_MIGRATION_MISMATCHES or len(
        raw_mismatches
    ) != len(REVIEWED_POST_MIGRATION_MISMATCHES):
        raise ManifestError("raw mismatch set is not the exact reviewed post-migration set")
    observed_catalog = _require_object(
        target.get("observed_target_catalog"), "target.observed_target_catalog"
    )
    if observed_catalog.get("mode") != "reviewed_v2_v3_v4_source_projection":
        raise ManifestError("target manifest is not a reviewed V2/V3/V4 projection")

    source_identity = inspect_identity(source_connection)
    target_identity = inspect_identity(target_connection)
    validate_identity(
        source_identity,
        _manifest_endpoint_expectation(source, role="source"),
        role="source transition attestation",
    )
    validate_identity(
        target_identity,
        _manifest_endpoint_expectation(target, role="target"),
        role="target transition attestation",
    )

    source_catalog = _require_object(source.get("catalog"), "source.catalog")
    target_catalog = _require_object(target.get("catalog"), "target.catalog")
    table_evidence: list[dict[str, Any]] = []

    ledger_specs = (
        (
            "probiga.schema_migration_v2",
            REVIEWED_V2_SOURCE_VERSIONS,
            REVIEWED_V2_TARGET_ONLY_VERSIONS,
        ),
        (
            "probiga.schema_migration_v3",
            REVIEWED_V3_SOURCE_VERSIONS,
            REVIEWED_V3_TARGET_ONLY_VERSIONS,
        ),
    )
    for table, expected_source, expected_target_only in ledger_specs:
        source_columns, source_pk, source_rows = _catalog_table_rows(
            source_connection, catalog=source_catalog, table=table
        )
        target_columns, target_pk, target_rows = _catalog_table_rows(
            target_connection, catalog=target_catalog, table=table
        )
        if source_columns != target_columns or source_pk != target_pk or source_pk != (
            "version",
        ):
            raise ManifestError(f"migration ledger projection differs: {table}")
        source_versions = frozenset(str(key[0]) for key in source_rows)
        target_versions = frozenset(str(key[0]) for key in target_rows)
        if source_versions != expected_source:
            raise ManifestError(f"source migration ledger is not the reviewed baseline: {table}")
        if target_versions - source_versions != expected_target_only:
            raise ManifestError(f"target migration ledger additions are not reviewed: {table}")
        if source_versions - target_versions:
            raise ManifestError(f"target migration ledger lost source versions: {table}")
        for key, source_row in source_rows.items():
            if canonical_row_bytes(source_row) != canonical_row_bytes(target_rows[key]):
                raise ManifestError(f"common migration ledger row changed: {table} {key!r}")
        table_evidence.append(
            {
                "table": table,
                "source_row_count": len(source_rows),
                "target_row_count": len(target_rows),
                "target_only_versions": sorted(expected_target_only),
                "common_rows_identical": True,
            }
        )

    transition_specs: dict[
        str, dict[tuple[object, ...], tuple[frozenset[str], Mapping[str, object]]]
    ] = {
        "probiga.st_model_registry_v3": {
            ("f3f473aba627475e9340958fc66fd3dd",): (
                frozenset({"lifecycle_status"}),
                {"lifecycle_status": ("PAPER_ACTIVE", "RETIRED")},
            )
        },
        "probiga.st_scheduled_tasks": {
            (66,): (
                frozenset({"enabled", "description"}),
                {"enabled": (1, 0), "description_suffix": " [V3.3.0已隔离旧选股入口]"},
            ),
            (69,): (
                frozenset({"enabled", "description"}),
                {"enabled": (1, 0), "description_suffix": " [V3.3.0已隔离旧选股入口]"},
            ),
        },
        "probiga.st_strategy_version_v2": {
            ("intraday_dynamic_activation", "intraday_dynamic_activation_v2.5.0"): (
                frozenset({"lifecycle_status", "suspended_at"}),
                {"lifecycle_status": ("PAPER_TRIAL", "SUSPENDED")},
            ),
            ("sector_preheat", "sector_preheat_v1.4.0"): (
                frozenset({"lifecycle_status", "suspended_at"}),
                {"lifecycle_status": ("PAPER_TRIAL", "SUSPENDED")},
            ),
        },
    }
    strategy_suspend_values: set[bytes] = set()
    for table, allowed in transition_specs.items():
        source_columns, source_pk, source_rows = _catalog_table_rows(
            source_connection, catalog=source_catalog, table=table
        )
        target_columns, target_pk, target_rows = _catalog_table_rows(
            target_connection, catalog=target_catalog, table=table
        )
        if source_columns != target_columns or source_pk != target_pk:
            raise ManifestError(f"transition table projection differs: {table}")
        if set(source_rows) != set(target_rows):
            raise ManifestError(f"transition table primary-key set differs: {table}")
        if not set(allowed).issubset(source_rows):
            raise ManifestError(f"reviewed transition row is absent: {table}")
        indexes = {column: index for index, column in enumerate(source_columns)}
        changed: list[dict[str, Any]] = []
        for key, source_row in source_rows.items():
            target_row = target_rows[key]
            if key not in allowed:
                if canonical_row_bytes(source_row) != canonical_row_bytes(target_row):
                    raise ManifestError(f"unreviewed row changed in transition table: {table} {key!r}")
                continue
            allowed_columns, rules = allowed[key]
            _require_rows_equal_except(
                table=table,
                key=key,
                columns=source_columns,
                source_row=source_row,
                target_row=target_row,
                allowed_columns=allowed_columns,
            )
            lifecycle = rules.get("lifecycle_status")
            if lifecycle is not None:
                index = indexes["lifecycle_status"]
                if (source_row[index], target_row[index]) != tuple(lifecycle):
                    raise ManifestError(f"lifecycle transition is not reviewed: {table} {key!r}")
            enabled = rules.get("enabled")
            if enabled is not None:
                index = indexes["enabled"]
                if (int(source_row[index]), int(target_row[index])) != tuple(enabled):
                    raise ManifestError(f"scheduler enable transition is not reviewed: {key!r}")
            suffix = rules.get("description_suffix")
            if suffix is not None:
                index = indexes["description"]
                if str(target_row[index]) != str(source_row[index]) + str(suffix):
                    raise ManifestError(f"scheduler description transition is not reviewed: {key!r}")
            if "suspended_at" in allowed_columns:
                index = indexes["suspended_at"]
                if source_row[index] is not None or target_row[index] is None:
                    raise ManifestError(f"strategy suspension timestamp is invalid: {key!r}")
                strategy_suspend_values.add(canonical_row_bytes((target_row[index],)))
            changed.append(
                {
                    "primary_key": [str(value) for value in key],
                    "source_row_sha256": _row_digest(source_row),
                    "target_row_sha256": _row_digest(target_row),
                    "changed_columns": sorted(allowed_columns),
                }
            )
        table_evidence.append(
            {
                "table": table,
                "source_row_count": len(source_rows),
                "target_row_count": len(target_rows),
                "unchanged_rows_identical": True,
                "reviewed_transitions": changed,
            }
        )
    if len(strategy_suspend_values) != 1:
        raise ManifestError("reviewed strategy rows do not share one suspension timestamp")

    return seal_document(
        {
            "format": "probiga.mysql55_to_mysql84.reviewed_post_migration_transition",
            "format_version": 1,
            "status": "passed",
            "created_at": _utc_now(),
            "source_manifest_sha256": source[DOCUMENT_DIGEST_FIELD],
            "target_manifest_sha256": target[DOCUMENT_DIGEST_FIELD],
            "raw_mismatch_signature": [
                {"kind": kind, "subject": subject}
                for kind, subject in sorted(REVIEWED_POST_MIGRATION_MISMATCHES)
            ],
            "source_identity": _critical_identity(source_identity),
            "target_identity": _critical_identity(target_identity),
            "target_tls_cipher_sha256": hashlib.sha256(
                str(target_identity.get("ssl_cipher") or "").encode("utf-8")
            ).hexdigest(),
            "tables": table_evidence,
            "unreviewed_difference_count": 0,
            "secrets_in_evidence": False,
        }
    )


def compare_manifests(
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    *,
    reviewed_post_migration_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_manifest_shape(source, role="source")
    _validate_manifest_shape(target, role="target")
    mismatches: list[dict[str, Any]] = []

    if source.get("config_sha256") != target.get("config_sha256"):
        _append_mismatch(mismatches, "config", "config_sha256", "source and target configs differ")
    if source.get("schemas") != target.get("schemas"):
        _append_mismatch(mismatches, "schema_list", "schemas", "source and target schema lists differ")
    restored_from = _require_object(target.get("restored_from"), "target.restored_from")
    if restored_from.get("source_manifest_sha256") != source.get(DOCUMENT_DIGEST_FIELD):
        _append_mismatch(
            mismatches,
            "provenance",
            "source_manifest_sha256",
            "target is not linked to this exact sealed source manifest",
        )
    source_snapshot = _require_object(source.get("snapshot"), "source.snapshot")
    if restored_from.get("source_snapshot_id") != source_snapshot.get("id"):
        _append_mismatch(
            mismatches, "provenance", "source_snapshot_id", "target snapshot link differs"
        )
    if restored_from.get("restore_artifact_sha256") != source_snapshot.get(
        "restore_artifact_sha256"
    ):
        _append_mismatch(
            mismatches, "provenance", "restore_artifact_sha256", "restore artifact link differs"
        )

    source_catalog = _require_object(source.get("catalog"), "source.catalog")
    target_catalog = _require_object(target.get("catalog"), "target.catalog")
    if _catalog_for_comparison(source_catalog) != _catalog_for_comparison(target_catalog):
        _append_mismatch(
            mismatches,
            "catalog",
            "logical_catalog",
            "table/column/primary-key catalogue differs",
        )

    source_measurements = _require_object(source.get("measurements"), "source.measurements")
    target_measurements = _require_object(target.get("measurements"), "target.measurements")
    if set(source_measurements) != set(target_measurements):
        _append_mismatch(
            mismatches, "measurement_plan", "tables", "measured table sets differ"
        )

    legacy_risks: list[dict[str, Any]] = []
    full_hashes_match = True
    sample_hashes_match = True
    for table in sorted(set(source_measurements) | set(target_measurements)):
        if table not in source_measurements or table not in target_measurements:
            continue
        left = _require_object(source_measurements[table], f"source.measurements.{table}")
        right = _require_object(target_measurements[table], f"target.measurements.{table}")
        if left.get("exact_count") != right.get("exact_count"):
            _append_mismatch(mismatches, "exact_count", table, "COUNT(*) differs")
        if left.get("boundaries") != right.get("boundaries"):
            _append_mismatch(mismatches, "boundary", table, "PK/date boundary differs")

        left_aggregates = _require_object(left.get("aggregates", {}), "source aggregates")
        right_aggregates = _require_object(right.get("aggregates", {}), "target aggregates")
        if set(left_aggregates) != set(right_aggregates):
            _append_mismatch(mismatches, "aggregate", table, "aggregate columns differ")
        for column in sorted(set(left_aggregates) & set(right_aggregates)):
            left_column = _require_object(left_aggregates[column], "source aggregate column")
            right_column = _require_object(right_aggregates[column], "target aggregate column")
            tolerance = str(left_column.get("absolute_tolerance", "0"))
            if tolerance != str(right_column.get("absolute_tolerance", "0")):
                _append_mismatch(
                    mismatches, "aggregate", f"{table}.{column}", "tolerances differ"
                )
                continue
            functions = (set(left_column) | set(right_column)) - {"absolute_tolerance"}
            for function in sorted(functions):
                if not _aggregate_values_match(
                    left_column.get(function), right_column.get(function), tolerance
                ):
                    _append_mismatch(
                        mismatches,
                        "aggregate",
                        f"{table}.{column}.{function}",
                        f"values differ beyond absolute tolerance {tolerance}",
                    )

        left_legacy = _require_object(left.get("legacy_zero_dates", {}), "source legacy")
        right_legacy = _require_object(right.get("legacy_zero_dates", {}), "target legacy")
        if set(left_legacy) != set(right_legacy):
            _append_mismatch(mismatches, "legacy_zero_date", table, "risk columns differ")
        for column in sorted(set(left_legacy) & set(right_legacy)):
            source_risk = _require_object(left_legacy[column], "source legacy risk")
            target_risk = _require_object(right_legacy[column], "target legacy risk")
            row_match = source_risk.get("stored_zero_count") == target_risk.get(
                "stored_zero_count"
            )
            target_default_safe = not bool(target_risk.get("default_is_zero_date"))
            risk_result = {
                "column": f"{table}.{column}",
                "risk": "mysql84_unsafe_zero_date_default",
                "source_default_is_zero_date": bool(source_risk.get("default_is_zero_date")),
                "target_default_is_safe": target_default_safe,
                "source_stored_zero_count": source_risk.get("stored_zero_count"),
                "target_stored_zero_count": target_risk.get("stored_zero_count"),
                "stored_zero_counts_match": row_match,
                "note": (
                    "Stored rows may match while the 5.5 zero-date default remains unsafe on MySQL 8.4."
                ),
            }
            legacy_risks.append(risk_result)
            if not row_match:
                _append_mismatch(
                    mismatches,
                    "legacy_zero_date",
                    f"{table}.{column}",
                    "stored zero-date counts differ",
                )
            if not target_default_safe:
                _append_mismatch(
                    mismatches,
                    "legacy_zero_date",
                    f"{table}.{column}",
                    "target still has an unsafe zero-date default",
                )

        left_hash = left.get("hash")
        right_hash = right.get("hash")
        if (left_hash is None) != (right_hash is None):
            _append_mismatch(mismatches, "hash", table, "hash plan differs")
            full_hashes_match = False
            sample_hashes_match = False
        elif left_hash is not None and right_hash is not None:
            left_hash_obj = _require_object(left_hash, "source hash")
            right_hash_obj = _require_object(right_hash, "target hash")
            metadata_fields = ("mode", "key_columns", "columns", "chunk_rows", "selector")
            metadata_match = all(
                left_hash_obj.get(field) == right_hash_obj.get(field) for field in metadata_fields
            )
            if not metadata_match:
                _append_mismatch(mismatches, "hash", table, "hash metadata differs")
            if left_hash_obj.get("mode") == "full_ordered_sha256":
                match = metadata_match and all(
                    left_hash_obj.get(field) == right_hash_obj.get(field)
                    for field in ("row_count", "overall_sha256", "chunks")
                )
                full_hashes_match = full_hashes_match and match
                if not match:
                    _append_mismatch(
                        mismatches, "full_logical_sha256", table, "full ordered hash differs"
                    )
            elif left_hash_obj.get("mode") == "deterministic_sample_sha256":
                match = metadata_match and left_hash_obj.get("buckets") == right_hash_obj.get(
                    "buckets"
                )
                sample_hashes_match = sample_hashes_match and match
                if not match:
                    _append_mismatch(
                        mismatches, "sample_sha256", table, "deterministic sample differs"
                    )
            else:
                match = metadata_match and left_hash_obj.get("windows") == right_hash_obj.get(
                    "windows"
                )
                sample_hashes_match = sample_hashes_match and match
                if not match:
                    _append_mismatch(
                        mismatches,
                        "pk_window_sample_sha256",
                        table,
                        "deterministic primary-key window sample differs",
                    )

    source_coverage = _require_object(source.get("coverage"), "source.coverage")
    target_coverage = _require_object(target.get("coverage"), "target.coverage")
    full_coverage = bool(source_coverage.get("full_logical_sha256_covers_all_base_tables")) and bool(
        target_coverage.get("full_logical_sha256_covers_all_base_tables")
    )
    samples_configured = bool(source_coverage.get("sampled_table_count")) or bool(
        target_coverage.get("sampled_table_count")
    )
    raw_mismatches = list(mismatches)
    reviewed_transitions_applied = reviewed_post_migration_attestation is not None
    if reviewed_transitions_applied:
        attestation = _require_object(
            reviewed_post_migration_attestation,
            "reviewed_post_migration_attestation",
        )
        verify_document(attestation)
        if (
            attestation.get("format")
            != "probiga.mysql55_to_mysql84.reviewed_post_migration_transition"
            or attestation.get("status") != "passed"
            or attestation.get("source_manifest_sha256")
            != source[DOCUMENT_DIGEST_FIELD]
            or attestation.get("target_manifest_sha256")
            != target[DOCUMENT_DIGEST_FIELD]
            or attestation.get("unreviewed_difference_count") != 0
        ):
            raise ManifestError("reviewed post-migration attestation is not bound to these manifests")
        mismatch_signature = frozenset(
            (str(item.get("kind") or ""), str(item.get("subject") or ""))
            for item in raw_mismatches
        )
        if mismatch_signature != REVIEWED_POST_MIGRATION_MISMATCHES or len(
            raw_mismatches
        ) != len(REVIEWED_POST_MIGRATION_MISMATCHES):
            raise ManifestError("attestation cannot waive a non-reviewed mismatch")
        mismatches = []
    configured_match = not mismatches
    final_snapshot = bool(source_snapshot.get("eligible_for_final_cutover_comparison"))
    count_coverage = bool(source_coverage.get("exact_counts_cover_all_base_tables")) and bool(
        target_coverage.get("exact_counts_cover_all_base_tables")
    )
    legacy_safe = all(item["target_default_is_safe"] for item in legacy_risks)
    risk_based_cutover_passed = (
        configured_match and final_snapshot and count_coverage and legacy_safe
    )
    report = {
        "format": REPORT_NAME,
        "format_version": FORMAT_VERSION,
        "created_at": _utc_now(),
        "source_manifest_sha256": source[DOCUMENT_DIGEST_FIELD],
        "target_manifest_sha256": target[DOCUMENT_DIGEST_FIELD],
        "snapshot_id": source_snapshot.get("id"),
        "result": {
            "configured_checks_match": configured_match,
            "exact_counts_cover_all_base_tables": count_coverage,
            "full_logical_sha256_coverage": full_coverage,
            "full_logical_sha256_match": full_coverage and full_hashes_match,
            "deterministic_samples_configured": samples_configured,
            "deterministic_samples_match": sample_hashes_match if samples_configured else None,
            "sample_or_crc_proves_full_equality": False,
            "physical_backup_bytes_proven_equal": False,
            "risk_based_cutover_checks_passed": risk_based_cutover_passed,
            "reviewed_post_migration_transitions_applied": reviewed_transitions_applied,
            "interpretation": (
                "A sampled match is partial evidence only. Full logical SHA-256, when configured "
                "for every column of every base table, is a complete logical read comparison for "
                "the asserted snapshot; it is not a checksum of physical backup files."
            ),
        },
        "mismatches": mismatches,
        "raw_mismatches": raw_mismatches if reviewed_transitions_applied else [],
        "reviewed_post_migration_attestation": (
            dict(reviewed_post_migration_attestation)
            if reviewed_post_migration_attestation is not None
            else None
        ),
        "legacy_zero_date_risks": legacy_risks,
    }
    return seal_document(report)


def _path(value: str | None) -> Path | None:
    return None if value is None else Path(value)


def _ensure_output_available(path: Path, *, overwrite: bool) -> None:
    resolved = path.expanduser().resolve()
    if resolved.exists() and not overwrite:
        raise ManifestError(f"refusing to overwrite existing output: {resolved}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and compare bounded-memory MySQL 5.5 -> 8.4 data manifests."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser(
        "probe-identity",
        help="Read only lightweight server identity fields; use the fingerprint to pin config.",
    )
    probe.add_argument("--option-file", type=Path, required=True)
    probe.add_argument("--ssl-ca", type=Path)
    probe.add_argument("--require-tls", action="store_true")
    probe.add_argument("--read-timeout", type=int, default=60)

    source = subparsers.add_parser("capture-source", help="Capture a sealed source manifest.")
    source.add_argument("--config", type=Path, required=True)
    source.add_argument("--option-file", type=Path, required=True)
    source.add_argument("--ssl-ca", type=Path)
    source.add_argument("--output", type=Path, required=True)
    source.add_argument("--overwrite", action="store_true")
    source.add_argument("--workers", type=int, default=1)
    source.add_argument("--read-timeout", type=int, default=86_400)
    source.add_argument("--checkpoint", type=Path)
    source.add_argument("--resume", action="store_true")
    source.add_argument(
        "--snapshot-mode",
        choices=("initial_consistent_snapshot", "cutover_writes_frozen"),
        required=True,
    )
    source.add_argument("--snapshot-id", required=True)
    source.add_argument("--assert-ddl-frozen", action="store_true")
    source.add_argument("--assert-writes-frozen", action="store_true")
    source.add_argument("--writes-frozen-at")
    source.add_argument("--restore-artifact-sha256")

    target = subparsers.add_parser("capture-target", help="Capture a sealed restored-target manifest.")
    target.add_argument("--config", type=Path, required=True)
    target.add_argument("--option-file", type=Path, required=True)
    target.add_argument("--ssl-ca", type=Path, required=True)
    target.add_argument("--source-manifest", type=Path, required=True)
    target.add_argument("--output", type=Path, required=True)
    target.add_argument("--overwrite", action="store_true")
    target.add_argument("--workers", type=int, default=1)
    target.add_argument("--read-timeout", type=int, default=86_400)
    target.add_argument("--checkpoint", type=Path)
    target.add_argument("--resume", action="store_true")
    target.add_argument("--assert-target-quiescent", action="store_true")
    target.add_argument(
        "--restored-artifact-sha256",
        help="Required for cutover manifests; independently attests the dump restored to target.",
    )

    compare = subparsers.add_parser("compare", help="Compare two sealed manifests offline.")
    compare.add_argument("--source-manifest", type=Path, required=True)
    compare.add_argument("--target-manifest", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--overwrite", action="store_true")
    compare.add_argument("--allow-reviewed-post-migration-transitions", action="store_true")
    compare.add_argument("--source-option-file", type=Path)
    compare.add_argument("--target-option-file", type=Path)
    compare.add_argument("--target-ssl-ca", type=Path)
    return parser


def _validate_timeout(value: int) -> int:
    if not 1 <= value <= 604_800:
        raise ManifestError("read-timeout must be in 1..604800 seconds")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "probe-identity":
            timeout = _validate_timeout(args.read_timeout)
            connection = _connect(
                args.option_file,
                ssl_ca=args.ssl_ca,
                require_tls=args.require_tls,
                read_timeout=timeout,
            )
            try:
                identity = inspect_identity(connection)
                if args.require_tls and not identity.get("ssl_cipher"):
                    raise ManifestError("identity probe connection is not using TLS")
            finally:
                connection.close()
            print(json.dumps(identity, ensure_ascii=False, sort_keys=True))
            return 0

        if args.command == "capture-source":
            _ensure_output_available(args.output, overwrite=args.overwrite)
            timeout = _validate_timeout(args.read_timeout)
            config = load_config(args.config)
            snapshot = validate_snapshot_arguments(
                mode=args.snapshot_mode,
                snapshot_id=args.snapshot_id,
                workers=args.workers,
                ddl_frozen=args.assert_ddl_frozen,
                writes_frozen=args.assert_writes_frozen,
                writes_frozen_at=args.writes_frozen_at,
                restore_artifact_sha256=args.restore_artifact_sha256,
            )
            manifest = capture_source_manifest(
                config=config,
                option_file=args.option_file,
                ssl_ca=args.ssl_ca,
                workers=args.workers,
                read_timeout=timeout,
                snapshot=snapshot,
                checkpoint_path=args.checkpoint,
                resume=args.resume,
            )
            atomic_write_json(args.output, manifest, overwrite=args.overwrite)
            print(
                json.dumps(
                    {
                        "status": "source_manifest_written",
                        "document_sha256": manifest[DOCUMENT_DIGEST_FIELD],
                        "output": str(args.output.expanduser().resolve()),
                    },
                    ensure_ascii=False,
                )
            )
            return 0

        if args.command == "capture-target":
            _ensure_output_available(args.output, overwrite=args.overwrite)
            timeout = _validate_timeout(args.read_timeout)
            config = load_config(args.config)
            source_manifest = load_sealed_json(args.source_manifest)
            manifest = capture_target_manifest(
                config=config,
                option_file=args.option_file,
                ssl_ca=args.ssl_ca,
                workers=args.workers,
                read_timeout=timeout,
                source_manifest=source_manifest,
                target_quiescent=args.assert_target_quiescent,
                restored_artifact_sha256=args.restored_artifact_sha256,
                checkpoint_path=args.checkpoint,
                resume=args.resume,
            )
            atomic_write_json(args.output, manifest, overwrite=args.overwrite)
            print(
                json.dumps(
                    {
                        "status": "target_manifest_written",
                        "document_sha256": manifest[DOCUMENT_DIGEST_FIELD],
                        "output": str(args.output.expanduser().resolve()),
                    },
                    ensure_ascii=False,
                )
            )
            return 0

        if args.command == "compare":
            _ensure_output_available(args.output, overwrite=args.overwrite)
            source_manifest = load_sealed_json(args.source_manifest)
            target_manifest = load_sealed_json(args.target_manifest)
            attestation = None
            if args.allow_reviewed_post_migration_transitions:
                if (
                    args.source_option_file is None
                    or args.target_option_file is None
                    or args.target_ssl_ca is None
                ):
                    raise ManifestError(
                        "reviewed post-migration transitions require both option files and target CA"
                    )
                raw_report = compare_manifests(source_manifest, target_manifest)
                raw_mismatches = _require_list(
                    raw_report.get("mismatches"), "raw comparison mismatches"
                )
                source_connection = _connect(
                    args.source_option_file,
                    ssl_ca=None,
                    require_tls=False,
                    read_timeout=300,
                )
                try:
                    target_connection = _connect(
                        args.target_option_file,
                        ssl_ca=args.target_ssl_ca,
                        require_tls=True,
                        read_timeout=300,
                    )
                    try:
                        attestation = attest_reviewed_post_migration_transitions(
                            source=source_manifest,
                            target=target_manifest,
                            source_connection=source_connection,
                            target_connection=target_connection,
                            raw_mismatches=raw_mismatches,
                        )
                    finally:
                        target_connection.close()
                finally:
                    source_connection.close()
            elif any(
                value is not None
                for value in (
                    args.source_option_file,
                    args.target_option_file,
                    args.target_ssl_ca,
                )
            ):
                raise ManifestError(
                    "live comparison credentials are valid only with the reviewed-transition gate"
                )
            report = compare_manifests(
                source_manifest,
                target_manifest,
                reviewed_post_migration_attestation=attestation,
            )
            atomic_write_json(args.output, report, overwrite=args.overwrite)
            matched = bool(
                _require_object(report.get("result"), "report.result").get(
                    "configured_checks_match"
                )
            )
            print(
                json.dumps(
                    {
                        "status": "match" if matched else "mismatch",
                        "document_sha256": report[DOCUMENT_DIGEST_FIELD],
                        "output": str(args.output.expanduser().resolve()),
                    },
                    ensure_ascii=False,
                )
            )
            return 0 if matched else 1

        raise ManifestError(f"unknown command: {args.command}")
    except (ManifestError, OSError, json.JSONDecodeError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
