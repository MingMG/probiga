"""Explicit schema report; --apply creates ONLY the new progress table.

This is an installation command, not a runtime gate or a business migration
engine. It never alters/drops a business table, index, constraint, or row.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from sqlalchemy import Integer, MetaData, Table, inspect

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from acquisition.config import Config
from acquisition.datasets import get_spec
from acquisition.providers.eastmoney import ALIST_FIELDS, DETAIL_FIELDS, FINANCE_FIELDS
from acquisition.store import STATE, Store, safe_error


# These are field capabilities, not synthetic values. Optional source fields
# remain optional: a particular missing value still fails the actual write.
WRITER_DEFAULT_FIELDS = frozenset({
    "data_source", "qmt_code", "received_at", "etl_sync_at", "batch_id",
    "source_time", "data_version",
})
MARKET_FIELDS = frozenset({
    "trade_time", "trade_date", "snapshot_at", "price", "close", "open", "high", "low",
    "volume", "amount", "change", "change_pct", "pre_close", "avg_price", "short_name",
    "k_type", "adjust_type", "turnover_ratio",
})
FINANCE_REVISION_FIELDS = frozenset({
    "revision_id", "identity_hash", "stock_code", "report_date", "report_type", "source",
    "published_at", "source_published_text", "publication_time_status", "known_at", "received_at",
    "revision_no", "supersedes_revision_id", "batch_id", "content_hash",
    "revision_fingerprint_hash", "payload_json", "created_at",
})


def writer_fields(spec):
    fields = set(WRITER_DEFAULT_FIELDS) | set(spec.key_columns) | {spec.code_column}
    if spec.name == "capital_flow_daily":
        fields.update({"trade_date", "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
                       "lg_net_inflow", "max_net_inflow"})
    elif spec.source == "guojin_qmt":
        fields.update(MARKET_FIELDS)
    elif spec.name == "finance":
        fields.update(FINANCE_FIELDS.values())
    elif spec.name in {"alist_daily", "alist_detail"}:
        fields.update((ALIST_FIELDS if spec.name == "alist_daily" else DETAIL_FIELDS).values())
        fields.add("report_side")
    elif spec.name == "notices":
        fields.update({"stock_code", "art_code", "notice_date", "title", "column_name",
                       "display_time", "association_validated"})
    return fields


def inspect_table(engine, name, expected_key, supplied_fields, *, always_null=(), lookup_key=None,
                  allow_repeated_key=False):
    report = {"table": name, "exists": False, "expected_unique": list(expected_key),
              "actual_unique": [], "required_input_columns": [], "foreign_keys": [],
              "migration_required": []}
    reader = inspect(engine)
    if not reader.has_table(name):
        report["migration_required"].append({"reason": "missing_table", "table": name})
        return report
    report["exists"] = True
    table = Table(name, MetaData(), autoload_with=engine)
    if name == "si_stock_finance" and "source_update_date" not in table.c:
        # The display cache must retain the native source version so an older
        # recovery result cannot replace a newer revision. Report this one
        # concrete migration; never execute business DDL here.
        report["migration_required"].append({
            "reason": "missing_finance_source_update_date", "table": name,
            "column": "source_update_date", "type": "VARCHAR(64)", "nullable": True,
            "suggested_ddl": "ALTER TABLE `si_stock_finance` ADD COLUMN `source_update_date` VARCHAR(64) NULL;",
        })
    unique_columns = [tuple(item["column_names"]) for item in reader.get_unique_constraints(name)]
    indexes = [tuple(item["column_names"]) for item in reader.get_indexes(name)]
    indexes += unique_columns
    shapes = [tuple(item["column_names"]) for item in reader.get_unique_constraints(name)]
    shapes += [tuple(item["column_names"]) for item in reader.get_indexes(name) if item.get("unique")]
    primary = tuple(reader.get_pk_constraint(name).get("constrained_columns") or ())
    if primary:
        shapes.append(primary)
        indexes.append(primary)
    report["actual_unique"] = [list(shape) for shape in sorted(set(shapes))]
    wanted = set(expected_key)
    lookup_key = tuple(lookup_key or expected_key)
    missing = wanted - set(table.c.keys())
    if missing:
        report["migration_required"].append({"reason": "missing_business_key_columns", "columns": sorted(missing)})
    def usable_prefix(columns):
        prefix = tuple(columns[:len(lookup_key)])
        return bool(prefix) and set(prefix).issubset(set(lookup_key))
    if wanted and not any(usable_prefix(columns) for columns in indexes):
        report["migration_required"].append({"reason": "missing_business_identity_index", "columns": list(lookup_key)})
    for shape in shapes:
        if shape and set(shape) < wanted:
            report["migration_required"].append({"reason": "legacy_unique_collapses_business_identity", "columns": list(shape)})
        if allow_repeated_key and set(shape) == wanted:
            report["migration_required"].append({"reason": "source_rows_require_non_unique_partition", "columns": list(shape)})
    for column in table.c:
        generated = bool(column.computed is not None or column.identity is not None or (
            column.primary_key and len(primary) == 1 and isinstance(column.type, Integer)
            and column.autoincrement is not False))
        if column.nullable or column.server_default is not None or generated:
            continue
        supported = column.name in supplied_fields and column.name not in always_null
        report["required_input_columns"].append({"column": column.name, "type": str(column.type),
                                                  "writer_has_field": supported})
        if not supported:
            report["migration_required"].append({"reason": "required_column_not_supplied",
                                                   "column": column.name, "type": str(column.type)})
    for foreign in reader.get_foreign_keys(name):
        columns = list(foreign.get("constrained_columns") or [])
        report["foreign_keys"].append({"columns": columns,
            "referred_table": foreign.get("referred_table"),
            "referred_columns": list(foreign.get("referred_columns") or [])})
        if name == "st_pit_finance_revision" and set(columns) & {"batch_id", "source_coverage_id"}:
            report["migration_required"].append({"reason": "legacy_fact_parent_requires_explicit_migration",
                "columns": columns, "referred_table": foreign.get("referred_table")})
    return report


def inspect_configuration(config, *, apply=False):
    names = config.data.get("datasets", [])
    if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
        raise ValueError("configuration must explicitly enable datasets")
    specs = [get_spec(name) for name in dict.fromkeys(names)]
    report = {"mode": "apply_progress_only" if apply else "check", "business_tables_modified": False,
              "databases": {}, "datasets": [], "migration_required": []}
    engines = {}
    try:
        for database in sorted({spec.database for spec in specs}):
            try:
                engine = config.engine(database)
                engines[database] = engine
                before = inspect(engine).has_table(STATE.name)
                if apply:
                    Store(engine).prepare_progress_schema()
                progress = inspect_table(engine, STATE.name, tuple(c.name for c in STATE.primary_key), set(STATE.c.keys()))
                if progress["exists"]:
                    existing = {column["name"] for column in inspect(engine).get_columns(STATE.name)}
                    missing = set(STATE.c.keys()) - existing
                    if missing:
                        progress["migration_required"].append({"reason": "missing_progress_columns", "columns": sorted(missing)})
                report["databases"][database] = {"progress_table": progress, "created_progress_table": apply and not before}
                report["migration_required"].extend({"database": database, **item} for item in progress["migration_required"])
            except Exception as exc:
                # Do not print URLs, connection strings, SQL or credentials.
                report["databases"][database] = {"error": safe_error(exc)}
                report["migration_required"].append({"database": database, "reason": "database_inspection_failed", "error": safe_error(exc)})
        for spec in specs:
            item = {"dataset": spec.name, "database": spec.database, "table": spec.table}
            if spec.database not in engines or "error" in report["databases"][spec.database]:
                item["status"] = "unavailable"
            elif not spec.table:
                item["status"] = "unsupported"
                report["migration_required"].append({"dataset": spec.name, "reason": "specialized_writer_not_implemented"})
            else:
                try:
                    lookup_key = ((spec.code_column, spec.replace_date_column)
                                  if spec.replace_date_column else spec.key_columns)
                    item["schema"] = inspect_table(
                        engines[spec.database], spec.table, spec.key_columns, writer_fields(spec),
                        lookup_key=lookup_key, allow_repeated_key=spec.name == "alist_detail")
                    issues = list(item["schema"]["migration_required"])
                    if spec.name == "finance":
                        revision = inspect_table(engines[spec.database], "st_pit_finance_revision", ("revision_id",), FINANCE_REVISION_FIELDS,
                                                 always_null={"published_at"})
                        item["revision_schema"] = revision
                        issues.extend({"table": "st_pit_finance_revision", **issue} for issue in revision["migration_required"])
                    item["status"] = "migration_required" if issues else "compatible"
                    report["migration_required"].extend({"dataset": spec.name, "database": spec.database, **issue} for issue in issues)
                except Exception as exc:
                    item.update(status="unavailable", error=safe_error(exc))
                    report["migration_required"].append({"dataset": spec.name, "reason": "table_inspection_failed", "error": safe_error(exc)})
            report["datasets"].append(item)
    finally:
        for engine in engines.values():
            engine.dispose()
    report["status"] = "migration_required" if report["migration_required"] else "compatible"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true", help="Create only acquisition_partition_state, never business tables")
    args = parser.parse_args(argv)
    try:
        report = inspect_configuration(Config.load(args.config), apply=args.apply)
    except Exception as exc:
        report = {"status": "error", "error": safe_error(exc), "business_tables_modified": False}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "compatible" else 2


if __name__ == "__main__":
    raise SystemExit(main())
