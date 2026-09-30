from __future__ import annotations

"""Make an online, all-business-schema MySQL 8.4 logical snapshot.

The source keeps serving DML. A dedicated TLS connection holds the backup
lock until mysqldump's single transaction has finished, and is pinged without
reconnection. System accounts, authentication hashes and system schemas are
intentionally not exported: a restored candidate has its own UUID and users.
An administrator option file is supplied outside the package and never copied.
"""

import argparse
import configparser
import hashlib
import json
import os
import re
import ssl
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


FORMAT = "probiga.secondary-edge.database-export.v1"
SYSTEM_SCHEMAS = ("information_schema", "mysql", "performance_schema", "sys")
GLOBAL_STATIC = frozenset({"SELECT", "SHOW VIEW", "TRIGGER", "EVENT"})
SOURCE_HOST = "127.0.0.1"
SOURCE_PORT = 3306
HEARTBEAT_SECONDS = 30
OPTION_KEYS = frozenset({"user", "password", "host", "port", "protocol",
                         "ssl-ca", "ssl-mode", "default-character-set",
                         "connect-timeout"})


class ExportError(RuntimeError):
    """A safe-to-display message, never an underlying driver error."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _option_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    escapes = {"b": "\b", "t": "\t", "n": "\n", "r": "\r",
               "\\": "\\", "'": "'", '"': '"', "s": " "}
    return re.sub(r"\\([btnr\\'\"s])", lambda match: escapes[match[1]], value)


def read_client_options(path: Path) -> dict[str, str]:
    """Accept one simple MySQL client file, not includes or dump modifiers."""
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            parser.read_file(handle)
    except Exception:
        raise ExportError("CLIENT_OPTION_FILE_INVALID: supply a readable UTF-8 [client] file") from None
    if parser.sections() != ["client"] or parser.defaults():
        raise ExportError("CLIENT_OPTION_FILE_INVALID: only [client] is allowed")
    raw = dict(parser.items("client"))
    if set(raw) - OPTION_KEYS:
        raise ExportError("CLIENT_OPTION_FILE_INVALID: includes and unsupported options are not allowed")
    options = {key: _option_value(value) for key, value in raw.items()}
    if not options.get("user") or not options.get("password"):
        raise ExportError("CLIENT_OPTION_FILE_INVALID: user and password are required")
    if options.get("host", SOURCE_HOST) != SOURCE_HOST or options.get("port", "3306") != "3306":
        raise ExportError("SOURCE_ENDPOINT_REFUSED: only 127.0.0.1:3306 is supported")
    if options.get("protocol", "tcp").lower() != "tcp":
        raise ExportError("SOURCE_ENDPOINT_REFUSED: TCP is required")
    if options.get("ssl-mode", "VERIFY_CA").upper() not in {"VERIFY_CA", "VERIFY_IDENTITY"}:
        raise ExportError("SOURCE_TLS_REFUSED: option file must not disable CA verification")
    if options.get("default-character-set", "utf8mb4").lower() != "utf8mb4":
        raise ExportError("SOURCE_CHARSET_REFUSED: utf8mb4 is required")
    return options


def open_source(options: dict[str, str], ca_file: Path):
    import pymysql
    context = ssl.create_default_context(cafile=str(ca_file))
    # The existing source certificate verifies against the CA, not the loopback
    # address. Do not weaken certificate validation to ssl-mode=REQUIRED.
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return pymysql.connect(host=SOURCE_HOST, port=SOURCE_PORT,
                           user=options["user"], password=options["password"],
                           charset="utf8mb4", autocommit=True, ssl=context,
                           connect_timeout=10, read_timeout=15, write_timeout=15,
                           cursorclass=pymysql.cursors.DictCursor)


def _rows(connection, query: str, arguments=None) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(query, arguments)
        return list(cursor.fetchall())


def _global_privileges(grants: list[str]) -> set[str]:
    privileges: set[str] = set()
    for grant in grants:
        match = re.match(r"^GRANT (.*?) ON \*\.\* TO ", grant, re.I)
        if not match:
            continue
        items = {item.strip().upper() for item in match[1].split(",")}
        if "ALL PRIVILEGES" in items or "ALL" in items:
            items |= GLOBAL_STATIC
        privileges |= items
    return privileges


def preflight_source(connection) -> dict[str, Any]:
    row = _rows(connection, "SELECT @@server_uuid AS server_uuid, @@hostname AS hostname, "
                "@@version AS version, @@port AS port, @@version_comment AS version_comment")[0]
    if (not re.match(r"^8\.4\.\d+(?:[-.]|$)", str(row.get("version", "")))
            or not str(row.get("version_comment", "")).lower().startswith("mysql")
            or int(row.get("port", 0)) != SOURCE_PORT):
        raise ExportError("SOURCE_VERSION_REFUSED: Oracle MySQL 8.4 on port 3306 is required")
    cipher = _rows(connection, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
    if not cipher or not next(iter(cipher[0].values())) or not cipher[0].get("Value"):
        raise ExportError("SOURCE_TLS_REFUSED: TLS is not active")
    grants = _rows(connection, "SHOW GRANTS")
    privileges = _global_privileges([str(next(iter(item.values()))) for item in grants])
    # Full global SELECT makes every business schema and every routine visible.
    # Schema-scoped application grants cannot prove that unseen objects do not
    # exist. Roles alone are conservatively rejected rather than silently omitted.
    if not GLOBAL_STATIC.issubset(privileges) or "BACKUP_ADMIN" not in privileges:
        raise ExportError("BACKUP_PRIVILEGES_REQUIRED: direct global SELECT, SHOW VIEW, "
                          "TRIGGER, EVENT and BACKUP_ADMIN are required; runtime credentials are insufficient")
    return {key: row[key] for key in ("server_uuid", "hostname", "version", "port")}


def _business_rows(connection, table: str, columns: str, schema_column: str,
                   ordering: str) -> list[dict[str, Any]]:
    placeholders = ",".join(["%s"] * len(SYSTEM_SCHEMAS))
    return _rows(connection, f"SELECT {columns} FROM information_schema.{table} "
                 f"WHERE {schema_column} NOT IN ({placeholders}) ORDER BY {ordering}", SYSTEM_SCHEMAS)


def inventory_source(connection) -> tuple[dict[str, Any], str]:
    schemas = _business_rows(connection, "SCHEMATA",
                             "SCHEMA_NAME AS name, DEFAULT_CHARACTER_SET_NAME AS charset, "
                             "DEFAULT_COLLATION_NAME AS collation", "SCHEMA_NAME", "SCHEMA_NAME")
    if not schemas:
        raise ExportError("BUSINESS_SCHEMA_MISSING: no non-system schemas were discovered")
    tables = _business_rows(connection, "TABLES",
                            "TABLE_SCHEMA AS `schema`, TABLE_NAME AS name, TABLE_TYPE AS type, "
                            "ENGINE AS engine, COALESCE(DATA_LENGTH,0)+COALESCE(INDEX_LENGTH,0) "
                            "AS estimated_bytes, TABLE_ROWS AS estimated_rows", "TABLE_SCHEMA",
                            "TABLE_SCHEMA,TABLE_NAME")
    for table in tables:
        table["estimated_bytes"] = int(table["estimated_bytes"] or 0)
        if table["estimated_rows"] is not None:
            table["estimated_rows"] = int(table["estimated_rows"])
    if any(table["type"] != "VIEW" and str(table.get("engine", "")).upper() != "INNODB"
           for table in tables):
        raise ExportError("NON_TRANSACTIONAL_TABLE_REFUSED: every business base table must be InnoDB")
    definitions: dict[str, list[dict[str, Any]]] = {}
    definitions["views"] = _business_rows(connection, "VIEWS",
        "TABLE_SCHEMA AS `schema`, TABLE_NAME AS name, DEFINER AS definer, "
        "VIEW_DEFINITION AS definition, CHECK_OPTION AS check_option, SECURITY_TYPE AS security_type",
        "TABLE_SCHEMA", "TABLE_SCHEMA,TABLE_NAME")
    definitions["triggers"] = _business_rows(connection, "TRIGGERS",
        "TRIGGER_SCHEMA AS `schema`, TRIGGER_NAME AS name, DEFINER AS definer, "
        "ACTION_STATEMENT AS definition, ACTION_TIMING AS timing, EVENT_MANIPULATION AS event, "
        "EVENT_OBJECT_SCHEMA AS table_schema, EVENT_OBJECT_TABLE AS table_name, "
        "ACTION_ORDER AS action_order, SQL_MODE AS sql_mode", "TRIGGER_SCHEMA",
        "TRIGGER_SCHEMA,TRIGGER_NAME")
    definitions["routines"] = _business_rows(connection, "ROUTINES",
        "ROUTINE_SCHEMA AS `schema`, ROUTINE_NAME AS name, ROUTINE_TYPE AS type, DEFINER AS definer, "
        "ROUTINE_DEFINITION AS definition, DTD_IDENTIFIER AS return_type, SQL_MODE AS sql_mode, "
        "SECURITY_TYPE AS security_type, IS_DETERMINISTIC AS deterministic, "
        "SQL_DATA_ACCESS AS data_access, ROUTINE_COMMENT AS comment", "ROUTINE_SCHEMA",
        "ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_TYPE")
    definitions["events"] = _business_rows(connection, "EVENTS",
        "EVENT_SCHEMA AS `schema`, EVENT_NAME AS name, DEFINER AS definer, EVENT_DEFINITION AS definition, "
        "EVENT_TYPE AS type, EXECUTE_AT AS execute_at, INTERVAL_VALUE AS interval_value, "
        "INTERVAL_FIELD AS interval_field, STARTS AS starts, ENDS AS ends, "
        "STATUS AS status, ON_COMPLETION AS on_completion, SQL_MODE AS sql_mode, TIME_ZONE AS time_zone",
        "EVENT_SCHEMA", "EVENT_SCHEMA,EVENT_NAME")
    if any(item.get("definition") is None for rows in definitions.values() for item in rows):
        raise ExportError("METADATA_INCOMPLETE: stored object definitions are not all visible")
    shape = {
        "schemas": schemas,
        "tables": [{key: value for key, value in table.items()
                    if key not in {"estimated_bytes", "estimated_rows"}} for table in tables],
        **definitions,
        "columns": _business_rows(connection, "COLUMNS",
            "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, "
            "COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_KEY, EXTRA, GENERATION_EXPRESSION",
            "TABLE_SCHEMA", "TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION"),
        "indexes": _business_rows(connection, "STATISTICS",
            "TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, "
            "COLLATION, SUB_PART, INDEX_TYPE, IS_VISIBLE, EXPRESSION", "TABLE_SCHEMA",
            "TABLE_SCHEMA,TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX"),
        "parameters": _business_rows(connection, "PARAMETERS",
            "SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, "
            "DTD_IDENTIFIER, CHARACTER_SET_NAME, COLLATION_NAME", "SPECIFIC_SCHEMA",
            "SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION"),
        "foreign_keys": _business_rows(connection, "KEY_COLUMN_USAGE",
            "TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, "
            "REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME",
            "TABLE_SCHEMA", "TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION"),
        "reference_rules": _business_rows(connection, "REFERENTIAL_CONSTRAINTS",
            "CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_NAME, UNIQUE_CONSTRAINT_SCHEMA, "
            "UNIQUE_CONSTRAINT_NAME, MATCH_OPTION, UPDATE_RULE, DELETE_RULE, REFERENCED_TABLE_NAME",
            "CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA,CONSTRAINT_NAME"),
        "checks": _business_rows(connection, "CHECK_CONSTRAINTS",
            "CONSTRAINT_SCHEMA, CONSTRAINT_NAME, CHECK_CLAUSE", "CONSTRAINT_SCHEMA",
            "CONSTRAINT_SCHEMA,CONSTRAINT_NAME"),
    }
    metadata = {"schemas": [
        {"name": schema["name"], "estimated_bytes": sum(
            int(table["estimated_bytes"] or 0) for table in tables if table["schema"] == schema["name"])}
        for schema in schemas], "tables": tables}
    metadata.update({kind: [{key: value for key, value in item.items()
                             if key != "definition" and key != "comment"}
                            for item in rows] for kind, rows in definitions.items()})
    return metadata, hashlib.sha256(_json(shape).encode("utf-8")).hexdigest()


def build_dump_command(executable: Path, option_file: Path, ca_file: Path,
                       partial: Path, schemas: list[dict[str, Any]]) -> list[str]:
    names = [item["name"] for item in schemas]
    if not names or any(not isinstance(name, str) or not name or name.startswith("-")
                        or "\0" in name or name.lower() in SYSTEM_SCHEMAS for name in names):
        raise ExportError("SCHEMA_NAME_REFUSED: schema cannot be safely passed to mysqldump")
    return [str(executable), f"--defaults-file={option_file}", "--no-login-paths",
            "--host=127.0.0.1", "--port=3306", "--protocol=TCP", "--ssl-mode=VERIFY_CA",
            f"--ssl-ca={ca_file}", "--default-character-set=utf8mb4", "--connect-timeout=10",
            "--max-allowed-packet=256M",
            "--single-transaction", "--quick", "--no-tablespaces", "--set-gtid-purged=OFF",
            "--routines", "--triggers", "--events", "--hex-blob", "--column-statistics=0",
            "--skip-lock-tables", f"--result-file={partial}", "--databases", *names]


def _stop_dump(process) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def monitor_dump(process, connection, *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep,
                 heartbeat_seconds: float = HEARTBEAT_SECONDS) -> None:
    last_ping = clock()
    while process.poll() is None:
        if clock() - last_ping >= heartbeat_seconds:
            try:
                connection.ping(reconnect=False)
            except Exception:
                _stop_dump(process)
                raise ExportError("BACKUP_LOCK_CONNECTION_LOST: dump stopped; no ready backup was published") from None
            last_ping = clock()
        sleep(0.5)
    if process.returncode != 0:
        raise ExportError(f"MYSQLDUMP_FAILED: exit code {process.returncode}; partial dump is not usable")
    try:
        connection.ping(reconnect=False)
    except Exception:
        raise ExportError("BACKUP_LOCK_CONNECTION_LOST: completed dump was rejected") from None


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".part")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(_json(metadata) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    # Rename without replace: another invocation must never lose an older backup.
    temporary.rename(path)


def export_database(option_file: Path, executable: Path, ca_file: Path, output_dir: Path,
                    *, connector: Callable = open_source, process_factory: Callable = subprocess.Popen,
                    monitor: Callable = monitor_dump) -> dict[str, Any]:
    option_file, executable, ca_file = (path.resolve(strict=True)
                                       for path in (option_file, executable, ca_file))
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    final = output_dir / "app.sql"
    partial = output_dir / "app.sql.part"
    metadata_path = output_dir / "metadata.json"
    if any(path.exists() for path in (final, partial, metadata_path,
                                      metadata_path.with_suffix(".json.part"))):
        raise ExportError("OUTPUT_EXISTS: preserve or move the existing export before retrying")
    options = read_client_options(option_file)
    if "ssl-ca" in options and Path(options["ssl-ca"]).resolve() != ca_file:
        raise ExportError("SOURCE_TLS_REFUSED: option-file CA and selected CA disagree")
    # Reserve before opening the source so concurrent exports cannot share an
    # output. A failed export deliberately leaves the .part file for inspection.
    with partial.open("xb"):
        pass
    connection = None
    process = None
    locked = False
    metadata: dict[str, Any] = {"format": FORMAT, "status": "ready", "started_at": utc_now(),
                                "excluded_system_schemas": list(SYSTEM_SCHEMAS), "snapshot_window": {}}
    try:
        connection = connector(options, ca_file)
        metadata["source"] = preflight_source(connection)
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION lock_wait_timeout=10")
            cursor.execute("LOCK INSTANCE FOR BACKUP")
        locked = True
        metadata["snapshot_window"]["lock_acquired_at"] = utc_now()
        inventory, fingerprint = inventory_source(connection)
        metadata.update(inventory)
        command = build_dump_command(executable, option_file, ca_file, partial, inventory["schemas"])
        child_env = dict(os.environ)
        for key in ("MYSQL_PWD", "MYSQL_HOST", "MYSQL_TCP_PORT", "MYSQL_UNIX_PORT",
                    "MYSQL_HOME", "MYSQL_TEST_LOGIN_FILE"):
            child_env.pop(key, None)
        metadata["snapshot_window"]["dump_started_at"] = utc_now()
        process = process_factory(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                  shell=False, env=child_env,
                                  creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        monitor(process, connection)
        metadata["snapshot_window"]["dump_finished_at"] = utc_now()
        _, final_fingerprint = inventory_source(connection)
        if fingerprint != final_fingerprint:
            raise ExportError("METADATA_CHANGED: stored objects changed during export; backup rejected")
        if partial.stat().st_size == 0:
            raise ExportError("MYSQLDUMP_EMPTY: backup rejected")
        with connection.cursor() as cursor:
            cursor.execute("UNLOCK INSTANCE")
        locked = False
        metadata["snapshot_window"]["lock_released_at"] = utc_now()
    except ExportError:
        raise
    except KeyboardInterrupt:
        raise ExportError("EXPORT_INTERRUPTED: partial dump is not usable") from None
    except Exception:
        # Driver/process errors may embed option values, SQL contents or a
        # password. The caller gets only this redacted diagnostic.
        raise ExportError("EXPORT_FAILED: verify admin credentials, TLS, free space and MySQL 8.4 tools") from None
    finally:
        _stop_dump(process)
        if connection is not None:
            if locked:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("UNLOCK INSTANCE")
                except Exception:
                    pass  # close also releases a surviving session's backup lock
            connection.close()
    metadata["dump"] = {"file": final.name, "bytes": partial.stat().st_size,
                        "sha256": file_digest(partial)}
    partial.rename(final)
    _write_metadata(metadata_path, metadata)
    return metadata


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-option-file", type=Path, required=True)
    parser.add_argument("--mysqldump", type=Path, required=True)
    parser.add_argument("--ssl-ca", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        metadata = export_database(args.client_option_file, args.mysqldump, args.ssl_ca, args.output_dir)
    except ExportError as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception:
        print("EXPORT_FAILED: no ready backup was published", file=sys.stderr)
        return 1
    print(_json({"status": "ready", "format": FORMAT, "schema_count": len(metadata["schemas"]),
                 "dump_bytes": metadata["dump"]["bytes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
