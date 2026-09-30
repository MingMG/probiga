from __future__ import annotations

"""Restore a consistent offline snapshot into the private candidate instance.

Passwords never enter command arguments or reports. A failed/interrupted
import is deliberately not rerun over an existing database: its durable
IMPORTING receipt requires explicit administrator recovery.
"""

import argparse
import configparser
import hashlib
import json
import os
import re
import secrets
import socket
import ssl
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .collector import ENDPOINT, initialize_schema


SYSTEM = {"information_schema", "mysql", "performance_schema", "sys"}
WRITER = "probiga_secondary_writer"
READER = "probiga_secondary_reader"
PRIVATE_TABLES = ("secondary_edge_latest_quote", "secondary_edge_history",
                  "secondary_edge_checkpoint", "secondary_edge_summary")
METADATA_FORMAT = "probiga.secondary-edge.database-export.v1"
PACKAGE_FORMAT = "probiga.windows-edge-offline.v1"
STATUS_FORMAT = "probiga.secondary-edge.target-database.v1"


class TargetError(RuntimeError):
    """Only fixed, safe-to-display reason codes leave this module."""


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _query(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def _execute(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)


def _identifier(name):
    if not isinstance(name, str) or not name or "\0" in name:
        raise TargetError("OBJECT_NAME_INVALID")
    return "`" + name.replace("`", "``") + "`"


def _digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _write_atomic(path, value, *, exclusive=False):
    # The installation root already has a protected inheritable Windows ACL.
    # New files inherit it; there is no world-readable temporary directory.
    if path.is_symlink():
        raise TargetError("TARGET_STATE_PATH_UNSAFE")
    if exclusive:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise TargetError("TARGET_TEMPORARY_STATE_ALREADY_EXISTS")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def load_package(package):
    manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8-sig"))
    metadata = json.loads((package / "database/metadata.json").read_text(encoding="utf-8-sig"))
    if (manifest.get("format") != PACKAGE_FORMAT or metadata.get("format") != METADATA_FORMAT
            or metadata.get("status") != "ready" or manifest.get("production_activation") is not False
            or not re.fullmatch(r"[0-9a-f]{40}", str(manifest.get("build_sha") or ""))
            or manifest["build_sha"] == "0" * 40):
        raise TargetError("OFFLINE_PACKAGE_NOT_READY")
    dump = metadata.get("dump") or {}
    if dump.get("file") != "app.sql" or not re.fullmatch(r"[0-9a-f]{64}", str(dump.get("sha256") or "")):
        raise TargetError("DATABASE_DUMP_RECEIPT_INVALID")
    sql_file = package / "database/app.sql"
    if (sql_file.is_symlink() or sql_file.stat().st_size != dump.get("bytes")
            or _digest(sql_file) != dump["sha256"]):
        raise TargetError("DATABASE_DUMP_HASH_MISMATCH")
    names = [row.get("name") for row in metadata.get("schemas", [])]
    if (not names or len(set(names)) != len(names)
            or any(not isinstance(name, str) or name.casefold() in SYSTEM | {ENDPOINT[2]}
                   or not name or "\0" in name for name in names)):
        raise TargetError("BUSINESS_SCHEMA_LIST_INVALID")
    for kind in ("tables", "views", "triggers", "routines", "events"):
        if not isinstance(metadata.get(kind), list):
            raise TargetError("DATABASE_OBJECT_INVENTORY_INCOMPLETE")
        for item in metadata[kind]:
            if item.get("schema") not in names or not isinstance(item.get("name"), str) or not item["name"]:
                raise TargetError("DATABASE_OBJECT_INVENTORY_INVALID")
    source = metadata.get("source") or {}
    if (not source.get("server_uuid") or not source.get("hostname")
            or str(source["hostname"]).casefold() != str(manifest.get("source_host") or "").casefold()):
        raise TargetError("SOURCE_IDENTITY_INCOMPLETE")
    return manifest, metadata, sql_file


def open_target(ca, password):
    import pymysql
    context = ssl.create_default_context(cafile=str(ca))
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return pymysql.connect(host=ENDPOINT[0], port=ENDPOINT[1], user="root", password=password,
                           charset="utf8mb4", autocommit=True, ssl=context,
                           connect_timeout=10, read_timeout=30, write_timeout=30,
                           cursorclass=pymysql.cursors.DictCursor)


def target_state(connection, source, local_host):
    rows = _query(connection, "SELECT @@server_uuid AS server_uuid, @@hostname AS hostname, "
                  "@@version AS version, @@version_comment AS version_comment, @@port AS port, "
                  "@@lower_case_table_names AS lower_case_table_names, "
                  "@@require_secure_transport AS require_secure_transport, "
                  "@@event_scheduler AS event_scheduler, "
                  "@@default_collation_for_utf8mb4 AS default_collation_for_utf8mb4")
    if len(rows) != 1:
        raise TargetError("TARGET_SERVER_IDENTITY_UNAVAILABLE")
    row = rows[0]
    if (int(row.get("port", 0)) != ENDPOINT[1]
            or not re.fullmatch(r"8\.4\.11(?:[-.].*)?", str(row.get("version") or ""))
            or "maria" in str(row.get("version_comment", "")).casefold()
            or int(row.get("lower_case_table_names", -1)) != 1
            or int(row.get("require_secure_transport", 0)) != 1
            or str(row.get("event_scheduler", "")).upper() != "OFF"):
        raise TargetError("TARGET_MYSQL_RUNTIME_CONTRACT_REFUSED")
    if (str(source["hostname"]).casefold() in {str(row.get("hostname", "")).casefold(), local_host.casefold()}
            or str(row.get("server_uuid", "")).casefold() == str(source["server_uuid"]).casefold()
            or not row.get("server_uuid")):
        raise TargetError("SOURCE_COMPUTER_OR_DATABASE_INSTANCE_REFUSED")
    cipher = _query(connection, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
    if not cipher or not cipher[0].get("Value"):
        raise TargetError("TARGET_TLS_REQUIRED")
    return row


def read_root_password(path):
    if path.is_symlink():
        raise TargetError("ROOT_CLIENT_FILE_UNSAFE")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        parser.read_string(path.read_text(encoding="utf-8"))
        options = dict(parser["client"])
        if (parser.sections() != ["client"] or parser.defaults()
                or set(options) != {"user", "password", "host", "port", "protocol", "ssl-mode", "default-character-set"}
                or options["user"] != "root" or options["host"] != ENDPOINT[0]
                or options["port"] != str(ENDPOINT[1]) or options["protocol"] != "TCP"
                or options["ssl-mode"] != "VERIFY_CA" or options["default-character-set"] != "utf8mb4"
                or not re.fullmatch(r"[A-Za-z0-9_-]{64}", options["password"])):
            raise ValueError()
        return options["password"]
    except Exception:
        raise TargetError("ROOT_CLIENT_FILE_INVALID") from None


def root_connection(root, source, local_host, *, connector=open_target):
    client = root / "root-client.ini"
    ca = root / "mysql-data/ca.pem"
    existing = client.exists()
    connection = connector(ca, read_root_password(client) if existing else "")
    try:
        target_state(connection, source, local_host)
        if not existing:
            password = secrets.token_urlsafe(48)
            # Write the future password before ALTER so power loss never loses
            # an already-applied password. A failed rotation blocks safely;
            # it never falls back to blank-root login on a later run.
            _write_atomic(client, "[client]\nuser=root\npassword=" + password
                          + "\nhost=127.0.0.1\nport=33085\nprotocol=TCP\nssl-mode=VERIFY_CA"
                          + "\ndefault-character-set=utf8mb4\n", exclusive=True)
            _execute(connection, "ALTER USER 'root'@'localhost' IDENTIFIED BY %s", (password,))
        return connection
    except BaseException:
        connection.close()
        raise


def business_schemas(connection, *, include_private=False):
    excluded = SYSTEM if include_private else SYSTEM | {ENDPOINT[2]}
    return {row["name"] for row in _query(connection,
            "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA")
            if str(row["name"]).casefold() not in excluded}


def _object_sets(metadata):
    result = {}
    for kind in ("tables", "views", "triggers", "routines", "events"):
        values = []
        for item in metadata[kind]:
            key = (item["schema"].casefold(), item["name"].casefold())
            if kind in {"tables", "routines"}:
                key += (str(item.get("type") or "").upper(),)
            values.append(key)
        if len(set(values)) != len(values):
            raise TargetError("DUPLICATE_DATABASE_OBJECT_INVENTORY")
        result[kind] = set(values)
    return result


def verify_inventory(connection, metadata):
    names = [row["name"] for row in metadata["schemas"]]
    if business_schemas(connection) != set(names):
        raise TargetError("RESTORED_SCHEMA_INVENTORY_MISMATCH")
    expected = _object_sets(metadata)
    fields = {
        "tables": ("TABLES", "TABLE_SCHEMA", "TABLE_NAME", ",TABLE_TYPE AS type"),
        "views": ("VIEWS", "TABLE_SCHEMA", "TABLE_NAME", ""),
        "triggers": ("TRIGGERS", "TRIGGER_SCHEMA", "TRIGGER_NAME", ""),
        "routines": ("ROUTINES", "ROUTINE_SCHEMA", "ROUTINE_NAME", ",ROUTINE_TYPE AS type"),
        "events": ("EVENTS", "EVENT_SCHEMA", "EVENT_NAME", ""),
    }
    restored = {"schemas": len(names)}
    for kind, (table, schema, name, extra) in fields.items():
        placeholders = ",".join(["%s"] * len(names))
        rows = _query(connection, f"SELECT {schema} AS `schema`,{name} AS name{extra} "
                      f"FROM information_schema.{table} WHERE {schema} IN ({placeholders})", tuple(names))
        actual = {(row["schema"].casefold(), row["name"].casefold()) +
                  ((str(row.get("type") or "").upper(),) if kind in {"tables", "routines"} else ())
                  for row in rows}
        if len(rows) != len(expected[kind]) or actual != expected[kind]:
            raise TargetError("RESTORED_" + kind.upper() + "_INVENTORY_MISMATCH")
        restored[kind] = len(rows)
    return restored


def prepare_definers(connection, metadata):
    """Keep exact DEFINER identities, without any source password or DML grants.

    Stored objects are preserved, not exercised here. Candidate views/read-only
    routines receive SELECT/EXECUTE; events remain globally OFF and the worker
    never mutates cloned business tables or calls their stored procedures.
    """
    accounts = set()
    for kind in ("views", "triggers", "routines", "events"):
        for item in metadata[kind]:
            raw = str(item.get("definer") or "")
            user, separator, host = raw.rpartition("@")
            if (not separator or not user or not host or "\0" in raw
                    or user in {WRITER, READER} or user.startswith("mysql.")):
                raise TargetError("SOURCE_DEFINER_IDENTITY_NOT_REPRESENTABLE")
            accounts.add((user, host))
    result = []
    for user, host in sorted(accounts):
        if (user, host) == ("root", "localhost"):
            # This is the fresh candidate administrator, never the exported
            # source credential. It must remain available for controlled restore.
            result.append({"user": user, "host": host, "state": "CANDIDATE_ADMINISTRATOR"})
            continue
        _execute(connection, "CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s ACCOUNT LOCK",
                 (user, host, secrets.token_urlsafe(48)))
        _execute(connection, "ALTER USER %s@%s ACCOUNT LOCK", (user, host))
        for schema in metadata["schemas"]:
            _execute(connection, "GRANT SELECT, EXECUTE ON " + _identifier(schema["name"]) + ".* TO %s@%s",
                     (user, host))
        result.append({"user": user, "host": host, "state": "LOCKED_READ_ONLY_DEFINER"})
    return result


def import_sql(root, sql_file, *, runner=subprocess.run):
    command = [str(root / "mysql84/bin/mysql.exe"),
               f"--defaults-file={root / 'root-client.ini'}", "--no-login-paths",
               "--host=127.0.0.1", "--port=33085", "--protocol=TCP", "--ssl-mode=VERIFY_CA",
               f"--ssl-ca={root / 'mysql-data/ca.pem'}", "--default-character-set=utf8mb4",
               "--connect-timeout=10", "--max-allowed-packet=256M", "--binary-mode"]
    environment = dict(os.environ)
    for key in ("MYSQL_PWD", "MYSQL_HOST", "MYSQL_TCP_PORT", "MYSQL_UNIX_PORT", "MYSQL_HOME", "MYSQL_TEST_LOGIN_FILE"):
        environment.pop(key, None)
    with sql_file.open("rb") as handle:
        result = runner(command, stdin=handle, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        shell=False, env=environment,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if result.returncode != 0:
        raise TargetError("DATABASE_RESTORE_FAILED_PARTIAL_IMPORT_REQUIRES_ADMINISTRATOR_RECOVERY")


def _function_restore_state(connection):
    rows = _query(connection, "SELECT @@global.log_bin AS log_bin, "
                  "@@global.log_bin_trust_function_creators AS trust_creators")
    if len(rows) != 1 or int(rows[0].get("log_bin", -1)) != 1:
        raise TargetError("TARGET_BINARY_LOGGING_CONTRACT_REFUSED")
    trust = int(rows[0].get("trust_creators", -1))
    if trust not in (0, 1):
        raise TargetError("TARGET_STORED_FUNCTION_POLICY_UNAVAILABLE")
    return trust


def verify_stored_function_policy(connection):
    if _function_restore_state(connection) != 0:
        raise TargetError("TARGET_STORED_FUNCTION_MAINTENANCE_NOT_CLOSED")


def _assert_restore_instance(connection, checked_state, source, local_host, status_file):
    try:
        fence = json.loads(status_file.read_text(encoding="utf-8"))
    except Exception:
        raise TargetError("RESTORE_MAINTENANCE_FENCE_UNAVAILABLE") from None
    if (fence.get("format") != STATUS_FORMAT or fence.get("status") != "IMPORTING"
            or fence.get("production_active") is not False
            or fence.get("target_uuid") != checked_state.get("server_uuid")):
        raise TargetError("RESTORE_MAINTENANCE_FENCE_IDENTITY_MISMATCH")
    actual = target_state(connection, source, local_host)
    if (actual["server_uuid"] != checked_state.get("server_uuid")
            or actual["hostname"] != checked_state.get("hostname")):
        raise TargetError("RESTORE_MAINTENANCE_INSTANCE_CHANGED")


@contextmanager
def stored_function_restore_window(connection, checked_state, source, local_host, status_file):
    """Preserve dump declarations in a fenced, private-instance restore window.

    This is normal MySQL restore maintenance, not a runtime permission or a
    modified routine definition. The setting is never persisted. Successful
    completion requires an explicit OFF and verification on the same gated
    instance. A crash retains IMPORTING and blocks automatic reruns; restarting
    this candidate mysqld clears the non-persisted setting before admin recovery.
    """
    _assert_restore_instance(connection, checked_state, source, local_host, status_file)
    verify_stored_function_policy(connection)
    try:
        # Include the setter in the try: the server may apply ON even if its
        # response is lost, so cleanup must be attempted after that failure too.
        _execute(connection, "SET GLOBAL log_bin_trust_function_creators=ON")
        if _function_restore_state(connection) != 1:
            raise TargetError("TARGET_STORED_FUNCTION_MAINTENANCE_NOT_OPEN")
        yield
    except Exception as exc:
        if isinstance(exc, TargetError):
            raise
        raise TargetError("STORED_FUNCTION_RESTORE_MAINTENANCE_FAILED") from None
    finally:
        try:
            # Never reset a replacement/source server. A broken connection or
            # changed identity is blocked, and the durable fence remains intact.
            _assert_restore_instance(connection, checked_state, source, local_host, status_file)
            _execute(connection, "SET GLOBAL log_bin_trust_function_creators=OFF")
            verify_stored_function_policy(connection)
            _assert_restore_instance(connection, checked_state, source, local_host, status_file)
        except Exception:
            raise TargetError("STORED_FUNCTION_RESTORE_MAINTENANCE_CLEANUP_FAILED_REQUIRES_ADMINISTRATOR_RECOVERY") from None


def chrome_executable():
    roots = [os.environ.get("ProgramFiles", r"C:\Program Files"),
             os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")]
    for base in roots:
        path = Path(base) / "Google/Chrome/Application/chrome.exe"
        if path.is_file():
            return path
    raise TargetError("CHROME_INSTALLATION_NOT_FOUND")


def create_private_runtime(connection, root, manifest, metadata, *, chrome_finder=chrome_executable):
    config_file = root / "config.json"
    if config_file.exists():
        raise TargetError("UNOWNED_RUNTIME_CONFIG_ALREADY_EXISTS")
    _execute(connection, "CREATE DATABASE " + _identifier(ENDPOINT[2])
             + " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    _execute(connection, "USE " + _identifier(ENDPOINT[2]))
    initialize_schema(connection)
    writer_password, reader_password = secrets.token_urlsafe(48), secrets.token_urlsafe(48)
    for user, password in ((WRITER, writer_password), (READER, reader_password)):
        _execute(connection, "CREATE USER %s@'127.0.0.1' IDENTIFIED BY %s REQUIRE SSL", (user, password))
    for table in PRIVATE_TABLES:
        _execute(connection, "GRANT SELECT, INSERT, UPDATE ON " + _identifier(ENDPOINT[2]) + "."
                 + _identifier(table) + " TO %s@'127.0.0.1'", (WRITER,))
    for schema in metadata["schemas"]:
        _execute(connection, "GRANT SELECT ON " + _identifier(schema["name"]) + ".* TO %s@'127.0.0.1'", (READER,))
    connection.commit()
    ca = str(root / "mysql-data/ca.pem")
    config = {
        "mysql": {"host": ENDPOINT[0], "port": ENDPOINT[1], "database": ENDPOINT[2],
                  "user": WRITER, "password": writer_password, "ssl_ca": ca},
        "qmt_home": str(root / "qmt"), "expected_build_sha": manifest["build_sha"],
        "sample_size": 50, "poll_seconds": 60, "history_days": 5,
        "ai": {"codex_exe": str(root / "codex/codex.exe"), "chrome_exe": str(chrome_finder()),
               "profile_dir": str(root / "ai/deepseek-profile"), "server_url": manifest.get("ai_server_url")},
    }
    clone_names = {row["name"] for row in metadata["schemas"]}
    source_schema = "probiga" if "probiga" in clone_names else "probiga_qmt_history" if "probiga_qmt_history" in clone_names else None
    if source_schema:
        config["source_db"] = {"host": ENDPOINT[0], "port": ENDPOINT[1], "database": source_schema,
                               "user": READER, "password": reader_password, "ssl_ca": ca}
    _write_atomic(config_file, _json(config) + "\n", exclusive=True)
    return config


def verify_runtime_config(root, manifest):
    config = json.loads((root / "config.json").read_text(encoding="utf-8"))
    db = config.get("mysql") or {}
    if ((db.get("host"), db.get("port"), db.get("database")) != ENDPOINT
            or db.get("user") != WRITER or not db.get("password")
            or config.get("expected_build_sha") != manifest["build_sha"]):
        raise TargetError("RESTORED_RUNTIME_CONFIG_IDENTITY_MISMATCH")


def verify_runtime_grants(connection, metadata):
    """Reject extra grants/default roles rather than trust a previous receipt."""
    expected = {
        WRITER: {f"{ENDPOINT[2]}.{table}".upper(): {"SELECT", "INSERT", "UPDATE"}
                 for table in PRIVATE_TABLES},
        READER: {f"{schema['name']}.*".upper(): {"SELECT"} for schema in metadata["schemas"]},
    }
    for user, scopes in expected.items():
        rows = _query(connection, "SHOW GRANTS FOR %s@'127.0.0.1'", (user,))
        observed = {}
        for row in rows:
            raw = str(next(iter(row.values())))
            if "GRANT OPTION" in raw.upper():
                raise TargetError("RUNTIME_ACCOUNT_PRIVILEGES_EXCEED_PRIVATE_SCOPE")
            match = re.match(r"^GRANT (.+?) ON (.+?) TO ", raw, re.I)
            if not match:
                raise TargetError("RUNTIME_ACCOUNT_ROLE_OR_GRANT_REFUSED")
            scope = match[2].replace("`", "").upper()
            privileges = {part.strip().upper() for part in match[1].split(",")}
            if scope in observed:
                raise TargetError("RUNTIME_ACCOUNT_DUPLICATE_GRANT_REFUSED")
            observed[scope] = privileges
        if observed != {"*.*": {"USAGE"}, **scopes}:
            raise TargetError("RUNTIME_ACCOUNT_PRIVILEGES_EXCEED_PRIVATE_SCOPE")
        account_rows = _query(connection, "SHOW CREATE USER %s@'127.0.0.1'", (user,))
        account = str(next(reversed(account_rows[0].values()))) if len(account_rows) == 1 else ""
        normalized = " ".join(account.upper().split())
        if " REQUIRE SSL" not in normalized or "ACCOUNT LOCK" in normalized:
            raise TargetError("RUNTIME_ACCOUNT_TLS_OR_LOGIN_CONTRACT_REFUSED")


def restore_target(root, package, *, connector=open_target, runner=subprocess.run,
                   local_host=None, chrome_finder=chrome_executable):
    root, package = root.absolute(), package.absolute()
    if root.is_symlink() or package.is_symlink() or not root.is_dir():
        raise TargetError("TARGET_ROOT_UNSAFE")
    manifest, metadata, sql_file = load_package(package)
    local_host = local_host or os.environ.get("COMPUTERNAME") or socket.gethostname()
    if local_host.casefold() == manifest["source_host"].casefold():
        raise TargetError("SOURCE_COMPUTER_OR_DATABASE_INSTANCE_REFUSED")
    status_file = root / "database-status.json"
    prior = json.loads(status_file.read_text(encoding="utf-8")) if status_file.exists() else None
    if prior and (prior.get("status") != "ready" or prior.get("format") != STATUS_FORMAT):
        raise TargetError("PARTIAL_RESTORE_REQUIRES_ADMINISTRATOR_RECOVERY")
    connection = root_connection(root, metadata["source"], local_host, connector=connector)
    try:
        state = target_state(connection, metadata["source"], local_host)
        if prior:
            if (prior.get("dump_sha256") != metadata["dump"]["sha256"]
                    or prior.get("build_sha") != manifest["build_sha"]
                    or prior.get("target_uuid") != state["server_uuid"]):
                raise TargetError("RESTORE_RECEIPT_IDENTITY_MISMATCH")
            if state.get("default_collation_for_utf8mb4") != "utf8mb4_general_ci":
                raise TargetError("PERSISTED_UTF8MB4_COLLATION_MISMATCH")
            verify_stored_function_policy(connection)
            verify_inventory(connection, metadata)
            verify_runtime_config(root, manifest)
            verify_runtime_grants(connection, metadata)
            return {**prior, "idempotent": True}
        if business_schemas(connection, include_private=True) or (root / "config.json").exists():
            raise TargetError("EXISTING_DATABASE_RESTORE_REFUSED")
        # Write a crash-durable fence before creating users or running SQL.
        _write_atomic(status_file, _json({"format": STATUS_FORMAT, "status": "IMPORTING",
                      "started_at": _now(), "target_uuid": state["server_uuid"],
                      "dump_sha256": metadata["dump"]["sha256"], "production_active": False}))
        _execute(connection, "SET PERSIST default_collation_for_utf8mb4='utf8mb4_general_ci'")
        definers = prepare_definers(connection, metadata)
        with stored_function_restore_window(connection, state, metadata["source"], local_host, status_file):
            import_sql(root, sql_file, runner=runner)
        counts = verify_inventory(connection, metadata)
        state = target_state(connection, metadata["source"], local_host)
        if state.get("default_collation_for_utf8mb4") != "utf8mb4_general_ci":
            raise TargetError("PERSISTED_UTF8MB4_COLLATION_MISMATCH")
        verify_stored_function_policy(connection)
        create_private_runtime(connection, root, manifest, metadata, chrome_finder=chrome_finder)
        verify_runtime_grants(connection, metadata)
        receipt = {"format": STATUS_FORMAT, "status": "ready", "restored_at": _now(),
                   "candidate_only": True, "production_active": False, "target_uuid": state["server_uuid"],
                   "source_uuid": metadata["source"]["server_uuid"], "build_sha": manifest["build_sha"],
                   "dump_sha256": metadata["dump"]["sha256"], "objects": counts, "definers": definers,
                   "event_scheduler": "OFF", "source_accounts_imported": False,
                   "log_bin_trust_function_creators": "OFF",
                   "stored_objects_functional_validation": "REQUIRES_CONTROLLED_PROMOTION_VALIDATION"}
        _write_atomic(status_file, _json(receipt) + "\n")
        return receipt
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        receipt = restore_target(args.root, args.package)
    except TargetError as exc:
        print(_json({"status": "blocked", "reason": str(exc)}))
        return 2
    except Exception as exc:
        print(_json({"status": "blocked", "reason": "TARGET_BOOTSTRAP_FAILED", "error_type": type(exc).__name__}))
        return 2
    print(_json({"status": "ready", "candidate_only": True, "production_active": False,
                 "objects": receipt["objects"], "idempotent": receipt.get("idempotent", False)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
