from __future__ import annotations

"""Complete stopped-instance migration; no database/service start or mutation.

The orchestrator owns shutdown and all controlled mysqld lifecycles. This
module copies every data/log/certificate file, produces an official init-file
for the owned target, and performs read-only identity/transport verification.
Neither successful installation nor verification authorizes production resume.
"""

import argparse
import configparser
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import stat
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath


LAYOUT_FORMAT = "probiga.cold-source-layout.v1"
PAUSE_FORMAT = "probiga.source-pause.v1"
SNAPSHOT_FORMAT = "probiga.cold-database-snapshot.v1"
STATUS_FORMAT = "probiga.cold-database-target.v1"
SERVICE = "ProBigA-MySQL84"
PORT = 3306
MEMORY_NAME = "ProBigA-Cold-Admin"
UUID_PATTERN = r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}"
TARGET_DIRS = {"data": "mysql-data", "logs": "mysql-logs", "config": "mysql-source-config", "certs": "mysql-certs"}
BUFFER_POOL = 4 * 1024 ** 3
REDO_CAPACITY = 2 * 1024 ** 3
PERSISTED_OPTIONS = {"default_collation_for_utf8mb4", "innodb_buffer_pool_size", "innodb_redo_log_capacity",
                     "event_scheduler", "require_secure_transport", "log_bin_trust_function_creators",
                     "max_connections", "sql_mode", "time_zone", "autocommit", "transaction_isolation",
                     "sync_binlog", "innodb_flush_log_at_trx_commit", "binlog_expire_logs_seconds",
                     "binlog_format", "binlog_row_image", "general_log", "slow_query_log",
                     "general_log_file", "slow_query_log_file"}
SYSTEM_SCHEMAS = {"information_schema", "performance_schema", "mysql", "sys"}
# Names beginning with mysql. are not intrinsically system accounts. Only
# Oracle's three fixed localhost identities and the two new local administrators
# may be excluded from the source-account lock and verification predicate.
NON_ADMIN_ACCOUNT_PREDICATE = (
    "NOT(User='root' AND Host IN ('localhost','127.0.0.1')) "
    "AND NOT(Host='localhost' AND User IN ('mysql.infoschema','mysql.session','mysql.sys'))"
)
OFFICIAL_REFERENCES = (
    "https://dev.mysql.com/doc/refman/8.4/en/resetting-permissions.html",
    "https://dev.mysql.com/doc/refman/8.4/en/persisted-system-variables.html",
    "https://dev.mysql.com/doc/refman/8.4/en/binary-log.html",
    "https://dev.mysql.com/doc/refman/8.4/en/transport-protocols.html",
    "https://dev.mysql.com/doc/refman/8.4/en/reset-persist.html",
    "https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.11/sql/persisted_variable.cc",
    "https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_skip_name_resolve",
)


class ColdError(RuntimeError):
    """Fixed public reason codes; driver output, configuration and secrets stay private."""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _digest(path, *, pulse=None):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 ** 2), b""):
            digest.update(chunk)
            if pulse:
                pulse()
    return digest.hexdigest()


class _SnapshotPulse:
    """Bound expensive CIM checks and emit only non-secret aggregate progress."""

    def __init__(self, runtime_probe, total_files, total_bytes, progress, clock):
        self.runtime_probe, self.progress, self.clock = runtime_probe, progress, clock
        self.total_files, self.total_bytes = total_files, total_bytes
        self.copied_files, self.copied_bytes = 0, 0
        self.started = self.last_guard = self.last_report = clock()

    def __call__(self, *, force_guard=False):
        current = self.clock()
        if force_guard or current - self.last_guard >= 5:
            require_stopped(SERVICE, runtime_probe=self.runtime_probe)
            self.last_guard = self.clock()
        if self.progress and current - self.last_report >= 30:
            self.progress({"status": "copying", "phase": "cold-copy-and-sha-verification",
                           "copied_files": self.copied_files, "copied_bytes": self.copied_bytes,
                           "total_files": self.total_files, "total_bytes": self.total_bytes,
                           "elapsed_seconds": int(current - self.started), "production_active": False})
            self.last_report = current


def _print_progress(value):
    print(_json(value), flush=True)


def _safe_path(path):
    path = Path(path).absolute()
    for ancestor in (path, *path.parents):
        if ancestor.exists() or ancestor.is_symlink():
            info = ancestor.lstat()
            if (stat.S_ISLNK(info.st_mode)
                    or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024)):
                raise ColdError("REPARSE_OR_SYMLINK_REFUSED")
    return path


def _relative(value):
    if not isinstance(value, str) or "\\" in value or "\0" in value:
        raise ColdError("SNAPSHOT_FILE_PATH_INVALID")
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(part in (".", "..") or ":" in part for part in path.parts):
        raise ColdError("SNAPSHOT_FILE_PATH_INVALID")
    return path


def _write(path, value, *, exclusive=False):
    _safe_path(path)
    if exclusive:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        return
    temporary = path.with_name("." + path.name + "." + secrets.token_hex(8) + ".tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def windows_runtime(*, runner=subprocess.run):
    if os.name != "nt":
        raise ColdError("WINDOWS_RUNTIME_PROBE_REQUIRED")
    script = ("$ErrorActionPreference='Stop';[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; [pscustomobject]@{"
              "processes=@(Get-CimInstance Win32_Process -Filter \"Name='mysqld.exe'\" | "
              "Select-Object ProcessId,Name); services=@(Get-CimInstance Win32_Service | "
              "Select-Object Name,State,StartMode,ProcessId)} | ConvertTo-Json -Depth 4 -Compress")
    result = runner(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), check=False)
    if result.returncode != 0:
        raise ColdError("WINDOWS_RUNTIME_PROBE_FAILED")
    return json.loads(result.stdout.decode("utf-8-sig"))


def require_stopped(service, *, runtime_probe=windows_runtime):
    state = runtime_probe()
    services = [row for row in state.get("services", []) if row.get("Name") == service]
    if (state.get("processes") or len(services) != 1 or services[0].get("State") != "Stopped"
            or services[0].get("StartMode") != "Disabled" or int(services[0].get("ProcessId", -1)) != 0):
        raise ColdError("SOURCE_NOT_STOPPED_DISABLED")


def require_protected_root(root, *, runner=subprocess.run):
    """Require an ACL protected by the installer before creating any credential."""
    if os.name != "nt":
        raise ColdError("WINDOWS_PROTECTED_ROOT_REQUIRED")
    script = ("$ErrorActionPreference='Stop';$a=Get-Acl -LiteralPath $env:PROBIGA_COLD_ACL_PATH;"
              "$r=@($a.Access|ForEach-Object {[pscustomobject]@{sid=$_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value;"
              "rights=[int]$_.FileSystemRights;inherited=$_.IsInherited;inheritance=[int]$_.InheritanceFlags;"
              "propagation=[int]$_.PropagationFlags;type=[int]$_.AccessControlType}});"
              "[pscustomobject]@{protected=$a.AreAccessRulesProtected;rules=$r}|ConvertTo-Json -Depth 4 -Compress")
    environment = dict(os.environ)
    environment["PROBIGA_COLD_ACL_PATH"] = str(root)
    result = runner(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                    env=environment, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), check=False)
    if result.returncode != 0:
        raise ColdError("TARGET_ROOT_ACL_VERIFICATION_FAILED")
    acl = json.loads(result.stdout.decode("utf-8-sig"))
    administrators = {"S-1-5-18", "S-1-5-32-544"}
    rules = acl.get("rules", [])
    found, extra = set(), set()
    valid = acl.get("protected") is True and bool(rules)
    for rule in rules:
        sid = rule.get("sid", "")
        valid = valid and rule.get("type") == 0 and rule.get("inherited") is False
        if sid in administrators:
            found.add(sid)
            valid = (valid and rule.get("rights") == 2032127 and rule.get("inheritance") == 3
                     and rule.get("propagation") == 0)
        else:
            extra.add(sid)
            # One original interactive user may traverse this root but cannot
            # list/read it. None of these rights inherit to credential files.
            valid = (valid and bool(re.fullmatch(r"S-1-5-21-(?:\d+-){3}\d+", sid))
                     and int(rule.get("rights", -1)) > 0 and int(rule.get("rights", -1)) & ~131232 == 0
                     and rule.get("inheritance") == 0 and rule.get("propagation") == 0)
    if not valid or found != administrators or len(extra) > 1:
        raise ColdError("TARGET_ROOT_ACL_NOT_RESTRICTED")


def _inventory(root, *, config_only=False):
    root = _safe_path(root)
    if config_only and root.is_file():
        files = [("my.ini", root)]
    elif config_only:
        files = [("my.ini", root / "my.ini")]
    else:
        if not root.is_dir():
            raise ColdError("SOURCE_ROOT_MISSING")
        files = []
        for current, directories, names in os.walk(root, followlinks=False):
            for directory in directories:
                _safe_path(Path(current) / directory)
            for name in names:
                path = _safe_path(Path(current) / name)
                if not stat.S_ISREG(path.lstat().st_mode):
                    raise ColdError("NON_REGULAR_FILE_REFUSED")
                files.append((path.relative_to(root).as_posix(), path))
    result, folded = {}, set()
    for relative, path in sorted(files):
        _relative(relative)
        _safe_path(path)
        info = path.stat()
        if relative.casefold() in folded:
            raise ColdError("CASE_COLLIDING_FILE_REFUSED")
        folded.add(relative.casefold())
        result[relative] = (info.st_size, info.st_mtime_ns)
    return result


def _parse_config(text):
    options, group = {}, None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("!"):
            raise ColdError("EXTERNAL_OPTION_FILE_INCLUDE_REFUSED")
        if line.startswith("[") and line.endswith("]"):
            group = line[1:-1].casefold()
            continue
        if group not in {"mysqld", "server", "mysqld-8.4"}:
            continue
        key, separator, value = line.partition("=")
        key = key.strip().lower().replace("-", "_")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", key) or key in options:
            raise ColdError("AMBIGUOUS_SOURCE_MYSQL_OPTION")
        options[key] = value.strip().strip('"').strip("'") if separator else "ON"
    if not options:
        raise ColdError("SOURCE_MYSQL_CONFIGURATION_MISSING")
    return options


def _auto_uuid(path):
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(path.read_text(encoding="utf-8-sig"))
    value = parser.get("auto", "server-uuid", fallback=None) or parser.get("auto", "server_uuid", fallback=None)
    if not value or not re.fullmatch(UUID_PATTERN, value):
        raise ColdError("DATABASE_AUTO_UUID_INVALID")
    return value.lower()


def _validate_pause(layout, pause):
    source = layout.get("source") or {}
    if (layout.get("format") != LAYOUT_FORMAT or set(layout.get("roots", {})) != set(TARGET_DIRS)
            or source.get("version") != "8.4.11" or not source.get("hostname")
            or source.get("service_name") != SERVICE or not re.fullmatch(UUID_PATTERN, str(source.get("server_uuid", "")))
            or _path_string(source.get("datadir", "")).rstrip("/").casefold() != _path_string(layout["roots"]["data"]).rstrip("/").casefold()
            or _path_string(source.get("logsdir", "")).rstrip("/").casefold() != _path_string(layout["roots"]["logs"]).rstrip("/").casefold()):
        raise ColdError("COLD_SOURCE_LAYOUT_INVALID")
    if (pause.get("format") != PAUSE_FORMAT or pause.get("status") != "paused"
            or str(pause.get("source_host", "")).casefold() != source["hostname"].casefold()
            or pause.get("source_server_uuid") != source["server_uuid"]
            or pause.get("source_service_name") != SERVICE
            or pause.get("source_service_state") != "Stopped" or pause.get("source_service_startup") != "Disabled"
            or pause.get("source_processes_running") is not False or pause.get("shutdown_complete") is not True
            or pause.get("source_automatically_resume") is not False or not pause.get("completed_at_utc")):
        raise ColdError("DURABLE_SOURCE_PAUSE_REQUIRED")


def _critical_files(files):
    names = {(row["root"], row["path"]) for row in files}
    if (not {("data", "auto.cnf"), ("data", "mysql.ibd"), ("data", "ibdata1"),
             ("config", "my.ini")}.issubset(names)
            or sum(name.startswith("undo_") for label, name in names if label == "data") < 2
            or not any("#innodb_redo/" in name and "#ib_redo" in name for _, name in names)
            or not any(name.endswith(".index") for label, name in names if label in {"data", "logs"})):
        raise ColdError("COMPLETE_PHYSICAL_MYSQL_FILE_SET_REQUIRED")


def snapshot_source(layout, pause, destination, *, runtime_probe=windows_runtime, progress=None, clock=None):
    _validate_pause(layout, pause)
    destination = _safe_path(destination)
    roots = {label: _safe_path(path) for label, path in layout["roots"].items()}
    for label, path in roots.items():
        if destination == path or path in destination.parents or destination in path.parents:
            raise ColdError("SNAPSHOT_DESTINATION_OVERLAPS_SOURCE")
        for other_label, other in roots.items():
            if label != other_label and (path == other or path in other.parents or other in path.parents):
                raise ColdError("SOURCE_ROOTS_OVERLAP")
    if destination.exists():
        raise ColdError("SNAPSHOT_DESTINATION_ALREADY_EXISTS")
    require_stopped(SERVICE, runtime_probe=runtime_probe)
    if _auto_uuid(roots["data"] / "auto.cnf") != layout["source"]["server_uuid"].lower():
        raise ColdError("SOURCE_AUTO_UUID_MISMATCH")
    inventories = {label: _inventory(path, config_only=label == "config") for label, path in roots.items()}
    pulse = _SnapshotPulse(runtime_probe, sum(map(len, inventories.values())),
                           sum(size for inventory in inventories.values() for size, _ in inventory.values()),
                           progress, clock or time.monotonic)
    pulse(force_guard=True)
    destination.mkdir(parents=True)
    _write(destination / "snapshot-state.json", _json({"status": "COPYING", "production_active": False}), exclusive=True)
    files = []
    for label, inventory in inventories.items():
        for relative, (size, modified) in inventory.items():
            pulse()
            source = roots[label] if label == "config" and roots[label].is_file() else roots[label] / relative
            target = destination / label / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            with source.open("rb") as original, target.open("xb") as copied:
                for chunk in iter(lambda: original.read(4 * 1024 ** 2), b""):
                    digest.update(chunk)
                    copied.write(chunk)
                    pulse.copied_bytes += len(chunk)
                    pulse()
                copied.flush()
                os.fsync(copied.fileno())
            shutil.copystat(source, target, follow_symlinks=False)
            if (source.stat().st_size, source.stat().st_mtime_ns) != (size, modified) or _digest(target, pulse=pulse) != digest.hexdigest():
                raise ColdError("SOURCE_CHANGED_OR_COPY_HASH_MISMATCH")
            files.append({"root": label, "path": relative, "bytes": size, "sha256": digest.hexdigest()})
            pulse.copied_files += 1
            pulse()
    pulse(force_guard=True)
    if inventories != {label: _inventory(path, config_only=label == "config") for label, path in roots.items()}:
        raise ColdError("SOURCE_FILE_INVENTORY_CHANGED")
    pulse(force_guard=True)
    _critical_files(files)
    metadata = {"format": SNAPSHOT_FORMAT, "status": "ready", "source": layout["source"],
                "source_roots": layout["roots"], "pause": pause, "files": files,
                "production_active": False, "source_automatically_resume": False}
    metadata["snapshot_id"] = hashlib.sha256(_json(metadata).encode("utf-8")).hexdigest()
    _write(destination / "metadata.json", _json(metadata) + "\n", exclusive=True)
    _write(destination / "snapshot-state.json", _json({"status": "ready", "snapshot_id": metadata["snapshot_id"]}))
    return metadata


def load_snapshot(package, *, verify_files=True):
    database = _safe_path(Path(package) / "database")
    metadata = json.loads((database / "metadata.json").read_text(encoding="utf-8-sig"))
    identity = dict(metadata)
    snapshot_id = identity.pop("snapshot_id", None)
    if (metadata.get("format") != SNAPSHOT_FORMAT or metadata.get("status") != "ready"
            or snapshot_id != hashlib.sha256(_json(identity).encode("utf-8")).hexdigest()
            or metadata.get("production_active") is not False):
        raise ColdError("COLD_SNAPSHOT_RECEIPT_INVALID")
    _validate_pause({"format": LAYOUT_FORMAT, "source": metadata.get("source"),
                     "roots": metadata.get("source_roots")}, metadata.get("pause") or {})
    observed = set()
    for row in metadata.get("files", []):
        label, relative = row.get("root"), _relative(row.get("path"))
        key = (label, str(relative).casefold())
        if (label not in TARGET_DIRS or key in observed or not isinstance(row.get("bytes"), int)
                or not re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256", "")))):
            raise ColdError("COLD_SNAPSHOT_FILE_INVENTORY_INVALID")
        observed.add(key)
        if verify_files:
            path = _safe_path(database / label / relative)
            if path.stat().st_size != row["bytes"] or _digest(path) != row["sha256"]:
                raise ColdError("COLD_SNAPSHOT_FILE_HASH_MISMATCH")
    if verify_files:
        for label in TARGET_DIRS:
            actual = set(_inventory(database / label))
            expected = {row["path"] for row in metadata["files"] if row["root"] == label}
            if actual != expected:
                raise ColdError("COLD_SNAPSHOT_UNLISTED_OR_MISSING_FILE")
    _critical_files(metadata["files"])
    return metadata, database


def _path_string(path):
    return str(path).replace("\\", "/")


def _is_absolute(value):
    return PureWindowsPath(value).is_absolute() or value.startswith("/")


def _mapped(value, metadata, root, *, relative_root="data"):
    normalized = value.replace("\\", "/")
    for label, original in sorted(metadata["source_roots"].items(), key=lambda item: -len(item[1])):
        prefix = original.replace("\\", "/").rstrip("/")
        if normalized.casefold() == prefix.casefold() or normalized.casefold().startswith(prefix.casefold() + "/"):
            suffix = normalized[len(prefix):].lstrip("/")
            if suffix and ".." in PurePosixPath(suffix).parts:
                raise ColdError("MYSQL_PATH_ESCAPES_OWNED_ROOT")
            return _path_string(root / TARGET_DIRS[label] / suffix)
    if _is_absolute(normalized):
        raise ColdError("EXTERNAL_MYSQL_PATH_NOT_IN_SNAPSHOT")
    relative = normalized.removeprefix("./")
    _relative(relative)
    return _path_string(root / TARGET_DIRS[relative_root] / relative)


def _persisted_values(path):
    if not path.exists():
        return {}
    document = json.loads(path.read_text(encoding="utf-8-sig"))
    result = {}
    version = document.get("Version")
    if version == 1 and not set(document) - {"Version", "mysql_server"}:
        groups = dict(document.get("mysql_server") or {})
        static = groups.pop("mysql_server_static_options", {})
        categories = [groups, static]
    elif version == 2 and not set(document) - {"Version", "mysql_dynamic_variables", "mysql_static_variables",
                                              "mysql_dynamic_parse_early_variables", "mysql_static_parse_early_variables"}:
        # Native 8.4.11 categories, as defined in Oracle persisted_variable.cc.
        # Encrypted/sensitive and unknown categories are deliberately refused.
        categories = [document.get(key, {}) for key in ("mysql_dynamic_variables", "mysql_static_variables",
                      "mysql_dynamic_parse_early_variables", "mysql_static_parse_early_variables")]
    else:
        raise ColdError("PERSISTED_MYSQL_FORMAT_UNSUPPORTED")
    for category in categories:
        if not isinstance(category, dict):
            raise ColdError("PERSISTED_MYSQL_VALUE_UNSUPPORTED")
        for name, setting in category.items():
            if (not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in result
                    or not isinstance(setting, dict) or not isinstance(setting.get("Value"), str)):
                raise ColdError("PERSISTED_MYSQL_VALUE_UNSUPPORTED")
            result[name] = setting["Value"]
    return result


def _sql_string(value):
    if any(character in value for character in ("\0", "\n", "\r")):
        raise ColdError("MYSQL_LITERAL_INVALID")
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def target_configuration(metadata, source_config, root, persisted):
    options = _parse_config(source_config)
    if "innodb_data_file_path" in options and options["innodb_data_file_path"] != "ibdata1:12M:autoextend":
        raise ColdError("NONDEFAULT_SYSTEM_TABLESPACE_REQUIRES_EXPLICIT_LAYOUT")
    for key in tuple(options):
        value = options[key]
        if key in {"basedir", "plugin_dir"}:
            options[key] = _path_string(root / "mysql84" / ("lib/plugin" if key == "plugin_dir" else ""))
        elif key == "tmpdir":
            # Scratch temporary files are not persistent cold-backup state.
            options[key] = _path_string(root / "mysql-tmp")
        elif key in {"datadir", "innodb_data_home_dir"}:
            options[key] = _path_string(root / TARGET_DIRS["data"])
        elif key == "innodb_directories":
            raise ColdError("EXTERNAL_INNODB_DIRECTORY_CONFIGURATION_REFUSED")
        elif key in {"init_file", "skip_grant_tables", "initialize", "initialize_insecure"}:
            raise ColdError("UNSAFE_SOURCE_DATABASE_BOOTSTRAP_OPTION")
        elif _is_absolute(value) or key in {"ssl_ca", "ssl_cert", "ssl_key", "log_bin", "log_bin_index",
                                            "log_error", "slow_query_log_file", "general_log_file", "pid_file",
                                            "innodb_log_group_home_dir", "innodb_undo_directory", "secure_file_priv"}:
            if value not in ("", "ON", "OFF", "NULL"):
                options[key] = _mapped(value, metadata, root)
    options.update({"basedir": _path_string(root / "mysql84"), "datadir": _path_string(root / "mysql-data"),
                    "port": str(PORT), "bind_address": "127.0.0.1", "mysqlx": "OFF", "event_scheduler": "OFF",
                    "require_secure_transport": "ON", "lower_case_table_names": "1", "local_infile": "OFF",
                    "innodb_buffer_pool_size": str(BUFFER_POOL), "innodb_redo_log_capacity": str(REDO_CAPACITY),
                    "tmpdir": _path_string(root / "mysql-tmp"), "general_log": "OFF",
                    "slow_query_log": "OFF", "shared_memory": "OFF", "shared_memory_base_name": MEMORY_NAME,
                    "log_bin_trust_function_creators": "OFF", "skip_replica_start": "ON"})
    for key in ("ssl_ca", "ssl_cert", "ssl_key"):
        if not options.get(key) or not Path(options[key]).is_file():
            raise ColdError("TARGET_COPIED_TLS_FILE_MISSING")
    options.pop("persisted_globals_load", None)
    options.pop("skip_networking", None)
    # This variable has no option-file startup form in MySQL 8.4.11.
    options.pop("default_collation_for_utf8mb4", None)
    # The unchanged original JSON is ignored only for official bootstrap. The
    # server itself rebuilds it via RESET PERSIST/SET PERSIST_ONLY in init-file.
    controlled = dict(persisted)
    for key, value in controlled.items():
        if key not in PERSISTED_OPTIONS:
            raise ColdError("UNRECOGNIZED_PERSISTED_OPTION_REFUSED")
        if _is_absolute(value):
            controlled[key] = _mapped(value, metadata, root)
    controlled.update({"innodb_buffer_pool_size": str(BUFFER_POOL), "innodb_redo_log_capacity": str(REDO_CAPACITY),
                       "default_collation_for_utf8mb4": "utf8mb4_general_ci", "event_scheduler": "OFF",
                       "require_secure_transport": "ON", "log_bin_trust_function_creators": "OFF"})
    for key in ("port", "datadir", "basedir", "bind_address", "mysqlx", "lower_case_table_names", "skip_networking",
                "shared_memory", "shared_memory_base_name", "persisted_globals_load", "init_file", "skip_replica_start"):
        if key in controlled:
            controlled[key] = options.get(key, "OFF" if key == "skip_networking" else "ON")
    lines = ["[mysqld]"]
    lines.extend(key + "=" + ('"' + value + '"' if "/" in value or " " in value else value)
                 for key, value in sorted(options.items()))
    return "\n".join(lines) + "\n", controlled, options


def bootstrap_sql(password, persisted):
    if not re.fullmatch(r"[A-Za-z0-9_-]{64}", password):
        raise ColdError("TARGET_ADMIN_PASSWORD_FORMAT_INVALID")
    statements = ["RESET PERSIST;", "SET GLOBAL event_scheduler=OFF;",
                  "SET GLOBAL default_collation_for_utf8mb4='utf8mb4_general_ci';",
                  "CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY " + _sql_string(password) + ";",
                  "ALTER USER 'root'@'localhost' IDENTIFIED BY " + _sql_string(password) + " REQUIRE NONE ACCOUNT UNLOCK;",
                  "GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;",
                  "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY " + _sql_string(password) + ";",
                  "ALTER USER 'root'@'127.0.0.1' IDENTIFIED BY " + _sql_string(password) + " REQUIRE SSL ACCOUNT UNLOCK;",
                  "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;",
                  "SET SESSION group_concat_max_len=1048576;",
                  "SET @cold_lock_sql=(SELECT COALESCE(CONCAT('ALTER USER ',GROUP_CONCAT(CONCAT(QUOTE(User),'@',QUOTE(Host)) SEPARATOR ','),' ACCOUNT LOCK'),'DO 0') FROM mysql.user WHERE " + NON_ADMIN_ACCOUNT_PREDICATE + ");",
                  "PREPARE cold_lock_stmt FROM @cold_lock_sql;", "EXECUTE cold_lock_stmt;", "DEALLOCATE PREPARE cold_lock_stmt;"]
    statements.extend("SET PERSIST_ONLY `" + key + "`=" + _sql_string(value) + ";" for key, value in sorted(persisted.items()))
    return "\n".join(statements) + "\n"


def _rewrite_indexes(metadata, root):
    changes = []
    for row in metadata["files"]:
        if row["root"] not in {"data", "logs"} or not row["path"].endswith(".index"):
            continue
        index = root / TARGET_DIRS[row["root"]] / row["path"]
        lines = index.read_text(encoding="utf-8-sig").splitlines()
        mapped = []
        for line in lines:
            if not line.strip():
                raise ColdError("EMPTY_BINARY_LOG_INDEX_ENTRY")
            target = Path(_mapped(line.strip(), metadata, root, relative_root=row["root"]))
            _safe_path(target)
            if not target.is_file() or target.suffix == ".index":
                raise ColdError("BINARY_LOG_INDEX_REFERENCE_NOT_IN_COPY")
            mapped.append(_path_string(target))
        if len(set(value.casefold() for value in mapped)) != len(mapped):
            raise ColdError("DUPLICATE_BINARY_LOG_INDEX_ENTRY")
        _write(index, "\n".join(mapped) + "\n")
        changes.append({"root": row["root"], "path": row["path"], "before_sha256": row["sha256"], "after_sha256": _digest(index)})
    return changes


def restore_target(root, package, *, local_host=None, acl_check=require_protected_root):
    root = _safe_path(root)
    metadata, database = load_snapshot(package)
    local_host = local_host or os.environ.get("COMPUTERNAME") or socket.gethostname()
    if local_host.casefold() == metadata["source"]["hostname"].casefold():
        raise ColdError("SOURCE_COMPUTER_RESTORE_REFUSED")
    acl_check(root)
    status_file = root / "database-status.json"
    if status_file.exists():
        prior = json.loads(status_file.read_text(encoding="utf-8"))
        if (prior.get("format") != STATUS_FORMAT or prior.get("snapshot_id") != metadata["snapshot_id"]
                or prior.get("owned_root") != str(root) or prior.get("target_hostname") != local_host
                or prior.get("status") not in {"materialized", "bootstrap-verified", "paused-ready"}):
            raise ColdError("PARTIAL_OR_FOREIGN_COLD_RESTORE_REQUIRES_ADMINISTRATOR_RECOVERY")
        return {**prior, "idempotent": True}
    for name in (*TARGET_DIRS.values(), "mysql-tmp", "root-client.ini", "my.ini", "bootstrap-init.sql", "mysql-source-auto.cnf"):
        if (root / name).exists():
            raise ColdError("UNOWNED_TARGET_DATABASE_FILE_REFUSED")
    receipt = {"format": STATUS_FORMAT, "status": "MATERIALIZING", "owned_root": str(root),
               "snapshot_id": metadata["snapshot_id"], "source_uuid": metadata["source"]["server_uuid"],
               "target_hostname": local_host, "service_name": SERVICE, "port": PORT,
               "production_active": False, "pending_production_activation": False,
               "source_automatically_resume": False, "created_at_utc": _now()}
    _write(status_file, _json(receipt), exclusive=True)
    (root / "mysql-tmp").mkdir()
    for row in metadata["files"]:
        relative = _relative(row["path"])
        target = root / TARGET_DIRS[row["root"]] / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(database / row["root"] / relative, target)
        if _digest(target) != row["sha256"]:
            raise ColdError("TARGET_PHYSICAL_COPY_HASH_MISMATCH")
    copied_auto = root / "mysql-data/auto.cnf"
    if _auto_uuid(copied_auto) != metadata["source"]["server_uuid"].lower():
        raise ColdError("COPIED_SOURCE_AUTO_UUID_MISMATCH")
    # Only the owned target's verified copy is moved aside. The source and
    # immutable cold payload retain auto.cnf byte-for-byte for rollback.
    os.replace(copied_auto, root / "mysql-source-auto.cnf")
    index_changes = _rewrite_indexes(metadata, root)
    persisted = _persisted_values(root / "mysql-data/mysqld-auto.cnf")
    config, controlled, options = target_configuration(metadata, (root / "mysql-source-config/my.ini").read_text(encoding="utf-8-sig"), root, persisted)
    password = secrets.token_urlsafe(48)
    _write(root / "root-client.ini", "[client]\nuser=root\npassword=" + password + "\n"
           "default-character-set=utf8mb4\n", exclusive=True)
    _write(root / "bootstrap-init.sql", bootstrap_sql(password, controlled), exclusive=True)
    _write(root / "my.ini", config, exclusive=True)
    receipt.update({"status": "materialized", "copied_files": len(metadata["files"]),
                    "copied_bytes": sum(row["bytes"] for row in metadata["files"]),
                    "tls_ca": options["ssl_ca"], "binlog_index_rewrites": index_changes,
                    "bootstrap_required_options": ["--skip-networking", "--persisted-globals-load=OFF",
                                                   "--shared-memory", "--shared-memory-base-name=" + MEMORY_NAME],
                    "target_uuid": None, "accounts_locked_until_resume": False,
                    "accounts_lock_requires_bootstrap_verification": True,
                    "production_task_ownership": "REQUIRES_COORDINATED_HOST_UUID_RELEASE"})
    _write(status_file, _json(receipt) + "\n")
    return receipt


def _owned_file_path(value, root):
    normalized = str(value).replace("\\", "/")
    if not _is_absolute(normalized):
        relative = normalized.removeprefix("./")
        _relative(relative)
        return
    allowed = [_path_string(root / "mysql-data").rstrip("/").casefold() + "/"]
    if ".." in PurePosixPath(normalized).parts or not any(normalized.casefold().startswith(prefix) for prefix in allowed):
        raise ColdError("EXTERNAL_TABLESPACE_DATAFILE_REFUSED")


def verify_identity(state, receipt, metadata, root, phase):
    source = metadata["source"]
    # MySQL's own get_options() sets mysqld_port=0 under skip-networking.
    # The normal final service, verified separately, must use exactly 3306.
    expected_port = 0 if phase == "memory" else PORT
    if (state.get("version") != "8.4.11" or int(state.get("port", -1)) != expected_port
            or not re.fullmatch(UUID_PATTERN, str(state.get("server_uuid", "")))
            or state["server_uuid"].casefold() == source["server_uuid"].casefold()
            or state.get("hostname", "").casefold() != receipt["target_hostname"].casefold()
            or state.get("hostname", "").casefold() == source["hostname"].casefold()
            or (receipt.get("target_uuid") and receipt["target_uuid"] != state["server_uuid"])):
        raise ColdError("TARGET_FINAL_INSTANCE_IDENTITY_REFUSED")
    if (int(state.get("lower_case_table_names", -1)) != 1 or int(state.get("require_secure_transport", 0)) != 1
            or str(state.get("event_scheduler", "")).upper() != "OFF"
            or int(state.get("buffer_pool_size", -1)) != BUFFER_POOL
            or int(state.get("redo_capacity", -1)) != REDO_CAPACITY
            or state.get("collation") != "utf8mb4_general_ci" or int(state.get("trust_creators", -1)) != 0
            or int(state.get("unlocked_source_accounts", -1)) != 0 or int(state.get("log_bin", -1)) != 1
            or int(state.get("skip_networking", -1)) != (1 if phase == "memory" else 0)
            or int(state.get("shared_memory", -1)) != (1 if phase == "memory" else 0)
            or state.get("shared_memory_base_name") != MEMORY_NAME
            or state.get("current_user") != ("root@localhost" if phase == "memory" else "root@127.0.0.1")):
        raise ColdError("TARGET_PAUSED_RUNTIME_POLICY_REFUSED")
    if _path_string(state.get("datadir", "")).rstrip("/").casefold() != _path_string(root / "mysql-data").casefold():
        raise ColdError("TARGET_DATABASE_DIRECTORY_IDENTITY_REFUSED")
    for key in ("tablespace_paths", "file_paths"):
        if not isinstance(state.get(key), list) or not any(isinstance(value, str) and value for value in state[key]):
            raise ColdError("TARGET_TABLESPACE_PATH_INVENTORY_UNAVAILABLE")
        for value in state[key]:
            if value is not None:
                _owned_file_path(value, root)
    if not {"probiga", "probiga_qmt_history", "biga"}.issubset(set(state.get("schema_names", []))):
        raise ColdError("TARGET_BUSINESS_SCHEMA_INVENTORY_MISSING")
    inventory = source.get("inventory") or {}
    schemas = set(inventory.get("schemas", []))
    if schemas:
        actual_schemas = set(state.get("schema_names", []))
        if not schemas & SYSTEM_SCHEMAS:
            actual_schemas -= SYSTEM_SCHEMAS
        if schemas != actual_schemas:
            raise ColdError("TARGET_SOURCE_SCHEMA_INVENTORY_MISMATCH")
    if "tables" in inventory:
        expected = {(row["schema"], row["name"], row["type"]) for row in inventory["tables"]}
        actual = {(row["schema"], row["name"], row["type"]) for row in state.get("table_inventory", [])}
        scope = schemas or {key[0] for key in expected}
        actual = {key for key in actual if key[0] in scope}
        if expected != actual:
            raise ColdError("TARGET_SOURCE_TABLE_INVENTORY_MISMATCH")
    if phase == "tcp" and (state.get("ssl_version") not in {"TLSv1.2", "TLSv1.3"} or not state.get("ssl_cipher")):
        raise ColdError("TARGET_TLS_TRANSPORT_NOT_VERIFIED")
    if _auto_uuid(root / "mysql-data/auto.cnf") != state["server_uuid"].lower():
        raise ColdError("TARGET_GENERATED_AUTO_UUID_MISMATCH")


IDENTITY_SQL = (
    "SELECT JSON_OBJECT('server_uuid',@@server_uuid,'hostname',@@hostname,'version',@@version,"
    "'port',@@port,'datadir',@@datadir,'current_user',CURRENT_USER(),"
    "'lower_case_table_names',@@lower_case_table_names,'require_secure_transport',@@require_secure_transport,"
    "'event_scheduler',@@event_scheduler,'buffer_pool_size',@@innodb_buffer_pool_size,"
    "'redo_capacity',@@innodb_redo_log_capacity,'collation',@@default_collation_for_utf8mb4,"
    "'trust_creators',@@log_bin_trust_function_creators,'log_bin',@@log_bin,'skip_networking',@@skip_networking,"
    "'shared_memory',@@shared_memory,'shared_memory_base_name',@@shared_memory_base_name,"
    "'mysql_time_zone',@@time_zone,'mysql_system_time_zone',@@system_time_zone,"
    "'mysql_utc_datetime',DATE_FORMAT(UTC_TIMESTAMP(),'%Y-%m-%dT%H:%i:%sZ'),"
    "'ssl_version',(SELECT VARIABLE_VALUE FROM performance_schema.session_status WHERE VARIABLE_NAME='Ssl_version'),"
    "'ssl_cipher',(SELECT VARIABLE_VALUE FROM performance_schema.session_status WHERE VARIABLE_NAME='Ssl_cipher'),"
    "'unlocked_source_accounts',(SELECT COUNT(*) FROM mysql.user WHERE account_locked='N' AND "
    + NON_ADMIN_ACCOUNT_PREDICATE + "),"
    "'schema_names',(SELECT JSON_ARRAYAGG(SCHEMA_NAME) FROM information_schema.SCHEMATA),"
    "'table_inventory',(SELECT JSON_ARRAYAGG(JSON_OBJECT('schema',TABLE_SCHEMA,'name',TABLE_NAME,'type',TABLE_TYPE)) FROM information_schema.TABLES),"
    "'tablespace_paths',(SELECT JSON_ARRAYAGG(PATH) FROM information_schema.INNODB_DATAFILES),"
    "'file_paths',(SELECT JSON_ARRAYAGG(FILE_NAME) FROM information_schema.FILES));\n"
)


def verify_target(root, package, *, phase="memory", runner=subprocess.run, acl_check=require_protected_root):
    if phase not in {"memory", "tcp"}:
        raise ColdError("VERIFICATION_PHASE_INVALID")
    root = _safe_path(root)
    # restore already hashed the complete immutable package before any startup.
    # Verification does not reread tens of GB on every bounded readiness retry.
    metadata, _ = load_snapshot(package, verify_files=False)
    acl_check(root)
    status_file = root / "database-status.json"
    receipt = json.loads(status_file.read_text(encoding="utf-8"))
    if (receipt.get("format") != STATUS_FORMAT or receipt.get("snapshot_id") != metadata["snapshot_id"]
            or receipt.get("owned_root") != str(root)
            or receipt.get("status") not in {"materialized", "bootstrap-verified", "paused-ready"}
            or receipt.get("production_active") is not False or receipt.get("pending_production_activation") is not False
            or receipt.get("port") != PORT or receipt.get("service_name") != SERVICE):
        raise ColdError("OWNED_TARGET_RESTORE_RECEIPT_REQUIRED")
    if phase == "tcp" and (receipt.get("memory_verified") is not True or not receipt.get("target_uuid")):
        raise ColdError("MEMORY_BOOTSTRAP_VERIFICATION_REQUIRED_BEFORE_TLS")
    command = [str(root / "mysql84/bin/mysql.exe"), "--defaults-file=" + str(root / "root-client.ini"),
               "--no-login-paths", "--batch", "--raw", "--skip-column-names", "--connect-timeout=5"]
    if phase == "memory":
        command += ["--protocol=MEMORY", "--shared-memory-base-name=" + MEMORY_NAME]
    else:
        command += ["--protocol=TCP", "--host=127.0.0.1", "--port=3306", "--ssl-mode=VERIFY_CA", "--ssl-ca=" + receipt["tls_ca"]]
    environment = dict(os.environ)
    for key in tuple(environment):
        if key.startswith("MYSQL_"):
            environment.pop(key)
    try:
        result = runner(command, input=IDENTITY_SQL.encode("utf-8"), stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, env=environment, timeout=20, check=False,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        if result.returncode != 0 or len(result.stdout) > 8 * 1024 ** 2:
            raise ColdError("TARGET_READ_ONLY_VERIFICATION_QUERY_FAILED")
        state = json.loads(result.stdout.decode("utf-8-sig"))
    except ColdError:
        raise
    except Exception:
        raise ColdError("TARGET_READ_ONLY_VERIFICATION_QUERY_FAILED") from None
    verify_identity(state, receipt, metadata, root, phase)
    receipt.update({"status": "paused-ready" if phase == "tcp" else "bootstrap-verified",
                    "target_uuid": state["server_uuid"], "verified_at_utc": _now(),
                    "memory_verified": True, "tls_verified": phase == "tcp",
                    "accounts_locked_until_resume": True,
                    "accounts_lock_requires_bootstrap_verification": False,
                    "time_report": {"mysql_session_time_zone": state.get("mysql_time_zone"),
                                    "mysql_system_time_zone": state.get("mysql_system_time_zone"),
                                    "mysql_reported_utc": state.get("mysql_utc_datetime"),
                                    "clock_calibration": "NOT_VERIFIED_REQUIRES_PRODUCTION_RESUME_GATE"},
                    "production_active": False, "pending_production_activation": False})
    _write(status_file, _json(receipt) + "\n")
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    snapshot = commands.add_parser("snapshot")
    snapshot.add_argument("--source-layout", type=Path, required=True)
    snapshot.add_argument("--pause-receipt", type=Path, required=True)
    snapshot.add_argument("--destination", type=Path, required=True)
    for name in ("restore", "verify", "verify-tls"):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument("--package", type=Path, required=True)
        if name == "verify":
            command.add_argument("--phase", choices=("memory", "tcp"), default="memory")
    args = parser.parse_args(argv)
    try:
        if args.action == "snapshot":
            receipt = snapshot_source(json.loads(args.source_layout.read_text(encoding="utf-8-sig")),
                                      json.loads(args.pause_receipt.read_text(encoding="utf-8-sig")), args.destination,
                                      progress=_print_progress)
        elif args.action == "restore":
            receipt = restore_target(args.root, args.package)
        else:
            receipt = verify_target(args.root, args.package, phase="tcp" if args.action == "verify-tls" else args.phase)
    except ColdError as exc:
        print(_json({"status": "blocked", "reason": str(exc), "production_active": False}))
        return 2
    except Exception as exc:
        print(_json({"status": "blocked", "reason": "COLD_DATABASE_OPERATION_FAILED", "error_type": type(exc).__name__, "production_active": False}))
        return 2
    print(_json({key: receipt[key] for key in ("status", "snapshot_id", "target_uuid", "copied_files", "copied_bytes", "production_active") if key in receipt}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
