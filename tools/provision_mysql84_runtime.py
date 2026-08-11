#!/usr/bin/env python3
"""Provision the least-scoped TLS runtime account and staged production env.

The command targets only an explicitly identified local Oracle MySQL 8.4.11
pre-cutover instance.  It creates one ``caching_sha2_password`` account bound
to ``127.0.0.1``, grants schema-level privileges for the three canonical
business schemas, verifies TLS with that account, and writes its random secret
only to ACL-protected local files.  The active project ``.env`` is never
modified by this command; a separate cutover step promotes the staged file.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import secrets
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.bootstrap_mysql84_instance import _run_icacls
from tools.run_mysql55_consistent_dump import assert_protected_client_option_file
from tools.run_mysql84_logical_restore import (
    RestoreError,
    inspect_target,
    read_admin_client_options,
    validate_ca_file,
)


EXPECTED_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
RUNTIME_USER = "probiga_runtime"
RUNTIME_HOST = "127.0.0.1"
APPLY_ACK = "I_CONFIRM_ISOLATED_MYSQL84_RUNTIME_PROVISIONING"
_ACCOUNT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")
_PASSWORD_RE = re.compile(r"^[A-Za-z0-9_-]{60,}$")
_GLOBAL_PRIVILEGE_COLUMNS = (
    "Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv",
    "Drop_priv", "Reload_priv", "Shutdown_priv", "Process_priv", "File_priv",
    "Grant_priv", "References_priv", "Index_priv", "Alter_priv", "Show_db_priv",
    "Super_priv", "Create_tmp_table_priv", "Lock_tables_priv", "Execute_priv",
    "Repl_slave_priv", "Repl_client_priv", "Create_view_priv", "Show_view_priv",
    "Create_routine_priv", "Alter_routine_priv", "Create_user_priv", "Event_priv",
    "Trigger_priv", "Create_tablespace_priv",
)


class ProvisionError(RuntimeError):
    """A runtime-account or staged-environment safety gate failed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _protect_general_file(path: Path) -> None:
    if os.name == "nt":
        _run_icacls(path, directory=False)
    else:
        path.chmod(0o600)


def _write_new_protected(path: Path, payload: bytes, *, mysql_option: bool) -> None:
    if not path.is_absolute():
        raise ProvisionError("secret output paths must be absolute")
    if path.exists():
        raise ProvisionError(f"refusing to overwrite existing secret artifact: {path}")
    if not path.parent.is_dir():
        raise ProvisionError(f"secret output parent does not exist: {path.parent}")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    _protect_general_file(path)
    if mysql_option:
        try:
            assert_protected_client_option_file(path)
        except Exception as exc:
            raise ProvisionError("runtime option file ACL verification failed") from exc


def generate_password() -> str:
    password = secrets.token_urlsafe(48)
    if _PASSWORD_RE.fullmatch(password) is None:
        raise ProvisionError("secure runtime password generation failed")
    return password


def build_runtime_option_file(*, user: str, password: str) -> bytes:
    if _ACCOUNT_RE.fullmatch(user) is None or _PASSWORD_RE.fullmatch(password) is None:
        raise ProvisionError("runtime account or password failed the safe alphabet gate")
    return (
        "[client]\n"
        "protocol=tcp\n"
        "host=127.0.0.1\n"
        "port=3306\n"
        f"user={user}\n"
        f"password={password}\n"
    ).encode("ascii")


def _read_runtime_password(path: Path, *, expected_user: str) -> str:
    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.read(protected, encoding="utf-8-sig")
    if parser.sections() != ["client"]:
        raise ProvisionError("runtime option file must contain one [client] section")
    expected = {
        "protocol": "tcp",
        "host": RUNTIME_HOST,
        "port": "3306",
        "user": expected_user,
    }
    for name, value in expected.items():
        if parser.get("client", name, fallback="", raw=True).strip() != value:
            raise ProvisionError(f"runtime option file {name} does not match the formal target")
    password = parser.get("client", "password", fallback="", raw=True).strip()
    if _PASSWORD_RE.fullmatch(password) is None:
        raise ProvisionError("runtime option file contains an invalid password")
    return password


def build_staged_env(
    source_text: str,
    *,
    user: str,
    password: str,
    formal_ca: Path,
) -> str:
    if _ACCOUNT_RE.fullmatch(user) is None or _PASSWORD_RE.fullmatch(password) is None:
        raise ProvisionError("runtime credential cannot be staged")
    if not formal_ca.is_absolute():
        raise ProvisionError("formal CA path must be absolute")
    mysql_url = (
        "mysql+pymysql://"
        f"{quote(user, safe='')}:{quote(password, safe='')}"
        "@127.0.0.1:3306/probiga?charset=utf8mb4"
    )
    replacements = {
        "MYSQL_URL": mysql_url,
        "MYSQL_TLS_REQUIRED": "true",
        "MYSQL_SSL_CA": formal_ca.as_posix(),
    }
    found: set[str] = set()
    output: list[str] = []
    for line in source_text.splitlines():
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
        if match and match.group(1) in replacements:
            name = match.group(1)
            if name in found:
                raise ProvisionError(f"source env contains duplicate {name}")
            output.append(f"{name}={replacements[name]}")
            found.add(name)
        else:
            output.append(line)
    for name in ("MYSQL_URL", "MYSQL_TLS_REQUIRED", "MYSQL_SSL_CA"):
        if name not in found:
            output.append(f"{name}={replacements[name]}")
    return "\n".join(output).rstrip("\n") + "\n"


def _connect_admin(options, ca_file: Path):
    try:
        return pymysql.connect(
            host=options.host,
            port=options.port,
            user=options.user,
            password=options.password,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
            cursorclass=DictCursor,
            ssl_ca=str(ca_file),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError as exc:
        raise ProvisionError("target administrator TLS connection failed") from exc


def _provision_account(connection, *, user: str, password: str) -> dict[str, object]:
    account = f"`{user}`@`{RUNTIME_HOST}`"
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE USER IF NOT EXISTS {account} IDENTIFIED WITH "
            "caching_sha2_password BY %s REQUIRE SSL",
            (password,),
        )
        cursor.execute(
            f"ALTER USER {account} IDENTIFIED WITH caching_sha2_password BY %s "
            "REQUIRE SSL PASSWORD EXPIRE NEVER ACCOUNT UNLOCK",
            (password,),
        )
        for schema in EXPECTED_SCHEMAS:
            cursor.execute(f"GRANT ALL PRIVILEGES ON `{schema}`.* TO {account}")
        cursor.execute(
            "SELECT * FROM mysql.user WHERE User=%s AND Host=%s",
            (user, RUNTIME_HOST),
        )
        row = cursor.fetchone()
        cursor.execute(f"SHOW GRANTS FOR {account}")
        grants = cursor.fetchall()
    if not isinstance(row, Mapping):
        raise ProvisionError("runtime account was not created")
    plugin = str(row.get("plugin") or "")
    ssl_type = str(row.get("ssl_type") or "")
    locked = str(row.get("account_locked") or "")
    unexpected_global = sorted(
        name for name in _GLOBAL_PRIVILEGE_COLUMNS if str(row.get(name) or "N").upper() == "Y"
    )
    if plugin != "caching_sha2_password" or ssl_type != "ANY" or locked != "N":
        raise ProvisionError("runtime account authentication/TLS state is incorrect")
    if unexpected_global:
        raise ProvisionError(
            "runtime account unexpectedly has global privileges: "
            + ", ".join(unexpected_global)
        )
    grant_text = sorted(str(next(iter(item.values()))) for item in grants)
    for schema in EXPECTED_SCHEMAS:
        if not any(f"ON `{schema}`.*" in grant for grant in grant_text):
            raise ProvisionError(f"runtime account is missing the {schema} schema grant")
    return {
        "user": user,
        "host": RUNTIME_HOST,
        "plugin": plugin,
        "ssl_type": ssl_type,
        "account_locked": locked,
        "global_privileges": unexpected_global,
        "schema_grants_verified": list(EXPECTED_SCHEMAS),
    }


def _verify_runtime_tls(
    *, port: int, user: str, password: str, ca_file: Path
) -> dict[str, str]:
    results: dict[str, str] = {}
    for schema in EXPECTED_SCHEMAS:
        try:
            connection = pymysql.connect(
                host=RUNTIME_HOST,
                port=port,
                user=user,
                password=password,
                database=schema,
                charset="utf8mb4",
                autocommit=True,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
                cursorclass=DictCursor,
                ssl_ca=str(ca_file),
                ssl_verify_cert=True,
            )
        except pymysql.MySQLError as exc:
            raise ProvisionError(f"runtime TLS connection failed for {schema}") from exc
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                tls = cursor.fetchone()
                cursor.execute("SELECT DATABASE() AS database_name")
                selected = cursor.fetchone()
        finally:
            connection.close()
        cipher = str((tls or {}).get("Value") or "")
        if not cipher or str((selected or {}).get("database_name") or "") != schema:
            raise ProvisionError(f"runtime TLS/schema verification failed for {schema}")
        results[schema] = cipher
    return results


def _write_evidence(path: Path, value: Mapping[str, object]) -> None:
    if not path.is_absolute() or path.exists():
        raise ProvisionError("evidence path must be absolute and new")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def run(args: argparse.Namespace) -> dict[str, object]:
    if args.apply_ack != APPLY_ACK:
        raise ProvisionError("exact isolated-target provisioning acknowledgement is required")
    if args.expected_target_port == 3306:
        raise ProvisionError("runtime provisioning must occur on the isolated pre-cutover port")
    if _ACCOUNT_RE.fullmatch(args.runtime_user) is None:
        raise ProvisionError("runtime user name is unsafe")
    formal_ca = args.formal_ca.expanduser().resolve(strict=True)
    if formal_ca.drive.casefold() == "f:":
        raise ProvisionError("formal CA must not be on removable drive F")
    source_env = args.source_env.expanduser().resolve(strict=True)
    option_path = args.runtime_option_file.expanduser().resolve(strict=False)
    staged_env = args.staged_env.expanduser().resolve(strict=False)
    evidence_path = args.evidence.expanduser().resolve(strict=False)
    for output in (staged_env, evidence_path):
        if output.exists():
            raise ProvisionError(f"refusing to overwrite existing output: {output}")

    ca_file = validate_ca_file(args.target_ssl_ca)
    admin_options = read_admin_client_options(
        args.target_admin_option_file, expected_port=args.expected_target_port
    )
    target = inspect_target(
        admin_options,
        ca_file,
        expected_server_uuid=args.expected_target_uuid,
        expected_server_port=args.expected_target_port,
        expected_datadir=args.expected_target_datadir,
    )
    if tuple(sorted(target.business_schemas)) != tuple(sorted(EXPECTED_SCHEMAS)):
        raise ProvisionError("runtime provisioning requires all restored business schemas")

    if option_path.exists():
        password = _read_runtime_password(option_path, expected_user=args.runtime_user)
        option_created = False
    else:
        password = generate_password()
        _write_new_protected(
            option_path,
            build_runtime_option_file(user=args.runtime_user, password=password),
            mysql_option=True,
        )
        option_created = True

    connection = _connect_admin(admin_options, ca_file)
    try:
        account = _provision_account(
            connection, user=args.runtime_user, password=password
        )
    finally:
        connection.close()
    tls = _verify_runtime_tls(
        port=args.expected_target_port,
        user=args.runtime_user,
        password=password,
        ca_file=ca_file,
    )
    staged_text = build_staged_env(
        source_env.read_text(encoding="utf-8-sig"),
        user=args.runtime_user,
        password=password,
        formal_ca=formal_ca,
    )
    _write_new_protected(staged_env, staged_text.encode("utf-8"), mysql_option=False)
    result: dict[str, object] = {
        "schema_version": 1,
        "status": "success",
        "finished_at_utc": _utc_now(),
        "target": asdict(target),
        "account": account,
        "runtime_tls_by_schema": tls,
        "runtime_option_file": str(option_path),
        "runtime_option_file_created": option_created,
        "staged_env": str(staged_env),
        "staged_env_sha256": _sha256(staged_env),
        "formal_ca": str(formal_ca),
        "secrets_in_evidence": False,
    }
    _write_evidence(evidence_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Provision ProBigA MySQL 8.4 TLS runtime account and staged env."
    )
    parser.add_argument("--target-admin-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--runtime-user", default=RUNTIME_USER)
    parser.add_argument("--runtime-option-file", type=Path, required=True)
    parser.add_argument("--source-env", type=Path, required=True)
    parser.add_argument("--staged-env", type=Path, required=True)
    parser.add_argument("--formal-ca", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--apply-ack", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run(args)
    except (ProvisionError, RestoreError, OSError, ValueError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
