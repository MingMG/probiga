#!/usr/bin/env python3
"""Provision a TLS-only, schema-scoped MySQL 8.4 migration account.

The account can change only the canonical ``probiga`` schema.  It receives no
global privileges; the separate administrator connection remains confined to
the guarded trigger-maintenance window that briefly changes the one required
global server variable.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.provision_mysql84_runtime import (  # noqa: E402
    _GLOBAL_PRIVILEGE_COLUMNS,
    _protect_general_file,
    generate_password,
)
from tools.run_mysql55_consistent_dump import (  # noqa: E402
    assert_protected_client_option_file,
)
from tools.run_mysql84_logical_restore import (  # noqa: E402
    RestoreError,
    inspect_target,
    read_admin_client_options,
    validate_ca_file,
)


MIGRATION_USER = "probiga_migrator"
MIGRATION_HOST = "127.0.0.1"
MIGRATION_SCHEMA = "probiga"
APPLY_ACK = "I_CONFIRM_ISOLATED_MYSQL84_MIGRATION_ACCOUNT_PROVISIONING"
_ACCOUNT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")
_PASSWORD_RE = re.compile(r"^[A-Za-z0-9_-]{60,}$")


class MigrationProvisionError(RuntimeError):
    """The isolated migration-account safety gate failed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_option_file(*, user: str, password: str, port: int) -> bytes:
    if _ACCOUNT_RE.fullmatch(user) is None or _PASSWORD_RE.fullmatch(password) is None:
        raise MigrationProvisionError("unsafe migration account credential")
    if not 1 <= port <= 65535 or port == 3306:
        raise MigrationProvisionError("migration option file requires an isolated port")
    return (
        "[client]\n"
        "protocol=tcp\n"
        "host=127.0.0.1\n"
        f"port={port}\n"
        f"user={user}\n"
        f"password={password}\n"
    ).encode("ascii")


def _write_new_protected(path: Path, payload: bytes) -> None:
    if not path.is_absolute() or path.exists() or not path.parent.is_dir():
        raise MigrationProvisionError(
            "migration option path must be absolute, new, and have an existing parent"
        )
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
    assert_protected_client_option_file(path)


def _read_password(path: Path, *, user: str, port: int) -> str:
    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.read(protected, encoding="utf-8-sig")
    expected = {
        "protocol": "tcp",
        "host": MIGRATION_HOST,
        "port": str(port),
        "user": user,
    }
    if parser.sections() != ["client"]:
        raise MigrationProvisionError("migration option file has an invalid shape")
    for name, value in expected.items():
        if parser.get("client", name, fallback="", raw=True).strip() != value:
            raise MigrationProvisionError(f"migration option file {name} mismatch")
    password = parser.get("client", "password", fallback="", raw=True).strip()
    if _PASSWORD_RE.fullmatch(password) is None:
        raise MigrationProvisionError("migration option file password is invalid")
    return password


def _connect_admin(options, ca_file: Path):
    try:
        return pymysql.connect(
            host=options.host,
            port=options.port,
            user=options.user,
            password=options.password,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
            ssl_ca=str(ca_file),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError as exc:
        raise MigrationProvisionError("administrator TLS connection failed") from exc


def provision(connection, *, user: str, password: str) -> dict[str, object]:
    account = f"`{user}`@`{MIGRATION_HOST}`"
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
        cursor.execute(f"REVOKE ALL PRIVILEGES, GRANT OPTION FROM {account}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON `{MIGRATION_SCHEMA}`.* TO {account}")
        cursor.execute(
            "SELECT * FROM mysql.user WHERE User=%s AND Host=%s",
            (user, MIGRATION_HOST),
        )
        row = cursor.fetchone()
        cursor.execute(f"SHOW GRANTS FOR {account}")
        grants = [str(next(iter(item.values()))) for item in cursor.fetchall()]
    if not isinstance(row, Mapping):
        raise MigrationProvisionError("migration account was not created")
    unexpected_global = sorted(
        name
        for name in _GLOBAL_PRIVILEGE_COLUMNS
        if str(row.get(name) or "N").upper() == "Y"
    )
    if unexpected_global:
        raise MigrationProvisionError("migration account has global privileges")
    if str(row.get("plugin") or "") != "caching_sha2_password":
        raise MigrationProvisionError("migration account uses the wrong plugin")
    if str(row.get("ssl_type") or "") != "ANY":
        raise MigrationProvisionError("migration account does not require TLS")
    if not any(f"ON `{MIGRATION_SCHEMA}`.*" in grant for grant in grants):
        raise MigrationProvisionError("migration account lacks the probiga schema grant")
    forbidden = [grant for grant in grants if " ON *.* " in grant and "USAGE ON *.*" not in grant]
    if forbidden:
        raise MigrationProvisionError("migration account has an unexpected global grant")
    return {
        "user": user,
        "host": MIGRATION_HOST,
        "schema": MIGRATION_SCHEMA,
        "plugin": "caching_sha2_password",
        "ssl_type": "ANY",
        "global_privileges": [],
    }


def verify_tls(*, port: int, user: str, password: str, ca_file: Path) -> str:
    try:
        connection = pymysql.connect(
            host=MIGRATION_HOST,
            port=port,
            user=user,
            password=password,
            database=MIGRATION_SCHEMA,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=10,
            ssl_ca=str(ca_file),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError as exc:
        raise MigrationProvisionError("migration account TLS connection failed") from exc
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            tls = cursor.fetchone() or {}
            cursor.execute("SELECT DATABASE() AS database_name")
            selected = cursor.fetchone() or {}
    finally:
        connection.close()
    cipher = str(tls.get("Value") or "")
    if not cipher or selected.get("database_name") != MIGRATION_SCHEMA:
        raise MigrationProvisionError("migration TLS/schema verification failed")
    return cipher


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    if not path.is_absolute() or path.exists():
        raise MigrationProvisionError("evidence path must be absolute and new")
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


def run(args: argparse.Namespace) -> dict[str, object]:
    if args.apply_ack != APPLY_ACK:
        raise MigrationProvisionError("exact migration-account acknowledgement is required")
    if args.expected_target_port == 3306:
        raise MigrationProvisionError("migration account must be provisioned pre-cutover")
    if _ACCOUNT_RE.fullmatch(args.migration_user) is None:
        raise MigrationProvisionError("unsafe migration user name")
    ca_file = validate_ca_file(args.target_ssl_ca)
    admin = read_admin_client_options(
        args.target_admin_option_file, expected_port=args.expected_target_port
    )
    target = inspect_target(
        admin,
        ca_file,
        expected_server_uuid=args.expected_target_uuid,
        expected_server_port=args.expected_target_port,
        expected_datadir=args.expected_target_datadir,
    )
    option_path = args.migration_option_file.expanduser().resolve(strict=False)
    if option_path.exists():
        password = _read_password(
            option_path, user=args.migration_user, port=args.expected_target_port
        )
        option_created = False
    else:
        password = generate_password()
        _write_new_protected(
            option_path,
            build_option_file(
                user=args.migration_user,
                password=password,
                port=args.expected_target_port,
            ),
        )
        option_created = True
    connection = _connect_admin(admin, ca_file)
    try:
        account = provision(
            connection, user=args.migration_user, password=password
        )
    finally:
        connection.close()
    tls_cipher = verify_tls(
        port=args.expected_target_port,
        user=args.migration_user,
        password=password,
        ca_file=ca_file,
    )
    result: dict[str, object] = {
        "schema_version": 1,
        "tool": "provision_mysql84_migration_account",
        "status": "success",
        "finished_at_utc": _utc_now(),
        "target": asdict(target),
        "account": account,
        "tls_cipher": tls_cipher,
        "migration_option_file": str(option_path),
        "migration_option_file_created": option_created,
        "secrets_in_evidence": False,
    }
    _atomic_json(args.evidence, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-admin-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--migration-user", default=MIGRATION_USER)
    parser.add_argument("--migration-option-file", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--apply-ack", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run(args)
    except (
        MigrationProvisionError,
        RestoreError,
        OSError,
        ValueError,
        pymysql.MySQLError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
