#!/usr/bin/env python3
"""Read-only TEST/CI audit for the five frozen V4 runtime identities.

Every role resolves only its dedicated URL and server-UUID environment
variables.  There is deliberately no MYSQL_URL, DATABASE_URL, or shared V4
URL fallback.  The command performs identity and effective-grant inspection;
it never creates users, renders grants, or mutates application tables.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable, Mapping, Sequence
import uuid

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.engine_factory import create_pooled_engine
from server.common.mysql_version_policy import (
    PRODUCTION_DATABASE_ACTIVATION_ALLOWED,
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_versions_label,
)
from server.integrations.v4_database_roles import (
    ROLE_MANIFEST_HASH,
    ROLE_MANIFEST_VERSION,
    V4RuntimeDatabaseRole,
    audit_current_user_role,
)


PRODUCTION_ACTIVATION_ALLOWED = PRODUCTION_DATABASE_ACTIVATION_ALLOWED
ACTIONABLE_OUTPUT_ALLOWED = False

_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}$"
)
_DATABASE_RE = {
    "TEST": re.compile(
        r"^[a-z0-9]+(?:_[a-z0-9]+)*_test(?:_[a-z0-9]+)*$",
        re.IGNORECASE,
    ),
    "CI": re.compile(
        r"^[a-z0-9]+(?:_[a-z0-9]+)*_ci(?:_[a-z0-9]+)*$",
        re.IGNORECASE,
    ),
}
_FORBIDDEN_IDENTITY_TOKEN_RE = re.compile(
    r"(?:^|[_.-])(?:prod(?:uction)?|live|business)(?:$|[_.-])",
    re.IGNORECASE,
)
_ADMIN_USERS = frozenset({"root", "admin", "administrator"})
_ROLE_ENV_TOKEN = {
    V4RuntimeDatabaseRole.PREDICTOR: "PREDICTOR",
    V4RuntimeDatabaseRole.OUTBOX_WORKER: "OUTBOX_WORKER",
    V4RuntimeDatabaseRole.V2_EXECUTOR: "V2_EXECUTOR",
    V4RuntimeDatabaseRole.EVALUATOR: "EVALUATOR",
    V4RuntimeDatabaseRole.API_READER: "API_READER",
}


class V4RuntimeGrantAuditCliError(RuntimeError):
    """A target or connected identity cannot be proven safe and exact."""


@dataclass(frozen=True, slots=True)
class RuntimeAuditTarget:
    environment: str
    role: V4RuntimeDatabaseRole
    url: str = field(repr=False)
    database: str
    expected_server_uuid: str
    expected_username: str
    url_environment_variable: str
    uuid_environment_variable: str


@dataclass(frozen=True, slots=True)
class RuntimeServerIdentity:
    database: str
    server_version: str
    server_uuid: str
    version_comment: str
    current_user: str


def role_environment_variables(
    environment: str,
    role: V4RuntimeDatabaseRole,
) -> tuple[str, str]:
    normalized = str(environment).strip().upper()
    if normalized not in _DATABASE_RE:
        raise V4RuntimeGrantAuditCliError(
            "environment must be exactly TEST or CI"
        )
    if type(role) is not V4RuntimeDatabaseRole:
        raise V4RuntimeGrantAuditCliError(
            "role must be exactly V4RuntimeDatabaseRole"
        )
    token = _ROLE_ENV_TOKEN[role]
    prefix = f"V4_{normalized}_{token}_MYSQL"
    return f"{prefix}_URL", f"{prefix}_SERVER_UUID"


def _canonical_uuid(value: object, name: str) -> str:
    if type(value) is not str:
        raise V4RuntimeGrantAuditCliError(f"{name} is required")
    normalized = value.strip().lower()
    if _CANONICAL_UUID_RE.fullmatch(normalized) is None:
        raise V4RuntimeGrantAuditCliError(
            f"{name} must be a canonical non-nil UUID"
        )
    try:
        parsed = uuid.UUID(normalized)
    except ValueError as exc:
        raise V4RuntimeGrantAuditCliError(
            f"{name} must be a canonical non-nil UUID"
        ) from exc
    if parsed.int == 0:
        raise V4RuntimeGrantAuditCliError(
            f"{name} must be a canonical non-nil UUID"
        )
    return str(parsed)


def _resolve_target(
    environment: str,
    role: V4RuntimeDatabaseRole,
    source: Mapping[str, str],
) -> RuntimeAuditTarget:
    normalized = str(environment).strip().upper()
    url_env, uuid_env = role_environment_variables(normalized, role)
    raw_url = str(source.get(url_env, "") or "").strip()
    if not raw_url:
        raise V4RuntimeGrantAuditCliError(f"{url_env} is required")
    try:
        parsed = make_url(raw_url)
    except ArgumentError as exc:
        raise V4RuntimeGrantAuditCliError(f"{url_env} is invalid") from exc
    if parsed.get_backend_name().lower() != "mysql":
        raise V4RuntimeGrantAuditCliError("runtime audit requires MySQL URLs")
    if parsed.query:
        raise V4RuntimeGrantAuditCliError(
            "runtime audit URL query parameters are forbidden"
        )
    host = str(parsed.host or "").strip()
    username = str(parsed.username or "").strip()
    password = "" if parsed.password is None else str(parsed.password)
    database = str(parsed.database or "").strip()
    if not host:
        raise V4RuntimeGrantAuditCliError(
            "runtime audit URL requires an explicit host"
        )
    if not username or not password:
        raise V4RuntimeGrantAuditCliError(
            "runtime audit URL requires explicit non-root credentials"
        )
    if username.casefold() in _ADMIN_USERS:
        raise V4RuntimeGrantAuditCliError(
            "administrative runtime audit credentials are forbidden"
        )
    if _DATABASE_RE[normalized].fullmatch(database) is None:
        raise V4RuntimeGrantAuditCliError(
            f"{normalized} runtime audit requires an explicit *_"
            f"{normalized.lower()}* database"
        )
    for identity in (host, username, database):
        if _FORBIDDEN_IDENTITY_TOKEN_RE.search(identity):
            raise V4RuntimeGrantAuditCliError(
                "runtime audit target contains a production/business identity"
            )
    return RuntimeAuditTarget(
        environment=normalized,
        role=role,
        url=raw_url,
        database=database,
        expected_server_uuid=_canonical_uuid(source.get(uuid_env), uuid_env),
        expected_username=username,
        url_environment_variable=url_env,
        uuid_environment_variable=uuid_env,
    )


def resolve_targets(
    *,
    environment: str,
    role: V4RuntimeDatabaseRole | None = None,
    environ: Mapping[str, str] | None = None,
) -> tuple[RuntimeAuditTarget, ...]:
    if role is not None and type(role) is not V4RuntimeDatabaseRole:
        raise V4RuntimeGrantAuditCliError(
            "role must be exactly V4RuntimeDatabaseRole or None"
        )
    source = os.environ if environ is None else environ
    roles = tuple(V4RuntimeDatabaseRole) if role is None else (role,)
    targets = tuple(
        _resolve_target(environment, selected_role, source)
        for selected_role in roles
    )
    if len(targets) > 1:
        if len({target.database for target in targets}) != 1:
            raise V4RuntimeGrantAuditCliError(
                "all five runtime URLs must bind the same database"
            )
        if len({target.expected_server_uuid for target in targets}) != 1:
            raise V4RuntimeGrantAuditCliError(
                "all five runtime UUIDs must bind the same MySQL server"
            )
        if len({target.expected_username for target in targets}) != len(targets):
            raise V4RuntimeGrantAuditCliError(
                "all five runtime URLs must use distinct usernames"
            )
    return targets


def _server_identity(
    connection: Any,
    target: RuntimeAuditTarget,
) -> RuntimeServerIdentity:
    backend = str(
        getattr(getattr(connection, "dialect", None), "name", "")
    ).lower()
    if backend != "mysql":
        raise V4RuntimeGrantAuditCliError(
            "runtime audit connection must use MySQL"
        )
    row = connection.execute(
        text(
            "SELECT VERSION() AS server_version, "
            "@@version_comment AS version_comment, "
            "DATABASE() AS database_name, @@server_uuid AS server_uuid, "
            "CURRENT_USER() AS authenticated_user"
        )
    ).mappings().one()
    if set(row) != {
        "server_version",
        "version_comment",
        "database_name",
        "server_uuid",
        "authenticated_user",
    }:
        raise V4RuntimeGrantAuditCliError(
            "runtime audit identity query is incomplete"
        )
    identity = RuntimeServerIdentity(
        database=str(row["database_name"] or "").strip(),
        server_version=str(row["server_version"] or "").strip(),
        server_uuid=str(row["server_uuid"] or "").strip().lower(),
        version_comment=str(row["version_comment"] or "").strip(),
        current_user=str(row["authenticated_user"] or "").strip(),
    )
    if not is_oracle_mysql_distribution(
        identity.server_version,
        identity.version_comment,
    ):
        raise V4RuntimeGrantAuditCliError(
            "runtime audit requires Oracle MySQL"
        )
    if not is_isolated_acceptance_version(identity.server_version):
        raise V4RuntimeGrantAuditCliError(
            "runtime audit requires Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )
    if identity.database != target.database:
        raise V4RuntimeGrantAuditCliError(
            "connected database differs from the dedicated runtime URL"
        )
    if _DATABASE_RE[target.environment].fullmatch(identity.database) is None:
        raise V4RuntimeGrantAuditCliError(
            "connected runtime database is not TEST/CI-scoped"
        )
    if identity.server_uuid != target.expected_server_uuid:
        raise V4RuntimeGrantAuditCliError(
            "connected MySQL UUID differs from the independent expectation"
        )
    authenticated_username = identity.current_user.split("@", 1)[0]
    if authenticated_username != target.expected_username:
        raise V4RuntimeGrantAuditCliError(
            "authenticated runtime user differs from the dedicated URL user"
        )
    return identity


def _role_report(
    target: RuntimeAuditTarget,
    *,
    engine_factory: Callable[..., Any],
) -> dict[str, Any]:
    engine = engine_factory(
        target.url,
        future=True,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        isolation_level="REPEATABLE READ",
    )
    try:
        with engine.connect() as connection:
            identity = _server_identity(connection, target)
            audit = audit_current_user_role(
                connection,
                role=target.role,
                expected_database=target.database,
            )
    finally:
        engine.dispose()
    if (
        audit.production_activation_allowed is not False
        or audit.actionable_output_allowed is not False
        or audit.manifest_hash != ROLE_MANIFEST_HASH
        or audit.current_user != identity.current_user
        or not all(
            (
                audit.grant_options_checked,
                audit.column_privileges_checked,
                audit.routine_privileges_checked,
                audit.proxy_privileges_checked,
                audit.physical_tables_checked,
            )
        )
    ):
        raise V4RuntimeGrantAuditCliError(
            "runtime role audit report failed closed"
        )
    return {
        "role": target.role.value,
        "status": "PASSED",
        "database": identity.database,
        "server_version": identity.server_version,
        "server_uuid": identity.server_uuid,
        "version_comment": identity.version_comment,
        "current_user": identity.current_user,
        "table_count": len(
            set(audit.table_grants) | set(audit.column_grants)
        ),
        "table_privilege_count": sum(
            len(value) for value in audit.table_grants.values()
        ),
        "column_privilege_count": sum(
            len(privileges)
            for columns in audit.column_grants.values()
            for privileges in columns.values()
        ),
        "privilege_count": (
            sum(len(value) for value in audit.table_grants.values())
            + sum(
                len(privileges)
                for columns in audit.column_grants.values()
                for privileges in columns.values()
            )
        ),
        "grant_options_checked": True,
        "column_privileges_checked": True,
        "routine_privileges_checked": True,
        "proxy_privileges_checked": True,
        "physical_tables_checked": True,
        "read_only": True,
        "production_activation_allowed": False,
        "actionable_output_allowed": False,
    }


def audit_runtime_roles(
    *,
    environment: str,
    role: V4RuntimeDatabaseRole | None = None,
    environ: Mapping[str, str] | None = None,
    engine_factory: Callable[..., Any] = create_pooled_engine,
) -> dict[str, Any]:
    targets = resolve_targets(
        environment=environment,
        role=role,
        environ=environ,
    )
    reports = tuple(
        _role_report(target, engine_factory=engine_factory) for target in targets
    )
    authenticated = {str(report["current_user"]) for report in reports}
    if len(authenticated) != len(reports):
        raise V4RuntimeGrantAuditCliError(
            "runtime roles did not authenticate as distinct accounts"
        )
    return {
        "status": "PASSED",
        "environment": targets[0].environment,
        "mode": "ALL_ROLES" if role is None else "SINGLE_ROLE",
        "role_count": len(reports),
        "manifest_version": ROLE_MANIFEST_VERSION,
        "manifest_hash": ROLE_MANIFEST_HASH,
        "read_only": True,
        "roles": reports,
        "production_activation_allowed": False,
        "actionable_output_allowed": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only exact-grant audit for dedicated TEST/CI V4 runtime users"
        )
    )
    parser.add_argument(
        "--environment",
        choices=("TEST", "CI"),
        default="TEST",
    )
    parser.add_argument(
        "--role",
        choices=tuple(role.value for role in V4RuntimeDatabaseRole),
        help="audit one role; omit to require and audit all five role URLs",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    selected = (
        None if args.role is None else V4RuntimeDatabaseRole(args.role)
    )
    try:
        report = audit_runtime_roles(
            environment=args.environment,
            role=selected,
        )
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAILED_CLOSED",
                    "message": "runtime role audit failed closed",
                    "read_only": True,
                    "production_activation_allowed": False,
                    "actionable_output_allowed": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


__all__ = [
    "ACTIONABLE_OUTPUT_ALLOWED",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "RuntimeAuditTarget",
    "RuntimeServerIdentity",
    "V4RuntimeGrantAuditCliError",
    "audit_runtime_roles",
    "main",
    "resolve_targets",
    "role_environment_variables",
]


if __name__ == "__main__":
    raise SystemExit(main())
