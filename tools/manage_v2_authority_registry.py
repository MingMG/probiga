#!/usr/bin/env python
"""Validate or apply one migration-014 authority-registry operation.

The default mode is deliberately local: it parses, validates and previews one
JSON operation without resolving a database URL or opening a connection.
``--apply`` is restricted to dedicated TEST/CI V2-evidence databases and never
authorizes production or actionable trading output.
"""

from __future__ import annotations

import argparse
import base64
import binascii
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from sqlalchemy.engine import Connection, URL, make_url
from sqlalchemy.exc import ArgumentError

from server.common.engine_factory import create_pooled_engine
from server.common.mysql_version_policy import (
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_versions_label,
)
from server.db.migrations_v2 import MIGRATIONS
from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    SignedAuthorityReceipt,
)
from server.integrations.v2_execution_evidence_authority_audit import (
    audit_v2_execution_evidence_authority_database,
)
from server.trading_v2.execution_evidence_schema_gate import (
    assert_v2_evidence_maintenance_fence_inactive,
    inspect_v2_execution_evidence_schema,
)


# Backward-compatible injection seam for callers/tests that replace the engine
# constructor without opening a real database connection.  The implementation
# itself is the shared application factory, not SQLAlchemy's raw constructor.
create_engine = create_pooled_engine


_MAX_OPERATION_BYTES = 1_048_576
_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}$"
)
_DATABASE_RE = {
    "TEST": re.compile(
        r"^[a-z0-9]+(?:_[a-z0-9]+)*_v2_evidence_test"
        r"(?:_[a-z0-9]+)*$",
        re.IGNORECASE,
    ),
    "CI": re.compile(
        r"^[a-z0-9]+(?:_[a-z0-9]+)*_v2_evidence_ci"
        r"(?:_[a-z0-9]+)*$",
        re.IGNORECASE,
    ),
}
_APPLY_ENVIRONMENT = {
    "TEST": (
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_URL",
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_SERVER_UUID",
    ),
    "CI": (
        "V2_EVIDENCE_CI_AUTHORITY_MYSQL_URL",
        "V2_EVIDENCE_CI_AUTHORITY_MYSQL_SERVER_UUID",
    ),
}
_FORBIDDEN_IDENTITY_TOKEN_RE = re.compile(
    r"(?:prod(?:uction)?|live|business)",
    re.IGNORECASE,
)
FROZEN_EVIDENCE_LEDGER = (
    (
        "20260803_011_v2_execution_evidence_bindings",
        "234a2b7a82573b5551b1485dd68598156e26d050d3b2d9b6a6ea76d3c34072d1",
    ),
    (
        "20260803_012_v2_execution_evidence_guards",
        "cf596bc5157ea5f6d835c07089556164cde9c0fcaf0c3ace10f10b15ba4b6fd1",
    ),
    (
        "20260803_013_v2_execution_evidence_natural_keys",
        "51addc459d4caae896ee656e901123646deb6a46584ac274092aa65026917eb8",
    ),
    (
        "20260803_014_v2_execution_authority_attestations",
        "984e2ea7c637c728745b9b21c3b508980cc046c1c434d9851619984918a3823d",
    ),
    (
        "20260803_015_v2_accounting_outcome_evidence",
        "8e06e57c38f7365fa471a7bde09f5cd4a3ea5aef5fee03c6195fd2930b725a2c",
    ),
)


class AuthorityRegistryCliError(RuntimeError):
    """Base class for a safe, operator-facing refusal."""


class AuthorityRegistryInputError(AuthorityRegistryCliError):
    """The operation document is malformed or non-canonical."""


class AuthorityRegistrySafetyError(AuthorityRegistryCliError):
    """The requested database target cannot be proven TEST/CI-only."""


class OperationKind(str, Enum):
    TRUST_KEY = "TRUST_KEY"
    RECEIPT = "RECEIPT"
    KEY_REVOCATION = "KEY_REVOCATION"
    RECEIPT_REVOCATION = "RECEIPT_REVOCATION"


@dataclass(frozen=True, slots=True)
class OperationPlan:
    operation: OperationKind
    request: object
    preview: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ApplyTarget:
    environment: str
    url: str
    parsed_url: URL
    database_name: str
    expected_server_uuid: str
    url_environment_variable: str
    uuid_environment_variable: str


@dataclass(frozen=True, slots=True)
class RuntimeIdentity:
    database_name: str
    server_version: str
    server_uuid: str
    version_comment: str


def _reject_constant(value: str) -> object:
    raise AuthorityRegistryInputError(f"JSON constant {value!r} is forbidden")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AuthorityRegistryInputError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _read_operation_text(path_value: str) -> str:
    if path_value == "-":
        value = sys.stdin.read(_MAX_OPERATION_BYTES + 1)
    else:
        path = Path(path_value)
        if not path.is_file():
            raise AuthorityRegistryInputError(
                f"operation file does not exist: {path_value}"
            )
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise AuthorityRegistryInputError(
                f"operation file cannot be inspected: {path_value}"
            ) from exc
        if size > _MAX_OPERATION_BYTES:
            raise AuthorityRegistryInputError("operation file is too large")
        try:
            value = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise AuthorityRegistryInputError(
                f"operation file is not readable UTF-8: {path_value}"
            ) from exc
    if len(value.encode("utf-8")) > _MAX_OPERATION_BYTES:
        raise AuthorityRegistryInputError("operation document is too large")
    if not value.strip():
        raise AuthorityRegistryInputError("operation document is empty")
    return value


def _load_operation_document(path_value: str) -> dict[str, object]:
    try:
        value = json.loads(
            _read_operation_text(path_value),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except AuthorityRegistryInputError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AuthorityRegistryInputError("operation document is invalid JSON") from exc
    if type(value) is not dict:
        raise AuthorityRegistryInputError("operation document must be a JSON object")
    _require_keys(value, required={"operation", "payload"}, name="operation")
    return value


def _require_keys(
    value: object,
    *,
    required: set[str],
    optional: set[str] | None = None,
    name: str,
) -> dict[str, object]:
    if type(value) is not dict:
        raise AuthorityRegistryInputError(f"{name} must be a JSON object")
    allowed = required | (optional or set())
    actual = set(value)
    if actual != required | (actual & (optional or set())) or not required <= actual:
        missing = sorted(required - actual)
        unexpected = sorted(actual - allowed)
        details: list[str] = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if unexpected:
            details.append("unexpected=" + ",".join(unexpected))
        raise AuthorityRegistryInputError(
            f"{name} fields differ" + (": " + "; ".join(details) if details else "")
        )
    return value


def _required_string(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise AuthorityRegistryInputError(f"{name} must be non-empty text")
    return value


def _aware_datetime(value: object, name: str) -> datetime:
    text_value = _required_string(value, name)
    try:
        parsed = datetime.fromisoformat(text_value)
    except ValueError as exc:
        raise AuthorityRegistryInputError(f"{name} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AuthorityRegistryInputError(f"{name} must include a UTC offset")
    return parsed


def _optional_datetime(value: object, name: str) -> datetime | None:
    return None if value is None else _aware_datetime(value, name)


def _date(value: object, name: str) -> date:
    text_value = _required_string(value, name)
    try:
        parsed = date.fromisoformat(text_value)
    except ValueError as exc:
        raise AuthorityRegistryInputError(f"{name} must be an ISO-8601 date") from exc
    if parsed.isoformat() != text_value:
        raise AuthorityRegistryInputError(f"{name} must use YYYY-MM-DD")
    return parsed


def _base64url_32(value: object, name: str) -> bytes:
    text_value = _required_string(value, name)
    try:
        decoded = base64.urlsafe_b64decode(
            text_value + "=" * ((4 - len(text_value) % 4) % 4)
        )
    except (ValueError, binascii.Error) as exc:
        raise AuthorityRegistryInputError(f"{name} must be base64url") from exc
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if len(decoded) != 32 or canonical != text_value:
        raise AuthorityRegistryInputError(
            f"{name} must be canonical unpadded base64url for exactly 32 bytes"
        )
    return decoded


def _writer_api() -> Any:
    try:
        from server.integrations import (  # pylint: disable=import-outside-toplevel
            v2_execution_evidence_authority_registry_writer as writer,
        )
    except ImportError as exc:
        raise AuthorityRegistryCliError(
            "authority registry writer package is unavailable"
        ) from exc
    return writer


def _claim(value: object) -> AuthorityClaim:
    fields = _require_keys(
        value,
        required={
            "evidence_type",
            "evidence_id",
            "source_provider",
            "source_payload_hash",
            "receipt_type",
            "receipt_id",
            "receipt_hash",
            "available_at",
            "trade_date",
        },
        optional={"event_at", "received_at"},
        name="payload.claim",
    )
    try:
        return AuthorityClaim(
            evidence_type=fields["evidence_type"],
            evidence_id=fields["evidence_id"],
            source_provider=fields["source_provider"],
            source_payload_hash=fields["source_payload_hash"],
            receipt_type=fields["receipt_type"],
            receipt_id=fields["receipt_id"],
            receipt_hash=fields["receipt_hash"],
            available_at=_aware_datetime(
                fields["available_at"], "payload.claim.available_at"
            ),
            trade_date=_date(fields["trade_date"], "payload.claim.trade_date"),
            event_at=_optional_datetime(
                fields.get("event_at"), "payload.claim.event_at"
            ),
            received_at=_optional_datetime(
                fields.get("received_at"), "payload.claim.received_at"
            ),
        )
    except (TypeError, ValueError) as exc:
        raise AuthorityRegistryInputError(f"authority claim is invalid: {exc}") from exc


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _pipe_hash(namespace: str, *values: str) -> str:
    return hashlib.sha256(
        (namespace + "|" + "|".join(values)).encode("utf-8")
    ).hexdigest()


def _build_plan(document: Mapping[str, object]) -> OperationPlan:
    try:
        operation = OperationKind(_required_string(document["operation"], "operation"))
    except ValueError as exc:
        raise AuthorityRegistryInputError(
            "operation must be TRUST_KEY, RECEIPT, KEY_REVOCATION, or "
            "RECEIPT_REVOCATION"
        ) from exc
    writer = _writer_api()
    payload = document["payload"]

    try:
        if operation is OperationKind.TRUST_KEY:
            fields = _require_keys(
                payload,
                required={
                    "source_provider",
                    "key_id",
                    "key_version",
                    "public_key_base64url",
                    "valid_from",
                },
                optional={"valid_to"},
                name="payload",
            )
            public_key = _base64url_32(
                fields["public_key_base64url"], "payload.public_key_base64url"
            )
            request = writer.AuthorityTrustKeyRegistration(
                source_provider=fields["source_provider"],
                key_id=fields["key_id"],
                key_version=fields["key_version"],
                public_key=public_key,
                valid_from=_aware_datetime(fields["valid_from"], "payload.valid_from"),
                valid_to=_optional_datetime(fields.get("valid_to"), "payload.valid_to"),
            )
            preview = {
                "source_provider": request.source_provider,
                "key_id": request.key_id,
                "key_version": request.key_version,
                "algorithm": "Ed25519",
                "public_key_hash": hashlib.sha256(public_key).hexdigest(),
                "valid_from": _utc_text(request.valid_from),
                "valid_to": (
                    None if request.valid_to is None else _utc_text(request.valid_to)
                ),
            }
        elif operation is OperationKind.RECEIPT:
            fields = _require_keys(
                payload,
                required={"claim", "envelope_json"},
                name="payload",
            )
            claim = _claim(fields["claim"])
            envelope_json = _required_string(
                fields["envelope_json"], "payload.envelope_json"
            )
            receipt = SignedAuthorityReceipt.from_json(envelope_json)
            request = writer.AuthorityReceiptRegistration(
                claim=claim,
                receipt=receipt,
            )
            preview = {
                "claim_hash": claim.claim_hash,
                "evidence_type": claim.evidence_type,
                "evidence_id": claim.evidence_id,
                "source_provider": claim.source_provider,
                "source_payload_hash": claim.source_payload_hash,
                "receipt_type": claim.receipt_type,
                "receipt_id": receipt.receipt_id,
                "receipt_hash": receipt.receipt_hash,
                "key_id": receipt.key_id,
                "key_version": receipt.key_version,
                "replay_nonce": receipt.replay_nonce,
                "issued_at": _utc_text(receipt.issued_at),
                "expires_at": _utc_text(receipt.expires_at),
                "envelope_hash": receipt.envelope_hash,
            }
        elif operation is OperationKind.KEY_REVOCATION:
            fields = _require_keys(
                payload,
                required={
                    "source_provider",
                    "key_id",
                    "key_version",
                    "revoked_at",
                    "reason_code",
                },
                name="payload",
            )
            revoked_at = _aware_datetime(fields["revoked_at"], "payload.revoked_at")
            request = writer.AuthorityKeyRevocation(
                source_provider=fields["source_provider"],
                key_id=fields["key_id"],
                key_version=fields["key_version"],
                revoked_at=revoked_at,
                reason_code=fields["reason_code"],
            )
            preview = {
                "source_provider": request.source_provider,
                "key_id": request.key_id,
                "key_version": request.key_version,
                "revoked_at": _utc_text(request.revoked_at),
                "reason_code": request.reason_code,
                "revocation_hash": _pipe_hash(
                    "trading-v2.authority-key-revocation.v1",
                    request.source_provider,
                    request.key_id,
                    request.key_version,
                    _utc_text(request.revoked_at),
                    request.reason_code,
                ),
            }
        else:
            fields = _require_keys(
                payload,
                required={
                    "receipt_id",
                    "receipt_hash",
                    "envelope_hash",
                    "revoked_at",
                    "reason_code",
                },
                name="payload",
            )
            revoked_at = _aware_datetime(fields["revoked_at"], "payload.revoked_at")
            request = writer.AuthorityReceiptRevocation(
                receipt_id=fields["receipt_id"],
                receipt_hash=fields["receipt_hash"],
                envelope_hash=fields["envelope_hash"],
                revoked_at=revoked_at,
                reason_code=fields["reason_code"],
            )
            preview = {
                "receipt_id": request.receipt_id,
                "receipt_hash": request.receipt_hash,
                "envelope_hash": request.envelope_hash,
                "revoked_at": _utc_text(request.revoked_at),
                "reason_code": request.reason_code,
                "revocation_hash": _pipe_hash(
                    "trading-v2.authority-receipt-revocation.v1",
                    request.receipt_id,
                    request.receipt_hash,
                    request.envelope_hash,
                    _utc_text(request.revoked_at),
                    request.reason_code,
                ),
            }
    except AuthorityRegistryInputError:
        raise
    except (TypeError, ValueError) as exc:
        raise AuthorityRegistryInputError(
            f"{operation.value} payload is invalid: {exc}"
        ) from exc
    return OperationPlan(operation=operation, request=request, preview=preview)


def parse_operation(path_value: str) -> OperationPlan:
    """Parse and locally validate one operation without touching a database."""

    return _build_plan(_load_operation_document(path_value))


def _canonical_uuid(value: object, name: str) -> str:
    if type(value) is not str:
        raise AuthorityRegistrySafetyError(f"{name} is required")
    normalized = value.strip().lower()
    if _CANONICAL_UUID_RE.fullmatch(normalized) is None:
        raise AuthorityRegistrySafetyError(f"{name} must be a canonical UUID")
    try:
        parsed = uuid.UUID(normalized)
    except ValueError as exc:
        raise AuthorityRegistrySafetyError(f"{name} must be a canonical UUID") from exc
    if parsed.int == 0:
        raise AuthorityRegistrySafetyError(f"{name} must not be the nil UUID")
    return str(parsed)


def _resolve_apply_target(
    environment: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> ApplyTarget:
    source = os.environ if environ is None else environ
    try:
        url_env, uuid_env = _APPLY_ENVIRONMENT[environment]
    except KeyError as exc:
        raise AuthorityRegistrySafetyError(
            "--environment must be exactly TEST or CI"
        ) from exc
    raw_url = str(source.get(url_env, "") or "").strip()
    if not raw_url:
        raise AuthorityRegistrySafetyError(f"{url_env} is required for --apply")
    try:
        parsed = make_url(raw_url)
    except ArgumentError as exc:
        raise AuthorityRegistrySafetyError(f"{url_env} is not a valid URL") from exc
    if parsed.get_backend_name().lower() != "mysql":
        raise AuthorityRegistrySafetyError("authority registry apply requires MySQL")
    if parsed.query:
        raise AuthorityRegistrySafetyError("authority registry URL queries are forbidden")
    if not str(parsed.host or "").strip():
        raise AuthorityRegistrySafetyError("authority registry URL requires an explicit host")
    if (
        not str(parsed.username or "").strip()
        or parsed.password is None
        or not str(parsed.password).strip()
    ):
        raise AuthorityRegistrySafetyError(
            "authority registry URL requires explicit non-root credentials"
        )
    if str(parsed.username).casefold() in {"root", "admin", "administrator"}:
        raise AuthorityRegistrySafetyError("administrative MySQL users are forbidden")
    database = str(parsed.database or "").strip()
    if _DATABASE_RE[environment].fullmatch(database) is None:
        raise AuthorityRegistrySafetyError(
            f"{environment} apply requires an explicit *_v2_evidence_"
            f"{environment.lower()}* database"
        )
    for identity in (str(parsed.host), str(parsed.username), database):
        if _FORBIDDEN_IDENTITY_TOKEN_RE.search(identity):
            raise AuthorityRegistrySafetyError(
                "authority registry URL contains a production/business identity"
            )
    expected_uuid = _canonical_uuid(source.get(uuid_env), uuid_env)
    return ApplyTarget(
        environment=environment,
        url=raw_url,
        parsed_url=parsed,
        database_name=database,
        expected_server_uuid=expected_uuid,
        url_environment_variable=url_env,
        uuid_environment_variable=uuid_env,
    )


def _runtime_identity(connection: Connection, target: ApplyTarget) -> RuntimeIdentity:
    row = connection.execute(
        text(
            "SELECT VERSION() AS server_version, "
            "@@version_comment AS version_comment, "
            "DATABASE() AS database_name, @@server_uuid AS server_uuid"
        )
    ).mappings().one()
    identity = RuntimeIdentity(
        database_name=str(row["database_name"] or "").strip(),
        server_version=str(row["server_version"] or "").strip(),
        server_uuid=str(row["server_uuid"] or "").strip().lower(),
        version_comment=str(row["version_comment"] or "").strip(),
    )
    if str(getattr(connection.dialect, "name", "")).lower() != "mysql":
        raise AuthorityRegistrySafetyError("runtime connection is not MySQL")
    if not is_oracle_mysql_distribution(
        identity.server_version,
        identity.version_comment,
    ):
        raise AuthorityRegistrySafetyError(
            "apply requires Oracle MySQL Community or Enterprise Server"
        )
    if not is_isolated_acceptance_version(identity.server_version):
        raise AuthorityRegistrySafetyError(
            "apply requires Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )
    if identity.database_name != target.database_name:
        raise AuthorityRegistrySafetyError(
            "connected database differs from the dedicated URL database"
        )
    if _DATABASE_RE[target.environment].fullmatch(identity.database_name) is None:
        raise AuthorityRegistrySafetyError("connected database is not TEST/CI-scoped")
    if identity.server_uuid != target.expected_server_uuid:
        raise AuthorityRegistrySafetyError(
            "connected MySQL server UUID differs from the independent expectation"
        )
    return identity


def _migration_checksum(statements: Sequence[str]) -> str:
    return hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()


def _expected_evidence_ledger() -> tuple[tuple[str, str], ...]:
    expected: list[tuple[str, str]] = []
    for migration in MIGRATIONS:
        version = str(migration["version"])
        parts = version.split("_", 2)
        if len(parts) < 2 or parts[1] not in {"011", "012", "013", "014", "015"}:
            continue
        statements = tuple(str(item) for item in migration["statements"])
        expected.append((version, _migration_checksum(statements)))
    declared = tuple(expected)
    if declared != FROZEN_EVIDENCE_LEDGER:
        raise AuthorityRegistrySafetyError(
            "code migrations 011 through 015 differ from the independently "
            "frozen authority-registry ledger"
        )
    return FROZEN_EVIDENCE_LEDGER


def _assert_exact_evidence_ledger(connection: Connection) -> tuple[str, ...]:
    expected = _expected_evidence_ledger()
    rows = connection.execute(
        text(
            "SELECT version, checksum FROM schema_migration_v2 "
            "WHERE version >= :binding_version ORDER BY version LOCK IN SHARE MODE"
        ),
        {"binding_version": expected[0][0]},
    ).mappings().all()
    observed = tuple(
        (str(row["version"]), str(row["checksum"])) for row in rows
    )
    if observed != expected:
        raise AuthorityRegistrySafetyError(
            "schema_migration_v2 does not exactly match migrations 011-015"
        )
    return tuple(version for version, _checksum_value in expected)


def _assert_schema_preflight(connection: Connection) -> tuple[str, ...]:
    report = inspect_v2_execution_evidence_schema(
        connection,
        require_guards=True,
        require_natural_keys=True,
        require_migration_ledger=True,
        require_authority_attestations=True,
        require_accounting_evidence=True,
        phase_scoped_migration_replay=False,
        maintenance_fence_expected_active=False,
        include_activation_blockers=True,
        canonical_hash_audit_passed=False,
    )
    if (
        not report.metadata_preflight_passed
        or not report.guards_checked
        or not report.migration_ledger_checked
        or not report.activation_checks_included
        or report.phase_scoped_migration_replay
        or not report.maintenance_fence_checked
        or report.maintenance_fence_active
        or report.production_activation_allowed
        or report.actionable_output_allowed
    ):
        raise AuthorityRegistrySafetyError(
            "V2 execution-evidence schema preflight failed: "
            + ",".join(report.structural_blockers or ("INVALID_REPORT",))
        )
    return report.structural_blockers


def _assert_authority_audit(connection: Connection, phase: str) -> Any:
    report = audit_v2_execution_evidence_authority_database(connection)
    if not report.audit_passed or report.production_activation_allowed:
        raise AuthorityRegistrySafetyError(
            f"authority registry {phase} stored-row audit failed"
        )
    return report


def _append_operation(connection: Connection, plan: OperationPlan) -> object:
    writer = _writer_api()
    functions = {
        OperationKind.TRUST_KEY: writer.append_authority_trust_key,
        OperationKind.RECEIPT: writer.append_authority_receipt,
        OperationKind.KEY_REVOCATION: writer.append_authority_key_revocation,
        OperationKind.RECEIPT_REVOCATION: writer.append_authority_receipt_revocation,
    }
    try:
        result = functions[plan.operation](connection, plan.request)
    except writer.AuthorityRegistryError as exc:
        raise AuthorityRegistryCliError(str(exc)) from exc
    if type(result) is not writer.AuthorityRegistryWriteResult:
        raise AuthorityRegistryCliError("authority registry writer returned an invalid result")
    if type(result.status) is not writer.AuthorityRegistryWriteStatus:
        raise AuthorityRegistryCliError("authority registry writer returned an invalid status")
    return result


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if type(value) is datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds")
        return _utc_text(value)
    if type(value) is date:
        return value.isoformat()
    if type(value) is bytes:
        return "<bytes omitted>"
    if type(value) is dict:
        return {
            str(key): _json_value(item)
            for key, item in value.items()
            if not re.search(
                r"(?:password|secret|private|public_key|signature|envelope_json)",
                str(key),
                re.IGNORECASE,
            )
        }
    if type(value) in {tuple, list}:
        return [_json_value(item) for item in value]
    if value is None or type(value) in {str, int, bool, float}:
        return value
    return str(value)


def _result_value(result: object) -> object:
    if is_dataclass(result) and not isinstance(result, type):
        return _json_value(asdict(result))
    return {"status": _json_value(getattr(result, "status", "UNKNOWN"))}


def _apply(
    plan: OperationPlan,
    target: ApplyTarget,
    *,
    engine_factory: Callable[..., Any] | None = None,
) -> dict[str, object]:
    factory = engine_factory or create_engine
    engine = factory(
        target.url,
        future=True,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        isolation_level="REPEATABLE READ",
    )
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                identity = _runtime_identity(connection, target)
                assert_v2_evidence_maintenance_fence_inactive(connection)
                ledger_versions = _assert_exact_evidence_ledger(connection)
                _assert_schema_preflight(connection)
                before = _assert_authority_audit(connection, "pre-write")
                result = _append_operation(connection, plan)
                after = _assert_authority_audit(connection, "post-write")
                transaction.commit()
            except BaseException:
                if transaction.is_active:
                    transaction.rollback()
                raise
    finally:
        engine.dispose()
    return {
        "mode": "APPLY",
        "environment": target.environment,
        "operation": plan.operation.value,
        "status": _json_value(getattr(result, "status", "UNKNOWN")),
        "result": _result_value(result),
        "database_name": identity.database_name,
        "server_version": identity.server_version,
        "server_uuid": identity.server_uuid,
        "migration_versions": list(ledger_versions),
        "pre_write_audit_rows": before.rows_reconstructed,
        "post_write_audit_rows": after.rows_reconstructed,
        "production_activation_allowed": False,
        "actionable_output_allowed": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation_file",
        help="UTF-8 JSON operation file, or '-' to read stdin",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write to a dedicated TEST/CI database after all fail-closed checks",
    )
    parser.add_argument(
        "--environment",
        type=str.upper,
        choices=tuple(_APPLY_ENVIRONMENT),
        help="required with --apply; never accepts a production environment",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if bool(args.apply) != (args.environment is not None):
            raise AuthorityRegistrySafetyError(
                "--apply and --environment TEST|CI must be supplied together"
            )
        plan = parse_operation(args.operation_file)
        if not args.apply:
            output: dict[str, object] = {
                "mode": "PREVIEW",
                "database_connection_attempted": False,
                "operation": plan.operation.value,
                "preview": dict(plan.preview),
                "production_activation_allowed": False,
                "actionable_output_allowed": False,
            }
        else:
            target = _resolve_apply_target(args.environment)
            output = _apply(plan, target)
    except AuthorityRegistryCliError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "production_activation_allowed": False,
                    "actionable_output_allowed": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:  # Fail closed without leaking URLs or credentials.
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": "authority registry operation failed closed",
                    "production_activation_allowed": False,
                    "actionable_output_allowed": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {"ok": True, **output},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
