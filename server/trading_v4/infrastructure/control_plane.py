"""Runtime-control persistence for the V4 control plane.

Job leasing is intentionally absent from this module.  The current
``st_job_run_v4`` schema has neither a persisted lease token nor a persisted
maximum-attempt contract, so an ABA-safe lease/retry implementation cannot be
provided without a forward-only migration.  Runtime controls, by contrast,
have the version and append-only transition identities needed for strict CAS.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import time
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError

from ..domain.hashes import freeze


PRODUCTION_ACTIVATION_ALLOWED = False
ACTIONABLE_OUTPUT_ALLOWED = False
PAPER_BUY_OUTBOX_STATE = "closed"


class RuntimeControlError(RuntimeError):
    """Base error for strict V4 runtime-control persistence."""


class RuntimeControlConflictError(RuntimeControlError):
    """The requested CAS no longer matches persisted state."""


class RuntimeControlIntegrityError(RuntimeControlError):
    """Persisted control or transition content failed exact validation."""


class RuntimeControlHardGateError(RuntimeControlError):
    """A runtime value attempted to relax a permanent safety gate."""


@dataclass(frozen=True)
class RuntimeControlSnapshot:
    control_key: str
    value: Any
    version: int
    updated_by: str
    reason: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class RuntimeControlTransition:
    transition_id: str
    control_key: str
    previous_value: Any | None
    next_value: Any
    next_version: int
    changed_by: str
    reason: str
    event_hash: str
    changed_at: datetime


@dataclass(frozen=True)
class RuntimeControlCASResult:
    """Outcome of one logical CAS command.

    ``superseded`` is true only when an old command hash is replayed after a
    later transition has already advanced the same control.  In that case the
    returned ``control`` is the current state while ``transition`` is the
    original, historically applied transition.  A replay at the transition's
    own version is idempotent but not superseded.
    """

    changed: bool
    control: RuntimeControlSnapshot
    transition: RuntimeControlTransition
    superseded: bool = False


_NORMALIZE_KEY_RE = re.compile(r"[^a-z0-9]+")
_PERMANENT_FALSE_ALIASES = frozenset(
    {
        "actionable_output_allowed",
        "actionable_output_enabled",
        "production_activation_allowed",
        "production_activation_enabled",
        "production_enabled",
        "real_order_allowed",
        "real_order_enabled",
        "real_orders_allowed",
        "real_orders_enabled",
        "real_trading_allowed",
        "real_trading_enabled",
        "v4_production_activation_allowed",
        "v4_production_activation_enabled",
        "v4_production_allowed",
        "v4_production_enabled",
    }
)
_PAPER_CLOSED_ALIASES = frozenset(
    {
        "paper_buy_allowed",
        "paper_buy_enabled",
        "paper_buy_outbox",
        "paper_buy_outbox_allowed",
        "paper_buy_outbox_enabled",
        "paper_buy_outbox_open",
        "paper_buy_outbox_state",
        "paper_outbox",
        "paper_outbox_allowed",
        "paper_outbox_enabled",
        "paper_outbox_open",
        "paper_outbox_state",
        "v4_paper_outbox_allowed",
        "v4_paper_outbox_enabled",
        "v4_paper_outbox_open",
        "v4_paper_outbox_state",
    }
)
_GATE_INDICATOR_KEYS = frozenset(
    {"allowed", "enabled", "mode", "open", "state", "status", "value"}
)
_DISABLED_STRINGS = frozenset(
    {
        "0",
        "block",
        "blocked",
        "closed",
        "deny",
        "denied",
        "disabled",
        "false",
        "off",
        "prohibited",
    }
)
def _normalize_key(value: str) -> str:
    return _NORMALIZE_KEY_RE.sub("_", value.strip().casefold()).strip("_")


def _canonical_gate_alias(value: str) -> str:
    """Canonicalize only for membership in the closed hard-gate alias sets.

    Removing separators closes case/camel/hyphen/underscore variants without
    using substring or suffix heuristics that would classify metadata keys
    such as ``production_activation_allowed_note`` as executable gates.
    """

    return _NORMALIZE_KEY_RE.sub("", value.strip().casefold())


_PERMANENT_FALSE_KEYS = frozenset(
    _canonical_gate_alias(value) for value in _PERMANENT_FALSE_ALIASES
)
_PAPER_CLOSED_KEYS = frozenset(
    _canonical_gate_alias(value) for value in _PAPER_CLOSED_ALIASES
)


def _required_text(value: Any, *, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return normalized


def _expected_version(value: Any) -> int:
    if type(value) is not int:
        raise TypeError("expected_version must be exactly int")
    if value < 0:
        raise ValueError("expected_version cannot be negative")
    return value


def _db_timestamp(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if type(current) is not datetime:
        raise TypeError("occurred_at must be exactly datetime or None")
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("runtime-control timestamps must be timezone-aware")
    return current.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_db_timestamp(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise RuntimeControlIntegrityError(
                f"persisted {field} is not a timestamp"
            ) from exc
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _public_timestamp(value: Any, *, field: str) -> datetime:
    return _parse_db_timestamp(value, field=field).replace(tzinfo=timezone.utc)


def _validate_json_value(value: Any, *, path: str = "value") -> None:
    if value is None or type(value) in {bool, str, int}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or infinity")
        return
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if type(key) is not str:
                raise TypeError(f"{path} JSON object keys must be strings")
            _validate_json_value(nested, path=f"{path}.{key}")
        return
    if type(value) is list:
        for index, nested in enumerate(value):
            _validate_json_value(nested, path=f"{path}[{index}]")
        return
    raise TypeError(f"{path} contains a non-JSON value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _decode_canonical_json(value: Any, *, field: str) -> Any:
    if type(value) is not str:
        raise RuntimeControlIntegrityError(f"persisted {field} must be text")
    try:
        decoded = json.loads(
            value,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"invalid JSON constant: {token}")
            ),
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeControlIntegrityError(
            f"persisted {field} is not strict JSON"
        ) from exc
    try:
        canonical = _canonical_json(decoded)
    except (TypeError, ValueError) as exc:
        raise RuntimeControlIntegrityError(
            f"persisted {field} contains unsupported JSON"
        ) from exc
    if canonical != value:
        raise RuntimeControlIntegrityError(
            f"persisted {field} is not canonical JSON"
        )
    return decoded


def _scalar_is_disabled(value: Any) -> bool:
    if value is False or (type(value) in {int, float} and value == 0):
        return True
    return bool(
        type(value) is str and value.strip().casefold() in _DISABLED_STRINGS
    )


def _gate_value_is_disabled(value: Any) -> bool:
    if _scalar_is_disabled(value):
        return True
    if not isinstance(value, Mapping):
        return False
    indicators = [
        nested
        for key, nested in value.items()
        if _normalize_key(key) in _GATE_INDICATOR_KEYS
    ]
    return bool(indicators) and all(_scalar_is_disabled(item) for item in indicators)


def _assert_hard_gates(control_key: str, value: Any) -> None:
    normalized_control_key = _normalize_key(control_key)
    canonical_control_key = _canonical_gate_alias(control_key)
    if canonical_control_key in _PERMANENT_FALSE_KEYS and not (
        _gate_value_is_disabled(value)
    ):
        raise RuntimeControlHardGateError(
            f"{normalized_control_key} is permanently false"
        )
    if canonical_control_key in _PAPER_CLOSED_KEYS and not (
        _gate_value_is_disabled(value)
    ):
        raise RuntimeControlHardGateError(
            f"{normalized_control_key} must remain closed"
        )

    def inspect(node: Any, path: str) -> None:
        if isinstance(node, Mapping):
            for key, nested in node.items():
                normalized = _normalize_key(key)
                canonical = _canonical_gate_alias(key)
                if canonical in _PERMANENT_FALSE_KEYS and not (
                    _gate_value_is_disabled(nested)
                ):
                    raise RuntimeControlHardGateError(
                        f"{path}.{key} is permanently false"
                    )
                if canonical in _PAPER_CLOSED_KEYS and not (
                    _gate_value_is_disabled(nested)
                ):
                    raise RuntimeControlHardGateError(
                        f"{path}.{key} must remain closed"
                    )
                inspect(nested, f"{path}.{key}")
        elif type(node) is list:
            for index, nested in enumerate(node):
                inspect(nested, f"{path}[{index}]")

    inspect(value, control_key)


def _event_hash(
    *,
    control_key: str,
    expected_version: int,
    next_value_json: str,
    changed_by: str,
    reason: str,
) -> str:
    payload = _canonical_json(
        {
            "changed_by": changed_by,
            "control_key": control_key,
            "expected_version": expected_version,
            "next_value_json": next_value_json,
            "reason": reason,
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_RUNTIME_CONTROL_MAX_ATTEMPTS = 4
_MYSQL_TRANSIENT_LOCK_CODES = frozenset({1205, 1213})
_TRANSIENT_LOCK_SQLSTATES = frozenset({"40001", "40P01"})
_TRANSIENT_LOCK_MESSAGES = (
    "database is locked",
    "database table is locked",
    "deadlock found",
    "lock wait timeout",
)


def _is_transient_lock_error(error: OperationalError) -> bool:
    """Recognize bounded-retry lock failures without masking other DB errors."""

    if not isinstance(error, OperationalError):
        return False
    original = getattr(error, "orig", None)
    candidates = (
        getattr(original, "errno", None),
        getattr(original, "sqlstate", None),
        getattr(original, "pgcode", None),
    )
    args = getattr(original, "args", ())
    if isinstance(args, tuple):
        candidates += args[:2]
    for value in candidates:
        if type(value) is int and value in _MYSQL_TRANSIENT_LOCK_CODES:
            return True
        normalized = str(value or "").strip().upper()
        if normalized in _TRANSIENT_LOCK_SQLSTATES:
            return True
        if normalized.isdigit() and int(normalized) in _MYSQL_TRANSIENT_LOCK_CODES:
            return True
    message = str(original or error).casefold()
    return any(marker in message for marker in _TRANSIENT_LOCK_MESSAGES)


class RuntimeControlRepository:
    """Versioned, append-only runtime-control CAS over an ``Engine``."""

    def __init__(self, engine: Engine):
        if not isinstance(engine, Engine):
            raise TypeError("engine must be a SQLAlchemy Engine")
        self.engine = engine

    @staticmethod
    def _for_update(connection: Connection) -> str:
        if connection.dialect.name.lower() in {"mysql", "mariadb"}:
            return " FOR UPDATE"
        return ""

    def _control(
        self,
        connection: Connection,
        control_key: str,
        *,
        for_update: bool = False,
    ) -> tuple[RuntimeControlSnapshot, dict[str, Any]] | None:
        suffix = self._for_update(connection) if for_update else ""
        row = connection.execute(
            text(
                "SELECT control_key, control_value_json, version, updated_by, "
                "reason, created_at, updated_at "
                "FROM st_runtime_control_v4 WHERE control_key = :control_key"
                + suffix
            ),
            {"control_key": control_key},
        ).mappings().first()
        if row is None:
            return None
        raw = dict(row)
        value = _decode_canonical_json(
            raw.get("control_value_json"),
            field="control_value_json",
        )
        stored_key = str(raw.get("control_key") or "")
        if stored_key != control_key:
            raise RuntimeControlIntegrityError(
                "runtime-control lookup returned a different control_key"
            )
        try:
            version = int(raw.get("version"))
        except (TypeError, ValueError) as exc:
            raise RuntimeControlIntegrityError(
                "persisted runtime-control version is invalid"
            ) from exc
        if version < 1:
            raise RuntimeControlIntegrityError(
                "persisted runtime-control version must be positive"
            )
        _assert_hard_gates(stored_key, value)
        snapshot = RuntimeControlSnapshot(
            control_key=stored_key,
            value=freeze(value),
            version=version,
            updated_by=str(raw.get("updated_by") or ""),
            reason=str(raw.get("reason") or ""),
            created_at=_public_timestamp(raw.get("created_at"), field="created_at"),
            updated_at=_public_timestamp(raw.get("updated_at"), field="updated_at"),
        )
        if not snapshot.updated_by or not snapshot.reason:
            raise RuntimeControlIntegrityError(
                "persisted runtime-control actor and reason are required"
            )
        if snapshot.updated_at < snapshot.created_at:
            raise RuntimeControlIntegrityError(
                "runtime-control updated_at precedes created_at"
            )
        return snapshot, raw

    def _transition(
        self,
        connection: Connection,
        event_hash: str,
    ) -> tuple[RuntimeControlTransition, dict[str, Any]] | None:
        row = connection.execute(
            text(
                "SELECT transition_id, control_key, previous_value_json, "
                "next_value_json, next_version, changed_by, reason, "
                "event_hash, changed_at "
                "FROM st_runtime_control_transition_v4 "
                "WHERE event_hash = :event_hash"
            ),
            {"event_hash": event_hash},
        ).mappings().first()
        if row is None:
            return None
        raw = dict(row)
        previous_raw = raw.get("previous_value_json")
        previous_value = (
            None
            if previous_raw is None
            else _decode_canonical_json(
                previous_raw,
                field="previous_value_json",
            )
        )
        next_value = _decode_canonical_json(
            raw.get("next_value_json"),
            field="next_value_json",
        )
        try:
            next_version = int(raw.get("next_version"))
        except (TypeError, ValueError) as exc:
            raise RuntimeControlIntegrityError(
                "persisted transition next_version is invalid"
            ) from exc
        transition = RuntimeControlTransition(
            transition_id=str(raw.get("transition_id") or ""),
            control_key=str(raw.get("control_key") or ""),
            previous_value=(
                None if previous_raw is None else freeze(previous_value)
            ),
            next_value=freeze(next_value),
            next_version=next_version,
            changed_by=str(raw.get("changed_by") or ""),
            reason=str(raw.get("reason") or ""),
            event_hash=str(raw.get("event_hash") or ""),
            changed_at=_public_timestamp(raw.get("changed_at"), field="changed_at"),
        )
        if transition.transition_id != transition.event_hash:
            raise RuntimeControlIntegrityError(
                "transition_id must equal its deterministic event_hash"
            )
        if transition.next_version < 1:
            raise RuntimeControlIntegrityError(
                "transition next_version must be positive"
            )
        _assert_hard_gates(transition.control_key, transition.next_value)
        return transition, raw

    def get_control(self, control_key: str) -> RuntimeControlSnapshot | None:
        key = _required_text(control_key, field="control_key", maximum=120)
        with self.engine.connect() as connection:
            stored = self._control(connection, key)
            return stored[0] if stored is not None else None

    def _resolve_replay(
        self,
        connection: Connection,
        *,
        event_hash: str,
        control_key: str,
        expected_version: int,
        next_value_json: str,
        changed_by: str,
        reason: str,
    ) -> RuntimeControlCASResult | None:
        stored_transition = self._transition(connection, event_hash)
        if stored_transition is None:
            return None
        transition, raw_transition = stored_transition
        expected = {
            "transition_id": event_hash,
            "control_key": control_key,
            "next_value_json": next_value_json,
            "next_version": expected_version + 1,
            "changed_by": changed_by,
            "reason": reason,
            "event_hash": event_hash,
        }
        conflicts = {
            field: (raw_transition.get(field), value)
            for field, value in expected.items()
            if (
                int(raw_transition.get(field)) != value
                if field == "next_version"
                else str(raw_transition.get(field) or "") != str(value)
            )
        }
        if conflicts:
            raise RuntimeControlIntegrityError(
                f"runtime-control event hash collision: {conflicts}"
            )
        stored_control = self._control(connection, control_key)
        if stored_control is None:
            raise RuntimeControlIntegrityError(
                "runtime-control transition exists without current state"
            )
        control, raw_control = stored_control
        if control.version < transition.next_version:
            raise RuntimeControlIntegrityError(
                "runtime-control version trails its applied transition"
            )
        if (
            control.version == transition.next_version
            and str(raw_control.get("control_value_json") or "")
            != next_value_json
        ):
            raise RuntimeControlIntegrityError(
                "runtime-control current value differs from its transition"
            )
        return RuntimeControlCASResult(
            changed=False,
            superseded=control.version > transition.next_version,
            control=control,
            transition=transition,
        )

    def _compare_and_set_once(
        self,
        control_key: str,
        *,
        expected_version: int,
        next_value: Any,
        changed_by: str,
        reason: str,
        occurred_at: datetime | None = None,
    ) -> RuntimeControlCASResult:
        """Run one transaction attempt for a normalized logical command.

        Version zero means the key must not yet exist.  The deterministic event
        identity excludes ``occurred_at`` so retrying the same logical command
        resolves to its already-applied transition instead of advancing twice.
        If later versions already exist, the replay result is explicitly
        marked ``superseded`` and never makes the old value current again.
        """

        key = _required_text(control_key, field="control_key", maximum=120)
        version = _expected_version(expected_version)
        actor = _required_text(changed_by, field="changed_by", maximum=120)
        normalized_reason = _required_text(reason, field="reason", maximum=500)
        next_json = _canonical_json(next_value)
        normalized_value = json.loads(next_json)
        _assert_hard_gates(key, normalized_value)
        changed_at = _db_timestamp(occurred_at)
        event_hash = _event_hash(
            control_key=key,
            expected_version=version,
            next_value_json=next_json,
            changed_by=actor,
            reason=normalized_reason,
        )

        try:
            with self.engine.begin() as connection:
                replay = self._resolve_replay(
                    connection,
                    event_hash=event_hash,
                    control_key=key,
                    expected_version=version,
                    next_value_json=next_json,
                    changed_by=actor,
                    reason=normalized_reason,
                )
                if replay is not None:
                    return replay

                stored = self._control(connection, key, for_update=True)
                if stored is None:
                    if version != 0:
                        raise RuntimeControlConflictError(
                            f"runtime control {key} does not exist; expected version {version}"
                        )
                    previous_json = None
                    next_version = 1
                else:
                    current, raw_current = stored
                    if current.version != version:
                        raise RuntimeControlConflictError(
                            f"runtime control {key} version changed: "
                            f"expected={version} actual={current.version}"
                        )
                    if current.updated_at.replace(tzinfo=None) > changed_at:
                        raise RuntimeControlConflictError(
                            f"runtime control {key} time would move backwards"
                        )
                    previous_json = str(raw_current["control_value_json"])
                    if previous_json == next_json:
                        raise RuntimeControlConflictError(
                            f"runtime control {key} update is a no-op"
                        )
                    next_version = version + 1

                # The database guards require transition-first ordering.  The
                # following current-state mutation either consumes this exact
                # transition in the same transaction or rolls it back.
                transition_result = connection.execute(
                    text(
                        "INSERT INTO st_runtime_control_transition_v4 ("
                        "transition_id, control_key, previous_value_json, "
                        "next_value_json, next_version, changed_by, reason, "
                        "event_hash, changed_at"
                        ") VALUES ("
                        ":transition_id, :control_key, :previous_value_json, "
                        ":next_value_json, :next_version, :changed_by, :reason, "
                        ":event_hash, :changed_at)"
                    ),
                    {
                        "transition_id": event_hash,
                        "control_key": key,
                        "previous_value_json": previous_json,
                        "next_value_json": next_json,
                        "next_version": next_version,
                        "changed_by": actor,
                        "reason": normalized_reason,
                        "event_hash": event_hash,
                        "changed_at": changed_at,
                    },
                )
                if int(transition_result.rowcount or 0) != 1:
                    raise RuntimeControlIntegrityError(
                        "runtime-control transition insert returned an unexpected row count"
                    )

                if stored is None:
                    result = connection.execute(
                        text(
                            "INSERT INTO st_runtime_control_v4 ("
                            "control_key, control_value_json, version, "
                            "updated_by, reason, created_at, updated_at"
                            ") VALUES ("
                            ":control_key, :control_value_json, :version, "
                            ":updated_by, :reason, :created_at, :updated_at)"
                        ),
                        {
                            "control_key": key,
                            "control_value_json": next_json,
                            "version": next_version,
                            "updated_by": actor,
                            "reason": normalized_reason,
                            "created_at": changed_at,
                            "updated_at": changed_at,
                        },
                    )
                else:
                    result = connection.execute(
                        text(
                            "UPDATE st_runtime_control_v4 SET "
                            "control_value_json = :control_value_json, "
                            "version = :next_version, updated_by = :updated_by, "
                            "reason = :reason, updated_at = :updated_at "
                            "WHERE control_key = :control_key "
                            "AND version = :expected_version"
                        ),
                        {
                            "control_key": key,
                            "control_value_json": next_json,
                            "next_version": next_version,
                            "expected_version": version,
                            "updated_by": actor,
                            "reason": normalized_reason,
                            "updated_at": changed_at,
                        },
                    )
                if int(result.rowcount or 0) != 1:
                    raise RuntimeControlConflictError(
                        f"runtime control {key} CAS affected an unexpected row count"
                    )

                stored_control = self._control(connection, key)
                stored_transition = self._transition(connection, event_hash)
                if stored_control is None or stored_transition is None:
                    raise RuntimeControlIntegrityError(
                        "runtime-control mutation could not be read back"
                    )
                control, raw_control = stored_control
                transition, raw_transition = stored_transition
                expected_control = {
                    "control_key": key,
                    "control_value_json": next_json,
                    "version": next_version,
                    "updated_by": actor,
                    "reason": normalized_reason,
                    "updated_at": changed_at,
                }
                control_conflicts = {
                    field: (raw_control.get(field), value)
                    for field, value in expected_control.items()
                    if (
                        _parse_db_timestamp(raw_control.get(field), field=field)
                        != value
                        if field == "updated_at"
                        else int(raw_control.get(field)) != value
                        if field == "version"
                        else str(raw_control.get(field) or "") != str(value)
                    )
                }
                expected_transition = {
                    "transition_id": event_hash,
                    "control_key": key,
                    "previous_value_json": previous_json,
                    "next_value_json": next_json,
                    "next_version": next_version,
                    "changed_by": actor,
                    "reason": normalized_reason,
                    "event_hash": event_hash,
                    "changed_at": changed_at,
                }
                transition_conflicts = {
                    field: (raw_transition.get(field), value)
                    for field, value in expected_transition.items()
                    if (
                        _parse_db_timestamp(raw_transition.get(field), field=field)
                        != value
                        if field == "changed_at"
                        else int(raw_transition.get(field)) != value
                        if field == "next_version"
                        else raw_transition.get(field) is not None
                        if value is None
                        else str(raw_transition.get(field) or "") != str(value)
                    )
                }
                if control_conflicts or transition_conflicts:
                    raise RuntimeControlIntegrityError(
                        "runtime-control exact read-back failed: "
                        f"control={control_conflicts} transition={transition_conflicts}"
                    )
                return RuntimeControlCASResult(
                    changed=True,
                    superseded=False,
                    control=control,
                    transition=transition,
                )
        except IntegrityError as exc:
            with self.engine.connect() as connection:
                replay = self._resolve_replay(
                    connection,
                    event_hash=event_hash,
                    control_key=key,
                    expected_version=version,
                    next_value_json=next_json,
                    changed_by=actor,
                    reason=normalized_reason,
                )
                if replay is not None:
                    return replay
            raise RuntimeControlConflictError(
                f"concurrent runtime-control mutation detected: {key}"
            ) from exc

    def _resolve_replay_after_rollback(
        self,
        *,
        event_hash: str,
        key: str,
        version: int,
        next_json: str,
        actor: str,
        normalized_reason: str,
    ) -> RuntimeControlCASResult | None:
        """Resolve an exact command only after entering a fresh transaction."""

        with self.engine.connect() as connection:
            return self._resolve_replay(
                connection,
                event_hash=event_hash,
                control_key=key,
                expected_version=version,
                next_value_json=next_json,
                changed_by=actor,
                reason=normalized_reason,
            )

    def compare_and_set_control(
        self,
        control_key: str,
        *,
        expected_version: int,
        next_value: Any,
        changed_by: str,
        reason: str,
        occurred_at: datetime | None = None,
    ) -> RuntimeControlCASResult:
        """Apply one logical command with exact replay and bounded lock retry.

        A stale-version or duplicate-key loser is accepted only when a fresh
        transaction finds the exact deterministic ``event_hash``.  Recognized
        deadlock/lock-timeout failures are retried with new transactions; an
        unrelated command never inherits another command's successful write.
        """

        key = _required_text(control_key, field="control_key", maximum=120)
        version = _expected_version(expected_version)
        actor = _required_text(changed_by, field="changed_by", maximum=120)
        normalized_reason = _required_text(reason, field="reason", maximum=500)
        next_json = _canonical_json(next_value)
        normalized_value = json.loads(next_json)
        _assert_hard_gates(key, normalized_value)
        fixed_time = _db_timestamp(occurred_at).replace(tzinfo=timezone.utc)
        event_hash = _event_hash(
            control_key=key,
            expected_version=version,
            next_value_json=next_json,
            changed_by=actor,
            reason=normalized_reason,
        )

        for attempt in range(_RUNTIME_CONTROL_MAX_ATTEMPTS):
            try:
                return self._compare_and_set_once(
                    key,
                    expected_version=version,
                    next_value=normalized_value,
                    changed_by=actor,
                    reason=normalized_reason,
                    occurred_at=fixed_time,
                )
            except RuntimeControlConflictError:
                replay = self._resolve_replay_after_rollback(
                    event_hash=event_hash,
                    key=key,
                    version=version,
                    next_json=next_json,
                    actor=actor,
                    normalized_reason=normalized_reason,
                )
                if replay is not None:
                    return replay
                raise
            except OperationalError as exc:
                if not _is_transient_lock_error(exc):
                    raise
                try:
                    replay = self._resolve_replay_after_rollback(
                        event_hash=event_hash,
                        key=key,
                        version=version,
                        next_json=next_json,
                        actor=actor,
                        normalized_reason=normalized_reason,
                    )
                except OperationalError as replay_error:
                    if not _is_transient_lock_error(replay_error):
                        raise
                    replay = None
                if replay is not None:
                    return replay
                if attempt + 1 >= _RUNTIME_CONTROL_MAX_ATTEMPTS:
                    raise RuntimeControlConflictError(
                        "runtime-control transient lock retry exhausted: "
                        f"{key} attempts={_RUNTIME_CONTROL_MAX_ATTEMPTS}"
                    ) from exc
                time.sleep(0.01 * (2**attempt))
        raise RuntimeControlConflictError(
            f"runtime-control retry state was exhausted unexpectedly: {key}"
        )


__all__ = (
    "ACTIONABLE_OUTPUT_ALLOWED",
    "PAPER_BUY_OUTBOX_STATE",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "RuntimeControlCASResult",
    "RuntimeControlConflictError",
    "RuntimeControlError",
    "RuntimeControlHardGateError",
    "RuntimeControlIntegrityError",
    "RuntimeControlRepository",
    "RuntimeControlSnapshot",
    "RuntimeControlTransition",
)
