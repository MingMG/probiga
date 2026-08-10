from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from ..domain import DecisionClock, DecisionContext, QualityStatus


RUN_CREATED = "CREATED"
RUN_RUNNING = "RUNNING"
RUN_VALIDATING = "VALIDATING"
RUN_COMMITTED = "COMMITTED"
RUN_FAILED = "FAILED"
RUN_CANCELLED = "CANCELLED"

TERMINAL_RUN_STATUSES = frozenset(
    {RUN_COMMITTED, RUN_FAILED, RUN_CANCELLED}
)
ALLOWED_RUN_TRANSITIONS: Mapping[str, frozenset[str]] = {
    RUN_CREATED: frozenset({RUN_RUNNING, RUN_FAILED, RUN_CANCELLED}),
    RUN_RUNNING: frozenset(
        {RUN_VALIDATING, RUN_FAILED, RUN_CANCELLED}
    ),
    RUN_VALIDATING: frozenset(
        {RUN_COMMITTED, RUN_FAILED, RUN_CANCELLED}
    ),
    RUN_COMMITTED: frozenset(),
    RUN_FAILED: frozenset(),
    RUN_CANCELLED: frozenset(),
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_IMMUTABLE_FIELDS = (
    "context_id",
    "account_id",
    "channel",
    "run_type",
    "trigger_type",
    "trigger_ref_id",
    "parent_run_uid",
    "model_set_version",
    "config_version",
    "code_commit_sha",
)
_CONTEXT_IMMUTABLE_FIELDS = (
    "context_id",
    "trade_date",
    "decision_at",
    "knowledge_cutoff_at",
    "decision_clock",
    "feature_as_of",
    "universe_version",
    "account_snapshot_id",
    "run_mode",
    "is_realtime",
    "freshness_status",
    "fallback_used",
    "data_manifest_json",
    "source_manifest_json",
    "quality_json",
    "factor_spec_versions_json",
    "forecast_contract_ids_json",
    "model_versions_json",
    "model_artifact_hashes_json",
    "model_training_cutoffs_json",
    "model_available_at_json",
    "calibration_versions_json",
    "calibration_artifact_hashes_json",
    "calibration_training_cutoffs_json",
    "calibration_available_at_json",
    "capability_statuses_json",
    "context_json",
    "data_snapshot_hash",
    "context_hash",
    "feature_version",
    "model_set_version",
    "config_version",
    "portfolio_policy_version",
    "execution_contract_version",
    "fee_schedule_version",
    "code_commit_sha",
    "random_seed",
)
_WATERMARK_IMMUTABLE_FIELDS = (
    "context_id",
    "source_key",
    "knowledge_time",
    "source_event_at",
    "first_seen_at",
    "received_at",
    "available_at",
    "record_count",
    "snapshot_id",
    "coverage",
    "lag_seconds",
    "batch_id",
    "schema_version",
    "content_hash",
    "quality_status",
    "details_json",
    "created_at",
)


class V4RepositoryError(RuntimeError):
    pass


class DecisionContextNotFoundError(V4RepositoryError):
    pass


class DecisionRunNotFoundError(V4RepositoryError):
    pass


class DecisionRunConflictError(V4RepositoryError):
    pass


class InvalidRunTransitionError(V4RepositoryError):
    pass


class HeadPublishError(V4RepositoryError):
    pass


class HeadPublishConflictError(HeadPublishError):
    pass


@dataclass(frozen=True)
class CreateDecisionRunResult:
    created: bool
    run: dict[str, Any]


@dataclass(frozen=True)
class CreateDecisionContextResult:
    created: bool
    context: dict[str, Any]


@dataclass(frozen=True)
class HeadPublishResult:
    changed: bool
    previous_run_uid: str | None
    head: dict[str, Any]


@dataclass(frozen=True)
class CommitAndPublishResult:
    run: dict[str, Any]
    publication: HeadPublishResult


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: str, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return normalized


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _optional_text(value: Any, *, field: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string or None")
    return value.strip()


def _timestamp(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("repository timestamps must be timezone-aware")
    return current.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _persisted_value_matches(stored: Any, expected: Any) -> bool:
    """Compare SQLite/MySQL values without weakening timestamp semantics."""

    if expected is None:
        return stored is None
    if isinstance(expected, datetime):
        return _parse_timestamp(stored) == expected
    if isinstance(expected, date):
        if isinstance(stored, datetime):
            return stored.date() == expected
        if isinstance(stored, date):
            return stored == expected
        return str(stored) == expected.isoformat()
    if isinstance(expected, int):
        try:
            return int(stored) == expected
        except (TypeError, ValueError):
            return False
    if isinstance(expected, Decimal):
        try:
            return Decimal(str(stored)) == expected
        except (InvalidOperation, TypeError, ValueError):
            return False
    if isinstance(expected, float):
        try:
            return Decimal(str(stored)) == Decimal(str(expected))
        except (InvalidOperation, TypeError, ValueError):
            return False
    return str(stored) == str(expected)


def _is_duplicate_key_error(
    error: IntegrityError,
    *,
    dialect_name: str,
) -> bool:
    original = getattr(error, "orig", None)
    arguments = getattr(original, "args", ())
    if dialect_name in {"mysql", "mariadb"}:
        return bool(arguments) and arguments[0] == 1062
    if dialect_name == "sqlite":
        message = str(original or error).casefold()
        return (
            "unique constraint failed" in message
            or "primary key" in message
        )
    return False


def _idempotent_insert(
    connection: Connection,
    statement: str,
    parameters: Mapping[str, Any],
) -> bool:
    """Insert once while ignoring duplicate keys only, never other warnings."""

    try:
        result = connection.execute(text(statement), dict(parameters))
    except IntegrityError as exc:
        if _is_duplicate_key_error(
            exc,
            dialect_name=connection.dialect.name.lower(),
        ):
            return False
        raise
    if int(result.rowcount or 0) != 1:
        raise DecisionRunConflictError(
            "idempotent insert returned an unexpected row count"
        )
    return True


def build_run_idempotency_key(
    *,
    context_id: str,
    account_id: str = "",
    channel: str,
    run_type: str,
    trigger_type: str,
    trigger_ref_id: str = "",
    parent_run_uid: str = "",
    model_set_version: str,
    config_version: str,
    code_commit_sha: str,
) -> str:
    """Build the immutable identity of one logical V4 decision run."""

    return _sha256(
        {
            "account_id": _optional_text(account_id, field="account_id"),
            "channel": _required_text(channel, field="channel"),
            "code_commit_sha": _required_text(
                code_commit_sha,
                field="code_commit_sha",
            ),
            "config_version": _required_text(
                config_version,
                field="config_version",
            ),
            "context_id": _required_text(context_id, field="context_id"),
            "model_set_version": _required_text(
                model_set_version,
                field="model_set_version",
            ),
            "parent_run_uid": _optional_text(
                parent_run_uid,
                field="parent_run_uid",
            ),
            "run_type": _required_text(run_type, field="run_type"),
            "trigger_ref_id": _optional_text(
                trigger_ref_id,
                field="trigger_ref_id",
            ),
            "trigger_type": _required_text(
                trigger_type,
                field="trigger_type",
            ),
        }
    )


class TradingV4Repository:
    """Persistence boundary for V4 run lifecycle and atomic publication.

    Infrastructure depends inward on immutable V4 contracts; the pure domain
    never depends outward on this repository.  Only a ``COMMITTED`` run may
    become a channel head.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    @staticmethod
    def _for_update(connection: Connection) -> str:
        return (
            " FOR UPDATE"
            if connection.dialect.name.lower() in {"mysql", "mariadb"}
            else ""
        )

    def _context(
        self,
        connection: Connection,
        context_id: str,
        *,
        for_update: bool = False,
    ) -> dict[str, Any] | None:
        suffix = self._for_update(connection) if for_update else ""
        row = connection.execute(
            text(
                """
                SELECT context_id, trade_date, decision_at,
                       knowledge_cutoff_at, decision_clock, feature_as_of,
                       universe_version, account_snapshot_id, run_mode,
                       is_realtime, freshness_status, fallback_used,
                       data_manifest_json, source_manifest_json, quality_json,
                       factor_spec_versions_json,
                       forecast_contract_ids_json, model_versions_json,
                       model_artifact_hashes_json,
                       model_training_cutoffs_json,
                       model_available_at_json,
                       calibration_versions_json,
                       calibration_artifact_hashes_json,
                       calibration_training_cutoffs_json,
                       calibration_available_at_json,
                       capability_statuses_json, context_json,
                       context_hash, data_snapshot_hash, feature_version,
                       model_set_version, config_version,
                       portfolio_policy_version,
                       execution_contract_version, fee_schedule_version,
                       code_commit_sha, random_seed, created_at
                FROM st_decision_context_v4
                WHERE context_id = :context_id
                """
                + suffix
            ),
            {"context_id": context_id},
        ).mappings().first()
        return dict(row) if row else None

    def _run(
        self,
        connection: Connection,
        run_uid: str,
        *,
        for_update: bool = False,
    ) -> dict[str, Any] | None:
        suffix = self._for_update(connection) if for_update else ""
        row = connection.execute(
            text(
                """
                SELECT r.*,
                       c.decision_at AS context_decision_at,
                       c.freshness_status AS context_freshness_status,
                       c.context_hash AS context_hash
                FROM st_decision_run_v4 r
                JOIN st_decision_context_v4 c
                  ON c.context_id = r.context_id
                WHERE r.run_uid = :run_uid
                """
                + suffix
            ),
            {"run_uid": run_uid},
        ).mappings().first()
        return dict(row) if row else None

    def _run_by_idempotency_key(
        self,
        connection: Connection,
        key: str,
    ) -> dict[str, Any] | None:
        row = connection.execute(
            text(
                """
                SELECT run_uid
                FROM st_decision_run_v4
                WHERE run_idempotency_key = :key
                """
            ),
            {"key": key},
        ).mappings().first()
        return self._run(connection, str(row["run_uid"])) if row else None

    def get_run(self, run_uid: str) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            return self._run(connection, run_uid)

    def create_or_get_context(
        self,
        context: DecisionContext,
        *,
        freshness_status: QualityStatus | str = QualityStatus.PASS,
        run_mode: str | None = None,
        is_realtime: bool | None = None,
        fallback_used: bool = False,
        feature_as_of: date | None = None,
        created_at: datetime | None = None,
    ) -> CreateDecisionContextResult:
        """Persist one immutable clean-room context and its watermarks.

        Retries return the existing row only when its deterministic context
        hash agrees.  No context or watermark is updated in place.
        """

        if type(context) is not DecisionContext:
            raise TypeError("context must be exactly DecisionContext")
        payload = context.as_dict()
        freshness = QualityStatus(freshness_status).value
        clock = DecisionClock(context.decision_clock)
        effective_mode = (
            clock.value
            if run_mode is None
            else _required_text(run_mode, field="run_mode")
        )
        effective_feature_as_of = feature_as_of or context.trade_date
        if not isinstance(effective_feature_as_of, date) or isinstance(
            effective_feature_as_of,
            datetime,
        ):
            raise TypeError("feature_as_of must be a date")
        if effective_feature_as_of > context.trade_date:
            raise ValueError("feature_as_of cannot follow trade_date")
        realtime = (
            clock in {DecisionClock.INTRADAY, DecisionClock.EVENT_DRIVEN}
            if is_realtime is None
            else bool(is_realtime)
        )
        factor_versions = dict(payload["factor_spec_versions"])
        model_versions = dict(payload["model_versions"])
        model_artifact_hashes = dict(payload["model_artifact_hashes"])
        model_training_cutoffs = dict(payload["model_training_cutoffs"])
        model_available_at = dict(payload["model_available_at"])
        calibration_versions = dict(payload["calibration_versions"])
        calibration_artifact_hashes = dict(
            payload["calibration_artifact_hashes"]
        )
        calibration_training_cutoffs = dict(
            payload["calibration_training_cutoffs"]
        )
        calibration_available_at = dict(
            payload["calibration_available_at"]
        )
        source_manifest = dict(payload["source_watermarks"])
        capabilities = dict(payload["capability_statuses"])
        feature_version = _sha256(factor_versions)
        model_set_version = _sha256(
            {
                "model_versions": model_versions,
                "model_artifact_hashes": model_artifact_hashes,
                "model_training_cutoffs": model_training_cutoffs,
                "model_available_at": model_available_at,
                "calibration_versions": calibration_versions,
                "calibration_artifact_hashes": (
                    calibration_artifact_hashes
                ),
                "calibration_training_cutoffs": (
                    calibration_training_cutoffs
                ),
                "calibration_available_at": calibration_available_at,
            }
        )
        now = _timestamp(created_at)
        values = {
            "context_id": context.context_id,
            "trade_date": context.trade_date,
            "decision_at": _timestamp(context.decision_time),
            "knowledge_cutoff_at": _timestamp(context.knowledge_cutoff),
            "decision_clock": clock.value,
            "feature_as_of": effective_feature_as_of,
            "universe_version": context.universe_version,
            "account_snapshot_id": context.account_snapshot_id,
            "run_mode": effective_mode,
            "is_realtime": int(realtime),
            "freshness_status": freshness,
            "fallback_used": int(bool(fallback_used)),
            "data_manifest_json": _canonical_json(payload["data_manifest"]),
            "source_manifest_json": _canonical_json(source_manifest),
            "quality_json": _canonical_json(
                {
                    "freshness_status": freshness,
                    "capability_statuses": capabilities,
                }
            ),
            "factor_spec_versions_json": _canonical_json(factor_versions),
            "forecast_contract_ids_json": _canonical_json(
                payload["forecast_contract_ids"]
            ),
            "model_versions_json": _canonical_json(model_versions),
            "model_artifact_hashes_json": _canonical_json(
                model_artifact_hashes
            ),
            "model_training_cutoffs_json": _canonical_json(
                model_training_cutoffs
            ),
            "model_available_at_json": _canonical_json(model_available_at),
            "calibration_versions_json": _canonical_json(
                calibration_versions
            ),
            "calibration_artifact_hashes_json": _canonical_json(
                calibration_artifact_hashes
            ),
            "calibration_training_cutoffs_json": _canonical_json(
                calibration_training_cutoffs
            ),
            "calibration_available_at_json": _canonical_json(
                calibration_available_at
            ),
            "capability_statuses_json": _canonical_json(capabilities),
            "context_json": _canonical_json(payload),
            "data_snapshot_hash": _require_sha256(
                context.raw_data_manifest_hash,
                field="raw_data_manifest_hash",
            ),
            "context_hash": _require_sha256(
                context.context_hash,
                field="context_hash",
            ),
            "feature_version": feature_version,
            "model_set_version": model_set_version,
            "config_version": context.config_hash,
            "portfolio_policy_version": context.portfolio_policy_version,
            "execution_contract_version": context.execution_contract_version,
            "fee_schedule_version": context.fee_schedule_version,
            "code_commit_sha": context.code_commit_sha,
            "random_seed": context.random_seed,
            "created_at": now,
        }
        with self.engine.begin() as connection:
            created = _idempotent_insert(
                connection,
                """
                    INSERT INTO st_decision_context_v4 (
                        context_id, trade_date, decision_at,
                        knowledge_cutoff_at, decision_clock, feature_as_of,
                        universe_version, account_snapshot_id, run_mode,
                        is_realtime, freshness_status, fallback_used,
                        data_manifest_json, source_manifest_json, quality_json,
                        factor_spec_versions_json,
                        forecast_contract_ids_json, model_versions_json,
                        model_artifact_hashes_json,
                        model_training_cutoffs_json,
                        model_available_at_json,
                        calibration_versions_json,
                        calibration_artifact_hashes_json,
                        calibration_training_cutoffs_json,
                        calibration_available_at_json,
                        capability_statuses_json, context_json,
                        data_snapshot_hash, context_hash, feature_version,
                        model_set_version, config_version,
                        portfolio_policy_version,
                        execution_contract_version, fee_schedule_version,
                        code_commit_sha, random_seed, created_at
                    ) VALUES (
                        :context_id, :trade_date, :decision_at,
                        :knowledge_cutoff_at, :decision_clock,
                        :feature_as_of, :universe_version,
                        :account_snapshot_id, :run_mode, :is_realtime,
                        :freshness_status, :fallback_used,
                        :data_manifest_json, :source_manifest_json,
                        :quality_json,
                        :factor_spec_versions_json,
                        :forecast_contract_ids_json, :model_versions_json,
                        :model_artifact_hashes_json,
                        :model_training_cutoffs_json,
                        :model_available_at_json,
                        :calibration_versions_json,
                        :calibration_artifact_hashes_json,
                        :calibration_training_cutoffs_json,
                        :calibration_available_at_json,
                        :capability_statuses_json, :context_json,
                        :data_snapshot_hash, :context_hash,
                        :feature_version, :model_set_version,
                        :config_version, :portfolio_policy_version,
                        :execution_contract_version, :fee_schedule_version,
                        :code_commit_sha, :random_seed, :created_at
                    )
                """,
                values,
            )
            stored = self._context(connection, context.context_id)
            if stored is None:
                collision = connection.execute(
                    text(
                        "SELECT context_id FROM st_decision_context_v4 "
                        "WHERE context_hash = :context_hash"
                    ),
                    {"context_hash": context.context_hash},
                ).scalar()
                if collision:
                    raise DecisionRunConflictError(
                        "context hash belongs to another context id: "
                        f"{collision}"
                    )
                raise DecisionContextNotFoundError(
                    f"unable to persist decision context: {context.context_id}"
                )
            if str(stored["context_hash"]) != context.context_hash:
                raise DecisionRunConflictError(
                    "context id was reused with different immutable content: "
                    f"{context.context_id}"
                )
            conflicts = {
                field: (stored.get(field), values[field])
                for field in _CONTEXT_IMMUTABLE_FIELDS
                if not _persisted_value_matches(
                    stored.get(field),
                    values[field],
                )
            }
            if conflicts:
                raise DecisionRunConflictError(
                    "context retry changed persisted immutable metadata: "
                    f"{conflicts}"
                )

            context_created_at = _parse_timestamp(stored.get("created_at"))
            if context_created_at is None:
                raise DecisionRunConflictError(
                    "stored decision context has an invalid created_at: "
                    f"{context.context_id}"
                )
            for source_key, watermark in context.source_watermarks.items():
                watermark_at = _timestamp(watermark.knowledge_time)
                lag_seconds = max(
                    0,
                    int(
                        (
                            context.knowledge_cutoff
                            - watermark.knowledge_time
                        ).total_seconds()
                    ),
                )
                watermark_payload = watermark.as_dict()
                content_hash = (
                    watermark.content_hash
                    if watermark.content_hash
                    else _sha256(watermark_payload)
                )
                watermark_values = {
                    "context_id": context.context_id,
                    "source_key": source_key,
                    "knowledge_time": watermark_at,
                    "source_event_at": (
                        _timestamp(watermark.source_event_at)
                        if watermark.source_event_at is not None
                        else None
                    ),
                    "first_seen_at": (
                        _timestamp(watermark.first_seen_at)
                        if watermark.first_seen_at is not None
                        else None
                    ),
                    "received_at": (
                        _timestamp(watermark.received_at)
                        if watermark.received_at is not None
                        else None
                    ),
                    "available_at": (
                        _timestamp(watermark.available_at)
                        if watermark.available_at is not None
                        else watermark_at
                    ),
                    "record_count": watermark.record_count,
                    "snapshot_id": watermark.snapshot_id,
                    "coverage": (
                        float(watermark.coverage)
                        if watermark.coverage is not None
                        else None
                    ),
                    "lag_seconds": lag_seconds,
                    "batch_id": watermark.batch_id or watermark.snapshot_id,
                    "schema_version": watermark.schema_version,
                    "content_hash": content_hash,
                    "quality_status": watermark.quality_status.value,
                    "details_json": _canonical_json(watermark_payload),
                    "created_at": context_created_at,
                }
                _idempotent_insert(
                    connection,
                    """
                        INSERT INTO st_source_watermark_v4 (
                            context_id, source_key, knowledge_time,
                            source_event_at, first_seen_at, received_at,
                            available_at, record_count, snapshot_id, coverage,
                            lag_seconds, batch_id, schema_version,
                            content_hash, quality_status, details_json,
                            created_at
                        ) VALUES (
                            :context_id, :source_key, :knowledge_time,
                            :source_event_at, :first_seen_at, :received_at,
                            :available_at, :record_count, :snapshot_id,
                            :coverage, :lag_seconds, :batch_id,
                            :schema_version, :content_hash, :quality_status,
                            :details_json, :created_at
                        )
                    """,
                    watermark_values,
                )
                stored_watermark = connection.execute(
                    text(
                        "SELECT context_id, source_key, knowledge_time, "
                        "source_event_at, first_seen_at, received_at, "
                        "available_at, record_count, snapshot_id, coverage, "
                        "lag_seconds, batch_id, schema_version, content_hash, "
                        "quality_status, details_json, created_at "
                        "FROM st_source_watermark_v4 "
                        "WHERE context_id = :context_id "
                        "AND source_key = :source_key"
                    ),
                    {
                        "context_id": context.context_id,
                        "source_key": source_key,
                    },
                ).mappings().first()
                if stored_watermark is None:
                    raise DecisionRunConflictError(
                        "source watermark could not be read after insert: "
                        f"{context.context_id}/{source_key}"
                    )
                watermark_conflicts = {
                    field: (stored_watermark.get(field), watermark_values[field])
                    for field in _WATERMARK_IMMUTABLE_FIELDS
                    if not _persisted_value_matches(
                        stored_watermark.get(field),
                        watermark_values[field],
                    )
                }
                if watermark_conflicts:
                    raise DecisionRunConflictError(
                        "source watermark retry changed immutable fields: "
                        f"{context.context_id}/{source_key}: "
                        f"{watermark_conflicts}"
                    )
            stored_source_keys = {
                str(value)
                for value in connection.execute(
                    text(
                        "SELECT source_key FROM st_source_watermark_v4 "
                        "WHERE context_id = :context_id"
                    ),
                    {"context_id": context.context_id},
                ).scalars()
            }
            expected_source_keys = set(context.source_watermarks)
            if stored_source_keys != expected_source_keys:
                raise DecisionRunConflictError(
                    "stored source watermark set differs from context: "
                    f"expected={sorted(expected_source_keys)} "
                    f"actual={sorted(stored_source_keys)}"
                )
            return CreateDecisionContextResult(
                created=created,
                context=stored,
            )

    def create_or_get_run(
        self,
        *,
        context_id: str,
        account_id: str = "",
        channel: str,
        run_type: str,
        trigger_type: str,
        trigger_ref_id: str = "",
        parent_run_uid: str = "",
        model_set_version: str,
        config_version: str,
        code_commit_sha: str,
        run_idempotency_key: str | None = None,
        run_uid: str | None = None,
        created_at: datetime | None = None,
    ) -> CreateDecisionRunResult:
        values = {
            "context_id": _required_text(context_id, field="context_id"),
            "account_id": _optional_text(account_id, field="account_id"),
            "channel": _required_text(channel, field="channel"),
            "run_type": _required_text(run_type, field="run_type"),
            "trigger_type": _required_text(trigger_type, field="trigger_type"),
            "trigger_ref_id": _optional_text(
                trigger_ref_id,
                field="trigger_ref_id",
            ),
            "parent_run_uid": _optional_text(
                parent_run_uid,
                field="parent_run_uid",
            ),
            "model_set_version": _required_text(
                model_set_version,
                field="model_set_version",
            ),
            "config_version": _required_text(
                config_version,
                field="config_version",
            ),
            "code_commit_sha": _required_text(
                code_commit_sha,
                field="code_commit_sha",
            ),
        }
        expected_key = build_run_idempotency_key(**values)
        if run_idempotency_key is not None and (
            _require_sha256(
                run_idempotency_key,
                field="run_idempotency_key",
            )
            != expected_key
        ):
            raise DecisionRunConflictError(
                "run_idempotency_key does not match run inputs"
            )
        key = expected_key
        candidate_uid = _required_text(
            run_uid or uuid.uuid4().hex,
            field="run_uid",
        )
        now = _timestamp(created_at)

        with self.engine.begin() as connection:
            stored_context = self._context(
                connection,
                values["context_id"],
            )
            if stored_context is None:
                raise DecisionContextNotFoundError(
                    f"decision context not found: {values['context_id']}"
                )
            context_run_fields = {
                "model_set_version": stored_context["model_set_version"],
                "config_version": stored_context["config_version"],
                "code_commit_sha": stored_context["code_commit_sha"],
            }
            context_conflicts = {
                field: (context_run_fields[field], values[field])
                for field in context_run_fields
                if str(context_run_fields[field]) != str(values[field])
            }
            if context_conflicts:
                raise DecisionRunConflictError(
                    "run versions do not match the immutable context: "
                    f"{context_conflicts}"
                )
            if values["parent_run_uid"]:
                parent = self._run(connection, values["parent_run_uid"])
                if parent is None:
                    raise DecisionRunNotFoundError(
                        "parent decision run not found: "
                        f"{values['parent_run_uid']}"
                    )
                if (
                    str(parent["account_id"]) != values["account_id"]
                    or str(parent["channel"]) != values["channel"]
                ):
                    raise DecisionRunConflictError(
                        "parent run must use the same account and channel"
                    )
            created = _idempotent_insert(
                connection,
                """
                    INSERT INTO st_decision_run_v4 (
                        run_uid, run_idempotency_key, context_id,
                        account_id, channel, run_type, trigger_type,
                        trigger_ref_id, parent_run_uid, status,
                        model_set_version, config_version, code_commit_sha,
                        created_at, updated_at
                    ) VALUES (
                        :run_uid, :run_idempotency_key, :context_id,
                        :account_id, :channel, :run_type, :trigger_type,
                        :trigger_ref_id, :parent_run_uid, :status,
                        :model_set_version, :config_version,
                        :code_commit_sha, :created_at, :updated_at
                    )
                """,
                {
                    **values,
                    "run_uid": candidate_uid,
                    "run_idempotency_key": key,
                    "status": RUN_CREATED,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            stored = self._run_by_idempotency_key(connection, key)
            if stored is None:
                by_uid = self._run(connection, candidate_uid)
                if by_uid is not None:
                    raise DecisionRunConflictError(
                        "run_uid already belongs to another idempotency key: "
                        f"{candidate_uid}"
                    )
                raise DecisionRunConflictError(
                    f"unable to resolve idempotent run: {key}"
                )
            conflicts = {
                field: (stored.get(field), values[field])
                for field in _RUN_IMMUTABLE_FIELDS
                if str(stored.get(field) or "") != str(values[field] or "")
            }
            if conflicts:
                raise DecisionRunConflictError(
                    "idempotency key was reused with different immutable "
                    f"inputs: {conflicts}"
                )
            return CreateDecisionRunResult(created=created, run=stored)

    def _transition_run(
        self,
        connection: Connection,
        run_uid: str,
        *,
        next_status: str,
        expected_status: str | None,
        occurred_at: datetime,
        result_hash: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        current = self._run(connection, run_uid, for_update=True)
        if current is None:
            raise DecisionRunNotFoundError(f"decision run not found: {run_uid}")
        current_status = str(current["status"])
        if expected_status is not None and current_status != expected_status:
            raise InvalidRunTransitionError(
                f"expected {expected_status}, found {current_status}: {run_uid}"
            )
        last_updated = _parse_timestamp(current.get("updated_at"))
        if last_updated is None:
            raise InvalidRunTransitionError(
                f"decision run has an invalid updated_at: {run_uid}"
            )
        if occurred_at < last_updated:
            raise InvalidRunTransitionError(
                "decision run transitions must be time-monotonic: "
                f"{run_uid}"
            )
        allowed = ALLOWED_RUN_TRANSITIONS.get(current_status, frozenset())
        if next_status not in allowed:
            raise InvalidRunTransitionError(
                f"invalid V4 run transition {current_status} -> "
                f"{next_status}: {run_uid}"
            )

        assignments = ["status = :next_status", "updated_at = :occurred_at"]
        params: dict[str, Any] = {
            "run_uid": run_uid,
            "current_status": current_status,
            "next_status": next_status,
            "occurred_at": occurred_at,
        }
        if next_status == RUN_RUNNING:
            assignments.append(
                "started_at = COALESCE(started_at, :occurred_at)"
            )
        elif next_status == RUN_VALIDATING:
            assignments.append("validated_at = :occurred_at")
        elif next_status == RUN_COMMITTED:
            if str(current.get("context_freshness_status") or "") == "FAIL":
                raise InvalidRunTransitionError(
                    "a FAIL decision context cannot be committed: "
                    f"{current['context_id']}"
                )
            params["result_hash"] = _require_sha256(
                str(result_hash or ""), field="result_hash"
            )
            assignments.extend(
                [
                    "result_hash = :result_hash",
                    "committed_at = :occurred_at",
                    "finished_at = :occurred_at",
                ]
            )
        elif next_status in {RUN_FAILED, RUN_CANCELLED}:
            if next_status == RUN_FAILED and not str(error_code or "").strip():
                raise InvalidRunTransitionError(
                    "FAILED transition requires an error_code"
                )
            params["error_code"] = str(error_code or next_status)
            params["error_message"] = str(error_message or "")[:1000]
            assignments.extend(
                [
                    "error_code = :error_code",
                    "error_message = :error_message",
                    "finished_at = :occurred_at",
                ]
            )

        result = connection.execute(
            text(
                "UPDATE st_decision_run_v4 SET "
                + ", ".join(assignments)
                + " WHERE run_uid = :run_uid AND status = :current_status"
            ),
            params,
        )
        if int(result.rowcount or 0) != 1:
            raise DecisionRunConflictError(
                f"concurrent decision run transition detected: {run_uid}"
            )
        updated = self._run(connection, run_uid)
        if updated is None:
            raise DecisionRunNotFoundError(f"decision run not found: {run_uid}")
        return updated

    def transition_run(
        self,
        run_uid: str,
        *,
        next_status: str,
        expected_status: str | None = None,
        occurred_at: datetime | None = None,
        result_hash: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        with self.engine.begin() as connection:
            return self._transition_run(
                connection,
                run_uid,
                next_status=next_status,
                expected_status=expected_status,
                occurred_at=_timestamp(occurred_at),
                result_hash=result_hash,
                error_code=error_code,
                error_message=error_message,
            )

    def mark_running(
        self, run_uid: str, *, occurred_at: datetime | None = None
    ) -> dict[str, Any]:
        return self.transition_run(
            run_uid,
            next_status=RUN_RUNNING,
            expected_status=RUN_CREATED,
            occurred_at=occurred_at,
        )

    def mark_validating(
        self, run_uid: str, *, occurred_at: datetime | None = None
    ) -> dict[str, Any]:
        return self.transition_run(
            run_uid,
            next_status=RUN_VALIDATING,
            expected_status=RUN_RUNNING,
            occurred_at=occurred_at,
        )

    def commit_run(
        self,
        run_uid: str,
        *,
        result_hash: str,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        return self.transition_run(
            run_uid,
            next_status=RUN_COMMITTED,
            expected_status=RUN_VALIDATING,
            occurred_at=occurred_at,
            result_hash=result_hash,
        )

    def fail_run(
        self,
        run_uid: str,
        *,
        error_code: str,
        error_message: str = "",
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        return self.transition_run(
            run_uid,
            next_status=RUN_FAILED,
            occurred_at=occurred_at,
            error_code=error_code,
            error_message=error_message,
        )

    def _head(
        self,
        connection: Connection,
        channel: str,
        account_id: str,
        *,
        for_update: bool = False,
    ) -> dict[str, Any] | None:
        suffix = self._for_update(connection) if for_update else ""
        row = connection.execute(
            text(
                """
                SELECT channel, account_id, run_uid, context_id,
                       head_version, published_at, published_by, updated_at
                FROM st_decision_channel_head_v4
                WHERE channel = :channel AND account_id = :account_id
                """
                + suffix
            ),
            {"channel": channel, "account_id": account_id},
        ).mappings().first()
        return dict(row) if row else None

    def get_head(
        self, channel: str, *, account_id: str = ""
    ) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            return self._head(
                connection,
                _required_text(channel, field="channel"),
                _optional_text(account_id, field="account_id"),
            )

    def _publish_head(
        self,
        connection: Connection,
        run_uid: str,
        *,
        published_by: str,
        published_at: datetime,
        expected_head_version: int | None,
    ) -> HeadPublishResult:
        normalized_published_at = _parse_timestamp(published_at)
        if normalized_published_at is None:
            raise HeadPublishError("published_at is not a valid timestamp")
        published_at = normalized_published_at
        target = self._run(connection, run_uid, for_update=True)
        if target is None:
            raise DecisionRunNotFoundError(f"decision run not found: {run_uid}")
        if str(target["status"]) != RUN_COMMITTED:
            raise HeadPublishError(
                f"only COMMITTED runs may be published: {run_uid}"
            )
        channel = str(target["channel"])
        account_id = str(target.get("account_id") or "")
        current = self._head(
            connection,
            channel,
            account_id,
            for_update=True,
        )
        current_version = int(current["head_version"]) if current else 0
        target_committed = _parse_timestamp(target.get("committed_at"))
        target_decision = _parse_timestamp(target.get("context_decision_at"))
        if target_committed is None or target_decision is None:
            raise HeadPublishError(
                f"committed run has invalid publication timestamps: {run_uid}"
            )
        if published_at < target_committed:
            raise HeadPublishError(
                f"published_at cannot precede committed_at: {run_uid}"
            )
        if current and str(current["run_uid"]) == run_uid:
            if str(current["context_id"]) != str(target["context_id"]):
                raise HeadPublishError(
                    "current head context does not match its decision run: "
                    f"{run_uid}"
                )
            return HeadPublishResult(
                changed=False,
                previous_run_uid=run_uid,
                head=current,
            )
        if (
            expected_head_version is not None
            and current_version != int(expected_head_version)
        ):
            raise HeadPublishConflictError(
                f"head version changed for {channel}/{account_id}: "
                f"expected={expected_head_version} actual={current_version}"
            )

        previous_run_uid = str(current["run_uid"]) if current else None
        if current:
            current_published = _parse_timestamp(current.get("published_at"))
            if current_published is None:
                raise HeadPublishError(
                    "current head has an invalid published_at: "
                    f"{previous_run_uid}"
                )
            if published_at < current_published:
                raise HeadPublishConflictError(
                    "published_at cannot precede the current head: "
                    f"target={run_uid} current={previous_run_uid}"
                )
            previous = self._run(connection, previous_run_uid or "")
            if previous is None:
                raise HeadPublishError(
                    "current head references a missing decision run: "
                    f"{previous_run_uid}"
                )
            if str(previous.get("status")) != RUN_COMMITTED:
                raise HeadPublishError(
                    "current head references a non-committed decision run: "
                    f"{previous_run_uid}"
                )
            if str(current["context_id"]) != str(previous["context_id"]):
                raise HeadPublishError(
                    "current head context does not match its decision run: "
                    f"{previous_run_uid}"
                )
            previous_decision = _parse_timestamp(
                previous.get("context_decision_at")
            )
            previous_committed = _parse_timestamp(
                previous.get("committed_at")
            )
            if previous_decision is None or previous_committed is None:
                raise HeadPublishError(
                    "current head has invalid publication timestamps: "
                    f"{previous_run_uid}"
                )
            target_order = (target_decision, target_committed, run_uid)
            previous_order = (
                previous_decision,
                previous_committed,
                previous_run_uid or "",
            )
            if target_order <= previous_order:
                if target_decision < previous_decision:
                    message = "an older decision context"
                elif target_committed < previous_committed:
                    message = "an older committed run"
                else:
                    message = "a non-advancing decision run"
                raise HeadPublishConflictError(
                    f"{message} cannot replace the current head: "
                    f"target={run_uid} current={previous_run_uid}"
                )

        next_version = current_version + 1
        params = {
            "channel": channel,
            "account_id": account_id,
            "run_uid": run_uid,
            "context_id": str(target["context_id"]),
            "head_version": next_version,
            "published_at": published_at,
            "published_by": _optional_text(
                published_by,
                field="published_by",
            ),
            "updated_at": published_at,
        }
        if current is None:
            connection.execute(
                text(
                    """
                    INSERT INTO st_decision_channel_head_v4 (
                        channel, account_id, run_uid, context_id,
                        head_version, published_at, published_by, updated_at
                    ) VALUES (
                        :channel, :account_id, :run_uid, :context_id,
                        :head_version, :published_at, :published_by,
                        :updated_at
                    )
                    """
                ),
                params,
            )
        else:
            result = connection.execute(
                text(
                    """
                    UPDATE st_decision_channel_head_v4
                    SET run_uid = :run_uid,
                        context_id = :context_id,
                        head_version = :head_version,
                        published_at = :published_at,
                        published_by = :published_by,
                        updated_at = :updated_at
                    WHERE channel = :channel
                      AND account_id = :account_id
                      AND head_version = :current_version
                    """
                ),
                {**params, "current_version": current_version},
            )
            if int(result.rowcount or 0) != 1:
                raise HeadPublishConflictError(
                    f"concurrent head publication: {channel}/{account_id}"
                )
        stored = self._head(connection, channel, account_id)
        if stored is None:
            raise HeadPublishError(
                f"published head could not be read: {channel}/{account_id}"
            )
        return HeadPublishResult(
            changed=True,
            previous_run_uid=previous_run_uid,
            head=stored,
        )

    def publish_committed_head(
        self,
        run_uid: str,
        *,
        published_by: str = "",
        published_at: datetime | None = None,
        expected_head_version: int | None = None,
    ) -> HeadPublishResult:
        try:
            with self.engine.begin() as connection:
                return self._publish_head(
                    connection,
                    run_uid,
                    published_by=published_by,
                    published_at=_timestamp(published_at),
                    expected_head_version=expected_head_version,
                )
        except IntegrityError as exc:
            raise HeadPublishConflictError(
                f"concurrent head publication for run {run_uid}"
            ) from exc

    def commit_and_publish_head(
        self,
        run_uid: str,
        *,
        result_hash: str,
        published_by: str = "",
        occurred_at: datetime | None = None,
        expected_head_version: int | None = None,
    ) -> CommitAndPublishResult:
        """Commit a validated run and move its head in one DML transaction."""

        now = _timestamp(occurred_at)
        expected_result_hash = _require_sha256(
            result_hash,
            field="result_hash",
        )
        try:
            with self.engine.begin() as connection:
                current = self._run(connection, run_uid, for_update=True)
                if current is None:
                    raise DecisionRunNotFoundError(
                        f"decision run not found: {run_uid}"
                    )
                if str(current["status"]) == RUN_COMMITTED:
                    if str(current.get("result_hash") or "") != expected_result_hash:
                        raise DecisionRunConflictError(
                            "committed run was retried with a different result_hash: "
                            f"{run_uid}"
                        )
                    run = current
                else:
                    run = self._transition_run(
                        connection,
                        run_uid,
                        next_status=RUN_COMMITTED,
                        expected_status=RUN_VALIDATING,
                        occurred_at=now,
                        result_hash=expected_result_hash,
                    )
                publication = self._publish_head(
                    connection,
                    run_uid,
                    published_by=published_by,
                    published_at=now,
                    expected_head_version=expected_head_version,
                )
                return CommitAndPublishResult(
                    run=run,
                    publication=publication,
                )
        except IntegrityError as exc:
            raise HeadPublishConflictError(
                f"concurrent commit/publication for run {run_uid}"
            ) from exc


__all__ = [
    "ALLOWED_RUN_TRANSITIONS",
    "RUN_CANCELLED",
    "RUN_COMMITTED",
    "RUN_CREATED",
    "RUN_FAILED",
    "RUN_RUNNING",
    "RUN_VALIDATING",
    "CommitAndPublishResult",
    "CreateDecisionContextResult",
    "CreateDecisionRunResult",
    "DecisionContextNotFoundError",
    "DecisionRunConflictError",
    "DecisionRunNotFoundError",
    "HeadPublishConflictError",
    "HeadPublishError",
    "HeadPublishResult",
    "InvalidRunTransitionError",
    "TradingV4Repository",
    "V4RepositoryError",
    "build_run_idempotency_key",
]
