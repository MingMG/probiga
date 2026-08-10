from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from server.common.mysql_lock import mysql_named_lock
from server.common.mysql_version_policy import (
    is_isolated_acceptance_version,
    isolated_acceptance_versions_label,
)


MIGRATION_TABLE = "schema_migration_v4"
MIGRATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration_v4 (
    version VARCHAR(80) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    statement_count INT NOT NULL,
    applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""

_MYSQL8_BINARY_REGEXP_RE = re.compile(
    r"\bBINARY\s+"
    r"(?P<operand>[A-Za-z_][A-Za-z0-9_]*"
    r"(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s+"
    r"(?P<negated>NOT\s+)?REGEXP\b",
    flags=re.IGNORECASE,
)


def _uses_mysql8_icu_regexp(connection: Connection) -> bool:
    """Return whether ``connection`` rejects binary-string REGEXP operands."""

    dialect = getattr(connection, "dialect", None)
    if dialect is None:
        return False
    dialect_name = str(getattr(dialect, "name", "")).casefold()
    if dialect_name == "mariadb" or bool(
        getattr(dialect, "is_mariadb", False)
    ):
        return False
    version_info = getattr(dialect, "server_version_info", None)
    if not isinstance(version_info, tuple) or len(version_info) < 2:
        return False
    return tuple(version_info[:2]) >= (8, 0)


def _mysql_regexp_compatible_statement(
    connection: Connection,
    statement: str,
) -> str:
    """Render one frozen V4 statement safely for MySQL 8 ICU REGEXP.

    MySQL 5.7 accepts ``BINARY value REGEXP pattern`` and the text of the
    frozen 001-007 migrations intentionally retains that contract.  MySQL 8
    rejects binary-string operands with ``ER_CHARACTER_SET_MISMATCH``.  At the
    execution boundary only, convert the operand back to an utf8mb4 character
    string with a binary collation.  This keeps case-sensitive matching while
    leaving non-REGEXP ``BINARY`` comparisons, migration statements, and their
    ledger checksums byte-for-byte unchanged.
    """

    if type(statement) is not str:
        raise TypeError("V4 migration statement must be exactly str")
    if not _uses_mysql8_icu_regexp(connection):
        return statement

    def replacement(match: re.Match[str]) -> str:
        negated = match.group("negated") or ""
        return (
            f"CONVERT({match.group('operand')} USING utf8mb4) "
            f"COLLATE utf8mb4_bin {negated}REGEXP"
        )

    rendered = _MYSQL8_BINARY_REGEXP_RE.sub(replacement, statement)
    if _MYSQL8_BINARY_REGEXP_RE.search(rendered) is not None:
        raise RuntimeError("unresolved MySQL 8 binary REGEXP operand")
    return rendered


def _execute_mysql_regexp_compatible_statement(
    connection: Connection,
    statement: str,
) -> Any:
    """Execute one statically allowlisted V4 SQL statement with ICU safety."""

    return connection.execute(
        text(_mysql_regexp_compatible_statement(connection, statement))
    )


@dataclass(frozen=True)
class V4MigrationResult:
    version: str
    status: str
    statement_count: int
    checksum: str


@dataclass(frozen=True)
class _AppliedMigrationRecord:
    checksum: str
    statement_count: int


# V4 migrations are deliberately expand-only.  Every DDL statement in this
# first migration is independently repeatable because MySQL implicitly commits
# DDL; a process may therefore stop after creating a table but before recording
# the migration ledger row.  A rerun safely completes the same plan.
MIGRATIONS = (
    {
        "version": "20260803_001_trading_v4_control_plane",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_decision_context_v4 (
                context_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                decision_at DATETIME(6) NOT NULL,
                knowledge_cutoff_at DATETIME(6) NOT NULL,
                decision_clock VARCHAR(32) NOT NULL,
                feature_as_of DATE NOT NULL,
                universe_version VARCHAR(120) NOT NULL,
                account_snapshot_id VARCHAR(120) NOT NULL,
                run_mode VARCHAR(32) NOT NULL,
                is_realtime TINYINT(1) NOT NULL DEFAULT 0,
                freshness_status VARCHAR(16) NOT NULL,
                fallback_used TINYINT(1) NOT NULL DEFAULT 0,
                data_manifest_json LONGTEXT NOT NULL,
                source_manifest_json LONGTEXT NOT NULL,
                quality_json LONGTEXT NOT NULL,
                factor_spec_versions_json LONGTEXT NOT NULL,
                forecast_contract_ids_json LONGTEXT NOT NULL,
                model_versions_json LONGTEXT NOT NULL,
                model_artifact_hashes_json LONGTEXT NOT NULL,
                model_training_cutoffs_json LONGTEXT NOT NULL,
                model_available_at_json LONGTEXT NOT NULL,
                calibration_versions_json LONGTEXT NOT NULL,
                calibration_artifact_hashes_json LONGTEXT NOT NULL,
                calibration_training_cutoffs_json LONGTEXT NOT NULL,
                calibration_available_at_json LONGTEXT NOT NULL,
                capability_statuses_json LONGTEXT NOT NULL,
                context_json LONGTEXT NOT NULL,
                data_snapshot_hash CHAR(64) NOT NULL,
                context_hash CHAR(64) NOT NULL,
                feature_version VARCHAR(120) NOT NULL,
                model_set_version VARCHAR(120) NOT NULL,
                config_version VARCHAR(120) NOT NULL,
                portfolio_policy_version VARCHAR(120) NOT NULL,
                execution_contract_version VARCHAR(120) NOT NULL,
                fee_schedule_version VARCHAR(120) NOT NULL,
                code_commit_sha VARCHAR(64) NOT NULL,
                random_seed BIGINT UNSIGNED NOT NULL,
                created_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v4_context_hash (context_hash),
                KEY idx_v4_context_decision (trade_date, decision_at),
                KEY idx_v4_context_snapshot (data_snapshot_hash),
                KEY idx_v4_context_quality (freshness_status, decision_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_source_watermark_v4 (
                context_id VARCHAR(64) NOT NULL,
                source_key VARCHAR(120) NOT NULL,
                knowledge_time DATETIME(6) NOT NULL,
                source_event_at DATETIME(6) NULL,
                first_seen_at DATETIME(6) NULL,
                received_at DATETIME(6) NULL,
                available_at DATETIME(6) NULL,
                record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                snapshot_id VARCHAR(160) NOT NULL DEFAULT '',
                coverage DECIMAL(18,8) NULL,
                lag_seconds INT NULL,
                batch_id VARCHAR(160) NOT NULL DEFAULT '',
                schema_version VARCHAR(120) NOT NULL DEFAULT '',
                content_hash CHAR(64) NOT NULL DEFAULT '',
                quality_status VARCHAR(16) NOT NULL,
                details_json LONGTEXT NOT NULL,
                created_at DATETIME(6) NOT NULL,
                CONSTRAINT fk_v4_watermark_context
                    FOREIGN KEY (context_id)
                    REFERENCES st_decision_context_v4 (context_id)
                    ON DELETE RESTRICT,
                PRIMARY KEY (context_id, source_key),
                KEY idx_v4_watermark_quality
                    (quality_status, available_at),
                KEY idx_v4_watermark_source
                    (source_key, available_at),
                KEY idx_v4_watermark_batch (source_key, batch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_decision_run_v4 (
                run_uid VARCHAR(64) PRIMARY KEY,
                run_idempotency_key CHAR(64) NOT NULL,
                context_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL DEFAULT '',
                channel VARCHAR(32) NOT NULL,
                run_type VARCHAR(40) NOT NULL,
                trigger_type VARCHAR(40) NOT NULL,
                trigger_ref_id VARCHAR(160) NOT NULL DEFAULT '',
                parent_run_uid VARCHAR(64) NOT NULL DEFAULT '',
                status VARCHAR(24) NOT NULL,
                model_set_version VARCHAR(120) NOT NULL,
                config_version VARCHAR(120) NOT NULL,
                code_commit_sha VARCHAR(64) NOT NULL,
                result_hash CHAR(64) NULL,
                error_code VARCHAR(100) NULL,
                error_message VARCHAR(1000) NULL,
                started_at DATETIME(6) NULL,
                validated_at DATETIME(6) NULL,
                committed_at DATETIME(6) NULL,
                finished_at DATETIME(6) NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                CONSTRAINT fk_v4_run_context
                    FOREIGN KEY (context_id)
                    REFERENCES st_decision_context_v4 (context_id)
                    ON DELETE RESTRICT,
                UNIQUE KEY uk_v4_run_idempotency (run_idempotency_key),
                UNIQUE KEY uk_v4_run_context_identity
                    (run_uid, context_id),
                KEY idx_v4_run_context (context_id, created_at),
                KEY idx_v4_run_channel
                    (channel, account_id, status, committed_at),
                KEY idx_v4_run_trigger
                    (trigger_type, trigger_ref_id, created_at),
                KEY idx_v4_run_parent (parent_run_uid, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_job_run_v4 (
                job_id VARCHAR(64) PRIMARY KEY,
                idempotency_key CHAR(64) NOT NULL,
                job_type VARCHAR(80) NOT NULL,
                scheduled_for DATETIME(6) NOT NULL,
                input_context_id VARCHAR(64) NOT NULL DEFAULT '',
                input_hash CHAR(64) NOT NULL DEFAULT '',
                run_uid VARCHAR(64) NOT NULL DEFAULT '',
                status VARCHAR(24) NOT NULL,
                attempt_count INT NOT NULL DEFAULT 0,
                lease_owner VARCHAR(160) NOT NULL DEFAULT '',
                lease_until DATETIME(6) NULL,
                next_attempt_at DATETIME(6) NULL,
                error_code VARCHAR(100) NULL,
                error_message VARCHAR(1000) NULL,
                started_at DATETIME(6) NULL,
                completed_at DATETIME(6) NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v4_job_idempotency (idempotency_key),
                KEY idx_v4_job_due
                    (status, next_attempt_at, scheduled_for),
                KEY idx_v4_job_lease (status, lease_until),
                KEY idx_v4_job_run (run_uid, created_at),
                KEY idx_v4_job_context (input_context_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_decision_channel_head_v4 (
                channel VARCHAR(32) NOT NULL,
                account_id VARCHAR(64) NOT NULL DEFAULT '',
                run_uid VARCHAR(64) NOT NULL,
                context_id VARCHAR(64) NOT NULL,
                head_version BIGINT NOT NULL,
                published_at DATETIME(6) NOT NULL,
                published_by VARCHAR(120) NOT NULL DEFAULT '',
                updated_at DATETIME(6) NOT NULL,
                CONSTRAINT fk_v4_head_run_context
                    FOREIGN KEY (run_uid, context_id)
                    REFERENCES st_decision_run_v4 (run_uid, context_id)
                    ON DELETE RESTRICT,
                PRIMARY KEY (channel, account_id),
                KEY idx_v4_head_run (run_uid, context_id),
                KEY idx_v4_head_context (context_id),
                KEY idx_v4_head_published (published_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_runtime_control_v4 (
                control_key VARCHAR(120) PRIMARY KEY,
                control_value_json LONGTEXT NOT NULL,
                version BIGINT NOT NULL,
                updated_by VARCHAR(120) NOT NULL,
                reason VARCHAR(500) NOT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                KEY idx_v4_control_updated (updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_runtime_control_transition_v4 (
                transition_id VARCHAR(64) PRIMARY KEY,
                control_key VARCHAR(120) NOT NULL,
                previous_value_json LONGTEXT NULL,
                next_value_json LONGTEXT NOT NULL,
                next_version BIGINT NOT NULL,
                changed_by VARCHAR(120) NOT NULL,
                reason VARCHAR(500) NOT NULL,
                event_hash CHAR(64) NOT NULL,
                changed_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v4_control_transition_hash (event_hash),
                UNIQUE KEY uk_v4_control_transition_version
                    (control_key, next_version),
                KEY idx_v4_control_transition_time
                    (control_key, changed_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
        ),
    },
)


JOB_LEASE_MIGRATION_VERSION = "20260804_002_v4_job_lease_repair"
JOB_LEASE_TABLE = "st_job_run_v4"
JOB_LEASE_EXHAUSTED_ERROR_CODE = "LEASE_EXPIRED_MAX_ATTEMPTS"
JOB_LEASE_EXHAUSTED_ERROR_MESSAGE = "lease expired after maximum attempts"
JOB_LEASE_DB_CLOCK_MAX_SKEW_SECONDS = 5
JOB_LEASE_MAX_DURATION_SECONDS = 900
JOB_LEASE_TRIGGER_NAMES = (
    "trg_v4_job_lease_bi",
    "trg_v4_job_lease_bu",
)

_JOB_LEASE_ADD_TOKEN_DDL = """
ALTER TABLE st_job_run_v4
    ADD COLUMN lease_token CHAR(64) NULL AFTER lease_owner
"""
_JOB_LEASE_ADD_MAX_ATTEMPTS_DDL = """
ALTER TABLE st_job_run_v4
    ADD COLUMN max_attempts INT UNSIGNED NOT NULL DEFAULT 3
    AFTER attempt_count
"""
_JOB_LEASE_ADD_DUE_INDEX_DDL = """
ALTER TABLE st_job_run_v4
    ADD KEY idx_v4_job_claim_due (
        status,
        next_attempt_at,
        lease_until,
        scheduled_for,
        attempt_count,
        max_attempts,
        job_id
    )
"""
_JOB_LEASE_ADD_TOKEN_INDEX_DDL = """
ALTER TABLE st_job_run_v4
    ADD UNIQUE KEY uk_v4_job_lease_token (lease_token)
"""
_JOB_LEASE_INSERT_TRIGGER_DDL = """
CREATE TRIGGER trg_v4_job_lease_bi
BEFORE INSERT ON st_job_run_v4
FOR EACH ROW
BEGIN
    IF NEW.status NOT IN (
        'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job status';
    END IF;
    IF NEW.max_attempts < 1
       OR NEW.attempt_count < 0
       OR NEW.attempt_count > NEW.max_attempts THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job attempt bounds';
    END IF;
    IF NEW.job_id = ''
       OR BINARY NEW.job_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.idempotency_key NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.job_type = ''
       OR BINARY NEW.job_type REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.input_context_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR (NEW.input_hash <> ''
           AND BINARY NEW.input_hash NOT REGEXP '^[0-9a-f]{64}$')
       OR BINARY NEW.run_uid REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.status REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.lease_owner REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR (NEW.error_code IS NOT NULL
           AND (NEW.error_code = ''
                OR BINARY NEW.error_code REGEXP
                    '(^[[:space:]])|([[:space:]]$)'))
       OR (NEW.error_message IS NOT NULL
           AND BINARY NEW.error_message REGEXP
                '(^[[:space:]])|([[:space:]]$)') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job exact text contract';
    END IF;
    IF NEW.created_at > DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND)
       OR NEW.updated_at > DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 job timestamp exceeds DB clock skew';
    END IF;
    IF NEW.status <> 'PENDING'
       OR NEW.attempt_count <> 0
       OR NEW.run_uid <> ''
       OR NEW.lease_owner <> ''
       OR NEW.lease_token IS NOT NULL
       OR NEW.lease_until IS NOT NULL
       OR NOT (NEW.next_attempt_at <=> NEW.scheduled_for)
       OR NEW.error_code IS NOT NULL
       OR NEW.error_message IS NOT NULL
       OR NEW.started_at IS NOT NULL
       OR NEW.completed_at IS NOT NULL
       OR NOT (NEW.updated_at <=> NEW.created_at) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid initial V4 job shape';
    END IF;
END
"""
_JOB_LEASE_UPDATE_TRIGGER_DDL = """
CREATE TRIGGER trg_v4_job_lease_bu
BEFORE UPDATE ON st_job_run_v4
FOR EACH ROW
BEGIN
    IF NEW.status NOT IN (
        'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job status';
    END IF;
    IF OLD.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'terminal V4 job is immutable';
    END IF;
    IF NOT (NEW.job_id <=> OLD.job_id)
       OR NOT (NEW.idempotency_key <=> OLD.idempotency_key)
       OR NOT (NEW.job_type <=> OLD.job_type)
       OR NOT (NEW.scheduled_for <=> OLD.scheduled_for)
       OR NOT (NEW.input_context_id <=> OLD.input_context_id)
       OR NOT (NEW.input_hash <=> OLD.input_hash)
       OR NOT (NEW.created_at <=> OLD.created_at)
       OR NEW.max_attempts <> OLD.max_attempts THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 job identity is immutable';
    END IF;
    IF NEW.job_id = ''
       OR BINARY NEW.job_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.idempotency_key NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.job_type = ''
       OR BINARY NEW.job_type REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.input_context_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR (NEW.input_hash <> ''
           AND BINARY NEW.input_hash NOT REGEXP '^[0-9a-f]{64}$')
       OR BINARY NEW.run_uid REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.status REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.lease_owner REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR (NEW.error_code IS NOT NULL
           AND (NEW.error_code = ''
                OR BINARY NEW.error_code REGEXP
                    '(^[[:space:]])|([[:space:]]$)'))
       OR (NEW.error_message IS NOT NULL
           AND BINARY NEW.error_message REGEXP
                '(^[[:space:]])|([[:space:]]$)') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job exact text contract';
    END IF;
    IF NEW.updated_at <= OLD.updated_at
       OR NEW.updated_at > DATE_ADD(
            UTC_TIMESTAMP(6), INTERVAL 5 SECOND
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 job updated_at violates DB clock';
    END IF;
    IF OLD.status = 'PENDING'
       AND NEW.status NOT IN ('RUNNING', 'FAILED', 'CANCELLED') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job status transition';
    END IF;
    IF OLD.status = 'RUNNING'
       AND NEW.status NOT IN (
           'RUNNING', 'PENDING', 'SUCCEEDED', 'FAILED', 'CANCELLED'
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job status transition';
    END IF;
    IF NEW.max_attempts < 1
       OR NEW.attempt_count < 0
       OR NEW.attempt_count > NEW.max_attempts
       OR NEW.attempt_count < OLD.attempt_count THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 job attempt bounds';
    END IF;
    IF NEW.error_message IS NOT NULL
       AND (NEW.error_code IS NULL OR TRIM(NEW.error_code) = '') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 job error message requires error code';
    END IF;
    IF NEW.status = 'RUNNING' THEN
        IF TRIM(NEW.lease_owner) = ''
           OR NEW.lease_token IS NULL
           OR BINARY NEW.lease_token NOT REGEXP '^[0-9a-f]{64}$'
           OR NEW.lease_until IS NULL
           OR NEW.lease_until <= UTC_TIMESTAMP(6)
           OR NEW.lease_until <= NEW.updated_at
           OR NEW.lease_until > DATE_ADD(
                NEW.updated_at, INTERVAL 900 SECOND
           )
           OR NEW.lease_until >
                DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 900 SECOND) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'RUNNING V4 job requires owner token until';
        END IF;
        IF OLD.status = 'PENDING' THEN
            IF NEW.attempt_count <> OLD.attempt_count + 1
               OR OLD.scheduled_for > UTC_TIMESTAMP(6)
               OR OLD.next_attempt_at IS NULL
               OR OLD.next_attempt_at > UTC_TIMESTAMP(6)
               OR NEW.run_uid <> ''
               OR NEW.next_attempt_at IS NOT NULL
               OR NEW.error_code IS NOT NULL
               OR NEW.error_message IS NOT NULL
               OR NEW.completed_at IS NOT NULL
               OR (OLD.started_at IS NULL
                   AND NOT (NEW.started_at <=> NEW.updated_at))
               OR (OLD.started_at IS NOT NULL
                   AND NOT (NEW.started_at <=> OLD.started_at)) THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid V4 job claim shape';
            END IF;
        ELSEIF NEW.attempt_count = OLD.attempt_count THEN
            IF OLD.lease_until IS NULL
               OR OLD.lease_until <= UTC_TIMESTAMP(6)
               OR NEW.lease_owner <> OLD.lease_owner
               OR NOT (NEW.lease_token <=> OLD.lease_token)
               OR NEW.lease_until <= OLD.lease_until
               OR NOT (NEW.run_uid <=> OLD.run_uid)
               OR NOT (NEW.next_attempt_at <=> OLD.next_attempt_at)
               OR NOT (NEW.error_code <=> OLD.error_code)
               OR NOT (NEW.error_message <=> OLD.error_message)
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR NOT (NEW.completed_at <=> OLD.completed_at) THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V4 heartbeat may only extend its lease';
            END IF;
        ELSEIF NEW.attempt_count = OLD.attempt_count + 1 THEN
            IF OLD.lease_until IS NULL
               OR OLD.lease_until > UTC_TIMESTAMP(6)
               OR (NEW.lease_token <=> OLD.lease_token)
               OR NEW.run_uid <> ''
               OR NEW.next_attempt_at IS NOT NULL
               OR NEW.error_code IS NOT NULL
               OR NEW.error_message IS NOT NULL
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR NEW.completed_at IS NOT NULL THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid or active V4 job reclaim';
            END IF;
        ELSE
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V4 job claim must advance one attempt';
        END IF;
    ELSE
        IF NEW.attempt_count <> OLD.attempt_count THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'only V4 claims may advance attempts';
        END IF;
        IF NEW.lease_owner <> ''
           OR NEW.lease_token IS NOT NULL
           OR NEW.lease_until IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'non-RUNNING V4 job cannot retain lease';
        END IF;
        IF NEW.error_code = 'LEASE_EXPIRED_MAX_ATTEMPTS' THEN
            IF OLD.status <> 'RUNNING'
               OR OLD.lease_until IS NULL
               OR OLD.lease_until > UTC_TIMESTAMP(6)
               OR OLD.attempt_count <> OLD.max_attempts
               OR NEW.status <> 'FAILED'
               OR NOT (NEW.error_message <=>
                    'lease expired after maximum attempts') THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'reserved V4 lease exhaustion shape';
            END IF;
        END IF;
        IF OLD.status = 'RUNNING'
           AND (OLD.lease_until IS NULL
                OR OLD.lease_until <= UTC_TIMESTAMP(6)) THEN
            IF NEW.status <> 'FAILED'
               OR OLD.attempt_count <> OLD.max_attempts
               OR NOT (NEW.error_code <=> 'LEASE_EXPIRED_MAX_ATTEMPTS')
               OR NOT (NEW.error_message <=>
                    'lease expired after maximum attempts') THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'expired V4 lease requires exhausted failure';
            END IF;
        END IF;
        IF NEW.status = 'PENDING' THEN
            IF OLD.status <> 'RUNNING'
               OR NEW.run_uid <> ''
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR NEW.attempt_count >= NEW.max_attempts
               OR NEW.next_attempt_at IS NULL
               OR NEW.next_attempt_at <= NEW.updated_at
               OR NEW.error_code IS NULL
               OR TRIM(NEW.error_code) = ''
               OR NEW.completed_at IS NOT NULL THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid V4 job retry shape';
            END IF;
        ELSEIF NEW.status = 'SUCCEEDED' THEN
            IF OLD.status <> 'RUNNING'
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR OLD.run_uid <> ''
               OR NEW.run_uid = ''
               OR NEW.next_attempt_at IS NOT NULL
               OR NEW.error_code IS NOT NULL
               OR NEW.error_message IS NOT NULL
               OR NEW.completed_at IS NULL
               OR NOT (NEW.completed_at <=> NEW.updated_at) THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid V4 job success shape';
            END IF;
        ELSEIF NEW.status = 'FAILED' THEN
            IF NEW.run_uid <> ''
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR NEW.next_attempt_at IS NOT NULL
               OR NEW.error_code IS NULL
               OR TRIM(NEW.error_code) = ''
               OR NEW.completed_at IS NULL
               OR NOT (NEW.completed_at <=> NEW.updated_at) THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid V4 job failure shape';
            END IF;
        ELSEIF NEW.status = 'CANCELLED' THEN
            IF NEW.run_uid <> ''
               OR NOT (NEW.started_at <=> OLD.started_at)
               OR NEW.next_attempt_at IS NOT NULL
               OR NEW.completed_at IS NULL
               OR NOT (NEW.completed_at <=> NEW.updated_at) THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid V4 job cancellation shape';
            END IF;
        END IF;
    END IF;
END
"""

# 001 is intentionally left byte-for-byte unchanged.  002 is independently
# recoverable after every implicit-commit boundary; see
# ``_apply_job_lease_statement`` below.
MIGRATIONS = (
    *MIGRATIONS,
    {
        "version": JOB_LEASE_MIGRATION_VERSION,
        "statements": (
            _JOB_LEASE_ADD_TOKEN_DDL,
            _JOB_LEASE_ADD_MAX_ATTEMPTS_DDL,
            _JOB_LEASE_ADD_DUE_INDEX_DDL,
            _JOB_LEASE_ADD_TOKEN_INDEX_DDL,
            _JOB_LEASE_INSERT_TRIGGER_DDL,
            _JOB_LEASE_UPDATE_TRIGGER_DDL,
        ),
    },
)


CONTROL_GUARD_MIGRATION_VERSION = "20260804_004_v4_control_plane_guards"

_CONTEXT_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_context_bu
BEFORE UPDATE ON st_decision_context_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 decision context is append-only';
END
"""
_CONTEXT_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_context_bd
BEFORE DELETE ON st_decision_context_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 decision context is append-only';
END
"""
_WATERMARK_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_watermark_bu
BEFORE UPDATE ON st_source_watermark_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 source watermark is append-only';
END
"""
_WATERMARK_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_watermark_bd
BEFORE DELETE ON st_source_watermark_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 source watermark is append-only';
END
"""
_RUN_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_run_bi
BEFORE INSERT ON st_decision_run_v4
FOR EACH ROW
BEGIN
    IF NEW.run_uid = ''
       OR BINARY NEW.run_uid REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.run_idempotency_key NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.context_id = ''
       OR NEW.channel = ''
       OR NEW.run_type = ''
       OR NEW.trigger_type = ''
       OR BINARY NEW.context_id REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.account_id REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.channel REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.run_type REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.trigger_type REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.trigger_ref_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.parent_run_uid REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.status <> 'CREATED'
       OR NEW.result_hash IS NOT NULL
       OR NEW.error_code IS NOT NULL
       OR NEW.error_message IS NOT NULL
       OR NEW.started_at IS NOT NULL
       OR NEW.validated_at IS NOT NULL
       OR NEW.committed_at IS NOT NULL
       OR NEW.finished_at IS NOT NULL
       OR NOT (NEW.updated_at <=> NEW.created_at) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid initial V4 decision run';
    END IF;
    IF (SELECT COUNT(*) FROM st_decision_context_v4 c
        WHERE c.context_id = NEW.context_id
          AND BINARY c.model_set_version = BINARY NEW.model_set_version
          AND BINARY c.config_version = BINARY NEW.config_version
          AND BINARY c.code_commit_sha = BINARY NEW.code_commit_sha) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 run identity does not match context';
    END IF;
    IF NEW.parent_run_uid <> ''
       AND (SELECT COUNT(*) FROM st_decision_run_v4 p
            WHERE p.run_uid = NEW.parent_run_uid
              AND BINARY p.channel = BINARY NEW.channel
              AND BINARY p.account_id = BINARY NEW.account_id) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 parent decision run';
    END IF;
END
"""
_RUN_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_run_bu
BEFORE UPDATE ON st_decision_run_v4
FOR EACH ROW
BEGIN
    IF NOT (BINARY NEW.run_uid <=> BINARY OLD.run_uid)
       OR NOT (BINARY NEW.run_idempotency_key <=>
            BINARY OLD.run_idempotency_key)
       OR NOT (BINARY NEW.context_id <=> BINARY OLD.context_id)
       OR NOT (BINARY NEW.account_id <=> BINARY OLD.account_id)
       OR NOT (BINARY NEW.channel <=> BINARY OLD.channel)
       OR NOT (BINARY NEW.run_type <=> BINARY OLD.run_type)
       OR NOT (BINARY NEW.trigger_type <=> BINARY OLD.trigger_type)
       OR NOT (BINARY NEW.trigger_ref_id <=> BINARY OLD.trigger_ref_id)
       OR NOT (BINARY NEW.parent_run_uid <=> BINARY OLD.parent_run_uid)
       OR NOT (BINARY NEW.model_set_version <=>
            BINARY OLD.model_set_version)
       OR NOT (BINARY NEW.config_version <=> BINARY OLD.config_version)
       OR NOT (BINARY NEW.code_commit_sha <=> BINARY OLD.code_commit_sha)
       OR NOT (NEW.created_at <=> OLD.created_at) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 decision run identity is immutable';
    END IF;
    IF OLD.status IN ('COMMITTED', 'FAILED', 'CANCELLED')
       OR NEW.updated_at < OLD.updated_at THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 decision run transition';
    END IF;
    IF NEW.status = 'RUNNING' THEN
        IF OLD.status <> 'CREATED'
           OR NOT (NEW.started_at <=> NEW.updated_at)
           OR NEW.validated_at IS NOT NULL
           OR NEW.result_hash IS NOT NULL
           OR NEW.error_code IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.committed_at IS NOT NULL
           OR NEW.finished_at IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid V4 run RUNNING shape';
        END IF;
    ELSEIF NEW.status = 'VALIDATING' THEN
        IF OLD.status <> 'RUNNING'
           OR NOT (NEW.started_at <=> OLD.started_at)
           OR NOT (NEW.validated_at <=> NEW.updated_at)
           OR NEW.result_hash IS NOT NULL
           OR NEW.error_code IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.committed_at IS NOT NULL
           OR NEW.finished_at IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid V4 run VALIDATING shape';
        END IF;
    ELSEIF NEW.status = 'COMMITTED' THEN
        IF OLD.status <> 'VALIDATING'
           OR NOT (NEW.started_at <=> OLD.started_at)
           OR NOT (NEW.validated_at <=> OLD.validated_at)
           OR NEW.result_hash IS NULL
           OR BINARY NEW.result_hash NOT REGEXP '^[0-9a-f]{64}$'
           OR NEW.error_code IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NOT (NEW.committed_at <=> NEW.updated_at)
           OR NOT (NEW.finished_at <=> NEW.updated_at) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid V4 run COMMITTED shape';
        END IF;
    ELSEIF NEW.status IN ('FAILED', 'CANCELLED') THEN
        IF OLD.status NOT IN ('CREATED', 'RUNNING', 'VALIDATING')
           OR NOT (NEW.started_at <=> OLD.started_at)
           OR NOT (NEW.validated_at <=> OLD.validated_at)
           OR NEW.result_hash IS NOT NULL
           OR NEW.error_code IS NULL
           OR NEW.error_code = ''
           OR BINARY NEW.error_code REGEXP
                '(^[[:space:]])|([[:space:]]$)'
           OR NEW.error_message IS NULL
           OR NEW.committed_at IS NOT NULL
           OR NOT (NEW.finished_at <=> NEW.updated_at) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid V4 terminal run shape';
        END IF;
    ELSE
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 decision run transition';
    END IF;
END
"""
_RUN_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_run_bd
BEFORE DELETE ON st_decision_run_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 decision run cannot be deleted';
END
"""
_HEAD_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_head_bi
BEFORE INSERT ON st_decision_channel_head_v4
FOR EACH ROW
BEGIN
    IF NEW.channel = ''
       OR BINARY NEW.channel REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.account_id REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NEW.head_version <> 1
       OR NOT (NEW.updated_at <=> NEW.published_at)
       OR (SELECT COUNT(*) FROM st_decision_run_v4 r
           WHERE r.run_uid = NEW.run_uid
             AND r.context_id = NEW.context_id
             AND BINARY r.channel = BINARY NEW.channel
             AND BINARY r.account_id = BINARY NEW.account_id
             AND r.status = 'COMMITTED'
             AND r.committed_at <= NEW.published_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid initial V4 channel head';
    END IF;
END
"""
_HEAD_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_head_bu
BEFORE UPDATE ON st_decision_channel_head_v4
FOR EACH ROW
BEGIN
    IF NOT (BINARY NEW.channel <=> BINARY OLD.channel)
       OR NOT (BINARY NEW.account_id <=> BINARY OLD.account_id)
       OR NEW.head_version <> OLD.head_version + 1
       OR NEW.published_at < OLD.published_at
       OR NOT (NEW.updated_at <=> NEW.published_at)
       OR (SELECT COUNT(*) FROM st_decision_run_v4 r
           WHERE r.run_uid = NEW.run_uid
             AND r.context_id = NEW.context_id
             AND BINARY r.channel = BINARY NEW.channel
             AND BINARY r.account_id = BINARY NEW.account_id
             AND r.status = 'COMMITTED'
             AND r.committed_at <= NEW.published_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 channel head update';
    END IF;
    IF (SELECT COUNT(*)
        FROM st_decision_run_v4 nr
        JOIN st_decision_context_v4 nc ON nc.context_id = nr.context_id
        JOIN st_decision_run_v4 orr ON orr.run_uid = OLD.run_uid
        JOIN st_decision_context_v4 oc ON oc.context_id = orr.context_id
        WHERE nr.run_uid = NEW.run_uid
          AND (nc.decision_at > oc.decision_at
               OR (nc.decision_at = oc.decision_at
                   AND nr.committed_at > orr.committed_at)
               OR (nc.decision_at = oc.decision_at
                   AND nr.committed_at = orr.committed_at
                   AND BINARY nr.run_uid > BINARY orr.run_uid))) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 channel head order cannot move backwards';
    END IF;
END
"""
_HEAD_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_head_bd
BEFORE DELETE ON st_decision_channel_head_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 channel head cannot be deleted';
END
"""
_CONTROL_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_runtime_control_bi
BEFORE INSERT ON st_runtime_control_v4
FOR EACH ROW
BEGIN
    IF NEW.control_key = ''
       OR BINARY NEW.control_key REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR JSON_VALID(NEW.control_value_json) <> 1
       OR NEW.version <> 1
       OR NEW.updated_by = ''
       OR BINARY NEW.updated_by REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NEW.reason = ''
       OR BINARY NEW.reason REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NOT (NEW.updated_at <=> NEW.created_at) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid initial V4 runtime control';
    END IF;
    IF (SELECT COUNT(*)
        FROM st_runtime_control_transition_v4 t
        WHERE t.control_key = NEW.control_key
          AND t.next_version = 1
          AND t.previous_value_json IS NULL
          AND BINARY t.next_value_json = BINARY NEW.control_value_json
          AND BINARY t.changed_by = BINARY NEW.updated_by
          AND BINARY t.reason = BINARY NEW.reason
          AND t.changed_at = NEW.created_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 runtime control lacks exact transition';
    END IF;
END
"""
_CONTROL_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_runtime_control_bu
BEFORE UPDATE ON st_runtime_control_v4
FOR EACH ROW
BEGIN
    IF NOT (BINARY NEW.control_key <=> BINARY OLD.control_key)
       OR NOT (NEW.created_at <=> OLD.created_at)
       OR NEW.version <> OLD.version + 1
       OR NEW.updated_at < OLD.updated_at
       OR BINARY NEW.control_value_json = BINARY OLD.control_value_json
       OR JSON_VALID(NEW.control_value_json) <> 1
       OR NEW.updated_by = ''
       OR BINARY NEW.updated_by REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NEW.reason = ''
       OR BINARY NEW.reason REGEXP '(^[[:space:]])|([[:space:]]$)' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 runtime control CAS';
    END IF;
    IF (SELECT COUNT(*)
        FROM st_runtime_control_transition_v4 t
        WHERE t.control_key = NEW.control_key
          AND t.next_version = NEW.version
          AND BINARY t.previous_value_json =
                BINARY OLD.control_value_json
          AND BINARY t.next_value_json = BINARY NEW.control_value_json
          AND BINARY t.changed_by = BINARY NEW.updated_by
          AND BINARY t.reason = BINARY NEW.reason
          AND t.changed_at = NEW.updated_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 runtime control lacks exact transition';
    END IF;
END
"""
_CONTROL_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_runtime_control_bd
BEFORE DELETE ON st_runtime_control_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 runtime control cannot be deleted';
END
"""
_TRANSITION_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_control_transition_bi
BEFORE INSERT ON st_runtime_control_transition_v4
FOR EACH ROW
BEGIN
    IF BINARY NEW.transition_id NOT REGEXP '^[0-9a-f]{64}$'
       OR BINARY NEW.event_hash NOT REGEXP '^[0-9a-f]{64}$'
       OR NOT (BINARY NEW.transition_id <=> BINARY NEW.event_hash)
       OR NEW.control_key = ''
       OR BINARY NEW.control_key REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NEW.next_version < 1
       OR JSON_VALID(NEW.next_value_json) <> 1
       OR (NEW.previous_value_json IS NOT NULL
           AND JSON_VALID(NEW.previous_value_json) <> 1)
       OR NEW.changed_by = ''
       OR BINARY NEW.changed_by REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR NEW.reason = ''
       OR BINARY NEW.reason REGEXP '(^[[:space:]])|([[:space:]]$)'
       OR (NEW.next_version = 1 AND NEW.previous_value_json IS NOT NULL)
       OR (NEW.next_version > 1 AND NEW.previous_value_json IS NULL) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 runtime transition row';
    END IF;
    IF NEW.next_version = 1 THEN
        IF (SELECT COUNT(*) FROM st_runtime_control_v4 c
            WHERE c.control_key = NEW.control_key) <> 0
           OR (SELECT COUNT(*)
               FROM st_runtime_control_transition_v4 existing
               WHERE existing.control_key = NEW.control_key) <> 0 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid V4 runtime transition genesis';
        END IF;
    ELSE
        IF BINARY NEW.next_value_json = BINARY NEW.previous_value_json
           OR (SELECT COUNT(*) FROM st_runtime_control_v4 c
               WHERE c.control_key = NEW.control_key
                 AND c.version = NEW.next_version - 1
                 AND BINARY c.control_value_json =
                        BINARY NEW.previous_value_json
                 AND c.updated_at <= NEW.changed_at) <> 1
           OR (SELECT COUNT(*)
               FROM st_runtime_control_transition_v4 p
               WHERE p.control_key = NEW.control_key
                 AND p.next_version = NEW.next_version - 1
                 AND BINARY p.next_value_json =
                        BINARY NEW.previous_value_json
                 AND p.changed_at <= NEW.changed_at) <> 1 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V4 runtime transition chain is broken';
        END IF;
    END IF;
END
"""
_TRANSITION_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_control_transition_bu
BEFORE UPDATE ON st_runtime_control_transition_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 runtime transition is append-only';
END
"""
_TRANSITION_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_control_transition_bd
BEFORE DELETE ON st_runtime_control_transition_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 runtime transition is append-only';
END
"""

CONTROL_GUARD_TRIGGER_SPECS = (
    ("trg_v4_context_bu", "UPDATE", "st_decision_context_v4", _CONTEXT_UPDATE_GUARD_DDL),
    ("trg_v4_context_bd", "DELETE", "st_decision_context_v4", _CONTEXT_DELETE_GUARD_DDL),
    ("trg_v4_watermark_bu", "UPDATE", "st_source_watermark_v4", _WATERMARK_UPDATE_GUARD_DDL),
    ("trg_v4_watermark_bd", "DELETE", "st_source_watermark_v4", _WATERMARK_DELETE_GUARD_DDL),
    ("trg_v4_run_bi", "INSERT", "st_decision_run_v4", _RUN_INSERT_GUARD_DDL),
    ("trg_v4_run_bu", "UPDATE", "st_decision_run_v4", _RUN_UPDATE_GUARD_DDL),
    ("trg_v4_run_bd", "DELETE", "st_decision_run_v4", _RUN_DELETE_GUARD_DDL),
    ("trg_v4_head_bi", "INSERT", "st_decision_channel_head_v4", _HEAD_INSERT_GUARD_DDL),
    ("trg_v4_head_bu", "UPDATE", "st_decision_channel_head_v4", _HEAD_UPDATE_GUARD_DDL),
    ("trg_v4_head_bd", "DELETE", "st_decision_channel_head_v4", _HEAD_DELETE_GUARD_DDL),
    ("trg_v4_runtime_control_bi", "INSERT", "st_runtime_control_v4", _CONTROL_INSERT_GUARD_DDL),
    ("trg_v4_runtime_control_bu", "UPDATE", "st_runtime_control_v4", _CONTROL_UPDATE_GUARD_DDL),
    ("trg_v4_runtime_control_bd", "DELETE", "st_runtime_control_v4", _CONTROL_DELETE_GUARD_DDL),
    ("trg_v4_control_transition_bi", "INSERT", "st_runtime_control_transition_v4", _TRANSITION_INSERT_GUARD_DDL),
    ("trg_v4_control_transition_bu", "UPDATE", "st_runtime_control_transition_v4", _TRANSITION_UPDATE_GUARD_DDL),
    ("trg_v4_control_transition_bd", "DELETE", "st_runtime_control_transition_v4", _TRANSITION_DELETE_GUARD_DDL),
)
CONTROL_GUARD_TRIGGER_NAMES = tuple(
    name for name, _event, _table, _statement in CONTROL_GUARD_TRIGGER_SPECS
)
CONTROL_GUARD_TABLES = tuple(
    sorted({table for _name, _event, table, _statement in CONTROL_GUARD_TRIGGER_SPECS})
)

CONTROL_GUARD_MIGRATION = {
    "version": CONTROL_GUARD_MIGRATION_VERSION,
    "statements": tuple(
        statement
        for _name, _event, _table, statement in CONTROL_GUARD_TRIGGER_SPECS
    ),
}


CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION = (
    "20260804_003_v4_claim_token_registry"
)
CLAIM_TOKEN_REGISTRY_TABLE = "st_job_claim_token_v4"
CLAIM_TOKEN_REGISTRY_TRIGGER_NAMES = (
    "trg_v4_job_claim_token_bi",
    "trg_v4_job_claim_token_bu",
    "trg_v4_job_claim_token_bd",
)

_CLAIM_TOKEN_REGISTRY_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_job_claim_token_v4 (
    lease_token CHAR(64) NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    attempt_count INT UNSIGNED NOT NULL,
    lease_owner VARCHAR(160) NOT NULL,
    claimed_at DATETIME(6) NOT NULL,
    lease_until DATETIME(6) NOT NULL,
    CONSTRAINT fk_v4_job_claim_token_job
        FOREIGN KEY (job_id)
        REFERENCES st_job_run_v4 (job_id)
        ON DELETE RESTRICT,
    PRIMARY KEY (lease_token),
    UNIQUE KEY uk_v4_job_claim_attempt (job_id, attempt_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""
_CLAIM_TOKEN_REGISTRY_INSERT_TRIGGER_DDL = """
CREATE TRIGGER trg_v4_job_claim_token_bi
BEFORE INSERT ON st_job_claim_token_v4
FOR EACH ROW
BEGIN
    IF BINARY NEW.lease_token NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.job_id = ''
       OR BINARY NEW.job_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.attempt_count < 1
       OR NEW.lease_owner = ''
       OR BINARY NEW.lease_owner REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.claimed_at > DATE_ADD(
            UTC_TIMESTAMP(6), INTERVAL 5 SECOND
       )
       OR NEW.lease_until <= NEW.claimed_at
       OR NEW.lease_until > DATE_ADD(
            NEW.claimed_at, INTERVAL 900 SECOND
       )
       OR NEW.lease_until >
            DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 900 SECOND) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 claim token registry row';
    END IF;
    IF (SELECT COUNT(*) FROM st_job_run_v4
        WHERE job_id = NEW.job_id
          AND status = 'RUNNING'
          AND attempt_count = NEW.attempt_count
          AND lease_owner = NEW.lease_owner
          AND lease_token = NEW.lease_token
          AND lease_until = NEW.lease_until
          AND updated_at = NEW.claimed_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 claim token lacks exact live lease';
    END IF;
END
"""
_CLAIM_TOKEN_REGISTRY_UPDATE_TRIGGER_DDL = """
CREATE TRIGGER trg_v4_job_claim_token_bu
BEFORE UPDATE ON st_job_claim_token_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 claim token registry is append-only';
END
"""
_CLAIM_TOKEN_REGISTRY_DELETE_TRIGGER_DDL = """
CREATE TRIGGER trg_v4_job_claim_token_bd
BEFORE DELETE ON st_job_claim_token_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 claim token registry is append-only';
END
"""

MIGRATIONS = (
    *MIGRATIONS,
    {
        "version": CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION,
        "statements": (
            _CLAIM_TOKEN_REGISTRY_TABLE_DDL,
            _CLAIM_TOKEN_REGISTRY_INSERT_TRIGGER_DDL,
            _CLAIM_TOKEN_REGISTRY_UPDATE_TRIGGER_DDL,
            _CLAIM_TOKEN_REGISTRY_DELETE_TRIGGER_DDL,
        ),
    },
)

MIGRATIONS = (*MIGRATIONS, CONTROL_GUARD_MIGRATION)


PIT_FACTOR_REGISTRY_MIGRATION_VERSION = (
    "20260804_005_v4_pit_factor_registry"
)
PIT_FACTOR_GUARD_MIGRATION_VERSION = "20260804_006_v4_pit_factor_guards"
PIT_FACTOR_LINEAGE_MIGRATION_VERSION = "20260804_007_v4_factor_lineage"
PIT_FACTOR_REGISTRY_TABLES = (
    "st_data_source_certification_v4",
    "st_factor_definition_v4",
    "st_entity_feature_snapshot_v4",
)

_DATA_SOURCE_CERTIFICATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_data_source_certification_v4 (
    source_key VARCHAR(120) NOT NULL,
    certification_version VARCHAR(120) NOT NULL,
    source_table VARCHAR(120) NOT NULL,
    event_time_column VARCHAR(120) NOT NULL DEFAULT '',
    knowledge_time_columns_json LONGTEXT NOT NULL,
    replay_eligibility VARCHAR(24) NOT NULL,
    certification_status VARCHAR(24) NOT NULL,
    availability_status VARCHAR(24) NOT NULL,
    research_status VARCHAR(24) NOT NULL,
    quality_status VARCHAR(24) NOT NULL,
    valid_from DATETIME(6) NOT NULL,
    valid_to DATETIME(6) NULL,
    contract_json LONGTEXT NOT NULL,
    evidence_hash CHAR(64) NOT NULL,
    certified_by VARCHAR(160) NOT NULL,
    certified_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (source_key, certification_version),
    KEY idx_v4_source_cert_status
        (certification_status, replay_eligibility, certified_at),
    KEY idx_v4_source_cert_validity (source_key, valid_from, valid_to),
    KEY idx_v4_source_cert_evidence (evidence_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""

_FACTOR_DEFINITION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_factor_definition_v4 (
    factor_key VARCHAR(120) NOT NULL,
    factor_version VARCHAR(120) NOT NULL,
    feature_set_version VARCHAR(120) NOT NULL,
    factor_role VARCHAR(32) NOT NULL,
    scope_type VARCHAR(32) NOT NULL,
    availability_status VARCHAR(24) NOT NULL,
    research_status VARCHAR(24) NOT NULL,
    quality_status VARCHAR(24) NOT NULL,
    missing_policy VARCHAR(24) NOT NULL,
    pit_eligible TINYINT(1) NOT NULL DEFAULT 0,
    required_source_keys_json LONGTEXT NOT NULL,
    formula_json LONGTEXT NOT NULL,
    output_schema_json LONGTEXT NOT NULL,
    definition_hash CHAR(64) NOT NULL,
    available_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (factor_key, factor_version),
    UNIQUE KEY uk_v4_factor_feature_set
        (factor_key, feature_set_version),
    KEY idx_v4_factor_availability
        (availability_status, pit_eligible, available_at),
    KEY idx_v4_factor_feature_set
        (feature_set_version, availability_status),
    KEY idx_v4_factor_definition_hash (definition_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""

_ENTITY_FEATURE_SNAPSHOT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_entity_feature_snapshot_v4 (
    snapshot_id CHAR(64) NOT NULL,
    run_uid VARCHAR(64) NOT NULL,
    scope_type VARCHAR(32) NOT NULL,
    scope_id VARCHAR(160) NOT NULL,
    feature_set_version VARCHAR(120) NOT NULL,
    knowledge_cutoff_at DATETIME(6) NOT NULL,
    computed_at DATETIME(6) NOT NULL,
    available_at DATETIME(6) NOT NULL,
    factor_count INT UNSIGNED NOT NULL,
    values_json LONGTEXT NOT NULL,
    quality_status VARCHAR(24) NOT NULL,
    quality_json LONGTEXT NOT NULL,
    source_certifications_json LONGTEXT NOT NULL,
    source_manifest_hash CHAR(64) NOT NULL,
    feature_hash CHAR(64) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    CONSTRAINT fk_v4_feature_snapshot_run
        FOREIGN KEY (run_uid)
        REFERENCES st_decision_run_v4 (run_uid)
        ON DELETE RESTRICT,
    PRIMARY KEY (snapshot_id),
    UNIQUE KEY uk_v4_feature_snapshot_identity
        (run_uid, scope_type, scope_id, feature_set_version),
    KEY idx_v4_feature_snapshot_scope
        (scope_type, scope_id, computed_at),
    KEY idx_v4_feature_snapshot_run (run_uid, computed_at),
    KEY idx_v4_feature_snapshot_hash (feature_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""

PIT_FACTOR_REGISTRY_TABLE_DDLS = (
    _DATA_SOURCE_CERTIFICATION_TABLE_DDL,
    _FACTOR_DEFINITION_TABLE_DDL,
    _ENTITY_FEATURE_SNAPSHOT_TABLE_DDL,
)
MIGRATIONS = (
    *MIGRATIONS,
    {
        "version": PIT_FACTOR_REGISTRY_MIGRATION_VERSION,
        "statements": PIT_FACTOR_REGISTRY_TABLE_DDLS,
    },
)

_FACTOR_MAX_AGE_COLUMN_DDL = """
ALTER TABLE st_factor_definition_v4
    ADD COLUMN max_age_seconds INT UNSIGNED NOT NULL AFTER pit_eligible
"""
_FACTOR_SOURCE_LINEAGE_COLUMN_DDL = """
ALTER TABLE st_factor_definition_v4
    ADD COLUMN required_source_certifications_json LONGTEXT NOT NULL
    AFTER required_source_keys_json
"""
_SNAPSHOT_FACTOR_LINEAGE_COLUMN_DDL = """
ALTER TABLE st_entity_feature_snapshot_v4
    ADD COLUMN factor_definitions_json LONGTEXT NOT NULL
        AFTER source_certifications_json
"""
_FACTOR_LINEAGE_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_factor_lineage_bi
BEFORE INSERT ON st_factor_definition_v4
FOR EACH ROW
BEGIN
    IF NEW.max_age_seconds < 1
       OR JSON_VALID(NEW.required_source_certifications_json) <> 1
       OR JSON_TYPE(NEW.required_source_certifications_json) <> 'ARRAY'
       OR JSON_LENGTH(NEW.required_source_certifications_json) < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 factor lineage';
    END IF;
END
"""
_SNAPSHOT_FACTOR_LINEAGE_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_feature_snapshot_lineage_bi
BEFORE INSERT ON st_entity_feature_snapshot_v4
FOR EACH ROW
BEGIN
    IF JSON_LENGTH(NEW.values_json) <> NEW.factor_count
       OR JSON_VALID(NEW.factor_definitions_json) <> 1
       OR JSON_TYPE(NEW.factor_definitions_json) <> 'ARRAY'
       OR JSON_LENGTH(NEW.factor_definitions_json) < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 feature snapshot lineage';
    END IF;
END
"""
PIT_FACTOR_LINEAGE_TRIGGER_SPECS = (
    (
        "trg_v4_factor_lineage_bi",
        "INSERT",
        "st_factor_definition_v4",
        _FACTOR_LINEAGE_INSERT_GUARD_DDL,
    ),
    (
        "trg_v4_feature_snapshot_lineage_bi",
        "INSERT",
        "st_entity_feature_snapshot_v4",
        _SNAPSHOT_FACTOR_LINEAGE_INSERT_GUARD_DDL,
    ),
)
PIT_FACTOR_LINEAGE_STATEMENTS = (
    _FACTOR_MAX_AGE_COLUMN_DDL,
    _FACTOR_SOURCE_LINEAGE_COLUMN_DDL,
    _SNAPSHOT_FACTOR_LINEAGE_COLUMN_DDL,
    _FACTOR_LINEAGE_INSERT_GUARD_DDL,
    _SNAPSHOT_FACTOR_LINEAGE_INSERT_GUARD_DDL,
)
PIT_FACTOR_LINEAGE_TABLE_DDLS = (
    _DATA_SOURCE_CERTIFICATION_TABLE_DDL,
    _FACTOR_DEFINITION_TABLE_DDL.replace(
        "    pit_eligible TINYINT(1) NOT NULL DEFAULT 0,\n",
        "    pit_eligible TINYINT(1) NOT NULL DEFAULT 0,\n"
        "    max_age_seconds INT UNSIGNED NOT NULL,\n",
    ).replace(
        "    required_source_keys_json LONGTEXT NOT NULL,\n",
        "    required_source_keys_json LONGTEXT NOT NULL,\n"
        "    required_source_certifications_json LONGTEXT NOT NULL,\n",
    ),
    _ENTITY_FEATURE_SNAPSHOT_TABLE_DDL.replace(
        "    source_certifications_json LONGTEXT NOT NULL,\n",
        "    source_certifications_json LONGTEXT NOT NULL,\n"
        "    factor_definitions_json LONGTEXT NOT NULL,\n",
    ),
)
_DATA_SOURCE_CERTIFICATION_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_source_cert_bi
BEFORE INSERT ON st_data_source_certification_v4
FOR EACH ROW
BEGIN
    IF NEW.source_key = ''
       OR NEW.certification_version = ''
       OR NEW.source_table = ''
       OR NEW.certified_by = ''
       OR BINARY NEW.source_key REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.certification_version REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.source_table REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.event_time_column REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.certified_by REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.replay_eligibility NOT IN
            ('PIT_CERTIFIED','FORWARD_ONLY','DISPLAY_ONLY',
             'REPLAY_INELIGIBLE')
       OR NEW.certification_status NOT IN
            ('PENDING','PASSED','FAILED','REVOKED')
       OR NEW.availability_status NOT IN
            ('ACTIVE','DEGRADED','BLOCKED')
       OR NEW.research_status NOT IN
            ('BACKTEST_READY','FORWARD_ONLY','DISPLAY_ONLY')
       OR NEW.quality_status NOT IN ('PASS','WARN','FAIL')
       OR (NEW.replay_eligibility = 'PIT_CERTIFIED'
           AND (NEW.certification_status <> 'PASSED'
                OR NEW.availability_status <> 'ACTIVE'
                OR NEW.research_status <> 'BACKTEST_READY'
                OR NEW.quality_status <> 'PASS'))
       OR (NEW.replay_eligibility <> 'PIT_CERTIFIED'
           AND NEW.research_status = 'BACKTEST_READY')
       OR (NEW.certification_status <> 'PASSED'
           AND (NEW.availability_status = 'ACTIVE'
                OR NEW.quality_status = 'PASS'))
       OR (NEW.replay_eligibility = 'PIT_CERTIFIED'
           AND NEW.event_time_column = '')
       OR JSON_VALID(NEW.knowledge_time_columns_json) <> 1
       OR JSON_TYPE(NEW.knowledge_time_columns_json) <> 'ARRAY'
       OR (NEW.replay_eligibility = 'PIT_CERTIFIED'
           AND JSON_LENGTH(NEW.knowledge_time_columns_json) < 1)
       OR JSON_VALID(NEW.contract_json) <> 1
       OR JSON_TYPE(NEW.contract_json) <> 'OBJECT'
       OR JSON_EXTRACT(NEW.contract_json, '$.revision_policy') IS NULL
       OR JSON_TYPE(
            JSON_EXTRACT(NEW.contract_json, '$.revision_policy')) <> 'STRING'
       OR JSON_UNQUOTE(
            JSON_EXTRACT(NEW.contract_json, '$.revision_policy')) = ''
       OR JSON_UNQUOTE(
            JSON_EXTRACT(NEW.contract_json, '$.revision_policy')) <>
          UPPER(TRIM(JSON_UNQUOTE(
            JSON_EXTRACT(NEW.contract_json, '$.revision_policy'))))
       OR (NEW.replay_eligibility = 'PIT_CERTIFIED'
           AND JSON_UNQUOTE(JSON_EXTRACT(
                NEW.contract_json, '$.revision_policy')) NOT IN
                ('APPEND_ONLY_REVISION_CHAIN',
                 'BITEMPORAL_REVISION_CHAIN','IMMUTABLE_EVENT_LOG'))
       OR BINARY NEW.evidence_hash NOT REGEXP '^[0-9a-f]{64}$'
       OR (NEW.valid_to IS NOT NULL
           AND NEW.valid_to <= NEW.valid_from)
       OR NEW.certified_at > NEW.created_at THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 source certification';
    END IF;
END
"""
_DATA_SOURCE_CERTIFICATION_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_source_cert_bu
BEFORE UPDATE ON st_data_source_certification_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 source certification is append-only';
END
"""
_DATA_SOURCE_CERTIFICATION_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_source_cert_bd
BEFORE DELETE ON st_data_source_certification_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 source certification is append-only';
END
"""

_FACTOR_DEFINITION_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_factor_definition_bi
BEFORE INSERT ON st_factor_definition_v4
FOR EACH ROW
BEGIN
    IF NEW.factor_key = ''
       OR NEW.factor_version = ''
       OR NEW.feature_set_version = ''
       OR BINARY NEW.factor_key REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.factor_version REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.feature_set_version REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.factor_role NOT IN
            ('GATE','STATE','ALPHA','RISK','COST','PORTFOLIO',
             'EXPLANATION')
       OR NEW.scope_type NOT IN
            ('MARKET','SECTOR','INSTRUMENT','PORTFOLIO')
       OR NEW.availability_status NOT IN
            ('ACTIVE','DEGRADED','BLOCKED')
       OR NEW.research_status NOT IN
            ('BACKTEST_READY','FORWARD_ONLY','DISPLAY_ONLY')
       OR NEW.quality_status NOT IN ('PASS','WARN','FAIL')
       OR NEW.missing_policy NOT IN
            ('BLOCK','PROPAGATE_NULL','DISPLAY_ONLY')
       OR NEW.pit_eligible NOT IN (0, 1)
       OR (NEW.pit_eligible = 1
           AND NEW.research_status <> 'BACKTEST_READY')
       OR (NEW.availability_status = 'ACTIVE'
           AND NEW.quality_status = 'FAIL')
       OR JSON_VALID(NEW.required_source_keys_json) <> 1
       OR JSON_TYPE(NEW.required_source_keys_json) <> 'ARRAY'
       OR JSON_LENGTH(NEW.required_source_keys_json) < 1
       OR JSON_VALID(NEW.formula_json) <> 1
       OR JSON_TYPE(NEW.formula_json) <> 'OBJECT'
       OR JSON_VALID(NEW.output_schema_json) <> 1
       OR JSON_TYPE(NEW.output_schema_json) <> 'OBJECT'
       OR BINARY NEW.definition_hash NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.available_at > NEW.created_at THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 factor definition';
    END IF;
END
"""
_FACTOR_DEFINITION_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_factor_definition_bu
BEFORE UPDATE ON st_factor_definition_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 factor definition is append-only';
END
"""
_FACTOR_DEFINITION_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_factor_definition_bd
BEFORE DELETE ON st_factor_definition_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 factor definition is append-only';
END
"""

_ENTITY_FEATURE_SNAPSHOT_INSERT_GUARD_DDL = """
CREATE TRIGGER trg_v4_feature_snapshot_bi
BEFORE INSERT ON st_entity_feature_snapshot_v4
FOR EACH ROW
BEGIN
    IF BINARY NEW.snapshot_id NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.run_uid = ''
       OR NEW.scope_id = ''
       OR NEW.feature_set_version = ''
       OR BINARY NEW.run_uid REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.scope_id REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR BINARY NEW.feature_set_version REGEXP
            '(^[[:space:]])|([[:space:]]$)'
       OR NEW.scope_type NOT IN
            ('MARKET','SECTOR','INSTRUMENT','PORTFOLIO')
       OR NEW.quality_status NOT IN ('PASS','WARN','FAIL')
       OR NEW.factor_count < 1
       OR JSON_VALID(NEW.values_json) <> 1
       OR JSON_TYPE(NEW.values_json) <> 'OBJECT'
       OR JSON_VALID(NEW.quality_json) <> 1
       OR JSON_TYPE(NEW.quality_json) <> 'OBJECT'
       OR JSON_VALID(NEW.source_certifications_json) <> 1
       OR JSON_TYPE(NEW.source_certifications_json) <> 'ARRAY'
       OR JSON_LENGTH(NEW.source_certifications_json) < 1
       OR BINARY NEW.source_manifest_hash NOT REGEXP '^[0-9a-f]{64}$'
       OR BINARY NEW.feature_hash NOT REGEXP '^[0-9a-f]{64}$'
       OR NEW.computed_at < NEW.knowledge_cutoff_at
       OR NEW.available_at < NEW.computed_at
       OR NEW.created_at < NEW.available_at THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid V4 feature snapshot';
    END IF;
    IF (SELECT COUNT(*)
        FROM st_decision_run_v4 r
        JOIN st_decision_context_v4 c ON c.context_id = r.context_id
        WHERE r.run_uid = NEW.run_uid
          AND r.status IN ('RUNNING','VALIDATING')
          AND c.knowledge_cutoff_at = NEW.knowledge_cutoff_at
          AND c.data_snapshot_hash = NEW.source_manifest_hash
          AND c.decision_at <= NEW.computed_at) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V4 feature snapshot lacks exact PIT run';
    END IF;
END
"""
_ENTITY_FEATURE_SNAPSHOT_UPDATE_GUARD_DDL = """
CREATE TRIGGER trg_v4_feature_snapshot_bu
BEFORE UPDATE ON st_entity_feature_snapshot_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 feature snapshot is append-only';
END
"""
_ENTITY_FEATURE_SNAPSHOT_DELETE_GUARD_DDL = """
CREATE TRIGGER trg_v4_feature_snapshot_bd
BEFORE DELETE ON st_entity_feature_snapshot_v4
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V4 feature snapshot is append-only';
END
"""

PIT_FACTOR_GUARD_TRIGGER_SPECS = (
    (
        "trg_v4_source_cert_bi",
        "INSERT",
        "st_data_source_certification_v4",
        _DATA_SOURCE_CERTIFICATION_INSERT_GUARD_DDL,
    ),
    (
        "trg_v4_source_cert_bu",
        "UPDATE",
        "st_data_source_certification_v4",
        _DATA_SOURCE_CERTIFICATION_UPDATE_GUARD_DDL,
    ),
    (
        "trg_v4_source_cert_bd",
        "DELETE",
        "st_data_source_certification_v4",
        _DATA_SOURCE_CERTIFICATION_DELETE_GUARD_DDL,
    ),
    (
        "trg_v4_factor_definition_bi",
        "INSERT",
        "st_factor_definition_v4",
        _FACTOR_DEFINITION_INSERT_GUARD_DDL,
    ),
    (
        "trg_v4_factor_definition_bu",
        "UPDATE",
        "st_factor_definition_v4",
        _FACTOR_DEFINITION_UPDATE_GUARD_DDL,
    ),
    (
        "trg_v4_factor_definition_bd",
        "DELETE",
        "st_factor_definition_v4",
        _FACTOR_DEFINITION_DELETE_GUARD_DDL,
    ),
    (
        "trg_v4_feature_snapshot_bi",
        "INSERT",
        "st_entity_feature_snapshot_v4",
        _ENTITY_FEATURE_SNAPSHOT_INSERT_GUARD_DDL,
    ),
    (
        "trg_v4_feature_snapshot_bu",
        "UPDATE",
        "st_entity_feature_snapshot_v4",
        _ENTITY_FEATURE_SNAPSHOT_UPDATE_GUARD_DDL,
    ),
    (
        "trg_v4_feature_snapshot_bd",
        "DELETE",
        "st_entity_feature_snapshot_v4",
        _ENTITY_FEATURE_SNAPSHOT_DELETE_GUARD_DDL,
    ),
)
PIT_FACTOR_GUARD_TRIGGER_NAMES = tuple(
    name
    for name, _event, _table, _statement in PIT_FACTOR_GUARD_TRIGGER_SPECS
)
MIGRATIONS = (
    *MIGRATIONS,
    {
        "version": PIT_FACTOR_GUARD_MIGRATION_VERSION,
        "statements": tuple(
            statement
            for _name, _event, _table, statement in (
                PIT_FACTOR_GUARD_TRIGGER_SPECS
            )
        ),
    },
)
MIGRATIONS = (
    *MIGRATIONS,
    {
        "version": PIT_FACTOR_LINEAGE_MIGRATION_VERSION,
        "statements": PIT_FACTOR_LINEAGE_STATEMENTS,
    },
)


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Za-z0-9_]+)",
    flags=re.IGNORECASE,
)
_COLUMN_RE = re.compile(
    r"^`?([A-Za-z0-9_]+)`?\s+"
    r"([A-Za-z]+(?:\([^)]*\))?(?:\s+UNSIGNED)?)(.*)$",
    flags=re.IGNORECASE | re.DOTALL,
)
_DEFAULT_RE = re.compile(
    r"\bDEFAULT\s+(CURRENT_TIMESTAMP(?:\(\d+\))?|'(?:''|[^'])*'|"
    r'"(?:""|[^"])*"|[^\s]+)',
    flags=re.IGNORECASE,
)
_PRIMARY_KEY_RE = re.compile(
    r"^PRIMARY\s+KEY\s*\(([^)]*)\)$",
    flags=re.IGNORECASE,
)
_INDEX_RE = re.compile(
    r"^(UNIQUE\s+)?KEY\s+`?([A-Za-z0-9_]+)`?\s*\(([^)]*)\)$",
    flags=re.IGNORECASE,
)
_CONSTRAINT_RE = re.compile(
    r"^CONSTRAINT\s+`?([A-Za-z0-9_]+)`?\s+"
    r"FOREIGN\s+KEY\s*\(([^)]*)\)\s+"
    r"REFERENCES\s+`?([A-Za-z0-9_]+)`?\s*\(([^)]*)\)\s+"
    r"ON\s+DELETE\s+([A-Za-z]+)$",
    flags=re.IGNORECASE,
)

JOB_LEASE_COLUMN_CONTRACT: dict[str, dict[str, Any]] = {
    "lease_token": {
        "type": "char(64)",
        "nullable": True,
        "default": None,
        "character_set": "utf8mb4",
        "collation": "utf8mb4_bin",
        "extra": "",
    },
    "max_attempts": {
        "type": "int unsigned",
        "nullable": False,
        "default": "3",
        "character_set": None,
        "collation": None,
        "extra": "",
    },
}
JOB_LEASE_INDEX_CONTRACT: dict[str, dict[str, Any]] = {
    "idx_v4_job_claim_due": {
        "unique": False,
        "columns": (
            "status",
            "next_attempt_at",
            "lease_until",
            "scheduled_for",
            "attempt_count",
            "max_attempts",
            "job_id",
        ),
        "sub_parts": (None, None, None, None, None, None, None),
        "collations": ("A", "A", "A", "A", "A", "A", "A"),
        "index_type": "BTREE",
    },
    "uk_v4_job_lease_token": {
        "unique": True,
        "columns": ("lease_token",),
        "sub_parts": (None,),
        "collations": ("A",),
        "index_type": "BTREE",
    },
}
CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT: dict[str, dict[str, Any]] = {
    "lease_token": {
        "type": "char(64)",
        "nullable": False,
        "default": None,
        "character_set": "utf8mb4",
        "collation": "utf8mb4_bin",
        "extra": "",
    },
    "job_id": {
        "type": "varchar(64)",
        "nullable": False,
        "default": None,
        "character_set": "utf8mb4",
        "collation": "utf8mb4_bin",
        "extra": "",
    },
    "attempt_count": {
        "type": "int unsigned",
        "nullable": False,
        "default": None,
        "character_set": None,
        "collation": None,
        "extra": "",
    },
    "lease_owner": {
        "type": "varchar(160)",
        "nullable": False,
        "default": None,
        "character_set": "utf8mb4",
        "collation": "utf8mb4_bin",
        "extra": "",
    },
    "claimed_at": {
        "type": "datetime(6)",
        "nullable": False,
        "default": None,
        "character_set": None,
        "collation": None,
        "extra": "",
    },
    "lease_until": {
        "type": "datetime(6)",
        "nullable": False,
        "default": None,
        "character_set": None,
        "collation": None,
        "extra": "",
    },
}
CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT: dict[str, dict[str, Any]] = {
    "PRIMARY": {
        "unique": True,
        "columns": ("lease_token",),
        "sub_parts": (None,),
        "collations": ("A",),
        "index_type": "BTREE",
    },
    "uk_v4_job_claim_attempt": {
        "unique": True,
        "columns": ("job_id", "attempt_count"),
        "sub_parts": (None, None),
        "collations": ("A", "A"),
        "index_type": "BTREE",
    },
}
CLAIM_TOKEN_REGISTRY_CONSTRAINT_CONTRACT: dict[str, dict[str, Any]] = {
    "fk_v4_job_claim_token_job": {
        "columns": ("job_id",),
        "referenced_table": JOB_LEASE_TABLE,
        "referenced_columns": ("job_id",),
        "on_delete": "RESTRICT",
    },
}


def _base_column_contract(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": value["type"],
        "nullable": value["nullable"],
        "default": value["default"],
    }


def _base_index_contract(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "unique": value["unique"],
        "columns": value["columns"],
    }


def _normalize_column_type(value: Any) -> str:
    normalized = " ".join(str(value).casefold().split())
    integer = re.fullmatch(
        r"(smallint|mediumint|int|integer|bigint)\(\d+\)( unsigned)?",
        normalized,
    )
    if integer is not None:
        suffix = integer.group(2) or ""
        return f"{integer.group(1)}{suffix}"
    return normalized


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {"'", '"'}
    ):
        quote = normalized[0]
        normalized = normalized[1:-1].replace(quote * 2, quote)
    expression = "".join(normalized.casefold().split())
    if re.fullmatch(r"current_timestamp(?:\(\d+\))?", expression):
        return expression
    return normalized


def _split_table_definitions(body: str) -> tuple[str, ...]:
    definitions: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    escaped = False
    for character in body:
        if quote is not None:
            current.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {"'", '"', "`"}:
            quote = character
            current.append(character)
        elif character == "(":
            depth += 1
            current.append(character)
        elif character == ")":
            depth -= 1
            current.append(character)
        elif character == "," and depth == 0:
            definition = " ".join("".join(current).split())
            if definition:
                definitions.append(definition)
            current = []
        else:
            current.append(character)
    definition = " ".join("".join(current).split())
    if definition:
        definitions.append(definition)
    return tuple(definitions)


def _identifier_list(value: str) -> tuple[str, ...]:
    identifiers: list[str] = []
    for raw_identifier in value.split(","):
        identifier = raw_identifier.strip().split()[0].strip("`")
        identifiers.append(identifier)
    return tuple(identifiers)


def _expected_schema(
    statements: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for statement in statements:
        table_match = _CREATE_TABLE_RE.search(statement)
        if table_match is None:
            continue
        table_name = table_match.group(1)
        body = statement[statement.find("(") + 1 : statement.rfind(")")]
        columns: dict[str, dict[str, Any]] = {}
        indexes: dict[str, dict[str, Any]] = {}
        constraints: dict[str, dict[str, Any]] = {}
        for definition in _split_table_definitions(body):
            primary_match = _PRIMARY_KEY_RE.match(definition)
            if primary_match is not None:
                indexes["PRIMARY"] = {
                    "unique": True,
                    "columns": _identifier_list(primary_match.group(1)),
                }
                continue
            index_match = _INDEX_RE.match(definition)
            if index_match is not None:
                indexes[index_match.group(2)] = {
                    "unique": bool(index_match.group(1)),
                    "columns": _identifier_list(index_match.group(3)),
                }
                continue
            constraint_match = _CONSTRAINT_RE.match(definition)
            if constraint_match is not None:
                constraints[constraint_match.group(1)] = {
                    "columns": _identifier_list(constraint_match.group(2)),
                    "referenced_table": constraint_match.group(3),
                    "referenced_columns": _identifier_list(
                        constraint_match.group(4)
                    ),
                    "on_delete": constraint_match.group(5).upper(),
                }
                continue
            column_match = _COLUMN_RE.match(definition)
            if column_match is not None:
                column_name = column_match.group(1)
                remainder = column_match.group(3)
                upper_remainder = remainder.upper()
                default_match = _DEFAULT_RE.search(remainder)
                columns[column_name] = {
                    "type": _normalize_column_type(column_match.group(2)),
                    "nullable": not (
                        "NOT NULL" in upper_remainder
                        or "PRIMARY KEY" in upper_remainder
                    ),
                    "default": _normalize_default(
                        default_match.group(1) if default_match else None
                    ),
                }
                if "PRIMARY KEY" in upper_remainder:
                    indexes["PRIMARY"] = {
                        "unique": True,
                        "columns": (column_name,),
                    }
        expected[table_name] = {
            "columns": columns,
            "indexes": indexes,
            "constraints": constraints,
        }
    return expected


def _validate_schema_on_connection(
    connection: Connection,
    statements: tuple[str, ...],
) -> None:
    """Fail closed when IF NOT EXISTS concealed a partial/wrong table."""

    expected = _expected_schema(statements)
    for table_name, signature in expected.items():
        table = connection.execute(
                text(
                    "SELECT ENGINE, TABLE_COLLATION "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = :table_name"
                ),
                {"table_name": table_name},
            ).mappings().first()
        if table is None:
            raise RuntimeError(
                f"V4 migration table is missing after DDL: {table_name}"
            )
        if str(table["ENGINE"] or "").casefold() != "innodb":
            raise RuntimeError(
                f"V4 table must use InnoDB: {table_name}"
            )
        if str(table["TABLE_COLLATION"] or "").casefold() != "utf8mb4_bin":
            raise RuntimeError(
                f"V4 table must use utf8mb4_bin: {table_name}"
            )

        actual_columns = {
            str(row["COLUMN_NAME"]): {
                "type": _normalize_column_type(row["COLUMN_TYPE"]),
                "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
                "default": _normalize_default(row["COLUMN_DEFAULT"]),
            }
            for row in connection.execute(
                    text(
                        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                        "COLUMN_DEFAULT "
                        "FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = DATABASE() "
                        "AND TABLE_NAME = :table_name"
                    ),
                    {"table_name": table_name},
            ).mappings()
        }
        expected_columns = signature["columns"]
        missing_columns = set(expected_columns) - set(actual_columns)
        allowed_forward_columns: dict[str, dict[str, Any]] = {}
        if table_name == JOB_LEASE_TABLE:
            allowed_forward_columns = {
                name: _base_column_contract(details)
                for name, details in JOB_LEASE_COLUMN_CONTRACT.items()
            }
        if table_name in PIT_FACTOR_REGISTRY_TABLES:
            final_columns = _expected_schema(PIT_FACTOR_LINEAGE_TABLE_DDLS)[
                table_name
            ]["columns"]
            allowed_forward_columns.update(
                {
                    name: details
                    for name, details in final_columns.items()
                    if name not in expected_columns
                }
            )
        unexpected_columns = (
            set(actual_columns)
            - set(expected_columns)
            - set(allowed_forward_columns)
        )
        column_mismatches = {
            name: (expected_columns[name], actual_columns.get(name))
            for name in expected_columns
            if name in actual_columns
            and actual_columns[name] != expected_columns[name]
        }
        forward_column_mismatches = {
            name: (details, actual_columns.get(name))
            for name, details in allowed_forward_columns.items()
            if name in actual_columns and actual_columns[name] != details
        }
        if (
            missing_columns
            or unexpected_columns
            or column_mismatches
            or forward_column_mismatches
        ):
            raise RuntimeError(
                "V4 table column drift detected for "
                f"{table_name}: missing={sorted(missing_columns)} "
                f"unexpected={sorted(unexpected_columns)} "
                "mismatches="
                f"{column_mismatches | forward_column_mismatches}"
            )

        actual_index_parts: dict[str, dict[str, Any]] = {}
        index_rows = connection.execute(
            text(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = :table_name "
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
            ),
            {"table_name": table_name},
        ).mappings()
        for row in index_rows:
            index_name = str(row["INDEX_NAME"])
            entry = actual_index_parts.setdefault(
                index_name,
                {
                    "unique": int(row["NON_UNIQUE"]) == 0,
                    "columns": [],
                },
            )
            entry["columns"].append(str(row["COLUMN_NAME"]))
        actual_indexes = {
            name: {
                "unique": details["unique"],
                "columns": tuple(details["columns"]),
            }
            for name, details in actual_index_parts.items()
        }
        expected_indexes = dict(signature["indexes"])
        allowed_forward_indexes: dict[str, dict[str, Any]] = {}
        if table_name == JOB_LEASE_TABLE:
            allowed_forward_indexes = {
                name: _base_index_contract(details)
                for name, details in JOB_LEASE_INDEX_CONTRACT.items()
            }
        missing_indexes = set(expected_indexes) - set(actual_indexes)
        unexpected_indexes = (
            set(actual_indexes)
            - set(expected_indexes)
            - set(allowed_forward_indexes)
        )
        index_mismatches = {
            name: (details, actual_indexes.get(name))
            for name, details in expected_indexes.items()
            if name in actual_indexes and actual_indexes[name] != details
        }
        forward_index_mismatches = {
            name: (details, actual_indexes.get(name))
            for name, details in allowed_forward_indexes.items()
            if name in actual_indexes and actual_indexes[name] != details
        }
        if (
            missing_indexes
            or unexpected_indexes
            or index_mismatches
            or forward_index_mismatches
        ):
            raise RuntimeError(
                "V4 table index drift detected for "
                f"{table_name}: missing={sorted(missing_indexes)} "
                f"unexpected={sorted(unexpected_indexes)} mismatches="
                f"{index_mismatches | forward_index_mismatches}"
            )

        actual_constraint_parts: dict[str, dict[str, Any]] = {}
        constraint_rows = connection.execute(
            text(
                "SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, "
                "k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, "
                "k.ORDINAL_POSITION, r.DELETE_RULE "
                "FROM information_schema.KEY_COLUMN_USAGE k "
                "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
                "ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA "
                "AND r.TABLE_NAME = k.TABLE_NAME "
                "AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                "WHERE k.CONSTRAINT_SCHEMA = DATABASE() "
                "AND k.TABLE_NAME = :table_name "
                "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
                "ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION"
            ),
            {"table_name": table_name},
        ).mappings()
        for row in constraint_rows:
            constraint_name = str(row["CONSTRAINT_NAME"])
            entry = actual_constraint_parts.setdefault(
                constraint_name,
                {
                    "columns": [],
                    "referenced_table": str(row["REFERENCED_TABLE_NAME"]),
                    "referenced_columns": [],
                    "on_delete": str(row["DELETE_RULE"]).upper(),
                },
            )
            entry["columns"].append(str(row["COLUMN_NAME"]))
            entry["referenced_columns"].append(
                str(row["REFERENCED_COLUMN_NAME"])
            )
        actual_constraints = {
            name: {
                "columns": tuple(details["columns"]),
                "referenced_table": details["referenced_table"],
                "referenced_columns": tuple(details["referenced_columns"]),
                "on_delete": details["on_delete"],
            }
            for name, details in actual_constraint_parts.items()
        }
        if actual_constraints != signature["constraints"]:
            raise RuntimeError(
                "V4 table foreign-key drift detected for "
                f"{table_name}: expected={signature['constraints']} "
                f"actual={actual_constraints}"
            )


def _normalize_optional_identifier(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip().strip("`").casefold()


def _column_signature(
    connection: Connection,
    table_name: str,
    column_name: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        text(
            "SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
            "CHARACTER_SET_NAME, COLLATION_NAME, EXTRA "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table_name "
            "AND COLUMN_NAME = :column_name"
        ),
        {"table_name": table_name, "column_name": column_name},
    ).mappings().first()
    if row is None:
        return None
    return {
        "type": _normalize_column_type(row["COLUMN_TYPE"]),
        "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
        "default": _normalize_default(row["COLUMN_DEFAULT"]),
        "character_set": _normalize_optional_identifier(
            row["CHARACTER_SET_NAME"]
        ),
        "collation": _normalize_optional_identifier(row["COLLATION_NAME"]),
        "extra": " ".join(str(row["EXTRA"] or "").casefold().split()),
    }


def _job_lease_column_signature(
    connection: Connection,
    column_name: str,
) -> dict[str, Any] | None:
    return _column_signature(connection, JOB_LEASE_TABLE, column_name)


def _claim_token_registry_column_signature(
    connection: Connection,
    column_name: str,
) -> dict[str, Any] | None:
    return _column_signature(
        connection,
        CLAIM_TOKEN_REGISTRY_TABLE,
        column_name,
    )


def _index_signature(
    connection: Connection,
    table_name: str,
    index_name: str,
) -> dict[str, Any] | None:
    rows = tuple(
        connection.execute(
            text(
                "SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, "
                "COLLATION, INDEX_TYPE "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = :table_name "
                "AND INDEX_NAME = :index_name "
                "ORDER BY SEQ_IN_INDEX"
            ),
            {"table_name": table_name, "index_name": index_name},
        ).mappings()
    )
    if not rows:
        return None
    return {
        "unique": int(rows[0]["NON_UNIQUE"]) == 0,
        "columns": tuple(str(row["COLUMN_NAME"]) for row in rows),
        "sub_parts": tuple(
            int(row["SUB_PART"]) if row["SUB_PART"] is not None else None
            for row in rows
        ),
        "collations": tuple(
            str(row["COLLATION"] or "").upper() or None for row in rows
        ),
        "index_type": str(rows[0]["INDEX_TYPE"] or "").upper(),
    }


def _job_lease_index_signature(
    connection: Connection,
    index_name: str,
) -> dict[str, Any] | None:
    return _index_signature(connection, JOB_LEASE_TABLE, index_name)


def _claim_token_registry_index_signature(
    connection: Connection,
    index_name: str,
) -> dict[str, Any] | None:
    return _index_signature(
        connection,
        CLAIM_TOKEN_REGISTRY_TABLE,
        index_name,
    )


def _normalize_trigger_body(value: Any) -> str:
    normalized = str(value or "").strip().rstrip(";").replace("`", "")
    return " ".join(normalized.casefold().split())


def _normalize_sql_mode(value: Any) -> tuple[str, ...]:
    return tuple(
        sorted(
            item.strip().upper()
            for item in str(value or "").split(",")
            if item.strip()
        )
    )


def _trigger_body_from_ddl(statement: str) -> str:
    marker = re.search(r"\bFOR\s+EACH\s+ROW\b", statement, re.IGNORECASE)
    if marker is None:
        raise RuntimeError("V4 trigger DDL lacks FOR EACH ROW")
    return statement[marker.end() :].strip()


def _job_lease_trigger_context(connection: Connection) -> dict[str, Any]:
    row = connection.execute(
        text(
            "SELECT DATABASE() AS CURRENT_DATABASE, "
            "CURRENT_USER() AS CURRENT_DEFINER, "
            "@@SESSION.sql_mode AS CURRENT_SQL_MODE, "
            "@@SESSION.character_set_client AS CURRENT_CHARACTER_SET_CLIENT, "
            "@@SESSION.collation_connection AS CURRENT_COLLATION_CONNECTION, "
            "(SELECT DEFAULT_COLLATION_NAME "
            " FROM information_schema.SCHEMATA "
            " WHERE SCHEMA_NAME = DATABASE()) AS DATABASE_COLLATION"
        )
    ).mappings().first()
    if row is None or not row["CURRENT_DATABASE"]:
        raise RuntimeError("V4 trigger context could not be attested")
    return {
        "schema": str(row["CURRENT_DATABASE"]),
        "definer": str(row["CURRENT_DEFINER"] or "")
        .replace("`", "")
        .replace("'", "")
        .casefold(),
        "sql_mode": _normalize_sql_mode(row["CURRENT_SQL_MODE"]),
        "character_set_client": _normalize_optional_identifier(
            row["CURRENT_CHARACTER_SET_CLIENT"]
        ),
        "collation_connection": _normalize_optional_identifier(
            row["CURRENT_COLLATION_CONNECTION"]
        ),
        "database_collation": _normalize_optional_identifier(
            row["DATABASE_COLLATION"]
        ),
    }


def _trigger_signatures(
    connection: Connection,
    table_name: str,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        text(
            "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_MANIPULATION, "
            "EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE, ACTION_ORDER, "
            "ACTION_CONDITION, ACTION_STATEMENT, ACTION_ORIENTATION, "
            "ACTION_TIMING, SQL_MODE, DEFINER, CHARACTER_SET_CLIENT, "
            "COLLATION_CONNECTION, DATABASE_COLLATION "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA = DATABASE() "
            "AND EVENT_OBJECT_TABLE = :table_name "
            "ORDER BY TRIGGER_NAME"
        ),
        {"table_name": table_name},
    ).mappings()
    signatures: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row["TRIGGER_NAME"])
        signatures[name] = {
            "trigger_schema": str(row["TRIGGER_SCHEMA"]),
            "event": str(row["EVENT_MANIPULATION"]).upper(),
            "object_schema": str(row["EVENT_OBJECT_SCHEMA"]),
            "object_table": str(row["EVENT_OBJECT_TABLE"]),
            "action_order": int(row["ACTION_ORDER"]),
            "action_condition": row["ACTION_CONDITION"],
            "action_statement": _normalize_trigger_body(
                row["ACTION_STATEMENT"]
            ),
            "orientation": str(row["ACTION_ORIENTATION"]).upper(),
            "timing": str(row["ACTION_TIMING"]).upper(),
            "sql_mode": _normalize_sql_mode(row["SQL_MODE"]),
            "definer": str(row["DEFINER"] or "")
            .replace("`", "")
            .replace("'", "")
            .casefold(),
            "character_set_client": _normalize_optional_identifier(
                row["CHARACTER_SET_CLIENT"]
            ),
            "collation_connection": _normalize_optional_identifier(
                row["COLLATION_CONNECTION"]
            ),
            "database_collation": _normalize_optional_identifier(
                row["DATABASE_COLLATION"]
            ),
        }
    return signatures


def _job_lease_trigger_signatures(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    return _trigger_signatures(connection, JOB_LEASE_TABLE)


def _claim_token_registry_trigger_signatures(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    return _trigger_signatures(connection, CLAIM_TOKEN_REGISTRY_TABLE)


def _expected_job_lease_triggers(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    context = _job_lease_trigger_context(connection)
    statements = {
        "trg_v4_job_lease_bi": ("INSERT", _JOB_LEASE_INSERT_TRIGGER_DDL),
        "trg_v4_job_lease_bu": ("UPDATE", _JOB_LEASE_UPDATE_TRIGGER_DDL),
    }
    return {
        name: {
            "trigger_schema": context["schema"],
            "event": event,
            "object_schema": context["schema"],
            "object_table": JOB_LEASE_TABLE,
            "action_order": 1,
            "action_condition": None,
            "action_statement": _normalize_trigger_body(
                _trigger_body_from_ddl(
                    _mysql_regexp_compatible_statement(connection, statement)
                )
            ),
            "orientation": "ROW",
            "timing": "BEFORE",
            "sql_mode": context["sql_mode"],
            "definer": context["definer"],
            "character_set_client": context["character_set_client"],
            "collation_connection": context["collation_connection"],
            "database_collation": context["database_collation"],
        }
        for name, (event, statement) in statements.items()
    }


def _expected_claim_token_registry_triggers(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    context = _job_lease_trigger_context(connection)
    statements = {
        "trg_v4_job_claim_token_bi": (
            "INSERT",
            _CLAIM_TOKEN_REGISTRY_INSERT_TRIGGER_DDL,
        ),
        "trg_v4_job_claim_token_bu": (
            "UPDATE",
            _CLAIM_TOKEN_REGISTRY_UPDATE_TRIGGER_DDL,
        ),
        "trg_v4_job_claim_token_bd": (
            "DELETE",
            _CLAIM_TOKEN_REGISTRY_DELETE_TRIGGER_DDL,
        ),
    }
    return {
        name: {
            "trigger_schema": context["schema"],
            "event": event,
            "object_schema": context["schema"],
            "object_table": CLAIM_TOKEN_REGISTRY_TABLE,
            "action_order": 1,
            "action_condition": None,
            "action_statement": _normalize_trigger_body(
                _trigger_body_from_ddl(
                    _mysql_regexp_compatible_statement(connection, statement)
                )
            ),
            "orientation": "ROW",
            "timing": "BEFORE",
            "sql_mode": context["sql_mode"],
            "definer": context["definer"],
            "character_set_client": context["character_set_client"],
            "collation_connection": context["collation_connection"],
            "database_collation": context["database_collation"],
        }
        for name, (event, statement) in statements.items()
    }


def _control_guard_trigger_signatures(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    signatures: dict[str, dict[str, Any]] = {}
    for table_name in CONTROL_GUARD_TABLES:
        table_signatures = _trigger_signatures(connection, table_name)
        overlap = set(signatures) & set(table_signatures)
        if overlap:
            raise RuntimeError(
                "duplicate V4 control guard trigger names: "
                f"{sorted(overlap)}"
            )
        signatures.update(table_signatures)
    return signatures


def _expected_control_guard_triggers(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    context = _job_lease_trigger_context(connection)
    return {
        name: {
            "trigger_schema": context["schema"],
            "event": event,
            "object_schema": context["schema"],
            "object_table": table_name,
            "action_order": 1,
            "action_condition": None,
            "action_statement": _normalize_trigger_body(
                _trigger_body_from_ddl(
                    _mysql_regexp_compatible_statement(connection, statement)
                )
            ),
            "orientation": "ROW",
            "timing": "BEFORE",
            "sql_mode": context["sql_mode"],
            "definer": context["definer"],
            "character_set_client": context["character_set_client"],
            "collation_connection": context["collation_connection"],
            "database_collation": context["database_collation"],
        }
        for name, event, table_name, statement in CONTROL_GUARD_TRIGGER_SPECS
    }


def _pit_factor_guard_trigger_signatures(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    signatures: dict[str, dict[str, Any]] = {}
    for table_name in PIT_FACTOR_REGISTRY_TABLES:
        table_signatures = _trigger_signatures(connection, table_name)
        overlap = set(signatures) & set(table_signatures)
        if overlap:
            raise RuntimeError(
                "duplicate V4 PIT factor guard trigger names: "
                f"{sorted(overlap)}"
            )
        signatures.update(table_signatures)
    return signatures


def _expected_pit_factor_guard_triggers(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    context = _job_lease_trigger_context(connection)
    return {
        name: {
            "trigger_schema": context["schema"],
            "event": event,
            "object_schema": context["schema"],
            "object_table": table_name,
            "action_order": 1,
            "action_condition": None,
            "action_statement": _normalize_trigger_body(
                _trigger_body_from_ddl(
                    _mysql_regexp_compatible_statement(connection, statement)
                )
            ),
            "orientation": "ROW",
            "timing": "BEFORE",
            "sql_mode": context["sql_mode"],
            "definer": context["definer"],
            "character_set_client": context["character_set_client"],
            "collation_connection": context["collation_connection"],
            "database_collation": context["database_collation"],
        }
        for name, event, table_name, statement in (
            PIT_FACTOR_GUARD_TRIGGER_SPECS
        )
    }


def _validate_job_lease_preflight_empty(connection: Connection) -> None:
    count = int(
        connection.execute(
            text("SELECT COUNT(*) FROM st_job_run_v4")
        ).scalar_one()
    )
    if count:
        raise RuntimeError(
            "V4 job lease migration requires an empty st_job_run_v4; "
            "existing rows were not modified"
        )


def _validate_job_lease_existing_prefix_contract(
    connection: Connection,
) -> None:
    """Reject any existing 002 drift before creating another object."""

    for name, expected in JOB_LEASE_COLUMN_CONTRACT.items():
        actual = _job_lease_column_signature(connection, name)
        if actual is not None and actual != expected:
            raise RuntimeError(
                "V4 job lease column drift blocks migration prefix recovery "
                f"for {name}: expected={expected} actual={actual}"
            )
    for name, expected in JOB_LEASE_INDEX_CONTRACT.items():
        actual = _job_lease_index_signature(connection, name)
        if actual is not None and actual != expected:
            raise RuntimeError(
                "V4 job lease index drift blocks migration prefix recovery "
                f"for {name}: expected={expected} actual={actual}"
            )
    expected_triggers = _expected_job_lease_triggers(connection)
    actual_triggers = _job_lease_trigger_signatures(connection)
    unexpected = set(actual_triggers) - set(expected_triggers)
    mismatches = {
        name: (expected_triggers[name], actual)
        for name, actual in actual_triggers.items()
        if name in expected_triggers and actual != expected_triggers[name]
    }
    if unexpected or mismatches:
        raise RuntimeError(
            "V4 job lease trigger drift blocks migration prefix recovery: "
            f"unexpected={sorted(unexpected)} mismatches={mismatches}"
        )


def _validate_job_lease_row_invariants(connection: Connection) -> None:
    row = _execute_mysql_regexp_compatible_statement(
        connection,
            "SELECT "
            "SUM(attempt_count < 0 OR max_attempts < 1 "
            "    OR attempt_count > max_attempts) AS invalid_attempts, "
            "SUM(status NOT IN "
            "    ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')) "
            "    AS invalid_statuses, "
            "SUM(status = 'RUNNING' AND "
            "    (TRIM(lease_owner) = '' OR lease_token IS NULL "
            "     OR BINARY lease_token NOT REGEXP '^[0-9a-f]{64}$' "
            "     OR lease_until IS NULL)) AS invalid_running_leases, "
            "SUM(status <> 'RUNNING' AND "
            "    (lease_owner <> '' OR lease_token IS NOT NULL "
            "     OR lease_until IS NOT NULL)) AS invalid_released_leases, "
            "SUM(status = 'PENDING' AND ("
            "    run_uid <> '' OR completed_at IS NOT NULL OR "
            "    (attempt_count = 0 AND "
            "        (run_uid <> '' "
            "         OR NOT (next_attempt_at <=> scheduled_for) "
            "         OR error_code IS NOT NULL OR error_message IS NOT NULL "
            "         OR started_at IS NOT NULL "
            "         OR NOT (updated_at <=> created_at))) OR "
            "    (attempt_count > 0 AND "
            "        (attempt_count >= max_attempts "
            "         OR next_attempt_at IS NULL OR error_code IS NULL "
            "         OR TRIM(error_code) = '' OR started_at IS NULL "
            "         OR next_attempt_at <= updated_at)))) "
            "    AS invalid_pending_shape, "
            "SUM(status = 'RUNNING' AND "
            "    (attempt_count < 1 OR run_uid <> '' "
            "     OR next_attempt_at IS NOT NULL "
            "     OR error_code IS NOT NULL OR error_message IS NOT NULL "
            "     OR started_at IS NULL OR completed_at IS NOT NULL "
            "     OR lease_until <= updated_at "
            "     OR lease_until > DATE_ADD("
            "        updated_at, INTERVAL 900 SECOND) "
            "     OR lease_until > DATE_ADD("
            "        UTC_TIMESTAMP(6), INTERVAL 900 SECOND))) "
            "    AS invalid_running_shape, "
            "SUM(status = 'SUCCEEDED' AND "
            "    (attempt_count < 1 OR run_uid = '' "
            "     OR next_attempt_at IS NOT NULL "
            "     OR error_code IS NOT NULL "
            "     OR error_message IS NOT NULL OR started_at IS NULL "
            "     OR completed_at IS NULL "
            "     OR NOT (completed_at <=> updated_at))) "
            "    AS invalid_success_shape, "
            "SUM(status = 'FAILED' AND "
            "    (run_uid <> '' OR next_attempt_at IS NOT NULL "
            "     OR error_code IS NULL "
            "     OR TRIM(error_code) = '' OR completed_at IS NULL "
            "     OR NOT (completed_at <=> updated_at))) "
            "    AS invalid_failure_shape, "
            "SUM(status = 'CANCELLED' AND "
            "    (run_uid <> '' OR next_attempt_at IS NOT NULL "
            "     OR completed_at IS NULL "
            "     OR NOT (completed_at <=> updated_at))) "
            "    AS invalid_cancel_shape, "
            "SUM(updated_at < created_at "
            "    OR (started_at IS NOT NULL "
            "        AND (started_at < created_at OR started_at > updated_at)) "
            "    OR (completed_at IS NOT NULL "
            "        AND (completed_at < created_at "
            "             OR completed_at > updated_at))) "
            "    AS invalid_chronology, "
            "SUM(created_at > DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND) "
            "    OR updated_at > DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND)) "
            "    AS invalid_future_timestamp, "
            "SUM(error_message IS NOT NULL AND "
            "    (error_code IS NULL OR TRIM(error_code) = '')) "
            "    AS invalid_error_shape, "
            "SUM(job_id = '' "
            "    OR BINARY job_id REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR BINARY idempotency_key NOT REGEXP '^[0-9a-f]{64}$' "
            "    OR job_type = '' "
            "    OR BINARY job_type REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR BINARY input_context_id REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR (input_hash <> '' AND "
            "        BINARY input_hash NOT REGEXP '^[0-9a-f]{64}$')) "
            "    AS invalid_identity_text, "
            "SUM(BINARY run_uid REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR BINARY status REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR BINARY lease_owner REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR (lease_token IS NOT NULL AND "
            "        BINARY lease_token NOT REGEXP '^[0-9a-f]{64}$') "
            "    OR (error_code IS NOT NULL AND "
            "        (error_code = '' OR BINARY error_code REGEXP "
            "            '(^[[:space:]])|([[:space:]]$)')) "
            "    OR (error_message IS NOT NULL AND "
            "        BINARY error_message REGEXP "
            "            '(^[[:space:]])|([[:space:]]$)')) "
            "    AS invalid_mutable_text "
            "FROM st_job_run_v4"
    ).mappings().first()
    if row is None:
        raise RuntimeError("V4 job lease row audit returned no result")
    violations = {
        name: int(row[name] or 0)
        for name in (
            "invalid_attempts",
            "invalid_statuses",
            "invalid_running_leases",
            "invalid_released_leases",
            "invalid_pending_shape",
            "invalid_running_shape",
            "invalid_success_shape",
            "invalid_failure_shape",
            "invalid_cancel_shape",
            "invalid_chronology",
            "invalid_future_timestamp",
            "invalid_error_shape",
            "invalid_identity_text",
            "invalid_mutable_text",
        )
    }
    if any(violations.values()):
        raise RuntimeError(
            f"V4 job lease row invariant drift detected: {violations}"
        )


def _validate_job_lease_final_contract(connection: Connection) -> None:
    job_create = next(
        statement
        for statement in tuple(MIGRATIONS[0]["statements"])
        if "CREATE TABLE IF NOT EXISTS st_job_run_v4" in statement
    )
    _validate_schema_on_connection(connection, (job_create,))
    for name, expected in JOB_LEASE_COLUMN_CONTRACT.items():
        actual = _job_lease_column_signature(connection, name)
        if actual != expected:
            raise RuntimeError(
                "V4 job lease column drift detected for "
                f"{name}: expected={expected} actual={actual}"
            )
    for name, expected in JOB_LEASE_INDEX_CONTRACT.items():
        actual = _job_lease_index_signature(connection, name)
        if actual != expected:
            raise RuntimeError(
                "V4 job lease index drift detected for "
                f"{name}: expected={expected} actual={actual}"
            )
    expected_triggers = _expected_job_lease_triggers(connection)
    actual_triggers = _job_lease_trigger_signatures(connection)
    if actual_triggers != expected_triggers:
        raise RuntimeError(
            "V4 job lease trigger drift detected: "
            f"expected={expected_triggers} actual={actual_triggers}"
        )
    _validate_job_lease_row_invariants(connection)


def _claim_token_registry_table_exists(connection: Connection) -> bool:
    return bool(
        int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = :table_name"
                ),
                {"table_name": CLAIM_TOKEN_REGISTRY_TABLE},
            ).scalar_one()
        )
    )


def _validate_claim_token_registry_preflight(connection: Connection) -> None:
    prior_claims = int(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM st_job_run_v4 "
                "WHERE attempt_count <> 0 OR lease_token IS NOT NULL"
            )
        ).scalar_one()
    )
    if prior_claims:
        raise RuntimeError(
            "V4 claim token registry cannot prove pre-migration token "
            "history; previously claimed jobs were not modified"
        )
    if not _claim_token_registry_table_exists(connection):
        return
    registry_rows = int(
        connection.execute(
            text("SELECT COUNT(*) FROM st_job_claim_token_v4")
        ).scalar_one()
    )
    if registry_rows:
        raise RuntimeError(
            "incomplete V4 claim token registry migration must remain empty"
        )


def _validate_claim_token_registry_existing_prefix_contract(
    connection: Connection,
) -> None:
    """Reject 003 drift before adding another implicit-commit object."""

    table_exists = _claim_token_registry_table_exists(connection)
    if table_exists:
        _validate_schema_on_connection(
            connection,
            (_CLAIM_TOKEN_REGISTRY_TABLE_DDL,),
        )
        for name, expected in CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT.items():
            actual = _claim_token_registry_column_signature(connection, name)
            if actual != expected:
                raise RuntimeError(
                    "V4 claim token registry column drift blocks recovery "
                    f"for {name}: expected={expected} actual={actual}"
                )
        for name, expected in CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT.items():
            actual = _claim_token_registry_index_signature(connection, name)
            if actual != expected:
                raise RuntimeError(
                    "V4 claim token registry index drift blocks recovery "
                    f"for {name}: expected={expected} actual={actual}"
                )
    expected_triggers = _expected_claim_token_registry_triggers(connection)
    actual_triggers = _claim_token_registry_trigger_signatures(connection)
    unexpected = set(actual_triggers) - set(expected_triggers)
    mismatches = {
        name: (expected_triggers[name], actual)
        for name, actual in actual_triggers.items()
        if name in expected_triggers and actual != expected_triggers[name]
    }
    if unexpected or mismatches or (actual_triggers and not table_exists):
        raise RuntimeError(
            "V4 claim token registry trigger drift blocks recovery: "
            f"unexpected={sorted(unexpected)} mismatches={mismatches}"
        )


def _validate_claim_token_registry_rows(connection: Connection) -> None:
    row = _execute_mysql_regexp_compatible_statement(
        connection,
            "SELECT "
            "SUM(BINARY r.lease_token NOT REGEXP '^[0-9a-f]{64}$' "
            "    OR r.job_id = '' "
            "    OR BINARY r.job_id REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR r.attempt_count < 1 "
            "    OR r.lease_owner = '' "
            "    OR BINARY r.lease_owner REGEXP "
            "        '(^[[:space:]])|([[:space:]]$)' "
            "    OR r.claimed_at > "
            "        DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND) "
            "    OR r.lease_until <= r.claimed_at "
            "    OR r.lease_until > "
            "        DATE_ADD(r.claimed_at, INTERVAL 900 SECOND) "
            "    OR j.job_id IS NULL "
            "    OR r.attempt_count > j.attempt_count "
            "    OR r.claimed_at < j.created_at "
            "    OR r.claimed_at > j.updated_at) AS invalid_registry_rows, "
            "(SELECT COUNT(*) FROM st_job_run_v4 live "
            " WHERE live.lease_token IS NOT NULL "
            " AND NOT EXISTS ("
            "     SELECT 1 FROM st_job_claim_token_v4 token "
            "     WHERE token.lease_token = live.lease_token "
            "       AND token.job_id = live.job_id "
            "       AND token.attempt_count = live.attempt_count "
            "       AND token.lease_owner = live.lease_owner "
            "       AND token.claimed_at <= live.updated_at "
            "       AND token.lease_until <= live.lease_until"
            " )) AS invalid_live_leases "
            "FROM st_job_claim_token_v4 r "
            "LEFT JOIN st_job_run_v4 j ON j.job_id = r.job_id"
    ).mappings().first()
    if row is None:
        raise RuntimeError("V4 claim token registry audit returned no result")
    violations = {
        "invalid_registry_rows": int(row["invalid_registry_rows"] or 0),
        "invalid_live_leases": int(row["invalid_live_leases"] or 0),
    }
    if any(violations.values()):
        raise RuntimeError(
            "V4 claim token registry row drift detected: "
            f"{violations}"
        )


def _validate_claim_token_registry_final_contract(
    connection: Connection,
) -> None:
    _validate_schema_on_connection(
        connection,
        (_CLAIM_TOKEN_REGISTRY_TABLE_DDL,),
    )
    expected_schema = _expected_schema((_CLAIM_TOKEN_REGISTRY_TABLE_DDL,))[
        CLAIM_TOKEN_REGISTRY_TABLE
    ]
    if expected_schema["constraints"] != (
        CLAIM_TOKEN_REGISTRY_CONSTRAINT_CONTRACT
    ):
        raise RuntimeError("V4 claim token registry FK contract is not frozen")
    for name, expected in CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT.items():
        actual = _claim_token_registry_column_signature(connection, name)
        if actual != expected:
            raise RuntimeError(
                "V4 claim token registry column drift detected for "
                f"{name}: expected={expected} actual={actual}"
            )
    for name, expected in CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT.items():
        actual = _claim_token_registry_index_signature(connection, name)
        if actual != expected:
            raise RuntimeError(
                "V4 claim token registry index drift detected for "
                f"{name}: expected={expected} actual={actual}"
            )
    expected_triggers = _expected_claim_token_registry_triggers(connection)
    actual_triggers = _claim_token_registry_trigger_signatures(connection)
    if actual_triggers != expected_triggers:
        raise RuntimeError(
            "V4 claim token registry trigger drift detected: "
            f"expected={expected_triggers} actual={actual_triggers}"
        )
    _validate_claim_token_registry_rows(connection)


def _control_guard_base_statements() -> tuple[str, ...]:
    statements = tuple(
        statement
        for statement in tuple(MIGRATIONS[0]["statements"])
        if (
            (match := _CREATE_TABLE_RE.search(statement)) is not None
            and match.group(1) in CONTROL_GUARD_TABLES
        )
    )
    if len(statements) != len(CONTROL_GUARD_TABLES):
        raise RuntimeError("V4 control guard base table inventory drifted")
    return statements


def _validate_control_guard_base_schema(connection: Connection) -> None:
    _validate_schema_on_connection(
        connection,
        _control_guard_base_statements(),
    )


def _validate_control_guard_rows(connection: Connection) -> None:
    invalid_contexts = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_decision_context_v4 WHERE "
                "context_id = '' "
                "OR BINARY context_id REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR knowledge_cutoff_at > decision_at "
                "OR is_realtime NOT IN (0, 1) "
                "OR fallback_used NOT IN (0, 1) "
                "OR freshness_status NOT IN ('PASS','WARN','FAIL') "
                "OR BINARY data_snapshot_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR BINARY context_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR JSON_VALID(data_manifest_json) <> 1 "
                "OR JSON_VALID(source_manifest_json) <> 1 "
                "OR JSON_VALID(quality_json) <> 1 "
                "OR JSON_VALID(factor_spec_versions_json) <> 1 "
                "OR JSON_VALID(forecast_contract_ids_json) <> 1 "
                "OR JSON_VALID(model_versions_json) <> 1 "
                "OR JSON_VALID(model_artifact_hashes_json) <> 1 "
                "OR JSON_VALID(model_training_cutoffs_json) <> 1 "
                "OR JSON_VALID(model_available_at_json) <> 1 "
                "OR JSON_VALID(calibration_versions_json) <> 1 "
                "OR JSON_VALID(calibration_artifact_hashes_json) <> 1 "
                "OR JSON_VALID(calibration_training_cutoffs_json) <> 1 "
                "OR JSON_VALID(calibration_available_at_json) <> 1 "
                "OR JSON_VALID(capability_statuses_json) <> 1 "
                "OR JSON_VALID(context_json) <> 1"
        ).scalar_one()
    )
    invalid_watermarks = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_source_watermark_v4 w "
                "LEFT JOIN st_decision_context_v4 c "
                "ON c.context_id = w.context_id WHERE "
                "w.context_id = '' OR w.source_key = '' "
                "OR BINARY w.context_id REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY w.source_key REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR c.context_id IS NULL "
                "OR w.knowledge_time > c.knowledge_cutoff_at "
                "OR NOT (w.created_at <=> c.created_at) "
                "OR w.record_count < 0 "
                "OR (w.coverage IS NOT NULL "
                "AND (w.coverage < 0 OR w.coverage > 1)) "
                "OR (w.lag_seconds IS NOT NULL AND w.lag_seconds < 0) "
                "OR w.quality_status NOT IN ('PASS','WARN','FAIL') "
                "OR (w.content_hash <> '' AND BINARY w.content_hash "
                "NOT REGEXP '^[0-9a-f]{64}$') "
                "OR JSON_VALID(w.details_json) <> 1"
        ).scalar_one()
    )
    invalid_runs = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_decision_run_v4 r "
                "LEFT JOIN st_decision_context_v4 c "
                "ON c.context_id = r.context_id WHERE "
                "r.run_uid = '' "
                "OR BINARY r.run_uid REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY r.run_idempotency_key NOT REGEXP "
                "'^[0-9a-f]{64}$' "
                "OR r.status NOT IN "
                "('CREATED','RUNNING','VALIDATING','COMMITTED',"
                "'FAILED','CANCELLED') "
                "OR c.context_id IS NULL "
                "OR NOT (BINARY r.model_set_version <=> "
                "BINARY c.model_set_version) "
                "OR NOT (BINARY r.config_version <=> BINARY c.config_version) "
                "OR NOT (BINARY r.code_commit_sha <=> BINARY c.code_commit_sha) "
                "OR r.updated_at < r.created_at "
                "OR (r.started_at IS NOT NULL AND "
                "(r.started_at < r.created_at OR r.started_at > r.updated_at)) "
                "OR (r.validated_at IS NOT NULL AND "
                "(r.started_at IS NULL OR r.validated_at < r.started_at "
                "OR r.validated_at > r.updated_at)) "
                "OR (r.committed_at IS NOT NULL AND "
                "(r.validated_at IS NULL OR r.committed_at < r.validated_at "
                "OR r.committed_at > r.updated_at)) "
                "OR (r.finished_at IS NOT NULL AND "
                "(r.finished_at < r.created_at OR r.finished_at > r.updated_at)) "
                "OR (r.parent_run_uid <> '' AND NOT EXISTS ("
                "SELECT 1 FROM st_decision_run_v4 p "
                "WHERE p.run_uid = r.parent_run_uid "
                "AND BINARY p.channel = BINARY r.channel "
                "AND BINARY p.account_id = BINARY r.account_id)) "
                "OR (r.status = 'CREATED' AND "
                "(r.result_hash IS NOT NULL OR r.error_code IS NOT NULL "
                "OR r.error_message IS NOT NULL OR r.started_at IS NOT NULL "
                "OR r.validated_at IS NOT NULL OR r.committed_at IS NOT NULL "
                "OR r.finished_at IS NOT NULL "
                "OR NOT (r.updated_at <=> r.created_at))) "
                "OR (r.status = 'RUNNING' AND "
                "(r.started_at IS NULL OR r.validated_at IS NOT NULL "
                "OR r.result_hash IS NOT NULL OR r.error_code IS NOT NULL "
                "OR r.error_message IS NOT NULL OR r.committed_at IS NOT NULL "
                "OR r.finished_at IS NOT NULL)) "
                "OR (r.status = 'VALIDATING' AND "
                "(r.started_at IS NULL OR r.validated_at IS NULL "
                "OR r.result_hash IS NOT NULL OR r.error_code IS NOT NULL "
                "OR r.error_message IS NOT NULL OR r.committed_at IS NOT NULL "
                "OR r.finished_at IS NOT NULL)) "
                "OR (r.status = 'COMMITTED' AND "
                "(r.started_at IS NULL OR r.validated_at IS NULL "
                "OR r.result_hash IS NULL OR BINARY r.result_hash "
                "NOT REGEXP '^[0-9a-f]{64}$' "
                "OR r.error_code IS NOT NULL OR r.error_message IS NOT NULL "
                "OR r.committed_at IS NULL OR r.finished_at IS NULL "
                "OR NOT (r.committed_at <=> r.updated_at) "
                "OR NOT (r.finished_at <=> r.updated_at))) "
                "OR (r.status IN ('FAILED','CANCELLED') AND "
                "(r.result_hash IS NOT NULL OR r.error_code IS NULL "
                "OR r.error_code = '' OR r.error_message IS NULL "
                "OR r.committed_at IS NOT NULL OR r.finished_at IS NULL "
                "OR NOT (r.finished_at <=> r.updated_at)))"
        ).scalar_one()
    )
    invalid_heads = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_decision_channel_head_v4 h "
                "LEFT JOIN st_decision_run_v4 r ON r.run_uid = h.run_uid "
                "WHERE h.channel = '' "
                "OR BINARY h.channel REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR h.head_version < 1 "
                "OR NOT (h.updated_at <=> h.published_at) "
                "OR r.run_uid IS NULL OR r.status <> 'COMMITTED' "
                "OR NOT (BINARY r.context_id <=> BINARY h.context_id) "
                "OR NOT (BINARY r.channel <=> BINARY h.channel) "
                "OR NOT (BINARY r.account_id <=> BINARY h.account_id) "
                "OR r.committed_at > h.published_at"
        ).scalar_one()
    )
    invalid_controls = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_runtime_control_v4 c WHERE "
                "c.control_key = '' "
                "OR BINARY c.control_key REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR JSON_VALID(c.control_value_json) <> 1 "
                "OR c.version < 1 "
                "OR c.updated_by = '' OR c.reason = '' "
                "OR c.updated_at < c.created_at "
                "OR (SELECT COUNT(*) "
                "FROM st_runtime_control_transition_v4 t "
                "WHERE t.control_key = c.control_key) <> c.version "
                "OR NOT EXISTS (SELECT 1 "
                "FROM st_runtime_control_transition_v4 genesis "
                "WHERE genesis.control_key = c.control_key "
                "AND genesis.next_version = 1 "
                "AND genesis.previous_value_json IS NULL "
                "AND genesis.changed_at = c.created_at) "
                "OR NOT EXISTS (SELECT 1 "
                "FROM st_runtime_control_transition_v4 latest "
                "WHERE latest.control_key = c.control_key "
                "AND latest.next_version = c.version "
                "AND BINARY latest.next_value_json = "
                "BINARY c.control_value_json "
                "AND BINARY latest.changed_by = BINARY c.updated_by "
                "AND BINARY latest.reason = BINARY c.reason "
                "AND latest.changed_at = c.updated_at)"
        ).scalar_one()
    )
    invalid_transitions = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) "
                "FROM st_runtime_control_transition_v4 t "
                "LEFT JOIN st_runtime_control_v4 c "
                "ON c.control_key = t.control_key WHERE "
                "BINARY t.transition_id NOT REGEXP '^[0-9a-f]{64}$' "
                "OR BINARY t.event_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR NOT (BINARY t.transition_id <=> BINARY t.event_hash) "
                "OR t.control_key = '' OR t.next_version < 1 "
                "OR JSON_VALID(t.next_value_json) <> 1 "
                "OR (t.previous_value_json IS NOT NULL "
                "AND JSON_VALID(t.previous_value_json) <> 1) "
                "OR t.changed_by = '' OR t.reason = '' "
                "OR (t.next_version = 1 "
                "AND t.previous_value_json IS NOT NULL) "
                "OR (t.next_version > 1 "
                "AND t.previous_value_json IS NULL) "
                "OR (t.next_version > 1 AND BINARY t.next_value_json = "
                "BINARY t.previous_value_json) "
                "OR c.control_key IS NULL OR t.next_version > c.version "
                "OR (t.next_version > 1 AND NOT EXISTS ("
                "SELECT 1 FROM st_runtime_control_transition_v4 p "
                "WHERE p.control_key = t.control_key "
                "AND p.next_version = t.next_version - 1 "
                "AND BINARY p.next_value_json = "
                "BINARY t.previous_value_json "
                "AND p.changed_at <= t.changed_at))"
        ).scalar_one()
    )
    violations = {
        "invalid_contexts": invalid_contexts,
        "invalid_watermarks": invalid_watermarks,
        "invalid_runs": invalid_runs,
        "invalid_heads": invalid_heads,
        "invalid_controls": invalid_controls,
        "invalid_transitions": invalid_transitions,
    }
    if any(violations.values()):
        raise RuntimeError(
            f"V4 non-job control-plane row drift detected: {violations}"
        )


def _validate_control_guard_existing_prefix_contract(
    connection: Connection,
) -> None:
    expected = _expected_control_guard_triggers(connection)
    actual = _control_guard_trigger_signatures(connection)
    unexpected = set(actual) - set(expected)
    mismatches = {
        name: (expected[name], signature)
        for name, signature in actual.items()
        if name in expected and signature != expected[name]
    }
    if unexpected or mismatches:
        raise RuntimeError(
            "V4 control guard trigger drift blocks recovery: "
            f"unexpected={sorted(unexpected)} mismatches={mismatches}"
        )


def _validate_control_guard_preflight(connection: Connection) -> None:
    _validate_control_guard_base_schema(connection)
    _validate_control_guard_rows(connection)


def _validate_control_guard_final_contract(connection: Connection) -> None:
    _validate_control_guard_base_schema(connection)
    expected = _expected_control_guard_triggers(connection)
    actual = _control_guard_trigger_signatures(connection)
    if actual != expected:
        raise RuntimeError(
            "V4 control guard trigger drift detected: "
            f"expected={expected} actual={actual}"
        )
    _validate_control_guard_rows(connection)


def _pit_factor_registry_table_exists(
    connection: Connection,
    table_name: str,
) -> bool:
    if table_name not in PIT_FACTOR_REGISTRY_TABLES:
        raise RuntimeError(f"untrusted V4 PIT factor table: {table_name}")
    return bool(
        int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = :table_name"
                ),
                {"table_name": table_name},
            ).scalar_one()
        )
    )


def _validate_pit_factor_registry_existing_prefix_contract(
    connection: Connection,
) -> None:
    """Reject a drifted 005 object before creating any later table."""

    for table_name, statement in zip(
        PIT_FACTOR_REGISTRY_TABLES,
        PIT_FACTOR_REGISTRY_TABLE_DDLS,
        strict=True,
    ):
        if _pit_factor_registry_table_exists(connection, table_name):
            _validate_schema_on_connection(connection, (statement,))


def _validate_pit_factor_registry_schema(connection: Connection) -> None:
    _validate_schema_on_connection(
        connection,
        PIT_FACTOR_REGISTRY_TABLE_DDLS,
    )


def _validate_pit_factor_registry_rows(connection: Connection) -> None:
    invalid_certifications = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) "
                "FROM st_data_source_certification_v4 c WHERE "
                "c.source_key = '' OR c.certification_version = '' "
                "OR c.source_table = '' OR c.certified_by = '' "
                "OR BINARY c.source_key REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY c.certification_version REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY c.source_table REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY c.event_time_column REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY c.certified_by REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR c.replay_eligibility NOT IN "
                "('PIT_CERTIFIED','FORWARD_ONLY','DISPLAY_ONLY',"
                "'REPLAY_INELIGIBLE') "
                "OR c.certification_status NOT IN "
                "('PENDING','PASSED','FAILED','REVOKED') "
                "OR c.availability_status NOT IN "
                "('ACTIVE','DEGRADED','BLOCKED') "
                "OR c.research_status NOT IN "
                "('BACKTEST_READY','FORWARD_ONLY','DISPLAY_ONLY') "
                "OR c.quality_status NOT IN ('PASS','WARN','FAIL') "
                "OR (c.replay_eligibility = 'PIT_CERTIFIED' "
                "AND (c.certification_status <> 'PASSED' "
                "OR c.availability_status <> 'ACTIVE' "
                "OR c.research_status <> 'BACKTEST_READY' "
                "OR c.quality_status <> 'PASS')) "
                "OR (c.replay_eligibility <> 'PIT_CERTIFIED' "
                "AND c.research_status = 'BACKTEST_READY') "
                "OR (c.certification_status <> 'PASSED' "
                "AND (c.availability_status = 'ACTIVE' "
                "OR c.quality_status = 'PASS')) "
                "OR (c.replay_eligibility = 'PIT_CERTIFIED' "
                "AND c.event_time_column = '') "
                "OR JSON_VALID(c.knowledge_time_columns_json) <> 1 "
                "OR JSON_TYPE(c.knowledge_time_columns_json) <> 'ARRAY' "
                "OR (c.replay_eligibility = 'PIT_CERTIFIED' "
                "AND JSON_LENGTH(c.knowledge_time_columns_json) < 1) "
                "OR JSON_VALID(c.contract_json) <> 1 "
                "OR JSON_TYPE(c.contract_json) <> 'OBJECT' "
                "OR JSON_EXTRACT(c.contract_json, '$.revision_policy') "
                "IS NULL "
                "OR JSON_TYPE(JSON_EXTRACT(c.contract_json, "
                "'$.revision_policy')) <> 'STRING' "
                "OR JSON_UNQUOTE(JSON_EXTRACT(c.contract_json, "
                "'$.revision_policy')) = '' "
                "OR JSON_UNQUOTE(JSON_EXTRACT(c.contract_json, "
                "'$.revision_policy')) <> UPPER(TRIM(JSON_UNQUOTE("
                "JSON_EXTRACT(c.contract_json, '$.revision_policy')))) "
                "OR (c.replay_eligibility = 'PIT_CERTIFIED' AND "
                "JSON_UNQUOTE(JSON_EXTRACT(c.contract_json, "
                "'$.revision_policy')) NOT IN "
                "('APPEND_ONLY_REVISION_CHAIN',"
                "'BITEMPORAL_REVISION_CHAIN','IMMUTABLE_EVENT_LOG')) "
                "OR BINARY c.evidence_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR (c.valid_to IS NOT NULL AND c.valid_to <= c.valid_from) "
                "OR c.certified_at > c.created_at"
        ).scalar_one()
    )
    invalid_definitions = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) FROM st_factor_definition_v4 f WHERE "
                "f.factor_key = '' OR f.factor_version = '' "
                "OR f.feature_set_version = '' "
                "OR BINARY f.factor_key REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY f.factor_version REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY f.feature_set_version REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR f.factor_role NOT IN "
                "('GATE','STATE','ALPHA','RISK','COST','PORTFOLIO',"
                "'EXPLANATION') "
                "OR f.scope_type NOT IN "
                "('MARKET','SECTOR','INSTRUMENT','PORTFOLIO') "
                "OR f.availability_status NOT IN "
                "('ACTIVE','DEGRADED','BLOCKED') "
                "OR f.research_status NOT IN "
                "('BACKTEST_READY','FORWARD_ONLY','DISPLAY_ONLY') "
                "OR f.quality_status NOT IN ('PASS','WARN','FAIL') "
                "OR f.missing_policy NOT IN "
                "('BLOCK','PROPAGATE_NULL','DISPLAY_ONLY') "
                "OR f.pit_eligible NOT IN (0, 1) "
                "OR (f.pit_eligible = 1 AND f.research_status "
                "<> 'BACKTEST_READY') "
                "OR (f.availability_status = 'ACTIVE' "
                "AND f.quality_status = 'FAIL') "
                "OR JSON_VALID(f.required_source_keys_json) <> 1 "
                "OR JSON_TYPE(f.required_source_keys_json) <> 'ARRAY' "
                "OR JSON_LENGTH(f.required_source_keys_json) < 1 "
                "OR JSON_VALID(f.formula_json) <> 1 "
                "OR JSON_TYPE(f.formula_json) <> 'OBJECT' "
                "OR JSON_VALID(f.output_schema_json) <> 1 "
                "OR JSON_TYPE(f.output_schema_json) <> 'OBJECT' "
                "OR BINARY f.definition_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR f.available_at > f.created_at"
        ).scalar_one()
    )
    invalid_snapshots = int(
        _execute_mysql_regexp_compatible_statement(
            connection,
                "SELECT COUNT(*) "
                "FROM st_entity_feature_snapshot_v4 s "
                "LEFT JOIN st_decision_run_v4 r ON r.run_uid = s.run_uid "
                "LEFT JOIN st_decision_context_v4 c "
                "ON c.context_id = r.context_id WHERE "
                "BINARY s.snapshot_id NOT REGEXP '^[0-9a-f]{64}$' "
                "OR s.run_uid = '' OR s.scope_id = '' "
                "OR s.feature_set_version = '' "
                "OR BINARY s.run_uid REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY s.scope_id REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR BINARY s.feature_set_version REGEXP "
                "'(^[[:space:]])|([[:space:]]$)' "
                "OR s.scope_type NOT IN "
                "('MARKET','SECTOR','INSTRUMENT','PORTFOLIO') "
                "OR s.quality_status NOT IN ('PASS','WARN','FAIL') "
                "OR s.factor_count < 1 "
                "OR JSON_VALID(s.values_json) <> 1 "
                "OR JSON_TYPE(s.values_json) <> 'OBJECT' "
                "OR JSON_VALID(s.quality_json) <> 1 "
                "OR JSON_TYPE(s.quality_json) <> 'OBJECT' "
                "OR JSON_VALID(s.source_certifications_json) <> 1 "
                "OR JSON_TYPE(s.source_certifications_json) <> 'ARRAY' "
                "OR JSON_LENGTH(s.source_certifications_json) < 1 "
                "OR BINARY s.source_manifest_hash "
                "NOT REGEXP '^[0-9a-f]{64}$' "
                "OR BINARY s.feature_hash NOT REGEXP '^[0-9a-f]{64}$' "
                "OR s.computed_at < s.knowledge_cutoff_at "
                "OR s.available_at < s.computed_at "
                "OR s.created_at < s.available_at "
                "OR r.run_uid IS NULL "
                "OR r.status NOT IN ('RUNNING','VALIDATING','COMMITTED') "
                "OR c.context_id IS NULL "
                "OR NOT (c.knowledge_cutoff_at <=> s.knowledge_cutoff_at) "
                "OR NOT (c.data_snapshot_hash <=> s.source_manifest_hash) "
                "OR c.decision_at > s.computed_at"
        ).scalar_one()
    )
    violations = {
        "invalid_certifications": invalid_certifications,
        "invalid_definitions": invalid_definitions,
        "invalid_snapshots": invalid_snapshots,
    }
    if any(violations.values()):
        raise RuntimeError(
            f"V4 PIT factor registry row drift detected: {violations}"
        )


def _validate_pit_factor_registry_final_contract(
    connection: Connection,
) -> None:
    _validate_pit_factor_registry_schema(connection)
    _validate_pit_factor_registry_rows(connection)


def _validate_pit_factor_guard_existing_prefix_contract(
    connection: Connection,
) -> None:
    expected = _expected_pit_factor_guard_triggers(connection)
    actual = _pit_factor_guard_trigger_signatures(connection)
    unexpected = set(actual) - set(expected)
    mismatches = {
        name: (expected[name], signature)
        for name, signature in actual.items()
        if name in expected and signature != expected[name]
    }
    if unexpected or mismatches:
        raise RuntimeError(
            "V4 PIT factor guard trigger drift blocks recovery: "
            f"unexpected={sorted(unexpected)} mismatches={mismatches}"
        )


def _validate_pit_factor_guard_preflight(connection: Connection) -> None:
    _validate_pit_factor_registry_final_contract(connection)


def _validate_pit_factor_guard_final_contract(
    connection: Connection,
) -> None:
    _validate_pit_factor_registry_schema(connection)
    expected = _expected_pit_factor_guard_triggers(connection)
    actual = _pit_factor_guard_trigger_signatures(connection)
    if actual != expected:
        raise RuntimeError(
            "V4 PIT factor guard trigger drift detected: "
            f"expected={expected} actual={actual}"
        )
    _validate_pit_factor_registry_rows(connection)


def _expected_pit_factor_lineage_triggers(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    context = _job_lease_trigger_context(connection)
    return {
        name: {
            "trigger_schema": context["schema"],
            "event": event,
            "object_schema": context["schema"],
            "object_table": table_name,
            "action_order": 2,
            "action_condition": None,
            "action_statement": _normalize_trigger_body(
                _trigger_body_from_ddl(
                    _mysql_regexp_compatible_statement(connection, statement)
                )
            ),
            "orientation": "ROW",
            "timing": "BEFORE",
            "sql_mode": context["sql_mode"],
            "definer": context["definer"],
            "character_set_client": context["character_set_client"],
            "collation_connection": context["collation_connection"],
            "database_collation": context["database_collation"],
        }
        for name, event, table_name, statement in PIT_FACTOR_LINEAGE_TRIGGER_SPECS
    }


def _validate_pit_factor_lineage_preflight(connection: Connection) -> None:
    factor_count = int(
        connection.execute(
            text("SELECT COUNT(*) FROM st_factor_definition_v4")
        ).scalar_one()
    )
    snapshot_count = int(
        connection.execute(
            text("SELECT COUNT(*) FROM st_entity_feature_snapshot_v4")
        ).scalar_one()
    )
    if factor_count or snapshot_count:
        raise RuntimeError(
            "V4 factor lineage migration requires empty factor and snapshot "
            "registries; legacy rows cannot be assigned invented lineage"
        )


def _pit_factor_lineage_column_exists(
    connection: Connection,
    table_name: str,
    column_name: str,
) -> bool:
    allowed = {
        ("st_factor_definition_v4", "max_age_seconds"),
        (
            "st_factor_definition_v4",
            "required_source_certifications_json",
        ),
        ("st_entity_feature_snapshot_v4", "factor_definitions_json"),
    }
    if (table_name, column_name) not in allowed:
        raise RuntimeError("untrusted V4 factor lineage column")
    return bool(
        int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = :table_name "
                    "AND COLUMN_NAME = :column_name"
                ),
                {"table_name": table_name, "column_name": column_name},
            ).scalar_one()
        )
    )


def _apply_pit_factor_lineage_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    if statement_index == 0:
        if not _pit_factor_lineage_column_exists(
            connection, "st_factor_definition_v4", "max_age_seconds"
        ):
            connection.execute(text(_FACTOR_MAX_AGE_COLUMN_DDL))
            connection.commit()
    elif statement_index == 1:
        if not _pit_factor_lineage_column_exists(
            connection,
            "st_factor_definition_v4",
            "required_source_certifications_json",
        ):
            connection.execute(text(_FACTOR_SOURCE_LINEAGE_COLUMN_DDL))
            connection.commit()
    elif statement_index == 2:
        if not _pit_factor_lineage_column_exists(
            connection,
            "st_entity_feature_snapshot_v4",
            "factor_definitions_json",
        ):
            connection.execute(text(_SNAPSHOT_FACTOR_LINEAGE_COLUMN_DDL))
            connection.commit()
    elif statement_index == 3:
        existing = _trigger_signatures(
            connection, "st_factor_definition_v4"
        )
        if "trg_v4_factor_lineage_bi" not in existing:
            connection.execute(text(_FACTOR_LINEAGE_INSERT_GUARD_DDL))
            connection.commit()
    elif statement_index == 4:
        existing = _trigger_signatures(
            connection, "st_entity_feature_snapshot_v4"
        )
        if "trg_v4_feature_snapshot_lineage_bi" not in existing:
            connection.execute(
                text(_SNAPSHOT_FACTOR_LINEAGE_INSERT_GUARD_DDL)
            )
            connection.commit()
    else:
        raise RuntimeError(
            f"unknown V4 factor lineage statement index: {statement_index}"
        )


def _validate_pit_factor_lineage_rows(connection: Connection) -> None:
    invalid_factors = int(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM st_factor_definition_v4 f WHERE "
                "f.max_age_seconds < 1 "
                "OR JSON_VALID(f.required_source_certifications_json) <> 1 "
                "OR JSON_TYPE(f.required_source_certifications_json) <> 'ARRAY' "
                "OR JSON_LENGTH(f.required_source_certifications_json) < 1"
            )
        ).scalar_one()
    )
    invalid_snapshots = int(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM st_entity_feature_snapshot_v4 s WHERE "
                "JSON_LENGTH(s.values_json) <> s.factor_count "
                "OR JSON_VALID(s.factor_definitions_json) <> 1 "
                "OR JSON_TYPE(s.factor_definitions_json) <> 'ARRAY' "
                "OR JSON_LENGTH(s.factor_definitions_json) < 1"
            )
        ).scalar_one()
    )
    if invalid_factors or invalid_snapshots:
        raise RuntimeError(
            "V4 factor lineage row drift detected: "
            f"invalid_factors={invalid_factors} "
            f"invalid_snapshots={invalid_snapshots}"
        )


def _validate_pit_factor_lineage_prefix_contract(
    connection: Connection,
) -> bool:
    max_age = _pit_factor_lineage_column_exists(
        connection, "st_factor_definition_v4", "max_age_seconds"
    )
    source_refs = _pit_factor_lineage_column_exists(
        connection,
        "st_factor_definition_v4",
        "required_source_certifications_json",
    )
    factor_refs = _pit_factor_lineage_column_exists(
        connection,
        "st_entity_feature_snapshot_v4",
        "factor_definitions_json",
    )
    if not any((max_age, source_refs, factor_refs)):
        return False
    factor_ddl = _FACTOR_DEFINITION_TABLE_DDL
    snapshot_ddl = _ENTITY_FEATURE_SNAPSHOT_TABLE_DDL
    if max_age:
        factor_ddl = factor_ddl.replace(
            "    pit_eligible TINYINT(1) NOT NULL DEFAULT 0,\n",
            "    pit_eligible TINYINT(1) NOT NULL DEFAULT 0,\n"
            "    max_age_seconds INT UNSIGNED NOT NULL,\n",
        )
    if source_refs:
        factor_ddl = factor_ddl.replace(
            "    required_source_keys_json LONGTEXT NOT NULL,\n",
            "    required_source_keys_json LONGTEXT NOT NULL,\n"
            "    required_source_certifications_json LONGTEXT NOT NULL,\n",
        )
    if factor_refs:
        snapshot_ddl = snapshot_ddl.replace(
            "    source_certifications_json LONGTEXT NOT NULL,\n",
            "    source_certifications_json LONGTEXT NOT NULL,\n"
            "    factor_definitions_json LONGTEXT NOT NULL,\n",
        )
    _validate_schema_on_connection(
        connection,
        (_DATA_SOURCE_CERTIFICATION_TABLE_DDL, factor_ddl, snapshot_ddl),
    )
    base_expected = _expected_pit_factor_guard_triggers(connection)
    lineage_expected = _expected_pit_factor_lineage_triggers(connection)
    actual = _pit_factor_guard_trigger_signatures(connection)
    allowed_names = set(base_expected) | set(lineage_expected)
    if not set(base_expected).issubset(actual) or set(actual) - allowed_names:
        raise RuntimeError("V4 factor lineage trigger prefix drift detected")
    for name, signature in actual.items():
        expected = base_expected.get(name, lineage_expected.get(name))
        if signature != expected:
            raise RuntimeError(
                f"V4 factor lineage trigger prefix drift detected: {name}"
            )
    _validate_pit_factor_registry_rows(connection)
    if max_age and source_refs and factor_refs:
        _validate_pit_factor_lineage_rows(connection)
    return True


def _validate_pit_factor_lineage_final_contract(
    connection: Connection,
) -> None:
    _validate_schema_on_connection(connection, PIT_FACTOR_LINEAGE_TABLE_DDLS)
    expected = {
        **_expected_pit_factor_guard_triggers(connection),
        **_expected_pit_factor_lineage_triggers(connection),
    }
    actual = _pit_factor_guard_trigger_signatures(connection)
    if actual != expected:
        raise RuntimeError(
            "V4 factor lineage trigger drift detected: "
            f"expected={expected} actual={actual}"
        )
    _validate_pit_factor_registry_rows(connection)
    _validate_pit_factor_lineage_rows(connection)


def _validate_schema(
    engine: Engine,
    statements: tuple[str, ...],
    *,
    connection: Connection | None = None,
    migration_version: str | None = None,
) -> None:
    def validate(opened: Connection) -> None:
        if migration_version in {
            PIT_FACTOR_REGISTRY_MIGRATION_VERSION,
            PIT_FACTOR_GUARD_MIGRATION_VERSION,
        } and _applied_migration_record(
            engine,
            PIT_FACTOR_LINEAGE_MIGRATION_VERSION,
            connection=opened,
        ) is not None:
            _validate_pit_factor_lineage_final_contract(opened)
            return
        if migration_version in {
            PIT_FACTOR_REGISTRY_MIGRATION_VERSION,
            PIT_FACTOR_GUARD_MIGRATION_VERSION,
        } and _validate_pit_factor_lineage_prefix_contract(opened):
            return
        _validate_schema_on_connection(opened, statements)
        if migration_version == JOB_LEASE_MIGRATION_VERSION:
            _validate_job_lease_final_contract(opened)
        elif migration_version == CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION:
            _validate_claim_token_registry_final_contract(opened)
        elif migration_version == CONTROL_GUARD_MIGRATION_VERSION:
            _validate_control_guard_final_contract(opened)
        elif migration_version == PIT_FACTOR_REGISTRY_MIGRATION_VERSION:
            _validate_pit_factor_registry_final_contract(opened)
        elif migration_version == PIT_FACTOR_GUARD_MIGRATION_VERSION:
            _validate_pit_factor_guard_final_contract(opened)
        elif migration_version == PIT_FACTOR_LINEAGE_MIGRATION_VERSION:
            _validate_pit_factor_lineage_final_contract(opened)

    if connection is not None:
        validate(connection)
        return
    with engine.connect() as opened:
        validate(opened)


def _execute_job_lease_static_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    """Execute only one of the six frozen 002 statements."""

    if statement_index == 0:
        connection.execute(text(_JOB_LEASE_ADD_TOKEN_DDL))
    elif statement_index == 1:
        connection.execute(text(_JOB_LEASE_ADD_MAX_ATTEMPTS_DDL))
    elif statement_index == 2:
        connection.execute(text(_JOB_LEASE_ADD_DUE_INDEX_DDL))
    elif statement_index == 3:
        connection.execute(text(_JOB_LEASE_ADD_TOKEN_INDEX_DDL))
    elif statement_index == 4:
        _execute_mysql_regexp_compatible_statement(
            connection,
            _JOB_LEASE_INSERT_TRIGGER_DDL,
        )
    elif statement_index == 5:
        _execute_mysql_regexp_compatible_statement(
            connection,
            _JOB_LEASE_UPDATE_TRIGGER_DDL,
        )
    else:
        raise RuntimeError(
            "unknown V4 job lease migration statement index: "
            f"{statement_index}"
        )
    connection.commit()


def _apply_job_lease_statement(
    connection: Connection,
    statement_index: int,
    statement: str,
) -> None:
    """Recover one 002 implicit-commit boundary without blind replay."""

    frozen_statements = tuple(MIGRATIONS[1]["statements"])
    if (
        not 0 <= statement_index < len(frozen_statements)
        or statement.strip() != frozen_statements[statement_index].strip()
    ):
        raise RuntimeError("untrusted V4 job lease migration statement")

    if statement_index in {0, 1}:
        column_name = "lease_token" if statement_index == 0 else "max_attempts"
        expected = JOB_LEASE_COLUMN_CONTRACT[column_name]
        actual = _job_lease_column_signature(connection, column_name)
        if actual is None:
            _execute_job_lease_static_statement(connection, statement_index)
            return
        if actual != expected:
            raise RuntimeError(
                "V4 job lease column drift blocks recovery for "
                f"{column_name}: expected={expected} actual={actual}"
            )
        return
    if statement_index in {2, 3}:
        index_name = (
            "idx_v4_job_claim_due"
            if statement_index == 2
            else "uk_v4_job_lease_token"
        )
        expected = JOB_LEASE_INDEX_CONTRACT[index_name]
        actual = _job_lease_index_signature(connection, index_name)
        if actual is None:
            _execute_job_lease_static_statement(connection, statement_index)
            return
        if actual != expected:
            raise RuntimeError(
                "V4 job lease index drift blocks recovery for "
                f"{index_name}: expected={expected} actual={actual}"
            )
        return
    if statement_index in {4, 5}:
        trigger_name = JOB_LEASE_TRIGGER_NAMES[statement_index - 4]
        expected = _expected_job_lease_triggers(connection)[trigger_name]
        actual = _job_lease_trigger_signatures(connection).get(trigger_name)
        if actual is None:
            _execute_job_lease_static_statement(connection, statement_index)
            return
        if actual != expected:
            raise RuntimeError(
                "V4 job lease trigger drift blocks recovery for "
                f"{trigger_name}: expected={expected} actual={actual}"
            )
        return
    raise RuntimeError(
        f"unknown V4 job lease migration statement index: {statement_index}"
    )


def _execute_claim_token_registry_static_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    """Execute only one of the four frozen 003 statements."""

    if statement_index == 0:
        connection.execute(text(_CLAIM_TOKEN_REGISTRY_TABLE_DDL))
    elif statement_index == 1:
        _execute_mysql_regexp_compatible_statement(
            connection,
            _CLAIM_TOKEN_REGISTRY_INSERT_TRIGGER_DDL,
        )
    elif statement_index == 2:
        _execute_mysql_regexp_compatible_statement(
            connection,
            _CLAIM_TOKEN_REGISTRY_UPDATE_TRIGGER_DDL,
        )
    elif statement_index == 3:
        _execute_mysql_regexp_compatible_statement(
            connection,
            _CLAIM_TOKEN_REGISTRY_DELETE_TRIGGER_DDL,
        )
    else:
        raise RuntimeError(
            "unknown V4 claim token registry migration statement index: "
            f"{statement_index}"
        )
    connection.commit()


def _apply_claim_token_registry_statement(
    connection: Connection,
    statement_index: int,
    statement: str,
) -> None:
    """Recover one 003 implicit-commit boundary without blind replay."""

    frozen_statements = tuple(MIGRATIONS[2]["statements"])
    if (
        not 0 <= statement_index < len(frozen_statements)
        or statement.strip() != frozen_statements[statement_index].strip()
    ):
        raise RuntimeError("untrusted V4 claim token registry migration statement")

    if statement_index == 0:
        if not _claim_token_registry_table_exists(connection):
            _execute_claim_token_registry_static_statement(
                connection,
                statement_index,
            )
            return
        _validate_schema_on_connection(
            connection,
            (_CLAIM_TOKEN_REGISTRY_TABLE_DDL,),
        )
        for name, expected in CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT.items():
            actual = _claim_token_registry_column_signature(connection, name)
            if actual != expected:
                raise RuntimeError(
                    "V4 claim token registry column drift blocks recovery "
                    f"for {name}: expected={expected} actual={actual}"
                )
        for name, expected in CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT.items():
            actual = _claim_token_registry_index_signature(connection, name)
            if actual != expected:
                raise RuntimeError(
                    "V4 claim token registry index drift blocks recovery "
                    f"for {name}: expected={expected} actual={actual}"
                )
        return

    trigger_name = CLAIM_TOKEN_REGISTRY_TRIGGER_NAMES[statement_index - 1]
    expected = _expected_claim_token_registry_triggers(connection)[trigger_name]
    actual = _claim_token_registry_trigger_signatures(connection).get(
        trigger_name
    )
    if actual is None:
        _execute_claim_token_registry_static_statement(
            connection,
            statement_index,
        )
        return
    if actual != expected:
        raise RuntimeError(
            "V4 claim token registry trigger drift blocks recovery for "
            f"{trigger_name}: expected={expected} actual={actual}"
        )


def _execute_control_guard_static_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    """Execute only one of the sixteen frozen 004 trigger statements."""

    if statement_index == 0:
        _execute_mysql_regexp_compatible_statement(
            connection, _CONTEXT_UPDATE_GUARD_DDL
        )
    elif statement_index == 1:
        _execute_mysql_regexp_compatible_statement(
            connection, _CONTEXT_DELETE_GUARD_DDL
        )
    elif statement_index == 2:
        _execute_mysql_regexp_compatible_statement(
            connection, _WATERMARK_UPDATE_GUARD_DDL
        )
    elif statement_index == 3:
        _execute_mysql_regexp_compatible_statement(
            connection, _WATERMARK_DELETE_GUARD_DDL
        )
    elif statement_index == 4:
        _execute_mysql_regexp_compatible_statement(
            connection, _RUN_INSERT_GUARD_DDL
        )
    elif statement_index == 5:
        _execute_mysql_regexp_compatible_statement(
            connection, _RUN_UPDATE_GUARD_DDL
        )
    elif statement_index == 6:
        _execute_mysql_regexp_compatible_statement(
            connection, _RUN_DELETE_GUARD_DDL
        )
    elif statement_index == 7:
        _execute_mysql_regexp_compatible_statement(
            connection, _HEAD_INSERT_GUARD_DDL
        )
    elif statement_index == 8:
        _execute_mysql_regexp_compatible_statement(
            connection, _HEAD_UPDATE_GUARD_DDL
        )
    elif statement_index == 9:
        _execute_mysql_regexp_compatible_statement(
            connection, _HEAD_DELETE_GUARD_DDL
        )
    elif statement_index == 10:
        _execute_mysql_regexp_compatible_statement(
            connection, _CONTROL_INSERT_GUARD_DDL
        )
    elif statement_index == 11:
        _execute_mysql_regexp_compatible_statement(
            connection, _CONTROL_UPDATE_GUARD_DDL
        )
    elif statement_index == 12:
        _execute_mysql_regexp_compatible_statement(
            connection, _CONTROL_DELETE_GUARD_DDL
        )
    elif statement_index == 13:
        _execute_mysql_regexp_compatible_statement(
            connection, _TRANSITION_INSERT_GUARD_DDL
        )
    elif statement_index == 14:
        _execute_mysql_regexp_compatible_statement(
            connection, _TRANSITION_UPDATE_GUARD_DDL
        )
    elif statement_index == 15:
        _execute_mysql_regexp_compatible_statement(
            connection, _TRANSITION_DELETE_GUARD_DDL
        )
    else:
        raise RuntimeError(
            "unknown V4 control guard migration statement index: "
            f"{statement_index}"
        )
    connection.commit()


def _apply_control_guard_statement(
    connection: Connection,
    statement_index: int,
    statement: str,
) -> None:
    """Recover one 004 implicit-commit boundary without blind replay."""

    frozen_statements = tuple(MIGRATIONS[3]["statements"])
    if (
        not 0 <= statement_index < len(frozen_statements)
        or statement.strip() != frozen_statements[statement_index].strip()
    ):
        raise RuntimeError("untrusted V4 control guard migration statement")
    trigger_name = CONTROL_GUARD_TRIGGER_NAMES[statement_index]
    expected = _expected_control_guard_triggers(connection)[trigger_name]
    actual = _control_guard_trigger_signatures(connection).get(trigger_name)
    if actual is None:
        _execute_control_guard_static_statement(connection, statement_index)
        return
    if actual != expected:
        raise RuntimeError(
            "V4 control guard trigger drift blocks recovery for "
            f"{trigger_name}: expected={expected} actual={actual}"
        )


def _execute_pit_factor_registry_static_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    """Execute exactly one of the three frozen 005 table statements."""

    if statement_index == 0:
        connection.execute(text(_DATA_SOURCE_CERTIFICATION_TABLE_DDL))
    elif statement_index == 1:
        connection.execute(text(_FACTOR_DEFINITION_TABLE_DDL))
    elif statement_index == 2:
        connection.execute(text(_ENTITY_FEATURE_SNAPSHOT_TABLE_DDL))
    else:
        raise RuntimeError(
            "unknown V4 PIT factor registry statement index: "
            f"{statement_index}"
        )
    connection.commit()


def _apply_pit_factor_registry_statement(
    connection: Connection,
    statement_index: int,
    statement: str,
) -> None:
    """Recover one 005 implicit-commit boundary without blind replay."""

    frozen_statements = tuple(MIGRATIONS[4]["statements"])
    if (
        not 0 <= statement_index < len(frozen_statements)
        or statement.strip() != frozen_statements[statement_index].strip()
    ):
        raise RuntimeError("untrusted V4 PIT factor registry statement")
    table_name = PIT_FACTOR_REGISTRY_TABLES[statement_index]
    if not _pit_factor_registry_table_exists(connection, table_name):
        _execute_pit_factor_registry_static_statement(
            connection,
            statement_index,
        )
        return
    _validate_schema_on_connection(connection, (statement,))


def _execute_pit_factor_guard_static_statement(
    connection: Connection,
    statement_index: int,
) -> None:
    """Execute exactly one of the nine frozen 006 trigger statements."""

    if statement_index == 0:
        _execute_mysql_regexp_compatible_statement(
            connection, _DATA_SOURCE_CERTIFICATION_INSERT_GUARD_DDL
        )
    elif statement_index == 1:
        _execute_mysql_regexp_compatible_statement(
            connection, _DATA_SOURCE_CERTIFICATION_UPDATE_GUARD_DDL
        )
    elif statement_index == 2:
        _execute_mysql_regexp_compatible_statement(
            connection, _DATA_SOURCE_CERTIFICATION_DELETE_GUARD_DDL
        )
    elif statement_index == 3:
        _execute_mysql_regexp_compatible_statement(
            connection, _FACTOR_DEFINITION_INSERT_GUARD_DDL
        )
    elif statement_index == 4:
        _execute_mysql_regexp_compatible_statement(
            connection, _FACTOR_DEFINITION_UPDATE_GUARD_DDL
        )
    elif statement_index == 5:
        _execute_mysql_regexp_compatible_statement(
            connection, _FACTOR_DEFINITION_DELETE_GUARD_DDL
        )
    elif statement_index == 6:
        _execute_mysql_regexp_compatible_statement(
            connection, _ENTITY_FEATURE_SNAPSHOT_INSERT_GUARD_DDL
        )
    elif statement_index == 7:
        _execute_mysql_regexp_compatible_statement(
            connection, _ENTITY_FEATURE_SNAPSHOT_UPDATE_GUARD_DDL
        )
    elif statement_index == 8:
        _execute_mysql_regexp_compatible_statement(
            connection, _ENTITY_FEATURE_SNAPSHOT_DELETE_GUARD_DDL
        )
    else:
        raise RuntimeError(
            "unknown V4 PIT factor guard statement index: "
            f"{statement_index}"
        )
    connection.commit()


def _apply_pit_factor_guard_statement(
    connection: Connection,
    statement_index: int,
    statement: str,
) -> None:
    """Recover one 006 implicit-commit boundary without blind replay."""

    frozen_statements = tuple(MIGRATIONS[5]["statements"])
    if (
        not 0 <= statement_index < len(frozen_statements)
        or statement.strip() != frozen_statements[statement_index].strip()
    ):
        raise RuntimeError("untrusted V4 PIT factor guard statement")
    trigger_name = PIT_FACTOR_GUARD_TRIGGER_NAMES[statement_index]
    expected = _expected_pit_factor_guard_triggers(connection)[trigger_name]
    actual = _pit_factor_guard_trigger_signatures(connection).get(
        trigger_name
    )
    if actual is None:
        _execute_pit_factor_guard_static_statement(
            connection,
            statement_index,
        )
        return
    if actual != expected:
        raise RuntimeError(
            "V4 PIT factor guard trigger drift blocks recovery for "
            f"{trigger_name}: expected={expected} actual={actual}"
        )


def _checksum(statements: tuple[str, ...]) -> str:
    return hashlib.sha256(
        "\n".join(statement.strip() for statement in statements).encode(
            "utf-8"
        )
    ).hexdigest()


def _mysql_dialect(engine: Engine) -> bool:
    return engine.dialect.name.lower() in {"mysql", "mariadb"}


def _migration_server_version(
    engine: Engine,
    connection: Connection,
) -> str:
    dialect = getattr(connection, "dialect", engine.dialect)
    if isinstance(connection, Connection):
        return str(
            connection.execute(text("SELECT VERSION()")).scalar() or ""
        ).strip()
    version_info = getattr(dialect, "server_version_info", None)
    if isinstance(version_info, tuple) and len(version_info) >= 3:
        return ".".join(str(item) for item in version_info[:3])
    return ""


def _validate_migration_server(
    engine: Engine,
    connection: Connection,
) -> None:
    dialect = getattr(connection, "dialect", engine.dialect)
    dialect_name = str(getattr(dialect, "name", "")).casefold()
    version = _migration_server_version(engine, connection)
    if dialect_name != "mysql" or not is_isolated_acceptance_version(version):
        raise RuntimeError(
            "V4 migrations require validated Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly; "
            f"server_version={version or 'unknown'}"
        )


def _migration_table_exists(
    engine: Engine,
    *,
    connection: Connection | None = None,
) -> bool:
    if connection is not None:
        return bool(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = :table_name
                    """
                ),
                {"table_name": MIGRATION_TABLE},
            ).scalar()
        )
    with engine.connect() as opened:
        return _migration_table_exists(engine, connection=opened)


def _applied_migration_record(
    engine: Engine,
    version: str,
    *,
    connection: Connection | None = None,
) -> _AppliedMigrationRecord | None:
    if not _migration_table_exists(engine, connection=connection):
        return None
    if connection is not None:
        row = connection.execute(
            text(
                "SELECT checksum, statement_count FROM schema_migration_v4 "
                "WHERE version = :version"
            ),
            {"version": version},
        ).mappings().first()
        if row is None:
            return None
        count = row["statement_count"]
        if type(count) is not int:
            raise RuntimeError(
                f"invalid V4 migration statement_count type: {version}"
            )
        return _AppliedMigrationRecord(
            checksum=str(row["checksum"]),
            statement_count=count,
        )
    with engine.connect() as opened:
        value = _applied_migration_record(
            engine,
            version,
            connection=opened,
        )
    return value


def _applied_checksum(
    engine: Engine,
    version: str,
    *,
    connection: Connection | None = None,
) -> str | None:
    record = _applied_migration_record(
        engine,
        version,
        connection=connection,
    )
    return record.checksum if record is not None else None


def _run_v4_migrations_unlocked(
    engine: Engine,
    *,
    dry_run: bool = False,
    connection: Connection | None = None,
) -> list[V4MigrationResult]:
    """Apply the expand-only V4 MySQL schema.

    MySQL DDL is not treated as transactional.  Each statement is idempotent,
    and the migration ledger is written only after every statement succeeds.
    A concurrent runner uses ``INSERT IGNORE`` and then verifies the stored
    checksum, so it cannot silently accept a different migration body.

    A non-MySQL engine is supported only for a dry-run plan.  Repository state
    logic is tested independently with SQLite.
    """

    if not _mysql_dialect(engine):
        if not dry_run:
            raise RuntimeError("V4 migrations require MySQL or MariaDB")
        return [
            V4MigrationResult(
                version=str(migration["version"]),
                status="would_apply",
                statement_count=len(tuple(migration["statements"])),
                checksum=_checksum(tuple(migration["statements"])),
            )
            for migration in MIGRATIONS
        ]
    if not dry_run and connection is None:
        raise RuntimeError(
            "V4 migration writes require the named-lock connection"
        )
    if not dry_run:
        assert connection is not None
        _validate_migration_server(engine, connection)
    if not dry_run:
        assert connection is not None
        connection.execute(text(MIGRATION_TABLE_DDL))
        connection.commit()
        _validate_schema(
            engine,
            (MIGRATION_TABLE_DDL,),
            connection=connection,
        )

    results: list[V4MigrationResult] = []
    for migration in MIGRATIONS:
        version = str(migration["version"])
        statements = tuple(migration["statements"])
        checksum = _checksum(statements)
        applied = _applied_migration_record(
            engine,
            version,
            connection=connection,
        )
        if applied is not None:
            if (
                applied.checksum != checksum
                or applied.statement_count != len(statements)
            ):
                raise RuntimeError(
                    "applied V4 migration ledger contract changed: "
                    f"{version} checksum={applied.checksum} "
                    f"statement_count={applied.statement_count}"
                )
            _validate_schema(
                engine,
                statements,
                connection=connection,
                migration_version=version,
            )
            results.append(
                V4MigrationResult(
                    version,
                    "exists",
                    len(statements),
                    checksum,
                )
            )
            continue
        if dry_run:
            results.append(
                V4MigrationResult(
                    version,
                    "would_apply",
                    len(statements),
                    checksum,
                )
            )
            continue

        if version == JOB_LEASE_MIGRATION_VERSION:
            assert connection is not None
            _validate_job_lease_preflight_empty(connection)
            _validate_job_lease_existing_prefix_contract(connection)
        elif version == CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION:
            assert connection is not None
            _validate_claim_token_registry_preflight(connection)
            _validate_claim_token_registry_existing_prefix_contract(
                connection
            )
        elif version == CONTROL_GUARD_MIGRATION_VERSION:
            assert connection is not None
            _validate_control_guard_preflight(connection)
            _validate_control_guard_existing_prefix_contract(connection)
        elif version == PIT_FACTOR_REGISTRY_MIGRATION_VERSION:
            assert connection is not None
            _validate_pit_factor_registry_existing_prefix_contract(
                connection
            )
        elif version == PIT_FACTOR_GUARD_MIGRATION_VERSION:
            assert connection is not None
            _validate_pit_factor_guard_preflight(connection)
            _validate_pit_factor_guard_existing_prefix_contract(connection)
        elif version == PIT_FACTOR_LINEAGE_MIGRATION_VERSION:
            assert connection is not None
            _validate_pit_factor_lineage_preflight(connection)

        for statement in statements:
            assert connection is not None
            if version == JOB_LEASE_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_job_lease_statement(
                    connection,
                    statement_index,
                    statement,
                )
            elif version == CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_claim_token_registry_statement(
                    connection,
                    statement_index,
                    statement,
                )
            elif version == CONTROL_GUARD_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_control_guard_statement(
                    connection,
                    statement_index,
                    statement,
                )
            elif version == PIT_FACTOR_REGISTRY_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_pit_factor_registry_statement(
                    connection,
                    statement_index,
                    statement,
                )
            elif version == PIT_FACTOR_GUARD_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_pit_factor_guard_statement(
                    connection,
                    statement_index,
                    statement,
                )
            elif version == PIT_FACTOR_LINEAGE_MIGRATION_VERSION:
                statement_index = statements.index(statement)
                _apply_pit_factor_lineage_statement(
                    connection,
                    statement_index,
                )
            else:
                connection.execute(text(statement))
                connection.commit()

        _validate_schema(
            engine,
            statements,
            connection=connection,
            migration_version=version,
        )

        ledger_statement = text(
            """
            INSERT IGNORE INTO schema_migration_v4 (
                version, checksum, statement_count
            ) VALUES (
                :version, :checksum, :statement_count
            )
            """
        )
        ledger_parameters = {
            "version": version,
            "checksum": checksum,
            "statement_count": len(statements),
        }
        assert connection is not None
        connection.execute(ledger_statement, ledger_parameters)
        connection.commit()
        recorded = _applied_migration_record(
            engine,
            version,
            connection=connection,
        )
        if (
            recorded is None
            or recorded.checksum != checksum
            or recorded.statement_count != len(statements)
        ):
            raise RuntimeError(
                f"V4 migration ledger record conflict: {version}"
            )
        results.append(
            V4MigrationResult(
                version,
                "applied",
                len(statements),
                checksum,
            )
        )
    return results


def run_v4_migrations(
    engine: Engine,
    *,
    dry_run: bool = False,
) -> list[V4MigrationResult]:
    """Plan or apply V4 DDL under a cross-process MySQL advisory lock."""

    if dry_run or not _mysql_dialect(engine):
        return _run_v4_migrations_unlocked(engine, dry_run=dry_run)
    with mysql_named_lock(
        engine,
        "probiga:trading_v4_schema",
        timeout_seconds=30,
    ) as lock_connection:
        return _run_v4_migrations_unlocked(
            engine,
            dry_run=False,
            connection=lock_connection,
        )


__all__ = [
    "CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT",
    "CLAIM_TOKEN_REGISTRY_CONSTRAINT_CONTRACT",
    "CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT",
    "CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION",
    "CLAIM_TOKEN_REGISTRY_TABLE",
    "CLAIM_TOKEN_REGISTRY_TRIGGER_NAMES",
    "CONTROL_GUARD_MIGRATION_VERSION",
    "CONTROL_GUARD_TABLES",
    "CONTROL_GUARD_TRIGGER_NAMES",
    "CONTROL_GUARD_TRIGGER_SPECS",
    "JOB_LEASE_COLUMN_CONTRACT",
    "JOB_LEASE_DB_CLOCK_MAX_SKEW_SECONDS",
    "JOB_LEASE_EXHAUSTED_ERROR_CODE",
    "JOB_LEASE_EXHAUSTED_ERROR_MESSAGE",
    "JOB_LEASE_INDEX_CONTRACT",
    "JOB_LEASE_MIGRATION_VERSION",
    "JOB_LEASE_MAX_DURATION_SECONDS",
    "JOB_LEASE_TABLE",
    "JOB_LEASE_TRIGGER_NAMES",
    "MIGRATION_TABLE",
    "MIGRATION_TABLE_DDL",
    "MIGRATIONS",
    "PIT_FACTOR_GUARD_MIGRATION_VERSION",
    "PIT_FACTOR_GUARD_TRIGGER_NAMES",
    "PIT_FACTOR_GUARD_TRIGGER_SPECS",
    "PIT_FACTOR_LINEAGE_MIGRATION_VERSION",
    "PIT_FACTOR_LINEAGE_STATEMENTS",
    "PIT_FACTOR_LINEAGE_TABLE_DDLS",
    "PIT_FACTOR_LINEAGE_TRIGGER_SPECS",
    "PIT_FACTOR_REGISTRY_MIGRATION_VERSION",
    "PIT_FACTOR_REGISTRY_TABLE_DDLS",
    "PIT_FACTOR_REGISTRY_TABLES",
    "V4MigrationResult",
    "run_v4_migrations",
]
