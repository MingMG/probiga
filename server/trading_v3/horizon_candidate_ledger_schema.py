from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection

from server.common.mysql_version_policy import (
    MYSQL_84_ISOLATED_ACCEPTANCE,
    isolated_acceptance_version,
    is_oracle_mysql_distribution,
)

from .horizon_protocol_v2_schema import (
    CURRENT_HORIZON_ARTIFACT_SCHEMA as HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    CURRENT_HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_POLICY_HASH,
    CURRENT_HORIZON_SELECTION_PROTOCOL,
    CURRENT_HORIZON_SUITE_SCHEMA as HISTORICAL_HORIZON_SUITE_SCHEMA_V2,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
    validate_horizon_protocol_v2_schema,
)


HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION = (
    "20260817_001_horizon_candidate_ledger_registration"
)

# Frozen V3 identities.  They deliberately live outside the model module so an
# already-applied database migration never changes when a later model protocol
# is released.
CURRENT_HORIZON_ARTIFACT_SCHEMA = (
    "probiga.trading-v3.independent-horizon-model-artifact.v3"
)
CURRENT_HORIZON_SUITE_SCHEMA = (
    "probiga.trading-v3.independent-horizon-model-suite.v3"
)
CANDIDATE_EVALUATION_LEDGER_SCHEMA = (
    "probiga.trading-v3.prequential-candidate-evaluation-ledger.v1"
)
CANDIDATE_LEDGER_BINDING_PROTOCOL = (
    "FULL_PREQUENTIAL_OOS_CANDIDATE_LEDGER_CONTENT_ADDRESS_V1"
)
CANDIDATE_LEDGER_ENCODING = "DETERMINISTIC_GZIP_CANONICAL_JSONL_V1"
CANDIDATE_LEDGER_REGISTRATION_PROTOCOL = (
    "STREAM_VERIFIED_CANDIDATE_LEDGER_REGISTRATION_V1"
)
PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL = (
    "PROCESS_VERIFIED_LEDGER_REGISTRATION_V1"
)


_LEDGER_VERIFIED_SQL = f"""
               AND a.candidate_ledger_schema_version
                   = '{CANDIDATE_EVALUATION_LEDGER_SCHEMA}'
               AND a.candidate_ledger_content_sha256
                   REGEXP '^[0-9a-f]{{64}}$'
               AND a.candidate_ledger_row_count > 0
               AND a.ledger_registration_evidence_hash
                   REGEXP '^[0-9a-f]{{64}}$'
               AND a.registration_verification_hash
                   REGEXP '^[0-9a-f]{{64}}$'
               AND a.registration_evidence_hash
                   REGEXP '^[0-9a-f]{{64}}$'
               AND a.registration_verification_hash = SHA2(CONCAT(
                    '{{\"artifact_hash\":\"', a.artifact_id,
                    '\",\"ledger_registration_evidence_hash\":\"',
                    a.ledger_registration_evidence_hash,
                    '\",\"protocol\":\"{PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL}',
                    '\",\"training_receipt_hash\":\"',
                    a.training_receipt_hash, '\"}}'
               ), 256)
"""


# MySQL 8.4 is intentional here: DROP CHECK is required to widen the frozen V2
# projection constraint without mutating its already-shipped migration.  The
# runner and validator reject every other server before this DDL is attempted.
HORIZON_CANDIDATE_LEDGER_DDL: tuple[str, ...] = (
    f"""
    ALTER TABLE st_horizon_model_artifact_v3
        ADD COLUMN candidate_ledger_schema_version VARCHAR(96) NULL
            AFTER selection_policy_hash,
        ADD COLUMN candidate_ledger_content_sha256 CHAR(64) NULL
            AFTER candidate_ledger_schema_version,
        ADD COLUMN candidate_ledger_row_count BIGINT UNSIGNED NULL
            AFTER candidate_ledger_content_sha256,
        ADD COLUMN ledger_registration_evidence_hash CHAR(64) NULL
            AFTER candidate_ledger_row_count,
        ADD COLUMN registration_verification_hash CHAR(64) NULL
            AFTER ledger_registration_evidence_hash,
        DROP CHECK chk_v3_horizon_model_protocol_projection,
        ADD CONSTRAINT chk_v3_horizon_model_protocol_projection
            CHECK (
                (
                    artifact_schema_version
                        = '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1}'
                    AND model_protocol IS NULL
                    AND selection_policy_hash IS NULL
                    AND candidate_ledger_schema_version IS NULL
                    AND candidate_ledger_content_sha256 IS NULL
                    AND candidate_ledger_row_count IS NULL
                    AND ledger_registration_evidence_hash IS NULL
                    AND registration_verification_hash IS NULL
                )
                OR
                (
                    artifact_schema_version
                        = '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2}'
                    AND model_protocol
                        = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
                    AND selection_policy_hash REGEXP '^[0-9a-f]{{64}}$'
                    AND candidate_ledger_schema_version IS NULL
                    AND candidate_ledger_content_sha256 IS NULL
                    AND candidate_ledger_row_count IS NULL
                    AND ledger_registration_evidence_hash IS NULL
                    AND registration_verification_hash IS NULL
                )
                OR
                (
                    artifact_schema_version
                        = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}'
                    AND model_protocol
                        = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
                    AND selection_policy_hash REGEXP '^[0-9a-f]{{64}}$'
                    AND candidate_ledger_schema_version
                        = '{CANDIDATE_EVALUATION_LEDGER_SCHEMA}'
                    AND candidate_ledger_content_sha256
                        REGEXP '^[0-9a-f]{{64}}$'
                    AND candidate_ledger_row_count >= 0
                    AND (
                        (
                            training_receipt_status = 'UNVERIFIED'
                            AND artifact_status = 'BLOCKED'
                            AND ledger_registration_evidence_hash IS NULL
                            AND registration_verification_hash IS NULL
                        )
                        OR
                        (
                            training_receipt_status = 'PROCESS_VERIFIED'
                            AND candidate_ledger_row_count > 0
                            AND ledger_registration_evidence_hash
                                REGEXP '^[0-9a-f]{{64}}$'
                            AND registration_verification_hash
                                REGEXP '^[0-9a-f]{{64}}$'
                        )
                    )
                )
            )
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_model_protocol_v2_bi",
    f"""
    CREATE TRIGGER trg_v3_horizon_model_protocol_v2_bi
    BEFORE INSERT ON st_horizon_model_artifact_v3
    FOR EACH ROW
    BEGIN
        IF NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.schema_version'
            )) <=> NEW.artifact_schema_version) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon artifact schema projection differs';
        END IF;

        IF NEW.artifact_schema_version
                = '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1}' THEN
            IF NEW.model_protocol IS NOT NULL
               OR NEW.selection_policy_hash IS NOT NULL
               OR NEW.candidate_ledger_schema_version IS NOT NULL
               OR NEW.candidate_ledger_content_sha256 IS NOT NULL
               OR NEW.candidate_ledger_row_count IS NOT NULL
               OR NEW.ledger_registration_evidence_hash IS NOT NULL
               OR NEW.registration_verification_hash IS NOT NULL
               OR NEW.artifact_status <> 'BLOCKED'
               OR NEW.training_receipt_status <> 'UNVERIFIED'
               OR NEW.order_authority <> 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 historical horizon protocol is audit-only';
            END IF;
        ELSEIF NEW.artifact_schema_version
                = '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2}' THEN
            IF NOT (NEW.model_protocol
                    <=> '{CURRENT_HORIZON_MODEL_PROTOCOL}')
               OR NOT (NEW.selection_policy_hash
                    <=> '{CURRENT_HORIZON_SELECTION_POLICY_HASH}')
               OR NEW.candidate_ledger_schema_version IS NOT NULL
               OR NEW.candidate_ledger_content_sha256 IS NOT NULL
               OR NEW.candidate_ledger_row_count IS NOT NULL
               OR NEW.ledger_registration_evidence_hash IS NOT NULL
               OR NEW.registration_verification_hash IS NOT NULL
               OR NEW.artifact_status <> 'BLOCKED'
               OR NEW.training_receipt_status <> 'UNVERIFIED'
               OR NEW.order_authority <> 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 historical V2 horizon protocol is audit-only';
            END IF;
        ELSEIF NEW.artifact_schema_version
                = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}' THEN
            IF NOT (NEW.model_protocol
                    <=> '{CURRENT_HORIZON_MODEL_PROTOCOL}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.model_protocol'
               )) <=> NEW.model_protocol)
               OR NOT (NEW.selection_policy_hash
                    <=> '{CURRENT_HORIZON_SELECTION_POLICY_HASH}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.selection_policy.selection_policy_hash'
               )) <=> NEW.selection_policy_hash)
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selection_evidence.selection_policy_hash'
               )) <=> NEW.selection_policy_hash)
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selection_evidence.protocol'
               )) <=> '{CURRENT_HORIZON_SELECTION_PROTOCOL}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selection_evidence.order_authority'
               )) <=> 'false')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selection_evidence.deployment_candidate_domain_verified'
               )) <=> 'false')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.economic_metrics_use_frozen_selection_ledger'
               )) <=> 'true')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.schema_version'
               )) <=> NEW.candidate_ledger_schema_version)
               OR NOT (NEW.candidate_ledger_schema_version
                    <=> '{CANDIDATE_EVALUATION_LEDGER_SCHEMA}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.binding_protocol'
               )) <=> '{CANDIDATE_LEDGER_BINDING_PROTOCOL}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.encoding'
               )) <=> '{CANDIDATE_LEDGER_ENCODING}')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.hash_algorithm'
               )) <=> 'SHA256_COMPRESSED_BYTES')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.content_sha256'
               )) <=> NEW.candidate_ledger_content_sha256)
               OR NEW.candidate_ledger_content_sha256
                    NOT REGEXP '^[0-9a-f]{{64}}$'
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.relative_path'
               )) <=> CONCAT(
                    'candidate-ledgers/sha256/',
                    LEFT(NEW.candidate_ledger_content_sha256, 2), '/',
                    NEW.candidate_ledger_content_sha256, '.jsonl.gz'
               ))
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.canonical_records_sha256'
               ) IS NULL
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.canonical_records_sha256'
               )) NOT REGEXP '^[0-9a-f]{{64}}$'
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.header_hash'
               ) IS NULL
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.header_hash'
               )) NOT REGEXP '^[0-9a-f]{{64}}$'
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.reference_hash'
               ) IS NULL
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.reference_hash'
               )) NOT REGEXP '^[0-9a-f]{{64}}$'
               OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.row_count'
               )) AS UNSIGNED) <=> NEW.candidate_ledger_row_count)
               OR NEW.candidate_ledger_row_count < 0
               OR COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.compressed_size_bytes'
               )) AS UNSIGNED), 0) <= 0
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.session_count'
               ) IS NULL
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.evaluation_row_count'
               ) IS NULL
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.evaluation_session_count'
               ) IS NULL
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.fold_count'
               ) IS NULL
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.candidate_evaluation_ledger.registration_verification_required'
               )) <=> 'true')
               OR NEW.order_authority <> 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 current horizon ledger projection differs';
            END IF;

            IF NEW.training_receipt_status = 'PROCESS_VERIFIED' THEN
                IF NEW.candidate_ledger_row_count <= 0
                   OR NEW.artifact_status NOT IN ('BLOCKED', 'OOS_VERIFIED')
                   OR NEW.registration_evidence_hash
                        NOT REGEXP '^[0-9a-f]{{64}}$'
                   OR NEW.ledger_registration_evidence_hash
                        NOT REGEXP '^[0-9a-f]{{64}}$'
                   OR NEW.registration_verification_hash
                        NOT REGEXP '^[0-9a-f]{{64}}$'
                   OR NEW.registration_verification_hash <> SHA2(CONCAT(
                        '{{\"artifact_hash\":\"', NEW.artifact_id,
                        '\",\"ledger_registration_evidence_hash\":\"',
                        NEW.ledger_registration_evidence_hash,
                        '\",\"protocol\":\"{PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL}',
                        '\",\"training_receipt_hash\":\"',
                        NEW.training_receipt_hash, '\"}}'
                   ), 256) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 process receipt requires stream verified ledger';
                END IF;
            ELSEIF NEW.training_receipt_status = 'UNVERIFIED' THEN
                IF NEW.artifact_status <> 'BLOCKED'
                   OR NEW.ledger_registration_evidence_hash IS NOT NULL
                   OR NEW.registration_verification_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 unverified receipt cannot claim ledger verification';
                END IF;
            ELSE
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 horizon training receipt status unsupported';
            END IF;
        ELSE
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon artifact schema is unsupported';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_contract_protocol_v2_bi",
    f"""
    CREATE TRIGGER trg_v3_horizon_contract_protocol_v2_bi
    BEFORE INSERT ON st_horizon_forecast_contract_v3
    FOR EACH ROW
    BEGIN
        DECLARE current_artifact_count BIGINT DEFAULT 0;
        IF NEW.prediction_kind = 'CALIBRATED_OOS' THEN
            SELECT COUNT(*)
              INTO current_artifact_count
              FROM st_horizon_model_artifact_v3 a
              JOIN st_decision_run_v3 r ON r.run_uid = NEW.run_uid
             WHERE a.model_key = NEW.model_key
               AND a.model_version = NEW.model_version
               AND a.horizon_days = NEW.horizon_days
               AND a.model_artifact_hash = NEW.model_artifact_hash
               AND a.feature_protocol_hash = NEW.feature_protocol_hash
               AND a.calibration_evidence_hash
                   = NEW.calibration_evidence_hash
               AND a.artifact_schema_version
                   = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}'
               AND a.model_protocol = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
               AND a.selection_policy_hash
                   = '{CURRENT_HORIZON_SELECTION_POLICY_HASH}'
               AND a.artifact_status = 'OOS_VERIFIED'
               AND a.training_receipt_status = 'PROCESS_VERIFIED'
               AND a.config_hash = r.config_hash
               AND a.created_at <= NEW.decision_as_of
               AND a.evidence_valid_until >= NEW.decision_as_of
               AND a.order_authority = 0
               {_LEDGER_VERIFIED_SQL};
            IF current_artifact_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibrated contract requires current V2 protocol / V3 ledger';
            END IF;
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_outcome_protocol_v2_bi",
    f"""
    CREATE TRIGGER trg_v3_horizon_outcome_protocol_v2_bi
    BEFORE INSERT ON st_horizon_outcome_v3
    FOR EACH ROW
    BEGIN
        DECLARE current_artifact_count BIGINT DEFAULT 0;
        IF EXISTS (
            SELECT 1
              FROM st_horizon_forecast_contract_v3 h
             WHERE h.contract_id = NEW.contract_id
               AND h.prediction_kind = 'CALIBRATED_OOS'
        ) THEN
            SELECT COUNT(*)
              INTO current_artifact_count
              FROM st_horizon_forecast_contract_v3 h
              JOIN st_decision_run_v3 r ON r.run_uid = h.run_uid
              JOIN st_horizon_model_artifact_v3 a
                ON a.model_key = h.model_key
               AND a.model_version = h.model_version
               AND a.horizon_days = h.horizon_days
               AND a.model_artifact_hash = h.model_artifact_hash
               AND a.feature_protocol_hash = h.feature_protocol_hash
               AND a.calibration_evidence_hash = h.calibration_evidence_hash
             WHERE h.contract_id = NEW.contract_id
               AND h.contract_hash = NEW.contract_hash
               AND a.artifact_schema_version
                   = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}'
               AND a.model_protocol = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
               AND a.selection_policy_hash
                   = '{CURRENT_HORIZON_SELECTION_POLICY_HASH}'
               AND a.artifact_status = 'OOS_VERIFIED'
               AND a.training_receipt_status = 'PROCESS_VERIFIED'
               AND a.config_hash = r.config_hash
               AND a.created_at <= h.decision_as_of
               AND a.evidence_valid_until >= h.decision_as_of
               AND a.order_authority = 0
               {_LEDGER_VERIFIED_SQL};
            IF current_artifact_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibrated outcome requires current V2 protocol / V3 ledger';
            END IF;
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_shadow_release_protocol_v2_bi",
    f"""
    CREATE TRIGGER trg_v3_shadow_release_protocol_v2_bi
    BEFORE INSERT ON st_shadow_release_v3
    FOR EACH ROW
    BEGIN
        DECLARE current_artifact_count BIGINT DEFAULT 0;
        DECLARE deployment_domain_verified VARCHAR(8) DEFAULT NULL;
        SELECT COUNT(*), MAX(JSON_UNQUOTE(JSON_EXTRACT(
                    a.artifact_json,
                    '$.oos_evidence.selection_evidence.deployment_candidate_domain_verified'
               )))
          INTO current_artifact_count, deployment_domain_verified
          FROM st_horizon_model_artifact_v3 a
         WHERE a.release_id = NEW.release_id
           AND a.model_key = NEW.model_key
           AND a.model_version = NEW.model_version
           AND a.horizon_days = NEW.horizon_days
           AND a.artifact_schema_version
               = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}'
           AND a.model_protocol = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
           AND a.selection_policy_hash
               = '{CURRENT_HORIZON_SELECTION_POLICY_HASH}'
           AND a.artifact_status = 'OOS_VERIFIED'
           AND a.training_receipt_status = 'PROCESS_VERIFIED'
           AND a.config_hash = NEW.config_hash
           AND a.order_authority = 0
           {_LEDGER_VERIFIED_SQL};
        IF current_artifact_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 release requires one current V2 artifact / V3 ledger';
        END IF;
        IF NEW.current_stage = 'PAPER_ELIGIBLE'
           AND COALESCE(deployment_domain_verified, 'false') <> 'true' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 diagnostic selection cannot become paper eligible';
        END IF;
    END
    """,
)


_EXPECTED_COLUMNS: Mapping[str, tuple[str, str]] = {
    "candidate_ledger_schema_version": ("varchar(96)", "YES"),
    "candidate_ledger_content_sha256": ("char(64)", "YES"),
    "candidate_ledger_row_count": ("bigint unsigned", "YES"),
    "ledger_registration_evidence_hash": ("char(64)", "YES"),
    "registration_verification_hash": ("char(64)", "YES"),
}

_EXPECTED_TRIGGERS: Mapping[str, tuple[str, ...]] = {
    "trg_v3_horizon_model_protocol_v2_bi": (
        "historical horizon protocol is audit-only",
        "historical V2 horizon protocol is audit-only",
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL,
    ),
    "trg_v3_horizon_contract_protocol_v2_bi": (
        "calibrated contract requires current V2 protocol",
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "ledger_registration_evidence_hash",
        "registration_verification_hash",
    ),
    "trg_v3_horizon_outcome_protocol_v2_bi": (
        "calibrated outcome requires current V2 protocol",
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "ledger_registration_evidence_hash",
        "registration_verification_hash",
    ),
    "trg_v3_shadow_release_protocol_v2_bi": (
        "release requires one current V2 artifact",
        "diagnostic selection cannot become paper eligible",
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "registration_verification_hash",
    ),
}


def validate_horizon_candidate_ledger_server(connection: Connection) -> None:
    version = str(connection.execute(text("SELECT VERSION()")).scalar() or "")
    version_comment = str(
        connection.execute(text("SELECT @@version_comment")).scalar() or ""
    )
    if (
        isolated_acceptance_version(version) != MYSQL_84_ISOLATED_ACCEPTANCE
        or not is_oracle_mysql_distribution(version, version_comment)
    ):
        raise RuntimeError(
            "Horizon V3 candidate-ledger migration requires validated Oracle "
            f"MySQL {MYSQL_84_ISOLATED_ACCEPTANCE} exactly; "
            f"server_version={version or 'unknown'}; "
            f"version_comment={version_comment or 'unknown'}"
        )


def validate_horizon_candidate_ledger_schema(
    connection: Connection,
    *,
    require_triggers: bool = True,
    require_isolated_server: bool = True,
) -> None:
    """Validate the current V3 ledger registry without rewriting V1/V2."""

    if require_isolated_server:
        validate_horizon_candidate_ledger_server(connection)
    validate_horizon_protocol_v2_schema(
        connection,
        require_triggers=require_triggers,
    )
    rows = connection.execute(text(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "AND TABLE_NAME = 'st_horizon_model_artifact_v3'"
    )).mappings().all()
    columns = {str(row["COLUMN_NAME"]): row for row in rows}
    for name, (column_type, nullable) in _EXPECTED_COLUMNS.items():
        row = columns.get(name)
        if (
            row is None
            or str(row["COLUMN_TYPE"]).casefold() != column_type
            or str(row["IS_NULLABLE"]).upper() != nullable
            or row["COLUMN_DEFAULT"] is not None
        ):
            raise RuntimeError(
                f"V3 horizon candidate-ledger column drift: {name}"
            )

    check_row = connection.execute(text(
        "SELECT cc.CHECK_CLAUSE "
        "FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
        "AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA = DATABASE() "
        "AND tc.TABLE_NAME = 'st_horizon_model_artifact_v3' "
        "AND tc.CONSTRAINT_NAME = "
        "'chk_v3_horizon_model_protocol_projection'"
    )).scalar()
    normalized_check = " ".join(str(check_row or "").replace("`", "").split())
    check_markers = (
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "ledger_registration_evidence_hash",
        "registration_verification_hash",
        "training_receipt_status",
    )
    if any(marker not in normalized_check for marker in check_markers):
        raise RuntimeError("V3 horizon candidate-ledger check drift")

    if not require_triggers:
        return

    trigger_rows = connection.execute(text(
        "SELECT TRIGGER_NAME, ACTION_STATEMENT "
        "FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA = DATABASE()"
    )).mappings().all()
    triggers = {
        str(row["TRIGGER_NAME"]): str(row["ACTION_STATEMENT"])
        for row in trigger_rows
    }
    for name, markers in _EXPECTED_TRIGGERS.items():
        body = triggers.get(name, "")
        if any(marker not in body for marker in markers):
            raise RuntimeError(
                f"V3 horizon candidate-ledger trigger drift: {name}"
            )


__all__ = [
    "CANDIDATE_EVALUATION_LEDGER_SCHEMA",
    "CANDIDATE_LEDGER_BINDING_PROTOCOL",
    "CANDIDATE_LEDGER_ENCODING",
    "CANDIDATE_LEDGER_REGISTRATION_PROTOCOL",
    "CURRENT_HORIZON_ARTIFACT_SCHEMA",
    "CURRENT_HORIZON_MODEL_PROTOCOL",
    "CURRENT_HORIZON_SELECTION_POLICY_HASH",
    "CURRENT_HORIZON_SELECTION_PROTOCOL",
    "CURRENT_HORIZON_SUITE_SCHEMA",
    "HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1",
    "HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2",
    "HISTORICAL_HORIZON_SUITE_SCHEMA_V1",
    "HISTORICAL_HORIZON_SUITE_SCHEMA_V2",
    "HORIZON_CANDIDATE_LEDGER_DDL",
    "HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION",
    "PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL",
    "validate_horizon_candidate_ledger_schema",
    "validate_horizon_candidate_ledger_server",
]
