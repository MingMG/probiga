from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .shadow_intelligence_schema import validate_shadow_intelligence_schema


HORIZON_PROTOCOL_V2_MIGRATION_VERSION = (
    "20260817_000_horizon_protocol_v2_governance"
)

# These values are deliberately frozen in the migration contract.  Runtime
# code asserts that the model implementation exposes the same identities, but
# an already-applied migration must never change when a future protocol ships.
HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1 = (
    "probiga.trading-v3.independent-horizon-model-artifact.v1"
)
HISTORICAL_HORIZON_SUITE_SCHEMA_V1 = (
    "probiga.trading-v3.independent-horizon-model-suite.v1"
)
CURRENT_HORIZON_ARTIFACT_SCHEMA = (
    "probiga.trading-v3.independent-horizon-model-artifact.v2"
)
CURRENT_HORIZON_SUITE_SCHEMA = (
    "probiga.trading-v3.independent-horizon-model-suite.v2"
)
CURRENT_HORIZON_MODEL_PROTOCOL = (
    "POINT_IN_TIME_INDEPENDENT_NUMPY_REGRESSION_V2"
)
CURRENT_HORIZON_SELECTION_PROTOCOL = (
    "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC_V2"
)
CURRENT_HORIZON_SELECTION_POLICY_HASH = (
    "824721cb771a3d73b4dcad9f7ff69acd300f74f291f2e87c81ad793a74b2d941"
)


# The original 20260804 migration is immutable.  Existing V1 rows receive the
# V1 schema default and NULL protocol projections, which makes them queryable
# as historical audit evidence without making them eligible for new runtime
# contracts, outcomes, or release transitions.
HORIZON_PROTOCOL_V2_DDL: tuple[str, ...] = (
    f"""
    ALTER TABLE st_horizon_model_artifact_v3
        ADD COLUMN artifact_schema_version VARCHAR(96) NOT NULL
            DEFAULT '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1}'
            AFTER artifact_status,
        ADD COLUMN model_protocol VARCHAR(96) NULL
            AFTER artifact_schema_version,
        ADD COLUMN selection_policy_hash CHAR(64) NULL
            AFTER model_protocol,
        ADD CONSTRAINT chk_v3_horizon_model_protocol_projection
            CHECK (
                (
                    artifact_schema_version
                        = '{HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1}'
                    AND model_protocol IS NULL
                    AND selection_policy_hash IS NULL
                )
                OR
                (
                    artifact_schema_version
                        = '{CURRENT_HORIZON_ARTIFACT_SCHEMA}'
                    AND model_protocol
                        = '{CURRENT_HORIZON_MODEL_PROTOCOL}'
                    AND selection_policy_hash REGEXP '^[0-9a-f]{{64}}$'
                )
            )
    """,
    """
    CREATE INDEX idx_v3_horizon_model_current_protocol
    ON st_horizon_model_artifact_v3 (
        artifact_schema_version, artifact_status,
        config_hash, code_version, created_at
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
               OR NEW.artifact_status <> 'BLOCKED'
               OR NEW.training_receipt_status <> 'UNVERIFIED'
               OR NEW.order_authority <> 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 historical horizon protocol is audit-only';
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
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selected_oos_sample_count'
               ) IS NULL
               OR CAST(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selected_oos_sample_count'
               )) AS SIGNED) < 0
               OR JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selected_oos_session_count'
               ) IS NULL
               OR CAST(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.selected_oos_session_count'
               )) AS SIGNED) < 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 current horizon protocol projection differs';
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
               AND a.order_authority = 0;
            IF current_artifact_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibrated contract requires current V2 protocol';
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
               AND a.order_authority = 0;
            IF current_artifact_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibrated outcome requires current V2 protocol';
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
           AND a.order_authority = 0;
        IF current_artifact_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 release requires one current V2 artifact';
        END IF;
        IF NEW.current_stage = 'PAPER_ELIGIBLE'
           AND COALESCE(deployment_domain_verified, 'false') <> 'true' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 diagnostic selection cannot become paper eligible';
        END IF;
    END
    """,
)


_EXPECTED_COLUMNS: Mapping[str, tuple[str, str, str | None]] = {
    "artifact_schema_version": (
        "varchar(96)", "NO", HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    ),
    "model_protocol": ("varchar(96)", "YES", None),
    "selection_policy_hash": ("char(64)", "YES", None),
}

_EXPECTED_TRIGGERS: Mapping[str, tuple[str, ...]] = {
    "trg_v3_horizon_model_protocol_v2_bi": (
        "historical horizon protocol is audit-only",
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        CURRENT_HORIZON_MODEL_PROTOCOL,
        CURRENT_HORIZON_SELECTION_POLICY_HASH,
    ),
    "trg_v3_horizon_contract_protocol_v2_bi": (
        "calibrated contract requires current V2 protocol",
        "training_receipt_status = 'PROCESS_VERIFIED'",
    ),
    "trg_v3_horizon_outcome_protocol_v2_bi": (
        "calibrated outcome requires current V2 protocol",
        "training_receipt_status = 'PROCESS_VERIFIED'",
    ),
    "trg_v3_shadow_release_protocol_v2_bi": (
        "release requires one current V2 artifact",
        "diagnostic selection cannot become paper eligible",
    ),
}


def validate_horizon_protocol_v2_schema(
    connection: Connection,
    *,
    require_triggers: bool = True,
) -> None:
    """Verify the additive V2 protocol migration without weakening V1 DDL."""

    validate_shadow_intelligence_schema(
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
    for name, (column_type, nullable, default) in _EXPECTED_COLUMNS.items():
        row = columns.get(name)
        observed_default = None if row is None else row["COLUMN_DEFAULT"]
        if (
            row is None
            or str(row["COLUMN_TYPE"]).casefold() != column_type
            or str(row["IS_NULLABLE"]).upper() != nullable
            or (
                default is None and observed_default is not None
            )
            or (
                default is not None
                and str(observed_default or "").strip("'\"") != default
            )
        ):
            raise RuntimeError(
                f"V3 horizon protocol V2 column drift: {name}"
            )

    index_rows = connection.execute(text(
        "SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "AND TABLE_NAME = 'st_horizon_model_artifact_v3' "
        "AND INDEX_NAME = 'idx_v3_horizon_model_current_protocol' "
        "ORDER BY SEQ_IN_INDEX"
    )).mappings().all()
    expected_index = (
        "artifact_schema_version", "artifact_status", "config_hash",
        "code_version", "created_at",
    )
    if (
        tuple(str(row["COLUMN_NAME"]) for row in index_rows)
        != expected_index
        or tuple(int(row["SEQ_IN_INDEX"]) for row in index_rows)
        != tuple(range(1, len(expected_index) + 1))
        or any(int(row["NON_UNIQUE"]) != 1 for row in index_rows)
    ):
        raise RuntimeError("V3 horizon protocol V2 index drift")

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
    if any(marker not in normalized_check for marker in (
        "artifact_schema_version",
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
        CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "selection_policy_hash",
    )):
        raise RuntimeError("V3 horizon protocol V2 check drift")

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
                f"V3 horizon protocol V2 trigger drift: {name}"
            )


__all__ = [
    "CURRENT_HORIZON_ARTIFACT_SCHEMA",
    "CURRENT_HORIZON_MODEL_PROTOCOL",
    "CURRENT_HORIZON_SELECTION_POLICY_HASH",
    "CURRENT_HORIZON_SELECTION_PROTOCOL",
    "CURRENT_HORIZON_SUITE_SCHEMA",
    "HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1",
    "HISTORICAL_HORIZON_SUITE_SCHEMA_V1",
    "HORIZON_PROTOCOL_V2_DDL",
    "HORIZON_PROTOCOL_V2_MIGRATION_VERSION",
    "validate_horizon_protocol_v2_schema",
]
