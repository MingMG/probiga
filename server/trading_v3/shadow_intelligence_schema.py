from __future__ import annotations

from collections.abc import Mapping
import re

from sqlalchemy import text
from sqlalchemy.engine import Connection


SHADOW_INTELLIGENCE_MIGRATION_VERSION = (
    "20260804_000_shadow_intelligence_runtime"
)


SHADOW_INTELLIGENCE_DDL: tuple[str, ...] = (
    """
    ALTER TABLE st_decision_run_v3
    ADD COLUMN requested_as_of DATE NULL AFTER trade_date
    """,
    """
    CREATE INDEX idx_v3_decision_requested_asof
    ON st_decision_run_v3 (requested_as_of, decision_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS st_horizon_model_artifact_v3 (
        artifact_id CHAR(64) PRIMARY KEY,
        release_id VARCHAR(160) NOT NULL,
        suite_release_id VARCHAR(160) NOT NULL,
        model_key VARCHAR(80) NOT NULL,
        model_version VARCHAR(80) NOT NULL,
        horizon_days TINYINT UNSIGNED NOT NULL,
        prediction_kind ENUM('CALIBRATED_OOS') NOT NULL,
        artifact_status ENUM('BLOCKED', 'OOS_VERIFIED') NOT NULL,
        training_start DATE,
        training_end DATE,
        validation_start DATE,
        validation_end DATE,
        training_session_count INT UNSIGNED NOT NULL,
        oos_session_count INT UNSIGNED NOT NULL,
        matured_sample_count INT UNSIGNED NOT NULL,
        oos_sample_count INT UNSIGNED NOT NULL,
        walk_forward_fold_count INT UNSIGNED NOT NULL,
        direction_rank_correlation DECIMAL(18,8),
        calibration_mae DECIMAL(18,8),
        brier_score DECIMAL(18,8),
        population_stability_index DECIMAL(18,8),
        net_expectancy_after_cost_pct DECIMAL(18,8),
        profit_factor DECIMAL(18,8),
        cost_coverage_ratio DECIMAL(18,8),
        dataset_hash CHAR(64) NOT NULL,
        feature_protocol_hash CHAR(64) NOT NULL,
        model_artifact_hash CHAR(64) NOT NULL,
        calibration_evidence_hash CHAR(64),
        registration_evidence_hash CHAR(64),
        training_receipt_status ENUM('UNVERIFIED', 'PROCESS_VERIFIED') NOT NULL,
        training_receipt_hash CHAR(64) NOT NULL,
        training_receipt_json JSON NOT NULL,
        config_hash CHAR(64) NOT NULL,
        code_version VARCHAR(80) NOT NULL,
        artifact_json JSON NOT NULL,
        metrics_json JSON NOT NULL,
        block_reasons_json JSON NOT NULL,
        evidence_valid_until DATETIME(6),
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_horizon_model_identity (
            model_key, model_version, horizon_days, model_artifact_hash
        ),
        UNIQUE KEY uk_v3_horizon_model_release (
            release_id
        ),
        UNIQUE KEY uk_v3_horizon_model_artifact_hash (
            model_artifact_hash
        ),
        KEY idx_v3_horizon_model_latest (
            model_key, horizon_days, artifact_status, created_at
        ),
        CONSTRAINT chk_v3_horizon_model_horizon
            CHECK (horizon_days IN (1, 5, 20)),
        CONSTRAINT chk_v3_horizon_model_clock
            CHECK (
                training_session_count > 0
                AND matured_sample_count >= oos_sample_count
                AND (
                    (
                        training_start IS NULL
                        AND training_end IS NULL
                        AND validation_start IS NULL
                        AND validation_end IS NULL
                        AND walk_forward_fold_count = 0
                    )
                    OR (
                        training_start <= training_end
                        AND training_start <= validation_start
                        AND validation_start <= validation_end
                        AND validation_end <= training_end
                        AND walk_forward_fold_count > 0
                    )
                )
            ),
        CONSTRAINT chk_v3_horizon_model_identity
            CHECK (artifact_id = model_artifact_hash),
        CONSTRAINT chk_v3_horizon_model_hashes
            CHECK (
                artifact_id REGEXP '^[0-9a-f]{64}$'
                AND dataset_hash REGEXP '^[0-9a-f]{64}$'
                AND feature_protocol_hash REGEXP '^[0-9a-f]{64}$'
                AND model_artifact_hash REGEXP '^[0-9a-f]{64}$'
                AND config_hash REGEXP '^[0-9a-f]{64}$'
                AND (
                    calibration_evidence_hash IS NULL
                    OR calibration_evidence_hash REGEXP '^[0-9a-f]{64}$'
                )
                AND (
                    registration_evidence_hash IS NULL
                    OR registration_evidence_hash REGEXP '^[0-9a-f]{64}$'
                )
                AND training_receipt_hash REGEXP '^[0-9a-f]{64}$'
            ),
        CONSTRAINT chk_v3_horizon_model_receipt
            CHECK (
                artifact_status <> 'OOS_VERIFIED'
                OR training_receipt_status = 'PROCESS_VERIFIED'
            ),
        CONSTRAINT chk_v3_horizon_model_verified_claim
            CHECK (
                artifact_status <> 'OOS_VERIFIED'
                OR (
                    calibration_evidence_hash IS NOT NULL
                    AND training_receipt_status = 'PROCESS_VERIFIED'
                    AND training_receipt_hash IS NOT NULL
                    AND evidence_valid_until IS NOT NULL
                    AND walk_forward_fold_count >= 3
                    AND direction_rank_correlation IS NOT NULL
                    AND calibration_mae IS NOT NULL
                    AND brier_score IS NOT NULL
                    AND population_stability_index IS NOT NULL
                    AND net_expectancy_after_cost_pct IS NOT NULL
                    AND profit_factor IS NOT NULL
                    AND cost_coverage_ratio IS NOT NULL
                )
            ),
        CONSTRAINT chk_v3_horizon_model_no_order
            CHECK (order_authority = 0)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_horizon_forecast_contract_v3 (
        contract_id CHAR(64) PRIMARY KEY,
        source_forecast_id VARCHAR(64) NOT NULL,
        run_uid VARCHAR(64) NOT NULL,
        stock_code VARCHAR(16) NOT NULL,
        model_key VARCHAR(80) NOT NULL,
        model_version VARCHAR(80) NOT NULL,
        source_strategy_key VARCHAR(64) NOT NULL,
        source_forecast_hash CHAR(64) NOT NULL,
        source_evidence_json JSON NOT NULL,
        decision_result_hash CHAR(64) NOT NULL,
        feature_protocol_hash CHAR(64) NOT NULL,
        model_artifact_hash CHAR(64) NOT NULL,
        model_inputs_json JSON NOT NULL,
        selection_status ENUM('SELECTED', 'REJECTED') NOT NULL,
        selection_reason_code VARCHAR(96) NOT NULL,
        selection_evidence_hash CHAR(64) NOT NULL,
        selection_evidence_json JSON NOT NULL,
        horizon_days TINYINT UNSIGNED NOT NULL,
        prediction_kind ENUM('PROXY_SCORE', 'CALIBRATED_OOS') NOT NULL,
        decision_as_of DATETIME(6) NOT NULL,
        feature_as_of DATETIME(6) NOT NULL,
        decision_session_date DATE NOT NULL,
        entry_trade_date DATE NOT NULL,
        earliest_exit_trade_date DATE NOT NULL,
        outcome_matures_on DATE NOT NULL,
        entry_session_sequence INT UNSIGNED NOT NULL,
        earliest_exit_session_sequence INT UNSIGNED NOT NULL,
        outcome_maturity_session_sequence INT UNSIGNED NOT NULL,
        score DECIMAL(18,8) NOT NULL,
        expected_return_net_pct DECIMAL(18,8),
        probability_positive DECIMAL(18,8),
        cost_assumption_pct DECIMAL(18,8) NOT NULL,
        cost_model_version VARCHAR(80) NOT NULL,
        calibration_evidence_hash CHAR(64),
        contract_hash CHAR(64) NOT NULL,
        contract_json JSON NOT NULL,
        decision_scope ENUM('RESEARCH_ONLY') NOT NULL DEFAULT 'RESEARCH_ONLY',
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_horizon_source_model (
            source_forecast_id, model_key, model_version,
            horizon_days, model_artifact_hash
        ),
        KEY idx_v3_horizon_maturity (outcome_matures_on, horizon_days),
        KEY idx_v3_horizon_release (
            model_key, model_version, horizon_days, decision_as_of
        ),
        KEY idx_v3_horizon_source_snapshot (
            source_strategy_key, selection_status, decision_session_date
        ),
        CONSTRAINT chk_v3_horizon_supported
            CHECK (horizon_days IN (1, 5, 20)),
        CONSTRAINT chk_v3_horizon_clock
            CHECK (
                feature_as_of <= decision_as_of
                AND entry_trade_date > decision_session_date
                AND earliest_exit_trade_date > entry_trade_date
                AND outcome_matures_on >= earliest_exit_trade_date
                AND entry_session_sequence > 0
                AND earliest_exit_session_sequence
                    = entry_session_sequence + 1
                AND outcome_maturity_session_sequence
                    = entry_session_sequence + horizon_days
            ),
        CONSTRAINT chk_v3_horizon_proxy_claims
            CHECK (
                (prediction_kind = 'PROXY_SCORE'
                 AND expected_return_net_pct IS NULL
                 AND probability_positive IS NULL
                 AND calibration_evidence_hash IS NULL)
                OR
                (prediction_kind = 'CALIBRATED_OOS'
                 AND expected_return_net_pct IS NOT NULL
                 AND probability_positive BETWEEN 0 AND 1
                 AND calibration_evidence_hash IS NOT NULL)
            ),
        CONSTRAINT chk_v3_horizon_no_order
            CHECK (
                decision_scope = 'RESEARCH_ONLY'
                AND order_authority = 0
            )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_horizon_outcome_v3 (
        outcome_id CHAR(64) PRIMARY KEY,
        contract_id CHAR(64) NOT NULL,
        contract_hash CHAR(64) NOT NULL,
        stock_code VARCHAR(16) NOT NULL,
        horizon_days TINYINT UNSIGNED NOT NULL,
        entry_trade_date DATE NOT NULL,
        exit_trade_date DATE NOT NULL,
        entry_price DECIMAL(20,8) NOT NULL,
        exit_price DECIMAL(20,8) NOT NULL,
        gross_return_pct DECIMAL(18,8) NOT NULL,
        realized_cost_pct DECIMAL(18,8) NOT NULL,
        realized_net_return_pct DECIMAL(18,8) NOT NULL,
        realized_mae_pct DECIMAL(18,8) NOT NULL,
        realized_mfe_pct DECIMAL(18,8) NOT NULL,
        bar_count INT UNSIGNED NOT NULL,
        cost_model_version VARCHAR(80) NOT NULL,
        market_data_source VARCHAR(120) NOT NULL,
        market_evidence_hash CHAR(64) NOT NULL,
        market_evidence_json JSON NOT NULL,
        execution_feasibility ENUM(
            'UNVERIFIED_RESEARCH', 'EXECUTABLE_VERIFIED'
        ) NOT NULL,
        outcome_hash CHAR(64) NOT NULL,
        outcome_status ENUM('MATURED_VERIFIED', 'QUARANTINED') NOT NULL,
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        observed_at DATETIME(6) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_horizon_outcome_contract (contract_id),
        UNIQUE KEY uk_v3_horizon_outcome_hash (outcome_hash),
        KEY idx_v3_horizon_outcome_learning (
            horizon_days, exit_trade_date, outcome_status
        ),
        CONSTRAINT fk_v3_horizon_outcome_contract
            FOREIGN KEY (contract_id)
            REFERENCES st_horizon_forecast_contract_v3 (contract_id),
        CONSTRAINT chk_v3_horizon_outcome_supported
            CHECK (horizon_days IN (1, 5, 20)),
        CONSTRAINT chk_v3_horizon_outcome_clock
            CHECK (
                exit_trade_date > entry_trade_date
                AND bar_count = horizon_days + 1
            ),
        CONSTRAINT chk_v3_horizon_outcome_prices
            CHECK (
                entry_price > 0
                AND exit_price > 0
                AND realized_cost_pct >= 0
            ),
        CONSTRAINT chk_v3_horizon_outcome_no_order
            CHECK (order_authority = 0),
        CONSTRAINT chk_v3_horizon_outcome_research_execution
            CHECK (execution_feasibility = 'UNVERIFIED_RESEARCH')
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_shadow_release_v3 (
        release_state_id CHAR(64) PRIMARY KEY,
        release_id VARCHAR(160) NOT NULL,
        transition_sequence BIGINT UNSIGNED NOT NULL,
        model_key VARCHAR(80) NOT NULL,
        model_version VARCHAR(80) NOT NULL,
        horizon_days TINYINT UNSIGNED NOT NULL,
        previous_stage ENUM(
            'DRAFT', 'SHADOW', 'CALIBRATION_REVIEW',
            'PAPER_ELIGIBLE', 'BLOCKED', 'RETIRED'
        ) NOT NULL,
        current_stage ENUM(
            'DRAFT', 'SHADOW', 'CALIBRATION_REVIEW',
            'PAPER_ELIGIBLE', 'BLOCKED', 'RETIRED'
        ) NOT NULL,
        release_event VARCHAR(64) NOT NULL,
        transition_accepted TINYINT(1) NOT NULL,
        reason_code VARCHAR(96) NOT NULL,
        gate_evaluation_id CHAR(64),
        evidence_hash CHAR(64) NOT NULL,
        config_hash CHAR(64) NOT NULL,
        transition_hash CHAR(64) NOT NULL,
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        occurred_at DATETIME(6) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_shadow_release_sequence (
            release_id, transition_sequence
        ),
        UNIQUE KEY uk_v3_shadow_release_transition (
            release_id, transition_hash
        ),
        KEY idx_v3_shadow_release_latest (
            model_key, horizon_days, transition_sequence
        ),
        CONSTRAINT chk_v3_shadow_release_horizon
            CHECK (horizon_days IN (1, 5, 20)),
        CONSTRAINT chk_v3_shadow_release_no_order
            CHECK (order_authority = 0),
        CONSTRAINT chk_v3_shadow_release_rejected_stays
            CHECK (
                transition_accepted = 1
                OR current_stage = previous_stage
            )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_calibration_gate_v3 (
        gate_evaluation_id CHAR(64) PRIMARY KEY,
        release_id VARCHAR(160) NOT NULL,
        model_key VARCHAR(80) NOT NULL,
        model_version VARCHAR(80) NOT NULL,
        horizon_days TINYINT UNSIGNED NOT NULL,
        prediction_kind ENUM('PROXY_SCORE', 'CALIBRATED_OOS') NOT NULL,
        gate_status ENUM('PASS', 'BLOCK') NOT NULL,
        recommended_stage ENUM('PAPER_ELIGIBLE', 'BLOCKED') NOT NULL,
        learning_run_id CHAR(64),
        evidence_provenance_status ENUM(
            'PERSISTED_VERIFIED', 'UNVERIFIED_PREVIEW'
        ) NOT NULL,
        matured_sample_count INT UNSIGNED,
        oos_sample_count INT UNSIGNED,
        walk_forward_fold_count INT UNSIGNED,
        direction_rank_correlation DECIMAL(18,8),
        calibration_mae DECIMAL(18,8),
        brier_score DECIMAL(18,8),
        population_stability_index DECIMAL(18,8),
        net_expectancy_after_cost_pct DECIMAL(18,8),
        profit_factor DECIMAL(18,8),
        cost_coverage_ratio DECIMAL(18,8),
        evidence_observed_at DATETIME(6),
        evidence_valid_until DATETIME(6),
        failure_codes_json JSON NOT NULL,
        evidence_json JSON NOT NULL,
        evidence_hash CHAR(64) NOT NULL,
        policy_hash CHAR(64) NOT NULL,
        config_hash CHAR(64) NOT NULL,
        code_version VARCHAR(80) NOT NULL,
        model_artifact_hash CHAR(64) NOT NULL,
        gate_result_hash CHAR(64) NOT NULL,
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        evaluated_at DATETIME(6) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_calibration_gate_evidence (
            release_id, evidence_hash, policy_hash, evaluated_at
        ),
        KEY idx_v3_calibration_gate_latest (
            release_id, evaluated_at, gate_status
        ),
        CONSTRAINT chk_v3_calibration_gate_horizon
            CHECK (horizon_days IN (1, 5, 20)),
        CONSTRAINT chk_v3_calibration_gate_no_order
            CHECK (order_authority = 0),
        CONSTRAINT chk_v3_calibration_gate_stage
            CHECK (
                (gate_status = 'PASS'
                 AND recommended_stage = 'PAPER_ELIGIBLE'
                 AND evidence_provenance_status = 'PERSISTED_VERIFIED'
                 AND learning_run_id IS NOT NULL)
                OR
                (gate_status = 'BLOCK'
                 AND recommended_stage = 'BLOCKED')
            ),
        CONSTRAINT chk_v3_calibration_gate_provenance
            CHECK (
                (evidence_provenance_status = 'PERSISTED_VERIFIED'
                 AND learning_run_id IS NOT NULL)
                OR
                (evidence_provenance_status = 'UNVERIFIED_PREVIEW'
                 AND learning_run_id IS NULL)
            )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_counterfactual_learning_run_v3 (
        learning_run_id CHAR(64) PRIMARY KEY,
        evaluation_date DATE NOT NULL,
        learning_status ENUM('COLLECTING', 'EVIDENCE_READY') NOT NULL,
        sample_count INT UNSIGNED NOT NULL,
        selected_win_count INT UNSIGNED NOT NULL,
        selected_loss_count INT UNSIGNED NOT NULL,
        rejected_win_count INT UNSIGNED NOT NULL,
        rejected_correct_count INT UNSIGNED NOT NULL,
        selection_precision DECIMAL(18,8),
        winner_recall DECIMAL(18,8),
        mean_absolute_forecast_error_pct DECIMAL(18,8),
        mean_brier_score DECIMAL(18,8),
        total_opportunity_cost_pct DECIMAL(18,8) NOT NULL,
        t1_sample_count INT UNSIGNED NOT NULL,
        t5_sample_count INT UNSIGNED NOT NULL,
        t20_sample_count INT UNSIGNED NOT NULL,
        t1_evidence_ready TINYINT(1) NOT NULL,
        t5_evidence_ready TINYINT(1) NOT NULL,
        t20_evidence_ready TINYINT(1) NOT NULL,
        evidence_source ENUM(
            'HORIZON_CONTRACT_OUTCOME_LEDGER'
        ) NOT NULL,
        metrics_json JSON NOT NULL,
        evidence_hash CHAR(64) NOT NULL,
        policy_hash CHAR(64) NOT NULL,
        config_hash CHAR(64) NOT NULL,
        code_version VARCHAR(80) NOT NULL,
        code_version_kind VARCHAR(40) NOT NULL,
        model_artifact_hashes_json JSON NOT NULL,
        provenance_hash CHAR(64) NOT NULL,
        learning_result_hash CHAR(64) NOT NULL,
        can_activate_model TINYINT(1) NOT NULL DEFAULT 0,
        order_authority TINYINT(1) NOT NULL DEFAULT 0,
        evaluated_at DATETIME(6) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_learning_evidence (
            evidence_hash, policy_hash, evaluation_date, provenance_hash
        ),
        KEY idx_v3_learning_latest (evaluation_date, evaluated_at),
        CONSTRAINT chk_v3_learning_no_activation
            CHECK (can_activate_model = 0 AND order_authority = 0),
        CONSTRAINT chk_v3_learning_counts
            CHECK (
                sample_count = selected_win_count
                    + selected_loss_count
                    + rejected_win_count
                    + rejected_correct_count
                AND sample_count = t1_sample_count
                    + t5_sample_count + t20_sample_count
                AND (
                    learning_status <> 'EVIDENCE_READY'
                    OR (
                        t1_evidence_ready = 1
                        AND t5_evidence_ready = 1
                        AND t20_evidence_ready = 1
                    )
                )
            )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    ALTER TABLE st_calibration_gate_v3
    ADD CONSTRAINT fk_v3_calibration_gate_learning_run
        FOREIGN KEY (learning_run_id)
        REFERENCES st_counterfactual_learning_run_v3 (learning_run_id)
    """,
    """
    ALTER TABLE st_shadow_release_v3
    ADD CONSTRAINT fk_v3_shadow_release_gate
        FOREIGN KEY (gate_evaluation_id)
        REFERENCES st_calibration_gate_v3 (gate_evaluation_id)
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_model_guard_bi",
    """
    CREATE TRIGGER trg_v3_horizon_model_guard_bi
    BEFORE INSERT ON st_horizon_model_artifact_v3
    FOR EACH ROW
    BEGIN
        DECLARE artifact_fold_count INT UNSIGNED DEFAULT 0;
        SET artifact_fold_count = COALESCE(
            JSON_LENGTH(JSON_EXTRACT(
                NEW.artifact_json, '$.walk_forward.folds'
            )),
            0
        );
        IF COALESCE(JSON_TYPE(NEW.artifact_json), '') <> 'OBJECT'
           OR COALESCE(JSON_TYPE(NEW.metrics_json), '') <> 'OBJECT'
           OR COALESCE(JSON_TYPE(NEW.block_reasons_json), '') <> 'ARRAY'
           OR COALESCE(JSON_TYPE(NEW.training_receipt_json), '') <> 'OBJECT'
           OR COALESCE(JSON_TYPE(JSON_EXTRACT(
                NEW.artifact_json, '$.gate.block_reasons'
           )), '') <> 'ARRAY' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon model artifact JSON shape is invalid';
        END IF;
        IF NEW.order_authority <> 0
           OR NEW.artifact_id <> NEW.model_artifact_hash THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon model artifact identity/order boundary is invalid';
        END IF;
        IF NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.artifact_hash'
            )) <=> NEW.model_artifact_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.release_id'
            )) <=> NEW.release_id)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.suite_release_id'
            )) <=> NEW.suite_release_id)
           OR NOT (NEW.release_id <=> CONCAT(
                NEW.suite_release_id, CHAR(58), NEW.model_key, CHAR(58),
                NEW.model_version, CHAR(58), 'T+',
                CAST(NEW.horizon_days AS CHAR)
            ))
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.model_key'
            )) <=> NEW.model_key)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.model_version'
            )) <=> NEW.model_version)
           OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.horizon_days'
            )) AS UNSIGNED) <=> NEW.horizon_days)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.prediction_kind'
            )) <=> NEW.prediction_kind)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.dataset_hash'
            )) <=> NEW.dataset_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.feature_protocol_hash'
            )) <=> NEW.feature_protocol_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.oos_evidence_hash'
            )) <=> NEW.calibration_evidence_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.config_hash'
            )) <=> NEW.config_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.code_version'
            )) <=> NEW.code_version)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.training_receipt_json, '$.schema_version'
            )) <=> 'probiga.trading-v3.process-training-receipt.v1')
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.training_receipt_json, '$.status'
            )) <=> NEW.training_receipt_status)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.training_receipt_json, '$.receipt_hash'
            )) <=> NEW.training_receipt_hash)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.training_receipt_json, '$.suite_release_id'
            )) <=> NEW.suite_release_id)
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.lifecycle'
            )) <=> 'SHADOW_RESEARCH_ONLY')
           OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json, '$.order_authority'
            )) <=> 'false')
           OR artifact_fold_count <> NEW.walk_forward_fold_count
           OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.oos_evidence.distinct_train_sessions'
            )) AS UNSIGNED) <=> NEW.training_session_count)
           OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.oos_evidence.distinct_oos_sessions'
            )) AS UNSIGNED) <=> NEW.oos_session_count)
           OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.oos_evidence.matured_sample_count'
            )) AS UNSIGNED) <=> NEW.matured_sample_count)
           OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.oos_evidence.oos_sample_count'
            )) AS UNSIGNED) <=> NEW.oos_sample_count)
           OR NOT (NEW.direction_rank_correlation <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.direction_rank_correlation'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.calibration_mae <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.oos_evidence.calibration_mae'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.brier_score <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.oos_evidence.brier_score'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.population_stability_index <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.population_stability_index'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.net_expectancy_after_cost_pct <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.net_expectancy_after_cost_pct'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.profit_factor <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.oos_evidence.profit_factor'
                )) AS DECIMAL(18,8)
            ))
           OR NOT (NEW.cost_coverage_ratio <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json,
                    '$.oos_evidence.cost_coverage_ratio'
                )) AS DECIMAL(18,8)
            ))
           OR COALESCE(JSON_CONTAINS(
                NEW.metrics_json,
                JSON_EXTRACT(NEW.artifact_json, '$.oos_evidence')
            ), 0) <> 1
           OR COALESCE(JSON_CONTAINS(
                JSON_EXTRACT(NEW.artifact_json, '$.oos_evidence'),
                NEW.metrics_json
            ), 0) <> 1
           OR COALESCE(JSON_CONTAINS(
                NEW.block_reasons_json,
                JSON_EXTRACT(NEW.artifact_json, '$.gate.block_reasons')
            ), 0) <> 1
           OR NOT (DATE(NEW.evidence_valid_until) <=> CAST(
                JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.valid_until'
                )) AS DATE
            )) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon model artifact projection differs from verified JSON';
        END IF;
        IF artifact_fold_count = 0 THEN
            IF NEW.training_start IS NOT NULL
               OR NEW.training_end IS NOT NULL
               OR NEW.validation_start IS NOT NULL
               OR NEW.validation_end IS NOT NULL THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 blocked model without folds must not invent validation dates';
            END IF;
        ELSEIF NOT (
            NEW.training_start <=> CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.final_model.training_start'
            )) AS DATE)
            AND NEW.training_end <=> CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.final_model.training_end'
            )) AS DATE)
            AND NEW.validation_start <=> CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                '$.walk_forward.folds[0].validation_start'
            )) AS DATE)
            AND NEW.validation_end <=> CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.artifact_json,
                CONCAT(
                    '$.walk_forward.folds[', artifact_fold_count - 1,
                    '].validation_end'
                )
            )) AS DATE)
        ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon model artifact fold clock projection differs';
        END IF;
        IF NEW.training_receipt_status = 'PROCESS_VERIFIED' THEN
            IF COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.process_nonce'
                )), '') NOT REGEXP '^[0-9a-f]{64}$'
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json,
                    CONCAT(
                        '$.artifact_hashes."',
                        CAST(NEW.horizon_days AS CHAR), '"'
                    )
                )) <=> NEW.model_artifact_hash)
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.config_hash'
                )) <=> NEW.config_hash)
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.code_version'
                )) <=> NEW.code_version)
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.artifact_code_hash'
                )) <=> JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.code_hash'
                )))
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.training_cutoff'
                )) <=> JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.training_cutoff'
                )))
               OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.exit_code'
                )) AS SIGNED) <=> 0)
               OR COALESCE(JSON_CONTAINS(
                    JSON_EXTRACT(NEW.artifact_json, '$.gate.block_reasons'),
                    NEW.block_reasons_json
                ), 0) <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 process training receipt projection differs';
            END IF;
        ELSEIF NEW.training_receipt_status = 'UNVERIFIED' THEN
            IF NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.reason'
                )) <=> 'TRAINING_RECEIPT_UNVERIFIED')
               OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.training_receipt_json, '$.artifact_hash'
                )) <=> NEW.model_artifact_hash)
               OR COALESCE(JSON_CONTAINS(
                    NEW.block_reasons_json,
                    JSON_QUOTE('TRAINING_RECEIPT_UNVERIFIED')
                ), 0) <> 1
               OR COALESCE(JSON_LENGTH(NEW.block_reasons_json), -1)
                    <> COALESCE(JSON_LENGTH(JSON_EXTRACT(
                        NEW.artifact_json, '$.gate.block_reasons'
                    )), -2) + 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 unverified training receipt must remain blocked';
            END IF;
        ELSE
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 training receipt status is invalid';
        END IF;
        IF NOT (
            (
                NEW.artifact_status = 'OOS_VERIFIED'
                AND NEW.training_receipt_status = 'PROCESS_VERIFIED'
                AND (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.gate.status'
                )) <=> 'PASS')
                AND (JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.artifact_json, '$.contract_eligible'
                )) <=> 'true')
                AND COALESCE(JSON_LENGTH(NEW.block_reasons_json), -1) = 0
            )
            OR (
                NEW.artifact_status = 'BLOCKED'
                AND (
                    (
                        (JSON_UNQUOTE(JSON_EXTRACT(
                            NEW.artifact_json, '$.gate.status'
                        )) <=> 'BLOCK')
                        AND (JSON_UNQUOTE(JSON_EXTRACT(
                            NEW.artifact_json, '$.contract_eligible'
                        )) <=> 'false')
                    )
                    OR NEW.training_receipt_status = 'UNVERIFIED'
                )
                AND COALESCE(JSON_LENGTH(NEW.block_reasons_json), 0) > 0
            )
        ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon model artifact gate projection differs';
        END IF;
        IF NEW.artifact_status = 'OOS_VERIFIED' AND NOT (
            NEW.prediction_kind = 'CALIBRATED_OOS'
            AND NEW.training_receipt_status = 'PROCESS_VERIFIED'
            AND NEW.training_receipt_hash REGEXP '^[0-9a-f]{64}$'
            AND NEW.training_start IS NOT NULL
            AND NEW.training_end IS NOT NULL
            AND NEW.validation_start IS NOT NULL
            AND NEW.validation_end IS NOT NULL
            AND NEW.training_session_count >= CASE NEW.horizon_days
                WHEN 1 THEN 120 WHEN 5 THEN 160 WHEN 20 THEN 240
                ELSE 999999999 END
            AND NEW.oos_session_count >= CASE NEW.horizon_days
                WHEN 1 THEN 40 WHEN 5 THEN 50 WHEN 20 THEN 80
                ELSE 999999999 END
            AND NEW.matured_sample_count >= CASE NEW.horizon_days
                WHEN 1 THEN 160 WHEN 5 THEN 120 WHEN 20 THEN 80
                ELSE 999999999 END
            AND NEW.oos_sample_count >= CASE NEW.horizon_days
                WHEN 1 THEN 100 WHEN 5 THEN 100 WHEN 20 THEN 80
                ELSE 999999999 END
            AND NEW.walk_forward_fold_count >= 3
            AND NEW.direction_rank_correlation >= 0.05
            AND NEW.calibration_mae <= 0.15
            AND NEW.brier_score <= 0.24
            AND NEW.population_stability_index <= 0.20
            AND NEW.net_expectancy_after_cost_pct > 0
            AND NEW.profit_factor >= 1.30
            AND NEW.cost_coverage_ratio >= 1.0
            AND NEW.calibration_evidence_hash IS NOT NULL
            AND NEW.evidence_valid_until > NEW.created_at
        ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 OOS model artifact does not satisfy frozen evidence policy';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_model_immutable_bu",
    """
    CREATE TRIGGER trg_v3_horizon_model_immutable_bu
    BEFORE UPDATE ON st_horizon_model_artifact_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon model artifact is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_model_immutable_bd",
    """
    CREATE TRIGGER trg_v3_horizon_model_immutable_bd
    BEFORE DELETE ON st_horizon_model_artifact_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon model artifact cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_contract_guard_bi",
    """
    CREATE TRIGGER trg_v3_horizon_contract_guard_bi
    BEFORE INSERT ON st_horizon_forecast_contract_v3
    FOR EACH ROW
    BEGIN
        DECLARE source_count BIGINT DEFAULT 0;
        DECLARE artifact_count BIGINT DEFAULT 0;
        SELECT COUNT(*)
          INTO source_count
          FROM st_alpha_forecast_v3 f
          JOIN st_decision_run_v3 r ON r.run_uid = f.run_uid
         WHERE f.forecast_id = NEW.source_forecast_id
           AND f.run_uid = NEW.run_uid
           AND f.stock_code = NEW.stock_code
           AND f.strategy_key = NEW.source_strategy_key
           AND r.status = 'COMPLETED'
           AND r.result_hash = NEW.decision_result_hash;
        IF source_count <> 1 OR NEW.order_authority <> 0 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon contract requires a completed immutable source';
        END IF;
        IF NEW.prediction_kind = 'CALIBRATED_OOS' THEN
            SELECT COUNT(*)
              INTO artifact_count
              FROM st_horizon_model_artifact_v3 a
              JOIN st_decision_run_v3 r ON r.run_uid = NEW.run_uid
             WHERE a.model_key = NEW.model_key
               AND a.model_version = NEW.model_version
               AND a.horizon_days = NEW.horizon_days
               AND a.artifact_status = 'OOS_VERIFIED'
               AND a.model_artifact_hash = NEW.model_artifact_hash
               AND a.feature_protocol_hash = NEW.feature_protocol_hash
               AND a.calibration_evidence_hash
                   = NEW.calibration_evidence_hash
               AND a.config_hash = r.config_hash
               AND a.created_at <= NEW.decision_as_of
               AND a.evidence_valid_until >= NEW.decision_as_of
               AND a.order_authority = 0;
            IF artifact_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibrated contract requires a current verified OOS artifact';
            END IF;
        ELSEIF NEW.prediction_kind <> 'PROXY_SCORE' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon contract prediction kind is invalid';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_contract_immutable_bu",
    """
    CREATE TRIGGER trg_v3_horizon_contract_immutable_bu
    BEFORE UPDATE ON st_horizon_forecast_contract_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon forecast contract is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_contract_immutable_bd",
    """
    CREATE TRIGGER trg_v3_horizon_contract_immutable_bd
    BEFORE DELETE ON st_horizon_forecast_contract_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon forecast contract cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_outcome_guard_bi",
    """
    CREATE TRIGGER trg_v3_horizon_outcome_guard_bi
    BEFORE INSERT ON st_horizon_outcome_v3
    FOR EACH ROW
    BEGIN
        DECLARE contract_count BIGINT DEFAULT 0;
        DECLARE calibrated_artifact_count BIGINT DEFAULT 0;
        SELECT COUNT(*)
          INTO contract_count
          FROM st_horizon_forecast_contract_v3 h
         WHERE h.contract_id = NEW.contract_id
           AND h.contract_hash = NEW.contract_hash
           AND h.stock_code = NEW.stock_code
           AND h.horizon_days = NEW.horizon_days
           AND h.entry_trade_date = NEW.entry_trade_date
           AND h.outcome_matures_on = NEW.exit_trade_date
           AND h.cost_model_version = NEW.cost_model_version
           AND h.cost_assumption_pct = NEW.realized_cost_pct
           AND h.order_authority = 0;
        IF contract_count <> 1
           OR NEW.order_authority <> 0
           OR NEW.execution_feasibility <> 'UNVERIFIED_RESEARCH' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 horizon outcome requires a frozen research contract and remains unverified research';
        END IF;
        SELECT COUNT(*)
          INTO calibrated_artifact_count
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
           AND h.prediction_kind = 'CALIBRATED_OOS'
           AND a.artifact_status = 'OOS_VERIFIED'
           AND a.config_hash = r.config_hash
           AND a.created_at <= h.decision_as_of
           AND a.evidence_valid_until >= h.decision_as_of
           AND a.order_authority = 0;
        IF EXISTS (
            SELECT 1
              FROM st_horizon_forecast_contract_v3 h
             WHERE h.contract_id = NEW.contract_id
               AND h.prediction_kind = 'CALIBRATED_OOS'
        ) AND calibrated_artifact_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 calibrated outcome requires its current verified OOS artifact';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_outcome_immutable_bu",
    """
    CREATE TRIGGER trg_v3_horizon_outcome_immutable_bu
    BEFORE UPDATE ON st_horizon_outcome_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon outcome evidence is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_horizon_outcome_immutable_bd",
    """
    CREATE TRIGGER trg_v3_horizon_outcome_immutable_bd
    BEFORE DELETE ON st_horizon_outcome_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 horizon outcome evidence cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_learning_run_guard_bi",
    """
    CREATE TRIGGER trg_v3_learning_run_guard_bi
    BEFORE INSERT ON st_counterfactual_learning_run_v3
    FOR EACH ROW
    BEGIN
        IF NEW.order_authority <> 0
           OR NEW.can_activate_model <> 0
           OR NEW.evidence_source
               <> 'HORIZON_CONTRACT_OUTCOME_LEDGER'
           OR NEW.sample_count <> NEW.t1_sample_count
               + NEW.t5_sample_count + NEW.t20_sample_count
           OR (
               NEW.learning_status = 'EVIDENCE_READY'
               AND NOT (
                   NEW.t1_evidence_ready = 1
                   AND NEW.t5_evidence_ready = 1
                   AND NEW.t20_evidence_ready = 1
                   AND NEW.t1_sample_count >= 160
                   AND NEW.t5_sample_count >= 120
                   AND NEW.t20_sample_count >= 80
               )
           ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 learning run provenance/readiness is invalid';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_calibration_gate_guard_bi",
    """
    CREATE TRIGGER trg_v3_calibration_gate_guard_bi
    BEFORE INSERT ON st_calibration_gate_v3
    FOR EACH ROW
    BEGIN
        DECLARE verified_count BIGINT DEFAULT 0;
        IF NEW.order_authority <> 0 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 calibration gate cannot grant order authority';
        END IF;
        IF NEW.gate_status = 'PASS' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 persisted PASS requires an external signed attestation pipeline';
        END IF;
        IF NEW.gate_status = 'PASS' AND NOT (
            NEW.prediction_kind = 'CALIBRATED_OOS'
            AND NEW.matured_sample_count >= CASE NEW.horizon_days
                WHEN 1 THEN 160 WHEN 5 THEN 120 WHEN 20 THEN 80
                ELSE 999999999 END
            AND NEW.oos_sample_count >= CASE NEW.horizon_days
                WHEN 1 THEN 100 WHEN 5 THEN 100 WHEN 20 THEN 80
                ELSE 999999999 END
            AND NEW.walk_forward_fold_count >= 3
            AND NEW.direction_rank_correlation >= 0.05
            AND NEW.calibration_mae <= 0.15
            AND NEW.brier_score <= 0.24
            AND NEW.population_stability_index <= 0.20
            AND NEW.net_expectancy_after_cost_pct > 0
            AND NEW.profit_factor >= 1.30
            AND NEW.cost_coverage_ratio >= 1.0
            AND NEW.evidence_observed_at <= NEW.evaluated_at
            AND NEW.evidence_valid_until >= NEW.evaluated_at
        ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 calibration PASS does not satisfy frozen policy';
        END IF;
        IF NEW.evidence_provenance_status = 'PERSISTED_VERIFIED' THEN
            SELECT COUNT(*)
              INTO verified_count
              FROM st_counterfactual_learning_run_v3 l
             WHERE l.learning_run_id = NEW.learning_run_id
               AND l.learning_status = 'EVIDENCE_READY'
               AND l.evidence_source
                   = 'HORIZON_CONTRACT_OUTCOME_LEDGER'
               AND l.config_hash = NEW.config_hash
               AND l.code_version = NEW.code_version
               AND l.policy_hash = NEW.policy_hash
               AND JSON_SEARCH(
                   l.model_artifact_hashes_json,
                   'one', NEW.model_artifact_hash
               ) IS NOT NULL
               AND JSON_UNQUOTE(JSON_EXTRACT(
                   NEW.evidence_json, '$.learning_result_hash'
               )) = l.learning_result_hash
               AND JSON_UNQUOTE(JSON_EXTRACT(
                   NEW.evidence_json, '$.learning_evidence_hash'
               )) = l.evidence_hash
               AND JSON_UNQUOTE(JSON_EXTRACT(
                   NEW.evidence_json, '$.learning_policy_hash'
               )) = l.policy_hash
               AND CASE NEW.horizon_days
                   WHEN 1 THEN l.t1_evidence_ready
                   WHEN 5 THEN l.t5_evidence_ready
                   WHEN 20 THEN l.t20_evidence_ready
                   ELSE 0
               END = 1;
            IF verified_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 calibration gate learning provenance is invalid';
            END IF;
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_shadow_release_guard_bi",
    """
    CREATE TRIGGER trg_v3_shadow_release_guard_bi
    BEFORE INSERT ON st_shadow_release_v3
    FOR EACH ROW
    BEGIN
        DECLARE latest_sequence BIGINT DEFAULT NULL;
        DECLARE latest_stage VARCHAR(32) DEFAULT NULL;
        DECLARE valid_gate_count BIGINT DEFAULT 0;

        IF NEW.order_authority <> 0 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'V3 shadow release cannot grant order authority';
        END IF;
        SELECT MAX(transition_sequence)
          INTO latest_sequence
          FROM st_shadow_release_v3
         WHERE release_id = NEW.release_id;
        IF latest_sequence IS NULL THEN
            IF NEW.transition_sequence <> 0
               OR NEW.previous_stage <> 'DRAFT'
               OR NEW.current_stage <> 'DRAFT'
               OR NEW.release_event <> 'INITIALIZE' THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 shadow release must initialize at DRAFT';
            END IF;
        ELSE
            SELECT current_stage
              INTO latest_stage
              FROM st_shadow_release_v3
             WHERE release_id = NEW.release_id
               AND transition_sequence = latest_sequence;
            IF NEW.transition_sequence <> latest_sequence + 1
               OR NEW.previous_stage <> latest_stage THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 shadow release sequence is not contiguous';
            END IF;
            IF NEW.transition_accepted = 0
               AND NEW.current_stage <> NEW.previous_stage THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 rejected release event must keep its stage';
            END IF;
            IF NEW.transition_accepted = 1
               AND NOT (
                   (NEW.previous_stage = NEW.current_stage)
                   OR (NEW.previous_stage = 'DRAFT'
                       AND NEW.current_stage = 'SHADOW')
                   OR (NEW.previous_stage = 'SHADOW'
                       AND NEW.current_stage IN (
                           'CALIBRATION_REVIEW', 'BLOCKED', 'RETIRED'
                       ))
                   OR (NEW.previous_stage = 'CALIBRATION_REVIEW'
                       AND NEW.current_stage IN (
                           'PAPER_ELIGIBLE', 'BLOCKED', 'RETIRED'
                       ))
                   OR (NEW.previous_stage = 'PAPER_ELIGIBLE'
                       AND NEW.current_stage IN ('BLOCKED', 'RETIRED'))
                   OR (NEW.previous_stage = 'BLOCKED'
                       AND NEW.current_stage IN ('SHADOW', 'RETIRED'))
               ) THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 shadow release transition is illegal';
            END IF;
            IF NEW.current_stage = 'PAPER_ELIGIBLE' THEN
                SELECT COUNT(*)
                  INTO valid_gate_count
                  FROM st_calibration_gate_v3 gate_row
                 WHERE gate_row.gate_evaluation_id = NEW.gate_evaluation_id
                   AND gate_row.release_id = NEW.release_id
                   AND gate_row.gate_status = 'PASS'
                   AND gate_row.evidence_provenance_status
                       = 'PERSISTED_VERIFIED'
                   AND gate_row.learning_run_id IS NOT NULL
                   AND gate_row.config_hash = NEW.config_hash
                   AND NOT EXISTS (
                       SELECT 1
                         FROM st_calibration_gate_v3 newer_gate
                        WHERE newer_gate.release_id = gate_row.release_id
                          AND (
                              newer_gate.evaluated_at
                                  > gate_row.evaluated_at
                              OR (
                                  newer_gate.evaluated_at
                                      = gate_row.evaluated_at
                                  AND newer_gate.gate_evaluation_id
                                      > gate_row.gate_evaluation_id
                              )
                          )
                   );
                IF valid_gate_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 paper eligibility requires latest persisted PASS gate';
                END IF;
            END IF;
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_shadow_release_immutable_bu",
    """
    CREATE TRIGGER trg_v3_shadow_release_immutable_bu
    BEFORE UPDATE ON st_shadow_release_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 shadow release evidence is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_shadow_release_immutable_bd",
    """
    CREATE TRIGGER trg_v3_shadow_release_immutable_bd
    BEFORE DELETE ON st_shadow_release_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 shadow release evidence cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_calibration_gate_immutable_bu",
    """
    CREATE TRIGGER trg_v3_calibration_gate_immutable_bu
    BEFORE UPDATE ON st_calibration_gate_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 calibration gate evidence is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_calibration_gate_immutable_bd",
    """
    CREATE TRIGGER trg_v3_calibration_gate_immutable_bd
    BEFORE DELETE ON st_calibration_gate_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 calibration gate evidence cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_learning_run_immutable_bu",
    """
    CREATE TRIGGER trg_v3_learning_run_immutable_bu
    BEFORE UPDATE ON st_counterfactual_learning_run_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 counterfactual learning run is immutable';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_v3_learning_run_immutable_bd",
    """
    CREATE TRIGGER trg_v3_learning_run_immutable_bd
    BEFORE DELETE ON st_counterfactual_learning_run_v3
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'V3 counterfactual learning run cannot be deleted';
    END
    """,
)


_EXPECTED_COLUMN_TYPES: Mapping[str, Mapping[str, str]] = {
    "st_decision_run_v3": {
        "requested_as_of": "date",
    },
    "st_horizon_model_artifact_v3": {
        "artifact_id": "char",
        "release_id": "varchar",
        "suite_release_id": "varchar",
        "horizon_days": "tinyint",
        "prediction_kind": "enum",
        "artifact_status": "enum",
        "training_start": "date",
        "training_end": "date",
        "validation_start": "date",
        "validation_end": "date",
        "training_session_count": "int",
        "oos_session_count": "int",
        "matured_sample_count": "int",
        "oos_sample_count": "int",
        "walk_forward_fold_count": "int",
        "direction_rank_correlation": "decimal",
        "calibration_mae": "decimal",
        "brier_score": "decimal",
        "population_stability_index": "decimal",
        "net_expectancy_after_cost_pct": "decimal",
        "profit_factor": "decimal",
        "cost_coverage_ratio": "decimal",
        "dataset_hash": "char",
        "feature_protocol_hash": "char",
        "model_artifact_hash": "char",
        "calibration_evidence_hash": "char",
        "registration_evidence_hash": "char",
        "training_receipt_status": "enum",
        "training_receipt_hash": "char",
        "training_receipt_json": "json",
        "artifact_json": "json",
        "metrics_json": "json",
        "block_reasons_json": "json",
        "order_authority": "tinyint",
    },
    "st_horizon_forecast_contract_v3": {
        "contract_id": "char",
        "horizon_days": "tinyint",
        "prediction_kind": "enum",
        "source_forecast_hash": "char",
        "source_evidence_json": "json",
        "model_inputs_json": "json",
        "selection_status": "enum",
        "selection_evidence_json": "json",
        "decision_session_date": "date",
        "entry_session_sequence": "int",
        "outcome_maturity_session_sequence": "int",
        "contract_json": "json",
        "cost_model_version": "varchar",
        "order_authority": "tinyint",
    },
    "st_horizon_outcome_v3": {
        "outcome_id": "char",
        "contract_id": "char",
        "horizon_days": "tinyint",
        "market_evidence_json": "json",
        "execution_feasibility": "enum",
        "outcome_status": "enum",
        "outcome_hash": "char",
        "order_authority": "tinyint",
    },
    "st_shadow_release_v3": {
        "release_state_id": "char",
        "transition_sequence": "bigint",
        "current_stage": "enum",
        "transition_hash": "char",
        "order_authority": "tinyint",
    },
    "st_calibration_gate_v3": {
        "gate_evaluation_id": "char",
        "gate_status": "enum",
        "learning_run_id": "char",
        "evidence_provenance_status": "enum",
        "failure_codes_json": "json",
        "evidence_hash": "char",
        "config_hash": "char",
        "model_artifact_hash": "char",
        "order_authority": "tinyint",
    },
    "st_counterfactual_learning_run_v3": {
        "learning_run_id": "char",
        "learning_status": "enum",
        "evidence_source": "enum",
        "metrics_json": "json",
        "model_artifact_hashes_json": "json",
        "provenance_hash": "char",
        "can_activate_model": "tinyint",
        "order_authority": "tinyint",
    },
}

_EXPECTED_COLUMN_ATTRIBUTES = {
    ("st_decision_run_v3", "requested_as_of"): ("YES", None),
    ("st_horizon_model_artifact_v3", "artifact_id"): ("NO", None),
    ("st_horizon_model_artifact_v3", "release_id"): ("NO", None),
    ("st_horizon_model_artifact_v3", "suite_release_id"): ("NO", None),
    ("st_horizon_model_artifact_v3", "model_key"): ("NO", None),
    ("st_horizon_model_artifact_v3", "model_version"): ("NO", None),
    ("st_horizon_model_artifact_v3", "horizon_days"): ("NO", None),
    ("st_horizon_model_artifact_v3", "prediction_kind"): ("NO", None),
    ("st_horizon_model_artifact_v3", "artifact_status"): ("NO", None),
    ("st_horizon_model_artifact_v3", "calibration_evidence_hash"):
        ("YES", None),
    ("st_horizon_model_artifact_v3", "registration_evidence_hash"):
        ("YES", None),
    ("st_horizon_model_artifact_v3", "training_receipt_status"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "training_receipt_hash"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "training_receipt_json"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "training_start"): ("YES", None),
    ("st_horizon_model_artifact_v3", "training_end"): ("YES", None),
    ("st_horizon_model_artifact_v3", "validation_start"): ("YES", None),
    ("st_horizon_model_artifact_v3", "validation_end"): ("YES", None),
    ("st_horizon_model_artifact_v3", "training_session_count"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "oos_session_count"): ("NO", None),
    ("st_horizon_model_artifact_v3", "matured_sample_count"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "oos_sample_count"): ("NO", None),
    ("st_horizon_model_artifact_v3", "walk_forward_fold_count"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "dataset_hash"): ("NO", None),
    ("st_horizon_model_artifact_v3", "feature_protocol_hash"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "model_artifact_hash"):
        ("NO", None),
    ("st_horizon_model_artifact_v3", "config_hash"): ("NO", None),
    ("st_horizon_model_artifact_v3", "code_version"): ("NO", None),
    ("st_horizon_model_artifact_v3", "artifact_json"): ("NO", None),
    ("st_horizon_model_artifact_v3", "metrics_json"): ("NO", None),
    ("st_horizon_model_artifact_v3", "block_reasons_json"): ("NO", None),
    ("st_horizon_model_artifact_v3", "evidence_valid_until"):
        ("YES", None),
    ("st_horizon_model_artifact_v3", "created_at"): ("NO", None),
    ("st_horizon_model_artifact_v3", "order_authority"): ("NO", "0"),
    ("st_horizon_forecast_contract_v3", "order_authority"): ("NO", "0"),
    ("st_horizon_outcome_v3", "order_authority"): ("NO", "0"),
    ("st_shadow_release_v3", "order_authority"): ("NO", "0"),
    ("st_calibration_gate_v3", "learning_run_id"): ("YES", None),
    ("st_calibration_gate_v3", "evidence_provenance_status"): ("NO", None),
    ("st_calibration_gate_v3", "order_authority"): ("NO", "0"),
    ("st_counterfactual_learning_run_v3", "evidence_source"): ("NO", None),
    ("st_counterfactual_learning_run_v3", "can_activate_model"): ("NO", "0"),
    ("st_counterfactual_learning_run_v3", "order_authority"): ("NO", "0"),
}

_EXPECTED_ENUM_COLUMN_TYPES = {
    ("st_horizon_model_artifact_v3", "prediction_kind"):
        "enum('CALIBRATED_OOS')",
    ("st_horizon_model_artifact_v3", "artifact_status"):
        "enum('BLOCKED','OOS_VERIFIED')",
    ("st_horizon_model_artifact_v3", "training_receipt_status"):
        "enum('UNVERIFIED','PROCESS_VERIFIED')",
    ("st_horizon_forecast_contract_v3", "prediction_kind"):
        "enum('PROXY_SCORE','CALIBRATED_OOS')",
    ("st_horizon_outcome_v3", "execution_feasibility"):
        "enum('UNVERIFIED_RESEARCH','EXECUTABLE_VERIFIED')",
    ("st_horizon_outcome_v3", "outcome_status"):
        "enum('MATURED_VERIFIED','QUARANTINED')",
    ("st_calibration_gate_v3", "gate_status"):
        "enum('PASS','BLOCK')",
    ("st_calibration_gate_v3", "evidence_provenance_status"):
        "enum('PERSISTED_VERIFIED','UNVERIFIED_PREVIEW')",
    ("st_counterfactual_learning_run_v3", "learning_status"):
        "enum('COLLECTING','EVIDENCE_READY')",
    ("st_counterfactual_learning_run_v3", "evidence_source"):
        "enum('HORIZON_CONTRACT_OUTCOME_LEDGER')",
}

_EXPECTED_INDEXES = {
    ("st_decision_run_v3", "idx_v3_decision_requested_asof"):
        (False, ("requested_as_of", "decision_at")),
    ("st_horizon_model_artifact_v3", "uk_v3_horizon_model_identity"):
        (True, (
            "model_key", "model_version", "horizon_days",
            "model_artifact_hash",
        )),
    ("st_horizon_model_artifact_v3", "uk_v3_horizon_model_release"):
        (True, ("release_id",)),
    ("st_horizon_model_artifact_v3", "uk_v3_horizon_model_artifact_hash"):
        (True, ("model_artifact_hash",)),
    ("st_horizon_model_artifact_v3", "idx_v3_horizon_model_latest"):
        (False, (
            "model_key", "horizon_days", "artifact_status", "created_at",
        )),
    ("st_horizon_forecast_contract_v3", "uk_v3_horizon_source_model"):
        (True, (
            "source_forecast_id", "model_key", "model_version",
            "horizon_days", "model_artifact_hash",
        )),
    ("st_horizon_outcome_v3", "uk_v3_horizon_outcome_contract"):
        (True, ("contract_id",)),
    ("st_calibration_gate_v3", "uk_v3_calibration_gate_evidence"):
        (True, ("release_id", "evidence_hash", "policy_hash", "evaluated_at")),
    ("st_counterfactual_learning_run_v3", "uk_v3_learning_evidence"):
        (True, (
            "evidence_hash", "policy_hash", "evaluation_date",
            "provenance_hash",
        )),
}

_EXPECTED_FOREIGN_KEYS = {
    "fk_v3_horizon_outcome_contract": (
        "st_horizon_outcome_v3", "contract_id",
        "st_horizon_forecast_contract_v3", "contract_id",
    ),
    "fk_v3_calibration_gate_learning_run": (
        "st_calibration_gate_v3", "learning_run_id",
        "st_counterfactual_learning_run_v3", "learning_run_id",
    ),
    "fk_v3_shadow_release_gate": (
        "st_shadow_release_v3", "gate_evaluation_id",
        "st_calibration_gate_v3", "gate_evaluation_id",
    ),
}

_EXPECTED_CHECKS = frozenset({
    "chk_v3_horizon_model_horizon",
    "chk_v3_horizon_model_clock",
    "chk_v3_horizon_model_identity",
    "chk_v3_horizon_model_hashes",
    "chk_v3_horizon_model_receipt",
    "chk_v3_horizon_model_no_order",
    "chk_v3_horizon_model_verified_claim",
    "chk_v3_horizon_no_order",
    "chk_v3_horizon_outcome_no_order",
    "chk_v3_horizon_outcome_research_execution",
    "chk_v3_shadow_release_no_order",
    "chk_v3_calibration_gate_no_order",
    "chk_v3_calibration_gate_provenance",
    "chk_v3_learning_no_activation",
    "chk_v3_learning_counts",
})

_CHECK_CLAUSE_MARKERS = {
    "chk_v3_horizon_model_horizon": (
        "horizon_days", "1", "5", "20",
    ),
    "chk_v3_horizon_model_clock": (
        "training_session_count > 0", "walk_forward_fold_count = 0",
        "validation_end <= training_end",
    ),
    "chk_v3_horizon_model_identity": (
        "artifact_id = model_artifact_hash",
    ),
    "chk_v3_horizon_model_hashes": (
        "artifact_id", "registration_evidence_hash", "training_receipt_hash",
        "^[0-9a-f]{64}$",
    ),
    "chk_v3_horizon_model_receipt": (
        "OOS_VERIFIED", "PROCESS_VERIFIED",
    ),
    "chk_v3_horizon_model_no_order": ("order_authority = 0",),
    "chk_v3_horizon_model_verified_claim": (
        "OOS_VERIFIED", "calibration_evidence_hash",
        "walk_forward_fold_count >= 3",
    ),
    "chk_v3_horizon_no_order": (
        "decision_scope", "'RESEARCH_ONLY'", "order_authority = 0",
    ),
    "chk_v3_horizon_outcome_no_order": ("order_authority = 0",),
    "chk_v3_horizon_outcome_research_execution": (
        "execution_feasibility", "'UNVERIFIED_RESEARCH'",
    ),
    "chk_v3_calibration_gate_no_order": ("order_authority = 0",),
    "chk_v3_learning_no_activation": (
        "can_activate_model = 0", "order_authority = 0",
    ),
}

_TRIGGER_BODY_MARKERS = {
    "trg_v3_horizon_model_guard_bi": (
        "OOS_VERIFIED", "training_session_count", "oos_session_count",
        "frozen evidence policy", "suite_release_id", "artifact_json",
        "projection differs", "walk_forward.folds", "training_receipt_json",
        "TRAINING_RECEIPT_UNVERIFIED", "PROCESS_VERIFIED",
    ),
    "trg_v3_horizon_contract_guard_bi": (
        "st_alpha_forecast_v3", "decision_result_hash",
        "st_horizon_model_artifact_v3", "OOS_VERIFIED",
    ),
    "trg_v3_horizon_outcome_guard_bi": (
        "st_horizon_forecast_contract_v3", "contract_hash",
        "UNVERIFIED_RESEARCH", "cost_model_version",
    ),
    "trg_v3_learning_run_guard_bi": (
        "HORIZON_CONTRACT_OUTCOME_LEDGER", "t20_evidence_ready",
    ),
    "trg_v3_calibration_gate_guard_bi": (
        "st_counterfactual_learning_run_v3", "learning_result_hash",
        "model_artifact_hash",
    ),
    "trg_v3_shadow_release_guard_bi": (
        "PAPER_ELIGIBLE", "PERSISTED_VERIFIED", "latest persisted PASS gate",
    ),
}


_MYSQL84_UTF8MB4_ESCAPED_LITERAL = re.compile(
    r"_utf8mb4\\'([^\\']*)\\'",
    re.IGNORECASE,
)
_MYSQL84_UTF8MB4_LITERAL = re.compile(
    r"_utf8mb4'([^']*)'",
    re.IGNORECASE,
)


def _normalize_mysql84_check_clause(value: object) -> str:
    """Normalize only MySQL 8.4's utf8mb4 literal rendering.

    ``information_schema.CHECK_CONSTRAINTS`` renders a declared
    ``'VALUE'`` as ``_utf8mb4\\'VALUE\\'`` through PyMySQL on MySQL 8.4.
    Keep the quotes and payload intact while removing that one known server
    rendering detail. Other introducers, malformed quoting, and changed
    literal payloads deliberately remain different so drift still fails.
    """

    normalized = str(value or "").replace("`", "")
    normalized = _MYSQL84_UTF8MB4_ESCAPED_LITERAL.sub(
        lambda match: "'" + match.group(1) + "'",
        normalized,
    )
    normalized = _MYSQL84_UTF8MB4_LITERAL.sub(
        lambda match: "'" + match.group(1) + "'",
        normalized,
    )
    return " ".join(normalized.split())

_EXPECTED_TRIGGERS = frozenset({
    "trg_v3_horizon_model_guard_bi",
    "trg_v3_horizon_model_immutable_bu",
    "trg_v3_horizon_model_immutable_bd",
    "trg_v3_horizon_contract_guard_bi",
    "trg_v3_horizon_contract_immutable_bu",
    "trg_v3_horizon_contract_immutable_bd",
    "trg_v3_horizon_outcome_guard_bi",
    "trg_v3_horizon_outcome_immutable_bu",
    "trg_v3_horizon_outcome_immutable_bd",
    "trg_v3_learning_run_guard_bi",
    "trg_v3_calibration_gate_guard_bi",
    "trg_v3_shadow_release_guard_bi",
    "trg_v3_shadow_release_immutable_bu",
    "trg_v3_shadow_release_immutable_bd",
    "trg_v3_calibration_gate_immutable_bu",
    "trg_v3_calibration_gate_immutable_bd",
    "trg_v3_learning_run_immutable_bu",
    "trg_v3_learning_run_immutable_bd",
})


def validate_shadow_intelligence_schema(
    connection: Connection,
    *,
    require_triggers: bool = True,
) -> None:
    """Reject a partially applied or type-drifted Shadow schema."""

    for table_name, expected in _EXPECTED_COLUMN_TYPES.items():
        rows = tuple(connection.execute(
            text(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
                "IS_NULLABLE, COLUMN_DEFAULT "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
            ),
            {"table_name": table_name},
        ).mappings())
        observed = {
            str(row["COLUMN_NAME"]): str(row["DATA_TYPE"]).casefold()
            for row in rows
        }
        if any(observed.get(column) != data_type for column, data_type in expected.items()):
            raise RuntimeError(
                f"V3 shadow intelligence schema drift: {table_name}"
            )
        by_name = {str(row["COLUMN_NAME"]): row for row in rows}
        for (expected_table, column), (nullable, default) in (
            _EXPECTED_COLUMN_ATTRIBUTES.items()
        ):
            if expected_table != table_name:
                continue
            row = by_name.get(column)
            observed_default = None if row is None else row["COLUMN_DEFAULT"]
            if (
                row is None
                or str(row["IS_NULLABLE"]) != nullable
                or (
                    default is not None
                    and str(observed_default).strip("'\"") != default
                )
            ):
                raise RuntimeError(
                    f"V3 shadow intelligence column contract drift: "
                    f"{table_name}.{column}"
                )
        for (expected_table, column), column_type in (
            _EXPECTED_ENUM_COLUMN_TYPES.items()
        ):
            if expected_table != table_name:
                continue
            row = by_name.get(column)
            if row is None or str(row["COLUMN_TYPE"]) != column_type:
                raise RuntimeError(
                    f"V3 shadow intelligence enum contract drift: "
                    f"{table_name}.{column}"
                )
    index_rows = connection.execute(text(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
        "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() "
        "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
    )).mappings().all()
    indexes: dict[tuple[str, str], tuple[bool, list[str]]] = {}
    for row in index_rows:
        key = (str(row["TABLE_NAME"]), str(row["INDEX_NAME"]))
        unique, columns = indexes.setdefault(
            key, (not bool(row["NON_UNIQUE"]), [])
        )
        columns.append(str(row["COLUMN_NAME"]))
        indexes[key] = (unique, columns)
    for key, (unique, columns) in _EXPECTED_INDEXES.items():
        observed_index = indexes.get(key)
        if observed_index is None or (
            observed_index[0] != unique
            or tuple(observed_index[1]) != columns
        ):
            raise RuntimeError(
                "V3 shadow intelligence index drift: " + ".".join(key)
            )
    foreign_rows = connection.execute(text(
        "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, "
        "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
        "FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL"
    )).mappings().all()
    foreign_keys = {
        str(row["CONSTRAINT_NAME"]): (
            str(row["TABLE_NAME"]), str(row["COLUMN_NAME"]),
            str(row["REFERENCED_TABLE_NAME"]),
            str(row["REFERENCED_COLUMN_NAME"]),
        )
        for row in foreign_rows
    }
    for name, expected in _EXPECTED_FOREIGN_KEYS.items():
        if foreign_keys.get(name) != expected:
            raise RuntimeError(
                f"V3 shadow intelligence foreign key drift: {name}"
            )
    check_rows = connection.execute(text(
        "SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE "
        "FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
        "AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA = DATABASE() "
        "AND tc.CONSTRAINT_TYPE = 'CHECK'"
    )).mappings().all()
    checks = {
        str(row["CONSTRAINT_NAME"]): str(row["CHECK_CLAUSE"])
        for row in check_rows
    }
    missing_checks = _EXPECTED_CHECKS - set(checks)
    if missing_checks:
        raise RuntimeError(
            "V3 shadow intelligence checks missing: "
            + ",".join(sorted(missing_checks))
        )
    for name, markers in _CHECK_CLAUSE_MARKERS.items():
        normalized = _normalize_mysql84_check_clause(checks.get(name, ""))
        if any(marker not in normalized for marker in markers):
            raise RuntimeError(
                f"V3 shadow intelligence check body drift: {name}"
            )
    if not require_triggers:
        return

    triggers = {
        str(item)
        for item in connection.execute(
            text(
                "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
                "WHERE TRIGGER_SCHEMA = DATABASE()"
            )
        ).scalars()
    }
    missing = _EXPECTED_TRIGGERS - triggers
    if missing:
        raise RuntimeError(
            "V3 shadow intelligence triggers missing: "
            + ",".join(sorted(missing))
        )
    trigger_rows = connection.execute(text(
        "SELECT TRIGGER_NAME, ACTION_STATEMENT FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA = DATABASE()"
    )).mappings().all()
    trigger_bodies = {
        str(row["TRIGGER_NAME"]): str(row["ACTION_STATEMENT"])
        for row in trigger_rows
    }
    for name, markers in _TRIGGER_BODY_MARKERS.items():
        body = trigger_bodies.get(name, "")
        if any(marker not in body for marker in markers):
            raise RuntimeError(
                f"V3 shadow intelligence trigger body drift: {name}"
            )


__all__ = [
    "SHADOW_INTELLIGENCE_DDL",
    "SHADOW_INTELLIGENCE_MIGRATION_VERSION",
    "validate_shadow_intelligence_schema",
]
