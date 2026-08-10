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
from server.common.scheduler_tasks import SCHEDULED_TASK_TABLE
from server.integrations.v3_execution_projection_outbox.schema import (
    V3_PROJECTION_OUTBOX_DDL,
    validate_v3_projection_outbox_schema,
)


MIGRATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration_v3 (
    version VARCHAR(80) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    statement_count INT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

MIGRATION_PROGRESS_TABLE = "schema_migration_v3_progress"
MIGRATION_PROGRESS_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {MIGRATION_PROGRESS_TABLE} (
    version VARCHAR(80) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    statement_count INT NOT NULL,
    completed_statement_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
"""

V3_PROJECTION_OUTBOX_MIGRATION_VERSION = (
    "20260804_001_v3_execution_projection_outbox"
)


@dataclass(frozen=True)
class V3MigrationResult:
    version: str
    status: str
    statement_count: int


@dataclass(frozen=True)
class _AppliedMigrationRecord:
    checksum: str
    statement_count: int


class V3MigrationAcceptanceFault(RuntimeError):
    """Intentional committed-DDL interruption for isolated acceptance only."""


@dataclass
class V3MigrationAcceptanceFaultHook:
    version: str
    committed_statement_count: int
    triggered: bool = False

    @classmethod
    def after_outbox_ddl_commit(
        cls,
        committed_statement_count: int,
    ) -> "V3MigrationAcceptanceFaultHook":
        if type(committed_statement_count) is not int or not (
            1 <= committed_statement_count < len(V3_PROJECTION_OUTBOX_DDL)
        ):
            raise ValueError(
                "outbox acceptance fault must leave the migration incomplete"
            )
        return cls(
            version=V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
            committed_statement_count=committed_statement_count,
        )

    def raise_if_matches(self, *, version: str, statement_count: int) -> None:
        if self.triggered:
            raise RuntimeError("a V3 acceptance fault hook cannot be reused")
        if (
            version == self.version
            and statement_count == self.committed_statement_count
        ):
            self.triggered = True
            raise V3MigrationAcceptanceFault(
                "intentional V3 outbox migration fault after committed DDL: "
                f"{version} statement={statement_count}"
            )

    def validate(self) -> None:
        if self.version != V3_PROJECTION_OUTBOX_MIGRATION_VERSION:
            raise ValueError(
                "V3 acceptance faults may target only the outbox migration"
            )
        if type(self.committed_statement_count) is not int or not (
            1 <= self.committed_statement_count < len(V3_PROJECTION_OUTBOX_DDL)
        ):
            raise ValueError(
                "outbox acceptance fault must leave the migration incomplete"
            )
        if type(self.triggered) is not bool or self.triggered:
            raise ValueError("V3 acceptance fault hook must be fresh")


MIGRATIONS = (
    {
        "version": "20260728_001_trading_v3_core",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_decision_run_v3 (
                run_uid VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                decision_at DATETIME NOT NULL,
                mode VARCHAR(24) NOT NULL,
                model_version VARCHAR(80) NOT NULL,
                lifecycle_status VARCHAR(32) NOT NULL,
                status VARCHAR(24) NOT NULL,
                dominant_regime VARCHAR(40) NOT NULL,
                risk_asset_cap DECIMAL(18,8) NOT NULL,
                regime_json LONGTEXT NOT NULL,
                portfolio_json LONGTEXT NOT NULL,
                forecast_count INT NOT NULL DEFAULT 0,
                validated_count INT NOT NULL DEFAULT 0,
                target_count INT NOT NULL DEFAULT 0,
                data_snapshot_hash CHAR(64) NOT NULL,
                result_hash CHAR(64) NOT NULL,
                error_message TEXT,
                created_at DATETIME NOT NULL,
                completed_at DATETIME,
                UNIQUE KEY uk_v3_decision_time
                    (trade_date, mode, model_version, decision_at),
                KEY idx_v3_decision_latest
                    (trade_date, decision_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_alpha_forecast_v3 (
                forecast_id VARCHAR(64) PRIMARY KEY,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                rank_no INT NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                strategy_key VARCHAR(64) NOT NULL,
                horizon_days INT NOT NULL,
                raw_score DECIMAL(18,8),
                expected_return_net_pct DECIMAL(18,8),
                return_q10_pct DECIMAL(18,8),
                return_q50_pct DECIMAL(18,8),
                return_q90_pct DECIMAL(18,8),
                probability_positive DECIMAL(18,8),
                expected_mae_pct DECIMAL(18,8),
                expected_mfe_pct DECIMAL(18,8),
                profit_factor DECIMAL(18,8),
                payoff_ratio DECIMAL(18,8),
                sample_count INT NOT NULL DEFAULT 0,
                confidence DECIMAL(18,8) NOT NULL DEFAULT 0,
                forecast_status VARCHAR(48) NOT NULL,
                theme_code VARCHAR(80) NOT NULL DEFAULT '',
                model_version VARCHAR(80) NOT NULL DEFAULT '',
                dataset_hash CHAR(64) NOT NULL DEFAULT '',
                feature_time DATETIME NOT NULL,
                valid_until DATETIME NOT NULL,
                initial_stop_pct DECIMAL(18,8) NOT NULL,
                reasons_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_forecast
                    (run_uid, stock_code, strategy_key),
                KEY idx_v3_forecast_latest
                    (trade_date, forecast_status, rank_no),
                KEY idx_v3_forecast_stock
                    (stock_code, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_target_portfolio_v3 (
                target_id VARCHAR(64) PRIMARY KEY,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                rank_no INT NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                target_weight DECIMAL(18,8) NOT NULL,
                target_value DECIMAL(20,2) NOT NULL,
                target_quantity INT NOT NULL,
                estimated_roundtrip_cost_pct DECIMAL(18,8) NOT NULL,
                expected_return_net_pct DECIMAL(18,8) NOT NULL,
                conservative_return_pct DECIMAL(18,8) NOT NULL,
                expected_mae_pct DECIMAL(18,8) NOT NULL,
                theme_code VARCHAR(80) NOT NULL DEFAULT '',
                strategy_keys_json LONGTEXT NOT NULL,
                reason TEXT NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'PLANNED',
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_target (run_uid, stock_code),
                KEY idx_v3_target_latest (trade_date, rank_no)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_position_state_v3 (
                position_state_id VARCHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                state VARCHAR(32) NOT NULL,
                quantity INT NOT NULL DEFAULT 0,
                sellable_quantity INT NOT NULL DEFAULT 0,
                average_cost DECIMAL(20,6) NOT NULL DEFAULT 0,
                current_weight DECIMAL(18,8) NOT NULL DEFAULT 0,
                target_weight DECIMAL(18,8) NOT NULL DEFAULT 0,
                entry_date DATE,
                add_count INT NOT NULL DEFAULT 0,
                thesis_version VARCHAR(80) NOT NULL,
                invalidation_json LONGTEXT NOT NULL,
                last_action VARCHAR(32) NOT NULL,
                last_reason_code VARCHAR(80) NOT NULL,
                last_reason TEXT NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_position (account_id, stock_code),
                KEY idx_v3_position_state (account_id, state)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_plan_v3 (
                execution_plan_id VARCHAR(64) PRIMARY KEY,
                run_uid VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                side VARCHAR(8) NOT NULL,
                quantity INT NOT NULL,
                limit_price DECIMAL(20,6),
                state VARCHAR(32) NOT NULL,
                reason_code VARCHAR(80) NOT NULL,
                source VARCHAR(40) NOT NULL DEFAULT 'V3_PORTFOLIO',
                real_order_allowed TINYINT(1) NOT NULL DEFAULT 0,
                idempotency_key VARCHAR(160) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_execution_idempotency (idempotency_key),
                KEY idx_v3_execution_pending
                    (account_id, trade_date, state)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_counterfactual_v3 (
                counterfactual_id VARCHAR(64) PRIMARY KEY,
                source_run_uid VARCHAR(64) NOT NULL,
                source_forecast_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                strategy_key VARCHAR(64) NOT NULL,
                horizon_days INT NOT NULL,
                accepted TINYINT(1) NOT NULL,
                reason_code VARCHAR(100) NOT NULL,
                expected_return_net_pct DECIMAL(18,8),
                realized_net_return_pct DECIMAL(18,8) NOT NULL,
                realized_mae_pct DECIMAL(18,8),
                realized_mfe_pct DECIMAL(18,8),
                missed_opportunity TINYINT(1) NOT NULL,
                false_positive TINYINT(1) NOT NULL,
                calibration_breach TINYINT(1) NOT NULL,
                attribution VARCHAR(160) NOT NULL,
                outcome_date DATE NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_counterfactual
                    (source_forecast_id, outcome_date),
                KEY idx_v3_counterfactual_audit
                    (outcome_date, missed_opportunity, strategy_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_opportunity_recall_v3 (
                recall_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                horizon_days INT NOT NULL,
                winner_threshold_pct DECIMAL(18,8) NOT NULL,
                winner_count INT NOT NULL,
                accepted_winner_count INT NOT NULL,
                missed_winner_count INT NOT NULL,
                recall_at_20 DECIMAL(18,8),
                recall_at_50 DECIMAL(18,8),
                accepted_average_net_return_pct DECIMAL(18,8),
                missed_reason_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_recall
                    (trade_date, horizon_days, winner_threshold_pct)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_tca_v3 (
                tca_id VARCHAR(64) PRIMARY KEY,
                execution_plan_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                side VARCHAR(8) NOT NULL,
                decision_price DECIMAL(20,6) NOT NULL,
                fill_price DECIMAL(20,6) NOT NULL,
                quantity INT NOT NULL,
                commission_cny DECIMAL(20,6) NOT NULL,
                tax_cny DECIMAL(20,6) NOT NULL,
                transfer_fee_cny DECIMAL(20,6) NOT NULL,
                slippage_cny DECIMAL(20,6) NOT NULL,
                implementation_shortfall_bps DECIMAL(18,8) NOT NULL,
                observed_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_tca_plan (execution_plan_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_model_registry_v3 (
                model_id VARCHAR(64) PRIMARY KEY,
                strategy_key VARCHAR(64) NOT NULL,
                model_version VARCHAR(80) NOT NULL,
                lifecycle_status VARCHAR(32) NOT NULL,
                training_start DATE NOT NULL,
                training_end DATE NOT NULL,
                validation_start DATE NOT NULL,
                validation_end DATE NOT NULL,
                dataset_hash CHAR(64) NOT NULL,
                feature_schema_hash CHAR(64) NOT NULL,
                calibration_json LONGTEXT NOT NULL,
                metrics_json LONGTEXT NOT NULL,
                config_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                activated_at DATETIME,
                UNIQUE KEY uk_v3_model_version
                    (strategy_key, model_version),
                KEY idx_v3_model_active
                    (strategy_key, lifecycle_status, activated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_validation_result_v3 (
                validation_id VARCHAR(64) PRIMARY KEY,
                model_version VARCHAR(80) NOT NULL,
                validation_type VARCHAR(48) NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                sample_count INT NOT NULL,
                net_expectancy_pct DECIMAL(18,8),
                payoff_ratio DECIMAL(18,8),
                profit_factor DECIMAL(18,8),
                maximum_drawdown_pct DECIMAL(18,8),
                cost_total_cny DECIMAL(20,6),
                opportunity_recall_at_20 DECIMAL(18,8),
                result_status VARCHAR(24) NOT NULL,
                block_reasons_json LONGTEXT NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_validation
                    (model_version, validation_type, period_start, period_end),
                KEY idx_v3_validation_latest
                    (model_version, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260729_002_restore_real_trading_hard_guard",
        "statements": (
            """
            UPDATE st_trade_account_v2
            SET real_trading_enabled = 0
            WHERE real_trading_enabled <> 0
            """,
            """
            DROP TRIGGER IF EXISTS
                trg_trade_account_v2_real_disabled_bi
            """,
            """
            CREATE TRIGGER trg_trade_account_v2_real_disabled_bi
            BEFORE INSERT ON st_trade_account_v2
            FOR EACH ROW
            BEGIN
                IF COALESCE(NEW.real_trading_enabled, 0) <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'real trading is disabled by database guard';
                END IF;
            END
            """,
            """
            DROP TRIGGER IF EXISTS
                trg_trade_account_v2_real_disabled_bu
            """,
            """
            CREATE TRIGGER trg_trade_account_v2_real_disabled_bu
            BEFORE UPDATE ON st_trade_account_v2
            FOR EACH ROW
            BEGIN
                IF COALESCE(NEW.real_trading_enabled, 0) <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'real trading is disabled by database guard';
                END IF;
            END
            """,
        ),
    },
    {
        "version": "20260730_003_add_forecast_feature_snapshot",
        "statements": (
            """
            ALTER TABLE st_alpha_forecast_v3
            ADD COLUMN features_json LONGTEXT NULL
            AFTER reasons_json
            """,
        ),
    },
    {
        "version": "20260730_004_trade_hypothesis_ledger",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_trade_hypothesis_v3 (
                hypothesis_id VARCHAR(64) PRIMARY KEY,
                hypothesis_key VARCHAR(160) NOT NULL,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                scope_type VARCHAR(24) NOT NULL,
                scope_code VARCHAR(32) NOT NULL,
                scope_name VARCHAR(160) NOT NULL,
                direction VARCHAR(16) NOT NULL,
                state VARCHAR(32) NOT NULL,
                prior_probability DECIMAL(18,8) NOT NULL,
                current_probability DECIMAL(18,8) NOT NULL,
                probability_kind VARCHAR(48) NOT NULL,
                confidence DECIMAL(18,8) NOT NULL,
                score DECIMAL(18,8) NOT NULL,
                horizon_minutes INT NOT NULL,
                alpha_half_life_minutes INT NOT NULL,
                proposed_action VARCHAR(48) NOT NULL,
                max_position_weight DECIMAL(18,8) NOT NULL,
                theme_code VARCHAR(160) NOT NULL DEFAULT '',
                role VARCHAR(40) NOT NULL,
                thesis TEXT NOT NULL,
                counter_thesis TEXT NOT NULL,
                supporting_evidence_json LONGTEXT NOT NULL,
                opposing_evidence_json LONGTEXT NOT NULL,
                triggers_json LONGTEXT NOT NULL,
                invalidations_json LONGTEXT NOT NULL,
                strategy_keys_json LONGTEXT NOT NULL,
                feature_time DATETIME NOT NULL,
                valid_until DATETIME NOT NULL,
                source_forecast_count INT NOT NULL DEFAULT 0,
                last_evidence_at DATETIME,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_hypothesis_run
                    (run_uid, hypothesis_key),
                KEY idx_v3_hypothesis_latest
                    (trade_date, scope_type, state, current_probability),
                KEY idx_v3_hypothesis_stock
                    (scope_code, trade_date, updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_hypothesis_evidence_v3 (
                evidence_id VARCHAR(64) PRIMARY KEY,
                hypothesis_id VARCHAR(64) NOT NULL,
                hypothesis_key VARCHAR(160) NOT NULL,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                observed_at DATETIME NOT NULL,
                evidence_type VARCHAR(64) NOT NULL,
                polarity VARCHAR(16) NOT NULL,
                strength DECIMAL(18,8) NOT NULL,
                source VARCHAR(80) NOT NULL,
                summary VARCHAR(500) NOT NULL,
                probability_before DECIMAL(18,8) NOT NULL,
                probability_after DECIMAL(18,8) NOT NULL,
                state_before VARCHAR(32) NOT NULL,
                state_after VARCHAR(32) NOT NULL,
                payload_json LONGTEXT NOT NULL,
                evidence_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_hypothesis_evidence_hash
                    (evidence_hash),
                KEY idx_v3_hypothesis_evidence_timeline
                    (hypothesis_id, observed_at),
                KEY idx_v3_hypothesis_evidence_latest
                    (trade_date, observed_at, polarity)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260730_005_decision_provenance",
        "statements": (
            """
            ALTER TABLE st_decision_run_v3
            ADD COLUMN config_hash CHAR(64) NOT NULL DEFAULT ''
                AFTER model_version,
            ADD COLUMN code_commit_sha VARCHAR(80) NOT NULL DEFAULT 'UNKNOWN'
                AFTER config_hash,
            ADD COLUMN calibration_set_hash CHAR(64) NOT NULL DEFAULT ''
                AFTER code_commit_sha
            """,
        ),
    },
    {
        "version": "20260730_006_target_theme_exposure",
        "statements": (
            """
            ALTER TABLE st_target_portfolio_v3
            ADD COLUMN theme_codes_json LONGTEXT NULL
                AFTER theme_code
            """,
        ),
    },
    {
        "version": "20260730_007_retire_legacy_models",
        "statements": (
            """
            UPDATE st_model_registry_v3
            SET lifecycle_status = 'RETIRED'
            WHERE lifecycle_status = 'PAPER_ACTIVE'
              AND model_version NOT LIKE '%v3.3.0%'
            """,
        ),
    },
    {
        "version": "20260730_008_disable_legacy_entry_routes",
        "statements": (
            f"""
            UPDATE {SCHEDULED_TASK_TABLE}
            SET enabled = 0,
                description = CONCAT(
                    COALESCE(description, ''),
                    ' [V3.3.0已隔离旧选股入口]'
                )
            WHERE task_type IN (
                'trading_v2_premarket_decision',
                'trading_v2_intraday_activation',
                'trading_v2_close_decision'
            )
            """,
        ),
    },
    {
        "version": "20260730_009_suspend_legacy_entry_strategies",
        "statements": (
            """
            UPDATE st_strategy_version_v2
            SET lifecycle_status = 'SUSPENDED',
                suspended_at = CURRENT_TIMESTAMP
            WHERE strategy_id IN (
                'sector_preheat',
                'intraday_dynamic_activation'
            )
              AND lifecycle_status IN (
                  'PAPER_TRIAL',
                  'PAPER_ACTIVE'
              )
            """,
        ),
    },
    {
        "version": "20260730_010_cancel_legacy_entry_orders",
        "statements": (
            """
            UPDATE st_order_v2 o
            JOIN st_trade_intent_v2 i
              ON i.intent_id = o.intent_id
            SET o.status = 'CANCELLED',
                o.waiting_reason = 'V3_ONLY_ROUTE',
                o.updated_at = CURRENT_TIMESTAMP
            WHERE o.account_id = 'paper-main-v2'
              AND o.side = 'BUY'
              AND o.filled_quantity = 0
              AND o.status IN (
                  'CREATED',
                  'RISK_APPROVED',
                  'QUEUED'
              )
              AND (
                  i.strategy_version LIKE 'stock_strategy_v2.%'
                  OR i.strategy_version LIKE 'sector_preheat_%'
                  OR i.strategy_version
                      LIKE 'intraday_dynamic_activation_%'
              )
            """,
        ),
    },
    {
        "version": "20260801_001_block_real_execution_plans",
        "statements": (
            """
            UPDATE st_execution_plan_v3
            SET real_order_allowed = 0
            WHERE real_order_allowed <> 0
            """,
            """
            DROP TRIGGER IF EXISTS
                trg_execution_plan_v3_real_disabled_bi
            """,
            """
            CREATE TRIGGER trg_execution_plan_v3_real_disabled_bi
            BEFORE INSERT ON st_execution_plan_v3
            FOR EACH ROW
            BEGIN
                IF COALESCE(NEW.real_order_allowed, 0) <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'real execution plans are disabled by database guard';
                END IF;
            END
            """,
            """
            DROP TRIGGER IF EXISTS
                trg_execution_plan_v3_real_disabled_bu
            """,
            """
            CREATE TRIGGER trg_execution_plan_v3_real_disabled_bu
            BEFORE UPDATE ON st_execution_plan_v3
            FOR EACH ROW
            BEGIN
                IF COALESCE(NEW.real_order_allowed, 0) <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'real execution plans are disabled by database guard';
                END IF;
            END
            """,
        ),
    },
    {
        "version": "20260801_002_repair_counterfactual_attribution",
        "statements": (
            """
            UPDATE st_counterfactual_v3 c
            JOIN st_alpha_forecast_v3 f
              ON f.forecast_id = c.source_forecast_id
            JOIN st_target_portfolio_v3 t
              ON t.run_uid = f.run_uid
             AND t.stock_code = f.stock_code
            SET c.accepted = 0,
                c.reason_code = COALESCE(
                    NULLIF(f.forecast_status, ''),
                    'NOT_TARGET_SIGNAL_STRATEGY'
                ),
                c.missed_opportunity = CASE
                    WHEN c.realized_net_return_pct > 0 THEN 1 ELSE 0
                END,
                c.false_positive = 0,
                c.attribution = CASE
                    WHEN c.realized_net_return_pct > 0
                    THEN CONCAT(
                        'MISSED_BY_',
                        COALESCE(
                            NULLIF(f.forecast_status, ''),
                            'NOT_TARGET_SIGNAL_STRATEGY'
                        )
                    )
                    ELSE 'DECISION_SUPPORTED'
                END
            WHERE c.accepted = 1
              AND JSON_CONTAINS(
                  COALESCE(t.strategy_keys_json, '[]'),
                  JSON_QUOTE(c.strategy_key)
              ) = 0
            """,
        ),
    },
    {
        "version": "20260801_003_unify_forward_execution_evidence",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_forward_trade_evidence_v3 (
                evidence_id CHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                source_run_uid VARCHAR(64) NOT NULL,
                source_forecast_id VARCHAR(64) NOT NULL DEFAULT '',
                stock_code VARCHAR(16) NOT NULL,
                strategy_key VARCHAR(64) NOT NULL,
                evidence_kind VARCHAR(32) NOT NULL,
                protocol_version VARCHAR(80) NOT NULL,
                entry_order_id VARCHAR(64) NOT NULL,
                entry_fill_id VARCHAR(64) NOT NULL,
                entry_trade_date DATE NOT NULL,
                entry_at DATETIME NOT NULL,
                entry_quantity BIGINT NOT NULL,
                entry_price DECIMAL(20,6) NOT NULL,
                entry_gross_cny DECIMAL(20,6) NOT NULL,
                entry_fee_cny DECIMAL(20,6) NOT NULL,
                closed_quantity BIGINT NOT NULL DEFAULT 0,
                exit_fill_ids_json LONGTEXT NOT NULL,
                exit_order_ids_json LONGTEXT NOT NULL,
                exit_at DATETIME,
                exit_average_price DECIMAL(20,6),
                exit_gross_cny DECIMAL(20,6) NOT NULL DEFAULT 0,
                exit_fee_cny DECIMAL(20,6) NOT NULL DEFAULT 0,
                realized_net_pnl_cny DECIMAL(20,6) NOT NULL DEFAULT 0,
                realized_net_return_pct DECIMAL(18,8),
                realized_mae_pct DECIMAL(18,8),
                realized_mfe_pct DECIMAL(18,8),
                exit_reason VARCHAR(100) NOT NULL DEFAULT '',
                evidence_status VARCHAR(32) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_forward_entry_strategy
                    (entry_fill_id, strategy_key),
                KEY idx_v3_forward_learning
                    (strategy_key, evidence_status, exit_at),
                KEY idx_v3_forward_account
                    (account_id, stock_code, entry_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            ALTER TABLE st_counterfactual_v3
            ADD COLUMN evidence_kind VARCHAR(24) NOT NULL DEFAULT 'SHADOW'
                AFTER strategy_key,
            ADD COLUMN selection_status VARCHAR(32) NOT NULL
                DEFAULT 'POLICY_REJECTED' AFTER evidence_kind,
            ADD COLUMN execution_status VARCHAR(32) NOT NULL
                DEFAULT 'NOT_APPLICABLE' AFTER selection_status,
            ADD COLUMN protocol_version VARCHAR(80) NOT NULL
                DEFAULT 'COUNTERFACTUAL_TECHNICAL_PROXY_V2'
                AFTER execution_status
            """,
            """
            UPDATE st_counterfactual_v3
            SET evidence_kind = 'SHADOW',
                selection_status = CASE
                    WHEN accepted = 1 THEN 'POLICY_SELECTED'
                    ELSE 'POLICY_REJECTED'
                END,
                execution_status = 'NOT_APPLICABLE',
                protocol_version = 'COUNTERFACTUAL_TECHNICAL_PROXY_V1'
            """,
            """
            ALTER TABLE st_opportunity_recall_v3
            ADD COLUMN strategy_key VARCHAR(64) NOT NULL DEFAULT ''
                AFTER horizon_days
            """,
            """
            DROP INDEX uk_v3_recall ON st_opportunity_recall_v3
            """,
            """
            CREATE UNIQUE INDEX uk_v3_recall_strategy
            ON st_opportunity_recall_v3 (
                trade_date, horizon_days, strategy_key,
                winner_threshold_pct
            )
            """,
        ),
    },
    {
        "version": "20260801_004_tag_opportunity_recall_evidence",
        "statements": (
            """
            ALTER TABLE st_opportunity_recall_v3
            ADD COLUMN evidence_kind VARCHAR(24) NOT NULL
                DEFAULT 'SHADOW' AFTER strategy_key,
            ADD COLUMN protocol_version VARCHAR(80) NOT NULL
                DEFAULT 'COUNTERFACTUAL_TECHNICAL_PROXY_V1'
                AFTER evidence_kind
            """,
            """
            UPDATE st_opportunity_recall_v3
            SET evidence_kind = 'SHADOW',
                protocol_version =
                    'COUNTERFACTUAL_TECHNICAL_PROXY_V1'
            """,
        ),
    },
    {
        "version": "20260801_005_freeze_sample_ownership",
        "statements": (
            """
            ALTER TABLE st_target_portfolio_v3
            ADD COLUMN primary_strategy_key VARCHAR(64) NOT NULL
                DEFAULT '' AFTER strategy_keys_json,
            ADD COLUMN primary_forecast_id VARCHAR(64) NOT NULL
                DEFAULT '' AFTER primary_strategy_key,
            ADD COLUMN attribution_snapshot_hash CHAR(64) NOT NULL
                DEFAULT '' AFTER primary_forecast_id
            """,
            """
            ALTER TABLE st_forward_trade_evidence_v3
            ADD COLUMN source_intent_id VARCHAR(64) NOT NULL DEFAULT ''
                AFTER source_forecast_id,
            ADD COLUMN sample_owner_role VARCHAR(24) NOT NULL
                DEFAULT 'PRIMARY' AFTER strategy_key,
            ADD COLUMN attribution_status VARCHAR(40) NOT NULL
                DEFAULT '' AFTER sample_owner_role,
            ADD COLUMN attribution_version VARCHAR(80) NOT NULL
                DEFAULT '' AFTER attribution_status,
            ADD COLUMN supporting_strategy_keys_json LONGTEXT NOT NULL
                AFTER attribution_version,
            ADD COLUMN ownership_hash CHAR(64) NOT NULL DEFAULT ''
                AFTER supporting_strategy_keys_json
            """,
            """
            CREATE UNIQUE INDEX uk_v3_forward_entry_owner
            ON st_forward_trade_evidence_v3 (entry_fill_id)
            """,
            """
            CREATE TRIGGER trg_v3_forward_owner_required_bi
            BEFORE INSERT ON st_forward_trade_evidence_v3
            FOR EACH ROW
            BEGIN
                IF NEW.source_run_uid = ''
                   OR NEW.source_forecast_id = ''
                   OR NEW.source_intent_id = ''
                   OR NEW.strategy_key = ''
                   OR NEW.sample_owner_role <> 'PRIMARY'
                   OR NEW.attribution_status = ''
                   OR NEW.attribution_version = ''
                   OR CHAR_LENGTH(NEW.ownership_hash) <> 64 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 forward sample ownership is incomplete';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_forward_owner_immutable_bu
            BEFORE UPDATE ON st_forward_trade_evidence_v3
            FOR EACH ROW
            BEGIN
                IF NEW.source_run_uid <> OLD.source_run_uid
                   OR NEW.source_forecast_id <> OLD.source_forecast_id
                   OR NEW.source_intent_id <> OLD.source_intent_id
                   OR NEW.stock_code <> OLD.stock_code
                   OR NEW.strategy_key <> OLD.strategy_key
                   OR NEW.sample_owner_role <> OLD.sample_owner_role
                   OR NEW.attribution_status <> OLD.attribution_status
                   OR NEW.attribution_version <> OLD.attribution_version
                   OR NEW.ownership_hash <> OLD.ownership_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 forward sample ownership is immutable';
                END IF;
            END
            """,
        ),
    },
    {
        "version": "20260801_006_counterfactual_backlog_queue",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_counterfactual_queue_v3 (
                forecast_id VARCHAR(64) PRIMARY KEY,
                queue_status VARCHAR(24) NOT NULL,
                defer_reason VARCHAR(80) NOT NULL,
                attempt_count INT NOT NULL DEFAULT 0,
                first_attempt_at DATETIME NOT NULL,
                last_attempt_at DATETIME NOT NULL,
                next_retry_at DATETIME NOT NULL,
                KEY idx_v3_counterfactual_retry
                    (queue_status, next_retry_at),
                KEY idx_v3_counterfactual_reason
                    (defer_reason, attempt_count)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260802_001_shadow_portfolio_evidence_isolation",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_shadow_portfolio_v3 (
                shadow_position_id CHAR(64) PRIMARY KEY,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                portfolio_kind VARCHAR(24) NOT NULL,
                group_key VARCHAR(160) NOT NULL,
                rank_no INT NOT NULL,
                source_forecast_id VARCHAR(64) NOT NULL,
                strategy_result_key CHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                strategy_key VARCHAR(64) NOT NULL,
                theme_code VARCHAR(160) NOT NULL DEFAULT '',
                horizon_days INT NOT NULL,
                selection_score DECIMAL(18,8),
                valid_until DATETIME NOT NULL,
                evidence_kind VARCHAR(24) NOT NULL DEFAULT 'SHADOW',
                protocol_version VARCHAR(80) NOT NULL,
                order_allowed TINYINT(1) NOT NULL DEFAULT 0,
                can_activate_model TINYINT(1) NOT NULL DEFAULT 0,
                result_status VARCHAR(24) NOT NULL DEFAULT 'OPEN',
                realized_net_return_pct DECIMAL(18,8),
                realized_mae_pct DECIMAL(18,8),
                realized_mfe_pct DECIMAL(18,8),
                missed_opportunity TINYINT(1) NOT NULL DEFAULT 0,
                false_positive TINYINT(1) NOT NULL DEFAULT 0,
                outcome_date DATE,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_shadow_group_result (
                    run_uid, portfolio_kind, group_key,
                    strategy_result_key
                ),
                KEY idx_v3_shadow_maturity (
                    result_status, valid_until
                ),
                KEY idx_v3_shadow_forecast (source_forecast_id),
                KEY idx_v3_shadow_group (
                    trade_date, portfolio_kind, group_key, rank_no
                )
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE UNIQUE INDEX uk_v3_counterfactual_forecast
            ON st_counterfactual_v3 (source_forecast_id)
            """,
            """
            UPDATE st_forward_trade_evidence_v3
            SET evidence_kind = 'EXECUTED_PAPER'
            WHERE evidence_kind <> 'EXECUTED_PAPER'
            """,
            """
            UPDATE st_counterfactual_v3
            SET evidence_kind = 'SHADOW',
                execution_status = 'NOT_APPLICABLE'
            WHERE evidence_kind <> 'SHADOW'
               OR execution_status <> 'NOT_APPLICABLE'
            """,
            "DROP TRIGGER IF EXISTS trg_v3_forward_owner_required_bi",
            """
            CREATE TRIGGER trg_v3_forward_owner_required_bi
            BEFORE INSERT ON st_forward_trade_evidence_v3
            FOR EACH ROW
            BEGIN
                IF NEW.source_run_uid = ''
                   OR NEW.source_forecast_id = ''
                   OR NEW.source_intent_id = ''
                   OR NEW.strategy_key = ''
                   OR NEW.sample_owner_role <> 'PRIMARY'
                   OR NEW.attribution_status = ''
                   OR NEW.attribution_version = ''
                   OR CHAR_LENGTH(NEW.ownership_hash) <> 64
                   OR NEW.evidence_kind <> 'EXECUTED_PAPER' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 executed forward evidence is incomplete';
                END IF;
                IF NOT EXISTS (
                    SELECT 1
                    FROM st_alpha_forecast_v3 f
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = NEW.source_intent_id
                    WHERE f.forecast_id = NEW.source_forecast_id
                      AND f.run_uid = NEW.source_run_uid
                      AND f.stock_code = NEW.stock_code
                      AND f.strategy_key = NEW.strategy_key
                      AND i.decision_run_uid = NEW.source_run_uid
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 executed evidence owner relation invalid';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_forward_owner_immutable_bu",
            """
            CREATE TRIGGER trg_v3_forward_owner_immutable_bu
            BEFORE UPDATE ON st_forward_trade_evidence_v3
            FOR EACH ROW
            BEGIN
                IF NEW.source_run_uid <> OLD.source_run_uid
                   OR NEW.source_forecast_id <> OLD.source_forecast_id
                   OR NEW.source_intent_id <> OLD.source_intent_id
                   OR NEW.stock_code <> OLD.stock_code
                   OR NEW.strategy_key <> OLD.strategy_key
                   OR NEW.sample_owner_role <> OLD.sample_owner_role
                   OR NEW.attribution_status <> OLD.attribution_status
                   OR NEW.attribution_version <> OLD.attribution_version
                   OR NEW.ownership_hash <> OLD.ownership_hash
                   OR NEW.evidence_kind <> 'EXECUTED_PAPER' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 executed evidence ownership is immutable';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_counterfactual_shadow_only_bi
            BEFORE INSERT ON st_counterfactual_v3
            FOR EACH ROW
            BEGIN
                IF NEW.evidence_kind <> 'SHADOW'
                   OR NEW.execution_status <> 'NOT_APPLICABLE' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 counterfactual evidence must remain shadow';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_counterfactual_shadow_only_bu
            BEFORE UPDATE ON st_counterfactual_v3
            FOR EACH ROW
            BEGIN
                IF NEW.source_forecast_id <> OLD.source_forecast_id
                   OR NEW.strategy_key <> OLD.strategy_key
                   OR NEW.evidence_kind <> 'SHADOW'
                   OR NEW.execution_status <> 'NOT_APPLICABLE' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 counterfactual identity is immutable shadow';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_shadow_portfolio_no_order_bi
            BEFORE INSERT ON st_shadow_portfolio_v3
            FOR EACH ROW
            BEGIN
                IF NEW.evidence_kind <> 'SHADOW'
                   OR NEW.order_allowed <> 0
                   OR NEW.can_activate_model <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 shadow portfolio cannot trade or activate';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_shadow_portfolio_no_order_bu
            BEFORE UPDATE ON st_shadow_portfolio_v3
            FOR EACH ROW
            BEGIN
                IF NEW.run_uid <> OLD.run_uid
                   OR NEW.portfolio_kind <> OLD.portfolio_kind
                   OR NEW.group_key <> OLD.group_key
                   OR NEW.source_forecast_id <> OLD.source_forecast_id
                   OR NEW.strategy_result_key <>
                      OLD.strategy_result_key
                   OR NEW.stock_code <> OLD.stock_code
                   OR NEW.strategy_key <> OLD.strategy_key
                   OR NEW.evidence_kind <> 'SHADOW'
                   OR NEW.order_allowed <> 0
                   OR NEW.can_activate_model <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 shadow portfolio identity is immutable';
                END IF;
            END
            """,
        ),
    },
    {
        "version": "20260802_002_generic_theme_signal_ledger",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_theme_signal_v3 (
                theme_signal_id CHAR(64) PRIMARY KEY,
                run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                source_forecast_id VARCHAR(64) NOT NULL,
                theme_feature_key CHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                strategy_key VARCHAR(64) NOT NULL,
                theme_code VARCHAR(160) NOT NULL,
                theme_name VARCHAR(160) NOT NULL,
                theme_source VARCHAR(40) NOT NULL,
                theme_cluster_keys_json LONGTEXT NOT NULL,
                horizon_days INT NOT NULL,
                raw_score DECIMAL(18,8),
                signal_status VARCHAR(48) NOT NULL,
                forecast_status VARCHAR(48) NOT NULL,
                expected_return_net_pct DECIMAL(18,8),
                selected_as_primary TINYINT(1) NOT NULL DEFAULT 0,
                feature_time DATETIME NOT NULL,
                valid_until DATETIME NOT NULL,
                features_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_v3_theme_signal_identity (
                    run_uid, stock_code, strategy_key, theme_feature_key
                ),
                KEY idx_v3_theme_signal_group (
                    trade_date, strategy_key, selected_as_primary
                ),
                KEY idx_v3_theme_signal_forecast (source_forecast_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            ALTER TABLE st_shadow_portfolio_v3
            ADD COLUMN source_theme_signal_id CHAR(64) NOT NULL DEFAULT ''
            AFTER source_forecast_id
            """,
            """
            CREATE INDEX idx_v3_shadow_theme_signal
            ON st_shadow_portfolio_v3 (source_theme_signal_id)
            """,
            "DROP TRIGGER IF EXISTS trg_v3_shadow_portfolio_no_order_bi",
            """
            CREATE TRIGGER trg_v3_shadow_portfolio_no_order_bi
            BEFORE INSERT ON st_shadow_portfolio_v3
            FOR EACH ROW
            BEGIN
                IF NEW.evidence_kind <> 'SHADOW'
                   OR NEW.order_allowed <> 0
                   OR NEW.can_activate_model <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 shadow portfolio cannot trade or activate';
                END IF;
                IF NEW.portfolio_kind = 'THEME'
                   AND (
                       NEW.source_theme_signal_id = ''
                       OR NOT EXISTS (
                           SELECT 1
                           FROM st_theme_signal_v3 s
                           WHERE s.theme_signal_id =
                                 NEW.source_theme_signal_id
                             AND s.run_uid = NEW.run_uid
                             AND s.stock_code = NEW.stock_code
                             AND s.strategy_key = NEW.strategy_key
                       )
                   ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 theme shadow source signal is invalid';
                END IF;
                IF NEW.portfolio_kind = 'STRATEGY'
                   AND NEW.source_theme_signal_id <> '' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 strategy shadow cannot borrow theme evidence';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_shadow_portfolio_no_order_bu",
            """
            CREATE TRIGGER trg_v3_shadow_portfolio_no_order_bu
            BEFORE UPDATE ON st_shadow_portfolio_v3
            FOR EACH ROW
            BEGIN
                IF NEW.run_uid <> OLD.run_uid
                   OR NEW.portfolio_kind <> OLD.portfolio_kind
                   OR NEW.group_key <> OLD.group_key
                   OR NEW.source_forecast_id <> OLD.source_forecast_id
                   OR NEW.source_theme_signal_id <>
                      OLD.source_theme_signal_id
                   OR NEW.strategy_result_key <>
                      OLD.strategy_result_key
                   OR NEW.stock_code <> OLD.stock_code
                   OR NEW.strategy_key <> OLD.strategy_key
                   OR NEW.evidence_kind <> 'SHADOW'
                   OR NEW.order_allowed <> 0
                   OR NEW.can_activate_model <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT =
                        'V3 shadow portfolio identity is immutable';
                END IF;
            END
            """,
            """
            CREATE TRIGGER trg_v3_theme_signal_immutable_bu
            BEFORE UPDATE ON st_theme_signal_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 theme signal evidence is immutable';
            END
            """,
        ),
    },
    {
        "version": "20260802_003_news_point_in_time_knowledge",
        "statements": (
            """
            ALTER TABLE st_news_flash
            ADD COLUMN first_seen_at DATETIME NULL
            AFTER publish_time
            """,
            """
            UPDATE st_news_flash
            SET first_seen_at = COALESCE(etl_sync_at, publish_time)
            WHERE first_seen_at IS NULL
            """,
            "DROP TRIGGER IF EXISTS trg_news_first_seen_bi",
            """
            CREATE TRIGGER trg_news_first_seen_bi
            BEFORE INSERT ON st_news_flash
            FOR EACH ROW
            BEGIN
                SET NEW.first_seen_at = COALESCE(
                    NEW.first_seen_at,
                    NEW.etl_sync_at,
                    NEW.publish_time
                );
            END
            """,
            "DROP TRIGGER IF EXISTS trg_news_first_seen_bu",
            """
            CREATE TRIGGER trg_news_first_seen_bu
            BEFORE UPDATE ON st_news_flash
            FOR EACH ROW
            BEGIN
                SET NEW.first_seen_at = COALESCE(
                    OLD.first_seen_at,
                    NEW.first_seen_at,
                    NEW.etl_sync_at,
                    NEW.publish_time
                );
            END
            """,
            """
            ALTER TABLE st_news_flash
            MODIFY COLUMN first_seen_at DATETIME NOT NULL
            """,
            """
            CREATE INDEX idx_news_publish_first_seen
            ON st_news_flash (publish_time, first_seen_at)
            """,
        ),
    },
    {
        "version": "20260803_001_v3_execution_projection_subscriber",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_execution_plan_binding_v3 (
                execution_plan_id VARCHAR(64) PRIMARY KEY,
                binding_id CHAR(64) NOT NULL,
                binding_hash CHAR(64) NOT NULL,
                source_intent_id VARCHAR(64) NOT NULL,
                source_order_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                run_uid VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                side VARCHAR(8) NOT NULL,
                quantity BIGINT UNSIGNED NOT NULL,
                bound_at DATETIME(6) NOT NULL,
                created_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v3_projection_binding_id (binding_id),
                UNIQUE KEY uk_v3_projection_binding_order (source_order_id),
                KEY idx_v3_projection_binding_intent (source_intent_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_projection_head_v3 (
                execution_plan_id VARCHAR(64) PRIMARY KEY,
                binding_id CHAR(64) NOT NULL,
                binding_hash CHAR(64) NOT NULL,
                source_order_id VARCHAR(64) NOT NULL,
                last_source_sequence BIGINT UNSIGNED NOT NULL,
                last_projection_id CHAR(64) NOT NULL,
                last_payload_hash CHAR(64) NOT NULL,
                last_plan_state VARCHAR(32) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v3_projection_head_order (source_order_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_projection_inbox_v3 (
                projection_id CHAR(64) PRIMARY KEY,
                payload_hash CHAR(64) NOT NULL,
                execution_plan_id VARCHAR(64) NOT NULL,
                binding_id CHAR(64) NOT NULL,
                binding_hash CHAR(64) NOT NULL,
                binding_bound_at DATETIME(6) NOT NULL,
                source_intent_id VARCHAR(64) NOT NULL,
                source_order_id VARCHAR(64) NOT NULL,
                source_order_created_at DATETIME(6) NOT NULL,
                source_event_id VARCHAR(255) NOT NULL,
                source_sequence BIGINT UNSIGNED NOT NULL,
                source_result_idempotency_key CHAR(64) NOT NULL,
                source_result_fingerprint CHAR(64) NOT NULL,
                source_transition_id CHAR(64) NOT NULL,
                source_transition_payload_hash CHAR(64) NOT NULL,
                source_order_state_hash CHAR(64) NOT NULL,
                source_order_status VARCHAR(32) NOT NULL,
                cumulative_filled_quantity BIGINT UNSIGNED NOT NULL,
                plan_state VARCHAR(32) NOT NULL,
                occurred_at DATETIME(6) NOT NULL,
                applied_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_v3_projection_source_event
                    (source_order_id, source_event_id),
                KEY idx_v3_projection_plan_sequence
                    (execution_plan_id, source_sequence)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_binding_immutable_bu",
            """
            CREATE TRIGGER trg_v3_projection_binding_immutable_bu
            BEFORE UPDATE ON st_execution_plan_binding_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 execution plan binding is immutable';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_binding_immutable_bd",
            """
            CREATE TRIGGER trg_v3_projection_binding_immutable_bd
            BEFORE DELETE ON st_execution_plan_binding_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 execution plan binding cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_head_sequence_bi",
            """
            CREATE TRIGGER trg_v3_projection_head_sequence_bi
            BEFORE INSERT ON st_execution_projection_head_v3
            FOR EACH ROW
            BEGIN
                IF NEW.last_source_sequence <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 projection head must start at sequence 1';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_head_sequence_bu",
            """
            CREATE TRIGGER trg_v3_projection_head_sequence_bu
            BEFORE UPDATE ON st_execution_projection_head_v3
            FOR EACH ROW
            BEGIN
                IF NEW.execution_plan_id <> OLD.execution_plan_id
                   OR NEW.binding_id <> OLD.binding_id
                   OR NEW.binding_hash <> OLD.binding_hash
                   OR NEW.source_order_id <> OLD.source_order_id THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 projection head binding is immutable';
                END IF;
                IF NEW.last_source_sequence <> OLD.last_source_sequence + 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'V3 projection head sequence must be contiguous';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_head_immutable_bd",
            """
            CREATE TRIGGER trg_v3_projection_head_immutable_bd
            BEFORE DELETE ON st_execution_projection_head_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 projection head cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_inbox_immutable_bu",
            """
            CREATE TRIGGER trg_v3_projection_inbox_immutable_bu
            BEFORE UPDATE ON st_execution_projection_inbox_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 projection inbox is immutable';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_v3_projection_inbox_immutable_bd",
            """
            CREATE TRIGGER trg_v3_projection_inbox_immutable_bd
            BEFORE DELETE ON st_execution_projection_inbox_v3
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'V3 projection inbox cannot be deleted';
            END
            """,
        ),
    },
)

# This is an outbox/read-model migration only.  It is deliberately appended to
# the existing V3 migration stream and does not create or replace any V2
# account, order, fill, cash, lot, position, or risk ledger.  Each CREATE is
# independently repeatable after MySQL's implicit DDL commit.
MIGRATIONS = MIGRATIONS + (
    {
        "version": V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
        "statements": tuple(V3_PROJECTION_OUTBOX_DDL),
    },
)


def _checksum(statements: tuple[str, ...]) -> str:
    return hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()


def _mysql_dialect(engine: Engine) -> bool:
    return str(engine.dialect.name).lower() in {"mysql", "mariadb"}


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
            "V3 migrations require validated Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly; "
            f"server_version={version or 'unknown'}"
        )


def _table_exists(connection: Connection, table_name: str) -> bool:
    return bool(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
            ),
            {"table_name": table_name},
        ).scalar()
    )


def _declared_contracts() -> dict[str, tuple[str, int]]:
    return {
        str(item["version"]): (
            _checksum(tuple(item["statements"])),
            len(tuple(item["statements"])),
        )
        for item in MIGRATIONS
    }


def _ensure_migration_metadata(connection: Connection) -> None:
    """Upgrade the runner-owned metadata without rewriting V3 migrations.

    Early V3 installations recorded only a checksum.  The runner adds and
    backfills ``statement_count`` under the same named lock, and rejects every
    unknown version, checksum or count before applying business-schema DDL.
    The progress table makes DML exactly-once and narrows DDL crash recovery to
    a single statement whose physical result can be inspected.
    """

    connection.execute(text(MIGRATION_TABLE_DDL))
    connection.commit()
    columns = {
        str(row["COLUMN_NAME"]): str(row["IS_NULLABLE"]).upper()
        for row in connection.execute(
            text(
                "SELECT COLUMN_NAME, IS_NULLABLE "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = 'schema_migration_v3'"
            )
        ).mappings()
    }
    if "statement_count" not in columns:
        connection.execute(
            text(
                "ALTER TABLE schema_migration_v3 "
                "ADD COLUMN statement_count INT NULL AFTER checksum"
            )
        )
        connection.commit()
        columns["statement_count"] = "YES"

    declared = _declared_contracts()
    rows = tuple(
        connection.execute(
            text(
                "SELECT version, checksum, statement_count "
                "FROM schema_migration_v3 ORDER BY version"
            )
        ).mappings()
    )
    for row in rows:
        version = str(row["version"])
        expected = declared.get(version)
        if expected is None:
            raise RuntimeError(
                f"unknown applied V3 migration blocks metadata upgrade: {version}"
            )
        expected_checksum, expected_count = expected
        if str(row["checksum"]) != expected_checksum:
            raise RuntimeError(
                f"applied V3 migration checksum changed: {version}"
            )
        observed_count = row["statement_count"]
        if observed_count is None:
            connection.execute(
                text(
                    "UPDATE schema_migration_v3 SET statement_count = :count "
                    "WHERE version = :version AND statement_count IS NULL"
                ),
                {"version": version, "count": expected_count},
            )
        elif type(observed_count) is not int or observed_count != expected_count:
            raise RuntimeError(
                f"applied V3 migration statement_count changed: {version}"
            )
    connection.commit()
    if columns.get("statement_count") == "YES":
        connection.execute(
            text(
                "ALTER TABLE schema_migration_v3 "
                "MODIFY COLUMN statement_count INT NOT NULL"
            )
        )
        connection.commit()

    connection.execute(text(MIGRATION_PROGRESS_TABLE_DDL))
    connection.commit()


def _applied_record(
    connection: Connection,
    version: str,
) -> _AppliedMigrationRecord | None:
    row = connection.execute(
        text(
            "SELECT checksum, statement_count FROM schema_migration_v3 "
            "WHERE version = :version"
        ),
        {"version": version},
    ).mappings().first()
    if row is None:
        return None
    count = row["statement_count"]
    if type(count) is not int:
        raise RuntimeError(
            f"invalid V3 migration statement_count type: {version}"
        )
    return _AppliedMigrationRecord(str(row["checksum"]), count)


def _progress_count(
    connection: Connection,
    *,
    version: str,
    checksum: str,
    statement_count: int,
) -> int:
    connection.execute(
        text(
            f"INSERT IGNORE INTO {MIGRATION_PROGRESS_TABLE} "
            "(version, checksum, statement_count, completed_statement_count) "
            "VALUES (:version, :checksum, :statement_count, 0)"
        ),
        {
            "version": version,
            "checksum": checksum,
            "statement_count": statement_count,
        },
    )
    connection.commit()
    row = connection.execute(
        text(
            f"SELECT checksum, statement_count, completed_statement_count "
            f"FROM {MIGRATION_PROGRESS_TABLE} WHERE version = :version"
        ),
        {"version": version},
    ).mappings().first()
    if row is None:
        raise RuntimeError(f"V3 migration progress row is missing: {version}")
    completed = row["completed_statement_count"]
    if (
        str(row["checksum"]) != checksum
        or type(row["statement_count"]) is not int
        or row["statement_count"] != statement_count
        or type(completed) is not int
        or not 0 <= completed <= statement_count
    ):
        raise RuntimeError(f"V3 migration progress contract changed: {version}")
    return completed


def _mark_progress(
    connection: Connection,
    *,
    version: str,
    previous_count: int,
    next_count: int,
) -> None:
    result = connection.execute(
        text(
            f"UPDATE {MIGRATION_PROGRESS_TABLE} "
            "SET completed_statement_count = :next_count "
            "WHERE version = :version "
            "AND completed_statement_count = :previous_count"
        ),
        {
            "version": version,
            "previous_count": previous_count,
            "next_count": next_count,
        },
    )
    if type(getattr(result, "rowcount", None)) is not int or result.rowcount != 1:
        raise RuntimeError(f"V3 migration progress CAS failed: {version}")


_DROP_TRIGGER_RE = re.compile(
    r"^\s*DROP\s+TRIGGER\s+IF\s+EXISTS\s+`?([a-z0-9_]+)`?\s*$",
    re.IGNORECASE,
)
_CREATE_TRIGGER_RE = re.compile(
    r"^\s*CREATE\s+TRIGGER\s+`?([a-z0-9_]+)`?\s+"
    r"(BEFORE|AFTER)\s+(INSERT|UPDATE|DELETE)\s+ON\s+"
    r"`?([a-z0-9_]+)`?\s+FOR\s+EACH\s+ROW\s+(.*)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_ALTER_ADD_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?([a-z0-9_]+)`?\s+ADD\s+COLUMN\b",
    re.IGNORECASE | re.DOTALL,
)
_ADD_COLUMN_NAME_RE = re.compile(
    r"\bADD\s+COLUMN\s+`?([a-z0-9_]+)`?",
    re.IGNORECASE,
)
_ALTER_MODIFY_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?([a-z0-9_]+)`?\s+MODIFY\s+COLUMN\s+"
    r"`?([a-z0-9_]+)`?\s+([a-z]+(?:\(\d+(?:,\d+)?\))?)\s+"
    r"(NOT\s+NULL|NULL)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DROP_INDEX_RE = re.compile(
    r"^\s*DROP\s+INDEX\s+`?([a-z0-9_]+)`?\s+ON\s+"
    r"`?([a-z0-9_]+)`?\s*$",
    re.IGNORECASE,
)
_CREATE_INDEX_RE = re.compile(
    r"^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+`?([a-z0-9_]+)`?\s+ON\s+"
    r"`?([a-z0-9_]+)`?\s*\(([^)]+)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _normalized_sql(value: object) -> str:
    return " ".join(str(value or "").replace("`", "").split()).casefold()


def _ddl_statement_already_applied(
    connection: Connection,
    statement: str,
) -> bool:
    """Recognize exact recoverable DDL after MySQL's implicit commit."""

    drop_trigger = _DROP_TRIGGER_RE.match(statement)
    if drop_trigger is not None:
        return not bool(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    "AND TRIGGER_NAME = :trigger_name"
                ),
                {"trigger_name": drop_trigger.group(1)},
            ).scalar()
        )

    create_trigger = _CREATE_TRIGGER_RE.match(statement)
    if create_trigger is not None:
        trigger_name, timing, event, table_name, body = create_trigger.groups()
        rows = tuple(
            connection.execute(
                text(
                    "SELECT EVENT_OBJECT_TABLE, ACTION_TIMING, "
                    "EVENT_MANIPULATION, ACTION_STATEMENT "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    "AND TRIGGER_NAME = :trigger_name"
                ),
                {"trigger_name": trigger_name},
            ).mappings()
        )
        if not rows:
            return False
        if len(rows) != 1:
            raise RuntimeError(f"duplicate V3 trigger metadata: {trigger_name}")
        row = rows[0]
        exact = (
            str(row["EVENT_OBJECT_TABLE"]).casefold() == table_name.casefold()
            and str(row["ACTION_TIMING"]).upper() == timing.upper()
            and str(row["EVENT_MANIPULATION"]).upper() == event.upper()
            and _normalized_sql(row["ACTION_STATEMENT"])
            == _normalized_sql(body)
        )
        if not exact:
            raise RuntimeError(
                f"V3 migration recovery found a drifted trigger: {trigger_name}"
            )
        return True

    alter_add = _ALTER_ADD_RE.match(statement)
    if alter_add is not None:
        table_name = alter_add.group(1)
        expected = tuple(
            item.casefold() for item in _ADD_COLUMN_NAME_RE.findall(statement)
        )
        observed = {
            str(row["COLUMN_NAME"]).casefold()
            for row in connection.execute(
                text(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
                ),
                {"table_name": table_name},
            ).mappings()
        }
        present = tuple(name in observed for name in expected)
        if all(present):
            return True
        if any(present):
            raise RuntimeError(
                f"V3 migration recovery found a partial ALTER TABLE: {table_name}"
            )
        return False

    alter_modify = _ALTER_MODIFY_RE.match(statement)
    if alter_modify is not None:
        table_name, column_name, column_type, nullable = alter_modify.groups()
        row = connection.execute(
            text(
                "SELECT COLUMN_TYPE, IS_NULLABLE "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
                "AND COLUMN_NAME = :column_name"
            ),
            {"table_name": table_name, "column_name": column_name},
        ).mappings().first()
        if row is None:
            return False
        return (
            str(row["COLUMN_TYPE"]).casefold() == column_type.casefold()
            and (str(row["IS_NULLABLE"]).upper() == "YES")
            == (nullable.upper() == "NULL")
        )

    drop_index = _DROP_INDEX_RE.match(statement)
    if drop_index is not None:
        index_name, table_name = drop_index.groups()
        return not bool(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
                    "AND INDEX_NAME = :index_name"
                ),
                {"table_name": table_name, "index_name": index_name},
            ).scalar()
        )

    create_index = _CREATE_INDEX_RE.match(statement)
    if create_index is not None:
        unique, index_name, table_name, raw_columns = create_index.groups()
        expected_columns = tuple(
            item.strip().strip("`").casefold()
            for item in raw_columns.split(",")
        )
        rows = tuple(
            connection.execute(
                text(
                    "SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
                    "AND INDEX_NAME = :index_name ORDER BY SEQ_IN_INDEX"
                ),
                {"table_name": table_name, "index_name": index_name},
            ).mappings()
        )
        if not rows:
            return False
        observed_columns = tuple(
            str(row["COLUMN_NAME"]).casefold() for row in rows
        )
        exact = (
            observed_columns == expected_columns
            and tuple(int(row["SEQ_IN_INDEX"]) for row in rows)
            == tuple(range(1, len(rows) + 1))
            and all(row["SUB_PART"] is None for row in rows)
            and all(
                int(row["NON_UNIQUE"]) == (0 if unique else 1)
                for row in rows
            )
        )
        if not exact:
            raise RuntimeError(
                f"V3 migration recovery found a drifted index: {index_name}"
            )
        return True

    return False


def _is_ddl(statement: str) -> bool:
    return bool(re.match(r"^\s*(?:CREATE|ALTER|DROP)\b", statement, re.I))


def _validate_applied_migration(
    connection: Connection,
    *,
    version: str,
) -> None:
    if version == V3_PROJECTION_OUTBOX_MIGRATION_VERSION:
        validate_v3_projection_outbox_schema(connection)


def _run_v3_migrations_unlocked(
    engine: Engine,
    *,
    dry_run: bool,
    connection: Connection | None,
    acceptance_fault_hook: V3MigrationAcceptanceFaultHook | None,
) -> list[V3MigrationResult]:
    if not _mysql_dialect(engine):
        if not dry_run:
            raise RuntimeError("V3 migrations require MySQL or MariaDB")
        return [
            V3MigrationResult(
                str(item["version"]),
                "would_apply",
                len(tuple(item["statements"])),
            )
            for item in MIGRATIONS
        ]
    if not dry_run and connection is None:
        raise RuntimeError("V3 migration writes require the named-lock connection")

    opened: Connection | None = None
    if connection is None:
        opened = engine.connect()
        connection = opened
    try:
        if dry_run:
            table_exists = _table_exists(connection, "schema_migration_v3")
            results: list[V3MigrationResult] = []
            for migration in MIGRATIONS:
                version = str(migration["version"])
                statements = tuple(migration["statements"])
                status = "would_apply"
                if table_exists:
                    row = connection.execute(
                        text(
                            "SELECT checksum FROM schema_migration_v3 "
                            "WHERE version = :version"
                        ),
                        {"version": version},
                    ).mappings().first()
                    if row is not None:
                        if str(row["checksum"]) != _checksum(statements):
                            raise RuntimeError(
                                f"applied V3 migration checksum changed: {version}"
                            )
                        status = "exists"
                results.append(V3MigrationResult(version, status, len(statements)))
            return results

        assert connection is not None
        _validate_migration_server(engine, connection)
        _ensure_migration_metadata(connection)
        results = []
        for migration in MIGRATIONS:
            version = str(migration["version"])
            statements = tuple(migration["statements"])
            checksum = _checksum(statements)
            statement_count = len(statements)
            applied = _applied_record(connection, version)
            if applied is not None:
                if (
                    acceptance_fault_hook is not None
                    and acceptance_fault_hook.version == version
                ):
                    raise RuntimeError(
                        "V3 acceptance fault target migration is already recorded"
                    )
                if (
                    applied.checksum != checksum
                    or applied.statement_count != statement_count
                ):
                    raise RuntimeError(
                        f"applied V3 migration contract changed: {version}"
                    )
                _validate_applied_migration(connection, version=version)
                results.append(
                    V3MigrationResult(version, "exists", statement_count)
                )
                continue

            completed = _progress_count(
                connection,
                version=version,
                checksum=checksum,
                statement_count=statement_count,
            )
            for index in range(completed, statement_count):
                statement = str(statements[index])
                next_count = index + 1
                if _is_ddl(statement):
                    recovered = _ddl_statement_already_applied(
                        connection,
                        statement,
                    )
                    if not recovered:
                        connection.execute(text(statement))
                        connection.commit()
                        if acceptance_fault_hook is not None:
                            acceptance_fault_hook.raise_if_matches(
                                version=version,
                                statement_count=next_count,
                            )
                    _mark_progress(
                        connection,
                        version=version,
                        previous_count=index,
                        next_count=next_count,
                    )
                    connection.commit()
                else:
                    try:
                        connection.execute(text(statement))
                        _mark_progress(
                            connection,
                            version=version,
                            previous_count=index,
                            next_count=next_count,
                        )
                        connection.commit()
                    except Exception:
                        connection.rollback()
                        raise

            _validate_applied_migration(connection, version=version)
            connection.execute(
                text(
                    "INSERT IGNORE INTO schema_migration_v3 "
                    "(version, checksum, statement_count) "
                    "VALUES (:version, :checksum, :statement_count)"
                ),
                {
                    "version": version,
                    "checksum": checksum,
                    "statement_count": statement_count,
                },
            )
            connection.commit()
            recorded = _applied_record(connection, version)
            if (
                recorded is None
                or recorded.checksum != checksum
                or recorded.statement_count != statement_count
            ):
                raise RuntimeError(f"V3 migration ledger conflict: {version}")
            results.append(V3MigrationResult(version, "applied", statement_count))
        return results
    finally:
        if opened is not None:
            opened.close()


def run_v3_migrations(
    engine: Engine,
    *,
    dry_run: bool = False,
    acceptance_fault_hook: V3MigrationAcceptanceFaultHook | None = None,
) -> list[V3MigrationResult]:
    """Apply V3 under one MySQL named lock with statement-level recovery."""

    if type(dry_run) is not bool:
        raise TypeError("dry_run must be bool")
    if acceptance_fault_hook is not None:
        if type(acceptance_fault_hook) is not V3MigrationAcceptanceFaultHook:
            raise TypeError(
                "acceptance_fault_hook must be V3MigrationAcceptanceFaultHook"
            )
        if dry_run:
            raise ValueError(
                "acceptance_fault_hook is unavailable for dry-run migrations"
            )
        acceptance_fault_hook.validate()
    if dry_run or not _mysql_dialect(engine):
        return _run_v3_migrations_unlocked(
            engine,
            dry_run=dry_run,
            connection=None,
            acceptance_fault_hook=acceptance_fault_hook,
        )
    with mysql_named_lock(
        engine,
        "probiga:trading_v3_schema",
        timeout_seconds=30,
    ) as connection:
        return _run_v3_migrations_unlocked(
            engine,
            dry_run=False,
            connection=connection,
            acceptance_fault_hook=acceptance_fault_hook,
        )


__all__ = [
    "MIGRATION_PROGRESS_TABLE",
    "MIGRATION_PROGRESS_TABLE_DDL",
    "MIGRATION_TABLE_DDL",
    "MIGRATIONS",
    "V3MigrationResult",
    "V3MigrationAcceptanceFault",
    "V3MigrationAcceptanceFaultHook",
    "V3_PROJECTION_OUTBOX_MIGRATION_VERSION",
    "run_v3_migrations",
]
