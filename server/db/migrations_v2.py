"""Versioned schema migrations for the isolated V2 paper-trading ledger.

These migrations are only called by ``tools/migrate_trading_v2.py``. API
startup, GET handlers, strategy scans and paper ticks never execute DDL.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from server.common.mysql_lock import mysql_named_lock
from server.common.mysql_metadata_compat import normalize_mysql_referential_rule
from server.common.mysql_version_policy import (
    is_isolated_acceptance_version,
    isolated_acceptance_versions_label,
)
from server.db.accounting_evidence_ddl import (
    ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL,
    ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL,
)


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class V2MigrationResult:
    version: str
    status: str
    statement_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


V2_MIGRATION_FAULT_AFTER_DDL_COMMIT = "AFTER_DDL_COMMIT"
V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE = "BEFORE_LEDGER_WRITE"


class V2MigrationAcceptanceFault(RuntimeError):
    """Intentional, test-only interruption at a committed migration boundary."""

    def __init__(
        self,
        *,
        version: str,
        phase: str,
        committed_statement_count: int | None,
    ) -> None:
        self.version = version
        self.phase = phase
        self.committed_statement_count = committed_statement_count
        suffix = (
            ""
            if committed_statement_count is None
            else f" after {committed_statement_count} committed statements"
        )
        super().__init__(
            f"intentional V2 migration acceptance fault: {version} {phase}{suffix}"
        )


class V2MigrationAcceptanceFaultHook:
    """One-shot fault hook available only through an explicit runner argument.

    It can target only opt-in execution-evidence migrations and only safe
    boundaries where a DDL statement has already committed or immediately
    before the migration ledger insert.  Ordinary migration calls pass no hook.
    """

    __slots__ = (
        "version",
        "phase",
        "committed_statement_count",
        "_triggered",
    )

    def __init__(
        self,
        *,
        version: str,
        phase: str,
        committed_statement_count: int | None = None,
    ) -> None:
        if version not in _EVIDENCE_MIGRATION_VERSIONS:
            raise ValueError(
                "acceptance fault hook may target only opt-in V2 evidence migrations"
            )
        if phase not in {
            V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
            V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
        }:
            raise ValueError("unsupported V2 migration acceptance fault phase")
        if phase == V2_MIGRATION_FAULT_AFTER_DDL_COMMIT:
            if (
                type(committed_statement_count) is not int
                or committed_statement_count < 1
            ):
                raise ValueError(
                    "AFTER_DDL_COMMIT requires committed_statement_count >= 1"
                )
        elif committed_statement_count is not None:
            raise ValueError(
                "BEFORE_LEDGER_WRITE does not accept committed_statement_count"
            )
        self.version = version
        self.phase = phase
        self.committed_statement_count = committed_statement_count
        self._triggered = False

    @classmethod
    def after_ddl_commit(
        cls,
        version: str,
        committed_statement_count: int,
    ) -> "V2MigrationAcceptanceFaultHook":
        return cls(
            version=version,
            phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
            committed_statement_count=committed_statement_count,
        )

    @classmethod
    def before_ledger_write(
        cls,
        version: str,
    ) -> "V2MigrationAcceptanceFaultHook":
        return cls(
            version=version,
            phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
        )

    @property
    def triggered(self) -> bool:
        return self._triggered

    def _validate_declaration(
        self,
        migrations: tuple[dict[str, Any], ...],
    ) -> None:
        if self._triggered:
            raise RuntimeError(
                "a triggered V2 migration acceptance fault hook cannot be reused"
            )
        migration = next(
            (
                item
                for item in migrations
                if str(item["version"]) == self.version
            ),
            None,
        )
        if migration is None:
            raise RuntimeError("acceptance fault target migration is not declared")
        statement_count = len(tuple(migration["statements"]))
        if (
            self.phase == V2_MIGRATION_FAULT_AFTER_DDL_COMMIT
            and int(self.committed_statement_count or 0) > statement_count
        ):
            raise ValueError(
                "acceptance fault committed_statement_count exceeds migration DDL"
            )

    def _raise_if_matches(
        self,
        *,
        version: str,
        phase: str,
        committed_statement_count: int | None,
    ) -> None:
        if self._triggered or version != self.version or phase != self.phase:
            return
        if (
            phase == V2_MIGRATION_FAULT_AFTER_DDL_COMMIT
            and committed_statement_count != self.committed_statement_count
        ):
            return
        self._triggered = True
        raise V2MigrationAcceptanceFault(
            version=version,
            phase=phase,
            committed_statement_count=committed_statement_count,
        )


MIGRATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration_v2 (
    version VARCHAR(80) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

V2_EVIDENCE_MAINTENANCE_FENCE_TABLE = (
    "schema_migration_v2_maintenance_fence"
)
V2_EVIDENCE_MAINTENANCE_FENCE_NAME = "execution_evidence_011_015"
V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE = "ACTIVE"
V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE = "INACTIVE"
V2_EVIDENCE_MAINTENANCE_FENCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} (
    fence_name VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    state VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_version VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    activated_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
"""


MIGRATIONS: tuple[dict[str, Any], ...] = (
    {
        "version": "20260725_001_trading_v2_core",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_trade_account_v2 (
                account_id VARCHAR(64) PRIMARY KEY,
                account_name VARCHAR(120) NOT NULL,
                status VARCHAR(40) NOT NULL,
                initial_cash DECIMAL(20,2) NOT NULL,
                cash_balance DECIMAL(20,2) NOT NULL,
                peak_equity DECIMAL(20,2) NOT NULL,
                policy_version VARCHAR(80) NOT NULL,
                policy_hash CHAR(64) NOT NULL,
                fee_profile_version VARCHAR(80) DEFAULT NULL,
                instrument_rule_version VARCHAR(80) DEFAULT NULL,
                real_trading_enabled TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CHECK (initial_cash >= 0),
                CHECK (cash_balance >= 0),
                CHECK (real_trading_enabled = 0)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_strategy_version_v2 (
                strategy_id VARCHAR(80) NOT NULL,
                version VARCHAR(80) NOT NULL,
                lifecycle_status VARCHAR(32) NOT NULL,
                instrument_scope VARCHAR(120) NOT NULL,
                manifest_json LONGTEXT NOT NULL,
                config_hash CHAR(64) NOT NULL,
                code_commit_sha VARCHAR(64) NOT NULL,
                validation_json LONGTEXT,
                promoted_at DATETIME DEFAULT NULL,
                suspended_at DATETIME DEFAULT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (strategy_id, version),
                KEY idx_strategy_v2_lifecycle (lifecycle_status, strategy_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_data_snapshot_v2 (
                snapshot_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                decision_at DATETIME NOT NULL,
                source_manifest_json LONGTEXT NOT NULL,
                data_snapshot_hash CHAR(64) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                blocked_capabilities_json LONGTEXT,
                code_commit_sha VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_data_snapshot_v2_hash (data_snapshot_hash),
                KEY idx_data_snapshot_v2_date (trade_date, decision_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_decision_run_v2 (
                run_uid VARCHAR(64) PRIMARY KEY,
                run_idempotency_key CHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                decision_at DATETIME NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                snapshot_id VARCHAR(64) NOT NULL,
                market_regime VARCHAR(32) NOT NULL,
                market_regime_version VARCHAR(80) NOT NULL,
                portfolio_policy_version VARCHAR(80) NOT NULL,
                config_version VARCHAR(80) NOT NULL,
                code_commit_sha VARCHAR(64) NOT NULL,
                account_state_hash CHAR(64) NOT NULL,
                random_seed BIGINT DEFAULT NULL,
                status VARCHAR(32) NOT NULL,
                result_hash CHAR(64) DEFAULT NULL,
                started_at DATETIME NOT NULL,
                finished_at DATETIME DEFAULT NULL,
                error_code VARCHAR(80) DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                UNIQUE KEY uk_decision_run_v2_idempotency (run_idempotency_key),
                KEY idx_decision_run_v2_date (trade_date, started_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_strategy_signal_v2 (
                run_uid VARCHAR(64) NOT NULL,
                strategy_version VARCHAR(80) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                action VARCHAR(24) NOT NULL,
                lifecycle_status VARCHAR(32) NOT NULL,
                raw_features_json LONGTEXT NOT NULL,
                raw_score DECIMAL(18,8) DEFAULT NULL,
                expected_return_net DECIMAL(18,8) DEFAULT NULL,
                expected_return_lower_bound DECIMAL(18,8) DEFAULT NULL,
                expected_return_source VARCHAR(160) DEFAULT NULL,
                initial_stop DECIMAL(20,6) DEFAULT NULL,
                invalidation_condition VARCHAR(1000) NOT NULL,
                risk_reward_ratio DECIMAL(18,8) DEFAULT NULL,
                valid_from DATETIME NOT NULL,
                valid_until DATETIME NOT NULL,
                data_snapshot_hash CHAR(64) NOT NULL,
                config_hash CHAR(64) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                competition_status VARCHAR(40) NOT NULL,
                rejection_code VARCHAR(100) DEFAULT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (run_uid, strategy_version, stock_code),
                KEY idx_strategy_signal_v2_stock (stock_code, valid_from)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_portfolio_plan_v2 (
                run_uid VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                plan_version INT NOT NULL,
                market_regime VARCHAR(32) NOT NULL,
                target_cash DECIMAL(20,2) NOT NULL,
                target_risk_asset_weight DECIMAL(18,8) NOT NULL,
                positions_json LONGTEXT NOT NULL,
                rejected_candidates_json LONGTEXT NOT NULL,
                worst_case_loss DECIMAL(20,2) NOT NULL,
                theme_exposure_json LONGTEXT NOT NULL,
                result_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (run_uid, account_id),
                UNIQUE KEY uk_portfolio_plan_v2_version (account_id, run_uid, plan_version)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_trade_intent_v2 (
                intent_id VARCHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                decision_run_uid VARCHAR(64) NOT NULL,
                strategy_version VARCHAR(80) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                action VARCHAR(16) NOT NULL,
                current_quantity BIGINT NOT NULL,
                target_quantity BIGINT NOT NULL,
                target_weight DECIMAL(18,8) NOT NULL,
                earliest_at DATETIME NOT NULL,
                expires_at DATETIME NOT NULL,
                limit_price DECIMAL(20,6) NOT NULL,
                worst_price DECIMAL(20,6) NOT NULL,
                initial_stop DECIMAL(20,6) NOT NULL,
                protective_stop DECIMAL(20,6) NOT NULL,
                invalidation_condition VARCHAR(1000) NOT NULL,
                reason_code VARCHAR(100) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                intent_version INT NOT NULL,
                idempotency_key CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_trade_intent_v2_idempotency (idempotency_key),
                KEY idx_trade_intent_v2_account (account_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_risk_decision_v2 (
                intent_id VARCHAR(64) PRIMARY KEY,
                decision_status VARCHAR(16) NOT NULL,
                requested_quantity BIGINT NOT NULL,
                approved_quantity BIGINT NOT NULL,
                trade_risk DECIMAL(20,2) NOT NULL,
                post_single_weight DECIMAL(18,8) NOT NULL,
                post_total_weight DECIMAL(18,8) NOT NULL,
                post_theme_weight DECIMAL(18,8) NOT NULL,
                post_open_risk DECIMAL(20,2) NOT NULL,
                post_cash DECIMAL(20,2) NOT NULL,
                checks_json LONGTEXT NOT NULL,
                first_failure VARCHAR(100) DEFAULT NULL,
                decision_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_order_v2 (
                order_id VARCHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                intent_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                side VARCHAR(8) NOT NULL,
                order_type VARCHAR(16) NOT NULL,
                limit_price DECIMAL(20,6) NOT NULL,
                quantity BIGINT NOT NULL,
                filled_quantity BIGINT NOT NULL DEFAULT 0,
                status VARCHAR(32) NOT NULL,
                waiting_reason VARCHAR(40) DEFAULT NULL,
                earliest_at DATETIME NOT NULL,
                expires_at DATETIME NOT NULL,
                idempotency_key CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_order_v2_idempotency (idempotency_key),
                KEY idx_order_v2_active (account_id, status, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_fill_v2 (
                fill_id VARCHAR(64) PRIMARY KEY,
                order_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                side VARCHAR(8) NOT NULL,
                quantity BIGINT NOT NULL,
                price DECIMAL(20,6) NOT NULL,
                gross_amount DECIMAL(20,2) NOT NULL,
                fee_amount DECIMAL(20,2) NOT NULL,
                net_cash_amount DECIMAL(20,2) NOT NULL,
                quote_event_id VARCHAR(120) NOT NULL,
                match_event_id VARCHAR(120) NOT NULL,
                idempotency_key CHAR(64) NOT NULL,
                filled_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_fill_v2_idempotency (idempotency_key),
                KEY idx_fill_v2_account (account_id, filled_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_position_lot_v2 (
                lot_id VARCHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                strategy_version VARCHAR(80) NOT NULL,
                opened_fill_id VARCHAR(64) NOT NULL,
                opened_trade_date DATE NOT NULL,
                settlement_date DATE NOT NULL,
                original_quantity BIGINT NOT NULL,
                remaining_quantity BIGINT NOT NULL,
                cost_price DECIMAL(20,6) NOT NULL,
                allocated_buy_fee DECIMAL(20,2) NOT NULL,
                position_state VARCHAR(40) NOT NULL,
                approved_target_quantity BIGINT NOT NULL,
                add_count INT NOT NULL DEFAULT 0,
                initial_stop DECIMAL(20,6) NOT NULL,
                protective_stop DECIMAL(20,6) NOT NULL,
                invalidation_condition VARCHAR(1000) NOT NULL,
                version INT NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL,
                closed_at DATETIME DEFAULT NULL,
                KEY idx_position_lot_v2_open (account_id, stock_code, remaining_quantity)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_cash_ledger_v2 (
                cash_event_id VARCHAR(64) PRIMARY KEY,
                account_id VARCHAR(64) NOT NULL,
                business_event_key VARCHAR(160) NOT NULL,
                event_type VARCHAR(40) NOT NULL,
                amount DECIMAL(20,2) NOT NULL,
                balance_after DECIMAL(20,2) NOT NULL,
                related_order_id VARCHAR(64) DEFAULT NULL,
                related_fill_id VARCHAR(64) DEFAULT NULL,
                reversal_of VARCHAR(64) DEFAULT NULL,
                occurred_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_cash_ledger_v2_business (business_event_key),
                KEY idx_cash_ledger_v2_account (account_id, occurred_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_equity_daily_v2 (
                account_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                cash_balance DECIMAL(20,2) NOT NULL,
                market_value DECIMAL(20,2) NOT NULL,
                receivables DECIMAL(20,2) NOT NULL,
                payables DECIMAL(20,2) NOT NULL,
                total_equity DECIMAL(20,2) NOT NULL,
                peak_equity DECIMAL(20,2) NOT NULL,
                drawdown DECIMAL(18,8) NOT NULL,
                price_snapshot_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (account_id, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_reconciliation_v2 (
                account_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                version INT NOT NULL,
                status VARCHAR(32) NOT NULL,
                cash_difference DECIMAL(20,2) NOT NULL,
                equity_difference DECIMAL(20,2) NOT NULL,
                position_difference BIGINT NOT NULL,
                order_difference BIGINT NOT NULL,
                fill_difference BIGINT NOT NULL,
                checks_json LONGTEXT NOT NULL,
                reconciliation_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (account_id, trade_date, version),
                KEY idx_reconciliation_v2_status (status, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_trade_event_v2 (
                event_id VARCHAR(64) PRIMARY KEY,
                trace_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) DEFAULT NULL,
                event_type VARCHAR(80) NOT NULL,
                entity_type VARCHAR(40) NOT NULL,
                entity_id VARCHAR(80) NOT NULL,
                event_payload_json LONGTEXT NOT NULL,
                payload_hash CHAR(64) NOT NULL,
                occurred_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                KEY idx_trade_event_v2_trace (trace_id, occurred_at),
                KEY idx_trade_event_v2_entity (entity_type, entity_id, occurred_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260725_002_trading_v2_jobs_and_lifecycle",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_job_v2 (
                job_id VARCHAR(64) PRIMARY KEY,
                job_type VARCHAR(40) NOT NULL,
                idempotency_key CHAR(64) NOT NULL,
                request_json LONGTEXT NOT NULL,
                status VARCHAR(24) NOT NULL,
                result_ref VARCHAR(80) DEFAULT NULL,
                error_code VARCHAR(80) DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                requested_by VARCHAR(80) NOT NULL,
                requested_at DATETIME NOT NULL,
                started_at DATETIME DEFAULT NULL,
                finished_at DATETIME DEFAULT NULL,
                UNIQUE KEY uk_job_v2_idempotency (idempotency_key),
                KEY idx_job_v2_status (status, requested_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_strategy_lifecycle_event_v2 (
                event_id VARCHAR(64) PRIMARY KEY,
                strategy_id VARCHAR(80) NOT NULL,
                strategy_version VARCHAR(80) NOT NULL,
                previous_status VARCHAR(32) NOT NULL,
                next_status VARCHAR(32) NOT NULL,
                reason VARCHAR(500) NOT NULL,
                operator_name VARCHAR(80) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                event_hash CHAR(64) NOT NULL,
                occurred_at DATETIME NOT NULL,
                UNIQUE KEY uk_strategy_lifecycle_v2_hash (event_hash),
                KEY idx_strategy_lifecycle_v2_strategy
                    (strategy_id, strategy_version, occurred_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260725_003_trading_v2_execution_research_ops",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_quote_event_v2 (
                quote_event_id CHAR(64) PRIMARY KEY,
                stock_code VARCHAR(16) NOT NULL,
                quote_at DATETIME NOT NULL,
                received_at DATETIME NOT NULL,
                bid1 DECIMAL(20,6) DEFAULT NULL,
                bid1_volume BIGINT DEFAULT NULL,
                ask1 DECIMAL(20,6) DEFAULT NULL,
                ask1_volume BIGINT DEFAULT NULL,
                last_price DECIMAL(20,6) DEFAULT NULL,
                pre_close DECIMAL(20,6) DEFAULT NULL,
                upper_limit DECIMAL(20,6) DEFAULT NULL,
                lower_limit DECIMAL(20,6) DEFAULT NULL,
                suspended TINYINT(1) NOT NULL DEFAULT 0,
                source_provider VARCHAR(80) NOT NULL,
                source_batch_id VARCHAR(120) NOT NULL,
                payload_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_quote_event_v2_payload (payload_hash),
                KEY idx_quote_event_v2_latest (stock_code, quote_at),
                KEY idx_quote_event_v2_received (received_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_capability_v2 (
                capability_code VARCHAR(100) PRIMARY KEY,
                status VARCHAR(16) NOT NULL,
                protocol_version VARCHAR(80) NOT NULL,
                consecutive_trade_days INT NOT NULL DEFAULT 0,
                evidence_json LONGTEXT NOT NULL,
                checked_at DATETIME NOT NULL,
                passed_at DATETIME DEFAULT NULL,
                updated_at DATETIME NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_fee_profile_v2 (
                fee_profile_version VARCHAR(80) NOT NULL,
                effective_from DATE NOT NULL,
                effective_to DATE DEFAULT NULL,
                security_type VARCHAR(40) NOT NULL,
                buy_commission_rate DECIMAL(18,10) NOT NULL,
                sell_commission_rate DECIMAL(18,10) NOT NULL,
                minimum_commission DECIMAL(20,2) NOT NULL,
                stamp_tax_sell_rate DECIMAL(18,10) NOT NULL,
                transfer_fee_buy_rate DECIMAL(18,10) NOT NULL,
                transfer_fee_sell_rate DECIMAL(18,10) NOT NULL,
                other_fee_json LONGTEXT NOT NULL,
                evidence_hash CHAR(64) NOT NULL,
                confirmation_status VARCHAR(32) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY
                    (fee_profile_version, security_type, effective_from),
                UNIQUE KEY uk_fee_profile_v2_evidence (evidence_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_instrument_rule_v2 (
                stock_code VARCHAR(16) NOT NULL,
                rule_version VARCHAR(80) NOT NULL,
                effective_from DATE NOT NULL,
                effective_to DATE DEFAULT NULL,
                security_type VARCHAR(40) NOT NULL,
                exchange_code VARCHAR(16) NOT NULL,
                can_buy TINYINT(1) NOT NULL,
                first_buy_minimum BIGINT NOT NULL,
                buy_lot_size BIGINT NOT NULL,
                sell_lot_size BIGINT NOT NULL,
                settlement_days INT NOT NULL,
                tick_size DECIMAL(20,6) NOT NULL,
                limit_ratio DECIMAL(18,8) DEFAULT NULL,
                special_treatment TINYINT(1) NOT NULL DEFAULT 0,
                suspended TINYINT(1) NOT NULL DEFAULT 0,
                permission_required VARCHAR(80) NOT NULL,
                permission_confirmed TINYINT(1) NOT NULL DEFAULT 0,
                fee_profile_version VARCHAR(80) NOT NULL,
                source_snapshot_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (stock_code, rule_version, effective_from),
                KEY idx_instrument_rule_v2_effective
                    (stock_code, effective_from, effective_to)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_backtest_run_v2 (
                backtest_uid VARCHAR(64) PRIMARY KEY,
                strategy_version VARCHAR(160) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                random_seed BIGINT NOT NULL,
                status VARCHAR(32) NOT NULL,
                request_hash CHAR(64) NOT NULL,
                data_snapshot_hash CHAR(64) NOT NULL,
                code_commit_sha VARCHAR(64) NOT NULL,
                config_hash CHAR(64) NOT NULL,
                protocol_version VARCHAR(80) NOT NULL,
                result_json LONGTEXT,
                result_hash CHAR(64) DEFAULT NULL,
                gate_status VARCHAR(32) NOT NULL,
                error_code VARCHAR(80) DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                started_at DATETIME NOT NULL,
                finished_at DATETIME DEFAULT NULL,
                UNIQUE KEY uk_backtest_v2_request (request_hash),
                KEY idx_backtest_v2_strategy
                    (strategy_version, start_date, end_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_backtest_trade_v2 (
                backtest_uid VARCHAR(64) NOT NULL,
                trade_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                entry_date DATE NOT NULL,
                exit_date DATE NOT NULL,
                quantity BIGINT NOT NULL,
                buy_fill_amount DECIMAL(20,2) NOT NULL,
                sell_fill_amount DECIMAL(20,2) NOT NULL,
                buy_fees DECIMAL(20,2) NOT NULL,
                sell_fees DECIMAL(20,2) NOT NULL,
                initial_risk_amount DECIMAL(20,2) NOT NULL,
                trade_net_pnl DECIMAL(20,2) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (backtest_uid, trade_id),
                KEY idx_backtest_trade_v2_stock (stock_code, exit_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_strategy_health_daily_v2 (
                strategy_version VARCHAR(160) NOT NULL,
                trade_date DATE NOT NULL,
                window_days INT NOT NULL,
                completed_trades INT NOT NULL,
                expectancy_cny DECIMAL(20,6) DEFAULT NULL,
                profit_factor DECIMAL(20,8) DEFAULT NULL,
                max_drawdown DECIMAL(18,8) NOT NULL,
                health_status VARCHAR(16) NOT NULL,
                action_code VARCHAR(80) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                result_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (strategy_version, trade_date, window_days),
                KEY idx_strategy_health_v2_status
                    (health_status, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_fault_drill_v2 (
                drill_id VARCHAR(64) PRIMARY KEY,
                drill_type VARCHAR(80) NOT NULL,
                environment VARCHAR(32) NOT NULL,
                planned_at DATETIME NOT NULL,
                started_at DATETIME DEFAULT NULL,
                finished_at DATETIME DEFAULT NULL,
                status VARCHAR(24) NOT NULL,
                expected_action VARCHAR(500) NOT NULL,
                observed_action VARCHAR(500) DEFAULT NULL,
                evidence_json LONGTEXT NOT NULL,
                result_hash CHAR(64) DEFAULT NULL,
                created_at DATETIME NOT NULL,
                KEY idx_fault_drill_v2_type (drill_type, planned_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_worker_heartbeat_v2 (
                worker_name VARCHAR(80) PRIMARY KEY,
                worker_instance VARCHAR(120) NOT NULL,
                status VARCHAR(24) NOT NULL,
                current_job_id VARCHAR(64) DEFAULT NULL,
                last_success_at DATETIME DEFAULT NULL,
                last_error_code VARCHAR(80) DEFAULT NULL,
                last_error_message VARCHAR(500) DEFAULT NULL,
                heartbeat_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260725_004_trading_v2_etf_truth_and_forward",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS si_etf_code (
                etf_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                exchange VARCHAR(8) NOT NULL,
                asset_class VARCHAR(32) NOT NULL,
                list_date DATE DEFAULT NULL,
                last_trade_date DATE DEFAULT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'active',
                primary_source VARCHAR(32) NOT NULL,
                validation_source VARCHAR(32) NOT NULL,
                sync_status VARCHAR(16) NOT NULL,
                updated_at DATETIME NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (etf_code),
                KEY idx_si_etf_code_status (status, asset_class)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS sm_etf_kline (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                etf_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                trade_time DATETIME NOT NULL,
                trade_date DATE NOT NULL,
                k_type TINYINT NOT NULL DEFAULT 1,
                adjust_type TINYINT NOT NULL,
                `open` DECIMAL(18,6) NOT NULL,
                `close` DECIMAL(18,6) NOT NULL,
                high DECIMAL(18,6) NOT NULL,
                low DECIMAL(18,6) NOT NULL,
                volume DECIMAL(24,4) NOT NULL,
                amount DECIMAL(24,4) DEFAULT NULL,
                pre_close DECIMAL(18,6) DEFAULT NULL,
                `change` DECIMAL(18,6) DEFAULT NULL,
                change_pct DECIMAL(18,8) DEFAULT NULL,
                data_source VARCHAR(32) NOT NULL,
                validation_source VARCHAR(32) NOT NULL,
                validation_status VARCHAR(16) NOT NULL,
                validation_price_max_delta DECIMAL(18,8) DEFAULT NULL,
                validation_volume_delta_pct DECIMAL(18,8) DEFAULT NULL,
                validation_checked_at DATETIME NOT NULL,
                received_at DATETIME NOT NULL,
                batch_id VARCHAR(64) NOT NULL,
                data_version CHAR(64) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                permission_status VARCHAR(16) NOT NULL DEFAULT 'public',
                PRIMARY KEY (id),
                UNIQUE KEY uk_sm_etf_kline_bar
                    (etf_code, trade_date, k_type, adjust_type),
                KEY idx_sm_etf_kline_date (trade_date),
                KEY idx_sm_etf_kline_code_date (etf_code, trade_date),
                KEY idx_sm_etf_kline_quality
                    (validation_status, quality_status, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_etf_forward_strategy (
                strategy_version VARCHAR(80) PRIMARY KEY,
                config_hash CHAR(64) NOT NULL,
                frozen_at DATETIME NOT NULL,
                forward_start_date DATE NOT NULL,
                mode VARCHAR(40) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'registered',
                config_json LONGTEXT NOT NULL,
                registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_etf_forward_config_hash (config_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_etf_forward_observation (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                strategy_version VARCHAR(80) NOT NULL,
                config_hash CHAR(64) NOT NULL,
                data_date DATE NOT NULL,
                observed_at DATETIME NOT NULL,
                data_source VARCHAR(40) NOT NULL,
                input_hash CHAR(64) NOT NULL,
                signal_type VARCHAR(40) NOT NULL,
                execution_date DATE DEFAULT NULL,
                target_json LONGTEXT NOT NULL,
                context_json LONGTEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_etf_forward_observation
                    (strategy_version, data_date),
                KEY idx_etf_forward_observation_date (data_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260725_005_trading_v2_theme_risk_chain",
        "statements": (
            """
            ALTER TABLE st_strategy_signal_v2
            ADD COLUMN theme_code VARCHAR(80) NOT NULL DEFAULT ''
            AFTER stock_code
            """,
            """
            ALTER TABLE st_trade_intent_v2
            ADD COLUMN theme_code VARCHAR(80) NOT NULL DEFAULT ''
            AFTER stock_code
            """,
            """
            ALTER TABLE st_position_lot_v2
            ADD COLUMN theme_code VARCHAR(80) NOT NULL DEFAULT ''
            AFTER stock_code
            """,
            """
            CREATE INDEX idx_position_lot_v2_theme
            ON st_position_lot_v2
                (account_id, theme_code, remaining_quantity)
            """,
        ),
    },
    {
        "version": "20260726_006_real_trading_hard_guard",
        "statements": (
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
        "version": "20260726_007_market_regime_transition_state",
        "statements": (
            """
            ALTER TABLE st_decision_run_v2
            ADD COLUMN market_regime_candidate VARCHAR(32)
            NOT NULL DEFAULT ''
            AFTER market_regime_version
            """,
            """
            ALTER TABLE st_decision_run_v2
            ADD COLUMN market_regime_candidate_streak INT
            NOT NULL DEFAULT 1
            AFTER market_regime_candidate
            """,
            """
            ALTER TABLE st_decision_run_v2
            ADD COLUMN market_regime_state_days INT
            NOT NULL DEFAULT 1
            AFTER market_regime_candidate_streak
            """,
            """
            ALTER TABLE st_decision_run_v2
            ADD COLUMN market_regime_cooldown_remaining INT
            NOT NULL DEFAULT 0
            AFTER market_regime_state_days
            """,
            """
            ALTER TABLE st_decision_run_v2
            ADD COLUMN market_regime_evidence_json LONGTEXT
            AFTER market_regime_cooldown_remaining
            """,
        ),
    },
    {
        "version": "20260727_008_intraday_dynamic_activation",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_intraday_market_state_v2 (
                state_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                observed_at DATETIME NOT NULL,
                decision_run_uid VARCHAR(64) NOT NULL,
                previous_market_regime VARCHAR(32) NOT NULL,
                state VARCHAR(40) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                actionable TINYINT(1) NOT NULL DEFAULT 0,
                observed_count INT NOT NULL,
                expected_count INT NOT NULL,
                coverage DECIMAL(18,8) NOT NULL,
                positive_breadth_pct DECIMAL(18,8) NOT NULL,
                equal_weight_return_pct DECIMAL(18,8) NOT NULL,
                median_return_pct DECIMAL(18,8) NOT NULL,
                confirming_points INT NOT NULL,
                source_provider VARCHAR(120) NOT NULL,
                config_version VARCHAR(80) NOT NULL,
                config_hash CHAR(64) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_intraday_market_state_minute
                    (trade_date, observed_at, config_hash),
                KEY idx_intraday_market_state_latest
                    (trade_date, observed_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_intraday_activation_v2 (
                activation_id VARCHAR(64) PRIMARY KEY,
                state_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                decision_run_uid VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                observed_at DATETIME NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                theme_code VARCHAR(80) NOT NULL,
                theme_name VARCHAR(160) NOT NULL,
                source_strategy_version VARCHAR(80) NOT NULL,
                role VARCHAR(40) NOT NULL,
                action VARCHAR(32) NOT NULL,
                status VARCHAR(32) NOT NULL,
                reason_code VARCHAR(100) NOT NULL,
                current_price DECIMAL(20,6) NOT NULL,
                current_return_pct DECIMAL(18,8) NOT NULL,
                relative_strength_pct DECIMAL(18,8) NOT NULL,
                intraday_amount_ratio DECIMAL(18,8) NOT NULL,
                theme_positive_breadth_pct DECIMAL(18,8) NOT NULL,
                theme_average_return_pct DECIMAL(18,8) NOT NULL,
                raw_score DECIMAL(18,8) NOT NULL,
                risk_reward_ratio DECIMAL(18,8) NOT NULL,
                leader_code VARCHAR(16) NOT NULL,
                leader_state VARCHAR(40) NOT NULL,
                opening_target_fraction DECIMAL(18,8) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                intent_id VARCHAR(64) DEFAULT NULL,
                order_id VARCHAR(64) DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                UNIQUE KEY uk_intraday_activation_candidate
                    (state_id, stock_code),
                KEY idx_intraday_activation_latest
                    (account_id, trade_date, observed_at),
                KEY idx_intraday_activation_status
                    (status, trade_date, observed_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_qmt_minute_sync_receipt_v2 (
                receipt_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                first_trade_time DATETIME NOT NULL,
                last_trade_time DATETIME NOT NULL,
                expected_count INT NOT NULL,
                observed_count INT NOT NULL,
                coverage DECIMAL(18,8) NOT NULL,
                row_count BIGINT NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_qmt_minute_receipt_window
                    (trade_date, first_trade_time, last_trade_time,
                     source_provider),
                KEY idx_qmt_minute_receipt_latest
                    (trade_date, last_trade_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_intraday_watch_quote_v2 (
                observation_id VARCHAR(64) PRIMARY KEY,
                state_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                observed_at DATETIME NOT NULL,
                source_quote_at DATETIME NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                price DECIMAL(20,6) NOT NULL,
                pre_close DECIMAL(20,6) NOT NULL,
                volume DECIMAL(24,4) NOT NULL,
                amount DECIMAL(24,4) NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_intraday_watch_quote
                    (trade_date, observed_at, stock_code),
                KEY idx_intraday_watch_quote_series
                    (stock_code, trade_date, observed_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260730_009_public_quote_failover",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_public_quote_current_v2 (
                stock_code VARCHAR(16) PRIMARY KEY,
                batch_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                quote_at DATETIME NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                price DECIMAL(20,6) NOT NULL,
                pre_close DECIMAL(20,6) NOT NULL,
                change_pct DECIMAL(18,8) NOT NULL,
                volume DECIMAL(24,4) NOT NULL,
                amount DECIMAL(24,4) NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                source_count INT NOT NULL,
                provider_mask VARCHAR(160) NOT NULL,
                price_deviation_pct DECIMAL(18,8) NOT NULL,
                received_at DATETIME NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                KEY idx_public_quote_current_batch
                    (batch_id, quote_at),
                KEY idx_public_quote_current_quality
                    (quality_status, quote_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_public_quote_receipt_v2 (
                batch_id VARCHAR(64) PRIMARY KEY,
                trade_date DATE NOT NULL,
                quote_at DATETIME NOT NULL,
                received_at DATETIME NOT NULL,
                expected_count INT NOT NULL,
                observed_count INT NOT NULL,
                coverage DECIMAL(18,8) NOT NULL,
                provider_count INT NOT NULL,
                minimum_sources_per_symbol INT NOT NULL,
                agreement_ratio DECIMAL(18,8) NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                maximum_price_deviation_pct DECIMAL(18,8) NOT NULL,
                maximum_source_latency_seconds DECIMAL(18,4) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                provider_status_json LONGTEXT NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                KEY idx_public_quote_receipt_latest
                    (trade_date, quote_at, quality_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260730_010_qmt_end_to_end_health",
        "statements": (
            """
            ALTER TABLE st_qmt_minute_sync_receipt_v2
            ADD COLUMN capture_mode VARCHAR(32) NOT NULL
                DEFAULT 'LEGACY_UNCLASSIFIED'
                AFTER source_provider
            """,
            """
            ALTER TABLE st_qmt_minute_sync_receipt_v2
            ADD COLUMN forward_eligible TINYINT(1) NOT NULL
                DEFAULT 0
                AFTER capture_mode
            """,
            """
            ALTER TABLE st_qmt_minute_sync_receipt_v2
            DROP INDEX uk_qmt_minute_receipt_window
            """,
            """
            ALTER TABLE st_qmt_minute_sync_receipt_v2
            ADD UNIQUE KEY uk_qmt_minute_receipt_window (
                trade_date, first_trade_time, last_trade_time,
                source_provider, capture_mode
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS st_qmt_realtime_sync_receipt_v2 (
                receipt_id VARCHAR(64) PRIMARY KEY,
                source_provider VARCHAR(80) NOT NULL,
                source_snapshot_token VARCHAR(128) NOT NULL,
                source_full_file_token VARCHAR(160) NOT NULL,
                source_generated_at DATETIME NOT NULL,
                heartbeat_at DATETIME NOT NULL,
                expected_count INT NOT NULL,
                observed_count INT NOT NULL,
                coverage DECIMAL(18,8) NOT NULL,
                published_at DATETIME NOT NULL,
                capture_mode VARCHAR(32) NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                evidence_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_qmt_realtime_source_snapshot
                    (source_provider, source_snapshot_token),
                KEY idx_qmt_realtime_receipt_latest
                    (capture_mode, quality_status, published_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260803_011_v2_execution_evidence_bindings",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_market_calendar_evidence_v2 (
                calendar_evidence_id CHAR(64) PRIMARY KEY,
                market_code VARCHAR(16) NOT NULL,
                trade_date DATE NOT NULL,
                calendar_version VARCHAR(80) NOT NULL,
                market_timezone VARCHAR(64) NOT NULL,
                calendar_payload_json LONGTEXT NOT NULL,
                calendar_payload_hash CHAR(64) NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                source_payload_json LONGTEXT NOT NULL,
                source_payload_hash CHAR(64) NOT NULL,
                source_receipt_id VARCHAR(128) DEFAULT NULL,
                source_receipt_hash CHAR(64) DEFAULT NULL,
                available_at DATETIME NOT NULL,
                history_origin VARCHAR(40) NOT NULL,
                history_origin_id VARCHAR(128) DEFAULT NULL,
                history_origin_at DATETIME DEFAULT NULL,
                authority_status VARCHAR(40) NOT NULL,
                authority_receipt_hash CHAR(64) DEFAULT NULL,
                evidence_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_calendar_evidence_v2_hash (evidence_hash),
                UNIQUE KEY uk_calendar_evidence_v2_binding
                    (calendar_evidence_id, evidence_hash),
                KEY idx_calendar_evidence_v2_date
                    (market_code, trade_date, available_at),
                CHECK (history_origin IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')),
                CHECK (authority_status IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY',
                     'EXTERNAL_RECEIPT_VERIFIED')),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR authority_receipt_hash IS NOT NULL),
                CHECK (authority_status = 'EXTERNAL_RECEIPT_VERIFIED'
                    OR authority_receipt_hash IS NULL),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR (source_receipt_hash IS NOT NULL
                        AND authority_receipt_hash = source_receipt_hash)),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR source_receipt_hash = source_payload_hash),
                CHECK ((source_receipt_id IS NULL) =
                    (source_receipt_hash IS NULL)),
                CHECK (JSON_VALID(calendar_payload_json)),
                CHECK (JSON_VALID(source_payload_json)),
                CHECK ((history_origin = 'UNKNOWN'
                        AND history_origin_id IS NULL
                        AND history_origin_at IS NULL)
                    OR (history_origin <> 'UNKNOWN'
                        AND history_origin_id IS NOT NULL
                        AND history_origin_at IS NOT NULL))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_quote_receipt_evidence_v2 (
                quote_evidence_id CHAR(64) PRIMARY KEY,
                quote_event_id CHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                trade_date DATE NOT NULL,
                market_timezone VARCHAR(64) NOT NULL,
                quote_at DATETIME NOT NULL,
                received_at DATETIME NOT NULL,
                available_at DATETIME NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                source_batch_id VARCHAR(120) NOT NULL,
                source_payload_hash CHAR(64) NOT NULL,
                source_receipt_type VARCHAR(40) NOT NULL,
                source_receipt_id VARCHAR(128) DEFAULT NULL,
                source_receipt_hash CHAR(64) DEFAULT NULL,
                receipt_payload_json LONGTEXT NOT NULL,
                receipt_payload_hash CHAR(64) NOT NULL,
                history_origin VARCHAR(40) NOT NULL,
                history_origin_id VARCHAR(128) DEFAULT NULL,
                history_origin_at DATETIME DEFAULT NULL,
                authority_status VARCHAR(40) NOT NULL,
                authority_receipt_hash CHAR(64) DEFAULT NULL,
                evidence_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_quote_evidence_v2_event (quote_event_id),
                UNIQUE KEY uk_quote_evidence_v2_hash (evidence_hash),
                UNIQUE KEY uk_quote_evidence_v2_binding
                    (quote_evidence_id, quote_event_id, evidence_hash),
                KEY idx_quote_evidence_v2_time
                    (stock_code, quote_at, available_at),
                CONSTRAINT fk_quote_evidence_v2_event
                    FOREIGN KEY (quote_event_id)
                    REFERENCES st_quote_event_v2 (quote_event_id),
                CHECK (history_origin IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')),
                CHECK (authority_status IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY',
                     'EXTERNAL_RECEIPT_VERIFIED')),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR authority_receipt_hash IS NOT NULL),
                CHECK (authority_status = 'EXTERNAL_RECEIPT_VERIFIED'
                    OR authority_receipt_hash IS NULL),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR (source_receipt_hash IS NOT NULL
                        AND authority_receipt_hash = source_receipt_hash)),
                CHECK (authority_status <> 'EXTERNAL_RECEIPT_VERIFIED'
                    OR source_receipt_hash = receipt_payload_hash),
                CHECK ((source_receipt_id IS NULL) =
                    (source_receipt_hash IS NULL)),
                CHECK (quote_event_id = source_payload_hash),
                CHECK (source_receipt_type IN
                    ('NONE', 'QMT_MINUTE', 'QMT_REALTIME',
                     'PUBLIC_CONSENSUS', 'OTHER')),
                CHECK ((source_receipt_type = 'NONE'
                        AND source_receipt_id IS NULL)
                    OR (source_receipt_type <> 'NONE'
                        AND source_receipt_id IS NOT NULL)),
                CHECK (quote_at <= received_at AND received_at <= available_at),
                CHECK (JSON_VALID(receipt_payload_json)),
                CHECK ((history_origin = 'UNKNOWN'
                        AND history_origin_id IS NULL
                        AND history_origin_at IS NULL)
                    OR (history_origin <> 'UNKNOWN'
                        AND history_origin_id IS NOT NULL
                        AND history_origin_at IS NOT NULL))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_fill_execution_evidence_v2 (
                fill_execution_evidence_id CHAR(64) PRIMARY KEY,
                fill_id VARCHAR(64) NOT NULL,
                order_id VARCHAR(64) NOT NULL,
                order_fill_sequence BIGINT NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                fill_payload_json LONGTEXT NOT NULL,
                fill_payload_hash CHAR(64) NOT NULL,
                order_payload_json LONGTEXT NOT NULL,
                order_payload_hash CHAR(64) NOT NULL,
                quote_event_id CHAR(64) NOT NULL,
                quote_evidence_id CHAR(64) NOT NULL,
                quote_evidence_hash CHAR(64) NOT NULL,
                calendar_evidence_id CHAR(64) NOT NULL,
                calendar_evidence_hash CHAR(64) NOT NULL,
                fee_profile_version VARCHAR(80) NOT NULL,
                fee_security_type VARCHAR(40) NOT NULL,
                fee_effective_from DATE NOT NULL,
                fee_effective_to DATE DEFAULT NULL,
                fee_created_at DATETIME NOT NULL,
                fee_schedule_json LONGTEXT NOT NULL,
                fee_schedule_hash CHAR(64) NOT NULL,
                instrument_rule_version VARCHAR(80) NOT NULL,
                instrument_rule_effective_from DATE NOT NULL,
                instrument_rule_effective_to DATE DEFAULT NULL,
                instrument_rule_created_at DATETIME NOT NULL,
                instrument_rule_json LONGTEXT NOT NULL,
                instrument_rule_hash CHAR(64) NOT NULL,
                matcher_version VARCHAR(80) NOT NULL,
                matcher_request_json LONGTEXT NOT NULL,
                matcher_request_hash CHAR(64) NOT NULL,
                matcher_response_json LONGTEXT NOT NULL,
                matcher_output_hash CHAR(64) NOT NULL,
                accounting_request_json LONGTEXT NOT NULL,
                accounting_request_hash CHAR(64) NOT NULL,
                settlement_evidence_json LONGTEXT NOT NULL,
                settlement_evidence_hash CHAR(64) NOT NULL,
                executed_at DATETIME NOT NULL,
                bound_at DATETIME NOT NULL,
                history_origin VARCHAR(40) NOT NULL,
                history_origin_id VARCHAR(128) DEFAULT NULL,
                history_origin_at DATETIME DEFAULT NULL,
                authority_status VARCHAR(40) NOT NULL,
                authority_receipt_hash CHAR(64) DEFAULT NULL,
                evidence_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_fill_execution_evidence_v2_fill (fill_id),
                UNIQUE KEY uk_fill_execution_evidence_v2_sequence
                    (order_id, order_fill_sequence),
                UNIQUE KEY uk_fill_execution_evidence_v2_hash (evidence_hash),
                UNIQUE KEY uk_fill_execution_evidence_v2_binding
                    (fill_execution_evidence_id, fill_id, evidence_hash),
                KEY idx_fill_execution_evidence_v2_order (order_id, executed_at),
                CONSTRAINT fk_fill_execution_evidence_v2_fill
                    FOREIGN KEY (fill_id) REFERENCES st_fill_v2 (fill_id),
                CONSTRAINT fk_fill_execution_evidence_v2_order
                    FOREIGN KEY (order_id) REFERENCES st_order_v2 (order_id),
                CONSTRAINT fk_fill_execution_evidence_v2_quote
                    FOREIGN KEY (
                        quote_evidence_id, quote_event_id,
                        quote_evidence_hash
                    ) REFERENCES st_quote_receipt_evidence_v2 (
                        quote_evidence_id, quote_event_id, evidence_hash
                    ),
                CONSTRAINT fk_fill_execution_evidence_v2_calendar
                    FOREIGN KEY (calendar_evidence_id, calendar_evidence_hash)
                    REFERENCES st_market_calendar_evidence_v2
                        (calendar_evidence_id, evidence_hash),
                CONSTRAINT fk_fill_execution_evidence_v2_fee
                    FOREIGN KEY (
                        fee_profile_version, fee_security_type,
                        fee_effective_from
                    ) REFERENCES st_fee_profile_v2 (
                        fee_profile_version, security_type, effective_from
                    ),
                CONSTRAINT fk_fill_execution_evidence_v2_rule
                    FOREIGN KEY (
                        stock_code, instrument_rule_version,
                        instrument_rule_effective_from
                    ) REFERENCES st_instrument_rule_v2 (
                        stock_code, rule_version, effective_from
                    ),
                CHECK (order_fill_sequence >= 1),
                CHECK (fee_effective_to IS NULL
                    OR fee_effective_to >= fee_effective_from),
                CHECK (instrument_rule_effective_to IS NULL
                    OR instrument_rule_effective_to >= instrument_rule_effective_from),
                CHECK (fee_created_at <= executed_at),
                CHECK (instrument_rule_created_at <= executed_at),
                CHECK (executed_at <= bound_at),
                CHECK (JSON_VALID(fill_payload_json)),
                CHECK (JSON_VALID(order_payload_json)),
                CHECK (JSON_VALID(fee_schedule_json)),
                CHECK (JSON_VALID(instrument_rule_json)),
                CHECK (JSON_VALID(matcher_request_json)),
                CHECK (JSON_VALID(matcher_response_json)),
                CHECK (JSON_VALID(accounting_request_json)),
                CHECK (JSON_VALID(settlement_evidence_json)),
                CHECK (history_origin IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')),
                CHECK (authority_status IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')),
                CHECK (authority_receipt_hash IS NULL),
                CHECK ((history_origin = 'UNKNOWN'
                        AND history_origin_id IS NULL
                        AND history_origin_at IS NULL)
                    OR (history_origin <> 'UNKNOWN'
                        AND history_origin_id IS NOT NULL
                        AND history_origin_at IS NOT NULL))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_cash_event_binding_v2 (
                cash_binding_id CHAR(64) PRIMARY KEY,
                cash_event_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                account_sequence BIGINT NOT NULL,
                cash_event_type VARCHAR(40) NOT NULL,
                related_order_id VARCHAR(64) DEFAULT NULL,
                related_fill_id VARCHAR(64) DEFAULT NULL,
                reversal_of VARCHAR(64) DEFAULT NULL,
                fill_execution_evidence_id CHAR(64) DEFAULT NULL,
                fill_execution_evidence_hash CHAR(64) DEFAULT NULL,
                previous_cash_event_id VARCHAR(64) DEFAULT NULL,
                previous_binding_id CHAR(64) DEFAULT NULL,
                previous_binding_hash CHAR(64) DEFAULT NULL,
                cash_event_payload_json LONGTEXT NOT NULL,
                cash_event_payload_hash CHAR(64) NOT NULL,
                occurred_at DATETIME NOT NULL,
                bound_at DATETIME NOT NULL,
                history_origin VARCHAR(40) NOT NULL,
                history_origin_id VARCHAR(128) DEFAULT NULL,
                history_origin_at DATETIME DEFAULT NULL,
                authority_status VARCHAR(40) NOT NULL,
                authority_receipt_hash CHAR(64) DEFAULT NULL,
                binding_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_cash_binding_v2_event (cash_event_id),
                UNIQUE KEY uk_cash_binding_v2_sequence
                    (account_id, account_sequence),
                UNIQUE KEY uk_cash_binding_v2_hash (binding_hash),
                UNIQUE KEY uk_cash_binding_v2_binding
                    (cash_binding_id, cash_event_id, binding_hash),
                CONSTRAINT fk_cash_binding_v2_event
                    FOREIGN KEY (cash_event_id)
                    REFERENCES st_cash_ledger_v2 (cash_event_id),
                CONSTRAINT fk_cash_binding_v2_order
                    FOREIGN KEY (related_order_id)
                    REFERENCES st_order_v2 (order_id),
                CONSTRAINT fk_cash_binding_v2_fill
                    FOREIGN KEY (related_fill_id)
                    REFERENCES st_fill_v2 (fill_id),
                CONSTRAINT fk_cash_binding_v2_reversal
                    FOREIGN KEY (reversal_of)
                    REFERENCES st_cash_ledger_v2 (cash_event_id),
                CONSTRAINT fk_cash_binding_v2_fill_evidence
                    FOREIGN KEY (
                        fill_execution_evidence_id, related_fill_id,
                        fill_execution_evidence_hash
                    ) REFERENCES st_fill_execution_evidence_v2 (
                        fill_execution_evidence_id, fill_id, evidence_hash
                    ),
                CONSTRAINT fk_cash_binding_v2_previous_event
                    FOREIGN KEY (previous_cash_event_id)
                    REFERENCES st_cash_ledger_v2 (cash_event_id),
                CONSTRAINT fk_cash_binding_v2_previous_binding
                    FOREIGN KEY (
                        previous_binding_id, previous_cash_event_id,
                        previous_binding_hash
                    ) REFERENCES st_cash_event_binding_v2 (
                        cash_binding_id, cash_event_id, binding_hash
                    ),
                CHECK (account_sequence >= 0),
                CHECK (cash_event_type IN
                    ('INITIAL_DEPOSIT', 'BUY_FILL', 'SELL_FILL')),
                CHECK (reversal_of IS NULL),
                CHECK ((cash_event_type IN ('BUY_FILL', 'SELL_FILL')
                        AND related_order_id IS NOT NULL
                        AND related_fill_id IS NOT NULL
                        AND fill_execution_evidence_id IS NOT NULL)
                    OR (cash_event_type NOT IN ('BUY_FILL', 'SELL_FILL')
                        AND related_fill_id IS NULL
                        AND fill_execution_evidence_id IS NULL)),
                CHECK ((fill_execution_evidence_id IS NULL) =
                    (fill_execution_evidence_hash IS NULL)),
                CHECK (fill_execution_evidence_id IS NULL
                    OR related_fill_id IS NOT NULL),
                CHECK ((previous_cash_event_id IS NULL) =
                    (previous_binding_id IS NULL)),
                CHECK ((previous_binding_id IS NULL) =
                    (previous_binding_hash IS NULL)),
                CHECK (account_sequence <> 0
                    OR (previous_cash_event_id IS NULL
                        AND previous_binding_id IS NULL)),
                CHECK (history_origin <> 'COMPLETE_FROM_DECLARED_ORIGIN'
                    OR account_sequence <> 0
                    OR (cash_event_type = 'INITIAL_DEPOSIT'
                        AND related_order_id IS NULL
                        AND related_fill_id IS NULL
                        AND reversal_of IS NULL)),
                CHECK (history_origin <> 'COMPLETE_FROM_DECLARED_ORIGIN'
                    OR account_sequence = 0
                    OR previous_binding_id IS NOT NULL),
                CHECK (occurred_at <= bound_at),
                CHECK (JSON_VALID(cash_event_payload_json)),
                CHECK (history_origin IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')),
                CHECK (authority_status IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')),
                CHECK (authority_receipt_hash IS NULL),
                CHECK ((history_origin = 'UNKNOWN'
                        AND history_origin_id IS NULL
                        AND history_origin_at IS NULL)
                    OR (history_origin <> 'UNKNOWN'
                        AND history_origin_id IS NOT NULL
                        AND history_origin_at IS NOT NULL))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_order_transition_v2 (
                transition_id CHAR(64) PRIMARY KEY,
                order_id VARCHAR(64) NOT NULL,
                account_id VARCHAR(64) NOT NULL,
                order_payload_json LONGTEXT NOT NULL,
                order_payload_hash CHAR(64) NOT NULL,
                transition_sequence BIGINT NOT NULL,
                previous_transition_id CHAR(64) DEFAULT NULL,
                previous_transition_hash CHAR(64) DEFAULT NULL,
                from_status VARCHAR(32) NOT NULL,
                to_status VARCHAR(32) NOT NULL,
                previous_filled_quantity BIGINT NOT NULL,
                next_filled_quantity BIGINT NOT NULL,
                waiting_reason VARCHAR(40) DEFAULT NULL,
                transition_kind VARCHAR(40) NOT NULL,
                related_fill_id VARCHAR(64) DEFAULT NULL,
                fill_execution_evidence_id CHAR(64) DEFAULT NULL,
                fill_execution_evidence_hash CHAR(64) DEFAULT NULL,
                source_event_type VARCHAR(80) NOT NULL,
                source_event_id VARCHAR(128) NOT NULL,
                source_event_hash CHAR(64) NOT NULL,
                occurred_at DATETIME NOT NULL,
                recorded_at DATETIME NOT NULL,
                history_origin VARCHAR(40) NOT NULL,
                history_origin_id VARCHAR(128) DEFAULT NULL,
                history_origin_at DATETIME DEFAULT NULL,
                authority_status VARCHAR(40) NOT NULL,
                authority_receipt_hash CHAR(64) DEFAULT NULL,
                transition_hash CHAR(64) NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE KEY uk_order_transition_v2_sequence
                    (order_id, transition_sequence),
                UNIQUE KEY uk_order_transition_v2_source
                    (order_id, source_event_type, source_event_id),
                UNIQUE KEY uk_order_transition_v2_hash (transition_hash),
                UNIQUE KEY uk_order_transition_v2_binding
                    (transition_id, transition_hash),
                CONSTRAINT fk_order_transition_v2_order
                    FOREIGN KEY (order_id) REFERENCES st_order_v2 (order_id),
                CONSTRAINT fk_order_transition_v2_previous
                    FOREIGN KEY (
                        previous_transition_id, previous_transition_hash
                    ) REFERENCES st_order_transition_v2 (
                        transition_id, transition_hash
                    ),
                CONSTRAINT fk_order_transition_v2_fill
                    FOREIGN KEY (related_fill_id)
                    REFERENCES st_fill_v2 (fill_id),
                CONSTRAINT fk_order_transition_v2_fill_evidence
                    FOREIGN KEY (
                        fill_execution_evidence_id, related_fill_id,
                        fill_execution_evidence_hash
                    ) REFERENCES st_fill_execution_evidence_v2 (
                        fill_execution_evidence_id, fill_id, evidence_hash
                    ),
                CHECK (transition_sequence >= 0),
                CHECK (previous_filled_quantity >= 0),
                CHECK (next_filled_quantity >= previous_filled_quantity),
                CHECK ((previous_transition_id IS NULL) =
                    (previous_transition_hash IS NULL)),
                CHECK (JSON_VALID(order_payload_json)),
                CHECK (from_status IN
                    ('CREATED', 'RISK_APPROVED', 'QUEUED',
                     'PARTIALLY_FILLED', 'FILLED', 'REJECTED',
                     'CANCELLED', 'EXPIRED')),
                CHECK (to_status IN
                    ('CREATED', 'RISK_APPROVED', 'QUEUED',
                     'PARTIALLY_FILLED', 'FILLED', 'REJECTED',
                     'CANCELLED', 'EXPIRED')),
                CHECK ((fill_execution_evidence_id IS NULL) =
                    (fill_execution_evidence_hash IS NULL)),
                CHECK (transition_kind IN
                    ('ORDER_CREATED', 'STATUS_CHANGE', 'FILL_APPLIED',
                     'WAITING_REASON_CHANGED')),
                CHECK ((transition_kind = 'FILL_APPLIED'
                        AND related_fill_id IS NOT NULL
                        AND fill_execution_evidence_id IS NOT NULL
                        AND next_filled_quantity > previous_filled_quantity)
                    OR (transition_kind <> 'FILL_APPLIED'
                        AND related_fill_id IS NULL
                        AND fill_execution_evidence_id IS NULL
                        AND next_filled_quantity = previous_filled_quantity)),
                CHECK (transition_sequence <> 0
                    OR (previous_transition_id IS NULL
                        AND previous_transition_hash IS NULL)),
                CHECK (transition_kind <> 'ORDER_CREATED'
                    OR (transition_sequence = 0
                        AND from_status = 'CREATED'
                        AND to_status = 'CREATED'
                        AND previous_filled_quantity = 0
                        AND next_filled_quantity = 0
                        AND waiting_reason IS NULL
                        AND related_fill_id IS NULL)),
                CHECK (history_origin <> 'COMPLETE_FROM_DECLARED_ORIGIN'
                    OR transition_sequence <> 0
                    OR transition_kind = 'ORDER_CREATED'),
                CHECK (history_origin <> 'COMPLETE_FROM_DECLARED_ORIGIN'
                    OR transition_sequence = 0
                    OR previous_transition_id IS NOT NULL),
                CHECK (occurred_at <= recorded_at),
                CHECK (history_origin IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')),
                CHECK (authority_status IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')),
                CHECK (authority_receipt_hash IS NULL),
                CHECK ((history_origin = 'UNKNOWN'
                        AND history_origin_id IS NULL
                        AND history_origin_at IS NULL)
                    OR (history_origin <> 'UNKNOWN'
                        AND history_origin_id IS NOT NULL
                        AND history_origin_at IS NOT NULL))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260803_012_v2_execution_evidence_guards",
        "statements": (
            "DROP TRIGGER IF EXISTS trg_market_calendar_evidence_v2_guard_bi",
            """
            CREATE TRIGGER trg_market_calendar_evidence_v2_guard_bi
            BEFORE INSERT ON st_market_calendar_evidence_v2
            FOR EACH ROW
            BEGIN
                IF NEW.history_origin NOT IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid calendar history origin';
                END IF;
                IF NOT (
                    (NEW.history_origin = 'UNKNOWN'
                     AND NEW.history_origin_id IS NULL
                     AND NEW.history_origin_at IS NULL)
                    OR
                    (NEW.history_origin <> 'UNKNOWN'
                     AND NEW.history_origin_id IS NOT NULL
                     AND NEW.history_origin_at IS NOT NULL)
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid calendar history boundary';
                END IF;
                IF NEW.authority_status NOT IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY',
                     'EXTERNAL_RECEIPT_VERIFIED') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid calendar authority status';
                END IF;
                IF (NEW.source_receipt_id IS NULL)
                    <> (NEW.source_receipt_hash IS NULL) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'incomplete calendar receipt reference';
                END IF;
                IF NEW.authority_status = 'EXTERNAL_RECEIPT_VERIFIED' THEN
                    IF NEW.authority_receipt_hash IS NULL
                       OR NEW.source_receipt_hash IS NULL
                       OR BINARY NEW.authority_receipt_hash
                          <> BINARY NEW.source_receipt_hash
                       OR BINARY NEW.source_receipt_hash
                          <> BINARY NEW.source_payload_hash THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'calendar authority receipt mismatch';
                    END IF;
                ELSEIF NEW.authority_receipt_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'calendar authority receipt not allowed';
                END IF;
                IF JSON_VALID(NEW.calendar_payload_json) <> 1
                   OR JSON_VALID(NEW.source_payload_json) <> 1
                   OR COALESCE(JSON_TYPE(JSON_EXTRACT(
                        NEW.calendar_payload_json, '$.trading_days')), '')
                        <> 'ARRAY'
                   OR COALESCE(JSON_TYPE(JSON_EXTRACT(
                        NEW.calendar_payload_json, '$.sessions')), '')
                        <> 'ARRAY'
                   OR COALESCE(JSON_LENGTH(JSON_EXTRACT(
                        NEW.calendar_payload_json, '$.sessions')), 0) < 1
                   OR COALESCE(JSON_CONTAINS(
                        JSON_EXTRACT(
                            NEW.calendar_payload_json, '$.trading_days'),
                        JSON_QUOTE(DATE_FORMAT(NEW.trade_date, '%Y-%m-%d'))
                      ), 0) <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid calendar JSON evidence';
                END IF;
                IF NEW.history_origin_at IS NOT NULL
                   AND NEW.history_origin_at > NEW.available_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'calendar predates history origin';
                END IF;
                IF NEW.available_at > NEW.created_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'calendar created before availability';
                END IF;
                IF CHAR_LENGTH(NEW.calendar_evidence_id) <> 64
                   OR BINARY NEW.calendar_evidence_id
                      <> BINARY LOWER(NEW.calendar_evidence_id)
                   OR NEW.calendar_evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.calendar_payload_hash) <> 64
                   OR BINARY NEW.calendar_payload_hash
                      <> BINARY LOWER(NEW.calendar_payload_hash)
                   OR NEW.calendar_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.source_payload_hash) <> 64
                   OR BINARY NEW.source_payload_hash
                      <> BINARY LOWER(NEW.source_payload_hash)
                   OR NEW.source_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.evidence_hash) <> 64
                   OR BINARY NEW.evidence_hash
                      <> BINARY LOWER(NEW.evidence_hash)
                   OR NEW.evidence_hash REGEXP '[^0-9a-f]'
                   OR (NEW.source_receipt_hash IS NOT NULL AND (
                       CHAR_LENGTH(NEW.source_receipt_hash) <> 64
                       OR BINARY NEW.source_receipt_hash
                          <> BINARY LOWER(NEW.source_receipt_hash)
                       OR NEW.source_receipt_hash REGEXP '[^0-9a-f]'))
                   OR (NEW.authority_receipt_hash IS NOT NULL AND (
                       CHAR_LENGTH(NEW.authority_receipt_hash) <> 64
                       OR BINARY NEW.authority_receipt_hash
                          <> BINARY LOWER(NEW.authority_receipt_hash)
                       OR NEW.authority_receipt_hash REGEXP '[^0-9a-f]')) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid calendar SHA256 field';
                END IF;
                IF BINARY NEW.calendar_evidence_id
                   <> BINARY NEW.evidence_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'calendar identity hash mismatch';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_market_calendar_evidence_v2_guard_bu",
            """
            CREATE TRIGGER trg_market_calendar_evidence_v2_guard_bu
            BEFORE UPDATE ON st_market_calendar_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'calendar evidence is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_market_calendar_evidence_v2_guard_bd",
            """
            CREATE TRIGGER trg_market_calendar_evidence_v2_guard_bd
            BEFORE DELETE ON st_market_calendar_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'calendar evidence cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_quote_receipt_evidence_v2_guard_bi",
            """
            CREATE TRIGGER trg_quote_receipt_evidence_v2_guard_bi
            BEFORE INSERT ON st_quote_receipt_evidence_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                DECLARE receipt_count INT DEFAULT 0;
                IF NEW.history_origin NOT IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote history origin';
                END IF;
                IF NOT (
                    (NEW.history_origin = 'UNKNOWN'
                     AND NEW.history_origin_id IS NULL
                     AND NEW.history_origin_at IS NULL)
                    OR
                    (NEW.history_origin <> 'UNKNOWN'
                     AND NEW.history_origin_id IS NOT NULL
                     AND NEW.history_origin_at IS NOT NULL)
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote history boundary';
                END IF;
                IF NEW.authority_status NOT IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY',
                     'EXTERNAL_RECEIPT_VERIFIED') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote authority status';
                END IF;
                IF NEW.source_receipt_type NOT IN
                    ('NONE', 'QMT_MINUTE', 'QMT_REALTIME',
                     'PUBLIC_CONSENSUS', 'OTHER') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote receipt type';
                END IF;
                IF (NEW.source_receipt_id IS NULL)
                    <> (NEW.source_receipt_hash IS NULL) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'incomplete quote receipt reference';
                END IF;
                IF (NEW.source_receipt_type = 'NONE'
                    AND (NEW.source_receipt_id IS NOT NULL
                         OR BINARY NEW.receipt_payload_json <> BINARY '{}'))
                   OR (NEW.source_receipt_type <> 'NONE'
                       AND NEW.source_receipt_id IS NULL) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote receipt type/reference mismatch';
                END IF;
                IF NEW.authority_status = 'EXTERNAL_RECEIPT_VERIFIED' THEN
                    IF NEW.authority_receipt_hash IS NULL
                       OR NEW.source_receipt_hash IS NULL
                       OR BINARY NEW.authority_receipt_hash
                          <> BINARY NEW.source_receipt_hash
                       OR BINARY NEW.source_receipt_hash
                          <> BINARY NEW.receipt_payload_hash
                       OR NEW.source_receipt_type IN ('NONE', 'OTHER') THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'quote authority receipt mismatch';
                    END IF;
                ELSEIF NEW.authority_receipt_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote authority receipt not allowed';
                END IF;
                IF BINARY NEW.quote_event_id
                   <> BINARY NEW.source_payload_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote event/payload mismatch';
                END IF;
                IF NEW.quote_at > NEW.received_at
                   OR NEW.received_at > NEW.available_at
                   OR DATE(NEW.quote_at) <> NEW.trade_date
                   OR (NEW.history_origin_at IS NOT NULL
                       AND NEW.history_origin_at > NEW.quote_at)
                   OR NEW.available_at > NEW.created_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote knowledge-time order';
                END IF;
                IF JSON_VALID(NEW.receipt_payload_json) <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote receipt JSON';
                END IF;
                IF NEW.source_receipt_type <> 'NONE' AND (
                    NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.receipt_payload_json, '$.quote_event_id'))
                        <=> BINARY NEW.quote_event_id)
                    OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.receipt_payload_json, '$.source_payload_hash'))
                        <=> BINARY NEW.source_payload_hash)
                    OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.receipt_payload_json, '$.source_provider'))
                        <=> BINARY NEW.source_provider)
                    OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.receipt_payload_json, '$.source_batch_id'))
                        <=> BINARY NEW.source_batch_id)
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote receipt payload identity mismatch';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_quote_event_v2 q
                WHERE q.quote_event_id = NEW.quote_event_id
                  AND BINARY q.stock_code = BINARY NEW.stock_code
                  AND q.quote_at = NEW.quote_at
                  AND q.received_at = NEW.received_at
                  AND BINARY q.source_provider = BINARY NEW.source_provider
                  AND BINARY q.source_batch_id = BINARY NEW.source_batch_id
                  AND BINARY q.payload_hash = BINARY NEW.source_payload_hash
                  AND q.created_at <= NEW.created_at;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote evidence differs from parent quote';
                END IF;
                IF NEW.source_receipt_type = 'QMT_MINUTE' THEN
                    SELECT COUNT(*) INTO receipt_count
                    FROM st_qmt_minute_sync_receipt_v2 r
                    WHERE r.receipt_id = NEW.source_receipt_id
                      AND r.trade_date = NEW.trade_date
                      AND BINARY r.source_provider = BINARY NEW.source_provider
                      AND NEW.quote_at BETWEEN
                          r.first_trade_time AND r.last_trade_time
                      AND r.created_at <= NEW.available_at
                      AND (NEW.authority_status
                           <> 'EXTERNAL_RECEIPT_VERIFIED'
                           OR (r.quality_status = 'PASS'
                               AND r.forward_eligible = 1));
                ELSEIF NEW.source_receipt_type = 'QMT_REALTIME' THEN
                    SELECT COUNT(*) INTO receipt_count
                    FROM st_qmt_realtime_sync_receipt_v2 r
                    WHERE r.receipt_id = NEW.source_receipt_id
                      AND BINARY r.source_provider = BINARY NEW.source_provider
                      AND r.source_generated_at <= NEW.quote_at
                      AND r.heartbeat_at >= NEW.quote_at
                      AND r.published_at <= NEW.available_at
                      AND r.created_at <= NEW.available_at
                      AND (NEW.authority_status
                           <> 'EXTERNAL_RECEIPT_VERIFIED'
                           OR r.quality_status = 'PASS');
                ELSEIF NEW.source_receipt_type = 'PUBLIC_CONSENSUS' THEN
                    SELECT COUNT(*) INTO receipt_count
                    FROM st_public_quote_receipt_v2 r
                    WHERE r.batch_id = NEW.source_receipt_id
                      AND r.trade_date = NEW.trade_date
                      AND r.quote_at = NEW.quote_at
                      AND r.received_at = NEW.received_at
                      AND BINARY r.source_provider = BINARY NEW.source_provider
                      AND r.created_at <= NEW.available_at
                      AND (NEW.authority_status
                           <> 'EXTERNAL_RECEIPT_VERIFIED'
                           OR r.quality_status = 'PASS');
                ELSE
                    SET receipt_count = 1;
                END IF;
                IF receipt_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote receipt registry mismatch';
                END IF;
                IF CHAR_LENGTH(NEW.quote_evidence_id) <> 64
                   OR BINARY NEW.quote_evidence_id
                      <> BINARY LOWER(NEW.quote_evidence_id)
                   OR NEW.quote_evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.quote_event_id) <> 64
                   OR BINARY NEW.quote_event_id
                      <> BINARY LOWER(NEW.quote_event_id)
                   OR NEW.quote_event_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.source_payload_hash) <> 64
                   OR BINARY NEW.source_payload_hash
                      <> BINARY LOWER(NEW.source_payload_hash)
                   OR NEW.source_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.receipt_payload_hash) <> 64
                   OR BINARY NEW.receipt_payload_hash
                      <> BINARY LOWER(NEW.receipt_payload_hash)
                   OR NEW.receipt_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.evidence_hash) <> 64
                   OR BINARY NEW.evidence_hash
                      <> BINARY LOWER(NEW.evidence_hash)
                   OR NEW.evidence_hash REGEXP '[^0-9a-f]'
                   OR (NEW.source_receipt_hash IS NOT NULL AND (
                       CHAR_LENGTH(NEW.source_receipt_hash) <> 64
                       OR BINARY NEW.source_receipt_hash
                          <> BINARY LOWER(NEW.source_receipt_hash)
                       OR NEW.source_receipt_hash REGEXP '[^0-9a-f]'))
                   OR (NEW.authority_receipt_hash IS NOT NULL AND (
                       CHAR_LENGTH(NEW.authority_receipt_hash) <> 64
                       OR BINARY NEW.authority_receipt_hash
                          <> BINARY LOWER(NEW.authority_receipt_hash)
                       OR NEW.authority_receipt_hash REGEXP '[^0-9a-f]')) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid quote SHA256 field';
                END IF;
                IF BINARY NEW.quote_evidence_id
                   <> BINARY NEW.evidence_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'quote identity hash mismatch';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_quote_receipt_evidence_v2_guard_bu",
            """
            CREATE TRIGGER trg_quote_receipt_evidence_v2_guard_bu
            BEFORE UPDATE ON st_quote_receipt_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'quote evidence is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_quote_receipt_evidence_v2_guard_bd",
            """
            CREATE TRIGGER trg_quote_receipt_evidence_v2_guard_bd
            BEFORE DELETE ON st_quote_receipt_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'quote evidence cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_fill_execution_evidence_v2_guard_bi",
            """
            CREATE TRIGGER trg_fill_execution_evidence_v2_guard_bi
            BEFORE INSERT ON st_fill_execution_evidence_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                DECLARE market_count INT DEFAULT 0;
                DECLARE fee_count INT DEFAULT 0;
                DECLARE rule_count INT DEFAULT 0;
                IF NEW.order_fill_sequence < 1
                   OR NEW.executed_at > NEW.bound_at
                   OR NEW.fee_created_at > NEW.executed_at
                   OR NEW.instrument_rule_created_at > NEW.executed_at
                   OR (NEW.fee_effective_to IS NOT NULL
                       AND NEW.fee_effective_to < NEW.fee_effective_from)
                   OR (NEW.instrument_rule_effective_to IS NOT NULL
                       AND NEW.instrument_rule_effective_to
                           < NEW.instrument_rule_effective_from) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid fill evidence time or sequence';
                END IF;
                IF NEW.history_origin NOT IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')
                   OR NEW.authority_status NOT IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')
                   OR NEW.authority_receipt_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid fill history or authority';
                END IF;
                IF NOT (
                    (NEW.history_origin = 'UNKNOWN'
                     AND NEW.history_origin_id IS NULL
                     AND NEW.history_origin_at IS NULL)
                    OR
                    (NEW.history_origin <> 'UNKNOWN'
                     AND NEW.history_origin_id IS NOT NULL
                     AND NEW.history_origin_at IS NOT NULL)
                ) OR (NEW.history_origin_at IS NOT NULL
                      AND NEW.history_origin_at > NEW.executed_at)
                   OR NEW.bound_at > NEW.created_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid fill history boundary';
                END IF;
                IF JSON_VALID(NEW.fill_payload_json) <> 1
                   OR JSON_VALID(NEW.order_payload_json) <> 1
                   OR JSON_VALID(NEW.fee_schedule_json) <> 1
                   OR JSON_VALID(NEW.instrument_rule_json) <> 1
                   OR JSON_VALID(NEW.matcher_request_json) <> 1
                   OR JSON_VALID(NEW.matcher_response_json) <> 1
                   OR JSON_VALID(NEW.accounting_request_json) <> 1
                   OR JSON_VALID(NEW.settlement_evidence_json) <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid fill evidence JSON';
                END IF;
                IF NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.fill_id'))
                        <=> BINARY NEW.fill_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.order_id'))
                        <=> BINARY NEW.order_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.account_id'))
                        <=> BINARY NEW.account_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.stock_code'))
                        <=> BINARY NEW.stock_code)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.quote_event_id'))
                        <=> BINARY NEW.quote_event_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.order_id'))
                        <=> BINARY NEW.order_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.account_id'))
                        <=> BINARY NEW.account_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.stock_code'))
                        <=> BINARY NEW.stock_code)
                   OR NOT (JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.order_type')) <=> 'LIMIT') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill payload identity mismatch';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_fill_v2 f
                JOIN st_order_v2 o ON o.order_id = f.order_id
                WHERE f.fill_id = NEW.fill_id
                  AND f.order_id = NEW.order_id
                  AND f.account_id = NEW.account_id
                  AND BINARY f.stock_code = BINARY NEW.stock_code
                  AND f.quote_event_id = NEW.quote_event_id
                  AND f.filled_at = NEW.executed_at
                  AND f.created_at BETWEEN NEW.executed_at AND NEW.bound_at
                  AND JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.side')) = f.side
                  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.quantity')) AS UNSIGNED)
                        = f.quantity
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.match_event_id'))
                        = BINARY f.match_event_id
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fill_payload_json, '$.idempotency_key'))
                        = BINARY f.idempotency_key
                  AND o.order_id = NEW.order_id
                  AND o.account_id = NEW.account_id
                  AND BINARY o.stock_code = BINARY NEW.stock_code
                  AND o.order_type = 'LIMIT'
                  AND JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.side')) = o.side
                  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.quantity')) AS UNSIGNED)
                        = o.quantity
                  AND o.created_at <= NEW.executed_at
                  AND o.earliest_at <= NEW.executed_at
                  AND NEW.executed_at < o.expires_at;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill evidence differs from V2 facts';
                END IF;
                SELECT COUNT(*) INTO market_count
                FROM st_quote_receipt_evidence_v2 q
                JOIN st_market_calendar_evidence_v2 c
                  ON c.calendar_evidence_id = NEW.calendar_evidence_id
                 AND c.evidence_hash = NEW.calendar_evidence_hash
                WHERE q.quote_evidence_id = NEW.quote_evidence_id
                  AND q.quote_event_id = NEW.quote_event_id
                  AND q.evidence_hash = NEW.quote_evidence_hash
                  AND BINARY q.stock_code = BINARY NEW.stock_code
                  AND q.trade_date = c.trade_date
                  AND BINARY q.market_timezone = BINARY c.market_timezone
                  AND q.available_at <= NEW.executed_at
                  AND c.available_at <= NEW.executed_at
                  AND q.trade_date = DATE(NEW.executed_at);
                IF market_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill market evidence mismatch';
                END IF;
                SELECT COUNT(*) INTO fee_count
                FROM st_fee_profile_v2 f
                WHERE f.fee_profile_version = NEW.fee_profile_version
                  AND f.security_type = NEW.fee_security_type
                  AND f.effective_from = NEW.fee_effective_from
                  AND f.effective_to <=> NEW.fee_effective_to
                  AND f.created_at = NEW.fee_created_at
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fee_schedule_json, '$.fee_profile_version'))
                        = BINARY f.fee_profile_version
                  AND JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.fee_schedule_json, '$.security_type'))
                        = f.security_type
                  AND f.effective_from <= DATE(NEW.executed_at)
                  AND (f.effective_to IS NULL
                       OR f.effective_to >= DATE(NEW.executed_at));
                IF fee_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill fee profile identity mismatch';
                END IF;
                SELECT COUNT(*) INTO rule_count
                FROM st_instrument_rule_v2 r
                WHERE BINARY r.stock_code = BINARY NEW.stock_code
                  AND r.rule_version = NEW.instrument_rule_version
                  AND r.effective_from = NEW.instrument_rule_effective_from
                  AND r.effective_to <=> NEW.instrument_rule_effective_to
                  AND r.created_at = NEW.instrument_rule_created_at
                  AND r.fee_profile_version = NEW.fee_profile_version
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.instrument_rule_json, '$.stock_code'))
                        = BINARY r.stock_code
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.instrument_rule_json, '$.rule_version'))
                        = BINARY r.rule_version
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.instrument_rule_json, '$.fee_profile_version'))
                        = BINARY r.fee_profile_version
                  AND r.effective_from <= DATE(NEW.executed_at)
                  AND (r.effective_to IS NULL
                       OR r.effective_to >= DATE(NEW.executed_at));
                IF rule_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill instrument rule mismatch';
                END IF;
                IF CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
                   OR BINARY NEW.fill_execution_evidence_id
                      <> BINARY LOWER(NEW.fill_execution_evidence_id)
                   OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.fill_payload_hash) <> 64
                   OR BINARY NEW.fill_payload_hash
                      <> BINARY LOWER(NEW.fill_payload_hash)
                   OR NEW.fill_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.order_payload_hash) <> 64
                   OR BINARY NEW.order_payload_hash
                      <> BINARY LOWER(NEW.order_payload_hash)
                   OR NEW.order_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.quote_event_id) <> 64
                   OR BINARY NEW.quote_event_id
                      <> BINARY LOWER(NEW.quote_event_id)
                   OR NEW.quote_event_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.quote_evidence_id) <> 64
                   OR BINARY NEW.quote_evidence_id
                      <> BINARY LOWER(NEW.quote_evidence_id)
                   OR NEW.quote_evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.quote_evidence_hash) <> 64
                   OR BINARY NEW.quote_evidence_hash
                      <> BINARY LOWER(NEW.quote_evidence_hash)
                   OR NEW.quote_evidence_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.calendar_evidence_id) <> 64
                   OR BINARY NEW.calendar_evidence_id
                      <> BINARY LOWER(NEW.calendar_evidence_id)
                   OR NEW.calendar_evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.calendar_evidence_hash) <> 64
                   OR BINARY NEW.calendar_evidence_hash
                      <> BINARY LOWER(NEW.calendar_evidence_hash)
                   OR NEW.calendar_evidence_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.fee_schedule_hash) <> 64
                   OR BINARY NEW.fee_schedule_hash
                      <> BINARY LOWER(NEW.fee_schedule_hash)
                   OR NEW.fee_schedule_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.instrument_rule_hash) <> 64
                   OR BINARY NEW.instrument_rule_hash
                      <> BINARY LOWER(NEW.instrument_rule_hash)
                   OR NEW.instrument_rule_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.matcher_request_hash) <> 64
                   OR BINARY NEW.matcher_request_hash
                      <> BINARY LOWER(NEW.matcher_request_hash)
                   OR NEW.matcher_request_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.matcher_output_hash) <> 64
                   OR BINARY NEW.matcher_output_hash
                      <> BINARY LOWER(NEW.matcher_output_hash)
                   OR NEW.matcher_output_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.accounting_request_hash) <> 64
                   OR BINARY NEW.accounting_request_hash
                      <> BINARY LOWER(NEW.accounting_request_hash)
                   OR NEW.accounting_request_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.settlement_evidence_hash) <> 64
                   OR BINARY NEW.settlement_evidence_hash
                      <> BINARY LOWER(NEW.settlement_evidence_hash)
                   OR NEW.settlement_evidence_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.evidence_hash) <> 64
                   OR BINARY NEW.evidence_hash
                      <> BINARY LOWER(NEW.evidence_hash)
                   OR NEW.evidence_hash REGEXP '[^0-9a-f]' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid fill SHA256 field';
                END IF;
                IF BINARY NEW.fill_execution_evidence_id
                   <> BINARY NEW.evidence_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'fill evidence identity hash mismatch';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_fill_execution_evidence_v2_guard_bu",
            """
            CREATE TRIGGER trg_fill_execution_evidence_v2_guard_bu
            BEFORE UPDATE ON st_fill_execution_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'fill evidence is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_fill_execution_evidence_v2_guard_bd",
            """
            CREATE TRIGGER trg_fill_execution_evidence_v2_guard_bd
            BEFORE DELETE ON st_fill_execution_evidence_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'fill evidence cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_cash_event_binding_v2_guard_bi",
            """
            CREATE TRIGGER trg_cash_event_binding_v2_guard_bi
            BEFORE INSERT ON st_cash_event_binding_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                DECLARE fill_count INT DEFAULT 0;
                DECLARE previous_count INT DEFAULT 0;
                IF NEW.account_sequence < 0
                   OR NEW.cash_event_type NOT IN
                        ('INITIAL_DEPOSIT', 'BUY_FILL', 'SELL_FILL')
                   OR NEW.reversal_of IS NOT NULL
                   OR NEW.occurred_at > NEW.bound_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash binding core fields';
                END IF;
                IF (NEW.cash_event_type IN ('BUY_FILL', 'SELL_FILL') AND (
                        NEW.related_order_id IS NULL
                        OR NEW.related_fill_id IS NULL
                        OR NEW.fill_execution_evidence_id IS NULL))
                   OR (NEW.cash_event_type NOT IN ('BUY_FILL', 'SELL_FILL') AND (
                        NEW.related_fill_id IS NOT NULL
                        OR NEW.fill_execution_evidence_id IS NOT NULL))
                   OR (NEW.fill_execution_evidence_id IS NULL)
                        <> (NEW.fill_execution_evidence_hash IS NULL)
                   OR (NEW.previous_cash_event_id IS NULL)
                        <> (NEW.previous_binding_id IS NULL)
                   OR (NEW.previous_binding_id IS NULL)
                        <> (NEW.previous_binding_hash IS NULL)
                   OR (NEW.account_sequence = 0 AND (
                        NEW.previous_cash_event_id IS NOT NULL
                        OR NEW.previous_binding_id IS NOT NULL)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash binding references';
                END IF;
                IF NEW.cash_event_type = 'INITIAL_DEPOSIT'
                   AND NEW.account_sequence <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'initial deposit must be cash genesis';
                END IF;
                IF NEW.history_origin NOT IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')
                   OR NEW.authority_status NOT IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')
                   OR NEW.authority_receipt_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash history or authority';
                END IF;
                IF NOT (
                    (NEW.history_origin = 'UNKNOWN'
                     AND NEW.history_origin_id IS NULL
                     AND NEW.history_origin_at IS NULL)
                    OR
                    (NEW.history_origin <> 'UNKNOWN'
                     AND NEW.history_origin_id IS NOT NULL
                     AND NEW.history_origin_at IS NOT NULL)
                ) OR (NEW.history_origin_at IS NOT NULL
                      AND NEW.history_origin_at > NEW.occurred_at)
                   OR NEW.bound_at > NEW.created_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash history boundary';
                END IF;
                IF NEW.history_origin = 'COMPLETE_FROM_DECLARED_ORIGIN' THEN
                    IF (NEW.account_sequence = 0 AND (
                            NEW.cash_event_type <> 'INITIAL_DEPOSIT'
                            OR NEW.related_order_id IS NOT NULL
                            OR NEW.related_fill_id IS NOT NULL
                            OR NEW.reversal_of IS NOT NULL))
                       OR (NEW.account_sequence > 0
                           AND NEW.previous_binding_id IS NULL) THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'incomplete cash history claim';
                    END IF;
                END IF;
                IF JSON_VALID(NEW.cash_event_payload_json) <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash event JSON';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_cash_ledger_v2 c
                WHERE c.cash_event_id = NEW.cash_event_id
                  AND c.account_id = NEW.account_id
                  AND c.event_type = NEW.cash_event_type
                  AND c.related_order_id <=> NEW.related_order_id
                  AND c.related_fill_id <=> NEW.related_fill_id
                  AND c.reversal_of <=> NEW.reversal_of
                  AND c.occurred_at = NEW.occurred_at
                  AND c.created_at BETWEEN NEW.occurred_at AND NEW.bound_at
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.cash_event_id'))
                        = BINARY c.cash_event_id
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.account_id'))
                        = BINARY c.account_id
                  AND JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.event_type'))
                        = c.event_type
                  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.business_event_key'))
                        = BINARY c.business_event_key
                  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.amount'))
                        AS DECIMAL(20,2)) = c.amount
                  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.cash_event_payload_json, '$.balance_after'))
                        AS DECIMAL(20,2)) = c.balance_after;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'cash evidence differs from V2 fact';
                END IF;
                IF NEW.fill_execution_evidence_id IS NOT NULL THEN
                    SELECT COUNT(*) INTO fill_count
                    FROM st_fill_execution_evidence_v2 f
                    WHERE f.fill_execution_evidence_id
                            = NEW.fill_execution_evidence_id
                      AND f.fill_id = NEW.related_fill_id
                      AND f.evidence_hash = NEW.fill_execution_evidence_hash
                      AND f.order_id = NEW.related_order_id
                      AND f.account_id = NEW.account_id
                      AND f.executed_at = NEW.occurred_at
                      AND f.bound_at <= NEW.bound_at
                      AND CONCAT(
                            JSON_UNQUOTE(JSON_EXTRACT(
                                f.fill_payload_json, '$.side')),
                            '_FILL') = NEW.cash_event_type
                      AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                            NEW.cash_event_payload_json,
                            '$.business_event_key'))
                            = BINARY CONCAT(
                                'FILL:',
                                JSON_UNQUOTE(JSON_EXTRACT(
                                    f.fill_payload_json,
                                    '$.idempotency_key')));
                    IF fill_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'cash/fill evidence mismatch';
                    END IF;
                END IF;
                IF NEW.previous_binding_id IS NOT NULL THEN
                    SELECT COUNT(*) INTO previous_count
                    FROM st_cash_event_binding_v2 p
                    WHERE p.cash_binding_id = NEW.previous_binding_id
                      AND p.cash_event_id = NEW.previous_cash_event_id
                      AND p.binding_hash = NEW.previous_binding_hash
                      AND p.account_id = NEW.account_id
                      AND p.account_sequence + 1 = NEW.account_sequence
                      AND p.bound_at <= NEW.bound_at
                      AND p.history_origin = NEW.history_origin
                      AND p.history_origin_id <=> NEW.history_origin_id
                      AND p.history_origin_at <=> NEW.history_origin_at;
                    IF previous_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'cash predecessor mismatch';
                    END IF;
                END IF;
                IF CHAR_LENGTH(NEW.cash_binding_id) <> 64
                   OR BINARY NEW.cash_binding_id
                      <> BINARY LOWER(NEW.cash_binding_id)
                   OR NEW.cash_binding_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.cash_event_payload_hash) <> 64
                   OR BINARY NEW.cash_event_payload_hash
                      <> BINARY LOWER(NEW.cash_event_payload_hash)
                   OR NEW.cash_event_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.binding_hash) <> 64
                   OR BINARY NEW.binding_hash
                      <> BINARY LOWER(NEW.binding_hash)
                   OR NEW.binding_hash REGEXP '[^0-9a-f]'
                   OR (NEW.fill_execution_evidence_id IS NOT NULL AND (
                       CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
                       OR BINARY NEW.fill_execution_evidence_id
                          <> BINARY LOWER(NEW.fill_execution_evidence_id)
                       OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
                       OR CHAR_LENGTH(NEW.fill_execution_evidence_hash) <> 64
                       OR BINARY NEW.fill_execution_evidence_hash
                          <> BINARY LOWER(NEW.fill_execution_evidence_hash)
                       OR NEW.fill_execution_evidence_hash REGEXP '[^0-9a-f]'))
                   OR (NEW.previous_binding_id IS NOT NULL AND (
                       CHAR_LENGTH(NEW.previous_binding_id) <> 64
                       OR BINARY NEW.previous_binding_id
                          <> BINARY LOWER(NEW.previous_binding_id)
                       OR NEW.previous_binding_id REGEXP '[^0-9a-f]'
                       OR CHAR_LENGTH(NEW.previous_binding_hash) <> 64
                       OR BINARY NEW.previous_binding_hash
                          <> BINARY LOWER(NEW.previous_binding_hash)
                       OR NEW.previous_binding_hash REGEXP '[^0-9a-f]')) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid cash SHA256 field';
                END IF;
                IF BINARY NEW.cash_binding_id <> BINARY NEW.binding_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'cash binding identity hash mismatch';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_cash_event_binding_v2_guard_bu",
            """
            CREATE TRIGGER trg_cash_event_binding_v2_guard_bu
            BEFORE UPDATE ON st_cash_event_binding_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'cash evidence is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_cash_event_binding_v2_guard_bd",
            """
            CREATE TRIGGER trg_cash_event_binding_v2_guard_bd
            BEFORE DELETE ON st_cash_event_binding_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'cash evidence cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_order_transition_v2_guard_bi",
            """
            CREATE TRIGGER trg_order_transition_v2_guard_bi
            BEFORE INSERT ON st_order_transition_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                DECLARE fill_count INT DEFAULT 0;
                DECLARE previous_count INT DEFAULT 0;
                IF NEW.transition_sequence < 0
                   OR NEW.previous_filled_quantity < 0
                   OR NEW.next_filled_quantity < NEW.previous_filled_quantity
                   OR NEW.occurred_at > NEW.recorded_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order transition core fields';
                END IF;
                IF NEW.from_status NOT IN
                    ('CREATED', 'RISK_APPROVED', 'QUEUED',
                     'PARTIALLY_FILLED', 'FILLED', 'REJECTED',
                     'CANCELLED', 'EXPIRED')
                   OR NEW.to_status NOT IN
                    ('CREATED', 'RISK_APPROVED', 'QUEUED',
                     'PARTIALLY_FILLED', 'FILLED', 'REJECTED',
                     'CANCELLED', 'EXPIRED')
                   OR NEW.transition_kind NOT IN
                    ('ORDER_CREATED', 'STATUS_CHANGE', 'FILL_APPLIED',
                     'WAITING_REASON_CHANGED') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order transition state';
                END IF;
                IF (NEW.previous_transition_id IS NULL)
                    <> (NEW.previous_transition_hash IS NULL)
                   OR (NEW.fill_execution_evidence_id IS NULL)
                    <> (NEW.fill_execution_evidence_hash IS NULL)
                   OR (NEW.transition_sequence = 0 AND (
                        NEW.previous_transition_id IS NOT NULL
                        OR NEW.previous_transition_hash IS NOT NULL)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order transition references';
                END IF;
                IF (NEW.transition_kind = 'FILL_APPLIED' AND (
                        NEW.related_fill_id IS NULL
                        OR NEW.fill_execution_evidence_id IS NULL
                        OR NEW.next_filled_quantity
                            <= NEW.previous_filled_quantity
                        OR NEW.to_status NOT IN
                            ('PARTIALLY_FILLED', 'FILLED')))
                   OR (NEW.transition_kind <> 'FILL_APPLIED' AND (
                        NEW.related_fill_id IS NOT NULL
                        OR NEW.fill_execution_evidence_id IS NOT NULL
                        OR NEW.next_filled_quantity
                            <> NEW.previous_filled_quantity)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order fill transition';
                END IF;
                IF NEW.transition_kind = 'ORDER_CREATED' AND NOT (
                    NEW.transition_sequence = 0
                    AND NEW.from_status = 'CREATED'
                    AND NEW.to_status = 'CREATED'
                    AND NEW.previous_filled_quantity = 0
                    AND NEW.next_filled_quantity = 0
                    AND NEW.waiting_reason IS NULL
                    AND NEW.related_fill_id IS NULL
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order genesis transition';
                END IF;
                IF NEW.history_origin NOT IN
                    ('UNKNOWN', 'START_AFTER_UNKNOWN',
                     'COMPLETE_FROM_DECLARED_ORIGIN')
                   OR NEW.authority_status NOT IN
                    ('UNKNOWN', 'CONTENT_HASH_ONLY')
                   OR NEW.authority_receipt_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order history or authority';
                END IF;
                IF NOT (
                    (NEW.history_origin = 'UNKNOWN'
                     AND NEW.history_origin_id IS NULL
                     AND NEW.history_origin_at IS NULL)
                    OR
                    (NEW.history_origin <> 'UNKNOWN'
                     AND NEW.history_origin_id IS NOT NULL
                     AND NEW.history_origin_at IS NOT NULL)
                ) OR (NEW.history_origin_at IS NOT NULL
                      AND NEW.history_origin_at > NEW.occurred_at)
                   OR NEW.recorded_at > NEW.created_at THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order history boundary';
                END IF;
                IF NEW.history_origin = 'COMPLETE_FROM_DECLARED_ORIGIN' AND (
                    (NEW.transition_sequence = 0
                     AND NEW.transition_kind <> 'ORDER_CREATED')
                    OR (NEW.transition_sequence > 0
                        AND NEW.previous_transition_id IS NULL)
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'incomplete order history claim';
                END IF;
                IF JSON_VALID(NEW.order_payload_json) <> 1
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.order_id'))
                        <=> BINARY NEW.order_id)
                   OR NOT (BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.order_payload_json, '$.account_id'))
                        <=> BINARY NEW.account_id) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order payload identity';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_order_v2 o
                WHERE o.order_id = NEW.order_id
                  AND o.account_id = NEW.account_id
                  AND o.created_at <= NEW.occurred_at
                  AND o.quantity >= NEW.next_filled_quantity
                  AND (NEW.to_status <> 'FILLED'
                       OR NEW.next_filled_quantity = o.quantity)
                  AND (NEW.to_status <> 'PARTIALLY_FILLED'
                       OR (NEW.next_filled_quantity > 0
                           AND NEW.next_filled_quantity < o.quantity))
                  AND JSON_EXTRACT(NEW.order_payload_json, '$.quantity')
                        = o.quantity;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'transition differs from parent order';
                END IF;
                IF NEW.transition_kind = 'ORDER_CREATED' THEN
                    SELECT COUNT(*) INTO parent_count
                    FROM st_order_v2 o
                    WHERE o.order_id = NEW.order_id
                      AND o.created_at = NEW.occurred_at;
                    IF parent_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'order genesis time mismatch';
                    END IF;
                END IF;
                IF NEW.from_status <> NEW.to_status AND NOT (
                    (NEW.from_status = 'CREATED' AND NEW.to_status IN
                        ('RISK_APPROVED', 'REJECTED', 'CANCELLED', 'EXPIRED'))
                    OR (NEW.from_status = 'RISK_APPROVED' AND NEW.to_status IN
                        ('QUEUED', 'REJECTED', 'CANCELLED', 'EXPIRED'))
                    OR (NEW.from_status = 'QUEUED' AND NEW.to_status IN
                        ('PARTIALLY_FILLED', 'FILLED',
                         'CANCELLED', 'EXPIRED'))
                    OR (NEW.from_status = 'PARTIALLY_FILLED'
                        AND NEW.to_status IN
                        ('PARTIALLY_FILLED', 'FILLED',
                         'CANCELLED', 'EXPIRED'))
                ) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'illegal V2 order state transition';
                END IF;
                IF NEW.from_status = NEW.to_status
                   AND NEW.transition_kind NOT IN
                    ('ORDER_CREATED', 'FILL_APPLIED',
                     'WAITING_REASON_CHANGED') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid same-state transition';
                END IF;
                IF NEW.fill_execution_evidence_id IS NOT NULL THEN
                    SELECT COUNT(*) INTO fill_count
                    FROM st_fill_execution_evidence_v2 f
                    WHERE f.fill_execution_evidence_id
                            = NEW.fill_execution_evidence_id
                      AND f.fill_id = NEW.related_fill_id
                      AND f.evidence_hash = NEW.fill_execution_evidence_hash
                      AND f.order_id = NEW.order_id
                      AND f.account_id = NEW.account_id
                      AND f.order_payload_hash = NEW.order_payload_hash
                      AND f.executed_at = NEW.occurred_at
                      AND f.bound_at <= NEW.recorded_at
                      AND JSON_EXTRACT(f.fill_payload_json, '$.quantity')
                            = NEW.next_filled_quantity
                              - NEW.previous_filled_quantity;
                    IF fill_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'transition/fill evidence mismatch';
                    END IF;
                END IF;
                IF NEW.previous_transition_id IS NOT NULL THEN
                    SELECT COUNT(*) INTO previous_count
                    FROM st_order_transition_v2 p
                    WHERE p.transition_id = NEW.previous_transition_id
                      AND p.transition_hash = NEW.previous_transition_hash
                      AND p.order_id = NEW.order_id
                      AND p.account_id = NEW.account_id
                      AND p.transition_sequence + 1
                            = NEW.transition_sequence
                      AND p.to_status = NEW.from_status
                      AND p.next_filled_quantity
                            = NEW.previous_filled_quantity
                      AND p.order_payload_hash = NEW.order_payload_hash
                      AND p.recorded_at <= NEW.occurred_at
                      AND p.history_origin = NEW.history_origin
                      AND p.history_origin_id <=> NEW.history_origin_id
                      AND p.history_origin_at <=> NEW.history_origin_at;
                    IF previous_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'order transition predecessor mismatch';
                    END IF;
                END IF;
                IF CHAR_LENGTH(NEW.transition_id) <> 64
                   OR BINARY NEW.transition_id
                      <> BINARY LOWER(NEW.transition_id)
                   OR NEW.transition_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.order_payload_hash) <> 64
                   OR BINARY NEW.order_payload_hash
                      <> BINARY LOWER(NEW.order_payload_hash)
                   OR NEW.order_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.source_event_hash) <> 64
                   OR BINARY NEW.source_event_hash
                      <> BINARY LOWER(NEW.source_event_hash)
                   OR NEW.source_event_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.transition_hash) <> 64
                   OR BINARY NEW.transition_hash
                      <> BINARY LOWER(NEW.transition_hash)
                   OR NEW.transition_hash REGEXP '[^0-9a-f]'
                   OR (NEW.previous_transition_id IS NOT NULL AND (
                       CHAR_LENGTH(NEW.previous_transition_id) <> 64
                       OR BINARY NEW.previous_transition_id
                          <> BINARY LOWER(NEW.previous_transition_id)
                       OR NEW.previous_transition_id REGEXP '[^0-9a-f]'
                       OR CHAR_LENGTH(NEW.previous_transition_hash) <> 64
                       OR BINARY NEW.previous_transition_hash
                          <> BINARY LOWER(NEW.previous_transition_hash)
                       OR NEW.previous_transition_hash REGEXP '[^0-9a-f]'))
                   OR (NEW.fill_execution_evidence_id IS NOT NULL AND (
                       CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
                       OR BINARY NEW.fill_execution_evidence_id
                          <> BINARY LOWER(NEW.fill_execution_evidence_id)
                       OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
                       OR CHAR_LENGTH(NEW.fill_execution_evidence_hash) <> 64
                       OR BINARY NEW.fill_execution_evidence_hash
                          <> BINARY LOWER(NEW.fill_execution_evidence_hash)
                       OR NEW.fill_execution_evidence_hash REGEXP '[^0-9a-f]')) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid order transition SHA256';
                END IF;
                IF BINARY NEW.transition_id <> BINARY NEW.transition_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'transition identity hash mismatch';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_order_transition_v2_guard_bu",
            """
            CREATE TRIGGER trg_order_transition_v2_guard_bu
            BEFORE UPDATE ON st_order_transition_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'order transition is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_order_transition_v2_guard_bd",
            """
            CREATE TRIGGER trg_order_transition_v2_guard_bd
            BEFORE DELETE ON st_order_transition_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'order transition cannot be deleted';
            END
            """,
        ),
    },
    {
        "version": "20260803_013_v2_execution_evidence_natural_keys",
        "statements": (
            """
            ALTER TABLE st_market_calendar_evidence_v2
            ADD UNIQUE KEY uk_calendar_evidence_v2_natural
                (market_code, trade_date, calendar_version)
            """,
        ),
    },
    {
        "version": "20260803_014_v2_execution_authority_attestations",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_execution_authority_trust_key_v2 (
                source_provider VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_version VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                algorithm VARCHAR(16) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                public_key BINARY(32) NOT NULL,
                public_key_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                valid_from DATETIME(6) NOT NULL,
                valid_to DATETIME(6) DEFAULT NULL,
                registered_at DATETIME(6) NOT NULL,
                PRIMARY KEY (source_provider, key_id, key_version),
                UNIQUE KEY uk_authority_trust_key_v2_hash
                    (public_key_hash),
                CHECK (algorithm = 'Ed25519'),
                CHECK (valid_to IS NULL OR valid_from < valid_to)
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_authority_receipt_v2 (
                receipt_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin PRIMARY KEY,
                receipt_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                claim_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                evidence_type VARCHAR(40) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                evidence_id CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                source_provider VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                source_payload_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                receipt_type VARCHAR(40) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_version VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                replay_nonce VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                issued_at DATETIME(6) NOT NULL,
                expires_at DATETIME(6) NOT NULL,
                envelope_json LONGTEXT NOT NULL,
                envelope_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                status VARCHAR(16) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                revoked_at DATETIME(6) DEFAULT NULL,
                created_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_authority_receipt_v2_claim (claim_hash),
                UNIQUE KEY uk_authority_receipt_v2_envelope (envelope_hash),
                UNIQUE KEY uk_authority_receipt_v2_replay
                    (source_provider, key_id, key_version, replay_nonce),
                UNIQUE KEY uk_authority_receipt_v2_binding
                    (receipt_id, receipt_hash, claim_hash),
                UNIQUE KEY uk_authority_receipt_v2_revocation_binding
                    (receipt_id, receipt_hash, envelope_hash),
                KEY idx_authority_receipt_v2_evidence
                    (evidence_type, evidence_id),
                CONSTRAINT fk_authority_receipt_v2_trust_key
                    FOREIGN KEY (source_provider, key_id, key_version)
                    REFERENCES st_execution_authority_trust_key_v2
                        (source_provider, key_id, key_version),
                CHECK (JSON_VALID(envelope_json)),
                CHECK (status = 'ACTIVE'),
                CHECK (revoked_at IS NULL),
                CHECK (issued_at < expires_at)
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_authority_key_revocation_v2 (
                source_provider VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                key_version VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                revoked_at DATETIME(6) NOT NULL,
                reason_code VARCHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                revocation_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                created_at DATETIME(6) NOT NULL,
                PRIMARY KEY (source_provider, key_id, key_version),
                UNIQUE KEY uk_authority_key_revocation_v2_hash
                    (revocation_hash),
                CONSTRAINT fk_authority_key_revocation_v2_key
                    FOREIGN KEY (source_provider, key_id, key_version)
                    REFERENCES st_execution_authority_trust_key_v2
                        (source_provider, key_id, key_version),
                CHECK (revoked_at <= created_at)
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_authority_receipt_revocation_v2 (
                receipt_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin PRIMARY KEY,
                receipt_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                envelope_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                revoked_at DATETIME(6) NOT NULL,
                reason_code VARCHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                revocation_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                created_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_authority_receipt_revocation_v2_hash
                    (revocation_hash),
                CONSTRAINT fk_authority_receipt_revocation_v2_receipt
                    FOREIGN KEY (receipt_id, receipt_hash, envelope_hash)
                    REFERENCES st_execution_authority_receipt_v2
                        (receipt_id, receipt_hash, envelope_hash),
                CHECK (revoked_at <= created_at)
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS st_execution_authority_attestation_v2 (
                claim_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin PRIMARY KEY,
                evidence_type VARCHAR(40) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                evidence_id CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                source_provider VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                source_payload_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                receipt_type VARCHAR(40) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                receipt_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                receipt_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                available_at DATETIME(6) NOT NULL,
                verifier_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                verifier_version VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                verified_at DATETIME(6) NOT NULL,
                verification_level VARCHAR(32) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                receipt_envelope_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                trust_key_id VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                trust_key_version VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                replay_nonce VARCHAR(128) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                decision_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                attestation_hash CHAR(64) CHARACTER SET ascii
                    COLLATE ascii_bin NOT NULL,
                created_at DATETIME(6) NOT NULL,
                UNIQUE KEY uk_authority_attestation_v2_hash
                    (attestation_hash),
                UNIQUE KEY uk_authority_attestation_v2_binding
                    (claim_hash, attestation_hash),
                UNIQUE KEY uk_authority_attestation_v2_replay
                    (source_provider, trust_key_id,
                     trust_key_version, replay_nonce),
                KEY idx_authority_attestation_v2_evidence
                    (evidence_type, evidence_id),
                CONSTRAINT fk_authority_attestation_v2_receipt
                    FOREIGN KEY (receipt_id, receipt_hash, claim_hash)
                    REFERENCES st_execution_authority_receipt_v2
                        (receipt_id, receipt_hash, claim_hash),
                CONSTRAINT fk_authority_attestation_v2_trust_key
                    FOREIGN KEY (
                        source_provider, trust_key_id, trust_key_version
                    ) REFERENCES st_execution_authority_trust_key_v2 (
                        source_provider, key_id, key_version
                    ),
                CHECK (verification_level = 'CRYPTOGRAPHIC'),
                CHECK (verified_at >= available_at),
                CHECK (created_at = verified_at)
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_trust_key_v2_guard_bi",
            """
            CREATE TRIGGER trg_execution_authority_trust_key_v2_guard_bi
            BEFORE INSERT ON st_execution_authority_trust_key_v2
            FOR EACH ROW
            BEGIN
                SET NEW.registered_at = UTC_TIMESTAMP(6);
                IF CHAR_LENGTH(NEW.source_provider) = 0
                   OR CHAR_LENGTH(NEW.key_id) = 0
                   OR CHAR_LENGTH(NEW.key_version) = 0
                   OR NEW.algorithm <> 'Ed25519'
                   OR (NEW.valid_to IS NOT NULL
                       AND NEW.valid_from >= NEW.valid_to)
                   OR CHAR_LENGTH(NEW.public_key_hash) <> 64
                   OR BINARY NEW.public_key_hash
                      <> BINARY LOWER(NEW.public_key_hash)
                   OR NEW.public_key_hash REGEXP '[^0-9a-f]'
                   OR BINARY NEW.public_key_hash
                      <> BINARY LOWER(SHA2(NEW.public_key, 256)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority trust key';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_trust_key_v2_guard_bu",
            """
            CREATE TRIGGER trg_execution_authority_trust_key_v2_guard_bu
            BEFORE UPDATE ON st_execution_authority_trust_key_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority trust key is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_trust_key_v2_guard_bd",
            """
            CREATE TRIGGER trg_execution_authority_trust_key_v2_guard_bd
            BEFORE DELETE ON st_execution_authority_trust_key_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority trust key cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_key_revocation_v2_guard_bi",
            """
            CREATE TRIGGER trg_execution_authority_key_revocation_v2_guard_bi
            BEFORE INSERT ON st_execution_authority_key_revocation_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                SET NEW.created_at = UTC_TIMESTAMP(6);
                IF NEW.revoked_at > NEW.created_at
                   OR CHAR_LENGTH(NEW.reason_code) = 0
                   OR CHAR_LENGTH(NEW.revocation_hash) <> 64
                   OR BINARY NEW.revocation_hash
                      <> BINARY LOWER(NEW.revocation_hash)
                   OR NEW.revocation_hash REGEXP '[^0-9a-f]'
                   OR BINARY NEW.revocation_hash <> BINARY LOWER(SHA2(
                        CAST(CONCAT(
                            'trading-v2.authority-key-revocation.v1|',
                            NEW.source_provider, '|', NEW.key_id, '|',
                            NEW.key_version, '|', DATE_FORMAT(
                                NEW.revoked_at,
                                '%Y-%m-%dT%H:%i:%s.%f+00:00'
                            ), '|', NEW.reason_code
                        ) AS BINARY), 256)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority key revocation';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_execution_authority_trust_key_v2 k
                WHERE k.source_provider = NEW.source_provider
                  AND k.key_id = NEW.key_id
                  AND k.key_version = NEW.key_version
                  AND k.registered_at <= NEW.created_at;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority key revocation parent differs';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_key_revocation_v2_guard_bu",
            """
            CREATE TRIGGER trg_execution_authority_key_revocation_v2_guard_bu
            BEFORE UPDATE ON st_execution_authority_key_revocation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority key revocation is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_key_revocation_v2_guard_bd",
            """
            CREATE TRIGGER trg_execution_authority_key_revocation_v2_guard_bd
            BEFORE DELETE ON st_execution_authority_key_revocation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority key revocation cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_revocation_v2_guard_bi",
            """
            CREATE TRIGGER trg_execution_authority_receipt_revocation_v2_guard_bi
            BEFORE INSERT ON st_execution_authority_receipt_revocation_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                SET NEW.created_at = UTC_TIMESTAMP(6);
                IF NEW.revoked_at > NEW.created_at
                   OR CHAR_LENGTH(NEW.reason_code) = 0
                   OR CHAR_LENGTH(NEW.revocation_hash) <> 64
                   OR BINARY NEW.revocation_hash
                      <> BINARY LOWER(NEW.revocation_hash)
                   OR NEW.revocation_hash REGEXP '[^0-9a-f]'
                   OR BINARY NEW.revocation_hash <> BINARY LOWER(SHA2(
                        CAST(CONCAT(
                            'trading-v2.authority-receipt-revocation.v1|',
                            NEW.receipt_id, '|', NEW.receipt_hash, '|',
                            NEW.envelope_hash, '|', DATE_FORMAT(
                                NEW.revoked_at,
                                '%Y-%m-%dT%H:%i:%s.%f+00:00'
                            ), '|', NEW.reason_code
                        ) AS BINARY), 256)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority receipt revocation';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_execution_authority_receipt_v2 r
                JOIN st_execution_authority_trust_key_v2 k
                  ON k.source_provider = r.source_provider
                 AND k.key_id = r.key_id
                 AND k.key_version = r.key_version
                WHERE r.receipt_id = NEW.receipt_id
                  AND r.receipt_hash = NEW.receipt_hash
                  AND r.envelope_hash = NEW.envelope_hash
                  AND r.created_at <= NEW.created_at;
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority receipt revocation parent differs';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_revocation_v2_guard_bu",
            """
            CREATE TRIGGER trg_execution_authority_receipt_revocation_v2_guard_bu
            BEFORE UPDATE ON st_execution_authority_receipt_revocation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority receipt revocation is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_revocation_v2_guard_bd",
            """
            CREATE TRIGGER trg_execution_authority_receipt_revocation_v2_guard_bd
            BEFORE DELETE ON st_execution_authority_receipt_revocation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority receipt revocation cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_v2_guard_bi",
            """
            CREATE TRIGGER trg_execution_authority_receipt_v2_guard_bi
            BEFORE INSERT ON st_execution_authority_receipt_v2
            FOR EACH ROW
            BEGIN
                DECLARE trust_count INT DEFAULT 0;
                SET NEW.created_at = UTC_TIMESTAMP(6);
                IF NEW.evidence_type NOT IN
                   ('MARKET_CALENDAR', 'QUOTE_RECEIPT', 'INSTRUMENT_RULE')
                   OR NEW.status <> 'ACTIVE'
                   OR NEW.revoked_at IS NOT NULL
                   OR NEW.issued_at >= NEW.expires_at
                   OR NEW.issued_at > NEW.created_at
                   OR NEW.created_at >= NEW.expires_at
                   OR NOT JSON_VALID(NEW.envelope_json)
                   OR JSON_TYPE(NEW.envelope_json) <> 'OBJECT'
                   OR JSON_LENGTH(NEW.envelope_json) <> 11
                   OR JSON_CONTAINS_PATH(
                        NEW.envelope_json, 'all',
                        '$.algorithm', '$.claim_hash',
                        '$.source_provider', '$.receipt_id',
                        '$.receipt_hash', '$.key_id', '$.key_version',
                        '$.replay_nonce', '$.issued_at', '$.expires_at',
                        '$.signature') <> 1
                   OR CHAR_LENGTH(NEW.receipt_id) = 0
                   OR CHAR_LENGTH(NEW.source_provider) = 0
                   OR CHAR_LENGTH(NEW.key_id) = 0
                   OR CHAR_LENGTH(NEW.key_version) = 0
                   OR CHAR_LENGTH(NEW.replay_nonce) = 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority receipt binding';
                END IF;
                IF JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.algorithm')) <> 'Ed25519'
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.claim_hash'))
                        <> BINARY NEW.claim_hash
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.source_provider'))
                        <> BINARY NEW.source_provider
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.receipt_id'))
                        <> BINARY NEW.receipt_id
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.receipt_hash'))
                        <> BINARY NEW.receipt_hash
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.key_id'))
                        <> BINARY NEW.key_id
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.key_version'))
                        <> BINARY NEW.key_version
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.replay_nonce'))
                        <> BINARY NEW.replay_nonce
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.issued_at'))
                        <> BINARY DATE_FORMAT(
                            NEW.issued_at,
                            '%Y-%m-%dT%H:%i:%s.%f+00:00')
                   OR BINARY JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.expires_at'))
                        <> BINARY DATE_FORMAT(
                            NEW.expires_at,
                            '%Y-%m-%dT%H:%i:%s.%f+00:00')
                   OR CHAR_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.envelope_json, '$.signature'))) = 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority envelope columns differ';
                END IF;
                IF CHAR_LENGTH(NEW.receipt_hash) <> 64
                   OR BINARY NEW.receipt_hash
                      <> BINARY LOWER(NEW.receipt_hash)
                   OR NEW.receipt_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.claim_hash) <> 64
                   OR BINARY NEW.claim_hash <> BINARY LOWER(NEW.claim_hash)
                   OR NEW.claim_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.evidence_id) <> 64
                   OR BINARY NEW.evidence_id
                      <> BINARY LOWER(NEW.evidence_id)
                   OR NEW.evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.source_payload_hash) <> 64
                   OR BINARY NEW.source_payload_hash
                      <> BINARY LOWER(NEW.source_payload_hash)
                   OR NEW.source_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.envelope_hash) <> 64
                   OR BINARY NEW.envelope_hash
                      <> BINARY LOWER(NEW.envelope_hash)
                   OR NEW.envelope_hash REGEXP '[^0-9a-f]' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority receipt SHA256';
                END IF;
                IF BINARY NEW.envelope_hash <> BINARY LOWER(SHA2(
                    CAST(CONCAT(
                        '{"namespace":"trading-v2.canonical-json.v1",'
                        '"payload":{"value":',
                        CONVERT(NEW.envelope_json USING utf8mb4),
                        '}}'
                    ) AS BINARY), 256)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority envelope hash mismatch';
                END IF;
                IF NEW.evidence_type IN
                   ('MARKET_CALENDAR', 'INSTRUMENT_RULE')
                   AND BINARY NEW.receipt_hash
                       <> BINARY NEW.source_payload_hash THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority payload receipt mismatch';
                END IF;
                SELECT COUNT(*) INTO trust_count
                FROM st_execution_authority_trust_key_v2 k
                WHERE k.source_provider = NEW.source_provider
                  AND k.key_id = NEW.key_id
                  AND k.key_version = NEW.key_version
                  AND k.algorithm = 'Ed25519'
                  AND k.registered_at <= NEW.created_at
                  AND k.valid_from <= NEW.issued_at
                  AND (k.valid_to IS NULL OR NEW.issued_at < k.valid_to)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_execution_authority_key_revocation_v2 kr
                      WHERE kr.source_provider = k.source_provider
                        AND kr.key_id = k.key_id
                        AND kr.key_version = k.key_version
                  );
                IF trust_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority trust key is not active';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_v2_guard_bu",
            """
            CREATE TRIGGER trg_execution_authority_receipt_v2_guard_bu
            BEFORE UPDATE ON st_execution_authority_receipt_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority receipt is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_receipt_v2_guard_bd",
            """
            CREATE TRIGGER trg_execution_authority_receipt_v2_guard_bd
            BEFORE DELETE ON st_execution_authority_receipt_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority receipt cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_attestation_v2_guard_bi",
            """
            CREATE TRIGGER trg_execution_authority_attestation_v2_guard_bi
            BEFORE INSERT ON st_execution_authority_attestation_v2
            FOR EACH ROW
            BEGIN
                DECLARE parent_count INT DEFAULT 0;
                IF NEW.verification_level <> 'CRYPTOGRAPHIC'
                   OR NEW.verified_at < NEW.available_at
                   OR NEW.created_at <> NEW.verified_at
                   OR CHAR_LENGTH(NEW.evidence_type) = 0
                   OR CHAR_LENGTH(NEW.source_provider) = 0
                   OR CHAR_LENGTH(NEW.verifier_id) = 0
                   OR CHAR_LENGTH(NEW.verifier_version) = 0
                   OR CHAR_LENGTH(NEW.trust_key_id) = 0
                   OR CHAR_LENGTH(NEW.trust_key_version) = 0
                   OR CHAR_LENGTH(NEW.replay_nonce) = 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority attestation binding';
                END IF;
                IF CHAR_LENGTH(NEW.claim_hash) <> 64
                   OR BINARY NEW.claim_hash <> BINARY LOWER(NEW.claim_hash)
                   OR NEW.claim_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.evidence_id) <> 64
                   OR BINARY NEW.evidence_id
                      <> BINARY LOWER(NEW.evidence_id)
                   OR NEW.evidence_id REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.source_payload_hash) <> 64
                   OR BINARY NEW.source_payload_hash
                      <> BINARY LOWER(NEW.source_payload_hash)
                   OR NEW.source_payload_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.receipt_hash) <> 64
                   OR BINARY NEW.receipt_hash
                      <> BINARY LOWER(NEW.receipt_hash)
                   OR NEW.receipt_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.receipt_envelope_hash) <> 64
                   OR BINARY NEW.receipt_envelope_hash
                      <> BINARY LOWER(NEW.receipt_envelope_hash)
                   OR NEW.receipt_envelope_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.decision_hash) <> 64
                   OR BINARY NEW.decision_hash
                      <> BINARY LOWER(NEW.decision_hash)
                   OR NEW.decision_hash REGEXP '[^0-9a-f]'
                   OR CHAR_LENGTH(NEW.attestation_hash) <> 64
                   OR BINARY NEW.attestation_hash
                      <> BINARY LOWER(NEW.attestation_hash)
                   OR NEW.attestation_hash REGEXP '[^0-9a-f]' THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'invalid authority attestation SHA256';
                END IF;
                SELECT COUNT(*) INTO parent_count
                FROM st_execution_authority_receipt_v2 r
                JOIN st_execution_authority_trust_key_v2 k
                  ON k.source_provider = r.source_provider
                 AND k.key_id = r.key_id
                 AND k.key_version = r.key_version
                WHERE r.receipt_id = NEW.receipt_id
                  AND r.receipt_hash = NEW.receipt_hash
                  AND r.claim_hash = NEW.claim_hash
                  AND r.evidence_type = NEW.evidence_type
                  AND r.evidence_id = NEW.evidence_id
                  AND r.source_provider = NEW.source_provider
                  AND r.source_payload_hash = NEW.source_payload_hash
                  AND r.receipt_type = NEW.receipt_type
                  AND r.envelope_hash = NEW.receipt_envelope_hash
                  AND r.key_id = NEW.trust_key_id
                  AND r.key_version = NEW.trust_key_version
                  AND r.replay_nonce = NEW.replay_nonce
                  AND r.status = 'ACTIVE'
                  AND r.revoked_at IS NULL
                  AND r.created_at <= NEW.available_at
                  AND r.issued_at <= NEW.available_at
                  AND r.expires_at > NEW.verified_at
                  AND k.algorithm = 'Ed25519'
                  AND k.registered_at <= NEW.available_at
                  AND k.valid_from <= r.issued_at
                  AND (k.valid_to IS NULL OR r.issued_at < k.valid_to)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_execution_authority_key_revocation_v2 kr
                      WHERE kr.source_provider = k.source_provider
                        AND kr.key_id = k.key_id
                        AND kr.key_version = k.key_version
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_execution_authority_receipt_revocation_v2 rr
                      WHERE rr.receipt_id = r.receipt_id
                        AND rr.receipt_hash = r.receipt_hash
                        AND rr.envelope_hash = r.envelope_hash
                  );
                IF parent_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'authority attestation parent differs';
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_attestation_v2_guard_bu",
            """
            CREATE TRIGGER trg_execution_authority_attestation_v2_guard_bu
            BEFORE UPDATE ON st_execution_authority_attestation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority attestation is append only';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_execution_authority_attestation_v2_guard_bd",
            """
            CREATE TRIGGER trg_execution_authority_attestation_v2_guard_bd
            BEFORE DELETE ON st_execution_authority_attestation_v2
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'authority attestation cannot be deleted';
            END
            """,
            "DROP TRIGGER IF EXISTS trg_market_calendar_evidence_v2_authority_bi",
            """
            CREATE TRIGGER trg_market_calendar_evidence_v2_authority_bi
            BEFORE INSERT ON st_market_calendar_evidence_v2
            FOR EACH ROW
            FOLLOWS trg_market_calendar_evidence_v2_guard_bi
            BEGIN
                DECLARE attestation_count INT DEFAULT 0;
                IF NEW.authority_status = 'EXTERNAL_RECEIPT_VERIFIED' THEN
                    SELECT COUNT(*) INTO attestation_count
                    FROM st_execution_authority_attestation_v2 a
                    JOIN st_execution_authority_receipt_v2 r
                      ON r.receipt_id = a.receipt_id
                     AND r.receipt_hash = a.receipt_hash
                     AND r.claim_hash = a.claim_hash
                    JOIN st_execution_authority_trust_key_v2 k
                      ON k.source_provider = a.source_provider
                     AND k.key_id = a.trust_key_id
                     AND k.key_version = a.trust_key_version
                    WHERE a.evidence_type = 'MARKET_CALENDAR'
                      AND BINARY a.evidence_id
                          = BINARY NEW.calendar_evidence_id
                      AND BINARY a.source_provider
                          = BINARY NEW.source_provider
                      AND BINARY a.source_payload_hash
                          = BINARY NEW.source_payload_hash
                      AND a.receipt_type = 'CALENDAR_OTHER'
                      AND BINARY a.receipt_id
                          = BINARY NEW.source_receipt_id
                      AND BINARY a.receipt_hash
                          = BINARY NEW.source_receipt_hash
                      AND a.available_at = DATE_SUB(
                          NEW.available_at, INTERVAL 8 HOUR
                      )
                      AND a.verification_level = 'CRYPTOGRAPHIC'
                      AND r.status = 'ACTIVE'
                      AND r.revoked_at IS NULL
                      AND r.created_at <= a.available_at
                      AND r.issued_at <= a.available_at
                      AND r.expires_at > a.verified_at
                      AND k.algorithm = 'Ed25519'
                      AND k.registered_at <= a.available_at
                      AND k.valid_from <= r.issued_at
                      AND (k.valid_to IS NULL
                           OR r.issued_at < k.valid_to)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM st_execution_authority_key_revocation_v2 kr
                          WHERE kr.source_provider = k.source_provider
                            AND kr.key_id = k.key_id
                            AND kr.key_version = k.key_version
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM st_execution_authority_receipt_revocation_v2 rr
                          WHERE rr.receipt_id = r.receipt_id
                            AND rr.receipt_hash = r.receipt_hash
                            AND rr.envelope_hash = r.envelope_hash
                      );
                    IF attestation_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'calendar authority attestation absent';
                    END IF;
                END IF;
            END
            """,
            "DROP TRIGGER IF EXISTS trg_quote_receipt_evidence_v2_authority_bi",
            """
            CREATE TRIGGER trg_quote_receipt_evidence_v2_authority_bi
            BEFORE INSERT ON st_quote_receipt_evidence_v2
            FOR EACH ROW
            FOLLOWS trg_quote_receipt_evidence_v2_guard_bi
            BEGIN
                DECLARE attestation_count INT DEFAULT 0;
                IF NEW.authority_status = 'EXTERNAL_RECEIPT_VERIFIED' THEN
                    SELECT COUNT(*) INTO attestation_count
                    FROM st_execution_authority_attestation_v2 a
                    JOIN st_execution_authority_receipt_v2 r
                      ON r.receipt_id = a.receipt_id
                     AND r.receipt_hash = a.receipt_hash
                     AND r.claim_hash = a.claim_hash
                    JOIN st_execution_authority_trust_key_v2 k
                      ON k.source_provider = a.source_provider
                     AND k.key_id = a.trust_key_id
                     AND k.key_version = a.trust_key_version
                    WHERE a.evidence_type = 'QUOTE_RECEIPT'
                      AND BINARY a.evidence_id
                          = BINARY NEW.quote_evidence_id
                      AND BINARY a.source_provider
                          = BINARY NEW.source_provider
                      AND BINARY a.source_payload_hash
                          = BINARY NEW.source_payload_hash
                      AND BINARY a.receipt_type
                          = BINARY NEW.source_receipt_type
                      AND BINARY a.receipt_id
                          = BINARY NEW.source_receipt_id
                      AND BINARY a.receipt_hash
                          = BINARY NEW.source_receipt_hash
                      AND a.available_at = DATE_SUB(
                          NEW.available_at, INTERVAL 8 HOUR
                      )
                      AND a.verification_level = 'CRYPTOGRAPHIC'
                      AND r.status = 'ACTIVE'
                      AND r.revoked_at IS NULL
                      AND r.created_at <= a.available_at
                      AND r.issued_at <= a.available_at
                      AND r.expires_at > a.verified_at
                      AND k.algorithm = 'Ed25519'
                      AND k.registered_at <= a.available_at
                      AND k.valid_from <= r.issued_at
                      AND (k.valid_to IS NULL
                           OR r.issued_at < k.valid_to)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM st_execution_authority_key_revocation_v2 kr
                          WHERE kr.source_provider = k.source_provider
                            AND kr.key_id = k.key_id
                            AND kr.key_version = k.key_version
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM st_execution_authority_receipt_revocation_v2 rr
                          WHERE rr.receipt_id = r.receipt_id
                            AND rr.receipt_hash = r.receipt_hash
                            AND rr.envelope_hash = r.envelope_hash
                      );
                    IF attestation_count <> 1 THEN
                        SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'quote authority attestation absent';
                    END IF;
                END IF;
            END
            """,
        ),
    },
    {
        "version": ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL,
        "statements": ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL,
    },
    {
        "version": "20260902_016_portfolio_public_quote_quorum",
        "statements": (
            """
            CREATE TABLE IF NOT EXISTS st_portfolio_public_quote_v1 (
                stock_code VARCHAR(16) PRIMARY KEY,
                batch_id VARCHAR(64) NOT NULL,
                trade_date DATE NOT NULL,
                quote_at DATETIME NOT NULL,
                short_name VARCHAR(128) NOT NULL,
                price DECIMAL(20,6) NOT NULL,
                pre_close DECIMAL(20,6) NOT NULL,
                change_pct DECIMAL(18,8) NOT NULL,
                volume DECIMAL(24,4) NOT NULL,
                amount DECIMAL(24,4) NOT NULL,
                source_provider VARCHAR(80) NOT NULL,
                source_count INT NOT NULL,
                provider_mask VARCHAR(160) NOT NULL,
                price_deviation_pct DECIMAL(18,8) NOT NULL,
                received_at DATETIME NOT NULL,
                quality_status VARCHAR(16) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                KEY idx_portfolio_public_quote_latest
                    (trade_date, quote_at, quality_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ),
    },
    {
        "version": "20260906_017_backtest_strategy_identity",
        "statements": (
            """
            ALTER TABLE st_backtest_run_v2
            ADD COLUMN strategy_id VARCHAR(80) NULL
            AFTER backtest_uid
            """,
        ),
    },
)


def _checksum(statements: tuple[str, ...]) -> str:
    import hashlib

    return hashlib.sha256("\n".join(item.strip() for item in statements).encode("utf-8")).hexdigest()


EVIDENCE_BINDING_VERSION = "20260803_011_v2_execution_evidence_bindings"
EVIDENCE_GUARD_VERSION = "20260803_012_v2_execution_evidence_guards"
EVIDENCE_NATURAL_KEY_VERSION = (
    "20260803_013_v2_execution_evidence_natural_keys"
)
EVIDENCE_AUTHORITY_VERSION = (
    "20260803_014_v2_execution_authority_attestations"
)
EVIDENCE_ACCOUNTING_VERSION = ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL
_EVIDENCE_MIGRATION_VERSIONS = frozenset(
    {
        EVIDENCE_BINDING_VERSION,
        EVIDENCE_GUARD_VERSION,
        EVIDENCE_NATURAL_KEY_VERSION,
        EVIDENCE_AUTHORITY_VERSION,
        EVIDENCE_ACCOUNTING_VERSION,
    }
)
_EVIDENCE_TABLES = frozenset(
    {
        "st_market_calendar_evidence_v2",
        "st_quote_receipt_evidence_v2",
        "st_fill_execution_evidence_v2",
        "st_cash_event_binding_v2",
        "st_order_transition_v2",
    }
)
_AUTHORITY_TABLES = frozenset(
    {
        "st_execution_authority_trust_key_v2",
        "st_execution_authority_receipt_v2",
        "st_execution_authority_key_revocation_v2",
        "st_execution_authority_receipt_revocation_v2",
        "st_execution_authority_attestation_v2",
    }
)
_AUTHORITY_GUARDS = {
    f"trg_{stem}_guard_{suffix}": (table_name, event)
    for table_name, stem in (
        (
            "st_execution_authority_trust_key_v2",
            "execution_authority_trust_key_v2",
        ),
        (
            "st_execution_authority_receipt_v2",
            "execution_authority_receipt_v2",
        ),
        (
            "st_execution_authority_key_revocation_v2",
            "execution_authority_key_revocation_v2",
        ),
        (
            "st_execution_authority_receipt_revocation_v2",
            "execution_authority_receipt_revocation_v2",
        ),
        (
            "st_execution_authority_attestation_v2",
            "execution_authority_attestation_v2",
        ),
    )
    for suffix, event in (
        ("bi", "INSERT"),
        ("bu", "UPDATE"),
        ("bd", "DELETE"),
    )
}
_ACCOUNTING_EVIDENCE_TABLES = frozenset(
    {
        "st_fill_accounting_outcome_v2",
        "st_lot_transition_evidence_v2",
        "st_fill_accounting_outcome_finalization_v2",
    }
)
_ACCOUNTING_EVIDENCE_GUARDS = {
    f"trg_{stem}_guard_{suffix}": (table_name, event)
    for table_name, stem in (
        (
            "st_fill_accounting_outcome_v2",
            "fill_accounting_outcome_v2",
        ),
        (
            "st_lot_transition_evidence_v2",
            "lot_transition_evidence_v2",
        ),
        (
            "st_fill_accounting_outcome_finalization_v2",
            "fill_accounting_finalization_v2",
        ),
    )
    for suffix, event in (
        ("bi", "INSERT"),
        ("bu", "UPDATE"),
        ("bd", "DELETE"),
    )
}
_EVIDENCE_GUARDS = {
    f"trg_{stem}_guard_{suffix}": (table_name, event)
    for table_name, stem in (
        (
            "st_market_calendar_evidence_v2",
            "market_calendar_evidence_v2",
        ),
        ("st_quote_receipt_evidence_v2", "quote_receipt_evidence_v2"),
        ("st_fill_execution_evidence_v2", "fill_execution_evidence_v2"),
        ("st_cash_event_binding_v2", "cash_event_binding_v2"),
        ("st_order_transition_v2", "order_transition_v2"),
    )
    for suffix, event in (
        ("bi", "INSERT"),
        ("bu", "UPDATE"),
        ("bd", "DELETE"),
    )
}
_EVIDENCE_GUARDS.update(
    {
        "trg_market_calendar_evidence_v2_authority_bi": (
            "st_market_calendar_evidence_v2",
            "INSERT",
        ),
        "trg_quote_receipt_evidence_v2_authority_bi": (
            "st_quote_receipt_evidence_v2",
            "INSERT",
        ),
    }
)
_EVIDENCE_COLUMN_RE = re.compile(
    r"^\s*([a-z][a-z0-9_]*)\s+"
    r"(?:BINARY|CHAR|VARCHAR|DATE|DATETIME|LONGTEXT|BIGINT|INT|TINYINT|DECIMAL)\b",
    flags=re.IGNORECASE | re.MULTILINE,
)
_DROP_TRIGGER_RE = re.compile(
    r"^\s*DROP\s+TRIGGER\s+IF\s+EXISTS\s+([a-z0-9_]+)\s*$",
    flags=re.IGNORECASE,
)


def _mysql_dialect(engine: Engine) -> bool:
    return str(getattr(engine.dialect, "name", "")).lower() in {
        "mysql",
        "mariadb",
    }


def _migration_table_exists(connection: Connection) -> bool:
    return bool(
        connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'schema_migration_v2'
                """
            )
        ).scalar()
    )


def _applied_checksum(
    connection: Connection,
    version: str,
    *,
    migration_table_exists: bool,
) -> str | None:
    if not migration_table_exists:
        return None
    row = connection.execute(
        text(
            "SELECT checksum FROM schema_migration_v2 "
            "WHERE version = :version"
        ),
        {"version": version},
    ).mappings().first()
    return None if row is None else str(row["checksum"])


def _maintenance_fence_table_exists(connection: Connection) -> bool:
    return bool(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'"
            )
        ).scalar()
    )


def _maintenance_fence_row(
    connection: Connection,
    *,
    for_update: bool,
) -> dict[str, Any] | None:
    suffix = " FOR UPDATE" if for_update else " LOCK IN SHARE MODE"
    row = connection.execute(
        text(
            "SELECT fence_name, state, target_version, generation, "
            "activated_at, updated_at "
            f"FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
            "WHERE fence_name = :fence_name" + suffix
        ),
        {"fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME},
    ).mappings().first()
    return None if row is None else dict(row)


def _bootstrap_maintenance_fence(connection: Connection) -> str:
    """Create and strictly validate the durable migration control row."""

    if not _maintenance_fence_table_exists(connection):
        connection.execute(text(V2_EVIDENCE_MAINTENANCE_FENCE_DDL))
        connection.commit()
    from server.trading_v2.execution_evidence_schema_gate import (
        _inspect_maintenance_fence,
    )

    table_blockers, _ = _inspect_maintenance_fence(
        connection,
        expected_active=None,
        require_row=False,
    )
    if table_blockers:
        raise RuntimeError(
            "V2 maintenance-fence table contract drifted: "
            + ", ".join(table_blockers)
        )
    row = _maintenance_fence_row(connection, for_update=True)
    if row is None:
        connection.execute(
            text(
                f"INSERT INTO {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "(fence_name, state, target_version, generation, "
                "activated_at, updated_at) "
                "VALUES (:fence_name, :state, :target_version, 0, "
                "UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            ),
            {
                "fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
                "state": V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
                "target_version": EVIDENCE_BINDING_VERSION,
            },
        )
        connection.commit()
    blockers, active = _inspect_maintenance_fence(
        connection,
        expected_active=None,
    )
    if blockers:
        raise RuntimeError(
            "V2 maintenance-fence contract drifted: " + ", ".join(blockers)
        )
    return (
        V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
        if active
        else V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )


def _activate_maintenance_fence(
    connection: Connection,
    *,
    target_version: str,
) -> None:
    """Drain shared writer locks, publish ACTIVE, and durably commit it."""

    try:
        row = _maintenance_fence_row(connection, for_update=True)
        if row is None:
            raise RuntimeError("V2 maintenance-fence row is missing")
        state = str(row.get("state") or "").upper()
        if state not in {
            V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
            V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
        }:
            raise RuntimeError("V2 maintenance-fence state is invalid")
        try:
            observed_generation = int(row["generation"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "V2 maintenance-fence generation is invalid"
            ) from exc
        if observed_generation < 0:
            raise RuntimeError("V2 maintenance-fence generation is invalid")
        expected_generation = observed_generation + int(
            state == V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
        )
        update_result = connection.execute(
            text(
                f"UPDATE {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "SET generation = :next_generation, "
                "activated_at = IF(:observed_state = :active_state, "
                "activated_at, UTC_TIMESTAMP(6)), "
                "state = :active_state, target_version = :target_version, "
                "updated_at = UTC_TIMESTAMP(6) "
                "WHERE fence_name = :fence_name "
                "AND state = :observed_state "
                "AND generation = :observed_generation"
            ),
            {
                "active_state": V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
                "target_version": target_version,
                "fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
                "observed_state": state,
                "observed_generation": observed_generation,
                "next_generation": expected_generation,
            },
        )
        if type(getattr(update_result, "rowcount", None)) is not int or (
            update_result.rowcount != 1
        ):
            raise RuntimeError(
                "V2 maintenance-fence ACTIVE update did not match one row"
            )
        verified = _maintenance_fence_row(connection, for_update=True)
        try:
            verified_generation = int(
                None if verified is None else verified.get("generation")
            )
        except (TypeError, ValueError):
            verified_generation = -1
        if (
            verified is None
            or str(verified.get("state") or "").upper()
            != V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
            or str(verified.get("target_version") or "") != target_version
            or verified_generation != expected_generation
        ):
            raise RuntimeError(
                "V2 maintenance-fence ACTIVE update verification failed"
            )
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            _LOGGER.exception(
                "V2 maintenance-fence ACTIVE rollback failed"
            )
        raise


def _deactivate_maintenance_fence(connection: Connection) -> None:
    """Clear ACTIVE only after the final structure, row and ledger audits."""

    try:
        row = _maintenance_fence_row(connection, for_update=True)
        if row is None or str(row.get("state") or "").upper() != (
            V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
        ):
            raise RuntimeError("V2 maintenance-fence is not ACTIVE at release")
        try:
            observed_generation = int(row["generation"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "V2 maintenance-fence generation is invalid at release"
            ) from exc
        if observed_generation < 0:
            raise RuntimeError(
                "V2 maintenance-fence generation is invalid at release"
            )
        update_result = connection.execute(
            text(
                f"UPDATE {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "SET state = :inactive_state, target_version = :target_version, "
                "updated_at = UTC_TIMESTAMP(6) WHERE fence_name = :fence_name "
                "AND state = :observed_state "
                "AND generation = :observed_generation"
            ),
            {
                "inactive_state": V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
                "target_version": EVIDENCE_ACCOUNTING_VERSION,
                "fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
                "observed_state": V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
                "observed_generation": observed_generation,
            },
        )
        if type(getattr(update_result, "rowcount", None)) is not int or (
            update_result.rowcount != 1
        ):
            raise RuntimeError(
                "V2 maintenance-fence INACTIVE update did not match one row"
            )
        verified = _maintenance_fence_row(connection, for_update=True)
        try:
            verified_generation = int(
                None if verified is None else verified.get("generation")
            )
        except (TypeError, ValueError):
            verified_generation = -1
        if (
            verified is None
            or str(verified.get("state") or "").upper()
            != V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
            or str(verified.get("target_version") or "")
            != EVIDENCE_ACCOUNTING_VERSION
            or verified_generation != observed_generation
        ):
            raise RuntimeError(
                "V2 maintenance-fence INACTIVE update verification failed"
            )
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            _LOGGER.exception(
                "V2 maintenance-fence INACTIVE rollback failed"
            )
        raise


def _declared_evidence_columns() -> dict[str, frozenset[str]]:
    migration = next(
        item
        for item in MIGRATIONS
        if str(item["version"]) == EVIDENCE_BINDING_VERSION
    )
    result: dict[str, frozenset[str]] = {}
    for statement in tuple(migration["statements"]):
        match = re.search(
            r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)",
            statement,
            flags=re.IGNORECASE,
        )
        if match is None:
            continue
        table_name = match.group(1).lower()
        if table_name in _EVIDENCE_TABLES:
            result[table_name] = frozenset(
                item.lower() for item in _EVIDENCE_COLUMN_RE.findall(statement)
            )
    return result


def _validate_evidence_binding_schema(connection: Connection) -> None:
    table_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                    'st_market_calendar_evidence_v2',
                    'st_quote_receipt_evidence_v2',
                    'st_fill_execution_evidence_v2',
                    'st_cash_event_binding_v2',
                    'st_order_transition_v2'
                  )
                ORDER BY TABLE_NAME
                """
            )
        ).mappings()
    )
    observed_tables = {
        str(row["TABLE_NAME"]).lower(): row for row in table_rows
    }
    if set(observed_tables) != set(_EVIDENCE_TABLES):
        raise RuntimeError("V2 execution-evidence table set drifted")
    for table_name, row in observed_tables.items():
        if str(row["ENGINE"] or "").lower() != "innodb":
            raise RuntimeError(
                f"V2 execution-evidence table must use InnoDB: {table_name}"
            )
        if not str(row["TABLE_COLLATION"] or "").lower().startswith(
            "utf8mb4_"
        ):
            raise RuntimeError(
                "V2 execution-evidence table must use utf8mb4: "
                f"{table_name}"
            )

    column_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TABLE_NAME, COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                    'st_market_calendar_evidence_v2',
                    'st_quote_receipt_evidence_v2',
                    'st_fill_execution_evidence_v2',
                    'st_cash_event_binding_v2',
                    'st_order_transition_v2'
                  )
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """
            )
        ).mappings()
    )
    observed_columns: dict[str, set[str]] = {}
    for row in column_rows:
        observed_columns.setdefault(
            str(row["TABLE_NAME"]).lower(),
            set(),
        ).add(str(row["COLUMN_NAME"]).lower())
    declared_columns = _declared_evidence_columns()
    for table_name, expected in declared_columns.items():
        if frozenset(observed_columns.get(table_name, set())) != expected:
            raise RuntimeError(
                f"V2 execution-evidence columns drifted: {table_name}"
            )


def _validate_evidence_guard_schema(connection: Connection) -> None:
    rows = tuple(
        connection.execute(
            text(
                """
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING,
                       EVENT_MANIPULATION, ACTION_STATEMENT, SQL_MODE,
                       DEFINER, CHARACTER_SET_CLIENT,
                       COLLATION_CONNECTION, DATABASE_COLLATION
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                  AND EVENT_OBJECT_TABLE IN (
                    'st_market_calendar_evidence_v2',
                    'st_quote_receipt_evidence_v2',
                    'st_fill_execution_evidence_v2',
                    'st_cash_event_binding_v2',
                    'st_order_transition_v2'
                  )
                ORDER BY TRIGGER_NAME
                """
            )
        ).mappings()
    )
    observed = {str(row["TRIGGER_NAME"]).lower(): row for row in rows}
    if set(observed) != set(_EVIDENCE_GUARDS):
        raise RuntimeError("V2 execution-evidence trigger set drifted")
    for trigger_name, (table_name, event) in _EVIDENCE_GUARDS.items():
        row = observed[trigger_name]
        if (
            str(row["EVENT_OBJECT_TABLE"]).lower() != table_name
            or str(row["ACTION_TIMING"]).upper() != "BEFORE"
            or str(row["EVENT_MANIPULATION"]).upper() != event
        ):
            raise RuntimeError(
                f"V2 execution-evidence trigger shape drifted: {trigger_name}"
            )
        body = " ".join(str(row["ACTION_STATEMENT"] or "").split()).upper()
        if "SIGNAL SQLSTATE '45000'" not in body:
            raise RuntimeError(
                f"V2 execution-evidence trigger is not fail-closed: {trigger_name}"
            )


def _declared_authority_columns() -> dict[str, frozenset[str]]:
    migration = next(
        item
        for item in MIGRATIONS
        if str(item["version"]) == EVIDENCE_AUTHORITY_VERSION
    )
    result: dict[str, frozenset[str]] = {}
    for statement in tuple(migration["statements"]):
        match = re.search(
            r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)",
            statement,
            flags=re.IGNORECASE,
        )
        if match is None or match.group(1).lower() not in _AUTHORITY_TABLES:
            continue
        result[match.group(1).lower()] = frozenset(
            item.lower() for item in _EVIDENCE_COLUMN_RE.findall(statement)
        )
    return result


def _validate_authority_schema(connection: Connection) -> None:
    from server.trading_v2 import execution_evidence_schema_gate as schema_gate

    expected_schema = schema_gate._authority_schema_signature()
    if set(expected_schema) != set(_AUTHORITY_TABLES):
        raise RuntimeError("V2 authority migration declaration is incomplete")
    database_name = str(
        connection.execute(text("SELECT DATABASE()")).scalar() or ""
    )
    if not database_name:
        raise RuntimeError("V2 authority validation requires a database")
    table_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                    'st_execution_authority_trust_key_v2',
                    'st_execution_authority_receipt_v2',
                    'st_execution_authority_key_revocation_v2',
                    'st_execution_authority_receipt_revocation_v2',
                    'st_execution_authority_attestation_v2'
                  )
                ORDER BY TABLE_NAME
                """
            )
        ).mappings()
    )
    observed_tables = {
        str(row["TABLE_NAME"]).lower(): row for row in table_rows
    }
    if set(observed_tables) != set(_AUTHORITY_TABLES):
        raise RuntimeError("V2 authority table set drifted")
    for table_name, row in observed_tables.items():
        if str(row["ENGINE"] or "").lower() != "innodb":
            raise RuntimeError(
                f"V2 authority table must use InnoDB: {table_name}"
            )
        if not str(row["TABLE_COLLATION"] or "").lower().startswith(
            "utf8mb4_"
        ):
            raise RuntimeError(
                f"V2 authority table must use utf8mb4: {table_name}"
            )
        if str(row["ROW_FORMAT"] or "").upper() != "DYNAMIC":
            raise RuntimeError(
                f"V2 authority table must use DYNAMIC row format: {table_name}"
            )
    column_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE,
                       IS_NULLABLE, COLUMN_DEFAULT, COLLATION_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                    'st_execution_authority_trust_key_v2',
                    'st_execution_authority_receipt_v2',
                    'st_execution_authority_key_revocation_v2',
                    'st_execution_authority_receipt_revocation_v2',
                    'st_execution_authority_attestation_v2'
                  )
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """
            )
        ).mappings()
    )
    observed_columns: dict[str, dict[str, dict[str, Any]]] = {}
    for row in column_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        column_name = str(row["COLUMN_NAME"]).lower()
        observed_columns.setdefault(table_name, {})[column_name] = {
            "type": schema_gate._normalize_column_type(row["COLUMN_TYPE"]),
            "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            "default": schema_gate._normalize_observed_default(
                row["COLUMN_DEFAULT"]
            ),
            "collation": str(row["COLLATION_NAME"] or "").lower(),
        }
    for table_name, signature in expected_schema.items():
        actual = observed_columns.get(table_name, {})
        comparable = {
            name: {
                "type": details["type"],
                "nullable": details["nullable"],
                "default": details["default"],
            }
            for name, details in actual.items()
        }
        if comparable != signature["columns"]:
            raise RuntimeError(f"V2 authority columns drifted: {table_name}")
        for column_name, expected in signature["columns"].items():
            if not expected["type"].startswith(("char", "varchar", "longtext")):
                continue
            collation = actual.get(column_name, {}).get("collation", "")
            expected_prefix = (
                "utf8mb4_"
                if column_name == "envelope_json"
                else "ascii_"
            )
            if not str(collation).startswith(expected_prefix):
                raise RuntimeError(
                    "V2 authority column collation drifted: "
                    f"{table_name}.{column_name}"
                )

    index_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE,
                       SEQ_IN_INDEX, COLUMN_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                    'st_execution_authority_trust_key_v2',
                    'st_execution_authority_receipt_v2',
                    'st_execution_authority_key_revocation_v2',
                    'st_execution_authority_receipt_revocation_v2',
                    'st_execution_authority_attestation_v2'
                  )
                ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                """
            )
        ).mappings()
    )
    index_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in index_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        index_name = str(row["INDEX_NAME"])
        entry = index_parts.setdefault(table_name, {}).setdefault(
            index_name,
            {
                "unique": int(row["NON_UNIQUE"]) == 0,
                "columns": [],
            },
        )
        entry["columns"].append(str(row["COLUMN_NAME"]).lower())
    observed_indexes = {
        table_name: {
            name: {
                "unique": details["unique"],
                "columns": tuple(details["columns"]),
            }
            for name, details in indexes.items()
        }
        for table_name, indexes in index_parts.items()
    }
    for table_name, signature in expected_schema.items():
        expected_indexes = signature["indexes"]
        actual_indexes = observed_indexes.get(table_name, {})
        for index_name, expected in expected_indexes.items():
            if actual_indexes.get(index_name) != expected:
                raise RuntimeError(
                    "V2 authority index drifted: "
                    f"{table_name}.{index_name}"
                )
        expected_unique = {
            name
            for name, details in expected_indexes.items()
            if details["unique"]
        }
        actual_unique = {
            name
            for name, details in actual_indexes.items()
            if details["unique"]
        }
        if actual_unique != expected_unique:
            raise RuntimeError(
                f"V2 authority unique-index set drifted: {table_name}"
            )

    foreign_key_rows = tuple(
        connection.execute(
            text(
                """
                SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME,
                       k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME,
                       k.REFERENCED_COLUMN_NAME, k.ORDINAL_POSITION,
                       r.DELETE_RULE, r.UPDATE_RULE
                FROM information_schema.KEY_COLUMN_USAGE k
                JOIN information_schema.REFERENTIAL_CONSTRAINTS r
                  ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                 AND r.TABLE_NAME = k.TABLE_NAME
                 AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                WHERE k.CONSTRAINT_SCHEMA = DATABASE()
                  AND k.TABLE_NAME IN (
                    'st_execution_authority_trust_key_v2',
                    'st_execution_authority_receipt_v2',
                    'st_execution_authority_key_revocation_v2',
                    'st_execution_authority_receipt_revocation_v2',
                    'st_execution_authority_attestation_v2'
                  )
                  AND k.REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME,
                         k.ORDINAL_POSITION
                """
            )
        ).mappings()
    )
    fk_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in foreign_key_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        constraint_name = str(row["CONSTRAINT_NAME"])
        if str(row["REFERENCED_TABLE_SCHEMA"]) != database_name:
            raise RuntimeError(
                "V2 authority foreign-key schema drifted: "
                f"{table_name}.{constraint_name}"
            )
        entry = fk_parts.setdefault(table_name, {}).setdefault(
            constraint_name,
            {
                "columns": [],
                "referenced_table": str(row["REFERENCED_TABLE_NAME"]).lower(),
                "referenced_columns": [],
                "on_delete": normalize_mysql_referential_rule(
                    row["DELETE_RULE"]
                ),
                "on_update": normalize_mysql_referential_rule(
                    row["UPDATE_RULE"]
                ),
            },
        )
        entry["columns"].append(str(row["COLUMN_NAME"]).lower())
        entry["referenced_columns"].append(
            str(row["REFERENCED_COLUMN_NAME"]).lower()
        )
    observed_fks = {
        table_name: {
            name: {
                "columns": tuple(details["columns"]),
                "referenced_table": details["referenced_table"],
                "referenced_columns": tuple(details["referenced_columns"]),
                "on_delete": details["on_delete"],
                "on_update": details["on_update"],
            }
            for name, details in constraints.items()
        }
        for table_name, constraints in fk_parts.items()
    }
    for table_name, signature in expected_schema.items():
        if observed_fks.get(table_name, {}) != signature["foreign_keys"]:
            raise RuntimeError(f"V2 authority foreign keys drifted: {table_name}")
    trigger_rows = tuple(
        connection.execute(
            text(
                """
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING,
                       EVENT_MANIPULATION, ACTION_STATEMENT, SQL_MODE,
                       DEFINER, CHARACTER_SET_CLIENT,
                       COLLATION_CONNECTION, DATABASE_COLLATION
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                  AND EVENT_OBJECT_TABLE IN (
                    'st_execution_authority_trust_key_v2',
                    'st_execution_authority_receipt_v2',
                    'st_execution_authority_key_revocation_v2',
                    'st_execution_authority_receipt_revocation_v2',
                    'st_execution_authority_attestation_v2'
                  )
                ORDER BY TRIGGER_NAME
                """
            )
        ).mappings()
    )
    observed_triggers = {
        str(row["TRIGGER_NAME"]).lower(): row for row in trigger_rows
    }
    if set(observed_triggers) != set(_AUTHORITY_GUARDS):
        raise RuntimeError("V2 authority trigger set drifted")
    expected_trigger_bodies = schema_gate._authority_trigger_bodies()
    if set(expected_trigger_bodies) != set(_AUTHORITY_GUARDS):
        raise RuntimeError("V2 authority trigger declaration is incomplete")
    for name, (table_name, event) in _AUTHORITY_GUARDS.items():
        row = observed_triggers[name]
        body = " ".join(str(row["ACTION_STATEMENT"] or "").split()).upper()
        if (
            str(row["EVENT_OBJECT_TABLE"]).lower() != table_name
            or str(row["ACTION_TIMING"]).upper() != "BEFORE"
            or str(row["EVENT_MANIPULATION"]).upper() != event
            or "SIGNAL SQLSTATE '45000'" not in body
            or " ".join(str(row["ACTION_STATEMENT"] or "").split())
            != expected_trigger_bodies[name]
        ):
            raise RuntimeError(f"V2 authority trigger drifted: {name}")
        sql_modes = {
            item.strip().upper()
            for item in str(row["SQL_MODE"] or "").split(",")
            if item.strip()
        }
        if (
            not ({"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} & sql_modes)
            or not {
                "NO_ZERO_DATE",
                "NO_ZERO_IN_DATE",
                "ERROR_FOR_DIVISION_BY_ZERO",
            }.issubset(sql_modes)
            or not str(row["DEFINER"] or "")
            or str(row["CHARACTER_SET_CLIENT"] or "").lower()
            not in {"utf8", "utf8mb4"}
            or not str(row["COLLATION_CONNECTION"] or "")
            .lower()
            .startswith(("utf8_", "utf8mb4_"))
            or not str(row["DATABASE_COLLATION"] or "")
            .lower()
            .startswith("utf8mb4_")
        ):
            raise RuntimeError(f"V2 authority trigger context drifted: {name}")


def _validate_accounting_evidence_schema(connection: Connection) -> None:
    """Validate the registered 015 tables, keys, FKs and exact trigger bodies."""

    from server.trading_v2 import execution_evidence_schema_gate as schema_gate

    expected_schema = schema_gate._accounting_schema_signature()
    expected_triggers = schema_gate._accounting_trigger_contracts()
    expected_bodies = schema_gate._accounting_trigger_bodies()
    if (
        set(expected_schema) != set(_ACCOUNTING_EVIDENCE_TABLES)
        or set(expected_triggers) != set(_ACCOUNTING_EVIDENCE_GUARDS)
        or set(expected_bodies) != set(_ACCOUNTING_EVIDENCE_GUARDS)
    ):
        raise RuntimeError("V2 accounting-evidence declaration is incomplete")
    database_name = str(
        connection.execute(text("SELECT DATABASE()")).scalar() or ""
    )
    if not database_name:
        raise RuntimeError("V2 accounting validation requires a database")
    table_literals = ", ".join(
        f"'{name}'" for name in sorted(_ACCOUNTING_EVIDENCE_TABLES)
    )

    table_rows = tuple(
        connection.execute(
            text(
                "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({table_literals}) ORDER BY TABLE_NAME"
            )
        ).mappings()
    )
    observed_tables = {
        str(row["TABLE_NAME"]).lower(): row for row in table_rows
    }
    if set(observed_tables) != set(_ACCOUNTING_EVIDENCE_TABLES):
        raise RuntimeError("V2 accounting-evidence table set drifted")
    for table_name, row in observed_tables.items():
        if (
            str(row["ENGINE"] or "").lower() != "innodb"
            or not str(row["TABLE_COLLATION"] or "")
            .lower()
            .startswith("utf8mb4_")
            or str(row["ROW_FORMAT"] or "").upper() != "DYNAMIC"
        ):
            raise RuntimeError(
                f"V2 accounting-evidence table shape drifted: {table_name}"
            )

    column_rows = tuple(
        connection.execute(
            text(
                "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                "COLUMN_DEFAULT, COLLATION_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({table_literals}) "
                "ORDER BY TABLE_NAME, ORDINAL_POSITION"
            )
        ).mappings()
    )
    observed_columns: dict[str, dict[str, dict[str, Any]]] = {}
    for row in column_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        column_name = str(row["COLUMN_NAME"]).lower()
        observed_columns.setdefault(table_name, {})[column_name] = {
            "type": schema_gate._normalize_column_type(row["COLUMN_TYPE"]),
            "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            "default": schema_gate._normalize_observed_default(
                row["COLUMN_DEFAULT"]
            ),
            "collation": str(row["COLLATION_NAME"] or "").lower(),
        }
    for table_name, signature in expected_schema.items():
        actual = observed_columns.get(table_name, {})
        comparable = {
            name: {
                "type": details["type"],
                "nullable": details["nullable"],
                "default": details["default"],
            }
            for name, details in actual.items()
        }
        if comparable != signature["columns"]:
            raise RuntimeError(
                f"V2 accounting-evidence columns drifted: {table_name}"
            )
        for column_name, expected in signature["columns"].items():
            if expected["type"].startswith(("char", "varchar", "longtext")):
                if not str(
                    actual.get(column_name, {}).get("collation", "")
                ).startswith("utf8mb4_"):
                    raise RuntimeError(
                        "V2 accounting-evidence column collation drifted: "
                        f"{table_name}.{column_name}"
                    )

    index_rows = tuple(
        connection.execute(
            text(
                "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, "
                "COLUMN_NAME FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({table_literals}) "
                "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
            )
        ).mappings()
    )
    index_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in index_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        name = str(row["INDEX_NAME"])
        entry = index_parts.setdefault(table_name, {}).setdefault(
            name,
            {
                "unique": int(row["NON_UNIQUE"]) == 0,
                "columns": [],
            },
        )
        entry["columns"].append(str(row["COLUMN_NAME"]).lower())
    observed_indexes = {
        table_name: {
            name: {
                "unique": details["unique"],
                "columns": tuple(details["columns"]),
            }
            for name, details in indexes.items()
        }
        for table_name, indexes in index_parts.items()
    }
    for table_name, signature in expected_schema.items():
        actual = observed_indexes.get(table_name, {})
        for name, expected in signature["indexes"].items():
            if actual.get(name) != expected:
                raise RuntimeError(
                    f"V2 accounting-evidence index drifted: {table_name}.{name}"
                )
        if {
            name for name, details in actual.items() if details["unique"]
        } != {
            name
            for name, details in signature["indexes"].items()
            if details["unique"]
        }:
            raise RuntimeError(
                f"V2 accounting-evidence unique indexes drifted: {table_name}"
            )

    foreign_key_rows = tuple(
        connection.execute(
            text(
                "SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME, "
                "k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME, "
                "k.REFERENCED_COLUMN_NAME, k.ORDINAL_POSITION, "
                "r.DELETE_RULE, r.UPDATE_RULE "
                "FROM information_schema.KEY_COLUMN_USAGE k "
                "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
                "ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA "
                "AND r.TABLE_NAME = k.TABLE_NAME "
                "AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                "WHERE k.CONSTRAINT_SCHEMA = DATABASE() "
                f"AND k.TABLE_NAME IN ({table_literals}) "
                "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
                "ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION"
            )
        ).mappings()
    )
    fk_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in foreign_key_rows:
        table_name = str(row["TABLE_NAME"]).lower()
        name = str(row["CONSTRAINT_NAME"])
        if str(row["REFERENCED_TABLE_SCHEMA"]) != database_name:
            raise RuntimeError(
                f"V2 accounting-evidence FK schema drifted: {table_name}.{name}"
            )
        entry = fk_parts.setdefault(table_name, {}).setdefault(
            name,
            {
                "columns": [],
                "referenced_table": str(row["REFERENCED_TABLE_NAME"]).lower(),
                "referenced_columns": [],
                "on_delete": normalize_mysql_referential_rule(
                    row["DELETE_RULE"]
                ),
                "on_update": normalize_mysql_referential_rule(
                    row["UPDATE_RULE"]
                ),
            },
        )
        entry["columns"].append(str(row["COLUMN_NAME"]).lower())
        entry["referenced_columns"].append(
            str(row["REFERENCED_COLUMN_NAME"]).lower()
        )
    observed_fks = {
        table_name: {
            name: {
                "columns": tuple(details["columns"]),
                "referenced_table": details["referenced_table"],
                "referenced_columns": tuple(details["referenced_columns"]),
                "on_delete": details["on_delete"],
                "on_update": details["on_update"],
            }
            for name, details in constraints.items()
        }
        for table_name, constraints in fk_parts.items()
    }
    for table_name, signature in expected_schema.items():
        if observed_fks.get(table_name, {}) != signature["foreign_keys"]:
            raise RuntimeError(
                f"V2 accounting-evidence foreign keys drifted: {table_name}"
            )

    trigger_rows = tuple(
        connection.execute(
            text(
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, "
                "EVENT_MANIPULATION, ACTION_STATEMENT, SQL_MODE, DEFINER, "
                "CHARACTER_SET_CLIENT, COLLATION_CONNECTION, "
                "DATABASE_COLLATION FROM information_schema.TRIGGERS "
                "WHERE TRIGGER_SCHEMA = DATABASE() "
                f"AND EVENT_OBJECT_TABLE IN ({table_literals}) "
                "ORDER BY TRIGGER_NAME"
            )
        ).mappings()
    )
    observed_triggers = {
        str(row["TRIGGER_NAME"]).lower(): row for row in trigger_rows
    }
    if set(observed_triggers) != set(expected_triggers):
        raise RuntimeError("V2 accounting-evidence trigger set drifted")
    for name, (event, table_name) in expected_triggers.items():
        row = observed_triggers[name]
        body = " ".join(str(row["ACTION_STATEMENT"] or "").split())
        modes = {
            item.strip().upper()
            for item in str(row["SQL_MODE"] or "").split(",")
            if item.strip()
        }
        if (
            str(row["EVENT_OBJECT_TABLE"]).lower() != table_name
            or str(row["ACTION_TIMING"]).upper() != "BEFORE"
            or str(row["EVENT_MANIPULATION"]).upper() != event
            or body != expected_bodies[name]
            or "SIGNAL SQLSTATE '45000'" not in body.upper()
            or not ({"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} & modes)
            or not {
                "NO_ZERO_DATE",
                "NO_ZERO_IN_DATE",
                "ERROR_FOR_DIVISION_BY_ZERO",
            }.issubset(modes)
            or not str(row["DEFINER"] or "")
            or str(row["CHARACTER_SET_CLIENT"] or "").lower()
            not in {"utf8", "utf8mb4"}
            or not str(row["COLLATION_CONNECTION"] or "")
            .lower()
            .startswith(("utf8_", "utf8mb4_"))
            or not str(row["DATABASE_COLLATION"] or "")
            .lower()
            .startswith("utf8mb4_")
        ):
            raise RuntimeError(
                f"V2 accounting-evidence trigger drifted: {name}"
            )


def _validate_evidence_schema_for_version(
    connection: Connection,
    version: str,
    *,
    maintenance_fence_expected_active: bool,
) -> None:
    if version not in _EVIDENCE_MIGRATION_VERSIONS:
        return
    from server.trading_v2.execution_evidence_schema_gate import (
        inspect_v2_execution_evidence_schema,
    )

    report = inspect_v2_execution_evidence_schema(
        connection,
        require_guards=(version != EVIDENCE_BINDING_VERSION),
        require_natural_keys=(version >= EVIDENCE_NATURAL_KEY_VERSION),
        require_migration_ledger=False,
        require_authority_attestations=(version >= EVIDENCE_AUTHORITY_VERSION),
        require_accounting_evidence=(version >= EVIDENCE_ACCOUNTING_VERSION),
        phase_scoped_migration_replay=True,
        maintenance_fence_expected_active=maintenance_fence_expected_active,
        include_activation_blockers=False,
    )
    if report.structural_blockers:
        raise RuntimeError(
            "V2 execution-evidence schema failed full structural validation: "
            + ", ".join(report.structural_blockers)
        )
    if version >= EVIDENCE_AUTHORITY_VERSION:
        _validate_authority_schema(connection)
    if version >= EVIDENCE_ACCOUNTING_VERSION:
        _validate_accounting_evidence_schema(connection)


def _validate_complete_evidence_schema(
    connection: Connection,
    *,
    maintenance_fence_expected_active: bool,
) -> None:
    """Run the public 011-015 structure and ledger gate after final replay."""

    from server.trading_v2.execution_evidence_schema_gate import (
        inspect_v2_execution_evidence_schema,
    )

    report = inspect_v2_execution_evidence_schema(
        connection,
        maintenance_fence_expected_active=maintenance_fence_expected_active,
        include_activation_blockers=False,
    )
    if report.structural_blockers:
        raise RuntimeError(
            "V2 execution-evidence final schema/ledger validation failed: "
            + ", ".join(report.structural_blockers)
        )


def _exact_accounting_count_vector(
    value: object,
    *,
    expected_names: tuple[str, ...],
) -> dict[str, int] | None:
    """Decode a report count vector without allowing dict key collapse."""

    if type(value) is not tuple or len(value) != len(expected_names):
        return None
    result: dict[str, int] = {}
    for expected_name, item in zip(expected_names, value, strict=True):
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or item[0] != expected_name
            or item[0] in result
            or type(item[1]) is not int
            or item[1] < 0
        ):
            return None
        result[item[0]] = item[1]
    return result


def _exact_accounting_text_ids(
    value: object,
    *,
    require_sha256: bool = False,
) -> tuple[str, ...] | None:
    if type(value) is not tuple:
        return None
    items: list[str] = []
    for item in value:
        if type(item) is not str or not item or item != item.strip():
            return None
        if require_sha256 and (
            len(item) != 64
            or item != item.lower()
            or any(character not in "0123456789abcdef" for character in item)
        ):
            return None
        items.append(item)
    if len(set(items)) != len(items) or tuple(items) != tuple(sorted(items)):
        return None
    return tuple(items)


def _accounting_audit_metrics_are_complete(
    report: object,
    *,
    audit_tables: tuple[str, ...],
    hash_fields: Mapping[str, tuple[str, ...]],
    parent_kinds: tuple[str, ...],
) -> bool:
    """Independently re-prove all accounting report summary metrics."""

    counts = _exact_accounting_count_vector(
        getattr(report, "table_counts", None),
        expected_names=audit_tables,
    )
    hashes = _exact_accounting_count_vector(
        getattr(report, "hash_verifications", None),
        expected_names=audit_tables,
    )
    if counts is None or hashes is None:
        return False
    if len(audit_tables) != 3:
        return False
    outcome_table, lot_effect_table, finalization_table = audit_tables
    expected_hashes = {
        table: counts[table] * len(hash_fields[table]) for table in audit_tables
    }
    metrics = (
        getattr(report, "hashes_verified", None),
        getattr(report, "rows_reconstructed", None),
        getattr(report, "finalized_outcomes", None),
        getattr(report, "lot_chains_checked", None),
        getattr(report, "parent_rows_checked", None),
    )
    if any(type(value) is not int or value < 0 for value in metrics):
        return False
    finalized_ids = _exact_accounting_text_ids(
        getattr(report, "finalized_outcome_ids", None),
        require_sha256=True,
    )
    lot_ids = _exact_accounting_text_ids(
        getattr(report, "lot_chain_ids", None),
    )
    raw_parent_checks = getattr(report, "parent_row_checks", None)
    if (
        finalized_ids is None
        or lot_ids is None
        or type(raw_parent_checks) is not tuple
    ):
        return False
    parent_checks: list[tuple[str, str]] = []
    for item in raw_parent_checks:
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or item[0] not in parent_kinds
            or type(item[1]) is not str
            or not item[1]
            or item[1] != item[1].strip()
        ):
            return False
        parent_checks.append(item)
    if (
        len(set(parent_checks)) != len(parent_checks)
        or tuple(parent_checks) != tuple(sorted(parent_checks))
    ):
        return False
    parent_lot_ids = tuple(
        identity for kind, identity in parent_checks if kind == "lot"
    )
    parent_kind_set = frozenset(kind for kind, _ in parent_checks)
    expected_parent_kinds = (
        frozenset(parent_kinds)
        if counts[outcome_table]
        else frozenset()
    )
    return (
        hashes == expected_hashes
        and report.hashes_verified == sum(hashes.values())
        and report.rows_reconstructed == sum(counts.values())
        and report.finalized_outcomes
        == counts[outcome_table]
        == counts[finalization_table]
        and report.finalized_outcomes == len(finalized_ids)
        and report.lot_chains_checked == len(lot_ids)
        and report.lot_chains_checked
        <= counts[lot_effect_table]
        and bool(report.lot_chains_checked)
        == bool(counts[lot_effect_table])
        and report.parent_rows_checked == len(parent_checks)
        and parent_kind_set == expected_parent_kinds
        and parent_lot_ids == lot_ids
        and getattr(report, "database_sha2_used", None) is True
        and getattr(report, "shared_row_locks_used", None) is True
    )


def _require_evidence_audit_consistent_isolation(
    connection: Connection,
) -> str:
    """Prove all multi-table evidence scans share one stable transaction view."""

    probe = getattr(connection, "get_isolation_level", None)
    if not callable(probe):
        raise RuntimeError(
            "V2 evidence audit connection must expose get_isolation_level()"
        )
    try:
        raw_level = probe()
    except Exception as exc:
        raise RuntimeError(
            "V2 evidence audit transaction isolation cannot be inspected"
        ) from exc
    if type(raw_level) is not str or not raw_level.strip():
        raise RuntimeError(
            "V2 evidence audit transaction isolation must be exact text"
        )
    normalized = " ".join(
        raw_level.upper().replace("_", " ").replace("-", " ").split()
    )
    if normalized not in {"REPEATABLE READ", "SERIALIZABLE"}:
        raise RuntimeError(
            "V2 evidence audit requires REPEATABLE READ or SERIALIZABLE"
        )
    return normalized


def _audit_evidence_rows_before_ledger(
    connection: Connection,
    *,
    version: str,
) -> None:
    """Lock and reconstruct persisted rows before publishing a ledger marker.

    The five core evidence tables and both extension layers use independent
    MySQL-SHA2/Python canonical auditors.  Empty tables and valid non-empty
    tables are both accepted; any incomplete reconstruction fails before the
    migration ledger can advance.  ``LOCK IN SHARE MODE`` retains next-key
    locks until the following ledger or fence-state commit.
    """

    _require_evidence_audit_consistent_isolation(connection)

    from server.integrations.v2_execution_evidence_audit import (
        EVIDENCE_JSON_HASH_COLUMNS,
        audit_v2_execution_evidence_database,
    )

    report = audit_v2_execution_evidence_database(connection)
    counts = dict(report.table_counts)
    expected_payloads = sum(
        counts.get(table_name, 0) * len(hash_columns)
        for table_name, hash_columns in EVIDENCE_JSON_HASH_COLUMNS.items()
    )
    if (
        frozenset(counts) != frozenset(_EVIDENCE_TABLES)
        or any(type(count) is not int or count < 0 for count in counts.values())
        or report.rows_reconstructed != sum(counts.values())
        or report.payload_hashes_verified != expected_payloads
        or not report.database_sha2_used
        or not report.shared_row_locks_used
    ):
        raise RuntimeError("V2 execution-evidence stored-row audit was incomplete")

    if version >= EVIDENCE_AUTHORITY_VERSION:
        from server.integrations.v2_execution_evidence_authority_audit import (
            AUTHORITY_AUDIT_TABLES,
            V2AuthorityStoredRowAuditReport,
            audit_v2_execution_evidence_authority_database,
        )

        try:
            authority_report = audit_v2_execution_evidence_authority_database(
                connection
            )
        except Exception as exc:
            raise RuntimeError(
                "V2 authority stored-row audit failed before migration ledger"
            ) from exc
        if type(authority_report) is not V2AuthorityStoredRowAuditReport:
            raise RuntimeError("V2 authority stored-row audit returned wrong type")
        authority_counts = dict(authority_report.table_counts)
        if (
            tuple(authority_counts) != AUTHORITY_AUDIT_TABLES
            or not authority_report.audit_passed
            or authority_report.production_activation_allowed is not False
        ):
            raise RuntimeError("V2 authority stored-row audit was incomplete")

    if version >= EVIDENCE_ACCOUNTING_VERSION:
        from server.integrations.v2_accounting_evidence_audit import (
            ACCOUNTING_AUDIT_HASH_FIELDS,
            ACCOUNTING_AUDIT_PARENT_KINDS,
            ACCOUNTING_AUDIT_TABLES,
            V2AccountingEvidenceAuditReport,
            audit_v2_accounting_evidence_database,
        )

        try:
            accounting_report = audit_v2_accounting_evidence_database(connection)
        except Exception as exc:
            raise RuntimeError(
                "V2 accounting stored-row audit failed before migration ledger"
            ) from exc
        if type(accounting_report) is not V2AccountingEvidenceAuditReport:
            raise RuntimeError("V2 accounting stored-row audit returned wrong type")
        if (
            accounting_report.audit_passed is not True
            or not _accounting_audit_metrics_are_complete(
                accounting_report,
                audit_tables=ACCOUNTING_AUDIT_TABLES,
                hash_fields=ACCOUNTING_AUDIT_HASH_FIELDS,
                parent_kinds=ACCOUNTING_AUDIT_PARENT_KINDS,
            )
            or accounting_report.production_activation_allowed is not False
            or accounting_report.actionable_output_allowed is not False
        ):
            raise RuntimeError("V2 accounting stored-row audit was incomplete")


def _validate_evidence_server(connection: Connection) -> None:
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    version = str(connection.execute(text("SELECT VERSION()")).scalar() or "")
    if (
        dialect != "mysql"
        or not is_isolated_acceptance_version(version)
    ):
        raise RuntimeError(
            "V2 execution-evidence migrations require validated Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )


def _evidence_statement_already_applied(
    connection: Connection,
    *,
    version: str,
    statement_index: int,
) -> bool:
    """Skip only recovery DDL whose complete frozen contract already exists.

    MySQL 5.7 implicitly commits DDL.  Recovery therefore has to recognize the
    exact 013 index and exact trigger definitions that may have committed before
    their migration ledger row.  A correct trigger is kept in service across a
    replayed DROP/CREATE pair; a drifted trigger is deliberately dropped and
    recreated.  Body, execution context and ``ACTION_ORDER`` must all match.
    """

    migration = next(
        (item for item in MIGRATIONS if str(item["version"]) == version),
        None,
    )
    if migration is None:
        return False
    statements = tuple(migration["statements"])
    if statement_index < 1 or statement_index > len(statements):
        raise RuntimeError("V2 evidence recovery statement index is invalid")
    statement = str(statements[statement_index - 1])

    # Versions 001-010 own their original forward-only DDL (including the
    # real-trading hard-guard triggers from 006).  The exact recovery parser
    # below is deliberately scoped to the opt-in 011-015 evidence extension;
    # treating legacy trigger DDL as evidence DDL would reject a fresh V2
    # installation as an "undeclared" evidence trigger.
    if version not in _EVIDENCE_MIGRATION_VERSIONS:
        return False

    if version == EVIDENCE_NATURAL_KEY_VERSION and statement_index == 1:
        rows = tuple(
            connection.execute(
                text(
                    """
                    SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'st_market_calendar_evidence_v2'
                      AND INDEX_NAME = 'uk_calendar_evidence_v2_natural'
                    ORDER BY SEQ_IN_INDEX
                    """
                )
            ).mappings()
        )
        if not rows:
            return False
        observed = tuple(
            (
                int(row["NON_UNIQUE"]),
                int(row["SEQ_IN_INDEX"]),
                str(row["COLUMN_NAME"]).lower(),
            )
            for row in rows
        )
        expected = (
            (0, 1, "market_code"),
            (0, 2, "trade_date"),
            (0, 3, "calendar_version"),
        )
        if observed != expected:
            raise RuntimeError(
                "V2 evidence natural-key recovery found a drifted existing index"
            )
        return True

    from server.trading_v2.execution_evidence_schema_gate import (
        _TRIGGER_RE,
        _all_trigger_bodies,
        _all_trigger_contracts,
        _trigger_action_order_contracts,
        _trigger_row_matches_contract,
    )

    drop_match = _DROP_TRIGGER_RE.match(statement)
    create_match = _TRIGGER_RE.search(statement)
    trigger_name = (
        drop_match.group(1).lower()
        if drop_match is not None
        else (
            create_match.group(1).lower()
            if create_match is not None
            else ""
        )
    )
    if not trigger_name:
        return False
    contracts = _all_trigger_contracts()
    bodies = _all_trigger_bodies()
    action_orders, _ = _trigger_action_order_contracts(contracts)
    if trigger_name not in contracts:
        raise RuntimeError(
            f"V2 evidence recovery found an undeclared trigger: {trigger_name}"
        )
    rows = tuple(
        connection.execute(
            text(
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, "
                "EVENT_MANIPULATION, ACTION_STATEMENT, ACTION_ORDER, "
                "SQL_MODE, DEFINER, CHARACTER_SET_CLIENT, "
                "COLLATION_CONNECTION, DATABASE_COLLATION "
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
        raise RuntimeError(
            f"V2 evidence recovery found duplicate trigger metadata: {trigger_name}"
        )
    exact = _trigger_row_matches_contract(
        dict(rows[0]),
        trigger_name=trigger_name,
        contracts=contracts,
        bodies=bodies,
        action_orders=action_orders,
    )
    if exact:
        return True
    if drop_match is not None:
        return False
    raise RuntimeError(
        f"V2 evidence recovery found a drifted trigger before CREATE: {trigger_name}"
    )


def _run_v2_migrations_unlocked(
    engine: Engine,
    *,
    dry_run: bool,
    allow_execution_evidence: bool,
    connection: Connection | None = None,
    acceptance_fault_hook: V2MigrationAcceptanceFaultHook | None = None,
) -> list[V2MigrationResult]:
    if acceptance_fault_hook is not None:
        if type(acceptance_fault_hook) is not V2MigrationAcceptanceFaultHook:
            raise TypeError(
                "acceptance_fault_hook must be V2MigrationAcceptanceFaultHook"
            )
        acceptance_fault_hook._validate_declaration(MIGRATIONS)
    if not _mysql_dialect(engine):
        if not dry_run:
            raise RuntimeError("V2 migrations require MySQL or MariaDB")
        return [
            V2MigrationResult(
                str(migration["version"]),
                "would_apply",
                len(tuple(migration["statements"])),
            )
            for migration in MIGRATIONS
        ]
    if not dry_run and connection is None:
        raise RuntimeError("V2 migration writes require the named-lock connection")

    opened: Connection | None = None
    if connection is None:
        opened = engine.connect()
        connection = opened
    try:
        migration_table_exists = _migration_table_exists(connection)
        if not dry_run:
            connection.execute(text(MIGRATION_TABLE_DDL))
            connection.commit()
            migration_table_exists = True

        maintenance_fence_state = V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
        maintenance_fence_active = False
        maintenance_started = False
        if not dry_run:
            maintenance_fence_state = _bootstrap_maintenance_fence(connection)
            maintenance_fence_active = (
                maintenance_fence_state == V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
            )
            if maintenance_fence_active and not allow_execution_evidence:
                raise RuntimeError(
                    "V2 execution-evidence maintenance fence is ACTIVE; "
                    "explicit allow_execution_evidence recovery is required"
                )

        results: list[V2MigrationResult] = []
        for migration in MIGRATIONS:
            version = str(migration["version"])
            statements = tuple(migration["statements"])
            checksum = _checksum(statements)
            applied = _applied_checksum(
                connection,
                version,
                migration_table_exists=migration_table_exists,
            )
            if applied is not None:
                if (
                    acceptance_fault_hook is not None
                    and acceptance_fault_hook.version == version
                ):
                    raise RuntimeError(
                        "acceptance fault target migration is already recorded"
                    )
                if applied != checksum:
                    raise RuntimeError(
                        f"applied V2 migration checksum changed: {version}"
                    )
            elif dry_run:
                results.append(
                    V2MigrationResult(version, "would_apply", len(statements))
                )
                continue

            if version in _EVIDENCE_MIGRATION_VERSIONS:
                if applied is None and not allow_execution_evidence:
                    raise RuntimeError(
                        "pending V2 execution-evidence migrations require the "
                        "explicit allow_execution_evidence gate"
                    )
                if allow_execution_evidence and not maintenance_started:
                    _validate_evidence_server(connection)
                    _activate_maintenance_fence(
                        connection,
                        target_version=version,
                    )
                    maintenance_fence_active = True
                    maintenance_started = True

            if applied is not None:
                _validate_evidence_schema_for_version(
                    connection,
                    version,
                    maintenance_fence_expected_active=maintenance_fence_active,
                )
                results.append(
                    V2MigrationResult(version, "exists", len(statements))
                )
                continue

            for statement_index, statement in enumerate(statements, start=1):
                already_applied = _evidence_statement_already_applied(
                    connection,
                    version=version,
                    statement_index=statement_index,
                )
                if not already_applied:
                    connection.execute(text(statement))
                    connection.commit()
                    if acceptance_fault_hook is not None:
                        acceptance_fault_hook._raise_if_matches(
                            version=version,
                            phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                            committed_statement_count=statement_index,
                        )
            _validate_evidence_schema_for_version(
                connection,
                version,
                maintenance_fence_expected_active=maintenance_fence_active,
            )
            if version in _EVIDENCE_MIGRATION_VERSIONS:
                _audit_evidence_rows_before_ledger(
                    connection,
                    version=version,
                )
            if acceptance_fault_hook is not None:
                acceptance_fault_hook._raise_if_matches(
                    version=version,
                    phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
                    committed_statement_count=None,
                )
            connection.execute(
                text(
                    "INSERT IGNORE INTO schema_migration_v2 "
                    "(version, checksum) VALUES (:version, :checksum)"
                ),
                {"version": version, "checksum": checksum},
            )
            connection.commit()
            recorded = _applied_checksum(
                connection,
                version,
                migration_table_exists=True,
            )
            if recorded != checksum:
                raise RuntimeError(
                    f"V2 migration ledger checksum conflict: {version}"
                )
            results.append(
                V2MigrationResult(version, "applied", len(statements))
            )
        if not dry_run:
            _validate_complete_evidence_schema(
                connection,
                maintenance_fence_expected_active=maintenance_fence_active,
            )
            if maintenance_fence_active:
                _audit_evidence_rows_before_ledger(
                    connection,
                    version=EVIDENCE_ACCOUNTING_VERSION,
                )
                _deactivate_maintenance_fence(connection)
                maintenance_fence_active = False
        return results
    finally:
        if opened is not None:
            opened.close()


def run_v2_migrations(
    engine: Engine,
    *,
    dry_run: bool = False,
    allow_execution_evidence: bool = False,
    connection: Connection | None = None,
    acceptance_fault_hook: V2MigrationAcceptanceFaultHook | None = None,
) -> list[V2MigrationResult]:
    """Plan or apply V2 DDL under one cross-process MySQL named lock.

    MySQL DDL commits implicitly, so each statement is committed and checked
    before the migration checksum is recorded.  The new execution-evidence
    migrations remain opt-in until isolated MySQL and privilege acceptance is
    complete; ordinary invocations cannot apply them accidentally.  A
    caller-owned ``connection`` must be idle and belong to ``engine``; when
    supplied, identity checks, the named lock and every DDL statement can stay
    on that exact physical checkout.
    """

    if type(dry_run) is not bool:
        raise TypeError("dry_run must be bool")
    if type(allow_execution_evidence) is not bool:
        raise TypeError("allow_execution_evidence must be bool")
    if acceptance_fault_hook is not None:
        if type(acceptance_fault_hook) is not V2MigrationAcceptanceFaultHook:
            raise TypeError(
                "acceptance_fault_hook must be V2MigrationAcceptanceFaultHook"
            )
        if dry_run:
            raise ValueError(
                "acceptance_fault_hook is unavailable for dry-run migrations"
            )
    if connection is not None:
        connection_dialect = str(
            getattr(getattr(connection, "dialect", None), "name", "") or ""
        ).lower()
        if connection_dialect != "mysql":
            raise RuntimeError(
                "caller-owned V2 migration connection must use MySQL"
            )
        connection_engine = getattr(connection, "engine", None)
        if connection_engine is not None and connection_engine is not engine:
            raise RuntimeError(
                "caller-owned V2 migration connection belongs to another engine"
            )
        in_transaction = getattr(connection, "in_transaction", None)
        if callable(in_transaction) and bool(in_transaction()):
            raise RuntimeError(
                "caller-owned V2 migration connection must not have an "
                "active transaction"
            )

    if dry_run:
        return _run_v2_migrations_unlocked(
            engine,
            dry_run=True,
            allow_execution_evidence=allow_execution_evidence,
            connection=connection,
            acceptance_fault_hook=acceptance_fault_hook,
        )
    if not _mysql_dialect(engine):
        return _run_v2_migrations_unlocked(
            engine,
            dry_run=False,
            allow_execution_evidence=allow_execution_evidence,
            acceptance_fault_hook=acceptance_fault_hook,
        )
    lock_options = (
        {"connection": connection} if connection is not None else {}
    )
    with mysql_named_lock(
        engine,
        "probiga:trading_v2_schema",
        timeout_seconds=30,
        **lock_options,
    ) as lock_connection:
        return _run_v2_migrations_unlocked(
            engine,
            dry_run=False,
            allow_execution_evidence=allow_execution_evidence,
            connection=lock_connection,
            acceptance_fault_hook=acceptance_fault_hook,
        )


__all__ = [
    "EVIDENCE_BINDING_VERSION",
    "EVIDENCE_AUTHORITY_VERSION",
    "EVIDENCE_GUARD_VERSION",
    "EVIDENCE_NATURAL_KEY_VERSION",
    "MIGRATION_TABLE_DDL",
    "MIGRATIONS",
    "V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE",
    "V2_EVIDENCE_MAINTENANCE_FENCE_DDL",
    "V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE",
    "V2_EVIDENCE_MAINTENANCE_FENCE_NAME",
    "V2_EVIDENCE_MAINTENANCE_FENCE_TABLE",
    "V2_MIGRATION_FAULT_AFTER_DDL_COMMIT",
    "V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE",
    "V2MigrationAcceptanceFault",
    "V2MigrationAcceptanceFaultHook",
    "V2MigrationResult",
    "run_v2_migrations",
]
