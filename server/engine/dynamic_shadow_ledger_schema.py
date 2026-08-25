# -*- coding: utf-8 -*-
"""Frozen schema for dynamic-strategy internal-paper trial evidence.

The tables in this module do not create an order path.  They bind a verified
dynamic candidate receipt to the existing V2 internal-paper OMS and the V3
fill-backed forward-evidence ledger.  Every authority column is permanently
false; a broker or real account is outside this contract.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from sqlalchemy import text


DYNAMIC_SHADOW_LEDGER_TABLE_NAMES = (
    "st_strategy_adapter_candidate_fact",
    "st_dynamic_shadow_trial_plan",
    "st_dynamic_shadow_trial_chain",
    "st_dynamic_shadow_trial_exit_binding",
)

_DYNAMIC_TABLE_COLLATION = "utf8mb4_unicode_ci"
_DYNAMIC_TABLE_ENGINE = "InnoDB"


def _column(
    name: str,
    column_type: str,
    ddl: str,
    *,
    default: str | None = None,
    extra: str = "",
    character: bool = False,
) -> dict[str, Any]:
    return {
        "name": name,
        "column_type": column_type,
        "is_nullable": "NO",
        "column_default": default,
        "extra": extra,
        "character_set_name": "utf8mb4" if character else None,
        "collation_name": _DYNAMIC_TABLE_COLLATION if character else None,
        "ddl": ddl,
    }


_DYNAMIC_COLUMN_CONTRACTS: dict[str, tuple[dict[str, Any], ...]] = {
    "st_strategy_adapter_candidate_fact": (
        _column("candidate_run_uid", "char(32)",
                "candidate_run_uid CHAR(32) NOT NULL", character=True),
        _column("stock_code", "varchar(16)",
                "stock_code VARCHAR(16) NOT NULL", character=True),
        _column("candidate_index", "int",
                "candidate_index INT NOT NULL"),
        _column("trade_date", "date", "trade_date DATE NOT NULL"),
        _column("candidate_json", "longtext",
                "candidate_json LONGTEXT NOT NULL", character=True),
        _column("candidate_hash", "char(64)",
                "candidate_hash CHAR(64) NOT NULL", character=True),
        _column(
            "created_at", "datetime(6)",
            "created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
            default="current_timestamp(6)", extra="DEFAULT_GENERATED",
        ),
    ),
    "st_dynamic_shadow_trial_plan": (
        _column("plan_id", "char(64)", "plan_id CHAR(64) NOT NULL",
                character=True),
        _column("candidate_run_uid", "char(32)",
                "candidate_run_uid CHAR(32) NOT NULL", character=True),
        _column("candidate_receipt_hash", "char(64)",
                "candidate_receipt_hash CHAR(64) NOT NULL", character=True),
        _column("strategy_key", "varchar(80)",
                "strategy_key VARCHAR(80) NOT NULL", character=True),
        _column("strategy_version", "varchar(160)",
                "strategy_version VARCHAR(160) NOT NULL", character=True),
        _column("strategy_version_hash", "char(64)",
                "strategy_version_hash CHAR(64) NOT NULL", character=True),
        _column("execution_binding_hash", "char(64)",
                "execution_binding_hash CHAR(64) NOT NULL", character=True),
        _column("trade_date", "date", "trade_date DATE NOT NULL"),
        _column("stock_code", "varchar(16)",
                "stock_code VARCHAR(16) NOT NULL", character=True),
        _column("account_id", "varchar(64)",
                "account_id VARCHAR(64) NOT NULL", character=True),
        _column("maximum_target_bp", "int",
                "maximum_target_bp INT NOT NULL"),
        _column("candidate_fact_hash", "char(64)",
                "candidate_fact_hash CHAR(64) NOT NULL", character=True),
        _column("candidate_signal_json", "longtext",
                "candidate_signal_json LONGTEXT NOT NULL", character=True),
        _column("candidate_signal_hash", "char(64)",
                "candidate_signal_hash CHAR(64) NOT NULL", character=True),
        _column("plan_payload_json", "longtext",
                "plan_payload_json LONGTEXT NOT NULL", character=True),
        _column("plan_hash", "char(64)", "plan_hash CHAR(64) NOT NULL",
                character=True),
        _column("plan_status", "varchar(32)",
                "plan_status VARCHAR(32) NOT NULL", character=True),
        _column(
            "automatic_real_order_submission", "tinyint(1)",
            "automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0",
            default="0",
        ),
        _column(
            "real_order_authority", "tinyint(1)",
            "real_order_authority TINYINT(1) NOT NULL DEFAULT 0",
            default="0",
        ),
        _column(
            "created_at", "datetime(6)",
            "created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
            default="current_timestamp(6)", extra="DEFAULT_GENERATED",
        ),
    ),
    "st_dynamic_shadow_trial_chain": (
        _column("chain_id", "char(64)", "chain_id CHAR(64) NOT NULL",
                character=True),
        _column("plan_id", "char(64)", "plan_id CHAR(64) NOT NULL",
                character=True),
        _column("source_intent_id", "varchar(64)",
                "source_intent_id VARCHAR(64) NOT NULL", character=True),
        _column("entry_order_id", "varchar(64)",
                "entry_order_id VARCHAR(64) NOT NULL", character=True),
        _column("entry_fill_id", "varchar(64)",
                "entry_fill_id VARCHAR(64) NOT NULL", character=True),
        _column("forward_evidence_id", "char(64)",
                "forward_evidence_id CHAR(64) NOT NULL", character=True),
        _column("intent_fact_hash", "char(64)",
                "intent_fact_hash CHAR(64) NOT NULL", character=True),
        _column("risk_decision_fact_hash", "char(64)",
                "risk_decision_fact_hash CHAR(64) NOT NULL", character=True),
        _column("entry_order_fact_hash", "char(64)",
                "entry_order_fact_hash CHAR(64) NOT NULL", character=True),
        _column("entry_fill_fact_hash", "char(64)",
                "entry_fill_fact_hash CHAR(64) NOT NULL", character=True),
        _column("forward_evidence_fact_hash", "char(64)",
                "forward_evidence_fact_hash CHAR(64) NOT NULL",
                character=True),
        _column("exit_set_hash", "char(64)",
                "exit_set_hash CHAR(64) NOT NULL", character=True),
        _column("exit_binding_count", "int",
                "exit_binding_count INT NOT NULL"),
        _column("chain_payload_json", "longtext",
                "chain_payload_json LONGTEXT NOT NULL", character=True),
        _column("chain_hash", "char(64)", "chain_hash CHAR(64) NOT NULL",
                character=True),
        _column(
            "automatic_real_order_submission", "tinyint(1)",
            "automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0",
            default="0",
        ),
        _column(
            "real_order_authority", "tinyint(1)",
            "real_order_authority TINYINT(1) NOT NULL DEFAULT 0",
            default="0",
        ),
        _column(
            "created_at", "datetime(6)",
            "created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
            default="current_timestamp(6)", extra="DEFAULT_GENERATED",
        ),
    ),
    "st_dynamic_shadow_trial_exit_binding": (
        _column("binding_id", "char(64)", "binding_id CHAR(64) NOT NULL",
                character=True),
        _column("chain_id", "char(64)", "chain_id CHAR(64) NOT NULL",
                character=True),
        _column("allocation_id", "char(64)",
                "allocation_id CHAR(64) NOT NULL", character=True),
        _column("exit_order_id", "varchar(64)",
                "exit_order_id VARCHAR(64) NOT NULL", character=True),
        _column("exit_fill_id", "varchar(64)",
                "exit_fill_id VARCHAR(64) NOT NULL", character=True),
        _column("allocation_fact_hash", "char(64)",
                "allocation_fact_hash CHAR(64) NOT NULL", character=True),
        _column("exit_order_fact_hash", "char(64)",
                "exit_order_fact_hash CHAR(64) NOT NULL", character=True),
        _column("exit_fill_fact_hash", "char(64)",
                "exit_fill_fact_hash CHAR(64) NOT NULL", character=True),
        _column("binding_payload_json", "longtext",
                "binding_payload_json LONGTEXT NOT NULL", character=True),
        _column("binding_hash", "char(64)",
                "binding_hash CHAR(64) NOT NULL", character=True),
        _column(
            "real_order_authority", "tinyint(1)",
            "real_order_authority TINYINT(1) NOT NULL DEFAULT 0",
            default="0",
        ),
        _column(
            "created_at", "datetime(6)",
            "created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
            default="current_timestamp(6)", extra="DEFAULT_GENERATED",
        ),
    ),
}

# Public immutable column inventory used by the governance trigger contract.
# Exposing names (not mutable contract dictionaries) keeps both schema and
# database immutability guards on one exact table definition.
DYNAMIC_SHADOW_LEDGER_COLUMN_NAMES = {
    table_name: tuple(column["name"] for column in columns)
    for table_name, columns in _DYNAMIC_COLUMN_CONTRACTS.items()
}

_DYNAMIC_INDEX_CONTRACTS: dict[
    str, dict[str, tuple[bool, tuple[str, ...], str]]
] = {
    "st_strategy_adapter_candidate_fact": {
        "PRIMARY": (
            True, ("candidate_run_uid", "stock_code"),
            "PRIMARY KEY (candidate_run_uid, stock_code)",
        ),
        "uk_strategy_adapter_candidate_index": (
            True, ("candidate_run_uid", "candidate_index"),
            "UNIQUE KEY uk_strategy_adapter_candidate_index "
            "(candidate_run_uid, candidate_index)",
        ),
        "uk_strategy_adapter_candidate_hash": (
            True, ("candidate_hash",),
            "UNIQUE KEY uk_strategy_adapter_candidate_hash (candidate_hash)",
        ),
    },
    "st_dynamic_shadow_trial_plan": {
        "PRIMARY": (True, ("plan_id",), "PRIMARY KEY (plan_id)"),
        "uk_dynamic_shadow_candidate_stock": (
            True, ("candidate_run_uid", "stock_code"),
            "UNIQUE KEY uk_dynamic_shadow_candidate_stock "
            "(candidate_run_uid, stock_code)",
        ),
        "uk_dynamic_shadow_plan_hash": (
            True, ("plan_hash",),
            "UNIQUE KEY uk_dynamic_shadow_plan_hash (plan_hash)",
        ),
        "idx_dynamic_shadow_plan_strategy": (
            False, ("strategy_key", "strategy_version", "trade_date"),
            "KEY idx_dynamic_shadow_plan_strategy "
            "(strategy_key, strategy_version, trade_date)",
        ),
        "idx_dynamic_shadow_plan_receipt": (
            False, ("candidate_receipt_hash",),
            "KEY idx_dynamic_shadow_plan_receipt (candidate_receipt_hash)",
        ),
        "idx_dynamic_shadow_plan_fact_hash": (
            False, ("candidate_fact_hash",),
            "KEY idx_dynamic_shadow_plan_fact_hash (candidate_fact_hash)",
        ),
    },
    "st_dynamic_shadow_trial_chain": {
        "PRIMARY": (True, ("chain_id",), "PRIMARY KEY (chain_id)"),
        "uk_dynamic_shadow_chain_plan": (
            True, ("plan_id",),
            "UNIQUE KEY uk_dynamic_shadow_chain_plan (plan_id)",
        ),
        "uk_dynamic_shadow_chain_evidence": (
            True, ("forward_evidence_id",),
            "UNIQUE KEY uk_dynamic_shadow_chain_evidence "
            "(forward_evidence_id)",
        ),
        "uk_dynamic_shadow_chain_hash": (
            True, ("chain_hash",),
            "UNIQUE KEY uk_dynamic_shadow_chain_hash (chain_hash)",
        ),
        "idx_dynamic_shadow_chain_intent": (
            False, ("source_intent_id",),
            "KEY idx_dynamic_shadow_chain_intent (source_intent_id)",
        ),
        "idx_dynamic_shadow_chain_entry_order": (
            False, ("entry_order_id",),
            "KEY idx_dynamic_shadow_chain_entry_order (entry_order_id)",
        ),
        "idx_dynamic_shadow_chain_entry_fill": (
            False, ("entry_fill_id",),
            "KEY idx_dynamic_shadow_chain_entry_fill (entry_fill_id)",
        ),
    },
    "st_dynamic_shadow_trial_exit_binding": {
        "PRIMARY": (True, ("binding_id",), "PRIMARY KEY (binding_id)"),
        "uk_dynamic_shadow_exit_allocation": (
            True, ("chain_id", "allocation_id"),
            "UNIQUE KEY uk_dynamic_shadow_exit_allocation "
            "(chain_id, allocation_id)",
        ),
        "uk_dynamic_shadow_exit_hash": (
            True, ("binding_hash",),
            "UNIQUE KEY uk_dynamic_shadow_exit_hash (binding_hash)",
        ),
        "idx_dynamic_shadow_exit_fill": (
            False, ("exit_fill_id",),
            "KEY idx_dynamic_shadow_exit_fill (exit_fill_id)",
        ),
        "idx_dynamic_shadow_exit_allocation": (
            False, ("allocation_id",),
            "KEY idx_dynamic_shadow_exit_allocation (allocation_id)",
        ),
        "idx_dynamic_shadow_exit_order": (
            False, ("exit_order_id",),
            "KEY idx_dynamic_shadow_exit_order (exit_order_id)",
        ),
    },
}

_DYNAMIC_FOREIGN_KEY_CONTRACTS: dict[
    str, tuple[str, tuple[str, ...], str, tuple[str, ...], str, str]
] = {
    "fk_strategy_adapter_candidate_run": (
        "st_strategy_adapter_candidate_fact", ("candidate_run_uid",),
        "st_strategy_adapter_run_receipt", ("run_uid",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_plan_candidate_run": (
        "st_dynamic_shadow_trial_plan", ("candidate_run_uid",),
        "st_strategy_adapter_run_receipt", ("run_uid",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_plan_candidate_receipt": (
        "st_dynamic_shadow_trial_plan", ("candidate_receipt_hash",),
        "st_strategy_adapter_run_receipt", ("receipt_hash",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_plan_candidate_fact": (
        "st_dynamic_shadow_trial_plan",
        ("candidate_run_uid", "stock_code"),
        "st_strategy_adapter_candidate_fact",
        ("candidate_run_uid", "stock_code"),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_plan_candidate_fact_hash": (
        "st_dynamic_shadow_trial_plan", ("candidate_fact_hash",),
        "st_strategy_adapter_candidate_fact", ("candidate_hash",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_plan": (
        "st_dynamic_shadow_trial_chain", ("plan_id",),
        "st_dynamic_shadow_trial_plan", ("plan_id",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_intent": (
        "st_dynamic_shadow_trial_chain", ("source_intent_id",),
        "st_trade_intent_v2", ("intent_id",), "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_risk": (
        "st_dynamic_shadow_trial_chain", ("source_intent_id",),
        "st_risk_decision_v2", ("intent_id",), "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_entry_order": (
        "st_dynamic_shadow_trial_chain", ("entry_order_id",),
        "st_order_v2", ("order_id",), "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_entry_fill": (
        "st_dynamic_shadow_trial_chain", ("entry_fill_id",),
        "st_fill_v2", ("fill_id",), "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_chain_forward_evidence": (
        "st_dynamic_shadow_trial_chain", ("forward_evidence_id",),
        "st_forward_trade_evidence_v3", ("evidence_id",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_exit_chain": (
        "st_dynamic_shadow_trial_exit_binding", ("chain_id",),
        "st_dynamic_shadow_trial_chain", ("chain_id",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_exit_allocation": (
        "st_dynamic_shadow_trial_exit_binding", ("allocation_id",),
        "st_forward_exit_allocation_v3", ("allocation_id",),
        "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_exit_order": (
        "st_dynamic_shadow_trial_exit_binding", ("exit_order_id",),
        "st_order_v2", ("order_id",), "RESTRICT", "RESTRICT",
    ),
    "fk_dynamic_shadow_exit_fill": (
        "st_dynamic_shadow_trial_exit_binding", ("exit_fill_id",),
        "st_fill_v2", ("fill_id",), "RESTRICT", "RESTRICT",
    ),
}

_DYNAMIC_CHECK_CONTRACTS: dict[str, tuple[str, str]] = {
    "ck_strategy_adapter_candidate_index": (
        "st_strategy_adapter_candidate_fact", "candidate_index >= 0",
    ),
    "ck_dynamic_shadow_plan_account": (
        "st_dynamic_shadow_trial_plan", "account_id = 'paper-main-v2'",
    ),
    "ck_dynamic_shadow_plan_weight": (
        "st_dynamic_shadow_trial_plan",
        "maximum_target_bp BETWEEN 1 AND 100",
    ),
    "ck_dynamic_shadow_plan_status": (
        "st_dynamic_shadow_trial_plan",
        "plan_status = 'PLANNED_SHADOW_TRIAL'",
    ),
    "ck_dynamic_shadow_plan_no_real_auto": (
        "st_dynamic_shadow_trial_plan",
        "automatic_real_order_submission = 0",
    ),
    "ck_dynamic_shadow_plan_no_real_authority": (
        "st_dynamic_shadow_trial_plan", "real_order_authority = 0",
    ),
    "ck_dynamic_shadow_chain_exit_count": (
        "st_dynamic_shadow_trial_chain", "exit_binding_count > 0",
    ),
    "ck_dynamic_shadow_chain_no_real_auto": (
        "st_dynamic_shadow_trial_chain",
        "automatic_real_order_submission = 0",
    ),
    "ck_dynamic_shadow_chain_no_real_authority": (
        "st_dynamic_shadow_trial_chain", "real_order_authority = 0",
    ),
    "ck_dynamic_shadow_exit_no_real_authority": (
        "st_dynamic_shadow_trial_exit_binding", "real_order_authority = 0",
    ),
}


def dynamic_shadow_ledger_ddl_statements() -> tuple[str, ...]:
    """Return additive MySQL 8.4 DDL in foreign-key dependency order."""

    return (
        """
        CREATE TABLE IF NOT EXISTS st_strategy_adapter_candidate_fact (
            candidate_run_uid CHAR(32) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            candidate_index INT NOT NULL,
            trade_date DATE NOT NULL,
            candidate_json LONGTEXT NOT NULL,
            candidate_hash CHAR(64) NOT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (candidate_run_uid, stock_code),
            UNIQUE KEY uk_strategy_adapter_candidate_index
                (candidate_run_uid, candidate_index),
            UNIQUE KEY uk_strategy_adapter_candidate_hash (candidate_hash),
            CONSTRAINT ck_strategy_adapter_candidate_index
                CHECK (candidate_index >= 0),
            CONSTRAINT fk_strategy_adapter_candidate_run
                FOREIGN KEY (candidate_run_uid)
                REFERENCES st_strategy_adapter_run_receipt (run_uid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_dynamic_shadow_trial_plan (
            plan_id CHAR(64) PRIMARY KEY,
            candidate_run_uid CHAR(32) NOT NULL,
            candidate_receipt_hash CHAR(64) NOT NULL,
            strategy_key VARCHAR(80) NOT NULL,
            strategy_version VARCHAR(160) NOT NULL,
            strategy_version_hash CHAR(64) NOT NULL,
            execution_binding_hash CHAR(64) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            account_id VARCHAR(64) NOT NULL,
            maximum_target_bp INT NOT NULL,
            candidate_fact_hash CHAR(64) NOT NULL,
            candidate_signal_json LONGTEXT NOT NULL,
            candidate_signal_hash CHAR(64) NOT NULL,
            plan_payload_json LONGTEXT NOT NULL,
            plan_hash CHAR(64) NOT NULL,
            plan_status VARCHAR(32) NOT NULL,
            automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
            real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_dynamic_shadow_candidate_stock
                (candidate_run_uid, stock_code),
            UNIQUE KEY uk_dynamic_shadow_plan_hash (plan_hash),
            KEY idx_dynamic_shadow_plan_strategy
                (strategy_key, strategy_version, trade_date),
            KEY idx_dynamic_shadow_plan_receipt (candidate_receipt_hash),
            KEY idx_dynamic_shadow_plan_fact_hash (candidate_fact_hash),
            CONSTRAINT fk_dynamic_shadow_plan_candidate_run
                FOREIGN KEY (candidate_run_uid)
                REFERENCES st_strategy_adapter_run_receipt (run_uid),
            CONSTRAINT fk_dynamic_shadow_plan_candidate_receipt
                FOREIGN KEY (candidate_receipt_hash)
                REFERENCES st_strategy_adapter_run_receipt (receipt_hash),
            CONSTRAINT fk_dynamic_shadow_plan_candidate_fact
                FOREIGN KEY (candidate_run_uid, stock_code)
                REFERENCES st_strategy_adapter_candidate_fact
                    (candidate_run_uid, stock_code),
            CONSTRAINT fk_dynamic_shadow_plan_candidate_fact_hash
                FOREIGN KEY (candidate_fact_hash)
                REFERENCES st_strategy_adapter_candidate_fact
                    (candidate_hash),
            CONSTRAINT ck_dynamic_shadow_plan_account
                CHECK (account_id = 'paper-main-v2'),
            CONSTRAINT ck_dynamic_shadow_plan_weight
                CHECK (maximum_target_bp BETWEEN 1 AND 100),
            CONSTRAINT ck_dynamic_shadow_plan_status
                CHECK (plan_status = 'PLANNED_SHADOW_TRIAL'),
            CONSTRAINT ck_dynamic_shadow_plan_no_real_auto
                CHECK (automatic_real_order_submission = 0),
            CONSTRAINT ck_dynamic_shadow_plan_no_real_authority
                CHECK (real_order_authority = 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_dynamic_shadow_trial_chain (
            chain_id CHAR(64) PRIMARY KEY,
            plan_id CHAR(64) NOT NULL,
            source_intent_id VARCHAR(64) NOT NULL,
            entry_order_id VARCHAR(64) NOT NULL,
            entry_fill_id VARCHAR(64) NOT NULL,
            forward_evidence_id CHAR(64) NOT NULL,
            intent_fact_hash CHAR(64) NOT NULL,
            risk_decision_fact_hash CHAR(64) NOT NULL,
            entry_order_fact_hash CHAR(64) NOT NULL,
            entry_fill_fact_hash CHAR(64) NOT NULL,
            forward_evidence_fact_hash CHAR(64) NOT NULL,
            exit_set_hash CHAR(64) NOT NULL,
            exit_binding_count INT NOT NULL,
            chain_payload_json LONGTEXT NOT NULL,
            chain_hash CHAR(64) NOT NULL,
            automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
            real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_dynamic_shadow_chain_plan (plan_id),
            UNIQUE KEY uk_dynamic_shadow_chain_evidence
                (forward_evidence_id),
            UNIQUE KEY uk_dynamic_shadow_chain_hash (chain_hash),
            KEY idx_dynamic_shadow_chain_intent (source_intent_id),
            KEY idx_dynamic_shadow_chain_entry_order (entry_order_id),
            KEY idx_dynamic_shadow_chain_entry_fill (entry_fill_id),
            CONSTRAINT fk_dynamic_shadow_chain_plan
                FOREIGN KEY (plan_id)
                REFERENCES st_dynamic_shadow_trial_plan (plan_id),
            CONSTRAINT fk_dynamic_shadow_chain_intent
                FOREIGN KEY (source_intent_id)
                REFERENCES st_trade_intent_v2 (intent_id),
            CONSTRAINT fk_dynamic_shadow_chain_risk
                FOREIGN KEY (source_intent_id)
                REFERENCES st_risk_decision_v2 (intent_id),
            CONSTRAINT fk_dynamic_shadow_chain_entry_order
                FOREIGN KEY (entry_order_id)
                REFERENCES st_order_v2 (order_id),
            CONSTRAINT fk_dynamic_shadow_chain_entry_fill
                FOREIGN KEY (entry_fill_id)
                REFERENCES st_fill_v2 (fill_id),
            CONSTRAINT fk_dynamic_shadow_chain_forward_evidence
                FOREIGN KEY (forward_evidence_id)
                REFERENCES st_forward_trade_evidence_v3 (evidence_id),
            CONSTRAINT ck_dynamic_shadow_chain_exit_count
                CHECK (exit_binding_count > 0),
            CONSTRAINT ck_dynamic_shadow_chain_no_real_auto
                CHECK (automatic_real_order_submission = 0),
            CONSTRAINT ck_dynamic_shadow_chain_no_real_authority
                CHECK (real_order_authority = 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS st_dynamic_shadow_trial_exit_binding (
            binding_id CHAR(64) PRIMARY KEY,
            chain_id CHAR(64) NOT NULL,
            allocation_id CHAR(64) NOT NULL,
            exit_order_id VARCHAR(64) NOT NULL,
            exit_fill_id VARCHAR(64) NOT NULL,
            allocation_fact_hash CHAR(64) NOT NULL,
            exit_order_fact_hash CHAR(64) NOT NULL,
            exit_fill_fact_hash CHAR(64) NOT NULL,
            binding_payload_json LONGTEXT NOT NULL,
            binding_hash CHAR(64) NOT NULL,
            real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_dynamic_shadow_exit_allocation
                (chain_id, allocation_id),
            UNIQUE KEY uk_dynamic_shadow_exit_hash (binding_hash),
            KEY idx_dynamic_shadow_exit_fill (exit_fill_id),
            KEY idx_dynamic_shadow_exit_allocation (allocation_id),
            KEY idx_dynamic_shadow_exit_order (exit_order_id),
            CONSTRAINT fk_dynamic_shadow_exit_chain
                FOREIGN KEY (chain_id)
                REFERENCES st_dynamic_shadow_trial_chain (chain_id),
            CONSTRAINT fk_dynamic_shadow_exit_allocation
                FOREIGN KEY (allocation_id)
                REFERENCES st_forward_exit_allocation_v3 (allocation_id),
            CONSTRAINT fk_dynamic_shadow_exit_order
                FOREIGN KEY (exit_order_id)
                REFERENCES st_order_v2 (order_id),
            CONSTRAINT fk_dynamic_shadow_exit_fill
                FOREIGN KEY (exit_fill_id)
                REFERENCES st_fill_v2 (fill_id),
            CONSTRAINT ck_dynamic_shadow_exit_no_real_authority
                CHECK (real_order_authority = 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    )


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dynamic_schema_contract_payload() -> dict[str, Any]:
    return {
        "schema": "probiga.dynamic-shadow-ledger-schema.v1",
        "engine": _DYNAMIC_TABLE_ENGINE,
        "collation": _DYNAMIC_TABLE_COLLATION,
        "tables": {
            table_name: {
                "columns": [
                    {
                        key: value for key, value in column.items()
                        if key != "ddl"
                    }
                    for column in _DYNAMIC_COLUMN_CONTRACTS[table_name]
                ],
                "indexes": {
                    name: {
                        "unique": unique,
                        "columns": list(columns),
                    }
                    for name, (unique, columns, _ddl) in sorted(
                        _DYNAMIC_INDEX_CONTRACTS[table_name].items()
                    )
                },
                "foreign_keys": {
                    name: {
                        "columns": list(columns),
                        "parent_table": parent_table,
                        "parent_columns": list(parent_columns),
                        "update_rule": update_rule,
                        "delete_rule": delete_rule,
                    }
                    for name, (
                        child_table, columns, parent_table, parent_columns,
                        update_rule, delete_rule,
                    ) in sorted(_DYNAMIC_FOREIGN_KEY_CONTRACTS.items())
                    if child_table == table_name
                },
                "checks": {
                    name: clause
                    for name, (child_table, clause) in sorted(
                        _DYNAMIC_CHECK_CONTRACTS.items()
                    )
                    if child_table == table_name
                },
            }
            for table_name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
        },
    }


DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH = _canonical_hash(
    _dynamic_schema_contract_payload()
)


def _rows(connection, sql: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(sql)).mappings().all()
    ]


def _dynamic_schema_snapshot(connection) -> dict[str, Any]:
    placeholders = ", ".join(
        f"'{name}'" for name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    )
    tables = _rows(
        connection,
        "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
        "TABLE_COLLATION AS table_collation "
        "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY TABLE_NAME",
    )
    columns = _rows(
        connection,
        "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
        "ORDINAL_POSITION AS ordinal_position, COLUMN_TYPE AS column_type, "
        "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, "
        "EXTRA AS extra, CHARACTER_SET_NAME AS character_set_name, "
        "COLLATION_NAME AS collation_name "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY TABLE_NAME, ORDINAL_POSITION",
    )
    indexes = _rows(
        connection,
        "SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, "
        "NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
        "COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
        "INDEX_TYPE AS index_type "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY TABLE_NAME, BINARY INDEX_NAME, SEQ_IN_INDEX",
    )
    foreign_keys = _rows(
        connection,
        "SELECT tc.TABLE_NAME AS table_name, "
        "tc.CONSTRAINT_NAME AS constraint_name, "
        "kcu.ORDINAL_POSITION AS ordinal_position, "
        "kcu.COLUMN_NAME AS column_name, "
        "kcu.REFERENCED_TABLE_NAME AS referenced_table_name, "
        "kcu.REFERENCED_COLUMN_NAME AS referenced_column_name, "
        "rc.UPDATE_RULE AS update_rule, rc.DELETE_RULE AS delete_rule "
        "FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.KEY_COLUMN_USAGE kcu "
        "ON kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND kcu.TABLE_NAME=tc.TABLE_NAME "
        "AND kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "JOIN information_schema.REFERENTIAL_CONSTRAINTS rc "
        "ON rc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND rc.TABLE_NAME=tc.TABLE_NAME "
        "AND rc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() "
        "AND tc.CONSTRAINT_TYPE='FOREIGN KEY' "
        f"AND tc.TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
    )
    checks = _rows(
        connection,
        "SELECT tc.TABLE_NAME AS table_name, "
        "tc.CONSTRAINT_NAME AS constraint_name, "
        "cc.CHECK_CLAUSE AS check_clause "
        "FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() "
        "AND tc.CONSTRAINT_TYPE='CHECK' "
        f"AND tc.TABLE_NAME IN ({placeholders}) "
        "ORDER BY BINARY tc.CONSTRAINT_NAME",
    )
    return {
        "tables": tables,
        "columns": columns,
        "indexes": indexes,
        "foreign_keys": foreign_keys,
        "checks": checks,
    }


def _normalized_default(value: Any, column_type: str) -> Any:
    if value is None:
        return None
    raw = str(value).strip()
    base_type = column_type.split("(", 1)[0]
    if base_type in {"tinyint", "int", "bigint"}:
        try:
            return int(raw)
        except ValueError:
            return raw.casefold()
    if base_type in {"datetime", "timestamp"}:
        return raw.casefold().replace("()", "")
    return raw


def _normalized_check_clause(value: Any) -> str:
    clause = str(value or "").strip().casefold().replace("`", "")
    clause = re.sub(r"_[a-z0-9]+(?=')", "", clause)

    def fully_wrapped(raw: str) -> bool:
        if not raw.startswith("(") or not raw.endswith(")"):
            return False
        depth = 0
        quoted = False
        index = 0
        while index < len(raw):
            char = raw[index]
            if char == "'":
                if quoted and index + 1 < len(raw) and raw[index + 1] == "'":
                    index += 2
                    continue
                quoted = not quoted
            elif not quoted:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0 and index != len(raw) - 1:
                        return False
                    if depth < 0:
                        return False
            index += 1
        return depth == 0 and not quoted

    while fully_wrapped(clause):
        clause = clause[1:-1].strip()
    return re.sub(r"\s+", "", clause)


def _column_matches(row: dict[str, Any], expected: dict[str, Any]) -> bool:
    extra = str(row.get("extra") or "").casefold().strip()
    # MySQL versions differ on exposing DEFAULT_GENERATED for an otherwise
    # identical default.  It is metadata decoration, not write authority.
    extra = re.sub(r"\bdefault_generated\b", "", extra).strip()
    return bool(
        str(row.get("column_type") or "").casefold()
        == expected["column_type"]
        and str(row.get("is_nullable") or "").upper()
        == expected["is_nullable"]
        and _normalized_default(
            row.get("column_default"), expected["column_type"]
        )
        == _normalized_default(
            expected["column_default"], expected["column_type"]
        )
        and not extra
        and (
            str(row.get("character_set_name") or "").casefold() or None
        ) == expected["character_set_name"]
        and (
            str(row.get("collation_name") or "").casefold() or None
        ) == expected["collation_name"]
    )


def _assess_dynamic_schema(
    snapshot: dict[str, Any],
) -> tuple[dict[str, dict[str, list[str]]], list[str]]:
    """Return additive gaps and immutable/incompatible drift errors."""

    gaps = {
        table_name: {
            "columns": [], "indexes": [], "foreign_keys": [], "checks": [],
        }
        for table_name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    }
    errors: list[str] = []
    observed_tables = {
        str(row.get("table_name") or ""): row
        for row in snapshot["tables"]
    }
    unexpected_tables = set(observed_tables) - set(
        DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    )
    if unexpected_tables:
        errors.append(
            "unexpected dynamic table metadata: "
            + ",".join(sorted(unexpected_tables))
        )

    columns_by_table: dict[str, list[dict[str, Any]]] = {
        name: [] for name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    }
    for row in snapshot["columns"]:
        table_name = str(row.get("table_name") or "")
        if table_name in columns_by_table:
            columns_by_table[table_name].append(row)
        else:
            errors.append(f"unexpected column owner: {table_name}")

    indexes_by_table: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: {} for name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    }
    for row in snapshot["indexes"]:
        table_name = str(row.get("table_name") or "")
        if table_name not in indexes_by_table:
            errors.append(f"unexpected index owner: {table_name}")
            continue
        indexes_by_table[table_name].setdefault(
            str(row.get("index_name") or ""), []
        ).append(row)

    fks_by_name: dict[str, list[dict[str, Any]]] = {}
    for row in snapshot["foreign_keys"]:
        fks_by_name.setdefault(
            str(row.get("constraint_name") or ""), []
        ).append(row)

    checks_by_name: dict[str, list[dict[str, Any]]] = {}
    for row in snapshot["checks"]:
        checks_by_name.setdefault(
            str(row.get("constraint_name") or ""), []
        ).append(row)

    for table_name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES:
        table = observed_tables.get(table_name)
        if table is None:
            gaps[table_name]["columns"] = [
                item["name"] for item in _DYNAMIC_COLUMN_CONTRACTS[table_name]
            ]
            gaps[table_name]["indexes"] = list(
                _DYNAMIC_INDEX_CONTRACTS[table_name]
            )
            gaps[table_name]["foreign_keys"] = [
                name for name, contract in _DYNAMIC_FOREIGN_KEY_CONTRACTS.items()
                if contract[0] == table_name
            ]
            gaps[table_name]["checks"] = [
                name for name, contract in _DYNAMIC_CHECK_CONTRACTS.items()
                if contract[0] == table_name
            ]
            continue
        if (
            str(table.get("engine") or "").casefold()
            != _DYNAMIC_TABLE_ENGINE.casefold()
            or str(table.get("table_collation") or "").casefold()
            != _DYNAMIC_TABLE_COLLATION
        ):
            errors.append(f"dynamic table engine/collation drift: {table_name}")

        expected_columns = _DYNAMIC_COLUMN_CONTRACTS[table_name]
        expected_by_name = {item["name"]: item for item in expected_columns}
        rows = sorted(
            columns_by_table[table_name],
            key=lambda row: int(row.get("ordinal_position") or 0),
        )
        observed_names = [str(row.get("column_name") or "") for row in rows]
        unexpected_columns = set(observed_names) - set(expected_by_name)
        if unexpected_columns or len(observed_names) != len(set(observed_names)):
            errors.append(f"dynamic column set drift: {table_name}")
        expected_subsequence = [
            item["name"] for item in expected_columns
            if item["name"] in observed_names
        ]
        if (
            observed_names != expected_subsequence
            or [int(row.get("ordinal_position") or 0) for row in rows]
            != list(range(1, len(rows) + 1))
        ):
            errors.append(f"dynamic column order drift: {table_name}")
        for row in rows:
            name = str(row.get("column_name") or "")
            expected = expected_by_name.get(name)
            if expected is not None and not _column_matches(row, expected):
                errors.append(f"dynamic column definition drift: {table_name}.{name}")
        gaps[table_name]["columns"] = [
            item["name"] for item in expected_columns
            if item["name"] not in observed_names
        ]

        expected_indexes = _DYNAMIC_INDEX_CONTRACTS[table_name]
        observed_indexes = indexes_by_table[table_name]
        if set(observed_indexes) - set(expected_indexes):
            errors.append(f"dynamic index-name set drift: {table_name}")
        for name, (unique, columns, _ddl) in expected_indexes.items():
            index_rows = sorted(
                observed_indexes.get(name, []),
                key=lambda row: int(row.get("seq_in_index") or 0),
            )
            if not index_rows:
                gaps[table_name]["indexes"].append(name)
                continue
            valid = bool(
                [int(row.get("seq_in_index") or 0) for row in index_rows]
                == list(range(1, len(index_rows) + 1))
                and tuple(
                    str(row.get("column_name") or "") for row in index_rows
                ) == columns
                and all(
                    int(row.get("non_unique") or 0) == (0 if unique else 1)
                    and row.get("sub_part") is None
                    and str(row.get("index_type") or "").upper() == "BTREE"
                    for row in index_rows
                )
            )
            if not valid:
                errors.append(f"dynamic index definition drift: {table_name}.{name}")

    unexpected_fks = set(fks_by_name) - set(_DYNAMIC_FOREIGN_KEY_CONTRACTS)
    if unexpected_fks:
        errors.append("dynamic foreign-key name set drift")
    for name, expected in _DYNAMIC_FOREIGN_KEY_CONTRACTS.items():
        table_name, columns, parent_table, parent_columns, update, delete = expected
        rows = sorted(
            fks_by_name.get(name, []),
            key=lambda row: int(row.get("ordinal_position") or 0),
        )
        if not rows:
            if table_name in observed_tables:
                gaps[table_name]["foreign_keys"].append(name)
            continue
        observed = (
            str(rows[0].get("table_name") or ""),
            tuple(str(row.get("column_name") or "") for row in rows),
            str(rows[0].get("referenced_table_name") or ""),
            tuple(
                str(row.get("referenced_column_name") or "") for row in rows
            ),
            str(rows[0].get("update_rule") or "").upper(),
            str(rows[0].get("delete_rule") or "").upper(),
        )
        consistent = all(
            str(row.get("table_name") or "") == observed[0]
            and str(row.get("referenced_table_name") or "") == observed[2]
            and str(row.get("update_rule") or "").upper() == observed[4]
            and str(row.get("delete_rule") or "").upper() == observed[5]
            for row in rows
        )
        if (
            observed != expected
            or [int(row.get("ordinal_position") or 0) for row in rows]
            != list(range(1, len(rows) + 1))
            or not consistent
        ):
            errors.append(f"dynamic foreign-key definition drift: {name}")

    unexpected_checks = set(checks_by_name) - set(_DYNAMIC_CHECK_CONTRACTS)
    if unexpected_checks:
        errors.append("dynamic check-constraint name set drift")
    for name, (table_name, clause) in _DYNAMIC_CHECK_CONTRACTS.items():
        rows = checks_by_name.get(name, [])
        if not rows:
            if table_name in observed_tables:
                gaps[table_name]["checks"].append(name)
            continue
        if (
            len(rows) != 1
            or str(rows[0].get("table_name") or "") != table_name
            or _normalized_check_clause(rows[0].get("check_clause"))
            != _normalized_check_clause(clause)
        ):
            errors.append(f"dynamic check definition drift: {name}")
    return gaps, errors


def _gap_count(gaps: dict[str, dict[str, list[str]]]) -> int:
    return sum(
        len(values)
        for table in gaps.values()
        for values in table.values()
    )


def validate_dynamic_shadow_ledger_schema(connection) -> dict[str, Any]:
    """Read-only exact startup gate for the four dynamic shadow tables."""

    snapshot = _dynamic_schema_snapshot(connection)
    gaps, errors = _assess_dynamic_schema(snapshot)
    if _gap_count(gaps):
        missing = {
            table: values for table, values in gaps.items()
            if any(values.values())
        }
        errors.append(
            "dynamic schema is incomplete: "
            + json.dumps(missing, ensure_ascii=False, sort_keys=True)
        )
    if errors:
        raise RuntimeError("；".join(errors[:20]))
    return {
        "scope": "dynamic_shadow_ledger",
        "table_count": len(DYNAMIC_SHADOW_LEDGER_TABLE_NAMES),
        "column_count": sum(
            len(columns) for columns in _DYNAMIC_COLUMN_CONTRACTS.values()
        ),
        "index_count": sum(
            len(indexes) for indexes in _DYNAMIC_INDEX_CONTRACTS.values()
        ),
        "foreign_key_count": len(_DYNAMIC_FOREIGN_KEY_CONTRACTS),
        "check_count": len(_DYNAMIC_CHECK_CONTRACTS),
        "contract_hash": DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _dynamic_table_row_counts(
    connection,
    existing_tables: set[str],
) -> dict[str, int]:
    counts = {name: 0 for name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES}
    for table_name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES:
        if table_name not in existing_tables:
            continue
        row = connection.execute(text(
            f"SELECT COUNT(*) AS row_count FROM `{table_name}`"
        )).mappings().first()
        counts[table_name] = int((row or {}).get("row_count") or 0)
    return counts


def preflight_dynamic_shadow_ledger_schema_upgrade(
    connection,
) -> dict[str, Any]:
    """Read-only proof that the dynamic ledger is safe to prepare.

    A completely absent schema, an exact schema, or an incomplete but empty
    schema is admissible.  Any incompatible object or any incomplete table
    that already contains rows is rejected before the writer-fenced cutover.
    """

    snapshot = _dynamic_schema_snapshot(connection)
    gaps, errors = _assess_dynamic_schema(snapshot)
    if errors:
        raise RuntimeError("；".join(errors[:20]))
    existing_tables = {
        str(row.get("table_name") or "") for row in snapshot["tables"]
    }
    row_counts = _dynamic_table_row_counts(connection, existing_tables)
    missing_object_count = _gap_count(gaps)
    if missing_object_count and sum(row_counts.values()) != 0:
        raise RuntimeError(
            "旧动态影子账本结构不完整且已有数据；禁止在切换窗口执行增量修复。"
            "row_counts="
            + json.dumps(row_counts, ensure_ascii=False, sort_keys=True)
        )
    if not existing_tables:
        status = "ABSENT_CREATE_ALLOWED"
    elif missing_object_count:
        status = "EMPTY_ADDITIVE_UPGRADE_ALLOWED"
    else:
        status = "EXACT"
    return {
        "scope": "dynamic_shadow_ledger",
        "status": status,
        "existing_table_count": len(existing_tables),
        "expected_table_count": len(DYNAMIC_SHADOW_LEDGER_TABLE_NAMES),
        "missing_object_count": missing_object_count,
        "row_counts": row_counts,
        "contract_hash": DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _foreign_key_add_clause(name: str) -> str:
    (
        _table_name, columns, parent_table, parent_columns,
        update_rule, delete_rule,
    ) = _DYNAMIC_FOREIGN_KEY_CONTRACTS[name]
    child_sql = ", ".join(f"`{column}`" for column in columns)
    parent_sql = ", ".join(f"`{column}`" for column in parent_columns)
    return (
        f"CONSTRAINT `{name}` FOREIGN KEY ({child_sql}) "
        f"REFERENCES `{parent_table}` ({parent_sql}) "
        f"ON UPDATE {update_rule} ON DELETE {delete_rule}"
    )


def _alter_statement_for_gaps(
    table_name: str,
    table_gaps: dict[str, list[str]],
) -> str:
    clauses: list[str] = []
    columns = _DYNAMIC_COLUMN_CONTRACTS[table_name]
    by_name = {item["name"]: item for item in columns}
    expected_names = [item["name"] for item in columns]
    for name in table_gaps["columns"]:
        index = expected_names.index(name)
        position = " FIRST" if index == 0 else f" AFTER `{expected_names[index - 1]}`"
        clauses.append(f"ADD COLUMN {by_name[name]['ddl']}{position}")
    for name in table_gaps["indexes"]:
        clauses.append(f"ADD {_DYNAMIC_INDEX_CONTRACTS[table_name][name][2]}")
    for name in table_gaps["foreign_keys"]:
        clauses.append(f"ADD {_foreign_key_add_clause(name)}")
    for name in table_gaps["checks"]:
        _owner, clause = _DYNAMIC_CHECK_CONTRACTS[name]
        clauses.append(f"ADD CONSTRAINT `{name}` CHECK ({clause})")
    if not clauses:
        return ""
    return f"ALTER TABLE `{table_name}`\n  " + ",\n  ".join(clauses)


def ensure_dynamic_shadow_ledger_schema(
    connection,
    *,
    writers_fenced: bool = False,
) -> dict[str, Any]:
    """Create/upgrade only additive, empty dynamic tables under writer fence.

    This helper deliberately neither opens nor commits a transaction.  The
    schema-preparation broker must pass its existing connection while every
    application writer is fenced.  MySQL DDL may auto-commit, so the complete
    read-only preflight and all four row counts happen before the first DDL.
    """

    if writers_fenced is not True:
        raise RuntimeError("dynamic schema upgrade requires the writer fence")
    snapshot = _dynamic_schema_snapshot(connection)
    gaps, errors = _assess_dynamic_schema(snapshot)
    if errors:
        raise RuntimeError("；".join(errors[:20]))
    missing_count = _gap_count(gaps)
    if missing_count == 0:
        validation = validate_dynamic_shadow_ledger_schema(connection)
        return {
            **validation,
            "upgrade_status": "UNCHANGED",
            "executed_statement_count": 0,
            "row_counts": {},
        }

    existing_tables = {
        str(row.get("table_name") or "") for row in snapshot["tables"]
    }
    row_counts = _dynamic_table_row_counts(connection, existing_tables)
    if sum(row_counts.values()) != 0:
        raise RuntimeError(
            "旧动态影子账本结构不完整且已有数据；禁止伪造回填或在线改写，"
            "必须先隔离/导出后按冻结合同重建。row_counts="
            + json.dumps(row_counts, ensure_ascii=False, sort_keys=True)
        )

    create_by_table = dict(zip(
        DYNAMIC_SHADOW_LEDGER_TABLE_NAMES,
        dynamic_shadow_ledger_ddl_statements(),
    ))
    statements: list[str] = []
    for table_name in DYNAMIC_SHADOW_LEDGER_TABLE_NAMES:
        if table_name not in existing_tables:
            statements.append(create_by_table[table_name])
            continue
        statement = _alter_statement_for_gaps(table_name, gaps[table_name])
        if statement:
            statements.append(statement)
    for statement in statements:
        connection.execute(text(statement))

    validation = validate_dynamic_shadow_ledger_schema(connection)
    return {
        **validation,
        "upgrade_status": (
            "CREATED" if not existing_tables else "UPGRADED"
        ),
        "executed_statement_count": len(statements),
        "row_counts": row_counts,
    }


__all__ = [
    "DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH",
    "DYNAMIC_SHADOW_LEDGER_TABLE_NAMES",
    "dynamic_shadow_ledger_ddl_statements",
    "ensure_dynamic_shadow_ledger_schema",
    "preflight_dynamic_shadow_ledger_schema_upgrade",
    "validate_dynamic_shadow_ledger_schema",
]
