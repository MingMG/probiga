# -*- coding: utf-8 -*-
"""Frozen append-only funding facts and exact incremental checkpoint chain.

The daily fact table is the single authoritative rolling NAV/exposure source.
Checkpoint rows contain only capital state, open holdings and an exact pointer
to a bounded, addressable fact-chain tip. Neither table grants order authority.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from decimal import Decimal
from typing import Any, Callable

from sqlalchemy import text


FUNDING_DAILY_FACT_TABLE_NAME = "st_strategy_funding_daily_fact"
FUNDING_CHECKPOINT_TABLE_NAME = "st_strategy_funding_checkpoint"
FUNDING_DAILY_FACT_SCHEMA = "probiga.strategy-funding-daily-fact.v1"
FUNDING_CHECKPOINT_SCHEMA = "probiga.strategy-funding-checkpoint.v2"
FUNDING_CHECKPOINT_CHAIN_SCHEMA = "probiga.strategy-funding-checkpoint-chain.v1"
FUNDING_CHECKPOINT_AUDIT_SCHEMA = "probiga.strategy-funding-checkpoint-audit-anchor.v1"
FUNDING_REPLAY_MAX_SESSIONS = 120
FUNDING_REPLAY_MAX_HOLDING_DAYS = 250
FUNDING_INCREMENTAL_MAX_SESSIONS = 370
FUNDING_CHECKPOINT_TARGET_AVG_BYTES = 8 * 1024
FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES = 8 * 1024 * 1024
FUNDING_CHECKPOINT_TOTAL_HARD_BYTES = 16 * 1024 * 1024
FUNDING_CHECKPOINT_BATCH_MAX_ROWS = 100
FUNDING_CHECKPOINT_BATCH_MAX_BYTES = 4 * 1024 * 1024
FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES = 1 * 1024 * 1024
FUNDING_CHECKPOINT_AUDIT_MAX_BYTES = 128 * 1024
FUNDING_CHECKPOINT_MIGRATION_KEY = "20260824_003_strategy_funding_checkpoint"
_COLLATION = "utf8mb4_unicode_ci"
_ENGINE = "InnoDB"


def _canonical_value(value: Any, *, path: str = "$") -> Any:
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"canonical JSON contains non-finite float at {path}")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"canonical JSON contains non-finite Decimal at {path}")
        return format(value, "f")
    if isinstance(value, list):
        return [
            _canonical_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical JSON key is not text at {path}")
            normalized[key] = _canonical_value(item, path=f"{path}.{key}")
        return normalized
    raise TypeError(
        f"canonical JSON contains unsupported {type(value).__name__} at {path}"
    )


def canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def checkpoint_state_hash(state: dict[str, Any]) -> str:
    if state.get("schema") != FUNDING_CHECKPOINT_SCHEMA:
        raise ValueError("资金检查点状态协议无效")
    return canonical_hash(state)


def funding_daily_fact_hash(fact: dict[str, Any]) -> str:
    if fact.get("schema") != FUNDING_DAILY_FACT_SCHEMA:
        raise ValueError("资金日频事实协议无效")
    return canonical_hash(fact)


def funding_daily_fact_identity(*, entity_type: str, entity_key: str,
                                entity_version: str, account_id: str,
                                trade_date: str, anchor_run_uid: str) -> str:
    return canonical_hash({
        "schema": "probiga.strategy-funding-daily-fact-identity.v1",
        "entity_type": entity_type, "entity_key": entity_key,
        "entity_version": entity_version, "account_id": account_id,
        "trade_date": trade_date, "anchor_run_uid": anchor_run_uid,
    })


def ordered_funding_fact_set_hash(rows: list[dict[str, Any]]) -> str:
    return canonical_hash({
        "schema": "probiga.strategy-funding-daily-fact-set.v1",
        "members": [{"fact_id": str(row.get("fact_id") or ""),
                     "fact_hash": str(row.get("fact_hash") or "")}
                    for row in rows],
    })


def checkpoint_chain_payload(*, checkpoint_hash: str,
                             previous_checkpoint_id: str,
                             previous_checkpoint_hash: str,
                             previous_chain_hash: str) -> dict[str, str]:
    return {"schema": FUNDING_CHECKPOINT_CHAIN_SCHEMA,
            "checkpoint_hash": checkpoint_hash,
            "previous_checkpoint_id": previous_checkpoint_id,
            "previous_checkpoint_hash": previous_checkpoint_hash,
            "previous_chain_hash": previous_chain_hash}


def checkpoint_chain_hash(*, checkpoint_hash: str,
                          previous_checkpoint_id: str = "",
                          previous_checkpoint_hash: str = "",
                          previous_chain_hash: str = "") -> str:
    return canonical_hash(checkpoint_chain_payload(
        checkpoint_hash=checkpoint_hash,
        previous_checkpoint_id=previous_checkpoint_id,
        previous_checkpoint_hash=previous_checkpoint_hash,
        previous_chain_hash=previous_chain_hash))


def checkpoint_identity(*, strategy_key: str, strategy_version: str,
                        account_id: str, trade_date: str,
                        anchor_run_uid: str) -> str:
    return canonical_hash({
        "schema": "probiga.strategy-funding-checkpoint-identity.v1",
        "strategy_key": strategy_key, "strategy_version": strategy_version,
        "account_id": account_id, "trade_date": trade_date,
        "anchor_run_uid": anchor_run_uid,
    })


_DAILY_FACT_COLUMNS = (
    ("fact_id", "char(64)", "NO", None),
    ("entity_type", "varchar(16)", "NO", None),
    ("entity_key", "varchar(160)", "NO", None),
    ("entity_version", "varchar(160)", "NO", None),
    ("entity_version_hash", "char(64)", "NO", None),
    ("execution_binding_hash", "char(64)", "YES", None),
    ("account_id", "varchar(64)", "NO", None),
    ("trade_date", "date", "NO", None),
    ("origin_checkpoint_id", "char(64)", "NO", None),
    ("previous_fact_id", "char(64)", "YES", None),
    ("previous_fact_hash", "char(64)", "YES", None),
    ("opening_cash_cny", "decimal(24,6)", "NO", None),
    ("closing_cash_cny", "decimal(24,6)", "NO", None),
    ("opening_equity_cny", "decimal(24,6)", "NO", None),
    ("closing_equity_cny", "decimal(24,6)", "NO", None),
    ("daily_return_pct", "decimal(24,12)", "NO", None),
    ("cumulative_fee_cny", "decimal(24,6)", "NO", None),
    ("high_watermark_equity_cny", "decimal(24,6)", "NO", None),
    ("stock_exposure_json", "longtext", "NO", None),
    ("closed_evidence_ids_json", "longtext", "NO", None),
    ("fact_json", "longtext", "NO", None),
    ("fact_hash", "char(64)", "NO", None),
    ("anchor_run_uid", "char(32)", "NO", None),
    ("canonical_result_hash", "char(64)", "NO", None),
    ("anchor_audit_id", "char(32)", "NO", None),
    ("anchor_audit_hash", "char(64)", "NO", None),
    ("automatic_real_order_submission", "tinyint(1)", "NO", "0"),
    ("real_order_authority", "tinyint(1)", "NO", "0"),
    ("created_at", "datetime(6)", "NO", "current_timestamp(6)"),
)
_DAILY_FACT_INDEXES = {
    "PRIMARY": (0, ("fact_id",)),
    "uk_strategy_funding_daily_fact_identity": (0, (
        "entity_type", "entity_key", "entity_version", "account_id",
        "trade_date", "anchor_run_uid")),
    "idx_strategy_funding_daily_fact_lookup": (1, (
        "entity_type", "entity_key", "entity_version", "account_id", "trade_date")),
    "idx_strategy_funding_daily_fact_previous": (1, ("previous_fact_id",)),
    "idx_strategy_funding_daily_fact_origin": (1, ("origin_checkpoint_id",)),
    "idx_strategy_funding_daily_fact_hash": (1, ("fact_hash",)),
    "idx_strategy_funding_daily_fact_account": (1, ("account_id",)),
    "idx_strategy_funding_daily_fact_run": (1, ("anchor_run_uid",)),
    "idx_strategy_funding_daily_fact_audit": (1, ("anchor_audit_id",)),
}
_DAILY_FACT_FOREIGN_KEYS = {
    "fk_strategy_funding_daily_fact_previous": (
        ("previous_fact_id",), FUNDING_DAILY_FACT_TABLE_NAME, ("fact_id",),
        "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_daily_fact_run": (
        ("anchor_run_uid",), "st_strategy_governance_run", ("run_uid",),
        "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_daily_fact_audit": (
        ("anchor_audit_id",), "st_strategy_governance_audit", ("audit_id",),
        "RESTRICT", "RESTRICT"),
}
_DAILY_FACT_CHECKS = {
    "ck_strategy_funding_daily_fact_entity":
        "entity_type = 'STRATEGY'",
    "ck_strategy_funding_daily_fact_predecessor":
        "(previous_fact_id IS NULL AND previous_fact_hash IS NULL) OR "
        "(previous_fact_id IS NOT NULL AND previous_fact_hash IS NOT NULL)",
    "ck_strategy_funding_daily_fact_money":
        "opening_cash_cny >= 0 AND closing_cash_cny >= 0 "
        "AND opening_equity_cny > 0 AND closing_equity_cny > 0 "
        "AND cumulative_fee_cny >= 0 AND high_watermark_equity_cny > 0",
    "ck_strategy_funding_daily_fact_json":
        "JSON_VALID(stock_exposure_json) "
        "AND JSON_VALID(closed_evidence_ids_json) AND JSON_VALID(fact_json)",
    "ck_strategy_funding_daily_fact_hash":
        "BINARY fact_hash = BINARY SHA2(fact_json, 256)",
    "ck_strategy_funding_daily_fact_no_real_auto":
        "automatic_real_order_submission = 0",
    "ck_strategy_funding_daily_fact_no_real_authority":
        "real_order_authority = 0",
}

_CHECKPOINT_COLUMNS = (
    ("checkpoint_id", "char(64)", "NO", None),
    ("strategy_key", "varchar(80)", "NO", None),
    ("strategy_version", "varchar(160)", "NO", None),
    ("strategy_version_hash", "char(64)", "NO", None),
    ("execution_binding_hash", "char(64)", "YES", None),
    ("account_id", "varchar(64)", "NO", None),
    ("trade_date", "date", "NO", None),
    ("replay_mode", "varchar(24)", "NO", None),
    ("replay_start_date", "date", "NO", None),
    ("replay_session_count", "int", "NO", None),
    ("max_holding_days", "int", "NO", None),
    ("opening_cash_cny", "decimal(24,6)", "NO", None),
    ("closing_cash_cny", "decimal(24,6)", "NO", None),
    ("opening_equity_cny", "decimal(24,6)", "NO", None),
    ("closing_equity_cny", "decimal(24,6)", "NO", None),
    ("cumulative_fee_cny", "decimal(24,6)", "NO", None),
    ("high_watermark_equity_cny", "decimal(24,6)", "NO", None),
    ("holdings_json", "longtext", "NO", None),
    ("history_start_date", "date", "NO", None),
    ("history_end_date", "date", "NO", None),
    ("history_fact_count", "int", "NO", None),
    ("history_opening_equity", "decimal(24,8)", "NO", None),
    ("history_opening_date", "date", "YES", None),
    ("history_tip_fact_id", "char(64)", "NO", None),
    ("history_tip_fact_hash", "char(64)", "NO", None),
    ("history_fact_set_hash", "char(64)", "NO", None),
    ("new_fact_count", "int", "NO", None),
    ("new_fact_set_hash", "char(64)", "NO", None),
    ("new_fact_first_id", "char(64)", "NO", None),
    ("new_fact_tip_id", "char(64)", "NO", None),
    ("evidence_watermark", "datetime(6)", "NO", None),
    ("input_set_hash", "char(64)", "NO", None),
    ("previous_checkpoint_id", "char(64)", "YES", None),
    ("previous_checkpoint_hash", "char(64)", "YES", None),
    ("previous_chain_hash", "char(64)", "YES", None),
    ("state_json", "longtext", "NO", None),
    ("checkpoint_hash", "char(64)", "NO", None),
    ("chain_payload_json", "longtext", "NO", None),
    ("chain_hash", "char(64)", "NO", None),
    ("anchor_run_uid", "char(32)", "NO", None),
    ("canonical_result_hash", "char(64)", "NO", None),
    ("anchor_audit_id", "char(32)", "NO", None),
    ("anchor_audit_hash", "char(64)", "NO", None),
    ("automatic_real_order_submission", "tinyint(1)", "NO", "0"),
    ("real_order_authority", "tinyint(1)", "NO", "0"),
    ("created_at", "datetime(6)", "NO", "current_timestamp(6)"),
)
_CHECKPOINT_INDEXES = {
    "PRIMARY": (0, ("checkpoint_id",)),
    "uk_strategy_funding_checkpoint_identity": (0, (
        "strategy_key", "strategy_version", "account_id", "trade_date",
        "anchor_run_uid")),
    "idx_strategy_funding_checkpoint_hash": (1, ("checkpoint_hash",)),
    "idx_strategy_funding_checkpoint_chain": (1, ("chain_hash",)),
    "idx_strategy_funding_checkpoint_audit": (1, ("anchor_audit_id",)),
    "idx_strategy_funding_checkpoint_lookup": (1, (
        "strategy_key", "strategy_version", "account_id", "trade_date")),
    "idx_strategy_funding_checkpoint_previous": (1, ("previous_checkpoint_id",)),
    "idx_strategy_funding_checkpoint_history_tip": (1, ("history_tip_fact_id",)),
    "idx_strategy_funding_checkpoint_new_first": (1, ("new_fact_first_id",)),
    "idx_strategy_funding_checkpoint_new_tip": (1, ("new_fact_tip_id",)),
    "idx_strategy_funding_checkpoint_account": (1, ("account_id",)),
    "idx_strategy_funding_checkpoint_run": (1, ("anchor_run_uid",)),
}
_CHECKPOINT_FOREIGN_KEYS = {
    "fk_strategy_funding_checkpoint_version": (
        ("strategy_key", "strategy_version"), "st_strategy_version",
        ("strategy_key", "version"), "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_previous": (
        ("previous_checkpoint_id",), FUNDING_CHECKPOINT_TABLE_NAME,
        ("checkpoint_id",), "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_history_tip": (
        ("history_tip_fact_id",), FUNDING_DAILY_FACT_TABLE_NAME,
        ("fact_id",), "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_new_first": (
        ("new_fact_first_id",), FUNDING_DAILY_FACT_TABLE_NAME,
        ("fact_id",), "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_new_tip": (
        ("new_fact_tip_id",), FUNDING_DAILY_FACT_TABLE_NAME,
        ("fact_id",), "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_run": (
        ("anchor_run_uid",), "st_strategy_governance_run", ("run_uid",),
        "RESTRICT", "RESTRICT"),
    "fk_strategy_funding_checkpoint_audit": (
        ("anchor_audit_id",), "st_strategy_governance_audit", ("audit_id",),
        "RESTRICT", "RESTRICT"),
}
_CHECKPOINT_CHECKS = {
    "ck_strategy_funding_checkpoint_mode":
        "replay_mode IN ('FULL_BOOTSTRAP','BOUNDED_INCREMENTAL')",
    "ck_strategy_funding_checkpoint_sessions":
        "(replay_mode = 'FULL_BOOTSTRAP' AND replay_session_count >= 1) OR "
        "(replay_mode = 'BOUNDED_INCREMENTAL' "
        "AND replay_session_count BETWEEN 1 AND 370)",
    "ck_strategy_funding_checkpoint_horizon":
        "max_holding_days BETWEEN 1 AND 250",
    "ck_strategy_funding_checkpoint_cash":
        "opening_cash_cny >= 0 AND closing_cash_cny >= 0",
    "ck_strategy_funding_checkpoint_equity":
        "opening_equity_cny > 0 AND closing_equity_cny > 0 "
        "AND cumulative_fee_cny >= 0 AND high_watermark_equity_cny > 0",
    "ck_strategy_funding_checkpoint_history":
        "history_fact_count BETWEEN 1 AND 120 "
        "AND history_start_date <= history_end_date "
        "AND history_end_date = trade_date AND history_opening_equity > 0",
    "ck_strategy_funding_checkpoint_new_facts":
        "new_fact_count BETWEEN 1 AND 370",
    "ck_strategy_funding_checkpoint_json":
        "JSON_VALID(holdings_json) AND JSON_VALID(state_json) "
        "AND JSON_VALID(chain_payload_json)",
    "ck_strategy_funding_checkpoint_state_hash":
        "BINARY checkpoint_hash = BINARY SHA2(state_json, 256)",
    "ck_strategy_funding_checkpoint_chain_hash":
        "BINARY chain_hash = BINARY SHA2(chain_payload_json, 256)",
    "ck_strategy_funding_checkpoint_predecessor":
        "(replay_mode = 'FULL_BOOTSTRAP' "
        "AND previous_checkpoint_id IS NULL "
        "AND previous_checkpoint_hash IS NULL "
        "AND previous_chain_hash IS NULL) OR "
        "(replay_mode = 'BOUNDED_INCREMENTAL' "
        "AND previous_checkpoint_id IS NOT NULL "
        "AND previous_checkpoint_hash IS NOT NULL "
        "AND previous_chain_hash IS NOT NULL)",
    "ck_strategy_funding_checkpoint_no_real_auto":
        "automatic_real_order_submission = 0",
    "ck_strategy_funding_checkpoint_no_real_authority":
        "real_order_authority = 0",
}


def funding_daily_fact_ddl_statement() -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {FUNDING_DAILY_FACT_TABLE_NAME} (
 fact_id CHAR(64) PRIMARY KEY, entity_type VARCHAR(16) NOT NULL,
 entity_key VARCHAR(160) NOT NULL, entity_version VARCHAR(160) NOT NULL,
 entity_version_hash CHAR(64) NOT NULL, execution_binding_hash CHAR(64) NULL,
 account_id VARCHAR(64) NOT NULL, trade_date DATE NOT NULL,
 origin_checkpoint_id CHAR(64) NOT NULL,
 previous_fact_id CHAR(64) NULL, previous_fact_hash CHAR(64) NULL,
 opening_cash_cny DECIMAL(24,6) NOT NULL,
 closing_cash_cny DECIMAL(24,6) NOT NULL,
 opening_equity_cny DECIMAL(24,6) NOT NULL,
 closing_equity_cny DECIMAL(24,6) NOT NULL,
 daily_return_pct DECIMAL(24,12) NOT NULL,
 cumulative_fee_cny DECIMAL(24,6) NOT NULL,
 high_watermark_equity_cny DECIMAL(24,6) NOT NULL,
 stock_exposure_json LONGTEXT NOT NULL,
 closed_evidence_ids_json LONGTEXT NOT NULL,
 fact_json LONGTEXT NOT NULL, fact_hash CHAR(64) NOT NULL,
 anchor_run_uid CHAR(32) NOT NULL, canonical_result_hash CHAR(64) NOT NULL,
 anchor_audit_id CHAR(32) NOT NULL, anchor_audit_hash CHAR(64) NOT NULL,
 automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
 real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY uk_strategy_funding_daily_fact_identity
  (entity_type,entity_key,entity_version,account_id,trade_date,anchor_run_uid),
 KEY idx_strategy_funding_daily_fact_lookup
  (entity_type,entity_key,entity_version,account_id,trade_date),
 KEY idx_strategy_funding_daily_fact_previous (previous_fact_id),
 KEY idx_strategy_funding_daily_fact_origin (origin_checkpoint_id),
 KEY idx_strategy_funding_daily_fact_hash (fact_hash),
 KEY idx_strategy_funding_daily_fact_account (account_id),
 KEY idx_strategy_funding_daily_fact_run (anchor_run_uid),
 KEY idx_strategy_funding_daily_fact_audit (anchor_audit_id),
 CONSTRAINT fk_strategy_funding_daily_fact_previous FOREIGN KEY
  (previous_fact_id) REFERENCES {FUNDING_DAILY_FACT_TABLE_NAME}(fact_id),
 CONSTRAINT fk_strategy_funding_daily_fact_run FOREIGN KEY
  (anchor_run_uid) REFERENCES st_strategy_governance_run(run_uid),
 CONSTRAINT fk_strategy_funding_daily_fact_audit FOREIGN KEY
  (anchor_audit_id) REFERENCES st_strategy_governance_audit(audit_id),
 CONSTRAINT ck_strategy_funding_daily_fact_entity
  CHECK (entity_type='STRATEGY'),
 CONSTRAINT ck_strategy_funding_daily_fact_predecessor CHECK
  ((previous_fact_id IS NULL AND previous_fact_hash IS NULL) OR
   (previous_fact_id IS NOT NULL AND previous_fact_hash IS NOT NULL)),
 CONSTRAINT ck_strategy_funding_daily_fact_money CHECK
  (opening_cash_cny>=0 AND closing_cash_cny>=0 AND opening_equity_cny>0
   AND closing_equity_cny>0 AND cumulative_fee_cny>=0
   AND high_watermark_equity_cny>0),
 CONSTRAINT ck_strategy_funding_daily_fact_json CHECK
  (JSON_VALID(stock_exposure_json) AND JSON_VALID(closed_evidence_ids_json)
   AND JSON_VALID(fact_json)),
 CONSTRAINT ck_strategy_funding_daily_fact_hash
  CHECK (BINARY fact_hash=BINARY SHA2(fact_json,256)),
 CONSTRAINT ck_strategy_funding_daily_fact_no_real_auto
  CHECK (automatic_real_order_submission=0),
 CONSTRAINT ck_strategy_funding_daily_fact_no_real_authority
  CHECK (real_order_authority=0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def funding_checkpoint_ddl_statement() -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {FUNDING_CHECKPOINT_TABLE_NAME} (
 checkpoint_id CHAR(64) PRIMARY KEY, strategy_key VARCHAR(80) NOT NULL,
 strategy_version VARCHAR(160) NOT NULL, strategy_version_hash CHAR(64) NOT NULL,
 execution_binding_hash CHAR(64) NULL, account_id VARCHAR(64) NOT NULL,
 trade_date DATE NOT NULL, replay_mode VARCHAR(24) NOT NULL,
 replay_start_date DATE NOT NULL, replay_session_count INT NOT NULL,
 max_holding_days INT NOT NULL, opening_cash_cny DECIMAL(24,6) NOT NULL,
 closing_cash_cny DECIMAL(24,6) NOT NULL,
 opening_equity_cny DECIMAL(24,6) NOT NULL,
 closing_equity_cny DECIMAL(24,6) NOT NULL,
 cumulative_fee_cny DECIMAL(24,6) NOT NULL,
 high_watermark_equity_cny DECIMAL(24,6) NOT NULL,
 holdings_json LONGTEXT NOT NULL, history_start_date DATE NOT NULL,
 history_end_date DATE NOT NULL, history_fact_count INT NOT NULL,
 history_opening_equity DECIMAL(24,8) NOT NULL, history_opening_date DATE NULL,
 history_tip_fact_id CHAR(64) NOT NULL, history_tip_fact_hash CHAR(64) NOT NULL,
 history_fact_set_hash CHAR(64) NOT NULL, new_fact_count INT NOT NULL,
 new_fact_set_hash CHAR(64) NOT NULL, new_fact_first_id CHAR(64) NOT NULL,
 new_fact_tip_id CHAR(64) NOT NULL, evidence_watermark DATETIME(6) NOT NULL,
 input_set_hash CHAR(64) NOT NULL, previous_checkpoint_id CHAR(64) NULL,
 previous_checkpoint_hash CHAR(64) NULL, previous_chain_hash CHAR(64) NULL,
 state_json LONGTEXT NOT NULL, checkpoint_hash CHAR(64) NOT NULL,
 chain_payload_json LONGTEXT NOT NULL, chain_hash CHAR(64) NOT NULL,
 anchor_run_uid CHAR(32) NOT NULL, canonical_result_hash CHAR(64) NOT NULL,
 anchor_audit_id CHAR(32) NOT NULL, anchor_audit_hash CHAR(64) NOT NULL,
 automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
 real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY uk_strategy_funding_checkpoint_identity
  (strategy_key,strategy_version,account_id,trade_date,anchor_run_uid),
 KEY idx_strategy_funding_checkpoint_hash (checkpoint_hash),
 KEY idx_strategy_funding_checkpoint_chain (chain_hash),
 KEY idx_strategy_funding_checkpoint_audit (anchor_audit_id),
 KEY idx_strategy_funding_checkpoint_lookup
  (strategy_key,strategy_version,account_id,trade_date),
 KEY idx_strategy_funding_checkpoint_previous (previous_checkpoint_id),
 KEY idx_strategy_funding_checkpoint_history_tip (history_tip_fact_id),
 KEY idx_strategy_funding_checkpoint_new_first (new_fact_first_id),
 KEY idx_strategy_funding_checkpoint_new_tip (new_fact_tip_id),
 KEY idx_strategy_funding_checkpoint_account (account_id),
 KEY idx_strategy_funding_checkpoint_run (anchor_run_uid),
 CONSTRAINT fk_strategy_funding_checkpoint_version FOREIGN KEY
  (strategy_key,strategy_version) REFERENCES st_strategy_version(strategy_key,version),
 CONSTRAINT fk_strategy_funding_checkpoint_previous FOREIGN KEY
  (previous_checkpoint_id) REFERENCES {FUNDING_CHECKPOINT_TABLE_NAME}(checkpoint_id),
 CONSTRAINT fk_strategy_funding_checkpoint_history_tip FOREIGN KEY
  (history_tip_fact_id) REFERENCES {FUNDING_DAILY_FACT_TABLE_NAME}(fact_id),
 CONSTRAINT fk_strategy_funding_checkpoint_new_first FOREIGN KEY
  (new_fact_first_id) REFERENCES {FUNDING_DAILY_FACT_TABLE_NAME}(fact_id),
 CONSTRAINT fk_strategy_funding_checkpoint_new_tip FOREIGN KEY
  (new_fact_tip_id) REFERENCES {FUNDING_DAILY_FACT_TABLE_NAME}(fact_id),
 CONSTRAINT fk_strategy_funding_checkpoint_run FOREIGN KEY
  (anchor_run_uid) REFERENCES st_strategy_governance_run(run_uid),
 CONSTRAINT fk_strategy_funding_checkpoint_audit FOREIGN KEY
  (anchor_audit_id) REFERENCES st_strategy_governance_audit(audit_id),
 CONSTRAINT ck_strategy_funding_checkpoint_mode
  CHECK (replay_mode IN ('FULL_BOOTSTRAP','BOUNDED_INCREMENTAL')),
 CONSTRAINT ck_strategy_funding_checkpoint_sessions CHECK
  ((replay_mode='FULL_BOOTSTRAP' AND replay_session_count>=1) OR
   (replay_mode='BOUNDED_INCREMENTAL' AND replay_session_count BETWEEN 1 AND 370)),
 CONSTRAINT ck_strategy_funding_checkpoint_horizon
  CHECK (max_holding_days BETWEEN 1 AND 250),
 CONSTRAINT ck_strategy_funding_checkpoint_cash
  CHECK (opening_cash_cny>=0 AND closing_cash_cny>=0),
 CONSTRAINT ck_strategy_funding_checkpoint_equity CHECK
  (opening_equity_cny>0 AND closing_equity_cny>0 AND cumulative_fee_cny>=0
   AND high_watermark_equity_cny>0),
 CONSTRAINT ck_strategy_funding_checkpoint_history CHECK
  (history_fact_count BETWEEN 1 AND 120 AND history_start_date<=history_end_date
   AND history_end_date=trade_date AND history_opening_equity>0),
 CONSTRAINT ck_strategy_funding_checkpoint_new_facts
  CHECK (new_fact_count BETWEEN 1 AND 370),
 CONSTRAINT ck_strategy_funding_checkpoint_json CHECK
  (JSON_VALID(holdings_json) AND JSON_VALID(state_json)
   AND JSON_VALID(chain_payload_json)),
 CONSTRAINT ck_strategy_funding_checkpoint_state_hash
  CHECK (BINARY checkpoint_hash=BINARY SHA2(state_json,256)),
 CONSTRAINT ck_strategy_funding_checkpoint_chain_hash
  CHECK (BINARY chain_hash=BINARY SHA2(chain_payload_json,256)),
 CONSTRAINT ck_strategy_funding_checkpoint_predecessor CHECK
  ((replay_mode='FULL_BOOTSTRAP' AND previous_checkpoint_id IS NULL
    AND previous_checkpoint_hash IS NULL AND previous_chain_hash IS NULL) OR
   (replay_mode='BOUNDED_INCREMENTAL' AND previous_checkpoint_id IS NOT NULL
    AND previous_checkpoint_hash IS NOT NULL AND previous_chain_hash IS NOT NULL)),
 CONSTRAINT ck_strategy_funding_checkpoint_no_real_auto
  CHECK (automatic_real_order_submission=0),
 CONSTRAINT ck_strategy_funding_checkpoint_no_real_authority
  CHECK (real_order_authority=0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


FUNDING_CHECKPOINT_TRIGGER_STATEMENTS = {
    "trg_strategy_funding_daily_fact_immutable_bu": f"""CREATE TRIGGER
trg_strategy_funding_daily_fact_immutable_bu BEFORE UPDATE ON
{FUNDING_DAILY_FACT_TABLE_NAME} FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT='strategy funding daily fact is append only'; END""",
    "trg_strategy_funding_daily_fact_immutable_bd": f"""CREATE TRIGGER
trg_strategy_funding_daily_fact_immutable_bd BEFORE DELETE ON
{FUNDING_DAILY_FACT_TABLE_NAME} FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT='strategy funding daily fact cannot be deleted'; END""",
    "trg_strategy_funding_checkpoint_immutable_bu": f"""CREATE TRIGGER
trg_strategy_funding_checkpoint_immutable_bu BEFORE UPDATE ON
{FUNDING_CHECKPOINT_TABLE_NAME} FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT='strategy funding checkpoint is append only'; END""",
    "trg_strategy_funding_checkpoint_immutable_bd": f"""CREATE TRIGGER
trg_strategy_funding_checkpoint_immutable_bd BEFORE DELETE ON
{FUNDING_CHECKPOINT_TABLE_NAME} FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT='strategy funding checkpoint cannot be deleted'; END""",
}
FUNDING_CHECKPOINT_TRIGGER_CONTRACTS = {
    "trg_strategy_funding_daily_fact_immutable_bu": (
        "BEFORE", "UPDATE", FUNDING_DAILY_FACT_TABLE_NAME,
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
        "'strategy funding daily fact is append only'; END"),
    "trg_strategy_funding_daily_fact_immutable_bd": (
        "BEFORE", "DELETE", FUNDING_DAILY_FACT_TABLE_NAME,
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
        "'strategy funding daily fact cannot be deleted'; END"),
    "trg_strategy_funding_checkpoint_immutable_bu": (
        "BEFORE", "UPDATE", FUNDING_CHECKPOINT_TABLE_NAME,
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
        "'strategy funding checkpoint is append only'; END"),
    "trg_strategy_funding_checkpoint_immutable_bd": (
        "BEFORE", "DELETE", FUNDING_CHECKPOINT_TABLE_NAME,
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
        "'strategy funding checkpoint cannot be deleted'; END"),
}

_TABLE_CONTRACTS = {
    FUNDING_DAILY_FACT_TABLE_NAME: {"columns": _DAILY_FACT_COLUMNS,
        "indexes": _DAILY_FACT_INDEXES, "foreign_keys": _DAILY_FACT_FOREIGN_KEYS,
        "checks": _DAILY_FACT_CHECKS},
    FUNDING_CHECKPOINT_TABLE_NAME: {"columns": _CHECKPOINT_COLUMNS,
        "indexes": _CHECKPOINT_INDEXES, "foreign_keys": _CHECKPOINT_FOREIGN_KEYS,
        "checks": _CHECKPOINT_CHECKS},
}


def _assert_frozen_contract_identifiers_unique() -> None:
    for table_name, contract in _TABLE_CONTRACTS.items():
        column_names = [str(row[0]) for row in contract["columns"]]
        if len(column_names) != len(set(column_names)):
            raise RuntimeError(f"duplicate frozen funding column: {table_name}")
        for category in ("indexes", "foreign_keys", "checks"):
            names = list(contract[category])
            if len(names) != len(set(names)):
                raise RuntimeError(
                    f"duplicate frozen funding {category}: {table_name}"
                )


_assert_frozen_contract_identifiers_unique()


def _schema_jsonable(value: Any) -> Any:
    """Convert only frozen Python contract containers to JSON containers."""

    if isinstance(value, tuple):
        return [_schema_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_schema_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _schema_jsonable(item) for key, item in value.items()}
    return value


def _schema_contract_payload() -> dict[str, Any]:
    return {"schema": "probiga.strategy-funding-checkpoint-schema.v2",
            "engine": _ENGINE, "collation": _COLLATION,
            "tables": _schema_jsonable(_TABLE_CONTRACTS),
            "triggers": _schema_jsonable(FUNDING_CHECKPOINT_TRIGGER_CONTRACTS),
            "ddl_sha256": canonical_hash({
                FUNDING_DAILY_FACT_TABLE_NAME: re.sub(
                    r"\s+", " ", funding_daily_fact_ddl_statement().strip()),
                FUNDING_CHECKPOINT_TABLE_NAME: re.sub(
                    r"\s+", " ", funding_checkpoint_ddl_statement().strip())}),
            "budgets": {
                "checkpoint_target_average_bytes": FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
                "checkpoint_total_target_bytes": FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
                "checkpoint_total_hard_bytes": FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
                "batch_max_rows": FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
                "batch_max_bytes": FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
                "manifest_max_bytes": FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
                "audit_max_bytes": FUNDING_CHECKPOINT_AUDIT_MAX_BYTES}}


FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH = canonical_hash(_schema_contract_payload())
FUNDING_CHECKPOINT_MIGRATION_HASH = FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH


def _rows(connection, sql: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(sql), {}).mappings().all()
    ]


def _normalize_default(value: Any, _column_type: str) -> Any:
    return None if value is None else str(value).strip().casefold().replace("()", "")


def _fully_wrapped(raw: str) -> bool:
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
            if char == "(": depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(raw) - 1: return False
                if depth < 0: return False
        index += 1
    return depth == 0 and not quoted


def _top_level_boolean_parts(raw: str, keyword: str) -> list[str]:
    parts, start, depth, quoted, between_pending = [], 0, 0, False, False
    index, lowered = 0, raw.casefold()
    while index < len(raw):
        char = raw[index]
        if char == "'":
            if quoted and index + 1 < len(raw) and raw[index + 1] == "'":
                index += 2; continue
            quoted = not quoted; index += 1; continue
        if quoted: index += 1; continue
        if char == "(": depth += 1; index += 1; continue
        if char == ")": depth -= 1; index += 1; continue
        if depth == 0 and (char.isalpha() or char == "_"):
            end = index + 1
            while end < len(raw) and (raw[end].isalnum() or raw[end] == "_"):
                end += 1
            word = lowered[index:end]
            if word == "between": between_pending = True
            elif word == "and" and between_pending: between_pending = False
            elif word == keyword:
                parts.append(raw[start:index].strip()); start = end
            index = end; continue
        index += 1
    if not parts: return [raw.strip()]
    parts.append(raw[start:].strip())
    return parts


def _normalize_check(value: Any) -> str:
    clause = str(value or "").strip().replace("`", "")
    clause = re.sub(
        r"_utf8mb4\\'([^\\']*)\\'",
        lambda match: "'" + match.group(1) + "'",
        clause,
        flags=re.IGNORECASE,
    )
    clause = re.sub(
        r"_utf8mb4'([^']*)'",
        lambda match: "'" + match.group(1) + "'",
        clause,
        flags=re.IGNORECASE,
    )
    clause = re.sub(r"\s+", " ", clause.casefold()).strip()
    def expression(raw: str) -> str:
        raw = raw.strip()
        while _fully_wrapped(raw): raw = raw[1:-1].strip()
        parts = _top_level_boolean_parts(raw, "or")
        if len(parts) > 1: return "or(" + ",".join(map(expression, parts)) + ")"
        parts = _top_level_boolean_parts(raw, "and")
        if len(parts) > 1: return "and(" + ",".join(map(expression, parts)) + ")"
        while _fully_wrapped(raw): raw = raw[1:-1].strip()
        return "atom(" + re.sub(r"\s+", "", raw) + ")"
    return expression(clause)


def _normalize_trigger_body(value: Any) -> str:
    pieces = re.split(r"('(?:''|[^'])*')", str(value or ""))
    for index in range(0, len(pieces), 2):
        outside = pieces[index].replace("`", "")
        outside = re.sub(r"\bSQLSTATE\s+VALUE\b", "SQLSTATE", outside,
                         flags=re.IGNORECASE)
        outside = re.sub(r"\s+", " ", outside).casefold()
        outside = re.sub(r"\s*=\s*", "=", outside)
        outside = re.sub(r"\s*;\s*", ";", outside)
        pieces[index] = outside
    return "".join(pieces).strip().rstrip(";").strip()


def _validate_table(connection, table_name: str) -> dict[str, int]:
    contract = _TABLE_CONTRACTS[table_name]
    table_rows = _rows(connection,
        "SELECT ENGINE AS engine,TABLE_COLLATION AS table_collation "
        "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME='{table_name}'")
    if len(table_rows) != 1: raise RuntimeError(f"资金事实表未迁移：{table_name}")
    if (str(table_rows[0].get("engine") or "").casefold() != _ENGINE.casefold()
            or str(table_rows[0].get("table_collation") or "").casefold()
            != _COLLATION):
        raise RuntimeError(f"资金事实表引擎或字符集漂移：{table_name}")
    columns = _rows(connection,
        "SELECT COLUMN_NAME AS column_name,ORDINAL_POSITION AS ordinal_position,"
        "COLUMN_TYPE AS column_type,IS_NULLABLE AS is_nullable,"
        "COLUMN_DEFAULT AS column_default,EXTRA AS extra,"
        "CHARACTER_SET_NAME AS character_set_name,COLLATION_NAME AS collation_name "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME='{table_name}' ORDER BY ORDINAL_POSITION")
    expected_columns = contract["columns"]
    if len(columns) != len(expected_columns):
        raise RuntimeError(f"资金事实字段数量漂移：{table_name}")
    for ordinal, (row, expected) in enumerate(zip(columns, expected_columns), 1):
        name, column_type, nullable, default = expected
        character = column_type.split("(", 1)[0] in {"char","varchar","text","longtext"}
        extra = re.sub(r"\bdefault_generated\b", "",
                       str(row.get("extra") or "").casefold()).strip()
        if (int(row.get("ordinal_position") or 0) != ordinal
            or str(row.get("column_name") or "") != name
            or str(row.get("column_type") or "").casefold() != column_type
            or str(row.get("is_nullable") or "").upper() != nullable
            or _normalize_default(row.get("column_default"), column_type)
               != _normalize_default(default, column_type) or extra
            or (character and (str(row.get("character_set_name") or "").casefold()
                != "utf8mb4" or str(row.get("collation_name") or "").casefold()
                != _COLLATION))
            or (not character and (row.get("character_set_name") is not None
                or row.get("collation_name") is not None))):
            raise RuntimeError(f"资金事实字段契约漂移：{table_name}.{name}")
    index_rows = _rows(connection,
        "SELECT INDEX_NAME AS index_name,NON_UNIQUE AS non_unique,"
        "SEQ_IN_INDEX AS seq_in_index,COLUMN_NAME AS column_name,"
        "SUB_PART AS sub_part,INDEX_TYPE AS index_type FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        f"AND TABLE_NAME='{table_name}' ORDER BY BINARY INDEX_NAME,SEQ_IN_INDEX")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in index_rows: grouped.setdefault(str(row.get("index_name") or ""), []).append(row)
    indexes = {}
    for name, rows in grouped.items():
        if any(int(r.get("seq_in_index") or 0) != i + 1 or r.get("sub_part") is not None
               or str(r.get("index_type") or "").upper() != "BTREE"
               for i, r in enumerate(rows)):
            raise RuntimeError(f"资金事实索引结构无效：{table_name}.{name}")
        indexes[name] = (int(rows[0].get("non_unique") or 0),
                         tuple(str(r.get("column_name") or "") for r in rows))
    if indexes != contract["indexes"]:
        raise RuntimeError(f"资金事实索引契约漂移：{table_name}")
    foreign_rows = _rows(connection,
        "SELECT tc.CONSTRAINT_NAME AS constraint_name,kcu.ORDINAL_POSITION AS ordinal_position,"
        "kcu.COLUMN_NAME AS column_name,kcu.REFERENCED_TABLE_NAME AS referenced_table_name,"
        "kcu.REFERENCED_COLUMN_NAME AS referenced_column_name,rc.UPDATE_RULE AS update_rule,"
        "rc.DELETE_RULE AS delete_rule FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.KEY_COLUMN_USAGE kcu ON kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND kcu.TABLE_NAME=tc.TABLE_NAME AND kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON rc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND rc.TABLE_NAME=tc.TABLE_NAME AND rc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() AND tc.CONSTRAINT_TYPE='FOREIGN KEY' "
        f"AND tc.TABLE_NAME='{table_name}' ORDER BY BINARY tc.CONSTRAINT_NAME,kcu.ORDINAL_POSITION")
    foreign: dict[str, list[dict[str, Any]]] = {}
    for row in foreign_rows: foreign.setdefault(str(row.get("constraint_name") or ""), []).append(row)
    if set(foreign) != set(contract["foreign_keys"]):
        raise RuntimeError(f"资金事实外键清单漂移：{table_name}")
    for name, expected in contract["foreign_keys"].items():
        rows = foreign[name]
        observed = (tuple(str(r.get("column_name") or "") for r in rows),
                    str(rows[0].get("referenced_table_name") or ""),
                    tuple(str(r.get("referenced_column_name") or "") for r in rows),
                    str(rows[0].get("update_rule") or "").upper(),
                    str(rows[0].get("delete_rule") or "").upper())
        if observed != expected:
            raise RuntimeError(f"资金事实外键契约漂移：{table_name}.{name}")
    check_rows = _rows(connection,
        "SELECT tc.CONSTRAINT_NAME AS constraint_name,cc.CHECK_CLAUSE AS check_clause "
        "FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() AND tc.CONSTRAINT_TYPE='CHECK' "
        f"AND tc.TABLE_NAME='{table_name}' ORDER BY BINARY tc.CONSTRAINT_NAME")
    checks = {str(r.get("constraint_name") or ""):_normalize_check(r.get("check_clause"))
              for r in check_rows}
    expected_checks = {n:_normalize_check(c) for n,c in contract["checks"].items()}
    if checks != expected_checks:
        raise RuntimeError(f"资金事实CHECK契约漂移：{table_name}")
    return {"column_count":len(expected_columns),"index_count":len(indexes),
            "foreign_key_count":len(foreign),"check_count":len(checks)}


def validate_strategy_funding_checkpoint_schema(connection) -> dict[str, Any]:
    tables = {name:_validate_table(connection, name) for name in
              (FUNDING_DAILY_FACT_TABLE_NAME,FUNDING_CHECKPOINT_TABLE_NAME)}
    trigger_rows = _rows(connection,
        "SELECT TRIGGER_NAME AS trigger_name,EVENT_MANIPULATION AS event,"
        "ACTION_TIMING AS timing,EVENT_OBJECT_TABLE AS table_name,"
        "ACTION_ORIENTATION AS orientation,ACTION_STATEMENT AS body,DEFINER AS definer "
        "FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() "
        f"AND EVENT_OBJECT_TABLE IN ('{FUNDING_DAILY_FACT_TABLE_NAME}',"
        f"'{FUNDING_CHECKPOINT_TABLE_NAME}') ORDER BY BINARY TRIGGER_NAME")
    triggers = {str(r.get("trigger_name") or ""):r for r in trigger_rows}
    if set(triggers) != set(FUNDING_CHECKPOINT_TRIGGER_CONTRACTS):
        raise RuntimeError("资金事实不可变触发器清单漂移")
    hashes = {}
    for name, expected in FUNDING_CHECKPOINT_TRIGGER_CONTRACTS.items():
        timing,event,table_name,body = expected; row = triggers[name]
        expected_body = _normalize_trigger_body(body)
        if (str(row.get("timing") or "").upper()!=timing
            or str(row.get("event") or "").upper()!=event
            or str(row.get("table_name") or "")!=table_name
            or str(row.get("orientation") or "").upper()!="ROW"
            or _normalize_trigger_body(row.get("body"))!=expected_body
            or not str(row.get("definer") or "").strip()):
            raise RuntimeError(f"资金事实不可变触发器漂移：{name}")
        hashes[name]=canonical_hash({"timing":timing,"event":event,"table":table_name,
                                     "orientation":"ROW","body":expected_body,
                                     "definer_present":True})
    return {"table_count":2,"tables":tables,"trigger_count":len(triggers),
            "trigger_contract_hash":canonical_hash(hashes),
            "contract_hash":FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
            "daily_path_base_authoritative_sessions":120,
            "daily_path_max_incremental_replay_sessions":370,
            "maximum_holding_buffer_sessions":250,
            "bootstrap_mode":"EXPLICIT_FULL_HISTORY_ONCE_PER_VERSION_ACCOUNT",
            "bootstrap_is_bounded":False,
            "rolling_history_storage":"ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN",
            "checkpoint_target_average_bytes":FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
            "checkpoint_total_target_bytes":FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
            "checkpoint_total_hard_bytes":FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
            "batch_max_rows":FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
            "batch_max_bytes":FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
            "manifest_max_bytes":FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
            "audit_max_bytes":FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
            "automatic_real_order_submission":False,"real_order_authority":False}


def ensure_strategy_funding_checkpoint_schema(connection, *,
        trigger_ddl_executor: Callable[[str], None] | None = None) -> dict[str, Any]:
    connection.execute(text(funding_daily_fact_ddl_statement()))
    connection.execute(text(funding_checkpoint_ddl_statement()))
    existing = {str(r.get("trigger_name") or "") for r in _rows(connection,
        "SELECT TRIGGER_NAME AS trigger_name FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA=DATABASE() "
        f"AND EVENT_OBJECT_TABLE IN ('{FUNDING_DAILY_FACT_TABLE_NAME}',"
        f"'{FUNDING_CHECKPOINT_TABLE_NAME}')")}
    if existing-set(FUNDING_CHECKPOINT_TRIGGER_STATEMENTS):
        raise RuntimeError("资金事实表存在未授权触发器")
    for name, statement in FUNDING_CHECKPOINT_TRIGGER_STATEMENTS.items():
        if name in existing: continue
        if trigger_ddl_executor is not None: trigger_ddl_executor(statement)
        else: connection.execute(text(statement))
    return validate_strategy_funding_checkpoint_schema(connection)


__all__ = [name for name in globals() if name.startswith("FUNDING_")] + [
    "canonical_hash","canonical_json","checkpoint_chain_hash",
    "checkpoint_chain_payload","checkpoint_identity","checkpoint_state_hash",
    "ensure_strategy_funding_checkpoint_schema","funding_checkpoint_ddl_statement",
    "funding_daily_fact_ddl_statement","funding_daily_fact_hash",
    "funding_daily_fact_identity","ordered_funding_fact_set_hash",
    "validate_strategy_funding_checkpoint_schema"]
