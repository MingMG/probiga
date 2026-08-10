"""Deterministic configuration for ProBigA's internal paper account.

These values are simulation assumptions, not Guojin broker confirmations.
They may activate only the isolated V2 paper ledger; real order submission
remains disabled by schema, account state, and runtime checks.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .bootstrap import ACCOUNT_ID
from .config import canonical_json_hash
from .instrument_registry import RULE_VERSION as QMT_RULE_VERSION
from .policy import load_portfolio_policy


PAPER_FEE_PROFILE_VERSION = "paper_fee_cn_v2.0.0"
PAPER_INSTRUMENT_RULE_VERSION = "paper_instrument_qmt_v2.0.0"
PAPER_INSTRUMENT_RULE_PREFIX = "paper_instrument_qmt_"
CONFIRMED_FEE_PAPER_RULE_VERSION = "paper_instrument_qmt_v2.1.0"

PAPER_FEE_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "security_type": "A_SHARE",
        "buy_commission_rate": "0.00025",
        "sell_commission_rate": "0.00025",
        "minimum_commission": "5.00",
        "stamp_tax_sell_rate": "0.0005",
        "transfer_fee_buy_rate": "0.00001",
        "transfer_fee_sell_rate": "0.00001",
        "other_fees": {},
    },
    {
        "security_type": "ETF",
        "buy_commission_rate": "0.00025",
        "sell_commission_rate": "0.00025",
        "minimum_commission": "5.00",
        "stamp_tax_sell_rate": "0",
        "transfer_fee_buy_rate": "0",
        "transfer_fee_sell_rate": "0",
        "other_fees": {},
    },
)


def is_internal_paper_configuration(account: Mapping[str, Any] | None) -> bool:
    account = account or {}
    return (
        str(account.get("instrument_rule_version") or "").startswith(
            PAPER_INSTRUMENT_RULE_PREFIX
        )
        and not bool(account.get("real_trading_enabled"))
    )


def bind_confirmed_fee_to_internal_paper(
    engine: Engine,
    *,
    fee_profile_version: str,
    effective_from: date,
    rule_version: str = CONFIRMED_FEE_PAPER_RULE_VERSION,
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    """Bind confirmed broker fees without claiming real-account permissions.

    The copied instrument permissions remain explicitly scoped to ProBigA's
    isolated paper account.  This clears only B-001; B-002 and the real-order
    kill switch are intentionally unchanged.
    """
    fee_profile_version = str(fee_profile_version or "").strip()
    rule_version = str(rule_version or "").strip()
    if not fee_profile_version or not rule_version:
        raise ValueError("fee_profile_version and rule_version are required")
    if not rule_version.startswith(PAPER_INSTRUMENT_RULE_PREFIX):
        raise ValueError("confirmed-fee paper rule must use the paper prefix")

    policy = load_portfolio_policy()
    now = datetime.now()
    with engine.begin() as connection:
        confirmed_types = {
            str(value)
            for value in connection.execute(
                text(
                    """
                    SELECT DISTINCT security_type
                    FROM st_fee_profile_v2
                    WHERE fee_profile_version = :fee_version
                      AND confirmation_status = 'CONFIRMED'
                      AND effective_from <= :effective_from
                      AND (
                          effective_to IS NULL
                          OR effective_to >= :effective_from
                      )
                    """
                ),
                {
                    "fee_version": fee_profile_version,
                    "effective_from": effective_from,
                },
            ).scalars()
        }
        missing_types = sorted({"A_SHARE", "ETF"} - confirmed_types)
        if missing_types:
            raise ValueError(
                "confirmed fee profile is incomplete: "
                + ",".join(missing_types)
            )

        account = connection.execute(
            text(
                """
                SELECT instrument_rule_version, real_trading_enabled
                FROM st_trade_account_v2
                WHERE account_id = :account_id FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise ValueError("V2 account not found")
        if bool(account["real_trading_enabled"]):
            raise ValueError("confirmed fees cannot reconfigure a real account")

        source_rule_version = str(
            account["instrument_rule_version"] or QMT_RULE_VERSION
        )
        if source_rule_version == rule_version:
            rule_rows = 0
        else:
            result = connection.execute(
                text(
                    """
                    INSERT INTO st_instrument_rule_v2
                    (stock_code, rule_version, effective_from, effective_to,
                     security_type, exchange_code, can_buy,
                     first_buy_minimum, buy_lot_size, sell_lot_size,
                     settlement_days, tick_size, limit_ratio,
                     special_treatment, suspended, permission_required,
                     permission_confirmed, fee_profile_version,
                     source_snapshot_hash, created_at)
                    SELECT
                        source.stock_code, :rule_version, :effective_from,
                        NULL, source.security_type, source.exchange_code,
                        source.can_buy, source.first_buy_minimum,
                        source.buy_lot_size, source.sell_lot_size,
                        source.settlement_days, source.tick_size,
                        source.limit_ratio, source.special_treatment,
                        source.suspended, source.permission_required,
                        source.permission_confirmed, :fee_version,
                        SHA2(CONCAT(
                            source.source_snapshot_hash, '|',
                            :rule_version, '|', :fee_version
                        ), 256),
                        :now
                    FROM st_instrument_rule_v2 source
                    WHERE source.rule_version = :source_rule_version
                      AND source.effective_from = (
                          SELECT MAX(latest.effective_from)
                          FROM st_instrument_rule_v2 latest
                          WHERE BINARY latest.stock_code =
                                BINARY source.stock_code
                            AND latest.rule_version =
                                :source_rule_version
                            AND latest.effective_from <= :effective_from
                      )
                    ON DUPLICATE KEY UPDATE
                        stock_code = VALUES(stock_code)
                    """
                ),
                {
                    "rule_version": rule_version,
                    "source_rule_version": source_rule_version,
                    "fee_version": fee_profile_version,
                    "effective_from": effective_from,
                    "now": now,
                },
            )
            rule_rows = max(0, int(result.rowcount or 0))

        bound_rules = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM st_instrument_rule_v2
                    WHERE rule_version = :rule_version
                      AND fee_profile_version = :fee_version
                    """
                ),
                {
                    "rule_version": rule_version,
                    "fee_version": fee_profile_version,
                },
            ).scalar()
            or 0
        )
        if bound_rules == 0:
            raise ValueError("no paper instrument rules were bound")

        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET fee_profile_version = :fee_version,
                    instrument_rule_version = :rule_version,
                    policy_version = :policy_version,
                    policy_hash = :policy_hash,
                    real_trading_enabled = 0,
                    updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {
                "fee_version": fee_profile_version,
                "rule_version": rule_version,
                "policy_version": policy.version,
                "policy_hash": policy.config_hash,
                "now": now,
                "account_id": account_id,
            },
        )
        event_payload = {
            "account_id": account_id,
            "scope": "PROBIGA_INTERNAL_PAPER_WITH_CONFIRMED_FEES",
            "fee_profile_version": fee_profile_version,
            "instrument_rule_version": rule_version,
            "effective_from": effective_from.isoformat(),
            "cleared_block": "B-001_ACTUAL_BROKER_FEES",
            "remaining_real_blocks": [
                "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS",
                "B-003_RELIABLE_LEVEL1_BID_ASK",
            ],
            "real_trading_enabled": False,
        }
        event_hash = canonical_json_hash(event_payload)
        connection.execute(
            text(
                """
                INSERT IGNORE INTO st_trade_event_v2
                (event_id, trace_id, account_id, event_type,
                 entity_type, entity_id, event_payload_json,
                 payload_hash, occurred_at, created_at)
                VALUES
                (:event_id, :trace_id, :account_id,
                 'CONFIRMED_FEE_BOUND_TO_PAPER', 'ACCOUNT', :account_id,
                 :payload, :payload_hash, :now, :now)
                """
            ),
            {
                "event_id": event_hash[:32],
                "trace_id": event_hash[32:],
                "account_id": account_id,
                "payload": json.dumps(
                    event_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "payload_hash": event_hash,
                "now": now,
            },
        )
    return {
        "status": "ok",
        "account_id": account_id,
        "effective_from": effective_from.isoformat(),
        "fee_profile_version": fee_profile_version,
        "instrument_rule_version": rule_version,
        "instrument_rule_rows_written": rule_rows,
        "instrument_rule_rows_bound": bound_rules,
        "cleared_real_trading_block": "B-001_ACTUAL_BROKER_FEES",
        "real_trading_enabled": False,
    }


def install_internal_paper_configuration(
    engine: Engine,
    *,
    effective_from: date,
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    """Install immutable paper assumptions and bind them to the V2 account."""
    policy = load_portfolio_policy()
    now = datetime.now()
    profile_rows = 0
    rule_rows = 0
    with engine.begin() as connection:
        for profile in PAPER_FEE_PROFILES:
            evidence = {
                "scope": "PROBIGA_INTERNAL_PAPER_ONLY",
                "not_broker_confirmation": True,
                "profile_version": PAPER_FEE_PROFILE_VERSION,
                "effective_from": effective_from.isoformat(),
                "profile": profile,
            }
            result = connection.execute(
                text(
                    """
                    INSERT INTO st_fee_profile_v2
                    (fee_profile_version, effective_from, effective_to,
                     security_type, buy_commission_rate,
                     sell_commission_rate, minimum_commission,
                     stamp_tax_sell_rate, transfer_fee_buy_rate,
                     transfer_fee_sell_rate, other_fee_json,
                     evidence_hash, confirmation_status, created_at)
                    VALUES
                    (:version, :effective_from, NULL, :security_type,
                     :buy_commission_rate, :sell_commission_rate,
                     :minimum_commission, :stamp_tax_sell_rate,
                     :transfer_fee_buy_rate, :transfer_fee_sell_rate,
                     :other_fees, :evidence_hash, 'PAPER_ASSUMPTION', :now)
                    ON DUPLICATE KEY UPDATE
                        fee_profile_version = VALUES(fee_profile_version)
                    """
                ),
                {
                    "version": PAPER_FEE_PROFILE_VERSION,
                    "effective_from": effective_from,
                    "security_type": profile["security_type"],
                    "buy_commission_rate": profile["buy_commission_rate"],
                    "sell_commission_rate": profile["sell_commission_rate"],
                    "minimum_commission": profile["minimum_commission"],
                    "stamp_tax_sell_rate": profile["stamp_tax_sell_rate"],
                    "transfer_fee_buy_rate": profile["transfer_fee_buy_rate"],
                    "transfer_fee_sell_rate": profile[
                        "transfer_fee_sell_rate"
                    ],
                    "other_fees": json.dumps(
                        profile["other_fees"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "evidence_hash": canonical_json_hash(evidence),
                    "now": now,
                },
            )
            profile_rows += max(0, int(result.rowcount or 0))

        result = connection.execute(
            text(
                """
                INSERT INTO st_instrument_rule_v2
                (stock_code, rule_version, effective_from, effective_to,
                 security_type, exchange_code, can_buy,
                 first_buy_minimum, buy_lot_size, sell_lot_size,
                 settlement_days, tick_size, limit_ratio,
                 special_treatment, suspended, permission_required,
                 permission_confirmed, fee_profile_version,
                 source_snapshot_hash, created_at)
                SELECT
                    source.stock_code, :paper_rule_version, :effective_from,
                    NULL, source.security_type, source.exchange_code,
                    source.can_buy, source.first_buy_minimum,
                    source.buy_lot_size, source.sell_lot_size,
                    source.settlement_days, source.tick_size,
                    source.limit_ratio, source.special_treatment,
                    source.suspended, source.permission_required,
                    1, :paper_fee_version,
                    SHA2(CONCAT(
                        source.source_snapshot_hash, '|',
                        :paper_rule_version, '|',
                        :paper_fee_version
                    ), 256),
                    :now
                FROM st_instrument_rule_v2 source
                WHERE source.rule_version = :qmt_rule_version
                  AND source.effective_from = (
                      SELECT MAX(latest.effective_from)
                      FROM st_instrument_rule_v2 latest
                      WHERE BINARY latest.stock_code =
                            BINARY source.stock_code
                        AND latest.rule_version = :qmt_rule_version
                        AND latest.effective_from <= :effective_from
                  )
                ON DUPLICATE KEY UPDATE
                    stock_code = VALUES(stock_code)
                """
            ),
            {
                "paper_rule_version": PAPER_INSTRUMENT_RULE_VERSION,
                "paper_fee_version": PAPER_FEE_PROFILE_VERSION,
                "qmt_rule_version": QMT_RULE_VERSION,
                "effective_from": effective_from,
                "now": now,
            },
        )
        rule_rows = max(0, int(result.rowcount or 0))
        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET fee_profile_version = :fee_version,
                    instrument_rule_version = :rule_version,
                    policy_version = :policy_version,
                    policy_hash = :policy_hash,
                    real_trading_enabled = 0,
                    updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {
                "fee_version": PAPER_FEE_PROFILE_VERSION,
                "rule_version": PAPER_INSTRUMENT_RULE_VERSION,
                "policy_version": policy.version,
                "policy_hash": policy.config_hash,
                "now": now,
                "account_id": account_id,
            },
        )
        event_payload = {
            "account_id": account_id,
            "scope": "PROBIGA_INTERNAL_PAPER_ONLY",
            "fee_profile_version": PAPER_FEE_PROFILE_VERSION,
            "instrument_rule_version": PAPER_INSTRUMENT_RULE_VERSION,
            "effective_from": effective_from.isoformat(),
            "real_trading_enabled": False,
        }
        event_hash = canonical_json_hash(event_payload)
        connection.execute(
            text(
                """
                INSERT IGNORE INTO st_trade_event_v2
                (event_id, trace_id, account_id, event_type,
                 entity_type, entity_id, event_payload_json,
                 payload_hash, occurred_at, created_at)
                VALUES
                (:event_id, :trace_id, :account_id,
                 'PAPER_CONFIGURATION_APPLIED', 'ACCOUNT', :account_id,
                 :payload, :payload_hash, :now, :now)
                """
            ),
            {
                "event_id": event_hash[:32],
                "trace_id": event_hash[32:],
                "account_id": account_id,
                "payload": json.dumps(
                    event_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "payload_hash": event_hash,
                "now": now,
            },
        )
    return {
        "status": "ok",
        "account_id": account_id,
        "effective_from": effective_from.isoformat(),
        "fee_profile_version": PAPER_FEE_PROFILE_VERSION,
        "instrument_rule_version": PAPER_INSTRUMENT_RULE_VERSION,
        "fee_profile_rows_written": profile_rows,
        "instrument_rule_rows_written": rule_rows,
        "real_trading_enabled": False,
    }
