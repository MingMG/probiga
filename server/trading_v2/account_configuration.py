"""Evidence-required V2 broker fee and account-permission configuration."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .bootstrap import ACCOUNT_ID
from .config import canonical_json_hash
from .domain import decimal_value
from .paper_configuration import is_internal_paper_configuration


REQUIRED_FEE_FIELDS = (
    "security_type",
    "buy_commission_rate",
    "sell_commission_rate",
    "minimum_commission",
    "stamp_tax_sell_rate",
    "transfer_fee_buy_rate",
    "transfer_fee_sell_rate",
)

OTHER_FEE_FIELDS = frozenset(
    {
        "buy_rate",
        "sell_rate",
        "buy_fixed",
        "sell_fixed",
        "buy_per_share",
        "sell_per_share",
    }
)


def _confirmed_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    if not isinstance(evidence, dict):
        raise ValueError("configuration evidence is required")
    if not str(evidence.get("source") or "").strip():
        raise ValueError("evidence.source is required")
    if not str(evidence.get("confirmed_at") or "").strip():
        raise ValueError("evidence.confirmed_at is required")
    if evidence.get("confirmed_by_user") is not True:
        raise ValueError("evidence.confirmed_by_user must be true")
    return evidence


def apply_fee_configuration(
    engine: Engine,
    payload: dict[str, Any],
    *,
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    evidence = _confirmed_evidence(payload)
    version = str(payload.get("fee_profile_version") or "").strip()
    effective_from = date.fromisoformat(
        str(payload.get("effective_from") or "")
    )
    profiles = payload.get("profiles")
    if not version or not isinstance(profiles, list) or not profiles:
        raise ValueError("fee_profile_version and profiles are required")
    normalized: list[dict[str, Any]] = []
    security_types: set[str] = set()
    for item in profiles:
        if not isinstance(item, dict):
            raise ValueError("each fee profile must be an object")
        missing = [
            field
            for field in REQUIRED_FEE_FIELDS
            if item.get(field) is None
        ]
        if missing:
            raise ValueError(f"fee profile missing fields: {missing}")
        row = {field: item[field] for field in REQUIRED_FEE_FIELDS}
        security_type = str(row["security_type"] or "").strip()
        if not security_type:
            raise ValueError("security_type must not be empty")
        if security_type in security_types:
            raise ValueError(
                f"duplicate fee profile security_type: {security_type}"
            )
        security_types.add(security_type)
        row["security_type"] = security_type
        for field in REQUIRED_FEE_FIELDS[1:]:
            value = decimal_value(row[field])
            if value < 0:
                raise ValueError(f"{field} must be non-negative")
            row[field] = value
        other_fees = item.get("other_fees")
        if not isinstance(other_fees, dict):
            raise ValueError(
                "other_fees must be an explicit object; use {} only when "
                "broker evidence confirms there are no other fees"
            )
        unknown = sorted(set(other_fees) - OTHER_FEE_FIELDS)
        if unknown:
            raise ValueError(f"unsupported other fee fields: {unknown}")
        normalized_other: dict[str, Decimal] = {}
        for field, raw in sorted(other_fees.items()):
            value = decimal_value(raw)
            if value < 0:
                raise ValueError(
                    f"other_fees.{field} must be non-negative"
                )
            normalized_other[field] = value
        row["other_fees"] = normalized_other
        normalized.append(row)
    evidence_hash = canonical_json_hash(
        {
            "version": version,
            "effective_from": effective_from,
            "profiles": normalized,
            "evidence": evidence,
        }
    )
    now = datetime.now()
    rows_written = 0
    with engine.begin() as connection:
        for row in normalized:
            row_evidence_hash = canonical_json_hash(
                {
                    "configuration_evidence_hash": evidence_hash,
                    "security_type": row["security_type"],
                }
            )
            existing = connection.execute(
                text(
                    """
                    SELECT evidence_hash, confirmation_status
                    FROM st_fee_profile_v2
                    WHERE fee_profile_version = :version
                      AND effective_from = :effective_from
                      AND security_type = :security_type
                    """
                ),
                {
                    "version": version,
                    "effective_from": effective_from,
                    "security_type": row["security_type"],
                },
            ).mappings().first()
            if existing:
                if (
                    str(existing["evidence_hash"]) != row_evidence_hash
                    or str(existing["confirmation_status"]) != "CONFIRMED"
                ):
                    raise ValueError(
                        "published fee profile version conflicts with "
                        f"new evidence: {row['security_type']}"
                    )
                continue
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
                     :buy_rate, :sell_rate, :minimum_commission,
                     :stamp_tax, :transfer_buy, :transfer_sell,
                     :other_fees, :evidence_hash, 'CONFIRMED', :created_at)
                    """
                ),
                {
                    "version": version,
                    "effective_from": effective_from,
                    "security_type": row["security_type"],
                    "buy_rate": row["buy_commission_rate"],
                    "sell_rate": row["sell_commission_rate"],
                    "minimum_commission": row["minimum_commission"],
                    "stamp_tax": row["stamp_tax_sell_rate"],
                    "transfer_buy": row["transfer_fee_buy_rate"],
                    "transfer_sell": row["transfer_fee_sell_rate"],
                    "other_fees": json.dumps(
                        row["other_fees"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "evidence_hash": row_evidence_hash,
                    "created_at": now,
                },
            )
            rows_written += max(0, int(result.rowcount or 0))
        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET fee_profile_version = :version, updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {
                "version": version,
                "now": now,
                "account_id": account_id,
            },
        )
    return {
        "status": "ok",
        "account_id": account_id,
        "fee_profile_version": version,
        "profile_count": len(normalized),
        "profile_rows_written": rows_written,
        "evidence_hash": evidence_hash,
    }


def apply_permission_configuration(
    engine: Engine,
    payload: dict[str, Any],
    *,
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    evidence = _confirmed_evidence(payload)
    version = str(payload.get("instrument_rule_version") or "").strip()
    permissions = payload.get("permissions")
    if not version or not isinstance(permissions, dict) or not permissions:
        raise ValueError(
            "instrument_rule_version and permissions are required"
        )
    allowed_types = {
        str(key): bool(value) for key, value in permissions.items()
    }
    evidence_hash = canonical_json_hash(
        {
            "version": version,
            "permissions": allowed_types,
            "evidence": evidence,
        }
    )
    now = datetime.now()
    updated = 0
    with engine.begin() as connection:
        account_fee_version = str(
            connection.execute(
                text(
                    """
                    SELECT fee_profile_version
                    FROM st_trade_account_v2
                    WHERE account_id = :account_id
                    """
                ),
                {"account_id": account_id},
            ).scalar()
            or ""
        )
        for permission_code, confirmed in sorted(allowed_types.items()):
            source_rows = connection.execute(
                text(
                    """
                    SELECT r.*
                    FROM st_instrument_rule_v2 r
                    WHERE r.permission_required = :permission_code
                      AND r.created_at = (
                          SELECT MAX(r2.created_at)
                          FROM st_instrument_rule_v2 r2
                          WHERE BINARY r2.stock_code =
                                BINARY r.stock_code
                            AND r2.effective_from =
                                r.effective_from
                      )
                    ORDER BY r.stock_code, r.effective_from
                    """
                ),
                {"permission_code": permission_code},
            ).mappings().all()
            for source in source_rows:
                existing = connection.execute(
                    text(
                        """
                        SELECT permission_confirmed,
                               fee_profile_version
                        FROM st_instrument_rule_v2
                        WHERE stock_code = :stock_code
                          AND rule_version = :rule_version
                          AND effective_from = :effective_from
                        """
                    ),
                    {
                        "stock_code": source["stock_code"],
                        "rule_version": version,
                        "effective_from": source["effective_from"],
                    },
                ).mappings().first()
                expected_fee_version = (
                    account_fee_version
                    or str(source["fee_profile_version"] or "")
                )
                if existing:
                    if (
                        bool(existing["permission_confirmed"])
                        != bool(confirmed)
                        or str(existing["fee_profile_version"] or "")
                        != expected_fee_version
                    ):
                        raise ValueError(
                            "published instrument rule version conflicts "
                            "with new permission evidence"
                        )
                    continue
                result = connection.execute(
                    text(
                        """
                        INSERT IGNORE INTO st_instrument_rule_v2
                        (stock_code, rule_version, effective_from,
                         effective_to, security_type, exchange_code,
                         can_buy, first_buy_minimum, buy_lot_size,
                         sell_lot_size, settlement_days, tick_size,
                         limit_ratio, special_treatment, suspended,
                         permission_required, permission_confirmed,
                         fee_profile_version, source_snapshot_hash,
                         created_at)
                        VALUES
                        (:stock_code, :rule_version, :effective_from,
                         :effective_to, :security_type, :exchange_code,
                         :can_buy, :first_buy_minimum, :buy_lot_size,
                         :sell_lot_size, :settlement_days, :tick_size,
                         :limit_ratio, :special_treatment, :suspended,
                         :permission_required, :permission_confirmed,
                         :fee_profile_version, :source_snapshot_hash,
                         :created_at)
                        """
                    ),
                    {
                        **dict(source),
                        "rule_version": version,
                        "permission_confirmed": 1 if confirmed else 0,
                        "fee_profile_version": expected_fee_version,
                        "created_at": now,
                    },
                )
                updated += max(0, int(result.rowcount or 0))
        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET instrument_rule_version = :version,
                    updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {
                "version": version,
                "now": now,
                "account_id": account_id,
            },
        )
        event_payload = {
            "account_id": account_id,
            "instrument_rule_version": version,
            "permissions": allowed_types,
            "evidence_hash": evidence_hash,
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
                 'ACCOUNT_PERMISSION_CONFIGURED', 'ACCOUNT', :account_id,
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
        "instrument_rule_version": version,
        "updated_rule_rows": updated,
        "evidence_hash": evidence_hash,
    }


def refresh_account_activation(
    engine: Engine,
    *,
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT fee_profile_version, instrument_rule_version,
                       real_trading_enabled
                FROM st_trade_account_v2
                WHERE account_id = :account_id FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not row:
            raise ValueError("V2 account not found")
        capability = connection.execute(
            text(
                """
                SELECT status FROM st_execution_capability_v2
                WHERE capability_code = 'B-003_RELIABLE_LEVEL1_BID_ASK'
                """
            )
        ).scalar()
        reconciliation = connection.execute(
            text(
                """
                SELECT status FROM st_reconciliation_v2
                WHERE account_id = :account_id
                ORDER BY trade_date DESC, version DESC LIMIT 1
                """
            ),
            {"account_id": account_id},
        ).scalar()
        internal_paper = is_internal_paper_configuration(row)
        blocks: list[str] = []
        if not row["fee_profile_version"]:
            blocks.append("PAPER_FEE_PROFILE_MISSING")
        if not row["instrument_rule_version"]:
            blocks.append("PAPER_INSTRUMENT_RULES_MISSING")
        usable_fee_profiles = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_fee_profile_v2
                    WHERE fee_profile_version = :version
                      AND confirmation_status IN
                          ('CONFIRMED','PAPER_ASSUMPTION')
                    """
                ),
                {"version": str(row["fee_profile_version"] or "")},
            ).scalar()
            or 0
        )
        confirmed_fee_types = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT security_type)
                    FROM st_fee_profile_v2
                    WHERE fee_profile_version = :version
                      AND confirmation_status = 'CONFIRMED'
                      AND security_type IN ('A_SHARE','ETF')
                    """
                ),
                {"version": str(row["fee_profile_version"] or "")},
            ).scalar()
            or 0
        )
        executable_rules = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_instrument_rule_v2
                    WHERE rule_version = :rule_version
                      AND permission_confirmed = 1
                      AND can_buy = 1
                      AND fee_profile_version = :fee_version
                    """
                ),
                {
                    "rule_version": str(
                        row["instrument_rule_version"] or ""
                    ),
                    "fee_version": str(
                        row["fee_profile_version"] or ""
                    ),
                },
            ).scalar()
            or 0
        )
        if row["fee_profile_version"] and usable_fee_profiles == 0:
            blocks.append("FEE_PROFILE_ROWS_MISSING")
        if (
            row["instrument_rule_version"]
            and row["fee_profile_version"]
            and executable_rules == 0
        ):
            blocks.append("FEE_RULE_BINDING_MISSING")
        if reconciliation != "PASS":
            blocks.append("RECONCILIATION_BLOCKED")
        next_status = "ACTIVE" if not blocks else "CONFIG_BLOCKED"
        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET status = :status, real_trading_enabled = 0,
                    updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {
                "status": next_status,
                "now": datetime.now(),
                "account_id": account_id,
            },
        )
    real_trading_blocks: list[str] = []
    if internal_paper:
        if confirmed_fee_types < 2:
            real_trading_blocks.append("B-001_ACTUAL_BROKER_FEES")
        real_trading_blocks.append(
            "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS"
        )
    if capability != "PASS":
        real_trading_blocks.append("B-003_RELIABLE_LEVEL1_BID_ASK")
    return {
        "status": next_status,
        "account_id": account_id,
        "blocks": blocks,
        "paper_blocks": blocks,
        "real_trading_blocks": real_trading_blocks,
        "configuration_scope": (
            (
                "PROBIGA_INTERNAL_PAPER_WITH_CONFIRMED_FEES"
                if confirmed_fee_types >= 2
                else "PROBIGA_INTERNAL_PAPER_ONLY"
            )
            if internal_paper
            else "EXTERNALLY_CONFIRMED"
        ),
        "fee_confirmation_status": (
            "CONFIRMED" if confirmed_fee_types >= 2 else "UNCONFIRMED"
        ),
        "real_trading_enabled": False,
    }
