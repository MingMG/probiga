"""Pure research fees and conservative stock rules shared by all replay readers."""
from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import Any
from server.common.pit_execution_guard import nonlinear_impact_rate
from server.trading_v2.config import canonical_json_hash
RULE_VERSION = "instrument_rules_qmt_v2.0.0"

def _execution_fee(
    value: float,
    *,
    account: dict[str, Any],
    sell: bool,
) -> float:
    result = max(
        float(account["minimum_commission_cny"]),
        value * float(account["commission_rate"]),
    )
    result += value * float(account["transfer_fee_rate"])
    if sell:
        result += value * float(account["sell_stamp_duty_rate"])
    result += value * float(account["default_slippage_rate"])
    return result



def _execution_fee_with_impact(
    value: float,
    *,
    account: dict[str, Any],
    sell: bool,
    participation_rate: float,
    maximum_participation_rate: float,
) -> tuple[float, float]:
    """Return configured fees plus a nonlinear turnover-impact surcharge."""

    impact_rate = nonlinear_impact_rate(
        participation_rate=participation_rate,
        maximum_participation_rate=maximum_participation_rate,
        base_slippage_rate=float(account["default_slippage_rate"]),
    )
    impact_cny = value * impact_rate
    return (
        _execution_fee(value, account=account, sell=sell) + impact_cny,
        impact_cny,
    )



def _stock_rule(row: dict[str, Any], effective_from: date) -> dict[str, Any]:
    code = str(row["stock_code"]).zfill(6)
    short_name = str(row.get("short_name") or "")
    exchange = str(row.get("exchange") or "")
    special = "ST" in short_name.upper()
    if code.startswith("68"):
        permission = "A_SHARE_STAR"
        first_buy_minimum, buy_lot_size = 200, 1
        limit_ratio = Decimal("0.20")
    elif code.startswith("30"):
        permission = "A_SHARE_GEM"
        first_buy_minimum, buy_lot_size = 100, 100
        limit_ratio = Decimal("0.20")
    elif code.startswith("92") or exchange.upper() in {"BJ", "BSE"}:
        permission = "A_SHARE_BSE"
        first_buy_minimum, buy_lot_size = 100, 1
        limit_ratio = Decimal("0.30")
    else:
        permission = "A_SHARE_MAIN"
        first_buy_minimum, buy_lot_size = 100, 100
        limit_ratio = Decimal("0.10")
    if special:
        # Current ST price-limit details require an effective-dated status
        # source. Until that source is registered, prohibit buys rather than
        # infer one universal ST rule.
        limit_ratio = None
    source_hash = canonical_json_hash(
        {
            "source": "si_all_code",
            "source_row": row,
            "rule_version": RULE_VERSION,
            "effective_from": effective_from,
        }
    )
    return {
        "stock_code": code,
        "rule_version": RULE_VERSION,
        "effective_from": effective_from,
        "security_type": "A_SHARE",
        "exchange_code": exchange or ("SH" if code.startswith("6") else "SZ"),
        "can_buy": not special,
        "first_buy_minimum": first_buy_minimum,
        "buy_lot_size": buy_lot_size,
        "sell_lot_size": 1,
        "settlement_days": 1,
        "tick_size": Decimal("0.01"),
        "limit_ratio": limit_ratio,
        "special_treatment": special,
        "permission_required": permission,
        "fee_profile_version": "UNCONFIRMED",
        "source_snapshot_hash": source_hash,
    }
