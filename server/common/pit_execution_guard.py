"""Fail-closed daily-bar execution guards for historical research.

Daily OHLCV can prove that some orders were impossible, but it cannot prove an
intraday queue position.  The helpers below therefore accept only finite,
liquid bars, reject definitively locked one-price limits and cap simulated
notional by an explicit daily-turnover participation rate.  They never create
orders or grant real-order authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, time
from typing import Any, Mapping
from zoneinfo import ZoneInfo


_LOCKED_LIMIT_MIN_MOVE = 0.045
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPEN_RECEIPT_SCHEMA = "probiga.immutable-open-execution-receipt.v1"
_OPEN_RECEIPT_SOURCES = frozenset({"QMT_CALL_AUCTION", "QMT_FIRST_1MIN"})
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _timestamp(value: Any) -> datetime:
    raw = str(value or "").strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def daily_bar_execution_disposition(
    row: Mapping[str, Any] | None,
    *,
    side: str,
) -> dict[str, Any]:
    """Classify a daily-bar open execution without inventing missing facts."""

    normalized_side = str(side or "").upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError("execution side must be BUY or SELL")
    if not isinstance(row, Mapping):
        return {
            "status": "DATA_BLOCKED",
            "reason": "MISSING_DAILY_BAR",
            "executable": False,
        }
    values = {
        key: _number(row.get(key))
        for key in ("open", "high", "low", "close", "pre_close", "volume", "amount")
    }
    if any(values[key] is None or values[key] <= 0 for key in ("open", "high", "low", "close", "pre_close")):
        return {
            "status": "DATA_BLOCKED",
            "reason": "INVALID_DAILY_PRICE",
            "executable": False,
        }
    if values["volume"] is None or values["amount"] is None:
        return {
            "status": "DATA_BLOCKED",
            "reason": "MISSING_DAILY_LIQUIDITY",
            "executable": False,
        }
    if values["volume"] <= 0 or values["amount"] <= 0:
        return {
            "status": "KNOWN_UNFILLED",
            "reason": "SUSPENDED_OR_ZERO_TURNOVER",
            "executable": False,
        }
    tolerance = max(1e-8, values["pre_close"] * 1e-6)
    one_price = abs(values["high"] - values["low"]) <= tolerance
    move = values["open"] / values["pre_close"] - 1.0
    locked_against_order = one_price and (
        (normalized_side == "BUY" and move >= _LOCKED_LIMIT_MIN_MOVE)
        or (normalized_side == "SELL" and move <= -_LOCKED_LIMIT_MIN_MOVE)
    )
    if locked_against_order:
        return {
            "status": "KNOWN_UNFILLED",
            "reason": (
                "LOCKED_LIMIT_UP" if normalized_side == "BUY"
                else "LOCKED_LIMIT_DOWN"
            ),
            "executable": False,
        }
    return {
        "status": "EXECUTABLE",
        "reason": "DAILY_BAR_EXECUTABLE",
        "executable": True,
        "open_price": values["open"],
        "daily_amount_cny": values["amount"],
    }


def build_open_execution_receipt(
    *,
    stock_code: str,
    trade_date: str,
    execution_price: float,
    observed_at: str | datetime,
    source_provider: str,
    source_payload_hash: str,
) -> dict[str, Any]:
    """Build a content-addressed receipt for an observed opening execution price.

    The helper is intentionally storage-agnostic.  ``immutable_storage`` is a
    contract assertion that must be backed by an append-only source before the
    receipt is supplied to research code; the validator never upgrades a
    mutable minute table into authoritative evidence by itself.
    """

    price = _number(execution_price)
    code = str(stock_code or "").strip().split(".")[0].zfill(6)
    day = str(trade_date or "")[:10]
    observed = _timestamp(observed_at)
    provider = str(source_provider or "").strip().upper()
    source_hash = str(source_payload_hash or "").strip().lower()
    if len(code) != 6 or not code.isdigit():
        raise ValueError("opening receipt stock code is invalid")
    if observed.date().isoformat() != day:
        raise ValueError("opening receipt observation date differs")
    if price is None or price <= 0:
        raise ValueError("opening receipt price is invalid")
    if provider not in _OPEN_RECEIPT_SOURCES:
        raise ValueError("opening receipt provider is not authoritative")
    if provider == "QMT_CALL_AUCTION" and not (
        time(9, 25) <= observed.time() <= time(9, 29, 59)
    ):
        raise ValueError("call-auction receipt is outside the final auction window")
    if provider == "QMT_FIRST_1MIN" and not (
        time(9, 30) <= observed.time() <= time(9, 31, 59)
    ):
        raise ValueError("first-minute receipt is outside the opening bar window")
    if not _SHA256_RE.fullmatch(source_hash):
        raise ValueError("opening receipt source payload hash is invalid")
    payload = {
        "schema": _OPEN_RECEIPT_SCHEMA,
        "stock_code": code,
        "trade_date": day,
        "execution_price": price,
        "observed_at": observed.isoformat(sep="T", timespec="seconds"),
        "source_provider": provider,
        "source_payload_hash": source_hash,
        "immutable_storage": True,
    }
    return {**payload, "receipt_hash": _sha256(payload)}


def validate_open_execution_receipt(
    receipt: Mapping[str, Any] | None,
    *,
    stock_code: str,
    trade_date: str,
    daily_open_price: float,
) -> dict[str, Any]:
    """Validate a call-auction/first-minute receipt without trusting its flags."""

    if not isinstance(receipt, Mapping):
        return {
            "valid": False,
            "reason": "MISSING_IMMUTABLE_OPEN_RECEIPT",
        }
    try:
        normalized = build_open_execution_receipt(
            stock_code=receipt.get("stock_code"),
            trade_date=receipt.get("trade_date"),
            execution_price=receipt.get("execution_price"),
            observed_at=receipt.get("observed_at"),
            source_provider=receipt.get("source_provider"),
            source_payload_hash=receipt.get("source_payload_hash"),
        )
        expected_code = str(stock_code or "").strip().split(".")[0].zfill(6)
        expected_day = str(trade_date or "")[:10]
        supplied_hash = str(receipt.get("receipt_hash") or "").strip().lower()
        if receipt.get("schema") != _OPEN_RECEIPT_SCHEMA:
            raise ValueError("opening receipt schema differs")
        if receipt.get("immutable_storage") is not True:
            raise ValueError("opening receipt storage is not append-only")
        if normalized["stock_code"] != expected_code:
            raise ValueError("opening receipt stock code differs")
        if normalized["trade_date"] != expected_day:
            raise ValueError("opening receipt trade date differs")
        if supplied_hash != normalized["receipt_hash"]:
            raise ValueError("opening receipt content hash differs")
        daily_open = _number(daily_open_price)
        if daily_open is None or daily_open <= 0:
            raise ValueError("daily opening reference is invalid")
        # A first-minute receipt can legitimately differ from the auction open,
        # but a very large mismatch is evidence of a wrong instrument/session.
        if abs(normalized["execution_price"] / daily_open - 1.0) > 0.12:
            raise ValueError("opening receipt price is inconsistent with daily open")
        return {
            "valid": True,
            "reason": "IMMUTABLE_OPEN_RECEIPT_VERIFIED",
            "execution_price": normalized["execution_price"],
            "receipt_hash": normalized["receipt_hash"],
            "source_provider": normalized["source_provider"],
            "observed_at": normalized["observed_at"],
        }
    except (ArithmeticError, TypeError, ValueError) as exc:
        return {
            "valid": False,
            "reason": f"INVALID_IMMUTABLE_OPEN_RECEIPT:{type(exc).__name__}",
        }


def participation_capped_quantity(
    *,
    desired_notional_cny: float,
    price: float,
    daily_amount_cny: float,
    maximum_participation_rate: float,
    board_lot: int = 100,
) -> dict[str, Any]:
    """Return the largest board-lot quantity inside a turnover capacity cap."""

    values = (
        _number(desired_notional_cny),
        _number(price),
        _number(daily_amount_cny),
        _number(maximum_participation_rate),
    )
    if (
        any(value is None or value <= 0 for value in values)
        or isinstance(board_lot, bool)
        or int(board_lot) < 1
    ):
        return {
            "valid": False,
            "reason": "INVALID_CAPACITY_INPUT",
            "quantity": 0,
        }
    desired, execution_price, daily_amount, participation_limit = values
    if participation_limit > 1:
        return {
            "valid": False,
            "reason": "INVALID_CAPACITY_INPUT",
            "quantity": 0,
        }
    maximum_notional = daily_amount * participation_limit
    accepted_notional = min(desired, maximum_notional)
    quantity = math.floor(accepted_notional / execution_price / int(board_lot)) * int(board_lot)
    actual_notional = quantity * execution_price
    if quantity <= 0:
        return {
            "valid": True,
            "reason": "CAPACITY_BELOW_ONE_BOARD_LOT",
            "quantity": 0,
            "maximum_notional_cny": maximum_notional,
            "actual_notional_cny": 0.0,
            "participation_rate": 0.0,
        }
    return {
        "valid": True,
        "reason": (
            "CAPACITY_CAPPED"
            if maximum_notional + 1e-9 < desired
            else "BOARD_LOT_ROUNDED"
            if actual_notional + 1e-9 < desired
            else "DESIRED_NOTIONAL_ACCEPTED"
        ),
        "quantity": quantity,
        "maximum_notional_cny": maximum_notional,
        "actual_notional_cny": actual_notional,
        "participation_rate": actual_notional / daily_amount,
    }


def nonlinear_impact_rate(
    *,
    participation_rate: float,
    maximum_participation_rate: float,
    base_slippage_rate: float,
) -> float:
    """Conservative square-root impact surcharge above configured slippage."""

    participation = _number(participation_rate)
    maximum = _number(maximum_participation_rate)
    slippage = _number(base_slippage_rate)
    if (
        participation is None or participation < 0
        or maximum is None or not 0 < maximum <= 1
        or slippage is None or slippage < 0
        or participation > maximum + 1e-12
    ):
        raise ValueError("impact model inputs are invalid")
    if participation == 0 or slippage == 0:
        return 0.0
    return slippage * math.sqrt(participation / maximum)


__all__ = [
    "build_open_execution_receipt",
    "daily_bar_execution_disposition",
    "nonlinear_impact_rate",
    "participation_capped_quantity",
    "validate_open_execution_receipt",
]
