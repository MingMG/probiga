"""Validate raw turnover units against same-session unadjusted prices.

Amounts are CNY and volumes are shares.  This boundary never guesses a unit
multiplier or repairs upstream data.  An incomplete or inconsistent sample
cannot establish an aggregate, a turnover weight, or a capacity ranking.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import math
from typing import Any, Iterable, Mapping

import pandas as pd


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _day(value: Any) -> str | None:
    try:
        return pd.Timestamp(value).date().isoformat() if pd.notna(value) else None
    except (TypeError, ValueError):
        return None


def amount_proof_matches(sample: Mapping[str, Any], proof: Mapping[str, Any], day: str | None) -> bool:
    """Bind a corroboration receipt to this exact raw dated range exception."""
    return (proof.get("trade_date") == day and proof.get("stock_code") == str(sample.get("stock_code"))
            and proof.get("kind") in {"QMT_DAT_LOT_RESOLUTION", "BSE_AUCTION_SCOPE"}
            and all(_number(proof.get(key)) == _number(sample.get(key)) for key in ("amount", "volume", "low", "high")))


def assess_amount_integrity(
    bars: pd.DataFrame | Iterable[Mapping[str, Any]] | None,
    *,
    trade_date: str | date | None = None,
    max_samples: int = 10,
) -> dict[str, Any]:
    """Return a bounded, JSON-safe receipt; OBSERVED requires every row to pass.

    A zero-volume/zero-amount suspension contributes zero without requiring a
    VWAP.  Positive trades require adjust_type=0 and amount/volume inside low
    and high. Adjusted prices cannot validate raw turnover. A review producer
    may attach corroboration for an exact exceptional DAT row. Other rows get
    only floating-point arithmetic tolerance (1e-8 of price).
    The receipt proves this input sample only, not completeness of a market.
    """
    if not isinstance(max_samples, int) or max_samples < 0:
        raise ValueError("max_samples must be a nonnegative integer")
    frame = bars.copy(deep=True) if isinstance(bars, pd.DataFrame) else pd.DataFrame(list(bars or []))
    expected_date = _day(trade_date) if trade_date is not None else None
    dates = {_day(value) for value in frame.get("trade_date", pd.Series(dtype=object))}
    actual_date = next(iter(dates)) if len(dates) == 1 and None not in dates else None
    receipt: dict[str, Any] = {
        "status": "UNKNOWN", "trade_date": expected_date or actual_date,
        "sample_count": len(frame), "checked_count": 0, "inconsistent_count": 0,
        "missing_count": 0, "suspended_count": 0, "samples": [],
        "units": {"amount": "CNY", "volume": "shares", "price": "unadjusted_CNY"},
        "definition": "positive-volume amount/volume must lie in same-day unadjusted [low, high]",
        "reason": None,
    }
    reasons: set[str] = set()
    codes = frame.get("stock_code", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    duplicate_codes = set(codes[codes.duplicated(keep=False)])
    for row in frame.to_dict(orient="records"):
        amount, volume, low, high, adjustment = (_number(row.get(key)) for key in
                                               ("amount", "volume", "low", "high", "adjust_type"))
        implied = amount / volume if amount is not None and volume is not None and volume > 0 else None
        reason = None
        missing = False
        code = str(row.get("stock_code") or "").strip()
        if not code:
            reason, missing = "missing_stock_code", True
        elif code in duplicate_codes:
            reason = "duplicate_stock_code"
        elif _day(row.get("trade_date")) is None or (expected_date and _day(row.get("trade_date")) != expected_date):
            reason, missing = "wrong_or_missing_trade_date", True
        elif amount is None or volume is None:
            reason, missing = "missing_amount_or_volume", True
        elif amount < 0 or volume < 0:
            reason = "negative_amount_or_volume"
        elif amount == 0 and volume == 0:
            receipt["suspended_count"] += 1
        elif amount == 0 or volume == 0:
            reason = "amount_volume_zero_mismatch"
        elif adjustment is None or adjustment != 0:
            reason, missing = "unadjusted_prices_required", True
        elif low is None or high is None or low <= 0 or high < low:
            reason, missing = "invalid_or_missing_price_range", True
        elif implied is None or not math.isfinite(implied) or not (low - max(high, 1.0) * 1e-8 <= implied <= high + max(high, 1.0) * 1e-8):
            proof = row.get("_review_amount_proof")
            if isinstance(proof, Mapping) and amount_proof_matches(row, proof, expected_date or actual_date):
                receipt.setdefault("corroborated_rows", []).append(dict(proof))
            else:
                reason = "implied_price_outside_daily_range"
        if reason:
            reasons.add(reason)
            receipt["missing_count" if missing else "inconsistent_count"] += 1
            if len(receipt["samples"]) < max_samples:
                receipt["samples"].append({
                    "stock_code": str(row.get("stock_code", "")), "reason": reason,
                    "amount": amount, "volume": volume, "implied_price": implied if implied is None or math.isfinite(implied) else None,
                    "low": low, "high": high, "adjust_type": adjustment,
                })
        else:
            receipt["checked_count"] += 1
    if not len(frame):
        reasons.add("empty_sample")
    if actual_date is None or (expected_date and actual_date != expected_date):
        reasons.add("nonunique_or_missing_trade_date")
    if not reasons:
        receipt["status"] = "OBSERVED"
    else:
        receipt["reason"] = ";".join(sorted(reasons))
    return receipt


def apply_amount_integrity(
    digest: Mapping[str, Any], *, target_quality: Mapping[str, Any],
    previous_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Copy a fresh or frozen digest and remove claims unproved by receipts.

    Receipt dates must match the digest dates, and receipt sample counts must
    cover the digest's price-valid sample.  This function does not mutate the
    source, regenerate text, replace bad values, or claim a sample is a market.
    Affected compact prose is cleared so the caller must render from the copy.
    """
    result = deepcopy(dict(digest))
    market = result.setdefault("market_structure_json", {})
    previous_date = market.get("decision_facts", {}).get("previous_trade_date")
    if not previous_date:
        previous_date = result.get("quality_json", {}).get("source_dates", {}).get("previous_bars")

    def checked(value: Mapping[str, Any] | None, expected: Any, count: Any) -> dict[str, Any]:
        receipt = deepcopy(dict(value or {}))
        if (receipt.get("status") != "OBSERVED"
                or receipt.get("checked_count") != receipt.get("sample_count")
                or receipt.get("missing_count", 0) or receipt.get("inconsistent_count", 0)):
            receipt["status"] = "UNKNOWN"
        if not expected or _day(receipt.get("trade_date")) != _day(expected):
            receipt.update(status="UNKNOWN", reason=";".join(filter(None, (receipt.get("reason"), "receipt_date_mismatch"))))
        if count is not None and receipt.get("sample_count") != count:
            receipt.update(status="UNKNOWN", reason=";".join(filter(None, (receipt.get("reason"), "receipt_sample_count_mismatch"))))
        return receipt

    current = checked(target_quality, result.get("review_date"), market.get("sample_count"))
    previous = checked(previous_quality, previous_date, market.get("previous_sample_count"))
    current_ok, previous_ok = current["status"] == "OBSERVED", previous["status"] == "OBSERVED"
    market["amount_integrity"] = {"current": current, "previous": previous}
    market["amount_quality"] = current
    market["previous_amount_quality"] = previous
    if not current_ok:
        for key in ("total_amount", "amount_weighted_return_pct"):
            market[key] = None
    if not previous_ok:
        market["previous_total_amount"] = None
    if not (current_ok and previous_ok):
        market["amount_change_pct"] = None

    industry = result.get("industry_rotation_json", {})
    if not current_ok and industry.get("status") == "available":
        industry["amount_status"] = "UNKNOWN"
        industry["amount_concentration_top5"] = None
        for key in ("industries", "leaders", "laggards"):
            for item in industry.get(key, []):
                for field in ("amount", "amount_share", "amount_rank"):
                    if field in item:
                        item[field] = None
        # Old frozen ranks used amount as the tie breaker.  Only rankings with
        # the explicit price-only contract survive failed amount validation.
        if industry.get("rank_basis") != "median_return_pct_then_industry_name":
            for key in ("industries", "leaders", "laggards"):
                for item in industry.get(key, []):
                    for field in ("rank", "previous_rank", "rank_change"):
                        if field in item:
                            item[field] = None
            industry.update(previous_comparison_available=False, rotation_state="数据不足",
                            rank_correlation=None, top3_overlap_count=None, top3_overlap=[])
    if (current_ok and not previous_ok and industry.get("status") == "available"
            and industry.get("rank_basis") != "median_return_pct_then_industry_name"):
        # A frozen digest's previous ranks also used amount to break price ties.
        # Current turnover passing does not validate those previous-day ranks.
        for key in ("industries", "leaders", "laggards"):
            for item in industry.get(key, []):
                for field in ("previous_rank", "rank_change"):
                    if field in item:
                        item[field] = None
        industry.update(previous_comparison_available=False, rotation_state="数据不足",
                        rank_correlation=None, top3_overlap_count=None, top3_overlap=[])
    if not current_ok:
        roles = market.get("decision_facts", {}).get("stock_roles", {})
        candidates = roles.get("candidates", [])
        roles["candidates"] = [item for item in candidates if "成交额" not in str(item.get("identity", ""))]
        for item in roles.get("candidates", []):
            evidence = item.get("evidence", {})
            for field in ("amount", "industry_amount_share", "amount_rank"):
                if field in evidence:
                    evidence[field] = None
            evidence["amount_status"] = "UNKNOWN"
        if not roles.get("candidates") and candidates:
            roles.update(evidence_type="UNKNOWN", reason="成交额未通过量价一致性核验，不能建立容量身份")
    quality = result.setdefault("quality_json", {})
    quality["amount_integrity"] = {"current": current, "previous": previous}
    if not (current_ok and previous_ok):
        warning = "成交额未通过同日未复权量价一致性核验；不展示不可靠总额、量能比较、金额权重和容量排名"
        warnings = quality.setdefault("warnings", [])
        if warning not in warnings:
            warnings.append(warning)
        if quality.get("status") != "blocked":
            quality["status"] = "warn"
        result["compact_review"] = ""
    return result
