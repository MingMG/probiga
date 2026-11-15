"""Weekly market facts derived from exact-date, quality-gated daily reviews."""

from datetime import date, timedelta
from hashlib import sha256
import json
import math


INDEX_NAMES = {
    "000001": "上证指数",
    "000688": "科创50",
    "000300": "沪深300",
    "399006": "创业板指",
    "000852": "中证1000",
    "932000": "中证2000",
}
TMT = ("电子", "通信", "计算机")


def _number(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _object(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def _value(row, key):
    value = row.get(key)
    return value.isoformat() if hasattr(value, "isoformat") else value


def _mean_if_complete(values):
    return sum(values) / len(values) if values and all(value is not None for value in values) else None


def _return_pct(close, reference):
    return round(100 * (close / reference - 1), 4) if close is not None and reference else None


def _day_facts(day, digest, margin):
    status = digest.get("publish_status") or "missing"
    market = _object(digest.get("market_structure_json")) if status == "ready" else {}
    facts = _object(market.get("decision_facts"))
    breadth = _object(facts.get("breadth"))
    industries = _object(digest.get("industry_rotation_json")) if status == "ready" else {}
    amount_quality = _object(market.get("amount_quality"))
    amount_ok = amount_quality.get("status") == "OBSERVED"
    industry_rows = industries.get("industries") or []
    sectors = {
        row["industry_name"]: {
            "sample_count": row.get("sample_count"),
            "median_return_pct": _number(row.get("median_return_pct")),
            "up_share": _number(row.get("up_coverage")),
            "amount_share": _number(row.get("amount_share")),
        }
        for row in industry_rows
    }
    tmt = [_number(sectors.get(name, {}).get("amount_share")) for name in TMT]
    return {
        "date": day,
        "review_status": status,
        "data_cutoff_at": _value(digest, "data_cutoff_at"),
        "generated_at": _value(digest, "generated_at"),
        "amount_cny": _number(market.get("total_amount")) if amount_ok else None,
        "amount_evidence_date": day if amount_ok else None,
        "amount_evidence_cutoff_at": _value(digest, "data_cutoff_at") if amount_ok else None,
        "median_return_pct": _number(market.get("return_median_pct")),
        "breadth": {key: breadth.get(key) for key in ("up_count", "down_count", "flat_count", "sample_count")}
        if breadth.get("evidence_type") == "OBSERVED" else {},
        "industry_status": industries.get("status") or "unavailable",
        "industry_reason": industries.get("reason"),
        "industries": sectors,
        "tmt_amount_share": round(sum(tmt), 6)
        if amount_ok and industries.get("amount_status") == "OBSERVED" and all(x is not None for x in tmt)
        else None,
        "margin_balance_cny": _number(margin.get("rzye")),
        "margin_ingested_at": _value(margin, "etl_sync_at"),
    }


def build_weekly_strategy(*, week_start, calendar, digests, indices, margins, source_errors):
    week_end = week_start + timedelta(days=6)
    week_start_text = week_start.isoformat()
    week_end_text = week_end.isoformat()
    sessions = sorted({_value(row, "trade_date") for row in calendar if row["trade_status"] == 1})
    week = [day for day in sessions if week_start_text <= day <= week_end_text]
    previous = [day for day in sessions if day < week_start_text]
    base = previous[-1] if previous else None
    by_day = {_value(row, "review_date"): row for row in digests}
    margin_by_day = {_value(row, "trade_date"): row for row in margins}
    facts_by_day = {
        day: _day_facts(day, by_day.get(day, {}), margin_by_day.get(day, {}))
        for day in sessions
    }
    # A later digest can contain a separately validated turnover record for
    # its previous session. Keep its original cutoff in this retrospective view.
    for day, later in zip(sessions, sessions[1:]):
        facts = facts_by_day[day]
        if facts["amount_cny"] is not None:
            continue
        digest = by_day.get(later, {})
        if digest.get("publish_status") != "ready":
            continue
        market = _object(digest.get("market_structure_json"))
        quality = _object(market.get("previous_amount_quality"))
        value = _number(market.get("previous_total_amount"))
        cutoff = _value(digest, "data_cutoff_at")
        if quality.get("status") == "OBSERVED" and quality.get("trade_date") == day and value is not None and cutoff:
            facts.update(amount_cny=value, amount_evidence_date=later, amount_evidence_cutoff_at=cutoff)

    days = [facts_by_day[day] for day in week]
    previous_week = [day for day in previous if day >= (week_start - timedelta(days=7)).isoformat()]
    previous_amounts = [facts_by_day[day]["amount_cny"] for day in previous_week]
    mean = _mean_if_complete([day["amount_cny"] for day in days])
    previous_mean = _mean_if_complete(previous_amounts)

    closes = {}
    duplicates = set()
    for row in indices:
        key = row["index_code"], _value(row, "trade_date")
        close = _number(row.get("close"))
        if key in closes:
            duplicates.add(key)
        closes[key] = close if close is not None and close > 0 else None
    for key in duplicates:
        closes[key] = None

    paths = []
    for code, name in INDEX_NAMES.items():
        reference = closes.get((code, base))
        points = [
            {
                "date": day,
                "close": closes.get((code, day)),
                "normalized": round(100 * closes[code, day] / reference, 4)
                if reference and closes.get((code, day)) is not None else None,
            }
            for day in ([base] if base else []) + week
        ]
        paths.append({
            "code": code, "name": name, "points": points,
            "week_return_pct": _return_pct(closes.get((code, week[-1])), reference) if week else None,
            "missing_dates": [point["date"] for point in points if point["close"] is None],
        })

    last_amount = facts_by_day[base]["amount_cny"] if base else None
    previous_trading_day = dict(zip(sessions[1:], sessions))
    for row in days:
        day = row["date"]
        row["amount_change_pct"] = _return_pct(row["amount_cny"], last_amount)
        last_amount = row["amount_cny"]
        prior = previous_trading_day.get(day)
        row["index_returns_pct"] = {
            code: _return_pct(closes.get((code, day)), closes.get((code, prior)))
            for code in INDEX_NAMES
        }
        large_cap = closes.get(("000300", day))
        small_cap = closes.get(("932000", day))
        row["small_large_ratio"] = round(small_cap / large_cap, 6) if small_cap and large_cap else None

    turns = [
        {"date": row["date"], "index": "上证指数", "index_return_pct": row["index_returns_pct"]["000001"],
         "amount_change_pct": row["amount_change_pct"]}
        for row in days
        if row["amount_change_pct"] is not None and row["amount_change_pct"] > 0
        and row["index_returns_pct"]["000001"] is not None and row["index_returns_pct"]["000001"] < 0
    ]
    data = {
        "method": "probiga-weekly-market-v1",
        "week_start": week_start_text,
        "week_end": week_end_text,
        "baseline_date": base,
        "mode": "historical_reconstruction",
        "sources": {
            "calendar": "si_trade_calendar",
            "review": "st_quant_review_digest / 已校验未复权日度快照",
            "turnover": "日度复盘全样本成交额（元）",
            "indices": "sm_index_kline / 日线收盘",
            "margin": "st_securities_margin / 融资余额",
            "industry": "日度复盘申万一级 / 收益中位数",
        },
        "coverage": {
            "sessions": len(week),
            "ready_reviews": sum(row["review_status"] == "ready" for row in days),
            "turnover_days": sum(row["amount_cny"] is not None for row in days),
            "industry_days": sum(row["industry_status"] == "available" for row in days),
            "margin_days": sum(row["margin_balance_cny"] is not None for row in days),
        },
        "source_errors": source_errors,
        "turnover": {"daily_mean_cny": mean, "previous_daily_mean_cny": previous_mean,
                     "change_pct": _return_pct(mean, previous_mean)},
        "higher_turnover_down_days": turns,
        "index_paths": paths,
        "days": days,
    }
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    data["snapshot_sha256"] = sha256(encoded.encode("utf-8")).hexdigest()
    return data
