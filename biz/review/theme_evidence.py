"""Bounded, date-exact theme observations for the research input boundary.

These tables describe attention ranks, quoted index returns and stored tags.
They do not establish PIT membership, fund flows, or a stock's trading role.
"""
from __future__ import annotations

from datetime import date, datetime, time as day_time
import math
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text

from biz.review.amount_integrity import assess_amount_integrity
from server.common.batch_db import routed_read_engine


_TZ = ZoneInfo("Asia/Shanghai")
_DEADLINE_SECONDS = 120
_QUERY_MS = 15000
_SOURCES = {
    1: ("同花顺概念热度榜", "source_attention_rank"),
    2: ("同花顺行业热度榜", "source_attention_rank"),
    3: ("东财一级行业行情榜", "source_quoted_turnover_rank"),
    4: ("东财二级行业行情榜", "source_quoted_turnover_rank"),
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _stamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return parsed.replace(tzinfo=_TZ) if parsed.tzinfo is None else parsed.astimezone(_TZ)
    except (TypeError, ValueError):
        return None


def _available(rows: list[dict], asof: datetime) -> list[dict]:
    # Unknown receipt times cannot prove an as-of observation.
    return [row for row in rows if (stamp := _stamp(row.get("received_at") or row.get("etl_sync_at"))) is not None and stamp <= asof]


def _receipt(row: dict, trade_date: str) -> dict:
    stamp = _stamp(row.get("received_at") or row.get("etl_sync_at"))
    return {
        "received_at": stamp.isoformat() if stamp else None,
        "received_after_session_close": bool(stamp and stamp > datetime.combine(date.fromisoformat(trade_date), day_time(15), _TZ)),
        "availability_note": "按实际入库时间使用；晚补的收盘事实仅作事后补记，不代表盘前或盘中已知",
    }


def _tag_key(value: Any) -> str:
    name = re.sub(r"\s+", "", str(value or ""))
    return re.sub(r"(?:概念|板块)$", "", name).casefold()


def _tag_keys(value: Any) -> set[str]:
    # A source may call an index 共封装光学(CPO) and a stock tag CPO概念.
    # Use explicitly written aliases, never fuzzy or hand-coded memberships.
    name = str(value or "")
    return {_tag_key(name), *(_tag_key(alias) for alias in re.findall(r"[（(]([^()（）]+)[)）]", name))} - {""}


def _tag_names(value: Any) -> set[str]:
    return set().union(*(_tag_keys(item) for item in re.split(r"[;；,，、|]", str(value or "")) if item.strip()))


def _prices(row: dict) -> tuple[dict, bool]:
    prices = {key: _number(row.get(key)) for key in ("open", "close", "pre_close", "high", "low")}
    valid = all(value is not None and value > 0 for value in prices.values())
    valid = bool(valid and prices["low"] <= min(prices["open"], prices["close"]) <= max(prices["open"], prices["close"]) <= prices["high"])
    valid = valid and _number(row.get("adjust_type")) == 0 and row.get("quality_status") in {"VERIFIED", "SOURCE_VERIFIED"}
    return prices, valid


def _sector_rows(rows: list[dict], dates: list[str], target: str) -> list[dict]:
    # Selection follows the source's attention order, never a supposed leader rank.
    current = [row for row in rows if _date(row["snapshot_date"]) == target]
    current.sort(key=lambda row: (int(row["plate_type"]), int(row.get("rank") or 999999), str(row.get("concept_code"))))
    chosen = [row for row in current if int(row["plate_type"]) == 1][:9]
    for plate_type in (2, 3, 4):
        chosen.extend([row for row in current if int(row["plate_type"]) == plate_type][:1])
    if len(chosen) < 12:
        keys = {(row["plate_type"], row["concept_code"]) for row in chosen}
        chosen.extend(row for row in current if (row["plate_type"], row["concept_code"]) not in keys)
    result = []
    for row in chosen[:12]:
        kind = int(row["plate_type"])
        matching = [item for item in rows if item["plate_type"] == row["plate_type"] and item["concept_code"] == row["concept_code"]]
        history = []
        for day in dates:
            matches = [item for item in matching if _date(item["snapshot_date"]) == day]
            # A duplicate business key does not establish a unique closing quote.
            if len(matches) == 1:
                item = matches[0]
                history.append({"trade_date": day, "change_pct": _number(item.get("change_pct")),
                                "rank": item.get("rank"), **_receipt(item, day)})
        complete = len(dates) == 5 and len(history) == 5 and all(item["change_pct"] is not None for item in history)
        cumulative = (math.prod(1 + item["change_pct"] / 100 for item in history) - 1) * 100 if complete else None
        result.append({
            "status": "OBSERVED", "name": row["concept_name"], "concept_name": row["concept_name"],
            "concept_code": row["concept_code"], "trade_date": target, "plate_type": kind,
            "source": _SOURCES[kind][0], "rank_basis": _SOURCES[kind][1], "rank": row.get("rank"),
            "change_pct": _number(row.get("change_pct")), "hot_tag": row.get("hot_tag"),
            "history": history, "history_observed_sessions": len(history),
            "history_expected_sessions": len(dates), "history_complete_5d": complete,
            "cumulative_return_5d_pct": round(cumulative, 6) if cumulative is not None else None,
            "history_note": "榜单缺失日期未知，不补零；仅完整五个交易日复利。不同来源指数口径不混算。",
            "identity_limit": "来源榜单与指数行情，不代表资金净流入、PIT成分覆盖或持续主线已成立",
            **_receipt(row, target),
        })
    return result


def _stock_rows(fused: list[dict], tags: list[dict], sectors: list[dict], bars: list[dict], target: str, dates: list[str]) -> list[dict]:
    by_code = {str(row["stock_code"]): row for row in tags}
    fused_by_code = {str(row["stock_code"]): row for row in fused}
    universe = list(dict.fromkeys([*fused_by_code, *by_code]))
    associations = {}
    for code in universe:
        tag_row, heat = by_code.get(code, {}), fused_by_code.get(code, {})
        tag_keys = _tag_names(tag_row.get("concept_tag"))
        associations[code] = [sector["name"] for sector in sectors
                              if (sector["plate_type"] == 1 and bool(_tag_keys(sector["name"]) & tag_keys))
                              or (sector["plate_type"] == 2 and _tag_key(sector["name"]) == _tag_key(heat.get("industry_name")))]
    # Give observed directions a candidate before filling the attention sample.
    chosen: list[str] = []
    for sector in sectors:
        matches = [code for code in universe if sector["name"] in associations[code]]
        for code in matches[:2]:
            if code not in chosen:
                chosen.append(code)
    chosen = list(dict.fromkeys(chosen + universe))[:16]
    result = []
    for code in chosen:
        heat, tag = fused_by_code.get(code, {}), by_code.get(code, {})
        current = [row for row in bars if str(row["stock_code"]) == code and _date(row["trade_date"]) == target]
        bar = current[0] if len(current) == 1 else {}
        prices, valid = _prices(bar)
        change = (prices["close"] / prices["pre_close"] - 1) * 100 if valid else None
        checks = {name: {"change_pct": _number(source.get("change_pct")),
                         "matches_unadjusted_close": abs(change - float(source["change_pct"])) <= 0.01 if change is not None and _number(source.get("change_pct")) is not None else None}
                  for name, source in (("fused", heat), ("ths", tag)) if source}
        price_status = "OBSERVED" if valid and not any(item["matches_unadjusted_close"] is False for item in checks.values()) else "UNKNOWN"
        previous_date = dates[-2] if len(dates) >= 2 else None
        previous_bars = [row for row in bars if str(row["stock_code"]) == code and _date(row["trade_date"]) == previous_date]
        previous_close = _number(previous_bars[0].get("close")) if len(previous_bars) == 1 else None
        previous_close_match = abs(previous_close - prices["pre_close"]) <= 0.011 if previous_close and prices["pre_close"] else None
        if previous_close_match is False:
            price_status = "UNKNOWN"
        amount = assess_amount_integrity([bar] if bar else [], trade_date=target, max_samples=1)
        if price_status != "OBSERVED":
            amount.update(status="UNKNOWN", reason=";".join(filter(None, (amount.get("reason"), "unverified_price_context"))))
        history = []
        for day in dates:
            matches = [row for row in bars if str(row["stock_code"]) == code and _date(row["trade_date"]) == day]
            if len(matches) == 1:
                item = matches[0]
                history_prices, history_valid = _prices(item)
                if day == target:
                    history_valid = history_valid and price_status == "OBSERVED"
                close, pre = history_prices["close"], history_prices["pre_close"]
                history.append({"trade_date": day, "status": "OBSERVED" if history_valid else "UNKNOWN", "close": close if history_valid else None,
                                "change_pct": (close / pre - 1) * 100 if history_valid else None,
                                **_receipt(item, day)})
        complete = len(dates) == 5 and len(history) == 5 and all(item["change_pct"] is not None for item in history)
        result.append({
            "status": "OBSERVED", "stock_code": code, "short_name": heat.get("short_name") or tag.get("short_name") or bar.get("short_name"),
            "trade_date": target, "sector_names": associations[code], "concept_tag": tag.get("concept_tag"),
            "tag_source": "同日同花顺个股标签；非PIT行业成员或完整概念成分表", "pop_tag": tag.get("pop_tag"),
            "heat_ranks": {"fused": heat.get("fused_rank"), "ths": tag.get("rank")},
            "rank_basis": "source_attention_rank_not_price_strength_or_leadership",
            "identity": "身份待确认", "identity_limit": "热度及标签只构成观察候选，不能证明龙头、中军或容量身份",
            "change_pct": round(change, 6) if change is not None and price_status == "OBSERVED" else None,
            "ohlc": {"status": price_status, **prices}, "price_verification": {"status": price_status, "same_day_source_checks": checks,
                                                     "source": bar.get("data_source"), "quality_status": bar.get("quality_status"),
                                                     "previous_close_matches_pre_close": previous_close_match,
                                                     "previous_close_note": "不一致可能涉及除权等原因，不擅自修正前收盘字段"},
            "amount_quality": amount, "amount": _number(bar.get("amount")) if amount["status"] == "OBSERVED" else None,
            "volume": _number(bar.get("volume")), "history": history, "history_complete_5d": complete,
            "cumulative_return_5d_pct": round((math.prod(1 + item["change_pct"] / 100 for item in history) - 1) * 100, 6) if complete else None,
            "received_at": {"fused": _receipt(heat, target)["received_at"], "ths_tag": _receipt(tag, target)["received_at"],
                            "price": _receipt(bar, target)["received_at"]},
        })
    return result


def collect_theme_evidence(engine, trade_date, *, asof) -> dict:
    """Read exact-date stored facts once per bulk stage; never collect or write.

    Queries use fresh connections, a 15-second server limit and zero retries.
    The total stage deadline is 120 seconds; failures retain observed earlier
    stages and name the failed boundary instead of presenting missing data as 0.
    """
    target = _date(trade_date)
    cutoff = _stamp(asof)
    if cutoff is None:
        raise ValueError("theme evidence asof must be an ISO datetime")
    if cutoff.date().isoformat() < target:
        raise ValueError("theme evidence cutoff is before the target session")
    deadline = time.monotonic() + _DEADLINE_SECONDS
    stages: list[dict] = []
    errors: list[str] = []

    def read(stage: str, sql: str, params: dict, expanding: tuple[str, ...] = ()) -> list[dict]:
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            errors.append(f"{stage}:total_deadline_exceeded")
            stages.append({"stage": stage, "status": "FAILED", "rows": 0, "reason": "total_deadline_exceeded"})
            return []
        statement = text(sql.replace("SELECT ", f"SELECT /*+ MAX_EXECUTION_TIME({min(_QUERY_MS, remaining_ms)}) */ ", 1))
        if expanding:
            statement = statement.bindparams(*(bindparam(key, expanding=True) for key in expanding))
        start = time.monotonic()
        try:
            with routed_read_engine(statement, engine).connect() as connection:
                rows = [dict(row) for row in connection.execute(statement, params).mappings().all()]
            if time.monotonic() > deadline:
                raise TimeoutError("total_deadline_exceeded")
            stages.append({"stage": stage, "status": "OBSERVED", "rows": len(rows), "seconds": round(time.monotonic() - start, 3)})
            return rows
        except Exception as exc:  # A missing optional source must remain visible, not retried.
            reason = f"{stage}:{type(exc).__name__}"
            errors.append(reason)
            stages.append({"stage": stage, "status": "FAILED", "rows": 0, "seconds": round(time.monotonic() - start, 3), "reason": reason})
            return []

    calendar = read("calendar", "SELECT trade_date FROM si_trade_calendar WHERE trade_status=1 AND trade_date<=:d ORDER BY trade_date DESC LIMIT 5", {"d": target})
    dates = sorted({_date(row["trade_date"]) for row in calendar})
    if target not in dates:
        return {"status": "UNKNOWN", "trade_date": target, "asof": cutoff.isoformat(), "sectors": [], "stocks": [],
                "amount_quality": {"target": assess_amount_integrity([], trade_date=target), "previous": assess_amount_integrity([])},
                "sources": stages, "limits": errors + ["目标日期未被交易日历确认为交易日；不以自然日回退"]}
    raw_sectors = read("theme_history", "SELECT snapshot_date,plate_type,`rank`,concept_code,concept_name,change_pct,hot_tag,etl_sync_at FROM st_hot_concept_ths_daily WHERE snapshot_date IN :dates AND plate_type IN (1,2,3,4) ORDER BY snapshot_date,plate_type,`rank` LIMIT 2500", {"dates": dates}, ("dates",))
    sectors_data = _available(raw_sectors, cutoff)
    sectors = _sector_rows(sectors_data, dates, target)
    raw_fused = read("fused_attention", "SELECT snapshot_date,fused_rank,stock_code,short_name,change_pct,industry_name,etl_sync_at FROM st_hot_rank_fused WHERE snapshot_date=:d ORDER BY fused_rank LIMIT 100", {"d": target})
    raw_tags = read("ths_tags", "SELECT snapshot_date,`rank`,stock_code,short_name,change_pct,pop_tag,concept_tag,etl_sync_at FROM st_hot_rank_ths WHERE snapshot_date=:d ORDER BY `rank` LIMIT 100", {"d": target})
    fused, tags = _available(raw_fused, cutoff), _available(raw_tags, cutoff)
    # Only the 16 direction-related candidates consume market-history reads.
    # A measured 100-candidate/two-session query exceeded the production limit;
    # this single <=80-row read replaces it without retrying that failed scope.
    selected_codes = [row["stock_code"] for row in _stock_rows(fused, tags, sectors, [], target, dates)]
    codes = selected_codes
    previous = dates[-2] if len(dates) >= 2 else None
    bar_sql = "SELECT stock_code,short_name,trade_date,adjust_type,open,close,pre_close,high,low,amount,volume,data_source,quality_status,received_at,etl_sync_at FROM sm_stock_kline WHERE trade_date IN :dates AND stock_code IN :codes AND k_type=1 AND adjust_type=0 LIMIT 81"
    bars = _available(read("selected_stock_prices_five_sessions", bar_sql, {"dates": dates, "codes": codes}, ("dates", "codes")), cutoff) if codes else []
    quality = {}
    for key, day in (("target", target), ("previous", previous)):
        receipt = assess_amount_integrity([row for row in bars if _date(row["trade_date"]) == day], trade_date=day)
        receipt.update(coverage_scope="same_day_attention_candidate_sample_not_whole_market", candidate_count=len(codes))
        quality[key] = receipt
    stocks = _stock_rows(fused, tags, sectors, bars, target, dates)
    coverage = [{"trade_date": day, "plate_type": kind, "observed_rows": sum(_date(row["snapshot_date"]) == day and int(row["plate_type"]) == kind for row in sectors_data)} for day in dates for kind in _SOURCES]
    return {
        "status": "OBSERVED" if sectors or stocks else "UNKNOWN", "trade_date": target, "asof": cutoff.isoformat(),
        "history_dates": dates, "sectors": sectors, "stocks": stocks, "amount_quality": quality,
        "sources": stages, "coverage": {"theme_by_date_and_type": coverage, "fused_same_day": len(fused), "ths_same_day": len(tags),
                                          "candidate_codes": len(codes), "selected_stock_count": len(stocks), "price_rows": len(bars),
                                          "excluded_after_cutoff_or_missing_receipt": len(raw_sectors) - len(sectors_data) + len(raw_fused) - len(fused) + len(raw_tags) - len(tags)},
        "source_descriptions": {str(kind): {"name": value[0], "rank_basis": value[1]} for kind, value in _SOURCES.items()},
        "limits": errors + ["热度不是价格强度、资金净流入或龙头身份；类型3/4为东财行业，不能称QMT行业。",
                            "同日THS标签是已存关联观察，不替代同日PIT成分表；不以最新成员回填历史。",
                            "量价核验限候选样本，不能据此确认全市场成交额；不一致金额不参与容量或金额排名。",
                            "晚补收盘事实保留实际入库时间，仅用于复盘补记；榜单缺失不当作零收益。"],
    }
