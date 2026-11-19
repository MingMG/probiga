"""Read-only reconstructions of the research tools demonstrated in the video."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from statistics import mean, median
from time import monotonic
import json
import re
import requests

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from server.api.routers._engine import get_engine
from server.api.routers.market_home import SHANGHAI, _daily_source_valid, VERIFIED_DAILY
from server.common.kline_data import get_kline_engine
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_daily_coverage import (
    MANIFEST_TABLE, NATIVE_EVIDENCE_DATASET, load_daily_native_evidence, load_daily_manifest_completion,
)
from server.common.qmt_announcement_pit import _live_announcement_reply_budget
from server.common.verified_daily_market_truth import VERIFIED_FREE_DAILY_ROW_SOURCES
from server.engine.strategy_industry_history import _exact_snapshot_contract, L1_INDUSTRY_TYPES

router = APIRouter(tags=["research-tools"])


def _read(factory, sql, params, *, deadline, limit=20000, stage="research-tools"):
    if monotonic() >= deadline:
        raise TimeoutError("research read deadline exceeded")
    with factory().connect() as connection:
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
            rows = [dict(row) for row in connection.execute(text(sql), params).mappings()]
    if len(rows) > limit:
        raise ValueError("research result exceeds its row bound")
    return rows


def _scope(day, deadline, count=40):
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("invalid research date")
    now = datetime.now(SHANGHAI)
    if day > now.date().isoformat() or (day == now.date().isoformat() and now.hour < 15):
        raise ValueError("所选交易日尚未收盘")
    rows = _read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(2000) */ trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date<=:day ORDER BY trade_date DESC LIMIT :count
    """, {"day": day, "count": count}, deadline=deadline, limit=count, stage="research-calendar")
    sessions = sorted({str(row["trade_date"])[:10] for row in rows})
    if not sessions or sessions[-1] != day:
        raise ValueError("所选日期不是已发布的交易日")
    return now.replace(tzinfo=None), sessions


def _bindings(values, prefix):
    params = {f"{prefix}{i}": value for i, value in enumerate(values)}
    return ",".join(":" + key for key in params), params


def _quality(params):
    qmt, qm = _bindings(sorted(VERIFIED_DAILY), "qmt")
    free, fm = _bindings(sorted(VERIFIED_FREE_DAILY_ROW_SOURCES), "free")
    params.update(qm | fm)
    return ("permission_status='SUPPORTED' AND ((data_source IN (" + qmt +
            ") AND quality_status IN ('VERIFIED','QMT_ATTESTED')) OR (data_source IN (" + free +
            ") AND quality_status='SOURCE_VERIFIED'))")


def _membership(day, deadline):
    with get_engine().connect() as connection:
        with _live_announcement_reply_budget(connection, deadline=deadline, stage="research-industry-membership"):
            run, members = _exact_snapshot_contract(connection, trade_date=day)
    return run, [row for row in members if row["industry_type"] in L1_INDUSTRY_TYPES]


def _envelope(day, now):
    return {"trade_date": day, "known_at": datetime.now(SHANGHAI).isoformat(timespec="seconds"),
            "input_cutoff_at":now.replace(tzinfo=SHANGHAI).isoformat(timespec="seconds"),
            "evidence_mode": "CURRENT_OBSERVATION", "status": "available"}


def _combine_complete(left, right):
    if left is None:
        return right
    return {code: {
        "stock_code": code, "short_name": right[code]["short_name"] or row["short_name"],
        "peak": max(float(row["peak"]), float(right[code]["peak"])),
        "period_amount": float(row["period_amount"]) + float(right[code]["period_amount"]),
        "last_close": right[code]["last_close"] if right[code]["last_close"] is not None else row["last_close"],
        "observations": row["observations"] + right[code]["observations"],
    } for code, row in left.items() if code in right}


def _native_partitions(start, end, now, deadline):
    candidates = _read(get_engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(2000) */ trade_date,manifest_hash FROM {MANIFEST_TABLE}
        WHERE dataset=:dataset AND status='EXACT' AND strategy_eligible=1
          AND trade_date BETWEEN :start AND :end AND captured_at<=:now
        ORDER BY captured_at DESC,manifest_hash DESC LIMIT 261
    """, {"dataset":NATIVE_EVIDENCE_DATASET,"start":start,"end":end,"now":now},
        deadline=deadline,limit=260,stage="native-research-partitions")
    partitions, evidence = {}, {}
    store = FormalAnalysisStore(get_engine())
    seen = set()
    for candidate in candidates:
        day = str(candidate["trade_date"])[:10]
        if day in seen:
            continue
        seen.add(day)
        params = {"day":day,"now":now}
        quality = _quality(params)
        ordinary = _read(get_kline_engine, f"""
            SELECT /*+ MAX_EXECUTION_TIME(2000) */ COUNT(*) AS n FROM sm_stock_kline
            WHERE trade_date=:day AND k_type=1 AND adjust_type=0 AND etl_sync_at<=:now AND {quality}
        """, params,deadline=deadline,limit=1,stage="native-research-source-selection")
        if ordinary[0]["n"]:
            continue
        digest = candidate["manifest_hash"]
        cached = store.get(digest,"research_native_daily",day)
        with get_engine().connect() as connection:
            with _live_announcement_reply_budget(connection,deadline=deadline,stage="native-research-proof"):
                if cached is None:
                    archived = load_daily_native_evidence(connection,day,decision_known_at=now,manifest_hash=digest)
                    if archived is None:
                        raise ValueError("原生分区缺少对应的完整归档证明")
                    manifest, completion = archived
                else:
                    manifest = cached["manifest"]
                    if (manifest["manifest_hash"] != digest or manifest["trade_date"] != day
                            or canonical_digest({k:v for k,v in manifest.items() if k!="manifest_hash"}) != digest
                            or datetime.fromisoformat(manifest["captured_at"]) > now):
                        raise ValueError("原生研究缓存的数据身份不一致")
                    completion = load_daily_manifest_completion(connection,day,manifest=manifest,decision_known_at=now)
                    if completion is None or canonical_digest(completion) != cached["completion_sha256"]:
                        raise ValueError("原生研究缓存的正式完成记录已改变")
        if cached is None:
            # Content-addressed artifacts survive API restarts and unrelated releases.
            store.get_or_put(digest,"research_native_daily",day,
                             {"manifest":manifest,"completion_sha256":canonical_digest(completion)})
        partitions[day] = {
            entity["stock_code"]: {
                "stock_code":entity["stock_code"],"short_name":None,
                "peak":float(entity["evidence"]["high"]),
                "period_amount":float(entity["evidence"]["amount"]),
                "last_close":float(entity["evidence"]["close"]) if day == end else None,
                "observations":1,
            } for entity in manifest["entities"] if entity["classification"] == "TRADED"
        }
        evidence[day] = {"manifest_hash":manifest["manifest_hash"],"run_id":manifest["run_id"],
                         "captured_at":manifest["captured_at"],"row_count":completion["row_count"]}
    return partitions, evidence


def build_seat_observation(day, *, sessions=5, horizon=1, seat=""):
    deadline = monotonic() + 25
    now, calendar = _scope(day, deadline, sessions + 11)
    window = calendar[-sessions:]
    raw = _read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ trade_date,stock_code,operate_name,
               a_buy_amount,a_sell_amount,reason
        FROM st_a_list_info WHERE trade_date BETWEEN :start AND :end
        ORDER BY trade_date,stock_code LIMIT 20001
    """, {"start": window[0], "end": day}, deadline=deadline, stage="seat-disclosures")
    # One issuer can appear under several listing reasons; do not add the same
    # seat disclosure repeatedly. Keep the coherent largest-gross disclosure.
    unique = {}
    for row in raw:
        code, name = str(row["stock_code"]), str(row["operate_name"] or "").strip()
        if not re.fullmatch(r"[0369][0-9]{5}", code) or not name:
            continue
        buy, sell = row["a_buy_amount"], row["a_sell_amount"]
        if buy is None or sell is None or buy < 0 or sell < 0:
            continue
        key = (str(row["trade_date"])[:10], code, name)
        normalized = {"trade_date": key[0], "stock_code": code, "seat": name,
                      "buy_amount": float(buy), "sell_amount": float(sell), "reason": str(row["reason"] or "")}
        previous = unique.get(key)
        if previous is None or (buy + sell, normalized["reason"]) > (
            previous["buy_amount"] + previous["sell_amount"], previous["reason"]
        ):
            unique[key] = normalized
    disclosures = list(unique.values())
    totals = Counter()
    for row in disclosures:
        totals[row["seat"]] += row["buy_amount"]
    options = [{"seat": name, "buy_amount": value} for name, value in totals.most_common()]
    selected = seat or (options[0]["seat"] if options else "")
    selected_rows = [row for row in disclosures if row["seat"] == selected]
    codes = sorted({row["stock_code"] for row in selected_rows})
    prices = {}
    if codes:
        clause, params = _bindings(codes, "code")
        params.update(start=window[0], end=day, now=now)
        quotes = _read(get_kline_engine, f"""
            SELECT /*+ INDEX(sm_stock_kline idx_kline_code_type_date) MAX_EXECUTION_TIME(5000) */ stock_code,short_name,trade_date,close,
                   data_source,quality_status,permission_status
            FROM sm_stock_kline WHERE stock_code IN ({clause}) AND trade_date BETWEEN :start AND :end
              AND k_type=1 AND adjust_type=0 AND etl_sync_at<=:now LIMIT 20001
        """, params, deadline=deadline, stage="seat-price-window")
        duplicates = set()
        for row in quotes:
            if not _daily_source_valid(row) or row["close"] is None or row["close"] <= 0:
                continue
            key = (str(row["trade_date"])[:10], row["stock_code"])
            if key in prices:
                duplicates.add(key)
            prices[key] = row
        for key in duplicates:
            prices.pop(key, None)
    results = []
    for row in selected_rows:
        index = calendar.index(row["trade_date"]) + horizon
        exit_day = calendar[index] if index < len(calendar) else None
        entry = prices.get((row["trade_date"], row["stock_code"]))
        exit_quote = prices.get((exit_day, row["stock_code"]))
        change = (round((float(exit_quote["close"]) / float(entry["close"]) - 1) * 100, 8)
                  if entry and exit_quote else None)
        results.append({**row, "short_name": (entry or {}).get("short_name") or row["stock_code"],
                        "exit_date": exit_day, "price_change_pct": change,
                        "observation_status": "MATURE" if change is not None else
                        "NOT_MATURE" if exit_day is None else "QUOTE_UNAVAILABLE"})
    mature = [row["price_change_pct"] for row in results if row["buy_amount"] > 0 and row["price_change_pct"] is not None]
    bounds = [("≤-5%", None, -5), ("-5%～0%", -5, 0), ("0%～5%", 0, 5), ("5%～10%", 5, 10), (">10%", 10, None)]
    buckets = [{"label": label, "count": sum((lo is None or x > lo) and (hi is None or x <= hi) for x in mature)}
               for label, lo, hi in bounds]
    subjects = {(row["trade_date"], row["stock_code"]) for row in selected_rows if row["buy_amount"] > 0}
    overlap = Counter(row["seat"] for row in disclosures if row["seat"] != selected and row["buy_amount"] > 0
                      and (row["trade_date"], row["stock_code"]) in subjects)
    return {**_envelope(day, now), "status": "available" if raw else "unavailable",
            "selected_seat": selected, "seats": options, "sessions": window,
            "horizon": horizon, "raw_disclosure_count": len(raw), "deduplicated_count": len(disclosures),
            "buy_amount": sum(row["buy_amount"] for row in selected_rows),
            "sell_amount": sum(row["sell_amount"] for row in selected_rows),
            "security_count": len(codes), "mature_count": len(mature),
            "mean_change_pct": mean(mature) if mature else None, "median_change_pct": median(mature) if mature else None,
            "positive_ratio_pct": sum(value > 0 for value in mature) / len(mature) * 100 if mature else None,
            "distribution": buckets, "rows": sorted(results, key=lambda row: (row["trade_date"], row["stock_code"]), reverse=True),
            "co_disclosures": [{"seat": name, "count": count} for name, count in overlap.most_common(8)],
            "method": "同日、同股、同席位取披露买卖总额最大的一条；金额属于榜单披露窗口，非已证明的单日实际成交。",
            "return_method": "披露日收盘至第N个交易日收盘的不复权价格变化；未成熟或缺行情保留未知，未计分红及费用。",
            "identity_basis": "仅为公开营业部名称；不推断操盘者身份，共同上榜不证明联动交易，价格变化不代表真实账户收益。"}


def build_period_drawdowns(day, *, start, tmt=False):
    deadline = monotonic() + 110
    now, calendar = _scope(day, deadline, 260)
    start = date.fromisoformat(start).isoformat()
    window = [value for value in calendar if value >= start]
    if start > day or not window or (date.fromisoformat(day) - date.fromisoformat(start)).days > 365:
        raise ValueError("区间必须位于所选日之前的一年内")
    native, native_evidence = _native_partitions(window[0], day, now, deadline)
    aggregate = None
    # Twenty-session scans exceeded the live query budget; five sessions keep
    # each native-data partition bounded without one request per security.
    for offset in range(0, len(window), 5):
        batch = window[offset:offset+5]
        ordinary_days = [session for session in batch if session not in native]
        clause, params = _bindings(ordinary_days, "day")
        params.update(day=day, now=now, count=len(ordinary_days))
        quality = _quality(params)
        part = _read(get_kline_engine, f"""
            SELECT /*+ INDEX(sm_stock_kline idx_date_ktype) MAX_EXECUTION_TIME(4000) */ stock_code,
                   MAX(CASE WHEN trade_date=:day THEN short_name END) AS short_name,
                   MAX(high) AS peak,SUM(amount) AS period_amount,
                   MAX(CASE WHEN trade_date=:day THEN close END) AS last_close,COUNT(*) AS observations
            FROM sm_stock_kline WHERE trade_date IN ({clause}) AND k_type=1 AND adjust_type=0
              AND etl_sync_at<=:now AND close>0 AND high>=close AND amount>=0 AND {quality}
            GROUP BY stock_code HAVING COUNT(*)=:count AND COUNT(DISTINCT trade_date)=:count LIMIT 7001
        """, params, deadline=deadline, limit=7000, stage=f"period-drawdown-batch:{offset//5+1}") if ordinary_days else []
        current = {row["stock_code"]: row for row in part}
        if not ordinary_days:
            current = None
        for session in batch:
            if session in native:
                current = _combine_complete(current, native[session])
        # Intersect complete partitions; missing bars never become zero returns.
        aggregate = _combine_complete(aggregate, current)
    rows = sorted((aggregate or {}).values(), key=lambda row: (-row["period_amount"], row["stock_code"]))
    complete_sample = len(rows)
    ranked = rows[:100]
    membership = {}
    if tmt:
        _, members = _membership(day, deadline)
        membership = {row["stock_code"]: row["industry_name"] for row in members}
        ranked = [row for row in ranked if any(name in membership.get(row["stock_code"], "") for name in ("电子", "计算机", "通信", "传媒"))]
    output = [{"stock_code": row["stock_code"], "short_name": row["short_name"],
               "peak": float(row["peak"]), "price": float(row["last_close"]),
               "period_amount": float(row["period_amount"]), "observations": row["observations"],
               "drawdown_pct": (float(row["peak"]) - float(row["last_close"])) / float(row["peak"]) * 100,
               "industry": membership.get(row["stock_code"])} for row in ranked if row["last_close"] is not None]
    output.sort(key=lambda row: (-row["drawdown_pct"], row["stock_code"]))
    return {**_envelope(day, now), "status":"available" if complete_sample else "unavailable",
            "start_date": start, "session_count": len(window), "tmt": tmt, "native_partitions":native_evidence,
            "complete_sample_count": complete_sample, "liquidity_top_count": min(100, complete_sample), "rows": output,
            "method": "完整窗口样本按区间成交额取前100，再按当日QMT一级行业筛选TMT；(区间最高价－所选收盘价)/区间最高价。",
            "coverage_note": "只使用窗口内每个交易日都有已核验不复权日线的股票；新上市、停牌或缺行情的非完整窗口样本不参与排序。"}


def build_industry_breadth(day, *, period=20):
    deadline = monotonic() + 18
    now, calendar = _scope(day, deadline, period)
    if len(calendar) != period:
        raise ValueError("均线所需交易日历不完整")
    clause, params = _bindings(calendar, "day")
    params.update(day=day, now=now, count=period)
    quality = _quality(params)
    quotes = _read(get_kline_engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(4500) */ stock_code,AVG(close) AS moving_average,
               MAX(CASE WHEN trade_date=:day THEN close END) AS last_close
        FROM sm_stock_kline WHERE trade_date IN ({clause}) AND k_type=1 AND adjust_type=0
          AND etl_sync_at<=:now AND close>0 AND {quality}
        GROUP BY stock_code HAVING COUNT(*)=:count AND COUNT(DISTINCT trade_date)=:count
        LIMIT 7001
    """, params, deadline=deadline, limit=7000, stage="industry-breadth-aggregate")
    valid = {row["stock_code"]: float(row["last_close"]) > float(row["moving_average"]) for row in quotes}
    run, members = _membership(day, deadline)
    groups = defaultdict(set)
    for row in members:
        groups[(row["industry_code"], row["industry_name"])].add(row["stock_code"])
    rows = []
    for (code, name), codes in groups.items():
        sample = codes & valid.keys()
        above = sum(valid[stock] for stock in sample)
        rows.append({"industry_code": code, "industry_name": name, "member_count": len(codes),
                     "sample_count": len(sample), "above_count": above,
                     "breadth_pct": above / len(sample) * 100 if sample else None})
    rows.sort(key=lambda row: (row["breadth_pct"] is None, -(row["breadth_pct"] or 0), row["industry_code"]))
    return {**_envelope(day, now), "period": period, "sample_count": len(valid),
            "above_count": sum(valid.values()), "breadth_pct": sum(valid.values()) / len(valid) * 100 if valid else None,
            "membership_hash": run["industry_hash"], "membership_captured_at": run["captured_at"], "rows": rows,
            "method": "收盘价严格高于含当日在内的N个交易日收盘价均值；行业宽度=站上均线数/有效成员数。",
            "reconstruction_basis": "视频明确提到20或30日均线宽度；原私有热度总分未给公式，此处展示可复算的均线宽度。"}


def _serve(builder, *args, **kwargs):
    from server.api.routers.hot_data import _cache_get, _cache_set

    key = "research-tools:" + builder.__name__ + json.dumps([args, kwargs], sort_keys=True)
    cached = _cache_get(key, ttl_seconds=120)
    if cached is not None:
        return cached
    try:
        result = builder(*args, **kwargs)
        if result.get("status") == "available":
            _cache_set(key, result)
        return result
    except ValueError as exc:
        raise HTTPException(422, detail=str(exc)) from exc
    except (SQLAlchemyError, RuntimeError, TimeoutError, requests.RequestException) as exc:
        raise HTTPException(503, detail={"stage": builder.__name__, "reason": str(exc)[:200]}) from exc


@router.get("/research/seat-observation")
def seat_observation(day: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$"),
                     sessions: int = Query(5, ge=5, le=10), horizon: int = Query(1, ge=1, le=10),
                     seat: str = Query("", max_length=160)):
    return _serve(build_seat_observation, day, sessions=sessions, horizon=horizon, seat=seat)


@router.get("/research/period-drawdowns")
def period_drawdowns(day: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$"),
                     start: str = Query(pattern=r"^\d{4}-\d{2}-\d{2}$"), tmt: bool = False):
    return _serve(build_period_drawdowns, day, start=start, tmt=tmt)


@router.get("/research/industry-breadth")
def industry_breadth(day: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$"),
                     period: int = Query(20, ge=20, le=30)):
    return _serve(build_industry_breadth, day, period=period)
