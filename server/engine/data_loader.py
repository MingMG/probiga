# -*- coding: utf-8 -*-
"""
统一数据加载器

从数据库加载股票全量分析数据，供四层引擎使用。
复用现有 hot_data.py 中的数据加载逻辑，保证数据一致性。
"""

import sys
from pathlib import Path as _Path
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text

# 添加项目根目录到 path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from server.api.routers._engine import get_engine
from server.common.pit_facts import (
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    load_event_facts,
    load_finance_history_facts,
    normalize_decision_at,
)


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    """执行SQL查询，返回字典列表"""
    import numpy as np
    try:
        df = pd.read_sql(text(sql), get_engine(), params=params)
        if df.empty:
            return []
        df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
        for c in df.columns:
            if df[c].dtype == "datetime64[ns]":
                df[c] = df[c].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        # 表不存在或列不存在时返回空列表，避免阻塞分析
        err_msg = str(e)
        if "doesn't exist" in err_msg or "Unknown column" in err_msg:
            return []
        raise


def _latest_kline_trade_date(trade_date: str | None = None) -> str:
    sql = "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1"
    params = {}
    if trade_date:
        sql += " AND trade_date <= :td"
        params["td"] = trade_date
    rows = _read_sql(sql, params)
    if rows and rows[0].get("d"):
        return str(rows[0]["d"])[:10]
    if trade_date:
        raise RuntimeError(f"sm_stock_kline 在 {trade_date} 及之前没有可用日线数据")
    return date.today().isoformat()


def _calc_percentile(current_value: float | None, history_values: list[float]) -> float | None:
    if current_value is None:
        return None
    valid_values = [float(v) for v in history_values if v is not None and float(v) > 0]
    if not valid_values:
        return None
    below = sum(1 for value in valid_values if value <= current_value)
    return round(below / len(valid_values) * 100, 1)


def _load_historical_valuation_samples(stock_code: str, trade_date: str) -> tuple[list[float], list[float]]:
    rows = _read_sql("""
        SELECT
            f.report_date,
            f.basic_eps,
            f.net_asset_ps,
            (
                SELECT k.close
                FROM sm_stock_kline k
                WHERE k.stock_code = f.stock_code
                  AND k.k_type = 1
                  AND k.trade_date <= f.report_date
                ORDER BY k.trade_date DESC
                LIMIT 1
            ) AS ref_close
        FROM si_stock_finance f
        WHERE f.stock_code = :c AND f.report_date <= :td
        ORDER BY f.report_date DESC
        LIMIT 20
    """, {"c": stock_code, "td": trade_date})

    pe_values: list[float] = []
    pb_values: list[float] = []
    for row in rows:
        ref_close = float(row.get("ref_close") or 0)
        eps = float(row.get("basic_eps") or 0)
        bvps = float(row.get("net_asset_ps") or 0)
        if ref_close > 0 and eps > 0:
            pe_values.append(ref_close / eps)
        if ref_close > 0 and bvps > 0:
            pb_values.append(ref_close / bvps)
    return pe_values, pb_values


def _pit_finance_bundle(
    stock_code: str,
    trade_date: str,
    decision_at: datetime | str | None,
    fact_cutoff_at: datetime | str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[float], list[float], dict[str, Any]]:
    """Load one strategy-safe finance prefix and valuation observations."""

    code = str(stock_code).zfill(6)
    if decision_at is None:
        evidence = {
            "pit_status": PIT_DATA_BLOCKED,
            "pit_reason": "PIT_FINANCE_EXACT_DECISION_TIME_REQUIRED",
            "manifest_hash": "",
            "revision_ids": [],
            "content_hashes": [],
        }
        return {}, [], [], [], evidence
    batch = load_finance_history_facts(
        get_engine(),
        codes=[code],
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        as_of_date=trade_date,
        limit_per_code=20,
    )
    status = batch.status_for(code)
    rows = [dict(row) for row in (batch.facts.get(code) or [])]
    coverage = dict(batch.coverage_by_code.get(code) or {})
    if status != PIT_AVAILABLE:
        rows = []
    latest = dict(rows[0]) if rows else {}
    evidence = {
        "pit_status": PIT_AVAILABLE if status == PIT_AVAILABLE else PIT_DATA_BLOCKED,
        "pit_reason": (
            batch.reason_for(code)
            or ("" if status == PIT_AVAILABLE else "PIT_FINANCE_COVERAGE_UNPROVEN")
        ),
        "manifest_hash": batch.manifest_hash,
        "revision_ids": [
            str(row.get("finance_revision_id") or "")
            for row in rows
            if row.get("finance_revision_id")
        ],
        "content_hashes": [
            str(row.get("finance_content_hash") or "")
            for row in rows
            if row.get("finance_content_hash")
        ],
        "authoritative_empty": bool(
            status == PIT_AVAILABLE and not rows and coverage
        ),
        "coverage_id": coverage.get("coverage_id"),
        "coverage_response_hash": coverage.get("coverage_response_hash"),
        "coverage_watermark_hash": coverage.get("coverage_watermark_hash"),
        "covered_through_at": coverage.get("covered_through_at"),
        "fact_cutoff_at": batch.fact_cutoff_at,
        "decision_at": batch.decision_at,
    }
    pe_values: list[float] = []
    pb_values: list[float] = []
    for row in rows:
        report_date = str(
            row.get("finance_report_date") or row.get("report_date") or ""
        )[:10]
        if not report_date:
            continue
        prices = _read_sql(
            """
            SELECT close AS ref_close
            FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1 AND trade_date <= :rd
            ORDER BY trade_date DESC LIMIT 1
            """,
            {"c": code, "rd": report_date},
        )
        ref_close = float((prices[0] if prices else {}).get("ref_close") or 0)
        eps = float(row.get("basic_eps") or 0)
        bvps = float(row.get("net_asset_ps") or 0)
        if ref_close > 0 and eps > 0:
            pe_values.append(ref_close / eps)
        if ref_close > 0 and bvps > 0:
            pb_values.append(ref_close / bvps)
    return latest, rows, pe_values, pb_values, evidence


def _pit_notice_bundle(
    stock_code: str,
    trade_date: str,
    decision_at: datetime | str | None,
    fact_cutoff_at: datetime | str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    code = str(stock_code).zfill(6)
    if decision_at is None:
        return [], {
            "event_pit_status": PIT_DATA_BLOCKED,
            "event_pit_reason": "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED",
            "event_manifest_hash": "",
            "event_revision_ids": [],
            "event_content_hashes": [],
        }
    decision = normalize_decision_at(decision_at)
    event_end = normalize_decision_at(
        fact_cutoff_at if fact_cutoff_at is not None else decision
    ).date()
    batch = load_event_facts(
        get_engine(),
        codes=[code],
        decision_at=decision,
        fact_cutoff_at=fact_cutoff_at,
        start_date=event_end - timedelta(days=14),
        end_date=event_end,
        require_qmt_complete_batch=True,
    )
    status = batch.status_for(code)
    coverage = dict(batch.coverage_by_code.get(code) or {})
    rows: list[dict[str, Any]] = []
    if status == PIT_AVAILABLE:
        for raw in (batch.facts.get(code) or [])[:10]:
            item = dict(raw)
            published_at = str(item.get("event_published_at") or "")
            item["notice_date"] = published_at[:10]
            rows.append(item)
    evidence = {
        "event_pit_status": (
            PIT_AVAILABLE if status == PIT_AVAILABLE else PIT_DATA_BLOCKED
        ),
        "event_pit_reason": (
            batch.reason_for(code)
            or ("" if status == PIT_AVAILABLE else "PIT_EVENT_COVERAGE_UNPROVEN")
        ),
        "event_manifest_hash": batch.manifest_hash,
        "event_revision_ids": [
            str(row.get("event_revision_id") or "")
            for row in rows
            if row.get("event_revision_id")
        ],
        "event_content_hashes": [
            str(row.get("event_content_hash") or "")
            for row in rows
            if row.get("event_content_hash")
        ],
        "event_authoritative_empty": bool(
            status == PIT_AVAILABLE and not rows and coverage
        ),
        "event_coverage_id": coverage.get("coverage_id"),
        "event_coverage_response_hash": coverage.get(
            "coverage_response_hash"
        ),
        "event_coverage_watermark_hash": coverage.get(
            "coverage_watermark_hash"
        ),
        "event_fact_cutoff_at": batch.fact_cutoff_at,
        "event_decision_at": batch.decision_at,
        "event_covered_through_at": coverage.get("covered_through_at"),
    }
    return rows, evidence


def _compute_technical(kline_data, cur_price):
    """计算技术指标：MA/MACD/KDJ/RSI/BOLL/支撑压力"""
    if not kline_data or len(kline_data) < 20:
        return {}

    # kline_data 按日期倒序，转为正序计算
    rows = list(reversed(kline_data))
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    n = len(closes)

    # 均线
    def ma(data, period):
        if len(data) < period:
            return None
        return round(sum(data[-period:]) / period, 2)

    ma5 = ma(closes, 5)
    ma10 = ma(closes, 10)
    ma20 = ma(closes, 20)
    ma60 = ma(closes, 60)
    ma120 = ma(closes, 120)
    ma250 = ma(closes, 250)

    # MACD (12, 26, 9)
    ema12 = closes[0]
    ema26 = closes[0]
    dif_list = []
    dea = 0
    for c in closes:
        ema12 = ema12 * 11 / 13 + c * 2 / 13
        ema26 = ema26 * 25 / 27 + c * 2 / 27
        dif = ema12 - ema26
        dea = dea * 8 / 10 + dif * 2 / 10
        dif_list.append({"dif": round(dif, 4), "dea": round(dea, 4), "hist": round((dif - dea) * 2, 4)})

    macd_cur = dif_list[-1] if dif_list else {}
    macd_prev = dif_list[-2] if len(dif_list) >= 2 else {}
    golden_cross = (macd_prev.get("dif", 0) < macd_prev.get("dea", 0) and
                    macd_cur.get("dif", 0) > macd_cur.get("dea", 0))

    # KDJ (9, 3, 3)
    k, d = 50, 50
    for i in range(8, n):
        period_high = max(highs[i - 8:i + 1])
        period_low = min(lows[i - 8:i + 1])
        if period_high == period_low:
            rsv = 50
        else:
            rsv = (closes[i] - period_low) / (period_high - period_low) * 100
        k = k * 2 / 3 + rsv / 3
        d = d * 2 / 3 + k / 3
    j = 3 * k - 2 * d

    # RSI (6, 12, 24)
    def calc_rsi(data, period):
        if len(data) < period + 1:
            return None
        gains, losses = [], []
        for i in range(len(data) - period, len(data)):
            diff = data[i] - data[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return round(100 - 100 / (1 + rs), 2)

    rsi6 = calc_rsi(closes, 6)
    rsi12 = calc_rsi(closes, 12)
    rsi24 = calc_rsi(closes, 24)

    # BOLL (20, 2)
    if n >= 20:
        boll_mid = sum(closes[-20:]) / 20
        variance = sum((c - boll_mid) ** 2 for c in closes[-20:]) / 20
        std = variance ** 0.5
        boll_upper = round(boll_mid + 2 * std, 2)
        boll_lower = round(boll_mid - 2 * std, 2)
        boll_mid = round(boll_mid, 2)
    else:
        boll_upper = boll_mid = boll_lower = None

    # 支撑位和压力位
    recent_lows_5 = lows[-5:] if len(lows) >= 5 else lows
    recent_highs_5 = highs[-5:] if len(highs) >= 5 else highs
    recent_lows_20 = lows[-20:] if len(lows) >= 20 else lows
    recent_highs_20 = highs[-20:] if len(highs) >= 20 else highs
    support = round(min(recent_lows_5), 2)
    resistance = round(max(recent_highs_5), 2)
    support_mid = round(min(recent_lows_20), 2)
    resistance_mid = round(max(recent_highs_20), 2)

    # 趋势判断
    short_trend = "上涨" if ma5 and ma10 and ma5 > ma10 else "下跌" if ma5 and ma10 else "震荡"
    mid_trend = "上涨" if ma20 and ma60 and ma20 > ma60 else "下跌" if ma20 and ma60 else "震荡"
    long_trend = "上涨" if ma120 and ma250 and ma120 > ma250 else "下跌" if ma120 and ma250 else "震荡"

    return {
        "ma": {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60, "ma120": ma120, "ma250": ma250},
        "macd": {**macd_cur, "golden_cross": golden_cross},
        "kdj": {"k": round(k, 2), "d": round(d, 2), "j": round(j, 2)},
        "rsi": {"rsi6": rsi6, "rsi12": rsi12, "rsi24": rsi24},
        "boll": {"upper": boll_upper, "mid": boll_mid, "lower": boll_lower},
        "support": support,
        "resistance": resistance,
        "support_mid": support_mid,
        "resistance_mid": resistance_mid,
        "trend": {"short": short_trend, "mid": mid_trend, "long": long_trend},
    }


class StockDataLoader:
    """
    统一数据加载器

    从数据库加载股票全量分析数据，供四层引擎使用。
    """

    def load_full_data(
        self,
        stock_code: str,
        trade_date: str | None = None,
        use_realtime: bool | None = None,
        include_market_mood: bool = True,
        *,
        strategy_context: bool = False,
        decision_at: datetime | str | None = None,
        fact_cutoff_at: datetime | str | None = None,
    ) -> dict:
        """
        加载7大模块数据（复用 stock-detail 的逻辑）

        返回格式：
        {
            "basic": {...},          # 基本信息
            "market": {...},         # 行情数据
            "capital": {...},        # 资金面
            "finance": {...},        # 财务面
            "valuation": {...},      # 估值面
            "technical": {...},      # 技术面
            "news": {...},           # 消息面
            "holder": {...},         # 股东人数
            "hot_rank": {...},       # 热门排名
            "lifting": {...},        # 解禁信息
            "mine_clearance": {...}, # 扫雷信息
            "holding": {...},        # 持仓信息
        }
        """
        code = stock_code.strip().zfill(6)
        use_realtime = (trade_date is None) if use_realtime is None else bool(use_realtime)

        # 获取最新交易日
        trade_date = _latest_kline_trade_date(trade_date)
        strategy_reader_decision = (
            decision_at
            if (not strategy_context or fact_cutoff_at is not None)
            else None
        )

        # ─── 基本信息 ───
        basic_rows = _read_sql(
            "SELECT stock_code, short_name, exchange, list_date FROM si_all_code WHERE stock_code = :c",
            {"c": code}
        )
        if not basic_rows:
            raise ValueError(f"股票 {code} 不存在")
        basic = basic_rows[0]

        strategy_reference_evidence = {
            "status": "DATA_BLOCKED" if strategy_context else "LEGACY_UNVERIFIED",
            "reason": (
                "LEGACY_CURRENT_REFERENCE_INPUTS_IGNORED:"
                "industry,concept,holder,hot_rank,lifting,mine_clearance"
                if strategy_context
                else "DISPLAY_ONLY_MUTABLE_REFERENCE_INPUTS"
            ),
            "funding_authority": False,
            "order_authority": False,
        }
        if strategy_context:
            # Current industry/concept mappings have no exact-date immutable
            # identity here.  Returning neutral values prevents migrations,
            # late backfills or current taxonomy from changing old scores.
            industry = None
            concepts: list[str] = []
        else:
            industry_rows = _read_sql(
                "SELECT plate_name FROM si_stock_plate_east WHERE stock_code = :c AND plate_type = '行业'",
                {"c": code}
            )
            if industry_rows and industry_rows[0].get("plate_name"):
                industry = industry_rows[0]["plate_name"]
            else:
                sw_rows = _read_sql(
                    "SELECT industry_name FROM si_industry_sw WHERE stock_code = :c AND industry_type = '申万一级'",
                    {"c": code}
                )
                industry = sw_rows[0]["industry_name"] if sw_rows and sw_rows[0].get("industry_name") else None
            concept_rows = _read_sql(
                "SELECT DISTINCT name FROM si_stock_concept_east WHERE stock_code = :c LIMIT 20",
                {"c": code}
            )
            concepts = [r["name"] for r in concept_rows if r.get("name")]

        # ─── 一、行情数据 ───
        quote = {}
        if use_realtime:
            cur_rows = _read_sql(
                "SELECT price, change_pct, snapshot_at FROM sm_stock_current WHERE stock_code = :c "
                "ORDER BY snapshot_at DESC LIMIT 1",
                {"c": code}
            )
            if cur_rows and cur_rows[0].get("price") is not None:
                quote = {**cur_rows[0], "source": "realtime"}
        if not quote:
            kline_rows = _read_sql(
                "SELECT close AS price, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close "
                "FROM sm_stock_kline WHERE stock_code = :c AND trade_date = :td AND k_type=1",
                {"c": code, "td": trade_date}
            )
            if kline_rows:
                quote = {**kline_rows[0], "source": "kline"}
        if quote.get("high") and quote.get("low") and quote.get("open") and float(quote.get("open", 0)) > 0:
            quote["amplitude"] = round((float(quote["high"]) - float(quote["low"])) / float(quote["open"]) * 100, 2)

        # 股本 + 市值
        cap_rows = _read_sql(
            "SELECT total_shares, limit_shares, list_a_shares FROM si_stock_shares WHERE stock_code = :c",
            {"c": code}
        )
        cap = cap_rows[0] if cap_rows else {}
        price_val = float(quote.get("price") or 0)
        total_shares = float(cap.get("total_shares") or 0)
        float_shares = float(cap.get("list_a_shares") or 0)

        # 量比
        vol_rows = _read_sql("""
            SELECT AVG(volume) AS avg_vol FROM (
                SELECT volume FROM sm_stock_kline
                WHERE stock_code = :c AND k_type=1 AND trade_date < :td
                ORDER BY trade_date DESC LIMIT 5
            ) t
        """, {"c": code, "td": trade_date})
        avg_vol = float(vol_rows[0]["avg_vol"]) if vol_rows and vol_rows[0].get("avg_vol") else 0
        cur_vol = float(quote.get("volume") or 0)
        volume_ratio = round(cur_vol / avg_vol, 2) if avg_vol > 0 else None

        # 财务指标：策略评分只读取不可变 PIT 前缀；旧表仅保留给实时展示。
        finance_evidence: dict[str, Any]
        pit_finance_rows: list[dict[str, Any]]
        if strategy_context:
            (
                fin,
                pit_finance_rows,
                pe_history,
                pb_history,
                finance_evidence,
            ) = _pit_finance_bundle(
                code, trade_date, strategy_reader_decision, fact_cutoff_at
            )
        else:
            fin_rows = _read_sql("""
                SELECT basic_eps, net_asset_ps, roe_wtd, roa_wtd, gross_margin, net_margin,
                       total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr,
                       report_date
                FROM si_stock_finance WHERE stock_code = :c AND report_date <= :td
                ORDER BY report_date DESC LIMIT 1
            """, {"c": code, "td": trade_date})
            fin = fin_rows[0] if fin_rows else {}
            pit_finance_rows = []
            pe_history, pb_history = _load_historical_valuation_samples(
                code, trade_date
            )
            finance_evidence = {
                "pit_status": "LEGACY_UNVERIFIED",
                "pit_reason": "DISPLAY_ONLY_MUTABLE_FINANCE_CACHE",
                "manifest_hash": "",
                "revision_ids": [],
                "content_hashes": [],
            }
        eps = float(fin.get("basic_eps") or 0)
        bvps = float(fin.get("net_asset_ps") or 0)
        pe_ttm = round(price_val / eps, 2) if eps and eps > 0 and price_val else None
        pb = round(price_val / bvps, 2) if bvps and bvps > 0 and price_val else None
        pe_percentile = _calc_percentile(pe_ttm, pe_history)
        pb_percentile = _calc_percentile(pb, pb_history)
        verdict = "偏高" if (pe_percentile is not None and pe_percentile > 70) else "偏低" if (pe_percentile is not None and pe_percentile < 30) else "合理" if pe_percentile is not None else None

        market = {
            "price": quote.get("price"),
            "change_pct": quote.get("change_pct"),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "close": quote.get("close") or quote.get("price"),
            "pre_close": quote.get("pre_close"),
            "volume": quote.get("volume"),
            "amount": quote.get("amount"),
            "turnover_ratio": quote.get("turnover_ratio"),
            "amplitude": quote.get("amplitude"),
            "pe_ttm": pe_ttm,
            "pb": pb,
            "volume_ratio": volume_ratio,
            "total_shares": total_shares,
            "float_shares": float_shares,
            "market_cap": round(price_val * total_shares, 2) if price_val and total_shares else None,
            "float_market_cap": round(price_val * float_shares, 2) if price_val and float_shares else None,
        }

        # ─── 二、资金面 ───
        # 注意：数据库中存储的单位是"元"，需要转换为"万元"供评分使用
        flow_td_rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily WHERE trade_date <= :td",
            {"td": trade_date},
        )
        flow_td = str(flow_td_rows[0]["d"])[:10] if flow_td_rows and flow_td_rows[0].get("d") else trade_date
        flow_rows = _read_sql(
            "SELECT main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source "
            "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :ftd",
            {"c": code, "ftd": flow_td}
        )
        flow_today = {}
        if flow_rows:
            # 从元转换为万元
            flow_today = {
                "main_net_inflow": float(flow_rows[0].get("main_net_inflow") or 0) / 10000,
                "max_net_inflow": float(flow_rows[0].get("max_net_inflow") or 0) / 10000,
                "lg_net_inflow": float(flow_rows[0].get("lg_net_inflow") or 0) / 10000,
                "mid_net_inflow": float(flow_rows[0].get("mid_net_inflow") or 0) / 10000,
                "sm_net_inflow": float(flow_rows[0].get("sm_net_inflow") or 0) / 10000,
                "data_source": flow_rows[0].get("data_source") or "east",
            }

        # 近3/5/20日资金流向累计（转换为万元）
        flow_multi = {}
        for days, label in [(3, "flow_3d"), (5, "flow_5d"), (20, "flow_20d")]:
            mf = _read_sql(f"""
                SELECT SUM(main_net_inflow) AS main_net_inflow
                FROM sm_stock_capital_flow_daily
                WHERE stock_code = :c AND trade_date <= :ftd AND trade_date >= (
                    SELECT trade_date FROM sm_stock_kline WHERE k_type=1 AND trade_date <= :td
                    GROUP BY trade_date ORDER BY trade_date DESC LIMIT 1 OFFSET {days - 1}
                )
            """, {"c": code, "td": flow_td, "ftd": flow_td})
            # 从元转换为万元
            flow_multi[label] = float(mf[0]["main_net_inflow"] or 0) / 10000 if mf and mf[0].get("main_net_inflow") else None

        # 龙虎榜（近20日）
        lhb_rows = _read_sql("""
            SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy
            FROM st_a_list_daily
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
        """, {"c": code, "td": trade_date})
        lhb = lhb_rows[0] if lhb_rows else {}

        lhb_seats = _read_sql("""
            SELECT trade_date, operate_name, a_net_amount, a_buy_amount, a_sell_amount
            FROM st_a_list_info
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
            ORDER BY trade_date DESC LIMIT 10
        """, {"c": code, "td": trade_date})

        capital = {
            "today": flow_today,
            "flow_3d": flow_multi.get("flow_3d"),
            "flow_5d": flow_multi.get("flow_5d"),
            "flow_20d": flow_multi.get("flow_20d"),
            "dragon_tiger": {
                "count_20d": int(lhb.get("cnt") or 0),
                "inst_net_buy": lhb.get("inst_net_buy"),
                "seats": lhb_seats,
            },
        }

        # ─── 股东人数 ───
        holder_rows = [] if strategy_context else _read_sql("""
            SELECT report_date, holder_num, holder_num_change, pre_holder_num,
                   holder_num_ratio, avg_free_shares
            FROM si_stock_holder WHERE stock_code = :c AND report_date <= :td
            ORDER BY report_date DESC LIMIT 2
        """, {"c": code, "td": trade_date})
        holder = {}
        if holder_rows:
            h0 = holder_rows[0]
            holder = {
                "report_date": str(h0.get("report_date", "")),
                "holder_num": int(h0["holder_num"]) if h0.get("holder_num") is not None else None,
                "holder_num_change": int(h0["holder_num_change"]) if h0.get("holder_num_change") is not None else None,
                "pre_holder_num": int(h0["pre_holder_num"]) if h0.get("pre_holder_num") is not None else None,
                "holder_num_ratio": float(h0["holder_num_ratio"]) if h0.get("holder_num_ratio") is not None else None,
                "avg_free_shares": float(h0["avg_free_shares"]) if h0.get("avg_free_shares") is not None else None,
            }

        # ─── 三、财务面 ───
        if strategy_context:
            fin_detail_rows = pit_finance_rows[:8]
        else:
            fin_detail_rows = _read_sql("""
                SELECT report_date, report_type, basic_eps, net_asset_ps,
                       total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr,
                       roe_wtd, roa_wtd, gross_margin, net_margin,
                       curr_ratio, quick_ratio, asset_liab_ratio
                FROM si_stock_finance WHERE stock_code = :c AND report_date <= :td
                ORDER BY report_date DESC LIMIT 8
            """, {"c": code, "td": trade_date})

        finance = {
            "latest": fin or {},
            "quarters": fin_detail_rows,
            **finance_evidence,
        }

        # ─── 四、估值面 ───
        pe_percentile = _calc_percentile(pe_ttm, pe_history)
        pb_percentile = _calc_percentile(pb, pb_history)

        valuation = {
            "pe_ttm": pe_ttm,
            "pe_percentile": pe_percentile,
            "pb": pb,
            "pb_percentile": pb_percentile,
            "verdict": "偏高" if (pe_percentile is not None and pe_percentile > 70) else "偏低" if (pe_percentile is not None and pe_percentile < 30) else "合理" if pe_percentile is not None else None,
            "finance_pit_status": finance_evidence["pit_status"],
            "finance_manifest_hash": finance_evidence["manifest_hash"],
        }

        # ─── 五、技术面 ───
        kline_250 = _read_sql("""
            SELECT trade_date, open, close, high, low, volume, change_pct
            FROM sm_stock_kline WHERE stock_code = :c AND k_type=1 AND trade_date <= :td
            ORDER BY trade_date DESC LIMIT 260
        """, {"c": code, "td": trade_date})

        technical = _compute_technical(kline_250, price_val)

        # ─── 六、消息面 ───
        if strategy_context:
            notices, event_evidence = _pit_notice_bundle(
                code, trade_date, strategy_reader_decision, fact_cutoff_at
            )
            # The mutable flash-news table has no received/revision history and
            # is therefore display-only until it adopts the shared PIT store.
            news = []
        else:
            notices = _read_sql("""
                SELECT notice_date, title, column_name, detail_url
                FROM si_notice_eastmoney WHERE stock_code = :c AND notice_date <= :td
                ORDER BY notice_date DESC LIMIT 10
            """, {"c": code, "td": trade_date})
            news = _read_sql("""
                SELECT publish_time, title, source
                FROM st_news_flash
                WHERE stocks LIKE :kw
                  AND publish_time < DATE_ADD(:td, INTERVAL 1 DAY)
                ORDER BY publish_time DESC LIMIT 10
            """, {"kw": f"%{code}%", "td": trade_date})
            event_evidence = {
                "event_pit_status": "LEGACY_UNVERIFIED",
                "event_pit_reason": "DISPLAY_ONLY_MUTABLE_EVENT_CACHE",
                "event_manifest_hash": "",
                "event_revision_ids": [],
                "event_content_hashes": [],
            }

        news_module = {
            "notices": notices,
            "news": news,
            **event_evidence,
        }

        # ─── 七、热门排名 ───
        hot_rank_rows = [] if strategy_context else _read_sql("""
            SELECT fused_rank, east_rank, ths_rank, total_score
            FROM st_hot_rank_fused
            WHERE stock_code = :c AND snapshot_date <= :td
            ORDER BY snapshot_date DESC LIMIT 1
        """, {"c": code, "td": trade_date})
        hot_rank = hot_rank_rows[0] if hot_rank_rows else {}

        # ─── 八、解禁信息 ───
        lifting_rows = [] if strategy_context else _read_sql("""
            SELECT lift_date, volume, amount, ratio
            FROM st_stock_lifting_last_month
            WHERE stock_code = :c AND lift_date >= :td
            ORDER BY lift_date ASC LIMIT 3
        """, {"c": code, "td": trade_date})
        lifting = {
            "has_lifting_soon": (
                None if strategy_context else len(lifting_rows) > 0
            ),
            "records": lifting_rows,
            "pit_status": (
                "DATA_BLOCKED" if strategy_context else "LEGACY_UNVERIFIED"
            ),
        }
        if lifting_rows:
            lifting["lift_date"] = str(lifting_rows[0].get("lift_date", ""))
            lifting["amount"] = lifting_rows[0].get("amount")
            lifting["ratio"] = lifting_rows[0].get("ratio")

        # ─── 九、扫雷信息 ───
        mine_rows = [] if strategy_context else _read_sql("""
            SELECT score, f_type, s_type, t_type, reason
            FROM st_mine_clearance_tdx WHERE stock_code = :c
        """, {"c": code})
        mine_clearance = mine_rows[0] if mine_rows else {}
        if strategy_context:
            mine_clearance = {
                "pit_status": "DATA_BLOCKED",
                "pit_reason": "LEGACY_CURRENT_MINE_INPUT_IGNORED",
            }

        # ─── 十、持仓信息 ───
        holding_rows = _read_sql(
            "SELECT shares, cost_price FROM st_user_portfolio WHERE stock_code = :c AND shares > 0",
            {"c": code}
        )
        holding = holding_rows[0] if holding_rows else None

        # ─── 十一、市场情绪（全市场涨跌统计）───
        market_mood = {}
        try:
            # 最新交易日全市场涨跌统计
            mood_rows = _read_sql("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as up_count,
                    SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as down_count,
                    SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) as flat_count,
                    SUM(CASE WHEN change_pct >= 9.9 THEN 1 ELSE 0 END) as limit_up,
                    SUM(CASE WHEN change_pct <= -9.9 THEN 1 ELSE 0 END) as limit_down,
                    AVG(change_pct) as avg_change
                FROM sm_stock_kline
                WHERE trade_date = :td AND k_type = 1
            """, {"td": trade_date})
            if mood_rows:
                m = mood_rows[0]
                total = int(m.get("total") or 0)
                up = int(m.get("up_count") or 0)
                down = int(m.get("down_count") or 0)
                market_mood = {
                    "total": total,
                    "up_count": up,
                    "down_count": down,
                    "flat_count": int(m.get("flat_count") or 0),
                    "limit_up": int(m.get("limit_up") or 0),
                    "limit_down": int(m.get("limit_down") or 0),
                    "avg_change": round(float(m.get("avg_change") or 0), 2),
                    "up_ratio": round(up / max(total, 1), 3),
                }

            # 近3日每日涨跌比（趋势判断）
            recent_mood = _read_sql("""
                SELECT trade_date,
                    SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as up_ratio,
                    AVG(change_pct) as avg_chg
                FROM sm_stock_kline
                WHERE k_type = 1 AND trade_date <= :td
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 3
            """, {"td": trade_date})
            if recent_mood:
                market_mood["recent_days"] = [
                    {"date": str(r["trade_date"]), "up_ratio": round(float(r["up_ratio"] or 0), 3), "avg_chg": round(float(r["avg_chg"] or 0), 2)}
                    for r in reversed(recent_mood)
                ]
        except Exception:
            pass

        # ─── 最新新闻时间 ───
        last_news_time = None
        if news:
            last_news_time = news[0].get("publish_time")
        elif notices:
            last_news_time = (
                notices[0].get("event_published_at")
                or str(notices[0].get("notice_date", ""))
            )

        return {
            "stock_code": code,
            "short_name": basic.get("short_name"),
            "industry": industry,
            "concepts": concepts,
            "exchange": basic.get("exchange"),
            "trade_date": trade_date,
            "last_news_time": last_news_time,
            "basic": basic,
            "market": market,
            "capital": capital,
            "finance": finance,
            "valuation": valuation,
            "technical": technical,
            "news": news_module,
            "holder": holder,
            "hot_rank": hot_rank,
            "lifting": lifting,
            "mine_clearance": mine_clearance,
            "holding": holding,
            "market_mood": market_mood,
            "strategy_reference_evidence": strategy_reference_evidence,
        }

    def load_light_data(
        self,
        stock_code: str,
        trade_date: str | None = None,
        use_realtime: bool | None = None,
        *,
        strategy_context: bool = False,
        decision_at: datetime | str | None = None,
        fact_cutoff_at: datetime | str | None = None,
    ) -> dict:
        """
        加载精简数据（K线+资金+基础财务+估值）

        用于批量筛选场景，减少数据库查询量。
        """
        code = stock_code.strip().zfill(6)
        _ = use_realtime  # 轻量模式暂不使用实时快照，保留参数以统一接口。

        # 获取最新交易日
        trade_date = _latest_kline_trade_date(trade_date)
        strategy_reader_decision = (
            decision_at
            if (not strategy_context or fact_cutoff_at is not None)
            else None
        )

        # 基本信息
        basic_rows = _read_sql(
            "SELECT stock_code, short_name FROM si_all_code WHERE stock_code = :c",
            {"c": code}
        )
        if not basic_rows:
            raise ValueError(f"股票 {code} 不存在")
        basic = basic_rows[0]

        # 行情
        kline_rows = _read_sql(
            "SELECT close AS price, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close "
            "FROM sm_stock_kline WHERE stock_code = :c AND trade_date = :td AND k_type=1",
            {"c": code, "td": trade_date}
        )
        quote = kline_rows[0] if kline_rows else {}
        price_val = float(quote.get("price") or 0)

        # 资金流向（数据库存储单位：元，转换为万元供评分使用）
        flow_td_rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily WHERE trade_date <= :td",
            {"td": trade_date},
        )
        flow_trade_date = str(flow_td_rows[0]["d"])[:10] if flow_td_rows and flow_td_rows[0].get("d") else trade_date
        flow_rows = _read_sql(
            "SELECT main_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow "
            "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :td",
            {"c": code, "td": flow_trade_date}
        )
        if flow_rows:
            flow_today = {
                "main_net_inflow": float(flow_rows[0].get("main_net_inflow") or 0) / 10000,
                "lg_net_inflow": float(flow_rows[0].get("lg_net_inflow") or 0) / 10000,
                "mid_net_inflow": float(flow_rows[0].get("mid_net_inflow") or 0) / 10000,
                "sm_net_inflow": float(flow_rows[0].get("sm_net_inflow") or 0) / 10000,
            }
        else:
            flow_today = {}

        # 财务
        if strategy_context:
            fin, _rows, pe_history, pb_history, finance_evidence = (
                _pit_finance_bundle(
                    code, trade_date, strategy_reader_decision, fact_cutoff_at
                )
            )
        else:
            fin_rows = _read_sql("""
                SELECT basic_eps, net_asset_ps, roe_wtd, gross_margin, asset_liab_ratio,
                       total_rev_yoy_gr, net_profit_yoy_gr, report_date
                FROM si_stock_finance WHERE stock_code = :c AND report_date <= :td
                ORDER BY report_date DESC LIMIT 1
            """, {"c": code, "td": trade_date})
            fin = fin_rows[0] if fin_rows else {}
            pe_history, pb_history = _load_historical_valuation_samples(
                code, trade_date
            )
            finance_evidence = {
                "pit_status": "LEGACY_UNVERIFIED",
                "pit_reason": "DISPLAY_ONLY_MUTABLE_FINANCE_CACHE",
                "manifest_hash": "",
                "revision_ids": [],
                "content_hashes": [],
            }

        # 估值
        eps = float(fin.get("basic_eps") or 0)
        bvps = float(fin.get("net_asset_ps") or 0)
        pe_ttm = round(price_val / eps, 2) if eps and eps > 0 and price_val else None
        pb = round(price_val / bvps, 2) if bvps and bvps > 0 and price_val else None
        pe_percentile = _calc_percentile(pe_ttm, pe_history)
        pb_percentile = _calc_percentile(pb, pb_history)
        verdict = "偏高" if (pe_percentile is not None and pe_percentile > 70) else "偏低" if (pe_percentile is not None and pe_percentile < 30) else "合理" if pe_percentile is not None else None
        if strategy_context:
            notices, event_evidence = _pit_notice_bundle(
                code, trade_date, strategy_reader_decision, fact_cutoff_at
            )
        else:
            notices = []
            event_evidence = {
                "event_pit_status": "LEGACY_UNVERIFIED",
                "event_pit_reason": "LIGHT_DISPLAY_DOES_NOT_LOAD_EVENTS",
                "event_manifest_hash": "",
                "event_revision_ids": [],
                "event_content_hashes": [],
            }

        return {
            "stock_code": code,
            "short_name": basic.get("short_name"),
            "trade_date": trade_date,
            "market": {"price": quote.get("price"), "change_pct": quote.get("change_pct")},
            "capital": {"today": flow_today},
            "finance": {"latest": fin, **finance_evidence},
            "news": {"notices": notices, "news": [], **event_evidence},
            "valuation": {
                "pe_ttm": pe_ttm,
                "pe_percentile": pe_percentile,
                "pb": pb,
                "pb_percentile": pb_percentile,
                "verdict": verdict,
                "finance_pit_status": finance_evidence["pit_status"],
                "finance_manifest_hash": finance_evidence["manifest_hash"],
            },
        }
