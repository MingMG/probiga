# -*- coding: utf-8 -*-
"""Broad-market ETF primary-market flow monitor.

The public daily fact available to us is the change in outstanding ETF
shares.  It says nothing about the identity of the creator/redeemer, so this
module deliberately keeps the fund-flow fact separate from the inferred
"stabilising capital" pattern shown by the UI.
"""
from __future__ import annotations

import io
import logging
import math
import statistics
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from xml.etree import ElementTree

import requests
from fastapi import APIRouter, Query

from server.api.routers._engine import get_engine
from server.common.sql_reader import read_sql_rows

logger = logging.getLogger(__name__)
router = APIRouter()

SSE_SHARE_URL = "https://query.sse.com.cn/commonQuery.do"
SZSE_SHARE_URL = "https://www.szse.cn/api/report/ShowReport"
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

SSE_SOURCE_URL = "https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml"
SZSE_SOURCE_URL = "https://www.szse.cn/market/fund/volume/etf/index.html"
HUJIN_SOURCE_URL = "https://www.huijin-inv.cn/huijin-inv/SC20252/2025-04/1002841.shtml"

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 ProBigA ETF Flow Monitor",
}
SSE_HEADERS = {**HTTP_HEADERS, "Referer": "https://www.sse.com.cn/"}
SZSE_HEADERS = {
    **HTTP_HEADERS,
    "Referer": SZSE_SOURCE_URL,
}


@dataclass(frozen=True)
class CoreETF:
    code: str
    short_name: str
    benchmark: str
    exchange: str


# A deliberately explicit observation pool.  These four products have complete
# locally validated price history in this deployment, so the page does not
# silently substitute a flaky third-party quote for the monetary estimate.
# It is an observation pool, not a claim to cover the whole ETF market.
CORE_ETFS: tuple[CoreETF, ...] = (
    CoreETF("510300", "沪深300ETF华泰柏瑞", "沪深300", "sh"),
    CoreETF("510500", "中证500ETF南方", "中证500", "sh"),
    CoreETF("512100", "中证1000ETF南方", "中证1000", "sh"),
    CoreETF("159915", "创业板ETF易方达", "创业板", "sz"),
)

CORE_BY_CODE = {item.code: item for item in CORE_ETFS}
CORE_CODES = tuple(CORE_BY_CODE)
CORE_CODE_PARAMS = {f"code_{index}": code for index, code in enumerate(CORE_CODES)}
CORE_CODE_BINDINGS = ", ".join(f":code_{index}" for index in range(len(CORE_CODES)))

_CACHE_TTL_SECONDS = 15 * 60
_MIN_RELIABLE_COVERAGE_PCT = 65.0
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _http_get(
    url: str,
    *,
    params: dict[str, str],
    headers: dict[str, str],
    timeout: int | tuple[int, int],
):
    return requests.get(url, params=params, headers=headers, timeout=timeout)


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _iso_date(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value or "")[:10]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except (TypeError, ValueError):
        return date.today()


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(key)
        if entry and now - entry[0] < _CACHE_TTL_SECONDS:
            return entry[1]
        _cache.pop(key, None)
    return None


def _cache_set(key: str, value: dict[str, Any]) -> None:
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)
        if len(_cache) > 24:
            oldest_key = min(_cache, key=lambda item: _cache[item][0])
            _cache.pop(oldest_key, None)


def _trading_dates(requested_date: str, limit: int) -> list[str]:
    rows = read_sql_rows(
        get_engine(),
        """
        SELECT DISTINCT trade_date
        FROM sm_etf_kline
        WHERE trade_date <= :trade_date
          AND k_type = 1
          AND adjust_type = 0
        ORDER BY trade_date DESC
        LIMIT :row_limit
        """,
        {"trade_date": requested_date, "row_limit": limit},
        context="broad_etf_flow_trade_dates",
        stringify_datetime=True,
    )
    dates = [_iso_date(row.get("trade_date")) for row in rows]
    dates = [item for item in dates if item]
    if dates:
        return dates

    # Database-free test/dev fallback.  Exchange calls will discard holidays.
    cursor = _parse_date(requested_date)
    fallback: list[str] = []
    while len(fallback) < limit:
        if cursor.weekday() < 5:
            fallback.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    return fallback


def _fetch_sse_share_day(trade_date: str) -> tuple[str, dict[str, dict[str, Any]]]:
    params = {
        "isPagination": "true",
        "pageHelp.pageSize": "10000",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "1",
        "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
        "STAT_DATE": trade_date,
    }
    response = _http_get(
        SSE_SHARE_URL,
        params=params,
        headers=SSE_HEADERS,
        timeout=(5, 12),
    )
    response.raise_for_status()
    result: dict[str, dict[str, Any]] = {}
    for row in response.json().get("result") or []:
        code = str(row.get("SEC_CODE") or "").zfill(6)
        if code not in CORE_BY_CODE:
            continue
        shares_10k = _safe_float(row.get("TOT_VOL"))
        if shares_10k is None:
            continue
        actual_date = _iso_date(row.get("STAT_DATE")) or trade_date
        result[code] = {
            "trade_date": actual_date,
            "etf_code": code,
            "fund_share": shares_10k * 10_000,
            "source": "上交所 ETF 规模",
        }
    return trade_date, result


def _xlsx_inline_rows(content: bytes) -> list[list[str]]:
    """Read simple inline-string XLSX rows without an openpyxl dependency."""
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(io.BytesIO(content)) as workbook:
        sheet_name = next(
            name
            for name in workbook.namelist()
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        )
        root = ElementTree.fromstring(workbook.read(sheet_name))

    result: list[list[str]] = []
    for row in root.findall(f".//{namespace}sheetData/{namespace}row"):
        cells: list[str] = []
        for cell in row.findall(f"{namespace}c"):
            inline = cell.find(f"{namespace}is/{namespace}t")
            value = cell.find(f"{namespace}v")
            cells.append((inline.text if inline is not None else value.text if value is not None else "") or "")
        result.append(cells)
    return result


def _fetch_szse_share_range(start_date: str, end_date: str) -> dict[str, dict[str, dict[str, Any]]]:
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "scsj_fund_jjgm",
        "TABKEY": "tab1",
        "txtStart": start_date,
        "txtEnd": end_date,
        "jjlb": "ETF",
    }
    response = _http_get(
        SZSE_SHARE_URL,
        params=params,
        headers=SZSE_HEADERS,
        timeout=(5, 30),
    )
    response.raise_for_status()
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for cells in _xlsx_inline_rows(response.content)[1:]:
        if len(cells) < 4:
            continue
        trade_date, code, _name, raw_share = cells[:4]
        code = str(code).zfill(6)
        if code not in CORE_BY_CODE:
            continue
        fund_share = _safe_float(str(raw_share).replace(",", ""))
        if fund_share is None:
            continue
        output.setdefault(_iso_date(trade_date), {})[code] = {
            "trade_date": _iso_date(trade_date),
            "etf_code": code,
            "fund_share": fund_share,
            "source": "深交所 ETF 基金规模",
        }
    return output


def _local_price_history(start_date: str, end_date: str) -> dict[str, dict[str, dict[str, Any]]]:
    rows = read_sql_rows(
        get_engine(),
        f"""
        SELECT etf_code, short_name, trade_date, close, pre_close,
               change_pct, amount, quality_status
        FROM sm_etf_kline
        WHERE etf_code IN ({CORE_CODE_BINDINGS})
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND k_type = 1
          AND adjust_type = 0
        ORDER BY trade_date, etf_code
        """,
        {**CORE_CODE_PARAMS, "start_date": start_date, "end_date": end_date},
        context="broad_etf_flow_prices",
        stringify_datetime=True,
    )
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        code = str(row.get("etf_code") or "").zfill(6)
        row_date = _iso_date(row.get("trade_date"))
        output.setdefault(row_date, {})[code] = {
            "close": _safe_float(row.get("close")),
            "pre_close": _safe_float(row.get("pre_close")),
            "change_pct": _safe_float(row.get("change_pct")),
            "amount": _safe_float(row.get("amount")),
            "price_source": "本地已校验 ETF 日线",
            "quality_status": str(row.get("quality_status") or "local"),
        }
    return output


def _market_id(code: str) -> int:
    return 1 if code.startswith(("5", "6")) else 0


def _fetch_eastmoney_price_history(code: str, start_date: str, end_date: str) -> tuple[str, dict[str, dict[str, Any]]]:
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "0",
        "beg": start_date.replace("-", ""),
        "end": end_date.replace("-", ""),
        "secid": f"{_market_id(code)}.{code}",
    }
    response = _http_get(
        EASTMONEY_KLINE_URL,
        params=params,
        headers=HTTP_HEADERS,
        timeout=(5, 12),
    )
    response.raise_for_status()
    rows = ((response.json().get("data") or {}).get("klines") or [])
    output: dict[str, dict[str, Any]] = {}
    previous_close: float | None = None
    for raw in rows:
        fields = str(raw).split(",")
        if len(fields) < 11:
            continue
        row_date = _iso_date(fields[0])
        close = _safe_float(fields[2])
        output[row_date] = {
            "close": close,
            "pre_close": previous_close,
            "change_pct": _safe_float(fields[8]),
            "amount": _safe_float(fields[6]),
            "price_source": "东方财富公开日线",
            "quality_status": "public_fallback",
        }
        previous_close = close
    return code, output


def _merge_nested(target: dict[str, dict[str, dict[str, Any]]], source: dict[str, dict[str, dict[str, Any]]]) -> None:
    for row_date, code_rows in source.items():
        target.setdefault(row_date, {}).update(code_rows)


def _collect_inputs(trade_dates: list[str]) -> tuple[
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, dict[str, Any]]],
    list[str],
]:
    chronological = sorted(trade_dates)
    start_date, end_date = chronological[0], chronological[-1]
    prices = _local_price_history(start_date, end_date)
    local_codes = {
        code
        for date_rows in prices.values()
        for code in date_rows
    }
    missing_price_codes = [code for code in CORE_CODES if code not in local_codes]
    shares: dict[str, dict[str, dict[str, Any]]] = {}
    warnings: list[str] = []

    worker_count = min(16, max(2, len(trade_dates) + len(missing_price_codes) + 1))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="broad-etf-flow") as pool:
        futures: dict[Any, tuple[str, str]] = {}
        # Start the slower range request first so it is never queued behind a
        # full batch of per-day SSE requests.  This keeps the first page load
        # inside the frontend's 45 second timeout even when SZSE is sluggish.
        szse_future = pool.submit(_fetch_szse_share_range, start_date, end_date)
        futures[szse_future] = ("szse", "range")
        for row_date in trade_dates:
            future = pool.submit(_fetch_sse_share_day, row_date)
            futures[future] = ("sse", row_date)
        for code in missing_price_codes:
            future = pool.submit(_fetch_eastmoney_price_history, code, start_date, end_date)
            futures[future] = ("price", code)

        for future in as_completed(futures):
            kind, label = futures[future]
            try:
                value = future.result()
                if kind == "sse":
                    row_date, code_rows = value
                    shares.setdefault(row_date, {}).update(code_rows)
                elif kind == "szse":
                    _merge_nested(shares, value)
                else:
                    code, code_rows = value
                    for row_date, price_row in code_rows.items():
                        prices.setdefault(row_date, {})[code] = price_row
            except Exception as exc:
                logger.warning("Broad ETF flow source %s %s failed: %s", kind, label, exc)
                warnings.append(f"{kind}:{label} 数据暂不可用")

    return shares, prices, warnings


def _build_flow_rows(
    trade_dates: list[str],
    shares: dict[str, dict[str, dict[str, Any]]],
    prices: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    chronological = sorted(trade_dates)
    output: list[dict[str, Any]] = []
    for index, row_date in enumerate(chronological):
        if index == 0:
            continue
        previous_date = chronological[index - 1]
        for meta in CORE_ETFS:
            current_share = (shares.get(row_date) or {}).get(meta.code)
            previous_share = (shares.get(previous_date) or {}).get(meta.code)
            price = (prices.get(row_date) or {}).get(meta.code) or {}
            previous_price = (prices.get(previous_date) or {}).get(meta.code) or {}
            if not current_share or not previous_share:
                continue
            current_value = _safe_float(current_share.get("fund_share"))
            previous_value = _safe_float(previous_share.get("fund_share"))
            multiplier = _safe_float(price.get("pre_close")) or _safe_float(previous_price.get("close"))
            if current_value is None or previous_value in (None, 0) or multiplier is None:
                continue
            share_change = current_value - previous_value
            share_change_pct = share_change / previous_value * 100
            quality_status = str(price.get("quality_status") or "public")
            net_amount: float | None = share_change * multiplier
            if abs(share_change_pct) >= 35:
                # A split/merge can look like a huge subscription.  Keep the
                # share fact visible but exclude it from monetary aggregation.
                net_amount = None
                quality_status = "corporate_action_suspected"
            output.append({
                "trade_date": row_date,
                "etf_code": meta.code,
                "short_name": meta.short_name,
                "benchmark": meta.benchmark,
                "exchange": meta.exchange,
                "net_amount": net_amount,
                "share_change": share_change,
                "share_change_pct": share_change_pct,
                "fund_share": current_value,
                "price": _safe_float(price.get("close")),
                "change_pct": _safe_float(price.get("change_pct")),
                "amount": _safe_float(price.get("amount")),
                "source": str(current_share.get("source") or "交易所公开份额"),
                "flow_method": "份额变化 × 前一交易日收盘价",
                "quality_status": quality_status,
            })
    return output


def _sum(rows: list[dict[str, Any]], field: str = "net_amount") -> float:
    return float(sum(float(row[field]) for row in rows if row.get(field) is not None))


def _window_sum(history: list[dict[str, Any]], count: int) -> float | None:
    window = history[-count:]
    if len(window) < count:
        return None
    if any(
        float(row.get("coverage_pct") or 0) < _MIN_RELIABLE_COVERAGE_PCT
        or row.get("net_amount") is None
        for row in window
    ):
        return None
    return float(sum(float(row["net_amount"]) for row in window))


def _percentile(history_values: list[float], value: float) -> float | None:
    if not history_values:
        return None
    return (1 + sum(1 for item in history_values if item <= value)) / (len(history_values) + 1)


def _money_text(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value / 100_000_000:.2f}亿元"


def _signal(
    history: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    expected: int,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    net_1d = float(history[-1].get("net_amount") or 0) if history else 0.0
    net_3d = _window_sum(history, 3)
    net_5d = _window_sum(history, 5)
    net_20d = _window_sum(history, 20)
    available = len(selected_rows)
    coverage_pct = available / expected * 100 if expected else 0.0
    prior_values = [
        float(row["net_amount"])
        for row in history[:-1]
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
        and row.get("net_amount") is not None
    ]
    prior_absolute = [abs(item) for item in prior_values]
    absolute_base = statistics.median(prior_absolute) if prior_absolute else 0.0
    noise = max(2_000_000_000.0, 0.5 * absolute_base)
    strong = max(20_000_000_000.0, 2.5 * absolute_base)
    material = max(5_000_000_000.0, 1.5 * absolute_base)
    percentile = _percentile(prior_absolute, abs(net_1d))

    valid_benchmarks: dict[str, float] = {}
    for row in selected_rows:
        if row.get("net_amount") is None:
            continue
        key = str(row.get("benchmark") or "其他")
        valid_benchmarks[key] = valid_benchmarks.get(key, 0.0) + float(row["net_amount"])
    positive_benchmarks = sum(1 for value in valid_benchmarks.values() if value > 500_000_000)
    negative_benchmarks = sum(1 for value in valid_benchmarks.values() if value < -500_000_000)

    valid_returns = [float(row["change_pct"]) for row in selected_rows if row.get("change_pct") is not None]
    median_return = statistics.median(valid_returns) if valid_returns else None
    reliable_recent_5 = [
        row
        for row in history[-5:]
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
        and row.get("net_amount") is not None
    ]
    positive_days_5 = sum(1 for row in reliable_recent_5 if float(row["net_amount"]) > noise)
    persistent = (
        net_5d is not None
        and len(history[-5:]) >= 4
        and len(reliable_recent_5) == len(history[-5:])
        and positive_days_5 >= 3
        and net_5d >= max(10_000_000_000.0, 2 * absolute_base)
    )
    broad = positive_benchmarks >= 3 and negative_benchmarks == 0
    counter_cyclical = median_return is not None and median_return <= -0.5 and net_1d > noise

    if coverage_pct < 65:
        signal = "数据不足，暂不判断"
        tone = "unknown"
    elif net_1d >= strong:
        signal = "较强护盘型资金线索" if broad or counter_cyclical else "当日强净申购"
        tone = "inflow"
    elif net_1d >= material or persistent:
        signal = "一定护盘型资金线索" if broad or counter_cyclical else "当日净申购"
        tone = "inflow"
    elif net_1d > noise:
        signal = "资金入场线索较弱"
        tone = "inflow"
    elif net_1d <= -strong:
        signal = "宽基强净赎回压力"
        tone = "outflow"
    elif net_1d < -noise:
        signal = "宽基净赎回压力"
        tone = "outflow"
    else:
        signal = "资金大致平衡，线索不足"
        tone = "neutral"

    history_days = sum(
        1
        for row in history[:-1]
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
        and row.get("net_amount") is not None
    )
    if coverage_pct >= 90 and history_days >= 12:
        confidence_label = "较高"
    elif coverage_pct >= 75 and history_days >= 8:
        confidence_label = "中等"
    elif coverage_pct >= 65:
        confidence_label = "较低"
    else:
        confidence_label = "不可用"
    confidence = round(min(100.0, coverage_pct * 0.7 + min(history_days, 12) / 12 * 30), 1)

    evidence: list[dict[str, str]] = []
    direction = "估算净申购" if net_1d >= 0 else "估算净赎回"
    evidence.append({
        "kind": "positive" if net_1d > noise else "negative" if net_1d < -noise else "neutral",
        "title": f"当日{direction} {abs(net_1d) / 100_000_000:.2f}亿元",
        "detail": f"核心观察池覆盖 {available}/{expected} 只；金额未按缺失覆盖率放大。",
    })
    if net_5d is None:
        evidence.append({
            "kind": "warning",
            "title": "近5日累计暂不计算",
            "detail": "窗口内存在覆盖低于65%的交易日，缺失 ETF 不会按0元计入累计。",
        })
    else:
        evidence.append({
            "kind": "positive" if net_5d > noise else "negative" if net_5d < -noise else "neutral",
            "title": f"近5日累计 {_money_text(net_5d)}",
            "detail": f"其中 {positive_days_5}/{min(5, len(history))} 日出现实质净申购。",
        })
    if valid_benchmarks:
        evidence.append({
            "kind": "positive" if broad else "neutral",
            "title": f"{positive_benchmarks}/{len(valid_benchmarks)} 类宽基同步流入",
            "detail": "同步性越高，稳定型配置线索越强；单一指数集中流入的解释力较弱。",
        })
    if counter_cyclical:
        evidence.append({
            "kind": "positive",
            "title": "下跌日出现逆势承接",
            "detail": f"观察池 ETF 涨跌幅中位数 {median_return:.2f}%，同时份额净增加。",
        })
    elif median_return is not None and median_return >= 0.5 and net_1d > noise:
        evidence.append({
            "kind": "neutral",
            "title": "资金与上涨同向",
            "detail": "可能包含趋势配置、申购套利或做市库存调整，护盘解释应降权。",
        })
    evidence.append({
        "kind": "warning",
        "title": "国家队身份未确认",
        "detail": (
            "ETF 总份额变化不披露申购、赎回主体。净赎回只能说明基金份额减少，"
            "不能据此断言国家队正在出货。"
        ),
    })

    return {
        "signal": signal,
        "signal_tone": tone,
        "confidence": confidence,
        "confidence_label": confidence_label,
        "identity_status": "未确认",
        "net_1d": net_1d,
        "net_3d": net_3d,
        "net_5d": net_5d,
        "net_20d": net_20d,
        "positive_count": sum(1 for row in selected_rows if (row.get("net_amount") or 0) > 0),
        "total_count": available,
        "coverage_pct": round(coverage_pct, 1),
        "history_days": history_days,
        "recent_percentile": round(percentile * 100, 1) if percentile is not None else None,
    }, evidence


def _build_payload(requested_date: str, days: int) -> dict[str, Any]:
    # One extra date is required to calculate the first daily share change.
    trade_dates_desc = _trading_dates(requested_date, days + 1)
    if len(trade_dates_desc) < 2:
        raise RuntimeError("没有足够交易日计算 ETF 份额变化")
    trade_dates = sorted(trade_dates_desc)
    shares, prices, warnings = _collect_inputs(trade_dates)
    flow_rows = _build_flow_rows(trade_dates, shares, prices)

    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in flow_rows:
        by_date.setdefault(str(row["trade_date"]), []).append(row)

    history: list[dict[str, Any]] = []
    for row_date in sorted(by_date):
        date_rows = [row for row in by_date[row_date] if row.get("net_amount") is not None]
        history.append({
            "trade_date": row_date,
            "net_amount": _sum(date_rows),
            "coverage_pct": round(len(date_rows) / len(CORE_ETFS) * 100, 1),
            "available": len(date_rows),
        })
    if not history:
        raise RuntimeError("交易所 ETF 份额数据暂不可用")

    # Prefer the requested/latest date when coverage clears the minimum gate;
    # otherwise fall back to the most recent usable date and expose the lag.
    usable = [
        row
        for row in history
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
    ]
    selected_history = (usable or history)[-1]
    trade_date = str(selected_history["trade_date"])
    history = [row for row in history if str(row["trade_date"]) <= trade_date][-days:]
    cumulative = 0.0
    cumulative_reliable = True
    for row in history:
        if float(row.get("coverage_pct") or 0) < _MIN_RELIABLE_COVERAGE_PCT:
            row["observed_net_amount"] = row.get("net_amount")
            row["net_amount"] = None
            cumulative_reliable = False
        if row.get("net_amount") is not None and cumulative_reliable:
            cumulative += float(row["net_amount"])
            row["cumulative_amount"] = cumulative
        else:
            row["cumulative_amount"] = None

    selected_rows_all = [row for row in flow_rows if row["trade_date"] == trade_date]
    selected_rows = [row for row in selected_rows_all if row.get("net_amount") is not None]
    summary, evidence = _signal(history, selected_rows, len(CORE_ETFS))

    benchmarks: list[dict[str, Any]] = []
    benchmark_names = sorted({item.benchmark for item in CORE_ETFS})
    for benchmark in benchmark_names:
        benchmark_history: list[tuple[str, list[dict[str, Any]]]] = []
        for item in history:
            row_date = str(item["trade_date"])
            rows = [
                row for row in flow_rows
                if row["trade_date"] == row_date
                and row["benchmark"] == benchmark
                and row.get("net_amount") is not None
            ]
            benchmark_history.append((row_date, rows))
        latest_rows = benchmark_history[-1][1] if benchmark_history else []
        def benchmark_window_sum(count: int) -> float | None:
            window = benchmark_history[-count:]
            if len(window) < count or any(not rows for _, rows in window):
                return None
            return _sum([row for _, rows in window for row in rows])

        benchmarks.append({
            "benchmark": benchmark,
            "net_1d": _sum(latest_rows) if latest_rows else None,
            "net_3d": benchmark_window_sum(3),
            "net_5d": benchmark_window_sum(5),
            "net_20d": benchmark_window_sum(20),
            "share_change": _sum(latest_rows, "share_change") if latest_rows else None,
            "positive_count": sum(1 for row in latest_rows if (row.get("net_amount") or 0) > 0),
            "total_count": len(latest_rows),
        })
    benchmarks.sort(
        key=lambda item: (
            item.get("net_1d") is not None,
            float(item.get("net_1d") or 0),
        ),
        reverse=True,
    )

    selected_codes = {str(row["etf_code"]) for row in selected_rows_all}
    missing = [
        {"etf_code": item.code, "short_name": item.short_name, "benchmark": item.benchmark}
        for item in CORE_ETFS
        if item.code not in selected_codes
    ]

    caveats = [
        "净申购估算 = ETF 总份额变化 × 前一交易日收盘价；它不是二级市场成交额。",
        "公开日频份额不能识别申购、赎回主体；国家队身份只有公告或专项持仓披露才能确认。",
        "净申购可能包含申购套利、做市库存调整和普通机构配置，不等于方向性看多。",
        "观察池覆盖主要核心宽基产品，不代表全市场所有同标的 ETF；缺失金额不会按覆盖率放大。",
    ]
    if warnings:
        caveats.append("部分数据源读取失败：" + "；".join(warnings[:6]))
    if any(row.get("observed_net_amount") is not None for row in history):
        caveats.append("覆盖低于65%的历史日已留空，不会把缺失 ETF 隐式按0元计入区间累计。")

    excluded_rows = [row for row in selected_rows_all if row.get("net_amount") is None]
    if excluded_rows:
        caveats.append("疑似份额拆分、合并等公司行为的记录保留在明细中，但不计入资金汇总。")

    if summary["coverage_pct"] < _MIN_RELIABLE_COVERAGE_PCT:
        status = "insufficient"
    elif any(summary.get(field) is None for field in ("net_3d", "net_5d", "net_20d")) or excluded_rows:
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "requested_date": requested_date,
        "trade_date": trade_date,
        "data_as_of": trade_date,
        "window_days": days,
        "scope_label": "4只核心宽基 ETF 观察池",
        "summary": summary,
        "history": history,
        "benchmarks": benchmarks,
        "etfs": sorted(
            selected_rows_all,
            key=lambda item: (
                item.get("net_amount") is not None,
                float(item.get("net_amount") or 0),
            ),
            reverse=True,
        ),
        "evidence": evidence,
        "caveats": caveats,
        "sources": [
            {"name": "上海证券交易所 ETF 规模", "url": SSE_SOURCE_URL, "note": "日终基金份额"},
            {"name": "深圳证券交易所 ETF 基金规模", "url": SZSE_SOURCE_URL, "note": "日度基金份额"},
            {"name": "中央汇金 2025 年增持公告", "url": HUJIN_SOURCE_URL, "note": "只用于身份确认边界，不用于推算当日金额"},
        ],
        "coverage": {
            "expected": len(CORE_ETFS),
            "available": len(selected_rows),
            "missing": missing,
            "excluded": [
                {
                    "etf_code": row.get("etf_code"),
                    "short_name": row.get("short_name"),
                    "quality_status": row.get("quality_status"),
                }
                for row in excluded_rows
            ],
        },
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


@router.get("/hot-data/broad-etf-flow")
def broad_etf_flow(
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
    days: int = Query(default=20, ge=5, le=20),
    refresh: bool = Query(default=False),
):
    requested = _parse_date(trade_date).isoformat()
    key = f"{requested}:{days}"
    if not refresh:
        cached = _cache_get(key)
        if cached is not None:
            return cached
    try:
        payload = _build_payload(requested, days)
    except Exception as exc:
        logger.exception("Broad ETF flow payload failed for %s", requested)
        return {
            "status": "error",
            "requested_date": requested,
            "trade_date": "",
            "data_as_of": "",
            "window_days": days,
            "summary": {
                "signal": "数据暂不可用",
                "signal_tone": "unknown",
                "confidence": 0,
                "confidence_label": "不可用",
                "identity_status": "未确认",
                "net_1d": None,
                "net_3d": None,
                "net_5d": None,
                "net_20d": None,
                "positive_count": 0,
                "total_count": 0,
                "coverage_pct": 0,
            },
            "history": [],
            "benchmarks": [],
            "etfs": [],
            "evidence": [{
                "kind": "warning",
                "title": "暂时无法读取交易所份额数据",
                "detail": "交易所或本地行情数据当前不可用，请稍后刷新。",
            }],
            "caveats": ["请稍后重试；页面不会用二级市场成交额替代 ETF 净申购。"],
            "sources": [],
            "coverage": {"expected": len(CORE_ETFS), "available": 0, "missing": []},
        }
    _cache_set(key, payload)
    return payload


__all__ = [
    "CORE_ETFS",
    "_build_flow_rows",
    "_signal",
    "_xlsx_inline_rows",
    "broad_etf_flow",
    "router",
]
