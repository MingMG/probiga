# -*- coding: utf-8 -*-
"""Broad-market ETF primary-market flow monitor.

The public daily fact available to us is the change in outstanding ETF
shares.  It says nothing about the identity of the creator/redeemer, so this
module deliberately keeps the fund-flow fact separate from the inferred
"stabilising capital" pattern shown by the UI.
"""
from __future__ import annotations

import io
import json
import logging
import math
import statistics
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal
from xml.etree import ElementTree

import requests
from fastapi import APIRouter, HTTPException, Query

from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import DAILY_CLOSE_READY_TIME, PRODUCTION_TIMEZONE
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
    "Accept-Encoding": "identity",
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


# Exchange-listed tracker pairs, verified against the SSE fund directory and
# SZSE's 创 series index guide. Each benchmark has a fixed comparison cohort.
CORE_ETFS: tuple[CoreETF, ...] = (
    CoreETF("510300", "沪深300ETF华泰柏瑞", "沪深300", "sh"),
    CoreETF("510500", "中证500ETF南方", "中证500", "sh"),
    CoreETF("512100", "中证1000ETF南方", "中证1000", "sh"),
    CoreETF("159915", "创业板ETF易方达", "创业板", "sz"),
    CoreETF("510310", "沪深300ETF易方达", "沪深300", "sh"),
    CoreETF("510510", "中证500ETF广发", "中证500", "sh"),
    CoreETF("560010", "中证1000ETF广发", "中证1000", "sh"),
    CoreETF("159952", "创业板ETF广发", "创业板", "sz"),
    CoreETF("510050", "上证50ETF华夏", "上证50", "sh"),
    CoreETF("510100", "上证50ETF易方达", "上证50", "sh"),
    CoreETF("563300", "中证2000ETF华泰柏瑞", "中证2000", "sh"),
    CoreETF("563200", "中证2000ETF富国", "中证2000", "sh"),
    CoreETF("588000", "科创50ETF华夏", "科创50", "sh"),
    CoreETF("588080", "科创50ETF易方达", "科创50", "sh"),
    CoreETF("563360", "中证A500ETF华泰柏瑞", "中证A500", "sh"),
    CoreETF("563800", "中证A500ETF广发", "中证A500", "sh"),
)

CORE_BY_CODE = {item.code: item for item in CORE_ETFS}
CORE_CODES = tuple(CORE_BY_CODE)
CORE_CODE_PARAMS = {f"code_{index}": code for index, code in enumerate(CORE_CODES)}
CORE_CODE_BINDINGS = ", ".join(f":code_{index}" for index in range(len(CORE_CODES)))
BENCHMARK_CODES = {
    benchmark: tuple(sorted(item.code for item in CORE_ETFS if item.benchmark == benchmark))
    for benchmark in sorted({item.benchmark for item in CORE_ETFS})
}
SCOPE_LABEL = f"{len(BENCHMARK_CODES)}个基准 · {len(CORE_ETFS)}只跟踪宽基 ETF"
# Up to 21 share dates and 16 quote histories fit in three waves at the
# existing 16-worker cap. Leave the longer SZSE range request its own budget.
_FAST_SOURCE_TIMEOUT = (4, 8)
_SOURCE_BATCH_SECONDS = 40.0
_SSE_BODY_LIMIT = 512 * 1024
_SZSE_BODY_LIMIT = 1024 * 1024
_SZSE_SHEET_LIMIT = 8 * 1024 * 1024
_PRICE_BODY_LIMIT = 64 * 1024
_SOURCE_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="broad-etf-flow")
_SOURCE_BATCH_SLOTS = threading.BoundedSemaphore(2)

_CACHE_TTL_SECONDS = 15 * 60
_MIN_RELIABLE_COVERAGE_PCT = 65.0
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _http_get(
    url: str,
    *,
    params: dict[str, str],
    headers: dict[str, str],
    timeout: tuple[int, int],
    max_bytes: int,
) -> bytes:
    deadline = time.monotonic() + sum(timeout)
    with requests.get(
        url, params=params, headers=headers, timeout=timeout,
        stream=True, allow_redirects=False,
    ) as response:
        response.raise_for_status()
        if response.is_redirect or response.headers.get("Content-Encoding", "identity").lower() != "identity":
            raise ValueError("ETF source response transport differs")
        if int(response.headers.get("Content-Length") or 0) > max_bytes:
            raise ValueError("ETF source body exceeds limit")
        body = bytearray()
        while time.monotonic() < deadline:
            chunk = response.raw.read1(min(16 * 1024, max_bytes + 1 - len(body)))
            if time.monotonic() >= deadline:
                break
            if not chunk:
                return bytes(body)
            body.extend(chunk)
            if len(body) > max_bytes:
                raise ValueError("ETF source body exceeds limit")
    raise TimeoutError("ETF source body deadline exceeded")


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
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="trade_date 必须为 YYYY-MM-DD") from exc
    if result.isoformat() != value:
        raise HTTPException(status_code=422, detail="trade_date 必须为 YYYY-MM-DD")
    return result


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
    now = datetime.now(PRODUCTION_TIMEZONE)
    last_elapsed = now.date() if now.time() >= DAILY_CLOSE_READY_TIME else now.date() - timedelta(days=1)
    rows = read_sql_rows(
        get_engine(),
        """
        SELECT DISTINCT trade_date
        FROM si_trade_calendar
        WHERE trade_date <= :trade_date
          AND trade_status = 1
        ORDER BY trade_date DESC
        LIMIT :row_limit
        """,
        {"trade_date": min(requested_date, last_elapsed.isoformat()), "row_limit": limit},
        context="broad_etf_flow_trade_dates",
        stringify_datetime=True,
    )
    return [_iso_date(row["trade_date"]) for row in rows]


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
    body = _http_get(
        SSE_SHARE_URL,
        params=params,
        headers=SSE_HEADERS,
        timeout=_FAST_SOURCE_TIMEOUT,
        max_bytes=_SSE_BODY_LIMIT,
    )
    observed_at = datetime.now(PRODUCTION_TIMEZONE).isoformat(timespec="seconds")
    result: dict[str, dict[str, Any]] = {}
    for row in json.loads(body).get("result") or []:
        code = str(row.get("SEC_CODE") or "").zfill(6)
        actual_date = _iso_date(row.get("STAT_DATE"))
        if code not in CORE_BY_CODE or CORE_BY_CODE[code].exchange != "sh" or actual_date != trade_date:
            continue
        shares_10k = _safe_float(row.get("TOT_VOL"))
        if shares_10k is None or shares_10k < 0:
            continue
        result[code] = {
            "trade_date": actual_date,
            "etf_code": code,
            "fund_share": shares_10k * 10_000,
            "source": "上交所 ETF 规模",
            "observed_at": observed_at,
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
        if workbook.getinfo(sheet_name).file_size > _SZSE_SHEET_LIMIT:
            raise ValueError("ETF share workbook sheet exceeds limit")
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
    body = _http_get(
        SZSE_SHARE_URL,
        params=params,
        headers=SZSE_HEADERS,
        timeout=(5, 30),
        max_bytes=_SZSE_BODY_LIMIT,
    )
    observed_at = datetime.now(PRODUCTION_TIMEZONE).isoformat(timespec="seconds")
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for cells in _xlsx_inline_rows(body)[1:]:
        if len(cells) < 4:
            continue
        trade_date, code, _name, raw_share = cells[:4]
        code = str(code).strip().zfill(6)
        actual_date = _iso_date(trade_date)
        if (code not in CORE_BY_CODE or CORE_BY_CODE[code].exchange != "sz"
                or not start_date <= actual_date <= end_date):
            continue
        fund_share = _safe_float(str(raw_share).replace(",", ""))
        if fund_share is None or fund_share < 0:
            continue
        output.setdefault(actual_date, {})[code] = {
            "trade_date": actual_date,
            "etf_code": code,
            "fund_share": fund_share,
            "source": "深交所 ETF 基金规模",
            "observed_at": observed_at,
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
            "price_source": "本地 ETF 日线",
            "quality_status": str(row.get("quality_status") or "unverified"),
        }
    return output


def _market_id(code: str) -> int:
    return 1 if code.startswith(("5", "6")) else 0


def _usable_price(row: dict[str, Any]) -> bool:
    return str(row.get("quality_status") or "").upper() in {
        "VALIDATED", "VERIFIED", "SOURCE_VERIFIED", "PUBLIC_FALLBACK",
    }


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
    body = _http_get(
        EASTMONEY_KLINE_URL,
        params=params,
        headers=HTTP_HEADERS,
        timeout=_FAST_SOURCE_TIMEOUT,
        max_bytes=_PRICE_BODY_LIMIT,
    )
    data = json.loads(body).get("data") or {}
    rows = data.get("klines") or []
    if rows and (str(data.get("code") or "") != code or str(data.get("market")) != str(_market_id(code))):
        raise ValueError("ETF 行情响应证券身份不符")
    output: dict[str, dict[str, Any]] = {}
    for raw in rows:
        fields = str(raw).split(",")
        if len(fields) < 11:
            continue
        row_date = _iso_date(fields[0])
        close = _safe_float(fields[2])
        if not start_date <= row_date <= end_date or close is None or close <= 0:
            continue
        if row_date in output:
            raise ValueError("ETF 行情响应交易日重复")
        output[row_date] = {
            "close": close,
            "change_pct": _safe_float(fields[8]),
            "amount": _safe_float(fields[6]),
            "price_source": "东方财富公开日线",
            "quality_status": "public_fallback",
        }
    return code, output


def _merge_nested(target: dict[str, dict[str, dict[str, Any]]], source: dict[str, dict[str, dict[str, Any]]]) -> None:
    for row_date, code_rows in source.items():
        target.setdefault(row_date, {}).update(code_rows)


def _collect_inputs(trade_dates: list[str], *, deadline: float) -> tuple[
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, dict[str, Any]]],
    list[str],
    Literal["FINISHED", "BUSY", "DEADLINE", "PARTIAL_SOURCE_FAILURE"],
]:
    chronological = sorted(trade_dates)
    start_date, end_date = chronological[0], chronological[-1]
    prices = _local_price_history(start_date, end_date)
    missing_price_codes = [code for code in CORE_CODES if any(
        not _usable_price(prices.get(day, {}).get(code, {})) for day in chronological
    )]
    shares: dict[str, dict[str, dict[str, Any]]] = {}
    warnings: list[str] = []
    source_status: Literal["FINISHED", "DEADLINE", "PARTIAL_SOURCE_FAILURE"] = "FINISHED"

    if time.monotonic() >= deadline:
        return shares, prices, ["ETF 数据源批次达到读取时限"], "DEADLINE"
    if not _SOURCE_BATCH_SLOTS.acquire(blocking=False):
        return shares, prices, ["ETF 数据源并发读取已满"], "BUSY"
    futures: dict[Any, tuple[str, str]] = {}
    try:
        # Start the slower range request first so it is never queued behind a
        # full batch of per-day SSE requests.  This keeps the first page load
        # inside the frontend's 45 second timeout even when SZSE is sluggish.
        szse_future = _SOURCE_EXECUTOR.submit(_fetch_szse_share_range, start_date, end_date)
        futures[szse_future] = ("szse", "range")
        for row_date in trade_dates:
            future = _SOURCE_EXECUTOR.submit(_fetch_sse_share_day, row_date)
            futures[future] = ("sse", row_date)
        for code in missing_price_codes:
            future = _SOURCE_EXECUTOR.submit(_fetch_eastmoney_price_history, code, start_date, end_date)
            futures[future] = ("price", code)

        for future in as_completed(futures, timeout=max(0.0, deadline - time.monotonic())):
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
                        date_prices = prices.setdefault(row_date, {})
                        if not _usable_price(date_prices.get(code, {})):
                            date_prices[code] = price_row
            except Exception as exc:
                logger.warning("Broad ETF flow source %s %s failed: %s", kind, label, exc)
                warnings.append(f"{kind}:{label} 数据暂不可用")
                source_status = "PARTIAL_SOURCE_FAILURE"
    except TimeoutError:
        warnings.append("ETF 数据源批次达到读取时限")
        source_status = "DEADLINE"
    finally:
        # Cancel this batch's queued work. Running calls keep their own
        # deadlines and stay within the shared executor's fixed worker cap.
        for future in futures:
            future.cancel()
        _SOURCE_BATCH_SLOTS.release()

    return shares, prices, warnings, source_status


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
            if current_value is None or current_value < 0 or previous_value is None or previous_value <= 0:
                continue
            usable_price = price if _usable_price(price) else {}
            multiplier = _safe_float(usable_price.get("pre_close"))
            if multiplier is None or multiplier <= 0:
                multiplier = _safe_float(previous_price.get("close")) if _usable_price(previous_price) else None
            share_change = current_value - previous_value
            share_change_pct = share_change / previous_value * 100
            quality_status = str(price.get("quality_status") or "public")
            net_amount = share_change * multiplier if multiplier is not None and multiplier > 0 else None
            if net_amount is None:
                quality_status = "missing_price_reference"
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
                "price": _safe_float(usable_price.get("close")),
                "change_pct": _safe_float(usable_price.get("change_pct")),
                "amount": _safe_float(usable_price.get("amount")),
                "source": str(current_share.get("source") or "交易所公开份额"),
                "share_observed_at": max(current_share.get("observed_at") or "",
                                         previous_share.get("observed_at") or "") or None,
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
        or row["etf_codes"] != window[-1]["etf_codes"]
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
    benchmark_amounts: dict[str, float],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    net_3d = _window_sum(history, 3)
    net_5d = _window_sum(history, 5)
    net_20d = _window_sum(history, 20)
    available = len(selected_rows)
    coverage_pct = available / expected * 100 if expected else 0.0
    net_1d = (_safe_float(history[-1].get("net_amount"))
              if history and coverage_pct >= _MIN_RELIABLE_COVERAGE_PCT else None)
    prior_values = [
        float(row["net_amount"])
        for row in history[:-1]
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
        and row.get("net_amount") is not None
        and row["etf_codes"] == history[-1]["etf_codes"]
    ]
    prior_absolute = [abs(item) for item in prior_values]
    absolute_base = statistics.median(prior_absolute) if prior_absolute else 0.0
    noise = max(2_000_000_000.0, 0.5 * absolute_base)
    strong = max(20_000_000_000.0, 2.5 * absolute_base)
    material = max(5_000_000_000.0, 1.5 * absolute_base)
    percentile = _percentile(prior_absolute, abs(net_1d)) if net_1d is not None else None

    positive_benchmarks = sum(1 for value in benchmark_amounts.values() if value > 500_000_000)
    negative_benchmarks = sum(1 for value in benchmark_amounts.values() if value < -500_000_000)

    valid_returns = [float(row["change_pct"]) for row in selected_rows if row.get("change_pct") is not None]
    median_return = statistics.median(valid_returns) if valid_returns else None
    reliable_recent_5 = [
        row
        for row in history[-5:]
        if float(row.get("coverage_pct") or 0) >= _MIN_RELIABLE_COVERAGE_PCT
        and row.get("net_amount") is not None
        and row["etf_codes"] == history[-1]["etf_codes"]
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
    counter_cyclical = (net_1d is not None and net_1d > noise
                       and median_return is not None and median_return <= -0.5)

    if net_1d is None:
        signal = "数据不足，暂不判断"
        tone = "unknown"
    elif net_1d >= strong:
        signal = "较强护盘型资金线索" if counter_cyclical else "当日强净申购"
        tone = "inflow"
    elif net_1d >= material or (net_1d > noise and persistent):
        signal = "一定护盘型资金线索" if counter_cyclical else "当日净申购"
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

    history_days = len(prior_values)
    if net_1d is None:
        confidence_label = "不可用"
    elif coverage_pct >= 90 and history_days >= 12:
        confidence_label = "较高"
    elif coverage_pct >= 75 and history_days >= 8:
        confidence_label = "中等"
    elif coverage_pct >= 65:
        confidence_label = "较低"
    else:
        confidence_label = "不可用"
    confidence = round(min(100.0, coverage_pct * 0.7 + min(history_days, 12) / 12 * 30), 1) if net_1d is not None else 0

    evidence: list[dict[str, str]] = []
    if net_1d is None:
        evidence.append({"kind": "warning", "title": "当日净申赎金额不可用",
                         "detail": f"可估算份额变动的产品仅 {available}/{expected} 只。"})
    else:
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
            "detail": "区间累计要求交易日完整、覆盖至少65%、产品样本一致。缺失 ETF 不会按0元计入累计。",
        })
    else:
        evidence.append({
            "kind": "positive" if net_5d > noise else "negative" if net_5d < -noise else "neutral",
            "title": f"近5日累计 {_money_text(net_5d)}",
            "detail": f"其中 {positive_days_5}/{min(5, len(history))} 日出现实质净申购。",
        })
    if benchmark_amounts:
        evidence.append({
            "kind": "positive" if broad else "neutral",
            "title": f"{positive_benchmarks}/{len(benchmark_amounts)} 类宽基同步流入",
            "detail": "同步性越高，稳定型配置线索越强；单一指数集中流入的解释力较弱。",
        })
    if counter_cyclical:
        evidence.append({
            "kind": "positive",
            "title": "下跌日出现逆势承接",
            "detail": f"观察池 ETF 涨跌幅中位数 {median_return:.2f}%，同时份额净增加。",
        })
    elif net_1d is not None and net_1d > noise and median_return is not None and median_return >= 0.5:
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
        "flow_pattern": "COUNTERCYCLICAL_CREATION" if counter_cyclical and tone == "inflow" else {
            "inflow": "NET_CREATION", "outflow": "NET_REDEMPTION", "neutral": "NO_CLEAR_PATTERN",
            "unknown": "UNKNOWN",
        }[tone],
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
    deadline = time.monotonic() + _SOURCE_BATCH_SECONDS
    # One extra date is required to calculate the first daily share change.
    trade_dates_desc = _trading_dates(requested_date, days + 1)
    if len(trade_dates_desc) < 2:
        raise RuntimeError("没有足够交易日计算 ETF 份额变化")
    trade_dates = sorted(trade_dates_desc)
    shares, prices, warnings, source_status = _collect_inputs(trade_dates, deadline=deadline)
    flow_rows = _build_flow_rows(trade_dates, shares, prices)

    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for row in flow_rows:
        by_date.setdefault(row["trade_date"], {})[row["etf_code"]] = row

    history: list[dict[str, Any]] = []
    for row_date in trade_dates[1:]:
        date_rows = [row for row in by_date.get(row_date, {}).values() if row.get("net_amount") is not None]
        history.append({
            "trade_date": row_date,
            "net_amount": _sum(date_rows) if date_rows else None,
            "coverage_pct": round(len(date_rows) / len(CORE_ETFS) * 100, 1),
            "available": len(date_rows),
            "etf_codes": sorted(row["etf_code"] for row in date_rows),
        })
    # The exchange calendar maps non-trading dates to the previous session.
    # Keep that exact session even when a source has not published its inputs.
    trade_date = trade_dates[-1]
    history = history[-days:]
    cumulative = 0.0
    cumulative_reliable = True
    for row in history:
        if float(row["coverage_pct"]) < _MIN_RELIABLE_COVERAGE_PCT:
            row["observed_net_amount"] = row.get("net_amount")
            row["net_amount"] = None
        if row["net_amount"] is None or row["etf_codes"] != history[0]["etf_codes"]:
            cumulative_reliable = False
        if cumulative_reliable:
            cumulative += float(row["net_amount"])
        row["cumulative_amount"] = cumulative if cumulative_reliable else None

    selected_rows_all = list(by_date.get(trade_date, {}).values())
    selected_rows = [row for row in selected_rows_all if row.get("net_amount") is not None]
    observed_at = max((row.get("share_observed_at") or "" for row in selected_rows_all), default="") or None
    activity_date = trade_dates[-1]
    activity = []
    for code, price in sorted(prices.get(activity_date, {}).items()):
        if code not in CORE_BY_CODE:
            continue
        usable = _usable_price(price)
        turnover = _safe_float(price.get("amount")) if usable else None
        activity.append({"etf_code": code, "turnover": turnover if turnover is not None and turnover >= 0 else None,
                         "change_pct": price.get("change_pct") if usable else None,
                         "source": price.get("price_source"),
                         "quality_status": price.get("quality_status") or "unverified"})

    benchmarks: list[dict[str, Any]] = []
    for benchmark, expected_codes in BENCHMARK_CODES.items():
        covered = [
            {code: row["net_amount"] for code, row in by_date.get(day["trade_date"], {}).items()
             if code in expected_codes and row["net_amount"] is not None}
            for day in history
        ]
        latest = covered[-1]

        def benchmark_window_sum(count: int) -> float | None:
            window = covered[-count:]
            if len(window) != count or any(len(day) != len(expected_codes) for day in window):
                return None
            return sum(sum(day.values()) for day in window)

        benchmarks.append({
            "benchmark": benchmark,
            "net_1d": benchmark_window_sum(1),
            "net_3d": benchmark_window_sum(3),
            "net_5d": benchmark_window_sum(5),
            "net_20d": benchmark_window_sum(20),
            "share_change": None,
            "positive_count": sum(1 for amount in latest.values() if amount > 0),
            "total_count": len(latest),
            "expected_etf_codes": list(expected_codes),
            "available_etf_codes": sorted(latest),
            "missing_etf_codes": [code for code in expected_codes if code not in latest],
        })
    benchmarks.sort(
        key=lambda item: (
            item.get("net_1d") is not None,
            float(item.get("net_1d") or 0),
        ),
        reverse=True,
    )
    summary, evidence = _signal(
        history, selected_rows, len(CORE_ETFS),
        {row["benchmark"]: row["net_1d"] for row in benchmarks if row["net_1d"] is not None},
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
        "按固定跟踪产品范围展示。基准估算要求窗口内每个交易日的跟踪产品齐全，份额变化见各产品明细。",
        "整体区间累计使用相同的产品样本，日度估算金额按实际覆盖展示。",
        "交易所份额的接口读取时间只证明本次查询所见，缺少原始发布时间时不可用于历史盘中回放。",
        "数据覆盖评分由可估算产品覆盖和有效历史样本数决定。",
    ]
    if trade_date != requested_date:
        caveats.append(f"请求日期 {requested_date} 对应最近已结束交易日 {trade_date}。")
    if warnings:
        caveats.append("部分数据源读取失败：" + "；".join(warnings[:6]))
    if any(row.get("observed_net_amount") is not None for row in history):
        caveats.append("覆盖低于65%的历史日已留空，不会把缺失 ETF 隐式按0元计入区间累计。")

    excluded_rows = [row for row in selected_rows_all if row.get("net_amount") is None]
    if any(row["quality_status"] == "corporate_action_suspected" for row in excluded_rows):
        caveats.append("疑似份额拆分、合并等公司行为的记录保留在明细中，但不计入资金汇总。")
    if any(row["quality_status"] == "missing_price_reference" for row in excluded_rows):
        caveats.append("缺少有效前收盘价的产品只显示份额变化，金额不计入资金汇总。")

    if summary["coverage_pct"] < _MIN_RELIABLE_COVERAGE_PCT:
        status = "insufficient"
    elif len(selected_rows) != len(CORE_ETFS) or any(
        summary[field] is None for field in ("net_3d", "net_5d", "net_20d")
    ):
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "source_batch_status": source_status,
        "requested_date": requested_date,
        "trade_date": trade_date,
        "data_as_of": trade_date,
        "window_days": days,
        "scope_label": SCOPE_LABEL,
        "summary": summary,
        "flow_evidence": {"basis": "EXCHANGE_DAILY_SHARE_CHANGE", "trade_date": trade_date,
                          "observed_at": observed_at, "publication_time_status": "UNKNOWN",
                          "historical_pit_eligible": False, "actor_status": "UNKNOWN",
                          "pattern": summary["flow_pattern"]},
        "market_activity": {"basis": "SECONDARY_MARKET_TURNOVER", "trade_date": activity_date,
                            "etfs": activity},
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
        "generated_at": datetime.now(PRODUCTION_TIMEZONE).isoformat(timespec="seconds"),
    }


@router.get("/hot-data/broad-etf-flow")
def broad_etf_flow(
    trade_date: str = Query(default_factory=lambda: datetime.now(PRODUCTION_TIMEZONE).date().isoformat()),
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
            "source_batch_status": "UNAVAILABLE",
            "requested_date": requested,
            "trade_date": "",
            "data_as_of": "",
            "window_days": days,
            "scope_label": SCOPE_LABEL,
            "summary": {
                "signal": "数据暂不可用",
                "signal_tone": "unknown",
                "confidence": 0,
                "confidence_label": "不可用",
                "identity_status": "未确认",
                "flow_pattern": "UNKNOWN",
                "net_1d": None,
                "net_3d": None,
                "net_5d": None,
                "net_20d": None,
                "positive_count": 0,
                "total_count": 0,
                "coverage_pct": 0,
            },
            "flow_evidence": {"basis": "EXCHANGE_DAILY_SHARE_CHANGE", "trade_date": None,
                              "observed_at": None, "publication_time_status": "UNKNOWN",
                              "historical_pit_eligible": False, "actor_status": "UNKNOWN",
                              "pattern": "UNKNOWN"},
            "market_activity": {"basis": "SECONDARY_MARKET_TURNOVER", "trade_date": None, "etfs": []},
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
    if payload["source_batch_status"] == "FINISHED":
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
