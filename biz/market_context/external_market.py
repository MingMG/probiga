"""Capture and score external-market conditions for A-share recommendations.

The declared runtime uses Eastmoney's public native quote interface directly.
External data is deliberately stored as a
snapshot before the recommendation batch starts.  That gives every generated
recommendation the same capture time and makes missing/stale sources visible
instead of silently treating them as neutral.
"""
from __future__ import annotations

import json
import logging
import math
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from urllib.parse import quote, urlencode
from urllib.request import Request, build_opener, ProxyHandler
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.sql_reader import read_sql_rows
from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)

logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_QUOTE_AGE_SECONDS = 96 * 60 * 60


def _public_json(request: Request, *, timeout: int) -> dict[str, Any]:
    with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))

EXTERNAL_MARKET_SYMBOLS: tuple[tuple[str, str], ...] = (
    ("nasdaq", "美股纳斯达克"),
    ("sp500", "美股标普500"),
    ("dow", "美股道琼斯"),
    ("nikkei", "日本日经225"),
    ("kospi", "韩国KOSPI"),
    ("hang_seng", "港股恒生指数"),
    ("taiwan", "台湾加权指数"),
    ("a50", "富时中国A50期货"),
    ("sp500_futures", "标普500期货"),
    ("nasdaq_futures", "纳斯达克期货"),
    ("dow_futures", "道琼斯期货"),
    ("crude_oil", "原油"),
    ("gold", "黄金"),
    ("silver", "白银"),
    ("copper", "铜"),
    ("usdcnh", "美元兑人民币"),
    ("usdjpy", "美元兑日元"),
    ("usdkrw", "美元兑韩元"),
    ("usdhkd", "美元兑港币"),
    ("us10y", "美国10年期国债收益率"),
    ("vix", "VIX恐慌指数"),
    ("us_lithium", "美股锂电ETF"),
    ("us_semiconductor", "美股半导体ETF"),
    ("us_ai", "美股人工智能ETF"),
    ("us_robotics", "美股机器人ETF"),
    ("us_clean_energy", "美股清洁能源ETF"),
    ("us_biotech", "美股生物科技ETF"),
    ("us_auto", "美股汽车ETF"),
    ("us_defense", "美股国防航空ETF"),
    ("us_software", "美股软件ETF"),
    ("us_cybersecurity", "美股网络安全ETF"),
    ("us_consumer", "美股可选消费ETF"),
    ("us_financial", "美股金融ETF"),
    ("us_agriculture", "美股农业商品ETF"),
    ("kr_semiconductor", "韩国三星电子"),
    ("kr_battery", "韩国LG新能源"),
    ("jp_semiconductor", "日本东京电子"),
    ("jp_robotics", "日本发那科"),
    ("jp_auto", "日本丰田汽车"),
    ("jp_battery", "日本松下控股"),
    ("taiwan_semiconductor", "中国台湾台积电"),
)

# A second provider supplies only missing exact instruments.
_YAHOO_FALLBACK_MAP = {
    "sp500_futures": "ES=F",
    "nasdaq_futures": "NQ=F",
    "dow_futures": "YM=F",
    "crude_oil": "CL=F",
    "gold": "GC=F",
    "silver": "SI=F",
    "copper": "HG=F",
    "nasdaq": "^IXIC",
    "sp500": "^GSPC",
    "dow": "^DJI",
    "nikkei": "^N225",
    "kospi": "^KS11",
    "hang_seng": "^HSI",
    "taiwan": "^TWII",
    "vix": "^VIX",
    "usdcnh": "CNH=X",
    "usdjpy": "JPY=X",
    "usdkrw": "KRW=X",
    "usdhkd": "HKD=X",
    "us10y": "^TNX",
    "us_lithium": "LIT",
    "us_semiconductor": "SOXX",
    "us_ai": "AIQ",
    "us_robotics": "BOTZ",
    "us_clean_energy": "ICLN",
    "us_biotech": "XBI",
    "us_auto": "CARZ",
    "us_defense": "ITA",
    "us_software": "IGV",
    "us_cybersecurity": "CIBR",
    "us_consumer": "XLY",
    "us_financial": "XLF",
    "us_agriculture": "DBA",
    "kr_semiconductor": "005930.KS",
    "kr_battery": "373220.KS",
    "jp_semiconductor": "8035.T",
    "jp_robotics": "6954.T",
    "jp_auto": "7203.T",
    "jp_battery": "6752.T",
    "taiwan_semiconductor": "2330.TW",
}

# Exact source-side market and instrument identities verified against the
# provider's quote and public security-search responses.  These are never
# selected by a fuzzy display-name match during collection.
_EASTMONEY_QUOTE_IDS = {
    "nasdaq": "100.NDX", "sp500": "100.SPX", "dow": "100.DJIA",
    "nikkei": "100.N225", "kospi": "100.KS11", "hang_seng": "100.HSI",
    "taiwan": "100.TWII", "vix": "167.VIX",
    "sp500_futures": "103.ES00Y", "nasdaq_futures": "103.NQ00Y",
    "dow_futures": "103.YM00Y", "a50": "104.CN00Y",
    "crude_oil": "102.CL00Y", "gold": "101.GC00Y",
    "silver": "101.SI00Y", "copper": "101.HG00Y",
    "usdcnh": "133.USDCNH", "usdjpy": "119.USDJPY",
    "usdkrw": "119.USDKRW", "usdhkd": "119.USDHKD", "us10y": "171.US10Y",
    "us_lithium": "107.LIT", "us_semiconductor": "105.SOXX",
    "us_ai": "105.AIQ", "us_robotics": "105.BOTZ", "us_clean_energy": "105.ICLN",
    "us_biotech": "107.XBI", "us_auto": "105.CARZ", "us_defense": "107.ITA",
    "us_software": "107.IGV", "us_cybersecurity": "105.CIBR",
    "us_consumer": "107.XLY", "us_financial": "107.XLF", "us_agriculture": "107.DBA",
    "kr_semiconductor": "177.005930", "kr_battery": "177.373220",
    "jp_semiconductor": "176.8035", "jp_robotics": "176.6954",
    "jp_auto": "176.7203", "jp_battery": "176.6752",
}


def _shanghai_naive(value: datetime) -> datetime:
    return value.astimezone(SHANGHAI).replace(tzinfo=None) if value.tzinfo else value


def _quote_time(timestamp: object, *, captured_at: datetime) -> datetime:
    seconds = _number(timestamp)
    if seconds is None or seconds < 1_000_000_000:
        raise ValueError("source quote timestamp is missing")
    value = datetime.fromtimestamp(seconds, SHANGHAI).replace(tzinfo=None)
    age = (_shanghai_naive(captured_at) - value).total_seconds()
    if age < -300 or age > MAX_QUOTE_AGE_SECONDS:
        raise ValueError(f"source quote timestamp is stale or future: age_seconds={age:.0f}")
    return value


def _parse_eastmoney_quote(symbol: str, row: dict[str, Any], *, captured_at: datetime) -> dict[str, Any]:
    quote_id = _EASTMONEY_QUOTE_IDS[symbol]
    observed_id = f"{row.get('f13')}.{row.get('f12')}"
    if observed_id != quote_id:
        raise ValueError(f"source quote identity differs: expected={quote_id} observed={observed_id}")
    price, previous, change = (_number(row.get(key)) for key in ("f2", "f18", "f3"))
    if price is None or price <= 0 or previous is None or previous <= 0 or change is None:
        raise ValueError("source quote lacks finite positive price/previous close or change")
    market_time = _quote_time(row.get("f124"), captured_at=captured_at)
    return {
        "symbol": symbol, "display_name": dict(EXTERNAL_MARKET_SYMBOLS)[symbol],
        "price": price, "previous_close": previous, "change_pct": change,
        "market_time": market_time.isoformat(sep=" "), "availability": "available",
        "source": "eastmoney.quote.ulist", "raw_code": quote_id,
        "payload": {"quote_id": quote_id, "source_timestamp": row["f124"], "fltt": 2,
                    "price": price, "previous_close": previous, "change_pct": change},
    }


def _load_eastmoney_quote_items(*, captured_at: datetime) -> tuple[dict[str, dict[str, Any]], list[str]]:
    query = urlencode({"secids": ",".join(_EASTMONEY_QUOTE_IDS.values()), "fltt": 2,
                       "invt": 2, "fields": "f12,f13,f2,f3,f18,f124"})
    request = Request("https://push2delay.eastmoney.com/api/qt/ulist.np/get?" + query,
                      headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"})
    payload = None
    errors = []
    for attempt in range(3):
        try:
            payload = _public_json(request, timeout=10)
            if not isinstance(payload, dict) or payload.get("rc") != 0:
                raise ValueError("Eastmoney quote service returned an unsuccessful response")
            break
        except Exception as exc:
            payload = None
            errors.append(f"eastmoney attempt={attempt + 1}: {type(exc).__name__}: {exc}")
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    data = payload.get("data") if isinstance(payload, dict) else None
    rows = data.get("diff") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return {}, errors + ["eastmoney quote response has no explicit instrument list"]
    by_id = {}
    for row in rows:
        if not isinstance(row, dict):
            return {}, errors + ["eastmoney quote response contains an invalid row"]
        identity = f"{row.get('f13')}.{row.get('f12')}"
        if identity not in _EASTMONEY_QUOTE_IDS.values() or identity in by_id:
            return {}, errors + [f"eastmoney quote identity is unexpected or duplicated: {identity}"]
        by_id[identity] = row
    items = {}
    for symbol, identity in _EASTMONEY_QUOTE_IDS.items():
        try:
            if identity not in by_id:
                raise ValueError("requested instrument missing from source response")
            items[symbol] = _parse_eastmoney_quote(symbol, by_id[identity], captured_at=captured_at)
        except (TypeError, ValueError, OverflowError) as exc:
            errors.append(f"eastmoney {symbol}: {exc}")
    return items, errors


def _parse_twse_quote(payload: dict[str, Any], *, captured_at: datetime) -> dict[str, Any]:
    """Read TSMC's Taiwan listing from the exchange's native quote response."""
    if not isinstance(payload, dict) or payload.get("rtcode") != "0000":
        raise ValueError("TWSE quote service did not confirm success")
    rows = payload.get("msgArray")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise ValueError("TWSE quote response must contain one requested instrument")
    row = rows[0]
    if row.get("c") != "2330" or row.get("ch") != "2330.tw" or row.get("ex") != "tse":
        raise ValueError("TWSE quote identity differs from tse_2330.tw")
    price, previous = _number(row.get("z")), _number(row.get("y"))
    if price is None or price <= 0 or previous is None or previous <= 0:
        raise ValueError("TWSE quote lacks finite positive price or previous close")
    traded_at = datetime.strptime(f"{row.get('d')} {row.get('t')}", "%Y%m%d %H:%M:%S")
    market_time = _quote_time(traded_at.replace(tzinfo=SHANGHAI).timestamp(), captured_at=captured_at)
    update_millis = _number(row.get("tlong"))
    updated_at = _quote_time(update_millis / 1000 if update_millis else None, captured_at=captured_at)
    if updated_at < traded_at or updated_at.date() != traded_at.date():
        raise ValueError("TWSE update clock and native trade date disagree")
    change = (price - previous) / previous * 100
    return {
        "symbol": "taiwan_semiconductor", "display_name": dict(EXTERNAL_MARKET_SYMBOLS)["taiwan_semiconductor"],
        "price": price, "previous_close": previous, "change_pct": change,
        "market_time": market_time.isoformat(sep=" "), "availability": "available",
        "source": "twse.mis.stock_info", "raw_code": "tse_2330.tw",
        "payload": {"source_timestamp": row["tlong"], "source_updated_at": updated_at.isoformat(sep=" "),
                    "trade_date": row["d"], "trade_time": row["t"], "price": price,
                    "previous_close": previous, "change_pct": change},
    }


def _load_twse_quote_item(*, captured_at: datetime) -> tuple[dict[str, Any] | None, list[str]]:
    request = Request(
        "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_2330.tw&json=1&delay=0",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/"},
    )
    errors = []
    for attempt in range(3):
        try:
            return _parse_twse_quote(_public_json(request, timeout=8), captured_at=captured_at), errors
        except Exception as exc:
            errors.append(f"twse taiwan_semiconductor attempt={attempt + 1}: {type(exc).__name__}: {exc}")
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    return None, errors


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "").replace("%", "")
        if value in {"", "-", "--", "nan", "None", "null"}:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_datetime(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        parsed = parsed.to_pydatetime()
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed


def _as_date(value: Any, default: date | None = None) -> date:
    parsed = _as_datetime(value)
    return parsed.date() if parsed else (default or datetime.now().date())


def _parse_yahoo_chart_payload(
    symbol: str,
    display_name: str,
    raw_code: str,
    payload: dict[str, Any],
    *,
    captured_at: datetime,
) -> dict[str, Any] | None:
    """Parse one Yahoo chart response without crossing ``captured_at``."""
    chart = payload.get("chart") if isinstance(payload, dict) else None
    results = chart.get("result") if isinstance(chart, dict) else None
    result = results[0] if isinstance(results, list) and results else None
    if not isinstance(result, dict):
        return None
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    if meta.get("symbol") != raw_code:
        return None
    cutoff_ts = _shanghai_naive(captured_at).replace(tzinfo=SHANGHAI).timestamp()
    market_ts = _number(meta.get("regularMarketTime"))
    price = None
    previous = None
    selected_ts = None
    timestamps = result.get("timestamp") if isinstance(result.get("timestamp"), list) else []
    indicators = result.get("indicators") if isinstance(result.get("indicators"), dict) else {}
    quotes = indicators.get("quote") if isinstance(indicators.get("quote"), list) else []
    closes = quotes[0].get("close") if quotes and isinstance(quotes[0], dict) else []
    safe_points: list[tuple[float, float]] = []
    for timestamp, close in zip(timestamps, closes or []):
        ts = _number(timestamp)
        value = _number(close)
        if ts is not None and value is not None and ts <= cutoff_ts:
            safe_points.append((ts, value))
    # A small clock-skew allowance is acceptable for a live request, but a
    # historical replay must never read today's regularMarketPrice.
    if market_ts is not None and market_ts <= cutoff_ts + 300:
        price = _number(meta.get("regularMarketPrice"))
        # ``chartPreviousClose`` is the close at the beginning of the selected
        # range, not necessarily yesterday's close.  The penultimate daily
        # point is the correct comparison for a live/current-session quote.
        if safe_points:
            latest_bar = safe_points[-1][1]
            same_as_live = price is not None and abs(latest_bar - price) <= max(0.0001, abs(price) * 0.0005)
            if same_as_live and len(safe_points) >= 2:
                previous = safe_points[-2][1]
            elif not same_as_live:
                previous = latest_bar
        if previous is None:
            previous = _number(meta.get("previousClose"))
        selected_ts = market_ts
    elif market_ts is not None:
        # The response has already advanced beyond the requested replay
        # cutoff.  Daily bars are mutable until the session closes, so using
        # them here would silently introduce future information.
        return None
    elif safe_points:
        selected_ts, price = safe_points[-1]
        if len(safe_points) >= 2:
            previous = safe_points[-2][1]
    if price is None or price <= 0 or previous is None or previous <= 0:
        return None
    # Yahoo quotes ``^TNX`` in tenths of a percentage point (for example
    # 42.1 means a 4.21% Treasury yield).  Normalize it to the same unit as
    # the AkShare bond source before the macro risk rules consume the value.
    if symbol == "us10y" and abs(float(price)) >= 20.0:
        price = float(price) / 10.0
        if previous is not None:
            previous = float(previous) / 10.0
    change_pct = None
    if previous not in (None, 0.0):
        change_pct = (float(price) - float(previous)) / abs(float(previous)) * 100.0
    try:
        market_time = _quote_time(selected_ts, captured_at=captured_at).isoformat(sep=" ")
    except (OverflowError, OSError, ValueError):
        return None
    return {
        "symbol": symbol,
        "display_name": display_name,
        "price": price,
        "change_pct": change_pct,
        "previous_close": previous,
        "market_time": market_time,
        "availability": "available",
        "source": "yahoo.finance.chart",
        "raw_code": raw_code,
        "payload": {
            "currency": meta.get("currency"),
            "exchangeName": meta.get("exchangeName"),
            "exchangeTimezoneName": meta.get("exchangeTimezoneName"),
            "regularMarketTime": meta.get("regularMarketTime"),
        },
    }


def _fetch_yahoo_fallback_item(
    symbol: str,
    display_name: str,
    raw_code: str,
    *,
    captured_at: datetime,
) -> dict[str, Any] | None:
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{quote(raw_code, safe='')}?interval=1d&range=10d"
    )
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    payload = _public_json(request, timeout=8)
    return _parse_yahoo_chart_payload(
        symbol, display_name, raw_code, payload, captured_at=captured_at,
    )


def _load_yahoo_fallback_items(
    symbols: Iterable[str],
    *,
    captured_at: datetime,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Fetch missing spot/FX symbols concurrently within the 09:08 budget."""
    display_names = dict(EXTERNAL_MARKET_SYMBOLS)
    requested = [symbol for symbol in dict.fromkeys(symbols) if symbol in _YAHOO_FALLBACK_MAP]
    if not requested:
        return {}, []
    items: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(6, len(requested))) as executor:
        futures = {
            executor.submit(
                _fetch_yahoo_fallback_item,
                symbol,
                display_names.get(symbol, symbol),
                _YAHOO_FALLBACK_MAP[symbol],
                captured_at=captured_at,
            ): symbol
            for symbol in requested
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                item = future.result()
            except Exception as exc:
                errors.append(f"yahoo {symbol}: {type(exc).__name__}: {exc}")
                continue
            if item is None:
                errors.append(f"yahoo {symbol}: no point-in-time quote")
            else:
                items[symbol] = item
    return items, errors


def _score_snapshot(items: list[dict[str, Any]]) -> tuple[float | None, str, str]:
    values = {item["symbol"]: item for item in items if item.get("availability") == "available"}
    core_symbols = ("nasdaq", "sp500", "dow", "nikkei", "kospi", "hang_seng", "taiwan")
    core_changes = [
        _number(values.get(symbol, {}).get("change_pct"))
        for symbol in core_symbols
        if _number(values.get(symbol, {}).get("change_pct")) is not None
    ]
    proxy_symbols = ("sp500_futures", "nasdaq_futures", "dow_futures", "a50")
    proxy_changes = [
        _number(values.get(symbol, {}).get("change_pct"))
        for symbol in proxy_symbols
        if _number(values.get(symbol, {}).get("change_pct")) is not None
    ]
    if not core_changes and len(proxy_changes) < 2:
        return None, "UNKNOWN", "外围现货指数和股指期货代理暂无足够数据，不参与决策"

    weighted = {
        "nasdaq": 0.22,
        "sp500": 0.20,
        "dow": 0.10,
        "nikkei": 0.16,
        "kospi": 0.14,
        "hang_seng": 0.10,
        "taiwan": 0.08,
        "sp500_futures": 0.12,
        "nasdaq_futures": 0.12,
        "dow_futures": 0.06,
        "a50": 0.10,
        "copper": 0.08,
    }
    pressure = 0.0
    weight_sum = 0.0
    for symbol, weight in weighted.items():
        change = _number(values.get(symbol, {}).get("change_pct"))
        if change is not None:
            pressure += change * weight
            weight_sum += weight
    # A rising dollar against CNH/KRW is a mild risk signal for A-shares.
    for symbol, weight in (("usdcnh", 0.18), ("usdkrw", 0.10)):
        change = _number(values.get(symbol, {}).get("change_pct"))
        if change is not None:
            pressure -= change * weight
            weight_sum += weight
    # Oil/gold jumps can indicate geopolitical or inflation pressure.  Keep the
    # adjustment deliberately small so one commodity cannot dominate equities.
    for symbol, weight in (("crude_oil", 0.04), ("gold", 0.03)):
        change = _number(values.get(symbol, {}).get("change_pct"))
        if change is not None:
            pressure -= change * weight
            weight_sum += weight

    vix_adjustment = 0.0
    vix_price = _number(values.get("vix", {}).get("price"))
    vix_change = _number(values.get("vix", {}).get("change_pct"))
    if vix_price is not None:
        if vix_price >= 30.0:
            vix_adjustment -= 3.0
        elif vix_price >= 25.0:
            vix_adjustment -= 2.0
        elif vix_price >= 20.0:
            vix_adjustment -= 1.0
        elif vix_price <= 15.0:
            vix_adjustment += 0.5
    if vix_change is not None:
        vix_adjustment -= max(-1.0, min(1.0, vix_change / 10.0))

    us10y = _number(values.get("us10y", {}).get("price"))
    if us10y is not None:
        if us10y >= 5.0:
            vix_adjustment -= 1.0
        elif us10y <= 3.5:
            vix_adjustment += 0.3

    score = max(0.0, min(100.0, 50.0 + pressure * 3.0 + vix_adjustment))
    support_threshold = 53.0 if core_changes else 50.75
    risk_threshold = 47.0 if core_changes else 49.25
    status = (
        "SUPPORT"
        if score >= support_threshold
        else ("RISK" if score <= risk_threshold else "NEUTRAL")
    )
    reason_parts = []
    for symbol in ("nasdaq", "sp500", "nikkei", "kospi", "hang_seng", "a50", "usdcnh", "vix", "us10y", "crude_oil", "gold"):
            item = values.get(symbol)
            change = _number(item.get("change_pct")) if item else None
            if change is not None:
                reason_parts.append(f"{item.get('display_name') or symbol} {change:+.2f}%")
    if not core_changes:
        reason_parts.insert(0, "现货指数缺失，使用股指期货/A50代理")
    completeness = f"有效{len(values)}/{len(EXTERNAL_MARKET_SYMBOLS)}项"
    return round(score, 1), status, "；".join(reason_parts[:8]) + f"（{completeness}）"


def _snapshot_quality(items: list[dict[str, Any]]) -> str:
    available = {
        str(item.get("symbol") or "")
        for item in items
        if item.get("availability") == "available"
    }
    core_available = len(
        available
        & {
            "nasdaq",
            "sp500",
            "dow",
            "nikkei",
            "kospi",
            "hang_seng",
            "taiwan",
        }
    )
    proxy_available = len(
        available
        & {"sp500_futures", "nasdaq_futures", "dow_futures", "a50"}
    )
    if core_available >= 3 and len(available) == len(EXTERNAL_MARKET_SYMBOLS):
        return "PASS"
    if proxy_available >= 2 and len(available) >= 5:
        return "WATCH"
    return "WATCH" if available else "UNKNOWN"


def fetch_external_market_snapshot(as_of: datetime | None = None) -> dict[str, Any]:
    """Capture native quotes independently and retain every missing instrument."""
    captured_at = _shanghai_naive(as_of or datetime.now(SHANGHAI)).replace(microsecond=0)
    eastmoney, errors = _load_eastmoney_quote_items(captured_at=captured_at)
    twse, twse_errors = _load_twse_quote_item(captured_at=captured_at)
    errors.extend(twse_errors)
    if twse is not None:
        eastmoney["taiwan_semiconductor"] = twse
    missing = [symbol for symbol, _name in EXTERNAL_MARKET_SYMBOLS if symbol not in eastmoney]
    yahoo, yahoo_errors = _load_yahoo_fallback_items(missing, captured_at=captured_at)
    errors.extend(yahoo_errors)
    items = []
    for symbol, name in EXTERNAL_MARKET_SYMBOLS:
        item = eastmoney.get(symbol) or yahoo.get(symbol)
        if item is None:
            reasons = [error for error in errors if symbol in error]
            item = {"symbol": symbol, "display_name": name, "price": None,
                    "previous_close": None, "change_pct": None, "market_time": None,
                    "availability": "missing", "source": "external.unavailable",
                    "payload": {"error": " | ".join(reasons or errors or ["no supported source response"])[:2000]}}
        items.append(item)
    missing_symbols = [item["symbol"] for item in items if item["availability"] != "available"]
    available_count = len(items) - len(missing_symbols)
    acquisition_status = "COMPLETE" if not missing_symbols else ("PARTIAL" if available_count else "FAILED")
    score, status, reason = _score_snapshot(items)
    if missing_symbols:
        reason = f"{reason}; missing {len(missing_symbols)}/{len(items)}: {','.join(missing_symbols)}"
    return {
        "snapshot_id": str(uuid.uuid4()), "context_date": captured_at.date().isoformat(),
        "captured_at": captured_at, "source": "+".join(sorted({item["source"] for item in items if item["availability"] == "available"})) or "external.unavailable",
        "items": items, "external_market_score": score, "external_market_status": status,
        "external_market_reason": reason or "外围数据暂无可用结果",
        "external_market_data_quality": _snapshot_quality(items),
        "acquisition_status": acquisition_status, "available_count": available_count,
        "expected_count": len(EXTERNAL_MARKET_SYMBOLS), "missing_symbols": missing_symbols,
        "source_warnings": errors,
    }


_EXTERNAL_MARKET_DDL = """
    CREATE TABLE IF NOT EXISTS st_external_market_context (
        id BIGINT NOT NULL AUTO_INCREMENT,
        snapshot_id VARCHAR(64) NOT NULL,
        context_date DATE NOT NULL,
        captured_at DATETIME NOT NULL,
        source VARCHAR(64) NOT NULL,
        symbol VARCHAR(64) NOT NULL,
        display_name VARCHAR(128) NOT NULL,
        price DECIMAL(20,6) NULL,
        change_pct DECIMAL(12,6) NULL,
        previous_close DECIMAL(20,6) NULL,
        market_time DATETIME NULL,
        availability VARCHAR(16) NOT NULL,
        payload_json LONGTEXT NULL,
        PRIMARY KEY (id),
        KEY idx_external_context_capture (context_date, captured_at),
        KEY idx_external_context_symbol (symbol, captured_at),
        KEY idx_external_context_snapshot (snapshot_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_EXTERNAL_MARKET_SCHEMA = {
    "st_external_market_context": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "snapshot_id": RuntimeColumn("varchar", False, character_length=64),
            "context_date": RuntimeColumn("date", False),
            "captured_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "source": RuntimeColumn("varchar", False, character_length=64),
            "symbol": RuntimeColumn("varchar", False, character_length=64),
            "display_name": RuntimeColumn("varchar", False, character_length=128),
            "price": RuntimeColumn("decimal", True, numeric_precision=20, numeric_scale=6),
            "change_pct": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=6),
            "previous_close": RuntimeColumn("decimal", True, numeric_precision=20, numeric_scale=6),
            "market_time": RuntimeColumn("datetime", True, datetime_precision=0),
            "availability": RuntimeColumn("varchar", False, character_length=16),
            "payload_json": RuntimeColumn("longtext", True),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("context_date", "captured_at")),
            RuntimeIndex(("symbol", "captured_at")),
            RuntimeIndex(("snapshot_id",)),
        ),
    ),
}


def privileged_migrate_external_market_tables(engine: Engine) -> None:
    """Create/normalize the external snapshot table in a release window."""

    with engine.begin() as conn:
        conn.execute(text(_EXTERNAL_MARKET_DDL))
        privileged_normalize_mysql_storage(conn, _EXTERNAL_MARKET_SCHEMA)
        validate_external_market_runtime(engine, connection=conn)


def validate_external_market_runtime(engine: Engine, *, connection=None) -> None:
    """Read-only fail-closed external market table contract."""

    validate_runtime_tables(
        engine,
        _EXTERNAL_MARKET_SCHEMA,
        context="external_market",
        connection=connection,
    )


def ensure_external_market_table(engine: Engine) -> None:
    """Compatibility guard: validate only; never mutate runtime schema."""

    validate_external_market_runtime(engine)


def store_external_market_snapshot(engine: Engine, snapshot: dict[str, Any]) -> dict[str, Any]:
    """Persist one coherent snapshot and return its summary."""
    ensure_external_market_table(engine)
    captured_at = snapshot.get("captured_at") or datetime.now().replace(microsecond=0)
    if isinstance(captured_at, str):
        captured_at = _as_datetime(captured_at) or datetime.now().replace(microsecond=0)
    snapshot_id = str(snapshot.get("snapshot_id") or uuid.uuid4())
    context_date = _as_date(snapshot.get("context_date"), default=captured_at.date())
    records = []
    for item in snapshot.get("items") or []:
        records.append({
            "snapshot_id": snapshot_id,
            "context_date": context_date,
            "captured_at": captured_at,
            "source": str(item.get("source") or snapshot.get("source") or "unknown")[:64],
            "symbol": str(item.get("symbol") or "")[:64],
            "display_name": str(item.get("display_name") or item.get("symbol") or "")[:128],
            "price": _number(item.get("price")),
            "change_pct": _number(item.get("change_pct")),
            "previous_close": _number(item.get("previous_close")),
            "market_time": _as_datetime(item.get("market_time")),
            "availability": str(item.get("availability") or "missing")[:16],
            "payload_json": json.dumps(item.get("payload") or {}, ensure_ascii=False, default=str)[:60000],
        })
    if records:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO st_external_market_context
                    (snapshot_id, context_date, captured_at, source, symbol, display_name,
                     price, change_pct, previous_close, market_time, availability, payload_json)
                VALUES
                    (:snapshot_id, :context_date, :captured_at, :source, :symbol, :display_name,
                     :price, :change_pct, :previous_close, :market_time, :availability, :payload_json)
            """), records)
    return {
        "snapshot_id": snapshot_id,
        "context_date": context_date.isoformat(),
        "captured_at": captured_at.isoformat(sep=" "),
        "external_market_status": snapshot.get("external_market_status") or "UNKNOWN",
        "external_market_score": snapshot.get("external_market_score"),
        "external_market_data_quality": snapshot.get("external_market_data_quality") or "UNKNOWN",
        "available_count": int(snapshot.get("available_count") or 0),
        "expected_count": int(snapshot.get("expected_count") or len(EXTERNAL_MARKET_SYMBOLS)),
        "source_warnings": snapshot.get("source_warnings") or [],
        "acquisition_status": snapshot.get("acquisition_status") or "FAILED",
        "missing_symbols": list(snapshot.get("missing_symbols") or []),
    }


def _parse_cutoff(as_of: datetime | str | date | None) -> datetime:
    if isinstance(as_of, datetime):
        return as_of
    if isinstance(as_of, date):
        return datetime.combine(as_of, datetime.max.time()).replace(microsecond=0)
    if as_of:
        return _as_datetime(as_of) or datetime.now()
    return datetime.now()


def load_latest_external_market_context(
    engine: Engine,
    as_of: datetime | str | date | None = None,
) -> dict[str, Any]:
    """Load the latest captured batch for the recommendation run."""
    defaults: dict[str, Any] = {
        "external_market_status": "UNKNOWN",
        "external_market_score": 50.0,
        "external_market_reason": "外围市场数据未抓取",
        "external_market_data_quality": "UNKNOWN",
        "external_market_captured_at": "",
        "external_market_source": "",
        "external_market_items_json": "[]",
    }
    try:
        ensure_external_market_table(engine)
        cutoff = _parse_cutoff(as_of)
        rows = pd.DataFrame(read_sql_rows(
            engine,
            """
                SELECT snapshot_id, context_date, captured_at, source, symbol, display_name,
                       price, change_pct, previous_close, market_time, availability, payload_json
                FROM st_external_market_context
                WHERE context_date = :context_date
                  AND captured_at <= :cutoff
                ORDER BY captured_at DESC, id DESC
                LIMIT 200
            """,
            {"context_date": cutoff.date(), "cutoff": cutoff},
            context="external_market.latest_context",
        ))
        if rows.empty:
            return defaults
        snapshot_id = str(rows.iloc[0].get("snapshot_id") or "")
        selected = rows[rows["snapshot_id"].astype(str) == snapshot_id].copy()
        items = []
        for row in selected.astype(object).where(pd.notna(selected), None).to_dict(orient="records"):
            items.append({
                "symbol": row.get("symbol"),
                "display_name": row.get("display_name"),
                "price": _number(row.get("price")),
                "change_pct": _number(row.get("change_pct")),
                "previous_close": _number(row.get("previous_close")),
                "market_time": str(row.get("market_time") or "")[:19],
                "availability": row.get("availability") or "missing",
                "source": row.get("source") or "",
                "payload": json.loads(row.get("payload_json") or "{}"),
            })
        score, status, reason = _score_snapshot(items)
        available_count = sum(item.get("availability") == "available" for item in items)
        quality = _snapshot_quality(items)
        missing_symbols = [symbol for symbol, _name in EXTERNAL_MARKET_SYMBOLS
                           if not any(item["symbol"] == symbol and item["availability"] == "available" for item in items)]
        item_sources = sorted({str(item.get("source") or "").strip() for item in items if item.get("source")})
        return {
            "external_market_status": status,
            "external_market_score": score if score is not None else 50.0,
            "external_market_reason": reason,
            "external_market_data_quality": quality,
            "external_market_captured_at": str(rows.iloc[0].get("captured_at") or "")[:19],
            "external_market_source": "+".join(item_sources) or "unknown",
            "external_market_items_json": json.dumps(items, ensure_ascii=False, default=str),
            "acquisition_status": "COMPLETE" if not missing_symbols else ("PARTIAL" if available_count else "FAILED"),
            "available_count": available_count,
            "expected_count": len(EXTERNAL_MARKET_SYMBOLS),
            "missing_symbols": missing_symbols,
        }
    except Exception as exc:  # data enrichment must never block the base recommendation
        logger.warning("External market context load skipped: %s", exc)
        return {**defaults, "external_market_reason": f"外围市场数据不可用：{exc}"}
