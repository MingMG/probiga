"""Capture and score external-market conditions for A-share recommendations.

The production host already carries AkShare, whose Eastmoney-backed global
market endpoints are used here.  External data is deliberately stored as a
snapshot before the recommendation batch starts.  That gives every generated
recommendation the same capture time and makes missing/stale sources visible
instead of silently treating them as neutral.
"""
from __future__ import annotations

import json
import logging
import math
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.sql_reader import read_sql_rows

logger = logging.getLogger(__name__)

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

_INDEX_MAP = {
    "NDX": "nasdaq",
    "IXIC": "nasdaq",
    "SPX": "sp500",
    "SP500": "sp500",
    "DJIA": "dow",
    "DJI": "dow",
    "N225": "nikkei",
    "NKY": "nikkei",
    "KS11": "kospi",
    "KOSPI": "kospi",
    "HSI": "hang_seng",
    "TWII": "taiwan",
    "TWJQ": "taiwan",
    "VIX": "vix",
}
_FUTURES_MAP = {
    "ES00Y": "sp500_futures",
    "NQ00Y": "nasdaq_futures",
    "YM00Y": "dow_futures",
    "CN00Y": "a50",
    "CL00Y": "crude_oil",
    "GC00Y": "gold",
    "SI00Y": "silver",
    "HG00Y": "copper",
}
_FOREX_MAP = {
    "USDCNH": "usdcnh",
    "USDJPY": "usdjpy",
    "USDKRW": "usdkrw",
    "USDHKD": "usdhkd",
}

# AkShare's Eastmoney global-index/FX frames occasionally return an empty
# frame while futures remain healthy.  Yahoo's chart endpoint is used only as
# a per-symbol fallback for the missing fields.  The timestamp is validated
# against the requested capture time, so a replay cannot consume a quote that
# was published after its point-in-time cutoff.
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
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _as_date(value: Any, default: date | None = None) -> date:
    parsed = _as_datetime(value)
    return parsed.date() if parsed else (default or datetime.now().date())


def _cell(row: dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def _frame_rows(frame: Any) -> list[dict[str, Any]]:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _find_row(
    rows: list[dict[str, Any]],
    *,
    code_names: tuple[str, ...],
    codes: Iterable[str],
    name_names: tuple[str, ...] = ("名称", "name", "品种名称"),
    name_tokens: Iterable[str] = (),
) -> dict[str, Any] | None:
    wanted = {str(code).strip().upper() for code in codes}
    for row in rows:
        code = str(_cell(row, code_names) or "").strip().upper()
        if code in wanted:
            return row
    tokens = tuple(str(token).lower() for token in name_tokens if token)
    if tokens:
        for row in rows:
            name = str(_cell(row, name_names) or "").lower()
            if any(token in name for token in tokens):
                return row
    return None


def _item(
    symbol: str,
    display_name: str,
    row: dict[str, Any] | None,
    *,
    source: str,
    price_names: tuple[str, ...] = ("最新价", "现价", "price", "close", "收盘价"),
    change_names: tuple[str, ...] = ("涨跌幅", "change_pct", "涨幅", "涨跌幅(%)"),
    previous_names: tuple[str, ...] = ("昨收价", "昨结", "previous_close", "前收", "昨收"),
    time_names: tuple[str, ...] = ("最新行情时间", "更新时间", "行情时间", "时间", "date", "日期"),
    raw_code: str = "",
) -> dict[str, Any]:
    row = row or {}
    price = _number(_cell(row, price_names))
    change_pct = _number(_cell(row, change_names))
    previous_close = _number(_cell(row, previous_names))
    market_time = _as_datetime(_cell(row, time_names))
    availability = "available" if price is not None or change_pct is not None else "missing"
    return {
        "symbol": symbol,
        "display_name": display_name,
        "price": price,
        "change_pct": change_pct,
        "previous_close": previous_close,
        "market_time": market_time.isoformat(sep=" ") if market_time else None,
        "availability": availability,
        "source": source,
        "raw_code": raw_code,
        "payload": {str(k): v for k, v in row.items()} if row else {},
    }


def _load_akshare_frames() -> tuple[dict[str, Any], list[str]]:
    """Load each remote frame independently so one unavailable endpoint is non-fatal."""
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        return {}, [f"akshare import failed: {exc}"]

    frames: dict[str, Any] = {}
    errors: list[str] = []
    loaders = {
        "index": "index_global_spot_em",
        "futures": "futures_global_spot_em",
        "forex": "forex_spot_em",
        "bond": "bond_zh_us_rate",
    }
    for key, function_name in loaders.items():
        try:
            function = getattr(ak, function_name)
            frames[key] = function()
        except Exception as exc:  # pragma: no cover - source failures vary by day
            errors.append(f"{function_name}: {exc}")
            logger.warning("External market source %s failed: %s", function_name, exc)
    return frames, errors


def _parse_eastmoney_vix_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Parse Eastmoney's VIX quote (secid=167.VIX).

    Eastmoney returns prices and changes in hundredths for this index.
    Keeping the parser separate makes the scale explicit and testable.
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None
    price_raw = _number(data.get("f43"))
    if price_raw is None:
        return None
    previous_raw = _number(data.get("f60"))
    change_pct_raw = _number(data.get("f170"))
    price = price_raw / 100.0 if abs(price_raw) >= 100.0 else price_raw
    previous = previous_raw / 100.0 if previous_raw is not None and abs(previous_raw) >= 100.0 else previous_raw
    change_pct = change_pct_raw / 100.0 if change_pct_raw is not None and abs(change_pct_raw) >= 10.0 else change_pct_raw
    market_time = None
    timestamp = _number(data.get("f86"))
    if timestamp is not None and timestamp > 1_000_000_000:
        try:
            market_time = datetime.fromtimestamp(timestamp).isoformat(sep=" ")
        except (OverflowError, OSError, ValueError):
            market_time = None
    return {
        "symbol": "vix",
        "display_name": "VIX恐慌指数",
        "price": price,
        "change_pct": change_pct,
        "previous_close": previous,
        "market_time": market_time,
        "availability": "available",
        "source": "eastmoney.quote.167.VIX",
        "raw_code": "167.VIX",
        "payload": data,
    }


def _fetch_eastmoney_vix_item() -> dict[str, Any] | None:
    """Fetch the VIX quote through the production-accessible Eastmoney endpoint."""
    try:
        from urllib.parse import urlencode
        from urllib.request import Request, urlopen

        query = urlencode({
            "invt": "2",
            "fltt": "1",
            "fields": "f43,f44,f45,f46,f60,f86,f169,f170,f58,f57",
            "secid": "167.VIX",
        })
        request = Request(
            f"https://push2.eastmoney.com/api/qt/stock/get?{query}",
            headers={"User-Agent": "Mozilla/5.0 ProBigA external-market"},
        )
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
        return _parse_eastmoney_vix_payload(payload)
    except Exception as exc:  # pragma: no cover - network failures vary by run
        logger.warning("Eastmoney VIX fallback failed: %s", exc)
        return None


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
    cutoff_ts = captured_at.timestamp()
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
    if price is None:
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
    market_time = None
    if selected_ts is not None:
        try:
            market_time = datetime.fromtimestamp(float(selected_ts)).isoformat(sep=" ")
        except (OverflowError, OSError, ValueError):
            market_time = None
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
    try:
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{quote(raw_code, safe='')}?interval=1d&range=10d"
        )
        request = Request(url, headers={"User-Agent": "Mozilla/5.0 ProBigA external-market"})
        with urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
        return _parse_yahoo_chart_payload(
            symbol,
            display_name,
            raw_code,
            payload,
            captured_at=captured_at,
        )
    except Exception as exc:  # pragma: no cover - network failures vary by run
        logger.warning("Yahoo external fallback %s failed: %s", raw_code, exc)
        return None


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
            except Exception as exc:  # defensive: worker should already absorb failures
                errors.append(f"yahoo {symbol}: {exc}")
                continue
            if item is None:
                errors.append(f"yahoo {symbol}: no point-in-time quote")
            else:
                items[symbol] = item
    return items, errors


def _index_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for code, symbol in _INDEX_MAP.items():
        if symbol in by_symbol:
            continue
        name_tokens = {
            "nasdaq": ("纳斯达克", "nasdaq", "纳指"),
            "sp500": ("标普", "s&p", "sp500", "标普500"),
            "dow": ("道琼斯", "dow"),
            "nikkei": ("日经", "nikkei"),
            "kospi": ("kospi", "韩国综合", "首尔"),
            "hang_seng": ("恒生", "hang seng"),
        "taiwan": ("台湾加权", "台湾证券交易所", "twse"),
            "a50": ("a50", "富时中国"),
            "vix": ("vix", "恐慌指数"),
        }.get(symbol, ())
        row = _find_row(
            rows,
            code_names=("代码", "code", "symbol", "指数代码"),
            codes=(code,),
            name_tokens=name_tokens,
        )
        if row is not None:
            by_symbol[symbol] = _item(
                symbol,
                dict(EXTERNAL_MARKET_SYMBOLS).get(symbol, symbol),
                row,
                source="akshare.index_global_spot_em",
                raw_code=code,
            )
    return list(by_symbol.values())


def _latest_non_null_bond_rows(
    rows: list[dict[str, Any]], as_of: date
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    candidates: list[tuple[date, dict[str, Any]]] = []
    for row in rows:
        row_date = _as_date(_cell(row, ("日期", "date", "period")), default=None)
        if row_date > as_of:
            continue
        if _number(_cell(row, ("美国国债收益率10年", "美国国债10年", "US10Y", "10年"))) is not None:
            candidates.append((row_date, row))
    ordered = sorted(candidates, key=lambda pair: pair[0])
    return (
        ordered[-1][1] if ordered else None,
        ordered[-2][1] if len(ordered) >= 2 else None,
    )


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
    if core_available >= 3 and len(available) >= 10:
        return "PASS"
    if proxy_available >= 2 and len(available) >= 5:
        return "WATCH"
    return "WATCH" if available else "UNKNOWN"


def fetch_external_market_snapshot(as_of: datetime | None = None) -> dict[str, Any]:
    """Fetch the current external snapshot, retaining explicit missing items."""
    captured_at = as_of or datetime.now().replace(microsecond=0)
    context_date = captured_at.date()
    frames, errors = _load_akshare_frames()
    index_rows = _frame_rows(frames.get("index"))
    futures_rows = _frame_rows(frames.get("futures"))
    forex_rows = _frame_rows(frames.get("forex"))
    bond_rows = _frame_rows(frames.get("bond"))

    items = _index_items(index_rows)
    existing = {item["symbol"] for item in items}
    if "vix" not in existing:
        vix_item = _fetch_eastmoney_vix_item()
        if vix_item:
            items.append(vix_item)
            existing.add("vix")
    for symbol, display_name in EXTERNAL_MARKET_SYMBOLS:
        if symbol in existing:
            continue
        item: dict[str, Any] | None = None
        if symbol in _FUTURES_MAP.values():
            code = next(code for code, mapped in _FUTURES_MAP.items() if mapped == symbol)
            row = _find_row(
                futures_rows,
                code_names=("代码", "code", "symbol", "期货代码"),
                codes=(code,),
            )
            item = _item(symbol, display_name, row, source="akshare.futures_global_spot_em", raw_code=code)
        elif symbol in _FOREX_MAP.values():
            code = next(code for code, mapped in _FOREX_MAP.items() if mapped == symbol)
            row = _find_row(
                forex_rows,
                code_names=("代码", "code", "symbol", "货币对"),
                codes=(code,),
            )
            item = _item(symbol, display_name, row, source="akshare.forex_spot_em", raw_code=code)
        elif symbol == "us10y":
            row, previous_row = _latest_non_null_bond_rows(bond_rows, context_date)
            if row is not None and previous_row is not None:
                latest_yield = _number(_cell(row, ("美国国债收益率10年", "美国国债10年", "US10Y", "10年")))
                previous_yield = _number(_cell(previous_row, ("美国国债收益率10年", "美国国债10年", "US10Y", "10年")))
                if latest_yield is not None and previous_yield not in (None, 0.0):
                    row = dict(row)
                    row["change_pct"] = (latest_yield - previous_yield) / abs(previous_yield) * 100.0
                    row["前值"] = previous_yield
            item = _item(
                symbol,
                display_name,
                row,
                source="akshare.bond_zh_us_rate",
                price_names=("美国国债收益率10年", "美国国债10年", "US10Y", "10年"),
                change_names=("涨跌幅", "change_pct"),
                previous_names=("前值", "previous_value"),
                time_names=("日期", "date"),
                raw_code="US10Y",
            )
        else:
            item = _item(symbol, display_name, None, source="akshare.index_global_spot_em", raw_code=symbol.upper())
        items.append(item)

    missing_fallback_symbols = [
        str(item.get("symbol") or "")
        for item in items
        if item.get("availability") != "available"
        and str(item.get("symbol") or "") in _YAHOO_FALLBACK_MAP
    ]
    yahoo_items, yahoo_errors = _load_yahoo_fallback_items(
        missing_fallback_symbols,
        captured_at=captured_at,
    )
    if yahoo_items:
        items = [
            yahoo_items.get(str(item.get("symbol") or ""), item)
            for item in items
        ]
    errors.extend(yahoo_errors)

    score, status, reason = _score_snapshot(items)
    available_count = sum(item.get("availability") == "available" for item in items)
    quality = _snapshot_quality(items)
    if errors:
        reason = f"{reason}; source warnings: {' | '.join(errors[:2])}" if reason else "; ".join(errors[:2])
    return {
        "snapshot_id": str(uuid.uuid4()),
        "context_date": context_date.isoformat(),
        "captured_at": captured_at,
        "source": "akshare_eastmoney+yahoo_finance" if yahoo_items else "akshare_eastmoney",
        "items": items,
        "external_market_score": score,
        "external_market_status": status,
        "external_market_reason": reason or "外围数据暂无可用结果",
        "external_market_data_quality": quality,
        "available_count": int(available_count),
        "expected_count": len(EXTERNAL_MARKET_SYMBOLS),
        "source_warnings": errors,
    }


def ensure_external_market_table(engine: Engine) -> None:
    """Create the append-only external snapshot table when needed."""
    with engine.begin() as conn:
        conn.execute(text("""
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


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
            })
        score, status, reason = _score_snapshot(items)
        available_count = sum(item.get("availability") == "available" for item in items)
        quality = _snapshot_quality(items)
        item_sources = sorted({str(item.get("source") or "").strip() for item in items if item.get("source")})
        return {
            "external_market_status": status,
            "external_market_score": score if score is not None else 50.0,
            "external_market_reason": reason,
            "external_market_data_quality": quality,
            "external_market_captured_at": str(rows.iloc[0].get("captured_at") or "")[:19],
            "external_market_source": "+".join(item_sources) or "unknown",
            "external_market_items_json": json.dumps(items, ensure_ascii=False, default=str),
        }
    except Exception as exc:  # data enrichment must never block the base recommendation
        logger.warning("External market context load skipped: %s", exc)
        return {**defaults, "external_market_reason": f"外围市场数据不可用：{exc}"}
