"""Sina's dated Level-1 money flow, using its public historical distribution API.

Native documentation: https://finance.sina.com.cn/temp/guest4377.shtml
Native client: https://n.sinaimg.cn/finance/cnstock/pc/zjlx.z.js
The API sends CNY; its client divides values by 10,000 for display. r0/r1/r2/r3
are >1m / 200k-1m / 50k-200k / <50k CNY trades. Thus the native labels 小单 and
散单 map to our middle and small size buckets. Our common main bucket is r0+r1,
not Sina's differently defined main label and not its all-size netamount.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import re
import threading
import time
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import requests

ENDPOINT = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"
SOURCE = "sina_l1"
PAGE_SIZE = 100
_RATE_LOCK = threading.Lock()
_NEXT_REQUEST = 0.0


def _request_slot():
    global _NEXT_REQUEST
    with _RATE_LOCK:
        now = time.monotonic()
        delay = max(0.0, _NEXT_REQUEST - now)
        _NEXT_REQUEST = max(now, _NEXT_REQUEST) + 0.25
    if delay:
        time.sleep(delay)


def sina_symbol(stock_code: str) -> str:
    if not re.fullmatch(r"(?:00|30|60|68|43|83|87|92)\d{4}", stock_code):
        raise ValueError("Sina flow requires one supported A-share code")
    prefix = "sh" if stock_code.startswith("6") else "sz" if stock_code.startswith(("0", "3")) else "bj"
    return prefix + stock_code


def _number(row, field):
    raw = row.get(field)
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"Sina flow component missing: {field}")
    try:
        value = Decimal(str(raw))
    except InvalidOperation as exc:
        raise ValueError(f"Sina flow component invalid: {field}") from exc
    if not value.is_finite():
        raise ValueError(f"Sina flow component nonfinite: {field}")
    return value


def parse_history(payload, *, stock_code: str, response_url: str, page: int) -> list[dict]:
    """Bind the data to the actual HTTPS request and native dated rows.

Sina returns a list without repeating a symbol. Unlike an adapter that simply
copies its argument, this requires the actual non-redirected response URL to
bind the requested market/code, page, ordering and history method. No content
claim of a source-side symbol is made. Any echoed symbol must also match.
"""
    symbol = sina_symbol(stock_code)
    url, expected = urlsplit(response_url), urlsplit(ENDPOINT)
    query = parse_qs(url.query)
    required = {"daima": [symbol], "page": [str(page)], "num": [str(PAGE_SIZE)],
                "sort": ["opendate"], "asc": ["0"]}
    if (url.scheme, url.netloc, url.path) != (expected.scheme, expected.netloc, expected.path) or any(
        query.get(key) != value for key, value in required.items()
    ):
        raise ValueError("Sina flow response request identity differs")
    if not isinstance(payload, list) or len(payload) > PAGE_SIZE:
        raise ValueError("Sina flow historical response is not a bounded list")
    rows, dates = [], []
    for raw in payload:
        if not isinstance(raw, dict) or ("symbol" in raw and raw["symbol"] != symbol):
            raise ValueError("Sina flow historical row identity differs")
        day = str(raw.get("opendate") or "")
        if date.fromisoformat(day).isoformat() != day or day in dates:
            raise ValueError("Sina flow historical date invalid or duplicated")
        dates.append(day)
        buckets = [_number(raw, f"r{i}_net") for i in range(4)]
        totals = [_number(raw, f"r{i}") for i in range(4)]
        if any(total < 0 or abs(net) > total + Decimal("0.05") for net, total in zip(buckets, totals)):
            raise ValueError("Sina flow net exceeds native bucket turnover")
        if abs(sum(buckets) - _number(raw, "netamount")) > Decimal("0.05"):
            raise ValueError("Sina flow native total disagrees with four buckets")
        rows.append({"stock_code": stock_code, "trade_date": day,
                     "main_net_inflow": float(buckets[0] + buckets[1]),
                     "max_net_inflow": float(buckets[0]), "lg_net_inflow": float(buckets[1]),
                     "mid_net_inflow": float(buckets[2]), "sm_net_inflow": float(buckets[3]),
                     "data_source": SOURCE})
    if dates != sorted(dates, reverse=True):
        raise ValueError("Sina flow historical ordering differs")
    return rows


@lru_cache(maxsize=2048)
def _history_page(stock_code: str, page: int, observation_day: str) -> tuple[dict, ...]:
    # A repair of several dates reuses one source history request per stock.
    # The observation-day key expires the cache across trading days.
    with requests.Session() as session:
        session.trust_env = False
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://money.finance.sina.com.cn/moneyflow/"})
        for attempt in range(3):
            try:
                _request_slot()
                response = session.get(ENDPOINT, params={"daima": sina_symbol(stock_code), "page": page,
                    "num": PAGE_SIZE, "sort": "opendate", "asc": 0}, timeout=15, allow_redirects=False)
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                status = getattr(exc.response, "status_code", None)
                if attempt == 2 or status not in {None, 408, 429, 500, 502, 503, 504}:
                    raise
                time.sleep(attempt + 1)
        if response.status_code != 200:
            raise ValueError("Sina flow request redirected instead of returning history")
        return tuple(parse_history(response.json(), stock_code=stock_code, response_url=response.url, page=page))


def fetch_sina_flow_row(stock_code: str, trade_date: str) -> dict | None:
    target = date.fromisoformat(trade_date).isoformat()
    observed = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    if target > observed:
        raise ValueError("Sina flow target is in the future")
    # The bounded repair window is 21 sessions; allow older suspended pages,
    # while never looping indefinitely on an empty or repeated source window.
    last_date = None
    for page in range(1, 5):
        rows = _history_page(stock_code, page, observed)
        if not rows:
            return None
        if last_date is not None and rows[0]["trade_date"] >= last_date:
            raise ValueError("Sina flow historical pages overlap or repeat")
        for row in rows:
            if row["trade_date"] == target:
                return dict(row)
        last_date = rows[-1]["trade_date"]
        if target >= last_date or len(rows) < PAGE_SIZE:
            return None
    return None
