"""Small direct Eastmoney adapters returning complete per-security raw units.

No database, task history, release identity, or legacy collector dependencies.
The injected client has requests' get() interface and no automatic retries.
Socket timeouts limit each I/O phase; the shared deadline also rejects a late
response and prevents starting another page/attempt. It is not thread killing.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
import json
import math
import re
import time
from zoneinfo import ZoneInfo

import requests

SHANGHAI = ZoneInfo("Asia/Shanghai")
FINANCE_URL = "https://datacenter.eastmoney.com/securities/api/data/get"
REPORT_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
NOTICE_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"
PAGE_SIZE = 100
MAX_PAGES = 1000

FINANCE_FIELDS = {
    "SECURITY_CODE": "stock_code", "SECURITY_NAME_ABBR": "short_name",
    "REPORT_DATE": "report_date", "REPORT_TYPE": "report_type",
    "NOTICE_DATE": "notice_date", "UPDATE_DATE": "source_update_date",
    "EPSJB": "basic_eps", "EPSXS": "diluted_eps", "EPSKCJB": "non_gaap_eps",
    "BPS": "net_asset_ps", "MGZBGJ": "cap_reserve_ps", "MGWFPLR": "undist_profit_ps",
    "MGJYXJJE": "oper_cf_ps", "TOTALOPERATEREVE": "total_rev", "MLR": "gross_profit",
    "PARENTNETPROFIT": "net_profit_attr_sh", "KCFJCXSYJLR": "non_gaap_net_profit",
    "TOTALOPERATEREVETZ": "total_rev_yoy_gr", "PARENTNETPROFITTZ": "net_profit_yoy_gr",
    "KCFJCXSYJLRTZ": "non_gaap_net_profit_yoy_gr", "YYZSRGDHBZC": "total_rev_qoq_gr",
    "NETPROFITRPHBZC": "net_profit_qoq_gr", "ROEJQ": "roe_wtd",
    "ROEKCJQ": "roe_non_gaap_wtd", "ZZCJLL": "roa_wtd", "XSMLL": "gross_margin",
    "XSJLL": "net_margin", "LD": "curr_ratio", "SD": "quick_ratio",
    "XJLLB": "cash_flow_ratio", "ZCFZL": "asset_liab_ratio",
}
ALIST_FIELDS = {
    "TRADE_DATE": "trade_date", "SECURITY_CODE": "stock_code",
    "SECURITY_NAME_ABBR": "short_name", "TRADE_ID": "trade_id",
    "CLOSE_PRICE": "close", "CHANGE_RATE": "change_cpt", "TURNOVERRATE": "turnover_ratio",
    "BILLBOARD_NET_AMT": "a_net_amount", "BILLBOARD_BUY_AMT": "a_buy_amount",
    "BILLBOARD_SELL_AMT": "a_sell_amount", "BILLBOARD_DEAL_AMT": "a_amount",
    "ACCUM_AMOUNT": "amount", "DEAL_NET_RATIO": "net_amount_rate",
    "DEAL_AMOUNT_RATIO": "a_amount_rate", "EXPLANATION": "reason",
}
DETAIL_FIELDS = {
    "TRADE_DATE": "trade_date", "SECURITY_CODE": "stock_code", "TRADE_ID": "trade_id",
    "OPERATEDEPT_CODE": "operate_code", "OPERATEDEPT_NAME": "operate_name",
    "NET": "a_net_amount", "BUY": "a_buy_amount", "SELL": "a_sell_amount",
    "TOTAL_BUYRIO": "a_buy_amount_rate", "TOTAL_SELLRIO": "a_sell_amount_rate",
    "EXPLANATION": "reason",
}
METHODS = {
    "finance": "eastmoney.RPT_F10_FINANCE_MAINFINADATA",
    "alist_daily": "eastmoney.RPT_DAILYBILLBOARD_DETAILSNEW",
    "alist_detail": "eastmoney.RPT_BILLBOARD_DAILYDETAILSBUY+SELL",
    "notices": "eastmoney.security.ann",
}


class ProviderError(Exception):
    def __init__(self, code: str, reason: str, *, source_wide: bool = False):
        super().__init__(reason)
        self.code, self.reason, self.source_wide = code, reason, source_wide


def _integer(value, field: str) -> int:
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        raise ProviderError("INVALID_PAGINATION", f"Invalid {field}")
    return int(value)


def _day(value) -> str:
    if not isinstance(value, str):
        raise ProviderError("INVALID_DATE", "Source/request date is missing or invalid")
    try:
        if len(value) == 10:
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError()
            return value
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError as exc:
        raise ProviderError("INVALID_DATE", "Source/request date is missing or invalid") from exc


def _instant(value) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        return parsed.astimezone(SHANGHAI)
    except (ValueError, TypeError) as exc:
        raise ProviderError("INVALID_REQUEST", "Request timestamps require an explicit timezone") from exc


def _notice_time(value: str) -> datetime:
    """Parse Eastmoney's native display timestamp, including colon milliseconds."""
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{1,6}", value):
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S:%f")
        else:
            parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    except ValueError as exc:
        raise ProviderError("INVALID_SOURCE_TIME", "Notice display timestamp is invalid") from exc


def _error(exc: ProviderError) -> dict:
    return {"status": "error", "rows": [], "error_code": exc.code, "reason": exc.reason}


def _data(rows: list[dict], *, empty_event: bool = False) -> dict:
    if rows:
        return {"status": "data", "rows": rows}
    if empty_event:
        return {"status": "no_data", "rows": [], "reason": "empty_event_set"}
    return _error(ProviderError("SOURCE_DATA_MISSING", "Source did not establish the requested data"))


class EastmoneyProvider:
    def __init__(self, client=None, clock=None, sleep=None):
        if client is None:
            client = requests.Session()
            client.trust_env = False
            # Explicitly disable library retries; only this adapter retries.
            client.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        self.client = client
        self.clock = clock or (lambda: datetime.now(SHANGHAI))
        self.sleep = sleep or time.sleep
        self._date_cache = {}

    def _now(self) -> datetime:
        return _instant(self.clock().isoformat())

    def _remaining(self, deadline: datetime) -> float:
        remaining = (deadline - self._now()).total_seconds()
        if remaining <= 0:
            raise ProviderError("DEADLINE_EXCEEDED", "Batch deadline was reached")
        return remaining

    def _retry_delay(self, response, attempt: int) -> float:
        delay = (2, 10)[attempt]
        raw = response.headers.get("Retry-After") if response is not None else None
        if raw:
            try:
                seconds = float(raw)
                if not math.isfinite(seconds) or seconds < 0:
                    raise ValueError()
            except ValueError:
                try:
                    when = parsedate_to_datetime(raw)
                    if when.tzinfo is None:
                        raise ValueError()
                    seconds = max(0.0, (when - self._now()).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    raise ProviderError("INVALID_RETRY_AFTER", "Invalid server retry deadline", source_wide=True)
            delay = max(delay, seconds)
        return delay

    def _get(self, url: str, params: dict, deadline: datetime) -> dict:
        for attempt in range(3):
            remaining = self._remaining(deadline)
            response = None
            retry = False
            try:
                response = self.client.get(
                    url, params=params,
                    headers={"Accept": "application/json", "User-Agent": "ProBigA-DirectAcquisition/1.0",
                             "Referer": "https://data.eastmoney.com/"},
                    timeout=(min(5.0, remaining / 2), min(30.0, remaining / 2)),
                    allow_redirects=False,
                )
                self._remaining(deadline)
                status = int(response.status_code)
                if status == 429 or status in {500, 502, 503, 504}:
                    retry = True
                elif status in {401, 403}:
                    raise ProviderError("SOURCE_ACCESS_DENIED", "Source rejected access", source_wide=True)
                elif status != 200:
                    raise ProviderError("HTTP_STATUS", f"Unexpected HTTP status {status}")
                else:
                    try:
                        # Preserve JSON decimal tokens before any binary-float
                        # conversion; the normalizer owns Decimal and scales.
                        payload = response.json(parse_float=str)
                    except (ValueError, TypeError):
                        raise ProviderError("INVALID_JSON", "Source returned invalid JSON")
                    self._remaining(deadline)
                    if not isinstance(payload, dict):
                        raise ProviderError("INVALID_RESPONSE", "Source response is not an object")
                    return payload
            except (requests.Timeout, requests.ConnectionError, TimeoutError, ConnectionError):
                retry = True
            finally:
                # No credentials, response bodies, or full exception URLs in errors.
                if response is not None:
                    response.close()
            if retry:
                if attempt == 2:
                    raise ProviderError("SOURCE_UNAVAILABLE", "Source unavailable after three bounded attempts", source_wide=True)
                delay = self._retry_delay(response, attempt)
                if delay >= self._remaining(deadline):
                    raise ProviderError("DEADLINE_EXCEEDED", "Retry delay exceeds remaining batch budget", source_wide=True)
                self.sleep(delay)
        raise AssertionError("unreachable")

    def _report_pages(self, url: str, params: dict, deadline: datetime, *, finance=False) -> list[dict]:
        page_key, size_key = ("p", "ps") if finance else ("pageNumber", "pageSize")
        rows, fingerprints = [], set()
        declared = None
        for page in range(1, MAX_PAGES + 1):
            payload = self._get(url, {**params, page_key: page, size_key: PAGE_SIZE}, deadline)
            if (page == 1 and payload.get("success") is False
                    and str(payload.get("code")) == "9201" and payload.get("result") is None):
                return []
            result = payload.get("result")
            if payload.get("success") is not True or not isinstance(result, dict):
                raise ProviderError("INVALID_RESPONSE", "Source report did not establish success")
            total = _integer(result.get("count"), "count")
            pages = _integer(result.get("pages"), "pages")
            if pages > MAX_PAGES:
                raise ProviderError("PAGINATION_LIMIT", "Source report exceeds the supported page budget")
            if (total == 0 and pages not in {0, 1}) or (total > 0 and pages < 1):
                raise ProviderError("INVALID_PAGINATION", "Source page count disagrees with total")
            if declared is not None and declared != (total, pages):
                raise ProviderError("PAGINATION_CHANGED", "Source report changed during pagination")
            declared = total, pages
            part = result.get("data")
            if part is None and total == 0:
                part = []
            if not isinstance(part, list) or any(not isinstance(item, dict) for item in part):
                raise ProviderError("INVALID_RESPONSE", "Source report rows are missing or invalid")
            for item in part:
                fingerprint = json.dumps(item, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                if fingerprint in fingerprints:
                    raise ProviderError("PAGINATION_DUPLICATE", "Duplicate source row across report pages")
                fingerprints.add(fingerprint)
            rows.extend(part)
            if page >= pages:
                if len(rows) != total:
                    raise ProviderError("PAGINATION_INCOMPLETE", "Source rows do not match the declared total")
                return rows
            if not part or len(rows) >= total:
                raise ProviderError("PAGINATION_INCOMPLETE", "Source report ended before its declared final page")
        raise ProviderError("PAGINATION_LIMIT", "Source report is incomplete")

    @staticmethod
    def _source_symbol(raw: dict) -> str | None:
        code = str(raw.get("SECURITY_CODE") or "")
        symbol = str(raw.get("SECUCODE") or "").upper()
        if symbol.endswith(".NQ"):
            # Eastmoney uses NQ for Beijing/NEEQ securities while the project
            # and QMT use the canonical BJ suffix.
            symbol = symbol[:-3] + ".BJ"
        return symbol if re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", symbol) and symbol[:6] == code else None

    def _finance_for_date(self, request: dict, deadline: datetime) -> dict[str, list[dict]]:
        key = ("finance", request["end_date"])
        if key in self._date_cache:
            return self._date_cache[key]
        grouped, identities = {}, {}
        for field in ("NOTICE_DATE", "UPDATE_DATE"):
            raw_rows = self._report_pages(FINANCE_URL, {
                "type": "RPT_F10_FINANCE_MAINFINADATA", "sty": "APP_F10_MAINFINADATA",
                "filter": f"({field}='{request['end_date']} 00:00:00')",
                "st": f"{field},SECURITY_CODE,REPORT_DATE,REPORT_TYPE", "sr": "1,1,-1,1",
                "source": "HSF10", "client": "PC"}, deadline, finance=True)
            for raw in raw_rows:
                source_code = str(raw.get("SECURITY_CODE") or "")
                if not re.fullmatch(r"\d{6}", source_code):
                    continue  # The report also contains non-listed application identifiers.
                symbol = self._source_symbol(raw)
                if symbol is None:
                    raise ProviderError("SECURITY_MISMATCH", "Finance issuer identity is invalid")
                if _day(raw.get(field)) != request["end_date"]:
                    raise ProviderError("SOURCE_DATE_MISMATCH", "Finance discovery date differs from request")
                report_day = _day(raw.get("REPORT_DATE"))
                report_type = raw.get("REPORT_TYPE")
                if report_day > request["end_date"] or report_type in (None, ""):
                    raise ProviderError("INVALID_RESPONSE", "Finance report identity is invalid")
                for source_field in ("NOTICE_DATE", "UPDATE_DATE"):
                    if raw.get(source_field) not in (None, ""):
                        _day(raw[source_field])
                row = {target: raw[source] for source, target in FINANCE_FIELDS.items() if source in raw}
                row.update({"stock_code": symbol[:6], "symbol": symbol,
                            "report_date": report_day, "raw": raw})
                identity = symbol, report_day, str(report_type)
                encoded = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
                previous = identities.get(identity)
                if previous is not None and previous != encoded:
                    raise ProviderError("DUPLICATE_IDENTITY", "Finance discovery returned conflicting revisions")
                if previous is None:
                    identities[identity] = encoded
                    grouped.setdefault(symbol, []).append(row)
        self._date_cache[key] = grouped
        return grouped

    def _alist_for_date(self, dataset: str, request: dict, deadline: datetime) -> dict[str, list[dict]]:
        key = (dataset, request["end_date"])
        if key in self._date_cache:
            return self._date_cache[key]
        reports = ["RPT_DAILYBILLBOARD_DETAILSNEW"] if dataset == "alist_daily" else ["RPT_BILLBOARD_DAILYDETAILSBUY", "RPT_BILLBOARD_DAILYDETAILSSELL"]
        grouped, identities = {}, set()
        for report in reports:
            rows = self._report_pages(REPORT_URL, {"reportName": report, "columns": "ALL",
                "filter": f"(TRADE_DATE='{request['end_date']}')",
                "sortColumns": "SECURITY_CODE,TRADE_DATE,TRADE_ID" if dataset == "alist_daily" else "SECURITY_CODE,TRADE_ID,OPERATEDEPT_CODE,OPERATEDEPT_NAME",
                "sortTypes": "1,-1,1" if dataset == "alist_daily" else "1,1,1,1",
                "source": "WEB", "client": "WEB"}, deadline)
            for raw in rows:
                symbol = self._source_symbol(raw)
                if symbol is None:
                    raise ProviderError("SECURITY_MISMATCH", "Billboard security identity is invalid")
                if _day(raw.get("TRADE_DATE")) != request["end_date"]:
                    raise ProviderError("SOURCE_DATE_MISMATCH", "Billboard date differs from request")
                if not str(raw.get("EXPLANATION") or "").strip():
                    raise ProviderError("INVALID_RESPONSE", "Billboard reason is missing")
                mapping = ALIST_FIELDS if dataset == "alist_daily" else DETAIL_FIELDS
                row = {target: raw[source] for source, target in mapping.items() if source in raw}
                row.update({"stock_code": symbol[:6], "symbol": symbol, "trade_date": request["end_date"], "raw": raw})
                if dataset == "alist_detail":
                    if not raw.get("OPERATEDEPT_CODE") or not raw.get("OPERATEDEPT_NAME"):
                        raise ProviderError("INVALID_RESPONSE", "Billboard department identity is missing")
                    row["report_side"] = "BUY" if report.endswith("BUY") else "SELL"
                identity = (json.dumps({key: value for key, value in row.items() if key != "raw"},
                                       ensure_ascii=False, sort_keys=True, default=str)
                            if dataset == "alist_detail"
                            else (symbol, str(row.get("trade_id"))))
                if identity in identities:
                    raise ProviderError("DUPLICATE_IDENTITY", "Billboard event identity is duplicated")
                identities.add(identity)
                grouped.setdefault(symbol, []).append(row)
        self._date_cache[key] = grouped
        return grouped

    def _notices_for_date(self, request: dict, deadline: datetime) -> dict[str, list[dict]]:
        key = ("notices", request["start_date"], request["end_date"])
        if key in self._date_cache:
            return self._date_cache[key]
        grouped, seen = {}, set()
        total = None
        observed = 0
        for page in range(1, MAX_PAGES + 1):
            payload = self._get(NOTICE_URL, {"sr": -1, "page_size": PAGE_SIZE, "page_index": page,
                "ann_type": "A", "client_source": "web", "f_node": 0, "s_node": 0,
                "stock_list": "", "begin_time": request["start_date"], "end_time": request["end_date"]}, deadline)
            data = payload.get("data")
            if payload.get("success") != 1 or not isinstance(data, dict):
                raise ProviderError("INVALID_RESPONSE", "Notice source did not establish success")
            count = _integer(data.get("total_hits"), "total_hits")
            if (_integer(data.get("page_index"), "page_index") != page
                    or _integer(data.get("page_size"), "page_size") != PAGE_SIZE):
                raise ProviderError("INVALID_PAGINATION", "Notice page identity differs")
            if total is not None and total != count:
                raise ProviderError("PAGINATION_CHANGED", "Notice source changed during pagination")
            total = count
            items = data.get("list")
            if items is None and total == 0:
                items = []
            if not isinstance(items, list):
                raise ProviderError("INVALID_RESPONSE", "Notice list is missing")
            for raw in items:
                if not isinstance(raw, dict) or not isinstance(raw.get("codes"), list):
                    raise ProviderError("INVALID_RESPONSE", "Notice security associations are missing")
                # The all-market feed also carries pre-listing application
                # identifiers such as A12345. They are outside this A-share
                # dataset; retain only canonical six-digit associations.
                associations = {item["stock_code"] for item in raw["codes"]
                                if isinstance(item, dict)
                                and isinstance(item.get("stock_code"), str)
                                and re.fullmatch(r"\d{6}", item["stock_code"])}
                if not associations:
                    continue
                art_code = raw.get("art_code")
                if not isinstance(art_code, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", art_code) or art_code in seen:
                    raise ProviderError("DUPLICATE_IDENTITY", "Notice article identity is missing or duplicated")
                seen.add(art_code)
                notice_day = _day(raw.get("notice_date"))
                if not request["start_date"] <= notice_day <= request["end_date"]:
                    raise ProviderError("SOURCE_DATE_MISMATCH", "Notice date is outside the requested range")
                title = raw.get("title") or raw.get("title_ch")
                if not isinstance(title, str) or not title.strip():
                    raise ProviderError("INVALID_RESPONSE", "Notice title is missing")
                columns = raw.get("columns") or []
                row = {"art_code": art_code, "notice_date": notice_day,
                    "title": title, "display_time": raw.get("display_time"),
                    "column_name": ",".join(str(item.get("column_name") or "") for item in columns if isinstance(item, dict)),
                    "source_security_codes": sorted(str(code) for code in associations if code), "raw": raw}
                # No received-at fallback for absent publication time.
                if raw.get("display_time"):
                    value = str(raw["display_time"])
                    if len(value) == 10:
                        # A date is not evidence of an exact display time.
                        if _day(value) > self._now().date().isoformat():
                            raise ProviderError("INVALID_SOURCE_TIME", "Notice display date is in the future")
                    else:
                        display = _notice_time(value)
                        if display > self._now():
                            raise ProviderError("INVALID_SOURCE_TIME", "Notice display timestamp is in the future")
                        row["display_time"] = display.isoformat()
                        row["source_time"] = display.isoformat()
                for code in associations:
                    grouped.setdefault(code, []).append(row)
            observed += len(items)
            if observed == total:
                self._date_cache[key] = grouped
                return grouped
            if not items or observed > total:
                raise ProviderError("PAGINATION_INCOMPLETE", "Notice pagination ended before declared total")
        raise ProviderError("PAGINATION_LIMIT", "Notice pagination exceeds the page budget")

    def fetch_batch(self, dataset: str, request: dict) -> dict:
        frozen = deepcopy(request)
        symbols = list(frozen.get("codes", []))
        outcomes = {}
        result = {"request": frozen, "source_method": METHODS.get(dataset, "eastmoney.unsupported"), "outcomes": outcomes}
        try:
            if (dataset not in METHODS or frozen.get("dataset") != dataset or frozen.get("source") != "eastmoney"
                    or not isinstance(frozen.get("request_id"), str) or not frozen["request_id"]
                    or not symbols
                    or any(not isinstance(symbol, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol) for symbol in symbols)
                    or len(set(symbols)) != len(symbols)):
                raise ProviderError("INVALID_REQUEST", "Batch identity, source, dataset, or canonical securities are invalid")
            start, end = _day(frozen.get("start_date")), _day(frozen.get("end_date"))
            if (start != frozen["start_date"] or end != frozen["end_date"] or start > end
                    or end > self._now().date().isoformat() or (dataset != "notices" and start != end)):
                raise ProviderError("INVALID_REQUEST", "Batch requires an exact non-future target date")
            if frozen.get("period") != "1d" or frozen.get("adjustment") != "none":
                raise ProviderError("INVALID_REQUEST", "HTTP dataset requires period=1d and adjustment=none")
            deadline = _instant(frozen.get("deadline_at"))
            if _instant(frozen.get("requested_at")) > self._now():
                raise ProviderError("INVALID_REQUEST", "Request start is in the future")
            self._remaining(deadline)
            if dataset == "finance":
                grouped = self._finance_for_date(frozen, deadline)
                outcomes.update({symbol: _data(grouped.get(symbol, []), empty_event=True) for symbol in symbols})
                result["received_at"] = self._now().isoformat()
                return result
            if dataset in {"alist_daily", "alist_detail"}:
                grouped = self._alist_for_date(dataset, frozen, deadline)
                outcomes.update({symbol: _data(grouped.get(symbol, []), empty_event=True) for symbol in symbols})
                result["received_at"] = self._now().isoformat()
                return result
            if dataset == "notices":
                grouped = self._notices_for_date(frozen, deadline)
                for symbol in symbols:
                    rows = [{**row, "stock_code": symbol[:6], "symbol": symbol}
                            for row in grouped.get(symbol[:6], [])]
                    outcomes[symbol] = _data(rows, empty_event=True)
                result["received_at"] = self._now().isoformat()
                return result
            raise ProviderError("INVALID_REQUEST", "Unsupported HTTP product")
        except ProviderError as exc:
            outcomes.update({symbol: _error(exc) for symbol in symbols})
        result["received_at"] = self._now().isoformat()
        return result
