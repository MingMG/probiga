"""Convert explicit full-QMT reference results; never infer a holiday."""
from datetime import date, time
import re
from collections.abc import Mapping

from .models import DatasetSpec, NormalizedBatch, NormalizedUnit, WorkUnit
from .normalize import NormalizationError, _timestamp

SYMBOL = re.compile(r"\d{6}\.(?:SH|SZ|BJ)")
EXCHANGES = {"SH": "SH", "SZ": "SZ", "BJ": "BJ", "SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
METHODS = {"instrument": "ContextInfo.get_instrument_detail",
           "sector": "ContextInfo.get_stock_list_in_sector",
           "calendar": "ContextInfo.get_trading_dates"}
TABLES = {"stock": ("si_all_code", "stock_code"),
          "index": ("si_all_index_code", "index_code"),
          "etf": ("si_etf_code", "etf_code")}


def _envelope(raw, period):
    request = raw.get("request") or {}
    codes = request.get("codes")
    request_id = request.get("request_id")
    if (request.get("dataset") != "reference" or request.get("source") != "guojin_qmt"
            or request.get("period") != period or request.get("adjustment") != "none"
            or not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", request_id)
            or raw.get("source_method") != METHODS[period]):
        raise NormalizationError("WRONG_REFERENCE_REQUEST", "reference dataset, period, source or method differs")
    if not isinstance(codes, list) or not codes or any(not isinstance(code, str) or not code.strip() for code in codes) or len(set(codes)) != len(codes):
        raise NormalizationError("WRONG_REFERENCE_REQUEST", "explicit unique reference scope is required")
    if period != "sector" and any(not SYMBOL.fullmatch(code) for code in codes):
        raise NormalizationError("WRONG_REFERENCE_REQUEST", "reference securities require exchange suffixes")
    try:
        start = date.fromisoformat(request["start_date"])
        end = date.fromisoformat(request["end_date"])
    except (KeyError, ValueError, TypeError) as exc:
        raise NormalizationError("WRONG_REFERENCE_DATE", "reference date must be ISO date") from exc
    if start != end:
        raise NormalizationError("WRONG_REFERENCE_DATE", "one request must cover one date")
    outcomes = raw.get("outcomes")
    if not isinstance(outcomes, dict) or set(outcomes) - set(codes):
        raise NormalizationError("WRONG_REFERENCE_SCOPE", "result contains unrequested scope")
    return request, outcomes, _timestamp(raw.get("received_at"))


def _rows(outcomes, key):
    outcome = outcomes.get(key)
    if not isinstance(outcome, dict):
        raise NormalizationError("MISSING_OUTCOME", f"reference result omitted {key}")
    if outcome.get("status") != "data":
        raise NormalizationError(str(outcome.get("error_code") or "REFERENCE_UNAVAILABLE"),
                                 str(outcome.get("reason") or "reference source did not return data"))
    rows = outcome.get("rows")
    if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
        raise NormalizationError("EMPTY_REFERENCE", "reference result contains no usable rows")
    return rows


def _optional_date(value, name):
    # QMT's zero date means unknown, not today's date or a manufactured epoch.
    if value is None or value in ("", 0, "0", "00000000", "0000-00-00"):
        return None
    try:
        return _timestamp(value).date().isoformat()
    except NormalizationError as exc:
        raise NormalizationError("INVALID_REFERENCE_DATE", f"native {name} is invalid") from exc


def normalize_reference(raw_batch, asset_class):
    """Return one existing-directory spec and independently converted units."""
    if asset_class not in TABLES:
        raise NormalizationError("UNSUPPORTED_ASSET_CLASS", "reference asset must be stock, index or etf")
    request, outcomes, received = _envelope(raw_batch, "instrument")
    table, code_column = TABLES[asset_class]
    spec = DatasetSpec("reference", "guojin_qmt", table, "primary", code_column,
                       (code_column,), "instrument", ("none",), asset_class, time(8, 30),
                       persisted_source="gj_big_qmt_inner")
    units = []
    for symbol in request["codes"]:
        unit = WorkUnit("reference", "guojin_qmt", request["start_date"], symbol, "instrument", "none")
        detail = {"source_method": METHODS["instrument"], "asset_class": asset_class}
        try:
            rows = _rows(outcomes, symbol)
            if len(rows) != 1:
                raise NormalizationError("DUPLICATE_INSTRUMENT", "instrument response must have one record")
            raw = rows[0]
            detail["instrument_raw"] = dict(raw)
            code = str(raw.get("InstrumentID") or "")
            exchange = EXCHANGES.get(str(raw.get("ExchangeID") or "").upper())
            if not re.fullmatch(r"\d{6}", code) or exchange is None or f"{code}.{exchange}" != symbol:
                raise NormalizationError("WRONG_INSTRUMENT_ID", "native instrument/exchange differs from requested symbol")
            if raw.get("qmt_code") is not None and raw["qmt_code"] != symbol:
                raise NormalizationError("WRONG_INSTRUMENT_ID", "result qmt_code differs")
            name = str(raw.get("InstrumentName") or "").strip()
            if not name:
                raise NormalizationError("MISSING_INSTRUMENT_NAME", "native instrument name is missing")
            opened = _optional_date(raw.get("OpenDate"), "OpenDate")
            expired = _optional_date(raw.get("ExpireDate"), "ExpireDate")
            if opened and expired and expired < opened:
                raise NormalizationError("INVALID_REFERENCE_DATE", "expiry precedes listing")
            if asset_class == "index" and code.startswith("395"):
                raise NormalizationError("UNSUPPORTED_INSTRUMENT", "SZSE volume statistics are not price indices")
            row = {code_column: code, "qmt_code": symbol, "exchange": exchange,
                   "list_date": opened, "data_source": "gj_big_qmt_inner",
                   "received_at": received.replace(tzinfo=None)}
            if asset_class == "index":
                row.update(name=name, source="gj_big_qmt_inner", expire_date=expired,
                           etl_sync_at=received.replace(tzinfo=None))
            elif asset_class == "stock":
                row.update(short_name=name, expire_date=expired, etl_sync_at=received.replace(tzinfo=None))
            else:
                status = ("inactive" if expired and expired < unit.target_date else
                          "pending" if opened and opened > unit.target_date else "active")
                row.update(short_name=name, last_trade_date=expired, status=status,
                           primary_source="gj_big_qmt_inner", validation_source="guojin_qmt",
                           sync_status="single_source", updated_at=received.replace(tzinfo=None))
                # Existing asset_class means investment exposure (equity/gold/etc),
                # which is not inferable from the fact that an instrument is an ETF.
                detail["requires_asset_class"] = True
            units.append(NormalizedUnit(unit, "complete", [row], detail=detail))
        except NormalizationError as exc:
            units.append(NormalizedUnit(unit, "error", [], exc.code, str(exc), detail))
    return spec, NormalizedBatch(request["request_id"], units, received)


def extract_sector_codes(raw_batch):
    """Return only securities from the exact configured sector outcomes."""
    request, outcomes, _received = _envelope(raw_batch, "sector")
    codes = set()
    for sector in request["codes"]:
        local = set()
        for row in _rows(outcomes, sector):
            code = row.get("qmt_code")
            if row.get("sector") != sector or not isinstance(code, str) or not SYMBOL.fullmatch(code):
                raise NormalizationError("WRONG_SECTOR_MEMBER", "sector membership identity is invalid")
            if code in local:
                raise NormalizationError("DUPLICATE_SECTOR_MEMBER", "source sector contains a duplicate security")
            local.add(code)
        codes.update(local)  # A security can legitimately belong to multiple requested sectors.
    return sorted(codes)


def merge_calendar_rows(raw_batch, existing_rows):
    """Keep existing authority and add proved past opens, never inferred closes."""
    request, outcomes, received = _envelope(raw_batch, "calendar")
    if isinstance(existing_rows, Mapping):
        existing_rows = [dict(value, trade_date=key) if isinstance(value, Mapping)
                         else {"trade_date": key, "trade_status": int(value)}
                         for key, value in existing_rows.items()]
    merged = {}
    for raw in existing_rows:
        day = _timestamp(raw.get("trade_date")).date().isoformat()
        status = raw.get("trade_status")
        if status not in (0, 1, False, True):
            raise NormalizationError("INVALID_CALENDAR", "existing calendar status must explicitly be open or closed")
        if day in merged:
            raise NormalizationError("DUPLICATE_CALENDAR", "existing calendar has duplicate dates")
        merged[day] = {**raw, "trade_date": day, "trade_status": int(status)}
    for symbol in request["codes"]:
        seen = set()
        for raw in _rows(outcomes, symbol):
            if raw.get("qmt_code") != symbol:
                raise NormalizationError("WRONG_CALENDAR_ID", "native calendar symbol differs")
            day = _timestamp(raw.get("native_time")).date().isoformat()
            if not request["start_date"] <= day <= request["end_date"] or day > received.date().isoformat():
                raise NormalizationError("UNPROVEN_CALENDAR_DATE", "native history does not prove this requested/past date")
            if day in seen:
                raise NormalizationError("DUPLICATE_CALENDAR", "native calendar has duplicate dates")
            seen.add(day)
            if day in merged and merged[day]["trade_status"] != 1:
                raise NormalizationError("CALENDAR_CONFLICT", "native open day contradicts the authoritative closed day")
            if day not in merged:
                merged[day] = {"trade_date": day, "trade_status": 1}
    return [merged[day] for day in sorted(merged)]
