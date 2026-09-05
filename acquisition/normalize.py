"""Pure source-to-table conversion. No network, database or legacy imports."""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import hashlib
import json
import re
from zoneinfo import ZoneInfo

from .models import DatasetSpec, NormalizedBatch, NormalizedUnit, WorkUnit

SHANGHAI = ZoneInfo("Asia/Shanghai")
ADJUSTMENTS = {"none": 0, "front": 1, "back": 2}
FLOW_FIELDS = ("main_net_inflow", "sm_net_inflow", "mid_net_inflow", "lg_net_inflow", "max_net_inflow")
FINANCE_NUMERIC_FIELDS = frozenset((
    "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps", "cap_reserve_ps",
    "undist_profit_ps", "oper_cf_ps", "total_rev", "gross_profit", "net_profit_attr_sh",
    "non_gaap_net_profit", "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
    "total_rev_qoq_gr", "net_profit_qoq_gr", "roe_wtd", "roe_non_gaap_wtd", "roa_wtd",
    "gross_margin", "net_margin", "curr_ratio", "quick_ratio", "cash_flow_ratio", "asset_liab_ratio",
))


class NormalizationError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def _timestamp(value):
    if value is None or value == "":
        raise NormalizationError("MISSING_SOURCE_TIME", "native timestamp is missing")
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, date):
            parsed = datetime.combine(value, datetime.min.time())
        else:
            raw = str(value).strip()
            if re.fullmatch(r"\d{14}", raw):
                parsed = datetime.strptime(raw, "%Y%m%d%H%M%S")
            elif re.fullmatch(r"\d{8}", raw):
                parsed = datetime.strptime(raw, "%Y%m%d")
            elif isinstance(value, (int, float, Decimal)) or re.fullmatch(r"\d{10}|\d{13}", raw):
                epoch = Decimal(raw)
                parsed = datetime.fromtimestamp(float(epoch / (1000 if abs(epoch) >= 10**12 else 1)), timezone.utc)
            else:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    except (ValueError, OverflowError, InvalidOperation, OSError) as exc:
        raise NormalizationError("INVALID_SOURCE_TIME", "native timestamp is invalid") from exc


def _date(value):
    return _timestamp(value).date().isoformat()


def _has_exact_time(value):
    """A date alone does not establish a native second-resolution timestamp."""
    if isinstance(value, datetime):
        return True
    raw = str(value or "").strip()
    return bool(re.search(r"[T ]\d{2}:\d{2}:\d{2}", raw) or re.fullmatch(r"\d{10}|\d{13}|\d{14}", raw))


def _first(row, *fields):
    for field in fields:
        if field in row and row[field] is not None:
            return row[field]
    return None


def _decimal(value, *, field, scale=6, precision=50, positive=False, nonnegative=False, optional=False):
    if value is None or value == "":
        if optional:
            return None
        raise NormalizationError("MISSING_FIELD", f"{field} is missing")
    try:
        with localcontext() as context:
            context.prec = 50
            result = Decimal(str(value))
            if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
                raise InvalidOperation
            result = result.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
            if (positive and result <= 0) or abs(result) >= Decimal(10) ** (precision - scale):
                raise InvalidOperation
            return abs(result) if result == 0 else result
    except (InvalidOperation, ValueError) as exc:
        raise NormalizationError("INVALID_NUMBER", f"{field} has an invalid numeric value") from exc


def _identity(raw, unit):
    expected = unit.code.split(".")[0]
    for field in ("symbol", "qmt_code"):
        if raw.get(field) is not None and str(raw[field]).upper() != unit.code:
            raise NormalizationError("WRONG_CODE", f"{field} differs from requested security")
    for field in ("stock_code", "index_code", "etf_code"):
        if raw.get(field) is not None and str(raw[field]) not in {expected, unit.code}:
            raise NormalizationError("WRONG_CODE", f"{field} differs from requested security")


def _metadata(catalog, code):
    if catalog is None:
        return {}
    item = catalog.get(code, {})
    return item if isinstance(item, dict) else vars(item)


def _market_row(spec, unit, raw, received, factors, metadata, request_id):
    stamp = _timestamp(_first(raw, "trade_time", "snapshot_at", "time", "stime", "timetag", "source_time", "native_index"))
    if stamp.date().isoformat() != unit.target_date:
        raise NormalizationError("WRONG_DATE", "native market time differs from target date")
    if raw.get("trade_date") is not None and _date(raw["trade_date"]) != unit.target_date:
        raise NormalizationError("WRONG_DATE", "market date fields differ")
    if stamp > received:
        raise NormalizationError("FUTURE_SOURCE_TIME", "native market time is later than receipt")
    minute = spec.period == "1m"
    current = spec.period == "tick"
    price_field = "price" if minute or current else "close"
    precision = 18 if spec.asset_class == "etf" else 50
    price = _decimal(_first(raw, price_field, "lastPrice", "close"), field=price_field, positive=True, precision=precision)
    row = {spec.code_column: unit.code.split(".")[0], "trade_time": stamp.replace(tzinfo=None),
           "trade_date": unit.target_date, price_field: price}
    if not minute:
        for field in ("open", "high", "low"):
            row[field] = _decimal(raw.get(field), field=field, positive=True, precision=precision)
        if row["high"] < max(row["open"], price, row["low"]) or row["low"] > min(row["open"], price, row["high"]):
            raise NormalizationError("INVALID_OHLC", "OHLC ordering differs")
    if not factors or set(factors) != {"volume", "amount"}:
        raise NormalizationError("UNSUPPORTED_UNITS", "explicit volume and amount factors are required")
    scale = 4 if spec.asset_class == "etf" else 6
    for field in ("volume", "amount"):
        factor = _decimal(factors[field], field=f"{field} factor", positive=True, scale=12)
        value = _decimal(raw.get(field), field=field, nonnegative=True, scale=12)
        row[field] = _decimal(value * factor, field=field, nonnegative=True, scale=scale, precision=24 if spec.asset_class == "etf" else 50)
    pre_close = _decimal(_first(raw, "pre_close", "preClose", "lastClose"), field="pre_close", positive=True, optional=True, precision=precision)
    change = _first(raw, "change")
    change_pct = _first(raw, "change_pct", "changePct")
    if change is None and pre_close is not None:
        change = price - pre_close
    if change_pct is None and pre_close is not None:
        change_pct = (price - pre_close) / pre_close * 100
    row["change"] = _decimal(change, field="change", optional=True, precision=precision)
    row["change_pct"] = _decimal(change_pct, field="change_pct", scale=8 if spec.asset_class == "etf" else 6, optional=True, precision=precision)
    row["etl_sync_at"] = received.replace(tzinfo=None)
    if minute:
        row["avg_price"] = _decimal(raw.get("avg_price"), field="avg_price", positive=True, optional=True)
        row["snapshot_at"] = received.replace(tzinfo=None)
    elif current:
        row["snapshot_at"] = stamp.replace(tzinfo=None)
    else:
        row["k_type"] = 1
        if spec.asset_class != "index":
            row["adjust_type"] = ADJUSTMENTS[unit.adjustment]
            row["pre_close"] = pre_close
            row["short_name"] = str(_first(raw, "short_name", "name") or metadata.get("short_name") or metadata.get("name") or "")
        if spec.asset_class == "stock":
            row["turnover_ratio"] = _decimal(raw.get("turnover_ratio"), field="turnover_ratio", nonnegative=True, optional=True)
    if spec.asset_class == "etf":
        if pre_close is None or not row["short_name"]:
            raise NormalizationError("MISSING_FIELD", "ETF requires native pre_close and catalog name")
        row.update(data_source=spec.persisted_source or spec.source,
                   received_at=received.replace(tzinfo=None), batch_id=request_id)
        row["data_version"] = hashlib.sha256(json.dumps({k: v for k, v in row.items() if k not in {
            "etl_sync_at", "received_at", "batch_id"}}, sort_keys=True, default=str).encode()).hexdigest()
        row.pop("etl_sync_at")  # The existing ETF table has received_at, not etl_sync_at.
    return row


def _http_row(spec, unit, raw, received, source_method):
    row = {k: v for k, v in raw.items() if k not in {"raw", "symbol", "qmt_code", "source_security_codes"}}
    row["stock_code"] = unit.code.split(".")[0]
    if spec.name == "finance":
        report = _date(raw.get("report_date"))
        if report > unit.target_date or report[5:] not in {"03-31", "06-30", "09-30", "12-31"}:
            raise NormalizationError("INVALID_REPORT_PERIOD", "financial report period is invalid")
        if not raw.get("report_type"):
            raise NormalizationError("MISSING_FIELD", "financial report_type is required")
        row["report_date"] = report
        for field in ("notice_date", "source_update_date"):
            value = _date(raw[field]) if raw.get(field) else None
            if value and value > received.date().isoformat():
                raise NormalizationError("FUTURE_SOURCE_DATE", f"{field} exceeds receipt date")
            if raw.get(field) and _has_exact_time(raw[field]) and _timestamp(raw[field]) > received:
                raise NormalizationError("FUTURE_SOURCE_DATE", f"{field} exceeds actual receipt time")
            # UPDATE_DATE can contain a real update time. Keep its original
            # precision for revision ordering instead of reducing it to a day.
            row[field] = raw[field] if field == "source_update_date" and value else value
        for field in FINANCE_NUMERIC_FIELDS & row.keys():
            if row[field] is None or row[field] == "":
                row[field] = None
                continue
            try:
                number = Decimal(str(row[field]))
                if not number.is_finite():
                    raise InvalidOperation
                row[field] = number
            except (InvalidOperation, ValueError) as exc:
                raise NormalizationError("INVALID_NUMBER", f"{field} is not finite financial data") from exc
        # Preserve the provider's precision in revision facts. The dedicated
        # finance writer applies the display table's separately declared scales.
        return row
    date_field = "notice_date" if spec.name == "notices" else "trade_date"
    if _date(raw.get(date_field)) != unit.target_date:
        raise NormalizationError("WRONG_DATE", f"{date_field} differs from target")
    row[date_field] = unit.target_date
    if spec.name == "capital_flow_daily":
        if not (unit.code.endswith((".SH", ".SZ")) and unit.code[:2] in {"00", "30", "60", "68"}):
            raise NormalizationError("UNSUPPORTED_MARKET", "daily flow supports SH/SZ A shares only")
        method = raw.get("source_method") or source_method
        persisted_source = {"eastmoney.fflow.daykline": "push2hist",
                            "eastmoney.clist.fflow.current": "push2delay"}.get(method)
        if persisted_source is None:
            raise NormalizationError("UNSUPPORTED_SOURCE_METHOD", "daily-flow source method is not configured")
        row = {"stock_code": row["stock_code"], "trade_date": unit.target_date,
               **{field: _decimal(raw.get(field), field=field) for field in FLOW_FIELDS},
               "data_source": persisted_source, "etl_sync_at": received.replace(tzinfo=None)}
        if raw.get("source_time") is not None:
            if not _has_exact_time(raw["source_time"]):
                raise NormalizationError("MISSING_SOURCE_TIME", "flow source_time does not contain an exact native time")
            stamp = _timestamp(raw["source_time"])
            if stamp.date().isoformat() != unit.target_date or stamp > received:
                raise NormalizationError("WRONG_DATE", "flow source_time differs from target/receipt")
            row["source_time"] = stamp.replace(tzinfo=None)
    elif spec.name == "notices":
        if not raw.get("art_code") or not raw.get("title"):
            raise NormalizationError("MISSING_FIELD", "announcement identity/title is missing")
        associated = raw.get("source_security_codes")
        if not isinstance(associated, list) or not {unit.code, unit.code.split(".")[0]} & set(associated):
            raise NormalizationError("UNPROVEN_ASSOCIATION", "announcement does not prove security association")
        if raw.get("display_time") and _has_exact_time(raw["display_time"]):
            row["display_time"] = _timestamp(raw["display_time"]).replace(tzinfo=None)
        if "source_time" in raw:
            row["source_time"] = (_timestamp(raw["source_time"]).replace(tzinfo=None)
                                  if _has_exact_time(raw["source_time"]) else None)
    elif spec.name in {"alist_daily", "alist_detail"}:
        if not raw.get("trade_id"):
            raise NormalizationError("MISSING_FIELD", "billboard trade_id is required")
        if spec.name == "alist_detail" and (not raw.get("operate_code") or raw.get("report_side") not in {"BUY", "SELL"}):
            raise NormalizationError("MISSING_FIELD", "billboard detail department/side is required")
        for field in tuple(row):
            if field in {"close", "change_cpt", "turnover_ratio", "a_net_amount", "a_buy_amount", "a_sell_amount", "a_amount", "amount", "net_amount_rate", "a_amount_rate", "a_buy_amount_rate", "a_sell_amount_rate"}:
                row[field] = _decimal(row[field], field=field, optional=True)
    else:
        raise NormalizationError("UNSUPPORTED_PRODUCT", "specialized product conversion is not implemented")
    return row


def normalize_batch(spec, raw_batch, received_at=None, *, volume_factors=None, minute_grids=None, catalog=None):
    request = raw_batch.get("request") or {}
    request_id = str(request.get("request_id") or "")
    codes = request.get("codes")
    if not request_id or len(request_id) > 64 or request.get("dataset") != spec.name or request.get("source") != spec.source:
        raise NormalizationError("WRONG_REQUEST", "batch dataset/source/request identity differs")
    if not isinstance(codes, list) or not codes or len(set(codes)) != len(codes) or any(not re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", str(code)) for code in codes):
        raise NormalizationError("WRONG_REQUEST", "request requires unique exchange-qualified security codes")
    target = _date(request.get("start_date"))
    if target != _date(request.get("end_date")):
        raise NormalizationError("WRONG_REQUEST", "one batch must cover exactly one date")
    period, adjustment = request.get("period"), request.get("adjustment")
    if period != spec.period or adjustment not in spec.adjustments or adjustment not in ADJUSTMENTS:
        raise NormalizationError("UNSUPPORTED_PRODUCT", "period/adjustment is not configured")
    received = _timestamp(received_at if received_at is not None else raw_batch.get("received_at"))
    outcomes = raw_batch.get("outcomes") or {}
    if not isinstance(outcomes, dict) or set(outcomes) - set(codes):
        raise NormalizationError("WRONG_REQUEST", "result contains unrequested securities")
    source_method = str(raw_batch.get("source_method") or "")
    if not source_method:
        raise NormalizationError("MISSING_SOURCE_METHOD", "result does not identify its actual source method")
    units = []
    for code in codes:
        unit = WorkUnit(spec.name, spec.source, target, code, period, adjustment)
        detail = {"source_method": source_method}
        try:
            outcome = outcomes.get(code)
            if not isinstance(outcome, dict):
                raise NormalizationError("MISSING_OUTCOME", "requested security outcome is missing")
            if outcome.get("status") == "error":
                raise NormalizationError(str(outcome.get("error_code") or "PROVIDER_ERROR"), str(outcome.get("reason") or "provider failed"))
            if outcome.get("status") == "no_data":
                reason = str(outcome.get("reason") or "").lower()
                allowed = {"suspended", "not_listed", "delisted", "no_trades"}
                if spec.name in {"notices", "alist_daily", "alist_detail"}:
                    allowed |= {"empty_event_set"}
                if reason not in allowed or outcome.get("rows"):
                    raise NormalizationError("UNPROVEN_NO_DATA", "empty result lacks an explicit supported source reason")
                units.append(NormalizedUnit(unit, "no_data", [], detail={**detail, "reason": reason}))
                continue
            raw_rows = outcome.get("rows")
            if outcome.get("status") != "data" or not isinstance(raw_rows, list) or not raw_rows:
                raise NormalizationError("EMPTY_RESPONSE", "data outcome must contain rows")
            metadata = _metadata(catalog, code)
            factors = (volume_factors or {}).get((source_method, period, spec.asset_class))
            rows = []
            for raw in raw_rows:
                _identity(raw, unit)
                rows.append(_market_row(spec, unit, raw, received, factors, metadata, request_id)
                            if spec.source == "guojin_qmt" and spec.name != "reference"
                            else _http_row(spec, unit, raw, received, source_method))
            identities = [tuple(row.get(field) for field in spec.key_columns) for row in rows]
            if not spec.key_columns or any(any(value is None for value in key) for key in identities) or len(set(identities)) != len(identities):
                raise NormalizationError("DUPLICATE_OR_MISSING_KEY", "business key is missing or duplicated")
            if spec.name == "stock_daily":
                volume, amount = rows[0]["volume"], rows[0]["amount"]
                if (volume == 0) != (amount == 0):
                    raise NormalizationError("INCONSISTENT_ACTIVITY", "daily volume and amount disagree on trading activity")
                detail["traded"] = volume > 0 and amount > 0
            if period == "1m":
                grid = (minute_grids or {}).get((spec.asset_class, code), metadata.get("minute_grid"))
                if not grid:
                    raise NormalizationError("UNSUPPORTED_TIME_GRID", "security-specific minute grid is not configured")
                grid = {str(value) for value in grid}
                actual = {row["trade_time"].strftime("%H:%M:%S") for row in rows}
                if any(row["trade_time"].second or row["trade_time"].microsecond for row in rows) or not grid.issubset(actual):
                    raise NormalizationError("MINUTE_GAP", "required native minute points are missing or invalid")
                if spec.asset_class != "index" and actual != grid:
                    raise NormalizationError("WRONG_TIME_GRID", "unexpected minute points for configured product")
                detail["out_of_scope_rows"] = len(actual - grid)
                rows = [row for row in rows if row["trade_time"].strftime("%H:%M:%S") in grid]
            if spec.name == "finance":
                detail.update(revision_rows=raw_rows, source_method=source_method)
            units.append(NormalizedUnit(unit, "complete", rows, detail=detail))
        except (NormalizationError, TypeError, ValueError, KeyError) as exc:
            units.append(NormalizedUnit(unit, "error", [], getattr(exc, "code", "INVALID_RESPONSE"), str(exc), detail))
    return NormalizedBatch(request_id, units, received)
