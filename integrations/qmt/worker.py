from __future__ import annotations

import json
import math
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import xtquant
from xtquant import xtdata


PROVIDER_ID = "gj_qmt"
OFFICIAL_NATIVE_APIS = (
    "connect",
    "get_market_data",
    "get_market_data_ex",
    "get_local_data",
    "get_full_tick",
    "get_full_kline",
    "subscribe_quote",
    "subscribe_whole_quote",
    "unsubscribe_quote",
    "download_history_data",
    "download_history_data2",
    "download_sector_data",
    "download_history_contracts",
    "get_sector_list",
    "get_stock_list_in_sector",
    "download_index_weight",
    "get_index_weight",
    "get_instrument_detail",
    "get_instrument_detail_list",
    "download_holiday_data",
    "get_trading_calendar",
    "get_divid_factors",
    "download_his_st_data",
    "get_his_st_data",
    "download_financial_data",
    "download_financial_data2",
    "get_financial_data",
    "get_market_time",
)

OFFICIAL_PERIODS = (
    "tick",
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "1d",
    "1w",
    "1mon",
    "transactioncount1m",
    "transactioncount1d",
    "orderflow1m",
    "orderflow1d",
    "northfinancechange1m",
    "northfinancechange1d",
    "interactiveqa",
    "announcement",
    "l2quote",
    "l2quoteaux",
    "l2order",
    "l2transaction",
    "l2transactioncount",
    "l2orderqueue",
)


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if hasattr(value, "item"):
        try:
            return _json_value(value.item())
        except Exception:
            pass
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    return value


def _records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(k): _json_value(v) for k, v in row.items()} for row in rows]


def _chunked(items: list[str], size: int) -> list[list[str]]:
    batch_size = max(1, int(size))
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _is_intraday_period(period: str) -> bool:
    return str(period or "").strip().lower() in {"1m", "tick", "transactioncount1m", "orderflow1m"}


def _normalize_qmt_date(value: str, *, include_time: bool, end_of_day: bool = False) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) == 8:
        if include_time:
            return digits + ("235959" if end_of_day else "000000")
        return digits
    return digits


def _subscribe(codes: list[str], period: str) -> None:
    for code in codes:
        try:
            xtdata.subscribe_quote(code, period=period, count=-1)
        except Exception:
            pass


def _download_history(codes: list[str], *, period: str, start_date: str = "", end_date: str = "") -> None:
    if not codes or os.environ.get("QMT_SKIP_HISTORY_DOWNLOAD") == "1":
        return

    start_time = _normalize_qmt_date(start_date, include_time=_is_intraday_period(period))
    end_time = _normalize_qmt_date(end_date, include_time=_is_intraday_period(period), end_of_day=True)
    if not start_time and not end_time:
        return

    downloader = getattr(xtdata, "download_history_data2", None)
    single = getattr(xtdata, "download_history_data", None)
    try:
        if callable(downloader):
            downloader(
                stock_list=codes,
                period=period,
                start_time=start_time,
                end_time=end_time,
            )
        elif callable(single):
            for code in codes:
                single(code, period, start_time, end_time)
    except Exception:
        pass

    wait_seconds = float(os.environ.get("QMT_HISTORY_WAIT_SECONDS", "0.8") or "0.8")
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _transform_kline(data: Any, *, start_date: str, end_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start_ts = pd.Timestamp(start_date).normalize() if start_date else None
    end_ts = pd.Timestamp(end_date).normalize() if end_date else None

    if not isinstance(data, dict):
        return rows

    for qmt_code, frame in data.items():
        if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        df = frame.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        if start_ts is not None:
            df = df[df.index >= start_ts]
        if end_ts is not None:
            df = df[df.index <= end_ts]
        if df.empty:
            continue

        stock_code = str(qmt_code).split(".", 1)[0].zfill(6)
        prev_close_series = pd.to_numeric(df.get("preClose"), errors="coerce")
        close_series = pd.to_numeric(df.get("close"), errors="coerce")
        fallback_prev = close_series.shift(1)
        prev_close_series = prev_close_series.fillna(fallback_prev)
        change_series = close_series - prev_close_series
        change_pct_series = change_series / prev_close_series.replace({0: pd.NA}) * 100

        for idx, row in df.iterrows():
            trade_date = idx.strftime("%Y-%m-%d")
            prev_close = prev_close_series.loc[idx]
            change = change_series.loc[idx]
            change_pct = change_pct_series.loc[idx]
            rows.append(
                {
                    "qmt_code": qmt_code,
                    "stock_code": stock_code,
                    "trade_time": f"{trade_date} 15:00:00",
                    "trade_date": trade_date,
                    "k_type": 1,
                    "adjust_type": 1,
                    "open": row.get("open"),
                    "close": row.get("close"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "volume": row.get("volume"),
                    "amount": row.get("amount"),
                    "change": change,
                    "change_pct": change_pct,
                    "turnover_ratio": row.get("turnover"),
                    "pre_close": prev_close,
                }
            )
    return rows


def _transform_minute(data: Any, *, trade_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        return rows

    for qmt_code, frame in data.items():
        if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        stock_code = str(qmt_code).split(".", 1)[0].zfill(6)
        df = frame.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        df = df[df.index.strftime("%Y-%m-%d") == trade_date]
        if df.empty:
            continue

        close_series = pd.to_numeric(df.get("close"), errors="coerce")
        prev_close_series = pd.to_numeric(df.get("preClose"), errors="coerce")
        change_series = close_series - prev_close_series
        change_pct_series = change_series / prev_close_series.replace({0: pd.NA}) * 100

        for idx, row in df.iterrows():
            volume = _safe_float(row.get("volume"))
            amount = _safe_float(row.get("amount"))
            avg_price = (amount / (volume * 100)) if amount is not None and volume not in (None, 0) else None
            rows.append(
                {
                    "qmt_code": qmt_code,
                    "stock_code": stock_code,
                    "trade_time": idx.strftime("%Y-%m-%d %H:%M:%S"),
                    "trade_date": trade_date,
                    "price": row.get("close"),
                    "avg_price": avg_price,
                    "change": change_series.loc[idx],
                    "change_pct": change_pct_series.loc[idx],
                    "volume": row.get("volume"),
                    "amount": row.get("amount"),
                    "pre_close": prev_close_series.loc[idx],
                }
            )
    return rows


def _transform_current(tick: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(tick, dict):
        return rows

    for qmt_code, info in tick.items():
        if not isinstance(info, dict):
            continue
        stock_code = str(qmt_code).split(".", 1)[0].zfill(6)
        last_price = _safe_float(info.get("lastPrice") or info.get("last_price"))
        last_close = _safe_float(info.get("lastClose") or info.get("last_close"))
        if (last_price or 0) <= 0 and (last_close or 0) <= 0:
            continue
        if (last_price or 0) <= 0:
            last_price = last_close
        change = (last_price - last_close) if last_price is not None and last_close not in (None, 0) else None
        change_pct = ((change / last_close) * 100) if change is not None and last_close not in (None, 0) else None
        timetag = str(info.get("timetag") or "").strip()
        if timetag:
            parsed = pd.to_datetime(timetag, format="%Y%m%d %H:%M:%S", errors="coerce")
            if pd.isna(parsed):
                parsed = pd.to_datetime(timetag, errors="coerce")
            snapshot_at = (
                parsed.strftime("%Y-%m-%d %H:%M:%S")
                if not pd.isna(parsed)
                else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            snapshot_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(
            {
                "qmt_code": qmt_code,
                "stock_code": stock_code,
                "price": last_price,
                "open": info.get("open"),
                "high": info.get("high"),
                "low": info.get("low"),
                "change": change,
                "change_pct": change_pct,
                "volume": info.get("volume"),
                "amount": info.get("amount"),
                "snapshot_at": snapshot_at,
                "pre_close": last_close,
            }
        )
    return rows


def _transform_tick(data: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        return rows

    for qmt_code, frame in data.items():
        if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        stock_code = str(qmt_code).split(".", 1)[0].zfill(6)
        df = frame.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        for idx, row in df.iterrows():
            record = {
                "qmt_code": qmt_code,
                "stock_code": stock_code,
                "trade_time": idx.strftime("%Y-%m-%d %H:%M:%S"),
            }
            for key, value in row.items():
                record[str(key)] = value
            rows.append(record)
    return rows


def _adjust_type_from_dividend_type(dividend_type: str) -> int:
    text = str(dividend_type or "").strip().lower()
    if text in {"front", "forward", "qfq"}:
        return 1
    if text in {"back", "backward", "hfq"}:
        return 2
    return 0


def _transform_flow(data: Any, *, start_date: str = "", end_date: str = "", minute_level: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        return rows

    start_ts = pd.Timestamp(start_date).normalize() if start_date else None
    end_ts = pd.Timestamp(end_date).normalize() if end_date else None

    for qmt_code, frame in data.items():
        if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        stock_code = str(qmt_code).split(".", 1)[0].zfill(6)
        df = frame.copy()
        if "time" in df.columns:
            df["_trade_ts"] = pd.to_datetime(df["time"], errors="coerce")
        else:
            df["_trade_ts"] = pd.to_datetime(df.index, errors="coerce")
        df = df[df["_trade_ts"].notna()].sort_values("_trade_ts")
        if start_ts is not None:
            df = df[df["_trade_ts"] >= start_ts]
        if end_ts is not None:
            if minute_level:
                df = df[df["_trade_ts"] < (end_ts + pd.Timedelta(days=1))]
            else:
                df = df[df["_trade_ts"] <= end_ts]
        if df.empty:
            continue

        for _, row in df.iterrows():
            trade_ts = row["_trade_ts"]
            max_net = _safe_float(row.get("netInflowMostAmount"))
            lg_net = _safe_float(row.get("netInflowBigAmount"))
            mid_net = _safe_float(row.get("netInflowMediumAmount"))
            sm_net = _safe_float(row.get("netInflowSmallAmount"))
            rows.append(
                {
                    "qmt_code": qmt_code,
                    "stock_code": stock_code,
                    "trade_time": trade_ts.strftime("%Y-%m-%d %H:%M:%S") if minute_level else f"{trade_ts.strftime('%Y-%m-%d')} 15:00:00",
                    "trade_date": trade_ts.strftime("%Y-%m-%d"),
                    "main_net_inflow": (max_net or 0.0) + (lg_net or 0.0),
                    "max_net_inflow": max_net,
                    "lg_net_inflow": lg_net,
                    "mid_net_inflow": mid_net,
                    "sm_net_inflow": sm_net,
                }
            )
    return rows


def _kline(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    start_date = payload.get("start_date") or ""
    end_date = payload.get("end_date") or ""
    dividend_type = payload.get("dividend_type") or "front"
    adjust_type = _adjust_type_from_dividend_type(dividend_type)
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_KLINE_BATCH_SIZE", "300"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        _subscribe(batch, "1d")
        _download_history(batch, period="1d", start_date=start_date, end_date=end_date)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=batch,
            period="1d",
            count=0,
            dividend_type=dividend_type,
            fill_data=False,
        )
        rows.extend(_transform_kline(data, start_date=start_date, end_date=end_date))
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.15") or "0.15"))
    for row in rows:
        row["adjust_type"] = adjust_type
    return {"rows": _records(rows), "errors": {}}


def _minute(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    trade_date = payload.get("trade_date") or ""
    start_date = payload.get("start_date") or trade_date
    end_date = payload.get("end_date") or trade_date
    count = int(payload.get("count") or os.environ.get("QMT_MINUTE_COUNT", "0"))
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_MINUTE_BATCH_SIZE", "200"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        _subscribe(batch, "1m")
        _download_history(batch, period="1m", start_date=start_date, end_date=end_date)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=batch,
            period="1m",
            count=count,
            dividend_type="none",
            fill_data=True,
        )
        rows.extend(_transform_minute(data, trade_date=trade_date))
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.15") or "0.15"))
    return {"rows": _records(rows), "errors": {}}


def _current(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_CURRENT_BATCH_SIZE", "500"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        _subscribe(batch, "1m")
        rows.extend(_transform_current(xtdata.get_full_tick(batch)))
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.08") or "0.08"))
    return {"rows": _records(rows), "errors": {}}


def _tick(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    count = int(payload.get("count") or 100)
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_TICK_BATCH_SIZE", "100"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        _subscribe(batch, "tick")
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.08") or "0.08"))
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=batch,
            period="tick",
            count=count,
            dividend_type="none",
            fill_data=True,
        )
        rows.extend(_transform_tick(data))
    return {"rows": _records(rows), "errors": {}}


def _flow_daily(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    start_date = payload.get("start_date") or ""
    end_date = payload.get("end_date") or ""
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_FLOW_DAILY_BATCH_SIZE", "250"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        if os.environ.get("QMT_FLOW_DOWNLOAD_HISTORY", "0") == "1":
            _download_history(batch, period="transactioncount1d", start_date=start_date, end_date=end_date)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=batch,
            period="transactioncount1d",
            count=0,
            dividend_type="none",
            fill_data=False,
        )
        rows.extend(_transform_flow(data, start_date=start_date, end_date=end_date, minute_level=False))
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.15") or "0.15"))
    return {"rows": _records(rows), "errors": {}}


def _flow_min(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = payload.get("stock_codes") or []
    start_date = payload.get("start_date") or payload.get("trade_date") or ""
    end_date = payload.get("end_date") or payload.get("trade_date") or ""
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_FLOW_MIN_BATCH_SIZE", "200"))

    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        if os.environ.get("QMT_FLOW_DOWNLOAD_HISTORY", "0") == "1":
            _download_history(batch, period="transactioncount1m", start_date=start_date, end_date=end_date)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=batch,
            period="transactioncount1m",
            count=0,
            dividend_type="none",
            fill_data=True,
        )
        rows.extend(_transform_flow(data, start_date=start_date, end_date=end_date, minute_level=True))
        time.sleep(float(os.environ.get("QMT_BATCH_SLEEP_SECONDS", "0.15") or "0.15"))
    return {"rows": _records(rows), "errors": {}}


def _sector_list(_: dict[str, Any]) -> dict[str, Any]:
    rows = [{"sector_name": str(name)} for name in (xtdata.get_sector_list() or []) if str(name).strip()]
    return {"rows": _records(rows), "errors": {}}


def _sector_members(payload: dict[str, Any]) -> dict[str, Any]:
    sector_name = str(payload.get("sector_name") or "").strip()
    realtime_tag = payload.get("realtime_tag", -1)
    members = xtdata.get_stock_list_in_sector(sector_name, real_timetag=realtime_tag) or []
    rows = []
    for qmt_code in members:
        text = str(qmt_code).strip().upper()
        if not text:
            continue
        rows.append(
            {
                "sector_name": sector_name,
                "qmt_code": text,
                "stock_code": text.split(".", 1)[0].zfill(6),
                "exchange": text.split(".", 1)[1] if "." in text else "",
            }
        )
    return {"rows": _records(rows), "errors": {}}


def _sector_members_many(payload: dict[str, Any]) -> dict[str, Any]:
    sector_names = [str(name).strip() for name in (payload.get("sector_names") or []) if str(name).strip()]
    realtime_tag = payload.get("realtime_tag", -1)
    rows: list[dict[str, Any]] = []
    for sector_name in sector_names:
        members = xtdata.get_stock_list_in_sector(sector_name, real_timetag=realtime_tag) or []
        for qmt_code in members:
            text = str(qmt_code).strip().upper()
            if not text:
                continue
            rows.append(
                {
                    "sector_name": sector_name,
                    "qmt_code": text,
                    "stock_code": text.split(".", 1)[0].zfill(6),
                    "exchange": text.split(".", 1)[1] if "." in text else "",
                }
            )
    return {"rows": _records(rows), "errors": {}}


def _instrument_detail_row(qmt_code: str, detail: dict[str, Any] | None) -> dict[str, Any]:
    detail = detail or {}
    exchange = str(detail.get("ExchangeID") or "").strip().upper()
    short_name = str(detail.get("InstrumentName") or "").strip()
    open_date = str(detail.get("OpenDate") or "").strip()
    if open_date in {"0", "00000000", ""}:
        open_date = ""
    expire_date = str(detail.get("ExpireDate") or "").strip()
    if expire_date in {"0", "00000000", ""}:
        expire_date = ""
    return {
        "qmt_code": qmt_code,
        "stock_code": qmt_code.split(".", 1)[0].zfill(6),
        "exchange": exchange,
        "short_name": short_name,
        "list_date": open_date,
        "expire_date": expire_date,
        "pre_close": detail.get("PreClose"),
        "up_stop_price": detail.get("UpStopPrice"),
        "down_stop_price": detail.get("DownStopPrice"),
        "float_volume": detail.get("FloatVolume"),
        "total_volume": detail.get("TotalVolume"),
    }


def _instrument_details(payload: dict[str, Any]) -> dict[str, Any]:
    stock_codes = [str(code).strip().upper() for code in (payload.get("stock_codes") or []) if str(code).strip()]
    batch_size = int(payload.get("batch_size") or os.environ.get("QMT_DETAIL_BATCH_SIZE", "400"))
    iscomplete = bool(payload.get("iscomplete"))
    rows: list[dict[str, Any]] = []
    for batch in _chunked(stock_codes, batch_size):
        detail_map: dict[str, Any] = {}
        try:
            detail_map = xtdata.get_instrument_detail_list(batch, iscomplete=iscomplete) or {}
        except Exception:
            detail_map = {}
        for qmt_code in batch:
            detail = detail_map.get(qmt_code)
            if detail is None:
                try:
                    detail = xtdata.get_instrument_detail(qmt_code, iscomplete=iscomplete)
                except Exception:
                    detail = None
            rows.append(_instrument_detail_row(qmt_code, detail))
    return {"rows": _records(rows), "errors": {}}


def _index_weight(payload: dict[str, Any]) -> dict[str, Any]:
    index_code = str(payload.get("index_code") or "").strip().upper()
    weight_map = xtdata.get_index_weight(index_code) or {}
    rows = []
    for qmt_code, weight in weight_map.items():
        text = str(qmt_code).strip().upper()
        if not text:
            continue
        rows.append(
            {
                "index_qmt_code": index_code,
                "index_code": index_code.split(".", 1)[0].zfill(6),
                "qmt_code": text,
                "stock_code": text.split(".", 1)[0].zfill(6),
                "exchange": text.split(".", 1)[1] if "." in text else "",
                "weight": weight,
            }
        )
    return {"rows": _records(rows), "errors": {}}


def _index_weight_many(payload: dict[str, Any]) -> dict[str, Any]:
    index_codes = [str(code).strip().upper() for code in (payload.get("index_codes") or []) if str(code).strip()]
    rows: list[dict[str, Any]] = []
    for index_code in index_codes:
        weight_map = xtdata.get_index_weight(index_code) or {}
        for qmt_code, weight in weight_map.items():
            text = str(qmt_code).strip().upper()
            if not text:
                continue
            rows.append(
                {
                    "index_qmt_code": index_code,
                    "index_code": index_code.split(".", 1)[0].zfill(6),
                    "qmt_code": text,
                    "stock_code": text.split(".", 1)[0].zfill(6),
                    "exchange": text.split(".", 1)[1] if "." in text else "",
                    "weight": weight,
                }
            )
    return {"rows": _records(rows), "errors": {}}


def _trading_calendar(payload: dict[str, Any]) -> dict[str, Any]:
    market = str(payload.get("market") or "SH").strip().upper() or "SH"
    start_date = _normalize_qmt_date(str(payload.get("start_date") or ""), include_time=False)
    end_date = _normalize_qmt_date(str(payload.get("end_date") or ""), include_time=False)
    dates = xtdata.get_trading_calendar(market, start_date, end_date) or []
    rows: list[dict[str, Any]] = []
    for item in dates:
        digits = "".join(ch for ch in str(item or "") if ch.isdigit())
        if len(digits) < 8:
            continue
        ymd = digits[:8]
        try:
            trade_dt = datetime.strptime(ymd, "%Y%m%d")
        except ValueError:
            continue
        rows.append(
            {
                "market": market,
                "trade_date": trade_dt.strftime("%Y-%m-%d"),
                "calendar_year": trade_dt.year,
                "trade_status": 1,
                "day_week": trade_dt.isoweekday(),
            }
        )
    return {"rows": _records(rows), "errors": {}}


def _connect() -> int | None:
    if os.environ.get("QMT_ENABLE_HELLO", "0") != "1":
        try:
            xtdata.enable_hello = False
        except Exception:
            pass
    raw_port = str(os.environ.get("QMT_PORT", "") or "").strip()
    port_candidates: list[int] = []
    if raw_port:
        try:
            port_candidates.append(int(raw_port))
        except Exception:
            pass
    for fallback_port in (58610, 58670, 58671, 58672, 58673, 58680):
        if fallback_port not in port_candidates:
            port_candidates.append(fallback_port)

    last_error: Exception | None = None
    for port in port_candidates:
        try:
            xtdata.connect(port=port, remember_if_success=False)
            return port
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    xtdata.connect()
    return None


def _capabilities(connection_port: int | None) -> dict[str, Any]:
    rows = [
        {
            "api_name": name,
            "available": callable(getattr(xtdata, name, None)),
            "provider": PROVIDER_ID,
        }
        for name in OFFICIAL_NATIVE_APIS
    ]
    return {
        "rows": rows,
        "errors": {},
        "provider": PROVIDER_ID,
        "connection_port": connection_port,
        "sdk_module": str(getattr(xtdata, "__file__", "") or ""),
        "sdk_version": str(getattr(xtquant, "__version__", "") or "unknown"),
        "periods": list(OFFICIAL_PERIODS),
    }


def _summarize_probe_value(value: Any) -> dict[str, Any]:
    if isinstance(value, pd.DataFrame):
        return {
            "kind": "dataframe",
            "row_count": int(len(value)),
            "fields": [str(column) for column in value.columns],
        }
    if isinstance(value, dict):
        fields: list[str] = []
        row_count = len(value)
        for nested in value.values():
            if isinstance(nested, pd.DataFrame):
                row_count = sum(len(item) for item in value.values() if isinstance(item, pd.DataFrame))
                fields = sorted({str(column) for item in value.values() if isinstance(item, pd.DataFrame) for column in item.columns})
                break
            if isinstance(nested, dict):
                fields = sorted(str(key) for key in nested.keys())
                break
        return {"kind": "dict", "row_count": int(row_count), "fields": fields}
    if isinstance(value, (list, tuple, set)):
        return {"kind": "list", "row_count": int(len(value)), "fields": []}
    return {"kind": type(value).__name__, "row_count": 0 if value is None else 1, "fields": []}


def _probe_core(connection_port: int | None) -> dict[str, Any]:
    now = datetime.now()
    start = (now - timedelta(days=40)).strftime("%Y%m%d")
    end = now.strftime("%Y%m%d")
    probes: list[tuple[str, Any]] = [
        ("sector_list", lambda: xtdata.get_sector_list()),
        ("stock_universe", lambda: xtdata.get_stock_list_in_sector("沪深A股")),
        ("index_universe", lambda: xtdata.get_stock_list_in_sector("沪深指数")),
        ("qmt_sector_indexes", lambda: xtdata.get_stock_list_in_sector("迅投一级行业板块加权指数")),
        ("stock_instrument", lambda: xtdata.get_instrument_detail("000001.SZ", False)),
        ("index_instrument", lambda: xtdata.get_instrument_detail("000300.SH", False)),
        ("stock_full_tick", lambda: xtdata.get_full_tick(["000001.SZ"])),
        ("index_full_tick", lambda: xtdata.get_full_tick(["000300.SH"])),
        ("index_weight", lambda: xtdata.get_index_weight("000300.SH")),
        ("trading_calendar", lambda: xtdata.get_trading_calendar("SH", start, end)),
        (
            "stock_daily_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="1d",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "index_daily_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000300.SH"],
                period="1d",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_minute_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="1m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "index_minute_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000300.SH"],
                period="1m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_5m_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="5m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_15m_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="15m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_30m_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="30m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_1h_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="1h",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_week_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="1w",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_month_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="1mon",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_tick_bar",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="tick",
                start_time=start,
                end_time=end,
                count=20,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_flow_daily",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="transactioncount1d",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_flow_min",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="transactioncount1m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_orderflow_daily",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="orderflow1d",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_orderflow_min",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="orderflow1m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "northbound_flow_daily",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="northfinancechange1d",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "northbound_flow_min",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="northfinancechange1m",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "interactive_qa",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="interactiveqa",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "announcement",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="announcement",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_quote",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2quote",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_quote_aux",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2quoteaux",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_order",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2order",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_transaction",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2transaction",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_transaction_count",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2transactioncount",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
        (
            "stock_l2_order_queue",
            lambda: xtdata.get_market_data_ex(
                field_list=[],
                stock_list=["000001.SZ"],
                period="l2orderqueue",
                start_time=start,
                end_time=end,
                count=2,
                dividend_type="none",
                fill_data=False,
            ),
        ),
    ]
    rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for name, callback in probes:
        started = time.monotonic()
        try:
            summary = _summarize_probe_value(callback())
            status = "SUPPORTED" if summary["row_count"] > 0 else "NO_DATA"
            rows.append(
                {
                    "probe_name": name,
                    "status": status,
                    "elapsed_ms": int((time.monotonic() - started) * 1000),
                    **summary,
                }
            )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors[name] = message
            lowered = message.casefold()
            if "function not realize" in lowered or "未支持此功能" in message:
                probe_status = "UNSUPPORTED_CLIENT"
            elif "permission" in lowered or "权限" in message or "未授权" in message:
                probe_status = "NOT_AUTHORIZED"
            else:
                probe_status = "FAILED"
            rows.append(
                {
                    "probe_name": name,
                    "status": probe_status,
                    "elapsed_ms": int((time.monotonic() - started) * 1000),
                    "kind": "error",
                    "row_count": 0,
                    "fields": [],
                    "error": message,
                }
            )
    return {
        "rows": rows,
        "errors": errors,
        "provider": PROVIDER_ID,
        "connection_port": connection_port,
        "probed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _refresh_reference_data(connection_port: int | None, payload: dict[str, Any]) -> dict[str, Any]:
    allowed_operations = (
        "download_sector_data",
        "download_index_weight",
        "download_holiday_data",
        "download_history_contracts",
        "download_his_st_data",
    )
    requested = [str(item).strip() for item in (payload.get("operations") or []) if str(item).strip()]
    operations = tuple(item for item in requested if item in allowed_operations) or allowed_operations
    rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for name in operations:
        callback = getattr(xtdata, name, None)
        if not callable(callback):
            rows.append({"api_name": name, "status": "SDK_UNSUPPORTED", "elapsed_ms": 0})
            continue
        started = time.monotonic()
        try:
            callback()
            rows.append(
                {
                    "api_name": name,
                    "status": "SUCCESS",
                    "elapsed_ms": int((time.monotonic() - started) * 1000),
                }
            )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors[name] = message
            lowered = message.casefold()
            if "function not realize" in lowered or "未支持此功能" in message:
                status = "UNSUPPORTED_CLIENT"
            elif "permission" in lowered or "权限" in message or "未授权" in message:
                status = "NOT_AUTHORIZED"
            else:
                status = "FAILED"
            rows.append(
                {
                    "api_name": name,
                    "status": status,
                    "elapsed_ms": int((time.monotonic() - started) * 1000),
                    "error": message,
                }
            )
    return {
        "rows": rows,
        "errors": errors,
        "provider": PROVIDER_ID,
        "connection_port": connection_port,
        "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def dispatch(payload: dict[str, Any], connection_port: int | None = None) -> dict[str, Any]:
    if connection_port is None:
        connection_port = _connect()
    action = payload.get("action")
    if action == "ping":
        return {
            "rows": [
                {
                    "status": "ok",
                    "provider": PROVIDER_ID,
                    "connection_port": connection_port,
                    "sdk_module": str(getattr(xtdata, "__file__", "") or ""),
                    "sdk_version": str(getattr(xtquant, "__version__", "") or "unknown"),
                }
            ],
            "errors": {},
        }
    if action == "capabilities":
        return _capabilities(connection_port)
    if action == "probe_core":
        return _probe_core(connection_port)
    if action == "refresh_reference_data":
        return _refresh_reference_data(connection_port, payload)
    if action == "kline":
        return _kline(payload)
    if action == "minute":
        return _minute(payload)
    if action == "current":
        return _current(payload)
    if action == "tick":
        return _tick(payload)
    if action == "flow_daily":
        return _flow_daily(payload)
    if action == "flow_min":
        return _flow_min(payload)
    if action == "sector_list":
        return _sector_list(payload)
    if action == "sector_members":
        return _sector_members(payload)
    if action == "sector_members_many":
        return _sector_members_many(payload)
    if action == "instrument_details":
        return _instrument_details(payload)
    if action == "index_weight":
        return _index_weight(payload)
    if action == "index_weight_many":
        return _index_weight_many(payload)
    if action == "trading_calendar":
        return _trading_calendar(payload)
    raise ValueError(f"unsupported action: {action}")


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        result = dispatch(payload)
        result["ok"] = True
        print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
