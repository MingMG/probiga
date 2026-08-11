#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch one trading day's A-share daily K data into sm_stock_kline.

This script is for the daily after-market pipeline. It uses the latest complete
previous daily-kline universe as the target stock pool, fetches the requested
date concurrently from Eastmoney, and writes only after coverage passes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from biz.stock_market.stock_kline_akshare import (  # noqa: E402
    _fetch_eastmoney_daily_kline,
    akshare_daily_to_sm_kline,
    em_code_to_sina_symbol,
)
from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline  # noqa: E402
from server.common.batch_db import create_batch_engine, routed_read_engine, write_frame  # noqa: E402
from server.common.config import get_kline_mysql_url  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402

_WORKERS = max(1, int(os.environ.get("KLINE_DAILY_WORKERS", "2")))
_REQUEST_DELAY = float(os.environ.get("KLINE_DAILY_REQUEST_DELAY", "0.25"))
_REQUEST_JITTER = float(os.environ.get("KLINE_DAILY_REQUEST_JITTER", "0.10"))
_MIN_COVERAGE = float(os.environ.get("KLINE_DAILY_MIN_COVERAGE", "0.97"))
_MAX_RETRIES = max(0, int(os.environ.get("KLINE_DAILY_MAX_RETRIES", "2")))
_SOURCES = [s.strip().lower() for s in os.environ.get("KLINE_DAILY_SOURCES", "sina,east").split(",") if s.strip()]
_BATCH_PAUSE = float(os.environ.get("KLINE_DAILY_BATCH_PAUSE", "5.0"))
_BATCH_PAUSE_EVERY = int(os.environ.get("KLINE_DAILY_BATCH_PAUSE_EVERY", "500"))
_COOLDOWN_THRESHOLD = int(os.environ.get("KLINE_DAILY_COOLDOWN_THRESHOLD", "10"))
_COOLDOWN_SECONDS = float(os.environ.get("KLINE_DAILY_COOLDOWN_SECONDS", "20"))
_STREAM_BATCH_SIZE = max(50, int(os.environ.get("KLINE_DAILY_STREAM_BATCH_SIZE", "200")))
_VERIFY_SAMPLE_SIZE = max(0, int(os.environ.get("KLINE_DAILY_VERIFY_SAMPLE_SIZE", "60")))
_VERIFY_MIN_MATCH_RATIO = float(os.environ.get("KLINE_DAILY_VERIFY_MIN_MATCH_RATIO", "0.60"))
_VERIFY_MAX_MISMATCH_RATIO = float(os.environ.get("KLINE_DAILY_VERIFY_MAX_MISMATCH_RATIO", "0"))


@dataclass
class FetchOutcome:
    code: str
    df: pd.DataFrame | None
    source: str = ""
    error: Exception | None = None
    no_data: bool = False


def _normalize_date(raw: str) -> str:
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]


def _to_yyyymmdd(raw: str) -> str:
    return _normalize_date(raw).replace("-", "")


def _fmt_date(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def _distinct_kline_url(primary_url: str) -> str:
    primary = str(primary_url or "").strip()
    try:
        kline_url = get_kline_mysql_url().strip()
    except Exception:
        return ""
    return kline_url if kline_url and kline_url != primary else ""


def _expected_trade_date(engine: Engine) -> str:
    with engine.connect() as conn:
        d = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date <= :today
        """), {"today": date.today().isoformat()}).scalar()
    return _fmt_date(d)


def _read_short_name_map(engine: Engine) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code, short_name FROM si_all_code")).fetchall()
    return {
        str(r[0]).strip().zfill(6): (str(r[1]).strip() if r[1] is not None else "")
        for r in rows
    }


def _latest_previous_universe_date(engine: Engine, target_date: str) -> str:
    min_count = int(os.environ.get("KLINE_DAILY_MIN_UNIVERSE", "1000"))
    with engine.connect() as conn:
        d = conn.execute(text("""
            SELECT trade_date
            FROM sm_stock_kline
            WHERE trade_date < :d AND k_type = 1 AND adjust_type = 0
            GROUP BY trade_date
            HAVING COUNT(DISTINCT stock_code) >= :min_count
            ORDER BY trade_date DESC
            LIMIT 1
        """), {"d": target_date, "min_count": min_count}).scalar()
    return _fmt_date(d)


def _read_stock_codes(engine: Engine, target_date: str) -> tuple[list[str], str]:
    universe_date = _latest_previous_universe_date(engine, target_date)
    if universe_date:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT stock_code
                FROM (
                  SELECT DISTINCT k.stock_code
                  FROM sm_stock_kline k
                  INNER JOIN si_all_code a
                    ON a.stock_code = k.stock_code
                  WHERE k.trade_date = :previous_date
                    AND k.k_type = 1
                    AND k.adjust_type = 0
                    AND k.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                    AND (a.list_date IS NULL OR a.list_date <= :target_date)
                  UNION
                  SELECT a.stock_code
                  FROM si_all_code a
                  WHERE a.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                    AND a.list_date = :target_date
                ) active_universe
                ORDER BY stock_code
            """), {
                "previous_date": universe_date,
                "target_date": target_date,
            }).fetchall()
        return (
            [str(r[0]).strip().zfill(6) for r in rows],
            f"sm_stock_kline:{universe_date}+si_all_code:list_date",
        )

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stock_code
            FROM si_all_code
            WHERE stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
              AND (list_date IS NULL OR list_date <= :d)
            ORDER BY stock_code
        """), {"d": target_date}).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows], "si_all_code"


def _fetch_one(code: str, target_date: str, short_name: str) -> FetchOutcome:
    last_error: Exception | None = None
    for source in _SOURCES:
        try:
            if source == "efinance":
                df = _fetch_efinance_one(code, target_date, short_name)
            elif source in ("sina", "east", "eastmoney", "em"):
                df = _fetch_builtin_one(code, target_date, short_name, source)
            else:
                raise ValueError(f"未知日K数据源: {source}")
            if df is not None and not df.empty:
                return FetchOutcome(code=code, df=df, source=source)
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
            continue
    if last_error is not None:
        return FetchOutcome(code=code, df=None, error=last_error)
    return FetchOutcome(code=code, df=None, no_data=True)


def _fetch_builtin_one(code: str, target_date: str, short_name: str, source: str) -> pd.DataFrame | None:
    api_date = _to_yyyymmdd(target_date)
    history_start = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y%m%d")
    last_error: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            if source == "sina":
                symbol = em_code_to_sina_symbol(code)
                raw = fetch_sina_a_daily_kline(symbol, history_start, api_date, "") if symbol else None
            else:
                raw = _fetch_eastmoney_daily_kline(code, history_start, api_date, "")
            if raw is None or raw.empty:
                return None
            out = akshare_daily_to_sm_kline(raw, code, 1, 0, short_name=short_name)
            if out is None or out.empty:
                return None
            out = out[out["trade_date"].astype(str).str[:10] == target_date]
            if out.empty:
                return None
            return out
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
            if attempt < _MAX_RETRIES:
                time.sleep(1.5 * (2 ** attempt) + random.uniform(0, 0.8))
    if last_error is not None:
        raise last_error
    return None


def _fetch_efinance_one(code: str, target_date: str, short_name: str) -> pd.DataFrame | None:
    import efinance as ef

    d = _to_yyyymmdd(target_date)
    raw = ef.stock.get_quote_history(code, beg=d, end=d)
    if raw is None or raw.empty:
        return None
    rename = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "change_pct",
        "涨跌额": "change",
        "换手率": "turnover",
    }
    df = raw.rename(columns=rename)
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].dt.strftime("%Y-%m-%d") == target_date]
    if df.empty:
        return None
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce") * 100
    for col in ("open", "close", "high", "low", "amount", "change_pct", "change", "turnover"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return akshare_daily_to_sm_kline(df, code, 1, 0, short_name=short_name)


def _normalize_source_name(source: str) -> str:
    value = str(source or "").strip().lower()
    return {
        "em": "east",
        "eastmoney": "east",
        "east_batch": "east",
        "eastmoney_batch": "east",
    }.get(value, value or "unknown")


def _with_data_source(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    out = frame.copy()
    out["_data_source"] = _normalize_source_name(source)
    return out


def _validate_daily_frame(frame: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """Reject structurally impossible daily bars before any replacement write."""
    required = {
        "stock_code", "trade_date", "k_type", "adjust_type",
        "open", "high", "low", "close", "volume", "amount",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"daily K-line frame missing required columns: {missing}")

    out = frame.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    for column in ("open", "high", "low", "close", "volume", "amount"):
        out[column] = pd.to_numeric(out[column], errors="coerce")

    duplicate_mask = out.duplicated(
        subset=["stock_code", "trade_date", "k_type", "adjust_type"],
        keep=False,
    )
    date_mask = out["trade_date"].ne(target_date)
    null_price_mask = out[["open", "high", "low", "close"]].isna().any(axis=1)
    nonpositive_mask = out[["open", "high", "low", "close"]].le(0).any(axis=1)
    bad_range_mask = (
        out["high"].lt(out[["open", "low", "close"]].max(axis=1))
        | out["low"].gt(out[["open", "high", "close"]].min(axis=1))
    )
    bad_activity_mask = out["volume"].lt(0) | out["amount"].lt(0)
    invalid_mask = (
        duplicate_mask
        | date_mask
        | null_price_mask
        | nonpositive_mask
        | bad_range_mask
        | bad_activity_mask
    )
    if invalid_mask.any():
        bad = out.loc[
            invalid_mask,
            ["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"],
        ].head(12)
        summary = {
            "duplicates": int(duplicate_mask.sum()),
            "wrong_date": int(date_mask.sum()),
            "null_price": int(null_price_mask.sum()),
            "nonpositive_price": int(nonpositive_mask.sum()),
            "bad_ohlc_range": int(bad_range_mask.sum()),
            "negative_volume_or_amount": int(bad_activity_mask.sum()),
        }
        raise RuntimeError(
            "daily K-line structural validation failed: "
            f"{summary}; samples={bad.to_dict(orient='records')}"
        )
    return out


def _row_values(frame: pd.DataFrame | None) -> dict[str, float] | None:
    if frame is None or frame.empty:
        return None
    row = frame.iloc[-1]
    values: dict[str, float] = {}
    for column in ("open", "high", "low", "close"):
        value = pd.to_numeric(row.get(column), errors="coerce")
        if pd.isna(value):
            return None
        values[column] = float(value)
    amount = pd.to_numeric(row.get("amount"), errors="coerce")
    if pd.notna(amount):
        values["amount"] = float(amount)
    return values


def _tencent_symbol(code: str) -> str:
    normalized = str(code or "").strip().zfill(6)
    if normalized.startswith("6"):
        return f"sh{normalized}"
    if normalized.startswith(("0", "3")):
        return f"sz{normalized}"
    if normalized.startswith(("4", "8", "9")):
        return f"bj{normalized}"
    return ""


def _parse_tencent_reference(
    payload: dict[str, Any],
    symbol: str,
    target_date: str,
) -> dict[str, float] | None:
    node = (payload.get("data") or {}).get(symbol) or {}
    rows = node.get("day") or node.get("qfqday") or []
    for values in rows:
        if not isinstance(values, list) or len(values) < 5 or str(values[0])[:10] != target_date:
            continue
        try:
            return {
                "open": float(values[1]),
                "close": float(values[2]),
                "high": float(values[3]),
                "low": float(values[4]),
            }
        except (TypeError, ValueError):
            return None
    return None


def _fetch_tencent_reference(code: str, target_date: str) -> dict[str, float] | None:
    import requests

    symbol = _tencent_symbol(code)
    if not symbol:
        return None
    url = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
    response = requests.get(
        url,
        params={"param": f"{symbol},day,{target_date},{target_date},10"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if int(payload.get("code") or 0) != 0:
        return None
    return _parse_tencent_reference(payload, symbol, target_date)


_THS_STOCK_PAGE = "https://stockpage.10jqka.com.cn/920002/"
_THS_KLINE_API = (
    "https://quota-h.10jqka.com.cn/"
    "fuyao/common_hq_aggr/quote/v1/single_kline"
)


@lru_cache(maxsize=1)
def _ths_auth_headers() -> dict[str, str]:
    """Discover the public HXKline credential shipped by the quote page."""
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": _THS_STOCK_PAGE,
    })
    response = session.get(_THS_STOCK_PAGE, timeout=20)
    response.raise_for_status()
    script_urls = re.findall(
        r'<script[^>]+src=["\']([^"\']+)["\']',
        unescape(response.text),
        flags=re.IGNORECASE,
    )
    auth: tuple[str, str] | None = None
    for raw_url in script_urls:
        script_url = (
            f"https:{raw_url}"
            if raw_url.startswith("//")
            else urljoin(_THS_STOCK_PAGE, raw_url)
        )
        script_response = session.get(script_url, timeout=20)
        script_response.raise_for_status()
        matched = re.search(
            r'id:"(hxkline-[^"]+)",token:"([^"]+)"',
            script_response.text,
        )
        if matched:
            auth = (matched.group(1), matched.group(2))
            break
    if auth is None:
        raise RuntimeError("Tonghuashun HXKline public authorization was not found")
    source_id, token = auth
    return {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Referer": _THS_STOCK_PAGE,
        "Origin": "https://stockpage.10jqka.com.cn",
        "X-Fuyao-Auth": token,
        "Source-Id": source_id,
        "Platform": "hxkline",
        "X-Auth-Type": "ths",
        "X-Auth-Version": "1.0",
        "X-Auth-ProgId": "7047",
        "X-Auth-AppName": "AINVEST",
    }


def _parse_ths_history(payload: dict[str, Any], code: str) -> pd.DataFrame:
    if int(payload.get("status_code") or 0) != 0:
        return pd.DataFrame()
    quote_rows = ((payload.get("data") or {}).get("quote_data") or [])
    if not quote_rows:
        return pd.DataFrame()
    node = next(
        (
            item for item in quote_rows
            if str(item.get("code") or "").strip().zfill(6) == str(code).zfill(6)
        ),
        quote_rows[0],
    )
    fields = [str(value) for value in (node.get("data_fields") or [])]
    field_positions = {field: offset for offset, field in enumerate(fields)}
    required = {"1", "7", "8", "9", "11"}
    if not required.issubset(field_positions):
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for values in node.get("value") or []:
        if not isinstance(values, list) or len(values) < len(fields):
            continue
        try:
            timestamp = pd.to_datetime(
                float(values[field_positions["1"]]),
                unit="ms",
                utc=True,
            ).tz_convert("Asia/Shanghai")
            record = {
                "trade_date": timestamp.strftime("%Y-%m-%d"),
                "open": float(values[field_positions["7"]]),
                "high": float(values[field_positions["8"]]),
                "low": float(values[field_positions["9"]]),
                "close": float(values[field_positions["11"]]),
            }
            if "13" in field_positions:
                record["volume"] = float(values[field_positions["13"]])
            if "19" in field_positions:
                record["amount"] = float(values[field_positions["19"]])
        except (TypeError, ValueError, OverflowError):
            continue
        records.append(record)
    return pd.DataFrame(records).drop_duplicates(
        subset=["trade_date"],
        keep="last",
    )


def _fetch_ths_history(
    code: str,
    *,
    count: int = 60,
    adjust_type: str = "actual",
) -> pd.DataFrame:
    """Fetch licensed-provider daily history for an A-share code."""
    import requests

    normalized = str(code or "").strip().zfill(6)
    if normalized.startswith("6"):
        market = "17"
    elif normalized.startswith(("0", "3")):
        market = "33"
    else:
        market = "151"
    payload = {
        "code_list": [{"codes": [normalized], "market": market}],
        "trade_class": "intraday",
        "time_period": "day_1",
        "trade_date": -1,
        "begin_time": -max(2, int(count)),
        "end_time": 0,
        "adjust_type": str(adjust_type or "actual"),
        "gpid": 0,
    }
    response = requests.post(
        _THS_KLINE_API,
        headers=_ths_auth_headers(),
        data=json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        timeout=25,
    )
    response.raise_for_status()
    return _parse_ths_history(response.json(), normalized)


def _fetch_ths_reference(code: str, target_date: str) -> dict[str, float] | None:
    frame = _fetch_ths_history(code, count=60, adjust_type="actual")
    if frame.empty:
        return None
    rows = frame[frame["trade_date"].astype(str).str[:10] == target_date]
    if rows.empty:
        return None
    row = rows.iloc[-1]
    return {
        column: float(row[column])
        for column in ("open", "high", "low", "close")
    }


def _fetch_independent_reference(
    code: str,
    target_date: str,
) -> tuple[str, dict[str, float] | None]:
    normalized = str(code or "").strip().zfill(6)
    if normalized.startswith(("4", "8", "92")):
        return "ths", _fetch_ths_reference(normalized, target_date)
    return "tencent", _fetch_tencent_reference(normalized, target_date)


def _rows_match(
    primary: dict[str, float],
    reference: dict[str, float],
    *,
    price_tolerance: float = 0.011,
    amount_relative_tolerance: float = 0.002,
) -> tuple[bool, dict[str, float]]:
    differences: dict[str, float] = {}
    matched = True
    for column in ("open", "high", "low", "close"):
        delta = abs(float(primary[column]) - float(reference[column]))
        differences[column] = round(delta, 6)
        if delta > price_tolerance:
            matched = False
    if "amount" in primary and "amount" in reference:
        amount_base = max(abs(float(primary["amount"])), abs(float(reference["amount"])), 1.0)
        amount_delta = abs(float(primary["amount"]) - float(reference["amount"])) / amount_base
        differences["amount_relative"] = round(amount_delta, 8)
        if amount_delta > amount_relative_tolerance:
            matched = False
    return matched, differences


def _cross_validate_daily_frame(
    frame: pd.DataFrame,
    target_date: str,
    short_names: dict[str, str],
    *,
    sample_size: int,
) -> dict[str, Any]:
    """Cross-check a deterministic sample against an independent provider."""
    if sample_size <= 0 or frame.empty:
        return {
            "status": "skipped",
            "requested": 0,
            "compared": 0,
            "matched": 0,
            "mismatched": 0,
            "unavailable": 0,
            "verified_codes": {},
        }

    source_by_code = {
        str(row["stock_code"]).zfill(6): _normalize_source_name(row.get("_data_source", "unknown"))
        for row in frame[["stock_code", "_data_source"]].to_dict(orient="records")
    }
    primary_by_code = {
        str(row["stock_code"]).zfill(6): {
            column: float(row[column])
            for column in ("open", "high", "low", "close", "amount")
        }
        for row in frame[
            ["stock_code", "open", "high", "low", "close", "amount"]
        ].to_dict(orient="records")
    }
    ordered_codes = sorted(
        primary_by_code,
        key=lambda code: hashlib.sha256(f"{target_date}:{code}".encode()).hexdigest(),
    )
    sample_codes = ordered_codes[: min(sample_size, len(ordered_codes))]

    def _verify(code: str) -> dict[str, Any]:
        primary_source = source_by_code.get(code, "unknown")
        reference_source = "ths" if code.startswith(("4", "8", "92")) else "tencent"
        try:
            reference_source, reference = _fetch_independent_reference(code, target_date)
            if reference is None:
                return {
                    "code": code,
                    "primary_source": primary_source,
                    "reference_source": reference_source,
                    "status": "unavailable",
                }
            matched, differences = _rows_match(primary_by_code[code], reference)
            return {
                "code": code,
                "primary_source": primary_source,
                "reference_source": reference_source,
                "status": "matched" if matched else "mismatched",
                "differences": differences,
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "code": code,
                "primary_source": primary_source,
                "reference_source": reference_source,
                "status": "unavailable",
                "error": f"{type(exc).__name__}: {str(exc)[:160]}",
            }

    workers = min(max(_WORKERS, 1), 4, len(sample_codes))
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = [pool.submit(_verify, code) for code in sample_codes]
        for future in as_completed(futures):
            results.append(future.result())

    matched_rows = [row for row in results if row["status"] == "matched"]
    mismatched_rows = [row for row in results if row["status"] == "mismatched"]
    unavailable_rows = [row for row in results if row["status"] == "unavailable"]
    compared = len(matched_rows) + len(mismatched_rows)
    min_compared = max(1, int(len(sample_codes) * _VERIFY_MIN_MATCH_RATIO))
    mismatch_ratio = len(mismatched_rows) / max(compared, 1)
    status = (
        "pass"
        if compared >= min_compared and mismatch_ratio <= _VERIFY_MAX_MISMATCH_RATIO
        else "fail"
    )
    verified_codes = {
        row["code"]: row["reference_source"]
        for row in matched_rows
    }
    return {
        "status": status,
        "requested": len(sample_codes),
        "minimum_compared": min_compared,
        "compared": compared,
        "matched": len(matched_rows),
        "mismatched": len(mismatched_rows),
        "unavailable": len(unavailable_rows),
        "mismatch_ratio": round(mismatch_ratio, 6),
        "verified_codes": verified_codes,
        "mismatch_samples": mismatched_rows[:10],
        "unavailable_samples": unavailable_rows[:10],
    }


def _ensure_provenance_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_kline_ingestion_run (
              run_id VARCHAR(36) NOT NULL,
              target_date DATE NOT NULL,
              mode VARCHAR(32) NOT NULL,
              source_chain VARCHAR(255) NOT NULL,
              universe_source VARCHAR(128) NOT NULL,
              expected_count INT NOT NULL,
              fetched_count INT NOT NULL,
              coverage DECIMAL(10,6) NOT NULL,
              source_counts_json LONGTEXT NOT NULL,
              cross_validation_json LONGTEXT NOT NULL,
              dataset_sha256 CHAR(64) NOT NULL,
              status VARCHAR(24) NOT NULL,
              started_at DATETIME NOT NULL,
              finished_at DATETIME NOT NULL,
              PRIMARY KEY (run_id),
              KEY idx_kline_ingestion_date (target_date, status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_kline_source_trace (
              trade_date DATE NOT NULL,
              stock_code VARCHAR(10) NOT NULL,
              k_type INT NOT NULL,
              adjust_type INT NOT NULL,
              data_source VARCHAR(32) NOT NULL,
              verification_status VARCHAR(24) NOT NULL,
              verified_source VARCHAR(32) NULL,
              row_sha256 CHAR(64) NOT NULL,
              run_id VARCHAR(36) NOT NULL,
              fetched_at DATETIME NOT NULL,
              PRIMARY KEY (trade_date, stock_code, k_type, adjust_type),
              KEY idx_kline_trace_run (run_id),
              KEY idx_kline_trace_source (data_source, verification_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


def _dataset_hash(frame: pd.DataFrame) -> str:
    columns = [
        "stock_code", "trade_date", "k_type", "adjust_type",
        "open", "high", "low", "close", "volume", "amount", "pre_close",
    ]
    canonical = frame[[column for column in columns if column in frame.columns]].copy()
    canonical = canonical.sort_values(["stock_code", "trade_date", "k_type", "adjust_type"])
    payload = canonical.to_json(orient="records", date_format="iso", double_precision=10)
    return hashlib.sha256(payload.encode()).hexdigest()


def _build_source_trace(
    frame: pd.DataFrame,
    *,
    run_id: str,
    verified_codes: dict[str, str],
    fetched_at: datetime,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    hash_columns = (
        "stock_code", "trade_date", "k_type", "adjust_type",
        "open", "high", "low", "close", "volume", "amount", "pre_close",
    )
    for row in frame.to_dict(orient="records"):
        code = str(row.get("stock_code") or "").zfill(6)
        source = _normalize_source_name(row.get("_data_source", "unknown"))
        verified_source = verified_codes.get(code)
        canonical = {
            column: (
                None
                if pd.isna(row.get(column))
                else str(row.get(column))
            )
            for column in hash_columns
        }
        row_hash = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        records.append({
            "trade_date": str(row.get("trade_date") or "")[:10],
            "stock_code": code,
            "k_type": int(row.get("k_type") or 1),
            "adjust_type": int(row.get("adjust_type") or 0),
            "data_source": source,
            "verification_status": "cross_checked" if verified_source else "source_only",
            "verified_source": verified_source,
            "row_sha256": row_hash,
            "run_id": run_id,
            "fetched_at": fetched_at,
        })
    return pd.DataFrame(records)


def _batch_items_to_daily_frame(items: list[dict], target_date: str) -> pd.DataFrame:
    """Build the latest completed daily bar from one full-market quote batch."""
    from tools.crawl_realtime_batch import _normalize_a_share_code, _quote_values, safe_float

    rows: list[dict] = []
    trade_time = f"{target_date} 15:00:00"
    for item in items:
        code = _normalize_a_share_code(item.get("f12"))
        if not code:
            continue
        quote = _quote_values(item)
        if quote["price"] <= 0:
            continue
        pre_close = safe_float(item.get("f18"))
        rows.append({
            "stock_code": code,
            "short_name": str(item.get("f14") or "").strip(),
            "trade_time": trade_time,
            "trade_date": target_date,
            "k_type": 1,
            "adjust_type": 0,
            "open": quote["open"],
            "close": quote["price"],
            "high": quote["high"],
            "low": quote["low"],
            # Eastmoney f5 is reported in hands for A shares; daily K stores shares.
            "volume": safe_float(item.get("f5")) * 100.0,
            "amount": safe_float(item.get("f6")),
            "change": quote["price"] - pre_close if pre_close > 0 else None,
            "change_pct": (
                (quote["price"] / pre_close - 1.0) * 100.0
                if pre_close > 0 else None
            ),
            "turnover_ratio": safe_float(item.get("f8")),
            "pre_close": pre_close if pre_close > 0 else None,
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["stock_code"], keep="last")


def _fetch_latest_batch_daily(target_date: str) -> pd.DataFrame:
    """Fetch the latest complete A-share daily bars in one paginated request path."""
    from tools.crawl_realtime_batch import EASTMONEY_A_SHARE_FS, fetch_batch

    items = fetch_batch(
        EASTMONEY_A_SHARE_FS,
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18",
    )
    return _batch_items_to_daily_frame(items, target_date)


def _delete_daily_kline(engine: Engine, target_date: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
        """), {"d": target_date})


def _apply_reference_prices(
    frame: pd.DataFrame,
    quote_map: dict[str, tuple[float, float]],
    previous_close_map: dict[str, float],
    *,
    tolerance: float = 0.005,
) -> tuple[pd.DataFrame, dict[str, int | float]]:
    """Fill pre-close/change fields and cross-check the daily close against a live batch quote."""
    out = frame.copy()
    for column in ("close", "pre_close", "change", "change_pct"):
        out[column] = pd.to_numeric(out.get(column), errors="coerce")
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)

    compared = mismatches = quote_references = previous_references = 0
    resolved_pre_close: list[float | None] = []
    for row in out[["stock_code", "close", "pre_close"]].to_dict(orient="records"):
        code = str(row.get("stock_code") or "").zfill(6)
        close = float(row.get("close") or 0)
        existing = float(row.get("pre_close") or 0) if pd.notna(row.get("pre_close")) else 0.0
        quote_price, quote_pre_close = quote_map.get(code, (0.0, 0.0))
        if close > 0 and quote_price > 0:
            compared += 1
            if abs(close / quote_price - 1.0) > tolerance:
                mismatches += 1
            elif quote_pre_close > 0:
                existing = quote_pre_close
                quote_references += 1
        if existing <= 0:
            existing = float(previous_close_map.get(code) or 0)
            if existing > 0:
                previous_references += 1
        resolved_pre_close.append(existing if existing > 0 else None)

    out["pre_close"] = resolved_pre_close
    valid = out["pre_close"].notna() & out["pre_close"].gt(0) & out["close"].notna() & out["close"].gt(0)
    out.loc[valid, "change"] = out.loc[valid, "close"] - out.loc[valid, "pre_close"]
    out.loc[valid, "change_pct"] = out.loc[valid, "change"] / out.loc[valid, "pre_close"] * 100.0
    return out, {
        "compared": compared,
        "mismatches": mismatches,
        "quote_references": quote_references,
        "previous_references": previous_references,
        "valid_change_fields": int(valid.sum()),
        "rows": len(out),
        "valid_ratio": round(float(valid.mean()) if len(out) else 0.0, 6),
    }


def _enrich_daily_change_fields(engine: Engine, target_date: str, frame: pd.DataFrame) -> pd.DataFrame:
    read_engine = routed_read_engine("SELECT stock_code, close FROM sm_stock_kline", engine)
    with read_engine.connect() as conn:
        previous_date = conn.execute(text("""
            SELECT MAX(trade_date) FROM sm_stock_kline
            WHERE trade_date < :d AND k_type=1 AND adjust_type=0
        """), {"d": target_date}).scalar()
        previous_rows = conn.execute(text("""
            SELECT stock_code, close FROM sm_stock_kline
            WHERE trade_date=:d AND k_type=1 AND adjust_type=0
        """), {"d": previous_date}).fetchall() if previous_date else []
    previous_close_map = {
        str(code).strip().zfill(6): float(close)
        for code, close in previous_rows
        if close is not None and float(close) > 0
    }

    quote_map: dict[str, tuple[float, float]] = {}
    if target_date == _expected_trade_date(engine):
        from tools.crawl_realtime_batch import EASTMONEY_A_SHARE_FS, fetch_batch, safe_float

        items = fetch_batch(
            EASTMONEY_A_SHARE_FS,
            "f2,f12,f18",
        )
        quote_map = {
            str(item.get("f12") or "").strip().zfill(6): (
                safe_float(item.get("f2")),
                safe_float(item.get("f18")),
            )
            for item in items
            if str(item.get("f12") or "").strip()
        }

    tolerance = float(os.environ.get("KLINE_DAILY_QUOTE_TOLERANCE", "0.005"))
    enriched, stats = _apply_reference_prices(
        frame,
        quote_map,
        previous_close_map,
        tolerance=max(0.0001, tolerance),
    )
    max_mismatch_ratio = float(os.environ.get("KLINE_DAILY_MAX_QUOTE_MISMATCH_RATIO", "0.01"))
    mismatch_ratio = int(stats["mismatches"]) / max(int(stats["compared"]), 1)
    min_change_coverage = float(os.environ.get("KLINE_DAILY_MIN_CHANGE_FIELD_COVERAGE", "0.98"))
    if int(stats["compared"]) >= 1000 and mismatch_ratio > max_mismatch_ratio:
        raise RuntimeError(
            f"daily K-line close cross-check failed: {stats['mismatches']}/{stats['compared']} "
            f"({mismatch_ratio:.2%}) exceed {max_mismatch_ratio:.2%}"
        )
    if float(stats["valid_ratio"]) < min_change_coverage:
        raise RuntimeError(
            f"daily K-line change-field coverage too low: {stats['valid_change_fields']}/{stats['rows']} "
            f"({float(stats['valid_ratio']):.2%}) < {min_change_coverage:.2%}"
        )
    print(f"  Close/pre-close cross-check: {stats}")
    return enriched


def _write_daily_kline(
    engine: Engine,
    target_date: str,
    df: pd.DataFrame,
    *,
    replace_existing: bool = True,
    provenance: dict[str, Any] | None = None,
) -> int:
    full_df = df.replace({np.nan: None, pd.NaT: None})
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date", "k_type", "adjust_type"], keep="last")
    full_df = _validate_daily_frame(full_df, target_date)
    full_df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    columns = [
        "stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
        "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
        "turnover_ratio", "pre_close", "etl_sync_at",
    ]
    run_record: dict[str, Any] | None = None
    trace_frame = pd.DataFrame()
    if provenance is not None:
        _ensure_provenance_tables(engine)
        finished_at = datetime.now().replace(microsecond=0)
        run_id = str(provenance.get("run_id") or uuid.uuid4())
        cross_validation = dict(provenance.get("cross_validation") or {})
        trace_frame = _build_source_trace(
            full_df,
            run_id=run_id,
            verified_codes=dict(cross_validation.get("verified_codes") or {}),
            fetched_at=finished_at,
        )
        run_record = {
            "run_id": run_id,
            "target_date": target_date,
            "mode": str(provenance.get("mode") or "history_per_stock"),
            "source_chain": ",".join(_SOURCES),
            "universe_source": str(provenance.get("universe_source") or ""),
            "expected_count": int(provenance.get("expected_count") or 0),
            "fetched_count": len(full_df),
            "coverage": round(float(provenance.get("coverage") or 0.0), 6),
            "source_counts_json": json.dumps(
                provenance.get("source_counts") or {},
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ),
            "cross_validation_json": json.dumps(
                cross_validation,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ),
            "dataset_sha256": _dataset_hash(full_df),
            "status": "written",
            "started_at": provenance.get("started_at") or finished_at,
            "finished_at": finished_at,
        }
    if replace_existing:
        # Replace the complete day in one transaction.  The old streaming
        # writer deleted the day first and appended batches one by one; a
        # second scheduler process could interleave its own delete and append.
        with engine.begin() as conn:
            conn.execute(
                text("""
                    DELETE FROM sm_stock_kline
                    WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                """),
                {"d": target_date},
            )
            written = write_frame(
                full_df[columns],
                "sm_stock_kline",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            if run_record is not None:
                conn.execute(
                    text("""
                        DELETE FROM st_kline_source_trace
                        WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                    """),
                    {"d": target_date},
                )
                write_frame(
                    trace_frame,
                    "st_kline_source_trace",
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
                conn.execute(text("""
                    INSERT INTO st_kline_ingestion_run (
                      run_id, target_date, mode, source_chain, universe_source,
                      expected_count, fetched_count, coverage, source_counts_json,
                      cross_validation_json, dataset_sha256, status, started_at, finished_at
                    ) VALUES (
                      :run_id, :target_date, :mode, :source_chain, :universe_source,
                      :expected_count, :fetched_count, :coverage, :source_counts_json,
                      :cross_validation_json, :dataset_sha256, :status, :started_at, :finished_at
                    )
                """), run_record)
            return written
    return write_frame(
        full_df[columns],
        "sm_stock_kline",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )


def _mirror_daily_kline_to_read_db(
    target_date: str,
    df: pd.DataFrame,
    primary_url: str,
    *,
    replace_existing: bool = True,
) -> int:
    kline_url = _distinct_kline_url(primary_url)
    if not kline_url:
        return 0
    mirror_engine = create_batch_engine(kline_url)
    return _write_daily_kline(mirror_engine, target_date, df, replace_existing=replace_existing)


def _fetch_daily_kline_unlocked(
    engine: Engine,
    target_date: str = "",
    *,
    min_coverage: float | None = None,
    dry_run: bool = False,
    verify_sample_size: int | None = None,
) -> int:
    run_started_at = datetime.now().replace(microsecond=0)
    target_date = _normalize_date(target_date) if target_date else _expected_trade_date(engine)
    if not target_date:
        print("无法确定目标交易日：si_trade_calendar 无可用交易日")
        return 2
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {target_date}")
        return 2

    stock_codes, universe_source = _read_stock_codes(engine, target_date)
    max_stocks = int(os.environ.get("KLINE_DAILY_MAX_STOCKS", "0"))
    if max_stocks > 0:
        stock_codes = stock_codes[:max_stocks]
    min_coverage = _MIN_COVERAGE if min_coverage is None else min_coverage
    verify_sample_size = _VERIFY_SAMPLE_SIZE if verify_sample_size is None else max(0, verify_sample_size)
    short_names = _read_short_name_map(engine)

    print(f"开始获取个股日K，目标日期: {target_date}")
    print(f"股票池: {universe_source}, 共 {len(stock_codes)} 只")
    print(f"数据源链: {' -> '.join(_SOURCES)}, 并发: {_WORKERS}, 最小覆盖率: {min_coverage:.0%}, dry_run={dry_run}")

    expected_date = _expected_trade_date(engine)
    latest_mode = os.environ.get("KLINE_DAILY_LATEST_MODE", "batch").strip().lower()
    if target_date == expected_date and latest_mode not in {"per_stock", "legacy", "history"}:
        started = time.time()
        full_df = _with_data_source(_fetch_latest_batch_daily(target_date), "east")
        if full_df.empty:
            print("latest daily K-line batch source returned no valid rows")
            return 2
        expected_codes = set(stock_codes)
        full_df = full_df[
            full_df["stock_code"].astype(str).str.zfill(6).isin(expected_codes)
        ].copy()
        fetched_codes = set(full_df["stock_code"].astype(str).str.zfill(6))
        matched = len(expected_codes & fetched_codes)
        coverage = matched / max(len(expected_codes), 1)
        missing = sorted(expected_codes - fetched_codes)
        print(
            f"  Batch latest daily bars: rows={len(full_df)}, expected_matches={matched}/"
            f"{len(expected_codes)} ({coverage:.1%}), new_codes={len(fetched_codes - expected_codes)}, "
            f"missing_sample={missing[:10]}"
        )
        if coverage < min_coverage:
            print(
                f"latest daily K-line batch coverage {coverage:.1%} below threshold "
                f"{min_coverage:.0%}; existing data retained"
            )
            return 3
        full_df = _validate_daily_frame(full_df, target_date)
        full_df = _enrich_daily_change_fields(engine, target_date, full_df)
        cross_validation = _cross_validate_daily_frame(
            full_df,
            target_date,
            short_names,
            sample_size=verify_sample_size,
        )
        print(f"  Cross-source validation: {cross_validation}")
        if cross_validation["status"] == "fail":
            print("cross-source validation failed; existing data retained")
            return 5
        elapsed_total = time.time() - started
        print(f"  Batch latest daily K-line completed in {elapsed_total:.1f}s")
        if dry_run:
            print(
                "[dry-run] batch coverage and cross-check passed; database unchanged; "
                f"dataset_sha256={_dataset_hash(full_df)}"
            )
            return 0
        written = _write_daily_kline(
            engine,
            target_date,
            full_df,
            replace_existing=True,
            provenance={
                "mode": "latest_batch",
                "universe_source": universe_source,
                "expected_count": len(stock_codes),
                "coverage": coverage,
                "source_counts": {"east": len(full_df)},
                "cross_validation": cross_validation,
                "started_at": run_started_at,
            },
        )
        print(f"daily K-line write completed: sm_stock_kline {target_date}, rows={written}")
        mirrored = _mirror_daily_kline_to_read_db(
            target_date,
            full_df,
            str(engine.url),
            replace_existing=True,
        )
        if mirrored:
            print(f"K-line read DB mirror completed: sm_stock_kline {target_date}, rows={mirrored}")
        return 0

    parts: list[pd.DataFrame] = []
    errors: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    success = failed = no_data = done = 0
    started = time.time()
    consecutive_fails = 0  # 连续失败计数，用于触发冷却

    def _worker(code: str) -> FetchOutcome:
        outcome = _fetch_one(code, target_date, short_names.get(code, ""))
        time.sleep(_REQUEST_DELAY + random.uniform(0, _REQUEST_JITTER))
        return outcome

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = {pool.submit(_worker, code): code for code in stock_codes}
        for future in as_completed(futures):
            done += 1
            code = futures[future]
            try:
                outcome = future.result()
            except Exception as e:  # pylint: disable=broad-except
                outcome = FetchOutcome(code=code, df=None, error=e)

            if outcome.df is not None and not outcome.df.empty:
                parts.append(_with_data_source(outcome.df, outcome.source))
                success += 1
                source_counter[outcome.source or "unknown"] += 1
                consecutive_fails = 0
            elif outcome.no_data:
                no_data += 1
                consecutive_fails = 0
            else:
                failed += 1
                consecutive_fails += 1
                errors[type(outcome.error).__name__ if outcome.error else "unknown"] += 1
                if failed <= 12:
                    print(f"  {code} 失败: {outcome.error}")

                # 连续失败过多时触发冷却，避免被服务器封IP
                if consecutive_fails >= _COOLDOWN_THRESHOLD:
                    print(f"  [cooldown] consecutive failures={consecutive_fails}; sleeping {_COOLDOWN_SECONDS:.0f}s...")
                    time.sleep(_COOLDOWN_SECONDS)
                    consecutive_fails = 0

            if done % 500 == 0 or done == len(stock_codes):
                elapsed = time.time() - started
                speed = done / elapsed if elapsed > 0 else 0
                eta = (len(stock_codes) - done) / speed if speed > 0 else 0
                print(
                    f"  进度 {done}/{len(stock_codes)} 成功={success} 无数据={no_data} 失败={failed} "
                    f"已用 {elapsed:.0f}s 预计剩余 {eta:.0f}s"
                )

            # 每处理 N 只股票暂停一段时间
            # Keep the resident set bounded. Partial writes are removed when
            # the final coverage is below the gate.
            if _BATCH_PAUSE_EVERY > 0 and done % _BATCH_PAUSE_EVERY == 0 and done < len(stock_codes):
                pause = _BATCH_PAUSE + random.uniform(0, 10)
                print(f"  批次暂停 {pause:.0f}s（已处理 {done} 只）...")
                time.sleep(pause)

    if not success:
        print("未获取到任何日K数据")
        return 2

    # One daily snapshot is small enough to keep in memory.  Do not write
    # partial batches: a failed run must not destroy a previously good day.
    full_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    full_df = full_df.drop_duplicates(
        subset=["stock_code", "trade_date", "k_type", "adjust_type"],
        keep="last",
    )
    coverage = len(full_df) / max(len(stock_codes), 1)

    if coverage >= min_coverage:
        full_df = _validate_daily_frame(full_df, target_date)
        full_df = _enrich_daily_change_fields(engine, target_date, full_df)
        cross_validation = _cross_validate_daily_frame(
            full_df,
            target_date,
            short_names,
            sample_size=verify_sample_size,
        )
    else:
        cross_validation = {
            "status": "skipped",
            "reason": "coverage_below_threshold",
            "verified_codes": {},
        }

    elapsed_total = time.time() - started
    print("\n===== 汇总 =====")
    print(f"  总数: {len(stock_codes)}")
    print(f"  成功: {success}")
    print(f"  无数据: {no_data}")
    print(f"  失败: {failed}")
    if errors:
        print(f"  失败分类: {dict(errors)}")
    if source_counter:
        print(f"  数据源命中: {dict(source_counter)}")
    print(f"  去重后行数: {len(full_df)}")
    print(f"  覆盖率: {len(full_df)}/{len(stock_codes)} ({coverage:.1%})")
    print(f"  耗时: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")

    print(f"  Cross-source validation: {cross_validation}")

    if coverage < min_coverage:
        print(f"覆盖率 {coverage:.1%} 低于阈值 {min_coverage:.0%}，已停止写库")
        return 3
    if cross_validation["status"] == "fail":
        print("cross-source validation failed; existing data retained")
        return 5
    if dry_run:
        print("[dry-run] 覆盖率达标，但不写入数据库")
        print(f"[dry-run] dataset_sha256={_dataset_hash(full_df)}")
        return 0

    written = _write_daily_kline(
        engine,
        target_date,
        full_df,
        replace_existing=True,
        provenance={
            "mode": "history_per_stock",
            "universe_source": universe_source,
            "expected_count": len(stock_codes),
            "coverage": coverage,
            "source_counts": dict(source_counter),
            "cross_validation": cross_validation,
            "started_at": run_started_at,
        },
    )
    print(f"写入完成: sm_stock_kline {target_date}, 共 {written} 行")
    mirrored = _mirror_daily_kline_to_read_db(
        target_date,
        full_df,
        str(engine.url),
        replace_existing=True,
    )
    if mirrored:
        print(f"K-line read DB mirror completed: sm_stock_kline {target_date}, rows={mirrored}")
    return 0


def fetch_daily_kline(
    target_date: str = "",
    *,
    min_coverage: float | None = None,
    dry_run: bool = False,
    verify_sample_size: int | None = None,
) -> int:
    """Fetch and atomically replace one day under a cross-process lock."""
    engine = create_batch_engine()
    lock_timeout = max(0, int(os.environ.get("KLINE_DAILY_LOCK_TIMEOUT", "30")))
    try:
        with mysql_named_lock(
            engine,
            "probiga:stock_kline_daily",
            timeout_seconds=lock_timeout,
        ):
            return _fetch_daily_kline_unlocked(
                engine,
                target_date,
                min_coverage=min_coverage,
                dry_run=dry_run,
                verify_sample_size=verify_sample_size,
            )
    except TimeoutError as exc:
        print(f"K-line daily sync skipped: {exc}", file=sys.stderr)
        return 4


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定交易日个股日K（并发+覆盖率保护）")
    parser.add_argument("date", nargs="?", default="", help="目标交易日 YYYY-MM-DD；不传则取交易日历最新开市日")
    parser.add_argument("--min-coverage", type=float, default=None, help="写库前最小覆盖率，默认读 KLINE_DAILY_MIN_COVERAGE 或 0.97")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并检查覆盖率，不写库")
    parser.add_argument(
        "--verify-sample-size",
        type=int,
        default=None,
        help="deterministic cross-source verification sample size; default KLINE_DAILY_VERIFY_SAMPLE_SIZE or 60",
    )
    args = parser.parse_args()
    return fetch_daily_kline(
        args.date,
        min_coverage=args.min_coverage,
        dry_run=args.dry_run,
        verify_sample_size=args.verify_sample_size,
    )


if __name__ == "__main__":
    raise SystemExit(main())
