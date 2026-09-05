#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中批量数据刷新
================
一次运行刷新：行情快照、资金流向、概念行情、指数行情。
用 push2delay 批量接口，全市场一次拿完。

用法:
  python tools/crawl_realtime_batch.py           # 刷新全部
  python tools/crawl_realtime_batch.py --only snapshot
  python tools/crawl_realtime_batch.py --only flow
  python tools/crawl_realtime_batch.py --only concept
  python tools/crawl_realtime_batch.py --only index
"""

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import urllib3
from sqlalchemy import text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import (
    create_batch_engine,
    quote_identifier,
    replace_table_rows,
    write_frame,
)
from server.common.mysql_lock import (
    CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
    mysql_named_lock,
)
from biz.stock_market.realtime_quotes import _ensure_rt_snapshot_table

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://data.eastmoney.com/",
})
SESSION.trust_env = False
SESSION.verify = False

BATCH_API = "https://push2delay.eastmoney.com/api/qt/clist/get"
CAPITAL_FLOW_RESULT_SCHEMA = "probiga.capital-flow-batch-result.v1"
CAPITAL_FLOW_TASK_TYPE = "capital_flow_batch_fast"
CAPITAL_FLOW_DATASET = "stock_capital_flow_daily"
SHANGHAI = ZoneInfo("Asia/Shanghai")
CAPITAL_FLOW_MARKETS = (
    "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
    "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2,"
    # Eastmoney moved newly listed Beijing Stock Exchange securities to this
    # selector.  The legacy t:80/t:7 filters do not return 920xxx stocks.
    "m:0+t:81+s:2048"
)
CAPITAL_FLOW_FIELDS = {
    "main_net_inflow": "f62",
    "max_net_inflow": "f66",
    "lg_net_inflow": "f72",
    "mid_net_inflow": "f78",
    "sm_net_inflow": "f84",
}
# Production schema is VARCHAR(16); keep a precise, non-aliased provider id.
CAPITAL_FLOW_PRIMARY_SOURCE = "east_push2delay"
# Formal publication only uses fallbacks whose response carries a source-side
# stock/market identity.  The legacy Baidu helper writes the requested code
# back into the result and converts missing components to zero, so it is not an
# admissible exact-coverage source.
# Both names are written by existing Eastmoney historical collectors.
# Accept their actual provenance without rewriting the persisted source.
CAPITAL_FLOW_FALLBACK_SOURCES = ("push2his", "push2hist")
CAPITAL_FLOW_PUSH2HIS_API = (
    "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
)
CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING = "verified_existing_exact"
CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR = "historical_exact_fallback_repair"
CAPITAL_FLOW_EXECUTION_CURRENT_LIVE = "current_live_refresh"
CAPITAL_FLOW_LIVE_READY_HHMM = 1520
_FLOW_CODE_RE = re.compile(r"^[0-9]{6}$")
_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")


def _signed_receipt(payload: dict) -> dict:
    result = dict(payload)
    canonical = json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    result["receipt_id"] = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
    return result


def _scheduler_build_sha() -> str:
    """Return the scheduler-bound immutable build identity for a formal receipt."""

    value = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if _SHA40_RE.fullmatch(value) is None or value == "0" * 40:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow scheduler build SHA is unavailable"
        )
    return value


def _is_trade_day(engine, day: date | None = None) -> bool:
    day = day or date.today()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM si_trade_calendar
                    WHERE trade_date = :d
                      AND trade_status = 1
                    """
                ),
                {"d": day.isoformat()},
            ).scalar()
        return bool(row)
    except Exception:
        return day.weekday() < 5


def is_trading_time(engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trade_day(engine, now.date()):
        return False
    current = now.hour * 100 + now.minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)


def _latest_stock_universe_count(engine) -> int:
    queries = [
        """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_stock_kline
        WHERE trade_date = (
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        )
          AND k_type = 1
        """,
        "SELECT COUNT(*) FROM si_all_code WHERE stock_code REGEXP '^(0|3|6)'",
    ]
    with engine.connect() as conn:
        for sql in queries:
            count = int(conn.execute(text(sql)).scalar() or 0)
            if count > 0:
                return count
    return 0


def _latest_open_trade_date(engine) -> str:
    try:
        with engine.connect() as conn:
            value = conn.execute(
                text(
                    """
                    SELECT MAX(trade_date)
                    FROM si_trade_calendar
                    WHERE trade_status = 1
                      AND trade_date <= CURDATE()
                    """
                )
            ).scalar()
        if value is not None:
            return str(value)[:10]
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative capital-flow calendar is unavailable"
        ) from exc
    raise RuntimeError(
        "DATA_BLOCKED: authoritative capital-flow calendar has no open session"
    )


def _canonical_trade_date(value: str) -> str:
    raw = str(value or "").strip()
    try:
        normalized = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow target date is invalid"
        ) from exc
    if raw != normalized:
        raise RuntimeError("DATA_BLOCKED: capital-flow target date is invalid")
    return normalized


def _capital_flow_target_kind(
    trade_date: str,
    *,
    now: datetime | None = None,
) -> str:
    """Classify a target against the Shanghai session, not merely MAX(calendar)."""

    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    else:
        current = current.astimezone(SHANGHAI)
    target = date.fromisoformat(trade_date)
    if target > current.date():
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow target is in the future"
        )
    if target < current.date():
        return "historical"
    hhmm = current.hour * 100 + current.minute
    if hhmm < CAPITAL_FLOW_LIVE_READY_HHMM:
        raise RuntimeError(
            "DATA_BLOCKED: current capital-flow session is not close-ready"
        )
    return "current"


def _require_open_trade_date(engine, trade_date: str) -> None:
    try:
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM si_trade_calendar "
                        "WHERE trade_date=:trade_date AND trade_status=1"
                    ),
                    {"trade_date": trade_date},
                ).scalar()
                or 0
            )
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative capital-flow calendar is unavailable"
        ) from exc
    if count != 1:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow target is not one authoritative open session"
        )


def _eastmoney_source_trade_date(value) -> str:
    try:
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        parsed = datetime.fromtimestamp(timestamp, SHANGHAI)
    except (OSError, OverflowError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow source timestamp is invalid"
        ) from exc
    if timestamp <= 0:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow source timestamp is invalid"
        )
    return parsed.date().isoformat()


def _exact_flow_source_items(items: list[dict], *, trade_date: str) -> list[dict]:
    """Keep only target-session rows and reject any newer live snapshot."""

    accepted: list[dict] = []
    future_dates: set[str] = set()
    for item in items:
        try:
            source_date = _eastmoney_source_trade_date(item.get("f124"))
        except RuntimeError:
            continue
        if source_date > trade_date:
            future_dates.add(source_date)
        elif source_date == trade_date:
            accepted.append(item)
    if future_dates:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow live source is newer than target date: "
            f"target={trade_date} source_dates={sorted(future_dates)}"
        )
    if not accepted:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow source has no exact target-date rows: "
            f"target={trade_date}"
        )
    return accepted


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _required_finite_float(value, *, field: str, stock_code: str) -> float:
    if value in (None, "", "-", "--"):
        raise ValueError(f"{field} is absent for stock_code={stock_code}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field} is not numeric for stock_code={stock_code}"
        ) from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} is not finite for stock_code={stock_code}")
    return number


def _read_target_traded_flow_codes(engine, trade_date: str) -> set[str]:
    """Return the exact target-session K-line set that requires daily flow."""

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT stock_code, volume, amount FROM sm_stock_kline "
                "WHERE trade_date=:trade_date AND k_type=1 AND adjust_type=0 "
                "ORDER BY stock_code"
            ),
            {"trade_date": trade_date},
        ).mappings().all()
    if not rows:
        raise RuntimeError(
            "DATA_BLOCKED: target-date K-line universe is empty for capital flow: "
            f"target={trade_date}"
        )
    traded: set[str] = set()
    seen: set[str] = set()
    for row in rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if not _FLOW_CODE_RE.fullmatch(code) or code == "000000":
            raise RuntimeError(
                "DATA_BLOCKED: target-date K-line contains invalid stock code"
            )
        if code in seen:
            raise RuntimeError(
                "DATA_BLOCKED: target-date K-line contains duplicate stock code: "
                f"stock_code={code}"
            )
        seen.add(code)
        # The Eastmoney daily-flow endpoint covers SH/SZ A shares, not BSE.
        # Match the historical repair's provider universe: unsupported BSE
        # securities must not trigger an endless fallback for an already
        # complete SH/SZ partition.
        if code[:2] not in {"00", "30", "60", "68"}:
            continue
        volume = _required_finite_float(
            row.get("volume"), field="volume", stock_code=code
        )
        amount = _required_finite_float(
            row.get("amount"), field="amount", stock_code=code
        )
        if volume < 0 or amount < 0:
            raise RuntimeError(
                "DATA_BLOCKED: target-date K-line volume/amount is negative: "
                f"stock_code={code}"
            )
        if volume != 0 or amount != 0:
            traded.add(code)
    if not traded:
        raise RuntimeError(
            "DATA_BLOCKED: target-date traded K-line universe is empty for capital flow: "
            f"target={trade_date}"
        )
    return traded


def _primary_flow_frame(
    items: list[dict],
    *,
    trade_date: str,
    target_codes: set[str],
) -> pd.DataFrame:
    """Normalize only complete primary-provider rows in the target universe."""

    rows: list[dict] = []
    seen: set[str] = set()
    for item in items:
        code = str(item.get("f12") or "").strip().zfill(6)
        if code not in target_codes:
            continue
        if code in seen:
            raise RuntimeError(
                "DATA_BLOCKED: Eastmoney capital-flow response contains duplicate "
                f"target stock: stock_code={code}"
            )
        seen.add(code)
        try:
            values = {
                output_name: _required_finite_float(
                    item.get(source_name),
                    field=source_name,
                    stock_code=code,
                )
                for output_name, source_name in CAPITAL_FLOW_FIELDS.items()
            }
        except ValueError:
            # A structurally incomplete primary row is unresolved, not a zero.
            # The exact-code fallback chain gets one chance to supply real data.
            continue
        rows.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                **values,
                "data_source": CAPITAL_FLOW_PRIMARY_SOURCE,
            }
        )
    return pd.DataFrame(rows)


def _validated_fallback_row(
    frame: pd.DataFrame | None,
    *,
    stock_code: str,
    trade_date: str,
) -> dict | None:
    if frame is None or frame.empty or len(frame) != 1:
        return None
    raw = frame.iloc[0].to_dict()
    code = str(raw.get("stock_code") or "").strip().zfill(6)
    source_date = str(raw.get("trade_date") or "")[:10]
    source = str(raw.get("data_source") or "").strip().lower()
    if (
        code != stock_code
        or source_date != trade_date
        or source not in CAPITAL_FLOW_FALLBACK_SOURCES
    ):
        return None
    try:
        values = {
            name: _required_finite_float(
                raw.get(name), field=name, stock_code=stock_code
            )
            for name in CAPITAL_FLOW_FIELDS
        }
    except ValueError:
        return None
    return {
        "stock_code": stock_code,
        "trade_date": trade_date,
        **values,
        "data_source": source,
    }


def _eastmoney_market_id(stock_code: str) -> int:
    """Return Eastmoney's response market identity for one A-share code."""

    code = str(stock_code or "").strip().zfill(6)
    if not _FLOW_CODE_RE.fullmatch(code) or code == "000000":
        raise ValueError("capital-flow fallback stock code is invalid")
    # Eastmoney identifies Shanghai as market 1.  Shenzhen and Beijing are
    # market 0; the exact six-digit response code disambiguates those boards.
    return 1 if code.startswith("6") else 0


def _fetch_exact_push2his_flow_row(
    stock_code: str,
    trade_date: str,
    *,
    client=None,
) -> dict | None:
    """Fetch one exact Eastmoney history row with source identity proof.

    Unlike the legacy helper, this parser never copies an unverified request
    identity into an accepted row and never coerces an absent component to
    zero.  A response without matching ``data.code`` and ``data.market`` is a
    hard integrity failure; an identity-valid response with no target-date row
    remains unresolved and is returned as ``None``.
    """

    code = str(stock_code or "").strip().zfill(6)
    target = _canonical_trade_date(trade_date)
    expected_market = _eastmoney_market_id(code)
    owns_client = client is None
    http = client or requests.Session()
    if owns_client:
        http.trust_env = False
        http.headers.update({
            "User-Agent": (
                "Mozilla/5.0 ProBigA-capital-flow-exact-fallback"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://data.eastmoney.com/",
        })
    try:
        response = http.get(
            CAPITAL_FLOW_PUSH2HIS_API,
            params={
                "lmt": "0",
                "klt": "101",
                "fields1": "f1,f2,f3,f7",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "secid": f"{expected_market}.{code}",
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
    finally:
        if owns_client:
            http.close()
    if not isinstance(payload, Mapping):
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow response is not an object"
        )
    try:
        result_code = int(payload.get("rc"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow response status is absent"
        ) from exc
    if result_code != 0:
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow response status failed: "
            f"rc={result_code}"
        )
    data = payload.get("data")
    if data is None:
        return None
    if not isinstance(data, Mapping):
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow response data is malformed"
        )
    response_code = str(data.get("code") or "").strip().zfill(6)
    raw_market = data.get("market")
    if isinstance(raw_market, bool):
        response_market = -1
    else:
        try:
            response_market = int(raw_market)
        except (TypeError, ValueError, OverflowError):
            response_market = -1
    if response_code != code or response_market != expected_market:
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow response identity differs: "
            f"requested={expected_market}.{code} "
            f"response={response_market}.{response_code}"
        )
    raw_lines = data.get("klines")
    if raw_lines is None:
        return None
    if not isinstance(raw_lines, list) or any(
        not isinstance(line, str) for line in raw_lines
    ):
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow rows are malformed"
        )
    matches: list[list[str]] = []
    for line in raw_lines:
        parts = [part.strip() for part in line.split(",")]
        if parts and parts[0] == target:
            matches.append(parts)
    if not matches:
        return None
    if len(matches) != 1:
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow target date is duplicated: "
            f"stock_code={code} trade_date={target}"
        )
    parts = matches[0]
    if len(parts) < 6:
        raise RuntimeError(
            "DATA_BLOCKED: push2his capital-flow target row is incomplete: "
            f"stock_code={code} trade_date={target}"
        )
    values = {
        "main_net_inflow": _required_finite_float(
            parts[1], field="f52", stock_code=code
        ),
        "sm_net_inflow": _required_finite_float(
            parts[2], field="f53", stock_code=code
        ),
        "mid_net_inflow": _required_finite_float(
            parts[3], field="f54", stock_code=code
        ),
        "lg_net_inflow": _required_finite_float(
            parts[4], field="f55", stock_code=code
        ),
        "max_net_inflow": _required_finite_float(
            parts[5], field="f56", stock_code=code
        ),
    }
    return {
        "stock_code": response_code,
        "trade_date": target,
        **values,
        "data_source": "push2his",
    }


def _fetch_missing_flow_rows(
    missing_codes: set[str],
    *,
    trade_date: str,
) -> pd.DataFrame:
    """Fetch exact missing identities using existing per-stock providers."""

    if not missing_codes:
        return pd.DataFrame()
    workers = max(1, min(8, int(os.environ.get("FLOW_FALLBACK_WORKERS", "4"))))
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_exact_push2his_flow_row, code, trade_date): code
            for code in sorted(missing_codes)
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                fetched = future.result()
            except Exception as exc:
                raise RuntimeError(
                    "DATA_BLOCKED: exact historical capital-flow fallback failed: "
                    f"stock_code={code} trade_date={trade_date} "
                    f"error_type={type(exc).__name__}"
                ) from exc
            row = _validated_fallback_row(
                pd.DataFrame([fetched]) if fetched is not None else None,
                stock_code=code,
                trade_date=trade_date,
            )
            if row is not None:
                rows.append(row)
    return pd.DataFrame(rows)


def _validate_exact_flow_frame(
    frame: pd.DataFrame,
    *,
    trade_date: str,
    target_codes: set[str],
) -> pd.DataFrame:
    required = {
        "stock_code",
        "trade_date",
        *CAPITAL_FLOW_FIELDS,
        "data_source",
    }
    if frame is None or frame.empty or not required.issubset(frame.columns):
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow providers returned no complete target frame"
        )
    result = frame.loc[:, sorted(required)].copy()
    result["stock_code"] = result["stock_code"].astype(str).str.strip().str.zfill(6)
    result["trade_date"] = result["trade_date"].astype(str).str[:10]
    if result["stock_code"].duplicated(keep=False).any():
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow provider chain returned duplicate stock codes"
        )
    if set(result["trade_date"]) != {trade_date}:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow provider chain returned a different date"
        )
    result_codes = set(result["stock_code"])
    missing = sorted(target_codes - result_codes)
    unexpected = sorted(result_codes - target_codes)
    if missing or unexpected:
        raise RuntimeError(
            "DATA_BLOCKED: exact target-date capital-flow coverage differs: "
            f"target={trade_date} expected={len(target_codes)} "
            f"actual={len(result_codes)} missing_count={len(missing)} "
            f"unexpected_count={len(unexpected)} "
            f"missing_sample={missing[:20]} unexpected_sample={unexpected[:20]}"
        )
    if result["data_source"].fillna("").astype(str).str.strip().eq("").any():
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow provider identity is absent"
        )
    for column in CAPITAL_FLOW_FIELDS:
        numeric = pd.to_numeric(result[column], errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric.to_numpy()).all():
            raise RuntimeError(
                "DATA_BLOCKED: capital-flow provider returned a non-finite value: "
                f"field={column}"
            )
        result[column] = numeric
    return result.sort_values("stock_code").reset_index(drop=True)


def _read_existing_flow_partition(engine, trade_date: str) -> pd.DataFrame:
    """Read only the exact persisted daily-flow partition requested by a run."""

    columns = [
        "stock_code",
        "trade_date",
        *CAPITAL_FLOW_FIELDS,
        "data_source",
    ]
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT stock_code, trade_date, main_net_inflow, "
                "max_net_inflow, lg_net_inflow, mid_net_inflow, "
                "sm_net_inflow, data_source "
                "FROM sm_stock_capital_flow_daily "
                "WHERE trade_date=:trade_date ORDER BY stock_code"
            ),
            {"trade_date": trade_date},
        ).mappings().all()
    return pd.DataFrame([dict(row) for row in rows], columns=columns)


def _inspect_reusable_flow_partition(
    frame: pd.DataFrame,
    *,
    trade_date: str,
    target_codes: set[str],
) -> tuple[pd.DataFrame, set[str]]:
    """Return verified persisted rows and the exact identities still needing repair."""

    required = {
        "stock_code",
        "trade_date",
        *CAPITAL_FLOW_FIELDS,
        "data_source",
    }
    if frame is None or frame.empty:
        return pd.DataFrame(columns=sorted(required)), set(target_codes)
    if not required.issubset(frame.columns):
        raise RuntimeError(
            "DATA_BLOCKED: persisted capital-flow partition shape is incomplete"
        )
    result = frame.loc[:, sorted(required)].copy()
    result["stock_code"] = result["stock_code"].astype(str).str.strip().str.zfill(6)
    result["trade_date"] = result["trade_date"].astype(str).str[:10]
    if result["stock_code"].duplicated(keep=False).any():
        raise RuntimeError(
            "DATA_BLOCKED: persisted capital-flow partition contains duplicate codes"
        )
    if set(result["trade_date"]) != {trade_date}:
        raise RuntimeError(
            "DATA_BLOCKED: persisted capital-flow partition contains another date"
        )
    result_codes = set(result["stock_code"])
    unexpected = sorted(result_codes - target_codes)
    if unexpected:
        raise RuntimeError(
            "DATA_BLOCKED: persisted capital-flow partition contains non-target codes: "
            f"target={trade_date} unexpected_count={len(unexpected)} "
            f"unexpected_sample={unexpected[:20]}"
        )

    allowed_sources = {
        CAPITAL_FLOW_PRIMARY_SOURCE,
        *CAPITAL_FLOW_FALLBACK_SOURCES,
    }
    valid = result["data_source"].fillna("").astype(str).str.strip().str.lower().isin(
        allowed_sources
    )
    result["data_source"] = (
        result["data_source"].fillna("").astype(str).str.strip().str.lower()
    )
    for column in CAPITAL_FLOW_FIELDS:
        numeric = pd.to_numeric(result[column], errors="coerce")
        finite = pd.Series(
            np.isfinite(numeric.to_numpy()),
            index=result.index,
        )
        valid &= numeric.notna() & finite
        result[column] = numeric
    verified = result.loc[valid].sort_values("stock_code").reset_index(drop=True)
    verified_codes = set(verified["stock_code"])
    # Invalid target identities are repair candidates, not a reason to discard
    # an otherwise verified historical partition.  The caller fetches only this
    # exact missing set and the delta publisher updates only those business keys.
    return verified, set(target_codes) - verified_codes


def _flow_partition_sha256(frame: pd.DataFrame) -> str:
    """Hash stable business fields so a reuse receipt identifies what was verified."""

    rows = []
    for raw in frame.sort_values("stock_code").to_dict("records"):
        rows.append({
            "stock_code": str(raw["stock_code"]).strip().zfill(6),
            "trade_date": str(raw["trade_date"])[:10],
            **{
                field: float(raw[field])
                for field in CAPITAL_FLOW_FIELDS
            },
            "data_source": str(raw["data_source"]).strip().lower(),
        })
    canonical = json.dumps(
        rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_flow_execution_evidence(
    evidence: dict | None,
    *,
    mode: str,
    target_kind: str,
    reuse_verified_existing: bool,
    existing_row_count: int,
    missing_before_count: int,
    rows_written: int,
    live_source_called: bool,
    historical_fallback_called: bool,
    live_primary_row_count: int,
    fallback_requested_count: int,
    fallback_returned_count: int,
    partition_replaced: bool,
    frame: pd.DataFrame,
    repair_frame: pd.DataFrame | None = None,
    verified_existing_frame: pd.DataFrame | None = None,
) -> None:
    if evidence is None:
        return
    captured_at = datetime.now().isoformat(timespec="seconds")
    evidence.clear()
    evidence.update({
        "mode": mode,
        "target_kind": target_kind,
        "captured_at": captured_at,
        "reuse_verified_existing": bool(reuse_verified_existing),
        "existing_row_count": int(existing_row_count),
        "missing_before_count": int(missing_before_count),
        "rows_written": int(rows_written),
        "live_source_called": bool(live_source_called),
        "historical_fallback_called": bool(historical_fallback_called),
        "network_accessed": bool(live_source_called or historical_fallback_called),
        "target_code_count": len(frame),
        "live_primary_row_count": int(live_primary_row_count),
        "fallback_requested_count": int(fallback_requested_count),
        "fallback_returned_count": int(fallback_returned_count),
        "partition_replaced": bool(partition_replaced),
        "partition_verified": True,
        "partition_sha256": _flow_partition_sha256(frame),
        "source_counts": {
            str(source): int(count)
            for source, count in sorted(
                frame["data_source"].astype(str).value_counts().items()
            )
        },
    })
    if verified_existing_frame is not None and not verified_existing_frame.empty:
        evidence["verified_existing_sha256"] = _flow_partition_sha256(
            verified_existing_frame
        )
    if repair_frame is not None and not repair_frame.empty:
        repair_sources = {
            str(source): int(count)
            for source, count in sorted(
                repair_frame["data_source"].astype(str).value_counts().items()
            )
        }
        evidence["repair"] = {
            "captured_at": captured_at,
            "row_count": len(repair_frame),
            "row_sha256": _flow_partition_sha256(repair_frame),
            "source_counts": repair_sources,
        }


def _upsert_flow_partition_delta_exact(
    engine,
    frame: pd.DataFrame,
    *,
    trade_date: str,
    expected_codes: set[str],
) -> tuple[pd.DataFrame, int]:
    """Write only missing/invalid identities, then verify the complete partition."""

    if frame is None or frame.empty:
        raise ValueError("capital-flow repair delta must not be empty")
    repair_codes = set(frame["stock_code"].astype(str).str.strip().str.zfill(6))
    delta = _validate_exact_flow_frame(
        frame,
        trade_date=trade_date,
        target_codes=repair_codes,
    )
    delta["etl_sync_at"] = datetime.now().replace(microsecond=0)
    statement = text(
        "INSERT INTO sm_stock_capital_flow_daily ("
        "stock_code, trade_date, main_net_inflow, max_net_inflow, "
        "lg_net_inflow, mid_net_inflow, sm_net_inflow, etl_sync_at, data_source"
        ") VALUES ("
        ":stock_code, :trade_date, :main_net_inflow, :max_net_inflow, "
        ":lg_net_inflow, :mid_net_inflow, :sm_net_inflow, :etl_sync_at, :data_source"
        ") ON DUPLICATE KEY UPDATE "
        "main_net_inflow=:main_net_inflow, "
        "max_net_inflow=:max_net_inflow, "
        "lg_net_inflow=:lg_net_inflow, "
        "mid_net_inflow=:mid_net_inflow, "
        "sm_net_inflow=:sm_net_inflow, "
        "etl_sync_at=:etl_sync_at, data_source=:data_source"
    )
    columns = [
        "stock_code",
        "trade_date",
        *CAPITAL_FLOW_FIELDS,
        "etl_sync_at",
        "data_source",
    ]
    with mysql_named_lock(
        engine,
        CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        timeout_seconds=max(0, int(os.environ.get("FLOW_DAILY_LOCK_TIMEOUT", "30"))),
    ) as conn:
        if conn.in_transaction():
            conn.commit()
        with conn.begin():
            before_rows = conn.execute(
                text(
                    "SELECT stock_code, trade_date, main_net_inflow, "
                    "max_net_inflow, lg_net_inflow, mid_net_inflow, "
                    "sm_net_inflow, data_source "
                    "FROM sm_stock_capital_flow_daily "
                    "WHERE trade_date=:trade_date ORDER BY stock_code"
                ),
                {"trade_date": trade_date},
            ).mappings().all()
            before_frame = pd.DataFrame(
                [dict(row) for row in before_rows],
                columns=[
                    "stock_code",
                    "trade_date",
                    *CAPITAL_FLOW_FIELDS,
                    "data_source",
                ],
            )
            verified_before, still_missing = _inspect_reusable_flow_partition(
                before_frame,
                trade_date=trade_date,
                target_codes=expected_codes,
            )
            unavailable_repairs = sorted(still_missing - repair_codes)
            if unavailable_repairs:
                raise RuntimeError(
                    "DATA_BLOCKED: exact historical capital-flow repair delta "
                    "is incomplete: "
                    f"target={trade_date} missing_count={len(unavailable_repairs)} "
                    f"missing_sample={unavailable_repairs[:20]}"
                )
            insert_frame = delta[delta["stock_code"].isin(still_missing)].copy()
            if not insert_frame.empty:
                conn.execute(
                    statement,
                    insert_frame.loc[:, columns].to_dict("records"),
                )
            stored_rows = conn.execute(
                text(
                    "SELECT stock_code, trade_date, main_net_inflow, "
                    "max_net_inflow, lg_net_inflow, mid_net_inflow, "
                    "sm_net_inflow, data_source "
                    "FROM sm_stock_capital_flow_daily "
                    "WHERE trade_date=:trade_date ORDER BY stock_code"
                ),
                {"trade_date": trade_date},
            ).mappings().all()
            stored_frame = pd.DataFrame([dict(row) for row in stored_rows])
            verified_after, remaining = _inspect_reusable_flow_partition(
                stored_frame,
                trade_date=trade_date,
                target_codes=expected_codes,
            )
            if remaining:
                raise RuntimeError(
                    "DATA_BLOCKED: exact historical capital-flow repair did not "
                    "resolve every target identity: "
                    f"target={trade_date} missing_count={len(remaining)} "
                    f"missing_sample={sorted(remaining)[:20]}"
                )
            if not verified_before.empty:
                preserved_codes = set(verified_before["stock_code"])
                preserved_after = verified_after[
                    verified_after["stock_code"].isin(preserved_codes)
                ].copy()
                if _flow_partition_sha256(
                    verified_before
                ) != _flow_partition_sha256(preserved_after):
                    raise RuntimeError(
                        "DATA_BLOCKED: verified historical capital-flow rows "
                        "changed during targeted repair"
                    )
            stored = _validate_exact_flow_frame(
                verified_after,
                trade_date=trade_date,
                target_codes=expected_codes,
            )
    return stored, len(insert_frame)


def _replace_table_rows_flow_partition_exact(
    engine,
    frame: pd.DataFrame,
    *,
    trade_date: str,
    expected_codes: set[str],
) -> int:
    """Atomically replace and verify one complete flow-date partition."""

    table_name = "sm_stock_capital_flow_daily"
    with mysql_named_lock(
        engine,
        CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        timeout_seconds=max(0, int(os.environ.get("FLOW_DAILY_LOCK_TIMEOUT", "30"))),
    ) as conn:
        if conn.in_transaction():
            conn.commit()
        with conn.begin():
            conn.execute(
                text(
                    f"DELETE FROM {quote_identifier(table_name)} "
                    "WHERE trade_date=:trade_date"
                ),
                {"trade_date": trade_date},
            )
            written = int(
                write_frame(
                    frame,
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
            )
            stored = {
                str(value).strip().zfill(6)
                for value in conn.execute(
                    text(
                        f"SELECT stock_code FROM {quote_identifier(table_name)} "
                        "WHERE trade_date=:trade_date"
                    ),
                    {"trade_date": trade_date},
                ).scalars().all()
            }
            if written != len(frame) or stored != expected_codes:
                raise RuntimeError(
                    "DATA_BLOCKED: capital-flow atomic publication verification "
                    f"differs: written={written}/{len(frame)} "
                    f"stored={len(stored)}/{len(expected_codes)}"
                )
    return written


def _code_scope(
    codes: list[str],
    *,
    column: str = "stock_code",
    prefix: str = "code",
) -> tuple[str, dict[str, str]]:
    normalized = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    if not normalized:
        raise ValueError("snapshot replacement code scope must not be empty")
    params = {f"{prefix}_{index}": code for index, code in enumerate(normalized)}
    placeholders = ", ".join(f":{name}" for name in params)
    return f"{quote_identifier(column)} IN ({placeholders})", params


def _publish_snapshot_and_archive(
    engine,
    frame: pd.DataFrame,
    *,
    archive_snapshot: bool,
) -> int:
    """Replace observed quote codes and append their archive in one transaction."""

    if frame is None or frame.empty:
        raise ValueError("realtime quote snapshot must not be empty")
    predicate, params = _code_scope(frame["stock_code"].astype(str).tolist())
    if archive_snapshot:
        _ensure_rt_snapshot_table(engine)
    archive_cols = [
        "stock_code",
        "short_name",
        "price",
        "change",
        "change_pct",
        "volume",
        "amount",
        "snapshot_at",
    ]
    with engine.begin() as connection:
        connection.execute(
            text(f"DELETE FROM {quote_identifier('sm_stock_current')} WHERE {predicate}"),
            params,
        )
        current_written = write_frame(
            frame,
            "sm_stock_current",
            connection,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        if int(current_written) != len(frame):
            raise RuntimeError(
                "sm_stock_current write mismatch: "
                f"expected={len(frame)} actual={current_written}"
            )
        if archive_snapshot:
            archived = write_frame(
                frame[archive_cols],
                "sm_rt_quote_snapshot",
                connection,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            if int(archived) != len(frame):
                raise RuntimeError(
                    "sm_rt_quote_snapshot write mismatch: "
                    f"expected={len(frame)} actual={archived}"
                )
    return int(len(frame))


def fetch_batch(
    fs: str,
    fields: str,
    page_size: int = 100,
    *,
    fid: str = "f3",
    po: str = "1",
) -> list[dict]:
    """分页获取批量数据"""
    all_items = []
    for pn in range(1, 200):
        params = {
            "fid": fid, "po": po,
            "pz": str(page_size), "pn": str(pn), "np": "1",
            "fltt": "2", "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fs": fs,
            "fields": fields,
        }
        for attempt in range(2):
            try:
                resp = SESSION.get(BATCH_API, params=params, timeout=15)
                data = resp.json()
                diff = (data.get("data") or {}).get("diff")
                if diff is not None:
                    break
            except Exception:
                if attempt == 0:
                    time.sleep(1)
                else:
                    diff = None
        if not diff:
            break
        all_items.extend(diff)
        if len(diff) < page_size:
            break
        time.sleep(0.1)
    return all_items


def refresh_snapshot(
    engine,
    *,
    min_coverage: float = 0.0,
    archive_snapshot: bool = False,
) -> int:
    """刷新个股行情快照 sm_stock_current"""
    items = fetch_batch(
        "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
        "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
        if not code or code == "000000":
            continue
        rows.append({
            "stock_code": code,
            "short_name": str(item.get("f14", "")),
            "price": safe_float(item.get("f2")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "snapshot_at": now,
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code"], keep="last")

    expected = _latest_stock_universe_count(engine)
    coverage = len(df) / max(expected, 1)
    if min_coverage > 0 and coverage < min_coverage:
        raise RuntimeError(
            f"sm_stock_current coverage below threshold: "
            f"{len(df)}/{expected} ({coverage:.1%}) < {min_coverage:.1%}"
        )

    df["etl_sync_at"] = now
    return _publish_snapshot_and_archive(
        engine,
        df,
        archive_snapshot=archive_snapshot,
    )


def refresh_flow(
    engine,
    *,
    trade_date: str | None = None,
    min_coverage: float = 0.0,
    require_source_date: bool = False,
    reuse_verified_existing: bool = False,
    execution_evidence: dict | None = None,
) -> int:
    """Refresh/reuse one exact traded-K flow partition with dated failover."""
    today = _canonical_trade_date(
        trade_date or _latest_open_trade_date(engine)
    )
    if min_coverage > 1:
        raise RuntimeError("capital-flow minimum coverage cannot exceed 1")
    if require_source_date:
        _require_open_trade_date(engine, today)
    latest_open = _canonical_trade_date(_latest_open_trade_date(engine))
    if today > latest_open:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow target is newer than the latest open session: "
            f"target={today} latest_open={latest_open}"
        )
    target_kind = _capital_flow_target_kind(today)
    if target_kind == "historical" and not reuse_verified_existing:
        raise RuntimeError(
            "DATA_BLOCKED: historical capital-flow target requires explicit "
            "verified-existing reuse"
        )

    target_codes = _read_target_traded_flow_codes(engine, today)
    existing = pd.DataFrame()
    verified_existing = pd.DataFrame()
    missing_codes = set(target_codes)
    # A complete fast-path is safe only for a closed historical session.  The
    # latest session can still contain an earlier intraday capture, so it must
    # always be refreshed from the live endpoint even when release catch-up
    # supplied the reuse flag.
    if reuse_verified_existing and target_kind == "historical":
        existing = _read_existing_flow_partition(engine, today)
        verified_existing, missing_codes = _inspect_reusable_flow_partition(
            existing,
            trade_date=today,
            target_codes=target_codes,
        )
        if not missing_codes:
            verified = _validate_exact_flow_frame(
                verified_existing,
                trade_date=today,
                target_codes=target_codes,
            )
            _record_flow_execution_evidence(
                execution_evidence,
                mode=CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING,
                target_kind=target_kind,
                reuse_verified_existing=True,
                existing_row_count=len(verified_existing),
                missing_before_count=0,
                rows_written=0,
                live_source_called=False,
                historical_fallback_called=False,
                live_primary_row_count=0,
                fallback_requested_count=0,
                fallback_returned_count=0,
                partition_replaced=False,
                frame=verified,
            )
            return len(verified)

    if target_kind == "historical":
        fallback = _fetch_missing_flow_rows(missing_codes, trade_date=today)
        try:
            fallback = _validate_exact_flow_frame(
                fallback,
                trade_date=today,
                target_codes=missing_codes,
            )
        except RuntimeError as exc:
            raise RuntimeError(
                "DATA_BLOCKED: exact historical capital-flow fallback could not "
                f"resolve the repair set: target={today} "
                f"repair_count={len(missing_codes)}"
            ) from exc
        frames = [
            part for part in (verified_existing, fallback)
            if part is not None and not part.empty
        ]
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        verified = _validate_exact_flow_frame(
            combined,
            trade_date=today,
            target_codes=target_codes,
        )
        delta = verified[verified["stock_code"].isin(missing_codes)].copy()
        stored, rows_written = _upsert_flow_partition_delta_exact(
            engine,
            delta,
            trade_date=today,
            expected_codes=target_codes,
        )
        _record_flow_execution_evidence(
            execution_evidence,
            mode=CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR,
            target_kind=target_kind,
            reuse_verified_existing=True,
            existing_row_count=len(verified_existing),
            missing_before_count=len(missing_codes),
            rows_written=rows_written,
            live_source_called=False,
            historical_fallback_called=True,
            live_primary_row_count=0,
            fallback_requested_count=len(missing_codes),
            fallback_returned_count=len(fallback),
            partition_replaced=False,
            frame=stored,
            repair_frame=delta,
            verified_existing_frame=verified_existing,
        )
        return len(stored)

    items = fetch_batch(
        CAPITAL_FLOW_MARKETS,
        "f12,f14,f62,f66,f72,f78,f84,f124",
        fid="f12",
        po="0",
    )
    if not items:
        raise RuntimeError("DATA_BLOCKED: Eastmoney capital-flow response is empty")
    if require_source_date:
        items = _exact_flow_source_items(items, trade_date=today)
    now = datetime.now().replace(microsecond=0)
    primary = _primary_flow_frame(
        items,
        trade_date=today,
        target_codes=target_codes,
    )
    primary_codes = (
        set(primary["stock_code"].astype(str)) if not primary.empty else set()
    )
    fallback = _fetch_missing_flow_rows(
        missing_codes - primary_codes,
        trade_date=today,
    )
    frames = [part for part in (primary, fallback) if not part.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df = _validate_exact_flow_frame(
        combined,
        trade_date=today,
        target_codes=target_codes,
    )
    # ``min_coverage`` remains a CLI compatibility flag.  Formal publication
    # is deliberately stricter: every target-session traded code is required.
    df["etl_sync_at"] = now
    row_count = _replace_table_rows_flow_partition_exact(
        engine,
        df,
        trade_date=today,
        expected_codes=target_codes,
    )
    rows_written = len(df)
    _record_flow_execution_evidence(
        execution_evidence,
        mode=CAPITAL_FLOW_EXECUTION_CURRENT_LIVE,
        target_kind=target_kind,
        reuse_verified_existing=reuse_verified_existing,
        existing_row_count=len(existing),
        missing_before_count=len(missing_codes),
        rows_written=rows_written,
        live_source_called=True,
        historical_fallback_called=bool(missing_codes - primary_codes),
        live_primary_row_count=len(primary),
        fallback_requested_count=len(missing_codes - primary_codes),
        fallback_returned_count=len(fallback),
        partition_replaced=True,
        frame=df,
    )
    return row_count


def refresh_concept_east(engine) -> int:
    """刷新东财概念行情 sm_concept_east_current"""
    items = fetch_batch(
        "m:90+t:3",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
        code = item.get("f12", "")
        if not code:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": safe_float(item.get("f17")),
            "price": safe_float(item.get("f2")),
            "high": safe_float(item.get("f15")),
            "low": safe_float(item.get("f16")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "snapshot_at": now,
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    df["etl_sync_at"] = now
    replace_table_rows(
        df,
        "sm_concept_east_current",
        engine,
        chunksize=500,
        method="multi",
    )
    return len(df)


def refresh_index(engine) -> int:
    """刷新指数行情 sm_index_current"""
    # 指数: 上证 m:1+t:2, 深证 m:0+t:2, 创业板 m:0+t:23, 科创 m:1+t:23
    items = fetch_batch(
        "m:1+t:2+f:!2,m:0+t:2+f:!2,m:1+t:23+f:!2,m:0+t:23+f:!2",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
        if not code:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": safe_float(item.get("f17")),
            "price": safe_float(item.get("f2")),
            "high": safe_float(item.get("f15")),
            "low": safe_float(item.get("f16")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "snapshot_at": now,
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    df["etl_sync_at"] = now
    replace_table_rows(
        df,
        "sm_index_current",
        engine,
        chunksize=500,
        method="multi",
    )
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="盘中批量数据刷新")
    parser.add_argument("--only", choices=["snapshot", "flow", "concept", "index", "all"],
                        default="all")
    parser.add_argument("--min-coverage", type=float, default=0.0)
    parser.add_argument("--archive-snapshot", action="store_true")
    parser.add_argument("--skip-closed", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--trade-date", default="")
    parser.add_argument(
        "--reuse-verified-existing",
        action="store_true",
        help=(
            "verify and reuse an exact persisted flow partition before any "
            "provider request; required for historical scheduler/release targets"
        ),
    )
    args = parser.parse_args()

    engine = create_batch_engine()
    if args.skip_closed and not is_trading_time(engine):
        result = {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
        if args.json:
            print(json.dumps(result, ensure_ascii=False, default=str))
        else:
            print(f"  skipped: {result['reason']}", flush=True)
        return 0
    t0 = time.time()
    results = {}
    flow_execution_evidence: dict = {}
    flow_build_sha = _scheduler_build_sha() if args.only == "flow" else ""

    if args.only in ("snapshot", "all"):
        n = refresh_snapshot(
            engine,
            min_coverage=args.min_coverage,
            archive_snapshot=args.archive_snapshot,
        )
        results["snapshot"] = n
        if not args.json:
            print(f"  snapshot: {n} stocks", flush=True)

    if args.only in ("flow", "all"):
        flow_trade_date = _canonical_trade_date(
            args.trade_date.strip() or _latest_open_trade_date(engine)
        )
        n = refresh_flow(
            engine,
            trade_date=flow_trade_date,
            min_coverage=args.min_coverage,
            require_source_date=True,
            reuse_verified_existing=args.reuse_verified_existing,
            execution_evidence=flow_execution_evidence,
        )
        results["flow"] = n
        if not args.json:
            print(f"  flow: {n} stocks", flush=True)

    if args.only in ("concept", "all"):
        n = refresh_concept_east(engine)
        results["concept"] = n
        if not args.json:
            print(f"  concept_east: {n}", flush=True)

    if args.only in ("index", "all"):
        n = refresh_index(engine)
        results["index"] = n
        if not args.json:
            print(f"  index: {n}", flush=True)

    elapsed = time.time() - t0
    generated_at = datetime.now().isoformat(timespec="seconds")
    if args.only == "flow":
        result = _signed_receipt({
            "schema": CAPITAL_FLOW_RESULT_SCHEMA,
            "status": "PASS",
            "task_type": CAPITAL_FLOW_TASK_TYPE,
            "dataset": CAPITAL_FLOW_DATASET,
            "build_sha": flow_build_sha,
            "trade_date": flow_trade_date,
            "source_trade_date": flow_trade_date,
            "source_timestamp_required": True,
            "row_count": int(results.get("flow") or 0),
            "execution_mode": flow_execution_evidence.get("mode"),
            "captured_at": flow_execution_evidence.get("captured_at"),
            "partition_sha256": flow_execution_evidence.get("partition_sha256"),
            "source_counts": flow_execution_evidence.get("source_counts", {}),
            "execution": flow_execution_evidence,
            "elapsed_seconds": round(elapsed, 1),
            "generated_at": generated_at,
        })
    else:
        result = {
            "status": "success",
            "results": results,
            **(
                {"flow_execution": flow_execution_evidence}
                if "flow" in results
                else {}
            ),
            "elapsed_seconds": round(elapsed, 1),
            "generated_at": generated_at,
        }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(f"  Done in {elapsed:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
