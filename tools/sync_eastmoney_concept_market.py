#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Strict Eastmoney-only publisher for concept current/daily/minute market data.

The live Eastmoney directory is fetched with complete pagination and is the
only code universe accepted by this publisher.  Every requested dataset must
cover that exact universe and its exact source date/time grid before any DML
runs.  Collection never falls back to adata, QMT, cached rows, or another
provider, and publication performs no runtime DDL.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.batch_db import quote_identifier
from server.common.legacy_table_surface import validate_required_table_surface
from server.common.mysql_lock import mysql_named_lock
from tools.env_config import create_tool_engine


SHANGHAI = ZoneInfo("Asia/Shanghai")
PROVIDER_ID = "eastmoney_public_market"
RECEIPT_SCHEMA = "probiga.eastmoney-concept-market-result.v1"
DIRECTORY_SCHEMA = "probiga.eastmoney-concept-directory.v1"
DIRECTORY_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
DAILY_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
MINUTE_URL = "https://push2delay.eastmoney.com/api/qt/stock/kline/get"
DIRECTORY_FILTER = "m:90+t:3"
DIRECTORY_FIELDS = "f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f124"
EASTMONEY_TOKEN = "b2884a393a59ad64002292a3e90d46a5"
MIN_DIRECTORY_CODES = 100
DEFAULT_PAGE_SIZE = 100
DEFAULT_WORKERS = 8
PUBLISH_LOCK_NAME = "probiga:east-concept-market"


CURRENT_COLUMNS = (
    "index_code",
    "trade_time",
    "trade_date",
    "open",
    "price",
    "high",
    "low",
    "volume",
    "amount",
    "change",
    "change_pct",
    "snapshot_at",
    "etl_sync_at",
)
DAILY_COLUMNS = (
    "index_code",
    "trade_time",
    "trade_date",
    "k_type",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "change",
    "change_pct",
    "etl_sync_at",
)
MINUTE_COLUMNS = (
    "index_code",
    "trade_time",
    "trade_date",
    "price",
    "avg_price",
    "change",
    "change_pct",
    "volume",
    "amount",
    "snapshot_at",
    "etl_sync_at",
)
DATASET_TABLE = {
    "current": "sm_concept_east_current",
    "kline": "sm_concept_east_kline",
    "minute": "sm_concept_east_minute",
}
DATASET_COLUMNS = {
    "current": CURRENT_COLUMNS,
    "kline": DAILY_COLUMNS,
    "minute": MINUTE_COLUMNS,
}


class DataBlocked(RuntimeError):
    """The provider or target-date contract is not publishable."""

    def __init__(self, message: str, *, result: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = dict(result) if isinstance(result, Mapping) else None


@dataclass(frozen=True)
class DirectorySnapshot:
    items: tuple[dict[str, Any], ...]
    codes: tuple[str, ...]
    evidence: dict[str, Any]


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _code_set_hash(codes: Iterable[str]) -> str:
    normalized = sorted({str(code).strip() for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _iso_date(value: Any, *, field: str) -> str:
    raw = str(value or "")[:10]
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise DataBlocked(f"DATA_BLOCKED: {field} is not an ISO date") from exc
    if parsed.isoformat() != raw:
        raise DataBlocked(f"DATA_BLOCKED: {field} is not an ISO date")
    return raw


def _finite_number(
    value: Any,
    *,
    field: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} is not numeric") from exc
    if not math.isfinite(number):
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} is not finite")
    if positive and number <= 0:
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} must be positive")
    if nonnegative and number < 0:
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} must be nonnegative")
    return number


def _eastmoney_source_time(value: Any) -> datetime:
    try:
        epoch = int(value)
        parsed = datetime.fromtimestamp(epoch, tz=SHANGHAI)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise DataBlocked("DATA_BLOCKED: Eastmoney f124 source time is invalid") from exc
    return parsed.replace(microsecond=0)


def _minute_grid(target_date: str) -> tuple[datetime, ...]:
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    result: list[datetime] = []
    for start, end in (
        (datetime_time(9, 31), datetime_time(11, 30)),
        (datetime_time(13, 1), datetime_time(15, 0)),
    ):
        cursor = datetime.combine(target, start)
        stop = datetime.combine(target, end)
        while cursor <= stop:
            result.append(cursor)
            cursor += timedelta(minutes=1)
    if len(result) != 240:
        raise AssertionError("canonical Eastmoney concept minute grid must have 240 bars")
    return tuple(result)


class EastmoneyConceptProvider:
    """Small provider adapter with no cross-provider fallback."""

    def __init__(self, *, retries: int = 3, timeout: int = 20) -> None:
        self.retries = max(1, int(retries))
        self.timeout = max(1, int(timeout))
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/125 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://quote.eastmoney.com/",
        }

    def _request_json(self, url: str, params: Mapping[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        request = Request(
            url + "?" + urlencode({key: str(value) for key, value in params.items()}),
            headers=self.headers,
            method="GET",
        )
        for attempt in range(self.retries):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8", errors="strict"))
                if not isinstance(payload, dict):
                    raise ValueError("response JSON is not an object")
                return payload
            except Exception as exc:  # bounded provider retry
                last_error = exc
                if attempt + 1 < self.retries:
                    time.sleep(2**attempt)
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney request failed after {self.retries} attempts: {url}"
        ) from last_error

    def fetch_directory_page(self, page: int, page_size: int) -> dict[str, Any]:
        return self._request_json(
            DIRECTORY_URL,
            {
                "fid": "f3",
                "po": "1",
                "pz": page_size,
                "pn": page,
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "ut": EASTMONEY_TOKEN,
                "fs": DIRECTORY_FILTER,
                "fields": DIRECTORY_FIELDS,
            },
        )

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> dict[str, Any]:
        return self._request_json(
            DAILY_URL,
            {
                "secid": f"90.{code}",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": "1",
                "beg": start_date.replace("-", ""),
                "end": end_date.replace("-", ""),
                "lmt": "1000000",
            },
        )

    def fetch_minute(self, code: str) -> dict[str, Any]:
        return self._request_json(
            MINUTE_URL,
            {
                "secid": f"90.{code}",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "1",
                "fqt": "1",
                "lmt": "300",
                "end": "20500101",
            },
        )


def _page_items(payload: Mapping[str, Any], *, page: int) -> tuple[int, list[dict]]:
    data = payload.get("data")
    if not isinstance(data, Mapping) or "total" not in data:
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney directory page {page} omitted total evidence"
        )
    try:
        total = int(data.get("total"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataBlocked("DATA_BLOCKED: Eastmoney directory total is invalid") from exc
    raw = data.get("diff")
    if isinstance(raw, Mapping):
        def sort_key(item: tuple[Any, Any]) -> tuple[int, str]:
            key = str(item[0])
            return (int(key), key) if key.isdigit() else (10**9, key)

        raw = [value for _key, value in sorted(raw.items(), key=sort_key)]
    if not isinstance(raw, list) or any(not isinstance(item, Mapping) for item in raw):
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney directory page {page} has invalid rows"
        )
    return total, [dict(item) for item in raw]


def fetch_complete_directory(
    provider: EastmoneyConceptProvider,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DirectorySnapshot:
    if page_size < 1:
        raise ValueError("page_size must be positive")
    first_payload = provider.fetch_directory_page(1, page_size)
    total, first = _page_items(first_payload, page=1)
    if total < MIN_DIRECTORY_CODES:
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney directory is implausibly small: "
            f"reported={total}, minimum={MIN_DIRECTORY_CODES}"
        )
    expected_pages = (total + page_size - 1) // page_size
    all_items = list(first)
    if len(first) != min(page_size, total):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney directory first page is partial: "
            f"rows={len(first)}, expected={min(page_size, total)}"
        )
    for page in range(2, expected_pages + 1):
        page_total, rows = _page_items(
            provider.fetch_directory_page(page, page_size), page=page
        )
        expected_rows = min(page_size, total - (page - 1) * page_size)
        if page_total != total or len(rows) != expected_rows:
            raise DataBlocked(
                "DATA_BLOCKED: Eastmoney directory pagination is incomplete or changed: "
                f"page={page}/{expected_pages}, rows={len(rows)}, "
                f"expected_rows={expected_rows}, reported_total={page_total}, expected_total={total}"
            )
        all_items.extend(rows)

    codes = [str(item.get("f12") or "").strip().upper() for item in all_items]
    if (
        len(all_items) != total
        or len(set(codes)) != total
        or any(not code.startswith("BK") or len(code) < 4 for code in codes)
    ):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney directory code inventory differs from pagination: "
            f"rows={len(all_items)}, distinct_codes={len(set(codes))}, reported={total}"
        )
    source_times = [_eastmoney_source_time(item.get("f124")) for item in all_items]
    source_dates = sorted({item.date().isoformat() for item in source_times})
    ordered = tuple(
        item for _code, item in sorted(zip(codes, all_items), key=lambda pair: pair[0])
    )
    ordered_codes = tuple(sorted(codes))
    evidence = {
        "schema": DIRECTORY_SCHEMA,
        "provider": PROVIDER_ID,
        "source_url": DIRECTORY_URL,
        "source_filter": DIRECTORY_FILTER,
        "reported_count": total,
        "observed_count": len(ordered_codes),
        "page_size": page_size,
        "pages_expected": expected_pages,
        "pages_fetched": expected_pages,
        "pagination_complete": True,
        "source_dates": source_dates,
        "first_source_time": min(source_times).isoformat(),
        "last_source_time": max(source_times).isoformat(),
        "code_set_sha256": _code_set_hash(ordered_codes),
    }
    evidence["manifest_sha256"] = _digest(evidence)
    return DirectorySnapshot(ordered, ordered_codes, evidence)


def validate_directory_target(
    snapshot: DirectorySnapshot,
    target_date: str,
    *,
    observed_at: datetime | None = None,
) -> None:
    target = _iso_date(target_date, field="target date")
    evidence = snapshot.evidence
    if (
        evidence.get("pagination_complete") is not True
        or int(evidence.get("reported_count") or 0) != len(snapshot.codes)
        or int(evidence.get("observed_count") or 0) != len(snapshot.codes)
        or evidence.get("source_dates") != [target]
        or evidence.get("code_set_sha256") != _code_set_hash(snapshot.codes)
    ):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney directory is not an exact target-date inventory"
        )
    first_source_time = datetime.fromisoformat(str(evidence["first_source_time"]))
    if first_source_time.astimezone(SHANGHAI).time() < datetime_time(15, 0):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney directory source time is before market close"
        )
    if observed_at is not None:
        observed = observed_at
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=SHANGHAI)
        observed = observed.astimezone(SHANGHAI)
        last_source_time = datetime.fromisoformat(str(evidence["last_source_time"]))
        if last_source_time.astimezone(SHANGHAI) > observed + timedelta(minutes=5):
            raise DataBlocked(
                "DATA_BLOCKED: Eastmoney directory source time is in the future"
            )


def build_current_frame(
    snapshot: DirectorySnapshot,
    *,
    target_date: str,
    ingested_at: datetime,
) -> pd.DataFrame:
    validate_directory_target(snapshot, target_date)
    rows: list[dict[str, Any]] = []
    for item in snapshot.items:
        source_time = _eastmoney_source_time(item.get("f124"))
        code = str(item.get("f12") or "").strip().upper()
        rows.append(
            {
                "index_code": code,
                "trade_time": source_time.replace(tzinfo=None),
                "trade_date": target_date,
                "open": _finite_number(item.get("f17"), field=f"{code}.open", positive=True),
                "price": _finite_number(item.get("f2"), field=f"{code}.price", positive=True),
                "high": _finite_number(item.get("f15"), field=f"{code}.high", positive=True),
                "low": _finite_number(item.get("f16"), field=f"{code}.low", positive=True),
                "volume": _finite_number(item.get("f5"), field=f"{code}.volume", nonnegative=True),
                "amount": _finite_number(item.get("f6"), field=f"{code}.amount", nonnegative=True),
                "change": _finite_number(item.get("f4"), field=f"{code}.change"),
                "change_pct": _finite_number(item.get("f3"), field=f"{code}.change_pct"),
                "snapshot_at": source_time.replace(tzinfo=None),
                "etl_sync_at": ingested_at,
            }
        )
    return _validate_frame_matrix(
        pd.DataFrame(rows),
        dataset="current",
        expected_codes=snapshot.codes,
        expected_dates=(target_date,),
        expected_rows_per_code=1,
    )


def _provider_lines(payload: Mapping[str, Any], *, code: str, field: str) -> list[str]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} has no data for {code}")
    if str(data.get("code") or "").strip().upper() != code:
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney {field} code mismatch for {code}"
        )
    rows = data.get(field)
    if not isinstance(rows, list) or not rows or any(not isinstance(row, str) for row in rows):
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {field} is empty for {code}")
    return rows


def _parse_daily_code(
    code: str,
    lines: Sequence[str],
    *,
    expected_dates: set[str],
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    observed: list[str] = []
    for line in lines:
        parts = line.split(",")
        if len(parts) < 11:
            raise DataBlocked(f"DATA_BLOCKED: Eastmoney daily row is malformed for {code}")
        trade_date = _iso_date(parts[0], field=f"{code} daily source date")
        if trade_date not in expected_dates:
            raise DataBlocked(
                f"DATA_BLOCKED: Eastmoney daily returned mixed/out-of-range date: "
                f"code={code}, date={trade_date}"
            )
        observed.append(trade_date)
        rows.append(
            {
                "index_code": code,
                "trade_time": datetime.strptime(trade_date, "%Y-%m-%d"),
                "trade_date": trade_date,
                "k_type": 1,
                "open": _finite_number(parts[1], field=f"{code}.{trade_date}.open", positive=True),
                "close": _finite_number(parts[2], field=f"{code}.{trade_date}.close", positive=True),
                "high": _finite_number(parts[3], field=f"{code}.{trade_date}.high", positive=True),
                "low": _finite_number(parts[4], field=f"{code}.{trade_date}.low", positive=True),
                "volume": _finite_number(parts[5], field=f"{code}.{trade_date}.volume", nonnegative=True),
                "amount": _finite_number(parts[6], field=f"{code}.{trade_date}.amount", nonnegative=True),
                "change_pct": _finite_number(parts[8], field=f"{code}.{trade_date}.change_pct"),
                "change": _finite_number(parts[9], field=f"{code}.{trade_date}.change"),
                "etl_sync_at": ingested_at,
            }
        )
    if set(observed) != expected_dates or len(observed) != len(expected_dates):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney daily date coverage is partial: "
            f"code={code}, observed={sorted(set(observed))}, expected={sorted(expected_dates)}"
        )
    return rows


def _parse_minute_code(
    code: str,
    lines: Sequence[str],
    *,
    target_date: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    expected_grid = set(_minute_grid(target_date))
    rows: list[dict[str, Any]] = []
    observed: list[datetime] = []
    for line in lines:
        parts = line.split(",")
        if len(parts) < 11:
            raise DataBlocked(f"DATA_BLOCKED: Eastmoney minute row is malformed for {code}")
        try:
            trade_time = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
        except ValueError as exc:
            raise DataBlocked(
                f"DATA_BLOCKED: Eastmoney minute source time is invalid for {code}"
            ) from exc
        if trade_time not in expected_grid:
            raise DataBlocked(
                "DATA_BLOCKED: Eastmoney minute returned mixed/out-of-grid time: "
                f"code={code}, trade_time={trade_time.isoformat()}"
            )
        observed.append(trade_time)
        rows.append(
            {
                "index_code": code,
                "trade_time": trade_time,
                "trade_date": target_date,
                "price": _finite_number(parts[2], field=f"{code}.{parts[0]}.price", positive=True),
                "avg_price": None,
                "change": _finite_number(parts[9], field=f"{code}.{parts[0]}.change"),
                "change_pct": _finite_number(parts[8], field=f"{code}.{parts[0]}.change_pct"),
                "volume": _finite_number(parts[5], field=f"{code}.{parts[0]}.volume", nonnegative=True) * 100,
                "amount": _finite_number(parts[6], field=f"{code}.{parts[0]}.amount", nonnegative=True),
                "snapshot_at": ingested_at,
                "etl_sync_at": ingested_at,
            }
        )
    if set(observed) != expected_grid or len(observed) != len(expected_grid):
        raise DataBlocked(
            "DATA_BLOCKED: Eastmoney minute grid is partial: "
            f"code={code}, observed={len(set(observed))}, expected={len(expected_grid)}"
        )
    return rows


def _collect_exact_codes(
    codes: Sequence[str],
    fetch_one,
    *,
    dataset: str,
    workers: int,
) -> list[dict[str, Any]]:
    results: dict[str, list[dict[str, Any]]] = {}
    failures: list[tuple[str, Exception]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = {pool.submit(fetch_one, code): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                rows = future.result()
                if not rows:
                    raise DataBlocked(f"DATA_BLOCKED: {dataset} returned no rows")
                results[code] = rows
            except Exception as exc:  # aggregate every failed shard before DML
                failures.append((code, exc))
    if failures or set(results) != set(codes):
        samples = [f"{code}:{str(exc)[:160]}" for code, exc in sorted(failures)[:5]]
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney {dataset} code coverage is partial: "
            f"expected={len(codes)}, observed={len(results)}, failures={len(failures)}, "
            f"samples={samples}"
        )
    return [row for code in sorted(codes) for row in results[code]]


def collect_daily_frame(
    provider: EastmoneyConceptProvider,
    snapshot: DirectorySnapshot,
    *,
    start_date: str,
    end_date: str,
    expected_dates: Sequence[str],
    ingested_at: datetime,
    workers: int,
) -> pd.DataFrame:
    expected = set(expected_dates)

    def fetch_one(code: str) -> list[dict[str, Any]]:
        payload = provider.fetch_daily(code, start_date, end_date)
        return _parse_daily_code(
            code,
            _provider_lines(payload, code=code, field="klines"),
            expected_dates=expected,
            ingested_at=ingested_at,
        )

    rows = _collect_exact_codes(snapshot.codes, fetch_one, dataset="daily", workers=workers)
    return _validate_frame_matrix(
        pd.DataFrame(rows),
        dataset="kline",
        expected_codes=snapshot.codes,
        expected_dates=tuple(expected_dates),
        expected_rows_per_code=len(expected_dates),
    )


def collect_minute_frame(
    provider: EastmoneyConceptProvider,
    snapshot: DirectorySnapshot,
    *,
    target_date: str,
    ingested_at: datetime,
    workers: int,
) -> pd.DataFrame:
    def fetch_one(code: str) -> list[dict[str, Any]]:
        payload = provider.fetch_minute(code)
        return _parse_minute_code(
            code,
            _provider_lines(payload, code=code, field="klines"),
            target_date=target_date,
            ingested_at=ingested_at,
        )

    rows = _collect_exact_codes(snapshot.codes, fetch_one, dataset="minute", workers=workers)
    return _validate_frame_matrix(
        pd.DataFrame(rows),
        dataset="minute",
        expected_codes=snapshot.codes,
        expected_dates=(target_date,),
        expected_rows_per_code=len(_minute_grid(target_date)),
    )


def _validate_frame_matrix(
    frame: pd.DataFrame,
    *,
    dataset: str,
    expected_codes: Sequence[str],
    expected_dates: Sequence[str],
    expected_rows_per_code: int,
) -> pd.DataFrame:
    columns = DATASET_COLUMNS[dataset]
    if frame is None or frame.empty or any(column not in frame.columns for column in columns):
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {dataset} frame is empty or malformed")
    result = frame.loc[:, list(columns)].copy()
    codes = result["index_code"].astype(str)
    dates = result["trade_date"].astype(str).str.slice(0, 10)
    identities = (
        ["index_code"]
        if dataset == "current"
        else ["index_code", "trade_date", "k_type"]
        if dataset == "kline"
        else ["index_code", "trade_time"]
    )
    if result.duplicated(subset=identities, keep=False).any():
        raise DataBlocked(f"DATA_BLOCKED: Eastmoney {dataset} has duplicate identities")
    expected_code_set = set(expected_codes)
    expected_date_set = set(expected_dates)
    counts = result.groupby("index_code", sort=True).size().to_dict()
    if (
        set(codes) != expected_code_set
        or set(dates) != expected_date_set
        or len(result) != len(expected_codes) * expected_rows_per_code
        or any(int(counts.get(code, 0)) != expected_rows_per_code for code in expected_codes)
    ):
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney {dataset} matrix is incomplete or mixed-date: "
            f"rows={len(result)}, expected_rows={len(expected_codes) * expected_rows_per_code}, "
            f"codes={len(set(codes))}/{len(expected_codes)}, dates={sorted(set(dates))}"
        )
    return result.sort_values(identities, kind="stable").reset_index(drop=True)


def load_open_trade_dates(engine, start_date: str, end_date: str) -> tuple[str, ...]:
    start = _iso_date(start_date, field="range start")
    end = _iso_date(end_date, field="range end")
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT trade_date FROM si_trade_calendar "
                "WHERE trade_status=1 AND trade_date BETWEEN :start_date AND :end_date "
                "ORDER BY trade_date"
            ),
            {"start_date": start, "end_date": end},
        ).fetchall()
    dates = tuple(str(row[0])[:10] for row in rows)
    if not dates or dates[0] < start or dates[-1] > end:
        raise DataBlocked("DATA_BLOCKED: authoritative trade-date range is empty or invalid")
    return dates


def resolve_publish_window(
    engine,
    *,
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    now: datetime | None = None,
) -> tuple[str, str, tuple[str, ...]]:
    authoritative = authoritative_closed_trade_date(engine, now=now)
    if not authoritative:
        raise DataBlocked("DATA_BLOCKED: authoritative closed trade date is unavailable")
    authoritative = _iso_date(authoritative, field="authoritative closed trade date")
    supplied_trade = _iso_date(trade_date, field="trade date") if trade_date else ""
    supplied_end = _iso_date(end_date, field="range end") if end_date else ""
    if supplied_trade and supplied_end and supplied_trade != supplied_end:
        raise DataBlocked("DATA_BLOCKED: trade date and range end disagree")
    target_end = supplied_end or supplied_trade or authoritative
    target_start = _iso_date(start_date, field="range start") if start_date else target_end
    if target_start > target_end:
        raise DataBlocked("DATA_BLOCKED: range start is after range end")
    if target_end > authoritative:
        raise DataBlocked(
            "DATA_BLOCKED: requested range is not closed: "
            f"end={target_end}, authoritative={authoritative}"
        )
    open_dates = load_open_trade_dates(engine, target_start, target_end)
    if target_end not in open_dates:
        raise DataBlocked(f"DATA_BLOCKED: range end is not an open session: {target_end}")
    return target_start, target_end, open_dates


def _scalar_for_sql(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _records(frame: pd.DataFrame, columns: Sequence[str]) -> list[dict[str, Any]]:
    return [
        {column: _scalar_for_sql(row[column]) for column in columns}
        for row in frame.loc[:, list(columns)].to_dict(orient="records")
    ]


@contextmanager
def _publication_connection(engine, *, use_mysql_lock: bool):
    if use_mysql_lock:
        with mysql_named_lock(
            engine,
            PUBLISH_LOCK_NAME,
            timeout_seconds=max(0, int(os.environ.get("EAST_CONCEPT_LOCK_TIMEOUT", "30"))),
        ) as connection:
            yield connection
    else:
        with engine.connect() as connection:
            yield connection


def _scope_predicate(dataset: str) -> tuple[str, dict[str, Any]]:
    if dataset == "current":
        return "", {}
    if dataset == "kline":
        return (
            " WHERE trade_date BETWEEN :start_date AND :end_date AND k_type=1",
            {},
        )
    return " WHERE trade_date BETWEEN :start_date AND :end_date", {}


def _dataset_evidence(frame: pd.DataFrame, dataset: str) -> dict[str, Any]:
    records = _records(frame, DATASET_COLUMNS[dataset])
    dates = sorted({str(row["trade_date"])[:10] for row in records})
    codes = sorted({str(row["index_code"]) for row in records})
    return {
        "dataset": dataset,
        "table": DATASET_TABLE[dataset],
        "provider": PROVIDER_ID,
        "source_url": (
            DIRECTORY_URL
            if dataset == "current"
            else DAILY_URL
            if dataset == "kline"
            else MINUTE_URL
        ),
        "row_count": len(records),
        "code_count": len(codes),
        "date_count": len(dates),
        "first_date": dates[0],
        "last_date": dates[-1],
        "code_set_sha256": _code_set_hash(codes),
        "content_sha256": _digest(records),
    }


def publish_frames_atomically(
    engine,
    frames: Mapping[str, pd.DataFrame],
    *,
    start_date: str,
    end_date: str,
    use_mysql_lock: bool = True,
) -> dict[str, dict[str, int]]:
    """Replace every requested full range in one transaction, with DML only."""

    datasets = tuple(sorted(frames))
    if not datasets or any(dataset not in DATASET_TABLE for dataset in datasets):
        raise ValueError("publisher requires known nonempty datasets")
    required_columns = {
        DATASET_TABLE[dataset]: set(DATASET_COLUMNS[dataset]) for dataset in datasets
    }
    validate_required_table_surface(
        engine,
        set(required_columns),
        context="Eastmoney concept market publisher",
        required_columns=required_columns,
    )
    expected = {dataset: _dataset_evidence(frames[dataset], dataset) for dataset in datasets}
    metrics: dict[str, dict[str, int]] = {}
    with _publication_connection(engine, use_mysql_lock=use_mysql_lock) as connection:
        if connection.in_transaction():
            connection.commit()
        with connection.begin():
            for dataset in datasets:
                table = quote_identifier(DATASET_TABLE[dataset])
                predicate, params = _scope_predicate(dataset)
                params.update({"start_date": start_date, "end_date": end_date})
                connection.execute(text(f"DELETE FROM {table}{predicate}"), params)
                columns = DATASET_COLUMNS[dataset]
                column_sql = ", ".join(quote_identifier(column) for column in columns)
                value_sql = ", ".join(f":{column}" for column in columns)
                rows = _records(frames[dataset], columns)
                for offset in range(0, len(rows), 1000):
                    connection.execute(
                        text(f"INSERT INTO {table} ({column_sql}) VALUES ({value_sql})"),
                        rows[offset : offset + 1000],
                    )
                aggregate = connection.execute(
                    text(
                        f"SELECT COUNT(*), COUNT(DISTINCT index_code), "
                        f"COUNT(DISTINCT trade_date) FROM {table}{predicate}"
                    ),
                    params,
                ).one()
                actual = {
                    "row_count": int(aggregate[0] or 0),
                    "code_count": int(aggregate[1] or 0),
                    "date_count": int(aggregate[2] or 0),
                }
                for key in ("row_count", "code_count", "date_count"):
                    if actual[key] != int(expected[dataset][key]):
                        raise RuntimeError(
                            "Eastmoney concept publish verification mismatch: "
                            f"dataset={dataset}, field={key}, "
                            f"expected={expected[dataset][key]}, actual={actual[key]}"
                        )
                metrics[dataset] = actual
    return metrics


def run_publisher(
    engine,
    provider: EastmoneyConceptProvider,
    *,
    datasets: Sequence[str],
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    now: datetime | None = None,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
) -> dict[str, Any]:
    requested = tuple(sorted(set(datasets)))
    if "all" in requested:
        requested = ("current", "kline", "minute")
    if not requested or any(dataset not in DATASET_TABLE for dataset in requested):
        raise DataBlocked(f"DATA_BLOCKED: unsupported dataset selection: {requested}")
    range_start, range_end, open_dates = resolve_publish_window(
        engine,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        now=now,
    )
    if "minute" in requested and range_start != range_end:
        raise DataBlocked("DATA_BLOCKED: concept minute publisher accepts one target day only")

    snapshot: DirectorySnapshot | None = None
    frames: dict[str, pd.DataFrame] = {}

    def partial_result() -> dict[str, Any]:
        return {
            "provider": PROVIDER_ID,
            "datasets": list(requested),
            "target_trade_date": range_end,
            "range_start": range_start,
            "range_end": range_end,
            "open_date_count": len(open_dates),
            "open_dates_sha256": _digest(list(open_dates)),
            "directory": snapshot.evidence if snapshot is not None else {},
            "dataset_results": {
                dataset: _dataset_evidence(frame, dataset)
                for dataset, frame in frames.items()
            },
            "db_metrics": {},
            "published": False,
        }

    try:
        current = now or datetime.now(SHANGHAI)
        if current.tzinfo is not None:
            current = current.astimezone(SHANGHAI)
        snapshot = fetch_complete_directory(provider)
        validate_directory_target(snapshot, range_end, observed_at=current)
        ingested_at = current.replace(tzinfo=None, microsecond=0)
        if "current" in requested:
            frames["current"] = build_current_frame(
                snapshot, target_date=range_end, ingested_at=ingested_at
            )
        if "kline" in requested:
            frames["kline"] = collect_daily_frame(
                provider,
                snapshot,
                start_date=range_start,
                end_date=range_end,
                expected_dates=open_dates,
                ingested_at=ingested_at,
                workers=workers,
            )
        if "minute" in requested:
            frames["minute"] = collect_minute_frame(
                provider,
                snapshot,
                target_date=range_end,
                ingested_at=ingested_at,
                workers=workers,
            )
        result = partial_result()
        if not dry_run:
            result["db_metrics"] = publish_frames_atomically(
                engine,
                frames,
                start_date=range_start,
                end_date=range_end,
            )
            result["published"] = True
        return result
    except Exception as exc:
        evidence = partial_result()
        if isinstance(exc, DataBlocked):
            exc.result = evidence
            raise
        raise DataBlocked(
            f"DATA_BLOCKED: Eastmoney concept publication failed: {exc}",
            result=evidence,
        ) from exc


def build_receipt(
    *,
    status: str,
    datasets: Sequence[str],
    started_at: datetime,
    finished_at: datetime,
    result: Mapping[str, Any] | None = None,
    reason: str = "",
    requested_trade_date: str = "",
    requested_start_date: str = "",
    requested_end_date: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "receipt_id": uuid.uuid4().hex,
        "status": str(status).upper(),
        "provider": PROVIDER_ID,
        "datasets": sorted(set(datasets)),
        "requested_trade_date": requested_trade_date or None,
        "requested_start_date": requested_start_date or None,
        "requested_end_date": requested_end_date or None,
        "target_trade_date": None,
        "range_start": None,
        "range_end": None,
        "directory_count": 0,
        "dataset_results": {},
        "published": False,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }
    if isinstance(result, Mapping):
        directory = result.get("directory")
        payload.update(
            {
                "datasets": list(result.get("datasets") or payload["datasets"]),
                "target_trade_date": result.get("target_trade_date"),
                "range_start": result.get("range_start"),
                "range_end": result.get("range_end"),
                "open_date_count": int(result.get("open_date_count") or 0),
                "open_dates_sha256": result.get("open_dates_sha256"),
                "directory_count": int(directory.get("observed_count") or 0)
                if isinstance(directory, Mapping)
                else 0,
                "directory": directory or {},
                "dataset_results": result.get("dataset_results") or {},
                "db_metrics": result.get("db_metrics") or {},
                "published": bool(result.get("published")),
            }
        )
    if reason:
        payload["reason"] = str(reason)[:1000]
    payload["result_sha256"] = _digest(payload)
    return payload


def main(
    argv: list[str] | None = None,
    *,
    engine_factory=None,
    provider_factory=None,
) -> int:
    parser = argparse.ArgumentParser(description="严格东财概念行情正式发布器")
    parser.add_argument("date_arg", nargs="?", default="", help=argparse.SUPPRESS)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=("current", "kline", "minute", "all"),
        default=[],
    )
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", help="兼容调度参数；输出始终为单行 JSON")
    args = parser.parse_args(argv)
    datasets = args.dataset or ["current"]
    if "all" in datasets:
        datasets = ["current", "kline", "minute"]
    trade_date = args.trade_date or (
        args.date_arg if str(args.date_arg).startswith("20") else ""
    )
    started_at = datetime.now(SHANGHAI).replace(microsecond=0)
    result: Mapping[str, Any] | None = None
    try:
        engine_builder = engine_factory or create_tool_engine
        provider_builder = provider_factory or EastmoneyConceptProvider
        result = run_publisher(
            engine_builder(),
            provider_builder(),
            datasets=datasets,
            trade_date=trade_date,
            start_date=args.start_date,
            end_date=args.end_date,
            workers=args.workers,
            dry_run=args.dry_run,
        )
        status = "PASS"
        exit_code = 0
        reason = ""
    except Exception as exc:
        status = "DATA_BLOCKED"
        exit_code = 2
        reason = str(exc) or type(exc).__name__
        if isinstance(exc, DataBlocked) and exc.result is not None:
            result = exc.result
    receipt = build_receipt(
        status=status,
        datasets=datasets,
        started_at=started_at,
        finished_at=datetime.now(SHANGHAI).replace(microsecond=0),
        result=result,
        reason=reason,
        requested_trade_date=trade_date,
        requested_start_date=args.start_date,
        requested_end_date=args.end_date,
    )
    print(_canonical_json(receipt), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
