#!/usr/bin/env python3
"""Publish one complete A-share daily partition from a free public source.

This collector runs in the normal platform Python runtime, outside QMT's
restricted embedded interpreter.  It writes provider-labelled rows and a
whole-partition receipt.  It never replaces an existing QMT partition and
never labels public data as QMT-attested.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
import sys
import time
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
VENDOR_ROOT = ROOT / "third_party"
if str(VENDOR_ROOT) not in sys.path:
    sys.path.insert(0, str(VENDOR_ROOT))

from server.common.batch_db import create_batch_engine, replace_table_rows_exact_keys
from server.common.kline_data import get_kline_engine
from server.common.mysql_lock import STOCK_KLINE_FREEZE_LOCK_NAME
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.verified_daily_market_truth import (
    FREE_DAILY_DATASET,
    FREE_DAILY_MIN_COVERAGE,
    FREE_DAILY_PARTITION_KEY,
    FREE_HISTORY_MIN_CATALOG_COVERAGE,
    VERIFIED_DAILY_RECEIPT_SCHEMA,
    canonical_daily_rows,
    load_source_daily_rows,
)
from tools.env_config import load_project_env


SOURCE = "tencent_qt"
BAOSTOCK_SOURCE = "baostock"
MULTI_SOURCE = "baostock_tencent"
VERIFIED_HISTORY_SOURCE = "tencent_qt_verified"
SINA_HISTORY_SOURCE = "sina_history"
RESULT_SCHEMA = "probiga.free-stock-daily-result.v1"
QUOTE_BATCH_SIZE = 100
QUOTE_DEADLINE_SECONDS = 180
HISTORY_DEADLINE_SECONDS = 600
BAOSTOCK_DEADLINE_SECONDS = 1800
DB_CODE_CHUNK_SIZE = 400
_LINE_RE = re.compile(r'v_([^=]+)="(.*)";?$')


class FreeDailyBlocked(RuntimeError):
    """The public provider did not prove a complete target-date partition."""


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _progress(stage: str, **details: Any) -> None:
    print(
        json.dumps({"stage": stage, **details}, ensure_ascii=False, sort_keys=True),
        file=sys.stderr,
        flush=True,
    )


def _date(value: str) -> str:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != raw:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD")
    return raw


def _latest_closed_session(engine: Any, now: datetime) -> str:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MAX(trade_date)
                  FROM si_trade_calendar
                 WHERE trade_status=1 AND trade_date<=:today
                """
            ),
            {"today": now.date().isoformat()},
        ).scalar()
    if value is None:
        raise FreeDailyBlocked("trade calendar has no closed session")
    return str(value)[:10]


def _fetch_quote_batch(
    session: requests.Session,
    codes: list[str],
    *,
    timeout: float,
) -> list[dict[str, Any]]:
    symbols = []
    for code in codes:
        if code.startswith(("00", "30")):
            symbols.append("sz" + code)
        elif code.startswith(("43", "83", "87", "92")):
            symbols.append("bj" + code)
        else:
            symbols.append("sh" + code)
    response = session.get(
        "https://qt.gtimg.cn/q=" + ",".join(symbols),
        timeout=timeout,
    )
    response.raise_for_status()
    text_body = response.content.decode("gbk", errors="replace")
    result: list[dict[str, Any]] = []
    for line in text_body.splitlines():
        match = _LINE_RE.match(line.strip())
        if not match:
            continue
        source_symbol, body = match.groups()
        parts = body.split("~")
        if len(parts) < 39:
            continue
        triple = parts[35].split("/")
        result.append(
            {
                "stock_code": str(parts[2]).strip().zfill(6),
                "short_name": str(parts[1]).strip()[:128],
                "source_symbol": source_symbol,
                "source_time": parts[30],
                "open": parts[5],
                "high": parts[33],
                "low": parts[34],
                "close": parts[3],
                "pre_close": parts[4],
                "volume_hands": parts[6],
                "amount_cny": triple[2] if len(triple) >= 3 else parts[37],
                "change": parts[31],
                "change_pct": parts[32],
                "turnover_ratio": parts[38],
            }
        )
    return result


def fetch_target_snapshot(
    codes: list[str],
    *,
    target_date: str,
    timeout: float = 20,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    requested = sorted({str(code).zfill(6) for code in codes if code})
    if not requested:
        raise FreeDailyBlocked("catalog target universe is empty")
    deadline = time.monotonic() + QUOTE_DEADLINE_SECONDS
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"}
    with requests.Session() as session:
        session.headers.update(headers)

        def pull(batch: list[str]) -> list[dict[str, Any]]:
            if time.monotonic() >= deadline:
                raise TimeoutError("free daily quote deadline exceeded")
            return _fetch_quote_batch(session, batch, timeout=timeout)

        batches = [
            requested[index : index + QUOTE_BATCH_SIZE]
            for index in range(0, len(requested), QUOTE_BATCH_SIZE)
        ]
        records: list[dict[str, Any]] = []
        failures: list[str] = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(pull, batch): batch for batch in batches}
            for future in as_completed(futures):
                try:
                    records.extend(future.result())
                except Exception as exc:
                    failures.append(f"{type(exc).__name__}: {exc}")
        if failures:
            raise FreeDailyBlocked(
                "free daily quote batches failed: " + "; ".join(failures[:5])
            )

    frame = pd.DataFrame(records)
    if frame.empty:
        raise FreeDailyBlocked("free daily quote source returned no rows")
    for column in (
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume_hands",
        "amount_cny",
        "change",
        "change_pct",
        "turnover_ratio",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["source_time"] = pd.to_datetime(
        frame["source_time"], format="%Y%m%d%H%M%S", errors="coerce"
    )
    frame["trade_date"] = frame["source_time"].dt.strftime("%Y-%m-%d")
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame = frame.drop_duplicates("stock_code", keep="last")
    outside = sorted(set(frame["stock_code"]) - set(requested))
    if outside:
        raise FreeDailyBlocked(
            "free daily source returned out-of-catalog codes: "
            + ",".join(outside[:10])
        )
    complete_core = frame[
        ["open", "high", "low", "close", "pre_close", "volume_hands", "amount_cny"]
    ].notna().all(axis=1)
    same_target = frame["trade_date"].eq(target_date)
    suspended = (
        complete_core
        & frame[["open", "high", "low"]].eq(0).all(axis=1)
        & frame["volume_hands"].eq(0)
        & frame["amount_cny"].eq(0)
    )
    traded = (
        complete_core
        & frame[["open", "high", "low", "close", "pre_close"]].gt(0).all(axis=1)
        & frame["high"].ge(frame[["open", "close"]].max(axis=1))
        & frame["low"].le(frame[["open", "close"]].min(axis=1))
        & frame["volume_hands"].ge(0)
        & frame["amount_cny"].ge(0)
    )
    target = frame.loc[same_target & (traded | suspended)].copy()
    target["_suspended"] = (
        same_target.loc[target.index] & suspended.loc[target.index]
    )
    received = set(target["stock_code"])
    coverage = len(received) / len(requested)
    if coverage < FREE_DAILY_MIN_COVERAGE:
        raise FreeDailyBlocked(
            "free daily catalog coverage below threshold: "
            f"{len(received)}/{len(requested)} ({coverage:.2%})"
        )
    target["volume"] = (target["volume_hands"] * 100).round().astype("int64")
    target["trade_time"] = target["trade_date"] + " 15:00:00"
    target["k_type"] = 1
    target["adjust_type"] = 0
    target["data_source"] = SOURCE
    target["quality_status"] = target["_suspended"].map(
        {True: "SUSPENDED", False: "SOURCE_VERIFIED"}
    )
    target["permission_status"] = target["_suspended"].map(
        {True: "NO_TRADE", False: "SUPPORTED"}
    )
    target["received_at"] = target["source_time"]
    target["etl_sync_at"] = _now()
    target = target[
        [
            "stock_code",
            "short_name",
            "trade_time",
            "trade_date",
            "k_type",
            "adjust_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount_cny",
            "change",
            "change_pct",
            "turnover_ratio",
            "pre_close",
            "data_source",
            "quality_status",
            "permission_status",
            "received_at",
            "etl_sync_at",
            "_suspended",
        ]
    ].rename(columns={"amount_cny": "amount"})
    traded_count = int((~target["_suspended"]).sum())
    suspended_count = int(target["_suspended"].sum())
    target = target.drop(columns=["_suspended"]).sort_values(
        "stock_code"
    ).reset_index(drop=True)
    return target, {
        "requested_count": len(requested),
        "received_count": len(received),
        "coverage_ratio": coverage,
        "traded_count": traded_count,
        "suspended_count": suspended_count,
        "captured_at": target["received_at"].max().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _source_symbol(code: str) -> str:
    if code.startswith(("00", "30")):
        return "sz" + code
    if code.startswith(("43", "83", "87", "92")):
        return "bj" + code
    return "sh" + code


def _baostock_symbol(code: str) -> str | None:
    if code.startswith(("43", "83", "87", "92")):
        return None
    return ("sz." if code.startswith(("00", "30")) else "sh.") + code


def fetch_baostock_primary_snapshot(
    codes: list[str],
    *,
    target_date: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Use Baostock for SH/SZ daily facts and Tencent for BJ plus gaps."""

    import baostock as bs

    fallback, tencent_evidence = fetch_target_snapshot(
        codes,
        target_date=target_date,
    )
    frame = fallback.copy()
    by_code = {
        str(row["stock_code"]).zfill(6): dict(row)
        for row in frame.to_dict(orient="records")
    }
    requested = sorted({str(code).zfill(6) for code in codes if code})
    fields = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,tradestatus,pctChg,turn"
    )
    deadline = time.monotonic() + BAOSTOCK_DEADLINE_SECONDS
    baostock_rows = 0
    failures: list[str] = []
    processed = 0
    with redirect_stdout(sys.stderr):
        login = bs.login()
        if login.error_code != "0":
            raise FreeDailyBlocked(
                f"Baostock login failed: {login.error_code} {login.error_msg}"
            )
        try:
            for code in requested:
                symbol = _baostock_symbol(code)
                if symbol is None:
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError("Baostock daily deadline exceeded")
                processed += 1
                try:
                    result = bs.query_history_k_data_plus(
                        symbol,
                        fields,
                        target_date,
                        target_date,
                        frequency="d",
                        adjustflag="3",
                    )
                    if result.error_code != "0":
                        failures.append(f"{code}:{result.error_code}")
                        continue
                    row = result.get_row_data() if result.next() else []
                    if not row:
                        failures.append(f"{code}:no_row")
                        continue
                    (
                        trade_date,
                        _source_code,
                        open_value,
                        high_value,
                        low_value,
                        close_value,
                        pre_close_value,
                        volume_value,
                        amount_value,
                        _adjust_flag,
                        trade_status,
                        pct_value,
                        turnover_value,
                    ) = row
                    if trade_date != target_date:
                        failures.append(f"{code}:date_mismatch")
                        continue
                    suspended = str(trade_status) == "0"
                    close_number = float(close_value)
                    pre_close_number = float(pre_close_value or close_value)
                    base = by_code.get(code, {})
                    updated = {
                        **base,
                        "stock_code": code,
                        "trade_time": f"{target_date} 15:00:00",
                        "trade_date": target_date,
                        "k_type": 1,
                        "adjust_type": 0,
                        "open": 0.0 if suspended else float(open_value),
                        "high": 0.0 if suspended else float(high_value),
                        "low": 0.0 if suspended else float(low_value),
                        "close": close_number,
                        "pre_close": pre_close_number,
                        "volume": 0 if suspended else int(float(volume_value)),
                        "amount": 0.0 if suspended else float(amount_value),
                        "change": (
                            0.0 if suspended else close_number - pre_close_number
                        ),
                        "change_pct": (
                            0.0 if suspended else float(pct_value or 0)
                        ),
                        "turnover_ratio": (
                            0.0 if suspended else float(turnover_value or 0)
                        ),
                        "data_source": BAOSTOCK_SOURCE,
                        "quality_status": (
                            "SUSPENDED" if suspended else "SOURCE_VERIFIED"
                        ),
                        "permission_status": (
                            "NO_TRADE" if suspended else "SUPPORTED"
                        ),
                        "received_at": _now(),
                        "etl_sync_at": _now(),
                    }
                    if suspended:
                        updated["change"] = 0.0
                        updated["change_pct"] = 0.0
                        updated["turnover_ratio"] = 0.0
                    by_code[code] = updated
                    baostock_rows += 1
                except Exception as exc:
                    failures.append(f"{code}:{type(exc).__name__}")
                if processed % 500 == 0:
                    _progress(
                        "baostock_progress",
                        completed=processed,
                        projected_total=sum(
                            1 for item in requested if _baostock_symbol(item)
                        ),
                        rows=baostock_rows,
                        failures=len(failures),
                    )
        finally:
            bs.logout()
    result_frame = pd.DataFrame(
        sorted(by_code.values(), key=lambda row: row["stock_code"])
    )
    received = set(result_frame["stock_code"].astype(str))
    coverage = len(received) / max(len(requested), 1)
    if coverage < FREE_DAILY_MIN_COVERAGE:
        raise FreeDailyBlocked(
            "Baostock/Tencent daily coverage below threshold: "
            f"{len(received)}/{len(requested)} ({coverage:.2%})"
        )
    suspended_count = int(
        result_frame["permission_status"].eq("NO_TRADE").sum()
    )
    baostock_count = int(result_frame["data_source"].eq(BAOSTOCK_SOURCE).sum())
    tencent_count = int(
        result_frame["data_source"].eq(SOURCE).sum()
    )
    return result_frame.reset_index(drop=True), {
        "requested_count": len(requested),
        "received_count": len(received),
        "coverage_ratio": coverage,
        "traded_count": len(result_frame) - suspended_count,
        "suspended_count": suspended_count,
        "baostock_count": baostock_count,
        "tencent_fallback_count": tencent_count,
        "baostock_failures": failures[:20],
        "captured_at": str(tencent_evidence["captured_at"]),
    }


def fetch_history_day(
    codes: list[str],
    *,
    target_date: str,
    timeout: float = 15,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Cross-check retained target-date bars against Tencent's raw day series."""

    requested = sorted({str(code).zfill(6) for code in codes if code})
    deadline = time.monotonic() + HISTORY_DEADLINE_SECONDS

    def pull(code: str) -> dict[str, Any] | None:
        if time.monotonic() >= deadline:
            raise TimeoutError("free historical verification deadline exceeded")
        symbol = _source_symbol(code)
        response = requests.get(
            "https://web.ifzq.gtimg.cn/appstock/app/kline/kline",
            params={
                "param": f"{symbol},day,{target_date},{target_date},5",
            },
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://gu.qq.com/",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        data = (response.json().get("data") or {}).get(symbol) or {}
        for item in data.get("day") or []:
            if len(item) < 6 or str(item[0]) != target_date:
                continue
            return {
                "stock_code": code,
                "open": item[1],
                "close": item[2],
                "high": item[3],
                "low": item[4],
                "volume": float(item[5]) * 100,
            }
        return None

    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(pull, code): code for code in requested}
        for future in as_completed(futures):
            try:
                row = future.result()
                if row is not None:
                    rows.append(row)
            except Exception as exc:
                failures.append(f"{futures[future]}:{type(exc).__name__}")
    if failures:
        raise FreeDailyBlocked(
            "free historical verification failed: " + ",".join(failures[:10])
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise FreeDailyBlocked("free historical source returned no rows")
    coverage = len(frame) / len(requested)
    if coverage < FREE_HISTORY_MIN_CATALOG_COVERAGE:
        raise FreeDailyBlocked(
            "free historical coverage below threshold: "
            f"{len(frame)}/{len(requested)} ({coverage:.2%})"
        )
    return frame, {
        "requested_count": len(requested),
        "received_count": len(frame),
        "coverage_ratio": coverage,
        "captured_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def fetch_sina_history_day(
    codes: list[str],
    *,
    target_date: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fetch exact historical OHLCV, amount and turnover from Sina."""

    from biz.stock_market.stock_kline_akshare import (
        akshare_daily_to_sm_kline,
        fetch_stock_daily_kline,
    )

    requested = sorted({str(code).zfill(6) for code in codes if code})
    deadline = time.monotonic() + HISTORY_DEADLINE_SECONDS

    def pull(code: str) -> pd.DataFrame | None:
        if time.monotonic() >= deadline:
            raise TimeoutError("Sina historical backfill deadline exceeded")
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
        start = (target - timedelta(days=10)).isoformat()
        raw = fetch_stock_daily_kline(
            code,
            start.replace("-", ""),
            target_date.replace("-", ""),
            "",
            timeout=20,
        )
        if raw is None or raw.empty:
            return None
        converted = akshare_daily_to_sm_kline(raw, code, 1, 0)
        return converted if converted is not None and not converted.empty else None

    parts: list[pd.DataFrame] = []
    failures: list[str] = []
    completed = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(pull, code): code for code in requested}
        for future in as_completed(futures):
            completed += 1
            try:
                frame = future.result()
                if frame is not None:
                    parts.append(frame)
            except Exception as exc:
                failures.append(f"{futures[future]}:{type(exc).__name__}")
            if completed % 500 == 0:
                _progress(
                    "sina_history_progress",
                    completed=completed,
                    total=len(requested),
                    rows=sum(len(item) for item in parts),
                )
    if failures:
        _progress(
            "sina_history_partial_failures",
            failed_count=len(failures),
            samples=failures[:10],
        )
    frame = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame()
    )
    if frame.empty:
        raise FreeDailyBlocked("Sina historical backfill returned no rows")
    frame = frame.loc[frame["trade_date"].astype(str).str[:10].eq(target_date)]
    frame = frame.drop_duplicates("stock_code", keep="last")
    coverage = len(frame) / len(requested)
    if coverage < FREE_HISTORY_MIN_CATALOG_COVERAGE:
        raise FreeDailyBlocked(
            "Sina historical catalog coverage below threshold: "
            f"{len(frame)}/{len(requested)} ({coverage:.2%})"
        )
    return frame.reset_index(drop=True), {
        "requested_count": len(requested),
        "received_count": len(frame),
        "coverage_ratio": coverage,
        "captured_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _load_existing_rows(
    engine: Any,
    *,
    target_date: str,
    codes: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with engine.connect() as connection:
        for offset in range(0, len(codes), DB_CODE_CHUNK_SIZE):
            chunk = codes[offset : offset + DB_CODE_CHUNK_SIZE]
            placeholders = ", ".join(
                f":code_{index}" for index in range(len(chunk))
            )
            rows.extend(
                dict(row)
                for row in connection.execute(
                    text(
                        f"""
                        SELECT stock_code, short_name, `open`, `close`, high, low,
                               volume, amount, `change`, change_pct,
                               turnover_ratio, pre_close
                          FROM sm_stock_kline
                         WHERE stock_code IN ({placeholders})
                           AND trade_date=:trade_date
                           AND k_type=1
                           AND adjust_type=0
                        """
                    ),
                    {
                        "trade_date": target_date,
                        **{
                            f"code_{index}": code
                            for index, code in enumerate(chunk)
                        },
                    },
                ).mappings()
            )
    return rows


def _latest_catalog(engine: Any, *, target_date: str, now: datetime) -> Any:
    with engine.connect() as connection:
        catalog = load_stock_catalog(connection, decision_known_at=now)
        catalog.eligible_codes(target_date)
    return catalog


def _source_counts(
    engine: Any,
    *,
    target_date: str,
    codes: list[str],
) -> dict[str, int]:
    """Read one source date through the stock_code-leading business index."""

    counts: dict[str, int] = {}
    with engine.connect() as connection:
        for offset in range(0, len(codes), DB_CODE_CHUNK_SIZE):
            chunk = codes[offset : offset + DB_CODE_CHUNK_SIZE]
            placeholders = ", ".join(
                f":code_{index}" for index in range(len(chunk))
            )
            rows = connection.execute(
                text(
                    f"""
                    SELECT data_source, COUNT(*) AS row_count
                      FROM sm_stock_kline
                     WHERE stock_code IN ({placeholders})
                       AND trade_date=:trade_date
                       AND k_type=1
                       AND adjust_type=0
                     GROUP BY data_source
                    """
                ),
                {
                    "trade_date": target_date,
                    **{
                        f"code_{index}": code
                        for index, code in enumerate(chunk)
                    },
                },
            ).mappings().all()
            for row in rows:
                source = str(row.get("data_source") or "")
                counts[source] = counts.get(source, 0) + int(
                    row.get("row_count") or 0
                )
    return counts


def _reject_mixed_source_partition(
    engine: Any,
    target_date: str,
    expected_codes: list[str],
) -> None:
    sources = _source_counts(
        engine,
        target_date=target_date,
        codes=expected_codes,
    )
    foreign = [
        (source, count)
        for source, count in sources.items()
        if source != SOURCE
    ]
    if foreign:
        raise FreeDailyBlocked(
            "target partition already belongs to another source: "
            + ",".join(f"{source}:{count}" for source, count in foreign)
        )


def _write_free_receipt(
    engine: Any,
    *,
    target_date: str,
    source: str,
    captured_at: str,
    row_count: int,
    codes: list[str],
    row_hash: str,
    coverage_ratio: float,
    catalog: Any,
    now: datetime,
    batch_id: str,
) -> dict[str, Any]:
    unsigned = {
        "schema": VERIFIED_DAILY_RECEIPT_SCHEMA,
        "dataset": FREE_DAILY_DATASET,
        "source": source,
        "target_date": target_date,
        "captured_at": captured_at,
        "row_count": row_count,
        "code_count": len(codes),
        "code_set_hash": canonical_digest(codes),
        "row_hash": row_hash,
        "coverage_ratio": float(coverage_ratio),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    receipt = {**unsigned, "receipt_id": canonical_digest(unsigned)}
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO acquisition_partition_state (
                    dataset, source, target_date, partition_key, status,
                    request_id, written_rows, last_attempt_at, last_success_at,
                    next_retry_at, last_error_code, last_error, detail_json,
                    updated_at
                ) VALUES (
                    :dataset, :source, :target_date, :partition_key, 'complete',
                    :request_id, :written_rows, :now_value, :now_value,
                    NULL, NULL, NULL, :detail_json, :now_value
                )
                ON DUPLICATE KEY UPDATE
                    status='complete', request_id=VALUES(request_id),
                    written_rows=VALUES(written_rows),
                    last_success_at=VALUES(last_success_at),
                    next_retry_at=NULL, last_error_code=NULL, last_error=NULL,
                    detail_json=VALUES(detail_json), updated_at=VALUES(updated_at)
                """
            ),
            {
                "dataset": FREE_DAILY_DATASET,
                "source": source,
                "target_date": target_date,
                "partition_key": FREE_DAILY_PARTITION_KEY,
                "request_id": batch_id[:64],
                "written_rows": row_count,
                "now_value": now,
                "detail_json": json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    return receipt


def publish_snapshot(
    engine: Any,
    *,
    target_date: str,
    now: datetime,
) -> dict[str, Any]:
    catalog = _latest_catalog(engine, target_date=target_date, now=now)
    _progress("catalog_ready", catalog_batch_id=catalog.batch_id)
    expected_codes = catalog.eligible_codes(target_date)
    _progress("source_check_start", expected_count=len(expected_codes))
    _reject_mixed_source_partition(engine, target_date, expected_codes)
    _progress("source_check_complete")
    _progress(
        "quote_fetch_start",
        expected_count=len(expected_codes),
        batch_size=QUOTE_BATCH_SIZE,
    )
    frame, evidence = fetch_target_snapshot(expected_codes, target_date=target_date)
    _progress(
        "quote_fetch_complete",
        row_count=len(frame),
        coverage_ratio=evidence["coverage_ratio"],
        traded_count=evidence["traded_count"],
        suspended_count=evidence["suspended_count"],
    )
    canonical = canonical_daily_rows(frame.to_dict(orient="records"))
    codes = [row["stock_code"] for row in canonical]
    row_hash = canonical_digest(canonical)
    captured_at = str(evidence["captured_at"])
    batch_id = f"free-tencent-{target_date.replace('-', '')}-{captured_at[11:].replace(':', '')}"
    frame["batch_id"] = batch_id
    frame["data_version"] = row_hash
    existing_counts = _source_counts(
        engine,
        target_date=target_date,
        codes=sorted(set(codes)),
    )
    reused = False
    incremental_codes: list[str] = []
    if existing_counts and set(existing_counts) <= {SOURCE}:
        with engine.connect() as connection:
            existing_rows = load_source_daily_rows(
                connection,
                source=SOURCE,
                target_date=target_date,
                codes=sorted(set(codes)),
            )
        existing_canonical = canonical_daily_rows(existing_rows)
        new_by_code = {row["stock_code"]: row for row in canonical}
        if all(
            new_by_code.get(row["stock_code"]) == row
            for row in existing_canonical
        ):
            existing_codes = {
                row["stock_code"] for row in existing_canonical
            }
            incremental_codes = sorted(set(codes) - existing_codes)
            reused = existing_counts == {SOURCE: len(frame)}
    if reused:
        _progress("database_write_reused", row_count=len(frame))
    elif incremental_codes:
        _progress(
            "database_write_incremental",
            inserted_codes=incremental_codes,
        )
        replace_table_rows_exact_keys(
            frame.loc[frame["stock_code"].isin(incremental_codes)],
            "sm_stock_kline",
            engine,
            key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
            lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
        )
        _progress(
            "database_write_incremental_complete",
            inserted_count=len(incremental_codes),
        )
    else:
        _progress("database_write_start", row_count=len(frame))
        replace_table_rows_exact_keys(
            frame,
            "sm_stock_kline",
            engine,
            key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
            lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
        )
        _progress("database_write_complete", row_count=len(frame))
    unsigned = {
        "schema": VERIFIED_DAILY_RECEIPT_SCHEMA,
        "dataset": FREE_DAILY_DATASET,
        "source": SOURCE,
        "target_date": target_date,
        "captured_at": captured_at,
        "row_count": len(frame),
        "code_count": len(set(codes)),
        "code_set_hash": canonical_digest(sorted(codes)),
        "row_hash": row_hash,
        "coverage_ratio": float(evidence["coverage_ratio"]),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    receipt = {**unsigned, "receipt_id": canonical_digest(unsigned)}
    _progress("database_readback_start", row_count=len(frame))
    counts = _source_counts(
        engine,
        target_date=target_date,
        codes=sorted(set(codes)),
    )
    if counts != {SOURCE: len(frame)}:
        raise FreeDailyBlocked("free daily write readback differs")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO acquisition_partition_state (
                    dataset, source, target_date, partition_key, status,
                    request_id, written_rows, last_attempt_at, last_success_at,
                    next_retry_at, last_error_code, last_error, detail_json,
                    updated_at
                ) VALUES (
                    :dataset, :source, :target_date, :partition_key, 'complete',
                    :request_id, :written_rows, :now_value, :now_value,
                    NULL, NULL, NULL, :detail_json, :now_value
                )
                ON DUPLICATE KEY UPDATE
                    status='complete', request_id=VALUES(request_id),
                    written_rows=VALUES(written_rows),
                    last_success_at=VALUES(last_success_at),
                    next_retry_at=NULL, last_error_code=NULL, last_error=NULL,
                    detail_json=VALUES(detail_json), updated_at=VALUES(updated_at)
                """
            ),
            {
                "dataset": FREE_DAILY_DATASET,
                "source": SOURCE,
                "target_date": target_date,
                "partition_key": FREE_DAILY_PARTITION_KEY,
                "request_id": batch_id[:64],
                "written_rows": len(frame),
                "now_value": now,
                "detail_json": json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    _progress("receipt_written", row_count=len(frame))
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "source": SOURCE,
        "target_date": target_date,
        "batch_id": batch_id,
        "catalog_batch_id": catalog.batch_id,
        "row_count": len(frame),
        "code_count": len(set(codes)),
        "coverage_ratio": float(evidence["coverage_ratio"]),
        "row_hash": row_hash,
        "receipt_id": receipt["receipt_id"],
        "quality_status": "SOURCE_VERIFIED",
        "qmt_attestation_claimed": False,
        "database_writes": True,
    }


def publish_baostock_snapshot(
    engine: Any,
    *,
    target_date: str,
    now: datetime,
) -> dict[str, Any]:
    """Publish Baostock SH/SZ facts with Tencent BJ/gap fallback."""

    catalog = _latest_catalog(engine, target_date=target_date, now=now)
    expected_codes = catalog.eligible_codes(target_date)
    allowed_sources = {BAOSTOCK_SOURCE, SOURCE}
    existing_counts = _source_counts(
        engine,
        target_date=target_date,
        codes=expected_codes,
    )
    foreign = sorted(set(existing_counts) - allowed_sources)
    if foreign:
        raise FreeDailyBlocked(
            "target partition already belongs to another source: "
            + ",".join(foreign)
        )
    _progress(
        "baostock_fetch_start",
        expected_count=len(expected_codes),
    )
    frame, evidence = fetch_baostock_primary_snapshot(
        expected_codes,
        target_date=target_date,
    )
    _progress(
        "baostock_fetch_complete",
        row_count=len(frame),
        coverage_ratio=evidence["coverage_ratio"],
        baostock_count=evidence["baostock_count"],
        tencent_fallback_count=evidence["tencent_fallback_count"],
        traded_count=evidence["traded_count"],
        suspended_count=evidence["suspended_count"],
    )
    canonical = canonical_daily_rows(frame.to_dict(orient="records"))
    codes = [str(row["stock_code"]) for row in canonical]
    row_hash = canonical_digest(canonical)
    captured_at = str(evidence["captured_at"])
    batch_id = (
        f"daily-verified-{target_date.replace('-', '')}-"
        f"{captured_at[11:].replace(':', '')}"
    )
    frame["batch_id"] = batch_id
    frame["data_version"] = row_hash
    expected_counts = {
        str(source): int(count)
        for source, count in frame["data_source"].value_counts().items()
    }
    reused = False
    if existing_counts and set(existing_counts) <= allowed_sources:
        with engine.connect() as connection:
            existing_rows = load_source_daily_rows(
                connection,
                source=MULTI_SOURCE,
                target_date=target_date,
                codes=sorted(set(codes)),
            )
        existing_canonical = canonical_daily_rows(existing_rows)
        new_by_code = {row["stock_code"]: row for row in canonical}
        reused = (
            len(existing_canonical) == len(canonical)
            and all(
                new_by_code.get(row["stock_code"]) == row
                for row in existing_canonical
            )
        )
    if reused:
        _progress("database_write_reused", row_count=len(frame))
    else:
        _progress("database_write_start", row_count=len(frame))
        replace_table_rows_exact_keys(
            frame,
            "sm_stock_kline",
            engine,
            key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
            lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
        )
        _progress("database_write_complete", row_count=len(frame))
    counts = _source_counts(
        engine,
        target_date=target_date,
        codes=sorted(set(codes)),
    )
    if counts != expected_counts:
        raise FreeDailyBlocked(
            "Baostock/Tencent write readback differs: "
            f"expected={expected_counts} actual={counts}"
        )
    receipt = _write_free_receipt(
        engine,
        target_date=target_date,
        source=MULTI_SOURCE,
        captured_at=captured_at,
        row_count=len(frame),
        codes=codes,
        row_hash=row_hash,
        coverage_ratio=float(evidence["coverage_ratio"]),
        catalog=catalog,
        now=now,
        batch_id=batch_id,
    )
    _progress("receipt_written", row_count=len(frame))
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "source": MULTI_SOURCE,
        "target_date": target_date,
        "batch_id": batch_id,
        "catalog_batch_id": catalog.batch_id,
        "row_count": len(frame),
        "code_count": len(set(codes)),
        "coverage_ratio": float(evidence["coverage_ratio"]),
        "baostock_count": evidence["baostock_count"],
        "tencent_fallback_count": evidence["tencent_fallback_count"],
        "suspended_count": evidence["suspended_count"],
        "row_hash": row_hash,
        "receipt_id": receipt["receipt_id"],
        "quality_status": "SOURCE_VERIFIED",
        "qmt_attestation_claimed": False,
        "database_writes": True,
    }


def publish_verified_history(
    engine: Any,
    *,
    target_date: str,
    now: datetime,
) -> dict[str, Any]:
    """Convert retained rows into a Tencent-verified partition.

    Historical QMT rows have no completed attestation after the vendor policy
    change.  Their OHLCV is independently checked against Tencent's raw day
    series; retained amount/turnover fields are preserved and the whole
    partition is relabelled as independently verified rather than QMT-attested.
    """

    catalog = _latest_catalog(engine, target_date=target_date, now=now)
    expected_codes = catalog.eligible_codes(target_date)
    _progress("catalog_ready", catalog_batch_id=catalog.batch_id)
    existing_rows = _load_existing_rows(
        engine,
        target_date=target_date,
        codes=expected_codes,
    )
    existing = {
        str(row["stock_code"]).zfill(6): row for row in existing_rows
    }
    _progress("history_check_start", expected_count=len(expected_codes))
    quotes, evidence = fetch_history_day(
        expected_codes,
        target_date=target_date,
    )
    verified: list[dict[str, Any]] = []
    mismatches: list[str] = []
    for quote in quotes.to_dict(orient="records"):
        code = str(quote["stock_code"]).zfill(6)
        retained = existing.get(code)
        if retained is None:
            mismatches.append(f"{code}:missing_retained_bar")
            continue
        try:
            quote_values = [
                float(quote[key]) for key in ("open", "close", "high", "low")
            ]
            retained_values = [
                float(retained[key]) for key in ("open", "close", "high", "low")
            ]
            volume_difference = abs(
                float(quote["volume"]) - float(retained["volume"])
            )
        except (TypeError, ValueError):
            mismatches.append(f"{code}:invalid_numeric")
            continue
        if (
            any(
                abs(left - right) > 0.011
                for left, right in zip(quote_values, retained_values)
            )
            or volume_difference > 100
        ):
            mismatches.append(f"{code}:value_mismatch")
            continue
        verified.append(
            {
                **retained,
                "stock_code": code,
                "trade_time": f"{target_date} 15:00:00",
                "trade_date": target_date,
                "k_type": 1,
                "adjust_type": 0,
                "open": quote_values[0],
                "close": quote_values[1],
                "high": quote_values[2],
                "low": quote_values[3],
                "volume": int(round(float(quote["volume"]))),
                "data_source": VERIFIED_HISTORY_SOURCE,
                "quality_status": "SOURCE_VERIFIED",
                "permission_status": "SUPPORTED",
                "received_at": now,
                "etl_sync_at": now,
            }
        )
    if mismatches:
        raise FreeDailyBlocked(
            "retained historical rows differ from Tencent: "
            + ",".join(mismatches[:10])
        )
    frame = pd.DataFrame(verified)
    if (
        frame.empty
        or len(frame) != len(existing)
        or len(frame) != len(quotes)
        or float(evidence["coverage_ratio"]) < FREE_HISTORY_MIN_CATALOG_COVERAGE
    ):
        raise FreeDailyBlocked(
            "historical verification did not cover every retained target bar"
        )
    canonical = canonical_daily_rows(frame.to_dict(orient="records"))
    codes = sorted(str(code) for code in frame["stock_code"].unique())
    row_hash = canonical_digest(canonical)
    captured_at = str(evidence["captured_at"])
    batch_id = (
        f"free-tencent-verified-{target_date.replace('-', '')}-"
        f"{captured_at[11:].replace(':', '')}"
    )
    frame["batch_id"] = batch_id
    frame["data_version"] = row_hash
    frame = frame[
        [
            "stock_code", "short_name", "trade_time", "trade_date", "k_type",
            "adjust_type", "open", "close", "high", "low", "volume", "amount",
            "change", "change_pct", "turnover_ratio", "pre_close",
            "data_source", "quality_status", "permission_status", "received_at",
            "etl_sync_at", "batch_id", "data_version",
        ]
    ]
    _progress("database_write_start", row_count=len(frame))
    replace_table_rows_exact_keys(
        frame,
        "sm_stock_kline",
        engine,
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
    )
    _progress("database_write_complete", row_count=len(frame))
    counts = _source_counts(
        engine,
        target_date=target_date,
        codes=codes,
    )
    if counts != {VERIFIED_HISTORY_SOURCE: len(frame)}:
        raise FreeDailyBlocked("verified historical write readback differs")
    unsigned = {
        "schema": VERIFIED_DAILY_RECEIPT_SCHEMA,
        "dataset": FREE_DAILY_DATASET,
        "source": VERIFIED_HISTORY_SOURCE,
        "target_date": target_date,
        "captured_at": captured_at,
        "row_count": len(frame),
        "code_count": len(codes),
        "code_set_hash": canonical_digest(codes),
        "row_hash": row_hash,
        "coverage_ratio": 1.0,
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    receipt = {**unsigned, "receipt_id": canonical_digest(unsigned)}
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO acquisition_partition_state (
                    dataset, source, target_date, partition_key, status,
                    request_id, written_rows, last_attempt_at, last_success_at,
                    next_retry_at, last_error_code, last_error, detail_json,
                    updated_at
                ) VALUES (
                    :dataset, :source, :target_date, :partition_key, 'complete',
                    :request_id, :written_rows, :now_value, :now_value,
                    NULL, NULL, NULL, :detail_json, :now_value
                )
                ON DUPLICATE KEY UPDATE
                    status='complete', request_id=VALUES(request_id),
                    written_rows=VALUES(written_rows),
                    last_success_at=VALUES(last_success_at),
                    next_retry_at=NULL, last_error_code=NULL, last_error=NULL,
                    detail_json=VALUES(detail_json), updated_at=VALUES(updated_at)
                """
            ),
            {
                "dataset": FREE_DAILY_DATASET,
                "source": VERIFIED_HISTORY_SOURCE,
                "target_date": target_date,
                "partition_key": FREE_DAILY_PARTITION_KEY,
                "request_id": batch_id[:64],
                "written_rows": len(frame),
                "now_value": now,
                "detail_json": json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    _progress("receipt_written", row_count=len(frame))
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "source": VERIFIED_HISTORY_SOURCE,
        "target_date": target_date,
        "batch_id": batch_id,
        "catalog_batch_id": catalog.batch_id,
        "row_count": len(frame),
        "code_count": len(codes),
        "coverage_ratio": 1.0,
        "row_hash": row_hash,
        "receipt_id": receipt["receipt_id"],
        "quality_status": "SOURCE_VERIFIED",
        "qmt_attestation_claimed": False,
        "database_writes": True,
    }


def publish_sina_history(
    engine: Any,
    *,
    target_date: str,
    now: datetime,
) -> dict[str, Any]:
    catalog = _latest_catalog(engine, target_date=target_date, now=now)
    expected_codes = catalog.eligible_codes(target_date)
    existing = _source_counts(
        engine,
        target_date=target_date,
        codes=expected_codes,
    )
    if existing == {SINA_HISTORY_SOURCE: sum(existing.values())}:
        with engine.connect() as connection:
            existing_rows = load_source_daily_rows(
                connection,
                source=SINA_HISTORY_SOURCE,
                target_date=target_date,
                codes=expected_codes,
            )
        canonical = canonical_daily_rows(existing_rows)
        codes = sorted(str(row["stock_code"]) for row in canonical)
        coverage = len(codes) / max(len(expected_codes), 1)
        captured_at = now.strftime("%Y-%m-%d %H:%M:%S")
        batch_id = (
            f"free-sina-{target_date.replace('-', '')}-"
            f"{captured_at[11:].replace(':', '')}"
        )
        receipt = _write_free_receipt(
            engine,
            target_date=target_date,
            source=SINA_HISTORY_SOURCE,
            captured_at=captured_at,
            row_count=len(codes),
            codes=codes,
            row_hash=canonical_digest(canonical),
            coverage_ratio=coverage,
            catalog=catalog,
            now=now,
            batch_id=batch_id,
        )
        _progress("receipt_resealed", row_count=len(codes))
        return {
            "schema": RESULT_SCHEMA,
            "status": "COMPLETE",
            "source": SINA_HISTORY_SOURCE,
            "target_date": target_date,
            "batch_id": batch_id,
            "catalog_batch_id": catalog.batch_id,
            "row_count": len(codes),
            "code_count": len(codes),
            "coverage_ratio": coverage,
            "row_hash": canonical_digest(canonical),
            "receipt_id": receipt["receipt_id"],
            "quality_status": "SOURCE_VERIFIED",
            "qmt_attestation_claimed": False,
            "database_writes": True,
        }
    if existing:
        raise FreeDailyBlocked(
            "Sina history backfill requires an empty target partition"
        )
    _progress("sina_history_start", expected_count=len(expected_codes))
    frame, evidence = fetch_sina_history_day(
        expected_codes,
        target_date=target_date,
    )
    _progress(
        "sina_history_complete",
        row_count=len(frame),
        coverage_ratio=evidence["coverage_ratio"],
    )
    frame = frame.copy()
    numeric = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pre_close",
        "change",
        "change_pct",
        "turnover_ratio",
    )
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    missing_pre_close = frame["pre_close"].isna()
    pct_base = 1 + frame["change_pct"] / 100
    frame.loc[missing_pre_close & pct_base.gt(0), "pre_close"] = (
        frame.loc[missing_pre_close & pct_base.gt(0), "close"]
        / pct_base.loc[missing_pre_close & pct_base.gt(0)]
    )
    valid = (
        frame[["open", "high", "low", "close", "pre_close", "volume", "amount"]]
        .notna()
        .all(axis=1)
        & frame[["open", "high", "low", "close", "pre_close"]].gt(0).all(axis=1)
        & frame["high"].ge(frame[["open", "close"]].max(axis=1))
        & frame["low"].le(frame[["open", "close"]].min(axis=1))
        & frame["volume"].ge(0)
        & frame["amount"].ge(0)
    )
    invalid_count = int((~valid).sum())
    if invalid_count:
        _progress("sina_history_invalid_rows_dropped", count=invalid_count)
    frame = frame.loc[valid].copy()
    coverage = len(frame) / max(len(expected_codes), 1)
    if coverage < FREE_HISTORY_MIN_CATALOG_COVERAGE:
        raise FreeDailyBlocked(
            "validated Sina history coverage below threshold: "
            f"{len(frame)}/{len(expected_codes)} ({coverage:.2%})"
        )
    evidence["coverage_ratio"] = coverage
    frame["data_source"] = SINA_HISTORY_SOURCE
    frame["quality_status"] = "SOURCE_VERIFIED"
    frame["permission_status"] = "SUPPORTED"
    frame["received_at"] = now
    frame["etl_sync_at"] = now
    canonical = canonical_daily_rows(frame.to_dict(orient="records"))
    codes = sorted(str(code) for code in frame["stock_code"].unique())
    row_hash = canonical_digest(canonical)
    captured_at = str(evidence["captured_at"])
    batch_id = (
        f"free-sina-{target_date.replace('-', '')}-"
        f"{captured_at[11:].replace(':', '')}"
    )
    frame["batch_id"] = batch_id
    frame["data_version"] = row_hash
    _progress("database_write_start", row_count=len(frame))
    replace_table_rows_exact_keys(
        frame,
        "sm_stock_kline",
        engine,
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
    )
    _progress("database_write_complete", row_count=len(frame))
    counts = _source_counts(
        engine,
        target_date=target_date,
        codes=codes,
    )
    if counts != {SINA_HISTORY_SOURCE: len(frame)}:
        raise FreeDailyBlocked("Sina historical write readback differs")
    unsigned = {
        "schema": VERIFIED_DAILY_RECEIPT_SCHEMA,
        "dataset": FREE_DAILY_DATASET,
        "source": SINA_HISTORY_SOURCE,
        "target_date": target_date,
        "captured_at": captured_at,
        "row_count": len(frame),
        "code_count": len(codes),
        "code_set_hash": canonical_digest(codes),
        "row_hash": row_hash,
        "coverage_ratio": float(evidence["coverage_ratio"]),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    receipt = {**unsigned, "receipt_id": canonical_digest(unsigned)}
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO acquisition_partition_state (
                    dataset, source, target_date, partition_key, status,
                    request_id, written_rows, last_attempt_at, last_success_at,
                    next_retry_at, last_error_code, last_error, detail_json,
                    updated_at
                ) VALUES (
                    :dataset, :source, :target_date, :partition_key, 'complete',
                    :request_id, :written_rows, :now_value, :now_value,
                    NULL, NULL, NULL, :detail_json, :now_value
                )
                ON DUPLICATE KEY UPDATE
                    status='complete', request_id=VALUES(request_id),
                    written_rows=VALUES(written_rows),
                    last_success_at=VALUES(last_success_at),
                    next_retry_at=NULL, last_error_code=NULL, last_error=NULL,
                    detail_json=VALUES(detail_json), updated_at=VALUES(updated_at)
                """
            ),
            {
                "dataset": FREE_DAILY_DATASET,
                "source": SINA_HISTORY_SOURCE,
                "target_date": target_date,
                "partition_key": FREE_DAILY_PARTITION_KEY,
                "request_id": batch_id[:64],
                "written_rows": len(frame),
                "now_value": now,
                "detail_json": json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    _progress("receipt_written", row_count=len(frame))
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "source": SINA_HISTORY_SOURCE,
        "target_date": target_date,
        "batch_id": batch_id,
        "catalog_batch_id": catalog.batch_id,
        "row_count": len(frame),
        "code_count": len(codes),
        "coverage_ratio": float(evidence["coverage_ratio"]),
        "row_hash": row_hash,
        "receipt_id": receipt["receipt_id"],
        "quality_status": "SOURCE_VERIFIED",
        "qmt_attestation_claimed": False,
        "database_writes": True,
    }


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default="latest",
        help="closed trade date YYYY-MM-DD, default latest calendar session",
    )
    parser.add_argument(
        "--verify-history",
        action="store_true",
        help="cross-check retained bars against Tencent when QMT has no receipt",
    )
    parser.add_argument(
        "--backfill-sina",
        action="store_true",
        help="backfill an empty historical partition from Sina with amount/turnover",
    )
    parser.add_argument(
        "--tencent-only",
        action="store_true",
        help="emergency mode: use Tencent without Baostock verification",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    engine = create_batch_engine()
    history = get_kline_engine()
    now = _now()
    target = (
        _latest_closed_session(engine, now)
        if args.date == "latest"
        else _date(args.date)
    )
    try:
        advertised_source = (
            SINA_HISTORY_SOURCE
            if args.backfill_sina
            else (
                VERIFIED_HISTORY_SOURCE
                if args.verify_history
                else (SOURCE if args.tencent_only else MULTI_SOURCE)
            )
        )
        _progress(
            "start",
            target_date=target,
            source=advertised_source,
            expected_min_coverage=FREE_DAILY_MIN_COVERAGE,
        )
        if sum(
            bool(value)
            for value in (
                args.verify_history,
                args.backfill_sina,
                args.tencent_only,
            )
        ) > 1:
            parser.error(
                "--verify-history, --backfill-sina and --tencent-only are mutually exclusive"
            )
        if args.backfill_sina:
            publisher = publish_sina_history
        elif args.verify_history:
            publisher = publish_verified_history
        elif args.tencent_only:
            publisher = publish_snapshot
        else:
            publisher = publish_baostock_snapshot
        result = publisher(history, target_date=target, now=now)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        result = {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "source": SOURCE,
            "target_date": target,
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "database_writes": False,
            "qmt_attestation_claimed": False,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 2
    finally:
        engine.dispose()
        history.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
