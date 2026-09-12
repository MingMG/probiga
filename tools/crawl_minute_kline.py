#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分钟数据爬取脚本
================
从 push2delay 获取正式股票目录的分钟K线和分钟资金流向。

用法:
  python tools/crawl_minute_kline.py --type stock    # 个股分钟K线
  python tools/crawl_minute_kline.py --type flow     # 分钟资金流向
  python tools/crawl_minute_kline.py --type stock --trade-date 2026-09-11
"""

import argparse
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import date, datetime, timedelta, time as wall_time
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, quote_identifier, routed_read_engine, write_frame
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from server.common.qmt_stock_catalog import load_target_stock_catalog
from server.common.qmt_history_coverage import load_minute_native_no_trade_evidence, _native_no_trade_codes
from server.common.qmt_daily_market_truth import QMT_DAILY_PROVIDER
from server.common.mysql_lock import (
    STOCK_MINUTE_FREEZE_LOCK_NAME,
    mysql_named_lock,
    supersede_overlapping_qmt_minute_forward_receipts,
)


def _is_trade_day(engine, day: datetime | None = None) -> bool:
    day = day or datetime.now()
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM si_trade_calendar
                    WHERE trade_date = :d
                      AND trade_status = 1
                    """
                ),
                {"d": day.strftime("%Y-%m-%d")},
            ).scalar()
        return bool(count)
    except Exception as exc:
        raise RuntimeError("authoritative trade calendar unavailable") from exc


def is_trading_time(engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trade_day(engine, now):
        return False
    current = now.hour * 100 + now.minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return int(default)


DELAY = _env_float("MINUTE_REQUEST_DELAY", "0.5")
JITTER = _env_float("MINUTE_REQUEST_JITTER", "0.3")
BATCH_EVERY = _env_int("MINUTE_BATCH_EVERY", "100")
BATCH_PAUSE = _env_float("MINUTE_BATCH_PAUSE", "20")
FETCH_ATTEMPTS = _env_int("MINUTE_FETCH_ATTEMPTS", "3")
RETRY_DELAY = _env_float("MINUTE_RETRY_DELAY", "1.0")
# Each lane retains its own request and batch pacing. The full universe must
# finish within the scheduler's 45-minute collection budget.
FETCH_WORKERS = 6

FLOW_TABLE = "sm_stock_capital_flow_min"
FLOW_WRITE_COLUMNS = (
    "stock_code",
    "trade_time",
    "main_net_inflow",
    "max_net_inflow",
    "lg_net_inflow",
    "mid_net_inflow",
    "sm_net_inflow",
    "snapshot_at",
    "etl_sync_at",
    "source_time",
    "received_at",
    "data_source",
)
RESULT_SCHEMA = "probiga.public-minute-collection.v1"
PUBLIC_MINUTE_TASKS = {
    "intraday_minute_kline": "stock", "intraday_minute_flow": "flow",
    "stock_minute": "stock", "stock_minute_flow": "flow",
}
DATASET_TABLES = {"stock": "sm_stock_minute", "flow": FLOW_TABLE}
VALUE_FIELDS = {
    "stock": ("price", "avg_price", "change", "change_pct", "volume", "amount"),
    "flow": ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"),
}
MINUTE_GRID = tuple(
    (datetime(2000, 1, 1, hour, minute) + timedelta(minutes=offset)).strftime("%H:%M")
    for hour, minute in ((9, 31), (13, 1)) for offset in range(120)
)
RESULT_REPLAY_BUDGET = 24000
SOURCE_DELAY_BUDGET_SECONDS = 180
PUBLIC_MINUTE_SOURCE = "east_push2delay"


class MinuteSourceError(ValueError):
    """One symbol is unproven; never retry it against another exchange."""


def _now() -> datetime:
    return datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0)


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _number(value, *, positive=False, nonnegative=False) -> Decimal:
    try:
        if value is None or isinstance(value, bool) or str(value).strip() in {"", "-"}:
            raise ValueError()
        result = Decimal(str(value))
        if not result.is_finite() or abs(result) >= Decimal("1e20") or result.as_tuple().exponent < -12:
            raise ValueError()
        if positive and result <= 0 or nonnegative and result < 0:
            raise ValueError()
        return result
    except (InvalidOperation, TypeError, ValueError):
        raise MinuteSourceError("INVALID_VALUE") from None


def _canonical_row(row, kind):
    values = []
    for field in VALUE_FIELDS[kind]:
        value = row.get(field)
        if kind == "stock" and field == "avg_price" and value is None:
            values.append(None)
            continue
        number = _number(value, positive=field in {"price", "avg_price"},
                         nonnegative=field in {"volume", "amount"})
        # Existing target columns are DECIMAL(50,6). Reject lossy publication;
        # never round an unproven source value into the persisted digest.
        if number != number.quantize(Decimal("0.000001")):
            raise MinuteSourceError("INVALID_PERSISTED_PRECISION")
        values.append(str(number.normalize()) if number else "0")
    stamp = datetime.fromisoformat(str(row["trade_time"]))
    return [str(row["stock_code"]), stamp.strftime("%Y-%m-%d %H:%M"), *values]


def _code_rows_digest(rows, kind):
    return _digest([_canonical_row(row, kind) for row in rows])

def _new_minute_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Referer": "https://quote.eastmoney.com/",
    })
    session.trust_env = False
    session.verify = True
    return session


def _fetch_native_minutes(code, market, *, session, trade_date, kind):
    if type(market) is not int or market not in {0, 1, 90}:
        raise MinuteSourceError("INVALID_MARKET")
    endpoint = "kline" if kind == "stock" else "fflow/kline"
    params = {
        "secid": f"{market}.{code}", "klt": "1", "fqt": "1",
        "fields1": "f1,f2,f3,f4,f5,f6,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "lmt": "300", "end": date.fromisoformat(trade_date).strftime("%Y%m%d"),
    }
    try:
        response = session.get(
            f"https://push2delay.eastmoney.com/api/qt/stock/{endpoint}/get",
            params=params, timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None
    try:
        payload = response.json()
    except ValueError:
        raise MinuteSourceError("INVALID_PAYLOAD") from None
    if not isinstance(payload, dict):
        raise MinuteSourceError("INVALID_PAYLOAD")
    data = payload.get("data")
    if data is None:
        return None
    if (
        not isinstance(data, dict) or data.get("code") != code
        or type(data.get("market")) is not int or data["market"] != market
    ):
        raise MinuteSourceError("WRONG_IDENTITY")
    rows = data.get("klines")
    if rows is None:
        return None
    if not isinstance(rows, list) or len(rows) > 300 or any(
        not isinstance(row, str) or len(row) > 1024 for row in rows
    ):
        raise MinuteSourceError("INVALID_PAYLOAD")
    return rows


def fetch_minute_kline(code: str, market: int, *, session, trade_date: str):
    return _fetch_native_minutes(code, market, session=session, trade_date=trade_date, kind="stock")


def fetch_minute_flow(code: str, market: int, *, session, trade_date: str):
    return _fetch_native_minutes(code, market, session=session, trade_date=trade_date, kind="flow")


def fetch_with_retries(fetcher, code: str, market: int) -> list[str] | None:
    """Retry one stock without turning a transient source miss into stale data."""
    attempts = max(1, int(FETCH_ATTEMPTS))
    for attempt in range(attempts):
        rows = fetcher(code, market)
        if rows:
            return rows
        if attempt < attempts - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


@contextmanager
def _minute_fetch_workers(codes: list[tuple[str, int]], fetcher):
    """Bound HTTP work and result memory; the consumer alone stages/publishes."""
    local = threading.local()
    stop = threading.Event()
    sessions = []
    session_lock = threading.Lock()

    def fetch(code, market):
        if stop.is_set():
            return code, None, _now()
        if not hasattr(local, "session"):
            local.session = _new_minute_session()
            local.count = 0
            with session_lock:
                sessions.append(local.session)
        try:
            rows = fetch_with_retries(
                lambda stock, exchange: fetcher(stock, exchange, session=local.session),
                code, market,
            )
        except MinuteSourceError as exc:
            rows = exc
        # This is the actual completed source request, before lane pacing or
        # the main thread's result queue/stage latency can make an old bar new.
        captured_at = _now()
        local.count += 1
        stop.wait(DELAY + random.uniform(0, JITTER))
        if BATCH_EVERY > 0 and local.count % BATCH_EVERY == 0:
            stop.wait(BATCH_PAUSE + random.uniform(0, 5))
        return code, rows, captured_at

    pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS, thread_name_prefix="minute-http")
    pending = set()
    remaining = iter(codes)

    def results():
        while True:
            while len(pending) < FETCH_WORKERS * 2:
                item = next(remaining, None)
                if item is None:
                    break
                pending.add(pool.submit(fetch, *item))
            if not pending:
                return
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                pending.remove(future)
                yield future.result()

    try:
        yield results()
    finally:
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        for session in sessions:
            session.close()


def _parse_minutes(code, klines, *, trade_date, kind):
    target = date.fromisoformat(trade_date)
    rows = []
    seen = set()
    for line in klines:
        parts = line.split(",")
        if len(parts) < (11 if kind == "stock" else 6):
            raise MinuteSourceError("INVALID_PAYLOAD")
        try:
            stamp = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
        except ValueError:
            raise MinuteSourceError("WRONG_DATE") from None
        if stamp.date() != target:
            continue
        if stamp.strftime("%H:%M") not in MINUTE_GRID or stamp in seen:
            raise MinuteSourceError("INVALID_MINUTE_GRID")
        seen.add(stamp)
        row = {"stock_code": code, "trade_time": stamp}
        if kind == "stock":
            opening, price, high, low = (_number(value, positive=True) for value in parts[1:5])
            if not low <= opening <= high or not low <= price <= high:
                raise MinuteSourceError("INVALID_VALUE")
            _number(parts[7], nonnegative=True)
            _number(parts[10], nonnegative=True)
            row.update({
                "trade_date": trade_date, "price": price, "avg_price": None,
                "volume": _number(parts[5], nonnegative=True) * 100,
                "amount": _number(parts[6], nonnegative=True),
                "change_pct": _number(parts[8]), "change": _number(parts[9]),
            })
        else:
            row.update({
                field: _number(parts[index])
                for field, index in zip(VALUE_FIELDS["flow"], (1, 5, 4, 3, 2))
            })
        rows.append(row)
    return sorted(rows, key=lambda row: row["trade_time"])


def parse_kline(code: str, klines: list[str], *, trade_date: str):
    return _parse_minutes(code, klines, trade_date=trade_date, kind="stock")


def parse_flow(code: str, klines: list[str], *, trade_date: str):
    return _parse_minutes(code, klines, trade_date=trade_date, kind="flow")


def catalog_stock_codes(catalog, codes):
    members = {row["stock_code"]: row["qmt_code"] for row in catalog.members}
    return [
        (code, {"SH": 1, "SZ": 0, "BJ": 0}[members[code].rsplit(".", 1)[1]])
        for code in codes
    ]


def verified_no_trade_codes(engine, catalog, *, trade_date, decision_known_at):
    """Only an independently replayed, catalog-bound native daily proof exempts a code."""
    try:
        with engine.connect() as connection:
            evidence = load_minute_native_no_trade_evidence(
                connection, trade_date=trade_date, decision_known_at=decision_known_at,
            )
        if evidence is None:
            return set(), None
        truth = evidence["daily_truth"]
        codes = _native_no_trade_codes(evidence, context={
            "trade_date": trade_date, "provider": QMT_DAILY_PROVIDER,
            "captured_at": decision_known_at.isoformat(sep=" "),
            "catalog_batch_id": catalog.batch_id, "catalog_manifest_hash": catalog.manifest_hash,
            "calendar_batch_id": truth["calendar_batch_id"],
            "calendar_manifest_hash": truth["calendar_manifest_hash"],
        })
        return codes, {
            "daily_run_id": truth["run_id"], "truth_hash": truth["truth_hash"],
            "proof_sha256": truth["no_row_exception_proof_sha256"],
            "calendar_batch_id": truth["calendar_batch_id"],
            "calendar_manifest_hash": truth["calendar_manifest_hash"],
        }
    except (RuntimeError, ValueError, KeyError, TypeError):
        # Lack of a completed daily proof never grants an exception. Intraday
        # bars may still be collected; any absent symbol stays explicitly missing.
        return set(), None


def _minute_code_column(table: str) -> str:
    return "index_code" if table in ("sm_index_minute", "sm_concept_east_minute") else "stock_code"


def _prepare_kline_frame(rows: list[dict], table: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")
    df["etl_sync_at"] = _now()
    # sm_index_minute / sm_concept_east_minute 用 index_code 列，有 snapshot_at
    if table in ("sm_index_minute", "sm_concept_east_minute"):
        df = df.rename(columns={"stock_code": "index_code"})
        df["snapshot_at"] = datetime.now().replace(microsecond=0)
    return df


def _create_kline_stage(engine, table: str) -> tuple[str, Connection]:
    stage = f"{table}_stage_{uuid.uuid4().hex[:12]}"
    connection = engine.connect()
    try:
        connection.execute(
            text(
                f"CREATE TEMPORARY TABLE {quote_identifier(stage)} "
                f"LIKE {quote_identifier(table)}"
            )
        )
        connection.commit()
        return stage, connection
    except BaseException:
        connection.close()
        raise


def _append_kline_stage(
    connection: Connection,
    stage: str,
    rows: list[dict],
    table: str,
) -> int:
    df = _prepare_kline_frame(rows, table)
    if df.empty:
        return 0
    with connection.begin():
        write_frame(
            df,
            stage,
            connection,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    return int(len(df))


def _publish_kline_stage(
    engine,
    connection: Connection,
    stage: str,
    table: str,
    *,
    receipt_engine=None,
) -> int:
    """Publish only code/date partitions proven present in a complete stage."""

    target = quote_identifier(table)
    staged = quote_identifier(stage)
    code_col = quote_identifier(_minute_code_column(table))
    staged_rows = int(
        connection.execute(text(f"SELECT COUNT(*) FROM {staged}")).scalar() or 0
    )
    column_rows = connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
            "AND EXTRA NOT LIKE '%GENERATED%' AND COLUMN_NAME <> 'id' "
            "ORDER BY ORDINAL_POSITION"
        ),
        {"table_name": table},
    ).fetchall()
    revoke_window = None
    if table == "sm_stock_minute":
        if receipt_engine is None:
            raise RuntimeError(
                "sm_stock_minute crawler publish requires the authority receipt engine"
            )
        revoke_window = connection.execute(
            text(f"SELECT MIN(trade_date), MAX(trade_date) FROM {staged}")
        ).one()
        if revoke_window[0] is None or revoke_window[1] is None:
            raise RuntimeError(
                "sm_stock_minute crawler stage has no receipt revocation window"
            )
    connection.commit()
    if staged_rows <= 0:
        raise RuntimeError(f"{table} minute stage is empty; preserving previous rows")
    columns = [quote_identifier(str(row[0])) for row in column_rows]
    if not columns:
        raise RuntimeError(f"{table} has no publishable columns")
    column_list = ", ".join(columns)

    with mysql_named_lock(
        engine,
        (
            STOCK_MINUTE_FREEZE_LOCK_NAME
            if table == "sm_stock_minute"
            else f"probiga:{table}"
        ),
        timeout_seconds=max(0, _env_int("MINUTE_PUBLISH_LOCK_TIMEOUT", "0")),
        connection=connection,
    ):
        connection.commit()
        if revoke_window is not None:
            supersede_overlapping_qmt_minute_forward_receipts(
                receipt_engine,
                first_trade_time=(
                    pd.Timestamp(revoke_window[0]).normalize().to_pydatetime()
                ),
                last_trade_time=(
                    pd.Timestamp(revoke_window[1]).normalize()
                    + pd.Timedelta(days=1)
                    - pd.Timedelta(microseconds=1)
                ).to_pydatetime(),
                reason="public stock-minute crawler publish",
            )
        with connection.begin():
            connection.execute(
                text(
                    f"DELETE target_rows FROM {target} AS target_rows "
                    f"INNER JOIN (SELECT DISTINCT {code_col}, trade_date FROM {staged}) AS scope_rows "
                    f"ON target_rows.{code_col} = scope_rows.{code_col} "
                    "AND target_rows.trade_date = scope_rows.trade_date"
                )
            )
            result = connection.execute(
                text(
                    f"INSERT INTO {target} ({column_list}) "
                    f"SELECT {column_list} FROM {staged}"
                )
            )
            if (
                result.rowcount is not None
                and result.rowcount >= 0
                and int(result.rowcount) != staged_rows
            ):
                raise RuntimeError(
                    f"{table} minute publish mismatch: "
                    f"expected={staged_rows} actual={result.rowcount}"
                )
    return staged_rows


def _drop_kline_stage(connection: Connection) -> None:
    connection.close()


def save_kline(
    engine,
    rows: list[dict],
    table: str,
    *,
    receipt_engine=None,
) -> int:
    """Atomically replace the exact code/date partitions represented by rows."""

    if not rows:
        return 0
    stage, connection = _create_kline_stage(engine, table)
    try:
        _append_kline_stage(connection, stage, rows, table)
        return _publish_kline_stage(
            engine,
            connection,
            stage,
            table,
            receipt_engine=receipt_engine,
        )
    finally:
        _drop_kline_stage(connection)


def _create_flow_stage(engine) -> tuple[str, Connection]:
    stage = f"{FLOW_TABLE}_stage_{uuid.uuid4().hex[:12]}"
    connection = engine.connect()
    try:
        connection.execute(
            text(
                f"CREATE TEMPORARY TABLE {quote_identifier(stage)} "
                f"LIKE {quote_identifier(FLOW_TABLE)}"
            )
        )
        connection.commit()
        return stage, connection
    except BaseException:
        connection.close()
        raise


def _drop_flow_stage(connection: Connection) -> None:
    connection.close()


def _append_flow_stage(connection: Connection, stage: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")
    now = _now()
    df["snapshot_at"] = now
    df["etl_sync_at"] = now
    with connection.begin():
        write_frame(
            df[list(FLOW_WRITE_COLUMNS)],
            stage,
            connection,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    return len(df)


def _publish_flow_stage(
    engine,
    stage_connection: Connection,
    stage: str,
    trade_date: str,
) -> int:
    """Replace only proven code/day partitions; retain missing codes' old rows."""
    columns = ", ".join(quote_identifier(column) for column in FLOW_WRITE_COLUMNS)
    lock_timeout = max(0, _env_int("FLOW_MINUTE_LOCK_TIMEOUT", "0"))
    with mysql_named_lock(
        engine,
        "probiga:capital_flow_minute",
        timeout_seconds=lock_timeout,
    ):
        with stage_connection.begin():
            stage_connection.execute(
                text(
                    f"DELETE target_rows FROM {quote_identifier(FLOW_TABLE)} AS target_rows "
                    f"INNER JOIN (SELECT DISTINCT stock_code FROM {quote_identifier(stage)}) AS scope_rows "
                    "ON target_rows.stock_code=scope_rows.stock_code "
                    "WHERE target_rows.trade_time >= :trade_date "
                    "AND target_rows.trade_time < DATE_ADD(:trade_date, INTERVAL 1 DAY)"
                ),
                {"trade_date": trade_date},
            )
            result = stage_connection.execute(
                text(
                    f"INSERT INTO {quote_identifier(FLOW_TABLE)} ({columns}) "
                    f"SELECT {columns} FROM {quote_identifier(stage)}"
                )
            )
    return int(result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0)


def _codes_csv(codes):
    return ",".join(sorted(codes))


def _parse_codes_csv(value):
    if not isinstance(value, str) or len(value) > 70000:
        raise ValueError("invalid code set")
    codes = value.split(",") if value else []
    if codes != sorted(set(codes)) or any(re.fullmatch(r"[0-9]{6}", code) is None for code in codes):
        raise ValueError("invalid code set")
    return codes


def _seal_result(result):
    result = dict(result)
    result.pop("receipt_sha256", None)
    result["receipt_sha256"] = _digest(result)
    return result


def _result_json(result):
    return json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def validate_result(result):
    """Validate the bounded terminal receipt, before any catalog or DB replay."""
    if not isinstance(result, dict) or len(_result_json(result).encode()) > RESULT_REPLAY_BUDGET:
        raise ValueError("receipt budget exceeded")
    unsigned = dict(result)
    proof = unsigned.pop("receipt_sha256", None)
    if proof != _digest(unsigned):
        raise ValueError("receipt digest mismatch")
    kind = result.get("dataset")
    if (result.get("schema") != RESULT_SCHEMA or kind not in DATASET_TABLES
            or result.get("table") != DATASET_TABLES[kind]
            or result.get("provider") != "eastmoney.push2delay" or result.get("status") != "written"
            or type(result.get("source_delay_budget_seconds")) is not int
            or result["source_delay_budget_seconds"] != SOURCE_DELAY_BUDGET_SECONDS):
        raise ValueError("invalid collection identity")
    for key in ("expected_count", "collected_count", "no_trade_count", "missing_count", "written_rows", "staged_rows"):
        if type(result.get(key)) is not int or result[key] < 0:
            raise ValueError("invalid count")
    expected, collected = result["expected_count"], result["collected_count"]
    limit = result.get("requested_limit")
    if type(limit) is not int or limit < 0:
        raise ValueError("invalid requested limit")
    missing = _parse_codes_csv(result.get("missing_codes_csv"))
    no_trade = _parse_codes_csv(result.get("no_trade_codes_csv"))
    if (not 0 < collected <= expected <= 10000
            or collected + len(missing) + len(no_trade) != expected
            or (limit > 0 and collected + len(no_trade) > limit)
            or len(missing) != result["missing_count"] or len(no_trade) != result["no_trade_count"]
            or set(missing) & set(no_trade)
            or _digest(missing) != result.get("missing_codes_sha256")
            or _digest(no_trade) != result.get("no_trade_codes_sha256")
            or not collected <= result["written_rows"] <= 240 * collected
            or result["staged_rows"] != result["written_rows"]):
        raise ValueError("invalid collection counts")
    coverage, minimum = result.get("coverage"), result.get("min_coverage")
    if (type(coverage) not in (int, float) or type(minimum) not in (int, float)
            or not 0 < minimum <= coverage <= 1
            or abs(coverage - (collected + len(no_trade)) / expected) > 1e-12
            or result.get("acquisition_status") != ("PARTIAL" if missing else "COMPLETE")):
        raise ValueError("invalid coverage")
    for key in ("catalog_manifest_hash", "expected_codes_sha256", "business_rows_sha256"):
        if not isinstance(result.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", result[key]):
            raise ValueError("invalid digest")
    for key, length in (("build_sha", 40), ("run_uid", 32)):
        if not re.fullmatch("[0-9a-f]{%d}" % length, str(result.get(key))) or result[key] == "0" * length:
            raise ValueError("invalid execution identity")
    target = date.fromisoformat(result["trade_date"])
    decision = datetime.fromisoformat(result["decision_known_at"])
    started = datetime.fromisoformat(result["started_at"])
    finished = datetime.fromisoformat(result["finished_at"])
    captured = datetime.fromisoformat(result["catalog_captured_at"])
    if (any(stamp.tzinfo is not None for stamp in (decision, started, finished, captured))
            or not captured <= decision == started <= finished
            or target > started.date() or (finished - started).total_seconds() > 2700
            or not isinstance(result.get("catalog_batch_id"), str)
            or not 0 < len(result["catalog_batch_id"]) <= 64):
        raise ValueError("invalid execution time")
    if result.get("requested_trade_date") not in (None, target.isoformat()):
        raise ValueError("invalid requested date")
    return "degraded" if missing else "success"


def _required_grid(trade_date, captured_at):
    target = date.fromisoformat(trade_date)
    if captured_at.tzinfo is not None or target > captured_at.date():
        raise MinuteSourceError("INVALID_CAPTURE_TIME")
    if target < captured_at.date() or captured_at.time() >= wall_time(15):
        return MINUTE_GRID
    cutoff = captured_at - timedelta(seconds=SOURCE_DELAY_BUDGET_SECONDS)
    return tuple(minute for minute in MINUTE_GRID
                 if datetime.fromisoformat(f"{trade_date} {minute}") <= cutoff)


def _validate_grid(rows, *, trade_date, captured_at):
    if not rows:
        return
    actual = [row["trade_time"].strftime("%H:%M") for row in rows]
    required = _required_grid(trade_date, captured_at)
    if actual != list(MINUTE_GRID[:len(actual)]):
        raise MinuteSourceError("INCOMPLETE_MINUTE_GRID")
    if rows[-1]["trade_time"] > captured_at:
        raise MinuteSourceError("FUTURE_MINUTE")
    if len(actual) < len(required):
        raise MinuteSourceError("STALE_SOURCE_MINUTE")


def replay_result(result, engine, *, started_at, now):
    """Independently replay the immutable directory and exact split-DB values."""
    disposition = validate_result(result)
    started = datetime.fromisoformat(result["started_at"])
    finished = datetime.fromisoformat(result["finished_at"])
    if not started_at.replace(microsecond=0) <= started <= finished <= now:
        raise ValueError("receipt is outside this execution")
    target = result["trade_date"]
    catalog, expected = load_target_stock_catalog(
        engine, target_date=target, decision_known_at=started,
        batch_id=result["catalog_batch_id"],
    )
    expected = sorted(expected)
    if (catalog.batch_id != result["catalog_batch_id"]
            or catalog.manifest_hash != result["catalog_manifest_hash"]
            or catalog.captured_at != result["catalog_captured_at"]
            or len(expected) != result["expected_count"]
            or _digest(expected) != result["expected_codes_sha256"]):
        raise ValueError("frozen catalog differs")
    if not _is_trade_day(engine, date.fromisoformat(target)):
        raise ValueError("target is not a trading session")
    missing = set(_parse_codes_csv(result["missing_codes_csv"]))
    no_trade = set(_parse_codes_csv(result["no_trade_codes_csv"]))
    if not missing | no_trade <= set(expected):
        raise ValueError("receipt contains foreign symbols")
    native_codes, native_ref = verified_no_trade_codes(
        engine, catalog, trade_date=target, decision_known_at=started,
    )
    if native_ref != result.get("native_no_trade_evidence") or not no_trade <= native_codes:
        raise ValueError("native no-trade evidence differs")
    collected = set(expected) - missing - no_trade
    selected = set(expected[:result["requested_limit"]]) if result["requested_limit"] else set(expected)
    if not collected | no_trade <= selected:
        raise ValueError("persisted universe exceeds requested limit")
    if collected & native_codes:
        raise ValueError("native no-trade evidence contradicts collected bars")
    kind, table = result["dataset"], result["table"]
    fields = ", ".join(quote_identifier(field) for field in VALUE_FIELDS[kind])
    date_column = ", trade_date" if kind == "stock" else ""
    query = text(
        f"SELECT stock_code, trade_time, {fields}, etl_sync_at, source_time, received_at, data_source{date_column} "
        f"FROM {quote_identifier(table)} "
        "WHERE trade_time >= :day AND trade_time < :next_day AND stock_code IN :codes "
        "ORDER BY stock_code, trade_time LIMIT :row_limit"
    ).bindparams(bindparam("codes", expanding=True))
    proofs, rows, current_code, count = [], [], None, 0
    current_capture = None

    def finish_code():
        if rows:
            _validate_grid(rows, trade_date=target, captured_at=current_capture)
            proofs.append([current_code, len(rows), _code_rows_digest(rows, kind),
                           current_capture.isoformat(sep=" ")])
            rows.clear()

    with routed_read_engine(query, engine).connect() as connection:
        stream = connection.execution_options(stream_results=True).execute(query, {
            "day": target, "next_day": (date.fromisoformat(target) + timedelta(days=1)).isoformat(),
            "codes": sorted(collected | no_trade), "row_limit": result["written_rows"] + 1,
        }).mappings()
        for row in stream:
            code = str(row["stock_code"])
            stamp = datetime.fromisoformat(str(row["trade_time"]))
            synced = datetime.fromisoformat(str(row["etl_sync_at"]))
            captured = datetime.fromisoformat(str(row["received_at"]))
            source_time = datetime.fromisoformat(str(row["source_time"]))
            if (code not in collected or not started <= captured <= synced <= finished
                    or source_time != stamp or row["data_source"] != PUBLIC_MINUTE_SOURCE
                    or stamp.date().isoformat() != target or stamp.second or stamp.microsecond
                    or stamp > finished
                    or (kind == "stock" and str(row["trade_date"]) != target)):
                raise ValueError("persisted identity or freshness differs")
            if code != current_code:
                finish_code()
                current_code = code
                current_capture = captured
            elif captured != current_capture:
                raise ValueError("persisted source capture identity differs")
            rows.append({**row, "trade_time": stamp})
            count += 1
            if count > result["written_rows"] or len(rows) > 240:
                raise ValueError("persisted row count differs")
        finish_code()
    if (count != result["written_rows"] or {proof[0] for proof in proofs} != collected
            or _digest(proofs) != result["business_rows_sha256"]):
        raise ValueError("persisted business values differ")
    return disposition


def _collect_minutes(engine, codes, *, kind, table, trade_date, min_coverage,
                     context, no_trade_codes, limit=0, receipt_engine=None):
    expected = sorted(code for code, _market in codes)
    if expected != sorted(set(expected)) or not expected or len(expected) > 10000:
        raise ValueError("invalid target universe")
    if not 0 < min_coverage <= 1:
        raise ValueError("invalid coverage gate")
    selected = codes[:limit] if limit > 0 else codes
    missing = set(expected)
    no_trade, proofs, reasons = set(), {}, {}
    buffer, staged_rows = [], 0
    is_flow = kind == "flow"
    if is_flow:
        stage, connection = _create_flow_stage(engine)
    else:
        stage, connection = _create_kline_stage(engine, table)
    drop = _drop_flow_stage if is_flow else _drop_kline_stage

    def append():
        nonlocal staged_rows
        if not buffer:
            return
        count = (_append_flow_stage(connection, stage, buffer) if is_flow
                 else _append_kline_stage(connection, stage, buffer, table))
        if count != len(buffer):
            raise RuntimeError("stage row count mismatch")
        staged_rows += count
        buffer.clear()

    try:
        fetcher = partial(fetch_minute_flow if is_flow else fetch_minute_kline, trade_date=trade_date)
        parser = parse_flow if is_flow else parse_kline
        with _minute_fetch_workers(selected, fetcher) as fetches:
            visited = set()
            for index, (code, native, captured_at) in enumerate(fetches, 1):
                if code not in missing or code in visited:
                    raise RuntimeError("unexpected or duplicate source identity")
                visited.add(code)
                try:
                    if isinstance(native, MinuteSourceError):
                        raise native
                    rows = parser(code, native or [], trade_date=trade_date)
                    if rows and code in no_trade_codes:
                        raise MinuteSourceError("NO_TRADE_CONTRADICTION")
                    if not datetime.fromisoformat(context["started_at"]) <= captured_at <= _now():
                        raise MinuteSourceError("INVALID_CAPTURE_TIME")
                    _validate_grid(rows, trade_date=trade_date, captured_at=captured_at)
                    if rows:
                        proofs[code] = [code, len(rows), _code_rows_digest(rows, kind), captured_at.isoformat(sep=" ")]
                        for row in rows:
                            row.update(source_time=row["trade_time"], received_at=captured_at,
                                       data_source=PUBLIC_MINUTE_SOURCE)
                        buffer.extend(rows)
                        missing.remove(code)
                    elif code in no_trade_codes:
                        no_trade.add(code)
                        missing.remove(code)
                    else:
                        raise MinuteSourceError("NO_TARGET_BARS")
                except MinuteSourceError as exc:
                    reason = str(exc)
                    reasons[reason] = reasons.get(reason, 0) + 1
                if len(buffer) >= 5000:
                    append()
                if index % 200 == 0:
                    print(f"Minute {kind}: checked={index}/{len(selected)} collected={len(proofs)} missing={len(missing)}", file=sys.stderr, flush=True)
            append()
        coverage = (len(proofs) + len(no_trade)) / len(expected)
        result = {
            **context, "schema": RESULT_SCHEMA, "provider": "eastmoney.push2delay",
            "dataset": kind, "table": table, "trade_date": trade_date,
            "requested_limit": limit,
            "source_delay_budget_seconds": SOURCE_DELAY_BUDGET_SECONDS,
            "status": "written", "acquisition_status": "PARTIAL" if missing else "COMPLETE",
            "expected_count": len(expected), "expected_codes_sha256": _digest(expected),
            "collected_count": len(proofs), "no_trade_count": len(no_trade),
            "missing_count": len(missing), "missing_codes_csv": _codes_csv(missing),
            "missing_codes_sha256": _digest(sorted(missing)),
            "no_trade_codes_csv": _codes_csv(no_trade), "no_trade_codes_sha256": _digest(sorted(no_trade)),
            "coverage": coverage, "min_coverage": min_coverage,
            "staged_rows": staged_rows, "written_rows": staged_rows,
            "business_rows_sha256": _digest([proofs[code] for code in sorted(proofs)]),
            "failure_reason_counts": reasons, "finished_at": _now().isoformat(sep=" "),
        }
        # Budget and the entire prospective success receipt are checked before
        # publication. No truncated output can authorize a successful write.
        if not proofs or coverage < min_coverage:
            result.update(status="coverage_failed", acquisition_status="FAILED", written_rows=0)
            return _seal_result(result)
        validate_result(_seal_result(result))
        published = (_publish_flow_stage(engine, connection, stage, trade_date) if is_flow
                     else _publish_kline_stage(engine, connection, stage, table, receipt_engine=receipt_engine))
        if published != staged_rows:
            raise RuntimeError("published row count mismatch")
        result["finished_at"] = _now().isoformat(sep=" ")
        return _seal_result(result)
    except BaseException:
        try:
            drop(connection)
        except Exception:
            pass
        raise
    finally:
        if sys.exc_info()[0] is None:
            drop(connection)


def crawl_kline(engine, codes, table, label, limit, min_coverage, *, trade_date,
                context, no_trade_codes=frozenset(), receipt_engine=None):
    return _collect_minutes(engine, codes, kind="stock", table=table, trade_date=trade_date,
                            min_coverage=min_coverage, context=context, no_trade_codes=no_trade_codes,
                            limit=limit, receipt_engine=receipt_engine)


def crawl_flow(engine, codes, limit, min_coverage, *, trade_date, context, no_trade_codes=frozenset()):
    return _collect_minutes(engine, codes, kind="flow", table=FLOW_TABLE, trade_date=trade_date,
                            min_coverage=min_coverage, context=context, no_trade_codes=no_trade_codes, limit=limit)


def _build_parser():
    parser = argparse.ArgumentParser(description="股票分钟数据采集；历史重采须明确 --trade-date")
    parser.add_argument("--type", required=True, choices=["stock", "flow"])
    parser.add_argument("--trade-date")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--request-delay", type=float, default=None)
    parser.add_argument("--request-jitter", type=float, default=None)
    parser.add_argument("--batch-every", type=int, default=None)
    parser.add_argument("--batch-pause", type=float, default=None)
    parser.add_argument("--fetch-attempts", type=int, default=None)
    parser.add_argument("--retry-delay", type=float, default=None)
    parser.add_argument("--min-coverage", type=float, default=_env_float("MINUTE_MIN_COVERAGE", "0.70"))
    parser.add_argument("--skip-closed", action="store_true")
    return parser


def main():
    global DELAY, JITTER, BATCH_EVERY, BATCH_PAUSE, FETCH_ATTEMPTS, RETRY_DELAY
    args = _build_parser().parse_args()
    started = _now()
    try:
        task_type = os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE")
        if task_type and PUBLIC_MINUTE_TASKS.get(task_type) != args.type:
            raise ValueError("scheduler task dataset differs")
        for option, variable, lower in (
            (args.request_delay, "DELAY", 0), (args.request_jitter, "JITTER", 0),
            (args.batch_every, "BATCH_EVERY", 0), (args.batch_pause, "BATCH_PAUSE", 0),
            (args.fetch_attempts, "FETCH_ATTEMPTS", 1), (args.retry_delay, "RETRY_DELAY", 0),
        ):
            if option is not None:
                if not 0 <= option <= 300:
                    raise ValueError("invalid request pacing")
                globals()[variable] = max(lower, option)
        engine = create_batch_engine()
        if args.skip_closed and args.trade_date is None and not is_trading_time(engine, started):
            print(_result_json({"status": "skipped", "reason": "market_closed", "now": started.isoformat(sep=" ")}))
            return 0
        target = date.fromisoformat(args.trade_date) if args.trade_date else started.date()
        if target > started.date() or not _is_trade_day(engine, target) or args.limit < 0:
            raise ValueError("invalid target trading date")
        build = str(os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA") or os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").lower()
        run_uid = str(os.environ.get("PROBIGA_SCHEDULER_HISTORY_RUN_UID") or uuid.uuid4().hex)
        if not re.fullmatch(r"[0-9a-f]{40}", build) or build == "0" * 40:
            raise ValueError("build identity unavailable")
        catalog, stock_codes = load_target_stock_catalog(
            engine, target_date=target.isoformat(), decision_known_at=started,
        )
        codes = catalog_stock_codes(catalog, stock_codes)
        no_trade, native_ref = verified_no_trade_codes(
            engine, catalog, trade_date=target.isoformat(), decision_known_at=started,
        )
        context = {
            "started_at": started.isoformat(sep=" "), "decision_known_at": started.isoformat(sep=" "),
            "requested_trade_date": args.trade_date, "build_sha": build, "run_uid": run_uid,
            "catalog_batch_id": catalog.batch_id, "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_captured_at": catalog.captured_at, "native_no_trade_evidence": native_ref,
        }
        if args.type == "stock":
            result = crawl_kline(get_kline_engine(), codes, "sm_stock_minute", "Stock 1-min", args.limit,
                                 args.min_coverage, trade_date=target.isoformat(), context=context,
                                 no_trade_codes=no_trade, receipt_engine=engine)
        else:
            result = crawl_flow(get_minute_engine(), codes, args.limit, args.min_coverage,
                                trade_date=target.isoformat(), context=context, no_trade_codes=no_trade)
        print(_result_json(result), flush=True)
        return 0 if result["status"] == "written" else 2
    except Exception as exc:
        print(_result_json({"schema": RESULT_SCHEMA, "status": "failed", "error": type(exc).__name__,
                            "reason": str(exc) if isinstance(exc, (ValueError, MinuteSourceError)) else "collection failed"}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
