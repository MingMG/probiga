#!/usr/bin/env python3
"""Import a closed-session A-share minute history from Sina's public feed.

This is the explicit public-source fallback used when the QMT host was unable
to retain the full session.  It never relabels the bars as QMT-native data.
Each stock/date partition is replaced only after the complete 238-bar Sina
clock has been parsed, and a durable receipt records source and content hashes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Sequence

import requests
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.config import get_kline_mysql_url  # noqa: E402
from server.common.engine_factory import create_pooled_engine  # noqa: E402
from server.common.minute_data import get_minute_engine  # noqa: E402
from server.common.qmt_stock_catalog import load_target_stock_catalog  # noqa: E402
from tools.crawl_minute_kline import (  # noqa: E402
    _append_kline_stage,
    _create_kline_stage,
    _drop_kline_stage,
    _publish_kline_stage,
)
from tools.env_config import load_project_env  # noqa: E402


SOURCE = "sina_public_1m"
DATA_VERSION = "sina_public_1m_238_v2"
TENCENT_SOURCE = "tencent_minute_verified"
TENCENT_DATA_VERSION = "tencent_minute_verified_238_v1"
RECEIPT_TABLE = "st_qmt_minute_sync_receipt_v2"
SOURCE_URL = (
    "https://quotes.sina.cn/cn/api/json_v2.php/"
    "CN_MarketDataService.getKLineData"
)
TENCENT_SOURCE_URL = (
    "https://ifzq.gtimg.cn/appstock/app/minute/query"
)
TENCENT_KLINE_SOURCE_URL = (
    "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
)
SOURCE_FAILURE_CIRCUIT = 20
MINUTE_GRID = (
    tuple(
        (dt.datetime(2000, 1, 1, 9, 31) + dt.timedelta(minutes=index)).strftime(
            "%H:%M:%S"
        )
        for index in range(120)
    )
    + tuple(
        (dt.datetime(2000, 1, 1, 13, 1) + dt.timedelta(minutes=index)).strftime(
            "%H:%M:%S"
        )
        for index in range(117)
    )
    + ("15:00:00",)
)
GRID_SHA256 = hashlib.sha256("".join(MINUTE_GRID).encode("ascii")).hexdigest()


class ImportErrorBase(RuntimeError):
    """A terminal source or validation failure for one stock."""


def _source_circuit_tripped(consecutive_failures: int) -> bool:
    return consecutive_failures >= SOURCE_FAILURE_CIRCUIT


def _normalize_storage_rows(rows: Sequence[dict]) -> list[dict]:
    quantum = Decimal("0.000001")
    fields = (
        "price",
        "avg_price",
        "change",
        "change_pct",
        "volume",
        "amount",
    )
    normalized: list[dict] = []
    for raw in rows:
        row = dict(raw)
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            number = value if isinstance(value, Decimal) else Decimal(str(value))
            if not number.is_finite():
                raise ImportErrorBase("invalid numeric storage value")
            number = number.quantize(quantum, rounding=ROUND_HALF_UP)
            row[field] = abs(number) if number == 0 else number
        normalized.append(row)
    return normalized


@dataclass(frozen=True)
class DailyAnchor:
    stock_code: str
    qmt_code: str
    pre_close: Decimal
    close: Decimal
    high: Decimal
    low: Decimal
    volume: Decimal | None = None
    amount: Decimal | None = None


def _decimal(
    value: object,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ImportErrorBase("invalid numeric source value") from exc
    if (
        not number.is_finite()
        or (positive and number <= 0)
        or (nonnegative and number < 0)
    ):
        raise ImportErrorBase("invalid numeric source value")
    return number


def _market_suffix(stock_code: str) -> str:
    if stock_code.startswith(("4", "8", "92")):
        return "BJ"
    if stock_code.startswith(("5", "6", "9")):
        return "SH"
    if stock_code.startswith(("0", "1", "2", "3")):
        return "SZ"
    raise ImportErrorBase(f"unsupported stock-code namespace: {stock_code}")


def _validate_anchor_totals(rows: Sequence[dict], anchor: DailyAnchor) -> None:
    if anchor.volume is not None:
        minute_volume = sum((row["volume"] for row in rows), Decimal("0"))
        volume_tolerance = max(
            Decimal("100"),
            abs(anchor.volume) * Decimal("0.000001"),
        )
        if abs(minute_volume - anchor.volume) > volume_tolerance:
            raise ImportErrorBase(
                "minute volume differs from the verified daily anchor"
            )
    if anchor.amount is not None:
        amounts = [row["amount"] for row in rows]
        if all(amount is not None for amount in amounts):
            minute_amount = sum(amounts, Decimal("0"))
            tolerance = max(
                Decimal("1"),
                abs(anchor.amount) * Decimal("0.0001"),
            )
            if abs(minute_amount - anchor.amount) > tolerance:
                raise ImportErrorBase(
                    "minute amount differs from the verified daily anchor"
                )


def _fill_sina_no_trade_minutes(
    rows: Sequence[dict],
    *,
    anchor: DailyAnchor,
    target: dt.date,
    captured_at: dt.datetime,
    batch_id: str,
) -> list[dict]:
    """Prove omitted Sina minutes are no-trade intervals, then fill them."""
    by_clock = {
        row["trade_time"].strftime("%H:%M:%S"): dict(row)
        for row in rows
    }
    previous_price = anchor.pre_close
    filled: list[dict] = []
    for clock in MINUTE_GRID:
        if clock in by_clock:
            row = by_clock[clock]
            previous_price = row["price"]
        else:
            stamp = dt.datetime.combine(
                target,
                dt.time.fromisoformat(clock),
            )
            change = previous_price - anchor.pre_close
            row = {
                "stock_code": anchor.stock_code,
                "trade_time": stamp,
                "trade_date": target,
                "price": previous_price,
                "avg_price": None,
                "change": change,
                "change_pct": (
                    change / anchor.pre_close * Decimal("100")
                ),
                "volume": Decimal("0"),
                "amount": Decimal("0"),
                "etl_sync_at": captured_at,
                "qmt_code": anchor.qmt_code,
                "data_source": SOURCE,
                "source_time": stamp,
                "received_at": captured_at,
                "batch_id": batch_id,
                "data_version": DATA_VERSION,
                "quality_status": "VERIFIED",
                "permission_status": "SUPPORTED",
            }
        filled.append(row)
    _validate_anchor_totals(filled, anchor)
    return filled


def _sina_symbol(stock_code: str) -> str:
    return _market_suffix(stock_code).lower() + stock_code


def parse_sina_session(
    payload: object,
    *,
    anchor: DailyAnchor,
    trade_date: str,
    captured_at: dt.datetime,
    batch_id: str,
) -> list[dict]:
    """Convert one Sina response to the canonical stock-minute row contract."""
    if not isinstance(payload, list):
        raise ImportErrorBase("source payload is not a list")
    target = dt.date.fromisoformat(trade_date)
    rows: list[dict] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise ImportErrorBase("source row is not an object")
        raw_stamp = str(item.get("day") or "").strip()
        try:
            stamp = dt.datetime.strptime(raw_stamp, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ImportErrorBase("source timestamp is invalid") from exc
        if stamp.date() != target:
            continue
        clock = stamp.strftime("%H:%M:%S")
        if clock not in MINUTE_GRID or clock in seen:
            raise ImportErrorBase("source minute grid is invalid")
        seen.add(clock)
        opening = _decimal(item.get("open"), positive=True)
        high = _decimal(item.get("high"), positive=True)
        low = _decimal(item.get("low"), positive=True)
        close = _decimal(item.get("close"), positive=True)
        volume = _decimal(item.get("volume"))
        amount = _decimal(item.get("amount"))
        if not low <= opening <= high or not low <= close <= high:
            raise ImportErrorBase("source OHLC is inconsistent")
        if volume < 0 or amount < 0:
            raise ImportErrorBase("source volume or amount is negative")
        change = close - anchor.pre_close
        change_pct = change / anchor.pre_close * Decimal("100")
        rows.append({
            "stock_code": anchor.stock_code,
            "trade_time": stamp,
            "trade_date": target,
            "price": close,
            "avg_price": None,
            "change": change,
            "change_pct": change_pct,
            "volume": volume,
            "amount": amount,
            "etl_sync_at": captured_at,
            "qmt_code": anchor.qmt_code,
            "data_source": SOURCE,
            "source_time": stamp,
            "received_at": captured_at,
            "batch_id": batch_id,
            "data_version": DATA_VERSION,
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
        })
    ordered = sorted(rows, key=lambda row: row["trade_time"])
    actual_grid = tuple(row["trade_time"].strftime("%H:%M:%S") for row in ordered)
    if actual_grid != MINUTE_GRID:
        return _fill_sina_no_trade_minutes(
            ordered,
            anchor=anchor,
            target=target,
            captured_at=captured_at,
            batch_id=batch_id,
        )
    if (
        ordered[0]["price"] <= 0
        or ordered[-1]["price"] <= 0
        or abs(ordered[-1]["price"] - anchor.close) > Decimal("0.011")
        or max(row["price"] for row in ordered) > anchor.high + Decimal("0.011")
        or min(row["price"] for row in ordered) < anchor.low - Decimal("0.011")
    ):
        raise ImportErrorBase("source bars differ from the verified daily anchor")
    _validate_anchor_totals(ordered, anchor)
    return ordered


def _row_digest(rows: Sequence[dict]) -> str:
    def stable_decimal(value: object) -> str:
        if value is None:
            return "null"
        number = Decimal(str(value))
        number = number.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        if number == 0:
            number = abs(number)
        return format(number, "f")

    encoded = [
        [
            row["stock_code"],
            row["trade_time"].isoformat(sep=" "),
            stable_decimal(row["price"]),
            stable_decimal(row["change"]),
            stable_decimal(row["change_pct"]),
            stable_decimal(row["volume"]),
            stable_decimal(row["amount"]),
        ]
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(encoded, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _proof_digest(proofs: Sequence[Sequence[object]]) -> str:
    return hashlib.sha256(
        json.dumps(
            list(proofs),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _load_anchors(
    engine,
    *,
    trade_date: str,
    stock_codes: Sequence[str],
) -> tuple[dict[str, DailyAnchor], frozenset[str]]:
    anchors: dict[str, DailyAnchor] = {}
    no_trade_codes: set[str] = set()
    statement = text(
        """
        SELECT stock_code, close, high, low, pre_close, volume, amount,
               quality_status, permission_status
        FROM sm_stock_kline
        WHERE trade_date=:trade_date AND k_type=1 AND adjust_type=0
          AND stock_code IN :stock_codes
        """
    ).bindparams(bindparam("stock_codes", expanding=True))
    for offset in range(0, len(stock_codes), 400):
        batch = list(stock_codes[offset:offset + 400])
        with engine.connect() as connection:
            rows = connection.execute(
                statement,
                {"trade_date": trade_date, "stock_codes": batch},
            ).mappings().all()
        for row in rows:
            code = str(row["stock_code"]).zfill(6)
            if code in anchors or code in no_trade_codes:
                continue
            if (
                row["quality_status"] == "SUSPENDED"
                and row["permission_status"] == "NO_TRADE"
            ):
                _decimal(row["pre_close"], positive=True)
                no_trade_codes.add(code)
                continue
            if (
                row["quality_status"] == "SUSPENDED"
                or row["permission_status"] == "NO_TRADE"
            ):
                raise ImportErrorBase(
                    f"inconsistent daily no-trade evidence: {code}"
                )
            anchor = DailyAnchor(
                stock_code=code,
                qmt_code=f"{code}.{_market_suffix(code)}",
                pre_close=_decimal(row["pre_close"], positive=True),
                close=_decimal(row["close"], positive=True),
                high=_decimal(row["high"], positive=True),
                low=_decimal(row["low"], positive=True),
                volume=_decimal(row["volume"], nonnegative=True),
                amount=_decimal(row["amount"], nonnegative=True),
            )
            anchors[code] = anchor
    missing = sorted(set(stock_codes) - set(anchors) - no_trade_codes)
    if missing:
        raise ImportErrorBase(
            "verified daily anchors are missing: " + ",".join(missing[:20])
        )
    return anchors, frozenset(no_trade_codes)


def _fetch_one(
    anchor: DailyAnchor,
    *,
    session: requests.Session,
    trade_date: str,
    batch_id: str,
    timeout: float,
) -> tuple[dict, str]:
    payload, raw = _bounded_json_get(
        session,
        SOURCE_URL,
        params={
            "symbol": _sina_symbol(anchor.stock_code),
            "scale": "1",
            "ma": "no",
            "datalen": "1023",
        },
        timeout=timeout,
    )
    captured_at = dt.datetime.now().replace(microsecond=0)
    rows = parse_sina_session(
        payload,
        anchor=anchor,
        trade_date=trade_date,
        captured_at=captured_at,
        batch_id=batch_id,
    )
    rows = _normalize_storage_rows(rows)
    digest = _row_digest(rows)
    return {
        "stock_code": anchor.stock_code,
        "rows": rows,
        "digest": digest,
        "captured_at": captured_at,
    }, raw


def _cache_path(
    directory: Path | None,
    *,
    source: str,
    stock_code: str,
) -> Path | None:
    if directory is None:
        return None
    return directory / f"{stock_code}.{source}.json.gz"


def _read_cached_payload(
    directory: Path | None,
    *,
    source: str,
    stock_code: str,
) -> tuple[object, dt.datetime] | None:
    path = _cache_path(
        directory,
        source=source,
        stock_code=stock_code,
    )
    if path is None or not path.is_file():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            wrapper = json.load(handle)
        payload = wrapper["payload"]
        captured_at = dt.datetime.fromisoformat(wrapper["captured_at"])
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    if captured_at.tzinfo is not None:
        return None
    return payload, captured_at


def _write_cached_payload(
    directory: Path | None,
    *,
    source: str,
    stock_code: str,
    payload: object,
    captured_at: dt.datetime,
) -> None:
    path = _cache_path(
        directory,
        source=source,
        stock_code=stock_code,
    )
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8") as handle:
        json.dump(
            {
                "source": source,
                "stock_code": stock_code,
                "captured_at": captured_at.isoformat(sep=" "),
                "payload": payload,
            },
            handle,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    temporary.replace(path)
    path.chmod(0o600)


def _new_source_session(proxy: str = "") -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Referer": "https://finance.sina.com.cn/",
    })
    session.trust_env = False
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def _bounded_json_get(
    session: requests.Session,
    url: str,
    *,
    params: dict,
    timeout: float,
    max_bytes: int = 2_000_000,
) -> tuple[object, str]:
    """Read one public JSON response under curl's total request deadline."""
    del session
    query = urllib.parse.urlencode(params)
    target = f"{url}?{query}" if query else url
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        format(timeout, ".3f"),
        "--connect-timeout",
        format(min(5.0, timeout), ".3f"),
        "--max-filesize",
        str(max_bytes),
        "--user-agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "--referer",
        "https://finance.sina.com.cn/",
        target,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=timeout + 2,
        )
    except subprocess.TimeoutExpired as exc:
        raise requests.Timeout(
            "public minute response deadline exceeded"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()
        raise requests.RequestException(
            f"public minute request failed: {detail[:300]}"
        )
    raw = completed.stdout
    if len(raw) > max_bytes:
        raise ImportErrorBase("public minute response exceeds size limit")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImportErrorBase("public minute response is not JSON") from exc
    return payload, raw.decode("utf-8")


def _result_from_payload(
    anchor: DailyAnchor,
    payload: object,
    *,
    source: str,
    trade_date: str,
    captured_at: dt.datetime,
    batch_id: str,
) -> dict:
    if source == SOURCE:
        parser = parse_sina_session
    else:
        symbol = _sina_symbol(anchor.stock_code)
        cached_stock = (payload.get("data") or {}).get(symbol) if isinstance(payload, dict) else None
        parser = (
            parse_tencent_kline_session
            if isinstance(cached_stock, dict) and "m1" in cached_stock
            else parse_tencent_session
        )
    rows = parser(
        payload,
        anchor=anchor,
        trade_date=trade_date,
        captured_at=captured_at,
        batch_id=batch_id,
    )
    rows = _normalize_storage_rows(rows)
    return {
        "stock_code": anchor.stock_code,
        "rows": rows,
        "digest": _row_digest(rows),
        "captured_at": captured_at,
    }


def parse_tencent_session(
    payload: object,
    *,
    anchor: DailyAnchor,
    trade_date: str,
    captured_at: dt.datetime,
    batch_id: str,
) -> list[dict]:
    """Build the 238-bar public session from cumulative Tencent snapshots."""
    if not isinstance(payload, dict):
        raise ImportErrorBase("Tencent payload is not an object")
    symbol = _sina_symbol(anchor.stock_code)
    stock = (payload.get("data") or {}).get(symbol)
    if not isinstance(stock, dict):
        raise ImportErrorBase("Tencent payload is missing the requested symbol")
    minute_data = stock.get("data")
    if not isinstance(minute_data, dict):
        raise ImportErrorBase("Tencent minute data is missing")
    if str(minute_data.get("date") or "") != trade_date.replace("-", ""):
        raise ImportErrorBase("Tencent minute data has the wrong date")
    raw_rows = minute_data.get("data")
    if not isinstance(raw_rows, list) or not raw_rows or len(raw_rows) > 400:
        raise ImportErrorBase("Tencent minute rows are invalid")
    cumulative: dict[str, tuple[Decimal, Decimal | None]] = {}
    prices: dict[str, Decimal] = {}
    for raw in raw_rows:
        if not isinstance(raw, str):
            raise ImportErrorBase("Tencent minute row is not text")
        parts = raw.split()
        if len(parts) < 3 or len(parts[0]) != 4 or not parts[0].isdigit():
            raise ImportErrorBase("Tencent minute row is invalid")
        clock = parts[0]
        if clock in cumulative:
            raise ImportErrorBase("Tencent minute clock is duplicated")
        prices[clock] = _decimal(parts[1], positive=True)
        cumulative[clock] = (
            _decimal(parts[2], nonnegative=True),
            (
                _decimal(parts[3], nonnegative=True)
                if len(parts) >= 4
                else None
            ),
        )
    previous_volume = Decimal("0")
    previous_amount: Decimal | None = Decimal("0")
    opening_snapshot = cumulative.get("0930")
    if opening_snapshot is None:
        raise ImportErrorBase("Tencent opening snapshot is missing")
    target = dt.date.fromisoformat(trade_date)
    rows: list[dict] = []
    for clock in MINUTE_GRID:
        compact = clock.replace(":", "")[:4]
        if compact not in cumulative or compact not in prices:
            raise ImportErrorBase(f"Tencent minute snapshot is missing: {clock}")
        price = prices[compact]
        volume, amount = cumulative[compact]
        volume_delta = volume - previous_volume
        amount_delta = (
            amount - previous_amount
            if amount is not None and previous_amount is not None
            else None
        )
        if volume_delta < 0 or (
            amount_delta is not None and amount_delta < -1
        ):
            raise ImportErrorBase("Tencent cumulative series is not monotonic")
        if amount_delta is not None and amount_delta < 0:
            amount_delta = Decimal("0")
        previous_volume, previous_amount = volume, amount
        stamp = dt.datetime.combine(
            target,
            dt.time.fromisoformat(clock),
        )
        change = price - anchor.pre_close
        rows.append({
            "stock_code": anchor.stock_code,
            "trade_time": stamp,
            "trade_date": target,
            "price": price,
            "avg_price": None,
            "change": change,
            "change_pct": change / anchor.pre_close * Decimal("100"),
            "volume": volume_delta * Decimal("100"),
            "amount": amount_delta,
            "etl_sync_at": captured_at,
            "qmt_code": anchor.qmt_code,
            "data_source": TENCENT_SOURCE,
            "source_time": stamp,
            "received_at": captured_at,
            "batch_id": batch_id,
            "data_version": TENCENT_DATA_VERSION,
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
        })
    ordered = sorted(rows, key=lambda row: row["trade_time"])
    actual_grid = tuple(row["trade_time"].strftime("%H:%M:%S") for row in ordered)
    if actual_grid != MINUTE_GRID:
        raise ImportErrorBase("Tencent session does not cover the canonical grid")
    if (
        abs(ordered[-1]["price"] - anchor.close) > Decimal("0.011")
        or max(row["price"] for row in ordered) > anchor.high + Decimal("0.011")
        or min(row["price"] for row in ordered) < anchor.low - Decimal("0.011")
    ):
        raise ImportErrorBase("Tencent bars differ from the verified daily anchor")
    return ordered


def parse_tencent_kline_session(
    payload: object,
    *,
    anchor: DailyAnchor,
    trade_date: str,
    captured_at: dt.datetime,
    batch_id: str,
) -> list[dict]:
    """Build the 238-bar session from Tencent's 241-row m1 history."""
    if not isinstance(payload, dict):
        raise ImportErrorBase("Tencent K-line payload is not an object")
    symbol = _sina_symbol(anchor.stock_code)
    stock = (payload.get("data") or {}).get(symbol)
    if not isinstance(stock, dict):
        raise ImportErrorBase("Tencent K-line payload is missing the symbol")
    raw_rows = stock.get("m1")
    if not isinstance(raw_rows, list) or len(raw_rows) > 400:
        raise ImportErrorBase("Tencent K-line rows are invalid")
    target_prefix = trade_date.replace("-", "")
    observed: dict[str, dict] = {}
    for raw in raw_rows:
        if not isinstance(raw, list) or len(raw) < 6:
            raise ImportErrorBase("Tencent K-line row is invalid")
        stamp = str(raw[0])
        if len(stamp) != 12 or not stamp.isdigit():
            raise ImportErrorBase("Tencent K-line timestamp is invalid")
        if not stamp.startswith(target_prefix):
            continue
        clock = f"{stamp[8:10]}:{stamp[10:12]}:00"
        if clock in observed:
            raise ImportErrorBase("Tencent K-line clock is duplicated")
        opening = _decimal(raw[1], positive=True)
        closing = _decimal(raw[2], positive=True)
        high = _decimal(raw[3], positive=True)
        low = _decimal(raw[4], positive=True)
        volume = _decimal(raw[5], nonnegative=True) * Decimal("100")
        if not low <= opening <= high or not low <= closing <= high:
            raise ImportErrorBase("Tencent K-line OHLC is inconsistent")
        observed[clock] = {
            "price": closing,
            "high": high,
            "low": low,
            "volume": volume,
        }
    if not observed:
        raise ImportErrorBase("Tencent K-line has no target-date rows")
    target = dt.date.fromisoformat(trade_date)
    rows: list[dict] = []
    for clock in MINUTE_GRID:
        if clock == "09:31:00":
            opening_minute = observed.get("09:30:00")
            first_minute = observed.get(clock)
            if opening_minute is None or first_minute is None:
                raise ImportErrorBase("Tencent opening K-line is missing")
            price = first_minute["price"]
            volume = opening_minute["volume"] + first_minute["volume"]
        elif clock == "15:00:00":
            closing_rows = [
                observed.get("14:58:00"),
                observed.get("14:59:00"),
                observed.get(clock),
            ]
            if any(value is None for value in closing_rows):
                raise ImportErrorBase("Tencent closing K-line is missing")
            price = closing_rows[-1]["price"]
            volume = sum(
                value["volume"]
                for value in closing_rows
            )
        else:
            value = observed.get(clock)
            if value is None:
                raise ImportErrorBase(f"Tencent K-line is missing: {clock}")
            price = value["price"]
            volume = value["volume"]
        stamp = dt.datetime.combine(target, dt.time.fromisoformat(clock))
        change = price - anchor.pre_close
        rows.append({
            "stock_code": anchor.stock_code,
            "trade_time": stamp,
            "trade_date": target,
            "price": price,
            "avg_price": None,
            "change": change,
            "change_pct": change / anchor.pre_close * Decimal("100"),
            "volume": volume,
            "amount": None,
            "etl_sync_at": captured_at,
            "qmt_code": anchor.qmt_code,
            "data_source": TENCENT_SOURCE,
            "source_time": stamp,
            "received_at": captured_at,
            "batch_id": batch_id,
            "data_version": "tencent_mkline_verified_238_v1",
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
        })
    if (
        abs(rows[-1]["price"] - anchor.close) > Decimal("0.011")
        or max(row["price"] for row in rows) > anchor.high + Decimal("0.011")
        or min(row["price"] for row in rows) < anchor.low - Decimal("0.011")
    ):
        raise ImportErrorBase("Tencent K-line differs from the daily anchor")
    _validate_anchor_totals(rows, anchor)
    return rows


def _fetch_tencent_one(
    anchor: DailyAnchor,
    *,
    session: requests.Session,
    trade_date: str,
    batch_id: str,
    timeout: float,
) -> tuple[dict, str]:
    captured_at = dt.datetime.now().replace(microsecond=0)
    symbol = _sina_symbol(anchor.stock_code)
    if not symbol.startswith("bj"):
        try:
            payload, raw = _bounded_json_get(
                session,
                TENCENT_KLINE_SOURCE_URL,
                params={"param": f"{symbol},m1,,320"},
                timeout=timeout,
            )
            rows = parse_tencent_kline_session(
                payload,
                anchor=anchor,
                trade_date=trade_date,
                captured_at=captured_at,
                batch_id=batch_id,
            )
        except (ImportErrorBase, requests.RequestException):
            payload, raw = _bounded_json_get(
                session,
                TENCENT_SOURCE_URL,
                params={"code": symbol},
                timeout=timeout,
            )
            rows = parse_tencent_session(
                payload,
                anchor=anchor,
                trade_date=trade_date,
                captured_at=captured_at,
                batch_id=batch_id,
            )
    else:
        payload, raw = _bounded_json_get(
            session,
            TENCENT_SOURCE_URL,
            params={"code": symbol},
            timeout=timeout,
        )
        rows = parse_tencent_session(
            payload,
            anchor=anchor,
            trade_date=trade_date,
            captured_at=captured_at,
            batch_id=batch_id,
        )
    rows = _normalize_storage_rows(rows)
    return {
        "stock_code": anchor.stock_code,
        "rows": rows,
        "digest": _row_digest(rows),
        "captured_at": captured_at,
    }, raw


def _paced_fetch(
    fetcher,
    anchor: DailyAnchor,
    *,
    request_delay: float,
    proxy: str = "",
    **kwargs,
):
    session = getattr(_SOURCE_SESSION_LOCAL, "session", None)
    if session is None:
        session = _new_source_session(proxy)
        _SOURCE_SESSION_LOCAL.session = session
    try:
        return fetcher(anchor, session=session, **kwargs)
    finally:
        time.sleep(max(0.0, request_delay))


_SOURCE_SESSION_LOCAL = threading.local()


def _receipt_id(trade_date: str, content_sha256: str) -> str:
    return hashlib.sha256(
        f"{SOURCE}|{trade_date}|{content_sha256}".encode("ascii")
    ).hexdigest()


def _write_receipt(
    engine,
    *,
    trade_date: str,
    expected_count: int,
    observed_count: int,
    row_count: int,
    content_sha256: str,
    status: str,
    evidence: dict,
    source_provider: str = SOURCE,
) -> str:
    receipt_id = _receipt_id(trade_date, content_sha256)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                INSERT INTO `{RECEIPT_TABLE}`
                  (receipt_id, trade_date, first_trade_time, last_trade_time,
                   expected_count, observed_count, coverage, row_count,
                   source_provider, capture_mode, forward_eligible,
                   quality_status, evidence_json, created_at)
                VALUES
                  (:receipt_id, :trade_date, :first_trade_time, :last_trade_time,
                   :expected_count, :observed_count, :coverage, :row_count,
                   :source_provider, 'HISTORICAL_REPAIR', 0,
                   :quality_status, :evidence_json, NOW())
                ON DUPLICATE KEY UPDATE
                  receipt_id=VALUES(receipt_id),
                  expected_count=VALUES(expected_count),
                  observed_count=VALUES(observed_count),
                  coverage=VALUES(coverage), row_count=VALUES(row_count),
                  quality_status=VALUES(quality_status),
                  evidence_json=VALUES(evidence_json)
                """
            ),
            {
                "receipt_id": receipt_id,
                "trade_date": trade_date,
                "first_trade_time": f"{trade_date} 09:31:00",
                "last_trade_time": f"{trade_date} 15:00:00",
                "expected_count": int(expected_count),
                "observed_count": int(observed_count),
                "coverage": (
                    float(observed_count) / float(expected_count)
                    if expected_count else 0.0
                ),
                "row_count": int(row_count),
                "source_provider": source_provider,
                "quality_status": status,
                "evidence_json": json.dumps(
                    {**evidence, "grid_sha256": GRID_SHA256},
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            },
        )
    return receipt_id


def _readback_proofs(
    engine,
    *,
    trade_date: str,
    sources: Sequence[str],
) -> tuple[int, int, str]:
    statement = text(
        """
        SELECT stock_code, trade_time, price, `change`, change_pct, volume, amount
        FROM sm_stock_minute
        WHERE trade_date=:trade_date AND data_source IN :sources
        ORDER BY stock_code, trade_time
        """
    ).bindparams(bindparam("sources", expanding=True))
    proofs: list[list[object]] = []
    rows: list[dict] = []
    current_code = None
    row_count = 0

    def finish_code() -> None:
        if not rows:
            return
        proofs.append([
            str(current_code),
            len(rows),
            _row_digest(rows),
        ])
        rows.clear()

    with engine.connect() as connection:
        stream = connection.execution_options(
            stream_results=True,
        ).execute(
            statement,
            {
                "trade_date": trade_date,
                "sources": list(sources),
            },
        ).mappings()
        for raw in stream:
            code = str(raw["stock_code"]).zfill(6)
            if code != current_code:
                finish_code()
                current_code = code
            stamp = raw["trade_time"]
            if not isinstance(stamp, dt.datetime):
                stamp = dt.datetime.fromisoformat(str(stamp))
            rows.append({
                "stock_code": code,
                "trade_time": stamp,
                "price": raw["price"],
                "change": raw["change"],
                "change_pct": raw["change_pct"],
                "volume": raw["volume"],
                "amount": raw["amount"],
            })
            row_count += 1
        finish_code()
    return row_count, len(proofs), _proof_digest(proofs)


def _selected_codes(args, primary_engine) -> list[str]:
    requested = sorted({str(code).strip().zfill(6) for code in args.stock_code})
    if any(len(code) != 6 or not code.isdigit() for code in requested):
        raise ImportErrorBase("stock codes must be six digits")
    if not args.all_catalog:
        if not requested:
            raise ImportErrorBase("provide --stock-code or --all-catalog")
        return requested
    decision_at = dt.datetime.now().replace(microsecond=0)
    _catalog, codes = load_target_stock_catalog(
        primary_engine,
        target_date=args.trade_date,
        decision_known_at=decision_at,
    )
    return sorted(codes)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--stock-code", action="append", default=[])
    parser.add_argument("--all-catalog", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--request-delay", type=float, default=0.25)
    parser.add_argument("--min-coverage", type=float, default=0.95)
    parser.add_argument("--checkpoint-dir")
    parser.add_argument("--proxy", default="")
    parser.add_argument("--json", action="store_true")
    return parser


def _report_json(report: dict, *, compact: bool) -> str:
    return json.dumps(
        report,
        ensure_ascii=False,
        indent=None if compact else 1,
        separators=(",", ":") if compact else None,
    )


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    load_project_env()
    dt.date.fromisoformat(args.trade_date)
    if (
        not 1 <= int(args.workers) <= 16
        or not 1 <= float(args.timeout) <= 60
        or not 0 <= float(args.request_delay) <= 5
        or not 0 < float(args.min_coverage) <= 1
    ):
        raise ValueError("collection bounds are outside the allowed range")
    primary_engine = create_batch_engine()
    kline_url = get_kline_mysql_url()
    kline_engine = (
        create_pooled_engine(kline_url, pool_config={
            "pool_size": 2, "max_overflow": 0, "pool_recycle": 300,
        })
        if kline_url else primary_engine
    )
    minute_engine = get_minute_engine()
    codes = _selected_codes(args, primary_engine)
    anchors, no_trade_codes = _load_anchors(
        kline_engine,
        trade_date=args.trade_date,
        stock_codes=codes,
    )
    if not anchors:
        raise ImportErrorBase("verified daily anchors contain no tradable codes")
    batch_id = f"{SOURCE}-{args.trade_date}-{uuid.uuid4().hex[:12]}"
    checkpoint_dir = (
        Path(args.checkpoint_dir).expanduser().resolve()
        if args.checkpoint_dir else None
    )
    if checkpoint_dir is not None:
        checkpoint_dir.mkdir(parents=True, exist_ok=True)

    stage, stage_connection = _create_kline_stage(
        minute_engine,
        "sm_stock_minute",
    )
    proofs: dict[str, list[object]] = {}
    source_counts: dict[str, int] = {}
    amount_missing_codes: set[str] = set()
    buffer: list[dict] = []
    staged_rows = 0

    def flush_buffer() -> None:
        nonlocal staged_rows
        if not buffer:
            return
        written = _append_kline_stage(
            stage_connection,
            stage,
            buffer,
            "sm_stock_minute",
        )
        if written != len(buffer):
            raise RuntimeError("minute stage row count mismatch")
        staged_rows += written
        buffer.clear()

    def consume(code: str, result: dict) -> None:
        if code in proofs:
            raise RuntimeError("duplicate completed stock code")
        rows = result["rows"]
        proofs[code] = [code, len(rows), _row_digest(rows)]
        for row in rows:
            source = str(row["data_source"])
            source_counts[source] = source_counts.get(source, 0) + 1
            if row["amount"] is None:
                amount_missing_codes.add(code)
        buffer.extend(rows)
        if len(buffer) >= 10000:
            flush_buffer()

    failures: dict[str, str] = {}
    remaining: list[str] = []
    for code in sorted(anchors):
        restored = False
        for source in (SOURCE, TENCENT_SOURCE):
            cached = _read_cached_payload(
                checkpoint_dir,
                source=source,
                stock_code=code,
            )
            if cached is None:
                continue
            payload, captured_at = cached
            try:
                consume(code, _result_from_payload(
                    anchors[code],
                    payload,
                    source=source,
                    trade_date=args.trade_date,
                    captured_at=captured_at,
                    batch_id=batch_id,
                ))
                restored = True
                break
            except ImportErrorBase:
                continue
        if not restored:
            remaining.append(code)
    restored_count = len(proofs)
    started = time.monotonic()
    consecutive_failures = 0
    source_circuit_open = False
    with ThreadPoolExecutor(max_workers=int(args.workers)) as pool:
        futures = {
            pool.submit(
                _paced_fetch,
                _fetch_one,
                anchors[code],
                trade_date=args.trade_date,
                batch_id=batch_id,
                timeout=float(args.timeout),
                request_delay=float(args.request_delay),
                proxy=args.proxy,
            ): code
            for code in remaining
        }
        for index, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            try:
                result, raw = future.result()
                consume(code, result)
                consecutive_failures = 0
                _write_cached_payload(
                    checkpoint_dir,
                    source=SOURCE,
                    stock_code=code,
                    payload=json.loads(raw),
                    captured_at=result["captured_at"],
                )
            except Exception as exc:
                failures[code] = f"{type(exc).__name__}: {exc}"
                consecutive_failures += 1
            if _source_circuit_tripped(consecutive_failures):
                source_circuit_open = True
                for pending in futures:
                    pending.cancel()
                print(
                    "minute source circuit open: "
                    f"consecutive_failures={consecutive_failures}",
                    file=sys.stderr,
                    flush=True,
                )
                break
            if index % 100 == 0:
                print(
                    f"minute source checked={restored_count + index}/"
                    f"{len(anchors)} "
                    f"complete={len(proofs)} failed={len(failures)}",
                    file=sys.stderr,
                    flush=True,
                )
    if source_circuit_open:
        for code in remaining:
            if code not in proofs:
                failures.setdefault(
                    code,
                    "MinuteSourceError: source circuit open",
                )

    if failures:
        fallback_failures: dict[str, str] = {}
        fallback_codes = sorted(set(failures) & set(anchors))
        with ThreadPoolExecutor(max_workers=int(args.workers)) as pool:
            futures = {
                pool.submit(
                    _paced_fetch,
                    _fetch_tencent_one,
                    anchors[code],
                    trade_date=args.trade_date,
                    batch_id=batch_id,
                    timeout=float(args.timeout),
                    request_delay=float(args.request_delay),
                    proxy=args.proxy,
                ): code
                for code in fallback_codes
            }
            for index, future in enumerate(as_completed(futures), 1):
                code = futures[future]
                try:
                    result, raw = future.result()
                    consume(code, result)
                    failures.pop(code, None)
                    _write_cached_payload(
                        checkpoint_dir,
                        source=TENCENT_SOURCE,
                        stock_code=code,
                        payload=json.loads(raw),
                        captured_at=result["captured_at"],
                    )
                except Exception as exc:
                    fallback_failures[code] = (
                        f"{type(exc).__name__}: {exc}"
                    )
                if index % 100 == 0:
                    print(
                        f"minute fallback checked={index}/{len(fallback_codes)} "
                        f"complete={len(proofs)} failed={len(fallback_failures)}",
                        file=sys.stderr,
                        flush=True,
                    )
        failures.update(fallback_failures)
    flush_buffer()

    coverage = (
        (len(proofs) + len(no_trade_codes)) / len(codes)
        if codes else 0.0
    )
    minimum_coverage = (
        1.0 if args.stock_code and not args.all_catalog
        else float(args.min_coverage)
    )
    content_sha256 = _proof_digest(
        [proofs[code] for code in sorted(proofs)]
    )
    provider = (
        f"{SOURCE}+{TENCENT_SOURCE}"
        if TENCENT_SOURCE in source_counts
        else SOURCE
    )
    evidence = {
        "source": SOURCE,
        "source_url": SOURCE_URL,
        "fallback_source": TENCENT_SOURCE,
        "fallback_source_url": TENCENT_SOURCE_URL,
        "provider": provider,
        "source_counts": source_counts,
        "amount_missing_samples": sorted(amount_missing_codes)[:20],
        "amount_missing_count": len(amount_missing_codes),
        "trade_date": args.trade_date,
        "batch_id": batch_id,
        "expected_codes": len(codes),
        "tradable_codes": len(anchors),
        "no_trade_codes": sorted(no_trade_codes),
        "no_trade_count": len(no_trade_codes),
        "complete_codes": len(proofs),
        "failed_codes": len(failures),
        "failure_samples": dict(sorted(failures.items())[:20]),
        "row_count": staged_rows,
        "minutes_per_code": len(MINUTE_GRID),
        "content_sha256": content_sha256,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    report = {
        "schema": "probiga.public-stock-minute-result.v1",
        "status": (
            "READY"
            if coverage >= minimum_coverage and staged_rows
            else "FAILED"
        ),
        "apply": bool(args.apply),
        "coverage": round(coverage, 8),
        "minimum_coverage": minimum_coverage,
        "acquisition_status": (
            "COMPLETE"
            if coverage == 1.0 and not failures
            else "PARTIAL"
        ),
        **evidence,
    }
    if report["status"] != "READY":
        if args.apply:
            _write_receipt(
                minute_engine,
                trade_date=args.trade_date,
                expected_count=len(anchors),
                observed_count=len(proofs),
                row_count=staged_rows,
                content_sha256=content_sha256,
                status="FAILED",
                evidence=report,
                source_provider=provider,
            )
        _drop_kline_stage(stage_connection)
        print(_report_json(report, compact=args.json))
        return 2

    receipt_id = _write_receipt(
        minute_engine,
        trade_date=args.trade_date,
        expected_count=len(anchors),
        observed_count=len(proofs),
        row_count=staged_rows,
        content_sha256=content_sha256,
        status="RUNNING",
        evidence=report,
        source_provider=provider,
    )
    report["receipt_id"] = receipt_id
    if not args.apply:
        _drop_kline_stage(stage_connection)
        print(_report_json(report, compact=args.json))
        return 0

    try:
        written = _publish_kline_stage(
            minute_engine,
            stage_connection,
            stage,
            "sm_stock_minute",
            receipt_engine=minute_engine,
        )
        readback_rows, readback_codes, readback_sha256 = _readback_proofs(
            minute_engine,
            trade_date=args.trade_date,
            sources=sorted(source_counts),
        )
        if (
            written != staged_rows
            or readback_rows != staged_rows
            or readback_codes != len(proofs)
            or readback_sha256 != content_sha256
        ):
            raise ImportErrorBase(
                "database readback differs from the parsed source content"
            )
        report.update({
            "status": "PASS",
            "written_rows": written,
            "readback_rows": readback_rows,
            "readback_codes": readback_codes,
            "readback_sha256": readback_sha256,
            "acquisition_status": (
                "COMPLETE"
                if len(proofs) == len(anchors) and not failures
                else "PARTIAL"
            ),
        })
        _write_receipt(
            minute_engine,
            trade_date=args.trade_date,
            expected_count=len(anchors),
            observed_count=len(proofs),
            row_count=readback_rows,
            content_sha256=content_sha256,
            status="PASS",
            evidence=report,
            source_provider=provider,
        )
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        _write_receipt(
            minute_engine,
            trade_date=args.trade_date,
            expected_count=len(anchors),
            observed_count=len(proofs),
            row_count=staged_rows,
            content_sha256=content_sha256,
            status="FAILED",
            evidence=report,
            source_provider=provider,
        )
        _drop_kline_stage(stage_connection)
        print(_report_json(report, compact=args.json))
        return 2
    _drop_kline_stage(stage_connection)
    print(_report_json(report, compact=args.json))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
