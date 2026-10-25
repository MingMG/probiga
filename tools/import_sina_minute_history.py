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
import hashlib
import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
from tools.crawl_minute_kline import save_kline  # noqa: E402
from tools.env_config import load_project_env  # noqa: E402


SOURCE = "sina_public_1m"
DATA_VERSION = "sina_public_1m_238_v1"
RECEIPT_TABLE = "st_qmt_minute_sync_receipt_v2"
SOURCE_URL = (
    "https://quotes.sina.cn/cn/api/json_v2.php/"
    "CN_MarketDataService.getKLineData"
)
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


@dataclass(frozen=True)
class DailyAnchor:
    stock_code: str
    qmt_code: str
    pre_close: Decimal
    close: Decimal
    high: Decimal
    low: Decimal


def _decimal(value: object, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ImportErrorBase("invalid numeric source value") from exc
    if not number.is_finite() or (positive and number <= 0):
        raise ImportErrorBase("invalid numeric source value")
    return number


def _market_suffix(stock_code: str) -> str:
    if stock_code.startswith(("5", "6", "9")):
        return "SH"
    if stock_code.startswith(("0", "1", "2", "3")):
        return "SZ"
    if stock_code.startswith(("4", "8")):
        return "BJ"
    raise ImportErrorBase(f"unsupported stock-code namespace: {stock_code}")


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
        raise ImportErrorBase(
            f"incomplete source grid: expected={len(MINUTE_GRID)} "
            f"actual={len(actual_grid)}"
        )
    if (
        ordered[0]["price"] <= 0
        or ordered[-1]["price"] <= 0
        or abs(ordered[-1]["price"] - anchor.close) > Decimal("0.011")
        or max(row["price"] for row in ordered) > anchor.high + Decimal("0.011")
        or min(row["price"] for row in ordered) < anchor.low - Decimal("0.011")
    ):
        raise ImportErrorBase("source bars differ from the verified daily anchor")
    return ordered


def _row_digest(rows: Sequence[dict]) -> str:
    def stable_decimal(value: object) -> str:
        number = Decimal(str(value))
        return format(number.quantize(Decimal("0.000001")), "f")

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


def _load_anchors(
    engine,
    *,
    trade_date: str,
    stock_codes: Sequence[str],
) -> dict[str, DailyAnchor]:
    anchors: dict[str, DailyAnchor] = {}
    statement = text(
        """
        SELECT stock_code, close, high, low, pre_close
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
            if code in anchors:
                continue
            anchor = DailyAnchor(
                stock_code=code,
                qmt_code=f"{code}.{_market_suffix(code)}",
                pre_close=_decimal(row["pre_close"], positive=True),
                close=_decimal(row["close"], positive=True),
                high=_decimal(row["high"], positive=True),
                low=_decimal(row["low"], positive=True),
            )
            anchors[code] = anchor
    missing = sorted(set(stock_codes) - set(anchors))
    if missing:
        raise ImportErrorBase(
            "verified daily anchors are missing: " + ",".join(missing[:20])
        )
    return anchors


def _fetch_one(
    anchor: DailyAnchor,
    *,
    session: requests.Session,
    trade_date: str,
    batch_id: str,
    timeout: float,
) -> tuple[dict, str]:
    response = session.get(
        SOURCE_URL,
        params={
            "symbol": _sina_symbol(anchor.stock_code),
            "scale": "1",
            "ma": "no",
            "datalen": "1023",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    captured_at = dt.datetime.now().replace(microsecond=0)
    rows = parse_sina_session(
        response.json(),
        anchor=anchor,
        trade_date=trade_date,
        captured_at=captured_at,
        batch_id=batch_id,
    )
    digest = _row_digest(rows)
    return {
        "stock_code": anchor.stock_code,
        "rows": rows,
        "digest": digest,
        "captured_at": captured_at,
    }, response.text


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
                "source_provider": SOURCE,
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


def _readback(engine, *, trade_date: str, codes: Sequence[str]) -> tuple[int, int, str]:
    statement = text(
        """
        SELECT stock_code, trade_time, price, `change`, change_pct, volume, amount
        FROM sm_stock_minute
        WHERE trade_date=:trade_date AND data_source=:source
          AND stock_code IN :codes
        ORDER BY stock_code, trade_time
        """
    ).bindparams(bindparam("codes", expanding=True))
    rows: list[dict] = []
    with engine.connect() as connection:
        for offset in range(0, len(codes), 400):
            batch = list(codes[offset:offset + 400])
            rows.extend(
                dict(row)
                for row in connection.execute(
                    statement,
                    {
                        "trade_date": trade_date,
                        "source": SOURCE,
                        "codes": batch,
                    },
                ).mappings()
            )
    normalized = [
        {
            "stock_code": str(row["stock_code"]).zfill(6),
            "trade_time": row["trade_time"],
            "price": row["price"],
            "change": row["change"],
            "change_pct": row["change_pct"],
            "volume": row["volume"],
            "amount": row["amount"],
        }
        for row in rows
    ]
    return len(normalized), len({row["stock_code"] for row in normalized}), _row_digest(
        normalized
    )


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
    parser.add_argument("--proxy", default="")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    load_project_env()
    dt.date.fromisoformat(args.trade_date)
    if not 1 <= int(args.workers) <= 16 or not 1 <= float(args.timeout) <= 60:
        raise ValueError("workers or timeout is outside the bounded range")
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
    anchors = _load_anchors(
        kline_engine,
        trade_date=args.trade_date,
        stock_codes=codes,
    )
    batch_id = f"{SOURCE}-{args.trade_date}-{uuid.uuid4().hex[:12]}"

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Referer": "https://finance.sina.com.cn/",
    })
    if args.proxy:
        session.proxies.update({"http": args.proxy, "https": args.proxy})
    results: dict[str, dict] = {}
    failures: dict[str, str] = {}
    started = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=int(args.workers)) as pool:
            futures = {
                pool.submit(
                    _fetch_one,
                    anchors[code],
                    session=session,
                    trade_date=args.trade_date,
                    batch_id=batch_id,
                    timeout=float(args.timeout),
                ): code
                for code in codes
            }
            for index, future in enumerate(as_completed(futures), 1):
                code = futures[future]
                try:
                    result, _raw = future.result()
                    results[code] = result
                except Exception as exc:
                    failures[code] = f"{type(exc).__name__}: {exc}"
                if index % 100 == 0:
                    print(
                        f"minute source checked={index}/{len(codes)} "
                        f"complete={len(results)} failed={len(failures)}",
                        file=sys.stderr,
                        flush=True,
                    )
    finally:
        session.close()

    coverage = len(results) / len(codes) if codes else 0.0
    minimum_coverage = 1.0 if args.stock_code and not args.all_catalog else 0.95
    content_rows = [
        row
        for code in sorted(results)
        for row in results[code]["rows"]
    ]
    content_sha256 = _row_digest(content_rows)
    evidence = {
        "source": SOURCE,
        "source_url": SOURCE_URL,
        "trade_date": args.trade_date,
        "batch_id": batch_id,
        "expected_codes": len(codes),
        "complete_codes": len(results),
        "failed_codes": len(failures),
        "failure_samples": dict(sorted(failures.items())[:20]),
        "row_count": len(content_rows),
        "minutes_per_code": len(MINUTE_GRID),
        "content_sha256": content_sha256,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    report = {
        "status": (
            "READY"
            if coverage >= minimum_coverage and content_rows
            else "FAILED"
        ),
        "apply": bool(args.apply),
        "coverage": round(coverage, 8),
        "minimum_coverage": minimum_coverage,
        **evidence,
    }
    if report["status"] != "READY":
        if args.apply:
            _write_receipt(
                minute_engine,
                trade_date=args.trade_date,
                expected_count=len(codes),
                observed_count=len(results),
                row_count=len(content_rows),
                content_sha256=content_sha256,
                status="FAILED",
                evidence=report,
            )
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return 2

    receipt_id = _write_receipt(
        minute_engine,
        trade_date=args.trade_date,
        expected_count=len(codes),
        observed_count=len(results),
        row_count=len(content_rows),
        content_sha256=content_sha256,
        status="RUNNING",
        evidence=report,
    )
    report["receipt_id"] = receipt_id
    if not args.apply:
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return 0

    try:
        written = save_kline(
            minute_engine,
            content_rows,
            "sm_stock_minute",
            receipt_engine=minute_engine,
        )
        readback_rows, readback_codes, readback_sha256 = _readback(
            minute_engine,
            trade_date=args.trade_date,
            codes=sorted(results),
        )
        if (
            written != len(content_rows)
            or readback_rows != len(content_rows)
            or readback_codes != len(results)
            or readback_sha256 != content_sha256
        ):
            raise ImportErrorBase(
                "database readback differs from the parsed source content"
            )
        report.update({
            "written_rows": written,
            "readback_rows": readback_rows,
            "readback_codes": readback_codes,
            "readback_sha256": readback_sha256,
        })
        _write_receipt(
            minute_engine,
            trade_date=args.trade_date,
            expected_count=len(codes),
            observed_count=len(results),
            row_count=readback_rows,
            content_sha256=content_sha256,
            status="PASS",
            evidence=report,
        )
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        _write_receipt(
            minute_engine,
            trade_date=args.trade_date,
            expected_count=len(codes),
            observed_count=len(results),
            row_count=len(content_rows),
            content_sha256=content_sha256,
            status="FAILED",
            evidence=report,
        )
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
