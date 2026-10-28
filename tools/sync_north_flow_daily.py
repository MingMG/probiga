#!/usr/bin/env python3
"""Publish the current northbound flow from the THS after-close snapshot.

Eastmoney no longer exposes northbound net buy/sell fields for recent
sessions.  Its rows are retained as unavailable for those fields, while the
THS day chart still provides the signed cumulative northbound flow.  This
collector binds that undated intraday payload to one authoritative closed
trade date and stores the completed 15:00 values atomically.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import json
import math
import time
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from server.common.authoritative_market_clock import authoritative_closed_trade_date
from tools.env_config import create_tool_engine, load_project_env


SOURCE_URL = "https://data.hexin.cn/market/hsgtApi/method/dayChart/"
SHANGHAI = ZoneInfo("Asia/Shanghai")
RESULT_SCHEMA = "probiga.north-flow-daily-result.v1"


def _number(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("north flow value is not finite")
    return number


def parse_ths_day_chart(
    payload: Mapping[str, Any],
    *,
    target_date: str,
) -> dict[str, Any]:
    """Validate a THS day chart and return a closed-session snapshot."""

    target = date.fromisoformat(target_date)
    times = payload.get("time")
    hgt = payload.get("hgt")
    sgt = payload.get("sgt")
    if (
        not isinstance(times, list)
        or not isinstance(hgt, list)
        or not isinstance(sgt, list)
        or not times
        or len(times) != len(hgt)
        or not sgt
    ):
        raise ValueError("THS north flow arrays are missing or misaligned")
    if str(times[-1]).strip() != "15:00":
        raise ValueError("THS north flow snapshot is not a completed 15:00 session")
    north_hgt = _number(hgt[-1])
    north_sgt = _number(sgt[-1])
    latest = {
        "trade_time": datetime.combine(target, datetime.min.time()).replace(hour=15),
        "net_hgt": int(round(north_hgt * 100_000_000)),
        "net_sgt": int(round(north_sgt * 100_000_000)),
        "net_tgt": int(round((north_hgt + north_sgt) * 100_000_000)),
    }
    # THS sometimes exposes only a trailing Shenzhen window while the Shanghai
    # series covers the full session.  Publish minute rows only when both
    # arrays share one complete clock axis; the daily/current close remains
    # independently provable from each final value.
    rows = []
    if len(times) == len(sgt):
        for clock, hgt_value, sgt_value in zip(times, hgt, sgt):
            clock_text = str(clock or "").strip()
            try:
                stamp = datetime.strptime(clock_text, "%H:%M")
            except ValueError as exc:
                raise ValueError(f"invalid THS north flow time: {clock_text}") from exc
            north_hgt = _number(hgt_value)
            north_sgt = _number(sgt_value)
            rows.append(
                {
                    "trade_time": datetime.combine(target, stamp.time()),
                    "net_hgt": int(round(north_hgt * 100_000_000)),
                    "net_sgt": int(round(north_sgt * 100_000_000)),
                    "net_tgt": int(round((north_hgt + north_sgt) * 100_000_000)),
                }
            )
        rows.sort(key=lambda row: row["trade_time"])
    return {
        "target_date": target.isoformat(),
        "daily": {
            "trade_date": target,
            "net_hgt": latest["net_hgt"],
            "net_sgt": latest["net_sgt"],
            "net_tgt": latest["net_tgt"],
        },
        "minute_rows": rows,
        "latest": latest,
    }


def fetch_ths_day_chart(*, timeout_seconds: int = 20, attempts: int = 3) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            response = requests.get(
                SOURCE_URL,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://data.10jqka.com.cn/",
                },
                timeout=timeout_seconds,
                proxies={},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("THS north flow response is not an object")
            times = payload.get("time")
            hgt = payload.get("hgt")
            sgt = payload.get("sgt")
            if (
                not isinstance(times, list)
                or not isinstance(hgt, list)
                or not isinstance(sgt, list)
                or len(times) != len(hgt)
                or not sgt
                or not times
            ):
                raise ValueError("THS north flow response is incomplete")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(float(attempt))
    raise RuntimeError(f"THS north flow request failed: {last_error}") from last_error


def publish_current_day(
    engine: Any,
    snapshot: Mapping[str, Any],
    *,
    apply: bool,
) -> dict[str, Any]:
    target = date.fromisoformat(str(snapshot["target_date"]))
    daily = snapshot["daily"]
    minute_rows = list(snapshot["minute_rows"])
    latest = snapshot["latest"]
    if apply:
        next_date = target + timedelta(days=1)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO st_north_flow_daily "
                    "(trade_date, net_hgt, net_sgt, net_tgt, buy_hgt, sell_hgt, "
                    "buy_sgt, sell_sgt, buy_tgt, sell_tgt, etl_sync_at) "
                    "VALUES (:trade_date, :net_hgt, :net_sgt, :net_tgt, "
                    "NULL, NULL, NULL, NULL, NULL, NULL, NOW()) "
                    "ON DUPLICATE KEY UPDATE "
                    "net_hgt=VALUES(net_hgt), net_sgt=VALUES(net_sgt), "
                    "net_tgt=VALUES(net_tgt), etl_sync_at=NOW()"
                ),
                daily,
            )
            if minute_rows:
                connection.execute(
                    text(
                        "DELETE FROM st_north_flow_min "
                        "WHERE trade_time>=:start_value AND trade_time<:end_value"
                    ),
                    {"start_value": target, "end_value": next_date},
                )
                connection.execute(
                    text(
                        "INSERT INTO st_north_flow_min "
                        "(trade_time, net_hgt, net_sgt, net_tgt, etl_sync_at) VALUES "
                        "(:trade_time, :net_hgt, :net_sgt, :net_tgt, NOW())"
                    ),
                    minute_rows,
                )
            connection.execute(
                text(
                    "DELETE FROM st_north_flow_current "
                    "WHERE trade_time>=:start_value AND trade_time<:end_value"
                ),
                {"start_value": target, "end_value": next_date},
            )
            connection.execute(
                text(
                    "INSERT INTO st_north_flow_current "
                    "(trade_time, net_hgt, net_sgt, net_tgt, etl_sync_at) "
                    "VALUES (:trade_time, :net_hgt, :net_sgt, :net_tgt, NOW())"
                ),
                latest,
            )
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "provider": "ths_hsgt_day_chart",
        "target_date": target.isoformat(),
        "daily": daily,
        "minute_row_count": len(minute_rows),
        "database_writes": bool(apply),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="latest")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    load_project_env()
    engine = create_tool_engine()
    try:
        target = (
            authoritative_closed_trade_date(engine)
            if args.date == "latest"
            else date.fromisoformat(args.date).isoformat()
        )
        today = datetime.now(SHANGHAI).date().isoformat()
        if target != today:
            raise RuntimeError(
                "THS day chart has no source date; only the current closed "
                "session can be bound without backdating"
            )
        payload = fetch_ths_day_chart(
            timeout_seconds=args.timeout_seconds,
            attempts=args.attempts,
        )
        snapshot = parse_ths_day_chart(payload, target_date=target)
        result = publish_current_day(engine, snapshot, apply=args.apply)
    except Exception as exc:
        result = {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "provider": "ths_hsgt_day_chart",
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "database_writes": False,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 2
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
