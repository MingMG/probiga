#!/usr/bin/env python3
"""Build the deterministic daily research pool from one completed session.

The pool deliberately does not depend on the AI analysis chain, QMT evidence
chain or a release-specific build identity. It reads the completed daily
market snapshot, applies transparent liquidity/momentum rules, optionally
joins the same-day main-capital flow, and persists a dated canonical pool.

Every run is bounded: the session query and the optional flow query have
server-side execution limits, and a missing dataset produces a DATA_BLOCKED
receipt instead of an empty success.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import sys
import time
from typing import Any, Iterable, Mapping
import uuid

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine


MIN_SESSION_ROWS = 3000
MIN_AMOUNT = 300_000_000.0
MIN_CLOSE_POSITION = 0.60
MIN_CHANGE_PCT = 0.50
DEFAULT_TOP = 30


class DailyPoolBlocked(RuntimeError):
    def __init__(self, stage: str, reason: str) -> None:
        self.stage = stage
        self.reason = reason
        super().__init__(f"{stage}:{reason}")


def limit_pct_for(stock_code: str) -> float:
    code = str(stock_code or "").strip()
    if code.startswith(("300", "301", "688")):
        return 19.5
    if code.startswith(("4", "8", "92")):
        return 29.5
    return 9.5


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def select_daily_pool(
    rows: Iterable[Mapping[str, Any]],
    flow: Mapping[str, float],
    *,
    top: int = DEFAULT_TOP,
) -> list[dict[str, Any]]:
    """Apply the documented rules and return a ranked, deterministic pool."""
    scored: list[dict[str, Any]] = []
    for row in rows:
        code = str(row.get("stock_code") or "").strip()
        name = str(row.get("stock_name") or row.get("short_name") or "").strip()
        if not code or "ST" in name.upper() or "退" in name:
            continue
        close = _as_float(row.get("close"))
        high = _as_float(row.get("high"))
        low = _as_float(row.get("low"))
        change = _as_float(row.get("change_pct"))
        amount = _as_float(row.get("amount"))
        if None in (close, high, low, change, amount):
            continue
        if high <= low or close <= 0 or amount < MIN_AMOUNT:
            continue
        limit_pct = limit_pct_for(code)
        if not (MIN_CHANGE_PCT <= change <= limit_pct * 0.98):
            continue
        close_position = (close - low) / (high - low)
        if close_position < MIN_CLOSE_POSITION:
            continue
        main_flow = flow.get(code)
        if main_flow is not None and main_flow <= 0:
            continue
        amount_score = min(amount / 2_000_000_000.0, 1.0)
        change_score = min(change / limit_pct, 1.0)
        flow_score = (
            min(main_flow / 500_000_000.0, 1.0)
            if main_flow is not None
            else 0.5
        )
        score = round(
            0.45 * amount_score
            + 0.30 * change_score
            + 0.15 * close_position
            + 0.10 * flow_score,
            6,
        )
        flow_text = (
            f"{main_flow / 1e8:.1f}亿"
            if main_flow is not None
            else "unavailable"
        )
        scored.append(
            {
                "stock_code": code,
                "stock_name": name,
                "score": score,
                "close_price": close,
                "change_pct": change,
                "amount": amount,
                "main_net_inflow": main_flow,
                "close_position": round(close_position, 6),
                "limit_pct": limit_pct,
                "reason": (
                    f"涨幅{change:.2f}% 收盘位于振幅{close_position * 100:.0f}% "
                    f"成交额{amount / 1e8:.1f}亿 主力净流入{flow_text}"
                ),
            }
        )
    scored.sort(
        key=lambda item: (
            -item["score"],
            -item["amount"],
            item["stock_code"],
        )
    )
    return scored[: max(1, int(top))]


def _bounded_session(connection) -> None:
    try:
        connection.execute(text("SET SESSION MAX_EXECUTION_TIME=60000"))
    except Exception:
        pass


def resolve_data_date(connection, requested: str) -> date:
    _bounded_session(connection)
    if requested:
        parsed = date.fromisoformat(requested)
        count = connection.execute(
            text(
                "SELECT COUNT(*) FROM sm_stock_kline "
                "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
            ),
            {"d": parsed},
        ).scalar_one()
        if int(count or 0) < MIN_SESSION_ROWS:
            raise DailyPoolBlocked(
                "daily_kline", f"session_coverage={int(count or 0)}"
            )
        return parsed
    value = connection.execute(
        text(
            "SELECT trade_date FROM sm_stock_kline "
            "WHERE k_type=1 AND adjust_type=0 "
            "ORDER BY trade_date DESC LIMIT 1"
        )
    ).scalar()
    if value is None:
        raise DailyPoolBlocked("daily_kline", "no_completed_session")
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def read_session_rows(connection, data_date: date) -> list[dict[str, Any]]:
    _bounded_session(connection)
    result = connection.execute(
        text(
            "SELECT stock_code, short_name AS stock_name, close, change_pct, "
            "amount, high, low, volume "
            "FROM sm_stock_kline "
            "WHERE trade_date=:d AND k_type=1 AND adjust_type=0 "
            "AND volume > 0 AND amount > 0"
        ),
        {"d": data_date},
    )
    return [dict(row) for row in result.mappings().all()]


def read_main_flow(
    connection, data_date: date
) -> tuple[dict[str, float], str]:
    try:
        connection.execute(text("SET SESSION MAX_EXECUTION_TIME=15000"))
        result = connection.execute(
            text(
                "SELECT stock_code, main_net_inflow "
                "FROM sm_stock_capital_flow_daily WHERE trade_date=:d"
            ),
            {"d": data_date},
        )
        rows = result.mappings().all()
    except Exception:
        return {}, "UNAVAILABLE"
    if len(rows) < 1000:
        return {}, "PARTIAL"
    flow = {
        str(row["stock_code"]).strip(): float(row["main_net_inflow"] or 0)
        for row in rows
        if str(row.get("stock_code") or "").strip()
    }
    return flow, "AVAILABLE"


def write_pool(
    connection,
    *,
    run_uid: str,
    pool_date: date,
    data_date: date,
    candidates: list[dict[str, Any]],
) -> int:
    if not candidates:
        return 0
    connection.execute(
        text(
            "DELETE FROM st_screener_candidate_pool "
            "WHERE source='daily_pool' AND as_of_date=:d"
        ),
        {"d": pool_date},
    )
    statement = text(
        "INSERT INTO st_screener_candidate_pool "
        "(stock_code, stock_name, source, screen_name, score, as_of_date, "
        " status, reason, payload_json) "
        "VALUES (:stock_code, :stock_name, 'daily_pool', "
        " 'close_strength_liquidity', :score, :as_of_date, 'PUBLISHED', "
        " :reason, :payload_json)"
    )
    payloads = []
    for rank_no, item in enumerate(candidates, start=1):
        payload = {
            **item,
            "run_uid": run_uid,
            "rank_no": rank_no,
            "data_date": data_date.isoformat(),
        }
        payloads.append(
            {
                "stock_code": item["stock_code"],
                "stock_name": item["stock_name"],
                "score": item["score"],
                "as_of_date": pool_date,
                "reason": item["reason"],
                "payload_json": json.dumps(
                    payload, ensure_ascii=False, sort_keys=True
                ),
            }
        )
    connection.execute(statement, payloads)
    return len(payloads)


def install_task(engine) -> dict[str, Any]:
    from server.common.scheduler_tasks import upsert_scheduler_task

    payload = {
        "task_name": "每日收盘确定性票池",
        "group_name": "策略研究",
        "task_type": "daily_pool",
        "script_path": "tools/run_daily_pool.py",
        "script_args": "--json",
        "cron_time": "18:30",
        "interval_minutes": 0,
        "enabled": 1,
        "date_param": "",
        "date_param_desc": "空=最近完整交易日",
        "sort_order": 220,
        "description": (
            "每个交易日收盘后按日K流动性/强度生成确定性研究票池，"
            "不依赖AI/QMT证据链，数据不齐时输出DATA_BLOCKED。"
        ),
    }
    return upsert_scheduler_task(
        engine,
        payload,
        lookup_where="task_type=:task_type AND script_path=:script_path",
        lookup_params={
            "task_type": payload["task_type"],
            "script_path": payload["script_path"],
        },
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    engine = create_batch_engine()
    if args.install_task:
        result = install_task(engine)
        return {
            "status": "ok",
            "operation": "install_task",
            "result": result,
        }
    run_uid = uuid.uuid4().hex
    stage = "session_date"
    try:
        with engine.begin() as connection:
            data_date = resolve_data_date(connection, args.date)
            stage = "daily_kline"
            rows = read_session_rows(connection, data_date)
            if len(rows) < MIN_SESSION_ROWS:
                raise DailyPoolBlocked(
                    stage, f"session_coverage={len(rows)}"
                )
            stage = "capital_flow"
            flow, flow_status = read_main_flow(connection, data_date)
            stage = "selection"
            candidates = select_daily_pool(rows, flow, top=args.top)
            if not candidates:
                raise DailyPoolBlocked("selection", "no_candidate")
            stage = "persist"
            written = (
                0
                if args.dry_run
                else write_pool(
                    connection,
                    run_uid=run_uid,
                    pool_date=data_date,
                    data_date=data_date,
                    candidates=candidates,
                )
            )
        return {
            "status": "ok",
            "operation": "daily_pool",
            "run_uid": run_uid,
            "data_date": data_date.isoformat(),
            "pool_date": data_date.isoformat(),
            "session_rows": len(rows),
            "flow_status": flow_status,
            "candidate_count": len(candidates) if args.dry_run else written,
            "dry_run": bool(args.dry_run),
            "top": [
                {
                    "rank": index,
                    "stock_code": item["stock_code"],
                    "stock_name": item["stock_name"],
                    "score": item["score"],
                    "change_pct": item["change_pct"],
                    "amount": item["amount"],
                }
                for index, item in enumerate(candidates[:10], start=1)
            ],
            "duration_seconds": round(time.monotonic() - started, 3),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except DailyPoolBlocked as exc:
        return {
            "status": "blocked",
            "operation": "daily_pool",
            "run_uid": run_uid,
            "stage": exc.stage,
            "reason": exc.reason,
            "duration_seconds": round(time.monotonic() - started, 3),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "operation": "daily_pool",
            "run_uid": run_uid,
            "stage": stage,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
            "duration_seconds": round(time.monotonic() - started, 3),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    finally:
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic daily research pool (not investment advice)"
    )
    parser.add_argument("--date", default="", help="YYYY-MM-DD; empty=latest")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--install-task", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
