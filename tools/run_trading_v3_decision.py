#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.decision_worker import run_daily_decision_v3
from tools.env_config import create_tool_engine, load_project_env


DEFAULT_UNIVERSE_LIMIT = 1200
DEFAULT_PER_SLEEVE_LIMIT = 300
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(microsecond=0)
    return value.astimezone(_SHANGHAI).replace(
        tzinfo=None,
        microsecond=0,
    )


def execution_enabled_for_request(
    *,
    as_of: date,
    decision_at: datetime,
    today: date | None = None,
) -> bool:
    """Only the current Shanghai trade date may touch the paper OMS."""

    local_decision_at = _shanghai_naive(decision_at)
    local_today = today or datetime.now(_SHANGHAI).date()
    return as_of == local_today and local_decision_at.date() == as_of


def resolve_decision_at(
    *,
    as_of: date,
    mode: str,
    as_of_was_explicit: bool,
    explicit: str = "",
    now: datetime | None = None,
) -> datetime:
    """Use a reproducible clock for historical as-of decisions."""

    if explicit:
        resolved = _shanghai_naive(
            datetime.fromisoformat(explicit.replace("Z", "+00:00"))
        )
        if resolved.date() != as_of:
            raise ValueError(
                "decision_at Shanghai date must equal the requested as_of"
            )
        return resolved
    current = _shanghai_naive(now or datetime.now(_SHANGHAI))
    if not as_of_was_explicit or as_of == current.date():
        return current
    decision_times = {
        "premarket": time(9, 26),
        "close": time(16, 5),
        "manual": time(16, 5),
    }
    return datetime.combine(as_of, decision_times[mode])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default="")
    parser.add_argument(
        "--decision-at",
        default="",
        help="ISO 时间；历史回放缺省使用固定盘前/收盘时钟",
    )
    parser.add_argument(
        "--mode",
        choices=("close", "premarket", "manual"),
        default="close",
    )
    parser.add_argument(
        "--universe-limit",
        type=int,
        default=DEFAULT_UNIVERSE_LIMIT,
    )
    parser.add_argument(
        "--per-sleeve-limit",
        type=int,
        default=DEFAULT_PER_SLEEVE_LIMIT,
    )
    parser.add_argument(
        "--replay-only",
        action="store_true",
        help="只生成历史决策证据，禁止持仓同步和模拟订单物化",
    )
    args = parser.parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    requested_as_of = (
        date.fromisoformat(args.as_of)
        if args.as_of
        else datetime.now(_SHANGHAI).date()
    )
    decision_at = resolve_decision_at(
        as_of=requested_as_of,
        mode=args.mode,
        as_of_was_explicit=bool(args.as_of),
        explicit=args.decision_at,
    )
    try:
        try:
            result = run_daily_decision_v3(
                primary,
                as_of=requested_as_of,
                decision_at=decision_at,
                mode=args.mode,
                universe_limit=max(100, min(args.universe_limit, 5000)),
                per_sleeve_limit=max(
                    50,
                    min(args.per_sleeve_limit, 5000),
                ),
                kline_engine=kline,
                execution_enabled=execution_enabled_for_request(
                    as_of=requested_as_of,
                    decision_at=decision_at,
                )
                and not args.replay_only,
            )
        except Exception as exc:
            result = {
                "schema": "probiga.trading-v3-decision-result.v1",
                "status": "failed",
                "retryable": True,
                "run_status": "FAILED",
                "actionable_status": "UNAVAILABLE",
                "decision_at": decision_at.isoformat(sep=" "),
                "error": str(exc),
            }
        from biz.analysis.trading_wecom import notify_v3_decision_result

        result["notification"] = notify_v3_decision_result(result)
    finally:
        primary.dispose()
        kline.dispose()
    # One schema-labelled JSON line gives the scheduler an unambiguous receipt
    # even when providers or notification libraries emit surrounding logs.
    print(json.dumps(result, ensure_ascii=False, default=str))
    if result.get("status") == "ok":
        return 0
    return 2 if result.get("status") == "blocked" else 1


if __name__ == "__main__":
    raise SystemExit(main())
