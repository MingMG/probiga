#!/usr/bin/env python3
"""Generate, persist and deliver the daily production candidate ranking."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers.screener import (
    ScreenerRunRequest,
    decode_screener_task_request,
    execute_screener_task,
)
from biz.analysis.trading_wecom import (
    notify_screener_failure,
    select_screener_delivery_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="intraday_sector")
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--request-token", default="")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    request = None
    try:
        request = (
            decode_screener_task_request(args.request_token)
            if args.request_token
            else ScreenerRunRequest(
                preset=args.preset,
                as_of_date=args.as_of_date,
                universe="market",
                top=args.top,
                filters={"exclude_st": True},
                notify=True,
            )
        )
        result = execute_screener_task(request)
    except Exception as exc:
        notification = (
            notify_screener_failure(
                preset=args.preset,
                reason="筛选任务执行失败，请查看调度审计",
                stage="生成或落库",
            )
            if request is None or bool(request.notify)
            else {"status": "skipped", "reason": "notification_not_requested"}
        )
        print(
            json.dumps(
                {
                    "status": "error",
                    "preset": args.preset,
                    "error": "SCREENER_TASK_FAILED",
                    "error_type": type(exc).__name__,
                    "notification": notification,
                },
                ensure_ascii=False,
            )
        )
        return 5
    delivery_rows = select_screener_delivery_rows(result)
    output = {
        "status": result.get("status"),
        "preset": result.get("preset"),
        "data_date": result.get("data_date"),
        "observed_at": result.get("observed_at"),
        "freshness": result.get("freshness"),
        "result_count": len(result.get("data") or []),
        "delivered_top_five": [
            {
                "rank": row.get("rank"),
                "stock_code": row.get("stock_code"),
                "stock_name": row.get("stock_name") or row.get("short_name"),
                "score": (
                    row.get("ensemble_score")
                    if row.get("ensemble_score") is not None
                    else row.get("score")
                ),
                "change_pct": row.get("change_pct"),
            }
            for row in delivery_rows[:5]
        ],
        "screened_count": len(result.get("data") or []),
        "qualified_count": len(delivery_rows),
        "run": result.get("run"),
        "notification": result.get("notification"),
        "error": result.get("error"),
    }
    if not result.get("data") and bool(request and request.notify):
        output["notification"] = notify_screener_failure(
            preset=args.preset,
            reason=str(result.get("error") or "本次筛选没有生成任何候选"),
            stage="结果校验",
        )
    print(json.dumps(output, ensure_ascii=False, default=str))
    if not (result.get("run") or {}).get("persisted"):
        return 2
    notification_status = str((result.get("notification") or {}).get("status") or "").lower()
    notification_reason = str((result.get("notification") or {}).get("reason") or "")
    if result.get("data") and notification_status == "skipped" and notification_reason != "same_snapshot_already_sent":
        return 3
    if result.get("data") and notification_status not in {"sent", "skipped"}:
        return 3
    if not result.get("data"):
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
