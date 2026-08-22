#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run the daily dynamic strategy governance close cycle."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)


def _load_project_env() -> None:
    from tools.env_config import load_project_env

    load_project_env()


def _create_tool_engine():
    from tools.env_config import create_tool_engine

    return create_tool_engine()


def _blocked(
    reason: str,
    target_trade_date: str = "",
    input_trade_date: str = "",
) -> int:
    print(
        json.dumps(
            {
                "status": "blocked",
                "reason": reason,
                "target_trade_date": target_trade_date,
                "input_trade_date": input_trade_date,
                "automatic_real_order_submission": False,
            },
            ensure_ascii=False,
            default=str,
        )
    )
    return 2


def _input_block_reason(
    snapshot: dict, target_trade_date: str, input_ready: bool, input_reason: str
) -> str:
    """Reject an internally consistent snapshot when it is for an older day."""

    if not input_ready:
        return str(input_reason or "治理输入未就绪")
    snapshot_trade_date = str(snapshot.get("trade_date") or "")[:10]
    snapshot_data_date = str(snapshot.get("data_date") or "")[:10]
    if (
        snapshot_trade_date != target_trade_date
        or snapshot_data_date != target_trade_date
    ):
        return (
            "底层票池尚未产出权威已收盘交易日数据"
            f"（要求{target_trade_date}，实际交易日"
            f"{snapshot_trade_date or 'missing'}、数据日"
            f"{snapshot_data_date or 'missing'}）"
        )
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="更新策略治理、竞技榜、票池和模拟权重")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    _load_project_env()

    requested_trade_date = str(args.trade_date or "").strip()
    calendar_engine = _create_tool_engine()
    try:
        try:
            target_trade_date = authoritative_closed_trade_date(
                calendar_engine
            )
        except Exception as exc:
            return _blocked(
                "权威交易日历暂不可用；未写入治理状态，模拟资金保持现金"
                f"（{type(exc).__name__}）"
            )
    finally:
        calendar_engine.dispose()
    if not target_trade_date:
        return _blocked(
            "权威交易日历没有已收盘交易日；未写入治理状态，模拟资金保持现金"
        )
    if requested_trade_date and requested_trade_date != target_trade_date:
        return _blocked(
            "指定交易日不是权威已收盘交易日"
            f"（要求{target_trade_date}，指定{requested_trade_date}）；"
            "未写入治理状态，模拟资金保持现金",
            target_trade_date,
        )

    from server.engine.strategy_governance import (
        GovernanceEvidenceNotReady,
        governance_snapshot,
    )

    try:
        result = governance_snapshot(
            trade_date=target_trade_date,
            persist=True,
            operator="scheduled_daily_governance",
            strategy_limit=max(1, min(500, args.limit)),
        )
    except GovernanceEvidenceNotReady as exc:
        return _blocked(
            "权威治理窗口或证据账本未就绪；未写入治理状态，"
            "模拟资金保持现金"
            f"（{type(exc).__name__}: {str(exc)[:300]}）",
            target_trade_date,
            target_trade_date,
        )
    print(json.dumps({
        "status": result.get("status"),
        "run_uid": result.get("run_uid"),
        "trade_date": result.get("trade_date"),
        "summary": result.get("summary"),
        "lifecycle_transitions": result.get("lifecycle_transitions"),
        "allocations": result.get("allocations"),
        "automatic_real_order_submission": False,
    }, ensure_ascii=False, default=str))
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
