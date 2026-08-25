# -*- coding: utf-8 -*-
"""Argument construction for scheduler-managed scripts."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

NO_DEFAULT_DATE_TASK_TYPES = {
    "early_briefing",
    "evening_review",
    "intraday_market_alert",
    "market_overview_daily",
    "news_daily",
    "public_quote_failover",
    "qmt_announcement_pit",
    "stock_snapshot_daily",
    "strategy_governance_daily",
    "trading_v2_intraday_activation",
    "trading_v2_job_worker",
    "trading_v2_level1_validation",
    "trading_v2_paper_tick",
    "trading_v2_reconciliation",
    "trading_v2_strategy_health",
    "trading_v3_counterfactual_audit",
    "trading_v3_continuous_calibration",
}

NO_DEFAULT_DATE_PATHS = {
    "biz/early_briefing/generate.py",
    "biz/evening_review/generate.py",
    "tools/run_intraday_market_alert.py",
    "biz/stock_market/sync_stock_snapshot.py",
    "tools/refresh_market_overview_daily.py",
}


def _date_param_args(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if ":" in raw:
        return [part for part in raw.split(":") if part]
    return raw.split()


def build_scheduler_task_args(row: Mapping[str, Any], script_path: str, today: str) -> list[str]:
    """Build command-line args consistently for manual and scheduled runs."""
    script_args_raw = str(row.get("script_args") or "").strip()
    date_param_raw = str(row.get("date_param") or "").strip()
    task_type = str(row.get("task_type") or "").strip()
    normalized_path = str(script_path or "").replace("\\", "/").strip()

    args = script_args_raw.split() if script_args_raw else []
    args.extend(_date_param_args(date_param_raw))
    if not args:
        if task_type not in NO_DEFAULT_DATE_TASK_TYPES and normalized_path not in NO_DEFAULT_DATE_PATHS:
            args.append(today)

    if "run_single_table" in normalized_path and len(args) == 1:
        args.append(today)
    return args
