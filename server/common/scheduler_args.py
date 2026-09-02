# -*- coding: utf-8 -*-
"""Argument construction for scheduler-managed scripts."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from tools.qmt_announcement_task_contract import (
    ANALYSIS_DAILY_PIPELINE_DECISION_TIME,
)

NO_DEFAULT_DATE_TASK_TYPES = {
    "early_briefing",
    "evening_review",
    "intraday_market_alert",
    "market_overview_daily",
    "news_daily",
    "portfolio_quote_refresh",
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

RELEASE_QMT_RANGE_TARGET_TASK_TYPES = frozenset(
    {
        "qmt_index_current",
        "qmt_index_kline",
        "qmt_index_minute",
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
    }
)
RELEASE_QMT_TRADE_DATE_TARGET_TASK_TYPES = frozenset(
    {"qmt_stock_minute_flow_canonical"}
)
RELEASE_EXPLICIT_TRADE_DATE_TARGET_TASK_TYPES = frozenset(
    {
        "eastmoney_concept_current",
        "eastmoney_concept_flow_snapshot",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "etf_forward_daily",
    }
)
RELEASE_LATEST_SESSION_TARGET_TASK_TYPES = frozenset(
    {"alist_daily", "alist_info"}
)
RELEASE_POSITIONAL_DATE_TARGET_TASK_TYPES = frozenset({"sector_heat_east"})
ANALYSIS_EXECUTION_TIME_TASK_TYPES = frozenset(
    {
        "analysis_fast",
        "analysis_morning_strict",
        "analysis_premarket_external",
    }
)
ANALYSIS_DAILY_EVIDENCE_TASK_TYPES = frozenset(
    {
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
    }
)


def _date_param_args(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if ":" in raw:
        return [part for part in raw.split(":") if part]
    return raw.split()


def _has_option(args: list[str], option: str) -> bool:
    return any(item == option or item.startswith(f"{option}=") for item in args)


def _option_values(args: list[str], option: str) -> list[str]:
    values: list[str] = []
    for index, item in enumerate(args):
        if item.startswith(f"{option}="):
            values.append(item.split("=", 1)[1])
        elif item == option:
            values.append(args[index + 1] if index + 1 < len(args) else "")
    return values


def _is_iso_date_arg(value: str) -> bool:
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError:
        return False
    return parsed.isoformat() == value


def _analysis_execution_time(row: Mapping[str, Any]) -> str:
    """Return the scheduler-bound Shanghai wall clock for analysis children."""

    raw = str(row.get("_scheduler_execution_time") or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            "scheduler analysis execution time is unavailable"
        ) from exc
    if (
        parsed.tzinfo is not None
        or parsed.isoformat(timespec="seconds") != raw
    ):
        raise ValueError("scheduler analysis execution time is unavailable")
    return raw


def _bind_analysis_execution_time(
    args: list[str],
    row: Mapping[str, Any],
) -> None:
    execution_time = _analysis_execution_time(row)
    explicit = _option_values(args, "--execution-time")
    if explicit and explicit != [execution_time]:
        raise ValueError(
            "analysis execution time differs from scheduler dispatch clock"
        )
    if not explicit:
        args.extend(["--execution-time", execution_time])


def _analysis_pipeline_decision_at(
    row: Mapping[str, Any],
    *,
    target_date: str,
) -> str:
    if str(row.get("_scheduler_pipeline_target_date") or "").strip() != target_date:
        raise ValueError(
            "scheduler analysis pipeline target date differs from scheduler"
        )
    raw = str(row.get("_scheduler_pipeline_decision_at") or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            "scheduler analysis pipeline decision cutoff is unavailable"
        ) from exc
    release_catchup = (
        str(row.get("_trigger_source") or "").strip() == "release_catchup"
    )
    if (
        parsed.tzinfo is not None
        or parsed.microsecond != 0
        or parsed.isoformat(timespec="seconds") != raw
        or (
            not release_catchup
            and raw
            != f"{target_date}T{ANALYSIS_DAILY_PIPELINE_DECISION_TIME}"
        )
        or (
            release_catchup
            and parsed
            < datetime.combine(
                date.fromisoformat(target_date),
                datetime.min.time(),
            ).replace(hour=15, minute=10)
        )
    ):
        raise ValueError(
            "scheduler analysis pipeline decision cutoff differs from contract"
        )
    return raw


def _bind_daily_evidence_identity(
    args: list[str],
    row: Mapping[str, Any],
    *,
    target_date: str,
) -> None:
    decision_at = _analysis_pipeline_decision_at(
        row,
        target_date=target_date,
    )
    explicit_targets = _option_values(args, "--target-date")
    explicit_cutoffs = _option_values(args, "--decision-at")
    if explicit_targets and explicit_targets != [target_date]:
        raise ValueError("analysis evidence target date differs from scheduler")
    if explicit_cutoffs and explicit_cutoffs != [decision_at]:
        raise ValueError("analysis evidence decision cutoff differs from scheduler")
    if not explicit_targets:
        args.extend(["--target-date", target_date])
    if not explicit_cutoffs:
        args.extend(["--decision-at", decision_at])


def build_scheduler_task_args(row: Mapping[str, Any], script_path: str, today: str) -> list[str]:
    """Build command-line args consistently for manual and scheduled runs."""
    script_args_raw = str(row.get("script_args") or "").strip()
    date_param_raw = str(row.get("date_param") or "").strip()
    task_type = str(row.get("task_type") or "").strip()
    normalized_path = str(script_path or "").replace("\\", "/").strip()
    release_catchup = (
        str(row.get("_trigger_source") or "").strip() == "release_catchup"
    )

    args = script_args_raw.split() if script_args_raw else []
    args.extend(_date_param_args(date_param_raw))
    if task_type in ANALYSIS_DAILY_EVIDENCE_TASK_TYPES:
        if not _is_iso_date_arg(today):
            raise ValueError("analysis evidence target date is invalid")
        if task_type == "analysis_upper_evidence_prepare":
            if args.count("--prepare-preliminary") != 1:
                raise ValueError(
                    "upper evidence task requires one canonical preliminary build"
                )
            if _has_option(args, "--preliminary-receipt-file"):
                raise ValueError(
                    "scheduled upper evidence may not consume a local receipt file"
                )
        _bind_daily_evidence_identity(args, row, target_date=today)
        return args
    if release_catchup and task_type == "qmt_membership_snapshot":
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up membership target date is invalid")
        if args != ["--apply", "--force-reference-refresh", "--json"]:
            raise ValueError(
                "release catch-up membership task arguments differ from contract"
            )
        return [
            "--verify-existing-snapshot",
            "--snapshot-date",
            today,
            "--json",
        ]
    if task_type == "stock_finance":
        if not _is_iso_date_arg(today):
            raise ValueError("stock finance target date is invalid")
        if args.count("--daily-incremental") != 1 or _has_option(
            args, "--seal-existing"
        ):
            raise ValueError(
                "scheduled stock finance requires the daily incremental contract"
            )
        explicit_dates = _option_values(args, "--as-of-date")
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "stock finance target date differs from scheduler"
            )
        if not explicit_dates:
            args.extend(["--as-of-date", today])
        return args
    if release_catchup and task_type in RELEASE_QMT_RANGE_TARGET_TASK_TYPES:
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up QMT target date is invalid")
        if args.count("--latest-session") != 1 or any(
            _has_option(args, option) for option in ("--start-date", "--end-date")
        ):
            raise ValueError(
                "release catch-up QMT range requires one latest-session selector"
            )
        args.remove("--latest-session")
        args.extend(["--start-date", today, "--end-date", today])
        return args
    if release_catchup and task_type in RELEASE_QMT_TRADE_DATE_TARGET_TASK_TYPES:
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up QMT target date is invalid")
        if args.count("--latest-session") != 1 or _has_option(
            args,
            "--trade-date",
        ):
            raise ValueError(
                "release catch-up QMT minute-flow requires one latest-session selector"
            )
        args.remove("--latest-session")
        args.extend(["--trade-date", today])
        return args
    if release_catchup and task_type == "capital_flow_batch_fast":
        if not _is_iso_date_arg(today):
            raise ValueError(
                "release catch-up capital-flow target date is invalid"
            )
        explicit_dates = _option_values(args, "--trade-date")
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up capital-flow date differs from authoritative target"
            )
        if not explicit_dates:
            args.extend(["--trade-date", today])
        return args
    if release_catchup and task_type in RELEASE_LATEST_SESSION_TARGET_TASK_TYPES:
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up alist target date is invalid")
        if args.count("--latest-session") != 1 or _has_option(
            args,
            "--trade-date",
        ):
            raise ValueError(
                "release catch-up alist requires one latest-session selector"
            )
        args.remove("--latest-session")
        args.extend(["--trade-date", today])
        return args
    if (
        release_catchup
        and task_type in RELEASE_EXPLICIT_TRADE_DATE_TARGET_TASK_TYPES
    ):
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up provider target date is invalid")
        explicit_dates = _option_values(args, "--trade-date")
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up provider date differs from authoritative target"
            )
        if any(
            _has_option(args, option) for option in ("--start-date", "--end-date")
        ):
            raise ValueError(
                "release catch-up provider requires one authoritative target"
            )
        if not explicit_dates:
            args.extend(["--trade-date", today])
        return args
    if release_catchup and task_type in RELEASE_POSITIONAL_DATE_TARGET_TASK_TYPES:
        if not _is_iso_date_arg(today):
            raise ValueError("release catch-up positional target date is invalid")
        explicit_dates = [item for item in args if _is_iso_date_arg(item)]
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up positional date differs from authoritative target"
            )
        if not explicit_dates:
            args.append(today)
        return args
    if task_type == "analysis_fast":
        if release_catchup:
            explicit_dates = _option_values(args, "--date")
            if explicit_dates and explicit_dates != [today]:
                raise ValueError(
                    "release catch-up analysis date differs from authoritative target"
                )
            pipeline_cutoff = _analysis_pipeline_decision_at(
                row,
                target_date=today,
            )
            if _analysis_execution_time(row) != pipeline_cutoff:
                raise ValueError(
                    "release catch-up analysis cutoff differs from daily pipeline"
                )
        if not _has_option(args, "--date"):
            args.extend(["--date", today])
        _bind_analysis_execution_time(args, row)
        return args
    if task_type == "analysis_morning_strict" and release_catchup:
        if "--strict-prev-trade-day" not in args:
            raise ValueError(
                "release catch-up morning analysis lost strict previous-session mode"
            )
        if _has_option(args, "--date"):
            raise ValueError(
                "release catch-up morning analysis may not override previous session"
            )
        release_execution_time = str(
            row.get("_release_execution_time") or ""
        ).strip()
        execution_time = _analysis_execution_time(row)
        if release_execution_time != execution_time:
            raise ValueError(
                "release catch-up morning execution time differs"
            )
        _bind_analysis_execution_time(args, row)
        return args
    if task_type in {
        "analysis_morning_strict",
        "analysis_premarket_external",
    }:
        _bind_analysis_execution_time(args, row)
        return args
    if task_type == "trading_v3_close_decision" and release_catchup:
        modes = _option_values(args, "--mode")
        if modes != ["close"]:
            raise ValueError(
                "release catch-up V3 decision requires close mode"
            )
        explicit_dates = _option_values(args, "--as-of")
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up V3 decision date differs from authoritative target"
            )
        decision_at = f"{today}T16:05:00"
        explicit_decision_times = _option_values(args, "--decision-at")
        if explicit_decision_times and explicit_decision_times != [decision_at]:
            raise ValueError(
                "release catch-up V3 decision clock differs from replay target"
            )
        if not explicit_dates:
            args.extend(["--as-of", today])
        if not explicit_decision_times:
            args.extend(["--decision-at", decision_at])
        if not _has_option(args, "--replay-only"):
            args.append("--replay-only")
        return args
    if task_type == "sim_trade_signal_prepare" and release_catchup:
        explicit_dates = _option_values(args, "--trade-date")
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up signal-pool date differs from current target"
            )
        if not explicit_dates:
            args.extend(["--trade-date", today])
        return args
    if task_type == "hot_fused" and release_catchup:
        if _has_option(args, "--date"):
            raise ValueError(
                "release catch-up fused hot-rank requires a positional date"
            )
        explicit_dates = [item for item in args if _is_iso_date_arg(item)]
        if explicit_dates and explicit_dates != [today]:
            raise ValueError(
                "release catch-up fused hot-rank date differs from current target"
            )
        if not explicit_dates:
            args.insert(0, today)
        return args
    if task_type == "stock_snapshot_daily":
        if release_catchup:
            explicit_dates = _option_values(args, "--date")
            if explicit_dates and explicit_dates != [today]:
                raise ValueError(
                    "release catch-up stock snapshot date differs from authoritative target"
                )
        if not _has_option(args, "--date"):
            args.extend(["--date", today])
        return args
    if task_type == "market_overview_daily":
        if release_catchup:
            if any(
                _has_option(args, option)
                for option in ("--dates", "--start-date", "--end-date")
            ):
                raise ValueError(
                    "release catch-up market overview requires one authoritative target"
                )
            explicit_dates = [item for item in args if _is_iso_date_arg(item)]
            if explicit_dates and explicit_dates != [today]:
                raise ValueError(
                    "release catch-up market overview date differs from authoritative target"
                )
        has_explicit_date = (
            any(
                _has_option(args, option)
                for option in ("--dates", "--start-date", "--end-date")
            )
            or any(_is_iso_date_arg(item) for item in args)
        )
        if not has_explicit_date:
            args.append(today)
        return args
    if not args:
        if task_type not in NO_DEFAULT_DATE_TASK_TYPES and normalized_path not in NO_DEFAULT_DATE_PATHS:
            args.append(today)

    if "run_single_table" in normalized_path and len(args) == 1:
        args.append(today)
    return args
