# -*- coding: utf-8 -*-
"""Post-run validation for scheduler tasks that are expected to write data."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier, routed_read_engine


@dataclass(frozen=True)
class TableRequirement:
    table: str
    min_rows: int = 1
    date_col: str | None = None
    target: str = "run_date"
    ready_time: str = "00:00"
    distinct_col: str | None = None
    min_distinct: int = 0
    where_sql: str = ""
    freshness_col: str | None = "etl_sync_at"
    require_fresh: bool = True


@dataclass(frozen=True)
class SchedulerValidationResult:
    checked: bool
    ok: bool
    message: str


def is_market_closed_skip_output(output: str | None) -> bool:
    """Return True for an intentional non-trading-day task skip.

    A skipped intraday task must not be post-validated against today's empty
    tables.  The previous behavior turned every weekend/holiday skip into a
    false scheduler failure and obscured real pipeline failures.
    """
    text_value = str(output or "")
    normalized = text_value.lower()
    return (
        "Skipped automatically:" in text_value
        or "skipped: market closed" in normalized
        or '"status": "skipped"' in text_value
        and '"reason": "market_closed"' in text_value
    )


def scheduler_output_status(
    task: Mapping[str, Any],
    output: str | None,
) -> str | None:
    """Map a task's machine-readable result to scheduler semantics.

    Level-1 validation returning BLOCK means the validator ran correctly but
    the capability is unavailable.  Persisting that as ``success`` hides a
    production prerequisite; treating it as ``failed`` would cause needless
    same-day retries.  ``blocked`` accurately represents both conditions.
    """
    if str(task.get("task_type") or "").strip() != (
        "trading_v2_level1_validation"
    ):
        return None
    for line in str(output or "").splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            payload = json.loads(candidate)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        capability_status = str(payload.get("status") or "").upper()
        if capability_status == "PASS":
            return "success"
        if capability_status == "BLOCK":
            return "blocked"
    return None


TASK_OUTPUT_REQUIREMENTS: dict[str, tuple[TableRequirement, ...]] = {
    "all_code": (
        TableRequirement("si_all_code", min_rows=1000, distinct_col="stock_code", min_distinct=1000, require_fresh=False),
    ),
    "all_index_code": (
        TableRequirement("si_all_index_code", min_rows=50, require_fresh=False),
    ),
    "index_constituent": (
        TableRequirement("si_index_constituent", min_rows=5000, require_fresh=False),
    ),
    "concept_code_east": (
        TableRequirement("si_concept_code_east", min_rows=100, require_fresh=False),
    ),
    "concept_constituent_east": (
        TableRequirement("si_concept_constituent_east", min_rows=1000, require_fresh=False),
    ),
    "stock_relations_qmt": (
        TableRequirement("si_stock_plate_east", min_rows=1000, require_fresh=False),
    ),
    "sector_heat_east": (
        TableRequirement(
            "st_hot_concept_ths_daily",
            min_rows=50,
            date_col="snapshot_date",
            distinct_col="concept_code",
            min_distinct=50,
            where_sql="plate_type IN (3, 4)",
        ),
    ),
    "hot_concept": (
        TableRequirement(
            "st_hot_concept_ths_daily",
            min_rows=20,
            date_col="snapshot_date",
            where_sql="plate_type IN (1, 2)",
        ),
    ),
    "hot_rank_ths": (
        TableRequirement("st_hot_rank_ths", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_pop_east": (
        TableRequirement("st_hot_pop_rank_east", min_rows=50, date_col="snapshot_date"),
    ),
    "fetch_hot_rank_xq": (
        TableRequirement("st_hot_rank_xq", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_rank_sina": (
        TableRequirement("st_hot_rank_sina", min_rows=50, date_col="snapshot_date"),
    ),
    "hot_fused": (
        TableRequirement("st_hot_rank_fused", min_rows=20, date_col="snapshot_date"),
    ),
    "hot_fused_3": (
        TableRequirement("st_hot_rank_multi_day", min_rows=20, date_col="stat_date", where_sql="stat_days = 3"),
    ),
    "hot_fused_5": (
        TableRequirement("st_hot_rank_multi_day", min_rows=20, date_col="stat_date", where_sql="stat_days = 5"),
    ),
    "alist_daily": (
        TableRequirement("st_a_list_daily", min_rows=1, date_col="trade_date"),
    ),
    "alist_info": (
        TableRequirement("st_a_list_info", min_rows=1, date_col="trade_date"),
    ),
    "sync_concept_ths": (
        TableRequirement("si_concept_code_ths", min_rows=100, require_fresh=False),
        TableRequirement("si_concept_constituent_ths", min_rows=50000, require_fresh=False),
    ),
    "capital_flow": (
        TableRequirement(
            "sm_stock_capital_flow_daily",
            min_rows=5000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "capital_flow_batch_fast": (
        TableRequirement(
            "sm_stock_capital_flow_daily",
            min_rows=5000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "stock_current": (
        TableRequirement(
            "sm_stock_current",
            min_rows=3000,
            date_col="snapshot_at",
            distinct_col="stock_code",
            min_distinct=5400,
        ),
    ),
    "stock_kline": (
        TableRequirement(
            "sm_stock_kline",
            min_rows=3000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="stock_code",
            min_distinct=3000,
        ),
    ),
    "stock_minute": (
        TableRequirement(
            "sm_stock_minute",
            min_rows=100000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="stock_code",
            min_distinct=3000,
        ),
    ),
    "stock_minute_flow": (
        TableRequirement(
            "sm_stock_capital_flow_min",
            min_rows=100000,
            date_col="trade_time",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="stock_code",
            min_distinct=4500,
        ),
    ),
    "concept_east_current": (
        TableRequirement(
            "sm_concept_east_current",
            min_rows=100,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_ths_current": (
        TableRequirement(
            "sm_concept_ths_current",
            min_rows=100,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_ths_minute": (
        TableRequirement(
            "sm_concept_ths_minute",
            min_rows=1000,
            date_col="trade_date",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "concept_flow": (
        TableRequirement(
            "sm_concept_capital_flow_east",
            min_rows=100,
            date_col="snapshot_at",
            ready_time="19:30",
            distinct_col="index_code",
            min_distinct=100,
        ),
    ),
    "index_current": (
        TableRequirement(
            "sm_index_current",
            min_rows=50,
            date_col="trade_date",
            target="latest_trade_date",
            distinct_col="index_code",
            min_distinct=50,
        ),
    ),
    "index_kline": (
        TableRequirement(
            "sm_index_kline",
            min_rows=50,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:20",
            distinct_col="index_code",
            min_distinct=50,
        ),
    ),
    "index_minute": (
        TableRequirement(
            "sm_index_minute",
            min_rows=1000,
            date_col="trade_date",
            target="latest_trade_date",
            ready_time="15:30",
            distinct_col="index_code",
            min_distinct=20,
        ),
    ),
    "intraday_realtime": (
        TableRequirement(
            "sm_stock_current",
            min_rows=3000,
            date_col="snapshot_at",
            distinct_col="stock_code",
            min_distinct=5400,
        ),
    ),
    "intraday_minute_kline": (
        TableRequirement(
            "sm_stock_minute",
            # Intraday runs begin shortly after the open.  A fixed full-day
            # row threshold falsely fails early runs even when nearly every
            # stock has already produced bars; coverage is enforced below.
            min_rows=5000,
            date_col="trade_date",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "intraday_minute_flow": (
        TableRequirement(
            "sm_stock_capital_flow_min",
            # The first 09:40 run has only a few bars per stock.  Distinct
            # stock coverage is the useful early-session completeness gate.
            min_rows=5000,
            date_col="trade_time",
            distinct_col="stock_code",
            min_distinct=5000,
        ),
    ),
    "market_overview_daily": (
        TableRequirement("sm_market_overview_daily", min_rows=1, date_col="trade_date", target="latest_kline_date", freshness_col="updated_at"),
    ),
    "stock_snapshot_daily": (
        TableRequirement(
            "sm_stock_snapshot",
            min_rows=1000,
            date_col="trade_date",
            target="latest_kline_date",
            distinct_col="stock_code",
            min_distinct=1000,
        ),
    ),
    "analysis_fast": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="latest_kline_date",
            distinct_col="stock_code",
            min_distinct=1000,
            freshness_col="updated_at",
        ),
        TableRequirement("st_recommended_stocks", min_rows=1, date_col="pick_date", target="latest_kline_date", freshness_col="created_at"),
    ),
    "analysis_morning_strict": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="previous_trade_date",
            distinct_col="stock_code",
            min_distinct=1000,
            freshness_col="updated_at",
        ),
        TableRequirement("st_recommended_stocks", min_rows=1, date_col="pick_date", target="previous_trade_date", freshness_col="created_at"),
    ),
    "analysis_premarket_external": (
        TableRequirement(
            "stock_analysis_result",
            min_rows=1000,
            date_col="analysis_date",
            target="previous_trade_date",
            distinct_col="stock_code",
            min_distinct=1000,
            freshness_col="updated_at",
        ),
        TableRequirement("st_recommended_stocks", min_rows=1, date_col="pick_date", target="previous_trade_date", freshness_col="created_at"),
    ),
}


def validate_scheduler_task_result(
    task: Mapping[str, Any],
    *,
    engine: Engine,
    started_at: datetime | None = None,
    now: datetime | None = None,
) -> SchedulerValidationResult:
    task_type = str(task.get("task_type") or "").strip()
    requirements = TASK_OUTPUT_REQUIREMENTS.get(task_type)
    if not requirements:
        return SchedulerValidationResult(checked=False, ok=True, message="no data validation configured")

    started_at = started_at or datetime.now()
    now = now or datetime.now()
    messages: list[str] = []
    try:
        for requirement in requirements:
            ok, message = _validate_requirement(engine, requirement, started_at=started_at, now=now)
            messages.append(message)
            if not ok:
                return SchedulerValidationResult(checked=True, ok=False, message=message)
    except Exception as exc:  # pylint: disable=broad-except
        return SchedulerValidationResult(checked=True, ok=False, message=f"validation error: {exc}")
    return SchedulerValidationResult(checked=True, ok=True, message="; ".join(messages))


def _validate_requirement(
    engine: Engine,
    requirement: TableRequirement,
    *,
    started_at: datetime,
    now: datetime,
) -> tuple[bool, str]:
    columns = _table_columns(engine, requirement.table)
    if not columns:
        return False, f"{requirement.table}: target table does not exist"

    target_date = _resolve_target_date(engine, requirement, started_at=started_at, now=now)
    where_parts: list[str] = []
    params: dict[str, Any] = {}
    if requirement.where_sql:
        where_parts.append(f"({requirement.where_sql})")
    if requirement.date_col:
        if requirement.date_col not in columns:
            return False, f"{requirement.table}: date column {requirement.date_col} does not exist"
        start_date = target_date.isoformat()
        end_date = (target_date + timedelta(days=1)).isoformat()
        where_parts.append(f"{quote_identifier(requirement.date_col)} >= :target_start")
        where_parts.append(f"{quote_identifier(requirement.date_col)} < :target_end")
        params.update({"target_start": start_date, "target_end": end_date})

    select_parts = ["COUNT(*) AS row_count"]
    if requirement.distinct_col:
        if requirement.distinct_col not in columns:
            return False, f"{requirement.table}: distinct column {requirement.distinct_col} does not exist"
        select_parts.append(f"COUNT(DISTINCT {quote_identifier(requirement.distinct_col)}) AS distinct_count")
    freshness_col = requirement.freshness_col if requirement.freshness_col in columns else None
    if freshness_col:
        select_parts.append(f"MAX({quote_identifier(freshness_col)}) AS max_freshness")
    if requirement.date_col:
        select_parts.append(f"MAX({quote_identifier(requirement.date_col)}) AS max_data_time")

    where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    row = _read_one(
        engine,
        f"SELECT {', '.join(select_parts)} FROM {quote_identifier(requirement.table)}{where_sql}",
        params,
    )
    row_count = int(row.get("row_count") or 0)
    if row_count < requirement.min_rows:
        date_note = f" for {target_date.isoformat()}" if requirement.date_col else ""
        return (
            False,
            f"{requirement.table}{date_note}: only {row_count} rows, expected >= {requirement.min_rows}",
        )

    distinct_count = int(row.get("distinct_count") or 0)
    if requirement.distinct_col and distinct_count < requirement.min_distinct:
        date_note = f" for {target_date.isoformat()}" if requirement.date_col else ""
        return (
            False,
            f"{requirement.table}{date_note}: only {distinct_count} distinct {requirement.distinct_col}, "
            f"expected >= {requirement.min_distinct}",
        )

    if requirement.require_fresh and freshness_col:
        max_freshness = _coerce_datetime(row.get("max_freshness"))
        fresh_after = started_at - timedelta(minutes=5)
        if not max_freshness or max_freshness < fresh_after:
            return (
                False,
                f"{requirement.table}: data exists but was not refreshed by this run "
                f"(max {freshness_col}={row.get('max_freshness')})",
            )

    target_note = f" date={target_date.isoformat()}" if requirement.date_col else ""
    distinct_note = f" distinct_{requirement.distinct_col}={distinct_count}" if requirement.distinct_col else ""
    return True, f"{requirement.table}{target_note} rows={row_count}{distinct_note}"


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    # The information_schema query below does not contain the target table in
    # its FROM clause, so route explicitly before inspecting external tables.
    metadata_engine = routed_read_engine(
        f"SELECT * FROM {quote_identifier(table_name)}",
        engine,
    )
    rows = _read_all(
        metadata_engine,
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        """,
        {"table_name": table_name},
    )
    return {str(row.get("COLUMN_NAME")) for row in rows}


def _read_one(engine: Engine, sql: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    rows = _read_all(engine, sql, params)
    return rows[0] if rows else {}


def _read_all(engine: Engine, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    read_engine = routed_read_engine(sql, engine)
    with read_engine.connect() as conn:
        result = conn.execute(text(sql), dict(params or {}))
        return [dict(row) for row in result.mappings().all()]


def _resolve_target_date(
    engine: Engine,
    requirement: TableRequirement,
    *,
    started_at: datetime,
    now: datetime,
) -> date:
    if requirement.target == "latest_trade_date":
        return _latest_trade_date(engine, now=now, ready_time=requirement.ready_time) or started_at.date()
    if requirement.target == "previous_trade_date":
        return _previous_trade_date(engine, ref_date=started_at.date()) or started_at.date()
    if requirement.target == "latest_kline_date":
        return _latest_kline_date(engine) or _latest_trade_date(engine, now=now, ready_time=requirement.ready_time) or started_at.date()
    return started_at.date()


def _latest_trade_date(engine: Engine, *, now: datetime, ready_time: str) -> date | None:
    comparator = "<=" if _time_reached(now, ready_time) else "<"
    row = _read_one(
        engine,
        f"""
        SELECT MAX(trade_date) AS trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date {comparator} :today
        """,
        {"today": now.date().isoformat()},
    )
    return _coerce_date(row.get("trade_date"))


def _previous_trade_date(engine: Engine, *, ref_date: date) -> date | None:
    row = _read_one(
        engine,
        """
        SELECT MAX(trade_date) AS trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date < :ref_date
        """,
        {"ref_date": ref_date.isoformat()},
    )
    return _coerce_date(row.get("trade_date")) or _latest_kline_date(engine)


def _latest_kline_date(engine: Engine) -> date | None:
    row = _read_one(
        engine,
        """
        SELECT MAX(trade_date) AS trade_date
        FROM sm_stock_kline
        WHERE k_type = 1
        """,
    )
    return _coerce_date(row.get("trade_date"))


def _time_reached(now: datetime, hhmm: str) -> bool:
    try:
        hour, minute = str(hhmm or "00:00").split(":", 1)
        return now.hour * 60 + now.minute >= int(hour) * 60 + int(minute)
    except Exception:
        return True


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = _coerce_datetime(value)
    return parsed.date() if parsed else None


def _coerce_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text_value = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value[: len(fmt)], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None
