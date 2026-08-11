# -*- coding: utf-8 -*-
"""
Data quality checks for the ProBigA stock research pipeline.

The checker is intentionally read-only. It verifies whether the core tables are
fresh and internally consistent enough to support review, recommendation, and
simulation workflows.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    message: str
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "details": self.details or {},
        }


def _status(ok: bool, warn: bool = False) -> str:
    if not ok:
        return "FAIL"
    return "WARN" if warn else "PASS"


def _scalar(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _row(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        row = result.mappings().first()
        return dict(row) if row else {}


def _rows(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(r) for r in result.mappings().all()]


def _table_exists(engine: Engine, table_name: str) -> bool:
    return bool(_scalar(engine, """
        SELECT COUNT(*)
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
    """, {"table_name": table_name}))


def _fmt_date(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def latest_trade_date(engine: Engine) -> str:
    d = _scalar(engine, "SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1")
    return _fmt_date(d)


def expected_latest_trade_date(engine: Engine, as_of: date | None = None) -> str:
    """Latest open trading day in the local trade calendar up to as_of."""
    as_of = as_of or date.today()
    d = _scalar(engine, """
        SELECT MAX(trade_date)
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date <= :as_of
    """, {"as_of": as_of.isoformat()})
    return _fmt_date(d)


def expected_intraday_date(engine: Engine, fallback_trade_date: str) -> str:
    """Today on trading days, otherwise the latest daily-kline trade date."""
    today = date.today().isoformat()
    is_trade_day = bool(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_trade_calendar
        WHERE trade_date = :d
          AND trade_status = 1
    """, {"d": today}) or 0)
    return today if is_trade_day else fallback_trade_date


def is_intraday_session(engine: Engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    is_trade_day = bool(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_trade_calendar
        WHERE trade_date = :d
          AND trade_status = 1
    """, {"d": now.date().isoformat()}) or 0)
    if not is_trade_day:
        return False
    current = now.hour * 100 + now.minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)


def next_trade_date(engine: Engine, after: date | None = None) -> str:
    after = after or date.today()
    d = _scalar(engine, """
        SELECT MIN(trade_date)
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date > :after
    """, {"after": after.isoformat()})
    return _fmt_date(d)


def intraday_readiness(
    engine: Engine,
    trade_date: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return an explicit realtime-trading readiness gate."""
    now = now or datetime.now()
    base_trade_date = trade_date or latest_trade_date(engine)
    intraday_date = expected_intraday_date(engine, base_trade_date) if base_trade_date else now.date().isoformat()
    session_open = is_intraday_session(engine, now)

    base = {
        "generated_at": now.isoformat(timespec="seconds"),
        "trade_date": base_trade_date,
        "intraday_date": intraday_date,
        "is_trading_time": session_open,
        "next_trade_date": next_trade_date(engine, now.date()),
    }

    if not session_open:
        return {
            **base,
            "status": "CLOSED",
            "allow_realtime_trading": False,
            "reason": "market_closed",
            "checks": [],
        }

    required = check_required_tables(engine)
    if required.status == "FAIL" or not base_trade_date:
        return {
            **base,
            "status": "NOT_READY",
            "allow_realtime_trading": False,
            "reason": "required_data_missing",
            "checks": [required.as_dict()],
        }

    checks = [
        check_realtime_freshness(engine, base_trade_date),
        check_intraday_foundation(engine, base_trade_date),
        check_scheduler_health(engine),
    ]
    bad = [c for c in checks if c.status != "PASS"]
    ready = not bad
    return {
        **base,
        "status": "READY" if ready else "NOT_READY",
        "allow_realtime_trading": ready,
        "reason": "ready" if ready else "checks_not_passed",
        "checks": [c.as_dict() for c in checks],
    }


def _date_lag_days(actual: str, expected: str) -> int:
    if not actual or not expected:
        return 9999
    try:
        return (datetime.strptime(expected[:10], "%Y-%m-%d").date() - datetime.strptime(actual[:10], "%Y-%m-%d").date()).days
    except ValueError:
        return 9999


def check_latest_trade_date_freshness(engine: Engine, trade_date: str) -> CheckResult:
    expected = expected_latest_trade_date(engine)
    if not expected:
        return CheckResult(
            name="latest_trade_date_freshness",
            status="WARN",
            message="交易日历缺少可用交易日，无法判断日K最新性",
            details={"actual_trade_date": trade_date, "expected_trade_date": ""},
        )

    lag_days = _date_lag_days(trade_date, expected)
    ok = trade_date == expected
    return CheckResult(
        name="latest_trade_date_freshness",
        status=_status(ok),
        message="日K已跟上最新交易日" if ok else f"日K滞后：当前 {trade_date or '-'}，应到 {expected}",
        details={"actual_trade_date": trade_date, "expected_trade_date": expected, "lag_calendar_days": lag_days},
    )


def check_required_tables(engine: Engine) -> CheckResult:
    tables = [
        "si_all_code",
        "si_trade_calendar",
        "sm_stock_kline",
        "sm_stock_current",
        "sm_stock_capital_flow_daily",
        "sm_index_current",
        "st_hot_rank_fused",
        "st_news_flash",
        "si_notice_eastmoney",
        "stock_analysis_result",
        "st_recommended_stocks",
        "st_scheduled_tasks",
    ]
    missing = [t for t in tables if not _table_exists(engine, t)]
    return CheckResult(
        name="required_tables",
        status=_status(not missing),
        message="核心表完整" if not missing else f"缺少核心表: {', '.join(missing)}",
        details={"missing": missing, "checked": tables},
    )


def check_stock_universe(engine: Engine) -> CheckResult:
    row = _row(engine, """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN stock_code REGEXP '^(0|3|6)' THEN 1 ELSE 0 END) AS a_share_count,
          SUM(CASE WHEN short_name IS NULL OR short_name = '' THEN 1 ELSE 0 END) AS missing_name
        FROM si_all_code
    """)
    total = int(row.get("total") or 0)
    a_share_count = int(row.get("a_share_count") or 0)
    missing_name = int(row.get("missing_name") or 0)
    ok = total >= 4500 and a_share_count >= 4300
    warn = missing_name > 0
    return CheckResult(
        name="stock_universe",
        status=_status(ok, warn),
        message=f"股票池 {total} 只，A股主代码 {a_share_count} 只，缺名称 {missing_name} 只",
        details=row,
    )


def check_trade_date_coverage(engine: Engine, trade_date: str) -> CheckResult:
    active_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_all_code
        WHERE stock_code REGEXP '^(0|3|6)'
          AND (list_date IS NULL OR list_date <= :d)
    """, {"d": trade_date}) or 0)
    kline_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1
    """, {"d": trade_date}) or 0)
    ratio = round(kline_count / max(active_count, 1), 4)
    ok = active_count > 0 and ratio >= 0.92
    warn = ok and ratio < 0.97
    return CheckResult(
        name="daily_kline_coverage",
        status=_status(ok, warn),
        message=f"{trade_date} 日K覆盖 {kline_count}/{active_count} ({ratio:.1%})",
        details={"trade_date": trade_date, "active_count": active_count, "kline_count": kline_count, "coverage": ratio},
    )


def check_kline_integrity(engine: Engine, trade_date: str) -> CheckResult:
    row = _row(engine, """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL THEN 1 ELSE 0 END) AS null_ohlc,
          SUM(CASE WHEN high < low OR high < open OR high < close OR low > open OR low > close THEN 1 ELSE 0 END) AS bad_ohlc,
          SUM(CASE WHEN close <= 0 OR open <= 0 THEN 1 ELSE 0 END) AS nonpositive_price,
          SUM(CASE WHEN ABS(change_pct) > 30 THEN 1 ELSE 0 END) AS extreme_change
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1
    """, {"d": trade_date})
    total = int(row.get("total") or 0)
    issue_count = sum(int(row.get(k) or 0) for k in ("null_ohlc", "bad_ohlc", "nonpositive_price", "extreme_change"))
    ok = total > 0 and issue_count == 0
    return CheckResult(
        name="daily_kline_integrity",
        status=_status(ok),
        message=f"{trade_date} 日K异常 {issue_count} 条" if issue_count else f"{trade_date} 日K价格结构正常",
        details=row,
    )


def check_duplicate_kline(engine: Engine, trade_date: str) -> CheckResult:
    dup_count = int(_scalar(engine, """
        SELECT COUNT(*) FROM (
          SELECT stock_code, trade_date, k_type, adjust_type, COUNT(*) AS c
          FROM sm_stock_kline
          WHERE trade_date = :d AND k_type = 1
          GROUP BY stock_code, trade_date, k_type, adjust_type
          HAVING c > 1
        ) t
    """, {"d": trade_date}) or 0)
    return CheckResult(
        name="duplicate_kline",
        status=_status(dup_count == 0),
        message="日K无重复键" if dup_count == 0 else f"日K重复键 {dup_count} 组",
        details={"trade_date": trade_date, "duplicate_groups": dup_count},
    )


def check_flow_coverage(engine: Engine, trade_date: str) -> CheckResult:
    flow_date = _fmt_date(_scalar(engine, "SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily"))
    flow_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_stock_capital_flow_daily
        WHERE trade_date = :d
    """, {"d": flow_date}) or 0) if flow_date else 0
    kline_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1
    """, {"d": trade_date}) or 0)
    ratio = round(flow_count / max(kline_count, 1), 4)
    same_day = flow_date == trade_date
    ok = bool(flow_date) and ratio >= 0.70
    warn = ok and (not same_day or ratio < 0.90)
    return CheckResult(
        name="capital_flow_coverage",
        status=_status(ok, warn),
        message=f"资金流日期 {flow_date or '-'}，覆盖 {flow_count}/{kline_count} ({ratio:.1%})",
        details={"trade_date": trade_date, "flow_date": flow_date, "flow_count": flow_count, "kline_count": kline_count, "coverage": ratio},
    )


def check_realtime_freshness(engine: Engine, trade_date: str) -> CheckResult:
    if not _table_exists(engine, "sm_rt_quote_snapshot"):
        return CheckResult("realtime_snapshot", "WARN", "缺少 sm_rt_quote_snapshot，盘中模拟会降级到其它行情源")
    intraday_date = expected_intraday_date(engine, trade_date)
    row = _row(engine, """
        SELECT MAX(snapshot_at) AS latest_snapshot, COUNT(DISTINCT stock_code) AS stock_count
        FROM sm_rt_quote_snapshot
        WHERE DATE(snapshot_at) = :d
    """, {"d": intraday_date})
    latest = row.get("latest_snapshot")
    stock_count = int(row.get("stock_count") or 0)
    ok = latest is not None and stock_count >= 1000
    warn = ok and stock_count < 4000
    msg = f"实时快照 {intraday_date} 覆盖 {stock_count} 只，最新 {latest or '-'}"
    return CheckResult("realtime_snapshot", _status(ok, warn), msg, row)


def check_intraday_foundation(engine: Engine, trade_date: str) -> CheckResult:
    """Check data needed by intraday monitors and minute-level strategies."""
    table_names = [
        "sm_stock_minute",
        "sm_stock_current",
        "sm_stock_capital_flow_min",
        "sm_rt_quote_snapshot",
    ]
    missing = [t for t in table_names if not _table_exists(engine, t)]
    if missing:
        return CheckResult(
            name="intraday_foundation",
            status="WARN",
            message=f"缺少盘中基础表: {', '.join(missing)}",
            details={"missing": missing},
        )

    expected_stocks = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1
    """, {"d": trade_date}) or 0)
    intraday_date = expected_intraday_date(engine, trade_date)

    minute_row = _row(engine, """
        SELECT MAX(trade_date) AS latest_date,
               COUNT(DISTINCT stock_code) AS stock_count,
               COUNT(*) AS row_count
        FROM sm_stock_minute
        WHERE trade_date = (SELECT MAX(trade_date) FROM sm_stock_minute)
    """)
    minute_date = _fmt_date(minute_row.get("latest_date"))
    minute_stocks = int(minute_row.get("stock_count") or 0)
    minute_rows = int(minute_row.get("row_count") or 0)
    minute_coverage = round(minute_stocks / max(expected_stocks, 1), 4)

    current_row = _row(engine, """
        SELECT MAX(snapshot_at) AS latest_snapshot,
               COUNT(DISTINCT stock_code) AS stock_count,
               COUNT(*) AS row_count
        FROM sm_stock_current
    """)
    current_latest = current_row.get("latest_snapshot")
    current_date = _fmt_date(current_latest)
    current_stocks = int(current_row.get("stock_count") or 0)
    current_rows = int(current_row.get("row_count") or 0)
    current_coverage = round(current_stocks / max(expected_stocks, 1), 4)

    flow_min_row = _row(engine, """
        SELECT DATE(MAX(trade_time)) AS latest_date,
               COUNT(DISTINCT stock_code) AS stock_count,
               COUNT(*) AS row_count
        FROM sm_stock_capital_flow_min
        WHERE DATE(trade_time) = (
            SELECT DATE(MAX(trade_time))
            FROM sm_stock_capital_flow_min
        )
    """)
    flow_min_date = _fmt_date(flow_min_row.get("latest_date"))
    flow_min_stocks = int(flow_min_row.get("stock_count") or 0)
    flow_min_rows = int(flow_min_row.get("row_count") or 0)
    flow_min_coverage = round(flow_min_stocks / max(expected_stocks, 1), 4)

    snapshot_latest = _scalar(engine, "SELECT MAX(snapshot_at) FROM sm_rt_quote_snapshot")
    snapshot_date = _fmt_date(snapshot_latest)
    snapshot_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_rt_quote_snapshot
        WHERE DATE(snapshot_at) = :d
    """, {"d": intraday_date}) or 0)
    snapshot_coverage = round(snapshot_count / max(expected_stocks, 1), 4)

    ready = (
        expected_stocks > 0
        and minute_date == intraday_date
        and minute_coverage >= 0.70
        and current_date == intraday_date
        and current_coverage >= 0.70
        and snapshot_date == intraday_date
        and snapshot_coverage >= 0.70
        and flow_min_date == intraday_date
        and flow_min_coverage >= 0.50
    )
    partial = minute_rows > 0 or current_rows > 0 or flow_min_rows > 0 or bool(snapshot_latest)
    status = "PASS" if ready else "WARN" if partial else "FAIL"
    if ready:
        msg = (
            f"盘中基础数据可用：分钟线 {minute_stocks}/{expected_stocks}，"
            f"实时行情 {current_stocks}/{expected_stocks}，快照 {snapshot_count}/{expected_stocks}"
        )
    else:
        msg = (
            f"盘中基础数据不足：分钟线 {minute_date or '-'} {minute_stocks}/{expected_stocks} ({minute_coverage:.1%})，"
            f"实时行情 {current_date or '-'} {current_stocks}/{expected_stocks} ({current_coverage:.1%})，"
            f"快照 {snapshot_date or '-'} {snapshot_count}/{expected_stocks} ({snapshot_coverage:.1%})，"
            f"分钟资金流 {flow_min_date or '-'} {flow_min_stocks}/{expected_stocks} ({flow_min_coverage:.1%})"
        )
    return CheckResult(
        name="intraday_foundation",
        status=status,
        message=msg,
        details={
            "trade_date": trade_date,
            "intraday_date": intraday_date,
            "expected_stock_count": expected_stocks,
            "minute_date": minute_date,
            "minute_stock_count": minute_stocks,
            "minute_row_count": minute_rows,
            "minute_coverage": minute_coverage,
            "current_date": current_date,
            "current_stock_count": current_stocks,
            "stock_current_rows": current_rows,
            "current_coverage": current_coverage,
            "flow_min_date": flow_min_date,
            "flow_min_stock_count": flow_min_stocks,
            "flow_min_rows": flow_min_rows,
            "flow_min_coverage": flow_min_coverage,
            "snapshot_date": snapshot_date,
            "snapshot_stock_count": snapshot_count,
            "snapshot_coverage": snapshot_coverage,
        },
    )


def check_news_and_notices(engine: Engine, trade_date: str) -> CheckResult:
    news_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM st_news_flash
        WHERE publish_time >= CONCAT(:d, ' 00:00:00')
    """, {"d": trade_date}) or 0)
    notice_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_notice_eastmoney
        WHERE notice_date >= DATE_SUB(:d, INTERVAL 7 DAY)
    """, {"d": trade_date}) or 0)
    ok = news_count >= 10 and notice_count >= 100
    warn = not ok and (news_count > 0 or notice_count > 0)
    return CheckResult(
        name="news_notice_freshness",
        status=_status(ok, warn),
        message=f"{trade_date} 新闻 {news_count} 条，近7日公告 {notice_count} 条",
        details={"trade_date": trade_date, "news_count": news_count, "notice_count_7d": notice_count},
    )


def check_analysis_outputs(engine: Engine, trade_date: str) -> CheckResult:
    analysis_date = _fmt_date(_scalar(engine, "SELECT MAX(analysis_date) FROM stock_analysis_result"))
    analysis_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM stock_analysis_result
        WHERE analysis_date = :d
    """, {"d": analysis_date}) or 0) if analysis_date else 0
    rec_date = _fmt_date(_scalar(engine, "SELECT MAX(pick_date) FROM st_recommended_stocks"))
    rec_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM st_recommended_stocks
        WHERE pick_date = :d
    """, {"d": rec_date}) or 0) if rec_date else 0
    ok = analysis_count >= 1000
    warn = ok and (analysis_date != trade_date or rec_date != trade_date or rec_count == 0)
    return CheckResult(
        name="analysis_outputs",
        status=_status(ok, warn),
        message=f"分析结果 {analysis_date or '-'}: {analysis_count} 条；推荐池 {rec_date or '-'}: {rec_count} 条",
        details={"trade_date": trade_date, "analysis_date": analysis_date, "analysis_count": analysis_count, "recommend_date": rec_date, "recommend_count": rec_count},
    )


def _scheduler_health_status(bad_tasks: list[dict[str, Any]]) -> str:
    """Scheduler issues are operational warnings unless the task table is missing."""
    return "WARN" if bad_tasks else "PASS"


def check_scheduler_health(engine: Engine) -> CheckResult:
    if not _table_exists(engine, "st_scheduled_tasks"):
        return CheckResult("scheduler_health", "WARN", "缺少调度任务表")
    rows = _rows(engine, """
        SELECT task_name, script_path, cron_time, interval_minutes,
               last_run_status, last_run_at, last_triggered_at,
               TIMESTAMPDIFF(MINUTE, last_run_at, NOW()) AS age_minutes
        FROM st_scheduled_tasks
        WHERE enabled = 1
          AND (
            COALESCE(last_run_status, '') IN ('failed', 'timeout', 'stopped', '')
            OR (last_run_status = 'running'
                AND (last_run_at IS NULL OR last_run_at < NOW() - INTERVAL 30 MINUTE))
            OR (last_run_status = 'success' AND last_run_at IS NULL)
          )
        ORDER BY last_run_at DESC
        LIMIT 20
    """)
    bad = [dict(r) for r in rows]
    status = _scheduler_health_status(bad)
    return CheckResult(
        name="scheduler_health",
        status=status,
        message="调度任务状态正常" if not bad else f"调度异常任务 {len(bad)} 个",
        details={"bad_tasks": bad},
    )


def check_schema_collation(engine: Engine) -> CheckResult:
    targets = [
        "si_all_code.stock_code",
        "sm_stock_kline.stock_code",
        "sm_stock_capital_flow_daily.stock_code",
        "stock_analysis_result.stock_code",
        "st_recommended_stocks.stock_code",
    ]
    rows = _rows(engine, """
        SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME) AS column_key, COLLATION_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND CONCAT(TABLE_NAME, '.', COLUMN_NAME) IN :targets
    """, {"targets": tuple(targets)})
    seen = {r.get("column_key"): r.get("COLLATION_NAME") for r in rows}
    missing = [t for t in targets if t not in seen]
    bad = {k: v for k, v in seen.items() if v and v != "utf8mb4_unicode_ci"}
    ok = not missing and not bad
    return CheckResult(
        name="schema_collation",
        status=_status(ok),
        message="关键代码列排序规则一致" if ok else "关键代码列排序规则不一致",
        details={"expected": "utf8mb4_unicode_ci", "bad": bad, "missing": missing},
    )


def check_sim_trade_integrity(engine: Engine) -> CheckResult:
    if not _table_exists(engine, "st_trade_flow"):
        return CheckResult("sim_trade_integrity", "WARN", "缺少 st_trade_flow，模拟交易尚未初始化")
    offhours = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM st_trade_flow
        WHERE COALESCE(trade_mode, 'live') = 'live'
          AND trans_time <> ''
          AND NOT (
            (trans_time >= '09:25:00' AND trans_time <= '11:31:00')
            OR (trans_time >= '12:59:00' AND trans_time <= '15:01:00')
          )
    """) or 0)
    invalid_modes = _rows(engine, """
        SELECT COALESCE(trade_mode, '') AS trade_mode, COUNT(*) AS cnt
        FROM st_trade_flow
        WHERE COALESCE(trade_mode, '') NOT IN ('live', 'backtest', 'forward', 'invalid_offhours')
        GROUP BY COALESCE(trade_mode, '')
    """)
    ok = offhours == 0 and not invalid_modes
    return CheckResult(
        name="sim_trade_integrity",
        status=_status(ok),
        message="模拟交易流水口径正常" if ok else f"模拟交易流水异常: 非交易时段live={offhours}, 未知模式={len(invalid_modes)}",
        details={"live_offhours_count": offhours, "invalid_modes": invalid_modes},
    )


def run_checks(engine: Engine, trade_date: str | None = None, include_realtime: bool = False) -> dict[str, Any]:
    required = check_required_tables(engine)
    if required.status == "FAIL":
        return {
            "status": "FAIL",
            "trade_date": trade_date or "",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "checks": [required.as_dict()],
        }

    trade_date = trade_date or latest_trade_date(engine)
    if not trade_date:
        return {
            "status": "FAIL",
            "trade_date": "",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "checks": [
                required.as_dict(),
                CheckResult("latest_trade_date", "FAIL", "sm_stock_kline 无交易日数据").as_dict(),
            ],
        }

    checks = [
        required,
        check_latest_trade_date_freshness(engine, trade_date),
        check_stock_universe(engine),
        check_trade_date_coverage(engine, trade_date),
        check_kline_integrity(engine, trade_date),
        check_duplicate_kline(engine, trade_date),
        check_flow_coverage(engine, trade_date),
        check_news_and_notices(engine, trade_date),
        check_analysis_outputs(engine, trade_date),
        check_scheduler_health(engine),
        check_schema_collation(engine),
        check_sim_trade_integrity(engine),
    ]
    if include_realtime:
        checks.append(check_realtime_freshness(engine, trade_date))
    if os.environ.get("DQ_SKIP_INTRADAY", "") != "1":
        checks.append(check_intraday_foundation(engine, trade_date))

    statuses = [c.status for c in checks]
    overall = "FAIL" if "FAIL" in statuses else "WARN" if "WARN" in statuses else "PASS"
    return {
        "status": overall,
        "trade_date": trade_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "checks": [c.as_dict() for c in checks],
    }


def print_report(report: dict[str, Any]) -> None:
    print(f"ProBigA 数据质量体检 | {report.get('trade_date') or '-'} | {report['status']}")
    print(f"生成时间: {report['generated_at']}")
    print("-" * 72)
    icon = {"PASS": "[OK]", "WARN": "[WARN]", "FAIL": "[FAIL]"}
    for item in report["checks"]:
        print(f"{icon.get(item['status'], '[?]')} {item['name']}: {item['message']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="ProBigA 数据质量体检")
    parser.add_argument("--date", default="", help="交易日 YYYY-MM-DD，默认使用 sm_stock_kline 最新交易日")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    parser.add_argument("--include-realtime", action="store_true", help="检查 sm_rt_quote_snapshot 当日覆盖")
    parser.add_argument("--fail-on-warn", action="store_true", help="有 WARN 时也返回非0")
    parser.add_argument("--skip-closed", action="store_true", help="非交易时段直接跳过并返回成功")
    parser.add_argument("--readiness", action="store_true", help="输出盘中实时交易就绪状态")
    args = parser.parse_args()

    engine = create_batch_engine()
    if args.readiness:
        readiness = intraday_readiness(engine, args.date.strip() or None)
        if args.json:
            print(json.dumps(readiness, ensure_ascii=False, default=str, indent=2))
        else:
            print(f"ProBigA 盘中就绪 | {readiness['status']} | allow={readiness['allow_realtime_trading']}")
        return 2 if readiness["status"] == "NOT_READY" else 0

    if args.skip_closed and not is_intraday_session(engine):
        skipped = {
            "status": "SKIPPED",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
        if args.json:
            print(json.dumps(skipped, ensure_ascii=False, default=str, indent=2))
        else:
            print("ProBigA 数据质量体检 | SKIPPED | market_closed")
        return 0

    report = run_checks(engine, args.date.strip() or None, include_realtime=args.include_realtime)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, default=str, indent=2))
    else:
        print_report(report)

    if report["status"] == "FAIL":
        return 2
    if args.fail_on_warn and report["status"] == "WARN":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
